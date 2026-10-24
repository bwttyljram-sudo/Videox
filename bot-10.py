# -*- coding: utf-8 -*-
"""
Vidox | فيديوكس  -  main bot (aiogram 3, long polling, plain Bot API token)

Start command on Render:   python bot.py
Environment variables (Render):
    BOT_TOKEN      - bot token from @BotFather
    TURSO_DATABASE_URL - libsql://your-db.turso.io
    TURSO_AUTH_TOKEN   - the Turso database auth token
Optional:
    ADMIN_IDS      - extra admin ids (comma separated), merged with the list below
    WEBAPP_URL     - only if you want to override the automatic Render URL
"""
import asyncio
import base64
import html
import io
import json
import logging
import os
import re
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from urllib.parse import quote

import httpx
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import (
    TelegramAPIError,
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramRetryAfter,
)
from aiogram.filters import Command, CommandObject, CommandStart, Filter
from aiogram.types import (
    BotCommand,
    BotCommandScopeChat,
    CallbackQuery,
    ChatMemberUpdated,
    ErrorEvent,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    WebAppInfo,
)

import keep_alive

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("vidox")

# =====================================================================
#  SETTINGS YOU CAN EDIT (not secrets) - edit here, then redeploy
# =====================================================================
STORAGE_CHANNEL_ID = -1003936270194          # private channel the admins upload videos to
BOT_USERNAME = "vdox_2_bot"                   # without @ (used for referral links)

ADMIN_IDS = {8902791537}          # more admins can be added here (any number)

# channels for the optional "join and get a gift" step
SUB_CHANNELS = [
    {"chat": "@ZenoX_Tools", "url": "https://t.me/ZenoX_Tools", "title": "ZenoX Tools"},
    {"chat": "@Videox_1", "url": "https://t.me/Videox_1", "title": "Videox"},
]

PAYMENT_BOT_USERNAME = "to_push_bot"   # the Stars payment bot
SUPPORT_USERNAME = "ti_tan_7"   # default support account; the admin can change it from /admin -> Settings
MANIFEST_CHANNEL_ID = 0     # optional text channel: a details message is posted there when a package is published

# If your phone shows the reply-keyboard buttons in the opposite left/right order, set True
KEYBOARD_MIRROR_AR = False

RATE_MAX, RATE_WINDOW = 8, 4.0   # max 8 actions per 4 seconds per user (admins are exempt)

# =====================================================================
#  ENVIRONMENT (set in Render)
# =====================================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
TURSO_DATABASE_URL = os.getenv("TURSO_DATABASE_URL", "").strip()
TURSO_AUTH_TOKEN = os.getenv("TURSO_AUTH_TOKEN", "").strip()
PORT = int(os.getenv("PORT", "10000"))

for _p in re.split(r"[,\s;]+", os.getenv("ADMIN_IDS", "")):
    if _p.strip().isdigit():
        ADMIN_IDS.add(int(_p))

WEBAPP_URL = os.getenv("WEBAPP_URL", "").strip()
# Auto-announcement of a new package (channel post + broadcast to all users)
UPDATES_CHANNEL = "@Videox_1"                              # updates channel (bot must be admin there)
AUTO_ANNOUNCE = os.getenv("AUTO_ANNOUNCE", "1").strip() != "0"   # set AUTO_ANNOUNCE=0 to disable
APP_SHORT_NAME = os.getenv("APP_SHORT_NAME", "").strip()   # optional: Mini App short name from BotFather
if not WEBAPP_URL:
    _ext = os.getenv("RENDER_EXTERNAL_URL", "").strip().rstrip("/")
    WEBAPP_URL = f"{_ext}/app" if _ext else ""

# =====================================================================
#  small helpers
# =====================================================================
esc = html.escape


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")


def iso_now() -> str:
    return iso(now_utc())


def fmt_size(n: Any) -> str:
    n = int(n or 0)
    if n >= 1 << 30:
        return f"{n / (1 << 30):.1f} GB"
    if n >= 1 << 20:
        return f"{n / (1 << 20):.0f} MB"
    if n >= 1 << 10:
        return f"{n / (1 << 10):.0f} KB"
    return f"{n} B"


def fmt_dur(s: Any) -> str:
    s = int(s or 0)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"


def bahrain_time(dt_str: Optional[str]) -> str:
    if not dt_str:
        return "-"
    try:
        d = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return (d + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return dt_str


# =====================================================================
#  Turso (libSQL) database - thin async client over the HTTP API, no extra SDK
# =====================================================================
class DBError(Exception):
    def __init__(self, status: int, text: str):
        super().__init__(f"DB {status}: {text[:300]}")
        self.status = status
        self.text = text


_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_INTRE = re.compile(r"^-?\d+$")
_OPS = {"eq": "=", "neq": "<>", "gte": ">=", "gt": ">", "lte": "<=", "lt": "<"}
_NOW_SQL = "strftime('%Y-%m-%dT%H:%M:%S+00:00','now')"


def _sv(v: Any) -> dict:
    """python value -> Turso (Hrana) value"""
    if v is None:
        return {"type": "null"}
    if isinstance(v, bool):
        return {"type": "integer", "value": str(int(v))}
    if isinstance(v, int):
        return {"type": "integer", "value": str(v)}
    if isinstance(v, float):
        return {"type": "float", "value": v}
    if isinstance(v, (bytes, bytearray)):
        return {"type": "blob", "base64": base64.b64encode(bytes(v)).decode()}
    if isinstance(v, (dict, list)):
        return {"type": "text", "value": json.dumps(v, ensure_ascii=False)}
    return {"type": "text", "value": str(v)}


def _pv(c: dict) -> Any:
    """Turso value -> python value"""
    t = c.get("type")
    if t == "null":
        return None
    if t == "integer":
        return int(c["value"])
    if t == "float":
        return float(c["value"])
    if t == "blob":
        s = c.get("base64", "")
        return base64.b64decode(s + "=" * (-len(s) % 4))
    return c.get("value")


def _stmt(sql: str, args: Optional[list] = None) -> dict:
    return {"sql": sql, "args": [_sv(a) for a in (args or [])]}


def _cv(s: str) -> Any:
    if s == "true":
        return 1
    if s == "false":
        return 0
    if _INTRE.match(s):
        return int(s)
    return s


def _where(filters: Optional[dict], prefix: str = "") -> tuple:
    parts: list = []
    args: list = []
    for k, v in (filters or {}).items():
        if not _IDENT.match(k):
            raise DBError(400, f"bad column {k}")
        op, _, rest = str(v).partition(".")
        col = prefix + k
        if op == "in":
            vals = [_cv(x) for x in rest.strip("()").split(",") if x != ""]
            if not vals:
                parts.append("0")
            else:
                parts.append(f"{col} IN ({','.join('?' * len(vals))})")
                args += vals
        elif op == "is":
            parts.append(f"{col} IS NULL" if rest == "null" else f"{col} IS NOT NULL")
        elif op in _OPS:
            parts.append(f"{col} {_OPS[op]} ?")
            args.append(_cv(rest))
        else:
            raise DBError(400, f"bad filter {v}")
    return (" WHERE " + " AND ".join(parts)) if parts else "", args


def _cols(columns: str) -> str:
    if columns.strip() == "*":
        return "*"
    out = []
    for c in columns.split(","):
        c = c.strip()
        if not _IDENT.match(c):
            raise DBError(400, f"bad column {c}")
        out.append(c)
    return ", ".join(out)


def _order(order: Optional[str]) -> str:
    if not order:
        return ""
    parts = []
    for tok in order.split(","):
        col, _, d = tok.strip().partition(".")
        if not _IDENT.match(col):
            raise DBError(400, f"bad order {tok}")
        parts.append(f"{col} {'DESC' if d.lower() == 'desc' else 'ASC'}")
    return " ORDER BY " + ", ".join(parts)


SCHEMA = [
    "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    f"""CREATE TABLE IF NOT EXISTS users (
        telegram_id INTEGER PRIMARY KEY, username TEXT, full_name TEXT,
        language TEXT NOT NULL DEFAULT 'en', lang_set INTEGER NOT NULL DEFAULT 0,
        onboarded INTEGER NOT NULL DEFAULT 0, channels_gift_claimed INTEGER NOT NULL DEFAULT 0,
        points_balance INTEGER NOT NULL DEFAULT 0 CHECK (points_balance >= 0),
        is_premium INTEGER NOT NULL DEFAULT 0, is_banned INTEGER NOT NULL DEFAULT 0,
        is_active INTEGER NOT NULL DEFAULT 1, referrer_id INTEGER,
        created_at TEXT NOT NULL DEFAULT ({_NOW_SQL}),
        last_active_at TEXT NOT NULL DEFAULT ({_NOW_SQL}))""",
    "CREATE INDEX IF NOT EXISTS users_referrer_idx ON users (referrer_id)",
    "CREATE INDEX IF NOT EXISTS users_last_active_idx ON users (last_active_at)",
    f"""CREATE TABLE IF NOT EXISTS categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL,
        sort_order INTEGER NOT NULL DEFAULT 0, is_visible INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL DEFAULT ({_NOW_SQL}))""",
    f"""CREATE TABLE IF NOT EXISTS packages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        category_id INTEGER REFERENCES categories (id) ON DELETE SET NULL,
        title TEXT NOT NULL, description TEXT,
        price_in_points INTEGER NOT NULL DEFAULT 0 CHECK (price_in_points >= 0),
        status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','active','hidden','deleted')),
        allow_save INTEGER NOT NULL DEFAULT 1, sort_order INTEGER NOT NULL DEFAULT 0,
        created_by INTEGER, created_at TEXT NOT NULL DEFAULT ({_NOW_SQL}), published_at TEXT)""",
    "CREATE INDEX IF NOT EXISTS packages_category_idx ON packages (category_id, status)",
    f"""CREATE TABLE IF NOT EXISTS videos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        package_id INTEGER NOT NULL REFERENCES packages (id) ON DELETE CASCADE,
        title TEXT, channel_id INTEGER NOT NULL, channel_message_id INTEGER NOT NULL,
        telegram_file_id TEXT, file_unique_id TEXT, duration INTEGER,
        file_size INTEGER NOT NULL DEFAULT 0, mime_type TEXT,
        has_thumb INTEGER NOT NULL DEFAULT 0, sort_order INTEGER NOT NULL DEFAULT 0,
        is_broken INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL DEFAULT ({_NOW_SQL}),
        UNIQUE (channel_id, channel_message_id))""",
    "CREATE INDEX IF NOT EXISTS videos_package_idx ON videos (package_id, sort_order)",
    """CREATE TABLE IF NOT EXISTS thumbs (
        video_id INTEGER PRIMARY KEY REFERENCES videos (id) ON DELETE CASCADE, data BLOB NOT NULL)""",
    f"""CREATE TABLE IF NOT EXISTS user_subscriptions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        telegram_id INTEGER NOT NULL REFERENCES users (telegram_id) ON DELETE CASCADE,
        package_id INTEGER NOT NULL REFERENCES packages (id) ON DELETE CASCADE,
        price_paid INTEGER NOT NULL DEFAULT 0, unlocked_at TEXT NOT NULL DEFAULT ({_NOW_SQL}),
        UNIQUE (telegram_id, package_id))""",
    f"""CREATE TABLE IF NOT EXISTS points_transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT, telegram_id INTEGER NOT NULL,
        amount INTEGER NOT NULL, type TEXT NOT NULL, ref_id TEXT, note TEXT, balance_after INTEGER,
        created_at TEXT NOT NULL DEFAULT ({_NOW_SQL}))""",
    "CREATE INDEX IF NOT EXISTS ptx_user_idx ON points_transactions (telegram_id, created_at)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ptx_unique_ref ON points_transactions (telegram_id, type, ref_id) WHERE ref_id IS NOT NULL",
    f"""CREATE TABLE IF NOT EXISTS user_views (
        telegram_id INTEGER NOT NULL, kind TEXT NOT NULL CHECK (kind IN ('category','package')),
        item_id INTEGER NOT NULL, viewed_at TEXT NOT NULL DEFAULT ({_NOW_SQL}),
        PRIMARY KEY (telegram_id, kind, item_id))""",
    f"""CREATE TABLE IF NOT EXISTS admin_state (
        admin_id INTEGER PRIMARY KEY, state TEXT NOT NULL, data TEXT NOT NULL DEFAULT '{{}}',
        updated_at TEXT NOT NULL DEFAULT ({_NOW_SQL}))""",
    f"""CREATE TABLE IF NOT EXISTS upload_sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        package_id INTEGER NOT NULL REFERENCES packages (id) ON DELETE CASCADE,
        admin_id INTEGER NOT NULL, mode TEXT NOT NULL CHECK (mode IN ('new','add','reupload')),
        status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','closed')),
        status_msg INTEGER, created_at TEXT NOT NULL DEFAULT ({_NOW_SQL}))""",
    "CREATE UNIQUE INDEX IF NOT EXISTS upload_one_open ON upload_sessions (status) WHERE status = 'open'",
    f"""CREATE TABLE IF NOT EXISTS broadcasts (
        id INTEGER PRIMARY KEY AUTOINCREMENT, admin_id INTEGER NOT NULL, audience TEXT NOT NULL,
        src_chat INTEGER NOT NULL, src_message INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'running'
            CHECK (status IN ('running','done','cancelled','interrupted','deleted')),
        total INTEGER NOT NULL DEFAULT 0, sent INTEGER NOT NULL DEFAULT 0,
        failed INTEGER NOT NULL DEFAULT 0, blocked INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT ({_NOW_SQL}), finished_at TEXT)""",
    """CREATE TABLE IF NOT EXISTS broadcast_recipients (
        broadcast_id INTEGER NOT NULL REFERENCES broadcasts (id) ON DELETE CASCADE,
        telegram_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
        PRIMARY KEY (broadcast_id, telegram_id))""",
    f"""CREATE TABLE IF NOT EXISTS promo_codes (
        code TEXT PRIMARY KEY, points INTEGER NOT NULL CHECK (points > 0),
        max_uses INTEGER NOT NULL DEFAULT 0, used INTEGER NOT NULL DEFAULT 0,
        expires_at TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1, created_by INTEGER,
        created_at TEXT NOT NULL DEFAULT ({_NOW_SQL}),
        CHECK (max_uses = 0 OR used <= max_uses))""",
    f"""CREATE TABLE IF NOT EXISTS user_consent (
        telegram_id INTEGER PRIMARY KEY, accepted_at TEXT NOT NULL DEFAULT ({_NOW_SQL}))""",
    f"""CREATE TABLE IF NOT EXISTS admin_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, admin_id INTEGER, action TEXT NOT NULL,
        details TEXT, created_at TEXT NOT NULL DEFAULT ({_NOW_SQL}))""",
    """INSERT OR IGNORE INTO settings (key, value) VALUES
        ('referral_reward','2'), ('channels_gift','50'), ('new_badge_days','14'),
        ('maintenance','false'), ('referrals_enabled','true'), ('gift_enabled','true')""",
]


class DB:
    """Turso (libSQL) over its HTTP API. Same small interface the rest of the bot uses."""

    def __init__(self, url: str, token: str):
        url = url.strip().rstrip("/")
        if url.startswith("libsql://"):
            url = "https://" + url[len("libsql://"):]
        elif url.startswith("wss://"):
            url = "https://" + url[len("wss://"):]
        elif not url.startswith("http"):
            url = "https://" + url
        self.endpoint = url + "/v2/pipeline"
        self.headers = {"Authorization": f"Bearer {token.strip()}", "Content-Type": "application/json"}
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0),
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20, keepalive_expiry=2.0),
        )
        self.lock = asyncio.Lock()

    # ------------------------------------------------------------ transport
    async def _post(self, requests: list, safe_retry: bool) -> list:
        body = {"requests": requests + [{"type": "close"}]}
        last: Any = None
        for attempt in range(4):
            try:
                r = await self.client.post(self.endpoint, headers=self.headers, json=body)
            except (httpx.ConnectError, httpx.ConnectTimeout) as e:
                last = e
                await asyncio.sleep(0.7 * (attempt + 1))
                continue
            except httpx.TransportError as e:
                last = e
                # "Server disconnected" = a pooled connection the server already closed while idle:
                # retry once on a fresh connection (also for writes), reads are always retried.
                stale = isinstance(e, httpx.RemoteProtocolError) and attempt == 0
                if safe_retry or stale:
                    await asyncio.sleep(0.4 * (attempt + 1))
                    continue
                raise DBError(0, f"connection lost: {e}")
            if r.status_code == 429 or r.status_code >= 500:
                last = r.text
                if attempt < 3 and (safe_retry or r.status_code in (429, 502, 503)):
                    await asyncio.sleep(0.7 * (attempt + 1))
                    continue
                raise DBError(r.status_code, r.text)
            if r.status_code >= 400:
                raise DBError(r.status_code, r.text)
            return r.json()["results"]
        raise DBError(0, f"connection failed: {last}")

    @staticmethod
    def _parse(res: dict) -> dict:
        cols = [c.get("name") for c in res.get("cols", [])]
        rows = [{cols[i]: _pv(cell) for i, cell in enumerate(row)} for row in res.get("rows", [])]
        return {"rows": rows, "affected": int(res.get("affected_row_count") or 0)}

    async def _run(self, stmts: list) -> list:
        """Run independent statements in one round-trip. Returns [{rows, affected}]."""
        reqs = [{"type": "execute", "stmt": _stmt("PRAGMA foreign_keys = ON")}]
        reqs += [{"type": "execute", "stmt": _stmt(s, a)} for s, a in stmts]
        safe = all(s.lstrip()[:6].upper() == "SELECT" for s, _ in stmts)
        results = await self._post(reqs, safe)
        out = []
        for res in results[1:1 + len(stmts)]:
            if res.get("type") == "error":
                raise DBError(400, res.get("error", {}).get("message", "error"))
            out.append(self._parse(res["response"]["result"]))
        return out

    async def _q(self, sql: str, args: Optional[list] = None) -> list:
        return (await self._run([(sql, args or [])]))[0]["rows"]

    async def tx(self, stmts: list) -> list:
        """All statements succeed together or none is applied (one atomic round-trip)."""
        n = len(stmts)
        steps: list = [{"stmt": _stmt("PRAGMA foreign_keys = ON")}, {"stmt": _stmt("BEGIN")}]
        for i, (s, a) in enumerate(stmts):
            steps.append({"stmt": _stmt(s, a), "condition": {"type": "ok", "step": 1 + i}})
        commit = 2 + n
        steps.append({"stmt": _stmt("COMMIT"), "condition": {"type": "ok", "step": commit - 1}})
        steps.append({"stmt": _stmt("ROLLBACK"),
                      "condition": {"type": "not", "cond": {"type": "ok", "step": commit}}})
        results = await self._post([{"type": "batch", "batch": {"steps": steps}}], False)
        r = results[0]
        if r.get("type") == "error":
            raise DBError(400, r.get("error", {}).get("message", "error"))
        res = r["response"]["result"]
        sr, se = res["step_results"], res["step_errors"]
        if sr[commit] is None:
            for i in range(2, commit + 1):
                if se[i]:
                    raise DBError(409, se[i].get("message", "transaction failed"))
            raise DBError(409, "transaction failed")
        return [self._parse(sr[2 + i]) for i in range(n)]

    async def init_schema(self) -> None:
        results = await self._post(
            [{"type": "execute", "stmt": _stmt(s)} for s in SCHEMA], False)
        for res in results[:len(SCHEMA)]:
            if res.get("type") == "error":
                raise DBError(400, res.get("error", {}).get("message", "schema error"))

    # ------------------------------------------------------------- CRUD
    async def select(self, table: str, columns: str = "*", filters: Optional[dict] = None,
                     order: Optional[str] = None, limit: Optional[int] = None,
                     offset: Optional[int] = None) -> list:
        if not _IDENT.match(table):
            raise DBError(400, "bad table")
        if "(" in columns:                      # user_subscriptions + package title
            w, a = _where(filters, "s.")
            sql = ("SELECT s.package_id AS package_id, p.title AS title FROM user_subscriptions s "
                   "JOIN packages p ON p.id = s.package_id" + w + " ORDER BY s.id")
            if limit is not None:
                sql += f" LIMIT {int(limit)}"
            rows = await self._q(sql, a)
            return [{"package_id": r["package_id"], "packages": {"title": r["title"]}} for r in rows]
        w, a = _where(filters)
        sql = f"SELECT {_cols(columns)} FROM {table}{w}{_order(order)}"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
            if offset:
                sql += f" OFFSET {int(offset)}"
        rows = await self._q(sql, a)
        if table == "admin_state":
            for r in rows:
                if isinstance(r.get("data"), str):
                    try:
                        r["data"] = json.loads(r["data"])
                    except ValueError:
                        r["data"] = {}
        return rows

    async def one(self, table: str, filters: Optional[dict] = None, columns: str = "*",
                  order: Optional[str] = None) -> Optional[dict]:
        rows = await self.select(table, columns, filters, order=order, limit=1)
        return rows[0] if rows else None

    async def count(self, table: str, filters: Optional[dict] = None) -> int:
        if not _IDENT.match(table):
            raise DBError(400, "bad table")
        w, a = _where(filters)
        rows = await self._q(f"SELECT COUNT(*) AS n FROM {table}{w}", a)
        return int(rows[0]["n"]) if rows else 0

    async def insert(self, table: str, rows: Any, on_conflict: Optional[str] = None,
                     upsert: bool = False, ignore_duplicates: bool = False) -> list:
        if not _IDENT.match(table):
            raise DBError(400, "bad table")
        lst = rows if isinstance(rows, list) else [rows]
        if not lst:
            return []
        stmts = []
        for row in lst:
            keys = list(row.keys())
            for k in keys:
                if not _IDENT.match(k):
                    raise DBError(400, f"bad column {k}")
            sql = f"INSERT INTO {table} ({', '.join(keys)}) VALUES ({', '.join('?' * len(keys))})"
            if upsert and on_conflict:
                targets = [c.strip() for c in on_conflict.split(",")]
                upd = [k for k in keys if k not in targets]
                if upd:
                    sql += (f" ON CONFLICT ({', '.join(targets)}) DO UPDATE SET "
                            + ", ".join(f"{k} = excluded.{k}" for k in upd))
                else:
                    sql += f" ON CONFLICT ({', '.join(targets)}) DO NOTHING"
            elif ignore_duplicates:
                sql += " ON CONFLICT DO NOTHING"
            sql += " RETURNING *"
            stmts.append((sql, [row[k] for k in keys]))
        out: list = []
        for i in range(0, len(stmts), 40):
            for res in await self._run(stmts[i:i + 40]):
                out += res["rows"]
        return out

    async def update(self, table: str, filters: dict, values: dict) -> list:
        if not _IDENT.match(table) or not filters or not values:
            raise DBError(400, "bad update")
        keys = list(values.keys())
        for k in keys:
            if not _IDENT.match(k):
                raise DBError(400, f"bad column {k}")
        w, a = _where(filters)
        sql = f"UPDATE {table} SET {', '.join(k + ' = ?' for k in keys)}{w} RETURNING *"
        return (await self._run([(sql, [values[k] for k in keys] + a)]))[0]["rows"]

    async def delete(self, table: str, filters: dict) -> None:
        if not _IDENT.match(table) or not filters:
            raise DBError(400, "bad delete")
        w, a = _where(filters)
        await self._run([(f"DELETE FROM {table}{w}", a)])

    # --------------------------------------------------------- thumbnails
    async def save_thumb(self, video_id: int, data: bytes) -> None:
        await self._run([
            ("INSERT OR REPLACE INTO thumbs (video_id, data) VALUES (?, ?)", [video_id, data]),
            ("UPDATE videos SET has_thumb = 1 WHERE id = ?", [video_id]),
        ])

    async def get_thumb(self, video_id: int) -> Optional[bytes]:
        rows = await self._q("SELECT data FROM thumbs WHERE video_id = ?", [video_id])
        return rows[0]["data"] if rows else None

    # ------------------------------------------------------------- "RPC"
    async def rpc(self, fn: str, args: Optional[dict] = None) -> Any:
        f = getattr(self, "rpc_" + fn, None)
        if f is None:
            raise DBError(404, f"unknown function {fn}")
        return await f(args or {})

    async def _user(self, uid: int) -> Optional[dict]:
        rows = await self._q("SELECT * FROM users WHERE telegram_id = ?", [uid])
        return rows[0] if rows else None

    async def _add_points(self, uid: int, amount: int, typ: str, ref: Optional[str],
                          note: Optional[str]) -> dict:
        u = await self._user(uid)
        if not u:
            return {"ok": False, "error": "no_user"}
        bal = u["points_balance"]
        if bal + amount < 0:
            return {"ok": False, "error": "insufficient", "balance": bal}
        try:
            await self.tx([
                ("INSERT INTO points_transactions (telegram_id, amount, type, ref_id, note, balance_after) "
                 "VALUES (?, ?, ?, ?, ?, (SELECT points_balance FROM users WHERE telegram_id = ?) + ?)",
                 [uid, amount, typ, ref, note, uid, amount]),
                ("UPDATE users SET points_balance = points_balance + ? WHERE telegram_id = ?", [amount, uid]),
            ])
        except DBError as e:
            m = str(e).upper()
            if "UNIQUE" in m:
                return {"ok": False, "error": "duplicate", "balance": bal}
            if "CHECK" in m:
                return {"ok": False, "error": "insufficient", "balance": bal}
            raise
        u2 = await self._user(uid)
        return {"ok": True, "balance": u2["points_balance"] if u2 else bal + amount}

    async def rpc_add_points(self, a: dict) -> dict:
        return await self._add_points(a["p_user"], int(a["p_amount"]), a["p_type"],
                                      a.get("p_ref"), a.get("p_note"))

    async def rpc_redeem_code(self, a: dict) -> dict:
        """Atomic: one redemption per user per code, optional max uses, expiry."""
        uid, code = a["p_user"], a["p_code"]
        row = (await self._q("SELECT * FROM promo_codes WHERE code = ?", [code]) or [None])[0]
        if not row or not row["active"]:
            return {"ok": False, "error": "invalid"}
        if row["expires_at"] <= iso_now():
            return {"ok": False, "error": "expired"}
        u = await self._user(uid)
        if not u:
            return {"ok": False, "error": "no_user"}
        pts = int(row["points"])
        try:
            await self.tx([
                ("INSERT INTO points_transactions (telegram_id, amount, type, ref_id, note, balance_after) "
                 "VALUES (?, ?, 'promo', ?, ?, (SELECT points_balance FROM users WHERE telegram_id = ?) + ?)",
                 [uid, pts, code, "promo code", uid, pts]),
                ("UPDATE promo_codes SET used = used + 1 WHERE code = ?", [code]),
                ("UPDATE users SET points_balance = points_balance + ? WHERE telegram_id = ?", [pts, uid]),
            ])
        except DBError as e:
            m = str(e).upper()
            if "UNIQUE" in m:
                return {"ok": False, "error": "used"}
            if "CHECK" in m:
                return {"ok": False, "error": "exhausted"}
            raise
        u2 = await self._user(uid)
        return {"ok": True, "points": pts, "balance": u2["points_balance"] if u2 else u["points_balance"] + pts}

    async def rpc_register_user(self, a: dict) -> dict:
        uid = a["p_id"]
        ref = a.get("p_ref")
        reward = int(a.get("p_reward") or 0)
        v_ref = None
        if ref and ref != uid and await self._user(ref):
            v_ref = ref
        res = (await self._run([(
            "INSERT OR IGNORE INTO users (telegram_id, username, full_name, language, referrer_id) "
            "VALUES (?, ?, ?, ?, ?)",
            [uid, a.get("p_username"), a.get("p_name"), a.get("p_lang") or "en", v_ref])]))[0]
        created = res["affected"] > 0
        if not created:
            await self._run([(
                f"UPDATE users SET username = ?, full_name = ?, is_active = 1, last_active_at = {_NOW_SQL} "
                "WHERE telegram_id = ?", [a.get("p_username"), a.get("p_name"), uid])])
        u = await self._user(uid)
        ref_ok, ref_bal = False, None
        if created and u and u.get("referrer_id") and reward > 0:
            r = await self._add_points(u["referrer_id"], reward, "referral", str(uid), None)
            if r.get("ok"):
                ref_ok, ref_bal = True, r["balance"]
        return {"is_new": created, "user": u, "ref_credited": ref_ok,
                "ref_id": (u or {}).get("referrer_id"), "ref_balance": ref_bal}

    async def rpc_purchase_package(self, a: dict) -> dict:
        uid, pid = a["p_user"], a["p_package"]
        pk = (await self._q("SELECT * FROM packages WHERE id = ?", [pid]) or [None])[0]
        if not pk or pk["status"] != "active":
            return {"ok": False, "error": "unavailable"}
        u = await self._user(uid)
        if not u:
            return {"ok": False, "error": "no_user"}
        bal, price = u["points_balance"], pk["price_in_points"]
        if await self._q("SELECT 1 AS x FROM user_subscriptions WHERE telegram_id = ? AND package_id = ?", [uid, pid]):
            return {"ok": False, "error": "owned", "balance": bal}
        if price == 0:
            return {"ok": False, "error": "free", "balance": bal}
        if bal < price:
            return {"ok": False, "error": "insufficient", "balance": bal, "price": price}
        try:
            await self.tx([
                ("INSERT INTO user_subscriptions (telegram_id, package_id, price_paid) VALUES (?, ?, ?)",
                 [uid, pid, price]),
                ("INSERT INTO points_transactions (telegram_id, amount, type, ref_id, balance_after) "
                 "VALUES (?, ?, 'unlock_package', NULL, (SELECT points_balance FROM users WHERE telegram_id = ?) - ?)",
                 [uid, -price, uid, price]),
                ("UPDATE users SET points_balance = points_balance - ? WHERE telegram_id = ?", [price, uid]),
            ])
        except DBError as e:
            m = str(e).upper()
            if "UNIQUE" in m:
                return {"ok": False, "error": "owned", "balance": bal}
            if "CHECK" in m:
                return {"ok": False, "error": "insufficient", "balance": bal, "price": price}
            raise
        u2 = await self._user(uid)
        return {"ok": True, "balance": u2["points_balance"] if u2 else bal - price, "price": price}

    async def rpc_claim_channels_gift(self, a: dict) -> dict:
        uid, amount = a["p_user"], max(int(a.get("p_amount") or 0), 0)
        u = await self._user(uid)
        if not u:
            return {"granted": False, "balance": 0}
        if u["channels_gift_claimed"]:
            return {"granted": False, "balance": u["points_balance"]}
        try:
            await self.tx([
                ("INSERT INTO points_transactions (telegram_id, amount, type, ref_id, balance_after) "
                 "VALUES (?, ?, 'channels_gift', 'gift', (SELECT points_balance FROM users WHERE telegram_id = ?) + ?)",
                 [uid, amount, uid, amount]),
                ("UPDATE users SET channels_gift_claimed = 1, points_balance = points_balance + ? "
                 "WHERE telegram_id = ?", [amount, uid]),
            ])
        except DBError as e:
            if "UNIQUE" in str(e).upper():
                return {"granted": False, "balance": u["points_balance"]}
            raise
        u2 = await self._user(uid)
        return {"granted": True, "balance": u2["points_balance"] if u2 else u["points_balance"] + amount}

    async def rpc_credit_points(self, a: dict) -> dict:
        """Used by the payment bot after a successful Stars payment."""
        uid = a["p_user"]
        await self._run([("INSERT OR IGNORE INTO users (telegram_id) VALUES (?)", [uid])])
        r = await self._add_points(uid, int(a["p_points"]), "purchase_points",
                                   str(a["p_charge_id"]), f"stars={a.get('p_stars') or 0}")
        if r.get("ok"):
            await self._run([("UPDATE users SET is_premium = 1 WHERE telegram_id = ?", [uid])])
        return r

    async def rpc_user_stats(self, a: dict) -> dict:
        uid = a["p_user"]
        r = await self._q("SELECT COUNT(*) AS n FROM users WHERE referrer_id = ?", [uid])
        e = await self._q("SELECT COALESCE(SUM(amount), 0) AS s FROM points_transactions "
                          "WHERE telegram_id = ? AND type = 'referral'", [uid])
        return {"referrals": r[0]["n"], "earned": e[0]["s"]}

    # ------------------------------------------------------------- videos
    async def rpc_add_video(self, a: dict) -> dict:
        rows = (await self._run([(
            "INSERT INTO videos (package_id, title, channel_id, channel_message_id, telegram_file_id, "
            "file_unique_id, duration, file_size, mime_type, sort_order) "
            "SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(MAX(sort_order), 0) + 1 FROM videos WHERE package_id = ? "
            "ON CONFLICT (channel_id, channel_message_id) DO NOTHING RETURNING id",
            [a["p_package"], a.get("p_title") or None, a["p_channel"], a["p_mid"], a.get("p_file_id"),
             a.get("p_unique"), a.get("p_duration"), a.get("p_size") or 0, a.get("p_mime"), a["p_package"]])]))[0]["rows"]
        if not rows:
            return {"video_id": None, "duplicate": True}
        return {"video_id": rows[0]["id"], "duplicate": False, "needs_thumb": True}

    async def rpc_replace_video(self, a: dict) -> dict:
        async with self.lock:
            if await self._q("SELECT 1 AS x FROM videos WHERE channel_id = ? AND channel_message_id = ?",
                             [a["p_channel"], a["p_mid"]]):
                return {"video_id": None, "duplicate": True, "replaced": False}
            old = await self._q(
                "SELECT id, has_thumb FROM videos WHERE package_id = ? AND channel_id <> ? "
                "ORDER BY sort_order, id LIMIT 1", [a["p_package"], a["p_channel"]])
            if old:
                ov = old[0]
                await self._run([(
                    "UPDATE videos SET channel_id = ?, channel_message_id = ?, telegram_file_id = ?, "
                    "file_unique_id = ?, duration = COALESCE(?, duration), "
                    "file_size = CASE WHEN ? > 0 THEN ? ELSE file_size END, "
                    "mime_type = COALESCE(?, mime_type), is_broken = 0 WHERE id = ?",
                    [a["p_channel"], a["p_mid"], a.get("p_file_id"), a.get("p_unique"), a.get("p_duration"),
                     a.get("p_size") or 0, a.get("p_size") or 0, a.get("p_mime"), ov["id"]])])
                return {"video_id": ov["id"], "duplicate": False, "replaced": True,
                        "needs_thumb": not ov["has_thumb"]}
        r = await self.rpc_add_video(a)
        r["replaced"] = False
        return r

    async def rpc_move_video(self, a: dict) -> bool:
        pid, frm, to = a["p_package"], int(a["p_from"]), int(a["p_to"])
        async with self.lock:
            ids = [r["id"] for r in await self._q(
                "SELECT id FROM videos WHERE package_id = ? ORDER BY sort_order, id", [pid])]
            n = len(ids)
            if frm < 1 or frm > n or to < 1 or to > n:
                return False
            vid = ids.pop(frm - 1)
            ids.insert(to - 1, vid)
            case = " ".join("WHEN ? THEN ?" for _ in ids)
            args: list = []
            for pos, i in enumerate(ids, start=1):
                args += [i, pos]
            await self._run([(
                f"UPDATE videos SET sort_order = CASE id {case} END WHERE id IN ({','.join('?' * len(ids))})",
                args + ids)])
        return True

    async def _renumber(self, pid: int, order_sql: str) -> None:
        await self._run([(
            "UPDATE videos SET sort_order = (SELECT rn FROM (SELECT id AS vid, ROW_NUMBER() OVER "
            f"(ORDER BY {order_sql}) AS rn FROM videos WHERE package_id = ?) WHERE vid = videos.id) "
            "WHERE package_id = ?", [pid, pid])])

    async def rpc_renumber_videos(self, a: dict) -> None:
        async with self.lock:
            await self._renumber(a["p_package"], "sort_order, id")

    async def rpc_sort_videos_by_message(self, a: dict) -> None:
        async with self.lock:
            await self._renumber(a["p_package"], "channel_message_id, id")

    # ----------------------------------------------------------- Mini App
    @staticmethod
    def _cutoff(days: int) -> str:
        return iso(now_utc() - timedelta(days=max(int(days), 0)))

    async def _view(self, uid: int, kind: str, item: int) -> None:
        await self._run([(
            "INSERT INTO user_views (telegram_id, kind, item_id, viewed_at) VALUES (?, ?, ?, ?) "
            "ON CONFLICT (telegram_id, kind, item_id) DO UPDATE SET viewed_at = excluded.viewed_at",
            [uid, kind, item, iso_now()])])

    async def rpc_app_categories(self, a: dict) -> list:
        uid, cut = a["p_user"], self._cutoff(a.get("p_days") or 14)
        rows = await self._q(
            "SELECT c.id AS id, c.name AS name, "
            "(SELECT COUNT(*) FROM packages p WHERE p.category_id = c.id AND p.status = 'active') AS cnt, "
            "EXISTS (SELECT 1 FROM packages p WHERE p.category_id = c.id AND p.status = 'active' "
            "  AND p.published_at IS NOT NULL AND p.published_at > ? "
            "  AND p.published_at > COALESCE((SELECT w.viewed_at FROM user_views w "
            "     WHERE w.telegram_id = ? AND w.kind = 'category' AND w.item_id = c.id), '')) AS is_new "
            "FROM categories c WHERE c.is_visible = 1 ORDER BY c.sort_order, c.id", [cut, uid])
        return [{"id": r["id"], "name": r["name"], "count": r["cnt"], "is_new": bool(r["is_new"])} for r in rows]

    async def rpc_app_packages(self, a: dict) -> list:
        uid, cid, ch = a["p_user"], a["p_category"], a["p_channel"]
        cut = self._cutoff(a.get("p_days") or 14)
        rows = await self._q(
            "SELECT p.id AS id, p.title AS title, p.price_in_points AS price, "
            "(SELECT COUNT(*) FROM videos v WHERE v.package_id = p.id) AS videos, "
            "(SELECT COALESCE(SUM(v.file_size), 0) FROM videos v WHERE v.package_id = p.id) AS size, "
            "(SELECT v.id FROM videos v WHERE v.package_id = p.id AND v.has_thumb = 1 "
            "   ORDER BY v.sort_order, v.id LIMIT 1) AS cover_id, "
            "(p.price_in_points = 0 OR EXISTS (SELECT 1 FROM user_subscriptions s "
            "   WHERE s.telegram_id = ? AND s.package_id = p.id)) AS owned, "
            "(p.published_at IS NOT NULL AND p.published_at > ? AND NOT EXISTS (SELECT 1 FROM user_views w "
            "   WHERE w.telegram_id = ? AND w.kind = 'package' AND w.item_id = p.id)) AS is_new, "
            "EXISTS (SELECT 1 FROM videos v WHERE v.package_id = p.id AND v.channel_id <> ?) AS maintenance "
            "FROM packages p WHERE p.category_id = ? AND p.status = 'active' "
            "ORDER BY p.sort_order, p.published_at DESC, p.id", [uid, cut, uid, ch, cid])
        await self._view(uid, "category", cid)
        return [{"id": r["id"], "title": r["title"], "price": r["price"], "videos": r["videos"],
                 "size": r["size"], "cover": (f"/thumb/{r['cover_id']}.jpg" if r["cover_id"] else None),
                 "owned": bool(r["owned"]), "is_new": bool(r["is_new"]),
                 "maintenance": bool(r["maintenance"])} for r in rows]

    async def rpc_app_package(self, a: dict) -> Optional[dict]:
        uid, pid, ch = a["p_user"], a["p_package"], a["p_channel"]
        pk = (await self._q("SELECT * FROM packages WHERE id = ? AND status = 'active'", [pid]) or [None])[0]
        if not pk:
            return None
        owned = pk["price_in_points"] == 0 or bool(await self._q(
            "SELECT 1 AS x FROM user_subscriptions WHERE telegram_id = ? AND package_id = ?", [uid, pid]))
        vids = await self._q(
            "SELECT id, title, duration, file_size, has_thumb, channel_id FROM videos "
            "WHERE package_id = ? ORDER BY sort_order, id", [pid])
        maint = any(v["channel_id"] != ch for v in vids)
        await self._view(uid, "package", pid)
        return {
            "id": pk["id"], "title": pk["title"], "description": pk["description"],
            "price": pk["price_in_points"], "owned": owned, "allow_save": bool(pk["allow_save"]),
            "maintenance": maint,
            "videos": [{"id": v["id"], "n": i, "title": v["title"], "duration": v["duration"],
                        "size": v["file_size"],
                        "thumb": (f"/thumb/{v['id']}.jpg" if v["has_thumb"] else None)}
                       for i, v in enumerate(vids, start=1)],
        }

    # -------------------------------------------------------------- stats
    async def rpc_admin_stats(self, a: dict) -> dict:
        days = int(a.get("p_days") or 0)
        since = iso(now_utc() - timedelta(days=days)) if days > 0 else ""
        r = (await self._q(
            "SELECT (SELECT COUNT(*) FROM users) AS users_total, "
            "(SELECT COUNT(*) FROM users WHERE created_at >= ?1) AS users_new, "
            "(SELECT COUNT(*) FROM users WHERE last_active_at >= ?1) AS users_active, "
            "(SELECT COUNT(*) FROM users WHERE is_active = 0) AS blocked, "
            "(SELECT COUNT(*) FROM users WHERE is_banned = 1) AS banned, "
            "(SELECT COALESCE(SUM(points_balance), 0) FROM users) AS points_total, "
            "(SELECT COALESCE(SUM(amount), 0) FROM points_transactions "
            "   WHERE type = 'purchase_points' AND created_at >= ?1) AS points_bought, "
            "(SELECT COALESCE(-SUM(amount), 0) FROM points_transactions "
            "   WHERE type = 'unlock_package' AND created_at >= ?1) AS points_spent, "
            "(SELECT COUNT(*) FROM user_subscriptions WHERE unlocked_at >= ?1) AS unlocks, "
            "(SELECT COUNT(*) FROM users WHERE referrer_id IS NOT NULL AND created_at >= ?1) AS referrals, "
            "(SELECT COUNT(*) FROM packages WHERE status = 'active') AS packages_active", [since]))[0]
        r["top_packages"] = await self._q(
            "SELECT p.title AS title, COUNT(*) AS c FROM user_subscriptions s "
            "JOIN packages p ON p.id = s.package_id WHERE s.unlocked_at >= ? "
            "GROUP BY p.id, p.title ORDER BY c DESC LIMIT 3", [since])
        r["top_referrers"] = await self._q(
            "SELECT r.telegram_id AS id, COALESCE(r.username, r.full_name, CAST(r.telegram_id AS TEXT)) AS name, "
            "COUNT(*) AS c FROM users u JOIN users r ON r.telegram_id = u.referrer_id "
            "WHERE u.created_at >= ? GROUP BY r.telegram_id ORDER BY c DESC LIMIT 5", [since])
        return r

    async def close(self) -> None:
        await self.client.aclose()


db: DB = None  # type: ignore  (created in main)
bot: Bot = None  # type: ignore  (created in main)

# =====================================================================
#  runtime settings (stored in the `settings` table, editable from /admin)
# =====================================================================
SETTINGS: dict = {
    "referral_reward": "2",
    "channels_gift": "50",
    "new_badge_days": "14",
    "maintenance": "false",
    "referrals_enabled": "true",
    "gift_enabled": "true",
    "support_username": SUPPORT_USERNAME,   # default; the admin can change it from the settings panel
}


def support_user() -> str:
    return (SETTINGS.get("support_username") or "").strip().lstrip("@")


def sint(key: str) -> int:
    try:
        return int(SETTINGS.get(key, "0"))
    except ValueError:
        return 0


def sbool(key: str) -> bool:
    return str(SETTINGS.get(key, "false")).lower() == "true"


async def load_settings() -> None:
    try:
        rows = await db.select("settings")
        for r in rows:
            SETTINGS[r["key"]] = r["value"]
    except Exception as e:
        log.warning("load_settings failed: %s", e)


async def set_setting(key: str, value: str) -> None:
    await db.insert("settings", {"key": key, "value": value}, on_conflict="key", upsert=True)
    SETTINGS[key] = value


async def settings_loop() -> None:
    while True:
        await asyncio.sleep(60)
        await load_settings()


# =====================================================================
#  texts for users (English default + Arabic)
# =====================================================================
STR = {
    "en": {
        "sub_prompt": "🎁 Join our channels and get <b>{gift} free points</b>!\n\nJoin the channels below, then press “Verify ✅”, or skip for now.",
        "btn_verify": "Verify ✅",
        "btn_skip": "Skip ⏭",
        "sub_not_yet": "You haven't joined all the channels yet.",
        "sub_error": "Verification is unavailable right now, please try again later.",
        "gift_ok": "🎉 Congratulations! You received <b>{gift} points</b> 💧",
        "gift_done": "You already claimed this gift.",
        "main_prompt": "To use the bot, press the button below 👇🏻",
        "menu_hint": "Use the menu below 👇🏻",
        "btn_open": "Open Vidox 🎬",
        "no_webapp": "The app is not available right now, please try again later.",
        "kb_watch": "Watch videos 📽",
        "kb_buy": "Buy points 💦",
        "kb_earn": "Earn free points 🪄",
        "kb_balance": "Account balance 📊",
        "kb_lang": "Languages ℹ️",
        "kb_help": "Bot guide 💡",
        "consent_prompt": "🔞 <b>Before you continue</b>\n\nThis bot is for adults only. By continuing you confirm that:\n• You are <b>18 years old or older</b>.\n• You have read and agree to the Terms (/terms) and the Privacy Policy (/privacy).\n• You are responsible for how you use the content.",
        "btn_consent_yes": "I'm 18+ and I agree ✅",
        "btn_consent_no": "I'm under 18 ❌",
        "consent_no": "Sorry, you can't use this bot. 🚫",
        "terms_text": "📜 <b>Terms of Use</b>\n\n1) Adults only (18+).\n2) Points are a digital good for your account; they are not transferable and not redeemable for cash.\n3) A package you unlock stays yours; videos are delivered to your chat with save protection where enabled.\n4) Do not share, resell or republish the content.\n5) We may suspend accounts that abuse the service, spam, or break these terms.\n6) Service is provided \"as is\" and may change at any time.\n7) For any problem contact /support.",
        "privacy_text": "🔒 <b>Privacy Policy</b>\n\nWe store only what the service needs: your Telegram id, name/username, language, points balance, purchases and referral data.\nWe do not sell your data and never ask for your Telegram password or login codes.\nPayments are made in Telegram Stars; we do not see your payment details.\nTo delete your data contact /support.",
        "support_text": "🛟 <b>Support</b>\n\nNeed help? Contact our support team 👇🏻",
        "support_none": "Support is not available right now.",
        "kb_code": "Use a code 🎟",
        "code_prompt": "🎟 Send your code now.\n(Send /start to cancel)",
        "code_ok": "🎉 Congratulations! You received <b>{pts} points</b> 💦\nYour balance: <b>{bal}</b>",
        "code_invalid": "❌ This code is not valid.",
        "code_expired": "⌛ This code has expired.",
        "code_used": "⚠️ You already used this code.",
        "code_exhausted": "😔 This code reached its maximum number of users.",
        "code_blocked": "⏳ Too many wrong attempts. Try again in a few minutes.",
        "watch_text": "Browse and watch videos from the button below 👇🏻",
        "buy_text": "You can buy points through our payment bot 👇🏻\n\nWhen you buy any package you get <b>lifetime perks</b> 🎉\n\n🚀 Faster access and smoother browsing\n🛟 Priority support\n🔔 Early access to new packages\n\nPress the button below to see the packages 👇🏻",
        "btn_buy": "Buy points 💦",
        "buy_soon": "The payment bot is coming soon.",
        "earn_text": "Earn <b>free points</b> 💦 by sharing your link with friends. You get <b>{reward} points</b> 💧 for every new member.\n\nCopy the link below or share it with the button 👇🏻\n\n<code>{link}</code>",
        "btn_share": "Share 📤",
        "share_text": "🔥 This bot has packages with thousands of videos — watch now!",
        "btn_claim": "🎁 Get {gift} points",
        "ref_joined": "🎉 A new member joined through your link. You received <b>{reward} points</b> 💧\nBalance: <b>{balance}</b>",
        "balance_text": "<b>Account balance 📊</b>\n━━━━━━━━━━\nBalance: <b>{balance}</b> points 💧\n━━━━━━━━━━\n<b>Your stats 🔰</b>\nMembers invited: <b>{refs}</b>\nTotal earnings: <b>{earned}</b> points 💧\nYour ID: <code>{uid}</code>\n━━━━━━━━━━\nPoints never expire 🎉",
        "lang_prompt": "Choose your language | اختر لغتك 👇🏻",
        "lang_changed": "✅ Language updated",
        "help_text": "💡 <b>How to use Vidox</b>\n\n1) Press “Watch videos” to open the app and browse the sections.\n2) Open any section to see its packages, with the number of videos and size under each one.\n3) Unlock a package with your points — it stays yours forever.\n4) Get points from “Buy points” or earn them free from “Earn free points”.\n5) Videos are sent to this chat so you can watch and save them.",
        "btn_support": "🛟 Support",
        "banned": "You are not allowed to use this bot.",
        "maint": "🛠 The bot is under maintenance, please try again soon.",
        "pts_added": "✅ {n} points were added to your balance.\nBalance: <b>{balance}</b>",
        "pts_removed": "ℹ️ {n} points were deducted from your balance.\nBalance: <b>{balance}</b>",
        "pkg_granted": "🎁 You were given access to the package: <b>{title}</b>",
    },
    "ar": {
        "sub_prompt": "🎁 انضم إلى قنواتنا واحصل على <b>{gift} نقطة مجانية</b>!\n\nانضم إلى القنوات أدناه ثم اضغط «تحقق ✅»، أو تجاوز الآن.",
        "btn_verify": "تحقق ✅",
        "btn_skip": "تجاوز ⏭",
        "sub_not_yet": "لم تنضم إلى كل القنوات بعد.",
        "sub_error": "التحقق غير متاح حالياً، حاول مرة أخرى لاحقاً.",
        "gift_ok": "🎉 مبروك! حصلت على <b>{gift} نقطة</b> 💧",
        "gift_done": "لقد استلمت هذه الهدية من قبل.",
        "main_prompt": "لاستخدام البوت اضغط الزر بالأسفل 👇🏻",
        "menu_hint": "استخدم القائمة بالأسفل 👇🏻",
        "btn_open": "فتح Vidox 🎬",
        "no_webapp": "التطبيق غير متاح حالياً، حاول مرة أخرى لاحقاً.",
        "kb_watch": "شاهد المقاطع 📽",
        "kb_buy": "شراء نقاط 💦",
        "kb_earn": "إربح نقاط مجاناً 🪄",
        "kb_balance": "رصيد الحساب 📊",
        "kb_lang": "اللغات ℹ️",
        "kb_help": "تعليمات البوت 💡",
        "consent_prompt": "🔞 <b>قبل أن تتابع</b>\n\nهذا البوت للبالغين فقط. بالمتابعة فأنت تؤكد أن:\n• عمرك <b>18 سنة أو أكثر</b>.\n• قرأت وتوافق على الشروط (/terms) وسياسة الخصوصية (/privacy).\n• أنت المسؤول عن طريقة استخدامك للمحتوى.",
        "btn_consent_yes": "عمري 18+ وأوافق ✅",
        "btn_consent_no": "عمري أقل من 18 ❌",
        "consent_no": "عذراً، لا يمكنك استخدام هذا البوت. 🚫",
        "terms_text": "📜 <b>شروط الاستخدام</b>\n\n1) للبالغين فقط (18+).\n2) النقاط سلعة رقمية لحسابك، لا تُحوَّل ولا تُستبدل بمال.\n3) الحزمة التي تفتحها تبقى لك، وتصلك المقاطع في محادثتك مع حماية الحفظ حيث تكون مفعّلة.\n4) يُمنع مشاركة المحتوى أو إعادة بيعه أو نشره.\n5) يحق لنا إيقاف الحسابات التي تسيء للخدمة أو ترسل سبام أو تخالف الشروط.\n6) الخدمة تُقدَّم كما هي وقد تتغير في أي وقت.\n7) لأي مشكلة تواصل معنا عبر /support.",
        "privacy_text": "🔒 <b>سياسة الخصوصية</b>\n\nنحفظ فقط ما تحتاجه الخدمة: آيدي تيليجرام والاسم/اليوزر واللغة ورصيد النقاط والمشتريات وبيانات الإحالة.\nلا نبيع بياناتك، ولا نطلب منك أبداً كلمة مرور تيليجرام أو أكواد الدخول.\nالدفع بنجوم تيليجرام ولا نطّلع على بيانات دفعك.\nلحذف بياناتك تواصل معنا عبر /support.",
        "support_text": "🛟 <b>الدعم</b>\n\nتحتاج مساعدة؟ تواصل مع فريق الدعم 👇🏻",
        "support_none": "الدعم غير متاح حالياً.",
        "kb_code": "إستخدم كود 🎟",
        "code_prompt": "🎟 أرسل الكود الآن.\n(أرسل /start للإلغاء)",
        "code_ok": "🎉 مبروك! لقد استلمت <b>{pts} نقطة</b> 💦\nرصيدك الآن: <b>{bal}</b>",
        "code_invalid": "❌ الكود غير صحيح.",
        "code_expired": "⌛ انتهت صلاحية هذا الكود.",
        "code_used": "⚠️ لقد استخدمت هذا الكود من قبل.",
        "code_exhausted": "😔 اكتمل عدد المستخدمين لهذا الكود.",
        "code_blocked": "⏳ محاولات خاطئة كثيرة. حاول بعد دقائق.",
        "watch_text": "شاهد وتصفّح المقاطع من الزر أدناه 👇🏻",
        "buy_text": "يمكنك شراء النقاط من خلال بوت الدفع الخاص بنا 👇🏻\n\nعند شرائك لأي باقة ستحصل على <b>مزايا دائمة</b> 🎉\n\n🚀 وصول أسرع وتصفح سلس\n🛟 أولوية في الدعم\n🔔 أولوية الوصول للحزم الجديدة\n\nلمعرفة تفاصيل الباقات اضغط على الزر بالأسفل 👇🏻",
        "btn_buy": "شراء نقاط 💦",
        "buy_soon": "بوت الدفع قريباً.",
        "earn_text": "اربح <b>نقاط مجانية</b> 💦 من خلال مشاركة الرابط مع أصدقائك. لكل عضو جديد تحصل على <b>{reward} نقطة</b> 💧\n\nانسخ الرابط أدناه أو شاركه من الزر بالأسفل 👇🏻\n\n<code>{link}</code>",
        "btn_share": "مشاركة 📤",
        "share_text": "🔥 هذا البوت يحتوي على حزم بآلاف المقاطع، شاهد الآن!",
        "btn_claim": "🎁 احصل على {gift} نقطة",
        "ref_joined": "🎉 انضم عضو جديد عبر رابطك، حصلت على <b>{reward} نقطة</b> 💧\nرصيدك: <b>{balance}</b>",
        "balance_text": "<b>رصيد حسابك 📊</b>\n━━━━━━━━━━\nالرصيد: <b>{balance}</b> نقطة 💧\n━━━━━━━━━━\n<b>إحصائياتك الشخصية 🔰</b>\nعدد أعضائك: <b>{refs}</b>\nإجمالي أرباحك: <b>{earned}</b> نقطة 💧\nالـ ID الخاص بحسابك: <code>{uid}</code>\n━━━━━━━━━━\nالنقاط صالحة بدون تاريخ انتهاء 🎉",
        "lang_prompt": "Choose your language | اختر لغتك 👇🏻",
        "lang_changed": "✅ تم تغيير اللغة",
        "help_text": "💡 <b>كيف تستخدم Vidox؟</b>\n\n1) اضغط «شاهد المقاطع» لفتح التطبيق وتصفح الأقسام.\n2) ادخل أي قسم لترى الحزم، وتحت كل حزمة عدد المقاطع وحجمها.\n3) افتح الحزمة بنقاطك وستبقى لك للأبد.\n4) احصل على النقاط من «شراء نقاط» أو اكسبها مجاناً من «إربح نقاط».\n5) تصلك المقاطع في هذه المحادثة لتشاهدها وتحفظها.",
        "btn_support": "🛟 الدعم",
        "banned": "غير مسموح لك باستخدام هذا البوت.",
        "maint": "🛠 البوت تحت الصيانة، حاول مرة أخرى بعد قليل.",
        "pts_added": "✅ تمت إضافة {n} نقطة إلى رصيدك.\nرصيدك: <b>{balance}</b>",
        "pts_removed": "ℹ️ تم خصم {n} نقطة من رصيدك.\nرصيدك: <b>{balance}</b>",
        "pkg_granted": "🎁 تم منحك حزمة: <b>{title}</b>",
    },
}


def t(lang: Optional[str], key: str, **kw: Any) -> str:
    s = STR.get(lang or "en", STR["en"]).get(key) or STR["en"][key]
    return s.format(**kw) if kw else s


KB_KEYS = ["kb_watch", "kb_buy", "kb_earn", "kb_balance", "kb_lang", "kb_help", "kb_code"]
KB_MAP: dict = {}
for _lg in STR:
    for _k in KB_KEYS:
        KB_MAP[STR[_lg][_k]] = _k
KB_TEXTS = set(KB_MAP)


# =====================================================================
#  messaging helpers
# =====================================================================
router = Router()

_alert_ts: dict = {}


async def safe_send(chat_id: int, text: str, **kw: Any) -> Optional[Message]:
    """Send a message; never raises. Marks users who blocked the bot as inactive."""
    for attempt in range(2):
        try:
            return await bot.send_message(chat_id, text, **kw)
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 0.5)
        except TelegramForbiddenError:
            if chat_id > 0 and chat_id not in ADMIN_IDS:
                try:
                    await db.update("users", {"telegram_id": f"eq.{chat_id}"}, {"is_active": False})
                except Exception:
                    pass
            return None
        except TelegramAPIError as e:
            log.warning("send_message to %s failed: %s", chat_id, e)
            return None
    return None


async def alert_admins(text: str, key: Optional[str] = None, every: int = 600) -> None:
    if key:
        if time.time() - _alert_ts.get(key, 0) < every:
            return
        _alert_ts[key] = time.time()
    for aid in ADMIN_IDS:
        await safe_send(aid, text)


# =====================================================================
#  user cache + registration + gate
# =====================================================================
_ucache: dict = {}
_last_touch: dict = {}


async def get_user(uid: int, fresh: bool = False) -> Optional[dict]:
    c = _ucache.get(uid)
    if c and not fresh and time.time() - c[0] < 30:
        return c[1]
    u = await db.one("users", {"telegram_id": f"eq.{uid}"})
    if u:
        _ucache[uid] = (time.time(), u)
        if len(_ucache) > 20000:
            _ucache.clear()
    return u


def drop_user(uid: int) -> None:
    _ucache.pop(uid, None)


async def register(fu: Any, ref: Optional[int] = None) -> dict:
    reward = sint("referral_reward") if (ref and sbool("referrals_enabled")) else 0
    res = await db.rpc("register_user", {
        "p_id": fu.id,
        "p_username": fu.username,
        "p_name": (fu.full_name or "")[:100],
        "p_lang": "en",
        "p_ref": ref,
        "p_reward": reward,
    })
    _ucache[fu.id] = (time.time(), res["user"])
    if res.get("ref_id"):
        drop_user(res["ref_id"])
    return res


async def _touch(fu: Any) -> None:
    try:
        await db.update("users", {"telegram_id": f"eq.{fu.id}"}, {
            "last_active_at": iso_now(), "is_active": True,
            "username": fu.username, "full_name": (fu.full_name or "")[:100],
        })
    except Exception as e:
        log.debug("touch failed: %s", e)


async def _deny(ev: Any, text: str) -> None:
    try:
        if isinstance(ev, CallbackQuery):
            await ev.answer(text, show_alert=True)
        else:
            await ev.answer(text)
    except Exception:
        pass


async def gate(ev: Any) -> Optional[dict]:
    """Returns the user row, or None when the update must be ignored (banned / maintenance)."""
    fu = ev.from_user
    if fu is None:
        return None
    u = await get_user(fu.id)
    if u is None:
        u = (await register(fu))["user"]
    is_admin = fu.id in ADMIN_IDS
    if u.get("is_banned") and not is_admin:
        await _deny(ev, t(u.get("language"), "banned"))
        return None
    if sbool("maintenance") and not is_admin:
        await _deny(ev, t(u.get("language"), "maint"))
        return None
    now = time.time()
    if now - _last_touch.get(fu.id, 0) > 300:
        _last_touch[fu.id] = now
        asyncio.create_task(_touch(fu))
    return u


class Throttle(BaseMiddleware):
    """Simple flood control: RATE_MAX events per RATE_WINDOW seconds per user."""

    def __init__(self) -> None:
        self.hits: dict = {}

    async def __call__(self, handler, event, data):
        fu = getattr(event, "from_user", None)
        if fu is not None and fu.id not in ADMIN_IDS:
            now = time.monotonic()
            q = self.hits.setdefault(fu.id, deque())
            while q and now - q[0] > RATE_WINDOW:
                q.popleft()
            if len(q) >= RATE_MAX:
                if isinstance(event, CallbackQuery):
                    try:
                        await event.answer()
                    except Exception:
                        pass
                return None
            q.append(now)
            if len(self.hits) > 20000:
                self.hits.clear()
        return await handler(event, data)


# =====================================================================
#  keyboards
# =====================================================================
def main_kb(lang: str) -> ReplyKeyboardMarkup:
    def b(k: str) -> KeyboardButton:
        return KeyboardButton(text=t(lang, k))

    rows = [
        [b("kb_watch")],
        [b("kb_earn"), b("kb_buy")],
        [b("kb_balance"), b("kb_code")],
        [b("kb_lang"), b("kb_help")],
    ]
    if lang == "ar" and KEYBOARD_MIRROR_AR:
        rows = [list(reversed(r)) for r in rows]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, is_persistent=True)


def open_app_markup(lang: str) -> Optional[InlineKeyboardMarkup]:
    if not WEBAPP_URL:
        return None
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=t(lang, "btn_open"), web_app=WebAppInfo(url=WEBAPP_URL))
    ]])


def lang_markup(ctx: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="English 🇺🇸", callback_data=f"u:lang:en:{ctx}")],
        [InlineKeyboardButton(text="العربية 🇾🇪", callback_data=f"u:lang:ar:{ctx}")],
    ])


def sub_markup(lang: str, gift: bool = False) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=ch["title"], url=ch["url"])] for ch in SUB_CHANNELS]
    rows.append([
        InlineKeyboardButton(text=t(lang, "btn_verify"), callback_data="u:vgift" if gift else "u:verify"),
        InlineKeyboardButton(text=t(lang, "btn_skip"), callback_data="u:close" if gift else "u:skip"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# =====================================================================
#  onboarding flow
# =====================================================================
async def send_lang_prompt(chat_id: int, ctx: str = "start") -> None:
    await safe_send(chat_id, "Choose your language: 👇🏻" if ctx == "start" else t("en", "lang_prompt"),
                    reply_markup=lang_markup(ctx))


async def send_sub_prompt(chat_id: int, u: dict) -> None:
    lang = u.get("language", "en")
    await safe_send(chat_id, t(lang, "sub_prompt", gift=sint("channels_gift")),
                    reply_markup=sub_markup(lang))


async def send_main(chat_id: int, u: dict) -> None:
    lang = u.get("language", "en")
    markup = open_app_markup(lang)
    if markup:
        await safe_send(chat_id, t(lang, "main_prompt"), reply_markup=markup)
    else:
        await safe_send(chat_id, t(lang, "no_webapp"))
    await safe_send(chat_id, t(lang, "menu_hint"), reply_markup=main_kb(lang))


async def go_onboarding(chat_id: int, u: dict) -> None:
    if not u.get("lang_set"):
        await send_lang_prompt(chat_id, "start")
        return
    if not u.get("onboarded"):
        if not await db.one("user_consent", {"telegram_id": f"eq.{chat_id}"}, "telegram_id"):
            lang = u.get("language", "en")
            await safe_send(chat_id, t(lang, "consent_prompt"), reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=t(lang, "btn_consent_yes"), callback_data="u:consent:yes")],
                [InlineKeyboardButton(text=t(lang, "btn_consent_no"), callback_data="u:consent:no")]]))
            return
        if sbool("gift_enabled"):
            await send_sub_prompt(chat_id, u)
            return
        await db.update("users", {"telegram_id": f"eq.{chat_id}"}, {"onboarded": True})
        drop_user(chat_id)
        u = await get_user(chat_id, fresh=True) or u
    await send_main(chat_id, u)


async def check_membership(uid: int) -> tuple:
    """returns (all_joined, verification_error)"""
    ok, err = True, False
    for ch in SUB_CHANNELS:
        try:
            mem = await bot.get_chat_member(ch["chat"], uid)
            st = getattr(mem.status, "value", mem.status)
            if st in ("left", "kicked") or (st == "restricted" and not getattr(mem, "is_member", True)):
                ok = False
        except TelegramAPIError as e:
            err = True
            log.warning("get_chat_member %s failed: %s", ch["chat"], e)
            await alert_admins(
                f"⚠️ تعذر التحقق من الاشتراك في {ch['chat']}.\nتأكد أن البوت مشرف في القناة.\n<code>{esc(str(e))[:200]}</code>",
                key=f"sub:{ch['chat']}",
            )
    return (ok and not err), err


@router.message(CommandStart(), F.chat.type == "private")
async def cmd_start(m: Message, command: CommandObject) -> None:
    if not m.from_user:
        return
    _code_wait.pop(m.from_user.id, None)
    ref = None
    mm = re.match(r"^ref_(\d{3,15})$", (command.args or "").strip())
    if mm:
        ref = int(mm.group(1))
    res = await register(m.from_user, ref)
    u = res["user"]
    if res.get("ref_credited") and res.get("ref_id"):
        ru = await get_user(res["ref_id"], fresh=True)
        await safe_send(res["ref_id"], t((ru or {}).get("language"), "ref_joined",
                                         reward=sint("referral_reward"),
                                         balance=res.get("ref_balance") or 0))
    is_admin = m.from_user.id in ADMIN_IDS
    if is_admin:
        asyncio.create_task(set_admin_commands(m.from_user.id))
    if u.get("is_banned") and not is_admin:
        await m.answer(t(u.get("language"), "banned"))
        return
    if sbool("maintenance") and not is_admin:
        await m.answer(t(u.get("language"), "maint"))
        return
    await go_onboarding(m.chat.id, u)


async def _delete(cb: CallbackQuery) -> None:
    try:
        await cb.message.delete()
    except Exception:
        pass


async def do_verify(cb: CallbackQuery, u: dict, onboarding: bool) -> None:
    lang = u.get("language", "en")
    uid = cb.from_user.id
    if not sbool("gift_enabled"):
        await cb.answer()
        return
    ok, err = await check_membership(uid)
    if err:
        await cb.answer(t(lang, "sub_error"), show_alert=True)
        return
    if not ok:
        await cb.answer(t(lang, "sub_not_yet"), show_alert=True)
        return
    res = await db.rpc("claim_channels_gift", {"p_user": uid, "p_amount": sint("channels_gift")})
    if onboarding:
        await db.update("users", {"telegram_id": f"eq.{uid}"}, {"onboarded": True})
    drop_user(uid)
    await cb.answer()
    await _delete(cb)
    if res and res.get("granted"):
        await safe_send(uid, t(lang, "gift_ok", gift=sint("channels_gift")))
    elif not onboarding:
        await safe_send(uid, t(lang, "gift_done"))
    if onboarding:
        await send_main(uid, (await get_user(uid, fresh=True)) or u)


@router.callback_query(F.data.startswith("u:"))
async def cb_user(cb: CallbackQuery) -> None:
    u = await gate(cb)
    if not u:
        return
    uid = cb.from_user.id
    parts = (cb.data or "").split(":")
    action = parts[1] if len(parts) > 1 else ""

    if action == "consent":
        await cb.answer()
        if len(parts) > 2 and parts[2] == "yes":
            try:
                await db.insert("user_consent", {"telegram_id": uid}, on_conflict="telegram_id", ignore_duplicates=True)
            except DBError as e:
                log.warning("consent save failed: %s", e)
                return
            await _delete(cb)
            await go_onboarding(uid, (await get_user(uid, fresh=True)) or u)
        else:
            await _delete(cb)
            await safe_send(uid, t(u.get("language"), "consent_no"))
        return

    if action == "lang" and len(parts) >= 4:
        code, ctx = parts[2], parts[3]
        if code not in ("en", "ar"):
            await cb.answer()
            return
        await db.update("users", {"telegram_id": f"eq.{uid}"}, {"language": code, "lang_set": True})
        drop_user(uid)
        u = await get_user(uid, fresh=True) or u
        await _delete(cb)
        await cb.answer()
        if ctx == "start":
            await go_onboarding(uid, u)
        else:
            await safe_send(uid, t(code, "lang_changed"), reply_markup=main_kb(code))
        return

    if action == "verify":
        await do_verify(cb, u, onboarding=True)
        return

    if action == "gift":
        # "Get N points" button: re-open the channels message (join links + verify) instead of verifying blindly
        await cb.answer()
        fresh = await get_user(uid, fresh=True) or u
        lang = fresh.get("language", "en")
        if not sbool("gift_enabled") or fresh.get("channels_gift_claimed"):
            await safe_send(uid, t(lang, "gift_done"))
        else:
            await safe_send(uid, t(lang, "sub_prompt", gift=sint("channels_gift")),
                            reply_markup=sub_markup(lang, gift=True))
        return

    if action == "vgift":
        await do_verify(cb, u, onboarding=False)
        return

    if action == "close":
        await _delete(cb)
        await cb.answer()
        return

    if action == "skip":
        await db.update("users", {"telegram_id": f"eq.{uid}"}, {"onboarded": True})
        drop_user(uid)
        await _delete(cb)
        await cb.answer()
        await send_main(uid, (await get_user(uid, fresh=True)) or u)
        return

    if action == "soon":
        await cb.answer(t(u.get("language"), "buy_soon"), show_alert=True)
        return

    await cb.answer()


# =====================================================================
#  the six reply-keyboard buttons
# =====================================================================
_code_wait: dict = {}      # uid -> time the user pressed "Use a code"
_code_fails: dict = {}     # uid -> [timestamps of wrong attempts]
CODE_WAIT_SECS = 300


def _code_blocked(uid: int) -> bool:
    now = time.time()
    fl = [x for x in _code_fails.get(uid, []) if now - x < 600]
    _code_fails[uid] = fl
    return len(fl) >= 5


class CodeInputFilter(Filter):
    async def __call__(self, message: Message) -> bool:
        uid = message.from_user.id if message.from_user else 0
        ts = _code_wait.get(uid)
        if ts is None:
            return False
        if time.time() - ts > CODE_WAIT_SECS:
            _code_wait.pop(uid, None)
            return False
        txt = message.text or ""
        if txt.startswith("/") or txt in KB_TEXTS:
            return False
        if uid in ADMIN_IDS and await get_astate(uid):
            return False
        return True


@router.message(F.chat.type == "private", F.text, CodeInputFilter())
async def on_code_input(m: Message) -> None:
    u = await gate(m)
    if not u:
        return
    uid = m.from_user.id
    lang = u.get("language", "en")
    if _code_blocked(uid):
        await m.answer(t(lang, "code_blocked"))
        return
    code = re.sub(r"\s+", "", (m.text or "")).upper()[:40]
    res = await db.rpc("redeem_code", {"p_user": uid, "p_code": code}) if code else {"ok": False, "error": "invalid"}
    if res.get("ok"):
        _code_wait.pop(uid, None)
        drop_user(uid)
        await m.answer(t(lang, "code_ok", pts=res["points"], bal=res["balance"]))
        return
    err = res.get("error", "invalid")
    if err == "invalid":
        _code_fails.setdefault(uid, []).append(time.time())
    else:
        _code_wait.pop(uid, None)
    key = {"invalid": "code_invalid", "expired": "code_expired", "used": "code_used",
           "exhausted": "code_exhausted"}.get(err, "code_invalid")
    await m.answer(t(lang, key))


@router.message(F.chat.type == "private", F.text.in_(KB_TEXTS))
async def on_keyboard_button(m: Message) -> None:
    u = await gate(m)
    if not u:
        return
    key = KB_MAP.get(m.text or "")
    lang = u.get("language", "en")
    uid = m.from_user.id
    _code_wait.pop(uid, None)

    if key == "kb_code":
        if _code_blocked(uid):
            await m.answer(t(lang, "code_blocked"))
        else:
            _code_wait[uid] = time.time()
            await m.answer(t(lang, "code_prompt"))

    elif key == "kb_watch":
        markup = open_app_markup(lang)
        await m.answer(t(lang, "watch_text") if markup else t(lang, "no_webapp"), reply_markup=markup)

    elif key == "kb_buy":
        if PAYMENT_BOT_USERNAME:
            btn = InlineKeyboardButton(
                text=t(lang, "btn_buy"),
                url=f"https://t.me/{PAYMENT_BOT_USERNAME}?start=from_vidox")
        else:
            btn = InlineKeyboardButton(text=t(lang, "btn_buy"), callback_data="u:soon")
        await m.answer(t(lang, "buy_text"), reply_markup=InlineKeyboardMarkup(inline_keyboard=[[btn]]))

    elif key == "kb_earn":
        link = f"https://t.me/{BOT_USERNAME}?start=ref_{uid}"
        share = f"https://t.me/share/url?url={quote(link)}&text={quote(t(lang, 'share_text'))}"
        rows = [[InlineKeyboardButton(text=t(lang, "btn_share"), url=share)]]
        fresh = await get_user(uid, fresh=True) or u
        if sbool("gift_enabled") and not fresh.get("channels_gift_claimed"):
            rows.append([InlineKeyboardButton(
                text=t(lang, "btn_claim", gift=sint("channels_gift")), callback_data="u:gift")])
        await m.answer(t(lang, "earn_text", reward=sint("referral_reward"), link=link),
                       reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

    elif key == "kb_balance":
        fresh = await get_user(uid, fresh=True) or u
        st = await db.rpc("user_stats", {"p_user": uid}) or {}
        await m.answer(t(lang, "balance_text", balance=fresh.get("points_balance", 0),
                         refs=st.get("referrals", 0), earned=st.get("earned", 0), uid=uid))

    elif key == "kb_lang":
        await m.answer(t(lang, "lang_prompt"), reply_markup=lang_markup("menu"))

    elif key == "kb_help":
        markup = None
        if support_user():
            markup = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text=t(lang, "btn_support"),
                                     url=f"https://t.me/{support_user()}")]])
        await m.answer(t(lang, "help_text"), reply_markup=markup)


@router.my_chat_member()
async def on_my_chat_member(ev: ChatMemberUpdated) -> None:
    """Tracks users who block / unblock the bot (keeps the stats accurate)."""
    if ev.chat.type != "private":
        return
    st = getattr(ev.new_chat_member.status, "value", ev.new_chat_member.status)
    try:
        await db.update("users", {"telegram_id": f"eq.{ev.chat.id}"},
                        {"is_active": st not in ("kicked", "left")})
        drop_user(ev.chat.id)
    except Exception as e:
        log.debug("my_chat_member update failed: %s", e)


# =====================================================================
#  ADMIN: helpers
# =====================================================================
BOT_ID = 0
thumb_queue: Optional[asyncio.Queue] = None


def ab(text: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, callback_data=data)


def akb(rows: list) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=rows)


BACK_HOME = [ab("🏠 اللوحة الرئيسية", "a:home")]
STATUS_ICON = {"active": "🟢", "draft": "🟡", "hidden": "🔴", "deleted": "⚫"}
STATUS_NAME = {"active": "نشطة", "draft": "قيد الإنشاء", "hidden": "مخفية", "deleted": "محذوفة"}


async def log_admin(aid: Optional[int], action: str, details: str = "") -> None:
    try:
        await db.insert("admin_logs", {"admin_id": aid, "action": action, "details": details[:500]})
    except Exception as e:
        log.debug("admin log failed: %s", e)


async def get_astate(aid: int) -> Optional[dict]:
    return await db.one("admin_state", {"admin_id": f"eq.{aid}"})


async def set_astate(aid: int, state: str, data: Optional[dict] = None) -> None:
    await db.insert("admin_state", {"admin_id": aid, "state": state, "data": data or {},
                                    "updated_at": iso_now()}, on_conflict="admin_id", upsert=True)


async def clear_astate(aid: int) -> None:
    try:
        await db.delete("admin_state", {"admin_id": f"eq.{aid}"})
    except Exception as e:
        log.debug("clear state failed: %s", e)


async def show(ev: Any, text: str, markup: Optional[InlineKeyboardMarkup] = None) -> None:
    """Edit the panel message when called from a button, otherwise send a new message."""
    if isinstance(ev, CallbackQuery):
        try:
            await ev.message.edit_text(text, reply_markup=markup)
            return
        except TelegramBadRequest as e:
            if "not modified" in str(e).lower():
                return
            await bot.send_message(ev.message.chat.id, text, reply_markup=markup)
            return
    await ev.answer(text, reply_markup=markup)


async def reupload_pkg_ids() -> set:
    rows = await db.select("videos", "package_id",
                           {"channel_id": f"neq.{STORAGE_CHANNEL_ID}"}, limit=5000)
    return {r["package_id"] for r in rows}


_chan_cache = [0.0, False]


async def channel_ok() -> bool:
    if time.time() - _chan_cache[0] < 60:
        return _chan_cache[1]
    ok = False
    try:
        mem = await bot.get_chat_member(STORAGE_CHANNEL_ID, BOT_ID)
        ok = getattr(mem.status, "value", mem.status) in ("administrator", "creator")
    except TelegramAPIError as e:
        log.warning("storage channel check failed: %s", e)
    _chan_cache[0], _chan_cache[1] = time.time(), ok
    return ok


# =====================================================================
#  ADMIN: main panel
# =====================================================================
async def panel_home(ev: Any) -> None:
    day_ago = iso(now_utc() - timedelta(hours=24))
    total = await db.count("users")
    new24 = await db.count("users", {"created_at": f"gte.{day_ago}"})
    active_pk = await db.count("packages", {"status": "eq.active"})
    re_ids = await reupload_pkg_ids()
    ok = await channel_ok()
    sess = await db.one("upload_sessions", {"status": "eq.open"})
    lines = [
        "🎛 <b>لوحة تحكم Vidox</b>",
        "━━━━━━━━━━",
        f"👥 المستخدمون: <b>{total}</b> (+{new24} آخر 24 ساعة)",
        f"📦 الحزم النشطة: <b>{active_pk}</b>",
        "📡 قناة التخزين: " + ("✅ متصلة" if ok else "⚠️ البوت ليس مشرفاً فيها أو غير متاحة"),
        f"‼️ حزم تحتاج إعادة رفع: <b>{len(re_ids)}</b>",
    ]
    if sess:
        lines.append("📥 توجد جلسة رفع مفتوحة الآن")
    if sbool("maintenance"):
        lines.append("🛠 وضع الصيانة: <b>مفعّل</b>")
    kb = akb([
        [ab("📦 الحزم", "a:pk:list:0"), ab("➕ حزمة جديدة", "a:np")],
        [ab("🗂 الأقسام والأزرار", "a:ct:list"), ab("👤 إدارة مستخدم", "a:us:find")],
        [ab("📊 الإحصائيات", "a:st:7"), ab("📢 إذاعة", "a:bc:start")],
        [ab("🎟 أكواد النقاط", "a:cd:list"), ab("⚙️ الإعدادات", "a:se")],
        [ab("🧾 سجل العمليات", "a:lg")],
    ])
    await show(ev, "\n".join(lines), kb)


# ---------------------------------------------------------------- commands
def _is_admin_msg(m: Message) -> bool:
    return bool(m.from_user and m.from_user.id in ADMIN_IDS and m.chat.type == "private")


ADMIN_COMMANDS = [
    BotCommand(command="admin", description="🎛 لوحة التحكم"),
    BotCommand(command="new_package", description="➕ حزمة جديدة"),
    BotCommand(command="stats", description="📊 الإحصائيات"),
    BotCommand(command="broadcast", description="📢 إذاعة"),
    BotCommand(command="stopbc", description="⛔ إيقاف الإذاعة"),
    BotCommand(command="user", description="👤 إدارة مستخدم"),
    BotCommand(command="cancel", description="❌ إلغاء العملية"),
    BotCommand(command="start", description="🔄 القائمة الرئيسية"),
    BotCommand(command="terms", description="📜 الشروط"),
    BotCommand(command="privacy", description="🔒 الخصوصية"),
    BotCommand(command="support", description="🛟 الدعم"),
]
USER_COMMANDS = [
    BotCommand(command="start", description="Start | ابدأ"),
    BotCommand(command="terms", description="Terms | الشروط"),
    BotCommand(command="privacy", description="Privacy | الخصوصية"),
    BotCommand(command="support", description="Support | الدعم"),
]


async def set_admin_commands(aid: int) -> None:
    """Shows the admin command list in the '/' menu of this admin's chat only."""
    try:
        await bot.set_my_commands(ADMIN_COMMANDS, scope=BotCommandScopeChat(chat_id=aid))
    except TelegramAPIError as e:
        log.debug("set_my_commands for %s failed (admin must press /start first): %s", aid, e)


async def setup_commands() -> None:
    try:
        await bot.set_my_commands(USER_COMMANDS)       # everyone else sees only /start
    except TelegramAPIError as e:
        log.warning("set default commands failed: %s", e)
    for aid in ADMIN_IDS:
        await set_admin_commands(aid)


@router.message(Command("admin"))
async def cmd_admin(m: Message) -> None:
    if not _is_admin_msg(m):
        return
    await clear_astate(m.from_user.id)
    await panel_home(m)


@router.message(Command("terms"), F.chat.type == "private")
async def cmd_terms(m: Message) -> None:
    u = await gate(m)
    if u:
        await m.answer(t(u.get("language"), "terms_text"))


@router.message(Command("privacy"), F.chat.type == "private")
async def cmd_privacy(m: Message) -> None:
    u = await gate(m)
    if u:
        await m.answer(t(u.get("language"), "privacy_text"))


@router.message(Command("support"), F.chat.type == "private")
async def cmd_support(m: Message) -> None:
    u = await gate(m)
    if not u:
        return
    lang = u.get("language")
    if not support_user():
        await m.answer(t(lang, "support_none"))
        return
    await m.answer(t(lang, "support_text"), reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=t(lang, "btn_support"), url=f"https://t.me/{support_user()}")]]))


@router.message(Command("cancel"))
async def cmd_cancel(m: Message) -> None:
    if not _is_admin_msg(m):
        return
    await clear_astate(m.from_user.id)
    await m.answer("✅ تم إلغاء العملية الحالية.", reply_markup=akb([BACK_HOME]))


@router.message(Command("new_package"))
async def cmd_new_package(m: Message) -> None:
    if not _is_admin_msg(m):
        return
    await adm_np_start(m)


@router.message(Command("stats"))
async def cmd_stats(m: Message) -> None:
    if not _is_admin_msg(m):
        return
    await stats_view(m, 7)


@router.message(Command("broadcast"))
async def cmd_broadcast(m: Message) -> None:
    if not _is_admin_msg(m):
        return
    await bc_start(m)


@router.message(Command("user"))
async def cmd_user(m: Message, command: CommandObject) -> None:
    if not _is_admin_msg(m):
        return
    arg = (command.args or "").strip()
    if arg.isdigit():
        await user_card(m, int(arg))
    else:
        await set_astate(m.from_user.id, "user_find")
        await m.answer("👤 أرسل الـ ID الرقمي للمستخدم (أو /cancel).")


@router.message(Command("stopbc"))
async def cmd_stopbc(m: Message) -> None:
    if not _is_admin_msg(m):
        return
    if not _bc_tasks:
        await m.answer("لا توجد إذاعة جارية.")
        return
    for bid in list(_bc_tasks):
        _bc_cancel.add(bid)
    await m.answer("⛔ جارٍ إيقاف الإذاعة...")


# =====================================================================
#  ADMIN: categories ("buttons" shown inside the Mini App)
# =====================================================================
async def ct_list(ev: Any) -> None:
    cats = await db.select("categories", order="sort_order.asc,id.asc")
    pks = await db.select("packages", "category_id", {"status": "neq.deleted"}, limit=5000)
    counts: dict = {}
    for p in pks:
        counts[p["category_id"]] = counts.get(p["category_id"], 0) + 1
    rows = []
    for c in cats:
        icon = "🟢" if c["is_visible"] else "🔴"
        rows.append([ab(f"{icon} {c['name'][:30]} ({counts.get(c['id'], 0)})", f"a:ct:{c['id']}")])
    rows.append([ab("➕ إنشاء زر جديد", "a:ct:new")])
    rows.append(BACK_HOME)
    text = "🗂 <b>الأقسام والأزرار</b>\nهذه الأزرار تظهر للمستخدم داخل التطبيق، وكل زر يحتوي حزماً."
    if not cats:
        text += "\n\nلا توجد أزرار بعد، أنشئ أول زر."
    await show(ev, text, akb(rows))


async def ct_detail(ev: Any, cid: int) -> None:
    c = await db.one("categories", {"id": f"eq.{cid}"})
    if not c:
        await ct_list(ev)
        return
    n = await db.count("packages", {"category_id": f"eq.{cid}", "status": "neq.deleted"})
    text = (f"🗂 <b>{esc(c['name'])}</b>\n━━━━━━━━━━\n"
            f"الحالة: {'🟢 ظاهر' if c['is_visible'] else '🔴 مخفي'}\n"
            f"عدد الحزم: <b>{n}</b>\nالترتيب: {c['sort_order']}")
    kb = akb([
        [ab("✏️ تغيير الاسم", f"a:ct:{cid}:ren"), ab("👁 إخفاء/إظهار", f"a:ct:{cid}:vis")],
        [ab("⬆️ أعلى", f"a:ct:{cid}:up"), ab("⬇️ أسفل", f"a:ct:{cid}:down")],
        [ab("🗑 حذف", f"a:ct:{cid}:del"), ab("🔙 رجوع", "a:ct:list")],
    ])
    await show(ev, text, kb)


async def adm_ct(cb: CallbackQuery, parts: list) -> None:
    aid = cb.from_user.id
    sub = parts[2] if len(parts) > 2 else "list"
    if sub == "list":
        await clear_astate(aid)
        await ct_list(cb)
        return
    if sub == "new":
        await set_astate(aid, "cat_new")
        await show(cb, "🗂 أرسل اسم الزر الجديد (أو /cancel).", akb([[ab("❌ إلغاء", "a:ct:list")]]))
        return
    if not sub.isdigit():
        return
    cid = int(sub)
    act = parts[3] if len(parts) > 3 else ""
    if act == "":
        await ct_detail(cb, cid)
    elif act == "ren":
        await set_astate(aid, "cat_ren", {"cid": cid})
        await show(cb, "✏️ أرسل الاسم الجديد للزر (أو /cancel).", akb([[ab("❌ إلغاء", f"a:ct:{cid}")]]))
    elif act == "vis":
        c = await db.one("categories", {"id": f"eq.{cid}"})
        if c:
            await db.update("categories", {"id": f"eq.{cid}"}, {"is_visible": not c["is_visible"]})
            await log_admin(aid, "category_visibility", f"{cid} -> {not c['is_visible']}")
        await ct_detail(cb, cid)
    elif act in ("up", "down"):
        cats = await db.select("categories", "id", order="sort_order.asc,id.asc")
        ids = [c["id"] for c in cats]
        if cid in ids:
            i = ids.index(cid)
            j = i - 1 if act == "up" else i + 1
            if 0 <= j < len(ids):
                ids[i], ids[j] = ids[j], ids[i]
            for pos, x in enumerate(ids, start=1):
                await db.update("categories", {"id": f"eq.{x}"}, {"sort_order": pos})
        await ct_detail(cb, cid)
    elif act == "del":
        n = await db.count("packages", {"category_id": f"eq.{cid}", "status": "neq.deleted"})
        if n:
            await cb.answer(f"لا يمكن الحذف: يحتوي {n} حزمة. انقل الحزم أو احذفها أولاً.", show_alert=True)
            return
        await db.delete("categories", {"id": f"eq.{cid}"})
        await log_admin(aid, "category_deleted", str(cid))
        await ct_list(cb)


# =====================================================================
#  ADMIN: packages
# =====================================================================
async def pkg_list(ev: Any, page: int) -> None:
    per = 8
    total = await db.count("packages", {"status": "neq.deleted"})
    rows_db = await db.select("packages", "id,title,price_in_points,status",
                              {"status": "neq.deleted"}, order="id.desc",
                              limit=per, offset=page * per)
    re_ids = await reupload_pkg_ids()
    rows = []
    for p in rows_db:
        mark = " ‼️" if p["id"] in re_ids else ""
        rows.append([ab(f"{STATUS_ICON.get(p['status'], '')}{mark} {p['title'][:26]} • {p['price_in_points']}",
                        f"a:pk:{p['id']}")])
    nav = []
    if page > 0:
        nav.append(ab("⬅️ السابق", f"a:pk:list:{page - 1}"))
    if (page + 1) * per < total:
        nav.append(ab("التالي ➡️", f"a:pk:list:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([ab("➕ حزمة جديدة", "a:np")])
    rows.append(BACK_HOME)
    text = f"📦 <b>الحزم</b> ({total})\n🟢 نشطة  🟡 قيد الإنشاء  🔴 مخفية  ‼️ تحتاج إعادة رفع"
    if not rows_db:
        text += "\n\nلا توجد حزم بعد."
    await show(ev, text, akb(rows))


async def pkg_detail(ev: Any, pid: int) -> None:
    pk = await db.one("packages", {"id": f"eq.{pid}"})
    if not pk or pk["status"] == "deleted":
        await show(ev, "الحزمة غير موجودة.", akb([[ab("🔙 رجوع", "a:pk:list:0")]]))
        return
    vids = await db.select("videos", "id,file_size,channel_id", {"package_id": f"eq.{pid}"}, limit=5000)
    old = sum(1 for v in vids if v["channel_id"] != STORAGE_CHANNEL_ID)
    size = sum(int(v["file_size"] or 0) for v in vids)
    cat = None
    if pk.get("category_id"):
        cat = await db.one("categories", {"id": f"eq.{pk['category_id']}"})
    lines = [
        f"📦 <b>{esc(pk['title'])}</b>",
        "━━━━━━━━━━",
        f"الحالة: {STATUS_ICON.get(pk['status'], '')} {STATUS_NAME.get(pk['status'], pk['status'])}",
        f"الزر (القسم): {esc(cat['name']) if cat else '— بدون —'}",
        f"السعر: <b>{pk['price_in_points']}</b> نقطة" + (" (مجانية)" if pk["price_in_points"] == 0 else ""),
        f"المقاطع: <b>{len(vids)}</b> • الحجم: <b>{fmt_size(size)}</b>",
        f"الحفظ للمشتري: {'✅ مسموح' if pk['allow_save'] else '🚫 ممنوع'}",
        f"🆔 {pk['id']}",
    ]
    if pk.get("description"):
        lines.insert(2, esc(pk["description"])[:300])
    if old:
        lines.append(f"\n‼️ <b>{old}</b> مقطع تابع لقناة قديمة — يلزم إعادة رفع")
    rows = [
        [ab("✏️ الاسم", f"a:pk:{pid}:title"), ab("📝 الوصف", f"a:pk:{pid}:desc")],
        [ab("💰 السعر", f"a:pk:{pid}:price"), ab("🗂 نقل لقسم", f"a:pk:{pid}:cat")],
        [ab("🎞 المقاطع والترتيب", f"a:vp:{pid}:0"), ab("➕ إضافة مقاطع", f"a:pk:{pid}:add")],
    ]
    second = [ab(f"💾 الحفظ: {'نعم' if pk['allow_save'] else 'لا'}", f"a:pk:{pid}:save")]
    if pk["status"] in ("active", "hidden"):
        second.insert(0, ab("👁 إخفاء" if pk["status"] == "active" else "👁 إظهار", f"a:pk:{pid}:vis"))
    rows.append(second)
    if old:
        rows.append([ab("‼️ إعادة رفع", f"a:pk:{pid}:reup")])
    rows.append([ab("🗑 حذف", f"a:pk:{pid}:del"), ab("🔙 رجوع", "a:pk:list:0")])
    await show(ev, "\n".join(lines), akb(rows))


async def adm_pk(cb: CallbackQuery, parts: list) -> None:
    aid = cb.from_user.id
    sub = parts[2] if len(parts) > 2 else "list"
    if sub == "list":
        await clear_astate(aid)
        await pkg_list(cb, int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0)
        return
    if not sub.isdigit():
        return
    pid = int(sub)
    act = parts[3] if len(parts) > 3 else ""
    if act == "":
        await clear_astate(aid)
        await pkg_detail(cb, pid)
        return
    pk = await db.one("packages", {"id": f"eq.{pid}"})
    if not pk or pk["status"] == "deleted":
        await pkg_list(cb, 0)
        return

    if act in ("title", "desc", "price"):
        prompt = {
            "title": "✏️ أرسل الاسم الجديد للحزمة",
            "desc": "📝 أرسل الوصف الجديد (أو أرسل - لحذف الوصف)",
            "price": "💰 أرسل السعر الجديد بالنقاط (0 = مجانية)",
        }[act]
        await set_astate(aid, "edit", {"field": act, "pid": pid})
        await show(cb, prompt + "\n(أو /cancel)", akb([[ab("❌ إلغاء", f"a:pk:{pid}")]]))
    elif act == "cat":
        cats = await db.select("categories", order="sort_order.asc,id.asc")
        rows = [[ab(("✅ " if c["id"] == pk.get("category_id") else "") + c["name"][:30],
                    f"a:pkc:{pid}:{c['id']}")] for c in cats]
        rows.append([ab("🔙 رجوع", f"a:pk:{pid}")])
        await show(cb, "🗂 اختر الزر (القسم) الذي ستظهر فيه الحزمة:", akb(rows))
    elif act == "vis":
        new = "hidden" if pk["status"] == "active" else "active"
        if pk["status"] in ("active", "hidden"):
            await db.update("packages", {"id": f"eq.{pid}"}, {"status": new})
            await log_admin(aid, "package_status", f"{pid} -> {new}")
        await pkg_detail(cb, pid)
    elif act == "save":
        await db.update("packages", {"id": f"eq.{pid}"}, {"allow_save": not pk["allow_save"]})
        await log_admin(aid, "package_allow_save", f"{pid} -> {not pk['allow_save']}")
        await pkg_detail(cb, pid)
    elif act == "del":
        await show(cb, f"🗑 هل أنت متأكد من حذف «{esc(pk['title'])}»؟\n"
                       "ستختفي من التطبيق، لكن مشتريات من اشتركوا فيها تبقى محفوظة.",
                   akb([[ab("✅ نعم، احذف", f"a:pk:{pid}:delok"), ab("❌ لا", f"a:pk:{pid}")]]))
    elif act == "delok":
        await db.update("packages", {"id": f"eq.{pid}"}, {"status": "deleted"})
        await log_admin(aid, "package_deleted", f"{pid} {pk['title']}")
        await pkg_list(cb, 0)
    elif act in ("add", "reup"):
        if act == "reup":
            mode = "reupload"
        else:
            mode = "new" if pk["status"] == "draft" else "add"
        sess = await start_session(cb, pid, mode)
        if sess:
            await show(cb, session_intro(pk, mode), akb([[ab("❌ إلغاء", "a:up:cancel")]]))


# ---------- video manager (manual ordering) ----------
async def vids_view(pid: int, page: int) -> tuple:
    pk = await db.one("packages", {"id": f"eq.{pid}"}, "id,title")
    vids = await db.select("videos", "id,title,duration,file_size,channel_id",
                           {"package_id": f"eq.{pid}"}, order="sort_order.asc,id.asc", limit=5000)
    per = 12
    pages = max(1, (len(vids) + per - 1) // per)
    page = min(max(page, 0), pages - 1)
    lines = [f"🎞 <b>{esc(pk['title']) if pk else pid}</b> — {len(vids)} مقطع (صفحة {page + 1}/{pages})", ""]
    for i, v in enumerate(vids[page * per:(page + 1) * per], start=page * per + 1):
        title = esc(v["title"] or f"مقطع {i}")[:40]
        warn = " ‼️" if v["channel_id"] != STORAGE_CHANNEL_ID else ""
        lines.append(f"<b>{i}.</b> {title} ⏱{fmt_dur(v['duration'])} 💾{fmt_size(v['file_size'])}{warn}")
    lines += [
        "",
        "أرسل أحد الأوامر:",
        "<code>5>2</code>  نقل المقطع 5 إلى الموضع 2",
        "<code>r 5 الاسم الجديد</code>  تغيير اسم المقطع 5",
        "<code>d 5</code>  حذف المقطع 5",
        "<code>p 2</code>  الانتقال للصفحة 2",
    ]
    nav = []
    if page > 0:
        nav.append(ab("⬅️", f"a:vp:{pid}:{page - 1}"))
    if page < pages - 1:
        nav.append(ab("➡️", f"a:vp:{pid}:{page + 1}"))
    rows = ([nav] if nav else []) + [[ab("🔙 رجوع للحزمة", f"a:pk:{pid}")]]
    return "\n".join(lines), akb(rows)


async def adm_vp(cb: CallbackQuery, parts: list) -> None:
    pid = int(parts[2])
    page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
    await set_astate(cb.from_user.id, "vids", {"pid": pid, "page": page})
    text, kb = await vids_view(pid, page)
    await show(cb, text, kb)


@router.callback_query(F.data.startswith("a:pkc:"))
async def cb_pkc(cb: CallbackQuery) -> None:
    if cb.from_user.id not in ADMIN_IDS:
        await cb.answer("غير مصرح", show_alert=True)
        return
    _, _, pid, cid = cb.data.split(":")
    await db.update("packages", {"id": f"eq.{pid}"}, {"category_id": int(cid)})
    await log_admin(cb.from_user.id, "package_moved", f"{pid} -> category {cid}")
    await cb.answer("✅ تم النقل")
    await pkg_detail(cb, int(pid))


# =====================================================================
#  ADMIN: new package + batch upload sessions
# =====================================================================
async def adm_np_start(ev: Any) -> None:
    cur = await db.one("upload_sessions", {"status": "eq.open"})
    if cur:
        await show(ev, "⚠️ توجد جلسة رفع مفتوحة. أنهِها أو ألغِها أولاً.",
                   akb([[ab("📥 عرض الجلسة", "a:up:show"), ab("❌ إلغاء الجلسة", "a:up:cancel")], BACK_HOME]))
        return
    cats = await db.select("categories", order="sort_order.asc,id.asc")
    if not cats:
        await show(ev, "🗂 لا توجد أزرار (أقسام) بعد. أنشئ زراً أولاً ثم ارجع لإضافة الحزمة.",
                   akb([[ab("➕ إنشاء زر جديد", "a:ct:new")], BACK_HOME]))
        return
    rows = [[ab(c["name"][:30], f"a:np:{c['id']}")] for c in cats]
    rows.append([ab("❌ إلغاء", "a:home")])
    await show(ev, "📦 <b>حزمة جديدة</b>\nاختر الزر (القسم) الذي ستُحفظ فيه الحزمة:", akb(rows))


async def adm_np(cb: CallbackQuery, parts: list) -> None:
    if len(parts) < 3:
        await adm_np_start(cb)
        return
    cid = int(parts[2])
    await set_astate(cb.from_user.id, "np_name", {"cid": cid})
    await show(cb, "📝 أرسل <b>اسم الحزمة</b> (أو /cancel).", akb([[ab("❌ إلغاء", "a:home")]]))


async def finish_new_package(m: Message, data: dict, price: int) -> None:
    aid = m.from_user.id
    cur = await db.one("upload_sessions", {"status": "eq.open"})
    if cur:
        await clear_astate(aid)
        await m.answer("⚠️ توجد جلسة رفع مفتوحة الآن، أنهِها أولاً ثم أنشئ الحزمة.",
                       reply_markup=akb([[ab("📥 عرض الجلسة", "a:up:show"), ab("❌ إلغاء الجلسة", "a:up:cancel")]]))
        return
    pk = (await db.insert("packages", {
        "category_id": data["cid"], "title": data["name"], "description": data.get("desc") or None,
        "price_in_points": price, "status": "draft", "created_by": aid,
    }))[0]
    sess = None
    try:
        sess = (await db.insert("upload_sessions", {"package_id": pk["id"], "admin_id": aid, "mode": "new"}))[0]
    except DBError:
        pass
    await clear_astate(aid)
    await log_admin(aid, "package_created", f"{pk['id']} {pk['title']} price={price}")
    if not sess:
        await m.answer("⚠️ تم إنشاء الحزمة لكن تعذر فتح جلسة الرفع. افتحها من «الحزم» ثم «إضافة مقاطع».")
        return
    _batches[sess["id"]] = {"count": 0, "task": None}
    await m.answer(session_intro(pk, "new"), reply_markup=akb([[ab("❌ إلغاء", "a:up:cancel")]]))


def session_intro(pk: dict, mode: str) -> str:
    title = esc(pk["title"])
    if mode == "new":
        return (f"✅ تم إنشاء الحزمة «<b>{title}</b>» (قيد الإنشاء).\n\n"
                "📥 أرسل الآن المقاطع داخل <b>قناة التخزين</b> — دفعة واحدة أو عدة دفعات.\n"
                "سأؤكد لك بعد كل دفعة، ثم تضغط «نشر وحفظ الحزمة» عند الانتهاء.")
    if mode == "reupload":
        return (f"‼️ <b>إعادة رفع</b> للحزمة «<b>{title}</b>».\n\n"
                "ارفع المقاطع في قناة التخزين <b>بنفس الترتيب القديم</b>، وسأطابق الأول بالأول "
                "ويبقى الترتيب والأسماء كما هي.")
    return (f"➕ إضافة مقاطع إلى «<b>{title}</b>».\n\n"
            "📥 أرسل المقاطع داخل قناة التخزين، وسأؤكد لك بعد كل دفعة.")


_batches: dict = {}


async def start_session(ev: Any, pid: int, mode: str) -> Optional[dict]:
    aid = ev.from_user.id
    cur = await db.one("upload_sessions", {"status": "eq.open"})
    if cur:
        pk = await db.one("packages", {"id": f"eq.{cur['package_id']}"}, "id,title")
        name = esc(pk["title"]) if pk else cur["package_id"]
        await show(ev, f"⚠️ توجد جلسة رفع مفتوحة الآن (الحزمة: {name} • الأدمن: <code>{cur['admin_id']}</code>).\n"
                       "أنهِها أو ألغِها أولاً.",
                   akb([[ab("📥 عرض الجلسة", "a:up:show"), ab("❌ إلغاء الجلسة", "a:up:cancel")], BACK_HOME]))
        return None
    try:
        sess = (await db.insert("upload_sessions", {"package_id": pid, "admin_id": aid, "mode": mode}))[0]
    except DBError:
        await show(ev, "⚠️ تعذر فتح الجلسة، حاول مرة أخرى.", akb([BACK_HOME]))
        return None
    _batches[sess["id"]] = {"count": 0, "task": None}
    return sess


def _session_markup(mode: str) -> InlineKeyboardMarkup:
    pub = "✅ نشر وحفظ الحزمة" if mode == "new" else "✅ حفظ وإنهاء"
    return akb([[ab("➕ إضافة المزيد", "a:up:more"), ab(pub, "a:up:pub")],
                [ab("❌ إلغاء", "a:up:cancel")]])


async def send_session_status(sid: int) -> None:
    sess = await db.one("upload_sessions", {"id": f"eq.{sid}"})
    if not sess or sess["status"] != "open":
        return
    pk = await db.one("packages", {"id": f"eq.{sess['package_id']}"}, "id,title")
    st = _batches.setdefault(sid, {"count": 0, "task": None})
    batch = st["count"]
    st["count"] = 0
    total = await db.count("videos", {"package_id": f"eq.{sess['package_id']}"})
    title = esc(pk["title"]) if pk else ""
    if sess["mode"] == "reupload":
        left = await db.count("videos", {"package_id": f"eq.{sess['package_id']}",
                                         "channel_id": f"neq.{STORAGE_CHANNEL_ID}"})
        text = (f"📦 <b>تحديث إعادة الرفع:</b> {title}\n"
                f"تم استلام: <b>{batch}</b> مقطع في هذه الدفعة.\n"
                f"إجمالي مقاطع الحزمة: <b>{total}</b>\n"
                f"المتبقي بدون رفع جديد: <b>{left}</b> " + ("✅" if left == 0 else "⚠️"))
    else:
        text = (f"📦 <b>تحديث الحزمة:</b> {title}\n"
                f"تم استلام: <b>{batch}</b> مقطع في هذه الدفعة.\n"
                f"إجمالي المقاطع في الحزمة حتى الآن: <b>{total}</b> مقطع.")
    if sess.get("status_msg"):
        try:
            await bot.delete_message(sess["admin_id"], sess["status_msg"])
        except Exception:
            pass
    msg = await safe_send(sess["admin_id"], text, reply_markup=_session_markup(sess["mode"]))
    if msg:
        try:
            await db.update("upload_sessions", {"id": f"eq.{sid}"}, {"status_msg": msg.message_id})
        except Exception:
            pass


async def _report_later(sid: int) -> None:
    try:
        await asyncio.sleep(3.5)
    except asyncio.CancelledError:
        return
    try:
        await send_session_status(sid)
    except Exception as e:
        log.warning("session status failed: %s", e)


def _cancel_batch_task(sid: int) -> None:
    st = _batches.get(sid)
    if st and st.get("task") and not st["task"].done():
        st["task"].cancel()


async def close_session(sid: int) -> None:
    _cancel_batch_task(sid)
    _batches.pop(sid, None)
    await db.update("upload_sessions", {"id": f"eq.{sid}"}, {"status": "closed"})


async def publish_session(ev: Any) -> None:
    sess = await db.one("upload_sessions", {"status": "eq.open"})
    if not sess:
        await show(ev, "لا توجد جلسة رفع مفتوحة.", akb([BACK_HOME]))
        return
    pid = sess["package_id"]
    pk = await db.one("packages", {"id": f"eq.{pid}"})
    vids = await db.select("videos", "id,file_size", {"package_id": f"eq.{pid}"}, limit=5000)
    if sess["mode"] == "new" and not vids:
        if isinstance(ev, CallbackQuery):
            await ev.answer("لم يصل أي مقطع بعد. أرسل المقاطع في قناة التخزين أولاً.", show_alert=True)
        return
    size = sum(int(v["file_size"] or 0) for v in vids)
    await close_session(sess["id"])
    if sess["mode"] == "new":
        await db.rpc("sort_videos_by_message", {"p_package": pid})
        await db.update("packages", {"id": f"eq.{pid}"}, {"status": "active", "published_at": iso_now()})
        await log_admin(ev.from_user.id, "package_published", f"{pid} {pk['title']} videos={len(vids)}")
        cat = None
        if pk.get("category_id"):
            cat = await db.one("categories", {"id": f"eq.{pk['category_id']}"})
        if MANIFEST_CHANNEL_ID:
            await safe_send(MANIFEST_CHANNEL_ID, (
                "📦 <b>تم نشر حزمة جديدة</b>\n━━━━━━━━━━\n"
                f"🏷 الاسم: {esc(pk['title'])}\n"
                f"📂 الزر: {esc(cat['name']) if cat else '-'}\n"
                f"💰 السعر: {pk['price_in_points']} نقطة\n"
                f"🎞 المقاطع: {len(vids)}\n"
                f"💾 الحجم: {fmt_size(size)}\n"
                f"📅 التاريخ: {bahrain_time(iso_now())}\n"
                f"🆔 pkg_{pid}\n━━━━━━━━━━\n#Vidox #pkg_{pid}"))
        text = "✅ تم نشر الحزمة بنجاح وأصبحت متاحة للمشتركين!"
        if AUTO_ANNOUNCE:
            text += "\n📢 جارٍ نشر الإعلان في القناة وإذاعته للمستخدمين تلقائياً."
            asyncio.create_task(announce_new_package(pk, len(vids), ev.from_user.id))
    else:
        await log_admin(ev.from_user.id, "upload_session_closed", f"{pid} mode={sess['mode']}")
        text = "✅ تم حفظ التغييرات وإنهاء جلسة الرفع."
    await show(ev, text, akb([[ab("📦 فتح الحزمة", f"a:pk:{pid}")], BACK_HOME]))


async def cancel_session(ev: Any) -> None:
    sess = await db.one("upload_sessions", {"status": "eq.open"})
    if not sess:
        await show(ev, "لا توجد جلسة رفع مفتوحة.", akb([BACK_HOME]))
        return
    pid = sess["package_id"]
    await close_session(sess["id"])
    if sess["mode"] == "new":
        await db.delete("packages", {"id": f"eq.{pid}"})
        note = "تم إلغاء الجلسة وحذف الحزمة المؤقتة."
    elif sess["mode"] == "add":
        await db.delete("videos", {"package_id": f"eq.{pid}", "created_at": f"gte.{sess['created_at']}"})
        await db.rpc("renumber_videos", {"p_package": pid})
        note = "تم إلغاء الجلسة وحذف المقاطع التي أُضيفت فيها."
    else:
        note = "تم إنهاء جلسة إعادة الرفع (ما طُوبق منها يبقى محفوظاً)."
    await log_admin(ev.from_user.id, "upload_session_cancelled", f"{pid} mode={sess['mode']}")
    await show(ev, "❌ " + note, akb([BACK_HOME]))


async def adm_up(cb: CallbackQuery, parts: list) -> None:
    act = parts[2] if len(parts) > 2 else ""
    if act == "more":
        await cb.answer("📥 أرسل الدفعة التالية في قناة التخزين.", show_alert=False)
    elif act == "pub":
        await publish_session(cb)
    elif act == "show":
        sess = await db.one("upload_sessions", {"status": "eq.open"})
        if not sess:
            await show(cb, "لا توجد جلسة رفع مفتوحة.", akb([BACK_HOME]))
            return
        _batches.setdefault(sess["id"], {"count": 0, "task": None})
        await send_session_status(sess["id"])
    elif act == "cancel":
        sess = await db.one("upload_sessions", {"status": "eq.open"})
        if not sess:
            await show(cb, "لا توجد جلسة رفع مفتوحة.", akb([BACK_HOME]))
            return
        warn = {"new": "سيتم حذف الحزمة المؤقتة وكل مقاطعها المسجلة.",
                "add": "سيتم حذف المقاطع التي أُضيفت في هذه الجلسة.",
                "reupload": "ما تمت مطابقته يبقى محفوظاً."}[sess["mode"]]
        await show(cb, f"❓ هل أنت متأكد من إلغاء جلسة الرفع؟\n{warn}",
                   akb([[ab("✅ نعم، ألغِ", "a:up:cancelok"), ab("❌ لا، أكمل", "a:up:show")]]))
    elif act == "cancelok":
        await cancel_session(cb)


# =====================================================================
#  ADMIN: videos arriving in the storage channel
# =====================================================================
def _media_info(m: Message) -> Optional[dict]:
    caption = (m.caption or "").strip()
    cap_title = caption.split("\n")[0][:100] if caption else None
    v = m.video
    if v:
        th = getattr(v, "thumbnail", None) or getattr(v, "thumb", None)
        return {"file_id": v.file_id, "unique": v.file_unique_id, "duration": v.duration,
                "size": v.file_size or 0, "mime": v.mime_type,
                "thumb": th.file_id if th else None,
                "title": cap_title or (re.sub(r"\.[A-Za-z0-9]{2,5}$", "", v.file_name) if getattr(v, "file_name", None) else None)}
    d = m.document
    if d and (d.mime_type or "").startswith("video/"):
        th = getattr(d, "thumbnail", None)
        return {"file_id": d.file_id, "unique": d.file_unique_id, "duration": None,
                "size": d.file_size or 0, "mime": d.mime_type,
                "thumb": th.file_id if th else None,
                "title": cap_title or (re.sub(r"\.[A-Za-z0-9]{2,5}$", "", d.file_name) if d.file_name else None)}
    return None


@router.channel_post()
async def on_channel_post(m: Message) -> None:
    if m.chat.id != STORAGE_CHANNEL_ID:
        return                                   # any other channel is ignored
    info = _media_info(m)
    if not info:
        return
    sess = await db.one("upload_sessions", {"status": "eq.open"})
    if not sess:
        return                                   # no open upload session -> not part of any package
    args = {
        "p_package": sess["package_id"], "p_channel": m.chat.id, "p_mid": m.message_id,
        "p_file_id": info["file_id"], "p_unique": info["unique"], "p_duration": info["duration"],
        "p_size": info["size"], "p_mime": info["mime"], "p_title": info["title"],
    }
    try:
        res = await db.rpc("replace_video" if sess["mode"] == "reupload" else "add_video", args)
    except DBError as e:
        log.error("add video failed: %s", e)
        await alert_admins(f"⚠️ فشل حفظ مقطع من القناة:\n<code>{esc(str(e))[:300]}</code>", key="addvideo")
        return
    if not res or res.get("duplicate") or not res.get("video_id"):
        return
    if info["thumb"] and thumb_queue is not None and res.get("needs_thumb", True):
        thumb_queue.put_nowait((res["video_id"], info["thumb"]))
    st = _batches.setdefault(sess["id"], {"count": 0, "task": None})
    st["count"] += 1
    if st.get("task") and not st["task"].done():
        st["task"].cancel()
    st["task"] = asyncio.create_task(_report_later(sess["id"]))


async def thumb_worker() -> None:
    """Downloads Telegram's small preview image of each video and stores it in the database."""
    while True:
        vid, file_id = await thumb_queue.get()
        try:
            f = await bot.get_file(file_id)
            buf = await bot.download_file(f.file_path)
            data = buf.read() if hasattr(buf, "read") else bytes(buf)
            await db.save_thumb(vid, data)
        except Exception as e:
            log.warning("thumbnail for video %s failed: %s", vid, e)
        await asyncio.sleep(0.15)


# =====================================================================
#  ADMIN: statistics
# =====================================================================
_DIG = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")


def norm_digits(s: str) -> str:
    return (s or "").translate(_DIG)


async def stats_view(ev: Any, days: int) -> None:
    s = await db.rpc("admin_stats", {"p_days": days}) or {}
    label = {1: "آخر 24 ساعة", 7: "آخر 7 أيام", 30: "آخر 30 يوماً", 0: "كل الفترة"}.get(days, "")
    total = s.get("users_total", 0) or 0
    active = s.get("users_active", 0) or 0
    rate = f"{(active * 100 // total) if total else 0}%"
    top_pk = "\n".join(f"  • {esc(x['title'])} ({x['c']})" for x in s.get("top_packages", [])) or "  —"
    top_ref = "\n".join(f"  • {esc(str(x['name']))} ({x['c']})" for x in s.get("top_referrers", [])) or "  —"
    text = (
        f"📊 <b>إحصائيات — {label}</b>\n━━━━━━━━━━\n"
        f"👥 الإجمالي: <b>{total}</b> | جدد: <b>{s.get('users_new', 0)}</b> | نشطون: <b>{active}</b>\n"
        f"🏆 نسبة النشاط: <b>{rate}</b>\n"
        f"🔗 إحالات: <b>{s.get('referrals', 0)}</b>\n"
        f"🚫 حظروا البوت: <b>{s.get('blocked', 0)}</b> | محظورون: <b>{s.get('banned', 0)}</b>\n"
        "━━━━━━━━━━\n"
        f"💧 نقاط متداولة: <b>{s.get('points_total', 0)}</b>\n"
        f"💰 نقاط مشتراة: <b>{s.get('points_bought', 0)}</b> | مصروفة: <b>{s.get('points_spent', 0)}</b>\n"
        f"🔓 حزم مفتوحة: <b>{s.get('unlocks', 0)}</b> | حزم نشطة: <b>{s.get('packages_active', 0)}</b>\n"
        "━━━━━━━━━━\n"
        f"🔥 الأكثر مبيعاً:\n{top_pk}\n"
        f"🏅 أفضل المُحيلين:\n{top_ref}"
    )
    kb = akb([
        [ab("24 ساعة", "a:st:1"), ab("7 أيام", "a:st:7"), ab("30 يوماً", "a:st:30"), ab("الكل", "a:st:0")],
        [ab("🔄 تحديث", f"a:st:{days}"), ab("🏠 الرئيسية", "a:home")],
    ])
    await show(ev, text, kb)


async def adm_stats(cb: CallbackQuery, parts: list) -> None:
    days = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 7
    await stats_view(cb, days)


# =====================================================================
#  ADMIN: broadcast (with progress, cancel, and delete-from-everyone)
# =====================================================================
_bc_tasks: dict = {}
_bc_cancel: set = set()
AUDIENCES = {
    "all": "👥 كل المستخدمين",
    "a7": "🔥 النشطون آخر 7 أيام",
    "buyers": "💎 من اشترى نقاطاً",
    "en": "🇺🇸 English",
    "ar": "🇾🇪 العربية",
}


async def audience_ids(aud: str) -> list:
    filters = {"is_active": "eq.true", "is_banned": "eq.false"}
    if aud == "a7":
        filters["last_active_at"] = f"gte.{iso(now_utc() - timedelta(days=7))}"
    elif aud == "buyers":
        filters["is_premium"] = "eq.true"
    elif aud in ("en", "ar"):
        filters["language"] = f"eq.{aud}"
    ids: list = []
    off = 0
    while True:
        rows = await db.select("users", "telegram_id", filters, order="telegram_id.asc",
                               limit=1000, offset=off)
        ids += [r["telegram_id"] for r in rows]
        if len(rows) < 1000:
            break
        off += 1000
    return ids


async def bc_start(ev: Any) -> None:
    if _bc_tasks:
        await show(ev, "⚠️ توجد إذاعة جارية الآن. انتظر انتهاءها أو أوقفها بالزر في رسالة التقدم.",
                   akb([BACK_HOME]))
        return
    rows = [[ab(name, f"a:bc:aud:{key}")] for key, name in AUDIENCES.items()]
    rows.append([ab("❌ إلغاء", "a:bc:cancel")])
    await show(ev, "📢 <b>إذاعة</b>\nاختر الجمهور:", akb(rows))


def _bar(pct: int) -> str:
    filled = max(0, min(10, pct // 10))
    return "█" * filled + "░" * (10 - filled)


async def _bc_one(src_chat: int, src_msg: int, uid: int, ann: Optional[dict] = None) -> tuple:
    for attempt in range(2):
        try:
            if ann:
                r = await bot.send_message(uid, ann["text"], reply_markup=ann.get("markup"))
            else:
                r = await bot.copy_message(uid, src_chat, src_msg)
            return "ok", r.message_id
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 0.5)
        except TelegramForbiddenError:
            return "blocked", 0
        except TelegramBadRequest as e:
            if "chat not found" in str(e).lower() or "deactivated" in str(e).lower():
                return "blocked", 0
            return "fail", 0
        except TelegramAPIError:
            return "fail", 0
    return "fail", 0


async def run_broadcast(bid: int, admin_chat: int, progress_id: int, src_chat: int, src_msg: int,
                        ids: list, ann: Optional[dict] = None) -> None:
    total = len(ids)
    sent = failed = blocked = 0
    recs: list = []
    blocked_ids: list = []
    status = "done"
    last_edit = 0.0

    async def render(final: bool) -> None:
        pct = 100 if total == 0 else int((sent + failed + blocked) * 100 / total)
        reach = (sent * 100 // total) if total else 0
        head = "📢 <b>اكتملت الإذاعة</b>" if final and status == "done" else (
            "⛔ <b>تم إيقاف الإذاعة</b>" if final else "📢 <b>جارٍ الإذاعة...</b>")
        text = (f"{head}\n{_bar(pct)} {pct}%\n"
                f"✅ وصلت: <b>{sent}</b>   ❌ فشلت: <b>{failed}</b>   🚫 حظروا البوت: <b>{blocked}</b>\n"
                f"📈 نسبة الوصول: <b>{reach}%</b> من {total}")
        if final:
            kb = akb([[ab("🗑 حذف من عند الجميع", f"a:bc:del:{bid}")], BACK_HOME])
        else:
            kb = akb([[ab("⛔ إلغاء الإذاعة", f"a:bc:stop:{bid}")]])
        try:
            await bot.edit_message_text(text, chat_id=admin_chat, message_id=progress_id, reply_markup=kb)
        except Exception:
            pass

    try:
        for i in range(0, total, 20):
            if bid in _bc_cancel:
                status = "cancelled"
                break
            chunk = ids[i:i + 20]
            results = await asyncio.gather(*(_bc_one(src_chat, src_msg, uid, ann) for uid in chunk))
            for uid, (res, mid) in zip(chunk, results):
                if res == "ok":
                    sent += 1
                    recs.append({"broadcast_id": bid, "telegram_id": uid, "message_id": mid})
                elif res == "blocked":
                    blocked += 1
                    blocked_ids.append(uid)
                else:
                    failed += 1
            if len(recs) >= 200:
                try:
                    await db.insert("broadcast_recipients", recs, on_conflict="broadcast_id,telegram_id",
                                    upsert=True)
                    recs = []
                except Exception as e:
                    log.warning("recipients insert failed: %s", e)
            if time.time() - last_edit > 3:
                last_edit = time.time()
                await render(False)
            await asyncio.sleep(1.0)
    except Exception as e:
        log.exception("broadcast crashed: %s", e)
        status = "interrupted"
    finally:
        if recs:
            try:
                await db.insert("broadcast_recipients", recs, on_conflict="broadcast_id,telegram_id", upsert=True)
            except Exception as e:
                log.warning("recipients insert failed: %s", e)
        for j in range(0, len(blocked_ids), 100):
            part = blocked_ids[j:j + 100]
            try:
                await db.update("users", {"telegram_id": f"in.({','.join(map(str, part))})"},
                                {"is_active": False})
            except Exception:
                pass
        try:
            await db.update("broadcasts", {"id": f"eq.{bid}"}, {
                "status": status, "sent": sent, "failed": failed, "blocked": blocked,
                "finished_at": iso_now()})
        except Exception:
            pass
        _bc_tasks.pop(bid, None)
        _bc_cancel.discard(bid)
        await render(True)


async def announce_new_package(pk: dict, vids_count: int, admin_id: int) -> None:
    """After a new package is published: post in the updates channel + broadcast to all users."""
    title = esc(pk["title"])
    text = ("تم 🔥 إضافة حزمة جديدة\n\n"
            f"📦 <b>{title}</b>\n🎞 {vids_count} مقطع\n\n"
            "شاهد الآن 👇🏻\n\n"
            "🔥 New package added — watch now 👇🏻")
    ch_chat, ch_msg = 0, 0
    # 1) channel post (inline web_app buttons are not allowed in channels -> url button)
    try:
        uname = BOT_USERNAME.lstrip("@")
        link = f"https://t.me/{uname}/{APP_SHORT_NAME}" if APP_SHORT_NAME else f"https://t.me/{uname}?start=open"
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚀 فتح التطبيق | Open App", url=link)]])
        m = await bot.send_message(UPDATES_CHANNEL, text, reply_markup=kb)
        ch_chat, ch_msg = m.chat.id, m.message_id
    except Exception as e:
        log.warning("channel announce failed: %s", e)
        await safe_send(admin_id, "⚠️ تعذر النشر في قناة التحديثات. تأكد أن البوت أدمن في القناة وله صلاحية نشر الرسائل.")
    # 2) broadcast to every active user (button opens the Mini App directly)
    if _bc_tasks:
        await safe_send(admin_id, "⚠️ توجد إذاعة جارية، لم يتم إرسال إعلان الحزمة للمستخدمين.")
        return
    try:
        ids = await audience_ids("all")
        if not ids:
            return
        markup = open_app_markup("ar")
        bc = (await db.insert("broadcasts", {
            "admin_id": admin_id, "audience": "all", "src_chat": ch_chat, "src_message": ch_msg,
            "total": len(ids)}))[0]
        await log_admin(admin_id, "auto_announce", f"pkg={pk['id']} bc={bc['id']} n={len(ids)}")
        prog = await safe_send(admin_id, f"📢 <b>جارٍ إذاعة الحزمة الجديدة...</b>\n{_bar(0)} 0%",
                               reply_markup=akb([[ab("⛔ إلغاء الإذاعة", f"a:bc:stop:{bc['id']}")]]))
        if not prog:
            await db.update("broadcasts", {"id": f"eq.{bc['id']}"}, {"status": "cancelled"})
            return
        _bc_tasks[bc["id"]] = asyncio.create_task(
            run_broadcast(bc["id"], admin_id, prog.message_id, ch_chat, ch_msg, ids,
                          {"text": text, "markup": markup}))
    except Exception as e:
        log.exception("auto announce failed: %s", e)
        await safe_send(admin_id, "⚠️ فشلت إذاعة الحزمة الجديدة تلقائياً.")


async def delete_broadcast(bid: int, admin_chat: int) -> None:
    deleted = 0
    off = 0
    while True:
        rows = await db.select("broadcast_recipients", "telegram_id,message_id",
                               {"broadcast_id": f"eq.{bid}"}, order="telegram_id.asc",
                               limit=1000, offset=off)
        if not rows:
            break
        for i in range(0, len(rows), 20):
            chunk = rows[i:i + 20]

            async def _del(r: dict) -> int:
                try:
                    await bot.delete_message(r["telegram_id"], r["message_id"])
                    return 1
                except TelegramRetryAfter as e:
                    await asyncio.sleep(e.retry_after + 0.5)
                except Exception:
                    pass
                return 0

            deleted += sum(await asyncio.gather(*(_del(r) for r in chunk)))
            await asyncio.sleep(1.0)
        if len(rows) < 1000:
            break
        off += 1000
    await db.update("broadcasts", {"id": f"eq.{bid}"}, {"status": "deleted"})
    await safe_send(admin_chat, f"🗑 تم حذف الإذاعة من عند <b>{deleted}</b> مستخدم.\n(تيليجرام يسمح بالحذف خلال 48 ساعة فقط)")


async def adm_bc(cb: CallbackQuery, parts: list) -> None:
    aid = cb.from_user.id
    act = parts[2] if len(parts) > 2 else "start"
    if act == "start":
        await bc_start(cb)
    elif act == "aud":
        key = parts[3] if len(parts) > 3 else "all"
        if key not in AUDIENCES:
            return
        await set_astate(aid, "bc_content", {"aud": key})
        await show(cb, f"📢 الجمهور: <b>{AUDIENCES[key]}</b>\n\nأرسل الآن الرسالة (نص أو صورة أو فيديو أو أي محتوى) "
                       "كما تريد أن تصل للمستخدمين.\n(أو /cancel)",
                   akb([[ab("❌ إلغاء", "a:bc:cancel")]]))
    elif act == "cancel":
        await clear_astate(aid)
        await panel_home(cb)
    elif act == "go":
        st = await get_astate(aid)
        if not st or st["state"] != "bc_confirm":
            await show(cb, "انتهت صلاحية الإذاعة، ابدأ من جديد.", akb([BACK_HOME]))
            return
        if _bc_tasks:
            await cb.answer("توجد إذاعة جارية الآن.", show_alert=True)
            return
        d = st["data"]
        ids = await audience_ids(d["aud"])
        if not ids:
            await show(cb, "لا يوجد مستخدمون في هذا الجمهور.", akb([BACK_HOME]))
            return
        bc = (await db.insert("broadcasts", {
            "admin_id": aid, "audience": d["aud"], "src_chat": d["src_chat"],
            "src_message": d["src_msg"], "total": len(ids)}))[0]
        await clear_astate(aid)
        await log_admin(aid, "broadcast_started", f"id={bc['id']} aud={d['aud']} n={len(ids)}")
        await show(cb, f"📢 <b>جارٍ الإذاعة...</b>\n{_bar(0)} 0%", akb([[ab("⛔ إلغاء الإذاعة", f"a:bc:stop:{bc['id']}")]]))
        _bc_tasks[bc["id"]] = asyncio.create_task(
            run_broadcast(bc["id"], cb.message.chat.id, cb.message.message_id,
                          d["src_chat"], d["src_msg"], ids))
    elif act == "stop":
        bid = int(parts[3])
        if bid in _bc_tasks:
            _bc_cancel.add(bid)
            await cb.answer("⛔ جارٍ الإيقاف...", show_alert=False)
        else:
            await cb.answer("الإذاعة منتهية.", show_alert=False)
    elif act == "del":
        bid = int(parts[3])
        await cb.answer("🗑 جارٍ الحذف من عند المستخدمين...", show_alert=False)
        await log_admin(aid, "broadcast_delete", str(bid))
        asyncio.create_task(delete_broadcast(bid, cb.message.chat.id))


# =====================================================================
#  ADMIN: user management
# =====================================================================
async def user_card(ev: Any, uid: int) -> None:
    u = await db.one("users", {"telegram_id": f"eq.{uid}"})
    if not u:
        await show(ev, f"لا يوجد مستخدم بالـ ID <code>{uid}</code>.", akb([BACK_HOME]))
        return
    subs = await db.select("user_subscriptions", "package_id,packages(title)",
                           {"telegram_id": f"eq.{uid}"}, limit=30)
    st = await db.rpc("user_stats", {"p_user": uid}) or {}
    names = "، ".join(esc((s.get("packages") or {}).get("title", str(s["package_id"])))[:25] for s in subs) or "—"
    uname = f"@{esc(u['username'])}" if u.get("username") else "—"
    flags = []
    if u.get("is_banned"):
        flags.append("🚫 محظور")
    if not u.get("is_active"):
        flags.append("🔕 أوقف البوت")
    if u.get("is_premium"):
        flags.append("💎 مشتري")
    text = (
        f"👤 <b>{esc(u.get('full_name') or '-')}</b> ({uname})\n━━━━━━━━━━\n"
        f"🆔 <code>{uid}</code>\n"
        f"💧 الرصيد: <b>{u['points_balance']}</b> نقطة\n"
        f"📦 الحزم ({len(subs)}): {names}\n"
        f"🔗 إحالاته: <b>{st.get('referrals', 0)}</b> • أرباحها: <b>{st.get('earned', 0)}</b>\n"
        f"🌐 اللغة: {u['language']}\n"
        f"📅 انضم: {bahrain_time(u['created_at'])}\n"
        f"🕒 آخر نشاط: {bahrain_time(u['last_active_at'])}\n"
        + ("الحالة: " + " • ".join(flags) if flags else "الحالة: عادي")
    )
    kb = akb([
        [ab("➕ إضافة نقاط", f"a:us:{uid}:add"), ab("➖ خصم نقاط", f"a:us:{uid}:sub")],
        [ab("🔓 منح حزمة", f"a:us:{uid}:gp"), ab("🔒 سحب حزمة", f"a:us:{uid}:rv")],
        [ab("✅ فك الحظر" if u.get("is_banned") else "🚫 حظر", f"a:us:{uid}:ban"),
         ab("📜 سجل نقاطه", f"a:us:{uid}:hist")],
        [ab("✉️ رسالة له", f"a:us:{uid}:msg"), ab("🔄 تحديث", f"a:us:{uid}")],
        BACK_HOME,
    ])
    await show(ev, text, kb)


async def adm_us(cb: CallbackQuery, parts: list) -> None:
    aid = cb.from_user.id
    sub = parts[2] if len(parts) > 2 else "find"
    if sub == "find":
        await set_astate(aid, "user_find")
        await show(cb, "👤 أرسل الـ ID الرقمي للمستخدم (أو /cancel).", akb([[ab("❌ إلغاء", "a:home")]]))
        return
    if not sub.isdigit():
        return
    uid = int(sub)
    act = parts[3] if len(parts) > 3 else ""
    u = await db.one("users", {"telegram_id": f"eq.{uid}"})
    if not u:
        await show(cb, "المستخدم غير موجود.", akb([BACK_HOME]))
        return
    if act == "":
        await user_card(cb, uid)
    elif act in ("add", "sub"):
        await set_astate(aid, "pts", {"uid": uid, "sign": 1 if act == "add" else -1})
        await show(cb, f"{'➕ إضافة' if act == 'add' else '➖ خصم'} نقاط — أرسل العدد (رقم صحيح)\n(أو /cancel)",
                   akb([[ab("❌ إلغاء", f"a:us:{uid}")]]))
    elif act == "ban":
        new = not u.get("is_banned")
        await db.update("users", {"telegram_id": f"eq.{uid}"}, {"is_banned": new})
        drop_user(uid)
        await log_admin(aid, "user_ban" if new else "user_unban", str(uid))
        await user_card(cb, uid)
    elif act == "hist":
        tx = await db.select("points_transactions", "amount,type,balance_after,created_at,note",
                             {"telegram_id": f"eq.{uid}"}, order="id.desc", limit=12)
        lines = [f"📜 <b>آخر عمليات {uid}</b>", ""]
        for x in tx:
            sign = "+" if x["amount"] > 0 else ""
            lines.append(f"{bahrain_time(x['created_at'])} • {x['type']} • <b>{sign}{x['amount']}</b> ← {x['balance_after']}")
        if not tx:
            lines.append("لا توجد عمليات.")
        await show(cb, "\n".join(lines), akb([[ab("🔙 رجوع", f"a:us:{uid}")]]))
    elif act == "msg":
        await set_astate(aid, "msg_user", {"uid": uid})
        await show(cb, "✉️ أرسل الرسالة التي تريد إيصالها للمستخدم (أو /cancel).",
                   akb([[ab("❌ إلغاء", f"a:us:{uid}")]]))
    elif act == "gp":
        pks = await db.select("packages", "id,title,price_in_points", {"status": "eq.active"},
                              order="id.desc", limit=30)
        owned = {s["package_id"] for s in await db.select(
            "user_subscriptions", "package_id", {"telegram_id": f"eq.{uid}"}, limit=500)}
        rows = [[ab(p["title"][:34], f"a:us:{uid}:gpk:{p['id']}")] for p in pks if p["id"] not in owned]
        rows.append([ab("🔙 رجوع", f"a:us:{uid}")])
        await show(cb, "🔓 اختر الحزمة التي تريد منحها للمستخدم (آخر 30 حزمة نشطة):", akb(rows))
    elif act == "gpk":
        pid = int(parts[4])
        pk = await db.one("packages", {"id": f"eq.{pid}"}, "id,title")
        await db.insert("user_subscriptions", {"telegram_id": uid, "package_id": pid, "price_paid": 0},
                        on_conflict="telegram_id,package_id", ignore_duplicates=True)
        await log_admin(aid, "grant_package", f"user={uid} package={pid}")
        await safe_send(uid, t(u.get("language"), "pkg_granted", title=esc(pk["title"]) if pk else pid))
        await cb.answer("✅ تم المنح")
        await user_card(cb, uid)
    elif act == "rv":
        subs = await db.select("user_subscriptions", "package_id,packages(title)",
                               {"telegram_id": f"eq.{uid}"}, limit=50)
        rows = [[ab("🔒 " + (s.get("packages") or {}).get("title", str(s["package_id"]))[:30],
                    f"a:us:{uid}:rvk:{s['package_id']}")] for s in subs]
        rows.append([ab("🔙 رجوع", f"a:us:{uid}")])
        await show(cb, "🔒 اختر الحزمة التي تريد سحبها من المستخدم:" if subs else "لا يملك المستخدم أي حزمة.",
                   akb(rows))
    elif act == "rvk":
        pid = int(parts[4])
        await db.delete("user_subscriptions", {"telegram_id": f"eq.{uid}", "package_id": f"eq.{pid}"})
        await log_admin(aid, "revoke_package", f"user={uid} package={pid}")
        await cb.answer("✅ تم السحب")
        await user_card(cb, uid)


# =====================================================================
#  ADMIN: settings + logs
# =====================================================================
SETTING_LABELS = {
    "maintenance": "🛠 وضع الصيانة",
    "referrals_enabled": "🔗 الإحالات",
    "gift_enabled": "🎁 هدية الاشتراك بالقنوات",
}
NUM_LABELS = {
    "referral_reward": "نقاط الإحالة",
    "channels_gift": "هدية القنوات",
    "new_badge_days": "أيام علامة «جديد»",
}


async def settings_view(ev: Any) -> None:
    rows = []
    for k, label in SETTING_LABELS.items():
        rows.append([ab(f"{label}: {'✅ مفعّل' if sbool(k) else '⬜ متوقف'}", f"a:se:t:{k}")])
    for k, label in NUM_LABELS.items():
        rows.append([ab(f"{label}: {sint(k)}  ✏️", f"a:se:n:{k}")])
    sup = support_user()
    rows.append([ab(f"🛟 يوزر الدعم: {('@' + sup) if sup else 'غير محدد'}  ✏️", "a:se:s:support_username")])
    rows.append(BACK_HOME)
    admins = "، ".join(f"<code>{a}</code>" for a in sorted(ADMIN_IDS))
    await show(ev, f"⚙️ <b>الإعدادات</b>\nاضغط لتغيير القيمة.\n\n👮 الأدمن: {admins}\n"
                   "(قائمة الأدمن والقنوات تُعدَّل من ملف bot.py)", akb(rows))


async def adm_se(cb: CallbackQuery, parts: list) -> None:
    aid = cb.from_user.id
    if len(parts) < 4:
        await settings_view(cb)
        return
    kind, key = parts[2], parts[3]
    if kind == "t" and key in SETTING_LABELS:
        new = "false" if sbool(key) else "true"
        await set_setting(key, new)
        await log_admin(aid, "setting", f"{key}={new}")
        await settings_view(cb)
    elif kind == "s" and key == "support_username":
        await set_astate(aid, "set_text", {"key": key})
        await show(cb, "🛟 أرسل يوزر حساب الدعم (مثال: @ti_tan_7).\nأرسل - لإخفاء زر الدعم.\n(أو /cancel)",
                   akb([[ab("❌ إلغاء", "a:se")]]))
    elif kind == "n" and key in NUM_LABELS:
        await set_astate(aid, "set_num", {"key": key})
        await show(cb, f"✏️ أرسل القيمة الجديدة لـ «{NUM_LABELS[key]}» (رقم صحيح)\n(أو /cancel)",
                   akb([[ab("❌ إلغاء", "a:se")]]))


async def adm_logs(cb: CallbackQuery) -> None:
    rows = await db.select("admin_logs", order="id.desc", limit=15)
    lines = ["🧾 <b>آخر العمليات</b>", ""]
    for r in rows:
        lines.append(f"{bahrain_time(r['created_at'])} • <code>{r.get('admin_id')}</code> • {esc(r['action'])} {esc(r.get('details') or '')[:60]}")
    if not rows:
        lines.append("لا توجد عمليات بعد.")
    await show(cb, "\n".join(lines), akb([BACK_HOME]))


# =====================================================================
#  ADMIN: callback dispatcher
# =====================================================================
@router.callback_query(F.data.startswith("a:"))
async def cb_admin(cb: CallbackQuery) -> None:
    aid = cb.from_user.id
    if aid not in ADMIN_IDS:
        await cb.answer("غير مصرح", show_alert=True)
        return
    parts = (cb.data or "").split(":")
    sec = parts[1] if len(parts) > 1 else "home"
    try:
        if sec == "home":
            await clear_astate(aid)
            await panel_home(cb)
        elif sec == "pk":
            await adm_pk(cb, parts)
        elif sec == "np":
            await adm_np(cb, parts)
        elif sec == "up":
            await adm_up(cb, parts)
        elif sec == "ct":
            await adm_ct(cb, parts)
        elif sec == "vp":
            await adm_vp(cb, parts)
        elif sec == "st":
            await adm_stats(cb, parts)
        elif sec == "bc":
            await adm_bc(cb, parts)
        elif sec == "us":
            await adm_us(cb, parts)
        elif sec == "se":
            await adm_se(cb, parts)
        elif sec == "cd":
            await adm_cd(cb, parts)
        elif sec == "lg":
            await adm_logs(cb)
    except DBError as e:
        log.error("admin action failed: %s", e)
        try:
            await cb.answer(f"خطأ في قاعدة البيانات: {str(e)[:150]}", show_alert=True)
        except Exception:
            pass
        return
    try:
        await cb.answer()
    except Exception:
        pass


# =====================================================================
#  ADMIN: promo (points) codes
# =====================================================================
_CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"


def gen_code() -> str:
    import secrets
    return "VX" + "".join(secrets.choice(_CODE_CHARS) for _ in range(6))


def _left_text(expires_at: str) -> str:
    try:
        d = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        secs = int((d - now_utc()).total_seconds())
    except Exception:
        return "-"
    if secs <= 0:
        return "منتهي"
    h, mnt = secs // 3600, (secs % 3600) // 60
    return f"{h}س {mnt}د" if h else f"{mnt}د"


async def codes_view(ev: Any) -> None:
    rows = await db._q("SELECT * FROM promo_codes ORDER BY created_at DESC LIMIT 10")
    lines = ["🎟 <b>أكواد النقاط</b>", "━━━━━━━━━━"]
    kb: list = []
    if not rows:
        lines.append("لا توجد أكواد بعد.")
    for r in rows:
        left = _left_text(r["expires_at"])
        live = bool(r["active"]) and left != "منتهي" and (not r["max_uses"] or r["used"] < r["max_uses"])
        uses = f"{r['used']}/{r['max_uses']}" if r["max_uses"] else f"{r['used']}/∞"
        lines.append(f"{'🟢' if live else '⚫'} <code>{esc(r['code'])}</code> • {r['points']} نقطة • "
                     f"استخدام {uses} • متبقي {left}")
        if live:
            kb.append([ab(f"⛔ إيقاف {r['code']}", f"a:cd:off:{r['code']}")])
    kb.append([ab("➕ كود جديد", "a:cd:new")])
    kb.append(BACK_HOME)
    await show(ev, "\n".join(lines), akb(kb))


async def adm_cd(cb: CallbackQuery, parts: list) -> None:
    aid = cb.from_user.id
    act = parts[2] if len(parts) > 2 else "list"
    if act == "list":
        await clear_astate(aid)
        await codes_view(cb)
    elif act == "new":
        await set_astate(aid, "cd_points", {})
        await show(cb, "🎟 <b>كود جديد</b>\n\n1/4 — أرسل <b>عدد النقاط</b> التي يمنحها الكود.\n(أو /cancel)",
                   akb([[ab("❌ إلغاء", "a:cd:list")]]))
    elif act == "off" and len(parts) > 3:
        await db.update("promo_codes", {"code": f"eq.{parts[3]}"}, {"active": 0})
        await log_admin(aid, "promo_code_off", parts[3])
        await codes_view(cb)


# =====================================================================
#  ADMIN: text input for multi-step flows
# =====================================================================
class AdminStateFilter(Filter):
    async def __call__(self, message: Message) -> Any:
        if not _is_admin_msg(message):
            return False
        txt = message.text or ""
        if txt.startswith("/") or txt in KB_TEXTS:
            return False
        row = await get_astate(message.from_user.id)
        return {"astate": row} if row else False


@router.message(AdminStateFilter())
async def on_admin_input(m: Message, astate: dict) -> None:
    aid = m.from_user.id
    st = astate["state"]
    data = astate.get("data") or {}
    txt = (m.text or "").strip()

    # ---- new package wizard
    if st == "np_name":
        if not txt or len(txt) > 100:
            await m.answer("الاسم مطلوب (حتى 100 حرف). أرسله من جديد.")
            return
        await set_astate(aid, "np_desc", {"cid": data["cid"], "name": txt})
        await m.answer("📝 أرسل <b>وصف الحزمة</b> (أو أرسل - للتخطي).")
        return
    if st == "np_desc":
        desc = None if txt in ("", "-") else txt[:500]
        await set_astate(aid, "np_price", {"cid": data["cid"], "name": data["name"], "desc": desc})
        await m.answer("💰 أرسل <b>السعر بالنقاط</b> (رقم، و0 = مجانية).")
        return
    if st == "np_price":
        v = norm_digits(txt)
        if not v.isdigit():
            await m.answer("أرسل رقماً صحيحاً فقط، مثل 150.")
            return
        await finish_new_package(m, data, int(v))
        return

    # ---- edit package fields
    if st == "edit":
        pid, field = data["pid"], data["field"]
        if field == "title":
            if not txt or len(txt) > 100:
                await m.answer("الاسم مطلوب (حتى 100 حرف).")
                return
            values: dict = {"title": txt}
        elif field == "desc":
            values = {"description": None if txt in ("", "-") else txt[:500]}
        else:
            v = norm_digits(txt)
            if not v.isdigit():
                await m.answer("أرسل رقماً صحيحاً فقط.")
                return
            values = {"price_in_points": int(v)}
        await db.update("packages", {"id": f"eq.{pid}"}, values)
        await clear_astate(aid)
        await log_admin(aid, "package_edit", f"{pid} {values}")
        await m.answer("✅ تم الحفظ.")
        await pkg_detail(m, pid)
        return

    # ---- categories
    if st == "cat_new":
        if not txt or len(txt) > 40:
            await m.answer("اسم الزر مطلوب (حتى 40 حرفاً).")
            return
        last = await db.one("categories", columns="sort_order", order="sort_order.desc")
        await db.insert("categories", {"name": txt, "sort_order": (last["sort_order"] if last else 0) + 1})
        await clear_astate(aid)
        await log_admin(aid, "category_created", txt)
        await m.answer("✅ تم إنشاء الزر.")
        await ct_list(m)
        return
    if st == "cat_ren":
        if not txt or len(txt) > 40:
            await m.answer("اسم الزر مطلوب (حتى 40 حرفاً).")
            return
        await db.update("categories", {"id": f"eq.{data['cid']}"}, {"name": txt})
        await clear_astate(aid)
        await log_admin(aid, "category_renamed", f"{data['cid']} {txt}")
        await ct_detail(m, data["cid"])
        return

    # ---- manual ordering inside a package
    if st == "vids":
        pid, page = data["pid"], data.get("page", 0)
        raw = norm_digits(txt)
        ids = [v["id"] for v in await db.select("videos", "id", {"package_id": f"eq.{pid}"},
                                               order="sort_order.asc,id.asc", limit=5000)]
        mv = re.match(r"^(\d+)\s*(?:>|->|→|الى|إلى)\s*(\d+)$", raw)
        rn = re.match(r"^[rR]\s+(\d+)\s+(.+)$", raw, re.S)
        dl = re.match(r"^[dD]\s+(\d+)$", raw)
        pg = re.match(r"^[pP]\s*(\d+)$", raw)
        note = ""
        if mv:
            ok = await db.rpc("move_video", {"p_package": pid, "p_from": int(mv.group(1)), "p_to": int(mv.group(2))})
            note = "✅ تم النقل." if ok else "⚠️ رقم غير صحيح."
            if ok:
                await log_admin(aid, "video_moved", f"pkg={pid} {mv.group(1)}->{mv.group(2)}")
        elif rn:
            n = int(rn.group(1))
            if 1 <= n <= len(ids):
                await db.update("videos", {"id": f"eq.{ids[n - 1]}"}, {"title": rn.group(2).strip()[:100]})
                note = "✅ تم تغيير الاسم."
            else:
                note = "⚠️ رقم غير صحيح."
        elif dl:
            n = int(dl.group(1))
            if 1 <= n <= len(ids):
                await db.delete("videos", {"id": f"eq.{ids[n - 1]}"})
                await db.rpc("renumber_videos", {"p_package": pid})
                await log_admin(aid, "video_deleted", f"pkg={pid} n={n}")
                note = "✅ تم حذف المقطع (يبقى في القناة)."
            else:
                note = "⚠️ رقم غير صحيح."
        elif pg:
            page = max(int(pg.group(1)) - 1, 0)
        else:
            await m.answer("لم أفهم الأمر. أمثلة: <code>5>2</code> • <code>r 5 اسم</code> • <code>d 5</code> • <code>p 2</code>")
            return
        await set_astate(aid, "vids", {"pid": pid, "page": page})
        text, kb = await vids_view(pid, page)
        await m.answer((note + "\n\n" if note else "") + text, reply_markup=kb)
        return

    # ---- user management
    if st == "user_find":
        v = norm_digits(txt)
        if not v.isdigit():
            await m.answer("أرسل الـ ID الرقمي فقط.")
            return
        await clear_astate(aid)
        await user_card(m, int(v))
        return
    if st == "pts":
        v = norm_digits(txt)
        if not v.isdigit() or int(v) == 0:
            await m.answer("أرسل رقماً صحيحاً أكبر من 0.")
            return
        uid, sign = data["uid"], data["sign"]
        res = await db.rpc("add_points", {"p_user": uid, "p_amount": sign * int(v), "p_type": "admin_adjust",
                                          "p_ref": None, "p_note": f"by admin {aid}"})
        await clear_astate(aid)
        if res and res.get("ok"):
            await log_admin(aid, "points_adjust", f"user={uid} {sign * int(v)} -> {res['balance']}")
            u = await get_user(uid, fresh=True)
            key = "pts_added" if sign > 0 else "pts_removed"
            await safe_send(uid, t((u or {}).get("language"), key, n=int(v), balance=res["balance"]))
            await m.answer(f"✅ تم. الرصيد الآن: <b>{res['balance']}</b>")
        else:
            err = (res or {}).get("error", "unknown")
            await m.answer("⚠️ رصيد المستخدم لا يكفي للخصم." if err == "insufficient" else f"⚠️ تعذر التنفيذ: {err}")
        await user_card(m, uid)
        return
    if st == "msg_user":
        uid = data["uid"]
        try:
            await bot.copy_message(uid, m.chat.id, m.message_id)
            await m.answer("✅ تم إرسال الرسالة.")
        except TelegramAPIError as e:
            await m.answer(f"⚠️ تعذر الإرسال: {esc(str(e))[:150]}")
        await clear_astate(aid)
        return

    # ---- broadcast content
    if st == "bc_content":
        ids = await audience_ids(data["aud"])
        await set_astate(aid, "bc_confirm", {"aud": data["aud"], "src_chat": m.chat.id,
                                             "src_msg": m.message_id, "n": len(ids)})
        try:
            await bot.copy_message(m.chat.id, m.chat.id, m.message_id)
        except TelegramAPIError:
            pass
        await m.answer(f"👆 هذه معاينة الرسالة.\nالجمهور: <b>{AUDIENCES[data['aud']]}</b>\n"
                       f"عدد المستلمين المتوقع: <b>{len(ids)}</b>",
                       reply_markup=akb([[ab("✅ إرسال", "a:bc:go"), ab("❌ إلغاء", "a:bc:cancel")]]))
        return

    # ---- promo code wizard
    if st == "cd_points":
        v = norm_digits(txt)
        if not v.isdigit() or not (0 < int(v) <= 1000000):
            await m.answer("أرسل رقماً صحيحاً أكبر من 0.")
            return
        await set_astate(aid, "cd_minutes", {"points": int(v)})
        await m.answer("2/4 — أرسل <b>مدة الصلاحية بالدقائق</b> (مثال: 30 = نصف ساعة، 1440 = يوم).")
        return
    if st == "cd_minutes":
        v = norm_digits(txt)
        if not v.isdigit() or not (0 < int(v) <= 525600):
            await m.answer("أرسل عدد الدقائق كرقم صحيح (حتى 525600).")
            return
        await set_astate(aid, "cd_uses", {"points": data["points"], "minutes": int(v)})
        await m.answer("3/4 — أرسل <b>أقصى عدد مستخدمين</b> للكود (0 = بلا حد).")
        return
    if st == "cd_uses":
        v = norm_digits(txt)
        if not v.isdigit() or int(v) > 10000000:
            await m.answer("أرسل رقماً صحيحاً (0 = بلا حد).")
            return
        await set_astate(aid, "cd_code", {"points": data["points"], "minutes": data["minutes"], "uses": int(v)})
        await m.answer("4/4 — أرسل <b>نص الكود</b> الذي تريده (أحرف وأرقام إنجليزية، 4–20)، "
                       "أو أرسل - ليُولَّد كود عشوائي.")
        return
    if st == "cd_code":
        if txt == "-":
            code = gen_code()
        else:
            code = re.sub(r"\s+", "", txt).upper()
            if not re.fullmatch(r"[A-Z0-9_-]{4,20}", code):
                await m.answer("الكود يجب أن يكون أحرفاً/أرقاماً إنجليزية من 4 إلى 20. أرسله من جديد أو أرسل -")
                return
        exp = iso(now_utc() + timedelta(minutes=data["minutes"]))
        try:
            await db.insert("promo_codes", {"code": code, "points": data["points"], "max_uses": data["uses"],
                                            "expires_at": exp, "created_by": aid})
        except DBError:
            await m.answer("⚠️ هذا الكود موجود مسبقاً. أرسل نصاً آخر أو أرسل -")
            return
        await clear_astate(aid)
        await log_admin(aid, "promo_code_created", f"{code} pts={data['points']} min={data['minutes']} uses={data['uses']}")
        mins = data["minutes"]
        dur = (f"{mins // 1440} يوم" if mins % 1440 == 0 else f"{mins // 60} ساعة" if mins % 60 == 0 else f"{mins} دقيقة")
        uses_t = f"لأول {data['uses']} مستخدم" if data["uses"] else "بلا حد للمستخدمين"
        await m.answer(
            "✅ تم إنشاء الكود:\n\n"
            f"<code>{esc(code)}</code>\n"
            f"💰 {data['points']} نقطة • ⏳ {dur} • 👥 {uses_t}\n\n"
            "📋 منشور جاهز للقناة:\n"
            f"🎁 كود نقاط جديد!\n<code>{esc(code)}</code>\n"
            f"صالح لمدة {dur} ({uses_t}) 🔥\n"
            "ادخل البوت واضغط «إستخدم كود 🎟» وأرسل الكود 👇🏻",
            reply_markup=akb([[ab("🎟 أكواد النقاط", "a:cd:list")], BACK_HOME]))
        return

    # ---- text settings (support username)
    if st == "set_text":
        v = "" if txt == "-" else txt.lstrip("@")
        if v and not re.fullmatch(r"[A-Za-z0-9_]{4,32}", v):
            await m.answer("يوزر غير صالح (أحرف إنجليزية وأرقام و_ من 4 إلى 32). أعد الإرسال أو أرسل -")
            return
        await set_setting("support_username", v)
        await clear_astate(aid)
        await log_admin(aid, "setting", f"support_username={v or '-'}")
        await settings_view(m)
        return

    # ---- numeric settings
    if st == "set_num":
        v = norm_digits(txt)
        if not v.isdigit():
            await m.answer("أرسل رقماً صحيحاً فقط.")
            return
        await set_setting(data["key"], str(int(v)))
        await clear_astate(aid)
        await log_admin(aid, "setting", f"{data['key']}={int(v)}")
        await settings_view(m)
        return


# =====================================================================
#  errors + startup
# =====================================================================
@router.errors()
async def on_error(ev: ErrorEvent) -> bool:
    exc = ev.exception
    log.exception("handler error: %r", exc)
    await alert_admins(f"⚠️ خطأ في البوت:\n<code>{esc(repr(exc))[:350]}</code>",
                       key=f"err:{type(exc).__name__}", every=300)
    return True


async def main() -> None:
    global db, bot, BOT_ID, thumb_queue
    missing = [n for n, v in (("BOT_TOKEN", BOT_TOKEN), ("TURSO_DATABASE_URL", TURSO_DATABASE_URL),
                              ("TURSO_AUTH_TOKEN", TURSO_AUTH_TOKEN)) if not v]
    if missing:
        raise SystemExit("Missing environment variables: " + ", ".join(missing))

    db = DB(TURSO_DATABASE_URL, TURSO_AUTH_TOKEN)
    await db.init_schema()
    bot = Bot(token=BOT_TOKEN,
              default=DefaultBotProperties(parse_mode=ParseMode.HTML, link_preview_is_disabled=True))
    me = await bot.get_me()
    BOT_ID = me.id
    log.info("Starting @%s (id %s)", me.username, me.id)

    await load_settings()
    try:
        await db.update("broadcasts", {"status": "eq.running"}, {"status": "interrupted"})
    except Exception as e:
        log.warning("startup cleanup failed: %s", e)

    thumb_queue = asyncio.Queue()
    ctx = keep_alive.Ctx(
        db=db, bot=bot, token=BOT_TOKEN, storage_channel_id=STORAGE_CHANNEL_ID,
        admin_ids=ADMIN_IDS, payment_bot_username=PAYMENT_BOT_USERNAME,
        support_username=SUPPORT_USERNAME, bot_username=BOT_USERNAME,
        setting_int=sint, setting_bool=sbool, alert_admins=alert_admins,
    )
    runner = await keep_alive.start_web(ctx, PORT)

    dp = Dispatcher()
    dp.message.outer_middleware(Throttle())
    dp.callback_query.outer_middleware(Throttle())
    dp.include_router(router)

    bg = [asyncio.create_task(thumb_worker()), asyncio.create_task(settings_loop())]
    try:
        # make sure we poll (a leftover webhook would block long polling)
        await bot.delete_webhook(drop_pending_updates=False)
        await setup_commands()
        if not await channel_ok():
            await alert_admins("⚠️ قناة التخزين غير متاحة: تأكد أن البوت مشرف فيها وأن المعرّف صحيح.",
                               key="storage-start", every=3600)
        await dp.start_polling(
            bot, allowed_updates=["message", "callback_query", "channel_post", "my_chat_member"])
    finally:
        for task in bg:
            task.cancel()
        await runner.cleanup()
        await db.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
