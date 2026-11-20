# -*- coding: utf-8 -*-
"""
Vidox | فيديوكس  -  channel-selling bot (v2)
Shows paid private channels (prices in Telegram Stars), delivers a personal join-request link after payment,
runs the "win a channel" referral contest, a Mini App showcase, a trust channel and daily DB backups.
The Stars payment itself is handled by the separate payment bot (same Turso database).

ENVIRONMENT (Render):
    BOT_TOKEN            - token of THIS bot
    TURSO_DATABASE_URL   - Turso database url
    TURSO_AUTH_TOKEN     - Turso auth token
  optional:
    ADMIN_IDS            - extra admin ids (comma separated); the main admin is hardcoded below
    WEBAPP_URL           - Mini App url (defaults to RENDER_EXTERNAL_URL + /app)
    RESTORE_WORD         - the word the admin writes as caption when sending a backup file to restore it
"""
import asyncio
import base64
import gzip
import html
import io
import json
import logging
import os
import re
import secrets
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import httpx
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import (TelegramAPIError, TelegramBadRequest, TelegramForbiddenError,
                                TelegramRetryAfter)
from aiogram.filters import Command, CommandObject, CommandStart, Filter
from aiogram.types import (BotCommand, BotCommandScopeChat, BufferedInputFile, CallbackQuery, ChatJoinRequest,
                           ChatMemberUpdated, ErrorEvent, InlineKeyboardButton, InlineKeyboardMarkup, InlineQuery,
                           InlineQueryResultArticle, InputTextMessageContent, MenuButtonWebApp, KeyboardButton, Message,
                           ReplyKeyboardMarkup, WebAppInfo)

import keep_alive

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("vidox")

# ----------------------------------------------------------------- config (hardcoded, not secret)
ADMIN_IDS = {8902791537, 6043858925}
BOT_USERNAME = "vdox_2_bot"

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
TURSO_DATABASE_URL = os.getenv("TURSO_DATABASE_URL", "").strip()
TURSO_AUTH_TOKEN = os.getenv("TURSO_AUTH_TOKEN", "").strip()
RESTORE_WORD = os.getenv("RESTORE_WORD", "استرجاع").strip()
PORT = int(os.getenv("PORT", "10000"))
_ext = os.getenv("RENDER_EXTERNAL_URL", "").strip().rstrip("/")
WEBAPP_URL = os.getenv("WEBAPP_URL", "").strip() or (f"{_ext}/app" if _ext else "")
for _p in re.split(r"[,\s;]+", os.getenv("ADMIN_IDS", "")):
    if _p.strip().isdigit():
        ADMIN_IDS.add(int(_p))

esc = html.escape


def webapp_url() -> str:
    return (S("webapp_url").strip() or WEBAPP_URL).strip()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")


def iso_now() -> str:
    return iso(now_utc())


def bahrain_time(dt_str: Optional[str]) -> str:
    if not dt_str:
        return "-"
    try:
        d = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return (d + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return dt_str


_DIG = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")


def norm_digits(s: str) -> str:
    return (s or "").translate(_DIG)


def mask_name(name: str) -> str:
    """'أحمد علي' -> 'أ*** ع**' (never shows the full name)"""
    words = [w for w in (name or "").split() if w][:2] or ["***"]
    return " ".join(w[0] + "*" * min(max(len(w) - 1, 2), 4) for w in words)


def fmt_size(n: int) -> str:
    n = int(n or 0)
    if n >= 1 << 30:
        return f"{n / (1 << 30):.1f} GB"
    if n >= 1 << 20:
        return f"{n / (1 << 20):.0f} MB"
    return f"{max(n // 1024, 1)} KB" if n else "-"


def fmt_dur(s: int) -> str:
    s = int(s or 0)
    h, m, sec = s // 3600, (s % 3600) // 60, s % 60
    return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"


# =====================================================================
#  Turso (libSQL) over HTTP
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


    # --------------------------------------------------------- thumbnails
    async def save_thumb(self, key: str, data: bytes) -> None:
        await self._run([("INSERT OR REPLACE INTO thumbs (key, data) VALUES (?, ?)", [key, data])])

    async def get_thumb(self, key: str) -> Optional[bytes]:
        rows = await self._q("SELECT data FROM thumbs WHERE key = ?", [key])
        return rows[0]["data"] if rows else None

    async def close(self) -> None:
        await self.client.aclose()


# =====================================================================
#  schema (fresh start: channel-selling platform)
# =====================================================================
SCHEMA = [
    f"""CREATE TABLE IF NOT EXISTS users (
        telegram_id INTEGER PRIMARY KEY, username TEXT, full_name TEXT,
        language TEXT NOT NULL DEFAULT 'ar', consent_at TEXT,
        interactions INTEGER NOT NULL DEFAULT 0, referred_by INTEGER, ref_counted INTEGER NOT NULL DEFAULT 0,
        contest_ok INTEGER NOT NULL DEFAULT 0, pending TEXT,
        is_banned INTEGER NOT NULL DEFAULT 0, is_active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL DEFAULT ({_NOW_SQL}), last_active_at TEXT NOT NULL DEFAULT ({_NOW_SQL}))""",
    "CREATE INDEX IF NOT EXISTS users_ref_idx ON users (referred_by)",
    f"""CREATE TABLE IF NOT EXISTS channels (
        id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL UNIQUE,
        title TEXT NOT NULL DEFAULT '', description TEXT, custom_desc TEXT, button_name TEXT NOT NULL DEFAULT '',
        username TEXT, members INTEGER NOT NULL DEFAULT 0, price_stars INTEGER NOT NULL DEFAULT 0,
        contest INTEGER NOT NULL DEFAULT 0, pinned INTEGER NOT NULL DEFAULT 0, active INTEGER NOT NULL DEFAULT 1,
        buy_color TEXT NOT NULL DEFAULT 'success', name_color TEXT NOT NULL DEFAULT 'primary',
        join_link TEXT, invoice_url TEXT, invoice_price INTEGER, photo_fid TEXT, has_photo INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT ({_NOW_SQL}))""",
    f"""CREATE TABLE IF NOT EXISTS channel_media (
        id INTEGER PRIMARY KEY AUTOINCREMENT, channel_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
        title TEXT, duration INTEGER NOT NULL DEFAULT 0, size INTEGER NOT NULL DEFAULT 0, thumb_fid TEXT,
        created_at TEXT NOT NULL DEFAULT ({_NOW_SQL}), UNIQUE (channel_id, message_id))""",
    "CREATE TABLE IF NOT EXISTS thumbs (key TEXT PRIMARY KEY, data BLOB NOT NULL)",
    # shared with the payment bot: it inserts paid orders, this bot delivers them
    f"""CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT, telegram_id INTEGER NOT NULL, channel_id INTEGER NOT NULL,
        stars INTEGER NOT NULL DEFAULT 0, charge_id TEXT NOT NULL UNIQUE, provider_charge_id TEXT,
        source TEXT NOT NULL DEFAULT 'stars', status TEXT NOT NULL DEFAULT 'paid' CHECK (status IN ('paid','refunded')),
        delivered INTEGER NOT NULL DEFAULT 0, trust_posted INTEGER NOT NULL DEFAULT 0, revoked INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT ({_NOW_SQL}), refunded_at TEXT)""",
    "CREATE INDEX IF NOT EXISTS orders_user_idx ON orders (telegram_id)",
    "CREATE INDEX IF NOT EXISTS orders_channel_idx ON orders (channel_id)",
    "CREATE INDEX IF NOT EXISTS orders_pending_idx ON orders (delivered, status)",
    f"""CREATE TABLE IF NOT EXISTS contest_claims (
        id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, seq INTEGER NOT NULL, channel_id INTEGER NOT NULL,
        created_at TEXT NOT NULL DEFAULT ({_NOW_SQL}), UNIQUE (user_id, seq))""",
    f"""CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, channel_id INTEGER, kind TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT ({_NOW_SQL}))""",
    "CREATE INDEX IF NOT EXISTS events_idx ON events (created_at, kind)",
    "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS sub_channels (id INTEGER PRIMARY KEY AUTOINCREMENT, chat TEXT NOT NULL, url TEXT NOT NULL, title TEXT NOT NULL DEFAULT '')",
    "CREATE TABLE IF NOT EXISTS admin_state (admin_id INTEGER PRIMARY KEY, state TEXT NOT NULL, data TEXT NOT NULL DEFAULT '{}')",
    f"""CREATE TABLE IF NOT EXISTS admin_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, admin_id INTEGER, action TEXT NOT NULL, details TEXT,
        created_at TEXT NOT NULL DEFAULT ({_NOW_SQL}))""",
    f"""CREATE TABLE IF NOT EXISTS broadcasts (
        id INTEGER PRIMARY KEY AUTOINCREMENT, admin_id INTEGER NOT NULL, audience TEXT NOT NULL,
        src_chat INTEGER NOT NULL, src_message INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'running',
        total INTEGER NOT NULL DEFAULT 0, sent INTEGER NOT NULL DEFAULT 0, failed INTEGER NOT NULL DEFAULT 0,
        blocked INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL DEFAULT ({_NOW_SQL}), finished_at TEXT)""",
    """CREATE TABLE IF NOT EXISTS broadcast_recipients (
        broadcast_id INTEGER NOT NULL, telegram_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
        PRIMARY KEY (broadcast_id, telegram_id))""",
]

SETTING_DEFAULTS = {
    "maintenance": "false",
    "brand_name": "Vidox",
    "webapp_url": "",
    "menu_text": "فتح",
    "shop_text": "",
    "trend_top": "3",
    "mig_v3": "0",
    "kb_app": "", "kb_win": "", "kb_mine": "", "kb_lang": "", "kb_support": "", "kb_help": "",
    "off_kb_app": "false", "off_kb_win": "false", "off_kb_mine": "false", "off_kb_lang": "false", "off_kb_support": "false", "off_kb_help": "false",
    "support_username": "ti_tan_7",
    "trust_chat": "",
    "trust_url": "",
    "contest_enabled": "true",
    "contest_target": "10",
    "contest_max": "1",
    "share_text": "",
    "last_backup_at": "0",
}
_settings: dict = {}


def S(key: str) -> str:
    v = _settings.get(key)
    return v if v is not None else SETTING_DEFAULTS.get(key, "")


def SI(key: str) -> int:
    try:
        return int(S(key))
    except ValueError:
        return 0


async def load_settings() -> None:
    try:
        rows = await db._q("SELECT key, value FROM settings")
        _settings.clear()
        _settings.update({r["key"]: r["value"] for r in rows})
    except Exception as e:
        log.warning("load_settings failed: %s", e)


async def set_setting(key: str, value: str) -> None:
    await db._run([("INSERT INTO settings (key, value) VALUES (?, ?) "
                    "ON CONFLICT (key) DO UPDATE SET value = excluded.value", [key, value])])
    _settings[key] = value


# =====================================================================
#  texts
# =====================================================================
TX = {
    "ar": {
        "consent": "🔞 <b>للبالغين فقط (+18)</b>\nبالمتابعة تؤكد أن عمرك 18 سنة أو أكثر وتوافق على /terms و/privacy",
        "consent_yes": "أنا +18 وأوافق ✅", "consent_no_btn": "عمري أقل ❌",
        "consent_no": "عذراً، البوت للبالغين فقط.",
        "shop": "تصفّح المحتوى من الأزرار 👇",
        "shop_empty": "لا توجد قنوات حالياً.",
        "menu_hint": "أهلاً بك 👋",
        "kb_app": "محتوى القنوات 🎬", "kb_win": "اربح قناة 🎁", "kb_mine": "قنواتي 📦",
        "kb_lang": "اللغة 🌐", "kb_support": "الدعم 🛟", "kb_help": "التعليمات 💡",
        "prev": "◀️", "next": "▶️", "back": "رجوع",
        "buy": "اشترِ الآن ⭐ {p}", "open": "فتح ✅", "open_ch": "فتح القناة ✅", "preparing": "⏳ رابط الدفع قيد التجهيز، حاول بعد قليل.",
        "more": "المزيد من التفاصيل", "trust": "🛡 دليل الثقة",
        "card": "🎬 <b>{title}</b>\n{desc}\n\n⭐ <b>{price}</b>  ·  👥 {members}  ·  🎞 {count}\n<i>💾 المحتوى مفتوح للحفظ · لا استرداد بعد الشراء</i>",
        "card_owned": "\n\n✅ <b>تملك هذه القناة</b>",
        "free_price": "مجانية",
        "delivered": "✅ <b>تم الشراء</b>\n{title}",
        "join_denied": "🔒 هذه القناة للمشترين فقط.",
        "mine_empty": "لا تملك قنوات بعد.",
        "mine": "📦 قنواتك:",
        "lang_pick": "🌐",
        "banned": "⛔ حسابك محظور.", "maint": "🛠 صيانة قصيرة، عد بعد قليل.",
        "win_off": "🎁 المسابقة متوقفة حالياً.",
        "win_sub": "🎁 <b>اربح قناة مجاناً</b>\nاشترك في القنوات ثم اضغط تحقق:",
        "verify": "تحقق ✅", "sub_no": "اشترك في كل القنوات أولاً.", "sub_err": "تعذر التحقق الآن، حاول بعد قليل.",
        "win": "🎁 <b>اربح قناة مجاناً</b>\nادعُ {target} أصدقاء برابطك.\n\n{bar} <b>{valid}/{target}</b>\n🏆 جوائز جاهزة: <b>{avail}</b>\n\n<code>{link}</code>",
        "win_max": "\n✅ وصلت للحد الأقصى من الجوائز.",
        "share": "📤 شارك الرابط", "claim": "🎁 استلم قناتك", "refresh": "🔄 تحديث",
        "pick": "اختر قناتك:", "pick_none": "لا توجد قنوات للمسابقة الآن.",
        "claimed": "🎉 مبروك! يصلك الرابط خلال لحظات.",
        "claim_no": "لا توجد جوائز متاحة لك.",
        "ref_joined": "👤 انضم صديق ✅  <b>{valid}/{target}</b>",
        "ref_done": "\n🎉 اكتمل الهدف! افتح «اربح قناة» لاستلام جائزتك.",
        "share_default": "🎁 قنوات مميزة بنجوم تيليجرام\nانضم عبر رابطي 👇",
        "share_title": "شارك الرابط", "share_locked": "اشترك في قنوات المسابقة من البوت أولاً",
        "support": "🛟 تحتاج مساعدة؟", "support_btn": "تواصل مع الدعم", "support_none": "الدعم غير متاح حالياً.",
        "help": "1) اختر قناة من الأزرار\n2) اشترِ بنجوم تيليجرام ⭐\n3) يصلك رابط دخولك فوراً\n\nمشكلة؟ /support",
        "terms": "📜 <b>الشروط</b>\n• للبالغين فقط (18+).\n• الدفع بنجوم تيليجرام وتصلك القناة فوراً.\n• المحتوى مفتوح للحفظ، لذلك لا استرداد بعد الشراء إلا عند تعذّر وصول الرابط.\n• الرابط خاص بحسابك.\n• للمشاكل: /support",
        "privacy": "🔒 <b>الخصوصية</b>\nنحفظ فقط: الآيدي والاسم واللغة وموافقتك ومشترياتك والإحالات.\nلا نبيع بياناتك ولا نطلب كلمات مرور.\nيظهر اسمك مُقنَّعاً في قناة الثقة عند الشراء.\nلحذف بياناتك: /support",
    },
    "en": {
        "consent": "🔞 <b>Adults only (18+)</b>\nBy continuing you confirm you are 18 or older and accept /terms and /privacy",
        "consent_yes": "I'm 18+ and agree ✅", "consent_no_btn": "I'm under 18 ❌",
        "consent_no": "Sorry, this bot is for adults only.",
        "shop": "Browse from the buttons 👇",
        "shop_empty": "No channels yet.",
        "menu_hint": "Welcome 👋",
        "kb_app": "Channels content 🎬", "kb_win": "Win a channel 🎁", "kb_mine": "My channels 📦",
        "kb_lang": "Language 🌐", "kb_support": "Support 🛟", "kb_help": "Guide 💡",
        "prev": "◀️", "next": "▶️", "back": "Back",
        "buy": "Buy now ⭐ {p}", "open": "Open ✅", "open_ch": "Open channel ✅", "preparing": "⏳ Payment link is being prepared, try again shortly.",
        "more": "More details", "trust": "🛡 Trust proof",
        "card": "🎬 <b>{title}</b>\n{desc}\n\n⭐ <b>{price}</b>  ·  👥 {members}  ·  🎞 {count}\n<i>💾 Content is open to save · no refunds after purchase</i>",
        "card_owned": "\n\n✅ <b>You own this channel</b>",
        "free_price": "Free",
        "delivered": "✅ <b>Purchase complete</b>\n{title}",
        "join_denied": "🔒 This channel is for buyers only.",
        "mine_empty": "You don't own any channel yet.",
        "mine": "📦 Your channels:",
        "lang_pick": "🌐",
        "banned": "⛔ Your account is banned.", "maint": "🛠 Short maintenance, come back soon.",
        "win_off": "🎁 The contest is paused.",
        "win_sub": "🎁 <b>Win a channel for free</b>\nJoin the channels, then tap Verify:",
        "verify": "Verify ✅", "sub_no": "Join all the channels first.", "sub_err": "Can't verify now, try again shortly.",
        "win": "🎁 <b>Win a channel for free</b>\nInvite {target} friends with your link.\n\n{bar} <b>{valid}/{target}</b>\n🏆 Rewards ready: <b>{avail}</b>\n\n<code>{link}</code>",
        "win_max": "\n✅ You reached the maximum rewards.",
        "share": "📤 Share link", "claim": "🎁 Claim your channel", "refresh": "🔄 Refresh",
        "pick": "Pick your channel:", "pick_none": "No contest channels right now.",
        "claimed": "🎉 Congrats! Your link arrives in a moment.",
        "claim_no": "No rewards available.",
        "ref_joined": "👤 A friend joined ✅  <b>{valid}/{target}</b>",
        "ref_done": "\n🎉 Goal reached! Open “Win a channel” to claim.",
        "share_default": "🎁 Premium channels with Telegram Stars\nJoin with my link 👇",
        "share_title": "Share link", "share_locked": "Join the contest channels in the bot first",
        "support": "🛟 Need help?", "support_btn": "Contact support", "support_none": "Support is unavailable now.",
        "help": "1) Pick a channel from the buttons\n2) Pay with Telegram Stars ⭐\n3) Get your join link instantly\n\nProblem? /support",
        "terms": "📜 <b>Terms</b>\n• Adults only (18+).\n• Paid in Telegram Stars, delivered instantly.\n• Content is open to save, so no refunds after purchase unless the link failed to reach you.\n• Your link is tied to your account.\n• Issues: /support",
        "privacy": "🔒 <b>Privacy</b>\nWe store only: id, name, language, your consent, purchases and referrals.\nWe never sell your data or ask for passwords.\nYour name appears masked in the trust channel when you buy.\nTo delete your data: /support",
    },
}


def t(lang: Optional[str], key: str, **kw: Any) -> str:
    s = TX.get(lang or "ar", TX["ar"]).get(key) or TX["ar"][key]
    s = s.replace("{brand}", S("brand_name"))
    return s.format(**kw) if kw else s


DEFAULT_SUBS = [
    {"chat": "@ZenoX_Tools", "url": "https://t.me/ZenoX_Tools", "title": "ZenoX Tools"},
    {"chat": "@Videox_1", "url": "https://t.me/Videox_1", "title": "Videox"},
]
GRID_COLS = 4          # small buttons per row
GRID_ROWS = 8          # rows per page
COLORS = {"success": "🟢 أخضر", "primary": "🔵 أزرق", "danger": "🔴 أحمر", "default": "⚪ عادي"}
KB_KEYS = ["kb_app", "kb_win", "kb_mine", "kb_lang", "kb_support", "kb_help"]


# =====================================================================
#  globals / helpers
# =====================================================================
router = Router()
db: DB
bot: Bot
BOT_ID = 0
_wake = asyncio.Event()
_alert_last: dict = {}


def ibtn(text: str, *, data: Optional[str] = None, url: Optional[str] = None, web: Optional[str] = None,
         switch: Optional[str] = None, style: Optional[str] = None) -> InlineKeyboardButton:
    kw: dict = {"text": text}
    if data is not None:
        kw["callback_data"] = data
    if url:
        kw["url"] = url
    if web:
        kw["web_app"] = WebAppInfo(url=web)
    if switch is not None:
        kw["switch_inline_query"] = switch
    if style in ("primary", "success", "danger"):
        try:
            return InlineKeyboardButton(**kw, style=style)
        except Exception:
            pass
    return InlineKeyboardButton(**kw)


def kb(rows: list) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=rows)


def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS


async def safe_send(chat_id: int, text: str, **kw: Any) -> Optional[Message]:
    for _ in range(2):
        try:
            return await bot.send_message(chat_id, text, **kw)
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 0.5)
        except TelegramAPIError as e:
            log.debug("send failed %s: %s", chat_id, e)
            return None
    return None


async def alert_admins(text: str, key: Optional[str] = None) -> None:
    if key:
        if time.time() - _alert_last.get(key, 0) < 600:
            return
        _alert_last[key] = time.time()
    for a in ADMIN_IDS:
        await safe_send(a, text)


async def log_admin(aid: Optional[int], action: str, details: str = "") -> None:
    try:
        await db._run([("INSERT INTO admin_logs (admin_id, action, details) VALUES (?, ?, ?)", [aid, action, details[:500]])])
    except Exception as e:
        log.debug("log failed: %s", e)


async def track(uid: Optional[int], channel_id: Optional[int], kind: str) -> None:
    try:
        await db._run([("INSERT INTO events (user_id, channel_id, kind) VALUES (?, ?, ?)", [uid, channel_id, kind])])
    except Exception:
        pass


def support_link() -> str:
    s = S("support_username").strip().lstrip("@")
    return f"https://t.me/{s}" if s else ""


def bot_link(payload: str = "") -> str:
    return f"https://t.me/{BOT_USERNAME}" + (f"?start={payload}" if payload else "")


async def get_user(uid: int) -> Optional[dict]:
    rows = await db._q("SELECT * FROM users WHERE telegram_id = ?", [uid])
    return rows[0] if rows else None


async def register_user(u: Any, ref: Optional[int]) -> dict:
    lang = "ar" if (u.language_code or "").lower().startswith("ar") else "en"
    if ref and ref != u.id:
        if not (await db._q("SELECT 1 AS x FROM users WHERE telegram_id = ?", [ref])):
            ref = None
    else:
        ref = None
    await db._run([("INSERT OR IGNORE INTO users (telegram_id, username, full_name, language, referred_by) "
                    "VALUES (?, ?, ?, ?, ?)", [u.id, u.username, u.full_name, lang, ref])])
    return (await get_user(u.id)) or {}


async def count_referral(user: dict) -> None:
    """The invited user reached 3 real interactions: count him for the inviter (once)."""
    try:
        res = await db._run([("UPDATE users SET ref_counted = 1 WHERE telegram_id = ? AND ref_counted = 0",
                              [user["telegram_id"]])])
        if not res[0]["affected"]:
            return
        ref = user["referred_by"]
        valid = (await db._q("SELECT COUNT(*) AS n FROM users WHERE referred_by = ? AND ref_counted = 1", [ref]))[0]["n"]
        target = max(SI("contest_target"), 1)
        ru = await get_user(ref)
        if ru and S("contest_enabled") == "true":
            txt = t(ru["language"], "ref_joined", valid=valid, target=target)
            if valid >= target and (valid - 1) // target < valid // target:
                txt += t(ru["language"], "ref_done")
            await safe_send(ref, txt)
    except Exception as e:
        log.warning("count_referral failed: %s", e)


async def gate(ev: Any, need_consent: bool = True) -> Optional[dict]:
    """One round-trip: count the interaction and load the user. Handles ban / maintenance / consent."""
    u = ev.from_user
    if not u or u.is_bot:
        return None
    res = await db._run([
        (f"UPDATE users SET interactions = interactions + 1, last_active_at = {_NOW_SQL}, is_active = 1, "
         "username = ?, full_name = ? WHERE telegram_id = ?", [u.username, u.full_name, u.id]),
        ("SELECT * FROM users WHERE telegram_id = ?", [u.id]),
    ])
    rows = res[1]["rows"]
    user = rows[0] if rows else await register_user(u, None)
    if user.get("interactions", 0) >= 3 and user.get("referred_by") and not user.get("ref_counted"):
        asyncio.create_task(count_referral(user))
    lang = user.get("language", "ar")

    async def say(text: str) -> None:
        if isinstance(ev, CallbackQuery):
            await ev.answer(re.sub(r"<[^>]+>", "", text), show_alert=True)
        else:
            await ev.answer(text)

    if user.get("is_banned") and not is_admin(u.id):
        await say(t(lang, "banned"))
        return None
    if S("maintenance") == "true" and not is_admin(u.id):
        await say(t(lang, "maint"))
        return None
    if need_consent and not user.get("consent_at"):
        chat_id = ev.message.chat.id if isinstance(ev, CallbackQuery) else ev.chat.id
        if isinstance(ev, CallbackQuery):
            await ev.answer()
        await send_consent(chat_id, lang)
        return None
    return user


async def send_consent(chat_id: int, lang: str) -> None:
    await safe_send(chat_id, t(lang, "consent"), reply_markup=kb([
        [ibtn(t(lang, "consent_yes"), data="k:y", style="success")],
        [ibtn(t(lang, "consent_no_btn"), data="k:n")]]))


def kb_label(lang: str, key: str) -> str:
    return S(key).strip() or t(lang, key)


def reply_kb(lang: str) -> ReplyKeyboardMarkup:
    on = {k: S("off_" + k) != "true" for k in KB_KEYS}
    rows = []
    if on["kb_app"] and webapp_url():
        rows.append([KeyboardButton(text=kb_label(lang, "kb_app"), web_app=WebAppInfo(url=webapp_url()))])
    for pair in (("kb_win", "kb_mine"), ("kb_lang", "kb_support", "kb_help")):
        r = [KeyboardButton(text=kb_label(lang, k)) for k in pair if on[k]]
        if r:
            rows.append(r)
    return ReplyKeyboardMarkup(keyboard=rows or [[KeyboardButton(text="🏠")]], resize_keyboard=True, is_persistent=True)


def menu_key(text: str) -> Optional[str]:
    for k in KB_KEYS[1:]:
        if text in {TX["ar"][k], TX["en"][k], S(k).strip()}:
            return k
    return None


class MenuFilter(Filter):
    async def __call__(self, message: Message) -> Any:
        k = menu_key(message.text or "")
        return {"mkey": k} if k else False


# =====================================================================
#  shop (channel list + card)
# =====================================================================
async def owned_ids(uid: int) -> set:
    rows = await db._q("SELECT channel_id FROM orders WHERE telegram_id = ? AND status = 'paid'", [uid])
    return {r["channel_id"] for r in rows}


def invoice_ready(c: dict) -> bool:
    return bool(c.get("invoice_url")) and c.get("invoice_price") == c.get("price_stars")


def buy_button(lang: str, c: dict, owned: bool, compact: bool = True) -> InlineKeyboardButton:
    if owned and c.get("join_link"):
        return ibtn(t(lang, "open" if compact else "open_ch"), url=c["join_link"], style="success")
    if int(c["price_stars"]) <= 0 and c.get("join_link"):
        return ibtn(t(lang, "open" if compact else "open_ch"), url=c["join_link"], style="success")
    if invoice_ready(c):
        return ibtn(t(lang, "buy", p=c["price_stars"]), url=c["invoice_url"], style=c.get("buy_color") or "success")
    return ibtn(t(lang, "buy", p=c["price_stars"]), data=f"c:nb:{c['id']}", style=c.get("buy_color") or "success")


SHOP_SQL = ("SELECT c.*, (SELECT COUNT(*) FROM orders o WHERE o.channel_id = c.id AND o.status = 'paid' "
            "AND o.source = 'stars') AS sales FROM channels c WHERE c.active = 1 ORDER BY c.id ASC")


def trending_ids(rows: list) -> set:
    ids = {c["id"] for c in rows if c["pinned"]}
    n = SI("trend_top")
    if n > 0:
        top = sorted((c for c in rows if c["sales"] > 0), key=lambda c: -c["sales"])[:n]
        ids |= {c["id"] for c in top}
    return ids


def grid_rows(rows: list, hot: set) -> list:
    """Small buttons (GRID_COLS per row); a trending channel keeps its place but takes 2 cells."""
    out, cur, used = [], [], 0
    for c in rows:
        w = 2 if c["id"] in hot else 1
        if used + w > GRID_COLS:
            out.append(cur)
            cur, used = [], 0
        cur.append((c, w == 2))
        used += w
        if used >= GRID_COLS:
            out.append(cur)
            cur, used = [], 0
    if cur:
        out.append(cur)
    return out


async def shop_view(uid: int, lang: str, page: int) -> tuple:
    rows = await db._q(SHOP_SQL)
    if not rows:
        return t(lang, "shop_empty"), None
    grid = grid_rows(rows, trending_ids(rows))
    pages = max((len(grid) + GRID_ROWS - 1) // GRID_ROWS, 1)
    page = max(0, min(page, pages - 1))
    k = []
    for line in grid[page * GRID_ROWS:(page + 1) * GRID_ROWS]:
        k.append([ibtn((("🔥 " + (c["button_name"] or c["title"])[:24]) if wide else (c["button_name"] or c["title"] or "-")[:16]),
                       data=f"c:v:{c['id']}:{page}") for c, wide in line])
    if pages > 1:
        nav = []
        if page > 0:
            nav.append(ibtn(t(lang, "prev"), data=f"c:p:{page - 1}"))
        nav.append(ibtn(f"{page + 1}/{pages}", data="c:x"))
        if page < pages - 1:
            nav.append(ibtn(t(lang, "next"), data=f"c:p:{page + 1}"))
        k.append(nav)
    return (S("shop_text").strip() or t(lang, "shop")), kb(k)


async def send_shop(chat_id: int, lang: str, page: int = 0) -> None:
    text, markup = await shop_view(chat_id, lang, page)
    await safe_send(chat_id, text, reply_markup=markup)


async def channel_by_id(cid: int) -> Optional[dict]:
    rows = await db._q("SELECT * FROM channels WHERE id = ?", [cid])
    return rows[0] if rows else None


async def media_count(cid: int) -> int:
    return (await db._q("SELECT COUNT(*) AS n FROM channel_media WHERE channel_id = ?", [cid]))[0]["n"]


def card_desc(c: dict) -> str:
    d = (c.get("custom_desc") or c.get("description") or "").strip()
    return esc(d[:160] + ("…" if len(d) > 160 else ""))


async def send_card(chat_id: int, lang: str, cid: int, page: int = 0) -> None:
    c = await channel_by_id(cid)
    if not c or not c["active"]:
        return
    owned = cid in await owned_ids(chat_id)
    price = t(lang, "free_price") if int(c["price_stars"]) <= 0 else f"{c['price_stars']}"
    text = t(lang, "card", title=esc(c["title"] or c["button_name"]), desc=card_desc(c),
             count=await media_count(cid), members=c["members"], price=price).replace("\n\n\n", "\n\n")
    if owned:
        text += t(lang, "card_owned")
    rows = [[buy_button(lang, c, owned, compact=False)]]
    sec = []
    if webapp_url():
        sec.append(ibtn(t(lang, "more"), web=f"{webapp_url()}?c={cid}"))
    if S("trust_url"):
        sec.append(ibtn(t(lang, "trust"), url=S("trust_url")))
    if sec:
        rows.append(sec)
    rows.append([ibtn(t(lang, "back"), data=f"c:p:{page}")])
    markup = kb(rows)
    await track(chat_id, cid, "view")
    if c["has_photo"]:
        try:
            src: Any = c["photo_fid"]
            if not src:
                data = await db.get_thumb(f"c{cid}")
                src = BufferedInputFile(data, "c.jpg") if data else None
            if src:
                m = await bot.send_photo(chat_id, src, caption=text, reply_markup=markup)
                if not c["photo_fid"] and m.photo:
                    await db._run([("UPDATE channels SET photo_fid = ? WHERE id = ?", [m.photo[-1].file_id, cid])])
                return
        except TelegramAPIError as e:
            log.debug("card photo failed: %s", e)
    await safe_send(chat_id, text, reply_markup=markup)


async def after_start(uid: int, user: dict, payload: str = "") -> None:
    lang = user.get("language", "ar")
    await safe_send(uid, t(lang, "menu_hint"), reply_markup=reply_kb(lang))
    m = re.match(r"^ch_(\d+)$", payload or "")
    if m:
        await send_card(uid, lang, int(m.group(1)))
    else:
        await send_shop(uid, lang)


# =====================================================================
#  commands
# =====================================================================
@router.message(CommandStart(), F.chat.type == "private")
async def cmd_start(m: Message, command: CommandObject) -> None:
    if not m.from_user:
        return
    arg = (command.args or "").strip()
    ref = None
    mm = re.match(r"^ref_(\d{3,15})$", arg)
    if mm:
        ref = int(mm.group(1))
    await register_user(m.from_user, ref)
    user = await gate(m, need_consent=False)
    if not user:
        return
    if not user.get("consent_at"):
        if arg.startswith("ch_"):
            await db._run([("UPDATE users SET pending = ? WHERE telegram_id = ?", [arg, m.from_user.id])])
        await send_consent(m.chat.id, user["language"])
        return
    await after_start(m.chat.id, user, arg)


@router.callback_query(F.data.startswith("k:"))
async def cb_consent(cb: CallbackQuery) -> None:
    user = await gate(cb, need_consent=False)
    if not user:
        return
    await cb.answer()
    try:
        await cb.message.delete()
    except Exception:
        pass
    if (cb.data or "") == "k:y":
        await db._run([(f"UPDATE users SET consent_at = {_NOW_SQL}, pending = NULL WHERE telegram_id = ? "
                        "AND consent_at IS NULL", [cb.from_user.id])])
        await after_start(cb.from_user.id, user, user.get("pending") or "")
    else:
        await safe_send(cb.from_user.id, t(user["language"], "consent_no"))


@router.message(Command("terms"), F.chat.type == "private")
async def cmd_terms(m: Message) -> None:
    user = await gate(m, need_consent=False)
    if user:
        await m.answer(t(user["language"], "terms"))


@router.message(Command("privacy"), F.chat.type == "private")
async def cmd_privacy(m: Message) -> None:
    user = await gate(m, need_consent=False)
    if user:
        await m.answer(t(user["language"], "privacy"))


async def send_support(chat_id: int, lang: str) -> None:
    link = support_link()
    if not link:
        await safe_send(chat_id, t(lang, "support_none"))
        return
    await safe_send(chat_id, t(lang, "support"), reply_markup=kb([[ibtn(t(lang, "support_btn"), url=link, style="primary")]]))


@router.message(Command("support", "paysupport"), F.chat.type == "private")
async def cmd_support(m: Message) -> None:
    user = await gate(m, need_consent=False)
    if user:
        await send_support(m.chat.id, user["language"])


async def send_mine(chat_id: int, lang: str) -> None:
    rows = await db._q("SELECT c.* FROM channels c WHERE c.id IN (SELECT channel_id FROM orders WHERE telegram_id = ? "
                       "AND status = 'paid') ORDER BY c.id", [chat_id])
    if not rows:
        await safe_send(chat_id, t(lang, "mine_empty"))
        return
    k = [[ibtn(f"✅ {(c['button_name'] or c['title'])[:40]}", url=c["join_link"], style="success")]
         for c in rows if c.get("join_link")]
    await safe_send(chat_id, t(lang, "mine"), reply_markup=kb(k))


@router.message(Command("mychannels"), F.chat.type == "private")
async def cmd_mine(m: Message) -> None:
    user = await gate(m)
    if user:
        await send_mine(m.chat.id, user["language"])


# ---- reply-keyboard buttons (labels/visibility are controlled by the admin)
@router.message(F.chat.type == "private", F.text, MenuFilter())
async def on_menu(m: Message, mkey: str) -> None:
    user = await gate(m)
    if not user:
        return
    lang, uid = user["language"], m.from_user.id
    if mkey == "kb_win":
        await show_contest(uid, user)
    elif mkey == "kb_mine":
        await send_mine(uid, lang)
    elif mkey == "kb_lang":
        await m.answer(t(lang, "lang_pick"), reply_markup=kb([[ibtn("العربية 🇸🇦", data="lg:ar"), ibtn("English 🇬🇧", data="lg:en")]]))
    elif mkey == "kb_support":
        await send_support(uid, lang)
    elif mkey == "kb_help":
        await m.answer(t(lang, "help"))


@router.callback_query(F.data.startswith("lg:"))
async def cb_lang(cb: CallbackQuery) -> None:
    user = await gate(cb)
    if not user:
        return
    lang = "en" if (cb.data or "").endswith("en") else "ar"
    await db._run([("UPDATE users SET language = ? WHERE telegram_id = ?", [lang, cb.from_user.id])])
    await cb.answer("✅")
    try:
        await cb.message.delete()
    except Exception:
        pass
    await safe_send(cb.from_user.id, t(lang, "menu_hint"), reply_markup=reply_kb(lang))
    await send_shop(cb.from_user.id, lang)


@router.callback_query(F.data.startswith("c:"))
async def cb_shop(cb: CallbackQuery) -> None:
    user = await gate(cb)
    if not user:
        return
    lang, uid = user["language"], cb.from_user.id
    parts = (cb.data or "").split(":")
    act = parts[1] if len(parts) > 1 else ""
    if act == "x":
        await cb.answer()
    elif act == "nb":
        await cb.answer(re.sub(r"<[^>]+>", "", t(lang, "preparing")), show_alert=True)
    elif act == "p":
        text, markup = await shop_view(uid, lang, int(parts[2]) if len(parts) > 2 else 0)
        await cb.answer()
        try:
            if cb.message.photo:
                await cb.message.delete()
                await safe_send(uid, text, reply_markup=markup)
            else:
                await cb.message.edit_text(text, reply_markup=markup)
        except TelegramBadRequest:
            pass
    elif act == "v" and len(parts) > 2:
        await cb.answer()
        try:
            await cb.message.delete()
        except Exception:
            pass
        await send_card(uid, lang, int(parts[2]), int(parts[3]) if len(parts) > 3 else 0)
    else:
        await cb.answer()


# =====================================================================
#  contest: win a channel
# =====================================================================
async def get_subs() -> list:
    return await db._q("SELECT * FROM sub_channels ORDER BY id")


async def check_membership(uid: int) -> tuple:
    ok, err = True, False
    for ch in await get_subs():
        try:
            mem = await bot.get_chat_member(int(ch["chat"]) if re.match(r"^-?\d+$", ch["chat"]) else ch["chat"], uid)
            st = getattr(mem.status, "value", mem.status)
            if st in ("left", "kicked") or (st == "restricted" and not getattr(mem, "is_member", True)):
                ok = False
        except TelegramAPIError as e:
            err = True
            await alert_admins(f"⚠️ تعذر التحقق من الاشتراك في {ch['chat']}.\nتأكد أن البوت مشرف في القناة.\n"
                               f"<code>{esc(str(e))[:200]}</code>", key=f"sub:{ch['chat']}")
    return (ok and not err), err


async def contest_stats(uid: int) -> dict:
    target = max(SI("contest_target"), 1)
    valid = (await db._q("SELECT COUNT(*) AS n FROM users WHERE referred_by = ? AND ref_counted = 1", [uid]))[0]["n"]
    claims = (await db._q("SELECT COUNT(*) AS n FROM contest_claims WHERE user_id = ?", [uid]))[0]["n"]
    earned = valid // target
    mx = SI("contest_max")
    capped = bool(mx) and earned >= mx
    if mx:
        earned = min(earned, mx)
    return {"target": target, "valid": valid, "claims": claims, "avail": max(earned - claims, 0),
            "maxed": bool(mx) and claims >= mx}


async def show_contest(uid: int, user: dict, edit: Optional[Message] = None) -> None:
    lang = user["language"]
    if S("contest_enabled") != "true":
        await safe_send(uid, t(lang, "win_off"))
        return
    ok, err = await check_membership(uid)
    if err:
        await safe_send(uid, t(lang, "sub_err"))
        return
    if not ok:
        await db._run([("UPDATE users SET contest_ok = 0 WHERE telegram_id = ?", [uid])])
        k = [[ibtn(ch["title"], url=ch["url"])] for ch in await get_subs()]
        k.append([ibtn(t(lang, "verify"), data="ct:chk", style="success")])
        if edit:
            try:
                await edit.edit_text(t(lang, "win_sub"), reply_markup=kb(k))
                return
            except TelegramBadRequest:
                return
        await safe_send(uid, t(lang, "win_sub"), reply_markup=kb(k))
        return
    await db._run([("UPDATE users SET contest_ok = 1 WHERE telegram_id = ?", [uid])])
    st = await contest_stats(uid)
    filled = min(10, int(st["valid"] * 10 / st["target"])) if st["target"] else 0
    bar = "█" * filled + "░" * (10 - filled)
    text = t(lang, "win", target=st["target"], valid=st["valid"], avail=st["avail"], bar=bar,
             link=bot_link(f"ref_{uid}"))
    if st["maxed"]:
        text += t(lang, "win_max")
    k = [[ibtn(t(lang, "share"), switch="share", style="primary")]]
    if st["avail"] > 0:
        k.append([ibtn(t(lang, "claim"), data="ct:pick", style="success")])
    k.append([ibtn(t(lang, "refresh"), data="ct:ref")])
    if edit:
        try:
            await edit.edit_text(text, reply_markup=kb(k))
        except TelegramBadRequest:
            pass
        return
    await safe_send(uid, text, reply_markup=kb(k))


@router.callback_query(F.data.startswith("ct:"))
async def cb_contest(cb: CallbackQuery) -> None:
    user = await gate(cb)
    if not user:
        return
    lang, uid = user["language"], cb.from_user.id
    parts = (cb.data or "").split(":")
    act = parts[1] if len(parts) > 1 else ""
    if act in ("chk", "ref"):
        if act == "chk":
            ok, err = await check_membership(uid)
            if err:
                await cb.answer(t(lang, "sub_err"), show_alert=True)
                return
            if not ok:
                await cb.answer(t(lang, "sub_no"), show_alert=True)
                return
        await cb.answer()
        await show_contest(uid, user, edit=cb.message)
    elif act == "pick":
        st = await contest_stats(uid)
        if st["avail"] <= 0:
            await cb.answer(t(lang, "claim_no"), show_alert=True)
            return
        owned = await owned_ids(uid)
        rows = await db._q("SELECT * FROM channels WHERE active = 1 AND contest = 1 ORDER BY pinned DESC, id")
        rows = [c for c in rows if c["id"] not in owned]
        await cb.answer()
        if not rows:
            await safe_send(uid, t(lang, "pick_none"))
            return
        k = [[ibtn(f"🎁 {(c['button_name'] or c['title'])[:40]}", data=f"ct:g:{c['id']}", style=None)]
             for c in rows]
        await safe_send(uid, t(lang, "pick"), reply_markup=kb(k))
    elif act == "g" and len(parts) > 2:
        cid = int(parts[2])
        ok, err = await check_membership(uid)
        if not ok:
            await cb.answer(t(lang, "sub_no" if not err else "sub_err"), show_alert=True)
            return
        st = await contest_stats(uid)
        c = await channel_by_id(cid)
        if st["avail"] <= 0 or not c or not c["active"] or not c["contest"] or cid in await owned_ids(uid):
            await cb.answer(t(lang, "claim_no"), show_alert=True)
            return
        try:
            await db.tx([
                ("INSERT INTO contest_claims (user_id, seq, channel_id) VALUES (?, ?, ?)", [uid, st["claims"] + 1, cid]),
                ("INSERT INTO orders (telegram_id, channel_id, stars, charge_id, source) VALUES (?, ?, 0, ?, 'contest')",
                 [uid, cid, f"contest:{uid}:{st['claims'] + 1}"]),
            ])
        except DBError:
            await cb.answer(t(lang, "claim_no"), show_alert=True)
            return
        await cb.answer()
        try:
            await cb.message.delete()
        except Exception:
            pass
        await safe_send(uid, t(lang, "claimed"))
        await log_admin(uid, "contest_claim", f"user={uid} channel={cid}")
        _wake.set()
    else:
        await cb.answer()


@router.inline_query()
async def on_inline(q: InlineQuery) -> None:
    uid = q.from_user.id
    row = await get_user(uid)
    lang = (row or {}).get("language", "ar")
    if not row or not row.get("contest_ok") or S("contest_enabled") != "true":
        await q.answer([], cache_time=0, is_personal=True, switch_pm_text=t(lang, "share_locked"),
                       switch_pm_parameter="win")
        return
    link = bot_link(f"ref_{uid}")
    text = S("share_text").strip() or t(lang, "share_default")
    text = esc(text).replace("{link}", link) if "{link}" in text else esc(text)
    res = InlineQueryResultArticle(
        id=f"s{uid}", title=t(lang, "share_title"), description=text[:80],
        input_message_content=InputTextMessageContent(message_text=text, parse_mode="HTML"),
        reply_markup=kb([[ibtn("🎁 " + ("افتح البوت" if lang == "ar" else "Open the bot"), url=link, style="success")]]))
    await q.answer([res], cache_time=0, is_personal=True)


# =====================================================================
#  private channel access: personal join-request links
# =====================================================================
@router.chat_join_request()
async def on_join_request(req: ChatJoinRequest) -> None:
    uid = req.from_user.id
    rows = await db._q("SELECT id FROM channels WHERE chat_id = ?", [req.chat.id])
    if not rows:
        return
    cid = rows[0]["id"]
    ok = (await db._q("SELECT 1 AS x FROM orders WHERE telegram_id = ? AND channel_id = ? AND status = 'paid' LIMIT 1",
                      [uid, cid]))
    try:
        if ok:
            await bot.approve_chat_join_request(req.chat.id, uid)
        else:
            await bot.decline_chat_join_request(req.chat.id, uid)
            u = await get_user(uid)
            await safe_send(req.user_chat_id, t((u or {}).get("language", "ar"), "join_denied"),
                            reply_markup=kb([[ibtn("🎬", url=bot_link(f"ch_{cid}"), style="success")]]))
    except TelegramAPIError as e:
        log.warning("join request failed: %s", e)


# =====================================================================
#  channel info / indexing helpers
# =====================================================================
async def create_join_link(chat_id: int) -> Optional[str]:
    try:
        lk = await bot.create_chat_invite_link(chat_id, name="vidox", creates_join_request=True)
        return lk.invite_link
    except TelegramAPIError as e:
        log.warning("invite link failed %s: %s", chat_id, e)
        return None


async def refresh_channel(cid: int, photo: bool = True) -> bool:
    c = await channel_by_id(cid)
    if not c:
        return False
    try:
        ch = await bot.get_chat(c["chat_id"])
        members = await bot.get_chat_member_count(c["chat_id"])
    except TelegramAPIError as e:
        log.warning("refresh failed %s: %s", cid, e)
        return False
    stmts = [("UPDATE channels SET title = ?, description = ?, username = ?, members = ? WHERE id = ?",
              [ch.title or c["title"], ch.description or "", ch.username, members, cid])]
    if not c["button_name"]:
        stmts.append(("UPDATE channels SET button_name = ? WHERE id = ?", [(ch.title or "")[:40], cid]))
    if photo and ch.photo:
        try:
            buf = io.BytesIO()
            await bot.download(ch.photo.big_file_id, destination=buf)
            stmts.append(("INSERT OR REPLACE INTO thumbs (key, data) VALUES (?, ?)", [f"c{cid}", buf.getvalue()]))
            stmts.append(("UPDATE channels SET has_photo = 1, photo_fid = NULL WHERE id = ?", [cid]))
        except Exception as e:
            log.warning("photo download failed: %s", e)
    await db._run(stmts)
    return True


def media_of(m: Message) -> Optional[dict]:
    obj = m.video or m.animation
    if not obj and m.document and (m.document.mime_type or "").startswith("video/"):
        obj = m.document
    if not obj:
        return None
    th = getattr(obj, "thumbnail", None)
    title = ((m.caption or "").strip().split("\n")[0] or getattr(obj, "file_name", "") or "")[:80]
    return {"title": title, "duration": int(getattr(obj, "duration", 0) or 0),
            "size": int(getattr(obj, "file_size", 0) or 0), "thumb_fid": th.file_id if th else None}


async def index_media(chat_id: int, message_id: int, info: dict) -> Optional[int]:
    rows = await db._q("SELECT id FROM channels WHERE chat_id = ?", [chat_id])
    if not rows:
        return None
    cid = rows[0]["id"]
    await db._run([("INSERT INTO channel_media (channel_id, message_id, title, duration, size, thumb_fid) "
                    "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (channel_id, message_id) DO UPDATE SET "
                    "title = excluded.title, duration = excluded.duration, size = excluded.size, "
                    "thumb_fid = excluded.thumb_fid", [cid, message_id, info["title"], info["duration"],
                                                       info["size"], info["thumb_fid"]])])
    return cid


@router.channel_post()
async def on_channel_post(m: Message) -> None:
    info = media_of(m)
    if info:
        try:
            await index_media(m.chat.id, m.message_id, info)
        except Exception as e:
            log.debug("channel_post index failed: %s", e)


# =====================================================================
#  admin helpers
# =====================================================================
BACK_HOME = [ibtn("🏠 الرئيسية", data="a:home")]


async def get_astate(aid: int) -> Optional[dict]:
    rows = await db._q("SELECT state, data FROM admin_state WHERE admin_id = ?", [aid])
    if not rows:
        return None
    try:
        d = json.loads(rows[0]["data"] or "{}")
    except ValueError:
        d = {}
    return {"state": rows[0]["state"], "data": d}


async def set_astate(aid: int, state: str, data: Optional[dict] = None) -> None:
    await db._run([("INSERT INTO admin_state (admin_id, state, data) VALUES (?, ?, ?) "
                    "ON CONFLICT (admin_id) DO UPDATE SET state = excluded.state, data = excluded.data",
                    [aid, state, json.dumps(data or {}, ensure_ascii=False)])])


async def clear_astate(aid: int) -> None:
    try:
        await db._run([("DELETE FROM admin_state WHERE admin_id = ?", [aid])])
    except Exception:
        pass


async def show(ev: Any, text: str, markup: Optional[InlineKeyboardMarkup] = None) -> None:
    if isinstance(ev, CallbackQuery):
        try:
            await ev.message.edit_text(text, reply_markup=markup)
            return
        except TelegramBadRequest as e:
            if "not modified" in str(e).lower():
                return
        await bot.send_message(ev.message.chat.id, text, reply_markup=markup)
    else:
        await ev.answer(text, reply_markup=markup)


def _bar(p: int) -> str:
    f = max(0, min(10, p // 10))
    return "█" * f + "░" * (10 - f)


def home_markup() -> InlineKeyboardMarkup:
    return kb([
        [ibtn("📺 القنوات", data="a:ch:list"), ibtn("🎁 الاشتراك الإجباري", data="a:sb:list")],
        [ibtn("🔘 الأزرار", data="a:kb:list"), ibtn("⚙️ الإعدادات", data="a:se:home")],
        [ibtn("📊 الإحصائيات", data="a:st:7"), ibtn("📢 إذاعة", data="a:bc:start")],
        [ibtn("👤 المستخدمون", data="a:us:find"), ibtn("💾 النسخ", data="a:bk:home")],
    ])


def home_text() -> str:
    return f"🛠 <b>{esc(S('brand_name'))}</b> · لوحة التحكم"


@router.message(Command("admin"), F.chat.type == "private")
async def cmd_admin(m: Message) -> None:
    if not is_admin(m.from_user.id):
        return
    await clear_astate(m.from_user.id)
    await m.answer(home_text(), reply_markup=home_markup())


# =====================================================================
#  channels management
# =====================================================================
async def ch_list_view() -> tuple:
    rows = await db._q("SELECT * FROM channels ORDER BY id")
    k = []
    for c in rows:
        flags = ("📌" if c["pinned"] else "") + ("🎁" if c["contest"] else "") + ("🙈" if not c["active"] else "")
        k.append([ibtn(f"#{c['id']} {(c['button_name'] or c['title'])[:22]} · {c['price_stars']}⭐ {flags}".strip(), data=f"a:ch:v:{c['id']}")])
    k.append([ibtn("➕ إضافة قناة", data="a:ch:add")])
    k.append(BACK_HOME)
    return ("📺 <b>القنوات</b>" if rows else "📺 <b>القنوات</b>\nلا توجد قنوات بعد."), kb(k)


async def ch_view(cid: int) -> tuple:
    c = await channel_by_id(cid)
    if not c:
        return "❌ غير موجودة", kb([BACK_HOME])
    n = await media_count(cid)
    sales = (await db._q("SELECT COUNT(*) AS n, COALESCE(SUM(stars),0) AS s FROM orders WHERE channel_id = ? AND status = 'paid' "
                         "AND source = 'stars'", [cid]))[0]
    inv = "✅" if invoice_ready(c) else "⏳"
    text = (f"📺 <b>#{c['id']} · {esc(c['title'])}</b>\n"
            f"🔘 الزر: {esc(c['button_name'])}\n💰 {c['price_stars']}⭐ · رابط الدفع {inv}\n"
            f"👥 {c['members']} · 🎞 {n} · 🛒 {sales['n']} ({sales['s']}⭐)\n"
            f"{'🎁 مسابقة · ' if c['contest'] else ''}{'📌 رائجة · ' if c['pinned'] else ''}{'🙈 مخفية' if not c['active'] else '👁 ظاهرة'}")
    i = c["id"]
    k = [
        [ibtn("🔘 اسم الزر", data=f"a:ch:btn:{i}"), ibtn("💰 السعر", data=f"a:ch:price:{i}")],
        [ibtn("📝 الوصف", data=f"a:ch:desc:{i}"), ibtn(f"🎨 لون الشراء: {COLORS.get(c['buy_color'], '')[:2]}", data=f"a:ch:bc:{i}")],
        [ibtn(("🎁 ✖ المسابقة" if c["contest"] else "🎁 للمسابقة"), data=f"a:ch:ct:{i}"),
         ibtn(("📌 ✖ رائجة" if c["pinned"] else "📌 رائجة"), data=f"a:ch:pin:{i}")],
        [ibtn(("🙈 إخفاء" if c["active"] else "👁 إظهار"), data=f"a:ch:vis:{i}"), ibtn("🔄 تحديث", data=f"a:ch:ref:{i}")],
        [ibtn("🔗 رابط جديد", data=f"a:ch:link:{i}"), ibtn("🧹 مسح الفهرس", data=f"a:ch:clr:{i}")],
        [ibtn("🗑 حذف", data=f"a:ch:del:{i}"), ibtn("⬅️ القنوات", data="a:ch:list")],
    ]
    return text, kb(k)


COLOR_CYCLE = ["primary", "success", "danger", "default"]


async def add_channel_from(m: Message, chat_id: int) -> None:
    aid = m.from_user.id
    try:
        ch = await bot.get_chat(chat_id)
        me = await bot.get_chat_member(chat_id, BOT_ID)
    except TelegramAPIError as e:
        await m.answer(f"❌ تعذر الوصول للقناة.\nأضف البوت مشرفاً فيها أولاً.\n<code>{esc(str(e))[:200]}</code>")
        return
    st = getattr(me.status, "value", me.status)
    if st not in ("administrator", "creator") or (st == "administrator" and not getattr(me, "can_invite_users", False)):
        await m.answer("❌ البوت يجب أن يكون <b>مشرفاً</b> في القناة مع صلاحية <b>دعوة المستخدمين عبر الرابط</b>.")
        return
    if (await db._q("SELECT 1 AS x FROM channels WHERE chat_id = ?", [ch.id])):
        await m.answer("ℹ️ هذه القناة مضافة مسبقاً.")
        return
    await db._run([("INSERT INTO channels (chat_id, title, button_name, username) VALUES (?, ?, ?, ?)",
                    [ch.id, ch.title or "", (ch.title or "")[:40], ch.username])])
    cid = (await db._q("SELECT id FROM channels WHERE chat_id = ?", [ch.id]))[0]["id"]
    link = await create_join_link(ch.id)
    if link:
        await db._run([("UPDATE channels SET join_link = ? WHERE id = ?", [link, cid])])
    await refresh_channel(cid)
    await clear_astate(aid)
    await log_admin(aid, "channel_add", f"{cid} {ch.title}")
    text, markup = await ch_view(cid)
    await m.answer("✅ أُضيفت. حدّد السعر 👇\n\n" + text, reply_markup=markup)


@router.callback_query(F.data.startswith("a:"))
async def cb_admin(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer()
        return
    parts = (cb.data or "").split(":")
    aid = cb.from_user.id
    sec = parts[1] if len(parts) > 1 else ""
    try:
        if sec == "home":
            await clear_astate(aid)
            await cb.answer()
            await show(cb, home_text(), home_markup())
        elif sec == "ch":
            await adm_ch(cb, parts)
        elif sec == "st":
            await adm_stats(cb, int(parts[2]) if len(parts) > 2 else 7)
        elif sec == "bc":
            await adm_bc(cb, parts)
        elif sec == "us":
            await adm_us(cb, parts)
        elif sec == "se":
            await adm_se(cb, parts)
        elif sec == "bk":
            await adm_bk(cb, parts)
        elif sec == "kb":
            await adm_kb(cb, parts)
        elif sec == "sb":
            await adm_sb(cb, parts)
        elif sec == "wp":
            await adm_wipe(cb, parts)
        else:
            await cb.answer()
    except DBError as e:
        await cb.answer("خطأ في قاعدة البيانات", show_alert=True)
        log.warning("admin db error: %s", e)


async def adm_ch(cb: CallbackQuery, parts: list) -> None:
    aid = cb.from_user.id
    act = parts[2] if len(parts) > 2 else "list"
    cid = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
    await clear_astate(aid) if act in ("list", "v") else None
    if act == "list":
        await cb.answer()
        text, markup = await ch_list_view()
        await show(cb, text, markup)
        return
    if act == "add":
        await set_astate(aid, "ch_add")
        await cb.answer()
        await show(cb, "➕ <b>إضافة قناة</b>\nاجعل البوت مشرفاً فيها (صلاحية الدعوة)، ثم حوّل رسالة منها أو أرسل @يوزرها.",
                   kb([[ibtn("إلغاء", data="a:ch:list")]]))
        return
    c = await channel_by_id(cid)
    if not c:
        await cb.answer("غير موجودة", show_alert=True)
        return
    if act == "v":
        await cb.answer()
        text, markup = await ch_view(cid)
        await show(cb, text, markup)
        return
    prompts = {"btn": ("ch_btn", "🔘 أرسل اسم الزر (قصير)"),
               "price": ("ch_price", "💰 أرسل السعر بالنجوم (0 = مجانية)"),
               "desc": ("ch_desc", "📝 أرسل الوصف، أو - لوصف القناة الأصلي")}
    if act in prompts:
        st, txt = prompts[act]
        await set_astate(aid, st, {"cid": cid})
        await cb.answer()
        await show(cb, txt, kb([[ibtn("إلغاء", data=f"a:ch:v:{cid}")]]))
        return
    flips = {"ct": "contest", "pin": "pinned", "vis": "active"}
    if act in flips:
        col = flips[act]
        await db._run([(f"UPDATE channels SET {col} = 1 - {col} WHERE id = ?", [cid])])
    elif act in ("nc", "bc"):
        col = "name_color" if act == "nc" else "buy_color"
        cur = c[col] if c[col] in COLOR_CYCLE else "primary"
        nxt = COLOR_CYCLE[(COLOR_CYCLE.index(cur) + 1) % len(COLOR_CYCLE)]
        await db._run([(f"UPDATE channels SET {col} = ? WHERE id = ?", [nxt, cid])])
    elif act == "ref":
        ok = await refresh_channel(cid)
        await cb.answer("✅ تم" if ok else "❌ فشل التحديث", show_alert=not ok)
        text, markup = await ch_view(cid)
        await show(cb, text, markup)
        return
    elif act == "link":
        link = await create_join_link(c["chat_id"])
        if not link:
            await cb.answer("❌ تعذر إنشاء الرابط (تحقق من صلاحيات البوت)", show_alert=True)
            return
        await db._run([("UPDATE channels SET join_link = ? WHERE id = ?", [link, cid])])
        await cb.answer("✅ رابط جديد (الروابط القديمة تبقى تعمل)")
        return
    elif act == "clr":
        await db._run([("DELETE FROM channel_media WHERE channel_id = ?", [cid])])
        await cb.answer("🧹 تم مسح الفهرس")
    elif act == "del":
        await cb.answer()
        await show(cb, f"⚠️ حذف القناة <b>{esc(c['title'])}</b> من المتجر؟\nلن يتأثر المشترون القدامى في القناة نفسها، لكن ستُحذف سجلاتها من البوت.",
                   kb([[ibtn("نعم، احذف", data=f"a:ch:del2:{cid}", style="danger")], [ibtn("تراجع", data=f"a:ch:v:{cid}")]]))
        return
    elif act == "del2":
        await db._run([("DELETE FROM channels WHERE id = ?", [cid]), ("DELETE FROM channel_media WHERE channel_id = ?", [cid]),
                       ("DELETE FROM thumbs WHERE key = ?", [f"c{cid}"])])
        await log_admin(aid, "channel_delete", str(cid))
        await cb.answer("🗑 تم")
        text, markup = await ch_list_view()
        await show(cb, text, markup)
        return
    await cb.answer()
    text, markup = await ch_view(cid)
    await show(cb, text, markup)


# =====================================================================
#  stats
# =====================================================================
async def adm_stats(cb: CallbackQuery, days: int) -> None:
    await cb.answer()
    since = iso(now_utc() - timedelta(days=days)) if days else "1970-01-01T00:00:00+00:00"
    r = await db._run([
        ("SELECT COUNT(*) AS n FROM users", []),
        ("SELECT COUNT(*) AS n FROM users WHERE created_at >= ?", [since]),
        ("SELECT COUNT(*) AS n FROM users WHERE last_active_at >= ?", [since]),
        ("SELECT COUNT(*) AS n FROM users WHERE ref_counted = 1 AND created_at >= ?", [since]),
        ("SELECT COUNT(*) AS n, COALESCE(SUM(stars),0) AS s FROM orders WHERE source = 'stars' AND status = 'paid' AND created_at >= ?", [since]),
        ("SELECT COUNT(*) AS n, COALESCE(SUM(stars),0) AS s FROM orders WHERE status = 'refunded' AND refunded_at >= ?", [since]),
        ("SELECT COUNT(*) AS n FROM contest_claims WHERE created_at >= ?", [since]),
        ("SELECT COUNT(*) AS n FROM events WHERE kind = 'view' AND created_at >= ?", [since]),
        ("SELECT COUNT(DISTINCT telegram_id) AS n FROM orders WHERE source = 'stars' AND status = 'paid' AND created_at >= ?", [since]),
        ("SELECT c.title AS title, COUNT(o.id) AS n, COALESCE(SUM(o.stars),0) AS s FROM orders o JOIN channels c ON c.id = o.channel_id "
         "WHERE o.source = 'stars' AND o.status = 'paid' AND o.created_at >= ? GROUP BY c.id ORDER BY n DESC LIMIT 5", [since]),
        ("SELECT c.title AS title, COUNT(e.id) AS n FROM events e JOIN channels c ON c.id = e.channel_id "
         "WHERE e.kind = 'view' AND e.created_at >= ? GROUP BY c.id ORDER BY n DESC LIMIT 5", [since]),
        ("SELECT substr(created_at,1,10) AS d, COUNT(*) AS n FROM users WHERE created_at >= ? GROUP BY d ORDER BY d DESC LIMIT 7",
         [iso(now_utc() - timedelta(days=7))]),
        ("SELECT substr(created_at,1,10) AS d, COUNT(*) AS n, COALESCE(SUM(stars),0) AS s FROM orders WHERE source = 'stars' "
         "AND status = 'paid' AND created_at >= ? GROUP BY d ORDER BY d DESC LIMIT 7", [iso(now_utc() - timedelta(days=7))]),
    ])
    g = lambda i, k="n": r[i]["rows"][0][k]  # noqa: E731
    views, buyers = g(7), g(8)
    conv = f"{(g(4) * 100 / views):.1f}%" if views else "—"
    label = {1: "اليوم", 7: "آخر 7 أيام", 30: "آخر 30 يوماً", 0: "كل الفترة"}.get(days, f"{days} يوم")
    top = "\n".join(f"  {i + 1}. {esc(x['title'])} — {x['n']} ({x['s']}⭐)" for i, x in enumerate(r[9]["rows"])) or "  —"
    topv = "\n".join(f"  {i + 1}. {esc(x['title'])} — {x['n']}" for i, x in enumerate(r[10]["rows"])) or "  —"
    sales = {x["d"]: x for x in r[12]["rows"]}
    daily = []
    for x in r[11]["rows"]:
        s = sales.get(x["d"], {"n": 0, "s": 0})
        daily.append(f"<code>{x['d'][5:]}</code> 👤{x['n']} 🛒{s['n']} ⭐{s['s']}")
    text = (f"📊 <b>الإحصائيات — {label}</b>\n\n"
            f"👥 كل المستخدمين: <b>{g(0)}</b>\n🆕 جدد: <b>{g(1)}</b> • 🔥 نشطون: <b>{g(2)}</b>\n"
            f"🤝 إحالات صالحة: <b>{g(3)}</b> • 🎁 جوائز مُستلمة: <b>{g(6)}</b>\n\n"
            f"🛒 طلبات مدفوعة: <b>{g(4)}</b> ({buyers} مشترٍ)\n⭐ النجوم: <b>{g(4, 's')}</b>\n"
            f"↩️ مستردة: <b>{g(5)}</b> ({g(5, 's')}⭐)\n"
            f"👁 مشاهدات بطاقات القنوات: <b>{views}</b> • التحويل: <b>{conv}</b>\n\n"
            f"🏆 الأكثر مبيعاً:\n{top}\n\n👁 الأكثر مشاهدة:\n{topv}\n\n📅 آخر 7 أيام:\n" + ("\n".join(daily) or "—"))
    k = [[ibtn("اليوم", data="a:st:1"), ibtn("7 أيام", data="a:st:7"), ibtn("30 يوم", data="a:st:30"), ibtn("الكل", data="a:st:0")],
         BACK_HOME]
    await show(cb, text, kb(k))


# =====================================================================
#  broadcast
# =====================================================================
_bc_cancel: set = set()
_bc_tasks: dict = {}


async def audience_ids(aud: str) -> list:
    if aud == "buyers":
        sql = ("SELECT telegram_id FROM users WHERE is_active = 1 AND is_banned = 0 AND telegram_id IN "
               "(SELECT telegram_id FROM orders WHERE status = 'paid')")
    elif aud == "non":
        sql = ("SELECT telegram_id FROM users WHERE is_active = 1 AND is_banned = 0 AND telegram_id NOT IN "
               "(SELECT telegram_id FROM orders WHERE status = 'paid')")
    else:
        sql = "SELECT telegram_id FROM users WHERE is_active = 1 AND is_banned = 0"
    return [r["telegram_id"] for r in await db._q(sql)]


async def _bc_one(src_chat: int, src_msg: int, uid: int) -> tuple:
    for _ in range(3):
        try:
            mm = await bot.copy_message(uid, src_chat, src_msg)
            return "ok", mm.message_id
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 0.5)
        except TelegramForbiddenError:
            return "blocked", 0
        except TelegramAPIError:
            return "fail", 0
    return "fail", 0


async def run_broadcast(bid: int, admin_chat: int, progress_id: int, src_chat: int, src_msg: int, ids: list) -> None:
    total = len(ids)
    sent = failed = blocked = 0
    recs: list = []
    blocked_ids: list = []
    status = "done"
    last_edit = 0.0

    async def render(final: bool) -> None:
        pct = 100 if total == 0 else int((sent + failed + blocked) * 100 / total)
        head = "📢 <b>اكتملت الإذاعة</b>" if final and status == "done" else (
            "⛔ <b>تم إيقاف الإذاعة</b>" if final else "📢 <b>جارٍ الإذاعة...</b>")
        text = (f"{head}\n{_bar(pct)} {pct}%\n✅ وصلت: <b>{sent}</b>   ❌ فشلت: <b>{failed}</b>   🚫 حظروا: <b>{blocked}</b>\n"
                f"📈 الوصول: <b>{(sent * 100 // total) if total else 0}%</b> من {total}")
        k = kb([[ibtn("🗑 حذف من عند الجميع", data=f"a:bc:del:{bid}")], BACK_HOME]) if final else \
            kb([[ibtn("⛔ إلغاء الإذاعة", data=f"a:bc:stop:{bid}")]])
        try:
            await bot.edit_message_text(text, chat_id=admin_chat, message_id=progress_id, reply_markup=k)
        except Exception:
            pass

    async def flush() -> None:
        nonlocal recs
        if recs:
            try:
                await db._run([("INSERT OR REPLACE INTO broadcast_recipients (broadcast_id, telegram_id, message_id) VALUES (?, ?, ?)",
                                [bid, u, mid]) for u, mid in recs])
            except Exception as e:
                log.warning("recipients insert failed: %s", e)
            recs = []

    try:
        for i in range(0, total, 20):
            if bid in _bc_cancel:
                status = "cancelled"
                break
            chunk = ids[i:i + 20]
            results = await asyncio.gather(*(_bc_one(src_chat, src_msg, u) for u in chunk))
            for u, (res, mid) in zip(chunk, results):
                if res == "ok":
                    sent += 1
                    recs.append((u, mid))
                elif res == "blocked":
                    blocked += 1
                    blocked_ids.append(u)
                else:
                    failed += 1
            if len(recs) >= 100:
                await flush()
            if time.time() - last_edit > 3:
                last_edit = time.time()
                await render(False)
            await asyncio.sleep(1.0)
    except Exception as e:
        log.exception("broadcast crashed: %s", e)
        status = "interrupted"
    finally:
        await flush()
        for u in blocked_ids:
            try:
                await db._run([("UPDATE users SET is_active = 0 WHERE telegram_id = ?", [u])])
            except Exception:
                pass
        try:
            await db._run([(f"UPDATE broadcasts SET status = ?, sent = ?, failed = ?, blocked = ?, finished_at = {_NOW_SQL} "
                            "WHERE id = ?", [status, sent, failed, blocked, bid])])
        except Exception:
            pass
        _bc_tasks.pop(bid, None)
        _bc_cancel.discard(bid)
        await render(True)


async def delete_broadcast(bid: int, admin_chat: int) -> None:
    rows = await db._q("SELECT telegram_id, message_id FROM broadcast_recipients WHERE broadcast_id = ?", [bid])
    deleted = 0
    for i in range(0, len(rows), 20):
        async def _del(r: dict) -> int:
            try:
                await bot.delete_message(r["telegram_id"], r["message_id"])
                return 1
            except TelegramRetryAfter as e:
                await asyncio.sleep(e.retry_after + 0.5)
            except Exception:
                pass
            return 0
        deleted += sum(await asyncio.gather(*(_del(r) for r in rows[i:i + 20])))
        await asyncio.sleep(1.0)
    await db._run([("UPDATE broadcasts SET status = 'deleted' WHERE id = ?", [bid])])
    await safe_send(admin_chat, f"🗑 حُذفت الإذاعة من عند <b>{deleted}</b> مستخدم.\n(تيليجرام يسمح بالحذف خلال 48 ساعة فقط)")


async def adm_bc(cb: CallbackQuery, parts: list) -> None:
    aid = cb.from_user.id
    act = parts[2] if len(parts) > 2 else "start"
    if act == "start":
        await cb.answer()
        await show(cb, "📢 <b>إذاعة</b>\nلمن تريد الإرسال؟", kb([
            [ibtn("👥 الجميع", data="a:bc:aud:all")], [ibtn("🛒 المشترون فقط", data="a:bc:aud:buyers")],
            [ibtn("🙋 غير المشترين", data="a:bc:aud:non")], BACK_HOME]))
    elif act == "aud":
        aud = parts[3]
        n = len(await audience_ids(aud))
        await set_astate(aid, "bc_msg", {"aud": aud})
        await cb.answer()
        await show(cb, f"📝 أرسل الآن الرسالة (نص/صورة/فيديو...) وسأرسلها إلى <b>{n}</b> مستخدم.", kb([[ibtn("إلغاء", data="a:home")]]))
    elif act == "go":
        stt = await get_astate(aid)
        if not stt or stt["state"] != "bc_ready":
            await cb.answer("انتهت الجلسة", show_alert=True)
            return
        d = stt["data"]
        ids = await audience_ids(d["aud"])
        await clear_astate(aid)
        await db._run([("INSERT INTO broadcasts (admin_id, audience, src_chat, src_message, total) VALUES (?, ?, ?, ?, ?)",
                        [aid, d["aud"], d["chat"], d["msg"], len(ids)])])
        bid = (await db._q("SELECT MAX(id) AS i FROM broadcasts"))[0]["i"]
        await cb.answer("🚀")
        pm = await bot.send_message(cb.message.chat.id, "📢 جارٍ الإذاعة...")
        _bc_tasks[bid] = asyncio.create_task(run_broadcast(bid, cb.message.chat.id, pm.message_id, d["chat"], d["msg"], ids))
    elif act == "stop":
        _bc_cancel.add(int(parts[3]))
        await cb.answer("⛔ سيتوقف بعد الدفعة الحالية")
    elif act == "del":
        await cb.answer("🗑 جارٍ الحذف...")
        asyncio.create_task(delete_broadcast(int(parts[3]), cb.message.chat.id))


# =====================================================================
#  users
# =====================================================================
async def user_card(uid: int) -> tuple:
    u = await get_user(uid)
    if not u:
        return "❌ المستخدم غير موجود.", kb([BACK_HOME])
    ords = await db._q("SELECT o.*, c.title FROM orders o LEFT JOIN channels c ON c.id = o.channel_id WHERE o.telegram_id = ? "
                       "ORDER BY o.id DESC LIMIT 15", [uid])
    refs = (await db._q("SELECT COUNT(*) AS n FROM users WHERE referred_by = ? AND ref_counted = 1", [uid]))[0]["n"]
    lines = "\n".join(f"• {esc(o['title'] or o['channel_id'])} — {o['stars']}⭐ {o['source']} {'↩️' if o['status'] == 'refunded' else ''}"
                      for o in ords) or "—"
    text = (f"👤 <b>{esc(u['full_name'] or '-')}</b> @{esc(u['username'] or '-')}\n<code>{uid}</code>\n"
            f"🌐 {u['language']} • 🔞 موافقة: {bahrain_time(u['consent_at']) if u['consent_at'] else 'لا'}\n"
            f"📅 انضم: {bahrain_time(u['created_at'])} • آخر نشاط: {bahrain_time(u['last_active_at'])}\n"
            f"🤝 إحالات صالحة: {refs} • 🚫 محظور: {'نعم' if u['is_banned'] else 'لا'}\n\n🛒 الطلبات:\n{lines}")
    k = [[ibtn(("✅ فك الحظر" if u["is_banned"] else "🚫 حظر"), data=f"a:us:ban:{uid}"), ibtn("🎁 منح قناة", data=f"a:us:grant:{uid}")],
         BACK_HOME]
    return text, kb(k)


async def adm_us(cb: CallbackQuery, parts: list) -> None:
    aid = cb.from_user.id
    act = parts[2] if len(parts) > 2 else "find"
    if act == "find":
        await set_astate(aid, "us_find")
        await cb.answer()
        await show(cb, "👤 أرسل <b>آيدي</b> المستخدم أو @username:", kb([[ibtn("إلغاء", data="a:home")]]))
    elif act == "ban":
        uid = int(parts[3])
        await db._run([("UPDATE users SET is_banned = 1 - is_banned WHERE telegram_id = ?", [uid])])
        await cb.answer("تم")
        text, markup = await user_card(uid)
        await show(cb, text, markup)
    elif act == "grant":
        uid = int(parts[3])
        rows = await db._q("SELECT id, title, button_name FROM channels ORDER BY id")
        await cb.answer()
        k = [[ibtn(f"#{c['id']} {(c['button_name'] or c['title'])[:30]}", data=f"a:us:g:{uid}:{c['id']}")] for c in rows]
        k.append([ibtn("⬅️", data="a:home")])
        await show(cb, "اختر القناة التي تريد منحها للمستخدم:", kb(k))
    elif act == "g":
        uid, cid = int(parts[3]), int(parts[4])
        try:
            await db.tx([("INSERT INTO orders (telegram_id, channel_id, stars, charge_id, source) VALUES (?, ?, 0, ?, 'grant')",
                          [uid, cid, f"grant:{uid}:{cid}:{int(time.time())}"])])
        except DBError:
            await cb.answer("❌ فشل", show_alert=True)
            return
        await log_admin(aid, "grant", f"user={uid} channel={cid}")
        await cb.answer("✅ مُنحت")
        _wake.set()
        text, markup = await user_card(uid)
        await show(cb, text, markup)


# =====================================================================
#  settings
# =====================================================================
SETTING_LABELS = {
    "brand_name": "🏷 الاسم", "support_username": "👤 يوزر الدعم", "trust_chat": "🛡 قناة الثقة",
    "trust_url": "🔗 رابط الثقة", "contest_target": "🎯 هدف الإحالات", "contest_max": "🏅 حد الجوائز",
    "share_text": "💬 نص المشاركة", "shop_text": "📝 نص المتجر", "trend_top": "🔥 عدد الرائجة",
    "webapp_url": "📱 رابط التطبيق", "menu_text": "🔘 اسم زر «فتح»",
}
SETTING_HINT = {
    "trust_chat": "أرسل @يوزر قناة الثقة (البوت مشرف فيها)", "trust_url": "رابط القناة الذي يفتحه زر «دليل الثقة»",
    "contest_target": "عدد الإحالات المطلوبة للجائزة", "contest_max": "أقصى جوائز لكل مستخدم (0 = بلا حد)",
    "share_text": "نص المشاركة. ضع {link} مكان الرابط", "shop_text": "النص فوق أزرار المتجر (- للافتراضي)",
    "trend_top": "كم قناة رائجة تلقائياً (0 = فقط المثبتة)", "webapp_url": "رابط التطبيق الكامل (- للافتراضي)",
    "menu_text": "اسم زر «فتح» بجانب خانة الكتابة", "brand_name": "اسم المنصة", "support_username": "يوزر الدعم بدون @",
}


def settings_markup() -> InlineKeyboardMarkup:
    k = [[ibtn(f"{'🟢' if S('maintenance') == 'true' else '⚪'} الصيانة", data="a:se:tg:maintenance"),
          ibtn(f"{'🟢' if S('contest_enabled') == 'true' else '⚪'} المسابقة", data="a:se:tg:contest_enabled")]]
    items = [ibtn(label, data=f"a:se:ed:{key}") for key, label in SETTING_LABELS.items()]
    for i in range(0, len(items), 2):
        k.append(items[i:i + 2])
    k.append(BACK_HOME)
    return kb(k)


def _v(key: str, default: str = "—") -> str:
    return esc(S(key)[:40]) or default


async def settings_text() -> str:
    return (f"⚙️ <b>الإعدادات</b>\n"
            f"🏷 {_v('brand_name')} · 👤 @{_v('support_username')}\n"
            f"🛡 {_v('trust_chat')} · 🔗 {_v('trust_url')}\n"
            f"🎯 {S('contest_target')} إحالة · 🏅 حد {S('contest_max')} · 🔥 رائجة {S('trend_top')}\n"
            f"📱 {esc(webapp_url()[:45]) or '—'} · 🔘 {_v('menu_text')}")


async def adm_se(cb: CallbackQuery, parts: list) -> None:
    aid = cb.from_user.id
    act = parts[2] if len(parts) > 2 else "home"
    if act == "home":
        await clear_astate(aid)
        await cb.answer()
        await show(cb, await settings_text(), settings_markup())
    elif act == "tg":
        key = parts[3]
        if key in ("maintenance", "contest_enabled"):
            await set_setting(key, "false" if S(key) == "true" else "true")
        await cb.answer("✅")
        await show(cb, await settings_text(), settings_markup())
    elif act == "ed":
        key = parts[3]
        await set_astate(aid, "se_edit", {"key": key})
        await cb.answer()
        cur = S(key)
        await show(cb, f"{SETTING_LABELS.get(key, key)}\n{SETTING_HINT.get(key, '')}" + (f"\n\nالحالي: <code>{esc(cur[:200])}</code>" if cur else ""),
                   kb([[ibtn("إلغاء", data="a:se:home")]]))


# ---- main-menu (reply keyboard) buttons: rename / hide
KB_NAMES = {"kb_app": "محتوى القنوات", "kb_win": "اربح قناة", "kb_mine": "قنواتي", "kb_lang": "اللغة",
            "kb_support": "الدعم", "kb_help": "التعليمات"}


async def adm_kb(cb: CallbackQuery, parts: list) -> None:
    aid = cb.from_user.id
    act = parts[2] if len(parts) > 2 else "list"
    key = parts[3] if len(parts) > 3 else ""
    if key and key not in KB_KEYS:
        await cb.answer()
        return
    if act == "list":
        await clear_astate(aid)
        await cb.answer()
        k = [[ibtn(f"{'🚫' if S('off_' + x) == 'true' else '✅'} {kb_label('ar', x)}", data=f"a:kb:v:{x}")] for x in KB_KEYS]
        k.append(BACK_HOME)
        await show(cb, "🔘 <b>أزرار القائمة الرئيسية</b>", kb(k))
    elif act in ("v", "tg", "rs"):
        if act == "tg":
            await set_setting("off_" + key, "false" if S("off_" + key) == "true" else "true")
        elif act == "rs":
            await set_setting(key, "")
        await cb.answer()
        off = S("off_" + key) == "true"
        await show(cb, f"🔘 <b>{esc(kb_label('ar', key))}</b> · {'مخفي' if off else 'ظاهر'}", kb([
            [ibtn("✏️ تغيير الاسم", data=f"a:kb:ed:{key}"), ibtn("👁 إظهار" if off else "🙈 إخفاء", data=f"a:kb:tg:{key}")],
            [ibtn("↩️ الاسم الافتراضي", data=f"a:kb:rs:{key}"), ibtn("⬅️", data="a:kb:list")]]))
    elif act == "ed":
        await set_astate(aid, "kb_edit", {"key": key})
        await cb.answer()
        await show(cb, "✏️ أرسل الاسم الجديد للزر (مع إيموجي إن أردت)", kb([[ibtn("إلغاء", data=f"a:kb:v:{key}")]]))


# ---- forced-subscription channels (contest gate)
async def sb_view() -> tuple:
    rows = await get_subs()
    k = []
    for r in rows:
        k.append([ibtn(f"🔄 {r['title'][:24]}", data=f"a:sb:rep:{r['id']}"), ibtn("🗑", data=f"a:sb:del:{r['id']}")])
    k.append([ibtn("➕ إضافة قناة", data="a:sb:add")])
    k.append(BACK_HOME)
    return ("🎁 <b>قنوات الاشتراك الإجباري</b>\nتظهر قبل رابط «اربح قناة». 🔄 استبدال · 🗑 حذف" if rows
            else "🎁 <b>قنوات الاشتراك الإجباري</b>\nلا توجد قنوات (المسابقة بلا اشتراك)."), kb(k)


async def adm_sb(cb: CallbackQuery, parts: list) -> None:
    aid = cb.from_user.id
    act = parts[2] if len(parts) > 2 else "list"
    sid = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
    if act == "list":
        await clear_astate(aid)
        await cb.answer()
        text, markup = await sb_view()
        await show(cb, text, markup)
    elif act in ("add", "rep"):
        await set_astate(aid, "sb_add", {"replace": sid})
        await cb.answer()
        await show(cb, "أرسل @يوزر القناة أو حوّل رسالة منها.\nاجعل البوت مشرفاً فيها ليتحقق من الاشتراك.", kb([[ibtn("إلغاء", data="a:sb:list")]]))
    elif act == "del":
        await db._run([("DELETE FROM sub_channels WHERE id = ?", [sid])])
        await cb.answer("🗑")
        text, markup = await sb_view()
        await show(cb, text, markup)


async def sb_save(m: Message, chat_id: Any, replace: int) -> None:
    try:
        ch = await bot.get_chat(chat_id)
        me = await bot.get_chat_member(ch.id, BOT_ID)
    except TelegramAPIError as e:
        await m.answer(f"❌ تعذر الوصول. أضف البوت مشرفاً أولاً.\n<code>{esc(str(e))[:120]}</code>")
        return
    url = f"https://t.me/{ch.username}" if ch.username else None
    if not url:
        try:
            url = (await bot.create_chat_invite_link(ch.id, name="vidox-sub")).invite_link
        except TelegramAPIError:
            await m.answer("❌ القناة خاصة: أعطِ البوت صلاحية الدعوة.")
            return
    chat = f"@{ch.username}" if ch.username else str(ch.id)
    if replace:
        await db._run([("UPDATE sub_channels SET chat = ?, url = ?, title = ? WHERE id = ?", [chat, url, ch.title or chat, replace])])
    else:
        await db._run([("INSERT INTO sub_channels (chat, url, title) VALUES (?, ?, ?)", [chat, url, ch.title or chat])])
    await clear_astate(m.from_user.id)
    text, markup = await sb_view()
    await m.answer("✅ تم\n\n" + text, reply_markup=markup)


# =====================================================================
#  backup / restore / wipe
# =====================================================================
BACKUP_TABLES = ["users", "sub_channels", "channels", "channel_media", "orders", "contest_claims", "events", "settings",
                 "admin_logs", "broadcasts", "broadcast_recipients"]
_restore: dict = {}


async def make_backup() -> bytes:
    dump: dict = {"version": 2, "created_at": iso_now(), "tables": {}}
    for tb in BACKUP_TABLES:
        dump["tables"][tb] = await db._q(f"SELECT * FROM {tb}")
    return gzip.compress(json.dumps(dump, ensure_ascii=False).encode("utf-8"))


async def send_backup(chat_ids: Optional[set] = None, note: str = "") -> int:
    data = await make_backup()
    name = f"vidox_backup_{now_utc().strftime('%Y%m%d_%H%M')}.json.gz"
    ok = 0
    for a in (chat_ids or ADMIN_IDS):
        try:
            await bot.send_document(a, BufferedInputFile(data, name),
                                    caption=f"💾 نسخة احتياطية {note}\n{len(data) // 1024} KB\nللاسترجاع: أرسل الملف مع التعليق <b>{esc(RESTORE_WORD)}</b>")
            ok += 1
        except TelegramAPIError as e:
            log.warning("backup send failed %s: %s", a, e)
    await set_setting("last_backup_at", str(int(time.time())))
    return ok


async def wipe_tables(drop_old: bool = True) -> None:
    rows = await db._q("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_litestream%'")
    keep = {"settings"}
    stmts = []
    for r in rows:
        n = r["name"]
        if n in keep or n.startswith('pay_') or not _IDENT.match(n):
            continue
        stmts.append((f"DROP TABLE IF EXISTS {n}", []) if drop_old else (f"DELETE FROM {n}", []))
    for i in range(0, len(stmts), 20):
        await db._run(stmts[i:i + 20])
    await db.init_schema()


async def restore_dump(dump: dict) -> int:
    await wipe_tables()
    total = 0
    for tb in BACKUP_TABLES:
        rows = dump.get("tables", {}).get(tb) or []
        stmts = []
        for r in rows:
            cols = [c for c in r.keys() if _IDENT.match(c)]
            stmts.append((f"INSERT OR REPLACE INTO {tb} ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})",
                          [r[c] for c in cols]))
        for i in range(0, len(stmts), 100):
            await db._run(stmts[i:i + 100])
        total += len(stmts)
    await load_settings()
    return total


async def adm_bk(cb: CallbackQuery, parts: list) -> None:
    act = parts[2] if len(parts) > 2 else "home"
    if act == "home":
        await cb.answer()
        last = int(S("last_backup_at") or 0)
        await show(cb, f"💾 <b>النسخ الاحتياطي</b>\nيُرسل تلقائياً كل 24 ساعة للمشرفين.\nآخر نسخة: {bahrain_time(iso(datetime.fromtimestamp(last, timezone.utc))) if last else '—'}\n\n"
                       f"♻️ للاسترجاع: أرسل ملف النسخة هنا مع التعليق <b>{esc(RESTORE_WORD)}</b>.",
                   kb([[ibtn("💾 نسخة الآن", data="a:bk:now", style="success")], BACK_HOME]))
    elif act == "now":
        await cb.answer("⏳")
        n = await send_backup({cb.from_user.id}, "(يدوية)")
        await safe_send(cb.message.chat.id, "✅ أُرسلت النسخة." if n else "❌ تعذر الإرسال.")
    elif act == "ok":
        dump = _restore.pop(cb.from_user.id, None)
        if not dump:
            await cb.answer("انتهت الجلسة، أعد إرسال الملف", show_alert=True)
            return
        await cb.answer("⏳")
        await show(cb, "⏳ جارٍ الاسترجاع... لا تغلق البوت.")
        try:
            n = await restore_dump(dump)
            await show(cb, f"✅ تم الاسترجاع ({n} سجل).")
            await log_admin(cb.from_user.id, "restore", str(n))
        except Exception as e:
            log.exception("restore failed")
            await show(cb, f"❌ فشل الاسترجاع: <code>{esc(str(e))[:300]}</code>")
    elif act == "no":
        _restore.pop(cb.from_user.id, None)
        await cb.answer()
        await show(cb, "تم الإلغاء.", kb([BACK_HOME]))


WIPE_PHRASE = "احذف كل شيء"


@router.message(Command("wipe_all"), F.chat.type == "private")
async def cmd_wipe(m: Message) -> None:
    if not is_admin(m.from_user.id):
        return
    await set_astate(m.from_user.id, "wipe_phrase")
    await m.answer(f"⚠️ <b>تحذير:</b> سيُحذف كل شيء (المستخدمون، القنوات، الطلبات...) ولا رجوع إلا بنسخة احتياطية.\n"
                   f"يُنصح بأخذ نسخة أولاً من /admin ← 💾.\n\nللتأكيد اكتب بالضبط:\n<code>{WIPE_PHRASE}</code>")


async def adm_wipe(cb: CallbackQuery, parts: list) -> None:
    await cb.answer()


@router.message(F.chat.type == "private", F.document)
async def on_admin_document(m: Message) -> None:
    if not m.from_user or not is_admin(m.from_user.id):
        return
    info = media_of(m)
    if info and m.forward_origin is not None:
        await on_admin_forward(m)
        return
    if (m.caption or "").strip() != RESTORE_WORD:
        return
    if (m.document.file_size or 0) > 19 * 1024 * 1024:
        await m.answer("❌ الملف أكبر من 20MB (حد تيليجرام للبوتات).")
        return
    try:
        buf = io.BytesIO()
        await bot.download(m.document.file_id, destination=buf)
        dump = json.loads(gzip.decompress(buf.getvalue()).decode("utf-8"))
        if dump.get("version") != 2 or "tables" not in dump:
            raise ValueError("bad format")
    except Exception as e:
        await m.answer(f"❌ ملف غير صالح: <code>{esc(str(e))[:200]}</code>")
        return
    _restore[m.from_user.id] = dump
    counts = ", ".join(f"{k}:{len(v)}" for k, v in dump["tables"].items() if v)
    await m.answer(f"♻️ <b>تأكيد الاسترجاع</b>\nتاريخ النسخة: {bahrain_time(dump.get('created_at'))}\n{counts}\n\n"
                   "⚠️ سيُستبدل كل ما في قاعدة البيانات الحالية.",
                   reply_markup=kb([[ibtn("نعم، استرجع", data="a:bk:ok", style="danger")], [ibtn("إلغاء", data="a:bk:no")]]))


# =====================================================================
#  admin forwards (index content / add channel) and text input
# =====================================================================
def origin_channel(m: Message) -> Optional[tuple]:
    fo = m.forward_origin
    ch = getattr(fo, "chat", None)
    mid = getattr(fo, "message_id", None)
    if ch is not None and mid:
        return ch.id, mid
    return None


@router.message(F.chat.type == "private", F.forward_origin, F.from_user.id.in_(ADMIN_IDS))
async def on_admin_forward(m: Message) -> None:
    aid = m.from_user.id
    st = await get_astate(aid)
    oc = origin_channel(m)
    if st and st["state"] == "sb_add":
        if not oc:
            await m.answer("❌ ليست رسالة محوّلة من قناة.")
            return
        await sb_save(m, oc[0], st["data"].get("replace", 0))
        return
    if st and st["state"] == "ch_add":
        if not oc:
            await m.answer("❌ هذه ليست رسالة محوّلة من قناة.")
            return
        await add_channel_from(m, oc[0])
        return
    info = media_of(m)
    if not (oc and info):
        return
    cid = await index_media(oc[0], oc[1], info)
    if cid:
        n = await media_count(cid)
        await m.reply(f"🎞 فُهرس (القناة #{cid}) • المجموع الآن: {n}")
    else:
        await m.reply("⚠️ هذه القناة غير مضافة في المتجر.")


class AdminStateFilter(Filter):
    async def __call__(self, message: Message) -> Any:
        if not message.from_user or not is_admin(message.from_user.id):
            return False
        if (message.text or "").startswith("/"):
            return False
        row = await get_astate(message.from_user.id)
        return {"astate": row} if row else False


@router.message(F.chat.type == "private", AdminStateFilter())
async def on_admin_input(m: Message, astate: dict) -> None:
    aid = m.from_user.id
    st, data = astate["state"], astate.get("data") or {}
    txt = (m.text or "").strip()
    num = norm_digits(txt)

    if st == "wipe_phrase":
        await clear_astate(aid)
        if txt != WIPE_PHRASE:
            await m.answer("تم الإلغاء (العبارة غير مطابقة).")
            return
        await m.answer("⏳ جارٍ المسح...")
        await wipe_tables()
        await log_admin(aid, "wipe_all")
        await m.answer("✅ تم مسح قاعدة البيانات بالكامل وإنشاء الجداول الجديدة.")
    elif st == "sb_add":
        if not re.match(r"^(@?[A-Za-z][A-Za-z0-9_]{3,}|-?\d{6,})$", num if num.lstrip("-").isdigit() else txt):
            await m.answer("أرسل @يوزر القناة أو حوّل رسالة منها.")
            return
        await sb_save(m, int(num) if num.lstrip("-").isdigit() else "@" + txt.lstrip("@"), data.get("replace", 0))
    elif st == "kb_edit":
        await set_setting(data["key"], txt[:30])
        await clear_astate(aid)
        await m.answer("✅ تم", reply_markup=kb([[ibtn("🔘 الأزرار", data="a:kb:list")], BACK_HOME]))
    elif st == "ch_add":
        target: Any = None
        if re.match(r"^-?\d{6,}$", num):
            target = int(num)
        elif re.match(r"^@?[A-Za-z][A-Za-z0-9_]{3,}$", txt):
            target = "@" + txt.lstrip("@")
        if target is None:
            await m.answer("أرسل @username أو رقم (-100...) أو حوّل رسالة من القناة.")
            return
        try:
            target = (await bot.get_chat(target)).id
        except TelegramAPIError as e:
            await m.answer(f"❌ تعذر الوصول: <code>{esc(str(e))[:150]}</code>")
            return
        await add_channel_from(m, target)
    elif st in ("ch_btn", "ch_price", "ch_desc"):
        cid = data["cid"]
        if st == "ch_btn":
            await db._run([("UPDATE channels SET button_name = ? WHERE id = ?", [txt[:40], cid])])
        elif st == "ch_price":
            if not num.isdigit() or int(num) > 100000:
                await m.answer("أرسل رقماً صحيحاً (0 للمجانية).")
                return
            await db._run([("UPDATE channels SET price_stars = ? WHERE id = ?", [int(num), cid])])
        else:
            await db._run([("UPDATE channels SET custom_desc = ? WHERE id = ?", [None if txt == "-" else txt[:1500], cid])])
        await clear_astate(aid)
        text, markup = await ch_view(cid)
        await m.answer("✅ تم.\n\n" + text, reply_markup=markup)
    elif st == "us_find":
        uid = None
        if num.isdigit():
            uid = int(num)
        else:
            r = await db._q("SELECT telegram_id FROM users WHERE LOWER(username) = ?", [txt.lstrip("@").lower()])
            uid = r[0]["telegram_id"] if r else None
        await clear_astate(aid)
        text, markup = await user_card(uid) if uid else ("❌ لم يُعثر على المستخدم.", kb([BACK_HOME]))
        await m.answer(text, reply_markup=markup)
    elif st == "se_edit":
        key = data["key"]
        val = "" if txt == "-" else txt
        if key in ("shop_text", "webapp_url", "share_text") and txt == "-":
            val = ""
        if key in ("contest_target", "contest_max", "trend_top"):
            if not num.isdigit():
                await m.answer("أرسل رقماً.")
                return
            val = str(max(int(num), 1 if key == "contest_target" else 0))
        if key == "support_username":
            val = val.lstrip("@")
        if key == "trust_chat" and val:
            try:
                ch = await bot.get_chat(val if not re.match(r"^-?\d+$", norm_digits(val)) else int(norm_digits(val)))
                val = str(ch.id)
                if not S("trust_url") and ch.username:
                    await set_setting("trust_url", f"https://t.me/{ch.username}")
            except TelegramAPIError as e:
                await m.answer(f"❌ تعذر الوصول للقناة (أضف البوت مشرفاً): <code>{esc(str(e))[:150]}</code>")
                return
        await set_setting(key, val)
        await clear_astate(aid)
        if key in ("webapp_url", "menu_text"):
            await apply_menu_button()
        await m.answer("✅ تم\n\n" + await settings_text(), reply_markup=settings_markup())
    elif st == "bc_msg":
        await set_astate(aid, "bc_ready", {"aud": data["aud"], "chat": m.chat.id, "msg": m.message_id})
        n = len(await audience_ids(data["aud"]))
        await m.reply(f"هل ترسل هذه الرسالة إلى <b>{n}</b> مستخدم؟",
                      reply_markup=kb([[ibtn("🚀 إرسال", data="a:bc:go", style="success")], [ibtn("إلغاء", data="a:home")]]))


# =====================================================================
#  order delivery / trust channel / revoke
# =====================================================================
_attempts: dict = {}


async def post_trust(o: dict, c: dict) -> None:
    chat = S("trust_chat")
    if not chat or o["source"] != "stars":
        return
    u = await get_user(o["telegram_id"])
    name = mask_name((u or {}).get("full_name") or "")
    text = (f"🛒 <b>عملية شراء جديدة</b>\n\n"
            f"👤 العميل: <b>{esc(name)}</b>\n"
            f"<blockquote>📺 {esc(c['title'] or c['button_name'])}\n⭐ {o['stars']} نجمة\n🕒 {bahrain_time(o['created_at'])}</blockquote>\n"
            f"✅ تم التسليم تلقائياً خلال ثوانٍ.")
    try:
        await bot.send_message(int(chat) if re.match(r"^-?\d+$", chat) else chat, text,
                               reply_markup=kb([[ibtn("🛒 شراء الان", url=bot_link(f"ch_{c['id']}"), style="success")]]))
    except TelegramAPIError as e:
        await alert_admins(f"⚠️ تعذر النشر في قناة الثقة: <code>{esc(str(e))[:200]}</code>", key="trust")
        return
    await db._run([("UPDATE orders SET trust_posted = 1 WHERE id = ?", [o["id"]])])


async def deliver_order(o: dict) -> None:
    claim = await db._run([("UPDATE orders SET delivered = 1 WHERE id = ? AND delivered = 0 AND status = 'paid'", [o["id"]])])
    if not claim[0]["affected"]:
        return
    c = await channel_by_id(o["channel_id"])
    u = await get_user(o["telegram_id"])
    lang = (u or {}).get("language", "ar")
    if not c:
        return
    if not c["join_link"]:
        link = await create_join_link(c["chat_id"])
        if link:
            await db._run([("UPDATE channels SET join_link = ? WHERE id = ?", [link, c["id"]])])
            c["join_link"] = link
    if not c["join_link"]:
        await db._run([("UPDATE orders SET delivered = 0 WHERE id = ?", [o["id"]])])
        await alert_admins(f"⚠️ تعذر إنشاء رابط دخول للقناة #{c['id']} — طلب #{o['id']} ينتظر. تأكد من صلاحيات البوت.", key=f"nolink{c['id']}")
        return
    try:
        await bot.send_message(o["telegram_id"], t(lang, "delivered", title=esc(c["title"] or c["button_name"])),
                               reply_markup=kb([[ibtn(t(lang, "open_ch"), url=c["join_link"], style="success")]]))
    except TelegramForbiddenError:
        pass
    except TelegramAPIError as e:
        n = _attempts.get(o["id"], 0) + 1
        _attempts[o["id"]] = n
        if n < 5:
            await db._run([("UPDATE orders SET delivered = 0 WHERE id = ?", [o["id"]])])
        log.warning("deliver failed #%s: %s", o["id"], e)
        return
    if o["source"] == "stars":
        await alert_admins(f"💰 <b>بيع جديد</b>\n📺 {esc(c['title'])}\n⭐ {o['stars']} • 👤 <code>{o['telegram_id']}</code> "
                           f"{esc((u or {}).get('full_name') or '')}")
    if not o["trust_posted"]:
        await post_trust(o, c)


async def revoke_order(o: dict) -> None:
    c = await channel_by_id(o["channel_id"])
    if c:
        still = await db._q("SELECT 1 AS x FROM orders WHERE telegram_id = ? AND channel_id = ? AND status = 'paid' LIMIT 1",
                            [o["telegram_id"], o["channel_id"]])
        if not still:
            try:
                await bot.ban_chat_member(c["chat_id"], o["telegram_id"])
                await bot.unban_chat_member(c["chat_id"], o["telegram_id"], only_if_banned=True)
            except TelegramAPIError as e:
                log.warning("revoke failed: %s", e)
        u = await get_user(o["telegram_id"])
        await safe_send(o["telegram_id"], "↩️ تم استرداد طلبك وإلغاء الوصول للقناة." if (u or {}).get("language", "ar") == "ar"
                        else "↩️ Your order was refunded and channel access was removed.")
    await db._run([("UPDATE orders SET revoked = 1 WHERE id = ?", [o["id"]])])


async def delivery_loop() -> None:
    while True:
        try:
            rows = await db._q("SELECT * FROM orders WHERE delivered = 0 AND status = 'paid' ORDER BY id LIMIT 20")
            for o in rows:
                await deliver_order(o)
            rows = await db._q("SELECT * FROM orders WHERE status = 'refunded' AND revoked = 0 ORDER BY id LIMIT 20")
            for o in rows:
                await revoke_order(o)
            rows = await db._q("SELECT * FROM orders WHERE delivered = 1 AND trust_posted = 0 AND status = 'paid' "
                               "AND source = 'stars' AND created_at >= ? LIMIT 10", [iso(now_utc() - timedelta(hours=2))])
            if S("trust_chat"):
                for o in rows:
                    c = await channel_by_id(o["channel_id"])
                    if c:
                        await post_trust(o, c)
        except Exception as e:
            log.warning("delivery loop: %s", e)
            await alert_admins(f"⚠️ خطأ في حلقة التسليم: <code>{esc(str(e))[:200]}</code>", key="delivery")
        try:
            await asyncio.wait_for(_wake.wait(), timeout=4)
        except asyncio.TimeoutError:
            pass
        _wake.clear()


async def backup_loop() -> None:
    while True:
        try:
            await load_settings()
            last = int(S("last_backup_at") or 0)
            if time.time() - last >= 24 * 3600:
                await send_backup(note="(تلقائية كل 24 ساعة)")
        except Exception as e:
            log.warning("backup loop: %s", e)
        await asyncio.sleep(600)


async def members_loop() -> None:
    while True:
        await asyncio.sleep(1800)
        try:
            for c in await db._q("SELECT id FROM channels WHERE active = 1"):
                await refresh_channel(c["id"], photo=False)
                await asyncio.sleep(1)
        except Exception as e:
            log.warning("members loop: %s", e)


@router.my_chat_member()
async def on_my_chat_member(ev: ChatMemberUpdated) -> None:
    if ev.chat.type == "private":
        if ev.new_chat_member.status in ("kicked", "left"):
            await db._run([("UPDATE users SET is_active = 0 WHERE telegram_id = ?", [ev.from_user.id])])
        elif ev.new_chat_member.status == "member":
            await db._run([("UPDATE users SET is_active = 1 WHERE telegram_id = ?", [ev.from_user.id])])


@router.error()
async def on_error(ev: ErrorEvent) -> bool:
    log.exception("handler error: %s", ev.exception)
    await alert_admins(f"⚠️ خطأ: <code>{esc(repr(ev.exception))[:300]}</code>", key=type(ev.exception).__name__)
    return True


USER_COMMANDS = [BotCommand(command="start", description="القائمة / Menu"),
                 BotCommand(command="mychannels", description="قنواتي / My channels"),
                 BotCommand(command="support", description="الدعم / Support"),
                 BotCommand(command="terms", description="الشروط / Terms"),
                 BotCommand(command="privacy", description="الخصوصية / Privacy")]


async def apply_menu_button() -> None:
    """The blue "open" button next to the message box (outside the chat) that opens the Mini App."""
    url = webapp_url()
    try:
        if url:
            await bot.set_chat_menu_button(menu_button=MenuButtonWebApp(text=(S("menu_text").strip() or "فتح")[:16],
                                                                         web_app=WebAppInfo(url=url)))
    except Exception as e:
        log.warning("menu button failed: %s", e)


async def main() -> None:
    global db, bot, BOT_ID
    missing = [n for n, v in (("BOT_TOKEN", BOT_TOKEN), ("TURSO_DATABASE_URL", TURSO_DATABASE_URL),
                              ("TURSO_AUTH_TOKEN", TURSO_AUTH_TOKEN)) if not v]
    if missing:
        raise SystemExit("Missing environment variables: " + ", ".join(missing))
    db = DB(TURSO_DATABASE_URL, TURSO_AUTH_TOKEN)
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML, link_preview_is_disabled=True))
    ctx = keep_alive.Ctx(db=db, bot=bot, token=BOT_TOKEN, admin_ids=ADMIN_IDS, bot_username=BOT_USERNAME,
                         support=lambda: S("support_username"), trust_url=lambda: S("trust_url"),
                         maintenance=lambda: S("maintenance") == "true",
                         brand=lambda: S("brand_name"))
    await keep_alive.start_web(ctx, PORT)          # open the port first so Render sees the service live
    cols = [r["name"] for r in await db._q("SELECT name FROM pragma_table_info('users')")]
    if cols and "ref_counted" not in cols:
        log.warning("Old database layout detected -> wiping it for the new platform (as requested)")
        await wipe_tables()
    await db.init_schema()
    await load_settings()
    if not await db._q("SELECT 1 AS x FROM sub_channels LIMIT 1"):
        await db._run([("INSERT INTO sub_channels (chat, url, title) VALUES (?, ?, ?)", [d["chat"], d["url"], d["title"]])
                       for d in DEFAULT_SUBS])
    if S("mig_v3") != "1":
        await db._run([("UPDATE channels SET buy_color = 'success' WHERE buy_color = 'danger'", [])])
        await set_setting("mig_v3", "1")
    me = await bot.get_me()
    BOT_ID = me.id
    log.info("Starting @%s", me.username)
    try:
        await db._run([("UPDATE broadcasts SET status = 'interrupted' WHERE status = 'running'", [])])
    except Exception:
        pass
    try:
        await bot.set_my_commands(USER_COMMANDS)
        for a in ADMIN_IDS:
            try:
                await bot.set_my_commands(USER_COMMANDS + [BotCommand(command="admin", description="لوحة التحكم")],
                                          scope=BotCommandScopeChat(chat_id=a))
            except Exception:
                pass
    except Exception as e:
        log.warning("set commands failed: %s", e)
    await apply_menu_button()
    dp = Dispatcher()
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=False)
    tasks = [asyncio.create_task(f()) for f in (delivery_loop, backup_loop, members_loop)]
    try:
        await dp.start_polling(bot, allowed_updates=["message", "callback_query", "channel_post", "chat_join_request",
                                                      "inline_query", "my_chat_member"])
    finally:
        for tk in tasks:
            tk.cancel()


if __name__ == "__main__":
    asyncio.run(main())
