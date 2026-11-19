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
                           InlineQueryResultArticle, InputTextMessageContent, KeyboardButton, Message,
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
        buy_color TEXT NOT NULL DEFAULT 'danger', name_color TEXT NOT NULL DEFAULT 'primary',
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
        "consent": "🔞 <b>قبل أن تبدأ</b>\n\nهذه المنصة للبالغين فقط، وبعض القنوات فيها مسلسلات وأفلام لا تناسب القُصَّر.\nبالمتابعة تؤكد أن:\n• عمرك <b>18 سنة أو أكثر</b>.\n• قرأت وتوافق على الشروط (/terms) وسياسة الخصوصية (/privacy).",
        "consent_yes": "عمري 18+ وأوافق ✅",
        "consent_no_btn": "عمري أقل من 18 ❌",
        "consent_no": "عذراً، لا يمكنك استخدام هذا البوت. 🚫",
        "shop": "🎬 <b>قنوات {brand} التعليمية</b>\n\nاختر القناة واشترِ بنجوم تيليجرام ⭐ وتصلك فوراً.\n<i>اضغط اسم القناة للتفاصيل أو «شراء» للدفع مباشرة.</i>",
        "shop_empty": "🎬 لا توجد قنوات متاحة حالياً، عد قريباً ✨",
        "menu_hint": "اختر من القائمة بالأسفل 👇🏻",
        "kb_app": "محتوى القنوات 🎬", "kb_win": "اربح قناة 🎁", "kb_mine": "قنواتي 📦",
        "kb_lang": "اللغة 🌐", "kb_support": "الدعم 🛟", "kb_help": "التعليمات 💡",
        "prev": "◀️ السابق", "next": "التالي ▶️", "back": "◀️ رجوع",
        "buy": "شراء {p} ⭐", "open": "فتح ✅", "open_ch": "فتح القناة ✅", "preparing": "⏳ جارٍ تجهيز رابط الدفع، حاول بعد قليل.",
        "more": "📖 المزيد من التفاصيل", "trust": "🛡 دليل الثقة",
        "card": "📺 <b>{title}</b>\n━━━━━━━━━━\n{desc}\n\n🎞 المحتوى: <b>{count}</b> مقطع\n👥 المشتركون: <b>{members}</b>\n⭐ السعر: <b>{price}</b>\n\n💾 <i>القناة مفتوحة: يمكنك تحويل المحتوى وحفظه بسهولة.</i>\n⚠️ <i>لا يوجد استرداد بعد الشراء.</i>",
        "card_owned": "\n\n✅ <b>أنت تملك هذه القناة.</b>",
        "free_price": "مجانية",
        "delivered": "🎉 <b>تم الشراء بنجاح!</b>\n\n📺 القناة: <b>{title}</b>\nاضغط الزر للانضمام 👇🏻\n\n🔒 الرابط خاص بحسابك، ولن يُقبل غيرك.\n💾 المحتوى مفتوح للتحويل والحفظ.",
        "join_denied": "🔒 هذه القناة لمشتريها فقط.\nاشترِها من البوت ثم اضغط الرابط من جديد 👇🏻",
        "mine_empty": "📦 لا تملك قنوات بعد. تصفّح القنوات من القائمة 🎬",
        "mine": "📦 <b>قنواتك</b>\nاضغط لفتح القناة:",
        "lang_pick": "🌐 اختر اللغة:",
        "banned": "⛔ حسابك محظور.", "maint": "🛠 البوت في صيانة قصيرة، عد بعد قليل.",
        "win_off": "🎁 المسابقة متوقفة حالياً.",
        "win_sub": "🎁 <b>اربح قناة مجاناً!</b>\n\nالخطوة 1️⃣: اشترك في قنوات البوت التالية ثم اضغط «تحقق»:",
        "verify": "تحقق ✅", "sub_no": "لم تشترك بعد في كل القنوات 👆", "sub_err": "تعذر التحقق الآن، حاول بعد قليل.",
        "win": "🎁 <b>اربح قناة مجاناً!</b>\n\n✅ الخطوة 1: الاشتراك في القنوات\n2️⃣ ادعُ <b>{target}</b> أشخاص عبر رابطك (يُحسب المدعو بعد تفاعله الحقيقي مع البوت)\n3️⃣ اختر القناة التي تريدها من القنوات المتاحة للمسابقة\n\n📊 تقدّمك: <b>{valid}/{target}</b> {bar}\n🏆 الجوائز المتاحة لك: <b>{avail}</b>\n\n🔗 رابطك:\n<code>{link}</code>",
        "win_max": "\n\n✅ وصلت للحد الأقصى من الجوائز.",
        "share": "📤 شارك رابط المنحة", "claim": "🎁 استلم قناتك", "refresh": "🔄 تحديث",
        "pick": "🎁 اختر القناة التي تريدها:", "pick_none": "لا توجد قنوات متاحة للمسابقة الآن.",
        "claimed": "🎉 مبروك! حصلت على القناة. سيصلك الرابط خلال لحظات 👇🏻",
        "claim_no": "لا توجد جوائز متاحة لك حالياً.",
        "ref_joined": "👤 انضم صديق عبر رابطك ✅\n📊 تقدّمك: <b>{valid}/{target}</b>",
        "ref_done": "\n🎉 أكملت الهدف! اضغط «اربح قناة 🎁» لاستلام جائزتك.",
        "share_default": "🎁 قنوات تعليمية مميزة (كورسات وشروحات)\nانضم عبر رابطي 👇🏻",
        "share_title": "شارك رابط المنحة", "share_locked": "اشترك في قنوات المسابقة أولاً من البوت",
        "support": "🛟 <b>الدعم</b>\n\nتحتاج مساعدة؟ تواصل مع فريق الدعم 👇🏻", "support_btn": "💬 تواصل مع الدعم", "support_none": "الدعم غير متاح حالياً.",
        "help": "💡 <b>كيف تستخدم البوت؟</b>\n\n1) تصفّح القنوات من القائمة أو من «محتوى القنوات 🎬».\n2) اضغط «شراء ⭐» وادفع بنجوم تيليجرام مباشرة.\n3) يصلك رابط الانضمام الخاص بك فوراً.\n4) القنوات مفتوحة: حوّل المحتوى واحفظه بسهولة.\n5) اربح قناة مجاناً من «اربح قناة 🎁».\n\nأي مشكلة؟ /support",
        "terms": "📜 <b>الشروط والأحكام</b>\n\n1) المنصة للبالغين فقط (18+).\n2) الدفع بنجوم تيليجرام ⭐ فقط، وتصلك القناة فور نجاح الدفع.\n3) القنوات مفتوحة للتحويل والحفظ، ولذلك <b>لا يوجد استرداد بعد الشراء</b>، إلا إذا تعذّر وصول الرابط بسبب خلل تقني فنعالجه لك.\n4) رابط الدخول خاص بحسابك ولا يُقبل غيرك.\n5) يُمنع إعادة بيع المحتوى أو نشره.\n6) قد نوقف الحسابات التي تسيء للخدمة.\n7) للمساعدة: /support",
        "privacy": "🔒 <b>سياسة الخصوصية</b>\n\nنحفظ ما تحتاجه الخدمة فقط: آيدي تيليجرام والاسم/اليوزر واللغة وموافقتك على الشروط ومشترياتك وبيانات الإحالة.\nلا نبيع بياناتك ولا نطلب كلمة مرور تيليجرام أو أكواد الدخول.\nيظهر اسمك مخفياً جزئياً فقط في قناة دليل الثقة عند الشراء.\nلحذف بياناتك: /support",
    },
    "en": {
        "consent": "🔞 <b>Before you start</b>\n\nThis platform is for adults only; some channels contain series and films not suitable for minors.\nBy continuing you confirm that:\n• You are <b>18 or older</b>.\n• You read and agree to the Terms (/terms) and Privacy Policy (/privacy).",
        "consent_yes": "I'm 18+ and I agree ✅",
        "consent_no_btn": "I'm under 18 ❌",
        "consent_no": "Sorry, you can't use this bot. 🚫",
        "shop": "🎬 <b>{brand} learning channels</b>\n\nPick a channel and pay with Telegram Stars ⭐, you get it instantly.\n<i>Tap the name for details or “Buy” to pay directly.</i>",
        "shop_empty": "🎬 No channels available right now, come back soon ✨",
        "menu_hint": "Use the menu below 👇🏻",
        "kb_app": "Channels content 🎬", "kb_win": "Win a channel 🎁", "kb_mine": "My channels 📦",
        "kb_lang": "Language 🌐", "kb_support": "Support 🛟", "kb_help": "Guide 💡",
        "prev": "◀️ Previous", "next": "Next ▶️", "back": "◀️ Back",
        "buy": "Buy {p} ⭐", "open": "Open ✅", "open_ch": "Open channel ✅", "preparing": "⏳ Preparing the payment link, try again shortly.",
        "more": "📖 More details", "trust": "🛡 Trust proof",
        "card": "📺 <b>{title}</b>\n━━━━━━━━━━\n{desc}\n\n🎞 Content: <b>{count}</b> videos\n👥 Members: <b>{members}</b>\n⭐ Price: <b>{price}</b>\n\n💾 <i>The channel is open: you can forward and save the content easily.</i>\n⚠️ <i>No refunds after purchase.</i>",
        "card_owned": "\n\n✅ <b>You own this channel.</b>",
        "free_price": "Free",
        "delivered": "🎉 <b>Purchase successful!</b>\n\n📺 Channel: <b>{title}</b>\nTap the button to join 👇🏻\n\n🔒 The link is tied to your account; nobody else is accepted.\n💾 The content is open to forward and save.",
        "join_denied": "🔒 This channel is for its buyers only.\nBuy it in the bot, then tap the link again 👇🏻",
        "mine_empty": "📦 You don't own any channel yet. Browse the channels from the menu 🎬",
        "mine": "📦 <b>Your channels</b>\nTap to open:",
        "lang_pick": "🌐 Choose your language:",
        "banned": "⛔ Your account is banned.", "maint": "🛠 Short maintenance, please come back soon.",
        "win_off": "🎁 The contest is paused right now.",
        "win_sub": "🎁 <b>Win a channel for free!</b>\n\nStep 1️⃣: join the following channels, then tap “Verify”:",
        "verify": "Verify ✅", "sub_no": "You haven't joined all the channels yet 👆", "sub_err": "Can't verify right now, try again shortly.",
        "win": "🎁 <b>Win a channel for free!</b>\n\n✅ Step 1: joined the channels\n2️⃣ Invite <b>{target}</b> people with your link (an invitee counts after real activity in the bot)\n3️⃣ Pick the channel you want from the contest channels\n\n📊 Progress: <b>{valid}/{target}</b> {bar}\n🏆 Rewards available: <b>{avail}</b>\n\n🔗 Your link:\n<code>{link}</code>",
        "win_max": "\n\n✅ You reached the maximum number of rewards.",
        "share": "📤 Share the gift link", "claim": "🎁 Claim your channel", "refresh": "🔄 Refresh",
        "pick": "🎁 Pick the channel you want:", "pick_none": "No contest channels are available now.",
        "claimed": "🎉 Congratulations! You got the channel. Your link arrives in a moment 👇🏻",
        "claim_no": "You have no rewards available now.",
        "ref_joined": "👤 A friend joined with your link ✅\n📊 Progress: <b>{valid}/{target}</b>",
        "ref_done": "\n🎉 Goal reached! Tap “Win a channel 🎁” to claim your reward.",
        "share_default": "🎁 Premium learning channels (courses & tutorials)\nJoin with my link 👇🏻",
        "share_title": "Share the gift link", "share_locked": "Join the contest channels in the bot first",
        "support": "🛟 <b>Support</b>\n\nNeed help? Contact our support team 👇🏻", "support_btn": "💬 Contact support", "support_none": "Support is not available right now.",
        "help": "💡 <b>How to use the bot</b>\n\n1) Browse channels from the list or “Channels content 🎬”.\n2) Tap “Buy ⭐” and pay with Telegram Stars directly.\n3) You get your personal join link instantly.\n4) Channels are open: forward and save the content easily.\n5) Win a channel for free from “Win a channel 🎁”.\n\nAny problem? /support",
        "terms": "📜 <b>Terms & Conditions</b>\n\n1) Adults only (18+).\n2) Payment is in Telegram Stars ⭐ only; you get the channel right after payment.\n3) Channels are open to forward and save, so <b>there are no refunds after purchase</b>, unless the link failed to reach you because of a technical problem, which we fix for you.\n4) Your join link is tied to your account; nobody else is accepted.\n5) Reselling or republishing the content is forbidden.\n6) We may suspend accounts that abuse the service.\n7) Help: /support",
        "privacy": "🔒 <b>Privacy Policy</b>\n\nWe store only what the service needs: Telegram id, name/username, language, your consent to the terms, purchases and referral data.\nWe don't sell your data and never ask for your Telegram password or login codes.\nYour name appears partially masked only in the trust channel when you buy.\nTo delete your data: /support",
    },
}


def t(lang: Optional[str], key: str, **kw: Any) -> str:
    s = TX.get(lang or "ar", TX["ar"]).get(key) or TX["ar"][key]
    s = s.replace("{brand}", S("brand_name"))
    return s.format(**kw) if kw else s


SUB_CHANNELS = [
    {"chat": "@ZenoX_Tools", "url": "https://t.me/ZenoX_Tools", "title": "ZenoX Tools"},
    {"chat": "@Videox_1", "url": "https://t.me/Videox_1", "title": "Videox"},
]
PAGE_SIZE = 10
COLORS = {"primary": "🔵 أزرق", "success": "🟢 أخضر", "danger": "🔴 أحمر", "default": "⚪ عادي"}


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


def reply_kb(lang: str) -> ReplyKeyboardMarkup:
    rows = []
    if WEBAPP_URL:
        rows.append([KeyboardButton(text=t(lang, "kb_app"), web_app=WebAppInfo(url=WEBAPP_URL))])
    rows.append([KeyboardButton(text=t(lang, "kb_win")), KeyboardButton(text=t(lang, "kb_mine"))])
    rows.append([KeyboardButton(text=t(lang, "kb_lang")), KeyboardButton(text=t(lang, "kb_support")),
                 KeyboardButton(text=t(lang, "kb_help"))])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, is_persistent=True)


def _menu_texts(key: str) -> set:
    return {TX["ar"][key], TX["en"][key]}


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
        return ibtn(t(lang, "buy", p=c["price_stars"]), url=c["invoice_url"], style=c.get("buy_color") or "danger")
    return ibtn(t(lang, "buy", p=c["price_stars"]), data=f"c:nb:{c['id']}", style=c.get("buy_color") or "danger")


SHOP_SQL = ("SELECT c.*, (SELECT COUNT(*) FROM orders o WHERE o.channel_id = c.id AND o.status = 'paid' "
            "AND o.source = 'stars') AS sales FROM channels c WHERE c.active = 1 "
            "ORDER BY c.pinned DESC, sales DESC, c.id ASC")


async def shop_view(uid: int, lang: str, page: int) -> tuple:
    rows = await db._q(SHOP_SQL)
    pages = max((len(rows) + PAGE_SIZE - 1) // PAGE_SIZE, 1)
    page = max(0, min(page, pages - 1))
    chunk = rows[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]
    if not rows:
        return t(lang, "shop_empty"), None
    owned = await owned_ids(uid)
    k = []
    for c in chunk:
        name = (c["button_name"] or c["title"] or "-")[:40]
        k.append([ibtn(name, data=f"c:v:{c['id']}:{page}", style=c.get("name_color") or "primary"),
                  buy_button(lang, c, c["id"] in owned)])
    if pages > 1:
        nav = []
        if page > 0:
            nav.append(ibtn(t(lang, "prev"), data=f"c:p:{page - 1}"))
        nav.append(ibtn(f"{page + 1}/{pages}", data="c:x"))
        if page < pages - 1:
            nav.append(ibtn(t(lang, "next"), data=f"c:p:{page + 1}"))
        k.append(nav)
    return t(lang, "shop"), kb(k)


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
    return esc(d[:350] + ("…" if len(d) > 350 else "")) or "—"


async def send_card(chat_id: int, lang: str, cid: int, page: int = 0) -> None:
    c = await channel_by_id(cid)
    if not c or not c["active"]:
        return
    owned = cid in await owned_ids(chat_id)
    price = t(lang, "free_price") if int(c["price_stars"]) <= 0 else f"{c['price_stars']} ⭐"
    text = t(lang, "card", title=esc(c["title"] or c["button_name"]), desc=card_desc(c),
             count=await media_count(cid), members=c["members"], price=price)
    if owned:
        text += t(lang, "card_owned")
    rows = [[buy_button(lang, c, owned, compact=False)]]
    if WEBAPP_URL:
        rows.append([ibtn(t(lang, "more"), web=f"{WEBAPP_URL}?c={cid}")])
    if S("trust_url"):
        rows.append([ibtn(t(lang, "trust"), url=S("trust_url"))])
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
    m = re.match(r"^ch_(\d+)$", payload or "")
    if m:
        await send_card(uid, lang, int(m.group(1)))
    await send_shop(uid, lang)
    await safe_send(uid, t(lang, "menu_hint"), reply_markup=reply_kb(lang))


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


# ---- reply-keyboard buttons
ALL_MENU = _menu_texts("kb_win") | _menu_texts("kb_mine") | _menu_texts("kb_lang") | _menu_texts("kb_support") | _menu_texts("kb_help")


@router.message(F.chat.type == "private", F.text.in_(ALL_MENU))
async def on_menu(m: Message) -> None:
    user = await gate(m)
    if not user:
        return
    lang, text, uid = user["language"], m.text, m.from_user.id
    if text in _menu_texts("kb_win"):
        await show_contest(uid, user)
    elif text in _menu_texts("kb_mine"):
        await send_mine(uid, lang)
    elif text in _menu_texts("kb_lang"):
        await m.answer(t(lang, "lang_pick"), reply_markup=kb([[ibtn("العربية 🇸🇦", data="lg:ar"), ibtn("English 🇬🇧", data="lg:en")]]))
    elif text in _menu_texts("kb_support"):
        await send_support(uid, lang)
    elif text in _menu_texts("kb_help"):
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
    await send_shop(cb.from_user.id, lang)
    await safe_send(cb.from_user.id, t(lang, "menu_hint"), reply_markup=reply_kb(lang))


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
async def check_membership(uid: int) -> tuple:
    ok, err = True, False
    for ch in SUB_CHANNELS:
        try:
            mem = await bot.get_chat_member(ch["chat"], uid)
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
        k = [[ibtn(ch["title"], url=ch["url"])] for ch in SUB_CHANNELS]
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
        k = [[ibtn(f"🎁 {(c['button_name'] or c['title'])[:40]}", data=f"ct:g:{c['id']}", style=c.get("name_color") or "primary")]
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
        [ibtn("📺 القنوات", data="a:ch:list"), ibtn("📊 الإحصائيات", data="a:st:7")],
        [ibtn("📢 إذاعة", data="a:bc:start"), ibtn("👤 مستخدم", data="a:us:find")],
        [ibtn("⚙️ الإعدادات", data="a:se:home"), ibtn("💾 نسخة احتياطية", data="a:bk:home")],
    ])


@router.message(Command("admin"), F.chat.type == "private")
async def cmd_admin(m: Message) -> None:
    if not is_admin(m.from_user.id):
        return
    await clear_astate(m.from_user.id)
    await m.answer(f"🛠 <b>لوحة تحكم {esc(S('brand_name'))}</b>", reply_markup=home_markup())


# =====================================================================
#  channels management
# =====================================================================
async def ch_list_view() -> tuple:
    rows = await db._q("SELECT * FROM channels ORDER BY id")
    k = []
    for c in rows:
        flags = ("📌" if c["pinned"] else "") + ("🎁" if c["contest"] else "") + ("🙈" if not c["active"] else "")
        k.append([ibtn(f"#{c['id']} {c['button_name'] or c['title']} • {c['price_stars']}⭐ {flags}", data=f"a:ch:v:{c['id']}")])
    k.append([ibtn("➕ إضافة قناة", data="a:ch:add")])
    k.append(BACK_HOME)
    return ("📺 <b>القنوات</b>\nاضغط على قناة لإدارتها." if rows else "📺 لا توجد قنوات بعد."), kb(k)


async def ch_view(cid: int) -> tuple:
    c = await channel_by_id(cid)
    if not c:
        return "❌ القناة غير موجودة.", kb([BACK_HOME])
    n = await media_count(cid)
    sales = (await db._q("SELECT COUNT(*) AS n, COALESCE(SUM(stars),0) AS s FROM orders WHERE channel_id = ? AND status = 'paid' "
                         "AND source = 'stars'", [cid]))[0]
    inv = "✅ جاهز" if invoice_ready(c) else "⏳ بانتظار بوت الدفع"
    text = (f"📺 <b>#{c['id']} — {esc(c['title'])}</b>\n"
            f"🔘 اسم الزر: <b>{esc(c['button_name'])}</b>\n💰 السعر: <b>{c['price_stars']}</b> ⭐ • رابط الدفع: {inv}\n"
            f"👥 الأعضاء: {c['members']} • 🎞 المحتوى المفهرس: {n}\n"
            f"🛒 المبيعات: {sales['n']} ({sales['s']}⭐)\n"
            f"🎨 لون الاسم: {COLORS.get(c['name_color'])} • لون الشراء: {COLORS.get(c['buy_color'])}\n"
            f"🎁 في المسابقة: {'نعم' if c['contest'] else 'لا'} • 📌 مثبتة: {'نعم' if c['pinned'] else 'لا'} • "
            f"👁 ظاهرة: {'نعم' if c['active'] else 'لا'}\n"
            f"📝 وصف مخصص: {'نعم' if c['custom_desc'] else 'لا (يُسحب من القناة)'}\n"
            f"<i>لفهرسة المحتوى: حوّل فيديوهات القناة إلى البوت (فورورد).</i>")
    i = c["id"]
    k = [
        [ibtn("🔘 اسم الزر", data=f"a:ch:btn:{i}"), ibtn("💰 السعر", data=f"a:ch:price:{i}")],
        [ibtn("🎨 لون الاسم", data=f"a:ch:nc:{i}"), ibtn("🎨 لون الشراء", data=f"a:ch:bc:{i}")],
        [ibtn(("🎁 إزالة من المسابقة" if c["contest"] else "🎁 إضافة للمسابقة"), data=f"a:ch:ct:{i}"),
         ibtn(("📌 إلغاء التثبيت" if c["pinned"] else "📌 تثبيت"), data=f"a:ch:pin:{i}")],
        [ibtn(("🙈 إخفاء" if c["active"] else "👁 إظهار"), data=f"a:ch:vis:{i}"), ibtn("📝 الوصف", data=f"a:ch:desc:{i}")],
        [ibtn("🔄 تحديث المعلومات", data=f"a:ch:ref:{i}"), ibtn("🔗 رابط دخول جديد", data=f"a:ch:link:{i}")],
        [ibtn("🧹 مسح الفهرس", data=f"a:ch:clr:{i}"), ibtn("🗑 حذف", data=f"a:ch:del:{i}")],
        [ibtn("⬅️ القنوات", data="a:ch:list")],
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
    await m.answer("✅ تمت إضافة القناة. حدّد سعرها بالنجوم (زر 💰) ليظهر زر الشراء.\n\n" + text, reply_markup=markup)


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
            await show(cb, f"🛠 <b>لوحة تحكم {esc(S('brand_name'))}</b>", home_markup())
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
        await show(cb, "➕ <b>إضافة قناة</b>\n\nأضف البوت مشرفاً في القناة (مع صلاحية دعوة المستخدمين)، ثم:\n"
                       "• حوّل أي رسالة من القناة إلى هنا، أو\n• أرسل @username القناة، أو\n• أرسل رقمها (-100...).",
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
    prompts = {"btn": ("ch_btn", "🔘 أرسل <b>اسم الزر</b> الجديد للقناة:"),
               "price": ("ch_price", "💰 أرسل <b>السعر بالنجوم</b> (رقم، و0 = مجانية):\n<i>سيُنشئ بوت الدفع رابطاً جديداً تلقائياً.</i>"),
               "desc": ("ch_desc", "📝 أرسل <b>الوصف</b> الذي يظهر للعملاء.\nأرسل <code>-</code> للرجوع إلى وصف القناة الأصلي.")}
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
    "brand_name": "🏷 اسم البوت/المنصة (يظهر في الرسائل والميني آب)",
    "support_username": "👤 يوزر الدعم", "trust_chat": "🛡 قناة الثقة (آيدي/@يوزر لنشر الإيصالات)",
    "trust_url": "🔗 رابط قناة الثقة (زر «دليل الثقة»)", "contest_target": "🎯 عدد الإحالات المطلوبة",
    "contest_max": "🏅 أقصى عدد جوائز لكل مستخدم (0 = بلا حد)", "share_text": "💬 نص المشاركة (استخدم {link} لمكان الرابط)",
}


def settings_markup() -> InlineKeyboardMarkup:
    k = [[ibtn(f"{'🟢' if S('maintenance') == 'true' else '⚪'} وضع الصيانة", data="a:se:tg:maintenance")],
         [ibtn(f"{'🟢' if S('contest_enabled') == 'true' else '⚪'} مسابقة «اربح قناة»", data="a:se:tg:contest_enabled")]]
    for key, label in SETTING_LABELS.items():
        k.append([ibtn(label, data=f"a:se:ed:{key}")])
    k.append(BACK_HOME)
    return kb(k)


async def settings_text() -> str:
    return (f"⚙️ <b>الإعدادات</b>\n\n🏷 الاسم: <b>{esc(S('brand_name'))}</b>\n👤 الدعم: @{esc(S('support_username'))}\n🛡 قناة الثقة: <code>{esc(S('trust_chat') or '-')}</code>\n"
            f"🔗 رابط الثقة: {esc(S('trust_url') or '-')}\n🎯 الهدف: {S('contest_target')} • 🏅 الحد: {S('contest_max')}\n"
            f"💬 نص المشاركة: {esc(S('share_text') or '(الافتراضي)')}")


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
        await cb.answer("تم")
        await show(cb, await settings_text(), settings_markup())
    elif act == "ed":
        key = parts[3]
        await set_astate(aid, "se_edit", {"key": key})
        await cb.answer()
        await show(cb, f"{SETTING_LABELS.get(key, key)}\n\nأرسل القيمة الجديدة (أو <code>-</code> للمسح):", kb([[ibtn("إلغاء", data="a:se:home")]]))


# =====================================================================
#  backup / restore / wipe
# =====================================================================
BACKUP_TABLES = ["users", "channels", "channel_media", "orders", "contest_claims", "events", "settings",
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
        if n in keep or not _IDENT.match(n):
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
        if key in ("contest_target", "contest_max"):
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
        await m.answer("✅ تم الحفظ.\n\n" + await settings_text(), reply_markup=settings_markup())
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
    await db.init_schema()
    await load_settings()
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
