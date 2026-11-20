# -*- coding: utf-8 -*-
"""
Vidox | فيديوكس - keep-alive web server + Mini App API (aiohttp)
  GET /, /health        -> "alive" (for UptimeRobot)
  GET /app              -> the Mini App (index.html)
  GET /img/c/{id}.jpg   -> channel cover      GET /img/v/{media_id}.jpg -> content thumbnail
  POST /api/init | /api/channel | /api/buy    -> JSON API, authenticated with Telegram initData (HMAC)
"""
import hashlib
import hmac
import io
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional
from urllib.parse import parse_qsl

from aiohttp import web

log = logging.getLogger("vidox.web")
HERE = os.path.dirname(os.path.abspath(__file__))
INDEX_PATH = os.path.join(HERE, "index.html")


@dataclass
class Ctx:
    db: Any
    bot: Any
    token: str
    admin_ids: set
    bot_username: str
    support: Callable
    trust_url: Callable
    maintenance: Callable
    brand: Callable = lambda: "Vidox"


def verify_init_data(init_data: str, token: str) -> Optional[dict]:
    if not init_data or len(init_data) > 4096:
        return None
    try:
        pairs = dict(parse_qsl(init_data, keep_blank_values=True, strict_parsing=True))
    except ValueError:
        return None
    got = pairs.pop("hash", None)
    if not got:
        return None
    check = "\n".join(f"{k}={pairs[k]}" for k in sorted(pairs))
    secret = hmac.new(b"WebAppData", token.encode(), hashlib.sha256).digest()
    if not hmac.compare_digest(hmac.new(secret, check.encode(), hashlib.sha256).hexdigest(), got):
        return None
    try:
        if time.time() - int(pairs.get("auth_date", "0")) > 24 * 3600:
            return None
        user = json.loads(pairs.get("user", ""))
        return user if isinstance(user, dict) and "id" in user else None
    except (ValueError, TypeError):
        return None


def jerr(status: int, code: str, **extra: Any) -> web.Response:
    return web.json_response({"ok": False, "error": code, **extra}, status=status)


def jok(**data: Any) -> web.Response:
    return web.json_response({"ok": True, **data})


_hits: dict = {}


def too_many(uid: int, limit: int = 40, window: float = 10.0) -> bool:
    now = time.monotonic()
    arr = [x for x in _hits.get(uid, []) if now - x < window]
    if len(arr) >= limit:
        _hits[uid] = arr
        return True
    arr.append(now)
    _hits[uid] = arr
    if len(_hits) > 20000:
        _hits.clear()
    return False


async def auth(request: web.Request) -> tuple:
    """-> (error_response | None, tg_user, body, db_user)"""
    ctx: Ctx = request.app["ctx"]
    try:
        body = await request.json()
    except Exception:
        return jerr(400, "bad_json"), None, None, None
    tg = verify_init_data(str(body.get("initData", "")), ctx.token)
    if not tg:
        return jerr(401, "unauthorized"), None, None, None
    uid = int(tg["id"])
    if too_many(uid):
        return jerr(429, "slow_down"), None, None, None
    lang = "ar" if str(tg.get("language_code", "")).lower().startswith("ar") else "en"
    full = ((tg.get("first_name") or "") + " " + (tg.get("last_name") or "")).strip()[:100]
    await ctx.db._run([("INSERT OR IGNORE INTO users (telegram_id, username, full_name, language) VALUES (?, ?, ?, ?)",
                        [uid, tg.get("username"), full, lang])])
    rows = await ctx.db._q("SELECT * FROM users WHERE telegram_id = ?", [uid])
    u = rows[0] if rows else {"telegram_id": uid, "language": lang}
    if u.get("is_banned") and uid not in ctx.admin_ids:
        return jerr(403, "banned"), None, None, None
    if ctx.maintenance() and uid not in ctx.admin_ids:
        return jerr(503, "maintenance"), None, None, None
    return None, tg, body, u


def ready(c: dict) -> bool:
    return bool(c.get("invoice_url")) and c.get("invoice_price") == c.get("price_stars")


async def api_init(request: web.Request) -> web.Response:
    ctx: Ctx = request.app["ctx"]
    err, tg, body, u = await auth(request)
    if err:
        return err
    uid = int(tg["id"])
    rows = await ctx.db._q(
        "SELECT c.*, (SELECT COUNT(*) FROM orders o WHERE o.channel_id = c.id AND o.status = 'paid' AND o.source = 'stars') AS sales, "
        "(SELECT COUNT(*) FROM channel_media m WHERE m.channel_id = c.id) AS media_count "
        "FROM channels c WHERE c.active = 1 ORDER BY c.pinned DESC, sales DESC, c.id")
    owned = {r["channel_id"] for r in await ctx.db._q(
        "SELECT channel_id FROM orders WHERE telegram_id = ? AND status = 'paid'", [uid])}
    hot = {c["id"] for c in sorted((r for r in rows if r["sales"] > 0), key=lambda r: -r["sales"])[:3]}
    out = []
    for c in rows:
        own = c["id"] in owned or int(c["price_stars"]) <= 0
        out.append({"id": c["id"], "name": c["button_name"] or c["title"], "title": c["title"],
                    "desc": (c["custom_desc"] or c["description"] or "")[:400], "count": c["media_count"],
                    "members": c["members"], "price": c["price_stars"], "owned": own, "cover": bool(c["has_photo"]),
                    "pinned": bool(c["pinned"]), "hot": bool(c["pinned"]) or (c["id"] in hot), "invoice": c["invoice_url"] if (ready(c) and not own) else None,
                    "link": c["join_link"] if own else None})
    return jok(lang=u.get("language", "ar"), channels=out, trust=ctx.trust_url(), support=ctx.support(),
               bot=ctx.bot_username, brand=ctx.brand(), accepted=bool(u.get("consent_at")))


async def api_channel(request: web.Request) -> web.Response:
    ctx: Ctx = request.app["ctx"]
    err, tg, body, u = await auth(request)
    if err:
        return err
    try:
        cid = int(body.get("id"))
    except (TypeError, ValueError):
        return jerr(400, "bad_id")
    await ctx.db._run([("INSERT INTO events (user_id, channel_id, kind) VALUES (?, ?, 'view')", [int(tg["id"]), cid])])
    media = await ctx.db._q("SELECT id, title, duration, size FROM channel_media WHERE channel_id = ? "
                            "ORDER BY message_id DESC LIMIT 300", [cid])
    return jok(media=media)


async def api_buy(request: web.Request) -> web.Response:
    ctx: Ctx = request.app["ctx"]
    err, tg, body, u = await auth(request)
    if err:
        return err
    try:
        cid = int(body.get("id"))
    except (TypeError, ValueError):
        return jerr(400, "bad_id")
    rows = await ctx.db._q("SELECT * FROM channels WHERE id = ? AND active = 1", [cid])
    if not rows:
        return jerr(404, "not_found")
    c = rows[0]
    await ctx.db._run([("INSERT INTO events (user_id, channel_id, kind) VALUES (?, ?, 'buy_click')", [int(tg["id"]), cid])])
    if not ready(c):
        return jerr(409, "preparing")
    return jok(invoice=c["invoice_url"])


async def img_channel(request: web.Request) -> web.Response:
    ctx: Ctx = request.app["ctx"]
    try:
        key = f"c{int(request.match_info['id'])}"
    except ValueError:
        raise web.HTTPNotFound()
    data = await ctx.db.get_thumb(key)
    if not data:
        raise web.HTTPNotFound()
    return web.Response(body=data, content_type="image/jpeg", headers={"Cache-Control": "public, max-age=3600"})


async def img_video(request: web.Request) -> web.Response:
    ctx: Ctx = request.app["ctx"]
    try:
        mid = int(request.match_info["id"])
    except ValueError:
        raise web.HTTPNotFound()
    key = f"v{mid}"
    data = await ctx.db.get_thumb(key)
    if not data:
        rows = await ctx.db._q("SELECT thumb_fid FROM channel_media WHERE id = ?", [mid])
        fid = rows[0]["thumb_fid"] if rows else None
        if not fid:
            raise web.HTTPNotFound()
        try:
            buf = io.BytesIO()
            await ctx.bot.download(fid, destination=buf)
            data = buf.getvalue()
            await ctx.db.save_thumb(key, data)
        except Exception as e:
            log.debug("thumb download failed: %s", e)
            raise web.HTTPNotFound()
    return web.Response(body=data, content_type="image/jpeg", headers={"Cache-Control": "public, max-age=86400"})


async def page_app(_r: web.Request) -> web.Response:
    try:
        with open(INDEX_PATH, "r", encoding="utf-8") as f:
            return web.Response(text=f.read(), content_type="text/html", headers={"Cache-Control": "no-cache"})
    except OSError:
        return web.Response(status=500, text="index.html missing")


async def alive(_r: web.Request) -> web.Response:
    return web.Response(text="Vidox is alive ✅")


@web.middleware
async def safe_mw(request: web.Request, handler: Any) -> web.StreamResponse:
    try:
        return await handler(request)
    except web.HTTPException:
        raise
    except Exception as e:  # never leak a bare 500 to the Mini App
        log.exception("web error on %s: %s", request.path, e)
        if request.path.startswith("/api/"):
            return jerr(500, "server", detail=str(e)[:120])
        return web.Response(status=500, text="error")


async def start_web(ctx: Ctx, port: int) -> web.AppRunner:
    app = web.Application(client_max_size=1024 * 64, middlewares=[safe_mw])
    app["ctx"] = ctx
    app.router.add_get("/", alive)
    app.router.add_get("/health", alive)
    app.router.add_get("/app", page_app)
    app.router.add_get(r"/img/c/{id}.jpg", img_channel)
    app.router.add_get(r"/img/v/{id}.jpg", img_video)
    app.router.add_post("/api/init", api_init)
    app.router.add_post("/api/channel", api_channel)
    app.router.add_post("/api/buy", api_buy)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()
    log.info("web server on port %s", port)
    return runner

