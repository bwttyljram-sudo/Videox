# -*- coding: utf-8 -*-
"""
Vidox | فيديوكس  -  keep-alive web server + Mini App API (aiohttp)

It does three jobs on the same port (Render gives the port in $PORT):
  1) GET /  and  GET /health   -> tiny "alive" reply for your external pinger
  2) GET /app                  -> serves the Mini App (index.html)
  3) POST /api/...             -> JSON API used by the Mini App
Every /api call is authenticated by verifying Telegram's signed `initData` (HMAC with the bot token).
"""
import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional
from urllib.parse import parse_qsl

from aiohttp import web
from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError, TelegramRetryAfter

log = logging.getLogger("vidox.web")

HERE = os.path.dirname(os.path.abspath(__file__))
INDEX_PATH = os.path.join(HERE, "index.html")
INIT_DATA_MAX_AGE = 24 * 3600


@dataclass
class Ctx:
    db: Any
    bot: Any
    token: str
    storage_channel_id: int
    admin_ids: set
    payment_bot_username: str
    support_username: str
    bot_username: str
    setting_int: Callable
    setting_bool: Callable
    alert_admins: Callable


# --------------------------------------------------------------------------
#  Telegram initData verification
# --------------------------------------------------------------------------
def verify_init_data(init_data: str, token: str) -> Optional[dict]:
    """Returns the Telegram user dict if the signature is valid and fresh, else None."""
    if not init_data or len(init_data) > 4096:
        return None
    try:
        pairs = dict(parse_qsl(init_data, keep_blank_values=True, strict_parsing=True))
    except ValueError:
        return None
    got_hash = pairs.pop("hash", None)
    if not got_hash:
        return None
    check = "\n".join(f"{k}={pairs[k]}" for k in sorted(pairs))
    secret = hmac.new(b"WebAppData", token.encode(), hashlib.sha256).digest()
    calc = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(calc, got_hash):
        return None
    try:
        if time.time() - int(pairs.get("auth_date", "0")) > INIT_DATA_MAX_AGE:
            return None
        user = json.loads(pairs.get("user", ""))
        if not isinstance(user, dict) or "id" not in user:
            return None
        return user
    except (ValueError, TypeError):
        return None


def jerr(status: int, code: str, **extra: Any) -> web.Response:
    return web.json_response({"ok": False, "error": code, **extra}, status=status)


def jok(**data: Any) -> web.Response:
    return web.json_response({"ok": True, **data})


# --------------------------------------------------------------------------
#  rate limit for the API (per user)
# --------------------------------------------------------------------------
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


# --------------------------------------------------------------------------
#  video delivery (copy_message from the storage channel into the user's chat)
# --------------------------------------------------------------------------
_sending: set = set()


async def copy_one(ctx: Ctx, uid: int, video: dict, protect: bool) -> str:
    for _ in range(3):
        try:
            await ctx.bot.copy_message(
                chat_id=uid,
                from_chat_id=video["channel_id"],
                message_id=video["channel_message_id"],
                protect_content=protect,
            )
            return "ok"
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 0.5)
        except TelegramForbiddenError:
            return "blocked"
        except TelegramAPIError as e:
            msg = str(e).lower()
            if "message to copy not found" in msg or "message_id_invalid" in msg or "not found" in msg:
                try:
                    await ctx.db.update("videos", {"id": f"eq.{video['id']}"}, {"is_broken": True})
                except Exception:
                    pass
                await ctx.alert_admins(
                    "⚠️ مقطع غير موجود في قناة التخزين (تعذر إرساله لمستخدم).\n"
                    f"الحزمة: {video['package_id']} • المقطع: {video['id']}\nقد يحتاج إعادة رفع.",
                    key=f"broken:{video['id']}", every=3600)
                return "missing"
            log.warning("copy_message failed: %s", e)
            return "fail"
    return "fail"


async def send_all_task(ctx: Ctx, uid: int, videos: list, protect: bool) -> None:
    try:
        for v in videos:
            res = await copy_one(ctx, uid, v, protect)
            if res == "blocked":
                break
            await asyncio.sleep(0.7)
    finally:
        _sending.discard(uid)


# --------------------------------------------------------------------------
#  handlers
# --------------------------------------------------------------------------
async def authed_user(request: web.Request) -> Any:
    """Returns (tg_user, db_user) or a Response with the error."""
    ctx: Ctx = request.app["ctx"]
    try:
        body = await request.json()
    except Exception:
        return jerr(400, "bad_json"), None, None
    tg = verify_init_data(str(body.get("initData", "")), ctx.token)
    if not tg:
        return jerr(401, "unauthorized"), None, None
    uid = int(tg["id"])
    if too_many(uid):
        return jerr(429, "slow_down"), None, None
    return None, tg, body


async def ensure_db_user(ctx: Ctx, tg: dict) -> dict:
    uid = int(tg["id"])
    u = await ctx.db.one("users", {"telegram_id": f"eq.{uid}"})
    if u:
        return u
    full = ((tg.get("first_name") or "") + " " + (tg.get("last_name") or "")).strip()[:100]
    res = await ctx.db.rpc("register_user", {
        "p_id": uid, "p_username": tg.get("username"), "p_name": full,
        "p_lang": "en", "p_ref": None, "p_reward": 0})
    return res["user"]


def gate_user(ctx: Ctx, u: dict) -> Optional[web.Response]:
    if u.get("is_banned") and u["telegram_id"] not in ctx.admin_ids:
        return jerr(403, "banned")
    if ctx.setting_bool("maintenance") and u["telegram_id"] not in ctx.admin_ids:
        return jerr(503, "maintenance")
    return None


async def api_init(request: web.Request) -> web.Response:
    ctx: Ctx = request.app["ctx"]
    err, tg, _ = await authed_user(request)
    if err:
        return err
    u = await ensure_db_user(ctx, tg)
    g = gate_user(ctx, u)
    if g:
        return g
    cats = await ctx.db.rpc("app_categories", {"p_user": u["telegram_id"], "p_days": ctx.setting_int("new_badge_days")})
    return jok(
        user={"id": u["telegram_id"], "balance": u["points_balance"], "lang": u["language"],
              "premium": u["is_premium"]},
        categories=cats or [],
        buy_url=(f"https://t.me/{ctx.payment_bot_username}?start=from_vidox" if ctx.payment_bot_username else ""),
    )


async def api_packages(request: web.Request) -> web.Response:
    ctx: Ctx = request.app["ctx"]
    err, tg, body = await authed_user(request)
    if err:
        return err
    u = await ensure_db_user(ctx, tg)
    g = gate_user(ctx, u)
    if g:
        return g
    try:
        cid = int(body.get("category_id"))
    except (TypeError, ValueError):
        return jerr(400, "bad_request")
    pks = await ctx.db.rpc("app_packages", {
        "p_user": u["telegram_id"], "p_category": cid,
        "p_days": ctx.setting_int("new_badge_days"), "p_channel": ctx.storage_channel_id})
    return jok(packages=pks or [], balance=u["points_balance"])


async def api_package(request: web.Request) -> web.Response:
    ctx: Ctx = request.app["ctx"]
    err, tg, body = await authed_user(request)
    if err:
        return err
    u = await ensure_db_user(ctx, tg)
    g = gate_user(ctx, u)
    if g:
        return g
    try:
        pid = int(body.get("package_id"))
    except (TypeError, ValueError):
        return jerr(400, "bad_request")
    pk = await ctx.db.rpc("app_package", {
        "p_user": u["telegram_id"], "p_package": pid, "p_channel": ctx.storage_channel_id})
    if not pk:
        return jerr(404, "not_found")
    return jok(package=pk, balance=u["points_balance"])


async def api_purchase(request: web.Request) -> web.Response:
    ctx: Ctx = request.app["ctx"]
    err, tg, body = await authed_user(request)
    if err:
        return err
    u = await ensure_db_user(ctx, tg)
    g = gate_user(ctx, u)
    if g:
        return g
    try:
        pid = int(body.get("package_id"))
    except (TypeError, ValueError):
        return jerr(400, "bad_request")
    res = await ctx.db.rpc("purchase_package", {"p_user": u["telegram_id"], "p_package": pid})
    if not res or not res.get("ok"):
        e = (res or {}).get("error", "failed")
        return jerr(409 if e in ("insufficient", "owned", "free") else 400, e,
                    balance=(res or {}).get("balance"), price=(res or {}).get("price"))
    return jok(balance=res["balance"], price=res["price"])


async def api_send(request: web.Request) -> web.Response:
    ctx: Ctx = request.app["ctx"]
    err, tg, body = await authed_user(request)
    if err:
        return err
    u = await ensure_db_user(ctx, tg)
    g = gate_user(ctx, u)
    if g:
        return g
    uid = u["telegram_id"]
    try:
        pid = int(body.get("package_id"))
    except (TypeError, ValueError):
        return jerr(400, "bad_request")
    pk = await ctx.db.one("packages", {"id": f"eq.{pid}", "status": "eq.active"})
    if not pk:
        return jerr(404, "not_found")
    if pk["price_in_points"] > 0:
        sub = await ctx.db.one("user_subscriptions", {"telegram_id": f"eq.{uid}", "package_id": f"eq.{pid}"})
        if not sub:
            return jerr(403, "not_owned")
    vid = body.get("video_id")
    filters = {"package_id": f"eq.{pid}"}
    if vid not in (None, "", "all"):
        try:
            filters["id"] = f"eq.{int(vid)}"
        except (TypeError, ValueError):
            return jerr(400, "bad_request")
    videos = await ctx.db.select("videos", "id,package_id,channel_id,channel_message_id",
                                 filters, order="sort_order.asc,id.asc", limit=1000)
    if not videos:
        return jerr(404, "no_videos")
    if any(v["channel_id"] != ctx.storage_channel_id for v in videos):
        return jerr(503, "maintenance_package")
    protect = not pk["allow_save"]
    if len(videos) == 1:
        res = await copy_one(ctx, uid, videos[0], protect)
        if res == "ok":
            return jok(sent=1, queued=False)
        if res == "blocked":
            return jerr(409, "start_bot")
        return jerr(502, "send_failed")
    if uid in _sending:
        return jerr(429, "busy")
    _sending.add(uid)
    asyncio.create_task(send_all_task(ctx, uid, videos, protect))
    return jok(sent=len(videos), queued=True)


async def thumb(request: web.Request) -> web.Response:
    ctx: Ctx = request.app["ctx"]
    try:
        vid = int(request.match_info["vid"])
    except ValueError:
        return web.Response(status=404)
    data = await ctx.db.get_thumb(vid)
    if not data:
        return web.Response(status=404)
    return web.Response(body=data, content_type="image/jpeg",
                        headers={"Cache-Control": "public, max-age=86400"})


async def health(request: web.Request) -> web.Response:
    return web.Response(text="Vidox is alive ✅")


async def serve_app(request: web.Request) -> web.Response:
    try:
        with open(INDEX_PATH, "rb") as f:
            data = f.read()
    except OSError:
        return web.Response(text="index.html not found", status=500)
    return web.Response(body=data, content_type="text/html", charset="utf-8",
                        headers={"Cache-Control": "no-cache"})


@web.middleware
async def safety_mw(request: web.Request, handler):
    try:
        resp = await handler(request)
    except web.HTTPException:
        raise
    except Exception as e:
        log.exception("api error: %s", e)
        return jerr(500, "server_error")
    resp.headers["X-Content-Type-Options"] = "nosniff"
    return resp


async def start_web(ctx: Ctx, port: int) -> web.AppRunner:
    app = web.Application(middlewares=[safety_mw], client_max_size=64 * 1024)
    app["ctx"] = ctx
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    app.router.add_head("/", health)
    app.router.add_head("/health", health)
    app.router.add_get("/app", serve_app)
    app.router.add_get("/thumb/{vid}.jpg", thumb)
    app.router.add_post("/api/init", api_init)
    app.router.add_post("/api/packages", api_packages)
    app.router.add_post("/api/package", api_package)
    app.router.add_post("/api/purchase", api_purchase)
    app.router.add_post("/api/send", api_send)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info("Web server listening on port %s", port)
    return runner

