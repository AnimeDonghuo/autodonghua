"""
Core of the AutoDonghua bot — client, shared state and helpers.
"""
import asyncio
import os
import time
from urllib.parse import urlparse

import aiohttp
import re
from telethon import TelegramClient, events
from telethon.errors.rpcerrorlist import UserNotParticipantError
from telethon.tl.functions.channels import GetParticipantRequest
from telethon.tl.functions.messages import (CheckChatInviteRequest,
                                            ExportChatInviteRequest)

from config import CONFIG, DOWNLOAD_DIR, THUMB_DIR
from db import Store
from style import RESTRICTED_MSG, escape_html, small_caps

store = Store()

from telethon.network import ConnectionTcpAbridged

app = TelegramClient("autodonghua_bot", CONFIG.get("api_id"),
                     CONFIG.get("api_hash"),
                     connection=ConnectionTcpAbridged,
                     flood_sleep_threshold=60)

bot_user = None  # set at startup (our own bot account)

# Optional health HTTP server (Koyeb: keeps the free-tier instance
# responsive on PORT and visible in the dashboard)
health_app = None


# ------------------------------------------------------------------ #
# VPS worker bridge: an off-site download+upload node that archives
# files into the DB channel while Koyeb keeps the bot brain (cards,
# posters, DMs, registry). The worker never logs in as the main bot.
# ------------------------------------------------------------------ #
VPS_BEAT_SECONDS = 90


async def vps_state():
    v = dict(await store.get_config("vps") or {})
    v["online"] = bool(v.get("secret")) and \
        (time.time() - (v.get("beat") or 0)) < VPS_BEAT_SECONDS
    return v


async def vps_beat(secret, caps=None):
    v = dict(await store.get_config("vps") or {})
    if not v.get("secret") or secret != v.get("secret"):
        return False
    v["beat"] = time.time()
    if caps:
        v["caps"] = caps
    await store.set_config("vps", v)
    return True


async def vps_claim(secret):
    v = dict(await store.get_config("vps") or {})
    if not v.get("secret") or secret != v.get("secret"):
        return None
    docs = await store.find("vps_jobs", {"status": "pending"},
                            limit=20, sort=[("ts", 1)])
    if not v.get("enabled"):
        # mode OFF: only per-batch VPS picks (force) may run
        docs = [d for d in docs if d.get("force")]
    if not docs:
        return None
    d = dict(docs[0])
    await store.col("vps_jobs").update_one(
        {"_id": d["_id"]},
        {"$set": {"status": "running", "started": time.time()}})
    return {"job": d,
            "creds": {"api_id": CONFIG.get("api_id"),
                      "api_hash": CONFIG.get("api_hash"),
                      "token": v.get("token")}}


async def vps_progress(secret, payload):
    """Live download/upload progress from the worker — mirrored
    into the job dict so /status & the owner panel show it exactly
    like a local Koyeb job."""
    v = dict(await store.get_config("vps") or {})
    if not v.get("secret") or secret != v.get("secret"):
        return False
    jid = (payload or {}).get("job")
    if not jid:
        return False
    await store.col("vps_jobs").update_one(
        {"_id": jid},
        {"$set": {"prog": {"phase": payload.get("phase"),
                           "done": int(payload.get("done") or 0),
                           "total": int(payload.get("total") or 0),
                           "speed": float(payload.get("speed") or 0),
                           "ts": time.time()}}})
    return True


async def vps_report(secret, payload):
    v = dict(await store.get_config("vps") or {})
    if not v.get("secret") or secret != v.get("secret"):
        return False
    jid = (payload or {}).get("job")
    if not jid:
        return False
    ok = bool((payload or {}).get("ok"))
    res = (payload or {}).get("result") or {}
    sets = {"status": "done" if ok else "failed",
            "finished": time.time()}
    if ok:
        sets["result"] = res
        v["jobs"] = (v.get("jobs") or 0) + 1
        v["bytes"] = (v.get("bytes") or 0) + int(res.get("size") or 0)
        sp = float(res.get("speed") or 0)
        if sp > 0:
            n = v["jobs"]
            v["speed"] = round(
                ((float(v.get("speed") or 0) * (n - 1)) + sp) / n, 2)
    else:
        sets["error"] = str((payload or {}).get("error") or "")[:200]
    doc = await store.find("vps_jobs", {"_id": jid})
    doc = doc[0] if doc else {}
    await store.col("vps_jobs").update_one({"_id": jid},
                                           {"$set": sets})
    await store.set_config("vps", v)
    if not ok:  # the owner must always know what went wrong
        try:
            import autodl
            asyncio.ensure_future(autodl._owner_dm(
                f"❌ <b>VPS job failed</b> — {doc.get('series', '?')} "
                f"Ep {doc.get('ep', '?')} "
                f"[{doc.get('quality', '?')}]:\n"
                f"<code>{escape_html(sets['error'])}</code>\n"
                f"<i>Koyeb falls back to doing it itself.</i>"))
        except Exception:
            pass
    return True


async def start_health_server():
    """Serve a tiny health endpoint on $PORT (default 8000)."""
    global health_app
    try:
        from aiohttp import web
    except Exception:
        return
    port = int(os.environ.get("PORT") or 8000)

    async def root(request):
        return web.json_response({
            "bot": "autodonghua",
            "status": "ok",
            "storage": store.mode,
            "uptime_s": round(time.time() - START_TS),
        })

    async def vps_job_h(request):
        secret = request.headers.get("X-VPS-Secret") or \
            request.query.get("secret")
        return web.json_response(await vps_claim(secret)
                                 or {"job": None})

    async def vps_beat_h(request):
        try:
            p = await request.json()
        except Exception:
            p = {}
        return web.json_response(
            {"ok": await vps_beat(p.get("secret"), p.get("caps"))})

    async def vps_report_h(request):
        try:
            p = await request.json()
        except Exception:
            p = {}
        return web.json_response(
            {"ok": await vps_report(p.get("secret"), p)})

    health_app = web.Application()
    health_app.router.add_get("/", root)
    health_app.router.add_get("/health", root)
    health_app.router.add_get("/vps/job", vps_job_h)
    health_app.router.add_post("/vps/beat", vps_beat_h)
    health_app.router.add_post("/vps/report", vps_report_h)

    async def vps_progress_h(request):
        try:
            p = await request.json()
        except Exception:
            p = {}
        return web.json_response(
            {"ok": await vps_progress(p.get("secret"), p)})

    health_app.router.add_post("/vps/progress", vps_progress_h)
    runner = web.AppRunner(health_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    try:
        await site.start()
        print(f"🌐 Health endpoint listening on 0.0.0.0:{port}")
    except Exception as exc:
        print(f"[health] could not bind port {port}: {exc}")


START_TS = time.time()

# In-memory runtime state
PENDING = {}   # post key -> pending post dict (confirmation flow)
WAITERS = {}   # chat_id -> (asyncio.Future, filter_fn) for conversations
AUTH = set()   # staff user ids that passed /login
LOAD = asyncio.Lock()   # serializes file processing (protects free-tier CPU/RAM)
DEFAULT_PASS = "AYU"


# ---------------------------------------------------------------------- #
# Staff login (passgate for the admin panel)
# ---------------------------------------------------------------------- #
async def load_auth():
    docs = await store.find("auth")
    AUTH.update(int(d["_id"]) for d in docs
                if d.get("_id") is not None)


def authed(user_id: int) -> bool:
    return int(user_id) in AUTH


async def login(user_id: int):
    AUTH.add(int(user_id))
    await store.col("auth").update_one(
        {"_id": int(user_id)}, {"$set": {"at": _now()}}, upsert=True)


async def logout(user_id: int):
    AUTH.discard(int(user_id))
    await store.col("auth").delete_one({"_id": int(user_id)})


def norm_channel_id(value):
    """Any channel-id spelling -> the full -100… form (None if not
    a number): bare positive, bare negative, full negative or
    full positive."""
    try:
        num = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    if num < 0:
        return num if num <= -1000000000000 else num - 1000000000000
    return -num if num >= 1000000000000 else -1000000000000 - num


async def resolve_channel(value):
    """Resolve a channel from @username, public/invite link or ANY
    id spelling.

    A raw -100… id carries no access_hash, and Telethon's entity
    cache is empty after every Koyeb redeploy — so get_entity(id)
    fails even when the bot is a full admin. The dialogs scan fixes
    that: every channel the bot is a member of is listed there with
    its access_hash.
    """
    value = str(value).strip()
    if not value:
        raise ValueError("empty channel value")
    # 1) username / public link / already-cached entity
    try:
        return await app.get_entity(value)
    except Exception:
        pass
    # 2) invite link (bot already inside -> ChatInviteAlready)
    m = re.search(r"(?:joinchat/|\+)([A-Za-z0-9_\-]{10,})", value)
    if m:
        try:
            res = await app(CheckChatInviteRequest(hash=m.group(1)))
            chat = getattr(res, "chat", None)
            if chat is not None:
                return chat
        except Exception:
            pass
    # 3) integer ids in every spelling
    full = norm_channel_id(value)
    if full is not None:
        try:
            return await app.get_entity(full)
        except Exception:
            pass
        # 4) dialogs scan — sees every channel the bot belongs to
        try:
            async for d in app.iter_dialogs():
                if d.id == full:
                    return d.entity
        except Exception:
            pass
    raise ValueError(f"cannot resolve channel {value!r}")


async def get_pass() -> str:
    return (await store.get_config("admin_pass")) or DEFAULT_PASS


async def is_staff(user_id: int) -> bool:
    """Owner, listed admin, or pass-logged-in user."""
    if is_owner(user_id) or authed(user_id):
        return True
    return await store.is_admin(int(user_id))


async def require_staff(e):
    """Login-gated staff check for every admin command."""
    if await is_staff(e.sender_id):
        return True
    if is_owner(e.sender_id) or await store.is_admin(e.sender_id):
        await e.respond(
            "🔒 <b>Staff Login Required</b>\n\n"
            "Send: <code>/login &lt;password&gt;</code>",
            parse_mode="html")
        return False
    await e.respond(RESTRICTED_MSG, parse_mode="html")
    return False


# ---------------------------------------------------------------------- #
# Permission helpers
# ---------------------------------------------------------------------- #
def is_owner(user_id: int) -> bool:
    return int(user_id) in (CONFIG.get("owner_ids") or [])


async def is_admin(user_id: int) -> bool:
    if is_owner(user_id):
        return True
    return await store.is_admin(int(user_id))


async def require_admin(e):
    """Reply with the styled restricted message if not admin/owner."""
    if await is_admin(e.sender_id):
        return True
    await e.respond(RESTRICTED_MSG, parse_mode="html")
    return False


# ---------------------------------------------------------------------- #
# Conversation helper (used by /recover, custom-image flow, ...)
# ---------------------------------------------------------------------- #
async def wait_for(chat_id, timeout=120, filt=None):
    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    WAITERS[chat_id] = (fut, filt)
    try:
        return await asyncio.wait_for(fut, timeout)
    finally:
        WAITERS.pop(chat_id, None)


async def resolve_waiters(e):
    w = WAITERS.get(e.chat_id)
    if w:
        fut, filt = w
        if not fut.done() and (filt is None or filt(e)):
            fut.set_result(e)
            return True
    return False


# ---------------------------------------------------------------------- #
# Force-subscribe helpers
# ---------------------------------------------------------------------- #
_MEM_CACHE = {}  # (chat, user) -> (ts, result) — force-sub checks are
# the hot path when users tap file links; cache 5 min per pair


async def member_status(chat_id: int, user_id: int):
    """True = member, False = not a member, None = unable to check."""
    key = (int(chat_id), int(user_id))
    now = time.time()
    hit = _MEM_CACHE.get(key)
    if hit and now - hit[0] < 300:
        return hit[1]
    try:
        res = (True if await app(GetParticipantRequest(
            channel=chat_id, user_id=user_id)) else None)
    except UserNotParticipantError:
        res = False
    except Exception:
        return None
    _MEM_CACHE[key] = (now, res)
    if len(_MEM_CACHE) > 4096:
        _MEM_CACHE.clear()
    return res


async def invite_link(chat_id: int, join_request: bool = False):
    """Invite link; with join_request=True it becomes a
    'send join request' link (Telegram's request-to-join flow)."""
    kwargs = {"peer": chat_id, "legacy_revoke_permanent": False,
              "request_needed": join_request}
    try:
        r = await app(ExportChatInviteRequest(**kwargs))
        return getattr(r, "link", None)
    except TypeError:
        try:
            r = await app(ExportChatInviteRequest(peer=chat_id))
            return getattr(r, "link", None)
        except Exception:
            return None
    except Exception:
        return None


async def forcesub_check(user_id: int):
    """Returns (ok, settings). ok=True when the user may proceed."""
    fs = await store.get_forcesub()
    mode = (fs or {}).get("mode") or "off"
    channels = (fs or {}).get("channels") or []
    if mode == "off" or not channels:
        return True, fs
    if mode == "strict":
        for ch in channels:
            status = await member_status(ch, user_id)
            if status is False:
                return False, fs
            if status is None:  # bot can't check -> fail open
                continue
        return True, fs
    if mode == "request":
        # user must be a member; join-request flow is offered below
        for ch in channels:
            status = await member_status(ch, user_id)
            if status is False:
                return False, fs
        return True, fs
    return True, fs


# ---------------------------------------------------------------------- #
# Image helpers
# ---------------------------------------------------------------------- #
async def url_bytes(url: str) -> bytes:
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25)) as s:
        async with s.get(url, headers={"User-Agent": "Mozilla/5.0"}) as r:
            if r.status != 200:
                raise RuntimeError(f"HTTP {r.status}")
            return await r.read()


async def image_bytes(img_doc) -> bytes:
    """Return raw image bytes from an images-collection document."""
    if not img_doc:
        return None
    if img_doc.get("type") == "url":
        return await url_bytes(img_doc["url"])
    if img_doc.get("type") == "photo":
        from telethon.tl.types import InputPhoto
        photo = InputPhoto(int(img_doc["id"]), int(img_doc["access_hash"]),
                           img_doc.get("file_reference", b""))
        path = os.path.join(THUMB_DIR, f"img_{img_doc['id']}.jpg")
        try:
            await app.download_media(photo, file=path)
            with open(path, "rb") as f:
                return f.read()
        except Exception:
            return None
    return None


def save_thumb(key: str, data: bytes) -> str:
    os.makedirs(THUMB_DIR, exist_ok=True)
    path = os.path.join(THUMB_DIR, f"{key}.jpg")
    with open(path, "wb") as f:
        f.write(data)
    return path


def image_host(url: str) -> str:
    try:
        return urlparse(url).netloc.replace("www.", "")
    except Exception:
        return url


# ---------------------------------------------------------------------- #
# Misc
# ---------------------------------------------------------------------- #
def is_video_doc(doc) -> bool:
    mime = getattr(doc, "mime_type", "") or ""
    name = (getattr(doc, "name", "") or "").lower()
    return mime.startswith("video/") or name.endswith(
        (".mkv", ".mp4", ".avi", ".webm", ".mov", ".m4v", ".ts", ".mpd"))


def now_key(uid: int) -> str:
    return f"p{uid}{int(time.time())}"


def styled_restricted():
    return RESTRICTED_MSG


def log_line(*parts) -> str:
    return " ".join(str(p) for p in parts)


def mention(user_id: int, name: str = None) -> str:
    if name:
        return f'<a href="tg://user?id={user_id}">{escape_html(name)}</a>'
    return f'<a href="tg://user?id={user_id}">{user_id}</a>'


def install_waiter_hook():
    """Catch-all handler (registered first) that resolves conversation waiters."""
    @app.on(events.NewMessage(incoming=True))
    async def _waiter_hook(e):
        await resolve_waiters(e)


def _now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------- #
# FloodWait-safe replies
# ---------------------------------------------------------------------- #
_FLOOD = {}  # chat_id -> unix ts until which Telegram refuses sends


def flood_left(chat_id) -> float:
    return max(0.0, _FLOOD.get(chat_id, 0) - time.time())


def flood_mark(chat_id, seconds) -> None:
    _FLOOD[chat_id] = time.time() + seconds
    print(f"[core] floodwait: chat {chat_id} blocked for {seconds}s")


async def flood_respond(e, *args, **kwargs):
    """e.respond() that survives FloodWaitError: the reply is
    scheduled for right after the flood window instead of crashing
    the handler with an unhandled traceback."""
    from telethon.errors import FloodWaitError
    try:
        return await e.respond(*args, **kwargs)
    except FloodWaitError as fw:
        flood_mark(getattr(e, "chat_id", 0) or 0, fw.seconds)
        asyncio.ensure_future(_late_call(e, "respond", fw.seconds,
                                          args, kwargs))
        return None


async def flood_edit(e, *args, **kwargs):
    """Same for e.edit() (callback queries)."""
    from telethon.errors import FloodWaitError
    try:
        return await e.edit(*args, **kwargs)
    except FloodWaitError as fw:
        flood_mark(getattr(e, "chat_id", 0) or 0, fw.seconds)
        asyncio.ensure_future(_late_call(e, "edit", fw.seconds,
                                         args, kwargs))
        return None


_THUMB_CACHE = {}


def thumb_16x9(data):
    """Center-crop poster bytes to 16:9 so the video thumb fills the
    player frame — no tall poster on a blank page. Falls back to the
    original bytes if PIL is unavailable."""
    if not data:
        return data
    import hashlib
    key = hashlib.md5(data).hexdigest()
    if key in _THUMB_CACHE:
        return _THUMB_CACHE[key]
    out = data
    try:
        import io as _io
        from PIL import Image
        img = Image.open(_io.BytesIO(data)).convert("RGB")
        w, h = img.size
        target = 16 / 9
        if w / h > target:
            nw = int(round(h * target))
            box = ((w - nw) // 2, 0, (w - nw) // 2 + nw, h)
        else:
            nh = int(round(w / target))
            box = (0, (h - nh) // 2, w, (h - nh) // 2 + nh)
        buf = _io.BytesIO()
        img.crop(box).save(buf, "JPEG", quality=85)
        out = buf.getvalue()
    except Exception as exc:
        print("[core] 16:9 thumb crop failed:", repr(exc)[:100])
    _THUMB_CACHE[key] = out
    return out


def fname_attrs(caption, path=None, default="video"):
    """DocumentAttributeFilename so the stored file carries the SAME
    name as the description written on the message. Telethon merges
    attributes per class, so the inferred DocumentAttributeVideo is
    preserved — always real video media with its thumbnail, never a
    plain document."""
    import os as _os
    import re as _re
    from telethon.tl.types import DocumentAttributeFilename
    base = (caption or "").strip().splitlines()[0].strip() if caption \
        else ""
    base = _re.sub(r'[\\/:*?"<>|\r\n\t]', "-", base)
    base = _re.sub(r"\s+", " ", base).strip()[:150] or default
    ext = ""
    if path:
        ext = _os.path.splitext(str(path))[1][:8]
    if not ext:
        ext = ".mp4"
    if not base.lower().endswith(ext.lower()):
        base += ext
    return [DocumentAttributeFilename(file_name=base)]


async def flood_send_file(e, *args, **kwargs):
    """send_file into the event's chat, FloodWait-safe (the backup
    flow used respond(file=, caption=) which send_message rejects)."""
    from telethon.errors import FloodWaitError
    try:
        return await e.client.send_file(e.chat_id, *args, **kwargs)
    except FloodWaitError as fw:
        flood_mark(e.chat_id or 0, fw.seconds)
        asyncio.ensure_future(_late_send_file(e, fw.seconds, args,
                                              kwargs))
        return None


async def _late_send_file(e, seconds, args, kwargs):
    await asyncio.sleep(seconds + 10)
    try:
        await e.client.send_file(e.chat_id, *args, **kwargs)
    except Exception as exc:
        print("[core] delayed send_file failed:", repr(exc)[:160])


async def _late_call(e, method, seconds, args, kwargs):
    await asyncio.sleep(seconds + 10)
    try:
        await getattr(e, method)(*args, **kwargs)
    except Exception as exc:
        print(f"[core] delayed {method} failed:", repr(exc)[:160])
