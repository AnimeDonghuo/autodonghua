"""
AutoDL engine — watches the Parciv donghua API for new episodes,
downloads them one-by-one, archives every quality in the TG DB
channel, and posts to the configured channels with:
  • poster image (resolved from the series name)
  • full info caption (genres, rating, year, quality, size)
  • the file-store share link
  • quality buttons (force-subscribe gated)

Dedupe (the ytdown pattern): a ``downloaded`` registry in MongoDB
holds one row per (series, episode, quality).  It is loaded into
memory at boot and written fire-and-forget after every successful
archive, so the watcher never downloads or posts the same episode
again — even after a redeploy.  If episode 1 is registered and the
API lists episodes 1 and 2, only episode 2 is downloaded.
"""
import asyncio
import base64
import io
import os
import re
import resource
import secrets
import shutil
import time

import aiohttp
from telethon.tl.custom import Button
from telethon.errors import FloodWaitError
from telethon import utils as tl_utils
from telethon.tl.types import (DocumentAttributeVideo)

from config import CONFIG, DOWNLOAD_DIR
import core as core_mod
from core import (LOAD, app, forcesub_check, invite_link,
                  store, url_bytes)
from handlers_user import forcesub_gate
from mediainfo_util import analyze, human_size
from parser import normalize_key
from sources import build_metadata
from style import escape_html, post_caption

API_ROOT = "https://api.parciv.workers.dev"
BUILD = "2026-10-21-d"  # stamp shown in /status — proves deploy freshness
DEFAULT_SECTIONS = ["donghua", "anime"]
DEFAULT_QUALITIES = ["720p", "1080p"]
KEEP_SEEN = 300
# The free API host sleeps when idle: the first call can 502 or
# hang while it wakes, so every read is retried with backoff.
API_RETRY_DELAYS = (2, 8, 20)
MIN_FREE_DISK = 150 * 1024 * 1024  # keep a safety margin


# ---------------------------------------------------------------------- #
# API
# ---------------------------------------------------------------------- #
async def _fetch_one(url: str):
    for attempt, delay in enumerate(API_RETRY_DELAYS):
        if attempt:
            await asyncio.sleep(delay)
        try:
            async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=30)) as s:
                async with s.get(url) as r:
                    if r.status != 200:
                        continue
                    data = await r.json()
            return data.get("items", [])
        except Exception as exc:
            print(f"[autodl] API read failed (try {attempt + 1}): {exc}")
    return []


async def fetch_latest(sections=None):
    """Every listed episode of the watched sections (donghua/anime),
    fetched IN PARALLEL so the watcher/handlers feel snappier."""
    secs = list(sections or DEFAULT_SECTIONS)
    res = await asyncio.gather(
        *[_fetch_one(f"{API_ROOT}/{s}/videos") for s in secs],
        return_exceptions=True)
    out, seen = [], set()
    for sec, items in zip(secs, res):
        if isinstance(items, BaseException):
            print(f"[autodl] section {sec} fetch failed: {items}")
            items = []
        for item in items:
            if item.get("id") in seen:
                continue
            seen.add(item.get("id"))
            item.setdefault("section", sec)
            out.append(item)
    return out


def parse_api_title(title: str):
    """'Wan Jie Du Zun 3rd Season Eps 218' -> (series, 218)"""
    m = re.search(r"\s*[Ee]ps?\s*(\d{1,4})\s*$", (title or "").strip())
    if m:
        return title[:m.start()].strip(), int(m.group(1))
    return (title or "").strip(), None


def quality_order(q: str) -> int:
    base = (q or "").split("-")[0]
    if base.isdigit():
        base += "p"  # the API also writes "360-eng" for "360p-eng"
    return {"360p": 1, "480p": 2, "720p": 3, "1080p": 4}.get(base, 0)


# ---------------------------------------------------------------------- #
# Settings
# ---------------------------------------------------------------------- #
async def get_settings():
    st = (await store.get_config("autodl")) or {}
    qs = st.get("qualities") or []
    if sorted(qs) == ["1080p", "720p"]:
        # the old built-in default — today "all qualities" is the
        # default, so migrate saved configs once
        qs = []
        st["qualities"] = []
        await save_settings(st)
    secs = st.get("sections") or []
    if sorted(secs) == ["donghua"]:
        # anime is watched now — migrate saved single-section configs
        st["sections"] = list(DEFAULT_SECTIONS)
        await save_settings(st)
    return {
        "enabled": bool(st.get("enabled")),
        "qualities": qs,
        "sections": st.get("sections") or list(DEFAULT_SECTIONS),
        "interval": int(st.get("interval") or 20),
        "last_ids": st.get("last_ids") or [],
    }


async def save_settings(st):
    await store.set_config("autodl", st)


# ---------------------------------------------------------------------- #
# The downloaded registry (MongoDB-backed, memory-cached)
# ---------------------------------------------------------------------- #
_registered: dict[str, float] = {}


def _key(norm_title: str, ep, quality: str) -> str:
    return f"{norm_title}:{ep}:{quality.lower()}"


async def load_registry():
    """Warm the in-memory cache at boot (no network per check)."""
    docs = await store.find("downloaded")
    _registered.update({d["_id"]: d.get("ts", 0) for d in docs})


def already_downloaded(norm_title: str, ep, quality: str) -> bool:
    return _key(norm_title, ep, quality) in _registered


async def mark_downloaded(api_id, norm_title, title, ep, quality,
                          db_info, link, size=0, db_extra=None):
    """Register one archived file — fire-and-forget, ytdown style."""
    key = _key(norm_title, ep, quality)
    _registered[key] = time.time()
    # bot deep link: https://t.me/<bot>?start=<b64 "get-<token>">
    token = secrets.token_hex(4)
    deep = ""
    bu = core_mod.bot_user
    if bu and getattr(bu, "username", None):
        payload = base64.b64encode(("get-" + token).encode()) \
                        .decode().rstrip("=")
        deep = f"https://t.me/{bu.username}?start={payload}"
    doc = {"_id": key, "api_id": int(api_id), "norm_title": norm_title,
           "title": title, "episode": ep, "quality": quality,
           "db": db_info, "db_extra": db_extra or [],
           "file_link": link, "deep_link": deep, "size": size,
           "ts": time.time()}
    if deep:
        try:
            await store.col("deeplinks").update_one(
                {"_id": token},
                {"$set": {"kind": "file", "key": key}}, upsert=True)
        except Exception:
            pass
    try:
        await store.col("downloaded").update_one(
            {"_id": key}, {"$set": doc}, upsert=True)
    except Exception as exc:
        print("[autodl] registry write failed:", exc)


async def registry_get(norm_title, ep, quality):
    return await store.col("downloaded").find_one(
        {"_id": _key(norm_title, ep, quality)})


async def ensure_deep_link(key: str) -> str:
    """Deep link for a registry row — created on the fly for rows
    archived before the deep-link feature existed."""
    rec = await store.col("downloaded").find_one({"_id": key})
    if rec and rec.get("deep_link"):
        return rec["deep_link"]
    bu = core_mod.bot_user
    if not (bu and getattr(bu, "username", None)):
        return ""
    token = secrets.token_hex(4)
    payload = base64.b64encode(("get-" + token).encode()) \
                    .decode().rstrip("=")
    deep = f"https://t.me/{bu.username}?start={payload}"
    try:
        await store.col("deeplinks").update_one(
            {"_id": token},
            {"$set": {"kind": "file", "key": key}}, upsert=True)
        await store.col("downloaded").update_one(
            {"_id": key}, {"$set": {"deep_link": deep}})
    except Exception:
        return ""
    return deep


async def registry_find_api(api_id, quality):
    """Registry row by API episode id + quality label."""
    docs = await store.find("downloaded",
                              {"api_id": int(api_id)})
    for d in docs:
        if (d.get("quality") or "").lower() == quality.lower():
            return d
    return None


async def registry_size() -> int:
    return len(_registered)


async def registry_reset():
    _registered.clear()
    if store.mode == "mongo":
        await store.db["downloaded"].delete_many({})


# ---------------------------------------------------------------------- #
# Helpers
# ---------------------------------------------------------------------- #
class JobCancelled(Exception):
    """Owner tapped /cancel_<id>."""


# The live job queue: one job runs at a time, the rest stay queued.
JOBS = {}          # job id -> job dict
CURRENT = None     # the running job
BATCH = {"n": 0, "total": 0}


def _new_job(series, ep, quality, url):
    jid = secrets.token_hex(4)
    job = {"id": jid, "title": series, "ep": ep, "quality": quality,
           "url": url, "phase": "download", "status": "running",
           "done": 0, "total": 0, "up_done": 0, "up_total": 0,
           "speed": 0.0, "cancel": False, "error": ""}
    JOBS[jid] = job
    return job


def _bar(pct: float, width: int = 12) -> str:
    filled = max(0, min(width, int(round(width * pct / 100))))
    return "█" * filled + "░" * (width - filled)


_CPU_SAMPLE = [0.0, 0]  # [wall ts, cgroup usage_usec]


def container_stats():
    """CPU % of the container quota (cgroup cpu.stat delta) + the
    cgroup's real RAM use vs its limit — not host loadavg/VmRSS."""
    cpus = float(os.cpu_count() or 1)
    try:
        quota = open("/sys/fs/cgroup/cpu.max",
                     encoding="utf-8").read().split()
        if quota[0] != "max":
            cpus = int(quota[0]) / int(quota[1])
    except Exception:
        pass
    cpu = 0.0
    try:
        use = 0
        for line in open("/sys/fs/cgroup/cpu.stat", encoding="utf-8"):
            if line.startswith("usage_usec"):
                use = int(line.split()[1])
                break
        now = time.time()
        pts, pus = _CPU_SAMPLE
        if pts and use >= pus and now > pts:
            cpu = min(999.0, (use - pus) / 1e6
                      / (now - pts) / cpus * 100)
        _CPU_SAMPLE[0], _CPU_SAMPLE[1] = now, use
    except Exception:
        try:
            cpu = min(999.0, os.getloadavg()[0] / cpus * 100)
        except Exception:
            pass
    ram = 0.0
    try:
        ram = int(open("/sys/fs/cgroup/memory.current",
                       encoding="utf-8").read().strip()) / 1048576.0
    except Exception:
        try:
            for line in open("/proc/self/status", encoding="utf-8"):
                if line.startswith("VmRSS:"):
                    ram = int(line.split()[1]) / 1024.0
                    break
        except Exception:
            ram = (resource.getrusage(
                resource.RUSAGE_SELF).ru_maxrss / 1024.0)
    total = 0.0
    try:
        val = open("/sys/fs/cgroup/memory.max",
                   encoding="utf-8").read().strip()
        if val != "max":
            total = int(val) / 1024 / 1024
    except Exception:
        pass
    if not total:
        try:
            for line in open("/proc/meminfo", encoding="utf-8"):
                if line.startswith("MemTotal:"):
                    total = int(line.split()[1]) / 1024.0
                    break
        except Exception:
            pass
    return cpu, ram, total


def _hs(num) -> str:
    return human_size(int(num)) or "0 B"


def job_panel(job=None) -> str:
    """The owner-DM progress card (ytdown style)."""
    cpu, ram, total = container_stats()
    lines = ["📋 <b>Your downloads</b>", ""]
    if job is None:
        lines.append(f"⏳ {BATCH['total']} file(s) queued — "
                     f"starting…")
    else:
        state = {"running": ("Running" if job["phase"] == "download"
                             else "Running"),
                 "done": "Done", "failed": "Failed",
                 "cancelled": "Cancelled"}[job["status"]]
        lines.append(f"🎬 {escape_html(job['title'])} "
                     f"Eps {job['ep']} [{job['quality']}]")
        lines.append(f"📋 Video {BATCH['n']}/{BATCH['total']} "
                     f"· {state}")
        lines.append("")
        if job["phase"] == "upload":
            pct = (job["up_done"] / job["up_total"] * 100
                   if job["up_total"] else 0.0)
            lines.append(f"{_bar(pct)}  {pct:.1f}%")
            lines.append(f"⬆ Telegram: {_hs(job['up_done'])}"
                         f" / {_hs(job['up_total'])}")
            lines.append(f"⬇ Source: {_hs(job['done'])}")
            lines.append("📌 Uploading to Telegram…")
        else:
            pct = (job["done"] / job["total"] * 100
                   if job["total"] else 0.0)
            lines.append(f"{_bar(pct)}  {pct:.1f}%")
            lines.append(f"⬆ Telegram: {_hs(job['up_done'])}"
                         f" / {_hs(job['up_total'])}")
            lines.append(f"⬇ Source: {_hs(job['done'])}"
                         + (f" / {_hs(job['total'])}"
                            if job["total"] else ""))
            eta = (int((job["total"] - job["done"]) / job["speed"])
                   if job["speed"] > 0 and job["total"] else 0)
            lines.append(f"🚀 {human_size(int(job['speed']))}/s "
                         f"· ⏳ ETA {eta}s")
        lines.append("")
        lines.append(f"🆔 {job['id']} · /cancel_{job['id']}")
    lines.append("")
    lines.append(f"📊 Container CPU: {cpu:.0f}% of quota · "
                 f"RAM: {ram:.1f} MiB / {total:.1f} MiB")
    queued = max(0, BATCH["total"] - BATCH["n"])
    active = 1 if job and job["status"] == "running" else 0
    lines.append(f"Active/queued: {active} / {queued} · "
                 f"send /cancel_&lt;id&gt; to stop one")
    return "\n".join(lines)


OWNER_ENT = None  # cached InputUser of the owner (fresh Koyeb sessions
# have an empty entity cache, so a bare owner id can't be messaged
# until we resolve it once via the dialog list)


async def _owner_id():
    ids = CONFIG.get("owner_ids") or []
    if ids:
        return int(ids[0])
    try:  # no owner configured yet: first added admin gets the panels
        doc = await store.col("admins").find_one()
        if doc:
            return int(doc["_id"])
    except Exception:
        pass
    return None


async def _resolve_owner():
    global OWNER_ENT
    owner = await _owner_id()
    if not owner:
        return None
    if OWNER_ENT is not None:
        return OWNER_ENT
    oid = int(owner)
    try:  # cheap path: session cache already knows them
        OWNER_ENT = await app.get_input_entity(oid)
        return OWNER_ENT
    except Exception:
        pass
    try:  # walk the dialog list — DMs/chats the bot is in server-side
        async for d in app.iter_dialogs():
            if d.id == oid:
                OWNER_ENT = d.entity
                return OWNER_ENT
    except Exception as exc:
        print("[autodl] owner resolve failed:", exc)
    return None


async def _owner_dm(text: str):
    ent = await _resolve_owner()
    if ent is None:
        print("[autodl] owner DM skipped — no owner/admin known "
                  "(send /setowner once)" )
        return None
    oid = getattr(ent, "id", 0)
    if not oid:  # input entities carry user_id/channel_id, not id
        try:
            from telethon import utils as _tu
            oid = _tu.get_peer_id(ent)
        except Exception:
            oid = 0
    left = core_mod.flood_left(oid)
    if left:  # mid-flood: deliver right after the window instead
        asyncio.ensure_future(_delayed_owner_dm(oid, text, left + 5))
        return None
    try:
        return await app.send_message(ent, text, parse_mode="html")
    except FloodWaitError as fw:
        core_mod.flood_mark(oid, fw.seconds)
        asyncio.ensure_future(_delayed_owner_dm(oid, text,
                                                fw.seconds + 5))
        return None
    except Exception as exc:
        print("[autodl] owner DM failed:", exc)
        return None


async def _delayed_owner_dm(oid, text, delay):
    await asyncio.sleep(delay)
    try:
        await app.send_message(oid, text, parse_mode="html")
    except Exception as exc:
        print("[autodl] delayed owner DM failed:", repr(exc)[:160])


async def _reporter(msg):
    """Legacy no-op keeper — the live panel is driven by panel_loop()
    against PANEL so /status and the batch panel are ONE message."""
    return


# The ONE live progress panel: batch start and /status both point it
# at their message; the old one is deleted so only the newest panel
# ever keeps updating.
PANEL = {"chat": None, "id": None}
SYNC_REQ = {"on": False}  # /syncbutton: run full sync at batch end


async def set_live_panel(chat_id, msg_id) -> None:
    old = (PANEL["chat"], PANEL["id"])
    PANEL["chat"], PANEL["id"] = chat_id, msg_id
    if old[0] and old != (chat_id, msg_id):
        try:
            await app.delete_messages(old[0], [old[1]])
            print(f"[autodl] old live panel {old} deleted")
        except Exception:
            pass


async def panel_loop():
    """Keeps whichever message is the live panel updated (batch or
    manual /status), paced and flood-aware."""
    last = None
    while True:
        try:
            chat, mid = PANEL["chat"], PANEL["id"]
            if chat:
                text = job_panel(CURRENT)
                if text != last:
                    try:
                        await app.edit_message(chat, mid, text,
                                               parse_mode="html")
                        last = text
                    except FloodWaitError as fw:
                        core_mod.flood_mark(chat, fw.seconds)
                    except Exception:
                        pass
            else:
                last = None
        except Exception as exc:
            print("[autodl] panel loop error:", repr(exc)[:160])
        await asyncio.sleep(6)


_HTTP = None


def http_session(total=900):
    """One shared aiohttp session for the whole process — reusing
    connections keeps Koyeb downloads from paying a fresh TCP+TLS
    handshake per file."""
    global _HTTP
    if _HTTP is None or _HTTP.closed:
        _HTTP = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=8, ttl_dns_cache=600))
    return _HTTP, aiohttp.ClientTimeout(total=total)


async def download_stream(url: str, dest: str, job=None) -> str:
    """Download with HTTP-range RESUME: a restart mid-file picks up
    from the bytes already on disk instead of starting over."""
    have = os.path.getsize(dest) if os.path.exists(dest) else 0
    headers = {"Range": f"bytes={have}-"} if have else {}
    s, timeout = http_session(900)
    async with s.get(url, headers=headers, timeout=timeout) as r:
            if r.status == 206 and have:  # partial content: append
                mode = "ab"
                rng = r.headers.get("Content-Range", "")
                total = int(rng.split("/")[-1]) if "/" in rng else 0
                done = have
            elif r.status == 200:
                mode = "wb"
                total = int(r.headers.get("Content-Length") or 0)
                done = 0
            else:
                raise RuntimeError(f"HTTP {r.status}")
            if job is not None:
                job["total"] = total
            last = time.time()
            last_done = done
            loop = asyncio.get_running_loop()
            with open(dest, mode) as f:
                async for chunk in r.content.iter_chunked(1 << 20):
                    if job is not None and job["cancel"]:
                        raise JobCancelled()
                    # disk write off the event loop: the bot keeps
                    # answering users while multi-GB files land
                    await loop.run_in_executor(None, f.write, chunk)
                    done += len(chunk)
                    if job is not None:
                        job["done"] = done
                        now = time.time()
                        if now - last >= 1.0:
                            job["speed"] = ((done - last_done)
                                            / (now - last))
                            last, last_done = now, done
    return dest


async def resolve_poster(item, series: str):
    """Poster for the post: API thumbnail first, then the
    multi-source engine (AniList/AnimXin/…) by series name."""
    thumb = item.get("thumbnail")
    if thumb:
        try:
            return await url_bytes(thumb), "API"
        except Exception:
            pass
    meta = await build_metadata(series)
    if meta.get("image"):
        try:
            return await url_bytes(meta["image"]), "auto"
        except Exception:
            pass
    return None, "none"


async def file_link(chat_id: int, msg_id: int) -> str:
    try:
        ent = await app.get_entity(chat_id)
        if getattr(ent, "username", None):
            return f"https://t.me/{ent.username}/{msg_id}"
    except Exception:
        pass
    return f"https://t.me/c/{str(chat_id).replace('-100', '')}/{msg_id}"


def _doc_ref(doc):
    if doc is None:
        return None
    return {
        "id": getattr(doc, "id", None),
        "access_hash": getattr(doc, "access_hash", None),
        "file_reference": getattr(doc, "file_reference", b""),
        "mime_type": getattr(doc, "mime_type", None),
        "size": getattr(doc, "size", None),
        "name": getattr(doc, "name", None),
    }


def _disk_ok(need_bytes: int) -> bool:
    try:
        free = shutil.disk_usage(DOWNLOAD_DIR).free
        return free > max(need_bytes * 1.2, MIN_FREE_DISK)
    except Exception:
        return True


def _clean_synopsis(desc: str) -> str:
    """The prose synopsis out of the API's description blob."""
    text = (desc or "").strip()
    for marker in ("Sinopsis ", "Synopsis ", "Sinopsis: ", "Synopsis: "):
        idx = text.find(marker)
        if idx != -1:
            return text[idx + len(marker):].strip()
    return text


def pick_eng_synopsis(meta, indo_syn: str) -> str:
    """English synopsis STRICTLY from the EN sources (AniList/Jikan).
    The Indonesian mirror scrapes (animxin/lucifer/anilive) must never
    leak into eng cards — that was the indo/eng synopsis crossover."""
    import re as _re
    bs = (meta or {}).get("by_source") or {}
    raw = ""
    for src in ("anilist", "jikan"):
        r = bs.get(src)
        desc = getattr(r, "description", None) if r else None
        if desc:
            raw = desc
            break
    raw = _re.sub(r"<[^>]+>", " ", raw)
    raw = _re.sub(r"\s+", " ", raw).strip()
    syn = _clean_synopsis(raw)
    if syn and indo_syn and syn.strip() == indo_syn.strip():
        return ""  # an indo blob mislabelled as english
    return syn


def _eng_title(meta) -> str:
    bs = (meta or {}).get("by_source") or {}
    for src in ("anilist", "jikan"):
        r = bs.get(src)
        t = (getattr(r, "title", "") or "").strip()
        if t:
            return t
    return ""


def dual_title(series: str, meta=None) -> str:
    """Poster title in the user's format: the actual name first,
    then the english version divided by | — e.g.
    'Shixiong A Shixiong: Zuizhong Ji | Big Brother'."""
    eng = _eng_title(meta)
    if eng and eng.lower() != (series or "").strip().lower():
        return f"{series} | {eng}"
    return series


def _audio_lang(item) -> str:
    kind = str(item.get("type") or item.get("section") or "").lower()
    return "Chinese" if kind == "donghua" else "Japanese"


def _has_eng(quality: str) -> bool:
    return "eng" in (quality or "").lower()


def _episode_label(eps) -> str:
    if not eps:
        return ""
    lo, hi = min(eps), max(eps)
    return f"{lo:02d}" if lo == hi else f"{lo:02d} To {hi:02d}"


_NET_CACHE = {}  # chat_id -> (ts, line): rarely changes


async def _network_line(chat_id) -> str:
    hit = _NET_CACHE.get(chat_id)
    if hit and time.time() - hit[0] < 3600:
        return hit[1]
    line = ""
    try:
        ent = await app.get_entity(chat_id)
        if getattr(ent, "username", None):
            line = f"🔗 𝗢𝘂𝗿 𝗡𝗲𝘁𝘄𝗼𝗿𝗸 @{ent.username}"
        else:
            link = await invite_link(chat_id)
            if link:
                line = f"🔗 𝗢𝘂𝗿 𝗡𝗲𝘁𝘄𝗼𝗿𝗸 {link}"
    except Exception:
        pass
    _NET_CACHE[chat_id] = (time.time(), line)
    return line


def _streams_for(item, qualities):
    """The item's streams whose base quality was asked for."""
    want = {q.lower() for q in qualities}
    out = []
    for s in item.get("streams", []):
        label = (s.get("quality") or "").lower()
        if label.split("-")[0] in want:
            out.append(s)
    return out


async def posting_channels():
    """Posting channels with their tag flags: a tag containing
    'eng' means that channel only gets the API's eng streams;
    every other tag (indo / none / quality tags) gets the base
    (untagged 360p…1080p) streams."""
    out = []
    for c in await store.list_channels():
        tag = (c.get("tag") or "").lower()
        out.append({"id": c["_id"], "tag": tag,
                    "eng": "eng" in tag})
    return out


def wanted_streams(streams, qualities, want_base: bool, want_eng: bool):
    """Streams matching the configured qualities AND the tags of
    the channels we post to (indo -> base, eng -> '-eng' only)."""
    want = {q.lower() for q in qualities}
    all_q = not want or "all" in want
    out = []
    for s in streams or []:
        label = (s.get("quality") or "").lower()
        base = label.split("-")[0]
        if base.isdigit():
            base += "p"  # "360-eng" means "360p-eng"
        if not all_q and base not in want:
            continue
        eng = _has_eng(label)
        if (eng and want_eng) or (not eng and want_base):
            out.append(s)
    return out


# ---------------------------------------------------------------------- #
# What is new?
# ---------------------------------------------------------------------- #
async def check_new():
    """(item, streams_to_download, all_streams) for every episode
    that still has a quality missing from the registry."""
    st = await get_settings()
    items = await fetch_latest(st["sections"])
    fresh = []
    for item in items:
        series, ep = parse_api_title(item.get("title", ""))
        norm = normalize_key(series)
        # always archive every flavour the API offers; channel tags
        # only decide WHERE each flavour gets posted
        streams = wanted_streams(item.get("streams", []),
                                 st["qualities"], True, True)
        if not streams:
            continue
        todo = [s for s in streams
                if not already_downloaded(
                    norm, ep, s.get("quality", ""))]
        if todo:
            fresh.append((item, todo, streams))
    return fresh


# ---------------------------------------------------------------------- #
# Core processing
# ---------------------------------------------------------------------- #
def group_fresh(fresh):
    """Group the fresh (item, todo, all_streams) tuples by series so
    one run produces ONE post per series ('Episode ⌯ 05 To 06')."""
    groups = {}
    for item, todo, all_streams in fresh:
        series, _ = parse_api_title(item.get("title", ""))
        norm = normalize_key(series)
        g = groups.setdefault(norm, {"norm": norm, "series": series,
                                     "entries": []})
        g["entries"].append((item, todo, all_streams))
    for g in groups.values():
        g["entries"].sort(key=lambda t: parse_api_title(
            t[0].get("title", ""))[1] or 0)
    out = list(groups.values())
    for g in out:
        g["section"] = g["entries"][0][0].get("section") or ""
    # donghua jumps the queue ahead of anime (eng channel priority)
    out.sort(key=lambda g: 0 if g.get("section") == "donghua" else 1)
    return out


def _free_up(need_mb=900):
    """Make room on the (tiny) Koyeb disk: drop oldest downloads
    until `need_mb` are free, so 700 MB+ uploads never die mid-way
    on ENOSPC."""
    try:
        st = os.statvfs(DOWNLOAD_DIR)
        free = st.f_bavail * st.f_frsize // 1048576
        if free >= need_mb:
            return free
        files = sorted(
            (os.path.join(DOWNLOAD_DIR, f)
             for f in os.listdir(DOWNLOAD_DIR)
             if f.endswith((".mp4", ".mkv", ".part"))),
            key=os.path.getmtime)
        for path in files:
            if free >= need_mb:
                break
            try:
                free += os.path.getsize(path) // 1048576
                os.remove(path)
                print("[autodl] disk guard: removed", path)
            except Exception:
                continue
        return free
    except Exception:
        return 0


async def run_job(group, item, ep, stream, db_ids, poster):
    """One queued file: download (with live progress + cancel),
    archive in every DB channel (with upload progress), register."""
    global CURRENT
    series = group["series"]
    norm = group["norm"]
    q = stream["quality"]
    from telethon.errors import FloodWaitError
    job = _new_job(series, ep, q, stream["url"])
    CURRENT = job
    BATCH["n"] += 1
    print(f"[autodl] job {BATCH['n']}/{BATCH['total']} start: "
          f"{series} Ep {ep} [{q}]")
    fname = f"{norm}_{ep}_{q}.mp4".replace(" ", "_")
    path = os.path.join(DOWNLOAD_DIR, fname)
    try:
        if not _disk_ok(0):
            _free_up(900)
        if not _disk_ok(0):
            job["status"] = "failed"
            job["error"] = "disk low"
            print("[autodl] disk low — skipping", fname)
            return job
        await download_stream(stream["url"], path, job)
        size = os.path.getsize(path)
        job["phase"] = "upload"

        async def up_cb(sent_bytes, total_bytes):
            job["up_done"] = sent_bytes
            job["up_total"] = total_bytes

        refs = []
        cap = (f"{series} — Ep {ep} [{q}] "
               f"{human_size(size)}")
        # hachoir reads REAL duration/resolution from the file on
        # disk (an InputFile object would yield the 0:00 1x1 dummy)
        _va, _mime = tl_utils.get_attributes(path,
                                             supports_streaming=True)
        attrs = core_mod.fname_attrs(f"{series} Eps {ep:02d} [{q}]",
                                     path) + [
            x for x in _va if isinstance(x, DocumentAttributeVideo)]
        thumb16 = core_mod.thumb_16x9(poster)
        primary = None  # first DB channel gets the real upload
        for dbc in db_ids:  # archive in every DB channel
            sent = None
            for attempt in (1, 2, 3):  # transient upload errors retry
                try:
                    if primary is None:
                        # explicit 512 KB parts = fewest round-trips
                        # = fastest upload Telegram allows
                        up = await app.upload_file(
                            path, part_size_kb=512, file_size=size,
                            progress_callback=up_cb)
                        sent = await app.send_file(
                            dbc, file=up, caption=cap,
                            thumb=thumb16, supports_streaming=True,
                            attributes=attrs)
                    else:
                        # server-side copy: reuse the media Telegram
                        # already stores — zero re-upload, instant
                        sent = await app.send_file(
                            dbc, file=primary.media, caption=cap)
                    break
                except FloodWaitError as fw:
                    core_mod.flood_mark(int(dbc), fw.seconds)
                    print(f"[autodl] archive floodwait {fw.seconds}s "
                          f"to {dbc} — sleeping it out")
                    await asyncio.sleep(min(fw.seconds, 900) + 5)
                except Exception as exc:
                    print(f"[autodl] archive try {attempt} failed to "
                          f"{dbc}: {exc}")
                    await asyncio.sleep(8)
            if sent is None:
                print(f"[autodl] archive FAILED to {dbc} after 3 tries")
                continue
            if primary is None:
                primary = sent
            refs.append({"channel": int(dbc),
                         "msg_id": sent.id,
                         "link": await file_link(dbc, sent.id),
                         "doc": _doc_ref(sent.document
                                         or sent.video)})
        if refs and len(refs) < len(db_ids):
            asyncio.ensure_future(_owner_dm(
                f"⚠️ <b>Archive incomplete</b> — {series} Ep {ep} "
                f"[{q}]: reached {len(refs)}/{len(db_ids)} DB "
                f"channel(s)"))
        if not refs:
            job["status"] = "failed"
            job["error"] = (f"archive failed to all {len(db_ids)} "
                            f"DB channel(s)")
            return job
        link = refs[0]["link"]
        primary, extra = refs[0], refs[1:]
        await mark_downloaded(item["id"], norm, series, ep, q,
                              {k: primary[k]
                               for k in ("channel", "msg_id", "doc")},
                              link, size=size, db_extra=extra)
        job["status"] = "done"
        print(f"[autodl] archived {series} ep{ep} {q} "
              f"in {len(refs)} DB channel(s)")
    except JobCancelled:
        job["status"] = "cancelled"
        print(f"[autodl] job {job['id']} cancelled by owner")
    except Exception as exc:
        job["status"] = "failed"
        job["error"] = str(exc)
        print(f"[autodl] download failed {series} ep{ep} {q}: {exc}")
    finally:
        try:
            os.remove(path)
        except Exception:
            pass
        CURRENT = None
    return job


async def serve_deeplink(e, payload: str) -> bool:
    """A t.me/<bot>?start=<b64> tap: serve that stored file (with
    the force-sub gate), like the reference bot's file links."""
    try:
        pad = payload + "=" * (-len(payload) % 4)
        text = base64.b64decode(pad.encode()).decode()
    except Exception:
        return False
    if not text.startswith("get-"):
        return False
    dl = await store.col("deeplinks").find_one({"_id": text[4:]})
    if not dl or dl.get("kind") != "file":
        return False
    rec = await store.col("downloaded").find_one({"_id": dl["key"]})
    if not rec:
        await e.respond("❌ That file is no longer stored — "
                        "ask an admin.", parse_mode="html")
        return True
    ok, fs = await forcesub_check(e.sender_id)
    if not ok:
        await forcesub_gate(e, fs)
        return True
    from handlers_files import serve_file
    got = await serve_file(e.sender_id, rec)
    await e.respond("📥 <b>File sent!</b> Check your downloads 👇"
                    if got else
                    "❌ File unavailable right now — ask an admin.",
                    parse_mode="html")
    return True


async def start_batch(fresh, force=False):
    """Run one batch under the global lock (watcher, /checknew,
    /searchad). force=True re-downloads even archived qualities."""
    async with LOAD:
        return await run_batch(fresh, force=force)


async def run_batch(fresh, force=False):
    """Process every fresh episode through the job queue, with a
    live progress panel DM'd to the owner."""
    groups = group_fresh(fresh)
    if not groups:
        return 0
    if not await store.ensure_mongo(45):
        print("[autodl] WARNING: MongoDB unreachable — archives will "
              "be skipped until it is back")
    BATCH["n"] = 0
    BATCH["skip"] = 0
    await store.set_config("pending_batch",
                           {"ts": time.time(), "groups": len(groups)})
    BATCH["total"] = sum(len(todo)
                         for g in groups
                         for _, todo, _ in g["entries"])
    msg = await _owner_dm(job_panel(None))
    if msg is not None:
        await set_live_panel(getattr(msg, "chat_id", None),
                             getattr(msg, "id", None))
    done = 0
    try:
        for g in groups:
            if await process_group(g, force=force):
                done += 1
    finally:
        ok = sum(1 for j in JOBS.values() if j["status"] == "done")
        bad = sum(1 for j in JOBS.values() if j["status"] == "failed")
        can = sum(1 for j in JOBS.values()
                  if j["status"] == "cancelled")
        if PANEL["chat"]:
            cpu, ram, total = container_stats()
            try:
                await app.edit_message(
                    PANEL["chat"], PANEL["id"],
                    f"✅ <b>Batch finished</b>\n\n"
                    f"⬇️ archived: <b>{ok}</b> · ❌ failed: <b>{bad}</b>"
                    f" · 🛑 cancelled: <b>{can}</b>\n"
                    f"📺 posted: <b>{done}</b> episode card(s)\n"
                    + (f"⚠️ skipped: <b>{BATCH.get('skip', 0)}</b> "
                       f"group(s) — no DB channel / storage offline\n"
                       if BATCH.get("skip") else "")
                    + "\n"
                    f"📊 Container CPU: {cpu:.0f}% of quota · "
                    f"RAM: {ram:.1f} MiB / {total:.1f} MiB",
                    parse_mode="html")
            except Exception:
                pass
            PANEL["chat"] = PANEL["id"] = None  # freeze the summary
        try:
            await repair_posts()
        except Exception as exc:
            print("[autodl] post-repair pass failed:", repr(exc)[:200])
        try:
            n = await refresh_all_cards()
            if n:
                print(f"[autodl] cards topped up with new "
                      f"qualities: {n}")
        except Exception as exc:
            print("[autodl] card refresh pass failed:",
                  repr(exc)[:200])
        if SYNC_REQ["on"]:  # /syncbutton: full sweep after the batch
            SYNC_REQ["on"] = False
            try:
                await sync_buttons_pass()
            except Exception as exc:
                print("[autodl] syncbutton pass failed:",
                      repr(exc)[:200])
        await store.set_config("pending_batch", None)
        JOBS.clear()
        BATCH["n"] = BATCH["total"] = 0
    return done


async def repair_posts():
    """After every batch: any touched episode whose card is missing
    from the posting channel(s) gets created & uploaded now."""
    global TOUCHED
    touched, TOUCHED = TOUCHED, set()
    if not touched:
        return
    items = {}
    try:
        for it in await fetch_latest(None):
            series, ep = parse_api_title(it.get("title", ""))
            items[(normalize_key(series), ep)] = it
    except Exception:
        pass
    for norm, ep in sorted(touched, key=str):
        recs = [r for r in await store.find(
            "downloaded", {"norm_title": norm, "episode": ep})
            if r.get("file_link")]
        if not recs:
            continue
        links = {r["quality"]: r["file_link"] for r in recs}
        deeps = {r["quality"]: (r.get("deep_link")
                                or await ensure_deep_link(r["_id"])
                                or r["file_link"]) for r in recs}
        item = items.get((norm, ep)) or {
            "title": recs[0].get("title") or norm, "genres": [],
            "rating": None, "description": ""}
        series = parse_api_title(item.get("title", ""))[0] or norm
        poster = None
        try:
            poster, _src = await resolve_poster(item, series)
        except Exception:
            pass
        print(f"[autodl] repair: creating missing card for "
              f"{series} Ep {ep}")
        await post_episode({"series": series, "norm": norm,
                            "entries": [(item, [], [])]},
                           ep, links, deeps, poster)


TOUCHED = set()  # (norm, ep) pairs processed by the running batch


async def find_post(norm, ep, channel=None, eng=None):
    """Existing card post for this episode — for one channel when
    given, otherwise for any channel. Docs created before
    per-channel tracking (no channel field) are adopted by flavour
    so they are never posted twice."""
    for d in await store.find("posts", {"norm": norm}):
        if ep not in (d.get("eps") or []):
            continue
        if channel is None or d.get("channel") == channel:
            return d
        if d.get("channel") is None and eng is not None:
            doc_eng = any(_has_eng(q) for q in (d.get("quals") or []))
            if doc_eng == eng:
                try:
                    await store.col("posts").update_one(
                        {"_id": d["_id"]},
                        {"$set": {"channel": channel}})
                except Exception:
                    pass
                return d
    return None


async def post_episode(group, ep, links, deeps, poster,
                   only=None) -> int:
    """One designed card post per finished episode, per posting
    channel filtered by its tag (indo / eng)."""
    import secrets
    series = group["series"]
    norm = group["norm"]
    first_item = group["entries"][0][0]
    # synopsis in the reader's language: API text (indo) for indo
    # channels, English (AniList/Jikan) for eng-tagged ones
    indo_syn = _clean_synopsis(first_item.get("description") or "")
    eng_syn = ""
    meta = None
    try:
        meta = await build_metadata(series)
        eng_syn = pick_eng_synopsis(meta, indo_syn)
    except Exception:
        eng_syn = ""
    base_card = dict(
        title=dual_title(series, meta),
        audio=_audio_lang(first_item),
        rating=(f"{first_item.get('rating')} / 10"
                if first_item.get("rating") else ""),
        genres=" ".join("#" + g.replace(" ", "")
                        for g in (first_item.get("genres") or [])[:5]),
    )
    quals_all = sorted(links, key=quality_order, reverse=True)
    for q in links:  # clean DM copies: file name only, no info
        try:
            await store.col("downloaded").update_one(
                {"_id": _key(norm, ep, q)},
                {"$set": {"clean_caption":
                          f"{series} Episode {ep} [{q}]"}})
        except Exception:
            pass

    posted = 0
    for ch in (only if only is not None else await posting_channels()):
        kept = {q: l for q, l in links.items()
                if _has_eng(q) == ch["eng"]}
        if not kept:
            continue
        quals = sorted(kept, key=quality_order, reverse=True)
        cap_text, syn_off, syn_len = post_caption(
            episode_label=_episode_label([ep]),
            sub="Eng Sub" if ch["eng"] else "Indo-Sub",
            qualities=" | ".join(quals),
            network=await _network_line(ch["id"]),
            store_links="",
            synopsis=eng_syn if ch["eng"] else indo_syn,
            **base_card)
        ents = _card_entities(cap_text, syn_off, syn_len)
        doc = await find_post(norm, ep, ch["id"], ch["eng"])
        if doc:
            # card already there: top up NEW quality buttons (e.g.
            # the batch restarted mid-episode and the rest of the
            # qualities landed later — the card must grow with them)
            if set(quals) - set(doc.get("quals") or []):
                await _apply_card_edit(
                    doc, ch["id"], cap_text,
                    _card_buttons(quals, deeps, doc["_id"]),
                    ents, quals)
            continue
        pid = secrets.token_hex(4)
        await store.col("posts").update_one(
            {"_id": pid},
            {"$set": {"_id": pid, "norm": norm, "title": series,
                      "eps": [ep], "quals": quals,
                      "channel": ch["id"], "eng": ch["eng"],
                      "section": first_item.get("section") or ""}},
            upsert=True)
        # links live in the quality BUTTONS only — the poster stays
        # clean; tapping a button DMs the file + its shareable link
        buttons = _card_buttons(quals, deeps, pid)
        sentm = await _send_card(ch["id"], poster, cap_text, buttons,
                                 f"{norm}_Ep{ep}_poster.jpg",
                                 entities=ents)
        if sentm:
            posted += 1
            try:
                await store.col("posts").update_one(
                    {"_id": pid},
                    {"$set": {"msg_id": getattr(sentm, "id", None)}})
            except Exception:
                pass
            print(f"[autodl] posted {series} Ep {ep} "
                  f"[{ch['tag'] or 'indo'}] to {ch['id']}")
            await asyncio.sleep(4)  # pace channel posts vs flood
        else:
            asyncio.ensure_future(_owner_dm(
                f"❌ <b>Card post failed</b> — {series} Ep {ep} to "
                f"<code>{ch['id']}</code> — the repair pass will "
                f"retry it"))
    return posted


def _card_buttons(quals, deeps, pid):
    """URL buttons: the shareable deep link is EMBEDDED in each
    quality button (long-press = copy/open, tap = bot DM)."""
    btn_row = [Button.url(f"🎬 {q}", deeps[q]) if deeps.get(q)
               else Button.inline(f"🎬 {q}",
                                  f"qdl2:{pid}:{q}".encode())
               for q in quals]
    return [btn_row[i:i + 3] for i in range(0, len(btn_row), 3)]


async def _channel_card_map(channel, cache, limit=600):
    """(title, episode-label) -> message id for every photo
    card in a channel. Bots may NOT use messages.Search
    (BotMethodInvalidError), so this is a plain history scan
    (GetHistory IS allowed for bots), cached per pass."""
    if channel in cache:
        return cache[channel]
    mp = {}
    try:
        async for m in app.iter_messages(channel, limit=limit):
            cap = m.message or ""
            if not m.photo or not cap:
                continue
            first = cap.split("\n", 1)[0].strip()
            first = first.split(" | ", 1)[0].strip()
            mm = re.search("‣ Episode ⌯ (.+)", cap)
            if mm:
                mp[(first, mm.group(1).strip())] = m.id
    except Exception as exc:
        print("[autodl] channel scan failed:", repr(exc)[:160])
    cache[channel] = mp
    return mp


async def find_card_message(channel, series, ep, cache=None):
    """Locate a legacy card (posts doc without msg_id)."""
    mp = await _channel_card_map(channel,
                                 cache if cache is not None
                                 else {})
    return mp.get((series, _episode_label([ep])))


async def _apply_card_edit(doc, channel, cap_text, buttons, ents,
                           quals, cache=None) -> bool:
    """Rewrite an existing card's caption+buttons in place."""
    msg_id = doc.get("msg_id")
    if not msg_id:
        msg_id = await find_card_message(channel,
                                         doc.get("title") or "",
                                         (doc.get("eps") or [None])[0],
                                         cache=cache)
        if not msg_id:
            print("[autodl] card refresh: message not found in "
                  f"{channel}")
            return False
    try:
        await app.edit_message(channel, msg_id, text=cap_text,
                               buttons=buttons,
                               formatting_entities=ents)
    except Exception as exc:
        print(f"[autodl] card refresh edit failed in {channel}: "
              f"{repr(exc)[:160]}")
        return False
    await store.col("posts").update_one(
        {"_id": doc["_id"]},
        {"$set": {"quals": quals, "msg_id": msg_id}})
    print(f"[autodl] card refreshed in {channel}: "
          f"{doc.get('title')} eps {doc.get('eps')} -> {quals}")
    return True


async def refresh_all_cards(gap=4) -> int:
    """Retro pass: every card whose episode later gained more
    archived qualities gets its buttons/caption topped up — covers
    restarts mid-batch and cards posted before this feature."""
    docs = await store.find("posts", {})
    chans = {c["id"]: c for c in await posting_channels()}
    items = {}
    scan_cache = {}  # one history scan per channel per pass
    changed = 0
    for doc in docs:
        try:
            norm = doc.get("norm")
            title = doc.get("title") or ""
            ch = chans.get(doc.get("channel"))
            eng = doc.get("eng")
            if eng is None:
                eng = bool(ch and ch["eng"])
            for ep in doc.get("eps") or []:
                recs = [r for r in await store.find(
                    "downloaded",
                    {"norm_title": norm, "episode": ep})
                    if r.get("file_link")]
                kept = {r["quality"]: r for r in recs
                        if _has_eng(r["quality"]) == eng}
                quals = sorted(kept, key=quality_order, reverse=True)
                if not quals or not (set(quals)
                                     - set(doc.get("quals") or [])):
                    continue
                ikey = (doc.get("section") or "", title, ep)
                if ikey not in items:
                    item = None
                    for sec in ((doc.get("section") or "donghua"),
                                "donghua", "anime"):
                        item = await _search_episode(sec, title, ep)
                        if item:
                            break
                    items[ikey] = item or {
                        "title": f"{title} Eps {ep}",
                        "type": doc.get("section") or "donghua",
                        "genres": [], "rating": None,
                        "description": ""}
                item = items[ikey]
                indo_syn = _clean_synopsis(item.get("description")
                                           or "")
                eng_syn = ""
                meta = None
                try:
                    meta = await build_metadata(title)
                    if eng:
                        eng_syn = pick_eng_synopsis(meta, indo_syn)
                except Exception:
                    eng_syn = ""
                deeps = {q: (r.get("deep_link")
                             or await ensure_deep_link(
                                 _key(norm, ep, q))
                             or r["file_link"])
                         for q, r in kept.items()}
                base_card = dict(
                    title=dual_title(title, meta),
                    audio=_audio_lang(item),
                    rating=(f"{item.get('rating')} / 10"
                            if item.get("rating") else ""),
                    genres=" ".join("#" + g.replace(" ", "")
                                    for g in (item.get("genres")
                                              or [])[:5]),
                )
                cap_text, syn_off, syn_len = post_caption(
                    episode_label=_episode_label([ep]),
                    sub="Eng Sub" if eng else "Indo-Sub",
                    qualities=" | ".join(quals),
                    network=await _network_line(doc["channel"]),
                    store_links="",
                    synopsis=eng_syn if eng else indo_syn,
                    **base_card)
                if await _apply_card_edit(
                        doc, doc["channel"], cap_text,
                        _card_buttons(quals, deeps, doc["_id"]),
                        _card_entities(cap_text, syn_off, syn_len),
                        quals, cache=scan_cache):
                    changed += 1
                    await asyncio.sleep(gap)
        except Exception as exc:
            print("[autodl] refresh card failed:", repr(exc)[:160])
    return changed


def _card_entities(text: str, syn_off: int, syn_len: int):
    """Whole-card bold + collapsed (expandable) synopsis quote —
    built by hand so no parser/layer can ever drop them. The bold
    never WRAPS the quote (three spans: before / inside / after)
    because clients mis-render a blockquote nested inside bold —
    that is what kept the quote arriving expanded."""
    from telethon.tl.types import (MessageEntityBold,
                                   MessageEntityBlockquote)
    total = len(text.encode("utf-16-le")) // 2
    if syn_len <= 0:
        return [MessageEntityBold(offset=0, length=total)]
    ents = []
    if syn_off > 0:
        ents.append(MessageEntityBold(offset=0, length=syn_off))
    ents.append(MessageEntityBold(offset=syn_off, length=syn_len))
    try:
        ents.append(MessageEntityBlockquote(
            offset=syn_off, length=syn_len, collapsed=True))
    except TypeError:  # ancient telethon without the flag
        print("[autodl] WARNING: this telethon has no collapsed "
              "blockquote flag — upgrade to telethon>=1.45")
        ents.append(MessageEntityBlockquote(
            offset=syn_off, length=syn_len))
    if total > syn_off + syn_len:
        ents.append(MessageEntityBold(
            offset=syn_off + syn_len, length=total - syn_off - syn_len))
    return ents


def _ents_ok(sent, entities) -> bool:
    """Did the sent message keep the bold + collapsed quote?"""
    got = getattr(sent, "entities", None) or []
    names = [(type(e).__name__, getattr(e, "collapsed", None))
             for e in got]
    has_bold = any(n == "MessageEntityBold" for n, _ in names)
    want_cq = any(getattr(e, "collapsed", False) for e in entities)
    has_cq = any(n == "MessageEntityBlockquote" and c
                 for n, c in names)
    return has_bold and (has_cq if want_cq else True)


_FMT_WARNED = False


def telethon_info() -> str:
    """Version line for /status — proves what the deploy runs and
    whether its TL layer even knows collapsed quotes."""
    import telethon
    import inspect
    from telethon.tl.types import MessageEntityBlockquote as bq
    ok = "collapsed" in inspect.signature(bq.__init__).parameters
    return (f"🧩 Telethon {telethon.__version__} · "
            f"collapsed-quote: {'yes' if ok else 'NO (upgrade!)'}")


async def _send_card(chat_id, poster, caption, buttons, fname,
                     entities=None):
    """Send one card, sleeping out any FloodWaitError so channel
    posting never trips Telegram's rate limit. Afterwards the sent
    message is checked: if the bold/collapsed-quote entities did not
    survive, one repair edit is tried and the owner is warned once."""
    from telethon.errors import FloodWaitError
    for _attempt in range(3):
        try:
            if poster:
                buf = io.BytesIO(poster)
                buf.name = fname
                sent = await app.send_file(chat_id, file=buf,
                                           caption=caption,
                                           buttons=buttons,
                                           formatting_entities=entities)
            else:
                sent = await app.send_message(chat_id, caption,
                                              buttons=buttons,
                                              formatting_entities=entities)
            if sent is not None and entities and \
                    not _ents_ok(sent, entities):
                print(f"[autodl] card entities missing on send to "
                      f"{chat_id} — trying one repair edit")
                try:
                    edited = await sent.edit(caption=caption,
                                             buttons=buttons,
                                             formatting_entities=entities)
                    if edited is not None:
                        sent = edited
                except Exception as exc:
                    print(f"[autodl] card repair edit failed: {exc}")
                if not _ents_ok(sent, entities):
                    global _FMT_WARNED
                    if not _FMT_WARNED:
                        _FMT_WARNED = True
                        asyncio.ensure_future(_owner_dm(
                            "⚠️ <b>Card formatting missing</b> on a "
                            f"channel post — build {BUILD}, "
                            f"{telethon_info()}. Please send /status "
                            "and share the reply so the deploy can be "
                            "checked."))
            return sent
        except FloodWaitError as fw:
            core_mod.flood_mark(chat_id, fw.seconds)
            if fw.seconds > 300:
                # an hours-long flood must NOT stall the batch: skip
                # this card now, the repair/backlog pass retries it
                print(f"[autodl] floodwait {fw.seconds}s to "
                      f"{chat_id} — too long, card deferred to the "
                      f"repair pass")
                return None
            print(f"[autodl] floodwait {fw.seconds}s before posting "
                  f"to {chat_id} — sleeping it out")
            await asyncio.sleep(fw.seconds + 5)
        except Exception as exc:
            print(f"[autodl] post failed to {chat_id}: {exc}")
            return None
    return None


async def backlog_cards(channel_id=None, gap=25):
    """Cards for files that are ALREADY archived but were never
    posted to a channel (e.g. an eng-tagged channel added after the
    eng downloads happened). Posted one by one with a gap so
    floodwait never triggers."""
    targets = [c for c in await posting_channels()
               if channel_id is None or c["id"] == channel_id]
    if not targets:
        return 0
    groups = {}
    for r in await store.find("downloaded", {}):
        if r.get("file_link"):
            groups.setdefault((r["norm_title"], r["episode"]),
                              {})[r["quality"]] = r
    if not groups:
        return 0
    items = {}
    try:
        for it in await fetch_latest(None):
            series, ep = parse_api_title(it.get("title", ""))
            items[(normalize_key(series), ep)] = it
    except Exception:
        pass
    made = 0
    paced = False
    for (norm, ep), byq in sorted(groups.items(), key=str):
        for ch in targets:
            kept = {q: r["file_link"] for q, r in byq.items()
                    if _has_eng(q) == ch["eng"]}
            if not kept or await find_post(norm, ep, ch["id"],
                                           ch["eng"]):
                continue
            deeps = {q: (r.get("deep_link")
                         or await ensure_deep_link(r["_id"])
                         or r["file_link"])
                     for q, r in byq.items() if q in kept}
            first = byq[next(iter(byq))]
            item = items.get((norm, ep)) or {
                "title": first.get("title") or norm, "genres": [],
                "rating": None, "description": ""}
            series = parse_api_title(item.get("title", ""))[0] or norm
            poster = None
            try:
                poster, _src = await resolve_poster(item, series)
            except Exception:
                pass
            if paced:
                await asyncio.sleep(gap)  # one by one, no floodwait
            paced = True
            print(f"[autodl] backlog card: {series} Ep {ep} "
                  f"[{ch['tag'] or 'indo'}] -> {ch['id']}")
            made += await post_episode(
                {"series": series, "norm": norm,
                 "entries": [(item, [], [])]},
                ep, kept, deeps, poster, only=[ch])
    return made


async def process_group(group, force=False) -> bool:
    """Download every missing quality of every new episode (one by
    one), archive in the DB channel, then ONE designed poster post
    per series with quality buttons covering the episode range."""
    global CURRENT
    db_ids = await store.db_channel_ids()
    if not db_ids:  # maybe we are mid-blip on local JSON
        await store.ensure_mongo(15)
        db_ids = await store.db_channel_ids()
    if not db_ids:
        BATCH["skip"] = BATCH.get("skip", 0) + 1
        print("[autodl] no DB channel set — skipping group")
        return False

    series = group["series"]
    norm = group["norm"]
    entries = group["entries"]
    first_item = entries[0][0]
    poster, _poster_src = await resolve_poster(first_item, series)

    eps_links = {}  # ep -> {quality: store link}
    eps_deeps = {}  # ep -> {quality: bot deep link}
    posted_any = False
    for item, todo, all_streams in entries:
        _, ep = parse_api_title(item.get("title", ""))
        for stream in todo:
            if not force and await registry_get(
                    norm, ep, stream["quality"]):
                continue  # another batch already archived this file
            # one stalled file must never kill the whole batch or
            # hold the global lock forever
            err = None
            for attempt in (1, 2):  # one retry before giving up
                err = None
                job = None
                try:
                    job = await asyncio.wait_for(
                        run_job(group, item, ep, stream, db_ids,
                                poster),
                        timeout=5400)
                except asyncio.TimeoutError:
                    err = "timeout (90 min)"
                except Exception as exc:
                    err = repr(exc)[:160]
                # a job can also fail QUIETLY (disk low / archive
                # error inside run_job) — never let that stay silent
                if err is None and job is not None \
                        and job.get("status") == "failed":
                    err = job.get("error") or "failed"
                if err is None:
                    break
                print(f"[autodl] job attempt {attempt} failed "
                      f"({err}): {series} Ep {ep} "
                      f"{stream['quality']}")
                if CURRENT is not None:
                    CURRENT["status"] = "failed"
                    CURRENT["error"] = err or "failed"
                CURRENT = None
                _free_up(900)
                if err == "disk low" or err.startswith("archive"):
                    break  # re-downloading cannot fix disk/channel
                await asyncio.sleep(3)
            if err:  # never silent: tell the owner
                print("[autodl] job FAILED:", series, "Ep", ep,
                      stream["quality"], err)
                asyncio.ensure_future(_owner_dm(
                    f"❌ <b>Job failed</b> — {series} Ep {ep} "
                    f"[{stream['quality']}]: {err}"))
            await asyncio.sleep(2)  # smooth the batch send rate

        links = {}
        deeps = {}
        for stream in all_streams:
            q = stream["quality"]
            rec = await registry_get(norm, ep, q)
            if rec and rec.get("file_link"):
                links[q] = rec["file_link"]
                deeps[q] = (rec.get("deep_link")
                            or await ensure_deep_link(
                                _key(norm, ep, q))
                            or rec["file_link"])
        if links:
            eps_links[ep] = links
            eps_deeps[ep] = deeps
            TOUCHED.add((norm, ep))
            posted_any |= bool(
                await post_episode(group, ep, links, deeps, poster))
        # indo archived but the API offers no eng streams yet?
        # remember (series, ep) — the eng watcher polls the search
        # API and grabs it the moment the eng sub lands
        if links and not any(_has_eng(s.get("quality", ""))
                             for s in item.get("streams", [])):
            await pending_eng_add(item.get("section") or "donghua",
                                  series, norm, ep)

    return posted_any or bool(eps_links)


# ---------------------------------------------------------------------- #
# Late eng subs — remembered until the API grows them
# ---------------------------------------------------------------------- #
def _pending_id(section, norm, ep) -> str:
    return f"{section}|{norm}|{ep}"


async def pending_eng_add(section, series, norm, ep) -> None:
    """Indo archived but the API has no eng streams yet: remember
    (series, ep) so the eng watcher retries via the search API."""
    try:
        await store.col("eng_pending").update_one(
            {"_id": _pending_id(section, norm, ep)},
            {"$set": {"section": section, "series": series,
                      "norm": norm, "ep": ep, "status": "pending"},
             "$setOnInsert": {"created": time.time()}},
            upsert=True)
    except Exception as exc:
        print("[autodl] pending-eng save failed:", exc)


async def _registry_has_eng(norm, ep) -> bool:
    """Any eng flavour of this episode already archived?"""
    for r in await store.find("downloaded",
                              {"norm_title": norm, "episode": ep}):
        if _has_eng(r.get("quality", "")) or _has_eng(r.get("_id", "")):
            return True
    return False


async def _search_episode(section: str, series: str, ep):
    """Direct episode lookup on the search API: donghua wants
    '<series> Eps NN', anime just the series name."""
    from urllib.parse import quote
    q = series if section == "anime" else f"{series} Eps {int(ep):02d}"
    for item in await _fetch_one(
            f"{API_ROOT}/{section}/search?q={quote(q)}"):
        s, e2 = parse_api_title(item.get("title", ""))
        if e2 == ep and normalize_key(s) == normalize_key(series):
            item.setdefault("section", section)
            return item
    return None


async def _eng_pending_pass(st) -> int:
    """One sweep: close what already landed, fetch what just did."""
    col = store.col("eng_pending")
    for doc in await store.find("eng_pending", {"status": "pending"}):
        if await _registry_has_eng(doc["norm"], doc["ep"]):
            await col.update_one({"_id": doc["_id"]},
                                 {"$set": {"status": "done",
                                           "done_ts": time.time()}})
    docs = await store.find("eng_pending", {"status": "pending"})
    docs.sort(key=lambda d: d.get("created", 0))
    done = 0
    for doc in docs[:10]:  # gentle per-tick cap
        section = doc.get("section") or "donghua"
        series, ep, norm = doc["series"], doc["ep"], doc["norm"]
        try:
            item = await _search_episode(section, series, ep)
        except Exception as exc:
            print("[autodl] eng search failed:", repr(exc)[:160])
            item = None
        await col.update_one({"_id": doc["_id"]},
                             {"$set": {"checked": time.time()},
                              "$inc": {"tries": 1}})
        if not item:
            continue
        wanted = wanted_streams(item.get("streams", []),
                                st["qualities"], False, True)
        missing = [s for s in wanted
                   if not already_downloaded(norm, ep,
                                             s.get("quality", ""))]
        if not missing:
            if wanted or await _registry_has_eng(norm, ep):
                await col.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"status": "done",
                              "done_ts": time.time()}})
            continue
        quals = ", ".join(s.get("quality", "") for s in missing)
        print(f"[autodl] eng sub arrived: {series} Ep {ep} — "
              f"{quals} — queueing")
        await _owner_dm(f"🌐 <b>Eng sub arrived</b> — {series} Ep {ep}"
                        f" [{quals}] — downloading & uploading now")
        allw = wanted_streams(item.get("streams", []),
                              st["qualities"], True, True)
        await start_batch([(item, missing, allw)])
        if await _registry_has_eng(norm, ep):
            await col.update_one(
                {"_id": doc["_id"]},
                {"$set": {"status": "done", "done_ts": time.time()}})
            done += 1
        await asyncio.sleep(10)
    return done


async def sync_buttons_pass(notify=True):
    """The /syncbutton sweep (call with LOAD held): 1) top up every
    card whose episode has more archived qualities than its buttons,
    2) for every indo card whose eng sub was missing, ask the search
    API — if eng landed, download+upload it now; otherwise remember
    it in eng_pending so the watcher keeps watching."""
    st = await get_settings()
    topped = await refresh_all_cards(gap=4)
    entries = []
    seen = set()
    for doc in await store.find("posts", {}):
        if doc.get("eng"):
            continue  # eng cards need no eng sweep
        norm = doc.get("norm")
        title = doc.get("title") or ""
        for ep in doc.get("eps") or []:
            key = (norm, ep)
            if key in seen:
                continue
            seen.add(key)
            if await _registry_has_eng(norm, ep):
                continue
            section = doc.get("section") or "donghua"
            item = await _search_episode(section, title, ep)
            if item is None and section == "donghua":
                item = await _search_episode("anime", title, ep)
                section = "anime" if item else section
            if item is None:
                await pending_eng_add(section, title, norm, ep)
                continue
            wanted = wanted_streams(item.get("streams", []),
                                    st["qualities"], False, True)
            missing = [s for s in wanted
                       if not already_downloaded(norm, ep,
                                                 s.get("quality", ""))]
            if not missing:
                await pending_eng_add(section, title, norm, ep)
                continue
            entries.append((item, missing,
                            wanted_streams(item.get("streams", []),
                                           st["qualities"],
                                           True, True)))
            if len(entries) >= 20:
                break
    downloaded = 0
    if entries:
        downloaded = await run_batch(entries)  # LOAD already held
    if notify:
        await _owner_dm(
            f"🔄 <b>Sync-button pass done</b>\n\n"
            f"🃏 cards topped up with missing quality buttons: "
            f"<b>{topped}</b>\n"
            f"🌐 eng subs fetched from the search API: "
            f"<b>{len(entries)}</b> episode(s), {downloaded} posted\n"
            f"⏳ still waiting on eng (watcher will keep checking): "
            f"the rest of your indo cards")
    print(f"[autodl] syncbutton: topped={topped} eng_entries="
          f"{len(entries)}")
    return topped, len(entries)


async def eng_pending_loop():
    """Every 15 min: ask the search API for every remembered episode
    whose eng sub was still missing when indo went out. Uploaded
    ones are marked done (and stay saved, so nothing repeats)."""
    await asyncio.sleep(40)
    while True:
        try:
            st = await get_settings()
            if not LOAD.locked():
                n = await _eng_pending_pass(st)
                if n:
                    print(f"[autodl] eng-pending pass finished "
                          f"{n} episode(s)")
        except Exception as exc:
            print("[autodl] eng-pending loop error:", exc)
        await asyncio.sleep(15 * 60)


async def autodl_loop():
    """Background watcher — polls the API for new episodes."""
    await asyncio.sleep(30)  # let the bot settle first
    running = False
    while True:
        try:
            st = await get_settings()
            if st["enabled"] and not running:
                running = True
                try:
                    fresh = await check_new()
                    if fresh:
                        if LOAD.locked():
                            print("[autodl] batch already running — "
                                  "watcher skips re-queueing")
                        else:
                            print(f"[autodl] {len(fresh)} episode(s) "
                                  f"missing from the DB — queueing")
                            await start_batch(fresh)
                finally:
                    running = False
        except Exception as exc:
            print("[autodl] loop error:", exc)
        st = await get_settings()
        await asyncio.sleep(max(60, st.get("interval", 20)) * 60)
