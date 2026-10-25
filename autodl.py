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
import json
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
from core import (LOAD, LOAD_V, app, forcesub_check, invite_link,
                  store, url_bytes)
from handlers_user import forcesub_gate
from mediainfo_util import analyze, human_size
from parser import normalize_key, fuzzy_ratio
from sources import build_metadata
from style import escape_html, post_caption

API_ROOT = "https://api.parciv.workers.dev"
BUILD = "2026-10-25-i"  # stamp shown in /status — proves deploy freshness
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
    if secs and all(isinstance(r, BaseException) for r in res):
        # EVERY source failed — that's an outage, not an empty
        # schedule; the watcher and /status must tell them apart
        API_ERR["ts"] = time.time()
        API_ERR["err"] = repr(res[0])[:160]
    else:
        API_ERR["ts"] = 0.0
        API_ERR["err"] = ""
    return out


def parse_api_title(title: str):
    """'Wan Jie Du Zun 3rd Season Eps 218' -> (series, 218).
    Tolerant: 'Ep/Episode/EP 20' and trailing tags after the number
    ('[720p]', 'Eng', '(Sub)') no longer hide the episode — an item
    must never fail the ep check just because its title carries
    extras."""
    t = (title or "").strip()
    m = None
    for mm in re.finditer(r"[Ee][Pp](?:s|isode)?\.?\s*(\d{1,4})(?!\d)",
                          t):
        m = mm  # last occurrence wins
    if m:
        return t[:m.start()].strip(), int(m.group(1))
    return t, None


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
    # eng flavours got their own selector (/qualityeng): before it
    # existed ONE list ruled both, so an unset eng list mirrors the
    # indo one instead of silently switching eng downloads to "all"
    qse = st.get("qualities_eng")
    if qse is None:
        qse = list(qs)
    return {
        "enabled": bool(st.get("enabled")),
        "qualities": qs,
        "qualities_eng": qse,
        "sections": st.get("sections") or list(DEFAULT_SECTIONS),
        "interval": int(st.get("interval") or 20),
        "last_ids": st.get("last_ids") or [],
    }


def base_quality(label) -> str:
    """'720p-eng' / '720-eng' / '720p' → '720p'."""
    b = (label or "").lower().split("-")[0]
    return b + "p" if b.isdigit() else b


def _sel_ok(base: str, selected) -> bool:
    """Empty list / 'all' = every quality the API offers."""
    want = {str(q).lower() for q in (selected or [])}
    return (not want) or ("all" in want) or (base in want)


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


# The live job queue: each MACHINE LANE runs one file at a time.
JOBS = {}          # job id -> job dict
CURRENT = None     # the running job (lane 1)
CURRENT2 = None    # lane 2 — /parallel batches & a 2nd concurrent batch
BATCHES = {}       # id(state) -> per-batch state; a Koyeb batch and a
                   # VPS batch can run AT THE SAME TIME (separate
                   # machines), so counters/stop-flags are per-batch
API_ERR = {"ts": 0.0, "err": ""}  # set when EVERY source section fails —
                                  # an outage, not "nothing new"
_LOOP_WARN = {"ts": 0.0}          # hourly gate for watcher error DMs


def busy() -> bool:
    """Any batch running on ANY lane?"""
    return LOAD.locked() or LOAD_V.locked()


def lane_free(executor) -> bool:
    """Would a batch with this executor start on its machine RIGHT
    NOW? An idle picked server must never queue behind the other
    machine's work."""
    if executor == "koyeb":
        return not LOAD.locked()
    if executor == "vps":
        return not LOAD_V.locked()
    return not busy()


def _batch_agg():
    """Combined n/total across every active batch (panel header)."""
    return (sum(s["n"] for s in BATCHES.values()),
            sum(s["total"] for s in BATCHES.values()))


async def _watch_mark(fresh=None, err=""):
    """Persist the watcher's last pass (when + result). /status
    renders it, so 'is the watcher alive / why no downloads' is
    never a guessing game again."""
    try:
        await store.set_config("watch", {
            "ts": time.time(),
            "fresh": -1 if fresh is None else int(fresh),
            "err": (err or "")[:200]})
    except Exception:
        pass


async def _loop_warn(msg):
    """Owner DM for watcher-level failures, hourly-gated. A silent
    log-only error is exactly how 'the bot just stopped downloading'
    happens — the owner must HEAR it (at most once an hour)."""
    try:
        now = time.time()
        if now - _LOOP_WARN.get("ts", 0) < 3600:
            return
        _LOOP_WARN["ts"] = now
        await _owner_dm(
            "❌ <b>AutoDL watcher</b>\n<code>"
            + escape_html(str(msg))[:400]
            + "</code>\nThe loop keeps retrying on its own — this "
              "warning repeats at most once an hour.")
    except Exception:
        pass


def cancel_all() -> int:
    """🛑 Stop EVERYTHING: flag the running jobs so they abort at
    their next chunk/callback, and halt EVERY active batch so no
    queued file starts. Returns how many job(s) were flagged."""
    n = 0
    for j in JOBS.values():
        if j["status"] in ("running", "queued"):
            j["cancel"] = True
            n += 1
    for s in BATCHES.values():
        s["stop"] = True
    return n


def _new_job(series, ep, quality, url):
    jid = secrets.token_hex(4)
    job = {"id": jid, "title": series, "ep": ep, "quality": quality,
           "url": url, "phase": "download", "status": "running",
           "done": 0, "total": 0, "up_done": 0, "up_total": 0,
           "speed": 0.0, "cancel": False, "error": "",
           "via": "Koyeb"}
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


PANEL_BUTTONS = [[Button.inline("🔄 Refresh", b"jrefresh"),
                  Button.inline("🛑 Cancel ALL", b"jcancelall")]]


def _job_card(job):
    """The progress body (bar + ⬆/⬇ + phase line) — rendered
    IDENTICALLY for both parallel lanes."""
    lines = []
    if job["phase"] in ("vps-queue", "vps-wait"):
        # handed to the VPS worker, no mirrored progress yet — never
        # render this window as a stalled 0% Koyeb download
        lines.append(f"{_bar(0)}  0.0%")
        lines.append("📌 " + ("Waiting for the 🖥 worker to come back "
                              "online — a minute at most, then this "
                              "file runs on it…"
                              if job["phase"] == "vps-wait" else
                              "Handed to your 🖥 VPS worker — it picks "
                              "this up within seconds…"))
        return lines
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
    return lines


def job_panel(job=None) -> str:
    """The owner-DM progress card (ytdown style)."""
    cpu, ram, total = container_stats()
    lines = ["📋 <b>Your downloads</b>", ""]
    # lane 1 may already be finished while lane 2 still works —
    # promote it so the panel never says "starting…" over a live job
    if job is None:
        job = CURRENT or CURRENT2
    agg_n, agg_total = _batch_agg()
    if job is None:
        if agg_total:
            lines.append(f"⏳ {agg_total} file(s) queued — "
                         f"starting…")
        else:
            lines.append("💤 Idle — nothing downloading right now")
    else:
        state = {"running": ("Running" if job["phase"] == "download"
                             else "Running"),
                 "done": "Done", "failed": "Failed",
                 "cancelled": "Cancelled"}[job["status"]]
        lines.append(f"🎬 {escape_html(job['title'])} "
                     f"Eps {job['ep']} [{job['quality']}]")
        lines.append(f"📋 Video {agg_n}/{agg_total} "
                     f"· {state}")
        lines.append("🛰 Running on: "
                     + ("<b>🖥 VPS worker</b>"
                        if job.get("via") == "VPS"
                        else "<b>☁️ Koyeb</b>"))
        if job.get("fb") and job.get("via") != "VPS":
            lines.append(f"⚠️ 🖥 VPS couldn't take this file "
                         f"({escape_html(str(job['fb']))}) — ☁️ Koyeb "
                         f"is doing it instead")
        lines.append("")
        lines += _job_card(job)
        lines.append("")
        lines.append(f"🆔 {job['id']} · /cancel_{job['id']}")
    j2 = CURRENT2 if job is not CURRENT2 else CURRENT
    if j2 is not None and j2 is not job:
        lines.append("")
        lines.append(f"🎬 <b>2nd lane</b>: {escape_html(j2['title'])} "
                     f"Eps {j2['ep']} [{j2['quality']}]")
        lines.append("🛰 Running on: "
                     + ("<b>🖥 VPS worker</b>"
                        if j2.get("via") == "VPS"
                        else "<b>☁️ Koyeb</b>"))
        lines.append("")
        lines += _job_card(j2)
        lines.append("")
        lines.append(f"🆔 {j2['id']} · /cancel_{j2['id']}")
    lines.append("")
    lines.append(f"📊 Container CPU: {cpu:.0f}% of quota · "
                 f"RAM: {ram:.1f} MiB / {total:.1f} MiB")
    queued = max(0, agg_total - agg_n)
    active = sum(1 for j in (CURRENT, CURRENT2)
                 if j is not None and j.get("status") == "running")
    lines.append(f"Active/queued: {active} / {queued} · "
                 f"/cancel_&lt;id&gt; stops one · /cancelall stops all")
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


async def _owner_dm(text: str, buttons=None):
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
        return await app.send_message(ent, text, parse_mode="html",
                                      buttons=buttons)
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
                                               parse_mode="html",
                                               buttons=PANEL_BUTTONS)
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


BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/124.0.0.0 Safari/537.36")


MIN_VIDEO_BYTES = 1024 * 1024   # no episode video is ever smaller —
                                # a 3 KB "video" is a CDN error page
# Content-Types that are NEVER a video: a dead CDN link answers HTTP
# 200 with one of these tiny error bodies.
ERROR_CTYPES = ("text/html", "application/json", "text/plain",
                "text/xml", "application/xml")
MAX_RESUME = 400    # hosts that drop every connection after a few MB
                    # need ONE resume per cut — a 359 MB episode cut
                    # every ~3 MB is ~120 of them, so the cap has to
                    # be far above any sane file size / cut size
STALL_LIMIT = 3     # attempts in a row that add ZERO bytes = dead


class _CdnError(RuntimeError):
    """A download failure that RESUMING cannot fix on this url: dead
    link, error-page body, HTTP status, a server that throws Range
    away. Abandons ONE candidate route so download_stream can try the
    next way of reaching the very same file."""


def _referer_for(url: str) -> str:
    """Referer for the 403 retry: the CDN's own root for a direct
    seedbox url, the API root for a proxy url."""
    m = re.match(r"(https?://[^/]+)/", url or "")
    if m and "parciv" not in m.group(1):
        return m.group(1) + "/"
    return API_ROOT + "/"


def _host_of(url: str) -> str:
    """The host of a url — error messages name the SERVER that
    misbehaved instead of wasting their 200 characters on the
    (very long) proxy link."""
    m = re.match(r"https?://([^/]+)", url or "")
    return m.group(1) if m else str(url)[:40]


def stream_candidates(url: str):
    """Every route to this one file, best first.

    The API hands out Cloudflare-Worker PROXY links
    (…/anime/stream?url=<encoded seedbox url>). A Worker is not
    allowed to forward the Range header (it is on Cloudflare's
    forbidden-header list, and the cache drops it on a miss too), so
    when that proxy cuts a transfer after a few megabytes it serves
    the SAME first bytes forever — exactly the
    'truncated download: 3062823/376614938 B' failure that kept
    killing 720p. The decoded seedbox url is candidate 2: the file
    server itself answers Range with 206, so the download can
    actually be CONTINUED instead of restarted. A direct url handed
    to us gets the proxy forms as its fallbacks."""
    out = [url]
    m = re.search(r"/(?:anime|donghua)/stream\?url=([^&\s]+)",
                  url or "")
    if m:
        try:
            from urllib.parse import unquote
            direct = unquote(m.group(1))
        except Exception:
            direct = ""
        if direct.startswith("http") and direct not in out:
            out.append(direct)
    elif str(url).startswith("http"):
        try:
            from urllib.parse import quote
            for sec in ("anime", "donghua"):
                p = "%s/%s/stream?url=%s" % (API_ROOT, sec,
                                             quote(url, safe=""))
                if p not in out:
                    out.append(p)
        except Exception:
            pass
    return out[:3]


async def download_stream(url: str, dest: str, job=None) -> str:
    """Download one episode file — ROUTED, RESUMED, VALIDATED.

    Three layers, because the CDNs behind the API are hostile:
      1. ROUTES — the proxy link first, the direct seedbox url it
         hides behind second (stream_candidates);
      2. RESUME — a route that cuts the connection after a few MB is
         re-requested with ``Range: bytes=<bytes on disk>-`` and the
         new bytes are APPENDED, over and over (up to MAX_RESUME)
         until the file is complete. A cut is not a failure any more;
         it is one more request. A restart mid-download still
         continues from the bytes already on disk.
      3. VALIDATE — an error-page Content-Type, a body that cannot be
         continued, or anything under MIN_VIDEO_BYTES raises instead
         of being archived as a 'video' and wired into episode cards.
    Every failure ends up VISIBLE (panel feedback + log), never as a
    3 MB 'episode'."""
    errors = []
    best = 0
    for cand in stream_candidates(url):
        try:
            return await _download_route(cand, dest, job)
        except JobCancelled:
            raise
        except Exception as exc:
            have = os.path.getsize(dest) if os.path.exists(dest) else 0
            best = max(best, have)
            errors.append(f"{_host_of(cand)}: {repr(exc)[:110]}")
            print(f"[autodl] download route failed ({cand[:90]}): "
                  f"{repr(exc)[:160]}")
            for gone in (dest, dest + SEG_SUFFIX,
                         dest + STATE_SUFFIX):
                try:
                    os.remove(gone)   # the next route can be a
                except Exception:     # different host/encode — never
                    pass              # continue somebody else's bytes
    raise RuntimeError(
        f"every CDN route failed ({len(errors)} tried, most got to "
        f"{human_size(best) or '0 B'}): " + " | ".join(errors)[:240])


SEG_PARTS = 4               # parallel Range connections per file
SEG_MIN_FILE = 12 << 20     # below this, one connection is plenty
MIN_SPEED = 64 * 1024       # B/s — under this the CDN is hopeless
SPEED_GRACE = 60            # seconds measured before judging it
MAX_ETA = 45 * 60           # projected finish beyond this = skip it
SEG_SUFFIX = ".seg"         # preallocated in-progress file
STATE_SUFFIX = ".dlpart"    # its resume sidecar (json)


class _SpeedWatch:
    """Honest aggregate throughput PLUS a verdict.

    A CDN that dribbles 1 KB/s is worse than one that refuses: it
    held a lane for hours (the '🚀 1.1 KB/s · ⏳ ETA 211965s' panel)
    while 34 other videos waited behind it. After SPEED_GRACE
    seconds, a route under MIN_SPEED — or one whose projected finish
    sits past MAX_ETA — is abandoned, the file is skipped, and the
    batch retries it at the end instead of grinding."""

    def __init__(self, total, done=0):
        self.total = int(total or 0)
        self.t0 = time.time()
        self.d0 = int(done or 0)
        self.rate = 0.0
        self._last = 0.0

    def update(self, done):
        now = time.time()
        if now - self._last >= 1.0:
            self._last = now
            self.rate = max(0, int(done) - self.d0) \
                / max(0.001, now - self.t0)
        return self.rate

    def verdict(self, done):
        """None while the route is healthy, a reason when hopeless."""
        el = time.time() - self.t0
        if el < SPEED_GRACE:
            return None
        rate = self.rate or (max(0, int(done) - self.d0)
                             / max(0.001, el))
        if rate < 1024:
            return f"CDN sent nothing for {int(el)}s"
        if rate < MIN_SPEED:
            return (f"CDN crawling at {int(rate // 1024)} KB/s "
                    f"(floor {MIN_SPEED // 1024} KB/s)")
        if self.total:
            eta = (self.total - int(done)) / rate
            if eta > MAX_ETA:
                return (f"CDN too slow — {int(rate // 1024)} KB/s, "
                        f"ETA {int(eta // 60)} min (cap "
                        f"{MAX_ETA // 60} min)")
        return None


def _seg_paths(dest):
    return dest + SEG_SUFFIX, dest + STATE_SUFFIX


def _partition(total, nparts):
    """[start, end, pos] triples covering exactly [0, total)."""
    step = max(1 << 20, (int(total) + nparts - 1) // nparts)
    out = []
    start = 0
    while start < total:
        end = min(int(total), start + step)
        out.append([start, end, start])
        start = end
    return out


def _load_state(path, url, total, parts):
    """The resume sidecar. Only one for the SAME url and size is
    trusted. Recording a pos BEHIND the bytes actually written is
    safe: re-fetching a range writes identical bytes back at the
    identical offset, so a crash between a write and a save costs a
    little re-download, never corruption."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            st = json.load(fh)
    except Exception:
        return None
    if st.get("url") != url or int(st.get("total") or -1) != int(total):
        return None
    saved = st.get("parts") or []
    if len(saved) != len(parts):
        return None
    for i, (part, sp) in enumerate(zip(parts, saved)):
        try:
            pos = int(sp[1])
            if int(sp[0]) != part[0]:
                return None
        except Exception:
            return None
        parts[i][2] = max(part[0], min(part[1], pos))
    return parts


def _save_state(path, url, total, parts):
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump({"url": url, "total": int(total),
                       "parts": [[p[0], p[2]] for p in parts]}, fh)
        os.replace(tmp, path)
    except Exception:
        pass


async def _probe(url):
    """One 1-byte Range request: does this route HONOUR Range, and
    how big is the file? Returns (total, ranges_ok, referer, error).
    The Cloudflare-Worker proxy answers 200 (Range is on its
    forbidden-header list) — that route can only be walked
    sequentially; the seedbox behind it answers 206 and can be
    downloaded SEG_PARTS connections at a time."""
    s, _ = http_session(900)
    timeout = aiohttp.ClientTimeout(total=90, sock_connect=30,
                                    sock_read=60)
    err = "probe failed"
    for referer in (False, True):
        headers = {"User-Agent": BROWSER_UA, "Accept": "*/*",
                   "Accept-Language": "en-US,en;q=0.9",
                   "Range": "bytes=0-0"}
        if referer:
            headers["Referer"] = _referer_for(url)
        try:
            async with s.get(url, headers=headers,
                             timeout=timeout) as r:
                ctype = (r.headers.get("Content-Type") or "").lower()
                if ctype.startswith(ERROR_CTYPES):
                    return 0, False, referer, (
                        f"CDN served an error page ({ctype}, HTTP "
                        f"{r.status}), not a video — the link is dead")
                rng = r.headers.get("Content-Range") or ""
                tail = rng.split("/")[-1] if "/" in rng else ""
                if r.status == 206:
                    return (int(tail) if tail.isdigit() else 0), \
                        True, referer, None
                if r.status == 200:
                    return int(r.headers.get("Content-Length") or 0), \
                        False, referer, None
                if r.status == 416:
                    return (int(tail) if tail.isdigit() else 0), \
                        False, referer, None
                if r.status in (401, 403, 451) and not referer:
                    err = f"HTTP {r.status}"
                    continue
                return 0, False, referer, f"HTTP {r.status}"
        except Exception as exc:
            err = repr(exc)[:110]
    return 0, False, False, err


async def _download_route(url: str, dest: str, job=None) -> str:
    """ONE candidate url: probe it, then take the fastest path it
    supports — SEG_PARTS parallel Range requests for a big file on a
    Range-honouring host, otherwise a sequential walk that resumes
    after every cut. Both are validated, and both abandon a route
    that crawls instead of holding the lane for hours."""
    total, ranges, referer, why = await _probe(url)
    if why:
        raise _CdnError(why)
    have = os.path.getsize(dest) if os.path.exists(dest) else 0
    if total and have >= total and have >= MIN_VIDEO_BYTES:
        if job is not None:
            job["done"] = job["total"] = have
        return dest                     # already on disk, complete
    if ranges and total >= SEG_MIN_FILE:
        return await _download_parallel(url, dest, job, total,
                                        referer)
    return await _download_sequential(url, dest, job, referer)


async def _download_parallel(url: str, dest: str, job, total,
                             referer=False) -> str:
    """SEG_PARTS concurrent Range requests into ONE preallocated file.

    This is how leech bots pull 6+ MB/s off a CDN that gives a single
    connection a trickle: four streams in flight instead of one, and
    a cut costs ONE part a few MB (it resumes itself) instead of
    failing the file. The in-progress file is ``<dest>.seg`` with a
    JSON sidecar of part offsets; only when EVERY part is complete
    and the size matches is it renamed onto ``dest`` — a half-built
    file can never be mistaken for a finished one, and a restart
    picks the parts up exactly where they stopped."""
    seg, state = _seg_paths(dest)
    parts = _partition(total, SEG_PARTS)
    have = os.path.getsize(dest) if os.path.exists(dest) else 0
    if _load_state(state, url, total, parts) is None:
        try:
            if have:
                # a dense prefix on `dest` (sequential path, or a run
                # from before this feature) is REAL data — move it
                # over and credit every byte it covers
                os.replace(dest, seg)
                have = os.path.getsize(seg)
            elif os.path.exists(seg):
                os.remove(seg)
        except Exception:
            have = 0
        fd0 = os.open(seg, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            os.truncate(fd0, total)
        finally:
            os.close(fd0)
        for p in parts:
            p[2] = max(p[0], min(p[1], have))
    present = lambda: sum(p[2] - p[0] for p in parts)   # noqa: E731
    if job is not None:
        job["total"] = total
        job["done"] = present()
    watch = _SpeedWatch(total, present())
    stop = {"err": None}
    loop = asyncio.get_running_loop()
    fd = os.open(seg, os.O_RDWR)
    try:
        async def _part(i):
            nonlocal referer
            s, _ = http_session(900)
            timeout = aiohttp.ClientTimeout(total=None, sock_connect=30,
                                            sock_read=180)
            stalls = 0
            resumes = 0
            saved = time.time()
            while True:
                if stop["err"] is not None:
                    return
                if job is not None and job["cancel"]:
                    raise JobCancelled()
                start, end, pos = parts[i]
                if pos >= end:
                    return
                headers = {"User-Agent": BROWSER_UA, "Accept": "*/*",
                           "Accept-Language": "en-US,en;q=0.9",
                           "Range": f"bytes={pos}-{end - 1}"}
                if referer:
                    headers["Referer"] = _referer_for(url)
                got, cut = 0, None
                try:
                    async with s.get(url, headers=headers,
                                     timeout=timeout) as r:
                        if r.status in (401, 403, 451) and not referer:
                            referer = True
                            continue
                        if r.status != 206:
                            raise _CdnError(
                                f"part {i + 1}/{len(parts)}: HTTP "
                                f"{r.status} for Range bytes={pos}-"
                                f"{end - 1}")
                        ctype = (r.headers.get("Content-Type")
                                 or "").lower()
                        if ctype.startswith(ERROR_CTYPES):
                            raise _CdnError(
                                f"CDN served an error page ({ctype}) "
                                f"mid-file — the link died")
                        async for chunk in \
                                r.content.iter_chunked(1 << 20):
                            if stop["err"] is not None:
                                return
                            if job is not None and job["cancel"]:
                                raise JobCancelled()
                            if len(chunk) > end - pos:
                                chunk = chunk[:end - pos]
                            if not chunk:
                                break
                            await loop.run_in_executor(
                                None, os.pwrite, fd, chunk, pos)
                            pos += len(chunk)
                            got += len(chunk)
                            parts[i][2] = pos
                            d = present()
                            rate = watch.update(d)
                            if job is not None:
                                job["done"] = d
                                job["speed"] = rate
                            bad = watch.verdict(d)
                            if bad:
                                stop["err"] = bad
                                raise _CdnError(bad)
                            now = time.time()
                            if now - saved >= 3.0:
                                saved = now
                                _save_state(state, url, total, parts)
                except (JobCancelled, _CdnError):
                    raise
                except Exception as exc:
                    cut = exc      # cut/reset/stall: the part resumes
                if parts[i][2] >= end:
                    return
                if got:
                    stalls = 0
                    resumes += 1
                    if resumes > MAX_RESUME:
                        raise _CdnError(
                            f"part {i + 1}: CDN cut it {resumes} "
                            f"times, still {parts[i][2]}/{end} B")
                    await asyncio.sleep(0.15)
                    continue
                stalls += 1
                if stalls >= STALL_LIMIT:
                    raise _CdnError(
                        f"part {i + 1}: no progress at {parts[i][2]}/"
                        f"{end} B ({repr(cut)[:70] if cut else 'EOF'})")
                await asyncio.sleep(0.8 * stalls)

        tasks = [asyncio.ensure_future(_part(i))
                 for i in range(len(parts))]
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            stop["err"] = stop["err"] or "aborted"
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        _save_state(state, url, total, parts)
        got = present()
        if got < total or os.path.getsize(seg) != total:
            raise _CdnError(f"parallel download incomplete: {got}/"
                            f"{total} B")
    finally:
        try:
            os.close(fd)
        except Exception:
            pass
    if total < MIN_VIDEO_BYTES:
        raise _CdnError(f"file is only {total} B — no episode is that "
                        "small; dead CDN link")
    try:
        os.remove(state)
    except Exception:
        pass
    os.replace(seg, dest)          # only a COMPLETE file lands here
    if job is not None:
        job["done"] = job["total"] = total
    print(f"[autodl] {len(parts)}-part parallel download done: "
          f"{total} B from {_host_of(url)}")
    return dest


async def _download_sequential(url: str, dest: str, job=None,
                               referer=False) -> str:
    """A route that ignores Range (the Cloudflare proxy) or a small
    file: one connection, resumed after every cut until complete."""
    s, _ = http_session(900)
    # total=None: a big file on a slow line can take far longer than
    # any fixed TOTAL timeout — the old 900 s cap killed large
    # transfers mid-file. Bound the STALL instead: 180 s without a
    # single byte is dead, slow-but-moving may run as long as it
    # needs — and _SpeedWatch decides when "slow" becomes hopeless.
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30,
                                    sock_read=180)
    loop = asyncio.get_running_loop()
    resumes = 0
    stalls = 0
    watch = None
    while True:
        have = os.path.getsize(dest) if os.path.exists(dest) else 0
        headers = {"User-Agent": BROWSER_UA, "Accept": "*/*",
                   "Accept-Language": "en-US,en;q=0.9"}
        if referer:
            headers["Referer"] = _referer_for(url)
        if have:
            headers["Range"] = f"bytes={have}-"
        done, total, cut, ignored = have, 0, None, False
        try:
            async with s.get(url, headers=headers, timeout=timeout) as r:
                status = r.status
                if status in (401, 403, 451) and not referer:
                    print(f"[autodl] download HTTP {status} — retrying "
                          f"with referer headers: {url[:90]}")
                    referer = True
                    continue
                if status == 416:
                    # Range Not Satisfiable — the bytes on disk reach
                    # past the end of the real file: complete when the
                    # server names the size, junk otherwise
                    rng = r.headers.get("Content-Range") or ""
                    tot = int(rng.split("/")[-1]) if "/" in rng else 0
                    if tot and have >= tot \
                            and have >= MIN_VIDEO_BYTES:
                        if job is not None:
                            job["done"] = job["total"] = have
                        return dest
                    raise _CdnError(f"HTTP 416 with {have} B on disk "
                                    f"(server size {tot} B)")
                if status == 206 and have:
                    mode = "ab"          # partial content: APPEND
                    rng = r.headers.get("Content-Range") or ""
                    total = int(rng.split("/")[-1]) if "/" in rng else 0
                elif status == 200:
                    mode, done = "wb", 0
                    total = int(r.headers.get("Content-Length") or 0)
                    ignored = bool(have)   # our Range was thrown away
                else:
                    raise _CdnError(f"HTTP {status}")
                ctype = (r.headers.get("Content-Type") or "").lower()
                if ctype.startswith(ERROR_CTYPES):
                    raise _CdnError(
                        f"CDN served an error page ({ctype}, HTTP "
                        f"{status}), not a video — the link is dead")
                if job is not None and total:
                    job["total"] = total
                if watch is None and total:
                    watch = _SpeedWatch(total, done)
                with open(dest, mode) as f:
                    async for chunk in r.content.iter_chunked(1 << 20):
                        if job is not None and job["cancel"]:
                            raise JobCancelled()
                        if total and done + len(chunk) > total:
                            # never write past the size the server
                            # DECLARED: an overshooting body (or a
                            # bogus resume offset) would otherwise be
                            # archived as a corrupt "video"
                            chunk = chunk[:max(0, total - done)]
                        if not chunk:
                            break
                        # disk write off the event loop: the bot keeps
                        # answering users while multi-GB files land
                        await loop.run_in_executor(None, f.write, chunk)
                        done += len(chunk)
                        if job is not None:
                            job["done"] = done
                        if watch is not None:
                            rate = watch.update(done)
                            if job is not None:
                                job["speed"] = rate
                            bad = watch.verdict(done)
                            if bad:
                                raise _CdnError(bad)
        except (JobCancelled, _CdnError):
            raise
        except Exception as exc:
            # connection reset, payload cut, socket stall: everything
            # already written stays on disk, so this IS resumable
            cut = exc
        if total and done >= total:
            if done < MIN_VIDEO_BYTES:
                raise _CdnError(
                    f"file is only {done} B — no episode is that "
                    "small; dead CDN link (error page body)")
            if job is not None:
                job["done"], job["total"] = done, total
            return dest
        if not total and cut is None:
            if done >= MIN_VIDEO_BYTES:
                return dest        # chunked, no length: trust the EOF
            raise _CdnError(f"file is only {done} B — no episode is "
                            "that small; dead CDN link")
        if ignored:
            raise _CdnError(
                f"server ignored Range (answered 200 for "
                f"bytes={have}-) and cut at {done}/"
                f"{total or '?'} B — this route can never be resumed")
        if total and total < MIN_VIDEO_BYTES:
            raise _CdnError(f"server says the whole file is {total} B "
                            "— no episode is that small; dead link")
        if done > have:
            stalls = 0
            resumes += 1
            if resumes > MAX_RESUME:
                raise RuntimeError(
                    f"CDN cut the transfer {resumes} times and still "
                    f"only {done}/{total} B landed")
            print(f"[autodl] resume {resumes}: {done}/"
                  f"{total or '?'} B on disk "
                  f"({repr(cut)[:60] if cut else 'server EOF'})")
            await asyncio.sleep(0.25)
            continue
        stalls += 1
        if stalls >= STALL_LIMIT:
            raise RuntimeError(
                f"CDN made no progress {stalls}× at {done}/{total} B "
                f"({repr(cut)[:80] if cut else 'clean EOF'})")
        await asyncio.sleep(1.0 * stalls)


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
    """The ENGLISH title only (AniList 'english' / MAL
    'title_english'). Romaji/pinyin main titles must never leak in
    as a fake '| english' suffix (the 'Wu Shang Shen Di 2' case)."""
    bs = (meta or {}).get("by_source") or {}
    for src in ("anilist", "jikan"):
        r = bs.get(src)
        t = (getattr(r, "eng_title", "") or "").strip()
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


def wanted_streams(streams, qualities, want_base: bool, want_eng: bool,
                   eng_qualities=None):
    """Streams matching the configured qualities AND the tags of
    the channels we post to (indo -> base, eng -> '-eng' only).
    /qualityindo and /qualityeng keep SEPARATE lists, so eng flavours
    are filtered by eng_qualities when the caller has one (None keeps
    the old single-list behaviour)."""
    out = []
    for s in streams or []:
        label = (s.get("quality") or "").lower()
        base = base_quality(label)
        eng = _has_eng(label)
        if eng:
            if not want_eng:
                continue
            if not _sel_ok(base, eng_qualities if eng_qualities
                           is not None else qualities):
                continue
        else:
            if not want_base:
                continue
            if not _sel_ok(base, qualities):
                continue
        out.append(s)
    return out


# ---------------------------------------------------------------------- #
# What is new?
# ---------------------------------------------------------------------- #
async def check_new():
    """(item, streams_to_download, all_streams) for every episode
    that still has a quality missing from the registry.

    TWO guards keep a batch honest:
      • DEDUPE — the API can list the same episode twice (two ids,
        'Eps 4' and 'Eps 04', or both sections), which used to queue
        the very same file twice: it failed as 'Video 1/35' and then
        again as 'Video 2/35' — the loop the owner saw. One (series,
        ep, quality) = one job per pass.
      • NO RETRO-FILL — ticking a quality later (/qualityindo,
        /qualityeng) applies to NEW episodes only. An episode that is
        already known keeps the selection that was in force when it
        was first seen, so old episodes are never dragged back into
        the queue for a quality nobody asked for then. /searchad and
        /syncbutton still top anything up on demand (force paths)."""
    st = await get_settings()
    items = await fetch_latest(st["sections"])
    fresh = []
    seen_keys = set()
    for item in items:
        series, ep = parse_api_title(item.get("title", ""))
        norm = normalize_key(series)
        # always archive every flavour the API offers; channel tags
        # only decide WHERE each flavour gets posted
        streams = wanted_streams(item.get("streams", []),
                                 st["qualities"], True, True,
                                 st["qualities_eng"])
        if not streams:
            continue
        allowed = await _ep_allowed(norm, ep, st)
        todo = []
        for s in streams:
            q = s.get("quality", "")
            if already_downloaded(norm, ep, q):
                continue
            if not allowed(q):
                continue      # quality was not selected for THIS ep
            k = (norm, str(ep), base_quality(q), _has_eng(q))
            if k in seen_keys:
                continue      # same file already queued this pass
            seen_keys.add(k)
            todo.append(s)
        if todo:
            fresh.append((item, todo, streams))
    return fresh


def registered_quals(norm, ep) -> set:
    """Every quality of this episode the registry already holds."""
    pre = f"{norm}:{ep}:"
    return {k[len(pre):] for k in _registered if k.startswith(pre)}


async def _ep_allowed(norm, ep, st):
    """Remember the selection in force the FIRST time an episode is
    seen; from then on only those qualities may be fetched for it
    (intersected with the live selection, so unticking still stops
    everything). Episodes taken before this feature existed adopt the
    qualities they ALREADY have — that is what clears the backlog of
    old episodes missing 720p instead of re-queueing all of them."""
    doc = None
    try:
        doc = await store.col("ep_seen").find_one(
            {"_id": f"{norm}:{ep}"})
    except Exception:
        doc = None
    if doc is None:
        known = registered_quals(norm, ep)
        if known:
            q = sorted({base_quality(x) for x in known
                        if not _has_eng(x)})
            qe = sorted({base_quality(x) for x in known if _has_eng(x)})
        else:
            q, qe = list(st["qualities"]), list(st["qualities_eng"])
        try:
            await store.col("ep_seen").update_one(
                {"_id": f"{norm}:{ep}"},
                {"$set": {"q": q, "qe": qe, "norm": norm, "ep": ep,
                          "ts": time.time()}}, upsert=True)
        except Exception:
            pass
    else:
        q = doc.get("q") or []
        qe = doc.get("qe") or []

    def allowed(label):
        base = base_quality(label)
        if _has_eng(label):
            return _sel_ok(base, st["qualities_eng"]) \
                and _sel_ok(base, qe)
        return _sel_ok(base, st["qualities"]) and _sel_ok(base, q)
    return allowed


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
             if f.endswith((".mp4", ".mkv", ".part",
                            ".seg", ".dlpart"))),
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


def sweep_orphans(max_age_h=12):
    """Koyeb-side disk hygiene (every watchdog tick): ANY file in
    DOWNLOAD_DIR older than max_age_h is trash — abandoned /fast
    ingests whose confirm menu was never tapped, leftovers of a
    hard-crashed job (normal jobs remove their file in run_job's
    finally; active downloads refresh their mtime with every chunk
    and a single file times out at 90 min, far below the cutoff).
    Returns (files, MB) reclaimed."""
    now = time.time()
    n = mb = 0
    try:
        for f in os.listdir(DOWNLOAD_DIR):
            p = os.path.join(DOWNLOAD_DIR, f)
            try:
                if not os.path.isfile(p):
                    continue
                if now - os.path.getmtime(p) < max_age_h * 3600:
                    continue
                mb += os.path.getsize(p) // 1048576
                os.remove(p)
                n += 1
                print("[autodl] orphan sweep: removed", f)
            except Exception:
                continue
    except Exception:
        pass
    return n, mb


VPS_JOB_TIMEOUT = 20 * 60


async def _vps_archive(job, series, ep, q, url, db0, poster,
                       force=False):
    """Hand download+upload to the VPS worker. Returns the archive
    Message in db0, or None so run_job falls back to Koyeb."""
    import base64 as _b64
    jid = secrets.token_hex(6)
    doc = {"_id": jid, "status": "pending", "ts": time.time(),
           "url": url, "channel": int(db0), "series": series,
           "ep": ep, "quality": q,
           "caption": f"{series} — Ep {ep} [{q}]",
           "filename": f"{series} Eps {int(ep):02d} [{q}].mp4",
           "force": bool(force)}
    if poster:
        doc["thumb_b64"] = _b64.b64encode(
            core_mod.thumb_16x9(poster) or b"").decode()
    await store.col("vps_jobs").update_one({"_id": jid},
                                           {"$set": doc},
                                           upsert=True)
    print(f"[autodl] VPS job {jid} queued: {series} Ep {ep} [{q}]")
    deadline = time.time() + VPS_JOB_TIMEOUT
    while time.time() < deadline:
        if job.get("cancel"):
            break
        d = await store.find("vps_jobs", {"_id": jid})
        d = d[0] if d else None
        if not d:
            job["fb"] = "job doc vanished"
            return None
        prog = d.get("prog") or {}
        if prog and (time.time() - prog.get("ts", 0)) < 30:
            # live VPS progress into the normal job/panel fields
            job.pop("fb", None)   # the worker IS on it now — any
            # progress flowing = the worker is ALIVE on this file:
            # a big 720p transfer can legitimately outrun the flat
            # 20-min deadline, which used to make Koyeb steal (and
            # DOUBLE-archive) a job that was still working — push
            # the deadline out while reports keep arriving
            deadline = time.time() + 10 * 60
            ph = prog.get("phase") or job["phase"]   # reason is moot
            job["phase"] = ph
            if ph == "upload":
                job["up_done"] = prog.get("done") or 0
                job["up_total"] = prog.get("total") or 0
            else:
                job["done"] = prog.get("done") or 0
                job["total"] = prog.get("total") or 0
            job["speed"] = prog.get("speed") or 0
            job["via"] = "VPS"
        st = d.get("status")
        if st == "done":
            msg_id = (d.get("result") or {}).get("msg_id")
            sent = None
            try:
                sent = await app.get_messages(int(db0), ids=msg_id)
            except Exception:
                sent = None
            await store.col("vps_jobs").delete_one({"_id": jid})
            if sent:
                return sent
            print(f"[autodl] VPS done but message missing "
                  f"({db0}/{msg_id}) — falling back")
            job["fb"] = "archived but message fetch failed"
            return None
        if st == "failed":
            print("[autodl] VPS job failed:",
                  str(d.get("error"))[:140])
            job["fb"] = ("worker failed: "
                         + str(d.get("error") or "unknown")[:80])
            await store.col("vps_jobs").delete_one({"_id": jid})
            return None
        await asyncio.sleep(3)
    await store.col("vps_jobs").delete_one({"_id": jid})
    if not job.get("cancel"):
        job["fb"] = "worker didn't finish in 20 min"
    print("[autodl] VPS job timed out / cancelled — Koyeb takes over")
    return None


async def run_job(group, item, ep, stream, db_ids, poster,
                  executor="auto", st=None):
    """One queued file: download (with live progress + cancel),
    archive in every DB channel (with upload progress), register.
    executor: 'auto' (VPS when mode ON + online, else Koyeb),
    'koyeb' (never VPS) or 'vps' (force VPS when online).
    st = the owning batch's state (counters + held lane locks)."""
    global CURRENT, CURRENT2
    series = group["series"]
    norm = group["norm"]
    q = stream["quality"]
    from telethon.errors import FloodWaitError
    job = _new_job(series, ep, q, stream["url"])
    if CURRENT is None:
        CURRENT = job
    else:
        CURRENT2 = job   # /parallel: second lane
    if st is None:
        st = {"n": 0, "total": 0, "locks": ()}
    st["n"] += 1
    print(f"[autodl] job {st['n']}/{st['total']} start: "
          f"{series} Ep {ep} [{q}]")
    fname = f"{norm}_{ep}_{q}.mp4".replace(" ", "_")
    path = os.path.join(DOWNLOAD_DIR, fname)
    _fb_lock = False
    try:
        vps_sent = None
        if executor != "koyeb":
            vps = await core_mod.vps_state()
            wants_vps = executor == "vps" or vps.get("enabled")
            if wants_vps and vps.get("secret") \
                    and not vps.get("online") \
                    and (time.time() - (vps.get("beat") or 0)) < 900:
                # TRANSIENT offline — a Koyeb redeploy gap (beats
                # can't land while the bot restarts), a beat hiccup,
                # or an old worker that can't beat mid-job. The
                # watchdog revives dead workers and beats land within
                # ~90 s, so wait a little instead of handing the file
                # to Koyeb while the user's VPS is right there. A
                # LONG-dead worker (beat >15 min old) skips the wait
                # and falls back immediately.
                job["via"] = "VPS"
                job["phase"] = "vps-wait"
                for _ in range(9):
                    await asyncio.sleep(10)
                    if job.get("cancel"):
                        break
                    vps = await core_mod.vps_state()
                    if vps.get("online"):
                        break
                job["phase"] = "download"
            if vps.get("online") and wants_vps:
                # panel must NEVER show a VPS-bound file as a stalled
                # Koyeb download during the queue/claim/login window
                job["via"] = "VPS"
                job["phase"] = "vps-queue"
                vps_sent = await _vps_archive(
                    job, series, ep, q, stream["url"], db_ids[0],
                    poster, force=(executor == "vps"))
                if vps_sent is None and not job.get("cancel"):
                    # worker failed (often a stale CDN url → 404):
                    # refresh it and give the VPS ONE more try before
                    # Koyeb takes the job over
                    fresh = await refresh_stream_url(
                        series, ep, q, stream["url"])
                    if fresh and fresh != stream["url"]:
                        stream["url"] = fresh
                        vps_sent = await _vps_archive(
                            job, series, ep, q, fresh, db_ids[0],
                            poster, force=(executor == "vps"))
            elif wants_vps and vps.get("secret") \
                    and not job.get("cancel"):
                job["fb"] = "worker offline"
        if vps_sent is not None:
            _sz = getattr(getattr(vps_sent, "file", None),
                          "size", 0) or 0
            if 0 < _sz < MIN_VIDEO_BYTES:
                # an OLD worker with no size gate archived a CDN
                # error page — refuse it, wipe it from the DB
                # channel, and redo the file on the validated path
                print(f"[autodl] VPS archived a {_sz} B broken file "
                      "— deleting + redoing on Koyeb")
                try:
                    await app.delete_messages(int(db_ids[0]),
                                              [vps_sent.id])
                except Exception:
                    pass
                vps_sent = None
                job["fb"] = (f"worker archived a broken {_sz} B file "
                             "(dead CDN link) — deleted, redoing")
        job["via"] = "VPS" if vps_sent is not None else "Koyeb"
        if vps_sent is not None:
            job.pop("fb", None)
        if vps_sent is None:
            job["phase"] = "download"   # out of the vps-queue/wait card
            if job.get("cancel"):
                raise JobCancelled()
            if LOAD not in st.get("locks", ()):
                # A VPS-lane batch falling back to Koyeb: take the
                # Koyeb lane lock first, so the free tier NEVER runs
                # two local files at once even when a Koyeb batch is
                # going. Lock order is V-then-K everywhere (same as
                # start_batch), so this can't deadlock.
                await LOAD.acquire()
                _fb_lock = True
            if not _disk_ok(0):
                _free_up(900)
            if not _disk_ok(0):
                job["status"] = "failed"
                job["error"] = "disk low"
                print("[autodl] disk low — skipping", fname)
                return job
            await _download_fresh(stream, path, job, series, ep, q)
            size = os.path.getsize(path)
        else:
            size = getattr(getattr(vps_sent, "file", None),
                           "size", 0) or 0
            print(f"[autodl] VPS archived {series} Ep {ep} [{q}] → "
                  f"{db_ids[0]} msg {vps_sent.id} "
                  f"({human_size(size)})")
        job["phase"] = "upload"

        async def up_cb(sent_bytes, total_bytes):
            if job["cancel"]:          # abort mid-upload, not just download
                raise JobCancelled()
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
        if vps_sent is not None:
            primary = vps_sent
            refs.append({
                "channel": int(vps_sent.chat_id),
                "msg_id": vps_sent.id,
                "link": await file_link(int(vps_sent.chat_id),
                                        vps_sent.id),
                "doc": _doc_ref(vps_sent.document
                                or vps_sent.video)})
        last_exc = None
        for dbc in (db_ids[1:] if vps_sent else db_ids):
            if job.get("cancel"):
                raise JobCancelled()
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
                    last_exc = f"FloodWait {fw.seconds}s"
                    core_mod.flood_mark(int(dbc), fw.seconds)
                    print(f"[autodl] archive floodwait {fw.seconds}s "
                          f"to {dbc} — sleeping it out")
                    await asyncio.sleep(min(fw.seconds, 900) + 5)
                except Exception as exc:
                    last_exc = repr(exc)[:160]
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
                            f"DB channel(s)"
                            + (f" — last error: {last_exc}"
                               if last_exc else ""))
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
        if _fb_lock:
            LOAD.release()
        try:
            os.remove(path)
        except Exception:
            pass
        if CURRENT2 is job:
            CURRENT2 = None
        elif CURRENT is job:
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


def _locks_for(executor):
    """Lane lock(s) an executor needs. 'auto' may run files on BOTH
    machines (parallel mode / VPS fallbacks), so it takes both —
    always V-then-K, the one global acquisition order (run_job's
    fallback wants K while holding V; any other order could
    deadlock)."""
    if executor == "koyeb":
        return (LOAD,)
    if executor == "vps":
        return (LOAD_V,)
    return (LOAD_V, LOAD)


async def start_batch(fresh, force=False, executor="auto"):
    """Run one batch under ITS OWN machine's lane lock (watcher,
    /checknew, /searchad). force=True re-downloads even archived
    qualities. executor: auto | koyeb | vps (per-batch pick).
    An explicit ☁️/🖥 pick only holds THAT lane — when the picked
    server is idle the batch starts IMMEDIATELY, even while the
    other machine is still busy with its own batch."""
    locks = _locks_for(executor)
    for lk in locks:
        await lk.acquire()
    try:
        return await run_batch(fresh, force=force,
                               executor=executor, locks=locks)
    finally:
        for lk in reversed(locks):
            lk.release()


async def run_batch(fresh, force=False, executor="auto", locks=()):
    """Process every fresh episode through the job queue, with a
    live progress panel DM'd to the owner. 'locks' = the lane locks
    the CALLER already holds (start_batch) — carried in the batch
    state so run_job never re-acquires a lock this task owns.
    A Koyeb batch and a VPS batch may run CONCURRENTLY: each keeps
    its own counters, and only the FIRST active batch owns/creates
    the live panel (one panel for everything, per design)."""
    groups = group_fresh(fresh)
    if not groups:
        return 0
    if not await store.ensure_mongo(45):
        print("[autodl] WARNING: MongoDB unreachable — archives will "
              "be skipped until it is back")
    await store.set_config("pending_batch",
                           {"ts": time.time(), "groups": len(groups)})
    total = sum(len(todo)
                for g in groups
                for _, todo, _ in g["entries"])
    # no await between the emptiness check and registering — the
    # event loop makes check+register atomic, so exactly one
    # concurrent batch ever owns the panel
    st = {"n": 0, "total": total, "skip": 0, "stop": False,
          "ok": 0, "bad": 0, "can": 0, "locks": tuple(locks),
          "panel": not BATCHES, "defer": [], "seen": set()}
    done = 0
    BATCHES[id(st)] = st
    try:
        # everything from registration on lives INSIDE the try — an
        # exception anywhere must still pop the batch state, or
        # every later batch loses its panel, summary and cleanup
        if st["panel"]:
            msg = await _owner_dm(job_panel(None), buttons=PANEL_BUTTONS)
            if msg is not None:
                await set_live_panel(getattr(msg, "chat_id", None),
                                     getattr(msg, "id", None))
        par = False
        if executor == "auto" and await _parallel_on():
            _st = await core_mod.vps_state()
            par = bool(_st.get("online"))
        if par:
            # 🔀 PARALLEL — two fully independent lanes: each lane
            # OWNS whole animes (or, for a single-anime batch, whole
            # episodes) and runs every quality of them itself.
            # Whoever finishes first starts its next one instantly —
            # no lane ever waits for the other.
            if len(groups) == 1 and len(groups[0]["entries"]) >= 2:
                _g = groups[0]
                _ents = _g["entries"]
                lane_groups = [dict(_g, entries=_ents[0::2]),
                               dict(_g, entries=_ents[1::2])]
            else:
                lane_groups = groups
            lane_a = lane_groups[0::2]
            lane_b = lane_groups[1::2]

            async def _lane(gs, ex):
                d = 0
                for g in gs:
                    if st.get("stop"):
                        print("[autodl] cancel-all — lane stopped")
                        break
                    if await process_group(g, force=force,
                                           executor=ex, st=st):
                        d += 1
                return d
            ra, rb = await asyncio.gather(_lane(lane_a, "koyeb"),
                                          _lane(lane_b, "vps"))
            done += ra + rb
        else:
            for g in groups:
                if st.get("stop"):
                    print("[autodl] cancel-all — batch stopped")
                    break
                if await process_group(g, force=force,
                                       executor=executor, st=st):
                    done += 1
        # ⏭ RETRY PASS — every file skipped during the batch (CDN
        # crawling, cut, timeout) gets one more go now that the queue
        # is empty, so nothing is silently dropped and nothing holds
        # the batch up while it is still working
        defer = list(st.get("defer") or [])
        st["defer"] = []
        if defer:
            st["retried"] = len(defer)
            print(f"[autodl] retry pass: {len(defer)} skipped file(s)")
            for (g, it, epn, s_, dbi, po, ex, _why) in defer:
                if st.get("stop"):
                    break
                try:
                    if not force and await registry_get(
                            g["norm"], epn, s_["quality"]):
                        continue    # another lane already archived it
                except Exception:
                    pass
                await _run_stream(g, it, epn, s_, dbi, po, ex, st,
                                  retry=True)
    finally:
        BATCHES.pop(id(st), None)
        last = not BATCHES   # the last batch standing does cleanup
        ok, bad, can = st["ok"], st["bad"], st["can"]
        if last and PANEL["chat"]:
            cpu, ram, total = container_stats()
            try:
                await app.edit_message(
                    PANEL["chat"], PANEL["id"],
                    f"✅ <b>Batch finished</b>\n\n"
                    f"⬇️ archived: <b>{ok}</b> · ❌ failed: <b>{bad}</b>"
                    f" · 🛑 cancelled: <b>{can}</b>\n"
                    f"📺 posted: <b>{done}</b> episode card(s)\n"
                    + (f"⏭ skipped mid-batch, retried at the end: "
                       f"<b>{st.get('retried', 0)}</b>\n"
                       if st.get("retried") else "")
                    + (f"⚠️ skipped: <b>{st.get('skip', 0)}</b> "
                       f"group(s) — no DB channel / storage offline\n"
                       if st.get("skip") else "")
                    + "\n"
                    f"📊 Container CPU: {cpu:.0f}% of quota · "
                    f"RAM: {ram:.1f} MiB / {total:.1f} MiB",
                    parse_mode="html",
                    buttons=[[Button.inline("🔄 Refresh",
                                            b"jrefresh")]])
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
                # pass the held lane locks down — the nested batch's
                # Koyeb fallbacks must never re-acquire what this
                # task already owns
                await sync_buttons_pass(locks=st["locks"])
            except Exception as exc:
                print("[autodl] syncbutton pass failed:",
                      repr(exc)[:200])
        if last:
            await store.set_config("pending_batch", None)
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
    topped = 0
    for ch in (only if only is not None else await posting_channels()):
        kept = {q: l for q, l in links.items()
                if _has_eng(q) == ch["eng"]}
        if not kept:
            print(f"[autodl] card skip: {series} Ep {ep} channel "
                  f"{ch['id']} tag={ch['tag'] or '-'} needs "
                  f"{'eng' if ch['eng'] else 'indo'} qualities, "
                  f"links have {sorted(links)}")
            continue
        quals = sorted(kept, key=quality_order, reverse=True)
        cap_text, syn_off, syn_len = post_caption(
            title=dual_title(series, meta) if ch["eng"] else series,
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
            alive = False
            if doc.get("msg_id"):
                try:
                    alive = bool(await app.get_messages(
                        ch["id"], ids=doc["msg_id"]))
                except Exception:
                    alive = False
            if alive:
                # card already there: top up NEW quality buttons
                # (e.g. the batch restarted mid-episode and the rest
                #  of the qualities landed later — it must grow)
                new_q = set(quals) - set(doc.get("quals") or [])
                if new_q:
                    await _apply_card_edit(
                        doc, ch["id"], cap_text,
                        _card_buttons(quals, deeps, doc["_id"]),
                        ents, quals)
                    topped += 1
                    print(f"[autodl] topped up {series} Ep {ep} "
                          f"[{ch['tag'] or 'indo'}] in {ch['id']} "
                          f"+{sorted(new_q)}")
                else:
                    print(f"[autodl] card already complete: "
                          f"{series} Ep {ep} "
                          f"[{ch['tag'] or 'indo'}] in {ch['id']}")
                continue
            # phantom doc: its message is gone from the channel —
            # drop the stale record so a fresh card goes out
            print(f"[autodl] stale posts doc {doc['_id']} (no live "
                  f"message in {ch['id']}) — reposting")
            try:
                await store.col("posts").delete_one(
                    {"_id": doc["_id"]})
            except Exception:
                pass
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
    return posted + topped


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
                    audio=_audio_lang(item),
                    rating=(f"{item.get('rating')} / 10"
                            if item.get("rating") else ""),
                    genres=" ".join("#" + g.replace(" ", "")
                                    for g in (item.get("genres")
                                              or [])[:5]),
                )
                cap_text, syn_off, syn_len = post_caption(
                    title=dual_title(title, meta) if eng else title,
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


async def _parallel_on() -> bool:
    """/parallel toggle: batches may run one file on Koyeb AND one on
    the VPS at the same time (two different bots = two upload
    lanes). Only meaningful while the worker is online."""
    try:
        return bool(await store.get_config("parallel", False))
    except Exception:
        return False


async def _run_stream(group, item, ep, stream, db_ids, poster,
                      executor="auto", st=None, retry=False):
    """One file of a group: ONE run_job attempt, honest failure
    reporting and lane cleanup. Returns True when it archived.

    A failure is no longer hammered on the spot — that used to burn
    2 × 90 minutes per hopeless file, and the panel showed the SAME
    episode as 'Video 1/35' and then 'Video 2/35' (the loop the owner
    saw). Now the file is DEFERRED: the batch moves to the next video
    immediately and run_batch gives every deferred file one more try
    once the queue is empty."""
    global CURRENT, CURRENT2
    series = group["series"]
    # one stalled file must never kill the whole batch or
    # hold a lane lock forever
    err = None
    job = None
    try:
        job = await asyncio.wait_for(
            run_job(group, item, ep, stream, db_ids, poster,
                    executor=executor, st=st),
            timeout=3600)
    except asyncio.TimeoutError:
        err = "timeout (60 min)"
    except Exception as exc:
        err = repr(exc)[:160]
    # a job can also fail QUIETLY (disk low / archive error inside
    # run_job) — never let that stay silent
    if err is None and job is not None \
            and job.get("status") == "failed":
        err = job.get("error") or "failed"
    if err is not None:
        print(f"[autodl] job failed ({err}): {series} Ep {ep} "
              f"{stream['quality']}")
        tgt = job if job is not None else (CURRENT or CURRENT2)
        if tgt is not None and tgt.get("status") == "running":
            tgt["status"] = "failed"
            tgt["error"] = err or "failed"
        if CURRENT is tgt:
            CURRENT = None
        if CURRENT2 is tgt:
            CURRENT2 = None
        if tgt is not None:
            JOBS.pop(tgt.get("id"), None)  # a dead attempt leaves the
            # registry at once — /cancel_<id> only finds live jobs
        _free_up(900)
    deferred = False
    if err is not None and not retry and st is not None \
            and err != "disk low" and not err.startswith("archive") \
            and not st.get("stop"):
        # ⏭ skip it NOW, retry when the queue is empty: a crawling
        # or cutting CDN must never hold the batch hostage
        st.setdefault("defer", []).append(
            (group, item, ep, stream, db_ids, poster, executor,
             err[:110]))
        deferred = True
        print(f"[autodl] deferring {series} Ep {ep} "
              f"[{stream['quality']}] to the end-of-batch retry")
    if job is not None:
        # per-batch tallies for the summary (the JOBS registry is
        # shared between concurrent batches — never clear it whole)
        if st is not None:
            _s = job.get("status")
            if _s == "done":
                st["ok"] += 1
            elif _s == "failed" and not deferred:
                st["bad"] += 1     # a deferred file is not lost yet
            elif _s == "cancelled":
                st["can"] += 1
        JOBS.pop(job.get("id"), None)
    if err and not deferred:  # never silent: tell the owner
        print("[autodl] job FAILED:", series, "Ep", ep,
              stream["quality"], err)
        asyncio.ensure_future(_owner_dm(
            f"❌ <b>Job failed</b> — {series} Ep {ep} "
            f"[{stream['quality']}]: {err}"))
    await asyncio.sleep(2)  # smooth the batch send rate
    return err is None


async def process_group(group, force=False, executor="auto",
                        st=None) -> bool:
    """Download every missing quality of every new episode (one by
    one), archive in the DB channel, then ONE designed poster post
    per series with quality buttons covering the episode range."""
    global CURRENT
    if st is None:
        st = {"skip": 0, "stop": False}
    db_ids = await store.db_channel_ids()
    if not db_ids:  # maybe we are mid-blip on local JSON
        await store.ensure_mongo(15)
        db_ids = await store.db_channel_ids()
    if not db_ids:
        st["skip"] = st.get("skip", 0) + 1
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
        if st.get("stop"):
            print("[autodl] cancel-all — group ends early")
            break
        _, ep = parse_api_title(item.get("title", ""))
        pend = []
        seen = st.setdefault("seen", set())
        for stream in todo:
            q = stream["quality"]
            if not force and await registry_get(norm, ep, q):
                continue  # another batch already archived this file
            k = (norm, str(ep), base_quality(q), _has_eng(q))
            if k in seen:
                # the API can list one episode twice — never run the
                # same file twice inside one batch
                print(f"[autodl] duplicate {series} Ep {ep} [{q}] "
                      "skipped (already queued in this batch)")
                continue
            pend.append(stream)
        for stream in pend:
            if st.get("stop"):
                break
            q = stream["quality"]
            seen.add((norm, str(ep), base_quality(q), _has_eng(q)))
            await _run_stream(group, item, ep, stream, db_ids,
                              poster, executor, st)

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
        # BOTH checks: the episode number AND the name (fuzzy-
        # tolerant, so a spelling variant never rejects a real hit)
        ep_ok = e2 == int(ep) or (e2 is None and section == "donghua")
        if not ep_ok:
            continue
        na, ns = normalize_key(s), normalize_key(series)
        if not (na == ns or na in ns or ns in na
                or fuzzy_ratio(s, series) >= 0.8):
            continue
        item.setdefault("section", section)
        return item
    return None


async def refresh_stream_urls(series, ep, quality, old_url=""):
    """EVERY current url the API lists for this exact series/ep/quality
    ('-eng' flavours included), the ones that DIFFER from the dead url
    first. CDN links expire constantly and the API can list several
    hosts for one quality, so a retry must not be limited to whichever
    url happened to come back first."""
    label = (quality or "").lower()
    eng = _has_eng(label)
    base = label.split("-")[0]
    if base.isdigit():
        base += "p"          # "360-eng" means "360p-eng"
    sections = ("anime", "donghua") if eng else ("donghua", "anime")
    diff, same = [], []
    for section in sections:
        try:
            item = await _search_episode(section, series, ep)
        except Exception as exc:
            print("[autodl] url-refresh search failed:",
                  repr(exc)[:120])
            continue
        if not item:
            continue
        for s in item.get("streams", []):
            q2 = (s.get("quality") or "").lower()
            b2 = q2.split("-")[0]
            if b2.isdigit():
                b2 += "p"
            if b2 != base or eng != _has_eng(q2):
                continue
            url = s.get("url") or ""
            if not url:
                continue
            (same if url == old_url else diff).append(url)
        if diff:
            break
    out = []
    for u in diff + same:
        if u not in out:
            out.append(u)
    if out:
        print(f"[autodl] refreshed {quality} urls for {series} Ep {ep}"
              f" — {len(out)} candidate(s)")
    return out


async def refresh_stream_url(series, ep, quality, old_url=""):
    """The API's CDN links expire (HTTP 404 / cut transfers) while a
    long queue works through its files. Re-query the search API and
    return the CURRENT best url for this exact series/ep/quality, or
    None when the API no longer lists it."""
    urls = await refresh_stream_urls(series, ep, quality, old_url)
    return urls[0] if urls else None


async def _download_fresh(stream, path, job, series, ep, q):
    """download_stream (which already walks every route of one url:
    proxy → direct seedbox, resuming each cut) PLUS fresh urls from
    the API when a url is simply dead/expired — up to two of them,
    each with its own full route+resume walk. Cancels propagate
    instantly and the LAST real error is what surfaces."""
    err = None
    tried = [stream["url"]]
    try:
        await download_stream(stream["url"], path, job)
        return
    except JobCancelled:
        raise
    except Exception as exc:
        err = exc
        print(f"[autodl] download failed ({repr(exc)[:150]}) — asking "
              f"the API for fresh {q} urls")
    fresh_urls = []
    try:
        fresh_urls = await refresh_stream_urls(series, ep, q,
                                               stream["url"])
    except Exception as exc:
        print("[autodl] url refresh failed:", repr(exc)[:120])
    for fresh in fresh_urls[:2]:
        if fresh in tried:
            continue
        tried.append(fresh)
        stream["url"] = fresh
        try:
            os.remove(path)   # another host = maybe another encode:
        except Exception:     # start clean, never append to bytes
            pass              # that came from a different server
        print(f"[autodl] fresh url for {series} Ep {ep} [{q}] — "
              f"retrying download")
        try:
            await download_stream(fresh, path, job)
            return
        except JobCancelled:
            raise
        except Exception as exc:
            err = exc
            print(f"[autodl] fresh url failed too "
                  f"({repr(exc)[:150]})")
    if err is not None:
        raise err
    raise RuntimeError(f"no working CDN url for {series} Ep {ep} "
                       f"[{q}] ({len(tried)} tried)")


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


async def sync_buttons_pass(notify=True, locks=()):
    """The /syncbutton sweep (call with the lane locks held, and
    pass them in 'locks' so the nested batch never re-acquires
    them): 1) top up every
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
        # lane locks already held by the caller
        downloaded = await run_batch(entries, locks=locks)
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
            if not busy():
                n = await _eng_pending_pass(st)
                if n:
                    print(f"[autodl] eng-pending pass finished "
                          f"{n} episode(s)")
        except Exception as exc:
            print("[autodl] eng-pending loop error:", exc)
        await asyncio.sleep(15 * 60)


async def autodl_loop():
    """Background watcher — polls the API for new episodes.
    This loop must NEVER die and never fail silently: a dead
    watcher looks exactly like 'the bot stopped downloading' while
    /status stays healthy — so every pass stamps its result into
    the 'watch' config (rendered by /status) and real failures
    reach the owner's DM (hourly-gated)."""
    await asyncio.sleep(30)  # let the bot settle first
    running = False
    while True:
        interval = 20
        try:
            st = await get_settings()
            interval = st.get("interval", 20)
            if st["enabled"] and not running:
                running = True
                try:
                    fresh = await check_new()
                    if fresh:
                        await _watch_mark(len(fresh))
                        if busy():
                            print("[autodl] batch already running — "
                                  "watcher skips re-queueing")
                        else:
                            print(f"[autodl] {len(fresh)} episode(s) "
                                  f"missing from the DB — queueing")
                            await start_batch(fresh)
                    elif API_ERR.get("ts"):
                        # every source section failed — outage, not
                        # 'nothing new'; say so in status AND DM
                        await _watch_mark(0, "source API unreachable: "
                                          + API_ERR.get("err", ""))
                        await _loop_warn("source API unreachable — "
                                         + API_ERR.get("err", ""))
                    else:
                        await _watch_mark(0)
                finally:
                    running = False
        except Exception as exc:
            print("[autodl] loop error:", repr(exc)[:200])
            await _watch_mark(None, repr(exc)[:200])
            await _loop_warn(f"watcher pass crashed: {exc!r}")
        try:  # re-read the interval — but NEVER die on it (an
            interval = (await get_settings()).get(   # unguarded read
                "interval", interval)                # here used to be
        except Exception:                            # able to kill the
            pass                                     # watcher forever)
        await asyncio.sleep(max(60, interval) * 60)
