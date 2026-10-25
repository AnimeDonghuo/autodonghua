#!/usr/bin/env python3
"""AUTO DONGHUA — VPS worker node.

Downloads episode files on your VPS (fast pipe) and archives them
into the DB channel, so the Koyeb instance (slow upload) only does
the light work: reference copies, posters, cards, DMs, registry.

Deployed & supervised by the bot itself over SSH (/addvps).
Talks to the Koyeb instance over HTTPS with the shared secret.
Needs ONLY telethon (+ optional Pillow/hachoir). Config file
vps_worker.json sits next to this file. Files are deleted after
EVERY job — win, fail or exception — and an orphan sweep runs at
boot and between jobs (a kill/reboot mid-download never executes
cleanup, so boot wipes whatever is left); worker.log is capped at
~4 MB. The VPS is built to run for months unattended without its
disk filling up. Live progress is reported so the Koyeb /status
panel shows the job exactly like a local one.
"""
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request


BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/124.0.0.0 Safari/537.36")

WORKER_BUILD = "4"   # bump on EVERY worker change — the watchdog
#                    # auto-updates VPSes whose beat reports an older one

HERE = os.path.dirname(os.path.abspath(__file__))
CFG_PATH = os.path.join(HERE, "vps_worker.json")
DL = os.path.join(HERE, "dl")

CFG = {}
_PROG = {"last": 0.0}
_COUNT = {"bytes": 0, "t0": time.time()}


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def api(path, payload=None, timeout=40):
    req = urllib.request.Request(
        CFG["base"] + path,
        data=json.dumps(payload or {}).encode()
        if payload is not None else None,
        headers={"Content-Type": "application/json",
                 "X-VPS-Secret": CFG["secret"]})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def caps():
    c = {"py": sys.version.split()[0], "build": WORKER_BUILD}
    for mod in ("telethon", "PIL", "hachoir"):
        try:
            __import__(mod)
            c[mod] = True
        except Exception:
            c[mod] = False
    try:
        st = os.statvfs(DL)
        c["disk_mb"] = st.f_bavail * st.f_frsize // 1048576
    except Exception:
        c["disk_mb"] = -1
    return c


def progress(job_id, phase, done, total):
    now = time.time()
    if now - _PROG["last"] < 3:
        return
    _PROG["last"] = now
    dt = max(0.5, now - _COUNT["t0"])
    speed = (_COUNT["bytes"] / dt) / 1048576
    try:
        api("/vps/progress", {"secret": CFG["secret"], "job": job_id,
                              "phase": phase, "done": int(done),
                              "total": int(total or 0),
                              "speed": round(speed, 2)})
    except Exception:
        pass


def _dl_request(url, referer=False):
    """Browser headers — episode CDNs answer the bare
    Python-urllib User-Agent with 403 Forbidden."""
    h = {"User-Agent": BROWSER_UA, "Accept": "*/*",
         "Accept-Language": "en-US,en;q=0.9"}
    if referer:
        h["Referer"] = "https://api.parciv.workers.dev/"
    return urllib.request.Request(url, headers=h)


def sweep(max_age_min=10):
    """Disk hygiene — the VPS must outlive months of unattended
    runs. This script is single-threaded, so between jobs NOTHING
    in dl/ is active: every file older than max_age_min is an
    orphan from a killed/crashed run and goes. Boot calls sweep(0)
    — a SIGTERM or reboot mid-download can't run finally blocks,
    so startup wipes the folder outright. worker.log is capped too
    (the shell redirect appends, so truncating is safe — the fd
    keeps writing at the new EOF)."""
    now = time.time()
    try:
        for f in os.listdir(DL):
            p = os.path.join(DL, f)
            try:
                if os.path.isfile(p) and \
                        now - os.path.getmtime(p) >= max_age_min * 60:
                    os.remove(p)
                    log("sweep: removed orphan", f)
            except Exception:
                pass
    except Exception:
        pass
    try:
        lp = os.path.join(HERE, "worker.log")
        if os.path.getsize(lp) > 4 * 1048576:
            with open(lp, "r+") as fh:
                fh.truncate(0)
            log("sweep: worker.log truncated (>4 MB)")
    except Exception:
        pass


def download(url, path, job_id):
    t0 = time.time()
    done = 0
    r = None
    for referer in (False, True):   # 403 → retry once with a referer
        try:
            r = urllib.request.urlopen(_dl_request(url, referer),
                                       timeout=120)
            break
        except urllib.error.HTTPError as ex:
            if ex.code in (401, 403, 451) and not referer:
                log("download HTTP", ex.code, "— retrying with "
                    "referer headers")
                continue
            raise
    with r, open(path, "wb") as fh:
        total = int(r.headers.get("Content-Length") or 0)
        while True:
            chunk = r.read(1 << 20)
            if not chunk:
                break
            fh.write(chunk)
            done += len(chunk)
            _COUNT["bytes"] = done
            progress(job_id, "download", done, total)
    secs = max(1.0, time.time() - t0)
    return done, done / secs / 1048576  # bytes, MB/s


def video_attrs(path, filename):
    from telethon import utils
    from telethon.tl.types import (DocumentAttributeFilename,
                                   DocumentAttributeVideo)
    va, _ = utils.get_attributes(path, supports_streaming=True)
    return [DocumentAttributeFilename(file_name=filename)] + [
        x for x in va if isinstance(x, DocumentAttributeVideo)]


def ensure_client(client, creds):
    if client:
        return client
    # telethon.SYNC — a plain (non-async) script must NOT use
    # telethon.TelegramClient: its send_file is a coroutine, so
    # 'sent.document' blew up with AttributeError("'coroutine'
    # object has no attribute 'document'"). The sync client runs
    # every method to completion and returns the real Message.
    from telethon.sync import TelegramClient
    client = TelegramClient(os.path.join(HERE, "vpsworker"),
                            creds["api_id"], creds["api_hash"])
    client.start(bot_token=creds["token"])
    log("worker bot session ready")
    return client


def run_one(job, client):
    os.makedirs(DL, exist_ok=True)
    jid = job["_id"]
    name = "".join(c if c.isalnum() or c in "._- " else "_"
                   for c in (job.get("filename") or "video.mp4"))
    path = os.path.join(DL, name)
    try:
        _COUNT.update(bytes=0, t0=time.time())
        log(f"job {jid}: downloading {job['url'][:70]}")
        size, speed = download(job["url"], path, jid)
        thumb = None
        if job.get("thumb_b64"):
            try:
                thumb = base64.b64decode(job["thumb_b64"])
            except Exception:
                thumb = None
        log(f"job {jid}: downloaded {size} B at {speed:.1f} MB/s — "
            f"uploading to {job['channel']}")
        _COUNT.update(bytes=0, t0=time.time())
        up = {"last": 0.0}

        def up_cb(sent_bytes, total_bytes):
            now = time.time()
            if now - up["last"] < 3:
                return
            up["last"] = now
            _COUNT["bytes"] = sent_bytes
            progress(jid, "upload", sent_bytes, total_bytes)

        sent = client.send_file(
            job["channel"], file=path,
            caption=job.get("caption") or "",
            thumb=thumb, supports_streaming=True,
            attributes=video_attrs(path, name),
            progress_callback=up_cb)
        if hasattr(sent, "__await__"):  # belt & braces: never a coroutine
            sent = client.loop.run_until_complete(sent)
        doc = sent.document or sent.video
        out = {"msg_id": sent.id, "size": size,
               "speed": round(speed, 2),
               "doc": {"id": doc.id,
                       "access_hash": doc.access_hash,
                       "file_reference": base64.b64encode(
                           doc.file_reference or b"").decode()}}
        log(f"job {jid}: archived msg {sent.id}")
        return out
    finally:
        # win, fail, exception or half-downloaded chunk — the file
        # NEVER outlives its job; the VPS disk is the worker's
        # lifeline (boot sweep catches even a hard kill)
        try:
            os.remove(path)
        except Exception:
            pass


def main():
    global CFG
    if not os.path.exists(CFG_PATH):
        sys.exit("vps_worker.json missing — run /addvps again")
    CFG = json.load(open(CFG_PATH))
    CFG["base"] = (CFG.get("base") or "").strip().rstrip("/")
    if CFG["base"] and "://" not in CFG["base"]:
        CFG["base"] = "https://" + CFG["base"]  # bare host ok
    os.makedirs(DL, exist_ok=True)
    sweep(0)   # boot wipe: anything in dl/ is a leftover of a
               # killed run (SIGTERM/reboot never runs cleanup)
    client = None
    log(f"worker starting → {CFG['base']}")
    while True:
        sweep()   # between-job orphan check + worker.log cap
        try:
            api("/vps/beat", {"secret": CFG["secret"],
                              "caps": caps()})
        except Exception as exc:
            log("beat failed:", repr(exc)[:120])
            time.sleep(10)
            continue
        try:
            data = api("/vps/job")
        except Exception as exc:
            log("job poll failed:", repr(exc)[:120])
            time.sleep(8)
            continue
        job = data.get("job")
        if not job:
            time.sleep(5)
            continue
        try:
            client = ensure_client(client, data.get("creds") or {})
            result = run_one(job, client)
            api("/vps/report", {"secret": CFG["secret"],
                                "job": job["_id"], "ok": True,
                                "result": result})
        except Exception as exc:
            log("job failed:", repr(exc)[:200])
            try:
                api("/vps/report", {"secret": CFG["secret"],
                                    "job": job["_id"], "ok": False,
                                    "error": repr(exc)[:200]})
            except Exception:
                pass
            time.sleep(5)


if __name__ == "__main__":
    main()
