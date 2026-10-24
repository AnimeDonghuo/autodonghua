#!/usr/bin/env python3
"""AUTO DONGHUA — VPS worker node.

Downloads episode files on your VPS (fast pipe) and archives them
into the DB channel, so the Koyeb instance (slow upload) only does
the light work: reference copies, posters, cards, DMs, registry.

Deployed & supervised by the bot itself over SSH (/addvps).
Talks to the Koyeb instance over HTTPS with the shared secret.
Needs ONLY telethon (+ optional Pillow/hachoir). Config file
vps_worker.json sits next to this file. Files are deleted right
after a successful upload; live progress is reported so the Koyeb
/status panel shows the job exactly like a local one.
"""
import base64
import json
import os
import sys
import time
import urllib.request

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
    c = {"py": sys.version.split()[0]}
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


def download(url, path, job_id):
    t0 = time.time()
    done = 0
    with urllib.request.urlopen(url, timeout=120) as r, \
            open(path, "wb") as fh:
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
    from telethon import TelegramClient
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
        job["channel"], file=path, caption=job.get("caption") or "",
        thumb=thumb, supports_streaming=True,
        attributes=video_attrs(path, name),
        progress_callback=up_cb)
    doc = sent.document or sent.video
    out = {"msg_id": sent.id, "size": size,
           "speed": round(speed, 2),
           "doc": {"id": doc.id, "access_hash": doc.access_hash,
                   "file_reference": base64.b64encode(
                       doc.file_reference or b"").decode()}}
    try:
        os.remove(path)  # VPS disk stays clean
    except Exception:
        pass
    log(f"job {jid}: archived msg {sent.id}")
    return out


def main():
    global CFG
    if not os.path.exists(CFG_PATH):
        sys.exit("vps_worker.json missing — run /addvps again")
    CFG = json.load(open(CFG_PATH))
    os.makedirs(DL, exist_ok=True)
    client = None
    log(f"worker starting → {CFG['base']}")
    while True:
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
