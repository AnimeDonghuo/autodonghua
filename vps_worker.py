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
import asyncio
import base64
import hashlib
import json
import os
import random
import sys
import threading
import time
import re
import urllib.error
import urllib.parse
import urllib.request
from urllib.parse import quote, unquote


BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/124.0.0.0 Safari/537.36")

def _self_build():
    """Content-hash of THIS file, computed at boot. Koyeb hashes
    its repo copy the same way, so ANY change to this file — even
    one nobody remembered to stamp — auto-triggers the watchdog
    update within ~5 minutes of the bot deploying. There is no
    manual version to bump and nothing to forget."""
    try:
        with open(os.path.abspath(__file__), "rb") as fh:
            return hashlib.md5(fh.read()).hexdigest()[:10]
    except Exception:
        return "0"


WORKER_BUILD = _self_build()   # reported in every beat's caps

HERE = os.path.dirname(os.path.abspath(__file__))
CFG_PATH = os.path.join(HERE, "vps_worker.json")
DL = os.path.join(HERE, "dl")

CFG = {}
MIN_VIDEO_BYTES = 1024 * 1024   # anything smaller is a CDN error
                                # page, never an episode video
SEG_PARTS = 4               # parallel Range connections per file
SEG_MIN_FILE = 12 << 20     # below this, one connection is plenty
MIN_SPEED = 64 * 1024       # B/s — under this the CDN is hopeless
SPEED_GRACE = 60            # seconds measured before judging it
MAX_ETA = 45 * 60           # projected finish beyond this = skip it
SEG_SUFFIX = ".seg"         # preallocated in-progress file
STATE_SUFFIX = ".dlpart"    # its resume sidecar (json)
_PROG = {"last": 0.0}
_COUNT = {"bytes": 0, "t0": time.time()}
_ACTIVE = {"job": None, "phase": "", "total": 0}   # the beat
#                                                    # thread's feed


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
    """MEMORY-ONLY hot-path telemetry. This used to fire a blocking
    urllib POST (Koyeb round-trip + two Mongo writes) every 3 s from
    INSIDE the download loop and the upload callback — stalling the
    very transfer it was reporting on and eating tens of percent of
    throughput. The beat thread ships these numbers every ~5 s now;
    a dict update here costs microseconds."""
    _ACTIVE["job"] = job_id
    _ACTIVE["phase"] = phase
    _ACTIVE["total"] = int(total or 0)
    _COUNT["bytes"] = int(done or 0)


API_ROOT = "https://api.parciv.workers.dev"
MAX_RESUME = 400    # one resume per CDN cut — a host that drops the
                    # connection every ~3 MB needs ~120 of them for a
                    # 359 MB episode, so the cap must be way above
                    # any real file-size / cut-size ratio
STALL_LIMIT = 3     # attempts in a row adding ZERO bytes = dead link
ERROR_CTYPES = ("text/html", "application/json", "text/plain",
                "text/xml", "application/xml")


class CdnError(Exception):
    """A failure that resuming cannot fix on THIS url (dead link,
    error page, HTTP status, a server that throws Range away). The
    caller moves on to the next route to the very same file."""


def _host_of(url):
    """The host of a url — a failure message should name the SERVER
    that misbehaved, not burn its characters on the long proxy link."""
    m = re.match(r"https?://([^/]+)", url or "")
    return m.group(1) if m else str(url)[:40]


def stream_candidates(url):
    """Every route to this one file, best first.

    The API hands out Cloudflare-Worker PROXY links
    (.../anime/stream?url=<encoded seedbox url>). A Worker cannot
    forward the Range header (it is on Cloudflare's forbidden-header
    list, and the cache drops it on a miss), so when that proxy cuts a
    transfer after a few megabytes it serves the SAME first bytes
    forever — that is the 'truncated download: 3062823/376614938 B'
    job failure that kept killing 720p. The decoded seedbox url is
    candidate 2: the file server itself answers Range with 206, so the
    download can be CONTINUED instead of restarted from zero."""
    out = [url]
    m = re.search(r"/(?:anime|donghua)/stream\?url=([^&\s]+)",
                  url or "")
    if m:
        try:
            direct = unquote(m.group(1))
        except Exception:
            direct = ""
        if direct.startswith("http") and direct not in out:
            out.append(direct)
    elif str(url).startswith("http"):
        try:
            for sec in ("anime", "donghua"):
                p = "%s/%s/stream?url=%s" % (API_ROOT, sec,
                                             quote(url, safe=""))
                if p not in out:
                    out.append(p)
        except Exception:
            pass
    return out[:3]


def _referer_for(url):
    """Referer for the 403 retry: the CDN's own root for a direct
    seedbox url, the API root for a proxy url."""
    m = re.match(r"(https?://[^/]+)/", url or "")
    if m and "parciv" not in m.group(1):
        return m.group(1) + "/"
    return API_ROOT + "/"


def _dl_request(url, referer=False, have=0, end=0):
    """Browser headers — episode CDNs answer the bare
    Python-urllib User-Agent with 403 Forbidden — plus the Range
    header that turns a cut transfer into a continuable one.
    ``end`` bounds the range (one parallel part's slice)."""
    h = {"User-Agent": BROWSER_UA, "Accept": "*/*",
         "Accept-Language": "en-US,en;q=0.9"}
    if referer:
        h["Referer"] = _referer_for(url)
    if end:
        # a bounded slice — one parallel part. `have` may legitimately
        # be 0 (the first part), so `end` is what decides the form
        h["Range"] = "bytes=%d-%d" % (have, end - 1)
    elif have:
        h["Range"] = "bytes=%d-" % have
    return urllib.request.Request(url, headers=h)


def sweep(max_age_min=10):
    """Disk hygiene — the VPS must outlive months of unattended
    runs. Sweeps only ever run BETWEEN jobs (boot, and the gap before
    the next claim), so NOTHING in dl/ is being written: every file
    older than max_age_min is an orphan from a killed/crashed run and
    goes — including the parallel downloader's <name>.seg /
    <name>.dlpart work files. Boot calls sweep(2) (a 2-minute floor
    so even an accidental double-start cannot wipe a file another
    worker is actively writing). worker.log is capped too (the shell
    redirect appends, so truncating is safe — the fd keeps writing at
    the new EOF)."""
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
    """Fetch + validate one episode file. Every route (proxy link,
    then the direct seedbox url behind it) gets a FULL resume loop,
    and the file is only returned when it is COMPLETE and big enough
    to be a video — a truncated transfer is one more request now, not
    a failed job."""
    t0 = time.time()
    errs = []
    best = 0
    for cand in stream_candidates(url):
        try:
            done = _download_route(cand, path, job_id)
            secs = max(0.25, time.time() - t0)
            return done, done / secs / 1048576   # bytes, MB/s
        except Exception as exc:
            have = os.path.getsize(path) if os.path.exists(path) else 0
            if have > best:
                best = have
            errs.append("%s: %s" % (_host_of(cand), repr(exc)[:120]))
            log("download route failed:", cand[:70], "-",
                repr(exc)[:140])
            for gone in (path, path + SEG_SUFFIX,
                         path + STATE_SUFFIX):
                try:
                    os.remove(gone)   # the next route can be a
                except Exception:     # different host/encode — never
                    pass              # continue somebody else's bytes
    raise RuntimeError("every CDN route failed (best %d MB on disk): %s"
                       % (best // 1048576, " | ".join(errs)[:220]))


def _seg_paths(path):
    return path + SEG_SUFFIX, path + STATE_SUFFIX


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


def _load_state(state_path, url, total, parts):
    """Resume sidecar — trusted only for the SAME url and size. A pos
    recorded BEHIND the bytes actually written is safe: re-fetching a
    range writes identical bytes back at the identical offset."""
    try:
        with open(state_path, "r") as fh:
            st = json.load(fh)
    except Exception:
        return None
    if st.get("url") != url or int(st.get("total") or -1) != int(total):
        return None
    saved = st.get("parts") or []
    if len(saved) != len(parts):
        return None
    for i in range(len(parts)):
        try:
            if int(saved[i][0]) != parts[i][0]:
                return None
            pos = int(saved[i][1])
        except Exception:
            return None
        parts[i][2] = max(parts[i][0], min(parts[i][1], pos))
    return parts


def _save_state(state_path, url, total, parts):
    tmp = state_path + ".tmp"
    try:
        with open(tmp, "w") as fh:
            json.dump({"url": url, "total": int(total),
                       "parts": [[p[0], p[2]] for p in parts]}, fh)
        os.replace(tmp, state_path)
    except Exception:
        pass


class SpeedWatch(object):
    """Aggregate throughput + a verdict.

    A CDN that dribbles 1 KB/s is worse than one that refuses: it
    held the worker (and the Koyeb lane waiting on it) for hours
    while the rest of the batch queued up. After SPEED_GRACE seconds
    a route under MIN_SPEED — or one whose projected finish is past
    MAX_ETA — is abandoned so the job fails fast, visibly, and the
    batch moves on to the next video."""

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
        el = time.time() - self.t0
        if el < SPEED_GRACE:
            return None
        rate = self.rate or (max(0, int(done) - self.d0)
                             / max(0.001, el))
        if rate < 1024:
            return "CDN sent nothing for %ds" % int(el)
        if rate < MIN_SPEED:
            return ("CDN crawling at %d KB/s (floor %d KB/s)"
                    % (rate // 1024, MIN_SPEED // 1024))
        if self.total:
            eta = (self.total - int(done)) / rate
            if eta > MAX_ETA:
                return ("CDN too slow — %d KB/s, ETA %d min (cap %d "
                        "min)" % (rate // 1024, eta // 60,
                                  MAX_ETA // 60))
        return None


def _probe(url):
    """One 1-byte Range request: does this route HONOUR Range and how
    big is the file? (total, ranges_ok, referer, error). The
    Cloudflare-Worker proxy answers 200 — Range is on its forbidden
    header list — so that route can only be walked sequentially; the
    seedbox behind it answers 206 and can be pulled SEG_PARTS
    connections at a time."""
    err = "probe failed"
    for referer in (False, True):
        try:
            req = _dl_request(url, referer, 0)
            req.add_header("Range", "bytes=0-0")
            r = urllib.request.urlopen(req, timeout=90)
        except urllib.error.HTTPError as ex:
            if ex.code in (401, 403, 451) and not referer:
                err = "HTTP %s" % ex.code
                continue
            ctype = ""
            rng = ""
            try:
                ctype = (ex.headers.get("Content-Type") or "").lower()
                rng = ex.headers.get("Content-Range") or ""
            except Exception:
                pass
            if ctype.startswith(ERROR_CTYPES):
                return 0, False, referer, (
                    "CDN served an error page (%s), not a video "
                    "— the link is dead" % ctype)
            tail = rng.split("/")[-1] if "/" in rng else ""
            if ex.code == 416:
                return (int(tail) if tail.isdigit() else 0), \
                    False, referer, None
            return 0, False, referer, "HTTP %s" % ex.code
        except Exception as ex:
            err = repr(ex)[:110]
            continue
        try:
            ctype = (r.headers.get("Content-Type") or "").lower()
            rng = r.headers.get("Content-Range") or ""
            code = getattr(r, "status", 0) or r.getcode()
            tail = rng.split("/")[-1] if "/" in rng else ""
            if ctype.startswith(ERROR_CTYPES):
                return 0, False, referer, (
                    "CDN served an error page (%s), not a video "
                    "— the link is dead" % ctype)
            if code == 206:
                return (int(tail) if tail.isdigit() else 0), \
                    True, referer, None
            if code == 200:
                return int(r.headers.get("Content-Length") or 0), \
                    False, referer, None
            if code == 416:
                return (int(tail) if tail.isdigit() else 0), \
                    False, referer, None
            return 0, False, referer, "HTTP %s" % code
        finally:
            try:
                r.close()
            except Exception:
                pass
    return 0, False, False, err


def _download_route(url, path, job_id):
    """ONE candidate url: probe it, then take the fastest path it
    supports — SEG_PARTS parallel Range requests for a big file on a
    Range-honouring host, otherwise a sequential walk that resumes
    after every cut. Both validate, both abandon a crawling route."""
    total, ranges, referer, why = _probe(url)
    if why:
        raise CdnError(why)
    have = os.path.getsize(path) if os.path.exists(path) else 0
    if total and have >= total and have >= MIN_VIDEO_BYTES:
        _COUNT["bytes"] = have
        progress(job_id, "download", have, have)
        return have                 # already on disk, complete
    if ranges and total >= SEG_MIN_FILE:
        return _download_parallel(url, path, job_id, total, referer)
    return _download_sequential(url, path, job_id, referer)


def _download_parallel(url, path, job_id, total, referer=False):
    """SEG_PARTS concurrent Range requests into ONE preallocated file.

    This is how leech bots pull several MB/s off a CDN that gives a
    single connection a trickle (or cuts it after a few MB): four
    streams in flight, each resuming ITSELF, so a cut costs one part
    a few megabytes instead of failing the job. The in-progress file
    is ``<path>.seg`` with a JSON sidecar of part offsets; only when
    EVERY part is complete and the size matches is it renamed onto
    ``path`` — a half-built file can never be uploaded as a video."""
    from concurrent.futures import ThreadPoolExecutor
    seg, state = _seg_paths(path)
    parts = _partition(total, SEG_PARTS)
    have = os.path.getsize(path) if os.path.exists(path) else 0
    lock = threading.Lock()
    if _load_state(state, url, total, parts) is None:
        try:
            if have:
                os.replace(path, seg)   # a dense prefix is REAL data
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

    def present():
        return sum(p[2] - p[0] for p in parts)

    _COUNT["bytes"] = present()
    progress(job_id, "download", present(), total)
    watch = SpeedWatch(total, present())
    stop = {"err": None}
    fd = os.open(seg, os.O_RDWR)
    saved = [time.time()]
    try:
        def part(i):
            nonlocal referer      # a 403 on any part arms it for all
            stalls = 0
            resumes = 0
            while True:
                if stop["err"] is not None:
                    return
                start, end, pos = parts[i]
                if pos >= end:
                    return
                got, cut = 0, None
                r = None
                try:
                    try:
                        r = urllib.request.urlopen(
                            _dl_request(url, referer, pos, end),
                            timeout=180)
                    except urllib.error.HTTPError as ex:
                        if ex.code in (401, 403, 451) and not referer:
                            referer = True
                            continue
                        raise CdnError("part %d: HTTP %s"
                                       % (i + 1, ex.code))
                    code = getattr(r, "status", 0) or r.getcode()
                    if code != 206:
                        raise CdnError("part %d: HTTP %s for Range "
                                       "bytes=%d-%d"
                                       % (i + 1, code, pos, end - 1))
                    ctype = (r.headers.get("Content-Type")
                             or "").lower()
                    if ctype.startswith(ERROR_CTYPES):
                        raise CdnError("CDN served an error page (%s) "
                                       "mid-file — the link died"
                                       % ctype)
                    while True:
                        if stop["err"] is not None:
                            return
                        try:
                            chunk = r.read(1 << 22)
                        except Exception as ex:
                            partial = getattr(ex, "partial", b"") or b""
                            cut = ex
                            if partial:
                                chunk = partial
                            else:
                                break
                        if not chunk:
                            break
                        if len(chunk) > end - pos:
                            chunk = chunk[:end - pos]
                        if not chunk:
                            break
                        os.pwrite(fd, chunk, pos)
                        pos += len(chunk)
                        got += len(chunk)
                        parts[i][2] = pos
                        d = present()
                        _COUNT["bytes"] = d
                        progress(job_id, "download", d, total)
                        rate = watch.update(d)
                        bad = watch.verdict(d)
                        if bad:
                            stop["err"] = bad
                            raise CdnError(bad)
                        now = time.time()
                        if now - saved[0] >= 3.0:
                            with lock:
                                if now - saved[0] >= 3.0:
                                    saved[0] = now
                                    _save_state(state, url, total,
                                                parts)
                        if cut is not None:
                            break
                except CdnError:
                    raise
                except Exception as ex:
                    cut = ex
                finally:
                    if r is not None:
                        try:
                            r.close()
                        except Exception:
                            pass
                if parts[i][2] >= end:
                    return
                if got:
                    stalls = 0
                    resumes += 1
                    if resumes > MAX_RESUME:
                        raise CdnError("part %d: CDN cut it %d times, "
                                       "still %d/%d B"
                                       % (i + 1, resumes, parts[i][2],
                                          end))
                    log("part", i + 1, "resume", resumes, "-",
                        parts[i][2], "/", end, "B")
                    time.sleep(0.15)
                    continue
                stalls += 1
                if stalls >= STALL_LIMIT:
                    raise CdnError("part %d: no progress at %d/%d B "
                                   "(%s)" % (i + 1, parts[i][2], end,
                                             repr(cut)[:70] if cut
                                             else "EOF"))
                time.sleep(0.8 * stalls)

        ex_pool = ThreadPoolExecutor(max_workers=len(parts))
        try:
            futs = [ex_pool.submit(part, i) for i in range(len(parts))]
            errs = []
            for f in futs:
                try:
                    f.result()
                except Exception as exc:
                    errs.append(exc)
                    stop["err"] = stop["err"] or repr(exc)[:120]
            if errs:
                raise errs[0]
        finally:
            ex_pool.shutdown(wait=True)
        _save_state(state, url, total, parts)
        got_total = present()
        if got_total < total or os.path.getsize(seg) != total:
            raise CdnError("parallel download incomplete: %d/%d B"
                           % (got_total, total))
    finally:
        try:
            os.close(fd)
        except Exception:
            pass
    if total < MIN_VIDEO_BYTES:
        raise CdnError("file is only %d B — no episode is that small"
                       % total)
    try:
        os.remove(state)
    except Exception:
        pass
    os.replace(seg, path)          # only a COMPLETE file lands here
    _COUNT["bytes"] = total
    progress(job_id, "download", total, total)
    log("parallel download done:", len(parts), "parts,", total, "B from",
        _host_of(url))
    return total


def _download_sequential(url, path, job_id, referer=False):
    """A route that ignores Range (the Cloudflare proxy) or a small
    file: one connection, resumed after every cut until complete."""
    import http.client
    resumes = 0
    stalls = 0
    watch = None
    while True:
        have = os.path.getsize(path) if os.path.exists(path) else 0
        done, total, cut, ignored = have, 0, None, False
        r = None
        try:
            try:
                r = urllib.request.urlopen(
                    _dl_request(url, referer, have), timeout=180)
            except urllib.error.HTTPError as ex:
                if ex.code in (401, 403, 451) and not referer:
                    log("download HTTP", ex.code,
                        "— retrying with referer headers")
                    referer = True
                    continue
                if ex.code == 416:
                    rng = ""
                    try:
                        rng = ex.headers.get("Content-Range") or ""
                    except Exception:
                        rng = ""
                    tot = int(rng.split("/")[-1]) if "/" in rng else 0
                    if tot and have >= tot and have >= MIN_VIDEO_BYTES:
                        _COUNT["bytes"] = have
                        progress(job_id, "download", have, have)
                        return have
                    raise CdnError("HTTP 416 with %d B on disk "
                                   "(server size %d B)" % (have, tot))
                raise CdnError("HTTP %s" % ex.code)
            code = getattr(r, "status", 0) or r.getcode()
            # dead CDN links (they expire constantly) often answer
            # HTTP 200 with a tiny text/html error PAGE — uploading
            # that garbage poisoned the DB channel + episode cards,
            # so every byte is validated BEFORE it can be archived
            ctype = (r.headers.get("Content-Type") or "").lower()
            if ctype.startswith(ERROR_CTYPES):
                raise CdnError(
                    "CDN served an error page (%s), not a video "
                    "— the link is dead" % ctype)
            if code == 206 and have:
                mode = "ab"       # partial content: APPEND
                rng = r.headers.get("Content-Range") or ""
                total = int(rng.split("/")[-1]) if "/" in rng else 0
            elif code == 200:
                mode, done = "wb", 0
                total = int(r.headers.get("Content-Length") or 0)
                ignored = bool(have)   # our Range was thrown away
            else:
                raise CdnError("HTTP %s" % code)
            if watch is None and total:
                watch = SpeedWatch(total, done)
            with open(path, mode) as fh:
                while True:
                    try:
                        chunk = r.read(1 << 22)   # 4 MB reads
                    except http.client.IncompleteRead as ex:
                        chunk = getattr(ex, "partial", b"") or b""
                        cut = ex
                        if total and done + len(chunk) > total:
                            chunk = chunk[:max(0, total - done)]
                        if chunk:
                            fh.write(chunk)
                            done += len(chunk)
                        break
                    except Exception as ex:   # reset / stall / EOF
                        cut = ex
                        break
                    if not chunk:
                        # urllib hands back a CLEAN b"" when the
                        # server closes early on a length-encoded
                        # body — that silent EOF was the "only a few
                        # MBs downloaded" mystery
                        break
                    if total and done + len(chunk) > total:
                        # never write past the DECLARED size — an
                        # overshooting body would be archived as a
                        # corrupt "video"
                        chunk = chunk[:max(0, total - done)]
                    if not chunk:
                        break
                    fh.write(chunk)
                    done += len(chunk)
                    _COUNT["bytes"] = done
                    progress(job_id, "download", done, total)
                    if watch is not None:
                        watch.update(done)
                        bad = watch.verdict(done)
                        if bad:
                            raise CdnError(bad)
        except CdnError:
            raise
        finally:
            if r is not None:
                try:
                    r.close()
                except Exception:
                    pass
        _COUNT["bytes"] = done
        progress(job_id, "download", done, total)
        if total and done >= total:
            if done < MIN_VIDEO_BYTES:
                raise CdnError("file is only %d B — no episode is "
                               "that small" % done)
            return done
        if not total and cut is None:
            if done >= MIN_VIDEO_BYTES:
                return done     # chunked, no length: trust the EOF
            raise CdnError("file is only %d B — no episode is that "
                           "small" % done)
        if ignored:
            raise CdnError(
                "server ignored Range (answered 200 for bytes=%d-) "
                "and cut at %d/%s B — this route can never be "
                "resumed" % (have, done, total or "?"))
        if total and total < MIN_VIDEO_BYTES:
            raise CdnError("server says the whole file is %d B — no "
                           "episode is that small" % total)
        if done > have:
            stalls = 0
            resumes += 1
            if resumes > MAX_RESUME:
                raise CdnError("CDN cut the transfer %d times and "
                               "still only %d/%d B landed"
                               % (resumes, done, total))
            log("resume", resumes, "-", done, "/", total, "B",
                ("(%s)" % repr(cut)[:60]) if cut else "(server EOF)")
            time.sleep(0.25)
            continue
        stalls += 1
        if stalls >= STALL_LIMIT:
            raise CdnError("CDN made no progress %dx at %d/%d B (%s)"
                           % (stalls, done, total,
                              repr(cut)[:80] if cut else "clean EOF"))
        time.sleep(1.0 * stalls)


PART_SIZE = 512 * 1024        # Telegram's maximum upload part
PARTS_IN_FLIGHT = 8           # concurrent saveBigFilePart calls
BIG_FILE_MIN = 10 * 1024 * 1024   # below this Telegram wants the
                                  # small-file path — stock uploader


def upload_fast(client, path, size, job_id):
    """Leech-bot style upload: PARTS_IN_FLIGHT big parts per
    round-trip BATCH instead of one part per round-trip. Telegram
    accepts big-file parts out of order, so N concurrent
    saveBigFilePart requests amortize the network latency N× —
    this is exactly how 6+ MB/s leechers beat single-stream
    uploaders on the same pipe (SEQUENTIAL 512 KB parts cap out at
    ~part-size/RTT ≈ 2–3 MB/s however good the connection is).

    Works on the SYNC telethon client: inside a coroutine running
    on client.loop, the syncified client(request) hands the
    coroutine back instead of blocking, so asyncio.gather pipelines
    the whole batch on the one session. Returns an InputFileBig
    handle for send_file, or None for the stock sequential uploader
    (small file, or any failure — a job is NEVER lost to the fast
    path; half-sent parts simply expire server-side)."""
    if size < BIG_FILE_MIN:
        return None
    from telethon.errors import FloodWaitError
    from telethon.tl.functions.upload import SaveBigFilePartRequest
    from telethon.tl.types import InputFileBig
    file_id = random.randint(-(2 ** 62), 2 ** 62)
    total_parts = (size + PART_SIZE - 1) // PART_SIZE
    loop = client.loop
    next_part = 0
    sent = 0

    def _batch(items):
        async def _go():
            for attempt in (1, 2):
                try:
                    # the syncified client(req) RETURNS the coroutine
                    # while its own loop is running — gather flies
                    # every part of this batch at once
                    await asyncio.gather(*[
                        client(SaveBigFilePartRequest(
                            file_id=file_id, file_part=i,
                            file_total_parts=total_parts, bytes=d))
                        for i, d in items])
                    return
                except FloodWaitError as fw:
                    if attempt == 2:
                        raise
                    wait = min(int(getattr(fw, "seconds", 5) or 5), 60)
                    log(f"job {job_id}: upload floodwait {wait}s — "
                        "sleeping it out")
                    time.sleep(wait + 1)
        loop.run_until_complete(_go())

    with open(path, "rb") as fh:
        while True:
            items = []
            while len(items) < PARTS_IN_FLIGHT:
                data = fh.read(PART_SIZE)
                if not data:
                    break
                items.append((next_part, data))
                next_part += 1
            if not items:
                break
            _batch(items)
            sent += sum(len(d) for _, d in items)
            progress(job_id, "upload", sent, size)
    if next_part != total_parts:      # paranoia — never hand send_file
        return None                   # a half-uploaded file
    return InputFileBig(id=file_id, parts=total_parts,
                        name=os.path.basename(path))


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
        _ACTIVE.update(job=jid, phase="upload", total=size)

        def up_cb(sent_bytes, total_bytes):
            progress(jid, "upload", sent_bytes, total_bytes)

        u0 = time.time()
        up = None
        try:
            # leech-bot speed: 8×512 KB parts in flight per batch
            up = upload_fast(client, path, size, jid)
        except Exception as exc:
            log(f"job {jid}: parallel upload failed ({exc!r}) — "
                "stock uploader takes over")
            up = None
        if up is None:
            # explicit 512 KB parts (Telegram's max) — send_file's
            # default picks 128/256 KB = 4–8× more round-trips
            up = client.upload_file(
                path, part_size_kb=512,
                file_size=os.path.getsize(path),
                progress_callback=up_cb)
            if hasattr(up, "__await__"):   # never a coroutine
                up = client.loop.run_until_complete(up)
        sent = client.send_file(
            job["channel"], file=up,
            caption=job.get("caption") or "",
            thumb=thumb, supports_streaming=True,
            attributes=video_attrs(path, name))
        if hasattr(sent, "__await__"):  # belt & braces: never a coroutine
            sent = client.loop.run_until_complete(sent)
        up_secs = max(0.1, time.time() - u0)
        up_speed = size / up_secs / 1048576
        log(f"job {jid}: uploaded {size} B in {up_secs:.0f}s "
            f"({up_speed:.2f} MB/s)")
        doc = sent.document or sent.video
        # 'speed' feeds /vps's "avg MB/s uploaded" — it used to
        # report the DOWNLOAD speed, so the stat said nothing about
        # upload performance; now it is the real upload throughput
        out = {"msg_id": sent.id, "size": size,
               "speed": round(up_speed, 2),
               "doc": {"id": doc.id,
                       "access_hash": doc.access_hash,
                       "file_reference": base64.b64encode(
                           doc.file_reference or b"").decode()}}
        log(f"job {jid}: archived msg {sent.id}")
        return out
    finally:
        _ACTIVE["job"] = None      # beat thread stops posting prog
        # win, fail, exception or half-downloaded chunk — the file
        # NEVER outlives its job; the VPS disk is the worker's
        # lifeline (boot sweep catches even a hard kill)
        try:
            os.remove(path)
        except Exception:
            pass


def _beat_loop():
    """Heartbeat + telemetry THREAD — the main loop cannot beat
    while run_one blocks for minutes on a download/upload, so a long
    job used to age the beat past Koyeb's 90 s freshness window and
    the worker showed OFFLINE *while it was uploading*. 45 s cadence
    keeps the beat fresh through every phase of every job — and this
    thread is now ALSO the only place that POSTs progress while a
    job runs (~5 s cadence), so the transfer path itself never
    blocks on telemetry again."""
    last_beat = 0.0
    while True:
        now = time.time()
        if now - last_beat >= 45:
            try:
                api("/vps/beat", {"secret": CFG["secret"],
                                  "caps": caps()})
                last_beat = now
            except Exception:
                pass
        try:
            jid = _ACTIVE["job"]
            if jid and _COUNT["bytes"] > 0 and \
                    now - _PROG["last"] >= 5:
                _PROG["last"] = now
                dt = max(0.5, now - _COUNT["t0"])
                api("/vps/progress", {
                    "secret": CFG["secret"], "job": jid,
                    "phase": _ACTIVE["phase"],
                    "done": int(_COUNT["bytes"]),
                    "total": int(_ACTIVE["total"] or 0),
                    "speed": round(
                        (_COUNT["bytes"] / dt) / 1048576, 2)})
        except Exception:
            pass
        time.sleep(1)


def _other_worker_running():
    """PID of ANOTHER live vps_worker.py process, or 0. Pure /proc
    argv walk — no pgrep needed (minimal boxes lack procps, and a
    watchdog that can't see the process starts a SECOND worker
    whose boot sweep deletes this one's in-progress downloads —
    the '720p file silently removed' mystery). Matching rule:
    argv[0] contains python AND one exact argv element is
    [path/]vps_worker.py, so SSH wrapper shells (bash -c '…
    vps_worker.py …') can never count."""
    me = os.getpid()
    try:
        pids = [d for d in os.listdir("/proc") if d.isdigit()]
    except Exception:
        return 0
    for d in pids:
        pid = int(d)
        if pid == me:
            continue
        try:
            with open("/proc/" + d + "/cmdline", "rb") as fh:
                argv = [a for a in fh.read().split(b"\0") if a]
        except Exception:
            continue
        if not argv:
            continue
        argv = [a.decode("utf-8", "replace") for a in argv]
        if "python" not in argv[0]:
            continue
        if any(a == "vps_worker.py" or a.endswith("/vps_worker.py")
               for a in argv[1:]):
            return pid
    return 0


def main():
    global CFG
    if not os.path.exists(CFG_PATH):
        sys.exit("vps_worker.json missing — run /addvps again")
    CFG = json.load(open(CFG_PATH))
    CFG["base"] = (CFG.get("base") or "").strip().rstrip("/")
    if CFG["base"] and "://" not in CFG["base"]:
        CFG["base"] = "https://" + CFG["base"]  # bare host ok
    os.makedirs(DL, exist_ok=True)
    other = _other_worker_running()
    if other:
        # TWO workers = duplicate job claims AND the second one's
        # boot sweep deleting the first one's active downloads —
        # refuse to start instead
        sys.exit("another vps_worker.py is already running (pid "
                 + str(other) + ") — exiting so its downloads stay "
                 "safe; check worker.log for why this was started")
    sweep(2)   # boot tidy: anything in dl/ older than 2 MINUTES is
               # a leftover of a killed run (SIGTERM/reboot never
               # runs cleanup) — the 2-min floor means even an
               # accidental double-start can never wipe a download
               # that is actively being written
    threading.Thread(target=_beat_loop, daemon=True).start()
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
