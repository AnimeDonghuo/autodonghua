"""
Media analysis helpers — extracts quality info from video files.

Backend priority:
  1. `mediainfo` CLI binary          (apt install mediainfo)
  2. `ffprobe` CLI binary            (apt install ffmpeg)
  3. `mediainfo` python package      (pip install mediainfo)
  4. filename hints                   (last resort)
"""
import asyncio
import json
import os
import re
import subprocess


async def analyze(path: str) -> dict:
    for backend in (_cli_mediainfo, _ffprobe, _lib_mediainfo):
        try:
            info = await asyncio.to_thread(backend, path)
            if info and (info.get("width") or info.get("duration_s")):
                return _finalize(info, path)
        except Exception:
            continue
    return _finalize(_filename_hints(path), path)


# ---------------------------------------------------------------------- #
# Backends
# ---------------------------------------------------------------------- #
def _run(cmd):
    proc = subprocess.run(cmd, capture_output=True, timeout=120)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.decode(errors="ignore")[:200])
    return proc.stdout.decode(errors="ignore")


def _cli_mediainfo(path):
    raw = _run(["mediainfo", "--Output=JSON", path])
    data = json.loads(raw)
    tracks = data.get("media", {}).get("track", [])
    info = {}
    for tr in tracks:
        ttype = tr.get("track_type")
        if ttype == "General":
            info.update({
                "format": tr.get("Format"),
                "duration_s": _to_float(tr.get("Duration")),
                "bitrate": _to_int(tr.get("OverallBitRate")),
                "size_bytes": _to_int(tr.get("FileSize")),
            })
        elif ttype == "Video" and "width" not in info:
            info.update({
                "width": _to_int(tr.get("Width")),
                "height": _to_int(tr.get("Height")),
                "fps": _to_float(tr.get("FrameRate")),
                "vcodec": tr.get("Format"),
                "vcodec_lib": tr.get("Encoded_Library"),
                "vprofile": tr.get("Format_Profile"),
                "vbitrate": _to_int(tr.get("BitRate")),
                "pix_fmt": tr.get("PixelFormat"),
            })
        elif ttype == "Audio" and "acodec" not in info:
            info.update({
                "acodec": tr.get("Format"),
                "achannels": tr.get("Channel(s)"),
                "abitrate": _to_int(tr.get("BitRate")),
                "alang": tr.get("Language"),
            })
    return info


def _ffprobe(path):
    raw = _run(["ffprobe", "-v", "quiet", "-print_format", "json",
                "-show_format", "-show_streams", path])
    data = json.loads(raw)
    info = {}
    fmt = data.get("format", {})
    info.update({
        "format": fmt.get("format_name"),
        "duration_s": _to_float(fmt.get("duration")),
        "bitrate": _to_int(fmt.get("bit_rate")),
        "size_bytes": _to_int(fmt.get("size")),
    })
    for st in data.get("streams", []):
        if st.get("codec_type") == "video" and "width" not in info:
            info.update({
                "width": _to_int(st.get("width")),
                "height": _to_int(st.get("height")),
                "fps": _eval_rational(st.get("r_frame_rate")),
                "vcodec": st.get("codec_name"),
                "vprofile": st.get("profile"),
                "vbitrate": _to_int(st.get("bit_rate")),
                "pix_fmt": st.get("pix_fmt"),
            })
        elif st.get("codec_type") == "audio" and "acodec" not in info:
            info.update({
                "acodec": st.get("codec_name"),
                "achannels": _channels(st.get("channels")),
                "abitrate": _to_int(st.get("bit_rate")),
                "alang": st.get("tags", {}).get("language"),
            })
    return info


def _lib_mediainfo(path):
    from mediainfo import MediaInfo  # type: ignore
    mi = MediaInfo.parse(path)
    info = {}
    for tr in mi.tracks:
        if tr.track_type == "General":
            info.update({"format": tr.format, "duration_s": tr.duration,
                         "bitrate": tr.overall_bit_rate, "size_bytes": tr.file_size})
        elif tr.track_type == "Video" and "width" not in info:
            info.update({"width": tr.width, "height": tr.height,
                         "fps": tr.frame_rate, "vcodec": tr.format,
                         "vcodec_lib": tr.encoded_library, "vprofile": tr.format_profile,
                         "vbitrate": tr.bit_rate, "pix_fmt": tr.pixel_format})
        elif tr.track_type == "Audio" and "acodec" not in info:
            info.update({"acodec": tr.format, "achannels": tr.channel_s,
                         "abitrate": tr.bit_rate, "alang": tr.language})
    return info


def _filename_hints(path):
    name = os.path.basename(path)
    info = {"size_bytes": _file_size(path)}
    m = re.search(r"(\d{3,4})[pi]", name, re.I)
    if m:
        h = int(m.group(1))
        info["height"] = h
        info["width"] = {2160: 3840, 1440: 2560, 1080: 1920, 720: 1280, 480: 854}.get(h)
    if re.search(r"hevc|x265|h265", name, re.I):
        info["vcodec"] = "HEVC"
    elif re.search(r"x264|h264|avc", name, re.I):
        info["vcodec"] = "AVC"
    return info


# ---------------------------------------------------------------------- #
# Normalisation helpers
# ---------------------------------------------------------------------- #
def _to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_int(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _eval_rational(v):
    if not v or "/" not in str(v):
        return _to_float(v)
    try:
        num, den = str(v).split("/")
        return float(num) / max(float(den), 0.001)
    except Exception:
        return None


def _channels(v):
    if v is None:
        return None
    try:
        n = int(v)
        return {1: "1.0", 2: "2.0", 6: "5.1", 8: "7.1"}.get(n, str(n))
    except Exception:
        return str(v)


def _file_size(path):
    try:
        return os.path.getsize(path)
    except OSError:
        return None


def _finalize(info: dict, path: str) -> dict:
    info.setdefault("size_bytes", _file_size(path))
    w, h = info.get("width"), info.get("height")
    if h and not w:
        w = {2160: 3840, 1440: 2560, 1080: 1920, 720: 1280, 480: 854}.get(h)
    info["resolution"] = _resolution_label(w, h)
    info["quality_label"] = quality_label(info)
    info["duration_str"] = _duration_str(info.get("duration_s"))
    info["size_str"] = human_size(info.get("size_bytes"))
    return info


def _resolution_label(w, h):
    if not h:
        return None
    if h >= 2160:
        return "2160p (4K)"
    if h >= 1440:
        return "1440p (2K)"
    if h >= 1080:
        return "1080p (FHD)"
    if h >= 720:
        return "720p (HD)"
    if h >= 480:
        return "480p (SD)"
    return f"{h}p"


def quality_label(info: dict) -> str:
    parts = [info.get("resolution") or "Unknown Res"]
    codec = (info.get("vcodec") or "").lower()
    if "hevc" in codec or "x265" in codec or "h265" in codec:
        parts.append("HEVC")
    elif "av1" in codec:
        parts.append("AV1")
    elif "vp9" in codec:
        parts.append("VP9")
    elif "avc" in codec or "x264" in codec or "h264" in codec:
        parts.append("x264")
    else:
        parts.append((info.get("vcodec") or "Video").upper())
    profile = str(info.get("vprofile") or "")
    if "10" in profile or "10 bit" in str(info.get("pix_fmt") or ""):
        parts.append("10-bit")
    return " ".join(parts)


def _duration_str(secs):
    if not secs:
        return None
    m, s = divmod(int(secs), 60)
    if m >= 60:
        h, m = divmod(m, 60)
        return f"{h}h {m}m"
    return f"{m}m {s}s"


def human_size(nbytes):
    if not nbytes:
        return None
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if nbytes < 1024 or unit == "TB":
            return f"{nbytes:.1f} {unit}" if unit != "B" else f"{int(nbytes)} B"
        nbytes /= 1024
    return f"{nbytes:.1f} TB"


def caption_lines(info: dict) -> list:
    lines = []
    if info.get("quality_label"):
        lines.append(f"💠 <b>Quality:</b> {info['quality_label']}")
    if info.get("vcodec"):
        extra = f" • {info['fps']:.0f}fps" if info.get("fps") else ""
        lines.append(f"🎞 <b>Video:</b> {info['vcodec']} • {info.get('resolution') or '?'}{extra}")
    if info.get("acodec"):
        ch = f" • {info['achannels']}ch" if info.get("achannels") else ""
        lines.append(f"🔊 <b>Audio:</b> {info['acodec']}{ch}")
    if info.get("size_str"):
        lines.append(f"💾 <b>Size:</b> {info['size_str']}")
    if info.get("duration_str"):
        lines.append(f"⏱ <b>Duration:</b> {info['duration_str']}")
    return lines
