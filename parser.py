"""
Advanced filename parser — identifies anime/donghua title & episode number
from fansub-style release names, e.g.:

  [Elysium] Shrouding the Heavens S02 - 01 [1080p HEVC 10bit] [AAC].mkv
  Sword.Art.Online.S01E07.mp4
  Soul Land Episode 123 (1080p) WEB-DL.mkv
  斗破苍穹 第01话.mp4
"""
import re

QUALITY_RE = re.compile(
    r"\b(2160p|1080p|1080i|720p|480p|360p|240p|4k|uhd|fhd|hd|sd|web-?dl|webrip|bdrip|bluray|"
    r"x264|x265|h264|h265|hevc|avc|av1|vp9|10-?bit|8-?bit|aac|flac|dts|ac3|ddp|ddp\d+|truehd|atmos|dolby|opus|"
    r"multi-?audio|subbed|subs?|dub(bed)?|raw|uncensored|censored|remux|encode|"
    r"yts|yify|rarbg|psa|anoxmous|evol|zoku|judas|ember|kaleido|eclipse|truth|hai"
    r"|vostfr|vff|vfq|vf|vo)\b", re.I)
SEASON_RE = re.compile(r"(?:^|[\s._\-\[\]()])s(\d{1,2})(?:[\s._\-\[\]()]|$)|season\s*(\d+)", re.I)
CHINESE_EP_RE = re.compile(r"第\s*(\d{1,4})\s*[话集回話集]")

EP_PATTERNS = [
    # S01E07 / E07 / EP07 / EP.07 / Episode 07
    re.compile(r"(?:^|[\s._\-\[\]()])(?:s(\d{1,2}))?e(?:p(?:isode)?s?)?(\d{1,3})(?:v\d)?(?=[\s._\-\[\]()]|$)", re.I),
    re.compile(r"(?:^|[\s._\-\[\]()])(?:ep(?:isode)?|episodes?)\s*[:._\-]?\s*(\d{1,3})(?:v\d)?(?=[\s._\-\[\]()]|$)", re.I),
    # [01] (01) — bracketed bare numbers
    re.compile(r"[\[\(](\d{1,3})[\]\)]"),
    # bare " - 01 " style (last resort; picks the LAST plausible
    # number, never one glued to digits like audio "5.1" or
    # codecs like "10bit")
    re.compile(r"(?<!\d)[\s._\-](\d{1,3})(?!\d)(?![Bb][Ii][Tt])"),
]


def parse_filename(name: str) -> dict:
    stem = name
    for ext in (".mkv", ".mp4", ".avi", ".webm", ".mov", ".m4v", ".ts", ".mpd",
                ".wmv", ".flv", ".rmvb", ".3gp"):
        if stem.lower().endswith(ext):
            stem = stem[: -len(ext)]
            break

    season = None
    season_matched = ""
    sm = SEASON_RE.search(stem)
    if sm:
        season = int(sm.group(1) or sm.group(2))
        season_matched = sm.group(0)

    episode = None
    matched = ""
    for i, pat in enumerate(EP_PATTERNS):
        if i == len(EP_PATTERNS) - 1:
            # bare-number pattern: use the LAST plausible match
            ms = [m for m in pat.finditer(stem)
                  if 0 < int(m.group(1)) <= 9999]
            if not ms:
                continue
            m = ms[-1]
        else:
            m = pat.search(stem)
            if not m:
                continue
        groups = [g for g in m.groups() if g is not None]
        if i == 0 and m.group(1):
            season = season or int(m.group(1))
        cand = int(groups[-1])
        if 0 < cand <= 9999:
            episode = cand
            matched = m.group(0)
            break

    cm = CHINESE_EP_RE.search(stem)
    if cm and episode is None:
        episode = int(cm.group(1))
        matched = cm.group(0)

    # Remove episode/season markers & release-group brackets from title
    title = stem
    if matched:
        title = title.replace(matched, " ", 1)
    if season_matched:
        title = title.replace(season_matched, " ", 1)
    title = re.sub(r"\[[^\]]{0,80}\]", " ", title)   # [Group] [Tags]
    title = re.sub(r"\([^\)]{0,80}\)", " ", title)   # (Group)
    title = QUALITY_RE.sub(" ", title)
    title = re.sub(r"\b\d+\.\d+\b", " ", title)      # audio 5.1 / 2.0
    # trailing release group "-GroupName"
    title = re.sub(r"[\s._\-]+[A-Za-z0-9]{2,12}$", " ", title)
    title = title.replace("第", " ").replace("话", " ").replace("集", " ")
    title = re.sub(r"[._\-]+", " ", title)
    title = re.sub(r"\s+", " ", title)
    title = re.sub(r"\s*season\s*$", "", title, flags=re.I).strip(" -_")

    group = ""
    gm = re.search(r"[\[\(]([A-Za-z0-9][A-Za-z0-9 ._-]{1,20})[\]\)]", name)
    if gm:
        group = gm.group(1).strip()

    qh = ""
    qm = re.search(r"(2160p|1080p|1080i|720p|480p|360p|4k)", name, re.I)
    if qm:
        qh = qm.group(1).lower()

    return {
        "title": title or stem.strip(),
        "episode": episode,
        "season": season,
        "group": group,
        "quality_hint": qh,
        "raw": name,
    }


def normalize_key(title: str) -> str:
    """Stable cache key so 'Soul Land', 'soul land', 'SoulLand' share one image."""
    return re.sub(r"[^a-z0-9]+", "", title.lower())


def fuzzy_ratio(a: str, b: str) -> float:
    from difflib import SequenceMatcher
    a, b = normalize_key(a), normalize_key(b)
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def best_match(title: str, candidates: list) -> tuple:
    """Return (candidate, score) with the highest fuzzy match."""
    best, score = None, 0.0
    for cand in candidates:
        s = fuzzy_ratio(title, cand)
        if s > score:
            best, score = cand, s
    return best, score
