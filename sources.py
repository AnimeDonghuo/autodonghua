"""
Multi-source metadata engine for anime / donghua info & images.

Sources (all attempted concurrently, graceful degradation per source):
  1. AniList            — GraphQL, no auth, highly reliable
  2. Jikan (MyAnimeList)— free REST API, great fallback
  3. AnimXin (animexin.dev) — donghua-focused, HTML scrape
  4. LuciferDonghua     — WordPress REST search + page scrape
  5. AniLive            — configurable base URL, WP-JSON / HTML scrape

Each source returns a SourceResult or None.  The aggregator merges them
with a priority order and reports which sources responded.
"""
import asyncio
import html
import re
import urllib.parse
from dataclasses import dataclass, field

import aiohttp

from parser import fuzzy_ratio

TIMEOUT = aiohttp.ClientTimeout(total=12)
HEADERS = {
    "User-Agent": "AutoDonghuaBot/1.0 (anime metadata aggregator)",
    "Accept": "application/json,text/html",
}

SOURCE_LABELS = {
    "anilist": "AniList",
    "jikan": "Jikan/MAL",
    "animxin": "AnimXin",
    "lucifer": "LuciferDonghua",
    "anilive": "AniLive",
}


@dataclass
class SourceResult:
    source: str
    title: str = ""
    image: str = None
    description: str = None
    url: str = None
    year: int = None
    score: float = None
    episodes: int = None
    genres: list = field(default_factory=list)


# ---------------------------------------------------------------------- #
# 1. AniList (GraphQL)
# ---------------------------------------------------------------------- #
ANILIST_QUERY = """
query ($q: String) {
  Page(page: 1, perPage: 5) {
    media(search: $q, type: ANIME) {
      id
      title { romaji english native }
      coverImage { large extraLarge color }
      description(asHtml: false)
      format
      seasonYear
      episodes
      genres
      averageScore
      siteUrl
    }
  }
}
"""


async def anilist(session, title: str):
    try:
        async with session.post(
                "https://graphql.anilist.co",
                json={"query": ANILIST_QUERY, "variables": {"q": title}},
                timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                return None
            data = (await resp.json()).get("data", {}).get("Page", {}).get("media", [])
    except Exception:
        return None
    if not data:
        return None
    scored = sorted(
        [(fuzzy_ratio(title, m.get("title", {}).get("english")
                      or m.get("title", {}).get("romaji") or ""), m) for m in data],
        key=lambda x: -x[0])
    _, m = scored[0]
    t = m.get("title", {})
    img = (m.get("coverImage") or {}).get("extraLarge") or (m.get("coverImage") or {}).get("large")
    return SourceResult(
        source="anilist",
        title=t.get("english") or t.get("romaji") or title,
        image=img,
        description=(m.get("description") or "")[:900],
        url=m.get("siteUrl"),
        year=m.get("seasonYear"),
        score=(m.get("averageScore") or 0) / 10 if m.get("averageScore") else None,
        episodes=m.get("episodes"),
        genres=m.get("genres") or [],
    )


# ---------------------------------------------------------------------- #
# 2. Jikan (MyAnimeList mirror)
# ---------------------------------------------------------------------- #
async def jikan(session, title: str):
    try:
        url = "https://api.jikan.moe/v4/anime"
        params = {"q": title, "limit": 5, "sfw": "true"}
        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                return None
            data = (await resp.json()).get("data", [])
    except Exception:
        return None
    if not data:
        return None
    scored = sorted([(fuzzy_ratio(title, a.get("title", "")), a) for a in data],
                    key=lambda x: -x[0])
    _, a = scored[0]
    return SourceResult(
        source="jikan",
        title=a.get("title") or title,
        image=(a.get("images", {}).get("jpg", {}) or {}).get("large_image_url"),
        description=(a.get("synopsis") or "")[:900],
        url=a.get("url"),
        year=a.get("year") or (a.get("aired", {}) or {}).get("from", "")[:4],
        score=a.get("score"),
        episodes=a.get("episodes"),
        genres=[g.get("name") for g in a.get("genres", [])],
    )


# ---------------------------------------------------------------------- #
# 3. AnimXin (animexin.dev) — HTML scrape
# ---------------------------------------------------------------------- #
ANIMXIN_BASE = "https://animexin.dev"
_IMG_RE = re.compile(r'<img[^>]+src="([^"]*wp-content/uploads/[^"]+?\.(?:jpg|jpeg|png))"', re.I)
_LINK_RE = re.compile(
    r'<a[^>]+href="(https://animexin\.dev/[^"?#]+)"[^>]*>\s*(?:<[^>]+>)*\s*([^<]{3,90}?)\s*(?:</[^>]+>)*\s*</a>', re.I)
_OG_IMG_RE = re.compile(r'property="og:image"\s+content="([^"]+)"', re.I)
_OG_DESC_RE = re.compile(r'(?:name|property)="og:description"\s+content="([^"]+)"', re.I)


async def _scrape_html_search(session, base: str, title: str):
    """Generic scrape of a WordPress-style `?s=` search page."""
    try:
        async with session.get(f"{base}/?s={urllib.parse.quote(title)}") as resp:
            if resp.status != 200:
                return None
            htmltext = await resp.text(errors="ignore")
    except Exception:
        return None
    imgs = _IMG_RE.findall(htmltext)
    links = [(href, html.unescape(txt).strip())
             for href, txt in _LINK_RE.findall(htmltext)]
    results = []
    li = 0
    for href, txt in links:
        img = imgs[li] if li < len(imgs) else None
        results.append({"title": txt, "url": href, "image": img})
        li += 1
    if not results:
        return None
    scored = sorted([(fuzzy_ratio(title, r["title"]), r) for r in results],
                    key=lambda x: -x[0])
    return scored[0][1]


async def _scrape_page_meta(session, url: str):
    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                return None, None
            text = await resp.text(errors="ignore")
    except Exception:
        return None, None
    m_img = _OG_IMG_RE.search(text)
    m_desc = _OG_DESC_RE.search(text)
    return (m_img.group(1) if m_img else None,
            m_desc.group(1) if m_desc else None)


async def animxin(session, title: str):
    hit = await _scrape_html_search(session, ANIMXIN_BASE, title)
    if not hit:
        return None
    img, desc = hit.get("image"), None
    if hit.get("url"):
        pg_img, pg_desc = await _scrape_page_meta(session, hit["url"])
        img = img or pg_img
        desc = pg_desc
    return SourceResult(
        source="animxin",
        title=hit.get("title") or title,
        image=img,
        description=desc,
        url=hit.get("url"),
    )


# ---------------------------------------------------------------------- #
# 4. LuciferDonghua — public WP-JSON search endpoint + page scrape
# ---------------------------------------------------------------------- #
LUCIFER_BASE = "https://luciferdonghua.org"


async def lucifer(session, title: str):
    try:
        async with session.get(
                f"{LUCIFER_BASE}/wp-json/wp/v2/search",
                params={"search": title, "per_page": 10}) as resp:
            if resp.status != 200:
                return await _lucifer_html_fallback(session, title)
            data = await resp.json()
    except Exception:
        return await _lucifer_html_fallback(session, title)
    if not data:
        return None
    anime = [d for d in data if d.get("subtype") == "anime"] or data
    scored = sorted([(fuzzy_ratio(title, d.get("title", "")), d) for d in anime],
                    key=lambda x: -x[0])
    hit = scored[0][1]
    img, desc = await _scrape_page_meta(session, hit.get("url", ""))
    return SourceResult(
        source="lucifer",
        title=hit.get("title") or title,
        image=img,
        description=desc,
        url=hit.get("url"),
    )


async def _lucifer_html_fallback(session, title: str):
    hit = await _scrape_html_search(session, LUCIFER_BASE, title)
    if not hit:
        return None
    img, desc = None, None
    if hit.get("url"):
        img, desc = await _scrape_page_meta(session, hit["url"])
    return SourceResult(source="lucifer", title=hit.get("title") or title,
                        image=img, description=desc, url=hit.get("url"))


# ---------------------------------------------------------------------- #
# 5. AniLive (configurable base URL, WP-JSON then HTML scrape)
# ---------------------------------------------------------------------- #
ANILIVE_BASE = "https://anilive.app"


async def anilive(session, title: str, base: str = None):
    base = base or ANILIVE_BASE
    # Attempt 1: WordPress JSON search (some AniLive mirrors expose it)
    try:
        async with session.get(f"{base}/wp-json/wp/v2/search",
                               params={"search": title, "per_page": 5}) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data:
                    hit = data[0]
                    img, desc = await _scrape_page_meta(session, hit.get("url", ""))
                    return SourceResult(source="anilive",
                                        title=hit.get("title") or title,
                                        image=img, description=desc,
                                        url=hit.get("url"))
    except Exception:
        pass
    # Attempt 2: plain HTML search page scrape
    hit = await _scrape_html_search(session, base, title)
    if not hit:
        return None
    img, desc = None, None
    if hit.get("url"):
        img, desc = await _scrape_page_meta(session, hit["url"])
    return SourceResult(source="anilive", title=hit.get("title") or title,
                        image=img, description=desc, url=hit.get("url"))


# ---------------------------------------------------------------------- #
# Aggregator
# ---------------------------------------------------------------------- #
FETCHERS = {
    "anilist": anilist,
    "jikan": jikan,
    "animxin": animxin,
    "lucifer": lucifer,
    "anilive": anilive,
}
# Priority used when merging fields from multiple sources
_IMAGE_PRIORITY = ["anilist", "jikan", "animxin", "lucifer", "anilive"]
_DESC_PRIORITY = ["anilist", "jikan", "lucifer", "animxin", "anilive"]


async def fetch_all(title: str, anilive_base: str = None) -> list:
    async with aiohttp.ClientSession(timeout=TIMEOUT, headers=HEADERS) as session:
        results = await asyncio.gather(
            anilist(session, title),
            jikan(session, title),
            animxin(session, title),
            lucifer(session, title),
            anilive(session, title, anilive_base),
            return_exceptions=True)
    out = []
    for r in results:
        if isinstance(r, SourceResult):
            out.append(r)
    return out


async def build_metadata(title: str, anilive_base: str = None) -> dict:
    """
    Merge every source into one metadata dict.

    Returns {
      title, image, description, url, year, score, episodes, genres,
      sources: [names that responded], by_source: {name: SourceResult}
    }
    """
    results = await fetch_all(title, anilive_base)
    by_source = {r.source: r for r in results}

    def pick(field, priority):
        for name in priority:
            r = by_source.get(name)
            if r and getattr(r, field, None):
                return getattr(r, field)
        return None

    return {
        "title": title,
        "image": pick("image", _IMAGE_PRIORITY),
        "description": pick("description", _DESC_PRIORITY),
        "url": pick("url", _IMAGE_PRIORITY),
        "year": pick("year", _IMAGE_PRIORITY),
        "score": pick("score", _IMAGE_PRIORITY),
        "episodes": pick("episodes", _IMAGE_PRIORITY),
        "genres": next((r.genres for r in
                        (by_source.get(n) for n in _IMAGE_PRIORITY) if r and r.genres), []),
        "sources": [SOURCE_LABELS[n] for n in
                    ("anilist", "jikan", "animxin", "lucifer", "anilive") if n in by_source],
        "by_source": by_source,
    }
