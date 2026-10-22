"""
File flow: staff sends a donghua/anime file →
  mediainfo quality extraction → name/episode identification →
  multi-source metadata (AniList, AniLive, AnimXin, LuciferDonghua) →
  image caching (custom or sourced, reused across episodes) →
  channel-picker confirmation → send → store in TG DB channel.
"""
import asyncio
import os
import re
import time
import traceback

from telethon import events
from telethon.tl.custom import Button
from telethon.tl.types import DocumentAttributeFilename, InputDocument

from config import DOWNLOAD_DIR, THUMB_DIR
from core import (AUTH, LOAD, PENDING, WAITERS, app, authed,
                  bot_user, fname_attrs, thumb_16x9,
                  image_bytes, is_staff, is_video_doc, now_key,
                  save_thumb, store, url_bytes, flood_respond, flood_edit)
from mediainfo_util import analyze, caption_lines, quality_label
from parser import normalize_key, parse_filename
from sources import build_metadata
from style import escape_html, RESTRICTED_MSG


FAST_ENG = {"video": "360p-eng", "video (2)": "720p-eng",
            "video (3)": "1080p-eng", "video (4)": "480p-eng"}


def _fast_filename(fname: str):
    """'Jie Men Zhi Xia Eps 20 [720p].mp4' -> ('Jie Men Zhi Xia', 20,
    '720p'). '[Video]' / '[Video (2)]' / ... are the eng flavours
    (360p/720p/1080p/480p-eng) — no mediainfo scan needed at all."""
    m = re.match(r"^(?P<series>.+?)\s+Eps\s+(?P<ep>\d{1,4})\s+"
                 r"\[(?P<tag>[^\]]+)\](?:\.\w+)?$", fname.strip())
    if not m:
        return None
    tag = m.group("tag").strip().lower()
    if re.fullmatch(r"\d{3,4}p", tag):
        q = tag
    elif tag in FAST_ENG:
        q = FAST_ENG[tag]
    elif tag.endswith("-eng"):
        q = tag
    else:
        return None
    return m.group("series").strip(), int(m.group("ep")), q


FAST_BATCH = {}   # chat_id -> {"items": {(norm, ep): {...}}, ...}
FAST_FLUSHING = set()  # chats whose Done build is running
FAST_LOCK = asyncio.Lock()
FAST_AUTO_SECONDS = 15 * 60


async def _persist_batch(chat_id: int):
    """Survive restarts/redeploys: the pending batch lives in Mongo
    too, so ✅ Done still works after Koyeb recycles the instance."""
    b = FAST_BATCH.get(chat_id)
    if not b:
        return
    items = {f"{n}|{ep}": {"series": it["series"],
                           "quals": sorted(it["quals"]),
                           "count": it["count"]}
             for (n, ep), it in b["items"].items()}
    try:
        await store.col("fast_batch").update_one(
            {"_id": int(chat_id)},
            {"$set": {"items": items,
                      "msg_id": b["msg"].id if b.get("msg") else None,
                      "updated": time.time()}},
            upsert=True)
        try:
            chk = await store.find("fast_batch", {"_id": int(chat_id)})
            n = len((chk or [{}])[0].get("items") or {}) if chk else 0
            print(f"[file-flow] batch persisted chat={chat_id} "
                  f"items={len(items)} readback={n}")
        except Exception as exc2:
            print("[file-flow] batch readback failed:",
                  repr(exc2)[:100])
    except Exception as exc:
        print("[file-flow] batch persist failed:", repr(exc)[:120])


async def _load_persisted(chat_id: int):
    """{(norm, ep): item} from the DB copy of the batch, or {}."""
    try:
        docs = await store.find("fast_batch", {"_id": int(chat_id)})
    except Exception:
        return {}, None
    if not docs:
        return {}, None
    d = docs[0]
    items = {}
    for key, it in (d.get("items") or {}).items():
        n, _, ep = key.partition("|")
        items[(n, int(ep))] = {"series": it.get("series", n),
                               "quals": set(it.get("quals") or []),
                               "count": it.get("count", 1)}
    return items, d.get("msg_id")


async def restore_fast_batches():
    """Boot: re-arm every batch that survived a restart."""
    try:
        docs = await store.find("fast_batch")
    except Exception:
        return
    for d in docs or []:
        chat = d.get("_id")
        items, msg_id = await _load_persisted(chat)
        if not items:
            try:
                await store.col("fast_batch").delete_one(
                    {"_id": chat})
            except Exception:
                pass
            continue
        msg = None
        if msg_id:
            try:
                msg = await app.get_messages(chat, ids=msg_id)
            except Exception:
                msg = None
        FAST_BATCH[chat] = {"items": items, "timer": None,
                            "msg": msg}
        await _arm_auto_flush(chat)
        print(f"[file-flow] restored pending batch for {chat}: "
              f"{len(items)} episode(s)")


def _fast_markup(chat_id: int) -> list:
    return [[Button.inline("➕ Add more",
                           f"fast:more:{chat_id}".encode())],
            [Button.inline("✅ Done — poster & cards",
                           f"fast:done:{chat_id}".encode())]]


def fast_batch_text(chat_id: int, ready: bool = False) -> str:
    b = FAST_BATCH.get(chat_id)
    if not b or not b["items"]:
        return "📥 <b>Batch empty</b> — send a file to start one."
    lines = []
    for (norm, ep), it in sorted(b["items"].items(),
                                 key=lambda kv: kv[0][1]):
        quals = ", ".join(sorted(it["quals"]))
        lines.append(f"• <b>{escape_html(it['series'])}</b> Ep {ep} "
                     f"— {quals} ({it['count']} file(s))")
    head = ("📥 <b>Ready for the next file</b> — send it and it joins "
            "this batch.\n\n" if ready else
            "✅ <b>Archived & registered</b> — the watcher skips "
            "these qualities now.\n\n")
    return (head + "\n".join(lines) +
            "\n\n<b>✅ Done</b> builds the posters & episode cards "
            "for the whole batch.")


async def _arm_auto_flush(chat_id: int):
    b = FAST_BATCH.get(chat_id)
    if not b:
        return
    if b.get("timer"):
        b["timer"].cancel()
    b["timer"] = asyncio.ensure_future(_auto_flush(chat_id))


async def _auto_flush(chat_id: int):
    await asyncio.sleep(FAST_AUTO_SECONDS)
    b = FAST_BATCH.get(chat_id)
    await flush_batch(chat_id, b.get("msg") if b else None, auto=True)


async def flush_batch(chat_id: int, msg=None, auto: bool = False):
    """The Done step: posters, descriptions and episode cards for
    every (series, episode) collected in this chat's batch."""
    import autodl
    async with FAST_LOCK:
        b = FAST_BATCH.pop(chat_id, None)
        if b and b.get("timer"):
            b["timer"].cancel()
        items = dict(b["items"]) if b and b["items"] else {}
        print(f"[file-flow] flush chat={chat_id} "
              f"memory_items={len(items)}")
        if not items:  # restart happened? the DB copy survives it
            items, msg_id = await _load_persisted(chat_id)
            print(f"[file-flow] flush chat={chat_id} "
                  f"db_items={len(items)}")
            if not msg and msg_id:
                try:
                    msg = await app.get_messages(chat_id, ids=msg_id)
                except Exception:
                    msg = None
        if not items:
            if msg:
                try:
                    await flood_edit(msg, "📥 <b>Batch empty</b> — "
                                          "nothing to finish.",
                                     parse_mode="html")
                except Exception:
                    pass
            return 0
        try:
            await store.col("fast_batch").delete_one(
                {"_id": int(chat_id)})
        except Exception:
            pass
        FAST_FLUSHING.add(chat_id)
        try:
            return await _run_flush(chat_id, msg, items, auto)
        finally:
            FAST_FLUSHING.discard(chat_id)


async def _run_flush(chat_id, msg, items, auto):
    import autodl
    done, failed = [], []
    todo = sorted(items.items(), key=lambda kv: kv[0][1])
    for idx, ((norm, ep), it) in enumerate(todo, 1):
        series = it["series"]
        step = f"{idx}/{len(todo)} — <b>{escape_html(series)}</b> Ep {ep}"
        try:
            if msg:
                await flood_edit(msg, f"🎬 <b>Building posters & "
                                      f"cards…</b> {step}: "
                                      f"🔍 searching API…",
                                 parse_mode="html")

            async def _find():
                for sec in ("donghua", "anime"):
                    found = await autodl._search_episode(sec, series,
                                                         ep)
                    if found:
                        return found
                return None
            try:  # never hang the whole build on a slow API
                item = await asyncio.wait_for(_find(), 30)
            except Exception:
                item = None
            item = item or {"id": 0,
                            "title": f"{series} Eps {ep:02d}",
                            "series": series, "type": "donghua",
                            "genres": [], "rating": None,
                            "description": "",
                            "section": "donghua"}
            if msg:
                await flood_edit(msg, f"🎬 <b>Building posters & "
                                      f"cards…</b> {step}: "
                                      f"🖼 poster…",
                                 parse_mode="html")
            poster, _src = await autodl.resolve_poster(item,
                                                       series)
            if msg:
                await flood_edit(msg, f"🎬 <b>Building posters & "
                                      f"cards…</b> {step}: "
                                      f"📺 posting cards…",
                                 parse_mode="html")
                recs = [r for r in await store.find(
                "downloaded", {"norm_title": norm, "episode": ep})
                if r.get("file_link")]
            links = {r["quality"]: r["file_link"] for r in recs}
            deeps = {r["quality"]: (r.get("deep_link")
                                    or await
                                    autodl.ensure_deep_link(
                                        autodl._key(norm, ep,
                                                    r["quality"]))
                                    or r["file_link"])
                     for r in recs}
            group = {"series": series, "norm": norm,
                     "entries": [(item, [],
                                  item.get("streams", []))]}
            posted = await autodl.post_episode(group, ep, links,
                                               deeps, poster)
            done.append(f"• <b>{escape_html(series)}</b> Ep {ep} "
                        f"→ {posted} card(s)")
        except Exception as exc:
            failed.append(f"• {escape_html(series)} Ep {ep}: "
                          f"<code>{escape_html(str(exc)[:80])}"
                          f"</code>")
    txt = (("⏰ <b>Auto-finish</b> (batch was idle) — " if auto else
            "🎬 <b>Batch finished</b> — ") +
           "posters & episode cards:\n\n" +
           "\n".join(done or ["• nothing"]))
    if failed:
        txt += "\n\n❌ <b>Failed:</b>\n" + "\n".join(failed)
    if msg:
        try:
            await flood_edit(msg, txt, parse_mode="html")
        except Exception:
            pass
    return len(done)


async def _fast_ingest(e, fname, series, ep, quality, media_obj):
    """Zero-scan ingest: the filename already says everything, so we
    only check the description. The file is archived BY REFERENCE
    (server-side copy — no re-upload) into every DB channel and
    registered so the API watcher never re-downloads this quality.
    Then the batch buttons appear: ➕ Add more / ✅ Done (Done builds
    posters & cards). No LOAD lock — sends never queue behind it."""
    import autodl
    norm = normalize_key(series)
    status = await flood_respond(
        e, f"🔄 <b>Fast ingest</b> — {escape_html(series)} Ep {ep} "
           f"[{quality}] — archiving by reference…",
        parse_mode="html")
    try:
        db_ids = await store.db_channel_ids()
        if not db_ids:
            if status:
                await flood_edit(status, "❌ No DB channel set — "
                                         "/adddbchannel first.")
            return
        size = getattr(media_obj, "size", 0) or 0
        cap = (f"{series} — Ep {ep} [{quality}] "
               f"{autodl.human_size(size)}")
        refs = []
        for dbc in db_ids:
            try:
                sentm = await app.send_file(dbc, file=media_obj,
                                            caption=cap,
                                            supports_streaming=True)
                refs.append({
                    "channel": int(dbc), "msg_id": sentm.id,
                    "link": await autodl.file_link(dbc, sentm.id),
                    "doc": autodl._doc_ref(sentm.document
                                           or sentm.video)})
            except Exception as exc:
                print(f"[file-flow] fast archive failed to "
                      f"{dbc}: {exc}")
        if not refs:
            if status:
                await flood_edit(status, "❌ Archive failed to "
                                         "every DB channel.")
            return
        primary, extra = refs[0], refs[1:]
        await autodl.mark_downloaded(
            0, norm, series, ep, quality,
            {k: primary[k] for k in ("channel", "msg_id", "doc")},
            primary["link"], size=size, db_extra=extra)
        b = FAST_BATCH.setdefault(e.chat_id, {"items": {},
                                              "timer": None,
                                              "msg": status})
        it = b["items"].setdefault((norm, ep),
                                   {"series": series, "quals": set(),
                                    "count": 0})
        it["quals"].add(quality)
        it["count"] += 1
        b["msg"] = status
        await _persist_batch(e.chat_id)
        await _arm_auto_flush(e.chat_id)
        if status:
            await flood_edit(status, fast_batch_text(e.chat_id),
                             parse_mode="html",
                             buttons=_fast_markup(e.chat_id))
    except Exception:
        err = traceback.format_exc()[-300:]
        if status:
            try:
                await flood_edit(status,
                                 f"❌ <b>Fast ingest failed:</b>\n"
                                 f"<code>{escape_html(err)}</code>",
                                 parse_mode="html")
            except Exception:
                pass
        print("[file-flow] fast ingest error:", err)


def _file_name(e) -> str:
    media = e.document or e.video
    for a in (getattr(media, "attributes", None) or []):
        if isinstance(a, DocumentAttributeFilename):
            return a.file_name
    return "video.mp4"


def build_caption(parsed: dict, info: dict, meta: dict, tag: str) -> str:
    lines = [f"🎬 <b>{escape_html(parsed['title'])}</b>"]
    if parsed.get("season"):
        lines.append(f"📦 <b>Season:</b> S{parsed['season']:02d}")
    if parsed.get("episode") is not None:
        lines.append(f"📺 <b>Episode:</b> {parsed['episode']}")
    lines.append("")
    lines += caption_lines(info)
    if meta:
        if meta.get("score"):
            lines.append(f"⭐ <b>Score:</b> {meta['score']}/10")
        if meta.get("year"):
            lines.append(f"📅 <b>Year:</b> {meta['year']}")
        if meta.get("genres"):
            lines.append(f"🎭 <b>Genres:</b> {', '.join(meta['genres'][:4])}")
    if tag:
        lines.append(f"🏷 <b>Tag:</b> {escape_html(tag)}")
    if bot_user and bot_user.username:
        lines.append(f"\n⚡ <b>Powered by:</b> @{bot_user.username}")
    return "\n".join(lines)[:1000]


def clean_caption(caption: str) -> str:
    """Caption without the tag line — what users get."""
    return "\n".join(l for l in caption.split("\n")
                     if not l.startswith("🏷"))


def confirmation_text(p: dict) -> str:
    parsed, info, meta = p["parsed"], p["info"], p["meta"]
    lines = [
        "📋 <b>Post Preview — Confirm & Send</b>",
        "",
        f"🎬 <b>{escape_html(parsed['title'])}</b>",
        f"📺 <b>Episode:</b> {parsed.get('episode') if parsed.get('episode') is not None else '—'}",
        f"💠 <b>Quality:</b> {escape_html(info.get('quality_label') or 'unknown')}",
        f"💾 <b>Size:</b> {info.get('size_str') or 'unknown'}",
        f"🖼 <b>Poster:</b> {escape_html(p.get('image_source') or 'none')}",
        f"🛰 <b>Sources:</b> {', '.join(meta.get('sources') or []) or 'none responded'}",
    ]
    if not p["channels"]:
        lines.append("\n⚠️ <b>No posting channels!</b> Add one with "
                     "/addchannel first.")
    lines.append("\n👇 Toggle channels, then press <b>📤 Send Now</b>.\n"
                 "🖼 <i>Custom Image</i> to set/replace the poster (saved "
                 "for this title forever).")
    return "\n".join(lines)


def markup(key: str, channels, selected) -> list:
    rows = []
    for c in channels:
        mark = "✅" if c["_id"] in selected else "⚪"
        name = (c.get("title") or str(c["_id"]))[:26]
        tag = c.get("tag") or ""
        label = f"{mark} {name}" + (f" [{tag}]" if tag else "")
        rows.append([Button.inline(label,
                                     f"post:tgl:{key}:{c['_id']}".encode())])
    if channels:
        rows.append([
            Button.inline("✔ Select All", f"post:all:{key}".encode()),
            Button.inline("📤 Send Now", f"post:send:{key}".encode())])
    rows.append([
        Button.inline("🖼 Custom Image", f"post:img:{key}".encode()),
        Button.inline("❌ Cancel", f"post:cancel:{key}".encode())])
    return rows


def _merge_quality_hint(info: dict, parsed: dict) -> dict:
    """When mediainfo/ffprobe aren't available (e.g. Koyeb free),
    fall back to quality tags found in the filename."""
    if not info.get("resolution") and parsed.get("quality_hint"):
        hint = parsed["quality_hint"].lower()
        res = {"4k": "2160p (4K)", "2160p": "2160p (4K)",
               "1080p": "1080p (FHD)", "1080i": "1080i (FHD)",
               "720p": "720p (HD)", "480p": "480p (SD)",
               "360p": "360p (SD)"}.get(hint, hint.upper())
        info["resolution"] = res
        h = int(hint.rstrip("pi"))
        info["height"] = h
        info["quality_label"] = quality_label(info)
    return info


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


@app.on(events.NewMessage(func=lambda e: (e.document or e.video)
                           and e.is_private))
async def on_file(e):
    # a conversation (recover / custom image) is in progress — don't touch it
    if e.chat_id in WAITERS:
        return
    if not await is_staff(e.sender_id):
        return await flood_respond(e, RESTRICTED_MSG, parse_mode="html")

    media_obj = e.video or e.document
    if not media_obj:
        return
    if e.document and not is_video_doc(e.document) and not e.video:
        return  # ignore non-media documents in the auto flow

    fname = _file_name(e)
    fast = _fast_filename(fname)
    if fast:  # description carries series/ep/quality: instant path,
        await _fast_ingest(e, fname, fast[0], fast[1], fast[2],
                           media_obj)   # never waits on LOAD
        return

    if LOAD.locked():
        return await flood_respond(e, 
            "⏳ <b>One file is being processed right now</b> — "
            "please wait a few seconds and send it again.\n\n"
            "<i>(free-tier instances process one file at a time)</i>",
            parse_mode="html")

    path = None
    status = None
    try:
        async with LOAD:
            status = await flood_respond(e, 
                "🔄 <b>Processing your file…</b>\n"
                "① <i>mediainfo quality scan…</i>", parse_mode="html")

            # 1) download + mediainfo
            safe = "".join(c if c.isalnum() or c in "._- " else "_"
                           for c in fname)
            path = await app.download_media(
                e.media, file=os.path.join(DOWNLOAD_DIR, safe))
            await status.edit(
                "🔄 <b>Processing your file…</b>\n"
                "② <i>name & episode detection…</i>", parse_mode="html")
            info = await analyze(path)

            # 2) identify name + episode
            parsed = parse_filename(fname)
            info = _merge_quality_hint(info, parsed)
            norm = normalize_key(parsed["title"])

            # 3) multi-source metadata
            await status.edit(
                "🔄 <b>Processing your file…</b>\n"
                "③ <i>AniList • AniLive • AnimXin • LuciferDonghua "
                "lookup…</i>", parse_mode="html")
            meta = await build_metadata(
                parsed["title"],
                anilive_base=await store.get_config("anilive_base"))

            # 4) resolve image: custom cache first (keyed by title →
            #    reused for every episode of the same name), then sources
            await status.edit(
                "🔄 <b>Processing your file…</b>\n"
                "④ <i>poster & caption…</i>", parse_mode="html")
            img_doc = await store.get_image(norm)
            if img_doc:
                image, image_source = img_doc, (
                    "Custom ✨ (saved)" if img_doc.get("custom")
                    else f"Cached • {img_doc.get('source', 'auto')}")
            elif meta.get("image"):
                src_name = next(
                    (s for s in ("anilist", "jikan", "animxin",
                                 "lucifer", "anilive")
                     if meta["by_source"].get(s)
                     and meta["by_source"][s].image), None)
                image = {"type": "url", "url": meta["image"]}
                image_source = f"{src_name or 'auto'} (auto)"
                await store.save_image(norm, url=meta["image"],
                                         source=src_name or "auto")
            else:
                image, image_source = None, "none"

            thumb_bytes = None
            if image:
                try:
                    thumb_bytes = await image_bytes(image)
                except Exception:
                    thumb_bytes = None
            thumb_path = None
            if thumb_bytes:
                thumb_path = save_thumb(now_key(e.sender_id), thumb_bytes)

            # 5) build caption + confirmation
            channels = await store.list_channels()
            caption = build_caption(parsed, info, meta,
                                      tag=channels[0].get("tag", "")
                                      if channels else "")

            key = now_key(e.sender_id)
            PENDING[key] = {
                "key": key, "user_id": e.sender_id, "chat_id": e.chat_id,
                "msg_id": status.id,
                "media_obj": media_obj, "path": path,
                "media_kind": "video" if e.video else "doc",
                "parsed": parsed, "info": info, "meta": meta, "norm": norm,
                "image": image, "image_source": image_source,
                "thumb_bytes": thumb_bytes, "thumb_path": thumb_path,
                "channels": [c["_id"] for c in channels],
                "channel_docs": channels,
                "channel_titles": {c["_id"]: c.get("title", str(c["_id"]))
                                    for c in channels},
                "selected": set(), "caption": caption,
                "clean_caption": clean_caption(caption),
                "tag": (channels[0].get("tag", "") if channels else ""),
            }
            p = PENDING[key]

            try:
                await status.edit(confirmation_text(p),
                                    file=thumb_path,
                                    buttons=markup(key, channels, set()),
                                    parse_mode="html")
            except Exception:
                await status.edit(confirmation_text(p),
                                    buttons=markup(key, channels, set()),
                                    parse_mode="html")
    except Exception:
        err = traceback.format_exc()[-400:]
        try:
            if status:
                await status.edit(f"❌ <b>Processing failed:</b>\n"
                                  f"<code>{escape_html(err)}</code>",
                                  parse_mode="html")
        except Exception:
            pass
        print("[file-flow] error:", err)


def _thumb_bytes(p):
    tb = p.get("thumb_bytes")
    tp = p.get("thumb_path")
    if not tb and tp:
        try:
            with open(tp, "rb") as fh:
                tb = fh.read()
        except Exception:
            tb = None
    return thumb_16x9(tb) if tb else None


async def send_pending(p, status_msg=None):
    """Send the pending post to all selected channels, then store a
    copy in the TG DB channel."""
    ok, fail = [], []
    for cid in sorted(p["selected"]):
        sent = False
        err = "unknown error"
        try:
            await app.send_file(
                cid, file=p["media_obj"], caption=p["caption"],
                thumb=_thumb_bytes(p),
                supports_streaming=True, force_document=False)
            sent = True
        except Exception as ex:
            err = str(ex)[:90]
        if not sent:
            # fallback: re-upload from the local file
            try:
                await app.send_file(
                    cid, file=p["path"], caption=p["caption"],
                    thumb=_thumb_bytes(p),
                    supports_streaming=True, force_document=False,
                    attributes=fname_attrs(p["caption"], p["path"]))
                sent = True
            except Exception as ex:
                err = str(ex)[:90]
        if sent:
            ok.append(cid)
        else:
            fail.append((cid, err))

    # ---- store a copy in the TG DB channel ----
    db_info = None
    db_chan = await store.get_config("db_channel")
    if db_chan:
        try:
            sent_db = await app.send_file(
                db_chan, file=p["media_obj"], caption=p["caption"],
                thumb=_thumb_bytes(p),
                supports_streaming=True, force_document=False)
            doc_obj = sent_db.document or sent_db.video
            db_info = {"channel": int(db_chan), "msg_id": sent_db.id,
                       "doc": _doc_ref(doc_obj)}
        except Exception:
            try:  # re-upload fallback
                sent_db = await app.send_file(
                    db_chan, file=p["path"], caption=p["caption"],
                    thumb=_thumb_bytes(p),
                    supports_streaming=True, force_document=False,
                    attributes=fname_attrs(p["caption"], p["path"]))
                doc_obj = sent_db.document or sent_db.video
                db_info = {"channel": int(db_chan), "msg_id": sent_db.id,
                           "doc": _doc_ref(doc_obj)}
            except Exception as ex:
                db_info = {"channel": int(db_chan), "error": str(ex)[:90]}

    await store.record_file({
        "norm_title": p["norm"], "title": p["parsed"]["title"],
        "episode": p["parsed"]["episode"], "season": p["parsed"]["season"],
        "quality": p["info"].get("quality_label"),
        "size_bytes": p["info"].get("size_bytes"),
        "file_name": p["parsed"]["raw"], "user_id": p["user_id"],
        "channels": sorted(p["selected"]), "caption": p["caption"],
        "clean_caption": p["clean_caption"],
        "image": p["image"], "image_source": p["image_source"],
        "sources": p["meta"].get("sources", []),
        "media_kind": p["media_kind"], "db": db_info,
    })

    names = ", ".join(
        f"<b>{escape_html(p['channel_titles'].get(c, str(c)))}</b>"
        for c in ok)
    text = f"✅ <b>Sent to {len(ok)} channel(s)!</b>\n{names}"
    if db_info and not db_info.get("error"):
        text += "\n🗂 Copy archived in the <b>DB channel</b>"
    if fail:
        text += "\n\n❌ <b>Failed:</b>\n" + "\n".join(
            f"• <code>{c}</code>: {escape_html(r)}" for c, r in fail)
    return text


async def serve_file(user_id: int, rec: dict):
    """Give a user a clean copy (no tag) of a stored file."""
    db = rec.get("db") or {}
    media = None
    ref = db.get("doc")
    if ref and ref.get("id") is not None:
        media = InputDocument(
            id=ref["id"], access_hash=ref["access_hash"],
            file_reference=ref.get("file_reference", b""))
    caption = (rec.get("clean_caption")
               or f"{rec.get('title', '')} Episode "
                  f"{rec.get('episode', '')} [{rec.get('quality', '')}]"
                  .strip())
    try:
        await app.send_file(
            user_id, file=media, caption=caption,
            supports_streaming=True)
        return True
    except Exception:
        pass
    # fallback: pull the message from the DB channel(s)
    attempts = [db] + list(rec.get("db_extra") or [])
    for extra in attempts:
        if not (extra.get("channel") and extra.get("msg_id")):
            continue
        try:
            m = await app.get_messages(extra["channel"],
                                       ids=extra["msg_id"])
            await app.send_file(
                user_id, file=m.document or m.video, caption=caption,
                supports_streaming=True)
            return True
        except Exception:
            continue
    return False
