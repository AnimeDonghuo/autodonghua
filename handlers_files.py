"""
File flow: staff sends a donghua/anime file →
  mediainfo quality extraction → name/episode identification →
  multi-source metadata (AniList, AniLive, AnimXin, LuciferDonghua) →
  image caching (custom or sourced, reused across episodes) →
  channel-picker confirmation → send → store in TG DB channel.
"""
import os
import time
import traceback

from telethon import events
from telethon.tl.custom import Button
from telethon.tl.types import DocumentAttributeFilename, InputDocument

from config import DOWNLOAD_DIR, THUMB_DIR
from core import (AUTH, LOAD, PENDING, WAITERS, app, authed, bot_user,
                  image_bytes, is_staff, is_video_doc, now_key,
                  save_thumb, store, url_bytes)
from mediainfo_util import analyze, caption_lines, quality_label
from parser import normalize_key, parse_filename
from sources import build_metadata
from style import escape_html, RESTRICTED_MSG


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
        return await e.respond(RESTRICTED_MSG, parse_mode="html")

    media_obj = e.video or e.document
    if not media_obj:
        return
    if e.document and not is_video_doc(e.document) and not e.video:
        return  # ignore non-media documents in the auto flow

    if LOAD.locked():
        return await e.respond(
            "⏳ <b>One file is being processed right now</b> — "
            "please wait a few seconds and send it again.\n\n"
            "<i>(free-tier instances process one file at a time)</i>",
            parse_mode="html")

    fname = _file_name(e)
    path = None
    status = None
    try:
        async with LOAD:
            status = await e.respond(
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
                thumb=p.get("thumb_bytes"), supports_streaming=True,
                force_document=(p["media_kind"] == "doc"))
            sent = True
        except Exception as ex:
            err = str(ex)[:90]
        if not sent:
            # fallback: re-upload from the local file
            try:
                await app.send_file(
                    cid, file=p["path"], caption=p["caption"],
                    thumb=p.get("thumb_path"), supports_streaming=True,
                    force_document=(p["media_kind"] == "doc"))
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
                thumb=p.get("thumb_bytes"), supports_streaming=True,
                force_document=(p["media_kind"] == "doc"))
            doc_obj = sent_db.document or sent_db.video
            db_info = {"channel": int(db_chan), "msg_id": sent_db.id,
                       "doc": _doc_ref(doc_obj)}
        except Exception:
            try:  # re-upload fallback
                sent_db = await app.send_file(
                    db_chan, file=p["path"], caption=p["caption"],
                    thumb=p.get("thumb_path"),
                    supports_streaming=True,
                    force_document=(p["media_kind"] == "doc"))
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
            supports_streaming=True, force_document=True)
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
                supports_streaming=True, force_document=True)
            return True
        except Exception:
            continue
    return False
