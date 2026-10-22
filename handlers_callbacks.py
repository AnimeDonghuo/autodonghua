"""
Inline-button callbacks: post confirmation flow, custom-image flow,
force-subscribe verification.
"""
import asyncio
import os

from telethon import events

from config import THUMB_DIR
from core import (PENDING, app, forcesub_check, is_staff,
                  save_thumb,
                  store, url_bytes, wait_for, flood_respond, flood_edit)
from handlers_files import (_fast_markup, confirmation_text,
                            fast_batch_text, flush_batch, markup,
                              send_pending, serve_file)
from handlers_user import forcesub_gate


def _docs(p):
    return p["channel_docs"]


# ---------------------------------------------------------------------- #
# Post confirmation flow
# ---------------------------------------------------------------------- #
@app.on(events.CallbackQuery(pattern=rb"post:tgl:([^:]+):(-?\d+)"))
async def cb_toggle(e):
    key = e.pattern_match.group(1).decode()
    cid = int(e.pattern_match.group(2).decode())
    p = PENDING.get(key)
    if not p or p["user_id"] != e.sender_id:
        return await e.answer("⛔ Not your post", alert=True)
    if cid in p["selected"]:
        p["selected"].discard(cid)
    else:
        p["selected"].add(cid)
    await flood_edit(e, confirmation_text(p),
                   buttons=markup(key, _docs(p), p["selected"]),
                   parse_mode="html")
    await e.answer()


@app.on(events.CallbackQuery(pattern=rb"post:all:(.+)"))
async def cb_select_all(e):
    key = e.pattern_match.group(1).decode()
    p = PENDING.get(key)
    if not p or p["user_id"] != e.sender_id:
        return await e.answer("⛔ Not your post", alert=True)
    p["selected"] = set(p["channels"])
    await flood_edit(e, confirmation_text(p),
                   buttons=markup(key, _docs(p), p["selected"]),
                   parse_mode="html")
    await e.answer()


@app.on(events.CallbackQuery(pattern=rb"post:img:(.+)"))
async def cb_custom_img(e):
    key = e.pattern_match.group(1).decode()
    p = PENDING.get(key)
    if not p or p["user_id"] != e.sender_id:
        return await e.answer("⛔ Not your post", alert=True)
    await e.answer("🖼 Send an image URL or upload a photo", alert=True)
    await flood_edit(e, confirmation_text(p)
                   + "\n\n🖼 <b>Send an image URL or upload a photo "
                     "now…</b> (saved forever for this title)",
                   buttons=markup(key, _docs(p), p["selected"]),
                   parse_mode="html")
    asyncio.ensure_future(_await_custom_image(key, p))


async def _await_custom_image(key, p):
    m = await wait_for(
        p["chat_id"], timeout=180,
        filt=lambda msg: msg.photo or
        (msg.text and msg.text.strip().startswith("http")))
    if not m or key not in PENDING:
        return
    if m.photo:
        ph = m.photo
        img = {"type": "photo", "id": ph.id,
               "access_hash": ph.access_hash,
               "file_reference": ph.file_reference}
        tpath = os.path.join(THUMB_DIR, f"{key}.jpg")
        try:
            await app.download_media(m.photo, file=tpath)
            with open(tpath, "rb") as f:
                p["thumb_bytes"] = f.read()
            p["thumb_path"] = tpath
        except Exception:
            pass
        source = "Custom ✨ (uploaded)"
    else:
        url = m.text.strip()
        img = {"type": "url", "url": url}
        try:
            p["thumb_bytes"] = await url_bytes(url)
            p["thumb_path"] = save_thumb(key, p["thumb_bytes"])
        except Exception:
            p["thumb_bytes"], p["thumb_path"] = None, None
        source = "Custom ✨ (url)"

    p["image"] = img
    p["image_source"] = source
    # persist: same title (any episode) reuses this forever
    await store.save_image(p["norm"], url=img.get("url"),
                             photo=None if img["type"] == "url" else img,
                             source="custom", custom=True)
    try:
        await app.edit_message(
            p["chat_id"], p["msg_id"], text=confirmation_text(p),
            file=p.get("thumb_path"),
            buttons=markup(key, _docs(p), p["selected"]),
            parse_mode="html")
    except Exception:
        await app.edit_message(
            p["chat_id"], p["msg_id"],
            text=confirmation_text(p)
                 + "\n\n🖼 Custom image saved!",
            buttons=markup(key, _docs(p), p["selected"]),
            parse_mode="html")
    try:
        await m.delete()
    except Exception:
        pass


@app.on(events.CallbackQuery(pattern=rb"post:send:(.+)"))
async def cb_send(e):
    key = e.pattern_match.group(1).decode()
    p = PENDING.pop(key, None)
    if not p or p["user_id"] != e.sender_id:
        return await e.answer("⛔ Not your post", alert=True)
    if not p["selected"]:
        return await e.answer("Select at least one channel first",
                                alert=True)
    await flood_edit(e, "📤 <b>Sending to selected channels…</b>",
                   parse_mode="html")
    text = await send_pending(p)
    await flood_edit(e, text, parse_mode="html")
    try:
        os.remove(p["path"])
    except Exception:
        pass


@app.on(events.CallbackQuery(pattern=rb"post:cancel:(.+)"))
async def cb_cancel(e):
    key = e.pattern_match.group(1).decode()
    p = PENDING.pop(key, None)
    if not p or p["user_id"] != e.sender_id:
        return await e.answer("⛔ Not your post", alert=True)
    try:
        os.remove(p["path"])
    except Exception:
        pass
    await flood_edit(e, "❌ Post cancelled.", parse_mode="html")


# ---------------------------------------------------------------------- #
# Force-subscribe verification
# ---------------------------------------------------------------------- #
@app.on(events.CallbackQuery(pattern=rb"fs:check"))
async def cb_fs_check(e):
    from core import forcesub_check
    ok, _ = await forcesub_check(e.sender_id)
    if ok:
        await flood_edit(e, "✅ <b>Verified!</b> You can now use "
                       "/request & get files 🎉", parse_mode="html")
    else:
        await e.answer("❌ Not a member yet — join first", alert=True)


@app.on(events.CallbackQuery(pattern=rb"fs:admin"))
async def cb_fs_admin(e):
    await e.answer("Contact the bot owner to join the channel.",
                     alert=True)


# ---------------------------------------------------------------------- #
# File store — user downloads a clean copy (no tag)
# ---------------------------------------------------------------------- #
@app.on(events.CallbackQuery(pattern=rb"fast:(more|done):(-?\d+)"))
async def cb_fast_batch(e):
    """➕ Add more / ✅ Done buttons of the filename fast path."""
    act = e.pattern_match.group(1).decode()
    chat = int(e.pattern_match.group(2))
    if not await is_staff(e.sender_id):
        return await e.answer("Staff only.", alert=True)
    if act == "more":
        await e.answer("Ready — send the next file.")
        try:
            await e.edit(fast_batch_text(chat, ready=True),
                         parse_mode="html",
                         buttons=_fast_markup(chat))
        except Exception:
            pass
        return
    await e.answer("Building posters & cards…")
    try:
        msg = await e.get_message()
    except Exception:
        msg = None
    n = await flush_batch(chat, msg)
    if not n:
        await e.answer(
            "No pending batch reached this instance (memory + DB "
            "both empty). If Koyeb is still draining an old "
            "instance, re-send the files once — the new build "
            "persists batches in Mongo.", alert=True)


@app.on(events.CallbackQuery(pattern=rb"getfile:(.+)"))
async def cb_getfile(e):
    rec_id = e.pattern_match.group(1).decode()
    ok, fs = await forcesub_check(e.sender_id)
    if not ok:
        await e.answer("🔒 Join the channel first", alert=True)
        return await forcesub_gate(e, fs)
    rec = await store.get_file(rec_id)
    if not rec:
        return await e.answer("❌ File no longer available",
                                  alert=True)
    await e.answer("📥 Sending to your DM…")
    got = await serve_file(e.sender_id, rec)
    if got:
        await flood_respond(e, "📥 <b>File sent!</b> Check your downloads 👇",
                          parse_mode="html")
    else:
        await flood_respond(e, "❌ File unavailable right now — ask an admin.",
                          parse_mode="html")
