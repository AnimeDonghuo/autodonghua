"""
AutoDL commands: /autodl, /checknew + quality-button callbacks.
"""
import asyncio

from telethon import events
from telethon.tl.custom import Button

import autodl
from core import app, is_owner, require_staff, store
from handlers_files import serve_file
from handlers_user import forcesub_gate
from style import escape_html


@app.on(events.NewMessage(pattern=r"^/autodl(?:\s|$)"))
async def cmd_autodl(e):
    if not await require_staff(e):
        return
    parts = e.raw_text.split()
    sub = parts[1].lower() if len(parts) > 1 else "status"
    st = await autodl.get_settings()

    if sub == "on":
        st["enabled"] = True
        await autodl.save_settings(st)
        pending = len(await autodl.check_new())
        qtxt = ", ".join(st["qualities"]) or "all"
        await e.respond(
            f"🤖 <b>AutoDL ENABLED</b>\n\n"
            f"Watching every <b>{st['interval']} min</b> — any "
            f"episode the DB does NOT have yet is downloaded "
            f"&amp; posted automatically.\n"
            f"Qualities: <b>{qtxt}</b> per channel tag\n"
            f"Registry: <b>{await autodl.registry_size()}</b> "
            f"archived files\n"
            f"⏳ Pending right now: <b>{pending}</b> episode(s) — "
            f"the first pass starts on the next tick.",
            parse_mode="html")
    elif sub == "off":
        st["enabled"] = False
        await autodl.save_settings(st)
        await e.respond("🤖 <b>AutoDL DISABLED</b>",
                        parse_mode="html")
    elif sub == "qualities" and len(parts) > 2:
        if parts[2].lower() == "all":
            st["qualities"] = []
            await autodl.save_settings(st)
            return await e.respond("✅ Qualities: <b>all</b> — every "
                                     "quality the API offers, per "
                                     "channel tag.",
                                     parse_mode="html")
        qs = [q.strip().lower() for q in parts[2].split(",")
              if q.strip()]
        if not qs:
            return await e.respond("📌 Usage: /autodl qualities "
                                     "360p,480p,720p,1080p")
        st["qualities"] = qs
        await autodl.save_settings(st)
        await e.respond(f"✅ Download qualities set: "
                        f"<b>{', '.join(qs)}</b>",
                        parse_mode="html")
    elif sub == "interval" and len(parts) > 2 and parts[2].isdigit():
        st["interval"] = max(5, int(parts[2]))
        await autodl.save_settings(st)
        await e.respond(f"✅ Poll interval: <b>{st['interval']} "
                        f"min</b>", parse_mode="html")
    elif sub == "sections" and len(parts) > 2:
        secs = [s.strip().lower()
                for s in parts[2].split(",")
                if s.strip().lower() in ("donghua", "anime")]
        if not secs:
            return await e.respond("📌 Usage: /autodl sections "
                                     "donghua,anime")
        st["sections"] = secs
        await autodl.save_settings(st)
        await e.respond(f"✅ Watching sections: "
                        f"<b>{', '.join(secs)}</b>",
                        parse_mode="html")
    elif sub == "reset":
        if not is_owner(e.sender_id):
            return await e.respond("👑 Owner only.")
        await autodl.registry_reset()
        await e.respond("♻️ <b>Download registry cleared</b> — "
                        "everything can be downloaded again.",
                        parse_mode="html")
    else:
        items = await autodl.fetch_latest(st["sections"])
        latest = items[0] if items else {}
        await e.respond(
            f"🤖 <b>AutoDL Status</b>\n\n"
            f"State: <b>{'🟢 ON' if st['enabled'] else '🔴 OFF'}</b>\n"
            f"Sections: <b>{', '.join(st['sections'])}</b>\n"
            f"Qualities: <b>{', '.join(st['qualities']) or 'all'}</b>\n"
            f"Interval: <b>{st['interval']} min</b>\n"
            f"Registry: <b>{await autodl.registry_size()}</b> "
            f"archived files\n"
            f"Latest on API: <b>"
            f"{escape_html(latest.get('title', '?'))}</b>",
            parse_mode="html")


# ---------------------------------------------------------------------- #
# /checknew — tap-to-select menu, then the queue runs the chosen ones
# ---------------------------------------------------------------------- #
SEL = {}  # message id -> {"fresh": [...], "chosen": set(), "by": uid}


def _sel_label(idx, item, chosen):
    series, ep = autodl.parse_api_title(item.get("title", ""))
    mark = "✅" if idx in chosen else "⬜"
    return f"{mark} {idx + 1}. {series[:22]} E{ep or '?'}"


def _sel_text(fresh, chosen):
    n_files = sum(len(todo) for _, todo, _ in fresh)
    lines = [f"🆕 <b>{len(fresh)} new episode(s)</b> · "
             f"{n_files} file(s) — tap to select", ""]
    for i, (item, todo, _) in enumerate(fresh):
        series, ep = autodl.parse_api_title(item.get("title", ""))
        mark = "✅" if i in chosen else "⬜"
        lines.append(f"{mark} <b>{i + 1}.</b> "
                     f"{escape_html(series)} Eps {ep} "
                     f"({len(todo)} file(s))")
    lines += ["", f"Selected: <b>{len(chosen)}</b> — press "
                  f"▶ Start to queue them."]
    return "\n".join(lines)


def _sel_buttons(fresh, chosen):
    rows = []
    for i in range(0, len(fresh), 2):
        row = []
        for j in (i, i + 1):
            if j < len(fresh):
                row.append(Button.inline(
                    _sel_label(j, fresh[j][0], chosen),
                    f"sel:{j}".encode()))
        rows.append(row)
    rows.append([Button.inline("✅ Select all", b"selall"),
                 Button.inline("⬜ Clear", b"selnone")])
    rows.append([Button.inline(f"▶ Start ({len(chosen)})",
                               b"seldone"),
                 Button.inline("❌ Cancel", b"selcancel")])
    return rows


@app.on(events.NewMessage(pattern=r"^/checknew$"))
async def cmd_checknew(e):
    if not await require_staff(e):
        return
    m = await e.respond("🔍 <b>Checking for new episodes…</b>",
                        parse_mode="html")
    fresh = await autodl.check_new()
    if not fresh:
        return await m.edit(
            "✅ <b>No new episodes</b> — everything the API "
            "lists is already in the registry.",
            parse_mode="html")
    SEL[m.id] = {"fresh": fresh, "chosen": set(),
                 "by": e.sender_id}
    await m.edit(_sel_text(fresh, set()),
                 buttons=_sel_buttons(fresh, set()),
                 parse_mode="html")


@app.on(events.CallbackQuery(pattern=rb"sel:(\d+)"))
async def cb_sel_toggle(e):
    st = SEL.get(e.message_id)
    if not st or e.sender_id != st["by"]:
        return await e.answer("Not your menu", alert=True)
    idx = int(e.pattern_match.group(1).decode())
    if idx in st["chosen"]:
        st["chosen"].discard(idx)
    else:
        st["chosen"].add(idx)
    await e.edit(_sel_text(st["fresh"], st["chosen"]),
                 buttons=_sel_buttons(st["fresh"], st["chosen"]),
                 parse_mode="html")


@app.on(events.CallbackQuery(pattern=rb"sel(all|none)"))
async def cb_sel_all(e):
    st = SEL.get(e.message_id)
    if not st or e.sender_id != st["by"]:
        return await e.answer("Not your menu", alert=True)
    if e.pattern_match.group(1).decode() == "all":
        st["chosen"] = set(range(len(st["fresh"])))
    else:
        st["chosen"] = set()
    await e.edit(_sel_text(st["fresh"], st["chosen"]),
                 buttons=_sel_buttons(st["fresh"], st["chosen"]),
                 parse_mode="html")


@app.on(events.CallbackQuery(pattern=rb"selcancel"))
async def cb_sel_cancel(e):
    SEL.pop(e.message_id, None)
    await e.edit("❌ Selection cancelled.", parse_mode="html",
                 buttons=None)


@app.on(events.CallbackQuery(pattern=rb"seldone"))
async def cb_sel_done(e):
    st = SEL.pop(e.message_id, None)
    if not st or e.sender_id != st["by"]:
        return await e.answer("Not your menu", alert=True)
    if not st["chosen"]:
        return await e.answer("Select at least one episode first",
                              alert=True)
    picked = [st["fresh"][i] for i in sorted(st["chosen"])]
    n_files = sum(len(t) for _, t, _ in picked)
    await e.edit(
        f"⏳ <b>Queue started</b> — {len(picked)} episode(s), "
        f"{n_files} file(s), one by one.\n\n"
        f"📩 Live progress panel is in the owner's DM.",
        parse_mode="html", buttons=None)

    async def _run():
        try:
            done = await autodl.start_batch(picked)
            try:
                hint = ("" if done >= len(picked) else
                        "\n\n⚠️ Some episodes were skipped — usually "
                        "means no DB channel is set (/dbchannels) or "
                        "MongoDB was offline; see Koyeb logs.")
                await e.respond(
                    f"✅ <b>Batch done!</b> Posted {done}/{len(picked)} "
                    f"selected episode(s) — archived in the DB channel "
                    f"&amp; registered in MongoDB." + hint,
                    parse_mode="html")
            except Exception:
                pass
        except Exception:
            import traceback
            tb = traceback.format_exc()
            print("[autodl] selection batch crashed:\n", tb)
            try:
                await autodl._owner_dm(
                    "❌ <b>Batch crashed</b>\n<pre>"
                    + tb[-900:] + "</pre>", )
            except Exception:
                pass
    asyncio.ensure_future(_run())


@app.on(events.NewMessage(pattern=r"^/status$"))
async def cmd_status(e):
    """Live AutoDL/transfer status — works for staff anywhere."""
    if not await require_staff(e):
        return
    from telethon import Button
    st = await autodl.get_settings()
    qs = st.get("qualities") or []
    head = (f"🤖 <b>AutoDL:</b> {'🟢 on' if st.get('enabled') else '🔴 off'}"
            f" · every {st.get('interval', 20)} min\n"
            f"🎞 Qualities: {'ALL the API offers' if not qs else ', '.join(qs)}\n"
            f"{autodl.telethon_info()}\n\n")
    m = await e.respond(head + autodl.job_panel(autodl.CURRENT),
                        parse_mode="html",
                        buttons=[Button.inline("🔄 Refresh", b"jrefresh")])



@app.on(events.CallbackQuery(pattern=rb"jrefresh"))
async def cb_jrefresh(e):
    if not await require_staff(e):
        return
    st = await autodl.get_settings()
    qs = st.get("qualities") or []
    head = (f"🤖 <b>AutoDL:</b> {'🟢 on' if st.get('enabled') else '🔴 off'}"
            f" · every {st.get('interval', 20)} min\n"
            f"🎞 Qualities: {'ALL the API offers' if not qs else ', '.join(qs)}\n"
            f"{autodl.telethon_info()}\n\n")
    try:
        await e.edit(head + autodl.job_panel(autodl.CURRENT),
                     parse_mode="html",
                     buttons=[Button.inline("🔄 Refresh", b"jrefresh")])
    except Exception:
        pass  # MessageNotModified = nothing changed, fine
    await e.answer("Refreshed")


@app.on(events.NewMessage(pattern=r"^/cancel_([a-f0-9]{4,16})$"))
async def cmd_cancel_job(e):
    """Owner-only: stop a running/queued AutoDL job."""
    from core import is_owner
    if not is_owner(e.sender_id):
        return await e.respond("👑 Owner only.")
    jid = e.pattern_match.group(1)
    job = autodl.JOBS.get(jid)
    if not job or job["status"] != "running":
        return await e.respond(f"❓ No running job "
                               f"<code>{jid}</code>.",
                               parse_mode="html")
    job["cancel"] = True
    await e.respond(f"🛑 Cancelling <code>{jid}</code> — the queue "
                    f"moves to the next file.", parse_mode="html")


@app.on(events.CallbackQuery(pattern=rb"qdl2:([\da-f]{4,16}):([\w\-]+)"))
async def cb_quality_range(e):
    """Quality button on a designed poster post — serves every
    episode of that quality in the post's range (force-sub gated)."""
    from core import forcesub_check
    pid = e.pattern_match.group(1).decode()
    quality = e.pattern_match.group(2).decode()

    ok, fs = await forcesub_check(e.sender_id)
    if not ok:
        await e.answer("🔒 Join the channel first", alert=True)
        return await forcesub_gate(e, fs)

    doc = await store.col("posts").find_one({"_id": pid})
    if not doc:
        return await e.answer("❌ Post not found", alert=True)
    sent = 0
    for ep in doc.get("eps", []):
        rec = await autodl.registry_get(doc["norm"], ep, quality)
        if rec and await serve_file(e.sender_id, rec):
            sent += 1
    # never post anything publicly in the channel — toast only
    await e.answer(f"📥 {sent} file(s) sent to your DM"
                   if sent else "❌ Files unavailable right now",
                   alert=not sent)


@app.on(events.CallbackQuery(pattern=rb"qdl:(\d+):([\w\-]+)"))
async def cb_quality(e):
    """Quality button in a channel post — force-sub gated."""
    from core import forcesub_check
    api_id = int(e.pattern_match.group(1).decode())
    quality = e.pattern_match.group(2).decode()

    ok, fs = await forcesub_check(e.sender_id)
    if not ok:
        await e.answer("🔒 Join the channel first", alert=True)
        return await forcesub_gate(e, fs)

    # registry first (AutoDL files), then the manual store
    rec = await autodl.registry_find_api(api_id, quality)
    if not rec:
        rec = await store.find_file_by_api(api_id, quality)
    if not rec:
        return await e.answer("❌ That quality isn't stored yet",
                              alert=True)
    await e.answer("📥 Sending…")
    got = await serve_file(e.sender_id, rec)
    if got:
        await e.respond("📥 <b>File sent!</b> Check your DM 👇",
                        parse_mode="html")
    else:
        link = rec.get("file_link")
        if link:
            await e.respond(
                f"🔗 <b>File Store link:</b>\n"
                f"{escape_html(link)}", parse_mode="html")
        else:
            await e.respond("❌ File unavailable right now — "
                            "ask an admin.", parse_mode="html")
