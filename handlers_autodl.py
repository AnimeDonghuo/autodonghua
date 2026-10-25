"""
AutoDL commands: /autodl, /checknew + quality-button callbacks.
"""
import asyncio
import re
import time

from telethon import events
from telethon.tl.custom import Button

import autodl
from core import (app, is_owner, require_staff, store, flood_respond,
                  flood_edit, vps_state)
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
        await flood_respond(e, 
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
        await flood_respond(e, "🤖 <b>AutoDL DISABLED</b>",
                        parse_mode="html")
    elif sub == "qualities" and len(parts) > 2:
        if parts[2].lower() == "all":
            st["qualities"] = []
            await autodl.save_settings(st)
            return await flood_respond(e, "✅ Qualities: <b>all</b> — every "
                                     "quality the API offers, per "
                                     "channel tag.",
                                     parse_mode="html")
        qs = [q.strip().lower() for q in parts[2].split(",")
              if q.strip()]
        if not qs:
            return await flood_respond(e, "📌 Usage: /autodl qualities "
                                     "360p,480p,720p,1080p")
        st["qualities"] = qs
        await autodl.save_settings(st)
        await flood_respond(e, f"✅ Download qualities set: "
                        f"<b>{', '.join(qs)}</b>",
                        parse_mode="html")
    elif sub == "interval" and len(parts) > 2 and parts[2].isdigit():
        st["interval"] = max(5, int(parts[2]))
        await autodl.save_settings(st)
        await flood_respond(e, f"✅ Poll interval: <b>{st['interval']} "
                        f"min</b>", parse_mode="html")
    elif sub == "sections" and len(parts) > 2:
        secs = [s.strip().lower()
                for s in parts[2].split(",")
                if s.strip().lower() in ("donghua", "anime")]
        if not secs:
            return await flood_respond(e, "📌 Usage: /autodl sections "
                                     "donghua,anime")
        st["sections"] = secs
        await autodl.save_settings(st)
        await flood_respond(e, f"✅ Watching sections: "
                        f"<b>{', '.join(secs)}</b>",
                        parse_mode="html")
    elif sub == "reset":
        if not is_owner(e.sender_id):
            return await flood_respond(e, "👑 Owner only.")
        await autodl.registry_reset()
        await flood_respond(e, "♻️ <b>Download registry cleared</b> — "
                        "everything can be downloaded again.",
                        parse_mode="html")
    else:
        items = await autodl.fetch_latest(st["sections"])
        latest = items[0] if items else {}
        await flood_respond(e, 
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
SEL_EXEC = {}  # message id -> picked list awaiting the Koyeb/VPS choice


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
    m = await flood_respond(e, "🔍 <b>Checking for new episodes…</b>",
                        parse_mode="html")
    fresh = await autodl.check_new()
    if not fresh:
        if autodl.API_ERR.get("ts"):
            return await m.edit(
                "⚠️ <b>Source API unreachable</b> — <code>"
                + str(autodl.API_ERR.get("err", ""))[:120]
                      .replace("&", "&amp;").replace("<", "&lt;")
                      .replace(">", "&gt;")
                + "</code>\nNothing can be checked until it "
                  "responds; the watcher retries every cycle.",
                parse_mode="html")
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
    await flood_edit(e, _sel_text(st["fresh"], st["chosen"]),
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
    await flood_edit(e, _sel_text(st["fresh"], st["chosen"]),
                 buttons=_sel_buttons(st["fresh"], st["chosen"]),
                 parse_mode="html")


@app.on(events.CallbackQuery(pattern=rb"selcancel"))
async def cb_sel_cancel(e):
    SEL.pop(e.message_id, None)
    await flood_edit(e, "❌ Selection cancelled.", parse_mode="html",
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
    vst = await vps_state()
    if vst.get("secret") and vst.get("ssh"):
        # a VPS is registered → the owner picks the executor first
        if len(SEL_EXEC) > 32:
            SEL_EXEC.pop(next(iter(SEL_EXEC)), None)
        SEL_EXEC[e.message_id] = picked

        def _lane(free):
            return ("🟢 idle — starts instantly" if free else
                    "⏳ busy — queues behind its current task")
        await flood_edit(e,
            f"⏳ <b>Queue ready</b> — {len(picked)} episode(s), "
            f"{n_files} file(s).\n\n"
            f"🛰 <b>Run it on…</b>\n"
            f"☁️ <b>Koyeb</b> — downloads+uploads here · "
            f"{_lane(not autodl.LOAD.locked())}\n"
            f"🖥 <b>VPS</b> — your worker does it · "
            f"{_lane(not autodl.LOAD_V.locked())} "
            + ("(online ✅)" if vst.get("online") else
               "(OFFLINE ⚠️ — falls back to Koyeb per file)"),
            parse_mode="html",
            buttons=[[Button.inline("☁️ Koyeb", b"selexec:koyeb"),
                      Button.inline("🖥 VPS", b"selexec:vps")]])
        return
    await _start_picked(e, picked, "auto")


async def _start_picked(e, picked, executor="auto"):
    """Kick the selected batch and report when it finishes."""
    n_files = sum(len(t) for _, t, _ in picked)
    where = {"koyeb": "☁️ Koyeb", "vps": "🖥 VPS worker",
             "auto": "auto (VPS if online, else Koyeb)"}[executor]
    # the picked machine idle → it starts RIGHT NOW even while the
    # other machine is still busy with its own batch
    if autodl.lane_free(executor):
        head = (f"🚀 <b>Starting now on {where}</b> — that server "
                f"is idle, so nothing waits.")
    else:
        head = (f"⏳ <b>Queued on {where}</b> — it finishes its "
                f"current task first, then yours starts "
                f"automatically.")
    await flood_edit(e,
        f"{head}\n\n{len(picked)} episode(s), {n_files} file(s), "
        f"one by one.\n\n"
        f"📩 Live progress panel is in the owner's DM.",
        parse_mode="html", buttons=None)

    async def _run():
        try:
            done = await autodl.start_batch(picked,
                                            executor=executor)
            try:
                hint = ("" if done >= len(picked) else
                        "\n\n⚠️ Some episodes were skipped — usually "
                        "means no DB channel is set (/dbchannels) or "
                        "MongoDB was offline; see Koyeb logs.")
                await flood_respond(e, 
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


@app.on(events.CallbackQuery(pattern=rb"selexec:(\w+)"))
async def cb_sel_exec(e):
    """Executor choice for a ready queue: ☁️ Koyeb or 🖥 VPS."""
    picked = SEL_EXEC.pop(e.message_id, None)
    if picked is None:
        return await e.answer("Menu expired — run /checknew again",
                              alert=True)
    await e.answer()
    await _start_picked(e, picked, e.pattern_match.group(1).decode())


@app.on(events.NewMessage(pattern=r"^/parallel(?: (on|off))?$"))
async def cmd_parallel(e):
    """Owner-only: run one file on Koyeb AND one on the VPS at the
    same time during batches."""
    if not is_owner(e.sender_id):
        return await flood_respond(e, "👑 Owner only.")
    sub = (e.pattern_match.group(1) or "").lower()
    cur = bool(await store.get_config("parallel", False))
    if sub:
        cur = (sub == "on")
        await store.set_config("parallel", cur)
    st = await vps_state()
    if cur:
        body = ("Batches run TWO files at once — one on ☁️ Koyeb, "
                "one on 🖥 your VPS (two different bots = two "
                "upload lanes).\nWorker right now: "
                + ("online ✅" if st.get("online") else
                   "OFFLINE — parallel falls back to one-by-one "
                   "until it beats again."))
    else:
        body = ("Normal mode: one file at a time (VPS when the mode "
                "is ON and the worker is online, else Koyeb).")
    await flood_respond(e, f"🔀 <b>Parallel mode: "
                           f"{'ON' if cur else 'OFF'}</b>\n\n{body}",
                        parse_mode="html")


# ---------------------------------------------------------------------- #
# /quality · /qualityindo · /qualityeng — which qualities may download
# ---------------------------------------------------------------------- #
QUALS = ["360p", "480p", "720p", "1080p"]
QKIND = {"i": ("qualities", "INDO (base 360p…1080p)", "🇮🇩 Indo"),
         "e": ("qualities_eng", "ENG (-eng flavours)", "🇬🇧 Eng")}


async def _q_get(kind):
    """(settings, key, current list, ticked set). An EMPTY list means
    'all qualities' — the convention /autodl has always used."""
    st = await autodl.get_settings()
    key = QKIND[kind][0]
    cur = [str(q).lower() for q in (st.get(key) or [])]
    ticks = set(QUALS) if (not cur or "all" in cur) \
        else {q for q in QUALS if q in cur}
    return st, key, cur, ticks


def _q_text(kind, cur, ticks):
    allq = not cur or "all" in cur
    lines = [f"🎞 <b>Download qualities — {QKIND[kind][1]}</b>", ""]
    for q in QUALS:
        lines.append(("✅ " if q in ticks else "⬜ ") + f"<b>{q}</b>")
    lines += [
        "",
        ("🌐 Right now: <b>ALL</b> of them." if allq else
         "🌐 Right now: <b>" + ", ".join(
             q for q in QUALS if q in ticks) + "</b>"),
        "",
        "Unticked qualities are never downloaded, never archived and "
        "never posted — for the watcher AND every /checknew, "
        "/searchad, /syncbutton batch.",
        "📌 Applies to <b>new</b> episodes from now on. Old episodes "
        "are NOT re-downloaded for a quality you tick later — use "
        "/searchad (one episode) or /syncbutton (eng sweep) when you "
        "want that on purpose.",
    ]
    return "\n".join(lines)


async def _q_render(kind):
    st, key, cur, ticks = await _q_get(kind)
    rows = []
    for i in range(0, len(QUALS), 2):
        rows.append([
            Button.inline(
                ("✅ " if q in ticks else "⬜ ") + q,
                f"qsel:{kind}:{q}".encode())
            for q in QUALS[i:i + 2]])
    allq = not cur or "all" in cur
    rows.append([Button.inline(
        ("✅ " if allq else "⬜ ") + "🌐 All qualities",
        f"qselall:{kind}".encode())])
    rows.append([Button.inline("🔙 Back", f"qhub".encode()),
                 Button.inline("✔️ Done", f"qseldone:{kind}".encode())])
    return _q_text(kind, cur, ticks), rows


@app.on(events.NewMessage(pattern=r"^/quality$"))
async def cmd_quality(e):
    """Owner/staff hub: pick which qualities may be downloaded."""
    if not await require_staff(e):
        return
    st = await autodl.get_settings()

    def show(sel):
        sel = [str(q).lower() for q in (sel or [])]
        return ("ALL" if (not sel or "all" in sel)
                else ", ".join(q for q in QUALS if q in sel) or "none")
    await flood_respond(
        e, "🎞 <b>Download qualities</b>\n\n"
           f"🇮🇩 Indo (base): <b>{show(st.get('qualities'))}</b>\n"
           f"🇬🇧 Eng (-eng): <b>{show(st.get('qualities_eng'))}</b>\n\n"
           "Tap one to choose exactly which qualities the bot may "
           "download. It is saved and applies to the watcher and to "
           "every batch from now on — a quality you never tick is "
           "never fetched (no more doomed 720p jobs).\n\n"
           "📌 New episodes only: ticking a quality later does NOT "
           "drag old episodes back into the queue.",
        parse_mode="html",
        buttons=[[Button.inline("🇮🇩 Indo qualities", b"qopen:i"),
                  Button.inline("🇬🇧 Eng qualities", b"qopen:e")]])


@app.on(events.NewMessage(pattern=r"^/qualityindo$"))
async def cmd_quality_indo(e):
    if not await require_staff(e):
        return
    text, rows = await _q_render("i")
    await flood_respond(e, text, parse_mode="html", buttons=rows)


@app.on(events.NewMessage(pattern=r"^/qualityeng$"))
async def cmd_quality_eng(e):
    if not await require_staff(e):
        return
    text, rows = await _q_render("e")
    await flood_respond(e, text, parse_mode="html", buttons=rows)


@app.on(events.CallbackQuery(pattern=rb"qopen:([ie])"))
async def cb_q_open(e):
    if not await require_staff(e):
        return
    await e.answer()
    kind = e.pattern_match.group(1).decode()
    text, rows = await _q_render(kind)
    await flood_edit(e, text, parse_mode="html", buttons=rows)


@app.on(events.CallbackQuery(pattern=rb"qsel:([ie]):([\w\-]+)"))
async def cb_q_toggle(e):
    if not await require_staff(e):
        return
    kind = e.pattern_match.group(1).decode()
    q = e.pattern_match.group(2).decode().lower()
    if q not in QUALS:
        return await e.answer("Unknown quality", alert=True)
    st, key, cur, ticks = await _q_get(kind)
    if q in ticks:
        ticks.discard(q)
        if not ticks:
            return await e.answer(
                "Keep at least one quality — or tap 🌐 All", alert=True)
    else:
        ticks.add(q)
    new = [x for x in QUALS if x in ticks]
    st[key] = [] if len(new) == len(QUALS) else new   # 4 ticks = ALL
    await autodl.save_settings(st)
    await e.answer(("✅ " if q in ticks else "⬜ ") + q +
                   (" — removed" if q not in ticks else " — added"))
    text, rows = await _q_render(kind)
    await flood_edit(e, text, parse_mode="html", buttons=rows)


@app.on(events.CallbackQuery(pattern=rb"qselall:([ie])"))
async def cb_q_all(e):
    if not await require_staff(e):
        return
    kind = e.pattern_match.group(1).decode()
    st, key, cur, ticks = await _q_get(kind)
    st[key] = []                      # empty = every quality offered
    await autodl.save_settings(st)
    await e.answer("🌐 All qualities")
    text, rows = await _q_render(kind)
    await flood_edit(e, text, parse_mode="html", buttons=rows)


@app.on(events.CallbackQuery(pattern=rb"qseldone:([ie])"))
async def cb_q_done(e):
    if not await require_staff(e):
        return
    kind = e.pattern_match.group(1).decode()
    st, key, cur, ticks = await _q_get(kind)
    label = QKIND[kind][2]
    shown = "ALL" if (not cur or "all" in cur) else ", ".join(
        q for q in QUALS if q in ticks)
    await e.answer("Saved")
    await flood_edit(
        e, f"✅ <b>{label} qualities saved: {escape_html(shown)}</b>\n\n"
           "Every new download from now on obeys it — the watcher, "
           "/checknew, /searchad and /syncbutton.\n"
           "📌 Old episodes are NOT re-downloaded for a quality you "
           "just ticked; /searchad does one episode on purpose.",
        parse_mode="html", buttons=None)


@app.on(events.CallbackQuery(pattern=rb"qhub$"))
async def cb_q_hub(e):
    if not await require_staff(e):
        return
    await e.answer()
    st = await autodl.get_settings()

    def show(sel):
        sel = [str(q).lower() for q in (sel or [])]
        return ("ALL" if (not sel or "all" in sel)
                else ", ".join(q for q in QUALS if q in sel) or "none")
    await flood_edit(
        e, "🎞 <b>Download qualities</b>\n\n"
           f"🇮🇩 Indo (base): <b>{show(st.get('qualities'))}</b>\n"
           f"🇬🇧 Eng (-eng): <b>{show(st.get('qualities_eng'))}</b>\n\n"
           "Tap one to choose exactly which qualities the bot may "
           "download — saved, and applied to every new download.",
        parse_mode="html",
        buttons=[[Button.inline("🇮🇩 Indo qualities", b"qopen:i"),
                  Button.inline("🇬🇧 Eng qualities", b"qopen:e")]])


async def _status_head():
    """Shared /status header: the AutoDL block PLUS the VPS worker's
    own stats whenever one is registered — Koyeb side unchanged."""
    st = await autodl.get_settings()
    qs = st.get("qualities") or []
    qe = st.get("qualities_eng") or []

    def _qtxt(sel):
        return "ALL" if not sel else ", ".join(sel)
    head = (f"🤖 <b>AutoDL:</b> {'🟢 on' if st.get('enabled') else '🔴 off'}"
            f" · every {st.get('interval', 20)} min\n"
            f"🎞 Qualities: 🇮🇩 {_qtxt(qs)} · 🇬🇧 {_qtxt(qe)} "
            f"(/quality)\n"
            f"{autodl.telethon_info()} · build {autodl.BUILD}\n")
    w = await store.get_config("watch") or {}
    if w.get("ts"):
        age = max(0, int((time.time() - w["ts"]) / 60))
        if w.get("err"):
            res = ("❌ " + str(w["err"])[:90].replace("&", "&amp;")
                                 .replace("<", "&lt;")
                                 .replace(">", "&gt;"))
        elif (w.get("fresh") or 0) > 0:
            res = f"🆕 {w['fresh']} episode(s) missing → queued"
        else:
            res = "registry up to date, nothing new to fetch"
        head += f"👀 Last check: {age} min ago — {res}\n"
    v = await vps_state()
    if v.get("secret"):
        caps = v.get("caps") or {}
        ssh = v.get("ssh") or {}
        gb = (v.get("bytes") or 0) / 1073741824
        head += (
            "\n🖥 <b>VPS worker:</b> "
            + ("🟢 online" if v.get("online") else "🔴 OFFLINE")
            + (f" (beat {int(time.time() - v['beat'])}s ago)"
               if v.get("beat") else "")
            + f" · mode {'ON' if v.get('enabled') else 'off'}\n"
            f"🖥 py {caps.get('py', '?')} · worker build "
            f"{caps.get('build', '?')} · disk "
            f"{caps.get('disk_mb', '?')} MB free · "
            f"{ssh.get('user', 'root')}@{ssh.get('host', '?')}\n"
            f"🖥 done {v.get('jobs', 0)} job(s) · {gb:.2f} GB "
            f"uploaded · avg {v.get('speed', 0):.1f} MB/s\n"
            f"🔀 Parallel: <b>{'ON' if await store.get_config('parallel', False) else 'off'}</b>\n")
    return head + "\n"


@app.on(events.NewMessage(pattern=r"^/status$"))
async def cmd_status(e):
    """Live AutoDL/transfer status — works for staff anywhere."""
    if not await require_staff(e):
        return
    from telethon import Button
    head = await _status_head()
    m = await flood_respond(e, head + autodl.job_panel(autodl.CURRENT),
                            parse_mode="html",
                            buttons=[[Button.inline("🔄 Refresh",
                                                    b"jrefresh"),
                                      Button.inline("🛑 Cancel ALL",
                                                    b"jcancelall")]])
    if m is not None:  # THIS message becomes the one live panel;
        await autodl.set_live_panel(m.chat_id, m.id)  # old one dies



@app.on(events.CallbackQuery(pattern=rb"jrefresh"))
async def cb_jrefresh(e):
    if not await require_staff(e):
        return
    head = await _status_head()
    try:
        await flood_edit(e, head + autodl.job_panel(autodl.CURRENT),
                         parse_mode="html",
                         buttons=[[Button.inline("🔄 Refresh",
                                                 b"jrefresh"),
                                   Button.inline("🛑 Cancel ALL",
                                                 b"jcancelall")]])
        await autodl.set_live_panel(e.chat_id, e.message_id)
    except Exception:
        pass  # MessageNotModified = nothing changed, fine
    await e.answer("Refreshed")


@app.on(events.CallbackQuery(pattern=rb"jcancelall"))
async def cb_cancel_all(e):
    """🛑 Cancel ALL button on the live /status panel."""
    if not await require_staff(e):
        return
    n = autodl.cancel_all()
    await e.answer("Cancelling everything…")
    await flood_respond(
        e, f"🛑 <b>Cancel ALL</b> — flagged {n} running job(s); the "
           f"batch will not start any more queued files. The panel "
           f"shows the final tally.", parse_mode="html")


@app.on(events.NewMessage(pattern=r"^/cancelall$"))
async def cmd_cancel_all(e):
    """Owner-only: stop every running + queued AutoDL job."""
    from core import is_owner
    if not is_owner(e.sender_id):
        return await flood_respond(e, "👑 Owner only.")
    n = autodl.cancel_all()
    await flood_respond(
        e, f"🛑 <b>Cancel ALL</b> — flagged {n} running job(s); no "
           f"queued file will start. Running upload/download aborts "
           f"within a couple of seconds.", parse_mode="html")


@app.on(events.NewMessage(pattern=r"^/cancel_([a-f0-9]{4,16})$"))
async def cmd_cancel_job(e):
    """Owner-only: stop a running/queued AutoDL job."""
    from core import is_owner
    if not is_owner(e.sender_id):
        return await flood_respond(e, "👑 Owner only.")
    jid = e.pattern_match.group(1)
    job = autodl.JOBS.get(jid)
    if not job or job["status"] != "running":
        return await flood_respond(e, f"❓ No running job "
                               f"<code>{jid}</code>.",
                               parse_mode="html")
    job["cancel"] = True
    await flood_respond(e, f"🛑 Cancelling <code>{jid}</code> — the queue "
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
        await flood_respond(e, "📥 <b>File sent!</b> Check your DM 👇",
                        parse_mode="html")
    else:
        link = rec.get("file_link")
        if link:
            await flood_respond(e, 
                f"🔗 <b>File Store link:</b>\n"
                f"{escape_html(link)}", parse_mode="html")
        else:
            await flood_respond(e, "❌ File unavailable right now — "
                            "ask an admin.", parse_mode="html")


# ---------------------------------------------------------------------- #
# /searchad — force re-download & re-upload any episode, any quality
# ---------------------------------------------------------------------- #
SELAD = {}


def _sad_parse(arg: str):
    """'Name 09' / 'Name Eps 09' / 'Name eps 9' -> (Name, 9)."""
    m = re.search(r"\s(\d{1,4})$", (arg or "").strip())
    if not m:
        return "", None
    name = arg[:m.start()].strip()
    name = re.sub(r"\s+eps?\.?$", "", name, flags=re.I).strip()
    return name, int(m.group(1))


def _sad_text(name, ep, streams, chosen):
    lines = [f"🔎 <b>{escape_html(name)}</b> Ep {ep} — both APIs "
             f"checked. Tap streams to (de)select, then ✅ Done:"]
    for i, (sec, it, s) in enumerate(streams):
        q = s.get("quality", "")
        norm = autodl.normalize_key(it.get("series") or name)
        done = (" · already archived"
                if autodl.already_downloaded(norm, ep, q) else "")
        mark = "✅" if i in chosen else "⬜️"
        lines.append(f"{mark} {i + 1}. {sec} · {q}{done}")
    return "\n".join(lines)


def _sad_buttons(streams, chosen):
    btns = [Button.inline(
        f"{'✅' if i in chosen else '⬜️'} {sec[:4]} {s.get('quality', '')}",
        f"sad:{i}".encode())
        for i, (sec, it, s) in enumerate(streams)]
    rows = [btns[i:i + 2] for i in range(0, len(btns), 2)]
    rows.append([Button.inline("✅ Done — force download", b"saddone"),
                 Button.inline("❌ Cancel", b"sadcancel")])
    return rows


@app.on(events.NewMessage(pattern=r"^/searchad(?:\s+(.+))?$"))
async def cmd_searchad(e):
    """Admin: look up ONE episode on BOTH apis and force a fresh
    download+upload of whatever streams you pick (registry ignored)."""
    if not await require_staff(e):
        return
    arg = (e.pattern_match.group(1) or "").strip()
    name, ep = _sad_parse(arg)
    if not arg or not ep:
        return await flood_respond(e, 
            "📌 Usage: <code>/searchad Name 09</code> — series name "
            "plus episode number. Checks the donghua AND anime API, "
            "lists every stream; picked ones are re-downloaded and "
            "re-uploaded even if already archived.",
            parse_mode="html")
    found, streams = [], []
    for sec in ("donghua", "anime"):
        it = await autodl._search_episode(sec, name, ep)
        if it:
            found.append(sec)
            streams += [(sec, it, s) for s in it.get("streams", [])]
    if not streams:
        return await flood_respond(e, 
            f"❌ Nothing found for <b>{escape_html(name)}</b> Ep {ep} "
            "in the donghua or anime API.", parse_mode="html")
    msg = await flood_respond(e, _sad_text(name, ep, streams, set()),
                          parse_mode="html",
                          buttons=_sad_buttons(streams, set()))
    SELAD[msg.id] = {"streams": streams, "chosen": set(),
                     "by": e.sender_id, "name": name, "ep": ep,
                     "found": found}


@app.on(events.CallbackQuery(pattern=rb"sad:(\d+)"))
async def cb_sad_toggle(e):
    st = SELAD.get(e.message_id)
    if not st or e.sender_id != st["by"]:
        return await e.answer("Not your menu", alert=True)
    i = int(e.pattern_match.group(1))
    if i in st["chosen"]:
        st["chosen"].discard(i)
    else:
        st["chosen"].add(i)
    await flood_edit(e, _sad_text(st["name"], st["ep"], st["streams"],
                           st["chosen"]),
                 parse_mode="html",
                 buttons=_sad_buttons(st["streams"], st["chosen"]))


@app.on(events.CallbackQuery(pattern=rb"sadcancel"))
async def cb_sad_cancel(e):
    SELAD.pop(e.message_id, None)
    await flood_edit(e, "❌ Search cancelled.", parse_mode="html",
                 buttons=None)


@app.on(events.CallbackQuery(pattern=rb"saddone"))
async def cb_sad_done(e):
    st = SELAD.pop(e.message_id, None)
    if not st or e.sender_id != st["by"]:
        return await e.answer("Not your menu", alert=True)
    if not st["chosen"]:
        return await e.answer("Select at least one stream first",
                              alert=True)
    picked = {}
    for i in sorted(st["chosen"]):
        sec, it, s = st["streams"][i]
        slot = picked.setdefault(sec, {"item": it, "todo": []})
        slot["todo"].append(s)
    entries = [(picked[sec]["item"], picked[sec]["todo"],
                picked[sec]["item"].get("streams", []))
               for sec in picked]
    n = sum(len(t) for _, t, _ in entries)
    await flood_edit(e, 
        f"⏳ <b>Force queue started</b> — {n} file(s) for "
        f"{escape_html(st['name'])} Ep {st['ep']} will be downloaded "
        "and uploaded again even though they may already be "
        "archived. Cards get their quality buttons topped up "
        "automatically.\n\n📩 Live progress panel is in the owner's "
        "DM.", parse_mode="html", buttons=None)

    async def _run():
        try:
            await autodl.start_batch(entries, force=True)
        except Exception as exc:
            print("[autodl] searchad batch failed:", repr(exc)[:200])
    asyncio.ensure_future(_run())


@app.on(events.NewMessage(pattern=r"^/syncbutton(?:\s|$)"))
async def cmd_syncbutton(e):
    """Staff: after the in-flight episode/batch completes, sweep every
    channel card — top up missing quality buttons, and for indo cards
    whose eng sub has since landed on the search API, download and
    upload it now. Queue is paused while the sweep runs."""
    if not await require_staff(e):
        return
    if autodl.busy():
        autodl.SYNC_REQ["on"] = True
        return await flood_respond(
            e, "🔄 <b>Sync queued</b> — the episode currently "
               "downloading will finish all its qualities first, "
               "then every card's buttons get topped up and missing "
               "eng subs are fetched from the search API. Summary "
               "lands in your DM.", parse_mode="html")

    async def _run():
        async with autodl.LOAD_V, autodl.LOAD:
            await autodl.sync_buttons_pass(
                locks=(autodl.LOAD_V, autodl.LOAD))
    asyncio.ensure_future(_run())
    await flood_respond(
        e, "🔄 <b>Sync started</b> — checking every channel card "
           "against the DB, topping up quality buttons and fetching "
           "any eng sub the search API now offers. Summary lands in "
           "your DM.", parse_mode="html")
