"""
Normal-user handlers: styled welcome, /request, force-subscribe gating,
free/paid tiers.
"""
from datetime import datetime, timezone

from telethon import events
from telethon.tl.custom import Button

from core import (app, bot_user, forcesub_check, invite_link,
                  is_admin, is_staff, member_status, mention,
                  store, url_bytes,
    flood_respond, flood_edit)
from sources import build_metadata, SOURCE_LABELS
from style import escape_html, welcome_text

FREE_LIMIT = 5      # requests per day for free users
PAID_LIMIT = 100    # requests per day for paid users


# ---------------------------------------------------------------------- #
# /start — staff sees the welcome + ALL commands as buttons;
# normal users get the styled welcome card
# ---------------------------------------------------------------------- #
STAFF_CMDS = [
    ("➕ Add Channel", "Send: /addchannel <id> [tag]"),
    ("🗑 Channels", "Send: /channels — list / delete"),
    ("🧲 Button Channel", "Send: /buttonchannel <link> <name>"),
    ("🔒 Force Sub", "Send: /forcesub strict|request <channel>"),
    ("💾 Backup", "Send: /backup"),
    ("♻️ Recover", "Send: /recover"),
    ("🗄 MongoDB", "Send: /addmongodb <uri>"),
    ("🗂 DB Channel", "Send: /setdbchannel <id>"),
    ("🗄 DB Channels", "Send: /adddbchannel <id> (multiple ok)"),
    ("🏷 Set Tag", "Send: /settag <channel_id> indo|eng"),
    ("🖼 Set Image", "Send: /setimg <title> <url>"),
    ("💎 Set Paid", "Send: /setpaid <user_id>"),
    ("👥 Users", "Send: /users"),
    ("📊 Usage", "Send: /usage"),
    ("📈 Stats", "Send: /stats"),
    ("📢 Broadcast", "Send: /broadcast <text>"),
    ("🤖 AutoDL", "Send: /autodl on"),
    ("🔍 Check New", "Send: /checknew"),
    ("🖼 Start Image", "Send: /setstartimg <url> or reply photo"),
    ("🔄 Sync Cmds", "Send: /sync"),
    ("🔑 Logout", "Send: /logout"),
]


def _staff_kb():
    rows = []
    for i in range(0, len(STAFF_CMDS), 3):
        chunk = STAFF_CMDS[i:i + 3]
        rows.append([Button.inline(label, f"cmd:{i+j}".encode())
                     for j, (label, _) in enumerate(chunk)])
    return rows


@app.on(events.NewMessage(pattern=r"^/start(?:\s|$)"))
async def cmd_start(e):
    sender = e.sender or await e.get_sender()
    await store.ensure_user(e.sender_id,
                            getattr(sender, "username", None),
                            getattr(sender, "first_name", None))
    # deep-link payload: t.me/<bot>?start=<b64 "get-…"> serves a file
    parts = e.raw_text.split()
    if len(parts) > 1:
        import autodl
        if await autodl.serve_deeplink(e, parts[1]):
            return
    user = await store.get_user(e.sender_id) or {}
    tier = user.get("tier", "free")
    tier_label = "💎 Paid" if tier == "paid" else "🆓 Free"
    uname = getattr(sender, "username", None)
    name = f"@{uname}" if uname else (
        getattr(sender, "first_name", None) or "Friend")

    buttons = []
    for b in await store.list_button_channels():
        buttons.append([Button.url(f"🔘 {b['_id']}", b["url"])])

    fs = await store.get_forcesub()
    fs_note = ""
    if (fs or {}).get("mode") not in (None, "off"):
        fs_note = ("\n\n🔒 <b>Force Subscribe is ON</b> — join "
                   "our channel to unlock files &amp; requests.")

    if await is_staff(e.sender_id):
        # ---- staff / admin / owner: welcome + every command
        text = (welcome_text(name, tier_label)
                + "\n\n🛡 <b>Staff Mode Active</b> — all commands:"
                + fs_note)
        cmd_rows = _staff_kb()
        buttons = (buttons + cmd_rows) if buttons else cmd_rows
        text += ("\n\n<b>Commands:</b>\n"
                 + "\n".join(f"• <code>{label.split(' ', 1)[0]}"
                             f"</code> — {hint.split(': ', 1)[-1]}"
                             for label, hint in STAFF_CMDS))
    else:
        # ---- normal user: the welcome card (banner + card + grid)
        bot_name = (getattr(bot_user, "first_name", None)
                    or "Auto Donghua Bot")
        credit = (await store.get_config("credit_line")) or (
            f"{bot_name} • Designed & Developed by AnimeDonghuo ✨")
        body = welcome_text(name, tier_label)
        text = (f"✨ <b>Wᴇʟᴄᴏᴍᴇ Tᴏ {escape_html(bot_name)}</b>\n"
                f"──────────────\n"
                f"<blockquote>{body}\n\n"
                f"Usᴇ ᴛʜᴇ ʙᴜᴛᴛᴏɴs ʙᴇʟᴏᴡ ᴛᴏ ɢᴇᴛ sᴛᴀʀᴛᴇᴅ!"
                f"</blockquote>\n"
                f"──────────────\n{credit}") + fs_note

        letters = [Button.inline(ch, f"dec:{i}".encode())
                   for i, ch in enumerate("WELCOME")]
        card_rows = [
            letters,
            [Button.inline("📋 Commands", b"usr:commands"),
             Button.inline("🔍 Sources", b"usr:sources")],
            [Button.inline("📤 Request", b"usr:request"),
             Button.inline("🗂 File Store", b"usr:search")],
            [Button.inline("📜 Changelog", b"usr:changelog"),
             Button.inline("❌ Close", b"usr:close")],
        ]
        chan_url = await _public_channel_url(fs)
        if chan_url:
            card_rows.append([Button.url("📢 Channel", chan_url)])
        buttons = (buttons + card_rows) if buttons else card_rows

    media = None
    img = await store.get_config("start_image")
    if img:
        try:
            if img.startswith(("http://", "https://")):
                import io as _io
                buf = _io.BytesIO(await url_bytes(img))
                buf.name = "start_banner.jpg"  # photo, not document
                media = buf
            else:
                media = img  # packed file_id
        except Exception:
            media = None
    await flood_respond(e, text, file=media, parse_mode="html",
                    buttons=buttons or None)


async def _public_channel_url(fs):
    """A joinable link for the card's Channel button."""
    try:
        chans_fs = (fs or {}).get("channels") or []
        if chans_fs:
            return await invite_link(chans_fs[0])
        chans = await store.list_channels()
        if chans:
            ent = await app.get_entity(chans[0]["_id"])
            if getattr(ent, "username", None):
                return f"https://t.me/{ent.username}"
            return await invite_link(ent.id)
    except Exception:
        return None


# ---------------------------------------------------------------------- #
# Force-subscribe gate message (2 modes)
# ---------------------------------------------------------------------- #
async def forcesub_gate(e, fs):
    mode = fs.get("mode")
    buttons = []
    for ch in fs.get("channels") or []:
        link = await invite_link(ch, join_request=(mode == "request"))
        label = ("📩 Send Join Request" if mode == "request"
                 else "🔗 Join Channel")
        if link:
            buttons.append([Button.url(label, link)])
    if not buttons:
        buttons.append([Button.inline("🔒 Contact Admin", b"fs:admin")])
    buttons.append([Button.inline("✅ I Joined / Requested — Check",
                                   b"fs:check")])
    msg = ("🔒 <b>Channel Required</b>\n\n"
           + ("You must <b>join</b> our channel first — then you'll get "
              "your files. 👇" if mode == "strict" else
              "You must <b>send a join request</b> to our channel — "
              "files unlock once approved. 👇"))
    await flood_respond(e, msg, buttons=buttons, parse_mode="html")


# ---------------------------------------------------------------------- #
# /request — request a specific episode / anime
# ---------------------------------------------------------------------- #
@app.on(events.NewMessage(pattern=r"^/request(?:\s|$)"))
async def cmd_request(e):
    sender = e.sender or await e.get_sender()
    uid = e.sender_id
    await store.ensure_user(uid, getattr(sender, "username", None),
                            getattr(sender, "first_name", None))

    # 1) force-subscribe gate
    ok, fs = await forcesub_check(uid)
    if not ok:
        return await forcesub_gate(e, fs)

    # 2) free / paid daily limits
    user = await store.get_user(uid) or {}
    tier = user.get("tier", "free")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if user.get("request_date") != today:
        await store.update_user(uid, {"$set": {"request_date": today,
                                                "requests_today": 0}})
        used = 0
    else:
        used = user.get("requests_today", 0)
    limit = PAID_LIMIT if tier == "paid" else FREE_LIMIT
    if used >= limit:
        return await flood_respond(e, 
            f"🚫 <b>Daily request limit reached</b> ({used}/{limit}).\n\n"
            f"💎 Upgrade to a <b>Paid</b> plan for {PAID_LIMIT} requests/day, "
            f"or come back tomorrow.", parse_mode="html")

    args = e.raw_text.split(None, 1)
    if len(args) < 2:
        return await flood_respond(e, 
            "📌 <b>Usage:</b> /request &lt;anime or donghua name&gt; [episode]\n\n"
            "Example: <code>/request Shrouding the Heavens 45</code>",
            parse_mode="html")

    query = args[1].strip()
    ep = None
    parts = query.rsplit(None, 1)
    if len(parts) == 2 and parts[1].isdigit():
        query, ep = parts[0], int(parts[1])

    m = await flood_respond(e, "🔎 <b>Searching AniList • AniLive • AnimXin • "
                        "LuciferDonghua…</b>", parse_mode="html")
    meta = await build_metadata(query,
                                anilive_base=await store.get_config(
                                    "anilive_base"))

    lines = [f"🎬 <b>{escape_html(meta['title'] or query)}</b>"]
    if ep:
        lines.append(f"📺 <b>Episode:</b> {ep}")
    if meta.get("year"):
        lines.append(f"📅 <b>Year:</b> {meta['year']}")
    if meta.get("score"):
        lines.append(f"⭐ <b>Score:</b> {meta['score']}/10")
    if meta.get("episodes"):
        lines.append(f"📺 <b>Total Episodes:</b> {meta['episodes']}")
    if meta.get("genres"):
        lines.append(f"🎭 <b>Genres:</b> {', '.join(meta['genres'][:5])}")
    lines.append(f"\n🛰 <b>Sources:</b> "
                 f"{', '.join(meta['sources']) or 'none responded'}")
    if meta.get("description"):
        lines.append(f"\n📖 {escape_html(meta['description'][:600])}")
    text = "\n".join(lines)

    buttons = []
    if meta.get("url"):
        buttons.append([Button.url("🔗 More Info", meta["url"])])

    img = None
    if meta.get("image"):
        try:
            from core import url_bytes
            img = await url_bytes(meta["image"])
        except Exception:
            img = None

    await store.inc_request(uid)
    try:
        if img:
            await m.edit(text, file=img, buttons=buttons or None,
                         parse_mode="html")
        else:
            await m.edit(text, buttons=buttons or None, parse_mode="html")
    except Exception:
        await m.edit(text + "\n\n<i>(poster unavailable)</i>",
                     parse_mode="html")

    # staff log
    log_ch = await store.get_config("log_channel")
    if log_ch:
        try:
            await app.send_message(
                log_ch,
                f"📥 <b>New Request</b>\n👤 {mention(uid, getattr(sender, 'first_name', None))}\n"
                f"🎬 {escape_html(query)}" + (f" — Ep {ep}" if ep else "")
                + f"\n💎 Tier: {tier}",
                parse_mode="html")
        except Exception:
            pass


# ---------------------------------------------------------------------- #
# /search + /get — fetch stored files (clean copy, no tag)
# ---------------------------------------------------------------------- #
@app.on(events.NewMessage(pattern=r"^/search(?:\s|$)"))
async def cmd_search(e):
    sender = e.sender or await e.get_sender()
    await store.ensure_user(e.sender_id, getattr(sender, "username", None),
                              getattr(sender, "first_name", None))
    ok, fs = await forcesub_check(e.sender_id)
    if not ok:
        return await forcesub_gate(e, fs)
    args = e.raw_text.split(None, 1)
    if len(args) < 2:
        return await flood_respond(e, "📌 <b>Usage:</b> /search &lt;anime name&gt; "
                                 "[episode]\nExample: <code>/search Soul Land "
                                 "5</code>", parse_mode="html")
    m = await flood_respond(e, "🔎 <b>Searching the file store…</b>",
                          parse_mode="html")
    recs = await store.search_files(args[1].strip(), limit=10)
    if not recs:
        return await m.edit("📭 <b>No files found</b> for that name yet.\n"
                            "Ask an admin, or use /request.",
                            parse_mode="html")
    from telethon.tl.custom import Button as _Btn
    from style import small_caps
    text = "🗂 <b>File Store Results</b>\n\n"
    kb = []
    for r in recs:
        ep = r.get("episode")
        label = f"🎬 {r.get('title', '?')}"
        if ep is not None:
            label += f" — Ep {ep}"
        label += f"  ·  {r.get('quality') or ''}"
        text += f"• {escape_html(label[:70])}\n"
        kb.append([_Btn.inline(f"📥 Get {r.get('title','?')[:30]}"
                                 + (f" Ep {ep}" if ep is not None else ""),
                                 f"getfile:{r['_id']}".encode())])
    await m.edit(text[:900], parse_mode="html", buttons=kb)


@app.on(events.NewMessage(pattern=r"^/get(?:\s|$)"))
async def cmd_get(e):
    sender = e.sender or await e.get_sender()
    await store.ensure_user(e.sender_id, getattr(sender, "username", None),
                              getattr(sender, "first_name", None))
    ok, fs = await forcesub_check(e.sender_id)
    if not ok:
        return await forcesub_gate(e, fs)
    args = e.raw_text.split(None, 1)
    if len(args) < 2:
        return await flood_respond(e, "📌 <b>Usage:</b> /get &lt;anime name&gt; "
                                 "[episode]", parse_mode="html")
    recs = await store.search_files(args[1].strip(), limit=1)
    if not recs:
        return await flood_respond(e, "📭 Not in the store yet. Use /search to "
                                 "browse, or /request to ask.",
                                 parse_mode="html")
    from handlers_files import serve_file
    got = await serve_file(e.sender_id, recs[0])
    await flood_respond(e, "📥 <b>File sent!</b> Check your downloads 👇"
                    if got else
                    "❌ File unavailable right now — ask an admin.",
                    parse_mode="html")


# ---------------------------------------------------------------------- #
# Unknown commands → styled notice for normal users
# ---------------------------------------------------------------------- #
KNOWN_COMMANDS = {
    "start", "help", "request", "search", "get", "login", "logout",
    "addchannel", "channels", "buttonchannel", "buttonchannels",
    "forcesub", "addmongodb", "mongos", "delmongodb", "backup",
    "recover", "dbchannel", "setdbchannel", "setlog", "setimg",
    "setpaid", "setpass", "addadmin", "deladmin", "admins",
    "users", "usage", "stats", "broadcast", "setsource", "sync",
    "autodl", "checknew", "setstartimg",
    "adddbchannel", "dbchannels", "deldbchannel", "settag",
    "status", "addmongo", "cancel",
    "addvps", "vps", "removevps", "searchad", "syncbutton", "setowner",
    "cancelall", "parallel", "fixsmall",
}


@app.on(events.NewMessage(pattern=r"^/(\w+)"))
async def unknown_cmd(e):
    cmd = e.pattern_match.group(1).lower()
    if cmd.startswith("cancel_"):
        return  # /cancel_<jobid> has its own handler
    if cmd in KNOWN_COMMANDS:
        return  # handled by its own handler
    if await is_admin(e.sender_id):
        return await flood_respond(e, "❓ Unknown command. Send /help.")
    from style import RESTRICTED_MSG
    await flood_respond(e, RESTRICTED_MSG, parse_mode="html")


# ---------------------------------------------------------------------- #
# Welcome-card buttons (normal users)
# ---------------------------------------------------------------------- #
USER_COMMANDS_TEXT = (
    "📋 <b>Cᴏᴍᴍᴀɴᴅs</b>\n\n"
    "• /request &lt;anime&gt; [ep] — request a file\n"
    "• /search &lt;name&gt; — search the file store\n"
    "• /get &lt;name&gt; [ep] — get a clean copy\n"
    "• /start — the welcome menu\n"
    "• /help — help")

SOURCES_TEXT = (
    "🔍 <b>Sᴏᴜʀᴄᴇs</b>\n\n"
    "• AniList\n• AnimXin\n• LuciferDonghua\n"
    "• MyAnimeList (Jikan)\n• AutoDL API (automatic posts)")

CHANGELOG_TEXT = (
    "📜 <b>Cʜᴀɴɢᴇʟᴏɢ</b>\n\n"
    "• AutoDL — new episodes download &amp; post automatically\n"
    "• File store — /search and /get clean copies\n"
    "• Quality buttons on every channel post\n"
    "• Force-subscribe gate on all file buttons\n"
    "• MongoDB registry — never re-downloads an episode")


@app.on(events.CallbackQuery(pattern=rb"dec:(\d)"))
async def cb_dec(e):
    await e.answer("✨ Wᴇʟᴄᴏᴍᴇ ✨")


@app.on(events.CallbackQuery(pattern=rb"usr:commands"))
async def cb_usr_commands(e):
    await flood_respond(e, USER_COMMANDS_TEXT, parse_mode="html")


@app.on(events.CallbackQuery(pattern=rb"usr:sources"))
async def cb_usr_sources(e):
    await flood_respond(e, SOURCES_TEXT, parse_mode="html")


@app.on(events.CallbackQuery(pattern=rb"usr:changelog"))
async def cb_usr_changelog(e):
    await flood_respond(e, CHANGELOG_TEXT, parse_mode="html")


@app.on(events.CallbackQuery(pattern=rb"usr:close"))
async def cb_usr_close(e):
    try:
        await e.delete()
    except Exception:
        pass
