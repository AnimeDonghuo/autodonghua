"""
Admin / owner commands.

/addchannel <id|@user> <tag>     — channel the bot posts files to
/buttonchannel <link> <name>     — buttons shown to normal users
/forcesub <off|strict|request> [channel]
/addmongodb <url>                — add MongoDB URI (persisted, multi-storage)
/mongos · /delmongodb <n>
/backup · /recover · /dbchannel <id> · /setlog <id>
/sync                            — sync command menu into the bot
/setimg <title> <url> · /setpaid <user_id> [paid|free]
/addadmin <id> · /deladmin <id> · /admins
/users · /usage · /stats · /broadcast · /setsource anilive <base>
"""
import asyncio
import hashlib
import json
import os
import secrets
import time

from telethon import events, utils
from telethon.tl.custom import Button
from telethon.tl.functions.bots import SetBotCommandsRequest
from telethon.tl.types import BotCommand, BotCommandScopeDefault

from backup import backup_filename, backup_summary, load_backup, write_backup
from config import BACKUP_DIR, CONFIG, save_config
from core import (app, get_pass, is_owner, login, logout,
                  require_staff, resolve_channel, store, wait_for,
                  flood_respond, flood_edit, flood_send_file,
                  vps_state)
from parser import normalize_key
from style import ADMIN_HOME, escape_html

COMMAND_MENU = [
    # — user —
    ("start", "Start the bot"),
    ("help", "Help & command list"),
    ("request", "Request an anime / donghua"),
    ("search", "Search the file store"),
    ("get", "Get a stored file by link/tag"),
    ("login", "Staff login"),
    ("logout", "Staff logout"),
    # — watching & downloading —
    ("status", "Live bot status panel"),
    ("checknew", "Check new episodes interactively"),
    ("autodl", "Start/stop the API download watcher"),
    ("cancelall", "Stop ALL running & queued download jobs"),
    ("searchad", "Admin: force re-download any ep from the API"),
    ("fixsmall", "Find & repair tiny broken archives (dead links)"),
    # — VPS worker —
    ("vps", "VPS mode on/off + worker status"),
    ("addvps", "Add + auto-provision a VPS upload worker"),
    ("removevps", "Remove the VPS worker"),
    ("parallel", "Run Koyeb + VPS download lanes at once"),
    # — channels & archive —
    ("channels", "List posting channels"),
    ("addchannel", "Add a posting channel"),
    ("buttonchannel", "Add a menu button channel"),
    ("buttonchannels", "List menu button channels"),
    ("forcesub", "Force-subscribe settings"),
    ("dbchannels", "List archive DB channels"),
    ("adddbchannel", "Add a TG archive DB channel"),
    ("deldbchannel", "Remove a DB channel"),
    ("dbchannel", "Set the single legacy DB channel"),
    ("setdbchannel", "Set the single legacy DB channel"),
    ("settag", "Set a channel tag (eng = eng subs only)"),
    ("setlog", "Set the log channel"),
    # — database & backup —
    ("addmongodb", "Add a MongoDB URI (persistent)"),
    ("mongos", "List configured MongoDB URIs"),
    ("delmongodb", "Remove a MongoDB by number"),
    ("backup", "Backup the database"),
    ("recover", "Restore from a backup file"),
    # — staff & users —
    ("admins", "List admins"),
    ("addadmin", "Promote an admin"),
    ("deladmin", "Demote an admin"),
    ("setowner", "Claim/transfer the owner"),
    ("setpass", "Set the staff password"),
    ("users", "List known users"),
    ("setpaid", "Mark a user paid/free"),
    ("broadcast", "Broadcast a message"),
    ("usage", "Storage & usage stats"),
    ("stats", "Bot stats"),
    # — looks, sources & sync —
    ("setimg", "Set a custom poster image"),
    ("setstartimg", "Set the /start banner image"),
    ("setsource", "Set the metadata source base URL"),
    ("syncbutton", "Sync card buttons + fetch late eng subs"),
    ("sync", "Refresh the bot command menu"),
]


def admin_kb():
    return [
        [Button.inline("➕ Add Channel", b"adm:addchannel"),
         Button.inline("🧲 Button Channel", b"adm:buttonchannel")],
        [Button.inline("🔒 Force Sub", b"adm:forcesub"),
         Button.inline("💾 Backup", b"adm:backup")],
        [Button.inline("🗄 MongoDB", b"adm:mongodb"),
         Button.inline("🔄 Sync Cmds", b"adm:sync")],
        [Button.inline("📊 Stats", b"adm:stats")],
    ]


async def _is_staff(uid: int) -> bool:
    return is_owner(uid) or await store.is_admin(uid)


# ---------------------------------------------------------------------- #
# Posting channels
# ---------------------------------------------------------------------- #
@app.on(events.NewMessage(pattern=r"^/addchannel(?:\s|$)"))
async def cmd_addchannel(e):
    if not await require_staff(e):
        return
    parts = e.raw_text.split()
    if len(parts) < 2:
        return await flood_respond(e, 
            "📌 <b>Usage:</b> /addchannel &lt;channel_id or @username&gt; [tag]\n\n"
            "Example: <code>/addchannel -1001234567890 1080p</code>",
            parse_mode="html")
    target, tag = parts[1], " ".join(parts[2:]).strip().lower()
    try:
        ent = await resolve_channel(target)
    except Exception:
        return await flood_respond(e, 
            "❌ Can't reach that channel.\n\n"
            "The bot must be an admin there. If it already is, "
            "try one of these instead of the raw id:\n"
            "• send this command INSIDE that channel\n"
            "• forward any message from it to me first\n"
            "• use @username or an invite link")
    from telethon.tl.types import Channel, Chat
    if not isinstance(ent, (Channel, Chat)):
        return await flood_respond(e, 
            "❌ That id belongs to a USER, not a channel.\n"
            "Send the CHANNEL id (the -100… one), its "
            "@username, or an invite link.")
    tag = tag or "indo"
    await store.add_channel(ent.id, tag,
                            title=getattr(ent, "title", None))
    _spawn_backlog(ent.id)
    await flood_respond(e, 
        f"✅ <b>Channel Added</b>\n\n"
        f"📢 <b>Name:</b> {escape_html(getattr(ent, 'title', '?'))}\n"
        f"🆔 <b>ID:</b> <code>{ent.id}</code>\n"
        f"🏷 <b>Tag:</b> {escape_html(tag)}\n\n"
        f"Tag rules: <b>indo</b> → 360p…1080p · <b>eng</b> → "
        f"only -eng streams. Change later: "
        f"/settag {ent.id} &lt;tag&gt;",
        parse_mode="html")


@app.on(events.NewMessage(pattern=r"^/channels$"))
async def cmd_channels(e):
    if not await require_staff(e):
        return
    chans = await store.list_channels()
    if not chans:
        return await flood_respond(e, "📭 No posting channels yet. Use /addchannel.")
    lines = ["📢 <b>Posting Channels</b>\n"]
    kb = []
    for c in chans:
        lines.append(f"• <code>{c['_id']}</code> — {escape_html(c.get('title') or '?')} "
                     f"🏷 <i>{escape_html(c.get('tag') or '')}</i>")
        kb.append([Button.inline(f"🗑 Delete {c['_id']}",
                                 b"adm:delchan:" + str(c["_id"]).encode())])
    await flood_respond(e, "\n".join(lines), parse_mode="html", buttons=kb)


@app.on(events.CallbackQuery(pattern=rb"adm:delchan:(-?\d+)"))
async def cb_delchan(e):
    if not await _is_staff(e.sender_id):
        return await e.answer("⛔ Admins only", alert=True)
    cid = int(e.pattern_match.group(1).decode())
    await store.remove_channel(cid)
    await flood_edit(e, f"✅ Channel <code>{cid}</code> removed.", parse_mode="html")


@app.on(events.NewMessage(pattern=r"^/buttonchannel(?:\s|$)"))
async def cmd_buttonchannel(e):
    if not await require_staff(e):
        return
    text = e.raw_text[len("/buttonchannel"):].strip()
    if not text:
        return await flood_respond(e, 
            "📌 <b>Usage:</b> /buttonchannel &lt;channel link&gt; &lt;button name&gt;\n\n"
            "Example: <code>/buttonchannel https://t.me/donghahub 🎬 Donghua Hub</code>",
            parse_mode="html")
    parts = text.split(None, 1)
    url = parts[0].strip()
    if not url.startswith("http"):
        url = "https://t.me/" + url.lstrip("@")
    name = parts[1].strip() if len(parts) > 1 else url
    await store.add_button_channel(name, url)
    await flood_respond(e, f"✅ Button added: <b>{escape_html(name)}</b> → "
                    f"{escape_html(url)}", parse_mode="html")


@app.on(events.NewMessage(pattern=r"^/buttonchannels$"))
async def cmd_buttonchannels(e):
    if not await require_staff(e):
        return
    bcs = await store.list_button_channels()
    if not bcs:
        return await flood_respond(e, "📭 No button channels yet. Use /buttonchannel.")
    lines = ["🔘 <b>Menu Button Channels</b>\n"]
    kb = []
    for b in bcs:
        lines.append(f"• <b>{escape_html(b['_id'])}</b> → {escape_html(b['url'])}")
        kb.append([Button.inline(f"🗑 {b['_id'][:30]}",
                                 b"adm:delbtn:" + b["_id"][:40].encode())])
    await flood_respond(e, "\n".join(lines), parse_mode="html", buttons=kb)


@app.on(events.CallbackQuery(pattern=rb"adm:delbtn:(.+)"))
async def cb_delbtn(e):
    if not await _is_staff(e.sender_id):
        return await e.answer("⛔ Admins only", alert=True)
    name = e.pattern_match.group(1).decode()
    await store.remove_button_channel(name)
    await flood_edit(e, f"✅ Button <b>{escape_html(name)}</b> removed.", parse_mode="html")


# ---------------------------------------------------------------------- #
# Force subscribe
# ---------------------------------------------------------------------- #
@app.on(events.NewMessage(pattern=r"^/forcesub(?:\s|$)"))
async def cmd_forcesub(e):
    if not await require_staff(e):
        return
    parts = e.raw_text.split()
    mode = parts[1].lower() if len(parts) > 1 else None
    if mode not in ("off", "strict", "request"):
        return await flood_respond(e, 
            "🔒 <b>Force Subscribe</b>\n\n"
            "<b>Modes:</b>\n"
            "• <code>/forcesub off</code> — disabled\n"
            "• <code>/forcesub strict &lt;channel_id&gt;</code> — user must JOIN "
            "first, then gets files\n"
            "• <code>/forcesub request &lt;channel_id&gt;</code> — user sends a "
            "JOIN REQUEST, files unlock after approval\n\n"
            "Example: <code>/forcesub strict -1001234567890</code>",
            parse_mode="html")
    fs = await store.get_forcesub()
    channels = list(fs.get("channels") or [])
    if mode != "off":
        if len(parts) > 2:
            try:
                ent = await resolve_channel(parts[2])
            except Exception:
                return await flood_respond(e, "❌ Can't reach that channel — the bot "
                           "must be an admin there (or send this "
                           "command inside it once).")
            if ent.id not in channels:
                channels.append(ent.id)
    await store.set_forcesub(mode, channels)
    mode_name = {"off": "OFF 🚫",
                 "strict": "STRICT — join first, then files ✅",
                 "request": "JOIN REQUEST — send request, then files 📩"}[mode]
    await flood_respond(e, 
        f"🔒 <b>Force Sub Updated</b>\n\n"
        f"Mode: <b>{mode_name}</b>\n"
        f"Channels: {', '.join('<code>%d</code>' % c for c in channels) or '—'}",
        parse_mode="html")


# ---------------------------------------------------------------------- #
# MongoDB management
# ---------------------------------------------------------------------- #
@app.on(events.NewMessage(pattern=r"^/addmongo(?:db)?(?:\s|$)"))
async def cmd_addmongodb(e):
    if not await require_staff(e):
        return
    parts = e.raw_text.split()
    uri = parts[1].strip() if len(parts) > 1 else ""
    if not uri:  # maybe the URI is in the replied-to message
        rep = await e.get_reply_message()
        if rep and rep.text and rep.text.strip().startswith("mongodb"):
            uri = rep.text.strip().split()[0]
    if not uri:
        PENDING[e.sender_id] = "addmongodb"
        return await flood_respond(e, 
            "📌 Send the MongoDB URI now, in one message:\n"
            "<code>mongodb+srv://user:pass@cluster/x</code>\n\n"
            "The URI is saved permanently — it survives restarts, and "
            "even if storage is full you can add more URIs (auto "
            "fail-over).", parse_mode="html")
    if not uri.startswith("mongodb"):
        return await flood_respond(e, "❌ That doesn't look like a MongoDB URI "
                               "(must start with mongodb:// or mongodb+srv://).")
    await store.add_mongo_uri(uri)
    ok = await store._try_connect(uri)
    if ok:
        store.mode = "mongo"
        await flood_respond(e, f"✅ <b>MongoDB Connected & Saved</b>\n\n"
                        f"<code>{escape_html(uri[:60])}</code>…\n\n"
                        f"All data now lives in MongoDB and survives restarts.",
                        parse_mode="html")
    else:
        await flood_respond(e, f"⚠️ URI saved but connection failed — check the string:\n"
                        f"<code>{escape_html(uri[:50])}</code>…", parse_mode="html")
    # mirror the list into the live DB for redundancy
    await store.set_config("mongo_uris", store.uris)


@app.on(events.NewMessage(pattern=r"^/setowner$"))
async def cmd_setowner(e):
    """Claim/transfer the owner slot (panels, queue, cancels)."""
    ids = CONFIG.get("owner_ids") or []
    if ids and int(e.sender_id) not in [int(i) for i in ids]:
        return await flood_respond(e, "👑 Owner only.")
    if not ids and not await store.is_admin(e.sender_id):
        return await flood_respond(e, "👑 An admin/owner must claim this.")
    CONFIG["owner_ids"] = [int(e.sender_id)]
    save_config()
    await store.save_runtime()
    import autodl as _ad
    _ad.OWNER_ENT = None
    await flood_respond(e, f"👑 <b>Owner set</b> to <code>{e.sender_id}</code> — "
                    f"progress panels &amp; batch reports will be "
                    f"DM'd to you.", parse_mode="html")


# ---------------- VPS SSH provisioning ----------------
VPS_DIR = "vpsnode"


def _norm_base(b):
    """Accept 'app.koyeb.app' OR 'https://app.koyeb.app' — always store a
    scheme'd root. A bare host silently broke every worker heartbeat
    (urllib: 'unknown url type')."""
    b = (b or "").strip().strip('"').strip("'")
    if not b:
        return ""
    if "://" not in b:
        b = "https://" + b
    return b.rstrip("/")


def _ssh_client(cfg):
    import paramiko
    cli = paramiko.SSHClient()
    cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    cli.connect(cfg["host"], username=cfg.get("user") or "root",
                password=cfg.get("pass"), timeout=25,
                allow_agent=False, look_for_keys=False,
                banner_timeout=25)
    return cli


async def ssh_exec(cfg, cmd, timeout=120):
    def _run():
        cli = _ssh_client(cfg)
        try:
            _i, out, err = cli.exec_command(cmd, timeout=timeout)
            rc = out.channel.recv_exit_status()
            return (rc, out.read().decode("utf-8", "replace"),
                    err.read().decode("utf-8", "replace"))
        finally:
            cli.close()
    return await asyncio.to_thread(_run)


async def ssh_put(cfg, data: bytes, remote_path: str):
    def _run():
        cli = _ssh_client(cfg)
        try:
            sftp = cli.open_sftp()
            with sftp.open(remote_path, "wb") as fh:
                fh.write(data)
            sftp.close()
        finally:
            cli.close()
    return await asyncio.to_thread(_run)


async def _ask(e, q):
    await flood_respond(e, q, parse_mode="html")
    try:
        r = await wait_for(e.chat_id, timeout=180)
    except Exception:
        return None
    return (r.text or "").strip() if r else None


async def _vps_fail(msg, where, exc):
    if msg:
        try:
            await flood_edit(msg, f"❌ <b>VPS setup failed</b> ({where}):\n"
                                  f"<code>{escape_html(str(exc)[:220])}"
                                  f"</code>", parse_mode="html")
        except Exception:
            pass
    try:
        import autodl
        await autodl._owner_dm(
            f"❌ <b>VPS setup failed</b> ({where}):\n"
            f"<code>{escape_html(str(exc)[:220])}</code>")
    except Exception:
        pass


async def _provision(chat_id, msg):
    """SSH into the VPS and install/deploy/start the worker, step by
    step, reporting every failure to the owner."""
    v = dict(await store.get_config("vps") or {})
    cfg = v.get("ssh") or {}
    try:
        if msg:
            await flood_edit(msg, f"🔌 Connecting to "
                                  f"<code>{escape_html(cfg.get('user'))}"
                                  f"@{escape_html(cfg.get('host'))}</code>…",
                             parse_mode="html")
        rc, out, _ = await ssh_exec(
            cfg, "echo ok; head -2 /etc/os-release 2>/dev/null", 40)
        if rc != 0 or "ok" not in out:
            raise RuntimeError(f"ssh rc={rc}: {out[:80]}")
        osrel = " ".join(out.splitlines()[1:3]) or "linux"
        if msg:
            await flood_edit(msg, f"🐍 <b>Connected</b> — "
                                  f"{escape_html(osrel)[:56]} — "
                                  f"checking python…",
                             parse_mode="html")

        def _ver(t):
            try:
                return tuple(int(x) for x in
                             (t or "").strip().split()[-1]
                             .split(".")[:2])
            except Exception:
                return (0, 0)
        rc, out, _ = await ssh_exec(cfg, "python3 -V 2>&1 || true", 30)
        if _ver(out) < (3, 7):
            if msg:
                await flood_edit(msg, "🐍 python3 too old / missing — "
                                      "apt-installing python3…",
                                 parse_mode="html")
            await ssh_exec(cfg, "apt-get update -y && apt-get "
                                "install -y python3 python3-pip", 900)
            rc, out, _ = await ssh_exec(cfg, "python3 -V 2>&1 || true",
                                        30)
        if _ver(out) < (3, 7):
            raise RuntimeError(
                f"VPS python3 is {'.'.join(map(str, _ver(out)))} — "
                "the worker needs Debian 10 (buster) or newer")
        if msg:
            await flood_edit(msg, "📦 Installing telethon on the VPS…",
                             parse_mode="html")
        await ssh_exec(cfg,
                       "python3 -c 'import telethon' 2>/dev/null || "
                       "python3 -m pip install --user telethon || "
                       "python3 -m pip install --break-system-packages "
                       "--user telethon || pip3 install telethon", 900)
        rc, out, _ = await ssh_exec(
            cfg, "python3 -c 'import telethon; "
                 "print(telethon.__version__)' 2>&1", 60)
        if rc != 0 or "Traceback" in out:
            raise RuntimeError("telethon install failed on the VPS")
        tver = out.strip().splitlines()[-1]
        await ssh_exec(cfg, "python3 -m pip install --user Pillow "
                            "hachoir >/dev/null 2>&1 || true", 300)
        if msg:
            await flood_edit(msg, f"🚀 telethon {escape_html(tver)} ✓ "
                                  f"— deploying worker…",
                             parse_mode="html")
        rc, out, _ = await ssh_exec(cfg, "echo $HOME", 20)
        home = out.strip() or "/root"
        await ssh_exec(cfg, f"mkdir -p {home}/{VPS_DIR}/dl", 30)
        worker = open(os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "vps_worker.py"), "rb").read()
        await ssh_put(cfg, worker, f"{home}/{VPS_DIR}/vps_worker.py")
        conf = json.dumps({"base": _norm_base(v.get("base")),
                           "secret": v.get("secret")})
        await ssh_put(cfg, conf.encode(),
                      f"{home}/{VPS_DIR}/vps_worker.json")
        await ssh_exec(cfg, f"chmod 600 {home}/{VPS_DIR}/"
                            "vps_worker.json", 20)
        cron = (f"@reboot cd {home}/{VPS_DIR} && nohup python3 -u "
                f"vps_worker.py >> worker.log 2>&1 </dev/null &")
        await ssh_exec(cfg, f"(crontab -l 2>/dev/null | grep -v "
                            f"vps_worker.py; echo '{cron}') | crontab -",
                       30)
        base_url = _norm_base(v.get("base"))
        if msg:
            await flood_edit(msg, f"🌍 Checking <code>"
                                  f"{escape_html(base_url)}</code> is "
                                  f"reachable from the VPS…",
                             parse_mode="html")
        probe = ("python3 -c 'import urllib.request,urllib.error\n"
                 "try:\n"
                 "    c=urllib.request.urlopen(urllib.request.Request(\""
                 + base_url + "/vps/job\"),timeout=15).status\n"
                 "except urllib.error.HTTPError as e:\n"
                 "    c=e.code\n"
                 "except Exception:\n"
                 "    c=0\n"
                 "print(c)'")
        rc, out, _ = await ssh_exec(cfg, probe, 60)
        got = (out or "").strip().splitlines()
        code = got[-1].strip() if got else "?"
        if code not in ("200", "401"):
            raise RuntimeError(
                f"Koyeb url NOT reachable from the VPS (HTTP {code}) — "
                f"it must be THIS bot's service url, like "
                f"https://your-app.koyeb.app. Fix it with /addvps or "
                f"/vps redeploy.")
        # 1) kill any old worker — MUST be its own SSH call: inside a
        #    compound command the shell's own cmdline contains the
        #    literal 'vps_worker.py' (the nohup part) and pkill -f
        #    would kill the shell before the worker ever starts
        await ssh_exec(cfg, "pkill -f '[v]ps_worker.py' 2>/dev/null; "
                            + _KILL_SCAN + "; sleep 1", 30)
        # 2) start the new one — pidfile pattern: stdout/stderr to the
        #    log, stdin from /dev/null, pid into worker.pid (a plain
        #    '& echo $!' capture keeps the SSH channel/pipe open and
        #    HANGS the call forever — verified locally)
        rc, out, _ = await ssh_exec(
            cfg, f"cd {home}/{VPS_DIR} && rm -f worker.pid; nohup "
                 f"python3 -u vps_worker.py >> worker.log 2>&1 "
                 f"</dev/null & echo $! > {home}/{VPS_DIR}/worker.pid; "
                 f"cat {home}/{VPS_DIR}/worker.pid", 30)
        pid = (out or "").strip().splitlines()[-1].strip() if out else ""
        # 3) honest liveness check — this shell has NO literal in it
        await asyncio.sleep(2)
        rc, out, _ = await ssh_exec(
            cfg, "pgrep -f '[v]ps_worker.py' 2>/dev/null; " + _PS_SCAN,
            30)
        if "NONE" in (out or "") or not (out or "").strip():
            rc, lg, _ = await ssh_exec(
                cfg, f"tail -5 {home}/{VPS_DIR}/worker.log 2>/dev/null",
                30)
            raise RuntimeError(
                "worker process did not stay alive (pid "
                + (pid or "?") + ") — log: "
                + ((lg or "").strip()[-200:] or "(empty)"))
        if msg:
            await flood_edit(msg, "💓 Waiting for worker heartbeat…",
                             parse_mode="html")
        online = False
        for _ in range(25):
            await asyncio.sleep(3)
            if (await vps_state()).get("online"):
                online = True
                break
        if not online:
            rc, out, _ = await ssh_exec(
                cfg, f"tail -5 {home}/{VPS_DIR}/worker.log 2>/dev/null",
                30)
            raise RuntimeError("worker did not heartbeat — log: "
                               + (out.strip()[-200:] or "(empty)")
                               + " — is the url THIS bot's Koyeb "
                                 "service? then /vps redeploy")
        st = await vps_state()
        caps = st.get("caps") or {}
        yes = lambda k: "✓" if caps.get(k) else "✗"
        if msg:
            await flood_edit(msg,
                             f"✅ <b>VPS connected & worker online</b>"
                             f"\n\n🐍 {caps.get('py')} · telethon "
                             f"{tver} · Pillow {yes('PIL')} · hachoir "
                             f"{yes('hachoir')}\n💾 "
                             f"{caps.get('disk_mb')} MB free · "
                             f"{escape_html(osrel)[:40]}\n\n"
                             f"🔁 Worker auto-starts on VPS reboot "
                             f"(cron installed).\n"
                             f"Now flip the mode: /vps on",
                             parse_mode="html")
    except Exception as exc:
        await _vps_fail(msg, "provisioning", exc)
        if msg:
            try:
                await flood_respond(
                    msg, "💡 Fix the issue above, then send "
                         "<code>/vps redeploy</code> to retry — or "
                         "<code>/removevps</code> to start over.",
                    parse_mode="html")
            except Exception:
                pass


@app.on(events.NewMessage(pattern=r"^/addvps(?:\s+(.+))?$"))
async def cmd_addvps(e):
    """Register + provision a VPS worker: user, IP, root password,
    worker bot token, koyeb url. The bot SSHes in and installs
    everything itself."""
    if not is_owner(e.sender_id):
        return await flood_respond(e, "👑 Owner only.")
    parts = (e.text or "").split(None, 1)
    args = parts[1].split() if len(parts) > 1 else []
    if len(args) >= 4:
        user, host, pw, token = args[:4]
        base = _norm_base(args[4] if len(args) > 4 else "")
    else:
        await flood_respond(e, "🖥 <b>Add your VPS</b> — answer the "
                               "next questions here (the bot installs "
                               "everything over SSH itself).",
                            parse_mode="html")
        user = (await _ask(e, "1️⃣ VPS username? <i>(default root)"
                              "</i>")) or "root"
        host = await _ask(e, "2️⃣ VPS IP / hostname?")
        pw = await _ask(e, "3️⃣ Password for that user?")
        token = await _ask(e, "4️⃣ <b>Worker bot token</b> — a SECOND "
                              "bot from @BotFather (never the main "
                              "bot token)?")
        base = await _ask(e, "5️⃣ Koyeb base url of THIS bot — the "
                             "service link Koyeb gave you, e.g. "
                             "<code>https://your-app.koyeb.app</code> "
                             "(a bare your-app.koyeb.app is fine too). "
                             "Send <code>-</code> to keep the stored one")
        base = "" if base in ("-", None) else _norm_base(base)
        if not (host and pw and token):
            return await flood_respond(e, "❌ VPS setup cancelled — "
                                          "missing answers.")
    v = dict(await store.get_config("vps") or {})
    v.update({"ssh": {"host": host.strip(), "user": user.strip(),
                      "pass": pw.strip()},
              "token": token.strip(),
              "base": _norm_base(base or v.get("base") or ""),
              "secret": v.get("secret") or secrets.token_hex(8),
              "enabled": False, "beat": 0})
    await store.set_config("vps", v)
    if not v["base"]:
        return await flood_respond(
            e, "❌ Still need the Koyeb base url — /addvps again with "
               "it as the 5th value.")
    msg = await flood_respond(e, "🔌 Starting VPS setup…",
                              parse_mode="html")
    asyncio.ensure_future(_provision(e.chat_id, msg))


@app.on(events.NewMessage(pattern=r"^/vps(?:\s+(on|off|check|redeploy|fix))?$"))
async def cmd_vps(e):
    """Show / toggle the VPS mode: ON = VPS downloads+uploads and
    Koyeb only posts; OFF = Koyeb handles everything alone."""
    if not is_owner(e.sender_id):
        return await flood_respond(e, "👑 Owner only.")
    v = dict(await store.get_config("vps") or {})
    if not v.get("secret"):
        return await flood_respond(e, "🖥 No VPS registered — "
                                      "/addvps first.")
    arg = e.pattern_match.group(1)
    if arg in ("redeploy", "fix"):
        if not (v.get("ssh") and v.get("token")):
            return await flood_respond(e, "❌ Nothing to redeploy — "
                                          "/addvps first.")
        msg = await flood_respond(e, "🛠 <b>Re-deploying the VPS "
                                     "worker…</b>", parse_mode="html")
        asyncio.ensure_future(_provision(e.chat_id, msg))
        return
    if arg == "check":
        ssh = v.get("ssh")
        lines = ["🩺 <b>VPS worker check</b>"]
        if ssh:
            try:
                _, out, _e = await ssh_exec(
                    ssh, "if [ -n \"$(pgrep -f '[v]ps_worker.py' "
                         "2>/dev/null; " + _PS_SCAN + ")\" ]; then "
                         "echo RUNNING; else echo STOPPED; fi", 25)
                lines.append("Process: <code>"
                             + escape_html((out or "?").strip()) + "</code>")
                _, lg, _e = await ssh_exec(
                    ssh, f"tail -n 8 ~/{VPS_DIR}/worker.log 2>/dev/null",
                    25)
                if (lg or "").strip():
                    lines.append("Log tail:\n<code>"
                                 + escape_html(lg.strip()[-500:])
                                 + "</code>")
                if "STOPPED" in (out or ""):
                    await ssh_exec(
                        ssh, f"cd ~/{VPS_DIR} && rm -f worker.pid; "
                             f"nohup python3 -u vps_worker.py "
                             f">> worker.log 2>&1 </dev/null & echo $! "
                             f"> ~/{VPS_DIR}/worker.pid; "
                             f"cat ~/{VPS_DIR}/worker.pid", 30)
                    lines.append("↻ Restarted it — /vps check again "
                                 "in ~1 min.")
            except Exception as ex:
                lines.append("SSH failed: <code>"
                             + escape_html(str(ex)[:160]) + "</code>")
        else:
            lines.append("No SSH details stored — /addvps first.")
        st2 = await vps_state()
        lines.append("Koyeb sees it: <b>"
                     + ("ONLINE ✅" if st2.get("online") else "OFFLINE ❌")
                     + "</b>"
                     + (f" · beat {int(time.time() - (st2.get('beat') or 0))}"
                        f"s ago" if st2.get("online") else ""))
        return await flood_respond(e, "\n".join(lines), parse_mode="html")
    if arg:
        v["enabled"] = (arg == "on")
        await store.set_config("vps", v)
    st = await vps_state()
    par = bool(await store.get_config("parallel", False))
    beat = time.time() - (st.get("beat") or 0)
    caps = st.get("caps") or {}
    yes = lambda k: "✓" if caps.get(k) else "✗"
    lines = [
        "🖥 <b>VPS worker</b>",
        f"Mode: <b>{'ON' if st.get('enabled') else 'OFF'}</b> — "
        + ("VPS downloads+uploads, Koyeb posts cards"
           if st.get("enabled") else
           "Koyeb handles everything, no VPS"),
        f"Worker: <b>{'online' if st.get('online') else 'OFFLINE'}</b>"
        + (f" (beat {int(beat)}s ago)" if st.get("beat") else ""),
        f"Parallel: <b>{'ON 🔀' if par else 'off'}</b> — /parallel "
        f"runs Koyeb + VPS at the same time",
        f"Caps: py {caps.get('py', '?')} · telethon {yes('telethon')} "
        f"· Pillow {yes('PIL')} · hachoir {yes('hachoir')} · disk "
        f"{caps.get('disk_mb', '?')} MB · worker build "
        f"{caps.get('build', '?')}",
        f"Done: <b>{st.get('jobs') or 0}</b> job(s) · "
        f"<b>{round((st.get('bytes') or 0) / 1073741824, 2)} GB</b> "
        f"uploaded · avg {st.get('speed') or 0} MB/s",
        f"Base: <code>{escape_html(st.get('base') or 'not set')}</code>",
        f"SSH: <code>{escape_html((st.get('ssh') or {}).get('user', '-'))}"
        f"@{escape_html((st.get('ssh') or {}).get('host', '-'))}</code>",
    ]
    if st.get("enabled") and not st.get("online"):
        lines.append("⚠️ ON but worker offline — jobs wait up to 90s "
                     "for the worker, then fall back to Koyeb; the "
                     "watchdog revives it automatically.")
    if st.get("online") and \
            str(caps.get("build") or "") != _worker_build():
        lines.append("🔄 <b>Worker update pending</b> — build "
                     f"<code>{escape_html(str(caps.get('build') or '?'))}"
                     f"</code> → <code>{_worker_build()}</code> ships "
                     "itself over SSH within ~5 min (as soon as the "
                     "worker is idle). Nothing to do.")
    await flood_respond(e, "\n".join(lines), parse_mode="html")


@app.on(events.NewMessage(pattern=r"^/fixsmall(?: (go|dry))?$"))
async def cmd_fixsmall(e):
    """Owner-only repair for the poisoned-archive era: dead CDN
    links (esp. the API's expiring 720p ones) used to be archived
    as ~3 KB 'videos' — error pages wired into DB channels and
    episode cards. Find every registry entry whose file is
    impossibly small, delete those broken messages and unregister
    them, so the watcher re-downloads the episodes through the now
    validated path. Dry run by default — /fixsmall go executes."""
    if not is_owner(e.sender_id):
        return await flood_respond(e, "👑 <b>Owner only.</b>")
    import base64 as _b64
    import autodl
    go = (e.pattern_match.group(1) or "") == "go"
    docs = await store.find("downloaded", {})
    small = [d for d in docs
             if 0 < int(d.get("size") or 0) < autodl.MIN_VIDEO_BYTES]
    if not small:
        return await flood_respond(
            e, "✅ <b>No broken entries</b> — every archived file in "
               "the registry is a plausible video size.",
            parse_mode="html")
    lines = [f"🩺 <b>Found {len(small)} broken archive "
             f"entry/entries</b> (archived file < "
             f"{autodl.MIN_VIDEO_BYTES // 1024} KB — a dead CDN link "
             f"got archived as a video):"]
    for d in small[:15]:
        lines.append(
            f"· {escape_html(str(d.get('title') or '?'))} Eps "
            f"{escape_html(str(d.get('episode') or '?'))} "
            f"[{escape_html(str(d.get('quality') or '?'))}] — "
            f"{int(d.get('size') or 0)} B")
    if len(small) > 15:
        lines.append(f"… and {len(small) - 15} more")
    if not go:
        lines.append("\nSend <b>/fixsmall go</b> to delete these "
                     "broken files from the DB channel(s), "
                     "unregister them and let the watcher "
                     "re-download the episodes cleanly.")
        return await flood_respond(e, "\n".join(lines),
                                   parse_mode="html")
    fixed = 0
    for d in small:
        for r in [d.get("db") or {}] + list(d.get("db_extra") or []):
            try:
                if r.get("channel") and r.get("msg_id"):
                    await app.delete_messages(int(r["channel"]),
                                              [int(r["msg_id"])])
            except Exception:
                pass
        try:  # the deep-link row that served this broken file
            deep = d.get("deep_link") or ""
            if "start=" in deep:
                payload = deep.split("start=")[-1]
                pad = payload + "=" * (-len(payload) % 4)
                txt = _b64.b64decode(pad.encode()).decode()
                if txt.startswith("get-"):
                    await store.col("deeplinks").delete_one(
                        {"_id": txt[4:]})
        except Exception:
            pass
        try:
            await store.col("downloaded").delete_one({"_id": d["_id"]})
            autodl._registered.pop(d["_id"], None)
            fixed += 1
        except Exception:
            pass
    await flood_respond(
        e, f"♻️ <b>Repaired {fixed}/{len(small)}</b> — broken files "
           f"deleted from the DB channel(s) and unregistered. The "
           f"watcher (or /checknew) re-downloads those episodes "
           f"through the validated path and posts fresh cards; the "
           f"OLD cards' buttons stay dead until the new cards land.",
        parse_mode="html")


# pgrep/pkill need procps — minimal VPS images do NOT have it, and
# when they lack it every check here reads "no process": the
# watchdog then starts a SECOND worker whose boot sweep DELETES the
# first one's in-progress download (the "720p file silently removed"
# mystery). These scans work on any Linux: a pure /proc argv walk —
# argv[0] must contain python AND an exact argv element must be
# [path/]vps_worker.py, so neither the SSH wrapper shell (bash -c
# '… vps_worker.py …') nor the scan itself can ever self-match.
_PS_SCAN = ("for d in /proc/[0-9]*; do "
            "a=$(tr '\\0' '\\n' < \"$d/cmdline\" 2>/dev/null); "
            "[ -z \"$a\" ] && continue; "
            "case \"$(echo \"$a\" | head -1)\" in *python*) "
            "echo \"$a\" | grep -qx -e vps_worker.py "
            "-e '.*/vps_worker.py' && echo \"${d#/proc/}\";; "
            "esac; done")
_KILL_SCAN = ("for d in /proc/[0-9]*; do "
              "a=$(tr '\\0' '\\n' < \"$d/cmdline\" 2>/dev/null); "
              "[ -z \"$a\" ] && continue; "
              "case \"$(echo \"$a\" | head -1)\" in *python*) "
              "echo \"$a\" | grep -qx -e vps_worker.py "
              "-e '.*/vps_worker.py' && kill \"${d#/proc/}\" "
              "2>/dev/null;; esac; done")


def _worker_build() -> str:
    """Content-hash of the vps_worker.py sitting next to this file
    (the one /vps redeploy + the watchdog ship to the VPS). The
    worker self-hashes the same way, so ANY byte difference —
    including a change nobody remembered to stamp — trips the
    auto-update; identical files hash identically, so there are
    never needless restarts."""
    try:
        data = open(os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "vps_worker.py"), "rb").read()
        return hashlib.md5(data).hexdigest()[:10]
    except Exception:
        return ""


async def _push_worker(st) -> str:
    """SFTP the repo's CURRENT vps_worker.py to ~/vpsnode; returns
    the worker's $HOME (the restart commands build on it). Shared
    by the auto-update and the offline restart, so a dead worker
    always comes back on the newest build in ONE shot."""
    ssh = st["ssh"]
    rc, out, _ = await ssh_exec(ssh, "echo $HOME", 20)
    home = (out or "").strip() or "/root"
    data = open(os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "vps_worker.py"), "rb").read()
    await ssh_put(ssh, data, f"{home}/{VPS_DIR}/vps_worker.py")
    return home


async def _worker_autoupdate(st):
    """Every beat carries the worker's build stamp. When the bot's
    copy is newer, re-upload it and restart the worker over SSH —
    nobody has to remember /vps redeploy after a bot update."""
    want = _worker_build()
    have = str((st.get("caps") or {}).get("build") or "0")
    if not want or have == want:
        return
    # NEVER restart a worker mid-job: since the heartbeat thread,
    # 'online' stays true DURING downloads/uploads, so check the
    # job queue — a claimed job or progress under 2 min old means
    # 'come back next tick' (5 min), not 'pkill now'
    try:
        inflight = await store.find("vps_jobs", {})
    except Exception:
        inflight = []
    now = time.time()
    for d in inflight or []:
        prog_ts = (d.get("prog") or {}).get("ts") or 0
        started = d.get("started") or d.get("ts") or 0
        if prog_ts > now - 120 or \
                (d.get("status") == "running" and started > now - 240):
            # REAL mid-job: progress flowing, or freshly claimed
            # (the worker's login gap before its first progress post)
            print("[vps] worker is mid-job — auto-update waits a tick")
            return
        # anything else is a ZOMBIE (old 'running' doc whose poller
        # died in a redeploy/crash) — deferring on those froze a VPS
        # on an ancient build FOREVER, so they no longer block
    ssh = st["ssh"]
    import autodl
    try:
        home = await _push_worker(st)
        await ssh_exec(ssh, "pkill -f '[v]ps_worker.py' 2>/dev/null; "
                            + _KILL_SCAN + "; sleep 1", 30)
        rc, out, _ = await ssh_exec(
            ssh, f"cd {home}/{VPS_DIR} && rm -f worker.pid; nohup "
                 f"python3 -u vps_worker.py >> worker.log 2>&1 "
                 f"</dev/null & echo $! > {home}/{VPS_DIR}/worker.pid; "
                 f"cat {home}/{VPS_DIR}/worker.pid", 30)
        pid = (out or "").strip().splitlines()[-1].strip() if out else ""
        ok = pid.isdigit()
        if ok:
            _, alive, _ = await ssh_exec(
                ssh, f"sleep 3; kill -0 {pid} 2>/dev/null && echo ALIVE "
                     f"|| echo DEAD", 30)
            ok = "ALIVE" in (alive or "")
        print(f"[vps] worker auto-update {have}→{want}:",
              "ok" if ok else "FAILED")
        if ok:
            await autodl._owner_dm(
                f"♻️ <b>VPS worker auto-updated</b> — build "
                f"<code>{have}</code> → <code>{want}</code>, restarted "
                f"over SSH; beat back in ~90s. No action needed.")
        elif time.time() - (st.get("upd_warn") or 0) > 3600:
            v = dict(st)
            v["upd_warn"] = time.time()
            await store.set_config("vps", v)
            await autodl._owner_dm(
                f"⚠️ <b>VPS worker update failed</b> (build "
                f"<code>{have}</code> → <code>{want}</code>) — it keeps "
                f"running the OLD version (that's why jobs still fail "
                f"there). Try /vps redeploy.")
    except Exception as exc:
        print("[vps] auto-update error:", repr(exc)[:120])


async def _disk_low_warn(st):
    """The worker sweeps its own dl/ folder at boot and between
    jobs, so a low VPS disk means something ELSE is filling it —
    tell the owner (once per hour) before downloads start dying on
    ENOSPC."""
    try:
        mb = (st.get("caps") or {}).get("disk_mb")
        if not isinstance(mb, int) or mb < 0 or mb >= 1024:
            return
        if time.time() - (st.get("disk_warn") or 0) < 3600:
            return
        v = dict(st)
        v["disk_warn"] = time.time()
        await store.set_config("vps", v)
        import autodl
        await autodl._owner_dm(
            f"⚠️ <b>VPS disk low</b> — only <b>{mb} MB</b> free. "
            f"The worker auto-cleans its download folder every job "
            f"and at boot, so something else is filling the disk — "
            f"check <code>~/vpsnode</code> over SSH. Downloads may "
            f"fail until space is freed (Koyeb falls back per "
            f"file).")
        print(f"[vps] low-disk warning sent ({mb} MB free)")
    except Exception as exc:
        print("[vps] disk warn error:", repr(exc)[:120])


async def vps_watchdog_loop():
    """Every 5 min: sweep Koyeb's download dir (orphans from
    abandoned ingests / hard crashes); if the mode is ON and the
    worker went silent, restart it on the VPS; if it is online but
    OUTDATED, auto-update it; warn on a low VPS disk and tell the
    owner when even a restart fails (once per hour each)."""
    while True:
        try:  # Koyeb disk hygiene — runs at boot and every tick
            import autodl
            n, mb = autodl.sweep_orphans()
            if n:
                print(f"[vps] Koyeb orphan sweep: {n} file(s), "
                      f"{mb} MB reclaimed")
        except Exception as exc:
            print("[vps] orphan sweep error:", repr(exc)[:120])
        await asyncio.sleep(300)
        try:
            st = await vps_state()
            if not st.get("ssh"):
                continue
            # purge ZOMBIE job docs: a live poller deletes its doc at
            # the 20-min VPS_JOB_TIMEOUT at the latest, so anything
            # older was orphaned by a redeploy/crash — and a leftover
            # 'running' doc used to defer the worker auto-update
            # forever (how a VPS froze on an ancient build)
            try:
                zq = {"ts": {"$lt": time.time() - 25 * 60}}
                for zd in await store.find("vps_jobs", zq) or []:
                    await store.col("vps_jobs").delete_one(
                        {"_id": zd["_id"]})
                    print("[vps] purged zombie job doc",
                          zd.get("_id"), zd.get("status"))
            except Exception as exc:
                print("[vps] zombie purge skipped:", repr(exc)[:120])
            if st.get("online"):
                await _disk_low_warn(st)
                # outdated worker? refresh it even while the mode is
                # OFF — per-batch 🖥 VPS picks still need it current
                await _worker_autoupdate(st)
                continue
            # OFFLINE: revive it EVEN WITH THE MODE OFF — per-batch
            # 🖥 picks need a live worker — and push the CURRENT
            # file first, so it comes back on the newest build in
            # one shot instead of the stale one it died on
            rc, out, _ = await ssh_exec(
                st["ssh"],
                "pgrep -f '[v]ps_worker.py' 2>/dev/null; " + _PS_SCAN,
                40)
            if out.strip():
                continue   # process alive but silent — network side
            await _push_worker(st)
            rc, out, _ = await ssh_exec(
                st["ssh"], "cd $HOME/vpsnode && rm -f worker.pid; "
                           "nohup python3 -u vps_worker.py "
                           ">> worker.log 2>&1 </dev/null & echo $! "
                           "> $HOME/vpsnode/worker.pid; "
                           "cat $HOME/vpsnode/worker.pid", 60)
            pid = (out or "").strip().splitlines()[-1].strip() if out else ""
            alive = ""
            if pid.isdigit():
                _, alive, _ = await ssh_exec(
                    st["ssh"], f"sleep 3; kill -0 {pid} 2>/dev/null && "
                               f"echo ALIVE || echo DEAD", 30)
            print("[vps] watchdog restart pid:", pid[:12] or "none",
                  (alive or "").strip() or "?")
            if "ALIVE" not in (alive or ""):
                if time.time() - (st.get("wd_warn") or 0) > 3600:
                    v = dict(st)
                    v["wd_warn"] = time.time()
                    await store.set_config("vps", v)
                    import autodl
                    await autodl._owner_dm(
                        "❌ <b>VPS worker is down</b> and the watchdog "
                        "could not restart it over SSH — jobs run on "
                        "Koyeb until it's back.")
        except Exception as exc:
            print("[vps] watchdog error:", repr(exc)[:120])


@app.on(events.NewMessage(pattern=r"^/removevps$"))
async def cmd_removevps(e):
    if not is_owner(e.sender_id):
        return await flood_respond(e, "👑 Owner only.")
    v = dict(await store.get_config("vps") or {})
    if v.get("ssh"):
        try:
            await ssh_exec(v["ssh"],
                           "pkill -f '[v]ps_worker.py' 2>/dev/null; "
                           + _KILL_SCAN + "; "
                           "crontab -l 2>/dev/null | grep -v "
                           "'[v]ps_worker.py' | crontab -", 60)
        except Exception as exc:
            await flood_respond(e, f"⚠️ Worker could not be stopped "
                                   f"over SSH: "
                                   f"<code>{escape_html(str(exc)[:100])}"
                                   f"</code>", parse_mode="html")
    await store.set_config("vps", {})
    for d in await store.find("vps_jobs"):
        try:
            await store.col("vps_jobs").delete_one({"_id": d["_id"]})
        except Exception:
            pass
    await flood_respond(
        e, "🖥 <b>VPS worker removed</b> — Koyeb handles everything "
           "again. On the VPS stop it with: "
           "<code>pkill -f vps_worker.py</code>", parse_mode="html")


@app.on(events.NewMessage(pattern=r"^/mongos$"))
async def cmd_mongos(e):
    if not await require_staff(e):
        return
    if not store.uris:
        return await flood_respond(e, "🗄 No MongoDB URIs yet. Use /addmongodb.")
    lines = ["🗄 <b>MongoDB URIs</b>\n"]
    for i, u in enumerate(store.uris, 1):
        mark = " <b>(active)</b>" if u == store.active_uri else ""
        lines.append(f"{i}. <code>{escape_html(u[:44])}</code>…{mark}")
    lines.append(f"\nMode: <b>{store.mode.upper()}</b>")
    await flood_respond(e, "\n".join(lines), parse_mode="html")


@app.on(events.NewMessage(pattern=r"^/delmongodb(?:\s|$)"))
async def cmd_delmongodb(e):
    if not await require_staff(e):
        return
    parts = e.raw_text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        return await flood_respond(e, "📌 Usage: /delmongodb &lt;number&gt;  (see /mongos)")
    idx = int(parts[1]) - 1
    if not (0 <= idx < len(store.uris)):
        return await flood_respond(e, "❌ Invalid number.")
    removed = store.uris.pop(idx)
    CONFIG["mongo_uris"] = store.uris
    save_config()
    if store.active_uri == removed:
        await store.init()
    await flood_respond(e, f"✅ Removed MongoDB #{idx + 1}:\n"
                    f"<code>{escape_html(removed[:50])}</code>…", parse_mode="html")


# ---------------------------------------------------------------------- #
# Backup & recover
# ---------------------------------------------------------------------- #
@app.on(events.NewMessage(pattern=r"^/backup$"))
async def cmd_backup(e):
    if not await require_staff(e):
        return
    m = await flood_respond(e, "💾 <b>Creating backup…</b>", parse_mode="html")
    data = await store.backup_all()
    path = os.path.join(BACKUP_DIR, backup_filename())
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=1, default=str)
    caption = (f"🗂 <b>AutoDonghua Backup</b>\n🕐 {data['exported_at']}\n\n"
               + backup_summary(data))
    db_chan = await store.get_config("db_channel")
    sent = []
    if db_chan:
        try:
            await app.send_file(db_chan, path, caption=caption, parse_mode="html")
            sent.append("db channel")
        except Exception as ex:
            sent.append(f"db channel failed ({ex})")
    try:
        await flood_send_file(
            e, path,
            caption=caption + "\n\n📌 Keep this file safe — use "
                    "/recover to restore everything from it.",
            parse_mode="html")
        sent.append("your chat")
    except Exception as ex:
        await m.edit(f"❌ Backup created but sending failed: {ex}")
        return
    await store.log_backup({"at": data["exported_at"], "path": path,
                            "collections": {k: len(v) for k, v in
                                            data["collections"].items()}})
    await m.delete()
    await flood_respond(e, f"✅ Backup saved to: <b>{', '.join(sent)}</b>", parse_mode="html")


@app.on(events.NewMessage(pattern=r"^/recover$"))
async def cmd_recover(e):
    if not await require_staff(e):
        return
    await flood_respond(e, "📥 <b>Recovery Mode</b>\n\nSend the backup JSON file now "
                    "(forward the one from your DB channel). I'll restore "
                    "everything — channels, images, users, files & settings.\n\n"
                    "<i>Waiting 3 minutes…</i>", parse_mode="html")
    msg = await wait_for(e.chat_id, timeout=180,
                         filt=lambda m: m.document and
                         m.document.name.endswith(".json"))
    if not msg:
        return await flood_respond(e, "⏱ Recovery cancelled (timeout).")
    path = await app.download_media(msg.media)
    try:
        data = load_backup(path)
    except Exception as ex:
        return await flood_respond(e, f"❌ Invalid backup file: {ex}")
    counts = await store.restore_all(data)
    summary = "\n".join(f"• {k}: <b>{v}</b>" for k, v in counts.items())
    await flood_respond(e, f"♻️ <b>Recovery Complete</b>\n\n{summary}\n\n"
                    f"The bot is fully restored — even after deletion or a ban, "
                    f"your data comes back with /recover.", parse_mode="html")


@app.on(events.NewMessage(pattern=r"^/dbchannel(?:\s|$)"))
async def cmd_dbchannel(e):
    if not await require_staff(e):
        return
    parts = e.raw_text.split()
    if len(parts) < 2:
        cur = await store.get_config("db_channel")
        return await flood_respond(e, f"📌 Current DB channel: <code>{cur}</code>\n"
                               f"Usage: /dbchannel &lt;channel_id&gt;",
                               parse_mode="html")
    try:
        ent = await resolve_channel(parts[1])
    except Exception:
        return await flood_respond(e, "❌ Can't reach that channel — the bot "
                           "must be an admin there (or send this "
                           "command inside it once).")
    from telethon.tl.types import Channel, Chat
    if not isinstance(ent, (Channel, Chat)):
        return await flood_respond(e, 
            "❌ That id belongs to a USER, not a channel.\n"
            "Send the CHANNEL id (the -100… one), its "
            "@username, or an invite link.")
    await store.set_config("db_channel", ent.id)
    await flood_respond(e, f"✅ Backups will be posted in "
                    f"<b>{escape_html(getattr(ent, 'title', '?'))}</b> "
                    f"(<code>{ent.id}</code>).", parse_mode="html")


@app.on(events.NewMessage(pattern=r"^/setlog(?:\s|$)"))
async def cmd_setlog(e):
    if not await require_staff(e):
        return
    parts = e.raw_text.split()
    if len(parts) < 2:
        cur = await store.get_config("log_channel")
        return await flood_respond(e, f"📌 Log channel: <code>{cur}</code>",
                               parse_mode="html")
    try:
        ent = await resolve_channel(parts[1])
    except Exception:
        return await flood_respond(e, "❌ Can't reach that channel — the bot "
                           "must be an admin there (or send this "
                           "command inside it once).")
    await store.set_config("log_channel", ent.id)
    await flood_respond(e, f"✅ Request logs go to <b>{escape_html(getattr(ent, 'title', '?'))}</b>.",
                    parse_mode="html")


# ---------------------------------------------------------------------- #
# Custom images
# ---------------------------------------------------------------------- #
@app.on(events.NewMessage(pattern=r"^/setimg(?:\s|$)"))
async def cmd_setimg(e):
    if not await require_staff(e):
        return
    text = e.raw_text[len("/setimg"):].strip()
    parts = text.rsplit(None, 1)
    if len(parts) < 2 or not parts[1].startswith("http"):
        return await flood_respond(e, 
            "📌 <b>Usage:</b> /setimg &lt;anime title&gt; &lt;image url&gt;\n\n"
            "Example: <code>/setimg Soul Land https://img.jpg</code>\n\n"
            "The image is saved by title — every future post with the same "
            "name (any episode) automatically reuses it.", parse_mode="html")
    title, url = parts[0].strip(), parts[1].strip()
    await store.save_image(normalize_key(title), url=url, source="custom", custom=True)
    await flood_respond(e, f"🖼 Image saved for <b>{escape_html(title)}</b>.\n"
                    f"All future posts of this title will use it automatically.",
                    parse_mode="html")


# ---------------------------------------------------------------------- #
# Users / tiers / admins
# ---------------------------------------------------------------------- #
@app.on(events.NewMessage(pattern=r"^/setpaid(?:\s|$)"))
async def cmd_setpaid(e):
    if not await require_staff(e):
        return
    parts = e.raw_text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        return await flood_respond(e, "📌 Usage: /setpaid &lt;user_id&gt; [paid|free]")
    uid = int(parts[1])
    tier = parts[2].lower() if len(parts) > 2 else None
    if tier not in ("paid", "free"):
        cur = await store.get_user(uid)
        tier = "free" if (cur or {}).get("tier") == "paid" else "paid"
    await store.set_tier(uid, tier)
    await flood_respond(e, f"💎 User <code>{uid}</code> is now <b>{tier.upper()}</b>.",
                    parse_mode="html")


@app.on(events.NewMessage(pattern=r"^/addadmin(?:\s|$)"))
async def cmd_addadmin(e):
    if not await require_staff(e):
        return
    if not is_owner(e.sender_id):
        return await flood_respond(e, "👑 Owner only.")
    parts = e.raw_text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        return await flood_respond(e, "📌 Usage: /addadmin &lt;user_id&gt;")
    await store.add_admin(int(parts[1]))
    await flood_respond(e, f"✅ <code>{parts[1]}</code> is now an admin.", parse_mode="html")


@app.on(events.NewMessage(pattern=r"^/deladmin(?:\s|$)"))
async def cmd_deladmin(e):
    if not await require_staff(e):
        return
    if not is_owner(e.sender_id):
        return await flood_respond(e, "👑 Owner only.")
    parts = e.raw_text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        return await flood_respond(e, "📌 Usage: /deladmin &lt;user_id&gt;")
    await store.remove_admin(int(parts[1]))
    await flood_respond(e, f"✅ Admin <code>{parts[1]}</code> removed.", parse_mode="html")


@app.on(events.NewMessage(pattern=r"^/admins$"))
async def cmd_admins(e):
    if not await require_staff(e):
        return
    admins = await store.list_admins()
    lines = [f"👑 <b>Owner:</b> "
             f"{', '.join('<code>%d</code>' % x for x in (CONFIG.get('owner_ids') or []))}",
             "🛡 <b>Admins:</b>"]
    for a in admins:
        lines.append(f"• <code>{a['_id']}</code>")
    await flood_respond(e, "\n".join(lines), parse_mode="html")


# ---------------------------------------------------------------------- #
# Stats & usage
# ---------------------------------------------------------------------- #
@app.on(events.NewMessage(pattern=r"^/users$"))
async def cmd_users(e):
    if not await require_staff(e):
        return
    users = await store.list_users(limit=10000)
    paid = [u for u in users if u.get("tier") == "paid"]
    out = [f"👥 <b>Users:</b> {len(users)}  (💎 paid: {len(paid)})"]
    for u in sorted(users, key=lambda x: x.get("last_seen", ""), reverse=True)[:10]:
        out.append(f"• <code>{u['_id']}</code> — {escape_html(u.get('first_name') or '?')} "
                   f"@{u.get('username') or '-'} [{u.get('tier')}] "
                   f"used: {u.get('bytes_used', 0) // 1048576} MB")
    await flood_respond(e, "\n".join(out), parse_mode="html")


@app.on(events.NewMessage(pattern=r"^/usage$"))
async def cmd_usage(e):
    if not await require_staff(e):
        return
    st = await store.stats()
    files = await store.count_files()
    out = [f"📊 <b>Storage & Usage</b>\n",
           f"🗄 Backend: <b>{st['mode'].upper()}</b>",
           f"💾 DB size: <b>{st.get('db_size_mb')} MB</b>"]
    for k, v in st["collections"].items():
        out.append(f"• {k}: {v}")
    out.append(f"\n📁 Files posted: <b>{files}</b>")
    await flood_respond(e, "\n".join(out), parse_mode="html")


@app.on(events.NewMessage(pattern=r"^/stats$"))
async def cmd_stats(e):
    if not await require_staff(e):
        return
    st = await store.stats()
    try:
        backups = len([f for f in os.listdir(BACKUP_DIR)
                       if f.endswith(".json")])
    except Exception:
        backups = 0
    await flood_respond(e,
        f"📈 <b>Bot Stats</b>\n\n"
        f"📺 Posting channels: <b>{st['collections'].get('channels', 0)}</b>\n"
        f"🔘 Button channels: <b>{st['collections'].get('button_channels', 0)}</b>\n"
        f"🖼 Cached images: <b>{st['collections'].get('images', 0)}</b>\n"
        f"💾 Backups made: <b>{backups}</b>\n"
        f"🗄 Storage: <b>{st['mode'].upper()}</b> ({st.get('db_size_mb')} MB)",
        parse_mode="html")


# ---------------------------------------------------------------------- #
# Broadcast & misc
# ---------------------------------------------------------------------- #
@app.on(events.NewMessage(pattern=r"^/broadcast(?:\s|$)"))
async def cmd_broadcast(e):
    if not await require_staff(e):
        return
    text = e.raw_text[len("/broadcast"):].strip()
    if not text and e.is_reply:
        r = await e.get_reply_message()
        text = r.text or r.raw_text or ""
    if not text:
        return await flood_respond(e, "📌 Usage: /broadcast &lt;message&gt; "
                               "or reply to a message.")
    users = await store.list_users(limit=10000)
    ok = fail = 0
    for u in users:
        try:
            await app.send_message(u["_id"], text, parse_mode="html")
            ok += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.05)
    await flood_respond(e, f"📢 Broadcast done: <b>{ok}</b> sent, <b>{fail}</b> failed.",
                    parse_mode="html")


@app.on(events.NewMessage(pattern=r"^/setsource(?:\s|$)"))
async def cmd_setsource(e):
    if not await require_staff(e):
        return
    parts = e.raw_text.split()
    if len(parts) >= 3 and parts[1].lower() == "anilive":
        await store.set_config("anilive_base", parts[2].strip().rstrip("/"))
        await flood_respond(e, f"✅ AniLive base URL set to "
                        f"<code>{escape_html(parts[2])}</code>", parse_mode="html")
    else:
        cur = await store.get_config("anilive_base")
        await flood_respond(e, f"📌 Usage: /setsource anilive &lt;base_url&gt;\n"
                        f"Current: <code>{cur or 'https://anilive.app'}</code>",
                        parse_mode="html")


async def sync_command_menu() -> int:
    """Push COMMAND_MENU to Telegram so the '/' autocomplete menu
    matches the handlers that actually exist. Runs at BOOT on every
    deploy — a stale menu used to hide each new command until
    somebody remembered to type /sync — and on manual /sync."""
    cmds = [BotCommand(c, d) for c, d in COMMAND_MENU]
    await app(SetBotCommandsRequest(
        commands=cmds, scope=BotCommandScopeDefault(), lang_code=""))
    return len(cmds)


@app.on(events.NewMessage(pattern=r"^/sync$"))
async def cmd_sync(e):
    if not await require_staff(e):
        return
    n = await sync_command_menu()
    await flood_respond(e, f"✅ <b>Commands synced!</b> {n} commands are now "
                    f"visible in the bot menu:\n\n" +
                    "\n".join(f"• <code>/{c}</code> — {escape_html(d)}"
                               for c, d in COMMAND_MENU),
                    parse_mode="html")


@app.on(events.NewMessage(pattern=r"^/help$"))
async def cmd_help(e):
    if await _is_staff(e.sender_id):
        text = ("🛠 <b>Admin Commands</b>\n\n"
                "• /addchannel &lt;id&gt; [tag] — posting channel\n"
                "• /channels — list posting channels\n"
                "• /buttonchannel &lt;link&gt; &lt;name&gt; — user-menu button\n"
                "• /forcesub &lt;off|strict|request&gt; [channel]\n"
                "• /addmongodb &lt;uri&gt; — add MongoDB (persistent)\n"
                "• /mongos · /delmongodb &lt;n&gt;\n"
                "• /backup · /recover · /dbchannel &lt;id&gt; · /setlog &lt;id&gt;\n"
                "• /setimg &lt;title&gt; &lt;url&gt; — custom poster\n"
                "• /setpaid &lt;user_id&gt; [paid|free]\n"
                "• /addadmin · /deladmin · /admins\n"
                "• /usage · /stats · /users · /broadcast\n"
                "• /setsource anilive &lt;base_url&gt;\n"
                "• /sync — refresh bot command menu")
        return await flood_respond(e, text, parse_mode="html")
    from style import WELCOME_TEMPLATE
    await flood_respond(e, WELCOME_TEMPLATE.format(
        username=e.sender.first_name or "Friend"), parse_mode="html")


# ---------------------------------------------------------------------- #
# Staff login / passgate
# ---------------------------------------------------------------------- #
@app.on(events.NewMessage(pattern=r"^/login(?:\s|$)"))
async def cmd_login(e):
    parts = e.raw_text.split()
    if len(parts) < 2:
        return await flood_respond(e, "🔑 <b>Staff Login</b>\n\n"
                               "Usage: <code>/login &lt;password&gt;</code>",
                               parse_mode="html")
    if parts[1].strip() == await get_pass():
        await login(e.sender_id)
        await flood_respond(e, "✅ <b>Logged in!</b> Admin panel unlocked.\n"
                        "Use /logout when done on a shared device.",
                        parse_mode="html")
    else:
        await flood_respond(e, "❌ Wrong password.", parse_mode="html")


@app.on(events.NewMessage(pattern=r"^/logout$"))
async def cmd_logout(e):
    await logout(e.sender_id)
    await flood_respond(e, "👋 Logged out. Admin commands are locked again.")


@app.on(events.NewMessage(pattern=r"^/setpass(?:\s|$)"))
async def cmd_setpass(e):
    if not await require_staff(e):
        return
    if not is_owner(e.sender_id):
        return await flood_respond(e, "👑 <b>Owner only</b> — you must be "
                               "logged in as the owner to change the "
                               "password.", parse_mode="html")
    parts = e.raw_text.split()
    if len(parts) < 2:
        return await flood_respond(e, "📌 Usage: /setpass &lt;new_password&gt;\n\n"
                               "(you must be logged in — owner only)")
    new_pass = parts[1].strip()
    if len(new_pass) < 3:
        return await flood_respond(e, "❌ Password too short (min 3 chars).")
    await store.set_config("admin_pass", new_pass)
    await flood_respond(e, f"✅ Password changed. Old sessions stay valid until "
                    f"/logout.", parse_mode="html")


@app.on(events.NewMessage(pattern=r"^/setdbchannel(?:\s|$)"))
async def cmd_setdbchannel(e):
    if not await require_staff(e):
        return
    parts = e.raw_text.split()
    if len(parts) < 2:
        cur = await store.get_config("db_channel")
        return await flood_respond(e, 
            f"🗂 <b>TG DB Channel</b> (file archive + backups)\n\n"
            f"Current: <code>{cur}</code>\n"
            f"Usage: /setdbchannel &lt;channel_id&gt;\n\n"
            f"Every posted file is archived here; users get clean "
            f"copies (no tag) from it via /search.",
            parse_mode="html")
    try:
        ent = await resolve_channel(parts[1])
    except Exception:
        return await flood_respond(e, 
            "❌ Can't reach that channel.\n\n"
            "The bot must be an admin there. If it already is, "
            "the id has no access hash yet — send this command "
            "INSIDE that channel once, or forward any message "
            "from it to me, or use @username / invite link.")
    await store.set_config("db_channel", ent.id)
    await flood_respond(e, f"✅ DB channel set to "
                    f"<b>{escape_html(getattr(ent, 'title', '?'))}</b> "
                    f"(<code>{ent.id}</code>). Files will be archived "
                    f"here.", parse_mode="html")


@app.on(events.NewMessage(pattern=r"^/setstartimg(?:\s|$)"))
async def cmd_setstartimg(e):
    """Banner image shown on the normal-user /start card."""
    if not await require_staff(e):
        return
    arg = e.raw_text.split(maxsplit=1)[1].strip() \
        if len(e.raw_text.split()) > 1 else ""
    reply = await e.get_reply_message()
    media_msg = reply if (reply is not None and
                          (reply.photo or
                           (reply.document and
                            (reply.document.mime_type or "")
                           .startswith("image/")))) else None
    if media_msg is not None:
        fid = utils.pack_bot_file_id(media_msg.media)
        await store.set_config("start_image", fid)
        return await flood_respond(e, "✅ /start banner set to the "
                               "replied photo.", parse_mode="html")
    if arg.lower() in ("off", "clear", "none"):
        await store.set_config("start_image", "")
        return await flood_respond(e, "🗑 /start banner removed — the "
                               "card is text-only now.",
                               parse_mode="html")
    if arg.startswith(("http://", "https://")):
        await store.set_config("start_image", arg)
        return await flood_respond(e, "✅ /start banner set to that "
                               "image URL.", parse_mode="html")
    # no arg, no reply: wait for the next photo / url message
    PENDING[e.sender_id] = "setstartimg"
    await flood_respond(e, "🖼 Send the banner now — either an image "
                    "(upload or file) or a direct image URL in one "
                    "message.\n<code>/setstartimg off</code> removes "
                    "it.", parse_mode="html")


@app.on(events.NewMessage(pattern=r"^/adddbchannel(?:\s|$)"))
async def cmd_adddbchannel(e):
    """Add a TG DB channel (file archive). Multiple allowed — all
    are saved in MongoDB and every AutoDL file is archived in all."""
    if not await require_staff(e):
        return
    parts = e.raw_text.split()
    if len(parts) < 2:
        cur = await store.list_db_channels()
        lines = "\n".join(f"• <code>{d['_id']}</code> — "
                          f"{escape_html(d.get('title') or '?')}"
                          for d in cur) or "• none yet"
        return await flood_respond(e, 
            f"🗄 <b>DB channels:</b>\n{lines}\n\n"
            f"Usage: /adddbchannel &lt;channel_id or @username&gt;",
            parse_mode="html")
    try:
        ent = await resolve_channel(parts[1])
    except Exception:
        return await flood_respond(e, 
            "❌ Can't reach that channel.\n\n"
            "The bot must be an admin there. If it already is, "
            "the id has no access hash yet — send this command "
            "INSIDE that channel once, or forward any message "
            "from it to me, or use @username / invite link.")
    from telethon.tl.types import Channel, Chat
    if not isinstance(ent, (Channel, Chat)):
        return await flood_respond(e, 
            "❌ That id belongs to a USER, not a channel.\n"
            "Send the CHANNEL id (the -100… one), its "
            "@username, or an invite link.")
    await store.add_db_channel(ent.id,
                               title=getattr(ent, "title", None))
    await store.set_config("db_channel", ent.id)  # legacy primary
    n = len(await store.list_db_channels())
    await flood_respond(e, 
        f"✅ <b>DB channel added</b>\n\n"
        f"📢 {escape_html(getattr(ent, 'title', '?'))} "
        f"(<code>{ent.id}</code>)\n"
        f"🗄 Total DB channels: <b>{n}</b> — every downloaded "
        f"file is archived in all of them.",
        parse_mode="html")


@app.on(events.NewMessage(pattern=r"^/dbchannels$"))
async def cmd_dbchannels(e):
    if not await require_staff(e):
        return
    cur = await store.list_db_channels()
    if not cur:
        return await flood_respond(e, "🗄 No DB channels yet — add one "
                               "with /adddbchannel <id>.")
    lines = "\n".join(f"• <code>{d['_id']}</code> — "
                      f"{escape_html(d.get('title') or '?')}"
                      for d in cur)
    await flood_respond(e, f"🗄 <b>DB channels ({len(cur)}):</b>\n{lines}\n\n"
                    f"Remove: /deldbchannel &lt;id&gt;",
                    parse_mode="html")


@app.on(events.NewMessage(pattern=r"^/deldbchannel(?:\s|$)"))
async def cmd_deldbchannel(e):
    if not await require_staff(e):
        return
    parts = e.raw_text.split()
    if len(parts) < 2:
        return await flood_respond(e, "📌 Usage: /deldbchannel "
                               "&lt;channel_id&gt;")
    try:
        cid = int(parts[1])
    except ValueError:
        return await flood_respond(e, "📌 That is not a channel id.")
    await store.remove_db_channel(cid)
    await flood_respond(e, f"🗑 DB channel <code>{cid}</code> removed.",
                    parse_mode="html")


def _spawn_backlog(cid):
    """Background: create missing cards for already-archived files
    of this channel's flavour, paced to avoid floodwait."""
    import autodl as _ad
    asyncio.ensure_future(_ad.backlog_cards(cid))


@app.on(events.NewMessage(pattern=r"^/settag(?:\s|$)"))
async def cmd_settag(e):
    """Change a posting channel's tag (indo / eng / eng sub)."""
    if not await require_staff(e):
        return
    parts = e.raw_text.split()
    if len(parts) < 3:
        chans = await store.list_channels()
        lines = "\n".join(f"• <code>{c['_id']}</code> — tag: "
                          f"<b>{escape_html(c.get('tag') or 'indo')}</b>"
                          for c in chans) or "• no channels yet"
        return await flood_respond(e, 
            f"🏷 <b>Channel tags</b>\n{lines}\n\n"
            f"Usage: /settag &lt;channel_id&gt; &lt;tag&gt;\n"
            f"• <b>indo</b> → base qualities 360p…1080p\n"
            f"• <b>eng</b> / <b>eng sub</b> → only -eng streams",
            parse_mode="html")
    from core import norm_channel_id
    cid = norm_channel_id(parts[1])
    if cid is None:
        return await flood_respond(e, "📌 That is not a channel id.")
    tag = " ".join(parts[2:]).strip().lower()
    await store.set_channel_tag(cid, tag)
    _spawn_backlog(cid)
    await flood_respond(e, 
        f"🏷 Tag for <code>{cid}</code> → <b>{escape_html(tag)}</b>\n"
        + ("Only -eng streams will be posted there (already "
           "archived ones get their cards one by one)."
           if "eng" in tag else
           "Base qualities 360p…1080p will be posted there (already "
           "archived ones get their cards one by one)."),
        parse_mode="html")


# Admin panel entry (button callbacks)
@app.on(events.CallbackQuery(pattern=rb"adm:(addchannel|buttonchannel|forcesub|backup|mongodb|sync|stats|panel)"))
async def cb_admin_panel(e):
    if not await require_staff(e):
        return
    hint = {
        "addchannel": "Send: /addchannel <channel_id> [tag]",
        "buttonchannel": "Send: /buttonchannel <link> <name>",
        "forcesub": "Send: /forcesub strict|request|off [channel_id]",
        "backup": "Send: /backup",
        "mongodb": "Send: /addmongodb <uri>",
        "sync": "Send: /sync",
        "stats": "Send: /stats or /usage",
        "panel": "Admin commands: /addchannel /buttonchannel /forcesub "
                 "/backup /recover /addmongodb /sync /usage /broadcast",
    }[e.pattern_match.group(1).decode()]
    await e.answer(hint, alert=True)


# Generic command-button callback (from the staff /start grid):
# buttons EXECUTE the command instead of just showing its usage.
class _Shim:
    """Wrap a callback (or any event) so command handlers see a
    NewMessage-like event with a synthetic raw_text."""

    def __init__(self, e, text: str, media=None):
        self._e = e
        self.raw_text = text
        self._media = media

    @property
    def sender_id(self):
        return self._e.sender_id

    @property
    def chat_id(self):
        return self._e.chat_id

    @property
    def sender(self):
        return self._e.sender

    async def respond(self, *a, **k):
        return await self._e.respond(*a, **k)

    async def reply(self, *a, **k):
        return await self._e.reply(*a, **k)

    async def get_reply_message(self):
        return self._media

    def __getattr__(self, name):
        return getattr(self._e, name)


PENDING = {}  # user_id -> command waiting for its arguments

NO_ARGS = {"channels", "users", "usage", "stats", "backup",
           "dbchannels", "sync", "autodl", "checknew", "logout",
           "admins", "mongos", "setstartimg"}

CMD_FUNCS = {
    "addchannel": cmd_addchannel,
    "channels": cmd_channels,
    "buttonchannel": cmd_buttonchannel,
    "forcesub": cmd_forcesub,
    "backup": cmd_backup,
    "recover": cmd_recover,
    "dbchannel": cmd_dbchannel,
    "setlog": cmd_setlog,
    "setimg": cmd_setimg,
    "setpaid": cmd_setpaid,
    "users": cmd_users,
    "usage": cmd_usage,
    "stats": cmd_stats,
    "broadcast": cmd_broadcast,
    "sync": cmd_sync,
    "setdbchannel": cmd_setdbchannel,
    "adddbchannel": cmd_adddbchannel,
    "dbchannels": cmd_dbchannels,
    "deldbchannel": cmd_deldbchannel,
    "settag": cmd_settag,
    "setstartimg": cmd_setstartimg,
    "logout": cmd_logout,
}

from handlers_autodl import cmd_autodl, cmd_checknew  # noqa: E402
CMD_FUNCS["autodl"] = cmd_autodl
CMD_FUNCS["addmongodb"] = cmd_addmongodb
CMD_FUNCS["checknew"] = cmd_checknew


@app.on(events.NewMessage())
async def pending_args(e):
    """The arguments message after a staff-grid button tap."""
    cmd = PENDING.pop(e.sender_id, None)
    if not cmd:
        return
    text = (e.raw_text or "").strip()
    if not text:
        if cmd == "setstartimg" and (
                e.photo or (e.document and
                            (e.document.mime_type or "")
                            .startswith("image/"))):
            fn = CMD_FUNCS.get(cmd)
            if fn is not None:
                await fn(_Shim(e, f"/{cmd}", media=e))
                raise events.StopPropagation
        return
    if text.startswith("/"):
        return  # a full command — let its own handler run
    fn = CMD_FUNCS.get(cmd)
    if fn is None:
        return
    await fn(_Shim(e, f"/{cmd} {text}"))
    raise events.StopPropagation


@app.on(events.CallbackQuery(pattern=rb"cmd:(\d+)"))
async def cb_cmd(e):
    if not await require_staff(e):
        return
    from handlers_user import STAFF_CMDS
    idx = int(e.pattern_match.group(1).decode())
    if not (0 <= idx < len(STAFF_CMDS)):
        return
    hint = STAFF_CMDS[idx][1]
    parts = hint.split()
    cmd = parts[1].lstrip("/") if len(parts) > 1 else ""
    fn = CMD_FUNCS.get(cmd)
    if fn is None:
        return await e.answer(hint, alert=True)
    if cmd in NO_ARGS:
        await e.answer(f"▶ /{cmd} …")
        await fn(_Shim(e, f"/{cmd}"))
    else:
        PENDING[e.sender_id] = cmd
        await e.answer(f"⌨ /{cmd} — send the arguments")
        await flood_respond(e, 
            f"⌨ <b>/{cmd}</b> — send the arguments now in one "
            f"message.\nExample: <code>{escape_html(hint)}</code>",
            parse_mode="html")


# Normal-user quick buttons
@app.on(events.CallbackQuery(pattern=rb"usr:(request|search)"))
async def cb_usr_action(e):
    what = e.pattern_match.group(1).decode()
    if what == "request":
        await e.answer("Send: /request <anime name> [episode]",
                       alert=True)
    else:
        await e.answer("Send: /search <anime name>", alert=True)
