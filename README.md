# 🤖 AutoDonghua Bot

An advanced auto file-store & post bot for **donghua & anime** on Telegram.
Send a video file to the bot — it automatically detects the **quality with
`mediainfo`**, identifies the **series name & episode number**, pulls
**posters & info from AniList, AniLive, AnimXin and LuciferDonghua**, then
shows a **confirmation screen** where you pick the channel(s) and hit **Send**.

## ✨ Features

- 📦 **Auto quality detection** — mediainfo (codec, resolution, bitrate,
  audio, duration, size) injected into every caption.
- 🧠 **Smart name/episode parsing** — fansub-style names, `S01E02`,
  `[Elysium] Title - 01 [1080p]`, Chinese `第01话`, etc.
- 🛰 **Multi-source metadata** — AniList, AniLive, AnimXin,
  LuciferDonghua (+ Jikan/MAL fallback), all queried concurrently.
- ✅ **Confirmation flow** — per-channel toggle buttons + **Send Now**.
- 🖼 **Poster cache** — custom image via URL (or upload); saved **by title**,
  so the same series with a new episode number reuses it automatically.
- 💾 **Backup system** — `/backup` posts a full DB dump into your DB
  channel; `/recover` restores everything from that file (bot survives
  deletion/ban).
- 🗄 **MongoDB storage** — `/addmongodb <uri>` persists settings across
  restarts; add multiple URIs (auto fail-over when one is full).
  Local JSON fallback when no DB is configured.
- 🔒 **Force subscribe** — `strict` (join first, then files) or
  `request` (send join request, then files) modes.
- 👥 **Free & paid users** — daily request limits per tier
  (`/setpaid <user_id>`).
- 📊 Storage & usage stats, user list, broadcast, request logging.
- 🔐 Admin/owner-only commands — normal users get a styled welcome card
  with your channel buttons.

## 🚀 Setup

```bash
pip install -r requirements.txt
# optional but recommended for quality detection:
sudo apt install mediainfo ffmpeg   # or: pip install mediainfo
cp config.json.example config.json  # fill api_id, api_hash, bot_token, owner_ids
python3 bot.py
```

Get `api_id`/`api_hash` from https://my.telegram.org and the token from
`@BotFather`. The bot must be **admin** in every channel you post to.

**Environment variables** (optional overrides): `API_ID`, `API_HASH`,
`BOT_TOKEN`, `OWNER_ID`, `MONGO_URI` (comma-separated for multiple),
`ANILIVE_BASE`, `PORT` (health endpoint, default 8000).

## ☁️ Koyeb (free tier) — recommended setup

Koyeb's free instance has an **ephemeral disk** (wiped on every
restart) and **autosleep**, so configure it like this:

1. **Deploy from GitHub** — link this repo; Koyeb auto-detects
   `requirements.txt` + `Procfile` (`web: python3 bot.py`).
2. **Environment / Secrets** (Service settings → Environment
   Variables):
   | Var | Value |
   |---|---|
   | `API_ID` | from my.telegram.org |
   | `API_HASH` | from my.telegram.org |
   | `BOT_TOKEN` | from @BotFather |
   | `OWNER_ID` | your Telegram user id |
   | `MONGO_URI` | free cluster from **MongoDB Atlas** (sr. connection string) |
   | `ANILIVE_BASE` | *(optional)* custom AniLive mirror |
3. **Why env vars?** `/addmongodb` still works, but on Koyeb the
   disk resets on restart — env vars are the durable copy.
   Everything else (channels, images, users, files, backups)
   lives in MongoDB, so nothing is lost when the instance
   restarts or sleeps.
4. **Keep it awake** — Koyeb free tier sleeps idle services. Add
   a free monitor (e.g. UptimeRobot) hitting
   `https://<your-app>.koyeb.app/health` every 5 min. The bot also
   serves `/` with live storage stats.
5. Free tier limits: 1 nano instance (256 MB RAM) — this bot runs
   well within that (Telethon + aiohttp + motor only).

## 📋 Commands

| Command | Who | What |
|---|---|---|
| `/start` | everyone | Styled welcome card + channel buttons |
| `/request <name> [ep]` | everyone | Look up an anime/donghua (force-sub & daily limits apply) |
| `/help` | everyone | Help (admins see the admin list) |
| `/addchannel <id> [tag]` | admin | Add a posting channel + quality tag |
| `/channels` | admin | List / delete posting channels |
| `/buttonchannel <link> <name>` | admin | Add a button to the user menu |
| `/forcesub <off\|strict\|request> [channel]` | admin | Force-subscribe mode |
| `/addmongodb <uri>` | admin | Add MongoDB URI (persisted, fail-over) |
| `/mongos` · `/delmongodb <n>` | admin | List / remove MongoDB URIs |
| `/backup` | admin | Dump the whole DB → file in DB channel + chat |
| `/recover` | admin | Restore everything from a backup file |
| `/dbchannel <id>` | admin | Where `/backup` files are posted |
| `/setimg <title> <url>` | admin | Set a custom poster for a title |
| `/setpaid <user_id> [paid\|free]` | admin | Toggle paid tier |
| `/addadmin` · `/deladmin` · `/admins` | owner | Manage admins |
| `/usage` · `/stats` · `/users` | admin | Storage, usage & user stats |
| `/broadcast <text>` | admin | Message all users |
| `/setsource anilive <base_url>` | admin | Custom AniLive mirror |
| `/sync` | admin | Sync commands into the bot menu |
| `/login <pass>` | staff | Unlock the admin panel (default pass: **AYU**) |
| `/logout` | staff | Lock the admin panel again |
| `/setpass <new>` | owner | Change the login password (owner + logged in) |
| `/setdbchannel <id>` | admin | TG DB channel — file archive + backups |
| `/search <name>` | everyone | Search stored files |
| `/get <name> [ep]` | everyone | Get a clean copy (no tag) |

## 🔐 Staff passgate

All admin commands now require a login: send `/login AYU`
(default password). The owner (`OWNER_ID` env) and admins added
via `/addadmin` are always staff; the password gates everyone
else. Only the **owner, while logged in**, can change it with
`/setpass`. `/logout` re-locks the session.

## 🤖 AutoDL — fully automatic posting

The bot can run itself: it watches the donghua API
(`api.parciv.workers.dev/donghua/videos`) for **new episodes**,
downloads them **one by one**, archives every quality in the
**DB channel**, and posts to your channels with:

- poster image (resolved from the series name via
  AniList/AnimXin/LuciferDonghua, falling back to the API thumbnail)
- full caption (genres, rating, year, quality, size)
- the **file-store share link**
- **quality buttons** (360p / 480p / 720p / 1080p / eng)

```
/autodl on                    — enable the watcher
/autodl qualities 720p,1080p  — which qualities to download
/autodl interval 20           — poll every N minutes
/autodl status
/checknew                     — check & post new episodes now
/autodl reset                 — owner: clear the registry
```

### 🎨 The poster design

Every AutoDL post is the designed card on top of the anime /
donghua poster image:

```
Guardian of Daxia

⟣────────────────────⟢

‣ Audio ⌯ Chinese | Indo-Sub
‣ Rating ⌯ 9.0 / 10
‣ Quality ⌯ 1080p | 720p | 480p
‣ Episode ⌯ 05 To 06
‣ Genres ⌯ #Action #Adventure

⟣────────────────────⟢

‣ Synopsis ⌯

<quote box — collapsed / half-shown>

🔗 𝗢𝗼𝗿 𝗡𝗲𝘁𝘄𝗼𝗿𝗸 @yourchannel
📥 𝗙𝗶𝗹𝗲 𝗦𝘁𝗼𝗿𝗲: <links>
```

- **Audio** — Chinese for donghua, Japanese for anime;
  `Indo-Sub` by default, `Indo-Sub | Eng Sub` when the API
  lists eng streams (360p-eng …).
- **Episode** — zero-padded; a range (`05 To 06`) when one run
  catches several episodes of the same series. One post per
  series per run.
- **Synopsis** — extracted from the API description (after the
  `Sinopsis` marker) and wrapped in a collapsible quote box, so
  it stays half-shown.
- **Quality buttons** — one per quality; tapping one (after the
  force-sub gate) DMs the user *every episode of that quality*
  in the post's range, as clean copies.

### 🔁 No re-downloads — MongoDB registry

Every successfully archived file is registered in MongoDB
(`downloaded` collection, keyed
`<series>:<episode>:<quality>`). The registry is loaded into
memory at boot, so **after a redeploy the watcher still knows
what it already posted**. If the API lists episode 1 *and* 2
and episode 1 is registered, **only episode 2 is downloaded** —
per quality: a registered 720p is skipped while a missing
1080p of the same episode is still fetched.

**Quality buttons + force-subscribe:** when force-sub is
active, a user who taps any quality button must join (or send
a join request, in `request` mode) before the bot DMs them
the file.

### ▶ Staff /start buttons EXECUTE

Tapping a command button on the staff /start grid now RUNS the
command: no-arg commands (`/channels`, `/stats`, `/backup`,
`/dbchannels`, `/checknew`, …) execute instantly; commands that
need arguments answer "send the arguments now" and your next
message is executed as that command (e.g. tap 📢 Add Channel,
then send `-100… indo`).

### 🆕 /checknew = tap-to-select menu

`/checknew` now lists every episode the DB doesn't have with
⬜/✅ toggle buttons (two per row), plus **Select all / Clear /
▶ Start / Cancel**. Tap what you want, hit ▶ Start — only the
chosen episodes enter the download queue (progress panel in
the owner DM as usual).

### 🔗 File-store deep links in posts

Every archived file gets a bot deep link in the same shape as
your reference: `https://t.me/<your_bot>?start=<base64>` — the
📥 File Store line of each poster post uses these (one per
episode, best quality). Anyone tapping it passes the force-sub
gate and receives the clean file in DM.

### ⚙️ Qualities = everything available

By default AutoDL takes **every quality the API offers** per
tag (indo: 360p…1080p · eng: every `-eng`). Restrict with
`/autodl qualities 720p,1080p`, or restore with
`/autodl qualities all`. `/autodl on` no longer "seeds" old
episodes: everything missing from the DB registry is picked up
on the first pass.

### 🏷 Channel tags (indo / eng)

`/addchannel <id> [tag]` — tag defaults to **indo**. Change any
time with `/settag <channel_id> <tag>` (bare `/settag` lists all
tags):

- **indo** (default) → base qualities `360p … 1080p`
- **eng** / **eng sub** → only the API's `-eng` streams
  (`360p-eng`, `360-eng`, `720p-eng`, `1080-eng` …); when the API
  has no eng for an episode that channel is simply skipped

### 📥 Job queue + live owner progress

AutoDL downloads run as a **queue**: one file at a time, the rest
queued. The owner gets a live panel in DM (edited every 3 s):

```
📋 Your downloads

🎬 Yao Shen Ji 10th Season Eps 37 [360p]
📋 Video 1/3 · Running

████████░░░░  67.0%
⬆ Telegram: 23.5 MiB / 35.1 MiB
⬇ Source: 30.5 MiB
🚀 1.2 MiB/s · ⏳ ETA 9s

📌 Uploading to Telegram…
🆔 82cc509f · /cancel_82cc509f

📊 Container CPU: 89% of quota · RAM: 262.1 MiB / 512.0 MiB
Active/queued: 1 / 2 · send /cancel_<id> to stop one
```

`/cancel_<jobid>` (owner) stops that job and the queue moves on;
when the batch ends the panel becomes a summary (archived /
failed / cancelled / posted).

### 🏷 Channel tags choose the qualities

- channel tag **indo** (or no tag / a quality tag) → only the
  base streams (`360p` … `1080p`) are downloaded & posted there
- channel tag **eng / eng sub** → only the API's eng streams
  (`360p-eng`, `720p-eng` …) are downloaded & posted there
- mixed setup → the watcher downloads the union once, archives
  it in the DB channel(s), and each channel gets its own poster
  post with only its own qualities + buttons (its own `Indo-Sub`
  or `Eng Sub` line too)

### 🗄 Multiple DB channels from Telegram

```
/adddbchannel <id|@username>   — add a DB archive channel
/dbchannels                    — list them
/deldbchannel <id>             — remove one
```

All of them are saved in MongoDB (`db_channels` collection);
every downloaded file is archived in **every** DB channel and
the registry keeps all copies (serving falls back through them
if one expires). `/setdbchannel` still works as the legacy
single-primary shortcut.

### 🎯 Multi-section watching

`/autodl sections donghua,anime` — watch both families of the
API (each has the same `/videos` listing shape). Default is
`donghua` only.

## 👤 Two different /start screens

- **Staff (owner/admin/pass-logged-in):** the styled welcome
  card **plus every command as buttons** and a command list.
- **Normal users:** the welcome **card** — banner image on
  top, the styled welcome text in a quote box, a credit line,
  and a button grid: `W E L C O M E` letters, 📋 Commands,
  🔍 Sources, 📤 Request, 🗂 File Store, 📜 Changelog,
  ❌ Close and 📢 Channel (your force-sub/public channel).

Set the banner with `/setstartimg <url>` or by replying to a
photo (`/setstartimg off` removes it). The credit line is
configurable via the `credit_line` config key.

## 🗂 TG DB channel (file archive)

Set it with `/setdbchannel <channel_id>`. Every posted file is
**automatically archived there** (with full caption + poster).
Normal users then:

1. `/search <name>` → results with **📥 Get** buttons
2. tap a button (or `/get <name> [ep]`) → the bot sends them a
   **clean copy — no channel tag** — straight to their DM
   (force-subscribe rules still apply).

If a file reference expires, the bot re-fetches it from the DB
channel automatically.

## 🔁 Typical posting flow

1. Admin sends a video file to the bot (PM).
2. Bot runs **mediainfo**, parses name/episode, queries all sources,
   resolves the poster (custom cache → AniList → Jikan → AnimXin →
   LuciferDonghua → AniLive).
3. Confirmation card shows the preview, per-channel toggles,
   **🖼 Custom Image** and **📤 Send Now**.
4. Selected channels receive the file with a rich caption
   (quality, codec, audio, size, duration, score, genres, tag).

## 🛟 Notes

- Files larger than 50 MB need a **local Bot API server** or a bot with
  MTProto upload support; Telethon re-uploads from the downloaded copy
  automatically if re-sending by file reference fails.
- Backups are plain JSON — keep them, and `/recover` anywhere to rebuild
  the bot's whole database.
