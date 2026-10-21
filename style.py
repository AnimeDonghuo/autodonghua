"""
Stylish Unicode font helpers + canned styled messages.

The small-caps map below reproduces exactly the font style the owner asked for
(letters without a small-caps glyph such as 's' and 'x' stay lowercase).
"""

SMALL_CAPS = {
    "a": "ᴀ", "b": "ʙ", "c": "ᴄ", "d": "ᴅ", "e": "ᴇ", "f": "ꜰ", "g": "ɢ",
    "h": "ʜ", "i": "ɪ", "j": "ᴊ", "k": "ᴋ", "l": "ʟ", "m": "ᴍ", "n": "ɴ",
    "o": "ᴏ", "p": "ᴘ", "q": "ꞯ", "r": "ʀ", "s": "s", "t": "ᴛ", "u": "ᴜ",
    "v": "ᴠ", "w": "ᴡ", "x": "x", "y": "ʏ", "z": "ᴢ",
}

SMALL_CAPS_FULL = dict(SMALL_CAPS)
SMALL_CAPS_FULL["s"] = "ꜱ"  # variant that also converts 's'


def small_caps(text: str, full: bool = False) -> str:
    """Convert text to the stylish small-caps font used across the bot."""
    table = SMALL_CAPS_FULL if full else SMALL_CAPS
    out = []
    for ch in text:
        if ch.isupper():
            out.append(ch)
        else:
            out.append(table.get(ch, ch))
    return "".join(out)


def bold(text: str) -> str:
    return f"<b>{text}</b>"


def escape_html(text: str) -> str:
    return (str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


WELCOME_TEMPLATE = (
    "Hɪ {username} 👋,\n\n"
    "Tʜɪs Is Aɴ Aᴅᴠᴀɴᴄᴇᴅ Aɴᴅ Yᴇᴛ Pᴏᴡᴇʀꜰᴜʟ Auto Donghua Bᴏᴛ\n\n"
    "Usɪɴɢ Tʜɪs Bᴏᴛ Yᴏᴜ Cᴀɴ Get Donghua & Anime Files \n\n"
    "Yᴏᴜ Cᴀɴ Aʟsᴏ Request A Sᴘᴇᴄɪꜰɪᴄ Ep Or Anime Usɪɴɢ /request Cmd\n\n"
    "Tʜɪs Bᴏᴛ Aʟsᴏ Sᴜᴘᴘᴏʀᴛs Fʀᴇᴇ And Paid Users\n"
)

RESTRICTED_MSG = (
    "⛔️ <b>Aᴄᴄᴇss Dᴇɴɪᴇᴅ</b>\n\n"
    "Tʜɪs Cᴏᴍᴍᴀɴᴅ Is Oɴʟʏ Fᴏʀ Aᴅᴍɪɴs & Oᴡɴᴇʀ.\n\n"
    "Yᴏᴜ Cᴀɴ Usᴇ:\n"
    "• /start — Start Tʜᴇ Bᴏᴛ\n"
    "• /request — Request Aɴʏ Aɴɪᴍᴇ / Dᴏɴɢʜᴜᴀ\n"
    "• /help — Hᴇʟᴘ"
)

ADMIN_HOME = (
    "🛡 <b>Aᴅᴍɪɴ Pᴀɴᴇʟ</b>\n\n"
    "Wᴇʟᴄᴏᴍᴇ To Tʜᴇ Aᴅᴠᴀɴᴄᴇᴅ Cᴏɴᴛʀᴏʟ Pᴀɴᴇʟ.\n"
    "Usᴇ Tʜᴇ Bᴜᴛᴛᴏɴs Bᴇʟᴏᴡ Or Type A Cᴏᴍᴍᴀɴᴅ."
)


def welcome_text(display_name: str, tier: str = "Fʀᴇᴇ") -> str:
    name = display_name or "Sᴛʀᴀɴɢᴇʀ"
    return WELCOME_TEMPLATE.format(username=name) + f"\n\n💎 Yᴏᴜʀ Plᴀɴ: <b>{tier}</b>"


# ---------------------------------------------------------------------- #
# Channel-post poster caption (the designed card)
# ---------------------------------------------------------------------- #
POST_DIV = "⟣────────────────────⟢"


def _u16(t: str) -> int:
    """Telegram counts entity offsets in UTF-16 code units."""
    return len(t.encode("utf-16-le")) // 2


def post_caption(*, title: str, episode_label: str, audio: str,
                 sub: str, rating: str, qualities: str, genres: str,
                 synopsis: str, network: str, store_links: str,
                 budget: int = 1024):
    """The designed poster caption as PLAIN text plus the synopsis
    span (offset, length) — the caller attaches the bold + collapsed
    quote entities itself, so no HTML parser is ever involved.

    Returns (text, syn_offset, syn_length)."""
    rows = [f"‣ Audio ⌯ {audio} | {sub}"]
    if rating:
        rows.append(f"‣ Rating ⌯ {rating}")
    if qualities:
        rows.append(f"‣ Quality ⌯ {qualities}")
    if episode_label:
        rows.append(f"‣ Episode ⌯ {episode_label}")
    if genres:
        rows.append(f"‣ Genres ⌯ {genres}")
    head = [title or "", "⟣────────────────────⟢"]
    head += rows
    head += ["⟣────────────────────⟢"]
    tail = []
    if network:
        tail.append(network)
    if store_links:
        tail.append(store_links)
    syn = (synopsis or "").strip()
    if not syn:  # no synopsis: no dangling "Synopsis" header either
        return "\n".join(head + tail), 0, 0
    head_syn = head + ["‣ Synopsis ⌯"]
    shell = "\n".join(head_syn + ["{SYN}"] + tail)
    used = _u16(shell) - _u16("{SYN}")
    max_syn = max(60, budget - used)
    if len(syn) > max_syn:
        syn = syn[:max_syn].rsplit(" ", 1)[0].rstrip() + "…"
    text = shell.replace("{SYN}", syn, 1)
    off = _u16("\n".join(head_syn) + "\n")
    return text, off, _u16(syn)
