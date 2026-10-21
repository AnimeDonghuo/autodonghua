"""
Advanced backup & recovery system.

/backup  → serialises the whole database and posts it as a file in the
           configured DB channel (and replies to the admin), so data lives
           outside the bot even if the bot is deleted or banned.
/recover → asks for a backup file; once the admin sends it, everything
           (channels, images, users, files, settings, admins) is restored.
"""
import json
import os
from datetime import datetime, timezone

from config import BACKUP_DIR


def backup_filename() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"autodonghua_backup_{ts}.json"


def write_backup(data: dict, path: str = None) -> str:
    path = path or os.path.join(BACKUP_DIR, backup_filename())
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=1, default=str)
    return path


def load_backup(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict) or "collections" not in data:
        raise ValueError("Not a valid AutoDonghua backup file")
    return data


def backup_summary(data: dict) -> str:
    parts = []
    for name, docs in (data.get("collections") or {}).items():
        parts.append(f"• <b>{name}</b>: {len(docs or [])}")
    return "\n".join(parts)
