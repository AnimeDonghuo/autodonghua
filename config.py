"""
AutoDonghua Bot - Configuration loader.
Loads config from config.json (persisted, survives restarts) with env-var overrides.
"""
import json
import os
import re

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")
DATA_DIR = os.path.join(BASE_DIR, "data")
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")
THUMB_DIR = os.path.join(BASE_DIR, "thumbs")
BACKUP_DIR = os.path.join(BASE_DIR, "backups")

for _d in (DATA_DIR, DOWNLOAD_DIR, THUMB_DIR, BACKUP_DIR):
    os.makedirs(_d, exist_ok=True)

DEFAULT_CONFIG = {
    "api_id": None,            # Telegram API ID (https://my.telegram.org)
    "api_hash": "",            # Telegram API hash
    "bot_token": "",           # Bot token from @BotFather
    "owner_ids": [],           # Super-admin user ids (comma separated env OWNER_ID also works)
    "log_channel": None,       # Where admin activity / requests are logged
    "db_channel": None,        # Channel where /backup files are posted
    "mongo_uris": [],          # MongoDB connection URIs (first working one is primary)
    "bot_name": "Auto Donghua Bot",
}


def load_config():
    cfg = dict(DEFAULT_CONFIG)
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                cfg.update(loaded)
        except Exception:
            pass

    # Environment variable overrides (useful for hosting platforms)
    env_api_id = os.environ.get("API_ID") or os.environ.get("APP_ID")
    if env_api_id:
        try:
            cfg["api_id"] = int(env_api_id)
        except ValueError:
            pass
    if os.environ.get("API_HASH"):
        cfg["api_hash"] = os.environ["API_HASH"].strip()
    if os.environ.get("BOT_TOKEN"):
        cfg["bot_token"] = os.environ["BOT_TOKEN"].strip()
    owner_env = os.environ.get("OWNER_ID") or os.environ.get("OWNER_IDS")
    if owner_env:
        ids = [int(x) for x in re.split(r"[,\s]+", owner_env.strip()) if x.strip().isdigit()]
        if ids:
            cfg["owner_ids"] = ids

    # MongoDB from env (ideal for Koyeb: set MONGO_URI once as a secret —
    # the ephemeral disk is wiped on every restart, so /addmongodb alone
    # is not durable there; env vars always survive restarts)
    mongo_env = os.environ.get("MONGO_URI") or os.environ.get("MONGODB_URI")
    if mongo_env and not cfg.get("mongo_uris"):
        cfg["mongo_uris"] = [u.strip() for u in mongo_env.split(",") if u.strip()]
    if os.environ.get("ANILIVE_BASE"):
        cfg["anilive_base"] = os.environ["ANILIVE_BASE"].strip().rstrip("/")

    return cfg


CONFIG = load_config()


def save_config():
    """Persist config to disk so settings survive restarts (even Mongo-less ones)."""
    tmp = CONFIG_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(CONFIG, f, indent=2)
    os.replace(tmp, CONFIG_PATH)
