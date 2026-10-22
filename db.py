"""
Storage layer for AutoDonghua Bot.

- Primary backend: MongoDB (Motor async client). Multiple URIs can be added
  with /addmongodb — if the primary storage is full / fails, the bot
  automatically fails over to the next URI in the list.
- Fallback backend: local JSON file (data/local_db.json) so the bot keeps
  working even with zero MongoDB configured.
"""
import asyncio
import functools
import json
import os
import time
from datetime import datetime, timezone

from config import CONFIG, DATA_DIR

try:
    from pymongo.errors import PyMongoError
except Exception:  # pragma: no cover
    class PyMongoError(Exception):
        pass

COLLECTIONS = [
    "config", "channels", "button_channels", "images", "files",
    "users", "admins", "requests", "backups", "forcesub",
    "downloaded", "auth", "posts", "db_channels", "deeplinks",
]

LOCAL_DB_PATH = os.path.join(DATA_DIR, "local_db.json")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class LocalCollection:
    """Minimal mongo-like collection backed by a Python list."""

    def __init__(self, data: dict, name: str):
        self.data = data
        self.name = name

    @property
    def docs(self):
        return self.data.setdefault(self.name, [])

    @staticmethod
    def _match(doc, query):
        if not query:
            return True
        for key, val in query.items():
            if isinstance(val, dict) and "$in" in val:
                if doc.get(key) not in val["$in"]:
                    return False
            elif doc.get(key) != val:
                return False
        return True

    async def find_one(self, query):
        for d in self.docs:
            if self._match(d, query):
                return d
        return None

    async def find(self, query=None, limit=0):
        out = [d for d in self.docs if self._match(d, query)]
        return out[:limit] if limit else out

    async def insert_one(self, doc):
        self.docs.append(doc)
        return doc

    async def update_one(self, query, update, upsert=True):
        for d in self.docs:
            if self._match(d, query):
                self._apply(d, update)
                return d
        if upsert:
            d = dict(query)
            self._apply(d, update)
            self.docs.append(d)
            return d
        return None

    async def delete_one(self, query):
        for i, d in enumerate(self.docs):
            if self._match(d, query):
                return self.docs.pop(i)
        return None

    async def count_documents(self, query=None):
        return len([d for d in self.docs if self._match(d, query)])

    async def replace_all(self, docs):
        self.data[self.name] = list(docs)

    @staticmethod
    def _apply(doc, update):
        for op, fields in update.items():
            if op == "$set":
                doc.update(fields)
            elif op == "$inc":
                for k, v in fields.items():
                    doc[k] = doc.get(k, 0) + v
            elif op == "$push":
                for k, v in fields.items():
                    doc.setdefault(k, []).append(v)
            elif op == "$addToSet":
                for k, v in fields.items():
                    arr = doc.setdefault(k, [])
                    if v not in arr:
                        arr.append(v)


class LocalDB:
    def __init__(self):
        self.data = {}
        self.load()

    def load(self):
        if os.path.exists(LOCAL_DB_PATH):
            try:
                with open(LOCAL_DB_PATH, "r", encoding="utf-8") as f:
                    self.data = json.load(f)
            except Exception:
                self.data = {}

    def save(self):
        tmp = LOCAL_DB_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=1, default=str)
        os.replace(tmp, LOCAL_DB_PATH)


class Store:
    def __init__(self):
        self.client = None
        self.db = None
        self.uris = list(CONFIG.get("mongo_uris") or [])
        self.active_uri = None
        self.mode = "local"
        self.local = LocalDB()

    # ------------------------------------------------------------------ #
    # Connection management
    # ------------------------------------------------------------------ #
    async def init(self):
        for uri in self.uris:
            if await self._try_connect(uri):
                self.mode = "mongo"
                self.active_uri = uri
                print(f"[DB] Connected to MongoDB (primary)")
                await self.load_runtime()
                return
        print("[DB] No MongoDB reachable — using local JSON storage")

    async def _try_connect(self, uri, timeout=8000):
        try:
            from motor.motor_asyncio import AsyncIOMotorClient
            client = AsyncIOMotorClient(
                uri, serverSelectionTimeoutMS=timeout)
            await client.admin.command("ping")
            self.client = client
            self.db = client.autodonghua
            self.active_uri = uri
            return True
        except Exception as exc:
            print(f"[DB] MongoDB unreachable ({uri[:24]}...): {exc}")
            return False

    async def _failover(self):
        """Retry every URI several times before trusting that Mongo
        is really down — one blip must not drop us to ephemeral
        local JSON (Koyeb disk is wiped on restart)."""
        for round_no in range(3):
            ordered = [self.active_uri] + [u for u in self.uris
                                           if u != self.active_uri]
            for uri in ordered:
                if uri and await self._try_connect(uri, timeout=10000):
                    if uri != self.active_uri:
                        print(f"[DB] Failed over to {uri[:24]}...")
                    else:
                        print("[DB] MongoDB connection restored")
                    self.mode = "mongo"
                    await self.load_runtime()
                    return
            await asyncio.sleep(2 + 2 * round_no)
        print("[DB] All MongoDB URIs failed — using local JSON storage")
        self.mode = "local"
        self.client = None
        self.db = None

    async def ensure_mongo(self, timeout=45) -> bool:
        """Block briefly until MongoDB is back (batches depend on
        the registry + db channels living there)."""
        if self.mode == "mongo":
            return True
        end = time.time() + timeout
        while time.time() < end:
            await self.init()
            if self.mode == "mongo":
                return True
            await asyncio.sleep(5)
        return self.mode == "mongo"

    # ------------------------------------------------------------------ #
    # Runtime config mirrored in MongoDB (Koyeb disk is ephemeral)
    # ------------------------------------------------------------------ #
    RUNTIME_KEYS = ("owner_ids", "mongo_uris", "log_channel",
                    "db_channel", "bot_name")

    async def save_runtime(self):
        if self.mode != "mongo":
            return
        doc = {k: CONFIG.get(k) for k in self.RUNTIME_KEYS}
        doc["_id"] = "runtime"
        try:
            await self.db["config"].update_one(
                {"_id": "runtime"}, {"$set": doc}, upsert=True)
        except Exception as exc:
            print("[DB] runtime config save failed:", exc)

    async def load_runtime(self):
        if self.mode != "mongo":
            return
        try:
            doc = await self.db["config"].find_one({"_id": "runtime"})
        except Exception:
            return
        if not doc:
            return
        changed = False
        for k in self.RUNTIME_KEYS:
            val = doc.get(k)
            if val in (None, "", []):
                continue
            if k == "mongo_uris":
                for u in val:
                    if u not in self.uris:
                        self.uris.append(u)
                        changed = True
                CONFIG["mongo_uris"] = self.uris
            elif CONFIG.get(k) != val:
                CONFIG[k] = val
                changed = True
        if changed:
            print("[DB] runtime config restored from MongoDB "
                  "(owner/uris/channels)")

    def col(self, name):
        if self.mode == "mongo":
            return self.db[name]
        return LocalCollection(self.local.data, name)

    async def find(self, name, query=None, limit=0, sort=None):
        """Backend-safe find(): Motor's find() returns a CURSOR
        (must be drained with to_list), while the local fallback's
        find() is an async function returning a list. Awaiting the
        Motor cursor directly crashes with
        'AsyncIOMotorCursor object can't be awaited'."""
        coll = self.col(name)
        if self.mode == "mongo":
            cur = coll.find(query or {})
            if sort:
                cur = cur.sort(sort)
            if limit:
                cur = cur.limit(limit)
            return await cur.to_list(length=limit or None)
        return await coll.find(query or {}, limit=limit)

    def resilient(fn):  # noqa: N805
        @functools.wraps(fn)
        async def wrapper(self, *a, **kw):
            if self.mode != "mongo":
                return await fn(self, *a, **kw)
            try:
                return await fn(self, *a, **kw)
            except PyMongoError:
                pass  # connection-class error: retry, then fail over
            # any other exception is a real bug — never blame the DB
            try:
                await asyncio.sleep(0.8)
                return await fn(self, *a, **kw)
            except PyMongoError:
                await self._failover()
                return await fn(self, *a, **kw)
        return wrapper

    async def heal_loop(self):
        """If a transient outage dropped us to local JSON, keep
        trying to get back to MongoDB without a redeploy."""
        while True:
            await asyncio.sleep(30)
            if self.mode == "mongo" or not self.uris:
                continue
            print("[DB] trying to reconnect to MongoDB…")
            before = self.mode
            await self.init()
            if self.mode == "mongo" and before != "mongo":
                print("[DB] reconnected — MongoDB is primary again")

    async def add_mongo_uri(self, uri: str) -> bool:
        uri = uri.strip()
        if uri not in self.uris:
            self.uris.append(uri)
            CONFIG["mongo_uris"] = self.uris
            from config import save_config
            save_config()
        await self.save_runtime()
        return True

    # ------------------------------------------------------------------ #
    # Config key/value
    # ------------------------------------------------------------------ #
    @resilient
    async def get_config(self, key, default=None):
        doc = await self.col("config").find_one({"_id": "global"})
        if doc and key in doc:
            return doc[key]
        return default

    @resilient
    async def set_config(self, key, value):
        await self.col("config").update_one(
            {"_id": "global"}, {"$set": {key: value, "updated_at": _now()}}, upsert=True)
        self.local.save()

    # ------------------------------------------------------------------ #
    # Channels
    # ------------------------------------------------------------------ #
    @resilient
    async def add_channel(self, channel_id: int, tag: str = "",
                            title: str = None):
        raw = int(channel_id)
        cid = self._norm_id(raw) or raw
        if raw != cid:
            await self.col("channels").delete_one({"_id": raw})
        await self.col("channels").update_one(
            {"_id": cid},
            {"$set": {"tag": tag or "", "title": title,
                        "added_at": _now()}}, upsert=True)

    @resilient
    async def remove_channel(self, channel_id: int):
        await self.col("channels").delete_one({"_id": int(channel_id)})

    @resilient
    async def list_channels(self):
        docs = [dict(d) for d in await self.find("channels")]
        for d in docs:  # heal bare-id rows from old builds
            n = self._norm_id(d.get("_id"))
            if n and n != d["_id"]:
                d["_id"] = n
        return docs

    @resilient
    async def set_channel_tag(self, channel_id: int, tag: str):
        channel_id = self._norm_id(channel_id) or int(channel_id)
        await self.col("channels").update_one(
            {"_id": int(channel_id)},
            {"$set": {"tag": tag}}, upsert=False)

    @resilient
    async def add_button_channel(self, name: str, url: str):
        await self.col("button_channels").update_one(
            {"_id": name}, {"$set": {"url": url, "added_at": _now()}}, upsert=True)

    @resilient
    async def remove_button_channel(self, name: str):
        await self.col("button_channels").delete_one({"_id": name})

    @resilient
    async def list_button_channels(self):
        return await self.find("button_channels")

    # ------------------------------------------------------------------ #
    # Image cache (keyed by normalized anime title)
    # ------------------------------------------------------------------ #
    @resilient
    async def get_image(self, norm_title: str):
        return await self.col("images").find_one({"_id": norm_title})

    @resilient
    async def save_image(self, norm_title: str, url: str = None, source: str = "custom",
                         photo: dict = None, custom: bool = False):
        doc = {"_id": norm_title, "updated_at": _now(), "custom": custom}
        if url:
            doc["type"] = "url"
            doc["url"] = url
        elif photo:
            doc["type"] = "photo"
            doc.update(photo)
        doc["source"] = source
        await self.col("images").update_one({"_id": norm_title}, {"$set": doc}, upsert=True)

    @resilient
    async def list_images(self, limit=50):
        return await self.find("images", limit=limit)

    # ------------------------------------------------------------------ #
    # Users / admins
    # ------------------------------------------------------------------ #
    @resilient
    async def get_user(self, user_id: int):
        return await self.col("users").find_one({"_id": int(user_id)})

    @resilient
    async def ensure_user(self, user_id: int, username: str = None, first_name: str = None):
        await self.col("users").update_one(
            {"_id": int(user_id)},
            {"$set": {"username": username, "first_name": first_name,
                      "last_seen": _now()},
             "$setOnInsert": {"tier": "free", "requests_today": 0,
                              "request_date": _now()[:10], "bytes_used": 0,
                              "joined_at": _now()}},
            upsert=True)

    @resilient
    async def update_user(self, user_id: int, update: dict):
        await self.col("users").update_one({"_id": int(user_id)}, update)

    @resilient
    async def inc_request(self, user_id: int):
        await self.col("users").update_one(
            {"_id": int(user_id)}, {"$inc": {"requests_today": 1}})

    @resilient
    async def set_tier(self, user_id: int, tier: str):
        await self.col("users").update_one(
            {"_id": int(user_id)}, {"$set": {"tier": tier}}, upsert=True)

    @resilient
    async def add_usage(self, user_id: int, nbytes: int):
        await self.col("users").update_one(
            {"_id": int(user_id)}, {"$inc": {"bytes_used": int(nbytes)}})

    @resilient
    async def list_users(self, limit=100):
        return await self.find("users", limit=limit)

    @resilient
    async def count_users(self):
        return await self.col("users").count_documents()

    @resilient
    async def is_admin(self, user_id: int) -> bool:
        if int(user_id) in (CONFIG.get("owner_ids") or []):
            return True
        doc = await self.col("admins").find_one({"_id": int(user_id)})
        return bool(doc)

    @resilient
    async def add_admin(self, user_id: int):
        await self.col("admins").update_one(
            {"_id": int(user_id)}, {"$set": {"added_at": _now()}}, upsert=True)

    @resilient
    async def remove_admin(self, user_id: int):
        await self.col("admins").delete_one({"_id": int(user_id)})

    @resilient
    async def list_admins(self):
        return await self.find("admins")

    # ------------------------------------------------------------------ #
    # File records
    # ------------------------------------------------------------------ #
    @resilient
    async def record_file(self, doc: dict):
        doc.setdefault("added_at", _now())
        doc.setdefault("_id", f"{doc.get('user_id', 0)}:{int(time.time())}")
        await self.col("files").insert_one(doc)
        await self.col("users").update_one(
            {"_id": int(doc.get("user_id", 0))}, {"$inc": {"bytes_used": int(doc.get("size_bytes", 0))}})

    @resilient
    async def list_files(self, limit=20):
        return (await self.find("files", limit=limit))[::-1]

    @resilient
    async def count_files(self):
        return await self.col("files").count_documents()

    @resilient
    async def find_file_by_title(self, norm_title: str):
        return await self.find("files", {"norm_title": norm_title})

    @staticmethod
    def _norm_id(value):
        """Any channel-id spelling -> full -100… form (None if not
        a number). Bare positive ids stored by old builds or typed
        by hand would otherwise be read as USER ids."""
        try:
            num = int(value)
        except (TypeError, ValueError):
            return None
        if num < 0:
            return num if num <= -1000000000000 else num - 1000000000000
        return -num if num >= 1000000000000 else -1000000000000 - num

    @resilient
    async def list_db_channels(self):
        """Every TG DB channel added from Telegram."""
        return await self.find("db_channels")

    @resilient
    async def add_db_channel(self, channel_id: int, title: str = None):
        raw = int(channel_id)
        cid = self._norm_id(raw) or raw
        if raw != cid:  # migrate a stale bare-id row
            await self.col("db_channels").delete_one({"_id": raw})
        await self.col("db_channels").update_one(
            {"_id": cid},
            {"$set": {"title": title, "added_at": _now()}},
            upsert=True)

    @resilient
    async def remove_db_channel(self, channel_id: int):
        raw = int(channel_id)
        cid = self._norm_id(raw) or raw
        await self.col("db_channels").delete_one({"_id": cid})
        if raw != cid:  # also drop a stale bare-id row
            await self.col("db_channels").delete_one({"_id": raw})

    @resilient
    async def db_channel_ids(self):
        """All DB channels (the legacy single config value is the
        fallback until the first /adddbchannel). Ids are normalized
        to the full -100… form."""
        ids = [self._norm_id(d["_id"]) or d["_id"]
               for d in await self.find("db_channels")]
        if not ids:
            cur = await self.get_config("db_channel")
            if cur:
                ids = [self._norm_id(cur) or int(cur)]
        return list(dict.fromkeys(ids))  # drop duplicates

    @resilient
    async def get_file(self, file_id: str):
        return await self.col("files").find_one({"_id": file_id})

    @resilient
    async def find_file_by_api(self, api_id: int, quality: str):
        docs = await self.find("files", {"api_id": int(api_id)})
        for d in docs:
            if d.get("quality") == quality:
                return d
        return None

    @resilient
    async def search_files(self, query: str, limit=20):
        """Fuzzy search stored files by title."""
        from parser import fuzzy_ratio
        docs = await self.find("files", limit=500)
        scored = []
        for d in docs:
            s = fuzzy_ratio(query, d.get("title", ""))
            if s > 0.45:
                scored.append((s, d))
        scored.sort(key=lambda x: -x[0])
        return [d for _, d in scored[:limit]]

    # ------------------------------------------------------------------ #
    # Requests log
    # ------------------------------------------------------------------ #
    @resilient
    async def log_request(self, user_id: int, query: str, ep=None):
        await self.col("requests").insert_one(
            {"_id": f"{int(user_id)}:{int(time.time())}", "user_id": int(user_id),
             "query": query, "episode": ep, "at": _now()})

    # ------------------------------------------------------------------ #
    # Force-subscribe settings
    # ------------------------------------------------------------------ #
    @resilient
    async def get_forcesub(self):
        doc = await self.col("forcesub").find_one({"_id": "settings"})
        return doc or {"_id": "settings", "mode": "off", "channels": []}

    @resilient
    async def set_forcesub(self, mode: str, channels: list):
        await self.col("forcesub").update_one(
            {"_id": "settings"}, {"$set": {"mode": mode, "channels": channels}}, upsert=True)

    # ------------------------------------------------------------------ #
    # Backups
    # ------------------------------------------------------------------ #
    @resilient
    async def log_backup(self, doc: dict):
        await self.col("backups").insert_one(doc)

    @resilient
    async def list_backups(self, limit=10):
        return (await self.find("backups", limit=limit))[::-1]

    # ------------------------------------------------------------------ #
    # Stats / whole-db backup & restore
    # ------------------------------------------------------------------ #
    async def stats(self) -> dict:
        out = {"mode": self.mode, "collections": {}}
        if self.mode == "mongo":
            try:
                st = await self.db.command("dbstats")
                out["db_size_mb"] = round(st.get("dataSize", 0) / 1048576, 2)
                out["storage_mb"] = round(st.get("storageSize", 0) / 1048576, 2)
            except Exception:
                out["db_size_mb"] = None
        else:
            try:
                out["db_size_mb"] = round(os.path.getsize(LOCAL_DB_PATH) / 1048576, 2)
            except Exception:
                out["db_size_mb"] = 0.0
        for name in COLLECTIONS:
            try:
                out["collections"][name] = await self.col(name).count_documents({})
            except Exception:
                out["collections"][name] = 0
        return out

    async def backup_all(self) -> dict:
        data = {"bot": "autodonghua", "version": 1, "exported_at": _now(),
                "storage_mode": self.mode, "collections": {}}
        for name in COLLECTIONS:
            docs = await self.find(name)
            data["collections"][name] = docs
        return data

    @resilient
    async def restore_all(self, data: dict) -> dict:
        counts = {}
        for name, docs in (data.get("collections") or {}).items():
            if self.mode == "mongo":
                await self.db[name].delete_many({})
                clean = [{k: v for k, v in d.items() if k != "_id"} or {"_id": d.get("_id")}
                         for d in docs]
                # keep _id when present and valid
                for d, orig in zip(clean, docs):
                    if orig.get("_id") is not None:
                        d["_id"] = orig["_id"]
                if clean:
                    await self.db[name].insert_many(clean)
            else:
                self.local.data[name] = docs
            counts[name] = len(docs)
        self.local.save()
        return counts
