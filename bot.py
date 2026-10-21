"""
AutoDonghua Bot — advanced auto file-store & post bot for donghua/anime.

Run:  python3 bot.py
"""
import asyncio
import sys

import core
from config import CONFIG


async def main():
    if not CONFIG.get("api_id") or not CONFIG.get("api_hash") \
            or not CONFIG.get("bot_token"):
        print("=" * 60)
        print("❌ Missing credentials!")
        print("   Fill api_id / api_hash / bot_token in config.json")
        print("   or set API_ID, API_HASH, BOT_TOKEN env vars.")
        print("=" * 60)
        sys.exit(1)

    print("🔌 Connecting storage backend…")
    await core.store.init()
    await core.load_auth()
    asyncio.ensure_future(core.store.heal_loop())

    # Koyeb-friendly health endpoint (keeps free-tier instance alive)
    await core.start_health_server()

    # register the conversation waiter hook FIRST so /recover and the
    # custom-image flow always work
    core.install_waiter_hook()

    # then register all handlers (registration order matters)
    import handlers_user
    import handlers_files
    import handlers_callbacks
    import handlers_admin
    import handlers_autodl

    # AutoDL background watcher (polls the donghua API)
    import autodl
    await autodl.load_registry()
    print(f"📥 AutoDL registry: {await autodl.registry_size()} "
          f"archived file(s)")
    asyncio.ensure_future(autodl.autodl_loop())

    async def _resume_batch():
        """Restart mid-batch? Continue where it left off: anything
        absent from the registry (or a batch still pending when we
        died) is re-queued once, ~25 s after boot."""
        await asyncio.sleep(25)
        try:
            pend = await core.store.get_config("pending_batch")
            st = await autodl.get_settings()
            if not pend and not st.get("enabled"):
                return
            if autodl.LOAD.locked():
                return
            fresh = await autodl.check_new()
            if fresh and not autodl.LOAD.locked():
                print(f"[autodl] resume after restart: {len(fresh)} "
                      f"episode(s) incomplete — queueing")
                await autodl.start_batch(fresh)
        except Exception as exc:
            print("[autodl] resume pass failed:", repr(exc)[:200])
    asyncio.ensure_future(_resume_batch())

    async def _backlog_pass():
        """Any channel missing cards for files we already archived
        gets them now, one by one (floodwait-safe)."""
        await asyncio.sleep(50)
        try:
            n = await autodl.backlog_cards(None)
            if n:
                print(f"[autodl] backlog cards created: {n}")
        except Exception as exc:
            print("[autodl] backlog pass failed:", repr(exc)[:200])
    asyncio.ensure_future(_backlog_pass())

    print("🚀 Connecting to Telegram…")
    await core.app.start(bot_token=CONFIG["bot_token"])
    core.bot_user = await core.app.get_me()
    print(f"✅ Bot ready as @{core.bot_user.username}")
    print(f"🗄 Storage mode: {core.store.mode.upper()}")
    print("💡 Tip: send /sync once to refresh the command menu.")
    await core.app.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
