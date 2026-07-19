"""
Runs every active clone bot as an isolated asyncio task inside the SAME
process as the master bot, using long-polling (not webhooks) so clones
need no public URL or port.

Isolation model: each clone's Application lives in its own task. A crash
inside one clone's update handler is caught by PTB's own error handler
per-update and does NOT propagate here. What DOES propagate here is a
crash in start-up (bad/revoked token, network failure) or an unhandled
exception escaping PTB's polling loop itself — those are caught per-task
in _run_one() so one broken clone can't kill the master bot or other
clones. There is no OS-level isolation: a clone that leaks memory or
spins the CPU can still degrade the whole process. That's a deliberate
tradeoff for running on a single Render dyno, not an oversight — see the
conversation for why.
"""

import asyncio
import logging

from telegram.ext import Application
from telegram.error import InvalidToken

from db import Database

logger = logging.getLogger(__name__)


class CloneRunner:
    def __init__(self, central_db: Database, instance_factory):
        """
        central_db: the Database connected to the CENTRAL Supabase
            (holds user_bots). Used to read clone credentials and to
            update last_active_at / is_active.
        instance_factory: callable(clone_row: dict) -> BotInstance
            Builds a fully-wired BotInstance (handlers registered) for
            one clone row from user_bots. Kept as a factory rather than
            importing BotInstance directly so this module has no
            dependency on bot_instance.py's internals.
        """
        self.central_db = central_db
        self.instance_factory = instance_factory
        self._tasks: dict[int, asyncio.Task] = {}
        self._apps: dict[int, Application] = {}

    async def start_all(self):
        clones = await self.central_db.list_all_active_clones()
        for clone in clones:
            await self.start_one(clone)
        logger.info("CloneRunner: started %d active clone(s)", len(clones))

    async def start_one(self, clone: dict):
        clone_id = clone["id"]
        if clone_id in self._tasks and not self._tasks[clone_id].done():
            logger.warning("Clone %s already running, skipping start", clone_id)
            return
        task = asyncio.create_task(
            self._run_one(clone), name=f"clone-{clone_id}"
        )
        self._tasks[clone_id] = task

    async def stop_one(self, clone_id: int):
        task = self._tasks.pop(clone_id, None)
        app = self._apps.pop(clone_id, None)
        if app is not None:
            try:
                await app.updater.stop()
                await app.stop()
                await app.shutdown()
            except Exception:
                logger.exception("Error stopping clone %s cleanly", clone_id)
        if task is not None and not task.done():
            task.cancel()

    async def stop_all(self):
        for clone_id in list(self._tasks.keys()):
            await self.stop_one(clone_id)

    def is_running(self, clone_id: int) -> bool:
        task = self._tasks.get(clone_id)
        return task is not None and not task.done()

    def get_bot(self, clone_id: int):
        """Returns the running clone's telegram.Bot, or None if it isn't
        currently running. Used by features (e.g. moderator broadcast)
        that must act AS the clone, not as the master bot."""
        app = self._apps.get(clone_id)
        return app.bot if app is not None else None

    async def _run_one(self, clone: dict):
        clone_id = clone["id"]
        try:
            instance = self.instance_factory(clone)
            app: Application = instance.build_application()
        except InvalidToken:
            logger.error("Clone %s has an invalid/revoked bot token — deactivating", clone_id)
            await self.central_db.set_clone_active(clone_id, False)
            return
        except Exception:
            logger.exception("Clone %s failed to build — deactivating", clone_id)
            await self.central_db.set_clone_active(clone_id, False)
            return

        self._apps[clone_id] = app
        try:
            # post_init is NOT used here — it only fires under PTB's own
            # run_polling()/run_webhook(), not under this manual
            # initialize/start/start_polling sequence. Connect the clone's
            # own DB pool explicitly instead, or every query against a
            # clone with its own supabase_url hits `pool.acquire()` on a
            # pool that was never created.
            if hasattr(instance, "connect_db"):
                await instance.connect_db()
            await app.initialize()
            if hasattr(instance, "setup_commands"):
                await instance.setup_commands(app)
            await app.start()
            await app.updater.start_polling(drop_pending_updates=True)
            # Park here until stop_one() cancels this task.
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Clone %s crashed while running", clone_id)
        finally:
            try:
                if app.updater.running:
                    await app.updater.stop()
                if app.running:
                    await app.stop()
                await app.shutdown()
            except Exception:
                logger.exception("Clone %s failed to shut down cleanly", clone_id)
            self._apps.pop(clone_id, None)


async def auto_expiry_job(central_db: Database, runner: CloneRunner, days: int = 8):
    """Run this on a schedule (e.g. PTB JobQueue on the master bot, every
    few hours). Deactivates clones with no activity for `days` and stops
    their running task. Does NOT delete data — matches the spec's
    'deactivate without deleting saved data'."""
    expired = await central_db.expired_clones(days=days)
    for clone in expired:
        logger.info(
            "Auto-expiring clone %s (@%s, owner %s) after %d days idle",
            clone["id"], clone["bot_username"], clone["user_id"], days,
        )
        await central_db.set_clone_active(clone["id"], False)
        await runner.stop_one(clone["id"])
