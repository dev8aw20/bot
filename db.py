import asyncpg
import ssl

from crypto import encrypt, decrypt


class Database:
    def __init__(self, url: str):
        self.url = url
        self.pool: asyncpg.Pool | None = None

    async def connect(self):
        is_local = any(
            host in self.url
            for host in ("localhost", "127.0.0.1")
        )

        ssl_context = None

        if not is_local:
            ssl_context = ssl.create_default_context()

            # Supabase Session Pooler compatibility
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

        self.pool = await asyncpg.create_pool(
            self.url,
            ssl=ssl_context,
            min_size=1,
            max_size=10,
            statement_cache_size=0,
        )

    async def disconnect(self):
        if self.pool:
            await self.pool.close()

    async def execute(self, query: str, *args):
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args)

    async def fetch(self, query: str, *args):
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def fetchval(self, query: str, *args):
        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, *args)

    async def delete_folder_cascade(self, folder_id: int):
        """Deletes a folder and everything under it (its batches, then
        their audios, plus its folder_pages rows) in one transaction.
        folders/batches/audios/folder_pages have no ON DELETE CASCADE on
        their foreign keys, so deleting the folder row alone fails with an
        FK violation the moment it has any batches OR any folder_pages
        rows (pagination for the output channel — every folder normally
        has at least one) — this does the deletes in the right order,
        atomically, so a mid-way failure can't leave orphans behind."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "DELETE FROM audios WHERE batch_id IN "
                    "(SELECT id FROM batches WHERE folder_id = $1)",
                    folder_id,
                )
                await conn.execute("DELETE FROM batches WHERE folder_id = $1", folder_id)
                await conn.execute("DELETE FROM folder_pages WHERE folder_id = $1", folder_id)
                await conn.execute("DELETE FROM folders WHERE id = $1", folder_id)

    async def init_schema(self):
        await self.execute("""
            CREATE TABLE IF NOT EXISTS folders (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                channel_id TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # channel_id = output channel (where batch/page buttons are posted).
        # source_channel_id = the private channel the bot listens to for
        # incoming audio (replaces the old Drive-link upload flow).
        await self.execute("""
            ALTER TABLE folders
            ADD COLUMN IF NOT EXISTS source_channel_id TEXT
        """)

        await self.execute("""
            CREATE TABLE IF NOT EXISTS batches (
                id SERIAL PRIMARY KEY,
                folder_id INTEGER REFERENCES folders(id),
                total_links INTEGER DEFAULT 0,
                channel_message_id TEXT,
                name TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        await self.execute("""
            ALTER TABLE batches
            ADD COLUMN IF NOT EXISTS name TEXT
        """)

        await self.execute("""
            ALTER TABLE batches
            ADD COLUMN IF NOT EXISTS folder_id INTEGER
            REFERENCES folders(id)
        """)

        await self.execute("""
            CREATE TABLE IF NOT EXISTS audios (
                id SERIAL PRIMARY KEY,
                batch_id INTEGER REFERENCES batches(id),
                telegram_file_id TEXT
            )
        """)

        # Legacy column from the old Google Drive flow. Kept nullable so old
        # rows aren't touched, but nothing writes to it anymore — audios are
        # now ingested directly from a Telegram channel with a file_id already
        # in hand, so there is never a link to download.
        await self.execute("""
            ALTER TABLE audios
            ADD COLUMN IF NOT EXISTS drive_link TEXT
        """)

        await self.execute("""
            ALTER TABLE audios
            ADD COLUMN IF NOT EXISTS episode_no TEXT
        """)

        await self.execute("""
            ALTER TABLE audios
            ADD COLUMN IF NOT EXISTS message_id TEXT
        """)

        # Captured at ingest time (handle_channel_audio) so the master
        # bot's CUSTOM CAPTION feature has something to fill
        # {file_name}/{file_size}/{caption} with. Nullable — rows
        # ingested before this existed just render those placeholders
        # blank.
        await self.execute("""
            ALTER TABLE audios
            ADD COLUMN IF NOT EXISTS file_name TEXT
        """)
        await self.execute("""
            ALTER TABLE audios
            ADD COLUMN IF NOT EXISTS file_size BIGINT
        """)
        await self.execute("""
            ALTER TABLE audios
            ADD COLUMN IF NOT EXISTS caption TEXT
        """)

        await self.execute("""
            CREATE INDEX IF NOT EXISTS idx_audios_episode_no
            ON audios (episode_no)
        """)

        await self.execute("""
            CREATE TABLE IF NOT EXISTS sent_logs (
                id SERIAL PRIMARY KEY,
                user_id TEXT NOT NULL,
                batch_id INTEGER NOT NULL,
                message_ids TEXT NOT NULL,
                delete_at TIMESTAMP NOT NULL
            )
        """)

        await self.execute("""
            CREATE TABLE IF NOT EXISTS folder_pages (
                id SERIAL PRIMARY KEY,
                folder_id INTEGER REFERENCES folders(id),
                page_index INTEGER NOT NULL,
                channel_message_id TEXT,
                UNIQUE(folder_id, page_index)
            )
        """)

        await self.execute("""
            CREATE TABLE IF NOT EXISTS force_join_channels (
                id SERIAL PRIMARY KEY,
                channel_id TEXT NOT NULL UNIQUE,
                invite_link TEXT NOT NULL,
                title TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        await self.execute("""
            CREATE TABLE IF NOT EXISTS join_requests (
                channel_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                requested_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (channel_id, user_id)
            )
        """)

        await self.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY,
                first_seen TIMESTAMP DEFAULT NOW(),
                last_seen TIMESTAMP DEFAULT NOW()
            )
        """)

        # ── Master bot's own settings (Settings menu: CUSTOM CAPTION,
        # CUSTOM BUTTON, PROTECT CONTENT). Singleton row, id is always 1 —
        # this is the ONE master bot's config, not per-clone (clones get
        # their own settings via clone_settings/force_join_channels).
        # custom_buttons stores raw text, one row per line, "Label - URL"
        # per button, "|" separates multiple buttons on the same row —
        # parsed at send time, not at save time, so a bad line only ever
        # breaks rendering, never save.
        await self.execute("""
            CREATE TABLE IF NOT EXISTS bot_settings (
                id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
                custom_caption TEXT,
                custom_buttons TEXT,
                protect_content BOOLEAN NOT NULL DEFAULT FALSE,
                about_support_link TEXT,
                about_bot_link TEXT,
                about_extra_links TEXT
            )
        """)

        # ADD COLUMN IF NOT EXISTS too, for bot_settings tables created
        # before these two columns existed — CREATE TABLE IF NOT EXISTS
        # above is a no-op on an already-existing table.
        await self.execute("""
            ALTER TABLE bot_settings
            ADD COLUMN IF NOT EXISTS about_support_link TEXT
        """)
        await self.execute("""
            ALTER TABLE bot_settings
            ADD COLUMN IF NOT EXISTS about_bot_link TEXT
        """)
        await self.execute("""
            ALTER TABLE bot_settings
            ADD COLUMN IF NOT EXISTS about_extra_links TEXT
        """)

        # ── Clone platform: central registry of every user-created clone ──
        # bot_token / supabase_url / supabase_key are stored ENCRYPTED
        # (see crypto.py). Never write plaintext into these columns —
        # always go through Database.create_clone / Database.get_clone.
        await self.execute("""
            CREATE TABLE IF NOT EXISTS user_bots (
                id SERIAL PRIMARY KEY,
                user_id TEXT NOT NULL,
                bot_token TEXT NOT NULL,
                bot_token_hash TEXT NOT NULL,
                supabase_url TEXT,
                supabase_key TEXT,
                bot_username TEXT,
                bot_name TEXT,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                is_public BOOLEAN NOT NULL DEFAULT TRUE,
                last_active_at TIMESTAMP DEFAULT NOW(),
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(bot_token_hash)
            )
        """)

        # bot_name (the bot's display/first name from getMe(), shown on
        # buttons instead of bot_username) didn't exist before this — rows
        # created earlier will have it NULL. Every read of bot_name falls
        # back to bot_username in the caller, so this is safe to leave
        # NULL rather than backfill (backfilling would need a live
        # getMe() call per clone token, which needs network access this
        # migration step doesn't have).
        await self.execute("""
            ALTER TABLE user_bots
            ADD COLUMN IF NOT EXISTS bot_name TEXT
        """)

        await self.execute("""
            CREATE INDEX IF NOT EXISTS idx_user_bots_user_id
            ON user_bots (user_id)
        """)

        await self.execute("""
            CREATE INDEX IF NOT EXISTS idx_user_bots_is_active
            ON user_bots (is_active)
        """)

        # ── Per-clone feature settings (dashboard: START MSG, NO FORWARD,
        # MODE, ACCESS TOKEN, AUTO DELETE). FORCE SUB used to live here as
        # force_sub_enabled/force_sub_channel_id (single channel) — it's
        # now unified with /forcejoin's multi-channel force_join_channels
        # table instead (see clone_features.py / bot_instance.py). Old
        # rows may still carry those two columns if this table predates
        # the migration; nothing reads them anymore.
        await self.execute("""
            CREATE TABLE IF NOT EXISTS clone_settings (
                clone_id INTEGER PRIMARY KEY REFERENCES user_bots(id) ON DELETE CASCADE,
                start_msg TEXT,
                no_forward_enabled BOOLEAN NOT NULL DEFAULT TRUE,
                hide_owner BOOLEAN NOT NULL DEFAULT FALSE,
                access_token TEXT,
                auto_delete_enabled BOOLEAN NOT NULL DEFAULT FALSE,
                auto_delete_minutes INTEGER NOT NULL DEFAULT 15,
                auto_delete_message TEXT,
                about_text TEXT,
                custom_caption TEXT,
                custom_buttons TEXT
            )
        """)

        # ADD COLUMN IF NOT EXISTS for clone_settings rows created before
        # about_text existed — same reasoning as the bot_settings ALTERs
        # above: CREATE TABLE IF NOT EXISTS is a no-op on an existing table.
        await self.execute("""
            ALTER TABLE clone_settings
            ADD COLUMN IF NOT EXISTS about_text TEXT
        """)

        # Per-clone CUSTOM CAPTION / CUSTOM BUTTON (mirrors bot_settings'
        # master-bot columns of the same name, but scoped per clone_id
        # here). ADD COLUMN IF NOT EXISTS for clone_settings rows created
        # before this migration.
        await self.execute("""
            ALTER TABLE clone_settings
            ADD COLUMN IF NOT EXISTS custom_caption TEXT
        """)
        await self.execute("""
            ALTER TABLE clone_settings
            ADD COLUMN IF NOT EXISTS custom_buttons TEXT
        """)

        await self.execute("""
            CREATE TABLE IF NOT EXISTS clone_moderators (
                clone_id INTEGER NOT NULL REFERENCES user_bots(id) ON DELETE CASCADE,
                user_id TEXT NOT NULL,
                added_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (clone_id, user_id)
            )
        """)

    # ── Per-clone settings helpers ───────────────────────────────────────
    async def get_clone_settings(self, clone_id: int) -> dict:
        row = await self.fetchrow(
            "SELECT * FROM clone_settings WHERE clone_id = $1", clone_id
        )
        if row:
            return dict(row)
        # Lazily create the defaults row the first time it's touched.
        await self.execute(
            "INSERT INTO clone_settings (clone_id) VALUES ($1) "
            "ON CONFLICT (clone_id) DO NOTHING",
            clone_id,
        )
        row = await self.fetchrow(
            "SELECT * FROM clone_settings WHERE clone_id = $1", clone_id
        )
        return dict(row)

    async def update_clone_settings(self, clone_id: int, **fields):
        if not fields:
            return
        await self.get_clone_settings(clone_id)  # ensure row exists
        set_clause = ", ".join(f"{k} = ${i+2}" for i, k in enumerate(fields))
        await self.execute(
            f"UPDATE clone_settings SET {set_clause} WHERE clone_id = $1",
            clone_id, *fields.values(),
        )

    async def get_bot_settings(self) -> dict:
        row = await self.fetchrow("SELECT * FROM bot_settings WHERE id = 1")
        if row:
            return dict(row)
        await self.execute(
            "INSERT INTO bot_settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING"
        )
        row = await self.fetchrow("SELECT * FROM bot_settings WHERE id = 1")
        return dict(row)

    async def update_bot_settings(self, **fields):
        if not fields:
            return
        await self.get_bot_settings()  # ensure row exists
        set_clause = ", ".join(f"{k} = ${i+1}" for i, k in enumerate(fields))
        await self.execute(
            f"UPDATE bot_settings SET {set_clause} WHERE id = 1",
            *fields.values(),
        )

    async def add_moderator(self, clone_id: int, user_id: str):
        await self.execute(
            "INSERT INTO clone_moderators (clone_id, user_id) VALUES ($1, $2) "
            "ON CONFLICT DO NOTHING",
            clone_id, user_id,
        )

    async def list_moderators(self, clone_id: int) -> list:
        rows = await self.fetch(
            "SELECT user_id FROM clone_moderators WHERE clone_id = $1", clone_id
        )
        return [r["user_id"] for r in rows]

    async def remove_moderator(self, clone_id: int, user_id: str):
        await self.execute(
            "DELETE FROM clone_moderators WHERE clone_id = $1 AND user_id = $2",
            clone_id, user_id,
        )

    # ── Clone registry helpers ───────────────────────────────────────────
    # bot_token_hash lets us enforce "this token is already registered"
    # and look up a clone by token WITHOUT decrypting every row to compare
    # (encryption is non-deterministic — same plaintext encrypts to a
    # different ciphertext each time, so it can't be used as a lookup key).
    @staticmethod
    def _token_hash(bot_token: str) -> str:
        import hashlib
        return hashlib.sha256(bot_token.encode()).hexdigest()

    async def count_active_clones(self, user_id: str) -> int:
        return await self.fetchval(
            "SELECT COUNT(*) FROM user_bots WHERE user_id = $1 AND is_active = TRUE",
            user_id,
        )

    async def token_already_registered(self, bot_token: str) -> bool:
        row = await self.fetchval(
            "SELECT 1 FROM user_bots WHERE bot_token_hash = $1",
            self._token_hash(bot_token),
        )
        return row is not None

    async def create_clone(
        self, user_id: str, bot_token: str, bot_username: str,
        supabase_url: str, supabase_key: str,
        bot_name: str = None,
        max_clones: int = 2,
    ) -> int | None:
        """Atomically enforce the per-user clone limit and insert.
        Returns the new clone id, or None if the user is already at
        max_clones (caller shows the limit-reached message in that case).

        supabase_url/supabase_key are REQUIRED, not optional. There is no
        shared-db mode: the schema has no clone_id column on
        folders/batches/audios, so a clone without its own database would
        silently read/write the exact same rows as the master bot and
        every other shared-db clone. Callers must validate the clone's own
        Supabase connection works BEFORE calling this (see master_menu.py's
        receive_supabase_key), so a bad credential fails during setup, not
        as a mystery crash the first time the clone queries its folders.

        pg_advisory_xact_lock serializes concurrent create_clone calls for
        the SAME user_id (hashed to a lock key) for the lifetime of this
        transaction, so two rapid double-taps of "Add Clone" can't both
        pass the count check before either insert commits. Different
        users don't block each other — the lock key is user-scoped.
        """
        if not supabase_url or not supabase_key:
            raise ValueError(
                "supabase_url and supabase_key are required — this platform "
                "does not support shared-db clones."
            )
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtext($1))", user_id
                )
                current = await conn.fetchval(
                    "SELECT COUNT(*) FROM user_bots "
                    "WHERE user_id = $1 AND is_active = TRUE",
                    user_id,
                )
                if current >= max_clones:
                    return None
                return await conn.fetchval("""
                    INSERT INTO user_bots
                        (user_id, bot_token, bot_token_hash, bot_username,
                         bot_name, supabase_url, supabase_key)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    RETURNING id
                """,
                    user_id, encrypt(bot_token), self._token_hash(bot_token),
                    bot_username, bot_name,
                    encrypt(supabase_url),
                    encrypt(supabase_key),
                )

    async def list_clones(self, user_id: str) -> list:
        rows = await self.fetch(
            "SELECT id, bot_username, bot_name, is_active FROM user_bots "
            "WHERE user_id = $1 ORDER BY created_at",
            user_id,
        )
        return [dict(r) for r in rows]

    async def list_all_active_clones(self) -> list:
        """Decrypted clone credentials for the runner to start on boot.
        Only ever call this from trusted platform code, never expose the
        decrypted values back to a Telegram chat."""
        rows = await self.fetch(
            "SELECT id, user_id, bot_token, supabase_url, supabase_key, "
            "bot_username, is_public FROM user_bots WHERE is_active = TRUE"
        )
        out = []
        for r in rows:
            d = dict(r)
            d["bot_token"] = decrypt(d["bot_token"])
            d["supabase_url"] = decrypt(d["supabase_url"]) if d["supabase_url"] else None
            d["supabase_key"] = decrypt(d["supabase_key"]) if d["supabase_key"] else None
            out.append(d)
        return out

    async def get_clone(self, clone_id: int) -> dict | None:
        row = await self.fetchrow(
            "SELECT id, user_id, bot_token, supabase_url, supabase_key, "
            "bot_username, bot_name, is_active, is_public FROM user_bots WHERE id = $1",
            clone_id,
        )
        if not row:
            return None
        d = dict(row)
        d["bot_token"] = decrypt(d["bot_token"])
        d["supabase_url"] = decrypt(d["supabase_url"]) if d["supabase_url"] else None
        d["supabase_key"] = decrypt(d["supabase_key"]) if d["supabase_key"] else None
        return d

    async def set_clone_active(self, clone_id: int, is_active: bool):
        await self.execute(
            "UPDATE user_bots SET is_active = $1 WHERE id = $2",
            is_active, clone_id,
        )

    async def touch_clone_activity(self, clone_id: int):
        await self.execute(
            "UPDATE user_bots SET last_active_at = NOW() WHERE id = $1",
            clone_id,
        )

    async def expired_clones(self, days: int = 8) -> list:
        """Clones inactive for `days` — used by the auto-expiry job."""
        rows = await self.fetch(
            "SELECT id, user_id, bot_username FROM user_bots "
            "WHERE is_active = TRUE AND last_active_at < NOW() - ($1 || ' days')::interval",
            str(days),
        )
        return [dict(r) for r in rows]

    async def delete_clone(self, clone_id: int):
        await self.execute("DELETE FROM user_bots WHERE id = $1", clone_id)