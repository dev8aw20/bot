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

    async def rebalance_folder_batches(self, folder_id: int, folder_name: str, batch_max: int) -> list[int]:
        """Re-sorts every audio in a folder by episode number (numeric ones
        ascending; non-numeric ids — e.g. the song fallback identifiers —
        keep their original upload order and sort after all numeric ones)
        and repacks them into batches of batch_max, filling existing
        batch ids in their oldest-to-newest order.

        Batch ids/order are never changed, only which audios live in
        which batch, so 'Batch N' labels stay correct. This means a
        late-arriving episode (e.g. Ep3 turning up after Ep51 already
        exists) slots into the right spot and pushes everything after it
        forward, cascading into later batches — same as a paper folder
        where Ep51 would spill into Batch 2 once Ep3 takes its rightful
        place in Batch 1.

        New batches are created only if there are literally more audios
        than existing batches can hold. Returns the list of batch ids
        that were touched, so the caller can re-render only those
        channel pages instead of the whole folder."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    """SELECT a.id, a.episode_no FROM audios a
                       JOIN batches b ON b.id = a.batch_id
                       WHERE b.folder_id = $1""",
                    folder_id,
                )
                if not rows:
                    return []

                def sort_key(r):
                    ep = r["episode_no"]
                    if ep is not None and ep.lstrip("-").isdigit():
                        return (0, int(ep), r["id"])
                    return (1, 0, r["id"])

                rows_sorted = sorted(rows, key=sort_key)

                batch_rows = await conn.fetch(
                    "SELECT id FROM batches WHERE folder_id = $1 ORDER BY id", folder_id
                )
                batch_ids = [r["id"] for r in batch_rows]

                chunks = [
                    rows_sorted[i:i + batch_max]
                    for i in range(0, len(rows_sorted), batch_max)
                ]

                # Only happens if there are more audios than existing
                # batches can hold — normally covered already because the
                # caller creates a holding batch before calling this, but
                # guarded here too in case a folder somehow has zero batches.
                while len(chunks) > len(batch_ids):
                    new_id = await conn.fetchval(
                        "INSERT INTO batches (folder_id, total_links, name) "
                        "VALUES ($1, 0, $2) RETURNING id",
                        folder_id, f"{folder_name} — Batch {len(batch_ids) + 1}",
                    )
                    batch_ids.append(new_id)

                touched = []
                for idx, batch_id in enumerate(batch_ids):
                    chunk = chunks[idx] if idx < len(chunks) else []
                    if chunk:
                        audio_ids = [r["id"] for r in chunk]
                        await conn.execute(
                            "UPDATE audios SET batch_id = $1 WHERE id = ANY($2::int[])",
                            batch_id, audio_ids,
                        )
                    await conn.execute(
                        "UPDATE batches SET total_links = $1 WHERE id = $2",
                        len(chunk), batch_id,
                    )
                    touched.append(batch_id)

                return touched

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

        # /ban /unban (master bot's OWNER_ID for the master's own users
        # table; a clone owner's self.owner_id for that clone's users
        # table — see bot.py / bot_instance.py). This table is always
        # THE CALLER'S OWN db (central db for master, self.db for a
        # clone), never shared across clones, so no clone_id column is
        # needed here.
        await self.execute("""
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS banned BOOLEAN NOT NULL DEFAULT FALSE
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

        # /cban /cunban — MASTER OWNER ONLY, forcibly disables a clone
        # bot. Deliberately a separate column from is_active: is_active
        # is the CLONE OWNER's own on/off switch (clone_toggle button),
        # and a plain is_active=FALSE would let them just flip their own
        # clone back on, defeating a master-level ban. cb_clone_toggle /
        # cb_clone_restart in master_menu.py must refuse when banned=TRUE.
        await self.execute("""
            ALTER TABLE user_bots
            ADD COLUMN IF NOT EXISTS banned BOOLEAN NOT NULL DEFAULT FALSE
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

        # frozen_until: NULL = not frozen. When set to a future timestamp,
        # the moderator temporarily loses staff access (see bot_instance.py
        # _is_staff) without being removed from the moderators list — the
        # owner can un-freeze early, or it lapses on its own once NOW()
        # passes it (no cron needed, every check is a live comparison).
        await self.execute("""
            ALTER TABLE clone_moderators
            ADD COLUMN IF NOT EXISTS frozen_until TIMESTAMP
        """)

    # ── /ban /unban helpers (master bot's OWNER_ID, or a clone owner's
    # self.owner_id — always operates on THIS Database instance's own
    # `users` table, central db for the master bot, self.db for a
    # clone) ─────────────────────────────────────────────────────────────
    async def ban_user(self, user_id: str):
        """Upsert so a user can be pre-banned even if they've never
        /start'ed this bot yet, not just an UPDATE on an existing row."""
        await self.execute(
            """INSERT INTO users (user_id, banned) VALUES ($1, TRUE)
               ON CONFLICT (user_id) DO UPDATE SET banned = TRUE""",
            user_id,
        )

    async def unban_user(self, user_id: str):
        await self.execute(
            "UPDATE users SET banned = FALSE WHERE user_id = $1", user_id
        )

    async def is_user_banned(self, user_id: str) -> bool:
        return bool(
            await self.fetchval(
                "SELECT banned FROM users WHERE user_id = $1", user_id
            )
        )

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

    async def list_moderators_detailed(self, clone_id: int) -> list:
        """Like list_moderators but includes frozen_until, so the dashboard
        can show freeze status without a second query per moderator."""
        rows = await self.fetch(
            "SELECT user_id, frozen_until FROM clone_moderators "
            "WHERE clone_id = $1 ORDER BY added_at",
            clone_id,
        )
        return [dict(r) for r in rows]

    async def remove_moderator(self, clone_id: int, user_id: str):
        await self.execute(
            "DELETE FROM clone_moderators WHERE clone_id = $1 AND user_id = $2",
            clone_id, user_id,
        )

    async def freeze_moderator(self, clone_id: int, user_id: str, minutes: int):
        """Suspend a moderator's staff access for `minutes` minutes without
        removing them from the moderators list."""
        await self.execute(
            "UPDATE clone_moderators SET frozen_until = NOW() + ($3 || ' minutes')::interval "
            "WHERE clone_id = $1 AND user_id = $2",
            clone_id, user_id, str(minutes),
        )

    async def unfreeze_moderator(self, clone_id: int, user_id: str):
        await self.execute(
            "UPDATE clone_moderators SET frozen_until = NULL "
            "WHERE clone_id = $1 AND user_id = $2",
            clone_id, user_id,
        )

    async def is_moderator_frozen(self, clone_id: int, user_id: str) -> bool:
        # Comparison done in SQL (frozen_until > NOW()) rather than pulling
        # the value into Python, so this can't drift out of sync with
        # whatever timezone the DB server's NOW() actually uses.
        return bool(
            await self.fetchval(
                "SELECT frozen_until > NOW() FROM clone_moderators "
                "WHERE clone_id = $1 AND user_id = $2",
                clone_id, user_id,
            )
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
            "SELECT id, bot_username, bot_name, is_active, banned FROM user_bots "
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

    async def list_all_clones(self) -> list:
        """EVERY clone, any owner, any status — for the master owner's
        STATS -> ALL CLONES listing. Deliberately does NOT select or
        decrypt bot_token/supabase_url/supabase_key: this is a display
        query for a Telegram chat, and those are credentials, not stats."""
        rows = await self.fetch(
            "SELECT id, user_id, bot_username, bot_name, is_active, banned "
            "FROM user_bots ORDER BY id"
        )
        return [dict(r) for r in rows]

    async def get_clone(self, clone_id: int) -> dict | None:
        row = await self.fetchrow(
            "SELECT id, user_id, bot_token, supabase_url, supabase_key, "
            "bot_username, bot_name, is_active, is_public, banned "
            "FROM user_bots WHERE id = $1",
            clone_id,
        )
        if not row:
            return None
        d = dict(row)
        d["bot_token"] = decrypt(d["bot_token"])
        d["supabase_url"] = decrypt(d["supabase_url"]) if d["supabase_url"] else None
        d["supabase_key"] = decrypt(d["supabase_key"]) if d["supabase_key"] else None
        return d

    async def get_clone_by_username(self, bot_username: str) -> dict | None:
        """Case-insensitive lookup, '@' prefix optional — for /cban /cunban
        where the master owner names a clone by its @username rather than
        its internal numeric clone_id."""
        row = await self.fetchrow(
            "SELECT id, user_id, bot_token, supabase_url, supabase_key, "
            "bot_username, bot_name, is_active, is_public, banned "
            "FROM user_bots WHERE lower(bot_username) = lower($1)",
            bot_username.lstrip("@"),
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

    async def set_clone_banned(self, clone_id: int, banned: bool):
        """MASTER OWNER ONLY — see the `banned` column comment in
        init_schema. Only flips the flag; bot.py's /cban is responsible
        for also stopping the runner task, and /cunban deliberately does
        NOT auto-restart the clone (leaves is_active as-is — the clone's
        own owner re-enables it via their dashboard once unbanned)."""
        await self.execute(
            "UPDATE user_bots SET banned = $1 WHERE id = $2",
            banned, clone_id,
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