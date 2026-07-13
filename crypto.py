"""
Symmetric encryption for anything we store on behalf of a clone owner:
bot_token, supabase_url, supabase_key. These are effectively master keys —
if the user_bots table leaks in plaintext, every clone bot AND every
clone owner's downstream Supabase project is compromised.

Requires ENCRYPTION_KEY in the environment: a urlsafe base64-encoded
32-byte key. Generate one once with:

    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

Store that value in your secrets manager / Render env vars — never in
git, never in the database itself. Rotating it invalidates every stored
credential, so back it up somewhere durable before you need it.
"""

import os
from cryptography.fernet import Fernet, InvalidToken

_key = os.environ.get("ENCRYPTION_KEY")
if not _key:
    raise RuntimeError(
        "ENCRYPTION_KEY is not set. Generate one with "
        "`python -c \"from cryptography.fernet import Fernet; "
        "print(Fernet.generate_key().decode())\"` and set it as an env var. "
        "Do not store secrets un-encrypted."
    )

_fernet = Fernet(_key.encode())


def encrypt(plaintext: str) -> str:
    if plaintext is None:
        return None
    return _fernet.encrypt(plaintext.encode()).decode()


def decrypt(ciphertext: str) -> str:
    if ciphertext is None:
        return None
    try:
        return _fernet.decrypt(ciphertext.encode()).decode()
    except InvalidToken:
        # Wrong key, corrupted value, or plaintext that was never encrypted
        # (e.g. a row inserted before this module existed). Fail loud —
        # silently returning garbage to asyncpg.create_pool() produces a
        # confusing connection error far from the real cause.
        raise ValueError(
            "Could not decrypt stored credential — wrong ENCRYPTION_KEY, "
            "corrupted value, or the value was never encrypted."
        )
