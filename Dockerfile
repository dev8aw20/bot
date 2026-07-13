# Single process: the master bot (webhook) + every active clone bot
# (long-polling, in-process asyncio tasks via clone_runner.py). See
# clone_runner.py's docstring for why that's one container, not one
# container per clone.
FROM python:3.12-slim

WORKDIR /app

# asyncpg/cryptography ship manylinux wheels for this base image, so no
# build-essential/gcc needed. If a future dependency needs compiling,
# add: RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Render (or wherever this runs) injects its own PORT at runtime; this
# default only matters for local/manual `docker run`.
ENV PORT=7860
EXPOSE 7860

# Required at runtime (fail fast if missing, don't bake real values in):
#   BOT_TOKEN, DATABASE_URL, ENCRYPTION_KEY, WEBHOOK_URL, WEBHOOK_SECRET
# Set LOCAL_TEST=1 to run polling instead of webhook (see bot.py's main()).
CMD ["python", "bot.py"]
