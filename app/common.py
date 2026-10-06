import json, os
from contextlib import closing
import psycopg2, redis

QUEUE = "note_commands"
CACHE_TTL = 60  # safety net: even a stale/wrong cache entry dies after 60s

r = redis.Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)

def db():
    return closing(psycopg2.connect(os.environ["DATABASE_URL"]))

def fetch_note(note_id):
    """Read from the source of truth."""
    with db() as conn, conn.cursor() as cur:
        cur.execute("SELECT id, text, created_at FROM notes WHERE id = %s", (note_id,))
        row = cur.fetchone()
    if not row:
        return None
    return {"id": row[0], "text": row[1], "created_at": row[2].isoformat()}

def cache_note(note):
    r.set(f"note:{note['id']}", json.dumps(note), ex=CACHE_TTL)
