"""process_A: stateless HTTP API. Never writes to a database; only publishes commands."""
import json, socket, uuid
import pika, os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from common import QUEUE, r, fetch_note, cache_note

INSTANCE = socket.gethostname()   # container id -> shows load balancing
app = FastAPI()

class NoteIn(BaseModel):
    text: str

def publish(msg: dict):
    # One connection per publish: simple, not fast. (Real systems keep a pool.)
    conn = pika.BlockingConnection(pika.URLParameters(os.environ["RABBIT_URL"]))
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE, durable=True)
    ch.basic_publish(
        exchange="", routing_key=QUEUE, body=json.dumps(msg),
        properties=pika.BasicProperties(delivery_mode=2),  # persist message to disk
    )
    conn.close()

@app.post("/notes", status_code=202)
def create_note(note: NoteIn):
    note_id = str(uuid.uuid4())             # ID created HERE so retries are idempotent
    publish({"id": note_id, "text": note.text})
    # 202 Accepted: "queued", NOT "stored". The write happens later.
    return {"id": note_id, "status": "queued", "served_by": INSTANCE}

@app.get("/notes/{note_id}")
def get_note(note_id: str):
    cached = r.get(f"note:{note_id}")                 # 1) cache hit/ fast path: Redis
    if cached:
        return {"note": json.loads(cached), "source": "redis", "served_by": INSTANCE}
    note = fetch_note(note_id)                        # 2) cache miss: ask the source of truth
    if not note:
        raise HTTPException(404, "not found (maybe still in the queue?)")
    return {"note": note, "source": "postgres", "served_by": INSTANCE}
