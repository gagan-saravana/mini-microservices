"""process_B: consumes commands, writes Postgres, THEN updates Redis (the consistency glue)."""
import json, os, random, time
import pika
from common import QUEUE, db, fetch_note, cache_note

DELAY = float(os.environ.get("WORKER_DELAY", "0"))
FAIL_AFTER_DB = float(os.environ.get("FAIL_AFTER_DB", "0"))

def handle(ch, method, properties, body):
    msg = json.loads(body)
    try:
        time.sleep(DELAY)  # pretend this is slow work

        # Step 1: source of truth. Idempotent: redelivery can't create duplicates.
        with db() as conn, conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO notes (id, text) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING",
                    (msg["id"], msg["text"]),
                )

        # Simulated crash in the dangerous gap: DB has the data, cache doesn't (yet).
        if random.random() < FAIL_AFTER_DB:
            raise RuntimeError("simulated crash between DB write and cache write")

        # Step 2: derived read model, built from what's actually in the DB.
        cache_note(fetch_note(msg["id"]))

        ch.basic_ack(method.delivery_tag)   # Step 3: only now tell RabbitMQ we're done
        print(f"processed {msg['id']}", flush=True)
    except Exception as e:
        print(f"FAILED {msg['id']}: {e} -> requeue", flush=True)
        time.sleep(1)
        ch.basic_nack(method.delivery_tag, requeue=True)  # redelivered, step 1 is a no-op, step 2 heals

def main():
    while True:  # wait for RabbitMQ
        try:
            conn = pika.BlockingConnection(pika.URLParameters(os.environ["RABBIT_URL"]))
            break
        except pika.exceptions.AMQPConnectionError:
            time.sleep(2)
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE, durable=True)
    ch.basic_qos(prefetch_count=1)          # one unacked message at a time
    ch.basic_consume(queue=QUEUE, on_message_callback=handle)  # manual ack (auto_ack=False)
    print("worker ready", flush=True)
    ch.start_consuming()

if __name__ == "__main__":
    main()
