import json
import os
import sqlite3
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MAIN_TOPIC = os.getenv("KAFKA_MAIN_TOPIC", "startup-events")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "startup-events-dlq")
DB_PATH = "startup_events.db"

consumer = KafkaConsumer(
    MAIN_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id="startup-db-consumer-group",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def validate_event(event):
    required_fields = ["event_id", "event_type", "timestamp", "business_id", "payload"]
    for field in required_fields:
        if field not in event:
            return False
    return True


def save_event_to_db(event):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO events (event_id, event_type, business_id, timestamp, payload_json)
            VALUES (?, ?, ?, ?, ?)
        """, (
            event["event_id"],
            event["event_type"],
            event["business_id"],
            event["timestamp"],
            json.dumps(event["payload"], ensure_ascii=False)
        ))

        cursor.execute("""
            INSERT OR IGNORE INTO idea_stats (
                business_id,
                submitted_count,
                validation_requested_count,
                validation_completed_count,
                validation_failed_count,
                feedback_viewed_count,
                last_event_timestamp
            )
            VALUES (?, 0, 0, 0, 0, 0, ?)
        """, (
            event["business_id"],
            event["timestamp"]
        ))

        event_type = event["event_type"]

        if event_type == "idea_submitted":
            cursor.execute("""
                UPDATE idea_stats
                SET submitted_count = submitted_count + 1,
                    last_event_timestamp = ?
                WHERE business_id = ?
            """, (event["timestamp"], event["business_id"]))

        elif event_type == "idea_validation_requested":
            cursor.execute("""
                UPDATE idea_stats
                SET validation_requested_count = validation_requested_count + 1,
                    last_event_timestamp = ?
                WHERE business_id = ?
            """, (event["timestamp"], event["business_id"]))

        elif event_type == "idea_validation_completed":
            cursor.execute("""
                UPDATE idea_stats
                SET validation_completed_count = validation_completed_count + 1,
                    last_event_timestamp = ?
                WHERE business_id = ?
            """, (event["timestamp"], event["business_id"]))

        elif event_type == "idea_validation_failed":
            cursor.execute("""
                UPDATE idea_stats
                SET validation_failed_count = validation_failed_count + 1,
                    last_event_timestamp = ?
                WHERE business_id = ?
            """, (event["timestamp"], event["business_id"]))

        elif event_type == "feedback_viewed":
            cursor.execute("""
                UPDATE idea_stats
                SET feedback_viewed_count = feedback_viewed_count + 1,
                    last_event_timestamp = ?
                WHERE business_id = ?
            """, (event["timestamp"], event["business_id"]))

        conn.commit()
        print(f"[DB] Sačuvan event_id={event['event_id']}")

    except sqlite3.IntegrityError:
        print(f"[SKIP] Event već postoji: {event['event_id']}")

    finally:
        conn.close()


print("DB Consumer pokrenut...")

for message in consumer:
    event = message.value

    print(f"[RECEIVED] partition={message.partition} offset={message.offset}")

    try:
        if not validate_event(event):
            raise ValueError("Invalid event schema")

        print(f"[VALID] event_type={event['event_type']} business_id={event['business_id']}")
        save_event_to_db(event)

    except Exception as e:
        print(f"[INVALID] slanje u DLQ: {e}")
        producer.send(DLQ_TOPIC, value=event)