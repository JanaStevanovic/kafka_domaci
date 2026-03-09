import json
import os
import random
import uuid
from datetime import datetime, timezone
from time import sleep

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MAIN_TOPIC = os.getenv("KAFKA_MAIN_TOPIC", "startup-events")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

EVENT_TYPES = [
    "idea_submitted",
    "idea_validation_requested",
    "idea_validation_completed",
    "idea_validation_failed",
    "feedback_viewed"
]

STARTUP_NAMES = ["AgroVision", "HealthSync", "EcoRoute", "EduSpark", "LegalFlow"]
STAGES = ["idea", "mvp", "pre-seed", "seed"]
MARKETS = ["Poljoprivreda", "Zdravstvo", "Edukacija", "Logistika", "Pravo"]


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def generate_valid_event():
    idea_id = f"idea_{random.randint(1000, 9999)}"
    event_type = random.choice(EVENT_TYPES)

    payload = {
        "user_id": f"user_{random.randint(1, 20)}",
        "startup_name": random.choice(STARTUP_NAMES),
        "problem": "Korisnici nemaju efikasan način za validaciju poslovne ideje",
        "solution": "AI platforma za analizu i validaciju startup ideja",
        "target_market": random.choice(MARKETS),
        "startup_stage": random.choice(STAGES)
    }

    if event_type == "idea_validation_completed":
        payload = {
            "score": round(random.uniform(1, 10), 2),
            "market_potential": random.choice(["low", "medium", "high"]),
            "risk_level": random.choice(["low", "medium", "high"]),
            "recommendation": "Potrebna je preciznija segmentacija korisnika."
        }

    elif event_type == "idea_validation_failed":
        payload = {
            "error_code": random.choice(["INVALID_INPUT", "TIMEOUT", "AI_SERVICE_ERROR"]),
            "error_message": "Došlo je do greške tokom validacije ideje."
        }

    elif event_type == "feedback_viewed":
        payload = {
            "user_id": f"user_{random.randint(1, 20)}",
            "viewed_at": now_iso()
        }

    elif event_type == "idea_validation_requested":
        payload = {
            "requested_by": f"user_{random.randint(1, 20)}",
            "validation_type": "idea_validation"
        }

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": now_iso(),
        "business_id": idea_id,
        "payload": payload
    }

    return idea_id, event


def generate_invalid_event():
    idea_id = f"idea_{random.randint(1000, 9999)}"

    invalid_event = {
        "event_type": "idea_submitted",
        "timestamp": now_iso(),
        "payload": {
            "startup_name": "BrokenStartup"
        }
    }

    return idea_id, invalid_event


def on_send_success(record_metadata):
    print(
        f"[OK] topic={record_metadata.topic} "
        f"partition={record_metadata.partition} "
        f"offset={record_metadata.offset}"
    )


def on_send_error(excp):
    print(f"[ERROR] Slanje poruke nije uspelo: {excp}")


if __name__ == "__main__":
    print("Producer je pokrenut...")

    for i in range(20):
        if random.random() < 0.2:
            key, event = generate_invalid_event()
            print(f"[SEND INVALID] key={key} event={event}")
        else:
            key, event = generate_valid_event()
            print(f"[SEND VALID] key={key} event_type={event['event_type']}")

        future = producer.send(MAIN_TOPIC, key=key, value=event)
        future.add_callback(on_send_success)
        future.add_errback(on_send_error)

        sleep(1)

    producer.flush()
    producer.close()
    print("Producer je završio slanje poruka.")