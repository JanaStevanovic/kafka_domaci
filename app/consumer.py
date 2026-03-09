import json
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MAIN_TOPIC = os.getenv("KAFKA_MAIN_TOPIC", "startup-events")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "startup-events-dlq")

consumer = KafkaConsumer(
    MAIN_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id="startup-validation-group",
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


print("Consumer pokrenut...")

for message in consumer:

    event = message.value

    print(
        f"[RECEIVED] partition={message.partition} "
        f"offset={message.offset}"
    )

    try:

        if not validate_event(event):
            raise ValueError("Invalid event schema")

        print(
            f"[VALID] event_type={event['event_type']} "
            f"business_id={event['business_id']}"
        )

    except Exception as e:

        print(f"[INVALID] sending to DLQ: {e}")

        producer.send(DLQ_TOPIC, value=event)