import io
import json

import fastavro
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"
GROUP_ID = "order-processors"
SCHEMA_PATH = "schemas/order.avsc"

with open(SCHEMA_PATH) as f:
    schema = fastavro.parse_schema(json.load(f))


def deserialize(raw: bytes) -> dict:
    buf = io.BytesIO(raw)
    return fastavro.schemaless_reader(buf, schema)


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=deserialize,
    )

    print(f"Consuming orders from topic '{TOPIC}'. Press Ctrl+C to stop.")
    try:
        for message in consumer:
            order = message.value
            total = order["quantity"] * order["unit_price"]
            print(
                f"[partition={message.partition} offset={message.offset}] "
                f"order={order['order_id']} status={order['status']} total=${total:.2f}"
            )
    except KeyboardInterrupt:
        print("Stopping consumer.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
