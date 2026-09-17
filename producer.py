import io
import json
import time
import uuid
from datetime import datetime, timezone

import fastavro
from faker import Faker
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"
SCHEMA_PATH = "schemas/order.avsc"

fake = Faker()

with open(SCHEMA_PATH) as f:
    schema = fastavro.parse_schema(json.load(f))


def build_order() -> dict:
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": str(uuid.uuid4()),
        "product_id": fake.ean13(),
        "quantity": fake.random_int(min=1, max=10),
        "unit_price": round(fake.pyfloat(min_value=1, max_value=500, right_digits=2), 2),
        "status": "PLACED",
        "created_at": int(datetime.now(timezone.utc).timestamp() * 1000),
    }


def serialize(order: dict) -> bytes:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, order)
    return buf.getvalue()


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=serialize,
        key_serializer=lambda k: k.encode("utf-8"),
    )

    print(f"Producing orders to topic '{TOPIC}'. Press Ctrl+C to stop.")
    try:
        while True:
            order = build_order()
            producer.send(TOPIC, key=order["order_id"], value=order)
            print(f"Sent order {order['order_id']} ({order['product_id']}, qty={order['quantity']})")
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping producer.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
