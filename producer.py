"""
producer.py
-----------
Generates fake order messages, serializes them with Avro (registering the
schema in Schema Registry automatically), and sends them to the 'orders' topic.

Run it while Kafka + Schema Registry are up (docker compose up -d).
Stop it any time with Ctrl+C.
"""

import os
import time
import random

from confluent_kafka import Producer
from confluent_kafka.serialization import (
    StringSerializer,
    SerializationContext,
    MessageField,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# --- Connection settings (match your docker-compose ports) ---
BOOTSTRAP_SERVERS = "localhost:29092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "orders"

# Sample products to pick from
PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]


def load_schema() -> str:
    """Read the Avro schema file into a string."""
    schema_path = os.path.join("schemas", "order.avsc")
    with open(schema_path, "r") as f:
        return f.read()


def order_to_dict(order: dict, ctx) -> dict:
    """Tell the Avro serializer how to turn our order into a plain dict.
    (Our order is already a dict, so we just return it.)"""
    return order


def delivery_report(err, msg):
    """Called once Kafka confirms (or fails) delivery of each message."""
    if err is not None:
        print(f"  ❌ Delivery failed: {err}")
    else:
        print(f"  ✅ Delivered to {msg.topic()} [partition {msg.partition()}] "
              f"offset {msg.offset()}")


def main():
    schema_str = load_schema()

    # Client that talks to Schema Registry
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    # Serializers: key as a plain string, value as Avro
    string_serializer = StringSerializer("utf_8")
    avro_serializer = AvroSerializer(
        schema_registry_client,
        schema_str,
        order_to_dict,
    )

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    print(f"Producing orders to topic '{TOPIC}'. Press Ctrl+C to stop.\n")

    order_id = 1001
    try:
        while True:
            order = {
                "orderId": str(order_id),
                "product": random.choice(PRODUCTS),
                "price": round(random.uniform(5.0, 500.0), 2),
            }

            # Serve any queued delivery callbacks
            producer.poll(0)

            producer.produce(
                topic=TOPIC,
                key=string_serializer(order["orderId"]),
                value=avro_serializer(
                    order, SerializationContext(TOPIC, MessageField.VALUE)
                ),
                on_delivery=delivery_report,
            )

            print(f"Sent order {order['orderId']}: "
                  f"{order['product']} @ ${order['price']}")

            order_id += 1
            time.sleep(1)  # one order per second

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        # Wait for any outstanding messages to be delivered
        producer.flush()
        print("Producer closed.")


if __name__ == "__main__":
    main()