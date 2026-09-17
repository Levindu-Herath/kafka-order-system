"""
producer.py  (Step 7 - now also sends occasional BAD orders)
------------------------------------------------------------
Generates fake order messages, Avro-serializes them, and sends them to
'orders'. To let you demo the Dead Letter Queue, ~10% of orders are
deliberately INVALID (negative price) - the consumer will route those
straight to the DLQ.

Run while Kafka + Schema Registry are up. Stop with Ctrl+C.
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

BOOTSTRAP_SERVERS = "localhost:29092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "orders"

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]

# ~10% of orders will be deliberately invalid (negative price) so you can
# demonstrate the Dead Letter Queue. Set to 0 to send only valid orders.
BAD_ORDER_RATE = 0.1


def load_schema() -> str:
    schema_path = os.path.join("schemas", "order.avsc")
    with open(schema_path, "r") as f:
        return f.read()


def order_to_dict(order: dict, ctx) -> dict:
    return order


def delivery_report(err, msg):
    if err is not None:
        print(f"  Delivery failed: {err}")
    else:
        print(f"  Delivered to {msg.topic()} [partition {msg.partition()}] "
              f"offset {msg.offset()}")


def main():
    schema_str = load_schema()
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    string_serializer = StringSerializer("utf_8")
    avro_serializer = AvroSerializer(
        schema_registry_client, schema_str, order_to_dict
    )

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    print(f"Producing orders to topic '{TOPIC}'. Press Ctrl+C to stop.\n")

    order_id = 1001
    try:
        while True:
            # Most orders are valid; occasionally send a BAD one (negative price)
            if random.random() < BAD_ORDER_RATE:
                price = round(random.uniform(-100.0, -1.0), 2)   # invalid
                tag = "  (BAD - negative price, should go to DLQ)"
            else:
                price = round(random.uniform(5.0, 500.0), 2)     # valid
                tag = ""

            order = {
                "orderId": str(order_id),
                "product": random.choice(PRODUCTS),
                "price": price,
            }

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
                  f"{order['product']} @ ${order['price']}{tag}")

            order_id += 1
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()
        print("Producer closed.")


if __name__ == "__main__":
    main()