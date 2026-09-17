"""
consumer.py  (Step 6 - retry logic)
-----------------------------------
Reads orders, keeps a running average, and now handles TEMPORARY failures
by retrying up to MAX_RETRIES times with exponential backoff.

To demonstrate retries, we simulate a transient failure on ~20% of messages
(FAILURE_RATE). Most recover within a retry or two.

The simulated failure happens BEFORE aggregation, so a message that
fails-then-succeeds is only counted once in the average.

Still to come: Dead Letter Queue (Step 7) for messages that fail permanently
or exhaust all retries.

Run in a second terminal while producer.py runs. Stop with Ctrl+C.
"""

import os
import time
import random

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

# --- Connection settings (match your docker-compose ports) ---
BOOTSTRAP_SERVERS = "localhost:29092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "orders"
GROUP_ID = "order-consumer-group"

# --- Retry settings ---
MAX_RETRIES = 3          # how many times to try processing a message
FAILURE_RATE = 0.2       # simulate a transient failure 20% of the time

# --- Aggregation state ---
total_sum = 0.0
count = 0
product_stats = {}


class TransientError(Exception):
    """A temporary, retryable failure (e.g. a brief network/db hiccup)."""
    pass


def load_schema() -> str:
    schema_path = os.path.join("schemas", "order.avsc")
    with open(schema_path, "r") as f:
        return f.read()


def dict_to_order(obj: dict, ctx) -> dict:
    return obj


def update_average(order: dict):
    """Update overall and per-product running averages, then print them."""
    global total_sum, count

    price = order["price"]
    product = order["product"]

    total_sum += price
    count += 1
    overall_avg = total_sum / count

    stats = product_stats.setdefault(product, {"sum": 0.0, "count": 0})
    stats["sum"] += price
    stats["count"] += 1
    product_avg = stats["sum"] / stats["count"]

    print(f"Order {order['orderId']}: {product} @ ${price:.2f}")
    print(f"   Running avg (overall): ${overall_avg:.2f}  over {count} orders")
    print(f"   Running avg ({product}): ${product_avg:.2f}  "
          f"over {stats['count']} orders")


def process_order(order: dict):
    """Process a single order. May raise TransientError (simulated) to
    demonstrate retry behaviour. Aggregation only runs if we get past the
    simulated failure."""
    if random.random() < FAILURE_RATE:
        raise TransientError("simulated temporary failure")

    update_average(order)


def process_with_retry(order: dict):
    """Try to process an order, retrying transient failures with backoff.
    Returns True on success, False if all retries were exhausted."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            process_order(order)
            if attempt > 1:
                print(f"   ✔ recovered on attempt {attempt}")
            return True
        except TransientError as e:
            wait = 0.5 * (2 ** (attempt - 1))  # 0.5s, 1s, 2s
            if attempt < MAX_RETRIES:
                print(f"   ⟳ order {order['orderId']} attempt {attempt} "
                      f"failed ({e}); retrying in {wait:.1f}s...")
                time.sleep(wait)
            else:
                print(f"   ✖ order {order['orderId']} failed after "
                      f"{MAX_RETRIES} attempts - giving up (DLQ comes next)")
    return False


def main():
    schema_str = load_schema()
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(
        schema_registry_client, schema_str, dict_to_order
    )

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])

    print(f"Consuming from topic '{TOPIC}'. Press Ctrl+C to stop.\n")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"  WARNING  Consumer error: {msg.error()}")
                continue

            order = avro_deserializer(
                msg.value(),
                SerializationContext(msg.topic(), MessageField.VALUE),
            )
            if order is None:
                continue

            process_with_retry(order)

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()