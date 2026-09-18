"""
Full consumer:
  - deserializes Avro orders
  - maintains a real-time running average (overall + per product)
  - retries TRANSIENT failures (simulated) with exponential backoff
  - routes PERMANENT failures to a Dead Letter Queue ('orders-dlq'):
        * invalid data (price <= 0)  -> straight to DLQ, no retries
        * retries exhausted          -> to DLQ
  - NEVER crashes: a bad message is quarantined and the consumer keeps going.

"""

import os
import json
import time
import random

from confluent_kafka import Consumer, Producer
from confluent_kafka.serialization import (
    StringSerializer,
    SerializationContext,
    MessageField,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

# --- Connection settings ---
BOOTSTRAP_SERVERS = "localhost:29092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"
GROUP_ID = "order-consumer-group"

# --- Retry settings ---
MAX_RETRIES = 3
FAILURE_RATE = 0.2       # simulate a transient failure 20% of the time

# --- Aggregation state ---
total_sum = 0.0
count = 0
product_stats = {}

# DLQ producer 
dlq_producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
string_serializer = StringSerializer("utf_8")


class TransientError(Exception):
    """A temporary, retryable failure."""
    pass


class PermanentError(Exception):
    """A permanent failure - do NOT retry, send to DLQ."""
    pass


def load_schema() -> str:
    schema_path = os.path.join("schemas", "order.avsc")
    with open(schema_path, "r") as f:
        return f.read()


def dict_to_order(obj: dict, ctx) -> dict:
    return obj


def send_to_dlq(order, reason, attempts):
    """Publish a failed message to the Dead Letter Queue with failure metadata."""
    dlq_record = {
        "failedOrder": order,
        "reason": reason,
        "attempts": attempts,
        "failedAt": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    key = order.get("orderId", "unknown") if isinstance(order, dict) else "unknown"
    dlq_producer.produce(
        topic=DLQ_TOPIC,
        key=string_serializer(str(key)),
        value=json.dumps(dlq_record).encode("utf-8"),
    )
    dlq_producer.poll(0)
    print(f"   ☠ order {key} sent to DLQ ('{DLQ_TOPIC}') - reason: {reason}")


def update_average(order: dict):
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


def validate(order: dict):
    """Reject permanently-broken messages. Raises PermanentError if invalid."""
    if order.get("price", 0) <= 0:
        raise PermanentError("invalid price (<= 0)")


def process_order(order: dict):
    """May raise TransientError (simulated) to demonstrate retries."""
    if random.random() < FAILURE_RATE:
        raise TransientError("simulated temporary failure")
    update_average(order)


def handle_message(order: dict):
    """Validate, then process with retries. Route permanent failures and
    exhausted retries to the DLQ. Never raises - the consumer keeps running."""
    # 1) Permanent (bad data) -> straight to DLQ, no retries
    try:
        validate(order)
    except PermanentError as e:
        send_to_dlq(order, str(e), attempts=0)
        return

    # 2) Transient -> retry with backoff
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            process_order(order)
            if attempt > 1:
                print(f"   ✔ recovered on attempt {attempt}")
            return
        except TransientError as e:
            wait = 0.5 * (2 ** (attempt - 1)) 
            if attempt < MAX_RETRIES:
                print(f"   ⟳ order {order['orderId']} attempt {attempt} "
                      f"failed ({e}); retrying in {wait:.1f}s...")
                time.sleep(wait)
            else:
                # 3) Retries exhausted -> DLQ
                send_to_dlq(order, "retries exhausted", attempts=MAX_RETRIES)


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

    print(f"Consuming from '{TOPIC}'. Bad messages go to '{DLQ_TOPIC}'.")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"  WARNING  Consumer error: {msg.error()}")
                continue

            # Deserialization itself can fail permanently -> DLQ the raw bytes
            try:
                order = avro_deserializer(
                    msg.value(),
                    SerializationContext(msg.topic(), MessageField.VALUE),
                )
            except Exception as e:
                send_to_dlq({"raw": str(msg.value())},
                            f"deserialization failed: {e}", attempts=0)
                continue

            if order is None:
                continue

            handle_message(order)

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        dlq_producer.flush()
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()