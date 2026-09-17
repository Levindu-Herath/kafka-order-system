"""
consumer.py  (Step 5 - running average)
---------------------------------------
Reads orders from 'orders', deserializes from Avro, and maintains a
real-time running average of prices (overall, and per product).

New in this step: the aggregation state (total_sum, count, per-product stats)
and the update_average() function.

Still to come: retry logic (Step 6) and Dead Letter Queue (Step 7).
Run in a second terminal while producer.py runs. Stop with Ctrl+C.
"""

import os

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

# --- Connection settings (match your docker-compose ports) ---
BOOTSTRAP_SERVERS = "localhost:29092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "orders"
GROUP_ID = "order-consumer-group"

# --- Aggregation state (lives in memory for the life of the consumer) ---
total_sum = 0.0          # sum of all prices seen
count = 0                # how many orders processed
product_stats = {}       # product -> {"sum": float, "count": int}


def load_schema() -> str:
    schema_path = os.path.join("schemas", "order.avsc")
    with open(schema_path, "r") as f:
        return f.read()


def dict_to_order(obj: dict, ctx) -> dict:
    return obj


def update_average(order: dict):
    """Update the overall and per-product running averages, then print them."""
    global total_sum, count

    price = order["price"]
    product = order["product"]

    # --- overall running average ---
    total_sum += price
    count += 1
    overall_avg = total_sum / count

    # --- per-product running average ---
    stats = product_stats.setdefault(product, {"sum": 0.0, "count": 0})
    stats["sum"] += price
    stats["count"] += 1
    product_avg = stats["sum"] / stats["count"]

    print(f"Order {order['orderId']}: {product} @ ${price:.2f}")
    print(f"   Running avg (overall): ${overall_avg:.2f}  "
          f"over {count} orders")
    print(f"   Running avg ({product}): ${product_avg:.2f}  "
          f"over {stats['count']} orders")


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

            update_average(order)

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()