"""
consumer.py  (Step 4 - happy path)
----------------------------------
Reads order messages from the 'orders' topic, deserializes them from Avro
back into a dict, and prints them.

No running average / retry / DLQ yet - those come in the next steps.
Run this in a SECOND terminal while producer.py is running.
Stop with Ctrl+C.
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


def load_schema() -> str:
    """Read the Avro schema file into a string."""
    schema_path = os.path.join("schemas", "order.avsc")
    with open(schema_path, "r") as f:
        return f.read()


def dict_to_order(obj: dict, ctx) -> dict:
    """Tell the Avro deserializer how to turn the decoded record into our
    object. (We just want the dict as-is.)"""
    return obj


def main():
    schema_str = load_schema()

    # Client that talks to Schema Registry (to fetch the schema for decoding)
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    avro_deserializer = AvroDeserializer(
        schema_registry_client,
        schema_str,
        dict_to_order,
    )

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            # start from the beginning of the topic the first time this group runs
            "auto.offset.reset": "earliest",
        }
    )

    consumer.subscribe([TOPIC])

    print(f"Consuming from topic '{TOPIC}'. Press Ctrl+C to stop.\n")

    try:
        while True:
            # Wait up to 1 second for a message
            msg = consumer.poll(1.0)

            if msg is None:
                continue  # no message this time, loop again

            if msg.error():
                print(f"  ⚠️  Consumer error: {msg.error()}")
                continue

            # Decode the Avro bytes back into a dict
            order = avro_deserializer(
                msg.value(),
                SerializationContext(msg.topic(), MessageField.VALUE),
            )

            if order is None:
                continue

            print(f"Received order {order['orderId']}: "
                  f"{order['product']} @ ${order['price']}")

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()