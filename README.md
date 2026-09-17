# Kafka Order System

A minimal Kafka producer/consumer demo simulating an e-commerce order stream, using Avro-encoded messages.

## Structure

- `docker-compose.yml` — Zookeeper, Kafka broker, and Kafka UI (http://localhost:8080)
- `schemas/order.avsc` — Avro schema for order events
- `producer.py` — generates fake orders and publishes them to the `orders` topic
- `consumer.py` — consumes and prints orders from the `orders` topic

## Setup

```bash
docker compose up -d
python -m venv venv
venv\Scripts\activate      # Windows
pip install -r requirements.txt
```

## Run

In separate terminals:

```bash
python producer.py
python consumer.py
```

Kafka UI is available at http://localhost:8080 to inspect topics and messages.

## Teardown

```bash
docker compose down
```
