"""
Kafka Producer: Simulates real-time order events for the Olist e-commerce platform.

Reads the last 10% of orders (chronologically) from the FastAPI, then publishes
them to Kafka topics with realistic time gaps between events.
"""

import json
import os
import sys
import time
from datetime import datetime

import requests
from confluent_kafka import Producer

API_BASE = os.environ.get("API_BASE", "http://api:8000/api/v1")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC_ORDERS = "ecommerce.orders.live"
TOPIC_LOGISTICS = "ecommerce.logistics.updates"
TIME_SCALE = float(os.environ.get("TIME_SCALE", "86400"))  # 1 real day = 1 simulated second


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}", file=sys.stderr)
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def fetch_all_orders():
    """Fetch all orders from the API, sorted by purchase timestamp."""
    all_orders = []
    page = 1
    while True:
        resp = requests.get(f"{API_BASE}/olist_orders_dataset", params={"page": page, "size": 5000})
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("data", [])
        if not rows:
            break
        all_orders.extend(rows)
        if len(all_orders) >= data["total_rows"]:
            break
        page += 1

    all_orders.sort(key=lambda x: x.get("order_purchase_timestamp", ""))
    return all_orders


def main():
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "olist-order-producer",
    })

    print("Fetching orders from API...")
    all_orders = fetch_all_orders()
    total = len(all_orders)
    print(f"Total orders: {total}")

    # Take the last 10% (chronologically)
    split_idx = int(total * 0.9)
    stream_orders = all_orders[split_idx:]
    print(f"Streaming last {len(stream_orders)} orders (10%) to Kafka...")

    prev_ts = None

    for i, order in enumerate(stream_orders):
        purchase_ts = order.get("order_purchase_timestamp")

        # Simulate realistic time gaps between orders
        if prev_ts and purchase_ts:
            try:
                t1 = datetime.fromisoformat(prev_ts.replace("Z", "+00:00"))
                t2 = datetime.fromisoformat(purchase_ts.replace("Z", "+00:00"))
                gap_seconds = (t2 - t1).total_seconds()
                sleep_time = max(0.01, gap_seconds / TIME_SCALE)
                time.sleep(sleep_time)
            except (ValueError, TypeError):
                pass

        prev_ts = purchase_ts

        # Publish order event
        customer_id = order.get("customer_id", "unknown")
        order_event = {
            "order_id": order["order_id"],
            "customer_id": customer_id,
            "order_status": order.get("order_status"),
            "order_purchase_timestamp": purchase_ts,
            "order_approved_at": order.get("order_approved_at"),
            "order_delivered_carrier_date": order.get("order_delivered_carrier_date"),
            "order_delivered_customer_date": order.get("order_delivered_customer_date"),
            "order_estimated_delivery_date": order.get("order_estimated_delivery_date"),
        }

        producer.produce(
            topic=TOPIC_ORDERS,
            key=customer_id.encode("utf-8"),
            value=json.dumps(order_event).encode("utf-8"),
            callback=delivery_report,
        )

        # Publish logistics update for delivered orders
        if order.get("order_delivered_customer_date"):
            logistics_event = {
                "order_id": order["order_id"],
                "customer_id": customer_id,
                "delivered_carrier_date": order.get("order_delivered_carrier_date"),
                "delivered_customer_date": order.get("order_delivered_customer_date"),
                "estimated_delivery_date": order.get("order_estimated_delivery_date"),
            }
            producer.produce(
                topic=TOPIC_LOGISTICS,
                key=customer_id.encode("utf-8"),
                value=json.dumps(logistics_event).encode("utf-8"),
                callback=delivery_report,
            )

        # Flush periodically to avoid buffer overflow
        if (i + 1) % 100 == 0:
            producer.flush()
            print(f"  Progress: {i + 1}/{len(stream_orders)} orders streamed")

    producer.flush()
    print(f"Done. Streamed {len(stream_orders)} orders to Kafka.")


if __name__ == "__main__":
    main()
