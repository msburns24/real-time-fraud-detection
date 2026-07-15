"""
Transaction Simulator (PROVIDED, working).

Publishes synthetic credit-card transactions to Kafka. Two phases:

  --backfill-hours N   emit N hours of *historical* transactions first, so the
                       rolling windows are already populated when live traffic
                       starts (this is why 24h features aren't empty in a short run).
  --duration S         then stream live for S seconds at --rate TPS.

Message schema (JSON):
  transaction_id, customer_id, amount, merchant_category, is_online, timestamp, is_fraud
"""
from __future__ import annotations

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

MERCHANT_CATEGORIES = [
    "grocery", "gas_station", "restaurant", "online_retail",
    "department_store", "pharmacy", "entertainment", "travel",
]


class TransactionSimulator:
    def __init__(self, bootstrap_servers: str, topic: str, num_customers: int = 200, seed: int = 789):
        self.rng = random.Random(seed)
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            linger_ms=20,
            retries=5,
        )
        # each customer has a typical spend so "fraud" can look anomalous
        self.customers = {
            f"CUST{idx:04d}": {"avg": self.rng.uniform(20, 200)}
            for idx in range(num_customers)
        }

    def _make_txn(self, when: datetime, fraud_rate: float) -> dict:
        cust_id = self.rng.choice(list(self.customers))
        profile = self.customers[cust_id]
        is_fraud = 1 if self.rng.random() < fraud_rate else 0
        if is_fraud:
            amount = round(profile["avg"] * self.rng.uniform(6, 20), 2)   # unusually large
            is_online = True
        else:
            amount = round(max(1.0, self.rng.gauss(profile["avg"], profile["avg"] * 0.35)), 2)
            is_online = self.rng.random() < 0.4
        return {
            "transaction_id": str(uuid.uuid4()),
            "customer_id": cust_id,
            "amount": amount,
            "merchant_category": self.rng.choice(MERCHANT_CATEGORIES),
            "is_online": is_online,
            "timestamp": when.astimezone(timezone.utc).isoformat(),
            "is_fraud": is_fraud,
        }

    def _send(self, txn: dict) -> None:
        self.producer.send(self.topic, key=txn["customer_id"], value=txn)

    def backfill(self, hours: int, per_hour: int = 400, fraud_rate: float = 0.01) -> None:
        now = datetime.now(timezone.utc)
        total = hours * per_hour
        print(f"[simulator] backfilling {total} transactions over the last {hours}h...", flush=True)
        for _ in range(total):
            when = now - timedelta(seconds=self.rng.uniform(0, hours * 3600))
            self._send(self._make_txn(when, fraud_rate))
        self.producer.flush()
        print("[simulator] backfill complete.", flush=True)

    def simulate_stream(self, duration_seconds: int, rate_per_second: int, fraud_rate: float) -> None:
        print(f"[simulator] streaming {rate_per_second} TPS for {duration_seconds}s...", flush=True)
        interval = 1.0 / max(1, rate_per_second)
        end = time.time() + duration_seconds
        sent = 0
        while time.time() < end:
            self._send(self._make_txn(datetime.now(timezone.utc), fraud_rate))
            sent += 1
            if sent % rate_per_second == 0:
                self.producer.flush()
            time.sleep(interval)
        self.producer.flush()
        print(f"[simulator] streamed {sent} transactions.", flush=True)

    def close(self) -> None:
        self.producer.flush()
        self.producer.close()


def main() -> None:
    p = argparse.ArgumentParser(description="Kafka transaction simulator")
    p.add_argument("--backfill-hours", type=int, default=24)
    p.add_argument("--duration", type=int, default=300)
    p.add_argument("--rate", type=int, default=50)
    p.add_argument("--fraud-rate", type=float, default=0.01)
    args = p.parse_args()

    sim = TransactionSimulator(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        topic=os.getenv("KAFKA_TOPIC", "transactions"),
    )
    try:
        if args.backfill_hours > 0:
            sim.backfill(args.backfill_hours, fraud_rate=args.fraud_rate)
        if args.duration > 0:
            sim.simulate_stream(args.duration, args.rate, args.fraud_rate)
    finally:
        sim.close()


if __name__ == "__main__":
    main()
