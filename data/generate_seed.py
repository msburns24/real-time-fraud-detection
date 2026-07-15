"""
Generate a deterministic, labeled seed dataset (PROVIDED).

Writes data/seed_transactions.csv with 24-48h of history so windowed features
are populated and so train.py has something to learn from. Fixed RNG seed →
reproducible for grading.

Run:  python data/generate_seed.py
"""
from __future__ import annotations

import csv
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

SEED = 789
NUM_CUSTOMERS = 200
NUM_ROWS = 6000
FRAUD_RATE = 0.03
BASE_TIME = datetime(2026, 1, 2, 0, 0, 0, tzinfo=timezone.utc)  # fixed anchor
MERCHANTS = ["grocery", "gas_station", "restaurant", "online_retail",
             "department_store", "pharmacy", "entertainment", "travel"]


def main() -> None:
    rng = random.Random(SEED)
    customers = {f"CUST{idx:04d}": rng.uniform(20, 200) for idx in range(NUM_CUSTOMERS)}
    out_path = os.path.join(os.path.dirname(__file__), "seed_transactions.csv")
    with open(out_path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["transaction_id", "customer_id", "amount",
                    "merchant_category", "is_online", "timestamp", "is_fraud"])
        for _ in range(NUM_ROWS):
            cust = rng.choice(list(customers))
            avg = customers[cust]
            is_fraud = 1 if rng.random() < FRAUD_RATE else 0
            if is_fraud:
                amount = round(avg * rng.uniform(6, 20), 2)
                is_online = True
            else:
                amount = round(max(1.0, rng.gauss(avg, avg * 0.35)), 2)
                is_online = rng.random() < 0.4
            when = BASE_TIME - timedelta(seconds=rng.uniform(0, 48 * 3600))
            w.writerow([str(uuid.UUID(int=rng.getrandbits(128))), cust, amount,
                        rng.choice(MERCHANTS), int(is_online),
                        when.isoformat(), is_fraud])
    print(f"wrote {NUM_ROWS} rows -> {out_path}")


if __name__ == "__main__":
    main()
