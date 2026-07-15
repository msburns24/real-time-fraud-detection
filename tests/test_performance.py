"""
Load-test harness (PROVIDED). Not a unit test — run it directly.

Fires N requests at /predict, records per-request latency, and reports
p50/p95/p99 + throughput. Writes results.json for your report.

  python tests/test_performance.py --n 1000 --url http://localhost:8000
  python tests/test_performance.py --n 1000 --url http://localhost:8080   # via nginx (blue-green)
"""
from __future__ import annotations

import argparse
import json
import os
import random
import time

import httpx

MERCHANTS = ["grocery", "gas_station", "online_retail", "travel", "restaurant"]


def _sample(rng: random.Random) -> dict:
    return {
        "transaction_id": f"perf-{rng.getrandbits(32):08x}",
        "customer_id": f"CUST{rng.randrange(200):04d}",
        "amount": round(rng.uniform(5, 3000), 2),
        "merchant_category": rng.choice(MERCHANTS),
        "is_online": rng.random() < 0.5,
        "timestamp": "2026-01-01T00:50:00Z",
    }


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = min(len(s) - 1, int(round((pct / 100.0) * (len(s) - 1))))
    return s[k]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=1000)
    ap.add_argument("--url", default=os.getenv("API_URL", "http://localhost:8000"))
    ap.add_argument("--seed", type=int, default=789)
    ap.add_argument("--out", default="results.json")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    latencies: list[float] = []
    errors = 0
    start = time.time()
    with httpx.Client(timeout=10.0) as client:
        for _ in range(args.n):
            t0 = time.perf_counter()
            try:
                r = client.post(f"{args.url}/predict", json=_sample(rng))
                ok = r.status_code == 200
            except Exception:
                ok = False
            latencies.append((time.perf_counter() - t0) * 1000.0)
            if not ok:
                errors += 1
    wall = time.time() - start

    report = {
        "requests": args.n,
        "errors": errors,
        "throughput_rps": round(args.n / wall, 1) if wall else 0.0,
        "latency_ms": {
            "p50": round(percentile(latencies, 50), 2),
            "p95": round(percentile(latencies, 95), 2),
            "p99": round(percentile(latencies, 99), 2),
            "max": round(max(latencies), 2) if latencies else 0.0,
        },
    }
    with open(args.out, "w") as fh:
        json.dump(report, fh, indent=2)
    print(json.dumps(report, indent=2))
    print(f"\nWrote {args.out}. (Note: {errors} non-200 responses — implement /predict if this is high.)")


if __name__ == "__main__":
    main()
