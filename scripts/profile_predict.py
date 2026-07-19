"""Micro-profile the /predict hot path to locate the dominant latency term.

Times each stage in-process (no HTTP), so the sum can be compared against the
end-to-end p50 from tests/test_performance.py — the gap is uvicorn/HTTP overhead.

Run from the repo root as a module (a bare file path breaks the `src` import):

    python -m scripts.profile_predict -n 2000 2>/dev/null
"""

from __future__ import annotations

import time

import typer

from src._logging import get_logger
from src.api.fraud_detector import FraudDetector
from src.api.main import Transaction, merge_features
from src.streaming.feature_store import FeatureStore

logger = get_logger("profile")
app = typer.Typer(add_completion=False)

SAMPLE = {
    "transaction_id": "prof-0001",
    "customer_id": "CUST0001",
    "amount": 250.0,
    "merchant_category": "online_retail",
    "is_online": True,
    "timestamp": "2026-01-01T00:50:00Z",
}


def _p(samples: list[float], pct: float) -> float:
    s = sorted(samples)
    k = min(len(s) - 1, int(round((pct / 100.0) * (len(s) - 1))))
    return round(s[k] * 1000, 4)


@app.command()
def main(num_tests: int = typer.Option(2000, "-n", "--num-tests")) -> None:
    detector = FraudDetector()
    store = FeatureStore()
    stages: dict[str, list[float]] = {
        "validate": [],
        "redis_get": [],
        "merge": [],
        "predict": [],
        "log": [],
    }

    for _ in range(num_tests):
        t = time.perf_counter()
        txn = Transaction(**SAMPLE).model_dump()
        stages["validate"].append(time.perf_counter() - t)

        t = time.perf_counter()
        stored = store.get_customer_features(SAMPLE["customer_id"])
        stages["redis_get"].append(time.perf_counter() - t)

        t = time.perf_counter()
        merged = merge_features(txn, stored)
        stages["merge"].append(time.perf_counter() - t)

        t = time.perf_counter()
        prediction = detector.predict(merged)
        stages["predict"].append(time.perf_counter() - t)

        t = time.perf_counter()
        logger.bind(customer_id=SAMPLE["customer_id"]).info("prediction served")
        stages["log"].append(time.perf_counter() - t)

    typer.echo(f"\nn={num_tests}  cache_hit={stored is not None}  {prediction}\n")
    typer.echo(f"{'stage':<12}{'p50 ms':>10}{'p95 ms':>10}")
    for name, samples in stages.items():
        typer.echo(f"{name:<12}{_p(samples, 50):>10}{_p(samples, 95):>10}")
    total = sum(_p(s, 50) for s in stages.values())
    typer.echo(f"{'TOTAL':<12}{round(total, 4):>10}")


if __name__ == "__main__":
    app()
