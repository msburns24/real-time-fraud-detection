"""
Streaming Feature Processor — YOU IMPLEMENT `features()`.

It consumes transactions from Kafka, keeps a rolling per-customer window, and
writes computed features to the Redis feature store. The Kafka wiring and the
per-customer bookkeeping are provided; the WINDOWING + AGGREGATION is your job
(graded: "Windowed aggregates match the provided fixture", 8 pts).

Contract (tests/test_streaming.py depends on this):
  p = FeatureProcessor(window_seconds=W)
  for txn in events_in_time_order: p.update(txn)
  p.features(customer_id, at_time) -> {"transaction_count": int, "avg_amount": float}
  where the result covers events with (at_time - W) < event_time <= at_time.
"""
from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import datetime, timezone


def to_epoch(ts) -> float:
    """Accept an ISO-8601 string or a numeric epoch and return epoch seconds."""
    if isinstance(ts, (int, float)):
        return float(ts)
    return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).timestamp()


class FeatureProcessor:
    def __init__(self, feature_store=None, window_seconds: int | None = None):
        self.store = feature_store
        self.window_seconds = window_seconds or int(os.getenv("FEATURE_WINDOW_SECONDS", "86400"))
        # per customer: list of (event_epoch, amount)  — provided bookkeeping
        self._events: dict[str, list[tuple[float, float]]] = defaultdict(list)

    def update(self, txn: dict) -> None:
        """Record one transaction into the per-customer window buffer."""
        self._events[txn["customer_id"]].append((to_epoch(txn["timestamp"]), float(txn["amount"])))

    # --- IMPLEMENT ---------------------------------------------------------
    def features(self, customer_id: str, at_time) -> dict:
        """Return windowed features for `customer_id` evaluated at `at_time`.

        Include only events with (at_time - window_seconds) < event_time <= at_time.
        Return {"transaction_count": <int>, "avg_amount": <float>}.
        If there are no events in the window, return count 0 and avg_amount 0.0.
        """
        raise NotImplementedError("TODO: filter to the window, then count + mean")
    # -----------------------------------------------------------------------

    def process_and_store(self, txn: dict) -> dict:
        """Update the window with `txn`, recompute features as of this event's
        time, and persist them. Returns the features written."""
        self.update(txn)
        feats = self.features(txn["customer_id"], txn["timestamp"])
        if self.store is not None:
            self.store.store_customer_features(txn["customer_id"], feats)
        return feats


def run() -> None:
    """Consumer loop (provided). Reads the topic and updates the store."""
    from kafka import KafkaConsumer
    from src.streaming.feature_store import FeatureStore

    consumer = KafkaConsumer(
        os.getenv("KAFKA_TOPIC", "transactions"),
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(","),
        group_id="feature-processor",
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    processor = FeatureProcessor(feature_store=FeatureStore())
    print("[feature-processor] consuming...", flush=True)
    for msg in consumer:
        try:
            processor.process_and_store(msg.value)
        except NotImplementedError:
            raise
        except Exception as exc:  # keep the consumer alive on bad records
            print(f"[feature-processor] skipped a record: {exc}", flush=True)


if __name__ == "__main__":
    run()
