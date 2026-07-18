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
from collections import defaultdict
from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING, TypeAlias

from src.config import settings

# ---- Type Hints --------------------------------------------------------------


if TYPE_CHECKING:
    EventTimestamp: TypeAlias = float
    TransactionAmount: TypeAlias = float
    CustomerID: TypeAlias = str
    EventsList: TypeAlias = list[tuple[EventTimestamp, TransactionAmount]]


# ---- Helpers -----------------------------------------------------------------


def to_epoch(ts) -> float:
    """Accept an ISO-8601 string or a numeric epoch and return epoch seconds."""
    if isinstance(ts, (int, float)):
        return float(ts)
    return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).timestamp()


def _mean(values: Sequence[float], default: float = 0.0) -> float:
    """Provides mean with option for default value if sequence is empty."""
    n = len(values)
    return default if n == 0 else sum(values) / n


def windowed_stats(
    events: EventsList,
    start_exclusive: EventTimestamp,
    end_inclusive: EventTimestamp,
) -> dict:
    """
    Aggregate `(event_epoch, amount)` pairs whose time falls in the window
    `(start_exclusive, end_inclusive]`.

    Pure function - no state or I/O, so can be reused independent of the Kafka
    consumer.
    """
    windowed = [
        (t, a) for (t, a) in events if start_exclusive < t <= end_inclusive
    ]
    amounts = [a for (_, a) in windowed]
    return dict(
        transaction_count=len(amounts),
        avg_amount=_mean(amounts, default=0.0),
        last_amount=windowed[-1][1] if windowed else 0.0,
        max_amount=max(amounts) if amounts else 0.0,
    )


class FeatureProcessor:
    def __init__(self, feature_store=None, window_seconds: int | None = None):
        self.store = feature_store
        self.window_seconds = window_seconds or settings.feature_window_seconds
        # per customer: list of (event_epoch, amount)  — provided bookkeeping
        self._events: dict[CustomerID, EventsList] = defaultdict(list)

    def update(self, txn: dict) -> None:
        """Record one transaction into the per-customer window buffer."""
        event = (to_epoch(txn["timestamp"]), float(txn["amount"]))
        self._events[txn["customer_id"]].append(event)

    def features(self, customer_id: str, at_time: float) -> dict:
        """
        Return windowed features for `customer_id` evaluated at `at_time`.

        Include only events with (at_time - window_seconds) < event_time <= at_time.
        Return {"transaction_count": <int>, "avg_amount": <float>}.
        If there are no events in the window, return count 0 and avg_amount 0.0.
        """
        end = to_epoch(at_time)
        start = end - self.window_seconds
        return windowed_stats(self._events.get(customer_id, []), start, end)

    def process_and_store(self, txn: dict) -> dict:
        """Update the window with `txn`, recompute features as of this event's
        time, and persist them. Returns the features written."""
        self.update(txn)
        feats = self.features(txn["customer_id"], txn["timestamp"])
        if self.store is not None:
            self.store.store_customer_features(txn["customer_id"], feats)
        return feats


# ---- Main --------------------------------------------------------------------


def run() -> None:
    """Consumer loop (provided). Reads the topic and updates the store."""
    from kafka import KafkaConsumer

    from src.streaming.feature_store import FeatureStore

    consumer = KafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_servers,
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
