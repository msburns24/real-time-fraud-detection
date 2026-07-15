"""Windowed-aggregate check (rubric: Streaming Feature Computation, 8 pts).

Feeds the deterministic fixture through the student's FeatureProcessor and
asserts the windowed count + average match exactly.
"""
import pytest

from tests.conftest import load_fixture
from src.streaming.feature_processor import FeatureProcessor


def test_windowed_aggregates_match_fixture():
    fx = load_fixture("window_fixture.json")
    proc = FeatureProcessor(window_seconds=fx["window_seconds"])
    for event in fx["events"]:
        proc.update(event)

    for customer_id, expected in fx["expected"].items():
        feats = proc.features(customer_id, fx["evaluate_at"])
        assert feats["transaction_count"] == expected["transaction_count"], (
            f"{customer_id}: count {feats['transaction_count']} != {expected['transaction_count']}"
        )
        assert feats["avg_amount"] == pytest.approx(expected["avg_amount"], rel=1e-3), (
            f"{customer_id}: avg {feats['avg_amount']} != {expected['avg_amount']}"
        )
