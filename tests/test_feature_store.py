"""Feature store checks (rubric: Redis Feature Store).

Requires Redis (auto-skips if it is not running). Verifies round-trip,
TTL, and batch retrieval against the student's FeatureStore implementation.
"""


def test_store_and_get_roundtrip(feature_store):
    feats = {"transaction_count": 3, "avg_amount": 200.0, "last_amount": 300.0}
    feature_store.store_customer_features("CUST_TEST_1", feats)
    got = feature_store.get_customer_features("CUST_TEST_1")
    assert got is not None
    assert int(got["transaction_count"]) == 3
    assert float(got["avg_amount"]) == 200.0


def test_ttl_is_set(feature_store):
    feature_store.store_customer_features("CUST_TEST_TTL", {"transaction_count": 1, "avg_amount": 10.0})
    ttl = feature_store.ttl("CUST_TEST_TTL")
    assert ttl > 0, "expected a positive TTL (features must expire)"
    assert ttl <= feature_store.ttl_seconds


def test_batch_retrieval(feature_store):
    feature_store.store_customer_features("CUST_B1", {"transaction_count": 1, "avg_amount": 5.0})
    feature_store.store_customer_features("CUST_B2", {"transaction_count": 2, "avg_amount": 6.0})
    out = feature_store.get_customer_features_batch(["CUST_B1", "CUST_B2", "CUST_MISSING"])
    assert set(out.keys()) == {"CUST_B1", "CUST_B2", "CUST_MISSING"}
    assert out["CUST_B1"] is not None and out["CUST_B2"] is not None
    assert out["CUST_MISSING"] is None
