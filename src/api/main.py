"""
FastAPI serving app.

PROVIDED: app setup, model/feature-store load at startup, /health, /model/info,
and the request/response schemas (so input validation → HTTP 422 works for free).
YOU IMPLEMENT: the bodies of /predict and /predict_batch (graded: FastAPI Serving, 16 pts).
"""

from __future__ import annotations

import time
from typing import List, Optional

import redis
from fastapi import FastAPI
from loguru import logger
from pydantic import BaseModel

from src.api.fraud_detector import FraudDetector
from src.streaming.feature_store import FeatureStore

app = FastAPI(title="TrustBank Fraud Detection API")

# Loaded once at startup (do NOT load the model per-request).
detector = FraudDetector()
store = FeatureStore()


class Transaction(BaseModel):
    customer_id: str
    amount: float
    merchant_category: str
    is_online: bool
    timestamp: str
    transaction_id: Optional[str] = None


class FraudPrediction(BaseModel):
    transaction_id: Optional[str] = None
    fraud_probability: float
    is_fraud: int
    model_version: str
    latency_ms: float


def merge_features(txn: dict, stored: dict | None) -> dict:
    """
    Combine a transaction with its cached customer features.

    `stored` may be None (unknown customer or a degraded lookup). Transaction
    fields take precedence: they describe *this* event, while stored features
    summarize the customer's recent history.
    """
    return {**(stored or {}), **txn}


def lookup_features(customer_id: str) -> dict | None:
    """
    Fetch cached features, degrading to None if Redis is unreachable.

    A feature-store outage must not take down scoring: the detector still
    produces a prediction from the transaction fields alone.
    """
    try:
        return store.get_customer_features(customer_id)
    except redis.RedisError as exc:
        logger.warning(f"feature lookup degraded for {customer_id}: {exc}")
        return None


def lookup_features_batch(customer_ids: list[str]) -> dict:
    """Batch form of `lookup_features` — an outage degrades to no features."""
    try:
        return store.get_customer_features_batch(customer_ids)
    except redis.RedisError as exc:
        logger.warning(
            f"batch feature lookup degraded ({len(customer_ids)} ids): {exc}"
        )
        return {}


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/model/info")
async def model_info():
    return {"model_version": detector.model_version}


@app.post("/predict", response_model=FraudPrediction)
async def predict_fraud(txn: Transaction):
    start = time.perf_counter()
    stored = lookup_features(txn.customer_id)
    merged = merge_features(txn.model_dump(), stored)
    prediction = detector.predict(merged)
    latency_ms = (time.perf_counter() - start) * 1000

    logger_bnd = logger.bind(
        customer_id=txn.customer_id,
        latency_ms=round(latency_ms, 3),
        fraud_probability=prediction["fraud_probability"],
        degraded=stored is None,
    )
    logger_bnd.info("prediction served")
    return FraudPrediction(
        transaction_id=txn.transaction_id,
        latency_ms=round(latency_ms, 3),
        **prediction,
    )


@app.post("/predict_batch", response_model=List[FraudPrediction])
async def predict_fraud_batch(txns: List[Transaction]):
    """Score a batch of transactions, fetching all features in one Redis call."""
    start = time.perf_counter()
    customer_ids = list({txn.customer_id for txn in txns})
    stored_by_customer = lookup_features_batch(customer_ids)

    predictions = []
    for txn in txns:
        merged = merge_features(
            txn.model_dump(), stored_by_customer.get(txn.customer_id)
        )
        prediction = detector.predict(merged)
        predictions.append(
            FraudPrediction(
                transaction_id=txn.transaction_id,
                latency_ms=round((time.perf_counter() - start) * 1000, 3),
                **prediction,
            )
        )
    return predictions
