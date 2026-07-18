"""
Feature Store (Redis) — YOU IMPLEMENT THE MARKED METHODS.

Contract (the provided tests in tests/test_feature_store.py depend on this):
  key layout     : "features:{customer_id}"  ->  JSON string of a features dict
  TTL            : every write expires after ttl_seconds
  batch retrieval: one round-trip (pipeline / MGET), not a loop of GETs

This is a graded deliverable (Redis Feature Store, 12 pts). The connection
setup is provided; the read/write logic is yours.
"""

from __future__ import annotations

import json
from typing import Optional

import redis
from redis import ConnectionPool

from src.config import settings

_POOLS: dict[tuple[str, int, str | None], ConnectionPool] = {}


def _get_pool(host: str, port: int, password: str | None) -> ConnectionPool:
    """Return the shared connection pool for Redis, creating it once."""
    target = (host, port, password)
    if target not in _POOLS:
        _POOLS[target] = ConnectionPool(
            host=host,
            port=port,
            password=password,
            decode_responses=True,
            # Fail fast instead of hanging if Redis is unreachable. The API's
            # graceful-degradation path (lookup_features) depends on this.
            socket_connect_timeout=1,
            socket_timeout=1,
        )
    return _POOLS[target]


class FeatureStore:
    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        password: str | None = None,
        ttl_seconds: int | None = None,
    ) -> None:
        host = host or settings.redis_host
        port = port or settings.redis_port
        password = password or settings.redis_password
        self.ttl_seconds = ttl_seconds or settings.feature_ttl_seconds
        
        pool = _get_pool(host, port, password)
        self.client = redis.Redis(connection_pool=pool)

    @staticmethod
    def _key(customer_id: str) -> str:
        return f"features:{customer_id}"

    def store_customer_features(self, customer_id: str, features: dict) -> None:
        """
        Serialize `features` to JSON and SET it under _key(customer_id)
        with an expiry of self.ttl_seconds. (Hint: redis SET(..., ex=...))
        """
        key = self._key(customer_id)
        value = json.dumps(features)
        self.client.set(key, value, ex=self.ttl_seconds)

    def get_customer_features(self, customer_id: str) -> Optional[dict]:
        """
        Return the features dict for a customer, or None if absent/expired.
        """
        key = self._key(customer_id)
        raw_value = self.client.get(key)
        return json.loads(raw_value) if raw_value else None

    def get_customer_features_batch(self, customer_ids: list[str]) -> dict:
        """
        Return {customer_id: features_dict_or_None} using a SINGLE round-trip
        (redis pipeline or MGET), not one GET per id.
        """
        if not customer_ids:
            return {}

        keys = list(map(self._key, customer_ids))
        raw_values = self.client.mget(keys)
        return {
            cid: (json.loads(value) if value else None)
            for cid, value in zip(customer_ids, raw_values)
        }

    def ttl(self, customer_id: str) -> int:
        """
        Remaining TTL in seconds (used by tests). -2 = no key, -1 = no expiry.
        """
        return self.client.ttl(self._key(customer_id))
