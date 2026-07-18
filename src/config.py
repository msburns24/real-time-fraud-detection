"""
Central application settings.

Every environment variable the system reads is declared here once, with its
default, so no module reaches for `os.getenv` on its own. Constructor arguments
elsewhere still act as explicit overrides (useful in tests).
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # Redis / feature store
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_password: str | None = None
    feature_ttl_seconds: int = 172_800  # 48h

    # Streaming
    feature_window_seconds: int = 86_400  # 24h
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "transactions"

    # Model
    model_path: str = "models/fraud_model_v1.pkl"

    @classmethod
    def from_env(cls) -> Settings:
        """
        Build settings from the environment, falling back to the defaults above.
        """
        return cls(
            redis_host=os.getenv("REDIS_HOST", cls.redis_host),
            redis_port=int(os.getenv("REDIS_PORT", str(cls.redis_port))),
            redis_password=os.getenv("REDIS_PASSWORD") or None,
            feature_ttl_seconds=int(
                os.getenv("FEATURE_TTL_SECONDS", str(cls.feature_ttl_seconds))
            ),
            feature_window_seconds=int(
                os.getenv(
                    "FEATURE_WINDOW_SECONDS", str(cls.feature_window_seconds)
                )
            ),
            kafka_bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", cls.kafka_bootstrap_servers
            ),
            kafka_topic=os.getenv("KAFKA_TOPIC", cls.kafka_topic),
            model_path=os.getenv("MODEL_PATH", cls.model_path),
        )

    @property
    def kafka_servers(self) -> list[str]:
        """Bootstrap servers as the list kafka-python expects."""
        return self.kafka_bootstrap_servers.split(",")


settings = Settings.from_env()
