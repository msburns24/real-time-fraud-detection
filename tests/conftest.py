"""Shared pytest fixtures."""
import json
import os

import pytest

FIX = os.path.join(os.path.dirname(__file__), "fixtures")


def load_fixture(name: str) -> dict:
    with open(os.path.join(FIX, name)) as fh:
        return json.load(fh)


@pytest.fixture
def client():
    """FastAPI test client. Importing the app does not require Redis to be up."""
    from fastapi.testclient import TestClient
    from src.api.main import app
    return TestClient(app)


@pytest.fixture
def feature_store():
    """A live FeatureStore, or skip if Redis is not reachable."""
    from src.streaming.feature_store import FeatureStore
    fs = FeatureStore(host=os.getenv("REDIS_HOST", "localhost"))
    try:
        fs.client.ping()
    except Exception:
        pytest.skip("Redis not reachable (start it with docker compose up)")
    return fs
