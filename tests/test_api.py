"""API contract checks (rubric: FastAPI Serving).

- /health returns 200                               (passes out of the box)
- missing required field returns HTTP 422           (passes out of the box: Pydantic)
- valid /predict returns a schema-valid response    (passes once you implement predict)
"""
from tests.conftest import load_fixture


def test_health_ok(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json().get("status") == "ok"


def test_predict_missing_field_returns_422(client):
    body = load_fixture("predict_sample.json")["missing_amount"]
    resp = client.post("/predict", json=body)
    assert resp.status_code == 422


def test_predict_returns_valid_schema(client):
    body = load_fixture("predict_sample.json")["valid"]
    resp = client.post("/predict", json=body)
    assert resp.status_code == 200, f"expected 200, got {resp.status_code}: {resp.text}"
    data = resp.json()
    for field in ("fraud_probability", "is_fraud", "model_version", "latency_ms"):
        assert field in data, f"missing '{field}' in response"
    assert 0.0 <= data["fraud_probability"] <= 1.0
    assert data["is_fraud"] in (0, 1)
