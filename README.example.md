# Project 2 — Real-Time Fraud Detection System (Starter Kit)

Everything runs locally with Docker — no cloud account required. **Kafka and Redis
are already provided and working** so you don't fight infrastructure. You implement
the fraud-detection logic **and** containerize your own app.

## Setup
```bash
cp .env.example .env
python data/generate_seed.py          # create the seed dataset
python -m src.models.train            # train the baseline model (optional; a fallback exists)
```

## Bring the system up
Kafka + Redis are defined for you in `docker-compose.yml`. You add your three app
services (`api`, `feature-processor`, `simulator`) and write the `Dockerfile`, then:
```bash
docker compose up --build
# API:        http://localhost:8000        (docs at /docs)
# Kafka host: localhost:29092
```
Tip: start just the provided infra while you develop —
`docker compose up kafka redis` — and run your API on your host until it works.

## What's provided vs. what you build
| Provided (don't change) | You implement |
|---|---|
| Kafka + Redis in `docker-compose.yml` | the `Dockerfile` (multi-stage, non-root, healthcheck) |
| `transaction_simulator.py` (producer + 24h backfill) | the `api`/`feature-processor`/`simulator` services in `docker-compose.yml` |
| `models/train.py`, `fraud_detector.py` (baseline + fallback) | `feature_store.py` — Redis get/set, TTL, batch |
| `deployment/` blue-green (nginx + compose + switch) | `feature_processor.py::features()` — the windowing |
| `.env.example`, seed generator, tests | `api/main.py` — `/predict`, `/predict_batch` bodies |
| | `docs/blue_green_design.md` + the switch demo |

## Self-check (the same tests we grade with)
```bash
pip install -r requirements.txt
pytest -q tests/            # windowing + API + feature store (Redis auto-skips if down)
python tests/test_performance.py --n 1000 --url http://localhost:8000   # p50/p95/p99
```

## Blue-green demo (no Kubernetes)
```bash
docker compose -f deployment/docker-compose.blue-green.yml up --build
bash deployment/switch_traffic.sh     # flip blue <-> green (run again to roll back)
# stable endpoint: http://localhost:8080
```

## Optional Azure bonus (+5)
Point the producer at an **Azure Event Hubs** Kafka endpoint (same code, different
`KAFKA_BOOTSTRAP_SERVERS` + SASL), and/or publish your image to **Azure Container
Registry**. Document it in your report.

## Transaction schema
```json
{"transaction_id":"...","customer_id":"CUST0001","amount":1500.0,
 "merchant_category":"online_retail","is_online":true,"timestamp":"2026-01-01T00:50:00Z"}
```
The feature store writes `{"transaction_count": int, "avg_amount": float, ...}` under `features:{customer_id}`.
