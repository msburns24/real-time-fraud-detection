# Real-Time Fraud Detection

Scores card transactions for fraud in real time. A Kafka stream of transactions
feeds a windowed feature processor, which writes rolling per-customer
aggregates into Redis; a FastAPI service joins those cached features with the
incoming transaction and scores it with a trained scikit-learn model.

Measured end-to-end: **p95 1.42 ms** at **1497 req/s**, against a 100 ms
requirement — roughly 70× headroom. Numbers and method in
[Performance](#performance); design rationale, failure modes and known
limitations in [docs/architecture.md](docs/architecture.md).

```
                  ┌──────────────────┐
                  │    simulator     │  produces transactions (+24h backfill)
                  └────────┬─────────┘
                           │  topic: transactions
                  ┌────────▼─────────┐
                  │      Kafka       │
                  └────────┬─────────┘
                           │  consumer group
                  ┌────────▼─────────┐
                  │ feature-processor│  rolling window → count / avg / last / max
                  └────────┬─────────┘
                           │  SET features:{customer_id}  (TTL 48h)
                  ┌────────▼─────────┐
                  │      Redis       │
                  └────────┬─────────┘
                           │  GET / MGET
                  ┌────────▼─────────┐
                  │   api (FastAPI)  │  /predict · /predict_batch
                  └──────────────────┘
```

## Quick start

Requires Docker with Compose v2. Everything runs locally.

```bash
cp .env.example .env
docker compose up --build
```

That starts all five services. Kafka and Redis gate on health checks, so the
API and processor only start once their dependencies actually accept
connections.

Give the simulator ~30s to replay its 24-hour backfill, then confirm features
have landed:

```bash
docker exec $(docker compose ps -q redis) redis-cli dbsize          # → 200
docker exec $(docker compose ps -q redis) redis-cli get features:CUST0001
```

Score a transaction:

```bash
curl -s localhost:8000/predict -H 'content-type: application/json' -d '{
  "transaction_id": "t-1",
  "customer_id": "CUST0001",
  "amount": 4000.0,
  "merchant_category": "online_retail",
  "is_online": true,
  "timestamp": "2026-01-01T00:50:00Z"
}'
```

Interactive docs: <http://localhost:8000/docs>.

### Running the API on your host instead

Useful while developing. Start only the infrastructure, and note that from the
host Kafka is on **29092**, not 9092:

```bash
docker compose up -d kafka redis
python -m venv .venv && .venv/bin/pip install -r requirements.txt
KAFKA_BOOTSTRAP_SERVERS=localhost:29092 REDIS_HOST=localhost \
  .venv/bin/uvicorn src.api.main:app --reload
```

The model artifact is committed, but to regenerate it:

```bash
.venv/bin/python data/generate_seed.py     # → data/seed_transactions.csv (6000 rows)
.venv/bin/python -m src.models.train       # → models/fraud_model_v1.pkl
```

If the artifact is missing, `FraudDetector` falls back to a transparent
rule-based scorer so the API still runs end to end.

## API

| Method | Path             | Notes                                              |
| ------ | ---------------- | -------------------------------------------------- |
| `GET`  | `/health`        | Liveness. Used by the container `HEALTHCHECK`.     |
| `GET`  | `/model/info`    | Reports the loaded model version.                  |
| `POST` | `/predict`       | Scores one transaction.                            |
| `POST` | `/predict_batch` | Scores a list; all features fetched in one `MGET`. |

Request body (`transaction_id` optional):

```json
{
  "transaction_id": "t-1",
  "customer_id": "CUST0001",
  "amount": 4000.0,
  "merchant_category": "online_retail",
  "is_online": true,
  "timestamp": "2026-01-01T00:50:00Z"
}
```

Response:

```json
{
  "transaction_id": "t-1",
  "fraud_probability": 1.0,
  "is_fraud": 1,
  "model_version": "sklearn-logreg-v1",
  "latency_ms": 0.612
}
```

`/predict_batch` takes a JSON array of the same objects and returns an array of
predictions **in request order**. It de-duplicates customer IDs and issues a
single `MGET`, so a 5-transaction batch across 3 customers costs one Redis
round-trip, not five. `latency_ms` on a batch item means _elapsed since the
batch began_, not per-item scoring time.

### Behaviour worth knowing

**Redis outages degrade rather than fail.** Feature lookups catch
`redis.RedisError` specifically — not a bare `except`, so genuine bugs still
surface — log a warning, and score from the transaction fields alone.
`/predict` returns 200 with Redis completely down. Connection and socket
timeouts are pinned at 1s, so a _blackholed_ host (dropped packets, no refusal)
degrades in ~1s instead of hanging indefinitely.

**Transaction fields win on collision.** Cached features and the incoming
transaction are merged with the transaction taking precedence — it describes
_this_ event, while cached features summarise history.

## Feature computation

The processor keeps a rolling window per customer and writes:

```json
{
  "transaction_count": 90,
  "avg_amount": 124.36,
  "last_amount": 127.64,
  "max_amount": 261.06
}
```

under `features:{customer_id}` with a 48h TTL, set atomically with the write
(`SET ... EX`), so a key can never be left without an expiry.

Window boundaries are **half-open**: an event counts when
`(at_time - window_seconds) < event_time <= at_time`. The model consumes
`["amount", "is_online", "avg_amount", "transaction_count"]`; `last_amount` and
`max_amount` are extra signal available to future models.

## Tests

```bash
.venv/bin/pytest -q tests/
```

7 tests. `test_streaming.py` runs with no infrastructure; the feature-store
tests skip automatically if Redis is unreachable, so bring Redis up to run the
full suite.

## Performance

```bash
.venv/bin/python tests/test_performance.py --n 5000 --url http://localhost:8000
```

Measured against the full stack, direct to the API
(`results.json`):

| Metric     | Result         |
| ---------- | -------------- |
| Requests   | 5000, 0 errors |
| Throughput | 1497 req/s     |
| p50        | 0.50 ms        |
| p95        | 1.42 ms        |
| p99        | 3.24 ms        |
| max        | 5.47 ms        |

Median of three consecutive runs (throughput 1392–1571 req/s, p95 1.27–1.49 ms,
zero errors throughout).

Run with `--n 5000` rather than the default 1000: at ~1.5k rps, 1000 requests
is under a second of sampling, which makes p99 mostly noise and lets startup
dominate throughput.

These are cache **hits**, verified rather than assumed — the simulator creates
`CUST0000`-`CUST0199` and the harness draws from `CUST{0..199}`, the same ID
space. So the figures cover the real lookup → merge → score path.

**Where the time goes** (`python -m scripts.profile_predict -n 2000`, per-stage
p50 in ms):

| Stage            | p50        |
| ---------------- | ---------- |
| input validation | 0.0049     |
| Redis `GET`      | 0.0494     |
| merge            | 0.0006     |
| model inference  | 0.0663     |
| logging          | 0.0215     |
| **total**        | **0.1427** |

Application code is only **19%** of a 0.769 ms request. Benchmarking `/health`
(which does no work) through the same client gives a transport floor of 0.260
ms — which leaves **~0.366 ms, 48% of every request, in FastAPI's per-request
model machinery**. Inbound validation is negligible at 0.005 ms, so the cost is
on the way out: `response_model` re-validates the already-constructed
`FraudPrediction` against the model that built it.

The system is framework-bound, not compute-bound. See
[the report](docs/report.md#p3-bottleneck-analysis) for the full decomposition
and what was done about it.

## Blue-green deployment

```bash
docker compose -f deployment/docker-compose.blue-green.yml up --build
bash deployment/switch_traffic.sh    # run again to roll back
```

Stable endpoint is <http://localhost:8080>; colours are directly reachable on
`:8001` (blue) and `:8002` (green) for health gating. Full design, cutover
sequence, rollback story and Kubernetes mapping:
**[docs/blue_green_design.md](docs/blue_green_design.md)**.

Verified under live load: 17,809 requests served by blue and 42,330 by green
across a mid-load cutover, 1 error in 60,000 (0.002%, a keep-alive close race).

## Configuration

All settings live in `src/config.py` as a frozen `Settings` dataclass — each
variable and its default is declared exactly once, and no other module calls
`os.getenv`. Constructor arguments still override, which is what the tests use.

| Variable                  | Default                     | Purpose                       |
| ------------------------- | --------------------------- | ----------------------------- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092`            | Broker list (comma-separated) |
| `KAFKA_TOPIC`             | `transactions`              | Source topic                  |
| `REDIS_HOST`              | `localhost`                 | Feature store host            |
| `REDIS_PORT`              | `6379`                      | Feature store port            |
| `REDIS_PASSWORD`          | _(none)_                    | Optional auth                 |
| `FEATURE_WINDOW_SECONDS`  | `86400`                     | Rolling window size           |
| `FEATURE_TTL_SECONDS`     | `172800`                    | Feature expiry in Redis       |
| `MODEL_PATH`              | `models/fraud_model_v1.pkl` | Model artifact                |

In containers, use `kafka:9092` / `redis`; from the host, `localhost:29092` /
`localhost`. Keep `FEATURE_WINDOW_SECONDS` consistent with the simulator's
`--backfill-hours` — a mismatch corrupts aggregates silently instead of failing
loudly.

> `.env.example` also lists `API_PORT=8000`, but nothing currently reads it —
> the port is fixed at 8000 in the Dockerfile's uvicorn command. Changing the
> published port means editing `docker-compose.yml`.

## Layout

```
src/
  config.py                        settings — single source for env vars
  _logging.py                      shared loguru setup (one format, per-service tag)
  api/main.py                      /predict, /predict_batch, degradation, latency logs
  api/fraud_detector.py            model load + rule-based fallback
  streaming/feature_processor.py   windowed_stats() + Kafka consumer loop
  streaming/feature_store.py       Redis get/set/batch, TTL, shared connection pool
  streaming/transaction_simulator.py  producer + backfill
scripts/
  bench_feature_store.py           Redis retrieval latency benchmark
  profile_predict.py               per-stage hot-path profiler
docs/
  architecture.md                  system design, failure modes, known limitations
  plan.md                          build plan + decision log
  blue_green_design.md             deployment design + cutover evidence
tests/                             7 tests + a load harness
```

`scripts/` modules import `from src...`, so run them as
`python -m scripts.<name>` from the repo root — invoking by file path puts
`scripts/` on `sys.path` and the import fails.

## Notes on the provided infrastructure

Two latent defects in the supplied Kafka configuration surfaced during
integration and are fixed in `docker-compose.yml`, each with an explanatory
comment:

1. **Health check never passed.** The probe called `kafka-topics.sh` bare, but
   `apache/kafka:3.8.0` keeps its scripts in `/opt/kafka/bin`, off the probe
   shell's `PATH`. The broker reported `unhealthy` forever while serving
   normally. Fixed with an absolute path.

2. **No consumer group could ever form.** `offsets.topic.replication.factor`
   defaults to 3, but this is a single-broker cluster, so `__consumer_offsets`
   could never be created and `FIND_COORDINATOR` timed out. The failure was
   silent: the producer wrote 9600 messages successfully while the consumer
   blocked forever on an empty assignment. Fixed with
   `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1`. Note this requires
   `docker compose down -v` — the broker persists the old value in its
   metadata, so a plain restart won't pick up the change.

A third bug was in `deployment/switch_traffic.sh`: it used `sed -i`, which
replaces the file's inode, while the compose file bind-mounts `nginx.conf` as a
single file. Single-file mounts bind the _inode_, so the container kept reading
the orphaned original — the script printed success and reloaded stale config
while traffic never moved. Fixed by rewriting in place.
