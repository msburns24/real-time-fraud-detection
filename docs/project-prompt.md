# Project 2: Real-Time Fraud Detection

## Overview

> **Scenario:** You work for TrustBank, a credit card company processing
> millions of transactions daily. You need to build a real-time fraud detection
> system that scores transactions within 100ms to minimize fraud while avoiding
> false positives that frustrate customers.

**Scope note:** This project covers **Units 7-10** — real-time streaming,
feature stores, model serving, and containerization. Everything below runs on
your **local machine with Docker** — no cloud account is required to earn full
marks.

### Key Deliverables

1. Real-time transaction ingestion with **Apache Kafka** (Azure Event Hubs
   optional — bonus)
2. Streaming feature-engineering pipeline (windowed aggregates in Python)
3. Low-latency **Redis** feature store
4. Containerized **FastAPI** model-serving API
5. Blue-green deployment — **design write-up + `docker-compose` swap** (no
   Kubernetes)
6. Performance testing & latency analysis

---

## Project Learning Objectives

By completing this project, you will:

- **Process** real-time event streams with Kafka (windowing, consumer groups)
- **Implement** a low-latency feature store
- **Serve** an ML model behind a FastAPI async API
- **Containerize** the system with Docker and `docker-compose`
- **Design** a blue-green deployment for zero-downtime releases
- **Measure and analyze** serving latency (p50/p95/p99)

Some of the tools you may need for this project:

- **Docker Desktop** installed locally
- **Python 3.11+**
- **Postman** or **curl** for API testing
- **VS Code** with Docker extension (or similar)

**Baseline stack (local — required):**

- **Apache Kafka** (via `docker-compose`) with **kafka-python**
- **Redis** (via `docker-compose`)
- **FastAPI** + **Uvicorn**

**Optional Azure track (bonus, up to +10):**

- **Azure Event Hubs** (Kafka-compatible endpoint) for ingestion, and/or
- **Azure Container Registry** to publish your image

---

## Using the Starter Kit

The starter kit gives you a **working Kafka + Redis** so you never fight
infrastructure — you spend your time on the parts that are the point of the
project.

**Provided for you (don't rebuild):** Kafka + Redis (already wired in
`docker-compose.yml`), the transaction simulator, a baseline model + training
script, the blue-green scripts, and the test suite.

**You build:** the `Dockerfile`, your app services in `docker-compose.yml`
(`api`, `feature-processor`, `simulator`), the Redis feature store, the
windowed feature computation, and the `/predict` logic.

**The tests are your compass, not a cage.** `tests/` defines what "correct"
means — run `pytest -q tests/` any time — but not _how_ to get there. Each step
below suggests **one** possible path; you are encouraged to find your own.
**Any approach that passes the tests and meets the rubric earns full marks.**

---

## Project Structure

Your project should have a similar structure to the one described below
(⭐️ = provided in the starter kit):

```
fraud-detection-system/
├── README.md
├── docker-compose.yml            # ⭐️ Kafka + Redis provided; you add the app services
├── requirements.txt              # ⭐️
├── Dockerfile                    # you write this
├── src/
│   ├── api/
│   │   ├── main.py               # FastAPI app — you write /predict
│   │   └── fraud_detector.py     # ⭐️ baseline detector provided
│   ├── streaming/
│   │   ├── transaction_simulator.py  # ⭐️ Kafka producer provided
│   │   ├── feature_processor.py      # you write the windowing
│   │   └── feature_store.py          # you write the Redis logic
│   └── models/
│       └── train.py              # ⭐️ baseline training provided
├── tests/                        # ⭐️ your self-check (same tests we grade with)
├── deployment/
│   ├── docker-compose.blue-green.yml # ⭐️
│   └── switch_traffic.sh             # ⭐️
└── docs/
    ├── architecture.md
    └── blue_green_design.md      # you write this
```

---

## Part A: Streaming Pipeline & Feature Store (40 points)

### Step 1: Start the Streaming Infrastructure (Kafka + Redis)

_Recall: Kafka is the event bus from Unit 7 — a distributed, partitioned log_
_that handles millions of events per second._

**Kafka and Redis are provided and working** in the starter kit's
`docker-compose.yml`. You don't set them up from scratch — start them with:

```bash
docker compose up kafka redis
# the `transactions` topic auto-creates with 3 partitions
```

> **💡 How to approach it** (one option — you're free to do it differently):
> develop your Python code on your host machine first, pointing it at
> `localhost:29092` (Kafka) and `localhost:6379` (Redis), before containerizing
> it in Part B. **Search:** `docker compose up single service`,
> `kafka topic partitions`.

_Optional Azure (bonus): point your producer at an **Azure Event Hubs Kafka
endpoint** instead of local Kafka — the same kafka-python code works with a
different `bootstrap_servers` + SASL config._

### Step 2: Transaction Data Simulator (provided)

The transaction simulator is **provided** — it's a Kafka producer that first
backfills 24h of history (so your windows populate) and then streams live
traffic. You run it, and may extend it if you like.

**File: `src/streaming/transaction_simulator.py`** (provided)

```bash
# run it (after Kafka is up):
python -m src.streaming.transaction_simulator --backfill-hours 24 --duration 300 --rate 50
```

> **💡 Good to know:** each message is JSON with `transaction_id`,
> `customer_id`, `amount`, `merchant_category`, `is_online`, `timestamp`. Open
> the file to see the schema before you build against it. **Search:**
> `kafka-python KafkaProducer`.

### Step 3: Streaming Feature Computation (you build this)

_Recall: In Unit 7 you learned windowing — tumbling vs. sliding windows, and
event time vs. processing time. Here you compute rolling features in a Kafka
**consumer** (plain Python)._

The starter kit provides a **seed dataset with 24h of history** and a
**configurable window** (`FEATURE_WINDOW_SECONDS`), so your features populate
even in a short run. The grading fixture uses a small, deterministic window
with known expected numbers — match it exactly.

> **💡 How to approach it** (one option — any solution that passes the fixture
> is fine): keep a running list of each customer's recent transactions, drop
> the ones older than your time window, then count them and average their
> amounts. **Search:** `rolling time window aggregation python`,
> `sliding window per key`, `collections deque`,
> `kafka-python consumer example`.

**File: `src/streaming/feature_processor.py`** — implement the marked method.

### Step 4: Feature Store (you build this)

Store pre-computed features in Redis so the API can read them in milliseconds.

> **💡 How to approach it** (one option — solve it your way): save each
> customer's features as a small JSON value under a key like
> `features:CUST1234`, set an **expiry (TTL)** when you write it, and read many
> customers in a single round-trip. **Search:** `redis-py set get`,
> `redis key expiry TTL`, `redis pipeline mget`.

**File: `src/streaming/feature_store.py`** — implement `store`, `get`, and
batch retrieval.

---

## Part B: Model Serving & Containerization (40 points)

### Step 5: FastAPI Model Serving (you build this)

_Recall: FastAPI is a modern Python web framework that's great for ML APIs —
fast, with automatic docs and type checking._

The app skeleton (`/health`, the request/response schemas) is provided; you
write the prediction logic.

> **💡 How to approach it** (one option — you can structure it however you
> like): in `/predict`, look up the customer's features from the store, combine
> them with the incoming transaction, call the provided detector, time the call
> for your latency log, and return the result. **Search:**
> `fastapi post endpoint pydantic`, `fastapi response_model`,
> `python time.perf_counter`.

**File: `src/api/main.py`** — implement `/predict` and `/predict_batch`.

### Step 6: Model Integration (baseline provided)

A baseline model + training script are **provided**, and model accuracy is
**not** graded (this is a systems course). Your job is only to make sure
`/predict` **loads and uses** the model, and that it's loaded **once** at
startup.

> **💡 Good to know:** the provided detector already falls back to a simple
> rule if no trained file is present, so the API runs from day one. **Search:**
> `joblib load model`.

### Step 7: Containerization (you build this)

Package your API into a container, then wire your app services into the
provided compose file.

> **💡 How to approach it** (one option — other valid Dockerfiles are fine):
> write a **multi-stage** Dockerfile (install dependencies in a build stage,
> copy them into a slim runtime stage), run as a **non-root** user, add a
> **healthcheck** on `/health`, and run `uvicorn`. Then add your `api`,
> `feature-processor`, and `simulator` services (each `build: .`) to
> `docker-compose.yml` alongside the provided Kafka + Redis. **Search:**
> `docker multi-stage build python`, `dockerfile non-root user`,
> `dockerfile HEALTHCHECK`, `docker compose build service`.

**Files:** `Dockerfile` (you write it) and the app-service section of
`docker-compose.yml` (Kafka + Redis are already there).

### Step 8: Blue-Green Deployment (design + demo)

_Recall: In Unit 10 you learned blue-green and canary releases. You'll
demonstrate the **concept** here without Kubernetes._

The kit provides an nginx-based two-version setup
(`deployment/docker-compose.blue-green.yml`) and a `switch_traffic.sh` script.

> **💡 How to approach it** (one option): run the two versions, flip traffic
> with the provided script, capture that it happens with **no dropped
> requests**, and write up the strategy — how traffic cuts over, how you roll
> back, and how this would map to Kubernetes later. **Search:**
> `blue green deployment nginx`, `nginx reload zero downtime`.

**Deliverable:** `docs/blue_green_design.md` + a demo of the swap.

---

## Performance Testing (10 points)

Use the **provided load-test harness** (`tests/test_performance.py`) and the
fixed-seed dataset. Report **p50 / p95 / p99** latency and **throughput**, then
analyze: where is the bottleneck, and what one change did you try?

_You are graded on **measuring and analyzing** latency correctly — not on
hitting a specific number. `<100ms` p95 is a **stretch target**, not a
pass/fail bar._

---

## Live Demonstration (10 points)

A **recorded screencast (3-5 min) is accepted** — you do not have to present
live. Demonstrate:

1. Sending transactions into Kafka
2. Real-time feature computation (features appear in Redis)
3. A prediction via the API, with the latency shown
4. A blue-green traffic switch (`docker-compose`)

---

## Submission Requirements

### 1. GitHub Repository

Your repository should include:

- All source code organized as shown in the project structure
- Your `Dockerfile`, the completed `docker-compose.yml`,
  `docker-compose.blue-green.yml`, and `switch_traffic.sh`
- `docs/blue_green_design.md`
- Complete `README` with setup instructions (`docker compose up`)
- Performance test results

### 2. Technical Report

Submit a `.pdf` (max **8 pages**) to Canvas covering:

**Part A: Streaming & Feature Store (3 pages)**

- System architecture diagram (Kafka → feature processor → Redis → API)
- Topic and partition design
- Windowing approach for the streaming features
- Feature store design and **measured** retrieval latency
- Links to implementation

**Part B: Serving & Containerization (3 pages)**

- API design and endpoints; Pydantic validation
- Containerization approach (multi-stage image + `docker-compose`)
- Blue-green design and screenshots of the swap
- Links to implementation

**Performance (2 pages)**

- Latency percentiles (p50, p95, p99) and throughput
- Bottleneck analysis and one optimization you tried

### 3. Live Demonstration

See above (recorded screencast accepted).

---

## Grading Rubric

_The tests in the starter kit define **what** counts as correct; **how** you
get there is up to you. Any solution that passes the tests and meets these
criteria earns full marks._

### Part A: Streaming Pipeline & Feature Store (40 points)

**Kafka Ingestion (10 points)**

- Provided Kafka + Redis start via `docker compose up`; the `transactions`
  topic exists with ≥2 partitions — **2**
- The simulator service produces transactions (required fields, `fraud_rate`)
  to your topic — **3**
- Your consumer reads the stream end-to-end (all partitions) — **5**

**Streaming Feature Computation (16 points)**

- Windowed aggregates match the provided fixture — **10**
- Windows keyed by `customer_id`; late / at-least-once handling documented —
  **3**
- Computed features written to the feature store — **3**

**Redis Feature Store (14 points)**

- `get` / `set` customer features passes the provided test — **5**
- TTL set and verified (feature expires) — **3**
- Batch retrieval implemented — **4**
- Retrieval p95 measured and reported — **2**

### Part B: Model Serving & Containerization (40 points)

**FastAPI Serving (16 points)**

- `/health` returns 200 — **1**
- Missing required field returns HTTP 422 (provided test) — **2**
- `/predict` returns schema-valid JSON with a real prediction (provided test) —
  **6**
- `/predict` retrieves store features and merges them with the transaction —
  **4**
- `/predict_batch` works — **3**

**Model Integration (3 points)**

- The trained artifact is wired into `/predict` and loaded **once** at startup
  (starter baseline acceptable) — **3**

**Containerization (17 points)**

- Multi-stage `Dockerfile` builds successfully — **5**
- Container runs as **non-root**; health check defined — **4**
- `docker compose up` runs the full system — your app services wired to the
  provided Kafka + Redis — **8**

**Blue-Green Deployment (4 points)**

- `blue_green_design.md` written **and** a working `docker-compose` two-version
  swap with zero dropped requests — **4**

### Performance (10 points)

- Uses the provided load harness + fixed-seed dataset — **2**
- Reports p50 / p95 / p99 and throughput — **4**
- Identifies a bottleneck and one optimization tried — **4**

_Graded on method and analysis, not on hitting a specific latency number._

### Live Demonstration (10 points)

_Recorded screencast accepted._

- Transactions sent to Kafka → features computed live — **3**
- Prediction returned via the API with latency shown — **4**
- Blue-green traffic switch demonstrated — **3**

### Bonus: Azure Track (up to +10 points)

- Ingestion via **Azure Event Hubs** (Kafka-compatible endpoint) — **+6**
- Container image published to **Azure Container Registry** — **+4**

### Additional Criteria

**Code Quality (deductions up to -10 points)**

- Poor code organization
- Missing comments/documentation
- No error handling
- Hardcoded credentials

**Report Quality (deductions up to -10 points)**

- Missing required sections
- Unclear explanations
- No screenshots/evidence
- Broken links

---

## Performance Testing Guide

### Load Testing Script

**File: `tests/test_performance.py`** _(a harness is provided in the starter
kit)_

```
# after `docker compose up`, fire 1000 requests and get p50/p95/p99 + throughput:
python tests/test_performance.py --n 1000 --url http://localhost:8000
```

It writes a `results.json` you can quote in your report.

---

### Some Recommendations

1. **Start with Small Scale**: Test with 10 TPS before scaling to 100 TPS
2. **Monitor Everything**: Add logging liberally while developing
3. **Test Locally First**: Everything runs under `docker-compose` — no cloud
   needed
4. **Profile Your Code**: Use cProfile to find bottlenecks
5. **Cache Aggressively**: Redis is your friend for fast feature retrieval
6. **Do not** load the model on every request - load once at startup
7. **Do not** make synchronous calls to external services - use async
8. **Do not** ignore connection pooling for Redis
9. **Do not** skip load testing - 100ms p95 is harder than you think

_Some additional optimization tips:_

**If latency is too high:**

- Profile your code to find bottlenecks
- Use batch prediction when possible
- Cache frequently accessed features
- Consider model simplification
- Use an async Redis client

**If throughput is too low:**

- Run multiple Uvicorn/Gunicorn workers
- Implement connection pooling
- Reuse the Kafka producer/consumer instead of recreating it

---

### Testing Checklist

Before submission, verify:

- Kafka + Redis run via `docker-compose`; `transactions` topic has ≥2
  partitions
- Simulator produces realistic transactions to Kafka
- Feature processor computes windowed features and writes them to Redis
- Feature store retrieves features in <50ms (measured)
- API returns predictions; latency is logged
- `docker compose up` brings up the whole system
- Blue-green swap works with zero dropped requests
- Load test run; p50 / p95 / p99 reported
- `README` complete; report includes all required sections

---

### Some Resources

Streaming & Store:

- [Apache Kafka Quickstart](https://kafka.apache.org/quickstart)
- [kafka-python Documentation](https://kafka-python.readthedocs.io/)
- [Redis Python Client](https://redis-py.readthedocs.io/)

Serving & Containers:

- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Docker Multi-stage Builds](https://docs.docker.com/build/building/multi-stage/)
- [Docker Compose](https://docs.docker.com/compose/)

Performance:

- [Python asyncio](https://docs.python.org/3/library/asyncio.html)

Optional Azure (bonus):

- [Use Azure Event Hubs from Kafka applications](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-for-kafka-ecosystem-overview)
- [Azure Container Registry](https://learn.microsoft.com/en-us/azure/container-registry/)
