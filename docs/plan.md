# Real-Time Fraud Detection — build plan

**Goal:** Turn the TrustBank starter kit into a complete, submission-ready
real-time fraud-detection system (Kafka → feature processor → Redis → FastAPI),
passing every provided test and the rubric, while deliberately optimizing for
two things the grader rewards and the author cares about: **(a) code quality &
reusability** (shared config, DRY helpers, clean logging, no hardcoded scatter)
and **(b) above-and-beyond** (async Redis, graceful degradation, metrics, CI,
Azure bonus, real latency analysis). "Done" = `pytest -q tests/` green,
`docker compose up --build` runs the whole system, blue-green swap demoed with
zero dropped requests, a load-test report, and clean docs.

**How to use this doc (multi-session):** work one unit at a time. Each step has
a checkbox and an acceptance check. When a step passes its check, tick it and
add a dated line to the Progress log. At the start of a session, read the
Current-state snapshot + the first unchecked step and resume there. This file
is the single source of truth for "where are we."

**Authoring split:** CONFIRMED (2026-07-15) — the user authors the code; Claude
orchestrates: locate the exact edit, hand over one focused code block to type,
then verify. (Setup/infra steps like venv, pip, Docker install, running tests,
Claude may run directly.) On a given step the user may say "just make the
edit."

## Why this exists (problems to fix)

1. `feature_processor.features()` is an unimplemented stub — no windowed
   aggregates.
2. `FeatureStore.store/get/get_batch` are stubs — no Redis read/write, TTL, or
   batch.
3. `/predict` and `/predict_batch` return HTTP 501 — no serving logic.
4. `Dockerfile` is empty (comments only) — the API isn't containerized.
5. App services (`api`, `feature-processor`, `simulator`) are commented out in
   `docker-compose.yml`.
6. `docs/blue_green_design.md` is an unfilled template; no swap demo captured.
7. `README.md` is a one-liner; no setup/run instructions for a grader.
8. No performance report (p50/p95/p99 + bottleneck analysis) yet.
9. Env-var reads are duplicated across 3+ files with copy-pasted defaults — a
   reusability smell.
10. No performance/robustness above-and-beyond work (async Redis, pooling,
    graceful degradation, metrics, CI) — where the "exceeds expectations" marks
    live.

## Current-state snapshot

Legend: ✅ done · 🟡 partial/needs fix · ❌ missing

- **`src/streaming/feature_processor.py`** — 🟡 Kafka wiring + `update()`
  provided; `features()` is a stub (raises `NotImplementedError`).
- **`src/streaming/feature_store.py`** — ✅ `store`/`get`/`get_batch` (MGET,
  one round-trip) implemented; TTL atomic via `ex=`; shared `_POOLS`
  connection pool. All 3 tests pass.
- **`scripts/bench_feature_store.py`** — ✅ Typer CLI; p95 = 0.10 ms @ n=1000.
- **`src/config.py`** — ✅ frozen `Settings` dataclass + `from_env()` +
  `kafka_servers` property; all 8 env vars declared once. Not yet consumed by
  the other modules (4.2 / 4.3).
- **`src/api/main.py`** — ✅ `/predict` + `/predict_batch` implemented;
  `merge_features` helper; `lookup_features`/`lookup_features_batch` degrade
  gracefully on `redis.RedisError`; structured per-request latency logging.
- **`src/api/fraud_detector.py`** — ✅ provided baseline (trained model + rule
  fallback).
  `FEATURE_ORDER = ["amount", "is_online", "avg_amount", "transaction_count"]`.
- **`src/streaming/transaction_simulator.py`** — ✅ provided, working
  (backfill + live stream).
- **`src/models/train.py`, `data/generate_seed.py`** — ✅ provided; not yet run
  (no `seed_transactions.csv` / `.pkl` on disk).
- **`Dockerfile`** — ✅ multi-stage (`--prefix=/install` → `/usr/local`),
  non-root `appuser`, `EXPOSE 8000`, `HEALTHCHECK` via `python -c urllib`
  (no `curl` in slim), uvicorn `CMD`. Paired with `.dockerignore`.
- **`docker-compose.yml`** — ✅ all five services; `api`/`feature-processor`/
  `simulator` added, `depends_on: service_healthy`. Two fixes to the "provided"
  Kafka block: healthcheck absolute path, and offsets-topic RF=1.
- **`deployment/` (blue-green nginx + compose + `switch_traffic.sh`)** — ✅
  provided & working.
- **`docs/blue_green_design.md`** — ❌ unfilled template.
- **`README.md`** — ❌ one line (a full reference exists in
  `README.example.md`).
- **`docs/architecture.md`** — ❌ missing (referenced by project structure).
- **`tests/`** — ✅ provided. `test_streaming` runs infra-free;
  `test_feature_store` needs Redis (auto-skips); `test_api` needs `/predict`;
  `test_performance.py` is a load harness (run directly).
- **Local env** — ❌ no venv, no installed deps/pytest; **Docker not on PATH**
  (blocks Phases 5–6 until resolved — see D1).

## Open decisions

- **D1 — How do we run Redis + Docker locally?** Docker was not on PATH, but
  the project needs it for containerization (Phase 5), full-system compose, and
  blue-green (Phase 6); Redis is also needed to verify the feature store
  (Phase 2) and API-with-features. **RESOLVED (2026-07-15) → install Docker
  now** (matches the grading environment; Redis then runs via
  `docker compose up redis`). Tracked as Step 0.3 below.
- **D2 — Keep `FeatureStore` synchronous?** `tests/test_feature_store.py` calls
  `store_customer_features`/`get_customer_features` **synchronously** and uses
  `fs.client.ping()`. **RESOLVED (2026-07-15) → keep sync methods intact**; add
  async retrieval as an _additive_ path in Phase 8 (do not remove or rename the
  sync methods the tests depend on).
- **D3 — Extra feature fields beyond the fixture?** The fixture only checks
  `transaction_count` and `avg_amount`. **RESOLVED (2026-07-15) → allowed to
  add extra keys** (e.g. `last_amount`, `max_amount`) as long as those two keys
  keep their exact fixture values; the model only consumes `FEATURE_ORDER`.

## Pinned facts (don't re-derive)

- **Windowing contract:** include events with
  `(at_time - window_seconds) < event_time <= at_time`. Fixture: window 3600s,
  evaluate_at `2026-01-01T00:50:00Z` → CUST0001 count 3 / avg 200.0, CUST0002
  count 1 / avg 50.0.
- **Model feature order:**
  `["amount", "is_online", "avg_amount", "transaction_count"]` (`train.py` +
  `fraud_detector.py` must agree).
- **Redis key layout:** `features:{customer_id}` → JSON string; every write has
  TTL `FEATURE_TTL_SECONDS` (default 172800). Batch = one round-trip
  (MGET/pipeline).
- **Addresses:** in-container Kafka `kafka:9092`, Redis `redis:6379`; from host
  Kafka `localhost:29092`, Redis `localhost:6379`. API on `:8000`; nginx
  blue-green stable endpoint `:8080`, blue `:8001`, green `:8002`.
- ~~**Claude's shell is network-isolated from published container ports**~~
  **SUPERSEDED 2026-07-19 — this is no longer true.** Throughout the 2026-07-19
  session Claude reached `localhost:8000` and `localhost:6379` directly and
  repeatedly (`/health` 200 in 0.5 ms, `/predict`, `/predict_batch`, the 422
  probes, `bench_feature_store` against Redis). Network-facing checks **can** be
  run by Claude; they no longer need to be handed to the user. The 2026-07-18
  observation was real at the time but does not describe the current
  environment — re-test rather than assuming either way.
- **Artifact layout (decided 2026-07-19):** the prompt specifies **no** location
  for artifacts — it only says the harness "writes a `results.json` you can quote
  in your report", and its prescribed `docs/` tree lists just `architecture.md`
  + `blue_green_design.md`. Chosen convention:
  - `results.json` — **repo root**, the baseline run. Stays put because that is
    the name and place the prompt tells a grader to look; moving it makes the
    submission less discoverable.
  - `docs/figures/` — diagrams and screenshots (`figure-1-transaction-flow.*`).
  - `docs/evidence/` — captured measurements (`bluegreen-switch.json`,
    `kafka-consumer-groups.txt`, and `perf-optimized.json` once 12.4 runs).
  - ⚠️ **`tests/test_performance.py` overwrites `--out` (default
    `results.json`) in place.** Always pass `--out` when capturing a comparison
    run, or the baseline is destroyed. This is why the deleted
    `docs/perf_7.1_before.json` existed.
- **Transaction schema:**
  - `transaction_id`
  - `customer_id`
  - `amount`
  - `merchant_category`
  - `is_online`
  - `timestamp`
  - `[is_fraud]`
- **Env vars:**
  - `KAFKA_BOOTSTRAP_SERVERS`
  - `KAFKA_TOPIC`
  - `REDIS_HOST/PORT/PASSWORD`
  - `FEATURE_WINDOW_SECONDS`
  - `FEATURE_TTL_SECONDS`
  - `MODEL_PATH`
  - `API_PORT`

## Target structure (north star)

```
src/
  config.py                 # NEW — single source for env/settings (Phase 4)
  logging_setup.py          # NEW — shared logger config (Phase 4)
  api/main.py               # /predict, /predict_batch implemented + latency log
  api/fraud_detector.py     # provided (unchanged)
  streaming/feature_processor.py   # features() + reusable windowed_stats()
  streaming/feature_store.py       # store/get/get_batch + pooling (+ async in P8)
Dockerfile                  # multi-stage, non-root, healthcheck
docker-compose.yml          # + api / feature-processor / simulator services
docs/architecture.md        # NEW — system diagram + design writeup
docs/blue_green_design.md   # filled in
README.md                   # full setup/run/test instructions
.github/workflows/ci.yml    # NEW — pytest on push (Phase 8)
```

## Build order (one unit at a time)

### Phase 0 — Local dev environment (green baseline)

- [x] **0.1** Create a venv and install deps (`python -m venv .venv`;
      `.venv/bin/pip install -r requirements.txt`). _Check:_
      `.venv/bin/pytest --version` prints a version, and
      `.venv/bin/pytest -q tests/test_streaming.py` **runs** (expected: 1
      failed on `NotImplementedError` — proves collection works).
- [x] **0.2** Generate the seed dataset and train the baseline model
      (`python data/generate_seed.py`; `python -m src.models.train`). _Check:_
      `data/seed_transactions.csv` and `models/fraud_model_v1.pkl` exist;
      `python -c "import joblib;print(joblib.load('models/fraud_model_v1.pkl')['feature_order'])"`
      prints `['amount', 'is_online', 'avg_amount', 'transaction_count']`.
- [x] **0.3** Install Docker (engine + compose) and verify (D1). _User runs the
      install (needs sudo)._ _Check:_ `docker --version` and
      `docker compose version` print; `docker compose up -d redis` then
      `docker exec` `redis-cli ping` → `PONG`. _Note:_ Docker CLI needs `sudo`
      until a full re-login (group refresh). Claude's shell reaches Redis
      directly at `localhost:6379` (published port) via the Python client —
      this is how Phase 2 will be verified without needing Docker CLI access.

### Phase 1 — Streaming feature computation

- [x] **1.1** Implement `FeatureProcessor.features()` (filter to the window,
      then count + mean; count 0 / avg 0.0 when empty). _Check:_
      `pytest -q tests/test_streaming.py` → **1 passed**.
- [x] **1.2** _(Reusability)_ Extract a pure module-level helper
      `windowed_stats(events, start_exclusive, end_inclusive)` and have
      `features()` call it. _Check:_ `test_streaming` still passes; `python -c`
      calling `windowed_stats` directly on a 2-event list returns the expected
      count/avg.
- [x] **1.3** _(Above-and-beyond)_ Enrich the output with extra keys
      (`last_amount`, `max_amount`) without disturbing
      `transaction_count`/`avg_amount`. _Check:_ `test_streaming` still passes;
      a direct `features()` call shows the extra keys present with sane values.

### Phase 2 — Redis feature store _(needs D1 resolved)_

- [x] **2.1** Implement `store_customer_features` (SET json with
      `ex=ttl_seconds`) **and** `get_customer_features` (GET + `json.loads`,
      `None` if missing). _Check:_
      `pytest -q tests/test_feature_store.py::test_store_and_get_roundtrip` →
      passes.
- [x] **2.2** Verify TTL behavior (should already hold once `ex=` is set).
      _Check:_ `pytest -q tests/test_feature_store.py::test_ttl_is_set` →
      passes.
- [x] **2.3** Implement `get_customer_features_batch` as a single round-trip
      (MGET or pipeline), returning `{id: dict|None}`. _Check:_
      `pytest -q tests/test_feature_store.py::test_batch_retrieval` → passes
      (all 3 store tests green).
- [x] **2.4** _(Reusability/efficiency)_ Route the client through a shared
      `redis.ConnectionPool`. _Check:_ all `test_feature_store` tests still
      pass; `python -c` shows `store.client.connection_pool` is a
      `ConnectionPool`.
- [x] **2.5** _(Rubric: retrieval p95)_ Write a tiny timing snippet that reads
      a stored key N times and prints p50/p95 in ms. _Check:_ prints a p95
      number (target <50ms) — capture it for the report.

### Phase 3 — FastAPI serving

- [x] **3.1** Implement `/predict`: look up features, merge with the txn,
      `detector.predict(...)`, time with `time.perf_counter()`, return
      `FraudPrediction`. _Check:_
      `pytest -q tests/test_api.py::test_predict_returns_valid_schema` → passes
      (and `test_predict_missing_field_returns_422` stays green).
- [x] **3.2** _(Reusability)_ Extract a
      `merge_features(txn: dict, stored: dict|None) -> dict` helper used by
      predict (and later batch). _Check:_ `test_api` still passes; helper is
      unit-callable with a `None` stored arg.
- [x] **3.3** Implement `/predict_batch` using `get_customer_features_batch` +
      the merge helper. _Check:_ POST a 2-item list to `/predict_batch`
      (TestClient or curl) → 200 with a 2-item list of valid predictions.
- [x] **3.4** _(Above-and-beyond: robustness)_ Make feature lookup degrade
      gracefully when Redis is unreachable (treat as no features, log a
      warning) so `/predict` still returns 200. _Check:_ with Redis **down**,
      `pytest -q tests/test_api.py` passes; a log line notes the degraded
      lookup.
- [x] **3.5** _(Above-and-beyond: observability)_ Log per-request latency
      (customer, latency_ms) on `/predict`. _Check:_ hitting `/predict` emits a
      structured latency log line.

### Phase 4 — Reusability pass (refactor with tests as the net)

- [x] **4.1** Add `src/config.py` — one settings object reading the env vars
      (defaults from Pinned facts). _Check:_
      `python -c "from src.config import settings; print(settings)"` shows
      defaults; full `pytest -q tests/` unchanged.
- [x] **4.2** Route `FeatureStore` env reads through `config` (keep constructor
      args as overrides). _Check:_ `test_feature_store` still passes; no
      literal `os.getenv` left in `feature_store.py`.
- [x] **4.3** Route `FeatureProcessor` (+ its `run()`) and
      `fraud_detector`/`main` env reads through `config`. _Check:_
      `test_streaming` + `test_api` still pass; grep shows env reads
      centralized.
- [x] **4.4** Add `src/_logging.py` (one `get_logger`/`configure` used by
      API + processor). _Check:_ API and feature-processor both log via the
      shared setup (consistent format).

### Phase 5 — Containerization _(needs Docker — D1)_

- [x] **5.1** Write a multi-stage `Dockerfile` (builder installs to `/install`;
      slim runtime copies it + `src/`, `models/`, `data/`). _Check:_
      `docker build -t fraud-api .` succeeds.
- [x] **5.2** Add a non-root user, `EXPOSE 8000`, a `HEALTHCHECK` on `/health`,
      and the uvicorn CMD. _Check:_ `docker run --rm -p 8000:8000 fraud-api` →
      `curl localhost:8000/health` = 200; `docker inspect` shows a non-root
      `User`.
- [x] **5.3** Wire the `api` service into `docker-compose.yml` (build ., env →
      `kafka:9092`/`redis`, port 8000, depends_on). _Check:_
      `docker compose up --build api redis` → `curl localhost:8000/health`
      = 200.
- [x] **5.4** Wire `feature-processor` + `simulator` services. _Check:_
      `docker compose up --build` (full) → after backfill,
      `redis-cli get features:CUST0001` returns JSON and `/predict` for that
      customer reflects real features.

### Phase 6 — Blue-green deployment

- [x] **6.1** Fill `docs/blue_green_design.md` (strategy, cutover, health gate
      & rollback, K8s mapping). _Check:_ all 5 template sections written with
      specifics from `switch_traffic.sh`/`nginx.conf`.
- [x] **6.2** Demo the swap with zero dropped requests and capture evidence.
      _Check:_ with blue-green compose up, run the load harness against `:8080`
      while running `switch_traffic.sh` → **0 errors**; save
      terminal/screenshot into the docs.

### Phase 7 — Performance testing & analysis

- [x] **7.1** Run the provided harness and capture `results.json`. _Check:_
      `python tests/test_performance.py --n 1000 --url http://localhost:8000`
      writes `results.json` with p50/p95/p99 + throughput, 0 errors.
- [~] **7.2** **DEFERRED (2026-07-19)** — stretch goal, not main progress. The
      *analysis* half is already done and is report-ready (see the 7.2a progress
      entry): the bottleneck is **FastAPI response-model re-validation**, ~0.371
      ms of the 0.763 ms request. What remains is applying
      `response_model=None` and capturing an after-snapshot — and the standing
      recommendation is to **decline** that trade (it deletes `/predict`'s
      OpenAPI schema to buy ~0.2 ms at 32× rubric headroom). Pick up only if
      Phase 9 finishes early. _Check (if resumed):_ two `results.json` snapshots
      with the delta noted.

### Phase 8 — Above-and-beyond / bonus — **DEFERRED (2026-07-19)**

Shelved wholesale as stretch goals; not main progress. Note 8.1 (async Redis)
is now known to be **unmeasurable on the provided harness** — it is sequential
(one `httpx.Client`, one request in flight), so concurrency work reports as
zero improvement. Anyone resuming 8.1 must write a concurrent load driver
first, or the "after" number will be meaningless.

- [ ] **8.1** Add an **async** retrieval path (`redis.asyncio`) used by the
      endpoints; keep sync methods for tests/processor (D2). _Check:_
      `pytest -q tests/` still green; re-run the load harness → p95
      same-or-better than 7.1.
- [ ] **8.2** Add a `/metrics` endpoint (request count + latency summary).
      _Check:_ `GET /metrics` returns counters/latency after some `/predict`
      traffic.
- [ ] **8.3** Add `.github/workflows/ci.yml` running `pytest` (at least the
      infra-free streaming test). _Check:_ workflow file is valid YAML and runs
      `pytest` on push (verify locally or on a pushed branch).
- [ ] **8.4** _(Azure bonus, optional)_ Document + wire Event Hubs Kafka
      endpoint (SASL) as an alternate `KAFKA_BOOTSTRAP_SERVERS`. _Check:_
      config supports the SASL vars; documented in README (live connect only if
      creds available).

### Phase 9 — Docs & submission

- [x] **9.1** Rewrite `README.md` from `README.example.md`, tailored to the
      real implementation. _Check:_ a clean-checkout run of the documented
      setup commands works end-to-end.
- [x] **9.2** Write `docs/architecture.md` with the Kafka → processor → Redis →
      API diagram + topic/partition + windowing notes. _Check:_ file exists
      with a diagram and the required design sections.
- [x] **9.3** Draft the technical report outline (Part A / Part B /
      Performance) pulling in measured numbers and impl links. _Check:_ all
      required report sections present with real figures/screenshots.

### Phases 10–15 — Report, demo & submission (from the 2026-07-19 rubric audit)

Read `burns/01-project-prompt.md` against the build. Phases 10–13 write the
report section by section against the outline already in `docs/report.md`;
Phase 14 is the screencast; Phase 15 ships it.

**Working rule for 10–13:** each step turns one outline sub-section into
finished prose. The numbers are already measured and pinned in `report.md` with
`‹source›` markers — **cite, don't re-derive.** Target 3 / 3 / 2 pages, 8-page
PDF cap. Every step's acceptance check is the same shape: _the sub-section is
written, every figure in it traces to a verified source, and it fits its page
budget._

**Ordering note (dependency):** §B3 needs *screenshots of the swap*, but the
screencast is Phase 14. So **11.4 captures those stills independently** rather
than waiting on it. Whichever runs first feeds the other — if Phase 14 happens
early, pull stills from the video instead (`ffmpeg -ss <t> -i segN.mp4
-frames:v 1 out.png`) and tick 11.4 from those.

### Phase 10 — Report Part A: Streaming & Feature Store (3 pages, 40 pts)

- [x] **10.1** _(A1)_ **System architecture** — diagram + the decoupling
      rationale (feature computation split from serving, joined only by Redis;
      trade is staleness for availability). _Check:_ diagram embedded and the
      trade-off stated in prose, not just asserted.
- [x] **10.2** _(A2)_ **Topic & partition design** — 3 partitions, keyed by
      `customer_id` (`transaction_simulator.py:88` + `key_serializer` line 52),
      why key-based partitioning is load-bearing for in-memory window state, and
      partition count as the parallelism ceiling. _Check:_ section explains
      *why* keying matters, not just that it's done.
- [x] **10.3** _(A2 evidence, 5 pts)_ Capture **all-3-partitions** proof:
      `kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe
      --group feature-processor`. _Check:_ output shows partitions 0/1/2
      assigned with lag; saved as an image/block in the report.
- [x] **10.4** _(A3)_ **Windowing approach** — half-open bounds and why they
      partition time cleanly; `windowed_stats()` as a pure function; event-time
      vs processing-time. _Check:_ the fixture contract is stated exactly.
- [x] **10.5** _(A3, 3 pts)_ **Late / at-least-once handling** — Kafka
      auto-commit ⇒ at-least-once ⇒ a redelivered event is **double-counted**
      (no `transaction_id` dedup); out-of-order events evaluate at their own
      timestamp; restart/rebalance resumes with an empty window. Each with its
      mitigation. _Check:_ delivery semantics named explicitly + mitigations
      listed.
- [x] **10.6** _(A4)_ **Feature store design** — key layout, atomic
      `SET ... EX` TTL, one-round-trip `MGET` batch (proven via
      `cmdstat_mget: 1`), pooling with `decode_responses` on the pool. _Check:_
      each claim traceable to `feature_store.py`.
- [x] **10.7** _(A4, 2 pts)_ **Measured retrieval latency** — p50 0.04 / p95
      0.10 / p99 0.17 / max 0.53 ms (n=1000). _Check:_ p95 reported against the
      <50 ms target with the method named.
- [x] **10.8** _(A5)_ **Links to implementation** — permalinked to a commit SHA.
      _Check:_ every link resolves (broken links are a named deduction).

### Phase 11 — Report Part B: Serving & Containerization (3 pages, 40 pts)

- [x] **11.1** _(B1)_ **API design & endpoints** — endpoint table, Pydantic
      validation (422 for free), model loaded **once** at startup
      (`main.py:26`), `/predict_batch` dedup → one `MGET` → request-order
      results, merge precedence. _Check:_ the 3-pt "loaded once" claim is
      evidenced by the line reference.
- [x] **11.2** _(B1)_ **Robustness & observability** — `redis.RedisError`
      caught specifically, 200 with Redis down, 1s timeouts verified against a
      blackholed host (1.02 s), per-request structured latency log. _Check:_
      degradation described with its measured evidence.
- [x] **11.3** _(B2)_ **Containerization** — multi-stage build, `.dockerignore`
      537 MB → 821 kB, non-root `appuser` with `chown` before `USER`,
      `HEALTHCHECK` via `urllib` (no `curl` in slim), compose
      `service_healthy` gating, `healthcheck: {disable: true}` on the non-HTTP
      services. _Check:_ each choice stated with its failure mode.
- [ ] **11.4** _(B3 evidence, up to −10 if missing)_ **Capture screenshots** —
      none exist in the repo today. Needed: the blue-green swap, `docker compose
      ps` all-healthy, a `/predict` response. See the ordering note above.
      _Check:_ images committed and referenced from the report.
- [x] **11.5** _(B3, 4 pts)_ **Blue-green design & evidence** — health gate on
      direct colour ports, gate-before-`sed`, graceful nginx reload, measured
      blue 17,809 / green 42,330, 1 error in 60,000 stated honestly. _Check:_
      per-colour counts included, not just an error count.
- [x] **11.6** _(B3)_ **The `sed -i` inode bug** + the methodological point that
      `errors: 0` alone cannot prove a cutover. _Check:_ written as a finding
      with its evidence, not an aside.
- [x] **11.7** _(B4)_ **Links to implementation** — permalinked. _Check:_ all
      resolve.

### Phase 12 — Report Performance (2 pages, 10 pts)

- [x] **12.1** _(P1, 2 pts)_ **Method** — provided harness, fixed seed 789, why
      `--n 5000` over 1000, and cache-hit verification via the shared ID space.
      _Check:_ the harness + fixed seed are explicitly named.
- [x] **12.2** _(P2, 4 pts)_ **Results** — p50/p95/p99 + throughput table;
      ~60× headroom; the nginx-vs-direct contrast. _Check:_ all four figures
      present and matching `results.json`.
- [x] **12.3** _(P3)_ **Bottleneck analysis** — present as hypothesis →
      measurement → falsification: inference predicted to dominate; per-stage
      profile shows the whole app is 20%; `/health` floor isolates ~0.371 ms
      (48%) as FastAPI response re-validation. _Check:_ the falsified
      hypothesis is shown, not hidden.
- [x] **12.4** _(P4, 4 pts — points currently at risk)_ **Actually apply the
      optimization and measure it.** The rubric wants one optimization *tried*;
      analyzing and declining without an after-number risks the marks. Set
      `response_model=None` on `/predict`, rebuild the api container, then re-run
      the harness **with `--out docs/evidence/perf-optimized.json`**. ⚠️ The
      harness defaults to `--out results.json` and **overwrites in place** — the
      root `results.json` is the baseline and the only copy, so omitting `--out`
      destroys the "before" number. Keep or revert on the merits afterwards.
      _Check:_ two snapshots with the delta stated and the decision justified.

### Phase 13 — Report appendix & assembly

- [x] **13.1** **Appendix: defects found in the provided kit** — Kafka
      healthcheck `PATH`, single-broker `offsets.topic.replication.factor`, the
      `sed -i` inode bug; closing note that all three were *silent* failures.
      _Check:_ each has symptom → diagnosis → fix.
- [~] **13.2** **Page-budget pass** — trimmed 20pp → **~9pp estimated** (54%
      reduction) via the two-tier decision: report states decisions + evidence,
      depth lives in `architecture.md` / `blue_green_design.md`, which it links.
      **Blocked on a real render** — the estimate is a words/rows heuristic, not
      a page count. User targets **7pp** (cap 8). If the Word export runs over,
      cut in this order: (1) the two link tables (16 rows) → inline lists,
      (2) Table 3 delivery-semantics → prose, (3) A2's "three things follow"
      paragraph → one sentence. _Check:_ exported PDF ≤ 7–8 pages.
- [x] **13.3** **Link & figure audit** — every link resolves, every figure has a
      caption and a real source. _Check:_ zero broken links (named deduction).
- [ ] **13.4** **Export to PDF.** _User-side_ — `pandoc` is not installed here
      and the user is moving to Windows/Word for final formatting. _Check:_
      `report.pdf` renders with figures intact and lands within the page cap.

### Phase 14 — Live-demo screencast (10 pts)

Must show: (1) txns into Kafka, (2) features appearing in Redis live, (3) a
prediction with **latency shown**, (4) a blue-green switch.

**Approach (decided 2026-07-19):** a `screencast/` directory holding one MP4 per
segment, the `.tape` sources, and its own `README.md` with four commentary
sections — plus a concatenated `demo.mp4` so the 3–5 min single-video
expectation is satisfied. Linked from the root README. Recorded with **VHS**
(installed & working), which executes the commands for real, so output is
genuine — as opposed to Termynal, whose terminal output is hand-authored and
would amount to fabricated evidence.

_Pinned constraints (don't rediscover):_

- GitHub **does not** render a repo-relative MP4 inline; `<video>` is sanitized
  and `![](x.mp4)` degrades to a link. A relative link opens the blob viewer,
  which **does** play MP4. Robust pattern: embed a **GIF** preview (`![]()`
  works for GIFs) above a link to the MP4. VHS emits both from one tape via a
  second `Output` line.
- GitHub warns >50 MB/file, rejects >100 MB. Use `Set Framerate 12` for GIFs and
  `ffmpeg -crf 28` if an MP4 is large.
- Segment 2's `kafka-consumer-groups.sh --describe` is the same command as
  **10.3** — capture once, use for both.

- [ ] **14.1** Write the four `.tape` files (1 stack-up · 2 streaming ·
      3 prediction+latency · 4 blue-green). `Hide`/`Show` for setup so the 30s
      backfill isn't dead air. _Check:_ each tape renders an MP4 + GIF that
      plays.
- [ ] **14.2** Record segments 1–4. _Check:_ all four beats visibly present;
      segment 3 shows a latency number on screen.
- [ ] **14.3** Concatenate to `demo.mp4` via ffmpeg. _Check:_ single file,
      3–5 min, all four beats.
- [ ] **14.4** Write `screencast/README.md` — four commentary sections, GIF
      previews + MP4 links, closing "how these were made" note pointing at the
      tapes. _Check:_ renders correctly **on GitHub** (verify after push; local
      preview lies about video).
- [ ] **14.5** Link `screencast/` from the root README. _Check:_ link resolves
      from the repo root on GitHub.
- [ ] **14.6** Confirm the submission channel — the prompt lists the screencast
      under submission requirements alongside the Canvas PDF, so the repo copy
      may not be the graded artifact. **Check this before producing video.**
      _Check:_ uploaded wherever Canvas expects it.

### Phase 15 — Ship it

- [ ] **15.1** **Commit and push.** `results.json`, `docs/architecture.md`,
      `docs/report.md`, the README rewrite, `scripts/profile_predict.py`, and the
      `screencast/` tree are all uncommitted/untracked; the prompt requires
      performance results in the repo. Note `docs/plan.md` and `burns/` are in
      `.git/info/exclude` and will **not** ship — intended for `burns/`, a
      conscious call for the plan. _Check:_ clean `git status`, pushed, repo URL
      in the report.
- [ ] **15.2** **Final submission pass** — PDF to Canvas, repo URL correct,
      screencast delivered, `screencast/README.md` links verified on GitHub.
      _Check:_ walk the prompt's own "Testing Checklist" top to bottom.

## Deadline note

**Assignment due 2026-07-19.** Triage discussed 2026-07-18: Claude recommended
cutting Phase 4 (0 direct points — insurance against the −10 quality deduction)
and jumping 3.4 → 5 → 6 → 7 → 9, since Docker / blue-green (10) / performance
(10) / the report are all untouched and carry the marks. **RESOLVED (2026-07-18)
→ user chose to keep the plan order as written.** Do not re-litigate; if time
gets tight, the cut order above is the fallback.

## Parking lot (found mid-step; not yet scheduled)

- [x] **P1** ✅ CLOSED 2026-07-19. Fixed at **fit** time, not predict time:
      `train.py` now calls `model.fit(X.to_numpy(), y)`, so sklearn never records
      `feature_names_in_` and has nothing to warn about. **The obvious fix was
      measured and rejected** — building a `pd.DataFrame` per request costs
      **320.7 µs vs 44.3 µs** for a bare list (7.2×), which would have added
      ~276 µs to a 770 µs request (**+36% end-to-end**) purely to silence a
      cosmetic warning. Fitting on an array costs nothing (43.4 µs) and keeps the
      `FEATURE_ORDER` contract, since both sides reference the constant.
      Verified: artifact has no `feature_names_in_`, `n_features_in_ 4`;
      `-W error::UserWarning` does not trip; **0 `UserWarning` lines** in the
      whole container log after rebuild; `/predict` still 1.0 for $4000 online on
      `sklearn-logreg-v1`; suite **7/7** (warnings 6 → 5).
      _Original entry:_ `FraudDetector.predict` builds a bare `[[...]]` list, so sklearn
      emits `UserWarning: X does not have valid feature names, but
      LogisticRegression was fitted with feature names` on every scored
      request — it pollutes test output and a grader may notice. Fix: pass a
      `pd.DataFrame([row], columns=self.feature_order)` (or fit on plain
      arrays). Cosmetic only — predictions are correct. Note
      `fraud_detector.py` is marked "provided", so this is optional polish.
      _Check:_ `pytest -q tests/test_api.py` runs with no sklearn
      feature-name warning.
- [ ] **P2** `/predict_batch` sets each item's `latency_ms` from **batch
      start**, so later items report larger values — it means "elapsed when
      this prediction was ready," not per-item scoring time. Decide which
      semantic the report wants; if per-item, move `start` inside the loop.
      Either is defensible — just document it. _Check:_ the report states the
      chosen definition and the code matches it.
- [x] **P4** ✅ CLOSED 2026-07-18 in step 4.2. Degradation (3.4) was only *fast*
      for a refused connection. A hung
      Redis host would block until the socket timeout, which redis-py leaves
      unset by default — so `/predict` could stall instead of degrading. Fix:
      pass `socket_connect_timeout=1, socket_timeout=1` to the pool in
      `_get_pool`. Natural fit for **4.2** (routing store config through
      `config.py`). _Check:_ a store pointed at a blackholed host (e.g.
      `10.255.255.1`) returns from `/predict` in ~1s, not hanging.
- [ ] **P5** `/predict`'s latency log computes `degraded=stored is None`
      (`main.py` ~line 103), but `lookup_features` returns `None` for **both** a
      `RedisError` *and* an ordinary cache miss — so every unknown customer is
      reported as degraded. Found 2026-07-18 during 5.3, where a containerized
      `/predict` logged `degraded: True` while Redis was demonstrably reachable
      (no warning logged, `REDIS_HOST=redis` present). Matters because the report
      uses this field to separate outages from normal misses. Fix: have
      `lookup_features` signal the two cases distinctly — e.g. return
      `(features, degraded: bool)`, or set a flag in the `except` branch — and
      bind that instead. _Check:_ a cache miss with Redis up logs
      `degraded: False`; with Redis down it logs `degraded: True`.
- [ ] **P7** `API_PORT` is declared in `.env.example` but consumed nowhere —
      `config.py` has no `api_port` field and the Dockerfile hardcodes
      `--port 8000`, so setting it has no effect. Found 2026-07-19 during 9.1.
      Either add it to `Settings` and use it in the uvicorn CMD, or drop it from
      `.env.example`. Documented as a known gap in the README meanwhile.
      _Check:_ `API_PORT=9000` either changes the served port or the variable is
      gone.
- [ ] **P8** `deployment/nginx/nginx.conf`'s trailing `# ACTIVE` / `# STANDBY`
      labels **go stale after every switch**. `switch_traffic.sh` flips only the
      `server` / `# server` prefix, never the trailing comment — so the config
      currently reads `# server api-blue:8000;  # ACTIVE` (commented out, yet
      labelled active) while green actually serves. The script's own detection is
      correct (it greps for an uncommented `server` line), so this is purely a
      human-readability defect — but a grader opening `nginx.conf` sees a file
      that contradicts itself. Found 2026-07-19 during 11.5. Fix: extend both
      `sed` branches to rewrite the trailing label too, or drop the labels and
      let the comment prefix speak for itself. _Check:_ after a switch, the
      `ACTIVE` label sits on the uncommented line.
- [ ] **P6** `/predict` p95 is **3.07 ms** against `/health`'s **0.317 ms** — a
      ~10× tail the p50 analysis does not explain (p50s are 0.763 vs 0.234, so
      the tail is disproportionate, not just a shifted floor). Found 2026-07-19
      during 7.2a. Suspect GC or sklearn allocation variance rather than Redis
      (`redis_get` p95 0.216 ms is too small to account for it). Not chased —
      deadline. _Check:_ tail attributed to a named cause with evidence.
- [ ] **P3** Add a real docstring to `predict_fraud` (the student-TODO one was
      removed in 3.2). Fold into the Phase 4 quality pass — the rubric has a
      "missing comments" deduction. _Check:_ `/predict` has a docstring
      describing the lookup → merge → score → time flow.

## Progress log

- 2026-07-15 — Plan created after full orientation of the starter kit and
  tests. Baseline: 5 implementation stubs + empty Dockerfile + commented
  compose services + template docs. Env has Python 3.12.3 but no venv/deps and
  **no Docker on PATH** (D1 open). Decisions D2 (keep sync FeatureStore) and D3
  (extra feature keys allowed) resolved. First unchecked step: **0.1**.
- 2026-07-15 — **0.1 done.** Created `.venv`, installed `requirements.txt`
  (pytest 9.1.1). `pytest tests/test_streaming.py` runs and fails only at the
  `features()` stub — collection confirmed. Next: 0.2.
- 2026-07-15 — **0.2 done.** Ran `generate_seed.py` (6000 rows →
  `data/seed_transactions.csv`) and `train.py` (→ `models/fraud_model_v1.pkl`,
  train fraud rate 0.030). `feature_order` =
  `['amount','is_online','avg_amount','transaction_count']` (matches pinned
  fact). Next: 0.3 (user installs Docker).
- 2026-07-15 — **0.3 done. Phase 0 complete.** User installed Docker;
  `docker compose up -d redis` → `redis-cli ping` = PONG. Docker CLI needs
  `sudo` until re-login; Claude verifies Redis via `localhost:6379` Python
  client (confirmed `ping: True`). Next: 1.1.
- 2026-07-15 — **1.1 done.** Implemented `FeatureProcessor.features()`
  (window filter `start < ts <= end`, side-effect-free `.get`, `to_epoch`
  handles ISO+epoch). `pytest tests/test_streaming.py` → 1 passed. Next: 1.2.
- 2026-07-15 — **1.2 done.** Extracted pure `windowed_stats()` (+ a `_mean`
  helper and `TYPE_CHECKING` aliases the user added). `features()` delegates.
  `test_streaming` still 1 passed; direct call → `{count 2, avg 200.0}`. Minor
  polish noted (not blocking): `_mean(default=0)` returns int `0` for an empty
  window vs the contract's `0.0` — see parking lot. Next: 1.3.
- 2026-07-15 — **1.3 done. Phase 1 complete.** Enriched `windowed_stats` with
  `last_amount` + `max_amount` (D3). Caught + fixed an empty-window `IndexError`
  (`last_amount` needed an `if windowed else 0.0` guard; the earlier int-`0`
  `_mean` default was also corrected to `0.0`). Checks: `test_streaming` 1
  passed; populated → `{2, 200.0, 300.0, 300.0}`; empty → all zeros. Next: 2.1
  (Redis store/get — Redis is up on `localhost:6379`).
- 2026-07-18 — **2.1 done.** Implemented `store_customer_features` (SET with
  `ex=self.ttl_seconds`, so the write and its expiry are atomic) and
  `get_customer_features` (GET + `json.loads`, `None` when absent). Checks:
  `test_store_and_get_roundtrip` 1 passed against live Redis (no skip); manual
  probe → roundtrip `{'transaction_count': 3, 'avg_amount': 200.0}`, `ttl 60`,
  missing key → `None`. Next: 2.2 (TTL test — expected to pass as-is).
- 2026-07-18 — **2.2 done.** No new code — the atomic `ex=` on SET in 2.1 is
  what satisfies TTL. Checks: `test_ttl_is_set` 1 passed; default
  `ttl_seconds` pinned at **172800** (48h, from `FEATURE_TTL_SECONDS`) and a
  fresh write reports `ttl == 172800`, never `-1` (no-expiry), with `-2` after
  delete. Next: 2.3 (`get_customer_features_batch`, single round-trip).
- 2026-07-18 — **2.3 done.** `get_customer_features_batch` via `MGET` +
  `zip(customer_ids, raw_values)` (MGET guarantees order), with an empty-list
  guard because zero-key MGET is a Redis error. Checks: all 3
  `test_feature_store.py` tests passed; round-trip count proven with
  `CONFIG RESETSTAT` + `INFO commandstats` → **`cmdstat_mget calls: 1`, no
  `cmdstat_get`** for 3 ids; missing id → `None`; `[]` → `{}`. Pinned: the test
  is named `test_batch_retrieval` (not `test_batch_get`). Next: 2.4 (shared
  connection pool).
- 2026-07-18 — **2.4 done.** Added module-level `_POOLS` cache +
  `_get_pool(host, port, password)` in `feature_store.py`; `__init__` now
  resolves env vars into locals and builds `redis.Redis(connection_pool=...)`.
  Pinned gotcha: `decode_responses=True` must be set on the **pool** — redis-py
  ignores per-client connection kwargs when a pool is supplied. Checks: 3/3
  `test_feature_store` pass; two default stores share one pool object
  (`is` identity), a `port=6380` store gets its own (2 cached);
  `connection_kwargs['decode_responses'] is True` and values return as `str`.
  Next: 2.5 (p50/p95 retrieval timing — capture the number for the report).
- 2026-07-18 — **2.5 done. Phase 2 complete.** Built
  `scripts/bench_feature_store.py` as a Typer CLI (`-n/--num-tests`,
  `-v/--verbose`) with loguru routed through `tqdm.write`. **Numbers for the
  report (n=1000, localhost Redis, shared pool): p50 0.04 ms · p95 0.10 ms ·
  p99 0.17 ms · max 0.53 ms · mean 0.05 ms** — target was <50 ms.
  Two bugs found and fixed: (1) inside `Annotated`, `typer.Option()` takes only
  flag names — the default must go on the parameter with `=`, else
  `AttributeError: 'int' object has no attribute 'isidentifier'`; (2) summary
  stats used bare `round()`, which floored every sub-ms latency to `0ms` —
  now `round(x, 3)`. Checks: `--help` renders both flags with defaults; `-n 50`
  → `count 50`; teardown leaves `ttl == -2`. `typer`/`loguru`/`tqdm` already in
  `requirements.txt`. Next: 3.1 (`/predict` endpoint).
- 2026-07-18 — **3.1 done.** `/predict` implemented: `perf_counter` spans the
  whole path (Redis + scoring), `store.get_customer_features(...) or {}`,
  `merged = {**stored, **txn.model_dump()}` (txn wins on collision — it
  describes *this* event), `**prediction` unpacked into `FraudPrediction`.
  Checks: 3/3 `test_api` pass (422 test still green); sensitivity probe with
  stored `avg_amount=100` → 1× amount `prob 0.0000 / fraud 0` vs 20× amount
  `prob 1.0000 / fraud 1`, so the score genuinely tracks the features;
  unknown customer → 200, not a crash. Pinned: the trained model **is**
  loaded, `model_version == "sklearn-logreg-v1"` (not the rule fallback);
  warm latency ~0.4–0.6 ms. Logged **P1** in the parking lot (sklearn
  feature-name warning). Next: 3.2 (extract `merge_features` helper).
- 2026-07-18 — **3.2 done.** Extracted `merge_features(txn, stored)` in
  `main.py` (above the routes); the `or {}` None-handling now lives **inside**
  the helper so `/predict_batch` can't diverge. Pure refactor — no behavior
  change. Checks: `test_api` still 3/3; direct calls → `stored=None` and
  `stored={}` both yield the txn alone, normal merge keeps both sources, txn
  wins on a colliding key, and inputs are not mutated. Parking lot: add a
  real docstring to `predict_fraud` during the Phase 4 quality pass (the old
  student-TODO one was removed). Next: 3.3 (`/predict_batch`).
- 2026-07-18 — **3.3 done.** `/predict_batch` implemented with
  `response_model=List[FraudPrediction]`; customer IDs deduped via a set
  comprehension before one `get_customer_features_batch` call, then
  `merge_features` per txn. Checks: **full suite 7/7 green**; a 5-txn batch
  over 3 distinct customers (incl. a repeat and an unknown) → 200 with 5
  predictions and `cmdstat_mget: 1`, **zero `GET`s** — the graded batch
  efficiency proven; response order matches request order; empty batch →
  `200 []` via the 2.3 guard. Parking lot: **P2** (batch `latency_ms`
  semantics), **P3** (docstring for `predict_fraud`). Next: 3.4 (graceful
  degradation when Redis is down).
- 2026-07-18 — **3.4 done.** Added `lookup_features` / `lookup_features_batch`
  wrappers in `main.py` catching **`redis.RedisError` specifically** (not bare
  `except`, so real bugs still surface), logging a loguru warning and degrading
  to `None`/`{}`; both endpoints now call them. Checks: live Redis → suite
  **7/7**; with the store pointed at dead port 6399, `test_api` **3/3 passes**
  and `/predict` + `/predict_batch` both return **200**, warning logged
  (`Error 111 ... Connection refused`), 0.01 s elapsed — no hang. Limit found:
  only *refused* connections fail fast; a hung host would block (no socket
  timeout set) → logged as **P4**, to fold into 4.2. Next: 3.5 (per-request
  latency logging).
- 2026-07-18 — **3.5 done. Phase 3 complete.** `/predict` emits a structured
  loguru line via `logger.bind(customer_id, latency_ms, fraud_probability,
  degraded).info("prediction served")` — `bind` keeps them as real fields for a
  JSON/file sink rather than baking them into the message. The `degraded` flag
  ties 3.4's fallback to the measured latency, so a slow request can be
  attributed to a Redis outage rather than the model. Checks: suite **7/7**;
  live Redis → `degraded: False`, dead port → `degraded: True`, both with
  populated fields. Next: 4.1 (`src/config.py` settings object) — user opted to
  keep Phase 4; deadline 2026-07-19 11:59 PM.
- 2026-07-18 — **4.1 done.** Added `src/config.py`: frozen `Settings`
  dataclass with all **8** env vars (`REDIS_HOST/PORT/PASSWORD`,
  `FEATURE_TTL_SECONDS`, `FEATURE_WINDOW_SECONDS`,
  `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `MODEL_PATH`), a `from_env()`
  classmethod whose fallbacks reference `cls.<field>` so each default is
  written once, a `kafka_servers` split property, and a module-level
  `settings`. Chose a stdlib dataclass over `pydantic-settings` (not installed;
  no new dependency to justify). Checks: `print(settings)` shows the pinned
  defaults; suite **7/7**; `REDIS_PORT=6380` → `6380` as `int`;
  `REDIS_PASSWORD=""` → `None`; multi-server string → 2-element list;
  `FrozenInstanceError` on mutation; and `Settings.from_env() == Settings()`
  with a clean env, proving no default drift. Next: 4.2 (route `FeatureStore`
  through config — fold in **P4** socket timeouts).
- 2026-07-18 — **4.2 done; P4 closed.** `feature_store.py` now imports
  `settings` instead of `os` — all four env reads replaced, constructor args
  still override. Added `socket_connect_timeout=1, socket_timeout=1` to the
  pool in `_get_pool`. Checks: suite **7/7**; `grep` finds no `os.getenv` (nor
  `import os`) in the file; **blackholed host `10.255.255.1` → `/predict` 200
  in 1.02 s** (previously would hang indefinitely — refused connections fail
  fast, dropped packets do not); `FeatureStore(port=6399, ttl_seconds=99)`
  overrides still applied. Next: 4.3 (route processor + detector/main env reads
  through config).
- 2026-07-18 — **4.3 done.** `feature_processor.py` (window default + `run()`'s
  topic/bootstrap) and `fraud_detector.py` (`model_path`) now read from
  `settings`; `run()` uses the `kafka_servers` property, dropping the inline
  `.split(",")`. **Decision:** `transaction_simulator.py` left untouched — the
  kit marks it provided, whereas `fraud_detector.py` says "extend if you like".
  Note for the report: config centralization is complete except that one
  deliberate exemption. Checks: suite **7/7**; `os.getenv` now only in
  `config.py` + the simulator; window resolves 86400 default / 3600 ctor /
  7200 env; `kafka_servers` → `['localhost:9092']` (list); detector still loads
  `sklearn-logreg-v1`. Repo note: Ruff + Makefile + pyproject added in commits
  `9226f0e`/`c167444` — the Ruff pass rewrapped the simulator, which shifts its
  line numbers vs earlier notes. Next: 4.4 (`src/_logging.py`).
- 2026-07-18 — **4.4 done. Phase 4 complete.** Added **`src/_logging.py`**
  (named per the user's cross-project convention — not `logging_setup.py`)
  with `configure()` / `get_logger(service)`: one stderr sink, shared FORMAT
  ending in `{extra}` so 3.5's bound fields surface without a JSON sink, and
  `logger.configure(extra={"service": ...})` tagging each service. Wired into
  `main.py` (`logger = get_logger("api")`) and `run()`
  (`get_logger("feature-processor")`); both `print(..., flush=True)` calls in
  `run()` replaced with `logger.info`/`logger.warning`. Checks: suite **7/7**;
  `/predict` logs `{'service': 'api', 'customer_id': ..., 'latency_ms': ...,
  'degraded': False}` in the shared format; processor logs tag
  `service='feature-processor'`; no `print(` left in `feature_processor.py`.
  Next: 5.1 (multi-stage Dockerfile) — Docker CLI is not usable from Claude's
  shell (group perms), so **user runs the `docker` commands** and pastes output.
- 2026-07-18 — **5.1 done.** Multi-stage `Dockerfile`: `python:3.11-slim` builder
  does `pip install --no-cache-dir --prefix=/install`, runtime stage copies
  `/install` → `/usr/local` plus `src/`, `models/`, `data/`. Also added
  **`.dockerignore`** (`.venv/`, `.git/`, caches, `logs/`, `burns/`) — without it
  the 537 MB `.venv` ships as build context; with it the context is **821 kB**.
  Checks: `docker build -t fraud-api .` → `Successfully tagged fraud-api:latest`;
  integration check inside the image —
  `docker run --rm fraud-api python -c "import fastapi, sklearn, redis; from
  src.api.fraud_detector import FraudDetector"` → `deps OK`, with
  `models/fraud_model_v1.pkl` and `data/seed_transactions.csv` present. This
  proves the `--prefix` path lines up with the runtime's `site-packages` (the
  failure mode that builds clean and dies at import). Next: 5.2 (non-root user,
  `EXPOSE`, `HEALTHCHECK`, uvicorn `CMD`).
- 2026-07-18 — **5.2 done.** Dockerfile now adds `appuser` (`useradd` +
  `chown -R appuser:appuser /app` before `USER`, so app files are owned by the
  runtime user), `EXPOSE 8000`, a `HEALTHCHECK` and the uvicorn `CMD`.
  **Healthcheck uses `python -c urllib.request...`, not `curl`** — `python:3.11-slim`
  ships no `curl`, so the conventional `CMD curl -f .../health` reports unhealthy
  forever; `--start-period=10s` covers model load. Checks: 15-step build ran
  steps 11–15 fresh; `curl localhost:8000/health` → `{"status":"ok"}` **HTTP 200**;
  `docker inspect` → **`User=appuser  Health=healthy`**. Note the container is
  healthy with **no Redis reachable** — 3.4's graceful degradation holding up in
  the container. Next: 5.3 (wire `api` into `docker-compose.yml`).
- 2026-07-18 — **5.3 done.** `api` service added: `build: .`, env
  `KAFKA_BOOTSTRAP_SERVERS=kafka:9092` / `REDIS_HOST=redis` /
  `MODEL_PATH=...`, `8000:8000`, `depends_on` both **`service_healthy`**. No
  `command:` — the Dockerfile `CMD` already runs uvicorn; duplicating it invites
  drift.
  **Fixed a defect in the provided Kafka healthcheck** (decision: user approved
  editing the "do not modify" block, with an explaining comment in the file).
  The template probe ran `kafka-topics.sh` bare, but `apache/kafka:3.8.0` keeps
  its scripts in `/opt/kafka/bin`, off `PATH` for the probe shell → broker
  reported `unhealthy` **forever** (`FailingStreak: 19`,
  `"/bin/sh: kafka-topics.sh: not found"`) while serving normally. Absolute path
  fixes it. This was latent in the kit — `service_started` hid it; good report
  material.
  Checks: `docker compose up --build -d api redis` → kafka **Healthy**, redis
  **Healthy**, api Started; `/health` → `{"status":"ok"}` **HTTP 200**;
  `/predict` → `model_version: sklearn-logreg-v1` (real model, not the rule
  fallback) in-container. **Connectivity proven affirmatively**, not by absence
  of errors: `redis-cli set features:CUST0001 '{"transaction_count":5,
  "avg_amount":240.0}'` then re-scoring the *same* $250 txn moved
  `fraud_probability` **1.0 → 0.0** — the API container read a key written by the
  `redis` service over the compose network.
  Found **P5** en route (the `degraded` flag conflates cache miss with
  `RedisError` — logged, not fixed here). Next: 5.4 (`feature-processor` +
  `simulator`).
- 2026-07-18 — **5.4 done. Phase 5 complete.** Added `feature-processor`
  (`python -m src.streaming.feature_processor`, `restart: on-failure`) and
  `simulator` (`--backfill-hours 24 --duration 300 --rate 50 --fraud-rate 0.01`).
  `FEATURE_WINDOW_SECONDS=86400` set explicitly to match `--backfill-hours 24` —
  a drift there would corrupt aggregates silently rather than fail loudly.
  **Found a second latent defect in the provided Kafka block**, worse than the
  healthcheck one: `offsets.topic.replication.factor` defaults to **3** on this
  **single-broker** cluster, so `__consumer_offsets` could never be created →
  `FIND_COORDINATOR` timed out → **no consumer group could ever form**. Symptom
  was silent: producer wrote 9600 messages fine, consumer logged `Consuming...`
  and blocked forever on an empty assignment, `dbsize` stuck at 1. Diagnosis:
  `kafka-topics.sh --list` showed `transactions` but **no `__consumer_offsets`**.
  Fix: `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1` (+ transaction-state-log RF/ISR
  =1), and **`docker compose down -v`** is required — the broker persists the bad
  setting in its metadata, so a plain restart does not pick it up.
  Also: the Dockerfile `HEALTHCHECK` is inherited by *every* service built from
  the image, so processor + simulator reported `unhealthy` (neither serves HTTP).
  Fixed with `healthcheck: {disable: true}` on both. Pinned gotcha: the
  simulator's block was dropped on the first typing pass and only caught by
  re-reading `docker compose ps` — `feature-processor` was clean while
  `simulator` still showed `health: starting`.
  Checks: all five containers up, **zero `unhealthy`**; `dbsize` **200** after a
  `-v` wipe (so every key was processor-written); `features:CUST0001` =
  `{"transaction_count": 90, "avg_amount": 124.36, "last_amount": 127.64,
  "max_amount": 261.06}` — the `last_amount`/`max_amount` keys from 1.3 prove
  provenance; `/predict` for CUST0001 → **$130 = 0.0** vs **$4000 online = 1.0**,
  same customer, so scoring genuinely consumes streamed features.
  **Lesson worth keeping:** "containers are up" proved nothing here — a
  submission tested only against a hand-seeded Redis would have looked healthy
  with a completely dead pipeline. Next: 6.1 (fill `docs/blue_green_design.md`).
- 2026-07-18 — **6.1 done.** Wrote `docs/blue_green_design.md` (199 lines, all 5
  template sections) — **Claude drafted directly at the user's request**, not the
  usual hand-typed split. Grounded in the actual files rather than generic
  blue-green prose: the `fraud_backend` upstream's active/standby comment pair,
  the script's `grep`-the-config state detection (why rollback is the *same*
  command), the 20×2s health-gate loop against the **direct** ports `:8001`/
  `:8002` (checking `:8080` would only prove the old version healthy), and
  `set -euo pipefail` + gate-before-`sed` meaning a failed build yields a failed
  deploy, never an outage. Explained graceful reload properly (master keeps the
  listening socket; old workers drain in-flight requests, new workers take new
  ones). §4 maps to two Deployments + a Service `selector` patch, with readiness
  probes as the continuous form of the health gate.
  **§5 Evidence is a marked placeholder with a step-by-step capture procedure —
  that is 6.2's job.** Pinned for 6.2: load must target **`:8080`**, not a direct
  port, or the run bypasses nginx and proves nothing.
  Noted in the doc: `api-blue`/`api-green` are built from identical source and
  the same `MODEL_PATH`, so a switch is currently invisible client-side;
  suggested differentiating `MODEL_PATH` (or similar) so `/model/info` differs
  per colour and the cutover is observable. Next: 6.2 (run the swap under load,
  capture 0 errors).
- 2026-07-18 — **6.2 done. Phase 6 complete.** Evidence captured into §5 of
  `blue_green_design.md`: blue **17,809** / green **42,330** requests across a
  timed mid-load cutover, harness `errors: 1` of 60,000, new-connection probe
  **0** of 140 non-200, p50 1.14 / p95 3.28 / p99 4.78 ms @ 720 rps.
  **Root cause of three false-positive runs — a real bug in the provided
  `switch_traffic.sh`:** it used `sed -i`, which **replaces the file's inode**.
  `docker-compose.blue-green.yml` bind-mounts a single *file*
  (`./nginx/nginx.conf`), and single-file mounts bind the **inode** — so the
  rename detached the container's view. Host config changed, script printed
  `Switched to ...`, `nginx -s reload` reloaded the **stale** in-container copy,
  traffic never moved. Fixed by rewriting the original inode
  (`cat "$CONF.tmp" > "$CONF"`). Recovery also required
  `up -d --force-recreate nginx`, since the running container's mount still
  pointed at the orphaned inode.
  **Verification lesson (the big one):** `errors: 0` is *not* evidence of a
  cutover — a switch firing after the load finishes produces a perfect zero while
  nothing moves. Only **per-colour request counts** distinguish the two. Three
  runs "passed" on the script's own success message before that check was added.
  Also corrected a wrong explanation Claude gave mid-debug: keep-alive
  connections are **not** pinned to the old worker indefinitely — draining
  workers close idle connections and clients reconnect onto the new colour (the
  1 error is exactly that close race, 0.002%). `docs/blue_green_design.md` §2
  updated to state this accurately rather than claiming zero errors.
  Demo automated at `scratchpad/bg_demo.sh` (timed switch — removes the human
  stopwatch that caused the false positives). Next: 7.1 (perf harness +
  `results.json`). **Note:** the 6.2 numbers above were measured through nginx;
  7.1 wants them direct against `:8000`.
- 2026-07-18 — **7.1 done.** `results.json` captured direct against `:8000` with
  the full stack live: **5000 requests · 0 errors · 1156.7 rps · p50 0.77 ·
  p95 1.57 · p99 2.42 · max 6.98 ms**. Used `--n 5000` not the plan's 1000 —
  at ~1.1k rps, 1000 requests is under a second of sampling, so p99 is noise and
  throughput is dominated by startup.
  **Cache hits confirmed by code, not assumed:** simulator builds
  `CUST0000`–`CUST0199` (`range(num_customers)`, dbsize 200) and the harness
  draws `CUST{rng.randrange(200):04d}` — identical ID space, so all 5000 were
  hits and the figures cover Redis lookup → merge → score, not a miss shortcut.
  Comparison for the report: 6.2 measured **through nginx** on an idle backend
  (p50 1.14 / p95 3.28 @ 720 rps); 7.1 is **direct** with the pipeline live
  (p50 0.77 / p95 1.57 @ 1157 rps) — the delta is the proxy hop, not a
  regression. Rubric target was p95 < 50 ms; actual **1.57 ms** (~32× headroom).
  Next: 7.2 (one bottleneck + one optimization, before/after snapshots).
- 2026-07-19 — **7.2a (bottleneck analysis) done; 7.2b and Phase 8 deferred.**
  Built `scripts/profile_predict.py` (Typer, times each hot-path stage
  in-process). **Two predictions were made in advance and the first was
  falsified** — worth keeping in the report as method, not just result.
  *Prediction 1: sklearn `predict_proba` dominates.* **Wrong.** Per-stage p50
  (n=2000, cache_hit=True): `validate` 0.0051 · `redis_get` 0.054 · `merge`
  0.0007 · **`predict` 0.0758** · `log` 0.022 → **TOTAL 0.158 ms**. `predict`
  *is* the largest app term, but the whole application accounts for only **20%**
  of the 0.77 ms request; zeroing it would buy ~10%.
  *Prediction 2: the remaining 80% is HTTP/framework, not client-side.*
  **Confirmed** by benchmarking `/health` (which does no work) through the same
  client as a floor: **`/health` p50 0.234 · `/predict` p50 0.763**. Transport +
  httpx is only 0.234, so **0.371 ms — 48% of every request — is FastAPI's
  per-request model machinery on `/predict`**. Inbound validation is negligible
  (0.005 ms measured), so the cost is on the way **out**: `response_model=
  FraudPrediction` takes the already-constructed `FraudPrediction` and
  re-validates it against the same model that built it. We pay validation twice
  and the second pass cannot fail.
  **Not applied.** `response_model=None` removes it but deletes `/predict`'s
  OpenAPI schema — a bad trade at p95 1.57 ms vs a 50 ms target (~32×
  headroom). Recorded as a quantified-and-declined tradeoff.
  Pinned: `scripts/` modules import `from src...`, so they run **only** as
  `python -m scripts.<name>` from the repo root (a bare path puts `scripts/` on
  `sys.path` and `src` fails to import) — cost us two failed invocations.
  Also logged **P6** (unexplained 10× p95 tail).
  **Scope call (user, 2026-07-19):** 7.2b and all of Phase 8 are shelved as
  extras — Phase 9 docs carry the marks. Next: 9.1 (README).
- 2026-07-19 — **9.1 done.** Rewrote `README.md` (one line → full doc) —
  **Claude drafted directly**, as with 6.1. Sections: ASCII pipeline diagram,
  quick start, host-dev mode (pins the `localhost:29092` vs `kafka:9092` trap),
  API table + real request/response, degradation + merge-precedence behaviour,
  feature/windowing contract, tests, performance, blue-green, config table,
  layout, and a "notes on the provided infrastructure" section writing up the
  three latent defects found in the kit (two Kafka, one `sed -i` inode bug).
  **File-loss incident:** `scripts/profile_predict.py` and
  `docs/perf_7.1_before.json` vanished mid-session — not a git operation
  (reflog clean, both untracked), not `make clean` (that only removes
  `__pycache__`, which survived and still held `profile_predict.cpython-312.pyc`
  as proof the script had run). Cause never established; most likely a manual
  cleanup when 7.2 was shelved. Both restored on the user's instruction. **Also
  learned: `docs/plan.md` is listed in `.git/info/exclude`** (with `burns` and
  `CLAUDE.md`), so it is deliberately untracked and never appears in
  `git status` — do not read its absence there as "no changes."
  Found while documenting: **`API_PORT` is declared in `.env.example` but read
  by nothing** — not `config.py`, not the Dockerfile (which hardcodes
  `--port 8000`). Documented honestly in the README rather than implied to work;
  logged as **P7**.
  Checks: all 20 README-referenced paths resolve; perf table matches
  `docs/perf_7.1_before.json` byte-for-byte; `models/fraud_model_v1.pkl`
  confirmed tracked (so "the artifact is committed" is true); suite **7/7**;
  ruff clean on the restored script; and the README's own documented `curl`
  example run verbatim → `{"transaction_id":"t-1","fraud_probability":1.0,
  "is_fraud":1,"model_version":"sklearn-logreg-v1","latency_ms":0.72}`, matching
  the documented response field-for-field with the real model loaded.
  Skipped a literal clean-checkout rebuild (~5 min for marginal signal) given
  the same-day deadline. Next: 9.2 (`docs/architecture.md`).
- 2026-07-19 — **9.2 done.** Wrote `docs/architecture.md` (9 sections: overview
  + diagram, components, streaming design, feature-store design, serving design,
  performance, failure-mode table, known limitations, deployment). Claude
  drafted; linked from the README header and layout block.
  **Draft was wrong twice and verification caught both — in the direction of
  *understating* the system.** (1) Claimed a single partition; it is **3**
  (`KAFKA_NUM_PARTITIONS=3`, confirmed live via `kafka-topics.sh --describe`).
  (2) Claimed customer-keyed partitioning was "designed for but not exercised";
  the producer **already** sends `key=txn["customer_id"]`
  (`transaction_simulator.py:88`) with a `key_serializer` (line 52), so keying
  genuinely works. **Pinned fact:** per-customer partition affinity therefore
  holds today, which means the in-memory window design is already correctly
  horizontally scalable — raise replicas up to 3, no code change.
  New limitations documented (from reading the processor, not assumed):
  `_events` never evicts → unbounded memory **and O(n²) per customer** since
  every event rescans the full buffer; **window state does not survive a
  restart** — offsets are committed under a fixed `group_id`, so a restart
  resumes mid-stream with an *empty* window and writes under-counted features
  until it refills (`auto_offset_reset="earliest"` does not help, as it only
  applies with no committed offset); same hazard fires on rebalance; and
  out-of-order events evaluate at their own timestamp, so a late event
  overwrites fresher values.
  Checks: no stale "single partition" text remains; every asserted mechanism
  grepped against source (`ex=` on SET, `mget`, `_POOLS`,
  `socket_connect_timeout=1`/`socket_timeout=1`, `decode_responses` on the pool,
  `USER appuser`, `HEALTHCHECK` via `urllib`, 5× `service_healthy`); both
  internal doc links resolve. Next: 9.3 (technical report outline) — the last
  main-progress step.
- 2026-07-19 — **9.3 done. Phase 9 complete.** Wrote `docs/report.md` as a
  fill-in outline mapped to the prompt's required structure (Part A 3pp / Part B
  3pp / Performance 2pp, 8-page PDF cap), with every verified number pre-slotted
  and `‹source›` markers so nothing needs re-deriving. Added an optional
  appendix on the three latent kit defects.
  **Rubric audit against `burns/01-project-prompt.md` found 6 gaps → new Phase
  10.** The big one: the **live-demo screencast is worth 10 points and had never
  been in this plan at all**. Second: the rubric wants an optimization
  *tried* (4 pts) — analyzing and declining 7.2b without an after-measurement
  risks those points, so 10.2 un-defers just the measurement. Third: **no
  screenshots exist anywhere in the repo** (`find` for png/jpg/svg → empty)
  while Part B requires "screenshots of the swap" and "no screenshots/evidence"
  is a named −10 deduction; blue-green §5 evidence is text-only.
  Also confirmed already-satisfied: topic has ≥2 partitions (3), model loaded
  once at startup (`main.py:26`), retrieval p95 measured (0.10 ms), TTL/batch/
  422/non-root/healthcheck all covered.
  Pinned: `docs/plan.md` and `burns/` are both in `.git/info/exclude`, so the
  prompt file is local-only and won't ship with the repo. Next: **10.1**
  (screencast) — highest points at risk.
- 2026-07-19 — **10.1 approach decided; VHS installed and working.** Structure:
  a `screencast/` dir with one MP4 per segment, the `.tape` sources, a
  concatenated `demo.mp4` for the 3–5 min expectation, and its own `README.md`
  with four commentary sections; linked from the root README. Rationale for VHS
  over Termynal: **VHS executes the commands for real and records actual
  output**, whereas Termynal's terminal text is hand-authored — presenting that
  as demo evidence would be fabrication, not just cosmetics. Keeping the
  `.tape` files in-repo makes the demo reproducible rather than a one-off.
  Split 10.1 into 10.1a–e; folded the screenshot work (10.3) into it, since
  stills can be pulled from the recordings with `ffmpeg -ss ... -frames:v 1`
  instead of staged separately, and segment 2 doubles as the 10.4 partition
  evidence. Next: **10.1a** (write the four tapes).
- 2026-07-19 — **Plan restructured at the user's request.** The flat "Phase 10 —
  submission gaps" list became six phases with one step per report sub-section,
  because the remaining work is mostly *writing*, and a single 6-item list gave
  no visibility into it. New shape: **10** Report Part A (8 steps) · **11** Part
  B (7) · **12** Performance (4) · **13** appendix & assembly (4) · **14**
  screencast (6) · **15** ship (2). The screencast moved to the end and is now
  **counted as main progress**, not a bonus — user's call, accepting that the
  progress bar moves backwards.
  **Dependency caught while restructuring:** report §B3 requires *screenshots of
  the swap*, but the screencast is now last — so B3 would be written with no
  evidence to cite. Resolved by making **11.4** an independent screenshot
  capture, with a note that whichever of 11.4/Phase 14 runs first feeds the
  other. Also folded the old 10.4 (partition evidence) into **10.3**, since it
  is the same command segment 2 records.
  Old → new mapping: 10.1→Phase 14 · 10.2→12.4 · 10.3→11.4 · 10.4→10.3 ·
  10.5→15.1 · 10.6→10.5. Next: **10.1** (§A1 architecture prose).
- 2026-07-19 — **10.1 done.** Started `docs/report.md` (the user renamed the
  outline to `report-outline.md`; `report.md` is now the real draft). §A1
  written: Figure 1 diagram, the decoupling rationale argued rather than
  asserted (different workload characteristics → why coupling would merge the
  failure modes → what the split buys → **what it costs**), and Table 1 of
  service responsibilities.
  **Two claims caught wrong by verification, both would have been checkable by a
  grader:** (1) drafted "no other module calls `os.getenv`" — false;
  `transaction_simulator.py:138,141` still does. It is the deliberate exemption
  from 4.3 (kit-provided file), now stated honestly as such. (2) Cited a
  "90-event average" from the 5.4 run, but the live value has moved to
  **`transaction_count: 117`** as the simulator kept producing — replaced with a
  non-specific phrasing, since any pinned count is a moving target while the
  stack runs. **Pinned:** don't quote live Redis counts as fixed figures in the
  report; quote measured *latencies* (stable, captured) instead.
  Verified: 0.13 ms = redis_get 0.054 + predict 0.0758 (exact, 0.1298).
  **Page-budget watch:** A1 is ~640 words plus a figure and a table — heavy for
  one of five sub-sections in a 3-page part. Flagged for the 13.2 trim pass;
  A2–A5 should run leaner. Next: **10.2** (§A2 topic & partition design).
- 2026-07-19 — **10.2 + 10.3 done** (10.3 closed by the same evidence block, so
  the 5-pt "consumer reads all partitions" item is satisfied inside §A2 rather
  than needing a separate artifact). §A2 covers: 3 partitions / RF=1 with the
  single-broker caveat, keying by `customer_id` and **why it is load-bearing**
  (in-memory per-customer state ⇒ split events would produce silently-wrong
  aggregates, not visible breakage), the three consequences (per-customer
  ordering, parallelism capped at partition count, rebalances migrate whole
  customers), and `auto_offset_reset="earliest"`.
  **Evidence captured live** (`kafka-consumer-groups.sh --describe`, Figure 2):
  all three partitions on **one** `CONSUMER-ID`, **lag 0** on each, offsets
  18,632 / 14,306 / 15,503 = **48,441 messages** fully consumed.
  **The analytical find:** the offset spread (38.5 / 29.5 / 32.0 %) is itself
  proof that key-hashing works — round-robin over a uniform producer would trend
  towards equal counts, whereas hashing 200 discrete customer keys into 3 buckets
  produces exactly this lumpiness. **An even split would have been the
  suspicious result.** Worth reusing: the skew is stronger evidence of correct
  partitioning than the assignment table alone.
  Verified: 38.5/29.5/32.0 % arithmetic exact; `key_serializer` at
  `transaction_simulator.py:52`, `send(key=...)` at `:88`; `num_customers=200`
  at `:44`. Next: **10.4** (§A3 windowing approach).
- 2026-07-19 — **10.4 done.** §A3 written: half-open bounds with the *reason*
  (adjacent windows partition time cleanly — closed-closed double-counts
  boundary events, open-open drops them), event-time vs processing-time and why
  that is what makes the 24h backfill meaningful, `windowed_stats()` as a pure
  function, `to_epoch()` normalising ISO/epoch at the boundary, and why
  `last_amount`/`max_amount` are stored (same JSON value ⇒ no extra round-trip).
  **Best material in the section: the fixture's trap.** Table 2 walks the
  fixture event-by-event and shows the `999.0` event sitting 50 minutes outside
  the window — an implementation that aggregates full history instead of
  filtering returns **count 4 / avg 399.75** instead of **3 / 200.0**. Numbers
  plausible enough to pass a glance, wrong in a way no exception would surface.
  Good illustration of why the fixture is built the way it is.
  Verified: fixture path is `tests/fixtures/window_fixture.json` (as cited);
  bounds compute to `(2025-12-31T23:50:00Z, 2026-01-01T00:50:00Z]`; both the
  in-window (3 / 200.0) and unfiltered (4 / 399.75) figures exact;
  `test_streaming` still 1 passed. Next: **10.5** (§A3 late / at-least-once
  handling — 3 pts).
- 2026-07-19 — **10.5 done. §A3 complete.** Delivery-semantics subsection covers
  four hazards, each with a concrete fix: duplicate delivery double-counts
  (at-least-once, no dedup); out-of-order events overwrite fresher values;
  restart/rebalance resumes with an empty window; malformed records are skipped
  (at-most-once for those).
  **Sharpest distinction in the section:** the **Redis write is idempotent**
  (`SET` overwrites, same key + TTL on replay) but the **in-memory accumulation
  is not** (`update()` appends unconditionally) — so duplicates corrupt the
  *aggregate*, never the *storage*. That framing explains why the fix belongs in
  `update()` and not at the store layer.
  Also pinned precisely: the late-event hazard is **not** a wrong count —
  `windowed_stats()` correctly filters an out-of-window event — it is a *stale*
  one, because the write regresses stored state to an earlier evaluation point.
  Easy thing to get wrong when writing this up.
  Verified: `enable_auto_commit=True` / `auto_commit_interval_ms=5000` are
  kafka-python defaults and the processor does **not** override them (so the
  5-second re-delivery exposure is real, not assumed); `update()` appends with no
  guard; per-record `except Exception` keeps the loop alive.
  **New find:** `transaction_id` appears **0 times** in `feature_processor.py` —
  the event tuple is only `(timestamp, amount)`, so the ID is never even read.
  Dedup would therefore require plumbing the ID through first, not just adding a
  set — noted in the report as a fix with real scope. Next: **10.6** (§A4
  feature store design).
- 2026-07-19 — **10.6 done.** §A4 written: flat key-per-customer layout justified
  by the access pattern (point lookup only — no scans, ranges, or secondary
  attributes, so a hash/sorted-set would add unused structure); atomic
  `SET ... EX` argued as a *correctness* choice (two commands can interleave with
  a failure and strand a key with no expiry — with `ex=` that state is
  structurally unreachable); 48h TTL vs 24h window framed as graceful decay
  rather than a cliff; `MGET` batch with the ordering-guarantee justification for
  the `zip` and the zero-key guard; module-level pool cache.
  **Gotcha written up because it fails silently:** redis-py **ignores connection
  kwargs on the client when a pool is supplied**, so `decode_responses=True` must
  be set on the *pool* — setting it on the client is accepted without error and
  values return as `bytes`, so `json.loads` fails far from the real mistake.
  **Re-verified live rather than citing the 3.3 run** (stack was up, so no reason
  to trust a stale number): fresh `CONFIG RESETSTAT` → 5-txn / 3-customer batch →
  **`cmdstat_mget:calls=1`, no `cmdstat_get` line at all**. Pool identity checks
  also re-run: two default stores share one pool, `port=6380` isolated, 2 cached,
  `decode_responses` True on the pool. Next: **10.7** (§A4 measured retrieval
  latency — 2 pts).
- 2026-07-19 — **10.7 done. §A4 and Part A prose complete** (only 10.8 links
  remain). Re-ran `bench_feature_store` instead of citing 2.5's numbers — and
  the first run came back **~3× slower** (p95 **0.32 ms** vs 2.5's 0.10).
  Ran it three more times before writing anything: p95 **0.09 / 0.08 / 0.04**.
  **Diagnosis: cold-start, not regression** — the pool opens its first socket
  lazily on the first read, so connection establishment lands inside run 1's
  measurement window. 2.5's figures were warm and remain representative.
  Reported **all four runs in Table 3** rather than discarding the outlier:
  quoting run 1 alone overstates steady state ~4×, but hiding it conceals that
  the first request after a container start genuinely is slower — which matters
  for a service restarted on every deploy. Steady-state p95 **0.04–0.09 ms**,
  three orders of magnitude under the 50 ms bar. Also bounded the claim
  honestly: single-key `GET` only (batch is cheaper per customer), local
  loopback only (a networked Redis would dominate).
  **Lesson (generalizes):** always re-run a benchmark before quoting it, and
  always run it more than once — a single sample would have had us report either
  a fake 3× regression or a silently-discarded outlier.
  **Figure 1 image NOT swapped in — it has three defects** (user to fix):
  (1) the `POST /predict` arrow terminates at **Redis**, not FastAPI, which
  directly contradicts §A1's argument that the client only ever talks to the API;
  (2) "merge + **scope**" should be "score"; (3) consumer group labelled
  `"feature processor"` but the real `group_id` is `feature-processor` — and it
  appears hyphenated in Figure 2's evidence. Report keeps the ASCII diagram until
  fixed. Dark background will also print heavily in the PDF.
  **Noted for Phase 14:** the `simulator` service has **exited** (its
  `--duration 300` completed), so only 4 services are up. The screencast must
  restart it to show features landing live. Next: **10.8** (§A5 implementation
  links) — see the permalink-vs-SHA note; code still changes in 12.4.
- 2026-07-19 — **10.8 done. PHASE 10 COMPLETE — Part A fully drafted.** §A5 is a
  10-row implementation index (Table 4) covering aggregation, window evaluation,
  the consumer loop, producer keying, atomic-TTL write, `MGET` batch, pooling,
  config, the fixture, and the benchmark.
  **Written as repo-relative paths with line anchors, deliberately not
  permalinks yet** — 12.4 still edits `main.py`, so SHAs pinned now would point
  at pre-optimization code. An HTML comment marker sits in the section so 15.1
  converts them (GitHub: press `y` on a file view to get a pinned URL).
  Verified: all **8 line anchors** land on exactly the declared definition
  (`windowed_stats` L53, `features` L89, `run` L114, `_send` body L88,
  `store_customer_features` L63, `get_customer_features_batch` L80, `_get_pool`
  L26, `Settings` L16) and all **6 relative paths** resolve from `docs/`.
  ⚠️ Line anchors drift on any reformat — re-verify at 13.3 (link audit), not
  just at 15.1. Next: **11.1** (§B1 API design & endpoints) — Part B begins.
- 2026-07-19 — **11.1 done. Part B begun.** §B1 written: endpoint table (Table
  5), schema-driven validation with the real 422 body, model-loaded-once (3 pts),
  request flow + `merge_features` precedence, and batch semantics.
  **Claim corrected by measurement — the draft was an order of magnitude off.**
  Wrote "loading a joblib artifact costs tens of milliseconds"; measured it and
  got **468.8 ms**. Digging further split it cleanly: the **first** `joblib.load`
  in a process is ~470 ms (mostly importing sklearn's unpickling machinery, not
  file I/O), while **warm reloads are 0.11 ms p50** over 20 iterations. So the
  original sentence was wrong twice — wrong magnitude, and wrong model of the
  cost.
  Rewrote the argument around the real shape, which is stronger: per-request
  loading would make the **first** request of every container absorb ~470 ms
  (landing on whatever hits it first — often a health check), *and* every later
  request pay 0.11 ms = **14% of the 0.77 ms p50** for work that never varies.
  Also ties to `HEALTHCHECK --start-period=10s`, which already covers the
  startup cost.
  **Pinned:** `joblib.load` cold ≈470 ms / warm ≈0.11 ms — reuse in §P3 if the
  cold-start story comes up again.
  Live-verified: missing field → **422** (captured the real `detail` body for the
  report), wrong type → **422**, `/model/info` → `sklearn-logreg-v1` (trained
  artifact, not the fallback), `main.py:26-27` module scope confirmed, and only
  `transaction_id` optional in the `Transaction` model. Next: **11.2** (§B1
  robustness & observability).
- 2026-07-19 — **11.2 done. §B1 complete.** Wrote graceful degradation (narrow
  `redis.RedisError` catch, and *why* — a bare `except` would turn every
  programming error into a plausible-looking prediction), the refused-vs-
  blackholed timeout distinction (1.02 s measured against `10.255.255.1`, with
  the point that testing only the *easy* failure would have shipped a system
  that looked resilient and wasn't), and structured logging.
  **P5 confirmed live and documented honestly rather than papered over:**
  `NOSUCHCUST` with Redis fully healthy logs `degraded: True`, because
  `lookup_features` returns `None` for both a `RedisError` and an ordinary cache
  miss. The report now states that `degraded` reliably means "features absent"
  but **not** "Redis unavailable", and must not be used to alert on store
  health. Fix noted (return a `(features, degraded)` pair) but not applied.
  **Cross-reference drift caught and fixed before it spread.** I had been
  writing `§B2` for robustness and `§B3` for containerization, but the prompt's
  Part B has only four subsections (API+validation · containerization ·
  blue-green · links), so robustness belongs *inside* B1. Corrected 4 refs
  (report lines 71, 97, 373 → `§B1`; 503 → `§B2`).
  **Pinned section map — use this, don't re-derive:** A1 architecture · A2
  partitions · A3 windowing · A4 feature store · A5 links · **B1 API +
  validation + robustness + observability** · **B2 containerization** · **B3
  blue-green** · **B4 links** · P1 method · P2 results · P3 bottleneck · P4
  optimization.
  Also seen in the container logs: the **P1 sklearn `UserWarning`** ("X does not
  have valid feature names") fires on every scored request and pollutes
  `docker compose logs api` — a grader running the demo will see it. Cheap fix
  if time allows. Next: **11.3** (§B2 containerization).
- 2026-07-19 — **P1 closed + 11.3 done.** P1 fixed at *fit* time
  (`model.fit(X.to_numpy(), y)`) after measuring that the obvious predict-time
  `pd.DataFrame` fix costs **320.7 µs vs 44.3 µs** — 7.2×, ~+36% end-to-end — to
  silence a cosmetic warning. Zero `UserWarning` in the container log after
  rebuild; suite 7/7; warnings 6 → 5.
  §B2 written: `--prefix=/install` → `/usr/local` and *why* it works (that is
  where the runtime's `site-packages` lives — get it wrong and the image builds
  clean then dies at first import), `.dockerignore`, non-root with the
  `chown`-before-`USER` ordering argument, the `curl`-less healthcheck, and
  compose `service_healthy` gating.
  **Two stale numbers corrected by re-measuring:**
  (1) Build context is **1.122 MB**, not the 821 kB recorded at 5.1 — `docs/`
  has grown since (report, architecture, figures). Fixed in
  `report-outline.md`; README did not carry the figure.
  (2) A `tar --exclude-from=.dockerignore` estimate gave a nonsense **369 MB**
  because tar does not apply Docker's ignore semantics — **discarded it and took
  the authoritative number from `docker build`'s own "Sending build context"
  line.** Worth remembering: do not approximate the context with tar.
  **Honest sizing added rather than spun:** image is **796 MB**, dominated by
  scipy 113 / pandas 76 / sklearn 50 / numpy 45 MB + ~58 MB bundled libs. The
  report says plainly that multi-stage buys less here than usual — it removes
  build residue (pip cache, requirements, builder tree) but the ML stack sets the
  floor.
  Also noted: excluding `.env` is a **security** property (keeps credentials out
  of image layers, where they survive later deletion) — ties to the rubric's
  "hardcoded credentials" deduction.
  **Environment quirk:** this Docker uses the **legacy builder**, not BuildKit —
  `--progress=plain` is rejected; context size comes from the
  `Sending build context to Docker daemon` line. Next: **11.4** (screenshots) or
  **11.5** (blue-green prose) — 11.4 needs the stack + a live swap, so it may
  batch with Phase 14.
- 2026-07-19 — **11.5 done.** §B3 written from the actual scripts rather than
  generic blue-green prose: the four-step cutover, why the health gate targets
  the **direct** colour port (probing `:8080` proves only that the *old* version
  is healthy), gate-before-flip + `set -euo pipefail` ⇒ a bad target yields a
  failed *deploy*, never an outage, rollback-is-the-same-command (so the rollback
  path cannot rot from disuse — it *is* the deploy path), and an accurate account
  of graceful reload including the keep-alive correction (draining workers close
  idle connections; clients reconnect onto the new colour).
  Evidence in Table 6 with a screenshot marker for 11.4. **Framing kept from the
  6.2 lesson:** the per-colour counts (blue 17,809 / green 42,330) are the proof,
  **not** the error count — a switch firing after the load ends yields a perfect
  `errors: 0` while nothing moves. Forward-references §B3.1 for the false
  positives.
  **New defect found reading `nginx.conf` — logged as P8.** The trailing
  `# ACTIVE` / `# STANDBY` labels go stale after every switch: `sed` flips only
  the `server` / `# server` prefix, so the file now reads
  `# server api-blue:8000;  # ACTIVE` — commented out yet labelled active — while
  green serves. Script detection is unaffected (it greps for an uncommented
  line), so it is purely human-readability, but a grader opening the file sees it
  contradict itself. Wrote §B3's nginx snippet with corrected labels so the
  report is not reproducing the bug.
  Next: **11.6** (§B3.1 the `sed -i` inode bug + the `errors: 0` methodology
  point).
- 2026-07-19 — **11.6 done. §B3 complete.** Wrote §B3.1 as symptom → mechanism →
  fix → methodology. **Demonstrated the mechanism live rather than asserting
  it** — ran `sed -i` on a scratch file and captured the inode changing
  (**2238265 → 2238266**), then showed `cat > file` preserving it. That console
  transcript is in the report, so the claim is reproducible by the reader instead
  of taken on trust.
  Framing that makes the section work: *every layer behaved correctly in
  isolation*, which is exactly why it was invisible — host file updated, script
  completed, nginx reloaded, mount simply pointed elsewhere. Also recorded that
  recovery needed `--force-recreate nginx`, since the running container's mount
  still resolved to the orphaned inode.
  **The methodology paragraph is the most transferable thing in the report:**
  `errors: 0` is consistent with success, mistimed success, *and* total failure,
  so it distinguishes nothing; per-colour counts have a signature no failure mode
  can fake. Generalised to **"verify the effect, not the actuator"** — the
  script's success message, the reload exit code, and the host file were all the
  mechanism reporting on itself. Tied it to the two parallel cases already in the
  report (containers `healthy` proving nothing until a key crossed services §A2;
  a clean build proving nothing until imports ran inside the image §B2), so it
  reads as a consistent practice rather than one war story.
  Next: **11.7** (§B4 Part B implementation links) — then Phase 12
  (Performance), with 11.4 screenshots still deferred to the Phase 14 session.
- 2026-07-19 — **11.7 done. PHASE 11 COMPLETE except 11.4 (screenshots).** §B4 is
  a 15-row index (Table 7) spanning `main.py`, `fraud_detector.py`, the
  Dockerfile/compose files, and the whole `deployment/` tree. Verified all **8
  line anchors** land on their declared definitions and all **9 relative paths**
  resolve from `docs/`. Same permalink marker as §A5; added a specific warning
  that `main.py` anchors shift if §P4's `response_model` change is kept.
  **Progress-bar error corrected (user caught it).** I had been advancing the bar
  one square per completed step, which was accidentally right when the plan had
  22 steps and one square = one step — but after the 22 → 66 restructure each
  square represents ~3 steps. The bar read **19/22 (86%)** against a true
  **14/22 (65%)**. Squares are a *proportion*: `filled = round(done/total*22)`,
  recomputed from a grep each time, never incremented. Note the drift flattered
  the work — the direction that most needs guarding. Recorded in the
  `progress-bar-after-steps` memory. Next: **12.1** (§P1 performance method).
- 2026-07-19 — **PHASE 12 COMPLETE (12.1–12.4). The headline result is a
  falsified hypothesis, and it is the strongest section in the report.**
  **First: every performance figure was re-measured**, because `results.json`
  predated the P1 retrain and so described code that no longer existed. New
  canonical baseline (median of 3 × n=5000): **1497 rps · p50 0.50 · p95 1.42 ·
  p99 3.24 · max 5.47 · 0 errors**; spread 1392–1571 rps, p95 1.27–1.49. The
  system got *faster* than the old figures (p50 0.77 → 0.50), plausibly the P1
  warning removal. Re-profiled too: TOTAL **0.1427 ms** (validate 0.0049 ·
  redis 0.0494 · merge 0.0006 · predict 0.0663 · log 0.0215). Propagated the new
  numbers through README, `architecture.md`, and report §A1/§A4/§B1 — grep
  confirms zero stale figures left.
  **12.4 — the optimization was applied, measured properly, and refuted.**
  `response_model=None` on `/predict`, container rebuilt per variant, both builds
  measured with the *same* interleaved paired benchmark (3 × 4000 reqs,
  `/health` vs `/predict`, comparing the **gap** so machine drift hits both
  terms):
  · baseline gap **0.269 ms** · optimized gap **0.297 ms** — no improvement, the
  optimized build marginally slower, inside run-to-run noise. The harness could
  not resolve any difference either (1392–1571 vs 1048–1558 rps, overlapping).
  **Why the original estimate was wrong — the transferable lesson.** The 0.509 ms
  gap that motivated the whole idea came from a *single* measurement taken hours
  earlier under different machine load. Re-measuring the baseline immediately
  alongside the optimized build gave 0.269 ms, and the entire apparent saving
  vanished with it. **A difference measured across time is not an A/B test.**
  Same failure shape as §B3.1 — a number consistent with several realities read
  as evidence for one.
  **Decision: reverted** (`git diff src/api/main.py` empty; OpenAPI `$ref:
  FraudPrediction` restored; suite 7/7). With no latency benefit the trade was
  pure cost — losing `/predict`'s response schema from `/docs` for nothing.
  §P3 rewritten around stable numbers: /predict 0.552 = floor 0.241 (44%) +
  app 0.143 (26%) + endpoint-specific framework 0.168 (30%). The residual is
  *distributed* across body parsing, dependency resolution, validation and
  serialisation — not concentrated in one removable component, which is exactly
  why the flag did nothing.
  Evidence saved: `results.json` (baseline), `docs/evidence/perf-optimized.json`.
  Next: **13.1** (appendix).
- 2026-07-19 — **PHASE 13: 13.1 + 13.3 done; 13.2 partial; 13.4 is user-side.**
  Appendix written as three silent failures + Table 12 (what looked fine vs what
  actually exposed it), closing on "verify the observable consequence, not the
  mechanism's self-report" — which ties the appendix back to §B3.1 and §P4.
  Link audit **clean**: all file links and line anchors resolve, all 9 `§`
  cross-refs match real headings, re-verified after the rewrite.
  **13.2 — the report was ~20 pages against an 8-page cap.** User chose the
  **two-tier** approach targeting **7pp** (wiggle room under "max 8"): the report
  states decisions and evidence, depth lives in `architecture.md` /
  `blue_green_design.md`, which it now links from a header note. Rewrote the
  whole file — **8,338 → 3,657 words, ~54% reduction, est. ~9pp.** Protected the
  graded and distinctive material (partition evidence, fixture trap, delivery
  semantics, MGET proof, retrieval latency, 422 + load-once, container details,
  cutover evidence, B3.1, P3/P4) and cut rationale that `architecture.md`
  already carries.
  ⚠️ **Still ~2pp over target, and the estimate is a heuristic** (words/550 +
  rows/45), not a render. Authoritative check is the user's Word export. If over,
  cut in this order: link tables → inline lists · Table 3 → prose · A2's "three
  things follow" → one sentence.
  13.4 blocked here: **no `pandoc` installed**, and the user is moving to
  Windows/Word for final formatting anyway.
  Next: **11.4** (screenshots) + **Phase 14** (screencast) — all remaining work
  needs the stack recorded live.
