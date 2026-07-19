# Technical Report — Real-Time Fraud Detection

> **Outline to fill in.** Target: PDF, **max 8 pages**, split 3 / 3 / 2 as the
> prompt specifies. Every number below is **measured and verified** — cite them
> directly, don't re-derive. Sources are noted in `‹source›` markers; delete
> those markers and all `TODO` / blockquote guidance before exporting.
>
> Rubric weight: Part A 40 · Part B 40 · Performance 10 (plus a separate 10 for
> the live demo, which is a screencast, not part of this document).
> Report-quality deductions up to −10 for missing sections, unclear
> explanations, **no screenshots/evidence**, or broken links.

**Author:** TODO
**Repository:** TODO — public GitHub URL
**Submitted:** 2026-07-19

---

## Part A — Streaming Pipeline & Feature Store (3 pages)

### A1. System architecture

> Lead with the diagram; it orients everything after it. Reuse the ASCII diagram
> from `docs/architecture.md` §1, or redraw it cleanly for the PDF.

- [ ] Diagram: simulator → Kafka → feature-processor → Redis → API ‹`docs/architecture.md` §1›
- [ ] The core design decision: **feature computation is decoupled from serving**,
      connected only through Redis. State the trade explicitly — the API's cost
      collapses to one `GET` + one model call, the processor can lag or restart
      without affecting availability, and the price paid is feature *staleness*
      (immaterial for 24h rolling aggregates). ‹`architecture.md` §1›
- [ ] Component table: what each of the five services owns. ‹`architecture.md` §2›

### A2. Topic & partition design

> Worth 5 rubric points for "consumer reads the stream end-to-end (all
> partitions)" plus 3 for keying + late/at-least-once handling. Be concrete.

- [ ] Topic `transactions`, **3 partitions** (`KAFKA_NUM_PARTITIONS=3`).
- [ ] Messages **keyed by `customer_id`** (`transaction_simulator.py:88`, with
      `key_serializer` at line 52) → the default partitioner hashes the key, so
      all of a customer's events land on one partition.
- [ ] **Why that matters:** window state is per-customer and in-memory, so
      correctness requires every event for a customer to reach the same consumer.
      Key-based partitioning guarantees it, and makes partition count the
      parallelism ceiling (3 partitions → up to 3 processor instances, rebalance
      migrates whole customers). ‹`architecture.md` §3›
- [ ] Consumer group `feature-processor`, `auto_offset_reset="earliest"` so a
      cold consumer replays history rather than starting blind.
- [ ] **Evidence that all 3 partitions are consumed** — TODO, see gap list below.

### A3. Windowing approach

- [ ] **Half-open window:** `(at_time - window_seconds) < event_time <= at_time`.
      Explain *why*: adjacent windows partition time cleanly, so a boundary event
      belongs to exactly one window — never both, never neither.
- [ ] `windowed_stats()` is a **pure function** of `(events, start, end)` — no
      I/O, no state — so aggregation is unit-testable in isolation and reusable
      outside the consumer loop.
- [ ] Event time vs. processing time: windows are evaluated on the **event
      timestamp**, not arrival time.
- [ ] Features written per event: `transaction_count`, `avg_amount`, plus
      `last_amount` / `max_amount` carried as extra signal at zero extra read
      cost (same JSON value).
- [ ] **Late / at-least-once handling** (rubric requires this documented):
      - Kafka auto-commit gives **at-least-once** delivery — a redelivered event
        is re-appended and double-counted, since there is no dedup on
        `transaction_id`.
      - Out-of-order events evaluate at *their own* timestamp, so a late event
        overwrites fresher values.
      - Restart/rebalance resumes from the committed offset with an **empty**
        in-memory window → temporary under-counting until it refills.
      - State the mitigations you'd apply (dedup set on `transaction_id`;
        evaluate at `max(seen_timestamp)`; checkpoint state or rewind one window
        on startup). ‹`architecture.md` §8›

### A4. Feature store design

- [ ] Key layout `features:{customer_id}` → JSON string.
- [ ] **TTL is atomic with the write** (`SET ... EX`, 48h against a 24h window) —
      so the failure mode where `SET` succeeds and a follow-up `EXPIRE` doesn't,
      leaving a key that never expires, is structurally impossible.
- [ ] **Batch reads are one round-trip:** IDs de-duplicated, single `MGET`,
      results zipped back by MGET's ordering guarantee. Proven with
      `INFO commandstats` → `cmdstat_mget: 1`, **zero `GET`s**, for a 5-txn batch
      over 3 customers.
- [ ] **Connection pooling** per `(host, port, password)`; `decode_responses` set
      on the *pool*, since redis-py ignores per-client kwargs when a pool is given.
- [ ] **Measured retrieval latency** (rubric: 2 pts, target <50 ms) — n=1000,
      shared pool: **p50 0.04 ms · p95 0.10 ms · p99 0.17 ms · max 0.53 ms**.
      ‹`scripts/bench_feature_store.py`›

### A5. Links to implementation

- [ ] `src/streaming/feature_processor.py` — `windowed_stats()` + consumer loop
- [ ] `src/streaming/feature_store.py` — get/set/batch, TTL, pooling
- [ ] `src/config.py` — single settings source
- [ ] Use permalinks (pin to a commit SHA) so links don't rot. **Broken links are
      an explicit deduction.**

---

## Part B — Model Serving & Containerization (3 pages)

### B1. API design & endpoints

- [ ] Endpoint table: `/health`, `/model/info`, `/predict`, `/predict_batch`.
- [ ] **Pydantic validation** (rubric calls this out): `Transaction` schema gives
      HTTP 422 on a missing/ill-typed field for free; `FraudPrediction` as
      `response_model` documents and validates the output. Include the 422 test
      as evidence.
- [ ] **Model loaded once at startup** (rubric: 3 pts) — module-level
      `detector = FraudDetector()` at `main.py:26`, never per-request.
- [ ] `/predict_batch`: IDs de-duplicated → one `MGET` → per-txn merge; returns
      **in request order**. Note the `latency_ms` semantic (elapsed since batch
      start, not per-item).
- [ ] **Merge precedence:** transaction fields beat cached features — the
      transaction describes *this* event, features summarise history.
- [ ] **Graceful degradation:** lookups catch `redis.RedisError` *specifically*
      (not a bare `except`, so real bugs still surface), log a warning, and score
      transaction-only. `/predict` returns **200 with Redis fully down**.
      Connect/socket timeouts pinned at 1s so a *blackholed* host degrades in
      ~1s instead of hanging — verified against `10.255.255.1` → 1.02 s.
- [ ] **Observability:** per-request structured log of `customer_id`,
      `latency_ms`, `fraud_probability`, `degraded`.

### B2. Containerization

- [ ] **Multi-stage build:** builder installs to `/install`; `python:3.11-slim`
      runtime copies it to `/usr/local` plus `src/`, `models/`, `data/`.
- [ ] `.dockerignore` — build context **537 MB → 821 kB** by excluding `.venv/`.
      Concrete, quotable number.
- [ ] **Non-root:** `useradd appuser` + `chown -R` *before* `USER`, so app files
      are owned by the runtime user. `docker inspect` → `User=appuser`.
- [ ] **Healthcheck without `curl`:** `python -c "import urllib.request..."` —
      `python:3.11-slim` ships no `curl`, so the conventional
      `CMD curl -f .../health` reports unhealthy forever. `--start-period=10s`
      covers model load. `docker inspect` → `Health=healthy`.
- [ ] **Compose:** five services, app services gated on `depends_on:
      service_healthy` for both Kafka and Redis. Processor and simulator set
      `healthcheck: {disable: true}` — they inherit the image's HTTP healthcheck
      but serve no HTTP.
- [ ] Screenshot: `docker compose ps` with all five up, zero `unhealthy`. **TODO**

### B3. Blue-green deployment

- [ ] Strategy, cutover mechanics, health gate, rollback, K8s mapping — summarise
      from `docs/blue_green_design.md`, don't duplicate it wholesale.
- [ ] Health gate runs against the **direct** colour ports `:8001`/`:8002`, not
      the stable `:8080` — gating on `:8080` would only prove the *old* version
      healthy.
- [ ] `set -euo pipefail` + gate-before-`sed` ⇒ a failed build yields a failed
      deploy, never an outage. Rollback is the *same command* (state detected by
      grepping the config).
- [ ] Nginx graceful reload: master keeps the listening socket, old workers drain
      in-flight requests, new workers take new ones.
- [ ] **Measured evidence:** blue **17,809** / green **42,330** requests across a
      timed mid-load cutover; **1 error in 60,000** (0.002%, a keep-alive close
      race — state this honestly rather than claiming a clean zero); new-connection
      probe **0/140** non-200; p50 1.14 / p95 3.28 / p99 4.78 ms @ 720 rps.
- [ ] **Screenshots of the swap** — rubric explicitly asks for these. **TODO**
- [ ] Worth including: the **`sed -i` inode bug** found in the provided script.
      It replaced the file's inode while compose bind-mounts `nginx.conf` as a
      single file (single-file mounts bind the *inode*), so the container kept
      reading the orphaned original — the script printed success and reloaded
      stale config while traffic never moved. Three runs "passed" on that false
      signal. **The methodological point:** `errors: 0` is not evidence of a
      cutover; only per-colour request counts distinguish a real switch from one
      that fired after the load finished.

### B4. Links to implementation

- [ ] `src/api/main.py`, `Dockerfile`, `docker-compose.yml`,
      `deployment/switch_traffic.sh`. Permalinked.

---

## Performance (2 pages)

### P1. Method

- [ ] Provided harness `tests/test_performance.py`, fixed seed 789 (rubric: 2 pts
      for using the provided harness + fixed-seed dataset).
- [ ] `--n 5000` rather than the default 1000, and say why: at ~1.1k rps, 1000
      requests is under a second of sampling, so p99 is noise and throughput is
      dominated by startup.
- [ ] **Cache hits verified, not assumed:** the simulator creates
      `CUST0000`–`CUST0199` and the harness draws `CUST{0..199}` — same ID space,
      so all 5000 requests exercise the real lookup → merge → score path rather
      than a miss shortcut.

### P2. Results

- [ ] Table ‹`docs/perf_7.1_before.json`›:

  | Metric     | Result         |
  | ---------- | -------------- |
  | Requests   | 5000, 0 errors |
  | Throughput | 1156.7 req/s   |
  | p50        | 0.77 ms        |
  | p95        | 1.57 ms        |
  | p99        | 2.42 ms        |
  | max        | 6.98 ms        |

- [ ] Against the 100 ms requirement: **~60× headroom** on p95.
- [ ] Optional contrast: 6.2 measured **through nginx** on an idle backend (p50
      1.14 / p95 3.28 @ 720 rps) vs. this **direct** run with the pipeline live
      (p50 0.77 / p95 1.57 @ 1157 rps) — the delta is the proxy hop, not a
      regression. Naming that distinction shows you know what you measured.

### P3. Bottleneck analysis

> This is the strongest section — it's a measurement that overturned a
> hypothesis. Present it that way: prediction → measurement → conclusion.

- [ ] **Hypothesis:** sklearn inference dominates.
- [ ] **Per-stage profile** (`scripts/profile_predict.py`, n=2000, p50 ms):

  | Stage            | p50       |
  | ---------------- | --------- |
  | input validation | 0.0051    |
  | Redis `GET`      | 0.054     |
  | merge            | 0.0007    |
  | model inference  | 0.0758    |
  | logging          | 0.022     |
  | **total**        | **0.158** |

- [ ] **Hypothesis falsified.** Inference *is* the largest application term, but
      the entire application is only **20%** of a 0.77 ms request. Driving it to
      zero would buy ~10%.
- [ ] **Isolating the rest:** benchmarking `/health` (which does no work) through
      the same client gives a **0.234 ms transport floor**. So
      `0.763 − 0.234 − 0.158 ≈ **0.371 ms — 48% of every request** — is FastAPI's
      per-request model machinery. Inbound validation is negligible (0.005 ms),
      so the cost is on the way **out**: `response_model` re-validates the
      already-constructed `FraudPrediction` against the very model that built it.
- [ ] **Conclusion: the system is framework-bound, not compute-bound.** Redis is
      nowhere near the constraint (p95 0.10 ms).

### P4. Optimization tried

> Rubric: 4 pts for "identifies a bottleneck **and one optimization tried**."
> See the gap list — this currently needs an applied before/after measurement.

- [ ] Change: `response_model=None` on `/predict`, removing the redundant
      response-validation pass.
- [ ] **Before:** p50 0.77 / p95 1.57 ms ‹`docs/perf_7.1_before.json`›
- [ ] **After:** TODO — re-run the harness with the change applied.
- [ ] **Decision and rationale:** whether kept or reverted, state it. The
      trade is ~0.2 ms against losing `/predict`'s OpenAPI schema; at 60×
      headroom on the latency requirement, the schema is arguably worth more.
      A quantified, deliberately declined optimization is a legitimate result —
      but only if the measurement was actually taken.

---

## Appendix — defects found in the provided infrastructure

> Optional, but it's differentiating material and costs half a page. Each was
> found by integration testing, not by reading.

- [ ] **Kafka healthcheck never passed.** Probe called `kafka-topics.sh` bare,
      but `apache/kafka:3.8.0` keeps scripts in `/opt/kafka/bin`, off the probe
      shell's `PATH` → broker reported `unhealthy` forever while serving fine.
- [ ] **No consumer group could ever form.**
      `offsets.topic.replication.factor` defaults to 3 on a single-broker
      cluster, so `__consumer_offsets` could never be created and
      `FIND_COORDINATOR` timed out. Silent failure: producer wrote 9600 messages
      successfully while the consumer blocked forever on an empty assignment.
      Fix needs `docker compose down -v` — the broker persists the bad value in
      its metadata.
- [ ] **`sed -i` inode bug** in `switch_traffic.sh` (see B3).
- [ ] Closing note: all three were **silent** failures that "looked healthy."
      The lesson — containers being up proved nothing; only end-to-end evidence
      (a key written by one service and read by another) did.
