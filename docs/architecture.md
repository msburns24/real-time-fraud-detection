# Architecture

How the system is put together, why it is shaped this way, and where it would
break under load. Setup and usage live in the [README](../README.md); the
deployment strategy has its own document,
[blue_green_design.md](blue_green_design.md).

## 1. System overview

The design separates **feature computation** from **model serving**, connected
only through Redis. That is the central architectural decision, and everything
else follows from it.

```
 ┌───────────────┐  transactions   ┌───────────┐   consumer group    ┌────────────────────┐
 │   simulator   ├────────────────▶│   Kafka   ├────────────────────▶│ feature-processor  │
 │  (producer)   │ key=customer_id │  (broker) │ "feature-processor" │   rolling window   │
 │               │  3 partitions   │           │                     │                    │
 └───────────────┘                 └───────────┘                     └─────────┬──────────┘
                                                                               │
                                                        SET features:{id} EX 48h
                                                                               │
                                                                     ┌─────────▼──────────┐
   client ──── POST /predict ──────────────────────────────────────▶ │       Redis        │
      ▲                                                              │  (feature store)   │
      │                          ┌────────────────────┐   GET/MGET   └─────────┬──────────┘
      └──── FraudPrediction ─────┤    api (FastAPI)   │◀───────────────────────┘
                                 │  merge + score     │
                                 └────────────────────┘
```

**Why split them.** Scoring must be fast and predictable; feature computation is
stateful and bursty. Coupling them would put Kafka consumption in the request
path, so a broker hiccup would become a serving outage and consumer lag would
become client latency. With Redis between them, the API's cost is one `GET` plus
one model call — measured at 0.13 ms of application work — and the processor can
lag, restart, or be redeployed without the API noticing. The trade is
**staleness**: the API serves the last features written, not features as of
right now. For fraud scoring on rolling 24-hour aggregates, seconds of staleness
are immaterial.

## 2. Components

| Component           | Responsibility                                            | Key files                          |
| ------------------- | --------------------------------------------------------- | ---------------------------------- |
| `simulator`         | Produces transactions; replays a 24h backfill on start     | `streaming/transaction_simulator.py` |
| `kafka`             | Durable, replayable transaction log                        | `docker-compose.yml`               |
| `feature-processor` | Consumes the topic, maintains windows, writes to Redis     | `streaming/feature_processor.py`   |
| `redis`             | Feature store — the interface between the two halves       | `streaming/feature_store.py`       |
| `api`               | Joins cached features with the request and scores it       | `api/main.py`, `api/fraud_detector.py` |

Configuration for all of them resolves through one frozen `Settings` dataclass
(`src/config.py`); no other module calls `os.getenv`. Logging goes through
`src/_logging.py`, which tags each service and preserves bound fields as
structured data rather than baking them into the message string.

## 3. Streaming design

### Topic and partitioning

One topic, `transactions`, with **3 partitions** (`KAFKA_NUM_PARTITIONS=3`),
consumed by group `feature-processor`.

Messages are **keyed by `customer_id`** — the producer sets
`key=txn["customer_id"]` with a `key_serializer`, so Kafka's default partitioner
hashes the key and every event for a given customer lands on the same partition.

That keying is what makes the design horizontally scalable, and it is load-
bearing rather than cosmetic. The processor holds per-customer window state in
memory, so correctness requires that all of a customer's events reach the same
consumer instance. Key-based partitioning guarantees it: a customer's events are
totally ordered within one partition, and a partition is assigned to exactly one
consumer in the group. Partition count is therefore the parallelism ceiling —
3 partitions supports up to 3 processor instances, and a rebalance migrates
whole customers rather than splitting one customer's state across consumers.

Only one processor instance runs today, so it is assigned all 3 partitions.
Scaling out is a matter of raising the replica count up to the partition count,
with no code change — though note the restart caveat in §8, which a rebalance
triggers for the migrated partitions.

Consumption uses `auto_offset_reset="earliest"`, so a first-time consumer
replays the entire retained log and rebuilds windows from history rather than
starting blind.

### Windowing

The window is **half-open**: an event is included when

```
(at_time - window_seconds) < event_time <= at_time
```

Exclusive at the start, inclusive at the end. This makes adjacent windows
partition time cleanly — an event on a boundary belongs to exactly one window,
never both or neither — which is what keeps counts stable as the window slides.

`windowed_stats()` is a **pure function** of `(events, start, end)` with no I/O
or state, so the aggregation logic is testable in isolation and reusable outside
the consumer loop. `FeatureProcessor.features()` is a thin wrapper that resolves
the window bounds and delegates.

Each event triggers a recompute over the customer's buffer and a write of:

```json
{"transaction_count": 90, "avg_amount": 124.36, "last_amount": 127.64, "max_amount": 261.06}
```

The model consumes only `avg_amount` and `transaction_count` (plus `amount` and
`is_online` from the request); `last_amount` and `max_amount` are carried as
extra signal for future models at no additional read cost, since they travel in
the same JSON value.

## 4. Feature store design

**Key layout:** `features:{customer_id}` → a JSON string.

**TTL is atomic with the write.** `SET ... EX` sets value and expiry in one
command, so a key can never exist without an expiry — the failure mode where a
`SET` succeeds and a follow-up `EXPIRE` doesn't, leaving a key that never
expires, is structurally impossible here. TTL is 48h against a 24h window, so
features outlive their window and a brief processor outage doesn't cause misses.

**Batch reads are one round-trip.** `get_customer_features_batch` de-duplicates
IDs and issues a single `MGET`, relying on MGET's ordering guarantee to zip
results back to their IDs. A 5-transaction batch spanning 3 customers costs one
Redis command, not five — verified with `INFO commandstats` (`cmdstat_mget: 1`,
zero `GET`s). Empty input short-circuits, because a zero-key `MGET` is an error.

**Connections are pooled** per `(host, port, password)` in a module-level cache,
so every `FeatureStore` in a process shares sockets instead of reconnecting.
`decode_responses=True` is set on the *pool*, not the client — redis-py ignores
per-client connection kwargs when a pool is supplied.

## 5. Serving design

The request path is: validate → look up features → merge → score → time → log.

**Merge precedence: the transaction wins.** Cached features describe the
customer's history; the transaction describes *this* event. On any key collision
the request's value is authoritative.

**Redis failure degrades, it does not propagate.** Lookups catch
`redis.RedisError` specifically — not a bare `except`, so genuine bugs still
surface — log a warning, and return `None`. The detector then scores from the
transaction alone. A total Redis outage costs accuracy, not availability:
`/predict` still returns 200. Socket and connect timeouts are pinned at 1s, which
covers the harder failure: a *refused* connection fails instantly, but a
blackholed host (packets dropped, no RST) would otherwise hang until the OS gave
up. With the timeout, it degrades in ~1s.

**Observability.** Each request logs `customer_id`, `latency_ms`,
`fraud_probability`, and a `degraded` flag as bound structured fields, so a slow
request can be attributed to a feature-store outage rather than the model.

## 6. Performance characteristics

Measured on the full stack, direct to the API (n=5000): **p50 0.77 ms · p95 1.57
ms · p99 2.42 ms · 1157 req/s · 0 errors**, against a 100 ms requirement.

Profiling the hot path in isolation (`python -m scripts.profile_predict`) puts
total application work at **0.158 ms p50** — Redis `GET` 0.054, model inference
0.076, logging 0.022, merge and validation negligible. Benchmarking `/health`
through the same client gives a 0.234 ms transport floor.

The conclusion is that **the system is framework-bound, not compute-bound**:
application logic is ~20% of a request, and ~48% is FastAPI's per-request model
machinery, dominated by `response_model` re-validating an already-constructed
`FraudPrediction`. That pass can be removed with `response_model=None`, at the
cost of `/predict`'s OpenAPI schema — quantified and deliberately declined,
since 1.57 ms against a 100 ms budget is ~60× headroom and the schema is worth
more than 0.2 ms.

Redis is not the bottleneck and is unlikely to become one soon: retrieval
benchmarks at p95 0.10 ms over 1000 reads.

## 7. Failure modes

| Failure                | Behaviour                                                      |
| ---------------------- | -------------------------------------------------------------- |
| Redis down/unreachable | `/predict` returns 200, scored without cached features; warning logged; ~1s worst case via socket timeout |
| Redis blackholed       | Same, bounded by the 1s connect/socket timeout rather than hanging |
| Model artifact missing | `FraudDetector` falls back to a transparent rule-based scorer   |
| Malformed Kafka record | Consumer logs and skips it; the loop stays alive                |
| Processor down         | API serves increasingly stale features until the 48h TTL expires, then scores transaction-only |
| Kafka down             | Processor stalls; API is unaffected (no broker in the request path) |
| Invalid request body   | FastAPI returns 422 from the pydantic schema                    |

## 8. Known limitations

Stated plainly, because they are the honest boundary of this implementation.

**Window state is in-memory and unbounded.** `FeatureProcessor._events` appends
every event to a per-customer list and never evicts. Two consequences: memory
grows without bound over a long run, and because each event recomputes over the
customer's full buffer, per-event cost grows linearly — O(n²) over a customer's
lifetime. It is comfortable at this scale (200 customers, 24h backfill) and
would not be in production. The fix is to drop events older than the window on
each update, which bounds both memory and per-event cost to the window's
contents; a ring buffer or incremental running sums would remove the rescan
entirely.

**Window state does not survive a restart.** Offsets are committed under a fixed
`group_id`, so a restarted processor resumes from its last committed offset —
but with an *empty* in-memory window. It therefore computes features from a
partial window until enough events replay to refill it, and writes those
under-counted values to Redis in the meantime. `auto_offset_reset="earliest"`
does not save this, as it only applies when no committed offset exists.
Addressing it means either checkpointing window state or deliberately rewinding
offsets by one window on startup.

**Out-of-order events evaluate at their own timestamp.** `process_and_store`
computes features as of the *arriving event's* time. A late-arriving event
therefore writes a window evaluated at an earlier point in time, overwriting
fresher values. Per-customer partition ordering (§3) keeps this rare here — one
producer's events for a customer arrive in order — but it becomes a live hazard
with multiple producers, or on producer retry, where a redelivered event can
arrive after newer ones. Evaluating at `max(seen_timestamp)` rather than the
current event's timestamp would make writes monotonic.

**Single processor instance.** The partitioning needed to scale out is in place
and correct (§3), but only one instance is run, so multi-consumer rebalancing is
untested. Note that scaling out interacts with the restart limitation above: a
rebalance hands partitions to a consumer with no in-memory window for those
customers, producing the same temporary under-counting.

**Out-of-order across partitions is bounded but real.** Kafka orders events
within a partition, not across them. Since a customer maps to exactly one
partition, per-customer ordering is guaranteed — which is the property that
matters here — but the topic as a whole has no global order.

**No authentication.** The API is unauthenticated, appropriate for a local
assignment and not for anything else.

## 9. Deployment

Containerized as a multi-stage build: a builder stage installs dependencies to
`/install`, and a `python:3.11-slim` runtime copies them plus `src/`, `models/`,
and `data/`. It runs as a non-root `appuser` and carries a `HEALTHCHECK`
implemented with `python -c urllib.request` rather than `curl`, which the slim
image does not ship.

Compose brings up all five services with `depends_on: service_healthy` gating,
so the API and processor start only once Kafka and Redis genuinely accept
connections.

Zero-downtime releases use blue-green with nginx: two identical stacks, a
health-gated cutover against the direct colour ports, and rollback by re-running
the same switch. Design and measured evidence — 17,809 requests on blue and
42,330 on green across a live cutover, 1 error in 60,000 — are in
[blue_green_design.md](blue_green_design.md).
