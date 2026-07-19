# Real-Time Fraud Detection — Technical Report

**Author:** Matthew Burns \
**Repository:** https://github.com/msburns24/real-time-fraud-detection \
**Date:** July 19, 2026

---

## Part A — Streaming Pipeline & Feature Store

### A1. System architecture

TrustBank's fraud detector is a five-service pipeline. A transaction simulator
publishes events to Kafka; a feature processor consumes them, maintains a
rolling per-customer window, and writes aggregates to Redis; a FastAPI service
reads those aggregates, joins them with the incoming transaction, and returns a
fraud score.

```
 ┌───────────────┐  transactions   ┌───────────┐   consumer group    ┌────────────────────┐
 │   simulator   ├────────────────▶│   Kafka   ├────────────────────▶│ feature-processor  │
 │  (producer)   │ key=customer_id │  (broker) │ "feature-processor" │   rolling window   │
 │               │  3 partitions   │           │                     │                    │
 └───────────────┘                 └───────────┘                     └─────────┬──────────┘
                                                                               │
                                                        SET features:{customer_id} EX 48h
                                                                               │
                                                                     ┌─────────▼──────────┐
   client ──── POST /predict ──────────────────────────────────────▶ │       Redis        │
      ▲                                                              │  (feature store)   │
      │                          ┌────────────────────┐   GET/MGET   └─────────┬──────────┘
      └──── FraudPrediction ─────┤    api (FastAPI)   │◀───────────────────────┘
                                 │  merge + score     │
                                 └────────────────────┘
```

**Figure 1** — Transaction flow. The two halves of the system communicate only
through Redis; there is no synchronous path between the API and Kafka.

#### The central design decision

The architecture's defining choice is that **feature computation is decoupled
from model serving**, joined only by the feature store. Nothing in the request
path touches Kafka.

The two workloads have genuinely different characteristics. Scoring must be
fast and predictable, with latency bounded on every request. Feature
computation is stateful, bursty, and inherently subject to consumer lag.
Coupling them — having `/predict` aggregate a customer's recent history at
request time — would put Kafka consumption inside the request path, and the two
failure modes would merge: a broker hiccup would become a serving outage, and
consumer lag would surface directly as client latency.

Separating them collapses the API's work to a single Redis `GET` plus one model
call, **measured at 0.13 ms combined** (§P3). The processor can lag, crash, or
be redeployed without the API noticing, because the API never asks it for
anything — it reads whatever was last written.

The cost of this design is **feature staleness**. The API serves the features
as of the processor's last write, not as of the current instant. That is a real
trade and worth naming: a system requiring strict read-your-writes consistency
between ingestion and scoring could not be built this way. For fraud scoring
over 24-hour rolling aggregates it is immaterial — a transaction arriving
seconds before the one being scored shifts a hundred-event average
imperceptibly, and the transaction's _own_ attributes, which dominate the
score, are always current because they come from the request itself.

The decoupling also produces the system's most useful resilience property.
Since the feature store is consulted rather than depended upon, its absence
degrades the prediction instead of failing it — a Redis outage costs accuracy,
not availability (§B2).

#### Components

| Service             | Responsibility                                          | Implementation                         |
| ------------------- | ------------------------------------------------------- | -------------------------------------- |
| `simulator`         | Produces transactions; replays 24h of backfill on start | `streaming/transaction_simulator.py`   |
| `kafka`             | Durable, replayable, partitioned transaction log        | `docker-compose.yml`                   |
| `feature-processor` | Consumes the topic, maintains windows, writes to Redis  | `streaming/feature_processor.py`       |
| `redis`             | Feature store — the interface between the two halves    | `streaming/feature_store.py`           |
| `api`               | Joins cached features with the request and scores it    | `api/main.py`, `api/fraud_detector.py` |

**Table 1** — Service responsibilities.

Two cross-cutting concerns are centralised rather than repeated. All
configuration resolves through a single frozen `Settings` dataclass
(`src/config.py`), so every environment variable and its default is declared
exactly once; constructor arguments remain available as explicit overrides,
which is how the tests inject their own values. The one deliberate exemption is
`transaction_simulator.py`, which the starter kit marks as provided and which
retains its own `os.getenv` calls — every module we authored reads through
`config`.

Logging runs through `src/_logging.py`, which applies one format across
services, tags each with its service name, and preserves bound fields as
structured data rather than interpolating them into the message string — so the
per-request latency records of §B2 stay machine-readable.

### A2. Topic and partition design

The system uses a single topic, `transactions`, configured with **3
partitions** and a replication factor of 1. The replication factor reflects the
single-broker development cluster; production would use 3 with
`min.insync.replicas=2`. The partition count is the more interesting number,
because it sets the system's parallelism ceiling.

#### Keying by customer

Every message is **keyed by `customer_id`**. The producer sets
`key=txn["customer_id"]` with a `key_serializer` that UTF-8 encodes it
(`transaction_simulator.py:88` and `:52`), so Kafka's default partitioner
hashes the key and routes all of a given customer's events to the same
partition.

This is not a cosmetic choice — it is what makes the design correct. The
feature processor holds per-customer window state **in memory**, as a map from
customer to their recent events. Correctness therefore depends on every event
for a customer reaching the same consumer instance: if a customer's events were
split across two consumers, each would compute aggregates over half the data
and the two would overwrite each other in Redis, producing counts and averages
that are silently wrong rather than obviously broken.

Key-based partitioning guarantees the required locality. A customer's events
are totally ordered within one partition, and a partition is assigned to
exactly one consumer in a group. The properties that follow are worth stating
explicitly:

- **Per-customer ordering** is guaranteed; global ordering across the topic is
  not, and is not needed.
- **Parallelism is capped at the partition count** — 3 partitions supports up
  to 3 processor instances. A fourth would sit idle.
- **Rebalances migrate whole customers**, never split one customer's state
  across consumers, so scaling out cannot corrupt aggregates (though it does
  reset in-memory windows — see §A3).

Consumption uses `auto_offset_reset="earliest"`, so a consumer with no
committed offset replays the retained log and rebuilds its windows from history
rather than starting blind against an empty state.

#### Evidence: all three partitions consumed end-to-end

```
GROUP             TOPIC         PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID
feature-processor transactions  1          14306           14306           0    kafka-python-3.0.8-0ba113c8…
feature-processor transactions  2          15503           15503           0    kafka-python-3.0.8-0ba113c8…
feature-processor transactions  0          18632           18632           0    kafka-python-3.0.8-0ba113c8…
```

**Figure 2** — `kafka-consumer-groups.sh --describe --group feature-processor`.

Three things are visible here, and each answers a different question.

**All three partitions are assigned, to a single consumer.** The `CONSUMER-ID`
is identical on all three rows — one processor instance holds partitions 0, 1
and 2 — confirming the stream is consumed end-to-end rather than partially.

**Lag is zero on every partition**, with `CURRENT-OFFSET` equal to
`LOG-END-OFFSET`. The consumer is fully caught up across **48,441 messages**,
so the pipeline keeps pace with the producer rather than falling progressively
behind.

**The offsets are unevenly distributed** — 18,632 / 14,306 / 15,503, a spread
of roughly 38% / 30% / 32%. That asymmetry is itself evidence that keying works
as intended. Round-robin partitioning of a uniform producer would drive the
three partitions towards equal counts; hashing 200 discrete customer keys into
3 buckets produces exactly this kind of lumpy distribution, because customers
are assigned whole and their transaction volumes differ. An even split would
have been the suspicious result.

### A3. Windowing approach

Features are rolling aggregates over a time window that slides with each event.
The window is **half-open**, including an event when

```
(at_time − window_seconds) < event_time ≤ at_time
```

exclusive at the start, inclusive at the end. The asymmetry is deliberate:
adjacent windows then partition time cleanly, so an event falling exactly on a
boundary belongs to exactly one window — never to both, and never to neither.
Closed-closed bounds would double-count boundary events across consecutive
windows; open-open bounds would drop them.

Windows are evaluated on **event time** — the `timestamp` carried in the
transaction — rather than processing time. This is what makes the 24-hour
backfill meaningful: replaying a day of history produces the same aggregates it
would have produced had those events arrived live, because the arithmetic
depends only on the timestamps, not on when the consumer happened to see them.

#### Worked example

The grading fixture (`tests/fixtures/window_fixture.json`) uses a 1-hour window
evaluated at `2026-01-01T00:50:00Z`, giving the half-open interval
`(2025-12-31T23:50:00Z, 2026-01-01T00:50:00Z]`.

| Customer | Event time             | Amount | In window?                  |
| -------- | ---------------------- | ------ | --------------------------- |
| CUST0001 | `2025-12-31T23:00:00Z` | 999.0  | ✗ — before the window opens |
| CUST0001 | `2026-01-01T00:00:00Z` | 100.0  | ✓                           |
| CUST0001 | `2026-01-01T00:30:00Z` | 200.0  | ✓                           |
| CUST0001 | `2026-01-01T00:45:00Z` | 300.0  | ✓                           |
| CUST0002 | `2026-01-01T00:40:00Z` | 50.0   | ✓                           |

**Table 2** — Fixture events and window membership.

CUST0001 yields `transaction_count = 3` and `avg_amount = 200.0`; CUST0002
yields `1` and `50.0`.

The `999.0` event is the fixture's trap. It sits 50 minutes outside the window
and is by far the largest amount present, so an implementation that aggregates a
customer's full history instead of filtering to the window returns
`transaction_count = 4` and `avg_amount = 399.75` — plausible-looking numbers
that are wrong in a way no type error or exception would reveal. The fixture is
built so the failure is loud in the assertion rather than silent in the output.

#### Implementation

The aggregation lives in
`windowed_stats(events, start_exclusive, end_inclusive)`, a **pure function** —
no I/O, no instance state, no clock access. Everything it needs arrives as
arguments, so it is unit-testable in isolation and reusable outside the Kafka
consumer entirely. `FeatureProcessor.features()` is a thin wrapper that resolves
the window bounds from the configured `FEATURE_WINDOW_SECONDS` and delegates.

Timestamp handling is normalised at the boundary by `to_epoch()`, which accepts
either an ISO-8601 string or a numeric epoch and returns epoch seconds, so the
comparison logic never branches on input format.

Each consumed event triggers a recompute over the customer's buffer and a write
of four values:

```json
{
  "transaction_count": 117,
  "avg_amount": 124.57,
  "last_amount": 174.63,
  "max_amount": 261.06
}
```

The model consumes only `avg_amount` and `transaction_count` (alongside `amount`
and `is_online` taken from the request itself). `last_amount` and `max_amount`
are computed and stored because they cost nothing extra — they ride in the same
JSON value, so retrieving them adds no round-trip and no additional key — and
they are the obvious next signals for a model reasoning about deviation from a
customer's recent behaviour.

#### Delivery semantics: late and duplicate events

The consumer commits offsets automatically — `enable_auto_commit` defaults to
`True` in kafka-python with a 5-second interval, and the processor does not
override it. The pipeline therefore has **at-least-once** delivery, and the
consequences are worth stating precisely rather than assumed away.

**Duplicate delivery double-counts.** If the processor crashes after handling a
batch but before the next auto-commit, those events are re-delivered on restart.
`update()` appends unconditionally, and there is no dedup on `transaction_id`,
so a redelivered event is counted twice — inflating `transaction_count` and
skewing `avg_amount` toward whichever amounts were replayed. Up to 5 seconds of
events are exposed to this on any unclean shutdown.

The distinction that matters here is between the two kinds of state. The **Redis
write is idempotent**: `SET features:{id}` overwrites, so replaying the same
event produces the same key with the same TTL. The **in-memory accumulation is
not**: the event list grows by one entry each time the event is seen. Duplicates
corrupt the aggregate, not the storage. The fix is a bounded set of recently-seen
`transaction_id`s consulted in `update()` — cheap, since it only needs to span
the commit interval rather than the whole window.

**Out-of-order events overwrite fresher values.** `process_and_store()` computes
features as of the *arriving* event's timestamp. A late event therefore writes a
window evaluated at an earlier point in time, replacing a more current
aggregate. Note that the late event's own contribution is handled correctly —
`windowed_stats()` filters it out if it falls outside the window — so the damage
is not a wrong count but a *stale* one: the write regresses the stored state to
an earlier evaluation point. Per-customer partition ordering (§A2) keeps this
rare with a single producer, but it becomes live with multiple producers or on
producer retry. Evaluating at `max(seen_timestamp)` per customer rather than the
current event's timestamp would make writes monotonic and close it.

**Restart and rebalance resume with an empty window.** Because offsets are
committed under a fixed `group_id`, a restarted processor resumes from its last
committed offset — but its in-memory window is empty. It then writes
under-counted features until enough events replay to refill it.
`auto_offset_reset="earliest"` does not rescue this: that setting applies only
when *no* committed offset exists, which is precisely not the restart case. The
same hazard fires on a consumer-group rebalance, where migrated partitions
arrive at a consumer with no state for those customers. Closing it means either
checkpointing window state or deliberately rewinding offsets by one window on
startup, accepting duplicate processing in exchange for a warm window.

**Malformed records are skipped, not retried.** The consumer loop catches
exceptions per record, logs a warning, and continues, so one bad message cannot
halt the pipeline. For those records the effective semantic is at-most-once —
a deliberate trade favouring availability of the aggregate over completeness of
any single event.

None of these are hypothetical failure modes invented for the report; each
follows directly from the auto-commit configuration and the in-memory state
design, and each has a concrete fix noted above that was scoped out rather than
overlooked.

### A4. Feature store design

Redis holds one key per customer, `features:{customer_id}`, whose value is the
JSON-serialised features dict. A flat key-per-customer layout is the right shape
here because the access pattern is exclusively point lookup by customer — the
API never scans, never ranges, and never queries by any other attribute. A hash
or sorted set would add structure the workload has no use for.

#### TTL is atomic with the write

Every write sets its own expiry in the same command:

```python
self.client.set(key, value, ex=self.ttl_seconds)
```

Using `SET ... EX` rather than a `SET` followed by an `EXPIRE` is a deliberate
correctness choice, not a micro-optimisation. Two commands can interleave with a
failure: if the process dies between them, or the connection drops after the
first, the key persists **without an expiry** and becomes permanently stale
data that nothing will ever reclaim. Because expiry travels with the write,
that state is structurally unreachable — there is no code path that can produce
a features key lacking a TTL.

The TTL is 48 hours against a 24-hour window, deliberately double. Features
therefore outlive the window that produced them, so a processor outage degrades
gracefully: the API keeps serving progressively staler features for up to two
days rather than falling off a cliff into cache misses the moment the pipeline
stops.

#### Batch retrieval in a single round-trip

`get_customer_features_batch()` maps the customer IDs to keys, issues one
`MGET`, and zips the results back:

```python
keys = list(map(self._key, customer_ids))
raw_values = self.client.mget(keys)
return {cid: (json.loads(v) if v else None)
        for cid, v in zip(customer_ids, raw_values)}
```

The `zip` is safe because `MGET` guarantees results are returned in request
order, with a null placeholder for missing keys — so position is a reliable
join. An empty input list short-circuits before the call, because `MGET` with
zero keys is a Redis error rather than an empty result.

This was verified behaviourally rather than assumed. Resetting Redis statistics
with `CONFIG RESETSTAT`, scoring a 5-transaction batch spanning 3 distinct
customers, then reading `INFO commandstats` shows **`cmdstat_mget: calls=1` and
no `cmdstat_get` entry at all** — one round-trip, and provably not a loop of
`GET`s that merely looks batched from the outside.

#### Connection pooling

Pools are cached at module level, keyed by `(host, port, password)`, so every
`FeatureStore` constructed against the same target shares one pool rather than
opening its own connections. Two default-constructed stores return the same pool
object by identity; one pointed at a different port gets its own.

One implementation detail is worth recording because it fails silently. When a
`ConnectionPool` is supplied, **redis-py ignores connection keyword arguments
passed to the client** — so `decode_responses=True` must be set on the *pool*.
Setting it on the client instead is accepted without error and simply has no
effect: values come back as raw `bytes`, and `json.loads` then fails at a
distance from the actual mistake.

The pool also carries `socket_connect_timeout=1` and `socket_timeout=1`. Those
belong to the serving story rather than the storage design and are covered in
§B2, but they are configured here because the pool is where connection
behaviour is owned.

Configuration follows the same override pattern as the rest of the system:
constructor arguments win when supplied, otherwise values resolve from
`settings`. That is what lets the tests point a store at a dead port or a
blackholed host without touching the environment.
