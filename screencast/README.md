# Screencast

A recorded tour of the running system. Everything here is a real run against
the live stack — no mock output, no edited terminal text.

**Watch the whole thing: [`demo.mp4`](demo.mp4)** (3 min 22 s, 824 KB) — a
title slide, then the nine segments below, each introduced by a title card.

Individual segments are also kept separately, each with a GIF preview that
renders inline on GitHub and an MP4 for playback.

---

## 1. The stack

[`segment1-stack.mp4`](segment1-stack.mp4)

![stack](segment1-stack.gif)

Five services up, all healthy. The interesting part is the *ordering*: Kafka and
Redis carry health checks, and the API and processor declare
`depends_on: service_healthy`, so they don't start until their dependencies
genuinely accept connections. Compose's default `depends_on` only waits for the
container to exist, which is how you get an API that boots, fails its first
Redis call and stays up looking fine.

## 2. Streaming

[`segment2-streaming.mp4`](segment2-streaming.mp4)

![streaming](segment2-streaming.gif)

The `transactions` topic has 3 partitions, all assigned to the single
`feature-processor` consumer, with lag moving as the simulator produces. Then a
customer's feature key is read twice six seconds apart and
`transaction_count` has advanced — showing the Kafka → processor → Redis path
is live, not a fixture loaded at startup.

Partition count is the parallelism ceiling: because the producer keys by
`customer_id`, a customer's events always land on the same partition, so
3 partitions supports up to 3 processor instances without splitting any
customer's window state across consumers.

## 3. Scoring

[`segment3-predict.mp4`](segment3-predict.mp4)

![predict](segment3-predict.gif)

The same customer, two transactions. A $130 grocery purchase scores 0.0; a
$4,000 online purchase scores 1.0. The point is that the verdict comes from the
*streamed history* — CUST0001's rolling average is about $125 — rather than
from a threshold on the amount. The segment ends on the per-request structured
log carrying `customer_id`, `latency_ms`, `fraud_probability` and a `degraded`
flag as bound fields.

## 4. Blue-green deployment

[`segment4-blue-green.mp4`](segment4-blue-green.mp4)

![blue-green](segment4-blue-green.gif)

Continuous load against the stable `:8080` endpoint, with the traffic switch
fired eight seconds in. The per-colour request counts are the evidence, and they
are what makes this a real demonstration: an error count alone proves nothing,
because a switch that fires *after* the load finishes also reports zero errors.
Only both colours showing non-zero traffic proves the cutover landed mid-load.
The script says so itself, printing `PASS` or `INCONCLUSIVE`.

## 5. Performance

[`segment5-performance.mp4`](segment5-performance.mp4)

![performance](segment5-performance.gif)

5,000 requests against the full stack, zero errors, against a 100 ms
requirement — roughly 34× headroom at p95. These are cache *hits*, verified
rather than assumed: the harness draws from the same `CUST0000`–`CUST0199` ID
space the simulator populates, so the numbers cover the real lookup → merge →
score path.

The exact figures on screen will differ from the report's Table 9 and from any
other take, because throughput varies by ~1.7× run to run on a developer
machine. The report quotes the median of five and says so; this is one run.
Note also that the tape passes `--out /tmp/demo_results.json` — the harness
overwrites `results.json` by default, and recording this segment would otherwise
replace the committed artifact the report cites.

## 6. Graceful degradation

[`segment6-resilience.mp4`](segment6-resilience.mp4)

![resilience](segment6-resilience.gif)

Redis is stopped mid-demo and the same $130 request is replayed. It still
returns 200 — but it now scores **1.0 instead of 0.0**, because there is no
history left to compare it against. That false positive is the honest cost of
degrading, and it is why the segment shows it rather than reusing the $4,000
transaction, which scores 1.0 either way and would have hidden the difference.

Availability is preserved, accuracy is not. Redis is then restarted and the
score returns to 0.0 with no API restart, because the connection pool
reconnects on demand.

## 7. Hardening and tests

[`segment7-container-tests.mp4`](segment7-container-tests.mp4)

![container](segment7-container-tests.gif)

Straight from the Docker daemon: the container runs as `appuser`, not root, and
its `HEALTHCHECK` reports `healthy` — a check implemented with
`python -c urllib.request` because the slim base image ships no `curl`. Then the
test suite, 7 passing.

## 8. Batch scoring

[`segment8-batch.mp4`](segment8-batch.mp4)

![batch](segment8-batch.gif)

Five transactions spanning three customers, scored in one request. The claim
worth proving is that this costs the feature store **one** round-trip rather
than five, so the segment proves it against Redis rather than asserting it:
command counters are reset immediately before the request, and afterwards
`INFO commandstats` shows `cmdstat_mget:calls=1` and no `GET` at all.

Note that `latency_ms` accumulates down the list — it measures elapsed time
since the batch began, not per-item scoring cost.

## 9. Where the time goes

[`segment9-profile.mp4`](segment9-profile.mp4)

![profile](segment9-profile.gif)

The hot path profiled in isolation over 2,000 iterations: Redis `GET` ~0.05 ms,
model inference ~0.06 ms, logging ~0.02 ms, validation and merge negligible —
about **0.14 ms of application work** in total. Benchmarking `/health`, which
does no work, through the same client shows a 0.26 ms transport floor.

Application code is therefore a minority of a request: the system is
**framework-bound, not compute-bound**. Full decomposition, and the optimisation
that was tried and rejected on the evidence, are in
[the report](../docs/report.md#p3-bottleneck-analysis).

---

## How these were made

Recorded with [VHS](https://github.com/charmbracelet/vhs). Each segment has a
`.tape` file in [`tapes/`](tapes) declaring its terminal size, theme
(Catppuccin Mocha) and the commands to type, so any segment can be re-recorded
reproducibly:

```bash
vhs screencast/tapes/06-resilience.tape
```

The commands themselves live in `scripts/demo_*.sh` rather than inline in the
tapes. That is deliberate: VHS's parser rejects embedded quotes and `{{...}}`
template braces, which rules out putting JSON payloads or `docker inspect`
format strings directly in a tape. Keeping the logic in shell scripts also
means the demos are runnable on their own, without VHS installed.

Segments are recorded at different heights so nothing scrolls off. To join
them, [`build_demo.sh`](build_demo.sh) pads each onto a common 1500×760 canvas
in the terminal's background colour, generates a title card per segment, and
concatenates. Re-running it after re-recording any segment rebuilds the whole
video:

```bash
bash screencast/build_demo.sh      # → demo.mp4
```

The stack must be running (`docker compose up -d`) before recording or
rebuilding, since every segment queries live containers.
