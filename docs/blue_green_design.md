# Blue-Green Deployment Design

Deliverable for Part B. This describes the zero-downtime release strategy used
in this project, implemented with `deployment/docker-compose.blue-green.yml`,
`deployment/nginx/nginx.conf`, and `deployment/switch_traffic.sh`. No
Kubernetes is involved; §4 maps the design onto K8s for a later unit.

## 1. Strategy

**Blue** and **green** are two complete, independently addressable instances of
the fraud-detection API — `api-blue` and `api-green` in
`deployment/docker-compose.blue-green.yml`. Both are built from the same
`Dockerfile` and both connect to the same `redis` service, so they share
feature state; only the application version differs between them.

Three ports matter, and the separation between them is the whole point of the
design:

| Endpoint         | Address                 | Purpose                                                          |
| ---------------- | ----------------------- | ---------------------------------------------------------------- |
| **Stable**       | `http://localhost:8080` | The only address clients use. nginx serves it; it never changes. |
| **Blue direct**  | `http://localhost:8001` | Out-of-band access to blue, for health checks and smoke tests.   |
| **Green direct** | `http://localhost:8002` | Out-of-band access to green, same purpose.                       |

At any moment **exactly one** of blue/green receives live traffic. That is
enforced in `nginx.conf`, where the `fraud_backend` upstream has one active
`server` line and one commented-out standby:

```nginx
upstream fraud_backend {
    server api-blue:8000;      # ACTIVE
    # server api-green:8000;   # STANDBY
}
```

A new version is released by deploying it to the **idle** colour while the
active colour keeps serving. If blue is live, the new build goes to green;
green comes up fully — container started, model loaded, `/health` returning 200
— entirely outside the request path, reachable only on its direct port `:8002`.
Nothing about the stable endpoint changes during this period. Startup cost,
model load time, and any crash-on-boot defect are absorbed by an instance no
client can reach.

This is the property that distinguishes blue-green from a rolling restart: at
no point is a half-started instance serving traffic, and at no point is
capacity reduced. The cost is that both versions run simultaneously, roughly
doubling resource use during a release.

## 2. Cutover

`deployment/switch_traffic.sh` performs the flip. It is **direction-agnostic**:
it reads the current state rather than taking an argument, which is what makes
rollback and roll-forward the same operation (§3).

**Step 1 — determine current state.** The script greps `nginx.conf` for an
uncommented blue server line:

```bash
if grep -qE '^[[:space:]]*server api-blue:8000;' "$CONF"; then
  current=blue; target=green; target_port=8002
else
  current=green; target=blue; target_port=8001
fi
```

The config file is the single source of truth for which colour is live — there
is no separate state to drift out of sync with reality.

**Step 2 — health gate.** Covered in §3. Traffic does not move until the target
proves itself.

**Step 3 — rewrite the upstream.** `sed` comments out the active server line
and uncomments the standby, inverting the block shown above. A `.bak` file is
left behind, so the previous config is recoverable.

**Step 4 — reload nginx:**

```bash
docker compose -f deployment/docker-compose.blue-green.yml exec -T nginx nginx -s reload
```

**Why the reload is zero-downtime.** `nginx -s reload` is a graceful reload,
not a restart. On receiving the signal the master process re-reads its
configuration and starts **new** worker processes bound to the new upstream.
Existing workers stop accepting new connections but are allowed to finish the
requests they are already handling, exiting only once their in-flight work
completes. The listening socket is owned by the master and is never closed, so
no connection is refused during the changeover. The practical result:

- Requests already in flight when the switch fires complete against the **old**
  version and return normally — they are not cancelled or retried.
- **New** connections are routed to the **new** version immediately.
- A client holding a **keep-alive** connection keeps using the old version
  until the draining worker closes that idle connection; the client then
  reconnects and lands on the new version. Migration is therefore
  near-immediate but not instantaneous, and it is driven by connection
  lifetime, not by request count.

The keep-alive path carries one small caveat, measured rather than assumed (see
§5): there is a narrow race in which a client sends a request just as the
draining worker closes the connection. A client that does not retry sees a
single failed request. In a 60,000-request run this occurred **once** (0.002%),
while the fresh-connection probe over the same cutover saw **zero** failures.
Standard HTTP clients with connection-retry enabled absorb this transparently;
it is a property of connection reuse across a reload, not of the deployment
strategy.

## 3. Health gate & rollback

**Health gate.** Before touching any configuration, the script polls the
target's **direct** port and refuses to proceed unless it answers:

```bash
ok=0
for _ in $(seq 1 20); do
  if curl -sf "http://localhost:${target_port}/health" >/dev/null 2>&1; then ok=1; break; fi
  sleep 2
done
if [ "$ok" -ne 1 ]; then
  echo "ERROR: $target is not healthy on :${target_port}. Staying on $current."
  exit 1
fi
```

Three properties are worth naming:

- **It polls the direct port, not `:8080`.** Checking the stable endpoint would
  only confirm the _old_ version is healthy — useless as a gate. Probing
  `:8001`/`:8002` is what makes the check meaningful.
- **It retries for up to 40 seconds** (20 attempts × 2s), which accommodates
  container start plus model load without requiring a human to time the
  release.
- **Failure is safe.** `curl -sf` treats any non-2xx as failure; on timeout the
  script exits **before** the `sed`, leaving `nginx.conf` untouched and the
  current version serving. Combined with `set -euo pipefail`, a broken build
  cannot take the endpoint down — the release simply does not happen. The worst
  outcome is a failed deploy, never an outage.

`/health` is served by `src/api/main.py` and returns 200 only once the module
has fully imported — which includes `FraudDetector()` loading the model file
and `FeatureStore()` constructing its connection pool. A container that starts
but cannot load its model never passes the gate.

**Rollback.** Run the same script again:

```bash
./deployment/switch_traffic.sh
```

Because direction is derived from current state, a second invocation flips
back. Rollback is therefore:

- **Symmetric** — identical mechanism, identical duration, identical
  zero-downtime guarantee. Nothing about reverting is a less-tested path than
  deploying.
- **Fast** — a config rewrite plus a graceful reload, seconds rather than a
  rebuild. The old version is still running and warm; nothing needs to be
  recreated.
- **Gated in both directions** — the rollback target is health-checked too.

The old version is deliberately left running after a switch. It is the rollback
target, and it costs nothing but idle memory to keep it available. It should
only be torn down once the new version has been observed healthy under real
traffic.

## 4. How this maps to Kubernetes

The same shape survives the translation, with different machinery. The two
compose services become two **Deployments** (`fraud-api-blue`,
`fraud-api-green`) carrying a distinguishing label such as `version: blue` /
`version: green`, each managing its own replica set. nginx's stable `:8080`
endpoint becomes a single **Service** whose `selector` includes that label; the
Service's ClusterIP and DNS name are what clients address, and they never
change. Cutover, instead of a `sed` on `nginx.conf` and a reload, becomes a
patch of the Service's selector
(`kubectl patch service fraud-api -p '{"spec":{"selector":{"version":"green"}}}'`),
which atomically repoints kube-proxy at the other Deployment's pods. The health
gate maps onto **readiness probes** hitting `/health`: pods are only added to
the Service's endpoint list once ready, so Kubernetes enforces the gate
continuously rather than only at switch time. Rollback remains a patch back to
the previous label.

## 5. Evidence

Captured 2026-07-18. Two load generators ran concurrently against the stable
endpoint `:8080` while `switch_traffic.sh` fired on a 20-second timer, so the
cutover was guaranteed to land mid-run:

- **Keep-alive load** — `tests/test_performance.py --n 60000`, one reused
  connection.
- **New-connection probe** — 140 `curl` requests at 0.5s intervals, each
  opening a fresh connection.

**Switch script output**

```
Active version: blue  ->  switching to: green
2026/07/18 23:13:39 [notice] 31#31: signal process started
Switched to green. Endpoint http://localhost:8080 is now serving green.
```

**Harness result**

```json
{
  "requests": 60000,
  "errors": 1,
  "throughput_rps": 720.4,
  "latency_ms": { "p50": 1.14, "p95": 3.28, "p99": 4.78, "max": 12.58 }
}
```

**New-connection probe:** 140 requests, **0 non-200 responses**.

**Traffic actually served per colour**

| Version                   | Requests served |
| ------------------------- | --------------- |
| `api-blue` (pre-switch)   | **17,809**      |
| `api-green` (post-switch) | **42,330**      |

This split is the proof the cutover occurred. `errors: 0` alone is **not**
sufficient evidence — a switch that fires after the load finishes yields a
clean zero while never moving traffic at all. Both colours serving is what
distinguishes a real cutover from an untested one. The final config, read from
inside the nginx container, confirms green active.

**On the single error.** One request of 60,000 (0.002%) failed, on the
keep-alive connection, at the moment the draining worker closed it — the race
described in §2. The fresh-connection probe recorded zero failures across the
same cutover, and a `max` latency of 12.58 ms shows no stalled or retried
connection. It is reported as measured rather than rounded to zero; an HTTP
client with connection retry would not have observed it.

**Method note.** The load must target `:8080`. A direct port (`:8001`/`:8002`)
bypasses nginx entirely and demonstrates nothing about the cutover.

**Defect found and fixed during this capture.** `switch_traffic.sh` originally
rewrote `nginx.conf` with `sed -i`, which replaces the file's **inode**.
Because `docker-compose.blue-green.yml` bind-mounts a single _file_ rather than
a directory, that rename silently detached the container's view of the config:
the host file changed, the script reported success, `nginx -s reload` reloaded
the **stale** in-container copy, and traffic never switched. Three consecutive
runs appeared to pass — the script printed `Switched to ...` each time — while
`api-green` served exactly zero requests. The fix rewrites the original inode
(`cat "$CONF.tmp" > "$CONF"`) so the mount holds; the per-colour counts above
are what proved it. Recovering from the detached state also required recreating
the nginx container, whose mount still pointed at the orphaned inode.

---

### Implementation note

`api-blue` and `api-green` in the provided compose file are currently built
from identical source and the same `MODEL_PATH`, so a switch is not externally
visible beyond the nginx logs. To make a demo show a _visible_ version change,
give the two services distinguishing configuration — for example a different
`MODEL_PATH` (pointing green at a retrained model) — so that `/model/info`
returns different values per colour and the cutover can be observed from the
client side. This does not change the deployment mechanism, only its
observability during the demo.
