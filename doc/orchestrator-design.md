# Orchestrator → Worker System — Design

**Status:** draft for review · **Stack:** Go + Postgres + Hetzner Cloud API
**Goal:** process ~1.2M jobs/day (20 categories × ~60k items) within a 4h window, at minimal cost, with no always-on worker fleet.

---

## 1. Overview

A **stateless orchestrator** computes how many ephemeral **worker VMs** are needed, provisions them via the Hetzner Cloud API, monitors progress against a shared Postgres work queue, and destroys the VMs when the run drains. Workers pull work themselves (no push-assignment), so failure handling is a property of the queue, not of the orchestrator.

```
                 ┌─────────────────────────────────────────┐
   cron / API →  │            ORCHESTRATOR (Go)             │
                 │  size → provision → monitor → teardown   │
                 └───────┬─────────────────────┬────────────┘
                         │ Hetzner API          │ SQL (poll progress)
             create/destroy VMs                 │
                         ▼                       ▼
        ┌────────┐  ┌────────┐  ┌────────┐   ┌──────────────┐
        │worker 1│  │worker 2│  │worker x│   │  POSTGRES    │
        │ own IP │  │ own IP │  │ own IP │   │  jobs queue  │
        └───┬────┘  └───┬────┘  └───┬────┘   │  runs table  │
            │ claim/ack (SKIP LOCKED)         └──────┬───────┘
            └──────────────┬──────────────────────────┘
                           ▼
              fetch URL → transform → write JSON (object store)
                                     + upsert metadata (Postgres)
```

**Key principle:** at-least-once delivery + idempotent writes = *effectively-once*. Every reliability guarantee below derives from this plus Postgres row locking.

---

## 2. Data model

```sql
-- The work queue. One row per item.
CREATE TABLE jobs (
  id            BIGGERIAL PRIMARY KEY,
  run_id        BIGINT      NOT NULL REFERENCES runs(id),
  category      TEXT        NOT NULL,           -- one of the ~20 types
  url           TEXT        NOT NULL,
  config_uri    TEXT,                           -- or config JSONB below
  config        JSONB,                          -- per-item override (usually null; config is per-category)
  idem_key      TEXT        NOT NULL,           -- hash(category || url) or supplied item_id
  status        TEXT        NOT NULL DEFAULT 'pending',  -- pending|leased|done|dead
  attempts      INT         NOT NULL DEFAULT 0,
  worker_id     TEXT,
  lease_expires TIMESTAMPTZ,
  last_error    TEXT,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Index that makes the claim query fast (partial, category-scoped).
CREATE INDEX jobs_claimable_idx ON jobs (category, id)
  WHERE status IN ('pending','leased');

-- One row per run. Gives you the run-level lock and progress accounting.
CREATE TABLE runs (
  id         BIGSERIAL PRIMARY KEY,
  status     TEXT        NOT NULL DEFAULT 'active',  -- active|done|failed
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ended_at   TIMESTAMPTZ
);
```

> Config is **per-category**, not per-item. Workers fetch each category's config **once** and cache it. `config_uri`/`config` columns exist only for the rare per-item override.

---

## 3. The claim mechanic (the heart of the system)

A worker runs **one goroutine per category**. Each goroutine claims **only from its category**, which is how you get "one item per category in flight per worker." There is no single query that returns one-row-per-category; it's N per-category claims.

```sql
-- Called by the goroutine responsible for category $2, on worker $1.
UPDATE jobs
SET status='leased',
    worker_id=$1,
    attempts=attempts+1,
    lease_expires = now() + interval '90 seconds',   -- DB time, never worker clock
    updated_at = now()
WHERE id = (
  SELECT id FROM jobs
  WHERE run_id = $3
    AND category = $2
    AND (status='pending'
         OR (status='leased' AND lease_expires < now()))   -- reclaim crashed leases
  ORDER BY id
  LIMIT 1
  FOR UPDATE SKIP LOCKED           -- concurrent claimers each get a different row
)
RETURNING id, url, config_uri, config, idem_key, attempts;
```

**Why this satisfies the requirements:**
- **No double-execution across workers** — `SKIP LOCKED` hands each concurrent claimer a distinct row.
- **Re-run only failures** — a crashed worker's leases expire; the `OR lease_expires < now()` clause reclaims exactly the incomplete items, not the whole batch.
- **One-per-category** — each goroutine is scoped to its `category`.

---

## 4. Orchestrator lifecycle

### 4.1 Sizing (not hardcoded)
```
x = ceil( max_category_items × avg_item_seconds / deadline_seconds )
```
Recomputed each run from live `COUNT(*) GROUP BY category` and a rolling average of recent item latency. Clamp to `[x_min, x_max]` for safety. Example: 60k × 1s / 14,400s ≈ 5.

### 4.2 Provision
- Acquire the **run lock** (see §7). If a run is already active, skip or queue.
- Create `runs` row → `run_id`.
- Enumerate work into `jobs` (idempotent: `INSERT ... ON CONFLICT (idem_key, run_id) DO NOTHING`).
- Call Hetzner API to create x servers **from a pre-baked snapshot** containing the worker binary (fast boot). Tag each VM: `run_id`, `role=worker`, `ttl=<deadline+buffer>`. Pass `run_id` + DB DSN via cloud-init/user-data.
- Each VM gets a **fresh ephemeral public IP** (free, and distinct per VM → your IP-diversity goal).

### 4.3 Monitor
Poll every ~15s:
```sql
SELECT
  count(*) FILTER (WHERE status IN ('pending','leased')) AS remaining,
  count(*) FILTER (WHERE status='dead')                   AS dead
FROM jobs WHERE run_id=$1;
```
Track throughput (items completed / elapsed) to detect stalls. Alert if projected finish > deadline.

### 4.4 Teardown
When `remaining = 0`:
- Mark `runs.status='done'`.
- **Destroy** (not stop — Hetzner bills until delete) every VM tagged with this `run_id`.
- Log final report: totals, dead-letter count, wall-clock, cost estimate.

---

## 5. Worker lifecycle

```go
func runWorker(ctx context.Context, runID int64, workerID string, categories []string) {
    var wg sync.WaitGroup
    for _, cat := range categories {                // one goroutine per category
        wg.Add(1)
        go func(cat string) {
            defer wg.Done()
            cfg := loadCategoryConfigOnce(cat)       // fetch + cache per category
            for {
                if ctx.Err() != nil { return }
                item, ok := claim(ctx, runID, workerID, cat)   // §3 query
                if !ok {
                    if nextCategory := stealTarget(runID, cat); nextCategory != "" {
                        cat = nextCategory           // work-stealing: help the backlog
                        continue
                    }
                    return                           // nothing left anywhere → exit
                }
                if err := process(ctx, item, cfg); err != nil {
                    handleFailure(ctx, item, err)    // requeue or dead-letter
                    continue
                }
                ack(ctx, item)                       // status='done'
                heartbeatIfLongRunning(item)         // extend lease for slow items
            }
        }(cat)
    }
    wg.Wait()
    selfTerminate()          // delete own VM via Hetzner API (belt-and-suspenders vs orchestrator)
}
```

### process() write path (idempotent)
1. `fetch(url)` — from this VM's unique IP.
2. `transform(data, cfg)`.
3. `PUT object at key = idem_key + ".json"` — overwrite-safe.
4. `UPSERT metadata ON CONFLICT (idem_key) DO UPDATE` — safe on retry.
5. Only after both writes succeed → `ack`. If the worker dies between step 3 and 5, the lease expires and the item re-runs; steps 3–4 overwrite identically. **Effectively-once.**

---

## 6. Reliability properties (mapped to your requirements)

| Requirement | How it's met |
|---|---|
| **Idempotent** | Object key + metadata upsert keyed on `idem_key`; re-run overwrites identically. |
| **Retryable** | Failed item stays `pending` (lease expires or explicit reset); `attempts` increments. |
| **Fail-safe** | Poison item → after `max_attempts` set `status='dead'` (dead-letter), run still completes. |
| **Reliable / at-least-once** | Lease + `SKIP LOCKED` guarantee delivery; nothing is lost on crash. |
| **Resilient** | Worker crash reclaimed via lease expiry; orchestrator crash reconciled via VM tags (§7). |
| **Scalable** | Add workers = start more VMs; they claim from the same pool, no rebalancing. x is a runtime knob. |
| **No double-run** | `FOR UPDATE SKIP LOCKED` — two workers never claim the same row. |

### Retry & backoff
- Transient failures (network, 5xx, timeout) → requeue with incremented `attempts`.
- **Per-host backoff**: on HTTP 429/503 from an upstream host, that goroutine backs off (exponential + jitter) so you don't hammer a rate-limited host.
- `max_attempts` (e.g. 5) → dead-letter with `last_error` recorded.

---

## 7. Fail-safe against the two scary failures

### Orchestrator crash (orphaned VMs = money leak)
- **Per-VM TTL**: every worker VM self-destructs after `deadline + buffer` regardless of anything (cloud-init installs a shutdown timer + self-delete). Hard ceiling on cost.
- **Reconciliation on restart**: orchestrator boot → list all Hetzner VMs tagged `role=worker`; for any whose `run_id` is `done`/absent, destroy them.

### Duplicate / overlapping runs
- **Run lock** via Postgres advisory lock or a unique partial index (`CREATE UNIQUE INDEX one_active_run ON runs (status) WHERE status='active'`). Second trigger either no-ops or waits.

### Clock safety
- All lease math uses Postgres `now()`. Workers never compare against their own clock. Eliminates skew-induced double-claims across VMs.

---

## 8. Logging & observability

**Structured JSON logs** (slog) on every component, correlated by `run_id` and `item_id`:

- **Orchestrator**: sizing decision (inputs + computed x), each VM create/destroy (with Hetzner server ID + IP), poll snapshots (remaining/dead/throughput), teardown summary.
- **Worker**: startup (worker_id, IP, categories), per-item `claim → done/fail` with `attempts` and duration, backoff events, self-terminate.
- **Error logging**: failures carry `item_id`, `attempts`, `category`, `last_error`, and whether they were requeued or dead-lettered. Dead-letters are a first-class, queryable state, not just a log line.

**Metrics worth emitting** (even to stdout for a start): items/sec per category, claim latency, lease-reclaim count (a spike = workers crashing), dead-letter rate, VM count, projected-vs-deadline.

**Correlation:** one `run_id` threads orchestrator → every worker → every item, so you can reconstruct any run end-to-end.

---

## 9. Local verifiability

The whole thing runs on a laptop with **no Hetzner account and no cloud spend**, because the only cloud-specific piece is the VM provisioner — which is an interface.

```go
type Provisioner interface {
    Create(ctx, runID int64, n int) ([]WorkerHandle, error)
    Destroy(ctx, handles []WorkerHandle) error
    List(ctx) ([]WorkerHandle, error)     // for reconciliation
}
```

- **Prod impl**: `HetznerProvisioner` (calls the API).
- **Local impl**: `DockerProvisioner` (or just `os/exec`) that starts N worker **containers/processes** instead of VMs.

`docker-compose.yml` for local test:
- `postgres` — the real queue.
- `mock-upstream` — a tiny Go HTTP server returning canned payloads (and deliberately 429/500/slow responses to exercise backoff + retry).
- `orchestrator` — runs with `DockerProvisioner`.
- Worker containers — spawned by the orchestrator.

**Tests to prove the guarantees locally:**
1. **No-duplication**: seed 1000 items, run 5 workers, assert every item processed exactly once and every object key written once.
2. **Crash recovery**: `docker kill` a worker mid-run, assert only its in-flight items get reclaimed and the run still completes.
3. **Poison item**: inject an always-500 URL, assert it dead-letters after `max_attempts` and the run still finishes.
4. **Idempotency**: run the same batch twice, assert object store and metadata are identical (no dupes).
5. **Orphan reap**: kill the orchestrator mid-run, restart, assert it reaps leftover workers.

---

## 10. Open questions (fill these to finalize numbers)

1. **avg_item_seconds** — real measurement, not a guess. Sets x precisely.
2. **Output JSON size + overwrite vs accumulate** — sets storage cost (see cost analysis).
3. **Idem key definition** — `hash(category+url)` or a supplied `item_id`?
4. **URL host distribution** — many hosts (natural spread) or few (need per-host rate governor)?
5. **Whose URLs** — yours/partner/third-party (governs how aggressive IP diversity should be).
6. **Work-stealing: confirmed?** (Design assumes yes.)

---

## 11. Build order (suggested)

Status legend (as of 2026-07-10): ✅ done · 🟡 partial · ⬜ not started.

1. ✅ Schema + claim query + idempotent write path (pure Postgres, no cloud).
2. 🟡 Worker binary with the goroutine-per-category loop + retry/backoff/dead-letter.
   Loop, lease-reclaim retry, and dead-lettering are done. **Gaps:** `process()`
   is still a fake (sleeps + always succeeds; no real fetch/transform), and
   per-host exponential backoff + jitter on 429/503 (§6) is not implemented —
   both deferred with the mock-upstream work in step 3.
3. 🟡 `DockerProvisioner` + docker-compose + the 5 local tests above. **Prove correctness here.**
   `DockerProvisioner` and the compose stack are done. **Gaps:** no `mock-upstream`
   service, and none of the 5 acceptance tests exist yet — the core is not yet
   proven per this step's own bar.
4. ✅ Orchestrator sizing + monitor + teardown.
5. 🟡 `HetznerProvisioner` + pre-baked snapshot + TTL + reconciliation.
   `HetznerProvisioner` (Create/Destroy/List) is implemented and verified against
   the live API; cloud-init TTL self-destruct and reconciliation are done. Snapshot
   build tooling exists (`packer/worker-snapshot.pkr.hcl` + `scripts/build-snapshot.sh`).
   **Gap:** the tooling has not been run to produce a real snapshot / boot a worker
   from it.
6. ✅ Structured logging/metrics throughout.
   `slog` threaded through. Monitor emits overall + per-category items/sec,
   dead-letter rate, and projected-finish-vs-deadline (warns if a run is on
   track to miss it). Claim emits claim latency and flags lease-reclaim
   events (Attempts > 1) at Warn. VM count logged at provision time. All
   verified live against the local stack by manually advancing job state
   between poll ticks and confirming the computed rates.
7. 🟡 Deploy: orchestrator on one small always-on VM; workers ephemeral.
   Orchestrator is dockerized (`Dockerfile.orchestrator`). Deploy artifacts done:
   `deploy/docker-compose.prod.yaml` (Postgres + migrate + orchestrator, combined
   on one VM, no MinIO -- prod uses Hetzner Object Storage), systemd oneshot
   unit + daily timer, and `deploy/README.md` runbook (private network, VM
   creation, firewall lockdown, first-run verification). **Gap:** not actually
   deployed to a real VM yet -- artifacts are written and compose-validated,
   not run against live infrastructure.
