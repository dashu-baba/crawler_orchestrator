# CLAUDE.md

Guidance for working in this repository. Full design lives in [`doc/orchestrator-design.md`](doc/orchestrator-design.md) — read it before making architectural changes.

## Working agreement — instructor, not author

**The user writes the code. Claude is the instructor; the user is the executor.**

- **Do not write, edit, or generate implementation code unless the user explicitly asks for it** (e.g. "write this function", "give me the code"). Default to *not* producing code.
- When asked how to build something, respond with: explanations, best practices, trade-offs, the relevant framework/library concepts, topics to learn, pseudocode or high-level steps, and pointers to docs — **not** finished implementations.
- Prefer guiding questions and small hints that let the user implement it themselves. Review and critique what they write; suggest improvements rather than rewriting it for them.
- Config, schema, and scaffolding count as code too — ask before generating them.
- If a request is ambiguous about whether they want code, **ask first**.

## What this is

A **stateless orchestrator** that provisions ephemeral **worker VMs**, feeds them from a shared Postgres work queue, and destroys them when the run drains. Target load: ~1.2M jobs/day (20 categories × ~60k items) inside a 4h window, at minimal cost, with no always-on worker fleet.

**Stack:** Go 1.25 · Postgres · Hetzner Cloud API (prod) / Docker (local).

**Status:** greenfield. Only the design doc exists so far — no Go code yet. Follow the build order below.

## Core invariants — do not violate

These are the reliability guarantees the whole system rests on. Any change that weakens one is a bug.

1. **At-least-once delivery + idempotent writes = effectively-once.** Every write path must be safe to run twice: object key = `idem_key + ".json"` (overwrite-safe), metadata `UPSERT ... ON CONFLICT (idem_key)`. Only `ack` (set `status='done'`) *after* both writes succeed.
2. **Claim via `FOR UPDATE SKIP LOCKED`.** This is the heart of the system (design §3). Two workers must never claim the same row. Never replace it with a plain `SELECT ... UPDATE`.
3. **Workers pull, orchestrator never pushes.** Failure handling is a property of the queue, not the orchestrator. Don't add push-assignment.
4. **All lease/time math uses Postgres `now()`, never the worker clock.** Prevents skew-induced double-claims across VMs. Crashed leases are reclaimed via `lease_expires < now()`.
5. **One goroutine per category per worker** — that's how "one item per category in flight" is achieved. There is no single query returning one-row-per-category; it's N per-category claims.
6. **Provisioner is an interface.** Cloud specifics live *only* behind `Provisioner` (`Create`/`Destroy`/`List`). Everything else must run locally with no Hetzner account and no cloud spend.

## Job / run state machine

- `jobs.status`: `pending → leased → done` (happy path); `leased → pending` on lease expiry (reclaim); `pending → dead` after `max_attempts` (dead-letter, run still completes).
- `runs.status`: `active → done` / `failed`. Only one `active` run at a time — enforced by a run lock (Postgres advisory lock or `UNIQUE INDEX ... WHERE status='active'`).
- Dead-letters are a **first-class queryable state**, not just a log line.

## Cost & safety guardrails (these prevent money leaks)

- **Destroy VMs, never stop them** — Hetzner bills until delete.
- **Per-VM TTL**: every worker self-destructs after `deadline + buffer` via cloud-init, regardless of orchestrator state. Hard ceiling on cost.
- **Reconciliation on orchestrator restart**: list all VMs tagged `role=worker`; destroy any whose `run_id` is `done`/absent.
- Workers `selfTerminate()` when the queue drains (belt-and-suspenders vs. orchestrator teardown).

## Conventions

- **Logging:** structured JSON via `slog`, always correlated by `run_id` and `item_id`. One `run_id` threads orchestrator → every worker → every item.
- **Idempotency key:** `hash(category || url)` or a supplied `item_id` (see open question §10.3 — confirm before hardcoding).
- **Config is per-category, not per-item.** Workers fetch each category's config once and cache it. `config_uri`/`config` columns exist only for the rare per-item override.
- **Retry/backoff:** transient failures (network, 5xx, timeout) requeue with incremented `attempts`. Per-host exponential backoff + jitter on 429/503. Dead-letter at `max_attempts` (e.g. 5) with `last_error` recorded.

## Local development

The whole system runs on a laptop via `docker-compose` (design §9) — swap `HetznerProvisioner` for `DockerProvisioner`:
- `postgres` — the real queue.
- `mock-upstream` — tiny Go server returning canned payloads plus deliberate 429/500/slow responses to exercise backoff/retry.
- `orchestrator` — runs with `DockerProvisioner`, spawns worker containers.

**Correctness is proven locally.** The five acceptance tests (design §9) are the definition of done for the core:
1. No-duplication · 2. Crash recovery · 3. Poison item dead-letters · 4. Idempotency (run batch twice) · 5. Orphan reap.

## Build order (design §11)

1. Schema + claim query + idempotent write path (pure Postgres, no cloud).
2. Worker binary: goroutine-per-category loop + retry/backoff/dead-letter.
3. `DockerProvisioner` + docker-compose + the 5 tests. **Prove correctness here.**
4. Orchestrator sizing + monitor + teardown.
5. `HetznerProvisioner` + pre-baked snapshot + TTL + reconciliation.
6. Structured logging/metrics throughout.
7. Deploy: orchestrator on one small always-on VM; workers ephemeral.

## Sizing formula

`x = ceil(max_category_items × avg_item_seconds / deadline_seconds)`, clamped to `[x_min, x_max]`. Recomputed each run from live `COUNT(*) GROUP BY category` + rolling latency average. Never hardcode worker count.

## Open questions (resolve before finalizing)

See design §10: `avg_item_seconds`, output JSON size/overwrite policy, idem-key definition, URL host distribution (per-host rate governor?), URL ownership (IP-diversity aggressiveness), and whether work-stealing is confirmed. Flag these rather than guessing.
