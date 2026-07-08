CREATE TYPE public.jobs_status AS ENUM (
	'pending',
	'leased',
    'done',
    'dead');

CREATE TYPE public.runs_status AS ENUM (
	'active',
	'done',
    'failed');

-- One row per run. Gives you the run-level lock and progress accounting.
CREATE TABLE runs (
  id         BIGSERIAL PRIMARY KEY,
  status     public.runs_status        NOT NULL DEFAULT 'active',  -- active|done|failed
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ended_at   TIMESTAMPTZ
);

-- Enforces "only one active run at a time" at the DB level.
CREATE UNIQUE INDEX runs_one_active_idx ON runs (status)
  WHERE status = 'active';

-- The work queue. One row per item.
CREATE TABLE jobs (
  id            BIGSERIAL PRIMARY KEY,
  run_id        BIGINT      REFERENCES runs(id),  -- NULL until a worker first claims this job
  category      TEXT        NOT NULL,           -- one of the ~20 types
  url           TEXT        NOT NULL,
  config_uri    TEXT,                           -- or config JSONB below
  config        JSONB,                          -- per-item override (usually null; config is per-category)
  idem_key      TEXT        NOT NULL UNIQUE,    -- hash(category || url) or supplied item_id; global dedup
  status        public.jobs_status        NOT NULL DEFAULT 'pending',  -- pending|leased|done|dead
  attempts      INT         NOT NULL DEFAULT 0,
  worker_id     TEXT,
  lease_expires TIMESTAMPTZ,
  last_error    TEXT,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Index that makes the claim query fast (partial, category-scoped).
CREATE INDEX jobs_claimable_idx ON jobs (category, id)
  WHERE status IN ('pending','leased');
