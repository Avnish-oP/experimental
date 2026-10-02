BEGIN;

CREATE TABLE IF NOT EXISTS jobs (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    status       TEXT NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    payload      JSONB NOT NULL DEFAULT '{}'::jsonb,
    locked_at    TIMESTAMPTZ,
    locked_by    TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    attempts     INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempt  INTEGER NOT NULL DEFAULT 3 CHECK (max_attempt > 0),
    available_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    lease_expires_at TIMESTAMPTZ
);

-- Upgrade databases created before leases were introduced.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;
UPDATE jobs SET lease_expires_at = COALESCE(locked_at, CURRENT_TIMESTAMP) + INTERVAL '10 seconds'
WHERE status = 'processing' AND lease_expires_at IS NULL;
CREATE INDEX IF NOT EXISTS jobs_processing_lease_idx
    ON jobs (lease_expires_at) WHERE status = 'processing';

-- Supports workers finding jobs that are ready to run.
CREATE INDEX IF NOT EXISTS jobs_pending_available_idx
    ON jobs (available_at, id)
    WHERE status = 'pending';

-- Supports finding stale locks after a worker stops unexpectedly.
CREATE INDEX IF NOT EXISTS jobs_processing_locked_idx
    ON jobs (locked_at)
    WHERE status = 'processing';

-- One record per claimed attempt, written atomically with the job update.
CREATE TABLE IF NOT EXISTS job_executions (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id      BIGINT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    worker_id   TEXT NOT NULL CHECK (length(btrim(worker_id)) > 0),
    attempt     INTEGER NOT NULL CHECK (attempt > 0),
    status      TEXT NOT NULL DEFAULT 'processing'
                CHECK (status IN ('processing', 'completed', 'failed')),
    started_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMPTZ,
    error_message TEXT,
    UNIQUE (job_id, attempt)
);
ALTER TABLE job_executions ADD COLUMN IF NOT EXISTS error_message TEXT;

-- A job can have only one active execution, including across retry attempts.
CREATE UNIQUE INDEX IF NOT EXISTS job_executions_one_active_idx
    ON job_executions (job_id)
    WHERE status = 'processing';

COMMIT;
