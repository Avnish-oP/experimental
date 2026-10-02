WITH candidate AS (
    SELECT id
    FROM jobs
    WHERE status = 'pending'
      AND available_at <= clock_timestamp()
      AND attempts < max_attempt
    ORDER BY available_at, id
    LIMIT 1
    FOR UPDATE SKIP LOCKED
), claimed AS (
    UPDATE jobs
    SET status = 'processing',
        locked_at = clock_timestamp(),
        locked_by = $1,
        lease_expires_at = clock_timestamp() + $2::double precision * INTERVAL '1 second',
        attempts = attempts + 1
    WHERE id = (SELECT id FROM candidate)
    RETURNING id, status, payload, locked_at, locked_by, created_at,
              completed_at, attempts, max_attempt, available_at, lease_expires_at
), recorded AS (
    INSERT INTO job_executions (job_id, worker_id, attempt, started_at)
    SELECT id, locked_by, attempts, locked_at
    FROM claimed
    RETURNING job_id
)
SELECT claimed.*
FROM claimed
JOIN recorded ON recorded.job_id = claimed.id;
