WITH expired AS MATERIALIZED (
    SELECT id, attempts
    FROM jobs
    WHERE status = 'processing' AND lease_expires_at <= clock_timestamp()
    ORDER BY lease_expires_at, id
    LIMIT 100
    FOR UPDATE SKIP LOCKED
), finished AS (
    UPDATE job_executions e
    SET status = 'failed', finished_at = clock_timestamp(), error_message = 'lease expired'
    FROM expired x
    WHERE e.job_id = x.id AND e.attempt = x.attempts AND e.status = 'processing'
    RETURNING e.job_id
), recovered AS (
    UPDATE jobs j
    SET status = CASE WHEN j.attempts < j.max_attempt THEN 'pending' ELSE 'failed' END,
        locked_at = NULL, locked_by = NULL, lease_expires_at = NULL,
        available_at = clock_timestamp() + $1::double precision * INTERVAL '1 second'
    FROM expired x
    WHERE j.id = x.id
    RETURNING j.id
)
SELECT COUNT(*) FROM recovered
CROSS JOIN (SELECT COUNT(*) FROM finished) execution_updates;
