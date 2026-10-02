package jobs

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"
)

// ErrLeaseLost means this attempt has expired, finished, or been replaced.
var ErrLeaseLost = errors.New("job lease lost")

//go:embed recover.sql
var recoverSQL string

// Heartbeat extends only the caller's current, unexpired claim. The attempt
// number is a fencing token, even if a worker ID is reused on a later attempt.
func (s *Store) Heartbeat(ctx context.Context, job Job, leaseDuration time.Duration) error {
	if job.LockedBy == nil {
		return ErrLeaseLost
	}
	if leaseDuration <= 0 {
		return fmt.Errorf("lease duration must be positive")
	}
	tag, err := s.pool.Exec(ctx, `UPDATE jobs
		SET lease_expires_at = clock_timestamp() + $4::double precision * INTERVAL '1 second'
		WHERE id = $1 AND locked_by = $2 AND attempts = $3
		AND status = 'processing' AND lease_expires_at > clock_timestamp()`,
		job.ID, *job.LockedBy, job.Attempts, leaseDuration.Seconds())
	if err != nil {
		return fmt.Errorf("heartbeat job: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

// Complete atomically finishes the job and execution only while this attempt
// still owns an unexpired lease. A stale worker cannot finish a newer attempt.
func (s *Store) Complete(ctx context.Context, job Job) error {
	if job.LockedBy == nil {
		return ErrLeaseLost
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin completion: %w", err)
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `UPDATE jobs SET status = 'completed',
		completed_at = clock_timestamp(), locked_at = NULL, locked_by = NULL, lease_expires_at = NULL
		WHERE id = $1 AND locked_by = $2 AND attempts = $3
		AND status = 'processing' AND lease_expires_at > clock_timestamp()`,
		job.ID, *job.LockedBy, job.Attempts)
	if err != nil {
		return fmt.Errorf("complete job: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	tag, err = tx.Exec(ctx, `UPDATE job_executions
		SET status = 'completed', finished_at = clock_timestamp()
		WHERE job_id = $1 AND worker_id = $2 AND attempt = $3 AND status = 'processing'`,
		job.ID, *job.LockedBy, job.Attempts)
	if err != nil {
		return fmt.Errorf("complete execution: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("active execution missing for job %d attempt %d", job.ID, job.Attempts)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit completion: %w", err)
	}
	return nil
}

// RecoverExpired atomically fails expired executions and requeues jobs with
// remaining attempts. Multiple recovery loops can safely run concurrently.
func (s *Store) RecoverExpired(ctx context.Context, retryDelay time.Duration) (int, error) {
	if retryDelay < 0 {
		return 0, fmt.Errorf("retry delay must not be negative")
	}
	var recovered int
	err := s.pool.QueryRow(ctx, recoverSQL, retryDelay.Seconds()).Scan(&recovered)
	if err != nil {
		return 0, fmt.Errorf("recover expired jobs: %w", err)
	}
	return recovered, nil
}
