package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestClaimIntegration(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration checks")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	// Isolate claim tests so they cannot claim existing application jobs.
	schema := fmt.Sprintf("claim_test_%d", time.Now().UnixNano())
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE"); err != nil {
			t.Errorf("clean up test schema: %v", err)
		}
	}()
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	config.MaxConns = 40
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	sql, err := os.ReadFile("../db/schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(sql)); err != nil {
		t.Fatal(err)
	}
	store := NewStore(pool)
	reset := func(t *testing.T) {
		t.Helper()
		if _, err := pool.Exec(ctx, "TRUNCATE jobs, job_executions RESTART IDENTITY"); err != nil {
			t.Fatal(err)
		}
	}
	create := func(t *testing.T, at time.Time) Job {
		t.Helper()
		job, err := store.Create(ctx, CreateParams{Payload: json.RawMessage(`{"test":true}`), AvailableAt: &at})
		if err != nil {
			t.Fatal(err)
		}
		return job
	}
	assertClaim := func(t *testing.T, job Job, id int64, worker string) {
		t.Helper()
		if job.ID != id || job.Status != "processing" || job.Attempts != 1 || job.LockedAt == nil || job.LockedBy == nil || *job.LockedBy != worker {
			t.Fatalf("unexpected claim: %+v", job)
		}
		persisted, err := store.Get(ctx, id)
		if err != nil || persisted.Status != "processing" || persisted.Attempts != 1 || persisted.LockedAt == nil || persisted.LockedBy == nil || *persisted.LockedBy != worker {
			t.Fatalf("claim was not committed: %+v, %v", persisted, err)
		}
		var recorded int
		err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM job_executions
			WHERE job_id = $1 AND worker_id = $2 AND attempt = $3
			AND status = 'processing' AND started_at = $4 AND finished_at IS NULL`,
			id, worker, job.Attempts, job.LockedAt).Scan(&recorded)
		if err != nil || recorded != 1 {
			t.Fatalf("expected one matching execution, got %d, %v", recorded, err)
		}
	}
	past := time.Now().Add(-time.Hour)

	t.Run("eligibility and ordering", func(t *testing.T) {
		reset(t)
		create(t, time.Now().Add(time.Hour))
		exhausted := create(t, past.Add(-time.Minute))
		if _, err := pool.Exec(ctx, "UPDATE jobs SET attempts = max_attempt WHERE id = $1", exhausted.ID); err != nil {
			t.Fatal(err)
		}
		later := create(t, past)
		earlier := create(t, past.Add(-time.Minute))
		for _, id := range []int64{earlier.ID, later.ID} {
			job, err := store.Claim(ctx, "eligibility-worker")
			if err != nil {
				t.Fatal(err)
			}
			assertClaim(t, job, id, "eligibility-worker")
		}
		if _, err := store.Claim(ctx, "eligibility-worker"); !errors.Is(err, ErrNoAvailableJobs) {
			t.Fatalf("expected no eligible jobs, got %v", err)
		}
	})

	t.Run("skip locked", func(t *testing.T) {
		reset(t)
		first := create(t, past)
		second := create(t, past)
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, "SELECT id FROM jobs WHERE id = $1 FOR UPDATE", first.ID); err != nil {
			t.Fatal(err)
		}
		claimCtx, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		job, err := store.Claim(claimCtx, "skip-worker")
		if err != nil {
			t.Fatalf("claim blocked behind a locked row: %v", err)
		}
		assertClaim(t, job, second.ID, "skip-worker")
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		job, err = store.Claim(ctx, "next-worker")
		if err != nil {
			t.Fatal(err)
		}
		assertClaim(t, job, first.ID, "next-worker")
	})

	t.Run("rollback restores all claim fields", func(t *testing.T) {
		reset(t)
		original := create(t, past)
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err := scanJob(tx.QueryRow(ctx, claimSQL, "rollback-worker", DefaultLeaseDuration.Seconds())); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		job, err := store.Get(ctx, original.ID)
		if err != nil || job.Status != "pending" || job.Attempts != 0 || job.LockedAt != nil || job.LockedBy != nil {
			t.Fatalf("partial claim survived rollback: %+v, %v", job, err)
		}
		var count int
		if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM job_executions").Scan(&count); err != nil || count != 0 {
			t.Fatalf("execution survived rollback: count=%d, %v", count, err)
		}
	})

	t.Run("execution insert failure rolls back job claim", func(t *testing.T) {
		reset(t)
		original := create(t, past)
		// Deliberately inject a conflicting audit record to make recording fail.
		if _, err := pool.Exec(ctx, `INSERT INTO job_executions (job_id, worker_id, attempt)
			VALUES ($1, 'existing-worker', 1)`, original.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Claim(ctx, "conflicting-worker"); err == nil || errors.Is(err, ErrNoAvailableJobs) {
			t.Fatalf("expected execution insert failure, got %v", err)
		}
		job, err := store.Get(ctx, original.ID)
		if err != nil || job.Status != "pending" || job.Attempts != 0 || job.LockedAt != nil || job.LockedBy != nil {
			t.Fatalf("job update survived execution insert failure: %+v, %v", job, err)
		}
	})

	t.Run("heartbeat recovery and stale attempt fencing", func(t *testing.T) {
		reset(t)
		create(t, past)
		first, err := store.ClaimWithLease(ctx, "reused-worker-id", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Heartbeat(ctx, first, 20*time.Second); err != nil {
			t.Fatal(err)
		}
		current, err := store.Get(ctx, first.ID)
		if err != nil || current.LeaseExpiresAt == nil || first.LeaseExpiresAt == nil || !current.LeaseExpiresAt.After(*first.LeaseExpiresAt) {
			t.Fatalf("heartbeat did not extend lease: %+v, %v", current, err)
		}
		if count, err := store.RecoverExpired(ctx, 0); err != nil || count != 0 {
			t.Fatalf("recovered a healthy claim: %d, %v", count, err)
		}
		if _, err := pool.Exec(ctx, "UPDATE jobs SET lease_expires_at = clock_timestamp() - INTERVAL '1 second' WHERE id = $1", first.ID); err != nil {
			t.Fatal(err)
		}
		if err := store.Heartbeat(ctx, first, time.Second); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("expired worker renewed lease: %v", err)
		}
		if err := store.Complete(ctx, first); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("expired worker completed job: %v", err)
		}
		if count, err := store.RecoverExpired(ctx, time.Hour); err != nil || count != 1 {
			t.Fatalf("expected one recovered job: %d, %v", count, err)
		}
		current, err = store.Get(ctx, first.ID)
		if err != nil || current.Status != "pending" || current.Attempts != 1 || current.LockedBy != nil || current.LockedAt != nil || current.LeaseExpiresAt != nil {
			t.Fatalf("bad recovered state: %+v, %v", current, err)
		}
		var failed int
		if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM job_executions WHERE job_id = $1
			AND attempt = 1 AND status = 'failed' AND finished_at IS NOT NULL
			AND error_message = 'lease expired'`, first.ID).Scan(&failed); err != nil || failed != 1 {
			t.Fatalf("old execution was not finished: %d, %v", failed, err)
		}
		if _, err := store.Claim(ctx, "too-early"); !errors.Is(err, ErrNoAvailableJobs) {
			t.Fatalf("retry delay was ignored: %v", err)
		}
		if _, err := pool.Exec(ctx, "UPDATE jobs SET available_at = clock_timestamp() WHERE id = $1", first.ID); err != nil {
			t.Fatal(err)
		}
		second, err := store.Claim(ctx, "reused-worker-id")
		if err != nil || second.Attempts != 2 {
			t.Fatalf("expected retry attempt 2: %+v, %v", second, err)
		}
		// The ID is identical: the attempt token must fence the old worker.
		if err := store.Heartbeat(ctx, first, time.Second); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("stale attempt renewed newer lease: %v", err)
		}
		if err := store.Complete(ctx, first); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("stale attempt completed newer claim: %v", err)
		}
		if err := store.Complete(ctx, second); err != nil {
			t.Fatal(err)
		}
		current, err = store.Get(ctx, first.ID)
		if err != nil || current.Status != "completed" || current.Attempts != 2 || current.CompletedAt == nil || current.LeaseExpiresAt != nil || current.LockedBy != nil {
			t.Fatalf("bad completed state: %+v, %v", current, err)
		}
		var completed int
		if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM job_executions WHERE job_id = $1
			AND attempt = 2 AND status = 'completed' AND finished_at IS NOT NULL`, first.ID).Scan(&completed); err != nil || completed != 1 {
			t.Fatalf("completion not recorded: %d, %v", completed, err)
		}
		if err := store.Complete(ctx, second); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("completed job accepted another completion: %v", err)
		}
	})

	t.Run("expired final attempt becomes failed", func(t *testing.T) {
		reset(t)
		original := create(t, past)
		if _, err := pool.Exec(ctx, "UPDATE jobs SET max_attempt = 1 WHERE id = $1", original.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Claim(ctx, "last-attempt"); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, "UPDATE jobs SET lease_expires_at = clock_timestamp() - INTERVAL '1 second'"); err != nil {
			t.Fatal(err)
		}
		if count, err := store.RecoverExpired(ctx, 0); err != nil || count != 1 {
			t.Fatalf("recovery: %d, %v", count, err)
		}
		job, err := store.Get(ctx, original.ID)
		if err != nil || job.Status != "failed" || job.Attempts != 1 || job.LeaseExpiresAt != nil {
			t.Fatalf("final attempt was requeued: %+v, %v", job, err)
		}
		if _, err := store.Claim(ctx, "another-worker"); !errors.Is(err, ErrNoAvailableJobs) {
			t.Fatalf("failed job was claimed again: %v", err)
		}
	})

	t.Run("concurrent recovery handles each expired claim once", func(t *testing.T) {
		reset(t)
		const count = 20
		for n := 0; n < count; n++ {
			create(t, past)
			if _, err := store.Claim(ctx, "expired-worker"); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := pool.Exec(ctx, "UPDATE jobs SET lease_expires_at = clock_timestamp() - INTERVAL '1 second'"); err != nil {
			t.Fatal(err)
		}
		type result struct {
			count int
			err   error
		}
		results := make(chan result, 8)
		var wg sync.WaitGroup
		for n := 0; n < 8; n++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				count, err := store.RecoverExpired(ctx, 0)
				results <- result{count, err}
			}()
		}
		wg.Wait()
		close(results)
		total := 0
		for result := range results {
			if result.err != nil {
				t.Fatal(result.err)
			}
			total += result.count
		}
		if total != count {
			t.Fatalf("recovered %d jobs, want %d", total, count)
		}
		var mismatches int
		if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM jobs j JOIN job_executions e ON e.job_id = j.id
			WHERE j.status <> 'pending' OR j.attempts <> 1 OR j.lease_expires_at IS NOT NULL
			OR e.status <> 'failed' OR e.finished_at IS NULL`).Scan(&mismatches); err != nil || mismatches != 0 {
			t.Fatalf("recovery mismatches: %d, %v", mismatches, err)
		}
	})

	t.Run("concurrent workers claim each job once", func(t *testing.T) {
		reset(t)
		const count, workers = 1000, 32
		if _, err := pool.Exec(ctx, `INSERT INTO jobs (payload)
			SELECT jsonb_build_object('test', true, 'number', n)
			FROM generate_series(1, $1::integer) n`, count); err != nil {
			t.Fatal(err)
		}
		type result struct {
			job    Job
			worker string
			err    error
		}
		results := make(chan result, count+workers)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for n := 0; n < workers; n++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				<-start
				worker := fmt.Sprintf("concurrent-%d", n)
				for {
					job, err := store.Claim(ctx, worker)
					if errors.Is(err, ErrNoAvailableJobs) {
						return
					}
					results <- result{job, worker, err}
					if err != nil {
						return
					}
				}
			}(n)
		}
		close(start)
		wg.Wait()
		close(results)
		seen := make(map[int64]bool)
		for result := range results {
			if result.err != nil {
				t.Fatal(result.err)
			}
			if seen[result.job.ID] {
				t.Fatalf("job %d claimed twice", result.job.ID)
			}
			seen[result.job.ID] = true
			assertClaim(t, result.job, result.job.ID, result.worker)
		}
		if len(seen) != count {
			t.Fatalf("claimed %d distinct jobs, want %d", len(seen), count)
		}
		if _, err := store.Claim(ctx, "extra-worker"); !errors.Is(err, ErrNoAvailableJobs) {
			t.Fatalf("expected empty queue, got %v", err)
		}
		var executions, distinctJobs, duplicates, mismatches int
		if err := pool.QueryRow(ctx, `SELECT COUNT(*), COUNT(DISTINCT job_id)
			FROM job_executions`).Scan(&executions, &distinctJobs); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM (
			SELECT job_id FROM job_executions GROUP BY job_id HAVING COUNT(*) > 1
		) duplicates`).Scan(&duplicates); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM jobs j
			LEFT JOIN job_executions e ON e.job_id = j.id
			WHERE e.id IS NULL OR j.status <> 'processing' OR j.attempts <> 1
			OR e.attempt <> j.attempts OR e.worker_id <> j.locked_by
			OR e.started_at <> j.locked_at OR e.status <> 'processing'`).Scan(&mismatches); err != nil {
			t.Fatal(err)
		}
		if executions != count || distinctJobs != count || duplicates != 0 || mismatches != 0 {
			t.Fatalf("executions=%d, distinct jobs=%d, duplicates=%d, mismatches=%d",
				executions, distinctJobs, duplicates, mismatches)
		}
		t.Logf("%d concurrent workers: %d executions, %d distinct jobs, %d duplicate claims, %d mismatches",
			workers, executions, distinctJobs, duplicates, mismatches)
	})
}
