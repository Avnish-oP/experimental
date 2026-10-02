package jobs

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

//go:embed claim.sql
var claimSQL string

var ErrNoAvailableJobs = errors.New("no available jobs")

const DefaultLeaseDuration = 10 * time.Second

// Claim locks and updates one eligible job and records its execution in a single
// transaction. It returns only after commit, so processing happens outside it.
func (s *Store) Claim(ctx context.Context, workerID string) (Job, error) {
	return s.ClaimWithLease(ctx, workerID, DefaultLeaseDuration)
}

func (s *Store) ClaimWithLease(ctx context.Context, workerID string, leaseDuration time.Duration) (Job, error) {
	if strings.TrimSpace(workerID) == "" {
		return Job{}, fmt.Errorf("worker ID must not be empty")
	}
	if leaseDuration <= 0 {
		return Job{}, fmt.Errorf("lease duration must be positive")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Job{}, fmt.Errorf("begin job claim: %w", err)
	}
	defer tx.Rollback(ctx)

	job, err := scanJob(tx.QueryRow(ctx, claimSQL, workerID, leaseDuration.Seconds()))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNoAvailableJobs
	}
	if err != nil {
		return Job{}, fmt.Errorf("claim job: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, fmt.Errorf("commit job claim: %w", err)
	}
	return job, nil
}
