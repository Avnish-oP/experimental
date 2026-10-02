package jobs

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Job struct {
	ID             int64           `json:"id"`
	Status         string          `json:"status"`
	Payload        json.RawMessage `json:"payload"`
	LockedAt       *time.Time      `json:"locked_at"`
	LockedBy       *string         `json:"locked_by"`
	CreatedAt      time.Time       `json:"created_at"`
	CompletedAt    *time.Time      `json:"completed_at"`
	Attempts       int32           `json:"attempts"`
	MaxAttempt     int32           `json:"max_attempt"`
	AvailableAt    time.Time       `json:"available_at"`
	LeaseExpiresAt *time.Time      `json:"lease_expires_at"`
}

type CreateParams struct {
	Payload     json.RawMessage `json:"payload"`
	MaxAttempt  *int32          `json:"max_attempt,omitempty"`
	AvailableAt *time.Time      `json:"available_at,omitempty"`
}

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

const columns = `id, status, payload, locked_at, locked_by, created_at,
    completed_at, attempts, max_attempt, available_at, lease_expires_at`

func (s *Store) Create(ctx context.Context, params CreateParams) (Job, error) {
	return scanJob(s.pool.QueryRow(ctx, `
		INSERT INTO jobs (payload, max_attempt, available_at)
		VALUES ($1, COALESCE($2::integer, 3), COALESCE($3::timestamptz, CURRENT_TIMESTAMP))
		RETURNING `+columns, params.Payload, params.MaxAttempt, params.AvailableAt))
}

func (s *Store) Get(ctx context.Context, id int64) (Job, error) {
	return scanJob(s.pool.QueryRow(ctx, `SELECT `+columns+` FROM jobs WHERE id = $1`, id))
}

func scanJob(row pgx.Row) (Job, error) {
	var job Job
	err := row.Scan(&job.ID, &job.Status, &job.Payload, &job.LockedAt,
		&job.LockedBy, &job.CreatedAt, &job.CompletedAt, &job.Attempts,
		&job.MaxAttempt, &job.AvailableAt, &job.LeaseExpiresAt)
	return job, err
}
