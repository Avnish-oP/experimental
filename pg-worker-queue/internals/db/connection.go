package db

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect opens a PostgreSQL pool using DATABASE_URL and verifies connectivity.
// The caller must close the returned pool when the application shuts down.
func Connect(ctx context.Context) (*pgxpool.Pool, error) {
	connectionString := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if connectionString == "" {
		return nil, fmt.Errorf("DATABASE_URL must be set")
	}

	connectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(connectCtx, connectionString)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
	}

	if err := pool.Ping(connectCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping PostgreSQL: %w", err)
	}

	return pool, nil
}
