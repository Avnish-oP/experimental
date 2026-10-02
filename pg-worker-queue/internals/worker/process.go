package worker

import (
	"context"
	"time"

	"pg-worker-queue/internals/jobs"
)

type ProcessingStats struct {
	WorkDuration       time.Duration
	CompletionDuration time.Duration
	Heartbeats         int
}

// ProcessJob is the same simulation, heartbeat, and completion path used by the
// worker executable and benchmark. The database pool is released during work.
func ProcessJob(ctx context.Context, store *jobs.Store, job jobs.Job, leaseDuration, heartbeatInterval, processingDuration time.Duration) (ProcessingStats, error) {
	var stats ProcessingStats
	started := time.Now()
	processingTimer := time.NewTimer(processingDuration)
	defer processingTimer.Stop()
	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()
	for {
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		case <-heartbeat.C:
			heartbeatCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := store.Heartbeat(heartbeatCtx, job, leaseDuration)
			cancel()
			if err != nil {
				return stats, err
			}
			stats.Heartbeats++
		case <-processingTimer.C:
			stats.WorkDuration = time.Since(started)
			completeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			completeStarted := time.Now()
			err := store.Complete(completeCtx, job)
			stats.CompletionDuration = time.Since(completeStarted)
			cancel()
			return stats, err
		}
	}
}
