package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"pg-worker-queue/internals/db"
	"pg-worker-queue/internals/jobs"
	"pg-worker-queue/internals/worker"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	host, err := os.Hostname()
	if err != nil {
		return err
	}
	id := flag.String("id", fmt.Sprintf("%s-%d", host, os.Getpid()), "worker group ID")
	concurrency := flag.Int("concurrency", 1, "number of worker goroutines")
	pollInterval := flag.Duration("poll-interval", time.Second, "delay when idle or after a database error")
	leaseDuration := flag.Duration("lease-duration", jobs.DefaultLeaseDuration, "claim lease duration")
	heartbeatInterval := flag.Duration("heartbeat-interval", 2*time.Second, "lease renewal interval")
	recoveryInterval := flag.Duration("recovery-interval", time.Second, "expired claim scan interval")
	retryDelay := flag.Duration("retry-delay", time.Second, "delay before an expired job can be retried")
	processingDuration := flag.Duration("processing-duration", 3*time.Second, "simulated payload processing duration")
	flag.Parse()
	if strings.TrimSpace(*id) == "" || *concurrency <= 0 || *pollInterval <= 0 || *leaseDuration <= 0 || *heartbeatInterval <= 0 || *heartbeatInterval >= *leaseDuration || *recoveryInterval <= 0 || *retryDelay < 0 || *processingDuration <= 0 {
		return fmt.Errorf("id must be nonempty; durations and concurrency must be positive; retry-delay must be nonnegative; heartbeat-interval must be shorter than lease-duration")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := db.Connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	store := jobs.NewStore(pool)
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		recoverLoop(ctx, store, *recoveryInterval, *retryDelay)
	}()
	for n := 1; n <= *concurrency; n++ {
		workers.Add(1)
		go func(workerID string) {
			defer workers.Done()
			claimLoop(ctx, store, workerID, *pollInterval, *leaseDuration, *heartbeatInterval, *processingDuration)
		}(fmt.Sprintf("%s/%d", *id, n))
	}
	workers.Wait()
	return nil
}

func claimLoop(ctx context.Context, store *jobs.Store, workerID string, pollInterval, leaseDuration, heartbeatInterval, processingDuration time.Duration) {
	log.Printf("worker %s started", workerID)
	for ctx.Err() == nil {
		claimCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		job, err := store.ClaimWithLease(claimCtx, workerID, leaseDuration)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			log.Printf("worker %s claimed job %d (attempt %d/%d)", workerID, job.ID, job.Attempts, job.MaxAttempt)
			if _, err := worker.ProcessJob(ctx, store, job, leaseDuration, heartbeatInterval, processingDuration); err != nil {
				log.Printf("worker %s stopped job %d: %v", workerID, job.ID, err)
				continue
			}
			log.Printf("worker %s completed job %d", workerID, job.ID)
			continue
		}
		if !errors.Is(err, jobs.ErrNoAvailableJobs) {
			log.Printf("worker %s: %v", workerID, err)
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func recoverLoop(ctx context.Context, store *jobs.Store, interval, retryDelay time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		recoveryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		count, err := store.RecoverExpired(recoveryCtx, retryDelay)
		cancel()
		if err != nil && ctx.Err() == nil {
			log.Printf("recover expired jobs: %v", err)
		} else if count > 0 {
			log.Printf("recovered %d expired jobs", count)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
