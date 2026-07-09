package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/dashu-baba/crawler-orchestrator/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"golang.org/x/sync/errgroup"
)

// Run starts one goroutine per category (one item per category in flight,
// per the design doc) and blocks until ctx is cancelled or one of them
// returns a non-nil error, at which point the others are cancelled too.
func Run(ctx context.Context, pool *pgxpool.Pool, minioClient *minio.Client, cfg Config) error {
	group, ctx := errgroup.WithContext(ctx)

	for _, category := range cfg.Categories {
		group.Go(func() error {
			return runCategory(ctx, pool, minioClient, cfg, category)
		})
	}

	return group.Wait()
}

// runCategory repeatedly claims, processes, writes, and acks jobs for a
// single category until ctx is cancelled or Claim itself fails. A failure
// in process/write/ack is not fatal: the job stays leased and is retried
// once its lease expires, rather than crashing this goroutine.
func runCategory(ctx context.Context, pool *pgxpool.Pool, minioClient *minio.Client, cfg Config, category string) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		job, err := Claim(ctx, pool, cfg.RunID, cfg.WorkerID, category, cfg.LeaseDuration)
		if err != nil {
			return err
		}

		if job == nil {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(1 * time.Second):
			}
			continue
		}

		slog.Info("claimed job", "category", category, "job_id", job.ID, "url", job.URL, "attempts", job.Attempts)

		// Defensive: a job can only arrive here already over budget if a
		// previous failure on its final attempt didn't get dead-lettered
		// (e.g. this worker crashed before handleFailure ran). Catch it
		// before wasting a process/write cycle on it.
		if int(job.Attempts) > cfg.MaxAttempts {
			deadLetter(ctx, pool, category, job, fmt.Errorf("exceeded max attempts (%d)", cfg.MaxAttempts))
			continue
		}

		payload, err := process(ctx, job)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("process failed", "category", category, "job_id", job.ID, "error", err)
			handleFailure(ctx, pool, category, cfg, job, err)
			continue
		}

		if err := store.Write(ctx, minioClient, cfg.MinIOBucket, job.IdemKey, payload); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("write failed", "category", category, "job_id", job.ID, "error", err)
			handleFailure(ctx, pool, category, cfg, job, err)
			continue
		}

		if err := Ack(ctx, pool, job.ID); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("ack failed", "category", category, "job_id", job.ID, "error", err)
			handleFailure(ctx, pool, category, cfg, job, err)
			continue
		}

		slog.Info("job done", "category", category, "job_id", job.ID)
	}
}

// handleFailure decides what a failed attempt means for the job's future:
// if this was already its last permitted attempt, dead-letter it now
// rather than leaving it leased to expire and get reclaimed just to be
// dead-lettered on the next pass. Otherwise, just record the error and
// leave it leased for retry.
func handleFailure(ctx context.Context, pool *pgxpool.Pool, category string, cfg Config, job *Job, cause error) {
	if int(job.Attempts) >= cfg.MaxAttempts {
		deadLetter(ctx, pool, category, job, cause)
		return
	}

	if err := RecordFailure(ctx, pool, job.ID, cause); err != nil {
		slog.Error("recording failure failed", "category", category, "job_id", job.ID, "error", err)
	}
}

// deadLetter is a best-effort wrapper around DeadLetter: if dead-lettering
// itself fails, that's logged but never propagated, since it shouldn't
// crash the worker — the job just gets picked up again once its lease
// expires and gets caught by the same check next time.
func deadLetter(ctx context.Context, pool *pgxpool.Pool, category string, job *Job, cause error) {
	if err := DeadLetter(ctx, pool, job.ID, cause); err != nil {
		slog.Error("dead-letter failed", "category", category, "job_id", job.ID, "error", err)
		return
	}
	slog.Warn("job dead-lettered", "category", category, "job_id", job.ID, "attempts", job.Attempts, "cause", cause)
}
