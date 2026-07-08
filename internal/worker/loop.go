package worker

import (
	"context"
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

		slog.Info("claimed job", "category", category, "job_id", job.ID, "url", job.URL)

		payload, err := process(ctx, job)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// Leave the job leased; its lease expires and it gets reclaimed.
			slog.Error("process failed", "category", category, "job_id", job.ID, "error", err)
			continue
		}

		if err := store.Write(ctx, minioClient, cfg.MinIOBucket, job.IdemKey, payload); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("write failed", "category", category, "job_id", job.ID, "error", err)
			continue
		}

		if err := Ack(ctx, pool, job.ID); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("ack failed", "category", category, "job_id", job.ID, "error", err)
			continue
		}

		slog.Info("job done", "category", category, "job_id", job.ID)
	}
}
