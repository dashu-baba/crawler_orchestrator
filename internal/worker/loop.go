package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

func Run(ctx context.Context, pool *pgxpool.Pool, cfg Config) error {
	group, ctx := errgroup.WithContext(ctx)

	for _, category := range cfg.Categories {
		group.Go(func() error {
			return runCategory(ctx, pool, cfg, category)
		})
	}

	return group.Wait()
}

func runCategory(ctx context.Context, pool *pgxpool.Pool, cfg Config, category string) error {
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
	}
}
