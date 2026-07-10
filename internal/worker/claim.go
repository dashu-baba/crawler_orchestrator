package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Claim atomically claims the next available job for the given category
// and run, or returns (nil, nil) if no job is currently claimable. It logs
// claim latency and flags reclaimed leases (Attempts > 1 means this row
// was previously claimed and its lease expired before being acked or
// dead-lettered) -- a spike in reclaims signals workers crashing or
// hanging, per the design doc's §8 metrics.
func Claim(ctx context.Context, pool *pgxpool.Pool, runID int64, workerID string, category string, leaseDuration time.Duration) (*Job, error) {
	sql := `UPDATE jobs
			SET status = 'leased',
				run_id = $3,
				worker_id = $1,
				attempts = attempts + 1,
				lease_expires = now() + make_interval(secs => $4),
				updated_at = now()
			WHERE id = (
			SELECT id FROM jobs
			WHERE category = $2
				AND (
				(run_id IS NULL AND status = 'pending')
				OR (run_id = $3 AND status = 'leased' AND lease_expires < now())
				)
			ORDER BY id
			LIMIT 1
			FOR UPDATE SKIP LOCKED
			)
			RETURNING id, url, config_uri, config, idem_key, attempts;
	`

	start := time.Now()
	var job Job
	err := pool.QueryRow(ctx, sql, workerID, category, runID, leaseDuration.Seconds()).Scan(
		&job.ID,
		&job.URL,
		&job.ConfigURI,
		&job.Config,
		&job.IdemKey,
		&job.Attempts,
	)
	latency := time.Since(start)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			slog.Debug("claim latency", "category", category, "run_id", runID, "found", false, "latency_ms", latency.Milliseconds())
			return nil, nil
		}
		return nil, fmt.Errorf("claiming job for category %s: %w", category, err)
	}

	slog.Info("claim latency", "category", category, "run_id", runID, "found", true, "latency_ms", latency.Milliseconds())
	if job.Attempts > 1 {
		slog.Warn("lease reclaimed", "category", category, "run_id", runID, "job_id", job.ID, "attempts", job.Attempts)
	}

	return &job, nil
}
