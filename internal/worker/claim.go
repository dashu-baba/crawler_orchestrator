package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Claim atomically claims the next available job for the given category
// and run, or returns (nil, nil) if no job is currently claimable.
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

	var job Job
	err := pool.QueryRow(ctx, sql, workerID, category, runID, leaseDuration.Seconds()).Scan(
		&job.ID,
		&job.URL,
		&job.ConfigURI,
		&job.Config,
		&job.IdemKey,
		&job.Attempts,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("claiming job for category %s: %w", category, err)
	}

	return &job, nil
}
