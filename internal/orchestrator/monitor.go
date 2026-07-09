package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RunProgress summarizes where a run stands. Remaining counts jobs still
// needing work: pending jobs haven't been claimed by anyone yet (run_id is
// still NULL at that point), leased jobs are in flight under this run.
// Dead and Done are scoped to this run_id since a job only carries a
// run_id once a worker of that run has claimed it.
type RunProgress struct {
	Remaining int64
	Dead      int64
	Done      int64
}

// Drained reports whether there's nothing left for this run to do.
func (p RunProgress) Drained() bool {
	return p.Remaining == 0
}

// PollProgress reads the current state of the queue for runID.
func PollProgress(ctx context.Context, pool *pgxpool.Pool, runID int64) (RunProgress, error) {
	var progress RunProgress

	err := pool.QueryRow(ctx, `
		SELECT
			count(*) FILTER (WHERE status = 'pending') +
			count(*) FILTER (WHERE status = 'leased' AND run_id = $1) AS remaining,
			count(*) FILTER (WHERE status = 'dead' AND run_id = $1)   AS dead,
			count(*) FILTER (WHERE status = 'done' AND run_id = $1)   AS done
		FROM jobs
		WHERE status = 'pending' OR run_id = $1
	`, runID).Scan(&progress.Remaining, &progress.Dead, &progress.Done)
	if err != nil {
		return RunProgress{}, fmt.Errorf("polling progress for run %d: %w", runID, err)
	}

	return progress, nil
}

// Monitor polls progress every interval, logging remaining/dead/done and a
// rolling throughput figure, until the backlog drains (Drained() == true)
// or ctx is cancelled. It returns the last progress snapshot either way,
// so a cancelled monitor still tells the caller how far the run got.
func Monitor(ctx context.Context, pool *pgxpool.Pool, runID int64, interval time.Duration) (RunProgress, error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var last RunProgress
	var lastPolledAt time.Time

	for {
		select {
		case <-ctx.Done():
			return last, ctx.Err()
		case <-ticker.C:
			progress, err := PollProgress(ctx, pool, runID)
			if err != nil {
				return last, err
			}

			now := time.Now()
			var itemsPerSec float64
			if !lastPolledAt.IsZero() {
				if elapsed := now.Sub(lastPolledAt).Seconds(); elapsed > 0 {
					completed := (progress.Done + progress.Dead) - (last.Done + last.Dead)
					itemsPerSec = float64(completed) / elapsed
				}
			}

			slog.Info("run progress",
				"run_id", runID,
				"remaining", progress.Remaining,
				"dead", progress.Dead,
				"done", progress.Done,
				"items_per_sec", itemsPerSec,
			)

			last, lastPolledAt = progress, now

			if progress.Drained() {
				return progress, nil
			}
		}
	}
}
