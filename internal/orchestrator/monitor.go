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

// completed is the count of terminal outcomes (done + dead) so far --
// used to compute throughput deltas between polls.
func (p RunProgress) completed() int64 {
	return p.Done + p.Dead
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

// PollProgressByCategory is PollProgress broken down per category, so
// throughput/backlog can be tracked per the design doc's "items/sec per
// category" metric (§8) -- the category with the largest backlog is the
// one sizing (ComputeSize) is driven by, so it's the one worth watching
// for stalls.
func PollProgressByCategory(ctx context.Context, pool *pgxpool.Pool, runID int64) (map[string]RunProgress, error) {
	rows, err := pool.Query(ctx, `
		SELECT
			category,
			count(*) FILTER (WHERE status = 'pending') +
			count(*) FILTER (WHERE status = 'leased' AND run_id = $1) AS remaining,
			count(*) FILTER (WHERE status = 'dead' AND run_id = $1)   AS dead,
			count(*) FILTER (WHERE status = 'done' AND run_id = $1)   AS done
		FROM jobs
		WHERE status = 'pending' OR run_id = $1
		GROUP BY category
	`, runID)
	if err != nil {
		return nil, fmt.Errorf("polling per-category progress for run %d: %w", runID, err)
	}
	defer rows.Close()

	byCategory := make(map[string]RunProgress)
	for rows.Next() {
		var category string
		var progress RunProgress
		if err := rows.Scan(&category, &progress.Remaining, &progress.Dead, &progress.Done); err != nil {
			return nil, fmt.Errorf("scanning per-category progress for run %d: %w", runID, err)
		}
		byCategory[category] = progress
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading per-category progress for run %d: %w", runID, err)
	}

	return byCategory, nil
}

// runStartedAt fetches when runID actually started, so Monitor can compare
// elapsed wall-clock time against the deadline (projected-vs-deadline, §8)
// using the real start rather than Monitor's own invocation time, which
// lags run creation by however long provisioning took.
func runStartedAt(ctx context.Context, pool *pgxpool.Pool, runID int64) (time.Time, error) {
	var startedAt time.Time
	if err := pool.QueryRow(ctx, `SELECT started_at FROM runs WHERE id = $1`, runID).Scan(&startedAt); err != nil {
		return time.Time{}, fmt.Errorf("reading started_at for run %d: %w", runID, err)
	}
	return startedAt, nil
}

// Monitor polls progress every interval, logging remaining/dead/done,
// overall and per-category throughput, dead-letter rate, and a
// projected-finish-vs-deadline check, until the backlog drains
// (Drained() == true) or ctx is cancelled. It returns the last progress
// snapshot either way, so a cancelled monitor still tells the caller how
// far the run got.
func Monitor(ctx context.Context, pool *pgxpool.Pool, runID int64, interval time.Duration, deadlineSeconds float64) (RunProgress, error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	startedAt, err := runStartedAt(ctx, pool, runID)
	if err != nil {
		return RunProgress{}, err
	}
	deadlineAt := startedAt.Add(time.Duration(deadlineSeconds * float64(time.Second)))

	var last RunProgress
	var lastByCategory map[string]RunProgress
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

			byCategory, err := PollProgressByCategory(ctx, pool, runID)
			if err != nil {
				return last, err
			}

			now := time.Now()
			var itemsPerSec, deadPerSec float64
			var elapsed float64
			if !lastPolledAt.IsZero() {
				if elapsed = now.Sub(lastPolledAt).Seconds(); elapsed > 0 {
					itemsPerSec = float64(progress.completed()-last.completed()) / elapsed
					deadPerSec = float64(progress.Dead-last.Dead) / elapsed
				}
			}

			var projectedFinishAt time.Time
			var overDeadline bool
			if itemsPerSec > 0 {
				projectedRemaining := time.Duration(float64(progress.Remaining) / itemsPerSec * float64(time.Second))
				projectedFinishAt = now.Add(projectedRemaining)
				overDeadline = projectedFinishAt.After(deadlineAt)
			}

			slog.Info("run progress",
				"run_id", runID,
				"remaining", progress.Remaining,
				"dead", progress.Dead,
				"done", progress.Done,
				"items_per_sec", itemsPerSec,
				"dead_per_sec", deadPerSec,
				"projected_finish_at", projectedFinishAt,
				"deadline_at", deadlineAt,
				"over_deadline", overDeadline,
			)
			if overDeadline {
				slog.Warn("run projected to miss its deadline",
					"run_id", runID,
					"projected_finish_at", projectedFinishAt,
					"deadline_at", deadlineAt,
				)
			}

			for category, catProgress := range byCategory {
				var catItemsPerSec float64
				if elapsed > 0 {
					if prev, ok := lastByCategory[category]; ok {
						catItemsPerSec = float64(catProgress.completed()-prev.completed()) / elapsed
					}
				}
				slog.Info("category progress",
					"run_id", runID,
					"category", category,
					"remaining", catProgress.Remaining,
					"dead", catProgress.Dead,
					"done", catProgress.Done,
					"items_per_sec", catItemsPerSec,
				)
			}

			last, lastByCategory, lastPolledAt = progress, byCategory, now

			if progress.Drained() {
				return progress, nil
			}
		}
	}
}
