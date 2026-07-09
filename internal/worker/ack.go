package worker

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Ack marks a job as done. Idempotent: setting status='done' on an
// already-done row is a no-op, so it's safe to call more than once for
// the same job (e.g. if the worker dies after ack but before the next
// claim reflects the new status).
func Ack(ctx context.Context, pool *pgxpool.Pool, jobID int64) error {
	_, err := pool.Exec(ctx, `UPDATE jobs SET status = 'done', updated_at = now() WHERE id = $1`, jobID)
	if err != nil {
		return fmt.Errorf("acking job %d: %w", jobID, err)
	}

	return nil
}

// RecordFailure records the error from a failed attempt without changing
// the job's status. The job stays 'leased' and is retried once its lease
// expires; this just leaves a trail for whoever's debugging a job that
// keeps failing.
func RecordFailure(ctx context.Context, pool *pgxpool.Pool, jobID int64, lastErr error) error {
	_, err := pool.Exec(ctx, `UPDATE jobs SET last_error = $2, updated_at = now() WHERE id = $1`, jobID, lastErr.Error())
	if err != nil {
		return fmt.Errorf("recording failure for job %d: %w", jobID, err)
	}

	return nil
}

// DeadLetter marks a job dead after it has exceeded max_attempts. The run
// still completes; a dead job is a first-class terminal state, not lost
// work — see CLAUDE.md's job state machine.
func DeadLetter(ctx context.Context, pool *pgxpool.Pool, jobID int64, lastErr error) error {
	_, err := pool.Exec(ctx, `UPDATE jobs SET status = 'dead', last_error = $2, updated_at = now() WHERE id = $1`, jobID, lastErr.Error())
	if err != nil {
		return fmt.Errorf("dead-lettering job %d: %w", jobID, err)
	}

	return nil
}
