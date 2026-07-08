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
