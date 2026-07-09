package orchestrator

import (
	"context"
	"errors"
	"fmt"

	"github.com/dashu-baba/crawler-orchestrator/internal/provisioner"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrRunAlreadyActive is returned by CreateRun when another run is still
// active. runs_one_active_idx (migrations/000001_init.up.sql) enforces
// this at the DB level, so it's a real conflict, not a transient error.
var ErrRunAlreadyActive = errors.New("a run is already active")

// ErrNoPendingJobs is returned by Provision when there's nothing to size
// workers for. Callers should treat this as "nothing to do right now",
// not a failure.
var ErrNoPendingJobs = errors.New("no pending jobs to provision for")

// CreateRun inserts a new active run row and returns its id.
func CreateRun(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	var runID int64

	err := pool.QueryRow(ctx, `INSERT INTO runs DEFAULT VALUES RETURNING id`).Scan(&runID)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return 0, ErrRunAlreadyActive
		}
		return 0, fmt.Errorf("creating run: %w", err)
	}

	return runID, nil
}

// Provision sizes the current pending backlog, creates a run, and starts
// that many worker handles for it. If Create fails after the run row
// already exists, the run is left active: the caller decides whether to
// retry provisioning into it or mark it failed, rather than Provision
// guessing.
func Provision(ctx context.Context, pool *pgxpool.Pool, prov provisioner.Provisioner, params SizingParams) (int64, []provisioner.WorkerHandle, error) {
	x, err := ComputeSize(ctx, pool, params)
	if err != nil {
		return 0, nil, fmt.Errorf("computing worker count: %w", err)
	}
	if x == 0 {
		return 0, nil, ErrNoPendingJobs
	}

	runID, err := CreateRun(ctx, pool)
	if err != nil {
		return 0, nil, err
	}

	handles, err := prov.Create(ctx, runID, x)
	if err != nil {
		return runID, nil, fmt.Errorf("creating %d workers for run %d: %w", x, runID, err)
	}

	return runID, handles, nil
}
