package orchestrator

import (
	"context"
	"fmt"

	"github.com/dashu-baba/crawler-orchestrator/internal/provisioner"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RunStatus is a terminal state for a run, mirroring the runs_status enum
// (migrations/000001_init.up.sql) minus 'active'.
type RunStatus string

const (
	RunStatusDone   RunStatus = "done"
	RunStatusFailed RunStatus = "failed"
)

// FinishRun marks the run as status and destroys every worker handle
// belonging to it. Workers are destroyed, never stopped -- Hetzner (and
// most clouds) bill until delete, so a stopped-but-not-deleted VM is a
// cost leak. Destroy is attempted even if marking the run status fails,
// and vice versa, so a partial failure doesn't leave both the DB and the
// VMs in a stale state.
func FinishRun(ctx context.Context, pool *pgxpool.Pool, prov provisioner.Provisioner, runID int64, handles []provisioner.WorkerHandle, status RunStatus) error {
	_, updateErr := pool.Exec(ctx, `UPDATE runs SET status = $2, ended_at = now() WHERE id = $1`, runID, string(status))
	if updateErr != nil {
		updateErr = fmt.Errorf("marking run %d as %s: %w", runID, status, updateErr)
	}

	destroyErr := prov.Destroy(ctx, handles)
	if destroyErr != nil {
		destroyErr = fmt.Errorf("destroying workers for run %d: %w", runID, destroyErr)
	}

	if updateErr != nil && destroyErr != nil {
		return fmt.Errorf("%w; %w", updateErr, destroyErr)
	}
	if updateErr != nil {
		return updateErr
	}
	return destroyErr
}
