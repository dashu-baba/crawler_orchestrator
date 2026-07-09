package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/dashu-baba/crawler-orchestrator/internal/provisioner"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Reconcile destroys every worker handle whose run is not active: its
// runs row is gone, done, or failed. Run this on orchestrator boot, before
// provisioning anything new -- it's what catches VMs/containers left
// behind by a crash between provisioning and teardown, per CLAUDE.md's
// cost guardrails (destroy, never leave running).
func Reconcile(ctx context.Context, pool *pgxpool.Pool, prov provisioner.Provisioner) error {
	handles, err := prov.List(ctx)
	if err != nil {
		return fmt.Errorf("listing worker handles: %w", err)
	}

	var orphaned []provisioner.WorkerHandle

	for _, h := range handles {
		active, err := runIsActive(ctx, pool, h.RunID)
		if err != nil {
			return fmt.Errorf("checking run %d status: %w", h.RunID, err)
		}
		if !active {
			orphaned = append(orphaned, h)
		}
	}

	if len(orphaned) == 0 {
		return nil
	}

	slog.Info("reconciling orphaned workers", "count", len(orphaned))

	if err := prov.Destroy(ctx, orphaned); err != nil {
		return fmt.Errorf("destroying %d orphaned worker(s): %w", len(orphaned), err)
	}

	return nil
}

func runIsActive(ctx context.Context, pool *pgxpool.Pool, runID int64) (bool, error) {
	var status string

	err := pool.QueryRow(ctx, `SELECT status FROM runs WHERE id = $1`, runID).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}

	return status == "active", nil
}
