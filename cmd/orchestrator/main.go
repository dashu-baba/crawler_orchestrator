package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dashu-baba/crawler-orchestrator/internal/db"
	"github.com/dashu-baba/crawler-orchestrator/internal/orchestrator"
	"github.com/dashu-baba/crawler-orchestrator/internal/provisioner"
)

func main() {
	if err := run(); err != nil {
		if errors.Is(err, orchestrator.ErrNoPendingJobs) {
			slog.Info("nothing to do", "reason", err)
			return
		}
		slog.Error("orchestrator failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := orchestrator.Load()
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	pool, err := db.NewPool(ctx, cfg.DBURL)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer pool.Close()

	var prov provisioner.Provisioner
	switch cfg.ProvisionerKind {
	case "hetzner":
		prov = provisioner.NewHetznerProvisioner(cfg.Hetzner)
	default:
		prov = provisioner.NewDockerProvisioner(cfg.Provisioner)
	}

	if err := orchestrator.Reconcile(ctx, pool, prov); err != nil {
		return fmt.Errorf("reconciling orphaned workers: %w", err)
	}

	runID, handles, err := orchestrator.Provision(ctx, pool, prov, orchestrator.SizingParams{
		AvgItemSeconds:  cfg.AvgItemSeconds,
		DeadlineSeconds: cfg.DeadlineSeconds,
		MinWorkers:      cfg.MinWorkers,
		MaxWorkers:      cfg.MaxWorkers,
	})
	if err != nil {
		return fmt.Errorf("provisioning run: %w", err)
	}

	slog.Info("run provisioned", "run_id", runID, "workers", len(handles))

	progress, monitorErr := orchestrator.Monitor(ctx, pool, runID, cfg.MonitorInterval, cfg.DeadlineSeconds)

	status := orchestrator.RunStatusDone
	if monitorErr != nil {
		status = orchestrator.RunStatusFailed
	}

	// Teardown must not inherit ctx: if we're here because ctx was
	// cancelled (SIGTERM/interrupt), an already-done context would make
	// FinishRun's DB update and Destroy calls fail immediately, leaving
	// the workers running and billing. Cleanup gets its own budget.
	teardownCtx, cancelTeardown := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelTeardown()

	if finishErr := orchestrator.FinishRun(teardownCtx, pool, prov, runID, handles, status); finishErr != nil {
		if monitorErr != nil {
			return fmt.Errorf("monitor failed: %w; finishing run also failed: %w", monitorErr, finishErr)
		}
		return fmt.Errorf("finishing run: %w", finishErr)
	}

	if monitorErr != nil {
		return fmt.Errorf("monitor failed: %w", monitorErr)
	}

	slog.Info("run complete", "run_id", runID, "remaining", progress.Remaining, "dead", progress.Dead, "done", progress.Done)

	return nil
}
