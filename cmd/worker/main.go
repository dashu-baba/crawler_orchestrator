package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dashu-baba/crawler-orchestrator/internal/db"
	"github.com/dashu-baba/crawler-orchestrator/internal/worker"
)

const dbConnectTimeout = 10 * time.Second

func main() {
	if err := run(); err != nil {
		slog.Error("Worker exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := worker.Load()
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	slog.Info("worker config loaded",
		"run_id", cfg.RunID,
		"worker_id", cfg.WorkerID,
		"categories", cfg.Categories,
		"lease_duration", cfg.LeaseDuration,
	)

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbCtx, cancel := context.WithTimeout(rootCtx, dbConnectTimeout)
	pool, err := db.NewPool(dbCtx, cfg.DBURL)
	cancel()
	if err != nil {
		return fmt.Errorf("db error: %w", err)
	}
	defer pool.Close()

	return worker.Run(rootCtx, pool, cfg)
}
