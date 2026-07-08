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
	"github.com/dashu-baba/crawler-orchestrator/internal/store"
	"github.com/dashu-baba/crawler-orchestrator/internal/worker"
)

const connectTimeout = 10 * time.Second

func main() {
	if err := run(); err != nil {
		slog.Error("Worker exited", "error", err)
		os.Exit(1)
	}
}

// run loads config, connects to Postgres and MinIO, and runs the claim
// loop until it's cancelled by SIGINT/SIGTERM or fails. Kept separate from
// main so every defer (pool.Close, etc.) always runs before exit — main
// only ever calls os.Exit once, after run has already returned.
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

	dbCtx, cancel := context.WithTimeout(rootCtx, connectTimeout)
	pool, err := db.NewPool(dbCtx, cfg.DBURL)
	cancel()
	if err != nil {
		return fmt.Errorf("db error: %w", err)
	}
	defer pool.Close()

	storeCtx, cancel := context.WithTimeout(rootCtx, connectTimeout)
	minioClient, err := store.NewClient(storeCtx, cfg.MinIOEndpoint, cfg.MinIOAccessKey, cfg.MinIOSecretKey, cfg.MinIOBucket, cfg.MinIOUseSSL)
	cancel()
	if err != nil {
		return fmt.Errorf("store error: %w", err)
	}

	return worker.Run(rootCtx, pool, minioClient, cfg)
}
