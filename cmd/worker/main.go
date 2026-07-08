package main

import (
	"log/slog"
	"os"

	"github.com/dashu-baba/crawler-orchestrator/internal/worker"
)

func main() {
	cfg, err := worker.Load()
	if err != nil {
		slog.Error("loading config", "error", err)
		os.Exit(1)
	}

	slog.Info("worker config loaded",
		"run_id", cfg.RunID,
		"worker_id", cfg.WorkerID,
		"categories", cfg.Categories,
		"lease_duration", cfg.LeaseDuration,
	)
}
