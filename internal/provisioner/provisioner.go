package provisioner

import (
	"context"
	"time"
)

type WorkerHandle struct {
	ID    string
	RunID int64
}

type Config struct {
	Image          string
	Network        string
	DBURL          string
	MinIOEndpoint  string
	MinIOAccessKey string
	MinIOSecretKey string
	MinIOBucket    string
	MinIOUseSSL    bool
	Categories     []string
	LeaseDuration  time.Duration
	MaxAttempts    int
}

type Provisioner interface {
	Create(ctx context.Context, runID int64, n int) ([]WorkerHandle, error)
	Destroy(ctx context.Context, handles []WorkerHandle) error
	List(ctx context.Context) ([]WorkerHandle, error)
}
