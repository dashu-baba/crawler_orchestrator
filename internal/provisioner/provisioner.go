package provisioner

import "context"

type WorkerHandle struct {
	ID    string
	RunID int64
}

type Provisioner interface {
	Create(ctx context.Context, runID int64, n int) ([]WorkerHandle, error)
	Destroy(ctx context.Context, handles []WorkerHandle) error
	List(ctx context.Context) ([]WorkerHandle, error)
}
