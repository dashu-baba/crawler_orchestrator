package provisioner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

type DockerProvisioner struct {
	cfg Config
}

func NewDockerProvisioner(cfg Config) *DockerProvisioner {
	return &DockerProvisioner{
		cfg: cfg,
	}
}

// Create starts n worker containers for runID. If any container fails to
// start, the ones already created are rolled back (destroyed) before
// returning, so callers never have to reason about a partially-created
// batch.
func (p *DockerProvisioner) Create(ctx context.Context, runID int64, n int) ([]WorkerHandle, error) {
	created := make([]WorkerHandle, 0, n)

	for i := 0; i < n; i++ {
		workerID := uuid.New().String()
		args := []string{
			"run", "-d",
			"--network", p.cfg.Network,
			"--label", "role=worker",
			"--label", fmt.Sprintf("run_id=%d", runID),
			"-e", fmt.Sprintf("RUN_ID=%d", runID),
			"-e", "WORKER_ID=" + workerID,
			"-e", "CATEGORIES=" + strings.Join(p.cfg.Categories, ","),
			"-e", "DB_URL=" + p.cfg.DBURL,
			"-e", fmt.Sprintf("LEASE_DURATION=%s", p.cfg.LeaseDuration),
			"-e", fmt.Sprintf("MAX_ATTEMPTS=%d", p.cfg.MaxAttempts),
			"-e", "MINIO_ENDPOINT=" + p.cfg.MinIOEndpoint,
			"-e", "MINIO_ACCESS_KEY=" + p.cfg.MinIOAccessKey,
			"-e", "MINIO_SECRET_KEY=" + p.cfg.MinIOSecretKey,
			"-e", "MINIO_BUCKET=" + p.cfg.MinIOBucket,
			"-e", fmt.Sprintf("MINIO_USE_SSL=%t", p.cfg.MinIOUseSSL),
			p.cfg.Image,
		}

		cmd := exec.CommandContext(ctx, "docker", args...)
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr

		if err := cmd.Run(); err != nil {
			createErr := fmt.Errorf("creating worker %d/%d: %w: %s", i+1, n, err, strings.TrimSpace(stderr.String()))
			if len(created) == 0 {
				return nil, createErr
			}
			if rollbackErr := p.Destroy(ctx, created); rollbackErr != nil {
				return nil, errors.Join(
					createErr,
					fmt.Errorf("rollback also failed, %d container(s) may be orphaned: %w", len(created), rollbackErr),
				)
			}
			return nil, createErr
		}

		containerID := strings.TrimSpace(stdout.String())
		created = append(created, WorkerHandle{ID: containerID, RunID: runID})
	}

	return created, nil
}

// Destroy removes every handle, best-effort: it keeps going even if some
// handles fail, and returns a joined error only after attempting all of
// them. Destroying an already-gone container is treated as success, not
// an error, so Destroy is safe to call more than once for the same
// handle.
func (p *DockerProvisioner) Destroy(ctx context.Context, handles []WorkerHandle) error {
	var errs []error

	for _, h := range handles {
		cmd := exec.CommandContext(ctx, "docker", "rm", "-f", h.ID)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr

		if err := cmd.Run(); err != nil {
			if strings.Contains(stderr.String(), "No such container") {
				continue
			}
			errs = append(errs, fmt.Errorf("destroying %s: %w: %s", h.ID, err, strings.TrimSpace(stderr.String())))
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

// List returns every container currently tagged role=worker, regardless
// of run, including stopped ones -- this is what makes reconciliation
// possible: the caller compares each handle's RunID against runs.status
// and destroys any whose run is done or absent.
func (p *DockerProvisioner) List(ctx context.Context) ([]WorkerHandle, error) {
	cmd := exec.CommandContext(ctx, "docker", "ps", "-a", "--no-trunc",
		"--filter", "label=role=worker",
		"--format", `{{.ID}}\t{{.Label "run_id"}}`,
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("listing worker containers: %w: %s", err, strings.TrimSpace(stderr.String()))
	}

	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	handles := make([]WorkerHandle, 0, len(lines))

	for _, line := range lines {
		if line == "" {
			continue
		}

		fields := strings.Split(line, "\t")
		if len(fields) != 2 {
			return nil, fmt.Errorf("unexpected docker ps output line: %q", line)
		}

		runID, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing run_id label %q: %w", fields[1], err)
		}

		handles = append(handles, WorkerHandle{ID: fields[0], RunID: runID})
	}

	return handles, nil
}
