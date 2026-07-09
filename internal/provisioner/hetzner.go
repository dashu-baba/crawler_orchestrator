package provisioner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const hetznerAPIBase = "https://api.hetzner.cloud/v1"

// HetznerConfig holds everything HetznerProvisioner needs: API access, the
// server shape to boot from the pre-baked snapshot (doc/orchestrator-design.md
// §4.2), and the env vars each worker needs, which get baked into the VM's
// cloud-init user_data.
type HetznerConfig struct {
	APIToken   string
	ServerType string   // e.g. "cx22"
	Image      string   // pre-baked snapshot ID or name containing the worker binary
	Location   string   // e.g. "nbg1"; empty lets Hetzner pick
	SSHKeys    []string // key names/IDs to install for operator access

	// PrivateNetworkID attaches every worker to this Hetzner private
	// network (in addition to its normal public IP, which workers keep
	// for crawl IP-diversity -- see doc/orchestrator-design.md §4.2). It
	// lets workers reach Postgres/Object Storage over the private
	// network instead of the public internet, so those services never
	// need a public-facing DB port. Zero means "no private network".
	PrivateNetworkID int64

	// TTLBuffer is added to DeadlineSeconds to compute each VM's
	// self-destruct time -- the belt-and-suspenders guardrail against an
	// orchestrator crash leaving VMs running (CLAUDE.md cost guardrails).
	DeadlineSeconds float64
	TTLBuffer       time.Duration

	// Passed through to the worker process via cloud-init user_data.
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

type HetznerProvisioner struct {
	cfg    HetznerConfig
	client *http.Client
}

func NewHetznerProvisioner(cfg HetznerConfig) *HetznerProvisioner {
	return &HetznerProvisioner{
		cfg:    cfg,
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

// hetznerServer mirrors the subset of the Hetzner API's server object this
// package needs.
type hetznerServer struct {
	ID     int64             `json:"id"`
	Labels map[string]string `json:"labels"`
}

type hetznerErrorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// do calls the Hetzner API and decodes the response into out (if non-nil).
// It returns the HTTP status code alongside any error so callers can
// distinguish "already gone" (404) from a real failure, the way Destroy
// needs to.
func (p *HetznerProvisioner) do(ctx context.Context, method, path string, body any, out any) (int, error) {
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, fmt.Errorf("marshaling request body: %w", err)
		}
		reqBody = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, hetznerAPIBase+path, reqBody)
	if err != nil {
		return 0, fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+p.cfg.APIToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("calling %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, fmt.Errorf("reading response body for %s %s: %w", method, path, err)
	}

	if resp.StatusCode >= 300 {
		var apiErr hetznerErrorBody
		if jsonErr := json.Unmarshal(respBody, &apiErr); jsonErr == nil && apiErr.Error.Code != "" {
			return resp.StatusCode, fmt.Errorf("%s %s: %s: %s", method, path, apiErr.Error.Code, apiErr.Error.Message)
		}
		return resp.StatusCode, fmt.Errorf("%s %s: unexpected status %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	if out != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, out); err != nil {
			return resp.StatusCode, fmt.Errorf("decoding response for %s %s: %w", method, path, err)
		}
	}

	return resp.StatusCode, nil
}

// imageParam renders the value for the Hetzner "image" field. A pre-baked
// snapshot is referenced by its numeric ID, which the API expects as a JSON
// integer -- a quoted string is rejected. A stock system image (e.g.
// "ubuntu-24.04") is referenced by name, a string. So: send an int when the
// configured image is all digits, otherwise a string.
func imageParam(image string) any {
	if id, err := strconv.ParseInt(image, 10, 64); err == nil {
		return id
	}
	return image
}

// Create boots n worker servers for runID from the pre-baked snapshot in
// cfg.Image, tagged role=worker/run_id=<runID> so List/Reconcile can find
// them later. Each VM gets its own cloud-init user_data embedding the env
// vars the worker binary needs plus a self-destruct timer. If any server
// fails to create, the ones already created are rolled back before
// returning, mirroring DockerProvisioner.Create.
func (p *HetznerProvisioner) Create(ctx context.Context, runID int64, n int) ([]WorkerHandle, error) {
	created := make([]WorkerHandle, 0, n)

	for i := 0; i < n; i++ {
		workerID := uuid.New().String()
		name := fmt.Sprintf("worker-%d-%s", runID, workerID)

		userData, err := p.cloudInit(runID, workerID)
		if err != nil {
			return nil, fmt.Errorf("building cloud-init for worker %d/%d: %w", i+1, n, err)
		}

		reqBody := map[string]any{
			"name":        name,
			"server_type": p.cfg.ServerType,
			"image":       imageParam(p.cfg.Image),
			"user_data":   userData,
			"labels": map[string]string{
				"role":   "worker",
				"run_id": strconv.FormatInt(runID, 10),
			},
		}
		if p.cfg.Location != "" {
			reqBody["location"] = p.cfg.Location
		}
		if len(p.cfg.SSHKeys) > 0 {
			reqBody["ssh_keys"] = p.cfg.SSHKeys
		}
		if p.cfg.PrivateNetworkID != 0 {
			reqBody["networks"] = []int64{p.cfg.PrivateNetworkID}
		}

		var out struct {
			Server hetznerServer `json:"server"`
		}

		if _, err := p.do(ctx, http.MethodPost, "/servers", reqBody, &out); err != nil {
			createErr := fmt.Errorf("creating worker %d/%d: %w", i+1, n, err)
			if len(created) == 0 {
				return nil, createErr
			}
			if rollbackErr := p.Destroy(ctx, created); rollbackErr != nil {
				return nil, errors.Join(
					createErr,
					fmt.Errorf("rollback also failed, %d server(s) may be orphaned: %w", len(created), rollbackErr),
				)
			}
			return nil, createErr
		}

		created = append(created, WorkerHandle{ID: strconv.FormatInt(out.Server.ID, 10), RunID: runID})
	}

	return created, nil
}

// Destroy deletes every handle, best-effort: it keeps going even if some
// fail and returns a joined error only after attempting all of them.
// Deleting an already-gone server (404) is treated as success, so Destroy
// is safe to call more than once for the same handle -- this matters
// because Reconcile and a VM's own cloud-init self-destruct timer can
// both end up trying to delete the same server.
func (p *HetznerProvisioner) Destroy(ctx context.Context, handles []WorkerHandle) error {
	var errs []error

	for _, h := range handles {
		status, err := p.do(ctx, http.MethodDelete, "/servers/"+h.ID, nil, nil)
		if err != nil && status != http.StatusNotFound {
			errs = append(errs, fmt.Errorf("destroying server %s: %w", h.ID, err))
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

// List returns every server currently tagged role=worker, across all
// pages -- this is what makes reconciliation possible: the caller
// compares each handle's RunID against runs.status and destroys any whose
// run is done or absent.
func (p *HetznerProvisioner) List(ctx context.Context) ([]WorkerHandle, error) {
	var handles []WorkerHandle
	page := 1

	for {
		var out struct {
			Servers []hetznerServer `json:"servers"`
			Meta    struct {
				Pagination struct {
					NextPage int `json:"next_page"`
				} `json:"pagination"`
			} `json:"meta"`
		}

		path := "/servers?label_selector=" + url.QueryEscape("role=worker") + "&page=" + strconv.Itoa(page) + "&per_page=50"
		if _, err := p.do(ctx, http.MethodGet, path, nil, &out); err != nil {
			return nil, fmt.Errorf("listing worker servers (page %d): %w", page, err)
		}

		for _, s := range out.Servers {
			runID, err := strconv.ParseInt(s.Labels["run_id"], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("parsing run_id label %q on server %d: %w", s.Labels["run_id"], s.ID, err)
			}
			handles = append(handles, WorkerHandle{ID: strconv.FormatInt(s.ID, 10), RunID: runID})
		}

		if out.Meta.Pagination.NextPage == 0 {
			break
		}
		page = out.Meta.Pagination.NextPage
	}

	return handles, nil
}

// cloudInit renders the user_data script a worker VM boots with: the env
// vars the worker binary needs, a systemd unit to run it, and a
// self-destruct timer at deadline+buffer. The timer calls the Hetzner API
// directly (using the VM's own metadata service to learn its server ID)
// rather than merely shutting down, because Hetzner bills a stopped
// server the same as a running one -- only deletion stops the meter
// (CLAUDE.md cost guardrails). Embedding a write-scoped API token in
// every worker's user_data is a real tradeoff (a compromised worker can
// call the whole project's API); accepted here because workers are
// short-lived and the Hetzner Cloud API has no finer-grained token
// scoping today.
func (p *HetznerProvisioner) cloudInit(runID int64, workerID string) (string, error) {
	ttlSeconds := int64(p.cfg.DeadlineSeconds) + int64(p.cfg.TTLBuffer.Seconds())
	if ttlSeconds <= 0 {
		return "", errors.New("computed TTL is <= 0; check DeadlineSeconds/TTLBuffer")
	}

	env := map[string]string{
		"RUN_ID":           strconv.FormatInt(runID, 10),
		"WORKER_ID":        workerID,
		"CATEGORIES":       strings.Join(p.cfg.Categories, ","),
		"DB_URL":           p.cfg.DBURL,
		"LEASE_DURATION":   p.cfg.LeaseDuration.String(),
		"MAX_ATTEMPTS":     strconv.Itoa(p.cfg.MaxAttempts),
		"MINIO_ENDPOINT":   p.cfg.MinIOEndpoint,
		"MINIO_ACCESS_KEY": p.cfg.MinIOAccessKey,
		"MINIO_SECRET_KEY": p.cfg.MinIOSecretKey,
		"MINIO_BUCKET":     p.cfg.MinIOBucket,
		"MINIO_USE_SSL":    strconv.FormatBool(p.cfg.MinIOUseSSL),
	}

	var envLines strings.Builder
	for k, v := range env {
		fmt.Fprintf(&envLines, "      %s=%s\n", k, v)
	}

	script := fmt.Sprintf(`#cloud-config
write_files:
  - path: /etc/worker.env
    permissions: '0600'
    content: |
%s  - path: /etc/systemd/system/worker.service
    content: |
      [Unit]
      Description=crawler worker
      After=network-online.target
      Wants=network-online.target

      [Service]
      EnvironmentFile=/etc/worker.env
      ExecStart=/usr/local/bin/worker
      Restart=on-failure

      [Install]
      WantedBy=multi-user.target
  - path: /usr/local/bin/self-destruct.sh
    permissions: '0700'
    content: |
      #!/bin/sh
      set -eu
      SERVER_ID=$(curl -s http://169.254.169.254/hetzner/v1/metadata/instance-id)
      curl -s -X DELETE \
        -H "Authorization: Bearer %s" \
        "https://api.hetzner.cloud/v1/servers/$SERVER_ID"

runcmd:
  - systemctl daemon-reload
  - systemctl enable --now worker.service
  - systemd-run --on-active=%ds --unit=self-destruct.service /usr/local/bin/self-destruct.sh
`, envLines.String(), p.cfg.APIToken, ttlSeconds)

	return script, nil
}
