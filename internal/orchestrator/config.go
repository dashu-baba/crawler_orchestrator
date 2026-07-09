package orchestrator

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dashu-baba/crawler-orchestrator/internal/provisioner"
)

// Config holds the orchestrator's startup configuration, loaded once from
// env vars.
type Config struct {
	DBURL           string
	DeadlineSeconds float64
	AvgItemSeconds  float64
	MinWorkers      int
	MaxWorkers      int
	MonitorInterval time.Duration

	// ProvisionerKind selects which Provisioner cmd/orchestrator wires up:
	// "docker" (local dev, no cloud spend) or "hetzner" (prod).
	ProvisionerKind string
	Provisioner     provisioner.Config
	Hetzner         provisioner.HetznerConfig
}

// Load reads and validates a Config from env vars, accumulating every
// validation failure rather than stopping at the first one, so a
// misconfigured orchestrator surfaces all of its problems in one go.
func Load() (Config, error) {
	var errs []error
	cfg := Config{}

	if dbURL, err := requireEnv("DB_URL"); err != nil {
		errs = append(errs, err)
	} else {
		cfg.DBURL = dbURL
		cfg.Provisioner.DBURL = dbURL
		cfg.Hetzner.DBURL = dbURL
	}

	provisionerKindVal := envOrDefault("PROVISIONER", "docker")
	switch provisionerKindVal {
	case "docker", "hetzner":
		cfg.ProvisionerKind = provisionerKindVal
	default:
		errs = append(errs, fmt.Errorf("PROVISIONER must be \"docker\" or \"hetzner\", got %q", provisionerKindVal))
	}

	if deadline, err := parseFloatEnv("DEADLINE_SECONDS", "14400"); err != nil {
		errs = append(errs, err)
	} else if deadline <= 0 {
		errs = append(errs, errors.New("DEADLINE_SECONDS must be > 0"))
	} else {
		cfg.DeadlineSeconds = deadline
		cfg.Hetzner.DeadlineSeconds = deadline
	}

	if avgItem, err := parseFloatEnv("AVG_ITEM_SECONDS", "1"); err != nil {
		errs = append(errs, err)
	} else if avgItem <= 0 {
		errs = append(errs, errors.New("AVG_ITEM_SECONDS must be > 0"))
	} else {
		cfg.AvgItemSeconds = avgItem
	}

	if minWorkers, err := parseIntEnv("MIN_WORKERS", "1"); err != nil {
		errs = append(errs, err)
	} else if minWorkers <= 0 {
		errs = append(errs, errors.New("MIN_WORKERS must be > 0"))
	} else {
		cfg.MinWorkers = minWorkers
	}

	if maxWorkers, err := parseIntEnv("MAX_WORKERS", "20"); err != nil {
		errs = append(errs, err)
	} else if maxWorkers <= 0 {
		errs = append(errs, errors.New("MAX_WORKERS must be > 0"))
	} else {
		cfg.MaxWorkers = maxWorkers
	}

	if cfg.MinWorkers > 0 && cfg.MaxWorkers > 0 && cfg.MinWorkers > cfg.MaxWorkers {
		errs = append(errs, fmt.Errorf("MIN_WORKERS (%d) can't exceed MAX_WORKERS (%d)", cfg.MinWorkers, cfg.MaxWorkers))
	}

	monitorIntervalVal := envOrDefault("MONITOR_INTERVAL", "15s")
	if monitorInterval, err := time.ParseDuration(monitorIntervalVal); err != nil {
		errs = append(errs, fmt.Errorf("parsing MONITOR_INTERVAL: %w", err))
	} else if monitorInterval <= 0 {
		errs = append(errs, errors.New("MONITOR_INTERVAL must be > 0"))
	} else {
		cfg.MonitorInterval = monitorInterval
	}

	if cfg.ProvisionerKind == "docker" {
		if image, err := requireEnv("WORKER_IMAGE"); err != nil {
			errs = append(errs, err)
		} else {
			cfg.Provisioner.Image = image
		}

		if network, err := requireEnv("WORKER_NETWORK"); err != nil {
			errs = append(errs, err)
		} else {
			cfg.Provisioner.Network = network
		}
	}

	if cfg.ProvisionerKind == "hetzner" {
		if token, err := requireEnv("HETZNER_API_TOKEN"); err != nil {
			errs = append(errs, err)
		} else {
			cfg.Hetzner.APIToken = token
		}

		if serverType, err := requireEnv("HETZNER_SERVER_TYPE"); err != nil {
			errs = append(errs, err)
		} else {
			cfg.Hetzner.ServerType = serverType
		}

		if image, err := requireEnv("HETZNER_IMAGE"); err != nil {
			errs = append(errs, err)
		} else {
			cfg.Hetzner.Image = image
		}

		// Location is optional -- an empty string lets Hetzner pick.
		cfg.Hetzner.Location = envOrDefault("HETZNER_LOCATION", "")

		if sshKeysVal := envOrDefault("HETZNER_SSH_KEYS", ""); sshKeysVal != "" {
			parts := strings.Split(sshKeysVal, ",")
			keys := make([]string, 0, len(parts))
			for _, k := range parts {
				if k = strings.TrimSpace(k); k != "" {
					keys = append(keys, k)
				}
			}
			cfg.Hetzner.SSHKeys = keys
		}

		ttlBufferVal := envOrDefault("HETZNER_TTL_BUFFER", "300s")
		if ttlBuffer, err := time.ParseDuration(ttlBufferVal); err != nil {
			errs = append(errs, fmt.Errorf("parsing HETZNER_TTL_BUFFER: %w", err))
		} else if ttlBuffer < 0 {
			errs = append(errs, errors.New("HETZNER_TTL_BUFFER can't be negative"))
		} else {
			cfg.Hetzner.TTLBuffer = ttlBuffer
		}
	}

	if categoriesVal, err := requireEnv("CATEGORIES"); err != nil {
		errs = append(errs, err)
	} else {
		parts := strings.Split(categoriesVal, ",")
		categories := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			categories = append(categories, p)
		}

		if len(categories) == 0 {
			errs = append(errs, errors.New("CATEGORIES must contain at least one category"))
		} else {
			cfg.Provisioner.Categories = categories
			cfg.Hetzner.Categories = categories
		}
	}

	leaseDurationVal := envOrDefault("LEASE_DURATION", "90s")
	if leaseDuration, err := time.ParseDuration(leaseDurationVal); err != nil {
		errs = append(errs, fmt.Errorf("parsing LEASE_DURATION: %w", err))
	} else if leaseDuration <= 0 {
		errs = append(errs, errors.New("lease duration can't be less than 1"))
	} else {
		cfg.Provisioner.LeaseDuration = leaseDuration
		cfg.Hetzner.LeaseDuration = leaseDuration
	}

	if maxAttempts, err := parseIntEnv("MAX_ATTEMPTS", "5"); err != nil {
		errs = append(errs, err)
	} else if maxAttempts <= 0 {
		errs = append(errs, errors.New("MAX_ATTEMPTS can't be less than 1"))
	} else {
		cfg.Provisioner.MaxAttempts = maxAttempts
		cfg.Hetzner.MaxAttempts = maxAttempts
	}

	if minioEndpoint, err := requireEnv("MINIO_ENDPOINT"); err != nil {
		errs = append(errs, err)
	} else {
		cfg.Provisioner.MinIOEndpoint = minioEndpoint
		cfg.Hetzner.MinIOEndpoint = minioEndpoint
	}

	if minioAccessKey, err := requireEnv("MINIO_ACCESS_KEY"); err != nil {
		errs = append(errs, err)
	} else {
		cfg.Provisioner.MinIOAccessKey = minioAccessKey
		cfg.Hetzner.MinIOAccessKey = minioAccessKey
	}

	if minioSecretKey, err := requireEnv("MINIO_SECRET_KEY"); err != nil {
		errs = append(errs, err)
	} else {
		cfg.Provisioner.MinIOSecretKey = minioSecretKey
		cfg.Hetzner.MinIOSecretKey = minioSecretKey
	}

	if minioBucket, err := requireEnv("MINIO_BUCKET"); err != nil {
		errs = append(errs, err)
	} else {
		cfg.Provisioner.MinIOBucket = minioBucket
		cfg.Hetzner.MinIOBucket = minioBucket
	}

	minioUseSSLVal := envOrDefault("MINIO_USE_SSL", "false")
	if minioUseSSL, err := strconv.ParseBool(minioUseSSLVal); err != nil {
		errs = append(errs, fmt.Errorf("parsing MINIO_USE_SSL: %w", err))
	} else {
		cfg.Provisioner.MinIOUseSSL = minioUseSSL
		cfg.Hetzner.MinIOUseSSL = minioUseSSL
	}

	if len(errs) > 0 {
		return Config{}, errors.Join(errs...)
	}

	return cfg, nil
}

// requireEnv returns an error if key is unset or empty, distinguishing
// "unset" from "set to empty string" the way os.Getenv alone cannot.
func requireEnv(key string) (string, error) {
	if v, ok := os.LookupEnv(key); ok {
		return v, nil
	}

	return "", fmt.Errorf("missing required env var %s", key)
}

// envOrDefault returns the value of key, or fallback if key is unset/empty.
func envOrDefault(key string, fallback string) string {
	val, err := requireEnv(key)
	if err != nil {
		return fallback
	}

	return val
}

func parseIntEnv(key, fallback string) (int, error) {
	v, err := strconv.Atoi(envOrDefault(key, fallback))
	if err != nil {
		return 0, fmt.Errorf("parsing %s: %w", key, err)
	}

	return v, nil
}

func parseFloatEnv(key, fallback string) (float64, error) {
	v, err := strconv.ParseFloat(envOrDefault(key, fallback), 64)
	if err != nil {
		return 0, fmt.Errorf("parsing %s: %w", key, err)
	}

	return v, nil
}
