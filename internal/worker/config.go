package worker

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds a worker's startup configuration, loaded once from env vars.
type Config struct {
	RunID          int64
	WorkerID       string
	Categories     []string
	DBURL          string
	LeaseDuration  time.Duration
	MinIOEndpoint  string
	MinIOAccessKey string
	MinIOSecretKey string
	MinIOBucket    string
	MinIOUseSSL    bool
}

// Load reads and validates a Config from env vars, accumulating every
// validation failure rather than stopping at the first one, so a
// misconfigured worker surfaces all of its problems in one go instead of
// one redeploy at a time.
func Load() (Config, error) {
	var errs []error
	cfg := Config{}

	if runIDVal, err := requireEnv("RUN_ID"); err != nil {
		errs = append(errs, err)
	} else if runID, perr := strconv.ParseInt(runIDVal, 10, 64); perr != nil {
		errs = append(errs, fmt.Errorf("parsing RUN_ID: %w", perr))
	} else {
		cfg.RunID = runID
	}

	if workerID, err := requireEnv("WORKER_ID"); err != nil {
		errs = append(errs, err)
	} else {
		cfg.WorkerID = workerID
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
			cfg.Categories = categories
		}
	}

	if dbURL, err := requireEnv("DB_URL"); err != nil {
		errs = append(errs, err)
	} else {
		cfg.DBURL = dbURL
	}

	leaseDurationVal := envOrDefault("LEASE_DURATION", "90s")
	if leaseDuration, err := time.ParseDuration(leaseDurationVal); err != nil {
		errs = append(errs, fmt.Errorf("parsing LEASE_DURATION: %w", err))
	} else if leaseDuration <= 0 {
		errs = append(errs, errors.New("lease duration can't be less than 1"))
	} else {
		cfg.LeaseDuration = leaseDuration
	}

	if minioEndpoint, err := requireEnv("MINIO_ENDPOINT"); err != nil {
		errs = append(errs, err)
	} else {
		cfg.MinIOEndpoint = minioEndpoint
	}

	if minioAccessKey, err := requireEnv("MINIO_ACCESS_KEY"); err != nil {
		errs = append(errs, err)
	} else {
		cfg.MinIOAccessKey = minioAccessKey
	}

	if minioSecretKey, err := requireEnv("MINIO_SECRET_KEY"); err != nil {
		errs = append(errs, err)
	} else {
		cfg.MinIOSecretKey = minioSecretKey
	}

	if minioBucket, err := requireEnv("MINIO_BUCKET"); err != nil {
		errs = append(errs, err)
	} else {
		cfg.MinIOBucket = minioBucket
	}

	minioUseSSLVal := envOrDefault("MINIO_USE_SSL", "false")
	if minioUseSSL, err := strconv.ParseBool(minioUseSSLVal); err != nil {
		errs = append(errs, fmt.Errorf("parsing MINIO_USE_SSL: %w", err))
	} else {
		cfg.MinIOUseSSL = minioUseSSL
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
