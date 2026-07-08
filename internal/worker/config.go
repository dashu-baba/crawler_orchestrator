package worker

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	RunID         int64
	WorkerID      string
	Categories    []string
	DBURL         string
	LeaseDuration time.Duration
}

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

	if len(errs) > 0 {
		return Config{}, errors.Join(errs...)
	}

	return cfg, nil
}

func requireEnv(key string) (string, error) {
	if v, ok := os.LookupEnv(key); ok {
		return v, nil
	}

	return "", fmt.Errorf("missing required env var %s", key)
}

func envOrDefault(key string, fallback string) string {
	val, err := requireEnv(key)
	if err != nil {
		return fallback
	}

	return val
}
