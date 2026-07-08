package worker

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

/*
 */
type Config struct {
	RunID         int64
	WorkerID      string
	Categories    []string
	DBURL         string
	LeaseDuration time.Duration
}

func Load() (Config, error) {
	var errs []error
	runIdVal, err := requireEnv("RUN_ID")
	if err != nil {
		errs = append(errs, err)
	}

	runId, err := strconv.ParseInt(runIdVal, 10, 64)
	if err != nil {
		errs = append(errs, fmt.Errorf("parsing RUN_ID: %w", err))
	}

	workerId, err := requireEnv("WORKER_ID")
	if err != nil {
		errs = append(errs, err)
	}

	categoriesVal, err := requireEnv("CATEGORIES")
	if err != nil {
		errs = append(errs, err)
	}

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
	}

	dbUrl, err := requireEnv("DB_URL")
	if err != nil {
		errs = append(errs, err)
	}

	leaseDurationVal := envOrDefault("LEASE_DURATION", "90s")
	leaseDuration, err := time.ParseDuration(leaseDurationVal)
	if err != nil {
		errs = append(errs, fmt.Errorf("parsing LEASE_DURATION: %w", err))
	}
	if leaseDuration <= 0 {
		errs = append(errs, errors.New("lease duration can't be less than 1"))
	}

	if len(errs) > 0 {
		return Config{}, errors.Join(errs...)
	}

	return Config{
		RunID:         runId,
		WorkerID:      workerId,
		Categories:    categories,
		DBURL:         dbUrl,
		LeaseDuration: leaseDuration,
	}, nil
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
