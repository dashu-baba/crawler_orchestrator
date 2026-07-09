package orchestrator

import (
	"context"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SizingParams controls how ComputeSize turns pending backlog into a
// worker count.
type SizingParams struct {
	AvgItemSeconds  float64
	DeadlineSeconds float64
	MinWorkers      int
	MaxWorkers      int
}

// ComputeSize returns how many worker VMs to provision for the current
// pending backlog: x = ceil(max_category_items * avg_item_seconds /
// deadline_seconds), clamped to [MinWorkers, MaxWorkers]. Sizing is driven
// by the category with the largest backlog, since every worker runs one
// goroutine per category (design doc §4.1/§5) and the slowest category
// sets the run's wall-clock time. Returns 0 if there is nothing pending.
func ComputeSize(ctx context.Context, pool *pgxpool.Pool, params SizingParams) (int, error) {
	rows, err := pool.Query(ctx, `SELECT count(*) FROM jobs WHERE status = 'pending' GROUP BY category`)
	if err != nil {
		return 0, fmt.Errorf("counting pending jobs by category: %w", err)
	}
	defer rows.Close()

	var maxCategoryItems int64
	for rows.Next() {
		var count int64
		if err := rows.Scan(&count); err != nil {
			return 0, fmt.Errorf("scanning category count: %w", err)
		}
		if count > maxCategoryItems {
			maxCategoryItems = count
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("reading category counts: %w", err)
	}

	if maxCategoryItems == 0 {
		return 0, nil
	}

	x := int(math.Ceil(float64(maxCategoryItems) * params.AvgItemSeconds / params.DeadlineSeconds))

	if x < params.MinWorkers {
		x = params.MinWorkers
	}
	if x > params.MaxWorkers {
		x = params.MaxWorkers
	}

	return x, nil
}
