package worker

import (
	"context"
	"encoding/json"
	"time"
)

const fakeProcessDuration = 200 * time.Millisecond

// result is the dummy payload written to object storage in place of a
// real crawl/transform result.
type result struct {
	IdemKey     string    `json:"idem_key"`
	URL         string    `json:"url"`
	ProcessedAt time.Time `json:"processed_at"`
}

// process simulates doing the real crawl/transform work: it does nothing
// but sleep briefly and build a placeholder payload. It always succeeds.
func process(ctx context.Context, job *Job) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(fakeProcessDuration):
	}

	return json.Marshal(result{
		IdemKey:     job.IdemKey,
		URL:         job.URL,
		ProcessedAt: time.Now().UTC(),
	})
}
