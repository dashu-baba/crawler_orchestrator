package worker

import "encoding/json"

// Job mirrors the columns returned by Claim's RETURNING clause.
type Job struct {
	ID        int64
	URL       string
	ConfigURI *string
	Config    json.RawMessage
	IdemKey   string
	Attempts  int32
}
