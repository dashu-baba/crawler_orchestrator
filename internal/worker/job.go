package worker

import "encoding/json"

type Job struct {
	ID        int64
	URL       string
	ConfigURI *string
	Config    json.RawMessage
	IdemKey   string
	Attempts  int32
}
