package scan

import (
	"bytes"
	"encoding/json"
	"errors"
)

var ErrCheckpointInvalid = errors.New("scanner checkpoint is invalid")

// ValidateCheckpoint keeps checkpoint data opaque to the host while enforcing
// the size and JSON-object boundary needed for safe resumable scans.
func ValidateCheckpoint(raw []byte, maxBytes int) error {
	if maxBytes <= 0 {
		maxBytes = DefaultSnapshotMaxCheckpoint
	}
	checkpoint := bytes.TrimSpace(raw)
	if len(checkpoint) == 0 || bytes.Equal(checkpoint, []byte("null")) || len(checkpoint) > maxBytes || !json.Valid(checkpoint) {
		return ErrCheckpointInvalid
	}
	var value map[string]json.RawMessage
	if err := json.Unmarshal(checkpoint, &value); err != nil || value == nil {
		return ErrCheckpointInvalid
	}
	return nil
}
