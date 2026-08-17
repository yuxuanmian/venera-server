package debugrecorder

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Recorder struct {
	mu   sync.Mutex
	file *os.File
}

func New(dataDir string, enabled bool) (*Recorder, error) {
	if !enabled {
		return &Recorder{}, nil
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create debug dir: %w", err)
	}
	path := filepath.Join(dataDir, "debug.jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open debug recorder: %w", err)
	}
	return &Recorder{file: f}, nil
}

func (r *Recorder) Record(entry map[string]any) error {
	if r == nil || r.file == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	line, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = r.file.Write(append(line, '\n'))
	return err
}

func (r *Recorder) Close() error {
	if r == nil || r.file == nil {
		return nil
	}
	return r.file.Close()
}
