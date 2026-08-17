package api

import (
	"net/http"
	"time"
)

type responseRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *responseRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

func (s *Server) recordRequest(r *http.Request, rr *responseRecorder, start time.Time) {
	if s.recorder == nil {
		return
	}
	entry := map[string]any{
		"ts":          time.Now().UTC().Format(time.RFC3339),
		"method":      r.Method,
		"path":        r.URL.Path,
		"query":       r.URL.RawQuery,
		"status":      rr.status,
		"bytes":       rr.bytes,
		"duration_ms": float64(time.Since(start).Microseconds()) / 1000.0,
	}
	if uid := userIDFrom(r.Context()); uid != "" {
		entry["user_id"] = uid
	}
	if did := deviceIDFrom(r.Context()); did != "" {
		entry["device_id"] = did
	}
	_ = s.recorder.Record(entry)
}
