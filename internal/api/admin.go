package api

import (
	"bufio"
	"database/sql"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"venera-server/internal/store"
)

func (s *Server) adminStatsHandler(w http.ResponseWriter, r *http.Request) {
	users, _ := s.store.CountUsers()
	mirror, _ := s.store.CountMirror()
	results, _ := s.store.CountResults()
	pending, _ := s.store.CountJobsByState("pending")
	failed, _ := s.store.CountJobsByState("failed")
	needs, _ := s.store.CountJobsByState("needs_resubmit")
	writeJSON(w, http.StatusOK, map[string]any{
		"users":          users,
		"mirror":         mirror,
		"results":        results,
		"pending":        pending,
		"failed":         failed,
		"needs_resubmit": needs,
	})
}

func (s *Server) adminJobsHandler(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.store.ListJobs(200)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "list jobs failed")
		return
	}
	out := make([]map[string]any, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, map[string]any{
			"job_id":        j.JobID,
			"user_id":       j.UserID,
			"source":        j.Source,
			"comic_id":      j.ComicID,
			"state":         j.State,
			"priority":      j.Priority,
			"attempts":      j.Attempts,
			"max_attempts":  j.MaxAttempts,
			"next_retry_at": j.NextRetryAt.String,
			"last_error":    j.LastError.String,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": out})
}

func (s *Server) adminMirrorHandler(w http.ResponseWriter, r *http.Request) {
	mirror, err := s.store.ListAllMirror()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "list mirror failed")
		return
	}
	// 限制返回数量，避免页面过大。
	if len(mirror) > 1000 {
		mirror = mirror[:1000]
	}
	out := make([]map[string]any, 0, len(mirror))
	for _, m := range mirror {
		out = append(out, map[string]any{
			"user_id":         m.UserID,
			"source":          m.Source,
			"comic_id":        m.ComicID,
			"due_at":          m.DueAt,
			"last_check_time": nullStr(m.LastCheckTime),
			"priority":        m.Priority,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"mirror": out})
}

func (s *Server) adminLogsHandler(w http.ResponseWriter, r *http.Request) {
	path := filepath.Join(s.cfg.DataDir, "debug.jsonl")
	logs, err := tailLines(path, 200)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"logs": ""})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"logs": logs})
}

func (s *Server) adminRefreshHandler(w http.ResponseWriter, r *http.Request) {
	mirror, err := s.store.ListAllMirror()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "list mirror failed")
		return
	}
	now := time.Now().UTC().Format(time.RFC3339)
	queued := 0
	for _, m := range mirror {
		active, err := s.store.HasActiveJob(m.UserID, m.Source, m.ComicID)
		if err != nil {
			continue
		}
		if active {
			continue
		}
		_ = s.store.InsertJob(store.Job{
			UserID:      m.UserID,
			Source:      m.Source,
			ComicID:     m.ComicID,
			State:       "pending",
			Priority:    "urgent",
			DueAt:       sql.NullString{String: now, Valid: true},
			ScheduledAt: sql.NullString{String: now, Valid: true},
			CreatedAt:   now,
		})
		queued++
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"queued": queued})
}

// serveAdmin serves the built React app from web/dist with SPA fallback.
func (s *Server) serveAdmin(w http.ResponseWriter, r *http.Request) {
	dist := s.cfg.AdminDistDir
	path := r.URL.Path
	if strings.HasPrefix(path, "/admin/api/") {
		http.NotFound(w, r)
		return
	}
	// 去掉 /admin 前缀，对应 dist 目录。
	if path == "/admin" || path == "/admin/" {
		path = "/index.html"
	} else {
		path = strings.TrimPrefix(path, "/admin/")
	}
	distAbs, err := filepath.Abs(dist)
	if err != nil {
		http.Error(w, "admin dist error", http.StatusInternalServerError)
		return
	}
	full := filepath.Join(distAbs, filepath.Clean(path))
	if full != distAbs && !strings.HasPrefix(full, distAbs+string(filepath.Separator)) {
		http.NotFound(w, r)
		return
	}
	if fi, err := os.Stat(full); err == nil && !fi.IsDir() {
		http.ServeFile(w, r, full)
		return
	}
	// SPA fallback
	index := filepath.Join(distAbs, "index.html")
	if _, err := os.Stat(index); err != nil {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte("<h1>Venera Admin</h1><p>前端未构建：请在 <code>web</code> 目录运行 <code>npm install && npm run build</code></p>"))
		return
	}
	http.ServeFile(w, r, index)
}

func tailLines(path string, n int) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var ring []string
	for sc.Scan() {
		ring = append(ring, sc.Text())
		if len(ring) > n {
			ring = ring[1:]
		}
	}
	if err := sc.Err(); err != nil {
		return "", err
	}
	return strings.Join(ring, "\n"), nil
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
