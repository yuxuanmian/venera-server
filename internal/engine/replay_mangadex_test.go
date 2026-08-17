package engine

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"venera-server/internal/config"
	"venera-server/internal/debugrecorder"
	"venera-server/internal/store"
)

type replayRecord struct {
	Method           string            `json:"method"`
	URL              string            `json:"url"`
	RequestBody      string            `json:"request_body,omitempty"`
	ResponseStatus   int               `json:"response_status"`
	ResponseHeaders  map[string]string `json:"response_headers,omitempty"`
	ResponseBodyBase string            `json:"response_body_base64,omitempty"`
	ResponseBodyText string            `json:"response_body_text,omitempty"`
}

type replayTransport struct {
	entries map[string]replayRecord
}

func newReplayTransport(path string) (*replayTransport, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	entries := map[string]replayRecord{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1024*1024), 20*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var rec replayRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			return nil, err
		}
		entries[rec.Method+" "+rec.URL] = rec
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return &replayTransport{entries: entries}, nil
}

func (t *replayTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	key := req.Method + " " + req.URL.String()
	rec, ok := t.entries[key]
	if !ok {
		return nil, &replayMissingError{key: key}
	}
	body := []byte(rec.ResponseBodyText)
	if rec.ResponseBodyBase != "" {
		decoded, err := base64.StdEncoding.DecodeString(rec.ResponseBodyBase)
		if err != nil {
			return nil, err
		}
		body = decoded
	}
	header := http.Header{}
	for k, v := range rec.ResponseHeaders {
		header.Set(k, v)
	}
	return &http.Response{
		StatusCode: rec.ResponseStatus,
		Header:     header,
		Body:       io.NopCloser(bytes.NewReader(body)),
		Request:    req,
	}, nil
}

type replayMissingError struct{ key string }

func (e *replayMissingError) Error() string { return "no recording for " + e.key }

func TestEngineRunJobMangaDexReplay(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatalf("abs repo root: %v", err)
	}
	jsonlPath := filepath.Join(repoRoot, "venera-server", "pocdata", "mangadex_3e11f43e-3d91-47c8-a2ad-e2f23d36ad4c.jsonl")
	if _, err := os.Stat(jsonlPath); err != nil {
		t.Skipf("MangaDex fixture not present: %v", err)
	}
	transport, err := newReplayTransport(jsonlPath)
	if err != nil {
		t.Fatalf("load replay: %v", err)
	}

	dataDir := t.TempDir()
	st, err := store.Open(dataDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	initPath := filepath.Join(repoRoot, "venera", "assets", "init.js")
	scriptDir := filepath.Join(repoRoot, "venera-configs")
	cfg := &config.Config{
		DataDir:        dataDir,
		InitJSPath:     initPath,
		ComicSourceDir: scriptDir,
	}
	rec, _ := debugrecorder.New(dataDir, false)
	t.Cleanup(func() { _ = rec.Close() })

	eng, err := New(cfg, st, rec)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	eng.client = &http.Client{Transport: transport}

	user := "u-replay"
	comicID := "3e11f43e-3d91-47c8-a2ad-e2f23d36ad4c"
	now := time.Now().UTC().Format(time.RFC3339)
	if err := st.InsertJob(store.Job{
		UserID:      user,
		Source:      "manga_dex",
		ComicID:     comicID,
		State:       "pending",
		Priority:    "normal",
		DueAt:       sql.NullString{String: now, Valid: true},
		ScheduledAt: sql.NullString{String: now, Valid: true},
		CreatedAt:   now,
	}); err != nil {
		t.Fatalf("insert job: %v", err)
	}
	jobs, err := st.GetPendingJobs(10)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("pending jobs = %d err=%v", len(jobs), err)
	}

	if err := eng.RunJob(t.Context(), jobs[0]); err != nil {
		t.Fatalf("run job: %v", err)
	}

	var state string
	if err := st.DB().QueryRow(`SELECT state FROM jobs WHERE job_id=?`, jobs[0].JobID).Scan(&state); err != nil {
		t.Fatalf("query job state: %v", err)
	}
	if state != "done" {
		t.Fatalf("state = %q, want done", state)
	}

	var outcome, payload string
	if err := st.DB().QueryRow(`SELECT outcome, COALESCE(payload,'') FROM results WHERE user_id=? AND source=? AND comic_id=?`, user, "manga_dex", comicID).Scan(&outcome, &payload); err != nil {
		t.Fatalf("query result: %v", err)
	}
	if outcome != "success" {
		t.Fatalf("outcome = %q, want success", outcome)
	}
	if !strings.Contains(payload, `"Mimi"`) {
		t.Fatalf("payload missing expected title: %s", payload)
	}
	if !strings.Contains(payload, `"230: Stepping On Cats Tail."`) {
		t.Fatalf("payload missing expected chapter: %s", payload)
	}
}

func TestEngineRunJobEhentaiReplay(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatalf("abs repo root: %v", err)
	}
	jsonlPath := filepath.Join(repoRoot, "venera-server", "pocdata", "ehentai_3444864.jsonl")
	if _, err := os.Stat(jsonlPath); err != nil {
		t.Skipf("ehentai fixture not present: %v", err)
	}
	transport, err := newReplayTransport(jsonlPath)
	if err != nil {
		t.Fatalf("load replay: %v", err)
	}

	dataDir := t.TempDir()
	st, err := store.Open(dataDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	initPath := filepath.Join(repoRoot, "venera", "assets", "init.js")
	scriptDir := filepath.Join(repoRoot, "venera-configs")
	cfg := &config.Config{
		DataDir:        dataDir,
		InitJSPath:     initPath,
		ComicSourceDir: scriptDir,
	}
	rec, _ := debugrecorder.New(dataDir, false)
	t.Cleanup(func() { _ = rec.Close() })

	eng, err := New(cfg, st, rec)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	eng.client = &http.Client{Transport: transport}

	user := "u-ehentai"
	comicID := "https://exhentai.org/g/3444864/94f80c44f8/"
	now := time.Now().UTC().Format(time.RFC3339)
	if err := st.InsertJob(store.Job{
		UserID:      user,
		Source:      "ehentai",
		ComicID:     comicID,
		State:       "pending",
		Priority:    "normal",
		DueAt:       sql.NullString{String: now, Valid: true},
		ScheduledAt: sql.NullString{String: now, Valid: true},
		CreatedAt:   now,
	}); err != nil {
		t.Fatalf("insert job: %v", err)
	}
	jobs, err := st.GetPendingJobs(10)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("pending jobs = %d err=%v", len(jobs), err)
	}

	if err := eng.RunJob(t.Context(), jobs[0]); err != nil {
		t.Fatalf("run job: %v", err)
	}

	var state string
	if err := st.DB().QueryRow(`SELECT state FROM jobs WHERE job_id=?`, jobs[0].JobID).Scan(&state); err != nil {
		t.Fatalf("query job state: %v", err)
	}
	if state != "done" {
		t.Fatalf("state = %q, want done", state)
	}

	var outcome, payload string
	if err := st.DB().QueryRow(`SELECT outcome, COALESCE(payload,'') FROM results WHERE user_id=? AND source=? AND comic_id=?`, user, "ehentai", comicID).Scan(&outcome, &payload); err != nil {
		t.Fatalf("query result: %v", err)
	}
	if outcome != "success" {
		t.Fatalf("outcome = %q, want success", outcome)
	}
	if !strings.Contains(payload, `Fuwafuwa Slim`) {
		t.Fatalf("payload missing expected title: %s", payload)
	}
}
