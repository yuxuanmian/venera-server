package engine

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"venera-server/internal/config"
	"venera-server/internal/debugrecorder"
	"venera-server/internal/store"
)

func newTestEngine(t *testing.T, script string) (*Engine, *store.Store, string) {
	t.Helper()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	scriptDir := filepath.Join(dataDir, "scripts")
	if err := os.MkdirAll(scriptDir, 0o755); err != nil {
		t.Fatalf("mkdir script dir: %v", err)
	}
	scriptPath := filepath.Join(scriptDir, "testsrc.js")
	if err := os.WriteFile(scriptPath, []byte(script), 0o644); err != nil {
		t.Fatalf("write script: %v", err)
	}

	initPath := filepath.Join("..", "..", "..", "venera", "assets", "init.js")
	absInit, err := filepath.Abs(initPath)
	if err != nil {
		t.Fatalf("abs init: %v", err)
	}

	cfg := &config.Config{
		DataDir:        dataDir,
		InitJSPath:     absInit,
		ComicSourceDir: scriptDir,
	}
	rec, _ := debugrecorder.New(dataDir, false)
	t.Cleanup(func() { _ = rec.Close() })

	eng, err := New(cfg, st, rec)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	return eng, st, dataDir
}

func TestEngineRunJobSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"title":"Engine Test","cover":"http://cover","updateTime":"2026-07-20","chapters":{"1":"1"}}`)
	}))
	defer srv.Close()

	script := fmt.Sprintf(`class TestEngineSource extends ComicSource {
		name = "TestEngineSource"
		key = "testsrc"
		version = "1.0.0"
		url = ""
		comic = {
			loadInfo: async (id) => {
				let res = await Network.get(%q + "/comic/" + id);
				let json = JSON.parse(res.body);
				return { id: id, title: json.title, cover: json.cover, updateTime: json.updateTime, chapters: json.chapters };
			}
		}
	}`, srv.URL)

	eng, st, _ := newTestEngine(t, script)
	user := "u1"
	if err := st.MergeMirror(store.Mirror{
		UserID: user, Source: "testsrc", ComicID: "123", DueAt: time.Now().Add(-time.Minute).UTC().Format(time.RFC3339), Priority: "normal", UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		t.Fatalf("merge mirror: %v", err)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if err := st.InsertJob(store.Job{
		UserID: user, Source: "testsrc", ComicID: "123", State: "pending", Priority: "normal", DueAt: sql.NullString{String: now, Valid: true}, ScheduledAt: sql.NullString{String: now, Valid: true}, CreatedAt: now,
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

	var outcome string
	var payload string
	if err := st.DB().QueryRow(`SELECT outcome, COALESCE(payload,'') FROM results WHERE user_id=? AND source=? AND comic_id=?`, user, "testsrc", "123").Scan(&outcome, &payload); err != nil {
		t.Fatalf("query result: %v", err)
	}
	if outcome != "success" {
		t.Fatalf("outcome = %q", outcome)
	}
	if payload == "" {
		t.Fatal("payload empty")
	}

	m, err := st.GetMirror(user, "testsrc", "123")
	if err != nil {
		t.Fatalf("get mirror: %v", err)
	}
	if m.LastCheckTime == "" {
		t.Fatal("last_check_time not updated")
	}
}

func TestEngineRunJobMissingScript(t *testing.T) {
	eng, st, _ := newTestEngine(t, "")
	user := "u1"
	now := time.Now().UTC().Format(time.RFC3339)
	if err := st.InsertJob(store.Job{
		UserID: user, Source: "missing", ComicID: "1", State: "pending", Priority: "normal", DueAt: sql.NullString{String: now, Valid: true}, ScheduledAt: sql.NullString{String: now, Valid: true}, CreatedAt: now,
	}); err != nil {
		t.Fatalf("insert job: %v", err)
	}
	jobs, _ := st.GetPendingJobs(10)
	if err := eng.RunJob(t.Context(), jobs[0]); err == nil {
		t.Fatal("expected error for missing script")
	}
	var state string
	_ = st.DB().QueryRow(`SELECT state FROM jobs WHERE job_id=?`, jobs[0].JobID).Scan(&state)
	if state != "failed" {
		t.Fatalf("state = %q, want failed", state)
	}
}

func TestEngineRunJobNeedsRelogin(t *testing.T) {
	script := `class TestReloginSource extends ComicSource {
		name = "TestReloginSource"
		key = "reloginsrc"
		version = "1.0.0"
		url = ""
		comic = {
			loadInfo: async (id) => {
				throw new Error("need relogin");
			}
		}
	}`
	eng, st, _ := newTestEngine(t, script)
	user := "u1"
	now := time.Now().UTC().Format(time.RFC3339)
	if err := st.MergeMirror(store.Mirror{
		UserID: user, Source: "testsrc", ComicID: "9", DueAt: time.Now().Add(-time.Minute).UTC().Format(time.RFC3339), Priority: "normal", UpdatedAt: now,
	}); err != nil {
		t.Fatalf("merge mirror: %v", err)
	}
	if err := st.InsertJob(store.Job{
		UserID: user, Source: "testsrc", ComicID: "9", State: "pending", Priority: "normal", DueAt: sql.NullString{String: now, Valid: true}, ScheduledAt: sql.NullString{String: now, Valid: true}, CreatedAt: now,
	}); err != nil {
		t.Fatalf("insert job: %v", err)
	}
	jobs, _ := st.GetPendingJobs(10)
	_ = eng.RunJob(t.Context(), jobs[0])

	var state string
	var outcome string
	_ = st.DB().QueryRow(`SELECT state FROM jobs WHERE job_id=?`, jobs[0].JobID).Scan(&state)
	if state != "needs_resubmit" {
		t.Fatalf("state = %q, want needs_resubmit", state)
	}
	_ = st.DB().QueryRow(`SELECT outcome FROM results WHERE user_id=? AND source=? AND comic_id=?`, user, "testsrc", "9").Scan(&outcome)
	if outcome != "needs_relogin" {
		t.Fatalf("outcome = %q", outcome)
	}
}

func TestClassifyError(t *testing.T) {
	cases := map[string]string{
		"please relogin":    "needs_relogin",
		"login required":    "needs_relogin",
		"404 not found":     "not_found",
		"some random error": "error",
	}
	for msg, want := range cases {
		if got := classifyError(msg); got != want {
			t.Errorf("classifyError(%q) = %q, want %q", msg, got, want)
		}
	}
}
