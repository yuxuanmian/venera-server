package scheduler

import (
	"context"
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
	"venera-server/internal/engine"
	"venera-server/internal/store"
)

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func insertMirror(t *testing.T, st *store.Store, user, source, comic string, due time.Time) {
	t.Helper()
	err := st.MergeMirror(store.Mirror{
		UserID:    user,
		Source:    source,
		ComicID:   comic,
		DueAt:     due.UTC().Format(time.RFC3339),
		Priority:  "normal",
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("insert mirror: %v", err)
	}
}

func countRows(t *testing.T, st *store.Store, query string) int {
	t.Helper()
	var n int
	if err := st.DB().QueryRow(query).Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", query, err)
	}
	return n
}

func TestScheduleDueCreatesJobsAndChunks(t *testing.T) {
	st := newTestStore(t)
	due := time.Now().Add(-time.Minute)
	for i := 0; i < 35; i++ {
		insertMirror(t, st, "u1", "src", string(rune('a'+i)), due)
	}

	if err := ScheduleDue(st); err != nil {
		t.Fatalf("schedule due: %v", err)
	}

	jobs := countRows(t, st, `SELECT COUNT(*) FROM jobs WHERE state='pending'`)
	if jobs != 35 {
		t.Fatalf("jobs = %d, want 35", jobs)
	}
	chunks := countRows(t, st, `SELECT COUNT(*) FROM chunks`)
	if chunks != 2 { // 30 + 5
		t.Fatalf("chunks = %d, want 2", chunks)
	}
}

func TestScheduleDueSkipsActive(t *testing.T) {
	st := newTestStore(t)
	due := time.Now().Add(-time.Minute)
	insertMirror(t, st, "u1", "src", "1", due)

	if err := ScheduleDue(st); err != nil {
		t.Fatalf("first schedule: %v", err)
	}
	if err := ScheduleDue(st); err != nil {
		t.Fatalf("second schedule: %v", err)
	}
	jobs := countRows(t, st, `SELECT COUNT(*) FROM jobs WHERE state='pending'`)
	if jobs != 1 {
		t.Fatalf("jobs = %d, want 1", jobs)
	}
}

func TestScheduleDueGroupsBySource(t *testing.T) {
	st := newTestStore(t)
	due := time.Now().Add(-time.Minute)
	insertMirror(t, st, "u1", "srcA", "1", due)
	insertMirror(t, st, "u1", "srcB", "1", due)

	if err := ScheduleDue(st); err != nil {
		t.Fatalf("schedule due: %v", err)
	}
	chunks := countRows(t, st, `SELECT COUNT(*) FROM chunks`)
	if chunks != 2 {
		t.Fatalf("chunks = %d, want 2", chunks)
	}
}

func insertPendingJob(t *testing.T, st *store.Store, user, source, comic string) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	if err := st.InsertJob(store.Job{
		UserID: user, Source: source, ComicID: comic, State: "pending", Priority: "normal",
		ScheduledAt: sql.NullString{String: now, Valid: true}, CreatedAt: now,
	}); err != nil {
		t.Fatalf("insert pending job: %v", err)
	}
}

func drainChannel(ch chan store.Job) int {
	n := 0
	for {
		select {
		case <-ch:
			n++
		default:
			return n
		}
	}
}

func TestDispatchOncePerSourceSingleFlight(t *testing.T) {
	st := newTestStore(t)
	s := New(st, nil, 0)
	s.workers = make(chan store.Job, 10)
	insertPendingJob(t, st, "u1", "src", "1")
	insertPendingJob(t, st, "u1", "src", "2")

	s.dispatchOnce(context.Background())
	if n := drainChannel(s.workers); n != 1 {
		t.Fatalf("dispatched = %d, want 1 (single flight per source)", n)
	}
}

func TestDispatchOnceDifferentSources(t *testing.T) {
	st := newTestStore(t)
	s := New(st, nil, 0)
	s.workers = make(chan store.Job, 10)
	insertPendingJob(t, st, "u1", "srcA", "1")
	insertPendingJob(t, st, "u1", "srcB", "1")

	s.dispatchOnce(context.Background())
	if n := drainChannel(s.workers); n != 2 {
		t.Fatalf("dispatched = %d, want 2", n)
	}
}

func TestDispatchOnceRespectsCooldown(t *testing.T) {
	st := newTestStore(t)
	s := New(st, nil, 0)
	s.workers = make(chan store.Job, 10)
	key := "src"
	s.nextAllowed[key] = time.Now().Add(time.Hour)
	insertPendingJob(t, st, "u1", "src", "1")

	s.dispatchOnce(context.Background())
	if n := drainChannel(s.workers); n != 0 {
		t.Fatalf("dispatched = %d, want 0 during cooldown", n)
	}
}

func TestReleaseSourceSetsIntervals(t *testing.T) {
	st := newTestStore(t)
	s := New(st, nil, 0)
	job := store.Job{UserID: "u1", Source: "src"}
	s.releaseSource(job)

	key := "src"
	s.mu.Lock()
	busy := s.busy[key]
	next := s.nextAllowed[key]
	last, ok := s.lastChunk[key]
	s.mu.Unlock()

	if busy {
		t.Fatal("busy should be false after release")
	}
	if !ok {
		t.Fatal("lastChunk should be set")
	}
	_ = last
	if next.IsZero() || next.Before(time.Now()) {
		t.Fatalf("nextAllowed not set correctly: %v", next)
	}
}

func TestSchedulerEndToEnd(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"title":"Sched","cover":"http://c","updateTime":"2026-07-20"}`)
	}))
	defer srv.Close()

	dataDir := t.TempDir()
	st, err := store.Open(dataDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	scriptDir := filepath.Join(dataDir, "scripts")
	_ = os.MkdirAll(scriptDir, 0o755)
	script := fmt.Sprintf(`class SchedSource extends ComicSource {
		name = "SchedSource"
		key = "schedsrc"
		version = "1.0.0"
		url = ""
		comic = { loadInfo: async (id) => {
			let res = await Network.get(%q + "/c/" + id);
			let j = JSON.parse(res.body);
			return { id: id, title: j.title, cover: j.cover, updateTime: j.updateTime };
		} }
	}`, srv.URL)
	_ = os.WriteFile(filepath.Join(scriptDir, "schedsrc.js"), []byte(script), 0o644)

	initPath, _ := filepath.Abs(filepath.Join("..", "..", "..", "venera", "assets", "init.js"))
	cfg := &config.Config{DataDir: dataDir, InitJSPath: initPath, ComicSourceDir: scriptDir}
	rec, _ := debugrecorder.New(dataDir, false)
	eng, err := engine.New(cfg, st, rec)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	sched := New(st, eng, 0)
	due := time.Now().Add(-time.Minute)
	insertMirror(t, st, "u1", "schedsrc", "77", due)

	if err := ScheduleDue(st); err != nil {
		t.Fatalf("schedule due: %v", err)
	}
	if err := sched.ProcessPending(context.Background()); err != nil {
		t.Fatalf("process pending: %v", err)
	}

	var state string
	_ = st.DB().QueryRow(`SELECT state FROM jobs LIMIT 1`).Scan(&state)
	if state != "done" {
		t.Fatalf("job state = %q, want done", state)
	}
	var outcome string
	_ = st.DB().QueryRow(`SELECT outcome FROM results WHERE comic_id='77'`).Scan(&outcome)
	if outcome != "success" {
		t.Fatalf("outcome = %q", outcome)
	}
}
