package store

import (
	"database/sql"
	"testing"
	"time"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func insertJob(t *testing.T, st *Store) int64 {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	if err := st.InsertJob(Job{
		UserID: "u1", Source: "src", ComicID: "1", State: "pending", Priority: "normal",
		ScheduledAt: sql.NullString{String: now, Valid: true}, CreatedAt: now,
	}); err != nil {
		t.Fatalf("insert job: %v", err)
	}
	var id int64
	if err := st.DB().QueryRow(`SELECT job_id FROM jobs ORDER BY job_id DESC LIMIT 1`).Scan(&id); err != nil {
		t.Fatalf("get job id: %v", err)
	}
	return id
}

func TestClaimJobOnlyPending(t *testing.T) {
	st := newStore(t)
	id := insertJob(t, st)

	ok, err := st.ClaimJob(id)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	// Second claim should fail (already running).
	ok, err = st.ClaimJob(id)
	if err != nil || ok {
		t.Fatalf("second claim: ok=%v err=%v", ok, err)
	}
	var state string
	_ = st.DB().QueryRow(`SELECT state FROM jobs WHERE job_id=?`, id).Scan(&state)
	if state != "running" {
		t.Fatalf("state = %q", state)
	}
}

func TestClaimJobFailsForExistingRunning(t *testing.T) {
	st := newStore(t)
	id := insertJob(t, st)
	_, _ = st.ClaimJob(id)
	ok, err := st.ClaimJob(id)
	if err != nil || ok {
		t.Fatalf("claim running: ok=%v err=%v", ok, err)
	}
}

func TestCompleteJob(t *testing.T) {
	st := newStore(t)
	id := insertJob(t, st)
	_, _ = st.ClaimJob(id)
	if err := st.CompleteJob(id); err != nil {
		t.Fatalf("complete: %v", err)
	}
	var state string
	_ = st.DB().QueryRow(`SELECT state FROM jobs WHERE job_id=?`, id).Scan(&state)
	if state != "done" {
		t.Fatalf("state = %q", state)
	}
}

func TestFailJobTransientBackoff(t *testing.T) {
	st := newStore(t)
	id := insertJob(t, st)
	_, _ = st.ClaimJob(id)
	if err := st.FailJob(id, "boom", true); err != nil {
		t.Fatalf("fail: %v", err)
	}
	var state string
	var attempts int
	var nextRetry sql.NullString
	var lastErr sql.NullString
	_ = st.DB().QueryRow(`SELECT state, attempts, next_retry_at, last_error FROM jobs WHERE job_id=?`, id).Scan(&state, &attempts, &nextRetry, &lastErr)
	if state != "pending" {
		t.Fatalf("state = %q, want pending", state)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d", attempts)
	}
	if !nextRetry.Valid {
		t.Fatal("next_retry_at not set")
	}
	if !lastErr.Valid || lastErr.String != "boom" {
		t.Fatalf("last_error = %v", lastErr)
	}
	// Not due yet: GetPendingJobs should not return it.
	jobs, _ := st.GetPendingJobs(10)
	if len(jobs) != 0 {
		t.Fatalf("pending jobs = %d, want 0 before retry time", len(jobs))
	}
}

func TestFailJobNonTransientTerminal(t *testing.T) {
	st := newStore(t)
	id := insertJob(t, st)
	_, _ = st.ClaimJob(id)
	if err := st.FailJob(id, "fatal", false); err != nil {
		t.Fatalf("fail: %v", err)
	}
	var state string
	_ = st.DB().QueryRow(`SELECT state FROM jobs WHERE job_id=?`, id).Scan(&state)
	if state != "failed" {
		t.Fatalf("state = %q", state)
	}
}

func TestFailJobMaxAttempts(t *testing.T) {
	st := newStore(t)
	id := insertJob(t, st)
	_, _ = st.ClaimJob(id)
	// max_attempts defaults to 3; third transient failure should go terminal.
	_ = st.FailJob(id, "e1", true)
	_, _ = st.ClaimJob(id)
	_ = st.FailJob(id, "e2", true)
	_, _ = st.ClaimJob(id)
	_ = st.FailJob(id, "e3", true)
	var state string
	var attempts int
	_ = st.DB().QueryRow(`SELECT state, attempts FROM jobs WHERE job_id=?`, id).Scan(&state, &attempts)
	if state != "failed" {
		t.Fatalf("state = %q, want failed", state)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d", attempts)
	}
}

func TestMarkNeedsResubmit(t *testing.T) {
	st := newStore(t)
	id := insertJob(t, st)
	if err := st.MarkNeedsResubmit(id, "relogin"); err != nil {
		t.Fatalf("mark: %v", err)
	}
	var state string
	_ = st.DB().QueryRow(`SELECT state FROM jobs WHERE job_id=?`, id).Scan(&state)
	if state != "needs_resubmit" {
		t.Fatalf("state = %q", state)
	}
}

func TestGetPendingJobsHonorsRetryTime(t *testing.T) {
	st := newStore(t)
	id := insertJob(t, st)
	_, _ = st.ClaimJob(id)
	_ = st.FailJob(id, "boom", true)
	// Force next_retry_at into the past to simulate elapsed backoff.
	if _, err := st.DB().Exec(`UPDATE jobs SET next_retry_at=? WHERE job_id=?`, time.Now().Add(-time.Minute).UTC().Format(time.RFC3339), id); err != nil {
		t.Fatalf("force retry time: %v", err)
	}
	jobs, err := st.GetPendingJobs(10)
	if err != nil {
		t.Fatalf("get pending: %v", err)
	}
	found := false
	for _, j := range jobs {
		if j.JobID == id {
			found = true
		}
	}
	if !found {
		t.Fatal("job not returned after retry time elapsed")
	}
}
