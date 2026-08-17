package store

import (
	"database/sql"
	"testing"
	"time"
)

func insertResultRow(t *testing.T, st *Store, user, source, comic, outcome, payload string) int64 {
	t.Helper()
	inserted, err := st.InsertResult(Result{
		UserID: user, Source: source, ComicID: comic, ScanTime: time.Now().UTC().Format(time.RFC3339),
		Outcome: outcome, Payload: sql.NullString{String: payload, Valid: payload != ""},
	})
	if err != nil || !inserted {
		t.Fatalf("insert result: ok=%v err=%v", inserted, err)
	}
	var id int64
	if err := st.DB().QueryRow(`SELECT MAX(result_id) FROM results`).Scan(&id); err != nil {
		t.Fatalf("get result id: %v", err)
	}
	return id
}

func TestPrunePayloads(t *testing.T) {
	st := newStore(t)
	insertResultRow(t, st, "u", "src", "1", "success", `{"n":1}`)
	insertResultRow(t, st, "u", "src", "1", "success", `{"n":2}`)

	if err := st.PrunePayloads("u", "src", "1"); err != nil {
		t.Fatalf("prune: %v", err)
	}
	var payloads []sql.NullString
	rows, _ := st.DB().Query(`SELECT payload FROM results WHERE user_id='u' AND source='src' AND comic_id='1' ORDER BY result_id`)
	defer rows.Close()
	for rows.Next() {
		var p sql.NullString
		_ = rows.Scan(&p)
		payloads = append(payloads, p)
	}
	if len(payloads) != 2 {
		t.Fatalf("rows = %d", len(payloads))
	}
	if payloads[0].Valid {
		t.Fatal("old payload should be NULL")
	}
	if !payloads[1].Valid || payloads[1].String != `{"n":2}` {
		t.Fatalf("new payload = %v", payloads[1])
	}
}

func TestDeleteResultsOlderThan(t *testing.T) {
	st := newStore(t)
	oldID := insertResultRow(t, st, "u", "src", "1", "success", `{"n":1}`)
	newID := insertResultRow(t, st, "u", "src", "2", "success", `{"n":2}`)
	old := time.Now().Add(-8 * 24 * time.Hour).UTC().Format(time.RFC3339)
	if _, err := st.DB().Exec(`UPDATE results SET created_at=? WHERE result_id=?`, old, oldID); err != nil {
		t.Fatalf("set old created_at: %v", err)
	}
	if err := st.DeleteResultsOlderThan(time.Now(), 7*24*time.Hour); err != nil {
		t.Fatalf("delete old: %v", err)
	}
	var remaining int
	_ = st.DB().QueryRow(`SELECT COUNT(*) FROM results`).Scan(&remaining)
	if remaining != 1 {
		t.Fatalf("remaining = %d, want 1", remaining)
	}
	var id int64
	_ = st.DB().QueryRow(`SELECT result_id FROM results`).Scan(&id)
	if id != newID {
		t.Fatalf("remaining id = %d, want %d", id, newID)
	}
}

func TestCleanupResults(t *testing.T) {
	st := newStore(t)
	insertResultRow(t, st, "u", "src", "1", "success", `{"n":1}`)
	insertResultRow(t, st, "u", "src", "1", "success", `{"n":2}`)
	oldID := insertResultRow(t, st, "u", "src", "2", "success", `{"n":3}`)
	old := time.Now().Add(-8 * 24 * time.Hour).UTC().Format(time.RFC3339)
	_, _ = st.DB().Exec(`UPDATE results SET created_at=? WHERE result_id=?`, old, oldID)

	if err := st.CleanupResults(7 * 24 * time.Hour); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	var remaining int
	_ = st.DB().QueryRow(`SELECT COUNT(*) FROM results`).Scan(&remaining)
	if remaining != 2 {
		t.Fatalf("remaining = %d, want 2", remaining)
	}
	var payloadCount int
	_ = st.DB().QueryRow(`SELECT COUNT(*) FROM results WHERE payload IS NOT NULL`).Scan(&payloadCount)
	if payloadCount != 1 {
		t.Fatalf("payload count = %d, want 1", payloadCount)
	}
}
