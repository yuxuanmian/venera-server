package store

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrNotFound = errors.New("not found")

type User struct {
	UserID    string
	TZ        string
	Locale    string
	CreatedAt string
}

type Device struct {
	DeviceID   string
	UserID     string
	TokenHash  string
	CreatedAt  string
	LastSeenAt string
}

type SourceSession struct {
	UserID          string
	Source          string
	CookieEncrypted string
	CookieHash      string
	UA              string
	Proxy           string
	ScriptHash      string
	InitJSHash      string
	UpdatedAt       string
}

type Mirror struct {
	UserID         string
	Source         string
	ComicID        string
	DueAt          string
	LastCheckTime  string
	LastUpdateTime string
	Priority       string
	UpdatedAt      string
}

type Result struct {
	ResultID       int64
	UserID         string
	Source         string
	ComicID        string
	ClientResultID sql.NullString
	ScanTime       string
	ScriptHash     string
	InitJSHash     string
	CookieHash     string
	Outcome        string
	Payload        sql.NullString
	Detail         sql.NullString
	CreatedAt      string
}

type Job struct {
	JobID       int64
	UserID      string
	Source      string
	ComicID     string
	ChunkID     sql.NullInt64
	State       string
	Priority    string
	DueAt       sql.NullString
	ScheduledAt sql.NullString
	CreatedAt   string
	Attempts    int
	MaxAttempts int
	NextRetryAt sql.NullString
	LastError   sql.NullString
}

// --- users ---

func (s *Store) UpsertUser(userID, tz, locale string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(`
		INSERT INTO users(user_id, tz, locale, created_at) VALUES(?, ?, ?, ?)
		ON CONFLICT(user_id) DO UPDATE SET tz=excluded.tz, locale=excluded.locale
	`, userID, tz, locale, now)
	return err
}

func (s *Store) GetUser(userID string) (*User, error) {
	row := s.db.QueryRow(`SELECT user_id, tz, locale, created_at FROM users WHERE user_id=?`, userID)
	var u User
	if err := row.Scan(&u.UserID, &u.TZ, &u.Locale, &u.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &u, nil
}

// --- devices ---

func (s *Store) UpsertDevice(deviceID, userID, tokenHash string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(`
		INSERT INTO devices(device_id, user_id, token_hash, created_at, last_seen_at)
		VALUES(?, ?, ?, ?, ?)
		ON CONFLICT(device_id) DO UPDATE SET
			user_id=excluded.user_id,
			token_hash=excluded.token_hash,
			last_seen_at=excluded.last_seen_at
	`, deviceID, userID, tokenHash, now, now)
	return err
}

func (s *Store) GetDeviceByTokenHash(tokenHash string) (*Device, error) {
	row := s.db.QueryRow(`SELECT device_id, user_id, token_hash, created_at, last_seen_at FROM devices WHERE token_hash=?`, tokenHash)
	var d Device
	if err := row.Scan(&d.DeviceID, &d.UserID, &d.TokenHash, &d.CreatedAt, &d.LastSeenAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &d, nil
}

func (s *Store) GetDeviceByID(deviceID string) (*Device, error) {
	row := s.db.QueryRow(`SELECT device_id, user_id, token_hash, created_at, last_seen_at FROM devices WHERE device_id=?`, deviceID)
	var d Device
	if err := row.Scan(&d.DeviceID, &d.UserID, &d.TokenHash, &d.CreatedAt, &d.LastSeenAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &d, nil
}

func (s *Store) UpdateDeviceLastSeen(deviceID string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(`UPDATE devices SET last_seen_at=? WHERE device_id=?`, now, deviceID)
	return err
}

func (s *Store) ListDevices(userID string) ([]Device, error) {
	rows, err := s.db.Query(`SELECT device_id, user_id, token_hash, created_at, last_seen_at FROM devices WHERE user_id=?`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Device
	for rows.Next() {
		var d Device
		if err := rows.Scan(&d.DeviceID, &d.UserID, &d.TokenHash, &d.CreatedAt, &d.LastSeenAt); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) DeleteDevice(deviceID, userID string) error {
	res, err := s.db.Exec(`DELETE FROM devices WHERE device_id=? AND user_id=?`, deviceID, userID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// --- source sessions ---

func (s *Store) UpsertSourceSession(sess SourceSession) error {
	_, err := s.db.Exec(`
		INSERT INTO source_sessions(user_id, source, cookie_encrypted, cookie_hash, ua, proxy, script_hash, init_js_hash, updated_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(user_id, source) DO UPDATE SET
			cookie_encrypted=excluded.cookie_encrypted,
			cookie_hash=excluded.cookie_hash,
			ua=excluded.ua,
			proxy=excluded.proxy,
			script_hash=excluded.script_hash,
			init_js_hash=excluded.init_js_hash,
			updated_at=excluded.updated_at
	`, sess.UserID, sess.Source, sess.CookieEncrypted, sess.CookieHash, sess.UA, sess.Proxy, sess.ScriptHash, sess.InitJSHash, sess.UpdatedAt)
	return err
}

func (s *Store) GetSourceSession(userID, source string) (*SourceSession, error) {
	row := s.db.QueryRow(`
		SELECT user_id, source, cookie_encrypted, cookie_hash, ua, proxy, script_hash, init_js_hash, updated_at
		FROM source_sessions WHERE user_id=? AND source=?
	`, userID, source)
	var sess SourceSession
	if err := row.Scan(&sess.UserID, &sess.Source, &sess.CookieEncrypted, &sess.CookieHash, &sess.UA, &sess.Proxy, &sess.ScriptHash, &sess.InitJSHash, &sess.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &sess, nil
}

// --- mirror & tombstones ---

func (s *Store) MergeMirror(m Mirror) error {
	existing, err := s.GetMirror(m.UserID, m.Source, m.ComicID)
	if err == nil {
		// Newer last_check_time wins. Empty last_check_time is treated as stale.
		if m.LastCheckTime != "" && existing.LastCheckTime != "" && !timeAfter(m.LastCheckTime, existing.LastCheckTime) {
			return nil
		}
	}
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	_, err = s.db.Exec(`
		INSERT INTO mirror(user_id, source, comic_id, due_at, last_check_time, last_update_time, priority, updated_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(user_id, source, comic_id) DO UPDATE SET
			due_at=excluded.due_at,
			last_check_time=excluded.last_check_time,
			last_update_time=excluded.last_update_time,
			priority=excluded.priority,
			updated_at=excluded.updated_at
	`, m.UserID, m.Source, m.ComicID, m.DueAt, nullIfEmpty(m.LastCheckTime), nullIfEmpty(m.LastUpdateTime), m.Priority, time.Now().UTC().Format(time.RFC3339))
	return err
}

func (s *Store) GetMirror(userID, source, comicID string) (*Mirror, error) {
	row := s.db.QueryRow(`
		SELECT user_id, source, comic_id, due_at, last_check_time, last_update_time, priority, updated_at
		FROM mirror WHERE user_id=? AND source=? AND comic_id=?
	`, userID, source, comicID)
	var m Mirror
	var lastCheck, lastUpdate sql.NullString
	if err := row.Scan(&m.UserID, &m.Source, &m.ComicID, &m.DueAt, &lastCheck, &lastUpdate, &m.Priority, &m.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	m.LastCheckTime = lastCheck.String
	m.LastUpdateTime = lastUpdate.String
	return &m, nil
}

func (s *Store) UpdateMirrorState(userID, source, comicID, dueAt, priority string) error {
	if dueAt == "" && priority == "" {
		return nil
	}
	_, err := s.db.Exec(`
		UPDATE mirror SET
			due_at = COALESCE(?, due_at),
			priority = COALESCE(?, priority),
			updated_at = ?
		WHERE user_id=? AND source=? AND comic_id=?
	`, nullIfEmpty(dueAt), nullIfEmpty(priority), time.Now().UTC().Format(time.RFC3339), userID, source, comicID)
	return err
}

func (s *Store) DeleteMirror(userID, source, comicID string) error {
	_, err := s.db.Exec(`DELETE FROM mirror WHERE user_id=? AND source=? AND comic_id=?`, userID, source, comicID)
	return err
}

func (s *Store) ListAllMirror() ([]Mirror, error) {
	rows, err := s.db.Query(`
		SELECT user_id, source, comic_id, due_at, last_check_time, last_update_time, priority, updated_at
		FROM mirror
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Mirror
	for rows.Next() {
		var m Mirror
		var lastCheck, lastUpdate sql.NullString
		if err := rows.Scan(&m.UserID, &m.Source, &m.ComicID, &m.DueAt, &lastCheck, &lastUpdate, &m.Priority, &m.UpdatedAt); err != nil {
			return nil, err
		}
		m.LastCheckTime = lastCheck.String
		m.LastUpdateTime = lastUpdate.String
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) ListJobs(limit int) ([]Job, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.db.Query(`
		SELECT job_id, user_id, source, comic_id, chunk_id, state, priority, due_at, scheduled_at, created_at,
		       attempts, max_attempts, next_retry_at, last_error
		FROM jobs
		ORDER BY job_id DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		var j Job
		var chunkID sql.NullInt64
		var dueAt, scheduledAt, nextRetry, lastError sql.NullString
		if err := rows.Scan(&j.JobID, &j.UserID, &j.Source, &j.ComicID, &chunkID, &j.State, &j.Priority, &dueAt, &scheduledAt, &j.CreatedAt,
			&j.Attempts, &j.MaxAttempts, &nextRetry, &lastError); err != nil {
			return nil, err
		}
		j.ChunkID = chunkID
		j.DueAt = dueAt
		j.ScheduledAt = scheduledAt
		j.NextRetryAt = nextRetry
		j.LastError = lastError
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *Store) CountUsers() (int, error)  { return s.count(`SELECT COUNT(*) FROM users`) }
func (s *Store) CountMirror() (int, error) { return s.count(`SELECT COUNT(*) FROM mirror`) }
func (s *Store) CountResults() (int, error) {
	return s.count(`SELECT COUNT(*) FROM results`)
}
func (s *Store) CountJobsByState(state string) (int, error) {
	return s.count(`SELECT COUNT(*) FROM jobs WHERE state=?`, state)
}

func (s *Store) count(query string, args ...any) (int, error) {
	var n int
	err := s.db.QueryRow(query, args...).Scan(&n)
	return n, err
}

func (s *Store) ListMirror(userID string) ([]Mirror, error) {
	rows, err := s.db.Query(`
		SELECT user_id, source, comic_id, due_at, last_check_time, last_update_time, priority, updated_at
		FROM mirror WHERE user_id=?
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Mirror
	for rows.Next() {
		var m Mirror
		var lastCheck, lastUpdate sql.NullString
		if err := rows.Scan(&m.UserID, &m.Source, &m.ComicID, &m.DueAt, &lastCheck, &lastUpdate, &m.Priority, &m.UpdatedAt); err != nil {
			return nil, err
		}
		m.LastCheckTime = lastCheck.String
		m.LastUpdateTime = lastUpdate.String
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) ListDueMirror(now string) ([]Mirror, error) {
	rows, err := s.db.Query(`
		SELECT user_id, source, comic_id, due_at, last_check_time, last_update_time, priority, updated_at
		FROM mirror
		WHERE due_at <> '' AND due_at <= ?
	`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Mirror
	for rows.Next() {
		var m Mirror
		var lastCheck, lastUpdate sql.NullString
		if err := rows.Scan(&m.UserID, &m.Source, &m.ComicID, &m.DueAt, &lastCheck, &lastUpdate, &m.Priority, &m.UpdatedAt); err != nil {
			return nil, err
		}
		m.LastCheckTime = lastCheck.String
		m.LastUpdateTime = lastUpdate.String
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) HasActiveJob(userID, source, comicID string) (bool, error) {
	var one int
	err := s.db.QueryRow(`
		SELECT 1 FROM jobs WHERE user_id=? AND source=? AND comic_id=? AND state IN ('pending','running','needs_resubmit') LIMIT 1
	`, userID, source, comicID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (s *Store) InsertChunk(userID, source string) (int64, error) {
	res, err := s.db.Exec(`
		INSERT INTO chunks(user_id, source, state, scheduled_at, created_at) VALUES(?, ?, 'pending', ?, ?)
	`, userID, source, time.Now().UTC().Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Store) UpsertTombstone(userID, source, comicID string) error {
	_, err := s.db.Exec(`
		INSERT INTO deleted_keys(user_id, source, comic_id, deleted_at) VALUES(?, ?, ?, ?)
		ON CONFLICT(user_id, source, comic_id) DO UPDATE SET deleted_at=excluded.deleted_at
	`, userID, source, comicID, time.Now().UTC().Format(time.RFC3339))
	return err
}

func (s *Store) HasTombstone(userID, source, comicID string) (bool, error) {
	var one int
	err := s.db.QueryRow(`SELECT 1 FROM deleted_keys WHERE user_id=? AND source=? AND comic_id=?`, userID, source, comicID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (s *Store) ClearTombstone(userID, source, comicID string) error {
	_, err := s.db.Exec(`DELETE FROM deleted_keys WHERE user_id=? AND source=? AND comic_id=?`, userID, source, comicID)
	return err
}

// --- sync events ---

// InsertSyncEvent returns true if the event was newly recorded; false if duplicate.
func (s *Store) InsertSyncEvent(eventID, userID, deviceID string) (bool, error) {
	res, err := s.db.Exec(`
		INSERT INTO sync_events(event_id, user_id, device_id, processed_at) VALUES(?, ?, ?, ?)
	`, eventID, userID, deviceID, time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		if isUniqueConstraint(err) {
			return false, nil
		}
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// --- jobs ---

func (s *Store) InsertJob(j Job) error {
	_, err := s.db.Exec(`
		INSERT INTO jobs(user_id, source, comic_id, chunk_id, state, priority, due_at, scheduled_at, created_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, j.UserID, j.Source, j.ComicID, nullIfEmptyInt(j.ChunkID), j.State, j.Priority, nullIfEmpty(j.DueAt.String), nullIfEmpty(j.ScheduledAt.String), time.Now().UTC().Format(time.RFC3339))
	return err
}

func (s *Store) GetPendingJobs(limit int) ([]Job, error) {
	if limit <= 0 {
		limit = 10
	}
	now := time.Now().UTC().Format(time.RFC3339)
	rows, err := s.db.Query(`
		SELECT job_id, user_id, source, comic_id, chunk_id, state, priority, due_at, scheduled_at, created_at,
		       attempts, max_attempts, next_retry_at, last_error
		FROM jobs
		WHERE state='pending' AND (next_retry_at IS NULL OR next_retry_at <= ?)
		ORDER BY CASE priority WHEN 'urgent' THEN 0 WHEN 'normal' THEN 1 ELSE 2 END, scheduled_at ASC
		LIMIT ?
	`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		var j Job
		var chunkID sql.NullInt64
		var dueAt, scheduledAt, nextRetry, lastError sql.NullString
		if err := rows.Scan(&j.JobID, &j.UserID, &j.Source, &j.ComicID, &chunkID, &j.State, &j.Priority, &dueAt, &scheduledAt, &j.CreatedAt,
			&j.Attempts, &j.MaxAttempts, &nextRetry, &lastError); err != nil {
			return nil, err
		}
		j.ChunkID = chunkID
		j.DueAt = dueAt
		j.ScheduledAt = scheduledAt
		j.NextRetryAt = nextRetry
		j.LastError = lastError
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *Store) MarkJobState(jobID int64, state string) error {
	_, err := s.db.Exec(`UPDATE jobs SET state=? WHERE job_id=?`, state, jobID)
	return err
}

// ClaimJob atomically transitions a pending job to running. Returns false if it could not be claimed.
func (s *Store) ClaimJob(jobID int64) (bool, error) {
	res, err := s.db.Exec(`UPDATE jobs SET state='running' WHERE job_id=? AND state='pending'`, jobID)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *Store) ResetRunningJobs() (int64, error) {
	res, err := s.db.Exec(`UPDATE jobs SET state='pending', next_retry_at=NULL WHERE state='running'`)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (s *Store) CompleteJob(jobID int64) error {
	_, err := s.db.Exec(`UPDATE jobs SET state='done', next_retry_at=NULL, last_error=NULL WHERE job_id=?`, jobID)
	return err
}

// FailJob records a failure. Transient failures keep the job pending with a backoff;
// non-transient failures exceed attempts and move to a terminal state.
func (s *Store) FailJob(jobID int64, lastError string, transient bool) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var attempts, maxAttempts int
	if err := tx.QueryRow(`SELECT attempts, max_attempts FROM jobs WHERE job_id=?`, jobID).Scan(&attempts, &maxAttempts); err != nil {
		return err
	}
	attempts++
	if !transient || attempts >= maxAttempts {
		_, err := tx.Exec(`UPDATE jobs SET state='failed', attempts=?, last_error=? WHERE job_id=?`, attempts, lastError, jobID)
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	nextRetry := time.Now().UTC().Add(backoff(attempts)).Format(time.RFC3339)
	if _, err := tx.Exec(`UPDATE jobs SET state='pending', attempts=?, next_retry_at=?, last_error=? WHERE job_id=?`, attempts, nextRetry, lastError, jobID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) ResetNeedsResubmitJobs(userID, source string) (int64, error) {
	res, err := s.db.Exec(`UPDATE jobs SET state='pending', next_retry_at=NULL WHERE user_id=? AND source=? AND state='needs_resubmit'`, userID, source)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (s *Store) MarkNeedsResubmit(jobID int64, lastError string) error {
	nextRetry := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	_, err := s.db.Exec(`UPDATE jobs SET state='needs_resubmit', next_retry_at=?, last_error=? WHERE job_id=?`, nextRetry, lastError, jobID)
	return err
}

func backoff(attempts int) time.Duration {
	d := 15 * time.Second * time.Duration(1<<uint(attempts-1))
	if d > time.Hour {
		return time.Hour
	}
	return d
}

func (s *Store) CancelPendingJobs(userID, source, comicID string) error {
	_, err := s.db.Exec(`UPDATE jobs SET state='cancelled' WHERE user_id=? AND source=? AND comic_id=? AND state='pending'`, userID, source, comicID)
	return err
}

// --- results ---

// InsertResult returns false if client_result_id already exists.
func (s *Store) InsertResult(r Result) (bool, error) {
	createdAt := time.Now().UTC().Format(time.RFC3339)
	res, err := s.db.Exec(`
		INSERT INTO results(user_id, source, comic_id, client_result_id, scan_time, script_hash, init_js_hash, cookie_hash, outcome, payload, detail, created_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, r.UserID, r.Source, r.ComicID, nullIfEmptyString(r.ClientResultID), r.ScanTime, r.ScriptHash, r.InitJSHash, r.CookieHash, r.Outcome, nullIfEmptyString(r.Payload), nullIfEmptyString(r.Detail), createdAt)
	if err != nil {
		if isUniqueConstraint(err) {
			return false, nil // duplicate client_result_id
		}
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *Store) ClearOldPayloads(userID, source, comicID string, keepResultID int64) error {
	_, err := s.db.Exec(`
		UPDATE results SET payload=NULL
		WHERE user_id=? AND source=? AND comic_id=? AND result_id<>?
	`, userID, source, comicID, keepResultID)
	return err
}

// PrunePayloads keeps only the newest result's payload per key, nulling the rest.
func (s *Store) PrunePayloads(userID, source, comicID string) error {
	_, err := s.db.Exec(`
		UPDATE results SET payload=NULL
		WHERE user_id=? AND source=? AND comic_id=? AND rowid <> (
			SELECT MAX(rowid) FROM results WHERE user_id=? AND source=? AND comic_id=?
		)
	`, userID, source, comicID, userID, source, comicID)
	return err
}

type ResultKey struct {
	UserID  string
	Source  string
	ComicID string
}

func (s *Store) ListResultKeys() ([]ResultKey, error) {
	rows, err := s.db.Query(`SELECT DISTINCT user_id, source, comic_id FROM results`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ResultKey
	for rows.Next() {
		var k ResultKey
		if err := rows.Scan(&k.UserID, &k.Source, &k.ComicID); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

func (s *Store) DeleteResultsOlderThan(now time.Time, retention time.Duration) error {
	cutoff := now.Add(-retention).UTC().Format(time.RFC3339)
	_, err := s.db.Exec(`DELETE FROM results WHERE created_at < ?`, cutoff)
	return err
}

// CleanupResults prunes stale payloads and deletes results older than retention.
func (s *Store) CleanupResults(retention time.Duration) error {
	keys, err := s.ListResultKeys()
	if err != nil {
		return err
	}
	for _, k := range keys {
		if err := s.PrunePayloads(k.UserID, k.Source, k.ComicID); err != nil {
			return err
		}
	}
	return s.DeleteResultsOlderThan(time.Now(), retention)
}

func (s *Store) ListResults(userID string, cursor int64, limit int) ([]Result, error) {
	if limit <= 0 {
		limit = 500
	}
	if limit > 1000 {
		limit = 1000
	}
	rows, err := s.db.Query(`
		SELECT result_id, user_id, source, comic_id, client_result_id, scan_time,
		       script_hash, init_js_hash, cookie_hash, outcome, payload, detail, created_at
		FROM results
		WHERE user_id=? AND result_id>?
		ORDER BY result_id ASC
		LIMIT ?
	`, userID, cursor, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Result
	for rows.Next() {
		var r Result
		var clientID, payload, detail sql.NullString
		if err := rows.Scan(&r.ResultID, &r.UserID, &r.Source, &r.ComicID, &clientID, &r.ScanTime,
			&r.ScriptHash, &r.InitJSHash, &r.CookieHash, &r.Outcome, &payload, &detail, &r.CreatedAt); err != nil {
			return nil, err
		}
		r.ClientResultID = clientID
		r.Payload = payload
		r.Detail = detail
		out = append(out, r)
	}
	return out, rows.Err()
}

// --- scripts ---

func (s *Store) ScriptExists(hash string) (bool, error) {
	var one int
	err := s.db.QueryRow(`SELECT 1 FROM scripts WHERE hash=?`, hash).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (s *Store) GetScriptPath(hash string) (string, error) {
	var path string
	err := s.db.QueryRow(`SELECT path FROM scripts WHERE hash=?`, hash).Scan(&path)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	return path, err
}

func (s *Store) InsertScript(hash, kind, path string, size int64) error {
	_, err := s.db.Exec(`
		INSERT INTO scripts(hash, kind, path, size, created_at) VALUES(?, ?, ?, ?, ?)
	`, hash, kind, path, size, time.Now().UTC().Format(time.RFC3339))
	return err
}

func (s *Store) FindMissingScriptHashes(hashes []string) ([]string, error) {
	var missing []string
	for _, h := range hashes {
		if h == "" {
			continue
		}
		ok, err := s.ScriptExists(h)
		if err != nil {
			return nil, err
		}
		if !ok {
			missing = append(missing, h)
		}
	}
	return missing, nil
}

// --- pairing codes ---

func (s *Store) CreatePairingCode(codeHash, userID, deviceID, expiresAt string) error {
	_, err := s.db.Exec(`
		INSERT INTO pairing_codes(code_hash, user_id, device_id, expires_at, used_at) VALUES(?, ?, ?, ?, NULL)
	`, codeHash, userID, deviceID, expiresAt)
	return err
}

func (s *Store) GetPairingCode(codeHash string) (userID, deviceID string, err error) {
	row := s.db.QueryRow(`SELECT user_id, device_id, expires_at, used_at FROM pairing_codes WHERE code_hash=?`, codeHash)
	var expiresAt, usedAt sql.NullString
	if err := row.Scan(&userID, &deviceID, &expiresAt, &usedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", ErrNotFound
		}
		return "", "", err
	}
	if usedAt.Valid {
		return "", "", ErrNotFound
	}
	t, err := time.Parse(time.RFC3339, expiresAt.String)
	if err != nil || time.Now().UTC().After(t) {
		return "", "", ErrNotFound
	}
	return userID, deviceID, nil
}

func (s *Store) UsePairingCode(codeHash string) error {
	_, err := s.db.Exec(`UPDATE pairing_codes SET used_at=? WHERE code_hash=?`, time.Now().UTC().Format(time.RFC3339), codeHash)
	return err
}

func (s *Store) InvalidateDeviceCodes(deviceID string) error {
	_, err := s.db.Exec(`DELETE FROM pairing_codes WHERE device_id=?`, deviceID)
	return err
}

// --- helpers ---

func timeAfter(a, b string) bool {
	ta, err1 := time.Parse(time.RFC3339, a)
	tb, err2 := time.Parse(time.RFC3339, b)
	if err1 != nil || err2 != nil {
		return a > b
	}
	return ta.After(tb)
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullIfEmptyString(v sql.NullString) any {
	if !v.Valid || v.String == "" {
		return nil
	}
	return v.String
}

func nullIfEmptyInt(v sql.NullInt64) any {
	if !v.Valid {
		return nil
	}
	return v.Int64
}

func isUniqueConstraint(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unique") || strings.Contains(msg, "constraint failed")
}

var _ = fmt.Sprintf
