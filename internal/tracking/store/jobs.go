package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"venera-server/internal/tracking/domain"
)

// ScanJobState is the durable lifecycle of one exact user/artifact demand.
// completed and failed rows remain schedulable when due, which preserves the
// normal cadence without requiring an in-memory timer to survive a restart.
type ScanJobState string

const (
	ScanJobPending   ScanJobState = "pending"
	ScanJobRunning   ScanJobState = "running"
	ScanJobCompleted ScanJobState = "completed"
	ScanJobFailed    ScanJobState = "failed"
)

type ScanJobPriority string

const (
	ScanJobPriorityNormal    ScanJobPriority = "normal"
	ScanJobPriorityExpedited ScanJobPriority = "expedited"
)

var ErrScanJobLeaseLost = errors.New("tracking scan job lease lost")

// ScanJobSpec is the scheduler projection of a current enabled demand. The
// demand digest changes whenever the exact interest/device fence changes.
type ScanJobSpec struct {
	UserID            string
	Artifact          domain.ArtifactIdentity
	CatalogID         string
	CatalogRevision   string
	RuntimeGeneration int64
	DemandDigest      string
	DueAt             time.Time
	Priority          ScanJobPriority
	MaxAttempts       int
}

type ScanJob struct {
	JobID             int64
	UserID            string
	Artifact          domain.ArtifactIdentity
	CatalogID         string
	CatalogRevision   string
	RuntimeGeneration int64
	DemandDigest      string
	State             ScanJobState
	Priority          ScanJobPriority
	DueAt             time.Time
	LeaseUntil        *time.Time
	LeaseToken        string
	Attempts          int
	MaxAttempts       int
	LastError         string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

func (job ScanJob) key() string {
	return job.UserID + "\x00" + job.Artifact.SourceKey + "\x00" + job.Artifact.FileName
}

func (spec ScanJobSpec) validate(now time.Time) (ScanJobSpec, error) {
	spec.UserID = strings.TrimSpace(spec.UserID)
	spec.CatalogID = strings.TrimSpace(spec.CatalogID)
	spec.DemandDigest = strings.TrimSpace(spec.DemandDigest)
	if spec.UserID == "" || spec.CatalogID == "" || spec.DemandDigest == "" {
		return ScanJobSpec{}, errors.New("scan job userID, catalogID, and demandDigest are required")
	}
	if err := spec.Artifact.Validate(); err != nil {
		return ScanJobSpec{}, err
	}
	if !domain.FullRevision(spec.CatalogRevision) {
		return ScanJobSpec{}, errors.New("scan job catalog revision must be a full commit")
	}
	if spec.RuntimeGeneration < 1 {
		return ScanJobSpec{}, errors.New("scan job runtime generation must be positive")
	}
	if spec.DueAt.IsZero() {
		spec.DueAt = now
	}
	spec.DueAt = spec.DueAt.UTC()
	if spec.Priority == "" {
		spec.Priority = ScanJobPriorityNormal
	}
	if spec.Priority != ScanJobPriorityNormal && spec.Priority != ScanJobPriorityExpedited {
		return ScanJobSpec{}, errors.New("scan job priority is invalid")
	}
	if spec.MaxAttempts <= 0 {
		spec.MaxAttempts = 3
	}
	return spec, nil
}

// ReconcileScanJobs makes the durable scheduler mirror the current enabled
// demand set. An unchanged demand preserves its state/due time; a changed
// catalog generation or demand fence is a new pending job due immediately.
func (r *Repository) ReconcileScanJobs(ctx context.Context, specs []ScanJobSpec, now time.Time) error {
	if r == nil || r.db == nil {
		return errors.New("tracking repository is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if now.IsZero() {
		now = timeNowUTC()
	}
	now = now.UTC()
	normalized := make([]ScanJobSpec, len(specs))
	seen := make(map[string]struct{}, len(specs))
	for index, raw := range specs {
		spec, err := raw.validate(now)
		if err != nil {
			return fmt.Errorf("scan job spec %d: %w", index, err)
		}
		key := spec.UserID + "\x00" + spec.Artifact.SourceKey + "\x00" + spec.Artifact.FileName
		if _, exists := seen[key]; exists {
			return errors.New("duplicate scan job spec")
		}
		seen[key] = struct{}{}
		normalized[index] = spec
	}
	sort.Slice(normalized, func(i, j int) bool {
		left := normalized[i].UserID + "\x00" + normalized[i].Artifact.SourceKey + "\x00" + normalized[i].Artifact.FileName
		right := normalized[j].UserID + "\x00" + normalized[j].Artifact.SourceKey + "\x00" + normalized[j].Artifact.FileName
		return left < right
	})

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin scan job reconciliation: %w", err)
	}
	defer tx.Rollback()
	existing, err := listScanJobsTx(ctx, tx, "")
	if err != nil {
		return err
	}
	byKey := make(map[string]ScanJob, len(existing))
	for _, job := range existing {
		byKey[job.key()] = job
	}
	for _, spec := range normalized {
		key := spec.UserID + "\x00" + spec.Artifact.SourceKey + "\x00" + spec.Artifact.FileName
		current, found := byKey[key]
		if !found {
			created := now
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO tracking_scan_jobs (
					user_id, source_key, file_name, catalog_id, catalog_revision,
					runtime_generation, demand_digest, state, priority, due_at,
					attempts, max_attempts, created_at, updated_at
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				spec.UserID, spec.Artifact.SourceKey, spec.Artifact.FileName,
				spec.CatalogID, spec.CatalogRevision, spec.RuntimeGeneration,
				spec.DemandDigest, string(ScanJobPending), string(spec.Priority),
				spec.DueAt.Format(wireTimeLayout), 0, spec.MaxAttempts,
				created.Format(wireTimeLayout), now.Format(wireTimeLayout),
			); err != nil {
				return fmt.Errorf("insert scan job: %w", err)
			}
			continue
		}
		identityChanged := current.CatalogID != spec.CatalogID ||
			current.CatalogRevision != spec.CatalogRevision ||
			current.RuntimeGeneration != spec.RuntimeGeneration ||
			current.DemandDigest != spec.DemandDigest
		if identityChanged {
			if _, err := tx.ExecContext(ctx, `
				UPDATE tracking_scan_jobs SET
					catalog_id = ?, catalog_revision = ?, runtime_generation = ?, demand_digest = ?,
					state = ?, priority = ?, due_at = ?, lease_until = NULL, lease_token = NULL,
					attempts = 0, max_attempts = ?, last_error = NULL, updated_at = ?
				WHERE job_id = ?`,
				spec.CatalogID, spec.CatalogRevision, spec.RuntimeGeneration, spec.DemandDigest,
				string(ScanJobPending), string(spec.Priority), spec.DueAt.Format(wireTimeLayout),
				spec.MaxAttempts, now.Format(wireTimeLayout), current.JobID,
			); err != nil {
				return fmt.Errorf("reset changed scan job: %w", err)
			}
			continue
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE tracking_scan_jobs SET priority = ?, max_attempts = ?, updated_at = ?
			WHERE job_id = ?`,
			string(spec.Priority), spec.MaxAttempts, now.Format(wireTimeLayout), current.JobID,
		); err != nil {
			return fmt.Errorf("refresh scan job metadata: %w", err)
		}
	}
	for _, current := range existing {
		if _, keep := seen[current.key()]; keep {
			continue
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM tracking_scan_jobs WHERE job_id = ?`, current.JobID); err != nil {
			return fmt.Errorf("remove obsolete scan job: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit scan job reconciliation: %w", err)
	}
	return nil
}

func (r *Repository) ListDueScanJobs(ctx context.Context, now time.Time, limit int) ([]ScanJob, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("tracking repository is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if now.IsZero() {
		now = timeNowUTC()
	}
	if limit <= 0 {
		limit = 1000
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT job_id, user_id, source_key, file_name, catalog_id, catalog_revision,
		       runtime_generation, demand_digest, state, priority, due_at,
		       lease_until, lease_token, attempts, max_attempts, last_error,
		       created_at, updated_at
		FROM tracking_scan_jobs
		WHERE state <> ? AND due_at <= ?
		ORDER BY CASE priority WHEN 'expedited' THEN 0 ELSE 1 END, due_at, job_id
		LIMIT ?`, string(ScanJobRunning), formatTime(now.UTC()), limit)
	if err != nil {
		return nil, fmt.Errorf("list due scan jobs: %w", err)
	}
	return scanJobRows(rows)
}

// NextScanWake returns the earliest persisted due/recovery boundary. A
// running lease is included so a crashed/abandoned worker cannot sleep past
// its reclaim point when the service is configured not to recover eagerly.
func (r *Repository) NextScanWake(ctx context.Context, now time.Time) (time.Time, bool, error) {
	if r == nil || r.db == nil {
		return time.Time{}, false, errors.New("tracking repository is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var value sql.NullString
	err := r.db.QueryRowContext(ctx, `
		SELECT MIN(wake_at) FROM (
			SELECT due_at AS wake_at FROM tracking_scan_jobs WHERE state <> 'running'
			UNION ALL
			SELECT lease_until AS wake_at FROM tracking_scan_jobs
			WHERE state = 'running' AND lease_until IS NOT NULL
		)`,
	).Scan(&value)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("find next scan wake: %w", err)
	}
	if !value.Valid || strings.TrimSpace(value.String) == "" {
		return time.Time{}, false, nil
	}
	parsed, err := parseTime(value.String)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("parse next scan wake: %w", err)
	}
	return parsed, true, nil
}

func (r *Repository) ClaimScanJob(ctx context.Context, jobID int64, leaseToken string, now time.Time, leaseDuration time.Duration) (bool, error) {
	if r == nil || r.db == nil {
		return false, errors.New("tracking repository is nil")
	}
	if jobID < 1 || strings.TrimSpace(leaseToken) == "" {
		return false, errors.New("scan job ID and lease token are required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if now.IsZero() {
		now = timeNowUTC()
	}
	if leaseDuration <= 0 {
		leaseDuration = 5 * time.Minute
	}
	result, err := r.db.ExecContext(ctx, `
		UPDATE tracking_scan_jobs SET
			state = ?, lease_until = ?, lease_token = ?, attempts = attempts + 1, updated_at = ?
		WHERE job_id = ? AND state <> ? AND due_at <= ?`,
		string(ScanJobRunning), formatTime(now.UTC().Add(leaseDuration)), leaseToken,
		formatTime(now.UTC()), jobID, string(ScanJobRunning), formatTime(now.UTC()))
	if err != nil {
		return false, fmt.Errorf("lease scan job: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("lease scan job result: %w", err)
	}
	return changed == 1, nil
}

func (r *Repository) CompleteScanJob(ctx context.Context, jobID int64, leaseToken string, nextDue, now time.Time) error {
	if r == nil || r.db == nil {
		return errors.New("tracking repository is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if nextDue.IsZero() {
		return errors.New("next scan due time is required")
	}
	if now.IsZero() {
		now = timeNowUTC()
	}
	result, err := r.db.ExecContext(ctx, `
		UPDATE tracking_scan_jobs SET
			state = ?, due_at = ?, lease_until = NULL, lease_token = NULL,
			attempts = 0, last_error = NULL, updated_at = ?
		WHERE job_id = ? AND state = ? AND lease_token = ?`,
		string(ScanJobCompleted), formatTime(nextDue.UTC()), formatTime(now.UTC()),
		jobID, string(ScanJobRunning), leaseToken)
	if err != nil {
		return fmt.Errorf("complete scan job: %w", err)
	}
	return requireScanJobMutation(result)
}

func (r *Repository) RescheduleScanJob(ctx context.Context, jobID int64, leaseToken string, dueAt, now time.Time, reason string) error {
	if r == nil || r.db == nil {
		return errors.New("tracking repository is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if dueAt.IsZero() {
		dueAt = timeNowUTC()
	}
	if now.IsZero() {
		now = timeNowUTC()
	}
	result, err := r.db.ExecContext(ctx, `
		UPDATE tracking_scan_jobs SET
			state = ?, due_at = ?, lease_until = NULL, lease_token = NULL,
			attempts = 0, last_error = ?, updated_at = ?
		WHERE job_id = ? AND state = ? AND lease_token = ?`,
		string(ScanJobPending), formatTime(dueAt.UTC()), nullableError(reason), formatTime(now.UTC()),
		jobID, string(ScanJobRunning), leaseToken)
	if err != nil {
		return fmt.Errorf("reschedule scan job: %w", err)
	}
	return requireScanJobMutation(result)
}

// FailScanJob persists one failed scheduler attempt. retryAfter applies while
// the durable attempt budget remains; once exhausted, cadence keeps the row
// visible for the next normal 12-hour freshness cycle.
func (r *Repository) FailScanJob(
	ctx context.Context,
	jobID int64,
	leaseToken string,
	now time.Time,
	reason string,
	retryable bool,
	retryAfter time.Duration,
	cadence time.Duration,
) error {
	if r == nil || r.db == nil {
		return errors.New("tracking repository is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if now.IsZero() {
		now = timeNowUTC()
	}
	if retryAfter <= 0 {
		retryAfter = 250 * time.Millisecond
	}
	if cadence <= 0 {
		cadence = 12 * time.Hour
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin failed scan job: %w", err)
	}
	defer tx.Rollback()
	var attempts, maxAttempts int
	err = tx.QueryRowContext(ctx, `
		SELECT attempts, max_attempts FROM tracking_scan_jobs
		WHERE job_id = ? AND state = ? AND lease_token = ?`,
		jobID, string(ScanJobRunning), leaseToken).Scan(&attempts, &maxAttempts)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrScanJobLeaseLost
	}
	if err != nil {
		return fmt.Errorf("read failed scan job: %w", err)
	}
	dueAt := now.UTC().Add(cadence)
	state := ScanJobFailed
	if retryable && attempts < maxAttempts {
		state = ScanJobPending
		dueAt = now.UTC().Add(retryAfter)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE tracking_scan_jobs SET
			state = ?, due_at = ?, lease_until = NULL, lease_token = NULL,
			last_error = ?, updated_at = ?
		WHERE job_id = ? AND state = ? AND lease_token = ?`,
		string(state), formatTime(dueAt), nullableError(reason), formatTime(now.UTC()),
		jobID, string(ScanJobRunning), leaseToken); err != nil {
		return fmt.Errorf("update failed scan job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit failed scan job: %w", err)
	}
	return nil
}

func (r *Repository) ReleaseScanJob(ctx context.Context, jobID int64, leaseToken string, now time.Time, reason string) error {
	if r == nil || r.db == nil {
		return errors.New("tracking repository is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if now.IsZero() {
		now = timeNowUTC()
	}
	result, err := r.db.ExecContext(ctx, `
		UPDATE tracking_scan_jobs SET
			state = ?, due_at = ?, lease_until = NULL, lease_token = NULL,
			last_error = ?, updated_at = ?
		WHERE job_id = ? AND state = ? AND lease_token = ?`,
		string(ScanJobPending), formatTime(now.UTC()), nullableError(reason), formatTime(now.UTC()),
		jobID, string(ScanJobRunning), leaseToken)
	if err != nil {
		return fmt.Errorf("release scan job: %w", err)
	}
	return requireScanJobMutation(result)
}

// RecoverRunningScanJobs returns running jobs to the durable pending queue.
// includeUnexpired is used during process startup, where this service is the
// sole owner of the database-backed scanner lifecycle.
func (r *Repository) RecoverRunningScanJobs(ctx context.Context, now time.Time, includeUnexpired bool) error {
	if r == nil || r.db == nil {
		return errors.New("tracking repository is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if now.IsZero() {
		now = timeNowUTC()
	}
	where := "state = 'running'"
	args := []any{string(ScanJobPending), formatTime(now.UTC()), formatTime(now.UTC())}
	if !includeUnexpired {
		where += " AND (lease_until IS NULL OR lease_until <= ?)"
		args = append(args, formatTime(now.UTC()))
	}
	query := `UPDATE tracking_scan_jobs SET
		state = ?, due_at = ?, lease_until = NULL, lease_token = NULL,
		last_error = COALESCE(last_error, 'worker lease recovered'), updated_at = ?
		WHERE ` + where
	if _, err := r.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("recover running scan jobs: %w", err)
	}
	return nil
}

func (r *Repository) ListScanJobs(ctx context.Context) ([]ScanJob, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("tracking repository is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT job_id, user_id, source_key, file_name, catalog_id, catalog_revision,
		       runtime_generation, demand_digest, state, priority, due_at,
		       lease_until, lease_token, attempts, max_attempts, last_error,
		       created_at, updated_at
		FROM tracking_scan_jobs ORDER BY job_id`)
	if err != nil {
		return nil, fmt.Errorf("list scan jobs: %w", err)
	}
	return scanJobRows(rows)
}

func listScanJobsTx(ctx context.Context, tx *sql.Tx, where string, args ...any) ([]ScanJob, error) {
	query := `
		SELECT job_id, user_id, source_key, file_name, catalog_id, catalog_revision,
		       runtime_generation, demand_digest, state, priority, due_at,
		       lease_until, lease_token, attempts, max_attempts, last_error,
		       created_at, updated_at
		FROM tracking_scan_jobs`
	if strings.TrimSpace(where) != "" {
		query += " WHERE " + where
	}
	query += " ORDER BY job_id"
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("read scan jobs: %w", err)
	}
	return scanJobRows(rows)
}

func scanJobRows(rows *sql.Rows) ([]ScanJob, error) {
	defer rows.Close()
	var result []ScanJob
	for rows.Next() {
		var job ScanJob
		var state, priority string
		var dueAt, createdAt, updatedAt string
		var leaseUntil, leaseToken, lastError sql.NullString
		if err := rows.Scan(
			&job.JobID, &job.UserID, &job.Artifact.SourceKey, &job.Artifact.FileName,
			&job.CatalogID, &job.CatalogRevision, &job.RuntimeGeneration, &job.DemandDigest,
			&state, &priority, &dueAt, &leaseUntil, &leaseToken, &job.Attempts,
			&job.MaxAttempts, &lastError, &createdAt, &updatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan scan job: %w", err)
		}
		job.State = ScanJobState(state)
		job.Priority = ScanJobPriority(priority)
		var err error
		job.DueAt, err = parseTime(dueAt)
		if err != nil {
			return nil, fmt.Errorf("scan job due time: %w", err)
		}
		job.CreatedAt, err = parseTime(createdAt)
		if err != nil {
			return nil, fmt.Errorf("scan job created time: %w", err)
		}
		job.UpdatedAt, err = parseTime(updatedAt)
		if err != nil {
			return nil, fmt.Errorf("scan job updated time: %w", err)
		}
		if leaseUntil.Valid && strings.TrimSpace(leaseUntil.String) != "" {
			parsed, parseErr := parseTime(leaseUntil.String)
			if parseErr != nil {
				return nil, fmt.Errorf("scan job lease time: %w", parseErr)
			}
			job.LeaseUntil = &parsed
		}
		if leaseToken.Valid {
			job.LeaseToken = leaseToken.String
		}
		if lastError.Valid {
			job.LastError = lastError.String
		}
		result = append(result, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read scan job rows: %w", err)
	}
	return result, nil
}

func requireScanJobMutation(result sql.Result) error {
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("scan job mutation result: %w", err)
	}
	if changed != 1 {
		return ErrScanJobLeaseLost
	}
	return nil
}

func nullableError(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	if len(value) > 512 {
		return value[:512]
	}
	return value
}
