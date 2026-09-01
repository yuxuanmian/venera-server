package v2scan

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"strings"
	"time"

	"venera-server/internal/v2domain"
	"venera-server/internal/v2store"
)

var (
	ErrDemandNotFound       = errors.New("scan demand not found")
	ErrJobNotFound          = errors.New("scan job not found")
	ErrLeaseLost            = errors.New("scan job lease is no longer valid")
	ErrStaleJob             = errors.New("scan job is stale")
	ErrInvalidDemand        = errors.New("scan demand is invalid")
	ErrInvalidJob           = errors.New("scan job is invalid")
	ErrNoExecutorAccount    = errors.New("no compatible executor source account")
	ErrDemandNotRunnable    = errors.New("scan demand is not runnable")
	ErrInvalidJobCompletion = errors.New("scan job completion is invalid")
)

type DemandKind string

const (
	DemandAccountSnapshot DemandKind = "accountSnapshot"
	DemandComicDetail     DemandKind = "comicDetail"
	DemandMaintenance     DemandKind = "maintenance"
)

type DemandState string

const (
	DemandActive   DemandState = "active"
	DemandBlocked  DemandState = "blocked"
	DemandInactive DemandState = "inactive"
)

type PriorityClass string

const (
	PriorityNormal    PriorityClass = "normal"
	PriorityExpedited PriorityClass = "expedited"
)

type JobKind string

const (
	JobAccountSnapshotSlice JobKind = "accountSnapshotSlice"
	JobComicDetailBatch     JobKind = "comicDetailBatch"
	JobMaintenance          JobKind = "maintenance"
)

type JobState string

const (
	JobReady     JobState = "ready"
	JobLeased    JobState = "leased"
	JobSucceeded JobState = "succeeded"
	JobRetryable JobState = "retryable"
	JobFailed    JobState = "failed"
	JobCancelled JobState = "cancelled"
)

type Demand struct {
	ID                    string
	Key                   string
	ArtifactID            string
	Kind                  DemandKind
	SourceAccountID       string
	ComicID               string
	VisibilityScope       string
	VariantKey            string
	ObservationContractID string
	State                 DemandState
	DueAt                 time.Time
	OldestAt              time.Time
	NextEligibleAt        time.Time
	ExecutionGeneration   int64
	PriorityClass         PriorityClass
	LastErrorCode         string
	Revision              v2domain.Revision
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

type Job struct {
	ID                      string
	DemandID                string
	ArtifactID              string
	Kind                    JobKind
	ExecutionGeneration     int64
	ExecutorSourceAccountID string
	PackageReleaseID        string
	FixedSessionEpoch       int64
	State                   JobState
	LeaseOwner              string
	LeaseExpiresAt          *time.Time
	AttemptCount            int
	NextEligibleAt          time.Time
	PayloadJSON             string
	ResultDigest            string
	CreatedAt               time.Time
	UpdatedAt               time.Time
}

type JobLease struct {
	Job       Job
	AttemptNo int
}

type DemandRepository struct {
	repo *v2store.Repository
}

func NewDemandRepository(repo *v2store.Repository) *DemandRepository {
	return &DemandRepository{repo: repo}
}

func AccountSnapshotDemandKey(artifactID, sourceAccountID string) string {
	return "accountSnapshot\x00" + artifactID + "\x00" + sourceAccountID
}

func ComicDetailDemandKey(artifactID, comicID, visibilityScope, variantKey, contractID string) string {
	return "comicDetail\x00" + artifactID + "\x00" + comicID + "\x00" + visibilityScope + "\x00" + variantKey + "\x00" + contractID
}

func MaintenanceDemandKey(artifactID, key string) string {
	return "maintenance\x00" + artifactID + "\x00" + key
}

func (r *DemandRepository) GetDemand(ctx context.Context, demandID string) (Demand, error) {
	if r == nil || r.repo == nil || demandID == "" {
		return Demand{}, ErrDemandNotFound
	}
	var demand Demand
	err := r.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		return scanDemand(tx.QueryRowContext(ctx, demandSelect+" WHERE demand_id = ?", demandID), &demand)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return Demand{}, ErrDemandNotFound
	}
	return demand, err
}

func (r *DemandRepository) GetDemandByKey(ctx context.Context, key string) (Demand, error) {
	if r == nil || r.repo == nil || key == "" {
		return Demand{}, ErrDemandNotFound
	}
	var demand Demand
	err := r.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		return scanDemand(tx.QueryRowContext(ctx, demandSelect+" WHERE demand_key = ?", key), &demand)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return Demand{}, ErrDemandNotFound
	}
	return demand, err
}

func (r *DemandRepository) ListDemands(ctx context.Context, artifactID string, kinds ...DemandKind) ([]Demand, error) {
	if r == nil || r.repo == nil {
		return nil, ErrInvalidDemand
	}
	var result []Demand
	err := r.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		query := demandSelect
		args := make([]any, 0, len(kinds)+1)
		conditions := make([]string, 0, len(kinds)+1)
		if artifactID != "" {
			conditions = append(conditions, "artifact_id = ?")
			args = append(args, artifactID)
		}
		if len(kinds) > 0 {
			placeholders := make([]string, len(kinds))
			for i, kind := range kinds {
				placeholders[i] = "?"
				args = append(args, string(kind))
			}
			conditions = append(conditions, "demand_kind IN ("+strings.Join(placeholders, ",")+")")
		}
		if len(conditions) > 0 {
			query += " WHERE " + strings.Join(conditions, " AND ")
		}
		query += " ORDER BY artifact_id, demand_kind, demand_key"
		rows, err := tx.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var demand Demand
			if err := scanDemand(rows, &demand); err != nil {
				return err
			}
			result = append(result, demand)
		}
		return rows.Err()
	})
	return result, err
}

// UpsertDemand creates a durable demand or changes its state without deleting
// the historical row. An active demand always has a positive generation so a
// materialized job can pin the generation before execution.
func (r *DemandRepository) UpsertDemand(ctx context.Context, desired Demand) (Demand, error) {
	if err := validateDemand(desired); err != nil {
		return Demand{}, err
	}
	var result Demand
	err := r.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		var existing Demand
		err := scanDemand(tx.QueryRowContext(ctx, demandSelect+" WHERE demand_key = ?", desired.Key), &existing)
		if errors.Is(err, sql.ErrNoRows) {
			now := r.repo.DB().Now()
			generation := desired.ExecutionGeneration
			if desired.State == DemandActive && generation < 1 {
				generation = 1
			}
			if desired.PriorityClass == "" {
				desired.PriorityClass = PriorityNormal
			}
			desired.ID = tx.NewID("demand")
			desired.ExecutionGeneration = generation
			desired.Revision = 1
			desired.CreatedAt = now
			desired.UpdatedAt = now
			if err := insertDemand(ctx, tx, desired); err != nil {
				return err
			}
			result = desired
			return nil
		}
		if err != nil {
			return err
		}
		now := r.repo.DB().Now()
		generation := existing.ExecutionGeneration
		if desired.State == DemandActive && existing.State != DemandActive {
			generation++
			if generation < 1 {
				generation = 1
			}
		}
		if desired.State == DemandActive && generation < 1 {
			generation = 1
		}
		oldestAt := existing.OldestAt
		if existing.State != DemandActive || oldestAt.IsZero() || desired.OldestAt.Before(oldestAt) {
			oldestAt = desired.OldestAt
		}
		dueAt := desired.DueAt
		if dueAt.IsZero() {
			dueAt = existing.DueAt
		}
		nextEligibleAt := desired.NextEligibleAt
		if nextEligibleAt.IsZero() {
			nextEligibleAt = existing.NextEligibleAt
		}
		priority := desired.PriorityClass
		if priority == "" {
			priority = existing.PriorityClass
		}
		if priority == "" {
			priority = PriorityNormal
		}
		lastError := desired.LastErrorCode
		if desired.State == DemandActive && lastError == "" {
			lastError = ""
		}
		_, err = tx.ExecContext(ctx, `
			UPDATE scan_demands SET
				artifact_id = ?, demand_kind = ?, source_account_id = ?, comic_id = ?,
				visibility_scope = ?, variant_key = ?, observation_contract_id = ?, state = ?,
				due_at = ?, oldest_at = ?, next_eligible_at = ?, execution_generation = ?,
				priority_class = ?, last_error_code = ?, revision = revision + 1, updated_at = ?
			WHERE demand_id = ?`,
			desired.ArtifactID, desired.Kind, nullableString(desired.SourceAccountID), nullableString(desired.ComicID),
			nullableString(desired.VisibilityScope), desired.VariantKey, nullableString(desired.ObservationContractID), desired.State,
			formatDemandTime(dueAt), formatDemandTime(oldestAt), formatDemandTime(nextEligibleAt), generation,
			priority, nullableString(lastError), formatDemandTime(now), existing.ID)
		if err != nil {
			return err
		}
		result = desired
		result.ID = existing.ID
		result.ExecutionGeneration = generation
		result.OldestAt = oldestAt
		result.DueAt = dueAt
		result.NextEligibleAt = nextEligibleAt
		result.PriorityClass = priority
		result.LastErrorCode = lastError
		result.Revision = existing.Revision + 1
		result.CreatedAt = existing.CreatedAt
		result.UpdatedAt = now
		return nil
	})
	return result, err
}

func (r *DemandRepository) MarkDemandState(ctx context.Context, demandID string, state DemandState, errorCode string) error {
	if demandID == "" || (state != DemandActive && state != DemandBlocked && state != DemandInactive) {
		return ErrInvalidDemand
	}
	return r.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		result, err := tx.ExecContext(ctx, `
			UPDATE scan_demands SET state = ?, last_error_code = ?, revision = revision + 1, updated_at = ?
			WHERE demand_id = ?`, state, nullableString(errorCode), formatDemandTime(r.repo.DB().Now()), demandID)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return ErrDemandNotFound
		}
		return nil
	})
}

func (r *DemandRepository) DeactivateMissing(ctx context.Context, artifactID string, kind DemandKind, activeKeys map[string]struct{}) error {
	if artifactID == "" || activeKeys == nil {
		return ErrInvalidDemand
	}
	demands, err := r.ListDemands(ctx, artifactID, kind)
	if err != nil {
		return err
	}
	for _, demand := range demands {
		if _, ok := activeKeys[demand.Key]; ok || demand.State == DemandInactive {
			continue
		}
		if err := r.MarkDemandState(ctx, demand.ID, DemandInactive, ""); err != nil {
			return err
		}
	}
	return nil
}

type MaterializeJobRequest struct {
	DemandID    string
	PayloadJSON string
	Kind        JobKind
}

func (r *DemandRepository) MaterializeJob(ctx context.Context, request MaterializeJobRequest) (Job, error) {
	if request.DemandID == "" || request.PayloadJSON == "" {
		return Job{}, ErrInvalidJob
	}
	if !json.Valid([]byte(request.PayloadJSON)) {
		return Job{}, ErrInvalidJob
	}
	if request.Kind != JobAccountSnapshotSlice && request.Kind != JobComicDetailBatch && request.Kind != JobMaintenance {
		return Job{}, ErrInvalidJob
	}
	var result Job
	err := r.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		demand, err := loadDemandTx(ctx, tx, request.DemandID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrDemandNotFound
		}
		if err != nil {
			return err
		}
		if demand.State != DemandActive || demand.ExecutionGeneration < 1 {
			return ErrDemandNotRunnable
		}
		if (demand.Kind == DemandAccountSnapshot && request.Kind != JobAccountSnapshotSlice) ||
			(demand.Kind == DemandComicDetail && request.Kind != JobComicDetailBatch) ||
			(demand.Kind == DemandMaintenance && request.Kind != JobMaintenance) {
			return ErrInvalidJob
		}
		var existing Job
		existingErr := scanJob(tx.QueryRowContext(ctx, jobSelect+" WHERE demand_id = ? AND execution_generation = ?", request.DemandID, demand.ExecutionGeneration), &existing)
		if existingErr == nil {
			result = existing
			return nil
		}
		if !errors.Is(existingErr, sql.ErrNoRows) {
			return existingErr
		}
		accountID, releaseID, epoch, err := chooseExecutorTx(ctx, tx, demand)
		if err != nil {
			return err
		}
		now := r.repo.DB().Now()
		result = Job{
			ID: tx.NewID("job"), DemandID: demand.ID, ArtifactID: demand.ArtifactID,
			Kind: request.Kind, ExecutionGeneration: demand.ExecutionGeneration,
			ExecutorSourceAccountID: accountID, PackageReleaseID: releaseID, FixedSessionEpoch: epoch,
			State: JobReady, AttemptCount: 0, NextEligibleAt: demand.NextEligibleAt,
			PayloadJSON: request.PayloadJSON, CreatedAt: now, UpdatedAt: now,
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO scan_jobs(
				job_id, demand_id, artifact_id, job_kind, execution_generation,
				executor_source_account_id, package_release_id, fixed_session_epoch,
				state, attempt_count, next_eligible_at, payload_json, created_at, updated_at
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'ready', 0, ?, ?, ?, ?)`,
			result.ID, result.DemandID, result.ArtifactID, result.Kind, result.ExecutionGeneration,
			result.ExecutorSourceAccountID, result.PackageReleaseID, result.FixedSessionEpoch,
			formatDemandTime(result.NextEligibleAt), result.PayloadJSON, formatDemandTime(now), formatDemandTime(now))
		return err
	})
	return result, err
}

func (r *DemandRepository) GetJob(ctx context.Context, jobID string) (Job, error) {
	if jobID == "" {
		return Job{}, ErrJobNotFound
	}
	var result Job
	err := r.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		return scanJob(tx.QueryRowContext(ctx, jobSelect+" WHERE job_id = ?", jobID), &result)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrJobNotFound
	}
	return result, err
}

func (r *DemandRepository) ListReadyJobs(ctx context.Context, artifactID string) ([]Job, error) {
	var jobs []Job
	err := r.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		query := jobSelect + " WHERE state IN ('ready','retryable') AND next_eligible_at <= ?"
		args := []any{formatDemandTime(r.repo.DB().Now())}
		if artifactID != "" {
			query += " AND artifact_id = ?"
			args = append(args, artifactID)
		}
		query += " ORDER BY artifact_id, next_eligible_at, created_at, job_id"
		rows, err := tx.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var job Job
			if err := scanJob(rows, &job); err != nil {
				return err
			}
			jobs = append(jobs, job)
		}
		return rows.Err()
	})
	return jobs, err
}

func (r *DemandRepository) LeaseJob(ctx context.Context, jobID, workerID string, lease time.Duration) (JobLease, error) {
	if jobID == "" || workerID == "" || lease <= 0 {
		return JobLease{}, ErrInvalidJob
	}
	var result JobLease
	err := r.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		now := r.repo.DB().Now()
		var current Job
		if err := scanJob(tx.QueryRowContext(ctx, jobSelect+" WHERE job_id = ?", jobID), &current); errors.Is(err, sql.ErrNoRows) {
			return ErrJobNotFound
		} else if err != nil {
			return err
		}
		if current.State != JobReady && current.State != JobRetryable {
			return ErrLeaseLost
		}
		if current.NextEligibleAt.After(now) {
			return ErrDemandNotRunnable
		}
		stale, err := jobStaleTx(ctx, tx, current)
		if err != nil {
			return err
		}
		if stale {
			if _, err := tx.ExecContext(ctx, `UPDATE scan_jobs SET state = 'cancelled', updated_at = ? WHERE job_id = ? AND state IN ('ready','retryable')`, formatDemandTime(now), jobID); err != nil {
				return err
			}
			return ErrStaleJob
		}
		leaseExpires := now.Add(lease)
		attemptNo := current.AttemptCount + 1
		update, err := tx.ExecContext(ctx, `
			UPDATE scan_jobs SET state = 'leased', lease_owner = ?, lease_expires_at = ?,
				attempt_count = ?, updated_at = ?
			WHERE job_id = ? AND state IN ('ready','retryable') AND next_eligible_at <= ?`,
			workerID, formatDemandTime(leaseExpires), attemptNo, formatDemandTime(now), jobID, formatDemandTime(now))
		if err != nil {
			return err
		}
		rows, err := update.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return ErrLeaseLost
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO scan_attempts(
				job_id, attempt_no, executor_source_account_id, worker_id, started_at, metrics_json
			) VALUES(?, ?, ?, ?, ?, '{}')`, jobID, attemptNo, current.ExecutorSourceAccountID, workerID, formatDemandTime(now)); err != nil {
			return err
		}
		current.State = JobLeased
		current.LeaseOwner = workerID
		current.LeaseExpiresAt = &leaseExpires
		current.AttemptCount = attemptNo
		current.UpdatedAt = now
		result = JobLease{Job: current, AttemptNo: attemptNo}
		return nil
	})
	return result, err
}

type CompleteJobRequest struct {
	JobID         string
	WorkerID      string
	Outcome       string
	ErrorCode     string
	ResultDigest  string
	MetricsJSON   string
	RetryAfter    time.Duration
	FinishedAt    time.Time
	ExpectedEpoch int64
}

// YieldJobRequest records a successfully processed slice while returning the
// durable job to ready.  A yielded slice is deliberately not a retry: the
// demand generation stays pinned until the final snapshot/detail publication
// completes.
type YieldJobRequest struct {
	JobID         string
	WorkerID      string
	ResultDigest  string
	MetricsJSON   string
	FinishedAt    time.Time
	ExpectedEpoch int64
}

func (r *DemandRepository) YieldJob(ctx context.Context, request YieldJobRequest) error {
	if request.JobID == "" || request.WorkerID == "" {
		return ErrInvalidJobCompletion
	}
	if request.MetricsJSON == "" {
		request.MetricsJSON = "{}"
	}
	if !json.Valid([]byte(request.MetricsJSON)) {
		return ErrInvalidJobCompletion
	}
	return r.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		now := r.repo.DB().Now()
		if request.FinishedAt.IsZero() {
			request.FinishedAt = now
		}
		var job Job
		if err := scanJob(tx.QueryRowContext(ctx, jobSelect+" WHERE job_id = ?", request.JobID), &job); errors.Is(err, sql.ErrNoRows) {
			return ErrJobNotFound
		} else if err != nil {
			return err
		}
		if job.State != JobLeased || job.LeaseOwner != request.WorkerID || job.LeaseExpiresAt == nil || !job.LeaseExpiresAt.After(now) {
			return ErrLeaseLost
		}
		stale := request.ExpectedEpoch > 0 && request.ExpectedEpoch != job.FixedSessionEpoch
		if !stale {
			var staleErr error
			stale, staleErr = jobStaleTx(ctx, tx, job)
			if staleErr != nil {
				return staleErr
			}
		}
		if stale {
			if err := finishLeasedJobTx(ctx, tx, job, request.WorkerID, JobCancelled, job.NextEligibleAt, "discarded", "stale_job", request); err != nil {
				return err
			}
			return ErrStaleJob
		}
		if err := finishLeasedJobTx(ctx, tx, job, request.WorkerID, JobReady, now, "succeeded", "", request); err != nil {
			return err
		}
		return nil
	})
}

func (r *DemandRepository) CompleteJob(ctx context.Context, request CompleteJobRequest) error {
	if request.JobID == "" || request.WorkerID == "" {
		return ErrInvalidJobCompletion
	}
	if request.Outcome != "succeeded" && request.Outcome != "retryable" && request.Outcome != "failed" && request.Outcome != "discarded" {
		return ErrInvalidJobCompletion
	}
	if request.MetricsJSON == "" {
		request.MetricsJSON = "{}"
	}
	if !json.Valid([]byte(request.MetricsJSON)) {
		return ErrInvalidJobCompletion
	}
	return r.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		now := r.repo.DB().Now()
		if request.FinishedAt.IsZero() {
			request.FinishedAt = now
		}
		var job Job
		if err := scanJob(tx.QueryRowContext(ctx, jobSelect+" WHERE job_id = ?", request.JobID), &job); errors.Is(err, sql.ErrNoRows) {
			return ErrJobNotFound
		} else if err != nil {
			return err
		}
		if job.State != JobLeased || job.LeaseOwner != request.WorkerID || job.LeaseExpiresAt == nil || !job.LeaseExpiresAt.After(now) {
			return ErrLeaseLost
		}
		stale := request.ExpectedEpoch > 0 && request.ExpectedEpoch != job.FixedSessionEpoch
		if !stale {
			var staleErr error
			stale, staleErr = jobStaleTx(ctx, tx, job)
			if staleErr != nil {
				return staleErr
			}
		}
		outcome := request.Outcome
		if stale {
			outcome = "discarded"
		}
		jobState := JobSucceeded
		nextEligible := job.NextEligibleAt
		switch outcome {
		case "retryable":
			jobState = JobRetryable
			delay := request.RetryAfter
			if delay <= 0 {
				delay = time.Hour
			}
			nextEligible = now.Add(delay)
		case "failed":
			jobState = JobFailed
		case "discarded":
			jobState = JobCancelled
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE scan_jobs SET state = ?, lease_owner = NULL, lease_expires_at = NULL,
				next_eligible_at = ?, result_digest = ?, updated_at = ?
			WHERE job_id = ? AND state = 'leased' AND lease_owner = ?`,
			jobState, formatDemandTime(nextEligible), nullableString(request.ResultDigest), formatDemandTime(now), request.JobID, request.WorkerID)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return ErrLeaseLost
		}
		result, err = tx.ExecContext(ctx, `
			UPDATE scan_attempts SET finished_at = ?, outcome = ?, error_code = ?, metrics_json = ?
			WHERE job_id = ? AND attempt_no = ? AND worker_id = ?`,
			formatDemandTime(request.FinishedAt), outcome, nullableString(request.ErrorCode), request.MetricsJSON, request.JobID, job.AttemptCount, request.WorkerID)
		if err != nil {
			return err
		}
		rows, err = result.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return ErrLeaseLost
		}
		var updateErr error
		if outcome == "succeeded" {
			nextEligible, updateErr = r.nextDemandEligibilityTx(ctx, tx, job, now)
			if updateErr == nil {
				var result sql.Result
				result, updateErr = tx.ExecContext(ctx, `
					UPDATE scan_demands SET due_at = ?, next_eligible_at = ?, execution_generation = execution_generation + 1,
						priority_class = 'normal', last_error_code = NULL, revision = revision + 1, updated_at = ?
					WHERE demand_id = ? AND execution_generation = ? AND state = 'active'`,
					formatDemandTime(now), formatDemandTime(nextEligible), formatDemandTime(now), job.DemandID, job.ExecutionGeneration)
				if updateErr == nil {
					var rows int64
					rows, updateErr = result.RowsAffected()
					if updateErr == nil && rows != 1 {
						updateErr = ErrStaleJob
					}
				}
			}
		} else if outcome == "retryable" {
			var result sql.Result
			result, updateErr = tx.ExecContext(ctx, `
				UPDATE scan_demands SET next_eligible_at = ?, priority_class = 'normal', last_error_code = ?,
					revision = revision + 1, updated_at = ?
				WHERE demand_id = ? AND execution_generation = ? AND state = 'active'`,
				formatDemandTime(nextEligible), nullableString(request.ErrorCode), formatDemandTime(now), job.DemandID, job.ExecutionGeneration)
			if updateErr == nil {
				var rows int64
				rows, updateErr = result.RowsAffected()
				if updateErr == nil && rows != 1 {
					updateErr = ErrStaleJob
				}
			}
		}
		return updateErr
	})
}

// DiscardJobAndAdvance is used when a scan proves that its source snapshot
// cannot be resumed.  The leased attempt is discarded and the demand moves to
// a new generation in the same transaction, so a stale result cannot publish.
func (r *DemandRepository) DiscardJobAndAdvance(ctx context.Context, request CompleteJobRequest) error {
	if request.JobID == "" || request.WorkerID == "" {
		return ErrInvalidJobCompletion
	}
	if request.MetricsJSON == "" {
		request.MetricsJSON = "{}"
	}
	if !json.Valid([]byte(request.MetricsJSON)) {
		return ErrInvalidJobCompletion
	}
	return r.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		now := r.repo.DB().Now()
		var job Job
		if err := scanJob(tx.QueryRowContext(ctx, jobSelect+" WHERE job_id = ?", request.JobID), &job); errors.Is(err, sql.ErrNoRows) {
			return ErrJobNotFound
		} else if err != nil {
			return err
		}
		if job.State != JobLeased || job.LeaseOwner != request.WorkerID || job.LeaseExpiresAt == nil || !job.LeaseExpiresAt.After(now) {
			return ErrLeaseLost
		}
		if request.ExpectedEpoch > 0 && request.ExpectedEpoch != job.FixedSessionEpoch {
			request.ErrorCode = "stale_job"
		}
		if err := finishLeasedJobTx(ctx, tx, job, request.WorkerID, JobCancelled, job.NextEligibleAt, "discarded", request.ErrorCode, request); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE scan_demands SET execution_generation = execution_generation + 1,
				due_at = ?, next_eligible_at = ?, priority_class = 'normal', last_error_code = ?,
				revision = revision + 1, updated_at = ?
			WHERE demand_id = ? AND execution_generation = ? AND state = 'active'`,
			formatDemandTime(now), formatDemandTime(now), nullableString(request.ErrorCode), formatDemandTime(now), job.DemandID, job.ExecutionGeneration)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return ErrStaleJob
		}
		return nil
	})
}

func finishLeasedJobTx(ctx context.Context, tx *v2store.Tx, job Job, workerID string, state JobState, nextEligible time.Time, outcome, errorCode string, request interface {
	getResultDigest() string
	getMetricsJSON() string
	getFinishedAt() time.Time
}) error {
	result, err := tx.ExecContext(ctx, `
		UPDATE scan_jobs SET state = ?, lease_owner = NULL, lease_expires_at = NULL,
			next_eligible_at = ?, result_digest = ?, updated_at = ?
		WHERE job_id = ? AND state = 'leased' AND lease_owner = ?`,
		state, formatDemandTime(nextEligible), nullableString(request.getResultDigest()), formatDemandTime(tx.Now()), job.ID, workerID)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return ErrLeaseLost
	}
	finishedAt := request.getFinishedAt()
	if finishedAt.IsZero() {
		finishedAt = tx.Now()
	}
	result, err = tx.ExecContext(ctx, `
		UPDATE scan_attempts SET finished_at = ?, outcome = ?, error_code = ?, metrics_json = ?
		WHERE job_id = ? AND attempt_no = ? AND worker_id = ?`,
		formatDemandTime(finishedAt), outcome, nullableString(errorCode), request.getMetricsJSON(), job.ID, job.AttemptCount, workerID)
	if err != nil {
		return err
	}
	rows, err = result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (request YieldJobRequest) getResultDigest() string  { return request.ResultDigest }
func (request YieldJobRequest) getMetricsJSON() string   { return request.MetricsJSON }
func (request YieldJobRequest) getFinishedAt() time.Time { return request.FinishedAt }

func (request CompleteJobRequest) getResultDigest() string  { return request.ResultDigest }
func (request CompleteJobRequest) getMetricsJSON() string   { return request.MetricsJSON }
func (request CompleteJobRequest) getFinishedAt() time.Time { return request.FinishedAt }

func (r *DemandRepository) nextDemandEligibilityTx(ctx context.Context, tx *v2store.Tx, job Job, now time.Time) (time.Time, error) {
	if job.Kind == JobAccountSnapshotSlice {
		var next string
		err := tx.QueryRowContext(ctx, `SELECT COALESCE(next_evaluation_at, '') FROM source_scan_status WHERE source_account_id = ?`, job.ExecutorSourceAccountID).Scan(&next)
		if err == nil && next != "" {
			parsed, parseErr := parseDemandTime(next)
			if parseErr != nil {
				return time.Time{}, parseErr
			}
			return parsed, nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return time.Time{}, err
		}
		return now, nil
	}
	if job.Kind == JobComicDetailBatch {
		var freshUntil string
		err := tx.QueryRowContext(ctx, `
			SELECT COALESCE(fresh_until, '') FROM content_observations o
			JOIN scan_demands d ON d.artifact_id = o.artifact_id AND d.comic_id = o.comic_id
				AND d.visibility_scope = o.visibility_scope AND d.variant_key = o.variant_key
				AND d.observation_contract_id = o.observation_contract_id
			WHERE d.demand_id = ? ORDER BY o.updated_at DESC LIMIT 1`, job.DemandID).Scan(&freshUntil)
		if err == nil && freshUntil != "" {
			parsed, parseErr := parseDemandTime(freshUntil)
			if parseErr != nil {
				return time.Time{}, parseErr
			}
			return parsed, nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return time.Time{}, err
		}
	}
	return now, nil
}

func (r *DemandRepository) RecoverExpiredLeases(ctx context.Context) (int, error) {
	var recovered int
	err := r.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		now := r.repo.DB().Now()
		rows, err := tx.QueryContext(ctx, `SELECT job_id, attempt_count FROM scan_jobs WHERE state = 'leased' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?`, formatDemandTime(now))
		if err != nil {
			return err
		}
		defer rows.Close()
		type expiredJob struct {
			id      string
			attempt int
		}
		var jobs []expiredJob
		for rows.Next() {
			var item expiredJob
			if err := rows.Scan(&item.id, &item.attempt); err != nil {
				return err
			}
			jobs = append(jobs, item)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		for _, item := range jobs {
			if _, err := tx.ExecContext(ctx, `UPDATE scan_jobs SET state = 'retryable', lease_owner = NULL, lease_expires_at = NULL, next_eligible_at = ?, updated_at = ? WHERE job_id = ? AND state = 'leased'`, formatDemandTime(now), formatDemandTime(now), item.id); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE scan_attempts SET finished_at = ?, outcome = 'leaseLost', error_code = 'lease_expired' WHERE job_id = ? AND attempt_no = ? AND outcome IS NULL`, formatDemandTime(now), item.id, item.attempt); err != nil {
				return err
			}
			recovered++
		}
		return nil
	})
	return recovered, err
}

const demandSelect = `SELECT demand_id, demand_key, artifact_id, demand_kind,
       COALESCE(source_account_id, ''), COALESCE(comic_id, ''), COALESCE(visibility_scope, ''),
       variant_key, COALESCE(observation_contract_id, ''), state, due_at, oldest_at,
       next_eligible_at, execution_generation, priority_class, COALESCE(last_error_code, ''),
       revision, created_at, updated_at FROM scan_demands`

const jobSelect = `SELECT job_id, demand_id, artifact_id, job_kind, execution_generation,
       executor_source_account_id, package_release_id, fixed_session_epoch, state,
       COALESCE(lease_owner, ''), COALESCE(lease_expires_at, ''), attempt_count, next_eligible_at,
       payload_json, COALESCE(result_digest, ''), created_at, updated_at FROM scan_jobs`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanDemand(row rowScanner, demand *Demand) error {
	var dueAt, oldestAt, nextEligibleAt, createdAt, updatedAt string
	var revision int64
	if err := row.Scan(&demand.ID, &demand.Key, &demand.ArtifactID, &demand.Kind,
		&demand.SourceAccountID, &demand.ComicID, &demand.VisibilityScope, &demand.VariantKey,
		&demand.ObservationContractID, &demand.State, &dueAt, &oldestAt, &nextEligibleAt,
		&demand.ExecutionGeneration, &demand.PriorityClass, &demand.LastErrorCode, &revision,
		&createdAt, &updatedAt); err != nil {
		return err
	}
	var err error
	if demand.DueAt, err = parseDemandTime(dueAt); err != nil {
		return err
	}
	if demand.OldestAt, err = parseDemandTime(oldestAt); err != nil {
		return err
	}
	if demand.NextEligibleAt, err = parseDemandTime(nextEligibleAt); err != nil {
		return err
	}
	if demand.CreatedAt, err = parseDemandTime(createdAt); err != nil {
		return err
	}
	if demand.UpdatedAt, err = parseDemandTime(updatedAt); err != nil {
		return err
	}
	demand.Revision = v2domain.Revision(revision)
	return nil
}

func scanJob(row rowScanner, job *Job) error {
	var leaseExpires, nextEligible, createdAt, updatedAt string
	var leaseOwner, resultDigest string
	var fixedEpoch int64
	if err := row.Scan(&job.ID, &job.DemandID, &job.ArtifactID, &job.Kind, &job.ExecutionGeneration,
		&job.ExecutorSourceAccountID, &job.PackageReleaseID, &fixedEpoch, &job.State,
		&leaseOwner, &leaseExpires, &job.AttemptCount, &nextEligible, &job.PayloadJSON,
		&resultDigest, &createdAt, &updatedAt); err != nil {
		return err
	}
	job.FixedSessionEpoch = fixedEpoch
	job.LeaseOwner = leaseOwner
	job.ResultDigest = resultDigest
	var err error
	if leaseExpires != "" {
		value, parseErr := parseDemandTime(leaseExpires)
		if parseErr != nil {
			return parseErr
		}
		job.LeaseExpiresAt = &value
	}
	if job.NextEligibleAt, err = parseDemandTime(nextEligible); err != nil {
		return err
	}
	if job.CreatedAt, err = parseDemandTime(createdAt); err != nil {
		return err
	}
	if job.UpdatedAt, err = parseDemandTime(updatedAt); err != nil {
		return err
	}
	return nil
}

func validateDemand(demand Demand) error {
	if demand.Key == "" || demand.ArtifactID == "" || demand.State == "" || demand.DueAt.IsZero() || demand.OldestAt.IsZero() || demand.NextEligibleAt.IsZero() {
		return ErrInvalidDemand
	}
	if demand.Kind != DemandAccountSnapshot && demand.Kind != DemandComicDetail && demand.Kind != DemandMaintenance {
		return ErrInvalidDemand
	}
	if demand.State == DemandActive && demand.ExecutionGeneration < 0 {
		return ErrInvalidDemand
	}
	if demand.Kind == DemandAccountSnapshot && demand.SourceAccountID == "" {
		return ErrInvalidDemand
	}
	if demand.Kind == DemandComicDetail && (demand.ComicID == "" || demand.VisibilityScope == "" || demand.ObservationContractID == "") {
		return ErrInvalidDemand
	}
	if demand.PriorityClass != "" && demand.PriorityClass != PriorityNormal && demand.PriorityClass != PriorityExpedited {
		return ErrInvalidDemand
	}
	return nil
}

func insertDemand(ctx context.Context, tx *v2store.Tx, demand Demand) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO scan_demands(
			demand_id, demand_key, artifact_id, demand_kind, source_account_id, comic_id,
			visibility_scope, variant_key, observation_contract_id, state, due_at, oldest_at,
			next_eligible_at, execution_generation, priority_class, last_error_code,
			revision, created_at, updated_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)`,
		demand.ID, demand.Key, demand.ArtifactID, demand.Kind, nullableString(demand.SourceAccountID), nullableString(demand.ComicID),
		nullableString(demand.VisibilityScope), demand.VariantKey, nullableString(demand.ObservationContractID), demand.State,
		formatDemandTime(demand.DueAt), formatDemandTime(demand.OldestAt), formatDemandTime(demand.NextEligibleAt),
		demand.ExecutionGeneration, demand.PriorityClass, nullableString(demand.LastErrorCode), formatDemandTime(demand.CreatedAt), formatDemandTime(demand.UpdatedAt))
	return err
}

func loadDemandTx(ctx context.Context, tx *v2store.Tx, demandID string) (Demand, error) {
	var demand Demand
	err := scanDemand(tx.QueryRowContext(ctx, demandSelect+" WHERE demand_id = ?", demandID), &demand)
	return demand, err
}

func chooseExecutorTx(ctx context.Context, tx *v2store.Tx, demand Demand) (string, string, int64, error) {
	var accountID, releaseID string
	var epoch int64
	now := formatDemandTime(tx.Now())
	if demand.Kind == DemandAccountSnapshot {
		if err := tx.QueryRowContext(ctx, `
			SELECT a.source_account_id, s.session_epoch
			FROM source_accounts a JOIN source_account_sessions s ON s.source_account_id = a.source_account_id
			WHERE a.source_account_id = ? AND a.artifact_id = ? AND a.state = 'active'
			  AND (s.expires_at IS NULL OR s.expires_at > ?)
			  AND NOT EXISTS (SELECT 1 FROM source_scan_status ss WHERE ss.source_account_id = a.source_account_id AND ss.status IN ('blocked','reauthRequired','paused'))`, demand.SourceAccountID, demand.ArtifactID, now).Scan(&accountID, &epoch); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return "", "", 0, ErrNoExecutorAccount
			}
			return "", "", 0, err
		}
	} else if demand.Kind == DemandComicDetail {
		if err := tx.QueryRowContext(ctx, `
			SELECT a.source_account_id, s.session_epoch
			FROM source_accounts a JOIN source_account_sessions s ON s.source_account_id = a.source_account_id
			WHERE a.artifact_id = ? AND a.state = 'active' AND a.visibility_scope = ?
			  AND a.scope_fresh_until > ?
			  AND (s.expires_at IS NULL OR s.expires_at > ?)
			  AND NOT EXISTS (SELECT 1 FROM source_scan_status ss WHERE ss.source_account_id = a.source_account_id AND ss.status IN ('blocked','reauthRequired','paused'))
			ORDER BY a.source_account_id`, demand.ArtifactID, demand.VisibilityScope, now, now).Scan(&accountID, &epoch); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return "", "", 0, ErrNoExecutorAccount
			}
			return "", "", 0, err
		}
	} else {
		if err := tx.QueryRowContext(ctx, `
			SELECT a.source_account_id, s.session_epoch
			FROM source_accounts a JOIN source_account_sessions s ON s.source_account_id = a.source_account_id
			WHERE a.artifact_id = ? AND a.state = 'active'
			  AND (s.expires_at IS NULL OR s.expires_at > ?)
			  AND NOT EXISTS (SELECT 1 FROM source_scan_status ss WHERE ss.source_account_id = a.source_account_id AND ss.status IN ('blocked','reauthRequired','paused'))
			ORDER BY a.source_account_id`, demand.ArtifactID, now).Scan(&accountID, &epoch); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return "", "", 0, ErrNoExecutorAccount
			}
			return "", "", 0, err
		}
	}
	if err := tx.QueryRowContext(ctx, `SELECT package_release_id FROM source_package_releases WHERE artifact_id = ? AND state = 'active'`, demand.ArtifactID).Scan(&releaseID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", 0, ErrNoExecutorAccount
		}
		return "", "", 0, err
	}
	return accountID, releaseID, epoch, nil
}

func jobStaleTx(ctx context.Context, tx *v2store.Tx, job Job) (bool, error) {
	var demandState string
	var demandGeneration int64
	if err := tx.QueryRowContext(ctx, `SELECT state, execution_generation FROM scan_demands WHERE demand_id = ?`, job.DemandID).Scan(&demandState, &demandGeneration); errors.Is(err, sql.ErrNoRows) {
		return true, nil
	} else if err != nil {
		return false, err
	}
	if demandState != string(DemandActive) || demandGeneration != job.ExecutionGeneration {
		return true, nil
	}
	var epoch int64
	var accountState string
	if err := tx.QueryRowContext(ctx, `SELECT session_epoch, state FROM source_accounts WHERE source_account_id = ?`, job.ExecutorSourceAccountID).Scan(&epoch, &accountState); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return true, nil
		}
		return false, err
	}
	if accountState != string(v2domain.SourceAccountActive) || epoch != job.FixedSessionEpoch {
		return true, nil
	}
	var releaseState string
	if err := tx.QueryRowContext(ctx, `SELECT state FROM source_package_releases WHERE package_release_id = ? AND artifact_id = ?`, job.PackageReleaseID, job.ArtifactID).Scan(&releaseState); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return true, nil
		}
		return false, err
	}
	return releaseState != "active", nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func formatDemandTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func parseDemandTime(value string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, value)
}

func digestJSON(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func canonicalJSON(value []byte) ([]byte, error) {
	var decoded any
	decoder := json.NewDecoder(strings.NewReader(string(value)))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, errors.New("multiple JSON values")
		}
		return nil, err
	}
	return json.Marshal(decoded)
}
func sortedDemandKeys(demands []Demand) []string {
	keys := make([]string, 0, len(demands))
	for _, demand := range demands {
		keys = append(keys, demand.Key)
	}
	sort.Strings(keys)
	return keys
}

func requestDigest(value any) string {
	encoded, _ := json.Marshal(value)
	return digestJSON(encoded)
}
