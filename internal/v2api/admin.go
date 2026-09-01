package v2api

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"os"
	urlpath "path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"venera-server/internal/v2store"
)

const (
	adminRecentJobLimit = 100
	adminAttemptLimit   = 10
)

var errAdminJobNotFound = errors.New("admin job not found")

type adminDemandCountsWire struct {
	Active   int64 `json:"active"`
	Blocked  int64 `json:"blocked"`
	Inactive int64 `json:"inactive"`
}

type adminJobCountsWire struct {
	Ready     int64 `json:"ready"`
	Leased    int64 `json:"leased"`
	Retryable int64 `json:"retryable"`
}

type adminOutcomeCountsWire struct {
	Succeeded int64 `json:"succeeded"`
	Failed    int64 `json:"failed"`
	Cancelled int64 `json:"cancelled"`
}

type adminGroupCountWire struct {
	Kind  string `json:"kind,omitempty"`
	State string `json:"state"`
	Count int64  `json:"count"`
}

type adminLaneWire struct {
	ArtifactID           string                `json:"artifactId"`
	RuntimeState         *string               `json:"runtimeState"`
	TargetConcurrency    *int64                `json:"targetConcurrency"`
	EffectiveConcurrency *int64                `json:"effectiveConcurrency"`
	NextEligibleAt       *string               `json:"nextEligibleAt"`
	LastErrorCode        *string               `json:"lastErrorCode"`
	Demands              []adminGroupCountWire `json:"demands"`
	Jobs                 []adminGroupCountWire `json:"jobs"`
}

type adminRecentJobWire struct {
	JobID          string  `json:"jobId"`
	DemandID       string  `json:"demandId"`
	ArtifactID     string  `json:"artifactId"`
	JobKind        string  `json:"jobKind"`
	State          string  `json:"state"`
	AttemptCount   int64   `json:"attemptCount"`
	CreatedAt      string  `json:"createdAt"`
	UpdatedAt      string  `json:"updatedAt"`
	NextEligibleAt string  `json:"nextEligibleAt"`
	LeaseExpiresAt *string `json:"leaseExpiresAt"`
	StartedAt      *string `json:"startedAt"`
	FinishedAt     *string `json:"finishedAt"`
	LastOutcome    *string `json:"lastOutcome"`
	LastErrorCode  *string `json:"lastErrorCode"`
}

type adminOverviewWire struct {
	ObservedAt          string                 `json:"observedAt"`
	DemandCounts        adminDemandCountsWire  `json:"demandCounts"`
	CurrentJobCounts    adminJobCountsWire     `json:"currentJobCounts"`
	RecentOutcomeCounts adminOutcomeCountsWire `json:"recentOutcomeCounts"`
	PausedLaneCount     int64                  `json:"pausedLaneCount"`
	Lanes               []adminLaneWire        `json:"lanes"`
	RecentJobs          []adminRecentJobWire   `json:"recentJobs"`
	RecentJobLimit      int                    `json:"recentJobLimit"`
	RecentJobsTruncated bool                   `json:"recentJobsTruncated"`
}

type adminDemandWire struct {
	DemandID            string  `json:"demandId"`
	ArtifactID          string  `json:"artifactId"`
	DemandKind          string  `json:"demandKind"`
	State               string  `json:"state"`
	DueAt               string  `json:"dueAt"`
	OldestAt            string  `json:"oldestAt"`
	NextEligibleAt      string  `json:"nextEligibleAt"`
	ExecutionGeneration int64   `json:"executionGeneration"`
	PriorityClass       string  `json:"priorityClass"`
	LastErrorCode       *string `json:"lastErrorCode"`
}

type adminJobWire struct {
	adminRecentJobWire
	ExecutionGeneration int64   `json:"executionGeneration"`
	ResultDigest        *string `json:"resultDigest"`
}

type adminAttemptWire struct {
	AttemptNo  int64   `json:"attemptNo"`
	StartedAt  string  `json:"startedAt"`
	FinishedAt *string `json:"finishedAt"`
	Outcome    *string `json:"outcome"`
	ErrorCode  *string `json:"errorCode"`
}

type adminAttemptsWire struct {
	Total int64              `json:"total"`
	Items []adminAttemptWire `json:"items"`
}

type adminSnapshotRunWire struct {
	SnapshotRunID string  `json:"snapshotRunId"`
	RunGeneration int64   `json:"runGeneration"`
	State         string  `json:"state"`
	ItemCount     int64   `json:"itemCount"`
	StartedAt     string  `json:"startedAt"`
	UpdatedAt     string  `json:"updatedAt"`
	CompletedAt   *string `json:"completedAt"`
	FailureCode   *string `json:"failureCode"`
}

type adminJobDetailWire struct {
	Job         adminJobWire          `json:"job"`
	Demand      adminDemandWire       `json:"demand"`
	Attempts    adminAttemptsWire     `json:"attempts"`
	SnapshotRun *adminSnapshotRunWire `json:"snapshotRun"`
}

func (router *Router) registerAdminRoutes() {
	router.mux.HandleFunc("GET /admin", func(w http.ResponseWriter, request *http.Request) {
		http.Redirect(w, request, "/admin/", http.StatusPermanentRedirect)
	})
	router.mux.HandleFunc("GET /admin/api/overview", router.adminOverviewHandler)
	router.mux.HandleFunc("GET /admin/api/jobs/{jobId}", router.adminJobDetailHandler)
	router.mux.HandleFunc("GET /admin/api", http.NotFound)
	router.mux.HandleFunc("GET /admin/api/", http.NotFound)
	router.mux.HandleFunc("POST /admin/api", http.NotFound)
	router.mux.HandleFunc("POST /admin/api/", http.NotFound)
	router.mux.HandleFunc("GET /admin/", router.adminStaticHandler)
}

func (router *Router) adminOverviewHandler(w http.ResponseWriter, request *http.Request) {
	overview, err := router.loadAdminOverview(request.Context())
	if err != nil {
		writeAPIError(router, w, request, http.StatusInternalServerError, "internal_error", "queue snapshot is unavailable", true, nil)
		return
	}
	writeJSON(router, w, request, http.StatusOK, overview)
}

func (router *Router) adminJobDetailHandler(w http.ResponseWriter, request *http.Request) {
	detail, err := router.loadAdminJobDetail(request.Context(), strings.TrimSpace(request.PathValue("jobId")))
	if errors.Is(err, errAdminJobNotFound) {
		writeAPIError(router, w, request, http.StatusNotFound, "job_not_found", "job was not found", false, nil)
		return
	}
	if err != nil {
		writeAPIError(router, w, request, http.StatusInternalServerError, "internal_error", "job detail is unavailable", true, nil)
		return
	}
	writeJSON(router, w, request, http.StatusOK, detail)
}

func (router *Router) loadAdminOverview(ctx context.Context) (adminOverviewWire, error) {
	result := adminOverviewWire{
		Lanes:          []adminLaneWire{},
		RecentJobs:     []adminRecentJobWire{},
		RecentJobLimit: adminRecentJobLimit,
	}
	err := router.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		result.ObservedAt = tx.Now().Format(time.RFC3339Nano)
		lanes := make(map[string]*adminLaneWire)

		runtimeRows, err := tx.QueryContext(ctx, `
			SELECT artifact_id, state, target_concurrency, effective_concurrency,
			       next_eligible_at, last_error_code
			FROM source_runtime_state ORDER BY artifact_id`)
		if err != nil {
			return err
		}
		for runtimeRows.Next() {
			var artifactID, state string
			var target, effective int64
			var nextEligible, lastError sql.NullString
			if err := runtimeRows.Scan(&artifactID, &state, &target, &effective, &nextEligible, &lastError); err != nil {
				runtimeRows.Close()
				return err
			}
			lane := ensureAdminLane(lanes, artifactID)
			lane.RuntimeState = stringPtr(state)
			lane.TargetConcurrency = int64Ptr(target)
			lane.EffectiveConcurrency = int64Ptr(effective)
			lane.NextEligibleAt = nullStringPtr(nextEligible)
			lane.LastErrorCode = nullStringPtr(lastError)
			if state == "paused" {
				result.PausedLaneCount++
			}
		}
		if err := runtimeRows.Close(); err != nil {
			return err
		}

		demandRows, err := tx.QueryContext(ctx, `
			SELECT artifact_id, demand_kind, state, COUNT(*)
			FROM scan_demands GROUP BY artifact_id, demand_kind, state
			ORDER BY artifact_id, demand_kind, state`)
		if err != nil {
			return err
		}
		for demandRows.Next() {
			var artifactID, kind, state string
			var count int64
			if err := demandRows.Scan(&artifactID, &kind, &state, &count); err != nil {
				demandRows.Close()
				return err
			}
			ensureAdminLane(lanes, artifactID).Demands = append(ensureAdminLane(lanes, artifactID).Demands, adminGroupCountWire{Kind: kind, State: state, Count: count})
			switch state {
			case "active":
				result.DemandCounts.Active += count
			case "blocked":
				result.DemandCounts.Blocked += count
			case "inactive":
				result.DemandCounts.Inactive += count
			}
		}
		if err := demandRows.Close(); err != nil {
			return err
		}

		jobRows, err := tx.QueryContext(ctx, `
			SELECT artifact_id, state, COUNT(*) FROM scan_jobs
			WHERE state IN ('ready','leased','retryable')
			GROUP BY artifact_id, state ORDER BY artifact_id, state`)
		if err != nil {
			return err
		}
		for jobRows.Next() {
			var artifactID, state string
			var count int64
			if err := jobRows.Scan(&artifactID, &state, &count); err != nil {
				jobRows.Close()
				return err
			}
			lane := ensureAdminLane(lanes, artifactID)
			lane.Jobs = append(lane.Jobs, adminGroupCountWire{State: state, Count: count})
			switch state {
			case "ready":
				result.CurrentJobCounts.Ready += count
			case "leased":
				result.CurrentJobCounts.Leased += count
			case "retryable":
				result.CurrentJobCounts.Retryable += count
			}
		}
		if err := jobRows.Close(); err != nil {
			return err
		}

		recentRows, err := tx.QueryContext(ctx, adminRecentJobsSQL, adminRecentJobLimit+1)
		if err != nil {
			return err
		}
		for recentRows.Next() {
			job, err := scanAdminRecentJob(recentRows)
			if err != nil {
				recentRows.Close()
				return err
			}
			result.RecentJobs = append(result.RecentJobs, job)
		}
		if err := recentRows.Close(); err != nil {
			return err
		}
		if len(result.RecentJobs) > adminRecentJobLimit {
			result.RecentJobsTruncated = true
			result.RecentJobs = result.RecentJobs[:adminRecentJobLimit]
		}
		for _, job := range result.RecentJobs {
			switch job.State {
			case "succeeded":
				result.RecentOutcomeCounts.Succeeded++
			case "failed":
				result.RecentOutcomeCounts.Failed++
			case "cancelled":
				result.RecentOutcomeCounts.Cancelled++
			}
		}

		artifactIDs := make([]string, 0, len(lanes))
		for artifactID := range lanes {
			artifactIDs = append(artifactIDs, artifactID)
		}
		sort.Strings(artifactIDs)
		for _, artifactID := range artifactIDs {
			result.Lanes = append(result.Lanes, *lanes[artifactID])
		}
		return nil
	})
	return result, err
}

const adminRecentJobsSQL = `
	SELECT j.job_id, j.demand_id, j.artifact_id, j.job_kind, j.state,
	       j.attempt_count, j.created_at, j.updated_at, j.next_eligible_at,
	       j.lease_expires_at,
	       (SELECT MIN(a.started_at) FROM scan_attempts a WHERE a.job_id = j.job_id),
	       (SELECT a.finished_at FROM scan_attempts a WHERE a.job_id = j.job_id ORDER BY a.attempt_no DESC LIMIT 1),
	       (SELECT a.outcome FROM scan_attempts a WHERE a.job_id = j.job_id ORDER BY a.attempt_no DESC LIMIT 1),
	       (SELECT a.error_code FROM scan_attempts a WHERE a.job_id = j.job_id ORDER BY a.attempt_no DESC LIMIT 1)
	FROM scan_jobs j ORDER BY j.updated_at DESC, j.job_id DESC LIMIT ?`

type adminRowScanner interface {
	Scan(dest ...any) error
}

func scanAdminRecentJob(row adminRowScanner) (adminRecentJobWire, error) {
	var result adminRecentJobWire
	var leaseExpires, started, finished, outcome, lastError sql.NullString
	err := row.Scan(
		&result.JobID, &result.DemandID, &result.ArtifactID, &result.JobKind, &result.State,
		&result.AttemptCount, &result.CreatedAt, &result.UpdatedAt, &result.NextEligibleAt,
		&leaseExpires, &started, &finished, &outcome, &lastError,
	)
	result.LeaseExpiresAt = nullStringPtr(leaseExpires)
	result.StartedAt = nullStringPtr(started)
	result.FinishedAt = nullStringPtr(finished)
	result.LastOutcome = nullStringPtr(outcome)
	result.LastErrorCode = nullStringPtr(lastError)
	return result, err
}

func (router *Router) loadAdminJobDetail(ctx context.Context, jobID string) (adminJobDetailWire, error) {
	var result adminJobDetailWire
	result.Attempts.Items = []adminAttemptWire{}
	if jobID == "" {
		return result, errAdminJobNotFound
	}
	err := router.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		var leaseExpires, started, finished, outcome, jobError, resultDigest sql.NullString
		var demandError, sourceAccount sql.NullString
		err := tx.QueryRowContext(ctx, `
			SELECT j.job_id, j.demand_id, j.artifact_id, j.job_kind, j.state,
			       j.attempt_count, j.created_at, j.updated_at, j.next_eligible_at,
			       j.lease_expires_at,
			       (SELECT MIN(a.started_at) FROM scan_attempts a WHERE a.job_id = j.job_id),
			       (SELECT a.finished_at FROM scan_attempts a WHERE a.job_id = j.job_id ORDER BY a.attempt_no DESC LIMIT 1),
			       (SELECT a.outcome FROM scan_attempts a WHERE a.job_id = j.job_id ORDER BY a.attempt_no DESC LIMIT 1),
			       (SELECT a.error_code FROM scan_attempts a WHERE a.job_id = j.job_id ORDER BY a.attempt_no DESC LIMIT 1),
			       j.execution_generation, j.result_digest,
			       d.demand_id, d.artifact_id, d.demand_kind, d.state, d.due_at,
			       d.oldest_at, d.next_eligible_at, d.execution_generation,
			       d.priority_class, d.last_error_code, d.source_account_id
			FROM scan_jobs j JOIN scan_demands d ON d.demand_id = j.demand_id
			WHERE j.job_id = ?`, jobID).Scan(
			&result.Job.JobID, &result.Job.DemandID, &result.Job.ArtifactID, &result.Job.JobKind, &result.Job.State,
			&result.Job.AttemptCount, &result.Job.CreatedAt, &result.Job.UpdatedAt, &result.Job.NextEligibleAt,
			&leaseExpires, &started, &finished, &outcome, &jobError,
			&result.Job.ExecutionGeneration, &resultDigest,
			&result.Demand.DemandID, &result.Demand.ArtifactID, &result.Demand.DemandKind, &result.Demand.State,
			&result.Demand.DueAt, &result.Demand.OldestAt, &result.Demand.NextEligibleAt,
			&result.Demand.ExecutionGeneration, &result.Demand.PriorityClass, &demandError, &sourceAccount,
		)
		if errors.Is(err, sql.ErrNoRows) {
			return errAdminJobNotFound
		}
		if err != nil {
			return err
		}
		result.Job.LeaseExpiresAt = nullStringPtr(leaseExpires)
		result.Job.StartedAt = nullStringPtr(started)
		result.Job.FinishedAt = nullStringPtr(finished)
		result.Job.LastOutcome = nullStringPtr(outcome)
		result.Job.LastErrorCode = nullStringPtr(jobError)
		result.Job.ResultDigest = nullStringPtr(resultDigest)
		result.Demand.LastErrorCode = nullStringPtr(demandError)

		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM scan_attempts WHERE job_id = ?`, jobID).Scan(&result.Attempts.Total); err != nil {
			return err
		}
		attemptRows, err := tx.QueryContext(ctx, `
			SELECT attempt_no, started_at, finished_at, outcome, error_code
			FROM scan_attempts WHERE job_id = ? ORDER BY attempt_no DESC LIMIT ?`, jobID, adminAttemptLimit)
		if err != nil {
			return err
		}
		for attemptRows.Next() {
			var item adminAttemptWire
			var finishedAt, attemptOutcome, errorCode sql.NullString
			if err := attemptRows.Scan(&item.AttemptNo, &item.StartedAt, &finishedAt, &attemptOutcome, &errorCode); err != nil {
				attemptRows.Close()
				return err
			}
			item.FinishedAt = nullStringPtr(finishedAt)
			item.Outcome = nullStringPtr(attemptOutcome)
			item.ErrorCode = nullStringPtr(errorCode)
			result.Attempts.Items = append(result.Attempts.Items, item)
		}
		if err := attemptRows.Close(); err != nil {
			return err
		}

		if result.Demand.DemandKind == "accountSnapshot" && sourceAccount.Valid {
			var snapshot adminSnapshotRunWire
			var completed, failure sql.NullString
			err := tx.QueryRowContext(ctx, `
				SELECT snapshot_run_id, run_generation, state, item_count,
				       started_at, updated_at, completed_at, failure_code
				FROM favorite_snapshot_runs
				WHERE source_account_id = ? AND run_generation = ?`,
				sourceAccount.String, result.Job.ExecutionGeneration).Scan(
				&snapshot.SnapshotRunID, &snapshot.RunGeneration, &snapshot.State, &snapshot.ItemCount,
				&snapshot.StartedAt, &snapshot.UpdatedAt, &completed, &failure,
			)
			if err == nil {
				snapshot.CompletedAt = nullStringPtr(completed)
				snapshot.FailureCode = nullStringPtr(failure)
				result.SnapshotRun = &snapshot
			} else if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}
		return nil
	})
	return result, err
}

func ensureAdminLane(lanes map[string]*adminLaneWire, artifactID string) *adminLaneWire {
	if lane := lanes[artifactID]; lane != nil {
		return lane
	}
	lane := &adminLaneWire{ArtifactID: artifactID, Demands: []adminGroupCountWire{}, Jobs: []adminGroupCountWire{}}
	lanes[artifactID] = lane
	return lane
}

func nullStringPtr(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	return stringPtr(value.String)
}

func stringPtr(value string) *string {
	copy := value
	return &copy
}

func int64Ptr(value int64) *int64 {
	copy := value
	return &copy
}

func (router *Router) adminStaticHandler(w http.ResponseWriter, request *http.Request) {
	root, err := filepath.Abs(router.cfg.AdminDistDir)
	if err != nil {
		http.Error(w, "admin UI build is unavailable", http.StatusServiceUnavailable)
		return
	}
	relative := strings.TrimPrefix(request.URL.Path, "/admin/")
	clean := strings.TrimPrefix(urlpath.Clean("/"+relative), "/")
	target := filepath.Join(root, filepath.FromSlash(clean))
	if !withinAdminRoot(root, target) {
		http.NotFound(w, request)
		return
	}
	info, statErr := os.Stat(target)
	if statErr == nil && info.IsDir() {
		target = filepath.Join(target, "index.html")
		info, statErr = os.Stat(target)
	}
	if errors.Is(statErr, os.ErrNotExist) && filepath.Ext(clean) == "" {
		target = filepath.Join(root, "index.html")
		info, statErr = os.Stat(target)
	}
	if statErr != nil {
		if errors.Is(statErr, os.ErrNotExist) && clean != "" && filepath.Ext(clean) != "" {
			http.NotFound(w, request)
			return
		}
		http.Error(w, "admin UI is not built; run npm run build in web", http.StatusServiceUnavailable)
		return
	}
	if info.IsDir() {
		http.NotFound(w, request)
		return
	}
	http.ServeFile(w, request, target)
}

func withinAdminRoot(root, target string) bool {
	relative, err := filepath.Rel(root, target)
	if err != nil {
		return false
	}
	return relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
