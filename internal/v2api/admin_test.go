package v2api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"venera-server/internal/v2store"
)

type adminQueueSeed struct {
	artifactID string
	accountID  string
	jobID      string
}

func TestAdminOverviewIsUnauthenticatedConsistentAndAllowlisted(t *testing.T) {
	fixture := newAPIFixture(t)
	seed := seedAdminQueue(t, fixture, 12)

	response := apiCall(t, fixture.router, http.MethodGet, "/admin/api/overview", nil, "", "", "")
	if response.Code != http.StatusOK {
		t.Fatalf("overview = %d %s", response.Code, response.Body.String())
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("overview cache control = %q", response.Header().Get("Cache-Control"))
	}
	data := decodeAPI(t, response)["data"].(map[string]any)
	if data["observedAt"] != fixture.now.Format(time.RFC3339Nano) {
		t.Fatalf("observedAt = %#v", data["observedAt"])
	}
	assertJSONCounts(t, data["demandCounts"], map[string]float64{"active": 3, "blocked": 1, "inactive": 2})
	assertJSONCounts(t, data["currentJobCounts"], map[string]float64{"ready": 1, "leased": 1, "retryable": 1})
	assertJSONCounts(t, data["recentOutcomeCounts"], map[string]float64{"succeeded": 1, "failed": 1, "cancelled": 1})
	if data["pausedLaneCount"] != float64(1) {
		t.Fatalf("paused lane count = %#v", data["pausedLaneCount"])
	}
	lanes := data["lanes"].([]any)
	if len(lanes) != 1 || lanes[0].(map[string]any)["artifactId"] != seed.artifactID || lanes[0].(map[string]any)["runtimeState"] != "paused" {
		t.Fatalf("lanes = %#v", lanes)
	}
	jobs := data["recentJobs"].([]any)
	if len(jobs) != 6 || jobs[0].(map[string]any)["jobId"] != "admin-job-cancelled" {
		t.Fatalf("recent jobs = %#v", jobs)
	}
	if data["recentJobLimit"] != float64(100) || data["recentJobsTruncated"] != false {
		t.Fatalf("recent range = limit %#v truncated %#v", data["recentJobLimit"], data["recentJobsTruncated"])
	}
	assertAdminResponseSafe(t, response.Body.String(), seed)
}

func TestAdminOverviewEmptyIsNotAnError(t *testing.T) {
	fixture := newAPIFixture(t)
	response := apiCall(t, fixture.router, http.MethodGet, "/admin/api/overview", nil, "", "", "")
	if response.Code != http.StatusOK {
		t.Fatalf("empty overview = %d %s", response.Code, response.Body.String())
	}
	data := decodeAPI(t, response)["data"].(map[string]any)
	if len(data["lanes"].([]any)) != 0 || len(data["recentJobs"].([]any)) != 0 {
		t.Fatalf("empty overview collections = %#v", data)
	}
}

func TestAdminJobDetailLimitsAttemptsAssociatesSnapshotAndRedacts(t *testing.T) {
	fixture := newAPIFixture(t)
	seed := seedAdminQueue(t, fixture, 12)
	response := apiCall(t, fixture.router, http.MethodGet, "/admin/api/jobs/"+seed.jobID, nil, "", "", "")
	if response.Code != http.StatusOK {
		t.Fatalf("job detail = %d %s", response.Code, response.Body.String())
	}
	data := decodeAPI(t, response)["data"].(map[string]any)
	if data["job"].(map[string]any)["jobId"] != seed.jobID {
		t.Fatalf("job detail job = %#v", data["job"])
	}
	attempts := data["attempts"].(map[string]any)
	items := attempts["items"].([]any)
	if attempts["total"] != float64(12) || len(items) != 10 || items[0].(map[string]any)["attemptNo"] != float64(12) || items[9].(map[string]any)["attemptNo"] != float64(3) {
		t.Fatalf("attempt window = %#v", attempts)
	}
	snapshot := data["snapshotRun"].(map[string]any)
	if snapshot["state"] != "published" || snapshot["itemCount"] != float64(42) {
		t.Fatalf("snapshot run = %#v", snapshot)
	}
	assertAdminResponseSafe(t, response.Body.String(), seed)

	missing := apiCall(t, fixture.router, http.MethodGet, "/admin/api/jobs/missing", nil, "", "", "")
	if missing.Code != http.StatusNotFound || !strings.Contains(missing.Body.String(), `"code":"job_not_found"`) {
		t.Fatalf("missing job = %d %s", missing.Code, missing.Body.String())
	}
}

func TestAdminStaticDistributionAndAPIFallthrough(t *testing.T) {
	fixture := newAPIFixture(t)
	dist := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dist, "assets"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dist, "index.html"), []byte("<main>queue-dashboard</main>"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dist, "assets", "app.js"), []byte("dashboardAsset"), 0o600); err != nil {
		t.Fatal(err)
	}
	fixture.router.cfg.AdminDistDir = dist

	redirect := apiCall(t, fixture.router, http.MethodGet, "/admin", nil, "", "", "")
	if redirect.Code != http.StatusPermanentRedirect || redirect.Header().Get("Location") != "/admin/" {
		t.Fatalf("admin redirect = %d headers=%v", redirect.Code, redirect.Header())
	}
	for _, path := range []string{"/admin/", "/admin/jobs/example"} {
		response := apiCall(t, fixture.router, http.MethodGet, path, nil, "", "", "")
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "queue-dashboard") {
			t.Fatalf("admin SPA %s = %d %s", path, response.Code, response.Body.String())
		}
	}
	asset := apiCall(t, fixture.router, http.MethodGet, "/admin/assets/app.js", nil, "", "", "")
	if asset.Code != http.StatusOK || !strings.Contains(asset.Body.String(), "dashboardAsset") {
		t.Fatalf("admin asset = %d %s", asset.Code, asset.Body.String())
	}
	missingAsset := apiCall(t, fixture.router, http.MethodGet, "/admin/assets/missing.js", nil, "", "", "")
	if missingAsset.Code != http.StatusNotFound {
		t.Fatalf("missing admin asset = %d %s", missingAsset.Code, missingAsset.Body.String())
	}
	missingAPI := apiCall(t, fixture.router, http.MethodGet, "/admin/api/unknown", nil, "", "", "")
	if missingAPI.Code != http.StatusNotFound || strings.Contains(missingAPI.Body.String(), "queue-dashboard") {
		t.Fatalf("missing admin API fell through = %d %s", missingAPI.Code, missingAPI.Body.String())
	}

	fixture.router.cfg.AdminDistDir = filepath.Join(t.TempDir(), "not-built")
	notBuilt := apiCall(t, fixture.router, http.MethodGet, "/admin/", nil, "", "", "")
	if notBuilt.Code != http.StatusServiceUnavailable || !strings.Contains(notBuilt.Body.String(), "npm run build") {
		t.Fatalf("missing admin build = %d %s", notBuilt.Code, notBuilt.Body.String())
	}
}

func TestAdminOverviewScaleAndTruncation(t *testing.T) {
	fixture := newAPIFixture(t)
	pkg := seedStableManwa(t, fixture)
	nowText := fixture.now.Format(time.RFC3339Nano)
	accountID := "admin-scale-account"
	if err := fixture.db.WriteTx(context.Background(), func(tx *v2store.Tx) error {
		if _, err := tx.ExecContext(context.Background(), `
			INSERT INTO source_accounts(
				source_account_id, artifact_id, state, identity_scheme, identity_digest,
				identity_display, visibility_scope, session_epoch, session_revision,
				identity_verified_at, scope_fresh_until, revision, created_at, updated_at
			) VALUES(?, ?, 'active', 'test', 'scale-digest', '', 'scale', 1, 1, ?, ?, 1, ?, ?)`,
			accountID, pkg.ArtifactID, nowText, fixture.now.Add(time.Hour).Format(time.RFC3339Nano), nowText, nowText); err != nil {
			return err
		}
		if _, err := tx.ExecContext(context.Background(), `
			INSERT INTO source_runtime_state(
				artifact_id, state, target_concurrency, effective_concurrency,
				failure_streak, revision, updated_at
			) VALUES(?, 'healthy', 2, 1, 0, 1, ?)`, pkg.ArtifactID, nowText); err != nil {
			return err
		}
		for index := 1; index < 100; index++ {
			artifactID := fmt.Sprintf("admin-lane-%03d", index)
			if _, err := tx.ExecContext(context.Background(), `
				INSERT INTO source_artifacts(
					artifact_id, source_key, catalog_id, managed_state, revision, created_at, updated_at
				) VALUES(?, ?, 'admin-scale', 'active', 1, ?, ?)`, artifactID, artifactID, nowText, nowText); err != nil {
				return err
			}
			if _, err := tx.ExecContext(context.Background(), `
				INSERT INTO source_runtime_state(
					artifact_id, state, target_concurrency, effective_concurrency,
					failure_streak, revision, updated_at
				) VALUES(?, 'healthy', 1, 0, 0, 1, ?)`, artifactID, nowText); err != nil {
				return err
			}
		}
		for index := 0; index < 101; index++ {
			demandID := fmt.Sprintf("admin-scale-demand-%03d", index)
			jobID := fmt.Sprintf("admin-scale-job-%03d", index)
			activity := fixture.now.Add(-time.Duration(index) * time.Second).Format(time.RFC3339Nano)
			if _, err := tx.ExecContext(context.Background(), `
				INSERT INTO scan_demands(
					demand_id, demand_key, artifact_id, demand_kind, source_account_id,
					state, due_at, oldest_at, next_eligible_at, execution_generation,
					priority_class, revision, created_at, updated_at
				) VALUES(?, ?, ?, 'accountSnapshot', ?, 'inactive', ?, ?, ?, 1, 'normal', 1, ?, ?)`,
				demandID, demandID, pkg.ArtifactID, accountID, activity, activity, activity, activity, activity); err != nil {
				return err
			}
			if _, err := tx.ExecContext(context.Background(), `
				INSERT INTO scan_jobs(
					job_id, demand_id, artifact_id, job_kind, execution_generation,
					executor_source_account_id, package_release_id, fixed_session_epoch,
					state, attempt_count, next_eligible_at, payload_json, created_at, updated_at
				) VALUES(?, ?, ?, 'accountSnapshotSlice', 1, ?, ?, 1, 'failed', 0, ?, '{}', ?, ?)`,
				jobID, demandID, pkg.ArtifactID, accountID, pkg.PackageReleaseID, activity, activity, activity); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed scaled admin queue: %v", err)
	}

	durations := make([]time.Duration, 0, 20)
	var last *httptest.ResponseRecorder
	for index := 0; index < 20; index++ {
		started := time.Now()
		last = apiCall(t, fixture.router, http.MethodGet, "/admin/api/overview", nil, "", "", "")
		durations = append(durations, time.Since(started))
		if last.Code != http.StatusOK {
			t.Fatalf("scaled overview = %d %s", last.Code, last.Body.String())
		}
	}
	sort.Slice(durations, func(left, right int) bool { return durations[left] < durations[right] })
	p95 := durations[18]
	if p95 >= 2*time.Second {
		t.Fatalf("scaled overview p95 = %s, want < 2s; samples=%v", p95, durations)
	}
	data := decodeAPI(t, last)["data"].(map[string]any)
	if len(data["lanes"].([]any)) != 100 || len(data["recentJobs"].([]any)) != 100 || data["recentJobsTruncated"] != true {
		t.Fatalf("scaled overview shape = lanes %d jobs %d truncated %#v", len(data["lanes"].([]any)), len(data["recentJobs"].([]any)), data["recentJobsTruncated"])
	}
	assertJSONCounts(t, data["recentOutcomeCounts"], map[string]float64{"failed": 100})
}

func seedAdminQueue(t *testing.T, fixture *apiFixture, attemptCount int) adminQueueSeed {
	t.Helper()
	pkg := seedStableManwa(t, fixture)
	seed := adminQueueSeed{artifactID: pkg.ArtifactID, accountID: "admin-private-account", jobID: "admin-job-succeeded"}
	states := []string{"ready", "leased", "retryable", "succeeded", "failed", "cancelled"}
	demandStates := []string{"active", "active", "active", "inactive", "blocked", "inactive"}
	now := fixture.now
	nowText := now.Format(time.RFC3339Nano)
	err := fixture.db.WriteTx(context.Background(), func(tx *v2store.Tx) error {
		if _, err := tx.ExecContext(context.Background(), `
			INSERT INTO source_accounts(
				source_account_id, artifact_id, state, identity_scheme, identity_digest,
				identity_display, attributes_json, visibility_scope, session_epoch,
				session_revision, identity_verified_at, scope_fresh_until, revision,
				created_at, updated_at
			) VALUES(?, ?, 'active', 'test', 'admin-private-digest', 'admin-private-display',
				'{"private":"attribute"}', 'private-scope', 1, 1, ?, ?, 1, ?, ?)`,
			seed.accountID, seed.artifactID, nowText, now.Add(time.Hour).Format(time.RFC3339Nano), nowText, nowText); err != nil {
			return err
		}
		if _, err := tx.ExecContext(context.Background(), `
			INSERT INTO source_runtime_state(
				artifact_id, state, target_concurrency, effective_concurrency,
				next_eligible_at, failure_streak, last_error_code, revision, updated_at
			) VALUES(?, 'paused', 3, 1, ?, 2, 'runtime_paused', 1, ?)`,
			seed.artifactID, now.Add(time.Minute).Format(time.RFC3339Nano), nowText); err != nil {
			return err
		}
		for index, state := range states {
			demandID := "admin-demand-" + state
			jobID := "admin-job-" + state
			generation := index + 1
			activity := now.Add(time.Duration(index-len(states)) * time.Minute)
			if _, err := tx.ExecContext(context.Background(), `
				INSERT INTO scan_demands(
					demand_id, demand_key, artifact_id, demand_kind, source_account_id,
					state, due_at, oldest_at, next_eligible_at, execution_generation,
					priority_class, last_error_code, revision, created_at, updated_at
				) VALUES(?, ?, ?, 'accountSnapshot', ?, ?, ?, ?, ?, ?, 'normal', ?, 1, ?, ?)`,
				demandID, "admin-key-"+state, seed.artifactID, seed.accountID, demandStates[index],
				activity.Format(time.RFC3339Nano), activity.Format(time.RFC3339Nano), activity.Format(time.RFC3339Nano), generation,
				"demand_"+state, activity.Format(time.RFC3339Nano), activity.Format(time.RFC3339Nano)); err != nil {
				return err
			}
			attempts := 0
			resultDigest := any(nil)
			if jobID == seed.jobID {
				attempts = attemptCount
				resultDigest = "safe-result-digest"
			}
			leaseOwner := any(nil)
			leaseExpires := any(nil)
			if state == "leased" {
				leaseOwner = "admin-private-worker"
				leaseExpires = now.Add(time.Minute).Format(time.RFC3339Nano)
			}
			if _, err := tx.ExecContext(context.Background(), `
				INSERT INTO scan_jobs(
					job_id, demand_id, artifact_id, job_kind, execution_generation,
					executor_source_account_id, package_release_id, fixed_session_epoch,
					state, lease_owner, lease_expires_at, attempt_count, next_eligible_at,
					payload_json, result_digest, created_at, updated_at
				) VALUES(?, ?, ?, 'accountSnapshotSlice', ?, ?, ?, 1, ?, ?, ?, ?, ?,
					'{"cookie":"admin-private-cookie","cursor":"admin-private-cursor"}', ?, ?, ?)`,
				jobID, demandID, seed.artifactID, generation, seed.accountID, pkg.PackageReleaseID,
				state, leaseOwner, leaseExpires, attempts, activity.Format(time.RFC3339Nano), resultDigest,
				activity.Add(-time.Minute).Format(time.RFC3339Nano), activity.Format(time.RFC3339Nano)); err != nil {
				return err
			}
		}
		for attemptNo := 1; attemptNo <= attemptCount; attemptNo++ {
			started := now.Add(time.Duration(-attemptCount+attemptNo-10) * time.Minute)
			outcome := "retryable"
			errorCode := fmt.Sprintf("attempt_%02d", attemptNo)
			if attemptNo == attemptCount {
				outcome = "succeeded"
				errorCode = ""
			}
			if _, err := tx.ExecContext(context.Background(), `
				INSERT INTO scan_attempts(
					job_id, attempt_no, executor_source_account_id, worker_id,
					started_at, finished_at, outcome, error_code, metrics_json
				) VALUES(?, ?, ?, 'admin-private-worker', ?, ?, ?, NULLIF(?, ''),
					'{"responseBody":"admin-private-body"}')`, seed.jobID, attemptNo, seed.accountID,
				started.Format(time.RFC3339Nano), started.Add(time.Second).Format(time.RFC3339Nano), outcome, errorCode); err != nil {
				return err
			}
		}
		_, err := tx.ExecContext(context.Background(), `
			INSERT INTO favorite_snapshot_runs(
				snapshot_run_id, source_account_id, package_release_id, session_epoch,
				run_generation, state, checkpoint_envelope, item_count, started_at,
				updated_at, completed_at
			) VALUES('admin-snapshot', ?, ?, 1, 4, 'published', x'736563726574', 42, ?, ?, ?)`,
			seed.accountID, pkg.PackageReleaseID, now.Add(-time.Hour).Format(time.RFC3339Nano), nowText, nowText)
		return err
	})
	if err != nil {
		t.Fatalf("seed admin queue: %v", err)
	}
	return seed
}

func assertJSONCounts(t *testing.T, raw any, expected map[string]float64) {
	t.Helper()
	counts := raw.(map[string]any)
	for key, want := range expected {
		if got := counts[key]; got != want {
			t.Errorf("count %s = %#v, want %v; all=%#v", key, got, want, counts)
		}
	}
}

func assertAdminResponseSafe(t *testing.T, body string, seed adminQueueSeed) {
	t.Helper()
	for _, forbidden := range []string{
		seed.accountID, "admin-private-digest", "admin-private-display", "admin-private-scope",
		"admin-private-cookie", "admin-private-cursor", "admin-private-worker", "admin-private-body",
		"payloadJson", "metricsJson", "checkpointEnvelope", "sourceAccountId", "leaseOwner", "workerId",
	} {
		if strings.Contains(body, forbidden) {
			t.Errorf("admin response contains forbidden value/key %q: %s", forbidden, body)
		}
	}
}
