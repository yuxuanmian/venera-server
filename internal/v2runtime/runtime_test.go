package v2runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"venera-server/internal/v2crypto"
	"venera-server/internal/v2manifest"
	"venera-server/internal/v2scan"
	"venera-server/internal/v2store"
	"venera-server/internal/v2worker"
)

type runtimeFixture struct {
	db            *v2store.DB
	repo          *v2store.Repository
	now           time.Time
	artifactID    string
	releaseID     string
	accountID     string
	clientID      string
	identityValue string
}

func newRuntimeFixture(t *testing.T) *runtimeFixture {
	t.Helper()
	fixture := &runtimeFixture{
		now:        time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC),
		artifactID: "runtime-artifact",
		releaseID:  "runtime-release",
		accountID:  "runtime-account",
	}
	root := bytes.Repeat([]byte{0x63}, v2crypto.RootSecretSize)
	keys, err := v2crypto.DeriveKeys(root)
	if err != nil {
		t.Fatal(err)
	}
	db, err := v2store.OpenWithOptions(filepath.Join(t.TempDir(), "runtime.db"), v2store.Options{Clock: func() time.Time { return fixture.now }})
	if err != nil {
		t.Fatal(err)
	}
	fixture.db = db
	fixture.repo = v2store.NewRepository(db, keys)
	t.Cleanup(func() { _ = db.Close() })

	if err := fixture.repo.UpsertSourceArtifact(context.Background(), v2store.SourceArtifactInput{ArtifactID: fixture.artifactID, SourceKey: "runtime-source", CatalogID: "runtime-catalog", ManagedState: "active"}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.repo.UpsertSourcePackageRelease(context.Background(), v2store.SourcePackageReleaseInput{
		PackageReleaseID: fixture.releaseID, ArtifactID: fixture.artifactID, CatalogID: "runtime-catalog", CatalogSequence: 1,
		CoreHash: "runtime-core", ScanningExtensionHash: "runtime-extension", ObservationContractID: "runtime-content-v1",
		AccountObservationContractID: "runtime-account-v1", AccountProbeContractID: "runtime-probe-v1", MarkerSchemesJSON: `["runtime-marker-v1"]`,
		SessionExportProfileID: "runtime-session-v1", PackageJSON: `{"scanning":{"operations":["scanFavoriteSnapshotSlice"]}}`, State: "active",
	}); err != nil {
		t.Fatal(err)
	}
	enrollment, err := fixture.repo.CreateEnrollmentCode(context.Background(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	clientToken, err := v2crypto.GenerateBearerToken()
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := fixture.repo.ClaimClientEnrollment(context.Background(), v2store.ClaimClientEnrollmentRequest{
		PendingClientID: "runtime-pending", TokenDigest: v2crypto.CredentialDigest(keys.CredentialHMAC, clientToken),
		EnrollmentCodeDigest: fixture.repo.EnrollmentCodeDigest(enrollment.Code), DisplayName: "runtime-test", Platform: "test", AppVersion: "test",
		IdempotencyKey: "runtime-enrollment",
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.clientID = string(claimed.Client.ID)
	fixture.identityValue, err = v2crypto.GenerateBearerToken()
	if err != nil {
		t.Fatal(err)
	}
	identityScheme := "runtime-identity-v1"
	identityCiphertext, err := v2crypto.SealSession(keys.SessionAEAD, []byte(fixture.identityValue), []byte("source-account-identity:"+fixture.accountID+":"+fixture.artifactID))
	if err != nil {
		t.Fatal(err)
	}
	sessionPlaintext := []byte(`{"session":true}`)
	sessionEnvelope, err := v2crypto.SealSession(keys.SessionAEAD, sessionPlaintext, []byte("source-account-session:"+fixture.accountID+":"+fixture.artifactID))
	if err != nil {
		t.Fatal(err)
	}
	identityDigest := v2crypto.IdentityDigest(keys.IdentityHMAC, fixture.artifactID, identityScheme, fixture.identityValue)
	sessionDigest := v2crypto.Digest(keys.CredentialHMAC, "session-candidate-v1\x00"+string(sessionPlaintext))
	now := fixture.now.Format(time.RFC3339Nano)
	expires := fixture.now.Add(24 * time.Hour).Format(time.RFC3339Nano)
	if err := fixture.db.WriteTx(context.Background(), func(tx *v2store.Tx) error {
		if _, err := tx.ExecContext(context.Background(), `INSERT INTO source_accounts(source_account_id, artifact_id, state, identity_scheme, identity_ciphertext, identity_digest, identity_display, attributes_json, visibility_scope, session_epoch, session_revision, identity_verified_at, scope_fresh_until, revision, created_at, updated_at) VALUES(?, ?, 'active', ?, ?, ?, '', '{}', 'runtime:scope', 1, 1, ?, ?, 1, ?, ?)`, fixture.accountID, fixture.artifactID, identityScheme, identityCiphertext, identityDigest, now, expires, now, now); err != nil {
			return err
		}
		if _, err := tx.ExecContext(context.Background(), `INSERT INTO source_account_sessions(source_account_id, export_profile_id, session_envelope, session_digest, session_epoch, session_revision, expires_at, validated_at, updated_at) VALUES(?, 'runtime-session-v1', ?, ?, 1, 1, ?, ?, ?)`, fixture.accountID, sessionEnvelope, sessionDigest, expires, now, now); err != nil {
			return err
		}
		if _, err := tx.ExecContext(context.Background(), `INSERT INTO source_scan_status(source_account_id, status, revision, updated_at) VALUES(?, 'idle', 1, ?)`, fixture.accountID, now); err != nil {
			return err
		}
		_, err := tx.ExecContext(context.Background(), `INSERT INTO client_cloud_claims(client_id, source_account_id, artifact_id, state, revision, activated_at, updated_at) VALUES(?, ?, ?, 'active', 1, ?, ?)`, fixture.clientID, fixture.accountID, fixture.artifactID, now, now)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return fixture
}

type scriptedRuntimeWorker struct {
	fixture   *runtimeFixture
	panicMode bool
	staleMode bool

	mu         sync.Mutex
	snapshotNo int
	active     int
	maxActive  int
	operations []string
}

func (w *scriptedRuntimeWorker) Run(ctx context.Context, request v2worker.RunRequest) (v2worker.RunResult, error) {
	select {
	case <-ctx.Done():
		return v2worker.RunResult{}, ctx.Err()
	default:
	}
	w.mu.Lock()
	w.operations = append(w.operations, request.Operation)
	w.active++
	if w.active > w.maxActive {
		w.maxActive = w.active
	}
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		w.active--
		w.mu.Unlock()
	}()
	if w.panicMode && request.Operation == v2worker.OperationScanFavoriteSnapshotSlice {
		panic("runtime test worker panic")
	}
	switch request.Operation {
	case v2worker.OperationProbeAccount:
		output, err := json.Marshal(map[string]any{
			"identity":        map[string]string{"scheme": "runtime-identity-v1", "value": w.fixture.identityValue},
			"display":         map[string]string{"name": "runtime", "secondary": ""},
			"attributes":      map[string]any{},
			"visibilityScope": "runtime:scope",
		})
		return v2worker.RunResult{Output: output}, err
	case v2worker.OperationScanFavoriteSnapshotSlice:
		w.mu.Lock()
		sequence := w.snapshotNo
		w.snapshotNo++
		w.mu.Unlock()
		if w.staleMode && sequence == 0 {
			if err := w.fixture.db.WriteTx(context.Background(), func(tx *v2store.Tx) error {
				if _, err := tx.ExecContext(context.Background(), `UPDATE source_accounts SET session_epoch = 2, session_revision = session_revision + 1, revision = revision + 1 WHERE source_account_id = ?`, w.fixture.accountID); err != nil {
					return err
				}
				_, err := tx.ExecContext(context.Background(), `UPDATE source_account_sessions SET session_epoch = 2, session_revision = session_revision + 1 WHERE source_account_id = ?`, w.fixture.accountID)
				return err
			}); err != nil {
				return v2worker.RunResult{}, err
			}
		}
		return v2worker.RunResult{Output: runtimeSnapshotSlice(w.fixture, sequence%2 == 1)}, nil
	default:
		return v2worker.RunResult{}, v2worker.ErrWorkerInvalid
	}
}

func runtimeSnapshotSlice(fixture *runtimeFixture, complete bool) json.RawMessage {
	if complete {
		return json.RawMessage(`{"expectedTotal":1,"items":[],"complete":true,"checkpoint":null}`)
	}
	return json.RawMessage(`{"expectedTotal":1,"items":[{"comicId":"runtime-comic","membership":{"origin":"remoteSnapshot","folderId":"0"},"summary":{"title":"Runtime comic","cover":null,"subtitle":null},"contentObservation":{"visibilityScope":"runtime:scope","markerEvidence":{"channel":"runtime","scheme":"runtime-marker-v1","value":"runtime-boundary"},"updateTime":null},"accountObservation":{"sourceUnreadByVariant":{"normal":true},"sourceUnread":true}}],"complete":false,"checkpoint":{"cursor":1}}`)
}

func runtimePackage(fixture *runtimeFixture) PackageRuntime {
	return PackageRuntime{
		ArtifactID: fixture.artifactID, PackageReleaseID: fixture.releaseID, CoreHash: "runtime-core", ScanningExtensionHash: "runtime-extension",
		ObservationContractID: "runtime-content-v1", AccountObservationContractID: "runtime-account-v1",
		MarkerSchemes: []string{"runtime-marker-v1"},
		AccountProbeContract: v2manifest.AccountProbeContract{
			ID: "runtime-probe-v1", Version: 1, IdentitySchemes: []string{"runtime-identity-v1"}, VisibilityScopePattern: `^runtime:scope$`,
		},
		SessionExportProfile: v2manifest.SessionExportProfile{ID: "runtime-session-v1", AllowedOrigins: []string{"https://runtime.invalid"}, MaxCookies: 4, MaxSerializedBytes: 4096},
		ScanPolicy:           v2manifest.ScanPolicy{Recommended: v2manifest.ScanPolicyRecommended{SnapshotSliceRequestBudget: 2, SnapshotSliceItemBudget: 1}},
	}
}

func waitForRuntime(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("runtime condition did not become true")
}

func startTestRuntime(t *testing.T, fixture *runtimeFixture, worker Worker) (*Runtime, context.CancelFunc, <-chan error) {
	t.Helper()
	runtime := newTestRuntime(t, fixture, worker, 20*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- runtime.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = runtime.Close(context.Background())
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("runtime stopped with error: %v", err)
			}
		default:
		}
	})
	return runtime, cancel, errCh
}

func newTestRuntime(t *testing.T, fixture *runtimeFixture, worker Worker, reconcileTick time.Duration) *Runtime {
	t.Helper()
	runtime, err := New(fixture.repo, Options{
		Clock: fixture.db.Now, WorkerProvider: func(artifactID string) (Worker, error) {
			if artifactID != fixture.artifactID {
				return nil, ErrWorkerUnavailable
			}
			return worker, nil
		}, Packages: []PackageRuntime{runtimePackage(fixture)}, WorkerCount: 1,
		LeaseDuration: time.Second, JobTick: 10 * time.Millisecond, ReconcileTick: reconcileTick,
	})
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func TestRuntimeStartExecutesDurableSnapshotSlicesAndAdvancesGeneration(t *testing.T) {
	fixture := newRuntimeFixture(t)
	worker := &scriptedRuntimeWorker{fixture: fixture}
	runtime, cancel, errCh := startTestRuntime(t, fixture, worker)
	demands := v2scan.NewDemandRepository(fixture.repo)
	waitForRuntime(t, func() bool {
		demand, err := demands.GetDemandByKey(context.Background(), v2scan.AccountSnapshotDemandKey(fixture.artifactID, fixture.accountID))
		if err != nil || demand.ExecutionGeneration != 2 {
			return false
		}
		job, err := demands.GetJob(context.Background(), mustJobID(t, fixture.db, demand.ID))
		return err == nil && job.State == v2scan.JobSucceeded
	})
	demand, err := demands.GetDemandByKey(context.Background(), v2scan.AccountSnapshotDemandKey(fixture.artifactID, fixture.accountID))
	if err != nil {
		t.Fatal(err)
	}
	if demand.ExecutionGeneration != 2 || !demand.NextEligibleAt.After(fixture.now) {
		t.Fatalf("demand after complete = %+v", demand)
	}
	jobID := mustJobID(t, fixture.db, demand.ID)
	job, err := demands.GetJob(context.Background(), jobID)
	if err != nil || job.State != v2scan.JobSucceeded {
		t.Fatalf("job after complete = %+v err=%v", job, err)
	}
	var runState string
	var itemCount int
	if err := fixture.db.SQL().QueryRow(`SELECT state, item_count FROM favorite_snapshot_runs WHERE source_account_id = ? AND run_generation = 1`, fixture.accountID).Scan(&runState, &itemCount); err != nil {
		t.Fatal(err)
	}
	if runState != "published" || itemCount != 1 {
		t.Fatalf("published run = state:%s items:%d", runState, itemCount)
	}
	var attempts, succeeded int
	if err := fixture.db.SQL().QueryRow(`SELECT COUNT(*), COALESCE(SUM(CASE WHEN outcome = 'succeeded' THEN 1 ELSE 0 END), 0) FROM scan_attempts WHERE job_id = ?`, jobID).Scan(&attempts, &succeeded); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || succeeded != 2 {
		t.Fatalf("yielded slice attempts = %d succeeded = %d", attempts, succeeded)
	}
	worker.mu.Lock()
	operations := append([]string(nil), worker.operations...)
	maxActive := worker.maxActive
	worker.mu.Unlock()
	if len(operations) != 3 || operations[0] != v2worker.OperationProbeAccount || maxActive > 1 {
		t.Fatalf("runtime worker operations=%v maxActive=%d", operations, maxActive)
	}
	if runtime.dispatcher.ActiveLeases() != 0 {
		t.Fatalf("active dispatcher leases after success = %d", runtime.dispatcher.ActiveLeases())
	}
	if err := runtime.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if current := mustJobID(t, fixture.db, demand.ID); current != jobID {
		t.Fatalf("job changed before next evaluation: %s -> %s", jobID, current)
	}
	cancel()
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimePanicReleasesDispatcherLease(t *testing.T) {
	fixture := newRuntimeFixture(t)
	worker := &scriptedRuntimeWorker{fixture: fixture, panicMode: true}
	runtime, cancel, errCh := startTestRuntime(t, fixture, worker)
	demands := v2scan.NewDemandRepository(fixture.repo)
	waitForRuntime(t, func() bool {
		demand, err := demands.GetDemandByKey(context.Background(), v2scan.AccountSnapshotDemandKey(fixture.artifactID, fixture.accountID))
		if err != nil {
			return false
		}
		jobID := mustJobID(t, fixture.db, demand.ID)
		job, err := demands.GetJob(context.Background(), jobID)
		return err == nil && job.State == v2scan.JobFailed && runtime.dispatcher.ActiveLeases() == 0
	})
	if runtime.dispatcher.ActiveLeases() != 0 {
		t.Fatalf("panic leaked active dispatcher lease = %d", runtime.dispatcher.ActiveLeases())
	}
	cancel()
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeDiscardsStaleEpochResultsWithoutPublishing(t *testing.T) {
	fixture := newRuntimeFixture(t)
	worker := &scriptedRuntimeWorker{fixture: fixture, staleMode: true}
	runtime := newTestRuntime(t, fixture, worker, time.Hour)
	ctx := context.Background()
	if err := runtime.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	if err := runtime.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	demands := v2scan.NewDemandRepository(fixture.repo)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var jobState string
		if err := fixture.db.SQL().QueryRow(`SELECT state FROM scan_jobs WHERE execution_generation = 1 AND artifact_id = ?`, fixture.artifactID).Scan(&jobState); err == nil && (jobState == string(v2scan.JobCancelled) || jobState == string(v2scan.JobFailed) || jobState == string(v2scan.JobRetryable)) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	demand, err := demands.GetDemandByKey(ctx, v2scan.AccountSnapshotDemandKey(fixture.artifactID, fixture.accountID))
	if err != nil {
		t.Fatal(err)
	}
	if demand.ExecutionGeneration != 2 {
		var jobState, attemptOutcome, attemptError string
		var epoch int64
		_ = fixture.db.SQL().QueryRow(`SELECT session_epoch FROM source_accounts WHERE source_account_id = ?`, fixture.accountID).Scan(&epoch)
		_ = fixture.db.SQL().QueryRow(`SELECT state FROM scan_jobs WHERE artifact_id = ? AND execution_generation = 1`, fixture.artifactID).Scan(&jobState)
		_ = fixture.db.SQL().QueryRow(`SELECT COALESCE(outcome, ''), COALESCE(error_code, '') FROM scan_attempts WHERE job_id = (SELECT job_id FROM scan_jobs WHERE artifact_id = ? AND execution_generation = 1) ORDER BY attempt_no DESC LIMIT 1`, fixture.artifactID).Scan(&attemptOutcome, &attemptError)
		worker.mu.Lock()
		operations := append([]string(nil), worker.operations...)
		worker.mu.Unlock()
		t.Fatalf("stale epoch demand generation = %d job=%s outcome=%s error=%s epoch=%d active=%d operations=%v", demand.ExecutionGeneration, jobState, attemptOutcome, attemptError, epoch, runtime.dispatcher.ActiveLeases(), operations)
	}
	if runtime.dispatcher.ActiveLeases() != 0 {
		t.Fatalf("stale epoch leaked active dispatcher lease = %d", runtime.dispatcher.ActiveLeases())
	}
	var runState, failureCode string
	if err := fixture.db.SQL().QueryRow(`SELECT state, COALESCE(failure_code, '') FROM favorite_snapshot_runs WHERE source_account_id = ? AND run_generation = 1`, fixture.accountID).Scan(&runState, &failureCode); err != nil {
		t.Fatal(err)
	}
	if runState != "abandoned" || failureCode != "stale" {
		t.Fatalf("stale epoch run = state:%s failure:%s", runState, failureCode)
	}
	var published int
	if err := fixture.db.SQL().QueryRow(`SELECT COUNT(*) FROM favorite_snapshot_runs WHERE source_account_id = ? AND state = 'published'`, fixture.accountID).Scan(&published); err != nil {
		t.Fatal(err)
	}
	if published != 0 {
		t.Fatalf("stale epoch published %d runs", published)
	}
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeErrorMapping(t *testing.T) {
	tooLong := int((7*24*time.Hour)/time.Second) + 1
	transientRetryAfter := 17
	cases := []struct {
		name string
		err  error
		want string
	}{
		{name: "expired session", err: v2store.ErrSourceSessionExpired, want: "auth_required"},
		{name: "invalid session", err: v2store.ErrSourceSessionInvalid, want: "auth_required"},
		{name: "snapshot changed", err: v2scan.ErrSnapshotChanged, want: "snapshot_changed"},
		{name: "snapshot incomplete", err: v2scan.ErrSnapshotIncomplete, want: "incomplete_snapshot"},
		{name: "duplicate snapshot item", err: v2scan.ErrSnapshotDuplicateItem, want: "incomplete_snapshot"},
		{name: "invalid snapshot slice", err: v2scan.ErrSnapshotInvalidSlice, want: "contract_drift"},
		{name: "snapshot item limit", err: v2scan.ErrSnapshotItemLimit, want: "contract_drift"},
		{name: "stale job", err: v2scan.ErrStaleJob, want: "stale"},
		{name: "deadline", err: v2worker.ErrWorkerDeadline, want: "transient"},
		{name: "structured rate limit", err: v2worker.StructuredError{Code: "rate_limited"}, want: "extension_failure"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := runtimeErrorCode(testCase.err); got != testCase.want {
				t.Fatalf("runtimeErrorCode(%v) = %q, want %q", testCase.err, got, testCase.want)
			}
		})
	}
	if got := runtimeErrorCode(v2worker.StructuredError{Code: "rate_limited"}); got != "extension_failure" {
		t.Fatalf("runtimeErrorCode does not unwrap worker-only errors: %q", got)
	}
	if got := allowedRuntimeErrorCode("rate_limited"); got != "rate_limited" {
		t.Fatalf("allowed rate_limited code = %q", got)
	}
	if got := allowedRuntimeErrorCode("unknown"); got != "extension_failure" {
		t.Fatalf("unknown code = %q", got)
	}
	if got := retryAfterFromError(v2worker.StructuredError{Code: "rate_limited", RetryAfterSeconds: &transientRetryAfter}); got != 17*time.Second {
		t.Fatalf("retry-after = %s, want 17s", got)
	}
	if got := retryAfterFromError(v2worker.StructuredError{Code: "rate_limited", RetryAfterSeconds: &tooLong}); got != 7*24*time.Hour {
		t.Fatalf("retry-after cap = %s, want 7d", got)
	}
	if got := retryAfterFromError(errors.New("no retry-after")); got != 0 {
		t.Fatalf("missing retry-after = %s, want 0", got)
	}
}

func mustJobID(t *testing.T, db *v2store.DB, demandID string) string {
	t.Helper()
	var jobID string
	if err := db.SQL().QueryRow(`SELECT job_id FROM scan_jobs WHERE demand_id = ? ORDER BY job_id LIMIT 1`, demandID).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	return jobID
}
