package v2runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sync"
	"time"

	"venera-server/internal/v2crypto"
	"venera-server/internal/v2manifest"
	"venera-server/internal/v2scan"
	"venera-server/internal/v2store"
	"venera-server/internal/v2sync"
	"venera-server/internal/v2worker"
)

var (
	ErrRuntimeInvalid      = errors.New("v2 runtime is invalid")
	ErrRuntimeClosed       = errors.New("v2 runtime is closed")
	ErrWorkerUnavailable   = errors.New("scanning worker is unavailable for artifact")
	ErrRuntimeScopeChanged = errors.New("source account scope changed")
	ErrRuntimeCapability   = errors.New("scanning capability is unavailable")
)

// Worker is the small host boundary used by Runtime.  The concrete QuickJS
// worker is deliberately hidden behind it so runtime tests can use a bounded
// fake without moving job state out of SQLite.
type Worker interface {
	Run(context.Context, v2worker.RunRequest) (v2worker.RunResult, error)
}

type WorkerProviderFunc func(string) (Worker, error)

func (f WorkerProviderFunc) WorkerForArtifact(artifactID string) (Worker, error) {
	if f == nil {
		return nil, ErrWorkerUnavailable
	}
	return f(artifactID)
}

type PackageRuntime struct {
	ArtifactID                   string
	PackageReleaseID             string
	CoreHash                     string
	ScanningExtensionHash        string
	ObservationContractID        string
	AccountObservationContractID string
	MarkerSchemes                []string
	AccountProbeContract         v2manifest.AccountProbeContract
	SessionExportProfile         v2manifest.SessionExportProfile
	ScanPolicy                   v2manifest.ScanPolicy
	DetailEnabled                bool
}

type Options struct {
	Clock                  func() time.Time
	WorkerProvider         WorkerProviderFunc
	Packages               []PackageRuntime
	Planner                *v2scan.Planner
	DemandRepository       *v2scan.DemandRepository
	Dispatcher             *v2scan.Dispatcher
	SnapshotExecutor       *v2scan.SnapshotExecutor
	SnapshotPublisher      *v2sync.Publisher
	DetailPublisher        *v2sync.DetailPublisher
	PreparationCoordinator *v2sync.PreparationCoordinator
	DetailCapabilities     []v2scan.DetailCapability
	WorkerCount            int
	WorkerOperationTimeout time.Duration
	LeaseDuration          time.Duration
	WorkerID               string
	JobTick                time.Duration
	ReconcileTick          time.Duration
}

type Runtime struct {
	repo           *v2store.Repository
	clock          func() time.Time
	workerProvider WorkerProviderFunc
	packages       map[string]PackageRuntime

	planner       *v2scan.Planner
	demands       *v2scan.DemandRepository
	dispatcher    *v2scan.Dispatcher
	snapshots     *v2scan.SnapshotExecutor
	publisher     *v2sync.Publisher
	detail        *v2sync.DetailPublisher
	preparation   *v2sync.PreparationCoordinator
	detailCaps    []v2scan.DetailCapability
	workerTimeout time.Duration
	workerCount   int
	jobTick       time.Duration
	reconcileTick time.Duration

	wake chan struct{}

	mu          sync.Mutex
	started     bool
	initialized bool
	closed      bool
	cancel      context.CancelFunc
	done        chan struct{}
	leaseWG     sync.WaitGroup
	reconcileMu sync.Mutex
}

func New(repo *v2store.Repository, options Options) (*Runtime, error) {
	if repo == nil || repo.DB() == nil {
		return nil, ErrRuntimeInvalid
	}
	clock := options.Clock
	if clock == nil {
		clock = repo.DB().Now
	}
	workerCount := options.WorkerCount
	if workerCount < 1 {
		workerCount = 1
	}
	workerTimeout := options.WorkerOperationTimeout
	if workerTimeout <= 0 {
		workerTimeout = 30 * time.Second
	}
	jobTick := options.JobTick
	if jobTick <= 0 {
		jobTick = time.Second
	}
	reconcileTick := options.ReconcileTick
	if reconcileTick <= 0 {
		reconcileTick = 30 * time.Second
	}
	packages := make(map[string]PackageRuntime, len(options.Packages))
	for _, pkg := range options.Packages {
		if pkg.ArtifactID == "" || pkg.PackageReleaseID == "" {
			continue
		}
		packages[pkg.ArtifactID] = pkg
	}
	demands := options.DemandRepository
	if demands == nil {
		demands = v2scan.NewDemandRepository(repo)
	}
	planner := options.Planner
	if planner == nil {
		planner = v2scan.NewPlanner(repo, v2scan.PlannerOptions{Clock: clock, DetailCapabilities: options.DetailCapabilities})
	}
	dispatcher := options.Dispatcher
	if dispatcher == nil {
		dispatcher = v2scan.NewDispatcher(demands, v2scan.DispatcherOptions{Clock: clock, MaxConcurrent: workerCount, LeaseDuration: options.LeaseDuration, WorkerID: options.WorkerID})
	}
	for artifactID := range packages {
		dispatcher.AddArtifact(artifactID)
	}
	snapshots := options.SnapshotExecutor
	if snapshots == nil {
		snapshots = v2scan.NewSnapshotExecutor(repo, 0, 0)
	}
	publisher := options.SnapshotPublisher
	if publisher == nil {
		publisher = v2sync.NewPublisher(repo)
	}
	detail := options.DetailPublisher
	if detail == nil {
		detail = v2sync.NewDetailPublisher(repo)
	}
	preparation := options.PreparationCoordinator
	if preparation == nil {
		preparation = v2sync.NewPreparationCoordinator(repo)
	}
	return &Runtime{
		repo: repo, clock: clock, workerProvider: options.WorkerProvider, packages: packages,
		planner: planner, demands: demands, dispatcher: dispatcher, snapshots: snapshots,
		publisher: publisher, detail: detail, preparation: preparation, detailCaps: append([]v2scan.DetailCapability(nil), options.DetailCapabilities...),
		workerTimeout: workerTimeout, workerCount: workerCount, jobTick: jobTick, reconcileTick: reconcileTick,
		wake: make(chan struct{}, 1),
	}, nil
}

func NewRuntime(repo *v2store.Repository, options Options) (*Runtime, error) {
	return New(repo, options)
}

func (r *Runtime) Initialize(ctx context.Context) error {
	if r == nil || r.repo == nil {
		return ErrRuntimeInvalid
	}
	r.reconcileMu.Lock()
	defer r.reconcileMu.Unlock()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRuntimeClosed
	}
	if r.initialized {
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()
	if _, err := r.demands.RecoverExpiredLeases(ctx); err != nil {
		return err
	}
	if _, err := r.planner.Plan(ctx); err != nil {
		return err
	}
	// A restart can find a fully published source and an outstanding
	// preparation even when no HTTP request follows it.  Readiness is a
	// durable check and is safe to repeat.
	if _, err := r.preparation.EvaluateReadiness(ctx); err != nil && !errors.Is(err, v2sync.ErrPreparationNotReady) {
		return err
	}
	r.mu.Lock()
	r.initialized = true
	r.mu.Unlock()
	return nil
}

// Wake coalesces HTTP, publication, and refresh signals.  It never carries a
// job or an in-memory queue entry.
func (r *Runtime) Wake() {
	if r == nil {
		return
	}
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *Runtime) Start(ctx context.Context) error {
	if r == nil {
		return ErrRuntimeInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRuntimeClosed
	}
	if r.started {
		r.mu.Unlock()
		return ErrRuntimeInvalid
	}
	r.started = true
	r.done = make(chan struct{})
	runtimeContext, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	initialized := r.initialized
	r.mu.Unlock()
	if !initialized {
		if err := r.Initialize(runtimeContext); err != nil {
			cancel()
			r.mu.Lock()
			r.started = false
			close(r.done)
			r.mu.Unlock()
			return err
		}
	}
	if err := r.ReconcileOnce(runtimeContext); err != nil && runtimeContext.Err() == nil {
		cancel()
		r.mu.Lock()
		r.started = false
		if r.done != nil {
			close(r.done)
		}
		r.cancel = nil
		r.mu.Unlock()
		return err
	}
	defer func() {
		cancel()
		r.leaseWG.Wait()
		r.mu.Lock()
		if r.done != nil {
			close(r.done)
		}
		r.cancel = nil
		r.mu.Unlock()
	}()

	jobTicker := time.NewTicker(r.jobTick)
	defer jobTicker.Stop()
	reconcileTicker := time.NewTicker(r.reconcileTick)
	defer reconcileTicker.Stop()
	for {
		select {
		case <-runtimeContext.Done():
			return nil
		case <-r.wake:
			_ = r.ReconcileOnce(runtimeContext)
		case <-jobTicker.C:
			_ = r.dispatchAvailable(runtimeContext)
		case <-reconcileTicker.C:
			_ = r.ReconcileOnce(runtimeContext)
		}
	}
}

func (r *Runtime) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	r.closed = true
	cancel := r.cancel
	done := r.done
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	finished := make(chan struct{})
	go func() {
		r.leaseWG.Wait()
		close(finished)
	}()
	select {
	case <-finished:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Runtime) ReconcileOnce(ctx context.Context) error {
	if r == nil || r.repo == nil {
		return ErrRuntimeInvalid
	}
	r.reconcileMu.Lock()
	defer r.reconcileMu.Unlock()
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return ErrRuntimeClosed
	}
	if _, err := r.demands.RecoverExpiredLeases(ctx); err != nil {
		return err
	}
	if _, err := r.planner.Plan(ctx); err != nil {
		return err
	}
	if _, err := r.preparation.EvaluateReadiness(ctx); err != nil && !errors.Is(err, v2sync.ErrPreparationNotReady) {
		return err
	}
	if err := r.materializeDue(ctx); err != nil {
		return err
	}
	return r.dispatchAvailable(ctx)
}

func (r *Runtime) materializeDue(ctx context.Context) error {
	demands, err := r.demands.ListDemands(ctx, "")
	if err != nil {
		return err
	}
	now := r.clock().UTC()
	for _, demand := range demands {
		if demand.State != v2scan.DemandActive || demand.ExecutionGeneration < 1 || demand.NextEligibleAt.After(now) {
			continue
		}
		kind := v2scan.JobMaintenance
		payload := json.RawMessage(`{}`)
		switch demand.Kind {
		case v2scan.DemandAccountSnapshot:
			kind = v2scan.JobAccountSnapshotSlice
		case v2scan.DemandComicDetail:
			kind = v2scan.JobComicDetailBatch
			payload, err = json.Marshal(map[string]string{
				"comicId": demand.ComicID, "visibilityScope": demand.VisibilityScope,
				"variantKey": demand.VariantKey, "observationContractId": demand.ObservationContractID,
			})
			if err != nil {
				return err
			}
		case v2scan.DemandMaintenance:
		default:
			continue
		}
		if _, err := r.demands.MaterializeJob(ctx, v2scan.MaterializeJobRequest{DemandID: demand.ID, Kind: kind, PayloadJSON: string(payload)}); err != nil {
			if errors.Is(err, v2scan.ErrDemandNotRunnable) || errors.Is(err, v2scan.ErrNoExecutorAccount) {
				continue
			}
			return err
		}
	}
	return nil
}

func (r *Runtime) dispatchAvailable(ctx context.Context) error {
	for {
		lease, err := r.dispatcher.Next(ctx)
		if errors.Is(err, v2scan.ErrNoReadyJob) || errors.Is(err, v2scan.ErrDispatcherBusy) {
			return nil
		}
		if err != nil {
			return err
		}
		r.leaseWG.Add(1)
		go func(lease v2scan.JobLease) {
			defer r.leaseWG.Done()
			r.executeLease(ctx, lease)
		}(lease)
	}
}

type runtimeFailure struct {
	Code       string
	RetryAfter time.Duration
	RunID      string
	AccountID  string
	Scope      *v2scan.ProbeResult
}

func (r *Runtime) executeLease(ctx context.Context, lease v2scan.JobLease) {
	finished := false
	finish := func(fn func() error) error {
		if finished {
			return v2scan.ErrLeaseLost
		}
		// The operation owns the dispatcher slot from this point.  Even if the
		// database returns an error, a later lease recovery handles the durable
		// row and no active counter leaks.
		finished = true
		return fn()
	}
	finishFailure := func(failure runtimeFailure) {
		if failure.Code == "" {
			failure.Code = "extension_failure"
		}
		if failure.AccountID == "" {
			failure.AccountID = lease.Job.ExecutorSourceAccountID
		}
		if failure.Scope != nil {
			_ = r.updateAccountScope(ctx, failure.AccountID, failure.Scope)
		}
		if failure.RunID != "" && (failure.Code == "snapshot_changed" || failure.Code == "incomplete_snapshot" || failure.Code == "stale" || failure.Code == "auth_required" || failure.Code == "contract_drift" || failure.Code == "extension_failure") {
			_ = r.snapshots.AbandonRun(ctx, failure.RunID, failure.Code)
		}
		if failure.Code == "snapshot_changed" || failure.Code == "incomplete_snapshot" || failure.Code == "stale" || failure.Scope != nil {
			_ = finish(func() error {
				return r.dispatcher.DiscardAndAdvance(ctx, v2scan.CompleteJobRequest{JobID: lease.Job.ID, WorkerID: lease.Job.LeaseOwner, ErrorCode: failure.Code, ExpectedEpoch: lease.Job.FixedSessionEpoch})
			})
			return
		}
		blocked := failure.Code == "auth_required" || failure.Code == "contract_drift" || failure.Code == "extension_failure" || failure.Code == "forbidden" || failure.Code == "not_found"
		if blocked {
			status := "blocked"
			if failure.Code == "auth_required" {
				status = "reauthRequired"
			}
			_ = r.setSourceStatus(ctx, failure.AccountID, status, failure.Code)
		}
		outcome := "retryable"
		if blocked {
			outcome = "failed"
		}
		if failure.Code == "stale" || failure.Code == "lease_lost" {
			outcome = "discarded"
		}
		delay := failure.RetryAfter
		if delay <= 0 {
			delay = v2scan.RetryDelay(lease.AttemptNo, time.Minute, 24*time.Hour)
		}
		_ = finish(func() error {
			return r.dispatcher.Complete(ctx, v2scan.CompleteJobRequest{JobID: lease.Job.ID, WorkerID: lease.Job.LeaseOwner, Outcome: outcome, ErrorCode: failure.Code, RetryAfter: delay, ExpectedEpoch: lease.Job.FixedSessionEpoch})
		})
		if blocked {
			_ = r.demands.MarkDemandState(ctx, lease.Job.DemandID, v2scan.DemandBlocked, failure.Code)
		}
	}
	defer func() {
		if recovered := recover(); recovered != nil && !finished {
			finishFailure(runtimeFailure{Code: "extension_failure", AccountID: lease.Job.ExecutorSourceAccountID})
		}
	}()

	var err error
	switch lease.Job.Kind {
	case v2scan.JobAccountSnapshotSlice:
		err = r.executeSnapshot(ctx, lease, finish)
	case v2scan.JobComicDetailBatch:
		err = r.executeDetail(ctx, lease, finish)
	default:
		err = fmt.Errorf("unsupported job kind %q", lease.Job.Kind)
	}
	if err == nil {
		return
	}
	var carrier failureCarrier
	if errors.As(err, &carrier) {
		finishFailure(carrier.RuntimeFailure())
		return
	}
	finishFailure(runtimeFailure{Code: runtimeErrorCode(err), RunID: snapshotRunID(err), AccountID: lease.Job.ExecutorSourceAccountID})
}

type failureCarrier interface {
	RuntimeFailure() runtimeFailure
}

type carriedFailure struct {
	runtimeFailure
	err error
}

func (e carriedFailure) Error() string                  { return e.Code }
func (e carriedFailure) Unwrap() error                  { return e.err }
func (e carriedFailure) RuntimeFailure() runtimeFailure { return e.runtimeFailure }

func withRuntimeFailure(f runtimeFailure, err error) error {
	return carriedFailure{runtimeFailure: f, err: err}
}

func runtimeErrorCode(err error) string {
	switch {
	case errors.Is(err, v2store.ErrSourceSessionExpired), errors.Is(err, v2store.ErrSourceSessionInvalid):
		return "auth_required"
	case errors.Is(err, v2scan.ErrSnapshotChanged), errors.Is(err, v2scan.ErrSnapshotIncomplete):
		if errors.Is(err, v2scan.ErrSnapshotIncomplete) {
			return "incomplete_snapshot"
		}
		return "snapshot_changed"
	case errors.Is(err, v2scan.ErrSnapshotDuplicateItem):
		return "incomplete_snapshot"
	case errors.Is(err, v2scan.ErrSnapshotInvalidSlice), errors.Is(err, v2scan.ErrSnapshotItemLimit):
		return "contract_drift"
	case errors.Is(err, v2scan.ErrSnapshotIdentityMismatch), errors.Is(err, v2sync.ErrPublishStale), errors.Is(err, v2sync.ErrDetailPublishStale), errors.Is(err, v2scan.ErrStaleJob):
		return "stale"
	case errors.Is(err, v2worker.ErrWorkerDeadline):
		return "transient"
	case errors.Is(err, ErrRuntimeCapability):
		return "extension_failure"
	default:
		return "extension_failure"
	}
}

func snapshotRunID(err error) string {
	var failure carriedFailure
	if errors.As(err, &failure) {
		return failure.RunID
	}
	return ""
}

type accountExecution struct {
	IdentityScheme  string
	IdentityValue   string
	IdentityDigest  string
	Attributes      map[string]any
	AttributesJSON  string
	VisibilityScope string
	Session         v2store.ActiveSourceSession
	Cookies         []http.Cookie
}

func (r *Runtime) loadAccountExecution(ctx context.Context, job v2scan.Job) (accountExecution, error) {
	active, err := r.repo.ReadActiveSourceSession(ctx, job.ExecutorSourceAccountID, job.ArtifactID, job.PackageReleaseID, job.FixedSessionEpoch)
	if err != nil {
		return accountExecution{}, err
	}
	var account accountExecution
	var identityCiphertext []byte
	err = r.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT identity_scheme, identity_ciphertext, identity_digest, attributes_json, visibility_scope FROM source_accounts WHERE source_account_id = ? AND artifact_id = ? AND state = 'active'`, job.ExecutorSourceAccountID, job.ArtifactID).Scan(&account.IdentityScheme, &identityCiphertext, &account.IdentityDigest, &account.AttributesJSON, &account.VisibilityScope)
	})
	if err != nil {
		return accountExecution{}, err
	}
	identity, err := v2crypto.OpenSession(r.repo.Keys().SessionAEAD, identityCiphertext, []byte("source-account-identity:"+job.ExecutorSourceAccountID+":"+job.ArtifactID))
	if err != nil || len(identity) == 0 || v2crypto.IdentityDigest(r.repo.Keys().IdentityHMAC, job.ArtifactID, account.IdentityScheme, string(identity)) != account.IdentityDigest {
		return accountExecution{}, v2store.ErrSourceSessionInvalid
	}
	account.IdentityValue = string(identity)
	if account.AttributesJSON == "" {
		account.AttributesJSON = "{}"
	}
	if err := json.Unmarshal([]byte(account.AttributesJSON), &account.Attributes); err != nil || account.Attributes == nil {
		return accountExecution{}, v2store.ErrSourceSessionInvalid
	}
	account.Session = active
	account.Cookies, err = cookiesFromSession(active.Session)
	if err != nil {
		return accountExecution{}, err
	}
	return account, nil
}

func cookiesFromSession(session map[string]any) ([]http.Cookie, error) {
	value, ok := session["cookies"]
	if !ok || value == nil {
		return nil, nil
	}
	entries, ok := value.([]any)
	if !ok {
		return nil, ErrRuntimeCapability
	}
	result := make([]http.Cookie, 0, len(entries))
	for _, entry := range entries {
		object, ok := entry.(map[string]any)
		if !ok {
			return nil, ErrRuntimeCapability
		}
		name, nameOK := object["name"].(string)
		cookieValue, valueOK := object["value"].(string)
		if !nameOK || !valueOK || name == "" {
			return nil, ErrRuntimeCapability
		}
		cookie := http.Cookie{Name: name, Value: cookieValue}
		if domain, ok := object["domain"].(string); ok {
			cookie.Domain = domain
		}
		if path, ok := object["path"].(string); ok {
			cookie.Path = path
		}
		if secure, ok := object["secure"].(bool); ok {
			cookie.Secure = secure
		}
		if httpOnly, ok := object["httpOnly"].(bool); ok {
			cookie.HttpOnly = httpOnly
		}
		result = append(result, cookie)
	}
	return result, nil
}

func (r *Runtime) packageFor(artifactID string) (PackageRuntime, error) {
	pkg, ok := r.packages[artifactID]
	if !ok {
		return PackageRuntime{}, ErrWorkerUnavailable
	}
	return pkg, nil
}

func (r *Runtime) workerFor(artifactID string) (Worker, error) {
	if r.workerProvider == nil {
		return nil, ErrWorkerUnavailable
	}
	worker, err := r.workerProvider.WorkerForArtifact(artifactID)
	if err != nil || worker == nil {
		if err != nil {
			return nil, err
		}
		return nil, ErrWorkerUnavailable
	}
	return worker, nil
}

func (r *Runtime) executeSnapshot(ctx context.Context, lease v2scan.JobLease, finish func(func() error) error) error {
	job := lease.Job
	pkg, err := r.packageFor(job.ArtifactID)
	if err != nil {
		return withRuntimeFailure(runtimeFailure{Code: "extension_failure", AccountID: job.ExecutorSourceAccountID}, err)
	}
	account, err := r.loadAccountExecution(ctx, job)
	if err != nil {
		return withRuntimeFailure(runtimeFailure{Code: runtimeErrorCode(err), AccountID: job.ExecutorSourceAccountID}, err)
	}
	worker, err := r.workerFor(job.ArtifactID)
	if err != nil {
		return withRuntimeFailure(runtimeFailure{Code: "extension_failure", AccountID: job.ExecutorSourceAccountID}, err)
	}
	if err := r.setSourceStatus(ctx, job.ExecutorSourceAccountID, "scanning", ""); err != nil {
		return err
	}
	run, err := r.snapshots.StartRun(ctx, v2scan.SnapshotRunRequest{SourceAccountID: job.ExecutorSourceAccountID, PackageReleaseID: job.PackageReleaseID, SessionEpoch: job.FixedSessionEpoch, RunGeneration: job.ExecutionGeneration})
	if err != nil {
		return withRuntimeFailure(runtimeFailure{Code: runtimeErrorCode(err), AccountID: job.ExecutorSourceAccountID}, err)
	}
	checkpoint, err := r.snapshots.ResumeCheckpoint(ctx, run.ID)
	if err != nil && !errors.Is(err, v2scan.ErrSnapshotRunState) {
		return withRuntimeFailure(runtimeFailure{Code: runtimeErrorCode(err), RunID: run.ID, AccountID: job.ExecutorSourceAccountID}, err)
	}
	firstSlice := run.State == "running" && run.ItemCount == 0 && len(checkpoint) == 0
	if firstSlice {
		probe := v2scan.NewAccountProbeRunner(nil)
		probe.Worker = workerAsConcrete(worker)
		// The account probe runner accepts the concrete worker boundary. A fake
		// runtime worker is handled by the direct probe adapter below.
		var probeResult v2scan.ProbeResult
		if concrete := workerAsConcrete(worker); concrete != nil {
			probe.Worker = concrete
			probe.Clock = r.clock
			probeResult, err = probe.Probe(ctx, v2scan.ProbeRequest{RequestID: "runtime-probe-" + job.ID, ArtifactID: job.ArtifactID, PackageReleaseID: job.PackageReleaseID, Contract: pkg.AccountProbeContract, SessionCookies: account.Cookies, Reason: "snapshot_validation", RequestedAt: r.clock()})
		} else {
			probeResult, err = runProbeWithWorker(ctx, worker, pkg, account.Cookies, job.ID, r.clock)
		}
		if err != nil {
			return withRuntimeFailure(runtimeFailure{Code: probeErrorCode(err), AccountID: job.ExecutorSourceAccountID, RunID: run.ID}, err)
		}
		probeDigest := v2crypto.IdentityDigest(r.repo.Keys().IdentityHMAC, job.ArtifactID, probeResult.IdentityScheme, probeResult.IdentityValue)
		if probeResult.IdentityScheme != account.IdentityScheme || probeDigest != account.IdentityDigest {
			return withRuntimeFailure(runtimeFailure{Code: "auth_required", AccountID: job.ExecutorSourceAccountID, RunID: run.ID}, errors.New("probe identity changed"))
		}
		if probeResult.VisibilityScope != account.VisibilityScope || !sameJSONObject(account.AttributesJSON, probeResult.AttributesJSON) {
			return withRuntimeFailure(runtimeFailure{Code: "scope_changed", AccountID: job.ExecutorSourceAccountID, Scope: &probeResult, RunID: run.ID}, ErrRuntimeScopeChanged)
		}
		if len(probeResult.SessionCookies) > 0 {
			if err := r.persistSessionCookies(ctx, job, account.Session, probeResult.SessionCookies, pkg.SessionExportProfile); err != nil {
				return withRuntimeFailure(runtimeFailure{Code: "stale", RunID: run.ID, AccountID: job.ExecutorSourceAccountID}, err)
			}
			account.Cookies = probeResult.SessionCookies
		}
	}
	maxRequests := pkg.ScanPolicy.Recommended.SnapshotSliceRequestBudget
	if maxRequests < 1 {
		maxRequests = 1
	}
	maxItems := pkg.ScanPolicy.Recommended.SnapshotSliceItemBudget
	if maxItems < 1 {
		maxItems = 1
	}
	deadline := time.Now().Add(r.workerTimeout)
	workerResult, err := worker.Run(ctx, v2worker.RunRequest{RequestID: "snapshot-" + job.ID + "-" + fmt.Sprint(lease.AttemptNo), Operation: v2worker.OperationScanFavoriteSnapshotSlice, Input: map[string]any{
		"checkpoint": checkpoint,
		"account":    map[string]any{"identity": map[string]string{"scheme": account.IdentityScheme, "value": account.IdentityValue}, "attributes": account.Attributes, "visibilityScope": account.VisibilityScope},
		"budget":     map[string]any{"maxRequests": maxRequests, "maxItems": maxItems, "deadlineAt": deadline.UTC().Format(time.RFC3339Nano)},
	}, DeadlineAt: deadline, Budget: v2worker.OperationBudget{MaxRequests: maxRequests, MaxItems: maxItems, DeadlineAt: deadline}, ArtifactID: job.ArtifactID, PackageReleaseID: job.PackageReleaseID, CoreHash: pkg.CoreHash, ScanningExtensionHash: pkg.ScanningExtensionHash, SessionEpoch: job.FixedSessionEpoch, SessionRevision: account.Session.SessionRevision, AllowedOrigins: pkg.SessionExportProfile.AllowedOrigins, SessionCookies: account.Cookies})
	if err != nil {
		return withRuntimeFailure(runtimeFailure{Code: workerErrorCode(err), RunID: run.ID, AccountID: job.ExecutorSourceAccountID, RetryAfter: retryAfterFromError(err)}, err)
	}
	if err := r.ensureJobCurrent(ctx, job); err != nil {
		return withRuntimeFailure(runtimeFailure{Code: "stale", RunID: run.ID, AccountID: job.ExecutorSourceAccountID}, err)
	}
	slice, err := v2scan.DecodeSnapshotSlice(workerResult.Output)
	if err != nil {
		return withRuntimeFailure(runtimeFailure{Code: "contract_drift", RunID: run.ID, AccountID: job.ExecutorSourceAccountID}, err)
	}
	if err := validateSnapshotScope(slice, account.VisibilityScope, pkg.MarkerSchemes); err != nil {
		return withRuntimeFailure(runtimeFailure{Code: "contract_drift", RunID: run.ID, AccountID: job.ExecutorSourceAccountID}, err)
	}
	if len(workerResult.SessionCookies) > 0 {
		if err := r.persistSessionCookies(ctx, job, account.Session, workerResult.SessionCookies, pkg.SessionExportProfile); err != nil {
			return withRuntimeFailure(runtimeFailure{Code: "stale", RunID: run.ID, AccountID: job.ExecutorSourceAccountID}, err)
		}
	}
	if err := r.snapshots.ApplySlice(ctx, run.ID, slice); err != nil {
		return withRuntimeFailure(runtimeFailure{Code: runtimeErrorCode(err), RunID: run.ID, AccountID: job.ExecutorSourceAccountID}, err)
	}
	if !slice.Complete {
		err := finish(func() error {
			return r.dispatcher.Yield(ctx, v2scan.YieldJobRequest{JobID: job.ID, WorkerID: job.LeaseOwner, ResultDigest: v2crypto.Digest(r.repo.Keys().CredentialHMAC, string(workerResult.Output)), MetricsJSON: "{}", ExpectedEpoch: job.FixedSessionEpoch})
		})
		if err != nil && !errors.Is(err, v2scan.ErrStaleJob) && !errors.Is(err, v2scan.ErrLeaseLost) {
			return err
		}
		r.Wake()
		return nil
	}
	completed, err := r.snapshots.CompleteRun(ctx, run.ID, slice.FinalBoundaryDigest)
	if err != nil {
		return withRuntimeFailure(runtimeFailure{Code: runtimeErrorCode(err), RunID: run.ID, AccountID: job.ExecutorSourceAccountID}, err)
	}
	if _, err := r.publisher.PublishSnapshot(ctx, v2sync.PublishSnapshotRequest{RunID: completed.ID, SourceAccountID: job.ExecutorSourceAccountID, PackageReleaseID: job.PackageReleaseID, ExpectedGeneration: job.ExecutionGeneration, ExpectedSessionEpoch: job.FixedSessionEpoch, FreshUntil: r.clock().Add(3 * time.Hour)}); err != nil {
		return withRuntimeFailure(runtimeFailure{Code: runtimeErrorCode(err), RunID: run.ID, AccountID: job.ExecutorSourceAccountID}, err)
	}
	if _, err := r.preparation.EvaluateReadiness(ctx); err != nil && !errors.Is(err, v2sync.ErrPreparationNotReady) {
		return err
	}
	if err := finish(func() error {
		return r.dispatcher.Complete(ctx, v2scan.CompleteJobRequest{JobID: job.ID, WorkerID: job.LeaseOwner, Outcome: "succeeded", ResultDigest: v2crypto.Digest(r.repo.Keys().CredentialHMAC, string(workerResult.Output)), ExpectedEpoch: job.FixedSessionEpoch})
	}); err != nil && !errors.Is(err, v2scan.ErrStaleJob) && !errors.Is(err, v2scan.ErrLeaseLost) {
		return err
	}
	r.Wake()
	return nil
}

func (r *Runtime) executeDetail(ctx context.Context, lease v2scan.JobLease, finish func(func() error) error) error {
	job := lease.Job
	pkg, err := r.packageFor(job.ArtifactID)
	if err != nil {
		return withRuntimeFailure(runtimeFailure{Code: "extension_failure"}, err)
	}
	account, err := r.loadAccountExecution(ctx, job)
	if err != nil {
		return withRuntimeFailure(runtimeFailure{Code: runtimeErrorCode(err)}, err)
	}
	worker, err := r.workerFor(job.ArtifactID)
	if err != nil {
		return withRuntimeFailure(runtimeFailure{Code: "extension_failure"}, err)
	}
	var payload struct {
		ComicID               string `json:"comicId"`
		VisibilityScope       string `json:"visibilityScope"`
		VariantKey            string `json:"variantKey"`
		ObservationContractID string `json:"observationContractId"`
	}
	if err := json.Unmarshal([]byte(job.PayloadJSON), &payload); err != nil {
		return withRuntimeFailure(runtimeFailure{Code: "contract_drift"}, err)
	}
	demand, err := r.demands.GetDemand(ctx, job.DemandID)
	if err != nil {
		return err
	}
	if payload.ComicID == "" {
		payload.ComicID = demand.ComicID
	}
	if payload.VisibilityScope == "" {
		payload.VisibilityScope = demand.VisibilityScope
	}
	if payload.VariantKey == "" {
		payload.VariantKey = demand.VariantKey
	}
	if payload.ObservationContractID == "" {
		payload.ObservationContractID = demand.ObservationContractID
	}
	capabilities := r.detailCaps
	if len(capabilities) == 0 && pkg.DetailEnabled {
		capabilities = []v2scan.DetailCapability{{ArtifactID: job.ArtifactID, ObservationContractID: payload.ObservationContractID, Enabled: true}}
	}
	executor := v2scan.NewDetailExecutor(capabilities, 1)
	deadline := time.Now().Add(r.workerTimeout)
	batch, err := executor.ExecuteBatch(ctx, []v2scan.DetailRequest{{ArtifactID: job.ArtifactID, PackageReleaseID: job.PackageReleaseID, VisibilityScope: payload.VisibilityScope, VariantKey: payload.VariantKey, ContractID: payload.ObservationContractID, ComicID: payload.ComicID, Reason: "due", Deadline: deadline}}, v2scan.DetailScanFunc(func(scanContext context.Context, request v2scan.DetailRequest) (json.RawMessage, error) {
		workerResult, err := worker.Run(scanContext, v2worker.RunRequest{RequestID: "detail-" + job.ID, Operation: v2worker.OperationScanComic, Input: map[string]any{"comicId": request.ComicID, "account": map[string]any{"identity": map[string]string{"scheme": account.IdentityScheme, "value": account.IdentityValue}, "attributes": account.Attributes, "visibilityScope": account.VisibilityScope}, "reason": request.Reason, "budget": map[string]any{"maxRequests": 2, "deadlineAt": deadline.UTC().Format(time.RFC3339Nano)}}, DeadlineAt: deadline, Budget: v2worker.OperationBudget{MaxRequests: 2, MaxItems: 1, DeadlineAt: deadline}, ArtifactID: job.ArtifactID, PackageReleaseID: job.PackageReleaseID, CoreHash: pkg.CoreHash, ScanningExtensionHash: pkg.ScanningExtensionHash, SessionEpoch: job.FixedSessionEpoch, SessionRevision: account.Session.SessionRevision, AllowedOrigins: pkg.SessionExportProfile.AllowedOrigins, SessionCookies: account.Cookies})
		if err != nil {
			return nil, err
		}
		decoded, err := v2scan.DecodeDetailOutput(workerResult.Output, request.ComicID)
		if err != nil {
			return nil, err
		}
		if len(workerResult.SessionCookies) > 0 {
			if err := r.persistSessionCookies(scanContext, job, account.Session, workerResult.SessionCookies, pkg.SessionExportProfile); err != nil {
				return nil, err
			}
		}
		return decoded.Payload, nil
	}))
	if err != nil {
		return withRuntimeFailure(runtimeFailure{Code: "extension_failure"}, err)
	}
	if len(batch.Results) != 1 {
		return withRuntimeFailure(runtimeFailure{Code: "contract_drift"}, ErrRuntimeCapability)
	}
	detailResult := batch.Results[0]
	if detailResult.Error != "" {
		code := "transient"
		if !detailResult.Retryable {
			code = "contract_drift"
		}
		return withRuntimeFailure(runtimeFailure{Code: code}, errors.New(detailResult.Error))
	}
	if err := v2scan.ValidateDetailResult(detailResult); err != nil {
		return withRuntimeFailure(runtimeFailure{Code: "contract_drift"}, err)
	}
	if _, err := r.detail.Publish(ctx, v2sync.PublishDetailRequest{DemandID: job.DemandID, ArtifactID: job.ArtifactID, ComicID: payload.ComicID, VisibilityScope: payload.VisibilityScope, VariantKey: payload.VariantKey, ObservationContractID: payload.ObservationContractID, SourceAccountID: job.ExecutorSourceAccountID, PackageReleaseID: job.PackageReleaseID, ExpectedGeneration: job.ExecutionGeneration, ExpectedSessionEpoch: job.FixedSessionEpoch, FreshUntil: r.clock().Add(3 * time.Hour), Result: detailResult}); err != nil {
		return withRuntimeFailure(runtimeFailure{Code: runtimeErrorCode(err)}, err)
	}
	if err := finish(func() error {
		return r.dispatcher.Complete(ctx, v2scan.CompleteJobRequest{JobID: job.ID, WorkerID: job.LeaseOwner, Outcome: "succeeded", ResultDigest: detailResult.Digest, ExpectedEpoch: job.FixedSessionEpoch})
	}); err != nil && !errors.Is(err, v2scan.ErrStaleJob) && !errors.Is(err, v2scan.ErrLeaseLost) {
		return err
	}
	r.Wake()
	return nil
}

func validateSnapshotScope(slice v2scan.SnapshotSliceResult, scope string, markerSchemes []string) error {
	for _, item := range slice.Items {
		var value struct {
			ComicID            string                            `json:"comicId"`
			Membership         struct{ Origin, FolderID string } `json:"membership"`
			ContentObservation struct {
				VisibilityScope string `json:"visibilityScope"`
				MarkerEvidence  struct {
					Scheme string `json:"scheme"`
				} `json:"markerEvidence"`
			} `json:"contentObservation"`
		}
		if err := json.Unmarshal(item.ItemJSON, &value); err != nil || value.ComicID != item.ComicID || value.Membership.Origin != "remoteSnapshot" || value.Membership.FolderID != "0" || value.ContentObservation.VisibilityScope != scope || !containsString(markerSchemes, value.ContentObservation.MarkerEvidence.Scheme) {
			return ErrRuntimeCapability
		}
	}
	return nil
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func sameJSONObject(left, right string) bool {
	var a, b any
	if json.Unmarshal([]byte(left), &a) != nil || json.Unmarshal([]byte(right), &b) != nil {
		return false
	}
	return reflect.DeepEqual(a, b)
}

func (r *Runtime) ensureJobCurrent(ctx context.Context, job v2scan.Job) error {
	var state, leaseOwner, artifactID, releaseID string
	var epoch, generation int64
	err := r.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		// Keep this check SQL-only; the durable lease remains the authority.
		return tx.QueryRowContext(ctx, `SELECT state, COALESCE(lease_owner, ''), artifact_id, package_release_id, fixed_session_epoch, execution_generation FROM scan_jobs WHERE job_id = ?`, job.ID).Scan(&state, &leaseOwner, &artifactID, &releaseID, &epoch, &generation)
	})
	if err != nil {
		return err
	}
	if state != string(v2scan.JobLeased) || leaseOwner != job.LeaseOwner || artifactID != job.ArtifactID || releaseID != job.PackageReleaseID || epoch != job.FixedSessionEpoch || generation != job.ExecutionGeneration {
		return v2scan.ErrStaleJob
	}
	var stale bool
	err = r.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		var demandState string
		var demandGeneration int64
		if err := tx.QueryRowContext(ctx, `SELECT state, execution_generation FROM scan_demands WHERE demand_id = ?`, job.DemandID).Scan(&demandState, &demandGeneration); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				stale = true
				return nil
			}
			return err
		}
		if demandState != string(v2scan.DemandActive) || demandGeneration != job.ExecutionGeneration {
			stale = true
			return nil
		}
		var accountEpoch int64
		var accountState string
		if err := tx.QueryRowContext(ctx, `SELECT session_epoch, state FROM source_accounts WHERE source_account_id = ?`, job.ExecutorSourceAccountID).Scan(&accountEpoch, &accountState); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				stale = true
				return nil
			}
			return err
		}
		if accountState != "active" || accountEpoch != job.FixedSessionEpoch {
			stale = true
			return nil
		}
		var releaseState string
		if err := tx.QueryRowContext(ctx, `SELECT state FROM source_package_releases WHERE package_release_id = ? AND artifact_id = ?`, job.PackageReleaseID, job.ArtifactID).Scan(&releaseState); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				stale = true
				return nil
			}
			return err
		}
		stale = releaseState != "active"
		return nil
	})
	if err != nil {
		return err
	}
	if stale {
		return v2scan.ErrStaleJob
	}
	return nil
}

func (r *Runtime) setSourceStatus(ctx context.Context, accountID, status, reason string) error {
	if accountID == "" {
		return ErrRuntimeInvalid
	}
	return r.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		now := r.clock().UTC().Format(time.RFC3339Nano)
		result, err := tx.ExecContext(ctx, `UPDATE source_scan_status SET status = ?, blocked_reason = ?, last_attempt_at = ?, revision = revision + 1, updated_at = ? WHERE source_account_id = ?`, status, nullableRuntimeString(reason), now, now, accountID)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows == 0 {
			_, err = tx.ExecContext(ctx, `INSERT INTO source_scan_status(source_account_id, status, blocked_reason, last_attempt_at, revision, updated_at) VALUES(?, ?, ?, ?, 1, ?)`, accountID, status, nullableRuntimeString(reason), now, now)
		}
		return err
	})
}

func (r *Runtime) updateAccountScope(ctx context.Context, accountID string, probe *v2scan.ProbeResult) error {
	if probe == nil {
		return ErrRuntimeInvalid
	}
	return r.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		var epoch, sessionRevision int64
		if err := tx.QueryRowContext(ctx, `SELECT session_epoch, session_revision FROM source_accounts WHERE source_account_id = ? AND state = 'active'`, accountID).Scan(&epoch, &sessionRevision); err != nil {
			return err
		}
		now := r.clock().UTC().Format(time.RFC3339Nano)
		attributes := probe.AttributesJSON
		if attributes == "" {
			attributes = "{}"
		}
		if _, err := tx.ExecContext(ctx, `UPDATE source_accounts SET attributes_json = ?, visibility_scope = ?, scope_fresh_until = ?, session_epoch = ?, session_revision = ?, revision = revision + 1, updated_at = ? WHERE source_account_id = ? AND session_epoch = ?`, attributes, probe.VisibilityScope, r.clock().Add(24*time.Hour).UTC().Format(time.RFC3339Nano), epoch+1, sessionRevision+1, now, accountID, epoch); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE source_account_sessions SET session_epoch = ?, session_revision = ?, validated_at = ?, updated_at = ? WHERE source_account_id = ? AND session_epoch = ?`, epoch+1, sessionRevision+1, now, now, accountID, epoch); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE source_scan_status SET status = 'scheduled', blocked_reason = NULL, next_evaluation_at = ?, revision = revision + 1, updated_at = ? WHERE source_account_id = ?`, now, now, accountID)
		return err
	})
}

func (r *Runtime) persistSessionCookies(ctx context.Context, job v2scan.Job, session v2store.ActiveSourceSession, cookies []http.Cookie, profile v2manifest.SessionExportProfile) error {
	if len(cookies) == 0 {
		return nil
	}
	if profile.MaxCookies > 0 && len(cookies) > profile.MaxCookies {
		return ErrRuntimeCapability
	}
	clone := make(map[string]any, len(session.Session)+1)
	for key, value := range session.Session {
		clone[key] = value
	}
	serializedCookies := make([]map[string]any, 0, len(cookies))
	for _, cookie := range cookies {
		serializedCookies = append(serializedCookies, map[string]any{"name": cookie.Name, "value": cookie.Value, "domain": cookie.Domain, "path": cookie.Path, "secure": cookie.Secure, "httpOnly": cookie.HttpOnly})
	}
	clone["cookies"] = serializedCookies
	plain, err := json.Marshal(clone)
	if err != nil || (profile.MaxSerializedBytes > 0 && int64(len(plain)) > profile.MaxSerializedBytes) {
		return ErrRuntimeCapability
	}
	digest := v2crypto.Digest(r.repo.Keys().CredentialHMAC, "session-candidate-v1\x00"+string(plain))
	if digest == session.SessionDigest {
		return nil
	}
	envelope, err := v2crypto.SealSession(r.repo.Keys().SessionAEAD, plain, []byte("source-account-session:"+job.ExecutorSourceAccountID+":"+job.ArtifactID))
	if err != nil {
		return err
	}
	return r.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE source_account_sessions SET session_envelope = ?, session_digest = ?, session_revision = session_revision + 1, validated_at = ?, updated_at = ? WHERE source_account_id = ? AND session_epoch = ? AND session_digest = ?`, envelope, digest, r.clock().UTC().Format(time.RFC3339Nano), r.clock().UTC().Format(time.RFC3339Nano), job.ExecutorSourceAccountID, job.FixedSessionEpoch, session.SessionDigest)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return v2scan.ErrStaleJob
		}
		return nil
	})
}

func workerAsConcrete(worker Worker) *v2worker.Worker {
	concrete, _ := worker.(*v2worker.Worker)
	return concrete
}

func runProbeWithWorker(ctx context.Context, worker Worker, pkg PackageRuntime, cookies []http.Cookie, jobID string, clock func() time.Time) (v2scan.ProbeResult, error) {
	deadline := time.Now().Add(15 * time.Second)
	result, err := worker.Run(ctx, v2worker.RunRequest{RequestID: "probe-" + jobID, Operation: v2worker.OperationProbeAccount, Input: map[string]string{"reason": "snapshot_validation", "requestedAt": clock().UTC().Format(time.RFC3339Nano)}, DeadlineAt: deadline, Budget: v2worker.OperationBudget{MaxRequests: 2, MaxItems: 1, DeadlineAt: deadline}, ArtifactID: pkg.ArtifactID, PackageReleaseID: pkg.PackageReleaseID, CoreHash: pkg.CoreHash, ScanningExtensionHash: pkg.ScanningExtensionHash, AllowedOrigins: pkg.SessionExportProfile.AllowedOrigins, SessionCookies: cookies})
	if err != nil {
		return v2scan.ProbeResult{}, err
	}
	probe, err := v2scan.DecodeProbeResult(result.Output, pkg.AccountProbeContract)
	if err != nil {
		return v2scan.ProbeResult{}, err
	}
	probe.SessionCookies = result.SessionCookies
	return probe, nil
}

func probeErrorCode(err error) string {
	var probe *v2scan.ProbeError
	if errors.As(err, &probe) && probe.Code != "" {
		return allowedRuntimeErrorCode(probe.Code)
	}
	return workerErrorCode(err)
}

func workerErrorCode(err error) string {
	var structured v2worker.StructuredError
	if errors.As(err, &structured) && structured.Code != "" {
		return allowedRuntimeErrorCode(structured.Code)
	}
	if errors.Is(err, v2worker.ErrWorkerDeadline) {
		return "transient"
	}
	return "extension_failure"
}

func allowedRuntimeErrorCode(code string) string {
	switch code {
	case "auth_required", "rate_limited", "transient", "contract_drift", "incomplete_snapshot", "snapshot_changed", "not_found", "forbidden", "extension_failure":
		return code
	default:
		return "extension_failure"
	}
}

func retryAfterFromError(err error) time.Duration {
	var structured v2worker.StructuredError
	if errors.As(err, &structured) && structured.RetryAfterSeconds != nil {
		seconds := *structured.RetryAfterSeconds
		if seconds < 0 {
			seconds = 0
		}
		if seconds > int((7*24*time.Hour)/time.Second) {
			seconds = int((7 * 24 * time.Hour) / time.Second)
		}
		return time.Duration(seconds) * time.Second
	}
	var probe *v2scan.ProbeError
	if errors.As(err, &probe) && probe.RetryAfterSeconds != nil {
		seconds := *probe.RetryAfterSeconds
		if seconds < 0 {
			seconds = 0
		}
		if seconds > int((7*24*time.Hour)/time.Second) {
			seconds = int((7 * 24 * time.Hour) / time.Second)
		}
		return time.Duration(seconds) * time.Second
	}
	return 0
}

func nullableRuntimeString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
