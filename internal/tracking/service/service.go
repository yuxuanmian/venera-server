package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	legacyStore "venera-server/internal/store"
	"venera-server/internal/tracking/catalog"
	"venera-server/internal/tracking/domain"
	trackingruntime "venera-server/internal/tracking/runtime"
	"venera-server/internal/tracking/scan"
	trackingstore "venera-server/internal/tracking/store"
	"venera-server/internal/tracking/worker"
)

const (
	defaultTrackingInterval = 12 * time.Hour
	defaultTrackingDeadline = 2 * time.Minute
	defaultTrackingLease    = 5 * time.Minute
	preLeaseBackoffBase     = 250 * time.Millisecond
	preLeaseBackoffMax      = time.Minute
)

// ServiceOptions wires the production Cloud scanner to the same catalog,
// runtime generation, SQL repository, and encrypted source-session store used
// by the API. Tests may use a temporary Git catalog and the normal Worker
// implementation; no scanner-only test harness is needed.
type ServiceOptions struct {
	Catalog             *catalog.Manager
	Runtime             *trackingruntime.Runtime
	Repository          *trackingstore.Repository
	LegacyStore         *legacyStore.Store
	CookieKey           []byte
	InitJSPath          string
	Interval            time.Duration
	RequestInterval     time.Duration
	MaxConcurrent       int
	SnapshotMaxRequests int
	SnapshotMaxItems    int
	SnapshotDeadline    time.Duration
	MaxAttempts         int
	Client              *http.Client
	UserAgent           string
	Clock               func() time.Time
}

// Service is the Server-owned scanner lifecycle. It reconciles enabled
// client interests, probes each account, runs the revision-owned scanner,
// and publishes only complete generation-fenced snapshots.
type Service struct {
	catalog             *catalog.Manager
	runtime             *trackingruntime.Runtime
	repository          *trackingstore.Repository
	legacyStore         *legacyStore.Store
	cookieKey           []byte
	initJSPath          string
	interval            time.Duration
	requestInterval     time.Duration
	maxConcurrent       int
	maxSnapshotRequests int
	maxSnapshotItems    int
	snapshotDeadline    time.Duration
	maxAttempts         int
	client              *http.Client
	userAgent           string
	clock               func() time.Time

	mu                   sync.Mutex
	started              bool
	cancel               context.CancelFunc
	wg                   sync.WaitGroup
	runMu                sync.Mutex
	wake                 chan struct{}
	leaseID              atomic.Uint64
	preLeaseFailureCount int
	preLeaseRetryAt      time.Time
}

type scanCheckpointKey struct {
	UserID   string
	Artifact domain.ArtifactIdentity
}

type scanGroup struct {
	UserID      string
	DeviceID    string
	Artifact    domain.ArtifactIdentity
	InterestIDs map[string]struct{}
	OldestAt    time.Time
	Demands     []trackingstore.ClientDemandFence
}

func NewService(options ServiceOptions) (*Service, error) {
	if options.Catalog == nil || options.Runtime == nil || options.Repository == nil || options.LegacyStore == nil {
		return nil, errors.New("tracking service dependencies are incomplete")
	}
	if len(options.CookieKey) == 0 {
		return nil, errors.New("tracking service cookie key is required")
	}
	if options.Interval <= 0 {
		options.Interval = defaultTrackingInterval
	}
	if options.RequestInterval < 0 {
		options.RequestInterval = 0
	}
	if options.MaxConcurrent <= 0 {
		options.MaxConcurrent = 2
	}
	if options.SnapshotMaxRequests <= 0 {
		options.SnapshotMaxRequests = 64
	}
	if options.SnapshotMaxItems <= 0 {
		options.SnapshotMaxItems = options.Repository.ObservationLimit()
	}
	if options.SnapshotDeadline <= 0 {
		options.SnapshotDeadline = defaultTrackingDeadline
	}
	if options.MaxAttempts <= 0 {
		options.MaxAttempts = 3
	}
	if options.Client == nil {
		options.Client = &http.Client{}
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.UserAgent == "" {
		options.UserAgent = "VeneraServer/2"
	}
	return &Service{
		catalog:             options.Catalog,
		runtime:             options.Runtime,
		repository:          options.Repository,
		legacyStore:         options.LegacyStore,
		cookieKey:           append([]byte(nil), options.CookieKey...),
		initJSPath:          options.InitJSPath,
		interval:            options.Interval,
		requestInterval:     options.RequestInterval,
		maxConcurrent:       options.MaxConcurrent,
		maxSnapshotRequests: options.SnapshotMaxRequests,
		maxSnapshotItems:    options.SnapshotMaxItems,
		snapshotDeadline:    options.SnapshotDeadline,
		maxAttempts:         options.MaxAttempts,
		client:              options.Client,
		userAgent:           options.UserAgent,
		clock:               options.Clock,
		wake:                make(chan struct{}, 1),
	}, nil
}

// Start launches the immediate scan and periodic follow-up scans. A failed
// candidate is logged and retried at the next interval; it never replaces
// the last complete observations by itself.
func (s *Service) Start(ctx context.Context) error {
	if s == nil {
		return errors.New("tracking service is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return nil
	}
	serviceContext, cancel := context.WithCancel(ctx)
	if err := s.repository.RecoverRunningScanJobs(serviceContext, s.clock().UTC(), true); err != nil {
		cancel()
		s.mu.Unlock()
		return fmt.Errorf("recover tracking scan jobs: %w", err)
	}
	s.started = true
	s.cancel = cancel
	s.wg.Add(1)
	s.mu.Unlock()
	go s.loop(serviceContext)
	return nil
}

func (s *Service) loop(ctx context.Context) {
	defer s.wg.Done()
	for {
		err := s.RunOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("tracking scan: %v", err)
		}
		s.recordPreLeaseResult(err)
		now := s.clock().UTC()
		delay := s.interval
		if next, found, err := s.repository.NextScanWake(ctx, now); err != nil {
			log.Printf("tracking scan wake: %v", err)
		} else if found {
			if !next.After(now) {
				delay = 0
			} else {
				delay = next.Sub(now)
			}
		}
		if retryAt := s.preLeaseRetryBoundary(); !retryAt.IsZero() && retryAt.After(now) {
			backoff := retryAt.Sub(now)
			if delay == 0 || backoff < delay {
				delay = backoff
			}
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-s.wake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
		}
	}
}

// Wake asks the lifecycle to reconcile durable scan demand immediately. It is
// intentionally lossy: a buffered edge is sufficient because RunOnce reads
// the complete current client/catalog projection before scheduling.
func (s *Service) Wake() {
	if s == nil {
		return
	}
	s.mu.Lock()
	// An explicit wake is a user/catalog demand edge and must not wait for the
	// lifecycle backoff accumulated by an earlier unavailable-authority run.
	s.preLeaseFailureCount = 0
	s.preLeaseRetryAt = time.Time{}
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Service) recordPreLeaseResult(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var preLease *preLeaseRunError
	if errors.As(err, &preLease) {
		s.preLeaseFailureCount++
		backoff := preLeaseBackoffBase
		for index := 1; index < s.preLeaseFailureCount && backoff < preLeaseBackoffMax; index++ {
			backoff *= 2
			if backoff > preLeaseBackoffMax {
				backoff = preLeaseBackoffMax
			}
		}
		s.preLeaseRetryAt = s.clock().UTC().Add(backoff)
		return
	}
	// Once the lifecycle reached the worker boundary, a worker or incomplete
	// slice result must not inherit unavailable-authority backoff.
	s.preLeaseFailureCount = 0
	s.preLeaseRetryAt = time.Time{}
}

func (s *Service) preLeaseRetryBoundary() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.preLeaseRetryAt
}

// Close stops the scanner lifecycle and waits for the current scan to leave
// its worker boundary. It is safe to call more than once.
func (s *Service) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	cancel := s.cancel
	s.cancel = nil
	s.started = false
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	s.wg.Wait()
	return nil
}

// RunOnce executes the same production pipeline used by Start and is exposed
// for API-level composition tests and controlled operator runs.
func (s *Service) RunOnce(ctx context.Context) error {
	if s == nil {
		return errors.New("tracking service is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.runMu.Lock()
	defer s.runMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.repository.RecoverRunningScanJobs(ctx, s.clock().UTC(), false); err != nil {
		return preLeaseError(err)
	}
	managed, ok := s.catalog.Active()
	if !ok {
		return preLeaseError(errors.New("tracking catalog is not active"))
	}
	authority, ok := s.runtime.Authority()
	if !ok {
		return preLeaseError(errors.New("tracking runtime is not active"))
	}
	if authority.CatalogID != managed.Authority.CatalogID ||
		authority.ActiveRevision != managed.Authority.ActiveRevision ||
		authority.Generation != managed.Authority.Generation {
		return preLeaseError(errors.New("tracking catalog/runtime authority diverged"))
	}
	states, err := s.repository.ListClientStates(ctx)
	if err != nil {
		return preLeaseError(err)
	}
	demands, err := s.runtime.ReconcileInterests(states)
	if err != nil {
		return preLeaseError(err)
	}
	demanded := make(map[serviceDemandKey]struct{}, len(demands))
	for _, demand := range demands {
		demanded[serviceDemandKey{Artifact: demand.Artifact, ComicID: demand.ComicID}] = struct{}{}
	}
	groups, err := s.buildGroups(states, demanded)
	if err != nil {
		return preLeaseError(err)
	}

	capabilities := make(map[domain.ArtifactIdentity]catalog.Capability, len(managed.Capabilities))
	dispatcher := scan.NewDispatcher(scan.DispatcherOptions{
		Clock:         s.clock,
		MaxConcurrent: s.maxConcurrent,
	})
	for _, capability := range managed.Capabilities {
		capabilities[capability.Artifact] = capability
		lane := scan.NewSourceLane(capability.Artifact)
		dispatcher.AddLane(lane)
	}
	jobSpecs := make([]trackingstore.ScanJobSpec, 0, len(groups))
	groupByKey := make(map[string]scanGroup, len(groups))
	for index := range groups {
		group := groups[index]
		if _, capable := capabilities[group.Artifact]; !capable {
			continue
		}
		demandDigest, digestErr := scanGroupDemandDigest(group, authority)
		if digestErr != nil {
			return preLeaseError(digestErr)
		}
		jobSpecs = append(jobSpecs, trackingstore.ScanJobSpec{
			UserID:            group.UserID,
			Artifact:          group.Artifact,
			CatalogID:         authority.CatalogID,
			CatalogRevision:   authority.ActiveRevision,
			RuntimeGeneration: authority.Generation,
			DemandDigest:      demandDigest,
			DueAt:             s.clock().UTC(),
			Priority:          trackingstore.ScanJobPriorityNormal,
			MaxAttempts:       s.maxAttempts,
		})
		groupByKey[scanGroupKey(group.UserID, group.Artifact)] = group
	}
	if err := s.repository.ReconcileScanJobs(ctx, jobSpecs, s.clock().UTC()); err != nil {
		return preLeaseError(err)
	}
	dueJobs, err := s.repository.ListDueScanJobs(ctx, s.clock().UTC(), len(jobSpecs))
	if err != nil {
		return preLeaseError(err)
	}
	candidates := make([]scan.LaneCandidate, 0, len(dueJobs))
	for _, job := range dueJobs {
		group, exists := groupByKey[scanGroupKey(job.UserID, job.Artifact)]
		if !exists {
			continue
		}
		oldest := group.OldestAt
		if oldest.IsZero() {
			oldest = s.clock().UTC()
		}
		candidates = append(candidates, scan.LaneCandidate{
			ID:        fmt.Sprintf("scan-job-%d", job.JobID),
			Artifact:  group.Artifact,
			Kind:      scan.JobAccountSnapshotSlice,
			OldestAt:  oldest,
			ReadyAt:   job.DueAt,
			CreatedAt: job.CreatedAt,
			Priority:  scanPriority(job.Priority),
			MaxWait:   time.Minute,
			Payload:   scanJobPayload{Job: job, Group: group},
		})
	}
	remaining := append([]scan.LaneCandidate(nil), candidates...)
	var runErrors []error
	type groupResult struct {
		job   trackingstore.ScanJob
		group scanGroup
		err   error
	}
	done := make(chan groupResult, len(remaining))
	running := 0
	for len(remaining) > 0 || running > 0 {
		for len(remaining) > 0 && running < s.maxConcurrent {
			if err := ctx.Err(); err != nil {
				break
			}
			candidate, nextErr := dispatcher.Next(remaining)
			if nextErr != nil {
				break
			}
			for index := range remaining {
				if remaining[index].ID == candidate.ID {
					remaining = append(remaining[:index], remaining[index+1:]...)
					break
				}
			}
			payload, valid := candidate.Payload.(scanJobPayload)
			if !valid {
				dispatcher.ReleaseLease()
				continue
			}
			leaseToken := s.nextLeaseToken(payload.Job.JobID)
			claimed, claimErr := s.repository.ClaimScanJob(ctx, payload.Job.JobID, leaseToken, s.clock().UTC(), s.scanLeaseDuration())
			if claimErr != nil {
				dispatcher.ReleaseLease()
				runErrors = append(runErrors, claimErr)
				continue
			}
			if !claimed {
				dispatcher.ReleaseLease()
				continue
			}
			running++
			go func(payload scanJobPayload, leaseToken string) {
				runErr := s.runGroupWithRetry(ctx, managed, authority, capabilities[payload.Job.Artifact], payload.Group)
				finishErr := s.finishScanJob(ctx, payload.Job, leaseToken, runErr)
				if finishErr != nil {
					runErr = errors.Join(runErr, finishErr)
				}
				dispatcher.ReleaseLease()
				done <- groupResult{job: payload.Job, group: payload.Group, err: runErr}
			}(payload, leaseToken)
		}
		if running == 0 {
			break
		}
		completed := <-done
		running--
		if completed.err != nil {
			runErrors = append(runErrors, fmt.Errorf("%s/%s/%s: %w", completed.group.UserID, completed.group.Artifact.SourceKey, completed.group.Artifact.FileName, completed.err))
		}
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(err, errors.Join(runErrors...))
	}
	return errors.Join(runErrors...)
}

type serviceDemandKey struct {
	Artifact domain.ArtifactIdentity
	ComicID  string
}

// preLeaseRunError identifies failures before a durable scan job is claimed.
// The lifecycle uses it only to bound retries; the public RunOnce error still
// unwraps to the original cause for callers and diagnostics.
type preLeaseRunError struct{ err error }

func (e *preLeaseRunError) Error() string { return e.err.Error() }

func (e *preLeaseRunError) Unwrap() error { return e.err }

func preLeaseError(err error) error {
	if err == nil {
		return nil
	}
	return &preLeaseRunError{err: err}
}

type scanJobPayload struct {
	Job   trackingstore.ScanJob
	Group scanGroup
}

func scanGroupKey(userID string, artifact domain.ArtifactIdentity) string {
	return userID + "\x00" + artifact.SourceKey + "\x00" + artifact.FileName
}

func scanPriority(priority trackingstore.ScanJobPriority) scan.PriorityClass {
	if priority == trackingstore.ScanJobPriorityExpedited {
		return scan.PriorityExpedited
	}
	return scan.PriorityNormal
}

func scanGroupDemandDigest(group scanGroup, authority domain.Authority) (string, error) {
	interestIDs := make([]string, 0, len(group.InterestIDs))
	for interestID := range group.InterestIDs {
		interestIDs = append(interestIDs, interestID)
	}
	sort.Strings(interestIDs)
	demands := append([]trackingstore.ClientDemandFence(nil), group.Demands...)
	sort.Slice(demands, func(i, j int) bool { return demands[i].DeviceID < demands[j].DeviceID })
	for index := range demands {
		demands[index].InterestIDs = append([]string(nil), demands[index].InterestIDs...)
		sort.Strings(demands[index].InterestIDs)
	}
	return domain.Digest(struct {
		UserID            string
		Artifact          domain.ArtifactIdentity
		CatalogID         string
		CatalogRevision   string
		RuntimeGeneration int64
		InterestIDs       []string
		Demands           []trackingstore.ClientDemandFence
	}{
		UserID:            group.UserID,
		Artifact:          group.Artifact,
		CatalogID:         authority.CatalogID,
		CatalogRevision:   authority.ActiveRevision,
		RuntimeGeneration: authority.Generation,
		InterestIDs:       interestIDs,
		Demands:           demands,
	})
}

func (s *Service) nextLeaseToken(jobID int64) string {
	sequence := s.leaseID.Add(1)
	return fmt.Sprintf("tracking-%d-%d-%d", time.Now().UnixNano(), jobID, sequence)
}

func (s *Service) scanLeaseDuration() time.Duration {
	duration := s.snapshotDeadline * time.Duration(s.maxAttempts)
	if duration < defaultTrackingLease {
		duration = defaultTrackingLease
	}
	return duration + time.Minute
}

func (s *Service) finishScanJob(ctx context.Context, job trackingstore.ScanJob, leaseToken string, runErr error) error {
	// A canceled scanner context must not also cancel the small durable state
	// transition that releases its lease. Without this boundary, a graceful
	// shutdown could leave an apparently running job until its lease expires.
	finishContext := context.WithoutCancel(ctx)
	now := s.clock().UTC()
	if runErr == nil {
		return s.repository.CompleteScanJob(finishContext, job.JobID, leaseToken, now.Add(s.interval), now)
	}
	if errors.Is(runErr, errIncompleteSnapshot) {
		return s.repository.RescheduleScanJob(finishContext, job.JobID, leaseToken, now, now, runErr.Error())
	}
	if errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) && ctx.Err() != nil {
		return s.repository.ReleaseScanJob(finishContext, job.JobID, leaseToken, now, runErr.Error())
	}
	return s.repository.FailScanJob(
		finishContext,
		job.JobID,
		leaseToken,
		now,
		runErr.Error(),
		retryableScanError(runErr),
		scanRetryAfter(runErr),
		s.interval,
	)
}

func scanRetryAfter(err error) time.Duration {
	var structured worker.StructuredError
	if errors.As(err, &structured) && structured.RetryAfterSeconds != nil && *structured.RetryAfterSeconds > 0 {
		return time.Duration(*structured.RetryAfterSeconds) * time.Second
	}
	return 250 * time.Millisecond
}

func (s *Service) buildGroups(states []domain.ClientState, demanded map[serviceDemandKey]struct{}) ([]scanGroup, error) {
	groups := make(map[scanCheckpointKey]*scanGroup)
	for _, state := range states {
		if !state.CloudEnabled {
			continue
		}
		device, err := s.legacyStore.GetDeviceByID(state.DeviceID)
		if err != nil {
			continue
		}
		for _, interest := range state.Interests {
			if _, ok := demanded[serviceDemandKey{Artifact: interest.Artifact, ComicID: interest.ComicID}]; !ok {
				continue
			}
			key := scanCheckpointKey{UserID: device.UserID, Artifact: interest.Artifact}
			group := groups[key]
			if group == nil {
				group = &scanGroup{
					UserID:      device.UserID,
					DeviceID:    state.DeviceID,
					Artifact:    interest.Artifact,
					InterestIDs: make(map[string]struct{}),
					OldestAt:    state.UpdatedAt,
				}
				groups[key] = group
			}
			group.InterestIDs[interest.ComicID] = struct{}{}
			var demand *trackingstore.ClientDemandFence
			for index := range group.Demands {
				if group.Demands[index].DeviceID == state.DeviceID {
					demand = &group.Demands[index]
					break
				}
			}
			if demand == nil {
				group.Demands = append(group.Demands, trackingstore.ClientDemandFence{
					DeviceID: state.DeviceID, StateRevision: state.StateRevision,
				})
				demand = &group.Demands[len(group.Demands)-1]
			}
			if !containsString(demand.InterestIDs, interest.ComicID) {
				demand.InterestIDs = append(demand.InterestIDs, interest.ComicID)
			}
			if !state.UpdatedAt.IsZero() && (group.OldestAt.IsZero() || state.UpdatedAt.Before(group.OldestAt)) {
				group.OldestAt = state.UpdatedAt
			}
		}
	}
	result := make([]scanGroup, 0, len(groups))
	for _, group := range groups {
		for index := range group.Demands {
			sort.Strings(group.Demands[index].InterestIDs)
		}
		result = append(result, *group)
	}
	sort.Slice(result, func(i, j int) bool {
		return scanGroupKey(result[i].UserID, result[i].Artifact) < scanGroupKey(result[j].UserID, result[j].Artifact)
	})
	return result, nil
}

func (s *Service) runGroup(
	ctx context.Context,
	managed catalog.Snapshot,
	authority domain.Authority,
	capability catalog.Capability,
	group scanGroup,
) error {
	token, err := s.runtime.Capture(group.Artifact)
	if err != nil {
		return err
	}
	if group.Artifact.SourceKey != "manwa" || group.Artifact.FileName != "manwa.js" {
		return errors.New("no approved production scanner contract for artifact")
	}
	session, err := s.legacyStore.GetSourceSession(group.UserID, group.Artifact.SourceKey)
	if err != nil {
		return fmt.Errorf("load source session: %w", err)
	}
	sessionEpoch := session.CookieHash + "\x00" + session.UpdatedAt
	cookies, err := scan.LoadEncryptedSourceSessionCookies(
		s.legacyStore,
		s.cookieKey,
		group.UserID,
		group.Artifact.SourceKey,
	)
	if err != nil {
		return err
	}
	runtimeWorker, err := s.newWorker(managed, capability)
	if err != nil {
		return err
	}
	contract := scan.AccountProbeContract{
		ID:                     "manwa-account-v1",
		Version:                1,
		IdentitySchemes:        []string{"manwa-username-v1"},
		AttributeFields:        []string{"accountLevel"},
		VisibilityScopePattern: `^manwa:level:\d+$`,
	}
	probe, err := scan.NewAccountProbeRunner(runtimeWorker).Probe(ctx, scan.ProbeRequest{
		RequestID:         "probe-" + group.UserID + "-" + group.Artifact.SourceKey,
		Artifact:          group.Artifact,
		Revision:          token.Revision,
		RuntimeGeneration: token.Generation,
		Contract:          contract,
		SessionCookies:    cookies,
		Reason:            "tracking_snapshot",
		RequestedAt:       s.clock().UTC(),
	})
	if err != nil {
		return err
	}
	var attributes map[string]any
	if err := json.Unmarshal([]byte(probe.AttributesJSON), &attributes); err != nil {
		return fmt.Errorf("decode account attributes: %w", err)
	}
	account := map[string]any{
		"identity": map[string]any{
			"scheme": probe.IdentityScheme,
			"value":  probe.IdentityValue,
		},
		"display": map[string]any{
			"name":      probe.DisplayName,
			"secondary": probe.DisplaySecondary,
		},
		"attributes":      attributes,
		"visibilityScope": probe.VisibilityScope,
	}

	fence := trackingstore.PublicationFence{
		SessionEpoch:    sessionEpoch,
		VisibilityScope: probe.VisibilityScope,
		Demands:         append([]trackingstore.ClientDemandFence(nil), group.Demands...),
	}
	if err := s.repository.RecordAccountScope(ctx, group.UserID, group.Artifact, sessionEpoch, probe.VisibilityScope); err != nil {
		return err
	}
	progress, progressFound, err := s.repository.GetPartialSnapshot(ctx, group.UserID, group.Artifact)
	if err != nil {
		if errors.Is(err, trackingstore.ErrPartialSnapshotInvalid) {
			_ = s.repository.DeletePartialSnapshot(ctx, group.UserID, group.Artifact)
			progressFound = false
		} else {
			return err
		}
	}
	if progressFound && !sameProgressFence(progress, authority, token, fence) {
		if err := s.repository.DeletePartialSnapshot(ctx, group.UserID, group.Artifact); err != nil {
			return err
		}
		progressFound = false
	}
	checkpoint := json.RawMessage(nil)
	accumulated := []domain.Observation(nil)
	if progressFound {
		checkpoint = append(json.RawMessage(nil), progress.Checkpoint...)
		accumulated = append([]domain.Observation(nil), progress.Observations...)
	}
	deadline := s.clock().Add(s.snapshotDeadline)
	executor := scan.NewSnapshotExecutor(runtimeWorker)
	executor.MaxItems = s.maxSnapshotItems
	executor.Clock = s.clock
	result, err := executor.Run(ctx, scan.SnapshotRequest{
		RequestID:         "snapshot-" + group.UserID + "-" + group.Artifact.SourceKey,
		Artifact:          group.Artifact,
		Revision:          token.Revision,
		RuntimeGeneration: token.Generation,
		Account:           account,
		Checkpoint:        checkpoint,
		Budget: worker.OperationBudget{
			MaxRequests: s.maxSnapshotRequests,
			MaxItems:    s.maxSnapshotItems,
			DeadlineAt:  deadline,
		},
		SessionCookies: probe.SessionCookies,
	})
	if err != nil {
		return err
	}
	if err := s.runtime.RequireCommit(token); err != nil {
		return err
	}
	if !result.Complete {
		combined, combineErr := combineObservations(accumulated, result.Observations, group.UserID)
		if combineErr != nil {
			_ = s.repository.DeletePartialSnapshot(ctx, group.UserID, group.Artifact)
			return combineErr
		}
		expected := result.ExpectedTotal
		if progressFound && progress.ExpectedTotal != nil && expected != nil && *progress.ExpectedTotal != *expected {
			_ = s.repository.DeletePartialSnapshot(ctx, group.UserID, group.Artifact)
			return errSnapshotBoundaryDrift
		}
		if expected == nil && progressFound {
			expected = progress.ExpectedTotal
		}
		if expected == nil {
			return errSnapshotBoundaryDrift
		}
		if err := s.repository.SavePartialSnapshot(ctx, trackingstore.PartialSnapshot{
			UserID: group.UserID, Artifact: group.Artifact, CatalogID: authority.CatalogID,
			CatalogRevision: token.Revision, RuntimeGeneration: token.Generation, Fence: fence,
			ExpectedTotal: expected, Checkpoint: result.Checkpoint, Observations: combined,
			UpdatedAt: s.clock().UTC(),
		}); err != nil {
			return err
		}
		return errIncompleteSnapshot
	}
	combined, err := combineObservations(accumulated, result.Observations, group.UserID)
	if err != nil {
		_ = s.repository.DeletePartialSnapshot(ctx, group.UserID, group.Artifact)
		return err
	}
	expected := result.ExpectedTotal
	if progressFound {
		if expected != nil && progress.ExpectedTotal != nil && *expected != *progress.ExpectedTotal {
			_ = s.repository.DeletePartialSnapshot(ctx, group.UserID, group.Artifact)
			return errSnapshotBoundaryDrift
		}
		expected = progress.ExpectedTotal
	}
	if expected != nil && len(combined) != *expected {
		_ = s.repository.DeletePartialSnapshot(ctx, group.UserID, group.Artifact)
		return errSnapshotBoundaryDrift
	}
	observations := make([]domain.Observation, 0, len(combined))
	for _, observation := range combined {
		if _, wanted := group.InterestIDs[observation.ComicID]; wanted {
			observations = append(observations, observation)
		}
	}
	currentSnapshot, err := trackingruntime.BuildSnapshot(
		authority,
		observations,
		s.clock().UTC(),
		s.repository.ObservationLimit(),
	)
	if err != nil {
		return err
	}
	if err := s.runtime.RequireCommit(token); err != nil {
		return err
	}
	if err := s.repository.ReplaceObservationsWithFence(
		ctx,
		group.UserID,
		group.Artifact,
		token.Revision,
		token.Generation,
		currentSnapshot.Observations,
		authority,
		fence,
	); err != nil {
		return err
	}
	return nil
}

func (s *Service) newWorker(managed catalog.Snapshot, capability catalog.Capability) (*worker.Worker, error) {
	extension, err := os.ReadFile(capability.ScannerPath)
	if err != nil {
		return nil, fmt.Errorf("read scanner extension: %w", err)
	}
	var core []byte
	if s.initJSPath != "" {
		core, err = os.ReadFile(s.initJSPath)
		if err != nil {
			return nil, fmt.Errorf("read scanner runtime: %w", err)
		}
	}
	return worker.NewWorker(worker.WorkerConfig{
		CoreScript:              core,
		ExtensionScript:         extension,
		AllowedOrigins:          []string{"https://manwa.me"},
		Client:                  s.client,
		UserAgent:               s.userAgent,
		MaxItems:                s.maxSnapshotItems,
		MaxCheckpointBytes:      16 << 10,
		DefaultOperationTimeout: s.snapshotDeadline,
		MinRequestStartInterval: s.requestInterval,
	})
}

var (
	errIncompleteSnapshot    = errors.New("scanner returned an incomplete snapshot")
	errSnapshotBoundaryDrift = errors.New("scanner snapshot boundary drift")
)

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func combineObservations(existing, incoming []domain.Observation, userID string) ([]domain.Observation, error) {
	result := make([]domain.Observation, 0, len(existing)+len(incoming))
	seen := make(map[string]struct{}, len(existing)+len(incoming))
	for _, source := range append(append([]domain.Observation(nil), existing...), incoming...) {
		if source.UserID != "" && source.UserID != userID {
			return nil, errors.New("scanner observation user boundary mismatch")
		}
		source.UserID = userID
		if _, exists := seen[source.ComicID]; exists {
			return nil, errSnapshotBoundaryDrift
		}
		seen[source.ComicID] = struct{}{}
		result = append(result, source)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ComicID < result[j].ComicID })
	return result, nil
}

func sameProgressFence(progress trackingstore.PartialSnapshot, authority domain.Authority, token trackingruntime.GenerationToken, fence trackingstore.PublicationFence) bool {
	return progress.CatalogID == authority.CatalogID &&
		progress.CatalogRevision == token.Revision &&
		progress.RuntimeGeneration == token.Generation &&
		publicationFenceEqual(progress.Fence, fence)
}

func publicationFenceEqual(left, right trackingstore.PublicationFence) bool {
	if left.SessionEpoch != right.SessionEpoch || left.VisibilityScope != right.VisibilityScope || len(left.Demands) != len(right.Demands) {
		return false
	}
	leftDemands := append([]trackingstore.ClientDemandFence(nil), left.Demands...)
	rightDemands := append([]trackingstore.ClientDemandFence(nil), right.Demands...)
	sort.Slice(leftDemands, func(i, j int) bool { return leftDemands[i].DeviceID < leftDemands[j].DeviceID })
	sort.Slice(rightDemands, func(i, j int) bool { return rightDemands[i].DeviceID < rightDemands[j].DeviceID })
	for index := range leftDemands {
		if leftDemands[index].DeviceID != rightDemands[index].DeviceID || leftDemands[index].StateRevision != rightDemands[index].StateRevision || !sameStringSlice(leftDemands[index].InterestIDs, rightDemands[index].InterestIDs) {
			return false
		}
	}
	return true
}

func (s *Service) runGroupWithRetry(
	ctx context.Context,
	managed catalog.Snapshot,
	authority domain.Authority,
	capability catalog.Capability,
	group scanGroup,
) error {
	var last error
	for attempt := 1; attempt <= s.maxAttempts; attempt++ {
		last = s.runGroup(ctx, managed, authority, capability, group)
		if last == nil || !retryableScanError(last) || attempt == s.maxAttempts {
			return last
		}
		delay := time.Duration(attempt) * 250 * time.Millisecond
		var structured worker.StructuredError
		if errors.As(last, &structured) && structured.RetryAfterSeconds != nil {
			delay = time.Duration(*structured.RetryAfterSeconds) * time.Second
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
	return last
}

func retryableScanError(err error) bool {
	if err == nil ||
		errors.Is(err, errIncompleteSnapshot) ||
		errors.Is(err, errSnapshotBoundaryDrift) ||
		errors.Is(err, trackingstore.ErrAuthorizationMismatch) ||
		errors.Is(err, trackingruntime.ErrGenerationRejected) {
		return false
	}
	var structured worker.StructuredError
	if errors.As(err, &structured) {
		return structured.Code == "transient" || structured.Code == "rate_limited"
	}
	return false
}

func sameStringSlice(left, right []string) bool {
	left = append([]string(nil), left...)
	right = append([]string(nil), right...)
	sort.Strings(left)
	sort.Strings(right)
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
