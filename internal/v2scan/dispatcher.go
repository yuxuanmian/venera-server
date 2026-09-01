package v2scan

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

var (
	ErrNoReadyJob     = errors.New("no ready scan job")
	ErrDispatcherBusy = errors.New("scan dispatcher has no available worker")
)

type DispatcherOptions struct {
	Clock         func() time.Time
	MaxConcurrent int
	LeaseDuration time.Duration
	WorkerID      string
}

type Dispatcher struct {
	repo          *DemandRepository
	clock         func() time.Time
	maxConcurrent int
	leaseDuration time.Duration
	workerID      string

	mu         sync.Mutex
	lanes      map[string]*SourceLane
	laneOrder  []string
	laneCursor int
	active     int
}

func NewDispatcher(repo *DemandRepository, options DispatcherOptions) *Dispatcher {
	clock := options.Clock
	if clock == nil && repo != nil && repo.repo != nil && repo.repo.DB() != nil {
		clock = repo.repo.DB().Now
	}
	if clock == nil {
		clock = time.Now
	}
	maxConcurrent := options.MaxConcurrent
	if maxConcurrent <= 0 {
		maxConcurrent = 1
	}
	leaseDuration := options.LeaseDuration
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}
	workerID := options.WorkerID
	if workerID == "" {
		workerID = "dispatcher"
	}
	return &Dispatcher{
		repo: repo, clock: clock, maxConcurrent: maxConcurrent,
		leaseDuration: leaseDuration, workerID: workerID, lanes: make(map[string]*SourceLane),
	}
}

func (d *Dispatcher) AddLane(lane *SourceLane) {
	if d == nil || lane == nil || lane.ArtifactID == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.lanes[lane.ArtifactID]; !exists {
		d.laneOrder = append(d.laneOrder, lane.ArtifactID)
		sort.Strings(d.laneOrder)
	}
	d.lanes[lane.ArtifactID] = lane
}

func (d *Dispatcher) AddArtifact(artifactID string) {
	if artifactID == "" {
		return
	}
	d.AddLane(NewSourceLane(artifactID))
}

func (d *Dispatcher) ActiveLeases() int {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.active
}

func (d *Dispatcher) ReleaseLease() {
	if d == nil {
		return
	}
	d.mu.Lock()
	if d.active > 0 {
		d.active--
	}
	d.mu.Unlock()
}

func (d *Dispatcher) Next(ctx context.Context) (JobLease, error) {
	if d == nil || d.repo == nil {
		return JobLease{}, ErrNoReadyJob
	}
	d.mu.Lock()
	if d.active >= d.maxConcurrent {
		d.mu.Unlock()
		return JobLease{}, ErrDispatcherBusy
	}
	// Reserve the global slot before reading durable candidates.  Reconcile
	// may be called concurrently by a wake and a fallback tick; reserving here
	// keeps the in-process cap authoritative in that case as well.
	d.active++
	reserved := true
	start := d.laneCursor
	d.mu.Unlock()
	releaseReservation := func() {
		if !reserved {
			return
		}
		reserved = false
		d.ReleaseLease()
	}

	jobs, err := d.repo.ListReadyJobs(ctx, "")
	if err != nil {
		releaseReservation()
		return JobLease{}, err
	}
	demands, err := d.repo.ListDemands(ctx, "")
	if err != nil {
		releaseReservation()
		return JobLease{}, err
	}
	demandByID := make(map[string]Demand, len(demands))
	for _, demand := range demands {
		demandByID[demand.ID] = demand
	}
	jobsByArtifact := make(map[string][]LaneCandidate)
	for _, job := range jobs {
		demand := demandByID[job.DemandID]
		jobsByArtifact[job.ArtifactID] = append(jobsByArtifact[job.ArtifactID], LaneCandidate{
			Job: job, OldestAt: demand.OldestAt, Priority: demand.PriorityClass,
			ReadyAt: job.NextEligibleAt,
		})
	}
	d.mu.Lock()
	for artifactID := range jobsByArtifact {
		if _, exists := d.lanes[artifactID]; !exists {
			d.lanes[artifactID] = NewSourceLane(artifactID)
			d.laneOrder = append(d.laneOrder, artifactID)
		}
	}
	sort.Strings(d.laneOrder)
	lanes := make([]*SourceLane, len(d.laneOrder))
	for i, artifactID := range d.laneOrder {
		lanes[i] = d.lanes[artifactID]
	}
	if len(lanes) > 0 {
		start = d.laneCursor % len(lanes)
	}
	d.mu.Unlock()
	if len(lanes) == 0 {
		return JobLease{}, ErrNoReadyJob
	}
	now := d.clock().UTC()
	for offset := 0; offset < len(lanes); offset++ {
		index := (start + offset) % len(lanes)
		lane := lanes[index]
		if lane == nil {
			continue
		}
		candidate, ok := lane.Pick(jobsByArtifact[lane.ArtifactID], now)
		if !ok {
			continue
		}
		leased, leaseErr := d.repo.LeaseJob(ctx, candidate.Job.ID, d.workerID, d.leaseDuration)
		if errors.Is(leaseErr, ErrLeaseLost) || errors.Is(leaseErr, ErrDemandNotRunnable) || errors.Is(leaseErr, ErrStaleJob) {
			continue
		}
		if leaseErr != nil {
			releaseReservation()
			return JobLease{}, leaseErr
		}
		d.mu.Lock()
		d.laneCursor = (index + 1) % len(lanes)
		d.mu.Unlock()
		reserved = false
		return leased, nil
	}
	releaseReservation()
	return JobLease{}, ErrNoReadyJob
}

func (d *Dispatcher) Complete(ctx context.Context, request CompleteJobRequest) error {
	if d == nil || d.repo == nil {
		return ErrInvalidJobCompletion
	}
	err := d.repo.CompleteJob(ctx, request)
	// A dispatcher slot belongs to the in-process lease attempt, not to the
	// durable outcome.  Release it for success, discard, retry, storage error,
	// and lease loss alike.
	d.ReleaseLease()
	return err
}

func (d *Dispatcher) Yield(ctx context.Context, request YieldJobRequest) error {
	if d == nil || d.repo == nil {
		return ErrInvalidJobCompletion
	}
	err := d.repo.YieldJob(ctx, request)
	d.ReleaseLease()
	return err
}

func (d *Dispatcher) DiscardAndAdvance(ctx context.Context, request CompleteJobRequest) error {
	if d == nil || d.repo == nil {
		return ErrInvalidJobCompletion
	}
	err := d.repo.DiscardJobAndAdvance(ctx, request)
	d.ReleaseLease()
	return err
}
