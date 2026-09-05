package scan

import (
	"errors"
	"sort"
	"sync"
	"time"

	"venera-server/internal/tracking/domain"
)

var (
	ErrNoReadyJob     = errors.New("no ready scan job")
	ErrDispatcherBusy = errors.New("scan dispatcher has no available worker")
)

type DispatcherOptions struct {
	Clock         func() time.Time
	MaxConcurrent int
}

type Dispatcher struct {
	clock         func() time.Time
	maxConcurrent int

	mu         sync.Mutex
	lanes      map[domain.ArtifactIdentity]*SourceLane
	laneOrder  []domain.ArtifactIdentity
	laneCursor int
	active     int
}

func NewDispatcher(options DispatcherOptions) *Dispatcher {
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	maxConcurrent := options.MaxConcurrent
	if maxConcurrent <= 0 {
		maxConcurrent = 1
	}
	return &Dispatcher{
		clock:         clock,
		maxConcurrent: maxConcurrent,
		lanes:         make(map[domain.ArtifactIdentity]*SourceLane),
	}
}

func (dispatcher *Dispatcher) AddLane(lane *SourceLane) {
	if dispatcher == nil || lane == nil {
		return
	}
	dispatcher.mu.Lock()
	defer dispatcher.mu.Unlock()
	if _, exists := dispatcher.lanes[lane.Artifact]; !exists {
		dispatcher.laneOrder = append(dispatcher.laneOrder, lane.Artifact)
		sort.Slice(dispatcher.laneOrder, func(i, j int) bool {
			return artifactLess(dispatcher.laneOrder[i], dispatcher.laneOrder[j])
		})
	}
	dispatcher.lanes[lane.Artifact] = lane
}

func (dispatcher *Dispatcher) AddArtifact(artifact domain.ArtifactIdentity) {
	if err := artifact.Validate(); err != nil {
		return
	}
	dispatcher.AddLane(NewSourceLane(artifact))
}

func (dispatcher *Dispatcher) ActiveLeases() int {
	if dispatcher == nil {
		return 0
	}
	dispatcher.mu.Lock()
	defer dispatcher.mu.Unlock()
	return dispatcher.active
}

func (dispatcher *Dispatcher) ReleaseLease() {
	if dispatcher == nil {
		return
	}
	dispatcher.mu.Lock()
	if dispatcher.active > 0 {
		dispatcher.active--
	}
	dispatcher.mu.Unlock()
}

// Next reserves one in-process worker slot and selects a candidate in
// round-robin artifact order. The caller must call ReleaseLease once the job
// finishes or is discarded.
func (dispatcher *Dispatcher) Next(candidates []LaneCandidate) (LaneCandidate, error) {
	if dispatcher == nil {
		return LaneCandidate{}, ErrNoReadyJob
	}
	dispatcher.mu.Lock()
	if dispatcher.active >= dispatcher.maxConcurrent {
		dispatcher.mu.Unlock()
		return LaneCandidate{}, ErrDispatcherBusy
	}
	dispatcher.active++
	defer func() {
		// A successful selection transfers the reservation to the caller. The
		// no-selection path releases it below.
		dispatcher.mu.Unlock()
	}()
	if len(dispatcher.laneOrder) == 0 {
		dispatcher.active--
		return LaneCandidate{}, ErrNoReadyJob
	}
	now := dispatcher.clock().UTC()
	start := dispatcher.laneCursor % len(dispatcher.laneOrder)
	for offset := 0; offset < len(dispatcher.laneOrder); offset++ {
		index := (start + offset) % len(dispatcher.laneOrder)
		lane := dispatcher.lanes[dispatcher.laneOrder[index]]
		if candidate, ok := lane.Pick(candidates, now); ok {
			dispatcher.laneCursor = (index + 1) % len(dispatcher.laneOrder)
			return candidate, nil
		}
	}
	dispatcher.active--
	return LaneCandidate{}, ErrNoReadyJob
}

func artifactLess(left, right domain.ArtifactIdentity) bool {
	if left.SourceKey != right.SourceKey {
		return left.SourceKey < right.SourceKey
	}
	return left.FileName < right.FileName
}
