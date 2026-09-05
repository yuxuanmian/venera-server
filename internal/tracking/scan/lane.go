package scan

import (
	"sort"
	"time"

	"venera-server/internal/tracking/domain"
)

type JobKind string

const (
	JobAccountSnapshotSlice JobKind = "accountSnapshotSlice"
	JobComicDetailBatch     JobKind = "comicDetailBatch"
	JobMaintenance          JobKind = "maintenance"
)

type PriorityClass string

const (
	PriorityNormal    PriorityClass = "normal"
	PriorityExpedited PriorityClass = "expedited"
)

type LaneCandidate struct {
	ID        string
	Artifact  domain.ArtifactIdentity
	Kind      JobKind
	OldestAt  time.Time
	ReadyAt   time.Time
	CreatedAt time.Time
	Priority  PriorityClass
	MaxWait   time.Duration
	Payload   any
}

type SourceLane struct {
	Artifact          domain.ArtifactIdentity
	SnapshotWeight    int
	DetailWeight      int
	MaintenanceWeight int
	MaxExpeditedBurst int
	categoryCursor    int
	expeditedBurst    int
}

func NewSourceLane(artifact domain.ArtifactIdentity) *SourceLane {
	return &SourceLane{
		Artifact:          artifact,
		SnapshotWeight:    4,
		DetailWeight:      2,
		MaintenanceWeight: 1,
		MaxExpeditedBurst: 2,
	}
}

func (lane *SourceLane) SetWeights(snapshot, detail, maintenance int) {
	if snapshot > 0 {
		lane.SnapshotWeight = snapshot
	}
	if detail > 0 {
		lane.DetailWeight = detail
	}
	if maintenance > 0 {
		lane.MaintenanceWeight = maintenance
	}
}

func (lane *SourceLane) SetMaxExpeditedBurst(value int) {
	if value > 0 {
		lane.MaxExpeditedBurst = value
	}
}

func (lane *SourceLane) Schedule() []JobKind {
	if lane == nil {
		return nil
	}
	schedule := make([]JobKind, 0, lane.SnapshotWeight+lane.DetailWeight+lane.MaintenanceWeight)
	for index := 0; index < lane.SnapshotWeight; index++ {
		schedule = append(schedule, JobAccountSnapshotSlice)
	}
	for index := 0; index < lane.DetailWeight; index++ {
		schedule = append(schedule, JobComicDetailBatch)
	}
	for index := 0; index < lane.MaintenanceWeight; index++ {
		schedule = append(schedule, JobMaintenance)
	}
	return schedule
}

func (lane *SourceLane) Pick(candidates []LaneCandidate, now time.Time) (LaneCandidate, bool) {
	if lane == nil || len(candidates) == 0 {
		return LaneCandidate{}, false
	}
	schedule := lane.Schedule()
	if len(schedule) == 0 {
		return LaneCandidate{}, false
	}
	eligible := make([]LaneCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.Artifact != lane.Artifact || (!candidate.ReadyAt.IsZero() && candidate.ReadyAt.After(now)) {
			continue
		}
		eligible = append(eligible, candidate)
	}
	if len(eligible) == 0 {
		return LaneCandidate{}, false
	}
	aged := make([]LaneCandidate, 0)
	for _, candidate := range eligible {
		maxWait := candidate.MaxWait
		if maxWait <= 0 {
			maxWait = 15 * time.Minute
		}
		oldest := candidate.OldestAt
		if oldest.IsZero() {
			oldest = candidate.CreatedAt
		}
		if candidate.Priority == PriorityExpedited || (!oldest.IsZero() && !now.Before(oldest.Add(maxWait))) {
			aged = append(aged, candidate)
		}
	}
	if len(aged) > 0 && lane.expeditedBurst < lane.MaxExpeditedBurst {
		sortCandidates(aged)
		lane.expeditedBurst++
		return aged[0], true
	}
	ordinary := eligible
	if len(aged) > 0 && lane.expeditedBurst >= lane.MaxExpeditedBurst {
		agedIDs := make(map[string]struct{}, len(aged))
		for _, candidate := range aged {
			agedIDs[candidate.ID] = struct{}{}
		}
		ordinary = make([]LaneCandidate, 0, len(eligible))
		for _, candidate := range eligible {
			if _, isAged := agedIDs[candidate.ID]; !isAged {
				ordinary = append(ordinary, candidate)
			}
		}
		if len(ordinary) == 0 {
			ordinary = eligible
		}
	}
	for offset := 0; offset < len(schedule); offset++ {
		kind := schedule[(lane.categoryCursor+offset)%len(schedule)]
		sameKind := make([]LaneCandidate, 0)
		for _, candidate := range ordinary {
			if candidate.Kind == kind {
				sameKind = append(sameKind, candidate)
			}
		}
		if len(sameKind) == 0 {
			continue
		}
		sortCandidates(sameKind)
		lane.categoryCursor = (lane.categoryCursor + offset + 1) % len(schedule)
		lane.expeditedBurst = 0
		return sameKind[0], true
	}
	return LaneCandidate{}, false
}

func (lane *SourceLane) ExpeditedBurst() int {
	if lane == nil {
		return 0
	}
	return lane.expeditedBurst
}

func sortCandidates(candidates []LaneCandidate) {
	sort.SliceStable(candidates, func(i, j int) bool {
		left := candidates[i].OldestAt
		right := candidates[j].OldestAt
		if left.IsZero() {
			left = candidates[i].CreatedAt
		}
		if right.IsZero() {
			right = candidates[j].CreatedAt
		}
		if left.Equal(right) {
			return candidates[i].ID < candidates[j].ID
		}
		if left.IsZero() {
			return false
		}
		if right.IsZero() {
			return true
		}
		return left.Before(right)
	})
}
