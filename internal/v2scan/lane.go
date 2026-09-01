package v2scan

import (
	"sort"
	"time"
)

type LaneCandidate struct {
	Job      Job
	OldestAt time.Time
	Priority PriorityClass
	ReadyAt  time.Time
	MaxWait  time.Duration
}

type SourceLane struct {
	ArtifactID        string
	SnapshotWeight    int
	DetailWeight      int
	MaintenanceWeight int
	MaxExpeditedBurst int
	categoryCursor    int
	expeditedBurst    int
}

func NewSourceLane(artifactID string) *SourceLane {
	return &SourceLane{
		ArtifactID: artifactID, SnapshotWeight: 4, DetailWeight: 2,
		MaintenanceWeight: 1, MaxExpeditedBurst: 2,
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
	schedule := make([]JobKind, 0, lane.SnapshotWeight+lane.DetailWeight+lane.MaintenanceWeight)
	for i := 0; i < lane.SnapshotWeight; i++ {
		schedule = append(schedule, JobAccountSnapshotSlice)
	}
	for i := 0; i < lane.DetailWeight; i++ {
		schedule = append(schedule, JobComicDetailBatch)
	}
	for i := 0; i < lane.MaintenanceWeight; i++ {
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
		if lane.ArtifactID != "" && candidate.Job.ArtifactID != lane.ArtifactID {
			continue
		}
		if !candidate.ReadyAt.IsZero() && candidate.ReadyAt.After(now) {
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
			oldest = candidate.Job.CreatedAt
		}
		if candidate.Priority == PriorityExpedited || (!oldest.IsZero() && !now.Before(oldest.Add(maxWait))) {
			aged = append(aged, candidate)
		}
	}
	if len(aged) > 0 && lane.expeditedBurst < lane.MaxExpeditedBurst {
		sort.SliceStable(aged, func(i, j int) bool {
			if aged[i].OldestAt.Equal(aged[j].OldestAt) {
				return aged[i].Job.ID < aged[j].Job.ID
			}
			return aged[i].OldestAt.Before(aged[j].OldestAt)
		})
		lane.expeditedBurst++
		return aged[0], true
	}
	ordinary := eligible
	if len(aged) > 0 && lane.expeditedBurst >= lane.MaxExpeditedBurst {
		agedIDs := make(map[string]struct{}, len(aged))
		for _, candidate := range aged {
			agedIDs[candidate.Job.ID] = struct{}{}
		}
		ordinary = make([]LaneCandidate, 0, len(eligible))
		for _, candidate := range eligible {
			if _, isAged := agedIDs[candidate.Job.ID]; !isAged {
				ordinary = append(ordinary, candidate)
			}
		}
		if len(ordinary) == 0 {
			ordinary = eligible
		}
	}

	for offset := 0; offset < len(schedule); offset++ {
		kind := schedule[(lane.categoryCursor+offset)%len(schedule)]
		var sameKind []LaneCandidate
		for _, candidate := range ordinary {
			if candidate.Job.Kind == kind {
				sameKind = append(sameKind, candidate)
			}
		}
		if len(sameKind) == 0 {
			continue
		}
		sort.SliceStable(sameKind, func(i, j int) bool {
			if sameKind[i].OldestAt.Equal(sameKind[j].OldestAt) {
				return sameKind[i].Job.ID < sameKind[j].Job.ID
			}
			return sameKind[i].OldestAt.Before(sameKind[j].OldestAt)
		})
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
