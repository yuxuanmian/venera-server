package scan

import (
	"errors"
	"testing"
	"time"

	"venera-server/internal/tracking/domain"
)

func TestLaneFairnessAndExpeditedBurst(t *testing.T) {
	now := time.Date(2026, time.September, 3, 10, 0, 0, 0, time.UTC)
	artifact := domain.ArtifactIdentity{SourceKey: "manwa", FileName: "manwa.js"}
	lane := NewSourceLane(artifact)
	lane.SetWeights(1, 1, 1)
	first, ok := lane.Pick([]LaneCandidate{
		{ID: "detail", Artifact: artifact, Kind: JobComicDetailBatch, OldestAt: now},
		{ID: "snapshot", Artifact: artifact, Kind: JobAccountSnapshotSlice, OldestAt: now},
	}, now)
	if !ok || first.ID != "snapshot" {
		t.Fatalf("first lane choice = %+v, %v", first, ok)
	}
	second, ok := lane.Pick([]LaneCandidate{
		{ID: "detail", Artifact: artifact, Kind: JobComicDetailBatch, OldestAt: now},
		{ID: "snapshot", Artifact: artifact, Kind: JobAccountSnapshotSlice, OldestAt: now},
	}, now)
	if !ok || second.ID != "detail" {
		t.Fatalf("second lane choice = %+v, %v", second, ok)
	}
}

func TestDispatcherKeepsExactArtifactsAndGlobalConcurrency(t *testing.T) {
	now := time.Date(2026, time.September, 3, 10, 0, 0, 0, time.UTC)
	manwa := domain.ArtifactIdentity{SourceKey: "manwa", FileName: "manwa.js"}
	copyA := domain.ArtifactIdentity{SourceKey: "copy_manga", FileName: "copy_manga.js"}
	dispatcher := NewDispatcher(DispatcherOptions{Clock: func() time.Time { return now }, MaxConcurrent: 1})
	dispatcher.AddArtifact(manwa)
	dispatcher.AddArtifact(copyA)
	candidate, err := dispatcher.Next([]LaneCandidate{
		{ID: "copy", Artifact: copyA, Kind: JobAccountSnapshotSlice, ReadyAt: now},
	})
	if err != nil || candidate.Artifact != copyA || dispatcher.ActiveLeases() != 1 {
		t.Fatalf("dispatcher selection = %+v, %v, leases=%d", candidate, err, dispatcher.ActiveLeases())
	}
	if _, err := dispatcher.Next([]LaneCandidate{{ID: "manwa", Artifact: manwa, Kind: JobAccountSnapshotSlice}}); !errors.Is(err, ErrDispatcherBusy) {
		t.Fatalf("busy error = %v", err)
	}
	dispatcher.ReleaseLease()
	if dispatcher.ActiveLeases() != 0 {
		t.Fatal("dispatcher lease was not released")
	}
}

func TestDispatcherDoesNotStarveCompetingAccountsInOneArtifactLane(t *testing.T) {
	now := time.Date(2026, time.September, 3, 10, 0, 0, 0, time.UTC)
	artifact := domain.ArtifactIdentity{SourceKey: "manwa", FileName: "manwa.js"}
	dispatcher := NewDispatcher(DispatcherOptions{Clock: func() time.Time { return now }, MaxConcurrent: 1})
	dispatcher.AddArtifact(artifact)
	candidates := []LaneCandidate{
		{ID: "user-a", Artifact: artifact, Kind: JobAccountSnapshotSlice, ReadyAt: now, OldestAt: now},
		{ID: "user-b", Artifact: artifact, Kind: JobAccountSnapshotSlice, ReadyAt: now, OldestAt: now},
	}
	first, err := dispatcher.Next(candidates)
	if err != nil || first.ID != "user-a" {
		t.Fatalf("first competing account = %+v err=%v", first, err)
	}
	dispatcher.ReleaseLease()
	second, err := dispatcher.Next([]LaneCandidate{candidates[1]})
	if err != nil || second.ID != "user-b" {
		t.Fatalf("second competing account = %+v err=%v", second, err)
	}
	dispatcher.ReleaseLease()
}
