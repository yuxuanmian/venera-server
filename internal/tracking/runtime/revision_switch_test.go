package runtime

import (
	"errors"
	"testing"
	"time"

	"venera-server/internal/tracking/domain"
)

const runtimeRevisionA = "0123456789abcdef0123456789abcdef01234567"
const runtimeRevisionB = "89abcdef0123456789abcdef0123456789abcdef"

func runtimeAuthority(revision string, generation int64) domain.Authority {
	return domain.Authority{
		CatalogID:      "owner/catalog",
		ActiveRevision: revision,
		Generation:     generation,
		Artifacts:      []domain.ArtifactIdentity{{SourceKey: "manwa", FileName: "manwa.js"}},
	}
}

func TestRevisionSwitchRejectsLateOldGeneration(t *testing.T) {
	runtime, err := New(runtimeAuthority(runtimeRevisionA, 1))
	if err != nil {
		t.Fatal(err)
	}
	artifact := domain.ArtifactIdentity{SourceKey: "manwa", FileName: "manwa.js"}
	oldToken, err := runtime.Capture(artifact)
	if err != nil {
		t.Fatal(err)
	}
	active, err := runtime.Activate(runtimeAuthority(runtimeRevisionB, 0))
	if err != nil {
		t.Fatal(err)
	}
	if active.Generation != 2 || active.ActiveRevision != runtimeRevisionB {
		t.Fatalf("unexpected switched authority: %+v", active)
	}
	if runtime.CanCommit(oldToken) || !errors.Is(runtime.RequireCommit(oldToken), ErrGenerationRejected) {
		t.Fatal("old generation was allowed to commit")
	}
	if runtime.GenerationRejections() != 1 {
		t.Fatalf("generation rejection count = %d, want 1", runtime.GenerationRejections())
	}
	newToken, err := runtime.Capture(artifact)
	if err != nil || !runtime.CanCommit(newToken) {
		t.Fatalf("new generation could not commit: token=%+v err=%v", newToken, err)
	}
}

func TestRevisionSwitchAllowsAnEmptyNewWindowAndCanonicalETag(t *testing.T) {
	authority := runtimeAuthority(runtimeRevisionB, 2)
	now := time.Date(2026, time.September, 2, 8, 15, 30, 123000000, time.UTC)
	snapshot, err := BuildSnapshot(authority, nil, now, 10000)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Observations) != 0 {
		t.Fatal("empty new-revision window was not allowed")
	}
	firstETag, err := snapshot.ETag("user-1", 3)
	if err != nil {
		t.Fatal(err)
	}
	secondETag, err := snapshot.ETag("user-1", 3)
	if err != nil || firstETag != secondETag {
		t.Fatalf("ETag was not deterministic: %q %q %v", firstETag, secondETag, err)
	}
	old := domain.Observation{
		Artifact:          authority.Artifacts[0],
		Revision:          runtimeRevisionA,
		ComicID:           "late",
		ObservedAt:        now.Add(-time.Minute),
		ValidUntil:        now.Add(time.Hour),
		RuntimeGeneration: 1,
	}
	if _, err := BuildSnapshot(authority, []domain.Observation{old}, now, 10000); err == nil {
		t.Fatal("late old-revision observation was accepted")
	}
}
