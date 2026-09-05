package runtime

import (
	"errors"
	"testing"
	"time"

	"venera-server/internal/tracking/domain"
)

func TestReconcileInterestsUsesExactCapabilityAndActiveGeneration(t *testing.T) {
	manwa := domain.ArtifactIdentity{SourceKey: "manwa", FileName: "manwa.js"}
	copyA := domain.ArtifactIdentity{SourceKey: "copy_manga", FileName: "copy_manga.js"}
	copyB := domain.ArtifactIdentity{SourceKey: "copy_manga", FileName: "copy_manga_multi_accounts.js"}
	authority := domain.Authority{
		CatalogID:      "owner/catalog",
		ActiveRevision: runtimeRevisionA,
		Generation:     5,
		Artifacts:      []domain.ArtifactIdentity{manwa},
	}
	clients := []domain.ClientState{
		{
			DeviceID:     "device-a",
			CloudEnabled: true,
			Interests: []domain.Interest{
				{Artifact: manwa, ComicID: "42"},
				{Artifact: copyA, ComicID: "1"},
			},
		},
		{
			DeviceID:     "device-b",
			CloudEnabled: false,
			Interests:    []domain.Interest{{Artifact: manwa, ComicID: "43"}},
		},
		{
			DeviceID:     "device-c",
			CloudEnabled: true,
			Interests:    []domain.Interest{{Artifact: copyB, ComicID: "2"}},
		},
	}
	demands, err := ReconcileInterests(authority, clients)
	if err != nil {
		t.Fatal(err)
	}
	if len(demands) != 1 || demands[0].Artifact != manwa || demands[0].ComicID != "42" ||
		demands[0].Revision != runtimeRevisionA || demands[0].RuntimeGeneration != 5 {
		t.Fatalf("demands = %+v", demands)
	}
}

func TestRuntimeRejectsOldObservationBeforePublish(t *testing.T) {
	artifact := domain.ArtifactIdentity{SourceKey: "manwa", FileName: "manwa.js"}
	runtimeState, err := New(domain.Authority{
		CatalogID:      "owner/catalog",
		ActiveRevision: runtimeRevisionA,
		Generation:     1,
		Artifacts:      []domain.ArtifactIdentity{artifact},
	})
	if err != nil {
		t.Fatal(err)
	}
	token, err := runtimeState.Capture(artifact)
	if err != nil {
		t.Fatal(err)
	}
	_, err = runtimeState.Activate(domain.Authority{
		CatalogID:      "owner/catalog",
		ActiveRevision: runtimeRevisionB,
		Artifacts:      []domain.ArtifactIdentity{artifact},
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.September, 3, 10, 0, 0, 0, time.UTC)
	observation := domain.Observation{
		Artifact:          artifact,
		Revision:          runtimeRevisionA,
		ComicID:           "42",
		ObservedAt:        now,
		ValidUntil:        now.Add(time.Hour),
		RuntimeGeneration: token.Generation,
		FavoriteUpdate: domain.FavoriteUpdate{
			State: &domain.UpdateState{LatestChapterID: stringPointer("chapter-42")},
		},
	}
	if err := runtimeState.ValidateObservation(token, observation, now); !errors.Is(err, ErrGenerationRejected) {
		t.Fatalf("late observation error = %v", err)
	}
}

func stringPointer(value string) *string { return &value }
