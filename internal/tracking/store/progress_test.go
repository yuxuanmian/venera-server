package store

import (
	"context"
	"errors"
	"fmt"
	"testing"

	legacyStore "venera-server/internal/store"
	"venera-server/internal/tracking/domain"
)

type progressFixture struct {
	legacy       *legacyStore.Store
	repository   *Repository
	authority    domain.Authority
	artifact     domain.ArtifactIdentity
	sessionEpoch string
	fence        PublicationFence
	interests    []domain.Interest
}

func newProgressFixture(t *testing.T, comicIDs []string) progressFixture {
	t.Helper()
	legacy, err := legacyStore.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = legacy.Close() })

	repository, err := NewRepository(legacy.DB(), 100)
	if err != nil {
		t.Fatal(err)
	}
	authority := testAuthority(testRevisionA, 1)
	artifact := authority.Artifacts[1]
	if err := legacy.UpsertUser("user-1", "UTC", "en"); err != nil {
		t.Fatal(err)
	}
	if err := legacy.UpsertDevice("device-1", "user-1", "token"); err != nil {
		t.Fatal(err)
	}
	const sessionUpdatedAt = "2026-09-05T00:00:00Z"
	if err := legacy.UpsertSourceSession(legacyStore.SourceSession{
		UserID:          "user-1",
		Source:          artifact.SourceKey,
		CookieEncrypted: "encrypted",
		CookieHash:      "cookie-a",
		UpdatedAt:       sessionUpdatedAt,
	}); err != nil {
		t.Fatal(err)
	}
	interests := make([]domain.Interest, 0, len(comicIDs))
	for _, comicID := range comicIDs {
		interests = append(interests, domain.Interest{
			DeviceID: "device-1",
			Artifact: artifact,
			ComicID:  comicID,
		})
	}
	state, err := repository.ReplaceClientState(context.Background(), domain.ClientState{
		DeviceID:     "device-1",
		CloudEnabled: true,
		Interests:    interests,
	}, authority)
	if err != nil {
		t.Fatal(err)
	}
	sessionEpoch := "cookie-a\x00" + sessionUpdatedAt
	fence := PublicationFence{
		SessionEpoch:    sessionEpoch,
		VisibilityScope: "manwa:level:2",
		Demands: []ClientDemandFence{{
			DeviceID:      state.DeviceID,
			StateRevision: state.StateRevision,
			InterestIDs:   append([]string(nil), comicIDs...),
		}},
	}
	if err := repository.RecordAccountScope(
		context.Background(),
		"user-1",
		artifact,
		sessionEpoch,
		fence.VisibilityScope,
	); err != nil {
		t.Fatal(err)
	}
	return progressFixture{
		legacy:       legacy,
		repository:   repository,
		authority:    authority,
		artifact:     artifact,
		sessionEpoch: sessionEpoch,
		fence:        fence,
		interests:    interests,
	}
}

func progressObservations(
	userID string,
	artifact domain.ArtifactIdentity,
	comicIDs []string,
) []domain.Observation {
	result := make([]domain.Observation, 0, len(comicIDs))
	for _, comicID := range comicIDs {
		result = append(result, testObservation(
			userID,
			artifact,
			testRevisionA,
			1,
			comicID,
		))
	}
	return result
}

func intPointer(value int) *int {
	return &value
}

func TestPartialSnapshotSurvivesRepositoryReconstructionAndReplacesCompleteSet(t *testing.T) {
	comicIDs := make([]string, 31)
	for index := range comicIDs {
		comicIDs[index] = fmt.Sprintf("%02d", index+1)
	}
	fixture := newProgressFixture(t, comicIDs)
	firstPage := progressObservations(
		"user-1",
		fixture.artifact,
		comicIDs[:16],
	)
	ctx := context.Background()
	partial := PartialSnapshot{
		UserID:            "user-1",
		Artifact:          fixture.artifact,
		CatalogID:         fixture.authority.CatalogID,
		CatalogRevision:   testRevisionA,
		RuntimeGeneration: 1,
		Fence:             fixture.fence,
		ExpectedTotal:     intPointer(31),
		Checkpoint:        []byte(`{"offset":16,"boundary":"first-page"}`),
		Observations:      firstPage,
	}
	if err := fixture.repository.SavePartialSnapshot(ctx, partial); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewRepository(fixture.legacy.DB(), 100)
	if err != nil {
		t.Fatal(err)
	}
	recovered, found, err := restarted.GetPartialSnapshot(
		ctx,
		"user-1",
		fixture.artifact,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !found || len(recovered.Observations) != 16 {
		t.Fatalf("recovered partial snapshot = found:%v value:%+v", found, recovered)
	}

	allObservations := append(
		append([]domain.Observation(nil), recovered.Observations...),
		progressObservations("user-1", fixture.artifact, comicIDs[16:])...,
	)
	partial.Checkpoint = []byte(`{"offset":31,"boundary":"last-page"}`)
	partial.Observations = allObservations
	if err := restarted.SavePartialSnapshot(ctx, partial); err != nil {
		t.Fatal(err)
	}
	recovered, found, err = restarted.GetPartialSnapshot(
		ctx,
		"user-1",
		fixture.artifact,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !found || len(recovered.Observations) != 31 {
		t.Fatalf("accumulated snapshot = found:%v value:%+v", found, recovered)
	}

	if err := restarted.ReplaceObservationsWithFence(
		ctx,
		"user-1",
		fixture.artifact,
		testRevisionA,
		1,
		recovered.Observations,
		fixture.authority,
		fixture.fence,
	); err != nil {
		t.Fatal(err)
	}
	if _, found, err := restarted.GetPartialSnapshot(ctx, "user-1", fixture.artifact); err != nil {
		t.Fatal(err)
	} else if found {
		t.Fatal("complete replacement left a partial snapshot behind")
	}
	current, err := restarted.ListCurrentObservations(
		ctx,
		"user-1",
		fixture.interests,
		fixture.authority,
		timeNowUTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(current) != 31 {
		t.Fatalf("current observations = %d, want 31", len(current))
	}
}

func TestPublicationFenceRejectsAuthorizationChangesBeforeCommit(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, fixture progressFixture)
	}{
		{
			name: "cloud off",
			mutate: func(t *testing.T, fixture progressFixture) {
				_, err := fixture.repository.ReplaceClientState(
					context.Background(),
					domain.ClientState{DeviceID: "device-1"},
					fixture.authority,
				)
				if err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "interest removed",
			mutate: func(t *testing.T, fixture progressFixture) {
				_, err := fixture.repository.ReplaceClientState(
					context.Background(),
					domain.ClientState{
						DeviceID:     "device-1",
						CloudEnabled: true,
					},
					fixture.authority,
				)
				if err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "session replaced",
			mutate: func(t *testing.T, fixture progressFixture) {
				if err := fixture.legacy.UpsertSourceSession(
					legacyStore.SourceSession{
						UserID:          "user-1",
						Source:          fixture.artifact.SourceKey,
						CookieEncrypted: "new-encrypted",
						CookieHash:      "cookie-b",
						UpdatedAt:       "2026-09-05T00:01:00Z",
					},
				); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "visibility scope changed",
			mutate: func(t *testing.T, fixture progressFixture) {
				if err := fixture.repository.RecordAccountScope(
					context.Background(),
					"user-1",
					fixture.artifact,
					fixture.sessionEpoch,
					"manwa:level:3",
				); err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newProgressFixture(t, []string{"42"})
			ctx := context.Background()
			seed := testObservation(
				"user-1",
				fixture.artifact,
				testRevisionA,
				1,
				"42",
			)
			if err := fixture.repository.ReplaceObservations(
				ctx,
				"user-1",
				fixture.artifact,
				testRevisionA,
				1,
				[]domain.Observation{seed},
				fixture.authority,
			); err != nil {
				t.Fatal(err)
			}
			test.mutate(t, fixture)

			late := testObservation(
				"user-1",
				fixture.artifact,
				testRevisionA,
				1,
				"late",
			)
			err := fixture.repository.ReplaceObservationsWithFence(
				ctx,
				"user-1",
				fixture.artifact,
				testRevisionA,
				1,
				[]domain.Observation{late},
				fixture.authority,
				fixture.fence,
			)
			if !errors.Is(err, ErrAuthorizationMismatch) {
				t.Fatalf("late publication error = %v", err)
			}
			current, err := fixture.repository.ListCurrentObservations(
				ctx,
				"user-1",
				[]domain.Interest{fixture.interests[0]},
				fixture.authority,
				timeNowUTC(),
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(current) != 1 || current[0].ComicID != "42" {
				t.Fatalf("old snapshot was replaced after rejection: %+v", current)
			}
		})
	}
}
