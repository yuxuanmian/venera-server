package store

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"venera-server/internal/tracking/domain"

	_ "modernc.org/sqlite"
)

const (
	testRevisionA = "0123456789abcdef0123456789abcdef01234567"
	testRevisionB = "89abcdef0123456789abcdef0123456789abcdef"
)

func newTestRepository(t *testing.T, limit ...int) (*Repository, *sql.DB) {
	t.Helper()
	db, err := sql.Open("sqlite", fmt.Sprintf("file:tracking-store-%s?mode=memory&cache=shared", t.Name()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	repository, err := NewRepository(db, limit...)
	if err != nil {
		t.Fatal(err)
	}
	return repository, db
}

func testAuthority(revision string, generation int64) domain.Authority {
	return domain.Authority{
		CatalogID:      "owner/catalog",
		ActiveRevision: revision,
		Generation:     generation,
		Artifacts: []domain.ArtifactIdentity{
			{SourceKey: "copy_manga", FileName: "copy_manga.js"},
			{SourceKey: "manwa", FileName: "manwa.js"},
		},
	}
}

func testObservation(userID string, artifact domain.ArtifactIdentity, revision string, generation int64, comicID string) domain.Observation {
	now := time.Now().UTC()
	latest := "chapter-" + comicID
	return domain.Observation{
		UserID:            userID,
		Artifact:          artifact,
		Revision:          revision,
		ComicID:           comicID,
		ObservedAt:        now.Add(-time.Minute),
		ValidUntil:        now.Add(time.Hour),
		RuntimeGeneration: generation,
		FavoriteUpdate: domain.FavoriteUpdate{
			State: &domain.UpdateState{LatestChapterID: &latest},
		},
	}
}

func TestReplaceClientStateIsCanonicalAndIdempotent(t *testing.T) {
	repository, _ := newTestRepository(t)
	ctx := context.Background()
	authority := testAuthority(testRevisionA, 1)
	input := domain.ClientState{
		DeviceID:     "device-1",
		CloudEnabled: true,
		Interests: []domain.Interest{
			{Artifact: authority.Artifacts[1], ComicID: "2"},
			{Artifact: authority.Artifacts[0], ComicID: "1"},
		},
	}
	first, err := repository.ReplaceClientState(ctx, input, authority)
	if err != nil {
		t.Fatal(err)
	}
	if first.StateRevision != 1 || len(first.Interests) != 2 {
		t.Fatalf("unexpected first state: %+v", first)
	}
	if !interestLess(first.Interests[0], first.Interests[1]) {
		t.Fatalf("interests were not canonicalized: %+v", first.Interests)
	}

	input.Interests = []domain.Interest{input.Interests[1], input.Interests[0]}
	second, err := repository.ReplaceClientState(ctx, input, authority)
	if err != nil {
		t.Fatal(err)
	}
	if second.StateRevision != first.StateRevision || !second.UpdatedAt.Equal(first.UpdatedAt) {
		t.Fatalf("identical replacement was not idempotent: first=%+v second=%+v", first, second)
	}

	input.Interests = append(input.Interests, input.Interests[0])
	if _, err := repository.ReplaceClientState(ctx, input, authority); err == nil {
		t.Fatal("duplicate interest was accepted")
	}
	input.Interests = []domain.Interest{{
		Artifact: domain.ArtifactIdentity{SourceKey: "copy_manga", FileName: "other.js"},
		ComicID:  "1",
	}}
	if _, err := repository.ReplaceClientState(ctx, input, authority); err == nil {
		t.Fatal("same-key different-artifact interest was accepted")
	}
}

func TestObservationReplacementUsesExactArtifactAndFence(t *testing.T) {
	repository, db := newTestRepository(t)
	ctx := context.Background()
	authority := testAuthority(testRevisionA, 1)
	activated := time.Now().UTC()
	if err := repository.SetCatalogState(ctx, CatalogState{
		CatalogID:      authority.CatalogID,
		ActiveRevision: authority.ActiveRevision,
		Generation:     authority.Generation,
		ActivatedAt:    activated,
		Digest:         "digest-a",
	}); err != nil {
		t.Fatal(err)
	}
	manwa := authority.Artifacts[1]
	copyManga := authority.Artifacts[0]
	if err := repository.ReplaceObservations(ctx, "user-1", manwa, testRevisionA, 1, []domain.Observation{
		testObservation("user-1", manwa, testRevisionA, 1, "manwa-1"),
	}, authority); err != nil {
		t.Fatal(err)
	}
	if err := repository.PutObservation(ctx, testObservation("user-1", copyManga, testRevisionA, 1, "copy-1"), authority); err != nil {
		t.Fatal(err)
	}
	interests := []domain.Interest{{Artifact: manwa, ComicID: "manwa-1"}}
	observations, err := repository.ListCurrentObservations(ctx, "user-1", interests, authority, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(observations) != 1 || observations[0].Artifact != manwa {
		t.Fatalf("exact artifact filtering failed: %+v", observations)
	}

	old := testObservation("user-1", manwa, testRevisionA, 1, "old")
	old.ValidUntil = time.Now().UTC().Add(-time.Minute)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO tracking_observations
		(user_id, source_key, file_name, comic_id, catalog_revision, observed_at, valid_until, payload_digest, runtime_generation)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		old.UserID, old.Artifact.SourceKey, old.Artifact.FileName, old.ComicID, old.Revision,
		formatTime(old.ObservedAt), formatTime(old.ValidUntil), "old-digest", old.RuntimeGeneration,
	); err != nil {
		t.Fatal(err)
	}
	observations, err = repository.ListCurrentObservations(ctx, "user-1", []domain.Interest{{Artifact: manwa, ComicID: "old"}}, authority, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(observations) != 0 {
		t.Fatalf("stale observation was returned: %+v", observations)
	}

	wrongRevision := testObservation("user-1", manwa, testRevisionB, 2, "late")
	if err := repository.PutObservation(ctx, wrongRevision, authority); err == nil {
		t.Fatal("old/new revision observation was accepted")
	}
	newAuthority := testAuthority(testRevisionB, 2)
	if err := repository.SetCatalogState(ctx, CatalogState{
		CatalogID:      newAuthority.CatalogID,
		ActiveRevision: newAuthority.ActiveRevision,
		Generation:     newAuthority.Generation,
		ActivatedAt:    time.Now().UTC(),
		Digest:         "digest-b",
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.PutObservation(ctx, wrongRevision, newAuthority); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.ListCurrentObservations(ctx, "user-1", []domain.Interest{{Artifact: manwa, ComicID: "late"}}, authority, time.Now().UTC()); err == nil {
		t.Fatal("old authority was allowed after catalog switch")
	}
}

func TestObservationReplacementRejectsDuplicatesAndLimitBeforeDelete(t *testing.T) {
	repository, _ := newTestRepository(t, 1)
	ctx := context.Background()
	authority := testAuthority(testRevisionA, 1)
	if err := repository.SetCatalogState(ctx, CatalogState{
		CatalogID:      authority.CatalogID,
		ActiveRevision: authority.ActiveRevision,
		Generation:     authority.Generation,
		ActivatedAt:    time.Now().UTC(),
		Digest:         "digest",
	}); err != nil {
		t.Fatal(err)
	}
	artifact := authority.Artifacts[1]
	seed := testObservation("user-1", artifact, testRevisionA, 1, "kept")
	if err := repository.ReplaceObservations(ctx, "user-1", artifact, testRevisionA, 1, []domain.Observation{seed}, authority); err != nil {
		t.Fatal(err)
	}
	duplicate := testObservation("user-1", artifact, testRevisionA, 1, "duplicate")
	duplicate.ComicID = seed.ComicID
	if err := repository.ReplaceObservations(ctx, "user-1", artifact, testRevisionA, 1, []domain.Observation{seed, duplicate}, authority); err == nil {
		t.Fatal("duplicate snapshot was accepted")
	}
	tooMany := testObservation("user-1", artifact, testRevisionA, 1, "too-many")
	if err := repository.ReplaceObservations(ctx, "user-1", artifact, testRevisionA, 1, []domain.Observation{seed, tooMany}, authority); err == nil {
		t.Fatal("over-limit snapshot was accepted")
	}
	observations, err := repository.ListCurrentObservations(ctx, "user-1", []domain.Interest{{Artifact: artifact, ComicID: seed.ComicID}}, authority, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(observations) != 1 || observations[0].ComicID != seed.ComicID {
		t.Fatalf("failed replacement changed existing snapshot: %+v", observations)
	}
}
