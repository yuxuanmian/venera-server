package v2sync

import (
	"context"
	"errors"
	"testing"
	"time"

	"venera-server/internal/v2scan"
	"venera-server/internal/v2store"
)

func addDetailSyncInterest(t *testing.T, fixture *syncFixture, artifactID, accountID, comicID, scope string) {
	t.Helper()
	now := fixture.now.Format(time.RFC3339Nano)
	err := fixture.db.WriteTx(context.Background(), func(tx *v2store.Tx) error {
		_, err := tx.ExecContext(context.Background(), `
			INSERT INTO tracking_interests(
				tracking_interest_id, artifact_id, comic_id, source_account_id, origin_kind, origin_key,
				visibility_scope, variant_key, state, title, revision, created_at, updated_at
			) VALUES(?, ?, ?, ?, 'remoteSnapshot', ?, ?, '', 'active', ?, 1, ?, ?)`,
			"detail-interest-"+comicID, artifactID, comicID, accountID, accountID, scope, "Detail "+comicID, now, now)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestDetailPublisherPublishesAndProjectsSingleObservation(t *testing.T) {
	fixture := newSyncFixture(t)
	releaseID := seedSyncRelease(t, fixture, "detail", "detail-account-v1")
	clientID, accountID := addSyncAccount(t, fixture, "detail", releaseID, "detail-owner", "manwa:level:2")
	addSyncClaim(t, fixture, clientID, accountID, "detail")
	addDetailSyncInterest(t, fixture, "detail", accountID, "comic-detail", "manwa:level:2")

	ctx := context.Background()
	planner := v2scan.NewPlanner(fixture.repo, v2scan.PlannerOptions{
		Clock: fixture.db.Now,
		DetailCapabilities: []v2scan.DetailCapability{{
			ArtifactID: "detail", ObservationContractID: "observation-detail", Enabled: true,
		}},
	})
	if _, err := planner.Plan(ctx); err != nil {
		t.Fatal(err)
	}
	demand, err := v2scan.NewDemandRepository(fixture.repo).GetDemandByKey(ctx, v2scan.ComicDetailDemandKey("detail", "comic-detail", "manwa:level:2", "", "observation-detail"))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := v2scan.DecodeDetailOutput([]byte(`{
        "comicId":"comic-detail",
        "summary":{"title":"Comic detail","cover":null,"subtitle":null,"chapterCount":1,"recentChapters":[]},
        "contentObservation":{"visibilityScope":"manwa:level:2","updateTime":"2026-08-28T00:00:00Z","chapterCount":1,"recentChapters":[],"markerEvidence":null},
        "sessionPatch":null
    }`), "comic-detail")
	if err != nil {
		t.Fatal(err)
	}
	before := syncCount(t, fixture, `SELECT high_change_seq FROM client_change_watermarks WHERE client_id = ?`, clientID)
	result, err := NewDetailPublisher(fixture.repo).Publish(ctx, PublishDetailRequest{
		DemandID: demand.ID, ArtifactID: "detail", ComicID: "comic-detail", VisibilityScope: "manwa:level:2",
		VariantKey: "", ObservationContractID: "observation-detail", SourceAccountID: accountID, PackageReleaseID: releaseID,
		ExpectedGeneration: demand.ExecutionGeneration, ExpectedSessionEpoch: 1, FreshUntil: fixture.now.Add(time.Hour), Result: decoded,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.ContentObservationID == "" || result.ObservationRevision != 1 || result.ClientChanges == 0 {
		t.Fatalf("detail publication result = %+v", result)
	}
	if count := syncCount(t, fixture, `SELECT COUNT(*) FROM content_observations WHERE artifact_id = ? AND comic_id = ?`, "detail", "comic-detail"); count != 1 {
		t.Fatalf("detail observations = %d", count)
	}
	if after := syncCount(t, fixture, `SELECT high_change_seq FROM client_change_watermarks WHERE client_id = ?`, clientID); after <= before {
		t.Fatalf("detail publication did not project a change: before=%d after=%d", before, after)
	}
}

func TestDetailPublisherDiscardsStaleGenerationWithoutWritingObservation(t *testing.T) {
	fixture := newSyncFixture(t)
	releaseID := seedSyncRelease(t, fixture, "detail-stale", "detail-stale-account-v1")
	_, accountID := addSyncAccount(t, fixture, "detail-stale", releaseID, "detail-stale-owner", "manwa:level:2")
	addDetailSyncInterest(t, fixture, "detail-stale", accountID, "comic-stale", "manwa:level:2")
	ctx := context.Background()
	planner := v2scan.NewPlanner(fixture.repo, v2scan.PlannerOptions{
		Clock: fixture.db.Now,
		DetailCapabilities: []v2scan.DetailCapability{{
			ArtifactID: "detail-stale", ObservationContractID: "observation-detail-stale", Enabled: true,
		}},
	})
	if _, err := planner.Plan(ctx); err != nil {
		t.Fatal(err)
	}
	demand, err := v2scan.NewDemandRepository(fixture.repo).GetDemandByKey(ctx, v2scan.ComicDetailDemandKey("detail-stale", "comic-stale", "manwa:level:2", "", "observation-detail-stale"))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := v2scan.DecodeDetailOutput([]byte(`{"comicId":"comic-stale","summary":null,"contentObservation":{"visibilityScope":"manwa:level:2","updateTime":null,"chapterCount":1,"recentChapters":[],"markerEvidence":null},"sessionPatch":null}`), "comic-stale")
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewDetailPublisher(fixture.repo).Publish(ctx, PublishDetailRequest{
		DemandID: demand.ID, ArtifactID: "detail-stale", ComicID: "comic-stale", VisibilityScope: "manwa:level:2",
		ObservationContractID: "observation-detail-stale", SourceAccountID: accountID, PackageReleaseID: releaseID,
		ExpectedGeneration: demand.ExecutionGeneration + 1, ExpectedSessionEpoch: 1, Result: decoded,
	})
	if !errors.Is(err, ErrDetailPublishStale) {
		t.Fatalf("stale detail publication error = %v", err)
	}
	if count := syncCount(t, fixture, `SELECT COUNT(*) FROM content_observations WHERE artifact_id = ?`, "detail-stale"); count != 0 {
		t.Fatalf("stale detail wrote %d observations", count)
	}
}
