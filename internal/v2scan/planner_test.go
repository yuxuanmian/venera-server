package v2scan

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"venera-server/internal/v2crypto"
	"venera-server/internal/v2store"
)

type scanFixture struct {
	db   *v2store.DB
	repo *v2store.Repository
	now  time.Time
}

func newScanFixture(t *testing.T) *scanFixture {
	t.Helper()
	fixture := &scanFixture{now: time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)}
	root := bytes.Repeat([]byte{0x73}, v2crypto.RootSecretSize)
	keys, err := v2crypto.DeriveKeys(root)
	if err != nil {
		t.Fatal(err)
	}
	db, err := v2store.OpenWithOptions(filepath.Join(t.TempDir(), "scan.db"), v2store.Options{Clock: func() time.Time { return fixture.now }})
	if err != nil {
		t.Fatal(err)
	}
	fixture.db = db
	fixture.repo = v2store.NewRepository(db, keys)
	t.Cleanup(func() { _ = db.Close() })
	return fixture
}

func seedScanRelease(t *testing.T, fixture *scanFixture, artifactID, contractID, packageJSON string) string {
	t.Helper()
	if err := fixture.repo.UpsertSourceArtifact(context.Background(), v2store.SourceArtifactInput{ArtifactID: artifactID, SourceKey: artifactID, CatalogID: "catalog-scan", ManagedState: "active"}); err != nil {
		t.Fatal(err)
	}
	releaseID := "release-" + artifactID
	if err := fixture.repo.UpsertSourcePackageRelease(context.Background(), v2store.SourcePackageReleaseInput{
		PackageReleaseID: releaseID, ArtifactID: artifactID, CatalogID: "catalog-scan", CatalogSequence: 1,
		CoreHash: "core-" + artifactID, ScanningExtensionHash: "scan-" + artifactID,
		ObservationContractID: contractID, AccountObservationContractID: contractID + "-account",
		AccountProbeContractID: contractID + "-probe", MarkerSchemesJSON: "[\"marker-v1\"]",
		SessionExportProfileID: "cookie-v1", PackageJSON: packageJSON, State: "active",
	}); err != nil {
		t.Fatal(err)
	}
	return releaseID
}

func addScanAccount(t *testing.T, fixture *scanFixture, artifactID, releaseID, identity, scope string) (string, string) {
	t.Helper()
	ctx := context.Background()
	code, err := fixture.repo.CreateEnrollmentCode(ctx, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	token := "token-" + identity + "-" + artifactID
	claimed, err := fixture.repo.ClaimClientEnrollment(ctx, v2store.ClaimClientEnrollmentRequest{
		PendingClientID:      "pending-" + identity + "-" + artifactID,
		TokenDigest:          v2crypto.CredentialDigest(fixture.repo.Keys().CredentialHMAC, token),
		EnrollmentCodeDigest: fixture.repo.EnrollmentCodeDigest(code.Code), DisplayName: identity,
		Platform: "test", AppVersion: "1", IdempotencyKey: "claim-" + identity + "-" + artifactID,
	})
	if err != nil {
		t.Fatal(err)
	}
	sessionPlaintext := []byte(`{"cookies":[{"name":"session","value":"` + identity + `"}]}`)
	sessionEnvelope, err := v2crypto.SealSession(fixture.repo.Keys().SessionAEAD, sessionPlaintext, []byte("candidate:"+string(claimed.Client.ID)+":"+artifactID))
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := fixture.repo.StageSessionCandidate(ctx, v2store.StageSessionCandidateRequest{
		ClientID: string(claimed.Client.ID), ArtifactID: artifactID, PackageReleaseID: releaseID,
		ExportProfileID: "cookie-v1", SessionEnvelope: sessionEnvelope,
		SessionDigest: v2crypto.Digest(fixture.repo.Keys().CredentialHMAC, "session-candidate-v1\x00"+string(sessionPlaintext)),
		TTL:           time.Hour, IdempotencyKey: "stage-" + identity + "-" + artifactID,
	})
	if err != nil {
		t.Fatal(err)
	}
	identityCiphertext, err := v2crypto.SealSession(fixture.repo.Keys().SessionAEAD, []byte(identity), []byte("candidate-identity:"+string(claimed.Client.ID)+":"+candidate.ID))
	if err != nil {
		t.Fatal(err)
	}
	completed, err := fixture.repo.CompleteCandidateProbe(ctx, v2store.CompleteCandidateProbeRequest{
		ClientID: string(claimed.Client.ID), CandidateID: candidate.ID, ExpectedRevision: candidate.Revision,
		IdentityScheme: "test-identity-v1", IdentityCiphertext: identityCiphertext,
		IdentityDigest:  v2crypto.IdentityDigest(fixture.repo.Keys().IdentityHMAC, artifactID, "test-identity-v1", identity),
		IdentityDisplay: identity, AttributesJSON: `{"accountLevel":2}`, VisibilityScope: scope,
		ScopeFreshUntil: fixture.now.Add(time.Hour), IdempotencyKey: "probe-" + identity + "-" + artifactID,
	})
	if err != nil {
		t.Fatal(err)
	}
	activated, err := fixture.repo.ActivateOrLinkSourceAccount(ctx, v2store.ActivateSourceAccountRequest{
		ClientID: string(claimed.Client.ID), CandidateID: completed.ID, ExpectedRevision: completed.Revision,
		IdempotencyKey: "activate-" + identity + "-" + artifactID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(claimed.Client.ID), string(activated.SourceAccount.ID)
}

func addActiveClaim(t *testing.T, fixture *scanFixture, clientID, accountID, artifactID string) {
	t.Helper()
	err := fixture.db.WriteTx(context.Background(), func(tx *v2store.Tx) error {
		_, err := tx.ExecContext(context.Background(), `INSERT INTO client_cloud_claims(client_id, source_account_id, artifact_id, state, revision, activated_at, updated_at) VALUES(?, ?, ?, 'active', 1, ?, ?)`, clientID, accountID, artifactID, fixture.now.Format(time.RFC3339Nano), fixture.now.Format(time.RFC3339Nano))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func addScanClientForAccount(t *testing.T, fixture *scanFixture, identity string) string {
	t.Helper()
	ctx := context.Background()
	code, err := fixture.repo.CreateEnrollmentCode(ctx, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	token := "token-" + identity
	claimed, err := fixture.repo.ClaimClientEnrollment(ctx, v2store.ClaimClientEnrollmentRequest{
		PendingClientID:      "pending-" + identity,
		TokenDigest:          v2crypto.CredentialDigest(fixture.repo.Keys().CredentialHMAC, token),
		EnrollmentCodeDigest: fixture.repo.EnrollmentCodeDigest(code.Code), DisplayName: identity,
		Platform: "test", AppVersion: "1", IdempotencyKey: "claim-" + identity,
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(claimed.Client.ID)
}

func addTrackingInterest(t *testing.T, fixture *scanFixture, id, artifactID, comicID, scope, variant string, sourceAccountID string) {
	t.Helper()
	originKind := "remoteSnapshot"
	if sourceAccountID == "" {
		originKind = "localUpload"
	}
	err := fixture.db.WriteTx(context.Background(), func(tx *v2store.Tx) error {
		_, err := tx.ExecContext(context.Background(), `INSERT INTO tracking_interests(tracking_interest_id, artifact_id, comic_id, source_account_id, origin_kind, origin_key, visibility_scope, variant_key, state, title, revision, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, 1, ?, ?)`, id, artifactID, comicID, nullableScanString(sourceAccountID), originKind, id, scope, variant, comicID, fixture.now.Format(time.RFC3339Nano), fixture.now.Format(time.RFC3339Nano))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func nullableScanString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func TestPlannerDeduplicatesSharedAccountAndTracksPreparationEligibility(t *testing.T) {
	fixture := newScanFixture(t)
	releaseID := seedScanRelease(t, fixture, "manwa", "manwa-observation-v1", `{"scanning":{"operations":["scanFavoriteSnapshotSlice"]}}`)
	accountClient, accountID := addScanAccount(t, fixture, "manwa", releaseID, "shared", "manwa:level:2")
	addActiveClaim(t, fixture, accountClient, accountID, "manwa")
	for i := 0; i < 10; i++ {
		clientID := addScanClientForAccount(t, fixture, "shared-client-"+string(rune('a'+i)))
		addActiveClaim(t, fixture, clientID, accountID, "manwa")
	}
	planner := NewPlanner(fixture.repo, PlannerOptions{Clock: func() time.Time { return fixture.now }})
	if _, err := planner.Plan(context.Background()); err != nil {
		t.Fatal(err)
	}
	demands, err := NewDemandRepository(fixture.repo).ListDemands(context.Background(), "manwa", DemandAccountSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	active := 0
	for _, demand := range demands {
		if demand.State == DemandActive {
			active++
		}
	}
	if active != 1 {
		t.Fatalf("shared account demands = %+v", demands)
	}
	if demands[0].Key != AccountSnapshotDemandKey("manwa", accountID) {
		t.Fatalf("demand key = %q", demands[0].Key)
	}
}

func TestPlannerMergesExactDetailScopeAndKeepsManwaDetailDisabled(t *testing.T) {
	fixture := newScanFixture(t)
	levelRelease := seedScanRelease(t, fixture, "level_detail", "level-detail-v1", `{"detailEnabled":true}`)
	_, levelAccountA := addScanAccount(t, fixture, "level_detail", levelRelease, "lv2-a", "level:2")
	_, levelAccountB := addScanAccount(t, fixture, "level_detail", levelRelease, "lv2-b", "level:2")
	addTrackingInterest(t, fixture, "lv2-a-interest", "level_detail", "comic-1", "level:2", "", levelAccountA)
	addTrackingInterest(t, fixture, "lv2-b-interest", "level_detail", "comic-1", "level:2", "", levelAccountB)
	addTrackingInterest(t, fixture, "lv3-interest", "level_detail", "comic-1", "level:3", "", levelAccountB)
	seedScanRelease(t, fixture, "manwa", "manwa-observation-v1", `{"scanning":{"operations":["scanFavoriteSnapshotSlice"]}}`)
	addTrackingInterest(t, fixture, "manwa-interest", "manwa", "comic-1", "manwa:level:2", "", "")
	planner := NewPlanner(fixture.repo, PlannerOptions{Clock: func() time.Time { return fixture.now }, DetailCapabilities: []DetailCapability{{ArtifactID: "level_detail", ObservationContractID: "level-detail-v1", Enabled: true}}})
	if _, err := planner.Plan(context.Background()); err != nil {
		t.Fatal(err)
	}
	demands, err := NewDemandRepository(fixture.repo).ListDemands(context.Background(), "level_detail", DemandComicDetail)
	if err != nil {
		t.Fatal(err)
	}
	if len(demands) != 2 {
		t.Fatalf("level detail demands = %+v", demands)
	}
	manwaDemands, err := NewDemandRepository(fixture.repo).ListDemands(context.Background(), "manwa", DemandComicDetail)
	if err != nil {
		t.Fatal(err)
	}
	if len(manwaDemands) != 0 {
		t.Fatalf("Manwa unexpectedly created detail demand = %+v", manwaDemands)
	}
}

func TestPlannerMergesAuthenticatedPicacgDetailsAndBlocksAnonymousExecution(t *testing.T) {
	fixture := newScanFixture(t)
	releaseID := seedScanRelease(t, fixture, "picacg", "picacg-detail-v1", `{"detailEnabled":true}`)
	_, accountA := addScanAccount(t, fixture, "picacg", releaseID, "picacg-a", "picacg:authenticated")
	_, accountB := addScanAccount(t, fixture, "picacg", releaseID, "picacg-b", "picacg:authenticated")
	addTrackingInterest(t, fixture, "picacg-a-interest", "picacg", "comic-1", "picacg:authenticated", "", accountA)
	addTrackingInterest(t, fixture, "picacg-b-interest", "picacg", "comic-1", "picacg:authenticated", "", accountB)
	planner := NewPlanner(fixture.repo, PlannerOptions{Clock: func() time.Time { return fixture.now }})
	if _, err := planner.Plan(context.Background()); err != nil {
		t.Fatal(err)
	}
	demands, err := NewDemandRepository(fixture.repo).ListDemands(context.Background(), "picacg", DemandComicDetail)
	if err != nil {
		t.Fatal(err)
	}
	if len(demands) != 1 || demands[0].State != DemandActive {
		t.Fatalf("Picacg detail demands = %+v", demands)
	}
	jobRepo := NewDemandRepository(fixture.repo)
	job, err := jobRepo.MaterializeJob(context.Background(), MaterializeJobRequest{DemandID: demands[0].ID, Kind: JobComicDetailBatch, PayloadJSON: `{}`})
	if err != nil {
		t.Fatal(err)
	}
	if job.ExecutorSourceAccountID != accountA && job.ExecutorSourceAccountID != accountB {
		t.Fatalf("Picacg executor = %q", job.ExecutorSourceAccountID)
	}
	jobAgain, err := jobRepo.MaterializeJob(context.Background(), MaterializeJobRequest{DemandID: demands[0].ID, Kind: JobComicDetailBatch, PayloadJSON: `{"changed":true}`})
	if err != nil {
		t.Fatal(err)
	}
	if jobAgain.ID != job.ID {
		t.Fatalf("Picacg detail was not deduplicated: %q / %q", job.ID, jobAgain.ID)
	}

	seedScanRelease(t, fixture, "picacg-anonymous", "picacg-anon-v1", `{"detailEnabled":true}`)
	addTrackingInterest(t, fixture, "picacg-anon-interest", "picacg-anonymous", "comic-1", "picacg:authenticated", "", "")
	if _, err := planner.Plan(context.Background()); err != nil {
		t.Fatal(err)
	}
	blocked, err := jobRepo.ListDemands(context.Background(), "picacg-anonymous", DemandComicDetail)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocked) != 1 || blocked[0].State != DemandBlocked || blocked[0].LastErrorCode != "no_authenticated_executor" {
		t.Fatalf("anonymous detail demand = %+v", blocked)
	}
	if _, err := jobRepo.MaterializeJob(context.Background(), MaterializeJobRequest{DemandID: blocked[0].ID, Kind: JobComicDetailBatch, PayloadJSON: `{}`}); err != ErrDemandNotRunnable {
		t.Fatalf("anonymous detail materialization error = %v", err)
	}
}

func TestPlannerPreparationBootstrapsOnlyUntilClaimTakesOver(t *testing.T) {
	fixture := newScanFixture(t)
	releaseID := seedScanRelease(t, fixture, "bootstrap", "bootstrap-detail-v1", `{"detailEnabled":true}`)
	clientID, accountID := addScanAccount(t, fixture, "bootstrap", releaseID, "bootstrap-owner", "scope:1")
	addTrackingInterest(t, fixture, "bootstrap-interest", "bootstrap", "comic-1", "scope:1", "", "")
	now := fixture.now.Format(time.RFC3339Nano)
	expires := fixture.now.Add(time.Hour).Format(time.RFC3339Nano)
	if err := fixture.db.WriteTx(context.Background(), func(tx *v2store.Tx) error {
		_, err := tx.ExecContext(context.Background(), `INSERT INTO cloud_mode_preparations(preparation_id, client_id, source_account_id, artifact_id, package_release_id, state, stage, fixed_session_epoch, fixed_inventory_revision, expires_at, revision, created_at, updated_at) VALUES('prep-bootstrap', ?, ?, 'bootstrap', ?, 'preparing', 'waitSnapshot', 1, 1, ?, 1, ?, ?)`, clientID, accountID, releaseID, expires, now, now)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	planner := NewPlanner(fixture.repo, PlannerOptions{Clock: func() time.Time { return fixture.now }})
	if _, err := planner.Plan(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := syncDemandForScanTest(t, fixture, AccountSnapshotDemandKey("bootstrap", accountID))
	if snapshot.State != DemandActive {
		t.Fatalf("preparation snapshot demand = %+v", snapshot)
	}
	detail, err := NewDemandRepository(fixture.repo).ListDemands(context.Background(), "bootstrap", DemandComicDetail)
	if err != nil {
		t.Fatal(err)
	}
	if len(detail) != 1 || detail[0].State != DemandActive {
		t.Fatalf("preparation detail demand = %+v", detail)
	}
	if count := syncCountForScanTest(t, fixture, `SELECT COUNT(*) FROM client_entity_states WHERE client_id = ?`, clientID); count != 0 {
		t.Fatalf("preparation created ordinary projection = %d", count)
	}
	if err := fixture.db.WriteTx(context.Background(), func(tx *v2store.Tx) error {
		_, err := tx.ExecContext(context.Background(), `UPDATE cloud_mode_preparations SET state = 'expired', updated_at = ? WHERE preparation_id = 'prep-bootstrap'`, now)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := planner.Plan(context.Background()); err != nil {
		t.Fatal(err)
	}
	if expired := syncDemandForScanTest(t, fixture, AccountSnapshotDemandKey("bootstrap", accountID)); expired.State != DemandInactive {
		t.Fatalf("expired preparation demand = %+v", expired)
	}
	addActiveClaim(t, fixture, clientID, accountID, "bootstrap")
	if _, err := planner.Plan(context.Background()); err != nil {
		t.Fatal(err)
	}
	claimed := syncDemandForScanTest(t, fixture, AccountSnapshotDemandKey("bootstrap", accountID))
	if claimed.State != DemandActive || claimed.ID != snapshot.ID {
		t.Fatalf("claim did not take over preparation demand = %+v", claimed)
	}
}

func syncDemandForScanTest(t *testing.T, fixture *scanFixture, key string) Demand {
	t.Helper()
	demand, err := NewDemandRepository(fixture.repo).GetDemandByKey(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	return demand
}

func syncCountForScanTest(t *testing.T, fixture *scanFixture, query string, args ...any) int64 {
	t.Helper()
	var count int64
	if err := fixture.db.SQL().QueryRow(query, args...).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestLaneAgingBurstAndJobLeaseRecoveryDiscardOldEpoch(t *testing.T) {
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	lane := NewSourceLane("artifact")
	lane.SetMaxExpeditedBurst(2)
	candidates := []LaneCandidate{
		{Job: Job{ID: "s1", ArtifactID: "artifact", Kind: JobAccountSnapshotSlice, CreatedAt: now}, OldestAt: now},
		{Job: Job{ID: "s2", ArtifactID: "artifact", Kind: JobAccountSnapshotSlice, CreatedAt: now}, OldestAt: now},
		{Job: Job{ID: "d1", ArtifactID: "artifact", Kind: JobComicDetailBatch, CreatedAt: now}, OldestAt: now},
		{Job: Job{ID: "m1", ArtifactID: "artifact", Kind: JobMaintenance, CreatedAt: now}, OldestAt: now},
	}
	first, ok := lane.Pick(candidates, now)
	if !ok || first.Job.ID != "s1" {
		t.Fatalf("first lane pick = %+v, %v", first, ok)
	}
	aged := []LaneCandidate{
		{Job: Job{ID: "old1", ArtifactID: "artifact", Kind: JobAccountSnapshotSlice, CreatedAt: now.Add(-time.Hour)}, OldestAt: now.Add(-time.Hour), MaxWait: time.Minute},
		{Job: Job{ID: "old2", ArtifactID: "artifact", Kind: JobAccountSnapshotSlice, CreatedAt: now.Add(-time.Hour)}, OldestAt: now.Add(-time.Hour), MaxWait: time.Minute},
		{Job: Job{ID: "normal", ArtifactID: "artifact", Kind: JobComicDetailBatch, CreatedAt: now}, OldestAt: now},
	}
	agedLane := NewSourceLane("artifact")
	for i := 0; i < 2; i++ {
		if _, ok := agedLane.Pick(aged, now); !ok {
			t.Fatal("aged lane did not return a job")
		}
	}
	third, ok := agedLane.Pick(aged, now)
	if !ok || third.Job.ID != "normal" {
		t.Fatalf("third aged pick = %+v, %v", third, ok)
	}

	fixture := newScanFixture(t)
	releaseID := seedScanRelease(t, fixture, "lease-artifact", "lease-observation-v1", `{}`)
	clientID, accountID := addScanAccount(t, fixture, "lease-artifact", releaseID, "lease-user", "scope:1")
	addActiveClaim(t, fixture, clientID, accountID, "lease-artifact")
	planner := NewPlanner(fixture.repo, PlannerOptions{Clock: func() time.Time { return fixture.now }})
	if _, err := planner.Plan(context.Background()); err != nil {
		t.Fatal(err)
	}
	demand, err := NewDemandRepository(fixture.repo).GetDemandByKey(context.Background(), AccountSnapshotDemandKey("lease-artifact", accountID))
	if err != nil {
		t.Fatal(err)
	}
	jobRepo := NewDemandRepository(fixture.repo)
	job, err := jobRepo.MaterializeJob(context.Background(), MaterializeJobRequest{DemandID: demand.ID, Kind: JobAccountSnapshotSlice, PayloadJSON: `{}`})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := jobRepo.LeaseJob(context.Background(), job.ID, "worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	fixture.now = fixture.now.Add(2 * time.Minute)
	recovered, err := jobRepo.RecoverExpiredLeases(context.Background())
	if err != nil || recovered != 1 {
		t.Fatalf("recovered = %d, err=%v", recovered, err)
	}
	released, err := jobRepo.LeaseJob(context.Background(), job.ID, "worker-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.WriteTx(context.Background(), func(tx *v2store.Tx) error {
		_, err := tx.ExecContext(context.Background(), `UPDATE source_accounts SET session_epoch = session_epoch + 1 WHERE source_account_id = ?`, accountID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := jobRepo.CompleteJob(context.Background(), CompleteJobRequest{JobID: released.Job.ID, WorkerID: "worker-b", Outcome: "succeeded", ExpectedEpoch: lease.Job.FixedSessionEpoch}); err != nil {
		t.Fatal(err)
	}
	final, err := jobRepo.GetJob(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != JobCancelled {
		t.Fatalf("stale job state = %s", final.State)
	}
	var outcome string
	if err := fixture.db.SQL().QueryRow(`SELECT outcome FROM scan_attempts WHERE job_id = ? ORDER BY attempt_no DESC LIMIT 1`, job.ID).Scan(&outcome); err != nil {
		t.Fatal(err)
	}
	if outcome != "discarded" {
		t.Fatalf("stale job outcome = %s", outcome)
	}
}

func TestDispatcherRoundRobinsSourceLanesAndLimitsLeases(t *testing.T) {
	fixture := newScanFixture(t)
	releaseA := seedScanRelease(t, fixture, "artifact-a", "artifact-a-v1", `{}`)
	releaseB := seedScanRelease(t, fixture, "artifact-b", "artifact-b-v1", `{}`)
	clientA, accountA := addScanAccount(t, fixture, "artifact-a", releaseA, "dispatcher-a", "scope:1")
	clientB, accountB := addScanAccount(t, fixture, "artifact-b", releaseB, "dispatcher-b", "scope:1")
	addActiveClaim(t, fixture, clientA, accountA, "artifact-a")
	addActiveClaim(t, fixture, clientB, accountB, "artifact-b")
	planner := NewPlanner(fixture.repo, PlannerOptions{Clock: func() time.Time { return fixture.now }})
	if _, err := planner.Plan(context.Background()); err != nil {
		t.Fatal(err)
	}
	jobRepo := NewDemandRepository(fixture.repo)
	for _, account := range []struct {
		artifact string
		account  string
	}{
		{artifact: "artifact-a", account: accountA},
		{artifact: "artifact-b", account: accountB},
	} {
		demand, err := jobRepo.GetDemandByKey(context.Background(), AccountSnapshotDemandKey(account.artifact, account.account))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := jobRepo.MaterializeJob(context.Background(), MaterializeJobRequest{DemandID: demand.ID, Kind: JobAccountSnapshotSlice, PayloadJSON: `{}`}); err != nil {
			t.Fatal(err)
		}
	}
	dispatcher := NewDispatcher(jobRepo, DispatcherOptions{Clock: func() time.Time { return fixture.now }, MaxConcurrent: 1, LeaseDuration: time.Minute, WorkerID: "dispatcher-test"})
	first, err := dispatcher.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.Next(context.Background()); err != ErrDispatcherBusy {
		t.Fatalf("busy dispatcher error = %v", err)
	}
	if err := dispatcher.Complete(context.Background(), CompleteJobRequest{JobID: first.Job.ID, WorkerID: "dispatcher-test", Outcome: "succeeded"}); err != nil {
		t.Fatal(err)
	}
	second, err := dispatcher.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second.Job.ArtifactID == first.Job.ArtifactID {
		t.Fatalf("source lanes were not round-robin: %s then %s", first.Job.ArtifactID, second.Job.ArtifactID)
	}
	if err := dispatcher.Complete(context.Background(), CompleteJobRequest{JobID: second.Job.ID, WorkerID: "dispatcher-test", Outcome: "succeeded"}); err != nil {
		t.Fatal(err)
	}
}
