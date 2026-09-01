package v2sync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"venera-server/internal/v2crypto"
	"venera-server/internal/v2scan"
	"venera-server/internal/v2store"
)

type syncFixture struct {
	db   *v2store.DB
	repo *v2store.Repository
	now  time.Time
}

func newSyncFixture(t *testing.T) *syncFixture {
	t.Helper()
	fixture := &syncFixture{now: time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)}
	root := bytes.Repeat([]byte{0x74}, v2crypto.RootSecretSize)
	keys, err := v2crypto.DeriveKeys(root)
	if err != nil {
		t.Fatal(err)
	}
	db, err := v2store.OpenWithOptions(filepath.Join(t.TempDir(), "sync.db"), v2store.Options{Clock: func() time.Time { return fixture.now }})
	if err != nil {
		t.Fatal(err)
	}
	fixture.db = db
	fixture.repo = v2store.NewRepository(db, keys)
	t.Cleanup(func() { _ = db.Close() })
	return fixture
}

func seedSyncRelease(t *testing.T, fixture *syncFixture, artifactID, accountContract string) string {
	t.Helper()
	if err := fixture.repo.UpsertSourceArtifact(context.Background(), v2store.SourceArtifactInput{ArtifactID: artifactID, SourceKey: artifactID, CatalogID: "catalog-sync", ManagedState: "active"}); err != nil {
		t.Fatal(err)
	}
	releaseID := "release-" + artifactID
	if err := fixture.repo.UpsertSourcePackageRelease(context.Background(), v2store.SourcePackageReleaseInput{
		PackageReleaseID: releaseID, ArtifactID: artifactID, CatalogID: "catalog-sync", CatalogSequence: 1,
		CoreHash: "core-" + artifactID, ScanningExtensionHash: "scan-" + artifactID,
		ObservationContractID: "observation-" + artifactID, AccountObservationContractID: accountContract,
		AccountProbeContractID: "probe-" + artifactID, MarkerSchemesJSON: `["marker-v1","manwa-list-chapter-id-v1"]`,
		SessionExportProfileID: "cookie-v1", PackageJSON: `{}`, State: "active",
	}); err != nil {
		t.Fatal(err)
	}
	return releaseID
}

func addSyncClient(t *testing.T, fixture *syncFixture, identity string) string {
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

func addSyncAccount(t *testing.T, fixture *syncFixture, artifactID, releaseID, identity, scope string) (string, string) {
	t.Helper()
	ctx := context.Background()
	clientID := addSyncClient(t, fixture, identity)
	// The account owner is the client enrolled above. Create a candidate with
	// the same client and promote it through the normal probe/link transaction.
	// Enrollment codes are deliberately not reused for account activation.
	// The enrollment result above already created the client; retrieve its
	// active identity through a candidate staged for that client.
	sessionPlaintext := []byte(`{"cookies":[{"name":"session","value":"` + identity + `"}]}`)
	sessionEnvelope, err := v2crypto.SealSession(fixture.repo.Keys().SessionAEAD, sessionPlaintext, []byte("candidate:"+clientID+":"+artifactID))
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := fixture.repo.StageSessionCandidate(ctx, v2store.StageSessionCandidateRequest{
		ClientID: clientID, ArtifactID: artifactID, PackageReleaseID: releaseID,
		ExportProfileID: "cookie-v1", SessionEnvelope: sessionEnvelope,
		SessionDigest: v2crypto.Digest(fixture.repo.Keys().CredentialHMAC, "session-candidate-v1\x00"+string(sessionPlaintext)),
		TTL:           time.Hour, IdempotencyKey: "stage-" + identity,
	})
	if err != nil {
		t.Fatal(err)
	}
	identityCiphertext, err := v2crypto.SealSession(fixture.repo.Keys().SessionAEAD, []byte(identity), []byte("candidate-identity:"+clientID+":"+candidate.ID))
	if err != nil {
		t.Fatal(err)
	}
	completed, err := fixture.repo.CompleteCandidateProbe(ctx, v2store.CompleteCandidateProbeRequest{
		ClientID: clientID, CandidateID: candidate.ID, ExpectedRevision: candidate.Revision,
		IdentityScheme: "sync-identity-v1", IdentityCiphertext: identityCiphertext,
		IdentityDigest:  v2crypto.IdentityDigest(fixture.repo.Keys().IdentityHMAC, artifactID, "sync-identity-v1", identity),
		IdentityDisplay: identity, AttributesJSON: `{"accountLevel":2}`, VisibilityScope: scope,
		ScopeFreshUntil: fixture.now.Add(time.Hour), IdempotencyKey: "probe-" + identity,
	})
	if err != nil {
		t.Fatal(err)
	}
	activated, err := fixture.repo.ActivateOrLinkSourceAccount(ctx, v2store.ActivateSourceAccountRequest{
		ClientID: clientID, CandidateID: completed.ID, ExpectedRevision: completed.Revision,
		IdempotencyKey: "activate-" + identity,
	})
	if err != nil {
		t.Fatal(err)
	}
	return clientID, string(activated.SourceAccount.ID)
}

func linkSyncClient(t *testing.T, fixture *syncFixture, clientID, accountID, artifactID string) {
	t.Helper()
	now := fixture.now.Format(time.RFC3339Nano)
	err := fixture.db.WriteTx(context.Background(), func(tx *v2store.Tx) error {
		_, err := tx.ExecContext(context.Background(), `INSERT INTO client_source_links(client_id, source_account_id, artifact_id, state, selected_for_artifact, revision, linked_at, updated_at) VALUES(?, ?, ?, 'linked', 1, 1, ?, ?)`, clientID, accountID, artifactID, now, now)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func addSyncClaim(t *testing.T, fixture *syncFixture, clientID, accountID, artifactID string) {
	t.Helper()
	now := fixture.now.Format(time.RFC3339Nano)
	err := fixture.db.WriteTx(context.Background(), func(tx *v2store.Tx) error {
		_, err := tx.ExecContext(context.Background(), `INSERT INTO client_cloud_claims(client_id, source_account_id, artifact_id, state, revision, activated_at, updated_at) VALUES(?, ?, ?, 'active', 1, ?, ?)`, clientID, accountID, artifactID, now, now)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func syncSnapshotItem(t *testing.T, comicID, scope string, unread bool) v2scan.SnapshotItem {
	t.Helper()
	value, err := json.Marshal(map[string]any{
		"comicId":    comicID,
		"membership": map[string]any{"origin": "remoteSnapshot", "folderId": "0"},
		"summary":    map[string]any{"title": "Title " + comicID, "cover": "https://cover.invalid/" + comicID, "subtitle": nil},
		"contentObservation": map[string]any{
			"visibilityScope": scope,
			"markerEvidence":  map[string]any{"channel": "source-defined", "scheme": "manwa-list-chapter-id-v1", "value": "normal:" + comicID},
			"updateTime":      nil,
		},
		"accountObservation": map[string]any{"sourceUnreadByVariant": map[string]bool{"normal": unread}, "sourceUnread": unread},
	})
	if err != nil {
		t.Fatal(err)
	}
	return v2scan.SnapshotItem{ComicID: comicID, ItemJSON: value}
}

func syncDemand(t *testing.T, fixture *syncFixture, artifactID, accountID string) v2scan.Demand {
	t.Helper()
	demand, err := v2scan.NewDemandRepository(fixture.repo).GetDemandByKey(context.Background(), v2scan.AccountSnapshotDemandKey(artifactID, accountID))
	if err != nil {
		t.Fatal(err)
	}
	return demand
}

func planSyncDemand(t *testing.T, fixture *syncFixture, artifactID, accountID string, nextGeneration bool) v2scan.Demand {
	t.Helper()
	ctx := context.Background()
	demandRepo := v2scan.NewDemandRepository(fixture.repo)
	if nextGeneration {
		current := syncDemand(t, fixture, artifactID, accountID)
		if err := demandRepo.MarkDemandState(ctx, current.ID, v2scan.DemandInactive, "test_next_generation"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := v2scan.NewPlanner(fixture.repo, v2scan.PlannerOptions{Clock: func() time.Time { return fixture.now }}).Plan(ctx); err != nil {
		t.Fatal(err)
	}
	return syncDemand(t, fixture, artifactID, accountID)
}

func runSyncSnapshot(t *testing.T, fixture *syncFixture, artifactID, releaseID, accountID string, generation int64, items []v2scan.SnapshotItem) string {
	t.Helper()
	var epoch int64
	if err := fixture.db.SQL().QueryRow(`SELECT session_epoch FROM source_accounts WHERE source_account_id = ?`, accountID).Scan(&epoch); err != nil {
		t.Fatal(err)
	}
	executor := v2scan.NewSnapshotExecutor(fixture.repo, 0, 0)
	run, err := executor.StartRun(context.Background(), v2scan.SnapshotRunRequest{SourceAccountID: accountID, PackageReleaseID: releaseID, SessionEpoch: epoch, RunGeneration: generation})
	if err != nil {
		t.Fatal(err)
	}
	total := len(items)
	if err := executor.ApplySlice(context.Background(), run.ID, v2scan.SnapshotSliceResult{ExpectedTotal: &total, Items: items, Complete: true, BoundaryDigest: "end"}); err != nil {
		t.Fatal(err)
	}
	completed, err := executor.CompleteRun(context.Background(), run.ID, "end")
	if err != nil {
		t.Fatal(err)
	}
	if completed.State != "complete" {
		t.Fatalf("snapshot state = %s", completed.State)
	}
	return run.ID
}

func publishSyncSnapshot(t *testing.T, fixture *syncFixture, runID, accountID, releaseID string, generation int64) PublishResult {
	t.Helper()
	result, err := NewPublisher(fixture.repo).PublishSnapshot(context.Background(), PublishSnapshotRequest{
		RunID: runID, SourceAccountID: accountID, PackageReleaseID: releaseID,
		ExpectedGeneration: generation, ExpectedSessionEpoch: 1, FreshUntil: fixture.now.Add(3 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func syncCount(t *testing.T, fixture *syncFixture, query string, args ...any) int64 {
	t.Helper()
	var count int64
	if err := fixture.db.SQL().QueryRow(query, args...).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func syncEntityOperation(t *testing.T, fixture *syncFixture, clientID, entityType, entityKey string) string {
	t.Helper()
	keyHash := projectionKeyHash(entityType, entityKey)
	var operation string
	if err := fixture.db.SQL().QueryRow(`SELECT operation FROM client_entity_states WHERE client_id = ? AND entity_type = ? AND key_hash = ?`, clientID, entityType, keyHash).Scan(&operation); err != nil {
		t.Fatal(err)
	}
	return operation
}

func TestPublicationRollsBackWhenAccountObservationContractIsInvalid(t *testing.T) {
	fixture := newSyncFixture(t)
	releaseID := seedSyncRelease(t, fixture, "rollback", "")
	clientID, accountID := addSyncAccount(t, fixture, "rollback", releaseID, "rollback-owner", "manwa:level:2")
	addSyncClaim(t, fixture, clientID, accountID, "rollback")
	demand := planSyncDemand(t, fixture, "rollback", accountID, false)
	runID := runSyncSnapshot(t, fixture, "rollback", releaseID, accountID, demand.ExecutionGeneration, []v2scan.SnapshotItem{syncSnapshotItem(t, "comic-rollback", "manwa:level:2", true)})
	_, err := NewPublisher(fixture.repo).PublishSnapshot(context.Background(), PublishSnapshotRequest{RunID: runID, SourceAccountID: accountID, PackageReleaseID: releaseID, ExpectedGeneration: demand.ExecutionGeneration, ExpectedSessionEpoch: 1, FreshUntil: fixture.now.Add(time.Hour)})
	if !errors.Is(err, ErrPublishInvalid) {
		t.Fatalf("publish error = %v", err)
	}
	if count := syncCount(t, fixture, `SELECT COUNT(*) FROM tracking_interests`); count != 0 {
		t.Fatalf("tracking interests after rollback = %d", count)
	}
	if count := syncCount(t, fixture, `SELECT COUNT(*) FROM account_observations`); count != 0 {
		t.Fatalf("account observations after rollback = %d", count)
	}
	var state string
	if err := fixture.db.SQL().QueryRow(`SELECT state FROM favorite_snapshot_runs WHERE snapshot_run_id = ?`, runID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "complete" {
		t.Fatalf("run state after rollback = %s", state)
	}
}

func TestPublicationFansOutClaimsAndKeepsAccountAndContentScopesSeparate(t *testing.T) {
	fixture := newSyncFixture(t)
	releaseID := seedSyncRelease(t, fixture, "scope", "scope-account-v1")
	clientA, accountOne := addSyncAccount(t, fixture, "scope", releaseID, "account-one", "manwa:level:2")
	addSyncClaim(t, fixture, clientA, accountOne, "scope")
	clientB := addSyncClient(t, fixture, "account-one-second-client")
	linkSyncClient(t, fixture, clientB, accountOne, "scope")
	addSyncClaim(t, fixture, clientB, accountOne, "scope")
	clientC := addSyncClient(t, fixture, "no-claim-client")
	clientD, accountTwo := addSyncAccount(t, fixture, "scope", releaseID, "account-two", "manwa:level:3")
	addSyncClaim(t, fixture, clientD, accountTwo, "scope")
	err := fixture.db.WriteTx(context.Background(), func(tx *v2store.Tx) error {
		_, err := tx.ExecContext(context.Background(), `INSERT INTO tracking_interests(tracking_interest_id, artifact_id, comic_id, source_account_id, origin_kind, origin_key, visibility_scope, variant_key, state, title, revision, created_at, updated_at) VALUES('level3-interest', 'scope', 'comic-1', ?, 'localUpload', 'local-upload-1', 'manwa:level:3', '', 'active', 'Level 3', 1, ?, ?)`, accountTwo, fixture.now.Format(time.RFC3339Nano), fixture.now.Format(time.RFC3339Nano))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	demand := planSyncDemand(t, fixture, "scope", accountOne, false)
	runID := runSyncSnapshot(t, fixture, "scope", releaseID, accountOne, demand.ExecutionGeneration, []v2scan.SnapshotItem{syncSnapshotItem(t, "comic-1", "manwa:level:2", true)})
	result := publishSyncSnapshot(t, fixture, runID, accountOne, releaseID, demand.ExecutionGeneration)
	if result.PublishedItems != 1 || result.AccountObservations != 1 || result.ContentObservations != 1 {
		t.Fatalf("publish result = %+v", result)
	}
	if high := syncCount(t, fixture, `SELECT high_change_seq FROM client_change_watermarks WHERE client_id = ?`, clientA); high == 0 {
		t.Fatal("account owner received no changes")
	}
	if high := syncCount(t, fixture, `SELECT high_change_seq FROM client_change_watermarks WHERE client_id = ?`, clientB); high == 0 {
		t.Fatal("second linked client received no changes")
	}
	if high := syncCount(t, fixture, `SELECT high_change_seq FROM client_change_watermarks WHERE client_id = ?`, clientC); high != 0 {
		t.Fatalf("unclaimed client changes = %d", high)
	}
	if operation := syncEntityOperation(t, fixture, clientA, "contentObservation", "content_obs_"+digestSyncString("scope\x00comic-1\x00manwa:level:2\x00observation-scope")); operation != "upsert" {
		t.Fatalf("owner content operation = %s", operation)
	}
	var contentForD int64
	if err := fixture.db.SQL().QueryRow(`SELECT COUNT(*) FROM client_entity_states WHERE client_id = ? AND entity_type = 'contentObservation' AND operation = 'upsert'`, clientD).Scan(&contentForD); err != nil {
		t.Fatal(err)
	}
	if contentForD != 0 {
		t.Fatalf("level-3 client received level-2 content = %d", contentForD)
	}
	if count := syncCount(t, fixture, `SELECT COUNT(*) FROM account_observations WHERE source_account_id = ?`, accountOne); count != 1 {
		t.Fatalf("account-one observations = %d", count)
	}
	if count := syncCount(t, fixture, `SELECT COUNT(*) FROM account_observations WHERE source_account_id = ?`, accountTwo); count != 0 {
		t.Fatalf("account-two observations crossed source account = %d", count)
	}
}

func TestPublicationValidationAndInterestTombstoneRecreate(t *testing.T) {
	fixture := newSyncFixture(t)
	releaseID := seedSyncRelease(t, fixture, "lifecycle", "lifecycle-account-v1")
	clientID, accountID := addSyncAccount(t, fixture, "lifecycle", releaseID, "lifecycle-owner", "manwa:level:2")
	addSyncClaim(t, fixture, clientID, accountID, "lifecycle")
	demand := planSyncDemand(t, fixture, "lifecycle", accountID, false)
	firstItems := []v2scan.SnapshotItem{syncSnapshotItem(t, "comic-1", "manwa:level:2", true), syncSnapshotItem(t, "comic-2", "manwa:level:2", false)}
	runID := runSyncSnapshot(t, fixture, "lifecycle", releaseID, accountID, demand.ExecutionGeneration, firstItems)
	publishSyncSnapshot(t, fixture, runID, accountID, releaseID, demand.ExecutionGeneration)
	firstContentRevision := syncCount(t, fixture, `SELECT observation_revision FROM content_observations WHERE artifact_id = 'lifecycle' AND comic_id = 'comic-1'`)
	firstAccountRevision := syncCount(t, fixture, `SELECT observation_revision FROM account_observations WHERE source_account_id = ? AND comic_id = 'comic-1'`, accountID)
	secondDemand := planSyncDemand(t, fixture, "lifecycle", accountID, true)
	secondRun := runSyncSnapshot(t, fixture, "lifecycle", releaseID, accountID, secondDemand.ExecutionGeneration, []v2scan.SnapshotItem{syncSnapshotItem(t, "comic-1", "manwa:level:2", true)})
	publishSyncSnapshot(t, fixture, secondRun, accountID, releaseID, secondDemand.ExecutionGeneration)
	if revision := syncCount(t, fixture, `SELECT observation_revision FROM content_observations WHERE artifact_id = 'lifecycle' AND comic_id = 'comic-1'`); revision != firstContentRevision {
		t.Fatalf("unchanged content observation revision = %d, want %d", revision, firstContentRevision)
	}
	if revision := syncCount(t, fixture, `SELECT validation_revision FROM content_observations WHERE artifact_id = 'lifecycle' AND comic_id = 'comic-1'`); revision != 2 {
		t.Fatalf("content validation revision = %d", revision)
	}
	if revision := syncCount(t, fixture, `SELECT observation_revision FROM account_observations WHERE source_account_id = ? AND comic_id = 'comic-1'`, accountID); revision != firstAccountRevision {
		t.Fatalf("unchanged account observation revision = %d, want %d", revision, firstAccountRevision)
	}
	interestTwo := "interest_comic-2_" + accountID
	if operation := syncEntityOperation(t, fixture, clientID, "trackingInterest", interestTwo); operation != "delete" {
		t.Fatalf("removed interest operation = %s", operation)
	}
	contentTwo := "content_obs_" + digestSyncString("lifecycle\x00comic-2\x00manwa:level:2\x00observation-lifecycle")
	if operation := syncEntityOperation(t, fixture, clientID, "contentObservation", contentTwo); operation != "delete" {
		t.Fatalf("removed content operation = %s", operation)
	}
	thirdDemand := planSyncDemand(t, fixture, "lifecycle", accountID, true)
	thirdRun := runSyncSnapshot(t, fixture, "lifecycle", releaseID, accountID, thirdDemand.ExecutionGeneration, firstItems)
	publishSyncSnapshot(t, fixture, thirdRun, accountID, releaseID, thirdDemand.ExecutionGeneration)
	if operation := syncEntityOperation(t, fixture, clientID, "trackingInterest", interestTwo); operation != "upsert" {
		t.Fatalf("recreated interest operation = %s", operation)
	}
	if operation := syncEntityOperation(t, fixture, clientID, "contentObservation", contentTwo); operation != "upsert" {
		t.Fatalf("recreated content operation = %s", operation)
	}
}

func TestChangePullCursorSnapshotDigestAndPrune(t *testing.T) {
	fixture := newSyncFixture(t)
	releaseID := seedSyncRelease(t, fixture, "sync", "sync-account-v1")
	clientID, accountID := addSyncAccount(t, fixture, "sync", releaseID, "sync-owner", "manwa:level:2")
	addSyncClaim(t, fixture, clientID, accountID, "sync")
	demand := planSyncDemand(t, fixture, "sync", accountID, false)
	runID := runSyncSnapshot(t, fixture, "sync", releaseID, accountID, demand.ExecutionGeneration, []v2scan.SnapshotItem{syncSnapshotItem(t, "comic-sync", "manwa:level:2", true)})
	publishSyncSnapshot(t, fixture, runID, accountID, releaseID, demand.ExecutionGeneration)
	store := NewChangeStore(fixture.repo)
	page, err := store.PullClientChanges(context.Background(), clientID, 0, 100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Changes) == 0 || page.HighWatermark == 0 || page.NextCursor == "" {
		t.Fatalf("change page = %+v", page)
	}
	seen := make(map[string]struct{}, len(page.Changes))
	for _, change := range page.Changes {
		key := change.EntityType + "\x00" + change.EntityKey
		if _, ok := seen[key]; ok {
			t.Fatalf("duplicate current entity change = %+v", change)
		}
		seen[key] = struct{}{}
	}
	if _, err := store.PullClientChangesCursor(context.Background(), clientID, page.NextCursor+"tampered", 100, 1<<20); !errors.Is(err, ErrCursorInvalid) {
		t.Fatalf("tampered cursor error = %v", err)
	}
	if _, err := store.PullClientChanges(context.Background(), clientID, 0, 100, 1); !errors.Is(err, ErrChangeByteBudget) {
		t.Fatalf("small byte budget error = %v", err)
	}
	streamer := NewSnapshotStreamer(fixture.repo, 0)
	stream, err := streamer.StreamClientSnapshot(context.Background(), SnapshotRequest{ClientID: clientID, Scope: SnapshotScopeSource, ArtifactID: "sync", SourceAccountID: accountID, Reason: "test"})
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(stream.Data), "\n"), "\n")
	if len(lines) < 3 {
		t.Fatalf("snapshot lines = %d", len(lines))
	}
	var header, footer map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &header); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &footer); err != nil {
		t.Fatal(err)
	}
	if header["type"] != "header" || footer["type"] != "footer" {
		t.Fatalf("snapshot boundary = %v / %v", header["type"], footer["type"])
	}
	partial := sha256.Sum256([]byte(strings.Join(lines[:len(lines)-1], "\n") + "\n"))
	if footer["digest"] != hex.EncodeToString(partial[:]) {
		t.Fatalf("snapshot footer digest = %v", footer["digest"])
	}
	all := sha256.Sum256(stream.Data)
	if stream.Digest != hex.EncodeToString(all[:]) {
		t.Fatalf("snapshot digest = %s", stream.Digest)
	}
	if _, err := streamer.StreamClientSnapshot(context.Background(), SnapshotRequest{ClientID: addSyncClient(t, fixture, "unauthorized"), Scope: SnapshotScopeSource, ArtifactID: "sync", SourceAccountID: accountID}); !errors.Is(err, ErrSnapshotAccessDenied) {
		t.Fatalf("unauthorized snapshot error = %v", err)
	}
	pruner := NewPruner(fixture.repo)
	highBeforePrune := syncCount(t, fixture, `SELECT high_change_seq FROM client_change_watermarks WHERE client_id = ?`, clientID)
	if result, err := pruner.PruneClientChanges(context.Background(), clientID, PruneOptions{MaxAge: time.Second, MaxRows: 1, Now: fixture.now.Add(5 * time.Minute)}); err != nil {
		t.Fatal(err)
	} else if result.DeletedChanges >= int(highBeforePrune) || syncCount(t, fixture, `SELECT low_change_seq FROM client_change_watermarks WHERE client_id = ?`, clientID) >= highBeforePrune {
		t.Fatalf("issued receipt did not pin its base change: %+v", result)
	}
	if _, err := pruner.ExpireReceipts(context.Background(), fixture.now.Add(SnapshotReceiptTTL+time.Hour)); err != nil {
		t.Fatal(err)
	}
	if result, err := pruner.PruneClientChanges(context.Background(), clientID, PruneOptions{MaxAge: time.Second, MaxRows: 1, Now: fixture.now.Add(SnapshotReceiptTTL + 2*time.Hour)}); err != nil {
		t.Fatal(err)
	} else if result.DeletedChanges == 0 || result.AdvancedClients != 1 {
		t.Fatalf("expired receipt did not allow prune: %+v", result)
	}
	if _, err := store.PullClientChanges(context.Background(), clientID, 0, 100, 1<<20); !errors.Is(err, ErrResyncRequired) {
		t.Fatalf("low watermark error = %v", err)
	}
	if _, err := io.ReadAll(streamer.SnapshotReader(stream)); err != nil {
		t.Fatal(err)
	}
}
