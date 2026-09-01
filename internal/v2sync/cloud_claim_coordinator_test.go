package v2sync

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"venera-server/internal/v2domain"
	"venera-server/internal/v2store"
)

type coordinatorClaimFixture struct {
	fixture     *syncFixture
	clientID    string
	accountID   string
	artifactID  string
	releaseID   string
	preparation v2store.CloudPreparation
	ready       v2store.CloudPreparation
}

func newCoordinatorClaimFixture(t *testing.T, label string) *coordinatorClaimFixture {
	t.Helper()
	fixture := newSyncFixture(t)
	artifactID := "coordinator-" + label
	releaseID := seedSyncRelease(t, fixture, artifactID, "account-"+artifactID)
	clientID, accountID := addSyncAccount(t, fixture, artifactID, releaseID, "coordinator-"+label, "scope:authenticated")
	setCoordinatorInventory(t, fixture, clientID, artifactID, releaseID, 0)
	selected, err := fixture.repo.GetSelectedClientSourceAccount(context.Background(), clientID, artifactID)
	if err != nil {
		t.Fatal(err)
	}
	preparation, err := fixture.repo.CreateCloudPreparation(context.Background(), v2store.CreateCloudPreparationRequest{
		ClientID: clientID, ArtifactID: artifactID,
		ExpectedSourceAccountRevision: selected.AccountRevision,
		ExpectedLinkRevision:          selected.LinkRevision,
		ExpectedInventoryRevision:     1,
		TTL:                           time.Hour,
		IdempotencyKey:                "coordinator-prep-" + label,
	})
	if err != nil {
		t.Fatal(err)
	}
	ready, err := fixture.repo.MarkPreparationSnapshotReady(context.Background(), v2store.MarkPreparationSnapshotReadyRequest{
		ClientID: clientID, PreparationID: preparation.ID, ExpectedRevision: preparation.Revision,
		SnapshotDigest: "digest-coordinator-" + label, IdempotencyKey: "coordinator-ready-" + label,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &coordinatorClaimFixture{fixture: fixture, clientID: clientID, accountID: accountID, artifactID: artifactID, releaseID: releaseID, preparation: preparation, ready: ready}
}

func setCoordinatorInventory(t *testing.T, fixture *syncFixture, clientID, artifactID, releaseID string, expectedRevision v2domain.Revision) {
	t.Helper()
	_, err := fixture.repo.ReplaceClientInventory(context.Background(), v2store.ReplaceClientInventoryRequest{
		ClientID: clientID, ExpectedRevision: expectedRevision,
		Entries: []v2domain.InventoryEntry{{
			ArtifactID: v2domain.ArtifactID(artifactID), PackageReleaseID: v2domain.PackageReleaseID(releaseID), ManagementMode: "managed",
			CompatibilityState: v2domain.CompatibilityCompatible, CoreHash: "core-" + artifactID,
			ClientExtensionsJSON: "{}", ObservationContractID: "observation-" + artifactID,
			AccountObservationContractID: "account-" + artifactID, AccountProbeContractID: "probe-" + artifactID,
		}},
		IdempotencyKey: "coordinator-inventory-" + clientID + "-" + strconv.FormatInt(int64(expectedRevision), 10),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func coordinatorCount(t *testing.T, fixture *coordinatorClaimFixture, query string, args ...any) int64 {
	t.Helper()
	var value int64
	if err := fixture.fixture.db.SQL().QueryRow(query, args...).Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func assertCoordinatorCommitRollback(t *testing.T, fixture *coordinatorClaimFixture, expectedHigh int64) {
	t.Helper()
	if count := coordinatorCount(t, fixture, `SELECT COUNT(*) FROM client_cloud_claims WHERE client_id = ? AND state = 'active'`, fixture.clientID); count != 0 {
		t.Fatalf("active claims after failed commit = %d", count)
	}
	var receiptState, preparationState string
	var preparationRevision int64
	if err := fixture.fixture.db.SQL().QueryRow(`SELECT state FROM snapshot_receipts WHERE snapshot_receipt_id = ?`, fixture.ready.SnapshotReceiptID).Scan(&receiptState); err != nil {
		t.Fatal(err)
	}
	if err := fixture.fixture.db.SQL().QueryRow(`SELECT state, revision FROM cloud_mode_preparations WHERE preparation_id = ?`, fixture.preparation.ID).Scan(&preparationState, &preparationRevision); err != nil {
		t.Fatal(err)
	}
	if receiptState != "issued" || preparationState != "snapshotReady" || preparationRevision != int64(fixture.ready.Revision) {
		t.Fatalf("failed commit states = receipt:%s preparation:%s/%d", receiptState, preparationState, preparationRevision)
	}
	if high := coordinatorCount(t, fixture, `SELECT high_change_seq FROM client_change_watermarks WHERE client_id = ?`, fixture.clientID); high != expectedHigh {
		t.Fatalf("watermark after failed commit = %d, want %d", high, expectedHigh)
	}
	if rows := coordinatorCount(t, fixture, `SELECT COUNT(*) FROM idempotency_records WHERE actor_id = ? AND route_key = 'commit-cloud-claim'`, fixture.clientID); rows != 0 {
		t.Fatalf("commit idempotency rows after rollback = %d", rows)
	}
}

func TestCloudClaimCoordinatorCommitProjectionFailureRollsBackMutation(t *testing.T) {
	fixture := newCoordinatorClaimFixture(t, "commit-fault")
	coordinator := NewCloudClaimCoordinator(fixture.fixture.repo)
	fault := errors.New("test projection failure after claim mutation")
	coordinator.projectionFault = func(point string) error {
		if point == "after-claim-projection" {
			return fault
		}
		return nil
	}
	beforeHigh := coordinatorCount(t, fixture, `SELECT high_change_seq FROM client_change_watermarks WHERE client_id = ?`, fixture.clientID)
	_, err := coordinator.CommitCloudClaim(context.Background(), v2store.CommitCloudClaimRequest{
		ClientID: fixture.clientID, PreparationID: fixture.ready.ID, ExpectedRevision: fixture.ready.Revision,
		SnapshotReceiptID: fixture.ready.SnapshotReceiptID, SnapshotDigest: "digest-coordinator-commit-fault", IdempotencyKey: "coordinator-commit-fault",
	})
	if !errors.Is(err, fault) {
		t.Fatalf("commit projection fault = %v, want injected failure", err)
	}
	assertCoordinatorCommitRollback(t, fixture, beforeHigh)

	coordinator.projectionFault = nil
	request := v2store.CommitCloudClaimRequest{
		ClientID: fixture.clientID, PreparationID: fixture.ready.ID, ExpectedRevision: fixture.ready.Revision,
		SnapshotReceiptID: fixture.ready.SnapshotReceiptID, SnapshotDigest: "digest-coordinator-commit-fault", IdempotencyKey: "coordinator-commit-fault",
	}
	committed, err := coordinator.CommitCloudClaim(context.Background(), request)
	if err != nil {
		t.Fatalf("retry commit after projection fault: %v", err)
	}
	if committed.Claim.State != v2domain.ClaimActive || committed.Claim.Revision != 1 {
		t.Fatalf("committed claim = %+v", committed)
	}
	highAfterCommit := coordinatorCount(t, fixture, `SELECT high_change_seq FROM client_change_watermarks WHERE client_id = ?`, fixture.clientID)
	if highAfterCommit == 0 || coordinatorCount(t, fixture, `SELECT COUNT(*) FROM client_cloud_claims WHERE client_id = ? AND state = 'active'`, fixture.clientID) != 1 {
		t.Fatalf("successful commit projection missing: high=%d", highAfterCommit)
	}
	replayed, err := coordinator.CommitCloudClaim(context.Background(), request)
	if err != nil || replayed.Claim.Revision != committed.Claim.Revision || replayed.HighChangeSeq != committed.HighChangeSeq {
		t.Fatalf("idempotent commit replay = %+v, err=%v", replayed, err)
	}
	if high := coordinatorCount(t, fixture, `SELECT high_change_seq FROM client_change_watermarks WHERE client_id = ?`, fixture.clientID); high != highAfterCommit {
		t.Fatalf("idempotent commit changed watermark from %d to %d", highAfterCommit, high)
	}
}

func TestCloudClaimCoordinatorWatermarkFailureRollsBackMutation(t *testing.T) {
	fixture := newCoordinatorClaimFixture(t, "watermark-fault")
	coordinator := NewCloudClaimCoordinator(fixture.fixture.repo)
	fault := errors.New("test watermark query failure")
	coordinator.highChangeSeqFault = func() error { return fault }
	beforeHigh := coordinatorCount(t, fixture, `SELECT high_change_seq FROM client_change_watermarks WHERE client_id = ?`, fixture.clientID)
	_, err := coordinator.CommitCloudClaim(context.Background(), v2store.CommitCloudClaimRequest{
		ClientID: fixture.clientID, PreparationID: fixture.ready.ID, ExpectedRevision: fixture.ready.Revision,
		SnapshotReceiptID: fixture.ready.SnapshotReceiptID, SnapshotDigest: "digest-coordinator-watermark-fault", IdempotencyKey: "coordinator-watermark-fault",
	})
	if !errors.Is(err, fault) {
		t.Fatalf("commit watermark failure = %v, want injected failure", err)
	}
	assertCoordinatorCommitRollback(t, fixture, beforeHigh)
}

func TestCloudClaimCoordinatorDeleteProjectionFailureRollsBackMutation(t *testing.T) {
	fixture := newCoordinatorClaimFixture(t, "delete-fault")
	coordinator := NewCloudClaimCoordinator(fixture.fixture.repo)
	committed, err := coordinator.CommitCloudClaim(context.Background(), v2store.CommitCloudClaimRequest{
		ClientID: fixture.clientID, PreparationID: fixture.ready.ID, ExpectedRevision: fixture.ready.Revision,
		SnapshotReceiptID: fixture.ready.SnapshotReceiptID, SnapshotDigest: "digest-coordinator-delete-fault", IdempotencyKey: "coordinator-delete-commit",
	})
	if err != nil {
		t.Fatal(err)
	}
	beforeHigh := coordinatorCount(t, fixture, `SELECT high_change_seq FROM client_change_watermarks WHERE client_id = ?`, fixture.clientID)
	beforeEntities := coordinatorCount(t, fixture, `SELECT COUNT(*) FROM client_entity_states WHERE client_id = ?`, fixture.clientID)
	coordinator.projectionFault = func(point string) error {
		if point == "after-entity-projection" {
			return errors.New("test delete projection failure")
		}
		return nil
	}
	deleteRequest := v2store.DeleteCloudClaimRequest{
		ClientID: fixture.clientID, ArtifactID: fixture.artifactID, SourceAccountID: fixture.accountID,
		ExpectedRevision: committed.Claim.Revision, IdempotencyKey: "coordinator-delete-fault",
	}
	if _, err := coordinator.DeleteCloudClaim(context.Background(), deleteRequest); err == nil {
		t.Fatal("delete projection fault unexpectedly succeeded")
	}
	if count := coordinatorCount(t, fixture, `SELECT COUNT(*) FROM client_cloud_claims WHERE client_id = ? AND state = 'active'`, fixture.clientID); count != 1 {
		t.Fatalf("active claims after failed delete = %d", count)
	}
	if high := coordinatorCount(t, fixture, `SELECT high_change_seq FROM client_change_watermarks WHERE client_id = ?`, fixture.clientID); high != beforeHigh {
		t.Fatalf("watermark after failed delete = %d, want %d", high, beforeHigh)
	}
	if entities := coordinatorCount(t, fixture, `SELECT COUNT(*) FROM client_entity_states WHERE client_id = ?`, fixture.clientID); entities != beforeEntities {
		t.Fatalf("projection entities after failed delete = %d, want %d", entities, beforeEntities)
	}

	coordinator.projectionFault = nil
	deleted, err := coordinator.DeleteCloudClaim(context.Background(), deleteRequest)
	if err != nil || deleted.Claim.State != v2domain.ClaimSuspended {
		t.Fatalf("successful delete = %+v, err=%v", deleted, err)
	}
	if count := coordinatorCount(t, fixture, `SELECT COUNT(*) FROM client_cloud_claims WHERE client_id = ? AND state = 'active'`, fixture.clientID); count != 0 {
		t.Fatalf("active claims after successful delete = %d", count)
	}
	if high := coordinatorCount(t, fixture, `SELECT high_change_seq FROM client_change_watermarks WHERE client_id = ?`, fixture.clientID); high <= beforeHigh {
		t.Fatalf("successful delete did not publish projection change: %d <= %d", high, beforeHigh)
	}
}

func TestCommitCloudClaimRejectsChangedFixedIdentityWithoutSideEffects(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *coordinatorClaimFixture)
	}{
		{
			name: "session epoch",
			mutate: func(t *testing.T, fixture *coordinatorClaimFixture) {
				t.Helper()
				if err := fixture.fixture.db.WriteTx(context.Background(), func(tx *v2store.Tx) error {
					_, err := tx.ExecContext(context.Background(), `UPDATE source_accounts SET session_epoch = session_epoch + 1 WHERE source_account_id = ?`, fixture.accountID)
					return err
				}); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "inventory revision",
			mutate: func(t *testing.T, fixture *coordinatorClaimFixture) {
				t.Helper()
				setCoordinatorInventory(t, fixture.fixture, fixture.clientID, fixture.artifactID, fixture.releaseID, 1)
			},
		},
		{
			name: "active release",
			mutate: func(t *testing.T, fixture *coordinatorClaimFixture) {
				t.Helper()
				if err := fixture.fixture.db.WriteTx(context.Background(), func(tx *v2store.Tx) error {
					_, err := tx.ExecContext(context.Background(), `UPDATE source_package_releases SET state = 'superseded' WHERE package_release_id = ?`, fixture.releaseID)
					return err
				}); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := newCoordinatorClaimFixture(t, "fixed-"+testCase.name)
			testCase.mutate(t, fixture)
			beforeHigh := coordinatorCount(t, fixture, `SELECT high_change_seq FROM client_change_watermarks WHERE client_id = ?`, fixture.clientID)
			_, err := NewCloudClaimCoordinator(fixture.fixture.repo).CommitCloudClaim(context.Background(), v2store.CommitCloudClaimRequest{
				ClientID: fixture.clientID, PreparationID: fixture.ready.ID, ExpectedRevision: fixture.ready.Revision,
				SnapshotReceiptID: fixture.ready.SnapshotReceiptID, SnapshotDigest: "digest-coordinator-fixed-" + testCase.name, IdempotencyKey: "coordinator-fixed-" + testCase.name,
			})
			if !errors.Is(err, v2store.ErrCloudClaimNotAllowed) {
				t.Fatalf("fixed identity error = %v, want preparation-required source error", err)
			}
			assertCoordinatorCommitRollback(t, fixture, beforeHigh)
		})
	}
}
