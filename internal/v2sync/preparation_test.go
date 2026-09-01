package v2sync

import (
	"context"
	"errors"
	"testing"
	"time"

	"venera-server/internal/v2domain"
	"venera-server/internal/v2scan"
	"venera-server/internal/v2store"
)

func createPreparationForSyncAccount(t *testing.T, fixture *syncFixture, clientID, accountID, artifactID string) v2store.CloudPreparation {
	t.Helper()
	var releaseID, coreHash, observationContractID, accountObservationContractID, accountProbeContractID string
	if err := fixture.db.SQL().QueryRow(`SELECT package_release_id, core_hash, observation_contract_id, account_observation_contract_id, account_probe_contract_id FROM source_package_releases WHERE artifact_id = ? AND state = 'active'`, artifactID).Scan(&releaseID, &coreHash, &observationContractID, &accountObservationContractID, &accountProbeContractID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.repo.ReplaceClientInventory(context.Background(), v2store.ReplaceClientInventoryRequest{
		ClientID: clientID, ExpectedRevision: 0,
		Entries: []v2domain.InventoryEntry{{
			ArtifactID:                   v2domain.ArtifactID(artifactID),
			PackageReleaseID:             v2domain.PackageReleaseID(releaseID),
			ManagementMode:               "managed",
			CompatibilityState:           v2domain.CompatibilityCompatible,
			CoreHash:                     coreHash,
			ClientExtensionsJSON:         "{}",
			ObservationContractID:        observationContractID,
			AccountObservationContractID: accountObservationContractID,
			AccountProbeContractID:       accountProbeContractID,
		}},
		IdempotencyKey: "inventory-" + clientID,
	}); err != nil {
		t.Fatal(err)
	}
	selected, err := fixture.repo.GetSelectedClientSourceAccount(context.Background(), clientID, artifactID)
	if err != nil {
		t.Fatal(err)
	}
	var actualAccountRevision, actualLinkRevision, actualInventoryRevision int64
	if err := fixture.db.SQL().QueryRow(`SELECT a.revision, l.revision, i.inventory_revision FROM source_accounts a JOIN client_source_links l ON l.source_account_id = a.source_account_id JOIN client_source_inventory i ON i.client_id = l.client_id AND i.artifact_id = l.artifact_id WHERE l.client_id = ? AND l.artifact_id = ? AND l.selected_for_artifact = 1`, clientID, artifactID).Scan(&actualAccountRevision, &actualLinkRevision, &actualInventoryRevision); err != nil {
		t.Fatal(err)
	}
	if selected.AccountRevision != v2domain.Revision(actualAccountRevision) || selected.LinkRevision != v2domain.Revision(actualLinkRevision) {
		t.Fatalf("selected revisions=%+v actual account=%d link=%d inventory=%d", selected, actualAccountRevision, actualLinkRevision, actualInventoryRevision)
	}
	preparation, err := fixture.repo.CreateCloudPreparation(context.Background(), v2store.CreateCloudPreparationRequest{
		ClientID:                      clientID,
		ArtifactID:                    artifactID,
		ExpectedSourceAccountRevision: selected.AccountRevision,
		ExpectedLinkRevision:          selected.LinkRevision,
		ExpectedInventoryRevision:     v2domain.Revision(actualInventoryRevision),
		TTL:                           time.Hour,
		IdempotencyKey:                "prep-" + clientID,
	})
	if err != nil {
		t.Fatalf("create preparation selected=%+v: %v", selected, err)
	}
	return preparation
}

func TestPreparationReadinessRequiresPublishedFreshSnapshot(t *testing.T) {
	fixture := newSyncFixture(t)
	releaseID := seedSyncRelease(t, fixture, "preparation-empty", "preparation-empty-account-v1")
	clientID, accountID := addSyncAccount(t, fixture, "preparation-empty", releaseID, "preparation-empty-owner", "manwa:level:2")
	addSyncClaim(t, fixture, clientID, accountID, "preparation-empty")
	if _, err := v2scan.NewPlanner(fixture.repo, v2scan.PlannerOptions{Clock: func() time.Time { return fixture.now }}).Plan(context.Background()); err != nil {
		t.Fatal(err)
	}
	preparation := createPreparationForSyncAccount(t, fixture, clientID, accountID, "preparation-empty")
	coordinator := NewPreparationCoordinator(fixture.repo)
	if advanced, err := coordinator.EvaluatePreparation(context.Background(), clientID, preparation.ID); !errors.Is(err, ErrPreparationNotReady) || advanced {
		t.Fatalf("unpublished preparation readiness = advanced:%t err:%v", advanced, err)
	}
	var state, receiptID string
	var revision int64
	if err := fixture.db.SQL().QueryRow(`SELECT state, revision, COALESCE(snapshot_receipt_id, '') FROM cloud_mode_preparations WHERE preparation_id = ?`, preparation.ID).Scan(&state, &revision, &receiptID); err != nil {
		t.Fatal(err)
	}
	if state != "preparing" || revision != 1 || receiptID != "" {
		t.Fatalf("unpublished preparation mutated: state=%s revision=%d receipt=%q", state, revision, receiptID)
	}

	demand := syncDemand(t, fixture, "preparation-empty", accountID)
	runID := runSyncSnapshot(t, fixture, "preparation-empty", releaseID, accountID, demand.ExecutionGeneration, nil)
	publishSyncSnapshot(t, fixture, runID, accountID, releaseID, demand.ExecutionGeneration)
	changesBeforeReady := syncCount(t, fixture, `SELECT COUNT(*) FROM client_changes WHERE client_id = ?`, clientID)
	advanced, err := coordinator.EvaluatePreparation(context.Background(), clientID, preparation.ID)
	if err != nil || !advanced {
		t.Fatalf("published empty preparation readiness = advanced:%t err:%v", advanced, err)
	}
	if err := fixture.db.SQL().QueryRow(`SELECT state, revision, COALESCE(snapshot_receipt_id, '') FROM cloud_mode_preparations WHERE preparation_id = ?`, preparation.ID).Scan(&state, &revision, &receiptID); err != nil {
		t.Fatal(err)
	}
	if state != "snapshotReady" || revision != 2 || receiptID == "" {
		t.Fatalf("published preparation state = %s revision=%d receipt=%q", state, revision, receiptID)
	}
	if count := syncCount(t, fixture, `SELECT COUNT(*) FROM client_changes WHERE client_id = ?`, clientID); count != changesBeforeReady {
		t.Fatalf("preparation readiness changed client changes from %d to %d", changesBeforeReady, count)
	}
}

func TestPreparationReadinessRequiresFreshObservationsForRemoteInterests(t *testing.T) {
	fixture := newSyncFixture(t)
	releaseID := seedSyncRelease(t, fixture, "preparation-interest", "preparation-interest-account-v1")
	clientID, accountID := addSyncAccount(t, fixture, "preparation-interest", releaseID, "preparation-interest-owner", "manwa:level:2")
	addSyncClaim(t, fixture, clientID, accountID, "preparation-interest")
	if err := fixture.db.WriteTx(context.Background(), func(tx *v2store.Tx) error {
		now := fixture.now.Format(time.RFC3339Nano)
		_, err := tx.ExecContext(context.Background(), `INSERT INTO tracking_interests(tracking_interest_id, artifact_id, comic_id, source_account_id, origin_kind, origin_key, visibility_scope, variant_key, state, title, revision, created_at, updated_at) VALUES('prep-interest', 'preparation-interest', 'comic-1', ?, 'remoteSnapshot', 'remote-1', 'manwa:level:2', '', 'active', 'Comic 1', 1, ?, ?)`, accountID, now, now)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := v2scan.NewPlanner(fixture.repo, v2scan.PlannerOptions{Clock: func() time.Time { return fixture.now }}).Plan(context.Background()); err != nil {
		t.Fatal(err)
	}
	preparation := createPreparationForSyncAccount(t, fixture, clientID, accountID, "preparation-interest")
	demand := syncDemand(t, fixture, "preparation-interest", accountID)
	runID := runSyncSnapshot(t, fixture, "preparation-interest", releaseID, accountID, demand.ExecutionGeneration, nil)
	publishSyncSnapshot(t, fixture, runID, accountID, releaseID, demand.ExecutionGeneration)
	if advanced, err := NewPreparationCoordinator(fixture.repo).EvaluatePreparation(context.Background(), clientID, preparation.ID); !errors.Is(err, ErrPreparationNotReady) || advanced {
		t.Fatalf("interest without observations readiness = advanced:%t err:%v", advanced, err)
	}
	var state string
	if err := fixture.db.SQL().QueryRow(`SELECT state FROM cloud_mode_preparations WHERE preparation_id = ?`, preparation.ID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "preparing" {
		t.Fatalf("interest preparation state = %s", state)
	}
}
