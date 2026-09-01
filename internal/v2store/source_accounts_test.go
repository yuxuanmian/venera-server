package v2store

import (
	"context"
	"errors"
	"testing"
	"time"

	"venera-server/internal/v2crypto"
	"venera-server/internal/v2domain"
)

func setTestInventory(t *testing.T, test *testRepository, clientID string, key string) {
	t.Helper()
	_, err := test.repo.ReplaceClientInventory(context.Background(), ReplaceClientInventoryRequest{
		ClientID: clientID, ExpectedRevision: 0,
		Entries: []v2domain.InventoryEntry{{
			ArtifactID: "artifact-manwa", PackageReleaseID: "release-manwa-1", ManagementMode: "managed",
			CompatibilityState: v2domain.CompatibilityCompatible, CoreHash: "core-hash",
			ClientExtensionsJSON: "{}", ObservationContractID: "manwa-observation-v1",
			AccountObservationContractID: "manwa-account-observation-v1", AccountProbeContractID: "manwa-account-probe-v1",
		}}, IdempotencyKey: key,
	})
	if err != nil {
		t.Fatalf("set inventory %s: %v", clientID, err)
	}
}

func TestSameIdentityReusesSourceAccountAndRefreshesSessionEpoch(t *testing.T) {
	test := newTestRepository(t)
	clientA := test.enroll(t, "same-A")
	clientB := test.enroll(t, "same-B")
	setTestInventory(t, test, string(clientA.ID), "inventory-same-a")
	setTestInventory(t, test, string(clientB.ID), "inventory-same-b")

	candidateA, _ := stageCandidate(t, test, clientA, "same-a", "same-user")
	activatedA, err := test.repo.ActivateOrLinkSourceAccount(context.Background(), ActivateSourceAccountRequest{
		ClientID: string(clientA.ID), CandidateID: candidateA.ID, ExpectedRevision: candidateA.Revision,
		IdempotencyKey: "activate-same-a",
	})
	if err != nil {
		t.Fatalf("activate client A candidate: %v", err)
	}
	candidateB, _ := stageCandidate(t, test, clientB, "same-b", "same-user")
	if candidateB.TargetSourceAccountID != string(activatedA.SourceAccount.ID) {
		t.Fatalf("same identity probe target = %q, want %q", candidateB.TargetSourceAccountID, activatedA.SourceAccount.ID)
	}
	if candidateB.ExpectedSourceAccountRevision == nil || *candidateB.ExpectedSourceAccountRevision != activatedA.SourceAccount.Revision {
		t.Fatalf("same identity probe expected account revision = %v, want %d", candidateB.ExpectedSourceAccountRevision, activatedA.SourceAccount.Revision)
	}
	activatedB, err := test.repo.ActivateOrLinkSourceAccount(context.Background(), ActivateSourceAccountRequest{
		ClientID: string(clientB.ID), CandidateID: candidateB.ID, ExpectedRevision: candidateB.Revision,
		IdempotencyKey: "activate-same-b",
	})
	if err != nil {
		t.Fatalf("activate client B candidate: %v", err)
	}
	if activatedA.SourceAccount.ID != activatedB.SourceAccount.ID {
		t.Fatalf("same identity created accounts %q and %q", activatedA.SourceAccount.ID, activatedB.SourceAccount.ID)
	}
	var accountCount, sessionCount int
	if err := test.db.SQL().QueryRow(`SELECT COUNT(*) FROM source_accounts WHERE artifact_id = 'artifact-manwa'`).Scan(&accountCount); err != nil {
		t.Fatalf("count source accounts: %v", err)
	}
	if err := test.db.SQL().QueryRow(`SELECT COUNT(*) FROM source_account_sessions`).Scan(&sessionCount); err != nil {
		t.Fatalf("count sessions: %v", err)
	}
	if accountCount != 1 || sessionCount != 1 {
		t.Fatalf("source account/session counts = %d/%d, want 1/1", accountCount, sessionCount)
	}
	var epoch int64
	if err := test.db.SQL().QueryRow(`SELECT session_epoch FROM source_accounts WHERE source_account_id = ?`, activatedA.SourceAccount.ID).Scan(&epoch); err != nil {
		t.Fatalf("read session epoch: %v", err)
	}
	if epoch != 2 {
		t.Fatalf("session epoch = %d, want 2 after second identity proof", epoch)
	}
	var identityCiphertext, sessionEnvelope []byte
	if err := test.db.SQL().QueryRow(`SELECT identity_ciphertext, (SELECT session_envelope FROM source_account_sessions WHERE source_account_id = source_accounts.source_account_id) FROM source_accounts WHERE source_account_id = ?`, activatedA.SourceAccount.ID).Scan(&identityCiphertext, &sessionEnvelope); err != nil {
		t.Fatalf("read stable account ciphertexts: %v", err)
	}
	identityPlaintext, err := v2crypto.OpenSession(test.repo.Keys().SessionAEAD, identityCiphertext, []byte("source-account-identity:"+string(activatedA.SourceAccount.ID)+":artifact-manwa"))
	if err != nil || string(identityPlaintext) != "same-user" {
		t.Fatalf("stable identity decrypt = %q, err=%v", identityPlaintext, err)
	}
	if _, err := v2crypto.OpenSession(test.repo.Keys().SessionAEAD, identityCiphertext, []byte("candidate-identity:"+string(clientB.ID)+":"+candidateB.ID)); !errors.Is(err, v2crypto.ErrInvalidEnvelope) {
		t.Fatalf("candidate identity AAD error = %v, want invalid envelope", err)
	}
	if _, err := v2crypto.OpenSession(test.repo.Keys().SessionAEAD, sessionEnvelope, []byte("candidate:"+string(clientB.ID)+":artifact-manwa")); !errors.Is(err, v2crypto.ErrInvalidEnvelope) {
		t.Fatalf("candidate session AAD error = %v, want invalid envelope", err)
	}
	activeSession, err := test.repo.ReadActiveSourceSession(context.Background(), string(activatedA.SourceAccount.ID), "artifact-manwa", "release-manwa-1", epoch)
	if err != nil {
		t.Fatalf("read active source session: %v", err)
	}
	if activeSession.SessionEpoch != epoch || activeSession.Session["cookies"] == nil {
		t.Fatalf("active source session = %+v", activeSession)
	}
	accountsA, err := test.repo.ListClientSourceAccounts(context.Background(), string(clientA.ID))
	if err != nil {
		t.Fatalf("list client A accounts: %v", err)
	}
	accountsB, err := test.repo.ListClientSourceAccounts(context.Background(), string(clientB.ID))
	if err != nil {
		t.Fatalf("list client B accounts: %v", err)
	}
	if len(accountsA) != 1 || len(accountsB) != 1 || accountsA[0].SourceAccountID != accountsB[0].SourceAccountID || !accountsA[0].Selected || !accountsB[0].Selected {
		t.Fatalf("cross-client links leaked or selection is wrong: A=%+v B=%+v", accountsA, accountsB)
	}
}

func TestSameIdentityRefreshIgnoresActiveClaimAndKeepsSelectedAccount(t *testing.T) {
	test := newTestRepository(t)
	client := test.enroll(t, "same-claim")
	setTestInventory(t, test, string(client.ID), "inventory-same-claim")

	first, _ := stageCandidate(t, test, client, "same-claim-first", "same-user")
	activated, err := test.repo.ActivateOrLinkSourceAccount(context.Background(), ActivateSourceAccountRequest{
		ClientID: string(client.ID), CandidateID: first.ID, ExpectedRevision: first.Revision,
		IdempotencyKey: "activate-same-claim-first",
	})
	if err != nil {
		t.Fatalf("activate first same-identity candidate: %v", err)
	}
	selected, err := test.repo.GetSelectedClientSourceAccount(context.Background(), string(client.ID), "artifact-manwa")
	if err != nil {
		t.Fatalf("read selected same-identity account: %v", err)
	}
	preparation, err := test.repo.CreateCloudPreparation(context.Background(), CreateCloudPreparationRequest{
		ClientID: string(client.ID), ArtifactID: "artifact-manwa",
		ExpectedSourceAccountRevision: selected.AccountRevision, ExpectedLinkRevision: selected.LinkRevision,
		ExpectedInventoryRevision: 1, TTL: time.Hour, IdempotencyKey: "prep-same-claim",
	})
	if err != nil {
		t.Fatalf("create same-identity claim preparation: %v", err)
	}
	ready, err := test.repo.MarkPreparationSnapshotReady(context.Background(), MarkPreparationSnapshotReadyRequest{
		ClientID: string(client.ID), PreparationID: preparation.ID, ExpectedRevision: preparation.Revision,
		SnapshotDigest: digestString("same-claim-snapshot"), IdempotencyKey: "ready-same-claim",
	})
	if err != nil {
		t.Fatalf("mark same-identity snapshot ready: %v", err)
	}
	if _, err := test.repo.CommitCloudClaim(context.Background(), CommitCloudClaimRequest{
		ClientID: string(client.ID), PreparationID: ready.ID, ExpectedRevision: ready.Revision,
		SnapshotReceiptID: ready.SnapshotReceiptID, SnapshotDigest: digestString("same-claim-snapshot"), IdempotencyKey: "commit-same-claim",
	}); err != nil {
		t.Fatalf("commit same-identity claim: %v", err)
	}

	refresh, _ := stageCandidate(t, test, client, "same-claim-refresh", "same-user")
	if refresh.TargetSourceAccountID != string(activated.SourceAccount.ID) {
		t.Fatalf("refresh target = %q, want %q", refresh.TargetSourceAccountID, activated.SourceAccount.ID)
	}
	refreshed, err := test.repo.ActivateOrLinkSourceAccount(context.Background(), ActivateSourceAccountRequest{
		ClientID: string(client.ID), CandidateID: refresh.ID, ExpectedRevision: refresh.Revision,
		IdempotencyKey: "activate-same-claim-refresh",
	})
	if err != nil {
		t.Fatalf("same-identity refresh with active claim: %v", err)
	}
	if refreshed.SourceAccount.ID != activated.SourceAccount.ID || !refreshed.Reused {
		t.Fatalf("same-identity refresh result = %+v", refreshed)
	}
	var epoch int64
	if err := test.db.SQL().QueryRow(`SELECT session_epoch FROM source_accounts WHERE source_account_id = ?`, activated.SourceAccount.ID).Scan(&epoch); err != nil {
		t.Fatalf("read refreshed epoch: %v", err)
	}
	if epoch != 2 {
		t.Fatalf("refreshed epoch = %d, want 2", epoch)
	}
}

func TestCandidateResealFailureRollsBackActivation(t *testing.T) {
	test := newTestRepository(t)
	client := test.enroll(t, "reseal-fault")
	setTestInventory(t, test, string(client.ID), "inventory-reseal-fault")
	fault := errors.New("test source-account reseal fault")
	test.db.fault = FaultFunc(func(point string) error {
		if point == "source-account.reseal" {
			return fault
		}
		return nil
	})
	candidate, _ := stageCandidate(t, test, client, "reseal-fault", "fault-user")
	if _, err := test.repo.ActivateOrLinkSourceAccount(context.Background(), ActivateSourceAccountRequest{
		ClientID: string(client.ID), CandidateID: candidate.ID, ExpectedRevision: candidate.Revision,
		IdempotencyKey: "activate-reseal-fault",
	}); !errors.Is(err, fault) {
		t.Fatalf("reseal fault error = %v, want injected fault", err)
	}
	var accounts, links, sessions int
	if err := test.db.SQL().QueryRow(`SELECT COUNT(*) FROM source_accounts`).Scan(&accounts); err != nil {
		t.Fatal(err)
	}
	if err := test.db.SQL().QueryRow(`SELECT COUNT(*) FROM client_source_links`).Scan(&links); err != nil {
		t.Fatal(err)
	}
	if err := test.db.SQL().QueryRow(`SELECT COUNT(*) FROM source_account_sessions`).Scan(&sessions); err != nil {
		t.Fatal(err)
	}
	if accounts != 0 || links != 0 || sessions != 0 {
		t.Fatalf("activation side effects after reseal fault = accounts:%d links:%d sessions:%d", accounts, links, sessions)
	}
	var state string
	var revision int64
	var sessionEnvelope, identityCiphertext []byte
	if err := test.db.SQL().QueryRow(`SELECT state, revision, session_envelope, probed_identity_ciphertext FROM source_session_candidates WHERE candidate_id = ?`, candidate.ID).Scan(&state, &revision, &sessionEnvelope, &identityCiphertext); err != nil {
		t.Fatal(err)
	}
	if state != "readyLink" || revision != int64(candidate.Revision) || len(sessionEnvelope) == 0 || len(identityCiphertext) == 0 {
		t.Fatalf("candidate after reseal fault = state:%s revision:%d session:%d identity:%d", state, revision, len(sessionEnvelope), len(identityCiphertext))
	}
	test.db.fault = nil
	if _, err := test.repo.ActivateOrLinkSourceAccount(context.Background(), ActivateSourceAccountRequest{
		ClientID: string(client.ID), CandidateID: candidate.ID, ExpectedRevision: candidate.Revision,
		IdempotencyKey: "activate-reseal-retry",
	}); err != nil {
		t.Fatalf("retry activation after reseal fault: %v", err)
	}
}

func TestSourceSwitchIsClientScopedAndClaimBlocksUntilRemoved(t *testing.T) {
	test := newTestRepository(t)
	clientA := test.enroll(t, "switch-A")
	clientB := test.enroll(t, "switch-B")
	setTestInventory(t, test, string(clientA.ID), "inventory-switch-a")
	setTestInventory(t, test, string(clientB.ID), "inventory-switch-b")

	first, _ := stageCandidate(t, test, clientA, "switch-first", "user-one")
	firstActivated, err := test.repo.ActivateOrLinkSourceAccount(context.Background(), ActivateSourceAccountRequest{
		ClientID: string(clientA.ID), CandidateID: first.ID, ExpectedRevision: first.Revision, IdempotencyKey: "activate-switch-first",
	})
	if err != nil {
		t.Fatalf("activate first account: %v", err)
	}
	clientBFirst, _ := stageCandidate(t, test, clientB, "switch-b", "user-one")
	if _, err := test.repo.ActivateOrLinkSourceAccount(context.Background(), ActivateSourceAccountRequest{
		ClientID: string(clientB.ID), CandidateID: clientBFirst.ID, ExpectedRevision: clientBFirst.Revision, IdempotencyKey: "activate-switch-b",
	}); err != nil {
		t.Fatalf("activate client B account: %v", err)
	}

	selectedAForPreparation, err := test.repo.GetSelectedClientSourceAccount(context.Background(), string(clientA.ID), "artifact-manwa")
	if err != nil {
		t.Fatalf("read client A account for preparation: %v", err)
	}
	preparation, err := test.repo.CreateCloudPreparation(context.Background(), CreateCloudPreparationRequest{
		ClientID: string(clientA.ID), ArtifactID: "artifact-manwa", ExpectedSourceAccountRevision: selectedAForPreparation.AccountRevision,
		ExpectedLinkRevision: selectedAForPreparation.LinkRevision, ExpectedInventoryRevision: 1, TTL: time.Hour, IdempotencyKey: "prep-switch",
	})
	if err != nil {
		t.Fatalf("create cloud preparation: %v", err)
	}
	ready, err := test.repo.MarkPreparationSnapshotReady(context.Background(), MarkPreparationSnapshotReadyRequest{
		ClientID: string(clientA.ID), PreparationID: preparation.ID, ExpectedRevision: preparation.Revision,
		SnapshotDigest: digestString("snapshot-a"), IdempotencyKey: "ready-switch",
	})
	if err != nil {
		t.Fatalf("mark snapshot ready: %v", err)
	}
	claim, err := test.repo.CommitCloudClaim(context.Background(), CommitCloudClaimRequest{
		ClientID: string(clientA.ID), PreparationID: ready.ID, ExpectedRevision: ready.Revision,
		SnapshotReceiptID: ready.SnapshotReceiptID, SnapshotDigest: digestString("snapshot-a"), IdempotencyKey: "commit-switch",
	})
	if err != nil {
		t.Fatalf("commit cloud claim: %v", err)
	}

	second, _ := stageCandidate(t, test, clientA, "switch-second", "user-two")
	if second.TargetSourceAccountID != "" || second.ExpectedSourceAccountRevision != nil {
		t.Fatalf("new identity probe target = %q revision = %v, want empty", second.TargetSourceAccountID, second.ExpectedSourceAccountRevision)
	}
	if _, err := test.repo.ActivateOrLinkSourceAccount(context.Background(), ActivateSourceAccountRequest{
		ClientID: string(clientA.ID), CandidateID: second.ID, ExpectedRevision: second.Revision, IdempotencyKey: "activate-switch-second",
	}); !errors.Is(err, ErrCloudClaimMustBeRemoved) {
		t.Fatalf("switch with active claim error = %v, want cloud claim removal", err)
	}
	if err := test.repo.DeleteCloudClaim(context.Background(), DeleteCloudClaimRequest{
		ClientID: string(clientA.ID), ArtifactID: "artifact-manwa", SourceAccountID: claim.SourceAccountID,
		ExpectedRevision: claim.Revision, IdempotencyKey: "delete-switch-claim",
	}); err != nil {
		t.Fatalf("delete cloud claim: %v", err)
	}
	if _, err := test.repo.ActivateOrLinkSourceAccount(context.Background(), ActivateSourceAccountRequest{
		ClientID: string(clientA.ID), CandidateID: second.ID, ExpectedRevision: second.Revision, IdempotencyKey: "activate-switch-second-retry",
	}); !errors.Is(err, ErrSwitchConfirmationRequired) {
		t.Fatalf("unconfirmed switch error = %v, want confirmation required", err)
	}
	confirmed, err := test.repo.ConfirmClientSourceSwitch(context.Background(), ActivateSourceAccountRequest{
		ClientID: string(clientA.ID), CandidateID: second.ID, ExpectedRevision: second.Revision, IdempotencyKey: "confirm-switch-second",
	})
	if err != nil {
		t.Fatalf("confirm source switch: %v", err)
	}
	if confirmed.SourceAccount.ID == firstActivated.SourceAccount.ID {
		t.Fatal("confirmed switch kept the old source account")
	}
	selectedA, err := test.repo.GetSelectedClientSourceAccount(context.Background(), string(clientA.ID), "artifact-manwa")
	if err != nil {
		t.Fatalf("read client A selected account: %v", err)
	}
	if selectedA.SourceAccountID != confirmed.SourceAccount.ID {
		t.Fatalf("client A selected account = %q, want %q", selectedA.SourceAccountID, confirmed.SourceAccount.ID)
	}
	selectedB, err := test.repo.GetSelectedClientSourceAccount(context.Background(), string(clientB.ID), "artifact-manwa")
	if err != nil {
		t.Fatalf("read client B selected account: %v", err)
	}
	if selectedB.SourceAccountID != firstActivated.SourceAccount.ID {
		t.Fatalf("client B selected account changed to %q", selectedB.SourceAccountID)
	}
	var oldLinkSelected int
	if err := test.db.SQL().QueryRow(`SELECT selected_for_artifact FROM client_source_links WHERE client_id = ? AND source_account_id = ?`, clientA.ID, firstActivated.SourceAccount.ID).Scan(&oldLinkSelected); err != nil {
		t.Fatalf("read old A link: %v", err)
	}
	if oldLinkSelected != 0 {
		t.Fatalf("old A link selected_for_artifact = %d, want 0", oldLinkSelected)
	}
}
