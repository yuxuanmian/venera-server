package v2store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRevokeClientDeletesClaimsAndPreparationsKeepsSourceAccount(t *testing.T) {
	test := newTestRepository(t)
	clientA := test.enroll(t, "revoke-A")
	clientB := test.enroll(t, "revoke-B")
	setTestInventory(t, test, string(clientA.ID), "inventory-revoke-a")
	setTestInventory(t, test, string(clientB.ID), "inventory-revoke-b")
	candidateA, _ := stageCandidate(t, test, clientA, "revoke-a", "shared-revoke")
	activatedA, err := test.repo.ActivateOrLinkSourceAccount(context.Background(), ActivateSourceAccountRequest{
		ClientID: string(clientA.ID), CandidateID: candidateA.ID, ExpectedRevision: candidateA.Revision, IdempotencyKey: "activate-revoke-a",
	})
	if err != nil {
		t.Fatalf("activate client A: %v", err)
	}
	candidateB, _ := stageCandidate(t, test, clientB, "revoke-b", "shared-revoke")
	if _, err := test.repo.ActivateOrLinkSourceAccount(context.Background(), ActivateSourceAccountRequest{
		ClientID: string(clientB.ID), CandidateID: candidateB.ID, ExpectedRevision: candidateB.Revision, IdempotencyKey: "activate-revoke-b",
	}); err != nil {
		t.Fatalf("activate client B: %v", err)
	}
	preparation, err := test.repo.CreateCloudPreparation(context.Background(), CreateCloudPreparationRequest{
		ClientID: string(clientA.ID), ArtifactID: "artifact-manwa", ExpectedSourceAccountRevision: activatedA.SourceAccount.Revision + 1,
		ExpectedLinkRevision: 1, ExpectedInventoryRevision: 1, TTL: time.Hour, IdempotencyKey: "prep-revoke",
	})
	if err != nil {
		t.Fatalf("create preparation: %v", err)
	}
	ready, err := test.repo.MarkPreparationSnapshotReady(context.Background(), MarkPreparationSnapshotReadyRequest{
		ClientID: string(clientA.ID), PreparationID: preparation.ID, ExpectedRevision: preparation.Revision,
		SnapshotDigest: digestString("revoke-snapshot"), IdempotencyKey: "ready-revoke",
	})
	if err != nil {
		t.Fatalf("mark preparation ready: %v", err)
	}
	claim, err := test.repo.CommitCloudClaim(context.Background(), CommitCloudClaimRequest{
		ClientID: string(clientA.ID), PreparationID: ready.ID, ExpectedRevision: ready.Revision,
		SnapshotReceiptID: ready.SnapshotReceiptID, SnapshotDigest: digestString("revoke-snapshot"), IdempotencyKey: "commit-revoke",
	})
	if err != nil {
		t.Fatalf("commit claim: %v", err)
	}
	if err := test.repo.RevokeClient(context.Background(), string(clientA.ID), clientA.Revision, "revoke-client"); err != nil {
		t.Fatalf("revoke client: %v", err)
	}
	var claimCount, preparationCount, accountCount int
	if err := test.db.SQL().QueryRow("SELECT COUNT(*) FROM client_cloud_claims WHERE client_id = ?", clientA.ID).Scan(&claimCount); err != nil {
		t.Fatalf("count revoked client claims: %v", err)
	}
	if err := test.db.SQL().QueryRow("SELECT COUNT(*) FROM cloud_mode_preparations WHERE client_id = ?", clientA.ID).Scan(&preparationCount); err != nil {
		t.Fatalf("count revoked client preparations: %v", err)
	}
	if err := test.db.SQL().QueryRow("SELECT COUNT(*) FROM source_accounts WHERE source_account_id = ?", claim.SourceAccountID).Scan(&accountCount); err != nil {
		t.Fatalf("count shared source account: %v", err)
	}
	if claimCount != 0 || preparationCount != 0 || accountCount != 1 {
		t.Fatalf("revoke counts claims/preparations/account = %d/%d/%d, want 0/0/1", claimCount, preparationCount, accountCount)
	}
	accountsB, err := test.repo.ListClientSourceAccounts(context.Background(), string(clientB.ID))
	if err != nil {
		t.Fatalf("list client B account after A revoke: %v", err)
	}
	if len(accountsB) != 1 || accountsB[0].SourceAccountID != activatedA.SourceAccount.ID {
		t.Fatalf("client B shared link changed after A revoke: %+v", accountsB)
	}
	if err := test.repo.RevokeClient(context.Background(), string(clientA.ID), clientA.Revision, "revoke-client"); err != nil {
		t.Fatalf("revoke replay: %v", err)
	}
	if _, err := test.repo.ListClientSourceAccounts(context.Background(), string(clientA.ID)); !errors.Is(err, ErrClientRevoked) {
		t.Fatalf("revoked client account list error = %v, want revoked", err)
	}
}
