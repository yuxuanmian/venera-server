package v2store

import (
	"context"
	"errors"
	"testing"
	"time"

	"venera-server/internal/v2crypto"
	"venera-server/internal/v2domain"
)

func TestEnrollmentIdempotencyRevisionAndRevoke(t *testing.T) {
	test := newTestRepository(t)
	code, err := test.repo.CreateEnrollmentCode(context.Background(), time.Hour)
	if err != nil {
		t.Fatalf("create enrollment code: %v", err)
	}
	token := "enrollment-token"
	request := ClaimClientEnrollmentRequest{
		PendingClientID:      "pending-client-1",
		TokenDigest:          v2crypto.CredentialDigest(test.repo.Keys().CredentialHMAC, token),
		EnrollmentCodeDigest: test.repo.EnrollmentCodeDigest(code.Code),
		DisplayName:          "Reader", Platform: "desktop", AppVersion: "1.0.0", IdempotencyKey: "idem-claim-1",
	}
	first, err := test.repo.ClaimClientEnrollment(context.Background(), request)
	if err != nil {
		t.Fatalf("claim enrollment: %v", err)
	}
	replay, err := test.repo.ClaimClientEnrollment(context.Background(), request)
	if err != nil {
		t.Fatalf("replay enrollment: %v", err)
	}
	if replay.Client.ID != first.Client.ID || replay.InitialCursor != first.InitialCursor {
		t.Fatalf("replay changed enrollment result: first=%+v replay=%+v", first, replay)
	}
	conflict := request
	conflict.DisplayName = "different"
	if _, err := test.repo.ClaimClientEnrollment(context.Background(), conflict); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting enrollment error = %v, want idempotency conflict", err)
	}
	if _, err := test.repo.GetClient(context.Background(), string(first.Client.ID)); err != nil {
		t.Fatalf("get enrolled client: %v", err)
	}
	if err := test.repo.RevokeClient(context.Background(), string(first.Client.ID), first.Client.Revision+1, "revoke-wrong"); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("wrong revoke revision error = %v, want revision conflict", err)
	}
	if err := test.repo.RevokeClient(context.Background(), string(first.Client.ID), first.Client.Revision, "revoke-1"); err != nil {
		t.Fatalf("revoke client: %v", err)
	}
	if _, err := test.repo.GetClientByTokenDigest(context.Background(), request.TokenDigest); !errors.Is(err, ErrAuthenticationFailed) {
		t.Fatalf("revoked token lookup error = %v, want authentication failure", err)
	}
}

func TestExpiredEnrollmentCodeIsNotClaimable(t *testing.T) {
	test := newTestRepository(t)
	code, err := test.repo.CreateEnrollmentCode(context.Background(), time.Minute)
	if err != nil {
		t.Fatalf("create enrollment code: %v", err)
	}
	test.now = test.now.Add(2 * time.Minute)
	_, err = test.repo.ClaimClientEnrollment(context.Background(), ClaimClientEnrollmentRequest{
		PendingClientID: "pending-expired", TokenDigest: "digest-expired",
		EnrollmentCodeDigest: test.repo.EnrollmentCodeDigest(code.Code), IdempotencyKey: "expired-claim",
	})
	if !errors.Is(err, ErrEnrollmentCodeExpired) {
		t.Fatalf("expired enrollment error = %v, want expired", err)
	}
	var count int
	if err := test.db.SQL().QueryRow("SELECT COUNT(*) FROM client_installations").Scan(&count); err != nil {
		t.Fatalf("count clients: %v", err)
	}
	if count != 0 {
		t.Fatalf("expired code created %d clients", count)
	}
}

func TestInventoryReplaceExpectedRevisionAndOverreach(t *testing.T) {
	test := newTestRepository(t)
	clientA := test.enroll(t, "A")
	clientB := test.enroll(t, "B")
	request := ReplaceClientInventoryRequest{
		ClientID: string(clientA.ID), ExpectedRevision: 0,
		Entries:        []v2domain.InventoryEntry{{ArtifactID: "artifact-manwa", PackageReleaseID: "release-manwa-1", ManagementMode: "managed", CompatibilityState: v2domain.CompatibilityCompatible, CoreHash: "core-hash", ClientExtensionsJSON: "{}", ObservationContractID: "manwa-observation-v1", AccountObservationContractID: "manwa-account-observation-v1", AccountProbeContractID: "manwa-account-probe-v1"}},
		IdempotencyKey: "inventory-a-1",
	}
	result, err := test.repo.ReplaceClientInventory(context.Background(), request)
	if err != nil {
		t.Fatalf("replace client A inventory: %v", err)
	}
	if result.Revision != 1 {
		t.Fatalf("inventory revision = %d, want 1", result.Revision)
	}
	if _, err := test.repo.ReplaceClientInventory(context.Background(), request); err != nil {
		t.Fatalf("inventory replay: %v", err)
	}
	wrongClient := request
	wrongClient.ClientID = string(clientB.ID)
	wrongClient.IdempotencyKey = "inventory-b-overreach"
	if _, err := test.repo.ReplaceClientInventory(context.Background(), wrongClient); err != nil {
		// This is a legitimate separate client inventory update; the repository
		// must scope it to B rather than reject it as A's data.
		t.Fatalf("independent client inventory update: %v", err)
	}
	wrongRevision := request
	wrongRevision.ExpectedRevision = 0
	wrongRevision.IdempotencyKey = "inventory-a-wrong-revision"
	wrongRevision.Entries = nil
	if _, err := test.repo.ReplaceClientInventory(context.Background(), wrongRevision); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("wrong inventory revision error = %v, want revision conflict", err)
	}
}
