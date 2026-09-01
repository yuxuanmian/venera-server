package v2store

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"venera-server/internal/v2crypto"
	"venera-server/internal/v2domain"
)

type testRepository struct {
	repo      *Repository
	db        *DB
	now       time.Time
	clientSeq int
}

func newTestRepository(t *testing.T) *testRepository {
	t.Helper()
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	root := bytes.Repeat([]byte{0x41}, v2crypto.RootSecretSize)
	keys, err := v2crypto.DeriveKeys(root)
	if err != nil {
		t.Fatalf("derive test keys: %v", err)
	}
	test := &testRepository{now: now}
	db, err := OpenWithOptions(filepath.Join(t.TempDir(), "v2.db"), Options{Clock: func() time.Time { return test.now }})
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	test.db = db
	test.repo = NewRepository(db, keys)
	if err := test.repo.UpsertSourceArtifact(context.Background(), SourceArtifactInput{
		ArtifactID: "artifact-manwa", SourceKey: "manwa", CatalogID: "catalog-test", ManagedState: "active",
	}); err != nil {
		t.Fatalf("seed artifact: %v", err)
	}
	if err := test.repo.UpsertSourcePackageRelease(context.Background(), SourcePackageReleaseInput{
		PackageReleaseID: "release-manwa-1", ArtifactID: "artifact-manwa", CatalogID: "catalog-test",
		CatalogSequence: 1, CoreHash: "core-hash", ScanningExtensionHash: "scan-hash",
		ObservationContractID: "manwa-observation-v1", AccountObservationContractID: "manwa-account-observation-v1",
		AccountProbeContractID: "manwa-account-probe-v1", MarkerSchemesJSON: "[\"manwa:level:2\"]",
		SessionExportProfileID: "manwa-cookie-v1", PackageJSON: "{}", State: "active",
	}); err != nil {
		t.Fatalf("seed release: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return test
}

func (test *testRepository) enroll(t *testing.T, label string) v2domain.Client {
	t.Helper()
	code, err := test.repo.CreateEnrollmentCode(context.Background(), time.Hour)
	if err != nil {
		t.Fatalf("create enrollment code %s: %v", label, err)
	}
	test.clientSeq++
	token := "test-token-" + label
	result, err := test.repo.ClaimClientEnrollment(context.Background(), ClaimClientEnrollmentRequest{
		PendingClientID:      "pending-" + label,
		TokenDigest:          v2crypto.CredentialDigest(test.repo.Keys().CredentialHMAC, token),
		EnrollmentCodeDigest: test.repo.EnrollmentCodeDigest(code.Code),
		DisplayName:          label, Platform: "test", AppVersion: "1.0.0", IdempotencyKey: "claim-" + label,
	})
	if err != nil {
		t.Fatalf("claim enrollment %s: %v", label, err)
	}
	return result.Client
}

func stageCandidate(t *testing.T, test *testRepository, client v2domain.Client, label, identity string) (SessionCandidate, string) {
	t.Helper()
	identityDigest := v2crypto.IdentityDigest(test.repo.Keys().IdentityHMAC, "artifact-manwa", "manwa-username-v1", identity)
	sessionPlaintext := []byte(`{"cookies":[{"name":"session","value":"` + label + `"}]}`)
	sessionEnvelope, err := v2crypto.SealSession(test.repo.Keys().SessionAEAD, sessionPlaintext, []byte("candidate:"+string(client.ID)+":artifact-manwa"))
	if err != nil {
		t.Fatalf("seal session %s: %v", label, err)
	}
	candidate, err := test.repo.StageSessionCandidate(context.Background(), StageSessionCandidateRequest{
		ClientID: string(client.ID), ArtifactID: "artifact-manwa", PackageReleaseID: "release-manwa-1",
		ExportProfileID: "manwa-cookie-v1", SessionEnvelope: sessionEnvelope,
		SessionDigest: v2crypto.Digest(test.repo.Keys().CredentialHMAC, "session-candidate-v1\x00"+string(sessionPlaintext)),
		TTL:           time.Hour, IdempotencyKey: "stage-" + label,
	})
	if err != nil {
		t.Fatalf("stage candidate %s: %v", label, err)
	}
	identityCiphertext, err := v2crypto.SealSession(test.repo.Keys().SessionAEAD, []byte(identity), []byte("candidate-identity:"+string(client.ID)+":"+candidate.ID))
	if err != nil {
		t.Fatalf("seal identity %s: %v", label, err)
	}
	completed, err := test.repo.CompleteCandidateProbe(context.Background(), CompleteCandidateProbeRequest{
		ClientID: string(client.ID), CandidateID: candidate.ID, ExpectedRevision: candidate.Revision,
		IdentityScheme: "manwa-username-v1", IdentityCiphertext: identityCiphertext, IdentityDigest: identityDigest,
		IdentityDisplay: identity, AttributesJSON: `{"accountLevel":2}`, VisibilityScope: "manwa:level:2",
		ScopeFreshUntil: test.now.Add(time.Hour), IdempotencyKey: "probe-" + label,
	})
	if err != nil {
		t.Fatalf("complete candidate %s: %v", label, err)
	}
	return completed, identity
}
