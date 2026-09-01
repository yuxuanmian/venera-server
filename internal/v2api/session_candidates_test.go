package v2api

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"venera-server/internal/v2crypto"
	"venera-server/internal/v2domain"
	"venera-server/internal/v2manifest"
	"venera-server/internal/v2scan"
	"venera-server/internal/v2store"
)

type fakeAccountProbe struct {
	Identity string
	Err      error
}

func (probe *fakeAccountProbe) Probe(context.Context, v2scan.ProbeRequest) (v2scan.ProbeResult, error) {
	if probe.Err != nil {
		return v2scan.ProbeResult{}, probe.Err
	}
	return v2scan.ProbeResult{
		IdentityScheme: "manwa-username-v1", IdentityValue: probe.Identity,
		DisplayName: probe.Identity, AttributesJSON: `{"accountLevel":2}`,
		VisibilityScope: "manwa:level:2",
	}, nil
}

func TestSourceAccountListAndSameIdentityReuse(t *testing.T) {
	fixture := newAPIFixture(t)
	pkg := seedStableManwa(t, fixture)
	clientA, tokenA := enrollCompatibleClient(t, fixture, "candidate-client-a", pkg)
	clientB, tokenB := enrollCompatibleClient(t, fixture, "candidate-client-b", pkg)
	fixture.router.SetAccountProbeRunner(&fakeAccountProbe{Identity: "shared-user"})

	first := postCandidate(t, fixture.router, tokenA, "candidate-a", candidateBody(pkg.PackageReleaseID, "session-a"))
	if first.Code != http.StatusCreated {
		t.Fatalf("first candidate = %d %s", first.Code, first.Body.String())
	}
	firstData := decodeAPI(t, first)["data"].(map[string]any)
	firstSource := firstData["sourceAccount"].(map[string]any)
	firstID := firstSource["sourceAccountId"].(string)
	if firstSource["accountRevision"] != float64(1) || firstSource["linkRevision"] != float64(1) || firstSource["revision"] != float64(1) {
		t.Fatalf("source account revisions = %#v", firstSource)
	}
	if firstData["state"] != string(v2domain.CandidateActivated) || firstSource["identity"].(map[string]any)["display"] != "shared-user" {
		t.Fatalf("first candidate data = %#v", firstData)
	}

	second := postCandidate(t, fixture.router, tokenB, "candidate-b", candidateBody(pkg.PackageReleaseID, "session-b"))
	if second.Code != http.StatusCreated {
		t.Fatalf("second candidate = %d %s", second.Code, second.Body.String())
	}
	secondID := decodeAPI(t, second)["data"].(map[string]any)["sourceAccount"].(map[string]any)["sourceAccountId"].(string)
	if secondID != firstID {
		t.Fatalf("same identity accounts = %s and %s", firstID, secondID)
	}
	list := apiCall(t, fixture.router, http.MethodGet, "/v2/source-accounts?artifactId=manwa", nil, tokenA, "", "2")
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), firstID) || strings.Contains(list.Body.String(), "identityDigest") {
		t.Fatalf("source account list = %d %s", list.Code, list.Body.String())
	}
	var accountCount int
	if err := fixture.db.SQL().QueryRow(`SELECT COUNT(*) FROM source_accounts WHERE artifact_id = 'manwa'`).Scan(&accountCount); err != nil {
		t.Fatal(err)
	}
	if accountCount != 1 {
		t.Fatalf("source account count = %d, want 1", accountCount)
	}
	_ = clientA
	_ = clientB
}

func TestCandidateValidationCrossClientAccessAndSecretCleanup(t *testing.T) {
	fixture := newAPIFixture(t)
	pkg := seedStableManwa(t, fixture)
	_, tokenA := enrollCompatibleClient(t, fixture, "candidate-owner", pkg)
	_, tokenB := enrollCompatibleClient(t, fixture, "candidate-other", pkg)

	badDomain := candidateBody(pkg.PackageReleaseID, "bad-domain")
	badDomain["session"].(map[string]any)["cookies"].([]any)[0].(map[string]any)["domain"] = "evil.test"
	bad := postCandidate(t, fixture.router, tokenA, "candidate-bad", badDomain)
	if bad.Code != http.StatusBadRequest || strings.Contains(bad.Body.String(), "evil.test") {
		t.Fatalf("bad candidate = %d %s", bad.Code, bad.Body.String())
	}
	unknown := candidateBody(pkg.PackageReleaseID, "unknown-field")
	unknown["unexpected"] = "reject"
	unknownResponse := postCandidate(t, fixture.router, tokenA, "candidate-unknown", unknown)
	if unknownResponse.Code != http.StatusBadRequest {
		t.Fatalf("unknown candidate field = %d %s", unknownResponse.Code, unknownResponse.Body.String())
	}

	created := postCandidate(t, fixture.router, tokenA, "candidate-pending", candidateBody(pkg.PackageReleaseID, "pending"))
	if created.Code != http.StatusAccepted {
		t.Fatalf("pending candidate = %d %s", created.Code, created.Body.String())
	}
	data := decodeAPI(t, created)["data"].(map[string]any)
	candidateID := data["candidateId"].(string)
	if cross := apiCall(t, fixture.router, http.MethodGet, "/v2/source-accounts/session-candidates/"+candidateID, nil, tokenB, "", "2"); cross.Code != http.StatusNotFound {
		t.Fatalf("cross-client candidate access = %d %s", cross.Code, cross.Body.String())
	}
	cancel := apiCall(t, fixture.router, http.MethodDelete, "/v2/source-accounts/session-candidates/"+candidateID, map[string]any{"expectedCandidateRevision": 1}, tokenA, "cancel-pending", "2")
	if cancel.Code != http.StatusOK || !strings.Contains(cancel.Body.String(), `"state":"cancelled"`) {
		t.Fatalf("cancel candidate = %d %s", cancel.Code, cancel.Body.String())
	}
	assertCandidateSecretsCleared(t, fixture.db, candidateID)

	expiring := postCandidate(t, fixture.router, tokenA, "candidate-expiring", candidateBody(pkg.PackageReleaseID, "expiring"))
	if expiring.Code != http.StatusAccepted {
		t.Fatalf("expiring candidate = %d %s", expiring.Code, expiring.Body.String())
	}
	expiringID := decodeAPI(t, expiring)["data"].(map[string]any)["candidateId"].(string)
	fixture.now = fixture.now.Add(16 * time.Minute)
	expired := apiCall(t, fixture.router, http.MethodGet, "/v2/source-accounts/session-candidates/"+expiringID, nil, tokenA, "", "2")
	if expired.Code != http.StatusOK || !strings.Contains(expired.Body.String(), `"state":"expired"`) {
		t.Fatalf("expired candidate = %d %s", expired.Code, expired.Body.String())
	}
	assertCandidateSecretsCleared(t, fixture.db, expiringID)
}

func TestDifferentIdentityRequiresClientScopedSwitchConfirmation(t *testing.T) {
	fixture := newAPIFixture(t)
	pkg := seedStableManwa(t, fixture)
	_, token := enrollCompatibleClient(t, fixture, "switch-client", pkg)
	probe := &fakeAccountProbe{Identity: "first-user"}
	fixture.router.SetAccountProbeRunner(probe)
	first := postCandidate(t, fixture.router, token, "switch-first", candidateBody(pkg.PackageReleaseID, "first"))
	if first.Code != http.StatusCreated {
		t.Fatalf("first switch candidate = %d %s", first.Code, first.Body.String())
	}
	firstAccount := decodeAPI(t, first)["data"].(map[string]any)["sourceAccount"].(map[string]any)["sourceAccountId"].(string)

	probe.Identity = "second-user"
	second := postCandidate(t, fixture.router, token, "switch-second", candidateBody(pkg.PackageReleaseID, "second"))
	if second.Code != http.StatusConflict || !strings.Contains(second.Body.String(), `"code":"switch_confirmation_required"`) {
		t.Fatalf("unconfirmed switch = %d %s", second.Code, second.Body.String())
	}
	secondError := decodeAPI(t, second)["error"].(map[string]any)
	secondDetails := secondError["details"].(map[string]any)
	candidateID := secondDetails["candidateId"].(string)
	candidateRevision := int64(secondDetails["revision"].(float64))
	if candidateRevision != 3 {
		t.Fatalf("switch candidate revision = %d, want 3", candidateRevision)
	}
	selected, err := fixture.repo.GetSelectedClientSourceAccount(context.Background(), clientIDForToken(t, fixture, token), "manwa")
	if err != nil {
		t.Fatal(err)
	}
	confirm := apiCall(t, fixture.router, http.MethodPost, "/v2/source-accounts/session-candidates/"+candidateID+"/confirm-link", map[string]any{
		"expectedCandidateRevision": candidateRevision, "expectedCurrentLinkRevision": int64(selected.Revision),
	}, token, "confirm-switch", "2")
	if confirm.Code != http.StatusOK {
		t.Fatalf("confirm switch = %d %s", confirm.Code, confirm.Body.String())
	}
	confirmedAccount := decodeAPI(t, confirm)["data"].(map[string]any)["sourceAccount"].(map[string]any)["sourceAccountId"].(string)
	if confirmedAccount == firstAccount {
		t.Fatal("confirmed switch kept the old account")
	}
	accounts, err := fixture.repo.ListClientSourceAccounts(context.Background(), clientIDForToken(t, fixture, token))
	if err != nil {
		t.Fatal(err)
	}
	linked := 0
	selectedCount := 0
	for _, account := range accounts {
		if account.LinkState == v2domain.LinkLinked {
			linked++
		}
		if account.Selected {
			selectedCount++
		}
	}
	if linked != 2 || selectedCount != 1 {
		t.Fatalf("account links after switch = linked:%d selected:%d, accounts=%+v", linked, selectedCount, accounts)
	}
}

func TestActiveClaimBlocksCandidateSwitch(t *testing.T) {
	fixture := newAPIFixture(t)
	pkg := seedStableManwa(t, fixture)
	clientID, token := enrollCompatibleClient(t, fixture, "claim-switch-client", pkg)
	probe := &fakeAccountProbe{Identity: "claim-first"}
	fixture.router.SetAccountProbeRunner(probe)
	first := postCandidate(t, fixture.router, token, "claim-first-candidate", candidateBody(pkg.PackageReleaseID, "claim-first"))
	if first.Code != http.StatusCreated {
		t.Fatalf("claim first candidate = %d %s", first.Code, first.Body.String())
	}
	firstData := decodeAPI(t, first)["data"].(map[string]any)
	account := firstData["sourceAccount"].(map[string]any)
	accountID := account["sourceAccountId"].(string)
	selected, err := fixture.repo.GetSelectedClientSourceAccount(context.Background(), clientID, "manwa")
	if err != nil {
		t.Fatal(err)
	}
	prep, err := fixture.repo.CreateCloudPreparation(context.Background(), v2store.CreateCloudPreparationRequest{
		ClientID: clientID, ArtifactID: "manwa", ExpectedSourceAccountRevision: v2domain.Revision(1),
		ExpectedLinkRevision: selected.Revision, ExpectedInventoryRevision: 1, TTL: time.Hour, IdempotencyKey: "claim-prep",
	})
	if err != nil {
		t.Fatal(err)
	}
	ready, err := fixture.repo.MarkPreparationSnapshotReady(context.Background(), v2store.MarkPreparationSnapshotReadyRequest{
		ClientID: clientID, PreparationID: prep.ID, ExpectedRevision: prep.Revision, SnapshotDigest: "sha256:claim-snapshot", IdempotencyKey: "claim-ready",
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := fixture.repo.CommitCloudClaim(context.Background(), v2store.CommitCloudClaimRequest{
		ClientID: clientID, PreparationID: ready.ID, ExpectedRevision: ready.Revision, SnapshotReceiptID: ready.SnapshotReceiptID,
		SnapshotDigest: "sha256:claim-snapshot", IdempotencyKey: "claim-commit",
	})
	if err != nil {
		t.Fatal(err)
	}
	if claim.SourceAccountID != accountID {
		t.Fatal("test claim selected a different account")
	}

	probe.Identity = "claim-second"
	blocked := postCandidate(t, fixture.router, token, "claim-second-candidate", candidateBody(pkg.PackageReleaseID, "claim-second"))
	if blocked.Code != http.StatusConflict || !strings.Contains(blocked.Body.String(), `"code":"cloud_claim_must_be_removed"`) {
		t.Fatalf("claim-blocked switch = %d %s", blocked.Code, blocked.Body.String())
	}
}

func seedStableManwa(t *testing.T, fixture *apiFixture) v2manifest.SourcePackage {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Join(filepath.Dir(file), "..", "..", "..", "venera-configs")
	raw, err := os.ReadFile(filepath.Join(root, "index-v2.json"))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := v2manifest.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	manifest.SourcePackages[0].Status = "stable"
	canonical, err := v2manifest.CanonicalJSON(manifest)
	if err != nil {
		t.Fatal(err)
	}
	hash, canonical, err := v2manifest.HashJSON(canonical)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.repo.ActivateManifestCatalog(context.Background(), v2store.ManifestCatalogInput{
		ManifestID: "manifest-test", CatalogID: manifest.CatalogID, ManifestURL: "https://catalog.test/index-v2.json",
		CatalogSequence: manifest.CatalogSequence, ManifestHash: hash, ManifestJSON: string(canonical), IdempotencyKey: "manifest-test",
	}); err != nil {
		t.Fatal(err)
	}
	pkg := manifest.SourcePackages[0]
	if err := fixture.repo.UpsertSourceArtifact(context.Background(), v2store.SourceArtifactInput{ArtifactID: pkg.ArtifactID, SourceKey: pkg.SourceKey, CatalogID: manifest.CatalogID, ManagedState: pkg.ManagedState}); err != nil {
		t.Fatal(err)
	}
	markerJSON, _ := json.Marshal(pkg.Tracking.MarkerSchemes)
	packageJSON, _ := v2manifest.CanonicalPackageJSON(pkg)
	if err := fixture.repo.UpsertSourcePackageRelease(context.Background(), v2store.SourcePackageReleaseInput{
		PackageReleaseID: pkg.PackageReleaseID, ArtifactID: pkg.ArtifactID, CatalogID: manifest.CatalogID, CatalogSequence: manifest.CatalogSequence,
		CoreHash: pkg.Core.SHA256, ScanningExtensionHash: pkg.Extensions[0].SHA256, ObservationContractID: pkg.Tracking.ObservationContractID,
		AccountObservationContractID: pkg.Tracking.AccountObservationContractID, AccountProbeContractID: pkg.AccountProbeContract.ID,
		MarkerSchemesJSON: string(markerJSON), SessionExportProfileID: pkg.SessionExportProfile.ID, PackageJSON: string(packageJSON), State: "active",
	}); err != nil {
		t.Fatal(err)
	}
	return pkg
}

func enrollCompatibleClient(t *testing.T, fixture *apiFixture, pending string, pkg v2manifest.SourcePackage) (string, string) {
	t.Helper()
	code, err := fixture.repo.CreateEnrollmentCode(context.Background(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	token, err := v2crypto.GenerateBearerToken()
	if err != nil {
		t.Fatal(err)
	}
	claim := apiCall(t, fixture.router, http.MethodPost, "/v2/onboarding/claim-enrollment", map[string]any{"pendingClientId": pending, "enrollmentCode": code.Code}, token, "claim-"+pending, "2")
	if claim.Code != http.StatusCreated {
		t.Fatalf("enroll %s = %d %s", pending, claim.Code, claim.Body.String())
	}
	data := decodeAPI(t, claim)["data"].(map[string]any)
	clientID := data["client"].(map[string]any)["clientId"].(string)
	entry := map[string]any{
		"artifactId": pkg.ArtifactID, "managementMode": "managed", "packageReleaseId": pkg.PackageReleaseID,
		"coreHash": pkg.Core.SHA256, "clientExtensionHashes": map[string]string{},
		"observationContractId": pkg.Tracking.ObservationContractID, "accountObservationContractId": pkg.Tracking.AccountObservationContractID,
		"accountProbeContractId": pkg.AccountProbeContract.ID, "markerSchemes": pkg.Tracking.MarkerSchemes,
	}
	inventory := apiCall(t, fixture.router, http.MethodPut, "/v2/client/source-inventory", map[string]any{"expectedInventoryRevision": 0, "entries": []any{entry}}, token, "inventory-"+pending, "2")
	if inventory.Code != http.StatusOK || !strings.Contains(inventory.Body.String(), `"compatibilityState":"compatible"`) {
		t.Fatalf("inventory %s = %d %s", pending, inventory.Code, inventory.Body.String())
	}
	return clientID, token
}

func candidateBody(release, value string) map[string]any {
	return map[string]any{
		"artifactId": "manwa", "packageReleaseId": release, "exportProfileId": "manwa-cookie-v1",
		"session": map[string]any{"cookies": []any{map[string]any{"name": "session", "value": value, "domain": "manwa.me", "path": "/", "secure": true}}},
	}
}

func postCandidate(t *testing.T, router http.Handler, token, idempotency string, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	return apiCall(t, router, http.MethodPost, "/v2/source-accounts/session-candidates", body, token, idempotency, "2")
}

func assertCandidateSecretsCleared(t *testing.T, db *v2store.DB, candidateID string) {
	t.Helper()
	var sessionEnvelope, identityCiphertext []byte
	err := db.SQL().QueryRow(`SELECT session_envelope, probed_identity_ciphertext FROM source_session_candidates WHERE candidate_id = ?`, candidateID).Scan(&sessionEnvelope, &identityCiphertext)
	if err != nil && err != sql.ErrNoRows {
		t.Fatal(err)
	}
	if err == nil && (len(sessionEnvelope) != 0 || len(identityCiphertext) != 0) {
		t.Fatalf("candidate %s retained ciphertext", candidateID)
	}
}

func clientIDForToken(t *testing.T, fixture *apiFixture, token string) string {
	t.Helper()
	digest := v2crypto.CredentialDigest(fixture.repo.Keys().CredentialHMAC, token)
	client, err := fixture.repo.GetClientByTokenDigest(context.Background(), digest)
	if err != nil {
		t.Fatal(err)
	}
	return string(client.ID)
}
