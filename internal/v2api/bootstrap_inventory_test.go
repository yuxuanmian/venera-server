package v2api

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"venera-server/internal/v2crypto"
	"venera-server/internal/v2store"
)

func TestBootstrapInventoryAndSourceStateRoutes(t *testing.T) {
	fixture := newAPIFixture(t)
	if err := fixture.repo.UpsertSourceArtifact(context.Background(), v2store.SourceArtifactInput{
		ArtifactID: "artifact-api", SourceKey: "api", CatalogID: "catalog-api", ManagedState: "active",
	}); err != nil {
		t.Fatalf("seed API artifact: %v", err)
	}
	if err := fixture.repo.UpsertSourcePackageRelease(context.Background(), v2store.SourcePackageReleaseInput{
		PackageReleaseID: "release-api", ArtifactID: "artifact-api", CatalogID: "catalog-api", CatalogSequence: 1,
		CoreHash: "core-api", MarkerSchemesJSON: "[]", PackageJSON: "{}", State: "active",
	}); err != nil {
		t.Fatalf("seed API release: %v", err)
	}
	code, err := fixture.repo.CreateEnrollmentCode(context.Background(), time.Hour)
	if err != nil {
		t.Fatalf("create API enrollment: %v", err)
	}
	token, err := v2crypto.GenerateBearerToken()
	if err != nil {
		t.Fatalf("generate API token: %v", err)
	}
	claim := apiCall(t, fixture.router, http.MethodPost, "/v2/onboarding/claim-enrollment", map[string]any{
		"pendingClientId": "pending-bootstrap", "enrollmentCode": code.Code,
	}, token, "claim-bootstrap", "2")
	if claim.Code != http.StatusCreated {
		t.Fatalf("claim bootstrap client: %d %s", claim.Code, claim.Body.String())
	}
	inventory := map[string]any{
		"expectedInventoryRevision": 0,
		"entries": []any{map[string]any{
			"artifactId": "artifact-api", "managementMode": "managed", "packageReleaseId": "release-api", "coreHash": "core-api",
			"clientExtensionHashes": map[string]string{}, "observationContractId": "obs", "accountObservationContractId": "aobs", "accountProbeContractId": "probe",
		}},
	}
	updated := apiCall(t, fixture.router, http.MethodPut, "/v2/client/source-inventory", inventory, token, "inventory-bootstrap", "2")
	if updated.Code != http.StatusOK || !strings.Contains(updated.Body.String(), `"compatibilityState":"unknown"`) {
		t.Fatalf("inventory response = %d %s", updated.Code, updated.Body.String())
	}
	replay := apiCall(t, fixture.router, http.MethodPut, "/v2/client/source-inventory", inventory, token, "inventory-bootstrap", "2")
	if replay.Code != http.StatusOK {
		t.Fatalf("inventory replay = %d %s", replay.Code, replay.Body.String())
	}
	conflict := map[string]any{"expectedInventoryRevision": 0, "entries": []any{}}
	conflictResponse := apiCall(t, fixture.router, http.MethodPut, "/v2/client/source-inventory", conflict, token, "inventory-bootstrap", "2")
	if conflictResponse.Code != http.StatusConflict || !strings.Contains(conflictResponse.Body.String(), `"code":"idempotency_conflict"`) {
		t.Fatalf("inventory conflict = %d %s", conflictResponse.Code, conflictResponse.Body.String())
	}
	bootstrap := apiCall(t, fixture.router, http.MethodGet, "/v2/bootstrap", nil, token, "", "2")
	if bootstrap.Code != http.StatusOK || !strings.Contains(bootstrap.Body.String(), `"packageReleaseId":"release-api"`) || !strings.Contains(bootstrap.Body.String(), `"cursor":"v2.`) {
		t.Fatalf("bootstrap response = %d %s", bootstrap.Code, bootstrap.Body.String())
	}
	states := apiCall(t, fixture.router, http.MethodGet, "/v2/client/source-states", nil, token, "", "2")
	if states.Code != http.StatusOK || !strings.Contains(states.Body.String(), `"artifactId":"artifact-api"`) || !strings.Contains(states.Body.String(), `"claimState":"absent"`) {
		t.Fatalf("source states response = %d %s", states.Code, states.Body.String())
	}
}
