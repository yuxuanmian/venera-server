package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	legacyStore "venera-server/internal/store"
	"venera-server/internal/tracking/domain"
	trackingstore "venera-server/internal/tracking/store"
)

const diagnosticsRevision = "0123456789abcdef0123456789abcdef01234567"
const diagnosticsOldRevision = "89abcdef0123456789abcdef0123456789abcdef"

func TestDiagnosticsRequiresAdminAndRedactsObservationPayloads(t *testing.T) {
	legacy, err := legacyStore.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	repository, err := trackingstore.NewRepository(legacy.DB())
	if err != nil {
		t.Fatal(err)
	}
	artifact := domain.ArtifactIdentity{SourceKey: "manwa", FileName: "manwa.js"}
	authority := domain.Authority{
		CatalogID:      "owner/catalog",
		ActiveRevision: diagnosticsRevision,
		Generation:     2,
		Artifacts:      []domain.ArtifactIdentity{artifact},
	}
	if _, err := repository.ReplaceClientState(context.Background(), domain.ClientState{
		DeviceID:     "device-1",
		CloudEnabled: true,
		Interests: []domain.Interest{{
			Artifact: artifact,
			ComicID:  "comic-private-42",
		}},
	}, authority); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(-time.Minute)
	oldAuthority := domain.Authority{
		CatalogID:      "owner/catalog",
		ActiveRevision: diagnosticsOldRevision,
		Generation:     1,
		Artifacts:      []domain.ArtifactIdentity{artifact},
	}
	if err := repository.ReplaceObservations(context.Background(), "old-user", artifact, diagnosticsOldRevision, 1, []domain.Observation{{
		Artifact:          artifact,
		Revision:          diagnosticsOldRevision,
		ComicID:           "old-private-comic",
		ObservedAt:        now,
		ValidUntil:        now.Add(time.Hour),
		RuntimeGeneration: 1,
	}}, oldAuthority); err != nil {
		t.Fatal(err)
	}
	if err := repository.ReplaceObservations(context.Background(), "user-1", artifact, diagnosticsRevision, 2, []domain.Observation{{
		Artifact:          artifact,
		Revision:          diagnosticsRevision,
		ComicID:           "comic-private-42",
		ObservedAt:        now,
		ValidUntil:        now.Add(time.Hour),
		RuntimeGeneration: 2,
		FavoriteUpdate: domain.FavoriteUpdate{
			Marker:   stringPointer("opaque-marker"),
			Metadata: map[string]any{"token": "secret-token", "safe": "ok"},
		},
	}}, authority); err != nil {
		t.Fatal(err)
	}

	admin := false
	handler, err := NewDiagnosticsHandler(DiagnosticsDependencies{
		Repository:           repository,
		Authority:            func() (domain.Authority, bool) { return authority, true },
		GenerationRejections: func() uint64 { return 3 },
		Runtime: func() DiagnosticsRuntime {
			return DiagnosticsRuntime{
				DemandCount:     1,
				JobCount:        2,
				CheckpointCount: 1,
			}
		},
		Admin: func(context.Context) bool { return admin },
	})
	if err != nil {
		t.Fatal(err)
	}
	denied := httptest.NewRecorder()
	handler.ServeHTTP(denied, httptest.NewRequest(http.MethodGet, "/admin/api/tracking/diagnostics", nil))
	if denied.Code != http.StatusForbidden {
		t.Fatalf("denied status = %d", denied.Code)
	}

	admin = true
	allowed := httptest.NewRecorder()
	handler.ServeHTTP(allowed, httptest.NewRequest(http.MethodGet, "/admin/api/tracking/diagnostics", nil))
	if allowed.Code != http.StatusOK {
		t.Fatalf("allowed status = %d body=%s", allowed.Code, allowed.Body.String())
	}
	body := allowed.Body.String()
	if !strings.Contains(body, `"effectiveInterestCount":1`) || !strings.Contains(body, `"freshObservationCount":1`) {
		t.Fatalf("diagnostic counts missing: %s", body)
	}
	if !strings.Contains(body, `"generationRejections":3`) || !strings.Contains(body, `"demandCount":1`) {
		t.Fatalf("runtime diagnostics missing: %s", body)
	}
	if !strings.Contains(body, `"oldRevisionCount":1`) || !strings.Contains(body, `"oldGenerationCount":1`) {
		t.Fatalf("exclusion diagnostics missing: %s", body)
	}
	if strings.Contains(body, "secret-token") || strings.Contains(body, "comic-private-42") || strings.Contains(body, "opaque-marker") {
		t.Fatalf("private payload leaked: %s", body)
	}
}

func stringPointer(value string) *string { return &value }
