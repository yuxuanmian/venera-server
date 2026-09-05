package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"venera-server/internal/tracking/domain"
	trackingstore "venera-server/internal/tracking/store"

	_ "modernc.org/sqlite"
)

const apiRevision = "0123456789abcdef0123456789abcdef01234567"

func newTestAPI(t *testing.T, limit ...int) (http.Handler, *trackingstore.Repository, domain.Authority) {
	t.Helper()
	db, err := sql.Open("sqlite", fmt.Sprintf("file:tracking-api-%s?mode=memory&cache=shared", t.Name()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	repository, err := trackingstore.NewRepository(db, limit...)
	if err != nil {
		t.Fatal(err)
	}
	authority := domain.Authority{
		CatalogID: "yuxuanmian/venera-configs", ActiveRevision: apiRevision, Generation: 1,
		Artifacts: []domain.ArtifactIdentity{{SourceKey: "manwa", FileName: "manwa.js"}},
	}
	if err := repository.SetCatalogState(context.Background(), trackingstore.CatalogState{
		CatalogID: authority.CatalogID, ActiveRevision: authority.ActiveRevision,
		Generation: authority.Generation, ActivatedAt: time.Now().UTC(), Digest: "authority-digest",
	}); err != nil {
		t.Fatal(err)
	}
	router, err := NewRouter(Dependencies{
		Repository: repository,
		Authority:  func() (domain.Authority, bool) { return authority, true },
		Principal:  func(context.Context) (string, string) { return "user-1", "device-1" },
	})
	if err != nil {
		t.Fatal(err)
	}
	return router, repository, authority
}

func apiRequest(t *testing.T, handler http.Handler, method, path string, payload any) *httptest.ResponseRecorder {
	t.Helper()
	var body *bytes.Reader
	if payload == nil {
		body = bytes.NewReader(nil)
	} else {
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		body = bytes.NewReader(data)
	}
	req := httptest.NewRequest(method, path, body)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	return rr
}

func apiInterest(comicID string) map[string]any {
	return map[string]any{
		"artifact": map[string]any{"sourceKey": "manwa", "fileName": "manwa.js"},
		"comicId":  comicID,
	}
}

func apiObservation(userID, comicID string, update domain.FavoriteUpdate) domain.Observation {
	now := time.Now().UTC()
	return domain.Observation{
		UserID: userID, Artifact: domain.ArtifactIdentity{SourceKey: "manwa", FileName: "manwa.js"},
		Revision: apiRevision, ComicID: comicID, ObservedAt: now.Add(-time.Minute),
		ValidUntil: now.Add(time.Hour), RuntimeGeneration: 1, FavoriteUpdate: update,
	}
}

func TestTrackingAPIRequiresAuthAndReturnsAuthority(t *testing.T) {
	repository, err := trackingstore.NewRepository(openAPITestDB(t))
	if err != nil {
		t.Fatal(err)
	}
	unauthenticated, err := NewRouter(Dependencies{Repository: repository})
	if err != nil {
		t.Fatal(err)
	}
	if response := apiRequest(t, unauthenticated, http.MethodGet, "/api/tracking/authority", nil); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated authority request status=%d body=%s", response.Code, response.Body.String())
	}

	router, _, _ := newTestAPI(t)
	response := apiRequest(t, router, http.MethodGet, "/api/tracking/authority", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("authority status=%d body=%s", response.Code, response.Body.String())
	}
	var envelope map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	data := envelope["data"].(map[string]any)
	if data["activeRevision"] != apiRevision {
		t.Fatalf("unexpected authority: %+v", data)
	}
	if _, exists := data["generation"]; exists {
		t.Fatalf("internal generation leaked: %+v", data)
	}
}

func TestTrackingAPIClientStateAndETagSnapshot(t *testing.T) {
	router, repository, authority := newTestAPI(t)
	stateResponse := apiRequest(t, router, http.MethodPut, "/api/tracking/client-state", map[string]any{
		"cloudEnabled": true, "interests": []any{apiInterest("comic-1")},
	})
	if stateResponse.Code != http.StatusOK {
		t.Fatalf("client-state status=%d body=%s", stateResponse.Code, stateResponse.Body.String())
	}
	if err := repository.PutObservation(context.Background(), apiObservation("user-1", "comic-1", domain.FavoriteUpdate{SourceUnread: boolPtr(true)}), authority); err != nil {
		t.Fatal(err)
	}
	response := apiRequest(t, router, http.MethodGet, "/api/tracking/observations", nil)
	if response.Code != http.StatusOK || response.Header().Get("ETag") == "" {
		t.Fatalf("observations status=%d etag=%q body=%s", response.Code, response.Header().Get("ETag"), response.Body.String())
	}
	etag := response.Header().Get("ETag")
	if !strings.Contains(response.Body.String(), "comic-1") {
		t.Fatalf("observation missing: %s", response.Body.String())
	}
	request := httptest.NewRequest(http.MethodGet, "/api/tracking/observations", nil)
	request.Header.Set("If-None-Match", etag)
	unchanged := httptest.NewRecorder()
	router.ServeHTTP(unchanged, request)
	if unchanged.Code != http.StatusNotModified || unchanged.Header().Get("ETag") != etag {
		t.Fatalf("expected stable 304, status=%d etag=%q", unchanged.Code, unchanged.Header().Get("ETag"))
	}
}

func TestTrackingAPIBoundsEvidenceAndRejectsInvalidState(t *testing.T) {
	router, repository, authority := newTestAPI(t)
	stateResponse := apiRequest(t, router, http.MethodPut, "/api/tracking/client-state", map[string]any{
		"cloudEnabled": true, "interests": []any{apiInterest("comic-utf8")},
	})
	if stateResponse.Code != http.StatusOK {
		t.Fatalf("client-state status=%d body=%s", stateResponse.Code, stateResponse.Body.String())
	}
	exactMarker := strings.Repeat("😀", 1023) + "abcd"
	exactMetadata := map[string]any{"value": strings.Repeat("😀", 1021)}
	if err := repository.PutObservation(context.Background(), apiObservation("user-1", "comic-utf8", domain.FavoriteUpdate{Marker: &exactMarker, Metadata: exactMetadata}), authority); err != nil {
		t.Fatal(err)
	}
	if err := repository.PutObservation(context.Background(), apiObservation("user-1", "comic-too-large", domain.FavoriteUpdate{Marker: stringPtr(exactMarker + "e")}), authority); err == nil {
		t.Fatal("4097-byte marker was accepted")
	}
	if err := repository.PutObservation(context.Background(), apiObservation("user-1", "comic-too-large-meta", domain.FavoriteUpdate{Metadata: map[string]any{"value": strings.Repeat("😀", 1021) + "a"}}), authority); err == nil {
		t.Fatal("4097-byte metadata was accepted")
	}
	missing := apiRequest(t, router, http.MethodPut, "/api/tracking/client-state", map[string]any{"cloudEnabled": true})
	if missing.Code != http.StatusBadRequest {
		t.Fatalf("missing interests status=%d body=%s", missing.Code, missing.Body.String())
	}
	unknown := apiRequest(t, router, http.MethodPut, "/api/tracking/client-state", map[string]any{"cloudEnabled": true, "interests": []any{}, "markerScheme": "v1"})
	if unknown.Code != http.StatusBadRequest {
		t.Fatalf("unknown field status=%d body=%s", unknown.Code, unknown.Body.String())
	}
}

func TestTrackingAPIProvidesFullCapAndDeterministicOverflow(t *testing.T) {
	router, repository, authority := newTestAPI(t)
	interests := make([]any, 10000)
	observations := make([]domain.Observation, 10000)
	for index := range interests {
		comicID := fmt.Sprintf("comic-%05d", index)
		interests[index] = apiInterest(comicID)
		observations[index] = apiObservation("user-1", comicID, domain.FavoriteUpdate{SourceUnread: boolPtr(index%2 == 0)})
	}
	stateResponse := apiRequest(t, router, http.MethodPut, "/api/tracking/client-state", map[string]any{"cloudEnabled": true, "interests": interests})
	if stateResponse.Code != http.StatusOK {
		t.Fatalf("full client-state status=%d body=%s", stateResponse.Code, stateResponse.Body.String())
	}
	if err := repository.ReplaceObservations(context.Background(), "user-1", authority.Artifacts[0], apiRevision, 1, observations, authority); err != nil {
		t.Fatal(err)
	}
	response := apiRequest(t, router, http.MethodGet, "/api/tracking/observations", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("full snapshot status=%d body=%s", response.Code, response.Body.String())
	}
	var envelope map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	snapshot := envelope["data"].(map[string]any)
	if len(snapshot["observations"].([]any)) != 10000 {
		t.Fatalf("expected full 10000 observation response")
	}

	limitedRouter, _, _ := newTestAPI(t, 2)
	overflow := apiRequest(t, limitedRouter, http.MethodPut, "/api/tracking/client-state", map[string]any{
		"cloudEnabled": true, "interests": []any{apiInterest("1"), apiInterest("2"), apiInterest("3")},
	})
	if overflow.Code != http.StatusOK {
		t.Fatalf("interest storage status=%d body=%s", overflow.Code, overflow.Body.String())
	}
	overflow = apiRequest(t, limitedRouter, http.MethodGet, "/api/tracking/observations", nil)
	if overflow.Code != http.StatusConflict || !strings.Contains(overflow.Body.String(), "observation_limit_exceeded") {
		t.Fatalf("expected deterministic overflow error, status=%d body=%s", overflow.Code, overflow.Body.String())
	}
}

func openAPITestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", fmt.Sprintf("file:tracking-api-auth-%s?mode=memory&cache=shared", t.Name()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func boolPtr(value bool) *bool       { return &value }
func stringPtr(value string) *string { return &value }
