package v2api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"venera-server/internal/v2config"
	"venera-server/internal/v2crypto"
	"venera-server/internal/v2store"
)

type apiFixture struct {
	router *Router
	repo   *v2store.Repository
	db     *v2store.DB
	now    time.Time
}

func newAPIFixture(t *testing.T) *apiFixture {
	t.Helper()
	fixture := &apiFixture{now: time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)}
	root := bytes.Repeat([]byte{0x51}, v2crypto.RootSecretSize)
	keys, err := v2crypto.DeriveKeys(root)
	if err != nil {
		t.Fatalf("derive api keys: %v", err)
	}
	db, err := v2store.OpenWithOptions(filepath.Join(t.TempDir(), "api.db"), v2store.Options{Clock: func() time.Time { return fixture.now }})
	if err != nil {
		t.Fatalf("open api database: %v", err)
	}
	fixture.db = db
	fixture.repo = v2store.NewRepository(db, keys)
	cfg := v2config.Default()
	cfg.RootSecret = root
	fixture.router = NewRouter(cfg, fixture.repo)
	t.Cleanup(func() { _ = db.Close() })
	return fixture
}

func apiCall(t *testing.T, router http.Handler, method, path string, body any, token, idempotency, protocol string) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		reader = bytes.NewReader(encoded)
	}
	request := httptest.NewRequest(method, path, reader)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if idempotency != "" {
		request.Header.Set("Idempotency-Key", idempotency)
	}
	if protocol != "" {
		request.Header.Set("X-Venera-Protocol", protocol)
	}
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	return recorder
}

func decodeAPI(t *testing.T, recorder *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var result map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode API response %d: %v; body=%s", recorder.Code, err, recorder.Body.String())
	}
	return result
}

func TestHealthHeadersProtocolAndNoOldRoutes(t *testing.T) {
	fixture := newAPIFixture(t)
	response := apiCall(t, fixture.router, http.MethodGet, "/v2/health", nil, "", "", "2")
	if response.Code != http.StatusOK {
		t.Fatalf("health status = %d, body=%s", response.Code, response.Body.String())
	}
	if response.Header().Get("X-Request-Id") == "" || response.Header().Get("Cache-Control") != "no-store" || response.Header().Get("X-Venera-Protocol") != "2" {
		t.Fatalf("health headers missing: %#v", response.Header())
	}
	health := decodeAPI(t, response)
	data := health["data"].(map[string]any)
	if data["status"] != "ok" || data["catalogId"] != "" {
		t.Fatalf("unexpected health data: %#v", data)
	}
	if _, ok := health["error"]; ok {
		t.Fatalf("health returned error: %#v", health)
	}
	for _, path := range []string{"/v2/invites", "/v2/pairing", "/v2/devices", "/v2/users", "/v2/client/mode"} {
		oldRoute := apiCall(t, fixture.router, http.MethodGet, path, nil, "", "", "2")
		if oldRoute.Code != http.StatusNotFound {
			t.Errorf("forbidden old route %s status = %d, want 404", path, oldRoute.Code)
		}
	}
	protocol := apiCall(t, fixture.router, http.MethodGet, "/v2/health", nil, "", "", "1")
	if protocol.Code != http.StatusUpgradeRequired || decodeAPI(t, protocol)["error"].(map[string]any)["code"] != "protocol_incompatible" {
		t.Fatalf("incompatible protocol response = %d %s", protocol.Code, protocol.Body.String())
	}
}

func TestClaimReplayConflictSelfRevokeAndRedaction(t *testing.T) {
	fixture := newAPIFixture(t)
	code, err := fixture.repo.CreateEnrollmentCode(context.Background(), time.Hour)
	if err != nil {
		t.Fatalf("create enrollment code: %v", err)
	}
	pendingToken, err := v2crypto.GenerateBearerToken()
	if err != nil {
		t.Fatalf("generate pending token: %v", err)
	}
	body := map[string]any{
		"pendingClientId": "pending-api-1", "enrollmentCode": code.Code,
		"displayName": "API client", "platform": "desktop", "appVersion": "1.6.0",
	}
	first := apiCall(t, fixture.router, http.MethodPost, "/v2/onboarding/claim-enrollment", body, pendingToken, "claim-api-1", "2")
	if first.Code != http.StatusCreated {
		t.Fatalf("claim status = %d, body=%s", first.Code, first.Body.String())
	}
	firstJSON := decodeAPI(t, first)
	firstData := firstJSON["data"].(map[string]any)
	clientData := firstData["client"].(map[string]any)
	clientID := clientData["clientId"].(string)
	if firstData["initialCursor"].(string) == "" || firstData["bootstrapRequired"] != true {
		t.Fatalf("claim data missing cursor/bootstrap: %#v", firstData)
	}
	replay := apiCall(t, fixture.router, http.MethodPost, "/v2/onboarding/claim-enrollment", body, pendingToken, "claim-api-1", "2")
	if replay.Code != http.StatusCreated {
		t.Fatalf("claim replay status = %d, body=%s", replay.Code, replay.Body.String())
	}
	if decodeAPI(t, replay)["data"].(map[string]any)["client"].(map[string]any)["clientId"] != clientID {
		t.Fatalf("claim replay created a different client")
	}
	conflictBody := map[string]any{}
	for key, value := range body {
		conflictBody[key] = value
	}
	conflictBody["displayName"] = "changed"
	conflict := apiCall(t, fixture.router, http.MethodPost, "/v2/onboarding/claim-enrollment", conflictBody, pendingToken, "claim-api-1", "2")
	if conflict.Code != http.StatusConflict || decodeAPI(t, conflict)["error"].(map[string]any)["code"] != "idempotency_conflict" {
		t.Fatalf("claim conflict response = %d %s", conflict.Code, conflict.Body.String())
	}

	self := apiCall(t, fixture.router, http.MethodGet, "/v2/client/self", nil, pendingToken, "", "2")
	if self.Code != http.StatusOK {
		t.Fatalf("self status = %d, body=%s", self.Code, self.Body.String())
	}
	if strings.Contains(self.Body.String(), pendingToken) || strings.Contains(self.Body.String(), "tokenDigest") {
		t.Fatal("self response unexpectedly exposed client credentials")
	}
	revoke := apiCall(t, fixture.router, http.MethodDelete, "/v2/client/self", map[string]any{"expectedRevision": 1}, pendingToken, "revoke-api-1", "2")
	if revoke.Code != http.StatusOK || !strings.Contains(revoke.Body.String(), `"state":"revoked"`) {
		t.Fatalf("revoke response = %d %s", revoke.Code, revoke.Body.String())
	}
	unauthorized := apiCall(t, fixture.router, http.MethodGet, "/v2/client/self", nil, pendingToken, "", "2")
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("revoked token status = %d, body=%s", unauthorized.Code, unauthorized.Body.String())
	}
	if strings.Contains(conflict.Body.String(), code.Code) || strings.Contains(conflict.Body.String(), pendingToken) {
		t.Fatal("error response exposed enrollment code or token")
	}
}

func TestOpenEnrollmentRequiresExplicitDevelopmentGate(t *testing.T) {
	fixture := newAPIFixture(t)
	pendingToken, err := v2crypto.GenerateBearerToken()
	if err != nil {
		t.Fatalf("generate pending token: %v", err)
	}
	body := map[string]any{
		"pendingClientId": "pending-open-enrollment",
		"displayName":     "Development client",
		"platform":        "desktop",
		"appVersion":      "1.6.0",
	}

	blocked := apiCall(t, fixture.router, http.MethodPost, "/v2/onboarding/claim-enrollment", body, pendingToken, "claim-open-blocked", "2")
	if blocked.Code != http.StatusBadRequest || decodeAPI(t, blocked)["error"].(map[string]any)["code"] != "enrollment_code_required" {
		t.Fatalf("closed enrollment response = %d %s", blocked.Code, blocked.Body.String())
	}

	fixture.router.cfg.DevOpenEnrollment = true
	fixture.router.cfg.Production = false
	opened := apiCall(t, fixture.router, http.MethodPost, "/v2/onboarding/claim-enrollment", body, pendingToken, "claim-open-enabled", "2")
	if opened.Code != http.StatusCreated {
		t.Fatalf("open enrollment response = %d %s", opened.Code, opened.Body.String())
	}
	self := apiCall(t, fixture.router, http.MethodGet, "/v2/client/self", nil, pendingToken, "", "2")
	if self.Code != http.StatusOK {
		t.Fatalf("open-enrolled client authentication = %d %s", self.Code, self.Body.String())
	}
}

func TestBodyLimitUnknownTokenAndExpiredEnrollment(t *testing.T) {
	fixture := newAPIFixture(t)
	fixture.router.cfg.HTTPBodyLimit = 32
	tooLarge := apiCall(t, fixture.router, http.MethodPost, "/v2/onboarding/claim-enrollment", map[string]any{"pendingClientId": strings.Repeat("x", 100)}, "invalid", "too-large", "2")
	if tooLarge.Code != http.StatusUnauthorized {
		// Authentication is deliberately checked before reading a potentially
		// secret-bearing request body.
		t.Fatalf("unknown token before body limit status = %d", tooLarge.Code)
	}
	validPending, err := v2crypto.GenerateBearerToken()
	if err != nil {
		t.Fatalf("generate valid pending token: %v", err)
	}
	tooLarge = apiCall(t, fixture.router, http.MethodPost, "/v2/onboarding/claim-enrollment", map[string]any{"pendingClientId": strings.Repeat("x", 100)}, validPending, "too-large-valid", "2")
	if tooLarge.Code != http.StatusBadRequest {
		t.Fatalf("body limit status = %d, body=%s", tooLarge.Code, tooLarge.Body.String())
	}
	fixture.router.cfg.HTTPBodyLimit = 4 << 20
	code, err := fixture.repo.CreateEnrollmentCode(context.Background(), time.Minute)
	if err != nil {
		t.Fatalf("create expiring code: %v", err)
	}
	fixture.now = fixture.now.Add(2 * time.Minute)
	expired := apiCall(t, fixture.router, http.MethodPost, "/v2/onboarding/claim-enrollment", map[string]any{
		"pendingClientId": "pending-expired-api", "enrollmentCode": code.Code,
	}, validPending, "expired-api", "2")
	if expired.Code != http.StatusGone || decodeAPI(t, expired)["error"].(map[string]any)["code"] != "enrollment_code_expired" {
		t.Fatalf("expired enrollment response = %d %s", expired.Code, expired.Body.String())
	}
	if strings.Contains(expired.Body.String(), code.Code) || strings.Contains(expired.Body.String(), validPending) {
		t.Fatal("expired enrollment response exposed code or token")
	}
}
