package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"venera-server/internal/adminweb"
	"venera-server/internal/catalog"
)

func testCatalogHandler(t *testing.T) http.Handler {
	return testCatalogHandlerWithWeb(t, nil)
}

func testCatalogHandlerWithWeb(t *testing.T, web http.Handler) http.Handler {
	t.Helper()
	client := catalog.NewClient(&http.Client{Transport: catalogRoundTrip(func(request *http.Request) (*http.Response, error) {
		body := "[]"
		if request.URL.Host == catalog.GitHubAPIHost {
			body = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	})})
	manager, err := catalog.NewManager(catalog.CatalogConfig{CatalogURL: "https://raw.githubusercontent.com/owner/repo/main/index.json"}, catalog.NewStore(t.TempDir()), client)
	if err != nil {
		t.Fatal(err)
	}
	return NewCatalogHandler(manager, web)
}

func TestCatalogAdminAssetsUseProductionStripPrefixRoute(t *testing.T) {
	handler := testCatalogHandlerWithWeb(
		t,
		http.StripPrefix("/admin/", adminweb.NewHandler()),
	)
	for _, path := range []string{"/admin/", "/admin/app.js", "/admin/style.css"} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("GET %s returned %d", path, recorder.Code)
		}
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/admin/api/unknown", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("unknown admin API returned %d", recorder.Code)
	}
}

type catalogRoundTrip func(*http.Request) (*http.Response, error)

func (f catalogRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestCatalogAuthorityContractAndHealth(t *testing.T) {
	handler := testCatalogHandler(t)
	request := httptest.NewRequest(http.MethodGet, "/api/catalog/authority", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), "catalog_not_activated") {
		t.Fatalf("authority response %d %s", recorder.Code, recorder.Body.String())
	}
	if recorder.Header().Get("Content-Type") == "" {
		t.Fatal("authority must be JSON")
	}
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/health", nil))
	if recorder.Code != http.StatusOK || recorder.Body.String() != "{\"status\":\"ok\"}\n" {
		t.Fatalf("health response %d %q", recorder.Code, recorder.Body.String())
	}
}

func TestCatalogCheckActivateAndUnknownRoutes(t *testing.T) {
	handler := testCatalogHandler(t)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/admin/api/catalog/check", strings.NewReader("{}")))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "candidate") {
		t.Fatalf("check response %d %s", recorder.Code, recorder.Body.String())
	}
	var checked struct {
		Candidate catalog.CheckedCatalog `json:"candidate"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &checked); err != nil {
		t.Fatal(err)
	}
	body := `{"catalogId":"owner/repo","revision":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/admin/api/catalog/activate", strings.NewReader(body)))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "active") {
		t.Fatalf("activate response %d %s", recorder.Code, recorder.Body.String())
	}
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/admin/api/not-real", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("unknown route status %d", recorder.Code)
	}
}
