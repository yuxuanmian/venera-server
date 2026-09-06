package adminweb

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEmbeddedCatalogAdminPage(t *testing.T) {
	recorder := httptest.NewRecorder()
	NewHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "Venera 漫画源配置") {
		t.Fatalf("embedded page status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}
