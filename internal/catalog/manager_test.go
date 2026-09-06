package catalog

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

func managerWithFakeCatalog(t *testing.T) (*Manager, *int) {
	t.Helper()
	requests := 0
	client := NewClient(&http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		body := "[]"
		if request.URL.Host == GitHubAPIHost {
			body = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	})})
	manager, err := NewManager(CatalogConfig{CatalogURL: "https://raw.githubusercontent.com/owner/repo/main/index.json"}, NewStore(t.TempDir()), client)
	if err != nil {
		t.Fatal(err)
	}
	return manager, &requests
}

func TestManagerCheckDoesNotActivateAndActivateUsesLocalSnapshot(t *testing.T) {
	manager, requests := managerWithFakeCatalog(t)
	if err := manager.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	candidate, err := manager.Check(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if candidate == nil || manager.AuthorityIsAvailableForTest() {
		t.Fatal("check must not publish active")
	}
	active, err := manager.Activate(context.Background(), candidate.CatalogID, candidate.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if active == nil || !manager.AuthorityIsAvailableForTest() || *requests != 2 {
		t.Fatalf("active=%#v requests=%d", active, *requests)
	}
	if _, err := manager.Activate(context.Background(), active.CatalogID, active.Revision); err != nil {
		t.Fatal(err)
	}
	if *requests != 2 {
		t.Fatal("same active activation must not fetch remotely")
	}
}

func (m *Manager) AuthorityIsAvailableForTest() bool {
	_, ok := m.Authority()
	return ok
}
