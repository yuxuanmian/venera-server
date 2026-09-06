package catalog

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestClientFullSHAOnlyFetchesPinnedIndex(t *testing.T) {
	var requests []string
	client := NewClient(&http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests = append(requests, request.URL.String())
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("[]")), Request: request}, nil
	})})
	configured := "https://raw.githubusercontent.com/owner/repo/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/index.json"
	_, _, index, err := client.FetchCatalog(context.Background(), configured)
	if err != nil {
		t.Fatal(err)
	}
	if len(index) != 0 || len(requests) != 1 || !strings.Contains(requests[0], "/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/index.json") {
		t.Fatalf("requests=%v index=%v", requests, index)
	}
}

func TestClientResolvesRefThenFetchesPinnedIndex(t *testing.T) {
	var requests []string
	client := NewClient(&http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests = append(requests, request.URL.String())
		body := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		if request.URL.Host == RawHost {
			body = "[]"
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	})})
	_, _, _, err := client.FetchCatalog(context.Background(), "https://raw.githubusercontent.com/owner/repo/main/index.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 2 || !strings.Contains(requests[0], "api.github.com/repos/owner/repo/commits/main") || !strings.Contains(requests[1], "/index.json") {
		t.Fatalf("requests=%v", requests)
	}
}
