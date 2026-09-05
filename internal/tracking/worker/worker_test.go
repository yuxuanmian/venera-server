package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func responseFor(request *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}
}

func TestHTTPBrokerBoundsOriginsSecretsRedirectsAndResponses(t *testing.T) {
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch request.URL.Path {
		case "/redirect":
			return &http.Response{
				StatusCode: http.StatusFound,
				Header:     http.Header{"Location": []string{"https://evil.test/steal"}},
				Body:       io.NopCloser(strings.NewReader("")),
				Request:    request,
			}, nil
		case "/large":
			return responseFor(request, http.StatusOK, "123456"), nil
		default:
			return responseFor(request, http.StatusOK, "ok"), nil
		}
	})
	broker, err := NewHTTPBroker(BrokerConfig{
		AllowedOrigins:        []string{"https://manwa.test"},
		Client:                &http.Client{Transport: transport},
		MaxRequests:           2,
		MaxResponseBytes:      5,
		MaxTotalResponseBytes: 5,
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := broker.Do(context.Background(), HTTPRequest{URL: "https://other.test/"}); !errors.Is(err, ErrHTTPOriginNotAllowed) {
		t.Fatalf("cross-origin request error = %v", err)
	}
	if _, err := broker.Do(context.Background(), HTTPRequest{
		URL:     "https://manwa.test/",
		Headers: map[string]string{"Cookie": "secret"},
	}); !errors.Is(err, ErrHTTPSecretHeader) {
		t.Fatalf("cookie header error = %v", err)
	}
	if _, err := broker.Do(context.Background(), HTTPRequest{URL: "https://manwa.test/redirect"}); !errors.Is(err, ErrHTTPRedirectBlocked) {
		t.Fatalf("redirect error = %v", err)
	}
	if _, err := broker.Do(context.Background(), HTTPRequest{URL: "https://manwa.test/large"}); !errors.Is(err, ErrHTTPResponseTooLarge) {
		t.Fatalf("large response error = %v", err)
	}
	if _, err := broker.Do(context.Background(), HTTPRequest{URL: "https://manwa.test/"}); !errors.Is(err, ErrHTTPBudgetExceeded) {
		t.Fatalf("request budget error = %v", err)
	}
}

func TestHTTPBrokerCookieJarsAreIsolated(t *testing.T) {
	jarOne, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	jarTwo, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	origin, _ := url.Parse("https://manwa.test/")
	jarOne.SetCookies(origin, []*http.Cookie{{Name: "session", Value: "one"}})
	jarTwo.SetCookies(origin, []*http.Cookie{{Name: "session", Value: "two"}})
	first, err := NewHTTPBroker(BrokerConfig{AllowedOrigins: []string{"https://manwa.test"}, CookieJar: jarOne})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewHTTPBroker(BrokerConfig{AllowedOrigins: []string{"https://manwa.test"}, CookieJar: jarTwo})
	if err != nil {
		t.Fatal(err)
	}
	if got := first.CookieJar().Cookies(origin); len(got) != 1 || got[0].Value != "one" {
		t.Fatalf("first jar = %#v", got)
	}
	if got := second.CookieJar().Cookies(origin); len(got) != 1 || got[0].Value != "two" {
		t.Fatalf("second jar = %#v", got)
	}
}

func TestWorkerRunsRegisteredCapabilityAndChecksExactArtifact(t *testing.T) {
	script := []byte(`SourceServerExtensions.register({
		artifactId: "manwa", sourceKey: "manwa", fileName: "manwa.js",
		capabilities: {scanning: {
			probeAccount: async function(input) {
				return {identity: {scheme: "manwa-username-v1", value: input.value},
					display: {name: "fixture"}, attributes: {accountLevel: 2},
					visibilityScope: "manwa:level:2", sessionPatch: null};
			},
			scanFavoriteSnapshotSlice: async function() { return {items: []}; }
		}}
	});`)
	worker, err := NewWorker(WorkerConfig{
		ExtensionScript:         script,
		AllowedOrigins:          []string{"https://manwa.test"},
		DefaultOperationTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := worker.Run(context.Background(), RunRequest{
		RequestID:  "probe-1",
		Operation:  OperationProbeAccount,
		Input:      map[string]string{"value": "fixture-account"},
		ArtifactID: "manwa",
		FileName:   "manwa.js",
		Budget:     OperationBudget{MaxRequests: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(result.Output, []byte(`"fixture-account"`)) {
		t.Fatalf("worker output = %s", result.Output)
	}

	_, err = worker.Run(context.Background(), RunRequest{
		RequestID:  "wrong-artifact",
		Operation:  OperationProbeAccount,
		Input:      map[string]string{},
		ArtifactID: "copy_manga",
		FileName:   "copy_manga.js",
		Budget:     OperationBudget{MaxRequests: 1},
	})
	var structured StructuredError
	if !errors.As(err, &structured) || structured.Code != "extension_failure" {
		t.Fatalf("wrong artifact error = %T %v", err, err)
	}
}

func TestIPCFramesRejectOversizeAndUnknownFields(t *testing.T) {
	var frames bytes.Buffer
	if err := WriteIPCFrame(&frames, IPCRequest{
		RequestID: "request-1",
		Operation: OperationProbeAccount,
		Payload:   []byte(`{}`),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadIPCFrame(&frames); err != nil {
		t.Fatal(err)
	}
	if err := WriteIPCBytes(io.Discard, bytes.Repeat([]byte("x"), MaxIPCFrameBytes+1)); !errors.Is(err, ErrIPCFrameTooLarge) {
		t.Fatalf("oversize write error = %v", err)
	}
	var malformed bytes.Buffer
	if err := WriteIPCBytes(&malformed, []byte(`{"requestId":"request-1","operation":"probeAccount","unknown":true}`)); err != nil {
		t.Fatal(err)
	}
	err := ServeIPC(context.Background(), &malformed, io.Discard, func(context.Context, IPCRequest) IPCResponse {
		t.Fatal("malformed request reached handler")
		return IPCResponse{}
	})
	if !errors.Is(err, ErrIPCFrameInvalid) {
		t.Fatalf("malformed IPC error = %v", err)
	}
}
