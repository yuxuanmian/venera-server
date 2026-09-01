package v2worker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func TestHTTPBrokerEnforcesOriginRedirectSecretsAndBudgets(t *testing.T) {
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch request.URL.Path {
		case "/redirect":
			return &http.Response{StatusCode: http.StatusFound, Header: http.Header{"Location": []string{"https://evil.test/steal"}}, Body: io.NopCloser(strings.NewReader("")), Request: request}, nil
		case "/large":
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("123456")), Request: request}, nil
		default:
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ok")), Request: request}, nil
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
	if _, err := broker.Do(context.Background(), HTTPRequest{URL: "https://manwa.test/", Headers: map[string]string{"Cookie": "secret"}}); !errors.Is(err, ErrHTTPSecretHeader) {
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

func TestHTTPBrokerCandidateJarsAreIsolated(t *testing.T) {
	jarOne := testCookieJar(t, "candidate-one")
	jarTwo := testCookieJar(t, "candidate-two")
	first, err := NewHTTPBroker(BrokerConfig{AllowedOrigins: []string{"https://manwa.test"}, CookieJar: jarOne})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewHTTPBroker(BrokerConfig{AllowedOrigins: []string{"https://manwa.test"}, CookieJar: jarTwo})
	if err != nil {
		t.Fatal(err)
	}
	firstCookies := first.CookieJar().Cookies(mustURL(t, "https://manwa.test/"))
	secondCookies := second.CookieJar().Cookies(mustURL(t, "https://manwa.test/"))
	if len(firstCookies) != 1 || firstCookies[0].Value != "candidate-one" {
		t.Fatalf("first jar = %#v", firstCookies)
	}
	if len(secondCookies) != 1 || secondCookies[0].Value != "candidate-two" {
		t.Fatalf("second jar = %#v", secondCookies)
	}
}

func TestWorkerReplaysManwaProbeAndDoesNotRetainRuntimeState(t *testing.T) {
	core, extension := manwaScripts(t)
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body := ""
		switch request.URL.Path {
		case "/ucenter":
			body = "<!doctype html><html><body><div class=\"center-main-info-right\"><p class=\"center-main-info-title\">脱敏昵称</p><p class=\"center-main-info-title\">用户名 : fixture-account</p></div></body></html>"
		case "/users/welfare":
			body = "<!doctype html><html><body><div class=\"center-main-info\"><div class=\"detail-list-comment-lv\"><span>Lv2</span></div></div></body></html>"
		default:
			return responseFor(request, http.StatusNotFound, "not found"), nil
		}
		return responseFor(request, http.StatusOK, body), nil
	})
	worker, err := NewWorker(WorkerConfig{
		CoreScript:              core,
		ExtensionScript:         extension,
		AllowedOrigins:          []string{"https://manwa.me"},
		Client:                  &http.Client{Transport: transport},
		DefaultOperationTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := RunRequest{
		RequestID:        "probe-1",
		Operation:        OperationProbeAccount,
		Input:            map[string]any{"reason": "candidate_validation", "requestedAt": time.Now().UTC().Format(time.RFC3339)},
		ArtifactID:       "manwa",
		PackageReleaseID: "manwa-release-1",
		Budget:           OperationBudget{MaxRequests: 2, MaxItems: 1},
	}
	first, err := worker.Run(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(first.Output), `"value":"fixture-account"`) || !strings.Contains(string(first.Output), `"accountLevel":2`) {
		t.Fatalf("first output = %s", first.Output)
	}
	secondRequest := request
	secondRequest.RequestID = "probe-2"
	second, err := worker.Run(context.Background(), secondRequest)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(second.Output), `"value":"fixture-account"`) || !strings.Contains(string(second.Output), `"accountLevel":2`) {
		t.Fatalf("second output = %s", second.Output)
	}
}

func TestWorkerCapturesSetCookieWithoutExposingCookieToJS(t *testing.T) {
	worker, err := NewWorker(WorkerConfig{
		ExtensionScript: []byte(`SourceServerExtensions.register({capabilities:{scanning:{probeAccount: async function() { const response = await sendMessage({method:"http",http_method:"GET",url:"https://manwa.me/",headers:{}}); return {identity:{scheme:"test-v1",value:String(response.status)},display:{name:"ok"},attributes:{},visibilityScope:"test:scope",sessionPatch:null}; }}}});`),
		AllowedOrigins:  []string{"https://manwa.me"},
		Client: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			response := responseFor(request, http.StatusOK, "ok")
			response.Header.Add("Set-Cookie", "session=server-value; Path=/")
			return response, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := worker.Run(context.Background(), RunRequest{RequestID: "cookie-capture", Operation: OperationProbeAccount, ArtifactID: "manwa", PackageReleaseID: "release", Budget: OperationBudget{MaxRequests: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.SessionCookies) != 1 || result.SessionCookies[0].Name != "session" || result.SessionCookies[0].Value != "server-value" {
		t.Fatalf("captured cookies = %#v", result.SessionCookies)
	}
	if strings.Contains(string(result.Output), "server-value") {
		t.Fatalf("cookie leaked into output = %s", result.Output)
	}
}

func TestWorkerRejectsCookieAccessUnknownErrorsOutputAndTimeout(t *testing.T) {
	base := RunRequest{RequestID: "request-1", Operation: OperationProbeAccount, ArtifactID: "manwa", PackageReleaseID: "release", Budget: OperationBudget{MaxRequests: 1}}
	for name, script := range map[string]string{
		"cookie":        `SourceServerExtensions.register({capabilities:{scanning:{probeAccount: async function() { return Network.getCookies("https://manwa.me"); }}}});`,
		"unknown-error": `SourceServerExtensions.register({capabilities:{scanning:{probeAccount: async function() { throw Scanning.error({code:"unknown",safeMessage:"bad"}); }}}});`,
		"large-output":  `SourceServerExtensions.register({capabilities:{scanning:{probeAccount: async function() { return {large:"` + strings.Repeat("x", 128) + `"}; }}}});`,
	} {
		t.Run(name, func(t *testing.T) {
			worker, err := NewWorker(WorkerConfig{ExtensionScript: []byte(script), AllowedOrigins: []string{"https://manwa.me"}, MaxOutputBytes: 64})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := worker.Run(context.Background(), base); !isExtensionFailure(err) {
				t.Fatalf("error = %T %v", err, err)
			}
		})
	}

	worker, err := NewWorker(WorkerConfig{
		ExtensionScript: []byte(`SourceServerExtensions.register({capabilities:{scanning:{probeAccount: function() { while (true) {} }}}});`),
		AllowedOrigins:  []string{"https://manwa.me"},
	})
	if err != nil {
		t.Fatal(err)
	}
	deadlineRequest := base
	deadlineRequest.RequestID = "timeout"
	deadlineRequest.DeadlineAt = time.Now().Add(50 * time.Millisecond)
	if _, err := worker.Run(context.Background(), deadlineRequest); !errors.Is(err, ErrWorkerDeadline) {
		t.Fatalf("timeout error = %v", err)
	}
}

func TestIPCFramingAndMalformedInput(t *testing.T) {
	var buffer strings.Builder
	request := IPCRequest{RequestID: "request-1", Operation: OperationProbeAccount, Payload: []byte(`{}`)}
	if err := WriteIPCFrame(&buffer, request); err != nil {
		t.Fatal(err)
	}
	payload, err := ReadIPCFrame(strings.NewReader(buffer.String()))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(payload), `"requestId":"request-1"`) {
		t.Fatalf("payload = %s", payload)
	}
	var malformed strings.Builder
	if err := WriteIPCBytes(&malformed, []byte(`{"requestId":"missing-operation"}`)); err != nil {
		t.Fatal(err)
	}
	err = ServeIPC(context.Background(), strings.NewReader(malformed.String()), io.Discard, func(context.Context, IPCRequest) IPCResponse {
		t.Fatal("malformed request reached handler")
		return IPCResponse{}
	})
	if !errors.Is(err, ErrIPCFrameInvalid) {
		t.Fatalf("malformed IPC error = %v", err)
	}
}

func isExtensionFailure(err error) bool {
	if errors.Is(err, ErrExtensionFailure) {
		return true
	}
	var structured StructuredError
	return errors.As(err, &structured) && structured.Code == "extension_failure"
}

func manwaScripts(t *testing.T) ([]byte, []byte) {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Join(filepath.Dir(file), "..", "..", "..", "venera-configs")
	core, err := os.ReadFile(filepath.Join(root, "_venera_.js"))
	if err != nil {
		t.Fatal(err)
	}
	extension, err := os.ReadFile(filepath.Join(root, "extensions", "server", "manwa", "scanning.js"))
	if err != nil {
		t.Fatal(err)
	}
	return core, extension
}

func responseFor(request *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: request}
}

func testCookieJar(t *testing.T, value string) http.CookieJar {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	jar.SetCookies(mustURL(t, "https://manwa.test/"), []*http.Cookie{{Name: "session", Value: value}})
	return jar
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
