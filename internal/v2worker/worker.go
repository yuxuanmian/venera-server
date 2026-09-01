package v2worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/buke/quickjs-go"

	"venera-server/internal/jsbridge"
)

const (
	OperationProbeAccount              = "probeAccount"
	OperationScanFavoriteSnapshotSlice = "scanFavoriteSnapshotSlice"
	OperationScanComic                 = "scanComic"
)

var (
	ErrWorkerDeadline   = errors.New("worker operation deadline exceeded")
	ErrWorkerInvalid    = errors.New("worker request is invalid")
	ErrExtensionFailure = errors.New("scanning extension failed")
)

type OperationBudget struct {
	MaxRequests int       `json:"maxRequests"`
	MaxItems    int       `json:"maxItems"`
	DeadlineAt  time.Time `json:"deadlineAt"`
}

type RunRequest struct {
	RequestID             string
	Operation             string
	Input                 any
	DeadlineAt            time.Time
	Budget                OperationBudget
	ArtifactID            string
	PackageReleaseID      string
	CoreHash              string
	ScanningExtensionHash string
	SessionEpoch          int64
	SessionRevision       int64
	AllowedOrigins        []string
	SessionCookies        []http.Cookie
}

type RunResult struct {
	Output         json.RawMessage
	SessionCookies []http.Cookie
	RequestCount   int
	ResponseBytes  int64
}

type StructuredError struct {
	Code              string `json:"code"`
	SafeMessage       string `json:"safeMessage"`
	RetryAfterSeconds *int   `json:"retryAfterSeconds,omitempty"`
}

func (e StructuredError) Error() string {
	return e.Code
}

type WorkerConfig struct {
	CoreScript              []byte
	ExtensionScript         []byte
	AllowedOrigins          []string
	Client                  *http.Client
	UserAgent               string
	MaxOutputBytes          int64
	MaxCheckpointBytes      int64
	MaxItems                int
	MaxResponseBytes        int64
	MaxRequestBodyBytes     int64
	MaxTotalResponseBytes   int64
	MemoryLimitBytes        uint64
	MaxStackBytes           uint64
	DefaultOperationTimeout time.Duration
	MinRequestStartInterval time.Duration
}

type Worker struct {
	config WorkerConfig
}

func NewWorker(config WorkerConfig) (*Worker, error) {
	if len(config.ExtensionScript) == 0 || len(config.AllowedOrigins) == 0 {
		return nil, ErrWorkerInvalid
	}
	if config.MaxOutputBytes <= 0 {
		config.MaxOutputBytes = 2 << 20
	}
	if config.MaxCheckpointBytes <= 0 {
		config.MaxCheckpointBytes = 16 << 10
	}
	if config.MaxItems <= 0 {
		config.MaxItems = 200
	}
	if config.MaxResponseBytes <= 0 {
		config.MaxResponseBytes = 8 << 20
	}
	if config.MaxRequestBodyBytes <= 0 {
		config.MaxRequestBodyBytes = 1 << 20
	}
	if config.MaxTotalResponseBytes <= 0 {
		config.MaxTotalResponseBytes = config.MaxResponseBytes
	}
	if config.DefaultOperationTimeout <= 0 {
		config.DefaultOperationTimeout = 15 * time.Second
	}
	if config.UserAgent == "" {
		config.UserAgent = "VeneraServer/2"
	}
	return &Worker{config: config}, nil
}

func (worker *Worker) Run(ctx context.Context, request RunRequest) (RunResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if request.Operation != OperationProbeAccount && request.Operation != OperationScanFavoriteSnapshotSlice && request.Operation != OperationScanComic {
		return RunResult{}, ErrWorkerInvalid
	}
	if request.RequestID == "" || request.ArtifactID == "" || request.PackageReleaseID == "" {
		return RunResult{}, ErrWorkerInvalid
	}
	inputBytes, err := json.Marshal(request.Input)
	if err != nil {
		return RunResult{}, ErrWorkerInvalid
	}
	if request.Budget.MaxRequests <= 0 {
		request.Budget.MaxRequests = 1
	}
	if request.Budget.MaxItems <= 0 {
		request.Budget.MaxItems = worker.config.MaxItems
	}
	deadline := request.DeadlineAt
	if deadline.IsZero() {
		deadline = request.Budget.DeadlineAt
	}
	if deadline.IsZero() {
		deadline = time.Now().Add(worker.config.DefaultOperationTimeout)
	}
	if !deadline.After(time.Now()) {
		return RunResult{}, ErrWorkerDeadline
	}
	operationContext, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	jar, err := cookiejar.New(nil)
	if err != nil {
		return RunResult{}, err
	}
	if len(request.SessionCookies) > 0 {
		origin, parseErr := url.Parse(worker.config.AllowedOrigins[0])
		if parseErr != nil {
			return RunResult{}, ErrWorkerInvalid
		}
		cookies := make([]*http.Cookie, 0, len(request.SessionCookies))
		for index := range request.SessionCookies {
			cookie := request.SessionCookies[index]
			cookies = append(cookies, &cookie)
		}
		jar.SetCookies(origin, cookies)
	}
	broker, err := NewHTTPBroker(BrokerConfig{
		AllowedOrigins: worker.config.AllowedOrigins, UserAgent: worker.config.UserAgent,
		MaxRequests: request.Budget.MaxRequests, MaxRequestBodyBytes: worker.config.MaxRequestBodyBytes,
		MaxResponseBytes: worker.config.MaxResponseBytes, MaxTotalResponseBytes: worker.config.MaxTotalResponseBytes,
		MinRequestStartInterval: worker.config.MinRequestStartInterval, CookieJar: jar, Client: worker.config.Client,
	})
	if err != nil {
		return RunResult{}, err
	}

	runtime := quickjs.NewRuntime()
	defer runtime.Close()
	if worker.config.MemoryLimitBytes > 0 {
		runtime.SetMemoryLimit(worker.config.MemoryLimitBytes)
	}
	if worker.config.MaxStackBytes > 0 {
		runtime.SetMaxStackSize(worker.config.MaxStackBytes)
	}
	runtime.SetInterruptHandler(func() int {
		select {
		case <-operationContext.Done():
			return 1
		default:
			return 0
		}
	})
	defer runtime.ClearInterruptHandler()
	quickContext := runtime.NewContext()
	defer quickContext.Close()

	var brokerErr error
	htmlBridge := newWorkerHTMLBridge()
	bridge := &jsbridge.Bridge{
		HTTP: func(qctx *quickjs.Context, message *quickjs.Value) *quickjs.Value {
			httpRequest, parseErr := requestFromJS(message)
			if parseErr != nil {
				brokerErr = parseErr
				return qctx.ThrowError(errors.New("HTTP broker rejected request"))
			}
			httpResponse, doErr := broker.Do(operationContext, httpRequest)
			if doErr != nil {
				brokerErr = doErr
				return qctx.ThrowError(errors.New("HTTP broker request failed"))
			}
			return responseToJS(qctx, message, httpResponse)
		},
		Cookie: func(qctx *quickjs.Context, message *quickjs.Value) *quickjs.Value {
			return qctx.ThrowError(errors.New("cookie access is unavailable to scanning extensions"))
		},
		Log: func(level, title, content string) {},
	}
	sendMessage := quickContext.NewFunction(func(qctx *quickjs.Context, this *quickjs.Value, args []*quickjs.Value) *quickjs.Value {
		if len(args) == 0 || args[0] == nil {
			return qctx.ThrowError(errors.New("sendMessage arguments are invalid"))
		}
		method := args[0].Get("method")
		if method != nil {
			isHTML := method.String() == "html"
			method.Free()
			if isHTML {
				return htmlBridge.Handle(qctx, args[0])
			}
		}
		return bridge.Handle(qctx, args[0])
	})
	quickContext.Globals().Set("sendMessage", sendMessage)

	if len(worker.config.CoreScript) > 0 {
		if err := evalScript(quickContext, string(worker.config.CoreScript)); err != nil {
			return RunResult{}, ErrExtensionFailure
		}
	}
	if err := evalScript(quickContext, `globalThis.SourceServerExtensions = { register: function(value) { globalThis.__v2_registered_extension = value; } }; globalThis.Scanning = { error: function(value) { return value; } };`); err != nil {
		return RunResult{}, ErrExtensionFailure
	}
	if err := evalScript(quickContext, string(worker.config.ExtensionScript)); err != nil {
		return RunResult{}, ErrExtensionFailure
	}

	inputLiteral := strconv.Quote(string(inputBytes))
	operationLiteral := strconv.Quote(request.Operation)
	expression := fmt.Sprintf(`(async function() {
		try {
			const extension = globalThis.__v2_registered_extension;
			const capability = extension && extension.capabilities && extension.capabilities.scanning && extension.capabilities.scanning[%s];
			if (typeof capability !== "function") {
				return JSON.stringify({ok:false,error:{code:"extension_failure",safeMessage:"extension failure"}});
			}
			const value = await capability(JSON.parse(%s));
			return JSON.stringify({ok:true,value:value === undefined ? null : value});
		} catch (error) {
			if (error && typeof error.code === "string" && typeof error.safeMessage === "string") {
				return JSON.stringify({ok:false,error:{code:error.code,safeMessage:error.safeMessage,retryAfterSeconds:error.retryAfterSeconds}});
			}
			return JSON.stringify({ok:false,error:{code:"extension_failure",safeMessage:"extension failure"}});
		}
	})()`, operationLiteral, inputLiteral)
	value := quickContext.Eval(expression, quickjs.EvalAwait(true))
	if value == nil {
		if brokerErr != nil {
			return RunResult{}, brokerErr
		}
		return RunResult{}, ErrExtensionFailure
	}
	defer value.Free()
	if value.IsException() {
		if brokerErr != nil {
			return RunResult{}, brokerErr
		}
		select {
		case <-operationContext.Done():
			return RunResult{}, ErrWorkerDeadline
		default:
		}
		return RunResult{}, ErrExtensionFailure
	}
	if brokerErr != nil {
		return RunResult{}, brokerErr
	}
	var envelope struct {
		OK    bool             `json:"ok"`
		Value json.RawMessage  `json:"value"`
		Error *StructuredError `json:"error"`
	}
	if err := json.Unmarshal([]byte(value.String()), &envelope); err != nil {
		return RunResult{}, ErrExtensionFailure
	}
	if !envelope.OK {
		if envelope.Error == nil || !validStructuredError(*envelope.Error) {
			return RunResult{}, ErrExtensionFailure
		}
		return RunResult{}, *envelope.Error
	}
	if int64(len(envelope.Value)) > worker.config.MaxOutputBytes {
		return RunResult{}, ErrExtensionFailure
	}
	if err := validateJSONValue(envelope.Value); err != nil {
		return RunResult{}, ErrExtensionFailure
	}
	return RunResult{Output: append(json.RawMessage(nil), envelope.Value...), SessionCookies: brokerCookies(broker), RequestCount: broker.requestCountValue(), ResponseBytes: broker.responseBytesValue()}, nil
}

func evalScript(ctx *quickjs.Context, source string) error {
	value := ctx.Eval(source)
	if value == nil {
		return ErrExtensionFailure
	}
	defer value.Free()
	if value.IsException() {
		return ErrExtensionFailure
	}
	return nil
}

func requestFromJS(message *quickjs.Value) (HTTPRequest, error) {
	if message == nil || !message.IsObject() {
		return HTTPRequest{}, ErrWorkerInvalid
	}
	methodValue := message.Get("http_method")
	urlValue := message.Get("url")
	if methodValue == nil || urlValue == nil {
		return HTTPRequest{}, ErrWorkerInvalid
	}
	method, rawURL := methodValue.String(), urlValue.String()
	methodValue.Free()
	urlValue.Free()
	request := HTTPRequest{Method: method, URL: rawURL, Headers: map[string]string{}}
	headers := message.Get("headers")
	if headers != nil {
		if headers.IsObject() {
			names, err := headers.PropertyNames()
			if err != nil {
				headers.Free()
				return HTTPRequest{}, ErrWorkerInvalid
			}
			for _, name := range names {
				value := headers.Get(name)
				if value != nil {
					request.Headers[name] = value.String()
					value.Free()
				}
			}
		}
		headers.Free()
	}
	data := message.Get("data")
	if data != nil {
		if !data.IsNull() && !data.IsUndefined() {
			if data.IsByteArray() {
				body, err := jsbridge.ValueToBytes(data)
				if err != nil {
					data.Free()
					return HTTPRequest{}, err
				}
				request.Body = body
			} else {
				request.Body = []byte(data.String())
			}
		}
		data.Free()
	}
	return request, nil
}

func responseToJS(ctx *quickjs.Context, message *quickjs.Value, response HTTPResponse) *quickjs.Value {
	result := ctx.NewObject()
	result.Set("status", ctx.NewInt32(int32(response.Status)))
	headers := ctx.NewObject()
	for name, value := range response.Headers {
		headers.Set(name, ctx.NewString(value))
	}
	result.Set("headers", headers)
	bytesValue := message.Get("bytes")
	if bytesValue != nil {
		defer bytesValue.Free()
	}
	if bytesValue != nil && bytesValue.Bool() {
		result.Set("body", ctx.NewArrayBuffer(response.Body))
	} else {
		result.Set("body", ctx.NewString(string(response.Body)))
	}
	return result
}

func validStructuredError(value StructuredError) bool {
	switch value.Code {
	case "auth_required", "rate_limited", "transient", "contract_drift", "incomplete_snapshot", "snapshot_changed", "not_found", "forbidden", "extension_failure":
	default:
		return false
	}
	if value.SafeMessage == "" || len([]byte(value.SafeMessage)) > 512 {
		return false
	}
	if value.RetryAfterSeconds != nil && (*value.RetryAfterSeconds < 0 || *value.RetryAfterSeconds > 604800) {
		return false
	}
	return true
}

func validateJSONValue(raw json.RawMessage) error {
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	if decoder.More() {
		return ErrExtensionFailure
	}
	return validateJSONNode(value)
}

func validateJSONNode(value any) error {
	switch typed := value.(type) {
	case nil, bool, string, json.Number:
		return nil
	case []any:
		for _, child := range typed {
			if err := validateJSONNode(child); err != nil {
				return err
			}
		}
	case map[string]any:
		for _, child := range typed {
			if err := validateJSONNode(child); err != nil {
				return err
			}
		}
	default:
		return ErrExtensionFailure
	}
	return nil
}

func brokerCookies(broker *HTTPBroker) []http.Cookie {
	// A worker result keeps cookie state in the host, never in the JS output.
	// CookieJar intentionally has no enumeration API; the active origin is the
	// only approved read boundary for a scanning operation.
	if len(broker.allowedOrigins) == 0 {
		return nil
	}
	cookies := broker.jar.Cookies(broker.allowedOrigins[0])
	result := make([]http.Cookie, 0, len(cookies))
	for _, cookie := range cookies {
		if cookie != nil {
			result = append(result, *cookie)
		}
	}
	return result
}

func (broker *HTTPBroker) requestCountValue() int {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	return broker.requestCount
}

func (broker *HTTPBroker) responseBytesValue() int64 {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	return broker.totalResponse
}
