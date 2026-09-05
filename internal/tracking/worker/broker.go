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
	"sync"
	"time"
)

var (
	ErrHTTPOriginNotAllowed = errors.New("http origin is not allowed")
	ErrHTTPSecretHeader     = errors.New("secret HTTP header is not available to scanning")
	ErrHTTPBudgetExceeded   = errors.New("HTTP budget exceeded")
	ErrHTTPResponseTooLarge = errors.New("HTTP response exceeds budget")
	ErrHTTPRequestTooLarge  = errors.New("HTTP request exceeds budget")
	ErrHTTPRedirectBlocked  = errors.New("HTTP redirect origin is not allowed")
)

type HTTPRequest struct {
	Method  string
	URL     string
	Headers map[string]string
	Body    []byte
}

type HTTPResponse struct {
	Status  int
	Headers map[string]string
	Body    []byte
}

type BrokerConfig struct {
	AllowedOrigins          []string
	UserAgent               string
	MaxRequests             int
	MaxRequestBodyBytes     int64
	MaxResponseBytes        int64
	MaxTotalResponseBytes   int64
	MinRequestStartInterval time.Duration
	Client                  *http.Client
	CookieJar               http.CookieJar
	Clock                   func() time.Time
}

type HTTPBroker struct {
	allowedOrigins        []*url.URL
	userAgent             string
	maxRequests           int
	maxRequestBodyBytes   int64
	maxResponseBytes      int64
	maxTotalResponseBytes int64
	minInterval           time.Duration
	client                *http.Client
	jar                   http.CookieJar
	clock                 func() time.Time

	mu            sync.Mutex
	requestCount  int
	totalResponse int64
	lastRequestAt time.Time
}

func NewHTTPBroker(config BrokerConfig) (*HTTPBroker, error) {
	if len(config.AllowedOrigins) == 0 {
		return nil, ErrHTTPOriginNotAllowed
	}
	origins := make([]*url.URL, 0, len(config.AllowedOrigins))
	for _, raw := range config.AllowedOrigins {
		parsed, err := url.Parse(raw)
		if err != nil || !strings.EqualFold(parsed.Scheme, "https") || parsed.Host == "" || parsed.User != nil || parsed.Path != "" && parsed.Path != "/" || parsed.RawQuery != "" || parsed.Fragment != "" {
			return nil, ErrHTTPOriginNotAllowed
		}
		origins = append(origins, parsed)
	}
	if config.UserAgent == "" {
		config.UserAgent = "VeneraServer/2"
	}
	if config.MaxRequests <= 0 {
		config.MaxRequests = 4
	}
	if config.MaxRequestBodyBytes <= 0 {
		config.MaxRequestBodyBytes = 1 << 20
	}
	if config.MaxResponseBytes <= 0 {
		config.MaxResponseBytes = 8 << 20
	}
	if config.MaxTotalResponseBytes <= 0 {
		config.MaxTotalResponseBytes = config.MaxResponseBytes
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	jar := config.CookieJar
	if jar == nil {
		var err error
		jar, err = cookiejar.New(nil)
		if err != nil {
			return nil, err
		}
	}
	client := config.Client
	if client == nil {
		client = &http.Client{}
	}
	broker := &HTTPBroker{
		allowedOrigins: origins, userAgent: config.UserAgent, maxRequests: config.MaxRequests,
		maxRequestBodyBytes: config.MaxRequestBodyBytes, maxResponseBytes: config.MaxResponseBytes,
		maxTotalResponseBytes: config.MaxTotalResponseBytes, minInterval: config.MinRequestStartInterval,
		client: client, jar: jar, clock: config.Clock,
	}
	return broker, nil
}

func (broker *HTTPBroker) Do(ctx context.Context, request HTTPRequest) (HTTPResponse, error) {
	parsed, err := url.Parse(request.URL)
	if err != nil || !broker.allowedOrigin(parsed) {
		return HTTPResponse{}, ErrHTTPOriginNotAllowed
	}
	if request.Method == "" {
		request.Method = http.MethodGet
	}
	if len(request.Body) > int(broker.maxRequestBodyBytes) {
		return HTTPResponse{}, ErrHTTPRequestTooLarge
	}
	for name := range request.Headers {
		switch strings.ToLower(name) {
		case "authorization", "cookie", "proxy-authorization", "set-cookie":
			return HTTPResponse{}, ErrHTTPSecretHeader
		}
	}
	if err := broker.reserveRequest(ctx); err != nil {
		return HTTPResponse{}, err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, request.Method, parsed.String(), bytesReader(request.Body))
	if err != nil {
		return HTTPResponse{}, err
	}
	for name, value := range request.Headers {
		httpRequest.Header.Set(name, value)
	}
	httpRequest.Header.Set("User-Agent", broker.userAgent)
	client := *broker.client
	previousRedirect := client.CheckRedirect
	client.Jar = broker.jar
	client.CheckRedirect = func(next *http.Request, history []*http.Request) error {
		if !broker.allowedOrigin(next.URL) {
			return ErrHTTPRedirectBlocked
		}
		next.Header.Del("Authorization")
		next.Header.Del("Cookie")
		if previousRedirect != nil {
			return previousRedirect(next, history)
		}
		return nil
	}
	response, err := client.Do(httpRequest)
	if err != nil {
		if errors.Is(err, ErrHTTPRedirectBlocked) {
			return HTTPResponse{}, ErrHTTPRedirectBlocked
		}
		return HTTPResponse{}, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, broker.maxResponseBytes+1))
	if err != nil {
		return HTTPResponse{}, err
	}
	if int64(len(body)) > broker.maxResponseBytes {
		return HTTPResponse{}, ErrHTTPResponseTooLarge
	}
	broker.mu.Lock()
	broker.totalResponse += int64(len(body))
	total := broker.totalResponse
	broker.mu.Unlock()
	if total > broker.maxTotalResponseBytes {
		return HTTPResponse{}, ErrHTTPBudgetExceeded
	}
	headers := make(map[string]string)
	for name, values := range response.Header {
		lower := strings.ToLower(name)
		if lower == "set-cookie" || lower == "authorization" || lower == "cookie" {
			continue
		}
		if len(values) > 0 {
			headers[name] = values[0]
		}
	}
	return HTTPResponse{Status: response.StatusCode, Headers: headers, Body: body}, nil
}

func (broker *HTTPBroker) reserveRequest(ctx context.Context) error {
	for {
		broker.mu.Lock()
		if broker.requestCount >= broker.maxRequests {
			broker.mu.Unlock()
			return ErrHTTPBudgetExceeded
		}
		now := broker.clock()
		wait := time.Duration(0)
		if !broker.lastRequestAt.IsZero() {
			wait = broker.minInterval - now.Sub(broker.lastRequestAt)
			if wait < 0 {
				wait = 0
			}
		}
		if wait == 0 {
			broker.requestCount++
			broker.lastRequestAt = now
			broker.mu.Unlock()
			return nil
		}
		broker.mu.Unlock()
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (broker *HTTPBroker) allowedOrigin(target *url.URL) bool {
	if target == nil || !strings.EqualFold(target.Scheme, "https") {
		return false
	}
	for _, origin := range broker.allowedOrigins {
		if strings.EqualFold(origin.Scheme, target.Scheme) && strings.EqualFold(origin.Host, target.Host) {
			return true
		}
	}
	return false
}

func (broker *HTTPBroker) CookieJar() http.CookieJar {
	return broker.jar
}

func bytesReader(value []byte) io.Reader {
	if len(value) == 0 {
		return nil
	}
	return bytes.NewReader(value)
}
