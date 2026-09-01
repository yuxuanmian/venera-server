package v2api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type requestContextKey string

const (
	requestIDContextKey    requestContextKey = "v2.requestId"
	clientContextKey       requestContextKey = "v2.client"
	pendingTokenContextKey requestContextKey = "v2.pendingTokenDigest"
)

type apiMeta struct {
	RequestID  string `json:"requestId"`
	ServerTime string `json:"serverTime"`
}

type apiSuccess struct {
	Data any     `json:"data"`
	Meta apiMeta `json:"meta"`
}

type apiErrorBody struct {
	Code         string         `json:"code"`
	Message      string         `json:"message"`
	Retryable    bool           `json:"retryable"`
	RetryAfterMS *int64         `json:"retryAfterMs"`
	Details      map[string]any `json:"details,omitempty"`
}

type apiErrorResponse struct {
	Error apiErrorBody `json:"error"`
	Meta  apiMeta      `json:"meta"`
}

func (router *Router) commonMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requestID := router.newRequestID()
		w.Header().Set("X-Request-Id", requestID)
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Venera-Protocol", "2")
		w.Header().Set("X-Venera-Min-App-Version", router.cfg.MinAppVersion)
		request = request.WithContext(contextWithRequestID(request.Context(), requestID))
		if request.Body != nil && request.Method != http.MethodGet && router.cfg.HTTPBodyLimit > 0 {
			request.Body = http.MaxBytesReader(w, request.Body, router.cfg.HTTPBodyLimit)
		}
		defer func() {
			if recovered := recover(); recovered != nil {
				// Do not serialize the panic value: it may contain a cookie, a
				// request body, or a filesystem path.
				writeAPIError(router, w, request, http.StatusInternalServerError, "internal_error", "internal server error", false, nil)
			}
		}()
		if !isAdminRequest(request) {
			major, ok := requestProtocolMajor(request.Header.Get("X-Venera-Protocol"))
			if !ok || major != 2 {
				writeAPIError(router, w, request, http.StatusUpgradeRequired, "protocol_incompatible", "protocol major is not supported", false, nil)
				return
			}
		}
		next.ServeHTTP(w, request)
	})
}

func isAdminRequest(request *http.Request) bool {
	return request.URL.Path == "/admin" || strings.HasPrefix(request.URL.Path, "/admin/")
}

func (router *Router) newRequestID() string {
	sequence := router.requestID.Add(1)
	return fmt.Sprintf("req_%d_%d", time.Now().UnixNano(), sequence)
}

func requestProtocolMajor(value string) (int, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if dot := strings.IndexByte(value, '.'); dot >= 0 {
		value = value[:dot]
	}
	major, err := strconv.Atoi(value)
	return major, err == nil && major >= 0
}

func contextWithRequestID(ctx context.Context, value string) context.Context {
	return context.WithValue(ctx, requestIDContextKey, value)
}

func requestIDFromContext(request *http.Request) string {
	if value, ok := request.Context().Value(requestIDContextKey).(string); ok {
		return value
	}
	return request.Header.Get("X-Request-Id")
}

func (router *Router) serverNow() time.Time {
	if router.repo != nil && router.repo.DB() != nil {
		return router.repo.DB().Now()
	}
	return time.Now().UTC()
}

func writeJSON(router *Router, w http.ResponseWriter, request *http.Request, status int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(apiSuccess{Data: data, Meta: apiMeta{
		RequestID: requestIDFromContext(request), ServerTime: router.serverNow().Format(time.RFC3339Nano),
	}})
}

func writeAPIError(router *Router, w http.ResponseWriter, request *http.Request, status int, code, message string, retryable bool, details map[string]any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(apiErrorResponse{
		Error: apiErrorBody{Code: code, Message: message, Retryable: retryable, RetryAfterMS: nil, Details: details},
		Meta:  apiMeta{RequestID: requestIDFromContext(request), ServerTime: router.serverNow().Format(time.RFC3339Nano)},
	})
}

func decodeJSON(w http.ResponseWriter, request *http.Request, target any) error {
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}
