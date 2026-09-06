package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"venera-server/internal/catalog"
)

type CatalogHandler struct {
	manager *catalog.Manager
	web     http.Handler
}

func NewCatalogHandler(manager *catalog.Manager, web http.Handler) *CatalogHandler {
	return &CatalogHandler{manager: manager, web: web}
}

func NewCatalogRouter(manager *catalog.Manager, web http.Handler) http.Handler {
	return NewCatalogHandler(manager, web)
}

func (h *CatalogHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/api/health":
		if r.Method != http.MethodGet {
			catalogMethodNotAllowed(w, http.MethodGet)
			return
		}
		catalogWriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case "/api/catalog/authority":
		if r.Method != http.MethodGet {
			catalogMethodNotAllowed(w, http.MethodGet)
			return
		}
		h.authority(w)
	case "/admin":
		if r.Method != http.MethodGet {
			catalogMethodNotAllowed(w, http.MethodGet)
			return
		}
		http.Redirect(w, r, "/admin/", http.StatusPermanentRedirect)
	case "/admin/api/catalog/status":
		if r.Method != http.MethodGet {
			catalogMethodNotAllowed(w, http.MethodGet)
			return
		}
		catalogWriteJSONNoStore(w, http.StatusOK, h.manager.Status())
	case "/admin/api/catalog/check":
		if r.Method != http.MethodPost {
			catalogMethodNotAllowed(w, http.MethodPost)
			return
		}
		if err := decodeEmptyObject(r); err != nil {
			catalogWriteError(w, http.StatusBadRequest, "catalog_invalid_request", "请求体必须是空 JSON 对象")
			return
		}
		candidate, err := h.manager.Check(r.Context())
		if err != nil {
			writeCatalogError(w, err)
			return
		}
		active, ok := h.manager.Authority()
		catalogWriteJSON(w, http.StatusOK, map[string]any{
			"candidate":    candidate,
			"sameAsActive": ok && catalog.SameIdentity(active, candidate.CatalogPointer),
		})
	case "/admin/api/catalog/activate":
		if r.Method != http.MethodPost {
			catalogMethodNotAllowed(w, http.MethodPost)
			return
		}
		var request struct {
			CatalogID string `json:"catalogId"`
			Revision  string `json:"revision"`
		}
		if err := decodeCatalogJSON(r, &request); err != nil || strings.TrimSpace(request.CatalogID) == "" || strings.TrimSpace(request.Revision) == "" {
			catalogWriteError(w, http.StatusBadRequest, "catalog_invalid_request", "请求体必须包含 catalogId 和 revision")
			return
		}
		active, err := h.manager.Activate(r.Context(), request.CatalogID, request.Revision)
		if err != nil {
			writeCatalogError(w, err)
			return
		}
		catalogWriteJSON(w, http.StatusOK, map[string]any{"active": active})
	case "/admin/":
		if r.Method != http.MethodGet {
			catalogMethodNotAllowed(w, http.MethodGet)
			return
		}
		if h.web == nil {
			http.NotFound(w, r)
			return
		}
		h.web.ServeHTTP(w, r)
	default:
		// The embedded handler is mounted below /admin/ in production. Route
		// every asset path through that same StripPrefix composition while
		// keeping /admin/api/* in the explicit API switch above. This prevents
		// the page shell from being served for unknown API paths.
		if strings.HasPrefix(r.URL.Path, "/admin/") && h.web != nil {
			h.web.ServeHTTP(w, r)
			return
		}
		http.NotFound(w, r)
	}
}

func (h *CatalogHandler) authority(w http.ResponseWriter) {
	if err := h.manager.StateError(); err != nil {
		catalogWriteError(w, http.StatusServiceUnavailable, "catalog_state_invalid", "漫画源发布状态损坏")
		return
	}
	pointer, ok := h.manager.Authority()
	if !ok {
		catalogWriteError(w, http.StatusServiceUnavailable, "catalog_not_activated", "尚未激活漫画源配置")
		return
	}
	catalogWriteJSONNoStore(w, http.StatusOK, map[string]any{
		"catalogId":      pointer.CatalogID,
		"activeRevision": pointer.Revision,
		"indexUrl":       pointer.IndexURL,
	})
}

func decodeEmptyObject(r *http.Request) error {
	var body map[string]json.RawMessage
	if err := decodeCatalogJSON(r, &body); err != nil {
		return err
	}
	if body == nil || len(body) != 0 {
		return errors.New("request must be an empty object")
	}
	return nil
}

func decodeCatalogJSON(r *http.Request, target any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, 16<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("request contains trailing JSON")
	}
	return nil
}

func writeCatalogError(w http.ResponseWriter, err error) {
	var operationErr *catalog.OperationError
	if errors.As(err, &operationErr) {
		status := http.StatusInternalServerError
		switch operationErr.Code {
		case "catalog_busy":
			status = http.StatusConflict
		case "catalog_snapshot_not_found":
			status = http.StatusNotFound
		case "catalog_fetch_failed":
			status = http.StatusBadGateway
		case "catalog_fetch_timeout":
			status = http.StatusGatewayTimeout
		case "catalog_invalid":
			status = http.StatusUnprocessableEntity
		case "catalog_state_invalid":
			status = http.StatusServiceUnavailable
		}
		catalogWriteError(w, status, operationErr.Code, operationErr.Message)
		return
	}
	catalogWriteError(w, http.StatusUnprocessableEntity, "catalog_invalid", err.Error())
}

func catalogWriteJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func catalogWriteJSONNoStore(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Cache-Control", "no-store")
	catalogWriteJSON(w, status, value)
}

func catalogWriteError(w http.ResponseWriter, status int, code, message string) {
	catalogWriteJSON(w, status, map[string]any{"error": catalog.ErrorInfo{Code: code, Message: message}})
}

func catalogMethodNotAllowed(w http.ResponseWriter, method string) {
	w.Header().Set("Allow", method)
	catalogWriteError(w, http.StatusMethodNotAllowed, "method_not_allowed", "不支持的请求方法")
}
