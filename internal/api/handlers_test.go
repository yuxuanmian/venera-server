package api

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"venera-server/internal/config"
	"venera-server/internal/debugrecorder"
	"venera-server/internal/store"
)

type testEnv struct {
	srv *Server
	st  *store.Store
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	dataDir := t.TempDir()
	cfg := &config.Config{DataDir: dataDir}
	st, err := store.Open(dataDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	rec, err := debugrecorder.New(dataDir, false)
	if err != nil {
		t.Fatalf("new recorder: %v", err)
	}
	t.Cleanup(func() { _ = rec.Close() })
	srv, err := NewServer(cfg, st, rec)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(filepath.Join(dataDir, "cookie.key")) })
	return &testEnv{srv: srv, st: st}
}

func doReq(t *testing.T, h http.Handler, method, path, token string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		rd = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, rd)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func decodeResp(t *testing.T, rr *httptest.ResponseRecorder, dst any) {
	t.Helper()
	if err := json.Unmarshal(rr.Body.Bytes(), dst); err != nil {
		t.Fatalf("decode response %q: %v", rr.Body.String(), err)
	}
}

func mustRegister(t *testing.T, env *testEnv, token, deviceID string) {
	t.Helper()
	rr := doReq(t, env.srv, "POST", "/api/register", token, map[string]any{
		"device_id": deviceID,
		"tz":        "Asia/Shanghai",
		"locale":    "zh-CN",
		"sources": []map[string]any{{
			"source":       "src",
			"cookie":       "cookie",
			"ua":           "ua",
			"script_hash":  "sh",
			"init_js_hash": "ih",
		}},
		"comics": []map[string]any{{
			"source":          "src",
			"comic_id":        "1",
			"due_at":          "2026-07-24T00:00:00Z",
			"last_check_time": "2026-07-23T00:00:00Z",
			"priority":        "normal",
		}},
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("register failed: %d %s", rr.Code, rr.Body.String())
	}
}

// --- health ---

func TestHealth(t *testing.T) {
	env := newTestEnv(t)
	rr := doReq(t, env.srv, "GET", "/api/health", "", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("health status = %d", rr.Code)
	}
	var resp struct {
		Data struct {
			Status string `json:"status"`
		} `json:"data"`
	}
	decodeResp(t, rr, &resp)
	if resp.Data.Status != "ok" {
		t.Fatalf("status = %q", resp.Data.Status)
	}
}

// --- register ---

func TestRegisterNewDevice(t *testing.T) {
	env := newTestEnv(t)
	rr := doReq(t, env.srv, "POST", "/api/register", "token-a", map[string]any{
		"device_id": "dev-a",
		"tz":        "Asia/Shanghai",
		"locale":    "zh-CN",
		"sources": []map[string]any{{
			"source": "src", "cookie": "c", "ua": "ua", "script_hash": "sh", "init_js_hash": "ih",
		}},
		"comics": []map[string]any{{
			"source": "src", "comic_id": "1", "due_at": "2026-07-24T00:00:00Z", "last_check_time": "2026-07-23T00:00:00Z",
		}},
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("register status = %d body=%s", rr.Code, rr.Body.String())
	}
	var resp struct {
		Data struct {
			UserID string `json:"user_id"`
		} `json:"data"`
	}
	decodeResp(t, rr, &resp)
	if resp.Data.UserID != "dev-a" {
		t.Fatalf("user_id = %q", resp.Data.UserID)
	}
}

func TestRegisterMissingDeviceID(t *testing.T) {
	env := newTestEnv(t)
	rr := doReq(t, env.srv, "POST", "/api/register", "token-a", map[string]any{
		"tz": "Asia/Shanghai",
	})
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d", rr.Code)
	}
}

func TestRegisterTZMismatch(t *testing.T) {
	env := newTestEnv(t)
	mustRegister(t, env, "token-a", "dev-a")
	rr := doReq(t, env.srv, "POST", "/api/register", "token-a", map[string]any{
		"device_id": "dev-a",
		"tz":        "America/New_York",
		"locale":    "en",
	})
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var resp struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	decodeResp(t, rr, &resp)
	if resp.Error.Code != "tz_mismatch" {
		t.Fatalf("code = %q", resp.Error.Code)
	}
}

func TestRegisterInvalidJSON(t *testing.T) {
	env := newTestEnv(t)
	req := httptest.NewRequest("POST", "/api/register", bytes.NewBufferString("{bad"))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	env.srv.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", rr.Code)
	}
}

func TestRegisterTombstoneSkip(t *testing.T) {
	env := newTestEnv(t)
	mustRegister(t, env, "token-a", "dev-a")
	// remove comic 1
	rr := doReq(t, env.srv, "POST", "/api/sync", "token-a", map[string]any{
		"events": []map[string]any{{
			"event_id": "e1", "type": "remove", "source": "src", "comic_id": "1",
		}},
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("sync remove status = %d", rr.Code)
	}
	// full snapshot again should skip tombstoned key
	rr = doReq(t, env.srv, "POST", "/api/register", "token-a", map[string]any{
		"device_id": "dev-a",
		"tz":        "Asia/Shanghai",
		"sources": []map[string]any{{
			"source": "src", "cookie": "c", "ua": "ua", "script_hash": "sh", "init_js_hash": "ih",
		}},
		"comics": []map[string]any{{
			"source": "src", "comic_id": "1", "due_at": "2026-07-25T00:00:00Z",
		}},
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("register status = %d", rr.Code)
	}
	_, err := env.st.GetMirror("dev-a", "src", "1")
	if err == nil {
		t.Fatalf("mirror should not exist after tombstone skip")
	}
}

// --- auth ---

func TestAuthRequired(t *testing.T) {
	env := newTestEnv(t)
	cases := []struct{ method, path string }{
		{"POST", "/api/sync"},
		{"POST", "/api/client-results"},
		{"POST", "/api/scripts"},
		{"GET", "/api/results"},
		{"POST", "/api/refresh"},
		{"POST", "/api/pairing-codes"},
		{"GET", "/api/devices"},
	}
	for _, c := range cases {
		rr := doReq(t, env.srv, c.method, c.path, "", nil)
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("%s %s status = %d", c.method, c.path, rr.Code)
		}
	}
}

// --- pairing ---

func TestPairingFlow(t *testing.T) {
	env := newTestEnv(t)
	mustRegister(t, env, "token-a", "dev-a")
	rr := doReq(t, env.srv, "POST", "/api/pairing-codes", "token-a", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("pairing code status = %d body=%s", rr.Code, rr.Body.String())
	}
	var codeResp struct {
		Data struct {
			Code string `json:"code"`
		} `json:"data"`
	}
	decodeResp(t, rr, &codeResp)
	if codeResp.Data.Code == "" {
		t.Fatal("empty pairing code")
	}

	// New device B joins with code.
	rr = doReq(t, env.srv, "POST", "/api/register", "token-b", map[string]any{
		"device_id": "dev-b",
		"join_code": codeResp.Data.Code,
		"tz":        "Asia/Shanghai",
		"locale":    "zh-CN",
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("register B status = %d body=%s", rr.Code, rr.Body.String())
	}
	var regResp struct {
		Data struct {
			UserID string `json:"user_id"`
		} `json:"data"`
	}
	decodeResp(t, rr, &regResp)
	if regResp.Data.UserID != "dev-a" {
		t.Fatalf("B user_id = %q, want dev-a", regResp.Data.UserID)
	}
}

func TestPairingCodeInvalid(t *testing.T) {
	env := newTestEnv(t)
	rr := doReq(t, env.srv, "POST", "/api/register", "token-b", map[string]any{
		"device_id": "dev-b",
		"join_code": "BADCODE",
		"tz":        "Asia/Shanghai",
	})
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d", rr.Code)
	}
}

func TestPairingNotAllowed(t *testing.T) {
	env := newTestEnv(t)
	mustRegister(t, env, "token-a", "dev-a")
	mustRegister(t, env, "token-b", "dev-b")
	rr := doReq(t, env.srv, "POST", "/api/pairing-codes", "token-a", nil)
	var codeResp struct {
		Data struct {
			Code string `json:"code"`
		} `json:"data"`
	}
	decodeResp(t, rr, &codeResp)
	rr = doReq(t, env.srv, "POST", "/api/register", "token-b", map[string]any{
		"device_id": "dev-b",
		"join_code": codeResp.Data.Code,
		"tz":        "Asia/Shanghai",
	})
	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d", rr.Code)
	}
}

// --- sync ---

func TestSyncEvents(t *testing.T) {
	env := newTestEnv(t)
	mustRegister(t, env, "token-a", "dev-a")

	rr := doReq(t, env.srv, "POST", "/api/sync", "token-a", map[string]any{
		"events": []map[string]any{
			{
				"event_id": "e-add", "type": "add",
				"comic": map[string]any{"source": "src", "comic_id": "2", "due_at": "2026-07-25T00:00:00Z"},
			},
			{
				"event_id": "e-upd", "type": "update_state",
				"comic": map[string]any{"source": "src", "comic_id": "1", "priority": "urgent"},
			},
			{
				"event_id": "e-check", "type": "check_now", "source": "src", "comic_id": "1",
			},
		},
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("sync status = %d body=%s", rr.Code, rr.Body.String())
	}
	var resp struct {
		Data struct {
			Acked []string `json:"acked_event_ids"`
		} `json:"data"`
	}
	decodeResp(t, rr, &resp)
	if len(resp.Data.Acked) != 3 {
		t.Fatalf("acked = %v", resp.Data.Acked)
	}

	// add created mirror
	if _, err := env.st.GetMirror("dev-a", "src", "2"); err != nil {
		t.Fatalf("mirror 2 not found: %v", err)
	}
	// update_state changed priority
	m, err := env.st.GetMirror("dev-a", "src", "1")
	if err != nil {
		t.Fatalf("mirror 1 not found: %v", err)
	}
	if m.Priority != "urgent" {
		t.Fatalf("priority = %q", m.Priority)
	}
}

func TestSyncDuplicateEvent(t *testing.T) {
	env := newTestEnv(t)
	mustRegister(t, env, "token-a", "dev-a")
	body := map[string]any{
		"events": []map[string]any{{
			"event_id": "dup", "type": "check_now", "source": "src", "comic_id": "1",
		}},
	}
	rr1 := doReq(t, env.srv, "POST", "/api/sync", "token-a", body)
	rr2 := doReq(t, env.srv, "POST", "/api/sync", "token-a", body)
	if rr1.Code != http.StatusOK || rr2.Code != http.StatusOK {
		t.Fatalf("statuses = %d %d", rr1.Code, rr2.Code)
	}
	var resp struct {
		Data struct {
			Acked []string `json:"acked_event_ids"`
		} `json:"data"`
	}
	decodeResp(t, rr2, &resp)
	if len(resp.Data.Acked) != 1 || resp.Data.Acked[0] != "dup" {
		t.Fatalf("dup acked = %v", resp.Data.Acked)
	}
}

func TestSyncInvalidEventType(t *testing.T) {
	env := newTestEnv(t)
	mustRegister(t, env, "token-a", "dev-a")
	rr := doReq(t, env.srv, "POST", "/api/sync", "token-a", map[string]any{
		"events": []map[string]any{{"event_id": "e1", "type": "bogus", "source": "src", "comic_id": "1"}},
	})
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d", rr.Code)
	}
}

// --- client-results ---

func TestClientResultsSuccess(t *testing.T) {
	env := newTestEnv(t)
	mustRegister(t, env, "token-a", "dev-a")
	rr := doReq(t, env.srv, "POST", "/api/client-results", "token-a", map[string]any{
		"results": []map[string]any{{
			"client_result_id": "r1",
			"source":           "src",
			"comic_id":         "1",
			"scan_time":        "2026-07-23T01:00:00Z",
			"script_hash":      "sh",
			"init_js_hash":     "ih",
			"cookie_hash":      sha256Hex("cookie"),
			"outcome":          "success",
			"payload":          map[string]any{"title": "x"},
		}},
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("client-results status = %d body=%s", rr.Code, rr.Body.String())
	}
	var resp struct {
		Data struct {
			Accepted []string `json:"accepted"`
			Rejected []any    `json:"rejected"`
		} `json:"data"`
	}
	decodeResp(t, rr, &resp)
	if len(resp.Data.Accepted) != 1 || len(resp.Data.Rejected) != 0 {
		t.Fatalf("accepted=%v rejected=%v", resp.Data.Accepted, resp.Data.Rejected)
	}
}

func TestClientResultsDuplicate(t *testing.T) {
	env := newTestEnv(t)
	mustRegister(t, env, "token-a", "dev-a")
	body := map[string]any{
		"results": []map[string]any{{
			"client_result_id": "r1", "source": "src", "comic_id": "1", "scan_time": "2026-07-23T01:00:00Z",
			"script_hash": "sh", "init_js_hash": "ih", "cookie_hash": sha256Hex("cookie"), "outcome": "success",
		}},
	}
	doReq(t, env.srv, "POST", "/api/client-results", "token-a", body)
	rr := doReq(t, env.srv, "POST", "/api/client-results", "token-a", body)
	var resp struct {
		Data struct {
			Accepted []string `json:"accepted"`
			Rejected []any    `json:"rejected"`
		} `json:"data"`
	}
	decodeResp(t, rr, &resp)
	if len(resp.Data.Accepted) != 1 {
		t.Fatalf("accepted = %v", resp.Data.Accepted)
	}
}

func TestClientResultsVersionRejected(t *testing.T) {
	env := newTestEnv(t)
	mustRegister(t, env, "token-a", "dev-a")
	rr := doReq(t, env.srv, "POST", "/api/client-results", "token-a", map[string]any{
		"results": []map[string]any{{
			"client_result_id": "r1", "source": "src", "comic_id": "1", "scan_time": "2026-07-23T01:00:00Z",
			"script_hash": "old", "init_js_hash": "ih", "cookie_hash": sha256Hex("cookie"), "outcome": "success",
		}},
	})
	var resp struct {
		Data struct {
			Accepted []string         `json:"accepted"`
			Rejected []map[string]any `json:"rejected"`
		} `json:"data"`
	}
	decodeResp(t, rr, &resp)
	if len(resp.Data.Rejected) != 1 || resp.Data.Rejected[0]["reason"] != "version_rejected" {
		t.Fatalf("rejected = %v", resp.Data.Rejected)
	}
}

func TestClientResultsSourceNotFound(t *testing.T) {
	env := newTestEnv(t)
	mustRegister(t, env, "token-a", "dev-a")
	rr := doReq(t, env.srv, "POST", "/api/client-results", "token-a", map[string]any{
		"results": []map[string]any{{
			"client_result_id": "r1", "source": "nosuch", "comic_id": "1", "scan_time": "2026-07-23T01:00:00Z",
			"script_hash": "sh", "init_js_hash": "ih", "cookie_hash": "x", "outcome": "success",
		}},
	})
	var resp struct {
		Data struct {
			Rejected []map[string]any `json:"rejected"`
		} `json:"data"`
	}
	decodeResp(t, rr, &resp)
	if len(resp.Data.Rejected) != 1 || resp.Data.Rejected[0]["reason"] != "source_not_found" {
		t.Fatalf("rejected = %v", resp.Data.Rejected)
	}
}

func TestClientResultsMissingFields(t *testing.T) {
	env := newTestEnv(t)
	mustRegister(t, env, "token-a", "dev-a")
	rr := doReq(t, env.srv, "POST", "/api/client-results", "token-a", map[string]any{
		"results": []map[string]any{{"client_result_id": "r1"}},
	})
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d", rr.Code)
	}
}

// --- scripts ---

func TestScriptsSuccess(t *testing.T) {
	env := newTestEnv(t)
	mustRegister(t, env, "token-a", "dev-a")
	content := "class A {}"
	rr := doReq(t, env.srv, "POST", "/api/scripts", "token-a", map[string]any{
		"kind": "comic_source", "hash": sha256Hex(content), "content": content,
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("scripts status = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestScriptsHashMismatch(t *testing.T) {
	env := newTestEnv(t)
	mustRegister(t, env, "token-a", "dev-a")
	rr := doReq(t, env.srv, "POST", "/api/scripts", "token-a", map[string]any{
		"kind": "comic_source", "hash": "deadbeef", "content": "class A {}",
	})
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d", rr.Code)
	}
}

func TestScriptsTooLarge(t *testing.T) {
	env := newTestEnv(t)
	mustRegister(t, env, "token-a", "dev-a")
	big := bytes.Repeat([]byte("a"), maxScriptSize+1)
	rr := doReq(t, env.srv, "POST", "/api/scripts", "token-a", map[string]any{
		"kind": "comic_source", "hash": sha256Hex(string(big)), "content": string(big),
	})
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d", rr.Code)
	}
}

// --- results ---

func TestResultsPagination(t *testing.T) {
	env := newTestEnv(t)
	mustRegister(t, env, "token-a", "dev-a")
	// Insert two results directly for deterministic pagination.
	now := time.Now().UTC().Format(time.RFC3339)
	_, _ = env.st.InsertResult(store.Result{
		UserID: "dev-a", Source: "src", ComicID: "1",
		ClientResultID: sqlNull("r1"), ScanTime: now, Outcome: "success",
		Payload: sqlNull(`{"n":1}`),
	})
	_, _ = env.st.InsertResult(store.Result{
		UserID: "dev-a", Source: "src", ComicID: "1",
		ClientResultID: sqlNull("r2"), ScanTime: now, Outcome: "success",
		Payload: sqlNull(`{"n":2}`),
	})
	rr := doReq(t, env.srv, "GET", "/api/results?cursor=0&limit=1", "token-a", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	var resp struct {
		Data struct {
			Cursor  int64 `json:"cursor"`
			HasMore bool  `json:"has_more"`
			Results []struct {
				ResultID int64 `json:"result_id"`
			} `json:"results"`
		} `json:"data"`
	}
	decodeResp(t, rr, &resp)
	if len(resp.Data.Results) != 1 || resp.Data.Results[0].ResultID == 0 {
		t.Fatalf("results = %+v", resp.Data.Results)
	}
	if !resp.Data.HasMore {
		t.Fatal("has_more should be true")
	}
}

// --- refresh ---

func TestRefresh(t *testing.T) {
	env := newTestEnv(t)
	mustRegister(t, env, "token-a", "dev-a")
	rr := doReq(t, env.srv, "POST", "/api/refresh", "token-a", map[string]any{"scope": "all"})
	if rr.Code != http.StatusAccepted {
		t.Fatalf("refresh status = %d body=%s", rr.Code, rr.Body.String())
	}
	var resp struct {
		Data struct {
			Queued int `json:"queued"`
		} `json:"data"`
	}
	decodeResp(t, rr, &resp)
	if resp.Data.Queued != 1 {
		t.Fatalf("queued = %d", resp.Data.Queued)
	}
}

func TestRefreshInvalidScope(t *testing.T) {
	env := newTestEnv(t)
	mustRegister(t, env, "token-a", "dev-a")
	rr := doReq(t, env.srv, "POST", "/api/refresh", "token-a", map[string]any{"scope": "partial"})
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d", rr.Code)
	}
}

// --- devices ---

func TestDevicesListAndDelete(t *testing.T) {
	env := newTestEnv(t)
	mustRegister(t, env, "token-a", "dev-a")
	rr := doReq(t, env.srv, "GET", "/api/devices", "token-a", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("list status = %d", rr.Code)
	}
	var resp struct {
		Data struct {
			Devices []map[string]any `json:"devices"`
		} `json:"data"`
	}
	decodeResp(t, rr, &resp)
	if len(resp.Data.Devices) != 1 {
		t.Fatalf("devices = %v", resp.Data.Devices)
	}

	rr = doReq(t, env.srv, "DELETE", "/api/devices/dev-a", "token-a", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("delete status = %d body=%s", rr.Code, rr.Body.String())
	}
	rr = doReq(t, env.srv, "GET", "/api/devices", "token-a", nil)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("after delete, token should be invalid, status = %d", rr.Code)
	}
}

// --- helpers ---

func sqlNull(s string) sql.NullString {
	return sql.NullString{String: s, Valid: true}
}
