package api

import (
	"net/http"
	"testing"

	"venera-server/internal/config"
	"venera-server/internal/debugrecorder"
	"venera-server/internal/store"
)

func TestDebugOpenAuthBypassesClientAndAdminAuthentication(t *testing.T) {
	dataDir := t.TempDir()
	cfg := &config.Config{
		DataDir:       dataDir,
		AdminToken:    "admin-secret",
		DebugOpenAuth: true,
		DebugUserID:   "debug-user",
		DebugDeviceID: "debug-device",
		DebugUserTZ:   "UTC",
		DebugLocale:   "en-US",
	}
	st, err := store.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	recorder, err := debugrecorder.New(dataDir, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recorder.Close() })
	srv, err := NewServer(cfg, st, recorder)
	if err != nil {
		t.Fatal(err)
	}

	tracking := doReq(t, srv, http.MethodGet, "/api/tracking/authority", "", nil)
	if tracking.Code != http.StatusServiceUnavailable {
		t.Fatalf("tracking auth bypass status = %d body=%s", tracking.Code, tracking.Body.String())
	}
	devices := doReq(t, srv, http.MethodGet, "/api/devices", "", nil)
	if devices.Code != http.StatusOK {
		t.Fatalf("client auth bypass status = %d body=%s", devices.Code, devices.Body.String())
	}
	var deviceResponse struct {
		Data struct {
			Devices []struct {
				DeviceID string `json:"device_id"`
			} `json:"devices"`
		} `json:"data"`
	}
	decodeResp(t, devices, &deviceResponse)
	if len(deviceResponse.Data.Devices) != 1 || deviceResponse.Data.Devices[0].DeviceID != "debug-device" {
		t.Fatalf("debug devices = %#v", deviceResponse.Data.Devices)
	}

	register := doReq(t, srv, http.MethodPost, "/api/register", "", map[string]any{
		"tz": "UTC",
	})
	if register.Code != http.StatusOK {
		t.Fatalf("open registration status = %d body=%s", register.Code, register.Body.String())
	}

	admin := doReq(t, srv, http.MethodGet, "/admin/api/stats", "", nil)
	if admin.Code != http.StatusOK {
		t.Fatalf("admin auth bypass status = %d body=%s", admin.Code, admin.Body.String())
	}
}

func TestDebugOpenAuthIsDisabledByDefault(t *testing.T) {
	env := newTestEnv(t)
	devices := doReq(t, env.srv, http.MethodGet, "/api/devices", "", nil)
	if devices.Code != http.StatusUnauthorized {
		t.Fatalf("default client auth status = %d body=%s", devices.Code, devices.Body.String())
	}
}
