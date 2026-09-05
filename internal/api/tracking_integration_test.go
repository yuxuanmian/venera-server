package api

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"venera-server/internal/config"
	"venera-server/internal/debugrecorder"
	"venera-server/internal/store"
	trackingcatalog "venera-server/internal/tracking/catalog"
)

func TestTrackingRoutesUseExistingAuthenticationAndCatalogLifecycle(t *testing.T) {
	dataDir := t.TempDir()
	catalogDir := t.TempDir()
	index := []map[string]any{{
		"key":      "manwa",
		"fileName": "manwa.js",
		"version":  "1.0.6",
		"cloudTracking": map[string]any{
			"scanner": "scanner.js",
		},
	}}
	data, err := json.Marshal(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(catalogDir, "index.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(catalogDir, "manwa.js"), []byte("module.exports = {};"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(catalogDir, "scanner.js"), []byte("module.exports = {};"), 0o644); err != nil {
		t.Fatal(err)
	}
	revision := initGitCatalog(t, catalogDir)
	cfg := &config.Config{
		DataDir: dataDir,
		Tracking: trackingcatalog.Config{
			CatalogID:        "yuxuanmian/venera-configs",
			Repository:       catalogDir,
			Revision:         revision,
			ObservationLimit: 10000,
			IndexMaxBytes:    1 << 20,
		},
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

	unauthenticated := doReq(t, srv, "GET", "/api/tracking/authority", "", nil)
	if unauthenticated.Code != 401 {
		t.Fatalf("unauthenticated tracking request status=%d body=%s", unauthenticated.Code, unauthenticated.Body.String())
	}
	mustRegister(t, &testEnv{srv: srv, st: st}, "token-tracking", "device-tracking")
	authority := doReq(t, srv, "GET", "/api/tracking/authority", "token-tracking", nil)
	if authority.Code != 200 || authority.Body.String() == "" {
		t.Fatalf("authority lifecycle status=%d body=%s", authority.Code, authority.Body.String())
	}
	state := doReq(t, srv, "PUT", "/api/tracking/client-state", "token-tracking", map[string]any{
		"cloudEnabled": true,
		"interests": []map[string]any{{
			"artifact": map[string]any{"sourceKey": "manwa", "fileName": "manwa.js"},
			"comicId":  "42",
		}},
	})
	if state.Code != 200 {
		t.Fatalf("client state lifecycle status=%d body=%s", state.Code, state.Body.String())
	}
	observations := doReq(t, srv, "GET", "/api/tracking/observations", "token-tracking", nil)
	if observations.Code != 200 || observations.Header().Get("ETag") == "" {
		t.Fatalf("observation lifecycle status=%d etag=%q body=%s", observations.Code, observations.Header().Get("ETag"), observations.Body.String())
	}
}

func TestTrackingRoutesComposeWithProductionScannerLifecycle(t *testing.T) {
	dataDir := t.TempDir()
	catalogDir := t.TempDir()
	index := `[{"key":"manwa","fileName":"manwa.js","version":"1.0.0",
      "cloudTracking":{"scanner":"scanner.js"}}]`
	if err := os.WriteFile(filepath.Join(catalogDir, "index.json"), []byte(index), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(catalogDir, "manwa.js"), []byte("module.exports = {};"), 0o644); err != nil {
		t.Fatal(err)
	}
	const scanner = `(() => {
  SourceServerExtensions.register({
    extensionId: "fixture.scanning",
    artifactId: "manwa",
    sourceKey: "manwa",
    fileName: "manwa.js",
    apiVersion: 1,
    capabilities: { scanning: {
      version: 1,
      probeAccount: async () => ({
        identity: {scheme: "manwa-username-v1", value: "fixture-account"},
        display: {name: "Fixture account", secondary: "fixture-account"},
        attributes: {accountLevel: 1},
        visibilityScope: "manwa:level:1",
        sessionPatch: null
      }),
      scanFavoriteSnapshotSlice: async () => ({
        expectedTotal: 1,
        items: [{comicId: "42", favoriteUpdate: {
          state: {latestChapterId: "chapter-42"}, sourceUnread: true
        }}],
        complete: true,
        checkpoint: null,
        sessionPatch: null
      })
    }}
  });
})();`
	if err := os.WriteFile(filepath.Join(catalogDir, "scanner.js"), []byte(scanner), 0o644); err != nil {
		t.Fatal(err)
	}
	revision := initGitCatalog(t, catalogDir)
	cfg := &config.Config{
		DataDir:     dataDir,
		WorkerCount: 1,
		Tracking: trackingcatalog.Config{
			CatalogID:        "yuxuanmian/venera-configs",
			Repository:       catalogDir,
			Revision:         revision,
			CacheDir:         t.TempDir(),
			ObservationLimit: 10000,
			IndexMaxBytes:    1 << 20,
		},
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
	token := "token-production-tracking"
	deviceID := "device-production-tracking"
	registered := doReq(t, srv, "POST", "/api/register", token, map[string]any{
		"device_id": deviceID,
		"tz":        "Asia/Shanghai",
		"locale":    "zh-CN",
		"sources": []map[string]any{{
			"source": "manwa",
			"cookie": "sid=fixture",
		}},
	})
	if registered.Code != 200 {
		t.Fatalf("production register status=%d body=%s", registered.Code, registered.Body.String())
	}
	state := doReq(t, srv, "PUT", "/api/tracking/client-state", token, map[string]any{
		"cloudEnabled": true,
		"interests": []map[string]any{{
			"artifact": map[string]any{"sourceKey": "manwa", "fileName": "manwa.js"},
			"comicId":  "42",
		}},
	})
	if state.Code != 200 {
		t.Fatalf("production client-state status=%d body=%s", state.Code, state.Body.String())
	}
	if err := srv.RunTrackingOnce(context.Background()); err != nil {
		t.Fatalf("production tracking run: %v", err)
	}
	observations := doReq(t, srv, "GET", "/api/tracking/observations", token, nil)
	if observations.Code != 200 {
		t.Fatalf("production observations status=%d body=%s", observations.Code, observations.Body.String())
	}
	var response struct {
		Data struct {
			Observations []struct {
				ComicID string `json:"comicId"`
			} `json:"observations"`
		} `json:"data"`
	}
	decodeResp(t, observations, &response)
	if len(response.Data.Observations) != 1 || response.Data.Observations[0].ComicID != "42" {
		t.Fatalf("production observations = %#v", response.Data.Observations)
	}
}

func TestTrackingScannerResumesSixteenOfThirtyOneAcrossServerRestart(t *testing.T) {
	dataDir := t.TempDir()
	catalogDir := t.TempDir()
	index := `[{"key":"manwa","fileName":"manwa.js","version":"1.0.0",
      "cloudTracking":{"scanner":"scanner.js"}}]`
	if err := os.WriteFile(filepath.Join(catalogDir, "index.json"), []byte(index), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(catalogDir, "manwa.js"), []byte("module.exports = {};"), 0o644); err != nil {
		t.Fatal(err)
	}
	const scanner = `(() => {
  const allItems = Array.from({length: 31}, (_, index) => {
    const comicId = String(index + 1);
    return {
      comicId,
      favoriteUpdate: {state: {latestChapterId: "chapter-" + comicId}}
    };
  });
  const probeAccount = async () => ({
    identity: {scheme: "manwa-username-v1", value: "fixture-account"},
    display: {name: "Fixture account", secondary: "fixture-account"},
    attributes: {accountLevel: 1},
    visibilityScope: "manwa:level:1",
    sessionPatch: null
  });
  const scanFavoriteSnapshotSlice = async (input = {}) => {
    const checkpoint = input.checkpoint;
    const start = checkpoint == null ? 0 : checkpoint.offset;
    const end = checkpoint == null ? 16 : 31;
    return {
      expectedTotal: 31,
      items: allItems.slice(start, end),
      complete: end === 31,
      checkpoint: end === 31 ? null : {offset: end},
      sessionPatch: null
    };
  };
  SourceServerExtensions.register({
    extensionId: "fixture.resumable",
    artifactId: "manwa",
    sourceKey: "manwa",
    fileName: "manwa.js",
    apiVersion: 1,
    capabilities: {scanning: {version: 1, probeAccount, scanFavoriteSnapshotSlice}}
  });
})();`
	if err := os.WriteFile(filepath.Join(catalogDir, "scanner.js"), []byte(scanner), 0o644); err != nil {
		t.Fatal(err)
	}
	revision := initGitCatalog(t, catalogDir)
	cfg := &config.Config{
		DataDir:                     dataDir,
		WorkerCount:                 1,
		TrackingSnapshotMaxRequests: 4,
		TrackingSnapshotMaxItems:    100,
		TrackingSnapshotDeadline:    time.Minute,
		Tracking: trackingcatalog.Config{
			CatalogID:        "yuxuanmian/venera-configs",
			Repository:       catalogDir,
			Revision:         revision,
			CacheDir:         t.TempDir(),
			ObservationLimit: 10000,
			IndexMaxBytes:    1 << 20,
		},
	}
	st, err := store.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	recorder, err := debugrecorder.New(dataDir, false)
	if err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	srv, err := NewServer(cfg, st, recorder)
	if err != nil {
		_ = recorder.Close()
		_ = st.Close()
		t.Fatal(err)
	}
	token := "token-resumable-tracking"
	deviceID := "device-resumable-tracking"
	registered := doReq(t, srv, "POST", "/api/register", token, map[string]any{
		"device_id": deviceID,
		"tz":        "Asia/Shanghai",
		"locale":    "zh-CN",
		"sources": []map[string]any{{
			"source": "manwa",
			"cookie": "sid=fixture",
		}},
	})
	if registered.Code != 200 {
		t.Fatalf("resumable register status=%d body=%s", registered.Code, registered.Body.String())
	}
	interests := make([]map[string]any, 0, 31)
	for index := 1; index <= 31; index++ {
		interests = append(interests, map[string]any{
			"artifact": map[string]any{"sourceKey": "manwa", "fileName": "manwa.js"},
			"comicId":  fmt.Sprintf("%d", index),
		})
	}
	state := doReq(t, srv, "PUT", "/api/tracking/client-state", token, map[string]any{
		"cloudEnabled": true,
		"interests":    interests,
	})
	if state.Code != 200 {
		t.Fatalf("resumable client-state status=%d body=%s", state.Code, state.Body.String())
	}
	firstErr := srv.RunTrackingOnce(context.Background())
	if firstErr == nil || !strings.Contains(firstErr.Error(), "incomplete") {
		t.Fatalf("first resumable tracking run error=%v", firstErr)
	}
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	if err := recorder.Close(); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	stRestarted, err := store.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stRestarted.Close() })
	recorderRestarted, err := debugrecorder.New(dataDir, false)
	if err != nil {
		_ = stRestarted.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recorderRestarted.Close() })
	restarted, err := NewServer(cfg, stRestarted, recorderRestarted)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	if err := restarted.RunTrackingOnce(context.Background()); err != nil {
		t.Fatalf("resumed tracking run: %v", err)
	}
	response := doReq(t, restarted, "GET", "/api/tracking/observations", token, nil)
	if response.Code != 200 {
		t.Fatalf("resumed observations status=%d body=%s", response.Code, response.Body.String())
	}
	var decoded struct {
		Data struct {
			Observations []struct {
				ComicID string `json:"comicId"`
			} `json:"observations"`
		} `json:"data"`
	}
	decodeResp(t, response, &decoded)
	if len(decoded.Data.Observations) != 31 {
		t.Fatalf("resumed observations = %d, want 31", len(decoded.Data.Observations))
	}
}

func TestTrackingClientStateChangeWakesStartedScanner(t *testing.T) {
	dataDir := t.TempDir()
	catalogDir := t.TempDir()
	index := `[ {"key":"manwa","fileName":"manwa.js","version":"1.0.0",
      "cloudTracking":{"scanner":"scanner.js"}} ]`
	if err := os.WriteFile(filepath.Join(catalogDir, "index.json"), []byte(index), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(catalogDir, "manwa.js"), []byte("module.exports = {};"), 0o644); err != nil {
		t.Fatal(err)
	}
	const scanner = `(() => {
  SourceServerExtensions.register({
    extensionId: "fixture.wake",
    artifactId: "manwa",
    sourceKey: "manwa",
    fileName: "manwa.js",
    apiVersion: 1,
    capabilities: { scanning: {
      version: 1,
      probeAccount: async () => ({
        identity: {scheme: "manwa-username-v1", value: "wake-account"},
        display: {name: "Wake account"},
        attributes: {accountLevel: 1},
        visibilityScope: "manwa:level:1",
        sessionPatch: null
      }),
      scanFavoriteSnapshotSlice: async () => ({
        expectedTotal: 1,
        items: [{comicId: "wake-42", favoriteUpdate: {
          state: {latestChapterId: "wake-chapter"}
        }}],
        complete: true,
        checkpoint: null,
        sessionPatch: null
      })
    }}
  });
})();`
	if err := os.WriteFile(filepath.Join(catalogDir, "scanner.js"), []byte(scanner), 0o644); err != nil {
		t.Fatal(err)
	}
	revision := initGitCatalog(t, catalogDir)
	cfg := &config.Config{
		DataDir:          dataDir,
		WorkerCount:      1,
		TrackingInterval: 12 * time.Hour,
		Tracking: trackingcatalog.Config{
			CatalogID:        "yuxuanmian/venera-configs",
			Repository:       catalogDir,
			Revision:         revision,
			CacheDir:         t.TempDir(),
			ObservationLimit: 10000,
			IndexMaxBytes:    1 << 20,
		},
	}
	st, err := store.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	recorder, err := debugrecorder.New(dataDir, false)
	if err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	srv, err := NewServer(cfg, st, recorder)
	if err != nil {
		_ = recorder.Close()
		_ = st.Close()
		t.Fatal(err)
	}
	trackingContext, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		_ = srv.Close()
		cancel()
		_ = recorder.Close()
		_ = st.Close()
	})
	if err := srv.StartTracking(trackingContext); err != nil {
		t.Fatal(err)
	}
	token := "token-wake-tracking"
	deviceID := "device-wake-tracking"
	registered := doReq(t, srv, "POST", "/api/register", token, map[string]any{
		"device_id": deviceID,
		"tz":        "Asia/Shanghai",
		"locale":    "zh-CN",
		"sources": []map[string]any{{
			"source": "manwa",
			"cookie": "sid=wake",
		}},
	})
	if registered.Code != 200 {
		t.Fatalf("wake register status=%d body=%s", registered.Code, registered.Body.String())
	}
	state := doReq(t, srv, "PUT", "/api/tracking/client-state", token, map[string]any{
		"cloudEnabled": true,
		"interests": []map[string]any{{
			"artifact": map[string]any{"sourceKey": "manwa", "fileName": "manwa.js"},
			"comicId":  "wake-42",
		}},
	})
	if state.Code != 200 {
		t.Fatalf("wake client-state status=%d body=%s", state.Code, state.Body.String())
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		response := doReq(t, srv, "GET", "/api/tracking/observations", token, nil)
		if response.Code == 200 {
			var decoded struct {
				Data struct {
					Observations []struct {
						ComicID string `json:"comicId"`
					} `json:"observations"`
				} `json:"data"`
			}
			decodeResp(t, response, &decoded)
			if len(decoded.Data.Observations) == 1 && decoded.Data.Observations[0].ComicID == "wake-42" {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("started scanner did not wake for changed client state")
}

func initGitCatalog(t *testing.T, directory string) string {
	t.Helper()
	for _, args := range [][]string{
		{"init", "-q", directory},
		{"-C", directory, "config", "user.email", "tracking-test@example.invalid"},
		{"-C", directory, "config", "user.name", "tracking-test"},
		{"-C", directory, "add", "."},
		{"-C", directory, "commit", "-q", "-m", "fixture"},
	} {
		if output, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("git %v failed: %v (%s)", args, err, strings.TrimSpace(string(output)))
		}
	}
	output, err := exec.Command("git", "-C", directory, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatalf("resolve fixture revision: %v", err)
	}
	return strings.TrimSpace(string(output))
}
