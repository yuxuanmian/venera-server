package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadDefaultsDebugOpenAuthOff(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("VENERA_CONFIG_FILE", "")
	t.Setenv("VENERA_DEBUG_OPEN_AUTH", "")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DebugOpenAuth {
		t.Fatal("debug open auth must default to disabled")
	}
}

func TestLoadReadsDebugOpenAuthFromOptionalJSONFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "server.json")
	if err := os.WriteFile(path, []byte(`{
  "debug_open_auth": true,
  "debug_user_id": "debug-user",
  "debug_device_id": "debug-device",
  "debug_user_tz": "Asia/Shanghai",
  "debug_locale": "zh-CN",
  "tracking_catalog_id": "yuxuanmian/venera-configs",
  "tracking_catalog_repository": "../venera-configs",
  "tracking_revision": "0123456789abcdef0123456789abcdef01234567",
  "tracking_cache_dir": "tracking-cache",
  "tracking_interval": "30m",
  "tracking_snapshot_max_requests": 8,
  "tracking_snapshot_max_items": 200,
  "tracking_snapshot_deadline": "45s",
  "tracking_max_attempts": 5,
  "tracking_observation_limit": 5000,
  "tracking_index_max_bytes": 1048576
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("VENERA_CONFIG_FILE", path)
	t.Setenv("VENERA_DEBUG_OPEN_AUTH", "")
	t.Setenv("VENERA_DEBUG_USER_ID", "")
	t.Setenv("VENERA_DEBUG_DEVICE_ID", "")
	t.Setenv("VENERA_DEBUG_USER_TZ", "")
	t.Setenv("VENERA_DEBUG_LOCALE", "")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.DebugOpenAuth || cfg.DebugUserID != "debug-user" || cfg.DebugDeviceID != "debug-device" {
		t.Fatalf("debug config = %#v", cfg)
	}
	if cfg.DebugUserTZ != "Asia/Shanghai" || cfg.DebugLocale != "zh-CN" {
		t.Fatalf("debug locale config = %#v", cfg)
	}
	if cfg.Tracking.CatalogID != "yuxuanmian/venera-configs" ||
		cfg.Tracking.Revision != "0123456789abcdef0123456789abcdef01234567" {
		t.Fatalf("tracking identity config = %#v", cfg.Tracking)
	}
	if cfg.Tracking.Repository != filepath.Join(filepath.Dir(path), "..", "venera-configs") ||
		cfg.Tracking.CacheDir != filepath.Join(filepath.Dir(path), "tracking-cache") {
		t.Fatalf("tracking paths = %#v", cfg.Tracking)
	}
	if cfg.TrackingInterval != 30*time.Minute ||
		cfg.TrackingSnapshotMaxRequests != 8 ||
		cfg.TrackingSnapshotMaxItems != 200 ||
		cfg.TrackingSnapshotDeadline != 45*time.Second ||
		cfg.TrackingMaxAttempts != 5 {
		t.Fatalf("tracking runtime config = %#v", cfg)
	}
	if cfg.Tracking.ObservationLimit != 5000 || cfg.Tracking.IndexMaxBytes != 1048576 {
		t.Fatalf("tracking limits = %#v", cfg.Tracking)
	}
}

func TestLoadEnvironmentOverridesDebugJSONFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "server.json")
	if err := os.WriteFile(path, []byte(`{
  "debug_open_auth": true,
  "debug_user_id": "file-user"
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("VENERA_CONFIG_FILE", path)
	t.Setenv("VENERA_DEBUG_OPEN_AUTH", "false")
	t.Setenv("VENERA_DEBUG_USER_ID", "env-user")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DebugOpenAuth {
		t.Fatal("environment should disable debug open auth")
	}
	if cfg.DebugUserID != "env-user" {
		t.Fatalf("debug user id = %q", cfg.DebugUserID)
	}
}

func TestLoadRejectsMissingExplicitConfigFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.json")
	t.Setenv("VENERA_CONFIG_FILE", path)

	if _, err := Load(); err == nil {
		t.Fatal("missing explicit config file should fail")
	}
}
