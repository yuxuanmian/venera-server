package v2config

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func validConfig() Config {
	cfg := Default()
	cfg.RootSecret = bytes.Repeat([]byte{0x7a}, RootSecretSize)
	return cfg
}

func TestConfigRejectsMissingSecretAndProductionHTTP(t *testing.T) {
	cfg := Default()
	if err := cfg.Validate(); err == nil {
		t.Fatal("missing root secret unexpectedly accepted")
	}
	cfg = validConfig()
	cfg.ManifestURL = "http://127.0.0.1:8080/index-v2.json"
	if err := cfg.Validate(); err == nil {
		t.Fatal("production HTTP manifest unexpectedly accepted")
	}
}

func TestConfigDebugHTTPGateAndIPv6Localhost(t *testing.T) {
	cfg := validConfig()
	cfg.Production = false
	cfg.HTTPDebugAllowed = true
	cfg.ManifestURL = "http://127.0.0.1:8080/index-v2.json"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("localhost debug manifest rejected: %v", err)
	}
	cfg.ManifestURL = "http://[::1]:8080/index-v2.json"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("IPv6 localhost debug manifest rejected: %v", err)
	}
	cfg.ManifestURL = "http://10.0.0.2:8080/index-v2.json"
	if err := cfg.Validate(); err == nil {
		t.Fatal("non-local debug manifest unexpectedly accepted")
	}
	cfg.DebugHTTPOrigins = []string{"http://10.0.0.2:8080/index-v2.json"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("explicit debug origin rejected: %v", err)
	}
	if !IsAllowedHTTPDebugEndpoint(cfg.ManifestURL, cfg.DebugHTTPOrigins) {
		t.Fatal("explicit debug origin not allowed")
	}
	if IsAllowedHTTPDebugEndpoint("https://10.0.0.2:8080/index-v2.json", cfg.DebugHTTPOrigins) {
		t.Fatal("HTTPS endpoint incorrectly classified as debug HTTP")
	}
}

func TestConfigAllowsOpenEnrollmentOnlyOutsideProduction(t *testing.T) {
	cfg := validConfig()
	cfg.DevOpenEnrollment = true
	if err := cfg.Validate(); err == nil {
		t.Fatal("production open enrollment unexpectedly accepted")
	}
	cfg.Production = false
	if err := cfg.Validate(); err != nil {
		t.Fatalf("development open enrollment rejected: %v", err)
	}
}

func TestParseRootSecretAcceptsOnlyExactly32Bytes(t *testing.T) {
	if _, err := ParseRootSecret(strings.Repeat("x", 31)); err == nil {
		t.Fatal("31-byte raw secret unexpectedly accepted")
	}
	secret, err := ParseRootSecret(strings.Repeat("x", 32))
	if err != nil || len(secret) != RootSecretSize {
		t.Fatalf("32-byte raw secret = %d, %v", len(secret), err)
	}
	if _, err := ParseRootSecret("not-a-secret"); err == nil {
		t.Fatal("invalid encoded secret unexpectedly accepted")
	}
}

func TestAdminDistDirDefaultsLoadsAndValidates(t *testing.T) {
	if got, want := Default().AdminDistDir, filepath.Join("web", "dist"); got != want {
		t.Fatalf("default admin dist dir = %q, want %q", got, want)
	}

	t.Setenv("VENERA_V2_ROOT_SECRET", strings.Repeat("x", RootSecretSize))
	t.Setenv("VENERA_V2_ADMIN_DIST_DIR", filepath.Join("testdata", "admin-dist"))
	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("load config with admin dist dir: %v", err)
	}
	if got, want := cfg.AdminDistDir, filepath.Join("testdata", "admin-dist"); got != want {
		t.Fatalf("loaded admin dist dir = %q, want %q", got, want)
	}

	cfg.AdminDistDir = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("empty admin dist dir unexpectedly accepted")
	}
}
