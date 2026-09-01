package v2config

import (
	"os"
	"strings"
	"testing"
)

func TestLoadFromEnvUsesLocalDotEnv(t *testing.T) {
	t.Chdir(t.TempDir())
	isolateEnvironment(t, "VENERA_V2_ROOT_SECRET", "VENERA_V2_MANIFEST_URL", "VENERA_V2_ADDR", "VENERA_V2_ADMIN_DIST_DIR", "VENERA_V2_DEV_OPEN_ENROLLMENT")

	secret := strings.Repeat("f", RootSecretSize)
	contents := strings.Join([]string{
		"# local V2 settings",
		`VENERA_V2_ROOT_SECRET = "` + secret + `"`,
		`VENERA_V2_MANIFEST_URL = "https://manifest.example/index-v2.json"`,
		"VENERA_V2_ADDR=127.0.0.1:8080 # local listener",
		"VENERA_V2_ADMIN_DIST_DIR='local-admin-dist'",
		"VENERA_V2_DEV_OPEN_ENROLLMENT=true",
	}, "\n")
	if err := os.WriteFile(".env", []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}
	if string(cfg.RootSecret) != secret {
		t.Fatalf("RootSecret = %q, want %q", cfg.RootSecret, secret)
	}
	if cfg.ManifestURL != "https://manifest.example/index-v2.json" {
		t.Fatalf("ManifestURL = %q", cfg.ManifestURL)
	}
	if got := os.Getenv("VENERA_V2_ADDR"); got != "127.0.0.1:8080" {
		t.Fatalf("VENERA_V2_ADDR = %q", got)
	}
	if cfg.AdminDistDir != "local-admin-dist" {
		t.Fatalf("AdminDistDir = %q", cfg.AdminDistDir)
	}
	if !cfg.DevOpenEnrollment || cfg.Production {
		t.Fatalf("development open enrollment = %v, production = %v", cfg.DevOpenEnrollment, cfg.Production)
	}
}

func TestLoadFromEnvPreservesProcessEnvironmentPrecedence(t *testing.T) {
	t.Chdir(t.TempDir())
	isolateEnvironment(t, "VENERA_V2_ROOT_SECRET", "VENERA_V2_MANIFEST_URL", "VENERA_V2_ADDR", "VENERA_V2_ADMIN_DIST_DIR")

	fileSecret := strings.Repeat("f", RootSecretSize)
	processSecret := strings.Repeat("p", RootSecretSize)
	contents := strings.Join([]string{
		"VENERA_V2_ROOT_SECRET=" + fileSecret,
		"VENERA_V2_MANIFEST_URL=https://file.example/index-v2.json",
		"VENERA_V2_ADDR=127.0.0.1:8080",
		"VENERA_V2_ADMIN_DIST_DIR=file-admin-dist",
	}, "\n")
	if err := os.WriteFile(".env", []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]string{
		"VENERA_V2_ROOT_SECRET":    processSecret,
		"VENERA_V2_MANIFEST_URL":   "https://process.example/index-v2.json",
		"VENERA_V2_ADDR":           "127.0.0.1:9090",
		"VENERA_V2_ADMIN_DIST_DIR": "process-admin-dist",
	} {
		if err := os.Setenv(key, value); err != nil {
			t.Fatal(err)
		}
	}

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}
	if string(cfg.RootSecret) != processSecret {
		t.Fatalf("RootSecret = %q, want process environment value", cfg.RootSecret)
	}
	if cfg.ManifestURL != "https://process.example/index-v2.json" {
		t.Fatalf("ManifestURL = %q, want process environment value", cfg.ManifestURL)
	}
	if got := os.Getenv("VENERA_V2_ADDR"); got != "127.0.0.1:9090" {
		t.Fatalf("VENERA_V2_ADDR = %q, want process environment value", got)
	}
	if cfg.AdminDistDir != "process-admin-dist" {
		t.Fatalf("AdminDistDir = %q, want process environment value", cfg.AdminDistDir)
	}
}

func TestLoadFromEnvWithoutDotEnvUsesProcessEnvironment(t *testing.T) {
	t.Chdir(t.TempDir())
	isolateEnvironment(t, "VENERA_V2_ROOT_SECRET")
	secret := strings.Repeat("p", RootSecretSize)
	if err := os.Setenv("VENERA_V2_ROOT_SECRET", secret); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}
	if string(cfg.RootSecret) != secret {
		t.Fatalf("RootSecret = %q, want process environment value", cfg.RootSecret)
	}
}

func TestLoadFromEnvRejectsMalformedDotEnv(t *testing.T) {
	t.Chdir(t.TempDir())
	isolateEnvironment(t, "VENERA_V2_ROOT_SECRET")
	if err := os.WriteFile(".env", []byte("VENERA_V2_ROOT_SECRET\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := LoadFromEnv(); err == nil || !strings.Contains(err.Error(), ".env line 1") {
		t.Fatalf("LoadFromEnv() error = %v, want .env line number", err)
	}
}

func isolateEnvironment(t *testing.T, keys ...string) {
	t.Helper()
	type environmentEntry struct {
		key     string
		value   string
		present bool
	}
	entries := make([]environmentEntry, 0, len(keys))
	for _, key := range keys {
		value, present := os.LookupEnv(key)
		entries = append(entries, environmentEntry{key: key, value: value, present: present})
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for _, entry := range entries {
			if entry.present {
				_ = os.Setenv(entry.key, entry.value)
			} else {
				_ = os.Unsetenv(entry.key)
			}
		}
	})
}
