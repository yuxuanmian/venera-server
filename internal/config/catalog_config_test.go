package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadCatalogFileAndEnvironmentPrecedence(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "catalog.json")
	if err := os.WriteFile(configPath, []byte(`{"addr":"127.0.0.1:9000","data_dir":"stored","catalog_url":"https://raw.githubusercontent.com/owner/repo/ref/index.json"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("VENERA_CONFIG_FILE", configPath)
	t.Setenv("VENERA_ADDR", "127.0.0.1:9100")
	t.Setenv("VENERA_DATA_DIR", "env-data")
	t.Setenv("VENERA_CATALOG_URL", "https://raw.githubusercontent.com/owner/repo/other/index.json")
	cfg, err := LoadCatalog()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != "127.0.0.1:9100" || cfg.CatalogURL != "https://raw.githubusercontent.com/owner/repo/other/index.json" {
		t.Fatalf("environment did not win: %#v", cfg)
	}
	if cfg.DataDir != filepath.Join(dir, "env-data") {
		t.Fatalf("data dir = %q", cfg.DataDir)
	}
}

func TestLoadCatalogAllowsDefaultFileMissingWithEnvironmentURL(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("VENERA_CONFIG_FILE", "")
	t.Setenv("VENERA_ADDR", "")
	t.Setenv("VENERA_DATA_DIR", "")
	t.Setenv("VENERA_CATALOG_URL", "https://raw.githubusercontent.com/owner/repo/ref/index.json")
	if _, err := LoadCatalog(); err != nil {
		t.Fatal(err)
	}
}

func TestLoadCatalogRejectsExplicitMissingUnknownAndInvalidFiles(t *testing.T) {
	t.Setenv("VENERA_CONFIG_FILE", filepath.Join(t.TempDir(), "missing.json"))
	t.Setenv("VENERA_CATALOG_URL", "https://raw.githubusercontent.com/owner/repo/ref/index.json")
	if _, err := LoadCatalog(); err == nil {
		t.Fatal("explicit missing file should fail")
	}

	path := filepath.Join(t.TempDir(), "unknown.json")
	if err := os.WriteFile(path, []byte(`{"catalog_url":"https://raw.githubusercontent.com/owner/repo/ref/index.json","old_tracking":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("VENERA_CONFIG_FILE", path)
	if _, err := LoadCatalog(); err == nil {
		t.Fatal("unknown fields should fail")
	}
}
