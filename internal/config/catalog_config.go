package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"venera-server/internal/catalog"
)

const DefaultCatalogConfigFile = "config.json"

// CatalogServerConfig is the deliberately small static configuration used by
// the catalog authority. Runtime state is kept by catalog.Store, never in the
// configuration file.
type CatalogServerConfig struct {
	ConfigFile string
	Addr       string
	DataDir    string
	CatalogURL string
}

type catalogFileConfig struct {
	Addr       string `json:"addr"`
	DataDir    string `json:"data_dir"`
	CatalogURL string `json:"catalog_url"`
}

// LoadCatalog applies the contract's precedence: non-empty environment,
// file, then the documented address/data defaults. The catalog URL has no
// implicit default and must be supplied by the file or environment.
func LoadCatalog() (*CatalogServerConfig, error) {
	fileName := strings.TrimSpace(os.Getenv("VENERA_CONFIG_FILE"))
	if fileName == "" {
		fileName = DefaultCatalogConfigFile
	}
	configFile, err := filepath.Abs(fileName)
	if err != nil {
		return nil, fmt.Errorf("resolve config file: %w", err)
	}

	fileValues, err := loadCatalogFile(configFile, os.Getenv("VENERA_CONFIG_FILE") != "")
	if err != nil {
		return nil, err
	}

	addr := firstNonEmpty(os.Getenv("VENERA_ADDR"), fileValues.Addr, "127.0.0.1:8080")
	dataDir := firstNonEmpty(os.Getenv("VENERA_DATA_DIR"), fileValues.DataDir, "data")
	catalogURL := firstNonEmpty(os.Getenv("VENERA_CATALOG_URL"), fileValues.CatalogURL, "")
	if strings.TrimSpace(addr) == "" {
		return nil, errors.New("addr must not be empty")
	}
	if strings.TrimSpace(dataDir) == "" {
		return nil, errors.New("data_dir must not be empty")
	}
	if strings.TrimSpace(catalogURL) == "" {
		return nil, errors.New("catalog_url is required")
	}
	if _, err := catalog.ParseConfiguredURL(catalogURL); err != nil {
		return nil, fmt.Errorf("catalog_url: %w", err)
	}

	// Both file and environment relative paths are anchored to the config
	// file directory. This keeps an env override deterministic for services.
	if !filepath.IsAbs(dataDir) {
		dataDir = filepath.Join(filepath.Dir(configFile), dataDir)
	}
	dataDir, err = filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("resolve data_dir: %w", err)
	}

	return &CatalogServerConfig{
		ConfigFile: configFile,
		Addr:       strings.TrimSpace(addr),
		DataDir:    dataDir,
		CatalogURL: strings.TrimSpace(catalogURL),
	}, nil
}

func loadCatalogFile(path string, explicitlySelected bool) (catalogFileConfig, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		// A default config may be absent when the caller supplies all required
		// values through the environment. An explicitly selected file never
		// silently disappears.
		if explicitlySelected || os.Getenv("VENERA_CATALOG_URL") == "" {
			return catalogFileConfig{}, fmt.Errorf("open config file %q: %w", path, err)
		}
		return catalogFileConfig{}, nil
	}
	if err != nil {
		return catalogFileConfig{}, fmt.Errorf("open config file %q: %w", path, err)
	}
	defer file.Close()

	var values catalogFileConfig
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&values); err != nil {
		return catalogFileConfig{}, fmt.Errorf("decode config file %q: %w", path, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return catalogFileConfig{}, fmt.Errorf("decode config file %q: trailing JSON value", path)
		}
		return catalogFileConfig{}, fmt.Errorf("decode config file %q: %w", path, err)
	}
	return values, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
