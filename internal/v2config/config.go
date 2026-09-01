package v2config

import (
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const RootSecretSize = 32

type Config struct {
	DBPath                 string
	ManifestURL            string
	ManifestCacheDir       string
	AdminDistDir           string
	RootSecret             []byte
	WorkerCount            int
	WorkerOperationTimeout time.Duration
	HTTPBodyLimit          int64
	HTTPDebugAllowed       bool
	DebugHTTPOrigins       []string
	DevOpenEnrollment      bool
	Production             bool
	MinAppVersion          string
	MinServerVersion       string
}

func Default() Config {
	return Config{
		DBPath:                 filepath.Join("data", "v2", "server-v2.db"),
		ManifestURL:            "https://raw.githubusercontent.com/yuxuanmian/venera-configs/yxm/index-v2.json",
		ManifestCacheDir:       filepath.Join("data", "v2", "manifest"),
		AdminDistDir:           filepath.Join("web", "dist"),
		WorkerCount:            1,
		WorkerOperationTimeout: 30 * time.Second,
		HTTPBodyLimit:          4 << 20,
		Production:             true,
		MinAppVersion:          "1.6.0",
		MinServerVersion:       "2.0.0",
	}
}

func LoadFromEnv() (Config, error) {
	if err := loadLocalDotEnv(); err != nil {
		return Config{}, fmt.Errorf("load local .env: %w", err)
	}
	cfg := Default()
	if value := os.Getenv("VENERA_V2_DB_PATH"); value != "" {
		cfg.DBPath = value
	}
	if value := os.Getenv("VENERA_V2_MANIFEST_URL"); value != "" {
		cfg.ManifestURL = value
	}
	if value := os.Getenv("VENERA_V2_MANIFEST_CACHE"); value != "" {
		cfg.ManifestCacheDir = value
	}
	if value := os.Getenv("VENERA_V2_ADMIN_DIST_DIR"); value != "" {
		cfg.AdminDistDir = value
	}
	if value := os.Getenv("VENERA_V2_ROOT_SECRET"); value != "" {
		secret, err := ParseRootSecret(value)
		if err != nil {
			return Config{}, err
		}
		cfg.RootSecret = secret
	}
	if value := os.Getenv("VENERA_V2_DEBUG_HTTP"); value == "true" {
		cfg.HTTPDebugAllowed = true
		cfg.Production = false
	}
	if value := os.Getenv("VENERA_V2_DEV_OPEN_ENROLLMENT"); value == "true" {
		cfg.DevOpenEnrollment = true
		cfg.Production = false
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func ParseRootSecret(value string) ([]byte, error) {
	if len(value) == RootSecretSize {
		secret := []byte(value)
		return append([]byte(nil), secret...), nil
	}
	for _, encoding := range []*base64.Encoding{
		base64.RawURLEncoding,
		base64.URLEncoding,
		base64.RawStdEncoding,
		base64.StdEncoding,
	} {
		secret, err := encoding.DecodeString(value)
		if err == nil && len(secret) == RootSecretSize {
			return secret, nil
		}
	}
	return nil, errors.New("VENERA_V2_ROOT_SECRET must contain exactly 32 bytes")
}

func (cfg Config) Validate() error {
	if len(cfg.RootSecret) != RootSecretSize {
		return errors.New("v2 root secret is missing or has invalid length")
	}
	if cfg.DBPath == "" || cfg.ManifestCacheDir == "" || cfg.AdminDistDir == "" {
		return errors.New("v2 database, manifest cache, and admin dist paths are required")
	}
	if cfg.WorkerCount < 1 || cfg.WorkerOperationTimeout <= 0 {
		return errors.New("v2 worker budget is invalid")
	}
	if cfg.HTTPBodyLimit < 1 {
		return errors.New("v2 HTTP body limit is invalid")
	}
	if cfg.DevOpenEnrollment && cfg.Production {
		return errors.New("open client enrollment is allowed only in development mode")
	}
	if err := validateEndpoint(cfg.ManifestURL, cfg.HTTPDebugAllowed, cfg.DebugHTTPOrigins, cfg.Production); err != nil {
		return fmt.Errorf("manifest URL: %w", err)
	}
	return nil
}

func validateEndpoint(raw string, debugAllowed bool, debugOrigins []string, production bool) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return errors.New("endpoint must be an absolute URL")
	}
	if strings.EqualFold(parsed.Scheme, "https") {
		return nil
	}
	if production || !strings.EqualFold(parsed.Scheme, "http") || !debugAllowed {
		return errors.New("HTTP endpoint is disabled outside an explicit debug configuration")
	}
	for _, allowed := range debugOrigins {
		if subtle.ConstantTimeCompare([]byte(raw), []byte(allowed)) == 1 {
			return nil
		}
	}
	if isLocalDebugHost(parsed.Hostname()) {
		return nil
	}
	return errors.New("HTTP endpoint is not in the debug allowlist")
}

func IsAllowedHTTPDebugEndpoint(raw string, origins []string) bool {
	parsed, err := url.Parse(raw)
	if err != nil || !strings.EqualFold(parsed.Scheme, "http") {
		return false
	}
	if isLocalDebugHost(parsed.Hostname()) {
		return true
	}
	for _, allowed := range origins {
		if subtle.ConstantTimeCompare([]byte(raw), []byte(allowed)) == 1 {
			return true
		}
	}
	return false
}

func isLocalDebugHost(host string) bool {
	host = strings.ToLower(host)
	return host == "localhost" || host == "127.0.0.1" || host == "::1" || host == "[::1]"
}
