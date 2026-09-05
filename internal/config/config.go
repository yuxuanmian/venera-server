package config

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"venera-server/internal/tracking/catalog"
)

const (
	DefaultConfigFile    = "config.json"
	DefaultDebugUserID   = "__venera_debug_user__"
	DefaultDebugDeviceID = "__venera_debug_device__"
	DefaultDebugUserTZ   = "UTC"
	DefaultDebugLocale   = "en-US"
)

type Config struct {
	ConfigFile                  string
	Addr                        string
	DataDir                     string
	AdminToken                  string
	CookieKey                   string
	DebugRecord                 bool
	DebugOpenAuth               bool
	DebugUserID                 string
	DebugDeviceID               string
	DebugUserTZ                 string
	DebugLocale                 string
	InitJSPath                  string
	ComicSourceDir              string
	AdminDistDir                string
	WorkerCount                 int
	RequestInterval             time.Duration
	ChunkCooldown               time.Duration
	TrackingInterval            time.Duration
	TrackingSnapshotMaxRequests int
	TrackingSnapshotMaxItems    int
	TrackingSnapshotDeadline    time.Duration
	TrackingMaxAttempts         int
	Tracking                    catalog.Config
}

type fileConfig struct {
	DebugOpenAuth               *bool  `json:"debug_open_auth"`
	DebugUserID                 string `json:"debug_user_id"`
	DebugDeviceID               string `json:"debug_device_id"`
	DebugUserTZ                 string `json:"debug_user_tz"`
	DebugLocale                 string `json:"debug_locale"`
	TrackingCatalogID           string `json:"tracking_catalog_id"`
	TrackingCatalogRepository   string `json:"tracking_catalog_repository"`
	TrackingRevision            string `json:"tracking_revision"`
	TrackingCacheDir            string `json:"tracking_cache_dir"`
	TrackingInterval            string `json:"tracking_interval"`
	TrackingSnapshotMaxRequests *int   `json:"tracking_snapshot_max_requests"`
	TrackingSnapshotMaxItems    *int   `json:"tracking_snapshot_max_items"`
	TrackingSnapshotDeadline    string `json:"tracking_snapshot_deadline"`
	TrackingMaxAttempts         *int   `json:"tracking_max_attempts"`
	TrackingObservationLimit    *int   `json:"tracking_observation_limit"`
	TrackingIndexMaxBytes       *int64 `json:"tracking_index_max_bytes"`
}

// Load reads the optional local JSON config first, then applies environment
// variables. Environment variables intentionally win so existing deployments
// can override a checked-in/shared config without editing it.
func Load() (*Config, error) {
	configPath := getenv("VENERA_CONFIG_FILE", DefaultConfigFile)
	resolvedConfigPath, err := filepath.Abs(configPath)
	if err != nil {
		return nil, fmt.Errorf("resolve config file: %w", err)
	}
	fileValues, err := loadFileConfig(resolvedConfigPath, configPath != DefaultConfigFile || os.Getenv("VENERA_CONFIG_FILE") != "")
	if err != nil {
		return nil, err
	}

	addr := getenv("VENERA_ADDR", ":8080")
	dataDir := getenv("VENERA_DATA_DIR", "data")
	adminToken := getenv("VENERA_ADMIN_TOKEN", "")
	cookieKey := getenv("VENERA_COOKIE_KEY", "")
	debugRecord := getenv("VENERA_DEBUG_RECORD", "false") == "true"
	debugOpenAuth := valueOrDefault(fileValues.DebugOpenAuth, false)
	if raw, ok := os.LookupEnv("VENERA_DEBUG_OPEN_AUTH"); ok && strings.TrimSpace(raw) != "" {
		debugOpenAuth, err = strconv.ParseBool(strings.TrimSpace(raw))
		if err != nil {
			return nil, fmt.Errorf("VENERA_DEBUG_OPEN_AUTH: %w", err)
		}
	}
	debugUserID := getenv("VENERA_DEBUG_USER_ID", valueOrDefaultString(fileValues.DebugUserID, DefaultDebugUserID))
	debugDeviceID := getenv("VENERA_DEBUG_DEVICE_ID", valueOrDefaultString(fileValues.DebugDeviceID, DefaultDebugDeviceID))
	debugUserTZ := getenv("VENERA_DEBUG_USER_TZ", valueOrDefaultString(fileValues.DebugUserTZ, DefaultDebugUserTZ))
	debugLocale := getenv("VENERA_DEBUG_LOCALE", valueOrDefaultString(fileValues.DebugLocale, DefaultDebugLocale))
	if debugUserID == "" || debugDeviceID == "" || debugUserTZ == "" || debugLocale == "" {
		return nil, fmt.Errorf("debug identity fields must not be empty")
	}
	initJSPath := getenv("VENERA_INIT_JS", "../venera/assets/init.js")
	comicSourceDir := getenv("VENERA_COMIC_SOURCE_DIR", "")
	adminDistDir := getenv("VENERA_ADMIN_DIST_DIR", "web/dist")
	workerCount := getenvInt("VENERA_WORKER_COUNT", 5)
	requestInterval := getenvDuration("VENERA_REQUEST_INTERVAL", 2*time.Second)
	chunkCooldown := getenvDuration("VENERA_CHUNK_COOLDOWN", 10*time.Second)
	trackingIntervalDefault := 12 * time.Hour
	if os.Getenv("VENERA_TRACKING_INTERVAL") == "" {
		trackingIntervalDefault, err = fileDuration(
			fileValues.TrackingInterval,
			"tracking_interval",
			trackingIntervalDefault,
		)
		if err != nil {
			return nil, err
		}
	}
	trackingSnapshotDeadlineDefault := 2 * time.Minute
	if os.Getenv("VENERA_TRACKING_SNAPSHOT_DEADLINE") == "" {
		trackingSnapshotDeadlineDefault, err = fileDuration(
			fileValues.TrackingSnapshotDeadline,
			"tracking_snapshot_deadline",
			trackingSnapshotDeadlineDefault,
		)
		if err != nil {
			return nil, err
		}
	}
	trackingInterval := getenvDuration("VENERA_TRACKING_INTERVAL", trackingIntervalDefault)
	trackingSnapshotMaxRequests := getenvInt(
		"VENERA_TRACKING_SNAPSHOT_MAX_REQUESTS",
		fileInt(fileValues.TrackingSnapshotMaxRequests, 64),
	)
	trackingSnapshotMaxItems := getenvInt(
		"VENERA_TRACKING_SNAPSHOT_MAX_ITEMS",
		fileInt(fileValues.TrackingSnapshotMaxItems, 0),
	)
	trackingSnapshotDeadline := getenvDuration(
		"VENERA_TRACKING_SNAPSHOT_DEADLINE",
		trackingSnapshotDeadlineDefault,
	)
	trackingMaxAttempts := getenvInt(
		"VENERA_TRACKING_MAX_ATTEMPTS",
		fileInt(fileValues.TrackingMaxAttempts, 3),
	)
	trackingCacheDir := configuredPath(
		"VENERA_TRACKING_CACHE_DIR",
		fileValues.TrackingCacheDir,
		resolvedConfigPath,
	)
	tracking := catalog.Config{
		CatalogID:        getenv("VENERA_TRACKING_CATALOG_ID", fileValues.TrackingCatalogID),
		Repository:       configuredPath("VENERA_TRACKING_CATALOG_REPOSITORY", fileValues.TrackingCatalogRepository, resolvedConfigPath),
		Revision:         getenv("VENERA_TRACKING_REVISION", fileValues.TrackingRevision),
		CacheDir:         trackingCacheDir,
		ObservationLimit: getenvInt("VENERA_TRACKING_OBSERVATION_LIMIT", fileInt(fileValues.TrackingObservationLimit, catalog.DefaultObservationLimit)),
		IndexMaxBytes:    getenvInt64("VENERA_TRACKING_INDEX_MAX_BYTES", fileInt64(fileValues.TrackingIndexMaxBytes, catalog.DefaultIndexMaxBytes)),
	}

	if !filepath.IsAbs(dataDir) {
		wd, err := os.Getwd()
		if err == nil {
			dataDir = filepath.Join(wd, dataDir)
		}
	}
	if !filepath.IsAbs(initJSPath) {
		wd, err := os.Getwd()
		if err == nil {
			initJSPath = filepath.Join(wd, initJSPath)
		}
	}

	return &Config{
		ConfigFile:                  resolvedConfigPath,
		Addr:                        addr,
		DataDir:                     dataDir,
		AdminToken:                  adminToken,
		CookieKey:                   cookieKey,
		DebugRecord:                 debugRecord,
		DebugOpenAuth:               debugOpenAuth,
		DebugUserID:                 debugUserID,
		DebugDeviceID:               debugDeviceID,
		DebugUserTZ:                 debugUserTZ,
		DebugLocale:                 debugLocale,
		InitJSPath:                  initJSPath,
		ComicSourceDir:              comicSourceDir,
		AdminDistDir:                adminDistDir,
		WorkerCount:                 workerCount,
		RequestInterval:             requestInterval,
		ChunkCooldown:               chunkCooldown,
		TrackingInterval:            trackingInterval,
		TrackingSnapshotMaxRequests: trackingSnapshotMaxRequests,
		TrackingSnapshotMaxItems:    trackingSnapshotMaxItems,
		TrackingSnapshotDeadline:    trackingSnapshotDeadline,
		TrackingMaxAttempts:         trackingMaxAttempts,
		Tracking:                    tracking,
	}, nil
}

func loadFileConfig(path string, required bool) (fileConfig, error) {
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		if required {
			return fileConfig{}, fmt.Errorf("open config file %q: %w", path, err)
		}
		return fileConfig{}, nil
	}
	if err != nil {
		return fileConfig{}, fmt.Errorf("open config file %q: %w", path, err)
	}
	defer file.Close()

	var values fileConfig
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&values); err != nil {
		return fileConfig{}, fmt.Errorf("decode config file %q: %w", path, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fileConfig{}, fmt.Errorf("decode config file %q: trailing JSON value", path)
		}
		return fileConfig{}, fmt.Errorf("decode config file %q: %w", path, err)
	}
	return values, nil
}

func valueOrDefault(value *bool, def bool) bool {
	if value != nil {
		return *value
	}
	return def
}

func valueOrDefaultString(value, def string) string {
	if value != "" {
		return value
	}
	return def
}

func fileInt(value *int, def int) int {
	if value != nil {
		return *value
	}
	return def
}

func fileInt64(value *int64, def int64) int64 {
	if value != nil {
		return *value
	}
	return def
}

func fileDuration(value, field string, def time.Duration) (time.Duration, error) {
	if value == "" {
		return def, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s in config file: %w", field, err)
	}
	return duration, nil
}

// configuredPath resolves file-based relative paths next to the config file,
// while preserving the existing working-directory behavior for environment
// variable overrides.
func configuredPath(envKey, fileValue, configPath string) string {
	value := fileValue
	base := filepath.Dir(configPath)
	if envValue := os.Getenv(envKey); envValue != "" {
		value = envValue
		if wd, err := os.Getwd(); err == nil {
			base = wd
		}
	}
	if value != "" && !filepath.IsAbs(value) {
		value = filepath.Join(base, value)
	}
	return value
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n := 0
	if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
		return n
	}
	return def
}

func getenvDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	return def
}

func getenvInt64(key string, def int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var value int64
	if _, err := fmt.Sscanf(v, "%d", &value); err == nil {
		return value
	}
	return def
}
