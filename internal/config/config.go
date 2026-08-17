package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type Config struct {
	Addr            string
	DataDir         string
	AdminToken      string
	CookieKey       string
	DebugRecord     bool
	InitJSPath      string
	ComicSourceDir  string
	AdminDistDir    string
	WorkerCount     int
	RequestInterval time.Duration
	ChunkCooldown   time.Duration
}

func Load() *Config {
	addr := getenv("VENERA_ADDR", ":8080")
	dataDir := getenv("VENERA_DATA_DIR", "data")
	adminToken := getenv("VENERA_ADMIN_TOKEN", "")
	cookieKey := getenv("VENERA_COOKIE_KEY", "")
	debugRecord := getenv("VENERA_DEBUG_RECORD", "false") == "true"
	initJSPath := getenv("VENERA_INIT_JS", "../venera/assets/init.js")
	comicSourceDir := getenv("VENERA_COMIC_SOURCE_DIR", "")
	adminDistDir := getenv("VENERA_ADMIN_DIST_DIR", "web/dist")
	workerCount := getenvInt("VENERA_WORKER_COUNT", 5)
	requestInterval := getenvDuration("VENERA_REQUEST_INTERVAL", 2*time.Second)
	chunkCooldown := getenvDuration("VENERA_CHUNK_COOLDOWN", 10*time.Second)

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
		Addr:            addr,
		DataDir:         dataDir,
		AdminToken:      adminToken,
		CookieKey:       cookieKey,
		DebugRecord:     debugRecord,
		InitJSPath:      initJSPath,
		ComicSourceDir:  comicSourceDir,
		AdminDistDir:    adminDistDir,
		WorkerCount:     workerCount,
		RequestInterval: requestInterval,
		ChunkCooldown:   chunkCooldown,
	}
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
