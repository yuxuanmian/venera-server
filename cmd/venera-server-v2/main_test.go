package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"venera-server/internal/v2config"
	"venera-server/internal/v2crypto"
)

func TestInitializeRuntimeHonorsStartupGatesAndGracefulShutdown(t *testing.T) {
	root := filepath.Join("..", "..", "..", "venera-configs")
	validCache := copyManifestArtifacts(t, root)
	rootSecret := make([]byte, v2crypto.RootSecretSize)
	for index := range rootSecret {
		rootSecret[index] = 0x61
	}

	badConfig := v2config.Default()
	badConfig.RootSecret = []byte("bad")
	badConfig.DBPath = filepath.Join(t.TempDir(), "bad", "server.db")
	badConfig.ManifestCacheDir = validCache
	if _, err := initializeRuntime(context.Background(), badConfig); err == nil || !strings.Contains(err.Error(), "config") {
		t.Fatalf("invalid config startup error = %v", err)
	}

	missingLKG := v2config.Default()
	missingLKG.RootSecret = rootSecret
	missingLKG.DBPath = filepath.Join(t.TempDir(), "missing", "server.db")
	missingLKG.ManifestCacheDir = t.TempDir()
	if _, err := initializeRuntime(context.Background(), missingLKG); err == nil || !strings.Contains(err.Error(), "manifest LKG") {
		t.Fatalf("missing LKG startup error = %v", err)
	}

	valid := v2config.Default()
	valid.RootSecret = rootSecret
	valid.DBPath = filepath.Join(t.TempDir(), "valid", "server.db")
	valid.ManifestCacheDir = validCache
	t.Setenv("VENERA_V2_ADDR", "127.0.0.1:0")
	runtime, err := initializeRuntime(context.Background(), valid)
	if err != nil {
		t.Fatalf("initialize valid runtime: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- runServer(ctx, runtime) }()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("graceful server shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server did not shut down")
	}
}

func copyManifestArtifacts(t *testing.T, sourceRoot string) string {
	t.Helper()
	cache := t.TempDir()
	manifest, err := os.ReadFile(filepath.Join(sourceRoot, "index-v2.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cache, "index-v2.json"), manifest, 0o600); err != nil {
		t.Fatal(err)
	}
	paths := []string{"manwa.js", filepath.Join("extensions", "server", "manwa", "scanning.js")}
	for _, relative := range paths {
		target := filepath.Join(cache, relative)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			t.Fatal(err)
		}
		body, err := os.ReadFile(filepath.Join(sourceRoot, relative))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return cache
}
