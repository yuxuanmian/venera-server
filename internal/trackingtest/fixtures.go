// Package trackingtest contains cross-runtime contract fixtures shared by
// Server tracking tests. It intentionally has no dependency on the tracking
// implementation packages.
package trackingtest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
)

const FixtureVersion = "tracking-v1-fixtures-1"

// FixturePath returns the checked-in fixture path, independent of the test's
// current working directory.
func FixturePath() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "testdata", "tracking-v1.json")
}

// LoadFixture decodes the canonical fixture without imposing implementation-
// specific types on the data consumed by individual package tests.
func LoadFixture() (map[string]any, error) {
	data, err := os.ReadFile(FixturePath())
	if err != nil {
		return nil, err
	}
	var fixture map[string]any
	if err := json.Unmarshal(data, &fixture); err != nil {
		return nil, err
	}
	return fixture, nil
}

// LoadFixtureBytes returns the exact bytes used for checksum assertions.
func LoadFixtureBytes() ([]byte, error) {
	return os.ReadFile(FixturePath())
}
