package catalog

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
)

const (
	DefaultObservationLimit = 10000
	DefaultIndexMaxBytes    = 4 << 20
)

var catalogIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$`)

// Config contains operator-owned catalog activation settings. Repository is
// never exposed through the public authority projection.
type Config struct {
	CatalogID        string
	Repository       string
	Revision         string
	CacheDir         string
	ObservationLimit int
	IndexMaxBytes    int64
}

func (c Config) Validate() error {
	if !catalogIDPattern.MatchString(c.CatalogID) {
		return errors.New("tracking catalog ID is required")
	}
	if c.Repository == "" {
		return errors.New("tracking catalog repository is required")
	}
	if c.Revision != "" && !isFullRevision(c.Revision) {
		return errors.New("tracking catalog revision must be a full commit")
	}
	if c.CacheDir != "" && !filepath.IsAbs(c.CacheDir) {
		return fmt.Errorf("tracking cache directory must be absolute")
	}
	if c.ObservationLimit <= 0 {
		return errors.New("tracking observation limit must be positive")
	}
	if c.IndexMaxBytes <= 0 {
		return errors.New("tracking index limit must be positive")
	}
	return nil
}
