package catalog

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"venera-server/internal/tracking/domain"
)

func isFullRevision(value string) bool { return domain.FullRevision(value) }

// Capability is a validated exact catalog entry. A missing CloudTracking
// object means Local-only and is deliberately not represented here.
type Capability struct {
	Artifact    domain.ArtifactIdentity
	Version     string
	ScannerPath string
}

// Snapshot is the immutable in-memory projection of one active revision.
type Snapshot struct {
	Authority    domain.Authority
	Capabilities []Capability
	Root         string
	ActivatedAt  time.Time
	Digest       string
}

// Manager validates a complete candidate before replacing the active
// snapshot. The old snapshot remains available after any failed activation.
type Manager struct {
	mu         sync.RWMutex
	cfg        Config
	active     *Snapshot
	generation int64
}

func NewManager(cfg Config) (*Manager, error) {
	if cfg.ObservationLimit == 0 {
		cfg.ObservationLimit = DefaultObservationLimit
	}
	if cfg.IndexMaxBytes == 0 {
		cfg.IndexMaxBytes = DefaultIndexMaxBytes
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Manager{cfg: cfg}, nil
}

// Activate resolves the configured full commit in the configured Git
// repository, materializes that commit into a private cache, and only then
// publishes its validated catalog snapshot. The repository working tree is
// never used as an executable checkout.
func (m *Manager) Activate(ctx context.Context, revision string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !isFullRevision(revision) {
		return errors.New("catalog_revision_invalid")
	}
	root, err := m.materializeRevision(ctx, revision)
	if err != nil {
		return err
	}
	return m.ActivatePath(ctx, revision, root)
}

func (m *Manager) materializeRevision(ctx context.Context, revision string) (string, error) {
	repository, err := filepath.Abs(m.cfg.Repository)
	if err != nil {
		return "", errors.New("catalog repository path is invalid")
	}
	info, err := os.Stat(repository)
	if err != nil || !info.IsDir() {
		return "", errors.New("catalog repository is unavailable")
	}
	resolved, err := gitOutput(ctx, repository, "rev-parse", "--verify", revision+"^{commit}")
	if err != nil {
		return "", fmt.Errorf("resolve catalog revision: %w", err)
	}
	if strings.TrimSpace(resolved) != revision {
		return "", errors.New("catalog revision does not match repository commit")
	}

	cacheDir := m.cfg.CacheDir
	if cacheDir == "" {
		cacheDir = filepath.Join(os.TempDir(), "venera-tracking-cache", safeCacheName(m.cfg.CatalogID))
	}
	cacheDir, err = filepath.Abs(cacheDir)
	if err != nil {
		return "", errors.New("tracking cache directory is invalid")
	}
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return "", fmt.Errorf("create tracking cache: %w", err)
	}
	staging := filepath.Join(cacheDir, "."+revision+".materializing.tmp")
	_ = os.RemoveAll(staging)
	if err := os.MkdirAll(staging, 0o755); err != nil {
		return "", fmt.Errorf("create catalog staging directory: %w", err)
	}
	archiveFile, err := os.CreateTemp(cacheDir, "."+revision+".archive-*.tmp")
	if err != nil {
		_ = os.RemoveAll(staging)
		return "", fmt.Errorf("create catalog archive: %w", err)
	}
	archivePath := archiveFile.Name()
	if err := archiveFile.Close(); err != nil {
		_ = os.RemoveAll(staging)
		_ = os.Remove(archivePath)
		return "", fmt.Errorf("prepare catalog archive: %w", err)
	}
	cleanup := func() {
		_ = os.RemoveAll(staging)
		_ = os.Remove(archivePath)
	}

	command := exec.CommandContext(ctx, "git", "-C", repository, "archive", "--format=tar", "--output", archivePath, revision)
	if output, err := command.CombinedOutput(); err != nil {
		cleanup()
		return "", fmt.Errorf("materialize catalog revision: %w (%s)", err, strings.TrimSpace(string(output)))
	}
	archive, err := os.Open(archivePath)
	if err != nil {
		cleanup()
		return "", fmt.Errorf("open catalog archive: %w", err)
	}
	extractErr := extractArchive(archive, staging)
	closeErr := archive.Close()
	if extractErr != nil {
		cleanup()
		return "", fmt.Errorf("extract catalog revision: %w", extractErr)
	}
	if closeErr != nil {
		cleanup()
		return "", fmt.Errorf("close catalog archive: %w", closeErr)
	}
	_ = os.Remove(archivePath)
	if err := os.WriteFile(filepath.Join(staging, ".venera-revision"), []byte(revision+"\n"), 0o644); err != nil {
		cleanup()
		return "", fmt.Errorf("write catalog identity: %w", err)
	}
	if _, err := validateCheckout(staging, revision, m.cfg); err != nil {
		cleanup()
		return "", fmt.Errorf("validate materialized catalog: %w", err)
	}

	target := filepath.Join(cacheDir, revision)
	if err := os.RemoveAll(target); err != nil {
		cleanup()
		return "", fmt.Errorf("replace cached catalog: %w", err)
	}
	if err := os.Rename(staging, target); err != nil {
		cleanup()
		return "", fmt.Errorf("publish cached catalog: %w", err)
	}
	return target, nil
}

func gitOutput(ctx context.Context, repository string, args ...string) (string, error) {
	commandArgs := append([]string{"-C", repository}, args...)
	output, err := exec.CommandContext(ctx, "git", commandArgs...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git command failed: %w", err)
	}
	return string(output), nil
}

func safeCacheName(value string) string {
	value = strings.NewReplacer("/", "_", "\\", "_").Replace(value)
	if value == "" {
		return "catalog"
	}
	return value
}

func extractArchive(reader io.Reader, root string) error {
	root, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	tarReader := tar.NewReader(reader)
	for {
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if header.Name == "" {
			return errors.New("catalog archive contains an empty path")
		}
		name := filepath.Clean(filepath.FromSlash(header.Name))
		if filepath.IsAbs(name) || name == "." || name == ".." || strings.HasPrefix(name, ".."+string(filepath.Separator)) {
			return errors.New("catalog archive path escapes staging directory")
		}
		target := filepath.Join(root, name)
		if !isWithin(root, target) {
			return errors.New("catalog archive path escapes staging directory")
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeXHeader, tar.TypeXGlobalHeader:
			if header.Size < 0 {
				return errors.New("catalog archive contains an invalid metadata size")
			}
			if _, err := io.CopyN(io.Discard, tarReader, header.Size); err != nil {
				return err
			}
		case tar.TypeReg, tar.TypeRegA:
			if header.Size < 0 {
				return errors.New("catalog archive contains an invalid file size")
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			file, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
			if err != nil {
				return err
			}
			written, copyErr := io.CopyN(file, tarReader, header.Size)
			closeErr := file.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
			if written != header.Size {
				return errors.New("catalog archive file is truncated")
			}
		default:
			return fmt.Errorf("catalog archive contains an unsupported link or special file: %q type=%d", header.Name, header.Typeflag)
		}
	}
}

// ActivatePath is primarily used by tests and by an operator-local material
// fetcher after it has materialized an immutable checkout.
func (m *Manager) ActivatePath(ctx context.Context, revision, root string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !isFullRevision(revision) {
		return errors.New("catalog_revision_invalid")
	}
	candidate, err := validateCheckout(root, revision, m.cfg)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.generation++
	candidate.Authority.Generation = m.generation
	candidate.Authority.CatalogID = m.cfg.CatalogID
	candidate.Digest, err = authorityDigest(candidate.Authority)
	if err != nil {
		return err
	}
	candidate.ActivatedAt = time.Now().UTC()
	m.active = &candidate
	return nil
}

func (m *Manager) Active() (Snapshot, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.active == nil {
		return Snapshot{}, false
	}
	copy := *m.active
	copy.Capabilities = append([]Capability(nil), m.active.Capabilities...)
	copy.Authority.Artifacts = append([]domain.ArtifactIdentity(nil), m.active.Authority.Artifacts...)
	return copy, true
}

func (m *Manager) Authority() (domain.Authority, bool) {
	snapshot, ok := m.Active()
	return snapshot.Authority, ok
}

func (m *Manager) Generation() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.generation
}

func (m *Manager) ObservationLimit() int { return m.cfg.ObservationLimit }

func (m *Manager) HasArtifact(artifact domain.ArtifactIdentity) bool {
	snapshot, ok := m.Active()
	if !ok {
		return false
	}
	for _, candidate := range snapshot.Capabilities {
		if candidate.Artifact == artifact {
			return true
		}
	}
	return false
}

func ParseIndex(data []byte, root string, maxBytes int64) ([]Capability, error) {
	if int64(len(data)) > maxBytes {
		return nil, errors.New("catalog index exceeds size limit")
	}
	return parseIndex(data, root)
}

func validateCheckout(root, revision string, cfg Config) (Snapshot, error) {
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		return Snapshot{}, errors.New("catalog checkout is unavailable")
	}
	identityPath := filepath.Join(root, ".venera-revision")
	if identityInfo, identityErr := os.Lstat(identityPath); identityErr == nil {
		if !identityInfo.Mode().IsRegular() || identityInfo.Mode()&os.ModeSymlink != 0 {
			return Snapshot{}, errors.New("catalog checkout identity is unavailable")
		}
		identity, readErr := os.ReadFile(identityPath)
		if readErr != nil || strings.TrimSpace(string(identity)) != revision {
			return Snapshot{}, errors.New("catalog checkout identity does not match revision")
		}
	} else if !errors.Is(identityErr, os.ErrNotExist) {
		return Snapshot{}, errors.New("catalog checkout identity is unavailable")
	}
	indexPath := filepath.Join(root, "index.json")
	indexInfo, err := os.Lstat(indexPath)
	if err != nil || !indexInfo.Mode().IsRegular() || indexInfo.Mode()&os.ModeSymlink != 0 {
		return Snapshot{}, errors.New("catalog index is unavailable")
	}
	if indexInfo.Size() > cfg.IndexMaxBytes {
		return Snapshot{}, errors.New("catalog index exceeds size limit")
	}
	data, err := os.ReadFile(indexPath)
	if err != nil {
		return Snapshot{}, errors.New("catalog index cannot be read")
	}
	capabilities, err := parseIndex(data, root)
	if err != nil {
		return Snapshot{}, err
	}
	artifacts := make([]domain.ArtifactIdentity, 0, len(capabilities))
	for _, capability := range capabilities {
		artifacts = append(artifacts, capability.Artifact)
	}
	return Snapshot{
		Authority: domain.Authority{
			CatalogID:      cfg.CatalogID,
			ActiveRevision: revision,
			Artifacts:      artifacts,
		},
		Capabilities: capabilities,
		Root:         root,
	}, nil
}

func parseIndex(data []byte, root string) ([]Capability, error) {
	var entries []map[string]json.RawMessage
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("invalid catalog index: %w", err)
	}
	seen := make(map[domain.ArtifactIdentity]struct{})
	capabilities := make([]Capability, 0)
	for _, entry := range entries {
		var key, fileName, version string
		if err := json.Unmarshal(entry["key"], &key); err != nil || key == "" {
			return nil, errors.New("catalog entry has invalid key")
		}
		if err := json.Unmarshal(entry["fileName"], &fileName); err != nil || fileName == "" {
			return nil, errors.New("catalog entry has invalid fileName")
		}
		_ = json.Unmarshal(entry["version"], &version)
		artifact := domain.ArtifactIdentity{SourceKey: key, FileName: fileName}
		if err := artifact.Validate(); err != nil {
			return nil, fmt.Errorf("catalog artifact %s: %w", fileName, err)
		}
		if _, exists := seen[artifact]; exists {
			return nil, errors.New("catalog contains duplicate artifact")
		}
		seen[artifact] = struct{}{}
		var cloud map[string]json.RawMessage
		if value, ok := entry["cloudTracking"]; ok && string(value) != "null" {
			if err := json.Unmarshal(value, &cloud); err != nil {
				return nil, errors.New("catalog cloudTracking must be an object")
			}
		}
		if cloud == nil {
			continue
		}
		var scanner string
		if err := json.Unmarshal(cloud["scanner"], &scanner); err != nil || scanner == "" {
			return nil, errors.New("catalog cloudTracking scanner is required")
		}
		safePath, err := SafeScannerPath(root, scanner)
		if err != nil {
			return nil, err
		}
		capabilities = append(capabilities, Capability{
			Artifact:    artifact,
			Version:     version,
			ScannerPath: safePath,
		})
	}
	return capabilities, nil
}

// SafeScannerPath resolves a scanner only when it remains below the pinned
// checkout and points to a regular file. Absolute paths and traversal are
// rejected before filesystem access can escape the checkout.
func SafeScannerPath(root, relative string) (string, error) {
	if filepath.IsAbs(relative) || strings.TrimSpace(relative) == "" {
		return "", errors.New("scanner path is not relative")
	}
	clean := filepath.Clean(relative)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", errors.New("scanner path escapes catalog checkout")
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", errors.New("catalog checkout path is invalid")
	}
	pathAbs, err := filepath.Abs(filepath.Join(rootAbs, clean))
	if err != nil || !isWithin(rootAbs, pathAbs) {
		return "", errors.New("scanner path escapes catalog checkout")
	}
	rootReal, err := filepath.EvalSymlinks(rootAbs)
	if err != nil {
		return "", errors.New("catalog checkout path is invalid")
	}
	pathReal, err := filepath.EvalSymlinks(pathAbs)
	if err != nil || !isWithin(rootReal, pathReal) {
		return "", errors.New("scanner path escapes catalog checkout")
	}
	info, err := os.Stat(pathReal)
	if err != nil || !info.Mode().IsRegular() {
		return "", errors.New("scanner path is not a regular file")
	}
	return pathReal, nil
}

func isWithin(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func authorityDigest(authority domain.Authority) (string, error) {
	data, err := domain.CanonicalJSON(authority)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
