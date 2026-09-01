package v2manifest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"venera-server/internal/v2config"
	"venera-server/internal/v2store"
)

type Manager struct {
	cfg    v2config.Config
	repo   *v2store.Repository
	client *http.Client
	now    func() time.Time
}

func NewManager(cfg v2config.Config, repo *v2store.Repository, client *http.Client) *Manager {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &Manager{cfg: cfg, repo: repo, client: client, now: time.Now}
}

func (manager *Manager) FetchAndActivate(ctx context.Context) (Manifest, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, manager.cfg.ManifestURL, nil)
	if err != nil {
		return Manifest{}, err
	}
	response, err := manager.client.Do(request)
	if err != nil {
		return Manifest{}, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return Manifest{}, fmt.Errorf("manifest endpoint returned status %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, MaxManifestSize+1))
	if err != nil {
		return Manifest{}, err
	}
	return manager.ValidateAndActivate(ctx, body)
}

func (manager *Manager) ValidateAndActivate(ctx context.Context, body []byte) (Manifest, error) {
	manifest, err := Parse(body)
	if err != nil {
		return Manifest{}, err
	}
	if !manifest.ExpiresAt.After(manager.now().UTC()) {
		return Manifest{}, errors.New("manifest is expired")
	}
	hash, canonical, err := HashJSON(body)
	if err != nil {
		return Manifest{}, err
	}
	if manager.repo == nil {
		return Manifest{}, errors.New("manifest repository is required")
	}
	active, activeErr := manager.repo.GetActiveManifestCatalog(ctx)
	if activeErr == nil {
		if active.CatalogSequence > manifest.CatalogSequence {
			return Manifest{}, v2store.ErrManifestSequenceRollback
		}
		if active.CatalogSequence == manifest.CatalogSequence && active.ManifestHash == hash {
			return manifest, nil
		}
	}
	if activeErr != nil && !errors.Is(activeErr, sql.ErrNoRows) {
		return Manifest{}, activeErr
	}
	manifestID := "manifest_" + strings.TrimPrefix(hash, "sha256:")[:20]
	packages := SortedPackages(manifest)
	artifacts := make([]v2store.SourceArtifactInput, 0, len(packages))
	releases := make([]v2store.SourcePackageReleaseInput, 0, len(packages))
	for _, pkg := range packages {
		packageJSON, marshalErr := CanonicalPackageJSON(pkg)
		if marshalErr != nil {
			return Manifest{}, marshalErr
		}
		state := pkg.Status
		if state == "stable" {
			state = "active"
		} else if state == "previous" {
			state = "superseded"
		}
		var scanningExtensionHash string
		for _, extension := range pkg.Extensions {
			if extension.Runtime == "server" && extension.Capabilities["scanning"] == 1 {
				scanningExtensionHash = extension.SHA256
				break
			}
		}
		artifacts = append(artifacts, v2store.SourceArtifactInput{
			ArtifactID: pkg.ArtifactID, SourceKey: pkg.SourceKey, CatalogID: manifest.CatalogID,
			ManagedState: pkg.ManagedState,
		})
		releases = append(releases, v2store.SourcePackageReleaseInput{
			PackageReleaseID: pkg.PackageReleaseID, ArtifactID: pkg.ArtifactID, CatalogID: manifest.CatalogID,
			CatalogSequence: manifest.CatalogSequence, CoreHash: pkg.Core.SHA256,
			ScanningExtensionHash:        scanningExtensionHash,
			ObservationContractID:        pkg.Tracking.ObservationContractID,
			AccountObservationContractID: pkg.Tracking.AccountObservationContractID,
			AccountProbeContractID:       pkg.AccountProbeContract.ID, MarkerSchemesJSON: marshalStrings(pkg.Tracking.MarkerSchemes),
			SessionExportProfileID: pkg.SessionExportProfile.ID, PackageJSON: string(packageJSON), State: state,
		})
	}
	if _, err := manager.repo.ActivateManifestBundle(ctx, v2store.ManifestBundleInput{
		Catalog: v2store.ManifestCatalogInput{
			ManifestID: manifestID, CatalogID: manifest.CatalogID, ManifestURL: manager.cfg.ManifestURL,
			CatalogSequence: manifest.CatalogSequence, ManifestHash: hash, ManifestJSON: string(canonical),
			IdempotencyKey: "manifest:" + hash,
		},
		Artifacts: artifacts,
		Releases:  releases,
	}); err != nil && !errors.Is(err, v2store.ErrManifestAlreadyActive) {
		return Manifest{}, err
	}
	if err := writeLastKnownGood(manager.cfg.ManifestCacheDir, canonical); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func CanonicalPackageJSON(pkg SourcePackage) ([]byte, error) {
	return json.Marshal(pkg)
}

func marshalStrings(values []string) string {
	encoded, _ := json.Marshal(values)
	return string(encoded)
}

func writeLastKnownGood(dir string, body []byte) error {
	if dir == "" {
		return errors.New("manifest cache directory is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(dir, ".index-v2-*.tmp")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if _, err := temporary.Write(body); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryName, filepath.Join(dir, "index-v2.json"))
}

func DownloadArtifact(ctx context.Context, client *http.Client, rawURL string, ref ArtifactRef, destinationDir string) (string, error) {
	if client == nil {
		client = http.DefaultClient
	}
	if ref.Size < 1 || ref.Size > MaxManifestSize*16 {
		return "", errors.New("artifact size is invalid")
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || !strings.EqualFold(parsed.Scheme, "https") || parsed.Host == "" {
		return "", errors.New("artifact URL is not an allowed HTTPS endpoint")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", err
	}
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", fmt.Errorf("artifact endpoint returned status %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, ref.Size+1))
	if err != nil {
		return "", err
	}
	if int64(len(body)) != ref.Size {
		return "", errors.New("artifact size mismatch")
	}
	sum := sha256.Sum256(body)
	if hex.EncodeToString(sum[:]) != ref.SHA256 {
		return "", errors.New("artifact hash mismatch")
	}
	if err := validateRelativePath(ref.Path); err != nil {
		return "", err
	}
	full, err := safeJoin(destinationDir, ref.Path)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return "", err
	}
	if existing, readErr := os.ReadFile(full); readErr == nil {
		if bytes.Equal(existing, body) {
			return full, nil
		}
		return "", errors.New("existing artifact differs from validated content")
	}
	temporary, err := os.CreateTemp(filepath.Dir(full), ".artifact-*.tmp")
	if err != nil {
		return "", err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if _, err := temporary.Write(body); err != nil {
		temporary.Close()
		return "", err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return "", err
	}
	if err := temporary.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(temporaryName, full); err != nil {
		return "", err
	}
	return full, nil
}
