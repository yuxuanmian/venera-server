package v2manifest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	CatalogID       = "yuxuanmian/venera-configs:yxm"
	ManifestSchema  = 2
	RuntimeAPIV1    = 1
	ScanningAPIV1   = 1
	MaxManifestSize = 8 << 20
)

var (
	artifactIDPattern     = regexp.MustCompile(`^[a-z][a-z0-9._-]*$`)
	packageReleasePattern = regexp.MustCompile(`^rel_[0-9a-f]{20}$`)
	sha256Pattern         = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

type Manifest struct {
	SchemaVersion   int             `json:"schemaVersion"`
	CatalogID       string          `json:"catalogId"`
	CatalogSequence int64           `json:"catalogSequence"`
	GeneratedAt     time.Time       `json:"generatedAt"`
	ExpiresAt       time.Time       `json:"expiresAt"`
	SourcePackages  []SourcePackage `json:"sourcePackages"`
}

type ArtifactRef struct {
	Path              string `json:"path"`
	SHA256            string `json:"sha256"`
	Size              int64  `json:"size"`
	MIME              string `json:"mime"`
	RuntimeAPIVersion int    `json:"runtimeApiVersion"`
	MinVeneraVersion  string `json:"minVeneraVersion,omitempty"`
	MinServerVersion  string `json:"minServerVersion,omitempty"`
}

type ExtensionRef struct {
	ExtensionID       string         `json:"extensionId"`
	Runtime           string         `json:"runtime"`
	Path              string         `json:"path"`
	SHA256            string         `json:"sha256"`
	Size              int64          `json:"size"`
	MIME              string         `json:"mime"`
	RuntimeAPIVersion int            `json:"runtimeApiVersion"`
	Required          bool           `json:"required"`
	Capabilities      map[string]int `json:"capabilities"`
}

type RuntimeCompatibility struct {
	MinVeneraVersion string `json:"minVeneraVersion"`
	MinServerVersion string `json:"minServerVersion"`
	HostAPIVersion   int    `json:"hostApiVersion"`
}

type MarkerPolicy struct {
	Preferred string   `json:"preferred"`
	Fallbacks []string `json:"fallbacks"`
}

type Tracking struct {
	ObservationContractID        string       `json:"observationContractId"`
	AccountObservationContractID string       `json:"accountObservationContractId"`
	MarkerPolicy                 MarkerPolicy `json:"markerPolicy"`
	ClientPolicy                 string       `json:"clientPolicy"`
	MarkerSchemes                []string     `json:"markerSchemes"`
}

type AccountProbeContract struct {
	ID                     string   `json:"id"`
	Version                int      `json:"version"`
	IdentitySchemes        []string `json:"identitySchemes"`
	AttributeFields        []string `json:"attributeFields"`
	VisibilityScopePattern string   `json:"visibilityScopePattern"`
}

type SessionExportProfile struct {
	ID                      string   `json:"id"`
	Version                 int      `json:"version"`
	RequiresExplicitConsent bool     `json:"requiresExplicitConsent"`
	AllowedOrigins          []string `json:"allowedOrigins"`
	AllowedCookieDomains    []string `json:"allowedCookieDomains"`
	AllowedCookiePathPolicy string   `json:"allowedCookiePathPolicy,omitempty"`
	MaxCookies              int      `json:"maxCookies"`
	MaxSerializedBytes      int64    `json:"maxSerializedBytes"`
}

type ScanPolicyRecommended struct {
	MaxConcurrency             int `json:"maxConcurrency"`
	MinRequestStartIntervalMS  int `json:"minRequestStartIntervalMs"`
	SnapshotSliceRequestBudget int `json:"snapshotSliceRequestBudget"`
	SnapshotSliceItemBudget    int `json:"snapshotSliceItemBudget"`
	DetailBatchTarget          int `json:"detailBatchTarget"`
}

type ScanPolicyLimits struct {
	MaxConcurrencyCeiling  int   `json:"maxConcurrencyCeiling"`
	RequestIntervalFloorMS int   `json:"requestIntervalFloorMs"`
	MaxBurstRequests       int   `json:"maxBurstRequests"`
	MaxSnapshotItems       int   `json:"maxSnapshotItems"`
	MaxOutputBytes         int64 `json:"maxOutputBytes"`
	MaxResponseBytes       int64 `json:"maxResponseBytes"`
}

type ScanPolicy struct {
	Recommended ScanPolicyRecommended `json:"recommended"`
	Limits      ScanPolicyLimits      `json:"limits"`
}

type SourcePackage struct {
	ArtifactID           string               `json:"artifactId"`
	SourceKey            string               `json:"sourceKey"`
	ManagedState         string               `json:"managedState"`
	PackageReleaseID     string               `json:"packageReleaseId"`
	DisplayName          string               `json:"displayName"`
	DisplayVersion       string               `json:"displayVersion"`
	Status               string               `json:"status"`
	Core                 ArtifactRef          `json:"core"`
	Extensions           []ExtensionRef       `json:"extensions"`
	RuntimeCompatibility RuntimeCompatibility `json:"runtimeCompatibility"`
	Tracking             Tracking             `json:"tracking"`
	AccountProbeContract AccountProbeContract `json:"accountProbeContract"`
	SessionExportProfile SessionExportProfile `json:"sessionExportProfile"`
	ScanPolicy           ScanPolicy           `json:"scanPolicy"`
}

func Parse(data []byte) (Manifest, error) {
	if len(data) == 0 || len(data) > MaxManifestSize {
		return Manifest{}, errors.New("manifest size is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode manifest: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return Manifest{}, errors.New("manifest has multiple JSON values")
	}
	if err := Validate(manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func Validate(manifest Manifest) error {
	if manifest.SchemaVersion != ManifestSchema || manifest.CatalogID != CatalogID || manifest.CatalogSequence < 0 || manifest.GeneratedAt.IsZero() || manifest.ExpiresAt.IsZero() || len(manifest.SourcePackages) == 0 {
		return errors.New("manifest header is invalid")
	}
	if !manifest.ExpiresAt.After(manifest.GeneratedAt) {
		return errors.New("manifest expiry is invalid")
	}
	artifactIDs := make(map[string]struct{}, len(manifest.SourcePackages))
	packageReleaseIDs := make(map[string]struct{}, len(manifest.SourcePackages))
	for i := range manifest.SourcePackages {
		pkg := &manifest.SourcePackages[i]
		if err := validatePackage(*pkg); err != nil {
			return fmt.Errorf("sourcePackages[%d]: %w", i, err)
		}
		if _, exists := artifactIDs[pkg.ArtifactID]; exists {
			return fmt.Errorf("duplicate artifactId %q", pkg.ArtifactID)
		}
		artifactIDs[pkg.ArtifactID] = struct{}{}
		if _, exists := packageReleaseIDs[pkg.PackageReleaseID]; exists {
			return fmt.Errorf("duplicate packageReleaseId %q", pkg.PackageReleaseID)
		}
		packageReleaseIDs[pkg.PackageReleaseID] = struct{}{}
	}
	return nil
}

func validatePackage(pkg SourcePackage) error {
	if !artifactIDPattern.MatchString(pkg.ArtifactID) || pkg.SourceKey == "" || (pkg.ManagedState != "active" && pkg.ManagedState != "paused") || !packageReleasePattern.MatchString(pkg.PackageReleaseID) || pkg.DisplayName == "" || pkg.DisplayVersion == "" {
		return errors.New("package identity is invalid")
	}
	if pkg.Status != "stable" && pkg.Status != "candidate" && pkg.Status != "previous" {
		return errors.New("package status is invalid")
	}
	if err := validateArtifact(pkg.Core); err != nil {
		return fmt.Errorf("core: %w", err)
	}
	assetPaths := map[string]struct{}{pkg.Core.Path: {}}
	serverScanning := 0
	seenExtensions := map[string]struct{}{}
	for i, extension := range pkg.Extensions {
		if extension.ExtensionID == "" || extension.Runtime != "client" && extension.Runtime != "server" || extension.RuntimeAPIVersion != RuntimeAPIV1 || len(extension.Capabilities) == 0 {
			return fmt.Errorf("extension %d identity is invalid", i)
		}
		if _, exists := seenExtensions[extension.ExtensionID]; exists {
			return fmt.Errorf("duplicate extension %q", extension.ExtensionID)
		}
		seenExtensions[extension.ExtensionID] = struct{}{}
		if err := validateArtifact(ArtifactRef{Path: extension.Path, SHA256: extension.SHA256, Size: extension.Size, MIME: extension.MIME, RuntimeAPIVersion: extension.RuntimeAPIVersion}); err != nil {
			return fmt.Errorf("extension %q: %w", extension.ExtensionID, err)
		}
		if _, exists := assetPaths[extension.Path]; exists {
			return fmt.Errorf("duplicate package asset path %q", extension.Path)
		}
		assetPaths[extension.Path] = struct{}{}
		for capability, version := range extension.Capabilities {
			if (capability != "tracking" && capability != "scanning" && capability != "comic-actions") || version != 1 {
				return fmt.Errorf("extension %q has unknown capability", extension.ExtensionID)
			}
			if extension.Runtime == "server" && capability == "scanning" {
				serverScanning++
			}
		}
	}
	if serverScanning > 1 {
		return errors.New("more than one server scanning provider")
	}
	if pkg.RuntimeCompatibility.HostAPIVersion != 1 || pkg.RuntimeCompatibility.MinServerVersion == "" || pkg.RuntimeCompatibility.MinVeneraVersion == "" {
		return errors.New("runtime compatibility is invalid")
	}
	if err := validateTracking(pkg.Tracking); err != nil {
		return err
	}
	if pkg.AccountProbeContract.Version != 1 || pkg.AccountProbeContract.ID == "" || len(pkg.AccountProbeContract.IdentitySchemes) == 0 || pkg.AccountProbeContract.VisibilityScopePattern == "" {
		return errors.New("account probe contract is invalid")
	}
	if err := validateSessionProfile(pkg.SessionExportProfile); err != nil {
		return err
	}
	if err := validateScanPolicy(pkg.ScanPolicy); err != nil {
		return err
	}
	return nil
}

func validateArtifact(artifact ArtifactRef) error {
	if err := validateRelativePath(artifact.Path); err != nil {
		return err
	}
	if !sha256Pattern.MatchString(artifact.SHA256) || artifact.Size < 1 || artifact.MIME == "" || artifact.RuntimeAPIVersion != RuntimeAPIV1 {
		return errors.New("artifact reference is invalid")
	}
	return nil
}

func validateTracking(tracking Tracking) error {
	if tracking.ObservationContractID == "" || tracking.AccountObservationContractID == "" || tracking.ClientPolicy == "" || (tracking.MarkerPolicy.Preferred != "host-default" && tracking.MarkerPolicy.Preferred != "source-defined") || len(tracking.MarkerSchemes) == 0 {
		return errors.New("tracking contract is invalid")
	}
	seen := map[string]struct{}{}
	for _, marker := range tracking.MarkerSchemes {
		if marker == "" {
			return errors.New("tracking marker scheme is invalid")
		}
		if _, exists := seen[marker]; exists {
			return errors.New("duplicate tracking marker scheme")
		}
		seen[marker] = struct{}{}
	}
	for _, fallback := range tracking.MarkerPolicy.Fallbacks {
		if fallback != "host-default" && fallback != "source-defined" {
			return errors.New("tracking fallback is invalid")
		}
	}
	return nil
}

func validateSessionProfile(profile SessionExportProfile) error {
	if profile.ID == "" || profile.Version != 1 || len(profile.AllowedOrigins) == 0 || len(profile.AllowedCookieDomains) == 0 || profile.MaxCookies < 1 || profile.MaxSerializedBytes < 1 {
		return errors.New("session export profile is invalid")
	}
	for _, origin := range profile.AllowedOrigins {
		parsed, err := url.Parse(origin)
		if err != nil || !strings.EqualFold(parsed.Scheme, "https") || parsed.Host == "" || parsed.Path != "" && parsed.Path != "/" {
			return errors.New("session profile origin is invalid")
		}
	}
	if profile.AllowedCookiePathPolicy != "" && profile.AllowedCookiePathPolicy != "standard" {
		return errors.New("session profile cookie path policy is invalid")
	}
	return nil
}

func validateScanPolicy(policy ScanPolicy) error {
	r, l := policy.Recommended, policy.Limits
	if r.MaxConcurrency < 1 || r.MaxConcurrency > l.MaxConcurrencyCeiling || r.MinRequestStartIntervalMS < l.RequestIntervalFloorMS || r.SnapshotSliceRequestBudget < 1 || r.SnapshotSliceItemBudget < 1 || r.DetailBatchTarget < 1 || l.MaxConcurrencyCeiling < 1 || l.MaxBurstRequests < 1 || l.MaxSnapshotItems < 1 || l.MaxOutputBytes < 1 || l.MaxResponseBytes < 1 {
		return errors.New("scan policy is invalid")
	}
	return nil
}

func validateRelativePath(value string) error {
	if value == "" || filepath.IsAbs(value) || strings.Contains(value, "\\") {
		return errors.New("artifact path is not relative")
	}
	clean := path.Clean(value)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return errors.New("artifact path escapes package root")
	}
	return nil
}

func CanonicalJSON(manifest Manifest) ([]byte, error) {
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

func HashJSON(data []byte) (string, []byte, error) {
	manifest, err := Parse(data)
	if err != nil {
		return "", nil, err
	}
	canonical, err := CanonicalJSON(manifest)
	if err != nil {
		return "", nil, err
	}
	sum := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(sum[:]), canonical, nil
}

func ValidatePackageFiles(root string, pkg SourcePackage) error {
	refs := []ArtifactRef{pkg.Core}
	for _, extension := range pkg.Extensions {
		refs = append(refs, ArtifactRef{Path: extension.Path, SHA256: extension.SHA256, Size: extension.Size, MIME: extension.MIME, RuntimeAPIVersion: extension.RuntimeAPIVersion})
	}
	for _, ref := range refs {
		if err := validateRelativePath(ref.Path); err != nil {
			return err
		}
		full, err := safeJoin(root, ref.Path)
		if err != nil {
			return err
		}
		file, err := os.Open(full)
		if err != nil {
			return fmt.Errorf("open manifest artifact %q: %w", ref.Path, err)
		}
		stat, statErr := file.Stat()
		if statErr != nil {
			file.Close()
			return statErr
		}
		if stat.Size() != ref.Size {
			file.Close()
			return fmt.Errorf("artifact %q size mismatch", ref.Path)
		}
		hasher := sha256.New()
		if _, err := io.CopyN(hasher, file, ref.Size+1); err != nil && !errors.Is(err, io.EOF) {
			file.Close()
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
		if hex.EncodeToString(hasher.Sum(nil)) != ref.SHA256 {
			return fmt.Errorf("artifact %q hash mismatch", ref.Path)
		}
	}
	return nil
}

func safeJoin(root, relative string) (string, error) {
	if err := validateRelativePath(relative); err != nil {
		return "", err
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	target := filepath.Join(rootAbs, filepath.FromSlash(relative))
	targetAbs, err := filepath.Abs(target)
	if err != nil {
		return "", err
	}
	prefix := rootAbs + string(os.PathSeparator)
	if targetAbs != rootAbs && !strings.HasPrefix(targetAbs, prefix) {
		return "", errors.New("artifact path escapes package root")
	}
	return targetAbs, nil
}

func SortedPackages(manifest Manifest) []SourcePackage {
	packages := append([]SourcePackage(nil), manifest.SourcePackages...)
	sort.Slice(packages, func(i, j int) bool { return packages[i].ArtifactID < packages[j].ArtifactID })
	return packages
}
