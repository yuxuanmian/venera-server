package v2manifest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"venera-server/internal/v2config"
	"venera-server/internal/v2crypto"
	"venera-server/internal/v2store"
)

func currentIndexV2(t *testing.T) []byte {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate manifest test")
	}
	path := filepath.Join(filepath.Dir(filename), "..", "..", "..", "venera-configs", "index-v2.json")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read current index-v2: %v", err)
	}
	return body
}

func TestParseAndValidateCurrentIndexV2(t *testing.T) {
	manifest, err := Parse(currentIndexV2(t))
	if err != nil {
		t.Fatalf("parse current index-v2: %v", err)
	}
	if manifest.CatalogID != CatalogID || manifest.CatalogSequence != 3 || len(manifest.SourcePackages) != 1 {
		t.Fatalf("unexpected manifest header: %+v", manifest)
	}
	pkg := manifest.SourcePackages[0]
	if pkg.ArtifactID != "manwa" || pkg.Status != "candidate" || len(pkg.Extensions) != 1 || pkg.Extensions[0].Runtime != "server" || pkg.Extensions[0].Capabilities["scanning"] != 1 {
		t.Fatalf("unexpected Manwa package: %+v", pkg)
	}
}

func TestManifestRejectsUnknownFieldDuplicateAndPathEscape(t *testing.T) {
	var raw map[string]any
	if err := json.Unmarshal(currentIndexV2(t), &raw); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	raw["unexpected"] = true
	unknown, _ := json.Marshal(raw)
	if _, err := Parse(unknown); err == nil {
		t.Fatal("unknown manifest field unexpectedly accepted")
	}
	manifest, err := Parse(currentIndexV2(t))
	if err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	sharedSourceKey := manifest
	sharedSourceKey.SourcePackages = append(append([]SourcePackage(nil), manifest.SourcePackages...), manifest.SourcePackages[0])
	sharedSourceKey.SourcePackages[1].ArtifactID = "manwa-copy"
	sharedSourceKey.SourcePackages[1].PackageReleaseID = "rel_22222222222222222222"
	if err := Validate(sharedSourceKey); err != nil {
		t.Fatalf("shared sourceKey should be accepted: %v", err)
	}
	duplicateRelease := manifest
	duplicateRelease.SourcePackages = append(append([]SourcePackage(nil), manifest.SourcePackages...), manifest.SourcePackages[0])
	duplicateRelease.SourcePackages[1].ArtifactID = "manwa-copy"
	if err := Validate(duplicateRelease); err == nil || !strings.Contains(err.Error(), "duplicate packageReleaseId") {
		t.Fatalf("duplicate package release validation error = %v", err)
	}
	duplicateCorePath := manifest
	duplicateCorePath.SourcePackages = append([]SourcePackage(nil), manifest.SourcePackages...)
	duplicateCorePath.SourcePackages[0].Core.Path = duplicateCorePath.SourcePackages[0].Extensions[0].Path
	if err := Validate(duplicateCorePath); err == nil || !strings.Contains(err.Error(), "duplicate package asset path") {
		t.Fatalf("core/extension duplicate path validation error = %v", err)
	}
	duplicateExtensionPath := manifest
	duplicateExtensionPath.SourcePackages = append([]SourcePackage(nil), manifest.SourcePackages...)
	duplicatePackage := duplicateExtensionPath.SourcePackages[0]
	duplicatePackage.Extensions = append([]ExtensionRef(nil), duplicatePackage.Extensions...)
	duplicateExtension := duplicatePackage.Extensions[0]
	duplicateExtension.ExtensionID = "manwa.tracking"
	duplicateExtension.Runtime = "client"
	duplicateExtension.Required = false
	duplicateExtension.Capabilities = map[string]int{"tracking": 1}
	duplicatePackage.Extensions = append(duplicatePackage.Extensions, duplicateExtension)
	duplicateExtensionPath.SourcePackages[0] = duplicatePackage
	if err := Validate(duplicateExtensionPath); err == nil || !strings.Contains(err.Error(), "duplicate package asset path") {
		t.Fatalf("extension/extension duplicate path validation error = %v", err)
	}
	duplicate := manifest
	duplicate.SourcePackages = append(append([]SourcePackage(nil), manifest.SourcePackages...), manifest.SourcePackages[0])
	if err := Validate(duplicate); err == nil || !strings.Contains(err.Error(), "duplicate artifactId") {
		t.Fatalf("duplicate artifact validation error = %v", err)
	}
	escape := manifest
	escape.SourcePackages = append([]SourcePackage(nil), manifest.SourcePackages...)
	escape.SourcePackages[0].Core.Path = "../outside.js"
	if err := Validate(escape); err == nil || !strings.Contains(err.Error(), "artifact path") {
		t.Fatalf("path escape validation error = %v", err)
	}
	capability := manifest
	capability.SourcePackages = append([]SourcePackage(nil), manifest.SourcePackages...)
	capability.SourcePackages[0].Extensions = append([]ExtensionRef(nil), manifest.SourcePackages[0].Extensions...)
	capability.SourcePackages[0].Extensions[0].Capabilities = map[string]int{"unknown": 1}
	if err := Validate(capability); err == nil || !strings.Contains(err.Error(), "unknown capability") {
		t.Fatalf("unknown capability validation error = %v", err)
	}
}

func TestManifestHashCanonicalAndArtifactFileVerification(t *testing.T) {
	first := currentIndexV2(t)
	hash1, canonical1, err := HashJSON(first)
	if err != nil {
		t.Fatalf("hash manifest: %v", err)
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, first, "", "  "); err != nil {
		t.Fatalf("indent manifest: %v", err)
	}
	hash2, canonical2, err := HashJSON(pretty.Bytes())
	if err != nil || hash1 != hash2 || !bytes.Equal(canonical1, canonical2) {
		t.Fatalf("canonical manifest hash changed: %s/%s err=%v", hash1, hash2, err)
	}
	content := []byte("approved artifact")
	sum := sha256.Sum256(content)
	pkg := SourcePackage{
		ArtifactID: "demo", SourceKey: "demo", ManagedState: "active", PackageReleaseID: "rel_1234567890abcdef1234",
		DisplayName: "Demo", DisplayVersion: "1", Status: "candidate",
		Core:       ArtifactRef{Path: "demo.js", SHA256: hex.EncodeToString(sum[:]), Size: int64(len(content)), MIME: "text/javascript", RuntimeAPIVersion: 1},
		Extensions: []ExtensionRef{}, RuntimeCompatibility: RuntimeCompatibility{MinVeneraVersion: "1", MinServerVersion: "2", HostAPIVersion: 1},
		Tracking:             Tracking{ObservationContractID: "obs", AccountObservationContractID: "aobs", MarkerPolicy: MarkerPolicy{Preferred: "source-defined"}, ClientPolicy: "marker", MarkerSchemes: []string{"marker"}},
		AccountProbeContract: AccountProbeContract{ID: "probe", Version: 1, IdentitySchemes: []string{"identity"}, VisibilityScopePattern: ".*"},
		SessionExportProfile: SessionExportProfile{ID: "session", Version: 1, AllowedOrigins: []string{"https://example.test"}, AllowedCookieDomains: []string{"example.test"}, MaxCookies: 1, MaxSerializedBytes: 100},
		ScanPolicy: ScanPolicy{
			Recommended: ScanPolicyRecommended{MaxConcurrency: 1, SnapshotSliceRequestBudget: 1, SnapshotSliceItemBudget: 1, DetailBatchTarget: 1},
			Limits:      ScanPolicyLimits{MaxConcurrencyCeiling: 1, MaxBurstRequests: 1, MaxSnapshotItems: 1, MaxOutputBytes: 1, MaxResponseBytes: 1},
		},
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "demo.js"), content, 0o600); err != nil {
		t.Fatalf("write artifact fixture: %v", err)
	}
	if err := ValidatePackageFiles(root, pkg); err != nil {
		t.Fatalf("validate artifact fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "demo.js"), []byte("tampered"), 0o600); err != nil {
		t.Fatalf("tamper artifact fixture: %v", err)
	}
	if err := ValidatePackageFiles(root, pkg); err == nil {
		t.Fatal("tampered artifact unexpectedly accepted")
	}
}

func TestManagerKeepsLastKnownGoodOnInvalidUpdate(t *testing.T) {
	manifestBody := currentIndexV2(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		_, _ = w.Write(manifestBody)
	}))
	defer server.Close()
	root := bytes.Repeat([]byte{0x37}, v2crypto.RootSecretSize)
	keys, err := v2crypto.DeriveKeys(root)
	if err != nil {
		t.Fatalf("derive manager keys: %v", err)
	}
	db, err := v2store.Open(filepath.Join(t.TempDir(), "manager.db"))
	if err != nil {
		t.Fatalf("open manager db: %v", err)
	}
	defer db.Close()
	repo := v2store.NewRepository(db, keys)
	cfg := v2config.Default()
	cfg.RootSecret = root
	cfg.ManifestURL = server.URL
	cfg.ManifestCacheDir = filepath.Join(t.TempDir(), "cache")
	manager := NewManager(cfg, repo, server.Client())
	if _, err := manager.FetchAndActivate(context.Background()); err != nil {
		t.Fatalf("activate valid manifest: %v", err)
	}
	knownGood, err := os.ReadFile(filepath.Join(cfg.ManifestCacheDir, "index-v2.json"))
	if err != nil {
		t.Fatalf("read last-known-good: %v", err)
	}
	manifest, err := Parse(manifestBody)
	if err != nil {
		t.Fatalf("parse manager fixture: %v", err)
	}
	manifest.CatalogSequence = 2
	rollbackBody, _ := json.Marshal(manifest)
	manifestBody = rollbackBody
	if _, err := manager.FetchAndActivate(context.Background()); !errors.Is(err, v2store.ErrManifestSequenceRollback) {
		t.Fatalf("rollback update error = %v, want sequence rollback", err)
	}
	unchanged, err := os.ReadFile(filepath.Join(cfg.ManifestCacheDir, "index-v2.json"))
	if err != nil {
		t.Fatalf("read unchanged last-known-good: %v", err)
	}
	if !bytes.Equal(knownGood, unchanged) {
		t.Fatal("invalid manifest update replaced last-known-good cache")
	}
}

func TestManagerManifestConflictRollsBackAtomicBundle(t *testing.T) {
	fixture := newManifestManagerTest(t)
	ctx := context.Background()
	oldReleaseID := "rel_" + strings.Repeat("a", 20)
	newReleaseID := "rel_" + strings.Repeat("b", 20)
	oldPackage := managerTestPackage(t, "artifact-a", oldReleaseID, "stable")
	firstBody := managerTestManifestBody(t, 1, oldPackage)
	if _, err := fixture.manager.ValidateAndActivate(ctx, firstBody); err != nil {
		t.Fatalf("activate first manager manifest: %v", err)
	}
	firstHash, firstCanonical, err := HashJSON(firstBody)
	if err != nil {
		t.Fatalf("hash first manager manifest: %v", err)
	}
	knownGood, err := os.ReadFile(filepath.Join(fixture.cfg.ManifestCacheDir, "index-v2.json"))
	if err != nil {
		t.Fatalf("read first manager LKG: %v", err)
	}
	if !bytes.Equal(knownGood, firstCanonical) {
		t.Fatal("first manager LKG differs from canonical manifest")
	}
	oldCatalog, err := fixture.repo.GetActiveManifestCatalog(ctx)
	if err != nil {
		t.Fatalf("read first active catalog: %v", err)
	}
	oldRelease := readManagerReleaseSnapshot(t, fixture, oldReleaseID)

	newPackage := managerTestPackage(t, "artifact-b", newReleaseID, "stable")
	conflictingPackage := managerTestPackage(t, "artifact-c", oldReleaseID, "stable")
	secondBody := managerTestManifestBody(t, 2, newPackage, conflictingPackage)
	if _, err := fixture.manager.ValidateAndActivate(ctx, secondBody); !errors.Is(err, v2store.ErrPackageReleaseArtifactConflict) {
		t.Fatalf("manager historical release conflict = %v, want sentinel", err)
	}
	active, err := fixture.repo.GetActiveManifestCatalog(ctx)
	if err != nil {
		t.Fatalf("read active catalog after manager conflict: %v", err)
	}
	if active.ManifestID != oldCatalog.ManifestID || active.CatalogID != oldCatalog.CatalogID ||
		active.CatalogSequence != oldCatalog.CatalogSequence || active.ManifestHash != firstHash || active.State != "active" {
		t.Fatalf("active catalog after manager conflict = %+v, want first catalog fields", active)
	}
	assertManagerCatalogState(t, fixture, oldCatalog.ManifestID, "active", 1)
	assertManagerRowCount(t, fixture, "SELECT COUNT(*) FROM manifest_catalogs WHERE catalog_sequence = 2", 0, "second catalog")
	assertManagerRowCount(t, fixture, "SELECT COUNT(*) FROM source_artifacts WHERE artifact_id = ?", 0, "second artifact", "artifact-b")
	assertManagerRowCount(t, fixture, "SELECT COUNT(*) FROM source_artifacts WHERE artifact_id = ?", 0, "conflicting artifact", "artifact-c")
	assertManagerRowCount(t, fixture, "SELECT COUNT(*) FROM source_package_releases WHERE package_release_id = ?", 0, "new release", newReleaseID)
	if got := readManagerReleaseSnapshot(t, fixture, oldReleaseID); got != oldRelease {
		t.Fatalf("historical release after manager conflict = %+v, want %+v", got, oldRelease)
	}
	unchanged, err := os.ReadFile(filepath.Join(fixture.cfg.ManifestCacheDir, "index-v2.json"))
	if err != nil {
		t.Fatalf("read manager LKG after conflict: %v", err)
	}
	if !bytes.Equal(unchanged, knownGood) {
		t.Fatal("manager conflict replaced last-known-good cache")
	}
	if _, err := fixture.manager.ValidateAndActivate(ctx, secondBody); !errors.Is(err, v2store.ErrPackageReleaseArtifactConflict) {
		t.Fatalf("manager conflict retry = %v, want sentinel", err)
	}
	activeAfterRetry, err := fixture.repo.GetActiveManifestCatalog(ctx)
	if err != nil {
		t.Fatalf("read active catalog after manager conflict retry: %v", err)
	}
	if activeAfterRetry.ManifestHash != firstHash || activeAfterRetry.CatalogSequence != oldCatalog.CatalogSequence {
		t.Fatalf("active catalog changed after manager conflict retry: %+v", activeAfterRetry)
	}
}

func TestManagerManifestSuccessfulUpgradeUsesAtomicBundle(t *testing.T) {
	fixture := newManifestManagerTest(t)
	ctx := context.Background()
	firstPackage := managerTestPackage(t, "artifact-a", "rel_"+strings.Repeat("a", 20), "stable")
	firstBody := managerTestManifestBody(t, 1, firstPackage)
	if _, err := fixture.manager.ValidateAndActivate(ctx, firstBody); err != nil {
		t.Fatalf("activate first upgrade manifest: %v", err)
	}
	secondPackage := managerTestPackage(t, "artifact-b", "rel_"+strings.Repeat("b", 20), "stable")
	thirdPackage := managerTestPackage(t, "artifact-c", "rel_"+strings.Repeat("c", 20), "candidate")
	secondBody := managerTestManifestBody(t, 2, secondPackage, thirdPackage)
	secondHash, secondCanonical, err := HashJSON(secondBody)
	if err != nil {
		t.Fatalf("hash successful upgrade manifest: %v", err)
	}
	if _, err := fixture.manager.ValidateAndActivate(ctx, secondBody); err != nil {
		t.Fatalf("activate successful upgrade manifest: %v", err)
	}
	active, err := fixture.repo.GetActiveManifestCatalog(ctx)
	if err != nil {
		t.Fatalf("read active catalog after successful upgrade: %v", err)
	}
	if active.CatalogSequence != 2 || active.ManifestHash != secondHash || active.State != "active" {
		t.Fatalf("active catalog after successful upgrade = %+v", active)
	}
	assertManagerCatalogState(t, fixture, active.ManifestID, "active", 1)
	assertManagerRowCount(t, fixture, "SELECT COUNT(*) FROM manifest_catalogs WHERE catalog_sequence = 1 AND state = 'superseded'", 1, "superseded first catalog")
	for _, artifactID := range []string{"artifact-b", "artifact-c"} {
		assertManagerRowCount(t, fixture, "SELECT COUNT(*) FROM source_artifacts WHERE artifact_id = ? AND catalog_id = ? AND managed_state = 'active'", 1, "upgraded artifact", artifactID, CatalogID)
	}
	for _, releaseID := range []string{"rel_" + strings.Repeat("b", 20), "rel_" + strings.Repeat("c", 20)} {
		assertManagerRowCount(t, fixture, "SELECT COUNT(*) FROM source_package_releases WHERE package_release_id = ? AND catalog_sequence = ?", 1, "upgraded release", releaseID, 2)
	}
	knownGood, err := os.ReadFile(filepath.Join(fixture.cfg.ManifestCacheDir, "index-v2.json"))
	if err != nil {
		t.Fatalf("read successful upgrade LKG: %v", err)
	}
	if !bytes.Equal(knownGood, secondCanonical) {
		t.Fatal("successful upgrade LKG differs from canonical manifest")
	}
}

type manifestManagerTestFixture struct {
	manager *Manager
	repo    *v2store.Repository
	db      *v2store.DB
	cfg     v2config.Config
}

func newManifestManagerTest(t *testing.T) *manifestManagerTestFixture {
	t.Helper()
	root := bytes.Repeat([]byte{0x52}, v2crypto.RootSecretSize)
	keys, err := v2crypto.DeriveKeys(root)
	if err != nil {
		t.Fatalf("derive manager test keys: %v", err)
	}
	db, err := v2store.Open(filepath.Join(t.TempDir(), "manager.db"))
	if err != nil {
		t.Fatalf("open manager test database: %v", err)
	}
	repo := v2store.NewRepository(db, keys)
	cfg := v2config.Default()
	cfg.RootSecret = root
	cfg.ManifestURL = "https://example.test/index-v2.json"
	cfg.ManifestCacheDir = filepath.Join(t.TempDir(), "cache")
	manager := NewManager(cfg, repo, nil)
	fixture := &manifestManagerTestFixture{manager: manager, repo: repo, db: db, cfg: cfg}
	t.Cleanup(func() { _ = db.Close() })
	return fixture
}

func managerTestPackage(t *testing.T, artifactID, packageReleaseID, status string) SourcePackage {
	t.Helper()
	manifest, err := Parse(currentIndexV2(t))
	if err != nil {
		t.Fatalf("parse manager package fixture: %v", err)
	}
	pkg := manifest.SourcePackages[0]
	pkg.ArtifactID = artifactID
	pkg.SourceKey = "shared"
	pkg.ManagedState = "active"
	pkg.PackageReleaseID = packageReleaseID
	pkg.Status = status
	return pkg
}

func managerTestManifestBody(t *testing.T, sequence int64, packages ...SourcePackage) []byte {
	t.Helper()
	manifest, err := Parse(currentIndexV2(t))
	if err != nil {
		t.Fatalf("parse manager manifest fixture: %v", err)
	}
	manifest.CatalogSequence = sequence
	manifest.SourcePackages = packages
	body, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manager manifest fixture: %v", err)
	}
	return body
}

type managerReleaseSnapshot struct {
	ArtifactID      string
	CatalogID       string
	CatalogSequence int64
	CoreHash        string
	PackageJSON     string
	State           string
	Revision        int64
}

func readManagerReleaseSnapshot(t *testing.T, fixture *manifestManagerTestFixture, packageReleaseID string) managerReleaseSnapshot {
	t.Helper()
	var snapshot managerReleaseSnapshot
	err := fixture.db.SQL().QueryRow(`
		SELECT artifact_id, catalog_id, catalog_sequence, core_hash, package_json, state, revision
		FROM source_package_releases WHERE package_release_id = ?`, packageReleaseID).Scan(
		&snapshot.ArtifactID, &snapshot.CatalogID, &snapshot.CatalogSequence,
		&snapshot.CoreHash, &snapshot.PackageJSON, &snapshot.State, &snapshot.Revision)
	if err != nil {
		t.Fatalf("read manager release %s: %v", packageReleaseID, err)
	}
	return snapshot
}

func assertManagerCatalogState(t *testing.T, fixture *manifestManagerTestFixture, manifestID, state string, revision int64) {
	t.Helper()
	var gotState string
	var gotRevision int64
	err := fixture.db.SQL().QueryRow("SELECT state, revision FROM manifest_catalogs WHERE manifest_id = ?", manifestID).Scan(&gotState, &gotRevision)
	if err != nil {
		t.Fatalf("read manager catalog %s: %v", manifestID, err)
	}
	if gotState != state || gotRevision != revision {
		t.Fatalf("manager catalog %s = %s/%d, want %s/%d", manifestID, gotState, gotRevision, state, revision)
	}
}

func assertManagerRowCount(t *testing.T, fixture *manifestManagerTestFixture, query string, want int, name string, args ...any) {
	t.Helper()
	var got int
	if err := fixture.db.SQL().QueryRow(query, args...).Scan(&got); err != nil {
		t.Fatalf("count %s: %v", name, err)
	}
	if got != want {
		t.Fatalf("count %s = %d, want %d", name, got, want)
	}
}
