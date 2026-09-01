package v2store

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestManifestActivationSequenceAndReplay(t *testing.T) {
	test := newTestRepository(t)
	firstInput := ManifestCatalogInput{
		ManifestID: "manifest-1", CatalogID: "catalog-test", ManifestURL: "https://example.test/index-v2.json",
		CatalogSequence: 1, ManifestHash: "hash-1", ManifestJSON: `{"catalogId":"catalog-test","catalogSequence":1}`,
	}
	first, err := test.repo.ActivateManifestCatalog(context.Background(), firstInput)
	if err != nil {
		t.Fatalf("activate first manifest: %v", err)
	}
	replay, err := test.repo.ActivateManifestCatalog(context.Background(), firstInput)
	if err != nil {
		t.Fatalf("replay first manifest: %v", err)
	}
	if replay.ManifestID != first.ManifestID || replay.CatalogSequence != first.CatalogSequence {
		t.Fatalf("manifest replay changed result: first=%+v replay=%+v", first, replay)
	}
	newKey := firstInput
	newKey.IdempotencyKey = "different-key"
	if _, err := test.repo.ActivateManifestCatalog(context.Background(), newKey); !errors.Is(err, ErrManifestAlreadyActive) {
		t.Fatalf("active manifest with new key error = %v, want already active", err)
	}
	rollback := firstInput
	rollback.ManifestID = "manifest-0"
	rollback.ManifestHash = "hash-0"
	rollback.CatalogSequence = 0
	rollback.IdempotencyKey = "manifest-rollback"
	if _, err := test.repo.ActivateManifestCatalog(context.Background(), rollback); !errors.Is(err, ErrManifestSequenceRollback) {
		t.Fatalf("manifest rollback error = %v, want sequence rollback", err)
	}
	second := ManifestCatalogInput{
		ManifestID: "manifest-2", CatalogID: "catalog-test", ManifestURL: firstInput.ManifestURL,
		CatalogSequence: 2, ManifestHash: "hash-2", ManifestJSON: `{"catalogId":"catalog-test","catalogSequence":2}`,
		IdempotencyKey: "manifest-2",
	}
	if _, err := test.repo.ActivateManifestCatalog(context.Background(), second); err != nil {
		t.Fatalf("activate second manifest: %v", err)
	}
	var activeCount, supersededCount int
	if err := test.db.SQL().QueryRow("SELECT COUNT(*) FROM manifest_catalogs WHERE state = 'active'").Scan(&activeCount); err != nil {
		t.Fatalf("count active manifests: %v", err)
	}
	if err := test.db.SQL().QueryRow("SELECT COUNT(*) FROM manifest_catalogs WHERE state = 'superseded'").Scan(&supersededCount); err != nil {
		t.Fatalf("count superseded manifests: %v", err)
	}
	if activeCount != 1 || supersededCount != 1 {
		t.Fatalf("manifest states active/superseded = %d/%d, want 1/1", activeCount, supersededCount)
	}
}

func TestSourcePackageReleaseRejectsCrossArtifactReuse(t *testing.T) {
	test := newTestRepository(t)
	ctx := context.Background()
	if err := test.repo.UpsertSourceArtifact(ctx, SourceArtifactInput{
		ArtifactID: "artifact-other", SourceKey: "manwa", CatalogID: "catalog-test", ManagedState: "active",
	}); err != nil {
		t.Fatalf("seed second artifact: %v", err)
	}
	originalInput := SourcePackageReleaseInput{
		PackageReleaseID: "release-manwa-1", ArtifactID: "artifact-manwa", CatalogID: "catalog-test",
		CatalogSequence: 1, CoreHash: "core-hash", ScanningExtensionHash: "scan-hash",
		ObservationContractID: "manwa-observation-v1", AccountObservationContractID: "manwa-account-observation-v1",
		AccountProbeContractID: "manwa-account-probe-v1", MarkerSchemesJSON: "[\"manwa:level:2\"]",
		SessionExportProfileID: "manwa-cookie-v1", PackageJSON: "{}", State: "active",
	}
	secondActiveInput := SourcePackageReleaseInput{
		PackageReleaseID: "release-other-1", ArtifactID: "artifact-other", CatalogID: "catalog-test",
		CatalogSequence: 1, CoreHash: "other-core-hash", ScanningExtensionHash: "other-scan-hash",
		ObservationContractID: "other-observation-v1", AccountObservationContractID: "other-account-observation-v1",
		AccountProbeContractID: "other-account-probe-v1", MarkerSchemesJSON: "[\"other-marker\"]",
		SessionExportProfileID: "other-cookie-v1", PackageJSON: "{\"artifact\":\"other\"}", State: "active",
	}
	if err := test.repo.UpsertSourcePackageRelease(ctx, secondActiveInput); err != nil {
		t.Fatalf("seed second active release: %v", err)
	}

	type releaseSnapshot struct {
		ArtifactID      string
		CatalogID       string
		CatalogSequence int64
		CoreHash        string
		PackageJSON     string
		State           string
		Revision        int64
	}
	readRelease := func(packageReleaseID string) releaseSnapshot {
		var snapshot releaseSnapshot
		err := test.db.SQL().QueryRow(`
			SELECT artifact_id, catalog_id, catalog_sequence, core_hash, package_json, state, revision
			FROM source_package_releases WHERE package_release_id = ?`, packageReleaseID).Scan(
			&snapshot.ArtifactID, &snapshot.CatalogID, &snapshot.CatalogSequence,
			&snapshot.CoreHash, &snapshot.PackageJSON, &snapshot.State, &snapshot.Revision)
		if err != nil {
			t.Fatalf("read release %s: %v", packageReleaseID, err)
		}
		return snapshot
	}

	originalBefore := readRelease(originalInput.PackageReleaseID)
	secondBefore := readRelease(secondActiveInput.PackageReleaseID)
	conflictInput := originalInput
	conflictInput.ArtifactID = secondActiveInput.ArtifactID
	conflictInput.CatalogSequence = 2
	conflictInput.CoreHash = "conflicting-core-hash"
	conflictInput.PackageJSON = "{\"artifact\":\"conflicting\"}"
	if err := test.repo.UpsertSourcePackageRelease(ctx, conflictInput); !errors.Is(err, ErrPackageReleaseArtifactConflict) {
		t.Fatalf("cross-artifact release error = %v, want sentinel", err)
	}
	if got := readRelease(originalInput.PackageReleaseID); got != originalBefore {
		t.Fatalf("original release changed after conflict: before=%+v after=%+v", originalBefore, got)
	}
	if got := readRelease(secondActiveInput.PackageReleaseID); got != secondBefore {
		t.Fatalf("second active release changed after conflict: before=%+v after=%+v", secondBefore, got)
	}

	if err := test.repo.UpsertSourcePackageRelease(ctx, originalInput); err != nil {
		t.Fatalf("same-artifact release replay: %v", err)
	}
	replayed := readRelease(originalInput.PackageReleaseID)
	if replayed.ArtifactID != originalInput.ArtifactID || replayed.Revision != originalBefore.Revision+1 {
		t.Fatalf("same-artifact replay result = %+v, want artifact %q and revision %d", replayed, originalInput.ArtifactID, originalBefore.Revision+1)
	}
}

func TestActivateManifestBundleSuccessAndReplay(t *testing.T) {
	test := newTestRepository(t)
	ctx := context.Background()
	artifacts := []SourceArtifactInput{
		{ArtifactID: "bundle-artifact-b", SourceKey: "shared", CatalogID: "catalog-test", ManagedState: "active"},
		{ArtifactID: "bundle-artifact-a", SourceKey: "shared", CatalogID: "catalog-test", ManagedState: "active"},
	}
	releases := []SourcePackageReleaseInput{
		bundleRelease("bundle-release-b", "bundle-artifact-b", 1, "active"),
		bundleRelease("bundle-release-a", "bundle-artifact-a", 1, "active"),
	}
	bundle := ManifestBundleInput{
		Catalog: ManifestCatalogInput{
			ManifestID: "bundle-manifest-1", CatalogID: "catalog-test", ManifestURL: "https://example.test/index-v2.json",
			CatalogSequence: 1, ManifestHash: "bundle-hash-1", ManifestJSON: `{}`,
			IdempotencyKey: "bundle-success-1",
		},
		Artifacts: artifacts,
		Releases:  releases,
	}
	originalArtifacts := append([]SourceArtifactInput(nil), bundle.Artifacts...)
	originalReleases := append([]SourcePackageReleaseInput(nil), bundle.Releases...)
	result, err := test.repo.ActivateManifestBundle(ctx, bundle)
	if err != nil {
		t.Fatalf("activate manifest bundle: %v", err)
	}
	if !reflect.DeepEqual(bundle.Artifacts, originalArtifacts) || !reflect.DeepEqual(bundle.Releases, originalReleases) {
		t.Fatal("bundle input slices were reordered by activation")
	}
	active, err := test.repo.GetActiveManifestCatalog(ctx)
	if err != nil {
		t.Fatalf("read active bundle catalog: %v", err)
	}
	if active.ManifestID != result.ManifestID || active.CatalogID != result.CatalogID ||
		active.CatalogSequence != result.CatalogSequence || active.ManifestHash != result.ManifestHash ||
		active.ManifestJSON != result.ManifestJSON || active.State != "active" {
		t.Fatalf("active bundle catalog = %+v, want fields from %+v", active, result)
	}
	var catalogRevision int64
	if err := test.db.SQL().QueryRow("SELECT revision FROM manifest_catalogs WHERE manifest_id = ?", result.ManifestID).Scan(&catalogRevision); err != nil {
		t.Fatalf("read bundle catalog revision: %v", err)
	}
	if catalogRevision != int64(result.Revision) {
		t.Fatalf("bundle catalog revision = %d, want %d", catalogRevision, result.Revision)
	}
	for _, artifactID := range []string{"bundle-artifact-a", "bundle-artifact-b"} {
		var sourceKey, catalogID, managedState string
		var revision int64
		if err := test.db.SQL().QueryRow(`
			SELECT source_key, catalog_id, managed_state, revision
			FROM source_artifacts WHERE artifact_id = ?`, artifactID).Scan(
			&sourceKey, &catalogID, &managedState, &revision); err != nil {
			t.Fatalf("read bundle artifact %s: %v", artifactID, err)
		}
		if sourceKey != "shared" || catalogID != "catalog-test" || managedState != "active" || revision != 1 {
			t.Fatalf("bundle artifact %s = %s/%s/%s/%d", artifactID, sourceKey, catalogID, managedState, revision)
		}
	}
	for _, releaseID := range []string{"bundle-release-a", "bundle-release-b"} {
		snapshot := readSourcePackageReleaseSnapshot(t, test, releaseID)
		if snapshot.State != "active" || snapshot.Revision != 1 {
			t.Fatalf("bundle release %s = %+v", releaseID, snapshot)
		}
	}

	artifactRevisionsBefore := map[string]int64{}
	for _, artifactID := range []string{"bundle-artifact-a", "bundle-artifact-b"} {
		var revision int64
		if err := test.db.SQL().QueryRow("SELECT revision FROM source_artifacts WHERE artifact_id = ?", artifactID).Scan(&revision); err != nil {
			t.Fatalf("read artifact revision %s: %v", artifactID, err)
		}
		artifactRevisionsBefore[artifactID] = revision
	}
	releaseRevisionsBefore := map[string]int64{}
	for _, releaseID := range []string{"bundle-release-a", "bundle-release-b"} {
		releaseRevisionsBefore[releaseID] = readSourcePackageReleaseSnapshot(t, test, releaseID).Revision
	}
	replayed, err := test.repo.ActivateManifestBundle(ctx, bundle)
	if err != nil {
		t.Fatalf("replay manifest bundle: %v", err)
	}
	if replayed != result {
		t.Fatalf("bundle replay = %+v, want %+v", replayed, result)
	}
	for artifactID, revision := range artifactRevisionsBefore {
		var got int64
		if err := test.db.SQL().QueryRow("SELECT revision FROM source_artifacts WHERE artifact_id = ?", artifactID).Scan(&got); err != nil {
			t.Fatalf("read replay artifact revision %s: %v", artifactID, err)
		}
		if got != revision {
			t.Fatalf("replay changed artifact %s revision from %d to %d", artifactID, revision, got)
		}
	}
	for releaseID, revision := range releaseRevisionsBefore {
		got := readSourcePackageReleaseSnapshot(t, test, releaseID).Revision
		if got != revision {
			t.Fatalf("replay changed release %s revision from %d to %d", releaseID, revision, got)
		}
	}
}

func TestActivateManifestBundleConflictRollsBackEverything(t *testing.T) {
	test := newTestRepository(t)
	ctx := context.Background()
	oldCatalogInput := ManifestCatalogInput{
		ManifestID: "old-manifest", CatalogID: "catalog-test", ManifestURL: "https://example.test/index-v2.json",
		CatalogSequence: 1, ManifestHash: "old-hash", ManifestJSON: `{}`, IdempotencyKey: "old-catalog",
	}
	oldCatalog, err := test.repo.ActivateManifestCatalog(ctx, oldCatalogInput)
	if err != nil {
		t.Fatalf("activate old catalog: %v", err)
	}
	oldRelease := readSourcePackageReleaseSnapshot(t, test, "release-manwa-1")
	bundle := ManifestBundleInput{
		Catalog: ManifestCatalogInput{
			ManifestID: "new-manifest", CatalogID: "catalog-test", ManifestURL: oldCatalog.ManifestURL,
			CatalogSequence: 2, ManifestHash: "new-hash", ManifestJSON: `{}`, IdempotencyKey: "bundle-conflict",
		},
		Artifacts: []SourceArtifactInput{
			{ArtifactID: "artifact-c", SourceKey: "shared", CatalogID: "catalog-test", ManagedState: "active"},
			{ArtifactID: "artifact-b", SourceKey: "shared", CatalogID: "catalog-test", ManagedState: "active"},
		},
		Releases: []SourcePackageReleaseInput{
			bundleRelease("release-manwa-1", "artifact-c", 2, "active"),
			bundleRelease("release-a", "artifact-b", 2, "active"),
		},
	}
	if _, err := test.repo.ActivateManifestBundle(ctx, bundle); !errors.Is(err, ErrPackageReleaseArtifactConflict) {
		t.Fatalf("bundle conflict error = %v, want sentinel", err)
	}
	active, err := test.repo.GetActiveManifestCatalog(ctx)
	if err != nil {
		t.Fatalf("read catalog after bundle conflict: %v", err)
	}
	if active.ManifestID != oldCatalog.ManifestID || active.CatalogID != oldCatalog.CatalogID ||
		active.CatalogSequence != oldCatalog.CatalogSequence || active.ManifestHash != oldCatalog.ManifestHash ||
		active.ManifestJSON != oldCatalog.ManifestJSON || active.State != oldCatalog.State {
		t.Fatalf("active catalog after conflict = %+v, want fields from %+v", active, oldCatalog)
	}
	var catalogRevision int64
	if err := test.db.SQL().QueryRow("SELECT revision FROM manifest_catalogs WHERE manifest_id = ?", oldCatalog.ManifestID).Scan(&catalogRevision); err != nil {
		t.Fatalf("read old catalog revision after conflict: %v", err)
	}
	if catalogRevision != int64(oldCatalog.Revision) {
		t.Fatalf("old catalog revision after conflict = %d, want %d", catalogRevision, oldCatalog.Revision)
	}
	for _, query := range []struct {
		name  string
		query string
		arg   string
	}{
		{name: "new catalog", query: "SELECT COUNT(*) FROM manifest_catalogs WHERE manifest_id = ?", arg: "new-manifest"},
		{name: "artifact b", query: "SELECT COUNT(*) FROM source_artifacts WHERE artifact_id = ?", arg: "artifact-b"},
		{name: "artifact c", query: "SELECT COUNT(*) FROM source_artifacts WHERE artifact_id = ?", arg: "artifact-c"},
		{name: "release a", query: "SELECT COUNT(*) FROM source_package_releases WHERE package_release_id = ?", arg: "release-a"},
	} {
		var count int
		if err := test.db.SQL().QueryRow(query.query, query.arg).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", query.name, err)
		}
		if count != 0 {
			t.Fatalf("%s has %d rows after conflict", query.name, count)
		}
	}
	if got := readSourcePackageReleaseSnapshot(t, test, "release-manwa-1"); got != oldRelease {
		t.Fatalf("historical release after conflict = %+v, want %+v", got, oldRelease)
	}
	var idempotencyRows int
	if err := test.db.SQL().QueryRow(`
		SELECT COUNT(*) FROM idempotency_records
		WHERE route_key = ? AND state IN ('completed', 'running')`, "activate-manifest-bundle").Scan(&idempotencyRows); err != nil {
		t.Fatalf("count bundle idempotency rows: %v", err)
	}
	if idempotencyRows != 0 {
		t.Fatalf("bundle conflict left %d idempotency rows", idempotencyRows)
	}
}

func TestActivateManifestBundleMidTransactionFailureRollsBackEverything(t *testing.T) {
	test := newTestRepository(t)
	ctx := context.Background()
	bundle := ManifestBundleInput{
		Catalog: ManifestCatalogInput{
			ManifestID: "mid-failure-manifest", CatalogID: "catalog-test", ManifestURL: "https://example.test/index-v2.json",
			CatalogSequence: 1, ManifestHash: "mid-failure-hash", ManifestJSON: `{}`, IdempotencyKey: "bundle-mid-failure",
		},
		Artifacts: []SourceArtifactInput{
			{ArtifactID: "artifact-b", SourceKey: "shared", CatalogID: "catalog-test", ManagedState: "invalid"},
			{ArtifactID: "artifact-a", SourceKey: "shared", CatalogID: "catalog-test", ManagedState: "active"},
		},
		Releases: []SourcePackageReleaseInput{
			bundleRelease("release-b", "artifact-b", 1, "active"),
			bundleRelease("release-a", "artifact-a", 1, "active"),
		},
	}
	if _, err := test.repo.ActivateManifestBundle(ctx, bundle); err == nil {
		t.Fatal("mid-transaction bundle failure unexpectedly succeeded")
	}
	var count int
	if err := test.db.SQL().QueryRow("SELECT COUNT(*) FROM manifest_catalogs WHERE manifest_id = ?", "mid-failure-manifest").Scan(&count); err != nil {
		t.Fatalf("count failed catalog: %v", err)
	}
	if count != 0 {
		t.Fatalf("failed bundle left %d catalog rows", count)
	}
	for _, query := range []string{
		"SELECT COUNT(*) FROM source_artifacts WHERE artifact_id IN ('artifact-a', 'artifact-b')",
		"SELECT COUNT(*) FROM source_package_releases WHERE package_release_id IN ('release-a', 'release-b')",
	} {
		if err := test.db.SQL().QueryRow(query).Scan(&count); err != nil {
			t.Fatalf("count failed bundle rows: %v", err)
		}
		if count != 0 {
			t.Fatalf("failed bundle left %d rows for query %q", count, query)
		}
	}
	if err := test.db.SQL().QueryRow(`
		SELECT COUNT(*) FROM idempotency_records
		WHERE route_key = ? AND state IN ('completed', 'running')`, "activate-manifest-bundle").Scan(&count); err != nil {
		t.Fatalf("count failed bundle idempotency rows: %v", err)
	}
	if count != 0 {
		t.Fatalf("failed bundle left %d idempotency rows", count)
	}
}

func TestDirectReleaseUpsertMissingArtifactReturnsSentinel(t *testing.T) {
	test := newTestRepository(t)
	err := test.repo.UpsertSourcePackageRelease(context.Background(), SourcePackageReleaseInput{
		PackageReleaseID: "release-missing-artifact", ArtifactID: "artifact-missing", CatalogID: "catalog-test",
		CatalogSequence: 1, CoreHash: "missing-core", MarkerSchemesJSON: "[]", PackageJSON: `{}`, State: "active",
	})
	if !errors.Is(err, ErrArtifactNotFound) {
		t.Fatalf("missing artifact error = %v, want ErrArtifactNotFound", err)
	}
}

func bundleRelease(packageReleaseID, artifactID string, catalogSequence int64, state string) SourcePackageReleaseInput {
	return SourcePackageReleaseInput{
		PackageReleaseID: packageReleaseID, ArtifactID: artifactID, CatalogID: "catalog-test",
		CatalogSequence: catalogSequence, CoreHash: packageReleaseID + "-core",
		ScanningExtensionHash: packageReleaseID + "-scan", ObservationContractID: packageReleaseID + "-observation",
		AccountObservationContractID: packageReleaseID + "-account-observation", AccountProbeContractID: packageReleaseID + "-probe",
		MarkerSchemesJSON: "[]", SessionExportProfileID: packageReleaseID + "-profile", PackageJSON: `{}`, State: state,
	}
}

type sourcePackageReleaseSnapshot struct {
	ArtifactID      string
	CatalogID       string
	CatalogSequence int64
	CoreHash        string
	PackageJSON     string
	State           string
	Revision        int64
}

func readSourcePackageReleaseSnapshot(t *testing.T, test *testRepository, packageReleaseID string) sourcePackageReleaseSnapshot {
	t.Helper()
	var snapshot sourcePackageReleaseSnapshot
	err := test.db.SQL().QueryRow(`
		SELECT artifact_id, catalog_id, catalog_sequence, core_hash, package_json, state, revision
		FROM source_package_releases WHERE package_release_id = ?`, packageReleaseID).Scan(
		&snapshot.ArtifactID, &snapshot.CatalogID, &snapshot.CatalogSequence,
		&snapshot.CoreHash, &snapshot.PackageJSON, &snapshot.State, &snapshot.Revision)
	if err != nil {
		t.Fatalf("read package release %s: %v", packageReleaseID, err)
	}
	return snapshot
}
