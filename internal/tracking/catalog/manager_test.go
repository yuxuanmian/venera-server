package catalog

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const revisionA = "0123456789abcdef0123456789abcdef01234567"
const revisionB = "89abcdef0123456789abcdef0123456789abcdef"

func writeCheckout(t *testing.T, root, index string, files ...string) {
	t.Helper()
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "index.json"), []byte(index), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		path := filepath.Join(root, file)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("module.exports = {};"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestManagerActivatesOnlyValidatedPinnedArtifacts(t *testing.T) {
	root := t.TempDir()
	writeCheckout(t, root, `[
      {"key":"manwa","fileName":"manwa.js","version":"1.0.6",
       "cloudTracking":{"scanner":"extensions/server/manwa/scanning.js"}},
      {"key":"copy_manga","fileName":"copy_manga.js","version":"1.0.0"}
    ]`, "manwa.js", filepath.FromSlash("extensions/server/manwa/scanning.js"))
	m, err := NewManager(Config{
		CatalogID:        "yuxuanmian/venera-configs",
		Repository:       root,
		ObservationLimit: 10000,
		IndexMaxBytes:    1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.ActivatePath(context.Background(), revisionA, root); err != nil {
		t.Fatal(err)
	}
	authority, ok := m.Authority()
	if !ok {
		t.Fatal("authority was not activated")
	}
	if authority.ActiveRevision != revisionA || authority.Generation != 1 {
		t.Fatalf("unexpected authority: %+v", authority)
	}
	if len(authority.Artifacts) != 1 || authority.Artifacts[0].FileName != "manwa.js" {
		t.Fatalf("unexpected capabilities: %+v", authority.Artifacts)
	}
	if !m.HasArtifact(authority.Artifacts[0]) {
		t.Fatal("active artifact was not found")
	}
}

func TestManagerRejectsBadCandidateAndKeepsLastKnownGood(t *testing.T) {
	root := t.TempDir()
	writeCheckout(t, root, `[{"key":"manwa","fileName":"manwa.js","version":"1.0.0",
      "cloudTracking":{"scanner":"scanner.js"}}]`, "manwa.js", "scanner.js")
	m, err := NewManager(Config{CatalogID: "owner/catalog", Repository: root})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.ActivatePath(context.Background(), revisionA, root); err != nil {
		t.Fatal(err)
	}
	if err := m.Activate(context.Background(), "not-a-commit"); err == nil {
		t.Fatal("invalid revision was accepted")
	}
	authority, _ := m.Authority()
	if authority.ActiveRevision != revisionA || authority.Generation != 1 {
		t.Fatalf("last known good was lost: %+v", authority)
	}
	if err := m.ActivatePath(context.Background(), revisionB, root); err != nil {
		t.Fatal(err)
	}
	updated, _ := m.Authority()
	if updated.ActiveRevision != revisionB || updated.Generation != 2 {
		t.Fatalf("candidate was not activated: %+v", updated)
	}
}

func TestManagerAcceptsSHA256RevisionAndRejectsNonFullValues(t *testing.T) {
	root := t.TempDir()
	writeCheckout(t, root, `[{"key":"manwa","fileName":"manwa.js",
      "cloudTracking":{"scanner":"scanner.js"}}]`, "manwa.js", "scanner.js")
	revision64 := strings.Repeat("b", 64)
	if err := os.WriteFile(filepath.Join(root, ".venera-revision"), []byte(revision64+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := NewManager(Config{CatalogID: "owner/catalog", Repository: root})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.ActivatePath(context.Background(), revision64, root); err != nil {
		t.Fatalf("64-character revision was rejected: %v", err)
	}
	for _, revision := range []string{
		strings.Repeat("a", 39),
		strings.Repeat("A", 40),
		strings.Repeat("a", 65),
	} {
		if err := m.ActivatePath(context.Background(), revision, root); err == nil {
			t.Fatalf("invalid revision was accepted: %q", revision)
		}
	}
}

func TestSafeScannerPathRejectsTraversalAndMissingFiles(t *testing.T) {
	root := t.TempDir()
	if _, err := SafeScannerPath(root, "../scanner.js"); err == nil {
		t.Fatal("traversal path was accepted")
	}
	if _, err := SafeScannerPath(root, filepath.Join(root, "scanner.js")); err == nil {
		t.Fatal("absolute path was accepted")
	}
	if _, err := SafeScannerPath(root, "missing.js"); err == nil {
		t.Fatal("missing scanner was accepted")
	}
}

func TestManagerRejectsMismatchedMaterializedCheckoutIdentity(t *testing.T) {
	root := t.TempDir()
	writeCheckout(t, root, `[{"key":"manwa","fileName":"manwa.js",
      "cloudTracking":{"scanner":"scanner.js"}}]`, "manwa.js", "scanner.js")
	if err := os.WriteFile(filepath.Join(root, ".venera-revision"), []byte(revisionB+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := NewManager(Config{CatalogID: "owner/catalog", Repository: root})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.ActivatePath(context.Background(), revisionA, root); err == nil {
		t.Fatal("mismatched checkout identity was accepted")
	}
	if _, ok := m.Active(); ok {
		t.Fatal("failed checkout identity published an active snapshot")
	}
}

func TestManagerMaterializesExactGitRevisionAndIgnoresWorkingTreeTampering(t *testing.T) {
	repository := t.TempDir()
	index := `[{"key":"manwa","fileName":"manwa.js","version":"1.0.0",
      "cloudTracking":{"scanner":"scanner.js"}}]`
	writeCheckout(t, repository, index, "manwa.js", "scanner.js")
	if err := os.WriteFile(filepath.Join(repository, "manwa.js"), []byte("original"), 0o644); err != nil {
		t.Fatal(err)
	}
	revision := initGitRevision(t, repository)

	// The operator's checkout is deliberately changed after the commit. A
	// production activation must still use the immutable commit object.
	if err := os.WriteFile(filepath.Join(repository, "index.json"), []byte("not-json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repository, "manwa.js"), []byte("tampered"), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := NewManager(Config{
		CatalogID:  "owner/catalog",
		Repository: repository,
		Revision:   revision,
		CacheDir:   t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Activate(context.Background(), revision); err != nil {
		t.Fatal(err)
	}
	snapshot, ok := m.Active()
	if !ok {
		t.Fatal("missing active snapshot")
	}
	if snapshot.Root == repository {
		t.Fatal("working tree was used as the active checkout")
	}
	content, err := os.ReadFile(filepath.Join(snapshot.Root, "manwa.js"))
	if err != nil || string(content) != "original" {
		t.Fatalf("materialized content = %q, err=%v", content, err)
	}

	// A tampered cached checkout is reconstructed from the same Git object on
	// the next activation rather than becoming a new authority.
	if err := os.WriteFile(filepath.Join(snapshot.Root, "manwa.js"), []byte("cache-tampered"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := m.Activate(context.Background(), revision); err != nil {
		t.Fatal(err)
	}
	snapshot, _ = m.Active()
	content, err = os.ReadFile(filepath.Join(snapshot.Root, "manwa.js"))
	if err != nil || string(content) != "original" {
		t.Fatalf("re-materialized content = %q, err=%v", content, err)
	}
}

func initGitRevision(t *testing.T, directory string) string {
	t.Helper()
	for _, args := range [][]string{
		{"init", "-q", directory},
		{"-C", directory, "config", "user.email", "catalog-test@example.invalid"},
		{"-C", directory, "config", "user.name", "catalog-test"},
		{"-C", directory, "add", "."},
		{"-C", directory, "commit", "-q", "-m", "fixture"},
	} {
		if output, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("git %v failed: %v (%s)", args, err, strings.TrimSpace(string(output)))
		}
	}
	output, err := exec.Command("git", "-C", directory, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatalf("resolve git revision: %v", err)
	}
	return strings.TrimSpace(string(output))
}
