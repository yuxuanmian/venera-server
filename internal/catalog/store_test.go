package catalog

import (
	"os"
	"path/filepath"
	"testing"
)

func testPointer() CatalogPointer {
	return CatalogPointer{CatalogID: "owner/repo", Revision: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", IndexURL: "https://raw.githubusercontent.com/owner/repo/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/index.json"}
}

func TestStoreSnapshotAndStateRoundTrip(t *testing.T) {
	store := NewStore(t.TempDir())
	pointer := testPointer()
	raw := []byte("[]\n")
	if err := store.SaveCheckedSnapshot(pointer, raw, CatalogIndex{}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.ReadSnapshot(pointer)
	if err != nil {
		t.Fatal(err)
	}
	if string(snapshot.IndexRaw) != string(raw) || len(snapshot.Index) != 0 {
		t.Fatalf("snapshot=%#v", snapshot)
	}
	state := ServerCatalogState{SchemaVersion: 1, Active: &PublishedCatalog{CatalogPointer: pointer, ActivatedAt: utcNow(), SourceCount: 0}}
	if err := store.WriteState(state); err != nil {
		t.Fatal(err)
	}
	readState, err := store.ReadState()
	if err != nil || readState.Active == nil || !SameIdentity(readState.Active.CatalogPointer, pointer) {
		t.Fatalf("state=%#v err=%v", readState, err)
	}
}

func TestStoreRejectsCorruptSnapshotAndKeepsStateBytes(t *testing.T) {
	store := NewStore(t.TempDir())
	pointer := testPointer()
	if err := store.SaveCheckedSnapshot(pointer, []byte("[]"), CatalogIndex{}); err != nil {
		t.Fatal(err)
	}
	if err := store.WriteState(ServerCatalogState{SchemaVersion: 1}); err != nil {
		t.Fatal(err)
	}
	statePath := store.StatePath()
	original, _ := os.ReadFile(statePath)
	if err := os.WriteFile(filepath.Join(store.SnapshotPath(pointer), "index.json"), []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadSnapshot(pointer); err == nil {
		t.Fatal("corrupt snapshot should fail")
	}
	current, _ := os.ReadFile(statePath)
	if string(current) != string(original) {
		t.Fatal("state changed while reading corrupt snapshot")
	}
}
