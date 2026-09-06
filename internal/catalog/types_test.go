package catalog

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestCatalogContractFixtureAndPointerValidation(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "catalog-contract.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Pointers   map[string]CatalogPointer `json:"pointers"`
		IndexCases []struct {
			ID    string          `json:"id"`
			Valid bool            `json:"valid"`
			Value json.RawMessage `json:"value"`
		} `json:"indexCases"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	for name, pointer := range fixture.Pointers {
		if err := ValidatePointer(pointer); err != nil {
			t.Fatalf("pointer %s: %v", name, err)
		}
	}
	for _, item := range fixture.IndexCases {
		index, err := ParseIndex(item.Value)
		if (err == nil) != item.Valid {
			t.Fatalf("index case %s valid=%v err=%v", item.ID, item.Valid, err)
		}
		if item.Valid && index == nil {
			t.Fatalf("valid index case %s returned nil", item.ID)
		}
	}
}

func TestBuildPinnedPointerPreservesSourceKeyCase(t *testing.T) {
	ref, err := ParseConfiguredURL("https://raw.githubusercontent.com/YuXuanMian/Venera-Configs/yxm/index.json")
	if err != nil {
		t.Fatal(err)
	}
	pointer, err := BuildPinnedPointer(ref, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatal(err)
	}
	if pointer.CatalogID != "yuxuanmian/venera-configs" || pointer.Revision == "" {
		t.Fatalf("pointer = %#v", pointer)
	}
	if _, err := PinnedSourceURL(pointer, "../escape.js"); err == nil {
		t.Fatal("path traversal should fail")
	}
}
