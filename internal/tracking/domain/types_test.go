package domain

import (
	"strings"
	"testing"
	"time"
)

func TestFullRevisionAcceptsNormativeSHA1AndSHA256Lengths(t *testing.T) {
	if !FullRevision(strings.Repeat("a", 40)) {
		t.Fatal("40-character lowercase revision was rejected")
	}
	if !FullRevision(strings.Repeat("b", 64)) {
		t.Fatal("64-character lowercase revision was rejected")
	}
}

func TestFullRevisionRejectsShortLongAndMixedCaseValues(t *testing.T) {
	for _, value := range []string{
		strings.Repeat("a", 39),
		strings.Repeat("a", 65),
		strings.Repeat("A", 40),
		strings.Repeat("a", 39) + "G",
		"HEAD",
		"main",
	} {
		if FullRevision(value) {
			t.Fatalf("invalid revision accepted: %q", value)
		}
	}
}

func TestAuthorityAndObservationUseTheSameRevisionSchema(t *testing.T) {
	revision := strings.Repeat("c", 64)
	artifact := ArtifactIdentity{SourceKey: "manwa", FileName: "manwa.js"}
	authority := Authority{
		CatalogID:      "owner/catalog",
		ActiveRevision: revision,
		Generation:     3,
		Artifacts:      []ArtifactIdentity{artifact},
	}
	if err := authority.Validate(); err != nil {
		t.Fatalf("64-character authority rejected: %v", err)
	}
	observation := Observation{
		Artifact:          artifact,
		Revision:          revision,
		ComicID:           "42",
		ObservedAt:        nowForDomainTest,
		ValidUntil:        nowForDomainTest.Add(24 * time.Hour),
		FavoriteUpdate:    FavoriteUpdate{Marker: stringPtr("marker")},
		RuntimeGeneration: 3,
	}
	if err := observation.Validate(nowForDomainTest.Add(time.Second)); err != nil {
		t.Fatalf("64-character observation rejected: %v", err)
	}
}

var nowForDomainTest = time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)

func stringPtr(value string) *string { return &value }
