package trackingtest

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestCanonicalFixtureVersionAndChecksum(t *testing.T) {
	data, err := LoadFixtureBytes()
	if err != nil {
		t.Fatalf("load fixture bytes: %v", err)
	}
	fixture, err := LoadFixture()
	if err != nil {
		t.Fatalf("load fixture: %v", err)
	}

	if got := fixture["fixtureVersion"]; got != FixtureVersion {
		t.Fatalf("fixture version = %v, want %s", got, FixtureVersion)
	}
	if got := fixture["contractVersion"]; got != "1.0.0" {
		t.Fatalf("contract version = %v, want 1.0.0", got)
	}
	digest := sha256.Sum256(data)
	if got := hex.EncodeToString(digest[:]); got !=
		"4cd03455994582d6c400fc8b32e8c6721869680a50b66644a176919086fc4db8" {
		t.Fatalf("fixture checksum = %s", got)
	}

	for _, key := range []string{"comparisonCases", "presentationCases"} {
		cases, ok := fixture[key].([]any)
		if !ok {
			t.Fatalf("%s is %T, want array", key, fixture[key])
		}
		want := 20
		if key == "presentationCases" {
			want = 24
		}
		if len(cases) != want {
			t.Fatalf("%s length = %d, want %d", key, len(cases), want)
		}
	}
}
