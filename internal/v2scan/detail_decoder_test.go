package v2scan

import (
	"errors"
	"testing"
)

func TestDecodeDetailOutputAcceptsContractObservation(t *testing.T) {
	result, err := DecodeDetailOutput([]byte(`{
        "comicId":"comic-1",
        "summary":{"title":"Comic 1","cover":null,"subtitle":null,"chapterCount":2,"recentChapters":[]},
        "contentObservation":{"visibilityScope":"picacg:authenticated","updateTime":"2026-08-29T00:00:00Z","chapterCount":2,"recentChapters":[],"markerEvidence":null},
        "sessionPatch":null
    }`), "comic-1")
	if err != nil {
		t.Fatalf("decode detail output: %v", err)
	}
	if result.ComicID != "comic-1" || result.Digest == "" || len(result.Payload) == 0 {
		t.Fatalf("decoded detail result = %+v", result)
	}
	if err := ValidateDetailResult(result); err != nil {
		t.Fatalf("validate decoded detail result: %v", err)
	}
}

func TestDecodeDetailOutputRejectsIdentityOrEnvelopeDrift(t *testing.T) {
	tests := map[string]string{
		"wrong comic":         `{"comicId":"other","summary":null,"contentObservation":{"visibilityScope":"scope","markerEvidence":null},"sessionPatch":null}`,
		"missing observation": `{"comicId":"comic-1","summary":null,"contentObservation":null,"sessionPatch":null}`,
		"session patch":       `{"comicId":"comic-1","summary":null,"contentObservation":{"visibilityScope":"scope","markerEvidence":null},"sessionPatch":{"set":{}}}`,
		"trailing JSON":       `{"comicId":"comic-1","summary":null,"contentObservation":{"visibilityScope":"scope","markerEvidence":null},"sessionPatch":null}{}`,
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeDetailOutput([]byte(raw), "comic-1"); !errors.Is(err, ErrDetailBatchInvalid) {
				t.Fatalf("decode error = %v, want %v", err, ErrDetailBatchInvalid)
			}
		})
	}
}
