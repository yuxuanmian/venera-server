package scan

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"venera-server/internal/tracking/domain"
)

func TestDecodeSnapshotSliceAcceptsStateMarkerAndUnknownFacts(t *testing.T) {
	raw := []byte(`{
		"expectedTotal": 3,
		"items": [
			{"comicId":"1","favoriteUpdate":{"state":{"latestChapterId":"chapter-1"},"sourceUnread":true}},
			{"comicId":"2","favoriteUpdate":{"marker":"opaque-full","sourceUnread":false,"metadata":{"fullLatestChapterId":"full-2"}}},
			{"comicId":"3","favoriteUpdate":{}}
		],
		"complete": true,
		"checkpoint": null,
		"sessionPatch": null
	}`)
	result, err := DecodeSnapshotSlice(raw)
	if err != nil {
		t.Fatal(err)
	}
	if result.ExpectedTotal == nil || *result.ExpectedTotal != 3 || !result.Complete || len(result.Items) != 3 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if result.Items[0].FavoriteUpdate.State == nil || result.Items[1].FavoriteUpdate.Marker == nil ||
		result.Items[2].FavoriteUpdate.State != nil || result.Items[2].FavoriteUpdate.Marker != nil ||
		result.Items[2].FavoriteUpdate.SourceUnread != nil || result.Items[2].FavoriteUpdate.Metadata != nil {
		t.Fatalf("unexpected update projections: %+v", result.Items)
	}
	for _, item := range result.Items {
		if !json.Valid(item.ItemJSON) || item.ItemDigest == "" {
			t.Fatalf("item was not canonicalized: %+v", item)
		}
	}
}

func TestDecodeSnapshotSliceRequiresCompleteContractAndRejectsDuplicates(t *testing.T) {
	base := `{"expectedTotal":1,"items":[{"comicId":"1","favoriteUpdate":{}}],"complete":true,"checkpoint":null}`
	for name, raw := range map[string]string{
		"unknown-field":                 `{"expectedTotal":1,"items":[],"complete":true,"checkpoint":null,"legacy":true}`,
		"duplicate":                     `{"expectedTotal":2,"items":[{"comicId":"1","favoriteUpdate":{}},{"comicId":"1","favoriteUpdate":{}}],"complete":true,"checkpoint":null}`,
		"incomplete-without-checkpoint": `{"expectedTotal":1,"items":[],"complete":false,"checkpoint":null}`,
		"missing-update":                `{"expectedTotal":1,"items":[{"comicId":"1"}],"complete":true,"checkpoint":null}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := DecodeSnapshotSlice([]byte(raw))
			if err == nil {
				t.Fatal("invalid snapshot was accepted")
			}
		})
	}
	result, err := DecodeSnapshotSlice([]byte(base))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 1 {
		t.Fatalf("unexpected item count: %d", len(result.Items))
	}
	if strings.TrimSpace(string(result.Checkpoint)) != "" {
		t.Fatal("unexpected sentinel behavior")
	}
}

func TestDecodeSnapshotSliceEnforcesSharedBounds(t *testing.T) {
	largeMarker := strings.Repeat("多", domain.MaxMarkerBytes)
	raw := `{"expectedTotal":1,"items":[{"comicId":"1","favoriteUpdate":{"marker":"` + largeMarker + `"}}],"complete":true,"checkpoint":null}`
	if _, err := DecodeSnapshotSlice([]byte(raw)); err == nil {
		t.Fatal("oversized UTF-8 marker was accepted")
	}
	tooMany := `{"expectedTotal":2,"items":[{"comicId":"1","favoriteUpdate":{}},{"comicId":"2","favoriteUpdate":{}}],"complete":true,"checkpoint":null}`
	if _, err := DecodeSnapshotSliceWithLimits([]byte(tooMany), 1, 128); !errors.Is(err, ErrSnapshotItemLimit) {
		t.Fatalf("item limit error = %v", err)
	}
}
