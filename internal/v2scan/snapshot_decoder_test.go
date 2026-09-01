package v2scan

import (
	"errors"
	"testing"
)

func TestDecodeSnapshotSliceAcceptsEmptyCompleteResult(t *testing.T) {
	result, err := DecodeSnapshotSlice([]byte(`{"expectedTotal":0,"items":[],"complete":true,"checkpoint":null,"sessionPatch":null}`))
	if err != nil {
		t.Fatalf("decode empty complete result: %v", err)
	}
	if result.ExpectedTotal == nil || *result.ExpectedTotal != 0 || !result.Complete || len(result.Items) != 0 || len(result.Checkpoint) != 4 || string(result.Checkpoint) != "null" {
		t.Fatalf("decoded empty result = %+v", result)
	}
}

func TestDecodeSnapshotSliceEnforcesManwaItemShape(t *testing.T) {
	result, err := DecodeSnapshotSlice([]byte(`{
        "expectedTotal":1,
        "items":[{
            "comicId":"comic-1",
            "membership":{"origin":"remoteSnapshot","folderId":"0"},
            "summary":{"title":"Comic 1","cover":null,"subtitle":null},
            "contentObservation":{"visibilityScope":"manwa:level:2","markerEvidence":{"channel":"source-defined","scheme":"manwa-list-chapter-id-v1","value":"normal:chapter-1|full:"},"updateTime":null},
            "accountObservation":{"sourceUnreadByVariant":{"normal":true},"sourceUnread":true}
        }],
        "complete":true,
        "checkpoint":null,
        "sessionPatch":null
    }`))
	if err != nil {
		t.Fatalf("decode valid item: %v", err)
	}
	if len(result.Items) != 1 || result.Items[0].ComicID != "comic-1" || result.Items[0].ItemDigest != "" {
		t.Fatalf("decoded item = %+v", result.Items[0])
	}
}

func TestDecodeSnapshotSliceRejectsUnsafeOrDriftingResults(t *testing.T) {
	tests := map[string]string{
		"non-null session patch": `{"expectedTotal":0,"items":[],"complete":true,"checkpoint":null,"sessionPatch":{"set":{}}}`,
		"trailing JSON":          `{"expectedTotal":0,"items":[],"complete":true,"checkpoint":null} {}`,
		"missing title":          `{"expectedTotal":1,"items":[{"comicId":"comic-1","membership":{"origin":"remoteSnapshot","folderId":"0"},"summary":{"title":"","cover":null,"subtitle":null},"contentObservation":{"visibilityScope":"manwa:level:2","markerEvidence":{"channel":"source-defined","scheme":"manwa-list-chapter-id-v1","value":"normal:c|full:"},"updateTime":null},"accountObservation":{"sourceUnreadByVariant":{"normal":false},"sourceUnread":false}}],"complete":true,"checkpoint":null}`,
		"missing normal unread":  `{"expectedTotal":1,"items":[{"comicId":"comic-1","membership":{"origin":"remoteSnapshot","folderId":"0"},"summary":{"title":"Comic 1","cover":null,"subtitle":null},"contentObservation":{"visibilityScope":"manwa:level:2","markerEvidence":{"channel":"source-defined","scheme":"manwa-list-chapter-id-v1","value":"normal:c|full:"},"updateTime":null},"accountObservation":{"sourceUnreadByVariant":{"full":false},"sourceUnread":false}}],"complete":true,"checkpoint":null}`,
		"non-null update time":   `{"expectedTotal":1,"items":[{"comicId":"comic-1","membership":{"origin":"remoteSnapshot","folderId":"0"},"summary":{"title":"Comic 1","cover":null,"subtitle":null},"contentObservation":{"visibilityScope":"manwa:level:2","markerEvidence":{"channel":"source-defined","scheme":"manwa-list-chapter-id-v1","value":"normal:c|full:"},"updateTime":"2026-08-29T00:00:00Z"},"accountObservation":{"sourceUnreadByVariant":{"normal":false},"sourceUnread":false}}],"complete":true,"checkpoint":null}`,
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeSnapshotSlice([]byte(raw)); !errors.Is(err, ErrSnapshotInvalidSlice) {
				t.Fatalf("decode error = %v, want %v", err, ErrSnapshotInvalidSlice)
			}
		})
	}
}
