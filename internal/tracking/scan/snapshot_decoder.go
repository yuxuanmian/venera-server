package scan

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"venera-server/internal/tracking/domain"
)

var (
	ErrSnapshotInvalidSlice  = errors.New("favorite snapshot slice is invalid")
	ErrSnapshotDuplicateItem = errors.New("favorite snapshot contains a duplicate item")
	ErrSnapshotItemLimit     = errors.New("favorite snapshot item limit exceeded")
)

const (
	DefaultSnapshotMaxItems      = domain.MaxInterests
	DefaultSnapshotMaxCheckpoint = 16 << 10
	maxComicIDBytes              = 1024
)

// SnapshotItem is the host-normalized result of one scanner item. ItemJSON is
// retained as the canonical source result for digests and diagnostics; the
// FavoriteUpdate field is the only fact that can reach observation storage.
type SnapshotItem struct {
	ComicID        string
	FavoriteUpdate domain.FavoriteUpdate
	ItemDigest     string
	ItemJSON       json.RawMessage
}

type SnapshotSliceResult struct {
	ExpectedTotal *int
	Items         []SnapshotItem
	Complete      bool
	Checkpoint    json.RawMessage
}

// DecodeSnapshotSlice strictly decodes the shared scanner result. Source
// metadata is bounded and retained only as metadata; comparison and
// presentation remain App-owned responsibilities.
func DecodeSnapshotSlice(raw []byte) (SnapshotSliceResult, error) {
	return DecodeSnapshotSliceWithLimits(raw, DefaultSnapshotMaxItems, DefaultSnapshotMaxCheckpoint)
}

func DecodeSnapshotSliceWithLimits(raw []byte, maxItems, maxCheckpoint int) (SnapshotSliceResult, error) {
	if len(raw) == 0 || maxItems <= 0 || maxCheckpoint <= 0 {
		return SnapshotSliceResult{}, ErrSnapshotInvalidSlice
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var wire snapshotSliceWire
	if err := decoder.Decode(&wire); err != nil {
		return SnapshotSliceResult{}, ErrSnapshotInvalidSlice
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return SnapshotSliceResult{}, ErrSnapshotInvalidSlice
	}
	if wire.Complete == nil || wire.Items == nil {
		return SnapshotSliceResult{}, ErrSnapshotInvalidSlice
	}
	if wire.ExpectedTotal != nil && (*wire.ExpectedTotal < 0 || *wire.ExpectedTotal > maxItems) {
		return SnapshotSliceResult{}, ErrSnapshotItemLimit
	}
	if !validNullOrAbsent(wire.SessionPatch) {
		return SnapshotSliceResult{}, ErrSnapshotInvalidSlice
	}

	checkpoint := bytes.TrimSpace(wire.Checkpoint)
	if *wire.Complete {
		if len(checkpoint) > 0 && !bytes.Equal(checkpoint, []byte("null")) {
			return SnapshotSliceResult{}, ErrSnapshotInvalidSlice
		}
		checkpoint = nil
	} else {
		if len(checkpoint) == 0 || bytes.Equal(checkpoint, []byte("null")) ||
			len(checkpoint) > maxCheckpoint || !json.Valid(checkpoint) {
			return SnapshotSliceResult{}, ErrSnapshotInvalidSlice
		}
	}
	if len(wire.Items) > maxItems {
		return SnapshotSliceResult{}, ErrSnapshotItemLimit
	}

	result := SnapshotSliceResult{
		ExpectedTotal: wire.ExpectedTotal,
		Complete:      *wire.Complete,
		Checkpoint:    append(json.RawMessage(nil), checkpoint...),
		Items:         make([]SnapshotItem, 0, len(wire.Items)),
	}
	seen := make(map[string]struct{}, len(wire.Items))
	for index, item := range wire.Items {
		comicID := strings.TrimSpace(item.ComicID)
		if !validComicID(comicID) {
			return SnapshotSliceResult{}, fmt.Errorf("%w: item %d comicId", ErrSnapshotInvalidSlice, index)
		}
		if _, exists := seen[comicID]; exists {
			return SnapshotSliceResult{}, ErrSnapshotDuplicateItem
		}
		seen[comicID] = struct{}{}
		if item.FavoriteUpdate == nil {
			return SnapshotSliceResult{}, fmt.Errorf("%w: item %d favoriteUpdate", ErrSnapshotInvalidSlice, index)
		}
		update, err := item.FavoriteUpdate.Validate()
		if err != nil {
			return SnapshotSliceResult{}, fmt.Errorf("%w: item %d favoriteUpdate: %v", ErrSnapshotInvalidSlice, index, err)
		}
		itemJSON, err := json.Marshal(struct {
			ComicID        string                `json:"comicId"`
			FavoriteUpdate domain.FavoriteUpdate `json:"favoriteUpdate"`
		}{ComicID: comicID, FavoriteUpdate: update})
		if err != nil {
			return SnapshotSliceResult{}, ErrSnapshotInvalidSlice
		}
		itemDigest, err := domain.Digest(json.RawMessage(itemJSON))
		if err != nil {
			return SnapshotSliceResult{}, ErrSnapshotInvalidSlice
		}
		result.Items = append(result.Items, SnapshotItem{
			ComicID:        comicID,
			FavoriteUpdate: update,
			ItemDigest:     itemDigest,
			ItemJSON:       itemJSON,
		})
	}
	return result, nil
}

type snapshotSliceWire struct {
	ExpectedTotal *int               `json:"expectedTotal"`
	Items         []snapshotItemWire `json:"items"`
	Complete      *bool              `json:"complete"`
	Checkpoint    json.RawMessage    `json:"checkpoint"`
	SessionPatch  json.RawMessage    `json:"sessionPatch"`
}

type snapshotItemWire struct {
	ComicID        string                 `json:"comicId"`
	FavoriteUpdate *domain.FavoriteUpdate `json:"favoriteUpdate"`
}

func validNullOrAbsent(value json.RawMessage) bool {
	value = bytes.TrimSpace(value)
	return len(value) == 0 || bytes.Equal(value, []byte("null"))
}

func validComicID(value string) bool {
	return value != "" && len([]byte(value)) <= maxComicIDBytes && utf8.ValidString(value) &&
		!strings.ContainsAny(value, "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\x7f")
}
