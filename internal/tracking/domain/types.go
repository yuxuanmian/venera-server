package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	MaxMarkerBytes   = 4096
	MaxMetadataBytes = 4096
	MaxRecentIDs     = 10
	MaxInterests     = 10000
)

// A full revision is a lowercase hexadecimal Git object name. Git SHA-1
// repositories use 40 characters while SHA-256 repositories use 64; both
// are immutable identities at this boundary. Short names, mixed case, and
// branch/tag expressions are intentionally rejected.
var revisionPattern = regexp.MustCompile(`^[0-9a-f]{40,64}$`)
var catalogIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$`)
var rfc3339WithZonePattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T.*(?:Z|[+-]\d{2}:\d{2})$`)

// ArtifactIdentity is the exact sourceKey/fileName pair selected by a
// catalog. sourceKey alone is intentionally never a Cloud identity.
type ArtifactIdentity struct {
	SourceKey string `json:"sourceKey"`
	FileName  string `json:"fileName"`
}

func (a ArtifactIdentity) Validate() error {
	if strings.TrimSpace(a.SourceKey) == "" {
		return errors.New("sourceKey is required")
	}
	name := strings.TrimSpace(a.FileName)
	if name == "" || name != a.FileName || strings.Contains(name, "/") || strings.Contains(name, "\\") || !strings.HasSuffix(name, ".js") {
		return errors.New("fileName must be a .js basename")
	}
	return nil
}

// UpdateState contains only host-defined content evidence. A nil pointer is
// an absent field; invalid siblings are rejected at the wire boundary before
// normalization rather than being silently compared.
type UpdateState struct {
	UpdatedAt        *time.Time `json:"updatedAt,omitempty"`
	LatestChapterID  *string    `json:"latestChapterId,omitempty"`
	ChapterCount     *int       `json:"chapterCount,omitempty"`
	RecentChapterIDs []string   `json:"recentChapterIds,omitempty"`
}

// MarshalJSON uses the shared wire form: timestamps are UTC with exactly
// millisecond precision, while absent optional fields remain absent.
func (s UpdateState) MarshalJSON() ([]byte, error) {
	type wire struct {
		UpdatedAt        string   `json:"updatedAt,omitempty"`
		LatestChapterID  *string  `json:"latestChapterId,omitempty"`
		ChapterCount     *int     `json:"chapterCount,omitempty"`
		RecentChapterIDs []string `json:"recentChapterIds,omitempty"`
	}
	var updatedAt string
	if s.UpdatedAt != nil {
		updatedAt = s.UpdatedAt.UTC().Format("2006-01-02T15:04:05.000Z07:00")
	}
	return json.Marshal(wire{
		UpdatedAt:        updatedAt,
		LatestChapterID:  s.LatestChapterID,
		ChapterCount:     s.ChapterCount,
		RecentChapterIDs: s.RecentChapterIDs,
	})
}

func (s UpdateState) Usable() bool {
	return s.UpdatedAt != nil || s.LatestChapterID != nil || s.ChapterCount != nil || len(s.RecentChapterIDs) > 0
}

func NormalizeUpdateState(input UpdateState) (UpdateState, error) {
	var out UpdateState
	if input.UpdatedAt != nil {
		if input.UpdatedAt.IsZero() {
			return out, errors.New("updatedAt is invalid")
		}
		t := input.UpdatedAt.UTC()
		out.UpdatedAt = &t
	}
	if input.LatestChapterID != nil {
		id := strings.TrimSpace(*input.LatestChapterID)
		if id == "" {
			return out, errors.New("latestChapterId is empty")
		}
		out.LatestChapterID = &id
	}
	if input.ChapterCount != nil {
		if *input.ChapterCount < 0 {
			return out, errors.New("chapterCount must be non-negative")
		}
		count := *input.ChapterCount
		out.ChapterCount = &count
	}
	if input.RecentChapterIDs != nil {
		seen := make(map[string]struct{}, len(input.RecentChapterIDs))
		for _, raw := range input.RecentChapterIDs {
			id := strings.TrimSpace(raw)
			if id == "" {
				return out, errors.New("recentChapterIds contains an empty ID")
			}
			if _, ok := seen[id]; ok {
				return out, errors.New("recentChapterIds contains a duplicate ID")
			}
			seen[id] = struct{}{}
			out.RecentChapterIDs = append(out.RecentChapterIDs, id)
			if len(out.RecentChapterIDs) > MaxRecentIDs {
				return out, fmt.Errorf("recentChapterIds exceeds %d IDs", MaxRecentIDs)
			}
		}
		if len(out.RecentChapterIDs) == 0 {
			return out, errors.New("recentChapterIds is empty")
		}
	}
	if !out.Usable() {
		return out, errors.New("update state has no usable fields")
	}
	return out, nil
}

// DecodeUpdateState validates the canonical JSON representation used by the
// scanner and HTTP boundaries.
func DecodeUpdateState(data []byte) (*UpdateState, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("update state must be an object: %w", err)
	}
	allowed := map[string]bool{"updatedAt": true, "latestChapterId": true, "chapterCount": true, "recentChapterIds": true}
	for key := range raw {
		if !allowed[key] {
			return nil, fmt.Errorf("unknown update state field %q", key)
		}
	}
	var input struct {
		UpdatedAt        *string   `json:"updatedAt"`
		LatestChapterID  *string   `json:"latestChapterId"`
		ChapterCount     *int      `json:"chapterCount"`
		RecentChapterIDs *[]string `json:"recentChapterIds"`
	}
	if err := json.Unmarshal(data, &input); err != nil {
		return nil, fmt.Errorf("invalid update state: %w", err)
	}
	var state UpdateState
	if input.UpdatedAt != nil {
		value := strings.TrimSpace(*input.UpdatedAt)
		if !rfc3339WithZonePattern.MatchString(value) {
			return nil, errors.New("updatedAt must be RFC3339 with timezone")
		}
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return nil, fmt.Errorf("updatedAt is invalid: %w", err)
		}
		state.UpdatedAt = &parsed
	}
	state.LatestChapterID = input.LatestChapterID
	state.ChapterCount = input.ChapterCount
	if input.RecentChapterIDs != nil {
		state.RecentChapterIDs = *input.RecentChapterIDs
	}
	normalized, err := NormalizeUpdateState(state)
	if err != nil {
		return nil, err
	}
	return &normalized, nil
}

// FavoriteUpdate is the shared normalized source observation payload.
type FavoriteUpdate struct {
	State        *UpdateState   `json:"state,omitempty"`
	SourceUnread *bool          `json:"sourceUnread,omitempty"`
	Marker       *string        `json:"marker,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
}

func (f FavoriteUpdate) Validate() (FavoriteUpdate, error) {
	out := f
	if f.State != nil {
		state, err := NormalizeUpdateState(*f.State)
		if err != nil {
			return FavoriteUpdate{}, err
		}
		out.State = &state
	}
	if f.Marker != nil {
		marker := strings.TrimSpace(*f.Marker)
		if marker == "" {
			return FavoriteUpdate{}, errors.New("marker is empty")
		}
		if utf8.RuneCountInString(marker) == 0 || len([]byte(marker)) > MaxMarkerBytes {
			return FavoriteUpdate{}, errors.New("marker exceeds UTF-8 byte limit")
		}
		out.Marker = &marker
	}
	if f.Metadata != nil {
		encoded, err := json.Marshal(f.Metadata)
		if err != nil {
			return FavoriteUpdate{}, fmt.Errorf("metadata is not JSON: %w", err)
		}
		if len(encoded) > MaxMetadataBytes {
			return FavoriteUpdate{}, errors.New("metadata exceeds UTF-8 byte limit")
		}
	}
	return out, nil
}

// Observation is a current fact owned by a pinned scanner revision.
type Observation struct {
	UserID            string           `json:"-"`
	Artifact          ArtifactIdentity `json:"artifact"`
	Revision          string           `json:"revision"`
	ComicID           string           `json:"comicId"`
	ObservedAt        time.Time        `json:"observedAt"`
	ValidUntil        time.Time        `json:"validUntil"`
	FavoriteUpdate    FavoriteUpdate   `json:"favoriteUpdate"`
	RuntimeGeneration int64            `json:"runtimeGeneration"`
	PayloadDigest     string           `json:"payloadDigest,omitempty"`
}

func (o Observation) Validate(now time.Time) error {
	if err := o.Artifact.Validate(); err != nil {
		return err
	}
	if !revisionPattern.MatchString(o.Revision) {
		return errors.New("revision must be a full 40-64 character lowercase hexadecimal commit")
	}
	if strings.TrimSpace(o.ComicID) == "" {
		return errors.New("comicId is required")
	}
	if o.ObservedAt.IsZero() || o.ValidUntil.IsZero() || !o.ValidUntil.After(o.ObservedAt) {
		return errors.New("observation freshness window is invalid")
	}
	if now.After(o.ValidUntil) {
		return errors.New("observation is stale")
	}
	_, err := o.FavoriteUpdate.Validate()
	return err
}

// Authority is the public Server-pinned Cloud capability projection.
type Authority struct {
	CatalogID      string             `json:"catalogId"`
	ActiveRevision string             `json:"activeRevision"`
	Generation     int64              `json:"-"`
	Artifacts      []ArtifactIdentity `json:"artifacts"`
}

func (a Authority) Validate() error {
	if !catalogIDPattern.MatchString(a.CatalogID) {
		return errors.New("catalogId is invalid")
	}
	if !revisionPattern.MatchString(a.ActiveRevision) {
		return errors.New("activeRevision must be a full 40-64 character lowercase hexadecimal commit")
	}
	seen := make(map[ArtifactIdentity]struct{}, len(a.Artifacts))
	for _, artifact := range a.Artifacts {
		if err := artifact.Validate(); err != nil {
			return err
		}
		if _, ok := seen[artifact]; ok {
			return errors.New("duplicate Cloud artifact")
		}
		seen[artifact] = struct{}{}
	}
	return nil
}

type Interest struct {
	DeviceID string           `json:"-"`
	Artifact ArtifactIdentity `json:"artifact"`
	ComicID  string           `json:"comicId"`
}

func (i Interest) Validate() error {
	if err := i.Artifact.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(i.ComicID) == "" {
		return errors.New("comicId is required")
	}
	return nil
}

type ClientState struct {
	DeviceID      string     `json:"-"`
	CloudEnabled  bool       `json:"cloudEnabled"`
	StateRevision int64      `json:"stateRevision"`
	UpdatedAt     time.Time  `json:"updatedAt"`
	Interests     []Interest `json:"interests"`
}

func (c ClientState) Validate() error {
	if strings.TrimSpace(c.DeviceID) == "" {
		return errors.New("deviceID is required")
	}
	if c.StateRevision < 0 {
		return errors.New("stateRevision must be non-negative")
	}
	if len(c.Interests) > MaxInterests {
		return fmt.Errorf("interests exceeds %d items", MaxInterests)
	}
	seen := make(map[Interest]struct{}, len(c.Interests))
	for _, interest := range c.Interests {
		if interest.DeviceID != "" && interest.DeviceID != c.DeviceID {
			return errors.New("interest deviceID does not match client state")
		}
		interest.DeviceID = c.DeviceID
		if err := interest.Validate(); err != nil {
			return err
		}
		if _, ok := seen[interest]; ok {
			return errors.New("duplicate tracking interest")
		}
		seen[interest] = struct{}{}
	}
	return nil
}

// CanonicalJSON makes ETags stable across all map insertion orders.
func CanonicalJSON(value any) ([]byte, error) {
	return json.Marshal(value)
}

// Digest returns a compact canonical JSON payload digest.
func Digest(value any) (string, error) {
	data, err := CanonicalJSON(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum[:]), nil
}

// FullRevision reports whether a string is an immutable commit identifier.
func FullRevision(value string) bool { return revisionPattern.MatchString(value) }

// RFC3339WithTimezone reports whether the string has an explicit timezone.
func RFC3339WithTimezone(value string) bool {
	if !rfc3339WithZonePattern.MatchString(strings.TrimSpace(value)) {
		return false
	}
	_, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value))
	return err == nil
}

// UnmarshalJSON rejects unknown fields for the shared observation object while
// still allowing empty FavoriteUpdate objects to represent unknown evidence.
func (f *FavoriteUpdate) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	for key := range raw {
		if key != "state" && key != "sourceUnread" && key != "marker" && key != "metadata" {
			return fmt.Errorf("unknown favoriteUpdate field %q", key)
		}
	}
	if value, ok := raw["state"]; ok && !bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		state, err := DecodeUpdateState(value)
		if err != nil {
			return err
		}
		f.State = state
	}
	if value, ok := raw["sourceUnread"]; ok && !bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		var unread bool
		if err := json.Unmarshal(value, &unread); err != nil {
			return errors.New("sourceUnread must be boolean or null")
		}
		f.SourceUnread = &unread
	}
	if value, ok := raw["marker"]; ok && !bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		var marker string
		if err := json.Unmarshal(value, &marker); err != nil {
			return errors.New("marker must be string or null")
		}
		f.Marker = &marker
	}
	if value, ok := raw["metadata"]; ok && !bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		if err := json.Unmarshal(value, &f.Metadata); err != nil {
			return errors.New("metadata must be an object or null")
		}
	}
	return nil
}
