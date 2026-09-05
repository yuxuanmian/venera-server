package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"venera-server/internal/tracking/domain"
)

const wireTimeLayout = "2006-01-02T15:04:05.000Z07:00"

// CatalogState is the durable Server pointer for the currently active
// validated catalog revision. The checkout itself remains filesystem data.
type CatalogState struct {
	CatalogID      string
	ActiveRevision string
	Generation     int64
	ActivatedAt    time.Time
	Digest         string
}

func (s CatalogState) Validate() error {
	if strings.TrimSpace(s.CatalogID) == "" {
		return errors.New("catalogID is required")
	}
	if !domain.FullRevision(s.ActiveRevision) {
		return errors.New("active revision must be a full commit")
	}
	if s.Generation < 1 {
		return errors.New("catalog generation must be positive")
	}
	if s.ActivatedAt.IsZero() {
		return errors.New("catalog activation time is required")
	}
	if strings.TrimSpace(s.Digest) == "" {
		return errors.New("catalog digest is required")
	}
	return nil
}

func (r *Repository) SetCatalogState(ctx context.Context, state CatalogState) error {
	if err := state.Validate(); err != nil {
		return err
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO tracking_catalog_state
			(catalog_id, active_revision, generation, activated_at, digest)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(catalog_id) DO UPDATE SET
			active_revision = excluded.active_revision,
			generation = excluded.generation,
			activated_at = excluded.activated_at,
			digest = excluded.digest`,
		state.CatalogID,
		state.ActiveRevision,
		state.Generation,
		formatTime(state.ActivatedAt),
		state.Digest,
	)
	if err != nil {
		return fmt.Errorf("set tracking catalog state: %w", err)
	}
	return nil
}

func (r *Repository) GetCatalogState(ctx context.Context, catalogID string) (CatalogState, bool, error) {
	var state CatalogState
	var activatedAt string
	err := r.db.QueryRowContext(ctx, `
		SELECT catalog_id, active_revision, generation, activated_at, digest
		FROM tracking_catalog_state WHERE catalog_id = ?`, catalogID).
		Scan(&state.CatalogID, &state.ActiveRevision, &state.Generation, &activatedAt, &state.Digest)
	if errors.Is(err, sql.ErrNoRows) {
		return CatalogState{}, false, nil
	}
	if err != nil {
		return CatalogState{}, false, fmt.Errorf("get tracking catalog state: %w", err)
	}
	parsed, err := parseTime(activatedAt)
	if err != nil {
		return CatalogState{}, false, fmt.Errorf("stored catalog activation time: %w", err)
	}
	state.ActivatedAt = parsed
	return state, true, nil
}

func normalizeClientState(input domain.ClientState) (domain.ClientState, error) {
	if err := input.Validate(); err != nil {
		return domain.ClientState{}, err
	}
	out := domain.ClientState{
		DeviceID:     strings.TrimSpace(input.DeviceID),
		CloudEnabled: input.CloudEnabled,
	}
	out.Interests = make([]domain.Interest, len(input.Interests))
	copy(out.Interests, input.Interests)
	for index := range out.Interests {
		out.Interests[index].DeviceID = out.DeviceID
	}
	sort.Slice(out.Interests, func(i, j int) bool {
		return interestLess(out.Interests[i], out.Interests[j])
	})
	for i := 1; i < len(out.Interests); i++ {
		if out.Interests[i-1] == out.Interests[i] {
			return domain.ClientState{}, errors.New("duplicate tracking interest")
		}
	}
	return out, nil
}

func interestLess(a, b domain.Interest) bool {
	if a.Artifact.SourceKey != b.Artifact.SourceKey {
		return a.Artifact.SourceKey < b.Artifact.SourceKey
	}
	if a.Artifact.FileName != b.Artifact.FileName {
		return a.Artifact.FileName < b.Artifact.FileName
	}
	return a.ComicID < b.ComicID
}

func sameClientState(a, b domain.ClientState) bool {
	if a.CloudEnabled != b.CloudEnabled || len(a.Interests) != len(b.Interests) {
		return false
	}
	for i := range a.Interests {
		if a.Interests[i] != b.Interests[i] {
			return false
		}
	}
	return true
}

func formatTime(value time.Time) string {
	return value.UTC().Format(wireTimeLayout)
}

func parseTime(value string) (time.Time, error) {
	if !domain.RFC3339WithTimezone(value) {
		return time.Time{}, fmt.Errorf("invalid RFC3339 timestamp")
	}
	return time.Parse(time.RFC3339Nano, value)
}
