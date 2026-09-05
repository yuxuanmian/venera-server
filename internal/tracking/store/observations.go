package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"venera-server/internal/tracking/domain"
)

var (
	ErrCatalogRevisionMismatch = errors.New("catalog_revision_mismatch")
	ErrGenerationMismatch      = errors.New("tracking_generation_mismatch")
	ErrObservationLimit        = errors.New("observation_limit_exceeded")
	errCatalogRevisionMismatch = ErrCatalogRevisionMismatch
	errGenerationMismatch      = ErrGenerationMismatch
	errObservationLimit        = ErrObservationLimit
)

// ReplaceObservations atomically publishes a complete snapshot for one exact
// user/artifact pair. Existing rows for that pair are removed only after every
// incoming observation and the active revision/generation fence validate.
func (r *Repository) ReplaceObservations(
	ctx context.Context,
	userID string,
	artifact domain.ArtifactIdentity,
	revision string,
	generation int64,
	observations []domain.Observation,
	authority domain.Authority,
) error {
	return r.replaceObservations(ctx, userID, artifact, revision, generation, observations, authority, PublicationFence{}, false)
}

// ReplaceObservationsWithFence publishes and clears the restart-safe partial
// accumulator in the same transaction. Every captured session, visibility,
// and enabled-demand fact is re-read immediately before the sole commit.
func (r *Repository) ReplaceObservationsWithFence(
	ctx context.Context,
	userID string,
	artifact domain.ArtifactIdentity,
	revision string,
	generation int64,
	observations []domain.Observation,
	authority domain.Authority,
	fence PublicationFence,
) error {
	return r.replaceObservations(ctx, userID, artifact, revision, generation, observations, authority, fence, true)
}

func (r *Repository) replaceObservations(
	ctx context.Context,
	userID string,
	artifact domain.ArtifactIdentity,
	revision string,
	generation int64,
	observations []domain.Observation,
	authority domain.Authority,
	fence PublicationFence,
	clearProgress bool,
) error {
	if strings.TrimSpace(userID) == "" {
		return errors.New("userID is required")
	}
	if err := authority.Validate(); err != nil {
		return fmt.Errorf("invalid tracking authority: %w", err)
	}
	if err := artifact.Validate(); err != nil {
		return err
	}
	if !containsArtifact(authority.Artifacts, artifact) {
		return errors.New("artifact is not Cloud-capable at active revision")
	}
	if revision != authority.ActiveRevision {
		return errCatalogRevisionMismatch
	}
	if authority.Generation > 0 && generation != authority.Generation {
		return errGenerationMismatch
	}
	if len(observations) > r.observationLimit {
		return errObservationLimit
	}
	now := timeNowUTC()
	normalized := make([]domain.Observation, len(observations))
	seen := make(map[string]struct{}, len(observations))
	for index, observation := range observations {
		prepared, err := r.prepareObservation(observation, userID, artifact, revision, generation, authority, now)
		if err != nil {
			return fmt.Errorf("observation %d: %w", index, err)
		}
		if _, ok := seen[prepared.ComicID]; ok {
			return errors.New("duplicate observation comicID")
		}
		seen[prepared.ComicID] = struct{}{}
		normalized[index] = prepared
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].ComicID < normalized[j].ComicID })

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin observation transaction: %w", err)
	}
	defer tx.Rollback()
	if err := r.checkFenceTx(ctx, tx, authority, revision, generation); err != nil {
		return err
	}
	if len(fence.Demands) > 0 {
		if err := r.verifyPublicationFenceTx(ctx, tx, userID, artifact, fence); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM tracking_observations
		WHERE user_id = ? AND source_key = ? AND file_name = ?`,
		userID, artifact.SourceKey, artifact.FileName,
	); err != nil {
		return fmt.Errorf("replace observations: clear old snapshot: %w", err)
	}
	for _, observation := range normalized {
		if err := insertObservationTx(ctx, tx, observation); err != nil {
			return err
		}
	}
	if clearProgress {
		if _, err := tx.ExecContext(ctx, `
			DELETE FROM tracking_scan_progress
			WHERE user_id = ? AND source_key = ? AND file_name = ?`,
			userID, artifact.SourceKey, artifact.FileName,
		); err != nil {
			return fmt.Errorf("clear partial snapshot: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit observations: %w", err)
	}
	return nil
}

// PutObservation writes one current fact without deleting other comics in the
// same artifact. Complete scanner snapshots should use ReplaceObservations.
func (r *Repository) PutObservation(ctx context.Context, observation domain.Observation, authority domain.Authority) error {
	if err := authority.Validate(); err != nil {
		return fmt.Errorf("invalid tracking authority: %w", err)
	}
	now := timeNowUTC()
	prepared, err := r.prepareObservation(observation, observation.UserID, observation.Artifact, observation.Revision, observation.RuntimeGeneration, authority, now)
	if err != nil {
		return err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin observation transaction: %w", err)
	}
	defer tx.Rollback()
	if err := r.checkFenceTx(ctx, tx, authority, prepared.Revision, prepared.RuntimeGeneration); err != nil {
		return err
	}
	if err := insertObservationTx(ctx, tx, prepared); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit observation: %w", err)
	}
	return nil
}

func (r *Repository) prepareObservation(
	input domain.Observation,
	userID string,
	artifact domain.ArtifactIdentity,
	revision string,
	generation int64,
	authority domain.Authority,
	now time.Time,
) (domain.Observation, error) {
	if strings.TrimSpace(userID) == "" {
		return domain.Observation{}, errors.New("userID is required")
	}
	if input.UserID != "" && input.UserID != userID {
		return domain.Observation{}, errors.New("observation userID does not match owner")
	}
	if input.Artifact != artifact {
		return domain.Observation{}, errors.New("observation artifact does not match snapshot artifact")
	}
	if input.Revision != revision {
		return domain.Observation{}, errCatalogRevisionMismatch
	}
	if input.RuntimeGeneration != generation {
		return domain.Observation{}, errGenerationMismatch
	}
	if !containsArtifact(authority.Artifacts, artifact) {
		return domain.Observation{}, errors.New("artifact is not Cloud-capable at active revision")
	}
	if err := input.Validate(now); err != nil {
		return domain.Observation{}, err
	}
	update, err := input.FavoriteUpdate.Validate()
	if err != nil {
		return domain.Observation{}, err
	}
	input.UserID = userID
	input.FavoriteUpdate = update
	if input.PayloadDigest == "" {
		input.PayloadDigest, err = observationDigest(input)
		if err != nil {
			return domain.Observation{}, fmt.Errorf("observation digest: %w", err)
		}
	}
	return input, nil
}

func (r *Repository) checkFenceTx(ctx context.Context, tx *sql.Tx, authority domain.Authority, revision string, generation int64) error {
	if revision != authority.ActiveRevision {
		return errCatalogRevisionMismatch
	}
	if authority.Generation > 0 && generation != authority.Generation {
		return errGenerationMismatch
	}
	var activeRevision string
	var activeGeneration int64
	err := tx.QueryRowContext(ctx, `
		SELECT active_revision, generation
		FROM tracking_catalog_state WHERE catalog_id = ?`, authority.CatalogID).
		Scan(&activeRevision, &activeGeneration)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read tracking catalog fence: %w", err)
	}
	if activeRevision != revision {
		return errCatalogRevisionMismatch
	}
	if authority.Generation > 0 && activeGeneration != generation {
		return errGenerationMismatch
	}
	return nil
}

func insertObservationTx(ctx context.Context, tx *sql.Tx, observation domain.Observation) error {
	stateJSON, metadataJSON, sourceUnread := sql.NullString{}, sql.NullString{}, any(nil)
	if observation.FavoriteUpdate.State != nil {
		data, err := json.Marshal(observation.FavoriteUpdate.State)
		if err != nil {
			return fmt.Errorf("encode observation update state: %w", err)
		}
		stateJSON = sql.NullString{String: string(data), Valid: true}
	}
	if observation.FavoriteUpdate.Metadata != nil {
		data, err := json.Marshal(observation.FavoriteUpdate.Metadata)
		if err != nil {
			return fmt.Errorf("encode observation metadata: %w", err)
		}
		metadataJSON = sql.NullString{String: string(data), Valid: true}
	}
	if observation.FavoriteUpdate.SourceUnread != nil {
		sourceUnread = boolInt(*observation.FavoriteUpdate.SourceUnread)
	}
	var marker any
	if observation.FavoriteUpdate.Marker != nil {
		marker = *observation.FavoriteUpdate.Marker
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO tracking_observations (
			user_id, source_key, file_name, comic_id, catalog_revision,
			update_state_json, source_unread, marker, metadata_json,
			observed_at, valid_until, payload_digest, runtime_generation
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(user_id, source_key, file_name, comic_id) DO UPDATE SET
			catalog_revision = excluded.catalog_revision,
			update_state_json = excluded.update_state_json,
			source_unread = excluded.source_unread,
			marker = excluded.marker,
			metadata_json = excluded.metadata_json,
			observed_at = excluded.observed_at,
			valid_until = excluded.valid_until,
			payload_digest = excluded.payload_digest,
			runtime_generation = excluded.runtime_generation`,
		observation.UserID,
		observation.Artifact.SourceKey,
		observation.Artifact.FileName,
		observation.ComicID,
		observation.Revision,
		stateJSON,
		sourceUnread,
		marker,
		metadataJSON,
		formatTime(observation.ObservedAt),
		formatTime(observation.ValidUntil),
		observation.PayloadDigest,
		observation.RuntimeGeneration,
	)
	if err != nil {
		return fmt.Errorf("write tracking observation: %w", err)
	}
	return nil
}

// ListCurrentObservations returns only rows for exact active interests, the
// active revision/generation, and a still-valid freshness window.
func (r *Repository) ListCurrentObservations(
	ctx context.Context,
	userID string,
	interests []domain.Interest,
	authority domain.Authority,
	now time.Time,
) ([]domain.Observation, error) {
	if strings.TrimSpace(userID) == "" {
		return nil, errors.New("userID is required")
	}
	if err := authority.Validate(); err != nil {
		return nil, fmt.Errorf("invalid tracking authority: %w", err)
	}
	if len(interests) > r.observationLimit {
		return nil, errObservationLimit
	}
	if err := r.checkFence(ctx, authority, authority.ActiveRevision, authority.Generation); err != nil {
		return nil, err
	}
	wanted := make(map[observationKey]struct{}, len(interests))
	for _, interest := range interests {
		if err := interest.Validate(); err != nil {
			return nil, err
		}
		if !containsArtifact(authority.Artifacts, interest.Artifact) {
			return nil, errors.New("artifact is not Cloud-capable at active revision")
		}
		wanted[observationKey{interest.Artifact, interest.ComicID}] = struct{}{}
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT user_id, source_key, file_name, comic_id, catalog_revision,
		       update_state_json, source_unread, marker, metadata_json,
		       observed_at, valid_until, payload_digest, runtime_generation
		FROM tracking_observations
		WHERE user_id = ? AND catalog_revision = ? AND valid_until > ?
		ORDER BY source_key, file_name, comic_id`,
		userID, authority.ActiveRevision, formatTime(now))
	if err != nil {
		return nil, fmt.Errorf("query current tracking observations: %w", err)
	}
	defer rows.Close()
	var result []domain.Observation
	for rows.Next() {
		observation, err := scanObservation(rows)
		if err != nil {
			return nil, err
		}
		if authority.Generation > 0 && observation.RuntimeGeneration != authority.Generation {
			continue
		}
		if _, ok := wanted[observationKey{observation.Artifact, observation.ComicID}]; !ok {
			continue
		}
		result = append(result, observation)
		if len(result) > r.observationLimit {
			return nil, errObservationLimit
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read current tracking observations: %w", err)
	}
	return result, nil
}

func (r *Repository) checkFence(ctx context.Context, authority domain.Authority, revision string, generation int64) error {
	state, found, err := r.GetCatalogState(ctx, authority.CatalogID)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	if state.ActiveRevision != revision {
		return errCatalogRevisionMismatch
	}
	if authority.Generation > 0 && state.Generation != generation {
		return errGenerationMismatch
	}
	return nil
}

type observationKey struct {
	Artifact domain.ArtifactIdentity
	ComicID  string
}

type rowScanner interface {
	Scan(...any) error
}

func scanObservation(row rowScanner) (domain.Observation, error) {
	var observation domain.Observation
	var stateJSON, marker, metadataJSON sql.NullString
	var sourceUnread sql.NullInt64
	var observedAt, validUntil string
	if err := row.Scan(
		&observation.UserID,
		&observation.Artifact.SourceKey,
		&observation.Artifact.FileName,
		&observation.ComicID,
		&observation.Revision,
		&stateJSON,
		&sourceUnread,
		&marker,
		&metadataJSON,
		&observedAt,
		&validUntil,
		&observation.PayloadDigest,
		&observation.RuntimeGeneration,
	); err != nil {
		return domain.Observation{}, fmt.Errorf("scan tracking observation: %w", err)
	}
	var err error
	observation.ObservedAt, err = parseTime(observedAt)
	if err != nil {
		return domain.Observation{}, fmt.Errorf("stored observation time: %w", err)
	}
	observation.ValidUntil, err = parseTime(validUntil)
	if err != nil {
		return domain.Observation{}, fmt.Errorf("stored observation expiry: %w", err)
	}
	if stateJSON.Valid {
		observation.FavoriteUpdate.State, err = domain.DecodeUpdateState([]byte(stateJSON.String))
		if err != nil {
			return domain.Observation{}, fmt.Errorf("stored observation update state: %w", err)
		}
	}
	if sourceUnread.Valid {
		if sourceUnread.Int64 != 0 && sourceUnread.Int64 != 1 {
			return domain.Observation{}, errors.New("stored observation source_unread is invalid")
		}
		value := sourceUnread.Int64 == 1
		observation.FavoriteUpdate.SourceUnread = &value
	}
	if marker.Valid {
		value := marker.String
		observation.FavoriteUpdate.Marker = &value
	}
	if metadataJSON.Valid {
		if err := json.Unmarshal([]byte(metadataJSON.String), &observation.FavoriteUpdate.Metadata); err != nil {
			return domain.Observation{}, fmt.Errorf("stored observation metadata: %w", err)
		}
	}
	return observation, nil
}

func observationDigest(observation domain.Observation) (string, error) {
	return domain.Digest(struct {
		Artifact       domain.ArtifactIdentity `json:"artifact"`
		Revision       string                  `json:"revision"`
		ComicID        string                  `json:"comicId"`
		ObservedAt     string                  `json:"observedAt"`
		ValidUntil     string                  `json:"validUntil"`
		FavoriteUpdate domain.FavoriteUpdate   `json:"favoriteUpdate"`
	}{
		Artifact:       observation.Artifact,
		Revision:       observation.Revision,
		ComicID:        observation.ComicID,
		ObservedAt:     formatTime(observation.ObservedAt),
		ValidUntil:     formatTime(observation.ValidUntil),
		FavoriteUpdate: observation.FavoriteUpdate,
	})
}
