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
	// ErrAuthorizationMismatch means a scan captured an authorization fact
	// that is no longer true at its publication boundary.
	ErrAuthorizationMismatch  = errors.New("tracking_authorization_mismatch")
	ErrPartialSnapshotInvalid = errors.New("tracking_partial_snapshot_invalid")
)

// ClientDemandFence is the exact enabled client state observed when a scan
// started. StateRevision catches changes outside this artifact as well as
// changes to the artifact's interest set.
type ClientDemandFence struct {
	DeviceID      string   `json:"deviceId"`
	StateRevision int64    `json:"stateRevision"`
	InterestIDs   []string `json:"interestIds"`
}

// PublicationFence contains the session, visibility, and client-demand facts
// that must still authorize a scanner result at the SQLite commit boundary.
type PublicationFence struct {
	SessionEpoch    string              `json:"sessionEpoch"`
	VisibilityScope string              `json:"visibilityScope"`
	Demands         []ClientDemandFence `json:"demands"`
}

// PartialSnapshot is the restart-safe accumulator for one exact
// user/artifact scan. Checkpoint remains opaque to the host; its digest gives
// the store a corruption/boundary identity without interpreting source data.
type PartialSnapshot struct {
	UserID            string
	Artifact          domain.ArtifactIdentity
	CatalogID         string
	CatalogRevision   string
	RuntimeGeneration int64
	Fence             PublicationFence
	ExpectedTotal     *int
	Checkpoint        json.RawMessage
	Observations      []domain.Observation
	BoundaryDigest    string
	UpdatedAt         time.Time
}

func normalizeClientDemandFence(input ClientDemandFence) (ClientDemandFence, error) {
	if strings.TrimSpace(input.DeviceID) == "" || input.StateRevision < 1 {
		return ClientDemandFence{}, ErrPartialSnapshotInvalid
	}
	ids := append([]string(nil), input.InterestIDs...)
	sort.Strings(ids)
	for index, id := range ids {
		if strings.TrimSpace(id) == "" || id != strings.TrimSpace(id) {
			return ClientDemandFence{}, ErrPartialSnapshotInvalid
		}
		if index > 0 && ids[index-1] == id {
			return ClientDemandFence{}, ErrPartialSnapshotInvalid
		}
	}
	return ClientDemandFence{DeviceID: input.DeviceID, StateRevision: input.StateRevision, InterestIDs: ids}, nil
}

func normalizePublicationFence(input PublicationFence) (PublicationFence, error) {
	if strings.TrimSpace(input.SessionEpoch) == "" ||
		strings.TrimSpace(input.VisibilityScope) == "" ||
		len(input.Demands) == 0 {
		return PublicationFence{}, ErrAuthorizationMismatch
	}
	result := PublicationFence{
		SessionEpoch:    input.SessionEpoch,
		VisibilityScope: input.VisibilityScope,
		Demands:         make([]ClientDemandFence, 0, len(input.Demands)),
	}
	seen := make(map[string]struct{}, len(input.Demands))
	for _, demand := range input.Demands {
		normalized, err := normalizeClientDemandFence(demand)
		if err != nil {
			return PublicationFence{}, err
		}
		if _, exists := seen[normalized.DeviceID]; exists {
			return PublicationFence{}, ErrPartialSnapshotInvalid
		}
		seen[normalized.DeviceID] = struct{}{}
		result.Demands = append(result.Demands, normalized)
	}
	sort.Slice(result.Demands, func(i, j int) bool {
		return result.Demands[i].DeviceID < result.Demands[j].DeviceID
	})
	return result, nil
}

func normalizePartialSnapshot(input PartialSnapshot, limit int) (PartialSnapshot, error) {
	if strings.TrimSpace(input.UserID) == "" ||
		strings.TrimSpace(input.CatalogID) == "" ||
		!domain.FullRevision(input.CatalogRevision) ||
		input.RuntimeGeneration < 1 {
		return PartialSnapshot{}, ErrPartialSnapshotInvalid
	}
	if err := input.Artifact.Validate(); err != nil {
		return PartialSnapshot{}, err
	}
	fence, err := normalizePublicationFence(input.Fence)
	if err != nil {
		return PartialSnapshot{}, err
	}
	checkpoint := strings.TrimSpace(string(input.Checkpoint))
	if checkpoint == "" || checkpoint == "null" || !json.Valid([]byte(checkpoint)) {
		return PartialSnapshot{}, ErrPartialSnapshotInvalid
	}
	var checkpointObject map[string]json.RawMessage
	if err := json.Unmarshal([]byte(checkpoint), &checkpointObject); err != nil || checkpointObject == nil {
		return PartialSnapshot{}, ErrPartialSnapshotInvalid
	}
	expected := input.ExpectedTotal
	if expected == nil || *expected < 0 || (limit > 0 && *expected > limit) {
		return PartialSnapshot{}, ErrPartialSnapshotInvalid
	}
	observations := make([]domain.Observation, len(input.Observations))
	seen := make(map[string]struct{}, len(input.Observations))
	now := input.UpdatedAt
	if now.IsZero() {
		now = timeNowUTC()
	}
	for index, observation := range input.Observations {
		if observation.UserID != input.UserID {
			return PartialSnapshot{}, ErrPartialSnapshotInvalid
		}
		if observation.Artifact != input.Artifact ||
			observation.Revision != input.CatalogRevision ||
			observation.RuntimeGeneration != input.RuntimeGeneration {
			return PartialSnapshot{}, ErrPartialSnapshotInvalid
		}
		if err := observation.Validate(now); err != nil {
			return PartialSnapshot{}, fmt.Errorf("%w: observation %d: %v", ErrPartialSnapshotInvalid, index, err)
		}
		if _, exists := seen[observation.ComicID]; exists {
			return PartialSnapshot{}, ErrPartialSnapshotInvalid
		}
		seen[observation.ComicID] = struct{}{}
		observations[index] = observation
	}
	if len(observations) > *expected {
		return PartialSnapshot{}, ErrPartialSnapshotInvalid
	}
	digest, err := domain.Digest(json.RawMessage(checkpoint))
	if err != nil {
		return PartialSnapshot{}, ErrPartialSnapshotInvalid
	}
	if input.BoundaryDigest != "" && input.BoundaryDigest != digest {
		return PartialSnapshot{}, ErrPartialSnapshotInvalid
	}
	return PartialSnapshot{
		UserID:            input.UserID,
		Artifact:          input.Artifact,
		CatalogID:         input.CatalogID,
		CatalogRevision:   input.CatalogRevision,
		RuntimeGeneration: input.RuntimeGeneration,
		Fence:             fence,
		ExpectedTotal:     expected,
		Checkpoint:        json.RawMessage(checkpoint),
		Observations:      observations,
		BoundaryDigest:    digest,
		UpdatedAt:         now.UTC(),
	}, nil
}

func (r *Repository) SavePartialSnapshot(ctx context.Context, input PartialSnapshot) error {
	normalized, err := normalizePartialSnapshot(input, r.observationLimit)
	if err != nil {
		return err
	}
	demandsJSON, err := json.Marshal(normalized.Fence.Demands)
	if err != nil {
		return fmt.Errorf("encode partial snapshot demands: %w", err)
	}
	observationsJSON, err := json.Marshal(normalized.Observations)
	if err != nil {
		return fmt.Errorf("encode partial snapshot observations: %w", err)
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin partial snapshot transaction: %w", err)
	}
	defer tx.Rollback()
	if err := r.verifyPublicationFenceTx(ctx, tx, normalized.UserID, normalized.Artifact, normalized.Fence); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO tracking_scan_progress (
			user_id, source_key, file_name, catalog_id, catalog_revision,
			runtime_generation, session_epoch, visibility_scope, demands_json,
			expected_total, checkpoint_json, observations_json, boundary_digest, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(user_id, source_key, file_name) DO UPDATE SET
			catalog_id = excluded.catalog_id,
			catalog_revision = excluded.catalog_revision,
			runtime_generation = excluded.runtime_generation,
			session_epoch = excluded.session_epoch,
			visibility_scope = excluded.visibility_scope,
			demands_json = excluded.demands_json,
			expected_total = excluded.expected_total,
			checkpoint_json = excluded.checkpoint_json,
			observations_json = excluded.observations_json,
			boundary_digest = excluded.boundary_digest,
			updated_at = excluded.updated_at`,
		normalized.UserID,
		normalized.Artifact.SourceKey,
		normalized.Artifact.FileName,
		normalized.CatalogID,
		normalized.CatalogRevision,
		normalized.RuntimeGeneration,
		normalized.Fence.SessionEpoch,
		normalized.Fence.VisibilityScope,
		string(demandsJSON),
		*normalized.ExpectedTotal,
		string(normalized.Checkpoint),
		string(observationsJSON),
		normalized.BoundaryDigest,
		formatTime(normalized.UpdatedAt),
	); err != nil {
		return fmt.Errorf("save partial snapshot: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit partial snapshot: %w", err)
	}
	return nil
}

func (r *Repository) GetPartialSnapshot(ctx context.Context, userID string, artifact domain.ArtifactIdentity) (PartialSnapshot, bool, error) {
	var input PartialSnapshot
	input.UserID = userID
	input.Artifact = artifact
	var demandsJSON, checkpointJSON, observationsJSON, updatedAt string
	var expectedTotal sql.NullInt64
	err := r.db.QueryRowContext(ctx, `
		SELECT catalog_id, catalog_revision, runtime_generation, session_epoch,
		       visibility_scope, demands_json, expected_total, checkpoint_json,
		       observations_json, boundary_digest, updated_at
		FROM tracking_scan_progress
		WHERE user_id = ? AND source_key = ? AND file_name = ?`,
		userID, artifact.SourceKey, artifact.FileName,
	).Scan(
		&input.CatalogID,
		&input.CatalogRevision,
		&input.RuntimeGeneration,
		&input.Fence.SessionEpoch,
		&input.Fence.VisibilityScope,
		&demandsJSON,
		&expectedTotal,
		&checkpointJSON,
		&observationsJSON,
		&input.BoundaryDigest,
		&updatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return PartialSnapshot{}, false, nil
	}
	if err != nil {
		return PartialSnapshot{}, false, fmt.Errorf("get partial snapshot: %w", err)
	}
	if !expectedTotal.Valid {
		return PartialSnapshot{}, false, ErrPartialSnapshotInvalid
	}
	if err := json.Unmarshal([]byte(demandsJSON), &input.Fence.Demands); err != nil {
		return PartialSnapshot{}, false, fmt.Errorf("%w: demands: %v", ErrPartialSnapshotInvalid, err)
	}
	if err := json.Unmarshal([]byte(observationsJSON), &input.Observations); err != nil {
		return PartialSnapshot{}, false, fmt.Errorf("%w: observations: %v", ErrPartialSnapshotInvalid, err)
	}
	for index := range input.Observations {
		input.Observations[index].UserID = userID
	}
	input.ExpectedTotal = new(int)
	*input.ExpectedTotal = int(expectedTotal.Int64)
	input.Checkpoint = json.RawMessage(checkpointJSON)
	input.UpdatedAt, err = parseTime(updatedAt)
	if err != nil {
		return PartialSnapshot{}, false, fmt.Errorf("%w: timestamp: %v", ErrPartialSnapshotInvalid, err)
	}
	normalized, err := normalizePartialSnapshot(input, r.observationLimit)
	if err != nil {
		return PartialSnapshot{}, false, err
	}
	return normalized, true, nil
}

func (r *Repository) DeletePartialSnapshot(ctx context.Context, userID string, artifact domain.ArtifactIdentity) error {
	if strings.TrimSpace(userID) == "" {
		return errors.New("userID is required")
	}
	if err := artifact.Validate(); err != nil {
		return err
	}
	_, err := r.db.ExecContext(ctx, `
		DELETE FROM tracking_scan_progress
		WHERE user_id = ? AND source_key = ? AND file_name = ?`,
		userID, artifact.SourceKey, artifact.FileName,
	)
	if err != nil {
		return fmt.Errorf("delete partial snapshot: %w", err)
	}
	return nil
}

// RecordAccountScope stores the latest validated visibility scope only when
// the source session epoch is still the one supplied by the probe.
func (r *Repository) RecordAccountScope(ctx context.Context, userID string, artifact domain.ArtifactIdentity, sessionEpoch, visibilityScope string) error {
	if strings.TrimSpace(userID) == "" || strings.TrimSpace(sessionEpoch) == "" || strings.TrimSpace(visibilityScope) == "" {
		return ErrAuthorizationMismatch
	}
	if err := artifact.Validate(); err != nil {
		return err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin account scope transaction: %w", err)
	}
	defer tx.Rollback()
	current, err := sourceSessionEpochTx(ctx, tx, userID, artifact.SourceKey)
	if err != nil {
		return err
	}
	if current != sessionEpoch {
		return ErrAuthorizationMismatch
	}
	now := timeNowUTC().Truncate(time.Millisecond)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO tracking_scan_account_scope
			(user_id, source_key, file_name, session_epoch, visibility_scope, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(user_id, source_key, file_name) DO UPDATE SET
			session_epoch = excluded.session_epoch,
			visibility_scope = excluded.visibility_scope,
			updated_at = excluded.updated_at`,
		userID, artifact.SourceKey, artifact.FileName, sessionEpoch, visibilityScope, formatTime(now),
	); err != nil {
		return fmt.Errorf("record account scope: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit account scope: %w", err)
	}
	return nil
}

func (r *Repository) verifyPublicationFenceTx(
	ctx context.Context,
	tx *sql.Tx,
	userID string,
	artifact domain.ArtifactIdentity,
	fence PublicationFence,
) error {
	normalized, err := normalizePublicationFence(fence)
	if err != nil {
		return err
	}
	currentSession, err := sourceSessionEpochTx(ctx, tx, userID, artifact.SourceKey)
	if err != nil {
		return err
	}
	if currentSession != normalized.SessionEpoch {
		return ErrAuthorizationMismatch
	}
	var storedEpoch, storedScope string
	err = tx.QueryRowContext(ctx, `
		SELECT session_epoch, visibility_scope
		FROM tracking_scan_account_scope
		WHERE user_id = ? AND source_key = ? AND file_name = ?`,
		userID, artifact.SourceKey, artifact.FileName,
	).Scan(&storedEpoch, &storedScope)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrAuthorizationMismatch
	}
	if err != nil {
		return fmt.Errorf("read account scope fence: %w", err)
	}
	if storedEpoch != normalized.SessionEpoch || storedScope != normalized.VisibilityScope {
		return ErrAuthorizationMismatch
	}

	actual := make(map[string]*ClientDemandFence, len(normalized.Demands))
	rows, err := tx.QueryContext(ctx, `
		SELECT cs.device_id, cs.state_revision, i.comic_id
		FROM tracking_client_state cs
		JOIN devices d ON d.device_id = cs.device_id
		JOIN tracking_interests i ON i.device_id = cs.device_id
		WHERE d.user_id = ? AND cs.cloud_enabled = 1
		  AND i.source_key = ? AND i.file_name = ?
		ORDER BY cs.device_id, i.comic_id`,
		userID, artifact.SourceKey, artifact.FileName,
	)
	if err != nil {
		return fmt.Errorf("read demand fence: %w", err)
	}
	for rows.Next() {
		var deviceID, comicID string
		var stateRevision int64
		if err := rows.Scan(&deviceID, &stateRevision, &comicID); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan demand fence: %w", err)
		}
		demand := actual[deviceID]
		if demand == nil {
			demand = &ClientDemandFence{DeviceID: deviceID, StateRevision: stateRevision}
			actual[deviceID] = demand
		}
		if demand.StateRevision != stateRevision {
			_ = rows.Close()
			return ErrAuthorizationMismatch
		}
		demand.InterestIDs = append(demand.InterestIDs, comicID)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("read demand fence rows: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close demand fence rows: %w", err)
	}
	if len(actual) != len(normalized.Demands) {
		return ErrAuthorizationMismatch
	}
	for _, expected := range normalized.Demands {
		current := actual[expected.DeviceID]
		if current == nil || current.StateRevision != expected.StateRevision ||
			!sameStringSet(current.InterestIDs, expected.InterestIDs) {
			return ErrAuthorizationMismatch
		}
	}
	return nil
}

func sourceSessionEpochTx(ctx context.Context, tx *sql.Tx, userID, sourceKey string) (string, error) {
	var cookieHash, updatedAt string
	err := tx.QueryRowContext(ctx, `
		SELECT cookie_hash, updated_at
		FROM source_sessions WHERE user_id = ? AND source = ?`,
		userID, sourceKey,
	).Scan(&cookieHash, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrAuthorizationMismatch
	}
	if err != nil {
		return "", fmt.Errorf("read source session epoch: %w", err)
	}
	if strings.TrimSpace(cookieHash) == "" || strings.TrimSpace(updatedAt) == "" {
		return "", ErrAuthorizationMismatch
	}
	return cookieHash + "\x00" + updatedAt, nil
}

func sameStringSet(left, right []string) bool {
	a := append([]string(nil), left...)
	b := append([]string(nil), right...)
	sort.Strings(a)
	sort.Strings(b)
	if len(a) != len(b) {
		return false
	}
	for index := range a {
		if a[index] != b[index] {
			return false
		}
	}
	return true
}
