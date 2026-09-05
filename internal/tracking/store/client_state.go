package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"venera-server/internal/tracking/domain"
)

// ReplaceClientState atomically replaces the complete enabled flag and exact
// interest set. Replacing a canonically identical document is idempotent and
// does not advance stateRevision or updatedAt.
func (r *Repository) ReplaceClientState(ctx context.Context, input domain.ClientState, authority domain.Authority) (domain.ClientState, error) {
	if err := authority.Validate(); err != nil {
		return domain.ClientState{}, fmt.Errorf("invalid tracking authority: %w", err)
	}
	normalized, err := normalizeClientState(input)
	if err != nil {
		return domain.ClientState{}, fmt.Errorf("invalid client state: %w", err)
	}
	for _, interest := range normalized.Interests {
		if !containsArtifact(authority.Artifacts, interest.Artifact) {
			return domain.ClientState{}, errors.New("artifact is not Cloud-capable at active revision")
		}
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.ClientState{}, fmt.Errorf("begin client state transaction: %w", err)
	}
	defer tx.Rollback()

	current, found, err := readClientStateTx(ctx, tx, normalized.DeviceID)
	if err != nil {
		return domain.ClientState{}, err
	}
	if found && sameClientState(current, normalized) {
		if err := tx.Commit(); err != nil {
			return domain.ClientState{}, fmt.Errorf("commit unchanged client state: %w", err)
		}
		return current, nil
	}

	now := timeNowUTC().Truncate(time.Millisecond)
	revision := int64(1)
	if found {
		revision = current.StateRevision + 1
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO tracking_client_state (device_id, cloud_enabled, state_revision, updated_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(device_id) DO UPDATE SET
			cloud_enabled = excluded.cloud_enabled,
			state_revision = excluded.state_revision,
			updated_at = excluded.updated_at`,
		normalized.DeviceID,
		boolInt(normalized.CloudEnabled),
		revision,
		formatTime(now),
	); err != nil {
		return domain.ClientState{}, fmt.Errorf("write client state: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM tracking_interests WHERE device_id = ?`, normalized.DeviceID); err != nil {
		return domain.ClientState{}, fmt.Errorf("replace tracking interests: %w", err)
	}
	for _, interest := range normalized.Interests {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO tracking_interests
				(device_id, source_key, file_name, comic_id, updated_at)
			VALUES (?, ?, ?, ?, ?)`,
			normalized.DeviceID,
			interest.Artifact.SourceKey,
			interest.Artifact.FileName,
			interest.ComicID,
			formatTime(now),
		); err != nil {
			return domain.ClientState{}, fmt.Errorf("write tracking interest: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return domain.ClientState{}, fmt.Errorf("commit client state: %w", err)
	}
	normalized.StateRevision = revision
	normalized.UpdatedAt = now
	return normalized, nil
}

func (r *Repository) GetClientState(ctx context.Context, deviceID string) (domain.ClientState, bool, error) {
	return readClientState(ctx, r.db, deviceID)
}

// ListClientStates returns every persisted client state in deterministic
// device order. The service uses the device table to resolve each device to
// its authenticated user before loading source sessions.
func (r *Repository) ListClientStates(ctx context.Context) ([]domain.ClientState, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT device_id FROM tracking_client_state ORDER BY device_id`)
	if err != nil {
		return nil, fmt.Errorf("list tracking client states: %w", err)
	}
	var deviceIDs []string
	for rows.Next() {
		var deviceID string
		if err := rows.Scan(&deviceID); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan tracking client state device: %w", err)
		}
		deviceIDs = append(deviceIDs, deviceID)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("read tracking client state devices: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close tracking client state devices: %w", err)
	}
	states := make([]domain.ClientState, 0, len(deviceIDs))
	for _, deviceID := range deviceIDs {
		state, found, err := r.GetClientState(ctx, deviceID)
		if err != nil {
			return nil, err
		}
		if found {
			states = append(states, state)
		}
	}
	return states, nil
}

func (r *Repository) ListInterests(ctx context.Context, deviceID string) ([]domain.Interest, error) {
	return readInterests(ctx, r.db, deviceID)
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func readClientState(ctx context.Context, q queryer, deviceID string) (domain.ClientState, bool, error) {
	var state domain.ClientState
	var cloudEnabled int
	var updatedAt string
	err := q.QueryRowContext(ctx, `
		SELECT device_id, cloud_enabled, state_revision, updated_at
		FROM tracking_client_state WHERE device_id = ?`, deviceID).
		Scan(&state.DeviceID, &cloudEnabled, &state.StateRevision, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ClientState{}, false, nil
	}
	if err != nil {
		return domain.ClientState{}, false, fmt.Errorf("get tracking client state: %w", err)
	}
	if cloudEnabled != 0 && cloudEnabled != 1 {
		return domain.ClientState{}, false, errors.New("stored cloud_enabled is invalid")
	}
	state.CloudEnabled = cloudEnabled == 1
	state.UpdatedAt, err = parseTime(updatedAt)
	if err != nil {
		return domain.ClientState{}, false, fmt.Errorf("stored client state timestamp: %w", err)
	}
	state.Interests, err = readInterests(ctx, q, deviceID)
	if err != nil {
		return domain.ClientState{}, false, err
	}
	return state, true, nil
}

func readClientStateTx(ctx context.Context, tx *sql.Tx, deviceID string) (domain.ClientState, bool, error) {
	return readClientState(ctx, tx, deviceID)
}

func readInterests(ctx context.Context, q queryer, deviceID string) ([]domain.Interest, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT source_key, file_name, comic_id
		FROM tracking_interests
		WHERE device_id = ?
		ORDER BY source_key, file_name, comic_id`, deviceID)
	if err != nil {
		return nil, fmt.Errorf("list tracking interests: %w", err)
	}
	defer rows.Close()
	var interests []domain.Interest
	for rows.Next() {
		var interest domain.Interest
		if err := rows.Scan(&interest.Artifact.SourceKey, &interest.Artifact.FileName, &interest.ComicID); err != nil {
			return nil, fmt.Errorf("scan tracking interest: %w", err)
		}
		interest.DeviceID = deviceID
		if err := interest.Validate(); err != nil {
			return nil, fmt.Errorf("stored tracking interest: %w", err)
		}
		interests = append(interests, interest)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read tracking interests: %w", err)
	}
	return interests, nil
}

func containsArtifact(artifacts []domain.ArtifactIdentity, want domain.ArtifactIdentity) bool {
	for _, artifact := range artifacts {
		if artifact == want {
			return true
		}
	}
	return false
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

var timeNowUTC = func() time.Time { return time.Now().UTC() }
