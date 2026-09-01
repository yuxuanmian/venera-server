package v2store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"venera-server/internal/v2crypto"
	"venera-server/internal/v2domain"
)

type EnrollmentCodeResult struct {
	CodeID    string
	Code      string
	ExpiresAt time.Time
}

type ClaimClientEnrollmentRequest struct {
	PendingClientID      string
	TokenDigest          string
	EnrollmentCodeDigest string
	DisplayName          string
	Platform             string
	AppVersion           string
	IdempotencyKey       string
}

type EnrollmentResult struct {
	Client        v2domain.Client
	InitialCursor string
}

type revokeClientResponse struct {
	ClientID string               `json:"clientId"`
	State    v2domain.ClientState `json:"state"`
	Revision v2domain.Revision    `json:"revision"`
}

func (r *Repository) EnrollmentCodeDigest(code string) string {
	return v2crypto.Digest(r.keys.CredentialHMAC, "enrollment-code-v1\x00"+code)
}

func (r *Repository) CreateEnrollmentCode(ctx context.Context, ttl time.Duration) (EnrollmentCodeResult, error) {
	if ttl <= 0 {
		return EnrollmentCodeResult{}, ErrInvalidArgument
	}
	code, err := v2crypto.GenerateBearerToken()
	if err != nil {
		return EnrollmentCodeResult{}, err
	}
	result := EnrollmentCodeResult{
		CodeID:    r.db.NewID("enroll"),
		Code:      code,
		ExpiresAt: r.db.Now().Add(ttl),
	}
	digest := r.EnrollmentCodeDigest(code)
	err = r.db.WriteTx(ctx, func(tx *Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO client_enrollment_codes(
				code_id, code_digest, state, expires_at, created_at, updated_at
			) VALUES(?, ?, 'active', ?, ?, ?)`,
			result.CodeID, digest, formatTime(result.ExpiresAt), formatTime(r.db.Now()), formatTime(r.db.Now()))
		return err
	})
	if err != nil {
		return EnrollmentCodeResult{}, err
	}
	return result, nil
}

func (r *Repository) ClaimClientEnrollment(ctx context.Context, request ClaimClientEnrollmentRequest) (EnrollmentResult, error) {
	if request.PendingClientID == "" || request.TokenDigest == "" || request.EnrollmentCodeDigest == "" {
		return EnrollmentResult{}, ErrInvalidArgument
	}
	return r.claimClientEnrollment(ctx, request, true)
}

// ClaimClientOpenEnrollment creates a client without consuming an enrollment
// code. Callers must guard this method behind an explicit development-only
// configuration; the repository deliberately does not infer deployment mode.
func (r *Repository) ClaimClientOpenEnrollment(ctx context.Context, request ClaimClientEnrollmentRequest) (EnrollmentResult, error) {
	if request.PendingClientID == "" || request.TokenDigest == "" || request.EnrollmentCodeDigest != "" {
		return EnrollmentResult{}, ErrInvalidArgument
	}
	return r.claimClientEnrollment(ctx, request, false)
}

func (r *Repository) claimClientEnrollment(ctx context.Context, request ClaimClientEnrollmentRequest, requireCode bool) (EnrollmentResult, error) {
	if request.IdempotencyKey == "" {
		return EnrollmentResult{}, ErrIdempotencyKeyRequired
	}
	var result EnrollmentResult
	requestForDigest := struct {
		PendingClientID      string `json:"pendingClientId"`
		TokenDigest          string `json:"tokenDigest"`
		EnrollmentCodeDigest string `json:"enrollmentCodeDigest"`
		DisplayName          string `json:"displayName"`
		Platform             string `json:"platform"`
		AppVersion           string `json:"appVersion"`
	}{
		PendingClientID: request.PendingClientID, TokenDigest: request.TokenDigest,
		EnrollmentCodeDigest: request.EnrollmentCodeDigest, DisplayName: request.DisplayName,
		Platform: request.Platform, AppVersion: request.AppVersion,
	}
	err := r.db.WriteTx(ctx, func(tx *Tx) error {
		replay, err := r.beginIdempotency(ctx, tx, "pendingClient", request.PendingClientID, "claim-enrollment", request.IdempotencyKey, requestForDigest)
		if err != nil {
			return err
		}
		if replay != nil {
			return decodeIdempotencyReplay(replay, &result)
		}

		now := r.db.Now()
		if requireCode {
			var codeState, expiresAt string
			var claimedClient sql.NullString
			err = tx.QueryRowContext(ctx, `
				SELECT state, expires_at, claimed_client_id
				FROM client_enrollment_codes WHERE code_digest = ?`, request.EnrollmentCodeDigest).Scan(&codeState, &expiresAt, &claimedClient)
			if errors.Is(err, sql.ErrNoRows) {
				return ErrEnrollmentCodeNotFound
			}
			if err != nil {
				return err
			}
			expires, err := parseStoredTime(expiresAt)
			if err != nil {
				return fmt.Errorf("parse enrollment expiry: %w", err)
			}
			if !expires.After(now) {
				_, _ = tx.ExecContext(ctx, `UPDATE client_enrollment_codes SET state = 'expired', updated_at = ? WHERE code_digest = ? AND state = 'active'`, formatTime(now), request.EnrollmentCodeDigest)
				return ErrEnrollmentCodeExpired
			}
			if codeState != "active" {
				return ErrEnrollmentCodeNotFound
			}
		}

		clientID := tx.NewID("client")
		createdAt := formatTime(now)
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO client_installations(
				client_id, token_digest, display_name, platform, app_version,
				state, revision, created_at, updated_at
			) VALUES(?, ?, ?, ?, ?, 'active', 1, ?, ?)`,
			clientID, request.TokenDigest, request.DisplayName, request.Platform,
			request.AppVersion, createdAt, createdAt); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO client_sync_state(client_id, updated_at) VALUES(?, ?)`, clientID, createdAt); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO client_change_watermarks(client_id, updated_at) VALUES(?, ?)`, clientID, createdAt); err != nil {
			return err
		}
		if requireCode {
			updated, err := tx.ExecContext(ctx, `
				UPDATE client_enrollment_codes
				SET state = 'claimed', claimed_client_id = ?, claimed_at = ?, updated_at = ?
				WHERE code_digest = ? AND state = 'active'`, clientID, createdAt, createdAt, request.EnrollmentCodeDigest)
			if err != nil {
				return err
			}
			if rows, err := updated.RowsAffected(); err != nil || rows != 1 {
				if err != nil {
					return err
				}
				return ErrEnrollmentCodeNotFound
			}
		}
		cursor, err := v2crypto.EncodeCursor(r.keys.CursorMAC, clientID, 0)
		if err != nil {
			return err
		}
		client := v2domain.Client{
			ID:          v2domain.ClientID(clientID),
			DisplayName: request.DisplayName,
			Platform:    request.Platform,
			AppVersion:  request.AppVersion,
			State:       v2domain.ClientActive,
			Revision:    1,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		result = EnrollmentResult{Client: client, InitialCursor: cursor}
		return r.completeIdempotency(ctx, tx, "pendingClient", request.PendingClientID, "claim-enrollment", request.IdempotencyKey, 201, result)
	})
	return result, err
}

func (r *Repository) GetClient(ctx context.Context, clientID string) (v2domain.Client, error) {
	if clientID == "" {
		return v2domain.Client{}, ErrClientNotFound
	}
	var client v2domain.Client
	err := r.db.ReadTx(ctx, func(tx *Tx) error {
		return scanClient(tx.QueryRowContext(ctx, `
			SELECT client_id, display_name, platform, app_version, state, revision,
			       last_seen_at, created_at, updated_at, revoked_at
			FROM client_installations WHERE client_id = ?`, clientID), &client)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return v2domain.Client{}, ErrClientNotFound
	}
	return client, err
}

func (r *Repository) GetClientByTokenDigest(ctx context.Context, tokenDigest string) (v2domain.Client, error) {
	if tokenDigest == "" {
		return v2domain.Client{}, ErrAuthenticationFailed
	}
	var client v2domain.Client
	err := r.db.ReadTx(ctx, func(tx *Tx) error {
		return scanClient(tx.QueryRowContext(ctx, `
			SELECT client_id, display_name, platform, app_version, state, revision,
			       last_seen_at, created_at, updated_at, revoked_at
			FROM client_installations WHERE token_digest = ?`, tokenDigest), &client)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return v2domain.Client{}, ErrAuthenticationFailed
	}
	if err != nil {
		return v2domain.Client{}, err
	}
	if client.State != v2domain.ClientActive {
		return v2domain.Client{}, ErrAuthenticationFailed
	}
	return client, nil
}

func (r *Repository) RevokeClient(ctx context.Context, clientID string, expectedRevision v2domain.Revision, idempotencyKey string) error {
	if clientID == "" {
		return ErrClientNotFound
	}
	if expectedRevision < 1 {
		return ErrExpectedRevisionRequired
	}
	if idempotencyKey == "" {
		return ErrIdempotencyKeyRequired
	}
	requestForDigest := struct {
		ClientID         string            `json:"clientId"`
		ExpectedRevision v2domain.Revision `json:"expectedRevision"`
	}{clientID, expectedRevision}
	return r.db.WriteTx(ctx, func(tx *Tx) error {
		replay, err := r.beginIdempotency(ctx, tx, "client", clientID, "revoke-client", idempotencyKey, requestForDigest)
		if err != nil {
			return err
		}
		if replay != nil {
			return nil
		}
		var state string
		var revision int64
		err = tx.QueryRowContext(ctx, `SELECT state, revision FROM client_installations WHERE client_id = ?`, clientID).Scan(&state, &revision)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrClientNotFound
		}
		if err != nil {
			return err
		}
		if v2domain.Revision(revision) != expectedRevision {
			return ErrRevisionConflict
		}
		if state != string(v2domain.ClientActive) {
			return ErrClientRevoked
		}
		now := formatTime(r.db.Now())
		if _, err := tx.ExecContext(ctx, `DELETE FROM client_cloud_claims WHERE client_id = ?`, clientID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM cloud_mode_preparations WHERE client_id = ?`, clientID); err != nil {
			return err
		}
		newRevision := expectedRevision + 1
		if _, err := tx.ExecContext(ctx, `
			UPDATE client_installations
			SET state = 'revoked', revision = ?, revoked_at = ?, updated_at = ?
			WHERE client_id = ? AND state = 'active' AND revision = ?`,
			newRevision, now, now, clientID, expectedRevision); err != nil {
			return err
		}
		response := revokeClientResponse{ClientID: clientID, State: v2domain.ClientRevoked, Revision: newRevision}
		return r.completeIdempotency(ctx, tx, "client", clientID, "revoke-client", idempotencyKey, 200, response)
	})
}

func scanClient(row interface{ Scan(...any) error }, client *v2domain.Client) error {
	var lastSeen, createdAt, updatedAt, revokedAt sql.NullString
	var state string
	var revision int64
	if err := row.Scan(&client.ID, &client.DisplayName, &client.Platform, &client.AppVersion,
		&state, &revision, &lastSeen, &createdAt, &updatedAt, &revokedAt); err != nil {
		return err
	}
	client.State = v2domain.ClientState(state)
	client.Revision = v2domain.Revision(revision)
	var err error
	client.CreatedAt, err = parseStoredTime(createdAt.String)
	if err != nil {
		return err
	}
	client.UpdatedAt, err = parseStoredTime(updatedAt.String)
	if err != nil {
		return err
	}
	if lastSeen.Valid {
		value, parseErr := parseStoredTime(lastSeen.String)
		if parseErr != nil {
			return parseErr
		}
		client.LastSeenAt = &value
	}
	if revokedAt.Valid {
		value, parseErr := parseStoredTime(revokedAt.String)
		if parseErr != nil {
			return parseErr
		}
		client.RevokedAt = &value
	}
	return nil
}

func digestString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
