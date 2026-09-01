package v2store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"venera-server/internal/v2crypto"
)

const idempotencyRetention = 24 * time.Hour

var (
	ErrInvalidArgument                = errors.New("invalid argument")
	ErrClientNotFound                 = errors.New("client not found")
	ErrClientRevoked                  = errors.New("client revoked")
	ErrEnrollmentCodeNotFound         = errors.New("enrollment code not found")
	ErrEnrollmentCodeExpired          = errors.New("enrollment code expired")
	ErrAuthenticationFailed           = errors.New("authentication failed")
	ErrRevisionConflict               = errors.New("revision conflict")
	ErrIdempotencyKeyRequired         = errors.New("idempotency key is required")
	ErrIdempotencyConflict            = errors.New("idempotency key conflicts with a previous request")
	ErrIdempotencyInProgress          = errors.New("idempotency request is already in progress")
	ErrManifestSequenceRollback       = errors.New("manifest catalog sequence rollback")
	ErrManifestAlreadyActive          = errors.New("manifest catalog is already active")
	ErrArtifactNotFound               = errors.New("source artifact not found")
	ErrPackageReleaseArtifactConflict = errors.New("source package release belongs to another artifact")
	ErrPackageReleaseNotFound         = errors.New("source package release not found")
	ErrCandidateNotFound              = errors.New("source session candidate not found")
	ErrCandidateExpired               = errors.New("source session candidate expired")
	ErrCandidateState                 = errors.New("source session candidate state does not allow this operation")
	ErrSwitchConfirmationRequired     = errors.New("source account switch confirmation is required")
	ErrCloudClaimMustBeRemoved        = errors.New("cloud claim must be removed before switching source account")
	ErrSourceAccountNotFound          = errors.New("source account not found")
	ErrSourceAccountNotSelected       = errors.New("source account is not selected")
	ErrCloudPreparationNotFound       = errors.New("cloud preparation not found")
	ErrCloudPreparationExpired        = errors.New("cloud preparation expired")
	ErrCloudPreparationState          = errors.New("cloud preparation state does not allow this operation")
	ErrCloudClaimNotFound             = errors.New("cloud claim not found")
	ErrCloudClaimAlreadyExists        = errors.New("cloud claim already exists")
	ErrInventoryIncompatible          = errors.New("source inventory is incompatible")
	ErrCloudClaimNotAllowed           = errors.New("cloud claim is not allowed")
	ErrExpectedRevisionRequired       = errors.New("expected revision is required")
	ErrExpectedRevisionMismatch       = errors.New("expected revision does not match")
	ErrCloudSnapshotReceiptInvalid    = errors.New("cloud snapshot receipt is invalid")
	ErrCloudSnapshotReceiptRequired   = errors.New("cloud snapshot receipt is required")
)

type Repository struct {
	db   *DB
	keys v2crypto.KeySet
}

func NewRepository(db *DB, keys v2crypto.KeySet) *Repository {
	return &Repository{db: db, keys: keys}
}

func (r *Repository) DB() *DB {
	return r.db
}

func (r *Repository) Keys() v2crypto.KeySet {
	return r.keys
}

type idempotencyReplay struct {
	Status int
	Body   []byte
}

func (r *Repository) beginIdempotency(
	ctx context.Context,
	tx *Tx,
	actorKind, actorID, routeKey, key string,
	request any,
) (*idempotencyReplay, error) {
	if actorKind == "" || actorID == "" || routeKey == "" || key == "" {
		return nil, ErrIdempotencyKeyRequired
	}
	requestDigest, err := requestDigest(request)
	if err != nil {
		return nil, fmt.Errorf("%w: idempotency request: %v", ErrInvalidArgument, err)
	}
	idempotencyDigest := v2crypto.Digest(r.keys.CredentialHMAC, key)
	now := r.db.Now()
	expiresAt := now.Add(idempotencyRetention)

	var storedRequestDigest, state, expiresAtText string
	var status sql.NullInt64
	var body []byte
	err = tx.QueryRowContext(ctx, `
		SELECT request_digest, response_status, response_body, state, expires_at
		FROM idempotency_records
		WHERE actor_kind = ? AND actor_id = ? AND route_key = ? AND idempotency_digest = ?`,
		actorKind, actorID, routeKey, idempotencyDigest).Scan(
		&storedRequestDigest, &status, &body, &state, &expiresAtText)
	if err == nil {
		storedExpiry, parseErr := time.Parse(time.RFC3339Nano, expiresAtText)
		if parseErr == nil && storedExpiry.After(now) {
			if storedRequestDigest != requestDigest {
				return nil, ErrIdempotencyConflict
			}
			if state == "completed" {
				return &idempotencyReplay{Status: int(status.Int64), Body: append([]byte(nil), body...)}, nil
			}
			return nil, ErrIdempotencyInProgress
		}
		if _, deleteErr := tx.ExecContext(ctx, `
			DELETE FROM idempotency_records
			WHERE actor_kind = ? AND actor_id = ? AND route_key = ? AND idempotency_digest = ?`,
			actorKind, actorID, routeKey, idempotencyDigest); deleteErr != nil {
			return nil, deleteErr
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	_, insertErr := tx.ExecContext(ctx, `
		INSERT INTO idempotency_records(
			actor_kind, actor_id, route_key, idempotency_digest, request_digest,
			state, expires_at, created_at, updated_at
		) VALUES(?, ?, ?, ?, ?, 'running', ?, ?, ?)`,
		actorKind, actorID, routeKey, idempotencyDigest, requestDigest,
		expiresAt.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	if insertErr != nil {
		// A different process may have won the unique-key race. Re-read it and
		// return the same safe conflict/in-progress result as the normal path.
		var raceRequestDigest, raceState, raceExpires string
		var raceStatus sql.NullInt64
		var raceBody []byte
		readErr := tx.QueryRowContext(ctx, `
			SELECT request_digest, response_status, response_body, state, expires_at
			FROM idempotency_records
			WHERE actor_kind = ? AND actor_id = ? AND route_key = ? AND idempotency_digest = ?`,
			actorKind, actorID, routeKey, idempotencyDigest).Scan(
			&raceRequestDigest, &raceStatus, &raceBody, &raceState, &raceExpires)
		if readErr != nil {
			return nil, insertErr
		}
		if raceRequestDigest != requestDigest {
			return nil, ErrIdempotencyConflict
		}
		if raceState == "completed" {
			return &idempotencyReplay{Status: int(raceStatus.Int64), Body: append([]byte(nil), raceBody...)}, nil
		}
		return nil, ErrIdempotencyInProgress
	}
	return nil, nil
}

func (r *Repository) completeIdempotency(ctx context.Context, tx *Tx, actorKind, actorID, routeKey, key string, status int, response any) error {
	if key == "" {
		return ErrIdempotencyKeyRequired
	}
	body, err := json.Marshal(response)
	if err != nil {
		return err
	}
	idempotencyDigest := v2crypto.Digest(r.keys.CredentialHMAC, key)
	result, err := tx.ExecContext(ctx, `
		UPDATE idempotency_records
		SET response_status = ?, response_body = ?, state = 'completed', updated_at = ?
		WHERE actor_kind = ? AND actor_id = ? AND route_key = ? AND idempotency_digest = ? AND state = 'running'`,
		status, body, r.db.Now().Format(time.RFC3339Nano), actorKind, actorID, routeKey, idempotencyDigest)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return ErrIdempotencyInProgress
	}
	return nil
}

func requestDigest(request any) (string, error) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func decodeIdempotencyReplay(replay *idempotencyReplay, target any) error {
	if replay == nil {
		return nil
	}
	if len(replay.Body) == 0 {
		return nil
	}
	return json.Unmarshal(replay.Body, target)
}

func formatTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func parseStoredTime(value string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, value)
}
