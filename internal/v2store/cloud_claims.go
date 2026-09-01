package v2store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"venera-server/internal/v2domain"
)

type CreateCloudPreparationRequest struct {
	ClientID                      string
	ArtifactID                    string
	ExpectedSourceAccountRevision v2domain.Revision
	ExpectedLinkRevision          v2domain.Revision
	ExpectedInventoryRevision     v2domain.Revision
	TTL                           time.Duration
	IdempotencyKey                string
}

type CloudPreparation struct {
	ID                     string                    `json:"preparationId"`
	ClientID               string                    `json:"clientId"`
	SourceAccountID        string                    `json:"sourceAccountId"`
	ArtifactID             string                    `json:"artifactId"`
	PackageReleaseID       string                    `json:"packageReleaseId"`
	State                  v2domain.PreparationState `json:"state"`
	Stage                  v2domain.PreparationStage `json:"stage"`
	FixedSessionEpoch      int64                     `json:"fixedSessionEpoch"`
	FixedInventoryRevision v2domain.Revision         `json:"fixedInventoryRevision"`
	BlockedReason          string                    `json:"blockedReason,omitempty"`
	SnapshotReceiptID      string                    `json:"snapshotReceiptId,omitempty"`
	ExpiresAt              time.Time                 `json:"expiresAt"`
	Revision               v2domain.Revision         `json:"revision"`
}

type MarkPreparationSnapshotReadyRequest struct {
	ClientID         string
	PreparationID    string
	ExpectedRevision v2domain.Revision
	SnapshotDigest   string
	IdempotencyKey   string
}

type CommitCloudClaimRequest struct {
	ClientID          string
	PreparationID     string
	ExpectedRevision  v2domain.Revision
	SnapshotReceiptID string
	SnapshotDigest    string
	IdempotencyKey    string
}

type DeleteCloudClaimRequest struct {
	ClientID         string
	ArtifactID       string
	SourceAccountID  string
	ExpectedRevision v2domain.Revision
	IdempotencyKey   string
}

type CloudClaimResult struct {
	ClientID        string              `json:"clientId"`
	SourceAccountID string              `json:"sourceAccountId"`
	ArtifactID      string              `json:"artifactId"`
	State           v2domain.ClaimState `json:"state"`
	Revision        v2domain.Revision   `json:"revision"`
}

func (r *Repository) CreateCloudPreparation(ctx context.Context, request CreateCloudPreparationRequest) (CloudPreparation, error) {
	if request.ClientID == "" || request.ArtifactID == "" || request.ExpectedSourceAccountRevision < 1 || request.ExpectedLinkRevision < 1 || request.ExpectedInventoryRevision < 1 || request.TTL <= 0 || request.IdempotencyKey == "" {
		return CloudPreparation{}, ErrInvalidArgument
	}
	requestForDigest := struct {
		ClientID                      string            `json:"clientId"`
		ArtifactID                    string            `json:"artifactId"`
		ExpectedSourceAccountRevision v2domain.Revision `json:"expectedSourceAccountRevision"`
		ExpectedLinkRevision          v2domain.Revision `json:"expectedLinkRevision"`
		ExpectedInventoryRevision     v2domain.Revision `json:"expectedInventoryRevision"`
		TTLSeconds                    int64             `json:"ttlSeconds"`
	}{request.ClientID, request.ArtifactID, request.ExpectedSourceAccountRevision, request.ExpectedLinkRevision, request.ExpectedInventoryRevision, int64(request.TTL / time.Second)}
	var result CloudPreparation
	err := r.db.WriteTx(ctx, func(tx *Tx) error {
		replay, err := r.beginIdempotency(ctx, tx, "client", request.ClientID, "create-cloud-preparation", request.IdempotencyKey, requestForDigest)
		if err != nil {
			return err
		}
		if replay != nil {
			return decodeIdempotencyReplay(replay, &result)
		}
		if err := ensureActiveClient(ctx, tx, request.ClientID); err != nil {
			return err
		}
		var accountID, accountState, inventoryRelease, inventoryCompatibility string
		var sessionEpoch, accountRevision, linkRevision, inventoryRevision int64
		if err := tx.QueryRowContext(ctx, `
			SELECT l.source_account_id, a.state, a.session_epoch, a.revision, l.revision,
			       COALESCE(i.package_release_id, ''), COALESCE(i.compatibility_state, ''),
			       COALESCE(i.inventory_revision, 0)
			FROM client_source_links l
			JOIN source_accounts a ON a.source_account_id = l.source_account_id
			LEFT JOIN client_source_inventory i
			  ON i.client_id = l.client_id AND i.artifact_id = l.artifact_id
			WHERE l.client_id = ? AND l.artifact_id = ? AND l.state = 'linked' AND l.selected_for_artifact = 1`,
			request.ClientID, request.ArtifactID).Scan(&accountID, &accountState, &sessionEpoch, &accountRevision, &linkRevision,
			&inventoryRelease, &inventoryCompatibility, &inventoryRevision); errors.Is(err, sql.ErrNoRows) {
			return ErrSourceAccountNotSelected
		} else if err != nil {
			return err
		}
		if accountState != string(v2domain.SourceAccountActive) {
			return ErrCloudClaimNotAllowed
		}
		if v2domain.Revision(accountRevision) != request.ExpectedSourceAccountRevision || v2domain.Revision(linkRevision) != request.ExpectedLinkRevision || v2domain.Revision(inventoryRevision) != request.ExpectedInventoryRevision {
			return ErrRevisionConflict
		}
		if inventoryCompatibility != string(v2domain.CompatibilityCompatible) || inventoryRelease == "" || inventoryRevision < 1 {
			return ErrInventoryIncompatible
		}
		var releaseState, releaseArtifact string
		if err := tx.QueryRowContext(ctx, `SELECT state, artifact_id FROM source_package_releases WHERE package_release_id = ?`, inventoryRelease).Scan(&releaseState, &releaseArtifact); errors.Is(err, sql.ErrNoRows) {
			return ErrPackageReleaseNotFound
		} else if err != nil {
			return err
		} else if releaseState != "active" || releaseArtifact != request.ArtifactID {
			return ErrInventoryIncompatible
		}
		now := r.db.Now()
		result = CloudPreparation{
			ID: tx.NewID("prep"), ClientID: request.ClientID, SourceAccountID: accountID,
			ArtifactID: request.ArtifactID, PackageReleaseID: inventoryRelease,
			State: v2domain.PreparationPreparing, Stage: v2domain.PreparationWaitSnapshot,
			FixedSessionEpoch: sessionEpoch, FixedInventoryRevision: v2domain.Revision(inventoryRevision),
			ExpiresAt: now.Add(request.TTL), Revision: 1,
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO cloud_mode_preparations(
				preparation_id, client_id, source_account_id, artifact_id, package_release_id,
				state, stage, fixed_session_epoch, fixed_inventory_revision, expires_at,
				revision, created_at, updated_at
			) VALUES(?, ?, ?, ?, ?, 'preparing', 'waitSnapshot', ?, ?, ?, 1, ?, ?)`,
			result.ID, request.ClientID, accountID, request.ArtifactID, inventoryRelease,
			result.FixedSessionEpoch, result.FixedInventoryRevision, formatTime(result.ExpiresAt), formatTime(now), formatTime(now))
		if err != nil {
			return err
		}
		return r.completeIdempotency(ctx, tx, "client", request.ClientID, "create-cloud-preparation", request.IdempotencyKey, 201, result)
	})
	return result, err
}

func (r *Repository) MarkPreparationSnapshotReady(ctx context.Context, request MarkPreparationSnapshotReadyRequest) (CloudPreparation, error) {
	if request.ClientID == "" || request.PreparationID == "" || request.ExpectedRevision < 1 || request.SnapshotDigest == "" || request.IdempotencyKey == "" {
		return CloudPreparation{}, ErrInvalidArgument
	}
	requestForDigest := struct {
		ClientID         string            `json:"clientId"`
		PreparationID    string            `json:"preparationId"`
		ExpectedRevision v2domain.Revision `json:"expectedRevision"`
		SnapshotDigest   string            `json:"snapshotDigest"`
	}{request.ClientID, request.PreparationID, request.ExpectedRevision, request.SnapshotDigest}
	var result CloudPreparation
	err := r.db.WriteTx(ctx, func(tx *Tx) error {
		replay, err := r.beginIdempotency(ctx, tx, "client", request.ClientID, "mark-snapshot-ready", request.IdempotencyKey, requestForDigest)
		if err != nil {
			return err
		}
		if replay != nil {
			return decodeIdempotencyReplay(replay, &result)
		}
		prep, err := loadPreparation(ctx, tx, request.ClientID, request.PreparationID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrCloudPreparationNotFound
		}
		if err != nil {
			return err
		}
		if prep.revision != int64(request.ExpectedRevision) {
			return ErrRevisionConflict
		}
		if expired, err := preparationExpired(prep, r.db.Now()); err != nil {
			return err
		} else if expired {
			return ErrCloudPreparationExpired
		}
		if prep.state != string(v2domain.PreparationPreparing) || prep.stage != string(v2domain.PreparationWaitSnapshot) {
			return ErrCloudPreparationState
		}
		receiptID := tx.NewID("receipt")
		now := r.db.Now()
		receiptExpiresAt := now.Add(15 * time.Minute)
		if prepExpiry, parseErr := parseStoredTime(prep.expiresAt); parseErr != nil {
			return parseErr
		} else if prepExpiry.Before(receiptExpiresAt) {
			receiptExpiresAt = prepExpiry
		}
		var baseChangeSeq int64
		if err := tx.QueryRowContext(ctx, "SELECT high_change_seq FROM client_change_watermarks WHERE client_id = ?", request.ClientID).Scan(&baseChangeSeq); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO snapshot_receipts(
				snapshot_receipt_id, client_id, source_account_id, artifact_id, scope_kind,
				package_release_id, fixed_session_epoch, base_change_seq, snapshot_digest,
				state, commit_expires_at, created_at
			) VALUES(?, ?, ?, ?, 'source', ?, ?, ?, ?, 'issued', ?, ?)`,
			receiptID, request.ClientID, prep.sourceAccountID, prep.artifactID, prep.packageReleaseID,
			prep.fixedSessionEpoch, baseChangeSeq, request.SnapshotDigest, formatTime(receiptExpiresAt), formatTime(now)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE cloud_mode_preparations SET state = 'snapshotReady', stage = 'deliverSnapshot',
				snapshot_receipt_id = ?, revision = revision + 1, updated_at = ?
			WHERE preparation_id = ? AND client_id = ? AND revision = ?`,
			receiptID, formatTime(now), request.PreparationID, request.ClientID, request.ExpectedRevision); err != nil {
			return err
		}
		loaded, err := loadPreparation(ctx, tx, request.ClientID, request.PreparationID)
		if err != nil {
			return err
		}
		result = preparationRowToPublic(loaded)
		return r.completeIdempotency(ctx, tx, "client", request.ClientID, "mark-snapshot-ready", request.IdempotencyKey, 200, result)
	})
	return result, err
}

func (r *Repository) CommitCloudClaim(ctx context.Context, request CommitCloudClaimRequest) (CloudClaimResult, error) {
	if request.ClientID == "" || request.PreparationID == "" || request.ExpectedRevision < 1 || request.SnapshotReceiptID == "" || request.SnapshotDigest == "" || request.IdempotencyKey == "" {
		return CloudClaimResult{}, ErrInvalidArgument
	}
	var result CloudClaimResult
	err := r.db.WriteTx(ctx, func(tx *Tx) error {
		var err error
		result, err = r.CommitCloudClaimTx(ctx, tx, request)
		return err
	})
	return result, err
}

// CommitCloudClaimTx performs the complete claim mutation in the caller's
// transaction. The caller owns the transaction boundary and may append the
// client projection before committing it.
func (r *Repository) CommitCloudClaimTx(ctx context.Context, tx *Tx, request CommitCloudClaimRequest) (CloudClaimResult, error) {
	if tx == nil || request.ClientID == "" || request.PreparationID == "" || request.ExpectedRevision < 1 || request.SnapshotReceiptID == "" || request.SnapshotDigest == "" || request.IdempotencyKey == "" {
		return CloudClaimResult{}, ErrInvalidArgument
	}
	requestForDigest := struct {
		ClientID          string            `json:"clientId"`
		PreparationID     string            `json:"preparationId"`
		ExpectedRevision  v2domain.Revision `json:"expectedRevision"`
		SnapshotReceiptID string            `json:"snapshotReceiptId"`
		SnapshotDigest    string            `json:"snapshotDigest"`
	}{request.ClientID, request.PreparationID, request.ExpectedRevision, request.SnapshotReceiptID, request.SnapshotDigest}
	var result CloudClaimResult
	replay, err := r.beginIdempotency(ctx, tx, "client", request.ClientID, "commit-cloud-claim", request.IdempotencyKey, requestForDigest)
	if err != nil {
		return CloudClaimResult{}, err
	}
	if replay != nil {
		if err := decodeIdempotencyReplay(replay, &result); err != nil {
			return CloudClaimResult{}, err
		}
		return result, nil
	}
	prep, err := loadPreparation(ctx, tx, request.ClientID, request.PreparationID)
	if errors.Is(err, sql.ErrNoRows) {
		return CloudClaimResult{}, ErrCloudPreparationNotFound
	}
	if err != nil {
		return CloudClaimResult{}, err
	}
	if prep.revision != int64(request.ExpectedRevision) {
		return CloudClaimResult{}, ErrRevisionConflict
	}
	if expired, err := preparationExpired(prep, r.db.Now()); err != nil {
		return CloudClaimResult{}, err
	} else if expired {
		return CloudClaimResult{}, ErrCloudPreparationExpired
	}
	if prep.state != string(v2domain.PreparationSnapshotReady) || prep.stage != string(v2domain.PreparationDeliverSnapshot) {
		return CloudClaimResult{}, ErrCloudPreparationState
	}
	if !prep.snapshotReceiptID.Valid || prep.snapshotReceiptID.String != request.SnapshotReceiptID {
		return CloudClaimResult{}, ErrCloudSnapshotReceiptInvalid
	}

	var receiptState, receiptExpires, receiptClient, receiptAccount, receiptArtifact string
	var receiptPackage, receiptDigest string
	var receiptEpoch int64
	if err := tx.QueryRowContext(ctx, `
		SELECT state, commit_expires_at, client_id, source_account_id, artifact_id,
		       package_release_id, fixed_session_epoch, snapshot_digest
		FROM snapshot_receipts WHERE snapshot_receipt_id = ?`, request.SnapshotReceiptID).Scan(
		&receiptState, &receiptExpires, &receiptClient, &receiptAccount, &receiptArtifact,
		&receiptPackage, &receiptEpoch, &receiptDigest); errors.Is(err, sql.ErrNoRows) {
		return CloudClaimResult{}, ErrCloudSnapshotReceiptInvalid
	} else if err != nil {
		return CloudClaimResult{}, err
	}
	expires, err := parseStoredTime(receiptExpires)
	if err != nil {
		return CloudClaimResult{}, ErrCloudSnapshotReceiptInvalid
	}
	if receiptState != "issued" || !expires.After(r.db.Now()) || receiptClient != request.ClientID ||
		receiptAccount != prep.sourceAccountID || receiptArtifact != prep.artifactID ||
		receiptPackage != prep.packageReleaseID || receiptEpoch != prep.fixedSessionEpoch ||
		receiptDigest != request.SnapshotDigest {
		return CloudClaimResult{}, ErrCloudSnapshotReceiptInvalid
	}

	var linkState, accountState, inventoryCompatibility, inventoryRelease string
	var accountEpoch int64
	if err := tx.QueryRowContext(ctx, `
		SELECT l.state, a.state, a.session_epoch, i.compatibility_state, COALESCE(i.package_release_id, '')
		FROM client_source_links l
		JOIN source_accounts a ON a.source_account_id = l.source_account_id
		JOIN client_source_inventory i ON i.client_id = l.client_id AND i.artifact_id = l.artifact_id
		WHERE l.client_id = ? AND l.source_account_id = ? AND l.artifact_id = ?
		  AND l.state = 'linked' AND l.selected_for_artifact = 1`,
		request.ClientID, prep.sourceAccountID, prep.artifactID).Scan(
		&linkState, &accountState, &accountEpoch, &inventoryCompatibility, &inventoryRelease); errors.Is(err, sql.ErrNoRows) {
		return CloudClaimResult{}, ErrCloudClaimNotAllowed
	} else if err != nil {
		return CloudClaimResult{}, err
	}
	if linkState != string(v2domain.LinkLinked) || accountState != string(v2domain.SourceAccountActive) || accountEpoch != prep.fixedSessionEpoch {
		return CloudClaimResult{}, ErrCloudClaimNotAllowed
	}

	var syncInventoryRevision int64
	if err := tx.QueryRowContext(ctx, `SELECT inventory_revision FROM client_sync_state WHERE client_id = ?`, request.ClientID).Scan(&syncInventoryRevision); errors.Is(err, sql.ErrNoRows) {
		return CloudClaimResult{}, ErrRevisionConflict
	} else if err != nil {
		return CloudClaimResult{}, err
	}
	var inventoryRevision int64
	if err := tx.QueryRowContext(ctx, `SELECT inventory_revision FROM client_source_inventory WHERE client_id = ? AND artifact_id = ?`, request.ClientID, prep.artifactID).Scan(&inventoryRevision); errors.Is(err, sql.ErrNoRows) {
		return CloudClaimResult{}, ErrInventoryIncompatible
	} else if err != nil {
		return CloudClaimResult{}, err
	}
	if syncInventoryRevision != int64(prep.fixedInventoryRevision) || inventoryRevision != int64(prep.fixedInventoryRevision) ||
		syncInventoryRevision != inventoryRevision || inventoryCompatibility != string(v2domain.CompatibilityCompatible) || inventoryRelease != prep.packageReleaseID {
		return CloudClaimResult{}, ErrCloudClaimNotAllowed
	}
	var activeReleaseCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM source_package_releases WHERE package_release_id = ? AND artifact_id = ? AND state = 'active'`, prep.packageReleaseID, prep.artifactID).Scan(&activeReleaseCount); err != nil {
		return CloudClaimResult{}, err
	}
	if activeReleaseCount != 1 {
		return CloudClaimResult{}, ErrCloudClaimNotAllowed
	}

	now := formatTime(r.db.Now())
	var currentRevision sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM client_cloud_claims WHERE client_id = ? AND source_account_id = ?`, request.ClientID, prep.sourceAccountID).Scan(&currentRevision); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return CloudClaimResult{}, err
	}
	newRevision := int64(1)
	if currentRevision.Valid {
		newRevision = currentRevision.Int64 + 1
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO client_cloud_claims(
			client_id, source_account_id, artifact_id, preparation_id, state,
			revision, activated_at, updated_at
		) VALUES(?, ?, ?, ?, 'active', ?, ?, ?)
		ON CONFLICT(client_id, source_account_id) DO UPDATE SET
			artifact_id = excluded.artifact_id,
			preparation_id = excluded.preparation_id,
			state = 'active', suspended_reason = NULL,
			revision = excluded.revision, updated_at = excluded.updated_at`,
		request.ClientID, prep.sourceAccountID, prep.artifactID, request.PreparationID, newRevision, now, now); err != nil {
		return CloudClaimResult{}, err
	}
	if result, err := tx.ExecContext(ctx, `UPDATE snapshot_receipts SET state = 'committed', committed_at = ? WHERE snapshot_receipt_id = ? AND state = 'issued'`, now, request.SnapshotReceiptID); err != nil {
		return CloudClaimResult{}, err
	} else if rows, err := result.RowsAffected(); err != nil {
		return CloudClaimResult{}, err
	} else if rows != 1 {
		return CloudClaimResult{}, ErrCloudSnapshotReceiptInvalid
	}
	if result, err := tx.ExecContext(ctx, `UPDATE cloud_mode_preparations SET state = 'committed', stage = 'done', revision = revision + 1, updated_at = ? WHERE preparation_id = ? AND client_id = ? AND revision = ?`, now, request.PreparationID, request.ClientID, request.ExpectedRevision); err != nil {
		return CloudClaimResult{}, err
	} else if rows, err := result.RowsAffected(); err != nil {
		return CloudClaimResult{}, err
	} else if rows != 1 {
		return CloudClaimResult{}, ErrRevisionConflict
	}
	result = CloudClaimResult{ClientID: request.ClientID, SourceAccountID: prep.sourceAccountID, ArtifactID: prep.artifactID, State: v2domain.ClaimActive, Revision: v2domain.Revision(newRevision)}
	if err := r.completeIdempotency(ctx, tx, "client", request.ClientID, "commit-cloud-claim", request.IdempotencyKey, 200, result); err != nil {
		return CloudClaimResult{}, err
	}
	return result, nil
}

func (r *Repository) DeleteCloudClaim(ctx context.Context, request DeleteCloudClaimRequest) error {
	if request.ClientID == "" || request.ArtifactID == "" || request.ExpectedRevision < 1 || request.IdempotencyKey == "" {
		return ErrInvalidArgument
	}
	return r.db.WriteTx(ctx, func(tx *Tx) error {
		_, err := r.DeleteCloudClaimTx(ctx, tx, request)
		return err
	})
}

// DeleteCloudClaimTx performs the claim suspension and idempotency mutation in
// the caller's transaction. The caller owns the transaction boundary.
func (r *Repository) DeleteCloudClaimTx(ctx context.Context, tx *Tx, request DeleteCloudClaimRequest) (CloudClaimResult, error) {
	if tx == nil || request.ClientID == "" || request.ArtifactID == "" || request.ExpectedRevision < 1 || request.IdempotencyKey == "" {
		return CloudClaimResult{}, ErrInvalidArgument
	}
	requestForDigest := struct {
		ClientID         string            `json:"clientId"`
		ArtifactID       string            `json:"artifactId"`
		SourceAccountID  string            `json:"sourceAccountId"`
		ExpectedRevision v2domain.Revision `json:"expectedRevision"`
	}{request.ClientID, request.ArtifactID, request.SourceAccountID, request.ExpectedRevision}
	var result CloudClaimResult
	replay, err := r.beginIdempotency(ctx, tx, "client", request.ClientID, "delete-cloud-claim", request.IdempotencyKey, requestForDigest)
	if err != nil {
		return CloudClaimResult{}, err
	}
	if replay != nil {
		if err := decodeIdempotencyReplay(replay, &result); err != nil {
			return CloudClaimResult{}, err
		}
		return result, nil
	}
	if err := ensureActiveClient(ctx, tx, request.ClientID); err != nil {
		return CloudClaimResult{}, err
	}
	accountID := request.SourceAccountID
	if accountID == "" {
		var selected string
		if err := tx.QueryRowContext(ctx, `SELECT source_account_id FROM client_source_links WHERE client_id = ? AND artifact_id = ? AND state = 'linked' AND selected_for_artifact = 1`, request.ClientID, request.ArtifactID).Scan(&selected); errors.Is(err, sql.ErrNoRows) {
			return CloudClaimResult{}, ErrCloudClaimNotFound
		} else if err != nil {
			return CloudClaimResult{}, err
		} else {
			accountID = selected
		}
	}
	var artifactID, state string
	var revision int64
	if err := tx.QueryRowContext(ctx, `SELECT artifact_id, state, revision FROM client_cloud_claims WHERE client_id = ? AND source_account_id = ?`, request.ClientID, accountID).Scan(&artifactID, &state, &revision); errors.Is(err, sql.ErrNoRows) {
		return CloudClaimResult{}, ErrCloudClaimNotFound
	} else if err != nil {
		return CloudClaimResult{}, err
	}
	if artifactID != request.ArtifactID {
		return CloudClaimResult{}, ErrCloudClaimNotFound
	}
	if v2domain.Revision(revision) != request.ExpectedRevision {
		return CloudClaimResult{}, ErrRevisionConflict
	}
	if state != string(v2domain.ClaimActive) {
		return CloudClaimResult{}, ErrCloudClaimNotFound
	}
	now := formatTime(r.db.Now())
	newRevision := request.ExpectedRevision + 1
	if result, err := tx.ExecContext(ctx, `UPDATE client_cloud_claims SET state = 'suspended', suspended_reason = 'clientRevoked', revision = ?, updated_at = ? WHERE client_id = ? AND source_account_id = ? AND revision = ?`, newRevision, now, request.ClientID, accountID, request.ExpectedRevision); err != nil {
		return CloudClaimResult{}, err
	} else if rows, err := result.RowsAffected(); err != nil {
		return CloudClaimResult{}, err
	} else if rows != 1 {
		return CloudClaimResult{}, ErrRevisionConflict
	}
	result = CloudClaimResult{ClientID: request.ClientID, SourceAccountID: accountID, ArtifactID: request.ArtifactID, State: v2domain.ClaimSuspended, Revision: newRevision}
	if err := r.completeIdempotency(ctx, tx, "client", request.ClientID, "delete-cloud-claim", request.IdempotencyKey, 200, result); err != nil {
		return CloudClaimResult{}, err
	}
	return result, nil
}

func (r *Repository) ListClientSourceStates(ctx context.Context, clientID string) ([]v2domain.SourceState, error) {
	if clientID == "" {
		return nil, ErrClientNotFound
	}
	var result []v2domain.SourceState
	err := r.db.ReadTx(ctx, func(tx *Tx) error {
		if err := ensureActiveClient(ctx, tx, clientID); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `
			SELECT a.artifact_id,
			       COALESCE(l.source_account_id, ''),
			       COALESCE(l.state, 'unlinked'),
			       COALESCE(c.state, ''),
			       COALESCE(ca.state, ''),
			       COALESCE(i.compatibility_state, 'unknown'),
			       COALESCE(ss.status, ''), ss.next_evaluation_at, ss.blocked_reason,
			       COALESCE(c.revision, l.revision, 1)
			FROM source_artifacts a
			LEFT JOIN client_source_inventory i ON i.client_id = ? AND i.artifact_id = a.artifact_id
			LEFT JOIN client_source_links l ON l.client_id = ? AND l.artifact_id = a.artifact_id
			  AND l.state = 'linked' AND l.selected_for_artifact = 1
			LEFT JOIN client_cloud_claims c ON c.client_id = ? AND c.artifact_id = a.artifact_id
			  AND c.state = 'active' AND c.source_account_id = l.source_account_id
			LEFT JOIN source_accounts ca ON ca.source_account_id = l.source_account_id
			LEFT JOIN source_scan_status ss ON ss.source_account_id = l.source_account_id
			ORDER BY a.artifact_id`, clientID, clientID, clientID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var state v2domain.SourceState
			var linkState, claimState, accountState, compatibility, sourceStatus string
			var nextEvaluation, blockedReason sql.NullString
			var revision int64
			if err := rows.Scan(&state.ArtifactID, &state.SelectedSourceAccountID, &linkState, &claimState,
				&accountState, &compatibility, &sourceStatus, &nextEvaluation, &blockedReason, &revision); err != nil {
				return err
			}
			state.LinkState = v2domain.LinkState(linkState)
			state.ClaimState = claimState
			state.CompatibilityState = v2domain.CompatibilityState(compatibility)
			state.SourceStatus = sourceStatus
			state.BlockedReason = blockedReason.String
			state.Revision = v2domain.Revision(revision)
			switch {
			case claimState == string(v2domain.ClaimActive):
				state.EffectiveState = "cloud"
			case accountState == string(v2domain.SourceAccountActive) && compatibility == string(v2domain.CompatibilityCompatible):
				state.EffectiveState = "eligible"
			case accountState != "":
				state.EffectiveState = accountState
			default:
				state.EffectiveState = "unlinked"
			}
			if nextEvaluation.Valid {
				parsed, parseErr := parseStoredTime(nextEvaluation.String)
				if parseErr != nil {
					return parseErr
				}
				state.NextEvaluationAt = &parsed
			}
			result = append(result, state)
		}
		return rows.Err()
	})
	return result, err
}

type preparationRow struct {
	id, clientID, sourceAccountID, artifactID, packageReleaseID string
	state, stage, expiresAt                                     string
	fixedSessionEpoch, fixedInventoryRevision, revision         int64
	blockedReason, snapshotReceiptID                            sql.NullString
}

func loadPreparation(ctx context.Context, tx *Tx, clientID, preparationID string) (preparationRow, error) {
	var row preparationRow
	err := tx.QueryRowContext(ctx, `
		SELECT preparation_id, client_id, source_account_id, artifact_id, package_release_id,
		       state, stage, fixed_session_epoch, fixed_inventory_revision, blocked_reason,
		       snapshot_receipt_id, expires_at, revision
		FROM cloud_mode_preparations WHERE preparation_id = ? AND client_id = ?`, preparationID, clientID).Scan(
		&row.id, &row.clientID, &row.sourceAccountID, &row.artifactID, &row.packageReleaseID,
		&row.state, &row.stage, &row.fixedSessionEpoch, &row.fixedInventoryRevision,
		&row.blockedReason, &row.snapshotReceiptID, &row.expiresAt, &row.revision)
	if err != nil {
		return preparationRow{}, err
	}
	return row, nil
}

func preparationRowToPublic(row preparationRow) CloudPreparation {
	return CloudPreparation{
		ID: row.id, ClientID: row.clientID, SourceAccountID: row.sourceAccountID, ArtifactID: row.artifactID,
		PackageReleaseID: row.packageReleaseID, State: v2domain.PreparationState(row.state), Stage: v2domain.PreparationStage(row.stage),
		FixedSessionEpoch: row.fixedSessionEpoch, FixedInventoryRevision: v2domain.Revision(row.fixedInventoryRevision),
		BlockedReason: row.blockedReason.String, SnapshotReceiptID: row.snapshotReceiptID.String,
		ExpiresAt: mustParseTime(row.expiresAt), Revision: v2domain.Revision(row.revision),
	}
}

func preparationExpired(preparation preparationRow, now time.Time) (bool, error) {
	expires, err := parseStoredTime(preparation.expiresAt)
	if err != nil {
		return false, err
	}
	return !expires.After(now), nil
}
