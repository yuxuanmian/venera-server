package v2sync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"venera-server/internal/v2crypto"
	"venera-server/internal/v2domain"
	"venera-server/internal/v2store"
)

var (
	ErrPreparationNotReady = errors.New("cloud preparation is not ready")
	ErrPreparationChanged  = errors.New("cloud preparation snapshot changed")
)

type PreparationCoordinator struct {
	repo *v2store.Repository
}

type PreparationSnapshotRequest struct {
	ClientID      string
	PreparationID string
	ReceiptID     string
}

type PreparationSnapshot struct {
	Data       []byte
	Digest     string
	HeaderHash string
	FooterHash string
	BaseSeq    int64
	BaseCursor string
	ReceiptID  string
}

type preparationTarget struct {
	ID                     string
	ClientID               string
	SourceAccountID        string
	ArtifactID             string
	PackageReleaseID       string
	State                  string
	Stage                  string
	FixedSessionEpoch      int64
	FixedInventoryRevision int64
	ExpiresAt              time.Time
	Revision               int64
}

func NewPreparationCoordinator(repo *v2store.Repository) *PreparationCoordinator {
	return &PreparationCoordinator{repo: repo}
}

// EvaluateReadiness advances every eligible preparation.  It is intentionally
// driven by publication/runtime work and never by a client GET request.
func (c *PreparationCoordinator) EvaluateReadiness(ctx context.Context) (int, error) {
	if c == nil || c.repo == nil {
		return 0, ErrPreparationNotReady
	}
	var targets []PreparationSnapshotRequest
	err := c.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT client_id, preparation_id FROM cloud_mode_preparations WHERE state = 'preparing' AND stage = 'waitSnapshot' AND expires_at > ? ORDER BY preparation_id`, syncTime(c.repo.DB().Now()))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var target PreparationSnapshotRequest
			if err := rows.Scan(&target.ClientID, &target.PreparationID); err != nil {
				return err
			}
			targets = append(targets, target)
		}
		return rows.Err()
	})
	if err != nil {
		return 0, err
	}
	ready := 0
	for _, target := range targets {
		advanced, err := c.EvaluatePreparation(ctx, target.ClientID, target.PreparationID)
		if err != nil {
			if errors.Is(err, ErrPreparationNotReady) || errors.Is(err, ErrPreparationChanged) {
				continue
			}
			return ready, err
		}
		if advanced {
			ready++
		}
	}
	return ready, nil
}

func (c *PreparationCoordinator) EvaluatePreparation(ctx context.Context, clientID, preparationID string) (bool, error) {
	if c == nil || c.repo == nil || clientID == "" || preparationID == "" {
		return false, ErrPreparationNotReady
	}
	var advanced bool
	err := c.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		prep, err := loadPreparationTargetTx(ctx, tx, clientID, preparationID)
		if errors.Is(err, sql.ErrNoRows) {
			return v2store.ErrCloudPreparationNotFound
		}
		if err != nil {
			return err
		}
		if !prep.ExpiresAt.After(tx.Now()) {
			return v2store.ErrCloudPreparationExpired
		}
		if prep.State == string(v2domain.PreparationSnapshotReady) {
			return nil
		}
		if prep.State != string(v2domain.PreparationPreparing) || prep.Stage != string(v2domain.PreparationWaitSnapshot) {
			return v2store.ErrCloudPreparationState
		}
		ready, err := c.checkReadinessTx(ctx, tx, prep)
		if err != nil {
			return err
		}
		if !ready {
			return ErrPreparationNotReady
		}
		receiptID := tx.NewID("receipt")
		snapshot, err := c.buildPreparationSnapshotTx(ctx, tx, prep, receiptID)
		if err != nil {
			return err
		}
		receiptExpiry := tx.Now().Add(SnapshotReceiptTTL)
		if prep.ExpiresAt.Before(receiptExpiry) {
			receiptExpiry = prep.ExpiresAt
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO snapshot_receipts(snapshot_receipt_id, client_id, source_account_id, artifact_id, scope_kind, package_release_id, fixed_session_epoch, base_change_seq, snapshot_digest, state, commit_expires_at, created_at) VALUES(?, ?, ?, ?, 'source', ?, ?, ?, ?, 'issued', ?, ?)`, receiptID, prep.ClientID, prep.SourceAccountID, prep.ArtifactID, prep.PackageReleaseID, prep.FixedSessionEpoch, snapshot.BaseSeq, snapshot.Digest, syncTime(receiptExpiry), syncTime(tx.Now())); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE cloud_mode_preparations SET state = 'snapshotReady', stage = 'deliverSnapshot', snapshot_receipt_id = ?, revision = revision + 1, updated_at = ? WHERE preparation_id = ? AND client_id = ? AND state = 'preparing' AND stage = 'waitSnapshot' AND revision = ?`, receiptID, syncTime(tx.Now()), prep.ID, prep.ClientID, prep.Revision)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return v2store.ErrRevisionConflict
		}
		advanced = true
		return nil
	})
	return advanced, err
}

func (c *PreparationCoordinator) BuildSnapshot(ctx context.Context, request PreparationSnapshotRequest) (PreparationSnapshot, error) {
	if c == nil || c.repo == nil || request.ClientID == "" || request.PreparationID == "" || request.ReceiptID == "" {
		return PreparationSnapshot{}, v2store.ErrCloudSnapshotReceiptInvalid
	}
	var snapshot PreparationSnapshot
	err := c.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		prep, err := loadPreparationTargetTx(ctx, tx, request.ClientID, request.PreparationID)
		if errors.Is(err, sql.ErrNoRows) {
			return v2store.ErrCloudPreparationNotFound
		}
		if err != nil {
			return err
		}
		if prep.State != string(v2domain.PreparationSnapshotReady) || request.ReceiptID == "" {
			return v2store.ErrCloudPreparationState
		}
		snapshot, err = c.buildPreparationSnapshotTx(ctx, tx, prep, request.ReceiptID)
		return err
	})
	return snapshot, err
}

func loadPreparationTargetTx(ctx context.Context, tx *v2store.Tx, clientID, preparationID string) (preparationTarget, error) {
	var target preparationTarget
	var expires string
	err := tx.QueryRowContext(ctx, `SELECT preparation_id, client_id, source_account_id, artifact_id, package_release_id, state, stage, fixed_session_epoch, fixed_inventory_revision, expires_at, revision FROM cloud_mode_preparations WHERE preparation_id = ? AND client_id = ?`, preparationID, clientID).Scan(&target.ID, &target.ClientID, &target.SourceAccountID, &target.ArtifactID, &target.PackageReleaseID, &target.State, &target.Stage, &target.FixedSessionEpoch, &target.FixedInventoryRevision, &expires, &target.Revision)
	if err != nil {
		return preparationTarget{}, err
	}
	target.ExpiresAt, err = time.Parse(time.RFC3339Nano, expires)
	return target, err
}

func (c *PreparationCoordinator) checkReadinessTx(ctx context.Context, tx *v2store.Tx, prep preparationTarget) (bool, error) {
	var clientState string
	if err := tx.QueryRowContext(ctx, `SELECT state FROM client_installations WHERE client_id = ?`, prep.ClientID).Scan(&clientState); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	if clientState != string(v2domain.ClientActive) {
		return false, nil
	}
	var accountState, accountArtifact, linkState string
	var accountEpoch, accountRevision, linkRevision int64
	if err := tx.QueryRowContext(ctx, `SELECT a.state, a.artifact_id, a.session_epoch, a.revision, l.state, l.revision FROM source_accounts a JOIN client_source_links l ON l.source_account_id = a.source_account_id AND l.artifact_id = a.artifact_id WHERE a.source_account_id = ? AND l.client_id = ? AND l.artifact_id = ? AND l.selected_for_artifact = 1`, prep.SourceAccountID, prep.ClientID, prep.ArtifactID).Scan(&accountState, &accountArtifact, &accountEpoch, &accountRevision, &linkState, &linkRevision); errors.Is(err, sql.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	if accountState != string(v2domain.SourceAccountActive) || accountArtifact != prep.ArtifactID || linkState != string(v2domain.LinkLinked) || accountEpoch != prep.FixedSessionEpoch {
		return false, nil
	}
	_ = accountRevision
	_ = linkRevision
	var inventoryRelease, inventoryCompatibility string
	var inventoryRevision, syncInventoryRevision int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(package_release_id, ''), compatibility_state, inventory_revision FROM client_source_inventory WHERE client_id = ? AND artifact_id = ?`, prep.ClientID, prep.ArtifactID).Scan(&inventoryRelease, &inventoryCompatibility, &inventoryRevision); errors.Is(err, sql.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT inventory_revision FROM client_sync_state WHERE client_id = ?`, prep.ClientID).Scan(&syncInventoryRevision); err != nil {
		return false, err
	}
	if inventoryRevision != prep.FixedInventoryRevision || syncInventoryRevision != prep.FixedInventoryRevision || inventoryRelease != prep.PackageReleaseID || inventoryCompatibility != string(v2domain.CompatibilityCompatible) {
		return false, nil
	}
	var releaseState, releaseArtifact string
	if err := tx.QueryRowContext(ctx, `SELECT state, artifact_id FROM source_package_releases WHERE package_release_id = ?`, prep.PackageReleaseID).Scan(&releaseState, &releaseArtifact); errors.Is(err, sql.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	if releaseState != "active" || releaseArtifact != prep.ArtifactID {
		return false, nil
	}
	var sessionEpoch int64
	var sessionExpires string
	if err := tx.QueryRowContext(ctx, `SELECT session_epoch, COALESCE(expires_at, '') FROM source_account_sessions WHERE source_account_id = ?`, prep.SourceAccountID).Scan(&sessionEpoch, &sessionExpires); err != nil {
		return false, err
	}
	if sessionEpoch != prep.FixedSessionEpoch || sessionExpires == "" {
		return false, nil
	}
	expires, err := time.Parse(time.RFC3339Nano, sessionExpires)
	if err != nil {
		return false, err
	}
	if !expires.After(tx.Now()) {
		return false, nil
	}

	var status, lastSnapshot, lastSuccess, nextEvaluation string
	if err := tx.QueryRowContext(ctx, `SELECT ss.status, COALESCE(a.last_snapshot_at, ''), COALESCE(ss.last_success_at, ''), COALESCE(ss.next_evaluation_at, '') FROM source_scan_status ss JOIN source_accounts a ON a.source_account_id = ss.source_account_id WHERE ss.source_account_id = ?`, prep.SourceAccountID).Scan(&status, &lastSnapshot, &lastSuccess, &nextEvaluation); err != nil {
		return false, nil
	}
	if status == "blocked" || status == "reauthRequired" || status == "paused" || lastSnapshot == "" || lastSuccess == "" || nextEvaluation == "" {
		return false, nil
	}
	next, err := time.Parse(time.RFC3339Nano, nextEvaluation)
	if err != nil || !next.After(tx.Now()) {
		return false, nil
	}
	var published int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM favorite_snapshot_runs WHERE source_account_id = ? AND package_release_id = ? AND session_epoch = ? AND state = 'published'`, prep.SourceAccountID, prep.PackageReleaseID, prep.FixedSessionEpoch).Scan(&published); err != nil {
		return false, err
	}
	if published < 1 {
		return false, nil
	}
	var observationContract, accountObservationContract string
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(observation_contract_id, ''), COALESCE(account_observation_contract_id, '') FROM source_package_releases WHERE package_release_id = ?`, prep.PackageReleaseID).Scan(&observationContract, &accountObservationContract); err != nil {
		return false, err
	}
	now := syncTime(tx.Now())
	var missing int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM tracking_interests i
		WHERE i.artifact_id = ? AND i.source_account_id = ? AND i.origin_kind = 'remoteSnapshot' AND i.state = 'active'
		  AND (NOT EXISTS (SELECT 1 FROM content_observations o WHERE o.artifact_id = i.artifact_id AND o.comic_id = i.comic_id AND o.visibility_scope = i.visibility_scope AND o.variant_key = i.variant_key AND o.observation_contract_id = ? AND o.status = 'fresh' AND o.fresh_until > ?)
		   OR NOT EXISTS (SELECT 1 FROM account_observations ao WHERE ao.source_account_id = i.source_account_id AND ao.comic_id = i.comic_id AND ao.account_observation_contract_id = ? AND ao.status = 'fresh' AND ao.fresh_until > ?))`, prep.ArtifactID, prep.SourceAccountID, observationContract, now, accountObservationContract, now).Scan(&missing); err != nil {
		return false, err
	}
	return missing == 0, nil
}

type preparationEntity struct {
	EntityType string
	EntityKey  string
	Revision   int64
	Payload    json.RawMessage
}

func (c *PreparationCoordinator) buildPreparationSnapshotTx(ctx context.Context, tx *v2store.Tx, prep preparationTarget, receiptID string) (PreparationSnapshot, error) {
	if receiptID == "" {
		return PreparationSnapshot{}, v2store.ErrCloudSnapshotReceiptInvalid
	}
	var baseSeq int64
	if err := tx.QueryRowContext(ctx, `SELECT high_change_seq FROM client_change_watermarks WHERE client_id = ?`, prep.ClientID).Scan(&baseSeq); err != nil {
		return PreparationSnapshot{}, err
	}
	var accountState, accountArtifact, scheme, display, attributes, scope string
	var accountEpoch, accountRevision int64
	if err := tx.QueryRowContext(ctx, `SELECT state, artifact_id, identity_scheme, identity_display, attributes_json, visibility_scope, session_epoch, revision FROM source_accounts WHERE source_account_id = ?`, prep.SourceAccountID).Scan(&accountState, &accountArtifact, &scheme, &display, &attributes, &scope, &accountEpoch, &accountRevision); err != nil {
		return PreparationSnapshot{}, err
	}
	if accountState != string(v2domain.SourceAccountActive) || accountArtifact != prep.ArtifactID || accountEpoch != prep.FixedSessionEpoch {
		return PreparationSnapshot{}, v2store.ErrCloudClaimNotAllowed
	}
	var inventoryRelease, inventoryCompatibility string
	var inventoryRevision int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(package_release_id, ''), compatibility_state, inventory_revision FROM client_source_inventory WHERE client_id = ? AND artifact_id = ?`, prep.ClientID, prep.ArtifactID).Scan(&inventoryRelease, &inventoryCompatibility, &inventoryRevision); err != nil {
		return PreparationSnapshot{}, v2store.ErrInventoryIncompatible
	}
	if inventoryRevision != prep.FixedInventoryRevision || inventoryRelease != prep.PackageReleaseID || inventoryCompatibility != string(v2domain.CompatibilityCompatible) {
		return PreparationSnapshot{}, v2store.ErrInventoryIncompatible
	}
	var releaseState, releaseArtifact string
	if err := tx.QueryRowContext(ctx, `SELECT state, artifact_id FROM source_package_releases WHERE package_release_id = ?`, prep.PackageReleaseID).Scan(&releaseState, &releaseArtifact); err != nil {
		return PreparationSnapshot{}, v2store.ErrPackageReleaseNotFound
	}
	if releaseState != "active" || releaseArtifact != prep.ArtifactID {
		return PreparationSnapshot{}, v2store.ErrPackageReleaseNotFound
	}
	entities := make([]preparationEntity, 0)
	accountPayload, err := json.Marshal(map[string]any{"sourceAccountId": prep.SourceAccountID, "artifactId": accountArtifact, "identity": map[string]any{"scheme": scheme, "display": display}, "attributes": safePreparationObject(attributes), "visibilityScope": scope, "state": accountState, "revision": accountRevision})
	if err != nil {
		return PreparationSnapshot{}, err
	}
	entities = append(entities, preparationEntity{EntityType: "sourceAccount", EntityKey: prep.SourceAccountID, Revision: accountRevision, Payload: accountPayload})
	rows, err := tx.QueryContext(ctx, `SELECT tracking_interest_id, comic_id, COALESCE(source_account_id, ''), origin_kind, origin_key, visibility_scope, variant_key, state, title, cover_url, revision FROM tracking_interests WHERE artifact_id = ? AND source_account_id = ? AND state = 'active' ORDER BY tracking_interest_id`, prep.ArtifactID, prep.SourceAccountID)
	if err != nil {
		return PreparationSnapshot{}, err
	}
	for rows.Next() {
		var id, comicID, sourceID, originKind, originKey, visibility, variant, state string
		var title, cover sql.NullString
		var revision int64
		if err := rows.Scan(&id, &comicID, &sourceID, &originKind, &originKey, &visibility, &variant, &state, &title, &cover, &revision); err != nil {
			rows.Close()
			return PreparationSnapshot{}, err
		}
		payload, err := json.Marshal(map[string]any{"trackingInterestId": id, "artifactId": prep.ArtifactID, "comicId": comicID, "sourceAccountId": sourceID, "origin": originKind, "originKey": originKey, "visibilityScope": visibility, "variantKey": variant, "state": state, "title": nullablePreparationString(title), "coverUrl": nullablePreparationString(cover), "revision": revision})
		if err != nil {
			rows.Close()
			return PreparationSnapshot{}, err
		}
		entities = append(entities, preparationEntity{EntityType: "trackingInterest", EntityKey: id, Revision: revision, Payload: payload})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return PreparationSnapshot{}, err
	}
	rows.Close()
	rows, err = tx.QueryContext(ctx, `SELECT DISTINCT o.content_observation_id, o.comic_id, o.visibility_scope, o.variant_key, o.observation_contract_id, o.marker_scheme, o.payload_json, o.status, o.observation_revision, o.validation_revision FROM content_observations o JOIN tracking_interests i ON i.artifact_id = o.artifact_id AND i.comic_id = o.comic_id AND i.visibility_scope = o.visibility_scope AND i.variant_key = o.variant_key AND i.state = 'active' AND i.source_account_id = ? WHERE o.artifact_id = ? ORDER BY o.content_observation_id`, prep.SourceAccountID, prep.ArtifactID)
	if err != nil {
		return PreparationSnapshot{}, err
	}
	for rows.Next() {
		var id, comicID, visibility, variant, contract, payloadText, status string
		var marker sql.NullString
		var observationRevision, validationRevision int64
		if err := rows.Scan(&id, &comicID, &visibility, &variant, &contract, &marker, &payloadText, &status, &observationRevision, &validationRevision); err != nil {
			rows.Close()
			return PreparationSnapshot{}, err
		}
		payload, err := json.Marshal(map[string]any{"contentObservationId": id, "artifactId": prep.ArtifactID, "comicId": comicID, "visibilityScope": visibility, "variantKey": variant, "observationContractId": contract, "markerScheme": nullablePreparationString(marker), "payload": safePreparationObject(payloadText), "status": status, "observationRevision": observationRevision, "validationRevision": validationRevision})
		if err != nil {
			rows.Close()
			return PreparationSnapshot{}, err
		}
		entities = append(entities, preparationEntity{EntityType: "contentObservation", EntityKey: id, Revision: observationRevision, Payload: payload})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return PreparationSnapshot{}, err
	}
	rows.Close()
	rows, err = tx.QueryContext(ctx, `SELECT account_observation_id, comic_id, account_observation_contract_id, payload_json, status, observation_revision, validation_revision FROM account_observations WHERE source_account_id = ? ORDER BY account_observation_id`, prep.SourceAccountID)
	if err != nil {
		return PreparationSnapshot{}, err
	}
	for rows.Next() {
		var id, comicID, contract, payloadText, status string
		var observationRevision, validationRevision int64
		if err := rows.Scan(&id, &comicID, &contract, &payloadText, &status, &observationRevision, &validationRevision); err != nil {
			rows.Close()
			return PreparationSnapshot{}, err
		}
		payload, err := json.Marshal(map[string]any{"accountObservationId": id, "sourceAccountId": prep.SourceAccountID, "comicId": comicID, "accountObservationContractId": contract, "payload": safePreparationObject(payloadText), "status": status, "observationRevision": observationRevision, "validationRevision": validationRevision})
		if err != nil {
			rows.Close()
			return PreparationSnapshot{}, err
		}
		entities = append(entities, preparationEntity{EntityType: "accountObservation", EntityKey: id, Revision: observationRevision, Payload: payload})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return PreparationSnapshot{}, err
	}
	rows.Close()
	var status string
	var blocked, lastSuccess, lastAttempt, nextEvaluation sql.NullString
	var statusRevision int64
	if err := tx.QueryRowContext(ctx, `SELECT status, blocked_reason, last_success_at, last_attempt_at, next_evaluation_at, revision FROM source_scan_status WHERE source_account_id = ?`, prep.SourceAccountID).Scan(&status, &blocked, &lastSuccess, &lastAttempt, &nextEvaluation, &statusRevision); err == nil {
		payload, marshalErr := json.Marshal(map[string]any{"sourceAccountId": prep.SourceAccountID, "artifactId": prep.ArtifactID, "status": status, "blockedReason": nullablePreparationString(blocked), "lastSuccessAt": nullablePreparationString(lastSuccess), "lastAttemptAt": nullablePreparationString(lastAttempt), "nextEvaluationAt": nullablePreparationString(nextEvaluation), "revision": statusRevision})
		if marshalErr != nil {
			return PreparationSnapshot{}, marshalErr
		}
		entities = append(entities, preparationEntity{EntityType: "sourceStatus", EntityKey: prep.SourceAccountID, Revision: statusRevision, Payload: payload})
	} else if !errors.Is(err, sql.ErrNoRows) {
		return PreparationSnapshot{}, err
	}
	sort.Slice(entities, func(i, j int) bool {
		if entities[i].EntityType != entities[j].EntityType {
			return entities[i].EntityType < entities[j].EntityType
		}
		return entities[i].EntityKey < entities[j].EntityKey
	})
	baseCursor, err := v2crypto.EncodeCursor(c.repo.Keys().CursorMAC, prep.ClientID, baseSeq)
	if err != nil {
		return PreparationSnapshot{}, err
	}
	header, err := preparationLine(map[string]any{"type": "header", "protocol": 2, "scope": "source", "artifactId": prep.ArtifactID, "sourceAccountId": prep.SourceAccountID, "baseCursor": baseCursor, "packageReleaseId": prep.PackageReleaseID})
	if err != nil {
		return PreparationSnapshot{}, err
	}
	var data bytes.Buffer
	data.Write(header)
	for _, entity := range entities {
		line, err := preparationLine(map[string]any{"type": "entity", "entityType": entity.EntityType, "entityKey": entity.EntityKey, "operation": "upsert", "revision": entity.Revision, "payload": entity.Payload})
		if err != nil {
			return PreparationSnapshot{}, err
		}
		data.Write(line)
	}
	partial := sha256.Sum256(data.Bytes())
	digest := "sha256:" + hex.EncodeToString(partial[:])
	footer, err := preparationLine(map[string]any{"type": "footer", "entityCount": len(entities), "snapshotDigest": digest, "snapshotReceipt": receiptID})
	if err != nil {
		return PreparationSnapshot{}, err
	}
	data.Write(footer)
	headerHash := sha256.Sum256(header)
	footerHash := sha256.Sum256(footer)
	return PreparationSnapshot{Data: append([]byte(nil), data.Bytes()...), Digest: digest, HeaderHash: hex.EncodeToString(headerHash[:]), FooterHash: hex.EncodeToString(footerHash[:]), BaseSeq: baseSeq, BaseCursor: baseCursor, ReceiptID: receiptID}, nil
}

func preparationLine(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(encoded) > SnapshotLineLimit {
		return nil, ErrSnapshotTooLarge
	}
	return append(encoded, '\n'), nil
}

func safePreparationObject(value string) json.RawMessage {
	if strings.TrimSpace(value) == "" || !json.Valid([]byte(value)) {
		return json.RawMessage(`{}`)
	}
	return json.RawMessage(value)
}

func nullablePreparationString(value sql.NullString) any {
	if !value.Valid || value.String == "" {
		return nil
	}
	return value.String
}
