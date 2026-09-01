package v2sync

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	"venera-server/internal/v2store"
)

var (
	ErrProjectionPayloadNotFound = errors.New("projection payload is no longer available")
	ErrProjectionInvalid         = errors.New("projection entity is invalid")
)

type ProjectionWriter struct {
	repo *v2store.Repository
}

func NewProjectionWriter(repo *v2store.Repository) *ProjectionWriter {
	return &ProjectionWriter{repo: repo}
}

func (p *ProjectionWriter) ProjectClient(ctx context.Context, clientID string) error {
	if p == nil || p.repo == nil || clientID == "" {
		return ErrProjectionInvalid
	}
	return p.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		return projectClientTx(ctx, tx, clientID)
	})
}

func projectClientTx(ctx context.Context, tx *v2store.Tx, clientID string) error {
	return projectClientTxWithFault(ctx, tx, clientID, nil)
}

func projectClientTxWithFault(ctx context.Context, tx *v2store.Tx, clientID string, fault func(string) error) error {
	rows, err := tx.QueryContext(ctx, `SELECT source_account_id, artifact_id FROM client_cloud_claims WHERE client_id = ? AND state = 'active' ORDER BY artifact_id, source_account_id`, clientID)
	if err != nil {
		return err
	}
	defer rows.Close()
	var claims [][2]string
	for rows.Next() {
		var claim [2]string
		if err := rows.Scan(&claim[0], &claim[1]); err != nil {
			return err
		}
		claims = append(claims, claim)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, claim := range claims {
		if err := projectClaimTx(ctx, tx, clientID, claim[0], claim[1]); err != nil {
			return err
		}
	}
	if err := injectProjectionFault(fault, "after-claim-projection"); err != nil {
		return err
	}
	if err := reconcileClientEntitiesTx(ctx, tx, clientID, claims, fault); err != nil {
		return err
	}
	return reconcileClientContentTx(ctx, tx, clientID)
}

func reconcileClientEntitiesTx(ctx context.Context, tx *v2store.Tx, clientID string, claims [][2]string, fault func(string) error) error {
	activeAccounts := make(map[string]struct{}, len(claims))
	for _, claim := range claims {
		activeAccounts[claim[0]] = struct{}{}
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT entity_type, entity_key, artifact_id, COALESCE(source_account_id, ''), entity_revision
		FROM client_entity_states
		WHERE client_id = ? AND operation = 'upsert' AND source_account_id IS NOT NULL
		ORDER BY entity_type, entity_key`, clientID)
	if err != nil {
		return err
	}
	type staleEntity struct {
		entityType      string
		entityKey       string
		artifactID      string
		sourceAccountID string
		revision        int64
	}
	var stale []staleEntity
	for rows.Next() {
		var entity staleEntity
		if err := rows.Scan(&entity.entityType, &entity.entityKey, &entity.artifactID, &entity.sourceAccountID, &entity.revision); err != nil {
			rows.Close()
			return err
		}
		if _, active := activeAccounts[entity.sourceAccountID]; !active {
			stale = append(stale, entity)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, entity := range stale {
		if err := deleteEntityTx(ctx, tx, clientID, entity.entityType, entity.entityKey, entity.artifactID, entity.sourceAccountID, entity.revision+1); err != nil {
			return err
		}
		if err := injectProjectionFault(fault, "after-entity-projection"); err != nil {
			return err
		}
	}
	return nil
}

func injectProjectionFault(fault func(string) error, point string) error {
	if fault == nil {
		return nil
	}
	return fault(point)
}

func reconcileClientContentTx(ctx context.Context, tx *v2store.Tx, clientID string) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT entity_key, artifact_id, entity_revision
		FROM client_entity_states
		WHERE client_id = ? AND entity_type = 'contentObservation' AND operation = 'upsert'
		ORDER BY entity_key`, clientID)
	if err != nil {
		return err
	}
	type contentEntity struct {
		entityKey  string
		artifactID string
		revision   int64
	}
	var entities []contentEntity
	for rows.Next() {
		var entity contentEntity
		if err := rows.Scan(&entity.entityKey, &entity.artifactID, &entity.revision); err != nil {
			rows.Close()
			return err
		}
		entities = append(entities, entity)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, entity := range entities {
		var visible int
		if err := tx.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM content_observations o
			JOIN tracking_interests i ON i.artifact_id = o.artifact_id AND i.comic_id = o.comic_id
			  AND i.visibility_scope = o.visibility_scope AND i.variant_key = o.variant_key AND i.state = 'active'
			JOIN client_cloud_claims c ON c.client_id = ? AND c.artifact_id = i.artifact_id
			  AND c.source_account_id = i.source_account_id AND c.state = 'active'
			JOIN client_source_links l ON l.client_id = c.client_id AND l.source_account_id = c.source_account_id
			  AND l.artifact_id = c.artifact_id AND l.state = 'linked'
			WHERE o.content_observation_id = ?`, clientID, entity.entityKey).Scan(&visible); err != nil {
			return err
		}
		if visible == 0 {
			if err := deleteEntityTx(ctx, tx, clientID, "contentObservation", entity.entityKey, entity.artifactID, "", entity.revision+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func projectClaimTx(ctx context.Context, tx *v2store.Tx, clientID, sourceAccountID, artifactID string) error {
	var accountRevision int64
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM source_accounts WHERE source_account_id = ? AND artifact_id = ?`, sourceAccountID, artifactID).Scan(&accountRevision); err != nil {
		return err
	}
	if err := upsertEntityTx(ctx, tx, clientID, "sourceAccount", sourceAccountID, artifactID, sourceAccountID, "upsert", accountRevision); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT tracking_interest_id, revision FROM tracking_interests WHERE source_account_id = ? AND artifact_id = ? AND state = 'active' ORDER BY tracking_interest_id`, sourceAccountID, artifactID)
	if err != nil {
		return err
	}
	for rows.Next() {
		var entityID string
		var revision int64
		if err := rows.Scan(&entityID, &revision); err != nil {
			rows.Close()
			return err
		}
		if err := upsertEntityTx(ctx, tx, clientID, "trackingInterest", entityID, artifactID, sourceAccountID, "upsert", revision); err != nil {
			rows.Close()
			return err
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	rows, err = tx.QueryContext(ctx, `SELECT account_observation_id, observation_revision FROM account_observations WHERE source_account_id = ? ORDER BY account_observation_id`, sourceAccountID)
	if err != nil {
		return err
	}
	for rows.Next() {
		var entityID string
		var revision int64
		if err := rows.Scan(&entityID, &revision); err != nil {
			rows.Close()
			return err
		}
		if err := upsertEntityTx(ctx, tx, clientID, "accountObservation", entityID, artifactID, sourceAccountID, "upsert", revision); err != nil {
			rows.Close()
			return err
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	if err := upsertEntityTx(ctx, tx, clientID, "sourceStatus", sourceAccountID, artifactID, sourceAccountID, "upsert", sourceStatusRevisionTx(ctx, tx, sourceAccountID)); err != nil {
		return err
	}
	var claimRevision int64
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM client_cloud_claims WHERE client_id = ? AND source_account_id = ? AND artifact_id = ? AND state = 'active'`, clientID, sourceAccountID, artifactID).Scan(&claimRevision); err != nil {
		return err
	}
	return upsertEntityTx(ctx, tx, clientID, "clientCloudState", artifactID, artifactID, sourceAccountID, "upsert", claimRevision)
}

func sourceStatusRevisionTx(ctx context.Context, tx *v2store.Tx, sourceAccountID string) int64 {
	var revision int64
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM source_scan_status WHERE source_account_id = ?`, sourceAccountID).Scan(&revision); err != nil || revision < 1 {
		return 1
	}
	return revision
}

func upsertEntityTx(ctx context.Context, tx *v2store.Tx, clientID, entityType, entityKey, artifactID, sourceAccountID, operation string, revision int64) error {
	if clientID == "" || entityType == "" || entityKey == "" || artifactID == "" || revision < 1 || (operation != "upsert" && operation != "delete") {
		return ErrProjectionInvalid
	}
	keyHash := projectionKeyHash(entityType, entityKey)
	var stateID, currentOperation string
	var currentRevision int64
	err := tx.QueryRowContext(ctx, `SELECT client_entity_state_id, operation, entity_revision FROM client_entity_states WHERE client_id = ? AND entity_type = ? AND key_hash = ?`, clientID, entityType, keyHash).Scan(&stateID, &currentOperation, &currentRevision)
	projectedRevision := revision
	if err == nil {
		if currentRevision >= revision && currentOperation == operation {
			return nil
		}
		if currentOperation == "delete" && operation == "upsert" && currentRevision >= revision {
			projectedRevision = currentRevision + 1
		} else if currentRevision > revision {
			return nil
		}
		_, err = tx.ExecContext(ctx, `UPDATE client_entity_states SET entity_key = ?, operation = ?, entity_revision = ?, source_account_id = ?, artifact_id = ?, updated_at = ? WHERE client_entity_state_id = ?`, entityKey, operation, projectedRevision, nullableProjectionString(sourceAccountID), artifactID, projectionNow(tx), stateID)
		if err != nil {
			return err
		}
	} else if errors.Is(err, sql.ErrNoRows) {
		stateID = projectionID(tx, "entity")
		_, err = tx.ExecContext(ctx, `INSERT INTO client_entity_states(client_entity_state_id, client_id, entity_type, entity_key, key_hash, operation, entity_revision, source_account_id, artifact_id, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, stateID, clientID, entityType, entityKey, keyHash, operation, revision, nullableProjectionString(sourceAccountID), artifactID, projectionNow(tx))
		if err != nil {
			return err
		}
	} else {
		return err
	}
	var high int64
	if err := tx.QueryRowContext(ctx, `SELECT high_change_seq FROM client_change_watermarks WHERE client_id = ?`, clientID).Scan(&high); err != nil {
		return err
	}
	seq := high + 1
	if _, err := tx.ExecContext(ctx, `INSERT INTO client_changes(client_id, client_change_seq, client_entity_state_id, entity_revision, created_at) VALUES(?, ?, ?, ?, ?)`, clientID, seq, stateID, projectedRevision, projectionNow(tx)); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE client_change_watermarks SET high_change_seq = ?, updated_at = ? WHERE client_id = ?`, seq, projectionNow(tx), clientID)
	return err
}

func deleteEntityTx(ctx context.Context, tx *v2store.Tx, clientID, entityType, entityKey, artifactID, sourceAccountID string, revision int64) error {
	return upsertEntityTx(ctx, tx, clientID, entityType, entityKey, artifactID, sourceAccountID, "delete", revision)
}

func entityPayloadTx(ctx context.Context, tx *v2store.Tx, entityType, entityKey, clientID, operation string) (json.RawMessage, error) {
	if operation == "delete" {
		return json.Marshal(map[string]any{"entityKey": entityKey})
	}
	switch entityType {
	case "sourceAccount":
		var artifactID, state, scheme, display, attributes, scope string
		var revision int64
		if err := tx.QueryRowContext(ctx, `SELECT artifact_id, state, identity_scheme, identity_display, attributes_json, visibility_scope, revision FROM source_accounts WHERE source_account_id = ?`, entityKey).Scan(&artifactID, &state, &scheme, &display, &attributes, &scope, &revision); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, ErrProjectionPayloadNotFound
			}
			return nil, err
		}
		return json.Marshal(struct {
			SourceAccountID string          `json:"sourceAccountId"`
			ArtifactID      string          `json:"artifactId"`
			Identity        accountIdentity `json:"identity"`
			Attributes      json.RawMessage `json:"attributes"`
			VisibilityScope string          `json:"visibilityScope"`
			State           string          `json:"state"`
			Revision        int64           `json:"revision"`
		}{entityKey, artifactID, accountIdentity{Scheme: scheme, Display: display}, rawObject(attributes), scope, state, revision})
	case "trackingInterest":
		var artifactID, comicID, sourceAccountID, originKind, originKey, scope, variant, state string
		var title, cover sql.NullString
		var revision int64
		if err := tx.QueryRowContext(ctx, `SELECT artifact_id, comic_id, COALESCE(source_account_id, ''), origin_kind, origin_key, visibility_scope, variant_key, state, title, cover_url, revision FROM tracking_interests WHERE tracking_interest_id = ?`, entityKey).Scan(&artifactID, &comicID, &sourceAccountID, &originKind, &originKey, &scope, &variant, &state, &title, &cover, &revision); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, ErrProjectionPayloadNotFound
			}
			return nil, err
		}
		return json.Marshal(map[string]any{"trackingInterestId": entityKey, "artifactId": artifactID, "comicId": comicID, "sourceAccountId": sourceAccountID, "origin": originKind, "originKey": originKey, "visibilityScope": scope, "variantKey": variant, "state": state, "title": nullableJSONValue(title), "coverUrl": nullableJSONValue(cover), "revision": revision})
	case "contentObservation":
		var artifactID, comicID, scope, variant, contract, marker sql.NullString
		var payload string
		var status string
		var observationRevision, validationRevision int64
		if err := tx.QueryRowContext(ctx, `SELECT artifact_id, comic_id, visibility_scope, variant_key, observation_contract_id, marker_scheme, payload_json, status, observation_revision, validation_revision FROM content_observations WHERE content_observation_id = ?`, entityKey).Scan(&artifactID, &comicID, &scope, &variant, &contract, &marker, &payload, &status, &observationRevision, &validationRevision); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, ErrProjectionPayloadNotFound
			}
			return nil, err
		}
		return json.Marshal(map[string]any{"contentObservationId": entityKey, "artifactId": artifactID.String, "comicId": comicID.String, "visibilityScope": scope.String, "variantKey": variant.String, "observationContractId": contract.String, "markerScheme": nullableJSONValue(marker), "payload": rawObject(payload), "status": status, "observationRevision": observationRevision, "validationRevision": validationRevision})
	case "accountObservation":
		var accountID, comicID, contract, payload, status string
		var observationRevision, validationRevision int64
		if err := tx.QueryRowContext(ctx, `SELECT source_account_id, comic_id, account_observation_contract_id, payload_json, status, observation_revision, validation_revision FROM account_observations WHERE account_observation_id = ?`, entityKey).Scan(&accountID, &comicID, &contract, &payload, &status, &observationRevision, &validationRevision); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, ErrProjectionPayloadNotFound
			}
			return nil, err
		}
		return json.Marshal(map[string]any{"accountObservationId": entityKey, "sourceAccountId": accountID, "comicId": comicID, "accountObservationContractId": contract, "payload": rawObject(payload), "status": status, "observationRevision": observationRevision, "validationRevision": validationRevision})
	case "sourceStatus":
		var artifactID, status string
		var blockedReason, lastSuccess, lastAttempt, nextEvaluation sql.NullString
		var revision int64
		if err := tx.QueryRowContext(ctx, `SELECT a.artifact_id, s.status, s.blocked_reason, s.last_success_at, s.last_attempt_at, s.next_evaluation_at, s.revision FROM source_scan_status s JOIN source_accounts a ON a.source_account_id = s.source_account_id WHERE s.source_account_id = ?`, entityKey).Scan(&artifactID, &status, &blockedReason, &lastSuccess, &lastAttempt, &nextEvaluation, &revision); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, ErrProjectionPayloadNotFound
			}
			return nil, err
		}
		return json.Marshal(map[string]any{"sourceAccountId": entityKey, "artifactId": artifactID, "status": status, "blockedReason": nullableJSONValue(blockedReason), "lastSuccessAt": nullableJSONValue(lastSuccess), "lastAttemptAt": nullableJSONValue(lastAttempt), "nextEvaluationAt": nullableJSONValue(nextEvaluation), "revision": revision})
	case "clientCloudState":
		var claimState, suspendedReason, compatibility, management string
		var accountID string
		var claimRevision int64
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(c.state, ''), COALESCE(c.suspended_reason, ''), COALESCE(c.source_account_id, ''), COALESCE(c.revision, 1), COALESCE(i.compatibility_state, 'unknown'), COALESCE(i.management_mode, 'notInstalled') FROM source_artifacts a LEFT JOIN client_cloud_claims c ON c.client_id = ? AND c.artifact_id = a.artifact_id AND c.state = 'active' LEFT JOIN client_source_inventory i ON i.client_id = ? AND i.artifact_id = a.artifact_id WHERE a.artifact_id = ?`, clientID, clientID, entityKey).Scan(&claimState, &suspendedReason, &accountID, &claimRevision, &compatibility, &management); err != nil {
			return nil, err
		}
		return json.Marshal(map[string]any{"artifactId": entityKey, "claimState": claimState, "suspendedReason": nullableStringJSON(suspendedReason), "sourceAccountId": nullableStringJSON(accountID), "compatibilityState": compatibility, "managementMode": management, "revision": claimRevision})
	default:
		return nil, ErrProjectionInvalid
	}
}

type accountIdentity struct {
	Scheme  string `json:"scheme"`
	Display string `json:"display"`
}

func rawObject(value string) json.RawMessage {
	if strings.TrimSpace(value) == "" || !json.Valid([]byte(value)) {
		return json.RawMessage(`{}`)
	}
	return json.RawMessage(value)
}

func nullableJSONValue(value sql.NullString) any {
	if !value.Valid || value.String == "" {
		return nil
	}
	return value.String
}

func nullableStringJSON(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func projectionKeyHash(entityType, entityKey string) string {
	sum := sha256.Sum256([]byte(entityType + "\x00" + entityKey))
	return hex.EncodeToString(sum[:])
}

func nullableProjectionString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func projectionNow(tx *v2store.Tx) string {
	return tx.Now().UTC().Format("2006-01-02T15:04:05.999999999Z07:00")
}

func projectionID(tx *v2store.Tx, prefix string) string {
	return tx.NewID(prefix)
}
