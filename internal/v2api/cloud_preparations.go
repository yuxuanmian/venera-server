package v2api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"venera-server/internal/v2crypto"
	"venera-server/internal/v2domain"
	"venera-server/internal/v2store"
	"venera-server/internal/v2sync"
)

var (
	errCloudClientNotLinked = errors.New("client is not linked to the requested source account")
	errCloudSourceBlocked   = errors.New("source is not available for cloud tracking")
	errCloudScopeStale      = errors.New("source account scope is stale")
	errCloudSnapshotChanged = errors.New("cloud preparation snapshot changed")
)

type createCloudPreparationBody struct {
	ExpectedSourceAccountRevision *int64 `json:"expectedSourceAccountRevision"`
	ExpectedLinkRevision          *int64 `json:"expectedLinkRevision"`
	ExpectedInventoryRevision     *int64 `json:"expectedInventoryRevision"`
}

type cloudPreparationWire struct {
	PreparationID          string  `json:"preparationId"`
	ClientID               string  `json:"clientId"`
	SourceAccountID        string  `json:"sourceAccountId"`
	ArtifactID             string  `json:"artifactId"`
	PackageReleaseID       string  `json:"packageReleaseId"`
	State                  string  `json:"state"`
	Stage                  string  `json:"stage"`
	FixedSessionEpoch      int64   `json:"fixedSessionEpoch"`
	FixedInventoryRevision int64   `json:"fixedInventoryRevision"`
	BlockedReason          *string `json:"blockedReason"`
	SnapshotReceiptID      string  `json:"snapshotReceiptId,omitempty"`
	NextEvaluationAt       *string `json:"nextEvaluationAt"`
	ExpiresAt              string  `json:"expiresAt"`
	Revision               int64   `json:"revision"`
}

type preparationRecord struct {
	ID                     string
	ClientID               string
	SourceAccountID        string
	ArtifactID             string
	PackageReleaseID       string
	State                  string
	Stage                  string
	FixedSessionEpoch      int64
	FixedInventoryRevision int64
	BlockedReason          sql.NullString
	SnapshotReceiptID      sql.NullString
	NextEvaluationAt       sql.NullString
	ExpiresAt              time.Time
	Revision               int64
}

func (router *Router) createCloudPreparationHandler(w http.ResponseWriter, request *http.Request) {
	client, err := router.authenticateClient(request)
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	idempotencyKey, err := requireIdempotencyKey(request)
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	var body createCloudPreparationBody
	if err := decodeJSON(w, request, &body); err != nil || body.ExpectedSourceAccountRevision == nil || body.ExpectedLinkRevision == nil || body.ExpectedInventoryRevision == nil || *body.ExpectedSourceAccountRevision < 1 || *body.ExpectedLinkRevision < 1 || *body.ExpectedInventoryRevision < 1 {
		writeAPIError(router, w, request, http.StatusBadRequest, "invalid_request", "request is invalid", false, nil)
		return
	}
	accountID := strings.TrimSpace(request.PathValue("sourceAccountId"))
	artifactID, targetErr := router.validatePreparationTarget(request.Context(), string(client.ID), accountID)
	if targetErr != nil {
		writeCloudError(router, w, request, targetErr)
		return
	}
	preparation, err := router.repo.CreateCloudPreparation(request.Context(), v2store.CreateCloudPreparationRequest{
		ClientID: string(client.ID), ArtifactID: artifactID,
		ExpectedSourceAccountRevision: v2domain.Revision(*body.ExpectedSourceAccountRevision),
		ExpectedLinkRevision:          v2domain.Revision(*body.ExpectedLinkRevision),
		ExpectedInventoryRevision:     v2domain.Revision(*body.ExpectedInventoryRevision),
		TTL:                           24 * time.Hour, IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		writeCloudError(router, w, request, err)
		return
	}
	router.wakeRuntime()
	writeJSON(router, w, request, http.StatusCreated, cloudPreparationWireFromPreparation(preparation))
}

func (router *Router) getCloudPreparationHandler(w http.ResponseWriter, request *http.Request) {
	client, err := router.authenticateClient(request)
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	record, err := router.loadPreparation(request.Context(), string(client.ID), strings.TrimSpace(request.PathValue("preparationId")))
	if err != nil {
		writeCloudError(router, w, request, err)
		return
	}
	writeJSON(router, w, request, http.StatusOK, cloudPreparationWireFromRecord(record))
}

func (router *Router) getCloudPreparationSnapshotHandler(w http.ResponseWriter, request *http.Request) {
	client, err := router.authenticateClient(request)
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	clientID := string(client.ID)
	preparationID := strings.TrimSpace(request.PathValue("preparationId"))
	record, err := router.loadPreparation(request.Context(), clientID, preparationID)
	if err != nil {
		writeCloudError(router, w, request, err)
		return
	}
	if record.State != string(v2domain.PreparationSnapshotReady) {
		writeCloudError(router, w, request, v2store.ErrCloudPreparationState)
		return
	}

	receiptID := record.SnapshotReceiptID.String
	snapshot, err := v2sync.NewPreparationCoordinator(router.repo).BuildSnapshot(request.Context(), v2sync.PreparationSnapshotRequest{
		ClientID: clientID, PreparationID: preparationID, ReceiptID: receiptID,
	})
	if err != nil {
		writeCloudError(router, w, request, err)
		return
	}
	receipt, err := router.loadPreparationReceipt(request.Context(), clientID, record, receiptID)
	if err != nil {
		writeCloudError(router, w, request, err)
		return
	}
	if receipt.SnapshotDigest != snapshot.Digest {
		writeCloudError(router, w, request, errCloudSnapshotChanged)
		return
	}
	router.writeNDJSONSnapshot(w, normalizedSnapshotFromPreparation(snapshot))
}

func normalizedSnapshotFromPreparation(snapshot v2sync.PreparationSnapshot) normalizedSnapshot {
	return normalizedSnapshot{
		Data: snapshot.Data, Digest: snapshot.Digest, HeaderHash: snapshot.HeaderHash,
		FooterHash: snapshot.FooterHash, BaseSeq: snapshot.BaseSeq,
		BaseCursor: snapshot.BaseCursor, ReceiptID: snapshot.ReceiptID,
	}
}

type preparationReceipt struct {
	ID             string
	SnapshotDigest string
	State          string
	ExpiresAt      time.Time
}

func (router *Router) commitCloudPreparationHandler(w http.ResponseWriter, request *http.Request) {
	client, err := router.authenticateClient(request)
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	idempotencyKey, err := requireIdempotencyKey(request)
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	var body struct {
		ExpectedPreparationRevision *int64 `json:"expectedPreparationRevision"`
		SnapshotReceipt             string `json:"snapshotReceipt"`
		SnapshotDigest              string `json:"snapshotDigest"`
	}
	if err := decodeJSON(w, request, &body); err != nil || body.ExpectedPreparationRevision == nil || *body.ExpectedPreparationRevision < 1 || body.SnapshotReceipt == "" || body.SnapshotDigest == "" {
		writeAPIError(router, w, request, http.StatusBadRequest, "invalid_request", "request is invalid", false, nil)
		return
	}
	result, err := v2sync.NewCloudClaimCoordinator(router.repo).CommitCloudClaim(request.Context(), v2store.CommitCloudClaimRequest{
		ClientID: string(client.ID), PreparationID: strings.TrimSpace(request.PathValue("preparationId")),
		ExpectedRevision: v2domain.Revision(*body.ExpectedPreparationRevision), SnapshotReceiptID: body.SnapshotReceipt,
		SnapshotDigest: body.SnapshotDigest, IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		writeCloudError(router, w, request, err)
		return
	}
	cursor, err := currentCursorFor(router.repo, string(client.ID), result.HighChangeSeq)
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	router.wakeRuntime()
	writeJSON(router, w, request, http.StatusOK, map[string]any{
		"claimState": "active", "effectiveState": "active", "claimRevision": int64(result.Claim.Revision), "cursor": cursor,
	})
}

func (router *Router) cancelCloudPreparationHandler(w http.ResponseWriter, request *http.Request) {
	client, err := router.authenticateClient(request)
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	if _, err := requireIdempotencyKey(request); err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	var body struct {
		ExpectedRevision *int64 `json:"expectedRevision"`
	}
	decodeErr := decodeJSON(w, request, &body)
	if decodeErr != nil && !errors.Is(decodeErr, io.EOF) {
		writeAPIError(router, w, request, http.StatusBadRequest, "invalid_request", "request is invalid", false, nil)
		return
	}
	clientID := string(client.ID)
	preparationID := strings.TrimSpace(request.PathValue("preparationId"))
	record, err := router.loadPreparation(request.Context(), clientID, preparationID)
	if err != nil {
		writeCloudError(router, w, request, err)
		return
	}
	if body.ExpectedRevision != nil && (*body.ExpectedRevision < 1 || int64(record.Revision) != *body.ExpectedRevision) {
		writeCloudError(router, w, request, v2store.ErrRevisionConflict)
		return
	}
	if record.State == string(v2domain.PreparationCancelled) {
		writeJSON(router, w, request, http.StatusOK, map[string]any{"preparationId": preparationID, "state": record.State, "revision": record.Revision})
		return
	}
	if record.State != string(v2domain.PreparationPreparing) && record.State != string(v2domain.PreparationSnapshotReady) {
		writeCloudError(router, w, request, v2store.ErrCloudPreparationState)
		return
	}
	var cancelledState string
	var cancelledRevision int64
	err = router.repo.DB().WriteTx(request.Context(), func(tx *v2store.Tx) error {
		result, err := tx.ExecContext(request.Context(), `UPDATE cloud_mode_preparations SET state = 'cancelled', stage = 'done', revision = revision + 1, updated_at = ? WHERE preparation_id = ? AND client_id = ? AND state IN ('preparing','snapshotReady') AND revision = ?`, formatAPITime(router.repo.DB().Now()), preparationID, clientID, record.Revision)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if count != 1 {
			return v2store.ErrRevisionConflict
		}
		return tx.QueryRowContext(request.Context(), `SELECT state, revision FROM cloud_mode_preparations WHERE preparation_id = ? AND client_id = ?`, preparationID, clientID).Scan(&cancelledState, &cancelledRevision)
	})
	if err != nil {
		writeCloudError(router, w, request, err)
		return
	}
	router.wakeRuntime()
	writeJSON(router, w, request, http.StatusOK, map[string]any{"preparationId": preparationID, "state": cancelledState, "revision": cancelledRevision})
}

func (router *Router) deleteCloudClaimHandler(w http.ResponseWriter, request *http.Request) {
	client, err := router.authenticateClient(request)
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	idempotencyKey, err := requireIdempotencyKey(request)
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	accountID := strings.TrimSpace(request.PathValue("sourceAccountId"))
	var artifactID string
	if err := router.repo.DB().ReadTx(request.Context(), func(tx *v2store.Tx) error {
		return tx.QueryRowContext(request.Context(), `SELECT artifact_id FROM source_accounts WHERE source_account_id = ?`, accountID).Scan(&artifactID)
	}); errors.Is(err, sql.ErrNoRows) {
		writeCloudError(router, w, request, v2store.ErrCloudClaimNotFound)
		return
	} else if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	var body struct {
		ExpectedRevision *int64 `json:"expectedRevision"`
	}
	if err := decodeJSON(w, request, &body); err != nil || body.ExpectedRevision == nil || *body.ExpectedRevision < 1 {
		writeAPIError(router, w, request, http.StatusBadRequest, "invalid_request", "request is invalid", false, nil)
		return
	}
	claimResult, err := v2sync.NewCloudClaimCoordinator(router.repo).DeleteCloudClaim(request.Context(), v2store.DeleteCloudClaimRequest{
		ClientID: string(client.ID), ArtifactID: artifactID, SourceAccountID: accountID,
		ExpectedRevision: v2domain.Revision(*body.ExpectedRevision), IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		writeCloudError(router, w, request, err)
		return
	}
	cursor, err := currentCursorFor(router.repo, string(client.ID), claimResult.HighChangeSeq)
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	router.wakeRuntime()
	writeJSON(router, w, request, http.StatusOK, map[string]any{
		"claimState": "suspended", "effectiveState": "inactive", "claimRevision": int64(claimResult.Claim.Revision),
		"cursor": cursor, "regularLocalCheckSuggested": true,
	})
}

func (router *Router) loadPreparation(ctx context.Context, clientID, preparationID string) (preparationRecord, error) {
	if clientID == "" || preparationID == "" {
		return preparationRecord{}, v2store.ErrCloudPreparationNotFound
	}
	var record preparationRecord
	err := router.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		var expiresAt string
		err := tx.QueryRowContext(ctx, `
			SELECT preparation_id, client_id, source_account_id, artifact_id, package_release_id,
			       state, stage, fixed_session_epoch, fixed_inventory_revision,
			       blocked_reason, snapshot_receipt_id, next_evaluation_at, expires_at, revision
			FROM cloud_mode_preparations WHERE preparation_id = ? AND client_id = ?`, preparationID, clientID).Scan(
			&record.ID, &record.ClientID, &record.SourceAccountID, &record.ArtifactID, &record.PackageReleaseID,
			&record.State, &record.Stage, &record.FixedSessionEpoch, &record.FixedInventoryRevision,
			&record.BlockedReason, &record.SnapshotReceiptID, &record.NextEvaluationAt, &expiresAt, &record.Revision)
		if err != nil {
			return err
		}
		record.ExpiresAt, err = time.Parse(time.RFC3339Nano, expiresAt)
		return err
	})
	if errors.Is(err, sql.ErrNoRows) {
		return preparationRecord{}, v2store.ErrCloudPreparationNotFound
	}
	if err != nil {
		return preparationRecord{}, err
	}
	if !record.ExpiresAt.After(router.serverNow()) && record.State != string(v2domain.PreparationCommitted) && record.State != string(v2domain.PreparationCancelled) {
		return preparationRecord{}, v2store.ErrCloudPreparationExpired
	}
	return record, nil
}

func (router *Router) validatePreparationTarget(ctx context.Context, clientID, sourceAccountID string) (string, error) {
	if sourceAccountID == "" {
		return "", errCloudClientNotLinked
	}
	var artifactID, accountState, accountFresh, managedState, runtimeState, sessionExpires string
	err := router.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		return tx.QueryRowContext(ctx, `
			SELECT a.artifact_id, a.state, a.scope_fresh_until, sa.managed_state, COALESCE(rs.state, 'healthy'), COALESCE(s.expires_at, '')
			FROM source_accounts a
			JOIN client_source_links l ON l.source_account_id = a.source_account_id AND l.artifact_id = a.artifact_id
			JOIN source_artifacts sa ON sa.artifact_id = a.artifact_id
			LEFT JOIN source_account_sessions s ON s.source_account_id = a.source_account_id
			LEFT JOIN source_runtime_state rs ON rs.artifact_id = a.artifact_id
			WHERE a.source_account_id = ? AND l.client_id = ? AND l.state = 'linked' AND l.selected_for_artifact = 1`, sourceAccountID, clientID).Scan(&artifactID, &accountState, &accountFresh, &managedState, &runtimeState, &sessionExpires)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return "", errCloudClientNotLinked
	}
	if err != nil {
		return "", err
	}
	if managedState != "active" || runtimeState == "paused" || runtimeState == "quarantined" {
		return "", errCloudSourceBlocked
	}
	if accountState != string(v2domain.SourceAccountActive) {
		return "", v2store.ErrCloudClaimNotAllowed
	}
	freshUntil, err := time.Parse(time.RFC3339Nano, accountFresh)
	if err != nil || !freshUntil.After(router.serverNow()) {
		return "", errCloudScopeStale
	}
	if sessionExpires != "" {
		expires, parseErr := time.Parse(time.RFC3339Nano, sessionExpires)
		if parseErr != nil || !expires.After(router.serverNow()) {
			return "", errCloudScopeStale
		}
	}
	return artifactID, nil
}

func (router *Router) loadPreparationReceipt(ctx context.Context, clientID string, preparation preparationRecord, receiptID string) (preparationReceipt, error) {
	if receiptID == "" || preparation.SnapshotReceiptID.String != receiptID {
		return preparationReceipt{}, v2store.ErrCloudSnapshotReceiptInvalid
	}
	var result preparationReceipt
	var expiresAt string
	err := router.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT snapshot_receipt_id, snapshot_digest, state, commit_expires_at FROM snapshot_receipts WHERE snapshot_receipt_id = ? AND client_id = ? AND source_account_id = ? AND artifact_id = ?`, receiptID, clientID, preparation.SourceAccountID, preparation.ArtifactID).Scan(&result.ID, &result.SnapshotDigest, &result.State, &expiresAt)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return preparationReceipt{}, v2store.ErrCloudSnapshotReceiptInvalid
	}
	if err != nil {
		return preparationReceipt{}, err
	}
	result.ExpiresAt, err = time.Parse(time.RFC3339Nano, expiresAt)
	if err != nil {
		return preparationReceipt{}, err
	}
	if result.State != "issued" || !result.ExpiresAt.After(router.serverNow()) {
		return preparationReceipt{}, v2store.ErrCloudPreparationExpired
	}
	return result, nil
}

func safeJSONObject(value string) json.RawMessage {
	if strings.TrimSpace(value) == "" || !json.Valid([]byte(value)) {
		return json.RawMessage(`{}`)
	}
	return json.RawMessage(value)
}

func nullableAPIString(value sql.NullString) any {
	if !value.Valid || value.String == "" {
		return nil
	}
	return value.String
}

func nullableAPIStringString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func cloudPreparationWireFromRecord(record preparationRecord) cloudPreparationWire {
	var blocked *string
	if record.BlockedReason.Valid && record.BlockedReason.String != "" {
		value := record.BlockedReason.String
		blocked = &value
	}
	var next *string
	if record.NextEvaluationAt.Valid && record.NextEvaluationAt.String != "" {
		value := record.NextEvaluationAt.String
		next = &value
	}
	return cloudPreparationWire{
		PreparationID: record.ID, ClientID: record.ClientID, SourceAccountID: record.SourceAccountID,
		ArtifactID: record.ArtifactID, PackageReleaseID: record.PackageReleaseID, State: record.State,
		Stage: record.Stage, FixedSessionEpoch: record.FixedSessionEpoch,
		FixedInventoryRevision: record.FixedInventoryRevision, BlockedReason: blocked,
		SnapshotReceiptID: record.SnapshotReceiptID.String, NextEvaluationAt: next,
		ExpiresAt: record.ExpiresAt.UTC().Format(time.RFC3339Nano), Revision: record.Revision,
	}
}

func cloudPreparationWireFromPreparation(preparation v2store.CloudPreparation) cloudPreparationWire {
	var blocked *string
	if preparation.BlockedReason != "" {
		value := preparation.BlockedReason
		blocked = &value
	}
	return cloudPreparationWire{
		PreparationID: preparation.ID, ClientID: preparation.ClientID, SourceAccountID: preparation.SourceAccountID,
		ArtifactID: preparation.ArtifactID, PackageReleaseID: preparation.PackageReleaseID,
		State: string(preparation.State), Stage: string(preparation.Stage), FixedSessionEpoch: preparation.FixedSessionEpoch,
		FixedInventoryRevision: int64(preparation.FixedInventoryRevision), BlockedReason: blocked,
		SnapshotReceiptID: preparation.SnapshotReceiptID, ExpiresAt: preparation.ExpiresAt.UTC().Format(time.RFC3339Nano),
		Revision: int64(preparation.Revision),
	}
}

func currentCursorFor(repo *v2store.Repository, clientID string, sequence int64) (string, error) {
	return v2crypto.EncodeCursor(repo.Keys().CursorMAC, clientID, sequence)
}

func formatAPITime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func writeCloudError(router *Router, w http.ResponseWriter, request *http.Request, err error) {
	switch {
	case errors.Is(err, errCloudClientNotLinked), errors.Is(err, v2store.ErrSourceAccountNotSelected):
		writeAPIError(router, w, request, http.StatusForbidden, "client_not_linked", "client is not linked to this source", false, nil)
	case errors.Is(err, errCloudSourceBlocked), errors.Is(err, errSourceNotManaged):
		writeAPIError(router, w, request, http.StatusForbidden, "source_not_managed", "source is not available for cloud tracking", false, nil)
	case errors.Is(err, errCloudScopeStale):
		writeAPIError(router, w, request, http.StatusLocked, "source_account_reauth_required", "source account requires reauthentication", true, nil)
	case errors.Is(err, errCloudSnapshotChanged):
		writeAPIError(router, w, request, http.StatusConflict, "snapshot_changed", "snapshot changed; create a new preparation", false, nil)
	case errors.Is(err, v2store.ErrCloudPreparationExpired):
		writeAPIError(router, w, request, http.StatusGone, "preparation_expired", "preparation or receipt expired", false, nil)
	case errors.Is(err, v2store.ErrCloudPreparationState):
		writeAPIError(router, w, request, http.StatusConflict, "preparation_state_invalid", "preparation is not in the requested state", false, nil)
	case errors.Is(err, v2store.ErrCloudSnapshotReceiptInvalid):
		writeAPIError(router, w, request, http.StatusGone, "preparation_expired", "preparation or receipt expired", false, nil)
	case errors.Is(err, v2store.ErrCloudClaimNotAllowed):
		writeAPIError(router, w, request, http.StatusConflict, "preparation_required", "cloud preparation is required", false, nil)
	default:
		writeStoreError(router, w, request, err)
	}
}
