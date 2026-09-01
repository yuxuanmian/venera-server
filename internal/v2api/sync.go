package v2api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"venera-server/internal/v2crypto"
	"venera-server/internal/v2store"
	"venera-server/internal/v2sync"
)

type pullChangesBody struct {
	Cursor     string `json:"cursor"`
	Limit      int    `json:"limit"`
	ByteBudget int    `json:"byteBudget"`
	WaitMS     int    `json:"waitMs"`
}

func (router *Router) pullChangesHandler(w http.ResponseWriter, request *http.Request) {
	client, err := router.authenticateClient(request)
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	var body pullChangesBody
	if err := decodeJSON(w, request, &body); err != nil || body.Cursor == "" || body.Limit < 0 || body.ByteBudget < 0 || body.WaitMS < 0 || body.WaitMS > 25000 {
		writeAPIError(router, w, request, http.StatusBadRequest, "invalid_request", "request is invalid", false, nil)
		return
	}

	store := v2sync.NewChangeStore(router.repo)
	deadline := time.Now().Add(time.Duration(body.WaitMS) * time.Millisecond)
	for {
		page, pullErr := store.PullClientChangesCursor(request.Context(), string(client.ID), body.Cursor, body.Limit, body.ByteBudget)
		if pullErr == nil && (len(page.Changes) > 0 || page.HasMore || body.WaitMS == 0 || !time.Now().Before(deadline)) {
			writeJSON(router, w, request, http.StatusOK, page)
			return
		}
		if pullErr != nil {
			router.writePullError(w, request, pullErr)
			return
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			writeJSON(router, w, request, http.StatusOK, page)
			return
		}
		interval := 100 * time.Millisecond
		if remaining < interval {
			interval = remaining
		}
		timer := time.NewTimer(interval)
		select {
		case <-request.Context().Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

func (router *Router) writePullError(w http.ResponseWriter, request *http.Request, err error) {
	switch {
	case errors.Is(err, v2sync.ErrCursorInvalid):
		writeAPIError(router, w, request, http.StatusBadRequest, "cursor_invalid", "cursor is invalid", false, nil)
	case errors.Is(err, v2sync.ErrResyncRequired):
		writeAPIError(router, w, request, http.StatusConflict, "resync_required", "a snapshot is required", false, map[string]any{"snapshotScope": "full"})
	case errors.Is(err, v2sync.ErrChangeByteBudget):
		writeAPIError(router, w, request, http.StatusBadRequest, "change_byte_budget_exceeded", "byte budget is too small", false, nil)
	case errors.Is(err, v2sync.ErrInvalidPull):
		writeAPIError(router, w, request, http.StatusBadRequest, "invalid_request", "request is invalid", false, nil)
	case errors.Is(err, v2sync.ErrChangeNotFound):
		writeAPIError(router, w, request, http.StatusNotFound, "resource_not_found", "resource not found", false, nil)
	default:
		writeStoreError(router, w, request, err)
	}
}

type snapshotRequestBody struct {
	Scope           string  `json:"scope"`
	ArtifactID      *string `json:"artifactId"`
	SourceAccountID *string `json:"sourceAccountId"`
	Reason          string  `json:"reason"`
}

func (router *Router) syncSnapshotHandler(w http.ResponseWriter, request *http.Request) {
	client, err := router.authenticateClient(request)
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	var body snapshotRequestBody
	if err := decodeJSON(w, request, &body); err != nil || (body.Scope != string(v2sync.SnapshotScopeFull) && body.Scope != string(v2sync.SnapshotScopeSource)) || len(body.Reason) > 200 {
		writeAPIError(router, w, request, http.StatusBadRequest, "invalid_request", "request is invalid", false, nil)
		return
	}
	artifactID, sourceAccountID := "", ""
	if body.ArtifactID != nil {
		artifactID = *body.ArtifactID
	}
	if body.SourceAccountID != nil {
		sourceAccountID = *body.SourceAccountID
	}
	if body.Scope == string(v2sync.SnapshotScopeSource) {
		if artifactID == "" || sourceAccountID == "" {
			writeAPIError(router, w, request, http.StatusBadRequest, "invalid_request", "source snapshot requires artifactId and sourceAccountId", false, nil)
			return
		}
	} else if artifactID != "" || sourceAccountID != "" {
		writeAPIError(router, w, request, http.StatusBadRequest, "invalid_request", "full snapshot cannot select a source account", false, nil)
		return
	}

	stream, err := v2sync.NewSnapshotStreamer(router.repo, v2sync.SnapshotDefaultMax).StreamClientSnapshot(request.Context(), v2sync.SnapshotRequest{
		ClientID: string(client.ID), Scope: v2sync.SnapshotScope(body.Scope), ArtifactID: artifactID, SourceAccountID: sourceAccountID, Reason: body.Reason,
	})
	if err != nil {
		switch {
		case errors.Is(err, v2sync.ErrSnapshotRequestInvalid):
			writeAPIError(router, w, request, http.StatusBadRequest, "invalid_request", "request is invalid", false, nil)
		case errors.Is(err, v2sync.ErrSnapshotAccessDenied):
			writeAPIError(router, w, request, http.StatusForbidden, "client_not_linked", "client is not linked to this source", false, nil)
		default:
			writeStoreError(router, w, request, err)
		}
		return
	}
	wire, err := normalizeProjectionSnapshot(router.repo, stream, v2sync.SnapshotScope(body.Scope), artifactID, sourceAccountID, body.Reason)
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	if err := router.updateSnapshotReceiptDigest(request.Context(), stream.Receipt.ID, wire.Digest); err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	router.writeNDJSONSnapshot(w, wire)
}

type normalizedSnapshot struct {
	Data       []byte
	Digest     string
	HeaderHash string
	FooterHash string
	BaseSeq    int64
	BaseCursor string
	ReceiptID  string
}

func normalizeProjectionSnapshot(repo *v2store.Repository, stream v2sync.SnapshotStream, scope v2sync.SnapshotScope, artifactID, sourceAccountID, reason string) (normalizedSnapshot, error) {
	if repo == nil || stream.Receipt.ID == "" || len(stream.Data) == 0 {
		return normalizedSnapshot{}, errors.New("snapshot stream is invalid")
	}
	lines := bytes.Split(bytes.TrimSuffix(stream.Data, []byte{'\n'}), []byte{'\n'})
	if len(lines) < 2 {
		return normalizedSnapshot{}, errors.New("snapshot stream is incomplete")
	}
	entities := make([]json.RawMessage, 0, len(lines)-2)
	for _, line := range lines[1 : len(lines)-1] {
		var value map[string]json.RawMessage
		if err := json.Unmarshal(line, &value); err != nil {
			return normalizedSnapshot{}, fmt.Errorf("decode snapshot entity: %w", err)
		}
		var kind string
		if err := json.Unmarshal(value["type"], &kind); err != nil || kind != "entity" {
			return normalizedSnapshot{}, errors.New("snapshot entity line is invalid")
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return normalizedSnapshot{}, err
		}
		entities = append(entities, encoded)
	}
	baseCursor, err := v2crypto.EncodeCursor(repo.Keys().CursorMAC, stream.Receipt.ClientID, stream.BaseSeq)
	if err != nil {
		return normalizedSnapshot{}, err
	}
	var artifactValue any
	var accountValue any
	var releaseValue any
	if scope == v2sync.SnapshotScopeSource {
		artifactValue, accountValue, releaseValue = artifactID, sourceAccountID, stream.Receipt.PackageReleaseID
	}
	header, err := snapshotLine(map[string]any{
		"type": "header", "protocol": 2, "scope": string(scope),
		"artifactId": artifactValue, "sourceAccountId": accountValue,
		"baseCursor": baseCursor, "packageReleaseId": releaseValue, "reason": reason,
	})
	if err != nil {
		return normalizedSnapshot{}, err
	}
	var data bytes.Buffer
	data.Write(header)
	for _, entity := range entities {
		data.Write(entity)
		data.WriteByte('\n')
		if data.Len() > v2sync.SnapshotDefaultMax {
			return normalizedSnapshot{}, v2sync.ErrSnapshotTooLarge
		}
	}
	partial := sha256.Sum256(data.Bytes())
	digest := "sha256:" + hex.EncodeToString(partial[:])
	footer, err := snapshotLine(map[string]any{
		"type": "footer", "entityCount": len(entities), "snapshotDigest": digest,
		"snapshotReceipt": stream.Receipt.ID,
	})
	if err != nil {
		return normalizedSnapshot{}, err
	}
	data.Write(footer)
	if data.Len() > v2sync.SnapshotDefaultMax {
		return normalizedSnapshot{}, v2sync.ErrSnapshotTooLarge
	}
	headerHash := sha256.Sum256(header)
	footerHash := sha256.Sum256(footer)
	return normalizedSnapshot{
		Data: append([]byte(nil), data.Bytes()...), Digest: digest,
		HeaderHash: hex.EncodeToString(headerHash[:]), FooterHash: hex.EncodeToString(footerHash[:]),
		BaseSeq: stream.BaseSeq, BaseCursor: baseCursor, ReceiptID: stream.Receipt.ID,
	}, nil
}

func snapshotLine(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(encoded) > v2sync.SnapshotLineLimit {
		return nil, v2sync.ErrSnapshotTooLarge
	}
	return append(encoded, '\n'), nil
}

func (router *Router) updateSnapshotReceiptDigest(ctx context.Context, receiptID, digest string) error {
	if receiptID == "" || digest == "" {
		return errors.New("snapshot receipt is invalid")
	}
	return router.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE snapshot_receipts SET snapshot_digest = ? WHERE snapshot_receipt_id = ? AND state = 'issued'`, digest, receiptID)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if count != 1 {
			return v2store.ErrCloudSnapshotReceiptInvalid
		}
		return nil
	})
}

func (router *Router) currentClientCursor(ctx context.Context, clientID string) (string, error) {
	var high int64
	err := router.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT high_change_seq FROM client_change_watermarks WHERE client_id = ?`, clientID).Scan(&high)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return "", v2store.ErrClientNotFound
	}
	if err != nil {
		return "", err
	}
	return v2crypto.EncodeCursor(router.repo.Keys().CursorMAC, clientID, high)
}

func (router *Router) writeNDJSONSnapshot(w http.ResponseWriter, snapshot normalizedSnapshot) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Venera-Snapshot-Digest", snapshot.Digest)
	w.Header().Set("X-Venera-Snapshot-Receipt", snapshot.ReceiptID)
	w.Header().Set("X-Venera-Snapshot-Base-Cursor", snapshot.BaseCursor)
	w.Header().Set("X-Venera-Snapshot-Header-Hash", snapshot.HeaderHash)
	w.Header().Set("X-Venera-Snapshot-Footer-Hash", snapshot.FooterHash)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(snapshot.Data)
}
