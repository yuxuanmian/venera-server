package v2sync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"time"

	"venera-server/internal/v2domain"
	"venera-server/internal/v2store"
)

var (
	ErrSnapshotRequestInvalid = errors.New("snapshot request is invalid")
	ErrSnapshotAccessDenied   = errors.New("snapshot is not visible to this client")
	ErrSnapshotTooLarge       = errors.New("snapshot exceeds the configured size limit")
)

const (
	SnapshotReceiptTTL = 15 * time.Minute
	SnapshotLineLimit  = 256 << 10
	SnapshotDefaultMax = 8 << 20
)

type SnapshotScope string

const (
	SnapshotScopeSource SnapshotScope = "source"
	SnapshotScopeFull   SnapshotScope = "full"
)

type SnapshotRequest struct {
	ClientID        string
	Scope           SnapshotScope
	ArtifactID      string
	SourceAccountID string
	Reason          string
}

type SnapshotReceipt struct {
	ID                string
	ClientID          string
	SourceAccountID   string
	ArtifactID        string
	Scope             SnapshotScope
	PackageReleaseID  string
	FixedSessionEpoch int64
	BaseChangeSeq     int64
	SnapshotDigest    string
	State             string
	CommitExpiresAt   time.Time
}

type SnapshotStream struct {
	Data       []byte
	Digest     string
	HeaderHash string
	FooterHash string
	BaseSeq    int64
	Receipt    SnapshotReceipt
}

type SnapshotStreamer struct {
	repo       *v2store.Repository
	maxBytes   int
	maxLine    int
	receiptTTL time.Duration
}

func NewSnapshotStreamer(repo *v2store.Repository, maxBytes int) *SnapshotStreamer {
	if maxBytes <= 0 {
		maxBytes = SnapshotDefaultMax
	}
	return &SnapshotStreamer{repo: repo, maxBytes: maxBytes, maxLine: SnapshotLineLimit, receiptTTL: SnapshotReceiptTTL}
}

func (s *SnapshotStreamer) StreamClientSnapshot(ctx context.Context, request SnapshotRequest) (SnapshotStream, error) {
	if s == nil || s.repo == nil || request.ClientID == "" || (request.Scope != SnapshotScopeSource && request.Scope != SnapshotScopeFull) {
		return SnapshotStream{}, ErrSnapshotRequestInvalid
	}
	if request.Scope == SnapshotScopeSource && (request.ArtifactID == "" || request.SourceAccountID == "") {
		return SnapshotStream{}, ErrSnapshotRequestInvalid
	}
	var stream SnapshotStream
	err := s.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		var clientState string
		if err := tx.QueryRowContext(ctx, `SELECT state FROM client_installations WHERE client_id = ?`, request.ClientID).Scan(&clientState); errors.Is(err, sql.ErrNoRows) {
			return ErrSnapshotAccessDenied
		} else if err != nil {
			return err
		} else if clientState != string(v2domain.ClientActive) {
			return v2store.ErrClientRevoked
		}
		if request.Scope == SnapshotScopeSource {
			var claimState string
			if err := tx.QueryRowContext(ctx, `SELECT state FROM client_cloud_claims WHERE client_id = ? AND artifact_id = ? AND source_account_id = ?`, request.ClientID, request.ArtifactID, request.SourceAccountID).Scan(&claimState); errors.Is(err, sql.ErrNoRows) {
				return ErrSnapshotAccessDenied
			} else if err != nil {
				return err
			} else if claimState != string(v2domain.ClaimActive) {
				return ErrSnapshotAccessDenied
			}
		}
		var low, high int64
		if err := tx.QueryRowContext(ctx, `SELECT low_change_seq, high_change_seq FROM client_change_watermarks WHERE client_id = ?`, request.ClientID).Scan(&low, &high); err != nil {
			return err
		}
		_ = low
		query := `SELECT entity_type, entity_key, operation, entity_revision FROM client_entity_states WHERE client_id = ? AND operation = 'upsert'`
		args := []any{request.ClientID}
		if request.Scope == SnapshotScopeSource {
			query += ` AND artifact_id = ? AND (source_account_id = ? OR entity_type IN ('contentObservation','clientCloudState'))`
			args = append(args, request.ArtifactID, request.SourceAccountID)
		} else {
			query += ` AND artifact_id IN (SELECT artifact_id FROM client_cloud_claims WHERE client_id = ? AND state = 'active')`
			args = append(args, request.ClientID)
		}
		query += ` ORDER BY entity_type, entity_key`
		rows, err := tx.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		type entityRow struct {
			EntityType, EntityKey, Operation string
			Revision                         int64
		}
		var entities []entityRow
		for rows.Next() {
			var entity entityRow
			if err := rows.Scan(&entity.EntityType, &entity.EntityKey, &entity.Operation, &entity.Revision); err != nil {
				return err
			}
			entities = append(entities, entity)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		header := map[string]any{"type": "header", "scope": string(request.Scope), "baseChangeSeq": high, "reason": request.Reason}
		headerLine, err := snapshotLine(header)
		if err != nil {
			return err
		}
		var data bytes.Buffer
		data.Write(headerLine)
		for _, entity := range entities {
			payload, err := entityPayloadTx(ctx, tx, entity.EntityType, entity.EntityKey, request.ClientID, entity.Operation)
			if err != nil {
				return err
			}
			line, err := snapshotLine(map[string]any{"type": "entity", "entityType": entity.EntityType, "entityKey": entity.EntityKey, "operation": entity.Operation, "revision": entity.Revision, "payload": payload})
			if err != nil {
				return err
			}
			data.Write(line)
			if data.Len() > s.maxBytes {
				return ErrSnapshotTooLarge
			}
		}
		partialDigest := sha256.Sum256(data.Bytes())
		footer := map[string]any{"type": "footer", "entityCount": len(entities), "baseChangeSeq": high, "digest": hex.EncodeToString(partialDigest[:])}
		footerLine, err := snapshotLine(footer)
		if err != nil {
			return err
		}
		data.Write(footerLine)
		if data.Len() > s.maxBytes {
			return ErrSnapshotTooLarge
		}
		allDigest := sha256.Sum256(data.Bytes())
		headerHash := sha256.Sum256(headerLine)
		footerHash := sha256.Sum256(footerLine)
		stream.Data = append([]byte(nil), data.Bytes()...)
		stream.Digest = hex.EncodeToString(allDigest[:])
		stream.HeaderHash = hex.EncodeToString(headerHash[:])
		stream.FooterHash = hex.EncodeToString(footerHash[:])
		stream.BaseSeq = high
		return nil
	})
	if err != nil {
		return SnapshotStream{}, err
	}
	now := s.repo.DB().Now()
	receipt := SnapshotReceipt{ID: s.repo.DB().NewID("receipt"), ClientID: request.ClientID, SourceAccountID: request.SourceAccountID, ArtifactID: request.ArtifactID, Scope: request.Scope, BaseChangeSeq: stream.BaseSeq, SnapshotDigest: stream.Digest, State: "issued", CommitExpiresAt: now.Add(s.receiptTTL)}
	if request.Scope == SnapshotScopeSource {
		var fixedEpoch int64
		var releaseID string
		err := s.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
			return tx.QueryRowContext(ctx, `SELECT a.session_epoch, r.package_release_id FROM client_cloud_claims c JOIN source_accounts a ON a.source_account_id = c.source_account_id JOIN source_package_releases r ON r.artifact_id = c.artifact_id AND r.state = 'active' WHERE c.client_id = ? AND c.artifact_id = ? AND c.source_account_id = ? AND c.state = 'active'`, request.ClientID, request.ArtifactID, request.SourceAccountID).Scan(&fixedEpoch, &releaseID)
		})
		if err != nil {
			return SnapshotStream{}, ErrSnapshotAccessDenied
		}
		receipt.FixedSessionEpoch = fixedEpoch
		receipt.PackageReleaseID = releaseID
	}
	err = s.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO snapshot_receipts(snapshot_receipt_id, client_id, source_account_id, artifact_id, scope_kind, package_release_id, fixed_session_epoch, base_change_seq, snapshot_digest, state, commit_expires_at, created_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'issued', ?, ?)`, receipt.ID, receipt.ClientID, nullableReceiptString(receipt.SourceAccountID), nullableReceiptString(receipt.ArtifactID), receipt.Scope, nullableReceiptString(receipt.PackageReleaseID), nullableReceiptInt(receipt.FixedSessionEpoch), receipt.BaseChangeSeq, receipt.SnapshotDigest, syncTime(receipt.CommitExpiresAt), syncTime(now))
		return err
	})
	if err != nil {
		return SnapshotStream{}, err
	}
	stream.Receipt = receipt
	return stream, nil
}

func (s *SnapshotStreamer) SnapshotReader(stream SnapshotStream) io.Reader {
	return bytes.NewReader(stream.Data)
}

func snapshotLine(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(encoded) > SnapshotLineLimit {
		return nil, ErrSnapshotTooLarge
	}
	return append(encoded, '\n'), nil
}

func nullableReceiptString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullableReceiptInt(value int64) any {
	if value <= 0 {
		return nil
	}
	return value
}
