package v2sync

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"venera-server/internal/v2crypto"
	"venera-server/internal/v2domain"
	"venera-server/internal/v2store"
)

var (
	ErrInvalidPull      = errors.New("client change pull is invalid")
	ErrCursorInvalid    = errors.New("client change cursor is invalid")
	ErrResyncRequired   = errors.New("client change cursor is below the low watermark")
	ErrChangeByteBudget = errors.New("client change byte budget exceeded")
	ErrChangeNotFound   = errors.New("client change client not found")
	ErrAckInvalid       = errors.New("client change acknowledgement is invalid")
)

type Change struct {
	ChangeSeq  int64           `json:"changeSeq"`
	EntityType string          `json:"entityType"`
	EntityKey  string          `json:"entityKey"`
	Operation  string          `json:"operation"`
	Revision   int64           `json:"revision"`
	Payload    json.RawMessage `json:"payload"`
}

type ChangePage struct {
	Changes       []Change `json:"changes"`
	NextCursor    string   `json:"nextCursor"`
	HasMore       bool     `json:"hasMore"`
	HighWatermark int64    `json:"highWatermark"`
}

type ChangeStore struct {
	repo *v2store.Repository
}

// Store is the public v2 synchronization repository name used by the
// publication and HTTP layers.
type Store = ChangeStore

func NewChangeStore(repo *v2store.Repository) *ChangeStore {
	return &ChangeStore{repo: repo}
}

func NewStore(repo *v2store.Repository) *ChangeStore {
	return NewChangeStore(repo)
}

func (s *ChangeStore) PullClientChanges(ctx context.Context, clientID string, after v2domain.ChangeSeq, limit, byteBudget int) (ChangePage, error) {
	if s == nil || s.repo == nil || clientID == "" || after < 0 {
		return ChangePage{}, ErrInvalidPull
	}
	if limit == 0 {
		limit = 200
	}
	if byteBudget == 0 {
		byteBudget = 1 << 20
	}
	if limit < 1 || limit > 1000 || byteBudget < 1 || byteBudget > 4<<20 {
		return ChangePage{}, ErrInvalidPull
	}
	var page ChangePage
	err := s.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		var clientState string
		if err := tx.QueryRowContext(ctx, `SELECT state FROM client_installations WHERE client_id = ?`, clientID).Scan(&clientState); errors.Is(err, sql.ErrNoRows) {
			return ErrChangeNotFound
		} else if err != nil {
			return err
		} else if clientState != string(v2domain.ClientActive) {
			return v2store.ErrClientRevoked
		}
		var low, high int64
		if err := tx.QueryRowContext(ctx, `SELECT low_change_seq, high_change_seq FROM client_change_watermarks WHERE client_id = ?`, clientID).Scan(&low, &high); err != nil {
			return err
		}
		if int64(after) < low {
			return ErrResyncRequired
		}
		page.HighWatermark = high
		rows, err := tx.QueryContext(ctx, `
			SELECT c.client_change_seq, c.client_entity_state_id, c.entity_revision,
			       s.entity_type, s.entity_key, s.operation
			FROM client_changes c
			JOIN client_entity_states s ON s.client_entity_state_id = c.client_entity_state_id
			WHERE c.client_id = ? AND c.client_change_seq > ? AND c.client_change_seq <= ?
			ORDER BY c.client_change_seq`, clientID, int64(after), high)
		if err != nil {
			return err
		}
		defer rows.Close()
		type changeRow struct {
			seq, entityRevision                       int64
			stateID, entityType, entityKey, operation string
		}
		latest := make(map[string]changeRow)
		for rows.Next() {
			var item changeRow
			if err := rows.Scan(&item.seq, &item.stateID, &item.entityRevision, &item.entityType, &item.entityKey, &item.operation); err != nil {
				return err
			}
			latest[item.stateID] = item
		}
		if err := rows.Err(); err != nil {
			return err
		}
		ordered := make([]changeRow, 0, len(latest))
		for _, item := range latest {
			ordered = append(ordered, item)
		}
		sort.Slice(ordered, func(i, j int) bool { return ordered[i].seq < ordered[j].seq })
		page.Changes = make([]Change, 0, minInt(limit, len(ordered)))
		usedBytes := 0
		for index, item := range ordered {
			if len(page.Changes) >= limit {
				page.HasMore = true
				break
			}
			payload, payloadErr := entityPayloadTx(ctx, tx, item.entityType, item.entityKey, clientID, item.operation)
			if payloadErr != nil {
				return payloadErr
			}
			change := Change{ChangeSeq: item.seq, EntityType: item.entityType, EntityKey: item.entityKey, Operation: item.operation, Revision: item.entityRevision, Payload: payload}
			encoded, err := json.Marshal(change)
			if err != nil {
				return err
			}
			if len(encoded) > byteBudget || usedBytes+len(encoded) > byteBudget {
				if len(page.Changes) == 0 {
					return ErrChangeByteBudget
				}
				page.HasMore = true
				break
			}
			page.Changes = append(page.Changes, change)
			usedBytes += len(encoded)
			if index == len(ordered)-1 {
				page.HasMore = false
			}
		}
		nextSeq := high
		if page.HasMore && len(page.Changes) > 0 {
			nextSeq = page.Changes[len(page.Changes)-1].ChangeSeq
		}
		cursor, err := v2crypto.EncodeCursor(s.repo.Keys().CursorMAC, clientID, nextSeq)
		if err != nil {
			return err
		}
		page.NextCursor = cursor
		return nil
	})
	return page, err
}

func (s *ChangeStore) PullClientChangesCursor(ctx context.Context, clientID, cursor string, limit, byteBudget int) (ChangePage, error) {
	decoded, err := v2crypto.DecodeCursor(s.repo.Keys().CursorMAC, cursor, clientID)
	if err != nil {
		return ChangePage{}, ErrCursorInvalid
	}
	return s.PullClientChanges(ctx, clientID, v2domain.ChangeSeq(decoded.ChangeSeq), limit, byteBudget)
}

func (s *ChangeStore) InitialCursor(clientID string) (string, error) {
	if s == nil || s.repo == nil || clientID == "" {
		return "", ErrInvalidPull
	}
	return v2crypto.EncodeCursor(s.repo.Keys().CursorMAC, clientID, 0)
}

func (s *ChangeStore) AckClientChanges(ctx context.Context, clientID string, seq v2domain.ChangeSeq) error {
	if s == nil || s.repo == nil || clientID == "" || seq < 0 {
		return ErrAckInvalid
	}
	return s.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		var low, high, current int64
		if err := tx.QueryRowContext(ctx, `SELECT low_change_seq, high_change_seq FROM client_change_watermarks WHERE client_id = ?`, clientID).Scan(&low, &high); errors.Is(err, sql.ErrNoRows) {
			return ErrChangeNotFound
		} else if err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT last_ack_change_seq FROM client_sync_state WHERE client_id = ?`, clientID).Scan(&current); err != nil {
			return err
		}
		if int64(seq) < low || int64(seq) > high {
			return ErrAckInvalid
		}
		if int64(seq) <= current {
			return nil
		}
		_, err := tx.ExecContext(ctx, `UPDATE client_sync_state SET last_ack_change_seq = ?, last_pull_at = ?, updated_at = ? WHERE client_id = ?`, int64(seq), syncTime(s.repo.DB().Now()), syncTime(s.repo.DB().Now()), clientID)
		return err
	})
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func syncTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}
