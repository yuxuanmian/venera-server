package v2sync

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"venera-server/internal/v2store"
)

type PruneOptions struct {
	MaxAge  time.Duration
	MaxRows int
	Now     time.Time
}

type PruneResult struct {
	DeletedChanges  int
	AdvancedClients int
	ExpiredReceipts int
	DeletedStaging  int
}

type Pruner struct {
	repo *v2store.Repository
}

func NewPruner(repo *v2store.Repository) *Pruner {
	return &Pruner{repo: repo}
}

func (p *Pruner) PruneClientChanges(ctx context.Context, clientID string, options PruneOptions) (PruneResult, error) {
	if p == nil || p.repo == nil || clientID == "" {
		return PruneResult{}, ErrChangeNotFound
	}
	if options.MaxAge <= 0 {
		options.MaxAge = 14 * 24 * time.Hour
	}
	if options.MaxRows <= 0 {
		options.MaxRows = 250000
	}
	if options.Now.IsZero() {
		options.Now = p.repo.DB().Now()
	}
	var result PruneResult
	err := p.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		var low, high int64
		if err := tx.QueryRowContext(ctx, `SELECT low_change_seq, high_change_seq FROM client_change_watermarks WHERE client_id = ?`, clientID).Scan(&low, &high); errors.Is(err, sql.ErrNoRows) {
			return ErrChangeNotFound
		} else if err != nil {
			return err
		}
		cutoff := options.Now.Add(-options.MaxAge).Format(time.RFC3339Nano)
		var ageThreshold sql.NullInt64
		if err := tx.QueryRowContext(ctx, `SELECT MAX(client_change_seq) FROM client_changes WHERE client_id = ? AND created_at < ?`, clientID, cutoff).Scan(&ageThreshold); err != nil {
			return err
		}
		countThreshold := high - int64(options.MaxRows)
		threshold := low
		if ageThreshold.Valid && ageThreshold.Int64 > threshold {
			threshold = ageThreshold.Int64
		}
		if countThreshold > threshold {
			threshold = countThreshold
		}
		if threshold > high {
			threshold = high
		}
		var pinnedBase sql.NullInt64
		if err := tx.QueryRowContext(ctx, `SELECT MIN(base_change_seq) FROM snapshot_receipts WHERE client_id = ? AND state = 'issued' AND commit_expires_at > ?`, clientID, syncTime(options.Now)).Scan(&pinnedBase); err != nil {
			return err
		}
		if pinnedBase.Valid && pinnedBase.Int64 > 0 && threshold >= pinnedBase.Int64 {
			threshold = pinnedBase.Int64 - 1
		}
		if threshold > low {
			res, err := tx.ExecContext(ctx, `DELETE FROM client_changes WHERE client_id = ? AND client_change_seq <= ?`, clientID, threshold)
			if err != nil {
				return err
			}
			deleted, err := res.RowsAffected()
			if err != nil {
				return err
			}
			result.DeletedChanges = int(deleted)
			result.AdvancedClients = 1
			if _, err := tx.ExecContext(ctx, `UPDATE client_change_watermarks SET low_change_seq = ?, updated_at = ? WHERE client_id = ?`, threshold, syncTime(options.Now), clientID); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE client_sync_state SET resync_required = 1, resync_reason = 'change_watermark_advanced', updated_at = ? WHERE client_id = ?`, syncTime(options.Now), clientID); err != nil {
				return err
			}
		}
		return nil
	})
	return result, err
}

func (p *Pruner) ExpireReceipts(ctx context.Context, now time.Time) (int, error) {
	if p == nil || p.repo == nil {
		return 0, ErrChangeNotFound
	}
	if now.IsZero() {
		now = p.repo.DB().Now()
	}
	var count int
	err := p.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE snapshot_receipts SET state = 'expired' WHERE state = 'issued' AND commit_expires_at <= ?`, syncTime(now))
		if err != nil {
			return err
		}
		rows, err := res.RowsAffected()
		count = int(rows)
		return err
	})
	return count, err
}

func (p *Pruner) DeleteExpiredSnapshotStaging(ctx context.Context, before time.Time) (int, error) {
	if p == nil || p.repo == nil {
		return 0, ErrChangeNotFound
	}
	if before.IsZero() {
		before = p.repo.DB().Now().Add(-24 * time.Hour)
	}
	var count int
	err := p.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		res, err := tx.ExecContext(ctx, `DELETE FROM favorite_snapshot_runs WHERE state IN ('failed','abandoned','published') AND updated_at < ?`, syncTime(before))
		if err != nil {
			return err
		}
		rows, err := res.RowsAffected()
		count = int(rows)
		return err
	})
	return count, err
}
