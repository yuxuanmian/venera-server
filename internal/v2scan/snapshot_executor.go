package v2scan

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"venera-server/internal/v2crypto"
	"venera-server/internal/v2store"
)

var (
	ErrSnapshotRunNotFound      = errors.New("favorite snapshot run not found")
	ErrSnapshotRunState         = errors.New("favorite snapshot run is not runnable")
	ErrSnapshotIdentityMismatch = errors.New("favorite snapshot run identity mismatch")
	ErrSnapshotInvalidSlice     = errors.New("favorite snapshot slice is invalid")
	ErrSnapshotDuplicateItem    = errors.New("favorite snapshot contains a duplicate item")
	ErrSnapshotIncomplete       = errors.New("favorite snapshot is incomplete")
	ErrSnapshotChanged          = errors.New("favorite snapshot changed during scan")
	ErrSnapshotItemLimit        = errors.New("favorite snapshot item limit exceeded")
)

const (
	DefaultSnapshotMaxItems      = 20000
	DefaultSnapshotMaxCheckpoint = 16 << 10
)

type SnapshotRun struct {
	ID                         string
	SourceAccountID            string
	PackageReleaseID           string
	SessionEpoch               int64
	RunGeneration              int64
	State                      string
	SnapshotValidationRevision int64
	ItemCount                  int
	StartedAt                  time.Time
	UpdatedAt                  time.Time
	CompletedAt                *time.Time
	FailureCode                string
}

type SnapshotRunRequest struct {
	SourceAccountID  string
	PackageReleaseID string
	SessionEpoch     int64
	RunGeneration    int64
}

type SnapshotItem struct {
	ComicID    string
	ItemDigest string
	ItemJSON   json.RawMessage
}

type SnapshotSliceResult struct {
	ExpectedTotal       *int
	Items               []SnapshotItem
	Complete            bool
	Checkpoint          json.RawMessage
	BoundaryDigest      string
	FinalBoundaryDigest string
}

// DecodeSnapshotSlice performs the host-side, strict decoding of the
// scanning extension result.  The returned ItemJSON is the canonical standard
// item shape consumed by the publisher; source-specific fields cannot pass
// through as an opaque object.
func DecodeSnapshotSlice(raw []byte) (SnapshotSliceResult, error) {
	if len(raw) == 0 || !json.Valid(raw) {
		return SnapshotSliceResult{}, ErrSnapshotInvalidSlice
	}
	var wire snapshotSliceWire
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return SnapshotSliceResult{}, ErrSnapshotInvalidSlice
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return SnapshotSliceResult{}, ErrSnapshotInvalidSlice
	}
	if wire.Complete == nil {
		return SnapshotSliceResult{}, ErrSnapshotInvalidSlice
	}
	if len(wire.SessionPatch) > 0 && string(wire.SessionPatch) != "null" {
		return SnapshotSliceResult{}, ErrSnapshotInvalidSlice
	}
	result := SnapshotSliceResult{ExpectedTotal: wire.ExpectedTotal, Complete: *wire.Complete, Checkpoint: append(json.RawMessage(nil), wire.Checkpoint...)}
	if result.Complete {
		if len(wire.Checkpoint) > 0 && string(wire.Checkpoint) != "null" {
			return SnapshotSliceResult{}, ErrSnapshotInvalidSlice
		}
	} else if len(wire.Checkpoint) == 0 || string(wire.Checkpoint) == "null" || !json.Valid(wire.Checkpoint) {
		return SnapshotSliceResult{}, ErrSnapshotInvalidSlice
	}
	if wire.Items == nil {
		return SnapshotSliceResult{}, ErrSnapshotInvalidSlice
	}
	result.Items = make([]SnapshotItem, 0, len(wire.Items))
	for _, item := range wire.Items {
		if strings.TrimSpace(item.ComicID) == "" || item.Membership == nil || item.Membership.Origin != "remoteSnapshot" || item.Membership.FolderID != "0" || item.Summary == nil || strings.TrimSpace(item.Summary.Title) == "" || item.ContentObservation == nil || item.AccountObservation == nil || item.AccountObservation.SourceUnread == nil || item.AccountObservation.SourceUnreadByVariant == nil {
			return SnapshotSliceResult{}, ErrSnapshotInvalidSlice
		}
		if item.ContentObservation.VisibilityScope == "" || item.ContentObservation.UpdateTime != nil || item.ContentObservation.MarkerEvidence == nil || item.ContentObservation.MarkerEvidence.Channel == "" || item.ContentObservation.MarkerEvidence.Scheme == "" || item.ContentObservation.MarkerEvidence.Value == "" {
			return SnapshotSliceResult{}, ErrSnapshotInvalidSlice
		}
		if _, ok := item.AccountObservation.SourceUnreadByVariant["normal"]; !ok {
			return SnapshotSliceResult{}, ErrSnapshotInvalidSlice
		}
		encoded, err := json.Marshal(item)
		if err != nil {
			return SnapshotSliceResult{}, ErrSnapshotInvalidSlice
		}
		result.Items = append(result.Items, SnapshotItem{ComicID: item.ComicID, ItemJSON: encoded})
	}
	return result, nil
}

type snapshotSliceWire struct {
	ExpectedTotal *int               `json:"expectedTotal"`
	Items         []snapshotItemWire `json:"items"`
	Complete      *bool              `json:"complete"`
	Checkpoint    json.RawMessage    `json:"checkpoint"`
	SessionPatch  json.RawMessage    `json:"sessionPatch"`
}

type snapshotItemWire struct {
	ComicID            string                          `json:"comicId"`
	Membership         *snapshotMembershipWire         `json:"membership"`
	Summary            *snapshotSummaryWire            `json:"summary"`
	ContentObservation *snapshotContentObservationWire `json:"contentObservation"`
	AccountObservation *snapshotAccountObservationWire `json:"accountObservation"`
}

type snapshotMembershipWire struct {
	Origin   string `json:"origin"`
	FolderID string `json:"folderId"`
}

type snapshotSummaryWire struct {
	Title    string  `json:"title"`
	Cover    *string `json:"cover"`
	Subtitle *string `json:"subtitle"`
}

type snapshotContentObservationWire struct {
	VisibilityScope string                  `json:"visibilityScope"`
	MarkerEvidence  *snapshotMarkerEvidence `json:"markerEvidence"`
	UpdateTime      *string                 `json:"updateTime"`
}

type snapshotMarkerEvidence struct {
	Channel string `json:"channel"`
	Scheme  string `json:"scheme"`
	Value   string `json:"value"`
}

type snapshotAccountObservationWire struct {
	SourceUnreadByVariant map[string]bool `json:"sourceUnreadByVariant"`
	SourceUnread          *bool           `json:"sourceUnread"`
}

func (e *SnapshotExecutor) ResumeCheckpoint(ctx context.Context, runID string) (json.RawMessage, error) {
	if e == nil || e.repo == nil || runID == "" {
		return nil, ErrSnapshotRunNotFound
	}
	var checkpoint json.RawMessage
	err := e.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		run, err := loadSnapshotRunTx(ctx, tx, runID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrSnapshotRunNotFound
		}
		if err != nil {
			return err
		}
		if run.State != "running" {
			return ErrSnapshotRunState
		}
		meta, err := e.loadCheckpoint(ctx, runID, run.CheckpointEnvelope)
		if err != nil {
			return err
		}
		if len(meta.Checkpoint) == 0 || string(meta.Checkpoint) == "null" {
			checkpoint = nil
		} else {
			checkpoint = append(json.RawMessage(nil), meta.Checkpoint...)
		}
		return nil
	})
	return checkpoint, err
}

// AbandonRun removes unpublished staging and makes the run ineligible for a
// later publication.  Callers that need a new scan generation advance the
// durable demand in the same operation that discards the leased job.
func (e *SnapshotExecutor) AbandonRun(ctx context.Context, runID, failureCode string) error {
	if e == nil || e.repo == nil || runID == "" {
		return ErrSnapshotRunNotFound
	}
	return e.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		now := formatSnapshotTime(e.repo.DB().Now())
		result, err := tx.ExecContext(ctx, `UPDATE favorite_snapshot_runs SET state = 'abandoned', failure_code = ?, checkpoint_envelope = NULL, updated_at = ? WHERE snapshot_run_id = ? AND state IN ('running','complete')`, nullableSnapshotString(failureCode), now, runID)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return ErrSnapshotRunNotFound
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM snapshot_staging_items WHERE snapshot_run_id = ?`, runID)
		return err
	})
}

type SnapshotExecutor struct {
	repo          *v2store.Repository
	maxItems      int
	maxCheckpoint int
}

func NewSnapshotExecutor(repo *v2store.Repository, maxItems, maxCheckpoint int) *SnapshotExecutor {
	if maxItems <= 0 {
		maxItems = DefaultSnapshotMaxItems
	}
	if maxCheckpoint <= 0 {
		maxCheckpoint = DefaultSnapshotMaxCheckpoint
	}
	return &SnapshotExecutor{repo: repo, maxItems: maxItems, maxCheckpoint: maxCheckpoint}
}

func (e *SnapshotExecutor) StartRun(ctx context.Context, request SnapshotRunRequest) (SnapshotRun, error) {
	if e == nil || e.repo == nil || request.SourceAccountID == "" || request.PackageReleaseID == "" || request.SessionEpoch < 1 || request.RunGeneration < 1 {
		return SnapshotRun{}, ErrSnapshotIdentityMismatch
	}
	var result SnapshotRun
	err := e.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		var accountState, accountArtifact string
		var currentEpoch int64
		if err := tx.QueryRowContext(ctx, `SELECT state, artifact_id, session_epoch FROM source_accounts WHERE source_account_id = ?`, request.SourceAccountID).Scan(&accountState, &accountArtifact, &currentEpoch); errors.Is(err, sql.ErrNoRows) {
			return ErrSnapshotRunNotFound
		} else if err != nil {
			return err
		}
		if accountState != "active" || currentEpoch != request.SessionEpoch {
			return ErrSnapshotIdentityMismatch
		}
		var releaseState string
		var releaseArtifact string
		if err := tx.QueryRowContext(ctx, `SELECT state, artifact_id FROM source_package_releases WHERE package_release_id = ?`, request.PackageReleaseID).Scan(&releaseState, &releaseArtifact); errors.Is(err, sql.ErrNoRows) {
			return ErrSnapshotIdentityMismatch
		} else if err != nil {
			return err
		} else if releaseState != "active" || accountArtifact != releaseArtifact {
			return ErrSnapshotIdentityMismatch
		}
		var existing SnapshotRun
		existingErr := scanSnapshotRun(tx.QueryRowContext(ctx, snapshotRunSelect+" WHERE source_account_id = ? AND run_generation = ?", request.SourceAccountID, request.RunGeneration), &existing)
		if existingErr == nil {
			if existing.PackageReleaseID != request.PackageReleaseID || existing.SessionEpoch != request.SessionEpoch || existing.State == "failed" || existing.State == "abandoned" {
				return ErrSnapshotIdentityMismatch
			}
			result = existing
			return nil
		}
		if !errors.Is(existingErr, sql.ErrNoRows) {
			return existingErr
		}
		now := e.repo.DB().Now()
		result = SnapshotRun{ID: tx.NewID("snapshot"), SourceAccountID: request.SourceAccountID, PackageReleaseID: request.PackageReleaseID, SessionEpoch: request.SessionEpoch, RunGeneration: request.RunGeneration, State: "running", StartedAt: now, UpdatedAt: now}
		_, err := tx.ExecContext(ctx, `
			INSERT INTO favorite_snapshot_runs(
				snapshot_run_id, source_account_id, package_release_id, session_epoch,
				run_generation, state, item_count, started_at, updated_at
			) VALUES(?, ?, ?, ?, ?, 'running', 0, ?, ?)`,
			result.ID, result.SourceAccountID, result.PackageReleaseID, result.SessionEpoch, result.RunGeneration,
			formatSnapshotTime(now), formatSnapshotTime(now))
		return err
	})
	return result, err
}

func (e *SnapshotExecutor) ApplySlice(ctx context.Context, runID string, slice SnapshotSliceResult) error {
	if e == nil || e.repo == nil || runID == "" {
		return ErrSnapshotRunNotFound
	}
	if slice.ExpectedTotal != nil && *slice.ExpectedTotal < 0 {
		return ErrSnapshotInvalidSlice
	}
	if slice.ExpectedTotal != nil && *slice.ExpectedTotal > e.maxItems {
		return ErrSnapshotItemLimit
	}
	if len(slice.Checkpoint) > e.maxCheckpoint {
		return ErrSnapshotInvalidSlice
	}
	if slice.Complete {
		if len(slice.Checkpoint) != 0 && string(slice.Checkpoint) != "null" {
			return ErrSnapshotInvalidSlice
		}
	} else if len(slice.Checkpoint) == 0 || string(slice.Checkpoint) == "null" || !json.Valid(slice.Checkpoint) {
		return ErrSnapshotInvalidSlice
	}
	for index := range slice.Items {
		item := slice.Items[index]
		if err := validateSnapshotItem(item); err != nil {
			return err
		}
		canonical, err := canonicalJSON(item.ItemJSON)
		if err != nil {
			return ErrSnapshotInvalidSlice
		}
		slice.Items[index].ItemJSON = canonical
		if slice.Items[index].ItemDigest == "" {
			slice.Items[index].ItemDigest = digestJSON(canonical)
		}
	}
	seen := make(map[string]struct{}, len(slice.Items))
	for _, item := range slice.Items {
		if _, ok := seen[item.ComicID]; ok {
			return ErrSnapshotDuplicateItem
		}
		seen[item.ComicID] = struct{}{}
	}

	return e.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		run, err := loadSnapshotRunTx(ctx, tx, runID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrSnapshotRunNotFound
		}
		if err != nil {
			return err
		}
		if run.State != "running" {
			return ErrSnapshotRunState
		}
		meta, err := e.loadCheckpoint(ctx, runID, run.CheckpointEnvelope)
		if err != nil {
			return err
		}
		if slice.ExpectedTotal != nil {
			if meta.ExpectedTotal != nil && *meta.ExpectedTotal != *slice.ExpectedTotal {
				return ErrSnapshotChanged
			}
			if meta.ExpectedTotal == nil {
				value := *slice.ExpectedTotal
				meta.ExpectedTotal = &value
			}
		} else if meta.ExpectedTotal != nil {
			return ErrSnapshotChanged
		}
		if slice.BoundaryDigest != "" {
			if meta.BoundaryDigest != "" && meta.BoundaryDigest != slice.BoundaryDigest {
				return ErrSnapshotChanged
			}
			if meta.BoundaryDigest == "" {
				meta.BoundaryDigest = slice.BoundaryDigest
			}
		}
		if slice.FinalBoundaryDigest != "" && meta.BoundaryDigest != "" && slice.FinalBoundaryDigest != meta.BoundaryDigest {
			return ErrSnapshotChanged
		}
		if meta.ExpectedTotal != nil && run.ItemCount+int64(len(slice.Items)) > int64(*meta.ExpectedTotal) {
			return ErrSnapshotDuplicateItem
		}
		if run.ItemCount+int64(len(slice.Items)) > int64(e.maxItems) {
			return ErrSnapshotItemLimit
		}
		for _, item := range slice.Items {
			_, err := tx.ExecContext(ctx, `INSERT INTO snapshot_staging_items(snapshot_run_id, comic_id, item_digest, item_json) VALUES(?, ?, ?, ?)`, runID, item.ComicID, item.ItemDigest, string(item.ItemJSON))
			if err != nil {
				if strings.Contains(strings.ToLower(err.Error()), "constraint") {
					return ErrSnapshotDuplicateItem
				}
				return err
			}
		}
		if slice.Complete {
			meta.Checkpoint = nil
		} else if len(slice.Checkpoint) > 0 && string(slice.Checkpoint) != "null" {
			meta.Checkpoint = append([]byte(nil), slice.Checkpoint...)
		}
		sealed, err := e.sealCheckpoint(runID, meta)
		if err != nil {
			return err
		}
		now := e.repo.DB().Now()
		_, err = tx.ExecContext(ctx, `UPDATE favorite_snapshot_runs SET checkpoint_envelope = ?, item_count = item_count + ?, updated_at = ? WHERE snapshot_run_id = ? AND state = 'running'`, sealed, len(slice.Items), formatSnapshotTime(now), runID)
		return err
	})
}

type snapshotCheckpoint struct {
	ExpectedTotal  *int            `json:"expectedTotal,omitempty"`
	BoundaryDigest string          `json:"boundaryDigest,omitempty"`
	Checkpoint     json.RawMessage `json:"checkpoint,omitempty"`
}

func (e *SnapshotExecutor) CompleteRun(ctx context.Context, runID, finalBoundaryDigest string) (SnapshotRun, error) {
	if e == nil || e.repo == nil || runID == "" {
		return SnapshotRun{}, ErrSnapshotRunNotFound
	}
	var result SnapshotRun
	err := e.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		run, err := loadSnapshotRunTx(ctx, tx, runID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrSnapshotRunNotFound
		}
		if err != nil {
			return err
		}
		if run.State != "running" {
			return ErrSnapshotRunState
		}
		meta, err := e.loadCheckpoint(ctx, runID, run.CheckpointEnvelope)
		if err != nil {
			return err
		}
		if meta.BoundaryDigest != "" && finalBoundaryDigest != meta.BoundaryDigest {
			return ErrSnapshotChanged
		}
		if len(meta.Checkpoint) > 0 && string(meta.Checkpoint) != "null" {
			return ErrSnapshotIncomplete
		}
		if meta.ExpectedTotal != nil && run.ItemCount != int64(*meta.ExpectedTotal) {
			return ErrSnapshotIncomplete
		}
		if meta.ExpectedTotal == nil && run.ItemCount == 0 && meta.BoundaryDigest == "" {
			return ErrSnapshotIncomplete
		}
		now := e.repo.DB().Now()
		if _, err := tx.ExecContext(ctx, `UPDATE favorite_snapshot_runs SET state = 'complete', checkpoint_envelope = NULL, completed_at = ?, updated_at = ? WHERE snapshot_run_id = ? AND state = 'running'`, formatSnapshotTime(now), formatSnapshotTime(now), runID); err != nil {
			return err
		}
		startedAt, err := time.Parse(time.RFC3339Nano, run.StartedAt)
		if err != nil {
			return err
		}
		result = SnapshotRun{
			ID: run.ID, SourceAccountID: run.SourceAccountID, PackageReleaseID: run.PackageReleaseID,
			SessionEpoch: run.SessionEpoch, RunGeneration: run.RunGeneration, State: "complete",
			SnapshotValidationRevision: run.ValidationRevision, ItemCount: int(run.ItemCount),
			StartedAt: startedAt, UpdatedAt: now, CompletedAt: &now,
			FailureCode: run.FailureCode,
		}
		return nil
	})
	return result, err
}

func (e *SnapshotExecutor) FailRun(ctx context.Context, runID, failureCode string, abandoned bool) error {
	state := "failed"
	if abandoned {
		state = "abandoned"
	}
	return e.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE favorite_snapshot_runs SET state = ?, failure_code = ?, updated_at = ? WHERE snapshot_run_id = ? AND state IN ('running','complete')`, state, nullableSnapshotString(failureCode), formatSnapshotTime(e.repo.DB().Now()), runID)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return ErrSnapshotRunNotFound
		}
		return nil
	})
}

func (e *SnapshotExecutor) GetRun(ctx context.Context, runID string) (SnapshotRun, error) {
	var result SnapshotRun
	err := e.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		return scanSnapshotRun(tx.QueryRowContext(ctx, snapshotRunSelect+" WHERE snapshot_run_id = ?", runID), &result)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return SnapshotRun{}, ErrSnapshotRunNotFound
	}
	return result, err
}

func (e *SnapshotExecutor) ListStagingItems(ctx context.Context, runID string) ([]SnapshotItem, error) {
	var result []SnapshotItem
	err := e.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT comic_id, item_digest, item_json FROM snapshot_staging_items WHERE snapshot_run_id = ? ORDER BY comic_id`, runID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var item SnapshotItem
			var itemJSON string
			if err := rows.Scan(&item.ComicID, &item.ItemDigest, &itemJSON); err != nil {
				return err
			}
			item.ItemJSON = json.RawMessage(itemJSON)
			result = append(result, item)
		}
		return rows.Err()
	})
	return result, err
}

func validateSnapshotItem(item SnapshotItem) error {
	if strings.TrimSpace(item.ComicID) == "" || len(item.ItemJSON) == 0 || !json.Valid(item.ItemJSON) {
		return ErrSnapshotInvalidSlice
	}
	canonical, err := canonicalJSON(item.ItemJSON)
	if err != nil {
		return ErrSnapshotInvalidSlice
	}
	if item.ItemDigest != "" && item.ItemDigest != digestJSON(canonical) {
		return ErrSnapshotInvalidSlice
	}
	return nil
}

func (e *SnapshotExecutor) loadCheckpoint(ctx context.Context, runID string, envelope []byte) (snapshotCheckpoint, error) {
	if len(envelope) == 0 {
		return snapshotCheckpoint{}, nil
	}
	plain, err := v2crypto.OpenSession(e.repo.Keys().SessionAEAD, envelope, []byte("snapshot:"+runID))
	if err != nil {
		return snapshotCheckpoint{}, err
	}
	var meta snapshotCheckpoint
	if err := json.Unmarshal(plain, &meta); err != nil {
		return snapshotCheckpoint{}, err
	}
	if len(meta.Checkpoint) > e.maxCheckpoint {
		return snapshotCheckpoint{}, ErrSnapshotInvalidSlice
	}
	return meta, nil
}

func (e *SnapshotExecutor) sealCheckpoint(runID string, meta snapshotCheckpoint) ([]byte, error) {
	encoded, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	return v2crypto.SealSession(e.repo.Keys().SessionAEAD, encoded, []byte("snapshot:"+runID))
}

const snapshotRunSelect = `SELECT snapshot_run_id, source_account_id, package_release_id, session_epoch,
       run_generation, state, snapshot_validation_revision, item_count, started_at,
       updated_at, COALESCE(completed_at, ''), COALESCE(failure_code, ''), checkpoint_envelope
       FROM favorite_snapshot_runs`

type snapshotRunScanner interface {
	Scan(dest ...any) error
}

type snapshotRunRow struct {
	ID, SourceAccountID, PackageReleaseID, State               string
	SessionEpoch, RunGeneration, ValidationRevision, ItemCount int64
	StartedAt, UpdatedAt, CompletedAt, FailureCode             string
	CheckpointEnvelope                                         []byte
}

func scanSnapshotRun(row snapshotRunScanner, result *SnapshotRun) error {
	var value snapshotRunRow
	if err := row.Scan(&value.ID, &value.SourceAccountID, &value.PackageReleaseID, &value.SessionEpoch, &value.RunGeneration, &value.State, &value.ValidationRevision, &value.ItemCount, &value.StartedAt, &value.UpdatedAt, &value.CompletedAt, &value.FailureCode, &value.CheckpointEnvelope); err != nil {
		return err
	}
	started, err := time.Parse(time.RFC3339Nano, value.StartedAt)
	if err != nil {
		return err
	}
	updated, err := time.Parse(time.RFC3339Nano, value.UpdatedAt)
	if err != nil {
		return err
	}
	result.ID = value.ID
	result.SourceAccountID = value.SourceAccountID
	result.PackageReleaseID = value.PackageReleaseID
	result.SessionEpoch = value.SessionEpoch
	result.RunGeneration = value.RunGeneration
	result.State = value.State
	result.SnapshotValidationRevision = value.ValidationRevision
	result.ItemCount = int(value.ItemCount)
	result.StartedAt = started
	result.UpdatedAt = updated
	result.FailureCode = value.FailureCode
	if value.CompletedAt != "" {
		completed, parseErr := time.Parse(time.RFC3339Nano, value.CompletedAt)
		if parseErr != nil {
			return parseErr
		}
		result.CompletedAt = &completed
	}
	return nil
}

func loadSnapshotRunTx(ctx context.Context, tx *v2store.Tx, runID string) (snapshotRunRow, error) {
	var value snapshotRunRow
	err := tx.QueryRowContext(ctx, snapshotRunSelect+" WHERE snapshot_run_id = ?", runID).Scan(&value.ID, &value.SourceAccountID, &value.PackageReleaseID, &value.SessionEpoch, &value.RunGeneration, &value.State, &value.ValidationRevision, &value.ItemCount, &value.StartedAt, &value.UpdatedAt, &value.CompletedAt, &value.FailureCode, &value.CheckpointEnvelope)
	return value, err
}

func formatSnapshotTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func nullableSnapshotString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
