package v2sync

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"venera-server/internal/v2domain"
	"venera-server/internal/v2scan"
	"venera-server/internal/v2store"
)

var (
	ErrPublishRunNotFound = errors.New("snapshot run is not ready for publication")
	ErrPublishStale       = errors.New("snapshot run identity is stale")
	ErrPublishInvalid     = errors.New("snapshot publication is invalid")
)

type PublishSnapshotRequest struct {
	RunID                string
	SourceAccountID      string
	PackageReleaseID     string
	ExpectedGeneration   int64
	ExpectedSessionEpoch int64
	FreshUntil           time.Time
}

type PublishResult struct {
	RunID                      string
	SourceAccountID            string
	PublishedItems             int
	AccountObservations        int
	ContentObservations        int
	ClientChanges              int
	SnapshotValidationRevision int64
}

type Publisher struct {
	repo *v2store.Repository
}

func NewPublisher(repo *v2store.Repository) *Publisher {
	return &Publisher{repo: repo}
}

func (p *Publisher) PublishSnapshot(ctx context.Context, request PublishSnapshotRequest) (PublishResult, error) {
	if p == nil || p.repo == nil || request.RunID == "" {
		return PublishResult{}, ErrPublishInvalid
	}
	var result PublishResult
	err := p.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		run, err := loadPublishRunTx(ctx, tx, request.RunID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrPublishRunNotFound
		}
		if err != nil {
			return err
		}
		if run.State != "complete" || (request.SourceAccountID != "" && request.SourceAccountID != run.SourceAccountID) || (request.PackageReleaseID != "" && request.PackageReleaseID != run.PackageReleaseID) || (request.ExpectedGeneration > 0 && request.ExpectedGeneration != run.RunGeneration) || (request.ExpectedSessionEpoch > 0 && request.ExpectedSessionEpoch != run.SessionEpoch) {
			return ErrPublishStale
		}
		var currentEpoch int64
		var accountState string
		if err := tx.QueryRowContext(ctx, `SELECT a.session_epoch, a.state FROM source_accounts a JOIN source_package_releases r ON r.artifact_id = a.artifact_id WHERE a.source_account_id = ? AND r.package_release_id = ?`, run.SourceAccountID, run.PackageReleaseID).Scan(&currentEpoch, &accountState); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrPublishStale
			}
			return err
		}
		if accountState != string(v2domain.SourceAccountActive) || currentEpoch != run.SessionEpoch {
			return ErrPublishStale
		}
		var artifactID string
		if err := tx.QueryRowContext(ctx, `SELECT artifact_id FROM source_package_releases WHERE package_release_id = ? AND state = 'active'`, run.PackageReleaseID).Scan(&artifactID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrPublishStale
			}
			return err
		}
		var demandState string
		var demandGeneration int64
		if err := tx.QueryRowContext(ctx, `SELECT state, execution_generation FROM scan_demands WHERE demand_key = ?`, "accountSnapshot\x00"+artifactID+"\x00"+run.SourceAccountID).Scan(&demandState, &demandGeneration); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrPublishStale
			}
			return err
		}
		if demandState != "active" || demandGeneration != run.RunGeneration {
			return ErrPublishStale
		}

		items, err := loadPublishItemsTx(ctx, tx, request.RunID)
		if err != nil {
			return err
		}
		if len(items) != int(run.ItemCount) {
			return ErrPublishInvalid
		}
		freshUntil := request.FreshUntil
		if freshUntil.IsZero() {
			freshUntil = tx.Now().Add(3 * time.Hour)
		}
		var observationContractID, accountObservationContractID, markerSchemesJSON string
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(observation_contract_id, ''), COALESCE(account_observation_contract_id, ''), marker_schemes_json FROM source_package_releases WHERE package_release_id = ?`, run.PackageReleaseID).Scan(&observationContractID, &accountObservationContractID, &markerSchemesJSON); err != nil {
			return err
		}
		var markerSchemes []string
		if err := json.Unmarshal([]byte(markerSchemesJSON), &markerSchemes); err != nil || len(markerSchemes) == 0 {
			return ErrPublishInvalid
		}
		var accountScope string
		if err := tx.QueryRowContext(ctx, `SELECT visibility_scope FROM source_accounts WHERE source_account_id = ?`, run.SourceAccountID).Scan(&accountScope); err != nil {
			return err
		}

		oldInterests, err := loadActiveRemoteInterestsTx(ctx, tx, artifactID, run.SourceAccountID)
		if err != nil {
			return err
		}
		present := make(map[string]struct{}, len(items))
		for _, item := range items {
			if item.Item.Membership.Origin != "remoteSnapshot" || item.Item.Membership.FolderID != "0" || item.Item.ComicID == "" || item.Item.Summary.Title == "" || item.Item.ContentObservation == nil || item.Item.ContentObservation.VisibilityScope != accountScope || item.Item.ContentObservation.MarkerEvidence == nil || item.Item.ContentObservation.MarkerEvidence.Channel == "" || item.Item.ContentObservation.MarkerEvidence.Scheme == "" || item.Item.ContentObservation.MarkerEvidence.Value == "" || !containsMarkerScheme(markerSchemes, item.Item.ContentObservation.MarkerEvidence.Scheme) {
				return ErrPublishInvalid
			}
			present[item.Item.ComicID] = struct{}{}
		}
		for _, old := range oldInterests {
			if _, ok := present[old.ComicID]; ok {
				continue
			}
			newRevision := old.Revision + 1
			if _, err := tx.ExecContext(ctx, `UPDATE tracking_interests SET state = 'inactive', revision = ?, updated_at = ? WHERE tracking_interest_id = ? AND state = 'active'`, newRevision, syncTime(tx.Now()), old.ID); err != nil {
				return err
			}
			if err := tombstoneInterestForClaimsTx(ctx, tx, old.ID, artifactID, run.SourceAccountID, newRevision); err != nil {
				return err
			}
		}

		for _, item := range items {
			interestID, interestRevision, err := upsertTrackingInterestTx(ctx, tx, artifactID, run.SourceAccountID, item, syncTime(tx.Now()))
			if err != nil {
				return err
			}
			_ = interestID
			_ = interestRevision
			if item.Item.AccountObservation != nil {
				if err := upsertAccountObservationTx(ctx, tx, run.SourceAccountID, item, accountObservationContractID, run.PackageReleaseID, freshUntil); err != nil {
					return err
				}
				result.AccountObservations++
			}
			if item.Item.ContentObservation != nil {
				if err := upsertContentObservationTx(ctx, tx, artifactID, item, observationContractID, run.PackageReleaseID, freshUntil); err != nil {
					return err
				}
				result.ContentObservations++
			}
		}

		now := tx.Now()
		if _, err := tx.ExecContext(ctx, `UPDATE source_accounts SET last_snapshot_at = ?, last_error_code = NULL, revision = revision + 1, updated_at = ? WHERE source_account_id = ?`, syncTime(now), syncTime(now), run.SourceAccountID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE source_scan_status SET status = 'idle', blocked_reason = NULL, last_success_at = ?, last_attempt_at = ?, next_evaluation_at = ?, revision = revision + 1, updated_at = ? WHERE source_account_id = ?`, syncTime(now), syncTime(now), syncTime(freshUntil), syncTime(now), run.SourceAccountID); err != nil {
			return err
		}
		validationRevision := run.ValidationRevision + 1
		if validationRevision < 1 {
			validationRevision = 1
		}
		if _, err := tx.ExecContext(ctx, `UPDATE favorite_snapshot_runs SET state = 'published', snapshot_validation_revision = ?, checkpoint_envelope = NULL, updated_at = ? WHERE snapshot_run_id = ? AND state = 'complete'`, validationRevision, syncTime(now), request.RunID); err != nil {
			return err
		}
		if err := projectPublishedAccountTx(ctx, tx, run.SourceAccountID, artifactID); err != nil {
			return err
		}
		result.RunID = run.ID
		result.SourceAccountID = run.SourceAccountID
		result.PublishedItems = len(items)
		result.SnapshotValidationRevision = validationRevision
		return nil
	})
	if err == nil {
		if _, planErr := v2scan.NewPlanner(p.repo, v2scan.PlannerOptions{Clock: p.repo.DB().Now}).Plan(ctx); planErr != nil {
			return result, planErr
		}
	}
	return result, err
}

func containsMarkerScheme(schemes []string, expected string) bool {
	for _, scheme := range schemes {
		if scheme == expected {
			return true
		}
	}
	return false
}

type publishRunRow struct {
	ID, SourceAccountID, PackageReleaseID, State               string
	SessionEpoch, RunGeneration, ValidationRevision, ItemCount int64
}

const publishRunSelect = `SELECT snapshot_run_id, source_account_id, package_release_id, state,
       session_epoch, run_generation, snapshot_validation_revision, item_count
       FROM favorite_snapshot_runs`

func loadPublishRunTx(ctx context.Context, tx *v2store.Tx, runID string) (publishRunRow, error) {
	var run publishRunRow
	err := tx.QueryRowContext(ctx, publishRunSelect+" WHERE snapshot_run_id = ?", runID).Scan(&run.ID, &run.SourceAccountID, &run.PackageReleaseID, &run.State, &run.SessionEpoch, &run.RunGeneration, &run.ValidationRevision, &run.ItemCount)
	return run, err
}

type publishMembership struct {
	Origin   string `json:"origin"`
	FolderID string `json:"folderId"`
}

type publishSummary struct {
	Title    string  `json:"title"`
	Cover    *string `json:"cover"`
	Subtitle *string `json:"subtitle"`
}

type publishMarkerEvidence struct {
	Channel string `json:"channel"`
	Scheme  string `json:"scheme"`
	Value   string `json:"value"`
}

type publishContentObservation struct {
	VisibilityScope string                 `json:"visibilityScope"`
	MarkerEvidence  *publishMarkerEvidence `json:"markerEvidence"`
	UpdateTime      *time.Time             `json:"updateTime"`
}

type publishAccountObservation struct {
	SourceUnreadByVariant map[string]bool `json:"sourceUnreadByVariant"`
	SourceUnread          bool            `json:"sourceUnread"`
}

type publishItem struct {
	ComicID            string                     `json:"comicId"`
	Membership         publishMembership          `json:"membership"`
	Summary            publishSummary             `json:"summary"`
	ContentObservation *publishContentObservation `json:"contentObservation"`
	AccountObservation *publishAccountObservation `json:"accountObservation"`
}

type publishItemRow struct {
	Item   publishItem
	Digest string
}

func loadPublishItemsTx(ctx context.Context, tx *v2store.Tx, runID string) ([]publishItemRow, error) {
	rows, err := tx.QueryContext(ctx, `SELECT comic_id, item_digest, item_json FROM snapshot_staging_items WHERE snapshot_run_id = ? ORDER BY comic_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []publishItemRow
	seen := make(map[string]struct{})
	for rows.Next() {
		var stagedComicID, digest, itemJSON string
		if err := rows.Scan(&stagedComicID, &digest, &itemJSON); err != nil {
			return nil, err
		}
		canonical, err := canonicalSyncJSON([]byte(itemJSON))
		if err != nil || digest != digestSyncJSON(canonical) {
			return nil, ErrPublishInvalid
		}
		var item publishItem
		if err := json.Unmarshal(canonical, &item); err != nil || item.ComicID == "" || item.ComicID != stagedComicID {
			return nil, ErrPublishInvalid
		}
		if _, ok := seen[item.ComicID]; ok {
			return nil, ErrPublishInvalid
		}
		seen[item.ComicID] = struct{}{}
		result = append(result, publishItemRow{Item: item, Digest: digest})
	}
	return result, rows.Err()
}

type remoteInterestRow struct {
	ID, ComicID string
	Revision    int64
}

func loadActiveRemoteInterestsTx(ctx context.Context, tx *v2store.Tx, artifactID, sourceAccountID string) ([]remoteInterestRow, error) {
	rows, err := tx.QueryContext(ctx, `SELECT tracking_interest_id, comic_id, revision FROM tracking_interests WHERE artifact_id = ? AND source_account_id = ? AND origin_kind = 'remoteSnapshot' AND origin_key = ? AND variant_key = '' AND state = 'active'`, artifactID, sourceAccountID, sourceAccountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []remoteInterestRow
	for rows.Next() {
		var item remoteInterestRow
		if err := rows.Scan(&item.ID, &item.ComicID, &item.Revision); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func upsertTrackingInterestTx(ctx context.Context, tx *v2store.Tx, artifactID, sourceAccountID string, item publishItemRow, now string) (string, int64, error) {
	var interestID string
	var revision int64
	var oldScope, oldTitle, oldCover sql.NullString
	var oldState string
	err := tx.QueryRowContext(ctx, `SELECT tracking_interest_id, revision, visibility_scope, state, title, cover_url FROM tracking_interests WHERE artifact_id = ? AND comic_id = ? AND origin_kind = 'remoteSnapshot' AND origin_key = ? AND variant_key = ''`, artifactID, item.Item.ComicID, sourceAccountID).Scan(&interestID, &revision, &oldScope, &oldState, &oldTitle, &oldCover)
	if errors.Is(err, sql.ErrNoRows) {
		interestID = "interest_" + item.Item.ComicID + "_" + sourceAccountID
		if len(interestID) > 180 {
			interestID = "interest_" + digestSyncString(artifactID+"\x00"+sourceAccountID+"\x00"+item.Item.ComicID)
		}
		revision = 1
		_, err = tx.ExecContext(ctx, `INSERT INTO tracking_interests(tracking_interest_id, artifact_id, comic_id, source_account_id, origin_kind, origin_key, visibility_scope, variant_key, state, title, cover_url, revision, created_at, updated_at) VALUES(?, ?, ?, ?, 'remoteSnapshot', ?, ?, '', 'active', ?, ?, 1, ?, ?)`, interestID, artifactID, item.Item.ComicID, sourceAccountID, sourceAccountID, item.Item.ContentObservation.VisibilityScope, nullablePublishString(item.Item.Summary.Title), nullablePublishPtr(item.Item.Summary.Cover), now, now)
		return interestID, revision, err
	}
	if err != nil {
		return "", 0, err
	}
	if oldState == "active" && oldScope.String == item.Item.ContentObservation.VisibilityScope && nullableSQLStringEqual(oldTitle, item.Item.Summary.Title) && nullableSQLStringPtrEqual(oldCover, item.Item.Summary.Cover) {
		return interestID, revision, nil
	}
	newRevision := revision + 1
	_, err = tx.ExecContext(ctx, `UPDATE tracking_interests SET visibility_scope = ?, state = 'active', title = ?, cover_url = ?, revision = ?, updated_at = ? WHERE tracking_interest_id = ?`, item.Item.ContentObservation.VisibilityScope, nullablePublishString(item.Item.Summary.Title), nullablePublishPtr(item.Item.Summary.Cover), newRevision, now, interestID)
	return interestID, newRevision, err
}

func nullableSQLStringEqual(value sql.NullString, expected string) bool {
	if expected == "" {
		return !value.Valid || value.String == ""
	}
	return value.Valid && value.String == expected
}

func nullableSQLStringPtrEqual(value sql.NullString, expected *string) bool {
	if expected == nil || *expected == "" {
		return !value.Valid || value.String == ""
	}
	return value.Valid && value.String == *expected
}

func upsertAccountObservationTx(ctx context.Context, tx *v2store.Tx, sourceAccountID string, item publishItemRow, contractID, releaseID string, freshUntil time.Time) error {
	if contractID == "" {
		return ErrPublishInvalid
	}
	payload, err := json.Marshal(item.Item.AccountObservation)
	if err != nil {
		return err
	}
	canonical, err := canonicalSyncJSON(payload)
	if err != nil {
		return ErrPublishInvalid
	}
	digest := digestSyncJSON(canonical)
	var id, oldDigest string
	var oldObservation, oldValidation int64
	err = tx.QueryRowContext(ctx, `SELECT account_observation_id, payload_digest, observation_revision, validation_revision FROM account_observations WHERE source_account_id = ? AND comic_id = ? AND account_observation_contract_id = ?`, sourceAccountID, item.Item.ComicID, contractID).Scan(&id, &oldDigest, &oldObservation, &oldValidation)
	now := tx.Now()
	if errors.Is(err, sql.ErrNoRows) {
		id = "account_obs_" + digestSyncString(sourceAccountID+"\x00"+item.Item.ComicID+"\x00"+contractID)
		_, err = tx.ExecContext(ctx, `INSERT INTO account_observations(account_observation_id, source_account_id, comic_id, account_observation_contract_id, package_release_id, payload_digest, payload_json, status, observation_revision, validation_revision, observed_at, validated_at, fresh_until, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, 'fresh', 1, 1, ?, ?, ?, ?)`, id, sourceAccountID, item.Item.ComicID, contractID, releaseID, digest, string(canonical), syncTime(now), syncTime(now), syncTime(freshUntil), syncTime(now))
		return err
	}
	if err != nil {
		return err
	}
	observationRevision := oldObservation
	observedAt := now
	if oldDigest != digest {
		observationRevision++
	} else {
		var observedText string
		if err := tx.QueryRowContext(ctx, `SELECT observed_at FROM account_observations WHERE account_observation_id = ?`, id).Scan(&observedText); err == nil {
			if parsed, parseErr := time.Parse(time.RFC3339Nano, observedText); parseErr == nil {
				observedAt = parsed
			}
		}
	}
	_, err = tx.ExecContext(ctx, `UPDATE account_observations SET package_release_id = ?, payload_digest = ?, payload_json = ?, status = 'fresh', observation_revision = ?, validation_revision = ?, observed_at = ?, validated_at = ?, fresh_until = ?, updated_at = ? WHERE account_observation_id = ?`, releaseID, digest, string(canonical), observationRevision, oldValidation+1, syncTime(observedAt), syncTime(now), syncTime(freshUntil), syncTime(now), id)
	return err
}

func upsertContentObservationTx(ctx context.Context, tx *v2store.Tx, artifactID string, item publishItemRow, contractID, releaseID string, freshUntil time.Time) error {
	if contractID == "" || item.Item.ContentObservation == nil || item.Item.ContentObservation.MarkerEvidence == nil {
		return ErrPublishInvalid
	}
	payload, err := json.Marshal(item.Item.ContentObservation)
	if err != nil {
		return err
	}
	canonical, err := canonicalSyncJSON(payload)
	if err != nil {
		return ErrPublishInvalid
	}
	digest := digestSyncJSON(canonical)
	var id, oldDigest string
	var oldObservation, oldValidation int64
	err = tx.QueryRowContext(ctx, `SELECT content_observation_id, payload_digest, observation_revision, validation_revision FROM content_observations WHERE artifact_id = ? AND comic_id = ? AND visibility_scope = ? AND variant_key = '' AND observation_contract_id = ?`, artifactID, item.Item.ComicID, item.Item.ContentObservation.VisibilityScope, contractID).Scan(&id, &oldDigest, &oldObservation, &oldValidation)
	now := tx.Now()
	if errors.Is(err, sql.ErrNoRows) {
		id = "content_obs_" + digestSyncString(artifactID+"\x00"+item.Item.ComicID+"\x00"+item.Item.ContentObservation.VisibilityScope+"\x00"+contractID)
		_, err = tx.ExecContext(ctx, `INSERT INTO content_observations(content_observation_id, artifact_id, comic_id, visibility_scope, variant_key, observation_contract_id, package_release_id, marker_scheme, payload_digest, payload_json, status, observation_revision, validation_revision, observed_at, validated_at, fresh_until, updated_at) VALUES(?, ?, ?, ?, '', ?, ?, ?, ?, ?, 'fresh', 1, 1, ?, ?, ?, ?)`, id, artifactID, item.Item.ComicID, item.Item.ContentObservation.VisibilityScope, contractID, releaseID, nullablePublishString(item.Item.ContentObservation.MarkerEvidence.Scheme), digest, string(canonical), syncTime(now), syncTime(now), syncTime(freshUntil), syncTime(now))
		return err
	}
	if err != nil {
		return err
	}
	observationRevision := oldObservation
	observedAt := now
	if oldDigest != digest {
		observationRevision++
	} else {
		var observedText string
		if err := tx.QueryRowContext(ctx, `SELECT observed_at FROM content_observations WHERE content_observation_id = ?`, id).Scan(&observedText); err == nil {
			if parsed, parseErr := time.Parse(time.RFC3339Nano, observedText); parseErr == nil {
				observedAt = parsed
			}
		}
	}
	_, err = tx.ExecContext(ctx, `UPDATE content_observations SET package_release_id = ?, marker_scheme = ?, payload_digest = ?, payload_json = ?, status = 'fresh', observation_revision = ?, validation_revision = ?, observed_at = ?, validated_at = ?, fresh_until = ?, updated_at = ? WHERE content_observation_id = ?`, releaseID, nullablePublishString(item.Item.ContentObservation.MarkerEvidence.Scheme), digest, string(canonical), observationRevision, oldValidation+1, syncTime(observedAt), syncTime(now), syncTime(freshUntil), syncTime(now), id)
	return err
}

func projectPublishedAccountTx(ctx context.Context, tx *v2store.Tx, sourceAccountID, artifactID string) error {
	rows, err := tx.QueryContext(ctx, `SELECT client_id FROM client_cloud_claims WHERE source_account_id = ? AND artifact_id = ? AND state = 'active' ORDER BY client_id`, sourceAccountID, artifactID)
	if err != nil {
		return err
	}
	defer rows.Close()
	var clients []string
	for rows.Next() {
		var clientID string
		if err := rows.Scan(&clientID); err != nil {
			return err
		}
		clients = append(clients, clientID)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, clientID := range clients {
		if err := projectClaimTx(ctx, tx, clientID, sourceAccountID, artifactID); err != nil {
			return err
		}
	}
	return projectContentForInterestedClientsTx(ctx, tx, artifactID)
}

func tombstoneInterestForClaimsTx(ctx context.Context, tx *v2store.Tx, interestID, artifactID, sourceAccountID string, revision int64) error {
	rows, err := tx.QueryContext(ctx, `SELECT client_id FROM client_cloud_claims WHERE source_account_id = ? AND artifact_id = ? AND state = 'active'`, sourceAccountID, artifactID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var clientID string
		if err := rows.Scan(&clientID); err != nil {
			return err
		}
		if err := deleteEntityTx(ctx, tx, clientID, "trackingInterest", interestID, artifactID, sourceAccountID, revision); err != nil {
			return err
		}
	}
	return rows.Err()
}

func projectContentForInterestedClientsTx(ctx context.Context, tx *v2store.Tx, artifactID string) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT c.client_id, o.content_observation_id, o.observation_revision
		FROM content_observations o
		JOIN tracking_interests i ON i.artifact_id = o.artifact_id AND i.comic_id = o.comic_id
		  AND i.visibility_scope = o.visibility_scope AND i.variant_key = o.variant_key AND i.state = 'active'
		JOIN client_cloud_claims c ON c.artifact_id = o.artifact_id AND c.state = 'active'
		JOIN client_source_links l ON l.client_id = c.client_id AND l.source_account_id = i.source_account_id
		  AND l.artifact_id = i.artifact_id AND l.state = 'linked'
		WHERE o.artifact_id = ?`, artifactID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var clientID, observationID string
		var revision int64
		if err := rows.Scan(&clientID, &observationID, &revision); err != nil {
			return err
		}
		if err := upsertEntityTx(ctx, tx, clientID, "contentObservation", observationID, artifactID, "", "upsert", revision); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()
	clients, err := tx.QueryContext(ctx, `SELECT DISTINCT client_id FROM client_entity_states WHERE entity_type = 'contentObservation' AND artifact_id = ? AND operation = 'upsert' ORDER BY client_id`, artifactID)
	if err != nil {
		return err
	}
	var clientIDs []string
	for clients.Next() {
		var clientID string
		if err := clients.Scan(&clientID); err != nil {
			clients.Close()
			return err
		}
		clientIDs = append(clientIDs, clientID)
	}
	if err := clients.Err(); err != nil {
		clients.Close()
		return err
	}
	clients.Close()
	for _, clientID := range clientIDs {
		if err := reconcileClientContentTx(ctx, tx, clientID); err != nil {
			return err
		}
	}
	return nil
}

func canonicalSyncJSON(value []byte) ([]byte, error) {
	var decoded any
	decoder := json.NewDecoder(strings.NewReader(string(value)))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, errors.New("multiple JSON values")
		}
		return nil, err
	}
	return json.Marshal(decoded)
}

func digestSyncJSON(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func digestSyncString(value string) string {
	return digestSyncJSON([]byte(value))
}

func nullablePublishString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullablePublishPtr(value *string) any {
	if value == nil || *value == "" {
		return nil
	}
	return *value
}
