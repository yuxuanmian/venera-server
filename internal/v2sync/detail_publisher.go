package v2sync

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"venera-server/internal/v2domain"
	"venera-server/internal/v2scan"
	"venera-server/internal/v2store"
)

var (
	ErrDetailPublishInvalid = errors.New("detail publication is invalid")
	ErrDetailPublishStale   = errors.New("detail publication is stale")
)

type PublishDetailRequest struct {
	DemandID              string
	ArtifactID            string
	ComicID               string
	VisibilityScope       string
	VariantKey            string
	ObservationContractID string
	SourceAccountID       string
	PackageReleaseID      string
	ExpectedGeneration    int64
	ExpectedSessionEpoch  int64
	FreshUntil            time.Time
	Result                v2scan.DetailResult
}

type PublishDetailResult struct {
	ArtifactID           string
	ComicID              string
	ContentObservationID string
	ObservationRevision  int64
	FreshUntil           time.Time
	ClientChanges        int
}

type DetailPublisher struct {
	repo *v2store.Repository
}

func NewDetailPublisher(repo *v2store.Repository) *DetailPublisher {
	return &DetailPublisher{repo: repo}
}

func (p *DetailPublisher) Publish(ctx context.Context, request PublishDetailRequest) (PublishDetailResult, error) {
	if p == nil || p.repo == nil || request.DemandID == "" || request.ArtifactID == "" || request.ComicID == "" || request.VisibilityScope == "" || request.ObservationContractID == "" || request.SourceAccountID == "" || request.PackageReleaseID == "" || request.ExpectedGeneration < 1 || request.ExpectedSessionEpoch < 1 || request.Result.ComicID != request.ComicID || len(request.Result.Payload) == 0 {
		return PublishDetailResult{}, ErrDetailPublishInvalid
	}
	if err := v2scan.ValidateDetailResult(request.Result); err != nil {
		return PublishDetailResult{}, ErrDetailPublishInvalid
	}
	var result PublishDetailResult
	err := p.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		var demandKind, demandState, demandArtifact, demandScope, demandVariant, demandContract string
		var demandGeneration int64
		if err := tx.QueryRowContext(ctx, `SELECT demand_kind, state, artifact_id, COALESCE(visibility_scope, ''), variant_key, COALESCE(observation_contract_id, ''), execution_generation FROM scan_demands WHERE demand_id = ?`, request.DemandID).Scan(&demandKind, &demandState, &demandArtifact, &demandScope, &demandVariant, &demandContract, &demandGeneration); errors.Is(err, sql.ErrNoRows) {
			return ErrDetailPublishStale
		} else if err != nil {
			return err
		}
		if demandKind != string(v2scan.DemandComicDetail) || demandState != string(v2scan.DemandActive) || demandArtifact != request.ArtifactID || demandScope != request.VisibilityScope || demandVariant != request.VariantKey || demandContract != request.ObservationContractID || demandGeneration != request.ExpectedGeneration {
			return ErrDetailPublishStale
		}
		var accountState, accountArtifact string
		var accountEpoch int64
		if err := tx.QueryRowContext(ctx, `SELECT state, artifact_id, session_epoch FROM source_accounts WHERE source_account_id = ?`, request.SourceAccountID).Scan(&accountState, &accountArtifact, &accountEpoch); errors.Is(err, sql.ErrNoRows) {
			return ErrDetailPublishStale
		} else if err != nil {
			return err
		}
		if accountState != string(v2domain.SourceAccountActive) || accountArtifact != request.ArtifactID || accountEpoch != request.ExpectedSessionEpoch {
			return ErrDetailPublishStale
		}
		var releaseState, releaseArtifact string
		if err := tx.QueryRowContext(ctx, `SELECT state, artifact_id FROM source_package_releases WHERE package_release_id = ?`, request.PackageReleaseID).Scan(&releaseState, &releaseArtifact); errors.Is(err, sql.ErrNoRows) {
			return ErrDetailPublishStale
		} else if err != nil {
			return err
		}
		if releaseState != "active" || releaseArtifact != request.ArtifactID {
			return ErrDetailPublishStale
		}
		var observation struct {
			VisibilityScope string `json:"visibilityScope"`
			MarkerEvidence  *struct {
				Scheme string `json:"scheme"`
			} `json:"markerEvidence"`
		}
		if err := json.Unmarshal(request.Result.Payload, &observation); err != nil || observation.VisibilityScope != request.VisibilityScope {
			return ErrDetailPublishInvalid
		}
		freshUntil := request.FreshUntil
		if freshUntil.IsZero() {
			freshUntil = tx.Now().Add(3 * time.Hour)
		}
		canonical, err := canonicalSyncJSON(request.Result.Payload)
		if err != nil {
			return ErrDetailPublishInvalid
		}
		digest := digestSyncJSON(canonical)
		markerScheme := ""
		if observation.MarkerEvidence != nil {
			markerScheme = observation.MarkerEvidence.Scheme
		}
		now := syncTime(tx.Now())
		var observationID, oldDigest string
		var observationRevision, validationRevision int64
		err = tx.QueryRowContext(ctx, `SELECT content_observation_id, payload_digest, observation_revision, validation_revision FROM content_observations WHERE artifact_id = ? AND comic_id = ? AND visibility_scope = ? AND variant_key = ? AND observation_contract_id = ?`, request.ArtifactID, request.ComicID, request.VisibilityScope, request.VariantKey, request.ObservationContractID).Scan(&observationID, &oldDigest, &observationRevision, &validationRevision)
		if errors.Is(err, sql.ErrNoRows) {
			observationID = "content_obs_" + digestSyncString(request.ArtifactID+"\x00"+request.ComicID+"\x00"+request.VisibilityScope+"\x00"+request.VariantKey+"\x00"+request.ObservationContractID)
			observationRevision, validationRevision = 1, 1
			_, err = tx.ExecContext(ctx, `INSERT INTO content_observations(content_observation_id, artifact_id, comic_id, visibility_scope, variant_key, observation_contract_id, package_release_id, marker_scheme, payload_digest, payload_json, status, observation_revision, validation_revision, observed_at, validated_at, fresh_until, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'fresh', ?, ?, ?, ?, ?, ?)`, observationID, request.ArtifactID, request.ComicID, request.VisibilityScope, request.VariantKey, request.ObservationContractID, request.PackageReleaseID, nullablePublishString(markerScheme), digest, string(canonical), observationRevision, validationRevision, now, now, syncTime(freshUntil), now)
		} else if err == nil {
			if oldDigest != digest {
				observationRevision++
			}
			validationRevision++
			_, err = tx.ExecContext(ctx, `UPDATE content_observations SET package_release_id = ?, marker_scheme = ?, payload_digest = ?, payload_json = ?, status = 'fresh', observation_revision = ?, validation_revision = ?, validated_at = ?, fresh_until = ?, updated_at = ? WHERE content_observation_id = ?`, request.PackageReleaseID, nullablePublishString(markerScheme), digest, string(canonical), observationRevision, validationRevision, now, syncTime(freshUntil), now, observationID)
		}
		if err != nil {
			return err
		}
		clients, err := detailProjectionClientsTx(ctx, tx, request)
		if err != nil {
			return err
		}
		for _, clientID := range clients {
			before, err := clientHighWatermarkTx(ctx, tx, clientID)
			if err != nil {
				return err
			}
			if err := projectClientTx(ctx, tx, clientID); err != nil {
				return err
			}
			after, err := clientHighWatermarkTx(ctx, tx, clientID)
			if err != nil {
				return err
			}
			if after > before {
				result.ClientChanges += int(after - before)
			}
		}
		result = PublishDetailResult{ArtifactID: request.ArtifactID, ComicID: request.ComicID, ContentObservationID: observationID, ObservationRevision: observationRevision, FreshUntil: freshUntil, ClientChanges: result.ClientChanges}
		return nil
	})
	return result, err
}

func detailProjectionClientsTx(ctx context.Context, tx *v2store.Tx, request PublishDetailRequest) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT c.client_id
		FROM client_cloud_claims c
		JOIN client_source_links l ON l.client_id = c.client_id AND l.source_account_id = c.source_account_id AND l.artifact_id = c.artifact_id AND l.state = 'linked'
		WHERE c.artifact_id = ? AND c.state = 'active'
		  AND EXISTS (
			SELECT 1 FROM tracking_interests i
			WHERE i.artifact_id = c.artifact_id AND i.comic_id = ? AND i.visibility_scope = ? AND i.variant_key = ? AND i.state = 'active'
			  AND (i.source_account_id IS NULL OR i.source_account_id = c.source_account_id)
		  )
		ORDER BY c.client_id`, request.ArtifactID, request.ComicID, request.VisibilityScope, request.VariantKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var clients []string
	for rows.Next() {
		var clientID string
		if err := rows.Scan(&clientID); err != nil {
			return nil, err
		}
		clients = append(clients, clientID)
	}
	return clients, rows.Err()
}

func clientHighWatermarkTx(ctx context.Context, tx *v2store.Tx, clientID string) (int64, error) {
	var high int64
	err := tx.QueryRowContext(ctx, `SELECT high_change_seq FROM client_change_watermarks WHERE client_id = ?`, clientID).Scan(&high)
	return high, err
}
