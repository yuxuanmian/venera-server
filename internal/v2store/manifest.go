package v2store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"venera-server/internal/v2domain"
)

type ManifestCatalogInput struct {
	ManifestID      string
	CatalogID       string
	ManifestURL     string
	CatalogSequence int64
	ManifestHash    string
	ManifestJSON    string
	IdempotencyKey  string
}

type ManifestCatalogRecord struct {
	ManifestID      string
	CatalogID       string
	ManifestURL     string
	CatalogSequence int64
	ManifestHash    string
	ManifestJSON    string
	State           string
	Revision        v2domain.Revision
}

type ReplaceClientInventoryRequest struct {
	ClientID         string
	ExpectedRevision v2domain.Revision
	Entries          []v2domain.InventoryEntry
	IdempotencyKey   string
}

type ClientInventoryResult struct {
	ClientID string                    `json:"clientId"`
	Revision v2domain.Revision         `json:"revision"`
	Entries  []v2domain.InventoryEntry `json:"entries"`
}

type SourceArtifactInput struct {
	ArtifactID   string
	SourceKey    string
	CatalogID    string
	ManagedState string
}

type SourcePackageReleaseInput struct {
	PackageReleaseID             string
	ArtifactID                   string
	CatalogID                    string
	CatalogSequence              int64
	CoreHash                     string
	ScanningExtensionHash        string
	ObservationContractID        string
	AccountObservationContractID string
	AccountProbeContractID       string
	MarkerSchemesJSON            string
	SessionExportProfileID       string
	PackageJSON                  string
	State                        string
}

type ManifestBundleInput struct {
	Catalog   ManifestCatalogInput
	Artifacts []SourceArtifactInput
	Releases  []SourcePackageReleaseInput
}

func (r *Repository) ActivateManifestCatalog(ctx context.Context, input ManifestCatalogInput) (ManifestCatalogRecord, error) {
	var err error
	input, err = normalizeManifestCatalogInput(input)
	if err != nil {
		return ManifestCatalogRecord{}, err
	}
	requestForDigest := struct {
		ManifestID      string `json:"manifestId"`
		CatalogID       string `json:"catalogId"`
		ManifestURL     string `json:"manifestUrl"`
		CatalogSequence int64  `json:"catalogSequence"`
		ManifestHash    string `json:"manifestHash"`
		ManifestJSON    string `json:"manifestJson"`
	}{input.ManifestID, input.CatalogID, input.ManifestURL, input.CatalogSequence, input.ManifestHash, input.ManifestJSON}
	var result ManifestCatalogRecord
	err = r.db.WriteTx(ctx, func(tx *Tx) error {
		replay, err := r.beginIdempotency(ctx, tx, "admin", "manifest-manager", "activate-manifest", input.IdempotencyKey, requestForDigest)
		if err != nil {
			return err
		}
		if replay != nil {
			return decodeIdempotencyReplay(replay, &result)
		}
		result, err = r.activateManifestCatalogTx(ctx, tx, input)
		if err != nil {
			return err
		}
		return r.completeIdempotency(ctx, tx, "admin", "manifest-manager", "activate-manifest", input.IdempotencyKey, 200, result)
	})
	return result, err
}

func (r *Repository) ActivateManifestBundle(ctx context.Context, input ManifestBundleInput) (ManifestCatalogRecord, error) {
	normalized, err := normalizeManifestBundleInput(input)
	if err != nil {
		return ManifestCatalogRecord{}, err
	}
	var result ManifestCatalogRecord
	err = r.db.WriteTx(ctx, func(tx *Tx) error {
		replay, err := r.beginIdempotency(
			ctx,
			tx,
			"admin",
			"manifest-manager",
			"activate-manifest-bundle",
			normalized.Catalog.IdempotencyKey,
			normalized,
		)
		if err != nil {
			return err
		}
		if replay != nil {
			return decodeIdempotencyReplay(replay, &result)
		}
		for _, release := range normalized.Releases {
			if err := r.checkPackageReleaseArtifactTx(ctx, tx, release.PackageReleaseID, release.ArtifactID); err != nil {
				return err
			}
		}
		result, err = r.activateManifestCatalogTx(ctx, tx, normalized.Catalog)
		if err != nil {
			return err
		}
		for _, artifact := range normalized.Artifacts {
			if err := r.upsertSourceArtifactTx(ctx, tx, artifact); err != nil {
				return err
			}
		}
		for _, release := range normalized.Releases {
			if err := r.upsertSourcePackageReleaseTx(ctx, tx, release); err != nil {
				return err
			}
		}
		return r.completeIdempotency(
			ctx,
			tx,
			"admin",
			"manifest-manager",
			"activate-manifest-bundle",
			normalized.Catalog.IdempotencyKey,
			200,
			result,
		)
	})
	return result, err
}

func normalizeManifestCatalogInput(input ManifestCatalogInput) (ManifestCatalogInput, error) {
	if input.ManifestID == "" || input.CatalogID == "" || input.ManifestURL == "" || input.ManifestHash == "" || input.ManifestJSON == "" || input.CatalogSequence < 0 {
		return ManifestCatalogInput{}, ErrInvalidArgument
	}
	if !json.Valid([]byte(input.ManifestJSON)) {
		return ManifestCatalogInput{}, fmt.Errorf("%w: manifest JSON is invalid", ErrInvalidArgument)
	}
	if input.IdempotencyKey == "" {
		input.IdempotencyKey = "manifest:" + input.ManifestHash
	}
	return input, nil
}

func normalizeSourceArtifactInput(input SourceArtifactInput) (SourceArtifactInput, error) {
	if input.ArtifactID == "" || input.SourceKey == "" || input.CatalogID == "" {
		return SourceArtifactInput{}, ErrInvalidArgument
	}
	if input.ManagedState == "" {
		input.ManagedState = "active"
	}
	return input, nil
}

func normalizeSourcePackageReleaseInput(input SourcePackageReleaseInput) (SourcePackageReleaseInput, error) {
	if input.PackageReleaseID == "" || input.ArtifactID == "" || input.CatalogID == "" || input.CatalogSequence < 0 || input.CoreHash == "" || input.MarkerSchemesJSON == "" || input.PackageJSON == "" {
		return SourcePackageReleaseInput{}, ErrInvalidArgument
	}
	if input.State == "" {
		input.State = "candidate"
	}
	if input.State != "candidate" && input.State != "active" && input.State != "superseded" && input.State != "rejected" {
		return SourcePackageReleaseInput{}, ErrInvalidArgument
	}
	return input, nil
}

func normalizeManifestBundleInput(input ManifestBundleInput) (ManifestBundleInput, error) {
	catalog, err := normalizeManifestCatalogInput(input.Catalog)
	if err != nil {
		return ManifestBundleInput{}, err
	}
	if len(input.Artifacts) == 0 || len(input.Artifacts) != len(input.Releases) {
		return ManifestBundleInput{}, fmt.Errorf("%w: manifest bundle package cardinality is invalid", ErrInvalidArgument)
	}
	artifacts := append([]SourceArtifactInput(nil), input.Artifacts...)
	for i := range artifacts {
		artifacts[i], err = normalizeSourceArtifactInput(artifacts[i])
		if err != nil {
			return ManifestBundleInput{}, fmt.Errorf("%w: manifest bundle artifact %d is invalid", ErrInvalidArgument, i)
		}
		if artifacts[i].CatalogID != catalog.CatalogID {
			return ManifestBundleInput{}, fmt.Errorf("%w: manifest bundle artifact catalog does not match", ErrInvalidArgument)
		}
	}
	releases := append([]SourcePackageReleaseInput(nil), input.Releases...)
	for i := range releases {
		releases[i], err = normalizeSourcePackageReleaseInput(releases[i])
		if err != nil {
			return ManifestBundleInput{}, fmt.Errorf("%w: manifest bundle release %d is invalid", ErrInvalidArgument, i)
		}
		if releases[i].CatalogID != catalog.CatalogID || releases[i].CatalogSequence != catalog.CatalogSequence {
			return ManifestBundleInput{}, fmt.Errorf("%w: manifest bundle release catalog does not match", ErrInvalidArgument)
		}
	}
	sort.Slice(artifacts, func(i, j int) bool {
		return artifacts[i].ArtifactID < artifacts[j].ArtifactID
	})
	sort.Slice(releases, func(i, j int) bool {
		return releases[i].PackageReleaseID < releases[j].PackageReleaseID
	})
	artifactIDs := make(map[string]struct{}, len(artifacts))
	for _, artifact := range artifacts {
		if _, exists := artifactIDs[artifact.ArtifactID]; exists {
			return ManifestBundleInput{}, fmt.Errorf("%w: duplicate artifact id %q", ErrInvalidArgument, artifact.ArtifactID)
		}
		artifactIDs[artifact.ArtifactID] = struct{}{}
	}
	releaseIDs := make(map[string]struct{}, len(releases))
	releasesByArtifact := make(map[string]int, len(releases))
	for _, release := range releases {
		if _, exists := releaseIDs[release.PackageReleaseID]; exists {
			return ManifestBundleInput{}, fmt.Errorf("%w: duplicate package release id %q", ErrInvalidArgument, release.PackageReleaseID)
		}
		releaseIDs[release.PackageReleaseID] = struct{}{}
		if _, exists := artifactIDs[release.ArtifactID]; !exists {
			return ManifestBundleInput{}, fmt.Errorf("%w: release artifact is not in manifest bundle", ErrInvalidArgument)
		}
		releasesByArtifact[release.ArtifactID]++
	}
	for _, artifact := range artifacts {
		if releasesByArtifact[artifact.ArtifactID] != 1 {
			return ManifestBundleInput{}, fmt.Errorf("%w: artifact %q does not have exactly one release", ErrInvalidArgument, artifact.ArtifactID)
		}
	}
	return ManifestBundleInput{Catalog: catalog, Artifacts: artifacts, Releases: releases}, nil
}

func (r *Repository) activateManifestCatalogTx(ctx context.Context, tx *Tx, input ManifestCatalogInput) (ManifestCatalogRecord, error) {
	var maxSequence sql.NullInt64
	if err := tx.QueryRowContext(ctx, "SELECT MAX(catalog_sequence) FROM manifest_catalogs").Scan(&maxSequence); err != nil {
		return ManifestCatalogRecord{}, err
	}
	if maxSequence.Valid && input.CatalogSequence < maxSequence.Int64 {
		return ManifestCatalogRecord{}, ErrManifestSequenceRollback
	}
	var existingState string
	var existingHash string
	existingErr := tx.QueryRowContext(ctx, `
		SELECT state, manifest_hash FROM manifest_catalogs WHERE catalog_id = ? AND catalog_sequence = ?`,
		input.CatalogID, input.CatalogSequence).Scan(&existingState, &existingHash)
	if existingErr == nil {
		if existingHash == input.ManifestHash && existingState == "active" {
			return ManifestCatalogRecord{}, ErrManifestAlreadyActive
		}
		return ManifestCatalogRecord{}, ErrManifestSequenceRollback
	}
	if !errors.Is(existingErr, sql.ErrNoRows) {
		return ManifestCatalogRecord{}, existingErr
	}
	if _, err := tx.ExecContext(ctx, `UPDATE manifest_catalogs SET state = 'superseded', updated_at = ? WHERE state = 'active'`, formatTime(r.db.Now())); err != nil {
		return ManifestCatalogRecord{}, err
	}
	now := formatTime(r.db.Now())
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO manifest_catalogs(
			manifest_id, catalog_id, manifest_url, catalog_sequence, manifest_hash,
			manifest_json, state, revision, fetched_at, activated_at, updated_at
		) VALUES(?, ?, ?, ?, ?, ?, 'active', 1, ?, ?, ?)`,
		input.ManifestID, input.CatalogID, input.ManifestURL, input.CatalogSequence,
		input.ManifestHash, input.ManifestJSON, now, now, now); err != nil {
		return ManifestCatalogRecord{}, err
	}
	return ManifestCatalogRecord{
		ManifestID: input.ManifestID, CatalogID: input.CatalogID, ManifestURL: input.ManifestURL,
		CatalogSequence: input.CatalogSequence, ManifestHash: input.ManifestHash,
		ManifestJSON: input.ManifestJSON, State: "active", Revision: 1,
	}, nil
}

func (r *Repository) GetActiveManifestCatalog(ctx context.Context) (ManifestCatalogRecord, error) {
	var record ManifestCatalogRecord
	err := r.db.ReadTx(ctx, func(tx *Tx) error {
		var state string
		var revision int64
		return tx.QueryRowContext(ctx, `
			SELECT manifest_id, catalog_id, manifest_url, catalog_sequence, manifest_hash,
			       manifest_json, state, revision
			FROM manifest_catalogs WHERE state = 'active'`).Scan(
			&record.ManifestID, &record.CatalogID, &record.ManifestURL, &record.CatalogSequence,
			&record.ManifestHash, &record.ManifestJSON, &state, &revision)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return ManifestCatalogRecord{}, sql.ErrNoRows
	}
	record.State = "active"
	return record, err
}

func (r *Repository) UpsertSourceArtifact(ctx context.Context, input SourceArtifactInput) error {
	var err error
	input, err = normalizeSourceArtifactInput(input)
	if err != nil {
		return err
	}
	return r.db.WriteTx(ctx, func(tx *Tx) error {
		return r.upsertSourceArtifactTx(ctx, tx, input)
	})
}

func (r *Repository) upsertSourceArtifactTx(ctx context.Context, tx *Tx, input SourceArtifactInput) error {
	now := formatTime(r.db.Now())
	_, err := tx.ExecContext(ctx, `
			INSERT INTO source_artifacts(artifact_id, source_key, catalog_id, managed_state, revision, created_at, updated_at)
			VALUES(?, ?, ?, ?, 1, ?, ?)
			ON CONFLICT(artifact_id) DO UPDATE SET
				source_key = excluded.source_key,
				catalog_id = excluded.catalog_id,
				managed_state = excluded.managed_state,
				revision = source_artifacts.revision + 1,
				updated_at = excluded.updated_at`,
		input.ArtifactID, input.SourceKey, input.CatalogID, input.ManagedState, now, now)
	return err
}

func (r *Repository) UpsertSourcePackageRelease(ctx context.Context, input SourcePackageReleaseInput) error {
	var err error
	input, err = normalizeSourcePackageReleaseInput(input)
	if err != nil {
		return err
	}
	return r.db.WriteTx(ctx, func(tx *Tx) error {
		return r.upsertSourcePackageReleaseTx(ctx, tx, input)
	})
}

func (r *Repository) checkPackageReleaseArtifactTx(ctx context.Context, tx *Tx, packageReleaseID, artifactID string) error {
	var existingArtifactID string
	err := tx.QueryRowContext(ctx, `
		SELECT artifact_id FROM source_package_releases WHERE package_release_id = ?`, packageReleaseID).Scan(&existingArtifactID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if existingArtifactID != artifactID {
		return ErrPackageReleaseArtifactConflict
	}
	return nil
}

func (r *Repository) upsertSourcePackageReleaseTx(ctx context.Context, tx *Tx, input SourcePackageReleaseInput) error {
	var existingArtifactID string
	if err := tx.QueryRowContext(ctx, `
		SELECT artifact_id FROM source_artifacts WHERE artifact_id = ?`, input.ArtifactID).Scan(&existingArtifactID); errors.Is(err, sql.ErrNoRows) {
		return ErrArtifactNotFound
	} else if err != nil {
		return err
	}
	if err := r.checkPackageReleaseArtifactTx(ctx, tx, input.PackageReleaseID, input.ArtifactID); err != nil {
		return err
	}
	if input.State == "active" {
		if _, err := tx.ExecContext(ctx, `UPDATE source_package_releases SET state = 'superseded', updated_at = ? WHERE artifact_id = ? AND state = 'active' AND package_release_id <> ?`, formatTime(r.db.Now()), input.ArtifactID, input.PackageReleaseID); err != nil {
			return err
		}
	}
	now := formatTime(r.db.Now())
	_, err := tx.ExecContext(ctx, `
			INSERT INTO source_package_releases(
				package_release_id, artifact_id, catalog_id, catalog_sequence, core_hash,
				scanning_extension_hash, observation_contract_id, account_observation_contract_id,
				account_probe_contract_id, marker_schemes_json, session_export_profile_id,
				package_json, state, revision, created_at, updated_at, activated_at
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, CASE WHEN ? = 'active' THEN ? ELSE NULL END)
			ON CONFLICT(package_release_id) DO UPDATE SET
				catalog_id = excluded.catalog_id,
				catalog_sequence = excluded.catalog_sequence,
				core_hash = excluded.core_hash,
				scanning_extension_hash = excluded.scanning_extension_hash,
				observation_contract_id = excluded.observation_contract_id,
				account_observation_contract_id = excluded.account_observation_contract_id,
				account_probe_contract_id = excluded.account_probe_contract_id,
				marker_schemes_json = excluded.marker_schemes_json,
				session_export_profile_id = excluded.session_export_profile_id,
				package_json = excluded.package_json,
				state = excluded.state,
				revision = source_package_releases.revision + 1,
				updated_at = excluded.updated_at,
				activated_at = excluded.activated_at`,
		input.PackageReleaseID, input.ArtifactID, input.CatalogID, input.CatalogSequence, input.CoreHash,
		input.ScanningExtensionHash, input.ObservationContractID, input.AccountObservationContractID,
		input.AccountProbeContractID, input.MarkerSchemesJSON, input.SessionExportProfileID,
		input.PackageJSON, input.State, now, now, input.State, now)
	return err
}

func (r *Repository) ReplaceClientInventory(ctx context.Context, request ReplaceClientInventoryRequest) (ClientInventoryResult, error) {
	if request.ClientID == "" || request.ExpectedRevision < 0 || request.IdempotencyKey == "" {
		return ClientInventoryResult{}, ErrInvalidArgument
	}
	seen := make(map[string]struct{}, len(request.Entries))
	for _, entry := range request.Entries {
		if entry.ArtifactID == "" || entry.ManagementMode == "" || entry.CompatibilityState == "" {
			return ClientInventoryResult{}, ErrInvalidArgument
		}
		key := string(entry.ArtifactID)
		if _, ok := seen[key]; ok {
			return ClientInventoryResult{}, fmt.Errorf("%w: duplicate artifact inventory", ErrInvalidArgument)
		}
		seen[key] = struct{}{}
	}
	sort.Slice(request.Entries, func(i, j int) bool { return request.Entries[i].ArtifactID < request.Entries[j].ArtifactID })
	requestForDigest := struct {
		ClientID         string                    `json:"clientId"`
		ExpectedRevision v2domain.Revision         `json:"expectedRevision"`
		Entries          []v2domain.InventoryEntry `json:"entries"`
	}{request.ClientID, request.ExpectedRevision, request.Entries}
	var result ClientInventoryResult
	err := r.db.WriteTx(ctx, func(tx *Tx) error {
		replay, err := r.beginIdempotency(ctx, tx, "client", request.ClientID, "replace-inventory", request.IdempotencyKey, requestForDigest)
		if err != nil {
			return err
		}
		if replay != nil {
			return decodeIdempotencyReplay(replay, &result)
		}
		var state string
		if err := tx.QueryRowContext(ctx, "SELECT state FROM client_installations WHERE client_id = ?", request.ClientID).Scan(&state); errors.Is(err, sql.ErrNoRows) {
			return ErrClientNotFound
		} else if err != nil {
			return err
		}
		if state != string(v2domain.ClientActive) {
			return ErrClientRevoked
		}
		var currentRevision int64
		if err := tx.QueryRowContext(ctx, "SELECT inventory_revision FROM client_sync_state WHERE client_id = ?", request.ClientID).Scan(&currentRevision); err != nil {
			return err
		}
		if v2domain.Revision(currentRevision) != request.ExpectedRevision {
			return ErrRevisionConflict
		}
		newRevision := request.ExpectedRevision + 1
		for _, entry := range request.Entries {
			if _, err := tx.ExecContext(ctx, "SELECT artifact_id FROM source_artifacts WHERE artifact_id = ?", entry.ArtifactID); errors.Is(err, sql.ErrNoRows) {
				return ErrArtifactNotFound
			} else if err != nil {
				return err
			}
			if entry.PackageReleaseID != "" {
				var releaseArtifact string
				if err := tx.QueryRowContext(ctx, "SELECT artifact_id FROM source_package_releases WHERE package_release_id = ?", entry.PackageReleaseID).Scan(&releaseArtifact); errors.Is(err, sql.ErrNoRows) {
					return ErrPackageReleaseNotFound
				} else if err != nil {
					return err
				} else if releaseArtifact != string(entry.ArtifactID) {
					return ErrInvalidArgument
				}
			}
		}
		now := formatTime(r.db.Now())
		if _, err := tx.ExecContext(ctx, `DELETE FROM client_source_inventory WHERE client_id = ?`, request.ClientID); err != nil {
			return err
		}
		for _, entry := range request.Entries {
			var release any
			if entry.PackageReleaseID != "" {
				release = string(entry.PackageReleaseID)
			}
			if entry.ClientExtensionsJSON == "" {
				entry.ClientExtensionsJSON = "{}"
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO client_source_inventory(
					client_id, artifact_id, package_release_id, management_mode, compatibility_state,
					core_hash, client_extensions_json, observation_contract_id,
					account_observation_contract_id, account_probe_contract_id, inventory_revision, updated_at
				) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				request.ClientID, entry.ArtifactID, release, entry.ManagementMode, entry.CompatibilityState,
				entry.CoreHash, entry.ClientExtensionsJSON, entry.ObservationContractID,
				entry.AccountObservationContractID, entry.AccountProbeContractID, newRevision, now); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE client_sync_state SET inventory_revision = ?, updated_at = ? WHERE client_id = ?`, newRevision, now, request.ClientID); err != nil {
			return err
		}
		result = ClientInventoryResult{ClientID: request.ClientID, Revision: newRevision, Entries: append([]v2domain.InventoryEntry(nil), request.Entries...)}
		return r.completeIdempotency(ctx, tx, "client", request.ClientID, "replace-inventory", request.IdempotencyKey, 200, result)
	})
	return result, err
}

func (r *Repository) GetClientInventory(ctx context.Context, clientID string) ([]v2domain.InventoryEntry, v2domain.Revision, error) {
	var entries []v2domain.InventoryEntry
	var revision v2domain.Revision
	err := r.db.ReadTx(ctx, func(tx *Tx) error {
		var current int64
		if err := tx.QueryRowContext(ctx, `SELECT inventory_revision FROM client_sync_state WHERE client_id = ?`, clientID).Scan(&current); err != nil {
			return err
		}
		revision = v2domain.Revision(current)
		rows, err := tx.QueryContext(ctx, `
			SELECT artifact_id, COALESCE(package_release_id, ''), management_mode, compatibility_state,
			       COALESCE(core_hash, ''), client_extensions_json,
			       COALESCE(observation_contract_id, ''), COALESCE(account_observation_contract_id, ''),
			       COALESCE(account_probe_contract_id, '')
			FROM client_source_inventory WHERE client_id = ? ORDER BY artifact_id`, clientID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var entry v2domain.InventoryEntry
			if err := rows.Scan(&entry.ArtifactID, &entry.PackageReleaseID, &entry.ManagementMode, &entry.CompatibilityState,
				&entry.CoreHash, &entry.ClientExtensionsJSON, &entry.ObservationContractID,
				&entry.AccountObservationContractID, &entry.AccountProbeContractID); err != nil {
				return err
			}
			entries = append(entries, entry)
		}
		return rows.Err()
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, 0, ErrClientNotFound
	}
	return entries, revision, err
}
