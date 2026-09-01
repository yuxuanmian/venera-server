package v2api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"venera-server/internal/v2domain"
	"venera-server/internal/v2manifest"
	"venera-server/internal/v2store"
)

type replaceInventoryBody struct {
	ExpectedInventoryRevision *int64               `json:"expectedInventoryRevision"`
	Entries                   []inventoryEntryBody `json:"entries"`
}

type inventoryEntryBody struct {
	ArtifactID                   string            `json:"artifactId"`
	ManagementMode               string            `json:"managementMode"`
	PackageReleaseID             string            `json:"packageReleaseId"`
	CoreHash                     string            `json:"coreHash"`
	ClientExtensionHashes        map[string]string `json:"clientExtensionHashes"`
	ObservationContractID        string            `json:"observationContractId"`
	AccountObservationContractID string            `json:"accountObservationContractId"`
	AccountProbeContractID       string            `json:"accountProbeContractId"`
	MarkerSchemes                []string          `json:"markerSchemes"`
}

type inventoryEntryResponse struct {
	ArtifactID                   string `json:"artifactId"`
	ManagementMode               string `json:"managementMode"`
	PackageReleaseID             string `json:"packageReleaseId,omitempty"`
	CompatibilityState           string `json:"compatibilityState"`
	CompatibilityReason          string `json:"reason,omitempty"`
	CoreHash                     string `json:"coreHash,omitempty"`
	ClientExtensionsJSON         string `json:"clientExtensionsJson,omitempty"`
	ObservationContractID        string `json:"observationContractId,omitempty"`
	AccountObservationContractID string `json:"accountObservationContractId,omitempty"`
	AccountProbeContractID       string `json:"accountProbeContractId,omitempty"`
}

type replaceInventoryResponse struct {
	ClientID          string                   `json:"clientId"`
	InventoryRevision int64                    `json:"inventoryRevision"`
	Entries           []inventoryEntryResponse `json:"entries"`
}

type sourceStateResponse struct {
	ArtifactID              string  `json:"artifactId"`
	SelectedSourceAccountID string  `json:"selectedSourceAccountId,omitempty"`
	LinkState               string  `json:"linkState"`
	ClaimState              string  `json:"claimState"`
	EffectiveState          string  `json:"effectiveState"`
	CompatibilityState      string  `json:"compatibilityState"`
	SourceStatus            string  `json:"sourceStatus"`
	FreshUntil              *string `json:"freshUntil"`
	NextEvaluationAt        *string `json:"nextEvaluationAt"`
	BlockedReason           *string `json:"blockedReason"`
	Revision                int64   `json:"revision"`
}

func (router *Router) replaceInventoryHandler(w http.ResponseWriter, request *http.Request) {
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
	var body replaceInventoryBody
	decodeErr := decodeJSON(w, request, &body)
	if decodeErr != nil || body.ExpectedInventoryRevision == nil || *body.ExpectedInventoryRevision < 0 {
		writeAPIError(router, w, request, http.StatusBadRequest, "invalid_request", "request is invalid", false, nil)
		return
	}
	manifest, manifestErr := router.repo.GetActiveManifestCatalog(request.Context())
	if manifestErr != nil && !errors.Is(manifestErr, sql.ErrNoRows) {
		writeStoreError(router, w, request, manifestErr)
		return
	}
	var parsed v2manifest.Manifest
	if manifestErr == nil {
		parsed, manifestErr = v2manifest.Parse([]byte(manifest.ManifestJSON))
		if manifestErr != nil {
			writeAPIError(router, w, request, http.StatusServiceUnavailable, "manifest_unavailable", "manifest is unavailable", true, nil)
			return
		}
	}
	entries := make([]v2domain.InventoryEntry, 0, len(body.Entries))
	reasons := make([]string, 0, len(body.Entries))
	for _, input := range body.Entries {
		encodedExtensions, marshalErr := json.Marshal(input.ClientExtensionHashes)
		if marshalErr != nil {
			writeAPIError(router, w, request, http.StatusBadRequest, "invalid_request", "request is invalid", false, nil)
			return
		}
		compatibility, reason := inventoryCompatibility(parsed, input)
		entries = append(entries, v2domain.InventoryEntry{
			ArtifactID: v2domain.ArtifactID(input.ArtifactID), PackageReleaseID: v2domain.PackageReleaseID(input.PackageReleaseID),
			ManagementMode: input.ManagementMode, CompatibilityState: compatibility, CoreHash: input.CoreHash,
			ClientExtensionsJSON: string(encodedExtensions), ObservationContractID: input.ObservationContractID,
			AccountObservationContractID: input.AccountObservationContractID, AccountProbeContractID: input.AccountProbeContractID,
		})
		reasons = append(reasons, reason)
	}
	result, err := router.repo.ReplaceClientInventory(request.Context(), v2store.ReplaceClientInventoryRequest{
		ClientID: string(client.ID), ExpectedRevision: v2domain.Revision(*body.ExpectedInventoryRevision), Entries: entries, IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	response := replaceInventoryResponse{ClientID: result.ClientID, InventoryRevision: int64(result.Revision), Entries: make([]inventoryEntryResponse, 0, len(result.Entries))}
	for i, entry := range result.Entries {
		response.Entries = append(response.Entries, inventoryEntryResponse{
			ArtifactID: string(entry.ArtifactID), ManagementMode: entry.ManagementMode, PackageReleaseID: string(entry.PackageReleaseID),
			CompatibilityState: string(entry.CompatibilityState), CompatibilityReason: reasons[i], CoreHash: entry.CoreHash,
			ClientExtensionsJSON: entry.ClientExtensionsJSON, ObservationContractID: entry.ObservationContractID,
			AccountObservationContractID: entry.AccountObservationContractID, AccountProbeContractID: entry.AccountProbeContractID,
		})
	}
	writeJSON(router, w, request, http.StatusOK, response)
}

func (router *Router) sourceStatesHandler(w http.ResponseWriter, request *http.Request) {
	client, err := router.authenticateClient(request)
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	items, err := router.repo.ListClientSourceStates(request.Context(), string(client.ID))
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	response := make([]sourceStateResponse, 0, len(items))
	for _, item := range items {
		claimState := item.ClaimState
		if claimState == "" {
			claimState = "absent"
		}
		sourceStatus := item.SourceStatus
		if sourceStatus == "" {
			sourceStatus = "idle"
		}
		effectiveState := "inactive"
		if item.ClaimState == "active" {
			effectiveState = "active"
		}
		if sourceStatus == "blocked" || sourceStatus == "reauthRequired" || sourceStatus == "paused" {
			effectiveState = "blocked"
		}
		var blockedReason *string
		if item.BlockedReason != "" {
			value := item.BlockedReason
			blockedReason = &value
		}
		var nextEvaluation *string
		if item.NextEvaluationAt != nil {
			value := item.NextEvaluationAt.UTC().Format(time.RFC3339Nano)
			nextEvaluation = &value
		}
		response = append(response, sourceStateResponse{
			ArtifactID: string(item.ArtifactID), SelectedSourceAccountID: string(item.SelectedSourceAccountID),
			LinkState: string(item.LinkState), ClaimState: claimState, EffectiveState: effectiveState,
			CompatibilityState: string(item.CompatibilityState), SourceStatus: sourceStatus,
			NextEvaluationAt: nextEvaluation, BlockedReason: blockedReason, Revision: int64(item.Revision),
		})
	}
	writeJSON(router, w, request, http.StatusOK, map[string]any{"items": response})
}

func inventoryCompatibility(manifest v2manifest.Manifest, input inventoryEntryBody) (v2domain.CompatibilityState, string) {
	if input.ManagementMode != "managed" {
		return v2domain.CompatibilityIncompatible, "client source is not managed"
	}
	for _, pkg := range manifest.SourcePackages {
		if pkg.ArtifactID != input.ArtifactID {
			continue
		}
		if pkg.ManagedState != "active" {
			return v2domain.CompatibilityIncompatible, "source is paused"
		}
		if input.PackageReleaseID != pkg.PackageReleaseID {
			return v2domain.CompatibilityIncompatible, "package release differs"
		}
		if input.CoreHash != pkg.Core.SHA256 {
			return v2domain.CompatibilityIncompatible, "core hash differs"
		}
		if input.ObservationContractID != pkg.Tracking.ObservationContractID || input.AccountObservationContractID != pkg.Tracking.AccountObservationContractID || input.AccountProbeContractID != pkg.AccountProbeContract.ID {
			return v2domain.CompatibilityIncompatible, "contract differs"
		}
		if !sameStringSet(input.MarkerSchemes, pkg.Tracking.MarkerSchemes) {
			return v2domain.CompatibilityIncompatible, "marker scheme differs"
		}
		for extensionID := range input.ClientExtensionHashes {
			known := false
			for _, extension := range pkg.Extensions {
				if extension.ExtensionID == extensionID {
					known = extension.Runtime == "client" && input.ClientExtensionHashes[extensionID] == extension.SHA256
					break
				}
			}
			if !known {
				return v2domain.CompatibilityIncompatible, "client extension differs"
			}
		}
		if pkg.Status != "stable" {
			return v2domain.CompatibilityUnknown, "package release is not stable"
		}
		return v2domain.CompatibilityCompatible, ""
	}
	return v2domain.CompatibilityUnknown, "artifact is not in the active manifest"
}

func sameStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	counts := make(map[string]int, len(left))
	for _, value := range left {
		counts[value]++
	}
	for _, value := range right {
		if counts[value] == 0 {
			return false
		}
		counts[value]--
	}
	return true
}
