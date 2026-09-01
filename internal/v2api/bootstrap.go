package v2api

import (
	"context"
	"database/sql"
	"errors"
	"net/http"

	"venera-server/internal/v2crypto"
	"venera-server/internal/v2domain"
	"venera-server/internal/v2store"
)

type bootstrapResponse struct {
	Client            clientBrief       `json:"client"`
	Catalog           bootstrapCatalog  `json:"catalog"`
	InventoryRevision int64             `json:"inventoryRevision"`
	Sync              bootstrapSync     `json:"sync"`
	Sources           []bootstrapSource `json:"sources"`
}

type bootstrapCatalog struct {
	CatalogID       string `json:"catalogId"`
	CatalogSequence int64  `json:"catalogSequence"`
	ManifestHash    string `json:"manifestHash"`
	ManifestURL     string `json:"manifestUrl"`
}

type bootstrapSync struct {
	Cursor         string `json:"cursor"`
	LowWatermark   int64  `json:"lowWatermark"`
	HighWatermark  int64  `json:"highWatermark"`
	ResyncRequired bool   `json:"resyncRequired"`
	ResyncReason   string `json:"resyncReason"`
}

type bootstrapSource struct {
	ArtifactID       string `json:"artifactId"`
	PackageReleaseID string `json:"packageReleaseId"`
	ManagedState     string `json:"managedState"`
	RuntimeState     string `json:"runtimeState"`
}

func (router *Router) bootstrapHandler(w http.ResponseWriter, request *http.Request) {
	client, err := router.authenticateClient(request)
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	response, err := router.buildBootstrap(request.Context(), client)
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	writeJSON(router, w, request, http.StatusOK, response)
}

func (router *Router) buildBootstrap(ctx context.Context, client v2domain.Client) (bootstrapResponse, error) {
	response := bootstrapResponse{
		Client:  clientBrief{ClientID: string(client.ID), Revision: int64(client.Revision)},
		Catalog: bootstrapCatalog{}, Sources: []bootstrapSource{},
	}
	if catalog, err := router.repo.GetActiveManifestCatalog(ctx); err == nil {
		response.Catalog = bootstrapCatalog{CatalogID: catalog.CatalogID, CatalogSequence: catalog.CatalogSequence, ManifestHash: catalog.ManifestHash, ManifestURL: catalog.ManifestURL}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return bootstrapResponse{}, err
	}
	_, inventoryRevision, err := router.repo.GetClientInventory(ctx, string(client.ID))
	if err != nil {
		return bootstrapResponse{}, err
	}
	response.InventoryRevision = int64(inventoryRevision)
	err = router.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		var low, high int64
		var resync int
		var resyncReason sql.NullString
		if err := tx.QueryRowContext(ctx, `
			SELECT low_change_seq, high_change_seq FROM client_change_watermarks WHERE client_id = ?`, string(client.ID)).Scan(&low, &high); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `
			SELECT resync_required, resync_reason FROM client_sync_state WHERE client_id = ?`, string(client.ID)).Scan(&resync, &resyncReason); err != nil {
			return err
		}
		cursor, err := v2crypto.EncodeCursor(router.repo.Keys().CursorMAC, string(client.ID), high)
		if err != nil {
			return err
		}
		response.Sync = bootstrapSync{Cursor: cursor, LowWatermark: low, HighWatermark: high, ResyncRequired: resync == 1, ResyncReason: resyncReason.String}
		rows, err := tx.QueryContext(ctx, `
			SELECT a.artifact_id, COALESCE(p.package_release_id, ''), a.managed_state,
			       COALESCE(rs.state, 'paused')
			FROM source_artifacts a
			LEFT JOIN source_package_releases p ON p.artifact_id = a.artifact_id AND p.state = 'active'
			LEFT JOIN source_runtime_state rs ON rs.artifact_id = a.artifact_id
			ORDER BY a.artifact_id`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var source bootstrapSource
			if err := rows.Scan(&source.ArtifactID, &source.PackageReleaseID, &source.ManagedState, &source.RuntimeState); err != nil {
				return err
			}
			response.Sources = append(response.Sources, source)
		}
		return rows.Err()
	})
	return response, err
}

// Keep the v2store import in this file's narrow bootstrap boundary instead of
// exposing database implementation details through the API package.
