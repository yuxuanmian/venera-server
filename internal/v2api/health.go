package v2api

import (
	"database/sql"
	"errors"
	"net/http"
)

type healthResponse struct {
	Status   string `json:"status"`
	Protocol struct {
		Major int `json:"major"`
		Minor int `json:"minor"`
	} `json:"protocol"`
	CatalogID       string `json:"catalogId"`
	CatalogSequence int64  `json:"catalogSequence"`
	ServerVersion   string `json:"serverVersion"`
}

func (router *Router) healthHandler(w http.ResponseWriter, request *http.Request) {
	response := healthResponse{Status: "ok", CatalogID: "", CatalogSequence: 0, ServerVersion: router.cfg.MinServerVersion}
	response.Protocol.Major = 2
	response.Protocol.Minor = 0
	if router.repo != nil {
		catalog, err := router.repo.GetActiveManifestCatalog(request.Context())
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			writeStoreError(router, w, request, err)
			return
		}
		if err == nil {
			response.CatalogID = catalog.CatalogID
			response.CatalogSequence = catalog.CatalogSequence
		}
	}
	writeJSON(router, w, request, http.StatusOK, response)
}
