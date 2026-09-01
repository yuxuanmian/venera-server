package v2api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"venera-server/internal/v2domain"
	"venera-server/internal/v2manifest"
	"venera-server/internal/v2store"
)

type sourceAccountWire struct {
	SourceAccountID string         `json:"sourceAccountId"`
	ArtifactID      string         `json:"artifactId"`
	Identity        identityWire   `json:"identity"`
	Attributes      map[string]any `json:"attributes"`
	VisibilityScope string         `json:"visibilityScope"`
	AccountState    string         `json:"accountState"`
	LinkState       string         `json:"linkState"`
	Selected        bool           `json:"selected"`
	AccountRevision int64          `json:"accountRevision"`
	LinkRevision    int64          `json:"linkRevision"`
	Revision        int64          `json:"revision"`
}

type identityWire struct {
	Scheme  string `json:"scheme"`
	Display string `json:"display"`
}

func (router *Router) sourceAccountsHandler(w http.ResponseWriter, request *http.Request) {
	client, err := router.authenticateClient(request)
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	items, err := router.repo.ListClientSourceAccounts(request.Context(), string(client.ID))
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	artifactID := strings.TrimSpace(request.URL.Query().Get("artifactId"))
	result := make([]sourceAccountWire, 0, len(items))
	for _, item := range items {
		if item.LinkState != v2domain.LinkLinked || artifactID != "" && string(item.ArtifactID) != artifactID {
			continue
		}
		result = append(result, sourceAccountSummaryWire(item))
	}
	writeJSON(router, w, request, http.StatusOK, map[string]any{"items": result})
}

func sourceAccountSummaryWire(item v2domain.SourceAccountSummary) sourceAccountWire {
	attributes := map[string]any{}
	if item.AttributesJSON != "" {
		if err := json.Unmarshal([]byte(item.AttributesJSON), &attributes); err != nil || attributes == nil {
			attributes = map[string]any{}
		}
	}
	return sourceAccountWire{
		SourceAccountID: string(item.SourceAccountID), ArtifactID: string(item.ArtifactID),
		Identity:   identityWire{Scheme: item.IdentityScheme, Display: item.IdentityDisplay},
		Attributes: attributes, VisibilityScope: item.VisibilityScope,
		AccountState: string(item.AccountState), LinkState: string(item.LinkState),
		Selected: item.Selected, AccountRevision: int64(item.AccountRevision),
		LinkRevision: int64(item.LinkRevision), Revision: int64(item.LinkRevision),
	}
}

func (router *Router) sourceAccountForResult(ctx context.Context, clientID string, account v2domain.SourceAccount) sourceAccountWire {
	items, err := router.repo.ListClientSourceAccounts(ctx, clientID)
	if err == nil {
		for _, item := range items {
			if item.SourceAccountID == account.ID && item.LinkState == v2domain.LinkLinked {
				return sourceAccountSummaryWire(item)
			}
		}
	}
	attributes := map[string]any{}
	_ = json.Unmarshal([]byte(account.AttributesJSON), &attributes)
	if attributes == nil {
		attributes = map[string]any{}
	}
	return sourceAccountWire{
		SourceAccountID: string(account.ID), ArtifactID: string(account.ArtifactID),
		Identity:   identityWire{Scheme: account.IdentityScheme, Display: account.IdentityDisplay},
		Attributes: attributes, VisibilityScope: account.VisibilityScope,
		AccountState: string(account.State), LinkState: string(v2domain.LinkLinked), Selected: true,
		AccountRevision: int64(account.Revision), LinkRevision: 0, Revision: 0,
	}
}

func activeManifestPackage(ctx context.Context, repo *v2store.Repository, artifactID, packageReleaseID string) (v2manifest.SourcePackage, error) {
	record, err := repo.GetActiveManifestCatalog(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return v2manifest.SourcePackage{}, errSourceRuntimeBlocked
	}
	if err != nil {
		return v2manifest.SourcePackage{}, err
	}
	manifest, err := v2manifest.Parse([]byte(record.ManifestJSON))
	if err != nil {
		return v2manifest.SourcePackage{}, errSourceRuntimeBlocked
	}
	for _, pkg := range manifest.SourcePackages {
		if pkg.ArtifactID == artifactID && pkg.PackageReleaseID == packageReleaseID {
			return pkg, nil
		}
	}
	return v2manifest.SourcePackage{}, v2store.ErrPackageReleaseNotFound
}

func compatibleInventoryForPackage(ctx context.Context, repo *v2store.Repository, clientID string, pkg v2manifest.SourcePackage) error {
	entries, _, err := repo.GetClientInventory(ctx, clientID)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.ArtifactID != v2domain.ArtifactID(pkg.ArtifactID) {
			continue
		}
		if entry.ManagementMode != "managed" || entry.CompatibilityState != v2domain.CompatibilityCompatible || string(entry.PackageReleaseID) != pkg.PackageReleaseID {
			return v2store.ErrInventoryIncompatible
		}
		return nil
	}
	return v2store.ErrInventoryIncompatible
}

var (
	errSourceRuntimeBlocked = errors.New("source runtime is blocked")
	errSourceNotManaged     = errors.New("source is not managed")
)
