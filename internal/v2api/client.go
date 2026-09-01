package v2api

import (
	"errors"
	"net/http"

	"venera-server/internal/v2domain"
	"venera-server/internal/v2store"
)

type clientSelfResponse struct {
	ClientID    string      `json:"clientId"`
	DisplayName string      `json:"displayName"`
	Platform    string      `json:"platform"`
	AppVersion  string      `json:"appVersion"`
	State       string      `json:"state"`
	Revision    int64       `json:"revision"`
	LastSeenAt  interface{} `json:"lastSeenAt"`
}

type revokeClientBody struct {
	ExpectedRevision *int64 `json:"expectedRevision"`
}

func (router *Router) clientSelfHandler(w http.ResponseWriter, request *http.Request) {
	client, err := router.authenticateClient(request)
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	var lastSeen interface{}
	if client.LastSeenAt != nil {
		lastSeen = client.LastSeenAt
	}
	writeJSON(router, w, request, http.StatusOK, clientSelfResponse{
		ClientID: string(client.ID), DisplayName: client.DisplayName, Platform: client.Platform,
		AppVersion: client.AppVersion, State: string(client.State), Revision: int64(client.Revision), LastSeenAt: lastSeen,
	})
}

func (router *Router) revokeClientHandler(w http.ResponseWriter, request *http.Request) {
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
	var body revokeClientBody
	if err := decodeJSON(w, request, &body); err != nil || body.ExpectedRevision == nil || *body.ExpectedRevision < 1 {
		writeAPIError(router, w, request, http.StatusBadRequest, "invalid_request", "request is invalid", false, nil)
		return
	}
	if err := router.repo.RevokeClient(request.Context(), string(client.ID), v2domain.Revision(*body.ExpectedRevision), idempotencyKey); err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	revoked, err := router.repo.GetClient(request.Context(), string(client.ID))
	if errors.Is(err, v2store.ErrClientNotFound) {
		writeStoreError(router, w, request, err)
		return
	}
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	writeJSON(router, w, request, http.StatusOK, clientSelfResponse{
		ClientID: string(revoked.ID), DisplayName: revoked.DisplayName, Platform: revoked.Platform,
		AppVersion: revoked.AppVersion, State: string(revoked.State), Revision: int64(revoked.Revision),
	})
}

func writeStoreError(router *Router, w http.ResponseWriter, request *http.Request, err error) {
	status, code, message, retryable := classifyStoreError(err)
	writeAPIError(router, w, request, status, code, message, retryable, nil)
}

func classifyStoreError(err error) (int, string, string, bool) {
	switch {
	case errors.Is(err, v2store.ErrAuthenticationFailed), errors.Is(err, v2store.ErrClientRevoked):
		return http.StatusUnauthorized, "authentication_failed", "authentication failed", false
	case errors.Is(err, v2store.ErrInvalidArgument), errors.Is(err, v2store.ErrIdempotencyKeyRequired):
		return http.StatusBadRequest, "invalid_request", "request is invalid", false
	case errors.Is(err, v2store.ErrEnrollmentCodeExpired):
		return http.StatusGone, "enrollment_code_expired", "enrollment code expired", false
	case errors.Is(err, v2store.ErrCandidateExpired):
		return http.StatusGone, "candidate_expired", "candidate expired", false
	case errors.Is(err, v2store.ErrCloudPreparationExpired):
		return http.StatusGone, "preparation_expired", "preparation expired", false
	case errors.Is(err, v2store.ErrRevisionConflict), errors.Is(err, v2store.ErrExpectedRevisionMismatch):
		return http.StatusConflict, "revision_conflict", "resource changed", false
	case errors.Is(err, v2store.ErrIdempotencyConflict):
		return http.StatusConflict, "idempotency_conflict", "idempotency key conflicts with a previous request", false
	case errors.Is(err, v2store.ErrIdempotencyInProgress):
		return http.StatusConflict, "idempotency_in_progress", "request is already in progress", true
	case errors.Is(err, v2store.ErrSwitchConfirmationRequired):
		return http.StatusConflict, "switch_confirmation_required", "source account switch confirmation is required", false
	case errors.Is(err, v2store.ErrCloudClaimMustBeRemoved):
		return http.StatusConflict, "cloud_claim_must_be_removed", "cloud claim must be removed before switching source account", false
	case errors.Is(err, v2store.ErrInventoryIncompatible), errors.Is(err, v2store.ErrCloudClaimNotAllowed):
		return http.StatusConflict, "preparation_required", "cloud preparation is required", false
	case errors.Is(err, v2store.ErrClientNotFound), errors.Is(err, v2store.ErrEnrollmentCodeNotFound), errors.Is(err, v2store.ErrCandidateNotFound), errors.Is(err, v2store.ErrCloudPreparationNotFound), errors.Is(err, v2store.ErrCloudClaimNotFound):
		return http.StatusNotFound, "resource_not_found", "resource not found", false
	default:
		return http.StatusInternalServerError, "internal_error", "internal server error", false
	}
}
