package v2api

import (
	"net/http"

	"venera-server/internal/v2store"
)

type claimEnrollmentBody struct {
	PendingClientID string `json:"pendingClientId"`
	EnrollmentCode  string `json:"enrollmentCode"`
	DisplayName     string `json:"displayName"`
	Platform        string `json:"platform"`
	AppVersion      string `json:"appVersion"`
}

type clientBrief struct {
	ClientID string `json:"clientId"`
	Revision int64  `json:"revision"`
	State    string `json:"state"`
}

type claimEnrollmentResponse struct {
	Client            clientBrief `json:"client"`
	InitialCursor     string      `json:"initialCursor"`
	BootstrapRequired bool        `json:"bootstrapRequired"`
}

func (router *Router) claimEnrollmentHandler(w http.ResponseWriter, request *http.Request) {
	pendingDigest, err := router.requirePendingToken(request)
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	idempotencyKey, err := requireIdempotencyKey(request)
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	var body claimEnrollmentBody
	if err := decodeJSON(w, request, &body); err != nil {
		writeAPIError(router, w, request, http.StatusBadRequest, "invalid_request", "request is invalid", false, nil)
		return
	}
	if body.PendingClientID == "" {
		writeAPIError(router, w, request, http.StatusBadRequest, "invalid_request", "request is invalid", false, nil)
		return
	}
	claimRequest := v2store.ClaimClientEnrollmentRequest{
		PendingClientID: body.PendingClientID, TokenDigest: pendingDigest,
		DisplayName: body.DisplayName, Platform: body.Platform, AppVersion: body.AppVersion,
		IdempotencyKey: idempotencyKey,
	}
	var result v2store.EnrollmentResult
	if body.EnrollmentCode == "" {
		if !router.cfg.DevOpenEnrollment {
			writeAPIError(router, w, request, http.StatusBadRequest, "enrollment_code_required", "enrollment code is required", false, nil)
			return
		}
		result, err = router.repo.ClaimClientOpenEnrollment(request.Context(), claimRequest)
	} else {
		claimRequest.EnrollmentCodeDigest = router.repo.EnrollmentCodeDigest(body.EnrollmentCode)
		result, err = router.repo.ClaimClientEnrollment(request.Context(), claimRequest)
	}
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	writeJSON(router, w, request, http.StatusCreated, claimEnrollmentResponse{
		Client:        clientBrief{ClientID: string(result.Client.ID), Revision: int64(result.Client.Revision), State: string(result.Client.State)},
		InitialCursor: result.InitialCursor, BootstrapRequired: true,
	})
}
