package v2api

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"venera-server/internal/v2crypto"
	"venera-server/internal/v2domain"
	"venera-server/internal/v2manifest"
	"venera-server/internal/v2scan"
	"venera-server/internal/v2store"
)

const (
	sessionCandidateTTL       = 15 * time.Minute
	sessionCandidateMaxBytes  = 16 << 10
	sessionCandidateRetention = 24 * time.Hour
)

type createSessionCandidateRequest struct {
	ArtifactID       string                 `json:"artifactId"`
	PackageReleaseID string                 `json:"packageReleaseId"`
	ExportProfileID  string                 `json:"exportProfileId"`
	Session          candidateSessionExport `json:"session"`
}

type candidateSessionExport struct {
	Cookies []candidateCookie `json:"cookies"`
}

type candidateCookie struct {
	Name     string     `json:"name"`
	Value    string     `json:"value"`
	Domain   string     `json:"domain"`
	Path     string     `json:"path"`
	Secure   bool       `json:"secure"`
	HTTPOnly bool       `json:"httpOnly,omitempty"`
	Expires  *time.Time `json:"expires,omitempty"`
	SameSite string     `json:"sameSite,omitempty"`
}

type candidateWire struct {
	CandidateID string             `json:"candidateId"`
	State       string             `json:"state"`
	ExpiresAt   time.Time          `json:"expiresAt"`
	Revision    int64              `json:"revision"`
	FailureCode string             `json:"failureCode,omitempty"`
	Source      *sourceAccountWire `json:"sourceAccount,omitempty"`
}

type confirmSessionCandidateRequest struct {
	ExpectedCandidateRevision   *int64 `json:"expectedCandidateRevision"`
	ExpectedCurrentLinkRevision *int64 `json:"expectedCurrentLinkRevision"`
}

type cancelSessionCandidateRequest struct {
	ExpectedCandidateRevision *int64 `json:"expectedCandidateRevision"`
}

type candidateRecord struct {
	Candidate v2store.SessionCandidate
}

func (router *Router) createSessionCandidateHandler(w http.ResponseWriter, request *http.Request) {
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
	var body createSessionCandidateRequest
	if err := decodeJSON(w, request, &body); err != nil {
		writeAPIError(router, w, request, http.StatusBadRequest, "invalid_request", "request is invalid", false, nil)
		return
	}
	pkg, err := activeManifestPackage(request.Context(), router.repo, body.ArtifactID, body.PackageReleaseID)
	if err != nil {
		writeCandidateError(router, w, request, err)
		return
	}
	if pkg.ManagedState != "active" {
		writeCandidateError(router, w, request, errSourceNotManaged)
		return
	}
	if body.ExportProfileID != pkg.SessionExportProfile.ID {
		writeAPIError(router, w, request, http.StatusBadRequest, "invalid_request", "request is invalid", false, nil)
		return
	}
	if err := compatibleInventoryForPackage(request.Context(), router.repo, string(client.ID), pkg); err != nil {
		writeCandidateError(router, w, request, err)
		return
	}
	cookies, canonicalSession, err := validateCandidateSession(body.Session, pkg.SessionExportProfile, router.serverNow())
	if err != nil {
		writeAPIError(router, w, request, http.StatusBadRequest, "invalid_request", "request is invalid", false, nil)
		return
	}
	envelope, err := v2crypto.SealSession(router.repo.Keys().SessionAEAD, canonicalSession, []byte("candidate:"+string(client.ID)+":"+pkg.ArtifactID))
	if err != nil {
		writeAPIError(router, w, request, http.StatusInternalServerError, "internal_error", "internal server error", false, nil)
		return
	}
	digest := v2crypto.Digest(router.repo.Keys().CredentialHMAC, "session-candidate-v1\x00"+string(canonicalSession))
	staged, err := router.repo.StageSessionCandidate(request.Context(), v2store.StageSessionCandidateRequest{
		ClientID: string(client.ID), ArtifactID: pkg.ArtifactID, PackageReleaseID: pkg.PackageReleaseID,
		ExportProfileID: pkg.SessionExportProfile.ID, SessionEnvelope: envelope, SessionDigest: digest,
		TTL: sessionCandidateTTL, IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		writeCandidateError(router, w, request, err)
		return
	}
	candidate, err := router.loadCandidateForAPI(request.Context(), string(client.ID), staged.ID)
	if err != nil {
		writeCandidateError(router, w, request, err)
		return
	}
	if candidate.Candidate.State == v2domain.CandidateActivated {
		router.writeActivatedCandidate(w, request, client, candidate.Candidate)
		return
	}
	if candidate.Candidate.State == v2domain.CandidateFailed || candidate.Candidate.State == v2domain.CandidateCancelled || candidate.Candidate.State == v2domain.CandidateExpired {
		writeJSON(router, w, request, http.StatusOK, candidateWireFrom(candidate.Candidate))
		return
	}
	if router.probe == nil {
		writeJSON(router, w, request, http.StatusAccepted, candidateWireFrom(candidate.Candidate))
		return
	}
	if candidate.Candidate.State == v2domain.CandidateSwitchConfirmationRequired {
		writeSwitchConfirmationRequired(router, w, request, candidate.Candidate)
		return
	}
	if candidate.Candidate.State == v2domain.CandidateReadyLink {
		router.activateStagedCandidate(w, request, client, candidate.Candidate, idempotencyKey)
		return
	}

	probeResult, err := router.probe.Probe(request.Context(), v2scan.ProbeRequest{
		RequestID: requestIDFromContext(request), ArtifactID: pkg.ArtifactID, PackageReleaseID: pkg.PackageReleaseID,
		Contract: pkg.AccountProbeContract, SessionCookies: cookies, Reason: "candidate_validation", RequestedAt: router.serverNow(),
	})
	if err != nil {
		failureCode := probeFailureCode(err)
		if updateErr := router.failCandidate(request.Context(), string(client.ID), candidate.Candidate.ID, candidate.Candidate.Revision, failureCode); updateErr != nil && !errors.Is(updateErr, v2store.ErrCandidateExpired) {
			writeCandidateError(router, w, request, updateErr)
			return
		}
		if errors.Is(err, v2store.ErrCandidateExpired) {
			writeCandidateError(router, w, request, err)
			return
		}
		writeAPIError(router, w, request, http.StatusUnprocessableEntity, "source_account_probe_failed", "source account probe failed", probeRetryable(err), nil)
		return
	}
	mergedCookies, err := mergeAndValidateCookies(cookies, probeResult.SessionCookies, pkg.SessionExportProfile, router.serverNow())
	if err != nil {
		_ = router.failCandidate(request.Context(), string(client.ID), candidate.Candidate.ID, candidate.Candidate.Revision, "extension_failure")
		writeAPIError(router, w, request, http.StatusUnprocessableEntity, "source_account_probe_failed", "source account probe failed", false, nil)
		return
	}
	mergedSession := candidateSessionExport{Cookies: make([]candidateCookie, 0, len(mergedCookies))}
	for _, cookie := range mergedCookies {
		mergedSession.Cookies = append(mergedSession.Cookies, cookieToWire(cookie, pkg.SessionExportProfile))
	}
	canonicalSession, err = json.Marshal(mergedSession)
	if err != nil || int64(len(canonicalSession)) > pkg.SessionExportProfile.MaxSerializedBytes || len(canonicalSession) > sessionCandidateMaxBytes {
		_ = router.failCandidate(request.Context(), string(client.ID), candidate.Candidate.ID, candidate.Candidate.Revision, "extension_failure")
		writeAPIError(router, w, request, http.StatusUnprocessableEntity, "source_account_probe_failed", "source account probe failed", false, nil)
		return
	}
	envelope, err = v2crypto.SealSession(router.repo.Keys().SessionAEAD, canonicalSession, []byte("candidate:"+string(client.ID)+":"+pkg.ArtifactID))
	if err != nil || router.replaceCandidateEnvelope(request.Context(), string(client.ID), candidate.Candidate.ID, candidate.Candidate.Revision, envelope, v2crypto.Digest(router.repo.Keys().CredentialHMAC, "session-candidate-v1\x00"+string(canonicalSession))) != nil {
		writeCandidateError(router, w, request, errCandidateProbeRace)
		return
	}
	identityCiphertext, err := v2crypto.SealSession(router.repo.Keys().SessionAEAD, []byte(probeResult.IdentityValue), []byte("candidate-identity:"+string(client.ID)+":"+candidate.Candidate.ID))
	if err != nil {
		writeCandidateError(router, w, request, err)
		return
	}
	completed, err := router.repo.CompleteCandidateProbe(request.Context(), v2store.CompleteCandidateProbeRequest{
		ClientID: string(client.ID), CandidateID: candidate.Candidate.ID, ExpectedRevision: candidate.Candidate.Revision,
		IdentityScheme: probeResult.IdentityScheme, IdentityCiphertext: identityCiphertext,
		IdentityDigest:  v2crypto.IdentityDigest(router.repo.Keys().IdentityHMAC, pkg.ArtifactID, probeResult.IdentityScheme, probeResult.IdentityValue),
		IdentityDisplay: probeResult.DisplayName, AttributesJSON: probeResult.AttributesJSON, VisibilityScope: probeResult.VisibilityScope,
		ScopeFreshUntil: router.serverNow().Add(24 * time.Hour), IdempotencyKey: candidateOperationKey(router.repo.Keys(), "probe", candidate.Candidate.ID, idempotencyKey),
	})
	if err != nil {
		if errors.Is(err, v2store.ErrCandidateExpired) {
			_ = router.expireCandidate(request.Context(), string(client.ID), candidate.Candidate.ID)
		}
		writeCandidateError(router, w, request, err)
		return
	}
	router.activateStagedCandidate(w, request, client, completed, idempotencyKey)
}

func (router *Router) activateStagedCandidate(w http.ResponseWriter, request *http.Request, client v2domain.Client, candidate v2store.SessionCandidate, idempotencyKey string) {
	result, err := router.repo.ActivateOrLinkSourceAccount(request.Context(), v2store.ActivateSourceAccountRequest{
		ClientID: string(client.ID), CandidateID: candidate.ID, ExpectedRevision: candidate.Revision,
		IdempotencyKey: candidateOperationKey(router.repo.Keys(), "activate", candidate.ID, idempotencyKey),
	})
	if errors.Is(err, v2store.ErrSwitchConfirmationRequired) {
		_ = router.markSwitchConfirmation(request.Context(), string(client.ID), candidate.ID, candidate.Revision)
		updated, loadErr := router.loadCandidateForAPI(request.Context(), string(client.ID), candidate.ID)
		if loadErr == nil {
			writeSwitchConfirmationRequired(router, w, request, updated.Candidate)
			return
		}
	}
	if err != nil {
		writeCandidateError(router, w, request, err)
		return
	}
	responseCandidate, loadErr := router.loadCandidateForAPI(request.Context(), string(client.ID), result.Candidate.ID)
	if loadErr != nil {
		writeCandidateError(router, w, request, loadErr)
		return
	}
	writeJSON(router, w, request, http.StatusCreated, candidateWire{
		CandidateID: responseCandidate.Candidate.ID, State: string(v2domain.CandidateActivated),
		ExpiresAt: responseCandidate.Candidate.ExpiresAt, Revision: int64(responseCandidate.Candidate.Revision),
		Source: sourceAccountWirePtr(router.sourceAccountForResult(request.Context(), string(client.ID), result.SourceAccount)),
	})
}

func (router *Router) writeActivatedCandidate(w http.ResponseWriter, request *http.Request, client v2domain.Client, candidate v2store.SessionCandidate) {
	if candidate.TargetSourceAccountID == "" {
		writeCandidateError(router, w, request, v2store.ErrCandidateState)
		return
	}
	items, err := router.repo.ListClientSourceAccounts(request.Context(), string(client.ID))
	if err != nil {
		writeCandidateError(router, w, request, err)
		return
	}
	for _, item := range items {
		if string(item.SourceAccountID) == candidate.TargetSourceAccountID && item.LinkState == v2domain.LinkLinked {
			wire := sourceAccountSummaryWire(item)
			writeJSON(router, w, request, http.StatusCreated, candidateWire{CandidateID: candidate.ID, State: string(v2domain.CandidateActivated), ExpiresAt: candidate.ExpiresAt, Revision: int64(candidate.Revision), Source: &wire})
			return
		}
	}
	writeCandidateError(router, w, request, v2store.ErrCandidateState)
}

func (router *Router) getSessionCandidateHandler(w http.ResponseWriter, request *http.Request) {
	client, err := router.authenticateClient(request)
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	candidateID := request.PathValue("candidateId")
	if err := router.expireCandidate(request.Context(), string(client.ID), candidateID); err != nil {
		writeCandidateError(router, w, request, err)
		return
	}
	candidate, err := router.loadCandidateForAPI(request.Context(), string(client.ID), candidateID)
	if err != nil {
		writeCandidateError(router, w, request, err)
		return
	}
	writeJSON(router, w, request, http.StatusOK, candidateWireFrom(candidate.Candidate))
}

func (router *Router) confirmSessionCandidateHandler(w http.ResponseWriter, request *http.Request) {
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
	var body confirmSessionCandidateRequest
	if err := decodeJSON(w, request, &body); err != nil || body.ExpectedCandidateRevision == nil || *body.ExpectedCandidateRevision < 1 || body.ExpectedCurrentLinkRevision == nil || *body.ExpectedCurrentLinkRevision < 0 {
		writeAPIError(router, w, request, http.StatusBadRequest, "invalid_request", "request is invalid", false, nil)
		return
	}
	candidateID := request.PathValue("candidateId")
	candidate, err := router.loadCandidateForAPI(request.Context(), string(client.ID), candidateID)
	if err != nil {
		writeCandidateError(router, w, request, err)
		return
	}
	if candidate.Candidate.State != v2domain.CandidateActivated {
		current, currentErr := router.repo.GetSelectedClientSourceAccount(request.Context(), string(client.ID), candidate.Candidate.ArtifactID)
		if errors.Is(currentErr, v2store.ErrSourceAccountNotSelected) {
			if *body.ExpectedCurrentLinkRevision != 0 {
				writeCandidateError(router, w, request, v2store.ErrRevisionConflict)
				return
			}
		} else if currentErr != nil {
			writeCandidateError(router, w, request, currentErr)
			return
		} else if int64(current.Revision) != *body.ExpectedCurrentLinkRevision {
			writeCandidateError(router, w, request, v2store.ErrRevisionConflict)
			return
		}
	}
	result, err := router.repo.ConfirmClientSourceSwitch(request.Context(), v2store.ActivateSourceAccountRequest{
		ClientID: string(client.ID), CandidateID: candidateID, ExpectedRevision: v2domain.Revision(*body.ExpectedCandidateRevision),
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		writeCandidateError(router, w, request, err)
		return
	}
	updated, err := router.loadCandidateForAPI(request.Context(), string(client.ID), candidateID)
	if err != nil {
		writeCandidateError(router, w, request, err)
		return
	}
	writeJSON(router, w, request, http.StatusOK, candidateWire{
		CandidateID: updated.Candidate.ID, State: string(v2domain.CandidateActivated), ExpiresAt: updated.Candidate.ExpiresAt,
		Revision: int64(updated.Candidate.Revision), Source: sourceAccountWirePtr(router.sourceAccountForResult(request.Context(), string(client.ID), result.SourceAccount)),
	})
}

func (router *Router) cancelSessionCandidateHandler(w http.ResponseWriter, request *http.Request) {
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
	var body cancelSessionCandidateRequest
	if err := decodeJSON(w, request, &body); err != nil || body.ExpectedCandidateRevision == nil || *body.ExpectedCandidateRevision < 1 {
		writeAPIError(router, w, request, http.StatusBadRequest, "invalid_request", "request is invalid", false, nil)
		return
	}
	if err := router.expireCandidate(request.Context(), string(client.ID), request.PathValue("candidateId")); err != nil {
		writeCandidateError(router, w, request, err)
		return
	}
	result, err := router.cancelCandidate(request.Context(), string(client.ID), request.PathValue("candidateId"), v2domain.Revision(*body.ExpectedCandidateRevision), idempotencyKey)
	if err != nil {
		writeCandidateError(router, w, request, err)
		return
	}
	writeJSON(router, w, request, http.StatusOK, result)
}

func (router *Router) loadCandidateForAPI(ctx context.Context, clientID, candidateID string) (candidateRecord, error) {
	if clientID == "" || candidateID == "" {
		return candidateRecord{}, v2store.ErrCandidateNotFound
	}
	var candidate v2store.SessionCandidate
	err := router.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		var target sql.NullString
		var failure, expires string
		var revision int64
		if err := tx.QueryRowContext(ctx, `
			SELECT candidate_id, artifact_id, target_source_account_id, package_release_id,
			       export_profile_id, session_digest, state, COALESCE(failure_code, ''), expires_at, revision
			FROM source_session_candidates WHERE candidate_id = ? AND client_id = ?`, candidateID, clientID).Scan(
			&candidate.ID, &candidate.ArtifactID, &target, &candidate.PackageReleaseID, &candidate.ExportProfileID,
			&candidate.SessionDigest, &candidate.State, &failure, &expires, &revision); errors.Is(err, sql.ErrNoRows) {
			return v2store.ErrCandidateNotFound
		} else if err != nil {
			return err
		}
		candidate.ClientID = clientID
		if target.Valid {
			candidate.TargetSourceAccountID = target.String
		}
		candidate.FailureCode = failure
		candidate.Revision = v2domain.Revision(revision)
		parsedExpires, parseErr := time.Parse(time.RFC3339Nano, expires)
		candidate.ExpiresAt = parsedExpires
		return parseErr
	})
	return candidateRecord{Candidate: candidate}, err
}

func (router *Router) expireCandidate(ctx context.Context, clientID, candidateID string) error {
	return router.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		_, err := tx.ExecContext(ctx, `
			UPDATE source_session_candidates SET state = 'expired', failure_code = NULL,
			       session_envelope = NULL, probed_identity_ciphertext = NULL,
			       revision = revision + 1, updated_at = ?
			WHERE candidate_id = ? AND client_id = ? AND state IN ('probing','readyLink','switchConfirmationRequired') AND expires_at <= ?`,
			router.serverNow().UTC().Format(time.RFC3339Nano), candidateID, clientID, router.serverNow().UTC().Format(time.RFC3339Nano))
		return err
	})
}

func (router *Router) replaceCandidateEnvelope(ctx context.Context, clientID, candidateID string, revision v2domain.Revision, envelope []byte, digest string) error {
	return router.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		result, err := tx.ExecContext(ctx, `
			UPDATE source_session_candidates SET session_envelope = ?, session_digest = ?, updated_at = ?
			WHERE candidate_id = ? AND client_id = ? AND state = 'probing' AND revision = ?`,
			envelope, digest, router.serverNow().UTC().Format(time.RFC3339Nano), candidateID, clientID, revision)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return errCandidateProbeRace
		}
		return nil
	})
}

func (router *Router) failCandidate(ctx context.Context, clientID, candidateID string, revision v2domain.Revision, failureCode string) error {
	return router.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		result, err := tx.ExecContext(ctx, `
			UPDATE source_session_candidates SET state = 'failed', failure_code = ?,
			       session_envelope = NULL, probed_identity_ciphertext = NULL,
			       revision = revision + 1, updated_at = ?
			WHERE candidate_id = ? AND client_id = ? AND state = 'probing' AND revision = ?`,
			failureCode, router.serverNow().UTC().Format(time.RFC3339Nano), candidateID, clientID, revision)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return errCandidateProbeRace
		}
		return nil
	})
}

func (router *Router) markSwitchConfirmation(ctx context.Context, clientID, candidateID string, revision v2domain.Revision) error {
	return router.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		result, err := tx.ExecContext(ctx, `
			UPDATE source_session_candidates SET state = 'switchConfirmationRequired', revision = revision + 1, updated_at = ?
			WHERE candidate_id = ? AND client_id = ? AND state = 'readyLink' AND revision = ?`,
			router.serverNow().UTC().Format(time.RFC3339Nano), candidateID, clientID, revision)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return errCandidateProbeRace
		}
		return nil
	})
}

func (router *Router) cancelCandidate(ctx context.Context, clientID, candidateID string, revision v2domain.Revision, idempotencyKey string) (candidateWire, error) {
	requestDigestBytes, err := json.Marshal(struct {
		CandidateID string            `json:"candidateId"`
		Revision    v2domain.Revision `json:"revision"`
	}{candidateID, revision})
	if err != nil {
		return candidateWire{}, err
	}
	requestSum := sha256.Sum256(requestDigestBytes)
	idempotencyDigest := v2crypto.Digest(router.repo.Keys().CredentialHMAC, idempotencyKey)
	var result candidateWire
	err = router.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		var storedDigest, state, expires string
		var status sql.NullInt64
		var storedBody []byte
		err := tx.QueryRowContext(ctx, `SELECT request_digest, response_status, response_body, state, expires_at FROM idempotency_records WHERE actor_kind = 'client' AND actor_id = ? AND route_key = 'cancel-session-candidate' AND idempotency_digest = ?`, clientID, idempotencyDigest).Scan(&storedDigest, &status, &storedBody, &state, &expires)
		if err == nil {
			expiresAt, parseErr := time.Parse(time.RFC3339Nano, expires)
			if parseErr == nil && expiresAt.After(router.serverNow()) {
				if storedDigest != fmt.Sprintf("%x", requestSum[:]) {
					return v2store.ErrIdempotencyConflict
				}
				if state == "completed" {
					return json.Unmarshal(storedBody, &result)
				}
				return v2store.ErrIdempotencyInProgress
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM idempotency_records WHERE actor_kind = 'client' AND actor_id = ? AND route_key = 'cancel-session-candidate' AND idempotency_digest = ?`, clientID, idempotencyDigest); err != nil {
				return err
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		now := router.serverNow()
		if _, err := tx.ExecContext(ctx, `INSERT INTO idempotency_records(actor_kind, actor_id, route_key, idempotency_digest, request_digest, state, expires_at, created_at, updated_at) VALUES('client', ?, 'cancel-session-candidate', ?, ?, 'running', ?, ?, ?)`, clientID, idempotencyDigest, fmt.Sprintf("%x", requestSum[:]), now.Add(sessionCandidateRetention).Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)); err != nil {
			return err
		}
		var currentState, expiresAtText string
		var currentRevision int64
		if err := tx.QueryRowContext(ctx, `SELECT state, expires_at, revision FROM source_session_candidates WHERE candidate_id = ? AND client_id = ?`, candidateID, clientID).Scan(&currentState, &expiresAtText, &currentRevision); errors.Is(err, sql.ErrNoRows) {
			return v2store.ErrCandidateNotFound
		} else if err != nil {
			return err
		}
		if currentState == string(v2domain.CandidateExpired) {
			return v2store.ErrCandidateExpired
		}
		if currentRevision != int64(revision) {
			return v2store.ErrRevisionConflict
		}
		if currentState != string(v2domain.CandidateProbing) && currentState != string(v2domain.CandidateReadyLink) && currentState != string(v2domain.CandidateSwitchConfirmationRequired) {
			return v2store.ErrCandidateState
		}
		expiresAt, err := time.Parse(time.RFC3339Nano, expiresAtText)
		if err != nil {
			return err
		}
		if !expiresAt.After(now) {
			if _, err := tx.ExecContext(ctx, `UPDATE source_session_candidates SET state = 'expired', session_envelope = NULL, probed_identity_ciphertext = NULL, revision = revision + 1, updated_at = ? WHERE candidate_id = ? AND client_id = ? AND revision = ?`, now.Format(time.RFC3339Nano), candidateID, clientID, revision); err != nil {
				return err
			}
			return v2store.ErrCandidateExpired
		}
		if _, err := tx.ExecContext(ctx, `UPDATE source_session_candidates SET state = 'cancelled', session_envelope = NULL, probed_identity_ciphertext = NULL, revision = revision + 1, updated_at = ? WHERE candidate_id = ? AND client_id = ? AND revision = ?`, now.Format(time.RFC3339Nano), candidateID, clientID, revision); err != nil {
			return err
		}
		result = candidateWire{CandidateID: candidateID, State: string(v2domain.CandidateCancelled), ExpiresAt: expiresAt, Revision: int64(revision + 1)}
		body, err := json.Marshal(result)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE idempotency_records SET response_status = 200, response_body = ?, state = 'completed', updated_at = ? WHERE actor_kind = 'client' AND actor_id = ? AND route_key = 'cancel-session-candidate' AND idempotency_digest = ? AND state = 'running'`, body, now.Format(time.RFC3339Nano), clientID, idempotencyDigest)
		return err
	})
	return result, err
}

func validateCandidateSession(session candidateSessionExport, profile v2manifest.SessionExportProfile, now time.Time) ([]http.Cookie, []byte, error) {
	if len(session.Cookies) == 0 || len(session.Cookies) > profile.MaxCookies {
		return nil, nil, errors.New("cookie count is invalid")
	}
	canonical, err := json.Marshal(session)
	if err != nil || int64(len(canonical)) > profile.MaxSerializedBytes || len(canonical) > sessionCandidateMaxBytes {
		return nil, nil, errors.New("cookie export is too large")
	}
	cookies := make([]http.Cookie, 0, len(session.Cookies))
	seen := make(map[string]struct{}, len(session.Cookies))
	for _, wire := range session.Cookies {
		cookie, err := cookieFromWire(wire, profile, now)
		if err != nil {
			return nil, nil, err
		}
		key := strings.ToLower(cookie.Name) + "\x00" + strings.ToLower(cookie.Domain) + "\x00" + cookie.Path
		if _, ok := seen[key]; ok {
			return nil, nil, errors.New("duplicate cookie")
		}
		seen[key] = struct{}{}
		cookies = append(cookies, cookie)
	}
	return cookies, canonical, nil
}

func mergeAndValidateCookies(initial []http.Cookie, captured []http.Cookie, profile v2manifest.SessionExportProfile, now time.Time) ([]http.Cookie, error) {
	merged := make(map[string]http.Cookie, len(initial)+len(captured))
	order := make([]string, 0, len(initial)+len(captured))
	add := func(cookie http.Cookie) error {
		wire := cookieToWire(cookie, profile)
		validated, err := cookieFromWire(wire, profile, now)
		if err != nil {
			return err
		}
		key := strings.ToLower(validated.Name) + "\x00" + strings.ToLower(validated.Domain) + "\x00" + validated.Path
		if _, exists := merged[key]; !exists {
			order = append(order, key)
		}
		merged[key] = validated
		return nil
	}
	for _, cookie := range initial {
		if err := add(cookie); err != nil {
			return nil, err
		}
	}
	for _, cookie := range captured {
		if err := add(cookie); err != nil {
			return nil, err
		}
	}
	if len(order) == 0 || len(order) > profile.MaxCookies {
		return nil, errors.New("cookie count is invalid")
	}
	result := make([]http.Cookie, 0, len(order))
	for _, key := range order {
		result = append(result, merged[key])
	}
	canonical, err := json.Marshal(candidateSessionExport{Cookies: func() []candidateCookie {
		values := make([]candidateCookie, 0, len(result))
		for _, cookie := range result {
			values = append(values, cookieToWire(cookie, profile))
		}
		return values
	}()})
	if err != nil || int64(len(canonical)) > profile.MaxSerializedBytes || len(canonical) > sessionCandidateMaxBytes {
		return nil, errors.New("cookie export is too large")
	}
	return result, nil
}

func cookieFromWire(wire candidateCookie, profile v2manifest.SessionExportProfile, now time.Time) (http.Cookie, error) {
	if wire.Name == "" || wire.Domain == "" || wire.Path == "" || !cookieDomainAllowed(wire.Domain, profile.AllowedCookieDomains) || !strings.HasPrefix(wire.Path, "/") || strings.ContainsAny(wire.Path, "\x00\r\n") {
		return http.Cookie{}, errors.New("cookie is invalid")
	}
	if wire.Expires != nil && !wire.Expires.After(now) {
		return http.Cookie{}, errors.New("cookie is expired")
	}
	cookie := http.Cookie{Name: wire.Name, Value: wire.Value, Domain: wire.Domain, Path: wire.Path, Secure: wire.Secure, HttpOnly: wire.HTTPOnly}
	if wire.Expires != nil {
		cookie.Expires = wire.Expires.UTC()
	}
	switch strings.ToLower(wire.SameSite) {
	case "", "default":
		cookie.SameSite = http.SameSiteDefaultMode
	case "lax":
		cookie.SameSite = http.SameSiteLaxMode
	case "strict":
		cookie.SameSite = http.SameSiteStrictMode
	case "none":
		cookie.SameSite = http.SameSiteNoneMode
		if !cookie.Secure {
			return http.Cookie{}, errors.New("same-site none cookie must be secure")
		}
	default:
		return http.Cookie{}, errors.New("cookie same-site is invalid")
	}
	if err := cookie.Valid(); err != nil {
		return http.Cookie{}, err
	}
	return cookie, nil
}

func cookieToWire(cookie http.Cookie, profile v2manifest.SessionExportProfile) candidateCookie {
	domain := cookie.Domain
	if domain == "" {
		domain = profile.AllowedCookieDomains[0]
	}
	path := cookie.Path
	if path == "" {
		path = "/"
	}
	var expires *time.Time
	if !cookie.Expires.IsZero() {
		value := cookie.Expires.UTC()
		expires = &value
	}
	sameSite := ""
	switch cookie.SameSite {
	case http.SameSiteLaxMode:
		sameSite = "lax"
	case http.SameSiteStrictMode:
		sameSite = "strict"
	case http.SameSiteNoneMode:
		sameSite = "none"
	}
	return candidateCookie{Name: cookie.Name, Value: cookie.Value, Domain: domain, Path: path, Secure: cookie.Secure, HTTPOnly: cookie.HttpOnly, Expires: expires, SameSite: sameSite}
}

func cookieDomainAllowed(domain string, allowed []string) bool {
	for _, value := range allowed {
		if strings.EqualFold(domain, value) {
			return true
		}
	}
	return false
}

func candidateWireFrom(candidate v2store.SessionCandidate) candidateWire {
	state := candidate.State
	if state == v2domain.CandidateReadyLink {
		state = v2domain.CandidateProbing
	}
	return candidateWire{CandidateID: candidate.ID, State: string(state), ExpiresAt: candidate.ExpiresAt, Revision: int64(candidate.Revision), FailureCode: candidate.FailureCode}
}

func sourceAccountWirePtr(value sourceAccountWire) *sourceAccountWire {
	return &value
}

func candidateOperationKey(keys v2crypto.KeySet, operation, candidateID, original string) string {
	return operation + "-" + v2crypto.Digest(keys.CredentialHMAC, candidateID+"\x00"+original)
}

var errCandidateProbeRace = errors.New("candidate changed during probe")

func probeFailureCode(err error) string {
	var probeErr *v2scan.ProbeError
	if errors.As(err, &probeErr) && probeErr.Code != "" {
		return probeErr.Code
	}
	return "extension_failure"
}

func probeRetryable(err error) bool {
	code := probeFailureCode(err)
	return code == "transient" || code == "rate_limited"
}

func writeSwitchConfirmationRequired(router *Router, w http.ResponseWriter, request *http.Request, candidate v2store.SessionCandidate) {
	writeAPIError(router, w, request, http.StatusConflict, "switch_confirmation_required", "source account switch confirmation is required", false, map[string]any{
		"candidateId": candidate.ID, "state": string(v2domain.CandidateSwitchConfirmationRequired), "revision": int64(candidate.Revision),
	})
}

func writeCandidateError(router *Router, w http.ResponseWriter, request *http.Request, err error) {
	var probeErr *v2scan.ProbeError
	if errors.As(err, &probeErr) {
		writeAPIError(router, w, request, http.StatusUnprocessableEntity, "source_account_probe_failed", "source account probe failed", probeRetryable(err), nil)
		return
	}
	switch {
	case errors.Is(err, errSourceRuntimeBlocked):
		writeAPIError(router, w, request, http.StatusServiceUnavailable, "source_runtime_blocked", "source runtime is blocked", true, nil)
	case errors.Is(err, errSourceNotManaged):
		writeAPIError(router, w, request, http.StatusForbidden, "source_not_managed", "source is not managed", false, nil)
	case errors.Is(err, errCandidateProbeRace):
		writeAPIError(router, w, request, http.StatusConflict, "revision_conflict", "resource changed", false, nil)
	case errors.Is(err, v2store.ErrCandidateState):
		writeAPIError(router, w, request, http.StatusConflict, "candidate_state_invalid", "candidate state does not allow this operation", false, nil)
	case errors.Is(err, v2store.ErrSourceAccountNotSelected):
		writeAPIError(router, w, request, http.StatusNotFound, "resource_not_found", "resource not found", false, nil)
	case errors.Is(err, v2store.ErrArtifactNotFound), errors.Is(err, v2store.ErrPackageReleaseNotFound):
		writeAPIError(router, w, request, http.StatusNotFound, "resource_not_found", "resource not found", false, nil)
	default:
		writeStoreError(router, w, request, err)
	}
}
