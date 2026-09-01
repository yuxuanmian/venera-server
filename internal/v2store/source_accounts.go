package v2store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"venera-server/internal/v2crypto"
	"venera-server/internal/v2domain"
)

var (
	ErrSourceSessionNotFound = errors.New("source account session not found")
	ErrSourceSessionExpired  = errors.New("source account session expired")
	ErrSourceSessionInvalid  = errors.New("source account session is invalid")
)

type StageSessionCandidateRequest struct {
	ClientID         string
	ArtifactID       string
	PackageReleaseID string
	ExportProfileID  string
	SessionEnvelope  []byte
	SessionDigest    string
	TTL              time.Duration
	IdempotencyKey   string
}

type SessionCandidate struct {
	ID                            string                  `json:"candidateId"`
	ClientID                      string                  `json:"clientId"`
	ArtifactID                    string                  `json:"artifactId"`
	TargetSourceAccountID         string                  `json:"targetSourceAccountId,omitempty"`
	PackageReleaseID              string                  `json:"packageReleaseId"`
	ExportProfileID               string                  `json:"exportProfileId"`
	SessionDigest                 string                  `json:"sessionDigest"`
	ProbedIdentityScheme          string                  `json:"identityScheme,omitempty"`
	ProbedIdentityDigest          string                  `json:"identityDigest,omitempty"`
	ProbedIdentityDisplay         string                  `json:"identityDisplay,omitempty"`
	ProbedAttributesJSON          string                  `json:"attributesJson,omitempty"`
	ProbedVisibilityScope         string                  `json:"visibilityScope,omitempty"`
	ExpectedSourceAccountRevision *v2domain.Revision      `json:"expectedSourceAccountRevision,omitempty"`
	State                         v2domain.CandidateState `json:"state"`
	FailureCode                   string                  `json:"failureCode,omitempty"`
	ExpiresAt                     time.Time               `json:"expiresAt"`
	Revision                      v2domain.Revision       `json:"revision"`
}

type CompleteCandidateProbeRequest struct {
	ClientID           string
	CandidateID        string
	ExpectedRevision   v2domain.Revision
	IdentityScheme     string
	IdentityCiphertext []byte
	IdentityDigest     string
	IdentityDisplay    string
	AttributesJSON     string
	VisibilityScope    string
	ScopeFreshUntil    time.Time
	IdempotencyKey     string
}

type ActivateSourceAccountRequest struct {
	ClientID         string
	CandidateID      string
	ExpectedRevision v2domain.Revision
	ConfirmSwitch    bool
	IdempotencyKey   string
}

type SourceAccountActivationResult struct {
	Candidate     SessionCandidate       `json:"candidate"`
	SourceAccount v2domain.SourceAccount `json:"sourceAccount"`
	Reused        bool                   `json:"reused"`
}

// ActiveSourceSession is the structured, host-side representation of an
// active source account session. Its Session value is decoded only after all
// account, release, epoch, expiry, and envelope checks have passed.
type ActiveSourceSession struct {
	SourceAccountID  string
	ArtifactID       string
	PackageReleaseID string
	ExportProfileID  string
	SessionDigest    string
	SessionEpoch     int64
	SessionRevision  int64
	ExpiresAt        time.Time
	Session          map[string]any
}

type candidateRow struct {
	id                      string
	clientID                string
	artifactID              string
	targetAccountID         sql.NullString
	packageReleaseID        string
	exportProfileID         string
	sessionEnvelope         []byte
	sessionDigest           string
	identityScheme          sql.NullString
	identityCiphertext      []byte
	identityDigest          sql.NullString
	identityDisplay         sql.NullString
	attributesJSON          sql.NullString
	visibilityScope         sql.NullString
	state                   string
	expectedAccountRevision sql.NullInt64
	failureCode             sql.NullString
	expiresAt               string
	revision                int64
}

func (r *Repository) StageSessionCandidate(ctx context.Context, request StageSessionCandidateRequest) (SessionCandidate, error) {
	if request.ClientID == "" || request.ArtifactID == "" || request.PackageReleaseID == "" || request.ExportProfileID == "" || len(request.SessionEnvelope) == 0 || request.SessionDigest == "" || request.TTL <= 0 || request.IdempotencyKey == "" {
		return SessionCandidate{}, ErrInvalidArgument
	}
	requestForDigest := struct {
		ClientID         string `json:"clientId"`
		ArtifactID       string `json:"artifactId"`
		PackageReleaseID string `json:"packageReleaseId"`
		ExportProfileID  string `json:"exportProfileId"`
		SessionDigest    string `json:"sessionDigest"`
		TTLSeconds       int64  `json:"ttlSeconds"`
	}{request.ClientID, request.ArtifactID, request.PackageReleaseID, request.ExportProfileID, request.SessionDigest, int64(request.TTL / time.Second)}
	var result SessionCandidate
	err := r.db.WriteTx(ctx, func(tx *Tx) error {
		replay, err := r.beginIdempotency(ctx, tx, "client", request.ClientID, "stage-session-candidate", request.IdempotencyKey, requestForDigest)
		if err != nil {
			return err
		}
		if replay != nil {
			return decodeIdempotencyReplay(replay, &result)
		}
		if err := ensureActiveClient(ctx, tx, request.ClientID); err != nil {
			return err
		}
		var releaseArtifact, releaseState string
		if err := tx.QueryRowContext(ctx, `SELECT artifact_id, state FROM source_package_releases WHERE package_release_id = ?`, request.PackageReleaseID).Scan(&releaseArtifact, &releaseState); errors.Is(err, sql.ErrNoRows) {
			return ErrPackageReleaseNotFound
		} else if err != nil {
			return err
		} else if releaseArtifact != request.ArtifactID {
			return ErrInvalidArgument
		} else if releaseState != "active" {
			return ErrPackageReleaseNotFound
		}
		var artifactExists int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM source_artifacts WHERE artifact_id = ?", request.ArtifactID).Scan(&artifactExists); err != nil {
			return err
		}
		if artifactExists != 1 {
			return ErrArtifactNotFound
		}
		now := r.db.Now()
		result = SessionCandidate{
			ID: requestID(tx, "candidate"), ClientID: request.ClientID, ArtifactID: request.ArtifactID,
			PackageReleaseID: request.PackageReleaseID, ExportProfileID: request.ExportProfileID,
			SessionDigest: request.SessionDigest, State: v2domain.CandidateProbing,
			ExpiresAt: now.Add(request.TTL), Revision: 1,
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO source_session_candidates(
				candidate_id, client_id, artifact_id, package_release_id, export_profile_id,
				session_envelope, session_digest, state, expires_at, revision, created_at, updated_at
			) VALUES(?, ?, ?, ?, ?, ?, ?, 'probing', ?, 1, ?, ?)`,
			result.ID, request.ClientID, request.ArtifactID, request.PackageReleaseID, request.ExportProfileID,
			request.SessionEnvelope, request.SessionDigest, formatTime(result.ExpiresAt), formatTime(now), formatTime(now))
		if err != nil {
			return err
		}
		return r.completeIdempotency(ctx, tx, "client", request.ClientID, "stage-session-candidate", request.IdempotencyKey, 201, result)
	})
	return result, err
}

func (r *Repository) CompleteCandidateProbe(ctx context.Context, request CompleteCandidateProbeRequest) (SessionCandidate, error) {
	if request.ClientID == "" || request.CandidateID == "" || request.ExpectedRevision < 1 || request.IdentityScheme == "" || len(request.IdentityCiphertext) == 0 || request.IdentityDigest == "" || request.VisibilityScope == "" || request.IdempotencyKey == "" {
		return SessionCandidate{}, ErrInvalidArgument
	}
	if request.AttributesJSON == "" {
		request.AttributesJSON = "{}"
	}
	if !json.Valid([]byte(request.AttributesJSON)) {
		return SessionCandidate{}, fmt.Errorf("%w: attributes JSON is invalid", ErrInvalidArgument)
	}
	if request.ScopeFreshUntil.IsZero() {
		request.ScopeFreshUntil = r.db.Now().Add(24 * time.Hour)
	}
	requestForDigest := struct {
		ClientID         string            `json:"clientId"`
		CandidateID      string            `json:"candidateId"`
		ExpectedRevision v2domain.Revision `json:"expectedRevision"`
		IdentityScheme   string            `json:"identityScheme"`
		IdentityDigest   string            `json:"identityDigest"`
		IdentityDisplay  string            `json:"identityDisplay"`
		AttributesJSON   string            `json:"attributesJson"`
		VisibilityScope  string            `json:"visibilityScope"`
		ScopeFreshUntil  string            `json:"scopeFreshUntil"`
	}{request.ClientID, request.CandidateID, request.ExpectedRevision, request.IdentityScheme, request.IdentityDigest, request.IdentityDisplay, request.AttributesJSON, request.VisibilityScope, formatTime(request.ScopeFreshUntil)}
	var result SessionCandidate
	err := r.db.WriteTx(ctx, func(tx *Tx) error {
		replay, err := r.beginIdempotency(ctx, tx, "client", request.ClientID, "complete-candidate-probe", request.IdempotencyKey, requestForDigest)
		if err != nil {
			return err
		}
		if replay != nil {
			return decodeIdempotencyReplay(replay, &result)
		}
		candidate, err := loadCandidate(ctx, tx, request.ClientID, request.CandidateID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrCandidateNotFound
		}
		if err != nil {
			return err
		}
		if candidate.revision != int64(request.ExpectedRevision) {
			return ErrRevisionConflict
		}
		if candidate.state != string(v2domain.CandidateProbing) {
			return ErrCandidateState
		}
		if expired, err := candidateExpired(candidate, r.db.Now()); err != nil {
			return err
		} else if expired {
			return ErrCandidateExpired
		}
		var target sql.NullString
		var expected sql.NullInt64
		if err := tx.QueryRowContext(ctx, `
			SELECT source_account_id, revision FROM source_accounts
			WHERE artifact_id = ? AND identity_scheme = ? AND identity_digest = ?`,
			candidate.artifactID, request.IdentityScheme, request.IdentityDigest).Scan(&target, &expected); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		newRevision := request.ExpectedRevision + 1
		if _, err := tx.ExecContext(ctx, `
			UPDATE source_session_candidates SET
				probed_identity_scheme = ?, probed_identity_ciphertext = ?, probed_identity_digest = ?,
				probed_identity_display = ?, probed_attributes_json = ?, probed_visibility_scope = ?,
				target_source_account_id = ?, expected_source_account_revision = ?,
				state = 'readyLink', revision = ?, updated_at = ?
			WHERE candidate_id = ? AND client_id = ? AND revision = ?`,
			request.IdentityScheme, request.IdentityCiphertext, request.IdentityDigest, request.IdentityDisplay,
			request.AttributesJSON, request.VisibilityScope, nullableString(target), nullableInt64(expected), newRevision, formatTime(r.db.Now()),
			request.CandidateID, request.ClientID, request.ExpectedRevision); err != nil {
			return err
		}
		loaded, err := loadCandidate(ctx, tx, request.ClientID, request.CandidateID)
		if err != nil {
			return err
		}
		result = candidateToPublic(loaded)
		return r.completeIdempotency(ctx, tx, "client", request.ClientID, "complete-candidate-probe", request.IdempotencyKey, 200, result)
	})
	return result, err
}

func (r *Repository) ActivateOrLinkSourceAccount(ctx context.Context, request ActivateSourceAccountRequest) (SourceAccountActivationResult, error) {
	return r.activateOrLinkSourceAccount(ctx, request, false, "activate-source-account")
}

func (r *Repository) ConfirmClientSourceSwitch(ctx context.Context, request ActivateSourceAccountRequest) (SourceAccountActivationResult, error) {
	request.ConfirmSwitch = true
	return r.activateOrLinkSourceAccount(ctx, request, true, "confirm-source-switch")
}

func (r *Repository) activateOrLinkSourceAccount(ctx context.Context, request ActivateSourceAccountRequest, confirmed bool, route string) (SourceAccountActivationResult, error) {
	if request.ClientID == "" || request.CandidateID == "" || request.ExpectedRevision < 1 || request.IdempotencyKey == "" {
		return SourceAccountActivationResult{}, ErrInvalidArgument
	}
	request.ConfirmSwitch = confirmed
	requestForDigest := struct {
		ClientID         string            `json:"clientId"`
		CandidateID      string            `json:"candidateId"`
		ExpectedRevision v2domain.Revision `json:"expectedRevision"`
		ConfirmSwitch    bool              `json:"confirmSwitch"`
	}{request.ClientID, request.CandidateID, request.ExpectedRevision, confirmed}
	var result SourceAccountActivationResult
	err := r.db.WriteTx(ctx, func(tx *Tx) error {
		replay, err := r.beginIdempotency(ctx, tx, "client", request.ClientID, route, request.IdempotencyKey, requestForDigest)
		if err != nil {
			return err
		}
		if replay != nil {
			return decodeIdempotencyReplay(replay, &result)
		}
		candidate, err := loadCandidate(ctx, tx, request.ClientID, request.CandidateID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrCandidateNotFound
		}
		if err != nil {
			return err
		}
		if candidate.revision != int64(request.ExpectedRevision) {
			return ErrRevisionConflict
		}
		if expired, err := candidateExpired(candidate, r.db.Now()); err != nil {
			return err
		} else if expired {
			return ErrCandidateExpired
		}
		if candidate.state == string(v2domain.CandidateSwitchConfirmationRequired) && !confirmed {
			return ErrSwitchConfirmationRequired
		}
		if candidate.state != string(v2domain.CandidateReadyLink) && candidate.state != string(v2domain.CandidateSwitchConfirmationRequired) {
			return ErrCandidateState
		}
		if !candidate.identityScheme.Valid || !candidate.identityDigest.Valid || !candidate.identityCiphertextValid() || !candidate.visibilityScope.Valid {
			return ErrCandidateState
		}

		current, hasCurrent, err := loadSelectedAccountIdentity(ctx, tx, request.ClientID, candidate.artifactID)
		if err != nil {
			return err
		}
		preferredAccountID := ""
		if hasCurrent && current.identityScheme == candidate.identityScheme.String && current.identityDigest == candidate.identityDigest.String {
			// A matching identity is a shared-session refresh, even when this
			// client currently has an active cloud claim. Keep its selected
			// account as the target instead of interpreting an empty candidate
			// target as a source switch.
			preferredAccountID = current.accountID
		} else if hasCurrent {
			var activeClaim int
			if err := tx.QueryRowContext(ctx, `
				SELECT COUNT(*) FROM client_cloud_claims
				WHERE client_id = ? AND artifact_id = ? AND state = 'active'`, request.ClientID, candidate.artifactID).Scan(&activeClaim); err != nil {
				return err
			}
			if activeClaim > 0 {
				return ErrCloudClaimMustBeRemoved
			}
			if !confirmed {
				return ErrSwitchConfirmationRequired
			}
		}

		account, reused, err := r.findOrCreateSourceAccount(ctx, tx, candidate, r.db.Now(), preferredAccountID)
		if err != nil {
			return err
		}
		if err := linkClientToAccount(ctx, tx, request.ClientID, candidate.artifactID, string(account.ID), r.db.Now()); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE source_session_candidates SET
				target_source_account_id = ?, state = 'activated', session_envelope = NULL,
				probed_identity_ciphertext = NULL, revision = revision + 1, updated_at = ?
			WHERE candidate_id = ? AND client_id = ? AND revision = ?`,
			account.ID, formatTime(r.db.Now()), request.CandidateID, request.ClientID, request.ExpectedRevision); err != nil {
			return err
		}
		loaded, err := loadCandidate(ctx, tx, request.ClientID, request.CandidateID)
		if err != nil {
			return err
		}
		result = SourceAccountActivationResult{Candidate: candidateToPublic(loaded), SourceAccount: account, Reused: reused}
		return r.completeIdempotency(ctx, tx, "client", request.ClientID, route, request.IdempotencyKey, 200, result)
	})
	return result, err
}

func (r *Repository) ListClientSourceAccounts(ctx context.Context, clientID string) ([]v2domain.SourceAccountSummary, error) {
	if clientID == "" {
		return nil, ErrClientNotFound
	}
	var result []v2domain.SourceAccountSummary
	err := r.db.ReadTx(ctx, func(tx *Tx) error {
		if err := ensureActiveClient(ctx, tx, clientID); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `
			SELECT l.source_account_id, l.artifact_id, a.identity_scheme, a.identity_display,
			       a.attributes_json, a.visibility_scope, a.state, l.state,
			       l.selected_for_artifact, a.revision, l.revision
			FROM client_source_links l
			JOIN source_accounts a ON a.source_account_id = l.source_account_id
			WHERE l.client_id = ? ORDER BY l.artifact_id, l.source_account_id`, clientID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var item v2domain.SourceAccountSummary
			var accountState, linkState string
			var selected int
			var accountRevision, linkRevision int64
			if err := rows.Scan(&item.SourceAccountID, &item.ArtifactID, &item.IdentityScheme, &item.IdentityDisplay,
				&item.AttributesJSON, &item.VisibilityScope, &accountState, &linkState, &selected, &accountRevision, &linkRevision); err != nil {
				return err
			}
			item.AccountState = v2domain.SourceAccountState(accountState)
			item.LinkState = v2domain.LinkState(linkState)
			item.Selected = selected == 1
			item.AccountRevision = v2domain.Revision(accountRevision)
			item.LinkRevision = v2domain.Revision(linkRevision)
			item.Revision = item.LinkRevision
			result = append(result, item)
		}
		return rows.Err()
	})
	return result, err
}

func (r *Repository) GetSelectedClientSourceAccount(ctx context.Context, clientID, artifactID string) (v2domain.SourceAccountSummary, error) {
	if clientID == "" || artifactID == "" {
		return v2domain.SourceAccountSummary{}, ErrInvalidArgument
	}
	var result v2domain.SourceAccountSummary
	err := r.db.ReadTx(ctx, func(tx *Tx) error {
		var accountState, linkState string
		var selected int
		var accountRevision, linkRevision int64
		err := tx.QueryRowContext(ctx, `
			SELECT l.source_account_id, l.artifact_id, a.identity_scheme, a.identity_display,
			       a.attributes_json, a.visibility_scope, a.state, l.state,
			       l.selected_for_artifact, a.revision, l.revision
			FROM client_source_links l
			JOIN source_accounts a ON a.source_account_id = l.source_account_id
			WHERE l.client_id = ? AND l.artifact_id = ? AND l.state = 'linked' AND l.selected_for_artifact = 1`,
			clientID, artifactID).Scan(&result.SourceAccountID, &result.ArtifactID, &result.IdentityScheme,
			&result.IdentityDisplay, &result.AttributesJSON, &result.VisibilityScope, &accountState,
			&linkState, &selected, &accountRevision, &linkRevision)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrSourceAccountNotSelected
		}
		if err != nil {
			return err
		}
		result.AccountState = v2domain.SourceAccountState(accountState)
		result.LinkState = v2domain.LinkState(linkState)
		result.Selected = selected == 1
		result.AccountRevision = v2domain.Revision(accountRevision)
		result.LinkRevision = v2domain.Revision(linkRevision)
		result.Revision = result.LinkRevision
		return nil
	})
	return result, err
}

func loadCandidate(ctx context.Context, tx *Tx, clientID, candidateID string) (candidateRow, error) {
	var result candidateRow
	err := tx.QueryRowContext(ctx, `
		SELECT candidate_id, client_id, artifact_id, target_source_account_id, package_release_id,
		       export_profile_id, session_envelope, session_digest, probed_identity_scheme,
		       probed_identity_ciphertext, probed_identity_digest, probed_identity_display,
		       probed_attributes_json, probed_visibility_scope, state,
		       expected_source_account_revision, failure_code, expires_at, revision
		FROM source_session_candidates WHERE candidate_id = ? AND client_id = ?`, candidateID, clientID).Scan(
		&result.id, &result.clientID, &result.artifactID, &result.targetAccountID, &result.packageReleaseID,
		&result.exportProfileID, &result.sessionEnvelope, &result.sessionDigest, &result.identityScheme,
		&result.identityCiphertext, &result.identityDigest, &result.identityDisplay, &result.attributesJSON,
		&result.visibilityScope, &result.state, &result.expectedAccountRevision, &result.failureCode,
		&result.expiresAt, &result.revision)
	return result, err
}

func candidateToPublic(candidate candidateRow) SessionCandidate {
	result := SessionCandidate{
		ID: candidate.id, ClientID: candidate.clientID, ArtifactID: candidate.artifactID,
		PackageReleaseID: candidate.packageReleaseID, ExportProfileID: candidate.exportProfileID,
		SessionDigest: candidate.sessionDigest, State: v2domain.CandidateState(candidate.state),
		ExpiresAt: mustParseTime(candidate.expiresAt), Revision: v2domain.Revision(candidate.revision),
	}
	if candidate.targetAccountID.Valid {
		result.TargetSourceAccountID = candidate.targetAccountID.String
	}
	if candidate.identityScheme.Valid {
		result.ProbedIdentityScheme = candidate.identityScheme.String
	}
	if candidate.identityDigest.Valid {
		result.ProbedIdentityDigest = candidate.identityDigest.String
	}
	if candidate.identityDisplay.Valid {
		result.ProbedIdentityDisplay = candidate.identityDisplay.String
	}
	if candidate.attributesJSON.Valid {
		result.ProbedAttributesJSON = candidate.attributesJSON.String
	}
	if candidate.visibilityScope.Valid {
		result.ProbedVisibilityScope = candidate.visibilityScope.String
	}
	if candidate.expectedAccountRevision.Valid {
		revision := v2domain.Revision(candidate.expectedAccountRevision.Int64)
		result.ExpectedSourceAccountRevision = &revision
	}
	if candidate.failureCode.Valid {
		result.FailureCode = candidate.failureCode.String
	}
	return result
}

func candidateExpired(candidate candidateRow, now time.Time) (bool, error) {
	expires, err := parseStoredTime(candidate.expiresAt)
	if err != nil {
		return false, err
	}
	return !expires.After(now), nil
}

func (candidate candidateRow) identityCiphertextValid() bool {
	return len(candidate.identityCiphertext) > 0
}

type selectedAccountRow struct {
	accountID      string
	identityScheme string
	identityDigest string
}

func loadSelectedAccountIdentity(ctx context.Context, tx *Tx, clientID, artifactID string) (selectedAccountRow, bool, error) {
	var result selectedAccountRow
	err := tx.QueryRowContext(ctx, `
		SELECT l.source_account_id, a.identity_scheme, a.identity_digest
		FROM client_source_links l
		JOIN source_accounts a ON a.source_account_id = l.source_account_id
		WHERE l.client_id = ? AND l.artifact_id = ? AND l.state = 'linked' AND l.selected_for_artifact = 1`, clientID, artifactID).Scan(
		&result.accountID, &result.identityScheme, &result.identityDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return selectedAccountRow{}, false, nil
	}
	return result, err == nil, err
}

func (r *Repository) findOrCreateSourceAccount(ctx context.Context, tx *Tx, candidate candidateRow, now time.Time, preferredAccountID string) (v2domain.SourceAccount, bool, error) {
	var account v2domain.SourceAccount
	var reused bool
	identityScheme := candidate.identityScheme.String
	identityDigest := candidate.identityDigest.String
	var accountID string
	var findErr error
	if preferredAccountID != "" {
		findErr = tx.QueryRowContext(ctx, `
			SELECT source_account_id FROM source_accounts
			WHERE source_account_id = ? AND artifact_id = ? AND identity_scheme = ? AND identity_digest = ?`,
			preferredAccountID, candidate.artifactID, identityScheme, identityDigest).Scan(&accountID)
	} else {
		findErr = tx.QueryRowContext(ctx, `
			SELECT source_account_id FROM source_accounts
			WHERE artifact_id = ? AND identity_scheme = ? AND identity_digest = ?`,
			candidate.artifactID, identityScheme, identityDigest).Scan(&accountID)
	}
	if findErr == nil {
		reused = true
	} else if !errors.Is(findErr, sql.ErrNoRows) {
		return v2domain.SourceAccount{}, false, findErr
	} else {
		if preferredAccountID != "" {
			return v2domain.SourceAccount{}, false, ErrSourceAccountNotFound
		}
		accountID = r.db.NewID("account")
	}
	stableSessionEnvelope, stableIdentityCiphertext, err := r.resealCandidate(ctx, candidate, accountID)
	if err != nil {
		return v2domain.SourceAccount{}, false, err
	}
	if !reused {
		nowText := formatTime(now)
		_, err := tx.ExecContext(ctx, `
			INSERT INTO source_accounts(
				source_account_id, artifact_id, state, identity_scheme, identity_ciphertext,
				identity_digest, identity_display, attributes_json, visibility_scope,
				session_epoch, session_revision, identity_verified_at, scope_fresh_until,
				revision, created_at, updated_at
			) VALUES(?, ?, 'active', ?, ?, ?, ?, ?, ?, 1, 1, ?, ?, 1, ?, ?)`,
			accountID, candidate.artifactID, identityScheme, stableIdentityCiphertext,
			identityDigest, candidate.identityDisplay.String, candidate.attributesJSON.String,
			candidate.visibilityScope.String, nowText, formatTime(now.Add(24*time.Hour)), nowText, nowText)
		if err != nil {
			// A separate server process may have inserted the same identity after
			// the lookup. The unique identity is authoritative; reuse it.
			if lookupErr := tx.QueryRowContext(ctx, `
				SELECT source_account_id FROM source_accounts
				WHERE artifact_id = ? AND identity_scheme = ? AND identity_digest = ?`,
				candidate.artifactID, identityScheme, identityDigest).Scan(&accountID); lookupErr != nil {
				return v2domain.SourceAccount{}, false, err
			}
			reused = true
			stableSessionEnvelope, stableIdentityCiphertext, err = r.resealCandidate(ctx, candidate, accountID)
			if err != nil {
				return v2domain.SourceAccount{}, false, err
			}
		}
	}
	if reused {
		var epoch, sessionRevision, revision int64
		if err := tx.QueryRowContext(ctx, `
			SELECT session_epoch, session_revision, revision FROM source_accounts WHERE source_account_id = ?`, accountID).Scan(&epoch, &sessionRevision, &revision); err != nil {
			return v2domain.SourceAccount{}, false, err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE source_accounts SET identity_ciphertext = ?, identity_display = ?, attributes_json = ?,
				visibility_scope = ?, session_epoch = ?, session_revision = ?, identity_verified_at = ?,
				scope_fresh_until = ?, state = 'active', revision = ?, updated_at = ?
			WHERE source_account_id = ?`,
			stableIdentityCiphertext, candidate.identityDisplay.String, candidate.attributesJSON.String,
			candidate.visibilityScope.String, epoch+1, sessionRevision+1, formatTime(now), formatTime(now.Add(24*time.Hour)), revision+1, formatTime(now), accountID); err != nil {
			return v2domain.SourceAccount{}, false, err
		}
	}
	var epoch, sessionRevision, revision int64
	if err := tx.QueryRowContext(ctx, `SELECT session_epoch, session_revision, revision FROM source_accounts WHERE source_account_id = ?`, accountID).Scan(&epoch, &sessionRevision, &revision); err != nil {
		return v2domain.SourceAccount{}, reused, err
	}
	nowText := formatTime(now)
	_, err = tx.ExecContext(ctx, `
		INSERT INTO source_account_sessions(
			source_account_id, export_profile_id, session_envelope, session_digest,
			session_epoch, session_revision, expires_at, validated_at, updated_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(source_account_id) DO UPDATE SET
			export_profile_id = excluded.export_profile_id,
			session_envelope = excluded.session_envelope,
			session_digest = excluded.session_digest,
			session_epoch = excluded.session_epoch,
			session_revision = excluded.session_revision,
			expires_at = excluded.expires_at,
			validated_at = excluded.validated_at,
			updated_at = excluded.updated_at`,
		accountID, candidate.exportProfileID, stableSessionEnvelope, candidate.sessionDigest,
		epoch, sessionRevision, candidate.expiresAt, nowText, nowText)
	if err != nil {
		return v2domain.SourceAccount{}, reused, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO source_scan_status(source_account_id, status, revision, updated_at)
		VALUES(?, 'idle', 1, ?)
		ON CONFLICT(source_account_id) DO NOTHING`, accountID, nowText); err != nil {
		return v2domain.SourceAccount{}, reused, err
	}
	var accountIDText, artifactIDText, accountState, identitySchemeText, identityDigestText string
	var identityDisplay, attributesJSON, visibilityScope string
	var identityVerifiedText, scopeFreshUntilText string
	if err := tx.QueryRowContext(ctx, `
		SELECT source_account_id, artifact_id, state, identity_scheme, identity_digest,
		       identity_display, attributes_json, visibility_scope, session_epoch,
		       session_revision, identity_verified_at, scope_fresh_until, revision
		FROM source_accounts WHERE source_account_id = ?`, accountID).Scan(
		&accountIDText, &artifactIDText, &accountState, &identitySchemeText, &identityDigestText,
		&identityDisplay, &attributesJSON, &visibilityScope, &account.SessionEpoch,
		&account.SessionRevision, &identityVerifiedText, &scopeFreshUntilText, &account.Revision); err != nil {
		return v2domain.SourceAccount{}, reused, err
	}
	account.ID = v2domain.SourceAccountID(accountIDText)
	account.ArtifactID = v2domain.ArtifactID(artifactIDText)
	account.State = v2domain.SourceAccountState(accountState)
	account.IdentityScheme = identitySchemeText
	account.IdentityDigest = identityDigestText
	account.IdentityDisplay = identityDisplay
	account.AttributesJSON = attributesJSON
	account.VisibilityScope = visibilityScope
	account.IdentityVerified, err = parseStoredTime(identityVerifiedText)
	if err != nil {
		return v2domain.SourceAccount{}, reused, err
	}
	account.ScopeFreshUntil, err = parseStoredTime(scopeFreshUntilText)
	if err != nil {
		return v2domain.SourceAccount{}, reused, err
	}
	return account, reused, nil
}

func (r *Repository) resealCandidate(ctx context.Context, candidate candidateRow, sourceAccountID string) ([]byte, []byte, error) {
	if sourceAccountID == "" || len(candidate.sessionEnvelope) == 0 || !candidate.identityCiphertextValid() {
		return nil, nil, ErrCandidateState
	}
	sessionPlaintext, err := v2crypto.OpenSession(
		r.keys.SessionAEAD,
		candidate.sessionEnvelope,
		[]byte("candidate:"+candidate.clientID+":"+candidate.artifactID),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("open candidate session: %w", err)
	}
	identityPlaintext, err := v2crypto.OpenSession(
		r.keys.SessionAEAD,
		candidate.identityCiphertext,
		[]byte("candidate-identity:"+candidate.clientID+":"+candidate.id),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("open candidate identity: %w", err)
	}
	if v2crypto.Digest(r.keys.CredentialHMAC, "session-candidate-v1\x00"+string(sessionPlaintext)) != candidate.sessionDigest {
		return nil, nil, ErrSourceSessionInvalid
	}
	if v2crypto.IdentityDigest(r.keys.IdentityHMAC, candidate.artifactID, candidate.identityScheme.String, string(identityPlaintext)) != candidate.identityDigest.String {
		return nil, nil, ErrSourceSessionInvalid
	}
	stableSession, err := v2crypto.SealSession(
		r.keys.SessionAEAD,
		sessionPlaintext,
		[]byte("source-account-session:"+sourceAccountID+":"+candidate.artifactID),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("seal source account session: %w", err)
	}
	stableIdentity, err := v2crypto.SealSession(
		r.keys.SessionAEAD,
		identityPlaintext,
		[]byte("source-account-identity:"+sourceAccountID+":"+candidate.artifactID),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("seal source account identity: %w", err)
	}
	if err := r.db.inject("source-account.reseal"); err != nil {
		return nil, nil, err
	}
	return stableSession, stableIdentity, nil
}

// ReadActiveSourceSession validates all persisted execution identity before
// decrypting the stable session envelope. The returned payload is structured
// JSON and never appears in logs or API responses.
func (r *Repository) ReadActiveSourceSession(ctx context.Context, sourceAccountID, artifactID, packageReleaseID string, expectedSessionEpoch int64) (ActiveSourceSession, error) {
	if sourceAccountID == "" || artifactID == "" || packageReleaseID == "" || expectedSessionEpoch < 1 {
		return ActiveSourceSession{}, ErrInvalidArgument
	}
	var result ActiveSourceSession
	err := r.db.ReadTx(ctx, func(tx *Tx) error {
		var accountState, accountArtifact, releaseState, releaseArtifact string
		var releaseProfile sql.NullString
		var sessionProfile, sessionDigest, expiresText string
		var sessionEnvelope []byte
		var accountEpoch, accountRevision, sessionEpoch, sessionRevision int64
		if err := tx.QueryRowContext(ctx, `
			SELECT a.state, a.artifact_id, a.session_epoch, a.session_revision,
			       s.export_profile_id, s.session_envelope, s.session_digest,
			       s.session_epoch, s.session_revision, COALESCE(s.expires_at, ''),
			       p.state, p.artifact_id, p.session_export_profile_id
			FROM source_accounts a
			JOIN source_account_sessions s ON s.source_account_id = a.source_account_id
			JOIN source_package_releases p ON p.package_release_id = ?
			WHERE a.source_account_id = ?`, packageReleaseID, sourceAccountID).Scan(
			&accountState, &accountArtifact, &accountEpoch, &accountRevision,
			&sessionProfile, &sessionEnvelope, &sessionDigest,
			&sessionEpoch, &sessionRevision, &expiresText,
			&releaseState, &releaseArtifact, &releaseProfile); errors.Is(err, sql.ErrNoRows) {
			return ErrSourceSessionNotFound
		} else if err != nil {
			return err
		}
		if accountState != string(v2domain.SourceAccountActive) || accountArtifact != artifactID ||
			releaseState != "active" || releaseArtifact != artifactID ||
			!releaseProfile.Valid || releaseProfile.String == "" || releaseProfile.String != sessionProfile ||
			accountEpoch != expectedSessionEpoch || sessionEpoch != expectedSessionEpoch ||
			accountEpoch != sessionEpoch || accountRevision < 1 || sessionRevision < 1 ||
			len(sessionEnvelope) == 0 || sessionDigest == "" || expiresText == "" {
			return ErrSourceSessionInvalid
		}
		expiresAt, err := parseStoredTime(expiresText)
		if err != nil {
			return fmt.Errorf("parse source account session expiry: %w", err)
		}
		if !expiresAt.After(r.db.Now()) {
			return ErrSourceSessionExpired
		}
		plaintext, err := v2crypto.OpenSession(
			r.keys.SessionAEAD,
			sessionEnvelope,
			[]byte("source-account-session:"+sourceAccountID+":"+artifactID),
		)
		if err != nil {
			return fmt.Errorf("open source account session: %w", err)
		}
		if v2crypto.Digest(r.keys.CredentialHMAC, "session-candidate-v1\x00"+string(plaintext)) != sessionDigest {
			return ErrSourceSessionInvalid
		}
		var session map[string]any
		if err := json.Unmarshal(plaintext, &session); err != nil || session == nil {
			return ErrSourceSessionInvalid
		}
		result = ActiveSourceSession{
			SourceAccountID: sourceAccountID, ArtifactID: artifactID, PackageReleaseID: packageReleaseID,
			ExportProfileID: sessionProfile, SessionDigest: sessionDigest,
			SessionEpoch: sessionEpoch, SessionRevision: sessionRevision, ExpiresAt: expiresAt,
			Session: session,
		}
		return nil
	})
	return result, err
}

func linkClientToAccount(ctx context.Context, tx *Tx, clientID, artifactID, accountID string, now time.Time) error {
	if _, err := tx.ExecContext(ctx, `
		UPDATE client_source_links SET selected_for_artifact = 0, updated_at = ?, revision = revision + 1
		WHERE client_id = ? AND artifact_id = ? AND state = 'linked' AND source_account_id <> ?`, formatTime(now), clientID, artifactID, accountID); err != nil {
		return err
	}
	var existingRevision int64
	err := tx.QueryRowContext(ctx, `SELECT revision FROM client_source_links WHERE client_id = ? AND source_account_id = ?`, clientID, accountID).Scan(&existingRevision)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO client_source_links(
				client_id, source_account_id, artifact_id, state, selected_for_artifact,
				revision, linked_at, updated_at
			) VALUES(?, ?, ?, 'linked', 1, 1, ?, ?)`, clientID, accountID, artifactID, formatTime(now), formatTime(now))
		return err
	}
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		UPDATE client_source_links SET state = 'linked', selected_for_artifact = 1,
			revision = ?, updated_at = ?, unlinked_at = NULL
		WHERE client_id = ? AND source_account_id = ?`, existingRevision+1, formatTime(now), clientID, accountID)
	return err
}

func ensureActiveClient(ctx context.Context, tx *Tx, clientID string) error {
	var state string
	err := tx.QueryRowContext(ctx, "SELECT state FROM client_installations WHERE client_id = ?", clientID).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrClientNotFound
	}
	if err != nil {
		return err
	}
	if state != string(v2domain.ClientActive) {
		return ErrClientRevoked
	}
	return nil
}

func requestID(tx *Tx, prefix string) string {
	return tx.NewID(prefix)
}

func nullableInt64(value sql.NullInt64) any {
	if !value.Valid {
		return nil
	}
	return value.Int64
}

func nullableString(value sql.NullString) any {
	if !value.Valid {
		return nil
	}
	return value.String
}

func mustParseTime(value string) time.Time {
	parsed, _ := parseStoredTime(value)
	return parsed
}
