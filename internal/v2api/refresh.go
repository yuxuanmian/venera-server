package v2api

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"
	"time"

	"venera-server/internal/v2domain"
	"venera-server/internal/v2store"
)

type refreshSignalBody struct {
	SourceAccountID string `json:"sourceAccountId"`
	ArtifactID      string `json:"artifactId"`
	Reason          string `json:"reason"`
	ClientSignalID  string `json:"clientSignalId"`
}

type refreshSignalResponse struct {
	Status           string           `json:"status"`
	SourceStatus     string           `json:"sourceStatus"`
	NextEvaluationAt *string          `json:"nextEvaluationAt"`
	BlockedReason    *string          `json:"blockedReason"`
	PollHint         *refreshPollHint `json:"pollHint"`
}

type refreshPollHint struct {
	IntervalMS int    `json:"intervalMs"`
	Until      string `json:"until"`
}

type refreshTarget struct {
	AccountID, ArtifactID, AccountState, ScopeFreshUntil string
	SourceStatus, BlockedReason, NextEvaluation          sql.NullString
	ManagedState, RuntimeState                           string
}

func (router *Router) refreshSignalHandler(w http.ResponseWriter, request *http.Request) {
	client, err := router.authenticateClient(request)
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	var body refreshSignalBody
	if err := decodeJSON(w, request, &body); err != nil || strings.TrimSpace(body.SourceAccountID) == "" || strings.TrimSpace(body.ArtifactID) == "" || strings.TrimSpace(body.Reason) == "" || len(body.Reason) > 200 || len(body.ClientSignalID) > 200 {
		writeAPIError(router, w, request, http.StatusBadRequest, "invalid_request", "request is invalid", false, nil)
		return
	}
	body.SourceAccountID = strings.TrimSpace(body.SourceAccountID)
	body.ArtifactID = strings.TrimSpace(body.ArtifactID)
	target, err := router.loadRefreshTarget(request.Context(), string(client.ID), body.SourceAccountID, body.ArtifactID)
	if err != nil {
		writeCloudError(router, w, request, err)
		return
	}
	now := router.serverNow()
	status := normalizeSourceStatus(target.SourceStatus.String, target.AccountState, target.RuntimeState)
	blockedReason := target.BlockedReason.String
	if blockedReason == "" && status == "reauthRequired" {
		blockedReason = "reauthRequired"
	}
	blocked := status == "blocked" || status == "reauthRequired" || status == "paused" || target.AccountState != string(v2domain.SourceAccountActive) || target.ManagedState != "active" || target.RuntimeState == "quarantined" || target.RuntimeState == "paused"
	if blocked {
		response, err := router.recordRefreshSignal(request.Context(), target, string(client.ID), "blocked", blockedReason, now)
		if err != nil {
			writeStoreError(router, w, request, err)
			return
		}
		writeJSON(router, w, request, http.StatusOK, response)
		return
	}
	freshUntil, parseErr := time.Parse(time.RFC3339Nano, target.ScopeFreshUntil)
	if parseErr == nil && freshUntil.After(now) {
		next := target.NextEvaluation.String
		if next == "" || mustParseAPITime(next).Before(now) {
			next = freshUntil.UTC().Format(time.RFC3339Nano)
		}
		response, err := router.recordRefreshSignal(request.Context(), target, string(client.ID), "notNeeded", "", now)
		if err != nil {
			writeStoreError(router, w, request, err)
			return
		}
		response.SourceStatus = status
		if next != "" {
			response.NextEvaluationAt = stringPointer(next)
		}
		writeJSON(router, w, request, http.StatusOK, response)
		return
	}
	response, err := router.recordRefreshSignal(request.Context(), target, string(client.ID), "accepted", "", now)
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	if response.Status == "accepted" || response.Status == "coalesced" {
		router.wakeRuntime()
	}
	writeJSON(router, w, request, http.StatusOK, response)
}

func (router *Router) loadRefreshTarget(ctx context.Context, clientID, accountID, artifactID string) (refreshTarget, error) {
	var target refreshTarget
	err := router.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		return tx.QueryRowContext(ctx, `
			SELECT c.source_account_id, c.artifact_id, a.state, a.scope_fresh_until,
			       COALESCE(ss.status, ''), ss.blocked_reason, ss.next_evaluation_at,
			       sa.managed_state, COALESCE(rs.state, 'healthy')
			FROM client_cloud_claims c
			JOIN source_accounts a ON a.source_account_id = c.source_account_id AND a.artifact_id = c.artifact_id
			JOIN client_source_links l ON l.client_id = c.client_id AND l.source_account_id = c.source_account_id AND l.artifact_id = c.artifact_id
			JOIN source_artifacts sa ON sa.artifact_id = c.artifact_id
			LEFT JOIN source_scan_status ss ON ss.source_account_id = c.source_account_id
			LEFT JOIN source_runtime_state rs ON rs.artifact_id = c.artifact_id
			WHERE c.client_id = ? AND c.source_account_id = ? AND c.artifact_id = ?
			  AND c.state = 'active' AND l.state = 'linked' AND l.selected_for_artifact = 1`, clientID, accountID, artifactID).Scan(
			&target.AccountID, &target.ArtifactID, &target.AccountState, &target.ScopeFreshUntil,
			&target.SourceStatus, &target.BlockedReason, &target.NextEvaluation, &target.ManagedState, &target.RuntimeState)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return refreshTarget{}, errCloudClientNotLinked
	}
	return target, err
}

func (router *Router) recordRefreshSignal(ctx context.Context, target refreshTarget, clientID, requestedState, blockedReason string, now time.Time) (refreshSignalResponse, error) {
	windowKey := now.UTC().Truncate(30 * time.Second).Format(time.RFC3339)
	result := refreshSignalResponse{Status: requestedState, SourceStatus: normalizeSourceStatus(target.SourceStatus.String, target.AccountState, target.RuntimeState)}
	err := router.repo.DB().WriteTx(ctx, func(tx *v2store.Tx) error {
		var signalID, state, nextEvaluation string
		var requestCount int64
		err := tx.QueryRowContext(ctx, `SELECT refresh_signal_id, state, request_count, COALESCE(next_evaluation_at, '') FROM refresh_signals WHERE source_account_id = ? AND window_key = ?`, target.AccountID, windowKey).Scan(&signalID, &state, &requestCount, &nextEvaluation)
		if errors.Is(err, sql.ErrNoRows) {
			signalID = tx.NewID("refresh")
			requestCount = 1
			if requestedState == "accepted" {
				nextEvaluation = formatAPITime(now)
			} else if requestedState == "notNeeded" {
				nextEvaluation = target.NextEvaluation.String
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO refresh_signals(refresh_signal_id, client_id, source_account_id, artifact_id, window_key, state, request_count, first_requested_at, last_requested_at, next_evaluation_at, blocked_reason) VALUES(?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)`, signalID, clientID, target.AccountID, target.ArtifactID, windowKey, requestedState, formatAPITime(now), formatAPITime(now), nullableRefreshNext(nextEvaluation), nullableRefreshReason(blockedReason))
			if err != nil {
				return err
			}
		} else if err != nil {
			return err
		} else {
			requestCount++
			state = "coalesced"
			if requestedState == "blocked" {
				state = "blocked"
			} else if requestedState == "notNeeded" {
				state = "notNeeded"
			}
			if requestedState == "accepted" && nextEvaluation == "" {
				nextEvaluation = formatAPITime(now)
			}
			_, err = tx.ExecContext(ctx, `UPDATE refresh_signals SET state = ?, request_count = ?, last_requested_at = ?, next_evaluation_at = ?, blocked_reason = ? WHERE refresh_signal_id = ?`, state, requestCount, formatAPITime(now), nullableRefreshNext(nextEvaluation), nullableRefreshReason(blockedReason), signalID)
			if err != nil {
				return err
			}
		}
		result.Status = state
		if result.Status == "" {
			result.Status = requestedState
		}
		if result.Status == "accepted" || result.Status == "coalesced" {
			result.SourceStatus, err = updateRefreshScanStatus(ctx, tx, target.AccountID, now)
			if err != nil {
				return err
			}
			until := now.Add(20 * time.Second).UTC().Format(time.RFC3339Nano)
			result.PollHint = &refreshPollHint{IntervalMS: 2500, Until: until}
		} else {
			result.SourceStatus = normalizeSourceStatus(target.SourceStatus.String, target.AccountState, target.RuntimeState)
		}
		if nextEvaluation != "" {
			result.NextEvaluationAt = stringPointer(nextEvaluation)
		}
		if blockedReason != "" {
			result.BlockedReason = stringPointer(blockedReason)
		}
		return nil
	})
	return result, err
}

func updateRefreshScanStatus(ctx context.Context, tx *v2store.Tx, accountID string, now time.Time) (string, error) {
	var status string
	var revision int64
	var nextEvaluation sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT status, revision, next_evaluation_at FROM source_scan_status WHERE source_account_id = ?`, accountID).Scan(&status, &revision, &nextEvaluation)
	if errors.Is(err, sql.ErrNoRows) {
		_, insertErr := tx.ExecContext(ctx, `INSERT INTO source_scan_status(source_account_id, status, next_evaluation_at, revision, updated_at) VALUES(?, 'scheduled', ?, 1, ?)`, accountID, formatAPITime(now), formatAPITime(now))
		return "scheduled", insertErr
	}
	if err != nil {
		return "", err
	}
	if status == "blocked" || status == "reauthRequired" || status == "paused" || status == "scanning" {
		return status, nil
	}
	_, err = tx.ExecContext(ctx, `UPDATE source_scan_status SET status = 'scheduled', next_evaluation_at = ?, revision = ?, updated_at = ? WHERE source_account_id = ? AND revision = ?`, formatAPITime(now), revision+1, formatAPITime(now), accountID, revision)
	return "scheduled", err
}

func nullableRefreshNext(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func nullableRefreshReason(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func stringPointer(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func mustParseAPITime(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return parsed
}
