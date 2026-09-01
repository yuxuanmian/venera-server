package v2api

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	"venera-server/internal/v2domain"
	"venera-server/internal/v2store"
)

// sourceStatesV2Handler deliberately reads the current claim (including a
// suspended row) instead of using the older summary helper.  A suspended
// claim is a useful fact to the client, while local/off remain App-owned
// choices and must never be reconstructed by the server.
func (router *Router) sourceStatesV2Handler(w http.ResponseWriter, request *http.Request) {
	client, err := router.authenticateClient(request)
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	items, err := router.sourceStateResponses(request.Context(), string(client.ID))
	if err != nil {
		writeStoreError(router, w, request, err)
		return
	}
	writeJSON(router, w, request, http.StatusOK, map[string]any{"items": items})
}

// sourceStateRows is kept separate from the wire type so SQL NULLs and
// invalid/legacy enum values can be normalized at the boundary.
type sourceStateRow struct {
	artifactID, selectedAccountID, linkState, claimState string
	accountState, compatibility, sourceStatus            string
	blockedReason, freshUntil, nextEvaluation            sql.NullString
	runtimeState, runtimeError                           string
	revision                                             int64
}

func (router *Router) querySourceStateRows(ctx context.Context, clientID string) ([]sourceStateRow, error) {
	var rowsResult []sourceStateRow
	err := router.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		rows, err := tx.QueryContext(ctx, `
			SELECT a.artifact_id,
			       COALESCE(l.source_account_id, ''),
			       COALESCE(l.state, 'unlinked'),
			       COALESCE(c.state, 'absent'),
			       COALESCE(sa.state, ''),
			       COALESCE(i.compatibility_state, 'unknown'),
			       COALESCE(ss.status, ''),
			       ss.blocked_reason, sa.scope_fresh_until, ss.next_evaluation_at,
			       COALESCE(rs.state, ''), COALESCE(rs.last_error_code, ''),
			       COALESCE(c.revision, l.revision, a.revision, 1)
			FROM source_artifacts a
			LEFT JOIN client_source_inventory i
			  ON i.client_id = ? AND i.artifact_id = a.artifact_id
			LEFT JOIN client_source_links l
			  ON l.client_id = ? AND l.artifact_id = a.artifact_id
			 AND l.state = 'linked' AND l.selected_for_artifact = 1
			LEFT JOIN client_cloud_claims c
			  ON c.client_id = ? AND c.artifact_id = a.artifact_id
			 AND c.source_account_id = l.source_account_id
			LEFT JOIN source_accounts sa
			  ON sa.source_account_id = l.source_account_id
			LEFT JOIN source_scan_status ss
			  ON ss.source_account_id = l.source_account_id
			LEFT JOIN source_runtime_state rs
			  ON rs.artifact_id = a.artifact_id
			ORDER BY a.artifact_id`, clientID, clientID, clientID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row sourceStateRow
			if err := rows.Scan(&row.artifactID, &row.selectedAccountID, &row.linkState, &row.claimState,
				&row.accountState, &row.compatibility, &row.sourceStatus, &row.blockedReason,
				&row.freshUntil, &row.nextEvaluation, &row.runtimeState, &row.runtimeError, &row.revision); err != nil {
				return err
			}
			rowsResult = append(rowsResult, row)
		}
		return rows.Err()
	})
	return rowsResult, err
}

func (router *Router) sourceStateResponses(ctx context.Context, clientID string) ([]sourceStateResponse, error) {
	rows, err := router.querySourceStateRows(ctx, clientID)
	if err != nil {
		return nil, err
	}
	result := make([]sourceStateResponse, 0, len(rows))
	for _, row := range rows {
		claimState := row.claimState
		if claimState != string(v2domain.ClaimActive) && claimState != string(v2domain.ClaimSuspended) {
			claimState = "absent"
		}
		linkState := row.linkState
		if linkState != string(v2domain.LinkLinked) {
			linkState = string(v2domain.LinkUnlinked)
		}
		compatibility := row.compatibility
		if compatibility != string(v2domain.CompatibilityCompatible) && compatibility != string(v2domain.CompatibilityIncompatible) && compatibility != string(v2domain.CompatibilityUnknown) {
			compatibility = string(v2domain.CompatibilityUnknown)
		}
		status := normalizeSourceStatus(row.sourceStatus, row.accountState, row.runtimeState)
		var blockedReason *string
		reason := row.blockedReason.String
		if reason == "" {
			reason = row.runtimeError
		}
		if reason == "" && status == "reauthRequired" {
			reason = "reauthRequired"
		}
		if reason != "" {
			blockedReason = &reason
		}
		var freshUntil *string
		if row.freshUntil.Valid && row.freshUntil.String != "" {
			value := row.freshUntil.String
			if parsed, parseErr := time.Parse(time.RFC3339Nano, value); parseErr == nil {
				value = parsed.UTC().Format(time.RFC3339Nano)
			}
			freshUntil = &value
		}
		var nextEvaluation *string
		if row.nextEvaluation.Valid && row.nextEvaluation.String != "" {
			value := row.nextEvaluation.String
			if parsed, parseErr := time.Parse(time.RFC3339Nano, value); parseErr == nil {
				value = parsed.UTC().Format(time.RFC3339Nano)
			}
			nextEvaluation = &value
		}
		effective := "inactive"
		if claimState == string(v2domain.ClaimActive) {
			effective = "active"
			if status == "blocked" || status == "reauthRequired" || status == "paused" || row.accountState != string(v2domain.SourceAccountActive) || compatibility != string(v2domain.CompatibilityCompatible) {
				effective = "blocked"
			}
		}
		result = append(result, sourceStateResponse{
			ArtifactID: row.artifactID, SelectedSourceAccountID: row.selectedAccountID,
			LinkState: linkState, ClaimState: claimState, EffectiveState: effective,
			CompatibilityState: compatibility, SourceStatus: status, FreshUntil: freshUntil,
			NextEvaluationAt: nextEvaluation, BlockedReason: blockedReason, Revision: row.revision,
		})
	}
	return result, nil
}

func normalizeSourceStatus(sourceStatus, accountState, runtimeState string) string {
	switch sourceStatus {
	case "idle", "scheduled", "scanning", "blocked", "reauthRequired", "paused":
		return sourceStatus
	}
	if accountState == string(v2domain.SourceAccountReauthRequired) {
		return "reauthRequired"
	}
	if accountState == string(v2domain.SourceAccountDisabled) || runtimeState == "paused" {
		return "paused"
	}
	if runtimeState == "quarantined" {
		return "blocked"
	}
	return "idle"
}
