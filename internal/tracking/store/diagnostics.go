package store

import (
	"context"
	"fmt"
	"time"

	"venera-server/internal/tracking/domain"
)

// DiagnosticArtifactSummary is deliberately comic-ID-free. Admins can see
// health and exclusion counts without receiving private titles or interest
// payloads.
type DiagnosticArtifactSummary struct {
	Artifact              domain.ArtifactIdentity `json:"artifact"`
	ObservationCount      int                     `json:"observationCount"`
	FreshObservationCount int                     `json:"freshObservationCount"`
	StaleObservationCount int                     `json:"staleObservationCount"`
	OldRevisionCount      int                     `json:"oldRevisionCount"`
	OldGenerationCount    int                     `json:"oldGenerationCount"`
}

type DiagnosticsSummary struct {
	CatalogID              string                      `json:"catalogId"`
	ActiveRevision         string                      `json:"activeRevision"`
	Generation             int64                       `json:"generation"`
	ClientCount            int                         `json:"clientCount"`
	EnabledClientCount     int                         `json:"enabledClientCount"`
	InterestCount          int                         `json:"interestCount"`
	EffectiveInterestCount int                         `json:"effectiveInterestCount"`
	ObservationCount       int                         `json:"observationCount"`
	FreshObservationCount  int                         `json:"freshObservationCount"`
	StaleObservationCount  int                         `json:"staleObservationCount"`
	OldRevisionCount       int                         `json:"oldRevisionCount"`
	OldGenerationCount     int                         `json:"oldGenerationCount"`
	Artifacts              []DiagnosticArtifactSummary `json:"artifacts"`
}

// Diagnostics returns bounded operational counts. It intentionally does not
// select comic_id, metadata, markers, cookies, or scanner output.
func (r *Repository) Diagnostics(ctx context.Context, authority domain.Authority, now time.Time) (DiagnosticsSummary, error) {
	if err := authority.Validate(); err != nil {
		return DiagnosticsSummary{}, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	result := DiagnosticsSummary{
		CatalogID:      authority.CatalogID,
		ActiveRevision: authority.ActiveRevision,
		Generation:     authority.Generation,
		Artifacts:      make([]DiagnosticArtifactSummary, 0, len(authority.Artifacts)),
	}
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tracking_client_state`).Scan(&result.ClientCount); err != nil {
		return DiagnosticsSummary{}, fmt.Errorf("count tracking clients: %w", err)
	}
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tracking_client_state WHERE cloud_enabled = 1`).Scan(&result.EnabledClientCount); err != nil {
		return DiagnosticsSummary{}, fmt.Errorf("count enabled tracking clients: %w", err)
	}
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tracking_interests`).Scan(&result.InterestCount); err != nil {
		return DiagnosticsSummary{}, fmt.Errorf("count tracking interests: %w", err)
	}
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tracking_observations`).Scan(&result.ObservationCount); err != nil {
		return DiagnosticsSummary{}, fmt.Errorf("count tracking observations: %w", err)
	}
	nowWire := formatTime(now)
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tracking_observations WHERE valid_until > ? AND catalog_revision = ? AND runtime_generation = ?`, nowWire, authority.ActiveRevision, authority.Generation).Scan(&result.FreshObservationCount); err != nil {
		return DiagnosticsSummary{}, fmt.Errorf("count fresh tracking observations: %w", err)
	}
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tracking_observations WHERE valid_until <= ?`, nowWire).Scan(&result.StaleObservationCount); err != nil {
		return DiagnosticsSummary{}, fmt.Errorf("count stale tracking observations: %w", err)
	}
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tracking_observations WHERE catalog_revision != ?`, authority.ActiveRevision).Scan(&result.OldRevisionCount); err != nil {
		return DiagnosticsSummary{}, fmt.Errorf("count old tracking revisions: %w", err)
	}
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tracking_observations WHERE runtime_generation != ?`, authority.Generation).Scan(&result.OldGenerationCount); err != nil {
		return DiagnosticsSummary{}, fmt.Errorf("count old tracking generations: %w", err)
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT i.source_key, i.file_name, COUNT(*)
		FROM tracking_interests i
		JOIN tracking_client_state c ON c.device_id = i.device_id
		WHERE c.cloud_enabled = 1
		GROUP BY i.source_key, i.file_name`)
	if err != nil {
		return DiagnosticsSummary{}, fmt.Errorf("query effective tracking interests: %w", err)
	}
	defer rows.Close()
	capable := make(map[domain.ArtifactIdentity]struct{}, len(authority.Artifacts))
	for _, artifact := range authority.Artifacts {
		capable[artifact] = struct{}{}
	}
	for rows.Next() {
		var sourceKey, fileName string
		var count int
		if err := rows.Scan(&sourceKey, &fileName, &count); err != nil {
			return DiagnosticsSummary{}, fmt.Errorf("scan effective tracking interests: %w", err)
		}
		if _, ok := capable[domain.ArtifactIdentity{SourceKey: sourceKey, FileName: fileName}]; ok {
			result.EffectiveInterestCount += count
		}
	}
	if err := rows.Err(); err != nil {
		return DiagnosticsSummary{}, fmt.Errorf("read effective tracking interests: %w", err)
	}
	for _, artifact := range authority.Artifacts {
		var summary DiagnosticArtifactSummary
		summary.Artifact = artifact
		if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tracking_observations WHERE source_key = ? AND file_name = ?`, artifact.SourceKey, artifact.FileName).Scan(&summary.ObservationCount); err != nil {
			return DiagnosticsSummary{}, fmt.Errorf("count artifact observations: %w", err)
		}
		if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tracking_observations WHERE source_key = ? AND file_name = ? AND valid_until > ? AND catalog_revision = ? AND runtime_generation = ?`, artifact.SourceKey, artifact.FileName, nowWire, authority.ActiveRevision, authority.Generation).Scan(&summary.FreshObservationCount); err != nil {
			return DiagnosticsSummary{}, fmt.Errorf("count artifact fresh observations: %w", err)
		}
		if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tracking_observations WHERE source_key = ? AND file_name = ? AND valid_until <= ?`, artifact.SourceKey, artifact.FileName, nowWire).Scan(&summary.StaleObservationCount); err != nil {
			return DiagnosticsSummary{}, fmt.Errorf("count artifact stale observations: %w", err)
		}
		if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tracking_observations WHERE source_key = ? AND file_name = ? AND catalog_revision != ?`, artifact.SourceKey, artifact.FileName, authority.ActiveRevision).Scan(&summary.OldRevisionCount); err != nil {
			return DiagnosticsSummary{}, fmt.Errorf("count artifact old revisions: %w", err)
		}
		if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tracking_observations WHERE source_key = ? AND file_name = ? AND runtime_generation != ?`, artifact.SourceKey, artifact.FileName, authority.Generation).Scan(&summary.OldGenerationCount); err != nil {
			return DiagnosticsSummary{}, fmt.Errorf("count artifact old generations: %w", err)
		}
		result.Artifacts = append(result.Artifacts, summary)
	}
	return result, nil
}
