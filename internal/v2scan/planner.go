package v2scan

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"venera-server/internal/v2store"
)

type DetailCapability struct {
	ArtifactID            string
	ObservationContractID string
	Enabled               bool
	Executor              string
}

type PlannerOptions struct {
	Clock              func() time.Time
	MaxBatchWait       time.Duration
	DetailCapabilities []DetailCapability
}

type PlanResult struct {
	DemandsCreated  int
	DemandsUpdated  int
	DemandsActive   int
	DemandsInactive int
}

type Planner struct {
	repo               *v2store.Repository
	clock              func() time.Time
	maxBatchWait       time.Duration
	detailCapabilities map[string]DetailCapability
}

func NewPlanner(repo *v2store.Repository, options PlannerOptions) *Planner {
	clock := options.Clock
	if clock == nil && repo != nil && repo.DB() != nil {
		clock = repo.DB().Now
	}
	if clock == nil {
		clock = time.Now
	}
	maxBatchWait := options.MaxBatchWait
	if maxBatchWait <= 0 {
		maxBatchWait = 15 * time.Minute
	}
	capabilities := make(map[string]DetailCapability, len(options.DetailCapabilities)*2)
	for _, capability := range options.DetailCapabilities {
		if capability.ArtifactID == "" {
			continue
		}
		key := capability.ArtifactID + "\x00" + capability.ObservationContractID
		capabilities[key] = capability
		if capability.ObservationContractID == "" {
			capabilities[capability.ArtifactID] = capability
		}
	}
	return &Planner{repo: repo, clock: clock, maxBatchWait: maxBatchWait, detailCapabilities: capabilities}
}

// Plan reconciles durable demands from active cloud claims, temporary
// preparations, and active tracking interests. It never deletes a demand:
// inactive rows retain their execution generation as an audit and stale-job
// guard.
func (p *Planner) Plan(ctx context.Context) (PlanResult, error) {
	if p == nil || p.repo == nil {
		return PlanResult{}, errors.New("planner repository is required")
	}
	now := p.clock().UTC()
	demands, err := p.desiredDemands(ctx, now)
	if err != nil {
		return PlanResult{}, err
	}
	demandRepo := NewDemandRepository(p.repo)
	result := PlanResult{}
	activeByArtifactKind := make(map[string]map[string]struct{})
	for _, desired := range demands {
		key := string(desired.Kind) + "\x00" + desired.ArtifactID
		if activeByArtifactKind[key] == nil {
			activeByArtifactKind[key] = make(map[string]struct{})
		}
		activeByArtifactKind[key][desired.Key] = struct{}{}
		before, beforeErr := demandRepo.GetDemandByKey(ctx, desired.Key)
		if beforeErr != nil && !errors.Is(beforeErr, ErrDemandNotFound) {
			return result, beforeErr
		}
		if beforeErr == nil && before.State == DemandActive {
			desired.OldestAt = before.OldestAt
			if !before.OldestAt.IsZero() && !now.Before(before.OldestAt.Add(p.maxBatchWait)) {
				desired.PriorityClass = PriorityExpedited
			}
		}
		updated, err := demandRepo.UpsertDemand(ctx, desired)
		if err != nil {
			return result, err
		}
		if errors.Is(beforeErr, ErrDemandNotFound) {
			result.DemandsCreated++
		} else if before.State != updated.State || before.ExecutionGeneration != updated.ExecutionGeneration || before.Revision != updated.Revision {
			result.DemandsUpdated++
		}
		if updated.State == DemandActive {
			result.DemandsActive++
		}
	}
	all, err := demandRepo.ListDemands(ctx, "")
	if err != nil {
		return result, err
	}
	for _, existing := range all {
		key := string(existing.Kind) + "\x00" + existing.ArtifactID
		activeKeys := activeByArtifactKind[key]
		if _, keep := activeKeys[existing.Key]; keep || existing.State == DemandInactive {
			continue
		}
		if err := demandRepo.MarkDemandState(ctx, existing.ID, DemandInactive, "no_active_claim_or_preparation"); err != nil {
			return result, err
		}
		result.DemandsInactive++
	}
	return result, nil
}

func (p *Planner) desiredDemands(ctx context.Context, now time.Time) ([]Demand, error) {
	var demands []Demand
	err := p.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		rows, err := tx.QueryContext(ctx, `
			SELECT a.source_account_id, a.artifact_id, COALESCE(ss.next_evaluation_at, ''),
			       COALESCE(ss.status, 'idle')
			FROM source_accounts a
			LEFT JOIN source_scan_status ss ON ss.source_account_id = a.source_account_id
			WHERE a.state = 'active' AND (
				EXISTS (SELECT 1 FROM client_cloud_claims c
				        WHERE c.source_account_id = a.source_account_id AND c.state = 'active')
				OR EXISTS (SELECT 1 FROM cloud_mode_preparations p
				          WHERE p.source_account_id = a.source_account_id
				            AND p.state IN ('preparing','snapshotReady') AND p.expires_at > ?)
			) ORDER BY a.artifact_id, a.source_account_id`, formatPlannerTime(now))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var accountID, artifactID, nextEvaluation, status string
			if err := rows.Scan(&accountID, &artifactID, &nextEvaluation, &status); err != nil {
				return err
			}
			dueAt := now
			if nextEvaluation != "" {
				if parsed, err := time.Parse(time.RFC3339Nano, nextEvaluation); err == nil && parsed.After(now) {
					dueAt = parsed
				}
			}
			state := DemandActive
			lastErrorCode := ""
			if status == "blocked" || status == "reauthRequired" || status == "paused" {
				state = DemandBlocked
				lastErrorCode = status
			}
			demands = append(demands, Demand{
				Key: AccountSnapshotDemandKey(artifactID, accountID), ArtifactID: artifactID,
				Kind: DemandAccountSnapshot, SourceAccountID: accountID, State: state,
				DueAt: dueAt, OldestAt: now, NextEligibleAt: dueAt, PriorityClass: p.priorityFor(now, now),
				ExecutionGeneration: 1, LastErrorCode: lastErrorCode,
			})
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	interests, err := p.detailInterests(ctx, now)
	if err != nil {
		return nil, err
	}
	demands = append(demands, interests...)
	return demands, nil
}

type detailInterestRow struct {
	artifactID            string
	comicID               string
	visibilityScope       string
	variantKey            string
	observationContractID string
	packageJSON           string
}

func (p *Planner) detailInterests(ctx context.Context, now time.Time) ([]Demand, error) {
	var result []Demand
	err := p.repo.DB().ReadTx(ctx, func(tx *v2store.Tx) error {
		rows, err := tx.QueryContext(ctx, `
			SELECT i.artifact_id, i.comic_id, i.visibility_scope, i.variant_key,
			       COALESCE(r.observation_contract_id, ''), COALESCE(r.package_json, '')
			FROM tracking_interests i
			LEFT JOIN source_package_releases r ON r.artifact_id = i.artifact_id AND r.state = 'active'
			WHERE i.state = 'active'
			ORDER BY i.artifact_id, i.comic_id, i.visibility_scope, i.variant_key`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row detailInterestRow
			if err := rows.Scan(&row.artifactID, &row.comicID, &row.visibilityScope, &row.variantKey, &row.observationContractID, &row.packageJSON); err != nil {
				return err
			}
			if !p.detailEnabled(row.artifactID, row.observationContractID, row.packageJSON) {
				continue
			}
			state := DemandActive
			lastErrorCode := ""
			var executorCount int
			if err := tx.QueryRowContext(ctx, `
				SELECT COUNT(*) FROM source_accounts a
				JOIN source_account_sessions s ON s.source_account_id = a.source_account_id
				WHERE a.artifact_id = ? AND a.visibility_scope = ? AND a.state = 'active'
				  AND a.scope_fresh_until > ?
				  AND (s.expires_at IS NULL OR s.expires_at > ?)
				  AND NOT EXISTS (SELECT 1 FROM source_scan_status ss WHERE ss.source_account_id = a.source_account_id AND ss.status IN ('blocked','reauthRequired','paused'))`,
				row.artifactID, row.visibilityScope, formatPlannerTime(now), formatPlannerTime(now)).Scan(&executorCount); err != nil {
				return err
			}
			if executorCount == 0 {
				state = DemandBlocked
				lastErrorCode = "no_authenticated_executor"
			}
			dueAt := now
			var freshUntil string
			err := tx.QueryRowContext(ctx, `
				SELECT fresh_until FROM content_observations
				WHERE artifact_id = ? AND comic_id = ? AND visibility_scope = ? AND variant_key = ? AND observation_contract_id = ?`,
				row.artifactID, row.comicID, row.visibilityScope, row.variantKey, row.observationContractID).Scan(&freshUntil)
			if err == nil {
				if parsed, parseErr := time.Parse(time.RFC3339Nano, freshUntil); parseErr == nil {
					dueAt = parsed
				}
			} else if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			result = append(result, Demand{
				Key:        ComicDetailDemandKey(row.artifactID, row.comicID, row.visibilityScope, row.variantKey, row.observationContractID),
				ArtifactID: row.artifactID, Kind: DemandComicDetail, ComicID: row.comicID,
				VisibilityScope: row.visibilityScope, VariantKey: row.variantKey,
				ObservationContractID: row.observationContractID, State: state, LastErrorCode: lastErrorCode,
				DueAt: dueAt, OldestAt: now, NextEligibleAt: dueAt,
				PriorityClass: p.priorityFor(now, now), ExecutionGeneration: 1,
			})
		}
		return rows.Err()
	})
	return result, err
}

func (p *Planner) detailEnabled(artifactID, contractID, packageJSON string) bool {
	if capability, ok := p.detailCapabilities[artifactID+"\x00"+contractID]; ok {
		return capability.Enabled
	}
	if capability, ok := p.detailCapabilities[artifactID]; ok {
		return capability.Enabled
	}
	if packageDetailEnabled(packageJSON) {
		return true
	}
	// Picacg's accepted contract is explicitly authenticated-detail based. All
	// other sources remain conservative unless their manifest/package declares
	// an enabled detail capability or the caller registers one above.
	return strings.Contains(strings.ToLower(artifactID), "picacg") || strings.Contains(strings.ToLower(contractID), "picacg")
}

func (p *Planner) priorityFor(now, oldest time.Time) PriorityClass {
	if !oldest.IsZero() && !now.Before(oldest.Add(p.maxBatchWait)) {
		return PriorityExpedited
	}
	return PriorityNormal
}

func packageDetailEnabled(packageJSON string) bool {
	var value struct {
		DetailEnabled bool `json:"detailEnabled"`
		Scanning      struct {
			Operations []string `json:"operations"`
			ScanComic  bool     `json:"scanComic"`
		} `json:"scanning"`
		Capabilities map[string]any `json:"capabilities"`
	}
	if json.Unmarshal([]byte(packageJSON), &value) != nil {
		return false
	}
	if value.DetailEnabled || value.Scanning.ScanComic {
		return true
	}
	for _, operation := range value.Scanning.Operations {
		if operation == "scanComic" {
			return true
		}
	}
	if raw, ok := value.Capabilities["scanComic"]; ok {
		if enabled, ok := raw.(bool); ok && enabled {
			return true
		}
	}
	return false
}

func formatPlannerTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}
