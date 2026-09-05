package runtime

import (
	"errors"
	"sort"
	"strings"
	"time"

	"venera-server/internal/tracking/domain"
)

var ErrDemandNotAuthorized = errors.New("tracking demand is not authorized")

// Demand is the deterministic union of enabled exact interests for one
// active revision. It contains no source-key-only fallback and no scanner
// output or comparison decision.
type Demand struct {
	Artifact          domain.ArtifactIdentity
	ComicID           string
	Revision          string
	RuntimeGeneration int64
}

// ReconcileInterests unions enabled clients against the Server authority.
// Interests for Local-only artifacts, stale revisions, or ambiguous identities
// never become Cloud demand.
func ReconcileInterests(authority domain.Authority, clients []domain.ClientState) ([]Demand, error) {
	if err := authority.Validate(); err != nil {
		return nil, err
	}
	capable := make(map[domain.ArtifactIdentity]struct{}, len(authority.Artifacts))
	for _, artifact := range authority.Artifacts {
		capable[artifact] = struct{}{}
	}
	unique := make(map[demandKey]Demand)
	for _, client := range clients {
		if !client.CloudEnabled {
			continue
		}
		for _, interest := range client.Interests {
			if interest.DeviceID != "" && interest.DeviceID != client.DeviceID {
				continue
			}
			if err := interest.Validate(); err != nil {
				continue
			}
			if _, ok := capable[interest.Artifact]; !ok {
				continue
			}
			comicID := strings.TrimSpace(interest.ComicID)
			unique[demandKey{Artifact: interest.Artifact, ComicID: comicID}] = Demand{
				Artifact:          interest.Artifact,
				ComicID:           comicID,
				Revision:          authority.ActiveRevision,
				RuntimeGeneration: authority.Generation,
			}
		}
	}
	result := make([]Demand, 0, len(unique))
	for _, demand := range unique {
		result = append(result, demand)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Artifact.SourceKey != result[j].Artifact.SourceKey {
			return result[i].Artifact.SourceKey < result[j].Artifact.SourceKey
		}
		if result[i].Artifact.FileName != result[j].Artifact.FileName {
			return result[i].Artifact.FileName < result[j].Artifact.FileName
		}
		return result[i].ComicID < result[j].ComicID
	})
	return result, nil
}

func (r *Runtime) ReconcileInterests(clients []domain.ClientState) ([]Demand, error) {
	if r == nil {
		return nil, ErrGenerationRejected
	}
	authority, ok := r.Authority()
	if !ok {
		return nil, ErrGenerationRejected
	}
	return ReconcileInterests(authority, clients)
}

// ValidateObservation checks the generation/revision fence before a caller
// publishes an observation. Store transactions repeat this check at commit.
func (r *Runtime) ValidateObservation(token GenerationToken, observation domain.Observation, now time.Time) error {
	if err := r.RequireCommit(token); err != nil {
		return err
	}
	if observation.Artifact != token.Artifact || observation.Revision != token.Revision ||
		observation.RuntimeGeneration != token.Generation {
		return ErrGenerationRejected
	}
	return observation.Validate(now)
}

type demandKey struct {
	Artifact domain.ArtifactIdentity
	ComicID  string
}
