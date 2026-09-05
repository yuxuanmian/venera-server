package runtime

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"venera-server/internal/tracking/domain"
)

var ErrSnapshotLimit = errors.New("observation_limit_exceeded")

type Snapshot struct {
	Authority    domain.Authority
	GeneratedAt  time.Time
	Observations []domain.Observation
}

// BuildSnapshot creates a deterministic current-observation projection. It
// drops expired facts and rejects mismatched revision/artifact/generation or
// duplicate comic identities; it never compares content.
func BuildSnapshot(authority domain.Authority, observations []domain.Observation, now time.Time, limit int) (Snapshot, error) {
	if err := authority.Validate(); err != nil {
		return Snapshot{}, err
	}
	if limit <= 0 {
		return Snapshot{}, errors.New("snapshot limit must be positive")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if len(observations) > limit {
		return Snapshot{}, ErrSnapshotLimit
	}
	result := make([]domain.Observation, 0, len(observations))
	seen := make(map[string]struct{}, len(observations))
	for index, observation := range observations {
		if now.After(observation.ValidUntil) {
			continue
		}
		if observation.Revision != authority.ActiveRevision {
			return Snapshot{}, fmt.Errorf("observation %d: revision mismatch", index)
		}
		if !containsArtifact(authority.Artifacts, observation.Artifact) {
			return Snapshot{}, fmt.Errorf("observation %d: artifact mismatch", index)
		}
		if authority.Generation > 0 && observation.RuntimeGeneration != authority.Generation {
			return Snapshot{}, fmt.Errorf("observation %d: %w", index, ErrGenerationRejected)
		}
		if observation.ValidUntil.IsZero() || observation.ObservedAt.IsZero() || !observation.ValidUntil.After(observation.ObservedAt) {
			return Snapshot{}, fmt.Errorf("observation %d: freshness window is invalid", index)
		}
		if _, ok := seen[observationKey(observation)]; ok {
			return Snapshot{}, errors.New("duplicate snapshot observation")
		}
		seen[observationKey(observation)] = struct{}{}
		result = append(result, observation)
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
	if len(result) > limit {
		return Snapshot{}, ErrSnapshotLimit
	}
	return Snapshot{Authority: authority, GeneratedAt: now.UTC(), Observations: result}, nil
}

func observationKey(observation domain.Observation) string {
	return observation.Artifact.SourceKey + "\x00" + observation.Artifact.FileName + "\x00" + observation.ComicID
}

func (s Snapshot) CanonicalBytes() ([]byte, error) {
	observations := make([]snapshotObservation, len(s.Observations))
	for index, observation := range s.Observations {
		observations[index] = snapshotObservation{
			Revision:       observation.Revision,
			Artifact:       observation.Artifact,
			ComicID:        observation.ComicID,
			ObservedAt:     formatTime(observation.ObservedAt),
			ValidUntil:     formatTime(observation.ValidUntil),
			FavoriteUpdate: observation.FavoriteUpdate,
		}
	}
	return domain.CanonicalJSON(struct {
		Authority    domain.Authority      `json:"authority"`
		GeneratedAt  string                `json:"generatedAt"`
		Observations []snapshotObservation `json:"observations"`
	}{
		Authority:    s.Authority,
		GeneratedAt:  formatTime(s.GeneratedAt),
		Observations: observations,
	})
}

func (s Snapshot) ETag(userID string, stateRevision int64) (string, error) {
	observations := make([]snapshotObservation, len(s.Observations))
	for index, observation := range s.Observations {
		observations[index] = snapshotObservation{
			Revision:       observation.Revision,
			Artifact:       observation.Artifact,
			ComicID:        observation.ComicID,
			ObservedAt:     formatTime(observation.ObservedAt),
			ValidUntil:     formatTime(observation.ValidUntil),
			FavoriteUpdate: observation.FavoriteUpdate,
		}
	}
	digest, err := domain.Digest(struct {
		UserID        string                `json:"userId"`
		StateRevision int64                 `json:"stateRevision"`
		Authority     domain.Authority      `json:"authority"`
		Generation    int64                 `json:"generation"`
		Observations  []snapshotObservation `json:"observations"`
	}{
		UserID:        userID,
		StateRevision: stateRevision,
		Authority:     s.Authority,
		Generation:    s.Authority.Generation,
		Observations:  observations,
	})
	if err != nil {
		return "", err
	}
	return `"` + digest + `"`, nil
}

type snapshotObservation struct {
	Revision       string                  `json:"revision"`
	Artifact       domain.ArtifactIdentity `json:"artifact"`
	ComicID        string                  `json:"comicId"`
	ObservedAt     string                  `json:"observedAt"`
	ValidUntil     string                  `json:"validUntil"`
	FavoriteUpdate domain.FavoriteUpdate   `json:"favoriteUpdate"`
}

func formatTime(value time.Time) string {
	return value.UTC().Format("2006-01-02T15:04:05.000Z")
}
