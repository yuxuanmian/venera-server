package runtime

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"venera-server/internal/tracking/domain"
)

var ErrGenerationRejected = errors.New("tracking_generation_rejected")

// GenerationToken is captured by a scanner at start and must match the
// active artifact, revision, and generation at its commit boundary.
type GenerationToken struct {
	Generation int64
	Artifact   domain.ArtifactIdentity
	Revision   string
}

type Runtime struct {
	mu         sync.RWMutex
	authority  domain.Authority
	configured bool
	next       int64
	rejections atomic.Uint64
}

func New(authority domain.Authority) (*Runtime, error) {
	if err := authority.Validate(); err != nil {
		return nil, err
	}
	if authority.Generation < 1 {
		authority.Generation = 1
	}
	return &Runtime{authority: authority, configured: true, next: authority.Generation}, nil
}

// Activate replaces the complete authority and advances generation even when
// the Git revision is a rollback to a previously seen value.
func (r *Runtime) Activate(authority domain.Authority) (domain.Authority, error) {
	if err := authority.Validate(); err != nil {
		return domain.Authority{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.next == 0 {
		r.next = 1
	}
	r.next++
	authority.Generation = r.next
	r.authority = authority
	r.configured = true
	return authority, nil
}

func (r *Runtime) Authority() (domain.Authority, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.configured {
		return domain.Authority{}, false
	}
	return r.authority, true
}

func (r *Runtime) Capture(artifact domain.ArtifactIdentity) (GenerationToken, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.configured {
		return GenerationToken{}, ErrGenerationRejected
	}
	if !containsArtifact(r.authority.Artifacts, artifact) {
		return GenerationToken{}, fmt.Errorf("artifact is not active: %w", ErrGenerationRejected)
	}
	return GenerationToken{
		Generation: r.authority.Generation,
		Artifact:   artifact,
		Revision:   r.authority.ActiveRevision,
	}, nil
}

func (r *Runtime) CanCommit(token GenerationToken) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.configured && token.Generation == r.authority.Generation &&
		token.Revision == r.authority.ActiveRevision && containsArtifact(r.authority.Artifacts, token.Artifact)
}

func (r *Runtime) RequireCommit(token GenerationToken) error {
	if !r.CanCommit(token) {
		r.rejections.Add(1)
		return ErrGenerationRejected
	}
	return nil
}

// GenerationRejections reports rejected late commits for the active runtime.
// It is intentionally a monotonic in-memory counter for diagnostics only.
func (r *Runtime) GenerationRejections() uint64 {
	if r == nil {
		return 0
	}
	return r.rejections.Load()
}

func containsArtifact(artifacts []domain.ArtifactIdentity, target domain.ArtifactIdentity) bool {
	for _, artifact := range artifacts {
		if artifact == target {
			return true
		}
	}
	return false
}
