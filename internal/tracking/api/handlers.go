package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"venera-server/internal/tracking/domain"
	trackingstore "venera-server/internal/tracking/store"
)

const (
	maxClientStateBytes = 2 << 20
	maxRequestIDLength  = 128
)

// Dependencies keeps the tracking API independent from the legacy internal
// API package. The existing auth middleware supplies Principal in production.
type Dependencies struct {
	Repository           *trackingstore.Repository
	Authority            func() (domain.Authority, bool)
	Principal            func(context.Context) (userID, deviceID string)
	OnClientStateChanged func()
}

type Handler struct {
	repository           *trackingstore.Repository
	authority            func() (domain.Authority, bool)
	principal            func(context.Context) (userID, deviceID string)
	onClientStateChanged func()
}

func NewHandler(deps Dependencies) (*Handler, error) {
	if deps.Repository == nil {
		return nil, errors.New("tracking API requires a repository")
	}
	return &Handler{
		repository:           deps.Repository,
		authority:            deps.Authority,
		principal:            deps.Principal,
		onClientStateChanged: deps.OnClientStateChanged,
	}, nil
}

func (h *Handler) authoritySnapshot(w http.ResponseWriter, r *http.Request) {
	if !h.requirePrincipal(w, r) {
		return
	}
	authority, ok := h.activeAuthority()
	if !ok {
		writeError(w, r, http.StatusServiceUnavailable, "tracking_not_configured", "tracking catalog is not configured")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": authority})
}

func (h *Handler) replaceClientState(w http.ResponseWriter, r *http.Request) {
	userID, deviceID, ok := h.principalOf(r)
	if !ok {
		writeError(w, r, http.StatusUnauthorized, "auth_error", "authentication required")
		return
	}
	authority, configured := h.activeAuthority()
	if !configured {
		writeError(w, r, http.StatusServiceUnavailable, "tracking_not_configured", "tracking catalog is not configured")
		return
	}
	var input clientStateInput
	if !decodeBody(w, r, &input, maxClientStateBytes) {
		return
	}
	if input.CloudEnabled == nil || input.Interests == nil {
		writeError(w, r, http.StatusBadRequest, "client_state_invalid", "cloudEnabled and interests are required")
		return
	}
	state := domain.ClientState{
		DeviceID:     deviceID,
		CloudEnabled: *input.CloudEnabled,
		Interests:    make([]domain.Interest, len(*input.Interests)),
	}
	for index, interest := range *input.Interests {
		state.Interests[index] = domain.Interest{
			DeviceID: interest.Artifact.SourceKey,
			Artifact: domain.ArtifactIdentity{
				SourceKey: interest.Artifact.SourceKey,
				FileName:  interest.Artifact.FileName,
			},
			ComicID: interest.ComicID,
		}
		state.Interests[index].DeviceID = deviceID
	}
	stored, err := h.repository.ReplaceClientState(r.Context(), state, authority)
	if err != nil {
		status, code := classifyClientStateError(err)
		writeError(w, r, status, code, err.Error())
		return
	}
	_ = userID // user identity is retained by auth and observation ownership.
	if h.onClientStateChanged != nil {
		h.onClientStateChanged()
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": stored})
}

func (h *Handler) observations(w http.ResponseWriter, r *http.Request) {
	userID, deviceID, ok := h.principalOf(r)
	if !ok {
		writeError(w, r, http.StatusUnauthorized, "auth_error", "authentication required")
		return
	}
	authority, configured := h.activeAuthority()
	if !configured {
		writeError(w, r, http.StatusServiceUnavailable, "tracking_not_configured", "tracking catalog is not configured")
		return
	}
	state, found, err := h.repository.GetClientState(r.Context(), deviceID)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "internal", "read tracking client state failed")
		return
	}
	var interests []domain.Interest
	if found && state.CloudEnabled {
		interests = state.Interests
	}
	observations, err := h.repository.ListCurrentObservations(r.Context(), userID, interests, authority, time.Now().UTC())
	if err != nil {
		status, code := classifyObservationError(err)
		writeError(w, r, status, code, err.Error())
		return
	}
	snapshot := observationSnapshot{
		Authority:    authority,
		GeneratedAt:  time.Now().UTC(),
		Observations: make([]observationWire, len(observations)),
	}
	for index, observation := range observations {
		snapshot.Observations[index] = toObservationWire(observation)
	}
	etag, err := snapshotETag(userID, state, snapshot)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "internal", "build tracking ETag failed")
		return
	}
	w.Header().Set("ETag", etag)
	if strings.TrimSpace(r.Header.Get("If-None-Match")) == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": snapshot})
}

func (h *Handler) activeAuthority() (domain.Authority, bool) {
	if h.authority == nil {
		return domain.Authority{}, false
	}
	return h.authority()
}

func (h *Handler) principalOf(r *http.Request) (string, string, bool) {
	if h.principal == nil {
		return "", "", false
	}
	userID, deviceID := h.principal(r.Context())
	return userID, deviceID, strings.TrimSpace(userID) != "" && strings.TrimSpace(deviceID) != ""
}

func (h *Handler) requirePrincipal(w http.ResponseWriter, r *http.Request) bool {
	_, _, ok := h.principalOf(r)
	if !ok {
		writeError(w, r, http.StatusUnauthorized, "auth_error", "authentication required")
	}
	return ok
}

type clientStateInput struct {
	CloudEnabled *bool            `json:"cloudEnabled"`
	Interests    *[]interestInput `json:"interests"`
}

type interestInput struct {
	Artifact artifactInput `json:"artifact"`
	ComicID  string        `json:"comicId"`
}

type artifactInput struct {
	SourceKey string `json:"sourceKey"`
	FileName  string `json:"fileName"`
}

type observationSnapshot struct {
	Authority    domain.Authority  `json:"authority"`
	GeneratedAt  time.Time         `json:"generatedAt"`
	Observations []observationWire `json:"observations"`
}

type observationWire struct {
	Revision       string                  `json:"revision"`
	Artifact       domain.ArtifactIdentity `json:"artifact"`
	ComicID        string                  `json:"comicId"`
	ObservedAt     string                  `json:"observedAt"`
	ValidUntil     string                  `json:"validUntil"`
	FavoriteUpdate domain.FavoriteUpdate   `json:"favoriteUpdate"`
}

func toObservationWire(observation domain.Observation) observationWire {
	return observationWire{
		Revision:       observation.Revision,
		Artifact:       observation.Artifact,
		ComicID:        observation.ComicID,
		ObservedAt:     observation.ObservedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
		ValidUntil:     observation.ValidUntil.UTC().Format("2006-01-02T15:04:05.000Z"),
		FavoriteUpdate: observation.FavoriteUpdate,
	}
}

func snapshotETag(userID string, state domain.ClientState, snapshot observationSnapshot) (string, error) {
	payload := struct {
		UserID        string            `json:"userId"`
		StateRevision int64             `json:"stateRevision"`
		Authority     domain.Authority  `json:"authority"`
		Generation    int64             `json:"generation"`
		Observations  []observationWire `json:"observations"`
	}{
		UserID:        userID,
		StateRevision: state.StateRevision,
		Authority:     snapshot.Authority,
		Generation:    snapshot.Authority.Generation,
		Observations:  snapshot.Observations,
	}
	data, err := domain.CanonicalJSON(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return `"` + hex.EncodeToString(sum[:]) + `"`, nil
}

func decodeBody(w http.ResponseWriter, r *http.Request, dst any, limit int64) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, limit))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		status := http.StatusBadRequest
		code := "invalid_json"
		if strings.Contains(err.Error(), "request body too large") {
			status = http.StatusRequestEntityTooLarge
			code = "request_too_large"
		}
		writeError(w, r, status, code, err.Error())
		return false
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeError(w, r, http.StatusBadRequest, "invalid_json", "request body must contain one JSON value")
		return false
	}
	return true
}

func classifyClientStateError(err error) (int, string) {
	message := err.Error()
	if strings.Contains(message, "artifact is not Cloud-capable") {
		return http.StatusConflict, "artifact_not_cloud_capable"
	}
	return http.StatusBadRequest, "client_state_invalid"
}

func classifyObservationError(err error) (int, string) {
	switch {
	case errors.Is(err, trackingstore.ErrObservationLimit):
		return http.StatusConflict, "observation_limit_exceeded"
	case errors.Is(err, trackingstore.ErrCatalogRevisionMismatch), errors.Is(err, trackingstore.ErrGenerationMismatch):
		return http.StatusConflict, err.Error()
	default:
		return http.StatusConflict, "tracking_observation_invalid"
	}
}

var requestSequence atomic.Uint64

func requestID(r *http.Request) string {
	value := strings.TrimSpace(r.Header.Get("X-Request-ID"))
	if len(value) > 0 && len(value) <= maxRequestIDLength {
		return value
	}
	return fmt.Sprintf("tracking-%d-%d", time.Now().UnixNano(), requestSequence.Add(1))
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{
			"code":       code,
			"message":    message,
			"request_id": requestID(r),
		},
	})
}
