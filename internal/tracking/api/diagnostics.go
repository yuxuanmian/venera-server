package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"venera-server/internal/tracking/domain"
	trackingstore "venera-server/internal/tracking/store"
)

type DiagnosticsDependencies struct {
	Repository           *trackingstore.Repository
	Authority            func() (domain.Authority, bool)
	GenerationRejections func() uint64
	Runtime              func() DiagnosticsRuntime
	Admin                func(context.Context) bool
}

// DiagnosticsRuntime is a read-only operational projection. It contains
// counts and safe reasons only; scanner payloads, comic IDs, and credentials
// are deliberately outside this type.
type DiagnosticsRuntime struct {
	DemandCount          int            `json:"demandCount"`
	JobCount             int            `json:"jobCount"`
	PendingJobCount      int            `json:"pendingJobCount"`
	CheckpointCount      int            `json:"checkpointCount"`
	ScannerErrorCount    int            `json:"scannerErrorCount"`
	GenerationRejections uint64         `json:"generationRejections"`
	Exclusions           map[string]int `json:"exclusions"`
}

type DiagnosticsHandler struct {
	repository           *trackingstore.Repository
	authority            func() (domain.Authority, bool)
	generationRejections func() uint64
	runtime              func() DiagnosticsRuntime
	admin                func(context.Context) bool
}

func NewDiagnosticsHandler(deps DiagnosticsDependencies) (*DiagnosticsHandler, error) {
	if deps.Repository == nil {
		return nil, errors.New("tracking diagnostics requires a repository")
	}
	return &DiagnosticsHandler{
		repository:           deps.Repository,
		authority:            deps.Authority,
		generationRejections: deps.GenerationRejections,
		runtime:              deps.Runtime,
		admin:                deps.Admin,
	}, nil
}

func (handler *DiagnosticsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if handler == nil || handler.repository == nil {
		writeError(w, r, http.StatusServiceUnavailable, "tracking_not_configured", "tracking diagnostics are unavailable")
		return
	}
	if handler.admin != nil && !handler.admin(r.Context()) {
		writeError(w, r, http.StatusForbidden, "admin_required", "administrator access required")
		return
	}
	if handler.authority == nil {
		writeError(w, r, http.StatusServiceUnavailable, "tracking_not_configured", "tracking catalog is not configured")
		return
	}
	authority, ok := handler.authority()
	if !ok {
		writeError(w, r, http.StatusServiceUnavailable, "tracking_not_configured", "tracking catalog is not configured")
		return
	}
	summary, err := handler.repository.Diagnostics(r.Context(), authority, time.Now().UTC())
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "internal", "read tracking diagnostics failed")
		return
	}
	runtime := DiagnosticsRuntime{
		Exclusions: map[string]int{
			"stale":         summary.StaleObservationCount,
			"oldRevision":   summary.OldRevisionCount,
			"oldGeneration": summary.OldGenerationCount,
		},
	}
	if handler.runtime != nil {
		runtime = handler.runtime()
		if runtime.Exclusions == nil {
			runtime.Exclusions = map[string]int{}
		}
	}
	if handler.generationRejections != nil {
		runtime.GenerationRejections = handler.generationRejections()
		runtime.Exclusions["generationRejection"] = int(runtime.GenerationRejections)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"data":    summary,
		"runtime": runtime,
	})
}
