package v2api

import (
	"net/http"
	"sync/atomic"

	"venera-server/internal/v2config"
	"venera-server/internal/v2scan"
	"venera-server/internal/v2store"
)

type Router struct {
	cfg         v2config.Config
	repo        *v2store.Repository
	mux         *http.ServeMux
	requestID   atomic.Uint64
	probe       v2scan.AccountProbe
	runtimeWake func()
}

func NewRouter(cfg v2config.Config, repo *v2store.Repository) *Router {
	router := &Router{cfg: cfg, repo: repo, mux: http.NewServeMux()}
	router.registerRoutes()
	return router
}

func NewServer(cfg v2config.Config, repo *v2store.Repository) *Router {
	return NewRouter(cfg, repo)
}

func (router *Router) SetAccountProbeRunner(probe v2scan.AccountProbe) {
	router.probe = probe
}

// SetRuntimeWake connects durable API mutations to the resident runtime. The
// callback is intentionally a non-blocking signal; planning and execution
// remain owned by v2runtime.
func (router *Router) SetRuntimeWake(wake func()) {
	router.runtimeWake = wake
}

func (router *Router) wakeRuntime() {
	if router != nil && router.runtimeWake != nil {
		router.runtimeWake()
	}
}

func (router *Router) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	router.commonMiddleware(router.mux).ServeHTTP(w, request)
}

func (router *Router) registerRoutes() {
	router.mux.HandleFunc("GET /v2/health", router.healthHandler)
	router.mux.HandleFunc("POST /v2/onboarding/claim-enrollment", router.claimEnrollmentHandler)
	router.mux.HandleFunc("GET /v2/client/self", router.clientSelfHandler)
	router.mux.HandleFunc("DELETE /v2/client/self", router.revokeClientHandler)
	router.mux.HandleFunc("GET /v2/bootstrap", router.bootstrapHandler)
	router.mux.HandleFunc("PUT /v2/client/source-inventory", router.replaceInventoryHandler)
	router.mux.HandleFunc("GET /v2/client/source-states", router.sourceStatesV2Handler)
	router.mux.HandleFunc("GET /v2/source-accounts", router.sourceAccountsHandler)
	router.mux.HandleFunc("POST /v2/source-accounts/session-candidates", router.createSessionCandidateHandler)
	router.mux.HandleFunc("GET /v2/source-accounts/session-candidates/{candidateId}", router.getSessionCandidateHandler)
	router.mux.HandleFunc("POST /v2/source-accounts/session-candidates/{candidateId}/confirm-link", router.confirmSessionCandidateHandler)
	router.mux.HandleFunc("DELETE /v2/source-accounts/session-candidates/{candidateId}", router.cancelSessionCandidateHandler)
	router.mux.HandleFunc("POST /v2/client/source-accounts/{sourceAccountId}/cloud-preparations", router.createCloudPreparationHandler)
	router.mux.HandleFunc("GET /v2/client/cloud-preparations/{preparationId}", router.getCloudPreparationHandler)
	router.mux.HandleFunc("GET /v2/client/cloud-preparations/{preparationId}/snapshot", router.getCloudPreparationSnapshotHandler)
	router.mux.HandleFunc("POST /v2/client/cloud-preparations/{preparationId}/commit", router.commitCloudPreparationHandler)
	router.mux.HandleFunc("DELETE /v2/client/cloud-preparations/{preparationId}", router.cancelCloudPreparationHandler)
	router.mux.HandleFunc("DELETE /v2/client/source-accounts/{sourceAccountId}/cloud-claim", router.deleteCloudClaimHandler)
	router.mux.HandleFunc("POST /v2/sync/pull", router.pullChangesHandler)
	router.mux.HandleFunc("POST /v2/sync/snapshots", router.syncSnapshotHandler)
	router.mux.HandleFunc("POST /v2/refresh-signals", router.refreshSignalHandler)
	router.registerAdminRoutes()
}
