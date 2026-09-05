package api

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"venera-server/internal/config"
	"venera-server/internal/cryptoutil"
	"venera-server/internal/debugrecorder"
	"venera-server/internal/store"
	trackingapi "venera-server/internal/tracking/api"
	"venera-server/internal/tracking/catalog"
	"venera-server/internal/tracking/domain"
	trackingruntime "venera-server/internal/tracking/runtime"
	trackingservice "venera-server/internal/tracking/service"
	trackingstore "venera-server/internal/tracking/store"
)

type Server struct {
	cfg                 *config.Config
	store               *store.Store
	recorder            *debugrecorder.Recorder
	cookieKey           []byte
	mux                 *http.ServeMux
	tracking            http.Handler
	trackingDiagnostics http.Handler
	trackingRuntime     *trackingruntime.Runtime
	trackingService     *trackingservice.Service
}

func NewServer(cfg *config.Config, st *store.Store, rec *debugrecorder.Recorder) (*Server, error) {
	key, err := cryptoutil.LoadOrCreateKeyWithOverride(cfg.DataDir, cfg.CookieKey)
	if err != nil {
		return nil, err
	}
	s := &Server{
		cfg:       cfg,
		store:     st,
		recorder:  rec,
		cookieKey: key,
		mux:       http.NewServeMux(),
	}
	if cfg.DebugOpenAuth {
		if err := s.ensureDebugPrincipal(); err != nil {
			return nil, fmt.Errorf("initialize debug principal: %w", err)
		}
	}
	trackingRepository, err := trackingstore.NewRepository(st.DB(), cfg.Tracking.ObservationLimit)
	if err != nil {
		return nil, err
	}
	var trackingManager *catalog.Manager
	if trackingConfigRequested(cfg) {
		trackingManager, err = catalog.NewManager(cfg.Tracking)
		if err != nil {
			return nil, fmt.Errorf("tracking catalog config: %w", err)
		}
		if err := trackingManager.Activate(context.Background(), cfg.Tracking.Revision); err != nil {
			return nil, fmt.Errorf("activate tracking catalog: %w", err)
		}
		snapshot, ok := trackingManager.Active()
		if !ok {
			return nil, fmt.Errorf("tracking catalog activation produced no active snapshot")
		}
		if err := trackingRepository.SetCatalogState(context.Background(), trackingstore.CatalogState{
			CatalogID:      snapshot.Authority.CatalogID,
			ActiveRevision: snapshot.Authority.ActiveRevision,
			Generation:     snapshot.Authority.Generation,
			ActivatedAt:    snapshot.ActivatedAt,
			Digest:         snapshot.Digest,
		}); err != nil {
			return nil, fmt.Errorf("persist tracking catalog state: %w", err)
		}
		s.trackingRuntime, err = trackingruntime.New(snapshot.Authority)
		if err != nil {
			return nil, fmt.Errorf("initialize tracking runtime: %w", err)
		}
		s.trackingService, err = trackingservice.NewService(trackingservice.ServiceOptions{
			Catalog:             trackingManager,
			Runtime:             s.trackingRuntime,
			Repository:          trackingRepository,
			LegacyStore:         st,
			CookieKey:           key,
			InitJSPath:          cfg.InitJSPath,
			Interval:            cfg.TrackingInterval,
			RequestInterval:     cfg.RequestInterval,
			MaxConcurrent:       cfg.WorkerCount,
			SnapshotMaxRequests: cfg.TrackingSnapshotMaxRequests,
			SnapshotMaxItems:    cfg.TrackingSnapshotMaxItems,
			SnapshotDeadline:    cfg.TrackingSnapshotDeadline,
			MaxAttempts:         cfg.TrackingMaxAttempts,
		})
		if err != nil {
			return nil, fmt.Errorf("initialize tracking service: %w", err)
		}
	}
	var authority func() (domain.Authority, bool)
	if trackingManager != nil {
		authority = trackingManager.Authority
	}
	s.tracking, err = trackingapi.NewRouter(trackingapi.Dependencies{
		Repository: trackingRepository,
		Authority:  authority,
		Principal: func(ctx context.Context) (string, string) {
			return userIDFrom(ctx), deviceIDFrom(ctx)
		},
		OnClientStateChanged: func() {
			if s.trackingService != nil {
				s.trackingService.Wake()
			}
		},
	})
	if err != nil {
		return nil, fmt.Errorf("new tracking API: %w", err)
	}
	diagnostics, err := trackingapi.NewDiagnosticsHandler(trackingapi.DiagnosticsDependencies{
		Repository: trackingRepository,
		Authority:  authority,
		GenerationRejections: func() uint64 {
			if s.trackingRuntime == nil {
				return 0
			}
			return s.trackingRuntime.GenerationRejections()
		},
	})
	if err != nil {
		return nil, fmt.Errorf("new tracking diagnostics API: %w", err)
	}
	s.trackingDiagnostics = diagnostics
	s.routes()
	return s, nil
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /api/health", s.withMiddleware(healthHandler))
	s.mux.Handle("/api/tracking/", s.withMiddleware(s.requireAuth(s.trackingServeHTTP)))

	s.mux.HandleFunc("POST /api/register", s.withMiddleware(s.registerHandler))
	s.mux.HandleFunc("POST /api/sync", s.withMiddleware(s.requireAuth(s.syncHandler)))
	s.mux.HandleFunc("POST /api/client-results", s.withMiddleware(s.requireAuth(s.clientResultsHandler)))
	s.mux.HandleFunc("POST /api/scripts", s.withMiddleware(s.requireAuth(s.scriptsHandler)))
	s.mux.HandleFunc("GET /api/results", s.withMiddleware(s.requireAuth(s.resultsHandler)))
	s.mux.HandleFunc("POST /api/refresh", s.withMiddleware(s.requireAuth(s.refreshHandler)))
	s.mux.HandleFunc("POST /api/pairing-codes", s.withMiddleware(s.requireAuth(s.pairingCodesHandler)))
	s.mux.HandleFunc("GET /api/devices", s.withMiddleware(s.requireAuth(s.devicesHandler)))
	s.mux.HandleFunc("DELETE /api/devices/{device_id}", s.withMiddleware(s.requireAuth(s.deleteDeviceHandler)))

	// Admin API
	s.mux.HandleFunc("GET /admin/api/stats", s.withMiddleware(s.requireAdminAuth(s.adminStatsHandler)))
	s.mux.HandleFunc("GET /admin/api/jobs", s.withMiddleware(s.requireAdminAuth(s.adminJobsHandler)))
	s.mux.HandleFunc("GET /admin/api/mirror", s.withMiddleware(s.requireAdminAuth(s.adminMirrorHandler)))
	s.mux.HandleFunc("GET /admin/api/logs", s.withMiddleware(s.requireAdminAuth(s.adminLogsHandler)))
	s.mux.HandleFunc("POST /admin/api/refresh", s.withMiddleware(s.requireAdminAuth(s.adminRefreshHandler)))
	s.mux.Handle("/admin/api/tracking/diagnostics", s.withMiddleware(s.requireAdminAuth(s.trackingDiagnosticsServeHTTP)))

	// Admin SPA
	s.mux.HandleFunc("/admin", s.withMiddleware(s.requireAdminAuth(s.serveAdmin)))
	s.mux.HandleFunc("/admin/", s.withMiddleware(s.requireAdminAuth(s.serveAdmin)))
}

func (s *Server) trackingServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.tracking.ServeHTTP(w, r)
}

func (s *Server) trackingDiagnosticsServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.trackingDiagnostics == nil {
		writeError(w, http.StatusServiceUnavailable, "tracking_not_configured", "tracking diagnostics are unavailable")
		return
	}
	s.trackingDiagnostics.ServeHTTP(w, r)
}

func trackingConfigRequested(cfg *config.Config) bool {
	return cfg.Tracking.CatalogID != "" || cfg.Tracking.Repository != "" ||
		cfg.Tracking.Revision != "" || cfg.Tracking.CacheDir != ""
}

func (s *Server) withMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rr := &responseRecorder{ResponseWriter: w}
		gzipMiddleware(next)(rr, r)
		if rr.status == 0 {
			rr.status = http.StatusOK
		}
		s.recordRequest(r, rr, start)
		log.Printf("%s %s %d %s", r.Method, r.URL.Path, rr.status, time.Since(start))
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// StartTracking starts the production Cloud scanner lifecycle. Keeping this
// explicit lets embedders start the HTTP API and scanner under the same
// process context, while tests can call RunTrackingOnce for one composition.
func (s *Server) StartTracking(ctx context.Context) error {
	if s.trackingService == nil {
		return nil
	}
	return s.trackingService.Start(ctx)
}

func (s *Server) RunTrackingOnce(ctx context.Context) error {
	if s.trackingService == nil {
		return fmt.Errorf("tracking is not configured")
	}
	return s.trackingService.RunOnce(ctx)
}

// NotifyTrackingCatalogActivated wakes the scanner after an embedder has
// published a new validated catalog/runtime generation. RunOnce will re-read
// the authority and turn every changed generation into a pending durable job.
func (s *Server) NotifyTrackingCatalogActivated() {
	if s != nil && s.trackingService != nil {
		s.trackingService.Wake()
	}
}

func (s *Server) Close() error {
	if s.trackingService == nil {
		return nil
	}
	return s.trackingService.Close()
}
