package api

import (
	"log"
	"net/http"
	"time"

	"venera-server/internal/config"
	"venera-server/internal/cryptoutil"
	"venera-server/internal/debugrecorder"
	"venera-server/internal/store"
)

type Server struct {
	cfg       *config.Config
	store     *store.Store
	recorder  *debugrecorder.Recorder
	cookieKey []byte
	mux       *http.ServeMux
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
	s.routes()
	return s, nil
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /api/health", s.withMiddleware(healthHandler))

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

	// Admin SPA
	s.mux.HandleFunc("/admin", s.withMiddleware(s.requireAdminAuth(s.serveAdmin)))
	s.mux.HandleFunc("/admin/", s.withMiddleware(s.requireAdminAuth(s.serveAdmin)))
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
