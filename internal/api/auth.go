package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net"
	"net/http"
	"strings"

	"venera-server/internal/config"
)

type ctxKey int

const (
	ctxUserID ctxKey = iota
	ctxDeviceID
)

// This token is only used to keep the legacy registration path compatible
// with debug-open mode. It is never accepted as a real credential while
// debug-open mode is disabled.
const debugOpenAuthToken = "__venera_debug_open_auth__"

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func (s *Server) debugPrincipal() (userID, deviceID string) {
	userID = s.cfg.DebugUserID
	if userID == "" {
		userID = config.DefaultDebugUserID
	}
	deviceID = s.cfg.DebugDeviceID
	if deviceID == "" {
		deviceID = config.DefaultDebugDeviceID
	}
	return userID, deviceID
}

func (s *Server) ensureDebugPrincipal() error {
	userID, deviceID := s.debugPrincipal()
	tz := s.cfg.DebugUserTZ
	if tz == "" {
		tz = config.DefaultDebugUserTZ
	}
	locale := s.cfg.DebugLocale
	if locale == "" {
		locale = config.DefaultDebugLocale
	}
	if err := s.store.UpsertUser(userID, tz, locale); err != nil {
		return err
	}
	return s.store.UpsertDevice(deviceID, userID, hashToken(debugOpenAuthToken))
}

func (s *Server) withDebugPrincipal(ctx context.Context) context.Context {
	userID, deviceID := s.debugPrincipal()
	ctx = context.WithValue(ctx, ctxUserID, userID)
	return context.WithValue(ctx, ctxDeviceID, deviceID)
}

func (s *Server) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.DebugOpenAuth {
			if err := s.ensureDebugPrincipal(); err != nil {
				writeError(w, http.StatusInternalServerError, "internal", "initialize debug principal failed")
				return
			}
			next(w, r.WithContext(s.withDebugPrincipal(r.Context())))
			return
		}
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") {
			writeError(w, http.StatusUnauthorized, "auth_error", "missing bearer token")
			return
		}
		token := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
		if token == "" {
			writeError(w, http.StatusUnauthorized, "auth_error", "empty bearer token")
			return
		}
		dev, err := s.store.GetDeviceByTokenHash(hashToken(token))
		if err != nil {
			writeError(w, http.StatusUnauthorized, "auth_error", "invalid token")
			return
		}
		_ = s.store.UpdateDeviceLastSeen(dev.DeviceID)
		ctx := context.WithValue(r.Context(), ctxUserID, dev.UserID)
		ctx = context.WithValue(ctx, ctxDeviceID, dev.DeviceID)
		next(w, r.WithContext(ctx))
	}
}

func (s *Server) requireAdminAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.DebugOpenAuth {
			next(w, r)
			return
		}
		if s.cfg.AdminToken == "" {
			// No token configured: only allow loopback clients (local admin UI).
			host, _, err := net.SplitHostPort(r.RemoteAddr)
			if err != nil {
				host = r.RemoteAddr
			}
			if host != "127.0.0.1" && host != "::1" {
				writeError(w, http.StatusForbidden, "admin_forbidden", "admin disabled for non-loopback without token")
				return
			}
			next(w, r)
			return
		}
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") {
			writeError(w, http.StatusUnauthorized, "admin_auth_error", "missing admin bearer token")
			return
		}
		token := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
		if token == "" || subtle.ConstantTimeCompare([]byte(token), []byte(s.cfg.AdminToken)) != 1 {
			writeError(w, http.StatusUnauthorized, "admin_auth_error", "invalid admin token")
			return
		}
		next(w, r)
	}
}

func userIDFrom(ctx context.Context) string {
	v, _ := ctx.Value(ctxUserID).(string)
	return v
}

func deviceIDFrom(ctx context.Context) string {
	v, _ := ctx.Value(ctxDeviceID).(string)
	return v
}
