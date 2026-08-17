package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net"
	"net/http"
	"strings"
)

type ctxKey int

const (
	ctxUserID ctxKey = iota
	ctxDeviceID
)

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func (s *Server) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
