package v2api

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"

	"venera-server/internal/v2crypto"
	"venera-server/internal/v2domain"
	"venera-server/internal/v2store"
)

func bearerToken(request *http.Request) (string, error) {
	value := strings.TrimSpace(request.Header.Get("Authorization"))
	parts := strings.SplitN(value, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
		return "", v2store.ErrAuthenticationFailed
	}
	if err := v2crypto.ValidateBearerTokenShape(parts[1]); err != nil {
		return "", v2store.ErrAuthenticationFailed
	}
	return parts[1], nil
}

func (router *Router) requireClient(request *http.Request) (v2domain.Client, error) {
	if value, ok := request.Context().Value(clientContextKey).(v2domain.Client); ok {
		return value, nil
	}
	token, err := bearerToken(request)
	if err != nil {
		return v2domain.Client{}, err
	}
	digest := v2crypto.CredentialDigest(router.repo.Keys().CredentialHMAC, token)
	client, err := router.repo.GetClientByTokenDigest(request.Context(), digest)
	if err != nil {
		return v2domain.Client{}, v2store.ErrAuthenticationFailed
	}
	return client, nil
}

func (router *Router) requirePendingToken(request *http.Request) (string, error) {
	if value, ok := request.Context().Value(pendingTokenContextKey).(string); ok && value != "" {
		return value, nil
	}
	token, err := bearerToken(request)
	if err != nil {
		return "", err
	}
	digest := v2crypto.CredentialDigest(router.repo.Keys().CredentialHMAC, token)
	return digest, nil
}

func requireIdempotencyKey(request *http.Request) (string, error) {
	key := strings.TrimSpace(request.Header.Get("Idempotency-Key"))
	if key == "" || len(key) > 200 || strings.ContainsAny(key, "\r\n") {
		return "", v2store.ErrIdempotencyKeyRequired
	}
	return key, nil
}

func withClientContext(request *http.Request, client v2domain.Client) *http.Request {
	return request.WithContext(context.WithValue(request.Context(), clientContextKey, client))
}

func (router *Router) authenticateClient(request *http.Request) (v2domain.Client, error) {
	return router.requireClient(request)
}

func isNoRows(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}
