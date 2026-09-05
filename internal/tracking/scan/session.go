package scan

import (
	"errors"
	"net/http"
	"regexp"
	"strings"

	"venera-server/internal/cryptoutil"
	legacyStore "venera-server/internal/store"
)

const (
	maxCookieHeaderBytes = 64 << 10
	maxSessionCookies    = 100
	maxCookieNameBytes   = 256
	maxCookieValueBytes  = 8 << 10
)

var cookieNamePattern = regexp.MustCompile(`^[!#$%&'*+\-.^_` + "`" + `|~0-9A-Za-z]+$`)

var (
	ErrSessionUnavailable = errors.New("source session is unavailable")
	ErrSessionInvalid     = errors.New("source session cookie header is invalid")
)

// LoadEncryptedSourceSessionCookies reads the existing master-server session
// row and decrypts it only at the scanner boundary. The plaintext never enters
// a JSON result, diagnostic payload, or log message.
func LoadEncryptedSourceSessionCookies(st *legacyStore.Store, key []byte, userID, source string) ([]http.Cookie, error) {
	if st == nil || len(key) == 0 || strings.TrimSpace(userID) == "" || strings.TrimSpace(source) == "" {
		return nil, ErrSessionUnavailable
	}
	session, err := st.GetSourceSession(userID, source)
	if err != nil {
		return nil, ErrSessionUnavailable
	}
	plaintext, err := cryptoutil.Decrypt(session.CookieEncrypted, key)
	if err != nil {
		return nil, ErrSessionUnavailable
	}
	return ParseCookieHeader(plaintext)
}

// ParseCookieHeader converts the legacy Cookie header format into isolated
// http.Cookie values accepted by the worker's per-run jar.
func ParseCookieHeader(header string) ([]http.Cookie, error) {
	if len([]byte(header)) > maxCookieHeaderBytes {
		return nil, ErrSessionInvalid
	}
	if strings.TrimSpace(header) == "" {
		return nil, nil
	}
	result := make([]http.Cookie, 0, 8)
	seen := make(map[string]struct{})
	for _, part := range strings.Split(header, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, value, ok := strings.Cut(part, "=")
		name = strings.TrimSpace(name)
		if !ok || !cookieNamePattern.MatchString(name) || len([]byte(name)) > maxCookieNameBytes ||
			strings.ContainsAny(name, "\x00\r\n") {
			return nil, ErrSessionInvalid
		}
		value = strings.TrimSpace(value)
		if len([]byte(value)) > maxCookieValueBytes || strings.ContainsAny(value, "\x00\r\n") {
			return nil, ErrSessionInvalid
		}
		if _, exists := seen[name]; exists {
			return nil, ErrSessionInvalid
		}
		seen[name] = struct{}{}
		result = append(result, http.Cookie{Name: name, Value: value})
		if len(result) > maxSessionCookies {
			return nil, ErrSessionInvalid
		}
	}
	return result, nil
}
