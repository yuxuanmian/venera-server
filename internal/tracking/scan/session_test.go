package scan

import (
	"errors"
	"testing"

	"venera-server/internal/cryptoutil"
	legacyStore "venera-server/internal/store"
)

func TestParseCookieHeaderBoundsAndDuplicateNames(t *testing.T) {
	cookies, err := ParseCookieHeader("sid=abc; theme=dark")
	if err != nil || len(cookies) != 2 || cookies[0].Name != "sid" || cookies[1].Value != "dark" {
		t.Fatalf("cookies = %#v, err = %v", cookies, err)
	}
	for _, header := range []string{"sid=abc; sid=def", "bad name=value", "sid=a\nsecret", "sid=" + string(make([]byte, maxCookieValueBytes+1))} {
		if _, err := ParseCookieHeader(header); !errors.Is(err, ErrSessionInvalid) {
			t.Fatalf("header %q error = %v", header, err)
		}
	}
}

func TestLoadEncryptedSourceSessionCookiesUsesExistingStoreBoundary(t *testing.T) {
	st, err := legacyStore.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	key := []byte("01234567890123456789012345678901")
	encrypted, err := cryptoutil.Encrypt("sid=secret; theme=dark", key)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertSourceSession(legacyStore.SourceSession{
		UserID:          "user-1",
		Source:          "manwa",
		CookieEncrypted: encrypted,
		CookieHash:      "hash",
		UpdatedAt:       "2026-09-03T00:00:00Z",
	}); err != nil {
		t.Fatal(err)
	}
	cookies, err := LoadEncryptedSourceSessionCookies(st, key, "user-1", "manwa")
	if err != nil || len(cookies) != 2 || cookies[0].Value != "secret" {
		t.Fatalf("cookies = %#v, err = %v", cookies, err)
	}
	if _, err := LoadEncryptedSourceSessionCookies(st, []byte("wrong"), "user-1", "manwa"); !errors.Is(err, ErrSessionUnavailable) {
		t.Fatalf("wrong-key error = %v", err)
	}
}
