package v2crypto

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func testKeySet(t *testing.T) KeySet {
	t.Helper()
	keys, err := DeriveKeys(bytes.Repeat([]byte{0x19}, RootSecretSize))
	if err != nil {
		t.Fatalf("derive keys: %v", err)
	}
	return keys
}

func TestSessionEnvelopeRoundTripWrongKeyAndTamper(t *testing.T) {
	keys := testKeySet(t)
	envelope, err := SealSession(keys.SessionAEAD, []byte("session-cookie"), []byte("artifact:manwa"))
	if err != nil {
		t.Fatalf("seal session: %v", err)
	}
	if !strings.HasPrefix(string(envelope), "v1.") {
		t.Fatalf("envelope version = %q", envelope)
	}
	plain, err := OpenSession(keys.SessionAEAD, envelope, []byte("artifact:manwa"))
	if err != nil || string(plain) != "session-cookie" {
		t.Fatalf("open session = %q, %v", plain, err)
	}
	wrongKeys := testKeySet(t)
	wrongKeys.SessionAEAD[0] ^= 0xff
	if _, err := OpenSession(wrongKeys.SessionAEAD, envelope, []byte("artifact:manwa")); !errors.Is(err, ErrInvalidEnvelope) {
		t.Fatalf("wrong key error = %v, want generic invalid envelope", err)
	}
	tampered := append([]byte(nil), envelope...)
	tampered[len(tampered)-1] = 'A'
	if _, err := OpenSession(keys.SessionAEAD, tampered, []byte("artifact:manwa")); !errors.Is(err, ErrInvalidEnvelope) {
		t.Fatalf("tampered envelope error = %v, want generic invalid envelope", err)
	}
	if _, err := OpenSession(keys.SessionAEAD, envelope, []byte("other-artifact")); !errors.Is(err, ErrInvalidEnvelope) {
		t.Fatalf("wrong associated data error = %v, want generic invalid envelope", err)
	}
}

func TestDigestDomainSeparationAndIdentityNormalizationContract(t *testing.T) {
	keys := testKeySet(t)
	credential := CredentialDigest(keys.CredentialHMAC, "same-value")
	identity := IdentityDigest(keys.IdentityHMAC, "artifact", "scheme", "same-value")
	if credential == identity {
		t.Fatal("credential and identity digests unexpectedly match")
	}
	if identity != IdentityDigest(keys.IdentityHMAC, "artifact", "scheme", "same-value") {
		t.Fatal("identity digest is not deterministic")
	}
	if identity == IdentityDigest(keys.IdentityHMAC, "other-artifact", "scheme", "same-value") {
		t.Fatal("artifact identity domain is not separated")
	}
	if identity == IdentityDigest(keys.IdentityHMAC, "artifact", "other-scheme", "same-value") {
		t.Fatal("identity scheme domain is not separated")
	}
}

func TestCursorProtocolClientBindingAndNegativeSequence(t *testing.T) {
	keys := testKeySet(t)
	cursor, err := EncodeCursor(keys.CursorMAC, "client-a", 42)
	if err != nil {
		t.Fatalf("encode cursor: %v", err)
	}
	decoded, err := DecodeCursor(keys.CursorMAC, cursor, "client-a")
	if err != nil || decoded.ClientID != "client-a" || decoded.ChangeSeq != 42 || decoded.Protocol != CursorProtocol {
		t.Fatalf("decode cursor = %+v, %v", decoded, err)
	}
	if _, err := DecodeCursor(keys.CursorMAC, cursor, "client-b"); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("cross-client cursor error = %v, want invalid cursor", err)
	}
	parts := strings.Split(cursor, ".")
	parts[2] = strings.TrimSuffix(parts[2], "A") + "A"
	if _, err := DecodeCursor(keys.CursorMAC, strings.Join(parts, "."), "client-a"); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("tampered cursor error = %v, want invalid cursor", err)
	}
	if _, err := EncodeCursor(keys.CursorMAC, "client-a", -1); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("negative cursor error = %v, want invalid cursor", err)
	}
	if _, err := DecodeCursor(keys.CursorMAC, "v2.e30.invalid", "client-a"); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("malformed cursor error = %v, want invalid cursor", err)
	}
}

func TestBearerTokenShapeAndIndependentAdminDigest(t *testing.T) {
	token, err := GenerateBearerToken()
	if err != nil {
		t.Fatalf("generate bearer token: %v", err)
	}
	if err := ValidateBearerTokenShape(token); err != nil {
		t.Fatalf("valid bearer token shape: %v", err)
	}
	if err := ValidateBearerTokenShape("short"); err == nil {
		t.Fatal("short bearer token unexpectedly accepted")
	}
	if DigestAdminBearerToken(token) == CredentialDigest(testKeySet(t).CredentialHMAC, token) {
		t.Fatal("admin digest reused credential HMAC domain")
	}
}
