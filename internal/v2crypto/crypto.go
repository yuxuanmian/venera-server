package v2crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	RootSecretSize  = 32
	EnvelopeVersion = "v1"
	CursorProtocol  = 2
)

var ErrInvalidEnvelope = errors.New("invalid protected envelope")
var ErrInvalidCursor = errors.New("invalid client cursor")

type KeySet struct {
	CredentialHMAC []byte
	IdentityHMAC   []byte
	SessionAEAD    []byte
	CursorMAC      []byte
	ReceiptMAC     []byte
}

func DeriveKeys(rootSecret []byte) (KeySet, error) {
	if len(rootSecret) != RootSecretSize {
		return KeySet{}, errors.New("root secret must be exactly 32 bytes")
	}
	return KeySet{
		CredentialHMAC: deriveHKDF(rootSecret, "credential-hmac-v1"),
		IdentityHMAC:   deriveHKDF(rootSecret, "identity-hmac-v1"),
		SessionAEAD:    deriveHKDF(rootSecret, "session-aead-v1"),
		CursorMAC:      deriveHKDF(rootSecret, "cursor-mac-v1"),
		ReceiptMAC:     deriveHKDF(rootSecret, "receipt-mac-v1"),
	}, nil
}

func deriveHKDF(secret []byte, context string) []byte {
	// RFC 5869 HKDF-SHA256 with a fixed, domain-specific info string.
	extract := hmac.New(sha256.New, make([]byte, sha256.Size))
	_, _ = extract.Write(secret)
	prk := extract.Sum(nil)
	expand := hmac.New(sha256.New, prk)
	_, _ = expand.Write([]byte(context))
	_, _ = expand.Write([]byte{1})
	return expand.Sum(nil)
}

func Digest(key []byte, value string) string {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(value))
	return hex.EncodeToString(mac.Sum(nil))
}

func CredentialDigest(key []byte, value string) string {
	return Digest(key, "credential-hmac-v1\x00"+value)
}

func IdentityDigest(key []byte, artifactID, scheme, normalizedValue string) string {
	return Digest(key, artifactID+"\x00"+scheme+"\x00"+normalizedValue)
}

func SealSession(key, plaintext, associatedData []byte) ([]byte, error) {
	return Seal(key, plaintext, associatedData)
}

func OpenSession(key, envelope, associatedData []byte) ([]byte, error) {
	return Open(key, envelope, associatedData)
}

func Seal(key, plaintext, associatedData []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrInvalidEnvelope
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, ErrInvalidEnvelope
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, ErrInvalidEnvelope
	}
	sealed := aead.Seal(nil, nonce, plaintext, associatedData)
	encoded := base64.RawURLEncoding.EncodeToString(append(nonce, sealed...))
	return []byte(EnvelopeVersion + "." + encoded), nil
}

func Open(key, envelope, associatedData []byte) ([]byte, error) {
	parts := strings.Split(string(envelope), ".")
	if len(parts) != 2 || parts[0] != EnvelopeVersion {
		return nil, ErrInvalidEnvelope
	}
	encoded, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, ErrInvalidEnvelope
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrInvalidEnvelope
	}
	aead, err := cipher.NewGCM(block)
	if err != nil || len(encoded) < aead.NonceSize() {
		return nil, ErrInvalidEnvelope
	}
	plain, err := aead.Open(nil, encoded[:aead.NonceSize()], encoded[aead.NonceSize():], associatedData)
	if err != nil {
		return nil, ErrInvalidEnvelope
	}
	return plain, nil
}

type CursorPayload struct {
	Protocol  int    `json:"protocol"`
	ClientID  string `json:"clientId"`
	ChangeSeq int64  `json:"changeSeq"`
}

func EncodeCursor(key []byte, clientID string, changeSeq int64) (string, error) {
	if clientID == "" || changeSeq < 0 {
		return "", ErrInvalidCursor
	}
	payload := CursorPayload{Protocol: CursorProtocol, ClientID: clientID, ChangeSeq: changeSeq}
	encodedPayload, err := json.Marshal(payload)
	if err != nil {
		return "", ErrInvalidCursor
	}
	part := base64.RawURLEncoding.EncodeToString(encodedPayload)
	unsigned := "v2." + part
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(unsigned))
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func DecodeCursor(key []byte, cursor, expectedClientID string) (CursorPayload, error) {
	parts := strings.Split(cursor, ".")
	if len(parts) != 3 || parts[0] != "v2" || parts[1] == "" || parts[2] == "" {
		return CursorPayload{}, ErrInvalidCursor
	}
	providedMAC, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return CursorPayload{}, ErrInvalidCursor
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(parts[0] + "." + parts[1]))
	if !hmac.Equal(providedMAC, mac.Sum(nil)) {
		return CursorPayload{}, ErrInvalidCursor
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return CursorPayload{}, ErrInvalidCursor
	}
	var payload CursorPayload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return CursorPayload{}, ErrInvalidCursor
	}
	if payload.Protocol != CursorProtocol ||
		payload.ClientID == "" ||
		payload.ChangeSeq < 0 ||
		expectedClientID != "" && payload.ClientID != expectedClientID {
		return CursorPayload{}, ErrInvalidCursor
	}
	return payload, nil
}

func GenerateBearerToken() (string, error) {
	raw := make([]byte, RootSecretSize)
	if _, err := io.ReadFull(rand.Reader, raw); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func DigestAdminBearerToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func ValidateBearerTokenShape(token string) error {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) != RootSecretSize || strings.Contains(token, "=") {
		return fmt.Errorf("invalid bearer token shape")
	}
	return nil
}
