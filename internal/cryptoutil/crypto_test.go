package cryptoutil

import (
	"bytes"
	"testing"
)

func TestEncryptDecryptRoundTrip(t *testing.T) {
	key := bytes.Repeat([]byte{1}, 32)
	plain := "session=abc; token=xyz"
	ct, err := Encrypt(plain, key)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	got, err := Decrypt(ct, key)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if got != plain {
		t.Fatalf("roundtrip mismatch: %q != %q", got, plain)
	}
}

func TestDecryptWrongKey(t *testing.T) {
	key := bytes.Repeat([]byte{2}, 32)
	other := bytes.Repeat([]byte{3}, 32)
	ct, err := Encrypt("secret", key)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if _, err := Decrypt(ct, other); err == nil {
		t.Fatal("expected error with wrong key")
	}
}

func TestDecryptTampered(t *testing.T) {
	key := bytes.Repeat([]byte{4}, 32)
	ct, err := Encrypt("secret", key)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	// Flip a byte in the base64-decoded payload is tricky; corrupt the string simply.
	if _, err := Decrypt(ct[:len(ct)-2]+"AA", key); err == nil {
		t.Fatal("expected error with tampered ciphertext")
	}
}

func TestLoadOrCreateKeyPersists(t *testing.T) {
	dir := t.TempDir()
	k1, err := LoadOrCreateKey(dir)
	if err != nil {
		t.Fatalf("load create: %v", err)
	}
	k2, err := LoadOrCreateKey(dir)
	if err != nil {
		t.Fatalf("load again: %v", err)
	}
	if len(k1) != 32 || !bytes.Equal(k1, k2) {
		t.Fatal("key not persisted/stable")
	}
}
