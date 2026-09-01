package main

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"venera-server/internal/v2config"
	"venera-server/internal/v2crypto"
	"venera-server/internal/v2domain"
	"venera-server/internal/v2store"
)

func TestEnrollmentCLIPrintsOneTimeCodeWithTTLAndNoSecretsInList(t *testing.T) {
	cfg := v2config.Default()
	cfg.RootSecret = []byte(strings.Repeat("r", v2crypto.RootSecretSize))
	cfg.DBPath = filepath.Join(t.TempDir(), "v2", "server.db")
	cfg.ManifestCacheDir = t.TempDir()
	var output strings.Builder
	if err := runWithConfig(context.Background(), cfg, []string{"client", "enrollment", "create", "--ttl", "10m"}, &output); err != nil {
		t.Fatalf("create enrollment code: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 1 || v2crypto.ValidateBearerTokenShape(lines[0]) != nil {
		t.Fatalf("enrollment stdout = %q", output.String())
	}
	code := lines[0]

	keys, err := v2crypto.DeriveKeys(cfg.RootSecret)
	if err != nil {
		t.Fatal(err)
	}
	db, err := v2store.Open(cfg.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	repo := v2store.NewRepository(db, keys)
	var state, expiresAt string
	if err := db.SQL().QueryRow(`SELECT state, expires_at FROM client_enrollment_codes WHERE code_digest = ?`, repo.EnrollmentCodeDigest(code)).Scan(&state, &expiresAt); err != nil {
		t.Fatal(err)
	}
	expires, err := time.Parse(time.RFC3339Nano, expiresAt)
	if err != nil || !expires.After(time.Now().UTC().Add(9*time.Minute)) || !expires.Before(time.Now().UTC().Add(11*time.Minute)) || state != "active" {
		t.Fatalf("enrollment record state/expiry = %s/%s", state, expiresAt)
	}
	listOutput := strings.Builder{}
	if err := runWithConfig(context.Background(), cfg, []string{"client", "list"}, &listOutput); err != nil {
		t.Fatalf("list clients: %v", err)
	}
	if strings.Contains(listOutput.String(), code) || strings.Contains(listOutput.String(), "tokenDigest") {
		t.Fatalf("client list exposed enrollment secret: %s", listOutput.String())
	}
	_ = db.Close()
}

func TestAdminCLIRevokeUnquarantineAndDBCheck(t *testing.T) {
	cfg := v2config.Default()
	cfg.RootSecret = []byte(strings.Repeat("s", v2crypto.RootSecretSize))
	cfg.DBPath = filepath.Join(t.TempDir(), "server.db")
	cfg.ManifestCacheDir = t.TempDir()
	keys, err := v2crypto.DeriveKeys(cfg.RootSecret)
	if err != nil {
		t.Fatal(err)
	}
	db, err := v2store.Open(cfg.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	repo := v2store.NewRepository(db, keys)
	code, err := repo.CreateEnrollmentCode(context.Background(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	token, err := v2crypto.GenerateBearerToken()
	if err != nil {
		t.Fatal(err)
	}
	enrolled, err := repo.ClaimClientEnrollment(context.Background(), v2store.ClaimClientEnrollmentRequest{
		PendingClientID: "cli-test", TokenDigest: v2crypto.CredentialDigest(keys.CredentialHMAC, token), EnrollmentCodeDigest: repo.EnrollmentCodeDigest(code.Code), IdempotencyKey: "cli-claim",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.WriteTx(context.Background(), func(tx *v2store.Tx) error {
		if _, err := tx.ExecContext(context.Background(), `INSERT INTO source_artifacts(artifact_id, source_key, catalog_id, managed_state, revision, created_at, updated_at) VALUES('cli-source', 'cli-source', 'cli-catalog', 'active', 1, ?, ?)`, db.Now().Format(time.RFC3339Nano), db.Now().Format(time.RFC3339Nano)); err != nil {
			return err
		}
		_, err := tx.ExecContext(context.Background(), `INSERT INTO source_runtime_state(artifact_id, state, target_concurrency, effective_concurrency, failure_streak, revision, updated_at) VALUES('cli-source', 'quarantined', 1, 0, 3, 1, ?)`, db.Now().Format(time.RFC3339Nano))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	var unquarantineOutput strings.Builder
	if err := runWithConfig(context.Background(), cfg, []string{"source", "unquarantine", "--artifact", "cli-source"}, &unquarantineOutput); err != nil {
		t.Fatalf("unquarantine: %v", err)
	}
	if !strings.Contains(unquarantineOutput.String(), `"runtimeState":"healthy"`) {
		t.Fatalf("unquarantine output = %s", unquarantineOutput.String())
	}
	var revokeOutput strings.Builder
	if err := runWithConfig(context.Background(), cfg, []string{"client", "revoke", "--client", string(enrolled.Client.ID), "--expected-revision", "1"}, &revokeOutput); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	var revoked map[string]any
	if err := json.Unmarshal([]byte(revokeOutput.String()), &revoked); err != nil || revoked["state"] != string(v2domain.ClientRevoked) {
		t.Fatalf("revoke output = %s", revokeOutput.String())
	}
	var checkOutput strings.Builder
	if err := runWithConfig(context.Background(), cfg, []string{"db", "check"}, &checkOutput); err != nil {
		t.Fatalf("db check: %v", err)
	}
	if !strings.Contains(checkOutput.String(), `"status":"ok"`) {
		t.Fatalf("db check output = %s", checkOutput.String())
	}
	for _, command := range [][]string{{"user", "invite"}, {"client", "pairing"}} {
		if err := runWithConfig(context.Background(), cfg, command, &strings.Builder{}); !errors.Is(err, errCLIUsage) {
			t.Fatalf("legacy admin command %v error = %v", command, err)
		}
	}
}
