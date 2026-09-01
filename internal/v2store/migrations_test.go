package v2store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMigrateEmptyAndRepeated(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "server-v2.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open empty v2 database: %v", err)
	}
	defer db.Close()

	assertMigrationState(t, db)
	if err := db.Migrate(context.Background()); err != nil {
		t.Fatalf("repeat migration: %v", err)
	}
	assertMigrationState(t, db)

	var foreignKeys int
	if err := db.SQL().QueryRow("PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		t.Fatalf("read foreign_keys pragma: %v", err)
	}
	if foreignKeys != 1 {
		t.Fatalf("foreign_keys = %d, want 1", foreignKeys)
	}

	var journalMode string
	if err := db.SQL().QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatalf("read journal_mode pragma: %v", err)
	}
	if strings.ToLower(journalMode) != "wal" {
		t.Fatalf("journal_mode = %q, want wal", journalMode)
	}

	var oldModelCount int
	if err := db.SQL().QueryRow(`
		SELECT COUNT(*) FROM sqlite_master
		WHERE type = 'table' AND name IN ('service_users', 'devices', 'source_bindings')`).Scan(&oldModelCount); err != nil {
		t.Fatalf("inspect forbidden old tables: %v", err)
	}
	if oldModelCount != 0 {
		t.Fatalf("found %d forbidden old-model tables", oldModelCount)
	}
}

func TestMigrationFaultRollsBackWholeBatch(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "faulted.db")
	db, err := OpenWithOptions(dbPath, Options{
		Fault: FaultFunc(func(point string) error {
			if point == "migration.statement.7" {
				return errors.New("injected migration fault")
			}
			return nil
		}),
	})
	if err == nil {
		db.Close()
		t.Fatal("faulted migration unexpectedly succeeded")
	}

	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open faulted database: %v", err)
	}
	defer raw.Close()
	var tableCount int
	if err := raw.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'").Scan(&tableCount); err != nil {
		t.Fatalf("inspect rolled back database: %v", err)
	}
	if tableCount != 0 {
		t.Fatalf("faulted migration left %d tables", tableCount)
	}

	clean, err := Open(dbPath)
	if err != nil {
		t.Fatalf("retry migration after rollback: %v", err)
	}
	defer clean.Close()
	assertMigrationState(t, clean)
}

func TestMigrationRejectsUnknownVersion(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "unknown.db")
	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open raw database: %v", err)
	}
	if _, err := raw.Exec(`
		CREATE TABLE schema_migrations (
			version INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			applied_at TEXT NOT NULL
		) STRICT;
		INSERT INTO schema_migrations(version, name, applied_at) VALUES(99, 'future', '2026-08-28T00:00:00Z')`); err != nil {
		raw.Close()
		t.Fatalf("seed unknown migration: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw database: %v", err)
	}

	if _, err := Open(dbPath); err == nil || !strings.Contains(err.Error(), "unknown v2 schema migration version 99") {
		t.Fatalf("unknown migration error = %v", err)
	}
}

func TestMigrationConstraintsAndForeignKeys(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "constraints.db"))
	if err != nil {
		t.Fatalf("open constraints database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.SQL().ExecContext(ctx, `
		INSERT INTO client_installations(client_id, token_digest, state, revision, created_at, updated_at)
		VALUES('cli_test', 'digest_test', 'active', 1, '2026-08-28T00:00:00Z', '2026-08-28T00:00:00Z')`); err != nil {
		t.Fatalf("insert client: %v", err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
		INSERT INTO client_sync_state(client_id, updated_at) VALUES('missing', '2026-08-28T00:00:00Z')`); err == nil {
		t.Fatal("foreign key violation unexpectedly succeeded")
	}
	if _, err := db.SQL().ExecContext(ctx, `
		INSERT INTO client_installations(client_id, token_digest, state, revision, created_at, updated_at, unknown_column)
		VALUES('cli_strict', 'digest_strict', 'active', 1, '2026-08-28T00:00:00Z', '2026-08-28T00:00:00Z', 'x')`); err == nil {
		t.Fatal("strict-table unknown column unexpectedly succeeded")
	}
	if _, err := db.SQL().ExecContext(ctx, `
		INSERT INTO source_artifacts(artifact_id, source_key, catalog_id, managed_state, revision, created_at, updated_at)
		VALUES('art_test', 'manwa', 'cat_test', 'active', 1, '2026-08-28T00:00:00Z', '2026-08-28T00:00:00Z')`); err != nil {
		t.Fatalf("insert source artifact: %v", err)
	}
	for _, releaseState := range []string{"active", "active"} {
		_, insertErr := db.SQL().ExecContext(ctx, `
			INSERT INTO source_package_releases(
				package_release_id, artifact_id, catalog_id, catalog_sequence, core_hash,
				marker_schemes_json, package_json, state, revision, created_at, updated_at
			) VALUES(?, 'art_test', 'cat_test', 1, 'hash', '{}', '{}', ?, 1, '2026-08-28T00:00:00Z', '2026-08-28T00:00:00Z')`,
			"rel_"+releaseState+time.Now().Format(".000000000"), releaseState)
		if releaseState == "active" && insertErr == nil {
			// The first active release is valid; the second is rejected below by
			// the partial unique index.
			if _, secondErr := db.SQL().ExecContext(ctx, `
				INSERT INTO source_package_releases(
					package_release_id, artifact_id, catalog_id, catalog_sequence, core_hash,
					marker_schemes_json, package_json, state, revision, created_at, updated_at
				) VALUES('rel_second', 'art_test', 'cat_test', 2, 'hash2', '{}', '{}', 'active', 1, '2026-08-28T00:00:00Z', '2026-08-28T00:00:00Z')`); secondErr == nil {
				t.Fatal("second active package release unexpectedly succeeded")
			}
			break
		}
	}
}

func TestWriteTransactionCommitFaultRollsBack(t *testing.T) {
	commitFault := true
	db, err := OpenWithOptions(filepath.Join(t.TempDir(), "commit-fault.db"), Options{
		Fault: FaultFunc(func(point string) error {
			if point == "transaction.commit" && commitFault {
				commitFault = false
				return errors.New("injected commit fault")
			}
			return nil
		}),
	})
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer db.Close()

	err = db.WriteTx(context.Background(), func(tx *Tx) error {
		_, err := tx.ExecContext(context.Background(), `
			INSERT INTO client_installations(client_id, token_digest, state, revision, created_at, updated_at)
			VALUES('cli_rollback', 'digest_rollback', 'active', 1, '2026-08-28T00:00:00Z', '2026-08-28T00:00:00Z')`)
		return err
	})
	if err == nil {
		t.Fatal("commit fault unexpectedly succeeded")
	}
	var count int
	if err := db.SQL().QueryRow("SELECT COUNT(*) FROM client_installations WHERE client_id = 'cli_rollback'").Scan(&count); err != nil {
		t.Fatalf("check rollback: %v", err)
	}
	if count != 0 {
		t.Fatalf("commit fault left %d client rows", count)
	}
}

func TestConcurrentWritersRetryBusyDatabase(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "busy.db")
	first, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open first database: %v", err)
	}
	defer first.Close()
	second, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open second database: %v", err)
	}
	defer second.Close()

	started := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- first.WriteTx(context.Background(), func(tx *Tx) error {
			close(started)
			time.Sleep(100 * time.Millisecond)
			_, err := tx.ExecContext(context.Background(), `
				INSERT INTO client_installations(client_id, token_digest, state, revision, created_at, updated_at)
				VALUES('cli_busy_1', 'digest_busy_1', 'active', 1, '2026-08-28T00:00:00Z', '2026-08-28T00:00:00Z')`)
			return err
		})
	}()
	<-started
	if err := second.WriteTx(context.Background(), func(tx *Tx) error {
		_, err := tx.ExecContext(context.Background(), `
			INSERT INTO client_installations(client_id, token_digest, state, revision, created_at, updated_at)
			VALUES('cli_busy_2', 'digest_busy_2', 'active', 1, '2026-08-28T00:00:00Z', '2026-08-28T00:00:00Z')`)
		return err
	}); err != nil {
		t.Fatalf("second writer did not retry busy database: %v", err)
	}
	if err := <-firstDone; err != nil {
		t.Fatalf("first writer: %v", err)
	}
}

func assertMigrationState(t *testing.T, db *DB) {
	t.Helper()
	var versionCount int
	if err := db.SQL().QueryRow("SELECT COUNT(*) FROM schema_migrations WHERE version = 1").Scan(&versionCount); err != nil {
		t.Fatalf("read migration version: %v", err)
	}
	if versionCount != 1 {
		t.Fatalf("migration version count = %d, want 1", versionCount)
	}
	var tableCount int
	if err := db.SQL().QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name <> 'sqlite_sequence'").Scan(&tableCount); err != nil {
		t.Fatalf("read table count: %v", err)
	}
	if tableCount != 30 {
		t.Fatalf("table count = %d, want 30", tableCount)
	}
	var fkViolations int
	if err := db.SQL().QueryRow("SELECT COUNT(*) FROM pragma_foreign_key_check").Scan(&fkViolations); err != nil {
		t.Fatalf("foreign key check: %v", err)
	}
	if fkViolations != 0 {
		t.Fatalf("foreign key violations = %d", fkViolations)
	}
}
