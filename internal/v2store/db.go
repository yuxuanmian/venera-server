package v2store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

// The migration is embedded so a server binary always carries the exact
// schema it was built and tested against.
//
//go:embed migrations/0001_v2.sql
var migrationFiles embed.FS

const (
	defaultBusyRetry = 5 * time.Second
	defaultBusySleep = 5 * time.Millisecond
)

type FaultInjector interface {
	Inject(point string) error
}

type FaultFunc func(point string) error

func (f FaultFunc) Inject(point string) error {
	if f == nil {
		return nil
	}
	return f(point)
}

type Options struct {
	Clock       func() time.Time
	IDGenerator func(prefix string) string
	Fault       FaultInjector
	BusyRetry   time.Duration
}

type DB struct {
	sql         *sql.DB
	writeMu     sync.Mutex
	migrationMu sync.Mutex
	clock       func() time.Time
	idGenerator func(prefix string) string
	fault       FaultInjector
	busyRetry   time.Duration
	idCounter   atomic.Uint64
}

func Open(path string) (*DB, error) {
	return OpenWithOptions(path, Options{})
}

func OpenWithOptions(path string, options Options) (*DB, error) {
	if path == "" {
		return nil, errors.New("v2 database path is required")
	}
	if path != ":memory:" && !strings.HasPrefix(path, "file:") {
		if dir := filepath.Dir(path); dir != "." && dir != "" {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, fmt.Errorf("create v2 database directory: %w", err)
			}
		}
	}

	database, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open v2 sqlite: %w", err)
	}

	db := &DB{
		sql:       database,
		clock:     options.Clock,
		fault:     options.Fault,
		busyRetry: options.BusyRetry,
	}
	if db.clock == nil {
		db.clock = time.Now
	}
	if db.busyRetry <= 0 {
		db.busyRetry = defaultBusyRetry
	}
	if options.IDGenerator != nil {
		db.idGenerator = options.IDGenerator
	} else {
		db.idGenerator = db.defaultID
	}
	// A small pool permits read requests while a writer is active. Every
	// connection is configured on acquisition in ReadTx/WriteTx.
	database.SetMaxOpenConns(8)
	database.SetMaxIdleConns(8)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.configure(ctx); err != nil {
		database.Close()
		return nil, fmt.Errorf("configure v2 sqlite: %w", err)
	}
	if err := db.Migrate(ctx); err != nil {
		database.Close()
		return nil, fmt.Errorf("migrate v2 sqlite: %w", err)
	}
	if err := db.ValidateReady(ctx); err != nil {
		database.Close()
		return nil, fmt.Errorf("validate v2 sqlite: %w", err)
	}
	return db, nil
}

func (db *DB) Close() error {
	if db == nil || db.sql == nil {
		return nil
	}
	return db.sql.Close()
}

func (db *DB) SQL() *sql.DB {
	return db.sql
}

func (db *DB) Now() time.Time {
	return db.clock().UTC()
}

func (db *DB) NewID(prefix string) string {
	return db.idGenerator(prefix)
}

func (db *DB) configure(ctx context.Context) error {
	return db.withConn(ctx, func(conn *sql.Conn) error {
		return configureConn(ctx, conn)
	})
}

func configureConn(ctx context.Context, conn *sql.Conn) error {
	for _, statement := range []string{
		"PRAGMA foreign_keys = ON",
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA temp_store = MEMORY",
	} {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("%s: %w", statement, err)
		}
	}
	return nil
}

func (db *DB) withConn(ctx context.Context, fn func(*sql.Conn) error) error {
	conn, err := db.sql.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(conn)
}

func (db *DB) Migrate(ctx context.Context) error {
	db.migrationMu.Lock()
	defer db.migrationMu.Unlock()

	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	return db.withConn(ctx, func(conn *sql.Conn) error {
		if err := configureConn(ctx, conn); err != nil {
			return err
		}
		if err := beginImmediate(ctx, conn, db.busyRetry); err != nil {
			return fmt.Errorf("begin migration: %w", err)
		}
		committed := false
		defer func() {
			if !committed {
				_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
			}
		}()

		migrationSQL, err := migrationFiles.ReadFile("migrations/0001_v2.sql")
		if err != nil {
			return fmt.Errorf("read v2 migration: %w", err)
		}
		statements := splitSQLStatements(string(migrationSQL))
		hasSchemaMigrations, err := tableExists(ctx, conn, "schema_migrations")
		if err != nil {
			return fmt.Errorf("inspect migration table: %w", err)
		}
		applied, err := migrationVersions(ctx, conn)
		if err != nil {
			return fmt.Errorf("inspect migration versions: %w", err)
		}
		for version := range applied {
			if version != 1 {
				return fmt.Errorf("unknown v2 schema migration version %d", version)
			}
		}
		if applied[1] {
			if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
				return fmt.Errorf("commit repeated migration: %w", err)
			}
			committed = true
			return nil
		}

		statementNumber := 0
		for _, statement := range statements {
			trimmed := strings.TrimSpace(statement)
			if trimmed == "" {
				continue
			}
			if hasSchemaMigrations && strings.HasPrefix(strings.ToUpper(trimmed), "CREATE TABLE SCHEMA_MIGRATIONS") {
				continue
			}
			statementNumber++
			if err := db.inject(fmt.Sprintf("migration.statement.%d", statementNumber)); err != nil {
				return fmt.Errorf("migration fault at statement %d: %w", statementNumber, err)
			}
			if _, err := conn.ExecContext(ctx, trimmed); err != nil {
				return fmt.Errorf("migration statement %d: %w", statementNumber, err)
			}
		}
		if _, err := conn.ExecContext(ctx,
			"INSERT INTO schema_migrations(version, name, applied_at) VALUES(1, '0001_v2', ?)",
			db.Now().Format(time.RFC3339Nano)); err != nil {
			return fmt.Errorf("record v2 migration: %w", err)
		}
		if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
			return fmt.Errorf("commit migration: %w", err)
		}
		committed = true
		return nil
	})
}

func (db *DB) ValidateReady(ctx context.Context) error {
	return db.withConn(ctx, func(conn *sql.Conn) error {
		if err := configureConn(ctx, conn); err != nil {
			return err
		}
		var journalMode string
		if err := conn.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journalMode); err != nil {
			return err
		}
		if !strings.EqualFold(journalMode, "wal") {
			return fmt.Errorf("journal mode is %q, want WAL", journalMode)
		}
		var foreignKeyViolations int
		if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_foreign_key_check").Scan(&foreignKeyViolations); err != nil {
			return err
		}
		if foreignKeyViolations != 0 {
			return fmt.Errorf("foreign key check found %d violations", foreignKeyViolations)
		}
		var activeManifestCount int
		if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM manifest_catalogs WHERE state = 'active'").Scan(&activeManifestCount); err != nil {
			return err
		}
		if activeManifestCount > 1 {
			return errors.New("multiple active manifest catalogs")
		}
		rows, err := conn.QueryContext(ctx, `
			SELECT artifact_id, COUNT(*) FROM source_package_releases
			WHERE state = 'active' GROUP BY artifact_id HAVING COUNT(*) > 1`)
		if err != nil {
			return err
		}
		defer rows.Close()
		if rows.Next() {
			return errors.New("multiple active package releases for an artifact")
		}
		return rows.Err()
	})
}

func (db *DB) inject(point string) error {
	if db.fault == nil {
		return nil
	}
	return db.fault.Inject(point)
}

func (db *DB) defaultID(prefix string) string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		counter := db.idCounter.Add(1)
		return fmt.Sprintf("%s_%d_%d", prefix, db.Now().UnixNano(), counter)
	}
	return prefix + "_" + hex.EncodeToString(raw[:])
}

func splitSQLStatements(script string) []string {
	return strings.Split(script, ";")
}

func tableExists(ctx context.Context, conn *sql.Conn, name string) (bool, error) {
	var count int
	err := conn.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?", name).Scan(&count)
	return count > 0, err
}

func migrationVersions(ctx context.Context, conn *sql.Conn) (map[int]bool, error) {
	if ok, err := tableExists(ctx, conn, "schema_migrations"); err != nil || !ok {
		return map[int]bool{}, err
	}
	rows, err := conn.QueryContext(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	versions := make(map[int]bool)
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		versions[version] = true
	}
	return versions, rows.Err()
}

func isBusyError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "database is locked") || strings.Contains(message, "database table is locked") || strings.Contains(message, "database is busy")
}

func beginImmediate(ctx context.Context, conn *sql.Conn, retryFor time.Duration) error {
	deadline := time.Now().Add(retryFor)
	for {
		if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err == nil {
			return nil
		} else if !isBusyError(err) || time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(defaultBusySleep):
		}
	}
}
