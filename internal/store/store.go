package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

func Open(dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	dbPath := filepath.Join(dataDir, "venera.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)

	if _, err := db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set WAL: %w", err)
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	if err := migrate(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	return &Store{db: db}, nil
}

func migrate(db *sql.DB) error {
	columns := []struct {
		table, column, ddl string
	}{
		{"jobs", "attempts", "ALTER TABLE jobs ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0"},
		{"jobs", "max_attempts", "ALTER TABLE jobs ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 3"},
		{"jobs", "next_retry_at", "ALTER TABLE jobs ADD COLUMN next_retry_at TEXT"},
		{"jobs", "last_error", "ALTER TABLE jobs ADD COLUMN last_error TEXT"},
	}
	for _, c := range columns {
		ok, err := columnExists(db, c.table, c.column)
		if err != nil {
			return err
		}
		if !ok {
			if _, err := db.Exec(c.ddl); err != nil {
				return fmt.Errorf("add column %s.%s: %w", c.table, c.column, err)
			}
		}
	}
	return nil
}

func columnExists(db *sql.DB, table, column string) (bool, error) {
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notnull, pk int
		var name, ctype string
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) DB() *sql.DB {
	return s.db
}
