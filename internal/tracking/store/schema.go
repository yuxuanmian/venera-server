package store

import (
	"context"
	"database/sql"
	"fmt"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS tracking_catalog_state (
    catalog_id TEXT PRIMARY KEY,
    active_revision TEXT NOT NULL,
    generation INTEGER NOT NULL,
    activated_at TEXT NOT NULL,
    digest TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tracking_client_state (
    device_id TEXT PRIMARY KEY,
    cloud_enabled INTEGER NOT NULL CHECK (cloud_enabled IN (0, 1)),
    state_revision INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tracking_interests (
    device_id TEXT NOT NULL,
    source_key TEXT NOT NULL,
    file_name TEXT NOT NULL,
    comic_id TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (device_id, source_key, file_name, comic_id)
);

CREATE TABLE IF NOT EXISTS tracking_observations (
    user_id TEXT NOT NULL,
    source_key TEXT NOT NULL,
    file_name TEXT NOT NULL,
    comic_id TEXT NOT NULL,
    catalog_revision TEXT NOT NULL,
    update_state_json TEXT,
    source_unread INTEGER CHECK (source_unread IN (0, 1)),
    marker TEXT,
    metadata_json TEXT,
    observed_at TEXT NOT NULL,
    valid_until TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    runtime_generation INTEGER NOT NULL,
    PRIMARY KEY (user_id, source_key, file_name, comic_id)
);

CREATE INDEX IF NOT EXISTS idx_tracking_interests_device
    ON tracking_interests(device_id, source_key, file_name);
CREATE INDEX IF NOT EXISTS idx_tracking_observations_revision
    ON tracking_observations(catalog_revision, runtime_generation);
CREATE INDEX IF NOT EXISTS idx_tracking_observations_user_freshness
    ON tracking_observations(user_id, valid_until);

CREATE TABLE IF NOT EXISTS tracking_scan_account_scope (
    user_id TEXT NOT NULL,
    source_key TEXT NOT NULL,
    file_name TEXT NOT NULL,
    session_epoch TEXT NOT NULL,
    visibility_scope TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (user_id, source_key, file_name)
);

CREATE TABLE IF NOT EXISTS tracking_scan_progress (
    user_id TEXT NOT NULL,
    source_key TEXT NOT NULL,
    file_name TEXT NOT NULL,
    catalog_id TEXT NOT NULL,
    catalog_revision TEXT NOT NULL,
    runtime_generation INTEGER NOT NULL,
    session_epoch TEXT NOT NULL,
    visibility_scope TEXT NOT NULL,
    demands_json TEXT NOT NULL,
    expected_total INTEGER,
    checkpoint_json TEXT NOT NULL,
    observations_json TEXT NOT NULL,
    boundary_digest TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (user_id, source_key, file_name)
);

CREATE INDEX IF NOT EXISTS idx_tracking_scan_progress_updated
    ON tracking_scan_progress(updated_at);

-- One durable scheduling record exists for each exact user/artifact demand.
-- The scanner payload remains in the immutable catalog/runtime; this table
-- owns only wake-up, retry, and lease state so a process restart cannot turn
-- a pending bounded slice into an in-memory-only promise.
CREATE TABLE IF NOT EXISTS tracking_scan_jobs (
    job_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    source_key TEXT NOT NULL,
    file_name TEXT NOT NULL,
    catalog_id TEXT NOT NULL,
    catalog_revision TEXT NOT NULL,
    runtime_generation INTEGER NOT NULL,
    demand_digest TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'running', 'completed', 'failed')),
    priority TEXT NOT NULL DEFAULT 'normal' CHECK (priority IN ('normal', 'expedited')),
    due_at TEXT NOT NULL,
    lease_until TEXT,
    lease_token TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (user_id, source_key, file_name)
);

CREATE INDEX IF NOT EXISTS idx_tracking_scan_jobs_due
    ON tracking_scan_jobs(state, due_at, priority, job_id);
CREATE INDEX IF NOT EXISTS idx_tracking_scan_jobs_lease
    ON tracking_scan_jobs(state, lease_until);
`

// Options controls limits owned by the tracking repository. The default is
// deliberately the shared V1 response cap.
type Options struct {
	ObservationLimit int
}

const DefaultObservationLimit = 10000

type Repository struct {
	db               *sql.DB
	observationLimit int
}

func NewRepository(db *sql.DB, limits ...int) (*Repository, error) {
	if db == nil {
		return nil, fmt.Errorf("tracking repository requires a database")
	}
	limit := DefaultObservationLimit
	if len(limits) > 0 && limits[0] > 0 {
		limit = limits[0]
	}
	if len(limits) > 1 {
		return nil, fmt.Errorf("tracking repository accepts at most one limit")
	}
	if _, err := db.ExecContext(context.Background(), schemaSQL); err != nil {
		return nil, fmt.Errorf("apply tracking schema: %w", err)
	}
	return &Repository{db: db, observationLimit: limit}, nil
}

func (r *Repository) DB() *sql.DB { return r.db }

func (r *Repository) ObservationLimit() int { return r.observationLimit }

func (r *Repository) SetObservationLimit(limit int) error {
	if limit <= 0 {
		return fmt.Errorf("tracking observation limit must be positive")
	}
	r.observationLimit = limit
	return nil
}
