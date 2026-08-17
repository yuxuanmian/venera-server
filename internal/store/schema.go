package store

const schemaSQL = `
CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    tz TEXT NOT NULL,
    locale TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS devices (
    device_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(user_id),
    token_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    last_seen_at TEXT
);

CREATE TABLE IF NOT EXISTS source_sessions (
    user_id TEXT NOT NULL,
    source TEXT NOT NULL,
    cookie_encrypted TEXT NOT NULL,
    cookie_hash TEXT NOT NULL,
    ua TEXT,
    proxy TEXT,
    script_hash TEXT,
    init_js_hash TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (user_id, source)
);

CREATE TABLE IF NOT EXISTS pairing_codes (
    code_hash TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    used_at TEXT
);

CREATE TABLE IF NOT EXISTS mirror (
    user_id TEXT NOT NULL,
    source TEXT NOT NULL,
    comic_id TEXT NOT NULL,
    due_at TEXT NOT NULL,
    last_check_time TEXT,
    last_update_time TEXT,
    priority TEXT NOT NULL DEFAULT 'normal',
    updated_at TEXT NOT NULL,
    PRIMARY KEY (user_id, source, comic_id)
);

CREATE TABLE IF NOT EXISTS deleted_keys (
    user_id TEXT NOT NULL,
    source TEXT NOT NULL,
    comic_id TEXT NOT NULL,
    deleted_at TEXT NOT NULL,
    PRIMARY KEY (user_id, source, comic_id)
);

CREATE TABLE IF NOT EXISTS jobs (
    job_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    source TEXT NOT NULL,
    comic_id TEXT NOT NULL,
    chunk_id INTEGER,
    state TEXT NOT NULL,
    priority TEXT NOT NULL,
    due_at TEXT,
    scheduled_at TEXT,
    created_at TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    next_retry_at TEXT,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    source TEXT NOT NULL,
    state TEXT NOT NULL,
    scheduled_at TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS results (
    result_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    source TEXT NOT NULL,
    comic_id TEXT NOT NULL,
    client_result_id TEXT UNIQUE,
    scan_time TEXT NOT NULL,
    script_hash TEXT,
    init_js_hash TEXT,
    cookie_hash TEXT,
    outcome TEXT NOT NULL,
    payload TEXT,
    detail TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS scripts (
    hash TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    path TEXT NOT NULL,
    size INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_events (
    event_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    processed_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_results_user_cursor ON results(user_id, result_id);
CREATE INDEX IF NOT EXISTS idx_results_user_key_time ON results(user_id, source, comic_id, scan_time);
CREATE INDEX IF NOT EXISTS idx_jobs_user_state ON jobs(user_id, state, priority, scheduled_at);
CREATE INDEX IF NOT EXISTS idx_mirror_user_due ON mirror(user_id, due_at);
`
