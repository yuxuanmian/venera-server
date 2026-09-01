CREATE TABLE schema_migrations (
    version        INTEGER PRIMARY KEY,
    name           TEXT NOT NULL,
    applied_at     TEXT NOT NULL
) STRICT;

CREATE TABLE client_installations (
    client_id          TEXT PRIMARY KEY,
    token_digest       TEXT NOT NULL UNIQUE,
    display_name       TEXT NOT NULL DEFAULT '',
    platform           TEXT NOT NULL DEFAULT '',
    app_version        TEXT NOT NULL DEFAULT '',
    state              TEXT NOT NULL CHECK (state IN ('active','revoked')),
    revision           INTEGER NOT NULL CHECK (revision >= 1),
    last_seen_at       TEXT,
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL,
    revoked_at         TEXT
) STRICT;

CREATE INDEX idx_client_installations_state
    ON client_installations(state, last_seen_at);

CREATE TABLE client_enrollment_codes (
    code_id             TEXT PRIMARY KEY,
    code_digest         TEXT NOT NULL UNIQUE,
    state               TEXT NOT NULL CHECK (state IN ('active','claimed','revoked','expired')),
    expires_at          TEXT NOT NULL,
    claimed_client_id   TEXT REFERENCES client_installations(client_id),
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    claimed_at          TEXT,
    revoked_at          TEXT
) STRICT;

CREATE INDEX idx_client_enrollment_codes_expiry
    ON client_enrollment_codes(state, expires_at);

CREATE TABLE idempotency_records (
    actor_kind          TEXT NOT NULL CHECK (actor_kind IN ('client','pendingClient','admin')),
    actor_id            TEXT NOT NULL,
    route_key           TEXT NOT NULL,
    idempotency_digest  TEXT NOT NULL,
    request_digest      TEXT NOT NULL,
    response_status     INTEGER,
    response_body       BLOB,
    state               TEXT NOT NULL CHECK (state IN ('running','completed','failed')),
    expires_at          TEXT NOT NULL,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    PRIMARY KEY (actor_kind, actor_id, route_key, idempotency_digest)
) WITHOUT ROWID, STRICT;

CREATE INDEX idx_idempotency_records_expiry
    ON idempotency_records(expires_at);

CREATE TABLE manifest_catalogs (
    manifest_id         TEXT PRIMARY KEY,
    catalog_id          TEXT NOT NULL,
    manifest_url        TEXT NOT NULL,
    catalog_sequence    INTEGER NOT NULL CHECK (catalog_sequence >= 0),
    manifest_hash       TEXT NOT NULL,
    manifest_json       TEXT NOT NULL,
    state               TEXT NOT NULL CHECK (state IN ('active','superseded','rejected')),
    revision            INTEGER NOT NULL CHECK (revision >= 1),
    fetched_at          TEXT NOT NULL,
    activated_at        TEXT,
    updated_at          TEXT NOT NULL,
    UNIQUE (catalog_id, catalog_sequence)
) STRICT;

CREATE UNIQUE INDEX idx_manifest_one_active
    ON manifest_catalogs(state) WHERE state = 'active';

CREATE TABLE source_artifacts (
    artifact_id         TEXT PRIMARY KEY,
    source_key          TEXT NOT NULL,
    catalog_id          TEXT NOT NULL,
    managed_state       TEXT NOT NULL CHECK (managed_state IN ('active','paused')),
    revision            INTEGER NOT NULL CHECK (revision >= 1),
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
) STRICT;

CREATE INDEX idx_source_artifacts_source_key
    ON source_artifacts(source_key);

CREATE TABLE source_package_releases (
    package_release_id          TEXT PRIMARY KEY,
    artifact_id                 TEXT NOT NULL REFERENCES source_artifacts(artifact_id),
    catalog_id                  TEXT NOT NULL,
    catalog_sequence            INTEGER NOT NULL CHECK (catalog_sequence >= 0),
    core_hash                   TEXT NOT NULL,
    scanning_extension_hash     TEXT,
    observation_contract_id     TEXT,
    account_observation_contract_id TEXT,
    account_probe_contract_id   TEXT,
    marker_schemes_json         TEXT NOT NULL,
    session_export_profile_id   TEXT,
    package_json                TEXT NOT NULL,
    state                       TEXT NOT NULL CHECK (state IN ('candidate','active','superseded','rejected')),
    revision                    INTEGER NOT NULL CHECK (revision >= 1),
    created_at                  TEXT NOT NULL,
    activated_at                TEXT,
    updated_at                  TEXT NOT NULL
) STRICT;

CREATE UNIQUE INDEX idx_source_package_one_active
    ON source_package_releases(artifact_id) WHERE state = 'active';

CREATE UNIQUE INDEX idx_source_package_one_candidate
    ON source_package_releases(artifact_id) WHERE state = 'candidate';

CREATE INDEX idx_source_package_catalog_sequence
    ON source_package_releases(artifact_id, catalog_sequence);

CREATE TABLE source_runtime_state (
    artifact_id             TEXT PRIMARY KEY REFERENCES source_artifacts(artifact_id),
    state                   TEXT NOT NULL CHECK (state IN ('healthy','degraded','quarantined','paused')),
    target_concurrency      INTEGER NOT NULL CHECK (target_concurrency >= 0),
    effective_concurrency   INTEGER NOT NULL CHECK (effective_concurrency >= 0),
    next_eligible_at        TEXT,
    failure_streak          INTEGER NOT NULL DEFAULT 0 CHECK (failure_streak >= 0),
    last_error_code         TEXT,
    revision                INTEGER NOT NULL CHECK (revision >= 1),
    updated_at              TEXT NOT NULL
) STRICT;

CREATE TABLE client_sync_state (
    client_id               TEXT PRIMARY KEY REFERENCES client_installations(client_id),
    last_ack_change_seq     INTEGER NOT NULL DEFAULT 0 CHECK (last_ack_change_seq >= 0),
    inventory_revision      INTEGER NOT NULL DEFAULT 0 CHECK (inventory_revision >= 0),
    resync_required         INTEGER NOT NULL DEFAULT 0 CHECK (resync_required IN (0,1)),
    resync_reason           TEXT,
    last_pull_at            TEXT,
    updated_at              TEXT NOT NULL
) STRICT;

CREATE TABLE client_source_inventory (
    client_id                   TEXT NOT NULL REFERENCES client_installations(client_id),
    artifact_id                 TEXT NOT NULL REFERENCES source_artifacts(artifact_id),
    package_release_id          TEXT REFERENCES source_package_releases(package_release_id),
    management_mode             TEXT NOT NULL CHECK (management_mode IN ('managed','manual','notInstalled')),
    compatibility_state         TEXT NOT NULL CHECK (compatibility_state IN ('compatible','incompatible','unknown')),
    core_hash                   TEXT,
    client_extensions_json      TEXT NOT NULL DEFAULT '{}',
    observation_contract_id     TEXT,
    account_observation_contract_id TEXT,
    account_probe_contract_id   TEXT,
    inventory_revision          INTEGER NOT NULL CHECK (inventory_revision >= 1),
    updated_at                  TEXT NOT NULL,
    PRIMARY KEY (client_id, artifact_id)
) WITHOUT ROWID, STRICT;

CREATE INDEX idx_client_source_inventory_release
    ON client_source_inventory(package_release_id, compatibility_state);

CREATE TABLE source_accounts (
    source_account_id        TEXT PRIMARY KEY,
    artifact_id              TEXT NOT NULL REFERENCES source_artifacts(artifact_id),
    state                    TEXT NOT NULL CHECK (state IN ('active','reauthRequired','disabled')),
    identity_scheme          TEXT NOT NULL,
    identity_ciphertext      BLOB,
    identity_digest          TEXT NOT NULL,
    identity_display         TEXT NOT NULL DEFAULT '',
    attributes_json          TEXT NOT NULL DEFAULT '{}',
    visibility_scope         TEXT NOT NULL,
    session_epoch            INTEGER NOT NULL CHECK (session_epoch >= 1),
    session_revision         INTEGER NOT NULL CHECK (session_revision >= 1),
    identity_verified_at     TEXT NOT NULL,
    scope_fresh_until        TEXT NOT NULL,
    last_snapshot_at         TEXT,
    last_error_code          TEXT,
    revision                 INTEGER NOT NULL CHECK (revision >= 1),
    created_at               TEXT NOT NULL,
    updated_at               TEXT NOT NULL,
    UNIQUE (artifact_id, identity_scheme, identity_digest),
    UNIQUE (source_account_id, artifact_id)
) STRICT;

CREATE INDEX idx_source_accounts_executor
    ON source_accounts(artifact_id, visibility_scope, state, scope_fresh_until);

CREATE TABLE source_account_sessions (
    source_account_id        TEXT PRIMARY KEY REFERENCES source_accounts(source_account_id),
    export_profile_id        TEXT NOT NULL,
    session_envelope         BLOB NOT NULL,
    session_digest           TEXT NOT NULL,
    session_epoch            INTEGER NOT NULL CHECK (session_epoch >= 1),
    session_revision         INTEGER NOT NULL CHECK (session_revision >= 1),
    expires_at               TEXT,
    validated_at             TEXT NOT NULL,
    updated_at               TEXT NOT NULL
) STRICT;

CREATE TABLE source_session_candidates (
    candidate_id                     TEXT PRIMARY KEY,
    client_id                        TEXT NOT NULL REFERENCES client_installations(client_id),
    artifact_id                      TEXT NOT NULL REFERENCES source_artifacts(artifact_id),
    target_source_account_id         TEXT REFERENCES source_accounts(source_account_id),
    package_release_id               TEXT NOT NULL REFERENCES source_package_releases(package_release_id),
    export_profile_id                TEXT NOT NULL,
    session_envelope                 BLOB,
    session_digest                   TEXT NOT NULL,
    probed_identity_scheme           TEXT,
    probed_identity_ciphertext       BLOB,
    probed_identity_digest           TEXT,
    probed_identity_display          TEXT,
    probed_attributes_json           TEXT,
    probed_visibility_scope          TEXT,
    state                            TEXT NOT NULL CHECK (state IN ('probing','readyLink','switchConfirmationRequired','activated','cancelled','expired','failed')),
    expected_source_account_revision INTEGER,
    failure_code                     TEXT,
    expires_at                       TEXT NOT NULL,
    revision                         INTEGER NOT NULL CHECK (revision >= 1),
    created_at                       TEXT NOT NULL,
    updated_at                       TEXT NOT NULL
) STRICT;

CREATE INDEX idx_source_session_candidates_client
    ON source_session_candidates(client_id, state, expires_at);

CREATE TABLE client_source_links (
    client_id              TEXT NOT NULL REFERENCES client_installations(client_id),
    source_account_id      TEXT NOT NULL,
    artifact_id            TEXT NOT NULL,
    state                  TEXT NOT NULL CHECK (state IN ('linked','unlinked')),
    selected_for_artifact  INTEGER NOT NULL CHECK (selected_for_artifact IN (0,1)),
    revision               INTEGER NOT NULL CHECK (revision >= 1),
    linked_at              TEXT NOT NULL,
    updated_at             TEXT NOT NULL,
    unlinked_at            TEXT,
    PRIMARY KEY (client_id, source_account_id),
    FOREIGN KEY (source_account_id, artifact_id)
        REFERENCES source_accounts(source_account_id, artifact_id)
) WITHOUT ROWID, STRICT;

CREATE UNIQUE INDEX idx_client_source_links_one_selected
    ON client_source_links(client_id, artifact_id)
    WHERE state = 'linked' AND selected_for_artifact = 1;

CREATE INDEX idx_client_source_links_account
    ON client_source_links(source_account_id, state);

CREATE TABLE cloud_mode_preparations (
    preparation_id          TEXT PRIMARY KEY,
    client_id               TEXT NOT NULL REFERENCES client_installations(client_id),
    source_account_id       TEXT NOT NULL,
    artifact_id             TEXT NOT NULL,
    package_release_id      TEXT NOT NULL REFERENCES source_package_releases(package_release_id),
    state                   TEXT NOT NULL CHECK (state IN ('preparing','snapshotReady','committed','cancelled','expired','failed')),
    stage                   TEXT NOT NULL CHECK (stage IN ('validateAccount','validateInventory','waitSnapshot','deliverSnapshot','done')),
    fixed_session_epoch     INTEGER NOT NULL CHECK (fixed_session_epoch >= 1),
    fixed_inventory_revision INTEGER NOT NULL CHECK (fixed_inventory_revision >= 1),
    blocked_reason          TEXT,
    next_evaluation_at      TEXT,
    snapshot_receipt_id     TEXT,
    expires_at              TEXT NOT NULL,
    revision                INTEGER NOT NULL CHECK (revision >= 1),
    created_at              TEXT NOT NULL,
    updated_at              TEXT NOT NULL,
    FOREIGN KEY (source_account_id, artifact_id)
        REFERENCES source_accounts(source_account_id, artifact_id)
) STRICT;

CREATE INDEX idx_cloud_mode_preparations_client
    ON cloud_mode_preparations(client_id, state, expires_at);

CREATE TABLE client_cloud_claims (
    client_id              TEXT NOT NULL REFERENCES client_installations(client_id),
    source_account_id      TEXT NOT NULL,
    artifact_id            TEXT NOT NULL,
    preparation_id         TEXT REFERENCES cloud_mode_preparations(preparation_id),
    state                  TEXT NOT NULL CHECK (state IN ('active','suspended')),
    suspended_reason       TEXT,
    revision               INTEGER NOT NULL CHECK (revision >= 1),
    activated_at           TEXT NOT NULL,
    updated_at             TEXT NOT NULL,
    PRIMARY KEY (client_id, source_account_id),
    FOREIGN KEY (source_account_id, artifact_id)
        REFERENCES source_accounts(source_account_id, artifact_id)
) WITHOUT ROWID, STRICT;

CREATE INDEX idx_client_cloud_claims_account
    ON client_cloud_claims(source_account_id, state);

CREATE TABLE favorite_snapshot_runs (
    snapshot_run_id         TEXT PRIMARY KEY,
    source_account_id       TEXT NOT NULL REFERENCES source_accounts(source_account_id),
    package_release_id      TEXT NOT NULL REFERENCES source_package_releases(package_release_id),
    session_epoch           INTEGER NOT NULL CHECK (session_epoch >= 1),
    run_generation          INTEGER NOT NULL CHECK (run_generation >= 1),
    state                   TEXT NOT NULL CHECK (state IN ('running','complete','publishing','published','failed','abandoned')),
    checkpoint_envelope     BLOB,
    snapshot_validation_revision INTEGER NOT NULL DEFAULT 0 CHECK (snapshot_validation_revision >= 0),
    item_count              INTEGER NOT NULL DEFAULT 0 CHECK (item_count >= 0),
    started_at              TEXT NOT NULL,
    updated_at              TEXT NOT NULL,
    completed_at            TEXT,
    failure_code            TEXT,
    UNIQUE (source_account_id, run_generation)
) STRICT;

CREATE INDEX idx_favorite_snapshot_runs_active
    ON favorite_snapshot_runs(source_account_id, state, updated_at);

CREATE TABLE snapshot_staging_items (
    snapshot_run_id         TEXT NOT NULL REFERENCES favorite_snapshot_runs(snapshot_run_id) ON DELETE CASCADE,
    comic_id                TEXT NOT NULL,
    item_digest             TEXT NOT NULL,
    item_json               TEXT NOT NULL,
    PRIMARY KEY (snapshot_run_id, comic_id)
) WITHOUT ROWID, STRICT;

CREATE TABLE tracking_interests (
    tracking_interest_id    TEXT PRIMARY KEY,
    artifact_id             TEXT NOT NULL REFERENCES source_artifacts(artifact_id),
    comic_id                TEXT NOT NULL,
    source_account_id       TEXT REFERENCES source_accounts(source_account_id),
    origin_kind             TEXT NOT NULL CHECK (origin_kind IN ('remoteSnapshot','localUpload','adminRepair')),
    origin_key              TEXT NOT NULL,
    visibility_scope        TEXT NOT NULL,
    variant_key             TEXT NOT NULL DEFAULT '',
    state                   TEXT NOT NULL CHECK (state IN ('active','inactive')),
    title                   TEXT,
    cover_url               TEXT,
    revision                INTEGER NOT NULL CHECK (revision >= 1),
    created_at              TEXT NOT NULL,
    updated_at              TEXT NOT NULL,
    UNIQUE (artifact_id, comic_id, origin_kind, origin_key, variant_key),
    CHECK (origin_kind <> 'remoteSnapshot' OR source_account_id IS NOT NULL)
) STRICT;

CREATE INDEX idx_tracking_interests_detail_demand
    ON tracking_interests(artifact_id, comic_id, visibility_scope, variant_key, state);

CREATE INDEX idx_tracking_interests_account
    ON tracking_interests(source_account_id, state);

CREATE TABLE content_observations (
    content_observation_id       TEXT PRIMARY KEY,
    artifact_id                 TEXT NOT NULL REFERENCES source_artifacts(artifact_id),
    comic_id                    TEXT NOT NULL,
    visibility_scope            TEXT NOT NULL,
    variant_key                 TEXT NOT NULL DEFAULT '',
    observation_contract_id     TEXT NOT NULL,
    package_release_id          TEXT NOT NULL REFERENCES source_package_releases(package_release_id),
    marker_scheme               TEXT,
    payload_digest              TEXT NOT NULL,
    payload_json                TEXT NOT NULL,
    status                      TEXT NOT NULL CHECK (status IN ('fresh','stale','suspect','notFoundConfirmed')),
    observation_revision        INTEGER NOT NULL CHECK (observation_revision >= 1),
    validation_revision         INTEGER NOT NULL CHECK (validation_revision >= 1),
    observed_at                 TEXT NOT NULL,
    validated_at                TEXT NOT NULL,
    fresh_until                 TEXT NOT NULL,
    updated_at                  TEXT NOT NULL,
    UNIQUE (artifact_id, comic_id, visibility_scope, variant_key, observation_contract_id)
) STRICT;

CREATE INDEX idx_content_observations_freshness
    ON content_observations(artifact_id, visibility_scope, fresh_until);

CREATE TABLE account_observations (
    account_observation_id       TEXT PRIMARY KEY,
    source_account_id            TEXT NOT NULL REFERENCES source_accounts(source_account_id),
    comic_id                     TEXT NOT NULL,
    account_observation_contract_id TEXT NOT NULL,
    package_release_id           TEXT NOT NULL REFERENCES source_package_releases(package_release_id),
    payload_digest               TEXT NOT NULL,
    payload_json                 TEXT NOT NULL,
    status                       TEXT NOT NULL CHECK (status IN ('fresh','stale')),
    observation_revision         INTEGER NOT NULL CHECK (observation_revision >= 1),
    validation_revision          INTEGER NOT NULL CHECK (validation_revision >= 1),
    observed_at                  TEXT NOT NULL,
    validated_at                 TEXT NOT NULL,
    fresh_until                  TEXT NOT NULL,
    updated_at                  TEXT NOT NULL,
    UNIQUE (source_account_id, comic_id, account_observation_contract_id)
) STRICT;

CREATE INDEX idx_account_observations_freshness
    ON account_observations(source_account_id, fresh_until);

CREATE TABLE source_scan_status (
    source_account_id       TEXT PRIMARY KEY REFERENCES source_accounts(source_account_id),
    status                  TEXT NOT NULL CHECK (status IN ('idle','scheduled','scanning','blocked','reauthRequired','paused')),
    blocked_reason          TEXT,
    last_success_at         TEXT,
    last_attempt_at         TEXT,
    next_evaluation_at      TEXT,
    revision                INTEGER NOT NULL CHECK (revision >= 1),
    updated_at              TEXT NOT NULL
) STRICT;

CREATE TABLE scan_demands (
    demand_id                   TEXT PRIMARY KEY,
    demand_key                  TEXT NOT NULL UNIQUE,
    artifact_id                 TEXT NOT NULL REFERENCES source_artifacts(artifact_id),
    demand_kind                 TEXT NOT NULL CHECK (demand_kind IN ('accountSnapshot','comicDetail','maintenance')),
    source_account_id           TEXT REFERENCES source_accounts(source_account_id),
    comic_id                    TEXT,
    visibility_scope            TEXT,
    variant_key                 TEXT NOT NULL DEFAULT '',
    observation_contract_id     TEXT,
    state                       TEXT NOT NULL CHECK (state IN ('active','blocked','inactive')),
    due_at                      TEXT NOT NULL,
    oldest_at                   TEXT NOT NULL,
    next_eligible_at            TEXT NOT NULL,
    execution_generation        INTEGER NOT NULL CHECK (execution_generation >= 0),
    priority_class              TEXT NOT NULL CHECK (priority_class IN ('normal','expedited')),
    last_error_code             TEXT,
    revision                    INTEGER NOT NULL CHECK (revision >= 1),
    created_at                  TEXT NOT NULL,
    updated_at                  TEXT NOT NULL,
    CHECK (
      (demand_kind = 'accountSnapshot' AND source_account_id IS NOT NULL AND comic_id IS NULL)
      OR (demand_kind = 'comicDetail' AND source_account_id IS NULL AND comic_id IS NOT NULL AND visibility_scope IS NOT NULL AND observation_contract_id IS NOT NULL)
      OR demand_kind = 'maintenance'
    )
) STRICT;

CREATE INDEX idx_scan_demands_due
    ON scan_demands(artifact_id, state, next_eligible_at, oldest_at);

CREATE TABLE scan_jobs (
    job_id                      TEXT PRIMARY KEY,
    demand_id                   TEXT NOT NULL REFERENCES scan_demands(demand_id),
    artifact_id                 TEXT NOT NULL REFERENCES source_artifacts(artifact_id),
    job_kind                    TEXT NOT NULL CHECK (job_kind IN ('accountSnapshotSlice','comicDetailBatch','maintenance')),
    execution_generation       INTEGER NOT NULL CHECK (execution_generation >= 1),
    executor_source_account_id TEXT NOT NULL REFERENCES source_accounts(source_account_id),
    package_release_id         TEXT NOT NULL REFERENCES source_package_releases(package_release_id),
    fixed_session_epoch        INTEGER NOT NULL CHECK (fixed_session_epoch >= 1),
    state                       TEXT NOT NULL CHECK (state IN ('ready','leased','succeeded','retryable','failed','cancelled')),
    lease_owner                 TEXT,
    lease_expires_at            TEXT,
    attempt_count               INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_eligible_at            TEXT NOT NULL,
    payload_json                TEXT NOT NULL,
    result_digest               TEXT,
    created_at                  TEXT NOT NULL,
    updated_at                  TEXT NOT NULL,
    UNIQUE (demand_id, execution_generation)
) STRICT;

CREATE INDEX idx_scan_jobs_ready
    ON scan_jobs(artifact_id, state, next_eligible_at, lease_expires_at);

CREATE TABLE scan_attempts (
    scan_attempt_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id                      TEXT NOT NULL REFERENCES scan_jobs(job_id),
    attempt_no                  INTEGER NOT NULL CHECK (attempt_no >= 1),
    executor_source_account_id  TEXT NOT NULL REFERENCES source_accounts(source_account_id),
    worker_id                   TEXT NOT NULL,
    started_at                  TEXT NOT NULL,
    finished_at                 TEXT,
    outcome                     TEXT CHECK (outcome IN ('succeeded','retryable','failed','leaseLost','discarded')),
    error_code                  TEXT,
    metrics_json                TEXT NOT NULL DEFAULT '{}',
    UNIQUE (job_id, attempt_no)
) STRICT;

CREATE INDEX idx_scan_attempts_started
    ON scan_attempts(started_at);

CREATE TABLE client_entity_states (
    client_entity_state_id  TEXT PRIMARY KEY,
    client_id               TEXT NOT NULL REFERENCES client_installations(client_id),
    entity_type             TEXT NOT NULL CHECK (entity_type IN ('sourceAccount','trackingInterest','contentObservation','accountObservation','sourceStatus','clientCloudState')),
    entity_key              TEXT NOT NULL,
    key_hash                TEXT NOT NULL,
    operation               TEXT NOT NULL CHECK (operation IN ('upsert','delete')),
    entity_revision         INTEGER NOT NULL CHECK (entity_revision >= 1),
    source_account_id       TEXT REFERENCES source_accounts(source_account_id),
    artifact_id             TEXT NOT NULL,
    updated_at              TEXT NOT NULL,
    UNIQUE (client_id, entity_type, key_hash)
) STRICT;

CREATE INDEX idx_client_entity_states_snapshot
    ON client_entity_states(client_id, artifact_id, entity_type, client_entity_state_id);

CREATE TABLE client_changes (
    change_id               INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id               TEXT NOT NULL REFERENCES client_installations(client_id),
    client_change_seq       INTEGER NOT NULL CHECK (client_change_seq >= 1),
    client_entity_state_id  TEXT NOT NULL REFERENCES client_entity_states(client_entity_state_id),
    entity_revision         INTEGER NOT NULL CHECK (entity_revision >= 1),
    created_at              TEXT NOT NULL,
    UNIQUE (client_id, client_change_seq)
) STRICT;

CREATE INDEX idx_client_changes_pull
    ON client_changes(client_id, client_change_seq);

CREATE TABLE client_change_watermarks (
    client_id               TEXT PRIMARY KEY REFERENCES client_installations(client_id),
    low_change_seq          INTEGER NOT NULL DEFAULT 0 CHECK (low_change_seq >= 0),
    high_change_seq         INTEGER NOT NULL DEFAULT 0 CHECK (high_change_seq >= low_change_seq),
    updated_at              TEXT NOT NULL
) STRICT;

CREATE TABLE snapshot_receipts (
    snapshot_receipt_id     TEXT PRIMARY KEY,
    client_id               TEXT NOT NULL REFERENCES client_installations(client_id),
    source_account_id       TEXT REFERENCES source_accounts(source_account_id),
    artifact_id             TEXT,
    scope_kind              TEXT NOT NULL CHECK (scope_kind IN ('source','full')),
    package_release_id      TEXT REFERENCES source_package_releases(package_release_id),
    fixed_session_epoch     INTEGER,
    base_change_seq         INTEGER NOT NULL CHECK (base_change_seq >= 0),
    snapshot_digest         TEXT NOT NULL,
    state                   TEXT NOT NULL CHECK (state IN ('issued','committed','expired')),
    commit_expires_at       TEXT NOT NULL,
    created_at              TEXT NOT NULL,
    committed_at            TEXT,
    CHECK ((scope_kind = 'source' AND source_account_id IS NOT NULL AND artifact_id IS NOT NULL)
        OR scope_kind = 'full')
) STRICT;

CREATE INDEX idx_snapshot_receipts_expiry
    ON snapshot_receipts(client_id, state, commit_expires_at);

CREATE TABLE refresh_signals (
    refresh_signal_id       TEXT PRIMARY KEY,
    client_id               TEXT NOT NULL REFERENCES client_installations(client_id),
    source_account_id       TEXT NOT NULL REFERENCES source_accounts(source_account_id),
    artifact_id             TEXT NOT NULL,
    window_key              TEXT NOT NULL,
    state                   TEXT NOT NULL CHECK (state IN ('accepted','coalesced','notNeeded','blocked')),
    request_count           INTEGER NOT NULL DEFAULT 1 CHECK (request_count >= 1),
    first_requested_at      TEXT NOT NULL,
    last_requested_at       TEXT NOT NULL,
    next_evaluation_at      TEXT,
    blocked_reason          TEXT,
    UNIQUE (source_account_id, window_key)
) STRICT;

CREATE INDEX idx_refresh_signals_account
    ON refresh_signals(source_account_id, last_requested_at);
