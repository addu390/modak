-- TierDB catalog schema, the cross-language contract between the Rust
-- extension and the Java workers. Columns stay executor-portable
-- (ints/text/jsonb) because pg_duckdb may push reads down to DuckDB.

CREATE SCHEMA IF NOT EXISTS tierdb;

-- Named warehouse bindings, credential_ref points at TIERDB_CREDENTIALS_<REF>.
CREATE TABLE IF NOT EXISTS tierdb.storage_profiles (
    profile_name        text        PRIMARY KEY,
    lake_format         text,
    warehouse           text        NOT NULL,
    lake_config         jsonb,
    credential_ref      text,
    is_default          boolean     NOT NULL DEFAULT false,
    created_at          timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS tierdb_storage_profiles_default_idx
    ON tierdb.storage_profiles (is_default) WHERE is_default;
INSERT INTO tierdb.storage_profiles (profile_name, warehouse, is_default)
SELECT 'default', '', true
 WHERE NOT EXISTS (SELECT 1 FROM tierdb.storage_profiles WHERE is_default);

-- Registered logical tables.
CREATE TABLE IF NOT EXISTS tierdb.tables (
    table_id            bigint       PRIMARY KEY,
    schema_name         text        NOT NULL,
    table_name          text        NOT NULL,
    primary_key_cols    text[]      NOT NULL,
    tier_key_col        text        NOT NULL,
    tier_key_type       text        NOT NULL DEFAULT 'bigint',
    partition_scheme    jsonb       NOT NULL,
    lake_format         text        NOT NULL,
    lake_table_ref      text        NOT NULL,
    storage_profile     text        NOT NULL DEFAULT 'default'
                                    REFERENCES tierdb.storage_profiles(profile_name),
    mode                text        NOT NULL DEFAULT 'tiered'
                                    CHECK (mode IN ('tiered','mirrored')),
    publication_name    text,
    slot_name           text,
    heap_retention_lag  bigint,
    lake_retention_lag  bigint,
    keep_heap           boolean     NOT NULL DEFAULT false,
    maintenance_policy  jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (schema_name, table_name),
    CHECK (mode = 'mirrored'
           OR (publication_name IS NULL AND slot_name IS NULL AND heap_retention_lag IS NULL)),
    CHECK (mode = 'tiered' OR (lake_retention_lag IS NULL AND NOT keep_heap)),
    CHECK (NOT keep_heap OR lake_retention_lag IS NULL)
);

-- The cut-line: per table, current T + pinned S, always advanced together.
CREATE TABLE IF NOT EXISTS tierdb.cutline (
    table_id            bigint       PRIMARY KEY REFERENCES tierdb.tables(table_id) ON DELETE CASCADE,
    tier_key_hi         bigint      NOT NULL,
    lake_snapshot_id    bigint      NOT NULL,
    replicated_lsn      bigint,
    retention_line      bigint,
    lake_props          jsonb,
    updated_at          timestamptz NOT NULL DEFAULT now()
);

-- Partition lifecycle map.
CREATE TABLE IF NOT EXISTS tierdb.partitions (
    table_id            bigint       NOT NULL REFERENCES tierdb.tables(table_id) ON DELETE CASCADE,
    partition_id        text        NOT NULL,
    tier_key_lo         bigint      NOT NULL,
    tier_key_hi         bigint      NOT NULL,
    state               text        NOT NULL
                                    CHECK (state IN ('hot','sealing','tiering','tiered','dropped')),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (table_id, partition_id)
);

-- Cold overlay: correction/late-write buffer for cold rows, merged on read.
CREATE TABLE IF NOT EXISTS tierdb.delta (
    table_id            bigint       NOT NULL REFERENCES tierdb.tables(table_id) ON DELETE CASCADE,
    pk                  text        NOT NULL,
    op                  smallint    NOT NULL,
    tier_key            bigint      NOT NULL,
    old_tier_key        bigint,
    version             bigint      NOT NULL,
    payload             jsonb,
    updated_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (table_id, pk)
);
CREATE INDEX IF NOT EXISTS tierdb_delta_tier_key_idx ON tierdb.delta (table_id, tier_key);

CREATE SEQUENCE IF NOT EXISTS tierdb.delta_version;

-- Active read pins. The oldest pin is the reclaim horizon.
CREATE TABLE IF NOT EXISTS tierdb.read_pins (
    pin_id                  bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    table_id                bigint       NOT NULL REFERENCES tierdb.tables(table_id) ON DELETE CASCADE,
    pinned_lake_snapshot_id bigint      NOT NULL,
    pinned_tier_key_hi      bigint      NOT NULL,
    started_at              timestamptz NOT NULL DEFAULT now(),
    expires_at              timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS tierdb_read_pins_horizon_idx ON tierdb.read_pins (table_id, pinned_lake_snapshot_id);

-- Idempotency + crash-resume journal for every lake-writing operation.
CREATE TABLE IF NOT EXISTS tierdb.op_log (
    op_id               uuid        PRIMARY KEY,
    table_id            bigint       NOT NULL REFERENCES tierdb.tables(table_id) ON DELETE CASCADE,
    op_kind             text        NOT NULL CHECK (op_kind IN ('tiering','compaction','maintenance','retention','ingest','load')),
    phase               text        NOT NULL,
    lake_snapshot_id    bigint,
    details             jsonb,
    updated_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS tierdb_op_log_table_idx ON tierdb.op_log (table_id, updated_at);

-- Stream Load label ledger.
CREATE TABLE IF NOT EXISTS tierdb.load_labels (
    table_id            bigint      NOT NULL REFERENCES tierdb.tables(table_id) ON DELETE CASCADE,
    label               text        NOT NULL,
    state               text        NOT NULL
                                    CHECK (state IN ('staged','committed','failed')),
    staged_files        jsonb,
    result              jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (table_id, label)
);
CREATE INDEX IF NOT EXISTS tierdb_load_labels_state_idx ON tierdb.load_labels (table_id, state);

-- In-flight initial copies (mirrored registration), resumable from last_pk.
CREATE TABLE IF NOT EXISTS tierdb.copy_progress (
    table_id            bigint       PRIMARY KEY REFERENCES tierdb.tables(table_id) ON DELETE CASCADE,
    consistent_point    bigint      NOT NULL,
    last_pk             jsonb,
    chunks_done         bigint      NOT NULL DEFAULT 0,
    updated_at          timestamptz NOT NULL DEFAULT now()
);

-- Lake health per table, counters and warnings are owned by the format plugin.
CREATE TABLE IF NOT EXISTS tierdb.lake_stats (
    table_id            bigint      PRIMARY KEY REFERENCES tierdb.tables(table_id) ON DELETE CASCADE,
    stats               jsonb       NOT NULL,
    warnings            jsonb       NOT NULL DEFAULT '[]'::jsonb,
    policy              jsonb,
    collected_at        timestamptz NOT NULL DEFAULT now()
);

-- Pending out-of-schedule maintenance triggers, claimed with DELETE RETURNING.
CREATE TABLE IF NOT EXISTS tierdb.maintenance_requests (
    table_id            bigint      PRIMARY KEY REFERENCES tierdb.tables(table_id) ON DELETE CASCADE,
    requested_at        timestamptz NOT NULL DEFAULT now(),
    requested_by        text        NOT NULL
);

-- One-row-per-table operational snapshot for humans and dashboards.
DROP VIEW IF EXISTS tierdb.status;
CREATE VIEW tierdb.status AS
SELECT t.table_id,
       t.schema_name,
       t.table_name,
       t.mode,
       c.tier_key_hi                      AS cutline_t,
       c.lake_snapshot_id                 AS cutline_s,
       c.replicated_lsn                   AS mirror_frontier,
       c.retention_line                   AS retention_line,
       c.updated_at                       AS cutline_updated_at,
       (SELECT count(*) FROM tierdb.delta d
         WHERE d.table_id = t.table_id)   AS delta_backlog,
       (SELECT count(*) FROM tierdb.read_pins p
         WHERE p.table_id = t.table_id)   AS read_pins,
       (SELECT count(*) FROM tierdb.load_labels l
         WHERE l.table_id = t.table_id
           AND l.state = 'staged')        AS staged_loads,
       EXISTS (SELECT 1 FROM tierdb.copy_progress cp
         WHERE cp.table_id = t.table_id)  AS copying,
       (SELECT jsonb_object_agg(s.state, s.n)
          FROM (SELECT state, count(*) AS n
                  FROM tierdb.partitions p
                 WHERE p.table_id = t.table_id
                 GROUP BY state) s)       AS partition_states
  FROM tierdb.tables t
  LEFT JOIN tierdb.cutline c USING (table_id);

CREATE TABLE IF NOT EXISTS tierdb.schema_meta (
    version             int         NOT NULL,
    applied_at          timestamptz NOT NULL DEFAULT now()
);
INSERT INTO tierdb.schema_meta (version)
SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM tierdb.schema_meta);
UPDATE tierdb.schema_meta SET version = greatest(version, 1), applied_at = now();
