-- Modak catalog schema, the cross-language contract.
-- These tables are the ONLY coordination channel between the Rust extension
-- and the Java workers, every atomic handoff is a plain Postgres transaction.
-- PORTABILITY: pg_duckdb may push read-path queries down to DuckDB, so every
-- column must stay executor-portable (plain ints/text/jsonb), which is why
-- table_id is bigint (not oid) and delta.pk is text (not bytea).

CREATE SCHEMA IF NOT EXISTS modak;

-- Registered logical tables.
-- Writer: Java (admin)   Reader: Rust + Java
CREATE TABLE IF NOT EXISTS modak.tables (
    table_id            bigint       PRIMARY KEY,          -- user table's OID
    schema_name         text        NOT NULL,
    table_name          text        NOT NULL,
    primary_key_cols    text[]      NOT NULL,             -- merge key
    tier_key_col        text        NOT NULL,             -- the aging key rows tier by
    partition_scheme    jsonb       NOT NULL,             -- e.g. {"unit":"hour"}
    lake_format         text        NOT NULL,             -- LakeStoragePlugin id
    lake_table_ref      text        NOT NULL,
    lake_props          jsonb,                            -- opaque per-format config
    schema_version      int         NOT NULL DEFAULT 1,
    -- 'tiered': data moves (recent in PG, old in the lake).
    -- 'mirrored': CDC keeps a full lake copy. PG keeps everything unless
    -- heap_retention_lag says otherwise.
    mode                text        NOT NULL DEFAULT 'tiered'
                                    CHECK (mode IN ('tiered','mirrored')),
    publication_name    text,                             -- mirrored: logical publication
    slot_name           text,                             -- mirrored: replication slot
    heap_retention_lag  bigint,                           -- mirrored: drop heap partitions
                                                          -- this far behind highwater, NULL = keep all
    lake_retention_lag  bigint,                           -- tiered: expire lake rows this far
                                                          -- behind the cut-line, NULL = keep forever
    created_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (schema_name, table_name)
);

-- The cut-line: per table, current T + pinned S, always advanced together.
-- Writer: Java             Reader: Rust
CREATE TABLE IF NOT EXISTS modak.cutline (
    table_id            bigint       PRIMARY KEY REFERENCES modak.tables(table_id) ON DELETE CASCADE,
    tier_key_hi         bigint      NOT NULL,             -- T: rows with tier_key >= T live in Postgres
    lake_snapshot_id    bigint      NOT NULL,             -- S: pinned cold-store version consistent with T
    replicated_lsn      bigint,                           -- F: mirror frontier (WAL pos as bigint),
                                                          -- NULL for tiered tables
    retention_line      bigint,                           -- R: lake rows with tier_key < R are
                                                          -- expired, NULL = nothing expired yet
    updated_at          timestamptz NOT NULL DEFAULT now()
);

-- Partition lifecycle map.
-- Writer: Java             Reader: Rust + Java
CREATE TABLE IF NOT EXISTS modak.partitions (
    table_id            bigint       NOT NULL REFERENCES modak.tables(table_id) ON DELETE CASCADE,
    partition_id        text        NOT NULL,
    tier_key_lo         bigint      NOT NULL,             -- [lo, hi) tier-key bounds
    tier_key_hi         bigint      NOT NULL,
    state               text        NOT NULL
                                    CHECK (state IN ('hot','sealing','tiering','tiered','dropped')),
    lake_files          jsonb,                            -- opaque data/manifest refs
    updated_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (table_id, partition_id)
);

-- Cold overlay: correction/late-write buffer for cold rows, merged on read.
-- Writer: Rust/PG         Reader: Rust + Java
CREATE TABLE IF NOT EXISTS modak.delta (
    table_id            bigint       NOT NULL REFERENCES modak.tables(table_id) ON DELETE CASCADE,
    pk                  text        NOT NULL,             -- canonical PK: single col = raw ::text,
                                                          -- composite = parts (\ and 0x1F escaped
                                                          -- with \) joined on chr(31)
    op                  smallint    NOT NULL,             -- 0 = upsert, 1 = tombstone
    tier_key            bigint      NOT NULL,             -- of the target cold row (< T)
    old_tier_key        bigint,                           -- where the lake still holds the row
                                                          -- when a SET moved it across tiers,
                                                          -- the fold deletes there instead
    version             bigint      NOT NULL,             -- newest-wins ordering
    payload             jsonb,                            -- row image, tombstones keep the pk fields
    updated_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (table_id, pk)
);
CREATE INDEX IF NOT EXISTS modak_delta_tier_key_idx ON modak.delta (table_id, tier_key);

-- Assignment-ordered versions for delta entries (newest-wins merge + fold clear guard).
CREATE SEQUENCE IF NOT EXISTS modak.delta_version;

-- Active read pins. The oldest pin is the reclaim horizon.
-- Writer: Rust             Reader: Java
CREATE TABLE IF NOT EXISTS modak.read_pins (
    pin_id                  bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    table_id                bigint       NOT NULL REFERENCES modak.tables(table_id) ON DELETE CASCADE,
    pinned_lake_snapshot_id bigint      NOT NULL,
    pinned_tier_key_hi      bigint      NOT NULL,
    started_at              timestamptz NOT NULL DEFAULT now(),
    expires_at              timestamptz NOT NULL          -- bounds a stuck query
);
CREATE INDEX IF NOT EXISTS modak_read_pins_horizon_idx ON modak.read_pins (table_id, pinned_lake_snapshot_id);

-- Idempotency + crash-resume log for tiering and compaction operations.
-- Writer: Java             Reader: Java
CREATE TABLE IF NOT EXISTS modak.tiering_log (
    op_id               uuid        PRIMARY KEY,
    table_id            bigint       NOT NULL REFERENCES modak.tables(table_id) ON DELETE CASCADE,
    op_kind             text        NOT NULL CHECK (op_kind IN ('tiering','compaction','maintenance','retention','ingest')),
    phase               text        NOT NULL,             -- flushing | committed | advanced | abandoned
    lake_snapshot_id    bigint,
    details             jsonb,
    updated_at          timestamptz NOT NULL DEFAULT now()
);

-- In-flight initial copies (mirrored registration). The row exists only while
-- the chunked copy runs, a re-run of register resumes from last_pk.
-- Writer: Java (admin)     Reader: Java
CREATE TABLE IF NOT EXISTS modak.copy_progress (
    table_id            bigint       PRIMARY KEY REFERENCES modak.tables(table_id) ON DELETE CASCADE,
    consistent_point    bigint      NOT NULL,             -- slot LSN streaming starts from
    last_pk             jsonb,                            -- pk of the last copied row (PG text forms)
    chunks_done         bigint      NOT NULL DEFAULT 0,
    updated_at          timestamptz NOT NULL DEFAULT now()
);

-- One-row-per-table operational snapshot for humans and dashboards.
-- DROP + CREATE (not OR REPLACE): re-applies cleanly when columns change.
DROP VIEW IF EXISTS modak.status;
CREATE VIEW modak.status AS
SELECT t.table_id,
       t.schema_name,
       t.table_name,
       t.mode,
       c.tier_key_hi                      AS cutline_t,
       c.lake_snapshot_id                 AS cutline_s,
       c.replicated_lsn                   AS mirror_frontier,
       c.retention_line                   AS retention_line,
       c.updated_at                       AS cutline_updated_at,
       (SELECT count(*) FROM modak.delta d
         WHERE d.table_id = t.table_id)   AS delta_backlog,
       (SELECT count(*) FROM modak.read_pins p
         WHERE p.table_id = t.table_id)   AS read_pins,
       EXISTS (SELECT 1 FROM modak.copy_progress cp
         WHERE cp.table_id = t.table_id)  AS copying,
       (SELECT jsonb_object_agg(s.state, s.n)
          FROM (SELECT state, count(*) AS n
                  FROM modak.partitions p
                 WHERE p.table_id = t.table_id
                 GROUP BY state) s)       AS partition_states
  FROM modak.tables t
  LEFT JOIN modak.cutline c USING (table_id);

-- Version stamp. This file is always the LATEST schema, databases created from
-- older versions upgrade through sql/migrations/V*.sql (run by the worker).
-- Bump the version here AND add a migration whenever the schema changes.
CREATE TABLE IF NOT EXISTS modak.schema_meta (
    version             int         NOT NULL,
    applied_at          timestamptz NOT NULL DEFAULT now()
);
INSERT INTO modak.schema_meta (version)
SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM modak.schema_meta);
UPDATE modak.schema_meta SET version = greatest(version, 1), applied_at = now();
