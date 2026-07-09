//! The TierDB Postgres extension, built with `pgrx`. A thin set of adapters
//! over the pure [`tierdb_core`] domain. Coordination with the Java workers is
//! entirely through the `tierdb` catalog tables, there is no cross-language RPC.

use pgrx::prelude::*;

::pgrx::pg_module_magic!();

pub mod catalog;
pub mod cold_mirror;
pub mod delta;
pub mod dml;
pub mod dml_rewrite;
mod embedded;
pub mod explain;
pub mod hook;
pub mod lake;
pub mod pin;
pub mod planner;
pub mod router;
mod threads;

#[pg_guard]
extern "C-unwind" fn _PG_init() {
    unsafe {
        hook::init();
        dml::init();
        embedded::init();
    }
}

#[pg_extern]
fn tierdb_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;
    use tierdb_core::domain::{Cutline, DeltaOp, KeyRange, LakeSnapshotId, Pk, TableId, TierKey};
    use tierdb_core::ports::{CutlineReader, DeltaReader, ReadPinRepository};
    use tierdb_core::TierDBError;

    use crate::catalog::PgCatalog;
    use crate::pin::PgReadPins;

    const CATALOG_DDL: &str = include_str!("../../../../sql/catalog.sql");

    const TABLE_A: TableId = TableId(90_001);

    fn seed_catalog() {
        Spi::run(CATALOG_DDL).expect("catalog.sql applies");
        Spi::run(
            "INSERT INTO tierdb.tables (table_id, schema_name, table_name, primary_key_cols,
                                       tier_key_col, partition_scheme, lake_format, lake_table_ref)
             VALUES (90001, 'public', 'events', ARRAY['id'], 'event_time',
                     '{\"unit\":\"hour\"}', 'iceberg', 'warehouse.events')",
        )
        .expect("register table");
        Spi::run("INSERT INTO tierdb.cutline (table_id, tier_key_hi, lake_snapshot_id) VALUES (90001, 100, 7)")
            .expect("init cutline");
    }

    #[pg_test]
    fn test_tierdb_version() {
        assert_eq!(crate::tierdb_version(), env!("CARGO_PKG_VERSION"));
    }

    #[pg_test]
    fn test_cutline_reads_atomic_pair() {
        seed_catalog();
        let cut = PgCatalog.current(TABLE_A).expect("cutline exists");
        assert_eq!(
            cut,
            Cutline {
                t: TierKey(100),
                snapshot: LakeSnapshotId(7)
            }
        );
    }

    #[pg_test]
    fn test_cutline_unknown_table_is_a_domain_error() {
        seed_catalog();
        match PgCatalog.current(TableId(1)) {
            Err(TierDBError::UnknownTable(t)) => assert_eq!(t, TableId(1)),
            other => panic!("expected UnknownTable, got {other:?}"),
        }
    }

    #[pg_test]
    fn test_delta_overlay_reads_ops_and_prunes_by_range() {
        seed_catalog();
        Spi::run(
            "INSERT INTO tierdb.delta (table_id, pk, op, tier_key, version, payload) VALUES
               (90001, '1', 0, 10, 1, '{\"v\":1}'),
               (90001, '2', 1, 50, 2, NULL),
               (90001, '3', 0, 99, 3, '{\"v\":3}')",
        )
        .expect("seed delta");

        let all = PgCatalog
            .overlay(TABLE_A, KeyRange::UNBOUNDED)
            .expect("overlay");
        assert_eq!(all.entries.len(), 3);
        assert_eq!(all.entries[0].pk, Pk("1".into()));
        assert_eq!(all.entries[0].op, DeltaOp::Upsert);
        assert_eq!(all.entries[1].op, DeltaOp::Tombstone);
        assert_eq!(all.entries[2].tier_key, TierKey(99));

        let mid = PgCatalog
            .overlay(
                TABLE_A,
                KeyRange {
                    lo: Some(TierKey(10)),
                    hi: Some(TierKey(99)),
                },
            )
            .expect("overlay range");
        assert_eq!(
            mid.entries.iter().map(|e| e.tier_key.0).collect::<Vec<_>>(),
            vec![10, 50],
        );
    }

    #[pg_test]
    fn test_delta_overlay_empty_when_no_corrections() {
        seed_catalog();
        let none = PgCatalog
            .overlay(TABLE_A, KeyRange::UNBOUNDED)
            .expect("overlay");
        assert!(
            none.is_empty(),
            "no corrections means an empty overlay (the common case)"
        );
    }

    fn seed_registered_table() -> pg_sys::Oid {
        Spi::run("SET tierdb.transparent_reads = off").expect("guc");
        Spi::run(CATALOG_DDL).expect("catalog.sql applies");
        Spi::run(
            "CREATE TABLE public.events (id bigint PRIMARY KEY, event_time bigint NOT NULL, val text)",
        )
        .expect("hot relation");
        Spi::run(
            "INSERT INTO tierdb.tables (table_id, schema_name, table_name, primary_key_cols,
                                       tier_key_col, partition_scheme, lake_format, lake_table_ref)
             SELECT 'public.events'::regclass::oid::bigint, 'public', 'events', ARRAY['id'], 'event_time',
                    '{\"unit\":\"hour\"}', 'iceberg', 'warehouse.events'",
        )
        .expect("register");
        Spi::run(
            "INSERT INTO tierdb.cutline (table_id, tier_key_hi, lake_snapshot_id, lake_props)
             SELECT 'public.events'::regclass::oid::bigint, 100, 7,
                    '{\"metadata_location\":\"/wh/events/metadata/00002-abc.metadata.json\"}'",
        )
        .expect("cutline");
        Spi::get_one::<pg_sys::Oid>("SELECT 'public.events'::regclass::oid")
            .expect("oid")
            .unwrap()
    }

    #[pg_test]
    fn test_read_begin_pins_and_returns_the_pair() {
        let oid = seed_registered_table();
        let (pin, t) = Spi::get_two_with_args::<i64, i64>(
            "SELECT pin_id, tier_key_hi FROM tierdb_read_begin($1)",
            &[oid.into()],
        )
        .expect("read_begin");
        assert_eq!(t, Some(100));
        let held = Spi::get_one::<i64>("SELECT count(*) FROM tierdb.read_pins")
            .expect("count")
            .unwrap();
        assert_eq!(held, 1, "pin holds the horizon");
        Spi::run_with_args("SELECT tierdb_read_end($1)", &[pin.unwrap().into()]).expect("read_end");
        let held = Spi::get_one::<i64>("SELECT count(*) FROM tierdb.read_pins")
            .expect("count")
            .unwrap();
        assert_eq!(held, 0);
    }

    #[pg_test]
    fn test_rewrite_scan_binds_pinned_constants_and_real_columns() {
        let oid = seed_registered_table();
        let sql = Spi::get_one_with_args::<String>("SELECT tierdb_rewrite_scan($1)", &[oid.into()])
            .expect("rewrite")
            .unwrap();
        assert!(
            sql.contains("00002-abc.metadata.json"),
            "S pinned by versioned metadata path:\n{sql}"
        );
        assert!(sql.contains("\"event_time\" >= 100") && sql.contains("\"event_time\" < 100"));
        assert!(
            sql.contains("r['id']::bigint"),
            "typed duckdb.row subscript:\n{sql}"
        );
        assert!(
            sql.contains("(d.payload ->> 'val')::text"),
            "delta projection:\n{sql}"
        );
        assert!(sql.contains("00002-abc.metadata.json"));
        let hot = sql.split("UNION ALL").next().unwrap();
        Spi::run("INSERT INTO public.events VALUES (7, 130, 'g')").unwrap();
        let n = Spi::get_one::<i64>(&format!("SELECT count(*) FROM ({hot}) q"))
            .expect("hot branch runs")
            .unwrap();
        assert_eq!(n, 1, "hot branch sees only tier_key >= T rows");
    }

    #[pg_test]
    fn test_rewrite_scan_requires_committed_lake_metadata() {
        let oid = seed_registered_table();
        Spi::run("UPDATE tierdb.cutline SET lake_props = NULL").expect("clear lake_props");
        let res = std::panic::catch_unwind(|| {
            Spi::get_one_with_args::<String>("SELECT tierdb_rewrite_scan($1)", &[oid.into()])
        });
        assert!(
            res.is_err(),
            "no committed snapshot errors, not a silent wrong scan"
        );
    }

    #[pg_test]
    fn test_upsert_routes_by_tier_key_against_the_cutline() {
        let oid = seed_registered_table();

        let target = Spi::get_one_with_args::<String>(
            "SELECT tierdb_upsert($1, '{\"id\": 7, \"event_time\": 150, \"val\": \"recent\"}'::jsonb)",
            &[oid.into()],
        )
        .expect("upsert hot")
        .unwrap();
        assert_eq!(target, "hot");
        let val = Spi::get_one::<String>("SELECT val FROM public.events WHERE id = 7")
            .expect("heap row")
            .unwrap();
        assert_eq!(val, "recent");

        let target = Spi::get_one_with_args::<String>(
            "SELECT tierdb_upsert($1, '{\"id\": 3, \"event_time\": 50, \"val\": \"corrected\"}'::jsonb)",
            &[oid.into()],
        )
        .expect("upsert cold")
        .unwrap();
        assert_eq!(target, "delta");
        let (op, payload) =
            Spi::get_two::<i16, pgrx::JsonB>("SELECT op, payload FROM tierdb.delta WHERE pk = '3'")
                .expect("delta row");
        assert_eq!(op, Some(0));
        assert_eq!(payload.unwrap().0["val"], "corrected");
        let heap = Spi::get_one::<i64>("SELECT count(*) FROM public.events WHERE id = 3")
            .expect("count")
            .unwrap();
        assert_eq!(heap, 0, "cold-bound row never touches the heap");
    }

    #[pg_test]
    fn test_newest_correction_wins_in_the_delta() {
        let oid = seed_registered_table();
        for val in ["first", "second"] {
            Spi::run_with_args(
                &format!(
                    "SELECT tierdb_upsert($1, '{{\"id\": 3, \"event_time\": 50, \"val\": \"{val}\"}}'::jsonb)"
                ),
                &[oid.into()],
            )
            .expect("upsert");
        }
        let (n, payload) = Spi::get_two::<i64, pgrx::JsonB>(
            "SELECT (SELECT count(*) FROM tierdb.delta), (SELECT payload FROM tierdb.delta WHERE pk = '3')",
        )
        .expect("delta");
        assert_eq!(n, Some(1), "one row per pk, corrections collapse");
        assert_eq!(payload.unwrap().0["val"], "second");
    }

    /// Direct mode with `duckdb.raw_query` shimmed into a log table.
    fn seed_direct_table() -> pg_sys::Oid {
        let oid = seed_registered_table();
        Spi::run("UPDATE tierdb.tables SET mode = 'direct'").expect("direct mode");
        Spi::run(
            "UPDATE tierdb.storage_profiles \
                SET warehouse = 'lakekeeper-wh', \
                    lake_config = '{\"catalog.uri\": \"http://lakekeeper:8181/catalog\"}' \
              WHERE profile_name = 'default'",
        )
        .expect("live catalog profile");
        Spi::run("CREATE SCHEMA duckdb").expect("shim schema");
        Spi::run("CREATE TABLE public.lake_log (sql text)").expect("shim log");
        Spi::run(
            "CREATE FUNCTION duckdb.raw_query(sql text) RETURNS void LANGUAGE sql \
             AS 'INSERT INTO public.lake_log VALUES (sql)'",
        )
        .expect("raw_query shim");
        oid
    }

    fn lake_log() -> Vec<String> {
        Spi::connect(|client| {
            client
                .select("SELECT sql FROM public.lake_log ORDER BY ctid", None, &[])
                .expect("log")
                .filter_map(|row| row.get::<String>(1).ok().flatten())
                .collect()
        })
    }

    #[pg_test]
    fn test_direct_upsert_commits_cold_rows_straight_to_the_lake() {
        let oid = seed_direct_table();

        let target = Spi::get_one_with_args::<String>(
            "SELECT tierdb_upsert($1, '{\"id\": 3, \"event_time\": 50, \"val\": \"corrected\"}'::jsonb)",
            &[oid.into()],
        )
        .expect("upsert cold")
        .unwrap();
        assert_eq!(target, "lake");
        assert_eq!(count("SELECT count(*) FROM tierdb.delta"), 0);
        assert_eq!(count("SELECT count(*) FROM public.events"), 0);

        let log = lake_log();
        assert!(
            log[0].starts_with("ATTACH IF NOT EXISTS 'lakekeeper-wh'"),
            "attach first: {log:?}"
        );
        assert!(
            log.iter()
                .any(|s| s.starts_with("DELETE FROM") && s.contains("__tierdb_lake_default")),
            "upsert clears the pk before inserting: {log:?}"
        );
        assert!(
            log.iter()
                .any(|s| s.starts_with("INSERT INTO") && s.contains("'corrected'")),
            "row values reach the lake insert: {log:?}"
        );
    }

    #[pg_test]
    fn test_direct_hot_rows_still_land_on_the_heap_only() {
        let oid = seed_direct_table();
        let target = Spi::get_one_with_args::<String>(
            "SELECT tierdb_upsert($1, '{\"id\": 7, \"event_time\": 150, \"val\": \"recent\"}'::jsonb)",
            &[oid.into()],
        )
        .expect("upsert hot")
        .unwrap();
        assert_eq!(target, "hot");
        assert_eq!(count("SELECT count(*) FROM public.events WHERE id = 7"), 1);
        assert!(lake_log().is_empty(), "hot writes never touch the lake");
    }

    #[pg_test]
    fn test_direct_delete_routes_cold_keys_to_the_lake() {
        let oid = seed_direct_table();
        let target = Spi::get_one_with_args::<String>(
            "SELECT tierdb_delete($1, '{\"id\": 3}'::jsonb, 50)",
            &[oid.into()],
        )
        .expect("delete cold")
        .unwrap();
        assert_eq!(target, "lake");
        assert_eq!(
            count("SELECT count(*) FROM tierdb.delta"),
            0,
            "no tombstones"
        );
        let log = lake_log();
        assert!(
            log.iter()
                .any(|s| s.starts_with("DELETE FROM") && s.contains("< 100")),
            "lake delete stays bounded to the cold side: {log:?}"
        );
    }

    #[pg_test]
    fn test_direct_retention_still_guards_the_lake_leg() {
        let oid = seed_direct_table();
        Spi::run("UPDATE tierdb.cutline SET retention_line = 40").expect("retention line");
        let res = std::panic::catch_unwind(|| {
            Spi::get_one_with_args::<String>(
                "SELECT tierdb_upsert($1, '{\"id\": 3, \"event_time\": 30, \"val\": \"x\"}'::jsonb)",
                &[oid.into()],
            )
        });
        assert!(
            res.is_err(),
            "expired rows are rejected before the lake leg"
        );
        assert!(lake_log().is_empty());
    }

    #[pg_test]
    fn test_direct_transparent_dml_is_rejected_unless_provably_hot() {
        seed_direct_table();
        Spi::run("INSERT INTO public.events VALUES (9, 140, 'new')").expect("hot row");
        Spi::run("UPDATE public.events SET val = 'seen' WHERE event_time >= 100 AND id = 9")
            .expect("provably hot DML passes through untouched");
        let res = std::panic::catch_unwind(|| {
            Spi::run("UPDATE public.events SET val = 'x' WHERE id = 9")
        });
        assert!(
            res.is_err(),
            "an unbounded UPDATE may touch lake rows and is rejected"
        );
    }

    #[pg_test]
    fn test_delete_routes_hot_dml_or_cold_tombstone() {
        let oid = seed_registered_table();
        Spi::run("INSERT INTO public.events VALUES (7, 150, 'recent')").unwrap();

        let target =
            Spi::get_one_with_args::<String>("SELECT tierdb_delete($1, '7', 150)", &[oid.into()])
                .expect("hot delete")
                .unwrap();
        assert_eq!(target, "hot");
        let left = Spi::get_one::<i64>("SELECT count(*) FROM public.events WHERE id = 7")
            .expect("count")
            .unwrap();
        assert_eq!(left, 0);

        let target =
            Spi::get_one_with_args::<String>("SELECT tierdb_delete($1, '3', 50)", &[oid.into()])
                .expect("cold delete")
                .unwrap();
        assert_eq!(target, "delta");
        let (op, tier) =
            Spi::get_two::<i16, i64>("SELECT op, tier_key FROM tierdb.delta WHERE pk = '3'")
                .expect("tombstone");
        assert_eq!(op, Some(1), "cold delete is a tombstone, not DML");
        assert_eq!(tier, Some(50));
    }

    #[pg_test]
    fn test_upsert_below_the_retention_line_is_rejected() {
        let oid = seed_registered_table();
        Spi::run("UPDATE tierdb.cutline SET retention_line = 40").expect("retention line");

        let target = Spi::get_one_with_args::<String>(
            "SELECT tierdb_upsert($1, '{\"id\": 3, \"event_time\": 40, \"val\": \"ok\"}'::jsonb)",
            &[oid.into()],
        )
        .expect("upsert at the line")
        .unwrap();
        assert_eq!(target, "delta");

        let res = std::panic::catch_unwind(|| {
            Spi::get_one_with_args::<String>(
                "SELECT tierdb_upsert($1, '{\"id\": 1, \"event_time\": 10, \"val\": \"gone\"}'::jsonb)",
                &[oid.into()],
            )
        });
        assert!(
            res.is_err(),
            "rows below R are expired from the lake, the correction must be rejected"
        );
    }

    #[pg_test]
    fn test_delete_below_the_retention_line_is_rejected() {
        let oid = seed_registered_table();
        Spi::run("UPDATE tierdb.cutline SET retention_line = 40").expect("retention line");
        let res = std::panic::catch_unwind(|| {
            Spi::get_one_with_args::<String>("SELECT tierdb_delete($1, '1', 10)", &[oid.into()])
        });
        assert!(res.is_err(), "below-R tombstones are rejected");
    }

    fn seed_composite_table() -> pg_sys::Oid {
        Spi::run("SET tierdb.transparent_reads = off").expect("guc");
        Spi::run(CATALOG_DDL).expect("catalog.sql applies");
        Spi::run(
            "CREATE TABLE public.locations (tenant bigint NOT NULL, vin text NOT NULL,
                                            event_time bigint NOT NULL, val text,
                                            PRIMARY KEY (tenant, vin))",
        )
        .expect("hot relation");
        Spi::run(
            "INSERT INTO tierdb.tables (table_id, schema_name, table_name, primary_key_cols,
                                       tier_key_col, partition_scheme, lake_format, lake_table_ref)
             SELECT 'public.locations'::regclass::oid::bigint, 'public', 'locations',
                    ARRAY['tenant','vin'], 'event_time',
                    '{\"unit\":\"hour\"}', 'iceberg', 'warehouse.locations'",
        )
        .expect("register");
        Spi::run(
            "INSERT INTO tierdb.cutline (table_id, tier_key_hi, lake_snapshot_id)
             SELECT 'public.locations'::regclass::oid::bigint, 100, 7",
        )
        .expect("cutline");
        Spi::get_one::<pg_sys::Oid>("SELECT 'public.locations'::regclass::oid")
            .expect("oid")
            .unwrap()
    }

    #[pg_test]
    fn test_composite_upsert_encodes_the_joined_pk() {
        let oid = seed_composite_table();
        let target = Spi::get_one_with_args::<String>(
            "SELECT tierdb_upsert($1, '{\"tenant\": 1, \"vin\": \"V1\", \"event_time\": 50, \"val\": \"cold\"}'::jsonb)",
            &[oid.into()],
        )
        .expect("upsert cold")
        .unwrap();
        assert_eq!(target, "delta");
        let pk = Spi::get_one::<String>("SELECT pk FROM tierdb.delta")
            .expect("delta pk")
            .unwrap();
        assert_eq!(pk, "1\u{1f}V1", "canonical joined encoding");
    }

    #[pg_test]
    fn test_composite_delete_takes_a_key_object() {
        let oid = seed_composite_table();
        Spi::run("INSERT INTO public.locations VALUES (1, 'V1', 150, 'recent')").unwrap();

        let target = Spi::get_one_with_args::<String>(
            "SELECT tierdb_delete($1, '{\"tenant\": 1, \"vin\": \"V1\"}'::jsonb, 150)",
            &[oid.into()],
        )
        .expect("hot delete")
        .unwrap();
        assert_eq!(target, "hot");
        let left = Spi::get_one::<i64>("SELECT count(*) FROM public.locations")
            .expect("count")
            .unwrap();
        assert_eq!(left, 0, "hot delete matched every pk column");

        let target = Spi::get_one_with_args::<String>(
            "SELECT tierdb_delete($1, '{\"tenant\": 2, \"vin\": \"V9\"}'::jsonb, 50)",
            &[oid.into()],
        )
        .expect("cold delete")
        .unwrap();
        assert_eq!(target, "delta");
        let (pk, payload) = Spi::get_two::<String, pgrx::JsonB>(
            "SELECT pk, payload FROM tierdb.delta WHERE op = 1",
        )
        .expect("tombstone");
        assert_eq!(pk, Some("2\u{1f}V9".into()));
        let payload = payload.unwrap().0;
        assert_eq!(
            payload["tenant"], 2,
            "tombstone payload keeps the pk fields"
        );
        assert_eq!(payload["vin"], "V9");
    }

    #[pg_test]
    fn test_single_pk_tombstone_payload_keeps_the_key_field() {
        let oid = seed_registered_table();
        Spi::run_with_args("SELECT tierdb_delete($1, '3', 50)", &[oid.into()])
            .expect("cold delete");
        let payload =
            Spi::get_one::<pgrx::JsonB>("SELECT payload FROM tierdb.delta WHERE pk = '3'")
                .expect("payload")
                .unwrap();
        assert_eq!(payload.0["id"], 3);
    }

    fn seed_transparent() {
        seed_registered_table();
        Spi::run("SET tierdb.transparent_reads = on").expect("guc");

        Spi::run("CREATE SCHEMA duckdb").expect("stub schema");
        Spi::run("CREATE TABLE public.fake_cold (r jsonb)").expect("fake cold tier");
        Spi::run(
            r#"INSERT INTO public.fake_cold VALUES
                 ('{"id": 1, "event_time": 10, "val": "a"}'),
                 ('{"id": 2, "event_time": 50, "val": "b"}')"#,
        )
        .expect("cold rows");
        Spi::run(
            "CREATE FUNCTION duckdb.query(sql text) RETURNS SETOF jsonb \
             LANGUAGE sql AS 'SELECT r FROM public.fake_cold'",
        )
        .expect("duckdb.query stub");

        Spi::run("INSERT INTO public.events VALUES (7, 130, 'recent'), (1, 10, 'a')")
            .expect("hot rows");
    }

    fn count(sql: &str) -> i64 {
        Spi::get_one::<i64>(sql).expect("query").unwrap()
    }

    #[pg_test]
    fn test_transparent_select_spans_both_tiers() {
        seed_transparent();
        assert_eq!(
            count("SELECT count(*) FROM public.events"),
            3,
            "1 hot + 2 cold; the already-tiered heap row is not double-counted"
        );
    }

    #[pg_test]
    fn test_transparent_predicates_apply_over_the_union() {
        seed_transparent();
        assert_eq!(
            count("SELECT count(*) FROM public.events WHERE event_time >= 100"),
            1
        );
        assert_eq!(
            count("SELECT count(*) FROM public.events WHERE event_time < 100"),
            2
        );
        assert_eq!(
            count("SELECT count(*) FROM public.events WHERE id = 2"),
            1,
            "cold-only row visible through plain SQL"
        );
    }

    #[pg_test]
    fn test_transparent_delta_correction_wins() {
        seed_transparent();
        Spi::run(
            "INSERT INTO tierdb.delta (table_id, pk, op, tier_key, version, payload)
             SELECT 'public.events'::regclass::oid::bigint, '2', 0, 50, 1,
                    '{\"id\": 2, \"event_time\": 50, \"val\": \"corrected\"}'",
        )
        .expect("correction");
        let val = Spi::get_one::<String>("SELECT val FROM public.events WHERE id = 2")
            .expect("query")
            .unwrap();
        assert_eq!(val, "corrected", "delta overlays the cold base row");
        assert_eq!(
            count("SELECT count(*) FROM public.events"),
            3,
            "still no duplicate"
        );
    }

    #[pg_test]
    fn test_transparent_tombstone_hides_the_cold_row() {
        seed_transparent();
        Spi::run(
            "INSERT INTO tierdb.delta (table_id, pk, op, tier_key, version)
             SELECT 'public.events'::regclass::oid::bigint, '2', 1, 50, 1",
        )
        .expect("tombstone");
        assert_eq!(count("SELECT count(*) FROM public.events"), 2);
        assert_eq!(count("SELECT count(*) FROM public.events WHERE id = 2"), 0);
    }

    #[pg_test]
    fn test_transparent_guc_off_restores_plain_heap_semantics() {
        seed_transparent();
        Spi::run("SET tierdb.transparent_reads = off").expect("guc");
        assert_eq!(
            count("SELECT count(*) FROM public.events"),
            2,
            "raw heap rows only"
        );
        Spi::run("SET tierdb.transparent_reads = on").expect("guc");
        assert_eq!(count("SELECT count(*) FROM public.events"), 3);
    }

    #[pg_test]
    fn test_transparent_read_pins_the_horizon() {
        seed_transparent();
        count("SELECT count(*) FROM public.events");
        let (pins, t) = Spi::get_two::<i64, i64>(
            "SELECT count(*), max(pinned_tier_key_hi) FROM tierdb.read_pins",
        )
        .expect("pins");
        assert_eq!(pins, Some(1), "planning the read acquired a pin");
        assert_eq!(t, Some(100), "pinned at the cut-line");
    }

    #[pg_test]
    fn test_transparent_joins_and_aggregates() {
        seed_transparent();
        assert_eq!(
            count("SELECT count(*) FROM public.events e JOIN public.events f ON e.id = f.id"),
            3,
            "both sides of the join see the merged view"
        );
        let sum = Spi::get_one::<i64>("SELECT sum(id)::bigint FROM public.events")
            .expect("agg")
            .unwrap();
        assert_eq!(sum, 10, "1 + 2 + 7 across tiers");
    }

    #[pg_test]
    fn test_transparent_subqueries_and_ctes_are_rewritten_too() {
        seed_transparent();
        assert_eq!(
            count("WITH c AS (SELECT * FROM public.events) SELECT count(*) FROM c"),
            3
        );
        assert_eq!(
            count("SELECT count(*) FROM (SELECT id FROM public.events WHERE event_time < 100) s"),
            2
        );
        assert_eq!(
            count(
                "SELECT count(*) FROM public.fake_cold f \
                   WHERE EXISTS (SELECT 1 FROM public.events e WHERE e.id = (f.r['id'])::bigint)"
            ),
            2,
            "sublink references are rewritten too"
        );
    }

    #[pg_test]
    fn test_transparent_dml_by_pk_still_lands_on_hot_rows() {
        seed_transparent();
        Spi::run("INSERT INTO public.events VALUES (9, 140, 'new')").expect("insert");
        Spi::run("UPDATE public.events SET val = 'seen' WHERE id = 9").expect("update");
        assert_eq!(count("SELECT count(*) FROM public.events"), 4);
        Spi::run("DELETE FROM public.events WHERE id = 9").expect("delete");
        assert_eq!(count("SELECT count(*) FROM public.events"), 3);
        assert_eq!(count("SELECT count(*) FROM tierdb.delta"), 0);
    }

    #[pg_test]
    fn test_transparent_leaves_unregistered_tables_untouched() {
        seed_transparent();
        assert_eq!(
            count("SELECT count(*) FROM public.fake_cold"),
            2,
            "raw rows"
        );
    }

    #[pg_test]
    fn test_transparent_update_cold_row_writes_delta() {
        seed_transparent();
        Spi::run(
            "DO $$ DECLARE n bigint; BEGIN \
               UPDATE public.events SET val = 'fixed' WHERE id = 2; \
               GET DIAGNOSTICS n = ROW_COUNT; \
               IF n <> 1 THEN RAISE EXCEPTION 'expected UPDATE 1, got %', n; END IF; \
             END $$",
        )
        .expect("cold update");
        let (op, payload) =
            Spi::get_two::<i16, pgrx::JsonB>("SELECT op, payload FROM tierdb.delta WHERE pk = '2'")
                .expect("delta row");
        assert_eq!(op, Some(0));
        let payload = payload.unwrap().0;
        assert_eq!(payload["val"], "fixed");
        assert_eq!(payload["event_time"], 50, "old row image carried over");
        let val = Spi::get_one::<String>("SELECT val FROM public.events WHERE id = 2")
            .expect("read")
            .unwrap();
        assert_eq!(
            val, "fixed",
            "the correction reads back through the overlay"
        );
    }

    #[pg_test]
    fn test_transparent_update_mixed_touches_both_tiers() {
        seed_transparent();
        Spi::run(
            "DO $$ DECLARE n bigint; BEGIN \
               UPDATE public.events SET val = 'v' || id::text; \
               GET DIAGNOSTICS n = ROW_COUNT; \
               IF n <> 3 THEN RAISE EXCEPTION 'expected UPDATE 3, got %', n; END IF; \
             END $$",
        )
        .expect("mixed update");
        let hot = Spi::get_one::<String>("SELECT val FROM public.events WHERE id = 7")
            .expect("hot")
            .unwrap();
        assert_eq!(hot, "v7");
        let cold = Spi::get_one::<String>("SELECT val FROM public.events WHERE id = 1")
            .expect("cold")
            .unwrap();
        assert_eq!(
            cold, "v1",
            "SET expressions evaluate over the old cold image"
        );
        assert_eq!(count("SELECT count(*) FROM tierdb.delta WHERE op = 0"), 2);
    }

    #[pg_test]
    fn test_transparent_update_provably_hot_stays_untouched() {
        seed_transparent();
        let id = Spi::get_one::<i64>(
            "UPDATE public.events SET val = 'seen' WHERE event_time >= 100 RETURNING id",
        )
        .expect("hot update")
        .unwrap();
        assert_eq!(id, 7);
        let id = Spi::get_one::<i64>(
            "UPDATE public.events SET val = 'again' WHERE event_time IN (130, 200) RETURNING id",
        )
        .expect("IN list proves hot")
        .unwrap();
        assert_eq!(id, 7);
        assert_eq!(count("SELECT count(*) FROM tierdb.delta"), 0);
    }

    #[pg_test]
    fn test_transparent_delete_cold_row_writes_tombstone() {
        seed_transparent();
        Spi::run(
            "DO $$ DECLARE n bigint; BEGIN \
               DELETE FROM public.events WHERE id = 2; \
               GET DIAGNOSTICS n = ROW_COUNT; \
               IF n <> 1 THEN RAISE EXCEPTION 'expected DELETE 1, got %', n; END IF; \
             END $$",
        )
        .expect("cold delete");
        let (op, payload) =
            Spi::get_two::<i16, pgrx::JsonB>("SELECT op, payload FROM tierdb.delta WHERE pk = '2'")
                .expect("tombstone");
        assert_eq!(op, Some(1));
        assert_eq!(payload.unwrap().0["id"], 2, "pk fields kept for the fold");
        assert_eq!(count("SELECT count(*) FROM public.events"), 2);
        assert_eq!(count("SELECT count(*) FROM public.events WHERE id = 2"), 0);
    }

    #[pg_test]
    fn test_transparent_delete_without_where_clears_both_tiers() {
        seed_transparent();
        Spi::run(
            "DO $$ DECLARE n bigint; BEGIN \
               DELETE FROM public.events; \
               GET DIAGNOSTICS n = ROW_COUNT; \
               IF n <> 3 THEN RAISE EXCEPTION 'expected DELETE 3, got %', n; END IF; \
             END $$",
        )
        .expect("full delete");
        assert_eq!(count("SELECT count(*) FROM public.events"), 0);
        assert_eq!(count("SELECT count(*) FROM tierdb.delta WHERE op = 1"), 2);
    }

    #[pg_test]
    fn test_transparent_dml_rolls_back_with_the_transaction() {
        seed_transparent();
        Spi::run(
            "DO $$ BEGIN \
               UPDATE public.events SET val = 'gone' WHERE id = 2; \
               RAISE EXCEPTION 'abort'; \
             EXCEPTION WHEN OTHERS THEN NULL; END $$",
        )
        .expect("aborted block");
        assert_eq!(count("SELECT count(*) FROM tierdb.delta"), 0);
    }

    #[pg_test]
    fn test_transparent_update_binds_plpgsql_params() {
        seed_transparent();
        Spi::run(
            "DO $$ DECLARE v text := 'from_param'; BEGIN \
               UPDATE public.events SET val = v WHERE id = 2; \
             END $$",
        )
        .expect("param update");
        let payload =
            Spi::get_one::<pgrx::JsonB>("SELECT payload FROM tierdb.delta WHERE pk = '2'")
                .expect("delta")
                .unwrap();
        assert_eq!(payload.0["val"], "from_param");
    }

    #[pg_test]
    fn test_transparent_dml_pins_the_horizon() {
        seed_transparent();
        Spi::run("UPDATE public.events SET val = 'x' WHERE id = 2").expect("update");
        let pins = count("SELECT count(*) FROM tierdb.read_pins");
        assert_eq!(pins, 1, "the cold scan is pinned like a read");
    }

    #[pg_test]
    fn test_transparent_writes_guc_off_leaves_dml_untouched() {
        seed_transparent();
        Spi::run("SET tierdb.transparent_writes = off").expect("guc");
        Spi::run("UPDATE public.events SET val = 'x' WHERE id = 2").expect("plain semantics");
        assert_eq!(
            count("SELECT count(*) FROM tierdb.delta"),
            0,
            "no rewrite: the cold row is simply not matched, as without the extension"
        );
    }

    #[pg_test]
    fn test_transparent_update_returning_covers_cold_rows() {
        seed_transparent();
        let (id, val) = Spi::get_two::<i64, String>(
            "UPDATE public.events SET val = 'fixed' WHERE id = 2 RETURNING id, val",
        )
        .expect("cold returning");
        assert_eq!(id, Some(2));
        assert_eq!(val, Some("fixed".into()), "new image, not the old one");
        assert_eq!(count("SELECT count(*) FROM tierdb.delta WHERE op = 0"), 1);
    }

    #[pg_test]
    fn test_transparent_update_returning_mixed_streams_both_tiers() {
        seed_transparent();
        let rows = Spi::connect_mut(|client| {
            let table = client
                .update(
                    "UPDATE public.events SET val = 'v' || id::text RETURNING id",
                    None,
                    &[],
                )
                .expect("mixed returning update");
            table
                .into_iter()
                .map(|row| row.get::<i64>(1).expect("id").unwrap())
                .collect::<Vec<_>>()
        });
        let mut rows = rows;
        rows.sort_unstable();
        assert_eq!(rows, vec![1, 2, 7], "hot row plus both injected cold rows");
        assert_eq!(count("SELECT count(*) FROM tierdb.delta WHERE op = 0"), 2);
    }

    #[pg_test]
    fn test_transparent_update_returning_into_plpgsql() {
        seed_transparent();
        Spi::run(
            "DO $$ DECLARE v text; n bigint; BEGIN \
               UPDATE public.events SET val = 'fixed' WHERE id = 2 RETURNING val INTO v; \
               GET DIAGNOSTICS n = ROW_COUNT; \
               IF v <> 'fixed' THEN RAISE EXCEPTION 'expected fixed, got %', v; END IF; \
               IF n <> 1 THEN RAISE EXCEPTION 'expected 1 row, got %', n; END IF; \
             END $$",
        )
        .expect("returning into");
    }

    #[pg_test]
    fn test_transparent_delete_returning_yields_old_cold_image() {
        seed_transparent();
        let tagged = Spi::get_one::<String>(
            "DELETE FROM public.events WHERE id = 2 RETURNING event_time::text || '/gone'",
        )
        .expect("cold delete returning")
        .unwrap();
        assert_eq!(tagged, "50/gone", "expressions evaluate over the old image");
        assert_eq!(count("SELECT count(*) FROM tierdb.delta WHERE op = 1"), 1);
    }

    #[pg_test]
    fn test_transparent_update_moves_cold_row_within_the_cold_tier() {
        seed_transparent();
        Spi::run("UPDATE public.events SET event_time = 60 WHERE id = 2").expect("cold move");
        let (tier, old_tier) = Spi::get_two::<i64, i64>(
            "SELECT tier_key, old_tier_key FROM tierdb.delta WHERE pk = '2'",
        )
        .expect("delta row");
        assert_eq!(tier, Some(60), "the upsert lands at the new tier");
        assert_eq!(
            old_tier,
            Some(50),
            "the fold must delete where the lake holds the image"
        );
        let t = Spi::get_one::<i64>("SELECT event_time FROM public.events WHERE id = 2")
            .expect("read")
            .unwrap();
        assert_eq!(t, 60, "the move reads back through the overlay");
    }

    #[pg_test]
    fn test_transparent_update_moves_cold_row_to_the_hot_tier() {
        seed_transparent();
        Spi::run(
            "DO $$ DECLARE n bigint; BEGIN \
               UPDATE public.events SET event_time = 150 WHERE id = 2; \
               GET DIAGNOSTICS n = ROW_COUNT; \
               IF n <> 1 THEN RAISE EXCEPTION 'expected UPDATE 1, got %', n; END IF; \
             END $$",
        )
        .expect("cold to hot move");
        let (op, tier) =
            Spi::get_two::<i16, i64>("SELECT op, tier_key FROM tierdb.delta WHERE pk = '2'")
                .expect("delta row");
        assert_eq!(op, Some(1), "the lake image gets a tombstone");
        assert_eq!(tier, Some(50), "at the tier the lake holds it");
        Spi::run("SET tierdb.transparent_reads = off").expect("guc");
        let heap = Spi::get_one::<i64>("SELECT event_time FROM public.events WHERE id = 2")
            .expect("heap row")
            .unwrap();
        assert_eq!(heap, 150, "the new image lives in the heap");
        Spi::run("SET tierdb.transparent_reads = on").expect("guc");
        assert_eq!(
            count("SELECT count(*) FROM public.events WHERE id = 2"),
            1,
            "no duplicate across tiers"
        );
    }

    #[pg_test]
    fn test_transparent_update_set_pk_is_rejected() {
        seed_transparent();
        let res =
            std::panic::catch_unwind(|| Spi::run("UPDATE public.events SET id = 9 WHERE id = 2"));
        assert!(res.is_err(), "the delta overlay is keyed by the pk");
    }

    #[pg_test]
    fn test_transparent_update_with_from_is_rejected() {
        seed_transparent();
        let res = std::panic::catch_unwind(|| {
            Spi::run(
                "UPDATE public.events e SET val = 'x' FROM public.fake_cold f \
                 WHERE e.id = (f.r['id'])::bigint",
            )
        });
        assert!(res.is_err(), "DML joins land in phase 3");
    }

    #[pg_test]
    fn test_transparent_update_with_cte_is_rejected() {
        seed_transparent();
        let res = std::panic::catch_unwind(|| {
            Spi::run("WITH x AS (SELECT 1) UPDATE public.events SET val = 'x' WHERE id = 2")
        });
        assert!(res.is_err());
    }

    #[pg_test]
    fn test_transparent_dml_below_retention_line_is_rejected() {
        seed_transparent();
        Spi::run("UPDATE tierdb.cutline SET retention_line = 40").expect("retention line");
        Spi::run("UPDATE public.events SET val = 'ok' WHERE id = 2")
            .expect("tier 50 is above the line");
        let res = std::panic::catch_unwind(|| Spi::run("DELETE FROM public.events WHERE id = 1"));
        assert!(res.is_err(), "tier 10 is expired, the statement must abort");
    }

    fn seed_mirrored(heap_retention_lag: Option<i64>) {
        seed_transparent();
        match heap_retention_lag {
            Some(lag) => Spi::run(&format!(
                "UPDATE tierdb.tables SET mode = 'mirrored', heap_retention_lag = {lag}"
            ))
            .expect("mirrored+retention"),
            None => Spi::run("UPDATE tierdb.tables SET mode = 'mirrored'").expect("mirrored"),
        }
    }

    #[pg_test]
    fn test_mirrored_default_reads_stay_on_the_heap() {
        seed_mirrored(None);
        assert_eq!(
            count("SELECT count(*) FROM public.events"),
            2,
            "no retention: the heap is complete, plain scans are untouched"
        );
    }

    #[pg_test]
    fn test_mirrored_with_retention_rewrites_at_the_stored_seam() {
        seed_mirrored(Some(1_000));
        assert_eq!(
            count("SELECT count(*) FROM public.events"),
            3,
            "heap below R is gone: reads union at the retention line like tiered"
        );
    }

    #[pg_test]
    fn test_mirrored_hybrid_spans_tiers_when_frontier_caught_up() {
        seed_mirrored(None);
        Spi::run("UPDATE tierdb.cutline SET replicated_lsn = 9223372036854775807")
            .expect("frontier");
        Spi::run("SET tierdb.mirrored_reads = 'hybrid'").expect("guc");
        assert_eq!(
            count("SELECT count(*) FROM public.events"),
            3,
            "hybrid seam at max(tier_key): newest row from the heap, bulk from the lake"
        );
    }

    #[pg_test]
    fn test_mirrored_hybrid_heap_fallback_on_frontier_timeout() {
        seed_mirrored(None);
        Spi::run("SET tierdb.mirrored_reads = 'hybrid'").expect("guc");
        Spi::run("SET tierdb.mirror_wait_ms = 0").expect("guc");
        assert_eq!(
            count("SELECT count(*) FROM public.events"),
            2,
            "timeout: heap-only read (always correct for no-retention mirrors)"
        );
    }

    fn seed_spill_table() -> pg_sys::Oid {
        Spi::run("SET tierdb.transparent_reads = off").expect("guc");
        Spi::run(CATALOG_DDL).expect("catalog.sql applies");
        Spi::run(
            "CREATE TABLE public.events (id bigint, event_time bigint NOT NULL, val text) \
             PARTITION BY RANGE (event_time)",
        )
        .expect("partitioned parent");
        Spi::run(
            "CREATE TABLE public.events_p1 PARTITION OF public.events \
             FOR VALUES FROM (100) TO (200)",
        )
        .expect("hot partition");
        Spi::run(
            "INSERT INTO tierdb.tables (table_id, schema_name, table_name, primary_key_cols,
                                       tier_key_col, partition_scheme, lake_format, lake_table_ref)
             SELECT 'public.events'::regclass::oid::bigint, 'public', 'events', ARRAY['id'], 'event_time',
                    '{\"unit\":\"hour\"}', 'iceberg', 'warehouse.events'",
        )
        .expect("register");
        Spi::run(
            "INSERT INTO tierdb.cutline (table_id, tier_key_hi, lake_snapshot_id, lake_props)
             SELECT 'public.events'::regclass::oid::bigint, 100, 7,
                    '{\"metadata_location\":\"/wh/events/metadata/00002-abc.metadata.json\"}'",
        )
        .expect("cutline");
        let oid = Spi::get_one::<pg_sys::Oid>("SELECT 'public.events'::regclass::oid")
            .expect("oid")
            .unwrap();
        Spi::run_with_args("SELECT tierdb_enable_transparent_writes($1)", &[oid.into()])
            .expect("enable spill");
        oid
    }

    #[pg_test]
    fn test_plain_insert_routes_cold_rows_to_delta() {
        seed_spill_table();
        Spi::run("INSERT INTO public.events VALUES (1, 50, 'late')").expect("cold insert");
        let (op, tier) =
            Spi::get_two::<i16, i64>("SELECT op, tier_key FROM tierdb.delta WHERE pk = '1'")
                .expect("delta row");
        assert_eq!(op, Some(0));
        assert_eq!(tier, Some(50));
        assert_eq!(
            count("SELECT count(*) FROM public.events"),
            0,
            "the spill stays empty; the row lives only in the delta"
        );
    }

    #[pg_test]
    fn test_mixed_insert_splits_and_counts_both_halves() {
        seed_spill_table();
        Spi::run(
            "DO $$ DECLARE n bigint; BEGIN \
               INSERT INTO public.events VALUES (1, 150, 'hot'), (2, 50, 'cold'); \
               GET DIAGNOSTICS n = ROW_COUNT; \
               IF n <> 2 THEN RAISE EXCEPTION 'expected INSERT 2, got %', n; END IF; \
             END $$",
        )
        .expect("mixed insert counts both tiers");
        assert_eq!(count("SELECT count(*) FROM public.events_p1"), 1);
        assert_eq!(count("SELECT count(*) FROM tierdb.delta"), 1);
    }

    #[pg_test]
    fn test_plain_insert_newest_correction_wins() {
        seed_spill_table();
        Spi::run("INSERT INTO public.events VALUES (3, 50, 'first')").expect("insert");
        Spi::run("INSERT INTO public.events VALUES (3, 50, 'second')").expect("insert");
        let (n, payload) = Spi::get_two::<i64, pgrx::JsonB>(
            "SELECT (SELECT count(*) FROM tierdb.delta), (SELECT payload FROM tierdb.delta WHERE pk = '3')",
        )
        .expect("delta");
        assert_eq!(n, Some(1));
        assert_eq!(payload.unwrap().0["val"], "second");
    }

    #[pg_test]
    fn test_plain_insert_rolls_back_with_the_transaction() {
        seed_spill_table();
        Spi::run(
            "DO $$ BEGIN \
               INSERT INTO public.events VALUES (9, 50, 'gone'); \
               RAISE EXCEPTION 'abort'; \
             EXCEPTION WHEN OTHERS THEN NULL; END $$",
        )
        .expect("aborted block");
        assert_eq!(
            count("SELECT count(*) FROM tierdb.delta"),
            0,
            "the delta write is transactional like any heap write"
        );
    }

    #[pg_test]
    fn test_plain_insert_below_retention_line_is_rejected() {
        seed_spill_table();
        Spi::run("UPDATE tierdb.cutline SET retention_line = 40").expect("retention line");
        let res = std::panic::catch_unwind(|| {
            Spi::run("INSERT INTO public.events VALUES (1, 10, 'expired')")
        });
        assert!(res.is_err(), "below-R inserts are rejected, not buffered");
    }

    #[pg_test]
    fn test_transparent_writes_guc_off_makes_cold_inserts_error() {
        seed_spill_table();
        Spi::run("INSERT INTO public.events VALUES (1, 50, 'x')").expect("routed when on");
        Spi::run("SET tierdb.transparent_writes = off").expect("guc");
        let res =
            std::panic::catch_unwind(|| Spi::run("INSERT INTO public.events VALUES (2, 50, 'x')"));
        assert!(res.is_err());
    }

    #[pg_test]
    fn test_recent_row_without_a_partition_is_still_an_error() {
        seed_spill_table();
        let res = std::panic::catch_unwind(|| {
            Spi::run("INSERT INTO public.events VALUES (1, 500, 'future')")
        });
        assert!(
            res.is_err(),
            "above the cut-line a missing partition is a premake gap, not a delta case"
        );
    }

    #[pg_test]
    fn test_returning_works_hot_and_rejects_cold() {
        seed_spill_table();
        let id =
            Spi::get_one::<i64>("INSERT INTO public.events VALUES (1, 150, 'hot') RETURNING id")
                .expect("hot returning")
                .unwrap();
        assert_eq!(id, 1);
        let res = std::panic::catch_unwind(|| {
            Spi::get_one::<i64>("INSERT INTO public.events VALUES (2, 50, 'cold') RETURNING id")
        });
        assert!(res.is_err(), "RETURNING cannot cover delta-routed rows");
    }

    #[pg_test]
    fn test_on_conflict_works_hot_and_rejects_cold() {
        seed_spill_table();
        Spi::run("INSERT INTO public.events VALUES (1, 150, 'hot') ON CONFLICT DO NOTHING")
            .expect("hot on conflict");
        let res = std::panic::catch_unwind(|| {
            Spi::run("INSERT INTO public.events VALUES (2, 50, 'cold') ON CONFLICT DO NOTHING")
        });
        assert!(
            res.is_err(),
            "heap conflict semantics do not translate to the delta"
        );
    }

    #[pg_test]
    fn test_enable_transparent_writes_is_idempotent() {
        let oid = seed_spill_table();
        let msg = Spi::get_one_with_args::<String>(
            "SELECT tierdb_enable_transparent_writes($1)",
            &[oid.into()],
        )
        .expect("second enable")
        .unwrap();
        assert!(msg.contains("already enabled"), "{msg}");
    }

    #[pg_test]
    fn test_disable_transparent_writes_restores_plain_errors() {
        let oid = seed_spill_table();
        Spi::run_with_args(
            "SELECT tierdb_disable_transparent_writes($1)",
            &[oid.into()],
        )
        .expect("disable");
        let res =
            std::panic::catch_unwind(|| Spi::run("INSERT INTO public.events VALUES (1, 50, 'x')"));
        assert!(res.is_err(), "no spill: plain partition-routing error");
    }

    #[pg_test]
    fn test_fully_mirrored_table_refuses_the_spill() {
        let oid = seed_spill_table();
        Spi::run_with_args(
            "SELECT tierdb_disable_transparent_writes($1)",
            &[oid.into()],
        )
        .expect("reset");
        Spi::run("UPDATE tierdb.tables SET mode = 'mirrored', heap_retention_lag = NULL")
            .expect("fully mirrored");
        let res = std::panic::catch_unwind(|| {
            Spi::run_with_args("SELECT tierdb_enable_transparent_writes($1)", &[oid.into()])
        });
        assert!(res.is_err(), "a complete heap takes every insert directly");
    }

    #[pg_test]
    fn test_copy_routes_cold_rows_too() {
        seed_spill_table();
        let path = std::env::temp_dir().join("tierdb_spill_copy_test.csv");
        std::fs::write(&path, "1,150,hot\n2,50,cold\n").expect("write csv");
        Spi::run(&format!(
            "COPY public.events FROM '{}' WITH (FORMAT csv)",
            path.display()
        ))
        .expect("copy");
        std::fs::remove_file(&path).ok();
        assert_eq!(count("SELECT count(*) FROM public.events_p1"), 1);
        assert_eq!(count("SELECT count(*) FROM tierdb.delta"), 1);
    }

    #[pg_test]
    fn test_plain_insert_is_visible_to_transparent_reads() {
        seed_spill_table();
        Spi::run("CREATE SCHEMA duckdb").expect("stub schema");
        Spi::run(
            "CREATE FUNCTION duckdb.query(sql text) RETURNS SETOF jsonb \
             LANGUAGE sql AS 'SELECT NULL::jsonb WHERE false'",
        )
        .expect("empty cold tier stub");
        Spi::run("INSERT INTO public.events VALUES (1, 150, 'hot'), (2, 50, 'cold')")
            .expect("mixed insert");
        Spi::run("SET tierdb.transparent_reads = on").expect("guc");
        assert_eq!(
            count("SELECT count(*) FROM public.events"),
            2,
            "the delta-routed row reads back through the overlay"
        );
    }

    fn seed_keep_heap_table() -> pg_sys::Oid {
        Spi::run("SET tierdb.transparent_reads = off").expect("guc");
        Spi::run(CATALOG_DDL).expect("catalog.sql applies");
        Spi::run(
            "CREATE TABLE public.events (id bigint, event_time bigint NOT NULL, val text) \
             PARTITION BY RANGE (event_time)",
        )
        .expect("partitioned parent");
        Spi::run(
            "CREATE TABLE public.events_p0 PARTITION OF public.events \
             FOR VALUES FROM (0) TO (100)",
        )
        .expect("kept cold partition");
        Spi::run(
            "CREATE TABLE public.events_p1 PARTITION OF public.events \
             FOR VALUES FROM (100) TO (200)",
        )
        .expect("hot partition");
        Spi::run(
            "INSERT INTO tierdb.tables (table_id, schema_name, table_name, primary_key_cols,
                                       tier_key_col, partition_scheme, lake_format, lake_table_ref, keep_heap)
             SELECT 'public.events'::regclass::oid::bigint, 'public', 'events', ARRAY['id'], 'event_time',
                    '{\"unit\":\"hour\"}', 'iceberg', 'warehouse.events', true",
        )
        .expect("register keep-heap");
        Spi::run(
            "INSERT INTO tierdb.cutline (table_id, tier_key_hi, lake_snapshot_id, lake_props)
             SELECT 'public.events'::regclass::oid::bigint, 100, 7,
                    '{\"metadata_location\":\"/wh/events/metadata/00002-abc.metadata.json\"}'",
        )
        .expect("cutline");
        let oid = Spi::get_one::<pg_sys::Oid>("SELECT 'public.events'::regclass::oid")
            .expect("oid")
            .unwrap();
        Spi::run_with_args(
            "SELECT tierdb_attach_cold_mirror($1, 'public.events_p0'::regclass)",
            &[oid.into()],
        )
        .expect("attach cold mirror");
        oid
    }

    #[pg_test]
    fn test_keep_heap_trigger_mirrors_plain_dml_into_the_delta() {
        seed_keep_heap_table();
        Spi::run("INSERT INTO public.events VALUES (1, 50, 'v1')").expect("insert below T");
        assert_eq!(
            count("SELECT count(*) FROM public.events_p0"),
            1,
            "the heap keeps the row"
        );
        let (op, payload) =
            Spi::get_two::<i16, pgrx::JsonB>("SELECT op, payload FROM tierdb.delta WHERE pk = '1'")
                .expect("delta upsert");
        assert_eq!(op, Some(0));
        assert_eq!(payload.unwrap().0["val"], "v1");

        Spi::run("UPDATE public.events SET val = 'v2' WHERE id = 1").expect("plain update");
        let payload =
            Spi::get_one::<pgrx::JsonB>("SELECT payload FROM tierdb.delta WHERE pk = '1'")
                .expect("delta")
                .unwrap();
        assert_eq!(
            payload.0["val"], "v2",
            "the update overwrites the delta image"
        );

        Spi::run("DELETE FROM public.events WHERE id = 1").expect("plain delete");
        assert_eq!(count("SELECT count(*) FROM public.events_p0"), 0);
        let op = Spi::get_one::<i16>("SELECT op FROM tierdb.delta WHERE pk = '1'")
            .expect("delta")
            .unwrap();
        assert_eq!(op, 1, "the delete becomes a tombstone");
    }

    #[pg_test]
    fn test_keep_heap_hot_partition_stays_unmirrored() {
        seed_keep_heap_table();
        Spi::run("INSERT INTO public.events VALUES (5, 150, 'hot')").expect("hot insert");
        Spi::run("UPDATE public.events SET val = 'hot2' WHERE id = 5").expect("hot update");
        assert_eq!(
            count("SELECT count(*) FROM tierdb.delta"),
            0,
            "hot rows need no mirror, the heap is their only home"
        );
    }

    #[pg_test]
    fn test_keep_heap_update_with_from_is_not_blocked() {
        seed_keep_heap_table();
        Spi::run("INSERT INTO public.events VALUES (1, 50, 'old')").expect("seed");
        Spi::run("CREATE TABLE public.fixes (id bigint, v text)").expect("aux table");
        Spi::run("INSERT INTO public.fixes VALUES (1, 'fixed')").expect("aux row");
        Spi::run("UPDATE public.events e SET val = f.v FROM public.fixes f WHERE e.id = f.id")
            .expect("FROM/USING guard does not apply to keep-heap tables");
        let payload =
            Spi::get_one::<pgrx::JsonB>("SELECT payload FROM tierdb.delta WHERE pk = '1'")
                .expect("delta")
                .unwrap();
        assert_eq!(payload.0["val"], "fixed");
    }

    #[pg_test]
    fn test_keep_heap_cross_partition_move_tombstones_the_old_tier() {
        seed_keep_heap_table();
        Spi::run("INSERT INTO public.events VALUES (1, 50, 'v1')").expect("seed");
        Spi::run("UPDATE public.events SET event_time = 150 WHERE id = 1")
            .expect("row moves above the cut-line");
        assert_eq!(count("SELECT count(*) FROM public.events_p1"), 1);
        let op = Spi::get_one::<i16>("SELECT op FROM tierdb.delta WHERE pk = '1'")
            .expect("delta")
            .unwrap();
        assert_eq!(
            op, 1,
            "the DELETE half of the move tombstones the cold image"
        );
    }

    #[pg_test]
    fn test_keep_heap_routed_functions_are_plain_heap_ops() {
        let oid = seed_keep_heap_table();
        Spi::run("INSERT INTO public.events VALUES (1, 50, 'v1')").expect("seed");

        let target = Spi::get_one_with_args::<String>(
            "SELECT tierdb_upsert($1, '{\"id\": 1, \"event_time\": 50, \"val\": \"v2\"}'::jsonb)",
            &[oid.into()],
        )
        .expect("upsert")
        .unwrap();
        assert_eq!(target, "hot");
        let val = Spi::get_one::<String>("SELECT val FROM public.events WHERE id = 1")
            .expect("heap row")
            .unwrap();
        assert_eq!(val, "v2", "the heap row is replaced in place");
        let (op, payload) =
            Spi::get_two::<i16, pgrx::JsonB>("SELECT op, payload FROM tierdb.delta WHERE pk = '1'")
                .expect("delta");
        assert_eq!(op, Some(0));
        assert_eq!(payload.unwrap().0["val"], "v2");

        let target = Spi::get_one_with_args::<String>(
            "SELECT tierdb_delete($1, '1'::jsonb, 50)",
            &[oid.into()],
        )
        .expect("delete")
        .unwrap();
        assert_eq!(target, "hot");
        assert_eq!(count("SELECT count(*) FROM public.events"), 0);
        let op = Spi::get_one::<i16>("SELECT op FROM tierdb.delta WHERE pk = '1'")
            .expect("delta")
            .unwrap();
        assert_eq!(op, 1, "the trigger records the tombstone");
    }

    #[pg_test]
    fn test_keep_heap_attach_is_idempotent_and_detach_drops_it() {
        let oid = seed_keep_heap_table();
        let msg = Spi::get_one_with_args::<String>(
            "SELECT tierdb_attach_cold_mirror($1, 'public.events_p0'::regclass)",
            &[oid.into()],
        )
        .expect("re-attach")
        .unwrap();
        assert!(msg.contains("already mirrored"), "{msg}");

        let dropped =
            Spi::get_one_with_args::<i64>("SELECT tierdb_detach_cold_mirror($1)", &[oid.into()])
                .expect("detach")
                .unwrap();
        assert_eq!(dropped, 1);
        Spi::run("INSERT INTO public.events VALUES (9, 50, 'unmirrored')").expect("insert");
        assert_eq!(
            count("SELECT count(*) FROM tierdb.delta"),
            0,
            "trigger is gone"
        );
    }

    #[pg_test]
    fn test_attach_cold_mirror_refuses_non_keep_heap_tables() {
        let oid = seed_spill_table();
        let res = std::panic::catch_unwind(|| {
            Spi::run_with_args(
                "SELECT tierdb_attach_cold_mirror($1, 'public.events_p1'::regclass)",
                &[oid.into()],
            )
        });
        assert!(res.is_err(), "the mirror applies only to keep-heap tables");
    }

    #[pg_test]
    fn test_keep_heap_seam_read_sees_the_plain_update_once() {
        seed_keep_heap_table();
        Spi::run("CREATE SCHEMA duckdb").expect("stub schema");
        Spi::run(
            "CREATE FUNCTION duckdb.query(sql text) RETURNS SETOF jsonb \
             LANGUAGE sql AS 'SELECT NULL::jsonb WHERE false'",
        )
        .expect("empty cold tier stub");
        Spi::run("INSERT INTO public.events VALUES (1, 50, 'v1'), (2, 150, 'hot')")
            .expect("seed both tiers");
        Spi::run("UPDATE public.events SET val = 'v2' WHERE id = 1").expect("plain update");

        Spi::run("SET tierdb.transparent_reads = on").expect("guc");
        let val = Spi::get_one::<String>("SELECT val FROM public.events WHERE id = 1")
            .expect("seam read")
            .unwrap();
        assert_eq!(val, "v2", "the delta carries the update into the seam view");
        assert_eq!(
            count("SELECT count(*) FROM public.events"),
            2,
            "kept heap rows below the cut-line are never scanned twice"
        );
    }

    fn explain(sql: &str) -> String {
        Spi::get_one_with_args::<String>(
            "SELECT string_agg(tierdb_explain, E'\n') FROM tierdb_explain($1)",
            &[sql.into()],
        )
        .expect("explain")
        .unwrap()
    }

    #[pg_test]
    fn test_explain_select_reports_the_two_tier_read() {
        seed_registered_table();
        let report = explain("SELECT * FROM public.events WHERE id = 2");
        assert!(report.contains("two-tier read"), "{report}");
        assert!(
            report.contains("heap partitions at event_time >= 100"),
            "{report}"
        );
        assert!(report.contains("pinned at snapshot 7"), "{report}");
        assert!(
            report.contains("tierdb.transparent_reads is off"),
            "the fixture turns the hook off, the report says so:\n{report}"
        );
    }

    #[pg_test]
    fn test_explain_select_without_registered_tables_is_untouched() {
        seed_registered_table();
        Spi::run("CREATE TABLE public.plain (id int)").expect("plain table");
        let report = explain("SELECT * FROM public.plain");
        assert!(report.contains("no registered tables"), "{report}");
    }

    #[pg_test]
    fn test_explain_update_hot_is_a_passthrough() {
        seed_registered_table();
        let report = explain("UPDATE public.events SET val = 'x' WHERE event_time >= 100");
        assert!(report.contains("provably hot"), "{report}");
        assert!(report.contains("passes through untouched"), "{report}");
    }

    #[pg_test]
    fn test_explain_update_by_pk_describes_both_halves() {
        seed_registered_table();
        let report = explain("UPDATE public.events SET val = 'x' WHERE id = 1");
        assert!(report.contains("may touch both tiers"), "{report}");
        assert!(report.contains("hot half"), "{report}");
        assert!(report.contains("cold half"), "{report}");
    }

    #[pg_test]
    fn test_explain_delete_below_the_cutline_is_cold() {
        seed_registered_table();
        let report = explain("DELETE FROM public.events WHERE event_time < 50");
        assert!(report.contains("provably cold"), "{report}");
    }

    #[pg_test]
    fn test_explain_update_reports_rejections_without_raising() {
        seed_registered_table();
        let report = explain("UPDATE public.events SET id = 9 WHERE id = 1");
        assert!(
            report.contains("rejected: SET on primary key column 'id'"),
            "{report}"
        );
    }

    #[pg_test]
    fn test_explain_insert_counts_literal_rows_per_destination() {
        seed_registered_table();
        let report = explain("INSERT INTO public.events VALUES (8, 130, 'hot'), (9, 50, 'cold')");
        assert!(
            report.contains("1 row(s) >= 100: heap partitions"),
            "{report}"
        );
        assert!(report.contains("1 row(s) < 100: tierdb.delta"), "{report}");
        assert!(
            report.contains("no spill partition"),
            "the fixture never enabled transparent writes:\n{report}"
        );
    }

    #[pg_test]
    fn test_explain_insert_without_literals_states_the_rule() {
        seed_registered_table();
        let report = explain(
            "INSERT INTO public.events SELECT id + 100, event_time, val FROM public.events",
        );
        assert!(
            report.contains("rows with event_time >= 100: heap partitions"),
            "{report}"
        );
        assert!(
            report.contains("rows with event_time < 100: tierdb.delta"),
            "{report}"
        );
    }

    #[pg_test]
    fn test_explain_never_writes_anything() {
        seed_registered_table();
        explain("INSERT INTO public.events VALUES (8, 130, 'hot')");
        explain("DELETE FROM public.events WHERE event_time < 50");
        assert_eq!(count("SELECT count(*) FROM public.events"), 0);
        assert_eq!(count("SELECT count(*) FROM tierdb.delta"), 0);
    }

    // Canonical micros for the timestamptz fixtures: T = 2026-07-01 00:00:00+00.
    const TS_CUT: i64 = 1_782_864_000_000_000;

    fn seed_timestamptz_table() -> pg_sys::Oid {
        Spi::run("SET tierdb.transparent_reads = off").expect("guc");
        Spi::run(CATALOG_DDL).expect("catalog.sql applies");
        Spi::run(
            "CREATE TABLE public.metrics (id bigint PRIMARY KEY, ts timestamptz NOT NULL, val text)",
        )
        .expect("hot relation");
        Spi::run(
            "INSERT INTO tierdb.tables (table_id, schema_name, table_name, primary_key_cols,
                                       tier_key_col, tier_key_type, partition_scheme, lake_format, lake_table_ref)
             SELECT 'public.metrics'::regclass::oid::bigint, 'public', 'metrics', ARRAY['id'], 'ts',
                    'timestamptz', '{\"unit\":\"day\"}', 'iceberg', 'warehouse.metrics'",
        )
        .expect("register");
        Spi::run(&format!(
            "INSERT INTO tierdb.cutline (table_id, tier_key_hi, lake_snapshot_id, lake_props)
             SELECT 'public.metrics'::regclass::oid::bigint, {TS_CUT}, 7,
                    '{{\"metadata_location\":\"/wh/metrics/metadata/00002-abc.metadata.json\"}}'",
        ))
        .expect("cutline");
        Spi::get_one::<pg_sys::Oid>("SELECT 'public.metrics'::regclass::oid")
            .expect("oid")
            .unwrap()
    }

    fn seed_timestamptz_transparent() -> pg_sys::Oid {
        let oid = seed_timestamptz_table();
        Spi::run("CREATE SCHEMA duckdb").expect("stub schema");
        Spi::run("CREATE TABLE public.fake_cold (r jsonb)").expect("fake cold tier");
        Spi::run(
            r#"INSERT INTO public.fake_cold VALUES
                 ('{"id": 1, "ts": "2026-06-29T08:00:00+00:00", "val": "a"}'),
                 ('{"id": 2, "ts": "2026-06-30T12:00:00+00:00", "val": "b"}')"#,
        )
        .expect("cold rows");
        Spi::run(
            "CREATE FUNCTION duckdb.query(sql text) RETURNS SETOF jsonb \
             LANGUAGE sql AS 'SELECT r FROM public.fake_cold'",
        )
        .expect("duckdb.query stub");
        // pg_duckdb resolves r['ts']::timestamptz natively, the jsonb stub needs a cast.
        Spi::run(
            "CREATE FUNCTION public.jsonb_to_tstz(j jsonb) RETURNS timestamptz \
             LANGUAGE sql AS 'SELECT (j #>> ''{}'')::timestamptz'",
        )
        .expect("cast fn");
        Spi::run(
            "CREATE CAST (jsonb AS timestamp with time zone) \
             WITH FUNCTION public.jsonb_to_tstz(jsonb)",
        )
        .expect("stub cast");
        Spi::run("INSERT INTO public.metrics VALUES (7, '2026-07-02 09:00:00+00', 'recent')")
            .expect("hot row");
        Spi::run("SET tierdb.transparent_reads = on").expect("guc");
        oid
    }

    #[pg_test]
    fn test_timestamptz_rewrite_scan_renders_native_literals() {
        let oid = seed_timestamptz_table();
        let sql = Spi::get_one_with_args::<String>("SELECT tierdb_rewrite_scan($1)", &[oid.into()])
            .expect("rewrite")
            .unwrap();
        assert!(
            sql.contains("\"ts\" >= TIMESTAMPTZ '2026-07-01 00:00:00.000000+00'"),
            "hot bound is a native literal:\n{sql}"
        );
        assert!(
            sql.contains("\"ts\" < TIMESTAMPTZ '2026-07-01 00:00:00.000000+00'"),
            "cold bound is a native literal:\n{sql}"
        );
        assert!(
            sql.contains("r['ts']::timestamp with time zone"),
            "typed duckdb.row subscript:\n{sql}"
        );
    }

    #[pg_test]
    fn test_timestamptz_transparent_select_spans_both_tiers() {
        seed_timestamptz_transparent();
        assert_eq!(count("SELECT count(*) FROM public.metrics"), 3);
        assert_eq!(
            count("SELECT count(*) FROM public.metrics WHERE ts < '2026-07-01 00:00:00+00'"),
            2
        );
        assert_eq!(
            count("SELECT count(*) FROM public.metrics WHERE id = 2"),
            1,
            "cold-only row visible through plain SQL"
        );
    }

    #[pg_test]
    fn test_timestamptz_upsert_and_native_delete_route_by_encoded_key() {
        let oid = seed_timestamptz_table();
        let target = Spi::get_one_with_args::<String>(
            "SELECT tierdb_upsert($1, '{\"id\": 3, \"ts\": \"2026-06-30T12:00:00+00:00\", \"val\": \"corrected\"}'::jsonb)",
            &[oid.into()],
        )
        .expect("upsert cold")
        .unwrap();
        assert_eq!(target, "delta");
        let tier = Spi::get_one::<i64>("SELECT tier_key FROM tierdb.delta WHERE pk = '3'")
            .expect("delta row")
            .unwrap();
        assert_eq!(tier, TS_CUT - 12 * 3_600_000_000, "canonical micros");

        Spi::run("INSERT INTO public.metrics VALUES (7, '2026-07-02 09:00:00+00', 'recent')")
            .expect("hot row");
        let target = Spi::get_one_with_args::<String>(
            "SELECT tierdb_delete($1, '7', TIMESTAMPTZ '2026-07-02 09:00:00+00')",
            &[oid.into()],
        )
        .expect("native hot delete")
        .unwrap();
        assert_eq!(target, "hot");
        assert_eq!(count("SELECT count(*) FROM public.metrics"), 0);

        let target = Spi::get_one_with_args::<String>(
            "SELECT tierdb_delete($1, '2', TIMESTAMPTZ '2026-06-30 12:00:00+00')",
            &[oid.into()],
        )
        .expect("native cold delete")
        .unwrap();
        assert_eq!(target, "delta");
    }

    #[pg_test]
    fn test_timestamptz_update_provably_hot_stays_untouched() {
        seed_timestamptz_transparent();
        let id = Spi::get_one::<i64>(
            "UPDATE public.metrics SET val = 'seen' \
             WHERE ts >= '2026-07-01 00:00:00+00' RETURNING id",
        )
        .expect("timestamptz const proves hot")
        .unwrap();
        assert_eq!(id, 7);
        assert_eq!(count("SELECT count(*) FROM tierdb.delta"), 0);
    }

    #[pg_test]
    fn test_timestamptz_cold_update_writes_canonical_delta() {
        seed_timestamptz_transparent();
        Spi::run("UPDATE public.metrics SET val = 'fixed' WHERE id = 2").expect("cold update");
        let (tier, payload) = Spi::get_two::<i64, pgrx::JsonB>(
            "SELECT tier_key, payload FROM tierdb.delta WHERE pk = '2'",
        )
        .expect("delta row");
        assert_eq!(tier, Some(TS_CUT - 12 * 3_600_000_000));
        assert_eq!(payload.unwrap().0["val"], "fixed");
        let val = Spi::get_one::<String>("SELECT val FROM public.metrics WHERE id = 2")
            .expect("read")
            .unwrap();
        assert_eq!(
            val, "fixed",
            "the correction reads back through the overlay"
        );
    }

    #[pg_test]
    fn test_read_pin_lifecycle_records_t_and_s() {
        seed_catalog();
        let pins = PgReadPins::default();
        let cut = PgCatalog.current(TABLE_A).expect("cutline");

        let pin = pins.acquire(TABLE_A, &cut).expect("acquire");
        let (s, t) = Spi::get_two_with_args::<i64, i64>(
            "SELECT pinned_lake_snapshot_id, pinned_tier_key_hi \
               FROM tierdb.read_pins WHERE pin_id = $1",
            &[pin.0.into()],
        )
        .expect("pin row");
        assert_eq!(
            s,
            Some(7),
            "pin freezes S, gating delta/snapshot reclamation"
        );
        assert_eq!(t, Some(100), "pin freezes T, gating partition drop");

        pins.release(pin).expect("release");
        let left = Spi::get_one::<i64>("SELECT count(*) FROM tierdb.read_pins")
            .expect("count")
            .unwrap();
        assert_eq!(left, 0, "released pin no longer holds the horizon");
    }
}

/// Required by `cargo pgrx test`.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec!["unix_socket_directories = '/tmp'"]
    }
}
