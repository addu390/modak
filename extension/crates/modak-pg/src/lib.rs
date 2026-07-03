//! The Modak Postgres extension, built with `pgrx` — a thin set of adapters
//! over the pure [`modak_core`] domain. Coordination with the Java workers is
//! entirely through the `modak` catalog tables (`sql/catalog.sql`); there is no
//! cross-language RPC.

use pgrx::prelude::*;

::pgrx::pg_module_magic!();

pub mod catalog;
pub mod hook;
pub mod pin;
pub mod planner;
pub mod router;

/// Runs at library load — at postmaster start when in shared_preload_libraries
/// (recommended, so transparent reads cover every backend), or at first use of
/// a modak function otherwise.
#[pg_guard]
extern "C-unwind" fn _PG_init() {
    unsafe { hook::init() };
}

#[pg_extern]
fn modak_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use modak_core::domain::{Cutline, DeltaOp, KeyRange, LakeSnapshotId, Pk, TableId, TierKey};
    use modak_core::ports::{CutlineReader, DeltaReader, ReadPinRepository};
    use modak_core::ModakError;
    use pgrx::prelude::*;

    use crate::catalog::PgCatalog;
    use crate::pin::PgReadPins;

    const CATALOG_DDL: &str = include_str!("../../../../sql/catalog.sql");

    const TABLE_A: TableId = TableId(90_001);

    fn seed_catalog() {
        Spi::run(CATALOG_DDL).expect("catalog.sql applies");
        Spi::run(
            "INSERT INTO modak.tables (table_id, schema_name, table_name, primary_key_cols,
                                       tier_key_col, partition_scheme, lake_format, lake_table_ref)
             VALUES (90001, 'public', 'events', ARRAY['id'], 'event_time',
                     '{\"unit\":\"hour\"}', 'iceberg', 'warehouse.events')",
        )
        .expect("register table");
        Spi::run("INSERT INTO modak.cutline (table_id, tier_key_hi, lake_snapshot_id) VALUES (90001, 100, 7)")
            .expect("init cutline");
    }

    #[pg_test]
    fn test_modak_version() {
        assert_eq!(crate::modak_version(), env!("CARGO_PKG_VERSION"));
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
            Err(ModakError::UnknownTable(t)) => assert_eq!(t, TableId(1)),
            other => panic!("expected UnknownTable, got {other:?}"),
        }
    }

    #[pg_test]
    fn test_delta_overlay_reads_ops_and_prunes_by_range() {
        seed_catalog();
        Spi::run(
            "INSERT INTO modak.delta (table_id, pk, op, tier_key, version, payload) VALUES
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
            "no corrections ⇒ empty overlay (the common case)"
        );
    }

    fn seed_registered_table() -> pg_sys::Oid {
        // These tests exercise the explicit protocol; keep the hook off their SELECTs.
        Spi::run("SET modak.transparent_reads = off").expect("guc");
        Spi::run(CATALOG_DDL).expect("catalog.sql applies");
        Spi::run(
            "CREATE TABLE public.events (id bigint PRIMARY KEY, event_time bigint NOT NULL, val text)",
        )
        .expect("hot relation");
        Spi::run(
            "INSERT INTO modak.tables (table_id, schema_name, table_name, primary_key_cols,
                                       tier_key_col, partition_scheme, lake_format, lake_table_ref, lake_props)
             SELECT 'public.events'::regclass::oid::bigint, 'public', 'events', ARRAY['id'], 'event_time',
                    '{\"unit\":\"hour\"}', 'iceberg', 'warehouse.events',
                    '{\"metadata_location\":\"/wh/events/metadata/00002-abc.metadata.json\"}'",
        )
        .expect("register");
        Spi::run(
            "INSERT INTO modak.cutline (table_id, tier_key_hi, lake_snapshot_id)
             SELECT 'public.events'::regclass::oid::bigint, 100, 7",
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
            "SELECT pin_id, tier_key_hi FROM modak_read_begin($1)",
            &[oid.into()],
        )
        .expect("read_begin");
        assert_eq!(t, Some(100));
        let held = Spi::get_one::<i64>("SELECT count(*) FROM modak.read_pins")
            .expect("count")
            .unwrap();
        assert_eq!(held, 1, "pin holds the horizon");
        Spi::run_with_args("SELECT modak_read_end($1)", &[pin.unwrap().into()]).expect("read_end");
        let held = Spi::get_one::<i64>("SELECT count(*) FROM modak.read_pins")
            .expect("count")
            .unwrap();
        assert_eq!(held, 0);
    }

    #[pg_test]
    fn test_rewrite_scan_binds_pinned_constants_and_real_columns() {
        let oid = seed_registered_table();
        let sql = Spi::get_one_with_args::<String>("SELECT modak_rewrite_scan($1)", &[oid.into()])
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
        Spi::run("UPDATE modak.tables SET lake_props = NULL").expect("clear lake_props");
        let res = std::panic::catch_unwind(|| {
            Spi::get_one_with_args::<String>("SELECT modak_rewrite_scan($1)", &[oid.into()])
        });
        assert!(
            res.is_err(),
            "no committed snapshot ⇒ error, not a silent wrong scan"
        );
    }

    #[pg_test]
    fn test_upsert_routes_by_tier_key_against_the_cutline() {
        let oid = seed_registered_table();

        let target = Spi::get_one_with_args::<String>(
            "SELECT modak_upsert($1, '{\"id\": 7, \"event_time\": 150, \"val\": \"recent\"}'::jsonb)",
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
            "SELECT modak_upsert($1, '{\"id\": 3, \"event_time\": 50, \"val\": \"corrected\"}'::jsonb)",
            &[oid.into()],
        )
        .expect("upsert cold")
        .unwrap();
        assert_eq!(target, "delta");
        let (op, payload) =
            Spi::get_two::<i16, pgrx::JsonB>("SELECT op, payload FROM modak.delta WHERE pk = '3'")
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
                    "SELECT modak_upsert($1, '{{\"id\": 3, \"event_time\": 50, \"val\": \"{val}\"}}'::jsonb)"
                ),
                &[oid.into()],
            )
            .expect("upsert");
        }
        let (n, payload) = Spi::get_two::<i64, pgrx::JsonB>(
            "SELECT (SELECT count(*) FROM modak.delta), (SELECT payload FROM modak.delta WHERE pk = '3')",
        )
        .expect("delta");
        assert_eq!(n, Some(1), "one row per pk — corrections collapse");
        assert_eq!(payload.unwrap().0["val"], "second");
    }

    #[pg_test]
    fn test_delete_routes_hot_dml_or_cold_tombstone() {
        let oid = seed_registered_table();
        Spi::run("INSERT INTO public.events VALUES (7, 150, 'recent')").unwrap();

        let target =
            Spi::get_one_with_args::<String>("SELECT modak_delete($1, '7', 150)", &[oid.into()])
                .expect("hot delete")
                .unwrap();
        assert_eq!(target, "hot");
        let left = Spi::get_one::<i64>("SELECT count(*) FROM public.events WHERE id = 7")
            .expect("count")
            .unwrap();
        assert_eq!(left, 0);

        let target =
            Spi::get_one_with_args::<String>("SELECT modak_delete($1, '3', 50)", &[oid.into()])
                .expect("cold delete")
                .unwrap();
        assert_eq!(target, "delta");
        let (op, tier) =
            Spi::get_two::<i16, i64>("SELECT op, tier_key FROM modak.delta WHERE pk = '3'")
                .expect("tombstone");
        assert_eq!(op, Some(1), "cold delete is a tombstone, not DML");
        assert_eq!(tier, Some(50));
    }

    #[pg_test]
    fn test_upsert_below_the_retention_line_is_rejected() {
        let oid = seed_registered_table();
        Spi::run("UPDATE modak.cutline SET retention_line = 40").expect("retention line");

        // At or above the line the delta path still works.
        let target = Spi::get_one_with_args::<String>(
            "SELECT modak_upsert($1, '{\"id\": 3, \"event_time\": 40, \"val\": \"ok\"}'::jsonb)",
            &[oid.into()],
        )
        .expect("upsert at the line")
        .unwrap();
        assert_eq!(target, "delta");

        let res = std::panic::catch_unwind(|| {
            Spi::get_one_with_args::<String>(
                "SELECT modak_upsert($1, '{\"id\": 1, \"event_time\": 10, \"val\": \"gone\"}'::jsonb)",
                &[oid.into()],
            )
        });
        assert!(
            res.is_err(),
            "rows below R are expired from the lake — the correction must be rejected"
        );
    }

    #[pg_test]
    fn test_delete_below_the_retention_line_is_rejected() {
        let oid = seed_registered_table();
        Spi::run("UPDATE modak.cutline SET retention_line = 40").expect("retention line");
        let res = std::panic::catch_unwind(|| {
            Spi::get_one_with_args::<String>("SELECT modak_delete($1, '1', 10)", &[oid.into()])
        });
        assert!(res.is_err(), "below-R tombstones are rejected");
    }

    fn seed_composite_table() -> pg_sys::Oid {
        Spi::run("SET modak.transparent_reads = off").expect("guc");
        Spi::run(CATALOG_DDL).expect("catalog.sql applies");
        Spi::run(
            "CREATE TABLE public.locations (tenant bigint NOT NULL, vin text NOT NULL,
                                            event_time bigint NOT NULL, val text,
                                            PRIMARY KEY (tenant, vin))",
        )
        .expect("hot relation");
        Spi::run(
            "INSERT INTO modak.tables (table_id, schema_name, table_name, primary_key_cols,
                                       tier_key_col, partition_scheme, lake_format, lake_table_ref)
             SELECT 'public.locations'::regclass::oid::bigint, 'public', 'locations',
                    ARRAY['tenant','vin'], 'event_time',
                    '{\"unit\":\"hour\"}', 'iceberg', 'warehouse.locations'",
        )
        .expect("register");
        Spi::run(
            "INSERT INTO modak.cutline (table_id, tier_key_hi, lake_snapshot_id)
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
            "SELECT modak_upsert($1, '{\"tenant\": 1, \"vin\": \"V1\", \"event_time\": 50, \"val\": \"cold\"}'::jsonb)",
            &[oid.into()],
        )
        .expect("upsert cold")
        .unwrap();
        assert_eq!(target, "delta");
        let pk = Spi::get_one::<String>("SELECT pk FROM modak.delta")
            .expect("delta pk")
            .unwrap();
        assert_eq!(pk, "1\u{1f}V1", "canonical joined encoding");
    }

    #[pg_test]
    fn test_composite_delete_takes_a_key_object() {
        let oid = seed_composite_table();
        Spi::run("INSERT INTO public.locations VALUES (1, 'V1', 150, 'recent')").unwrap();

        let target = Spi::get_one_with_args::<String>(
            "SELECT modak_delete($1, '{\"tenant\": 1, \"vin\": \"V1\"}'::jsonb, 150)",
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
            "SELECT modak_delete($1, '{\"tenant\": 2, \"vin\": \"V9\"}'::jsonb, 50)",
            &[oid.into()],
        )
        .expect("cold delete")
        .unwrap();
        assert_eq!(target, "delta");
        let (pk, payload) =
            Spi::get_two::<String, pgrx::JsonB>("SELECT pk, payload FROM modak.delta WHERE op = 1")
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
        Spi::run_with_args("SELECT modak_delete($1, '3', 50)", &[oid.into()]).expect("cold delete");
        let payload = Spi::get_one::<pgrx::JsonB>("SELECT payload FROM modak.delta WHERE pk = '3'")
            .expect("payload")
            .unwrap();
        assert_eq!(payload.0["id"], 3);
    }

    // ── Transparent reads (the planner hook, crate::hook) ──────────────────
    //
    // duckdb.query() is faked with a jsonb-returning SQL function, so the whole
    // transparent union runs inside plain Postgres.
    fn seed_transparent() {
        seed_registered_table();
        Spi::run("SET modak.transparent_reads = on").expect("guc");

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

        // A hot row plus an already-tiered row still in the heap: T must dedupe.
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
            "INSERT INTO modak.delta (table_id, pk, op, tier_key, version, payload)
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
            "INSERT INTO modak.delta (table_id, pk, op, tier_key, version)
             SELECT 'public.events'::regclass::oid::bigint, '2', 1, 50, 1",
        )
        .expect("tombstone");
        assert_eq!(count("SELECT count(*) FROM public.events"), 2);
        assert_eq!(count("SELECT count(*) FROM public.events WHERE id = 2"), 0);
    }

    #[pg_test]
    fn test_transparent_guc_off_restores_plain_heap_semantics() {
        seed_transparent();
        Spi::run("SET modak.transparent_reads = off").expect("guc");
        assert_eq!(
            count("SELECT count(*) FROM public.events"),
            2,
            "raw heap rows only"
        );
        Spi::run("SET modak.transparent_reads = on").expect("guc");
        assert_eq!(count("SELECT count(*) FROM public.events"), 3);
    }

    #[pg_test]
    fn test_transparent_read_pins_the_horizon() {
        seed_transparent();
        count("SELECT count(*) FROM public.events");
        let (pins, t) = Spi::get_two::<i64, i64>(
            "SELECT count(*), max(pinned_tier_key_hi) FROM modak.read_pins",
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
    fn test_transparent_dml_keeps_plain_heap_semantics() {
        seed_transparent();
        Spi::run("INSERT INTO public.events VALUES (9, 140, 'new')").expect("insert");
        Spi::run("UPDATE public.events SET val = 'seen' WHERE id = 9").expect("update");
        assert_eq!(count("SELECT count(*) FROM public.events"), 4);
        Spi::run("DELETE FROM public.events WHERE id = 9").expect("delete");
        assert_eq!(count("SELECT count(*) FROM public.events"), 3);
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

    // ── Table modes on the read path ────────────────────────────────────────

    fn seed_mirrored(heap_retention_lag: Option<i64>) {
        seed_transparent();
        match heap_retention_lag {
            Some(lag) => Spi::run(&format!(
                "UPDATE modak.tables SET mode = 'mirrored', heap_retention_lag = {lag}"
            ))
            .expect("mirrored+retention"),
            None => Spi::run("UPDATE modak.tables SET mode = 'mirrored'").expect("mirrored"),
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
        // Frontier ahead of any possible WAL position: the bounded wait passes.
        Spi::run("UPDATE modak.cutline SET replicated_lsn = 9223372036854775807")
            .expect("frontier");
        Spi::run("SET modak.mirrored_reads = 'hybrid'").expect("guc");
        assert_eq!(
            count("SELECT count(*) FROM public.events"),
            3,
            "hybrid seam at max(tier_key): newest row from the heap, bulk from the lake"
        );
    }

    #[pg_test]
    fn test_mirrored_hybrid_heap_fallback_on_frontier_timeout() {
        seed_mirrored(None);
        // Frontier never set (initial copy 'pending'): the wait must time out.
        Spi::run("SET modak.mirrored_reads = 'hybrid'").expect("guc");
        Spi::run("SET modak.mirror_wait_ms = 0").expect("guc");
        assert_eq!(
            count("SELECT count(*) FROM public.events"),
            2,
            "timeout: heap-only read (always correct for no-retention mirrors)"
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
               FROM modak.read_pins WHERE pin_id = $1",
            &[pin.0.into()],
        )
        .expect("pin row");
        assert_eq!(
            s,
            Some(7),
            "pin freezes S — gates delta/snapshot reclamation"
        );
        assert_eq!(t, Some(100), "pin freezes T — gates partition drop");

        pins.release(pin).expect("release");
        let left = Spi::get_one::<i64>("SELECT count(*) FROM modak.read_pins")
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
        // CARGO_TARGET_DIR paths can exceed the 103-byte unix-socket limit; use /tmp.
        vec!["unix_socket_directories = '/tmp'"]
    }
}
