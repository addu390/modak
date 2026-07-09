//! Write router. `tierdb_upsert` and `tierdb_delete` route each record by
//! tier-key vs the cut-line, recent rows to the heap and cold-targeting
//! corrections to `tierdb.delta` entries that compaction later folds.

use pgrx::prelude::*;
use tierdb_core::domain::{Cutline, RouteTarget, TableId, TierKey};
use tierdb_core::planner::route;
use tierdb_core::ports::CutlineReader;
use tierdb_core::sqlgen::encode_pk;

use tierdb_core::dml::{
    delta_write_sql, heap_delete_by_pk_sql, heap_insert_from_jsonb_sql, ColdSink,
    DELTA_OP_TOMBSTONE, DELTA_OP_UPSERT,
};
use tierdb_core::mode::Mode;

use crate::catalog::{catalog_err, PgCatalog};
use crate::delta::{
    check_retention, delete_key_values, ident, or_error, pk_values, tier_key_of, write_meta,
    WriteMeta,
};

#[pg_extern]
fn tierdb_upsert(table: pg_sys::Oid, row: pgrx::JsonB) -> String {
    let t = TableId(table.into());
    let meta = or_error(write_meta(t));
    let cut = or_error(PgCatalog.current(t));

    let tier_key = or_error(tier_key_of(&row, &meta));
    let pk_vals = or_error(pk_values(&row, &meta.pk_cols));
    let pk = encode_pk(&pk_vals);

    if let mode @ Mode::Direct { .. } = or_error(meta.mode()) {
        return direct_upsert(t, &meta, mode, &cut, tier_key, &pk_vals, row);
    }

    if meta.keep_heap {
        heap_delete_by_pk(&meta, &pk_vals, None);
        let sql = heap_insert_from_jsonb_sql(&meta.schema, &meta.table);
        or_error(Spi::run_with_args(&sql, &[row.into()]).map_err(catalog_err));
        return "hot".to_string();
    }

    match route(TierKey(tier_key), &cut) {
        RouteTarget::Hot => {
            let sql = heap_insert_from_jsonb_sql(&meta.schema, &meta.table);
            or_error(Spi::run_with_args(&sql, &[row.into()]).map_err(catalog_err));
            "hot".to_string()
        }
        RouteTarget::Cold => {
            check_retention(t, &meta, tier_key);
            or_error(
                Spi::run_with_args(
                    &delta_write_sql(),
                    &[
                        (t.0 as i64).into(),
                        pk.into(),
                        DELTA_OP_UPSERT.into(),
                        tier_key.into(),
                        row.into(),
                    ],
                )
                .map_err(catalog_err),
            );
            "delta".to_string()
        }
    }
}

#[pg_extern]
fn tierdb_delete(table: pg_sys::Oid, key: pgrx::JsonB, tier_key: i64) -> String {
    let t = TableId(table.into());
    let meta = or_error(write_meta(t));
    let cut = or_error(PgCatalog.current(t));

    let (values, key_payload) = or_error(delete_key_values(&key, &meta.pk_cols));

    if let mode @ Mode::Direct { .. } = or_error(meta.mode()) {
        return direct_delete(t, &meta, mode, &cut, tier_key, values);
    }

    if meta.keep_heap {
        heap_delete_by_pk(&meta, &values, None);
        return "hot".to_string();
    }

    match route(TierKey(tier_key), &cut) {
        RouteTarget::Hot => {
            heap_delete_by_pk(&meta, &values, Some(cut.t.0));
            "hot".to_string()
        }
        RouteTarget::Cold => {
            check_retention(t, &meta, tier_key);
            let pk = encode_pk(&values);
            or_error(
                Spi::run_with_args(
                    &delta_write_sql(),
                    &[
                        (t.0 as i64).into(),
                        pk.into(),
                        DELTA_OP_TOMBSTONE.into(),
                        tier_key.into(),
                        pgrx::JsonB(key_payload).into(),
                    ],
                )
                .map_err(catalog_err),
            );
            "delta".to_string()
        }
    }
}

extension_sql!(
    r#"
CREATE FUNCTION tierdb_delete(oid, jsonb, timestamptz) RETURNS text
LANGUAGE sql AS 'SELECT tierdb_delete($1, $2, (extract(epoch from $3) * 1000000)::bigint)';
CREATE FUNCTION tierdb_delete(oid, jsonb, timestamp) RETURNS text
LANGUAGE sql AS 'SELECT tierdb_delete($1, $2, (extract(epoch from $3) * 1000000)::bigint)';
CREATE FUNCTION tierdb_delete(oid, jsonb, date) RETURNS text
LANGUAGE sql AS 'SELECT tierdb_delete($1, $2, ($3 - DATE ''1970-01-01'')::bigint)';
"#,
    name = "tierdb_delete_native_overloads",
    requires = [tierdb_delete]
);

/// Heap leg first, then the synchronous lake commit; a lake failure aborts
/// the transaction and rolls the heap leg back with it.
fn direct_upsert(
    t: TableId,
    meta: &WriteMeta,
    mode: Mode,
    cut: &Cutline,
    tier_key: i64,
    pk_vals: &[String],
    row: pgrx::JsonB,
) -> String {
    let plan = mode.plan_insert(route(TierKey(tier_key), cut));
    if plan.check_retention {
        check_retention(t, meta, tier_key);
    }

    if plan.to_heap {
        if plan.cold.is_some() {
            heap_delete_by_pk(meta, pk_vals, None);
        }
        let sql = heap_insert_from_jsonb_sql(&meta.schema, &meta.table);
        or_error(
            Spi::run_with_args(&sql, &[pgrx::JsonB(row.0.clone()).into()]).map_err(catalog_err),
        );
    }

    match plan.cold {
        Some(ColdSink::Lake) => {
            or_error(crate::lake::upsert_rows(t, std::slice::from_ref(&row.0)));
            "lake".to_string()
        }
        Some(ColdSink::Delta) => error!("tierdb: direct tables never buffer in tierdb.delta"),
        None => "hot".to_string(),
    }
}

fn direct_delete(
    t: TableId,
    meta: &WriteMeta,
    mode: Mode,
    cut: &Cutline,
    tier_key: i64,
    values: Vec<String>,
) -> String {
    let plan = mode.plan_delete(route(TierKey(tier_key), cut));
    if plan.check_retention {
        check_retention(t, meta, tier_key);
    }
    if plan.from_heap {
        heap_delete_by_pk(meta, &values, plan.heap_floor.then_some(cut.t.0));
    }
    match plan.cold {
        Some(ColdSink::Lake) => {
            or_error(crate::lake::delete_keys(t, &[values], Some(cut.t)));
            "lake".to_string()
        }
        Some(ColdSink::Delta) => error!("tierdb: direct tables never buffer in tierdb.delta"),
        None => "hot".to_string(),
    }
}

fn heap_delete_by_pk(meta: &WriteMeta, values: &[String], tier_floor: Option<i64>) {
    let floor = tier_floor.map(|t| {
        format!(
            "{} >= {}",
            ident(&meta.tier_key_col),
            meta.tier_key_type.pg_literal(t)
        )
    });
    let sql = heap_delete_by_pk_sql(&meta.schema, &meta.table, &meta.pk_cols, floor.as_deref());
    let args: Vec<pgrx::datum::DatumWithOid> = values.iter().map(|v| v.clone().into()).collect();
    or_error(Spi::run_with_args(&sql, &args).map_err(catalog_err));
}
