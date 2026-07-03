//! Write router. `modak_upsert` and `modak_delete` route each record by
//! tier-key vs the cut-line, recent rows to the heap and cold-targeting
//! corrections to `modak.delta` entries that compaction later folds.

use modak_core::domain::{RouteTarget, TableId, TierKey};
use modak_core::planner::route;
use modak_core::ports::CutlineReader;
use modak_core::sqlgen::encode_pk;
use pgrx::prelude::*;

use crate::catalog::{catalog_err, PgCatalog};
use crate::delta::{
    check_retention, delete_key_values, ident, or_error, pk_values, tier_key_of, write_meta,
    TOMBSTONE_DELTA_SQL, UPSERT_DELTA_SQL,
};

/// Routes one full row image. Returns which tier took it: `hot` or `delta`.
#[pg_extern]
fn modak_upsert(table: pg_sys::Oid, row: pgrx::JsonB) -> String {
    let t = TableId(table.into());
    let meta = or_error(write_meta(t));
    let cut = or_error(PgCatalog.current(t));

    let tier_key = or_error(tier_key_of(&row, &meta.tier_key_col));
    let pk = encode_pk(&or_error(pk_values(&row, &meta.pk_cols)));

    match route(TierKey(tier_key), &cut) {
        RouteTarget::Hot => {
            let sql = format!(
                "INSERT INTO {}.{} SELECT * FROM jsonb_populate_record(NULL::{}.{}, $1)",
                ident(&meta.schema),
                ident(&meta.table),
                ident(&meta.schema),
                ident(&meta.table),
            );
            or_error(Spi::run_with_args(&sql, &[row.into()]).map_err(catalog_err));
            "hot".to_string()
        }
        RouteTarget::Delta => {
            check_retention(t, tier_key);
            or_error(
                Spi::run_with_args(
                    UPSERT_DELTA_SQL,
                    &[(t.0 as i64).into(), pk.into(), tier_key.into(), row.into()],
                )
                .map_err(catalog_err),
            );
            "delta".to_string()
        }
    }
}

/// Routes a delete. The key is a json object of the pk fields, with a bare
/// scalar accepted for single-column keys. The tier-key is explicit because a
/// cold target has no heap row to look it up from. Returns `hot` or `delta`.
#[pg_extern]
fn modak_delete(table: pg_sys::Oid, key: pgrx::JsonB, tier_key: i64) -> String {
    let t = TableId(table.into());
    let meta = or_error(write_meta(t));
    let cut = or_error(PgCatalog.current(t));

    let (values, key_payload) = or_error(delete_key_values(&key, &meta.pk_cols));

    match route(TierKey(tier_key), &cut) {
        RouteTarget::Hot => {
            let conditions = meta
                .pk_cols
                .iter()
                .enumerate()
                .map(|(i, c)| format!("{}::text = ${}", ident(c), i + 1))
                .collect::<Vec<_>>()
                .join(" AND ");
            // The tier bound scopes the statement to the hot tier, so the
            // transparent-DML rewrite proves it hot and leaves it alone.
            let sql = format!(
                "DELETE FROM {}.{} WHERE {conditions} AND {} >= {}",
                ident(&meta.schema),
                ident(&meta.table),
                ident(&meta.tier_key_col),
                cut.t.0,
            );
            let args: Vec<pgrx::datum::DatumWithOid> =
                values.into_iter().map(|v| v.into()).collect();
            or_error(Spi::run_with_args(&sql, &args).map_err(catalog_err));
            "hot".to_string()
        }
        RouteTarget::Delta => {
            check_retention(t, tier_key);
            let pk = encode_pk(&values);
            or_error(
                Spi::run_with_args(
                    TOMBSTONE_DELTA_SQL,
                    &[
                        (t.0 as i64).into(),
                        pk.into(),
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
