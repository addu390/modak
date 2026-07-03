//! Write router: `modak_upsert` / `modak_delete` route each record by tier-key
//! vs the cut-line `T` — recent rows go to the heap, cold-targeting corrections
//! become `modak.delta` entries that compaction later folds. Explicit functions
//! because a cold row has no heap row for a trigger to fire on.

use modak_core::domain::{RouteTarget, TableId, TierKey};
use modak_core::planner::route;
use modak_core::ports::CutlineReader;
use modak_core::sqlgen::encode_pk;
use modak_core::{ModakError, Result};
use pgrx::prelude::*;

use crate::catalog::{catalog_err, PgCatalog};

struct WriteMeta {
    schema: String,
    table: String,
    pk_cols: Vec<String>,
    tier_key_col: String,
}

const WRITE_META_SQL: &str = "SELECT schema_name, table_name, primary_key_cols, tier_key_col \
     FROM modak.tables WHERE table_id = $1";

// Sequence-assigned versions: newest-wins ordering, gap-tolerant fold clears.
const UPSERT_DELTA_SQL: &str = "INSERT INTO modak.delta AS d \
       (table_id, pk, op, tier_key, version, payload) \
     VALUES ($1, $2, 0, $3, nextval('modak.delta_version'), $4) \
     ON CONFLICT (table_id, pk) DO UPDATE \
       SET op = 0, tier_key = EXCLUDED.tier_key, version = EXCLUDED.version, \
           payload = EXCLUDED.payload, updated_at = now() \
     WHERE EXCLUDED.version >= d.version";

// Tombstones keep the key fields as payload: the equality delete needs typed values.
const TOMBSTONE_DELTA_SQL: &str = "INSERT INTO modak.delta AS d \
       (table_id, pk, op, tier_key, version, payload) \
     VALUES ($1, $2, 1, $3, nextval('modak.delta_version'), $4) \
     ON CONFLICT (table_id, pk) DO UPDATE \
       SET op = 1, tier_key = EXCLUDED.tier_key, version = EXCLUDED.version, \
           payload = EXCLUDED.payload, updated_at = now() \
     WHERE EXCLUDED.version >= d.version";

fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn write_meta(table: TableId) -> Result<WriteMeta> {
    Spi::connect(|client| {
        let mut rows = client
            .select(WRITE_META_SQL, Some(1), &[(table.0 as i64).into()])
            .map_err(catalog_err)?;
        let row = rows.next().ok_or(ModakError::UnknownTable(table))?;
        let schema = row
            .get_by_name::<String, _>("schema_name")
            .map_err(catalog_err)?
            .ok_or_else(|| catalog_err("schema_name is NULL"))?;
        let name = row
            .get_by_name::<String, _>("table_name")
            .map_err(catalog_err)?
            .ok_or_else(|| catalog_err("table_name is NULL"))?;
        let pk_cols = row
            .get_by_name::<Vec<String>, _>("primary_key_cols")
            .map_err(catalog_err)?
            .ok_or_else(|| catalog_err("primary_key_cols is NULL"))?;
        let tier = row
            .get_by_name::<String, _>("tier_key_col")
            .map_err(catalog_err)?
            .ok_or_else(|| catalog_err("tier_key_col is NULL"))?;
        if pk_cols.is_empty() {
            return Err(ModakError::Planning(format!(
                "table {table:?} has no primary key columns"
            )));
        }
        Ok(WriteMeta {
            schema,
            table: name,
            pk_cols,
            tier_key_col: tier,
        })
    })
}

fn json_field_as_text(row: &pgrx::JsonB, field: &str) -> Result<String> {
    json_value_as_text(row.0.get(field), field)
}

fn json_value_as_text(value: Option<&serde_json::Value>, what: &str) -> Result<String> {
    match value {
        Some(serde_json::Value::String(s)) => Ok(s.clone()),
        Some(serde_json::Value::Number(n)) => Ok(n.to_string()),
        Some(serde_json::Value::Bool(b)) => Ok(b.to_string()),
        Some(other) => Err(ModakError::Planning(format!(
            "'{what}' has unsupported json type: {other}"
        ))),
        None => Err(ModakError::Planning(format!(
            "row is missing required field '{what}'"
        ))),
    }
}

fn pk_values(row: &pgrx::JsonB, pk_cols: &[String]) -> Result<Vec<String>> {
    pk_cols.iter().map(|c| json_field_as_text(row, c)).collect()
}

/// The delete key: an object keyed by pk column, or (single-column keys only)
/// a bare scalar. Returns the per-column values plus the object form used as
/// the tombstone payload.
fn delete_key(key: &pgrx::JsonB, pk_cols: &[String]) -> Result<(Vec<String>, serde_json::Value)> {
    match &key.0 {
        serde_json::Value::Object(_) => {
            let values = pk_values(key, pk_cols)?;
            Ok((values, key.0.clone()))
        }
        scalar if pk_cols.len() == 1 => {
            let text = json_value_as_text(Some(scalar), &pk_cols[0])?;
            let payload = serde_json::json!({ &pk_cols[0]: scalar });
            Ok((vec![text], payload))
        }
        _ => Err(ModakError::Planning(format!(
            "composite-key delete needs a json object with fields {pk_cols:?}"
        ))),
    }
}

fn or_error<T>(r: Result<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => error!("modak: {e}"),
    }
}

/// Rows below the retention line no longer exist in the lake, so a delta entry
/// for them could never be folded back. Reject instead of silently resurrecting.
fn check_retention(table: TableId, tier_key: i64) {
    if let Some(line) = or_error(PgCatalog.retention_line(table)) {
        if tier_key < line.0 {
            error!(
                "modak: tier_key {tier_key} is below the retention line {} — \
                 rows this old have been expired from the lake",
                line.0
            );
        }
    }
}

/// Routes one full row image. Returns which tier took it: `hot` or `delta`.
#[pg_extern]
fn modak_upsert(table: pg_sys::Oid, row: pgrx::JsonB) -> String {
    let t = TableId(table.into());
    let meta = or_error(write_meta(t));
    let cut = or_error(PgCatalog.current(t));

    let tier_key = or_error(json_field_as_text(&row, &meta.tier_key_col).and_then(|s| {
        s.parse::<i64>().map_err(|_| {
            ModakError::Planning(format!(
                "tier-key field '{}' is not a bigint",
                meta.tier_key_col
            ))
        })
    }));
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

/// Routes a delete. The key is a json object of the pk fields (a bare scalar is
/// accepted for single-column keys); the tier-key is explicit because a cold
/// target has no heap row to look it up from. Returns `hot` or `delta`.
#[pg_extern]
fn modak_delete(table: pg_sys::Oid, key: pgrx::JsonB, tier_key: i64) -> String {
    let t = TableId(table.into());
    let meta = or_error(write_meta(t));
    let cut = or_error(PgCatalog.current(t));

    let (values, key_payload) = or_error(delete_key(&key, &meta.pk_cols));

    match route(TierKey(tier_key), &cut) {
        RouteTarget::Hot => {
            let conditions = meta
                .pk_cols
                .iter()
                .enumerate()
                .map(|(i, c)| format!("{}::text = ${}", ident(c), i + 1))
                .collect::<Vec<_>>()
                .join(" AND ");
            let sql = format!(
                "DELETE FROM {}.{} WHERE {conditions}",
                ident(&meta.schema),
                ident(&meta.table),
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
