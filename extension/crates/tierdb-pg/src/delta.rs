//! Write-path internals shared by the explicit router functions and the
//! transparent-insert spill route: table write metadata, row key extraction,
//! and the retention floor check. The delta write SQL itself lives in core.

use pgrx::prelude::*;
use tierdb_core::domain::TableId;
use tierdb_core::{Result, TierDBError, TierKeyType};

use crate::catalog::{catalog_err, PgCatalog};

pub(crate) struct WriteMeta {
    pub schema: String,
    pub table: String,
    pub pk_cols: Vec<String>,
    pub tier_key_col: String,
    pub tier_key_type: TierKeyType,
    pub keep_heap: bool,
}

const WRITE_META_SQL: &str = "SELECT schema_name, table_name, primary_key_cols, tier_key_col, \
            tier_key_type, keep_heap \
     FROM tierdb.tables WHERE table_id = $1";

pub(crate) fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub(crate) fn write_meta(table: TableId) -> Result<WriteMeta> {
    Spi::connect(|client| {
        let mut rows = client
            .select(WRITE_META_SQL, Some(1), &[(table.0 as i64).into()])
            .map_err(catalog_err)?;
        let row = rows.next().ok_or(TierDBError::UnknownTable(table))?;
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
        let tier_type = row
            .get_by_name::<String, _>("tier_key_type")
            .map_err(catalog_err)?
            .unwrap_or_else(|| "bigint".into());
        let keep_heap = row
            .get_by_name::<bool, _>("keep_heap")
            .map_err(catalog_err)?
            .unwrap_or(false);
        if pk_cols.is_empty() {
            return Err(TierDBError::Planning(format!(
                "table {table:?} has no primary key columns"
            )));
        }
        Ok(WriteMeta {
            schema,
            table: name,
            pk_cols,
            tier_key_col: tier,
            tier_key_type: TierKeyType::from_name(&tier_type)?,
            keep_heap,
        })
    })
}

pub(crate) fn json_field_as_text(row: &pgrx::JsonB, field: &str) -> Result<String> {
    json_value_as_text(row.0.get(field), field)
}

pub(crate) fn json_value_as_text(value: Option<&serde_json::Value>, what: &str) -> Result<String> {
    match value {
        Some(serde_json::Value::String(s)) => Ok(s.clone()),
        Some(serde_json::Value::Number(n)) => Ok(n.to_string()),
        Some(serde_json::Value::Bool(b)) => Ok(b.to_string()),
        Some(other) => Err(TierDBError::Planning(format!(
            "'{what}' has unsupported json type: {other}"
        ))),
        None => Err(TierDBError::Planning(format!(
            "row is missing required field '{what}'"
        ))),
    }
}

pub(crate) fn pk_values(row: &pgrx::JsonB, pk_cols: &[String]) -> Result<Vec<String>> {
    pk_cols.iter().map(|c| json_field_as_text(row, c)).collect()
}

/// The delete key: an object keyed by pk column, or (single-column keys only)
/// a bare scalar. Returns the per-column values plus the object form used as
/// the tombstone payload.
pub(crate) fn delete_key_values(
    key: &pgrx::JsonB,
    pk_cols: &[String],
) -> Result<(Vec<String>, serde_json::Value)> {
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
        _ => Err(TierDBError::Planning(format!(
            "composite-key delete needs a json object with fields {pk_cols:?}"
        ))),
    }
}

pub(crate) fn tier_key_of(row: &pgrx::JsonB, meta: &WriteMeta) -> Result<i64> {
    json_field_as_text(row, &meta.tier_key_col).and_then(|s| meta.tier_key_type.encode_text(&s))
}

pub(crate) fn or_error<T>(r: Result<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => error!("tierdb: {e}"),
    }
}

/// Rows below the retention line no longer exist in the lake, so a delta entry
/// for them could never be folded back. Reject instead of silently resurrecting.
pub(crate) fn check_retention(table: TableId, meta: &WriteMeta, tier_key: i64) {
    let line = or_error(PgCatalog.retention_line(table)).map(|l| l.0);
    if tierdb_core::dml::retention_rejects(tier_key, line) {
        error!(
            "tierdb: tier_key {} is below the retention line {}, \
             rows this old have been expired from the lake",
            meta.tier_key_type.pg_literal(tier_key),
            meta.tier_key_type.pg_literal(line.unwrap_or_default())
        );
    }
}
