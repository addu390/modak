//! The direct-mode cold leg: lake DML committed synchronously through
//! pg_duckdb's `duckdb.raw_query` against the live catalog attachment.

use pgrx::prelude::*;
use tierdb_core::dialect::{duckdb_literal, duckdb_type, lake_row_tuple, PgDuckdb, SqlDialect};
use tierdb_core::domain::{TableId, TierKey};
use tierdb_core::lake::LakeCatalog;
use tierdb_core::read::Cold;
use tierdb_core::sqlgen::lake_commit_lock_sql;
use tierdb_core::table::Table;
use tierdb_core::{Result, TierDBError};

pub(crate) fn ensure_attached(catalog: &LakeCatalog) -> Result<()> {
    raw_query(&catalog.attach_sql())
}

pub(crate) fn upsert_rows(table: TableId, rows: &[serde_json::Value]) -> Result<()> {
    let planned = crate::planner::table_meta(table)?;
    let meta = planned.table;
    let (catalog, cold) = direct_target(&meta)?;

    let tuples: Vec<Vec<String>> = rows
        .iter()
        .map(|r| lake_row_tuple(r, &meta.columns))
        .collect();

    let stmts = PgDuckdb.lake_upsert(&meta, &cold, &tuples)?;
    exec_lake_dml(table, catalog, &stmts)
}

/// Delete by primary key; `keys` are text values in `pk_cols` order.
pub(crate) fn delete_keys(
    table: TableId,
    keys: &[Vec<String>],
    tier_lt: Option<TierKey>,
) -> Result<()> {
    let planned = crate::planner::table_meta(table)?;
    let meta = planned.table;
    let (catalog, cold) = direct_target(&meta)?;

    let pk_types: Vec<String> = meta
        .pk_cols
        .iter()
        .map(|pk| {
            meta.columns
                .iter()
                .find(|c| &c.name == pk)
                .map(|c| duckdb_type(&c.sql_type))
                .ok_or_else(|| {
                    TierDBError::Planning(format!("primary key column '{pk}' is not a heap column"))
                })
        })
        .collect::<Result<_>>()?;

    let pk_rows: Vec<Vec<String>> = keys
        .iter()
        .map(|vals| {
            vals.iter()
                .zip(&pk_types)
                .map(|(v, ty)| duckdb_literal(&serde_json::Value::String(v.clone()), ty))
                .collect()
        })
        .collect();

    let stmt = PgDuckdb.lake_delete(&meta, &cold, &pk_rows, tier_lt)?;
    exec_lake_dml(table, catalog, std::slice::from_ref(&stmt))
}

/// Direct-mode write target: the live catalog and the `Cold::Live` handle over it.
fn direct_target(meta: &Table) -> Result<(&LakeCatalog, Cold)> {
    let catalog = meta.catalog.as_ref().ok_or_else(|| {
        TierDBError::Planning("a lake write was routed to a table without a live catalog".into())
    })?;
    let table_ref = meta.lake_table_ref.as_deref().ok_or_else(|| {
        TierDBError::Planning(format!(
            "direct table {}.{} has no lake_table_ref",
            meta.schema, meta.name
        ))
    })?;
    Ok((
        catalog,
        Cold::Live {
            catalog: catalog.alias().to_string(),
            table_ref: table_ref.to_string(),
        },
    ))
}

/// Attach, take the per-table commit lock, execute. The DuckDB commit rides
/// pg_duckdb's transaction callback, so an abort rolls both sides back.
fn exec_lake_dml(table: TableId, catalog: &LakeCatalog, stmts: &[String]) -> Result<()> {
    if unsafe { pg_sys::RecoveryInProgress() } {
        return Err(TierDBError::Planning(
            "cannot commit a cold lake write on a read-only standby".into(),
        ));
    }

    // pg_duckdb's mixed-write guard rejects a heap write and a DuckDB write
    // in one transaction; SET LOCAL scopes the relaxation to this one.
    Spi::run("SET LOCAL duckdb.unsafe_allow_mixed_transactions = on")
        .map_err(|e| TierDBError::Planning(format!("relaxing the mixed-write guard: {e}")))?;

    ensure_attached(catalog)?;
    Spi::run_with_args(lake_commit_lock_sql(), &[(table.0 as i64).into()])
        .map_err(|e| TierDBError::Planning(format!("lake commit lock failed: {e}")))?;

    for stmt in stmts {
        raw_query(stmt)?;
    }
    Ok(())
}

fn raw_query(sql: &str) -> Result<()> {
    Spi::run_with_args("SELECT duckdb.raw_query($1)", &[sql.into()])
        .map_err(|e| TierDBError::Planning(format!("lake statement failed: {e}")))
}
