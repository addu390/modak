//! The read protocol as an explicit SQL surface:
//! `modak_read_begin` pins `(T, S)`, `modak_rewrite_scan` returns the unified
//! SELECT, `modak_read_end` releases the pin. All three run inside one
//! transaction so the cut-line, delta, and pin are one consistent MVCC picture.

use modak_core::domain::TableId;
use modak_core::ports::{CutlineReader, DeltaReader, ReadPinRepository};
use modak_core::sqlgen::{render_scan, Column, TableMeta};
use modak_core::{planner as core_planner, ModakError, Result};
use pgrx::prelude::*;

use crate::catalog::{catalog_err, PgCatalog};
use crate::pin::PgReadPins;

const META_SQL: &str = "SELECT schema_name, table_name, primary_key_cols, tier_key_col, \
            lake_props ->> 'metadata_location' AS metadata_location \
     FROM modak.tables WHERE table_id = $1";

const COLUMNS_SQL: &str = "SELECT a.attname::text AS name, \
            format_type(a.atttypid, a.atttypmod) AS sql_type \
     FROM pg_catalog.pg_attribute a \
     WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped \
     ORDER BY a.attnum";

pub(crate) fn table_meta(table: TableId) -> Result<TableMeta> {
    let (schema, name, pk_cols, tier_col, metadata_location) = Spi::connect(|client| {
        let mut rows = client
            .select(META_SQL, Some(1), &[(table.0 as i64).into()])
            .map_err(catalog_err)?;
        let row = rows.next().ok_or(ModakError::UnknownTable(table))?;
        let schema = row
            .get_by_name::<String, _>("schema_name")
            .map_err(catalog_err)?;
        let name = row
            .get_by_name::<String, _>("table_name")
            .map_err(catalog_err)?;
        let pk = row
            .get_by_name::<Vec<String>, _>("primary_key_cols")
            .map_err(catalog_err)?;
        let tier = row
            .get_by_name::<String, _>("tier_key_col")
            .map_err(catalog_err)?;
        let meta = row
            .get_by_name::<String, _>("metadata_location")
            .map_err(catalog_err)?;
        Ok::<_, ModakError>((schema, name, pk, tier, meta))
    })?;

    let schema = schema.ok_or_else(|| catalog_err("schema_name is NULL"))?;
    let name = name.ok_or_else(|| catalog_err("table_name is NULL"))?;
    let pk_cols = pk_cols.ok_or_else(|| catalog_err("primary_key_cols is NULL"))?;
    if pk_cols.is_empty() {
        return Err(ModakError::Planning(format!(
            "table {table:?} has no primary key columns"
        )));
    }
    let metadata_location = metadata_location.ok_or_else(|| {
        ModakError::Planning(
            "cold tier has no committed lake snapshot yet \
             (modak.tables.lake_props->>'metadata_location' is NULL)"
                .into(),
        )
    })?;

    // pg_attribute genuinely keys by oid, only the modak.* contract avoids it.
    let columns = Spi::connect(|client| {
        let rows = client
            .select(COLUMNS_SQL, None, &[pg_sys::Oid::from(table.0).into()])
            .map_err(catalog_err)?;
        let mut cols = Vec::new();
        for row in rows {
            let name = row
                .get_by_name::<String, _>("name")
                .map_err(catalog_err)?
                .ok_or_else(|| catalog_err("pg_attribute.attname is NULL"))?;
            let sql_type = row
                .get_by_name::<String, _>("sql_type")
                .map_err(catalog_err)?
                .ok_or_else(|| catalog_err("format_type returned NULL"))?;
            cols.push(Column { name, sql_type });
        }
        Ok::<_, ModakError>(cols)
    })?;
    if columns.is_empty() {
        return Err(ModakError::Planning(format!(
            "relation {schema}.{name} (oid {}) has no columns, is it a live table?",
            table.0
        )));
    }

    Ok(TableMeta {
        table_id: table,
        hot_schema: schema,
        hot_table: name,
        columns,
        pk_cols,
        tier_key_col: tier_col.ok_or_else(|| catalog_err("tier_key_col is NULL"))?,
        lake_metadata_location: metadata_location,
    })
}

fn or_error<T>(r: Result<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => error!("modak: {e}"),
    }
}

#[pg_extern]
fn modak_read_begin(
    table: pg_sys::Oid,
) -> TableIterator<
    'static,
    (
        name!(pin_id, i64),
        name!(tier_key_hi, i64),
        name!(lake_snapshot_id, i64),
    ),
> {
    let t = TableId(table.into());
    let cut = or_error(PgCatalog.current(t));
    let pin = or_error(PgReadPins::default().acquire(t, &cut));
    TableIterator::once((pin.0, cut.t.0, cut.snapshot.0))
}

#[pg_extern]
fn modak_rewrite_scan(table: pg_sys::Oid) -> String {
    let t = TableId(table.into());
    let meta = or_error(table_meta(t));
    let cut = or_error(PgCatalog.current(t));
    let delta = or_error(PgCatalog.overlay(t, modak_core::domain::KeyRange::UNBOUNDED));
    let plan = core_planner::rewrite(&core_planner::UserQuery::default(), &cut, &delta);
    or_error(render_scan(&plan, &meta))
}

#[pg_extern]
fn modak_read_end(pin_id: i64) {
    or_error(PgReadPins::default().release(modak_core::domain::PinId(pin_id)));
}
