//! The read protocol as an explicit SQL surface:

use tierdb_core::domain::TableId;
use tierdb_core::ports::{CutlineReader, DeltaReader, ReadPinRepository};
use tierdb_core::sqlgen::{render_scan, Column, TableMeta};
use tierdb_core::{planner as core_planner, TierDBError, Result};
use pgrx::prelude::*;

use crate::catalog::{catalog_err, PgCatalog};
use crate::pin::PgReadPins;

const META_SQL: &str = "SELECT t.schema_name, t.table_name, t.primary_key_cols, t.tier_key_col, \
            t.tier_key_type, \
            c.lake_props ->> 'metadata_location' AS metadata_location \
     FROM tierdb.tables t LEFT JOIN tierdb.cutline c USING (table_id) \
     WHERE t.table_id = $1";

const COLUMNS_SQL: &str = "SELECT a.attname::text AS name, \
            format_type(a.atttypid, a.atttypmod) AS sql_type \
     FROM pg_catalog.pg_attribute a \
     WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped \
     ORDER BY a.attnum";

pub(crate) fn table_meta(table: TableId) -> Result<TableMeta> {
    let (schema, name, pk_cols, tier_col, tier_type, metadata_location) = Spi::connect(|client| {
        let mut rows = client
            .select(META_SQL, Some(1), &[(table.0 as i64).into()])
            .map_err(catalog_err)?;
        let row = rows.next().ok_or(TierDBError::UnknownTable(table))?;
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
        let tier_type = row
            .get_by_name::<String, _>("tier_key_type")
            .map_err(catalog_err)?;
        let meta = row
            .get_by_name::<String, _>("metadata_location")
            .map_err(catalog_err)?;
        Ok::<_, TierDBError>((schema, name, pk, tier, tier_type, meta))
    })?;

    let schema = schema.ok_or_else(|| catalog_err("schema_name is NULL"))?;
    let name = name.ok_or_else(|| catalog_err("table_name is NULL"))?;
    let pk_cols = pk_cols.ok_or_else(|| catalog_err("primary_key_cols is NULL"))?;
    if pk_cols.is_empty() {
        return Err(TierDBError::Planning(format!(
            "table {table:?} has no primary key columns"
        )));
    }
    let metadata_location = metadata_location.ok_or_else(|| {
        TierDBError::Planning(
            "cold tier has no committed lake snapshot yet \
             (tierdb.cutline.lake_props->>'metadata_location' is NULL)"
                .into(),
        )
    })?;

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
        Ok::<_, TierDBError>(cols)
    })?;
    if columns.is_empty() {
        return Err(TierDBError::Planning(format!(
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
        tier_key_type: tierdb_core::TierKeyType::from_name(
            &tier_type.unwrap_or_else(|| "bigint".into()),
        )?,
        lake_metadata_location: metadata_location,
    })
}

fn or_error<T>(r: Result<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => error!("tierdb: {e}"),
    }
}

#[pg_extern]
fn tierdb_read_begin(
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
fn tierdb_rewrite_scan(table: pg_sys::Oid) -> String {
    let t = TableId(table.into());
    let meta = or_error(table_meta(t));
    let cut = or_error(PgCatalog.current(t));
    let delta = or_error(PgCatalog.overlay(t, tierdb_core::domain::KeyRange::UNBOUNDED));
    let plan = core_planner::rewrite(&core_planner::UserQuery::default(), &cut, &delta);
    or_error(render_scan(&plan, &meta))
}

#[pg_extern]
fn tierdb_read_end(pin_id: i64) {
    or_error(PgReadPins::default().release(tierdb_core::domain::PinId(pin_id)));
}
