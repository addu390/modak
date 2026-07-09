use std::collections::BTreeMap;

use pgrx::prelude::*;
use tierdb_core::dialect::PgDuckdb;
use tierdb_core::domain::TableId;
use tierdb_core::lake::LakeCatalog;
use tierdb_core::mode::Mode;
use tierdb_core::ports::{CutlineReader, ReadPinRepository};
use tierdb_core::sqlgen::{render_scan, Column};
use tierdb_core::table::Table;
use tierdb_core::{Result, TierDBError};

use crate::catalog::{catalog_err, PgCatalog};
use crate::pin::PgReadPins;

const META_SQL: &str = "SELECT t.schema_name, t.table_name, t.primary_key_cols, t.tier_key_col, \
            t.tier_key_type, t.lake_format, t.mode, t.keep_heap, t.heap_retention_lag, \
            t.lake_table_ref, t.storage_profile, \
            sp.warehouse, sp.lake_config, c.lake_props \
     FROM tierdb.tables t \
     LEFT JOIN tierdb.cutline c USING (table_id) \
     LEFT JOIN tierdb.storage_profiles sp ON sp.profile_name = t.storage_profile \
     WHERE t.table_id = $1";

const COLUMNS_SQL: &str = "SELECT a.attname::text AS name, \
            format_type(a.atttypid, a.atttypmod) AS sql_type \
     FROM pg_catalog.pg_attribute a \
     WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped \
     ORDER BY a.attnum";

pub(crate) struct PlannedTable {
    pub table: Table,
    pub lake_props: BTreeMap<String, String>,
}

impl PlannedTable {
    pub fn scan(&self, t: tierdb_core::domain::TierKey) -> Result<tierdb_core::read::Read> {
        self.table.scan_pinned(t, &self.lake_props)
    }
}

pub(crate) fn table_meta(table: TableId) -> Result<PlannedTable> {
    struct MetaRow {
        schema: Option<String>,
        name: Option<String>,
        pk_cols: Option<Vec<String>>,
        tier_col: Option<String>,
        tier_type: Option<String>,
        lake_format: Option<String>,
        mode: Option<String>,
        keep_heap: Option<bool>,
        heap_retention_lag: Option<i64>,
        lake_table_ref: Option<String>,
        storage_profile: Option<String>,
        warehouse: Option<String>,
        lake_config: BTreeMap<String, String>,
        lake_props: BTreeMap<String, String>,
    }

    let m = Spi::connect(|client| {
        let mut rows = client
            .select(META_SQL, Some(1), &[(table.0 as i64).into()])
            .map_err(catalog_err)?;
        let row = rows.next().ok_or(TierDBError::UnknownTable(table))?;
        Ok::<_, TierDBError>(MetaRow {
            schema: row
                .get_by_name::<String, _>("schema_name")
                .map_err(catalog_err)?,
            name: row
                .get_by_name::<String, _>("table_name")
                .map_err(catalog_err)?,
            pk_cols: row
                .get_by_name::<Vec<String>, _>("primary_key_cols")
                .map_err(catalog_err)?,
            tier_col: row
                .get_by_name::<String, _>("tier_key_col")
                .map_err(catalog_err)?,
            tier_type: row
                .get_by_name::<String, _>("tier_key_type")
                .map_err(catalog_err)?,
            lake_format: row
                .get_by_name::<String, _>("lake_format")
                .map_err(catalog_err)?,
            mode: row.get_by_name::<String, _>("mode").map_err(catalog_err)?,
            keep_heap: row
                .get_by_name::<bool, _>("keep_heap")
                .map_err(catalog_err)?,
            heap_retention_lag: row
                .get_by_name::<i64, _>("heap_retention_lag")
                .map_err(catalog_err)?,
            lake_table_ref: row
                .get_by_name::<String, _>("lake_table_ref")
                .map_err(catalog_err)?,
            storage_profile: row
                .get_by_name::<String, _>("storage_profile")
                .map_err(catalog_err)?,
            warehouse: row
                .get_by_name::<String, _>("warehouse")
                .map_err(catalog_err)?,
            lake_config: props_map(
                row.get_by_name::<pgrx::JsonB, _>("lake_config")
                    .map_err(catalog_err)?,
            ),
            lake_props: props_map(
                row.get_by_name::<pgrx::JsonB, _>("lake_props")
                    .map_err(catalog_err)?,
            ),
        })
    })?;

    let schema = m.schema.ok_or_else(|| catalog_err("schema_name is NULL"))?;
    let name = m.name.ok_or_else(|| catalog_err("table_name is NULL"))?;
    let pk_cols = m
        .pk_cols
        .ok_or_else(|| catalog_err("primary_key_cols is NULL"))?;
    if pk_cols.is_empty() {
        return Err(TierDBError::Planning(format!(
            "table {table:?} has no primary key columns"
        )));
    }

    let lake_format = m
        .lake_format
        .ok_or_else(|| catalog_err("tierdb.tables.lake_format is NULL"))?;
    let mode = Mode::from_catalog(
        m.mode
            .as_deref()
            .ok_or_else(|| catalog_err("mode is NULL"))?,
        m.keep_heap.unwrap_or(false),
        m.heap_retention_lag,
    )?;

    let catalog = if mode.is_direct() {
        let profile = m
            .storage_profile
            .clone()
            .ok_or_else(|| catalog_err("storage_profile is NULL"))?;
        Some(
            LakeCatalog::from_profile(
                &lake_format,
                &profile,
                &m.warehouse.clone().unwrap_or_default(),
                &m.lake_config,
            )?
            .ok_or_else(|| {
                TierDBError::Planning(format!(
                    "direct table {schema}.{name} needs a live catalog endpoint, \
                     but storage profile '{profile}' does not configure one"
                ))
            })?,
        )
    } else {
        None
    };

    let tier_col = m.tier_col;
    let tier_type = m.tier_type;

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

    let tier_key_col = tier_col.ok_or_else(|| catalog_err("tier_key_col is NULL"))?;
    let tier_key_type =
        tierdb_core::TierKeyType::from_name(&tier_type.unwrap_or_else(|| "bigint".into()))?;

    Ok(PlannedTable {
        table: Table {
            id: table,
            schema,
            name,
            columns,
            pk_cols,
            tier_key_col,
            tier_key_type,
            mode,
            lake_format,
            lake_table_ref: m.lake_table_ref,
            catalog,
        },
        lake_props: m.lake_props,
    })
}

fn props_map(props: Option<pgrx::JsonB>) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if let Some(pgrx::JsonB(serde_json::Value::Object(map))) = props {
        for (key, value) in map {
            let text = match value {
                serde_json::Value::String(s) => s,
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::Bool(b) => b.to_string(),
                _ => continue,
            };
            out.insert(key, text);
        }
    }
    out
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
    let planned = or_error(table_meta(t));
    if let Some(c) = &planned.table.catalog {
        or_error(crate::lake::ensure_attached(c));
    }
    let cut = or_error(PgCatalog.current(t));
    let read = or_error(planned.scan(cut.t));
    or_error(render_scan(&planned.table, Some(&read), &PgDuckdb))
}

#[pg_extern]
fn tierdb_read_end(pin_id: i64) {
    or_error(PgReadPins::default().release(tierdb_core::domain::PinId(pin_id)));
}
