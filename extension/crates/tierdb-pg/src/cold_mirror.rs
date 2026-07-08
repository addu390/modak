//! Keep-heap cold mirror. Tiered partitions the worker keeps on the heap get
//! a row trigger that mirrors every change into `tierdb.delta`, so plain DML
//! below the cut-line stays visible to seam reads and folds into the lake.

use pgrx::prelude::*;
use tierdb_core::domain::TableId;
use tierdb_core::sqlgen::encode_pk;

use tierdb_core::dml::{delta_write_sql, DELTA_OP_TOMBSTONE, DELTA_OP_UPSERT};

use crate::catalog::catalog_err;
use crate::delta::{or_error, pk_values, tier_key_of, write_meta, WriteMeta};
use crate::dml::fresh_lookup;

#[pg_extern]
fn tierdb_cold_mirror_route(
    table: pg_sys::Oid,
    op: &str,
    row: pgrx::JsonB,
    old_row: Option<pgrx::JsonB>,
) {
    let t = TableId(table.into());
    let meta = or_error(write_meta(t));
    match op {
        "DELETE" => tombstone(t, &meta, &row),
        "INSERT" => upsert(t, &meta, &row),
        "UPDATE" => {
            if let Some(old) = old_row {
                let old_pk = encode_pk(&or_error(pk_values(&old, &meta.pk_cols)));
                let new_pk = encode_pk(&or_error(pk_values(&row, &meta.pk_cols)));
                if old_pk != new_pk {
                    tombstone(t, &meta, &old);
                }
            }
            upsert(t, &meta, &row);
        }
        other => error!("tierdb: unknown cold mirror op '{other}'"),
    }
}

fn upsert(t: TableId, meta: &WriteMeta, row: &pgrx::JsonB) {
    let tier_key = or_error(tier_key_of(row, meta));
    let pk = encode_pk(&or_error(pk_values(row, &meta.pk_cols)));
    or_error(
        Spi::run_with_args(
            &delta_write_sql(),
            &[
                (t.0 as i64).into(),
                pk.into(),
                DELTA_OP_UPSERT.into(),
                tier_key.into(),
                pgrx::JsonB(row.0.clone()).into(),
            ],
        )
        .map_err(catalog_err),
    );
}

fn tombstone(t: TableId, meta: &WriteMeta, row: &pgrx::JsonB) {
    let tier_key = or_error(tier_key_of(row, meta));
    let pk = encode_pk(&or_error(pk_values(row, &meta.pk_cols)));
    let mut payload = serde_json::Map::new();
    for col in &meta.pk_cols {
        payload.insert(
            col.clone(),
            row.0.get(col).cloned().unwrap_or(serde_json::Value::Null),
        );
    }
    or_error(
        Spi::run_with_args(
            &delta_write_sql(),
            &[
                (t.0 as i64).into(),
                pk.into(),
                DELTA_OP_TOMBSTONE.into(),
                tier_key.into(),
                pgrx::JsonB(serde_json::Value::Object(payload)).into(),
            ],
        )
        .map_err(catalog_err),
    );
}

extension_sql!(
    r#"
CREATE SCHEMA IF NOT EXISTS tierdb;
CREATE FUNCTION tierdb.cold_mirror() RETURNS trigger
LANGUAGE plpgsql AS $body$
BEGIN
    IF TG_OP = 'DELETE' THEN
        PERFORM tierdb_cold_mirror_route(TG_ARGV[0]::oid, TG_OP, to_jsonb(OLD), NULL::jsonb);
    ELSIF TG_OP = 'UPDATE' THEN
        PERFORM tierdb_cold_mirror_route(TG_ARGV[0]::oid, TG_OP, to_jsonb(NEW), to_jsonb(OLD));
    ELSE
        PERFORM tierdb_cold_mirror_route(TG_ARGV[0]::oid, TG_OP, to_jsonb(NEW), NULL::jsonb);
    END IF;
    RETURN NULL;
END
$body$;
"#,
    name = "cold_mirror_trigger",
    requires = [tierdb_cold_mirror_route]
);

pub(crate) const TRIGGER_NAME: &str = "tierdb_cold_mirror";

#[pg_extern]
fn tierdb_attach_cold_mirror(table: pg_sys::Oid, partition: pg_sys::Oid) -> String {
    let t = TableId(table.into());
    let meta = or_error(write_meta(t));
    if !meta.keep_heap {
        error!(
            "tierdb: {}.{} does not keep its heap, the cold mirror applies \
             only to keep-heap tiered tables",
            meta.schema, meta.table
        );
    }
    let qualified: String = fresh_lookup("SELECT $1::regclass::text", &[partition.into()])
        .unwrap_or_else(|| error!("tierdb: no relation with oid {}", u32::from(partition)));

    let attached = fresh_lookup::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = $1 AND tgname = $2)",
        &[partition.into(), TRIGGER_NAME.into()],
    )
    .unwrap_or(false);
    if attached {
        return format!("{qualified}: already mirrored");
    }
    or_error(
        Spi::run(&format!(
            "CREATE TRIGGER {TRIGGER_NAME} AFTER INSERT OR UPDATE OR DELETE ON {qualified} \
             FOR EACH ROW EXECUTE FUNCTION tierdb.cold_mirror('{}')",
            t.0
        ))
        .map_err(catalog_err),
    );
    format!("{qualified}: cold mirror attached")
}

#[pg_extern]
fn tierdb_detach_cold_mirror(table: pg_sys::Oid) -> i64 {
    let children: Vec<String> = Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT i.inhrelid::regclass::text FROM pg_inherits i \
                   JOIN pg_trigger g ON g.tgrelid = i.inhrelid \
                  WHERE i.inhparent = $1 AND g.tgname = $2",
                None,
                &[table.into(), TRIGGER_NAME.into()],
            )
            .unwrap_or_else(|e| error!("tierdb: {e}"));
        rows.filter_map(|row| row.get::<String>(1).ok().flatten())
            .collect()
    });
    let mut dropped = 0;
    for child in &children {
        or_error(
            Spi::run(&format!("DROP TRIGGER IF EXISTS {TRIGGER_NAME} ON {child}"))
                .map_err(catalog_err),
        );
        dropped += 1;
    }
    dropped
}
