use crate::domain::TierKey;
use crate::read::{Cold, Read};
use crate::table::Table;
use crate::{Result, TierDBError};

pub use crate::lake::LakeCatalog;
pub use crate::table::Column;

pub const READ_PIN_TTL_SECS: i64 = 3600;

pub fn read_pin_acquire_sql() -> &'static str {
    "INSERT INTO tierdb.read_pins \
       (table_id, pinned_lake_snapshot_id, pinned_tier_key_hi, expires_at) \
     VALUES ($1, $2, $3, now() + make_interval(secs => $4)) \
     RETURNING pin_id"
}

pub fn read_pin_release_sql() -> &'static str {
    "DELETE FROM tierdb.read_pins WHERE pin_id = $1"
}

pub fn lake_commit_lock_sql() -> &'static str {
    "SELECT pg_advisory_xact_lock(hashtextextended('tierdb_lake_' || $1::text, 0))"
}

pub(crate) fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub(crate) fn lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

pub fn encode_pk(values: &[String]) -> String {
    if let [only] = values {
        return only.clone();
    }
    values
        .iter()
        .map(|v| v.replace('\\', "\\\\").replace('\u{1f}', "\\\u{1f}"))
        .collect::<Vec<_>>()
        .join("\u{1f}")
}

pub fn pk_sql_expr(qualifier: &str, pk_cols: &[String]) -> String {
    if let [only] = pk_cols {
        return format!("{qualifier}.{}::text", ident(only));
    }
    pk_cols
        .iter()
        .map(|c| {
            format!(
                "replace(replace({qualifier}.{}::text, chr(92), chr(92)||chr(92)), \
                 chr(31), chr(92)||chr(31))",
                ident(c)
            )
        })
        .collect::<Vec<_>>()
        .join(" || chr(31) || ")
}

pub(crate) fn column_list(table: &Table) -> String {
    table
        .columns
        .iter()
        .map(|c| ident(&c.name))
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn render_scan(
    table: &Table,
    read: Option<&Read>,
    dialect: &dyn crate::dialect::SqlDialect,
) -> Result<String> {
    if table.columns.is_empty() {
        return Err(TierDBError::Planning(format!(
            "table {:?} has no columns",
            table.id
        )));
    }
    let hot_proj = dialect.hot_projection(table);
    let heap = dialect.heap_from(table);
    match read {
        None | Some(Read::Heap) => Ok(format!("SELECT {hot_proj} FROM {heap}")),
        Some(Read::Seam { t, cold }) => {
            let tier = ident(&table.tier_key_col);
            let cold_sql = render_cold(*t, table, cold, dialect)?;
            Ok(format!(
                "SELECT {hot_proj} FROM {heap} WHERE {tier} >= {t}\n\
                 UNION ALL\n\
                 {cold_sql}",
                t = table.tier_key_type.pg_literal(t.0),
            ))
        }
    }
}

pub fn lake_base_select(table: &Table, cold: &Cold) -> Result<String> {
    let inner_duckdb_sql = format!(
        "SELECT {cols} FROM {scan}",
        cols = column_list(table),
        scan = cold.scan_expr(&table.lake_format)?,
    );
    let base_projection = table
        .columns
        .iter()
        .map(|c| format!("r[{}]::{} AS {}", lit(&c.name), c.sql_type, ident(&c.name)))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "SELECT {base_projection}\nFROM duckdb.query({inner_lit}) r",
        inner_lit = lit(&inner_duckdb_sql),
    ))
}

pub(crate) fn pg_delta_projection(table: &Table) -> String {
    table
        .columns
        .iter()
        .map(|c| {
            format!(
                "(d.payload ->> {})::{} AS {}",
                lit(&c.name),
                c.sql_type,
                ident(&c.name)
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn render_cold_branch_spooled(t: TierKey, table: &Table, cold: &Cold) -> Result<String> {
    let projection = table
        .columns
        .iter()
        .map(|c| {
            format!(
                "(j ->> {})::{} AS {}",
                lit(&c.name),
                c.sql_type,
                ident(&c.name)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let base = format!(
        "SELECT {projection}\nFROM tierdb_lake_rows({table_id}, {t}, {pin_token}) j",
        table_id = table.id.0,
        t = t.0,
        pin_token = lit(&cold.to_token(&table.lake_format)?),
    );
    render_cold_merge(t, table, &base, "tierdb.delta", &pg_delta_projection(table))
}

fn render_cold(
    t: TierKey,
    table: &Table,
    cold: &Cold,
    dialect: &dyn crate::dialect::SqlDialect,
) -> Result<String> {
    let delta_from = dialect.delta_from();
    let delta_projection = dialect.delta_projection(table);
    match cold {
        Cold::Live { .. } => render_cold_lake_only(t, table, &dialect.lake_base(table, cold)?),
        Cold::Merge { .. } => render_cold_merge(
            t,
            table,
            &dialect.lake_base(table, cold)?,
            &delta_from,
            &delta_projection,
        ),
        Cold::Delta => render_cold_delta_only(t, table, &delta_from, &delta_projection),
    }
}

fn render_cold_lake_only(t: TierKey, table: &Table, base: &str) -> Result<String> {
    let col_list = column_list(table);
    let tier = ident(&table.tier_key_col);
    Ok(format!(
        "SELECT {col_list} FROM (\n\
           {base}\n\
         ) cold\n\
         WHERE {tier} < {t}",
        t = table.tier_key_type.pg_literal(t.0),
    ))
}

fn render_cold_merge(
    t: TierKey,
    table: &Table,
    base: &str,
    delta_from: &str,
    delta_projection: &str,
) -> Result<String> {
    if table.pk_cols.is_empty() {
        return Err(TierDBError::Planning(format!(
            "table {:?} has no primary key columns",
            table.id
        )));
    }

    let table_id = table.id.0;
    let col_list = column_list(table);
    let pk_match = format!("d.pk = {}", pk_sql_expr("b", &table.pk_cols));
    let tier = ident(&table.tier_key_col);

    Ok(format!(
        "SELECT {col_list} FROM (\n\
           SELECT {col_list} FROM (\n\
             {base}\n\
           ) b\n\
           WHERE NOT EXISTS (\n\
             SELECT 1 FROM {delta_from} d\n\
             WHERE d.table_id = {table_id} AND {pk_match}\n\
           )\n\
           UNION ALL\n\
           SELECT {delta_projection}\n\
           FROM {delta_from} d\n\
           WHERE d.table_id = {table_id} AND d.op = 0\n\
         ) cold\n\
         WHERE {tier} < {t}",
        t = table.tier_key_type.pg_literal(t.0),
    ))
}

fn render_cold_delta_only(
    t: TierKey,
    table: &Table,
    delta_from: &str,
    delta_projection: &str,
) -> Result<String> {
    let table_id = table.id.0;
    let col_list = column_list(table);
    let tier = ident(&table.tier_key_col);
    Ok(format!(
        "SELECT {col_list} FROM (\n\
           SELECT {delta_projection}\n\
           FROM {delta_from} d\n\
           WHERE d.table_id = {table_id} AND d.op = 0\n\
         ) cold\n\
         WHERE {tier} < {t}",
        t = table.tier_key_type.pg_literal(t.0),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::PgDuckdb;
    use crate::domain::{TableId, TierKey};
    use crate::mode::Mode;
    use crate::tier_key::TierKeyType;
    use std::collections::BTreeMap;

    fn table() -> Table {
        Table {
            id: TableId(90001),
            schema: "public".into(),
            name: "events".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    sql_type: "bigint".into(),
                },
                Column {
                    name: "event_time".into(),
                    sql_type: "bigint".into(),
                },
                Column {
                    name: "val".into(),
                    sql_type: "text".into(),
                },
            ],
            pk_cols: vec!["id".into()],
            tier_key_col: "event_time".into(),
            tier_key_type: TierKeyType::Bigint,
            mode: Mode::Tiered { keep_heap: false },
            lake_format: "iceberg".into(),
            lake_table_ref: Some("ns.events".into()),
            catalog: None,
        }
    }

    fn merge() -> Cold {
        Cold::Merge {
            props: BTreeMap::from([(
                "metadata_location".into(),
                "/wh/events/metadata/00002-abc.metadata.json".into(),
            )]),
        }
    }

    fn live() -> Cold {
        Cold::Live {
            catalog: "__tierdb_lake_default".into(),
            table_ref: "tierdb.public_events".into(),
        }
    }

    #[test]
    fn renders_the_full_two_tier_shape() {
        let read = Read::Seam {
            t: TierKey(100),
            cold: merge(),
        };
        let sql = render_scan(&table(), Some(&read), &PgDuckdb).unwrap();
        assert!(sql.contains("WHERE \"event_time\" >= 100"), "{sql}");
        assert!(sql.contains("iceberg_scan"), "{sql}");
        assert!(sql.contains("NOT EXISTS"), "{sql}");
        assert!(sql.contains("d.op = 0"), "{sql}");
    }

    #[test]
    fn direct_cold_is_lake_only_with_no_delta_overlay() {
        let read = Read::Seam {
            t: TierKey(100),
            cold: live(),
        };
        let sql = render_scan(&table(), Some(&read), &PgDuckdb).unwrap();
        assert!(
            !sql.contains("NOT EXISTS") && !sql.contains("d.op = 0"),
            "{sql}"
        );
        assert!(
            sql.contains("\"__tierdb_lake_default\".\"tierdb\".\"public_events\""),
            "{sql}"
        );
    }

    #[test]
    fn heap_read_is_plain_select() {
        let sql = render_scan(&table(), Some(&Read::Heap), &PgDuckdb).unwrap();
        assert_eq!(
            sql,
            "SELECT \"id\", \"event_time\", \"val\" FROM \"public\".\"events\""
        );
    }

    #[test]
    fn cold_token_round_trips() {
        let c = merge();
        let back = Cold::from_token(&c.to_token("iceberg").unwrap()).unwrap();
        assert_eq!(
            back.scan_expr("iceberg").unwrap(),
            c.scan_expr("iceberg").unwrap()
        );
    }
}
