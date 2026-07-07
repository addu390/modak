//! Renders a [`QueryPlan`] into the two-tier SQL shape: hot scan (`tier_key
//! >= T`) UNION ALL a pinned cold lake scan (chosen by [`LakePin`]) merged
//! with `tierdb.delta`.

use std::collections::BTreeMap;

use crate::domain::TableId;
use crate::planner::QueryPlan;
use crate::tier_key::TierKeyType;
use crate::{TierDBError, Result};

const PIN_TOKEN_SEP: char = '\u{1f}';

/// Everything the renderer needs to know about one registered table.
/// Assembled by the adapter (tierdb-pg) from `tierdb.tables` + `pg_attribute`.
#[derive(Debug, Clone)]
pub struct TableMeta {
    pub table_id: TableId,
    pub hot_schema: String,
    pub hot_table: String,
    pub columns: Vec<Column>,
    pub pk_cols: Vec<String>,
    pub tier_key_col: String,
    pub tier_key_type: TierKeyType,
    pub pin: LakePin,
}

/// The single owner of format-specific read knowledge; a new format is one more variant.
#[derive(Debug, Clone)]
pub enum LakePin {
    Iceberg { metadata_location: String },
}

impl LakePin {
    pub fn from_catalog(
        format: &str,
        props: &BTreeMap<String, String>,
        _snapshot: Option<i64>,
    ) -> Result<LakePin> {
        match format {
            "iceberg" => Ok(LakePin::Iceberg {
                metadata_location: require(props, "metadata_location")?,
            }),
            other => Err(unsupported_format(other)),
        }
    }

    /// Opaque token for the `tierdb_lake_rows` seam: format tag first, variant tail after.
    pub fn to_token(&self) -> String {
        match self {
            LakePin::Iceberg { metadata_location } => {
                format!("iceberg{PIN_TOKEN_SEP}{metadata_location}")
            }
        }
    }

    pub fn from_token(token: &str) -> Result<LakePin> {
        let mut parts = token.split(PIN_TOKEN_SEP);
        match parts.next().unwrap_or("") {
            "iceberg" => Ok(LakePin::Iceberg {
                metadata_location: token_field(parts.next(), "iceberg.metadata_location")?,
            }),
            other => Err(unsupported_format(other)),
        }
    }

    fn scan_expr(&self) -> String {
        match self {
            LakePin::Iceberg { metadata_location } => {
                format!("iceberg_scan({})", lit(metadata_location))
            }
        }
    }
}

fn require(props: &BTreeMap<String, String>, key: &str) -> Result<String> {
    props.get(key).cloned().ok_or_else(|| {
        TierDBError::Planning(format!(
            "cold tier has no committed lake snapshot yet \
             (tierdb.cutline.lake_props->>'{key}' is missing)"
        ))
    })
}

fn token_field(value: Option<&str>, what: &str) -> Result<String> {
    value
        .map(str::to_string)
        .ok_or_else(|| TierDBError::Planning(format!("lake pin token is missing '{what}'")))
}

fn unsupported_format(format: &str) -> TierDBError {
    TierDBError::Planning(format!(
        "unsupported lake_format '{format}' (supported: iceberg)"
    ))
}

/// A relation column and its Postgres type name (`format_type()` output).
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub sql_type: String,
}

fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Canonical text encoding of a composite PK. Single-column keys stay the raw
/// `::text` value, multi-column keys escape `\` and the unit separator (0x1F)
/// in each part, then join on 0x1F. `encode_pk` and `pk_sql_expr` must stay
/// in lockstep, since `tierdb.delta.pk` is written by one and matched by the other.
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

/// The same encoding as [`encode_pk`], as a SQL expression over `qualifier.col`
/// columns. Uses only `replace`/`chr`/`||` so it evaluates identically on the
/// Postgres and DuckDB executors.
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

pub fn render_scan(plan: &QueryPlan, meta: &TableMeta) -> Result<String> {
    let t = plan.recent.tier_lo;
    let cold = render_cold_branch(t, meta)?;
    let hot_rel = format!("{}.{}", ident(&meta.hot_schema), ident(&meta.hot_table));
    let col_list = column_list(meta);
    let tier = ident(&meta.tier_key_col);
    Ok(format!(
        "SELECT {col_list} FROM {hot_rel} WHERE {tier} >= {t}\n\
         UNION ALL\n\
         {cold}",
        t = meta.tier_key_type.pg_literal(t.0),
    ))
}

/// The pinned lake scan as one SELECT over `duckdb.query()`. S is pinned by
/// the format's own immutable snapshot. The tier predicate stays outside the
/// DuckDB literal until pg_duckdb picks up the duckdb-iceberg#940 fix
/// (DuckDB >= 1.5.2).
pub fn lake_base_select(meta: &TableMeta) -> String {
    let inner_duckdb_sql = format!(
        "SELECT {cols} FROM {scan}",
        cols = column_list(meta),
        scan = meta.pin.scan_expr(),
    );
    let base_projection = meta
        .columns
        .iter()
        .map(|c| format!("r[{}]::{} AS {}", lit(&c.name), c.sql_type, ident(&c.name)))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT {base_projection}\nFROM duckdb.query({inner_lit}) r",
        inner_lit = lit(&inner_duckdb_sql),
    )
}

/// The cold half alone, the pinned lake scan merged with the `tierdb.delta`
/// overlay and bounded to `tier_key < T`. The read path unions it with the
/// hot scan.
pub fn render_cold_branch(t: crate::domain::TierKey, meta: &TableMeta) -> Result<String> {
    render_cold(t, meta, &lake_base_select(meta))
}

/// The same cold half sourced from `tierdb_lake_rows()` instead of a direct
/// scan. The DML rewrite needs this, since pg_duckdb refuses any statement
/// where a DuckDB scan feeds a Postgres write.
pub fn render_cold_branch_spooled(t: crate::domain::TierKey, meta: &TableMeta) -> Result<String> {
    let projection = meta
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
        table_id = meta.table_id.0,
        t = t.0,
        pin_token = lit(&meta.pin.to_token()),
    );
    render_cold(t, meta, &base)
}

fn render_cold(t: crate::domain::TierKey, meta: &TableMeta, base: &str) -> Result<String> {
    if meta.columns.is_empty() {
        return Err(TierDBError::Planning(format!(
            "table {:?} has no columns",
            meta.table_id
        )));
    }
    if meta.pk_cols.is_empty() {
        return Err(TierDBError::Planning(format!(
            "table {:?} has no primary key columns",
            meta.table_id
        )));
    }

    let table_id = meta.table_id.0;
    let col_list = column_list(meta);

    let delta_projection = meta
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
        .join(", ");

    // pk compares as canonical text so the merge runs identically on both executors.
    let pk_match = format!("d.pk = {}", pk_sql_expr("b", &meta.pk_cols));

    let tier = ident(&meta.tier_key_col);

    Ok(format!(
        "SELECT {col_list} FROM (\n\
           SELECT {col_list} FROM (\n\
             {base}\n\
           ) b\n\
           WHERE NOT EXISTS (\n\
             SELECT 1 FROM tierdb.delta d\n\
             WHERE d.table_id = {table_id} AND {pk_match}\n\
           )\n\
           UNION ALL\n\
           SELECT {delta_projection}\n\
           FROM tierdb.delta d\n\
           WHERE d.table_id = {table_id} AND d.op = 0\n\
         ) cold\n\
         WHERE {tier} < {t}",
        t = meta.tier_key_type.pg_literal(t.0),
    ))
}

fn column_list(meta: &TableMeta) -> String {
    meta.columns
        .iter()
        .map(|c| ident(&c.name))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Cutline, DeltaSnapshot, LakeSnapshotId, TierKey};
    use crate::planner::{rewrite, UserQuery};

    fn meta() -> TableMeta {
        TableMeta {
            table_id: TableId(90001),
            hot_schema: "public".into(),
            hot_table: "events".into(),
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
            pin: LakePin::Iceberg {
                metadata_location: "/wh/events/metadata/00002-abc.metadata.json".into(),
            },
        }
    }

    fn plan() -> QueryPlan {
        let cut = Cutline {
            t: TierKey(100),
            snapshot: LakeSnapshotId(7),
        };
        rewrite(&UserQuery::default(), &cut, &DeltaSnapshot::default())
    }

    #[test]
    fn renders_the_full_two_tier_shape() {
        let sql = render_scan(&plan(), &meta()).unwrap();
        let expected = "\
SELECT \"id\", \"event_time\", \"val\" FROM \"public\".\"events\" WHERE \"event_time\" >= 100
UNION ALL
SELECT \"id\", \"event_time\", \"val\" FROM (
SELECT \"id\", \"event_time\", \"val\" FROM (
SELECT r['id']::bigint AS \"id\", r['event_time']::bigint AS \"event_time\", r['val']::text AS \"val\"
FROM duckdb.query('SELECT \"id\", \"event_time\", \"val\" FROM iceberg_scan(''/wh/events/metadata/00002-abc.metadata.json'')') r
) b
WHERE NOT EXISTS (
SELECT 1 FROM tierdb.delta d
WHERE d.table_id = 90001 AND d.pk = b.\"id\"::text
)
UNION ALL
SELECT (d.payload ->> 'id')::bigint AS \"id\", (d.payload ->> 'event_time')::bigint AS \"event_time\", (d.payload ->> 'val')::text AS \"val\"
FROM tierdb.delta d
WHERE d.table_id = 90001 AND d.op = 0
) cold
WHERE \"event_time\" < 100";
        assert_eq!(sql, expected);
    }

    #[test]
    fn binds_t_as_constant_and_pins_s_via_metadata_location() {
        let sql = render_scan(&plan(), &meta()).unwrap();
        assert!(
            sql.contains("00002-abc.metadata.json"),
            "S pinned by the versioned metadata path"
        );
        assert!(
            sql.contains(">= 100") && sql.contains("< 100"),
            "T split both branches"
        );
    }

    #[test]
    fn keeps_the_tier_predicate_outside_the_duckdb_literal() {
        // Predicate stays on the Postgres side of duckdb.query() (duckdb-iceberg#940).
        let sql = render_scan(&plan(), &meta()).unwrap();
        assert!(
            sql.contains(".metadata.json'')') r"),
            "no predicate inside the duckdb literal, got:\n{sql}"
        );
        assert!(
            sql.ends_with("WHERE \"event_time\" < 100"),
            "outer tier predicate still bounds the cold branch, got:\n{sql}"
        );
    }

    #[test]
    fn escapes_quotes_in_identifiers_and_paths() {
        let mut m = meta();
        m.hot_table = "we\"ird".into();
        m.pin = LakePin::Iceberg {
            metadata_location: "/wh/o'brien/meta.json".into(),
        };
        let sql = render_scan(&plan(), &m).unwrap();
        assert!(sql.contains("\"we\"\"ird\""));
        assert!(sql.contains("o''''brien"));
    }

    #[test]
    fn pin_token_round_trips() {
        let pin = LakePin::Iceberg {
            metadata_location: "/wh/events/metadata/00002-abc.metadata.json".into(),
        };
        let back = LakePin::from_token(&pin.to_token()).unwrap();
        assert_eq!(back.scan_expr(), pin.scan_expr());
    }

    #[test]
    fn from_catalog_reads_only_the_owning_variants_keys() {
        let mut props = BTreeMap::new();
        props.insert("metadata_location".to_string(), "/wh/m.json".to_string());

        let ice = LakePin::from_catalog("iceberg", &props, None).unwrap();
        assert!(matches!(ice, LakePin::Iceberg { .. }));

        assert!(LakePin::from_catalog("hudi", &props, None).is_err());
    }

    #[test]
    fn rejects_tables_without_columns() {
        let mut m = meta();
        m.columns.clear();
        assert!(matches!(
            render_scan(&plan(), &m),
            Err(TierDBError::Planning(_))
        ));
    }

    #[test]
    fn composite_pk_matches_on_the_escaped_joined_encoding() {
        let mut m = meta();
        m.pk_cols = vec!["id".into(), "val".into()];
        let sql = render_scan(&plan(), &m).unwrap();
        let expected = "d.pk = replace(replace(b.\"id\"::text, chr(92), chr(92)||chr(92)), \
                        chr(31), chr(92)||chr(31)) || chr(31) || \
                        replace(replace(b.\"val\"::text, chr(92), chr(92)||chr(92)), \
                        chr(31), chr(92)||chr(31))";
        assert!(sql.contains(expected), "got:\n{sql}");
    }

    #[test]
    fn encode_pk_is_raw_for_single_and_escaped_joined_for_composite() {
        assert_eq!(encode_pk(&["a\\b".into()]), "a\\b", "single stays raw");
        assert_eq!(
            encode_pk(&["a\\b".into(), "c\u{1f}d".into()]),
            "a\\\\b\u{1f}c\\\u{1f}d"
        );
        assert_eq!(encode_pk(&["1".into(), "2".into()]), "1\u{1f}2");
    }

    #[test]
    fn rejects_tables_without_pk_columns() {
        let mut m = meta();
        m.pk_cols.clear();
        assert!(matches!(
            render_scan(&plan(), &m),
            Err(TierDBError::Planning(_))
        ));
    }
}
