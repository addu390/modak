//! The executor seam. [`crate::sqlgen`] renders the two-tier read shape once;
//! each executor supplies a [`SqlDialect`] for the fragments that differ.
//! Format-specific lake knowledge stays in [`crate::sqlgen::LakePin`].

use crate::sqlgen::{column_list, ident, lit, LakePin, TableMeta};

pub trait SqlDialect {
    fn heap_from(&self, meta: &TableMeta) -> String;
    fn hot_projection(&self, meta: &TableMeta) -> String;
    fn lake_base(&self, meta: &TableMeta, pin: &LakePin) -> String;
    fn delta_from(&self) -> String;
    fn delta_projection(&self, meta: &TableMeta) -> String;
}

pub struct PgDuckdb;

impl SqlDialect for PgDuckdb {
    fn heap_from(&self, meta: &TableMeta) -> String {
        format!("{}.{}", ident(&meta.hot_schema), ident(&meta.hot_table))
    }

    fn hot_projection(&self, meta: &TableMeta) -> String {
        column_list(meta)
    }

    fn lake_base(&self, meta: &TableMeta, pin: &LakePin) -> String {
        crate::sqlgen::lake_base_select(meta, pin)
    }

    fn delta_from(&self) -> String {
        "tierdb.delta".to_string()
    }

    fn delta_projection(&self, meta: &TableMeta) -> String {
        crate::sqlgen::pg_delta_projection(meta)
    }
}

pub struct DuckDbNative<'a> {
    pub dsn: &'a str,
}

impl SqlDialect for DuckDbNative<'_> {
    fn heap_from(&self, meta: &TableMeta) -> String {
        format!(
            "postgres_scan({}, {}, {})",
            lit(self.dsn),
            lit(&meta.hot_schema),
            lit(&meta.hot_table),
        )
    }

    fn hot_projection(&self, meta: &TableMeta) -> String {
        column_casts(meta)
    }

    fn lake_base(&self, meta: &TableMeta, pin: &LakePin) -> String {
        format!(
            "SELECT {casts} FROM {scan}",
            casts = column_casts(meta),
            scan = pin.scan_expr(),
        )
    }

    fn delta_from(&self) -> String {
        format!("postgres_scan({}, 'tierdb', 'delta')", lit(self.dsn))
    }

    fn delta_projection(&self, meta: &TableMeta) -> String {
        meta.columns
            .iter()
            .map(|c| {
                let ty = duckdb_type(&c.sql_type);
                if ty.ends_with("[]") {
                    format!(
                        "CAST(json_extract(CAST(d.payload AS JSON), {key}) AS {ty}) AS {q}",
                        key = lit(&c.name),
                        q = ident(&c.name),
                    )
                } else {
                    format!(
                        "CAST(json_extract_string(CAST(d.payload AS VARCHAR), {key}) AS {ty}) AS {q}",
                        key = lit(&c.name),
                        q = ident(&c.name),
                    )
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn column_casts(meta: &TableMeta) -> String {
    meta.columns
        .iter()
        .map(|c| {
            format!(
                "CAST({q} AS {ty}) AS {q}",
                q = ident(&c.name),
                ty = duckdb_type(&c.sql_type),
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn duckdb_type(pg_type: &str) -> String {
    let t = pg_type.trim().to_ascii_lowercase();
    if let Some(elem) = t.strip_suffix("[]") {
        return format!("{}[]", duckdb_type(elem.trim()));
    }
    let head = t.split('(').next().unwrap_or(&t).trim();
    match head {
        "bigint" | "int8" => "BIGINT".to_string(),
        "integer" | "int" | "int4" => "INTEGER".to_string(),
        "smallint" | "int2" => "SMALLINT".to_string(),
        "boolean" | "bool" => "BOOLEAN".to_string(),
        "numeric" | "decimal" => decimal_type(&t),
        "double precision" | "float8" => "DOUBLE".to_string(),
        "real" | "float4" => "REAL".to_string(),
        "date" => "DATE".to_string(),
        "time with time zone" | "timetz" => "TIME WITH TIME ZONE".to_string(),
        "time" | "time without time zone" => "TIME".to_string(),
        "timestamp with time zone" | "timestamptz" => "TIMESTAMP WITH TIME ZONE".to_string(),
        "timestamp" | "timestamp without time zone" => "TIMESTAMP".to_string(),
        "uuid" => "UUID".to_string(),
        "bytea" => "BLOB".to_string(),
        _ => "VARCHAR".to_string(),
    }
}

fn decimal_type(t: &str) -> String {
    let args = t.split('(').nth(1).and_then(|s| s.split(')').next());
    if let Some(args) = args {
        let mut parts = args.split(',').map(|s| s.trim());
        let precision = parts.next().and_then(|s| s.parse::<u32>().ok());
        let scale = parts
            .next()
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(0);
        if let Some(precision) = precision {
            if (1..=38).contains(&precision) && scale <= precision {
                return format!("DECIMAL({precision},{scale})");
            }
        }
    }
    "DOUBLE".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{TableId, TierKey};
    use crate::sqlgen::{render_scan, Column, ReadCut};
    use crate::TierKeyType;

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
        }
    }

    fn iceberg(meta: &str) -> LakePin {
        LakePin::Iceberg {
            metadata_location: meta.into(),
        }
    }

    #[test]
    fn duckdb_hot_only_without_a_cutline() {
        let sql = render_scan(&meta(), None, &DuckDbNative { dsn: "host=h" }).unwrap();
        assert!(sql.contains("postgres_scan('host=h', 'public', 'events')"));
        assert!(!sql.contains("UNION ALL"));
        assert!(!sql.contains("tierdb"));
    }

    #[test]
    fn duckdb_cold_is_delta_only_without_a_lake_snapshot() {
        let c = ReadCut {
            t: TierKey(100),
            pin: None,
        };
        let sql = render_scan(&meta(), Some(c), &DuckDbNative { dsn: "host=h" }).unwrap();
        assert!(sql.contains("WHERE \"event_time\" >= 100"));
        assert!(sql.contains("WHERE \"event_time\" < 100"));
        assert!(!sql.contains("iceberg_scan"));
        assert!(!sql.contains("NOT EXISTS"));
        assert!(sql.contains("d.table_id = 90001 AND d.op = 0"));
        assert!(sql.contains("postgres_scan('host=h', 'tierdb', 'delta')"));
    }

    #[test]
    fn duckdb_cold_merges_iceberg_base_with_delta() {
        let path = "/wh/events/metadata/00002-abc.metadata.json";
        let pin = iceberg(path);
        let c = ReadCut {
            t: TierKey(100),
            pin: Some(&pin),
        };
        let sql = render_scan(&meta(), Some(c), &DuckDbNative { dsn: "host=h" }).unwrap();
        assert!(sql.contains(&format!("iceberg_scan('{path}')")));
        assert!(sql.contains("NOT EXISTS"));
        assert!(sql.contains("d.pk = b.\"id\"::text"));
        assert!(sql.contains("d.table_id = 90001 AND d.op = 0"));
        assert!(sql.contains("CAST(\"id\" AS BIGINT) AS \"id\""));
        assert!(sql.contains("WHERE \"event_time\" >= 100"));
        assert!(sql.contains("WHERE \"event_time\" < 100"));
    }

    #[test]
    fn numeric_preserves_precision() {
        assert_eq!(duckdb_type("numeric(10,2)"), "DECIMAL(10,2)");
        assert_eq!(duckdb_type("numeric(12)"), "DECIMAL(12,0)");
        assert_eq!(duckdb_type("numeric"), "DOUBLE");
        assert_eq!(duckdb_type("numeric(50,10)"), "DOUBLE");
        assert_eq!(duckdb_type("character varying(255)"), "VARCHAR");
        assert_eq!(
            duckdb_type("timestamp with time zone"),
            "TIMESTAMP WITH TIME ZONE"
        );
    }

    #[test]
    fn arrays_map_to_native_lists() {
        assert_eq!(duckdb_type("integer[]"), "INTEGER[]");
        assert_eq!(duckdb_type("text[]"), "VARCHAR[]");
        assert_eq!(duckdb_type("numeric(12,3)[]"), "DECIMAL(12,3)[]");
    }
}
