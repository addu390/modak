use std::collections::BTreeMap;

use crate::domain::TierKey;
use crate::table::Table;
use crate::{Result, TierDBError};

const TOKEN_SEP: char = '\u{1f}';

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Read {
    Heap,
    Seam { t: TierKey, cold: Cold },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cold {
    Delta,
    Live { catalog: String, table_ref: String },
    Merge { props: BTreeMap<String, String> },
}

impl Cold {
    pub fn from_pinned_props(
        format: &str,
        props: &BTreeMap<String, String>,
    ) -> Result<Option<Cold>> {
        match format {
            "iceberg" if !props.contains_key("metadata_location") => Ok(None),
            "iceberg" => {
                if props
                    .get("metadata_location")
                    .map(|s| s.is_empty())
                    .unwrap_or(true)
                {
                    return Err(TierDBError::Planning(
                        "cold tier has no committed lake snapshot yet \
                         (tierdb.cutline.lake_props->>'metadata_location' is missing)"
                            .into(),
                    ));
                }
                Ok(Some(Cold::Merge {
                    props: props.clone(),
                }))
            }
            other => Err(TierDBError::Planning(format!(
                "unsupported lake_format '{other}' (supported: iceberg)"
            ))),
        }
    }

    pub fn to_token(&self, format: &str) -> Result<String> {
        match self {
            Cold::Merge { props } if format == "iceberg" => {
                let loc = props.get("metadata_location").ok_or_else(|| {
                    TierDBError::Planning("merge cold is missing metadata_location".into())
                })?;
                Ok(format!("iceberg{TOKEN_SEP}{loc}"))
            }
            Cold::Live { catalog, table_ref } if format == "iceberg" => Ok(format!(
                "iceberg_live{TOKEN_SEP}{catalog}{TOKEN_SEP}{table_ref}"
            )),
            Cold::Delta => Err(TierDBError::Planning(
                "delta-only cold has no lake token".into(),
            )),
            _ => Err(TierDBError::Planning(format!(
                "unsupported lake_format '{format}' for cold token"
            ))),
        }
    }

    pub fn from_token(token: &str) -> Result<Cold> {
        let mut parts = token.split(TOKEN_SEP);
        match parts.next().unwrap_or("") {
            "iceberg" => {
                let loc = parts.next().ok_or_else(|| {
                    TierDBError::Planning(
                        "lake token is missing 'iceberg.metadata_location'".into(),
                    )
                })?;
                Ok(Cold::Merge {
                    props: BTreeMap::from([("metadata_location".into(), loc.to_string())]),
                })
            }
            "iceberg_live" => Ok(Cold::Live {
                catalog: parts
                    .next()
                    .ok_or_else(|| {
                        TierDBError::Planning("lake token is missing 'iceberg_live.catalog'".into())
                    })?
                    .to_string(),
                table_ref: parts
                    .next()
                    .ok_or_else(|| {
                        TierDBError::Planning(
                            "lake token is missing 'iceberg_live.table_ref'".into(),
                        )
                    })?
                    .to_string(),
            }),
            other => Err(TierDBError::Planning(format!(
                "unsupported lake_format '{other}' (supported: iceberg)"
            ))),
        }
    }

    pub fn scan_expr(&self, format: &str) -> Result<String> {
        match (self, format) {
            (Cold::Merge { props }, "iceberg") => {
                let loc = props.get("metadata_location").ok_or_else(|| {
                    TierDBError::Planning("merge cold is missing metadata_location".into())
                })?;
                Ok(format!("iceberg_scan({})", lit(loc)))
            }
            (Cold::Live { .. }, "iceberg") => Ok(self.live_relation()),
            (Cold::Delta, _) => Err(TierDBError::Planning(
                "delta-only cold has no lake scan expression".into(),
            )),
            (_, other) => Err(TierDBError::Planning(format!(
                "unsupported lake_format '{other}' (supported: iceberg)"
            ))),
        }
    }

    pub fn write_target(&self) -> Result<String> {
        match self {
            Cold::Live { .. } => Ok(self.live_relation()),
            _ => Err(TierDBError::Planning(
                "a pinned lake snapshot is read-only; \
                 direct-mode writes need a live catalog attachment"
                    .into(),
            )),
        }
    }

    fn live_relation(&self) -> String {
        match self {
            Cold::Live { catalog, table_ref } => {
                let mut parts = vec![ident(catalog)];
                parts.extend(table_ref.split('.').map(ident));
                parts.join(".")
            }
            _ => unreachable!(),
        }
    }
}

impl Table {
    pub fn scan(
        &self,
        t: TierKey,
        pinned_props: Option<&BTreeMap<String, String>>,
    ) -> Result<Read> {
        if self.mode.heap_complete() {
            return Ok(Read::Heap);
        }
        let cold = self.cold_at(pinned_props)?;
        Ok(Read::Seam { t, cold })
    }

    pub fn scan_pinned(&self, t: TierKey, pinned_props: &BTreeMap<String, String>) -> Result<Read> {
        if self.mode.heap_complete() || self.mode.is_direct() {
            return self.scan(t, None);
        }
        let cold = Cold::from_pinned_props(&self.lake_format, pinned_props)?.ok_or_else(|| {
            TierDBError::Planning(
                "cold tier has no committed lake snapshot yet \
                 (tierdb.cutline.lake_props->>'metadata_location' is missing)"
                    .into(),
            )
        })?;
        Ok(Read::Seam { t, cold })
    }

    pub fn scan_hybrid(&self, t: TierKey, pinned_props: &BTreeMap<String, String>) -> Result<Read> {
        let cold = Cold::from_pinned_props(&self.lake_format, pinned_props)?.ok_or_else(|| {
            TierDBError::Planning(format!(
                "table {}.{} hybrid read needs a pinned lake snapshot",
                self.schema, self.name
            ))
        })?;
        Ok(Read::Seam { t, cold })
    }

    fn cold_at(&self, pinned_props: Option<&BTreeMap<String, String>>) -> Result<Cold> {
        if self.mode.is_direct() {
            let catalog = self.catalog.as_ref().ok_or_else(|| {
                TierDBError::Planning(format!(
                    "direct table {}.{} needs a live catalog endpoint",
                    self.schema, self.name
                ))
            })?;
            let table_ref = self.lake_table_ref.as_deref().ok_or_else(|| {
                TierDBError::Planning(format!(
                    "direct table {}.{} has no lake_table_ref",
                    self.schema, self.name
                ))
            })?;
            return Ok(Cold::Live {
                catalog: catalog.alias().to_string(),
                table_ref: table_ref.to_string(),
            });
        }
        match pinned_props {
            Some(props) => {
                Ok(Cold::from_pinned_props(&self.lake_format, props)?.unwrap_or(Cold::Delta))
            }
            None => Ok(Cold::Delta),
        }
    }
}

fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::TableId;
    use crate::mode::Mode;
    use crate::table::{Column, Table};
    use crate::tier_key::TierKeyType;

    fn table(mode: Mode) -> Table {
        Table {
            id: TableId(1),
            schema: "public".into(),
            name: "events".into(),
            columns: vec![Column {
                name: "id".into(),
                sql_type: "bigint".into(),
            }],
            pk_cols: vec!["id".into()],
            tier_key_col: "ts".into(),
            tier_key_type: TierKeyType::Bigint,
            mode,
            lake_format: "iceberg".into(),
            lake_table_ref: Some("ns.events".into()),
            catalog: None,
        }
    }

    #[test]
    fn mirrored_without_retention_is_heap() {
        let t = table(Mode::Mirrored {
            heap_retention: false,
        });
        assert_eq!(t.scan(TierKey(100), None).unwrap(), Read::Heap);
    }

    #[test]
    fn tiered_without_props_is_delta() {
        let t = table(Mode::Tiered { keep_heap: false });
        assert_eq!(
            t.scan(TierKey(100), None).unwrap(),
            Read::Seam {
                t: TierKey(100),
                cold: Cold::Delta
            }
        );
    }

    #[test]
    fn tiered_with_props_is_merge() {
        let t = table(Mode::Tiered { keep_heap: false });
        let props = BTreeMap::from([("metadata_location".into(), "/m".into())]);
        match t.scan(TierKey(100), Some(&props)).unwrap() {
            Read::Seam {
                cold: Cold::Merge { props: p },
                ..
            } => assert_eq!(p.get("metadata_location").unwrap(), "/m"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn direct_needs_catalog() {
        let t = table(Mode::Direct { keep_heap: false });
        assert!(t.scan(TierKey(100), None).is_err());
    }

    #[test]
    fn pinned_without_committed_metadata_errors() {
        let t = table(Mode::Tiered { keep_heap: false });
        let err = t.scan_pinned(TierKey(100), &BTreeMap::new()).unwrap_err();
        assert!(
            err.to_string().contains("no committed lake snapshot"),
            "{err}"
        );
    }

    #[test]
    fn pinned_with_props_is_merge() {
        let t = table(Mode::Tiered { keep_heap: false });
        let props = BTreeMap::from([("metadata_location".into(), "/m".into())]);
        assert_eq!(
            t.scan_pinned(TierKey(100), &props).unwrap(),
            t.scan(TierKey(100), Some(&props)).unwrap()
        );
    }

    #[test]
    fn pinned_heap_complete_is_heap() {
        let t = table(Mode::Mirrored {
            heap_retention: false,
        });
        assert_eq!(
            t.scan_pinned(TierKey(100), &BTreeMap::new()).unwrap(),
            Read::Heap
        );
    }
}
