use std::collections::BTreeMap;

use crate::{Result, TierDBError};

#[derive(Debug, Clone)]
pub enum LakeCatalog {
    Iceberg {
        alias: String,
        warehouse: String,
        endpoint: String,
    },
}

impl LakeCatalog {
    pub fn from_profile(
        format: &str,
        profile_name: &str,
        warehouse: &str,
        lake_config: &BTreeMap<String, String>,
    ) -> Result<Option<LakeCatalog>> {
        match format {
            "iceberg" => match lake_config.get("catalog.uri") {
                Some(endpoint) if !endpoint.is_empty() && !warehouse.is_empty() => {
                    Ok(Some(LakeCatalog::Iceberg {
                        alias: Self::alias_for(profile_name),
                        warehouse: warehouse.to_string(),
                        endpoint: endpoint.clone(),
                    }))
                }
                _ => Ok(None),
            },
            other => Err(TierDBError::Planning(format!(
                "unsupported lake_format '{other}' (supported: iceberg)"
            ))),
        }
    }

    pub fn alias_for(profile_name: &str) -> String {
        format!("__tierdb_lake_{profile_name}")
    }

    pub fn alias(&self) -> &str {
        match self {
            LakeCatalog::Iceberg { alias, .. } => alias,
        }
    }

    pub fn attach_sql(&self) -> String {
        match self {
            LakeCatalog::Iceberg {
                alias,
                warehouse,
                endpoint,
            } => format!(
                "ATTACH IF NOT EXISTS {} AS {} (TYPE iceberg, ENDPOINT {})",
                lit(warehouse),
                ident(alias),
                lit(endpoint),
            ),
        }
    }
}

fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}
