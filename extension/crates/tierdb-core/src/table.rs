use crate::domain::TableId;
use crate::lake::LakeCatalog;
use crate::mode::Mode;
use crate::tier_key::TierKeyType;

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub sql_type: String,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub id: TableId,
    pub schema: String,
    pub name: String,
    pub columns: Vec<Column>,
    pub pk_cols: Vec<String>,
    pub tier_key_col: String,
    pub tier_key_type: TierKeyType,
    pub mode: Mode,
    pub lake_format: String,
    pub lake_table_ref: Option<String>,
    pub catalog: Option<LakeCatalog>,
}
