pub mod dialect;
pub mod dml;
pub mod domain;
pub mod lake;
pub mod mode;
pub mod planner;
pub mod ports;
pub mod read;
pub mod sqlgen;
pub mod table;
pub mod tier_key;

pub use domain::*;
pub use mode::{ColdSink, DeletePlan, InsertPlan, Mode, UpdatePlan};
pub use read::{Cold, Read};
pub use table::{Column, Table};
pub use tier_key::TierKeyType;

#[derive(Debug, thiserror::Error)]
pub enum TierDBError {
    #[error("table {0:?} is not registered with tierdb")]
    UnknownTable(domain::TableId),
    #[error("catalog error: {0}")]
    Catalog(String),
    #[error("planning error: {0}")]
    Planning(String),
    #[error("execution error: {0}")]
    Execution(String),
}

pub type Result<T> = std::result::Result<T, TierDBError>;
