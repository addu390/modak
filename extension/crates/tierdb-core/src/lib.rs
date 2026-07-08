//! The pure consistency domain of TierDB, with no Postgres, DuckDB, or Iceberg
//! dependencies. The [`ports`] traits are implemented by adapters in the
//! `tierdb-pg` extension.

pub mod dialect;
pub mod dml;
pub mod domain;
pub mod planner;
pub mod ports;
pub mod sqlgen;
pub mod tier_key;

pub use domain::*;
pub use tier_key::TierKeyType;

/// Crate-wide error type. Adapters map their backend errors into this.
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
