//! The pure consistency domain of Modak, with no Postgres, DuckDB, or Iceberg
//! dependencies. The [`ports`] traits are implemented by adapters in the
//! `modak-pg` extension.

pub mod dml;
pub mod domain;
pub mod planner;
pub mod ports;
pub mod sqlgen;

pub use domain::*;

/// Crate-wide error type. Adapters map their backend errors into this.
#[derive(Debug, thiserror::Error)]
pub enum ModakError {
    #[error("table {0:?} is not registered with modak")]
    UnknownTable(domain::TableId),
    #[error("catalog error: {0}")]
    Catalog(String),
    #[error("planning error: {0}")]
    Planning(String),
    #[error("execution error: {0}")]
    Execution(String),
}

pub type Result<T> = std::result::Result<T, ModakError>;
