//! Ports (interfaces) the domain depends on. Adapters live in `modak-pg`, so
//! nothing here knows about a concrete lake format, DuckDB, or pgrx.

use crate::domain::{Cutline, DeltaSnapshot, KeyRange, PinId, TableId};
use crate::Result;

/// Read-only view of the consistency facts the planner needs (reads `modak.cutline`).
pub trait CutlineReader {
    fn current(&self, table: TableId) -> Result<Cutline>;
}

/// The pinned correction overlay for cold rows, read in the query's MVCC snapshot.
pub trait DeltaReader {
    fn overlay(&self, table: TableId, range: KeyRange) -> Result<DeltaSnapshot>;
}

/// Acquires/releases a read pin. A pin records the whole `(T, S)` pair a query
/// froze, gating both partition drop and delta/snapshot reclamation.
pub trait ReadPinRepository {
    fn acquire(&self, table: TableId, cut: &Cutline) -> Result<PinId>;
    fn release(&self, pin: PinId) -> Result<()>;
}
