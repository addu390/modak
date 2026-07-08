//! Read-pin adapter: acquire/release rows in `tierdb.read_pins`, whose oldest
//! entry is the reclaim horizon the Java workers respect.

use pgrx::prelude::*;
use tierdb_core::domain::{Cutline, PinId, TableId};
use tierdb_core::ports::ReadPinRepository;
use tierdb_core::sqlgen::{read_pin_acquire_sql, read_pin_release_sql, READ_PIN_TTL_SECS};
use tierdb_core::Result;

use crate::catalog::catalog_err;

/// SPI-backed read-pin repository.
pub struct PgReadPins {
    pub ttl_secs: i64,
}

impl Default for PgReadPins {
    fn default() -> Self {
        Self {
            ttl_secs: READ_PIN_TTL_SECS,
        }
    }
}

impl ReadPinRepository for PgReadPins {
    fn acquire(&self, table: TableId, cut: &Cutline) -> Result<PinId> {
        let id = Spi::get_one_with_args::<i64>(
            read_pin_acquire_sql(),
            &[
                (table.0 as i64).into(),
                cut.snapshot.0.into(),
                cut.t.0.into(),
                (self.ttl_secs as f64).into(),
            ],
        )
        .map_err(catalog_err)?
        .ok_or_else(|| catalog_err("INSERT INTO tierdb.read_pins returned no pin_id"))?;
        Ok(PinId(id))
    }

    fn release(&self, pin: PinId) -> Result<()> {
        Spi::run_with_args(read_pin_release_sql(), &[pin.0.into()]).map_err(catalog_err)
    }
}
