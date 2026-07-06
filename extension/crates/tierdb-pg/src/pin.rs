//! Read-pin adapter: acquire/release rows in `tierdb.read_pins`, whose oldest
//! entry is the reclaim horizon the Java workers respect.

use tierdb_core::domain::{Cutline, PinId, TableId};
use tierdb_core::ports::ReadPinRepository;
use tierdb_core::Result;
use pgrx::prelude::*;

use crate::catalog::catalog_err;

const DEFAULT_TTL_SECS: i64 = 3600;

const ACQUIRE_SQL: &str = "INSERT INTO tierdb.read_pins \
       (table_id, pinned_lake_snapshot_id, pinned_tier_key_hi, expires_at) \
     VALUES ($1, $2, $3, now() + make_interval(secs => $4)) \
     RETURNING pin_id";

const RELEASE_SQL: &str = "DELETE FROM tierdb.read_pins WHERE pin_id = $1";

/// SPI-backed read-pin repository.
pub struct PgReadPins {
    pub ttl_secs: i64,
}

impl Default for PgReadPins {
    fn default() -> Self {
        Self {
            ttl_secs: DEFAULT_TTL_SECS,
        }
    }
}

impl ReadPinRepository for PgReadPins {
    fn acquire(&self, table: TableId, cut: &Cutline) -> Result<PinId> {
        let id = Spi::get_one_with_args::<i64>(
            ACQUIRE_SQL,
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
        Spi::run_with_args(RELEASE_SQL, &[pin.0.into()]).map_err(catalog_err)
    }
}
