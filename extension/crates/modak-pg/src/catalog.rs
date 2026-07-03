//! Catalog adapters: read `modak.cutline` and `modak.delta` via SPI,
//! implementing the pure [`modak_core::ports`] read ports. Every call runs
//! inside the caller's transaction, so cut-line and delta reads are one
//! consistent MVCC pair.

use modak_core::domain::{
    Cutline, DeltaEntry, DeltaOp, DeltaSnapshot, KeyRange, LakeSnapshotId, Pk, TableId, TierKey,
};
use modak_core::ports::{CutlineReader, DeltaReader};
use modak_core::{ModakError, Result};
use pgrx::prelude::*;

/// SPI-backed reader of the Modak catalog.
pub struct PgCatalog;

const CUTLINE_SQL: &str =
    "SELECT tier_key_hi, lake_snapshot_id FROM modak.cutline WHERE table_id = $1";

const RETENTION_SQL: &str = "SELECT retention_line FROM modak.cutline WHERE table_id = $1";

const DELTA_SQL: &str = "SELECT pk, op, tier_key, version FROM modak.delta \
     WHERE table_id = $1 \
       AND ($2::bigint IS NULL OR tier_key >= $2) \
       AND ($3::bigint IS NULL OR tier_key <  $3) \
     ORDER BY pk";

pub(crate) fn catalog_err(e: impl core::fmt::Display) -> ModakError {
    ModakError::Catalog(e.to_string())
}

impl CutlineReader for PgCatalog {
    fn current(&self, table: TableId) -> Result<Cutline> {
        Spi::connect(|client| {
            let mut rows = client
                .select(CUTLINE_SQL, Some(1), &[(table.0 as i64).into()])
                .map_err(catalog_err)?;
            let row = rows.next().ok_or(ModakError::UnknownTable(table))?;
            let t = row
                .get_by_name::<i64, _>("tier_key_hi")
                .map_err(catalog_err)?
                .ok_or_else(|| catalog_err("modak.cutline.tier_key_hi is NULL"))?;
            let s = row
                .get_by_name::<i64, _>("lake_snapshot_id")
                .map_err(catalog_err)?
                .ok_or_else(|| catalog_err("modak.cutline.lake_snapshot_id is NULL"))?;
            Ok(Cutline {
                t: TierKey(t),
                snapshot: LakeSnapshotId(s),
            })
        })
    }
}

impl PgCatalog {
    /// The retention line `R` (lake rows with `tier_key < R` are expired),
    /// or `None` when nothing has been expired yet.
    pub fn retention_line(&self, table: TableId) -> Result<Option<TierKey>> {
        Spi::connect(|client| {
            let mut rows = client
                .select(RETENTION_SQL, Some(1), &[(table.0 as i64).into()])
                .map_err(catalog_err)?;
            let row = rows.next().ok_or(ModakError::UnknownTable(table))?;
            let line = row
                .get_by_name::<i64, _>("retention_line")
                .map_err(catalog_err)?;
            Ok(line.map(TierKey))
        })
    }
}

impl DeltaReader for PgCatalog {
    fn overlay(&self, table: TableId, range: KeyRange) -> Result<DeltaSnapshot> {
        let lo = range.lo.map(|k| k.0);
        let hi = range.hi.map(|k| k.0);
        Spi::connect(|client| {
            let rows = client
                .select(
                    DELTA_SQL,
                    None,
                    &[(table.0 as i64).into(), lo.into(), hi.into()],
                )
                .map_err(catalog_err)?;
            let mut entries = Vec::new();
            for row in rows {
                let pk = row
                    .get_by_name::<String, _>("pk")
                    .map_err(catalog_err)?
                    .ok_or_else(|| catalog_err("modak.delta.pk is NULL"))?;
                let op = row
                    .get_by_name::<i16, _>("op")
                    .map_err(catalog_err)?
                    .ok_or_else(|| catalog_err("modak.delta.op is NULL"))?;
                let tier_key = row
                    .get_by_name::<i64, _>("tier_key")
                    .map_err(catalog_err)?
                    .ok_or_else(|| catalog_err("modak.delta.tier_key is NULL"))?;
                let version = row
                    .get_by_name::<i64, _>("version")
                    .map_err(catalog_err)?
                    .ok_or_else(|| catalog_err("modak.delta.version is NULL"))?;
                let op = match op {
                    0 => DeltaOp::Upsert,
                    1 => DeltaOp::Tombstone,
                    other => return Err(catalog_err(format!("unknown modak.delta.op {other}"))),
                };
                entries.push(DeltaEntry {
                    pk: Pk(pk),
                    op,
                    tier_key: TierKey(tier_key),
                    version,
                });
            }
            Ok(DeltaSnapshot { entries })
        })
    }
}
