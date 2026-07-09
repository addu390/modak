//! Value types of the hot/cold seam. Deliberately lake-format-agnostic: the
//! cold store is referred to only through a pinned version id
//! ([`LakeSnapshotId`]) and opaque handles held elsewhere.

/// Identity of a TierDB-managed logical table (a Postgres relation OID in practice).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableId(pub u32);

/// A value along a table's tier-key (e.g. epoch micros). Defines data
/// temperature and aging order. A write that changes a row's tier-key moves
/// the row to the side of the cut-line the new value falls on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TierKey(pub i64);

/// A pinned version of the cold store, format-agnostic. Modeled as a monotonic
/// `i64`, which fits every long-versioned lake format (Iceberg, Paimon, Delta).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LakeSnapshotId(pub i64);

/// A read-pin handle (row id in `tierdb.read_pins`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PinId(pub i64);

/// Canonical text form of a primary key (the merge key across tiers). A String
/// keeps the read-path merge portable across the Postgres and DuckDB executors.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Pk(pub String);

/// The cut-line. Rows with `tier_key >= t` live in Postgres, rows below in
/// the cold base at version `snapshot`. `t` and `snapshot` always advance
/// together as one atomic fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cutline {
    pub t: TierKey,
    pub snapshot: LakeSnapshotId,
}

/// A half-open range over the tier-key, used for pushdown/pruning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyRange {
    pub lo: Option<TierKey>,
    pub hi: Option<TierKey>,
}

impl KeyRange {
    pub const UNBOUNDED: KeyRange = KeyRange { lo: None, hi: None };
}

/// A single override for a cold PK. Newest `version` wins, `Tombstone` removes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaOp {
    Upsert,
    Tombstone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaEntry {
    pub pk: Pk,
    pub op: DeltaOp,
    pub tier_key: TierKey,
    pub version: i64,
}

/// The pinned correction overlay for cold rows, as read within a query's MVCC
/// snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeltaSnapshot {
    pub entries: Vec<DeltaEntry>,
}

impl DeltaSnapshot {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Which side of the cut-line a record falls on: `Hot` (`>= T`) or `Cold` (`< T`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteTarget {
    Hot,
    Cold,
}
