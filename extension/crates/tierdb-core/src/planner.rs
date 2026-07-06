//! The pure rewrite/route rules of the seam, with no I/O. [`route`] sends each
//! record hot or cold by tier-key vs the cut-line, and [`rewrite`] turns a
//! user query into a recent plus cold-merge [`QueryPlan`].

use crate::domain::{Cutline, DeltaSnapshot, LakeSnapshotId, RouteTarget, TierKey};

/// A minimal representation of the incoming user query.
#[derive(Debug, Clone, Default)]
pub struct UserQuery {
    pub sql: String,
}

/// The recent (hot) branch: scan Postgres partitions where `tier_key >= t_lo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecentBranch {
    pub tier_lo: TierKey,
}

/// The cold branch: scan the lake at `snapshot` where `tier_key < t_hi`,
/// merged against the pinned delta when non-empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdBranch {
    pub tier_hi: TierKey,
    pub snapshot: LakeSnapshotId,
    pub merge_delta: bool,
}

/// The resolved, consistency-complete plan handed to the executor. `T` and
/// `S` are already bound, so the executor performs no consistency reasoning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPlan {
    pub recent: RecentBranch,
    pub cold: ColdBranch,
}

pub fn route(tier_key: TierKey, cut: &Cutline) -> RouteTarget {
    if tier_key >= cut.t {
        RouteTarget::Hot
    } else {
        RouteTarget::Delta
    }
}

pub fn rewrite(_q: &UserQuery, cut: &Cutline, delta: &DeltaSnapshot) -> QueryPlan {
    QueryPlan {
        recent: RecentBranch { tier_lo: cut.t },
        cold: ColdBranch {
            tier_hi: cut.t,
            snapshot: cut.snapshot,
            merge_delta: !delta.is_empty(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{DeltaEntry, DeltaOp, Pk};

    fn cutline() -> Cutline {
        Cutline {
            t: TierKey(100),
            snapshot: LakeSnapshotId(7),
        }
    }

    #[test]
    fn route_boundary_is_half_open_recent_inclusive() {
        let c = cutline();
        assert_eq!(route(TierKey(101), &c), RouteTarget::Hot);
        assert_eq!(route(TierKey(100), &c), RouteTarget::Hot);
        assert_eq!(route(TierKey(99), &c), RouteTarget::Delta);
    }

    #[test]
    fn rewrite_splits_at_cutline_and_binds_snapshot() {
        let c = cutline();
        let plan = rewrite(&UserQuery::default(), &c, &DeltaSnapshot::default());
        assert_eq!(plan.recent.tier_lo, TierKey(100));
        assert_eq!(plan.cold.tier_hi, TierKey(100));
        assert_eq!(plan.cold.snapshot, LakeSnapshotId(7));
        assert!(!plan.cold.merge_delta, "no delta, no merge needed");
    }

    #[test]
    fn rewrite_flags_delta_merge_when_overlay_present() {
        let c = cutline();
        let delta = DeltaSnapshot {
            entries: vec![DeltaEntry {
                pk: Pk("1".into()),
                op: DeltaOp::Upsert,
                tier_key: TierKey(50),
                version: 1,
            }],
        };
        let plan = rewrite(&UserQuery::default(), &c, &delta);
        assert!(plan.cold.merge_delta);
    }
}
