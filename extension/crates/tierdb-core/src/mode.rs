//! | mode                           | cold read              | hot write | cold write        |
//! |--------------------------------|------------------------|-----------|-------------------|
//! | `Tiered`                       | pinned lake ⊕ delta    | heap      | delta buffer      |
//! | `Tiered { keep_heap }`         | pinned lake ⊕ delta    | heap      | heap + delta      |
//! | `Direct`                       | live lake, no overlay  | heap      | lake, synchronous |
//! | `Direct { keep_heap }`         | live lake, no overlay  | heap      | heap + lake       |
//! | `Mirrored`                     | (heap holds every row) | heap      | heap              |
//! | `Mirrored { heap_retention }`  | pinned lake ⊕ delta    | heap      | delta buffer      |
//!
//! Deletes are the dual of inserts; updates compose the two.

use crate::domain::RouteTarget;
use crate::{Result, TierDBError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColdSink {
    Delta,
    Lake,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Tiered { keep_heap: bool },
    Direct { keep_heap: bool },
    Mirrored { heap_retention: bool },
}

impl Mode {
    pub fn from_catalog(
        mode: &str,
        keep_heap: bool,
        heap_retention_lag: Option<i64>,
    ) -> Result<Self> {
        match mode {
            "tiered" => Ok(Self::Tiered { keep_heap }),
            "direct" => Ok(Self::Direct { keep_heap }),
            "mirrored" => Ok(Self::Mirrored {
                heap_retention: heap_retention_lag.is_some(),
            }),
            other => Err(TierDBError::Planning(format!(
                "unknown table mode '{other}'"
            ))),
        }
    }

    pub fn plan_insert(self, target: RouteTarget) -> InsertPlan {
        let cold = target == RouteTarget::Cold;
        match self {
            Self::Tiered { keep_heap } => InsertPlan {
                to_heap: keep_heap || !cold,
                cold: cold.then_some(ColdSink::Delta),
                check_retention: cold,
            },
            Self::Direct { keep_heap } => InsertPlan {
                to_heap: keep_heap || !cold,
                cold: cold.then_some(ColdSink::Lake),
                check_retention: cold,
            },
            Self::Mirrored {
                heap_retention: true,
            } => InsertPlan {
                to_heap: !cold,
                cold: cold.then_some(ColdSink::Delta),
                check_retention: false,
            },
            Self::Mirrored {
                heap_retention: false,
            } => InsertPlan {
                to_heap: true,
                cold: None,
                check_retention: false,
            },
        }
    }

    pub fn routes_by_cut(self) -> bool {
        matches!(
            self,
            Self::Tiered { keep_heap: false }
                | Self::Direct { keep_heap: false }
                | Self::Mirrored {
                    heap_retention: true
                }
        )
    }

    pub fn heap_complete(self) -> bool {
        matches!(
            self,
            Self::Mirrored {
                heap_retention: false
            }
        )
    }

    pub fn is_direct(self) -> bool {
        matches!(self, Self::Direct { .. })
    }

    pub fn plan_delete(self, target: RouteTarget) -> DeletePlan {
        let ins = self.plan_insert(target);
        DeletePlan {
            from_heap: ins.to_heap,
            cold: ins.cold,
            heap_floor: self.routes_by_cut() && target == RouteTarget::Hot,
            check_retention: self.routes_by_cut() && target == RouteTarget::Cold,
        }
    }

    pub fn plan_update(self, old_target: RouteTarget, new_target: RouteTarget) -> UpdatePlan {
        UpdatePlan {
            remove_old: self.plan_delete(old_target),
            write_new: self.plan_insert(new_target),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InsertPlan {
    pub to_heap: bool,
    pub cold: Option<ColdSink>,
    pub check_retention: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeletePlan {
    pub from_heap: bool,
    pub heap_floor: bool,
    pub cold: Option<ColdSink>,
    pub check_retention: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdatePlan {
    pub remove_old: DeletePlan,
    pub write_new: InsertPlan,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::RouteTarget::{Cold, Hot};

    const ALL: [Mode; 6] = [
        Mode::Tiered { keep_heap: false },
        Mode::Tiered { keep_heap: true },
        Mode::Direct { keep_heap: false },
        Mode::Direct { keep_heap: true },
        Mode::Mirrored {
            heap_retention: false,
        },
        Mode::Mirrored {
            heap_retention: true,
        },
    ];

    #[test]
    fn the_insert_routing_matrix() {
        let expect = |to_heap, cold, check_retention| InsertPlan {
            to_heap,
            cold,
            check_retention,
        };
        let matrix = [
            (
                Mode::Tiered { keep_heap: false },
                Hot,
                expect(true, None, false),
            ),
            (
                Mode::Tiered { keep_heap: false },
                Cold,
                expect(false, Some(ColdSink::Delta), true),
            ),
            (
                Mode::Tiered { keep_heap: true },
                Hot,
                expect(true, None, false),
            ),
            (
                Mode::Tiered { keep_heap: true },
                Cold,
                expect(true, Some(ColdSink::Delta), true),
            ),
            (
                Mode::Direct { keep_heap: false },
                Hot,
                expect(true, None, false),
            ),
            (
                Mode::Direct { keep_heap: false },
                Cold,
                expect(false, Some(ColdSink::Lake), true),
            ),
            (
                Mode::Direct { keep_heap: true },
                Hot,
                expect(true, None, false),
            ),
            (
                Mode::Direct { keep_heap: true },
                Cold,
                expect(true, Some(ColdSink::Lake), true),
            ),
            (
                Mode::Mirrored {
                    heap_retention: false,
                },
                Hot,
                expect(true, None, false),
            ),
            (
                Mode::Mirrored {
                    heap_retention: false,
                },
                Cold,
                expect(true, None, false),
            ),
            (
                Mode::Mirrored {
                    heap_retention: true,
                },
                Hot,
                expect(true, None, false),
            ),
            (
                Mode::Mirrored {
                    heap_retention: true,
                },
                Cold,
                expect(false, Some(ColdSink::Delta), false),
            ),
        ];
        for (mode, target, want) in matrix {
            assert_eq!(mode.plan_insert(target), want, "{mode:?} {target:?}");
        }
    }

    #[test]
    fn delete_is_the_dual_of_insert() {
        for mode in ALL {
            for target in [Hot, Cold] {
                let ins = mode.plan_insert(target);
                let del = mode.plan_delete(target);
                assert_eq!(del.from_heap, ins.to_heap, "{mode:?} {target:?}");
                assert_eq!(del.cold, ins.cold, "{mode:?} {target:?}");
            }
        }
    }

    #[test]
    fn heap_complete_and_direct() {
        assert!(Mode::Mirrored {
            heap_retention: false
        }
        .heap_complete());
        assert!(!Mode::Mirrored {
            heap_retention: true
        }
        .heap_complete());
        assert!(Mode::Direct { keep_heap: false }.is_direct());
        assert!(!Mode::Tiered { keep_heap: false }.is_direct());
    }

    #[test]
    fn from_catalog_round_trips() {
        assert_eq!(
            Mode::from_catalog("tiered", true, None).unwrap(),
            Mode::Tiered { keep_heap: true }
        );
        assert_eq!(
            Mode::from_catalog("direct", false, None).unwrap(),
            Mode::Direct { keep_heap: false }
        );
        assert_eq!(
            Mode::from_catalog("mirrored", false, Some(7)).unwrap(),
            Mode::Mirrored {
                heap_retention: true
            }
        );
        assert_eq!(
            Mode::from_catalog("mirrored", false, None).unwrap(),
            Mode::Mirrored {
                heap_retention: false
            }
        );
    }
}
