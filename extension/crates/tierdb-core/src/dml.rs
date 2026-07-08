//! The pure DML half of transparent writes. Classifies a statement's tier-key
//! predicate against the cut-line and renders the rewritten UPDATE or DELETE
//! whose cold half writes `tierdb.delta` through a data-modifying CTE.

use crate::domain::{RouteTarget, TierKey};
use crate::sqlgen::{pk_sql_expr, render_cold_branch_spooled, LakePin, TableMeta};
use crate::{Result, TierDBError};

/// A comparison of the tier-key column against a constant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
}

/// What a WHERE clause says about the tier key, as much of it as the adapter
/// could extract. Anything it cannot prove becomes [`TierPredicate::Unknown`].
#[derive(Debug, Clone)]
pub enum TierPredicate {
    Cmp(CmpOp, i64),
    And(Vec<TierPredicate>),
    Or(Vec<TierPredicate>),
    Unknown,
}

/// Which side of the cut-line the matched rows can be on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Classification {
    Hot,
    Cold,
    Mixed,
}

pub fn classify(p: &TierPredicate, t: TierKey) -> Classification {
    match p {
        TierPredicate::Cmp(op, c) => classify_cmp(*op, *c, t.0),
        TierPredicate::And(parts) => {
            if parts.iter().any(|p| classify(p, t) == Classification::Hot) {
                Classification::Hot
            } else if parts.iter().any(|p| classify(p, t) == Classification::Cold) {
                Classification::Cold
            } else {
                Classification::Mixed
            }
        }
        TierPredicate::Or(parts) => {
            if parts.iter().all(|p| classify(p, t) == Classification::Hot) {
                Classification::Hot
            } else if !parts.is_empty()
                && parts.iter().all(|p| classify(p, t) == Classification::Cold)
            {
                Classification::Cold
            } else {
                Classification::Mixed
            }
        }
        TierPredicate::Unknown => Classification::Mixed,
    }
}

fn classify_cmp(op: CmpOp, c: i64, t: i64) -> Classification {
    match op {
        CmpOp::Eq => {
            if c >= t {
                Classification::Hot
            } else {
                Classification::Cold
            }
        }
        CmpOp::Ge => {
            if c >= t {
                Classification::Hot
            } else {
                Classification::Mixed
            }
        }
        CmpOp::Gt => {
            if c >= t - 1 {
                Classification::Hot
            } else {
                Classification::Mixed
            }
        }
        CmpOp::Lt => {
            if c <= t {
                Classification::Cold
            } else {
                Classification::Mixed
            }
        }
        CmpOp::Le => {
            if c < t {
                Classification::Cold
            } else {
                Classification::Mixed
            }
        }
    }
}

/// Deparsed fragments of the original statement, over unqualified column
/// names so the same text binds in both halves.
#[derive(Debug, Clone, Default)]
pub struct DmlFragments {
    pub where_sql: Option<String>,
    pub set_items: Vec<(String, String)>,
    pub returning: Vec<(String, String)>,
}

fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// The pieces shared by both verbs, the cold source with the statement's
/// WHERE applied and the hot half's WHERE with the tier bound added.
struct Halves {
    cold_from: String,
    cold_where: String,
    hot_where: String,
}

fn halves(meta: &TableMeta, t: TierKey, pin: &LakePin, frag: &DmlFragments) -> Result<Halves> {
    let cold_branch = render_cold_branch_spooled(t, meta, pin)?;
    let tier = ident(&meta.tier_key_col);
    let t_lit = meta.tier_key_type.pg_literal(t.0);
    // The always-true gate references the CTE so the delta write runs during
    // ExecutorRun, where the command tag still sees the tallied cold rows. An
    // unreferenced modifying CTE only runs at ExecutorFinish, too late.
    let gate = "(SELECT count(*) FROM __tierdb_cold) IS NOT NULL";
    let (cold_where, hot_where) = match &frag.where_sql {
        Some(w) => (
            format!("({w})"),
            format!("({w}) AND m.{tier} >= {t_lit} AND {gate}"),
        ),
        None => ("true".into(), format!("m.{tier} >= {t_lit} AND {gate}")),
    };
    Ok(Halves {
        cold_from: format!("({cold_branch}) m", cold_branch = cold_branch),
        cold_where,
        hot_where,
    })
}

/// Wraps the tier key in `tierdb_cold_dml`, which checks the retention floor,
/// tallies the row, stashes its RETURNING outputs, and passes the key through.
fn guarded_tier(tier_expr: &str, retention: Option<i64>, frag: &DmlFragments) -> String {
    let r = match retention {
        Some(r) => r.to_string(),
        None => "NULL::bigint".into(),
    };
    let stash = if frag.returning.is_empty() {
        "NULL::text[]".into()
    } else {
        let elems = frag
            .returning
            .iter()
            .map(|(_, e)| format!("({e})::text"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("ARRAY[{elems}]")
    };
    format!("tierdb_cold_dml({tier_expr}, {r}, {stash})")
}

// old_tier_key must keep pointing at the partition the lake still holds the
// image in, which the existing delta row knows better than an incoming write.
pub const DELTA_CONFLICT_ARM: &str = "ON CONFLICT (table_id, pk) DO UPDATE\n\
       SET op = EXCLUDED.op, tier_key = EXCLUDED.tier_key,\n\
           old_tier_key = NULLIF(COALESCE(d.old_tier_key, d.tier_key), EXCLUDED.tier_key),\n\
           version = EXCLUDED.version, payload = EXCLUDED.payload, updated_at = now()\n\
       WHERE EXCLUDED.version >= d.version";

pub const DELTA_OP_UPSERT: i16 = 0;
pub const DELTA_OP_TOMBSTONE: i16 = 1;

fn delta_conflict_returning() -> String {
    format!("{DELTA_CONFLICT_ARM}\nRETURNING 1")
}

/// Parameterized single-row delta write, plain SQL that runs the same under SPI
/// or libpq. Params: $1 table_id, $2 pk, $3 op, $4 tier_key, $5 payload.
pub fn delta_write_sql() -> String {
    format!(
        "INSERT INTO tierdb.delta AS d \
           (table_id, pk, op, tier_key, version, payload)\n\
         VALUES ($1, $2, $3, $4, nextval('tierdb.delta_version'), $5)\n\
         {DELTA_CONFLICT_ARM}"
    )
}

/// Heap insert of a full row supplied as jsonb ($1).
pub fn heap_insert_from_jsonb_sql(schema: &str, table: &str) -> String {
    let rel = format!("{}.{}", ident(schema), ident(table));
    format!("INSERT INTO {rel} SELECT * FROM jsonb_populate_record(NULL::{rel}, $1)")
}

/// Heap delete of one row by its primary key, matched as text so the same
/// text parameters bind regardless of the key's stored type. `floor`, when
/// present, is a pre-rendered `"tier_col" >= <literal>` guard that keeps a
/// concurrent tier move from letting a hot delete touch a row gone cold.
pub fn heap_delete_by_pk_sql(
    schema: &str,
    table: &str,
    pk_cols: &[String],
    floor: Option<&str>,
) -> String {
    let conditions = pk_cols
        .iter()
        .enumerate()
        .map(|(i, c)| format!("{}::text = ${}", ident(c), i + 1))
        .collect::<Vec<_>>()
        .join(" AND ");
    let bound = floor.map(|f| format!(" AND {f}")).unwrap_or_default();
    format!(
        "DELETE FROM {}.{} WHERE {conditions}{bound}",
        ident(schema),
        ident(table),
    )
}

/// A cold write below the lake's retention line can never be folded back.
pub fn retention_rejects(tier_key: i64, retention_line: Option<i64>) -> bool {
    matches!(retention_line, Some(line) if tier_key < line)
}

/// A table's persistence shape. Each variant carries only the option that
/// variant can legally hold, so callers pass one value instead of a flag soup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableShape {
    Tiered { keep_heap: bool },
    Mirrored { heap_retention: bool },
}

impl TableShape {
    pub fn from_catalog(
        mode: &str,
        keep_heap: bool,
        heap_retention_lag: Option<i64>,
    ) -> Result<Self> {
        match mode {
            "tiered" => Ok(Self::Tiered { keep_heap }),
            "mirrored" => Ok(Self::Mirrored {
                heap_retention: heap_retention_lag.is_some(),
            }),
            other => Err(TierDBError::Planning(format!(
                "unknown table mode '{other}'"
            ))),
        }
    }

    /// The single source of routing policy: where one incoming row must land.
    pub fn plan_insert(self, target: RouteTarget) -> InsertPlan {
        let cold = target == RouteTarget::Delta;
        match self {
            Self::Tiered { keep_heap } => InsertPlan {
                to_heap: keep_heap || !cold,
                to_delta: cold,
                check_retention: cold,
            },
            Self::Mirrored {
                heap_retention: true,
            } => InsertPlan {
                to_heap: !cold,
                to_delta: cold,
                check_retention: false,
            },
            Self::Mirrored {
                heap_retention: false,
            } => InsertPlan {
                to_heap: true,
                to_delta: false,
                check_retention: false,
            },
        }
    }

    /// True when the shape splits rows across tiers by the cut-line, so a hot
    /// row lives only on the heap and a cold row only in the lake overlay.
    /// The full-heap shapes (keep-heap tiered, fully mirrored) hold every row
    /// on the heap and never route a live row by the cut.
    fn routes_by_cut(self) -> bool {
        matches!(
            self,
            Self::Tiered { keep_heap: false }
                | Self::Mirrored {
                    heap_retention: true
                }
        )
    }

    /// The delete policy is the dual of the insert policy: a row is removed from
    /// exactly the tiers an insert of it would have written. The heap floor
    /// guards a routed hot delete against a row that has since gone cold.
    pub fn plan_delete(self, target: RouteTarget) -> DeletePlan {
        let ins = self.plan_insert(target);
        DeletePlan {
            from_heap: ins.to_heap,
            tombstone: ins.to_delta,
            heap_floor: self.routes_by_cut() && target == RouteTarget::Hot,
            check_retention: self.routes_by_cut() && target == RouteTarget::Delta,
        }
    }

    /// An update touches both tiers as a remove of the row's old placement plus
    /// a write of its new placement, which also carries cross-tier moves and
    /// primary-key changes without any special casing.
    pub fn plan_update(self, old_target: RouteTarget, new_target: RouteTarget) -> UpdatePlan {
        UpdatePlan {
            remove_old: self.plan_delete(old_target),
            write_new: self.plan_insert(new_target),
        }
    }
}

/// Where a single incoming row must land.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InsertPlan {
    pub to_heap: bool,
    pub to_delta: bool,
    pub check_retention: bool,
}

/// Which tiers one matched row must be removed from. `heap_floor` asks the heap
/// delete to carry the tier guard; `tombstone` writes a `tierdb.delta` marker so
/// a folded lake row disappears from seam reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeletePlan {
    pub from_heap: bool,
    pub heap_floor: bool,
    pub tombstone: bool,
    pub check_retention: bool,
}

/// An update as its two touch-both-tiers halves: remove the row's old placement,
/// then write its new placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdatePlan {
    pub remove_old: DeletePlan,
    pub write_new: InsertPlan,
}

fn pk_pairs(meta: &TableMeta) -> String {
    meta.pk_cols
        .iter()
        .map(|c| format!("{}, m.{}", lit(c), ident(c)))
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn render_update(
    meta: &TableMeta,
    t: TierKey,
    pin: &LakePin,
    retention: Option<i64>,
    frag: &DmlFragments,
) -> Result<String> {
    if frag.set_items.is_empty() {
        return Err(TierDBError::Planning("UPDATE with no SET items".into()));
    }
    let h = halves(meta, t, pin, frag)?;
    let tier = ident(&meta.tier_key_col);
    let moves_tier = frag.set_items.iter().any(|(c, _)| *c == meta.tier_key_col);

    // The new row image, SET expressions over the old values where assigned
    // and the old value elsewhere. Everything downstream reads from it.
    let mut projection = meta
        .columns
        .iter()
        .map(
            |c| match frag.set_items.iter().find(|(col, _)| *col == c.name) {
                Some((_, expr)) => format!("({expr}) AS {}", ident(&c.name)),
                None => format!("m.{}", ident(&c.name)),
            },
        )
        .collect::<Vec<_>>();
    if moves_tier {
        projection.push(format!("m.{tier} AS __tierdb_old_tier"));
    }
    let new_image = format!(
        "SELECT {} FROM {} WHERE {}",
        projection.join(", "),
        h.cold_from,
        h.cold_where,
    );

    // Tier moves decompose per row. A row still cold becomes a delta upsert
    // at the new tier remembering the old one. A row now hot becomes a delta
    // tombstone at the lake's partition plus a heap insert (__tierdb_move).
    let t_lit = meta.tier_key_type.pg_literal(t.0);
    let new_c = meta.tier_key_type.canonical_expr(&format!("m.{tier}"));
    let old_c = meta.tier_key_type.canonical_expr("m.__tierdb_old_tier");
    let (op, delta_tier, old_tier, payload) = if moves_tier {
        (
            format!("CASE WHEN m.{tier} >= {t_lit} THEN 1 ELSE 0 END"),
            format!("CASE WHEN m.{tier} >= {t_lit} THEN {old_c} ELSE {new_c} END"),
            format!("CASE WHEN m.{tier} < {t_lit} THEN NULLIF({old_c}, {new_c}) END"),
            format!(
                "CASE WHEN m.{tier} >= {t_lit} THEN jsonb_build_object({}) \
                 ELSE to_jsonb(m) - '__tierdb_old_tier' END",
                pk_pairs(meta),
            ),
        )
    } else {
        (
            "0".into(),
            new_c.clone(),
            "NULL::bigint".into(),
            "to_jsonb(m)".into(),
        )
    };

    let move_cte = if moves_tier {
        let cols = meta
            .columns
            .iter()
            .map(|c| ident(&c.name))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            ",\n__tierdb_move AS (\n\
               INSERT INTO {} ({cols})\n\
               SELECT {cols} FROM __tierdb_new m WHERE m.{tier} >= {t_lit}\n\
               RETURNING 1\n\
             )",
            hot_rel(meta),
        )
    } else {
        String::new()
    };

    let set_list = frag
        .set_items
        .iter()
        .map(|(col, expr)| format!("{} = ({expr})", ident(col)))
        .collect::<Vec<_>>()
        .join(", ");
    let delta_conflict = delta_conflict_returning();
    Ok(format!(
        "WITH __tierdb_new AS (\n\
           {new_image}\n\
         ),\n\
         __tierdb_cold AS (\n\
           INSERT INTO tierdb.delta AS d \
             (table_id, pk, op, tier_key, old_tier_key, version, payload)\n\
           SELECT {table_id}, {pk}, {op}, {guarded}, {old_tier},\n\
                  nextval('tierdb.delta_version'), {payload}\n\
           FROM __tierdb_new m\n\
           {delta_conflict}\n\
         ){move_cte}\n\
         UPDATE {hot_rel} m SET {set_list} WHERE {hot_where}{ret}",
        table_id = meta.table_id.0,
        pk = pk_sql_expr("m", &meta.pk_cols),
        guarded = guarded_tier(&delta_tier, retention, frag),
        hot_rel = hot_rel(meta),
        hot_where = h.hot_where,
        ret = returning_clause(frag),
    ))
}

pub fn render_delete(
    meta: &TableMeta,
    t: TierKey,
    pin: &LakePin,
    retention: Option<i64>,
    frag: &DmlFragments,
) -> Result<String> {
    let h = halves(meta, t, pin, frag)?;
    let delta_conflict = delta_conflict_returning();
    Ok(format!(
        "WITH __tierdb_cold AS (\n\
           INSERT INTO tierdb.delta AS d \
             (table_id, pk, op, tier_key, old_tier_key, version, payload)\n\
           SELECT {table_id}, {pk}, 1, {guarded}, NULL::bigint,\n\
                  nextval('tierdb.delta_version'), jsonb_build_object({pk_pairs})\n\
           FROM {cold_from} WHERE {cold_where}\n\
           {delta_conflict}\n\
         )\n\
         DELETE FROM {hot_rel} m WHERE {hot_where}{ret}",
        table_id = meta.table_id.0,
        pk = pk_sql_expr("m", &meta.pk_cols),
        guarded = guarded_tier(
            &meta
                .tier_key_type
                .canonical_expr(&format!("m.{}", ident(&meta.tier_key_col))),
            retention,
            frag
        ),
        pk_pairs = pk_pairs(meta),
        cold_from = h.cold_from,
        cold_where = h.cold_where,
        hot_rel = hot_rel(meta),
        hot_where = h.hot_where,
        ret = returning_clause(frag),
    ))
}

fn hot_rel(meta: &TableMeta) -> String {
    format!("{}.{}", ident(&meta.hot_schema), ident(&meta.hot_table))
}

fn returning_clause(frag: &DmlFragments) -> String {
    if frag.returning.is_empty() {
        String::new()
    } else {
        let items = frag
            .returning
            .iter()
            .map(|(name, expr)| format!("({expr}) AS {}", ident(name)))
            .collect::<Vec<_>>()
            .join(", ");
        format!(" RETURNING {items}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::TableId;
    use crate::sqlgen::Column;

    const T: TierKey = TierKey(100);

    fn cmp(op: CmpOp, c: i64) -> TierPredicate {
        TierPredicate::Cmp(op, c)
    }

    #[test]
    fn equality_proves_a_side_exactly() {
        assert_eq!(classify(&cmp(CmpOp::Eq, 100), T), Classification::Hot);
        assert_eq!(classify(&cmp(CmpOp::Eq, 99), T), Classification::Cold);
    }

    #[test]
    fn tiered_routes_hot_to_heap_and_cold_to_delta() {
        use crate::domain::RouteTarget::{Delta, Hot};
        let shape = TableShape::Tiered { keep_heap: false };
        assert_eq!(
            shape.plan_insert(Hot),
            InsertPlan {
                to_heap: true,
                to_delta: false,
                check_retention: false
            }
        );
        assert_eq!(
            shape.plan_insert(Delta),
            InsertPlan {
                to_heap: false,
                to_delta: true,
                check_retention: true
            }
        );
    }

    #[test]
    fn keep_heap_tiered_always_keeps_the_heap_and_mirrors_cold() {
        use crate::domain::RouteTarget::{Delta, Hot};
        let shape = TableShape::Tiered { keep_heap: true };
        assert_eq!(
            shape.plan_insert(Hot),
            InsertPlan {
                to_heap: true,
                to_delta: false,
                check_retention: false
            }
        );
        assert_eq!(
            shape.plan_insert(Delta),
            InsertPlan {
                to_heap: true,
                to_delta: true,
                check_retention: true
            }
        );
    }

    #[test]
    fn fully_mirrored_writes_heap_only_regardless_of_side() {
        use crate::domain::RouteTarget::{Delta, Hot};
        let shape = TableShape::Mirrored {
            heap_retention: false,
        };
        let expect = InsertPlan {
            to_heap: true,
            to_delta: false,
            check_retention: false,
        };
        assert_eq!(shape.plan_insert(Hot), expect);
        assert_eq!(shape.plan_insert(Delta), expect);
    }

    #[test]
    fn mirrored_with_heap_retention_routes_like_tiered_without_lake_expiry() {
        use crate::domain::RouteTarget::{Delta, Hot};
        let shape = TableShape::Mirrored {
            heap_retention: true,
        };
        assert_eq!(
            shape.plan_insert(Hot),
            InsertPlan {
                to_heap: true,
                to_delta: false,
                check_retention: false
            }
        );
        assert_eq!(
            shape.plan_insert(Delta),
            InsertPlan {
                to_heap: false,
                to_delta: true,
                check_retention: false
            }
        );
    }

    #[test]
    fn delete_is_the_dual_of_insert_for_every_shape() {
        use crate::domain::RouteTarget::{Delta, Hot};
        for shape in [
            TableShape::Tiered { keep_heap: false },
            TableShape::Tiered { keep_heap: true },
            TableShape::Mirrored {
                heap_retention: false,
            },
            TableShape::Mirrored {
                heap_retention: true,
            },
        ] {
            for target in [Hot, Delta] {
                let ins = shape.plan_insert(target);
                let del = shape.plan_delete(target);
                assert_eq!(del.from_heap, ins.to_heap, "{shape:?} {target:?}");
                assert_eq!(del.tombstone, ins.to_delta, "{shape:?} {target:?}");
            }
        }
    }

    #[test]
    fn tiered_delete_removes_from_one_side_by_the_cut() {
        use crate::domain::RouteTarget::{Delta, Hot};
        let shape = TableShape::Tiered { keep_heap: false };
        assert_eq!(
            shape.plan_delete(Hot),
            DeletePlan {
                from_heap: true,
                heap_floor: true,
                tombstone: false,
                check_retention: false
            }
        );
        assert_eq!(
            shape.plan_delete(Delta),
            DeletePlan {
                from_heap: false,
                heap_floor: false,
                tombstone: true,
                check_retention: true
            }
        );
    }

    #[test]
    fn keep_heap_tiered_delete_touches_the_heap_and_tombstones_only_cold() {
        use crate::domain::RouteTarget::{Delta, Hot};
        let shape = TableShape::Tiered { keep_heap: true };
        assert_eq!(
            shape.plan_delete(Hot),
            DeletePlan {
                from_heap: true,
                heap_floor: false,
                tombstone: false,
                check_retention: false
            }
        );
        assert_eq!(
            shape.plan_delete(Delta),
            DeletePlan {
                from_heap: true,
                heap_floor: false,
                tombstone: true,
                check_retention: false
            }
        );
    }

    #[test]
    fn fully_mirrored_delete_is_heap_only() {
        use crate::domain::RouteTarget::{Delta, Hot};
        let shape = TableShape::Mirrored {
            heap_retention: false,
        };
        let heap_only = DeletePlan {
            from_heap: true,
            heap_floor: false,
            tombstone: false,
            check_retention: false,
        };
        assert_eq!(shape.plan_delete(Hot), heap_only);
        assert_eq!(shape.plan_delete(Delta), heap_only);
    }

    #[test]
    fn mirrored_with_heap_retention_deletes_like_tiered() {
        use crate::domain::RouteTarget::{Delta, Hot};
        let shape = TableShape::Mirrored {
            heap_retention: true,
        };
        assert_eq!(
            shape.plan_delete(Hot),
            DeletePlan {
                from_heap: true,
                heap_floor: true,
                tombstone: false,
                check_retention: false
            }
        );
        assert_eq!(
            shape.plan_delete(Delta),
            DeletePlan {
                from_heap: false,
                heap_floor: false,
                tombstone: true,
                check_retention: true
            }
        );
    }

    #[test]
    fn lower_bounds_prove_hot_only_at_or_above_t() {
        assert_eq!(classify(&cmp(CmpOp::Ge, 100), T), Classification::Hot);
        assert_eq!(classify(&cmp(CmpOp::Ge, 99), T), Classification::Mixed);
        assert_eq!(classify(&cmp(CmpOp::Gt, 99), T), Classification::Hot);
        assert_eq!(classify(&cmp(CmpOp::Gt, 98), T), Classification::Mixed);
    }

    #[test]
    fn upper_bounds_prove_cold_only_at_or_below_t() {
        assert_eq!(classify(&cmp(CmpOp::Lt, 100), T), Classification::Cold);
        assert_eq!(classify(&cmp(CmpOp::Lt, 101), T), Classification::Mixed);
        assert_eq!(classify(&cmp(CmpOp::Le, 99), T), Classification::Cold);
        assert_eq!(classify(&cmp(CmpOp::Le, 100), T), Classification::Mixed);
    }

    #[test]
    fn and_takes_the_strongest_part_even_with_unknowns() {
        let p = TierPredicate::And(vec![TierPredicate::Unknown, cmp(CmpOp::Ge, 100)]);
        assert_eq!(classify(&p, T), Classification::Hot);
        let p = TierPredicate::And(vec![cmp(CmpOp::Lt, 50), TierPredicate::Unknown]);
        assert_eq!(classify(&p, T), Classification::Cold);
        let p = TierPredicate::And(vec![TierPredicate::Unknown]);
        assert_eq!(classify(&p, T), Classification::Mixed);
    }

    #[test]
    fn or_needs_every_arm_on_the_same_side() {
        let hot = TierPredicate::Or(vec![cmp(CmpOp::Eq, 100), cmp(CmpOp::Eq, 150)]);
        assert_eq!(classify(&hot, T), Classification::Hot);
        let cold = TierPredicate::Or(vec![cmp(CmpOp::Eq, 10), cmp(CmpOp::Eq, 20)]);
        assert_eq!(classify(&cold, T), Classification::Cold);
        let mixed = TierPredicate::Or(vec![cmp(CmpOp::Eq, 10), cmp(CmpOp::Eq, 150)]);
        assert_eq!(classify(&mixed, T), Classification::Mixed);
        let unknown_arm = TierPredicate::Or(vec![cmp(CmpOp::Eq, 150), TierPredicate::Unknown]);
        assert_eq!(classify(&unknown_arm, T), Classification::Mixed);
    }

    #[test]
    fn empty_or_matches_nothing_and_stays_untouched() {
        assert_eq!(classify(&TierPredicate::Or(vec![]), T), Classification::Hot);
    }

    #[test]
    fn no_predicate_is_mixed() {
        assert_eq!(classify(&TierPredicate::Unknown, T), Classification::Mixed);
        assert_eq!(
            classify(&TierPredicate::And(vec![]), T),
            Classification::Mixed
        );
    }

    fn meta() -> TableMeta {
        TableMeta {
            table_id: TableId(90001),
            hot_schema: "public".into(),
            hot_table: "events".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    sql_type: "bigint".into(),
                },
                Column {
                    name: "event_time".into(),
                    sql_type: "bigint".into(),
                },
                Column {
                    name: "val".into(),
                    sql_type: "text".into(),
                },
            ],
            pk_cols: vec!["id".into()],
            tier_key_col: "event_time".into(),
            tier_key_type: crate::TierKeyType::Bigint,
        }
    }

    fn pin() -> LakePin {
        LakePin::Iceberg {
            metadata_location: "/wh/events/metadata/00002-abc.metadata.json".into(),
        }
    }

    #[test]
    fn update_splits_into_delta_cte_and_bounded_heap_update() {
        let frag = DmlFragments {
            where_sql: Some("val = 'x'".into()),
            set_items: vec![("val".into(), "'y'".into())],
            returning: vec![],
        };
        let sql = render_update(&meta(), T, &pin(), None, &frag).unwrap();
        assert!(sql.starts_with("WITH __tierdb_new AS ("), "{sql}");
        assert!(!sql.contains("__tierdb_move"), "no tier move here:\n{sql}");
        assert!(
            sql.contains("SELECT m.\"id\", m.\"event_time\", ('y') AS \"val\" FROM"),
            "cold source projects the new row image:\n{sql}"
        );
        assert!(sql.contains("to_jsonb(m)"), "{sql}");
        assert!(
            sql.contains("tierdb_cold_dml(m.\"event_time\", NULL::bigint, NULL::text[])"),
            "{sql}"
        );
        assert!(
            sql.contains("WHERE (val = 'x')"),
            "cold half filters by the original WHERE:\n{sql}"
        );
        assert!(
            sql.ends_with(
                "UPDATE \"public\".\"events\" m SET \"val\" = ('y') \
                 WHERE (val = 'x') AND m.\"event_time\" >= 100 \
                 AND (SELECT count(*) FROM __tierdb_cold) IS NOT NULL"
            ),
            "{sql}"
        );
    }

    #[test]
    fn dml_sources_cold_rows_through_the_spool_not_the_direct_scan() {
        let frag = DmlFragments {
            where_sql: Some("id = 3".into()),
            set_items: vec![("val".into(), "'y'".into())],
            returning: vec![],
        };
        let update = render_update(&meta(), T, &pin(), None, &frag).unwrap();
        let delete = render_delete(&meta(), T, &pin(), None, &frag).unwrap();
        for sql in [&update, &delete] {
            assert!(
                sql.contains(
                    "tierdb_lake_rows(90001, 100, \
                     'iceberg\u{1f}/wh/events/metadata/00002-abc.metadata.json')"
                ),
                "cold half spools the lake scan (pg_duckdb cannot feed a write):\n{sql}"
            );
            assert!(
                !sql.contains("duckdb.query"),
                "no direct DuckDB scan inside a modifying statement:\n{sql}"
            );
        }
    }

    #[test]
    fn update_returning_stashes_new_image_expressions() {
        let frag = DmlFragments {
            where_sql: Some("id = 3".into()),
            set_items: vec![("val".into(), "'y'".into())],
            returning: vec![
                ("id".into(), "id".into()),
                ("tag".into(), "(val || '!')".into()),
            ],
        };
        let sql = render_update(&meta(), T, &pin(), None, &frag).unwrap();
        assert!(
            sql.contains(
                "tierdb_cold_dml(m.\"event_time\", NULL::bigint, \
                 ARRAY[(id)::text, ((val || '!'))::text])"
            ),
            "stash evaluates over the new-image projection:\n{sql}"
        );
        assert!(
            sql.ends_with(" RETURNING (id) AS \"id\", ((val || '!')) AS \"tag\""),
            "{sql}"
        );
    }

    #[test]
    fn delete_writes_tombstones_with_pk_payload() {
        let frag = DmlFragments {
            where_sql: Some("id = 3".into()),
            set_items: vec![],
            returning: vec![],
        };
        let sql = render_delete(&meta(), T, &pin(), Some(40), &frag).unwrap();
        assert!(
            sql.contains(", 1, tierdb_cold_dml(m.\"event_time\", 40, NULL::text[]),"),
            "{sql}"
        );
        assert!(sql.contains("jsonb_build_object('id', m.\"id\")"), "{sql}");
        assert!(
            sql.ends_with(
                "DELETE FROM \"public\".\"events\" m \
                 WHERE (id = 3) AND m.\"event_time\" >= 100 \
                 AND (SELECT count(*) FROM __tierdb_cold) IS NOT NULL"
            ),
            "{sql}"
        );
    }

    #[test]
    fn delete_returning_stashes_old_image_expressions() {
        let frag = DmlFragments {
            where_sql: Some("id = 3".into()),
            set_items: vec![],
            returning: vec![("val".into(), "val".into())],
        };
        let sql = render_delete(&meta(), T, &pin(), None, &frag).unwrap();
        assert!(
            sql.contains("tierdb_cold_dml(m.\"event_time\", NULL::bigint, ARRAY[(val)::text])"),
            "{sql}"
        );
        assert!(sql.ends_with(" RETURNING (val) AS \"val\""), "{sql}");
    }

    #[test]
    fn missing_where_updates_both_tiers_entirely() {
        let frag = DmlFragments {
            where_sql: None,
            set_items: vec![("val".into(), "'y'".into())],
            returning: vec![],
        };
        let sql = render_update(&meta(), T, &pin(), None, &frag).unwrap();
        assert!(sql.contains("WHERE true\n"), "{sql}");
        assert!(
            sql.ends_with(
                "WHERE m.\"event_time\" >= 100 \
                 AND (SELECT count(*) FROM __tierdb_cold) IS NOT NULL"
            ),
            "{sql}"
        );
    }

    #[test]
    fn update_without_set_items_is_rejected() {
        assert!(render_update(&meta(), T, &pin(), None, &DmlFragments::default()).is_err());
    }

    #[test]
    fn set_tier_key_decomposes_into_move_ctes() {
        let frag = DmlFragments {
            where_sql: Some("id = 1".into()),
            set_items: vec![("event_time".into(), "event_time + 200".into())],
            returning: vec![],
        };
        let sql = render_update(&meta(), T, &pin(), None, &frag).unwrap();
        assert!(
            sql.contains("m.\"event_time\" AS __tierdb_old_tier"),
            "old tier projected alongside the new image:\n{sql}"
        );
        assert!(
            sql.contains("CASE WHEN m.\"event_time\" >= 100 THEN 1 ELSE 0 END"),
            "op decided per row:\n{sql}"
        );
        assert!(
            sql.contains(
                "CASE WHEN m.\"event_time\" >= 100 THEN m.__tierdb_old_tier \
                 ELSE m.\"event_time\" END"
            ),
            "tombstone lands at the old tier, upsert at the new:\n{sql}"
        );
        assert!(
            sql.contains(
                "CASE WHEN m.\"event_time\" < 100 \
                 THEN NULLIF(m.__tierdb_old_tier, m.\"event_time\") END"
            ),
            "cold moves remember where the lake holds the image:\n{sql}"
        );
        assert!(
            sql.contains("to_jsonb(m) - '__tierdb_old_tier'"),
            "payload drops the projection helper:\n{sql}"
        );
        assert!(
            sql.contains(
                "__tierdb_move AS (\n\
                 INSERT INTO \"public\".\"events\" (\"id\", \"event_time\", \"val\")\n\
                 SELECT \"id\", \"event_time\", \"val\" FROM __tierdb_new m \
                 WHERE m.\"event_time\" >= 100"
            ),
            "rows that moved hot insert into the heap:\n{sql}"
        );
    }

    #[test]
    fn delta_conflict_preserves_the_lake_partition() {
        let frag = DmlFragments {
            where_sql: Some("id = 1".into()),
            set_items: vec![("val".into(), "'y'".into())],
            returning: vec![],
        };
        let sql = render_update(&meta(), T, &pin(), None, &frag).unwrap();
        assert!(
            sql.contains(
                "old_tier_key = NULLIF(COALESCE(d.old_tier_key, d.tier_key), EXCLUDED.tier_key)"
            ),
            "{sql}"
        );
        let sql = render_delete(&meta(), T, &pin(), None, &frag).unwrap();
        assert!(
            sql.contains(
                "old_tier_key = NULLIF(COALESCE(d.old_tier_key, d.tier_key), EXCLUDED.tier_key)"
            ),
            "{sql}"
        );
    }
}
