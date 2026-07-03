//! `modak_explain`: the routing report. Analyzes a statement, runs the same
//! classification the hooks run against live catalog state, and reports where
//! rows will come from or go to, without executing anything.

use core::ffi::{c_int, c_void, CStr};
use std::ffi::CString;
use std::ptr;

use modak_core::dml::{classify, Classification};
use modak_core::domain::TableId;
use modak_core::ports::CutlineReader;
use pgrx::prelude::*;

use crate::catalog::PgCatalog;
use crate::delta::{write_meta, WriteMeta};
use crate::dml_rewrite::{const_i64, extract_predicate};
use crate::hook;

/// One line per row, like EXPLAIN. The report reflects the cut-line at call
/// time; tiering moves it, so the same statement can route differently later.
#[pg_extern]
fn modak_explain(sql: &str) -> SetOfIterator<'static, String> {
    let lines = unsafe { hook::with_hook_guard(|| explain(sql)) };
    SetOfIterator::new(lines)
}

unsafe fn explain(sql: &str) -> Vec<String> {
    let c_sql =
        CString::new(sql).unwrap_or_else(|_| error!("modak: statement contains a NUL byte"));
    let raw_list = pg_sys::pg_parse_query(c_sql.as_ptr());
    if pg_sys::list_length(raw_list) != 1 {
        error!("modak: modak_explain takes exactly one statement");
    }
    let raw = pg_sys::list_nth(raw_list, 0) as *mut pg_sys::RawStmt;
    let stmt = (*raw).stmt;
    match (*stmt).type_ {
        pg_sys::NodeTag::T_CopyStmt => return explain_copy(stmt as *mut pg_sys::CopyStmt),
        pg_sys::NodeTag::T_SelectStmt
        | pg_sys::NodeTag::T_InsertStmt
        | pg_sys::NodeTag::T_UpdateStmt
        | pg_sys::NodeTag::T_DeleteStmt => {}
        _ => return vec!["utility statement: modak does not route it".into()],
    }
    let query =
        pg_sys::parse_analyze_fixedparams(raw, c_sql.as_ptr(), ptr::null(), 0, ptr::null_mut());
    match (*query).commandType {
        pg_sys::CmdType::CMD_SELECT => explain_select(query),
        pg_sys::CmdType::CMD_INSERT => explain_insert(query),
        pg_sys::CmdType::CMD_UPDATE => explain_dml(query, "UPDATE"),
        pg_sys::CmdType::CMD_DELETE => explain_dml(query, "DELETE"),
        _ => vec!["statement: modak does not route it".into()],
    }
}

/// How modak treats a relation, straight from the catalog.
enum Disposition {
    Unregistered,
    /// Mirrored without retention: the heap is complete.
    HeapComplete,
    /// Tiered, or mirrored with heap retention: reads and writes split at the seam.
    Seam {
        mode: String,
    },
}

fn disposition(relid: pg_sys::Oid) -> Disposition {
    let row = Spi::connect(|client| {
        let mut rows = client
            .select(
                "SELECT mode, heap_retention_lag IS NOT NULL \
                 FROM modak.tables WHERE table_id = $1",
                Some(1),
                &[(u32::from(relid) as i64).into()],
            )
            .ok()?;
        let row = rows.next()?;
        let mode = row.get::<String>(1).ok()??;
        let has_retention = row.get::<bool>(2).ok()??;
        Some((mode, has_retention))
    });
    match row {
        None => Disposition::Unregistered,
        Some((mode, false)) if mode == "mirrored" => Disposition::HeapComplete,
        Some((mode, true)) if mode == "mirrored" => Disposition::Seam {
            mode: "mirrored + heap retention".into(),
        },
        Some((mode, _)) => Disposition::Seam { mode },
    }
}

fn qualified(meta: &WriteMeta) -> String {
    format!("{}.{}", meta.schema, meta.table)
}

fn meta_of(relid: pg_sys::Oid) -> WriteMeta {
    match write_meta(TableId(u32::from(relid))) {
        Ok(m) => m,
        Err(e) => error!("modak: {e}"),
    }
}

fn cutline_of(relid: pg_sys::Oid) -> modak_core::domain::Cutline {
    match PgCatalog.current(TableId(u32::from(relid))) {
        Ok(c) => c,
        Err(e) => error!("modak: {e}"),
    }
}

fn retention_of(relid: pg_sys::Oid) -> Option<i64> {
    match PgCatalog.retention_line(TableId(u32::from(relid))) {
        Ok(r) => r.map(|line| line.0),
        Err(e) => error!("modak: {e}"),
    }
}

fn delta_backlog(relid: pg_sys::Oid) -> i64 {
    Spi::get_one_with_args::<i64>(
        "SELECT count(*) FROM modak.delta WHERE table_id = $1",
        &[(u32::from(relid) as i64).into()],
    )
    .ok()
    .flatten()
    .unwrap_or(0)
}

// SELECT: every registered relation in the tree gets a section describing
// which tiers serve it.

struct Collect {
    relids: Vec<pg_sys::Oid>,
}

unsafe fn explain_select(query: *mut pg_sys::Query) -> Vec<String> {
    let mut ctx = Collect { relids: Vec::new() };
    collect_walker(
        query as *mut pg_sys::Node,
        &mut ctx as *mut Collect as *mut c_void,
    );

    let mut lines = Vec::new();
    let mut sections = 0;
    for relid in ctx.relids {
        match disposition(relid) {
            Disposition::Unregistered => {}
            Disposition::HeapComplete => {
                sections += 1;
                let meta = meta_of(relid);
                if hook::hybrid_requested() {
                    lines.push(format!(
                        "{}: mirrored, hybrid read requested",
                        qualified(&meta)
                    ));
                    lines.push(format!(
                        "  waits up to {} ms for the mirror frontier, then serves the \
                         bulk from the lake; on timeout falls back to the heap",
                        hook::mirror_wait_ms(),
                    ));
                    lines.push(format!(
                        "  seam: max({}) - {} (modak.hybrid_lag)",
                        meta.tier_key_col,
                        hook::hybrid_lag(),
                    ));
                } else {
                    lines.push(format!(
                        "{}: mirrored, heap is complete, plain heap scan \
                         (SET modak.mirrored_reads = 'hybrid' to read from the lake)",
                        qualified(&meta)
                    ));
                }
            }
            Disposition::Seam { mode } => {
                sections += 1;
                let meta = meta_of(relid);
                let cut = cutline_of(relid);
                lines.push(format!("{} ({mode}): two-tier read", qualified(&meta)));
                lines.push(format!(
                    "  hot: heap partitions at {} >= {}",
                    meta.tier_key_col, cut.t.0
                ));
                lines.push(format!(
                    "  cold: iceberg pinned at snapshot {}, {} delta row(s) merged, newest wins",
                    cut.snapshot.0,
                    delta_backlog(relid),
                ));
                lines.push(format!(
                    "  pin: (T={}, S={}) held for the transaction",
                    cut.t.0, cut.snapshot.0
                ));
            }
        }
    }
    if sections == 0 {
        return vec!["SELECT: no registered tables, planned by Postgres untouched".into()];
    }
    if !hook::transparent_reads_on() {
        lines.push(
            "note: modak.transparent_reads is off in this session, plain scans \
             see the heap only"
                .into(),
        );
    }
    lines
}

#[pg_guard]
unsafe extern "C-unwind" fn collect_walker(node: *mut pg_sys::Node, context: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }
    let ctx = &mut *(context as *mut Collect);
    match (*node).type_ {
        pg_sys::NodeTag::T_Query => pg_sys::query_tree_walker_impl(
            node as *mut pg_sys::Query,
            Some(collect_walker),
            context,
            pg_sys::QTW_EXAMINE_RTES_BEFORE as c_int,
        ),
        pg_sys::NodeTag::T_RangeTblEntry => {
            let rte = node as *mut pg_sys::RangeTblEntry;
            if (*rte).rtekind == pg_sys::RTEKind::RTE_RELATION
                && !ctx.relids.contains(&(*rte).relid)
            {
                ctx.relids.push((*rte).relid);
            }
            false
        }
        _ => pg_sys::expression_tree_walker_impl(node, Some(collect_walker), context),
    }
}

// UPDATE and DELETE: the classifier's verdict plus what the rewrite would do,
// including the shapes it would reject, reported instead of raised.

unsafe fn explain_dml(query: *mut pg_sys::Query, verb: &str) -> Vec<String> {
    let rte = pg_sys::list_nth((*query).rtable, (*query).resultRelation - 1)
        as *mut pg_sys::RangeTblEntry;
    let relid = (*rte).relid;
    match disposition(relid) {
        Disposition::Unregistered => {
            vec![format!("{verb}: target is not registered, plain heap DML")]
        }
        Disposition::HeapComplete => {
            let meta = meta_of(relid);
            vec![format!(
                "{verb} on {}: mirrored, heap is complete, plain heap DML \
                 (CDC trails it into the lake)",
                qualified(&meta)
            )]
        }
        Disposition::Seam { mode } => {
            let meta = meta_of(relid);
            let cut = cutline_of(relid);
            let tier_attno = pg_sys::get_attnum(relid, cstr(&meta.tier_key_col).as_ptr());
            let quals = (*(*query).jointree).quals;
            let predicate = extract_predicate(quals, (*query).resultRelation, tier_attno);
            let verdict = classify(&predicate, cut.t);

            let mut lines = vec![format!(
                "{verb} on {} ({mode}): cut-line T={}",
                qualified(&meta),
                cut.t.0
            )];
            if verdict == Classification::Hot {
                lines.push(format!(
                    "  verdict: provably hot ({} >= {}), statement passes through untouched",
                    meta.tier_key_col, cut.t.0
                ));
                return lines;
            }
            lines.push(match verdict {
                Classification::Cold => {
                    "  verdict: provably cold, only the delta half can match rows".into()
                }
                _ => "  verdict: may touch both tiers, rewritten into two halves".into(),
            });

            for rejection in dml_rejections(query, &meta, verb) {
                lines.push(format!("  rejected: {rejection}"));
            }

            lines.push(format!(
                "  hot half: the original {verb} bounded to {} >= {} on the heap",
                meta.tier_key_col, cut.t.0
            ));
            lines.push(
                "  cold half: matching lake rows become modak.delta entries, visible \
                 immediately, folded into iceberg by the worker"
                    .into(),
            );
            if verb == "UPDATE" && sets_column(query, &meta.tier_key_col) {
                lines.push(format!(
                    "  tier move: SET touches {}, rows are relocated to where the \
                     new value says they belong",
                    meta.tier_key_col
                ));
            }
            if !(*query).returningList.is_null() {
                lines.push(
                    "  returning: hot rows from the heap statement, cold rows injected \
                     from the delta write"
                        .into(),
                );
            }
            if let Some(r) = retention_of(relid) {
                lines.push(format!(
                    "  retention: rows with {} < {r} are expired and rejected",
                    meta.tier_key_col
                ));
            }
            lines
        }
    }
}

/// The statement shapes the rewrite raises on, reported as lines instead.
unsafe fn dml_rejections(query: *mut pg_sys::Query, meta: &WriteMeta, verb: &str) -> Vec<String> {
    let mut out = Vec::new();
    if !(*query).cteList.is_null() {
        out.push(format!("{verb} with WITH may touch cold rows"));
    }
    if pg_sys::list_length((*query).rtable) > 1 {
        out.push(format!("{verb} with FROM/USING may touch cold rows"));
    }
    let tlist = (*query).targetList;
    for i in 0..pg_sys::list_length(tlist) {
        let tle = pg_sys::list_nth(tlist, i) as *mut pg_sys::TargetEntry;
        if (*tle).resjunk || (*tle).resname.is_null() {
            continue;
        }
        let col = CStr::from_ptr((*tle).resname)
            .to_string_lossy()
            .into_owned();
        if verb == "UPDATE" && meta.pk_cols.contains(&col) {
            out.push(format!("SET on primary key column '{col}'"));
        }
    }
    out
}

unsafe fn sets_column(query: *mut pg_sys::Query, col: &str) -> bool {
    let tlist = (*query).targetList;
    for i in 0..pg_sys::list_length(tlist) {
        let tle = pg_sys::list_nth(tlist, i) as *mut pg_sys::TargetEntry;
        if (*tle).resjunk || (*tle).resname.is_null() {
            continue;
        }
        if CStr::from_ptr((*tle).resname).to_string_lossy() == col {
            return true;
        }
    }
    false
}

// INSERT: routing is per row via the spill trigger, so the report gives the
// rule, and exact counts when the tier keys are literal.

unsafe fn explain_insert(query: *mut pg_sys::Query) -> Vec<String> {
    let rte = pg_sys::list_nth((*query).rtable, (*query).resultRelation - 1)
        as *mut pg_sys::RangeTblEntry;
    let relid = (*rte).relid;
    match disposition(relid) {
        Disposition::Unregistered => {
            vec!["INSERT: target is not registered, plain heap insert".into()]
        }
        Disposition::HeapComplete => {
            let meta = meta_of(relid);
            vec![format!(
                "INSERT into {}: mirrored, the heap takes every row \
                 (CDC trails it into the lake)",
                qualified(&meta)
            )]
        }
        Disposition::Seam { mode } => {
            let meta = meta_of(relid);
            let cut = cutline_of(relid);
            let retention = retention_of(relid);
            let mut lines = vec![format!(
                "INSERT into {} ({mode}): routed per row by {} against the cut-line T={}",
                qualified(&meta),
                meta.tier_key_col,
                cut.t.0
            )];
            match literal_tier_keys(query, relid, &meta.tier_key_col) {
                Some(keys) => {
                    let hot = keys.iter().filter(|k| **k >= cut.t.0).count();
                    let expired = retention
                        .map(|r| keys.iter().filter(|k| **k < r).count())
                        .unwrap_or(0);
                    let cold = keys.len() - hot - expired;
                    if hot > 0 {
                        lines.push(format!("  {hot} row(s) >= {}: heap partitions", cut.t.0));
                    }
                    if cold > 0 {
                        lines.push(format!(
                            "  {cold} row(s) < {}: modak.delta via the spill partition, \
                             visible immediately, folded by the worker",
                            cut.t.0
                        ));
                    }
                    if expired > 0 {
                        lines.push(format!(
                            "  {expired} row(s) below the retention line {}: rejected, \
                             the statement fails",
                            retention.unwrap_or_default()
                        ));
                    }
                }
                None => {
                    lines.push(format!(
                        "  rows with {} >= {}: heap partitions",
                        meta.tier_key_col, cut.t.0
                    ));
                    lines.push(format!(
                        "  rows with {} < {}: modak.delta via the spill partition, \
                         visible immediately, folded by the worker",
                        meta.tier_key_col, cut.t.0
                    ));
                    if let Some(r) = retention {
                        lines.push(format!(
                            "  rows with {} < {r}: rejected, expired from the lake",
                            meta.tier_key_col
                        ));
                    }
                }
            }
            if !spill_enabled(relid, &meta) {
                lines.push(
                    "  note: no spill partition, cold rows fail partition routing \
                     (run modak_enable_transparent_writes)"
                        .into(),
                );
            }
            if !(*query).returningList.is_null() {
                lines.push("  note: RETURNING fails if any row routes cold".into());
            }
            if !(*query).onConflict.is_null() {
                lines.push("  note: ON CONFLICT fails if any row routes cold".into());
            }
            lines
        }
    }
}

/// The tier keys of an INSERT's rows when every one is a literal: a Const in
/// the target list (single row) or a column of an all-Const VALUES list.
unsafe fn literal_tier_keys(
    query: *mut pg_sys::Query,
    relid: pg_sys::Oid,
    tier_key_col: &str,
) -> Option<Vec<i64>> {
    let tier_attno = pg_sys::get_attnum(relid, cstr(tier_key_col).as_ptr());
    if tier_attno == pg_sys::InvalidAttrNumber as pg_sys::AttrNumber {
        return None;
    }
    let tlist = (*query).targetList;
    let mut tle_expr: Option<*mut pg_sys::Node> = None;
    for i in 0..pg_sys::list_length(tlist) {
        let tle = pg_sys::list_nth(tlist, i) as *mut pg_sys::TargetEntry;
        if (*tle).resno == tier_attno && !(*tle).resjunk {
            tle_expr = Some((*tle).expr as *mut pg_sys::Node);
        }
    }
    let expr = tle_expr?;
    if let Some(v) = const_i64(expr) {
        return Some(vec![v]);
    }
    // Multi-row VALUES: the target entry is a Var into the VALUES RTE, and
    // the tier keys sit at that Var's position in each values list.
    if (*expr).type_ != pg_sys::NodeTag::T_Var {
        return None;
    }
    let var = expr as *mut pg_sys::Var;
    let values_rte =
        pg_sys::list_nth((*query).rtable, (*var).varno as c_int - 1) as *mut pg_sys::RangeTblEntry;
    if (*values_rte).rtekind != pg_sys::RTEKind::RTE_VALUES {
        return None;
    }
    let lists = (*values_rte).values_lists;
    let mut out = Vec::with_capacity(pg_sys::list_length(lists) as usize);
    for i in 0..pg_sys::list_length(lists) {
        let row = pg_sys::list_nth(lists, i) as *mut pg_sys::List;
        let cell = pg_sys::list_nth(row, (*var).varattno as c_int - 1) as *mut pg_sys::Node;
        out.push(const_i64(cell)?);
    }
    Some(out)
}

fn spill_enabled(relid: pg_sys::Oid, meta: &WriteMeta) -> bool {
    let spill = format!("{}_modak_spill", meta.table);
    Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_inherits i \
           JOIN pg_class c ON c.oid = i.inhrelid \
          WHERE i.inhparent = $1 AND c.relname = $2)",
        &[relid.into(), spill.into()],
    )
    .ok()
    .flatten()
    .unwrap_or(false)
}

// COPY FROM routes exactly like INSERT, per row through the spill.

unsafe fn explain_copy(stmt: *mut pg_sys::CopyStmt) -> Vec<String> {
    if !(*stmt).is_from || (*stmt).relation.is_null() {
        return vec!["COPY TO: a read, see the SELECT report".into()];
    }
    let relid = pg_sys::RangeVarGetRelidExtended(
        (*stmt).relation,
        pg_sys::AccessShareLock as pg_sys::LOCKMODE,
        0,
        None,
        ptr::null_mut(),
    );
    match disposition(relid) {
        Disposition::Unregistered => {
            vec!["COPY FROM: target is not registered, plain heap copy".into()]
        }
        Disposition::HeapComplete => {
            let meta = meta_of(relid);
            vec![format!(
                "COPY {} FROM: mirrored, the heap takes every row \
                 (CDC trails it into the lake)",
                qualified(&meta)
            )]
        }
        Disposition::Seam { mode } => {
            let meta = meta_of(relid);
            let cut = cutline_of(relid);
            let mut lines = vec![
                format!(
                    "COPY {} FROM ({mode}): routed per row like INSERT",
                    qualified(&meta)
                ),
                format!(
                    "  rows with {} >= {}: heap partitions",
                    meta.tier_key_col, cut.t.0
                ),
                format!(
                    "  rows with {} < {}: modak.delta via the spill partition",
                    meta.tier_key_col, cut.t.0
                ),
            ];
            if let Some(r) = retention_of(relid) {
                lines.push(format!(
                    "  rows with {} < {r}: rejected, expired from the lake",
                    meta.tier_key_col
                ));
            }
            lines
        }
    }
}

fn cstr(s: &str) -> CString {
    CString::new(s).expect("identifier contains no NUL bytes")
}
