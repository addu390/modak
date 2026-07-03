//! Transparent reads. A `planner_hook` turns each registered `RTE_RELATION`
//! into an `RTE_SUBQUERY` holding the same union scan the explicit protocol
//! runs, following Postgres' own view-expansion recipe. Read pins are
//! transaction-scoped and roll back on abort, so a crashed client never leaks one.

use core::ffi::{c_char, c_int, c_void};
use std::cell::Cell;
use std::ffi::CString;
use std::ptr;
use std::sync::Mutex;

use modak_core::domain::{Cutline, TableId, TierKey};
use modak_core::ports::{CutlineReader, DeltaReader, ReadPinRepository};
use modak_core::sqlgen::render_scan;
use modak_core::{planner as core_planner, ModakError};
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;

use crate::catalog::PgCatalog;
use crate::pin::PgReadPins;
use crate::planner::table_meta;

/// `SET modak.transparent_reads = off` restores plain heap semantics.
static TRANSPARENT_READS: GucSetting<bool> = GucSetting::<bool>::new(true);

/// `SET modak.mirrored_reads = 'hybrid'` opts a session into two-tier reads on
/// MIRRORED tables without retention (default `'heap'`: the heap alone is
/// complete and correct, so plain scans are untouched).
static MIRRORED_READS: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"heap"));

/// Bounded wait (ms) for the mirror frontier to pass the session's WAL
/// position before a hybrid read pins. On timeout the query falls back to the heap.
static MIRROR_WAIT_MS: GucSetting<i32> = GucSetting::<i32>::new(5_000);

/// Hybrid seam margin in tier-key units: the union splits at
/// `max(tier_key) - modak.hybrid_lag` (recent slice from the heap, the bulk from
/// the lake).
static HYBRID_LAG: GucSetting<i32> = GucSetting::<i32>::new(0);

/// `SET modak.explain = on` makes every routing decision raise a NOTICE.
static EXPLAIN: GucSetting<bool> = GucSetting::<bool>::new(false);

static mut PREV_PLANNER_HOOK: pg_sys::planner_hook_type = None;

thread_local! {
    // Re-entrancy guard: the hook's own SPI queries must never be rewritten.
    // Reset on (sub)xact abort so a failed rewrite can't wedge the session.
    static IN_HOOK: Cell<bool> = const { Cell::new(false) };
}

/// Pins acquired by transparent reads in the current transaction, released at
/// PRE_COMMIT (abort rolls the pin rows back with the transaction).
static SESSION_PINS: Mutex<Vec<i64>> = Mutex::new(Vec::new());

/// Called from `_PG_init`. Safe to run at postmaster (shared_preload_libraries)
/// or at first library load in a backend.
pub(crate) unsafe fn init() {
    GucRegistry::define_bool_guc(
        c"modak.transparent_reads",
        c"Rewrite SELECTs on modak-registered tables to span hot and cold tiers.",
        c"When on, the planner substitutes the two-tier union scan for every \
          registered table in a SELECT, pinning (T, S) for the transaction.",
        &TRANSPARENT_READS,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"modak.mirrored_reads",
        c"Read mode for MIRRORED tables without retention: 'heap' or 'hybrid'.",
        c"'heap' (default) leaves plain scans untouched because the heap holds \
          everything. 'hybrid' rewrites to the two-tier union after a bounded \
          wait for the mirror frontier, pushing the bulk of the scan to the lake.",
        &MIRRORED_READS,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"modak.mirror_wait_ms",
        c"Bounded wait for the mirror frontier before a hybrid read (ms).",
        c"A hybrid read first waits until modak.cutline.replicated_lsn passes \
          pg_current_wal_lsn(); past this timeout it falls back to the heap \
          with a NOTICE.",
        &MIRROR_WAIT_MS,
        0,
        600_000,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"modak.hybrid_lag",
        c"Hybrid seam margin in tier-key units.",
        c"A hybrid read unions heap rows with tier_key >= max(tier_key) - lag \
          and lake rows below that seam.",
        &HYBRID_LAG,
        0,
        i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"modak.explain",
        c"Raise a NOTICE for every modak routing decision.",
        c"When on, transparent reads, DML rewrites, and cold-row routing each \
          report what they did and why. modak_explain() gives the same report \
          for a statement without running it.",
        &EXPLAIN,
        GucContext::Userset,
        GucFlags::default(),
    );
    PREV_PLANNER_HOOK = pg_sys::planner_hook;
    pg_sys::planner_hook = Some(modak_planner);
    pg_sys::RegisterXactCallback(Some(xact_callback), ptr::null_mut());
    pg_sys::RegisterSubXactCallback(Some(subxact_callback), ptr::null_mut());
}

#[pg_guard]
unsafe extern "C-unwind" fn modak_planner(
    parse: *mut pg_sys::Query,
    query_string: *const c_char,
    cursor_options: c_int,
    bound_params: pg_sys::ParamListInfo,
) -> *mut pg_sys::PlannedStmt {
    if !crate::threads::is_main_thread() {
        return match PREV_PLANNER_HOOK {
            Some(prev) => prev(parse, query_string, cursor_options, bound_params),
            None => crate::threads::raw::standard_planner(
                parse,
                query_string,
                cursor_options,
                bound_params,
            ),
        };
    }
    let parse = crate::dml_rewrite::maybe_rewrite(parse).unwrap_or(parse);
    if should_consider(parse) {
        rewrite_registered_rtes(parse);
    }
    match PREV_PLANNER_HOOK {
        Some(prev) => prev(parse, query_string, cursor_options, bound_params),
        None => pg_sys::standard_planner(parse, query_string, cursor_options, bound_params),
    }
}

pub(crate) fn in_hook() -> bool {
    IN_HOOK.with(Cell::get)
}

pub(crate) fn explain_on() -> bool {
    EXPLAIN.get()
}

pub(crate) fn transparent_reads_on() -> bool {
    TRANSPARENT_READS.get()
}

pub(crate) fn mirror_wait_ms() -> i32 {
    MIRROR_WAIT_MS.get()
}

pub(crate) fn hybrid_lag() -> i32 {
    HYBRID_LAG.get()
}

/// Runs `f` with the re-entrancy guard set and an active snapshot available,
/// the same envelope `rewrite_registered_rtes` uses for its own SPI.
pub(crate) unsafe fn with_hook_guard<T>(f: impl FnOnce() -> T) -> T {
    IN_HOOK.with(|g| g.set(true));
    let pushed_snapshot = if !pg_sys::ActiveSnapshotSet() {
        pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
        true
    } else {
        false
    };
    let out = f();
    if pushed_snapshot {
        pg_sys::PopActiveSnapshot();
    }
    IN_HOOK.with(|g| g.set(false));
    out
}

/// Records a pin acquired during planning for release at PRE_COMMIT.
pub(crate) fn remember_pin(pin: i64) {
    SESSION_PINS.lock().unwrap().push(pin);
}

unsafe fn should_consider(parse: *mut pg_sys::Query) -> bool {
    if !TRANSPARENT_READS.get() || IN_HOOK.with(Cell::get) {
        return false;
    }
    if !pg_sys::IsTransactionState() {
        return false;
    }
    // Reads only: DML and SELECT FOR UPDATE keep plain heap semantics.
    if (*parse).commandType != pg_sys::CmdType::CMD_SELECT
        || !(*parse).utilityStmt.is_null()
        || (*parse).hasModifyingCTE
        || !(*parse).rowMarks.is_null()
    {
        return false;
    }
    // The modak schema existing is the cheapest "extension installed here" probe.
    pg_sys::get_namespace_oid(c"modak".as_ptr(), true) != pg_sys::InvalidOid
}

// A walker (not a top-level rtable loop) reaches sub-selects, CTEs, and sublinks.
struct WalkContext {
    // Substituted subqueries reference the same relation, never rewrite them recursively.
    ours: Vec<*mut pg_sys::Query>,
}

unsafe fn rewrite_registered_rtes(parse: *mut pg_sys::Query) {
    with_hook_guard(|| {
        let mut ctx = WalkContext { ours: Vec::new() };
        pg_sys::query_tree_walker_impl(
            parse,
            Some(walker),
            &mut ctx as *mut WalkContext as *mut c_void,
            pg_sys::QTW_EXAMINE_RTES_BEFORE as c_int,
        );
    })
}

#[pg_guard]
unsafe extern "C-unwind" fn walker(node: *mut pg_sys::Node, context: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }
    let ctx = &mut *(context as *mut WalkContext);
    match (*node).type_ {
        pg_sys::NodeTag::T_Query => {
            let query = node as *mut pg_sys::Query;
            if ctx.ours.contains(&query) {
                return false;
            }
            pg_sys::query_tree_walker_impl(
                query,
                Some(walker),
                context,
                pg_sys::QTW_EXAMINE_RTES_BEFORE as c_int,
            )
        }
        pg_sys::NodeTag::T_RangeTblEntry => {
            let rte = node as *mut pg_sys::RangeTblEntry;
            if (*rte).rtekind == pg_sys::RTEKind::RTE_RELATION {
                match rewrite_kind((*rte).relid) {
                    Rewrite::Skip => {}
                    kind => substitute(rte, ctx, kind),
                }
            }
            // range_table_walker descends into subquery RTEs itself.
            false
        }
        _ => pg_sys::expression_tree_walker_impl(node, Some(walker), context),
    }
}

/// How a registered relation's scan is sourced.
#[derive(PartialEq)]
pub(crate) enum Rewrite {
    /// Plain heap scan (unregistered, or a MIRRORED table whose heap is complete).
    Skip,
    /// Union at the stored cut-line, for TIERED and for MIRRORED with
    /// retention, where the heap below R is gone and the lake provably holds it.
    Seam,
    /// MIRRORED without retention, session opted in: union at a computed seam
    /// after a bounded wait for the mirror frontier.
    Hybrid,
}

pub(crate) unsafe fn rewrite_kind(relid: pg_sys::Oid) -> Rewrite {
    if u32::from(relid) < pg_sys::FirstNormalObjectId {
        return Rewrite::Skip;
    }
    let relkind = pg_sys::get_rel_relkind(relid);
    if relkind != pg_sys::RELKIND_RELATION as c_char
        && relkind != pg_sys::RELKIND_PARTITIONED_TABLE as c_char
    {
        return Rewrite::Skip;
    }
    // Dropped columns would skew the positional subquery-to-RTE output mapping.
    let sql = "SELECT t.mode, t.heap_retention_lag IS NOT NULL, \
                      EXISTS (SELECT 1 FROM pg_catalog.pg_attribute \
                              WHERE attrelid = $2 AND attisdropped) \
               FROM modak.tables t WHERE t.table_id = $1";
    let row = Spi::connect(|client| {
        let mut rows = client
            .select(
                sql,
                Some(1),
                &[(u32::from(relid) as i64).into(), relid.into()],
            )
            .ok()?;
        let row = rows.next()?;
        let mode = row.get::<String>(1).ok()??;
        let has_retention = row.get::<bool>(2).ok()??;
        let skewed = row.get::<bool>(3).ok()??;
        Some((mode, has_retention, skewed))
    });
    match row {
        None => Rewrite::Skip,
        Some((_, _, true)) => Rewrite::Skip,
        Some((mode, has_retention, _)) if mode == "mirrored" => {
            if has_retention {
                Rewrite::Seam
            } else if hybrid_requested() {
                Rewrite::Hybrid
            } else {
                Rewrite::Skip
            }
        }
        Some(_) => Rewrite::Seam,
    }
}

pub(crate) fn hybrid_requested() -> bool {
    MIRRORED_READS
        .get()
        .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"hybrid"))
}

/// The view-expansion recipe (rewriteHandler.c, `ApplyRetrieveRule`): swap the
/// relation RTE for a subquery RTE but keep `relid`, `rellockmode`, and
/// `perminfoindex`, so the original table is still locked and ACL-checked.
unsafe fn substitute(rte: *mut pg_sys::RangeTblEntry, ctx: &mut WalkContext, kind: Rewrite) {
    let table = TableId(u32::from((*rte).relid));

    let meta = or_error(table_meta(table));
    let cut = match kind {
        Rewrite::Hybrid => match hybrid_cutline(table, &meta) {
            Some(cut) => cut,
            None => return, // frontier wait timed out (NOTICE raised): heap read
        },
        _ => or_error(PgCatalog.current(table)),
    };
    let delta = or_error(PgCatalog.overlay(table, modak_core::domain::KeyRange::UNBOUNDED));
    let plan = core_planner::rewrite(&core_planner::UserQuery::default(), &cut, &delta);
    let sql = or_error(render_scan(&plan, &meta));

    if EXPLAIN.get() {
        notice!(
            "modak: {}.{} reads both tiers: heap at {} >= {}, iceberg pinned \
             at snapshot {} merged with {} delta row(s)",
            meta.hot_schema,
            meta.hot_table,
            meta.tier_key_col,
            cut.t.0,
            cut.snapshot.0,
            delta.entries.len(),
        );
    }

    let pin = or_error(PgReadPins::default().acquire(table, &cut));
    SESSION_PINS.lock().unwrap().push(pin.0);

    let subquery = analyze_generated_sql(&sql);
    (*rte).rtekind = pg_sys::RTEKind::RTE_SUBQUERY;
    (*rte).subquery = subquery;
    (*rte).security_barrier = false;
    (*rte).tablesample = ptr::null_mut();
    (*rte).inh = false; // must not be set for a subquery RTE

    ctx.ours.push(subquery);
}

/// The hybrid seam for a MIRRORED no-retention table. Waits (bounded) for the
/// mirror frontier to pass the session's WAL position, after which everything
/// this query's snapshot sees is provably in the lake, then splits the union
/// at `max(tier_key) - modak.hybrid_lag`. None falls back to the heap.
unsafe fn hybrid_cutline(table: TableId, meta: &modak_core::sqlgen::TableMeta) -> Option<Cutline> {
    if !wait_for_frontier(table) {
        notice!(
            "modak: mirror frontier for {}.{} did not catch up within \
             modak.mirror_wait_ms; reading the heap instead",
            meta.hot_schema,
            meta.hot_table
        );
        return None;
    }
    let stored = or_error(PgCatalog.current(table));
    let sql = format!(
        "SELECT max({}) FROM {}.{}",
        quote_ident(&meta.tier_key_col),
        quote_ident(&meta.hot_schema),
        quote_ident(&meta.hot_table),
    );
    let highwater = Spi::get_one::<i64>(&sql).ok().flatten()?;
    Some(Cutline {
        t: TierKey(highwater.saturating_sub(i64::from(HYBRID_LAG.get()))),
        snapshot: stored.snapshot,
    })
}

/// True once `modak.cutline.replicated_lsn` passes the WAL position observed
/// at call time. Bounded by `modak.mirror_wait_ms`.
unsafe fn wait_for_frontier(table: TableId) -> bool {
    // Insert position, since under synchronous_commit=off the write pointer may lag.
    let target =
        match Spi::get_one::<i64>("SELECT (pg_current_wal_insert_lsn() - '0/0'::pg_lsn)::bigint") {
            Ok(Some(t)) => t,
            _ => return false,
        };
    let deadline =
        std::time::Instant::now() + std::time::Duration::from_millis(MIRROR_WAIT_MS.get() as u64);
    loop {
        let caught_up = Spi::get_one_with_args::<bool>(
            "SELECT replicated_lsn IS NOT NULL AND replicated_lsn >= $1 \
             FROM modak.cutline WHERE table_id = $2",
            &[target.into(), (table.0 as i64).into()],
        )
        .ok()
        .flatten()
        .unwrap_or(false);
        if caught_up {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        pgrx::check_for_interrupts!();
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

unsafe fn analyze_generated_sql(sql: &str) -> *mut pg_sys::Query {
    analyze_generated_sql_with_params(sql, &[])
}

/// `param_types[i]` is the type of `$i+1` in the generated SQL. Entries for
/// parameter numbers the text does not reference may be `InvalidOid`.
pub(crate) unsafe fn analyze_generated_sql_with_params(
    sql: &str,
    param_types: &[pg_sys::Oid],
) -> *mut pg_sys::Query {
    let c_sql = CString::new(sql).expect("generated SQL contains no NUL bytes");
    let raw_list = pg_sys::pg_parse_query(c_sql.as_ptr());
    let raw = pg_sys::list_nth(raw_list, 0) as *mut pg_sys::RawStmt;
    let (types, n) = if param_types.is_empty() {
        (ptr::null(), 0)
    } else {
        (param_types.as_ptr(), param_types.len() as c_int)
    };
    pg_sys::parse_analyze_fixedparams(raw, c_sql.as_ptr(), types, n, ptr::null_mut())
}

fn or_error<T>(r: modak_core::Result<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e @ ModakError::Planning(_)) => error!(
            "modak: transparent read failed ({e}); \
             SET modak.transparent_reads = off to bypass"
        ),
        Err(e) => error!("modak: {e}"),
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn xact_callback(event: pg_sys::XactEvent::Type, _arg: *mut c_void) {
    match event {
        // Still in-transaction, so SPI works with a pushed snapshot and the
        // DELETE commits with it.
        pg_sys::XactEvent::XACT_EVENT_PRE_COMMIT => {
            let pins: Vec<i64> = std::mem::take(&mut *SESSION_PINS.lock().unwrap());
            if pins.is_empty() {
                return;
            }
            pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
            for pin in pins {
                let _ = Spi::run_with_args(
                    "DELETE FROM modak.read_pins WHERE pin_id = $1",
                    &[pin.into()],
                );
            }
            pg_sys::PopActiveSnapshot();
        }
        // Abort rolls the pin rows back. Drop bookkeeping and unwedge the guard.
        pg_sys::XactEvent::XACT_EVENT_ABORT
        | pg_sys::XactEvent::XACT_EVENT_PARALLEL_ABORT
        | pg_sys::XactEvent::XACT_EVENT_COMMIT
        | pg_sys::XactEvent::XACT_EVENT_PARALLEL_COMMIT => {
            SESSION_PINS.lock().unwrap().clear();
            IN_HOOK.with(|f| f.set(false));
        }
        _ => {}
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn subxact_callback(
    event: pg_sys::SubXactEvent::Type,
    _my_subid: pg_sys::SubTransactionId,
    _parent_subid: pg_sys::SubTransactionId,
    _arg: *mut c_void,
) {
    if event == pg_sys::SubXactEvent::SUBXACT_EVENT_ABORT_SUB {
        IN_HOOK.with(|f| f.set(false));
    }
}

// pg_tests live in lib.rs: pgrx only discovers tests in a `#[pg_schema] mod tests`.
