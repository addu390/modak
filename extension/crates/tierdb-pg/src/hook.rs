//! Transparent reads. A `planner_hook` turns each registered `RTE_RELATION`
//! into an `RTE_SUBQUERY` holding the same union scan the explicit protocol
//! runs, following Postgres' own view-expansion recipe.

use core::ffi::{c_char, c_int, c_void};
use std::cell::Cell;
use std::ffi::CString;
use std::ptr;
use std::sync::Mutex;

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;
use tierdb_core::dialect::PgDuckdb;
use tierdb_core::domain::{Cutline, TableId, TierKey};
use tierdb_core::mode::Mode;
use tierdb_core::ports::{CutlineReader, DeltaReader, ReadPinRepository};
use tierdb_core::sqlgen::render_scan;
use tierdb_core::table::Table;
use tierdb_core::TierDBError;

use crate::catalog::PgCatalog;
use crate::pin::PgReadPins;
use crate::planner::table_meta;

static TRANSPARENT_READS: GucSetting<bool> = GucSetting::<bool>::new(true);

static MIRRORED_READS: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"heap"));

static MIRROR_WAIT_MS: GucSetting<i32> = GucSetting::<i32>::new(5_000);

static HYBRID_LAG: GucSetting<i32> = GucSetting::<i32>::new(0);

static EXPLAIN: GucSetting<bool> = GucSetting::<bool>::new(false);

static mut PREV_PLANNER_HOOK: pg_sys::planner_hook_type = None;

thread_local! {
    static IN_HOOK: Cell<bool> = const { Cell::new(false) };
}

static SESSION_PINS: Mutex<Vec<i64>> = Mutex::new(Vec::new());

pub(crate) unsafe fn init() {
    GucRegistry::define_bool_guc(
        c"tierdb.transparent_reads",
        c"Rewrite SELECTs on tierdb-registered tables to span hot and cold tiers.",
        c"When on, the planner substitutes the two-tier union scan for every \
          registered table in a SELECT, pinning (T, S) for the transaction.",
        &TRANSPARENT_READS,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"tierdb.mirrored_reads",
        c"Read mode for MIRRORED tables without retention: 'heap' or 'hybrid'.",
        c"'heap' (default) leaves plain scans untouched because the heap holds \
          everything. 'hybrid' rewrites to the two-tier union after a bounded \
          wait for the mirror frontier, pushing the bulk of the scan to the lake.",
        &MIRRORED_READS,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"tierdb.mirror_wait_ms",
        c"Bounded wait for the mirror frontier before a hybrid read (ms).",
        c"A hybrid read first waits until tierdb.cutline.replicated_lsn passes \
          pg_current_wal_lsn(); past this timeout it falls back to the heap \
          with a NOTICE.",
        &MIRROR_WAIT_MS,
        0,
        600_000,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"tierdb.hybrid_lag",
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
        c"tierdb.explain",
        c"Raise a NOTICE for every tierdb routing decision.",
        c"When on, transparent reads, DML rewrites, and cold-row routing each \
          report what they did and why. tierdb_explain() gives the same report \
          for a statement without running it.",
        &EXPLAIN,
        GucContext::Userset,
        GucFlags::default(),
    );

    PREV_PLANNER_HOOK = pg_sys::planner_hook;
    pg_sys::planner_hook = Some(tierdb_planner);
    pg_sys::RegisterXactCallback(Some(xact_callback), ptr::null_mut());
    pg_sys::RegisterSubXactCallback(Some(subxact_callback), ptr::null_mut());
}

#[pg_guard]
unsafe extern "C-unwind" fn tierdb_planner(
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
    if (*parse).commandType != pg_sys::CmdType::CMD_SELECT
        || !(*parse).utilityStmt.is_null()
        || (*parse).hasModifyingCTE
        || !(*parse).rowMarks.is_null()
    {
        return false;
    }
    pg_sys::get_namespace_oid(c"tierdb".as_ptr(), true) != pg_sys::InvalidOid
}

struct WalkContext {
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
            false
        }
        _ => pg_sys::expression_tree_walker_impl(node, Some(walker), context),
    }
}

/// How a registered relation's scan is sourced.
#[derive(PartialEq)]
pub(crate) enum Rewrite {
    Skip,
    Seam,
    SeamKeepHeap,
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

    let sql = "SELECT t.mode, t.heap_retention_lag, t.keep_heap, \
                      EXISTS (SELECT 1 FROM pg_catalog.pg_attribute \
                              WHERE attrelid = $2 AND attisdropped) \
               FROM tierdb.tables t WHERE t.table_id = $1";
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
        let heap_retention_lag = row.get::<i64>(2).ok()?;
        let keep_heap = row.get::<bool>(3).ok()??;
        let skewed = row.get::<bool>(4).ok()??;
        Some((mode, heap_retention_lag, keep_heap, skewed))
    });

    let mode = match row {
        None => return Rewrite::Skip,
        Some((_, _, _, true)) => return Rewrite::Skip,
        Some((mode, heap_retention_lag, keep_heap, _)) => {
            match Mode::from_catalog(&mode, keep_heap, heap_retention_lag) {
                Ok(mode) => mode,
                Err(_) => return Rewrite::Skip,
            }
        }
    };

    if mode.heap_complete() {
        return if hybrid_requested() {
            Rewrite::Hybrid
        } else {
            Rewrite::Skip
        };
    }
    if !mode.routes_by_cut() {
        return Rewrite::SeamKeepHeap;
    }
    Rewrite::Seam
}

pub(crate) fn hybrid_requested() -> bool {
    MIRRORED_READS
        .get()
        .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"hybrid"))
}

unsafe fn substitute(rte: *mut pg_sys::RangeTblEntry, ctx: &mut WalkContext, kind: Rewrite) {
    let table = TableId(u32::from((*rte).relid));

    let planned = or_error(table_meta(table));
    if let Some(catalog) = &planned.table.catalog {
        or_error(crate::lake::ensure_attached(catalog));
    }
    let (cut, read) = match kind {
        Rewrite::Hybrid => {
            let Some(cut) = hybrid_cutline(table, &planned.table) else {
                return;
            };
            let read = or_error(planned.table.scan_hybrid(cut.t, &planned.lake_props));
            (cut, read)
        }
        _ => {
            let cut = or_error(PgCatalog.current(table));
            let read = or_error(planned.scan(cut.t));
            (cut, read)
        }
    };
    let sql = or_error(render_scan(&planned.table, Some(&read), &PgDuckdb));

    if EXPLAIN.get() {
        let delta = or_error(PgCatalog.overlay(table, tierdb_core::domain::KeyRange::UNBOUNDED));
        let cold_side = if planned.table.catalog.is_some() {
            "live lake catalog".to_string()
        } else {
            format!("iceberg pinned at snapshot {}", cut.snapshot.0)
        };
        notice!(
            "tierdb: {}.{} reads both tiers: heap at {} >= {}, {} merged \
             with {} delta row(s)",
            planned.table.schema,
            planned.table.name,
            planned.table.tier_key_col,
            planned.table.tier_key_type.pg_literal(cut.t.0),
            cold_side,
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
    (*rte).inh = false;

    ctx.ours.push(subquery);
}

unsafe fn hybrid_cutline(table: TableId, meta: &Table) -> Option<Cutline> {
    if !wait_for_frontier(table) {
        notice!(
            "tierdb: mirror frontier for {}.{} did not catch up within \
             tierdb.mirror_wait_ms; reading the heap instead",
            meta.schema,
            meta.name
        );
        return None;
    }
    let stored = or_error(PgCatalog.current(table));
    let sql = format!(
        "SELECT {} FROM {}.{}",
        meta.tier_key_type
            .canonical_expr(&format!("max({})", quote_ident(&meta.tier_key_col))),
        quote_ident(&meta.schema),
        quote_ident(&meta.name),
    );
    let highwater = Spi::get_one::<i64>(&sql).ok().flatten()?;
    Some(Cutline {
        t: TierKey(highwater.saturating_sub(i64::from(HYBRID_LAG.get()))),
        snapshot: stored.snapshot,
    })
}

unsafe fn wait_for_frontier(table: TableId) -> bool {
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
             FROM tierdb.cutline WHERE table_id = $2",
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

fn or_error<T>(r: tierdb_core::Result<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e @ TierDBError::Planning(_)) => error!(
            "tierdb: transparent read failed ({e}); \
             SET tierdb.transparent_reads = off to bypass"
        ),
        Err(e) => error!("tierdb: {e}"),
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
                    "DELETE FROM tierdb.read_pins WHERE pin_id = $1",
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
