//! Transparent INSERT. A DEFAULT partition (the spill) catches rows with no
//! heap partition, and its BEFORE INSERT trigger routes them to `modak.delta`
//! while suppressing the heap write. Hot rows never touch any of this.

use core::ffi::{c_int, c_void};
use std::cell::RefCell;
use std::ffi::CString;
use std::ptr;

use modak_core::domain::TableId;
use modak_core::ports::CutlineReader;
use modak_core::sqlgen::encode_pk;
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;

use crate::catalog::{catalog_err, PgCatalog};
use crate::delta::{
    check_retention, ident, or_error, pk_values, tier_key_of, write_meta, UPSERT_DELTA_SQL,
};

static TRANSPARENT_WRITES: GucSetting<bool> = GucSetting::<bool>::new(true);

/// One entry per live DML executor: the cold-row tally for the command tag,
/// stashed cold RETURNING outputs, and the clause flags the spill route vetoes.
struct WriteFrame {
    query_desc: usize,
    cold_rows: u64,
    stashed: Vec<Vec<Option<String>>>,
    has_returning: bool,
    has_on_conflict: bool,
}

thread_local! {
    static WRITE_FRAMES: RefCell<Vec<WriteFrame>> = const { RefCell::new(Vec::new()) };
}

pub(crate) fn transparent_writes_on() -> bool {
    TRANSPARENT_WRITES.get()
}

static mut PREV_EXECUTOR_START: pg_sys::ExecutorStart_hook_type = None;
static mut PREV_EXECUTOR_RUN: pg_sys::ExecutorRun_hook_type = None;
static mut PREV_EXECUTOR_END: pg_sys::ExecutorEnd_hook_type = None;

pub(crate) unsafe fn init() {
    GucRegistry::define_bool_guc(
        c"modak.transparent_writes",
        c"Route INSERTs of cold rows on modak-registered tables to modak.delta.",
        c"When on, rows below the cut-line land in modak.delta through the \
          spill partition instead of failing partition routing. When off, \
          such rows raise the plain missing-partition error.",
        &TRANSPARENT_WRITES,
        GucContext::Userset,
        GucFlags::default(),
    );
    PREV_EXECUTOR_START = pg_sys::ExecutorStart_hook;
    pg_sys::ExecutorStart_hook = Some(executor_start);
    PREV_EXECUTOR_RUN = pg_sys::ExecutorRun_hook;
    pg_sys::ExecutorRun_hook = Some(executor_run);
    PREV_EXECUTOR_END = pg_sys::ExecutorEnd_hook;
    pg_sys::ExecutorEnd_hook = Some(executor_end);
    pg_sys::RegisterXactCallback(Some(xact_callback), ptr::null_mut());
}

#[pg_guard]
unsafe extern "C-unwind" fn executor_start(query_desc: *mut pg_sys::QueryDesc, eflags: c_int) {
    // pg_duckdb worker threads re-enter here. pgrx forbids pg_sys calls off
    // the main thread, so forward untouched.
    if !crate::threads::is_main_thread() {
        match PREV_EXECUTOR_START {
            Some(prev) => prev(query_desc, eflags),
            None => crate::threads::raw::standard_ExecutorStart(query_desc, eflags),
        }
        return;
    }
    let op = (*query_desc).operation;
    if op == pg_sys::CmdType::CMD_INSERT
        || op == pg_sys::CmdType::CMD_UPDATE
        || op == pg_sys::CmdType::CMD_DELETE
    {
        let planned = (*query_desc).plannedstmt;
        let plan = (*planned).planTree;
        let has_on_conflict = !plan.is_null()
            && (*plan).type_ == pg_sys::NodeTag::T_ModifyTable
            && (*(plan as *mut pg_sys::ModifyTable)).onConflictAction
                != pg_sys::OnConflictAction::ONCONFLICT_NONE;
        WRITE_FRAMES.with_borrow_mut(|frames| {
            frames.push(WriteFrame {
                query_desc: query_desc as usize,
                cold_rows: 0,
                stashed: Vec::new(),
                has_returning: (*planned).hasReturning,
                has_on_conflict,
            })
        });
    }
    match PREV_EXECUTOR_START {
        Some(prev) => prev(query_desc, eflags),
        None => pg_sys::standard_ExecutorStart(query_desc, eflags),
    }
}

/// The command tag and SPI_processed read `es_processed` right after
/// ExecutorRun returns, so cold rows fold in here, not at ExecutorEnd.
/// Stashed RETURNING rows go to the still-open destination here too.
#[pg_guard]
unsafe extern "C-unwind" fn executor_run(
    query_desc: *mut pg_sys::QueryDesc,
    direction: pg_sys::ScanDirection::Type,
    count: u64,
    execute_once: bool,
) {
    if !crate::threads::is_main_thread() {
        match PREV_EXECUTOR_RUN {
            Some(prev) => prev(query_desc, direction, count, execute_once),
            None => crate::threads::raw::standard_ExecutorRun(
                query_desc,
                direction,
                count,
                execute_once,
            ),
        }
        return;
    }
    match PREV_EXECUTOR_RUN {
        Some(prev) => prev(query_desc, direction, count, execute_once),
        None => pg_sys::standard_ExecutorRun(query_desc, direction, count, execute_once),
    }
    let (cold_rows, stashed) = WRITE_FRAMES.with_borrow_mut(|frames| {
        let Some(top) = frames.last_mut() else {
            return (0, None);
        };
        if top.query_desc != query_desc as usize {
            return (0, None);
        }
        let cold = top.cold_rows;
        if top.cold_rows > 0 {
            (*(*query_desc).estate).es_processed += top.cold_rows;
            top.cold_rows = 0;
        }
        let stashed = (!top.stashed.is_empty()).then(|| std::mem::take(&mut top.stashed));
        (cold, stashed)
    });
    if cold_rows > 0 && crate::hook::explain_on() {
        notice!("modak: {cold_rows} cold row(s) written to modak.delta");
    }
    if let Some(rows) = stashed {
        inject_returning(query_desc, rows);
    }
}

/// Sends cold rows' RETURNING outputs to the statement's destination. Values
/// arrive as text and re-enter through each output column's input function.
unsafe fn inject_returning(query_desc: *mut pg_sys::QueryDesc, rows: Vec<Vec<Option<String>>>) {
    let tupdesc = (*query_desc).tupDesc;
    let dest = (*query_desc).dest;
    if tupdesc.is_null() || dest.is_null() {
        error!("modak: cold RETURNING rows with no statement destination");
    }
    let natts = (*tupdesc).natts as usize;
    let slot = pg_sys::MakeSingleTupleTableSlot(tupdesc, &pg_sys::TTSOpsVirtual);
    for row in rows {
        if row.len() != natts {
            error!(
                "modak: cold RETURNING stash has {} values, statement returns {natts}",
                row.len()
            );
        }
        pg_sys::ExecClearTuple(slot);
        for (i, value) in row.iter().enumerate() {
            let att = (*tupdesc).attrs.as_ptr().add(i);
            match value {
                Some(text) => {
                    let mut infunc = pg_sys::InvalidOid;
                    let mut typioparam = pg_sys::InvalidOid;
                    pg_sys::getTypeInputInfo((*att).atttypid, &mut infunc, &mut typioparam);
                    let c = CString::new(text.as_str()).unwrap_or_else(|_| {
                        error!("modak: cold RETURNING value contains a NUL byte")
                    });
                    *(*slot).tts_values.add(i) = pg_sys::OidInputFunctionCall(
                        infunc,
                        c.as_ptr().cast_mut(),
                        typioparam,
                        (*att).atttypmod,
                    );
                    *(*slot).tts_isnull.add(i) = false;
                }
                None => {
                    *(*slot).tts_values.add(i) = pg_sys::Datum::from(0);
                    *(*slot).tts_isnull.add(i) = true;
                }
            }
        }
        pg_sys::ExecStoreVirtualTuple(slot);
        if let Some(receive) = (*dest).receiveSlot {
            receive(slot, dest);
        }
    }
    pg_sys::ExecDropSingleTupleTableSlot(slot);
}

#[pg_guard]
unsafe extern "C-unwind" fn executor_end(query_desc: *mut pg_sys::QueryDesc) {
    if !crate::threads::is_main_thread() {
        match PREV_EXECUTOR_END {
            Some(prev) => prev(query_desc),
            None => crate::threads::raw::standard_ExecutorEnd(query_desc),
        }
        return;
    }
    WRITE_FRAMES.with_borrow_mut(|frames| {
        // Frames above the match belong to errored executors that never
        // reached their End, so they go too.
        if let Some(i) = frames
            .iter()
            .rposition(|f| f.query_desc == query_desc as usize)
        {
            frames.truncate(i);
        }
    });
    match PREV_EXECUTOR_END {
        Some(prev) => prev(query_desc),
        None => pg_sys::standard_ExecutorEnd(query_desc),
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn xact_callback(event: pg_sys::XactEvent::Type, _arg: *mut c_void) {
    match event {
        pg_sys::XactEvent::XACT_EVENT_ABORT
        | pg_sys::XactEvent::XACT_EVENT_PARALLEL_ABORT
        | pg_sys::XactEvent::XACT_EVENT_COMMIT
        | pg_sys::XactEvent::XACT_EVENT_PARALLEL_COMMIT => {
            WRITE_FRAMES.with_borrow_mut(Vec::clear);
        }
        _ => {}
    }
}

/// The spill trigger's target. Below the cut-line the row becomes a delta
/// upsert. At or above it the heap is missing a partition and that stays an error.
#[pg_extern]
fn modak_spill_route(table: pg_sys::Oid, row: pgrx::JsonB) {
    let t = TableId(table.into());
    let meta = or_error(write_meta(t));
    let cut = or_error(PgCatalog.current(t));
    let tier_key = or_error(tier_key_of(&row, &meta.tier_key_col));

    if tier_key >= cut.t.0 {
        error!(
            "modak: no heap partition of {}.{} covers tier_key {tier_key} \
             (cut-line is {}); the premake worker is behind or the range was \
             never created",
            meta.schema, meta.table, cut.t.0
        );
    }
    if !TRANSPARENT_WRITES.get() {
        error!(
            "modak: tier_key {tier_key} is below the cut-line {} and \
             modak.transparent_writes is off; SET it on, or use modak_upsert()",
            cut.t.0
        );
    }
    WRITE_FRAMES.with_borrow(|frames| {
        if let Some(top) = frames.last() {
            if top.has_returning {
                error!(
                    "modak: INSERT ... RETURNING cannot return rows routed to \
                     modak.delta; drop RETURNING or use modak_upsert()"
                );
            }
            if top.has_on_conflict {
                error!(
                    "modak: INSERT ... ON CONFLICT does not apply to rows routed \
                     to modak.delta (delta writes are already newest-wins \
                     upserts); drop the clause or use modak_upsert()"
                );
            }
        }
    });
    check_retention(t, tier_key);

    let pk = encode_pk(&or_error(pk_values(&row, &meta.pk_cols)));
    or_error(
        Spi::run_with_args(
            UPSERT_DELTA_SQL,
            &[(t.0 as i64).into(), pk.into(), tier_key.into(), row.into()],
        )
        .map_err(catalog_err),
    );

    WRITE_FRAMES.with_borrow_mut(|frames| {
        if let Some(top) = frames.last_mut() {
            top.cold_rows += 1;
        }
    });
}

/// The tier-key pass-through in a rewritten UPDATE/DELETE's delta CTE. Guards
/// the retention floor, tallies the row for the command tag, and stashes the
/// row's RETURNING outputs for the executor to inject.
#[pg_extern]
fn modak_cold_dml(
    tier_key: i64,
    retention_line: Option<i64>,
    returning: Option<Vec<Option<String>>>,
) -> i64 {
    if let Some(line) = retention_line {
        if tier_key < line {
            error!(
                "modak: tier_key {tier_key} is below the retention line {line}, \
                 rows this old have been expired from the lake"
            );
        }
    }
    WRITE_FRAMES.with_borrow_mut(|frames| {
        if let Some(top) = frames.last_mut() {
            top.cold_rows += 1;
            if let Some(row) = returning {
                top.stashed.push(row);
            }
        }
    });
    tier_key
}

// The trigger body is plpgsql because to_jsonb(NEW) is the exact row
// encoding every other write path already speaks.
extension_sql!(
    r#"
CREATE SCHEMA IF NOT EXISTS modak;
CREATE FUNCTION modak.spill_insert() RETURNS trigger
LANGUAGE plpgsql AS $body$
BEGIN
    PERFORM modak_spill_route(TG_ARGV[0]::oid, to_jsonb(NEW));
    RETURN NULL;
END
$body$;
"#,
    name = "spill_insert_trigger",
    requires = [modak_spill_route]
);

const SPILL_SUFFIX: &str = "_modak_spill";

fn spill_name(table: &str) -> String {
    format!("{table}{SPILL_SUFFIX}")
}

// This function performs DDL, so its lookups must see the transaction's
// latest state rather than the calling statement's snapshot.
fn fresh_lookup<T: FromDatum + IntoDatum>(
    sql: &str,
    args: &[pgrx::datum::DatumWithOid],
) -> Option<T> {
    Spi::connect_mut(|client| {
        let mut rows = match client.update(sql, Some(1), args) {
            Ok(rows) => rows,
            Err(e) => error!("modak: lookup failed: {e}"),
        };
        let row = rows.next()?;
        match row.get::<T>(1) {
            Ok(v) => v,
            Err(e) => error!("modak: lookup failed: {e}"),
        }
    })
}

/// Attaches the spill, a DEFAULT partition plus its routing trigger.
/// Idempotent. Refuses fully mirrored tables, where a row without a
/// partition is a real error.
#[pg_extern]
fn modak_enable_transparent_writes(table: pg_sys::Oid) -> String {
    let t = TableId(table.into());
    let meta = or_error(write_meta(t));
    let qualified = format!("{}.{}", ident(&meta.schema), ident(&meta.table));

    let heap_complete = fresh_lookup::<bool>(
        "SELECT mode = 'mirrored' AND heap_retention_lag IS NULL \
         FROM modak.tables WHERE table_id = $1",
        &[(t.0 as i64).into()],
    )
    .unwrap_or(false);
    if heap_complete {
        error!(
            "modak: {qualified} is fully mirrored; the heap takes every \
             insert and needs no spill"
        );
    }

    let existing_default = fresh_lookup::<String>(
        "SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid \
         WHERE i.inhparent = $1 AND pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT'",
        &[table.into()],
    );
    let spill = spill_name(&meta.table);
    match existing_default {
        Some(name) if name == spill => return format!("{qualified}: already enabled"),
        Some(name) => error!(
            "modak: {qualified} already has a DEFAULT partition ({name}); \
             transparent writes need the DEFAULT slot for the spill"
        ),
        None => {}
    }

    let spill_qualified = format!("{}.{}", ident(&meta.schema), ident(&spill));
    or_error(
        Spi::run(&format!(
            "CREATE TABLE {spill_qualified} PARTITION OF {qualified} DEFAULT"
        ))
        .map_err(catalog_err),
    );
    or_error(
        Spi::run(&format!(
            "CREATE TRIGGER modak_spill BEFORE INSERT ON {spill_qualified} \
             FOR EACH ROW EXECUTE FUNCTION modak.spill_insert('{}')",
            t.0
        ))
        .map_err(catalog_err),
    );
    format!("{qualified}: transparent writes enabled")
}

/// Drops the spill partition (and with it the trigger). The spill is always
/// empty, so this never destroys data.
#[pg_extern]
fn modak_disable_transparent_writes(table: pg_sys::Oid) -> String {
    let t = TableId(table.into());
    let meta = or_error(write_meta(t));
    let spill_qualified = format!(
        "{}.{}",
        ident(&meta.schema),
        ident(&spill_name(&meta.table))
    );
    or_error(Spi::run(&format!("DROP TABLE IF EXISTS {spill_qualified}")).map_err(catalog_err));
    format!("{spill_qualified}: dropped")
}
