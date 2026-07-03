//! Transparent UPDATE and DELETE. The planner-hook branch that rewrites a
//! statement on a registered table into a hot half plus a delta-writing cold
//! half. Statements provably confined to the hot tier pass through untouched.

use core::ffi::{c_char, c_int, c_void, CStr};
use std::ffi::CString;

use modak_core::dml::{classify, Classification, CmpOp, DmlFragments, TierPredicate};
use modak_core::dml::{render_delete, render_update};
use modak_core::domain::TableId;
use modak_core::ports::{CutlineReader, ReadPinRepository};
use pgrx::prelude::*;

use crate::catalog::PgCatalog;
use crate::dml::transparent_writes_on;
use crate::hook;
use crate::pin::PgReadPins;
use crate::planner::table_meta;

/// Both halves range over this alias, so deparsed fragments bind in each.
const ALIAS: &CStr = c"m";

/// Returns the replacement query, or `None` to leave the statement untouched.
/// Statements the split cannot honor error out loudly instead.
pub(crate) unsafe fn maybe_rewrite(parse: *mut pg_sys::Query) -> Option<*mut pg_sys::Query> {
    let cmd = (*parse).commandType;
    if cmd != pg_sys::CmdType::CMD_UPDATE && cmd != pg_sys::CmdType::CMD_DELETE {
        return None;
    }
    if !(*parse).utilityStmt.is_null() || (*parse).resultRelation == 0 {
        return None;
    }
    if !transparent_writes_on() || hook::in_hook() || !pg_sys::IsTransactionState() {
        return None;
    }
    let rte = rt_fetch(parse, (*parse).resultRelation);
    let relid = (*rte).relid;
    if u32::from(relid) < pg_sys::FirstNormalObjectId {
        return None;
    }
    let relkind = pg_sys::get_rel_relkind(relid);
    if relkind != pg_sys::RELKIND_RELATION as c_char
        && relkind != pg_sys::RELKIND_PARTITIONED_TABLE as c_char
    {
        return None;
    }
    // ONLY changes which heap rows the statement means, leave it alone.
    if !(*rte).inh {
        return None;
    }
    if pg_sys::get_namespace_oid(c"modak".as_ptr(), true) == pg_sys::InvalidOid {
        return None;
    }
    hook::with_hook_guard(|| rewrite(parse, cmd, relid))
}

unsafe fn rewrite(
    parse: *mut pg_sys::Query,
    cmd: pg_sys::CmdType::Type,
    relid: pg_sys::Oid,
) -> Option<*mut pg_sys::Query> {
    if hook::rewrite_kind(relid) != hook::Rewrite::Seam {
        return None;
    }
    let table = TableId(u32::from(relid));
    let cut = or_error(PgCatalog.current(table));

    // Classification needs only the tier column and T, so provably hot
    // statements return before anything touches lake metadata.
    let write_meta = or_error(crate::delta::write_meta(table));
    let tier_attno = pg_sys::get_attnum(relid, cstring(&write_meta.tier_key_col).as_ptr());
    let quals = (*(*parse).jointree).quals;
    let predicate = extract_predicate(quals, (*parse).resultRelation, tier_attno);
    let verb = if cmd == pg_sys::CmdType::CMD_UPDATE {
        "UPDATE"
    } else {
        "DELETE"
    };
    let verdict = classify(&predicate, cut.t);
    if verdict == Classification::Hot {
        if hook::explain_on() {
            notice!(
                "modak: {verb} on {}.{} is provably hot ({} >= {}), untouched",
                write_meta.schema,
                write_meta.table,
                write_meta.tier_key_col,
                cut.t.0,
            );
        }
        return None;
    }
    let meta = or_error(table_meta(table));
    if !(*parse).cteList.is_null() {
        error!(
            "modak: {verb} with WITH on a registered table may touch cold rows; \
             rewrite without the CTE or bound the statement to recent tier keys"
        );
    }
    if pg_sys::list_length((*parse).rtable) > 1 {
        error!(
            "modak: {verb} with FROM/USING may touch cold rows; join in a \
             subquery, bound the statement to recent tier keys, or use the \
             routed functions"
        );
    }

    let dpcontext = pg_sys::deparse_context_for(ALIAS.as_ptr(), relid);
    let mut frag = DmlFragments {
        where_sql: (!quals.is_null()).then(|| deparse(quals, dpcontext)),
        set_items: Vec::new(),
        returning: returning_items(parse, dpcontext),
    };
    if cmd == pg_sys::CmdType::CMD_UPDATE {
        frag.set_items = set_items(parse, &meta, dpcontext, verb);
    }

    let retention = or_error(PgCatalog.retention_line(table)).map(|r| r.0);
    let sql = or_error(match cmd {
        pg_sys::CmdType::CMD_UPDATE => render_update(&meta, cut.t, retention, &frag),
        _ => render_delete(&meta, cut.t, retention, &frag),
    });

    if hook::explain_on() {
        let side = if verdict == Classification::Cold {
            "provably cold, only the delta half can match"
        } else {
            "may touch both tiers"
        };
        notice!(
            "modak: {verb} on {}.{} rewritten ({side}): heap half at {} >= {}, \
             cold half writes modak.delta from the pinned lake scan",
            meta.hot_schema,
            meta.hot_table,
            meta.tier_key_col,
            cut.t.0,
        );
    }

    let pin = or_error(PgReadPins::default().acquire(table, &cut));
    hook::remember_pin(pin.0);

    let params = param_types(parse);
    let generated = hook::analyze_generated_sql_with_params(&sql, &params);
    // Without this pass defaults, rules, and RLS silently stop applying.
    let rewritten = pg_sys::QueryRewrite(generated);
    if pg_sys::list_length(rewritten) != 1 {
        error!(
            "modak: rules on {} conflict with the transparent DML rewrite",
            meta.hot_table
        );
    }
    Some(pg_sys::list_nth(rewritten, 0) as *mut pg_sys::Query)
}

/// The SET list as `(column, deparsed expression)` pairs, rejecting shapes a
/// delta row image cannot represent.
unsafe fn set_items(
    parse: *mut pg_sys::Query,
    meta: &modak_core::sqlgen::TableMeta,
    dpcontext: *mut pg_sys::List,
    verb: &str,
) -> Vec<(String, String)> {
    let mut items = Vec::new();
    let tlist = (*parse).targetList;
    for i in 0..pg_sys::list_length(tlist) {
        let tle = pg_sys::list_nth(tlist, i) as *mut pg_sys::TargetEntry;
        if (*tle).resjunk {
            continue;
        }
        let col = CStr::from_ptr((*tle).resname)
            .to_string_lossy()
            .into_owned();
        if meta.pk_cols.contains(&col) {
            error!(
                "modak: {verb} may touch cold rows and sets primary key column \
                 '{col}'; the delta overlay is keyed by the primary key"
            );
        }
        let expr = (*tle).expr as *mut pg_sys::Node;
        let tag = (*expr).type_;
        if tag == pg_sys::NodeTag::T_SubscriptingRef || tag == pg_sys::NodeTag::T_FieldStore {
            error!(
                "modak: {verb} may touch cold rows and assigns into part of \
                 '{col}'; set the whole column instead"
            );
        }
        items.push((col, deparse(expr, dpcontext)));
    }
    items
}

/// RETURNING as `(output name, deparsed expression)` pairs. The renderer puts
/// the names on the hot half and the expressions in the cold stash, so both
/// halves produce the same output shape.
unsafe fn returning_items(
    parse: *mut pg_sys::Query,
    dpcontext: *mut pg_sys::List,
) -> Vec<(String, String)> {
    let rlist = (*parse).returningList;
    let mut items = Vec::new();
    for i in 0..pg_sys::list_length(rlist) {
        let tle = pg_sys::list_nth(rlist, i) as *mut pg_sys::TargetEntry;
        if (*tle).resjunk {
            continue;
        }
        let name = CStr::from_ptr((*tle).resname)
            .to_string_lossy()
            .into_owned();
        items.push((name, deparse((*tle).expr as *mut pg_sys::Node, dpcontext)));
    }
    items
}

unsafe fn deparse(node: *mut pg_sys::Node, dpcontext: *mut pg_sys::List) -> String {
    let c = pg_sys::deparse_expression(node, dpcontext, true, false);
    CStr::from_ptr(c).to_string_lossy().into_owned()
}

fn cstring(s: &str) -> CString {
    CString::new(s).expect("identifier contains no NUL bytes")
}

unsafe fn rt_fetch(parse: *mut pg_sys::Query, index: c_int) -> *mut pg_sys::RangeTblEntry {
    pg_sys::list_nth((*parse).rtable, index - 1) as *mut pg_sys::RangeTblEntry
}

fn or_error<T>(r: modak_core::Result<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => error!("modak: {e}"),
    }
}

// Predicate extraction. Turns the WHERE clause into the TierPredicate the
// classifier reasons over. Anything unprovable becomes Unknown, which
// classifies as Mixed and takes the safe two-tier path.

pub(crate) unsafe fn extract_predicate(
    node: *mut pg_sys::Node,
    target_varno: c_int,
    tier_attno: pg_sys::AttrNumber,
) -> TierPredicate {
    if node.is_null() || tier_attno == pg_sys::InvalidAttrNumber as pg_sys::AttrNumber {
        return TierPredicate::Unknown;
    }
    match (*node).type_ {
        pg_sys::NodeTag::T_BoolExpr => {
            let bexpr = node as *mut pg_sys::BoolExpr;
            let parts = (0..pg_sys::list_length((*bexpr).args))
                .map(|i| {
                    extract_predicate(
                        pg_sys::list_nth((*bexpr).args, i) as *mut pg_sys::Node,
                        target_varno,
                        tier_attno,
                    )
                })
                .collect();
            match (*bexpr).boolop {
                pg_sys::BoolExprType::AND_EXPR => TierPredicate::And(parts),
                pg_sys::BoolExprType::OR_EXPR => TierPredicate::Or(parts),
                _ => TierPredicate::Unknown,
            }
        }
        pg_sys::NodeTag::T_OpExpr => {
            extract_cmp(node as *mut pg_sys::OpExpr, target_varno, tier_attno)
        }
        pg_sys::NodeTag::T_ScalarArrayOpExpr => extract_in_list(
            node as *mut pg_sys::ScalarArrayOpExpr,
            target_varno,
            tier_attno,
        ),
        _ => TierPredicate::Unknown,
    }
}

unsafe fn extract_cmp(
    op: *mut pg_sys::OpExpr,
    target_varno: c_int,
    tier_attno: pg_sys::AttrNumber,
) -> TierPredicate {
    if pg_sys::list_length((*op).args) != 2 {
        return TierPredicate::Unknown;
    }
    let Some(name) = builtin_op_name((*op).opno) else {
        return TierPredicate::Unknown;
    };
    let left = pg_sys::list_nth((*op).args, 0) as *mut pg_sys::Node;
    let right = pg_sys::list_nth((*op).args, 1) as *mut pg_sys::Node;

    let (value, reversed) = if is_tier_var(left, target_varno, tier_attno) {
        match const_i64(right) {
            Some(v) => (v, false),
            None => return TierPredicate::Unknown,
        }
    } else if is_tier_var(right, target_varno, tier_attno) {
        match const_i64(left) {
            Some(v) => (v, true),
            None => return TierPredicate::Unknown,
        }
    } else {
        return TierPredicate::Unknown;
    };

    let cmp = match (name.as_str(), reversed) {
        ("=", _) => CmpOp::Eq,
        ("<", false) | (">", true) => CmpOp::Lt,
        ("<=", false) | (">=", true) => CmpOp::Le,
        (">", false) | ("<", true) => CmpOp::Gt,
        (">=", false) | ("<=", true) => CmpOp::Ge,
        _ => return TierPredicate::Unknown,
    };
    TierPredicate::Cmp(cmp, value)
}

unsafe fn extract_in_list(
    saop: *mut pg_sys::ScalarArrayOpExpr,
    target_varno: c_int,
    tier_attno: pg_sys::AttrNumber,
) -> TierPredicate {
    if !(*saop).useOr || pg_sys::list_length((*saop).args) != 2 {
        return TierPredicate::Unknown;
    }
    if builtin_op_name((*saop).opno).as_deref() != Some("=") {
        return TierPredicate::Unknown;
    }
    let left = pg_sys::list_nth((*saop).args, 0) as *mut pg_sys::Node;
    let right = pg_sys::list_nth((*saop).args, 1) as *mut pg_sys::Node;
    if !is_tier_var(left, target_varno, tier_attno) {
        return TierPredicate::Unknown;
    }
    let Some(values) = in_list_values(right) else {
        return TierPredicate::Unknown;
    };
    TierPredicate::Or(
        values
            .into_iter()
            .map(|v| TierPredicate::Cmp(CmpOp::Eq, v))
            .collect(),
    )
}

/// At planner-hook time an IN list is still an ArrayExpr of Consts, while a
/// bound array parameter arrives as a Const of an array type.
unsafe fn in_list_values(node: *mut pg_sys::Node) -> Option<Vec<i64>> {
    let node = strip_relabel(node);
    if (*node).type_ == pg_sys::NodeTag::T_ArrayExpr {
        let arr = node as *mut pg_sys::ArrayExpr;
        if (*arr).multidims {
            return None;
        }
        let mut out = Vec::new();
        for i in 0..pg_sys::list_length((*arr).elements) {
            out.push(const_i64(
                pg_sys::list_nth((*arr).elements, i) as *mut pg_sys::Node
            )?);
        }
        return Some(out);
    }
    const_i64_array(node)
}

/// The operator's catalog name, for built-in operators only. User-defined
/// operators can mean anything, so they classify as Unknown.
unsafe fn builtin_op_name(opno: pg_sys::Oid) -> Option<String> {
    if u32::from(opno) >= pg_sys::FirstNormalObjectId {
        return None;
    }
    let name = pg_sys::get_opname(opno);
    if name.is_null() {
        return None;
    }
    Some(CStr::from_ptr(name).to_string_lossy().into_owned())
}

unsafe fn is_tier_var(
    node: *mut pg_sys::Node,
    target_varno: c_int,
    tier_attno: pg_sys::AttrNumber,
) -> bool {
    let node = strip_relabel(node);
    if (*node).type_ != pg_sys::NodeTag::T_Var {
        return false;
    }
    let var = node as *mut pg_sys::Var;
    (*var).varno == target_varno && (*var).varlevelsup == 0 && (*var).varattno == tier_attno
}

unsafe fn strip_relabel(node: *mut pg_sys::Node) -> *mut pg_sys::Node {
    let mut node = node;
    loop {
        match (*node).type_ {
            pg_sys::NodeTag::T_RelabelType => {
                node = (*(node as *mut pg_sys::RelabelType)).arg as *mut pg_sys::Node;
            }
            // Built-in integer widening casts preserve the value, see through them.
            pg_sys::NodeTag::T_FuncExpr => {
                let f = node as *mut pg_sys::FuncExpr;
                let int_result = (*f).funcresulttype == pg_sys::INT8OID
                    || (*f).funcresulttype == pg_sys::INT4OID
                    || (*f).funcresulttype == pg_sys::INT2OID;
                if u32::from((*f).funcid) < pg_sys::FirstNormalObjectId
                    && (*f).funcformat == pg_sys::CoercionForm::COERCE_IMPLICIT_CAST
                    && int_result
                    && pg_sys::list_length((*f).args) == 1
                {
                    node = pg_sys::list_nth((*f).args, 0) as *mut pg_sys::Node;
                } else {
                    return node;
                }
            }
            _ => return node,
        }
    }
}

pub(crate) unsafe fn const_i64(node: *mut pg_sys::Node) -> Option<i64> {
    let node = strip_relabel(node);
    if (*node).type_ != pg_sys::NodeTag::T_Const {
        return None;
    }
    let c = node as *mut pg_sys::Const;
    if (*c).constisnull {
        return None;
    }
    let datum = (*c).constvalue;
    match (*c).consttype {
        t if t == pg_sys::INT8OID => Some(datum.value() as i64),
        t if t == pg_sys::INT4OID => Some(datum.value() as u32 as i32 as i64),
        t if t == pg_sys::INT2OID => Some(datum.value() as u16 as i16 as i64),
        _ => None,
    }
}

unsafe fn const_i64_array(node: *mut pg_sys::Node) -> Option<Vec<i64>> {
    let node = strip_relabel(node);
    if (*node).type_ != pg_sys::NodeTag::T_Const {
        return None;
    }
    let c = node as *mut pg_sys::Const;
    if (*c).constisnull {
        return None;
    }
    let elem = match (*c).consttype {
        t if t == pg_sys::INT8ARRAYOID => pg_sys::INT8OID,
        t if t == pg_sys::INT4ARRAYOID => pg_sys::INT4OID,
        _ => return None,
    };
    let arr = pg_sys::pg_detoast_datum((*c).constvalue.cast_mut_ptr()) as *mut pg_sys::ArrayType;
    let mut elems: *mut pg_sys::Datum = std::ptr::null_mut();
    let mut nulls: *mut bool = std::ptr::null_mut();
    let mut n: c_int = 0;
    let (elmlen, elmbyval, elmalign) = if elem == pg_sys::INT8OID {
        (8, true, b'd' as c_char)
    } else {
        (4, true, b'i' as c_char)
    };
    pg_sys::deconstruct_array(
        arr, elem, elmlen, elmbyval, elmalign, &mut elems, &mut nulls, &mut n,
    );
    let mut out = Vec::with_capacity(n as usize);
    for i in 0..n as usize {
        if *nulls.add(i) {
            return None;
        }
        let d = *elems.add(i);
        out.push(if elem == pg_sys::INT8OID {
            d.value() as i64
        } else {
            d.value() as u32 as i32 as i64
        });
    }
    Some(out)
}

// Deparsed fragments print bound parameters as $n. The generated text is
// re-analyzed with the original statement's parameter types so plpgsql and
// prepared statements keep working.

struct ParamCollect {
    types: Vec<pg_sys::Oid>,
}

unsafe fn param_types(parse: *mut pg_sys::Query) -> Vec<pg_sys::Oid> {
    let mut ctx = ParamCollect { types: Vec::new() };
    pg_sys::query_tree_walker_impl(
        parse,
        Some(param_walker),
        &mut ctx as *mut ParamCollect as *mut c_void,
        0,
    );
    ctx.types
}

#[pg_guard]
unsafe extern "C-unwind" fn param_walker(node: *mut pg_sys::Node, context: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }
    let ctx = &mut *(context as *mut ParamCollect);
    match (*node).type_ {
        pg_sys::NodeTag::T_Query => pg_sys::query_tree_walker_impl(
            node as *mut pg_sys::Query,
            Some(param_walker),
            context,
            0,
        ),
        pg_sys::NodeTag::T_Param => {
            let param = node as *mut pg_sys::Param;
            if (*param).paramkind == pg_sys::ParamKind::PARAM_MULTIEXPR {
                error!(
                    "modak: multi-column SET from a subquery may touch cold rows; \
                     set the columns individually"
                );
            }
            if (*param).paramkind == pg_sys::ParamKind::PARAM_EXTERN {
                let id = (*param).paramid as usize;
                if ctx.types.len() < id {
                    ctx.types.resize(id, pg_sys::InvalidOid);
                }
                ctx.types[id - 1] = (*param).paramtype;
            }
            false
        }
        _ => pg_sys::expression_tree_walker_impl(node, Some(param_walker), context),
    }
}
