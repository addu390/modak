//! pg_duckdb runs parts of a plan on DuckDB worker threads, and those
//! threads re-enter the executor hooks.

pub(crate) fn is_main_thread() -> bool {
    #[cfg(target_os = "macos")]
    return unsafe { libc::pthread_main_np() == 1 };
    #[cfg(target_os = "linux")]
    return unsafe { libc::syscall(libc::SYS_gettid) == libc::c_long::from(libc::getpid()) };
    #[allow(unreachable_code)]
    true
}

#[allow(non_snake_case)]
pub(crate) mod raw {
    use core::ffi::c_int;
    use pgrx::pg_sys;

    extern "C-unwind" {
        pub(crate) fn standard_ExecutorStart(query_desc: *mut pg_sys::QueryDesc, eflags: c_int);
        pub(crate) fn standard_ExecutorRun(
            query_desc: *mut pg_sys::QueryDesc,
            direction: pg_sys::ScanDirection::Type,
            count: u64,
            execute_once: bool,
        );
        pub(crate) fn standard_ExecutorEnd(query_desc: *mut pg_sys::QueryDesc);
        pub(crate) fn standard_planner(
            parse: *mut pg_sys::Query,
            query_string: *const core::ffi::c_char,
            cursor_options: c_int,
            bound_params: pg_sys::ParamListInfo,
        ) -> *mut pg_sys::PlannedStmt;
    }
}
