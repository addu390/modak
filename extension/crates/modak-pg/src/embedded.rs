//! Embedded worker mode. When `modak.embedded_worker` is on and the library
//! is preloaded, a background worker supervises the Java daemon as a child
//! process: same binary, same catalog protocol, postmaster-managed lifecycle.

use std::ffi::CString;
use std::process::{Child, Command};
use std::time::Duration;

use pgrx::bgworkers::{
    BackgroundWorker, BackgroundWorkerBuilder, BgWorkerStartTime, SignalWakeFlags,
};
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;

static EMBEDDED_WORKER: GucSetting<bool> = GucSetting::<bool>::new(false);

static WORKER_COMMAND: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);

const BUNDLED_JAVA: &str = "/opt/modak/runtime/bin/java";
const BUNDLED_JAR: &str = "/opt/modak/modak-console.jar";

static WORKER_DATABASE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"postgres"));

static WORKER_ENV: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);

pub(crate) unsafe fn init() {
    if !pg_sys::process_shared_preload_libraries_in_progress {
        return;
    }

    GucRegistry::define_bool_guc(
        c"modak.embedded_worker",
        c"Run the Modak worker daemon as a postmaster-managed child process.",
        c"Requires modak in shared_preload_libraries. The extension registers a \
          background worker that launches and supervises the daemon, restarting \
          it if it exits. Off by default: external workers are unaffected.",
        &EMBEDDED_WORKER,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"modak.worker_command",
        c"Command line the embedded supervisor runs to start the worker daemon.",
        c"For example 'java -jar /opt/modak/modak-console.jar run'. Unset, the \
          bundled runtime under /opt/modak is used when present. The child \
          inherits the postmaster environment plus MODAK_PG_URL pointing at \
          this instance and any modak.worker_env entries.",
        &WORKER_COMMAND,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"modak.worker_database",
        c"Database the embedded worker daemon connects to.",
        c"Used to build the default MODAK_PG_URL for the child process.",
        &WORKER_DATABASE,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"modak.worker_env",
        c"Extra environment for the embedded worker daemon, as key=value;key=value.",
        c"Applied after the inherited postmaster environment and the derived \
          MODAK_PG_URL, so entries here win. Typical use: MODAK_PG_USER, \
          MODAK_WAREHOUSE, MODAK_S3_* credentials.",
        &WORKER_ENV,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    if !EMBEDDED_WORKER.get() {
        return;
    }

    BackgroundWorkerBuilder::new("modak worker supervisor")
        .set_library("modak")
        .set_function("modak_worker_supervisor")
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(15)))
        .enable_shmem_access(None)
        .load();
}

#[pg_guard]
#[no_mangle]
extern "C-unwind" fn modak_worker_supervisor(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM);

    let Some(command) = worker_command() else {
        pgrx::log!(
            "modak supervisor: modak.worker_command is not set and no bundled \
             worker found at {BUNDLED_JAR}, nothing to run"
        );
        return;
    };
    let argv: Vec<String> = command.split_whitespace().map(str::to_owned).collect();
    if argv.is_empty() {
        pgrx::log!("modak supervisor: modak.worker_command is empty, nothing to run");
        return;
    }

    let mut child = match spawn(&argv) {
        Ok(child) => child,
        Err(e) => panic!("modak supervisor: failed to start '{command}': {e}"),
    };
    pgrx::log!("modak supervisor: started '{command}' (pid {})", child.id());

    loop {
        if !BackgroundWorker::wait_latch(Some(Duration::from_secs(1))) {
            shutdown(&mut child);
            return;
        }
        match child.try_wait() {
            Ok(Some(status)) => {
                panic!("modak supervisor: worker exited unexpectedly ({status}), restarting");
            }
            Ok(None) => {}
            Err(e) => panic!("modak supervisor: failed to poll the worker: {e}"),
        }
    }
}

fn worker_command() -> Option<String> {
    if let Some(command) = WORKER_COMMAND.get() {
        return Some(command.to_string_lossy().into_owned());
    }
    let bundled = std::path::Path::new(BUNDLED_JAVA).exists()
        && std::path::Path::new(BUNDLED_JAR).exists();
    bundled.then(|| format!("{BUNDLED_JAVA} -jar {BUNDLED_JAR} run"))
}

fn spawn(argv: &[String]) -> std::io::Result<Child> {
    use std::os::unix::process::CommandExt;

    let mut cmd = Command::new(&argv[0]);
    cmd.args(&argv[1..]).process_group(0);

    let database = WORKER_DATABASE
        .get()
        .map(|d| d.to_string_lossy().into_owned())
        .unwrap_or_else(|| "postgres".to_owned());
    let port = unsafe { pg_sys::PostPortNumber };
    cmd.env(
        "MODAK_PG_URL",
        format!("jdbc:postgresql://localhost:{port}/{database}"),
    );

    if let Some(env) = WORKER_ENV.get() {
        for pair in env.to_string_lossy().split(';') {
            if pair.trim().is_empty() {
                continue;
            }
            let Some((key, value)) = pair.split_once('=') else {
                pgrx::warning!("modak supervisor: ignoring malformed worker_env entry '{pair}'");
                continue;
            };
            cmd.env(key.trim(), value.trim());
        }
    }

    cmd.spawn()
}

fn shutdown(child: &mut Child) {
    let pid = child.id() as i32;
    pgrx::log!("modak supervisor: shutting down the worker (pid {pid})");
    unsafe {
        libc::kill(-pid, libc::SIGTERM);
    }
    for _ in 0..100 {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) => std::thread::sleep(Duration::from_millis(100)),
            Err(_) => return,
        }
    }
    pgrx::warning!("modak supervisor: worker ignored SIGTERM for 10s, killing");
    unsafe {
        libc::kill(-pid, libc::SIGKILL);
    }
    let _ = child.wait();
}
