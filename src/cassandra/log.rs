use crate::cassandra_sys::CassLogLevel_;
use crate::cassandra_sys::CassLogMessage;
// use cassandra_sys::cass_log_cleanup; @deprecated
use crate::cassandra::util::ProtectedInner;
use crate::cassandra_sys::cass_log_set_callback;
use crate::cassandra_sys::cass_log_set_level;
use crate::cassandra_sys::CassLogCallback;
// use cassandra_sys::cass_log_set_queue_size; @deprecated

use slog;
use std::borrow::Borrow;
use std::boxed::Box;
use std::ffi::CStr;
use std::os::raw;
use std::ptr;

/// The possible logging levels that can be set.
#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Copy, Clone, Hash)]
#[allow(missing_docs)] // Meanings are defined in CQL documentation.
#[allow(non_camel_case_types)] // Names are traditional.
pub enum LogLevel {
    DISABLED,
    CRITICAL,
    ERROR,
    WARN,
    INFO,
    DEBUG,
    TRACE,
    LAST_ENTRY,
}

enhance_nullary_enum!(LogLevel, CassLogLevel_, {
    (DISABLED, CASS_LOG_DISABLED, "DISABLED"),
    (CRITICAL, CASS_LOG_CRITICAL, "CRITICAL"),
    (ERROR, CASS_LOG_ERROR, "ERROR"),
    (WARN, CASS_LOG_WARN, "WARN"),
    (INFO, CASS_LOG_INFO, "INFO"),
    (DEBUG, CASS_LOG_DEBUG, "DEBUG"),
    (TRACE, CASS_LOG_TRACE, "TRACE"),
    (LAST_ENTRY, CASS_LOG_LAST_ENTRY, "LAST_ENTRY"),
});

/// Sets the log level.
///
/// <b>Note:</b> This needs to be done before any call that might log, such as
/// any of the cass_cluster_*() or cass_ssl_*() functions.
/// <b>Default:</b> WARN
pub fn set_level(level: LogLevel) {
    unsafe { cass_log_set_level(level.inner()) }
}

/// Called by Cassandra for every log if logging is enabled. Passes the log to the configured
/// slog logger.
unsafe extern "C" fn logger_callback(log: *const CassLogMessage, data: *mut raw::c_void) {
    let log = &*log;
    let logger: &slog::Logger = &*(data as *const _);

    let message: &str = &CStr::from_ptr(log.message.as_ptr()).to_string_lossy();
    let time_ms: u64 = log.time_ms;
    let file: &str = &CStr::from_ptr(log.file).to_string_lossy();
    let line: i32 = log.line;
    let function: &str = &CStr::from_ptr(log.file).to_string_lossy();
    let kv = o!(
        "time_ms" => time_ms,
        "file" => file,
        "line" => line,
        "function" => function
    );

    // Issue the correct level of log call. Sadly even though the `log!` macro exists,
    // it's fundamental to slog that the log level is statically known for a given invocation.
    // We can't do that in this case, so we have to use this tedious workaround.
    match log.severity {
        CassLogLevel_::CASS_LOG_DISABLED | CassLogLevel_::CASS_LOG_CRITICAL => {
            crit!(logger, "{}", message; kv)
        }
        CassLogLevel_::CASS_LOG_ERROR => error!(logger, "{}", message; kv),
        CassLogLevel_::CASS_LOG_WARN => warn!(logger, "{}", message; kv),
        CassLogLevel_::CASS_LOG_INFO => info!(logger, "{}", message; kv),
        CassLogLevel_::CASS_LOG_DEBUG => debug!(logger, "{}", message; kv),
        CassLogLevel_::CASS_LOG_TRACE | CassLogLevel_::CASS_LOG_LAST_ENTRY => {
            trace!(logger, "{}", message; kv)
        }
    };
}

/// Set or unset a logger to receive all Cassandra driver logs.
pub fn set_logger(logger: Option<slog::Logger>) {
    unsafe {
        match logger {
            Some(logger) => {
                // Pass ownership to C. In fact we leak the logger; it never gets freed.
                // We don't expect this to be called many times, so we're not worried.
                let data = Box::new(logger);
                cass_log_set_callback(Some(logger_callback), Box::into_raw(data) as _)
            }
            None => cass_log_set_callback(None, ptr::null_mut()),
        }
    }
}
