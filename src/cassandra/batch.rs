use crate::cassandra::consistency::Consistency;
use crate::cassandra::error::*;
use crate::cassandra::future::CassFuture;
use crate::cassandra::policy::retry::RetryPolicy;
use crate::cassandra::statement::Statement;
use crate::cassandra::util::{Protected, ProtectedInner, ProtectedWithSession};
use crate::{CassResult, Session};

use crate::cassandra_sys::cass_batch_add_statement;
use crate::cassandra_sys::cass_batch_free;
use crate::cassandra_sys::cass_batch_new;
use crate::cassandra_sys::cass_batch_set_consistency;
use crate::cassandra_sys::cass_batch_set_custom_payload;
use crate::cassandra_sys::cass_batch_set_retry_policy;
use crate::cassandra_sys::cass_batch_set_serial_consistency;
use crate::cassandra_sys::cass_batch_set_timestamp;
use crate::cassandra_sys::cass_custom_payload_free;
use crate::cassandra_sys::cass_custom_payload_new;
use crate::cassandra_sys::cass_custom_payload_set_n;
use crate::cassandra_sys::cass_session_execute_batch;
use crate::cassandra_sys::CassBatch as _Batch;
use crate::cassandra_sys::CassBatchType_;
use crate::cassandra_sys::CassConsistency;
use crate::cassandra_sys::CassCustomPayload as _CassCustomPayload;
use std::ffi::NulError;
use std::os::raw::c_char;

#[derive(Debug)]
struct BatchInner(*mut _Batch);

/// A group of statements that are executed as a single batch.
/// <b>Note:</b> Batches are not supported by the binary protocol version 1.
#[derive(Debug)]
pub struct Batch(BatchInner, Session);

// The underlying C type has no thread-local state, but does not support access
// from multiple threads: https://datastax.github.io/cpp-driver/topics/#thread-safety
unsafe impl Send for BatchInner {}

impl ProtectedInner<*mut _Batch> for BatchInner {
    #[inline(always)]
    fn inner(&self) -> *mut _Batch {
        self.0
    }
}

impl Protected<*mut _Batch> for BatchInner {
    #[inline(always)]
    fn build(inner: *mut _Batch) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        Self(inner)
    }
}

impl ProtectedInner<*mut _Batch> for Batch {
    #[inline(always)]
    fn inner(&self) -> *mut _Batch {
        self.0.inner()
    }
}

impl ProtectedWithSession<*mut _Batch> for Batch {
    #[inline(always)]
    fn build(inner: *mut _Batch, session: Session) -> Self {
        Self(BatchInner::build(inner), session)
    }

    #[inline(always)]
    fn session(&self) -> &Session {
        &self.1
    }
}

/// Custom payloads not fully supported yet
#[derive(Debug)]
pub struct CustomPayload(*mut _CassCustomPayload);

impl ProtectedInner<*mut _CassCustomPayload> for CustomPayload {
    fn inner(&self) -> *mut _CassCustomPayload {
        self.0
    }
}

impl Protected<*mut _CassCustomPayload> for CustomPayload {
    fn build(inner: *mut _CassCustomPayload) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        CustomPayload(inner)
    }
}

impl Default for CustomPayload {
    /// creates a new custom payload
    fn default() -> Self {
        unsafe { CustomPayload(cass_custom_payload_new()) }
    }
}
impl CustomPayload {
    /// Sets an item to the custom payload.
    pub fn set(&self, name: String, value: &[u8]) -> Result<()> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            Ok(cass_custom_payload_set_n(
                self.0,
                name_ptr,
                name.len(),
                value.as_ptr(),
                value.len(),
            ))
        }
    }
}

impl Drop for CustomPayload {
    fn drop(&mut self) {
        unsafe { cass_custom_payload_free(self.0) }
    }
}

impl Drop for BatchInner {
    /// Frees a batch instance. Batches can be immediately freed after being
    /// executed.
    fn drop(&mut self) {
        unsafe { cass_batch_free(self.0) }
    }
}

impl Batch {
    /// Creates a new batch statement with batch type.
    pub(crate) fn new(batch_type: BatchType, session: Session) -> Batch {
        unsafe { Batch(BatchInner(cass_batch_new(batch_type.inner())), session) }
    }

    /// Returns the session of which this batch is bound to.
    pub fn session(&self) -> &Session {
        ProtectedWithSession::session(self)
    }

    /// Executes this batch.
    pub async fn execute(self) -> Result<CassResult> {
        let (batch, session) = (self.0, self.1);
        let execute_future = {
            let execute_batch =
                unsafe { cass_session_execute_batch(session.inner(), batch.inner()) };
            CassFuture::build(session, execute_batch)
        };
        execute_future.await
    }

    /// Sets the batch's consistency level
    pub fn set_consistency(&mut self, consistency: Consistency) -> Result<&mut Self> {
        unsafe { cass_batch_set_consistency(self.inner(), consistency.inner()).to_result(self) }
    }

    /// Sets the batch's serial consistency level.
    ///
    /// <b>Default:</b> Not set
    pub fn set_serial_consistency(&mut self, consistency: Consistency) -> Result<&mut Self> {
        unsafe {
            cass_batch_set_serial_consistency(self.inner(), consistency.inner()).to_result(self)
        }
    }

    /// Sets the batch's timestamp.
    pub fn set_timestamp(&mut self, timestamp: i64) -> Result<&Self> {
        unsafe { cass_batch_set_timestamp(self.inner(), timestamp).to_result(self) }
    }

    /// Sets the batch's retry policy.
    pub fn set_retry_policy(&mut self, retry_policy: RetryPolicy) -> Result<&mut Self> {
        unsafe { cass_batch_set_retry_policy(self.inner(), retry_policy.inner()).to_result(self) }
    }

    /// Sets the batch's custom payload.
    pub fn set_custom_payload(&mut self, custom_payload: CustomPayload) -> Result<&mut Self> {
        unsafe { cass_batch_set_custom_payload(self.inner(), custom_payload.0).to_result(self) }
    }

    /// Adds a statement to a batch.
    pub fn add_statement(&mut self, statement: Statement) -> Result<&Self> {
        // If their sessions are not the same, we can reject at this level.
        if self.session() != statement.session() {
            return Err(ErrorKind::BatchSessionMismatch(
                self.session().clone(),
                statement.session().clone(),
            )
            .into());
        }
        unsafe { cass_batch_add_statement(self.inner(), statement.inner()).to_result(self) }
    }
}

/// A type of batch.
#[derive(Debug, Eq, PartialEq, Copy, Clone, Hash)]
#[allow(missing_docs)] // Meanings are defined in CQL documentation.
#[allow(non_camel_case_types)] // Names are traditional.
pub enum BatchType {
    LOGGED,
    UNLOGGED,
    COUNTER,
}

enhance_nullary_enum!(BatchType, CassBatchType_, {
    (LOGGED, CASS_BATCH_TYPE_LOGGED, "LOGGED"),
    (UNLOGGED, CASS_BATCH_TYPE_UNLOGGED, "UNLOGGED"),
    (COUNTER, CASS_BATCH_TYPE_COUNTER, "COUNTER"),
});
