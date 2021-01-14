use crate::cassandra::batch::CustomPayload;
use crate::cassandra::collection::List;
use crate::cassandra::collection::Map;
// use decimal::d128;
use crate::cassandra::collection::Set;
use crate::cassandra::consistency::Consistency;
use crate::cassandra::error::*;
use crate::cassandra::future::CassFuture;
use crate::cassandra::inet::Inet;
use crate::cassandra::policy::retry::RetryPolicy;
use crate::cassandra::result::CassResult;
use crate::cassandra::tuple::Tuple;
use crate::cassandra::user_type::UserType;
use crate::cassandra::util::{Protected, ProtectedInner, ProtectedWithSession};
use crate::cassandra::uuid::Uuid;
use crate::Session;

use crate::cassandra_sys::cass_false;
use crate::cassandra_sys::cass_session_execute;
use crate::cassandra_sys::cass_statement_add_key_index;
use crate::cassandra_sys::cass_statement_bind_bool;
use crate::cassandra_sys::cass_statement_bind_bool_by_name_n;
use crate::cassandra_sys::cass_statement_bind_bytes;
use crate::cassandra_sys::cass_statement_bind_bytes_by_name_n;
use crate::cassandra_sys::cass_statement_bind_collection;
use crate::cassandra_sys::cass_statement_bind_collection_by_name_n;
use crate::cassandra_sys::cass_statement_bind_decimal;
use crate::cassandra_sys::cass_statement_bind_decimal_by_name_n;
use crate::cassandra_sys::cass_statement_bind_double;
use crate::cassandra_sys::cass_statement_bind_double_by_name_n;
use crate::cassandra_sys::cass_statement_bind_float;
use crate::cassandra_sys::cass_statement_bind_float_by_name_n;
use crate::cassandra_sys::cass_statement_bind_inet;
use crate::cassandra_sys::cass_statement_bind_inet_by_name_n;
use crate::cassandra_sys::cass_statement_bind_int16;
use crate::cassandra_sys::cass_statement_bind_int16_by_name_n;
use crate::cassandra_sys::cass_statement_bind_int32;
use crate::cassandra_sys::cass_statement_bind_int32_by_name_n;
use crate::cassandra_sys::cass_statement_bind_int64;
use crate::cassandra_sys::cass_statement_bind_int64_by_name_n;
use crate::cassandra_sys::cass_statement_bind_int8;
use crate::cassandra_sys::cass_statement_bind_int8_by_name_n;
use crate::cassandra_sys::cass_statement_bind_null;
use crate::cassandra_sys::cass_statement_bind_null_by_name_n;
use crate::cassandra_sys::cass_statement_bind_string_by_name_n;
use crate::cassandra_sys::cass_statement_bind_string_n;
use crate::cassandra_sys::cass_statement_bind_tuple;
use crate::cassandra_sys::cass_statement_bind_tuple_by_name_n;
use crate::cassandra_sys::cass_statement_bind_uint32;
use crate::cassandra_sys::cass_statement_bind_uint32_by_name_n;
use crate::cassandra_sys::cass_statement_bind_user_type;
use crate::cassandra_sys::cass_statement_bind_user_type_by_name_n;
use crate::cassandra_sys::cass_statement_bind_uuid;
use crate::cassandra_sys::cass_statement_bind_uuid_by_name_n;
use crate::cassandra_sys::cass_statement_free;
use crate::cassandra_sys::cass_statement_new_n;
use crate::cassandra_sys::cass_statement_set_consistency;
use crate::cassandra_sys::cass_statement_set_custom_payload;
use crate::cassandra_sys::cass_statement_set_keyspace_n;
use crate::cassandra_sys::cass_statement_set_paging_size;
use crate::cassandra_sys::cass_statement_set_paging_state;
use crate::cassandra_sys::cass_statement_set_paging_state_token;
use crate::cassandra_sys::cass_statement_set_request_timeout;
use crate::cassandra_sys::cass_statement_set_retry_policy;
use crate::cassandra_sys::cass_statement_set_serial_consistency;
use crate::cassandra_sys::cass_statement_set_timestamp;
use crate::cassandra_sys::cass_true;
use crate::cassandra_sys::CassStatement as _Statement;
use crate::cassandra_sys::CASS_UINT64_MAX;

use std::os::raw::c_char;
use time::Duration;

#[derive(Debug)]
struct StatementInner(*mut _Statement);

impl StatementInner {
    fn new(query: &str, parameter_count: usize) -> Self {
        let query_ptr = query.as_ptr() as *const c_char;
        Self(unsafe { cass_statement_new_n(query_ptr, query.len(), parameter_count) })
    }
}

/// A statement object is an executable query. It represents either a regular
/// (adhoc) statement or a prepared statement. It maintains the queries' parameter
/// values along with query options (consistency level, paging state, etc.)
///
/// <b>Note:</b> Parameters for regular queries are not supported by the binary protocol
/// version 1.
#[derive(Debug)]
pub struct Statement(StatementInner, Session);

// The underlying C type has no thread-local state, but does not support access
// from multiple threads: https://datastax.github.io/cpp-driver/topics/#thread-safety
unsafe impl Send for StatementInner {}

impl ProtectedInner<*mut _Statement> for StatementInner {
    #[inline(always)]
    fn inner(&self) -> *mut _Statement {
        self.0
    }
}

impl Protected<*mut _Statement> for StatementInner {
    #[inline(always)]
    fn build(inner: *mut _Statement) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        Self(inner)
    }
}

impl ProtectedInner<*mut _Statement> for Statement {
    #[inline(always)]
    fn inner(&self) -> *mut _Statement {
        self.0.inner()
    }
}

impl ProtectedWithSession<*mut _Statement> for Statement {
    #[inline(always)]
    fn build(inner: *mut _Statement, session: Session) -> Self {
        Statement(StatementInner::build(inner), session)
    }

    #[inline(always)]
    fn session(&self) -> &Session {
        &self.1
    }
}

// statement,
//  key,
//  i % 2 == 0,
//  i as f32 / 2.0f32,
//  i as f64 / 200.0,
//  i as i32 * 10,
//  i as i64 * 100);

impl Drop for StatementInner {
    /// Frees a statement instance. Statements can be immediately freed after
    /// being prepared, executed or added to a batch.
    fn drop(&mut self) {
        unsafe { cass_statement_free(self.0) }
    }
}

/// All Rust types that can be bound to a cassandra statement
/// //FIXME not yet implemented
// pub enum CassBindable {
//
// }
/// Any rust type that can have a default bind implementation
pub trait BindRustType<T> {
    /// binds a rust type to C* by index
    fn bind(&mut self, index: usize, value: T) -> Result<&mut Statement>;
    /// binds a rust type to C* by name
    fn bind_by_name(&mut self, col: &str, value: T) -> Result<&mut Statement>;
}

impl BindRustType<bool> for Statement {
    fn bind(&mut self, index: usize, value: bool) -> Result<&mut Self> {
        self.bind_bool(index, value)
    }

    fn bind_by_name(&mut self, col: &str, value: bool) -> Result<&mut Self> {
        self.bind_bool_by_name(col, value)
    }
}

impl BindRustType<f32> for Statement {
    fn bind(&mut self, index: usize, value: f32) -> Result<&mut Self> {
        self.bind_float(index, value)
    }

    fn bind_by_name(&mut self, col: &str, value: f32) -> Result<&mut Self> {
        self.bind_float_by_name(col, value)
    }
}

impl BindRustType<f64> for Statement {
    fn bind(&mut self, index: usize, value: f64) -> Result<&mut Self> {
        self.bind_double(index, value)
    }

    fn bind_by_name(&mut self, col: &str, value: f64) -> Result<&mut Self> {
        self.bind_double_by_name(col, value)
    }
}

impl BindRustType<i8> for Statement {
    fn bind(&mut self, index: usize, value: i8) -> Result<&mut Self> {
        self.bind_int8(index, value)
    }

    fn bind_by_name(&mut self, col: &str, value: i8) -> Result<&mut Self> {
        self.bind_int8_by_name(col, value)
    }
}

impl BindRustType<i16> for Statement {
    fn bind(&mut self, index: usize, value: i16) -> Result<&mut Self> {
        self.bind_int16(index, value)
    }

    fn bind_by_name(&mut self, col: &str, value: i16) -> Result<&mut Self> {
        self.bind_int16_by_name(col, value)
    }
}

impl BindRustType<i32> for Statement {
    fn bind(&mut self, index: usize, value: i32) -> Result<&mut Self> {
        self.bind_int32(index, value)
    }

    fn bind_by_name(&mut self, col: &str, value: i32) -> Result<&mut Self> {
        self.bind_int32_by_name(col, value)
    }
}

impl BindRustType<i64> for Statement {
    fn bind(&mut self, index: usize, value: i64) -> Result<&mut Self> {
        self.bind_int64(index, value)
    }

    fn bind_by_name(&mut self, col: &str, value: i64) -> Result<&mut Self> {
        self.bind_int64_by_name(col, value)
    }
}

impl BindRustType<u32> for Statement {
    fn bind(&mut self, index: usize, value: u32) -> Result<&mut Self> {
        self.bind_uint32(index, value)
    }

    fn bind_by_name(&mut self, col: &str, value: u32) -> Result<&mut Self> {
        self.bind_uint32_by_name(col, value)
    }
}

impl<'a> BindRustType<&'a str> for Statement {
    fn bind(&mut self, index: usize, value: &str) -> Result<&mut Self> {
        self.bind_string(index, value)
    }

    fn bind_by_name(&mut self, col: &str, value: &str) -> Result<&mut Self> {
        self.bind_string_by_name(col, value)
    }
}

impl BindRustType<Set> for Statement {
    fn bind(&mut self, index: usize, value: Set) -> Result<&mut Self> {
        self.bind_set(index, value)
    }

    fn bind_by_name(&mut self, col: &str, value: Set) -> Result<&mut Self> {
        self.bind_set_by_name(col, value)
    }
}

impl BindRustType<List> for Statement {
    fn bind(&mut self, index: usize, value: List) -> Result<&mut Self> {
        self.bind_list(index, value)
    }

    fn bind_by_name(&mut self, col: &str, value: List) -> Result<&mut Self> {
        self.bind_list_by_name(col, value)
    }
}

impl BindRustType<Uuid> for Statement {
    fn bind(&mut self, index: usize, value: Uuid) -> Result<&mut Self> {
        self.bind_uuid(index, value)
    }

    fn bind_by_name(&mut self, col: &str, value: Uuid) -> Result<&mut Self> {
        self.bind_uuid_by_name(col, value)
    }
}

impl BindRustType<uuid::Uuid> for Statement {
    fn bind(&mut self, index: usize, value: uuid::Uuid) -> Result<&mut Self> {
        self.bind_uuid(index, value.into())
    }

    fn bind_by_name(&mut self, col: &str, value: uuid::Uuid) -> Result<&mut Self> {
        self.bind_uuid_by_name(col, value.into())
    }
}

impl BindRustType<Inet> for Statement {
    fn bind(&mut self, index: usize, value: Inet) -> Result<&mut Self> {
        self.bind_inet(index, value)
    }

    fn bind_by_name(&mut self, col: &str, value: Inet) -> Result<&mut Self> {
        self.bind_inet_by_name(col, value)
    }
}

impl BindRustType<Map> for Statement {
    fn bind(&mut self, index: usize, value: Map) -> Result<&mut Self> {
        self.bind_map(index, value)
    }

    fn bind_by_name(&mut self, col: &str, value: Map) -> Result<&mut Self> {
        self.bind_map_by_name(col, value)
    }
}

impl BindRustType<Tuple> for Statement {
    fn bind(&mut self, index: usize, value: Tuple) -> Result<&mut Self> {
        self.bind_tuple(index, value)
    }

    fn bind_by_name(&mut self, col: &str, value: Tuple) -> Result<&mut Self> {
        self.bind_tuple_by_name(col, value)
    }
}

impl BindRustType<&UserType> for Statement {
    fn bind(&mut self, index: usize, value: &UserType) -> Result<&mut Self> {
        self.bind_user_type(index, value)
    }

    fn bind_by_name(&mut self, col: &str, value: &UserType) -> Result<&mut Self> {
        self.bind_user_type_by_name(col, value)
    }
}

impl BindRustType<Vec<u8>> for Statement {
    fn bind(&mut self, index: usize, value: Vec<u8>) -> Result<&mut Self> {
        self.bind_bytes(index, value)
    }

    fn bind_by_name(&mut self, col: &str, value: Vec<u8>) -> Result<&mut Self> {
        self.bind_bytes_by_name(col, value)
    }
}

impl Statement {
    /// Creates a new query statement.
    pub(crate) fn new(session: Session, query: &str, parameter_count: usize) -> Self {
        Statement(StatementInner::new(query, parameter_count), session)
    }

    /// Returns the session of which this statement is bound to.
    pub fn session(&self) -> &Session {
        ProtectedWithSession::session(self)
    }

    /// Executes the statement.
    pub async fn execute(self) -> Result<CassResult> {
        let (statement, session) = (self.0, self.1);
        let fut = {
            let execute = unsafe { cass_session_execute(session.inner(), statement.inner()) };
            <CassFuture<CassResult>>::build(session, execute)
        };
        fut.await
    }

    //    ///Binds an arbitrary CassBindable type to a cassandra statement
    //    ///FIXME not yet implemented
    //    pub fn bind(&mut self, params: Vec<CassBindable>) {
    //        let _ = params;
    //        unimplemented!();
    //    }

    /// Adds a key index specifier to this a statement.
    /// When using token-aware routing, this can be used to tell the driver which
    /// parameters within a non-prepared, parameterized statement are part of
    /// the partition key.
    ///
    /// Use consecutive calls for composite partition keys.
    ///
    /// This is not necessary for prepared statements, as the key
    /// parameters are determined in the metadata processed in the prepare phase.
    pub fn add_key_index(&mut self, index: usize) -> Result<&mut Self> {
        unsafe { cass_statement_add_key_index(self.inner(), index).to_result(self) }
    }

    /// Sets the statement's keyspace for use with token-aware routing.
    ///
    /// This is not necessary for prepared statements, as the keyspace
    /// is determined in the metadata processed in the prepare phase.
    pub fn set_keyspace(&mut self, keyspace: String) -> Result<&mut Self> {
        unsafe {
            let keyspace_ptr = keyspace.as_ptr() as *const c_char;
            cass_statement_set_keyspace_n(self.inner(), keyspace_ptr, keyspace.len())
                .to_result(self)
        }
    }

    /// Sets the statement's consistency level.
    ///
    /// <b>Default:</b> CASS_CONSISTENCY_LOCAL_ONE
    pub fn set_consistency(&mut self, consistency: Consistency) -> Result<&mut Self> {
        unsafe { cass_statement_set_consistency(self.inner(), consistency.inner()).to_result(self) }
    }

    /// Sets the statement's serial consistency level.
    ///
    /// <b>Default:</b> Not set
    pub fn set_serial_consistency(&mut self, serial_consistency: Consistency) -> Result<&mut Self> {
        unsafe {
            cass_statement_set_serial_consistency(self.inner(), serial_consistency.inner())
                .to_result(self)
        }
    }

    /// Sets the statement's page size.
    ///
    /// <b>Default:</b> -1 (Disabled)
    pub fn set_paging_size(&mut self, page_size: i32) -> Result<&mut Self> {
        unsafe { cass_statement_set_paging_size(self.inner(), page_size).to_result(self) }
    }

    /// Sets the statement's paging state. This can be used to get the next page of
    /// data in a multi-page query.
    pub fn set_paging_state(&mut self, result: CassResult) -> Result<&mut Self> {
        unsafe { cass_statement_set_paging_state(self.inner(), result.inner()).to_result(self) }
    }

    /// Sets the statement's paging state.  This can be used to get the next page of
    /// data in a multi-page query.
    ///
    /// <b>Warning:</b> The paging state should not be exposed to or come from
    /// untrusted environments. The paging state could be spoofed and potentially
    /// used to gain access to other data.
    pub fn set_paging_state_token(&mut self, paging_state: &[u8]) -> Result<&mut Self> {
        unsafe {
            cass_statement_set_paging_state_token(
                self.inner(),
                paging_state.as_ptr() as *const i8,
                paging_state.len(),
            )
            .to_result(self)
        }
    }

    /// Sets the statement's timestamp.
    pub fn set_timestamp(&mut self, timestamp: i64) -> Result<&mut Self> {
        unsafe { cass_statement_set_timestamp(self.inner(), timestamp).to_result(self) }
    }

    /// Sets the statement's timeout for waiting for a response from a node.
    /// Some(Duration::milliseconds(0)) sets no timeout, and None disables it
    /// (to use the cluster-level request timeout).
    pub fn set_statement_request_timeout(&mut self, timeout: Option<Duration>) -> &mut Self {
        unsafe {
            let timeout_millis = match timeout {
                None => CASS_UINT64_MAX as u64,
                Some(time) => time.whole_milliseconds() as u64,
            };
            cass_statement_set_request_timeout(self.inner(), timeout_millis);
        }
        self
    }

    /// Sets the statement's retry policy.
    pub fn set_retry_policy(&mut self, retry_policy: RetryPolicy) -> Result<&mut Self> {
        unsafe {
            cass_statement_set_retry_policy(self.inner(), retry_policy.inner()).to_result(self)
        }
    }

    /// Sets the statement's custom payload.
    pub fn set_custom_payload(&mut self, payload: CustomPayload) -> Result<&mut Self> {
        unsafe { cass_statement_set_custom_payload(self.inner(), payload.inner()).to_result(self) }
    }

    /// Binds null to a query or bound statement at the specified index.
    pub fn bind_null(&mut self, index: usize) -> Result<&mut Self> {
        unsafe { cass_statement_bind_null(self.inner(), index).to_result(self) }
    }

    /// Binds a null to all the values with the specified name.
    ///
    /// This can only be used with statements created by
    /// cass_prepared_bind().
    pub fn bind_null_by_name(&mut self, name: &str) -> Result<&mut Self> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            cass_statement_bind_null_by_name_n(self.inner(), name_ptr, name.len()).to_result(self)
        }
    }

    /// Binds a "tinyint" to a query or bound statement at the specified index.
    pub fn bind_int8(&mut self, index: usize, value: i8) -> Result<&mut Self> {
        unsafe { cass_statement_bind_int8(self.inner(), index, value).to_result(self) }
    }

    /// Binds a "tinyint" to all the values with the specified name.
    pub fn bind_int8_by_name(&mut self, name: &str, value: i8) -> Result<&mut Self> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            cass_statement_bind_int8_by_name_n(self.inner(), name_ptr, name.len(), value)
                .to_result(self)
        }
    }

    /// Binds an "smallint" to a query or bound statement at the specified index.
    pub fn bind_int16(&mut self, index: usize, value: i16) -> Result<&mut Self> {
        unsafe { cass_statement_bind_int16(self.inner(), index, value).to_result(self) }
    }

    /// Binds a "smallint" to all the values with the specified name.
    pub fn bind_int16_by_name(&mut self, name: &str, value: i16) -> Result<&mut Self> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            cass_statement_bind_int16_by_name_n(self.inner(), name_ptr, name.len(), value)
                .to_result(self)
        }
    }

    /// Binds an "int" to a query or bound statement at the specified index.
    pub fn bind_int32(&mut self, index: usize, value: i32) -> Result<&mut Self> {
        unsafe { cass_statement_bind_int32(self.inner(), index, value).to_result(self) }
    }

    /// Binds an "int" to all the values with the specified name.
    pub fn bind_int32_by_name(&mut self, name: &str, value: i32) -> Result<&mut Self> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            cass_statement_bind_int32_by_name_n(self.inner(), name_ptr, name.len(), value)
                .to_result(self)
        }
    }

    /// Binds a "date" to a query or bound statement at the specified index.
    pub fn bind_uint32(&mut self, index: usize, value: u32) -> Result<&mut Self> {
        unsafe { cass_statement_bind_uint32(self.inner(), index, value).to_result(self) }
    }

    /// Binds a "date" to all the values with the specified name.
    ///
    /// This can only be used with statements created by
    /// cass_prepared_bind().
    pub fn bind_uint32_by_name(&mut self, name: &str, value: u32) -> Result<&mut Self> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            cass_statement_bind_uint32_by_name_n(self.inner(), name_ptr, name.len(), value)
                .to_result(self)
        }
    }

    /// Binds a "bigint", "counter", "timestamp" or "time" to a query or
    /// bound statement at the specified index.
    pub fn bind_int64(&mut self, index: usize, value: i64) -> Result<&mut Self> {
        unsafe { cass_statement_bind_int64(self.inner(), index, value).to_result(self) }
    }

    /// Binds a "bigint", "counter", "timestamp" or "time" to all values
    /// with the specified name.
    pub fn bind_int64_by_name(&mut self, name: &str, value: i64) -> Result<&mut Self> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            cass_statement_bind_int64_by_name_n(self.inner(), name_ptr, name.len(), value)
                .to_result(self)
        }
    }

    /// Binds a "float" to a query or bound statement at the specified index.
    pub fn bind_float(&mut self, index: usize, value: f32) -> Result<&mut Self> {
        unsafe { cass_statement_bind_float(self.inner(), index, value).to_result(self) }
    }

    /// Binds a "float" to all the values with the specified name.
    ///
    /// This can only be used with statements created by
    /// cass_prepared_bind().
    pub fn bind_float_by_name(&mut self, name: &str, value: f32) -> Result<&mut Self> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            cass_statement_bind_float_by_name_n(self.inner(), name_ptr, name.len(), value)
                .to_result(self)
        }
    }

    /// Binds a "double" to a query or bound statement at the specified index.
    pub fn bind_double(&mut self, index: usize, value: f64) -> Result<&mut Self> {
        unsafe { cass_statement_bind_double(self.inner(), index, value).to_result(self) }
    }

    /// Binds a "double" to all the values with the specified name.
    ///
    /// This can only be used with statements created by
    /// cass_prepared_bind().
    pub fn bind_double_by_name(&mut self, name: &str, value: f64) -> Result<&mut Self> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            cass_statement_bind_double_by_name_n(self.inner(), name_ptr, name.len(), value)
                .to_result(self)
        }
    }

    /// Binds a "boolean" to a query or bound statement at the specified index.
    pub fn bind_bool(&mut self, index: usize, value: bool) -> Result<&mut Self> {
        unsafe {
            cass_statement_bind_bool(
                self.inner(),
                index,
                if value { cass_true } else { cass_false },
            )
            .to_result(self)
        }
    }

    /// Binds a "boolean" to all the values with the specified name.
    ///
    /// This can only be used with statements created by
    /// cass_prepared_bind().
    pub fn bind_bool_by_name(&mut self, name: &str, value: bool) -> Result<&mut Self> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            cass_statement_bind_bool_by_name_n(
                self.inner(),
                name_ptr,
                name.len(),
                if value { cass_true } else { cass_false },
            )
            .to_result(self)
        }
    }

    /// Binds an "ascii", "text" or "varchar" to a query or bound statement
    /// at the specified index.
    pub fn bind_string(&mut self, index: usize, value: &str) -> Result<&mut Self> {
        unsafe {
            let value_ptr = value.as_ptr() as *const c_char;
            cass_statement_bind_string_n(self.inner(), index, value_ptr, value.len())
                .to_result(self)
        }
    }

    /// Binds an "ascii", "text" or "varchar" to all the values
    /// with the specified name.
    ///
    /// This can only be used with statements created by
    /// cass_prepared_bind().
    pub fn bind_string_by_name(&mut self, name: &str, value: &str) -> Result<&mut Self> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;

            // cass_statement_bind_string_by_name_n is incorrectly ignoring the
            // value_length parameter so we have to allocate a new
            // NULL-terminated string.
            let value_cstr = std::ffi::CString::new(value)?;

            cass_statement_bind_string_by_name_n(
                self.inner(),
                name_ptr,
                name.len(),
                value_cstr.as_ptr(),
                value.len(),
            )
            .to_result(self)
        }
    }

    /// Binds a "blob", "varint" or "custom" to a query or bound statement at the specified index.
    pub fn bind_bytes(&mut self, index: usize, value: Vec<u8>) -> Result<&mut Self> {
        unsafe {
            cass_statement_bind_bytes(self.inner(), index, value.as_ptr(), value.len())
                .to_result(self)
        }
    }

    /// Binds a "blob", "varint" or "custom" to all the values with the
    /// specified name.
    ///
    /// This can only be used with statements created by
    /// cass_prepared_bind().
    pub fn bind_bytes_by_name(&mut self, name: &str, mut value: Vec<u8>) -> Result<&mut Self> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            cass_statement_bind_bytes_by_name_n(
                self.inner(),
                name_ptr,
                name.len(),
                value.as_mut_ptr(),
                value.len(),
            )
            .to_result(self)
        }
    }

    /// Binds a "uuid" or "timeuuid" to a query or bound statement at the specified index.
    pub fn bind_uuid(&mut self, index: usize, value: Uuid) -> Result<&mut Self> {
        unsafe { cass_statement_bind_uuid(self.inner(), index, value.inner()).to_result(self) }
    }

    /// Binds a "uuid" or "timeuuid" to all the values
    /// with the specified name.
    ///
    /// This can only be used with statements created by
    /// cass_prepared_bind().
    pub fn bind_uuid_by_name(&mut self, name: &str, value: Uuid) -> Result<&mut Self> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            cass_statement_bind_uuid_by_name_n(self.inner(), name_ptr, name.len(), value.inner())
                .to_result(self)
        }
    }

    /// Binds an "inet" to a query or bound statement at the specified index.
    pub fn bind_inet(&mut self, index: usize, value: Inet) -> Result<&mut Self> {
        unsafe { cass_statement_bind_inet(self.inner(), index, value.inner()).to_result(self) }
    }

    /// Binds an "inet" to all the values with the specified name.
    pub fn bind_inet_by_name(&mut self, name: &str, value: Inet) -> Result<&mut Self> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            cass_statement_bind_inet_by_name_n(self.inner(), name_ptr, name.len(), value.inner())
                .to_result(self)
        }
    }

    //  ///Bind a "decimal" to a query or bound statement at the specified index.
    //    pub fn bind_decimal(&self,
    //                                index: i32,
    //                                value: d128)
    //                                -> Result<&mut Self, CassError> {
    //            unsafe {
    //                CassError::build(
    //                    cass_statement_bind_decimal(
    //                        self.inner(),
    //                        index,
    //                        value
    //                    )
    //                ).wrap(&mut self)
    //            }
    //        }

    // Binds a "decimal" to all the values with the specified name.
    //
    // This can only be used with statements created by
    // cass_prepared_bind().
    //    pub fn bind_decimal_by_name<'a>(&'a self,
    //                                    name: &str,
    //                                    value: String)
    //                                    -> Result<&'a Self, CassError> {
    //        unsafe {
    //            let name = CString::new(name).unwrap();
    //            CassError::build(
    //            cass_statement_bind_decimal_by_name(
    //                self.inner(),
    //                name.as_ptr(),
    //                value
    //            )
    //        ).wrap(&self)
    //        }
    //    }

    /// Bind a "map" to a query or bound statement at the specified index.
    pub fn bind_map(&mut self, index: usize, map: Map) -> Result<&mut Self> {
        unsafe { cass_statement_bind_collection(self.inner(), index, map.inner()).to_result(self) }
    }

    /// Bind a "map" to all the values with the
    /// specified name.
    ///
    /// This can only be used with statements created by
    /// cass_prepared_bind().
    pub fn bind_map_by_name(&mut self, name: &str, map: Map) -> Result<&mut Self> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            cass_statement_bind_collection_by_name_n(
                self.inner(),
                name_ptr,
                name.len(),
                map.inner(),
            )
            .to_result(self)
        }
    }
    /// Bind a "set" to a query or bound statement at the specified index.
    pub fn bind_set(&mut self, index: usize, collection: Set) -> Result<&mut Self> {
        unsafe {
            cass_statement_bind_collection(self.inner(), index, collection.inner()).to_result(self)
        }
    }

    /// Bind a "set" to all the values with the
    /// specified name.
    ///
    /// This can only be used with statements created by
    /// cass_prepared_bind().
    pub fn bind_set_by_name(&mut self, name: &str, collection: Set) -> Result<&mut Self> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            cass_statement_bind_collection_by_name_n(
                self.inner(),
                name_ptr,
                name.len(),
                collection.inner(),
            )
            .to_result(self)
        }
    }

    /// Bind a "list" to a query or bound statement at the specified index.
    pub fn bind_list(&mut self, index: usize, collection: List) -> Result<&mut Self> {
        unsafe {
            cass_statement_bind_collection(self.inner(), index, collection.inner()).to_result(self)
        }
    }

    /// Bind a "list" to all the values with the
    /// specified name.
    ///
    /// This can only be used with statements created by
    /// cass_prepared_bind().
    pub fn bind_list_by_name(&mut self, name: &str, collection: List) -> Result<&mut Self> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            cass_statement_bind_collection_by_name_n(
                self.inner(),
                name_ptr,
                name.len(),
                collection.inner(),
            )
            .to_result(self)
        }
    }

    /// Bind a "tuple" to a query or bound statement at the specified index.
    pub fn bind_tuple(&mut self, index: usize, value: Tuple) -> Result<&mut Self> {
        unsafe { cass_statement_bind_tuple(self.inner(), index, value.inner()).to_result(self) }
    }

    /// Bind a "tuple" to all the values with the specified name.
    ///
    /// This can only be used with statements created by
    /// cass_prepared_bind().
    pub fn bind_tuple_by_name(&mut self, name: &str, value: Tuple) -> Result<&mut Self> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            cass_statement_bind_tuple_by_name_n(self.inner(), name_ptr, name.len(), value.inner())
                .to_result(self)
        }
    }

    /// Bind a user defined type to a query or bound statement at the
    /// specified index.
    pub fn bind_user_type(&mut self, index: usize, value: &UserType) -> Result<&mut Self> {
        unsafe { cass_statement_bind_user_type(self.inner(), index, value.inner()).to_result(self) }
    }

    /// Bind a user defined type to a query or bound statement with the
    /// specified name.
    pub fn bind_user_type_by_name(&mut self, name: &str, value: &UserType) -> Result<&mut Self> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            cass_statement_bind_user_type_by_name_n(
                self.inner(),
                name_ptr,
                name.len(),
                value.inner(),
            )
            .to_result(self)
        }
    }
}
