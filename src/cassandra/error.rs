use crate::cassandra::consistency::Consistency;
use crate::cassandra::util::{Protected, ProtectedInner};
use crate::cassandra::value::ValueType;
use crate::cassandra::write_type::WriteType;

use crate::cassandra_sys::cass_error_desc;
use crate::cassandra_sys::cass_error_result_code;
use crate::cassandra_sys::cass_error_result_free;
use crate::cassandra_sys::cass_future_error_code;
use crate::cassandra_sys::cass_future_error_message;
use crate::cassandra_sys::cass_future_get_error_result;
use crate::cassandra_sys::CassErrorResult as CassErrorResult_;
use crate::cassandra_sys::CassError_;
use crate::cassandra_sys::CassFuture as _Future;
use crate::cassandra_sys::CASS_OK;
use crate::cassandra_sys::{
    cass_error_num_arg_types, cass_error_result_arg_type, cass_error_result_consistency,
    cass_error_result_data_present, cass_error_result_function, cass_error_result_keyspace,
    cass_error_result_num_failures, cass_error_result_responses_received,
    cass_error_result_responses_required, cass_error_result_table, cass_error_result_write_type,
};
use crate::cassandra_sys::{cass_false, cass_true};
use crate::Session;

use std::error::Error as IError;
use std::ffi::{CStr, CString};
use std::fmt::{Debug, Display, Formatter};
use std::os::raw::c_char;
use std::{fmt, mem, slice, str};

// Define the errors that may be returned by this driver.
error_chain! {
    foreign_links {
        StringContainsNul(::std::ffi::NulError)
            #[doc = "Attempted to pass a string containing `\\0` to Cassandra"];

        InvalidUtf8(::std::str::Utf8Error)
            #[doc = "Attempted to decode an invalid UTF-8-encoded string"];
    }

    errors {
        /// Cassandra error.
        CassError(code: CassErrorCode, msg: String) {
            description("Cassandra error")
            display("Cassandra error {:?}: {}", &code, &msg)
        }

        /// Errors that happen when an invalid session is passed to a batch.
        BatchSessionMismatch(batch_session: Session, statement_session: Session) {
            description("Batch cannot add a statement belonging to another session.")
            display("Batch's session {:?} != {:?}", &batch_session, &statement_session)
        }

        /// Cassandra error result with extended information.
        CassErrorResult(
            code: CassErrorCode,
            msg: String,
            consistency: Consistency,
            actual: i32,
            required: i32,
            num_failures: i32,
            data_present: bool,
            write_type: WriteType,
            keyspace: Option<String>,
            table: Option<String>,
            function: Option<(String, Vec<String>)>
        ) {
            description("Cassandra detailed error")
            display("Cassandra detailed error {:?}: {}", &code, &msg)
        }

        /// Unsupported type encountered.
        UnsupportedType(expected: &'static str, actual: ValueType) {
            description("Unsupported type")
            display("Unsupported type {}; expected {}", actual, expected)
        }

    }
}

/// Extension trait for `CassError_`.
pub(crate) trait CassErrorExt {
    /// If this operation is successful, return `default`, otherwise an appropriate error.
    fn to_result<T>(&self, default: T) -> Result<T>;

    /// This is definitely an error - return it as such.
    fn to_error(&self) -> Error;
}

impl CassErrorExt for CassError_ {
    fn to_result<T>(&self, default: T) -> Result<T> {
        unsafe {
            match *self {
                CASS_OK => Ok(default),
                _ => {
                    let message = CStr::from_ptr(cass_error_desc(*self))
                        .to_string_lossy()
                        .into_owned();
                    Err(ErrorKind::CassError(CassErrorCode::build(*self), message).into())
                }
            }
        }
    }

    fn to_error(&self) -> Error {
        unsafe {
            let message = CStr::from_ptr(cass_error_desc(*self))
                .to_string_lossy()
                .into_owned();
            ErrorKind::CassError(CassErrorCode::build(*self), message).into()
        }
    }
}

impl CassErrorExt for CassErrorCode {
    fn to_result<T>(&self, default: T) -> Result<T> {
        self.inner().to_result(default)
    }
    fn to_error(&self) -> Error {
        self.inner().to_error()
    }
}

/// Build an error from the code, message, and optional `CassErrorResult_`.
pub(crate) unsafe fn build_error_result(
    code: CassErrorCode,
    message: String,
    e: *const CassErrorResult_,
) -> Error {
    if e.is_null() {
        // No extended error available; just take the basic one.
        ErrorKind::CassError(code, message).into()
    } else {
        // Get the extended error.
        let consistency = Consistency::build(cass_error_result_consistency(e));
        let actual = cass_error_result_responses_received(e);
        // See https://datastax-oss.atlassian.net/browse/CPP-502 for these names.
        // cassandra-sys uses the actual names and works around the header bug.
        let required = cass_error_result_responses_required(e);
        let num_failures = cass_error_result_num_failures(e);
        let data_present = cass_error_result_data_present(e) != cass_false;
        let write_type = WriteType::build(cass_error_result_write_type(e));
        let keyspace = get_lossy_string(|s, s_len| cass_error_result_keyspace(e, s, s_len));
        let table = get_lossy_string(|s, s_len| cass_error_result_table(e, s, s_len));
        let function = get_lossy_string(|s, s_len| cass_error_result_function(e, s, s_len));
        let function_call = function.map(|function| {
            let i = cass_error_num_arg_types(e);
            let mut args = vec![];
            for i in 0..i {
                let arg = get_lossy_string(|s, s_len| cass_error_result_arg_type(e, i, s, s_len))
                    .unwrap_or("<error>".to_string());
                args.push(arg);
            }
            (function, args)
        });
        cass_error_result_free(e);
        ErrorKind::CassErrorResult(
            code,
            message,
            consistency,
            actual,
            required,
            num_failures,
            data_present,
            write_type,
            keyspace,
            table,
            function_call,
        )
        .into()
    }
}

/// Extract the error code and message from a Cassandra driver future
pub(crate) unsafe fn get_cass_future_error(rc: CassError_, inner: *mut _Future) -> Error {
    let code = CassErrorCode::build(rc);
    let message = get_lossy_string(|s, s_len| {
        cass_future_error_message(inner, s, s_len);
        CASS_OK
    })
    .unwrap(); // always OK so cannot fail
    build_error_result(code, message, cass_future_get_error_result(inner))
}

/// A Cassandra failure error code.
#[derive(Debug, Eq, PartialEq, Copy, Clone, Hash)]
#[allow(missing_docs)] // Meanings are defined in CQL documentation.
#[allow(non_camel_case_types)] // Names are traditional.
pub enum CassErrorCode {
    // deliberately omits CASS_OK
    LIB_BAD_PARAMS,
    LIB_NO_STREAMS,
    LIB_UNABLE_TO_INIT,
    LIB_MESSAGE_ENCODE,
    LIB_HOST_RESOLUTION,
    LIB_UNEXPECTED_RESPONSE,
    LIB_REQUEST_QUEUE_FULL,
    LIB_NO_AVAILABLE_IO_THREAD,
    LIB_WRITE_ERROR,
    LIB_NO_HOSTS_AVAILABLE,
    LIB_INDEX_OUT_OF_BOUNDS,
    LIB_INVALID_ITEM_COUNT,
    LIB_INVALID_VALUE_TYPE,
    LIB_REQUEST_TIMED_OUT,
    LIB_UNABLE_TO_SET_KEYSPACE,
    LIB_CALLBACK_ALREADY_SET,
    LIB_INVALID_STATEMENT_TYPE,
    LIB_NAME_DOES_NOT_EXIST,
    LIB_UNABLE_TO_DETERMINE_PROTOCOL,
    LIB_NULL_VALUE,
    LIB_NOT_IMPLEMENTED,
    LIB_UNABLE_TO_CONNECT,
    LIB_UNABLE_TO_CLOSE,
    LIB_NO_PAGING_STATE,
    LIB_PARAMETER_UNSET,
    LIB_INVALID_ERROR_RESULT_TYPE,
    LIB_INVALID_FUTURE_TYPE,
    LIB_INTERNAL_ERROR,
    LIB_INVALID_CUSTOM_TYPE,
    LIB_INVALID_DATA,
    LIB_NOT_ENOUGH_DATA,
    LIB_INVALID_STATE,
    LIB_NO_CUSTOM_PAYLOAD,
    LIB_EXECUTION_PROFILE_INVALID,
    SERVER_SERVER_ERROR,
    SERVER_PROTOCOL_ERROR,
    SERVER_BAD_CREDENTIALS,
    SERVER_UNAVAILABLE,
    SERVER_OVERLOADED,
    SERVER_IS_BOOTSTRAPPING,
    SERVER_TRUNCATE_ERROR,
    SERVER_WRITE_TIMEOUT,
    SERVER_READ_TIMEOUT,
    SERVER_READ_FAILURE,
    SERVER_FUNCTION_FAILURE,
    SERVER_WRITE_FAILURE,
    SERVER_SYNTAX_ERROR,
    SERVER_UNAUTHORIZED,
    SERVER_INVALID_QUERY,
    SERVER_CONFIG_ERROR,
    SERVER_ALREADY_EXISTS,
    SERVER_UNPREPARED,
    SSL_INVALID_CERT,
    SSL_INVALID_PRIVATE_KEY,
    SSL_NO_PEER_CERT,
    SSL_INVALID_PEER_CERT,
    SSL_IDENTITY_MISMATCH,
    SSL_PROTOCOL_ERROR,
    // deliberately omits LAST_ENTRY
}

enhance_nullary_enum!(CassErrorCode, CassError_, {
    (LIB_BAD_PARAMS, CASS_ERROR_LIB_BAD_PARAMS, "LIB_BAD_PARAMS"),
    (LIB_NO_STREAMS, CASS_ERROR_LIB_NO_STREAMS, "LIB_NO_STREAMS"),
    (LIB_UNABLE_TO_INIT, CASS_ERROR_LIB_UNABLE_TO_INIT, "LIB_UNABLE_TO_INIT"),
    (LIB_MESSAGE_ENCODE, CASS_ERROR_LIB_MESSAGE_ENCODE, "LIB_MESSAGE_ENCODE"),
    (LIB_HOST_RESOLUTION, CASS_ERROR_LIB_HOST_RESOLUTION, "LIB_HOST_RESOLUTION"),
    (LIB_UNEXPECTED_RESPONSE, CASS_ERROR_LIB_UNEXPECTED_RESPONSE, "LIB_UNEXPECTED_RESPONSE"),
    (LIB_REQUEST_QUEUE_FULL, CASS_ERROR_LIB_REQUEST_QUEUE_FULL, "LIB_REQUEST_QUEUE_FULL"),
    (LIB_NO_AVAILABLE_IO_THREAD, CASS_ERROR_LIB_NO_AVAILABLE_IO_THREAD, "LIB_NO_AVAILABLE_IO_THREAD"),
    (LIB_WRITE_ERROR, CASS_ERROR_LIB_WRITE_ERROR, "LIB_WRITE_ERROR"),
    (LIB_NO_HOSTS_AVAILABLE, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, "LIB_NO_HOSTS_AVAILABLE"),
    (LIB_INDEX_OUT_OF_BOUNDS, CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS, "LIB_INDEX_OUT_OF_BOUNDS"),
    (LIB_INVALID_ITEM_COUNT, CASS_ERROR_LIB_INVALID_ITEM_COUNT, "LIB_INVALID_ITEM_COUNT"),
    (LIB_INVALID_VALUE_TYPE, CASS_ERROR_LIB_INVALID_VALUE_TYPE, "LIB_INVALID_VALUE_TYPE"),
    (LIB_REQUEST_TIMED_OUT, CASS_ERROR_LIB_REQUEST_TIMED_OUT, "LIB_REQUEST_TIMED_OUT"),
    (LIB_UNABLE_TO_SET_KEYSPACE, CASS_ERROR_LIB_UNABLE_TO_SET_KEYSPACE, "LIB_UNABLE_TO_SET_KEYSPACE"),
    (LIB_CALLBACK_ALREADY_SET, CASS_ERROR_LIB_CALLBACK_ALREADY_SET, "LIB_CALLBACK_ALREADY_SET"),
    (LIB_INVALID_STATEMENT_TYPE, CASS_ERROR_LIB_INVALID_STATEMENT_TYPE, "LIB_INVALID_STATEMENT_TYPE"),
    (LIB_NAME_DOES_NOT_EXIST, CASS_ERROR_LIB_NAME_DOES_NOT_EXIST, "LIB_NAME_DOES_NOT_EXIST"),
    (LIB_UNABLE_TO_DETERMINE_PROTOCOL, CASS_ERROR_LIB_UNABLE_TO_DETERMINE_PROTOCOL, "LIB_UNABLE_TO_DETERMINE_PROTOCOL"),
    (LIB_NULL_VALUE, CASS_ERROR_LIB_NULL_VALUE, "LIB_NULL_VALUE"),
    (LIB_NOT_IMPLEMENTED, CASS_ERROR_LIB_NOT_IMPLEMENTED, "LIB_NOT_IMPLEMENTED"),
    (LIB_UNABLE_TO_CONNECT, CASS_ERROR_LIB_UNABLE_TO_CONNECT, "LIB_UNABLE_TO_CONNECT"),
    (LIB_UNABLE_TO_CLOSE, CASS_ERROR_LIB_UNABLE_TO_CLOSE, "LIB_UNABLE_TO_CLOSE"),
    (LIB_NO_PAGING_STATE, CASS_ERROR_LIB_NO_PAGING_STATE, "LIB_NO_PAGING_STATE"),
    (LIB_PARAMETER_UNSET, CASS_ERROR_LIB_PARAMETER_UNSET, "LIB_PARAMETER_UNSET"),
    (LIB_INVALID_ERROR_RESULT_TYPE, CASS_ERROR_LIB_INVALID_ERROR_RESULT_TYPE, "LIB_INVALID_ERROR_RESULT_TYPE"),
    (LIB_INVALID_FUTURE_TYPE, CASS_ERROR_LIB_INVALID_FUTURE_TYPE, "LIB_INVALID_FUTURE_TYPE"),
    (LIB_INTERNAL_ERROR, CASS_ERROR_LIB_INTERNAL_ERROR, "LIB_INTERNAL_ERROR"),
    (LIB_INVALID_CUSTOM_TYPE, CASS_ERROR_LIB_INVALID_CUSTOM_TYPE, "LIB_INVALID_CUSTOM_TYPE"),
    (LIB_INVALID_DATA, CASS_ERROR_LIB_INVALID_DATA, "LIB_INVALID_DATA"),
    (LIB_NOT_ENOUGH_DATA, CASS_ERROR_LIB_NOT_ENOUGH_DATA, "LIB_NOT_ENOUGH_DATA"),
    (LIB_INVALID_STATE, CASS_ERROR_LIB_INVALID_STATE, "LIB_INVALID_STATE"),
    (LIB_NO_CUSTOM_PAYLOAD, CASS_ERROR_LIB_NO_CUSTOM_PAYLOAD, "LIB_NO_CUSTOM_PAYLOAD"),
    (LIB_EXECUTION_PROFILE_INVALID, CASS_ERROR_LIB_EXECUTION_PROFILE_INVALID, "LIB_EXECUTION_PROFILE_INVALID"),
    (SERVER_SERVER_ERROR, CASS_ERROR_SERVER_SERVER_ERROR, "SERVER_SERVER_ERROR"),
    (SERVER_PROTOCOL_ERROR, CASS_ERROR_SERVER_PROTOCOL_ERROR, "SERVER_PROTOCOL_ERROR"),
    (SERVER_BAD_CREDENTIALS, CASS_ERROR_SERVER_BAD_CREDENTIALS, "SERVER_BAD_CREDENTIALS"),
    (SERVER_UNAVAILABLE, CASS_ERROR_SERVER_UNAVAILABLE, "SERVER_UNAVAILABLE"),
    (SERVER_OVERLOADED, CASS_ERROR_SERVER_OVERLOADED, "SERVER_OVERLOADED"),
    (SERVER_IS_BOOTSTRAPPING, CASS_ERROR_SERVER_IS_BOOTSTRAPPING, "SERVER_IS_BOOTSTRAPPING"),
    (SERVER_TRUNCATE_ERROR, CASS_ERROR_SERVER_TRUNCATE_ERROR, "SERVER_TRUNCATE_ERROR"),
    (SERVER_WRITE_TIMEOUT, CASS_ERROR_SERVER_WRITE_TIMEOUT, "SERVER_WRITE_TIMEOUT"),
    (SERVER_READ_TIMEOUT, CASS_ERROR_SERVER_READ_TIMEOUT, "SERVER_READ_TIMEOUT"),
    (SERVER_READ_FAILURE, CASS_ERROR_SERVER_READ_FAILURE, "SERVER_READ_FAILURE"),
    (SERVER_FUNCTION_FAILURE, CASS_ERROR_SERVER_FUNCTION_FAILURE, "SERVER_FUNCTION_FAILURE"),
    (SERVER_WRITE_FAILURE, CASS_ERROR_SERVER_WRITE_FAILURE, "SERVER_WRITE_FAILURE"),
    (SERVER_SYNTAX_ERROR, CASS_ERROR_SERVER_SYNTAX_ERROR, "SERVER_SYNTAX_ERROR"),
    (SERVER_UNAUTHORIZED, CASS_ERROR_SERVER_UNAUTHORIZED, "SERVER_UNAUTHORIZED"),
    (SERVER_INVALID_QUERY, CASS_ERROR_SERVER_INVALID_QUERY, "SERVER_INVALID_QUERY"),
    (SERVER_CONFIG_ERROR, CASS_ERROR_SERVER_CONFIG_ERROR, "SERVER_CONFIG_ERROR"),
    (SERVER_ALREADY_EXISTS, CASS_ERROR_SERVER_ALREADY_EXISTS, "SERVER_ALREADY_EXISTS"),
    (SERVER_UNPREPARED, CASS_ERROR_SERVER_UNPREPARED, "SERVER_UNPREPARED"),
    (SSL_INVALID_CERT, CASS_ERROR_SSL_INVALID_CERT, "SSL_INVALID_CERT"),
    (SSL_INVALID_PRIVATE_KEY, CASS_ERROR_SSL_INVALID_PRIVATE_KEY, "SSL_INVALID_PRIVATE_KEY"),
    (SSL_NO_PEER_CERT, CASS_ERROR_SSL_NO_PEER_CERT, "SSL_NO_PEER_CERT"),
    (SSL_INVALID_PEER_CERT, CASS_ERROR_SSL_INVALID_PEER_CERT, "SSL_INVALID_PEER_CERT"),
    (SSL_IDENTITY_MISMATCH, CASS_ERROR_SSL_IDENTITY_MISMATCH, "SSL_IDENTITY_MISMATCH"),
    (SSL_PROTOCOL_ERROR, CASS_ERROR_SSL_PROTOCOL_ERROR, "SSL_PROTOCOL_ERROR"),
}, omit { CASS_OK, CASS_ERROR_LAST_ENTRY });

/// Extract an optional C string lossily (i.e., using a replacement char for non-UTF-8 sequences).
pub(crate) unsafe fn get_lossy_string<F>(get: F) -> Option<String>
where
    F: Fn(*mut *const ::std::os::raw::c_char, *mut usize) -> CassError_,
{
    let mut msg = mem::zeroed();
    let mut msg_len = mem::zeroed();
    match (get)(&mut msg, &mut msg_len) {
        CASS_OK => (),
        _ => return None,
    }
    let slice = slice::from_raw_parts(msg as *const u8, msg_len as usize);
    Some(String::from_utf8_lossy(slice).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_conversion() {
        assert_eq!(
            CassErrorCode::build(CassError_::CASS_ERROR_SERVER_PROTOCOL_ERROR),
            CassErrorCode::SERVER_PROTOCOL_ERROR
        );
        match CassErrorCode::LIB_INVALID_DATA.inner() {
            CassError_::CASS_ERROR_LIB_INVALID_DATA => (),
            e => panic!("Unexpected return value {:?}", e),
        }
    }

    /// Test the enhance_nullary_enum! macro `omit` functionality works correctly.
    #[test]
    #[should_panic(expected = "Unexpected variant CassError_ :: CASS_OK")]
    pub fn test_omitted_conversion_should_fail() {
        CassErrorCode::build(CassError_::CASS_OK);
    }
}
