use crate::cassandra::collection::{List, Map, Set};
use crate::cassandra::data_type::ConstDataType;
use crate::cassandra::error::*;
use crate::cassandra::inet::Inet;
use crate::cassandra::tuple::Tuple;
use crate::cassandra::util::{Protected, ProtectedInner};
use crate::cassandra::uuid::Uuid;

use crate::cassandra_sys::cass_false;
use crate::cassandra_sys::cass_true;
use crate::cassandra_sys::cass_user_type_data_type;
use crate::cassandra_sys::cass_user_type_free;
use crate::cassandra_sys::cass_user_type_set_bool;
use crate::cassandra_sys::cass_user_type_set_bool_by_name_n;
use crate::cassandra_sys::cass_user_type_set_bytes;
use crate::cassandra_sys::cass_user_type_set_bytes_by_name_n;
use crate::cassandra_sys::cass_user_type_set_collection;
use crate::cassandra_sys::cass_user_type_set_collection_by_name_n;
use crate::cassandra_sys::cass_user_type_set_decimal;
use crate::cassandra_sys::cass_user_type_set_decimal_by_name_n;
use crate::cassandra_sys::cass_user_type_set_double;
use crate::cassandra_sys::cass_user_type_set_double_by_name_n;
use crate::cassandra_sys::cass_user_type_set_float;
use crate::cassandra_sys::cass_user_type_set_float_by_name_n;
use crate::cassandra_sys::cass_user_type_set_inet;
use crate::cassandra_sys::cass_user_type_set_inet_by_name_n;
use crate::cassandra_sys::cass_user_type_set_int16;
use crate::cassandra_sys::cass_user_type_set_int16_by_name_n;
use crate::cassandra_sys::cass_user_type_set_int32;
use crate::cassandra_sys::cass_user_type_set_int32_by_name_n;
use crate::cassandra_sys::cass_user_type_set_int64;
use crate::cassandra_sys::cass_user_type_set_int64_by_name_n;
use crate::cassandra_sys::cass_user_type_set_int8;
use crate::cassandra_sys::cass_user_type_set_int8_by_name_n;
use crate::cassandra_sys::cass_user_type_set_null;
use crate::cassandra_sys::cass_user_type_set_null_by_name_n;
use crate::cassandra_sys::cass_user_type_set_string_by_name_n;
use crate::cassandra_sys::cass_user_type_set_string_n;
use crate::cassandra_sys::cass_user_type_set_tuple;
use crate::cassandra_sys::cass_user_type_set_tuple_by_name_n;
use crate::cassandra_sys::cass_user_type_set_uint32;
use crate::cassandra_sys::cass_user_type_set_uint32_by_name_n;
use crate::cassandra_sys::cass_user_type_set_user_type;
use crate::cassandra_sys::cass_user_type_set_user_type_by_name_n;
use crate::cassandra_sys::cass_user_type_set_uuid;
use crate::cassandra_sys::cass_user_type_set_uuid_by_name_n;
use crate::cassandra_sys::CassUserType as _UserType;

use std::os::raw::c_char;
// use cassandra::iterator::FieldIterator;

/// A user defined type
#[derive(Debug)]
pub struct UserType(*mut _UserType);

// The underlying C type has no thread-local state, but does not support access
// from multiple threads: https://datastax.github.io/cpp-driver/topics/#thread-safety
unsafe impl Send for UserType {}

impl ProtectedInner<*mut _UserType> for UserType {
    fn inner(&self) -> *mut _UserType {
        self.0
    }
}

impl Protected<*mut _UserType> for UserType {
    fn build(inner: *mut _UserType) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        UserType(inner)
    }
}

impl Drop for UserType {
    fn drop(&mut self) {
        unsafe { cass_user_type_free(self.0) }
    }
}

// impl Drop for UserType {
//    fn drop(&mut self) {unsafe{
//        cass_user_type_free(self.0)
//    }}
// }

impl UserType {
    /// Gets the data type of a user defined type.
    pub fn data_type(&self) -> ConstDataType {
        unsafe { ConstDataType::build(cass_user_type_data_type(self.0)) }
    }

    /// Sets a null in a user defined type at the specified index.
    pub fn set_null(&mut self, index: usize) -> Result<&mut Self> {
        unsafe { cass_user_type_set_null(self.0, index).to_result(self) }
    }

    /// Sets a null in a user defined type at the specified name.
    pub fn set_null_by_name<S>(&mut self, name: S) -> Result<&mut Self>
    where
        S: Into<String>,
    {
        unsafe {
            let name_str = name.into();
            let name_ptr = name_str.as_ptr() as *const c_char;
            cass_user_type_set_null_by_name_n(self.0, name_ptr, name_str.len()).to_result(self)
        }
    }

    /// Sets a "tinyint" in a user defined type at the specified index.
    pub fn set_int8(&mut self, index: usize, value: i8) -> Result<&mut Self> {
        unsafe { cass_user_type_set_int8(self.0, index, value).to_result(self) }
    }

    /// Sets a "tinyint" in a user defined type at the specified name.
    pub fn set_int8_by_name<S>(&mut self, name: S, value: i8) -> Result<&mut Self>
    where
        S: Into<String>,
    {
        unsafe {
            let name_str = name.into();
            let name_ptr = name_str.as_ptr() as *const c_char;
            cass_user_type_set_int8_by_name_n(self.0, name_ptr, name_str.len(), value)
                .to_result(self)
        }
    }

    /// Sets an "smallint" in a user defined type at the specified index.
    pub fn set_int16(&mut self, index: usize, value: i16) -> Result<&mut Self> {
        unsafe { cass_user_type_set_int16(self.0, index, value).to_result(self) }
    }

    /// Sets an "smallint" in a user defined type at the specified name.
    pub fn set_int16_by_name<S>(&mut self, name: S, value: i16) -> Result<&mut Self>
    where
        S: Into<String>,
    {
        unsafe {
            let name_str = name.into();
            let name_ptr = name_str.as_ptr() as *const c_char;
            cass_user_type_set_int16_by_name_n(self.0, name_ptr, name_str.len(), value)
                .to_result(self)
        }
    }

    /// Sets an "int" in a user defined type at the specified index.
    pub fn set_int32(&mut self, index: usize, value: i32) -> Result<&mut Self> {
        unsafe { cass_user_type_set_int32(self.0, index, value).to_result(self) }
    }

    /// Sets an "int" in a user defined type at the specified name.
    pub fn set_int32_by_name<S>(&mut self, name: S, value: i32) -> Result<&mut Self>
    where
        S: Into<String>,
    {
        unsafe {
            let name_str = name.into();
            let name_ptr = name_str.as_ptr() as *const c_char;
            cass_user_type_set_int32_by_name_n(self.0, name_ptr, name_str.len(), value)
                .to_result(self)
        }
    }

    /// Sets a "date" in a user defined type at the specified index.
    pub fn set_uint32(&mut self, index: usize, value: u32) -> Result<&mut Self> {
        unsafe { cass_user_type_set_uint32(self.0, index, value).to_result(self) }
    }

    /// Sets a "date" in a user defined type at the specified name.
    pub fn set_uint32_by_name<S>(&mut self, name: S, value: u32) -> Result<&mut Self>
    where
        S: Into<String>,
    {
        unsafe {
            let name_str = name.into();
            let name_ptr = name_str.as_ptr() as *const c_char;
            cass_user_type_set_uint32_by_name_n(self.0, name_ptr, name_str.len(), value)
                .to_result(self)
        }
    }

    /// Sets an "bigint", "counter", "timestamp" or "time" in a
    /// user defined type at the specified index.
    pub fn set_int64(&mut self, index: usize, value: i64) -> Result<&mut Self> {
        unsafe { cass_user_type_set_int64(self.0, index, value).to_result(self) }
    }

    /// Sets an "bigint", "counter", "timestamp" or "time" in a
    /// user defined type at the specified name.
    pub fn set_int64_by_name<S>(&mut self, name: S, value: i64) -> Result<&mut Self>
    where
        S: Into<String>,
    {
        unsafe {
            let name_str = name.into();
            let name_ptr = name_str.as_ptr() as *const c_char;
            cass_user_type_set_int64_by_name_n(self.0, name_ptr, name_str.len(), value)
                .to_result(self)
        }
    }

    /// Sets a "float" in a user defined type at the specified index.
    pub fn set_float(&mut self, index: usize, value: f32) -> Result<&mut Self> {
        unsafe { cass_user_type_set_float(self.0, index, value).to_result(self) }
    }

    /// Sets a "float" in a user defined type at the specified name.
    pub fn set_float_by_name<S>(&mut self, name: S, value: f32) -> Result<&mut Self>
    where
        S: Into<String>,
    {
        unsafe {
            let name_str = name.into();
            let name_ptr = name_str.as_ptr() as *const c_char;
            cass_user_type_set_float_by_name_n(self.0, name_ptr, name_str.len(), value)
                .to_result(self)
        }
    }

    /// Sets an "double" in a user defined type at the specified index.
    pub fn set_double(&mut self, index: usize, value: f64) -> Result<&mut Self> {
        unsafe { cass_user_type_set_double(self.0, index, value).to_result(self) }
    }

    /// Sets an "double" in a user defined type at the specified name.

    pub fn set_double_by_name<S>(&mut self, name: S, value: f64) -> Result<&mut Self>
    where
        S: Into<String>,
    {
        unsafe {
            let name_str = name.into();
            let name_ptr = name_str.as_ptr() as *const c_char;
            cass_user_type_set_double_by_name_n(self.0, name_ptr, name_str.len(), value)
                .to_result(self)
        }
    }

    /// Sets a "boolean" in a user defined type at the specified index.
    pub fn set_bool(&mut self, index: usize, value: bool) -> Result<&mut Self> {
        unsafe {
            cass_user_type_set_bool(self.0, index, if value { cass_true } else { cass_false })
                .to_result(self)
        }
    }

    /// Sets a "boolean" in a user defined type at the specified name.
    pub fn set_bool_by_name<S>(&mut self, name: S, value: bool) -> Result<&mut Self>
    where
        S: Into<String>,
    {
        unsafe {
            let name_str = name.into();
            let name_ptr = name_str.as_ptr() as *const c_char;
            cass_user_type_set_bool_by_name_n(
                self.0,
                name_ptr,
                name_str.len(),
                if value { cass_true } else { cass_false },
            )
            .to_result(self)
        }
    }

    /// Sets an "ascii", "text" or "varchar" in a user defined type at the
    /// specified index.
    pub fn set_stringl<S>(&mut self, index: usize, value: S) -> Result<&mut Self>
    where
        S: Into<String>,
    {
        unsafe {
            let value_str = value.into();
            let value_ptr = value_str.as_ptr() as *const c_char;
            cass_user_type_set_string_n(self.0, index, value_ptr, value_str.len()).to_result(self)
        }
    }

    /// Sets an "ascii", "text" or "varchar" in a user defined type at the
    /// specified name.
    pub fn set_string_by_name<S>(&mut self, name: S, value: S) -> Result<&mut Self>
    where
        S: Into<String>,
    {
        unsafe {
            let name_str = name.into();
            let name_ptr = name_str.as_ptr() as *const c_char;
            let value_str = value.into();
            let value_ptr = value_str.as_ptr() as *const c_char;
            cass_user_type_set_string_by_name_n(
                self.0,
                name_ptr,
                name_str.len(),
                value_ptr,
                value_str.len(),
            )
            .to_result(self)
        }
    }

    // FIXME. right way to pass the vec?
    /// Sets a "blob" "varint" or "custom" in a user defined type at the specified index.
    pub fn set_bytes(&mut self, index: usize, value: Vec<u8>) -> Result<&mut Self> {
        unsafe {
            cass_user_type_set_bytes(self.0, index, value.as_ptr(), value.len()).to_result(self)
        }
    }

    /// Sets a "blob", "varint" or "custom" in a user defined type at the specified name.
    pub fn set_bytes_by_name<S>(&mut self, name: S, value: Vec<u8>) -> Result<&mut Self>
    where
        S: Into<String>,
    {
        unsafe {
            let name_str = name.into();
            let name_ptr = name_str.as_ptr() as *const c_char;
            cass_user_type_set_bytes_by_name_n(
                self.0,
                name_ptr,
                name_str.len(),
                value.as_ptr(),
                value.len(),
            )
            .to_result(self)
        }
    }

    /// Sets a "uuid" or "timeuuid" in a user defined type at the specified index.
    pub fn set_uuid<S>(&mut self, index: usize, value: S) -> Result<&mut Self>
    where
        S: Into<Uuid>,
    {
        unsafe { cass_user_type_set_uuid(self.0, index, value.into().inner()).to_result(self) }
    }

    /// Sets a "uuid" or "timeuuid" in a user defined type at the specified name.
    pub fn set_uuid_by_name<S, U>(&mut self, name: S, value: U) -> Result<&mut Self>
    where
        S: Into<String>,
        U: Into<Uuid>,
    {
        unsafe {
            let name_str = name.into();
            let name_ptr = name_str.as_ptr() as *const c_char;
            cass_user_type_set_uuid_by_name_n(
                self.0,
                name_ptr,
                name_str.len(),
                value.into().inner(),
            )
            .to_result(self)
        }
    }

    /// Sets a "inet" in a user defined type at the specified index.
    pub fn set_inet<S>(&mut self, index: usize, value: S) -> Result<&mut Self>
    where
        S: Into<Inet>,
    {
        unsafe { cass_user_type_set_inet(self.0, index, value.into().inner()).to_result(self) }
    }

    /// Sets a "inet" in a user defined type at the specified name.
    pub fn set_inet_by_name<S, U>(&mut self, name: S, value: U) -> Result<&mut Self>
    where
        S: Into<String>,
        U: Into<Inet>,
    {
        unsafe {
            let name_str = name.into();
            let name_ptr = name_str.as_ptr() as *const c_char;
            cass_user_type_set_inet_by_name_n(
                self.0,
                name_ptr,
                name_str.len(),
                value.into().inner(),
            )
            .to_result(self)
        }
    }

    /// Sets a list in a user defined type at the specified index.
    pub fn set_list<S>(&mut self, index: usize, value: S) -> Result<&mut Self>
    where
        S: Into<List>,
    {
        unsafe {
            cass_user_type_set_collection(self.0, index, value.into().inner()).to_result(self)
        }
    }

    /// Sets a list in a user defined type at the specified name.
    pub fn set_list_by_name<S, V>(&mut self, name: S, value: V) -> Result<&mut Self>
    where
        S: Into<String>,
        V: Into<List>,
    {
        unsafe {
            let name_str = name.into();
            let name_ptr = name_str.as_ptr() as *const c_char;
            cass_user_type_set_collection_by_name_n(
                self.0,
                name_ptr,
                name_str.len(),
                value.into().inner(),
            )
            .to_result(self)
        }
    }

    /// Sets a map in a user defined type at the specified index.
    pub fn set_map<S>(&mut self, index: usize, value: S) -> Result<&mut Self>
    where
        S: Into<Map>,
    {
        unsafe {
            cass_user_type_set_collection(self.0, index, value.into().inner()).to_result(self)
        }
    }

    /// Sets a map in a user defined type at the specified name.
    pub fn set_map_by_name<S, V>(&mut self, name: S, value: V) -> Result<&mut Self>
    where
        S: Into<String>,
        V: Into<Map>,
    {
        unsafe {
            let name_str = name.into();
            let name_ptr = name_str.as_ptr() as *const c_char;
            cass_user_type_set_collection_by_name_n(
                self.0,
                name_ptr,
                name_str.len(),
                value.into().inner(),
            )
            .to_result(self)
        }
    }

    /// Sets a "set" in a user defined type at the specified index.
    pub fn set_set<S>(&mut self, index: usize, value: S) -> Result<&mut Self>
    where
        S: Into<Set>,
    {
        unsafe {
            cass_user_type_set_collection(self.0, index, value.into().inner()).to_result(self)
        }
    }

    /// Sets a "set" in a user defined type at the specified name.
    pub fn set_set_by_name<S, V>(&mut self, name: S, value: V) -> Result<&mut Self>
    where
        S: Into<String>,
        V: Into<Set>,
    {
        unsafe {
            let name_str = name.into();
            let name_ptr = name_str.as_ptr() as *const c_char;
            cass_user_type_set_collection_by_name_n(
                self.0,
                name_ptr,
                name_str.len(),
                value.into().inner(),
            )
            .to_result(self)
        }
    }

    /// Sets a "tuple" in a user defined type at the specified index.
    pub fn set_tuple(&mut self, index: usize, value: Tuple) -> Result<&mut Self> {
        unsafe { cass_user_type_set_tuple(self.0, index, value.inner()).to_result(self) }
    }

    /// Sets a "tuple" in a user defined type at the specified name.
    pub fn set_tuple_by_name<S>(&mut self, name: S, value: Tuple) -> Result<&mut Self>
    where
        S: Into<String>,
    {
        unsafe {
            let name_str = name.into();
            let name_ptr = name_str.as_ptr() as *const c_char;
            cass_user_type_set_tuple_by_name_n(self.0, name_ptr, name_str.len(), value.inner())
                .to_result(self)
        }
    }

    /// Sets a user defined type in a user defined type at the specified index.
    pub fn set_user_type(&mut self, index: usize, value: UserType) -> Result<&mut Self> {
        unsafe { cass_user_type_set_user_type(self.0, index, value.0).to_result(self) }
    }

    /// Sets a user defined type in a user defined type at the specified name.
    pub fn set_user_type_by_name<S>(&mut self, name: S, value: UserType) -> Result<&mut Self>
    where
        S: Into<String>,
    {
        unsafe {
            let name_str = name.into();
            let name_ptr = name_str.as_ptr() as *const c_char;
            cass_user_type_set_user_type_by_name_n(self.0, name_ptr, name_str.len(), value.0)
                .to_result(self)
        }
    }
}
