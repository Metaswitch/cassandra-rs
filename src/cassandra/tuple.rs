use crate::cassandra::collection::{List, Map, Set};
use crate::cassandra::data_type::ConstDataType;
use crate::cassandra::data_type::DataType;
use crate::cassandra::error::*;
use crate::cassandra::inet::Inet;
use crate::cassandra::user_type::UserType;
use crate::cassandra::util::{Protected, ProtectedInner};
use crate::cassandra::uuid::Uuid;

use crate::cassandra_sys::cass_false;
use crate::cassandra_sys::cass_true;
use crate::cassandra_sys::cass_tuple_data_type;
use crate::cassandra_sys::cass_tuple_free;
use crate::cassandra_sys::cass_tuple_new;
use crate::cassandra_sys::cass_tuple_new_from_data_type;
use crate::cassandra_sys::cass_tuple_set_bool;
use crate::cassandra_sys::cass_tuple_set_bytes;
use crate::cassandra_sys::cass_tuple_set_collection;
use crate::cassandra_sys::cass_tuple_set_decimal;
use crate::cassandra_sys::cass_tuple_set_double;
use crate::cassandra_sys::cass_tuple_set_float;
use crate::cassandra_sys::cass_tuple_set_inet;
use crate::cassandra_sys::cass_tuple_set_int16;
use crate::cassandra_sys::cass_tuple_set_int32;
use crate::cassandra_sys::cass_tuple_set_int64;
use crate::cassandra_sys::cass_tuple_set_int8;
use crate::cassandra_sys::cass_tuple_set_null;
use crate::cassandra_sys::cass_tuple_set_string_n;
use crate::cassandra_sys::cass_tuple_set_tuple;
use crate::cassandra_sys::cass_tuple_set_uint32;
use crate::cassandra_sys::cass_tuple_set_user_type;
use crate::cassandra_sys::cass_tuple_set_uuid;
use crate::cassandra_sys::CassTuple as _Tuple;

use std::net::IpAddr;
use std::os::raw::c_char;

/// A tuple of values.
#[derive(Debug)]
pub struct Tuple(*mut _Tuple);

// The underlying C type has no thread-local state, but does not support access
// from multiple threads: https://datastax.github.io/cpp-driver/topics/#thread-safety
unsafe impl Send for Tuple {}

impl ProtectedInner<*mut _Tuple> for Tuple {
    fn inner(&self) -> *mut _Tuple {
        self.0
    }
}

impl Protected<*mut _Tuple> for Tuple {
    fn build(inner: *mut _Tuple) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        Tuple(inner)
    }
}

impl Tuple {
    /// Creates a new tuple.
    pub fn new(item_count: usize) -> Self {
        unsafe { Tuple(cass_tuple_new(item_count)) }
    }

    /// Creates a new tuple from an existing data type.
    pub fn new_from_data_type(data_type: DataType) -> Tuple {
        unsafe { Tuple(cass_tuple_new_from_data_type(data_type.inner())) }
    }

    /// Gets the data type of a tuple.
    pub fn data_type(&mut self) -> ConstDataType {
        unsafe { ConstDataType::build(cass_tuple_data_type(self.0)) }
    }

    /// Sets an null in a tuple at the specified index.
    pub fn set_null(&mut self, index: usize) -> Result<&mut Self> {
        unsafe { cass_tuple_set_null(self.0, index).to_result(self) }
    }

    /// Sets a "tinyint" in a tuple at the specified index.
    pub fn set_int8(&mut self, index: usize, value: i8) -> Result<&mut Self> {
        unsafe { cass_tuple_set_int8(self.0, index, value).to_result(self) }
    }

    /// Sets an "smallint" in a tuple at the specified index.
    pub fn set_int16(&mut self, index: usize, value: i16) -> Result<&mut Self> {
        unsafe { cass_tuple_set_int16(self.0, index, value).to_result(self) }
    }

    /// Sets an "int" in a tuple at the specified index.
    pub fn set_int32(&mut self, index: usize, value: i32) -> Result<&mut Self> {
        unsafe { cass_tuple_set_int32(self.0, index, value).to_result(self) }
    }

    /// Sets a "date" in a tuple at the specified index.
    pub fn set_uint32(&mut self, index: usize, value: u32) -> Result<&mut Self> {
        unsafe { cass_tuple_set_uint32(self.0, index, value).to_result(self) }
    }

    /// Sets a "bigint", "counter", "timestamp" or "time" in a tuple at the
    /// specified index.
    pub fn set_int64(&mut self, index: usize, value: i64) -> Result<&mut Self> {
        unsafe { cass_tuple_set_int64(self.0, index, value).to_result(self) }
    }

    /// Sets a "float" in a tuple at the specified index.
    pub fn set_float(&mut self, index: usize, value: f32) -> Result<&mut Self> {
        unsafe { cass_tuple_set_float(self.0, index, value).to_result(self) }
    }

    /// Sets a "double" in a tuple at the specified index.
    pub fn set_double(&mut self, index: usize, value: f64) -> Result<&mut Self> {
        unsafe { cass_tuple_set_double(self.0, index, value).to_result(self) }
    }

    /// Sets a "boolean" in a tuple at the specified index.
    pub fn set_bool(&mut self, index: usize, value: bool) -> Result<&mut Self> {
        unsafe {
            cass_tuple_set_bool(self.0, index, if value { cass_true } else { cass_false })
                .to_result(self)
        }
    }

    /// Sets an "ascii", "text" or "varchar" in a tuple at the specified index.
    pub fn set_string<S>(&mut self, index: usize, value: S) -> Result<&mut Self>
    where
        S: Into<String>,
    {
        unsafe {
            let value_str = value.into();
            let value_ptr = value_str.as_ptr() as *const c_char;
            cass_tuple_set_string_n(self.0, index, value_ptr, value_str.len()).to_result(self)
        }
    }

    /// Sets a "blob", "varint" or "custom" in a tuple at the specified index.
    pub fn set_bytes(&mut self, index: usize, value: Vec<u8>) -> Result<&mut Self> {
        unsafe { cass_tuple_set_bytes(self.0, index, value.as_ptr(), value.len()).to_result(self) }
    }

    /// Sets a "uuid" or "timeuuid" in a tuple at the specified index.
    pub fn set_uuid<S>(&mut self, index: usize, value: S) -> Result<&mut Self>
    where
        S: Into<Uuid>,
    {
        unsafe { cass_tuple_set_uuid(self.0, index, value.into().inner()).to_result(self) }
    }

    /// Sets an "inet" in a tuple at the specified index.
    pub fn set_inet(&mut self, index: usize, value: IpAddr) -> Result<&mut Self> {
        let inet = Inet::from(&value);
        unsafe { cass_tuple_set_inet(self.0, index, inet.inner()).to_result(self) }
    }

    /// Sets a list in a tuple at the specified index.
    pub fn set_list<S>(&mut self, index: usize, value: S) -> Result<&mut Self>
    where
        S: Into<List>,
    {
        unsafe { cass_tuple_set_collection(self.0, index, value.into().inner()).to_result(self) }
    }

    /// Sets a map in a tuple at the specified index.
    pub fn set_map<S>(&mut self, index: usize, value: S) -> Result<&mut Self>
    where
        S: Into<Map>,
    {
        unsafe { cass_tuple_set_collection(self.0, index, value.into().inner()).to_result(self) }
    }

    /// Sets a set" in a tuple at the specified index.
    pub fn set_set<S>(&mut self, index: usize, value: S) -> Result<&mut Self>
    where
        S: Into<Set>,
    {
        unsafe { cass_tuple_set_collection(self.0, index, value.into().inner()).to_result(self) }
    }

    /// Sets a "tuple" in a tuple at the specified index.
    pub fn set_tuple(&mut self, index: usize, value: Tuple) -> Result<&mut Self> {
        unsafe { cass_tuple_set_tuple(self.0, index, value.0).to_result(self) }
    }

    /// Sets a "udt" in a tuple at the specified index.
    pub fn set_user_type(&mut self, index: usize, value: &UserType) -> Result<&mut Self> {
        unsafe { cass_tuple_set_user_type(self.0, index, value.inner()).to_result(self) }
    }
}

impl Drop for Tuple {
    fn drop(&mut self) {
        unsafe { cass_tuple_free(self.0) }
    }
}
