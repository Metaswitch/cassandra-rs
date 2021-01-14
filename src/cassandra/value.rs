use crate::cassandra::data_type::ConstDataType;
use crate::cassandra::error::*;
use crate::cassandra::inet::Inet;
use crate::cassandra::iterator::MapIterator;
use crate::cassandra::iterator::SetIterator;
use crate::cassandra::iterator::UserTypeIterator;
use crate::cassandra::util::{Protected, ProtectedInner};
use crate::cassandra::uuid::Uuid;

use crate::cassandra_sys::cass_collection_append_decimal;
use crate::cassandra_sys::cass_iterator_fields_from_user_type;
use crate::cassandra_sys::cass_iterator_from_collection;
use crate::cassandra_sys::cass_iterator_from_map;
use crate::cassandra_sys::cass_true;
use crate::cassandra_sys::cass_value_data_type;
use crate::cassandra_sys::cass_value_get_bool;
use crate::cassandra_sys::cass_value_get_bytes;
use crate::cassandra_sys::cass_value_get_double;
use crate::cassandra_sys::cass_value_get_float;
use crate::cassandra_sys::cass_value_get_inet;
use crate::cassandra_sys::cass_value_get_int16;
use crate::cassandra_sys::cass_value_get_int32;
use crate::cassandra_sys::cass_value_get_int64;
use crate::cassandra_sys::cass_value_get_int8;
use crate::cassandra_sys::cass_value_get_string;
use crate::cassandra_sys::cass_value_get_uint32;
use crate::cassandra_sys::cass_value_get_uuid;
use crate::cassandra_sys::cass_value_is_collection;
use crate::cassandra_sys::cass_value_is_null;
use crate::cassandra_sys::cass_value_item_count;
use crate::cassandra_sys::cass_value_primary_sub_type;
use crate::cassandra_sys::cass_value_secondary_sub_type;
use crate::cassandra_sys::cass_value_type;
use crate::cassandra_sys::CassValue as _CassValue;
use crate::cassandra_sys::CassValueType_;
use crate::cassandra_sys::CASS_ERROR_LIB_INVALID_VALUE_TYPE;
use crate::cassandra_sys::CASS_ERROR_LIB_NULL_VALUE;
use crate::cassandra_sys::CASS_VALUE_TYPE_ASCII;
use crate::cassandra_sys::CASS_VALUE_TYPE_BIGINT;
use crate::cassandra_sys::CASS_VALUE_TYPE_BLOB;
use crate::cassandra_sys::CASS_VALUE_TYPE_BOOLEAN;
use crate::cassandra_sys::CASS_VALUE_TYPE_COUNTER;
use crate::cassandra_sys::CASS_VALUE_TYPE_CUSTOM;
use crate::cassandra_sys::CASS_VALUE_TYPE_DATE;
use crate::cassandra_sys::CASS_VALUE_TYPE_DECIMAL;
use crate::cassandra_sys::CASS_VALUE_TYPE_DOUBLE;
use crate::cassandra_sys::CASS_VALUE_TYPE_DURATION;
use crate::cassandra_sys::CASS_VALUE_TYPE_FLOAT;
use crate::cassandra_sys::CASS_VALUE_TYPE_INET;
use crate::cassandra_sys::CASS_VALUE_TYPE_INT;
use crate::cassandra_sys::CASS_VALUE_TYPE_LAST_ENTRY;
use crate::cassandra_sys::CASS_VALUE_TYPE_LIST;
use crate::cassandra_sys::CASS_VALUE_TYPE_MAP;
use crate::cassandra_sys::CASS_VALUE_TYPE_SET;
use crate::cassandra_sys::CASS_VALUE_TYPE_SMALL_INT;
use crate::cassandra_sys::CASS_VALUE_TYPE_TEXT;
use crate::cassandra_sys::CASS_VALUE_TYPE_TIME;
use crate::cassandra_sys::CASS_VALUE_TYPE_TIMESTAMP;
use crate::cassandra_sys::CASS_VALUE_TYPE_TIMEUUID;
use crate::cassandra_sys::CASS_VALUE_TYPE_TINY_INT;
use crate::cassandra_sys::CASS_VALUE_TYPE_TUPLE;
use crate::cassandra_sys::CASS_VALUE_TYPE_UDT;
use crate::cassandra_sys::CASS_VALUE_TYPE_UNKNOWN;
use crate::cassandra_sys::CASS_VALUE_TYPE_UUID;
use crate::cassandra_sys::CASS_VALUE_TYPE_VARCHAR;
use crate::cassandra_sys::CASS_VALUE_TYPE_VARINT;

use std::ffi::CString;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::mem;
use std::ptr;
use std::slice;
use std::str;

/// The type of a Cassandra value.
#[derive(Debug, Eq, PartialEq, Copy, Clone, Hash)]
#[allow(missing_docs)] // Meanings are defined in CQL documentation.
#[allow(non_camel_case_types)] // Names are traditional.
pub enum ValueType {
    UNKNOWN,
    CUSTOM,
    ASCII,
    BIGINT,
    BLOB,
    BOOLEAN,
    COUNTER,
    DECIMAL,
    DOUBLE,
    FLOAT,
    INT,
    TEXT,
    TIMESTAMP,
    UUID,
    VARCHAR,
    VARINT,
    TIMEUUID,
    INET,
    DATE,
    TIME,
    SMALL_INT,
    TINY_INT,
    DURATION,
    LIST,
    MAP,
    SET,
    UDT,
    TUPLE,
}

enhance_nullary_enum!(ValueType, CassValueType_, {
    (UNKNOWN, CASS_VALUE_TYPE_UNKNOWN, "UNKNOWN"),
    (CUSTOM, CASS_VALUE_TYPE_CUSTOM, "CUSTOM"),
    (ASCII, CASS_VALUE_TYPE_ASCII, "ASCII"),
    (BIGINT, CASS_VALUE_TYPE_BIGINT, "BIGINT"),
    (BLOB, CASS_VALUE_TYPE_BLOB, "BLOB"),
    (BOOLEAN, CASS_VALUE_TYPE_BOOLEAN, "BOOLEAN"),
    (COUNTER, CASS_VALUE_TYPE_COUNTER, "COUNTER"),
    (DECIMAL, CASS_VALUE_TYPE_DECIMAL, "DECIMAL"),
    (DOUBLE, CASS_VALUE_TYPE_DOUBLE, "DOUBLE"),
    (FLOAT, CASS_VALUE_TYPE_FLOAT, "FLOAT"),
    (INT, CASS_VALUE_TYPE_INT, "INT"),
    (TEXT, CASS_VALUE_TYPE_TEXT, "TEXT"),
    (TIMESTAMP, CASS_VALUE_TYPE_TIMESTAMP, "TIMESTAMP"),
    (UUID, CASS_VALUE_TYPE_UUID, "UUID"),
    (VARCHAR, CASS_VALUE_TYPE_VARCHAR, "VARCHAR"),
    (VARINT, CASS_VALUE_TYPE_VARINT, "VARINT"),
    (TIMEUUID, CASS_VALUE_TYPE_TIMEUUID, "TIMEUUID"),
    (INET, CASS_VALUE_TYPE_INET, "INET"),
    (DATE, CASS_VALUE_TYPE_DATE, "DATE"),
    (TIME, CASS_VALUE_TYPE_TIME, "TIME"),
    (SMALL_INT, CASS_VALUE_TYPE_SMALL_INT, "SMALL_INT"),
    (TINY_INT, CASS_VALUE_TYPE_TINY_INT, "TINY_INT"),
    (DURATION, CASS_VALUE_TYPE_DURATION, "DURATION"),
    (LIST, CASS_VALUE_TYPE_LIST, "LIST"),
    (MAP, CASS_VALUE_TYPE_MAP, "MAP"),
    (SET, CASS_VALUE_TYPE_SET, "SET"),
    (UDT, CASS_VALUE_TYPE_UDT, "UDT"),
    (TUPLE, CASS_VALUE_TYPE_TUPLE, "TUPLE"),
}, omit { CASS_VALUE_TYPE_LAST_ENTRY });

/// A single primitive value or a collection of values.
pub struct Value(*const _CassValue);

// The underlying C type is read-only so thread-safe.
unsafe impl Send for Value {}
unsafe impl Sync for Value {}

impl ProtectedInner<*const _CassValue> for Value {
    fn inner(&self) -> *const _CassValue {
        self.0
    }
}

impl Protected<*const _CassValue> for Value {
    fn build(inner: *const _CassValue) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        Value(inner)
    }
}

/// Write a set iterator to a formatter.
pub(crate) fn write_set<F>(f: &mut Formatter, set: Result<SetIterator>, writer: F) -> fmt::Result
where
    F: Fn(&mut Formatter, Value) -> fmt::Result,
{
    write!(f, "[")?;
    match set {
        Err(_) => write!(f, "<error>")?,
        Ok(iter) => {
            for item in iter {
                writer(f, item)?
            }
        }
    }
    write!(f, "]")?;
    Ok(())
}

/// Write a map iterator to a formatter.
pub(crate) fn write_map<F>(f: &mut Formatter, set: Result<MapIterator>, writer: F) -> fmt::Result
where
    F: Fn(&mut Formatter, Value, Value) -> fmt::Result,
{
    write!(f, "{{")?;
    match set {
        Err(_) => write!(f, "<error>")?,
        Ok(iter) => {
            for item in iter {
                writer(f, item.0, item.1)?
            }
        }
    }
    write!(f, "}}")?;
    Ok(())
}

pub(crate) fn write_value<T, F>(f: &mut Formatter, v: Result<T>, writer: F) -> fmt::Result
where
    F: Fn(&mut Formatter, T) -> fmt::Result,
{
    match v {
        Err(_) => write!(f, "<error>"),
        Ok(v) => writer(f, v),
    }
}

impl Debug for Value {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        if self.is_null() {
            Ok(())
        } else {
            match self.get_type() {
                ValueType::UNKNOWN => write!(f, "<unknown>"),
                ValueType::CUSTOM => write!(f, "<custom>"),
                ValueType::ASCII | ValueType::TEXT | ValueType::VARCHAR => {
                    write_value(f, self.get_string(), |f, v| write!(f, "{:?}", v))
                }
                ValueType::DECIMAL => write_value(f, self.get_bytes(), |f, v| write!(f, "{:?}", v)),
                ValueType::COUNTER => write_value(f, self.get_i64(), |f, v| write!(f, "{:?}", v)),
                ValueType::BIGINT => write_value(f, self.get_i64(), |f, v| write!(f, "{:?}", v)),
                ValueType::DATE => write_value(f, self.get_string(), |f, v| write!(f, "{:?}", v)),
                ValueType::TIME => write_value(f, self.get_string(), |f, v| write!(f, "{:?}", v)),
                ValueType::VARINT => write_value(f, self.get_bytes(), |f, v| write!(f, "{:?}", v)),
                ValueType::BOOLEAN => write_value(f, self.get_bool(), |f, v| write!(f, "{:?}", v)),
                ValueType::DOUBLE => write_value(f, self.get_f64(), |f, v| write!(f, "{:?}", v)),
                ValueType::FLOAT => write_value(f, self.get_f32(), |f, v| write!(f, "{:?}", v)),
                ValueType::BLOB => write_value(f, self.get_bytes(), |f, v| write!(f, "{:?}", v)),
                ValueType::INT => write_value(f, self.get_i32(), |f, v| write!(f, "{:?}", v)),
                ValueType::SMALL_INT => write_value(f, self.get_i16(), |f, v| write!(f, "{:?}", v)),
                ValueType::TINY_INT => write_value(f, self.get_i8(), |f, v| write!(f, "{:?}", v)),
                ValueType::DURATION => write_value(f, self.get_i32(), |f, v| write!(f, "{:?}", v)),
                ValueType::INET => write_value(f, self.get_inet(), |f, v| write!(f, "{:?}", v)),
                ValueType::TIMESTAMP => write_value(f, self.get_i64(), |f, v| write!(f, "{:?}", v)),
                ValueType::TIMEUUID => {
                    write_value(f, self.get_uuid(), |f, v| write!(f, "TIMEUUID: {}", v))
                }
                ValueType::UUID => write_value(f, self.get_uuid(), |f, v| write!(f, "UUID: {}", v)),
                ValueType::SET | ValueType::LIST => {
                    write_set(f, self.get_set(), |f, i| write!(f, "{:?}, ", i))
                }
                ValueType::MAP => write_map(f, self.get_map(), |f, k, v| {
                    write!(f, "{:?} => {:?}, ", k, v)
                }),
                ValueType::UDT => write!(f, "<udt>"),
                ValueType::TUPLE => write_set(f, self.get_set(), |f, i| write!(f, "{:?}, ", i)),
            }
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        if self.is_null() {
            Ok(())
        } else {
            match self.get_type() {
                ValueType::UNKNOWN => write!(f, "{}", "unknown"),
                ValueType::CUSTOM => write!(f, "{}", "custom"),
                ValueType::ASCII | ValueType::TEXT | ValueType::VARCHAR => {
                    write_value(f, self.get_string(), |f, v| write!(f, "{}", v))
                }
                ValueType::DECIMAL => {
                    write_value(f, self.get_bytes(), |f, v| write!(f, "DECIMAL:{:?}", v))
                }
                ValueType::COUNTER => write_value(f, self.get_i64(), |f, v| write!(f, "{}", v)),
                ValueType::BIGINT => write_value(f, self.get_i64(), |f, v| write!(f, "{}", v)),
                ValueType::DATE => write_value(f, self.get_string(), |f, v| write!(f, "{}", v)),
                ValueType::TIME => write_value(f, self.get_string(), |f, v| write!(f, "{}", v)),
                ValueType::VARINT => {
                    write_value(f, self.get_bytes(), |f, v| write!(f, "VARINT:{:?}", v))
                }
                ValueType::BOOLEAN => write_value(f, self.get_bool(), |f, v| write!(f, "{}", v)),
                ValueType::DOUBLE => write_value(f, self.get_f64(), |f, v| write!(f, "{}", v)),
                ValueType::FLOAT => write_value(f, self.get_f32(), |f, v| write!(f, "{}", v)),
                ValueType::BLOB => {
                    write_value(f, self.get_bytes(), |f, v| write!(f, "BLOB:{:?}", v))
                }
                ValueType::INT => write_value(f, self.get_i32(), |f, v| write!(f, "{}", v)),
                ValueType::SMALL_INT => write_value(f, self.get_i16(), |f, v| write!(f, "{}", v)),
                ValueType::TINY_INT => write_value(f, self.get_i8(), |f, v| write!(f, "{}", v)),
                ValueType::DURATION => write_value(f, self.get_i32(), |f, v| write!(f, "{:?}", v)),
                ValueType::INET => {
                    write_value(f, self.get_inet(), |f, v| write!(f, "INET:{:?}", v))
                }
                ValueType::TIMESTAMP => write_value(f, self.get_i64(), |f, v| write!(f, "{}", v)),
                ValueType::TIMEUUID => {
                    write_value(f, self.get_uuid(), |f, v| write!(f, "TIMEUUID:{}", v))
                }
                ValueType::UUID => write_value(f, self.get_uuid(), |f, v| write!(f, "UUID:{}", v)),
                ValueType::SET | ValueType::LIST => {
                    write_set(f, self.get_set(), |f, i| write!(f, "{}, ", i))
                }
                ValueType::MAP => {
                    write_map(f, self.get_map(), |f, k, v| write!(f, "{} => {}, ", k, v))
                }
                ValueType::UDT => write!(f, "<udt>"),
                ValueType::TUPLE => write_set(f, self.get_set(), |f, i| write!(f, "{}, ", i)),
            }
        }
    }
}

impl Value {
    /// Get the raw bytes of this Cassandra value.
    pub fn get_bytes(&self) -> Result<&[u8]> {
        unsafe {
            let mut output = mem::zeroed();
            let mut output_size = mem::zeroed();
            let result = cass_value_get_bytes(self.0, &mut output, &mut output_size);
            // raw2utf8(output, output_size).unwrap()
            let slice = slice::from_raw_parts(output, output_size as usize);
            result.to_result(slice)
        }
    }
    // pub fn get_decimal<'a>(&'a self, mut output: String) ->
    // Result<String,CassError> {unsafe{
    // CassError::build(cass_value_get_decimal(self.0,&mut
    // output)).wrap(output)
    //    }}

    /// Get the type of this Cassandra value
    pub fn get_type(&self) -> ValueType {
        unsafe { ValueType::build(cass_value_type(self.0)) }
    }

    /// Get the data type of this Cassandra value
    pub fn data_type(&self) -> ConstDataType {
        unsafe { ConstDataType::build(cass_value_data_type(self.0)) }
    }

    /// Returns true if a specified value is null.
    pub fn is_null(&self) -> bool {
        unsafe { cass_value_is_null(self.0) == cass_true }
    }

    /// Returns true if a specified value is a collection.
    pub fn is_collection(&self) -> bool {
        unsafe { cass_value_is_collection(self.0) == cass_true }
    }

    //    pub fn item_count(&self) -> u64 {
    //        unsafe { cass_value_item_count(self.0) }
    //    }

    // 	///Get the primary sub-type for a collection. This returns the sub-type for a
    // 	///list or set and the key type for a map.
    //    pub fn primary_sub_type(&self) -> ValueType {
    //        unsafe { ValueType::build(cass_value_primary_sub_type(self.0)).unwrap() }
    //    }

    // 	///Get the secondary sub-type for a collection. This returns the value type for a map.
    //    pub fn secondary_sub_type(&self) -> ValueType {
    //        unsafe { ValueType::build(cass_value_secondary_sub_type(self.0)).unwrap() }
    //    }

    /// Gets this value as a set iterator.
    pub fn get_set(&self) -> Result<SetIterator> {
        unsafe {
            match self.get_type() {
                ValueType::SET | ValueType::LIST | ValueType::TUPLE => {
                    let iter = cass_iterator_from_collection(self.0);
                    if iter.is_null() {
                        // No iterator, probably because this set is_null. Complain.
                        Err(CASS_ERROR_LIB_NULL_VALUE.to_error())
                    } else {
                        Ok(SetIterator::build(iter))
                    }
                }
                _ => Err(CASS_ERROR_LIB_INVALID_VALUE_TYPE.to_error()),
            }
        }
    }

    /// Gets this value as a map iterator.
    pub fn get_map(&self) -> Result<MapIterator> {
        unsafe {
            match self.get_type() {
                ValueType::MAP => {
                    let iter = cass_iterator_from_map(self.0);
                    if iter.is_null() {
                        // No iterator, probably because this map is_null. Complain.
                        Err(CASS_ERROR_LIB_NULL_VALUE.to_error())
                    } else {
                        Ok(MapIterator::build(iter))
                    }
                }
                _ => Err(CASS_ERROR_LIB_INVALID_VALUE_TYPE.to_error()),
            }
        }
    }

    /// Gets an iterator over the fields of the user type in this column or errors if you ask for the wrong type
    pub fn get_user_type(&self) -> Result<UserTypeIterator> {
        unsafe {
            match self.get_type() {
                ValueType::UDT => {
                    let iter = cass_iterator_fields_from_user_type(self.0);
                    if iter.is_null() {
                        // No iterator, probably because this user_type field is null. Complain.
                        Err(CASS_ERROR_LIB_NULL_VALUE.to_error())
                    } else {
                        Ok(UserTypeIterator::build(iter))
                    }
                }
                _ => Err(CASS_ERROR_LIB_INVALID_VALUE_TYPE.to_error()),
            }
        }
    }

    /// Get this value as a string slice
    pub fn get_str(&self) -> Result<&str> {
        unsafe {
            let mut message_ptr = mem::zeroed();
            let mut message_length = mem::zeroed();
            cass_value_get_string(self.0, &mut message_ptr, &mut message_length)
                .to_result(())
                .and_then(|_| {
                    let slice =
                        slice::from_raw_parts(message_ptr as *const u8, message_length as usize);
                    Ok(str::from_utf8(slice)?)
                })
        }
    }

    /// Get this value as a string
    pub fn get_string(&self) -> Result<String> {
        self.get_str().map(str::to_string)
    }

    /// Get this value as an Inet
    pub fn get_inet(&self) -> Result<Inet> {
        unsafe {
            let mut inet = mem::zeroed();
            cass_value_get_inet(self.0, &mut inet).to_result(Inet::build(inet))
        }
    }

    /// Get this value as an i32
    pub fn get_i32(&self) -> Result<i32> {
        unsafe {
            let mut output = mem::zeroed();
            cass_value_get_int32(self.0, &mut output).to_result(output)
        }
    }

    /// Get this value as a u32
    pub fn get_u32(&self) -> Result<u32> {
        unsafe {
            let mut output = mem::zeroed();
            cass_value_get_uint32(self.0, &mut output).to_result(output)
        }
    }

    /// Get this value as an i16
    pub fn get_i16(&self) -> Result<i16> {
        unsafe {
            let mut output = mem::zeroed();
            cass_value_get_int16(self.0, &mut output).to_result(output)
        }
    }

    /// Get this value as an i8
    pub fn get_i8(&self) -> Result<i8> {
        unsafe {
            let mut output = mem::zeroed();
            cass_value_get_int8(self.0, &mut output).to_result(output)
        }
    }

    /// Get this value as an i64
    pub fn get_i64(&self) -> Result<i64> {
        unsafe {
            let mut output = mem::zeroed();
            cass_value_get_int64(self.0, &mut output).to_result(output)
        }
    }

    /// Get this value as a float
    pub fn get_f32(&self) -> Result<f32> {
        unsafe {
            let mut output = mem::zeroed();
            cass_value_get_float(self.0, &mut output).to_result(output)
        }
    }

    /// Get this value as a double
    pub fn get_f64(&self) -> Result<f64> {
        unsafe {
            let mut output = mem::zeroed();
            cass_value_get_double(self.0, &mut output).to_result(output)
        }
    }

    /// Get this value asa boolean
    pub fn get_bool(&self) -> Result<bool> {
        unsafe {
            let mut output = mem::zeroed();
            cass_value_get_bool(self.0, &mut output).to_result(output == cass_true)
        }
    }

    /// Get this value as a UUID
    pub fn get_uuid(&self) -> Result<Uuid> {
        unsafe {
            let mut uuid = mem::zeroed();
            cass_value_get_uuid(self.0, &mut uuid).to_result(Uuid::build(uuid))
        }
    }
}
