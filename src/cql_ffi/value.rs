use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt;
use std::ffi::CString;
use std::str;
use std::slice;

use cql_ffi::error::CassandraError;
use cql_ffi::inet::Inet;
use cql_ffi::uuid::Uuid;
use cql_ffi::collection::map::MapIterator;
use cql_ffi::collection::set::SetIterator;
use cql_ffi::error::CassandraErrorTypes;
use cql_ffi::udt::UserTypeIterator;

use cql_bindgen::CassValue as _Value;
use cql_bindgen::cass_value_secondary_sub_type;
use cql_bindgen::cass_value_primary_sub_type;
use cql_bindgen::cass_value_item_count;
use cql_bindgen::cass_value_is_collection;
use cql_bindgen::cass_value_is_null;
use cql_bindgen::cass_value_type;
// use cql_bindgen::cass_value_get_decimal;
use cql_bindgen::cass_value_get_inet;
use cql_bindgen::cass_value_get_string;
use cql_bindgen::cass_value_get_bytes;
use cql_bindgen::cass_value_get_uuid;
use cql_bindgen::cass_value_get_bool;
use cql_bindgen::cass_value_get_double;
use cql_bindgen::cass_value_get_float;
use cql_bindgen::cass_value_get_int64;
use cql_bindgen::cass_value_get_int32;
// use cql_bindgen::cass_iterator_from_user_type;
use cql_bindgen::cass_iterator_from_collection;
use cql_bindgen::cass_iterator_from_map;
use cql_bindgen::cass_value_data_type;


use cql_bindgen::CASS_VALUE_TYPE_UNKNOWN;
use cql_bindgen::CASS_VALUE_TYPE_CUSTOM;
use cql_bindgen::CASS_VALUE_TYPE_ASCII;
use cql_bindgen::CASS_VALUE_TYPE_BIGINT;
use cql_bindgen::CASS_VALUE_TYPE_BLOB;
use cql_bindgen::CASS_VALUE_TYPE_BOOLEAN;
use cql_bindgen::CASS_VALUE_TYPE_COUNTER;
use cql_bindgen::CASS_VALUE_TYPE_DECIMAL;
use cql_bindgen::CASS_VALUE_TYPE_DOUBLE;
use cql_bindgen::CASS_VALUE_TYPE_FLOAT;
use cql_bindgen::CASS_VALUE_TYPE_INT;
use cql_bindgen::CASS_VALUE_TYPE_TEXT;
use cql_bindgen::CASS_VALUE_TYPE_TIMESTAMP;
use cql_bindgen::CASS_VALUE_TYPE_UUID;
use cql_bindgen::CASS_VALUE_TYPE_VARCHAR;
use cql_bindgen::CASS_VALUE_TYPE_TIMEUUID;
use cql_bindgen::CASS_VALUE_TYPE_INET;
use cql_bindgen::CASS_VALUE_TYPE_LIST;
use cql_bindgen::CASS_VALUE_TYPE_SET;
use cql_bindgen::CASS_VALUE_TYPE_MAP;
use cql_bindgen::CASS_VALUE_TYPE_VARINT;
use cql_bindgen::CASS_VALUE_TYPE_UDT;
use cql_bindgen::CASS_VALUE_TYPE_TUPLE;
use cql_bindgen::CASS_VALUE_TYPE_LAST_ENTRY;

use std::mem;

pub struct Value(*const _Value);

#[derive(Debug)]
pub enum ValueType {
    UNKNOWN = CASS_VALUE_TYPE_UNKNOWN as isize,
    CUSTOM = CASS_VALUE_TYPE_CUSTOM as isize,
    ASCII = CASS_VALUE_TYPE_ASCII as isize,
    BIGINT = CASS_VALUE_TYPE_BIGINT as isize,
    BLOB = CASS_VALUE_TYPE_BLOB as isize,
    BOOLEAN = CASS_VALUE_TYPE_BOOLEAN as isize,
    COUNTER = CASS_VALUE_TYPE_COUNTER as isize,
    DECIMAL = CASS_VALUE_TYPE_DECIMAL as isize,
    DOUBLE = CASS_VALUE_TYPE_DOUBLE as isize,
    FLOAT = CASS_VALUE_TYPE_FLOAT as isize,
    INT = CASS_VALUE_TYPE_INT as isize,
    TEXT = CASS_VALUE_TYPE_TEXT as isize,
    TIMESTAMP = CASS_VALUE_TYPE_TIMESTAMP as isize,
    UUID = CASS_VALUE_TYPE_UUID as isize,
    VARCHAR = CASS_VALUE_TYPE_VARCHAR as isize,
    VARINT = CASS_VALUE_TYPE_VARINT as isize,
    TIMEUUID = CASS_VALUE_TYPE_TIMEUUID as isize,
    INET = CASS_VALUE_TYPE_INET as isize,
    LIST = CASS_VALUE_TYPE_LIST as isize,
    MAP = CASS_VALUE_TYPE_MAP as isize,
    SET = CASS_VALUE_TYPE_SET as isize,
    UDT = CASS_VALUE_TYPE_UDT as isize,
    TUPLE = CASS_VALUE_TYPE_TUPLE as isize,
    LASTENTRY = CASS_VALUE_TYPE_LAST_ENTRY as isize,
}

impl ValueType {
    pub fn build(_type: u32) -> Self {
        match _type {
            CASS_VALUE_TYPE_UNKNOWN => ValueType::UNKNOWN,
            CASS_VALUE_TYPE_CUSTOM => ValueType::CUSTOM,
            CASS_VALUE_TYPE_ASCII => ValueType::ASCII,
            CASS_VALUE_TYPE_BIGINT => ValueType::BIGINT,
            CASS_VALUE_TYPE_BLOB => ValueType::BLOB,
            CASS_VALUE_TYPE_BOOLEAN => ValueType::BOOLEAN,
            CASS_VALUE_TYPE_COUNTER => ValueType::COUNTER,
            CASS_VALUE_TYPE_DECIMAL => ValueType::DECIMAL,
            CASS_VALUE_TYPE_DOUBLE => ValueType::DOUBLE,
            CASS_VALUE_TYPE_FLOAT => ValueType::FLOAT,
            CASS_VALUE_TYPE_INT => ValueType::INT,
            CASS_VALUE_TYPE_TEXT => ValueType::TEXT,
            CASS_VALUE_TYPE_TIMESTAMP => ValueType::TIMESTAMP,
            CASS_VALUE_TYPE_UUID => ValueType::UUID,
            CASS_VALUE_TYPE_VARCHAR => ValueType::VARCHAR,
            CASS_VALUE_TYPE_VARINT => ValueType::VARINT,
            CASS_VALUE_TYPE_TIMEUUID => ValueType::TIMEUUID,
            CASS_VALUE_TYPE_INET => ValueType::INET,
            CASS_VALUE_TYPE_LIST => ValueType::LIST,
            CASS_VALUE_TYPE_MAP => ValueType::MAP,
            CASS_VALUE_TYPE_SET => ValueType::SET,
            CASS_VALUE_TYPE_UDT => ValueType::UDT,
            err => panic!("impossible value type{}", err),
        }
    }
}

impl Debug for Value {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self.is_null() {
            true => Ok(()),
            false => {
                match self.get_type() {
                    ValueType::UNKNOWN => write!(f, "{:?}", "unknown"),
                    ValueType::CUSTOM => write!(f, "{:?}", "custom"),
                    ValueType::ASCII => write!(f, "{:?}", self.get_string().unwrap()),
                    ValueType::BIGINT => write!(f, "{:?}", self.get_int64().unwrap()),
                    ValueType::VARCHAR => write!(f, "{:?}", self.get_string().unwrap()),
                    ValueType::BOOLEAN => write!(f, "{:?}", self.get_bool().unwrap()),
                    ValueType::DOUBLE => write!(f, "{:?}", self.get_double().unwrap()),
                    ValueType::FLOAT => write!(f, "{:?}", self.get_float().unwrap()),
                    ValueType::INT => write!(f, "{:?}", self.get_int32().unwrap()),
                    ValueType::TIMEUUID => write!(f, "TIMEUUID: {:?}", self.get_uuid().unwrap()),
                    ValueType::SET => {
                        try!(write!(f, "["));
                        for item in self.as_set_iterator().unwrap() {
                            try!(write!(f, "SET {:?} ", item))
                        }
                        try!(write!(f, "]"));
                        Ok(())
                    }
                    ValueType::MAP => {
                        for item in self.as_map_iterator().unwrap() {
                            try!(write!(f, "MAP {:?}:{:?}", item.0, item.1))
                        }
                        Ok(())
                    }
                    ValueType::UDT => {
                        //                    for item in self.as_map_iterator().unwrap() {
                        //                        try!(write!(f, "MAP {:?}:{:?}", item.0,item.1))
                        //                    }
                        Ok(())
                    }

                    // FIXME
                    err => write!(f, "{:?}", err),
                }
            }
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self.is_null() {
            true => Ok(()),
            false => {
                match self.get_type() {
                    ValueType::UNKNOWN => write!(f, "{}", "unknown"),
                    ValueType::CUSTOM => write!(f, "{}", "custom"),
                    ValueType::ASCII => write!(f, "{}", self.get_string().unwrap()),
                    ValueType::BIGINT => write!(f, "{}", self.get_int64().unwrap()),
                    ValueType::VARCHAR => write!(f, "{}", self.get_string().unwrap()),
                    ValueType::BOOLEAN => write!(f, "{}", self.get_bool().unwrap()),
                    ValueType::DOUBLE => write!(f, "{}", self.get_double().unwrap()),
                    ValueType::FLOAT => write!(f, "{}", self.get_float().unwrap()),
                    ValueType::INT => write!(f, "{}", self.get_int32().unwrap()),
                    ValueType::TIMEUUID => write!(f, "TIMEUUID: {}", self.get_uuid().unwrap()),
                    ValueType::SET => {
                        try!(write!(f, "["));
                        for item in self.as_set_iterator().unwrap() {
                            try!(write!(f, "{} ", item))
                        }
                        try!(write!(f, "]"));
                        Ok(())
                    }
                    ValueType::MAP => {
                        for item in self.as_map_iterator().unwrap() {
                            try!(write!(f, "MAP {}:{}", item.0, item.1))
                        }
                        Ok(())
                    }

                    // FIXME
                    err => write!(f, "{:?}", err),
                }
            }
        }
    }
}

impl Value {
    pub fn new(value: *const _Value) -> Self {
        // println!("building value: {:?}", Value(value).get_type());
        Value(value)
    }

    pub fn fill_uuid(&self, mut uuid: Uuid) -> Result<Uuid, CassandraError> {
        unsafe { CassandraError::build(cass_value_get_uuid(self.0, &mut uuid.0)).wrap(uuid) }
    }

    pub fn fill_string(&self) -> Result<String, CassandraError> {
        unsafe {
            let output = mem::zeroed();
            let output_length = mem::zeroed();
            let err = cass_value_get_string(self.0, output, output_length);

            let slice = slice::from_raw_parts(output as *const u8, output_length as usize);
            let string = str::from_utf8(slice).unwrap().to_owned();
            CassandraError::build(err).wrap(string)
        }
    }

    // FIXME test this
    pub fn get_bytes(&self) -> Result<Vec<*const u8>, CassandraError> {
        unsafe {
            let mut output: *const u8 = mem::zeroed();
            let output_size = mem::zeroed();
            let result = cass_value_get_bytes(self.0, &mut output, output_size);
            // let output:*mut u8 = &mut*output;
            let slice: Vec<*const u8> = Vec::from_raw_parts(&mut output, output_size as usize, output_size as usize);
            let r = CassandraError::build(result);
            r.wrap(slice)
        }
    }

    // pub fn get_decimal<'a>(&'a self, mut output: String) ->
    // Result<String,CassandraError> {unsafe{
    // CassandraError::build(cass_value_get_decimal(self.0,&mut
    // output)).wrap(output)
    //    }}

    pub fn get_type(&self) -> ValueType {
        unsafe { ValueType::build(cass_value_type(self.0)) }
    }

    pub fn is_null(&self) -> bool {
        unsafe { cass_value_is_null(self.0) > 0 }
    }

    pub fn is_collection(&self) -> bool {
        unsafe { cass_value_is_collection(self.0) > 0 }
    }

    pub fn item_count(&self) -> u64 {
        unsafe { cass_value_item_count(self.0) }
    }

    pub fn primary_sub_type(&self) -> ValueType {
        unsafe { ValueType::build(cass_value_primary_sub_type(self.0)) }
    }

    pub fn secondary_sub_type(&self) -> ValueType {
        unsafe { ValueType::build(cass_value_secondary_sub_type(self.0)) }
    }

    pub fn as_set_iterator(&self) -> Result<SetIterator, CassandraError> {
        unsafe {
            match self.get_type() {
                ValueType::SET => Ok(SetIterator(cass_iterator_from_collection(self.0))),
                _ => Err(CassandraError::build(CassandraErrorTypes::LIB_INVALID_VALUE_TYPE as u32)),
            }
        }
    }

    pub fn as_map_iterator(&self) -> Result<MapIterator, CassandraError> {
        unsafe {
            match self.get_type() {
                ValueType::MAP => Ok(MapIterator(cass_iterator_from_map(self.0))),
                _ => Err(CassandraError::build(CassandraErrorTypes::LIB_INVALID_VALUE_TYPE as u32)),
            }
        }
    }

    //    pub fn as_user_type_iterator(&self) -> Result<UserTypeIterator, CassandraError> {
    //        unsafe {
    //            match self.get_type() {
    //                ValueType::UDT => Ok(UserTypeIterator(cass_iterator_from_user_type(self.0))),
    //                _ => Err(CassandraError::build(CassandraErrorTypes::LIB_INVALID_VALUE_TYPE as u32)),
    //            }
    //        }
    //    }


    // ~ pub fn map_iter(&self) -> Result<MapIterator,CassandraError> {unsafe{
    // ~ match self.get_type() {
    // ~ ValueType::MAP => Ok(MapIterator(cass_iterator_from_map(self.0))),
    // ~ type_no => {
    // ~ println!("wrong_type: {:?}", type_no);
    // ~ Err(CassandraError::build(CassandraErrorTypes::LIB_INVALID_VALUE_TYPE as
    // u32))
    // ~ }
    // ~ }
    // ~ }}

    pub fn get_string(&self) -> Result<String, CassandraError> {
        unsafe {
            let message: CString = mem::zeroed();
            let mut message = message.as_ptr();
            let mut message_length = mem::zeroed();
            cass_value_get_string(self.0, &mut message, &mut (message_length));

            let slice = slice::from_raw_parts(message as *const u8, message_length as usize);
            let err = CassandraError::build(cass_value_get_string(self.0, &mut message, &mut (message_length)));
            err.wrap(str::from_utf8(slice).unwrap().to_owned())
        }
    }

    // ~ pub fn get_string(&self) -> Result<String,CassandraError> {unsafe{
    // ~ let mut output = mem::zeroed();
    // ~ let mut output_size = mem::zeroed();
    // ~ let output = &mut output;
    // ~ let foo = self.0;
    // ~ cass_value_get_string(foo, output, output_size);
    // ~ let err = CassandraError::build(cass_value_get_string(self.0, output,
    // output_size));

    // ~ let slice = slice::from_raw_parts(output,output_size as usize);
    // ~ let string = str::from_utf8(slice).unwrap().to_string();



    // ~ err.wrap(string)
    // ~ }}

    pub fn get_inet(&self, mut output: Inet) -> Result<Inet, CassandraError> {
        unsafe { CassandraError::build(cass_value_get_inet(self.0, &mut output.0)).wrap(output) }
    }

    pub fn get_int32(&self) -> Result<i32, CassandraError> {
        unsafe {
            let mut output = mem::zeroed();
            CassandraError::build(cass_value_get_int32(self.0, &mut output)).wrap(output)
        }
    }

    pub fn get_int64(&self) -> Result<i64, CassandraError> {
        unsafe {
            let mut output = mem::zeroed();
            CassandraError::build(cass_value_get_int64(self.0, &mut output)).wrap(output)
        }
    }

    pub fn get_float(&self) -> Result<f32, CassandraError> {
        unsafe {
            let mut output = mem::zeroed();
            CassandraError::build(cass_value_get_float(self.0, &mut output)).wrap(output)
        }
    }

    pub fn get_double(&self) -> Result<f64, CassandraError> {
        unsafe {
            let mut output = mem::zeroed();
            CassandraError::build(cass_value_get_double(self.0, &mut output)).wrap(output)
        }
    }

    pub fn get_bool(&self) -> Result<bool, CassandraError> {
        unsafe {
            let mut output = mem::zeroed();
            CassandraError::build(cass_value_get_bool(self.0, &mut output)).wrap(output > 0)
        }
    }

    pub fn get_uuid(&self) -> Result<Uuid, CassandraError> {
        unsafe {
            let mut output: Uuid = mem::zeroed();
            CassandraError::build(cass_value_get_uuid(self.0, &mut output.0)).wrap(output)
        }
    }
}
