use crate::cassandra::data_type::DataType;
use crate::cassandra::error::*;
use crate::cassandra::field::Field;
use crate::cassandra::schema::aggregate_meta::AggregateMeta;
use crate::cassandra::schema::column_meta::ColumnMeta;
use crate::cassandra::schema::function_meta::FunctionMeta;
use crate::cassandra::schema::keyspace_meta::KeyspaceMeta;
use crate::cassandra::schema::table_meta::TableMeta;
use crate::cassandra::util::{Protected, ProtectedInner};
use crate::cassandra::value::Value;

// use cassandra_sys::CassIteratorType as _CassIteratorType;
use crate::cassandra_sys::cass_false;
use crate::cassandra_sys::CassIterator as _CassIterator;
// use cassandra_sys::cass_iterator_type;
use crate::cassandra_sys::cass_iterator_free;
use crate::cassandra_sys::cass_iterator_get_aggregate_meta;
use crate::cassandra_sys::cass_iterator_get_column_meta;
use crate::cassandra_sys::cass_iterator_get_function_meta;
use crate::cassandra_sys::cass_iterator_get_keyspace_meta;
use crate::cassandra_sys::cass_iterator_get_map_key;
use crate::cassandra_sys::cass_iterator_get_map_value;
use crate::cassandra_sys::cass_iterator_get_meta_field_name;
use crate::cassandra_sys::cass_iterator_get_meta_field_value;
use crate::cassandra_sys::cass_iterator_get_table_meta;
use crate::cassandra_sys::cass_iterator_get_user_type_field_name;
use crate::cassandra_sys::cass_iterator_get_user_type_field_value;
use crate::cassandra_sys::cass_iterator_get_value;
use crate::cassandra_sys::cass_iterator_next;
use crate::cassandra_sys::cass_true;
use std::{mem, slice, str};

/// Iterates over the  aggregate metadata entries(??)
#[derive(Debug)]
pub struct AggregateIterator(*mut _CassIterator);

// The underlying C type has no thread-local state, but does not support access
// from multiple threads: https://datastax.github.io/cpp-driver/topics/#thread-safety
unsafe impl Send for AggregateIterator {}

impl Drop for AggregateIterator {
    fn drop(&mut self) {
        unsafe { cass_iterator_free(self.0) }
    }
}

impl Iterator for AggregateIterator {
    type Item = AggregateMeta;
    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        unsafe {
            match cass_iterator_next(self.0) {
                cass_false => None,
                cass_true => {
                    let field_value = cass_iterator_get_aggregate_meta(self.0);
                    Some(AggregateMeta::build(field_value))
                }
            }
        }
    }
}

/// Iterater over the fields of a UDT
#[derive(Debug)]
pub struct UserTypeIterator(*mut _CassIterator);

// The underlying C type has no thread-local state, but does not support access
// from multiple threads: https://datastax.github.io/cpp-driver/topics/#thread-safety
unsafe impl Send for UserTypeIterator {}

impl Drop for UserTypeIterator {
    fn drop(&mut self) {
        unsafe { cass_iterator_free(self.0) }
    }
}

impl Iterator for UserTypeIterator {
    type Item = (String, Value);
    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        unsafe {
            match cass_iterator_next(self.0) {
                cass_false => None,
                cass_true => Some((self.get_field_name(), self.get_field_value())),
            }
        }
    }
}

impl UserTypeIterator {
    fn get_field_name(&mut self) -> String {
        unsafe {
            let mut name = mem::zeroed();
            let mut name_length = mem::zeroed();
            cass_iterator_get_user_type_field_name(self.0, &mut name, &mut name_length)
                .to_result(())
                .and_then(|_| {
                    let slice = slice::from_raw_parts(name as *const u8, name_length as usize);
                    let name = str::from_utf8(slice)?.to_owned();
                    Ok(name)
                })
                .expect("Cassandra error during iteration")
        }
    }
    fn get_field_value(&mut self) -> Value {
        unsafe { Value::build(cass_iterator_get_user_type_field_value(self.0)) }
    }
}

/// Iterater over the  function metadata entries(??)
#[derive(Debug)]
pub struct FunctionIterator(*mut _CassIterator);

// The underlying C type has no thread-local state, but does not support access
// from multiple threads: https://datastax.github.io/cpp-driver/topics/#thread-safety
unsafe impl Send for FunctionIterator {}

impl Iterator for FunctionIterator {
    type Item = FunctionMeta;
    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        unsafe {
            match cass_iterator_next(self.0) {
                cass_false => None,
                cass_true => Some(FunctionMeta::build(cass_iterator_get_function_meta(self.0))),
            }
        }
    }
}

/// Iterater over the table's metadata entries(??)
#[derive(Debug)]
pub struct TableIterator(*mut _CassIterator);

// The underlying C type has no thread-local state, but does not support access
// from multiple threads: https://datastax.github.io/cpp-driver/topics/#thread-safety
unsafe impl Send for TableIterator {}

impl Iterator for TableIterator {
    type Item = TableMeta;
    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        unsafe {
            match cass_iterator_next(self.0) {
                cass_false => None,
                cass_true => Some(TableMeta::build(cass_iterator_get_table_meta(self.0))),
            }
        }
    }
}

/// Iterater over the keyspace's metadata entries(??)
#[derive(Debug)]
pub struct KeyspaceIterator(*mut _CassIterator);

// The underlying C type has no thread-local state, but does not support access
// from multiple threads: https://datastax.github.io/cpp-driver/topics/#thread-safety
unsafe impl Send for KeyspaceIterator {}

impl Iterator for KeyspaceIterator {
    type Item = KeyspaceMeta;
    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        unsafe {
            match cass_iterator_next(self.0) {
                cass_false => None,
                cass_true => Some(KeyspaceMeta::build(cass_iterator_get_keyspace_meta(self.0))),
            }
        }
    }
}

/// Iterater over the columns's metadata entries(??)
#[derive(Debug)]
pub struct ColumnIterator(*mut _CassIterator);

// The underlying C type has no thread-local state, but does not support access
// from multiple threads: https://datastax.github.io/cpp-driver/topics/#thread-safety
unsafe impl Send for ColumnIterator {}

impl Iterator for ColumnIterator {
    type Item = ColumnMeta;
    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        unsafe {
            match cass_iterator_next(self.0) {
                cass_false => None,
                cass_true => Some(ColumnMeta::build(cass_iterator_get_column_meta(self.0))),
            }
        }
    }
}

/// Iterater over the field's metadata entries(??)
#[derive(Debug)]
pub struct FieldIterator(*mut _CassIterator);

// The underlying C type has no thread-local state, but does not support access
// from multiple threads: https://datastax.github.io/cpp-driver/topics/#thread-safety
unsafe impl Send for FieldIterator {}

impl Iterator for FieldIterator {
    type Item = Field;
    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        unsafe {
            match cass_iterator_next(self.0) {
                cass_false => None,
                cass_true => {
                    let mut name = mem::zeroed();
                    let mut name_length = mem::zeroed();
                    cass_iterator_get_meta_field_name(self.0, &mut name, &mut name_length)
                        .to_result(())
                        .and_then(|_| {
                            let slice =
                                slice::from_raw_parts(name as *const u8, name_length as usize);
                            let name = str::from_utf8(slice)?.to_owned();
                            let value = Value::build(cass_iterator_get_meta_field_value(self.0));
                            Ok(Some(Field { name, value }))
                        })
                }
                .expect("Cassandra error during iteration"),
            }
        }
    }
}

// pub struct CassIteratorType(_CassIteratorType);

// impl CassIteratorType {
//    pub fn new(_type: _CassIteratorType) -> Self { CassIteratorType(_type) }
// }

// impl Protected<*mut _Batch> for CassIterator {
//    fn inner(&self) -> *mut _CassIterator {
//        self.0
//    }
//    fn build(inner: *mut _CassIterator) -> Self {
//        CassIterator(inner)
//    }
// }

impl ProtectedInner<*mut _CassIterator> for UserTypeIterator {
    fn inner(&self) -> *mut _CassIterator {
        self.0
    }
}

impl Protected<*mut _CassIterator> for UserTypeIterator {
    fn build(inner: *mut _CassIterator) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        UserTypeIterator(inner)
    }
}

impl ProtectedInner<*mut _CassIterator> for AggregateIterator {
    fn inner(&self) -> *mut _CassIterator {
        self.0
    }
}

impl Protected<*mut _CassIterator> for AggregateIterator {
    fn build(inner: *mut _CassIterator) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        AggregateIterator(inner)
    }
}

impl ProtectedInner<*mut _CassIterator> for FunctionIterator {
    fn inner(&self) -> *mut _CassIterator {
        self.0
    }
}

impl Protected<*mut _CassIterator> for FunctionIterator {
    fn build(inner: *mut _CassIterator) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        FunctionIterator(inner)
    }
}

impl ProtectedInner<*mut _CassIterator> for KeyspaceIterator {
    fn inner(&self) -> *mut _CassIterator {
        self.0
    }
}

impl Protected<*mut _CassIterator> for KeyspaceIterator {
    fn build(inner: *mut _CassIterator) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        KeyspaceIterator(inner)
    }
}

impl ProtectedInner<*mut _CassIterator> for FieldIterator {
    fn inner(&self) -> *mut _CassIterator {
        self.0
    }
}
impl Protected<*mut _CassIterator> for FieldIterator {
    fn build(inner: *mut _CassIterator) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        FieldIterator(inner)
    }
}

impl ProtectedInner<*mut _CassIterator> for ColumnIterator {
    fn inner(&self) -> *mut _CassIterator {
        self.0
    }
}

impl Protected<*mut _CassIterator> for ColumnIterator {
    fn build(inner: *mut _CassIterator) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        ColumnIterator(inner)
    }
}

impl ProtectedInner<*mut _CassIterator> for TableIterator {
    fn inner(&self) -> *mut _CassIterator {
        self.0
    }
}

impl Protected<*mut _CassIterator> for TableIterator {
    fn build(inner: *mut _CassIterator) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        TableIterator(inner)
    }
}

impl ProtectedInner<*mut _CassIterator> for MapIterator {
    fn inner(&self) -> *mut _CassIterator {
        self.0
    }
}

impl Protected<*mut _CassIterator> for MapIterator {
    fn build(inner: *mut _CassIterator) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        MapIterator(inner)
    }
}

impl ProtectedInner<*mut _CassIterator> for SetIterator {
    fn inner(&self) -> *mut _CassIterator {
        self.0
    }
}

impl Protected<*mut _CassIterator> for SetIterator {
    fn build(inner: *mut _CassIterator) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        SetIterator(inner)
    }
}

/// Iterater over the set's metadata entries(??)
#[derive(Debug)]
pub struct SetIterator(*mut _CassIterator);

// The underlying C type has no thread-local state, but does not support access
// from multiple threads: https://datastax.github.io/cpp-driver/topics/#thread-safety
unsafe impl Send for SetIterator {}

// impl<'a> Display for &'a SetIterator {
//    fn fmt(&self, f:&mut Formatter) -> fmt::Result {
//        for item in self {
//            try!(write!(f, "{}\t", item));
//        }
//        Ok(())
//    }
// }

impl Drop for SetIterator {
    fn drop(&mut self) {
        unsafe { cass_iterator_free(self.0) }
    }
}

impl Iterator for SetIterator {
    type Item = Value;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        unsafe {
            match cass_iterator_next(self.0) {
                cass_false => None,
                cass_true => Some(self.get_value()),
            }
        }
    }
}

impl SetIterator {
    fn get_value(&mut self) -> Value {
        unsafe { Value::build(cass_iterator_get_value(self.0)) }
    }
}

/// An iterator over the k/v pair in the map
#[derive(Debug)]
pub struct MapIterator(*mut _CassIterator);

// The underlying C type has no thread-local state, but does not support access
// from multiple threads: https://datastax.github.io/cpp-driver/topics/#thread-safety
unsafe impl Send for MapIterator {}

impl MapIterator {
    fn get_key(&mut self) -> Value {
        unsafe { Value::build(cass_iterator_get_map_key(self.0)) }
    }
    fn get_value(&mut self) -> Value {
        unsafe { Value::build(cass_iterator_get_map_value(self.0)) }
    }

    /// Gets the next k/v pair in the map
    pub fn get_pair(&mut self) -> (Value, Value) {
        (self.get_key(), self.get_value())
    }
}

/// An iterator over the elements of a Cassandra tuple
#[derive(Debug)]
pub struct TupleIterator(pub *mut _CassIterator);

// The underlying C type has no thread-local state, but does not support access
// from multiple threads: https://datastax.github.io/cpp-driver/topics/#thread-safety
unsafe impl Send for TupleIterator {}

impl Drop for TupleIterator {
    fn drop(&mut self) {
        unsafe { cass_iterator_free(self.0) }
    }
}

impl Iterator for TupleIterator {
    type Item = Value;
    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        unsafe {
            match cass_iterator_next(self.0) {
                cass_false => None,
                cass_true => Some(self.get_value()),
            }
        }
    }
}

impl TupleIterator {
    fn get_value(&mut self) -> Value {
        unsafe { Value::build(cass_iterator_get_value(self.0)) }
    }
}

impl Drop for MapIterator {
    fn drop(&mut self) {
        unsafe { cass_iterator_free(self.0) }
    }
}

impl Iterator for MapIterator {
    type Item = (Value, Value);
    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        unsafe {
            match cass_iterator_next(self.0) {
                cass_false => None,
                cass_true => Some(self.get_pair()),
            }
        }
    }
}
