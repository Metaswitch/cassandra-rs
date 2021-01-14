use crate::cassandra::data_type::ConstDataType;

use crate::cassandra::iterator::FieldIterator;
use crate::cassandra::util::{Protected, ProtectedInner};
use crate::cassandra::value::Value;
use crate::cassandra_sys::cass_column_meta_data_type;
use crate::cassandra_sys::cass_column_meta_field_by_name_n;
use crate::cassandra_sys::cass_column_meta_name;
use crate::cassandra_sys::cass_column_meta_type;
use crate::cassandra_sys::cass_iterator_fields_from_column_meta;
use crate::cassandra_sys::CassColumnMeta as _CassColumnMeta;
use crate::cassandra_sys::CassColumnType as _CassColumnType;

/// Column metadata
#[derive(Debug)]
pub struct ColumnMeta(*const _CassColumnMeta);

use std::mem;
use std::os::raw::c_char;
use std::slice;
use std::str;

impl ProtectedInner<*const _CassColumnMeta> for ColumnMeta {
    fn inner(&self) -> *const _CassColumnMeta {
        self.0
    }
}

impl Protected<*const _CassColumnMeta> for ColumnMeta {
    fn build(inner: *const _CassColumnMeta) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        ColumnMeta(inner)
    }
}

impl ColumnMeta {
    /// returns an iterator over the fields of this column
    pub fn field_iter(&mut self) -> FieldIterator {
        unsafe { FieldIterator::build(cass_iterator_fields_from_column_meta(self.0)) }
    }

    /// Gets the name of the column.
    pub fn name(&self) -> String {
        unsafe {
            let mut name = mem::zeroed();
            let mut name_length = mem::zeroed();
            cass_column_meta_name(self.0, &mut name, &mut name_length);
            let slice = slice::from_raw_parts(name as *const u8, name_length as usize);
            str::from_utf8(slice).expect("must be utf8").to_owned()
        }
    }

    /// Gets the type of the column.
    pub fn get_type(&self) -> _CassColumnType {
        unsafe { cass_column_meta_type(self.0) }
    }

    /// Gets the data type of the column.
    pub fn data_type(&self) -> ConstDataType {
        unsafe { ConstDataType::build(cass_column_meta_data_type(self.0)) }
    }

    /// Gets a metadata field for the provided name. Metadata fields allow direct
    /// access to the column data found in the underlying "columns" metadata table.
    pub fn field_by_name(&self, name: &str) -> Option<Value> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            let field = cass_column_meta_field_by_name_n(self.0, name_ptr, name.len());
            if field.is_null() {
                None
            } else {
                Some(Value::build(field))
            }
        }
    }
}
