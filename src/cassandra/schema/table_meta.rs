use crate::cassandra::iterator::ColumnIterator;
use crate::cassandra::iterator::FieldIterator;

use crate::cassandra::schema::column_meta::ColumnMeta;
use crate::cassandra::util::{Protected, ProtectedInner};
use crate::cassandra::value::Value;
use crate::cassandra_sys::cass_iterator_columns_from_table_meta;
use crate::cassandra_sys::cass_iterator_fields_from_table_meta;
use crate::cassandra_sys::cass_table_meta_clustering_key;
use crate::cassandra_sys::cass_table_meta_clustering_key_count;
use crate::cassandra_sys::cass_table_meta_column;
use crate::cassandra_sys::cass_table_meta_column_by_name;
use crate::cassandra_sys::cass_table_meta_column_count;
use crate::cassandra_sys::cass_table_meta_field_by_name;
use crate::cassandra_sys::cass_table_meta_name;
use crate::cassandra_sys::cass_table_meta_partition_key;
use crate::cassandra_sys::cass_table_meta_partition_key_count;
use crate::cassandra_sys::CassTableMeta as _CassTableMeta;
use std::mem;
use std::slice;

use std::str;

/// Table metadata
#[derive(Debug)]
pub struct TableMeta(*const _CassTableMeta);

impl ProtectedInner<*const _CassTableMeta> for TableMeta {
    fn inner(&self) -> *const _CassTableMeta {
        self.0
    }
}

impl Protected<*const _CassTableMeta> for TableMeta {
    fn build(inner: *const _CassTableMeta) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        TableMeta(inner)
    }
}

impl TableMeta {
    /// returns an iterator over the fields of this table
    pub fn field_iter(&mut self) -> FieldIterator {
        unsafe { FieldIterator::build(cass_iterator_fields_from_table_meta(self.0)) }
    }

    /// An iterator over the columns in this table
    pub fn columns_iter(&self) -> ColumnIterator {
        unsafe { ColumnIterator::build(cass_iterator_columns_from_table_meta(self.0)) }
    }

    /// Gets the column metadata for the provided column name.
    pub fn column_by_name(&self, name: &str) -> ColumnMeta {
        // TODO: can return NULL
        unsafe {
            ColumnMeta::build(cass_table_meta_column_by_name(
                self.0,
                name.as_ptr() as *const i8,
            ))
        }
    }

    /// Gets the name of the table.
    pub fn get_name(&self) -> String {
        unsafe {
            let mut name = mem::zeroed();
            let mut name_length = mem::zeroed();
            cass_table_meta_name(self.0, &mut name, &mut name_length);
            str::from_utf8(slice::from_raw_parts(
                name as *const u8,
                name_length as usize,
            ))
            .expect("must be utf8")
            .to_owned()
        }
    }

    /// Gets the total number of columns for the table.
    pub fn column_count(&self) -> usize {
        unsafe { cass_table_meta_column_count(self.0) }
    }

    /// Gets the column metadata for the provided index.
    pub fn column(&self, index: usize) -> ColumnMeta {
        // TODO: can return NULL
        unsafe { ColumnMeta::build(cass_table_meta_column(self.0, index)) }
    }

    /// Gets the number of columns for the table's partition key.
    pub fn partition_key_count(&self) -> usize {
        unsafe { cass_table_meta_partition_key_count(self.0) }
    }

    /// Gets the partition key column metadata for the provided index.
    pub fn partition_key(&self, index: usize) -> Option<ColumnMeta> {
        unsafe {
            let key = cass_table_meta_partition_key(self.0, index);
            if key.is_null() {
                None
            } else {
                Some(ColumnMeta::build(key))
            }
        }
    }

    /// Gets the number of columns for the table's clustering key
    pub fn clustering_key_count(&self) -> usize {
        unsafe { cass_table_meta_clustering_key_count(self.0) }
    }

    /// Gets the clustering key column metadata for the provided index.
    pub fn cluster_key(&self, index: usize) -> Option<ColumnMeta> {
        unsafe {
            let key = cass_table_meta_clustering_key(self.0, index);
            if key.is_null() {
                None
            } else {
                Some(ColumnMeta::build(key))
            }
        }
    }

    /// Gets a metadata field for the provided name. Metadata fields allow direct
    /// access to the column data found in the underlying "tables" metadata table.
    pub fn field_by_name(&self, name: &str) -> Option<Value> {
        // fixme replace CassValule with a custom type
        unsafe {
            let value = cass_table_meta_field_by_name(self.0, name.as_ptr() as *const i8);
            if value.is_null() {
                None
            } else {
                Some(Value::build(value))
            }
        }
    }
}
