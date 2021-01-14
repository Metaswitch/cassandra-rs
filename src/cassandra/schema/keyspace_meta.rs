use crate::cassandra::data_type::ConstDataType;
use crate::cassandra::iterator::AggregateIterator;
use crate::cassandra::iterator::FieldIterator;
use crate::cassandra::iterator::FunctionIterator;
use crate::cassandra::iterator::TableIterator;
use crate::cassandra::iterator::UserTypeIterator;
use crate::cassandra::schema::aggregate_meta::AggregateMeta;

use crate::cassandra::schema::function_meta::FunctionMeta;
use crate::cassandra::schema::table_meta::TableMeta;
use crate::cassandra::util::{Protected, ProtectedInner};

use crate::cassandra_sys::cass_iterator_aggregates_from_keyspace_meta;
use crate::cassandra_sys::cass_iterator_fields_from_keyspace_meta;
use crate::cassandra_sys::cass_iterator_functions_from_keyspace_meta;
use crate::cassandra_sys::CassKeyspaceMeta as _CassKeyspaceMeta;
use crate::cassandra_sys::CassValue as _CassValue;

use crate::cassandra_sys::cass_iterator_tables_from_keyspace_meta;
use crate::cassandra_sys::cass_iterator_user_types_from_keyspace_meta;
use crate::cassandra_sys::cass_keyspace_meta_aggregate_by_name_n;
use crate::cassandra_sys::cass_keyspace_meta_field_by_name_n;
use crate::cassandra_sys::cass_keyspace_meta_function_by_name_n;
use crate::cassandra_sys::cass_keyspace_meta_name;
use crate::cassandra_sys::cass_keyspace_meta_table_by_name_n;
use crate::cassandra_sys::cass_keyspace_meta_user_type_by_name_n;
use crate::cassandra_sys::raw2utf8;
use std::mem;
use std::os::raw::c_char;

/// A snapshot of the schema's metadata.
#[derive(Debug)]
pub struct KeyspaceMeta(*const _CassKeyspaceMeta);

impl ProtectedInner<*const _CassKeyspaceMeta> for KeyspaceMeta {
    fn inner(&self) -> *const _CassKeyspaceMeta {
        self.0
    }
}

impl Protected<*const _CassKeyspaceMeta> for KeyspaceMeta {
    fn build(inner: *const _CassKeyspaceMeta) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        KeyspaceMeta(inner)
    }
}

#[derive(Debug)]
pub struct MetadataFieldValue(*const _CassValue);

impl KeyspaceMeta {
    /// Iterator over the aggregates in this keyspace
    pub fn aggregrates_iter(&self) -> AggregateIterator {
        unsafe { AggregateIterator::build(cass_iterator_aggregates_from_keyspace_meta(self.0)) }
    }

    /// Iterator over the field in this keyspace
    pub fn fields_iter(&self) -> FieldIterator {
        unsafe { FieldIterator::build(cass_iterator_fields_from_keyspace_meta(self.0)) }
    }

    /// Gets the table metadata for the provided table name.
    pub fn table_by_name(&self, name: &str) -> Option<TableMeta> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            let value = cass_keyspace_meta_table_by_name_n(self.0, name_ptr, name.len());
            if value.is_null() {
                None
            } else {
                Some(TableMeta::build(value))
            }
        }
    }

    /// Gets the data type for the provided type name.
    pub fn user_type_by_name(&self, name: &str) -> Option<ConstDataType> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            let value = cass_keyspace_meta_user_type_by_name_n(self.0, name_ptr, name.len());
            if value.is_null() {
                None
            } else {
                Some(ConstDataType::build(value))
            }
        }
    }

    /// Gets the function metadata for the provided function name.
    pub fn get_function_by_name(&self, name: &str, arguments: Vec<&str>) -> Option<FunctionMeta> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            let arguments_str = arguments.join(",");
            let arguments_ptr = arguments_str.as_ptr() as *const c_char;
            let value = cass_keyspace_meta_function_by_name_n(
                self.0,
                name_ptr,
                name.len(),
                arguments_ptr,
                arguments_str.len(),
            );
            if value.is_null() {
                None
            } else {
                Some(FunctionMeta::build(value))
            }
        }
    }

    /// Gets the aggregate metadata for the provided aggregate name.
    pub fn aggregate_by_name(&self, name: &str, arguments: Vec<&str>) -> Option<AggregateMeta> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            let arguments_str = arguments.join(",");
            let arguments_ptr = arguments_str.as_ptr() as *const c_char;
            let agg = cass_keyspace_meta_aggregate_by_name_n(
                self.0,
                name_ptr,
                name.len(),
                arguments_ptr,
                arguments_str.len(),
            );
            if agg.is_null() {
                None
            } else {
                Some(AggregateMeta::build(agg))
            }
        }
    }

    /// Iterator over the tables in this keyspaces
    pub fn table_iter(&mut self) -> TableIterator {
        unsafe { TableIterator::build(cass_iterator_tables_from_keyspace_meta(self.0)) }
    }

    /// Iterator over the functions in this keyspaces
    pub fn function_iter(&mut self) -> FunctionIterator {
        unsafe { FunctionIterator::build(cass_iterator_functions_from_keyspace_meta(self.0)) }
    }

    /// Iterator over the UDTs in this keyspaces
    pub fn user_type_iter(&mut self) -> UserTypeIterator {
        unsafe { UserTypeIterator::build(cass_iterator_user_types_from_keyspace_meta(self.0)) }
    }

    /// Gets the name of the keyspace.
    pub fn name(&self) -> String {
        unsafe {
            let mut name = mem::zeroed();
            let mut name_length = mem::zeroed();
            cass_keyspace_meta_name(self.0, &mut name, &mut name_length);
            raw2utf8(name, name_length).expect("must be utf8")
        }
    }

    /// Gets a metadata field for the provided name. Metadata fields allow direct
    /// access to the column data found in the underlying "keyspaces" metadata table.
    pub fn field_by_name(&self, name: &str) -> Option<MetadataFieldValue> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            let value = cass_keyspace_meta_field_by_name_n(self.0, name_ptr, name.len());
            if value.is_null() {
                None
            } else {
                Some(MetadataFieldValue(value))
            }
        }
    }
}
