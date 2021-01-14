use crate::cassandra::data_type::ConstDataType;
use crate::cassandra::iterator::FieldIterator;

use crate::cassandra::schema::function_meta::FunctionMeta;
use crate::cassandra::util::{Protected, ProtectedInner};
use crate::cassandra::value::Value;

use crate::cassandra_sys::cass_aggregate_meta_argument_count;
use crate::cassandra_sys::cass_aggregate_meta_argument_type;
use crate::cassandra_sys::cass_aggregate_meta_field_by_name_n;
use crate::cassandra_sys::cass_aggregate_meta_final_func;
use crate::cassandra_sys::cass_aggregate_meta_full_name;
use crate::cassandra_sys::cass_aggregate_meta_init_cond;
use crate::cassandra_sys::cass_aggregate_meta_name;
use crate::cassandra_sys::cass_aggregate_meta_return_type;
use crate::cassandra_sys::cass_aggregate_meta_state_func;
use crate::cassandra_sys::cass_aggregate_meta_state_type;
use crate::cassandra_sys::cass_iterator_fields_from_aggregate_meta;
use crate::cassandra_sys::raw2utf8;
use crate::cassandra_sys::CassAggregateMeta as _CassAggregateMeta;
use std::mem;
use std::os::raw::c_char;

/// Metadata about a cassandra aggregate
#[derive(Debug)]
pub struct AggregateMeta(*const _CassAggregateMeta);

impl ProtectedInner<*const _CassAggregateMeta> for AggregateMeta {
    fn inner(&self) -> *const _CassAggregateMeta {
        self.0
    }
}

impl Protected<*const _CassAggregateMeta> for AggregateMeta {
    fn build(inner: *const _CassAggregateMeta) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        AggregateMeta(inner)
    }
}

impl AggregateMeta {
    /// An iterator over the fields of an aggregate
    pub fn fields_iter(&self) -> FieldIterator {
        unsafe { FieldIterator::build(cass_iterator_fields_from_aggregate_meta(self.0)) }
    }

    /// Gets the name of the aggregate.
    pub fn get_name(&self) -> String {
        unsafe {
            let mut name = mem::zeroed();
            let mut name_length = mem::zeroed();
            cass_aggregate_meta_name(self.0, &mut name, &mut name_length);
            raw2utf8(name, name_length).expect("must be utf8")
        }
    }

    /// Gets the full name of the aggregate.
    pub fn full_name(&self) -> String {
        unsafe {
            let mut name = mem::zeroed();
            let mut name_length = mem::zeroed();
            cass_aggregate_meta_full_name(self.0, &mut name, &mut name_length);
            raw2utf8(name, name_length).expect("must be utf8")
        }
    }

    /// Gets the number of arguments this aggregate takes.
    pub fn argument_count(&self) -> usize {
        unsafe { cass_aggregate_meta_argument_count(self.0) }
    }

    /// Gets the aggregate's argument type for the provided index.
    pub fn argument_type(&self, index: usize) -> ConstDataType {
        // TODO: can return NULL
        unsafe { ConstDataType::build(cass_aggregate_meta_argument_type(self.0, index)) }
    }

    /// Gets the aggregate's argument return type.
    pub fn return_type(&self) -> ConstDataType {
        unsafe { ConstDataType::build(cass_aggregate_meta_return_type(self.0)) }
    }

    /// Gets the aggregate's argument state type.
    pub fn state_type(&self) -> ConstDataType {
        unsafe { ConstDataType::build(cass_aggregate_meta_state_type(self.0)) }
    }

    /// Gets the function metadata for the aggregate's state function.
    pub fn state_func(&self) -> FunctionMeta {
        unsafe { FunctionMeta::build(cass_aggregate_meta_state_func(self.0)) }
    }

    /// Gets the function metadata for the aggregates's final function.
    pub fn final_func(&self) -> FunctionMeta {
        unsafe { FunctionMeta::build(cass_aggregate_meta_final_func(self.0)) }
    }

    ///  Gets the initial condition value for the aggregate.
    pub fn init_cond(&self) -> Value {
        unsafe { Value::build(cass_aggregate_meta_init_cond(self.0)) }
    }

    ///  Gets a metadata field for the provided name. Metadata fields allow direct
    /// access to the column data found in the underlying "aggregates" metadata table.
    pub fn field_by_name(&self, name: &str) -> Option<Value> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            let agg = cass_aggregate_meta_field_by_name_n(self.0, name_ptr, name.len());
            if agg.is_null() {
                None
            } else {
                Some(Value::build(agg))
            }
        }
    }
}
