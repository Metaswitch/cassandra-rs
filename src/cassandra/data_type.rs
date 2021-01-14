use crate::cassandra::error::*;
use crate::cassandra::user_type::UserType;
use crate::cassandra::util::{Protected, ProtectedInner};
use crate::cassandra::value::ValueType;

use crate::cassandra_sys::cass_data_sub_type_count;
use crate::cassandra_sys::cass_data_type_add_sub_type;
use crate::cassandra_sys::cass_data_type_add_sub_type_by_name_n;
use crate::cassandra_sys::cass_data_type_add_sub_value_type;
use crate::cassandra_sys::cass_data_type_add_sub_value_type_by_name_n;
use crate::cassandra_sys::cass_data_type_class_name;
use crate::cassandra_sys::cass_data_type_free;
use crate::cassandra_sys::cass_data_type_keyspace;
use crate::cassandra_sys::cass_data_type_new;
use crate::cassandra_sys::cass_data_type_new_from_existing;
use crate::cassandra_sys::cass_data_type_new_tuple;
use crate::cassandra_sys::cass_data_type_new_udt;
use crate::cassandra_sys::cass_data_type_set_class_name_n;
use crate::cassandra_sys::cass_data_type_set_keyspace_n;
use crate::cassandra_sys::cass_data_type_set_type_name_n;
use crate::cassandra_sys::cass_data_type_sub_data_type;
use crate::cassandra_sys::cass_data_type_sub_data_type_by_name_n;
use crate::cassandra_sys::cass_data_type_sub_type_name;
use crate::cassandra_sys::cass_data_type_type;
use crate::cassandra_sys::cass_data_type_type_name;
use crate::cassandra_sys::cass_user_type_new_from_data_type;
use crate::cassandra_sys::CassDataType as _CassDataType;

use std::ffi::CString;
use std::os::raw::c_char;

/// Any cassandra datatype
#[derive(Debug)]
pub struct DataType(*mut _CassDataType);
#[derive(Debug)]
pub struct ConstDataType(*const _CassDataType);

// The underlying C types have no thread-local state, but do not support access
// from multiple threads: https://datastax.github.io/cpp-driver/topics/#thread-safety
unsafe impl Send for DataType {}
unsafe impl Send for ConstDataType {}

impl ProtectedInner<*mut _CassDataType> for DataType {
    fn inner(&self) -> *mut _CassDataType {
        self.0
    }
}

impl Protected<*mut _CassDataType> for DataType {
    fn build(inner: *mut _CassDataType) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        DataType(inner)
    }
}

impl ProtectedInner<*const _CassDataType> for ConstDataType {
    fn inner(&self) -> *const _CassDataType {
        self.0
    }
}

impl Protected<*const _CassDataType> for ConstDataType {
    fn build(inner: *const _CassDataType) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        ConstDataType(inner)
    }
}

impl Drop for DataType {
    /// Frees a data type instance.
    fn drop(&mut self) {
        unsafe { cass_data_type_free(self.0) }
    }
}

impl ConstDataType {
    /// Creates a new user defined type from existing data type.
    pub fn new_user_type(&self) -> UserType {
        unsafe { UserType::build(cass_user_type_new_from_data_type(self.0)) }
    }
}

impl DataType {
    /// Creates a new data type with value type.
    pub fn new(value_type: ValueType) -> Self {
        unsafe { DataType(cass_data_type_new(value_type.inner())) }
    }

    /// Creates a new data type from an existing data type.
    // TODO: can return NULL
    pub fn new_user_type(&self) -> UserType {
        unsafe { UserType::build(cass_user_type_new_from_data_type(self.0)) }
    }

    /// Creates a new data type from an existing data type.
    pub fn new_from_existing(&self) -> Self {
        unsafe { DataType(cass_data_type_new_from_existing(self.0)) }
    }

    /// Creates a new tuple data type.
    pub fn new_tuple(item_count: usize) -> Self {
        unsafe { DataType(cass_data_type_new_tuple(item_count)) }
    }

    /// Creates a new UDT (user defined type) data type.
    pub fn new_udt(field_count: usize) -> DataType {
        unsafe { DataType(cass_data_type_new_udt(field_count)) }
    }

    /// Gets the value type of the specified data type.
    pub fn get_type(data_type: DataType) -> ValueType {
        unsafe { ValueType::build(cass_data_type_type(data_type.0)) }
    }

    /// Gets the type name of a UDT data type.
    pub fn type_name<S>(data_type: DataType, type_name: S) -> Result<()>
    where
        S: Into<String>,
    {
        unsafe {
            let type_name2 = CString::new(type_name.into())?;
            let err = cass_data_type_type_name(
                data_type.0,
                &mut type_name2.as_ptr(),
                &mut (type_name2.as_bytes().len()),
            );
            err.to_result(())
        }
    }

    /// Sets the type name of a UDT data type.
    ///
    /// <b>Note:</b> Only valid for UDT data types.
    pub fn set_type_name<S>(data_type: DataType, type_name: S) -> Result<()>
    where
        S: Into<String>,
    {
        unsafe {
            let type_name_str = type_name.into();
            let type_name_ptr = type_name_str.as_ptr() as *const c_char;
            cass_data_type_set_type_name_n(data_type.0, type_name_ptr, type_name_str.len())
                .to_result(())
        }
    }

    /// Gets the type name of a UDT data type.
    ///
    /// <b>Note:</b> Only valid for UDT data types.
    pub fn keyspace<S>(data_type: DataType, keyspace: S) -> Result<()>
    where
        S: Into<String>,
    {
        unsafe {
            let keyspace2 = CString::new(keyspace.into())?;
            cass_data_type_keyspace(
                data_type.0,
                &mut (keyspace2.as_ptr()),
                &mut (keyspace2.as_bytes().len()),
            )
            .to_result(())
        }
    }

    /// Sets the keyspace of a UDT data type.
    ///
    /// <b>Note:</b> Only valid for UDT data types.
    pub fn set_keyspace<S>(data_type: DataType, keyspace: S) -> Result<()>
    where
        S: Into<String>,
    {
        unsafe {
            let keyspace_str = keyspace.into();
            let keyspace_ptr = keyspace_str.as_ptr() as *const c_char;
            cass_data_type_set_keyspace_n(data_type.0, keyspace_ptr, keyspace_str.len())
                .to_result(())
        }
    }

    /// Gets the class name of a custom data type.
    ///
    /// <b>Note:</b> Only valid for custom data types.
    pub fn class_name<S>(data_type: DataType, class_name: S) -> Result<()>
    where
        S: Into<String>,
    {
        unsafe {
            let class_name2 = CString::new(class_name.into())?;
            cass_data_type_class_name(
                data_type.0,
                &mut class_name2.as_ptr(),
                &mut (class_name2.as_bytes().len()),
            )
            .to_result(())
        }
    }

    /// Sets the class name of a custom data type.
    ///
    /// <b>Note:</b> Only valid for custom data types.
    pub fn set_class_name<S>(&self, class_name: S) -> Result<()>
    where
        S: Into<String>,
    {
        unsafe {
            let class_name_str = class_name.into();
            let class_name_ptr = class_name_str.as_ptr() as *const c_char;
            cass_data_type_set_class_name_n(self.0, class_name_ptr, class_name_str.len())
                .to_result(())
        }
    }

    /// Gets the sub-data type count of a UDT (user defined type), tuple
    /// or collection.
    ///
    /// <b>Note:</b> Only valid for UDT, tuple and collection data types.
    pub fn sub_type_count<S>(&self) -> usize {
        unsafe { cass_data_sub_type_count(self.0) }
    }

    /// Gets the sub-data type of a UDT (user defined type), tuple or collection at
    /// the specified index.
    ///
    /// <b>Note:</b> Only valid for UDT, tuple and collection data types.
    pub fn sub_data_type(&self, index: usize) -> ConstDataType {
        // TODO: can return NULL
        unsafe { ConstDataType::build(cass_data_type_sub_data_type(self.0, index)) }
    }

    /// Gets the sub-data type of a UDT (user defined type) at the specified index.
    ///
    /// <b>Note:</b> Only valid for UDT data types.
    pub fn sub_data_type_by_name<S>(data_type: DataType, name: S) -> ConstDataType
    where
        S: Into<String>,
    {
        unsafe {
            let name_str = name.into();
            let name_ptr = name_str.as_ptr() as *const c_char;
            // TODO: can return NULL
            ConstDataType::build(cass_data_type_sub_data_type_by_name_n(
                data_type.0,
                name_ptr,
                name_str.len(),
            ))
        }
    }

    /// Gets the sub-type name of a UDT (user defined type) at the specified index.
    ///
    /// <b>Note:</b> Only valid for UDT data types.
    pub fn sub_type_name<S>(data_type: DataType, index: usize, name: S) -> Result<()>
    where
        S: Into<String>,
    {
        unsafe {
            let name2 = CString::new(name.into())?;
            cass_data_type_sub_type_name(
                data_type.0,
                index,
                &mut name2.as_ptr(),
                &mut (name2.as_bytes().len()),
            )
            .to_result(())
        }
    }

    /// Adds a sub-data type to a tuple or collection.
    ///
    /// <b>Note:</b> Only valid for tuple and collection data types.
    pub fn add_sub_type(&self, sub_data_type: DataType) -> Result<()> {
        unsafe { cass_data_type_add_sub_type(self.0, sub_data_type.0).to_result(()) }
    }

    /// Gets the sub-data type of a UDT (user defined type) at the specified index.
    ///
    /// <b>Note:</b> Only valid for UDT data types.
    pub fn add_sub_type_by_name<S>(&mut self, name: S, sub_data_type: DataType) -> Result<()>
    where
        S: Into<String>,
    {
        unsafe {
            let name_str = name.into();
            let name_ptr = name_str.as_ptr() as *const c_char;
            cass_data_type_add_sub_type_by_name_n(self.0, name_ptr, name_str.len(), sub_data_type.0)
                .to_result(())
        }
    }

    /// Adds a sub-data type to a tuple or collection using a value type.
    ///
    /// <b>Note:</b> Only valid for tuple and collection data types.
    pub fn add_sub_value_type<S>(&self, sub_value_type: ValueType) -> Result<()>
    where
        S: Into<String>,
    {
        unsafe { cass_data_type_add_sub_value_type(self.0, sub_value_type.inner()).to_result(()) }
    }

    /// Adds a sub-data type to a tuple or collection using a value type.
    ///
    /// <b>Note:</b> Only valid for tuple and collection data types.
    pub fn add_sub_value_type_by_name<S>(&self, name: S, typ: ValueType) -> Result<()>
    where
        S: Into<String>,
    {
        unsafe {
            let name_str = name.into();
            let name_ptr = name_str.as_ptr() as *const c_char;
            cass_data_type_add_sub_value_type_by_name_n(
                self.0,
                name_ptr,
                name_str.len(),
                typ.inner(),
            )
            .to_result(())
        }
    }

    //    pub fn set_type_name_n<S>(data_type: DataType, type_name: S) -> Result<(), CassError>
    //        where S: Into<String>
    //    {
    //        unsafe {
    //            let type_name = CString::new(type_name.into()).unwrap();
    //            CassError::build(cass_data_type_set_type_name_n(data_type.0,
    //                                                            type_name.as_ptr(),
    //                                                            type_name.as_bytes().len() as u64))
    //                .wrap(())
    //        }
    //    }

    //    pub fn set_class_name_n<S>(data_type: DataType, class_name: S) -> Result<(), CassError>
    //        where S: Into<String>
    //    {
    //        unsafe {
    //            let class_name = CString::new(class_name.into()).unwrap();
    //            CassError::build(cass_data_type_set_class_name_n(data_type.0,
    //                                                             class_name.as_ptr(),
    //                                                             class_name.as_bytes().len() as u64))
    //                .wrap(())
    //        }
    //    }

    //    pub fn sub_data_type_by_name_n<S>(data_type: DataType, name: S) -> ConstDataType
    //        where S: Into<String>
    //    {
    //        unsafe {
    //            let name = CString::new(name.into()).unwrap();
    //            // TODO: can return NULL
    //            ConstDataType::build(cass_data_type_sub_data_type_by_name_n(data_type.0,
    //                                                                 name.as_ptr(),
    //                                                                 name.as_bytes().len() as u64))
    //        }
    //    }
}
