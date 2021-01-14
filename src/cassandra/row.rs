use crate::cassandra::error::*;

use crate::cassandra::inet::Inet;
use crate::cassandra::iterator::{MapIterator, SetIterator, UserTypeIterator};
use crate::cassandra::result::CassResult;
use crate::cassandra::util::{Protected, ProtectedInner};
use crate::cassandra::uuid::Uuid;
use crate::cassandra::value::Value;
use crate::cassandra_sys::cass_false;
use crate::cassandra_sys::cass_iterator_free;
use crate::cassandra_sys::cass_iterator_from_row;
use crate::cassandra_sys::cass_iterator_get_column;
use crate::cassandra_sys::cass_iterator_next;
use crate::cassandra_sys::cass_row_get_column;
use crate::cassandra_sys::cass_row_get_column_by_name_n;
use crate::cassandra_sys::cass_true;
use crate::cassandra_sys::CassIterator as _CassIterator;
use crate::cassandra_sys::CassRow as _Row;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::iter;
use std::iter::IntoIterator;
use std::marker::PhantomData;
use std::os::raw::c_char;

/// A collection of column values. Read-only, so thread-safe.
pub struct Row<'a>(*const _Row, PhantomData<&'a CassResult>);

unsafe impl<'a> Sync for Row<'a> {}
unsafe impl<'a> Send for Row<'a> {}

impl<'a> ProtectedInner<*const _Row> for Row<'a> {
    fn inner(&self) -> *const _Row {
        self.0
    }
}

impl<'a> Protected<*const _Row> for Row<'a> {
    fn build(inner: *const _Row) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        Row(inner, PhantomData)
    }
}

impl<'a> Debug for Row<'a> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        for column in self {
            write!(f, "{:?}\t", Value::build(column.inner()))?;
        }
        Ok(())
    }
}

impl<'a> Display for Row<'a> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        for column in self {
            write!(f, "{}\t", Value::build(column.inner()))?;
        }
        Ok(())
    }
}

/// Auto inferencing conversion from c* to rust
pub trait AsRustType<T> {
    /// convert while reading cassandra columns
    fn get(&self, index: usize) -> Result<T>;

    /// convert while reading cassandra columns by name
    fn get_by_name<S>(&self, name: S) -> Result<T>
    where
        S: Into<String>;
}

impl AsRustType<bool> for Row<'_> {
    fn get(&self, index: usize) -> Result<bool> {
        let col = self.get_column(index)?;
        col.get_bool()
    }

    fn get_by_name<S>(&self, name: S) -> Result<bool>
    where
        S: Into<String>,
    {
        self.get_column_by_name(name)?.get_bool()
    }
}

impl AsRustType<String> for Row<'_> {
    fn get(&self, index: usize) -> Result<String> {
        let col = self.get_column(index)?;
        col.get_string()
    }

    fn get_by_name<S>(&self, name: S) -> Result<String>
    where
        S: Into<String>,
    {
        let col = self.get_column_by_name(name)?;
        col.get_string()
    }
}

impl AsRustType<f64> for Row<'_> {
    fn get(&self, index: usize) -> Result<f64> {
        let col = self.get_column(index)?;
        col.get_f64()
    }

    fn get_by_name<S>(&self, name: S) -> Result<f64>
    where
        S: Into<String>,
    {
        let col = self.get_column_by_name(name)?;
        col.get_f64()
    }
}

impl AsRustType<f32> for Row<'_> {
    fn get(&self, index: usize) -> Result<f32> {
        let col = self.get_column(index)?;
        col.get_f32()
    }

    fn get_by_name<S>(&self, name: S) -> Result<f32>
    where
        S: Into<String>,
    {
        let col = self.get_column_by_name(name)?;
        col.get_f32()
    }
}

impl AsRustType<i64> for Row<'_> {
    fn get(&self, index: usize) -> Result<i64> {
        let col = self.get_column(index)?;
        col.get_i64()
    }

    fn get_by_name<S>(&self, name: S) -> Result<i64>
    where
        S: Into<String>,
    {
        let col = self.get_column_by_name(name)?;
        col.get_i64()
    }
}

impl AsRustType<i32> for Row<'_> {
    fn get(&self, index: usize) -> Result<i32> {
        let col = self.get_column(index)?;
        col.get_i32()
    }

    fn get_by_name<S>(&self, name: S) -> Result<i32>
    where
        S: Into<String>,
    {
        let col = self.get_column_by_name(name)?;
        col.get_i32()
    }
}

impl AsRustType<i16> for Row<'_> {
    fn get(&self, index: usize) -> Result<i16> {
        let col = self.get_column(index)?;
        col.get_i16()
    }

    fn get_by_name<S>(&self, name: S) -> Result<i16>
    where
        S: Into<String>,
    {
        let col = self.get_column_by_name(name)?;
        col.get_i16()
    }
}

impl AsRustType<i8> for Row<'_> {
    fn get(&self, index: usize) -> Result<i8> {
        let col = self.get_column(index)?;
        col.get_i8()
    }

    fn get_by_name<S>(&self, name: S) -> Result<i8>
    where
        S: Into<String>,
    {
        let col = self.get_column_by_name(name)?;
        col.get_i8()
    }
}

impl AsRustType<u32> for Row<'_> {
    fn get(&self, index: usize) -> Result<u32> {
        let col = self.get_column(index)?;
        col.get_u32()
    }

    fn get_by_name<S>(&self, name: S) -> Result<u32>
    where
        S: Into<String>,
    {
        let col = self.get_column_by_name(name)?;
        col.get_u32()
    }
}

impl AsRustType<Inet> for Row<'_> {
    fn get(&self, index: usize) -> Result<Inet> {
        let col = self.get_column(index)?;
        col.get_inet()
    }

    fn get_by_name<S>(&self, name: S) -> Result<Inet>
    where
        S: Into<String>,
    {
        let col = self.get_column_by_name(name)?;
        col.get_inet()
    }
}

impl AsRustType<SetIterator> for Row<'_> {
    fn get(&self, index: usize) -> Result<SetIterator> {
        let col = self.get_column(index)?;
        col.get_set()
    }

    fn get_by_name<S>(&self, name: S) -> Result<SetIterator>
    where
        S: Into<String>,
    {
        let col = self.get_column_by_name(name)?;
        col.get_set()
    }
}

impl AsRustType<MapIterator> for Row<'_> {
    fn get(&self, index: usize) -> Result<MapIterator> {
        let col = self.get_column(index)?;
        col.get_map()
    }

    fn get_by_name<S>(&self, name: S) -> Result<MapIterator>
    where
        S: Into<String>,
    {
        let col = self.get_column_by_name(name)?;
        col.get_map()
    }
}

impl AsRustType<UserTypeIterator> for Row<'_> {
    fn get(&self, index: usize) -> Result<UserTypeIterator> {
        let col = self.get_column(index)?;
        col.get_user_type()
    }

    fn get_by_name<S>(&self, name: S) -> Result<UserTypeIterator>
    where
        S: Into<String>,
    {
        let col = self.get_column_by_name(name)?;
        col.get_user_type()
    }
}

impl AsRustType<Uuid> for Row<'_> {
    fn get(&self, index: usize) -> Result<Uuid> {
        let col = self.get_column(index)?;
        col.get_uuid()
    }

    fn get_by_name<S>(&self, name: S) -> Result<Uuid>
    where
        S: Into<String>,
    {
        let col = self.get_column_by_name(name)?;
        col.get_uuid()
    }
}

impl AsRustType<uuid::Uuid> for Row<'_> {
    fn get(&self, index: usize) -> Result<uuid::Uuid> {
        let col = self.get_column(index)?;
        col.get_uuid().map(|x| x.into())
    }

    fn get_by_name<S>(&self, name: S) -> Result<uuid::Uuid>
    where
        S: Into<String>,
    {
        let col = self.get_column_by_name(name)?;
        col.get_uuid().map(|x| x.into())
    }
}

impl AsRustType<Vec<u8>> for Row<'_> {
    fn get(&self, index: usize) -> Result<Vec<u8>> {
        let col = self.get_column(index)?;
        col.get_bytes().map(|b| b.to_vec())
    }

    fn get_by_name<S>(&self, name: S) -> Result<Vec<u8>>
    where
        S: Into<String>,
    {
        let col = self.get_column_by_name(name)?;
        col.get_bytes().map(|b| b.to_vec())
    }
}

impl<'a> Row<'a> {
    /// Get a particular column by index
    pub fn get_column(&self, index: usize) -> Result<Value> {
        unsafe {
            let col = cass_row_get_column(self.0, index);
            if col.is_null() {
                Err(CassErrorCode::LIB_INDEX_OUT_OF_BOUNDS.to_error())
            } else {
                Ok(Value::build(col))
            }
        }
    }

    /// Get a particular column by name
    pub fn get_column_by_name<S>(&self, name: S) -> Result<Value>
    where
        S: Into<String>,
    {
        unsafe {
            let name_str = name.into();
            let name_ptr = name_str.as_ptr() as *const c_char;
            let col = cass_row_get_column_by_name_n(self.0, name_ptr, name_str.len());
            if col.is_null() {
                Err(CassErrorCode::LIB_INDEX_OUT_OF_BOUNDS.to_error())
            } else {
                Ok(Value::build(col))
            }
        }
    }
}

/// An iterator over the columns in a row
#[derive(Debug)]
pub struct RowIterator(pub *mut _CassIterator);

// The underlying C type has no thread-local state, but does not support access
// from multiple threads: https://datastax.github.io/cpp-driver/topics/#thread-safety
unsafe impl Send for RowIterator {}

impl Drop for RowIterator {
    fn drop(&mut self) {
        unsafe { cass_iterator_free(self.0) }
    }
}

impl iter::Iterator for RowIterator {
    type Item = Value;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        unsafe {
            match cass_iterator_next(self.0) {
                cass_false => None,
                cass_true => Some(Value::build(cass_iterator_get_column(self.0))),
            }
        }
    }
}

impl<'a> Iterator for &'a RowIterator {
    type Item = Value;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        unsafe {
            match cass_iterator_next(self.0) {
                cass_false => None,
                cass_true => Some(Value::build(cass_iterator_get_column(self.0))),
            }
        }
    }
}

impl Display for RowIterator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        for item in self {
            write!(f, "{}\t", Value::build(item.inner()))?;
        }
        Ok(())
    }
}

impl IntoIterator for Row<'_> {
    type Item = Value;
    type IntoIter = RowIterator;

    /// Creates a new iterator for the specified row. This can be
    /// used to iterate over columns in a row.
    fn into_iter(self) -> Self::IntoIter {
        unsafe { RowIterator(cass_iterator_from_row(self.0)) }
    }
}

impl<'a> IntoIterator for &'a Row<'_> {
    type Item = Value;
    type IntoIter = RowIterator;
    fn into_iter(self) -> Self::IntoIter {
        unsafe { RowIterator(cass_iterator_from_row(self.0)) }
    }
}
