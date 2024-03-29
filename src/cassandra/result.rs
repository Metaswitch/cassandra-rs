#![allow(non_camel_case_types)]
#![allow(dead_code)]
#![allow(missing_copy_implementations)]

use crate::cassandra::data_type::ConstDataType;
use crate::cassandra::error::*;
use crate::cassandra::iterator::LendingIterator;
use crate::cassandra::row::Row;
use crate::cassandra::util::{Protected, ProtectedInner};
use crate::cassandra::value::ValueType;

use crate::cassandra_sys::cass_false;
use crate::cassandra_sys::cass_iterator_free;
use crate::cassandra_sys::cass_iterator_from_result;
use crate::cassandra_sys::cass_iterator_get_row;
use crate::cassandra_sys::cass_iterator_next;
use crate::cassandra_sys::cass_result_column_count;
use crate::cassandra_sys::cass_result_column_data_type;
use crate::cassandra_sys::cass_result_column_name;
use crate::cassandra_sys::cass_result_column_type;
use crate::cassandra_sys::cass_result_first_row;
use crate::cassandra_sys::cass_result_free;
use crate::cassandra_sys::cass_result_has_more_pages;
use crate::cassandra_sys::cass_result_paging_state_token;
use crate::cassandra_sys::cass_result_row_count;
use crate::cassandra_sys::cass_true;
use crate::cassandra_sys::CassIterator as _CassIterator;
use crate::cassandra_sys::CassResult as _CassResult;

use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::marker::PhantomData;

use std::slice;
use std::str;

/// The result of a query.
/// A result object is read-only and is thread-safe to read or iterate over
/// concurrently, since we do not bind any setters (e.g., `set_metadata`).
pub struct CassResult(*const _CassResult);
unsafe impl Sync for CassResult {}
unsafe impl Send for CassResult {}

impl ProtectedInner<*const _CassResult> for CassResult {
    fn inner(&self) -> *const _CassResult {
        self.0
    }
}

impl Protected<*const _CassResult> for CassResult {
    fn build(inner: *const _CassResult) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        CassResult(inner)
    }
}

impl Debug for CassResult {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        writeln!(f, "Result row count: {:?}", self.row_count())?;
        let mut iter = self.iter();
        while let Some(row) = iter.next() {
            writeln!(f, "{:?}", row)?;
        }
        Ok(())
    }
}

impl Display for CassResult {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        writeln!(f, "Result row count: {}", self.row_count())?;
        let mut iter = self.iter();
        while let Some(row) = iter.next() {
            writeln!(f, "{}", row)?;
        }
        Ok(())
    }
}

impl Drop for CassResult {
    fn drop(&mut self) {
        unsafe { cass_result_free(self.0) }
    }
}

impl CassResult {
    /// Gets the number of rows for the specified result.
    pub fn row_count(&self) -> u64 {
        unsafe { cass_result_row_count(self.0) as u64 }
    }

    /// Gets the number of columns per row for the specified result.
    pub fn column_count(&self) -> u64 {
        unsafe { cass_result_column_count(self.0) as u64 }
    }

    /// Gets the column name at index for the specified result.
    pub fn column_name(&self, index: usize) -> Result<&str> {
        let mut name = std::ptr::null();
        let mut name_length = 0;
        unsafe {
            cass_result_column_name(self.0, index, &mut name, &mut name_length)
                .to_result(())
                .and_then(|_| {
                    let slice = slice::from_raw_parts(name as *const u8, name_length);
                    Ok(str::from_utf8(slice)?)
                })
        }
    }

    /// Gets the column type at index for the specified result.
    pub fn column_type(&self, index: usize) -> ValueType {
        unsafe { ValueType::build(cass_result_column_type(self.0, index)) }
    }

    /// Gets the column datatype at index for the specified result.
    pub fn column_data_type(&self, index: usize) -> ConstDataType {
        // TODO: can return NULL
        unsafe { ConstDataType::build(cass_result_column_data_type(self.0, index)) }
    }

    /// Gets the first row of the result.
    pub fn first_row(&self) -> Option<Row> {
        unsafe {
            match self.row_count() {
                0 => None,
                _ => Some(Row::build(cass_result_first_row(self.0))),
            }
        }
    }

    /// Returns true if there are more pages.
    pub fn has_more_pages(&self) -> bool {
        unsafe { cass_result_has_more_pages(self.0) == cass_true }
    }

    /// Gets the statement's paging state. This can be used to get the next page of
    /// data in a multi-page query, by using `set_paging_state_token`.
    ///
    /// Returns:
    ///   - `Ok(None)` if there are no more pages, and thus no paging state token.
    ///   - `Ok(Some(Vec<u8>))` if there are more pages, and a paging state token.
    ///   - `Err(_)` if there was an error getting the paging state token.
    ///
    /// [`set_paging_state_token`]: Statement::set_paging_state_token
    pub fn paging_state_token(&self) -> Result<Option<Vec<u8>>> {
        if !self.has_more_pages() {
            return Ok(None);
        }

        let mut token_ptr = std::ptr::null();
        let mut token_length = 0;
        unsafe {
            cass_result_paging_state_token(self.0, &mut token_ptr, &mut token_length)
                .to_result(())
                .map(|_| Some(slice::from_raw_parts(token_ptr as *const u8, token_length).to_vec()))
        }
    }

    /// Creates a new iterator for the specified result. This can be
    /// used to iterate over rows in the result.
    pub fn iter(&self) -> ResultIterator {
        unsafe {
            ResultIterator(
                cass_iterator_from_result(self.0),
                cass_result_row_count(self.0),
                PhantomData,
            )
        }
    }
}

/// An iterator over the results of a query. The result holds the data, so
/// the result must last for at least the lifetime of the iterator.
///
/// This is a lending iterator (you must stop using each item before you move to
/// the next), and so it does not implement `std::iter::Iterator`. The best way
/// to use it is as follows:
///
/// ```no_run
/// # use cassandra_cpp::*;
/// # let result: CassResult = unimplemented!();
/// let mut iter = result.iter();
/// while let Some(row) = iter.next() {
///   // ... do something with `row` ...
/// }
/// ```
///
/// A Cassandra iterator is a `LendingIterator` because it borrows from some
/// underlying value, but owns a single item. Each time `next()` is invoked it
/// decodes the current item into that item, thus invalidating its previous
/// value.
#[derive(Debug)]
pub struct ResultIterator<'a>(*mut _CassIterator, usize, PhantomData<&'a _CassResult>);

// The underlying C type has no thread-local state, and forbids only concurrent
// mutation/free: https://datastax.github.io/cpp-driver/topics/#thread-safety
unsafe impl Send for ResultIterator<'_> {}
unsafe impl Sync for ResultIterator<'_> {}

impl<'a> Drop for ResultIterator<'a> {
    fn drop(&mut self) {
        unsafe { cass_iterator_free(self.0) }
    }
}

impl LendingIterator for ResultIterator<'_> {
    type Item<'a> = Row<'a> where Self: 'a;

    fn next(&mut self) -> Option<<Self as LendingIterator>::Item<'_>> {
        unsafe {
            match cass_iterator_next(self.0) {
                cass_false => None,
                cass_true => Some(self.get_row()),
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.1))
    }
}

impl ResultIterator<'_> {
    /// Gets the current row in the result set
    pub fn get_row(&self) -> Row {
        unsafe { Row::build(cass_iterator_get_row(self.0)) }
    }
}
