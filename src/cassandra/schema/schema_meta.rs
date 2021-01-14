use crate::cassandra::iterator::KeyspaceIterator;

use crate::cassandra::schema::keyspace_meta::KeyspaceMeta;
use crate::cassandra::util::{Protected, ProtectedInner};
use crate::cassandra_sys::cass_iterator_keyspaces_from_schema_meta;
use crate::cassandra_sys::cass_schema_meta_free;
use crate::cassandra_sys::cass_schema_meta_keyspace_by_name_n;
use crate::cassandra_sys::cass_schema_meta_snapshot_version;
use crate::cassandra_sys::CassSchemaMeta as _CassSchemaMeta;
use std::os::raw::c_char;

/// A snapshot of the schema's metadata
#[derive(Debug)]
pub struct SchemaMeta(*const _CassSchemaMeta);

impl Drop for SchemaMeta {
    fn drop(&mut self) {
        unsafe {
            cass_schema_meta_free(self.0);
        }
    }
}

impl ProtectedInner<*const _CassSchemaMeta> for SchemaMeta {
    fn inner(&self) -> *const _CassSchemaMeta {
        self.0
    }
}

impl Protected<*const _CassSchemaMeta> for SchemaMeta {
    fn build(inner: *const _CassSchemaMeta) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        SchemaMeta(inner)
    }
}

impl SchemaMeta {
    /// Gets the version of the schema metadata snapshot.
    pub fn snapshot_version(&self) -> u32 {
        unsafe { cass_schema_meta_snapshot_version(self.0) }
    }

    /// Gets the keyspace metadata for the provided keyspace name.
    pub fn get_keyspace_by_name(&self, keyspace: &str) -> KeyspaceMeta {
        // TODO: can return NULL
        unsafe {
            let keyspace_ptr = keyspace.as_ptr() as *const c_char;
            KeyspaceMeta::build(cass_schema_meta_keyspace_by_name_n(
                self.0,
                keyspace_ptr,
                keyspace.len(),
            ))
        }
    }

    /// Returns an iterator over the keyspaces in this schema
    pub fn keyspace_iter(&mut self) -> KeyspaceIterator {
        unsafe { KeyspaceIterator::build(cass_iterator_keyspaces_from_schema_meta(self.0)) }
    }
}
