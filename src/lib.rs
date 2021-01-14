//! This is a wrapper around the DataStax C++ driver for Cassandra. It aims to be 100% safe with minimal overhead added
#![deny(missing_docs)]
#![allow(unknown_lints)]
#![allow(doc_markdown)]
#![allow(unused_imports)] // TODO: remove
#![allow(dead_code)] // TODO: remove
// `error_chain!` can recurse deeply
#![recursion_limit = "1024"]

#[macro_use]
extern crate slog;
#[macro_use]
extern crate error_chain;

use cassandra_cpp_sys as cassandra_sys;

pub use crate::cassandra::batch::{Batch, BatchType, CustomPayload};
pub use crate::cassandra::cluster::{Cluster, CqlProtocol};
pub use crate::cassandra::collection::{CassCollection, List, Map, Set};
pub use crate::cassandra::consistency::Consistency;
pub use crate::cassandra::data_type::DataType;
// pub use cassandra::write_type::*;
pub use crate::cassandra::field::Field;
pub use crate::cassandra::inet::Inet;
// pub use cassandra::util::*;
// pub use cassandra::metrics::*;
pub use crate::cassandra::iterator::{
    AggregateIterator, ColumnIterator, FieldIterator, FunctionIterator, KeyspaceIterator,
    MapIterator, SetIterator, TableIterator, UserTypeIterator,
};
pub use crate::cassandra::log::{set_level, set_logger, LogLevel};
pub use crate::cassandra::policy::retry::RetryPolicy;
pub use crate::cassandra::prepared::PreparedStatement;
pub use crate::cassandra::result::CassResult;
pub use crate::cassandra::row::AsRustType;
pub use crate::cassandra::row::Row;
pub use crate::cassandra::schema::aggregate_meta::AggregateMeta;
pub use crate::cassandra::schema::column_meta::ColumnMeta;
pub use crate::cassandra::schema::function_meta::FunctionMeta;
pub use crate::cassandra::schema::keyspace_meta::KeyspaceMeta;
pub use crate::cassandra::schema::schema_meta::SchemaMeta;
pub use crate::cassandra::schema::table_meta::TableMeta;
pub use crate::cassandra::session::Session;
pub use crate::cassandra::ssl::{Ssl, SslVerifyFlag};
pub use crate::cassandra::statement::BindRustType;
pub use crate::cassandra::statement::Statement;
// pub use cassandra::custom_payload::CustomPayload;
pub use crate::cassandra::time::TimestampGen;
pub use crate::cassandra::tuple::Tuple;
pub use crate::cassandra::user_type::UserType;
pub use crate::cassandra::uuid::{Uuid, UuidGen};
pub use crate::cassandra::value::{Value, ValueType};

pub use crate::cassandra::error::*;

// #[macro_use]
mod cassandra {
    #[macro_use]
    pub mod util;
    pub mod batch;
    pub mod cluster;
    pub mod collection;
    pub mod consistency;
    pub mod custom_payload;
    pub mod data_type;
    pub mod error;
    pub mod field;
    pub mod future;
    pub mod inet;
    pub mod iterator;
    pub mod log;
    pub mod metrics;
    pub mod policy;
    pub mod prepared;
    pub mod result;
    pub mod row;
    pub mod schema;
    pub mod session;
    pub mod ssl;
    pub mod statement;
    pub mod time;
    pub mod tuple;
    pub mod user_type;
    pub mod uuid;
    pub mod value;
    pub mod write_type;
}

// #[phase(plugin)] extern crate bindgen;
// #[allow(dead_code, uppercase_variables, non_camel_case_types)]
// mod mysql_bindings {
//    bindgen!("/usr/include/mysql/mysql.h", match="mysql.h", link="mysql");
// }
