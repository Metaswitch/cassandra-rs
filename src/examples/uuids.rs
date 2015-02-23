extern crate cql_ffi;

use cql_ffi::CassSession;
use cql_ffi::CassUuid;
use cql_ffi::CassStatement;
use cql_ffi::CassResult;
use cql_ffi::CassError;
use cql_ffi::CassUuidGen;
use cql_ffi::CassCluster;

static INSERT_QUERY:&'static str = "INSERT INTO examples.log (key, time, entry) VALUES (?, ?, ?);";
static SELECT_QUERY:&'static str = "SELECT * FROM examples.log WHERE key = ?";
static CREATE_KEYSPACE:&'static str = "CREATE KEYSPACE IF NOT EXISTS examples WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '3' };";
static CREATE_TABLE:&'static str = "CREATE TABLE IF NOT EXISTS examples.log (key text, time timeuuid, entry text, PRIMARY KEY (key, time));";

fn insert_into_log(session:&mut CassSession, key:&str, time:CassUuid, entry:&str) -> Result<CassResult,CassError> {
	let statement = CassStatement::new(INSERT_QUERY, 3);
	statement.bind_string(0, key).unwrap();
	statement.bind_uuid(1, time).unwrap();
	statement.bind_string(2, &entry).unwrap();
	let mut future = session.execute_statement(&statement);
	future.wait()
}

fn select_from_log(session:&mut CassSession, key:&str) -> Result<CassResult,CassError> {
	let statement = &CassStatement::new(SELECT_QUERY, 1);
	statement.bind_string(0, &key).unwrap();
	let mut future = session.execute_statement(statement);
	let results = try!(future.wait());
	for row in results.iter() {
		let time = row.get_column(1);
		let entry = row.get_column(2);
		let time_str = time.get_string();
		println!("{:?}.{:?}", time_str, entry.get_string());
	}
	Ok(results)
}

fn main() {
	let uuid_gen = CassUuidGen::new();
	let cluster = &CassCluster::new().set_contact_points("127.0.0.1").unwrap();
	let session = &mut CassSession::new().connect(cluster).wait().unwrap();
	
	session.execute(CREATE_KEYSPACE, 0);
	session.execute(CREATE_TABLE, 0);
	insert_into_log(session, "test", uuid_gen.get_time(), "Log entry #1").unwrap();
	insert_into_log(session, "test", uuid_gen.get_time(), "Log entry #2").unwrap();
	insert_into_log(session, "test", uuid_gen.get_time(), "Log entry #3").unwrap();
	insert_into_log(session, "test", uuid_gen.get_time(), "Log entry #4").unwrap();
	let result = select_from_log(session, "test").unwrap();
	println!("{:?}", result);
}