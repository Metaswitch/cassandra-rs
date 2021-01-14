//! Helper methods for test code.
//!
#![allow(dead_code)] // Some functions are only used in some tests.

use cassandra_cpp::*;

/// Get a new session to the test Cassandra instance.
pub async fn create_test_session() -> Session {
    let contact_points = "127.0.0.1";

    let mut cluster = Cluster::default();
    cluster.set_contact_points(contact_points).unwrap();
    cluster.set_load_balance_round_robin();

    cluster
        .connect()
        .await
        .expect("Failed to connect to Cassandra")
}

/// Create the keyspace for testing.
pub async fn create_example_keyspace(session: &Session) {
    session
        .execute(
            "CREATE KEYSPACE IF NOT EXISTS examples WITH replication = { \'class\': \
         \'SimpleStrategy\', \'replication_factor\': \'1\' };",
        )
        .await
        .expect("Failed to create example keyspace");
}
