//! Test use of Rust futures (not ResultFuture etc).
//! Based on `async`.

mod help;

use cassandra_cpp::*;
use futures::future::BoxFuture;

static NUM_CONCURRENT_REQUESTS: usize = 1000;

fn insert_into_async(
    session: &Session,
    key: String,
    count: usize,
) -> Result<Vec<BoxFuture<Result<CassResult>>>> {
    let mut futures: Vec<BoxFuture<Result<CassResult>>> = Vec::new();
    for i in 0..count {
        let key: &str = &(key.clone() + &i.to_string());
        let mut statement = session.statement(
            "INSERT INTO examples.async (key, bln, flt, dbl, i32, i64)
        	VALUES (?, ?, ?, ?, ?, ?);",
        );

        statement.bind(0, key)?;
        statement.bind(1, i % 2 == 0)?;
        statement.bind(2, i as f32 / 2.0f32)?;
        statement.bind(3, i as f64 / 200.0)?;
        statement.bind(4, i as i32 * 10)?;
        statement.bind(5, i as i64 * 100)?;

        futures.push(Box::pin(statement.execute()));
    }

    Ok(futures)
}

/// Smoke-test the implementation of Rust futures.
#[tokio::test]
pub async fn test_rust_futures() -> Result<()> {
    let session = help::create_test_session().await;
    help::create_example_keyspace(&session).await;

    session
        .execute(
            "CREATE TABLE IF NOT EXISTS examples.async(key text, bln boolean, flt float, dbl \
             double, i32 int, i64 bigint, PRIMARY KEY (key));",
        )
        .await?;

    session.execute("USE examples").await?;

    let inserts = insert_into_async(&session, "test".to_owned(), NUM_CONCURRENT_REQUESTS).unwrap();

    futures::future::try_join_all(inserts).await?;

    Ok(())
}

/// Test early drops of Rust futures (a buggy implementation would lead to SIGSEGV, SIGILL,
/// SIGABRT and friends).
#[tokio::test]
pub async fn test_early_drop_rust_futures() -> Result<()> {
    let session = help::create_test_session().await;
    help::create_example_keyspace(&session).await;

    let big_future = async {
        session
            .execute(
                "CREATE TABLE IF NOT EXISTS examples.async(key text, bln boolean, flt float, dbl \
                 double, i32 int, i64 bigint, PRIMARY KEY (key));",
            )
            .await?;
        session.execute("USE examples").await?;

        let mut inserts =
            insert_into_async(&session, "test".to_owned(), NUM_CONCURRENT_REQUESTS).unwrap();
        // Put in reverse, so we poll the later ones (which won't be ready) before the earlier ones
        // (which will be immediately ready)
        inserts.reverse();
        // Wait for one of them to complete, and drop all the other in-flight ones.
        let (first_result, _, _) = futures::future::select_all(inserts).await;
        first_result
    };

    big_future.await?;

    let more_inserts = insert_into_async(&session, "test".to_owned(), NUM_CONCURRENT_REQUESTS)?;

    futures::future::try_join_all(more_inserts).await?;

    Ok(())
}
