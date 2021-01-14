mod help;

use cassandra_cpp::*;
use futures::future::BoxFuture;

static NUM_CONCURRENT_REQUESTS: usize = 1000;

fn insert_into_async(session: &Session, key: String) -> Result<Vec<BoxFuture<Result<CassResult>>>> {
    let mut futures: Vec<BoxFuture<Result<CassResult>>> = Vec::new();
    for i in 0..NUM_CONCURRENT_REQUESTS {
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

#[tokio::test]
pub async fn test_async() -> Result<()> {
    let session = help::create_test_session().await;
    help::create_example_keyspace(&session).await;

    session
        .execute(
            "CREATE TABLE IF NOT EXISTS examples.async(key text, bln boolean, flt float, dbl \
            double, i32 int, i64 bigint, PRIMARY KEY (key));",
        )
        .await
        .unwrap();

    session.execute("USE examples").await.unwrap();

    let futures = insert_into_async(&session, "test".to_owned())?;
    for future in futures {
        future.await?;
    }

    Ok(())
}
