mod help;

use cassandra_cpp::*;

static NUM_CONCURRENT_REQUESTS: usize = 100;
const PAGE_SIZE: i32 = 10;

static CREATE_TABLE: &'static str =
    "CREATE TABLE IF NOT EXISTS examples.paging (key ascii, value text, PRIMARY KEY \
     (key));";
static SELECT_QUERY: &'static str = "SELECT * FROM paging";
static INSERT_QUERY: &'static str = "INSERT INTO paging (key, value) VALUES (?, ?);";

// FIXME uuids not yet working
async fn insert_into_paging(session: &Session /* , uuid_gen:&mut UuidGen */) -> Result<()> {
    let mut futures = Vec::with_capacity(NUM_CONCURRENT_REQUESTS as usize);
    let prepared_statement = session.prepare(INSERT_QUERY).await?;

    for i in 0..NUM_CONCURRENT_REQUESTS {
        let key: &str = &(i.to_string());
        println!("key ={:?}", key);
        let mut statement = prepared_statement.bind();
        statement.bind(0, key)?;
        statement.bind(1, key)?;
        let future = statement.execute();
        futures.push(future);
    }

    futures::future::try_join_all(futures).await?;

    Ok(())
}

async fn select_from_paging(session: &Session) -> Result<Vec<(String, String)>> {
    let mut has_more_pages = true;
    let mut res = vec![];
    let mut prev_result = None;

    // FIXME must understand statement lifetime better for paging
    while has_more_pages {
        let mut statement = session.statement(SELECT_QUERY);
        statement.set_paging_size(PAGE_SIZE)?;
        if let Some(result) = prev_result.take() {
            statement.set_paging_state(result)?;
        }

        let result = statement.execute().await?;
        println!("{:?}", result);
        for row in result.iter() {
            match row.get_column(0)?.get_string() {
                Ok(key) => {
                    let key_str = key.to_string();
                    let value = row.get_column(1)?.get_string()?;
                    println!("key: '{:?}' value: '{:?}'", &key_str, &value);
                    res.push((key_str, value));
                }
                Err(err) => panic!(err),
            }
        }
        has_more_pages = result.has_more_pages();
        if has_more_pages {
            prev_result = Some(result);
        }
    }
    Ok(res)
}

#[tokio::test]
async fn test_paging() -> Result<()> {
    // let uuid_gen = &mut UuidGen::new();

    let session = help::create_test_session().await;
    help::create_example_keyspace(&session).await;

    session.execute(CREATE_TABLE).await?;
    session.execute("USE examples").await?;
    insert_into_paging(&session /* , uuid_gen */).await?;
    let mut results: Vec<(String, String)> = select_from_paging(&session).await?;
    results.sort_by_key(|kv| kv.0.clone());
    results.dedup_by_key(|kv| kv.0.clone());
    assert_eq!(results.len(), NUM_CONCURRENT_REQUESTS);

    Ok(())
}
