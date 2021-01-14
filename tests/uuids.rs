mod help;

use cassandra_cpp::*;
use std::str::FromStr;

static TRUNCATE_QUERY: &'static str = "TRUNCATE examples.log;";
static INSERT_QUERY: &'static str = "INSERT INTO examples.log (key, time, entry) VALUES (?, ?, ?);";
static SELECT_QUERY: &'static str = "SELECT * FROM examples.log WHERE key = ?";
static CREATE_TABLE: &'static str =
    "CREATE TABLE IF NOT EXISTS examples.log (key text, time timeuuid, entry text, \
     PRIMARY KEY (key, time));";

async fn insert_into_log(
    session: &Session,
    key: &str,
    time: Uuid,
    entry: &str,
) -> Result<CassResult> {
    let mut statement = session.statement(INSERT_QUERY);
    statement.bind(0, key)?;
    statement.bind(1, time)?;
    statement.bind(2, entry)?;
    statement.execute().await
}

async fn select_from_log(session: &Session, key: &str) -> Result<Vec<(Uuid, String)>> {
    let mut statement = session.statement(SELECT_QUERY);
    statement.bind(0, key)?;
    let results = statement.execute().await?;
    Ok(results
        .iter()
        .map(|r| {
            let t: Uuid = r.get_column(1).expect("time").get_uuid().expect("time");
            let e: String = r.get(2).expect("entry");
            (t, e)
        })
        .collect())
}

#[tokio::test]
async fn test_uuids() -> Result<()> {
    let uuid_gen = UuidGen::default();

    let session = help::create_test_session().await;
    help::create_example_keyspace(&session).await;

    session.execute(CREATE_TABLE).await?;
    session.execute(TRUNCATE_QUERY).await?;

    println!("uuid_gen = {}", uuid_gen.gen_time());
    insert_into_log(&session, "test", uuid_gen.gen_time(), "Log entry #1").await?;
    insert_into_log(&session, "test", uuid_gen.gen_time(), "Log entry #2").await?;
    insert_into_log(&session, "test", uuid_gen.gen_time(), "Log entry #3").await?;
    insert_into_log(&session, "test", uuid_gen.gen_time(), "Log entry #4").await?;
    let mut results = select_from_log(&session, "test").await?;
    println!("{:?}", results);

    // Check the resulting UUIDs are in order.
    results.sort_by_key(|ref kv| kv.0);
    assert_eq!(results[0].1, "Log entry #1");
    assert_eq!(results[1].1, "Log entry #2");
    assert_eq!(results[2].1, "Log entry #3");
    assert_eq!(results[3].1, "Log entry #4");

    let mut uniques = results.iter().map(|ref kv| kv.0).collect::<Vec<Uuid>>();
    uniques.dedup();
    assert_eq!(4, uniques.len());

    Ok(())
}

#[test]
fn test_uuids_from() {
    const TEST_UUID: &'static str = "a0a1a2a3-a4a5-a6a7-a8a9-aaabacadaeaf";
    let uuid_id_from_str = uuid::Uuid::parse_str(TEST_UUID).unwrap();
    let cass_id_from_str = Uuid::from_str(TEST_UUID).unwrap();

    let cass_id_from_uuid = Uuid::from(uuid_id_from_str);
    let uuid_id_from_cass = uuid::Uuid::from(cass_id_from_str);
    assert_eq!(uuid_id_from_str, uuid_id_from_cass);
    assert_eq!(cass_id_from_str, cass_id_from_uuid);
}
