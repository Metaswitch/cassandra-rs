mod help;

use cassandra_cpp::*;
use std::collections::HashSet;

async fn insert_into_collections(
    session: &Session,
    key: &str,
    items: &Vec<String>,
) -> Result<CassResult> {
    let mut statement =
        session.statement("INSERT INTO examples.collections (key, items) VALUES (?, ?);");
    statement.bind(0, key)?;
    let mut set = Set::new(2);
    for item in items {
        set.append_string(item)?;
    }
    statement.bind_set(1, set)?;
    statement.execute().await
}

async fn insert_null_into_collections(session: &Session, key: &str) -> Result<CassResult> {
    let mut statement = session.statement("INSERT INTO examples.collections (key) VALUES (?);");
    statement.bind(0, key)?;
    statement.execute().await
}

async fn select_from_collections(session: &Session, key: &str) -> Result<Vec<String>> {
    let mut statement = session.statement("SELECT items FROM examples.collections WHERE key = ?");
    statement.bind(0, key)?;
    let result = statement.execute().await?;
    println!("{:?}", result);
    let mut res = vec![];
    for row in result.iter() {
        let column = row.get_column(0);
        let items_iterator: SetIterator = column?.get_set()?;
        for item in items_iterator {
            println!("item: {:?}", item);
            res.push(item.get_string().expect("Should exist").to_string());
        }
    }
    Ok(res)
}

#[tokio::test]
async fn test_collections() -> Result<()> {
    let items = vec![
        "apple".to_string(),
        "orange".to_string(),
        "banana".to_string(),
        "mango".to_string(),
    ];

    let session = help::create_test_session().await;
    help::create_example_keyspace(&session).await;

    session
        .execute(
            "
            CREATE TABLE IF NOT EXISTS examples.collections (key text, items set<text>, PRIMARY \
            KEY (key))
        ",
        )
        .await?;

    insert_into_collections(&session, "test", &items).await?;
    let result = select_from_collections(&session, "test").await?;

    let set0: HashSet<_> = items.iter().collect();
    let set1: HashSet<_> = result.iter().collect();
    assert_eq!(set0, set1, "expected {:?} but got {:?}", &items, &result);

    insert_null_into_collections(&session, "empty").await?;
    select_from_collections(&session, "empty")
        .await
        .expect_err("Should fail cleanly");

    Ok(())
}
