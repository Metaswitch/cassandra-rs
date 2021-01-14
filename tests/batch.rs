mod help;

use cassandra_cpp::*;
use std::collections::HashSet;

#[derive(Debug, Eq, PartialEq, Hash)]
struct Pair {
    key: String,
    value: String,
}

async fn insert_into_batch_with_prepared(session: &Session, pairs: &Vec<Pair>) -> Result<()> {
    let prepared = session
        .prepare("INSERT INTO examples.pairs (key, value) VALUES (?, ?)")
        .await?;
    let mut batch = session.batch(BatchType::LOGGED);
    batch.set_consistency(Consistency::ONE)?;
    for pair in pairs {
        let mut statement = prepared.bind();
        statement.bind(0, pair.key.as_ref() as &str)?;
        statement.bind(1, pair.value.as_ref() as &str)?;
        batch.add_statement(statement)?;
    }
    batch.execute().await?;
    Ok(())
}

async fn retrieve_batch(session: &Session) -> Result<Vec<Pair>> {
    let result = session.execute("SELECT * from examples.pairs").await?;
    let v = result
        .iter()
        .map(|r| Pair {
            key: r.get(0).expect("Key"),
            value: r.get(1).expect("Value"),
        })
        .collect();
    Ok(v)
}

#[tokio::test]
async fn test_batch() -> Result<()> {
    let pairs = vec![
        Pair {
            key: "a".to_string(),
            value: "1".to_string(),
        },
        Pair {
            key: "b".to_string(),
            value: "2".to_string(),
        },
        Pair {
            key: "c".to_string(),
            value: "3".to_string(),
        },
        Pair {
            key: "d".to_string(),
            value: "4".to_string(),
        },
    ];

    let session = help::create_test_session().await;
    help::create_example_keyspace(&session).await;

    session
        .execute(
            "CREATE TABLE IF NOT EXISTS examples.pairs (key text, value text, PRIMARY KEY (key));",
        )
        .await?;
    insert_into_batch_with_prepared(&session, &pairs).await?;
    let result = retrieve_batch(&session).await?;

    let set0: HashSet<_> = pairs.iter().collect();
    let set1: HashSet<_> = result.iter().collect();
    assert_eq!(set0, set1, "expected {:?} but got {:?}", &pairs, &result);

    Ok(())
}
