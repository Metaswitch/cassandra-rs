mod help;

use cassandra_cpp::*;

#[tokio::test]
async fn test_bind_by_name() -> Result<()> {
    let keyspace = "system_schema";
    let table = "tables";

    let session = help::create_test_session().await;

    let query = format!(
        "select column_name, type from system_schema.columns where keyspace_name = '{}' and \
         table_name = '{}'",
        keyspace, table
    );

    let prepared_statement = session.prepare(query).await?;
    for _ in 0..1000 {
        let statement = prepared_statement.bind();
        let result = statement.execute().await?;
        for row in result.iter() {
            let name: String = row.get_by_name("column_name")?;
            let ftype: String = row.get_by_name("type")?;
            // Actual values are not important; we're checking it doesn't panic or fail to return info.

            println!("{} {}", name, ftype);
        }
    }

    Ok(())
}
