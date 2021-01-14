use cassandra_cpp::*;

async fn create_session() -> Result<Session> {
    let contact_points = "127.0.0.1";
    let mut cluster = Cluster::default();
    cluster.set_contact_points(contact_points)?;
    cluster.set_load_balance_round_robin();

    cluster.connect().await
}

async fn execute_statement() -> Result<()> {
    let session = create_session().await?;
    let result = session
        .statement("SELECT keyspace_name FROM system_schema.keyspaces;")
        .execute()
        .await?;

    for row in result.iter() {
        let col: String = row.get_by_name("keyspace_name")?;
        print!("ks = {}", col);
    }

    Ok(())
}

async fn execute_prepared_statement() -> Result<()> {
    let session = create_session().await?;
    let prepared_statement = session
        .prepare("SELECT value FROM kv_table WHERE key = ?")
        .await?;

    let mut statement = prepared_statement.bind();
    statement.bind_string(0, "key")?;

    let result = statement.execute().await?;
    for row in result.iter() {
        let col: String = row.get_by_name("value")?;
        print!("value = {}", col);
    }

    Ok(())
}

async fn execute_batch_statement() -> Result<()> {
    let session = create_session().await?;
    let mut batch = session.batch(BatchType::LOGGED);
    batch.add_statement(session.statement("INSERT INTO kv_table (key, value) VALUES (?, ?)"))?;
    batch.add_statement(session.statement("INSERT INTO kv_table (key, value) VALUES (?, ?)"))?;
    batch.add_statement(session.statement("INSERT INTO kv_table (key, value) VALUES (?, ?)"))?;
    let _result = batch.execute().await?;

    Ok(())
}

fn assert_send_static_future<F, T>(f: F) -> F
where
    F: std::future::Future<Output = T> + Send + 'static,
{
    f
}

#[tokio::main]
async fn main() -> Result<()> {
    // tokio, et-al expect the future to be send + 'static, this ensures that the operations we do above are
    // send + static, something that can quickly turn not to be, because of the lower level FFI code, and the await boundaries
    // of the `async fn`'s within cassandra-rs.
    assert_send_static_future(execute_prepared_statement()).await?;
    assert_send_static_future(execute_statement()).await?;
    assert_send_static_future(execute_batch_statement()).await?;

    Ok(())
}
