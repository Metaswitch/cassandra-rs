use cassandra_cpp::*;

#[tokio::main]
async fn main() -> Result<()> {
    let mut cluster = Cluster::default();
    cluster.set_contact_points("127.0.0.1").unwrap();
    cluster.set_load_balance_round_robin();
    let session = cluster.connect().await?;

    let result = session
        .execute("SELECT keyspace_name FROM system_schema.keyspaces;")
        .await?;
    println!("{}", result);

    for row in result.iter() {
        let col: String = row.get_by_name("keyspace_name")?;
        println!("ks name = {}", col);
    }

    Ok(())
}
