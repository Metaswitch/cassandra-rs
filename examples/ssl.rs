use cassandra_cpp::*;
use std::fs;

#[tokio::main]
async fn main() -> Result<()> {
    let col_name = "keyspace_name";
    let contact_points = "127.0.0.1";
    let tls_ca_certificate_path = "ca/certificate/path";

    let cert =
        fs::read_to_string(tls_ca_certificate_path).expect("Failed to open certificate file");
    let mut ssl = cassandra_cpp::Ssl::default();
    ssl.add_trusted_cert(cert)?;
    ssl.set_verify_flags(&[cassandra_cpp::SslVerifyFlag::PEER_IDENTITY]);

    let mut cluster = Cluster::default();
    cluster.set_contact_points(contact_points).unwrap();
    cluster.set_load_balance_round_robin();
    cluster.set_ssl(ssl);

    let session = cluster.connect().await?;
    let result = session
        .execute("SELECT keyspace_name FROM system_schema.keyspaces;")
        .await
        .unwrap();

    println!("{}", result);
    for row in result.iter() {
        let col: String = row.get_by_name(col_name).unwrap();
        println!("ks name = {}", col);
    }

    Ok(())
}
