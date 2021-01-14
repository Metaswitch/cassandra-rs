mod help;

use cassandra_cpp::*;

#[tokio::test]
async fn test_using_consistency() -> Result<()> {
    let session = help::create_test_session().await;
    let mut statement = session.statement("select 1 + 1;");
    statement.set_consistency(Consistency::LOCAL_ONE)?;
    statement.set_serial_consistency(Consistency::SERIAL)?;

    let mut batch = session.batch(BatchType::LOGGED);
    batch.add_statement(statement)?;
    batch.set_consistency(Consistency::TWO)?;
    batch.set_serial_consistency(Consistency::LOCAL_SERIAL)?;

    Ok(())
}

#[test]
fn test_parsing_printing_consistency() {
    let all = Consistency::variants();

    for c in all {
        let s = c.to_string();
        let c2: Consistency = s.parse().expect(&format!("Failed on {:?} as {}", c, s));
        assert_eq!(c2, *c, "with intermediate {}", s);
    }

    // Just a few spot checks to confirm the formatting hasn't regressed
    // or changed unexpectedly.
    assert_eq!(Consistency::LOCAL_QUORUM.to_string(), "LOCAL_QUORUM");
    assert_eq!(format!("{}", Consistency::LOCAL_QUORUM), "LOCAL_QUORUM");
    assert_eq!("THREE".parse::<Consistency>().unwrap(), Consistency::THREE);
    let _ = "INVALID"
        .parse::<Consistency>()
        .expect_err("Should have failed to parse");
}

#[test]
fn test_using_loglevel() {
    set_level(LogLevel::DISABLED);
    set_level(LogLevel::DEBUG);

    assert!(LogLevel::DEBUG > LogLevel::WARN);
    assert!(LogLevel::WARN > LogLevel::CRITICAL);
}

#[test]
fn test_parsing_printing_loglevel() {
    for v in LogLevel::variants() {
        let s = v.to_string();
        let v2: LogLevel = s.parse().expect(&format!("Failed on {:?} as {}", v, s));
        assert_eq!(v2, *v, "with intermediate {}", s);
    }

    // Just a few spot checks to confirm the formatting hasn't regressed
    // or changed unexpectedly.
    assert_eq!(LogLevel::INFO.to_string(), "INFO");
    assert_eq!(format!("{}", LogLevel::WARN), "WARN");
    assert_eq!("ERROR".parse::<LogLevel>().unwrap(), LogLevel::ERROR);
    let _ = "INVALID"
        .parse::<LogLevel>()
        .expect_err("Should have failed to parse");
}

#[test]
fn test_using_ssl_verify_flags() {
    let mut ssl = Ssl::default();
    ssl.set_verify_flags(&vec![]);
    ssl.set_verify_flags(&vec![SslVerifyFlag::NONE]);
    ssl.set_verify_flags(&vec![SslVerifyFlag::PEER_CERT]);
    ssl.set_verify_flags(&vec![
        SslVerifyFlag::PEER_IDENTITY_DNS,
        SslVerifyFlag::PEER_CERT,
    ]);
}

#[test]
fn test_parsing_printing_ssl_verify_flags() {
    for v in SslVerifyFlag::variants() {
        let s = v.to_string();
        let v2: SslVerifyFlag = s.parse().expect(&format!("Failed on {:?} as {}", v, s));
        assert_eq!(v2, *v, "with intermediate {}", s);
    }

    // Just a few spot checks to confirm the formatting hasn't regressed
    // or changed unexpectedly.
    assert_eq!(
        SslVerifyFlag::PEER_IDENTITY_DNS.to_string(),
        "PEER_IDENTITY_DNS"
    );
    assert_eq!(format!("{}", SslVerifyFlag::PEER_CERT), "PEER_CERT");
    assert_eq!(
        "NONE".parse::<SslVerifyFlag>().unwrap(),
        SslVerifyFlag::NONE
    );
    let _ = "INVALID"
        .parse::<SslVerifyFlag>()
        .expect_err("Should have failed to parse");
}

#[test]
fn test_using_cql_protocol_version() {
    let mut cluster = Cluster::default();
    // Test that switching protocols works, the choice of version 3 and 4 is arbitrary.
    cluster.set_protocol_version(4).unwrap();
    cluster.set_protocol_version(3).unwrap();
}

#[test]
fn test_parsing_printing_batch_type() {
    for v in BatchType::variants() {
        let s = v.to_string();
        let v2: BatchType = s.parse().expect(&format!("Failed on {:?} as {}", v, s));
        assert_eq!(v2, *v, "with intermediate {}", s);
    }

    // Just a few spot checks to confirm the formatting hasn't regressed
    // or changed unexpectedly.
    assert_eq!(BatchType::LOGGED.to_string(), "LOGGED");
    assert_eq!(format!("{}", BatchType::UNLOGGED), "UNLOGGED");
    assert_eq!("COUNTER".parse::<BatchType>().unwrap(), BatchType::COUNTER);
    let _ = "INVALID"
        .parse::<BatchType>()
        .expect_err("Should have failed to parse");
}
