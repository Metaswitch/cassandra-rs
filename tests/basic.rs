mod help;

use cassandra_cpp::*;
use std::time::SystemTime;
use time::Duration;

#[derive(Debug, PartialEq, Copy, Clone, Default)]
struct Udt {
    dt: u32,
    tm: i64,
}

#[derive(Debug, PartialEq, Clone, Default)]
struct Basic {
    bln: bool,
    flt: f32,
    dbl: f64,
    i8: i8,
    i16: i16,
    i32: i32,
    i64: i64,
    ts: i64,
    addr: Inet,
    tu: Uuid,
    id: Uuid,
    ct: Udt,
    txt: String,
}

/// Create the table for basic testing.
async fn create_basic_table(session: &Session) -> Result<()> {
    session
        .execute("CREATE TYPE IF NOT EXISTS examples.udt (dt date, tm time);")
        .await?;
    session
        .execute(
            "
                CREATE TABLE IF NOT EXISTS examples.basic (key text, bln boolean, flt \
                float, dbl double, i8 tinyint, i16 smallint, i32 int, i64 bigint, \
                ts timestamp, addr inet, tu timeuuid, id uuid, ct udt, txt text, PRIMARY KEY (key));
            ",
        )
        .await?;
    session.execute("TRUNCATE examples.basic;").await?;
    Ok(())
}

async fn insert_into_basic_by_name(
    session: &Session,
    key: &str,
    basic: &Basic,
) -> Result<CassResult> {
    let mut statement = session.statement("
        INSERT INTO examples.basic (key, bln, flt, dbl, i8, i16, i32, i64, ts, addr, tu, id, ct, txt) \
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    ");

    let ct_type = DataType::new_udt(2);
    ct_type.add_sub_value_type_by_name::<&str>("dt", ValueType::DATE)?;
    ct_type.add_sub_value_type_by_name::<&str>("tm", ValueType::TIME)?;
    let mut ct_udt = ct_type.new_user_type();
    ct_udt.set_uint32_by_name("dt", basic.ct.dt)?;
    ct_udt.set_int64_by_name("tm", basic.ct.tm)?;

    statement.bind_by_name("key", key)?;
    statement.bind_by_name("bln", basic.bln)?;
    statement.bind_by_name("flt", basic.flt)?;
    statement.bind_by_name("dbl", basic.dbl)?;
    statement.bind_by_name("i8", basic.i8)?;
    statement.bind_by_name("i16", basic.i16)?;
    statement.bind_by_name("i32", basic.i32)?;
    statement.bind_by_name("i64", basic.i64)?;
    statement.bind_by_name("ts", basic.ts)?;
    statement.bind_by_name("addr", basic.addr)?;
    statement.bind_by_name("tu", basic.tu)?;
    statement.bind_by_name("id", basic.id)?;
    statement.bind_by_name("ct", &ct_udt)?;
    statement.bind_by_name("txt", basic.txt.as_str())?;

    statement.execute().await
}

async fn insert_into_basic(session: &Session, key: &str, basic: &Basic) -> Result<CassResult> {
    let mut statement = session.statement("
        INSERT INTO examples.basic (key, bln, flt, dbl, i8, i16, i32, i64, ts, addr, tu, id, ct, txt) \
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    ");

    let ct_type = DataType::new_udt(2);
    ct_type.add_sub_value_type_by_name::<&str>("dt", ValueType::DATE)?;
    ct_type.add_sub_value_type_by_name::<&str>("tm", ValueType::TIME)?;
    let mut ct_udt = ct_type.new_user_type();
    ct_udt.set_uint32_by_name("dt", basic.ct.dt)?;
    ct_udt.set_int64_by_name("tm", basic.ct.tm)?;

    statement.bind(0, key)?;
    statement.bind(1, basic.bln)?;
    statement.bind(2, basic.flt)?;
    statement.bind(3, basic.dbl)?;
    statement.bind(4, basic.i8)?;
    statement.bind(5, basic.i16)?;
    statement.bind(6, basic.i32)?;
    statement.bind(7, basic.i64)?;
    statement.bind(8, basic.ts)?;
    statement.bind(9, basic.addr)?;
    statement.bind(10, basic.tu)?;
    statement.bind(11, basic.id)?;
    statement.bind(12, &ct_udt)?;
    statement.bind(13, basic.txt.as_str())?;

    statement.execute().await
}

fn basic_from_result(result: CassResult) -> Result<Option<Basic>> {
    match result.first_row() {
        None => Ok(None),
        Some(row) => {
            // todo: refactor?
            let fields_iter: UserTypeIterator = row.get(12)?;
            let mut dt: u32 = 0;
            let mut tm: i64 = 0;
            for field in fields_iter {
                match field.0.as_ref() {
                    "dt" => dt = field.1.get_u32()?,
                    "tm" => tm = field.1.get_i64()?,
                    _ => panic!("Unexpected field: {:?}", field),
                }
            }

            Ok(Some(Basic {
                bln: row.get(1)?,
                flt: row.get(2)?,
                dbl: row.get(3)?,
                i8: row.get(4)?,
                i16: row.get(5)?,
                i32: row.get(6)?,
                i64: row.get(7)?,
                ts: row.get(8)?,
                addr: row.get(9)?,
                tu: row.get(10)?,
                id: row.get(11)?,
                ct: Udt { dt: dt, tm: tm },
                txt: row.get(13)?,
            }))
        }
    }
}

const SELECT_QUERY: &str = "
SELECT key, bln, flt, dbl, i8, i16, i32, i64, ts, addr, tu, id, ct, txt \
FROM examples.basic WHERE key = ?";

async fn select_from_basic(session: &Session, key: &str) -> Result<Option<Basic>> {
    let mut statement = session.statement(SELECT_QUERY);
    statement.bind_string(0, key)?;
    let result = statement.execute().await?;
    println!("Result: \n{:?}\n", result);
    basic_from_result(result)
}

async fn select_from_basic_prepared(
    prepared: &PreparedStatement,
    key: &str,
) -> Result<Option<Basic>> {
    let mut statement = prepared.bind();
    statement.bind_string(0, key)?;
    let result = statement.execute().await?;
    basic_from_result(result)
}

#[tokio::test]
async fn test_simple() -> Result<()> {
    let session = help::create_test_session().await;
    let result = session
        .execute("SELECT keyspace_name FROM system_schema.keyspaces;")
        .await?;

    println!("{}", result);
    let mut names = vec![];
    for row in result.iter() {
        let col: String = row.get_by_name("keyspace_name").unwrap();
        println!("ks name = {}", col);
        names.push(col);
    }

    assert!(names.contains(&"system_schema".to_string()));
    assert!(names.contains(&"system_auth".to_string()));

    Ok(())
}

#[tokio::test]
async fn test_basic_error() {
    let session = help::create_test_session().await;
    session
        .execute("CREATE GOBBLEDEGOOK;")
        .await
        .expect_err("Should cleanly return an error");
}

#[tokio::test]
async fn test_basic_round_trip() -> Result<()> {
    let session = help::create_test_session().await;
    help::create_example_keyspace(&session).await;
    create_basic_table(&session).await?;
    let uuid_gen = UuidGen::default();

    let ts = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        * 1_000;

    let input = Basic {
        bln: true,
        flt: 0.001f32,
        dbl: 0.0002f64,
        i8: 1,
        i16: 2,
        i32: 3,
        i64: 4,
        ts: ts as i64,
        addr: "127.0.0.1".parse().unwrap(),
        tu: uuid_gen.gen_time(),
        id: uuid_gen.gen_random(),
        ct: Udt {
            dt: ts as u32,
            tm: ts as i64,
        },
        txt: "some\0unicode text 😊".to_string(),
    };

    insert_into_basic(&session, "test", &input).await?;
    let output = select_from_basic(&session, "test")
        .await?
        .expect("no output from select");

    println!("{:?}", input);
    println!("{:?}", output);

    assert!(input == output);

    // We are forced to use a null terminated CString to workaround a bug in the
    // cpp driver. Therefore, null char is not allowed.
    let input = {
        let mut input = input;
        input.txt = "some unicode text 😊".to_string();
        input
    };

    insert_into_basic_by_name(&session, "test_by_name", &input).await?;
    let output = select_from_basic(&session, "test_by_name")
        .await?
        .expect("no output from select");

    assert!(input == output);

    Ok(())
}

#[tokio::test]
async fn test_prepared_round_trip() -> Result<()> {
    let session = help::create_test_session().await;
    help::create_example_keyspace(&session).await;
    create_basic_table(&session).await?;
    let uuid_gen = UuidGen::default();

    let ts = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        * 1_000;
    let input = Basic {
        bln: true,
        flt: 0.001f32,
        dbl: 0.0002f64,
        i8: 1,
        i16: 2,
        i32: 3,
        i64: 4,
        ts: ts as i64,
        addr: "127.0.0.1".parse().unwrap(),
        tu: uuid_gen.gen_time(),
        id: uuid_gen.gen_random(),
        ct: Udt {
            dt: ts as u32,
            tm: ts as i64,
        },
        txt: "some\0text".to_string(),
    };

    println!("Basic insertions");
    insert_into_basic(&session, "prepared_test", &input).await?;
    println!("Preparing");
    let prepared = session.prepare(SELECT_QUERY).await?;

    let output = select_from_basic_prepared(&prepared, "prepared_test")
        .await?
        .expect("did not find row");
    assert_eq!(input, output, "Input:  {:?}\noutput: {:?}", &input, &output);

    Ok(())
}

#[tokio::test]
async fn test_null_retrieval() -> Result<()> {
    let session = help::create_test_session().await;
    help::create_example_keyspace(&session).await;
    create_basic_table(&session).await?;

    // Insert a partial row.
    session
        .execute("INSERT INTO examples.basic (key, bln, flt) VALUES ('vacant', true, 3.14);")
        .await?;

    // Read the whole row.
    let result = session
        .execute(
            "SELECT key, bln, flt, dbl, i8, i16, i32, i64, ts, addr, tu, id, ct, txt \
              FROM examples.basic WHERE key = 'vacant';",
        )
        .await?;

    // Check response is as expected.
    assert_eq!(1, result.row_count());
    let row = result.first_row().unwrap();

    let v: String = row.get(0).unwrap();
    assert_eq!(v, "vacant".to_string());
    assert!(!row.get_column(0).unwrap().is_null());

    let v: bool = row.get(1).unwrap();
    assert_eq!(v, true);
    assert!(!row.get_column(1).unwrap().is_null());

    let v: f32 = row.get(2).unwrap();
    assert_eq!(v, 3.14f32);
    assert!(!row.get_column(2).unwrap().is_null());

    let c = row.get_column(3)?;
    c.get_f64().expect_err("should be null");
    assert!(c.is_null());

    let c = row.get_column(4)?;
    c.get_i8().expect_err("should be null");
    assert!(c.is_null());

    let c = row.get_column(5)?;
    c.get_i16().expect_err("should be null");
    assert!(c.is_null());

    let c = row.get_column(6)?;
    c.get_i32().expect_err("should be null");
    assert!(c.is_null());

    let c = row.get_column(7)?;
    c.get_i64().expect_err("should be null");
    assert!(c.is_null());

    let c = row.get_column(8)?;
    c.get_u32().expect_err("should be null");
    assert!(c.is_null());

    let c = row.get_column(9)?;
    c.get_inet().expect_err("should be null");
    assert!(c.is_null());

    let c = row.get_column(10)?;
    c.get_uuid().expect_err("should be null");
    assert!(c.is_null());

    let c = row.get_column(11)?;
    c.get_uuid().expect_err("should be null");
    assert!(c.is_null());

    let c = row.get_column(12)?;
    c.get_user_type().expect_err("should be null");
    assert!(c.is_null());

    let c = row.get_column(13)?;
    c.get_user_type().expect_err("should be null");
    assert!(c.is_null());

    Ok(())
}

#[tokio::test]
async fn test_null_insertion() -> Result<()> {
    let session = help::create_test_session().await;
    help::create_example_keyspace(&session);
    create_basic_table(&session);

    // Insert some explicit nulls.
    let mut s = session.statement(
        "INSERT INTO examples.basic (key, bln, flt, dbl, i8, i16, i32, i64, ts, addr, tu, id, ct, txt) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
    );
    s.bind(0, "shrdlu")?;
    s.bind(1, false)?;
    s.bind_null(2)?;
    s.bind(3, 2.72f64)?;
    // deliberately omit 4 - this should be equivalent to binding null
    s.bind_null(5)?;
    s.bind_null(6)?;
    s.bind_null(7)?;
    s.bind_null(8)?;
    s.bind_null(9)?;
    s.bind_null(10)?;
    s.bind_null(11)?;
    s.bind_null(12)?;
    s.bind_null(13)?;
    s.execute().await?;

    // Read them back.
    let result = session
        .execute(
            "SELECT key, bln, flt, dbl, i8, i16, i32, i64, ts, addr, tu, id, ct, txt \
              FROM examples.basic WHERE key = 'shrdlu';",
        )
        .await?;
    assert_eq!(1, result.row_count());
    let row = result.first_row().unwrap();

    assert!(!row.get_column(0)?.is_null());
    assert!(!row.get_column(1)?.is_null());
    assert!(row.get_column(2)?.is_null());
    assert!(!row.get_column(3)?.is_null());
    assert!(row.get_column(4)?.is_null());
    assert!(row.get_column(5)?.is_null());
    assert!(row.get_column(6)?.is_null());
    assert!(row.get_column(7)?.is_null());
    assert!(row.get_column(8)?.is_null());
    assert!(row.get_column(9)?.is_null());
    assert!(row.get_column(10)?.is_null());
    assert!(row.get_column(11)?.is_null());
    assert!(row.get_column(12)?.is_null());
    assert!(row.get_column(13)?.is_null());

    Ok(())
}

/// Check for a needle in a haystack, and fail if not present.
fn assert_contains(haystack: String, needle: &str) {
    assert!(
        haystack.contains(needle),
        "assert_contains: `{}` not found in `{}`",
        needle,
        &haystack
    );
}

#[tokio::test]
async fn test_rendering() -> Result<()> {
    let session = help::create_test_session().await;
    let result = session
        .execute("SELECT * FROM system_schema.tables;")
        .await?;
    println!("test_rendering: {}", result);

    let row = result.iter().next().unwrap();
    let compaction_col = row.get_column_by_name("compaction")?;

    // Check rendering of a column
    let column_debug = format!("{:?}", compaction_col);
    println!("Column debug: {}", &column_debug);
    assert_contains(
        column_debug,
        r#"{"class" => "org.apache.cassandra.db.compaction"#,
    );

    let column_display = format!("{}", compaction_col);
    println!("Column display: {}", &column_display);
    assert_contains(
        column_display,
        r#"{class => org.apache.cassandra.db.compaction"#,
    );

    // Check retrieving a string and a str.
    let keyspace_col = row.get_column_by_name("keyspace_name").unwrap();
    let str: &str = keyspace_col.get_str().unwrap();
    println!("Str is {}", str);
    assert!(str.len() > 0, "empty str");
    let str: String = keyspace_col.get_string().unwrap();
    println!("String is {}", str);
    assert!(str.len() > 0, "empty string");

    // Check invalid retrieval type.
    keyspace_col
        .get_map()
        .expect_err("Should fail with invalid value type");

    Ok(())
}

#[tokio::test]
async fn test_error_reporting() -> Result<()> {
    let session = help::create_test_session().await;

    // Simple error.
    let mut statement = session.statement("SELECT * from system_schema.tables;");
    statement.set_consistency(Consistency::THREE).unwrap(); // assuming we only have one node, this must fail
    let err = statement.execute().await.expect_err("Should have failed!");

    println!("Got error {} kind {:?}", err, err.kind());
    match *err.kind() {
        ErrorKind::CassError(CassErrorCode::LIB_NO_HOSTS_AVAILABLE, _) => (),
        ref k => panic!("Unexpected error kind {}", k),
    }

    // Detailed error.
    let err = session
        .execute("SELECT gibberish from system_schema.tables;")
        .await
        .expect_err("Should have failed!");
    println!("Got error {} kind {:?}", err, err.kind());
    match *err.kind() {
        ErrorKind::CassErrorResult(
            CassErrorCode::SERVER_INVALID_QUERY,
            _,
            _,
            -1,
            -1,
            -1,
            _,
            _,
            _,
            _,
            _,
        ) => (),
        ref k => panic!("Unexpected error kind {}", k),
    }

    // UTF-8 error - return an invalid UTF-8 string
    // Interpret -1 (0xFFFFFFFF) as a UTF-8 string, but 0xFF... is invalid UTF-8.
    help::create_example_keyspace(&session).await;
    create_basic_table(&session).await?;
    session
        .execute("INSERT INTO examples.basic (key, i32) VALUES ('utf8', -1);")
        .await?;
    let result = session
        .execute("SELECT i32 FROM examples.basic WHERE key = 'utf8';")
        .await?;
    let row = result.iter().next().unwrap();
    let err = row
        .get_column(0)?
        .get_string()
        .expect_err("Should have failed");
    println!("Got error {} kind {:?}", err, err.kind());
    match *err.kind() {
        ErrorKind::InvalidUtf8(_) => (),
        ref k => panic!("Unexpected error kind {}", k),
    }

    Ok(())
}

#[tokio::test]
async fn test_result() {
    let session = help::create_test_session().await;
    let result = session
        .execute("SELECT * FROM system_schema.tables;")
        .await
        .unwrap();

    assert_eq!("keyspace_name", result.column_name(0).unwrap());
    assert_eq!("table_name", result.column_name(1).unwrap());
}

#[tokio::test]
async fn test_statement_timeout() {
    let session = help::create_test_session().await;
    let mut query = session.statement("SELECT * FROM system_schema.tables;");
    query.set_statement_request_timeout(Some(Duration::milliseconds(30000 as i64)));
    let result = query.execute().await.unwrap();

    assert_eq!("keyspace_name", result.column_name(0).unwrap());
    assert_eq!("table_name", result.column_name(1).unwrap());
}
