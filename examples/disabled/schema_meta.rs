use cassandra_cpp::*;

fn print_function(
    session: &Session,
    keyspace: &str,
    function: &str,
    arguments: Vec<&str>,
) -> Result<()> {
    let schema_meta = session.get_schema_meta();
    let keyspace_meta: KeyspaceMeta = schema_meta.get_keyspace_by_name(keyspace);

    let function_meta = keyspace_meta
        .get_function_by_name(function, arguments)
        .unwrap();
    print_function_meta(function_meta, 0);
    Ok(())
}

fn print_function_meta(meta: FunctionMeta, indent: i32) {
    print_indent(indent);
    let name = meta.get_name();
    println!("Function \"name\": {}", name);

    print_meta_fields(meta.fields_iter(), indent + 1);
    println!("");
}

// fn print_schema_map(map: MapIterator) {
//    let mut is_first = true;
//
//    print!("{{ ");
//    for pair in map {
//        if !is_first {
//            print!(", ")
//        }
//        print_schema_value(pair.0);
//        print!(" : ");
//        print_schema_value(pair.1);
//        is_first = false;
//    }
//    print!(" }}");
// }

// fn print_schema_set(set: SetIterator) {
//    let mut is_first = true;
//    print!("{{ ");
//    for item in set {
//        if !is_first {
//            print!(", ")
//        }
//        print_schema_value(item);
//        is_first = false;
//    }
//    print!(" }}");
// }

fn print_aggregate_meta(meta: AggregateMeta, indent: i32) {
    print_indent(indent);
    println!("Aggregate \"{}\":", meta.get_name());
    print_meta_fields(meta.fields_iter(), indent + 1);
    println!("");
}

fn print_meta_fields(iterator: FieldIterator, indent: i32) {
    for item in iterator {
        print_indent(indent);
        println!("{}: ", item.name);
        print_schema_value(item.value);
        println!("");
    }
}

fn print_schema_value(value: Value) {
    // FIXME
    let value = match value.get_type() {
        //        CASS_VALUE_TYPE_INT => value.get_i32().unwrap().to_string(),
        //    CASS_VALUE_TYPE_BOOL => if value.get_bool().unwrap() { "true".to_owned() } else { "false".to_owned() },
        //        CASS_VALUE_TYPE_DOUBLE => value.get_dbl().unwrap().to_string(),
        //
        // CASS_VALUE_TYPE_TEXT | CASS_VALUE_TYPE_ASCII | CASS_VALUE_TYPE_VARCHAR =>
        // value.get_string().unwrap().to_string(),
        //        CASS_VALUE_TYPE_UUID => value.get_uuid().unwrap().to_string(),
        //        CASS_VALUE_TYPE_LIST => {
        //            print_schema_set(value.get_set().unwrap());
        //            "".to_owned()
        //        }
        //        CASS_VALUE_TYPE_MAP => {
        //            print_schema_map(value.get_map().unwrap());
        //            "".to_owned()
        //        }
        //        CASS_VALUE_TYPE_BLOB => {
        //            print_schema_bytes(value.get_bytes().unwrap());
        //            "".to_owned()
        //        }
        _ => "<unhandled type>".to_owned(),
    };
    print!("{}", value);
}

// fn print_schema_bytes(bytes: &[u8]) {
//    print!("0x");
//    for byte in bytes {
//        print!("{}", byte);
//    }
// }

fn main() {
    let result = cass();
    println!("{:?}", result);
}

fn cass() -> Result<()> {
    let mut cluster = Cluster::default();
    cluster.set_contact_points("127.0.0.1").unwrap();
    cluster.set_load_balance_round_robin();

    let create_ks = stmt!(
        "CREATE KEYSPACE IF NOT EXISTS examples WITH replication = { \'class\': \
         \'SimpleStrategy\', \'replication_factor\': \'1\' };"
    );
    let create_table = stmt!(
        "CREATE TABLE IF NOT EXISTS examples.schema_meta (key text, value bigint, PRIMARY KEY \
         (key));"
    );

    let create_func1 = stmt!(
        "CREATE FUNCTION IF NOT EXISTS examples.avg_state(state tuple<int, bigint>, val int) \
         CALLED ON NULL INPUT RETURNS tuple<int, bigint> LANGUAGE java AS 'if (val != null) { \
         state.setInt(0, state.getInt(0) + 1); state.setLong(1, state.getLong(1) + \
         val.intValue()); } return state;';"
    );
    let create_func2 = stmt!(
        "CREATE FUNCTION IF NOT EXISTS examples.avg_final (state tuple<int, bigint>) CALLED ON \
         NULL INPUT RETURNS double LANGUAGE java AS 'double r = 0; if (state.getInt(0) == 0) \
         return null; r = state.getLong(1); r /= state.getInt(0); return Double.valueOf(r);';"
    );

    let create_aggregate = stmt!(
        "CREATE AGGREGATE examples.average(int) SFUNC avg_state STYPE tuple<int, bigint> \
         FINALFUNC avg_final INITCOND(0, 0);"
    );

    match cluster.connect() {
        Ok(ref mut session) => {
            session.execute(&create_ks).wait()?;
            print_keyspace(session, "examples");
            session.execute(&create_table).wait()?;
            session.execute(&create_func1).wait()?;
            session.execute(&create_func2).wait()?;
            session.execute(&create_aggregate).wait()?;
            let schema = &session.get_schema_meta();
            let keyspace = schema.get_keyspace_by_name("examples");
            let mut table = keyspace.table_by_name("schema_meta").unwrap();
            print_table_meta(&mut table, 0);
            print_function(
                session,
                "examples",
                "avg_state",
                vec!["tuple<int,bigint>", "int"],
            )?;
            print_function(session, "examples", "avg_final", vec!["tuple<int,bigint>"])?;
            print_aggregate(session, "examples", "average", vec!["int"])?;
            Ok(())
        }
        _ => panic!(),
    }
}

fn print_aggregate(
    session: &Session,
    keyspace: &str,
    aggregate: &str,
    arguments: Vec<&str>,
) -> Result<()> {
    let schema_meta = session.get_schema_meta();
    let keyspace_meta = schema_meta.get_keyspace_by_name(keyspace);

    let aggregate_meta = keyspace_meta
        .aggregate_by_name(aggregate, arguments)
        .unwrap();
    print_aggregate_meta(aggregate_meta, 0);
    Ok(())
    //    } else {
    //      println!("Unable to find \"{}\" aggregate in the schema metadata", aggregate);
    //    }
    //  } else {
    //    println!("Unable to find \"{}\" keyspace in the schema metadata", keyspace);
    //  }

    // cass_schema_meta_free(schema_meta);
}

fn print_table_meta(meta: &mut TableMeta, indent: i32) {
    print_indent(indent);
    let name = meta.get_name();
    println!("Table \"{}\":\n", name);

    print_meta_fields(meta.field_iter(), indent + 1);
    println!("");

    for mut column in meta.columns_iter() {
        print_column_meta(&mut column, indent + 1);
    }
    println!("");
}

fn print_column_meta(meta: &mut ColumnMeta, indent: i32) {
    print_indent(indent);
    let name = meta.name();
    println!("Column \"{}\":", name);
    print_meta_fields(meta.field_iter(), indent + 1);
    println!("");
}

fn print_indent(indent: i32) {
    for _ in 0..indent {
        print!("\t");
    }
}

fn print_keyspace(session: &Session, keyspace: &str) {
    let schema_meta = session.get_schema_meta();
    let mut keyspace_meta = schema_meta.get_keyspace_by_name(keyspace);
    print_keyspace_meta(&mut keyspace_meta, 0);
}

fn print_keyspace_meta(keyspace_meta: &mut KeyspaceMeta, indent: i32) {
    print_indent(indent);
    let name = keyspace_meta.name();
    println!("Keyspace \"{}\":\n", name);

    print_meta_fields(keyspace_meta.fields_iter(), indent + 1);
    println!("");

    for mut table_meta in keyspace_meta.table_iter() {
        print_table_meta(&mut table_meta, indent + 1);
    }
    println!("");
}
