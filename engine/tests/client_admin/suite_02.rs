use prdb_engine::client_protocol::{
    ClientAdminCommand, ClientFrameDecoder, decode_client_admin_command,
};
use prdb_engine::client_submit::{AdminClient, AdminColumnDefault, AdminColumnDefinition};
use prdb_engine::tuple::{ScalarType, ScalarValue};
use std::net::TcpListener;
use std::time::Duration;

use super::helpers::*;

pub(super) fn admin_client_creates_and_drops_schemas() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let resolve = read_frame(&mut stream, &mut decoder);
        let resolve_payload = String::from_utf8(resolve.payload.clone()).unwrap();
        assert!(resolve_payload.contains("\"operation\":\"metadata.identities.resolve\""));
        assert!(resolve_payload.contains("\"name\":\"app_owner\",\"kind\":\"either\""));
        respond(
            &mut stream,
            &resolve,
            format!("{{\"identity_id\":\"{}\"}}", "01".repeat(16)).as_bytes(),
        );
        for (operation, arguments, operation_id, result) in [
            (
                "schemas.create",
                r#"{"name":"analytics","owner_id":"01010101010101010101010101010101"}"#,
                [0x61; 16],
                format!(
                    "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"schema_id\":\"{}\",\"commit_sequence\":\"41\"}}",
                    hex(&[0x61; 16]),
                    "a3".repeat(16)
                ),
            ),
            (
                "schemas.rename",
                r#"{"schema_id":"a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3","name":"insights"}"#,
                [0x62; 16],
                format!(
                    "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"schema_id\":\"{}\",\"name\":\"insights\",\"commit_sequence\":\"42\"}}",
                    hex(&[0x62; 16]),
                    "a3".repeat(16)
                ),
            ),
            (
                "schemas.set_owner",
                r#"{"schema_id":"a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3","owner_id":"01010101010101010101010101010101"}"#,
                [0x65; 16],
                format!(
                    "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"schema_id\":\"{}\",\"owner_id\":\"{}\",\"commit_sequence\":\"43\"}}",
                    hex(&[0x65; 16]),
                    "a3".repeat(16),
                    "01".repeat(16)
                ),
            ),
            (
                "schemas.drop",
                r#"{"schema_id":"a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3"}"#,
                [0x63; 16],
                format!(
                    "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"schema_id\":\"{}\",\"commit_sequence\":\"44\"}}",
                    hex(&[0x63; 16]),
                    "a3".repeat(16)
                ),
            ),
        ] {
            let prepare = read_frame(&mut stream, &mut decoder);
            let payload = String::from_utf8(prepare.payload.clone()).unwrap();
            assert!(payload.contains(&format!("\"operation\":\"{operation}\"")));
            assert!(
                payload.contains(&format!("\"arguments\":{arguments}")),
                "unexpected payload: {payload}; expected {arguments}"
            );
            respond(
                &mut stream,
                &prepare,
                format!(
                    "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                    hex(&operation_id)
                )
                .as_bytes(),
            );
            let start = read_frame(&mut stream, &mut decoder);
            respond(&mut stream, &start, result.as_bytes());
        }
        let resolve_role = read_frame(&mut stream, &mut decoder);
        let resolve_role_payload = String::from_utf8(resolve_role.payload.clone()).unwrap();
        assert!(resolve_role_payload.contains("\"name\":\"app_admin\",\"kind\":\"role\""));
        respond(
            &mut stream,
            &resolve_role,
            format!("{{\"identity_id\":\"{}\"}}", "02".repeat(16)).as_bytes(),
        );
        let prepare = read_frame(&mut stream, &mut decoder);
        let prepare_payload = String::from_utf8(prepare.payload.clone()).unwrap();
        assert!(prepare_payload.contains("\"operation\":\"schemas.create\""));
        assert!(prepare_payload.contains(&format!(
            "\"arguments\":{{\"name\":\"role_schema\",\"owner_id\":\"{}\"}}",
            "02".repeat(16)
        )));
        respond(
            &mut stream,
            &prepare,
            format!(
                "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                hex(&[0x66; 16])
            )
            .as_bytes(),
        );
        let start = read_frame(&mut stream, &mut decoder);
        respond(
            &mut stream,
            &start,
            format!(
                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"schema_id\":\"{}\",\"commit_sequence\":\"47\"}}",
                hex(&[0x66; 16]),
                "a4".repeat(16)
            )
            .as_bytes(),
        );
        let prepare_default_owner = read_frame(&mut stream, &mut decoder);
        let default_owner_payload =
            String::from_utf8(prepare_default_owner.payload.clone()).unwrap();
        assert!(default_owner_payload.contains("\"operation\":\"schemas.create\""));
        assert!(
            default_owner_payload.contains("\"arguments\":{\"name\":\"current_owner_schema\"}")
        );
        respond(
            &mut stream,
            &prepare_default_owner,
            format!(
                "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                hex(&[0x67; 16])
            )
            .as_bytes(),
        );
        let start = read_frame(&mut stream, &mut decoder);
        respond(
            &mut stream,
            &start,
            format!(
                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"schema_id\":\"{}\",\"commit_sequence\":\"48\"}}",
                hex(&[0x67; 16]),
                "a5".repeat(16)
            )
            .as_bytes(),
        );
    });

    let mut secret = [5; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    let schema = client
        .execute_create_schema_sql("CREATE SCHEMA analytics AUTHORIZATION app_owner;")
        .unwrap();
    assert_eq!(schema.identity_id, [0xa3; 16]);
    client
        .rename_schema(schema.identity_id, "insights")
        .unwrap();
    client
        .set_schema_owner(schema.identity_id, [1; 16])
        .unwrap();
    client.drop_schema(schema.identity_id).unwrap();
    let role_schema = client
        .execute_create_schema_sql("CREATE SCHEMA role_schema AUTHORIZATION ROLE app_admin;")
        .unwrap();
    assert_eq!(role_schema.identity_id, [0xa4; 16]);
    let current_owner_schema = client
        .execute_create_schema_sql("CREATE SCHEMA current_owner_schema;")
        .unwrap();
    assert_eq!(current_owner_schema.identity_id, [0xa5; 16]);
    server.join().unwrap();
}

pub(super) fn admin_client_executes_schema_rename_and_drop_sql_through_control_protocol() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        for (operation, arguments, operation_id, result) in [
            (
                "schemas.rename",
                r#"{"schema_id":"a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3","name":"application"}"#,
                [0x67; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","schema_id":"{}","name":"application","commit_sequence":"45"}}"#,
                    hex(&[0x67; 16]),
                    "a3".repeat(16)
                ),
            ),
            (
                "schemas.drop",
                r#"{"schema_id":"a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3"}"#,
                [0x68; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","schema_id":"{}","commit_sequence":"46"}}"#,
                    hex(&[0x68; 16]),
                    "a3".repeat(16)
                ),
            ),
        ] {
            let schemas = read_frame(&mut stream, &mut decoder);
            assert!(
                String::from_utf8(schemas.payload.clone())
                    .unwrap()
                    .contains(r#""operation":"metadata.schemas.list""#)
            );
            respond(
                &mut stream,
                &schemas,
                format!(
                    r#"{{"schemas":[{{"schema_id":"{}","name":"app"}}],"next_cursor":null}}"#,
                    "a3".repeat(16)
                )
                .as_bytes(),
            );
            let prepare = read_frame(&mut stream, &mut decoder);
            let payload = String::from_utf8(prepare.payload.clone()).unwrap();
            assert!(payload.contains(&format!("\"operation\":\"{operation}\"")));
            assert!(payload.contains(&format!("\"arguments\":{arguments}")));
            respond(
                &mut stream,
                &prepare,
                format!(
                    r#"{{"operation_id":"{}","state":"prepared"}}"#,
                    hex(&operation_id)
                )
                .as_bytes(),
            );
            let start = read_frame(&mut stream, &mut decoder);
            respond(&mut stream, &start, result.as_bytes());
        }
    });

    let mut secret = [5; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    let renamed = client
        .execute_rename_schema_sql("ALTER SCHEMA app RENAME TO application")
        .unwrap();
    assert_eq!(renamed.commit_sequence, 45);
    let dropped = client
        .execute_drop_schema_sql("DROP SCHEMA app RESTRICT")
        .unwrap();
    assert_eq!(dropped.commit_sequence, 46);
    server.join().unwrap();
}

pub(super) fn admin_client_executes_table_rename_and_drop_sql_through_control_protocol() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        for (operation, arguments, operation_id, sequence) in [
            (
                "tables.rename",
                r#"{"table_id":"b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4","name":"archive"}"#,
                [0x69; 16],
                47,
            ),
            (
                "tables.drop",
                r#"{"table_id":"b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4"}"#,
                [0x6a; 16],
                48,
            ),
        ] {
            let schemas = read_frame(&mut stream, &mut decoder);
            assert!(
                String::from_utf8(schemas.payload.clone())
                    .unwrap()
                    .contains(r#""operation":"metadata.schemas.list""#)
            );
            respond(
                &mut stream,
                &schemas,
                format!(
                    r#"{{"schemas":[{{"schema_id":"{}","name":"app"}}],"next_cursor":null}}"#,
                    "a3".repeat(16)
                )
                .as_bytes(),
            );
            let relations = read_frame(&mut stream, &mut decoder);
            let relation_payload = String::from_utf8(relations.payload.clone()).unwrap();
            assert!(relation_payload.contains(r#""operation":"metadata.relations.list""#));
            assert!(relation_payload.contains(&"a3".repeat(16)));
            respond(
                &mut stream,
                &relations,
                format!(r#"{{"relations":[{{"relation_id":"{}","name":"events"}}],"next_cursor":null}}"#, "b4".repeat(16)).as_bytes(),
            );
            let prepare = read_frame(&mut stream, &mut decoder);
            let payload = String::from_utf8(prepare.payload.clone()).unwrap();
            assert!(payload.contains(&format!("\"operation\":\"{operation}\"")));
            assert!(payload.contains(&format!("\"arguments\":{arguments}")));
            respond(
                &mut stream,
                &prepare,
                format!(
                    r#"{{"operation_id":"{}","state":"prepared"}}"#,
                    hex(&operation_id)
                )
                .as_bytes(),
            );
            let start = read_frame(&mut stream, &mut decoder);
            respond(
                &mut stream,
                &start,
                format!(r#"{{"operation_id":"{}","outcome":"committed","table_id":"{}","commit_sequence":"{}"}}"#, hex(&operation_id), "b4".repeat(16), sequence).as_bytes(),
            );
        }
    });

    let mut secret = [5; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    let renamed = client
        .execute_rename_table_sql("ALTER TABLE app.events RENAME TO archive")
        .unwrap();
    assert_eq!(renamed.commit_sequence, 47);
    let dropped = client
        .execute_drop_table_sql("DROP TABLE app.events RESTRICT")
        .unwrap();
    assert_eq!(dropped.commit_sequence, 48);
    server.join().unwrap();
}

pub(super) fn admin_client_adds_key_constraint_from_qualified_sql_names() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let schemas = read_frame(&mut stream, &mut decoder);
        respond(
            &mut stream,
            &schemas,
            format!(
                r#"{{"schemas":[{{"schema_id":"{}","name":"app"}}],"next_cursor":null}}"#,
                "a3".repeat(16)
            )
            .as_bytes(),
        );
        let relations = read_frame(&mut stream, &mut decoder);
        respond(
            &mut stream,
            &relations,
            format!(
                r#"{{"relations":[{{"relation_id":"{}","name":"events"}}],"next_cursor":null}}"#,
                "b4".repeat(16)
            )
            .as_bytes(),
        );
        let columns = read_frame(&mut stream, &mut decoder);
        assert!(
            String::from_utf8(columns.payload.clone())
                .unwrap()
                .contains(r#""operation":"metadata.columns.list""#)
        );
        respond(
            &mut stream,
            &columns,
            format!(r#"{{"columns":[{{"column_id":"{}","name":"tenant","ordinal":0,"scalar_type":"uuid","nullable":false}},{{"column_id":"{}","name":"event_id","ordinal":1,"scalar_type":"integer","nullable":false}}],"next_cursor":null}}"#, "c5".repeat(16), "c6".repeat(16)).as_bytes(),
        );
        let prepare = read_frame(&mut stream, &mut decoder);
        let payload = String::from_utf8(prepare.payload.clone()).unwrap();
        assert!(payload.contains(r#""operation":"constraints.create_unique""#));
        assert!(payload.contains(r#""operation_version":2"#));
        assert!(payload.contains(&format!(
            r#""arguments":{{"table_id":"{}","column_ids":["{}","{}"],"name":"tenant_event_key"}}"#,
            "b4".repeat(16),
            "c5".repeat(16),
            "c6".repeat(16)
        )));
        respond(
            &mut stream,
            &prepare,
            format!(
                r#"{{"operation_id":"{}","state":"prepared"}}"#,
                "d7".repeat(16)
            )
            .as_bytes(),
        );
        let start = read_frame(&mut stream, &mut decoder);
        respond(
            &mut stream,
            &start,
            format!(r#"{{"operation_id":"{}","outcome":"committed","constraint_id":"{}","index_id":"{}","commit_sequence":"77"}}"#, "d7".repeat(16), "e8".repeat(16), "e9".repeat(16)).as_bytes(),
        );
    });

    let mut secret = [5; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    let created = client
        .execute_add_key_constraint_sql(
            "ALTER TABLE app.events ADD CONSTRAINT tenant_event_key UNIQUE (tenant, event_id)",
        )
        .unwrap();
    assert_eq!(created.constraint_id, [0xe8; 16]);
    assert_eq!(created.index_id, [0xe9; 16]);
    assert_eq!(created.commit_sequence, 77);
    server.join().unwrap();
}

pub(super) fn admin_client_binds_foreign_key_sql_to_visible_columns_and_unique_target() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let replies = [
            (
                "metadata.schemas.list",
                format!(
                    r#"{{"schemas":[{{"schema_id":"{}","name":"app"}}],"next_cursor":null}}"#,
                    "a3".repeat(16)
                ),
            ),
            (
                "metadata.relations.list",
                format!(
                    r#"{{"relations":[{{"relation_id":"{}","name":"orders"}}],"next_cursor":null}}"#,
                    "b4".repeat(16)
                ),
            ),
            (
                "metadata.schemas.list",
                format!(
                    r#"{{"schemas":[{{"schema_id":"{}","name":"app"}}],"next_cursor":null}}"#,
                    "a3".repeat(16)
                ),
            ),
            (
                "metadata.relations.list",
                format!(
                    r#"{{"relations":[{{"relation_id":"{}","name":"customers"}}],"next_cursor":null}}"#,
                    "b5".repeat(16)
                ),
            ),
            (
                "metadata.columns.list",
                format!(
                    r#"{{"columns":[{{"column_id":"{}","name":"customer_id","ordinal":0,"scalar_type":"integer","nullable":false}}],"next_cursor":null}}"#,
                    "c5".repeat(16)
                ),
            ),
            (
                "metadata.columns.list",
                format!(
                    r#"{{"columns":[{{"column_id":"{}","name":"id","ordinal":0,"scalar_type":"integer","nullable":false}}],"next_cursor":null}}"#,
                    "c6".repeat(16)
                ),
            ),
            (
                "metadata.constraints.list",
                format!(
                    r#"{{"constraints":[{{"constraint_id":"{}","name":"pk_orders","kind":"primary_key","column_ids":["{}"],"referenced_table_id":null,"referenced_column_ids":[]}}],"next_cursor":null}}"#,
                    "d7".repeat(16),
                    "c6".repeat(16)
                ),
            ),
        ];
        for (operation, body) in replies {
            let request = read_frame(&mut stream, &mut decoder);
            assert!(
                String::from_utf8_lossy(&request.payload)
                    .contains(&format!(r#""operation":"{operation}""#))
            );
            respond(&mut stream, &request, body.as_bytes());
        }
        let prepare = read_frame(&mut stream, &mut decoder);
        let payload = String::from_utf8(prepare.payload.clone()).unwrap();
        assert!(payload.contains(r#""operation":"foreign_keys.create""#));
        assert!(payload.contains(r#""operation_version":2"#));
        assert!(payload.contains(&format!(
            r#""arguments":{{"source_table_id":"{}","source_column_ids":["{}"],"target_constraint_id":"{}","name":"orders_customer_fk"}}"#,
            "b4".repeat(16),
            "c5".repeat(16),
            "d7".repeat(16)
        )));
        respond(
            &mut stream,
            &prepare,
            format!(
                r#"{{"operation_id":"{}","state":"prepared"}}"#,
                "e8".repeat(16)
            )
            .as_bytes(),
        );
        let start = read_frame(&mut stream, &mut decoder);
        respond(
            &mut stream,
            &start,
            format!(r#"{{"operation_id":"{}","outcome":"committed","foreign_key_id":"{}","commit_sequence":"79"}}"#, "e8".repeat(16), "f9".repeat(16)).as_bytes(),
        );
    });

    let mut secret = [5; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    let created = client
        .execute_add_foreign_key_sql(
            "ALTER TABLE app.orders ADD CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id) REFERENCES app.customers (id)",
        )
        .unwrap();
    assert_eq!(created.foreign_key_id, [0xf9; 16]);
    assert_eq!(created.commit_sequence, 79);
    server.join().unwrap();
}

pub(super) fn admin_client_drops_only_a_foreign_key_attached_to_the_selected_table() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        for (operation, body) in [
            (
                "metadata.schemas.list",
                format!(
                    r#"{{"schemas":[{{"schema_id":"{}","name":"app"}}],"next_cursor":null}}"#,
                    "a3".repeat(16)
                ),
            ),
            (
                "metadata.relations.list",
                format!(
                    r#"{{"relations":[{{"relation_id":"{}","name":"orders"}}],"next_cursor":null}}"#,
                    "b4".repeat(16)
                ),
            ),
            (
                "metadata.constraints.list",
                format!(
                    r#"{{"constraints":[{{"constraint_id":"{}","name":"fk_orders_customer","kind":"foreign_key","column_ids":["{}"],"referenced_table_id":"{}","referenced_column_ids":["{}"]}}],"next_cursor":null}}"#,
                    "c5".repeat(16),
                    "c6".repeat(16),
                    "d7".repeat(16),
                    "e8".repeat(16)
                ),
            ),
        ] {
            let request = read_frame(&mut stream, &mut decoder);
            assert!(
                String::from_utf8_lossy(&request.payload)
                    .contains(&format!(r#""operation":"{operation}""#))
            );
            respond(&mut stream, &request, body.as_bytes());
        }
        let prepare = read_frame(&mut stream, &mut decoder);
        let payload = String::from_utf8(prepare.payload.clone()).unwrap();
        assert!(payload.contains(r#""operation":"foreign_keys.drop""#));
        assert!(payload.contains(&format!(
            r#""arguments":{{"foreign_key_id":"{}"}}"#,
            "c5".repeat(16)
        )));
        respond(
            &mut stream,
            &prepare,
            format!(
                r#"{{"operation_id":"{}","state":"prepared"}}"#,
                "f9".repeat(16)
            )
            .as_bytes(),
        );
        let start = read_frame(&mut stream, &mut decoder);
        respond(
            &mut stream,
            &start,
            format!(r#"{{"operation_id":"{}","outcome":"committed","foreign_key_id":"{}","commit_sequence":"80"}}"#, "f9".repeat(16), "c5".repeat(16)).as_bytes(),
        );
    });

    let mut secret = [5; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    let result = client
        .execute_drop_constraint_sql(
            "ALTER TABLE app.orders DROP CONSTRAINT 'c5c5c5c5-c5c5-c5c5-c5c5-c5c5c5c5c5c5'",
        )
        .unwrap();
    assert_eq!(result.commit_sequence, 80);
    server.join().unwrap();
}

pub(super) fn admin_client_drops_primary_key_from_generated_constraint_sql() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        for (operation, body) in [
            (
                "metadata.schemas.list",
                format!(
                    r#"{{"schemas":[{{"schema_id":"{}","name":"app"}}],"next_cursor":null}}"#,
                    "a3".repeat(16)
                ),
            ),
            (
                "metadata.relations.list",
                format!(
                    r#"{{"relations":[{{"relation_id":"{}","name":"orders"}}],"next_cursor":null}}"#,
                    "b4".repeat(16)
                ),
            ),
            (
                "metadata.constraints.list",
                format!(
                    r#"{{"constraints":[{{"constraint_id":"{}","name":"pk_orders","kind":"primary_key","column_ids":["{}"],"referenced_table_id":null,"referenced_column_ids":[]}}],"next_cursor":null}}"#,
                    "c5".repeat(16),
                    "d6".repeat(16)
                ),
            ),
        ] {
            let request = read_frame(&mut stream, &mut decoder);
            assert!(
                String::from_utf8_lossy(&request.payload)
                    .contains(&format!(r#""operation":"{operation}""#))
            );
            respond(&mut stream, &request, body.as_bytes());
        }
        let prepare = read_frame(&mut stream, &mut decoder);
        let payload = String::from_utf8(prepare.payload.clone()).unwrap();
        assert!(payload.contains(r#""operation":"constraints.drop_primary_key""#));
        assert!(payload.contains(&format!(
            r#""arguments":{{"constraint_id":"{}"}}"#,
            "c5".repeat(16)
        )));
        respond(
            &mut stream,
            &prepare,
            format!(
                r#"{{"operation_id":"{}","state":"prepared"}}"#,
                "e7".repeat(16)
            )
            .as_bytes(),
        );
        let start = read_frame(&mut stream, &mut decoder);
        respond(
            &mut stream,
            &start,
            format!(
                r#"{{"operation_id":"{}","outcome":"committed","constraint_id":"{}","commit_sequence":"82"}}"#,
                "e7".repeat(16),
                "c5".repeat(16)
            )
            .as_bytes(),
        );
    });

    let mut secret = [5; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    let dropped = client
        .execute_drop_constraint_sql(
            "ALTER TABLE app.orders DROP CONSTRAINT 'c5c5c5c5-c5c5-c5c5-c5c5-c5c5c5c5c5c5'",
        )
        .unwrap();
    assert_eq!(dropped.commit_sequence, 82);
    server.join().unwrap();
}

pub(super) fn admin_client_binds_check_sql_names_to_eir_dependencies() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        for (operation, body) in [
            (
                "metadata.schemas.list",
                format!(
                    r#"{{"schemas":[{{"schema_id":"{}","name":"app"}}],"next_cursor":null}}"#,
                    "a3".repeat(16)
                ),
            ),
            (
                "metadata.relations.list",
                format!(
                    r#"{{"relations":[{{"relation_id":"{}","name":"events"}}],"next_cursor":null}}"#,
                    "b4".repeat(16)
                ),
            ),
            (
                "metadata.columns.list",
                format!(
                    r#"{{"columns":[{{"column_id":"{}","name":"enabled","ordinal":0,"scalar_type":"boolean","nullable":true}},{{"column_id":"{}","name":"deleted","ordinal":1,"scalar_type":"integer","nullable":true}}],"next_cursor":null}}"#,
                    "c5".repeat(16),
                    "c6".repeat(16)
                ),
            ),
        ] {
            let request = read_frame(&mut stream, &mut decoder);
            assert!(
                String::from_utf8_lossy(&request.payload)
                    .contains(&format!(r#""operation":"{operation}""#))
            );
            respond(&mut stream, &request, body.as_bytes());
        }
        let prepare = read_frame(&mut stream, &mut decoder);
        let payload = String::from_utf8(prepare.payload.clone()).unwrap();
        assert!(payload.contains(r#""operation":"constraints.create_check""#));
        assert!(payload.contains(r#""operation_version":2"#));
        assert!(payload.contains(r#""name":"positive_amount""#));
        assert!(payload.contains(&format!(
            r#""dependency_column_ids":["{}","{}"]"#,
            "c5".repeat(16),
            "c6".repeat(16)
        )));
        assert!(payload.contains(r#""expression_eir1":""#));
        assert!(matches!(
            decode_client_admin_command(&prepare.payload),
            Ok(ClientAdminCommand::PrepareCreateCheckConstraint {
                expression,
                dependency_column_ids,
                ..
            }) if expression == prdb_engine::authorization::Expr::And(vec![
                prdb_engine::authorization::Expr::Not(Box::new(
                    prdb_engine::authorization::Expr::IsNull(Box::new(
                        prdb_engine::authorization::Expr::And(vec![
                            prdb_engine::authorization::Expr::Field(0),
                            prdb_engine::authorization::Expr::Or(vec![
                                prdb_engine::authorization::Expr::GreaterEqual(
                                    Box::new(prdb_engine::authorization::Expr::Field(1)),
                                    Box::new(prdb_engine::authorization::Expr::Literal(Some(ScalarValue::Integer(-2)))),
                                ),
                                prdb_engine::authorization::Expr::IsNull(Box::new(
                                    prdb_engine::authorization::Expr::GreaterEqual(
                                        Box::new(prdb_engine::authorization::Expr::Field(1)),
                                        Box::new(prdb_engine::authorization::Expr::Literal(Some(ScalarValue::Integer(-2)))),
                                    ),
                                )),
                            ]),
                        ]),
                    )),
                )),
                prdb_engine::authorization::Expr::Equal(
                    Box::new(prdb_engine::authorization::Expr::And(vec![
                        prdb_engine::authorization::Expr::Field(0),
                        prdb_engine::authorization::Expr::Or(vec![
                            prdb_engine::authorization::Expr::GreaterEqual(
                                Box::new(prdb_engine::authorization::Expr::Field(1)),
                                Box::new(prdb_engine::authorization::Expr::Literal(Some(ScalarValue::Integer(-2)))),
                            ),
                            prdb_engine::authorization::Expr::IsNull(Box::new(
                            prdb_engine::authorization::Expr::GreaterEqual(
                                Box::new(prdb_engine::authorization::Expr::Field(1)),
                                Box::new(prdb_engine::authorization::Expr::Literal(Some(ScalarValue::Integer(-2)))),
                            ),
                            )),
                        ]),
                    ])),
                    Box::new(prdb_engine::authorization::Expr::Literal(Some(ScalarValue::Boolean(true)))),
                ),
            ]) && dependency_column_ids == [[0xc5; 16], [0xc6; 16]]
        ));
        respond(
            &mut stream,
            &prepare,
            format!(
                r#"{{"operation_id":"{}","state":"prepared"}}"#,
                "d7".repeat(16)
            )
            .as_bytes(),
        );
        let start = read_frame(&mut stream, &mut decoder);
        respond(&mut stream, &start, format!(r#"{{"operation_id":"{}","outcome":"committed","constraint_id":"{}","expression_id":"{}","commit_sequence":"78"}}"#, "d7".repeat(16), "e8".repeat(16), "e9".repeat(16)).as_bytes());
    });

    let mut secret = [5; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    let created = client
        .execute_add_check_constraint_sql(
            "ALTER TABLE app.events ADD CONSTRAINT positive_amount CHECK ((enabled AND (deleted >= -2 OR (deleted >= -2) IS NULL)) IS TRUE)",
        )
        .unwrap();
    assert_eq!(created.constraint_id, [0xe8; 16]);
    assert_eq!(created.expression_id, [0xe9; 16]);
    assert_eq!(created.commit_sequence, 78);
    server.join().unwrap();
}

pub(super) fn admin_client_binds_nullability_sql_to_metadata_ids() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        for (operation, body) in [
            (
                "metadata.schemas.list",
                format!(
                    r#"{{"schemas":[{{"schema_id":"{}","name":"app"}}],"next_cursor":null}}"#,
                    "a3".repeat(16)
                ),
            ),
            (
                "metadata.relations.list",
                format!(
                    r#"{{"relations":[{{"relation_id":"{}","name":"employees"}}],"next_cursor":null}}"#,
                    "b4".repeat(16)
                ),
            ),
            (
                "metadata.columns.list",
                format!(
                    r#"{{"columns":[{{"column_id":"{}","name":"active","ordinal":0,"scalar_type":"boolean","nullable":true}}],"next_cursor":null}}"#,
                    "c5".repeat(16)
                ),
            ),
        ] {
            let request = read_frame(&mut stream, &mut decoder);
            assert!(
                String::from_utf8_lossy(&request.payload)
                    .contains(&format!(r#""operation":"{operation}""#))
            );
            respond(&mut stream, &request, body.as_bytes());
        }
        let prepare = read_frame(&mut stream, &mut decoder);
        let payload = String::from_utf8(prepare.payload.clone()).unwrap();
        assert!(payload.contains(r#""operation":"tables.alter_column_nullability""#));
        assert!(payload.contains(&format!(
            r#""arguments":{{"table_id":"{}","column_id":"{}","nullable":"false"}}"#,
            "b4".repeat(16),
            "c5".repeat(16)
        )));
        respond(
            &mut stream,
            &prepare,
            format!(
                r#"{{"operation_id":"{}","state":"prepared"}}"#,
                "d7".repeat(16)
            )
            .as_bytes(),
        );
        let start = read_frame(&mut stream, &mut decoder);
        respond(&mut stream, &start, format!(r#"{{"operation_id":"{}","outcome":"committed","table_id":"{}","column_id":"{}","nullable":false,"commit_sequence":"79"}}"#, "d7".repeat(16), "b4".repeat(16), "c5".repeat(16)).as_bytes());
    });

    let mut secret = [5; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    let altered = client
        .execute_alter_column_nullability_sql(
            "ALTER TABLE app.employees ALTER COLUMN active SET NOT NULL",
        )
        .unwrap();
    assert_eq!(altered.commit_sequence, 79);
    server.join().unwrap();
}

pub(super) fn admin_client_binds_column_default_sql_to_metadata_type() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        for (operation, body) in [
            (
                "metadata.schemas.list",
                format!(
                    r#"{{"schemas":[{{"schema_id":"{}","name":"app"}}],"next_cursor":null}}"#,
                    "a3".repeat(16)
                ),
            ),
            (
                "metadata.relations.list",
                format!(
                    r#"{{"relations":[{{"relation_id":"{}","name":"employees"}}],"next_cursor":null}}"#,
                    "b4".repeat(16)
                ),
            ),
            (
                "metadata.columns.list",
                format!(
                    r#"{{"columns":[{{"column_id":"{}","name":"priority","ordinal":0,"scalar_type":"integer","nullable":false}}],"next_cursor":null}}"#,
                    "c5".repeat(16)
                ),
            ),
        ] {
            let request = read_frame(&mut stream, &mut decoder);
            assert!(
                String::from_utf8_lossy(&request.payload)
                    .contains(&format!(r#""operation":"{operation}""#))
            );
            respond(&mut stream, &request, body.as_bytes());
        }
        let prepare = read_frame(&mut stream, &mut decoder);
        let payload = String::from_utf8(prepare.payload.clone()).unwrap();
        assert!(payload.contains(r#""operation":"tables.set_column_default""#));
        assert!(payload.contains(&format!(
            r#""arguments":{{"table_id":"{}","column_id":"{}","type":"integer","default":{{"kind":"literal","value":"42"}} }}"#,
            "b4".repeat(16),
            "c5".repeat(16)
        )));
        respond(
            &mut stream,
            &prepare,
            format!(
                r#"{{"operation_id":"{}","state":"prepared"}}"#,
                "d7".repeat(16)
            )
            .as_bytes(),
        );
        let start = read_frame(&mut stream, &mut decoder);
        respond(&mut stream, &start, format!(r#"{{"operation_id":"{}","outcome":"committed","table_id":"{}","column_id":"{}","commit_sequence":"80"}}"#, "d7".repeat(16), "b4".repeat(16), "c5".repeat(16)).as_bytes());
    });

    let mut secret = [5; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    let altered = client
        .execute_alter_column_default_sql(
            "ALTER TABLE app.employees ALTER COLUMN priority SET DEFAULT 42",
        )
        .unwrap();
    assert_eq!(altered.commit_sequence, 80);
    server.join().unwrap();
}

pub(super) fn admin_client_adds_typed_column_from_sql_names() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        for (operation, body) in [
            (
                "metadata.schemas.list",
                format!(
                    r#"{{"schemas":[{{"schema_id":"{}","name":"app"}}],"next_cursor":null}}"#,
                    "a3".repeat(16)
                ),
            ),
            (
                "metadata.relations.list",
                format!(
                    r#"{{"relations":[{{"relation_id":"{}","name":"events"}}],"next_cursor":null}}"#,
                    "b4".repeat(16)
                ),
            ),
        ] {
            let request = read_frame(&mut stream, &mut decoder);
            assert!(
                String::from_utf8_lossy(&request.payload)
                    .contains(&format!(r#""operation":"{operation}""#))
            );
            respond(&mut stream, &request, body.as_bytes());
        }
        let prepare = read_frame(&mut stream, &mut decoder);
        let payload = String::from_utf8(prepare.payload.clone()).unwrap();
        assert!(payload.contains(r#""operation":"tables.add_column""#));
        assert!(
            payload.contains(r#""table_id":"b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4b4","column":{"name":"label","type":"varchar(30)","nullable":false,"generated_uuid_identity":false,"default":{"kind":"literal","value":"unknown"}}"#),
            "unexpected PREPARE payload: {payload}"
        );
        respond(
            &mut stream,
            &prepare,
            format!(
                r#"{{"operation_id":"{}","state":"prepared"}}"#,
                "d7".repeat(16)
            )
            .as_bytes(),
        );
        let start = read_frame(&mut stream, &mut decoder);
        respond(&mut stream, &start, format!(r#"{{"operation_id":"{}","outcome":"committed","table_id":"{}","column_id":"{}","schema_version":"2","commit_sequence":"80"}}"#, "d7".repeat(16), "b4".repeat(16), "e8".repeat(16)).as_bytes());
    });

    let mut secret = [5; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    let added = client
        .execute_add_column_sql(
            "ALTER TABLE app.events ADD COLUMN label VARCHAR(30) NOT NULL DEFAULT 'unknown'",
        )
        .unwrap();
    assert_eq!(added.column_id, [0xe8; 16]);
    assert_eq!(added.commit_sequence, 80);
    server.join().unwrap();
}

pub(super) fn admin_client_renames_column_from_qualified_sql_names() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        for (operation, body) in [
            (
                "metadata.schemas.list",
                format!(
                    r#"{{"schemas":[{{"schema_id":"{}","name":"app"}}],"next_cursor":null}}"#,
                    "a3".repeat(16)
                ),
            ),
            (
                "metadata.relations.list",
                format!(
                    r#"{{"relations":[{{"relation_id":"{}","name":"events"}}],"next_cursor":null}}"#,
                    "b4".repeat(16)
                ),
            ),
            (
                "metadata.columns.list",
                format!(
                    r#"{{"columns":[{{"column_id":"{}","name":"old_name","ordinal":0,"scalar_type":"text","nullable":true}}],"next_cursor":null}}"#,
                    "c5".repeat(16)
                ),
            ),
        ] {
            let request = read_frame(&mut stream, &mut decoder);
            assert!(
                String::from_utf8_lossy(&request.payload)
                    .contains(&format!(r#""operation":"{operation}""#))
            );
            respond(&mut stream, &request, body.as_bytes());
        }
        let prepare = read_frame(&mut stream, &mut decoder);
        let payload = String::from_utf8(prepare.payload.clone()).unwrap();
        assert!(payload.contains(r#""operation":"tables.rename_column""#));
        assert!(payload.contains(&format!(
            r#""arguments":{{"table_id":"{}","column_id":"{}","name":"renamed"}}"#,
            "b4".repeat(16),
            "c5".repeat(16)
        )));
        respond(
            &mut stream,
            &prepare,
            format!(
                r#"{{"operation_id":"{}","state":"prepared"}}"#,
                "d7".repeat(16)
            )
            .as_bytes(),
        );
        let start = read_frame(&mut stream, &mut decoder);
        respond(&mut stream, &start, format!(r#"{{"operation_id":"{}","outcome":"committed","table_id":"{}","column_id":"{}","name":"renamed","commit_sequence":"81"}}"#, "d7".repeat(16), "b4".repeat(16), "c5".repeat(16)).as_bytes());
    });

    let mut secret = [5; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    let renamed = client
        .execute_rename_column_sql("ALTER TABLE app.events RENAME COLUMN old_name TO renamed")
        .unwrap();
    assert_eq!(renamed.column_id, [0xc5; 16]);
    assert_eq!(renamed.commit_sequence, 81);
    server.join().unwrap();
}

pub(super) fn admin_client_drops_column_from_qualified_sql_names() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        for (operation, body) in [
            (
                "metadata.schemas.list",
                format!(
                    r#"{{"schemas":[{{"schema_id":"{}","name":"app"}}],"next_cursor":null}}"#,
                    "a3".repeat(16)
                ),
            ),
            (
                "metadata.relations.list",
                format!(
                    r#"{{"relations":[{{"relation_id":"{}","name":"events"}}],"next_cursor":null}}"#,
                    "b4".repeat(16)
                ),
            ),
            (
                "metadata.columns.list",
                format!(
                    r#"{{"columns":[{{"column_id":"{}","name":"obsolete","ordinal":0,"scalar_type":"text","nullable":true}}],"next_cursor":null}}"#,
                    "c5".repeat(16)
                ),
            ),
        ] {
            let request = read_frame(&mut stream, &mut decoder);
            assert!(
                String::from_utf8_lossy(&request.payload)
                    .contains(&format!(r#""operation":"{operation}""#))
            );
            respond(&mut stream, &request, body.as_bytes());
        }
        let prepare = read_frame(&mut stream, &mut decoder);
        let payload = String::from_utf8(prepare.payload.clone()).unwrap();
        assert!(payload.contains(r#""operation":"tables.drop_column""#));
        assert!(payload.contains(&format!(
            r#""arguments":{{"table_id":"{}","column_id":"{}"}}"#,
            "b4".repeat(16),
            "c5".repeat(16)
        )));
        respond(
            &mut stream,
            &prepare,
            format!(
                r#"{{"operation_id":"{}","state":"prepared"}}"#,
                "d7".repeat(16)
            )
            .as_bytes(),
        );
        let start = read_frame(&mut stream, &mut decoder);
        respond(&mut stream, &start, format!(r#"{{"operation_id":"{}","outcome":"committed","table_id":"{}","column_id":"{}","commit_sequence":"82"}}"#, "d7".repeat(16), "b4".repeat(16), "c5".repeat(16)).as_bytes());
    });

    let mut secret = [5; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    let dropped = client
        .execute_drop_column_sql("ALTER TABLE app.events DROP COLUMN obsolete RESTRICT")
        .unwrap();
    assert_eq!(dropped.column_id, [0xc5; 16]);
    assert_eq!(dropped.commit_sequence, 82);
    server.join().unwrap();
}

pub(super) fn admin_client_renames_and_drops_tables() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        for (operation, arguments, operation_id, sequence) in [
            (
                "tables.rename",
                format!(r#"{{"table_id":"{}","name":"events_v2"}}"#, "a4".repeat(16)),
                [0x71; 16],
                51,
            ),
            (
                "tables.drop",
                format!(r#"{{"table_id":"{}"}}"#, "a4".repeat(16)),
                [0x72; 16],
                52,
            ),
        ] {
            let prepare = read_frame(&mut stream, &mut decoder);
            let payload = String::from_utf8(prepare.payload.clone()).unwrap();
            assert!(payload.contains(&format!("\"operation\":\"{operation}\"")));
            assert!(payload.contains(&format!("\"arguments\":{arguments}")));
            respond(
                &mut stream,
                &prepare,
                format!(
                    "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                    hex(&operation_id)
                )
                .as_bytes(),
            );
            let start = read_frame(&mut stream, &mut decoder);
            respond(
                &mut stream,
                &start,
                format!(
                    "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"table_id\":\"{}\",\"commit_sequence\":\"{sequence}\"}}",
                    hex(&operation_id),
                    "a4".repeat(16)
                )
                .as_bytes(),
            );
        }
    });

    let mut secret = [5; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    client.rename_table([0xa4; 16], "events_v2").unwrap();
    client.drop_table([0xa4; 16]).unwrap();
    server.join().unwrap();
}

pub(super) fn admin_client_creates_table_with_typed_columns_and_defaults() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let prepare = read_frame(&mut stream, &mut decoder);
        let payload = String::from_utf8(prepare.payload.clone()).unwrap();
        assert_eq!(
            payload,
            r#"{"phase":"prepare","operation_version":1,"operation":"tables.create","arguments":{"schema_id":"01010101010101010101010101010101","name":"events","columns":[{"name":"id","type":"uuid","nullable":false,"generated_uuid_identity":true},{"name":"title","type":"text","nullable":false,"generated_uuid_identity":false,"default":{"kind":"literal","value":"hello \"db\""}},{"name":"amount","type":"decimal(6,2)","nullable":true,"generated_uuid_identity":false,"default":{"kind":"literal","value":"12.34"}},{"name":"owner","type":"uuid","nullable":false,"generated_uuid_identity":false,"default":{"kind":"current_principal_id"}}]}}"#
        );
        let operation_id = [0x81; 16];
        respond(
            &mut stream,
            &prepare,
            format!(
                "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                hex(&operation_id)
            )
            .as_bytes(),
        );
        let start = read_frame(&mut stream, &mut decoder);
        respond(
            &mut stream,
            &start,
            format!(
                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"table_id\":\"{}\",\"column_ids\":[\"{}\",\"{}\",\"{}\",\"{}\"],\"commit_sequence\":\"61\"}}",
                hex(&operation_id),
                "a5".repeat(16),
                "b1".repeat(16),
                "b2".repeat(16),
                "b3".repeat(16),
                "b4".repeat(16)
            )
            .as_bytes(),
        );
    });

    let columns = [
        AdminColumnDefinition {
            name: "id".into(),
            scalar_type: ScalarType::Uuid,
            nullable: false,
            generated_uuid_identity: true,
            default: None,
        },
        AdminColumnDefinition {
            name: "title".into(),
            scalar_type: ScalarType::Text,
            nullable: false,
            generated_uuid_identity: false,
            default: Some(AdminColumnDefault::Literal(ScalarValue::Text(
                "hello \"db\"".into(),
            ))),
        },
        AdminColumnDefinition {
            name: "amount".into(),
            scalar_type: ScalarType::Decimal {
                precision: 6,
                scale: 2,
            },
            nullable: true,
            generated_uuid_identity: false,
            default: Some(AdminColumnDefault::Literal(ScalarValue::Decimal(1234))),
        },
        AdminColumnDefinition {
            name: "owner".into(),
            scalar_type: ScalarType::Uuid,
            nullable: false,
            generated_uuid_identity: false,
            default: Some(AdminColumnDefault::CurrentPrincipalId),
        },
    ];
    let mut secret = [5; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    let created = client.create_table([1; 16], "events", &columns).unwrap();
    assert_eq!(created.table_id, [0xa5; 16]);
    assert_eq!(
        created.column_ids,
        [[0xb1; 16], [0xb2; 16], [0xb3; 16], [0xb4; 16]]
    );
    assert_eq!(created.commit_sequence, 61);
    server.join().unwrap();
}

pub(super) fn admin_client_executes_create_table_sql_on_authenticated_control_channel() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let schemas = read_frame(&mut stream, &mut decoder);
        assert!(
            String::from_utf8(schemas.payload.clone())
                .unwrap()
                .contains(r#""operation":"metadata.schemas.list""#)
        );
        respond(
            &mut stream,
            &schemas,
            format!(
                r#"{{"schemas":[{{"schema_id":"{}","name":"app"}}],"next_cursor":null}}"#,
                "01".repeat(16)
            )
            .as_bytes(),
        );
        let prepare = read_frame(&mut stream, &mut decoder);
        let payload = String::from_utf8(prepare.payload.clone()).unwrap();
        assert!(payload.contains(r#""operation":"tables.create""#));
        assert!(payload.contains(r#""name":"events""#));
        assert!(payload.contains(r#""generated_uuid_identity":true"#));
        assert!(payload.contains(r#""primary_key_columns":["id"]"#));
        assert!(payload.contains(r#""unique_constraints":[["amount"]]"#));
        assert!(payload.contains(r#""operation_version":4"#));
        assert!(payload.contains(r#""primary_key_name":"events_pk""#));
        assert!(payload.contains(r#""unique_constraint_names":["amount_key"]"#));
        assert!(payload.contains(r#""foreign_key_names":["owner_fk"]"#));
        assert!(payload.contains(r#""check_constraint_names":["amount_valid","title_nonempty"]"#));
        assert!(payload.contains(r#""type":"decimal(8,2)""#));
        assert!(payload.contains(r#""default":{"kind":"literal","value":"-1.20"}"#));
        assert!(payload.contains(r#""value":"hello 'sql'""#));
        let decoded = decode_client_admin_command(&prepare.payload).unwrap();
        assert!(matches!(
            decoded,
            ClientAdminCommand::PrepareCreateTable { check_expressions, check_constraint_names, primary_key_name, unique_constraint_names, foreign_key_names, .. }
                if check_constraint_names == [Some("amount_valid".to_owned()), Some("title_nonempty".to_owned())]
                    && primary_key_name.as_deref() == Some("events_pk")
                    && unique_constraint_names == [Some("amount_key".to_owned())]
                    && foreign_key_names == [Some("owner_fk".to_owned())]
                    && check_expressions == [
                        prdb_engine::authorization::Expr::GreaterEqual(
                            Box::new(prdb_engine::authorization::Expr::Field(1)),
                            Box::new(prdb_engine::authorization::Expr::Literal(Some(ScalarValue::Decimal(-120)))),
                        ),
                        prdb_engine::authorization::Expr::Not(Box::new(
                            prdb_engine::authorization::Expr::IsNull(Box::new(
                                prdb_engine::authorization::Expr::Field(2),
                            )),
                        )),
                    ]
        ));
        let operation_id = [0x81; 16];
        respond(
            &mut stream,
            &prepare,
            format!(
                r#"{{"operation_id":"{}","state":"prepared"}}"#,
                hex(&operation_id)
            )
            .as_bytes(),
        );
        let start = read_frame(&mut stream, &mut decoder);
        respond(
            &mut stream,
            &start,
            format!(r#"{{"operation_id":"{}","outcome":"committed","table_id":"{}","column_ids":["{}","{}","{}","{}"],"commit_sequence":"62"}}"#, hex(&operation_id), "a5".repeat(16), "b1".repeat(16), "b2".repeat(16), "b3".repeat(16), "b4".repeat(16)).as_bytes(),
        );
    });

    let mut secret = [5; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    let created = client
        .execute_create_table_sql(
            "CREATE TABLE app.events (id UUID GENERATED ALWAYS AS IDENTITY CONSTRAINT events_pk PRIMARY KEY, amount DECIMAL(8, 2) DEFAULT -1.20 CONSTRAINT amount_key UNIQUE CONSTRAINT amount_valid CHECK (amount >= -1.20), title TEXT DEFAULT 'hello ''sql''' CONSTRAINT title_nonempty CHECK (title IS NOT NULL), owner UUID DEFAULT CURRENT_PRINCIPAL_ID() CONSTRAINT owner_fk REFERENCES app.events (id));",
        )
        .unwrap();
    assert_eq!(created.table_id, [0xa5; 16]);
    assert_eq!(
        created.column_ids,
        [[0xb1; 16], [0xb2; 16], [0xb3; 16], [0xb4; 16]]
    );
    assert_eq!(created.commit_sequence, 62);
    server.join().unwrap();
}

pub(super) fn admin_client_executes_self_referencing_create_table_sql_with_v2_payload() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let schemas = read_frame(&mut stream, &mut decoder);
        assert!(
            String::from_utf8_lossy(&schemas.payload)
                .contains(r#""operation":"metadata.schemas.list""#)
        );
        respond(
            &mut stream,
            &schemas,
            format!(
                r#"{{"schemas":[{{"schema_id":"{}","name":"app"}}],"next_cursor":null}}"#,
                "01".repeat(16)
            )
            .as_bytes(),
        );
        let prepare = read_frame(&mut stream, &mut decoder);
        let payload = String::from_utf8(prepare.payload.clone()).unwrap();
        assert!(
            payload.contains(r#""operation_version":2,"#),
            "expected tables.create v2 payload, got {payload}"
        );
        assert!(payload.contains(r#""foreign_key_source_columns":[["parent_id"]],"#));
        assert!(payload.contains(
            r#""foreign_key_target_constraint_ids":["00000000000000000000000000000000"],"#
        ));
        assert!(payload.contains(r#""foreign_key_target_columns":[["id"]]"#));
        assert!(matches!(
            decode_client_admin_command(&prepare.payload),
            Ok(ClientAdminCommand::PrepareCreateTable { foreign_keys, .. })
                if foreign_keys.len() == 1
                    && foreign_keys[0].source_columns == ["parent_id"]
                    && foreign_keys[0].target_columns.as_deref() == Some(["id".to_owned()].as_slice())
        ));
        let operation_id = [0x82; 16];
        respond(
            &mut stream,
            &prepare,
            format!(
                r#"{{"operation_id":"{}","state":"prepared"}}"#,
                hex(&operation_id)
            )
            .as_bytes(),
        );
        let start = read_frame(&mut stream, &mut decoder);
        respond(
            &mut stream,
            &start,
            format!(
                r#"{{"operation_id":"{}","outcome":"committed","table_id":"{}","column_ids":["{}","{}"],"commit_sequence":"63"}}"#,
                hex(&operation_id),
                "a6".repeat(16),
                "b1".repeat(16),
                "b2".repeat(16)
            )
            .as_bytes(),
        );
    });

    let mut secret = [5; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    let created = client
        .execute_create_table_sql(
            "CREATE TABLE app.tree (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES app.tree (id));",
        )
        .unwrap();
    assert_eq!(created.table_id, [0xa6; 16]);
    assert_eq!(created.column_ids, [[0xb1; 16], [0xb2; 16]]);
    assert_eq!(created.commit_sequence, 63);
    server.join().unwrap();
}

pub(super) fn admin_client_adds_renames_and_drops_columns() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let cases = [
            (
                "tables.add_column",
                format!(
                    r#"{{"table_id":"{}","column":{{"name":"label","type":"text","nullable":false,"generated_uuid_identity":false,"default":{{"kind":"literal","value":"new"}}}}}}"#,
                    "a6".repeat(16)
                ),
                [0x91; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","table_id":"{}","column_id":"{}","schema_version":"2","commit_sequence":"71"}}"#,
                    hex(&[0x91; 16]),
                    "a6".repeat(16),
                    "b6".repeat(16)
                ),
            ),
            (
                "tables.rename_column",
                format!(
                    r#"{{"table_id":"{}","column_id":"{}","name":"display_label"}}"#,
                    "a6".repeat(16),
                    "b6".repeat(16)
                ),
                [0x92; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","table_id":"{}","column_id":"{}","name":"display_label","commit_sequence":"72"}}"#,
                    hex(&[0x92; 16]),
                    "a6".repeat(16),
                    "b6".repeat(16)
                ),
            ),
            (
                "tables.drop_column",
                format!(
                    r#"{{"table_id":"{}","column_id":"{}"}}"#,
                    "a6".repeat(16),
                    "b6".repeat(16)
                ),
                [0x93; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","table_id":"{}","column_id":"{}","commit_sequence":"73"}}"#,
                    hex(&[0x93; 16]),
                    "a6".repeat(16),
                    "b6".repeat(16)
                ),
            ),
        ];
        for (operation, arguments, operation_id, result) in cases {
            let prepare = read_frame(&mut stream, &mut decoder);
            let payload = String::from_utf8(prepare.payload.clone()).unwrap();
            assert!(payload.contains(&format!("\"operation\":\"{operation}\"")));
            assert!(payload.contains(&format!("\"arguments\":{arguments}")));
            respond(
                &mut stream,
                &prepare,
                format!(
                    r#"{{"operation_id":"{}","state":"prepared"}}"#,
                    hex(&operation_id)
                )
                .as_bytes(),
            );
            let start = read_frame(&mut stream, &mut decoder);
            respond(&mut stream, &start, result.as_bytes());
        }
    });

    let column = AdminColumnDefinition {
        name: "label".into(),
        scalar_type: ScalarType::Text,
        nullable: false,
        generated_uuid_identity: false,
        default: Some(AdminColumnDefault::Literal(ScalarValue::Text("new".into()))),
    };
    let mut secret = [5; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    let added = client.add_column([0xa6; 16], &column).unwrap();
    assert_eq!(added.column_id, [0xb6; 16]);
    client
        .rename_column(added.table_id, added.column_id, "display_label")
        .unwrap();
    client.drop_column(added.table_id, added.column_id).unwrap();
    server.join().unwrap();
}

pub(super) fn admin_client_creates_table_with_foreign_key_in_one_typed_operation() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        for (operation, body) in [
            (
                "metadata.schemas.list",
                format!(
                    r#"{{"schemas":[{{"schema_id":"{}","name":"app"}}],"next_cursor":null}}"#,
                    "a3".repeat(16)
                ),
            ),
            (
                "metadata.schemas.list",
                format!(
                    r#"{{"schemas":[{{"schema_id":"{}","name":"crm"}}],"next_cursor":null}}"#,
                    "a4".repeat(16)
                ),
            ),
            (
                "metadata.relations.list",
                format!(
                    r#"{{"relations":[{{"relation_id":"{}","name":"customers"}}],"next_cursor":null}}"#,
                    "b5".repeat(16)
                ),
            ),
            (
                "metadata.columns.list",
                format!(
                    r#"{{"columns":[{{"column_id":"{}","name":"id","ordinal":0,"scalar_type":"integer","nullable":false}}],"next_cursor":null}}"#,
                    "c6".repeat(16)
                ),
            ),
            (
                "metadata.constraints.list",
                format!(
                    r#"{{"constraints":[{{"constraint_id":"{}","name":"pk_orders","kind":"primary_key","column_ids":["{}"],"referenced_table_id":null,"referenced_column_ids":[]}}],"next_cursor":null}}"#,
                    "d7".repeat(16),
                    "c6".repeat(16)
                ),
            ),
        ] {
            let request = read_frame(&mut stream, &mut decoder);
            assert!(
                String::from_utf8_lossy(&request.payload)
                    .contains(&format!(r#""operation":"{operation}""#))
            );
            respond(&mut stream, &request, body.as_bytes());
        }
        let prepare = read_frame(&mut stream, &mut decoder);
        let payload = String::from_utf8(prepare.payload.clone()).unwrap();
        assert!(payload.contains(r#""operation":"tables.create""#));
        assert!(payload.contains(&format!(
            r#""foreign_key_source_columns":[["customer_id"]],"foreign_key_target_constraint_ids":["{}"]"#,
            "d7".repeat(16)
        )));
        assert!(matches!(
            decode_client_admin_command(&prepare.payload),
            Ok(ClientAdminCommand::PrepareCreateTable { foreign_keys, .. })
                if foreign_keys == [prdb_engine::storage_manager::CreateTableForeignKeyDefinition {
                    source_columns: vec!["customer_id".to_owned()],
                    target_constraint_id: [0xd7; 16],
                    target_columns: None,
                }]
        ));
        respond(
            &mut stream,
            &prepare,
            format!(
                r#"{{"operation_id":"{}","state":"prepared"}}"#,
                "e8".repeat(16)
            )
            .as_bytes(),
        );
        let start = read_frame(&mut stream, &mut decoder);
        respond(
            &mut stream,
            &start,
            format!(r#"{{"operation_id":"{}","outcome":"committed","table_id":"{}","column_ids":["{}"],"commit_sequence":"81"}}"#, "e8".repeat(16), "f9".repeat(16), "ca".repeat(16)).as_bytes(),
        );
    });

    let mut secret = [5; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    let created = client
        .execute_create_table_sql(
            "CREATE TABLE app.order_lines (customer_id INTEGER REFERENCES crm.customers (id))",
        )
        .unwrap();
    assert_eq!(created.table_id, [0xf9; 16]);
    assert_eq!(created.column_ids, [[0xca; 16]]);
    assert_eq!(created.commit_sequence, 81);
    server.join().unwrap();
}
