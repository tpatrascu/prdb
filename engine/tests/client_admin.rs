use prdb_engine::client_protocol::{ClientFrame, ClientFrameDecoder};
use prdb_engine::client_submit::{
    AdminClient, AdminColumnDefault, AdminColumnDefinition, AdminSystemPrivilege,
    CancellationResult, InvocationStatus, SubmissionClient, TerminalOutcome,
};
use prdb_engine::tuple::{ScalarType, ScalarValue};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

fn read_frame(stream: &mut TcpStream, decoder: &mut ClientFrameDecoder) -> ClientFrame {
    let mut input = [0u8; 4096];
    loop {
        let count = stream.read(&mut input).unwrap();
        assert_ne!(count, 0);
        let mut frames = decoder.push(&input[..count]).unwrap();
        if !frames.is_empty() {
            assert_eq!(frames.len(), 1);
            return frames.remove(0);
        }
    }
}

fn respond(stream: &mut TcpStream, request: &ClientFrame, payload: &[u8]) {
    stream
        .write_all(
            &ClientFrame {
                kind: 100,
                request_id: request.request_id,
                response_sequence: 0,
                payload: payload.to_vec(),
            }
            .encode()
            .unwrap(),
        )
        .unwrap();
}

fn establish_control_session(stream: &mut TcpStream, decoder: &mut ClientFrameDecoder) {
    let hello = read_frame(stream, decoder);
    assert_eq!(hello.kind, 1);
    assert!(
        String::from_utf8(hello.payload.clone())
            .unwrap()
            .contains("\"mode\":\"control\"")
    );
    respond(
        stream,
        &hello,
        br#"{"transport":"plaintext_tcp","limits":{"chunk_bytes":131072,"control_bytes":32768,"payload_bytes":524288},"mode":"control","minor":0,"major":1}"#,
    );
    let auth = read_frame(stream, decoder);
    assert_eq!(auth.kind, 2);
    respond(
        stream,
        &auth,
        br#"{"session_id":"01010101010101010101010101010101","authenticated":true}"#,
    );
}

fn establish_query_session(stream: &mut TcpStream, decoder: &mut ClientFrameDecoder) {
    let hello = read_frame(stream, decoder);
    assert_eq!(hello.kind, 1);
    assert!(
        String::from_utf8(hello.payload.clone())
            .unwrap()
            .contains("\"mode\":\"query\"")
    );
    respond(
        stream,
        &hello,
        br#"{"transport":"plaintext_tcp","limits":{"chunk_bytes":131072,"control_bytes":32768,"payload_bytes":524288},"mode":"query","minor":0,"major":1}"#,
    );
    let auth = read_frame(stream, decoder);
    assert_eq!(auth.kind, 2);
    respond(
        stream,
        &auth,
        br#"{"session_id":"01010101010101010101010101010101","authenticated":true}"#,
    );
}

#[test]
fn submission_client_decodes_cancellation_acknowledgements() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        for state in [
            "accepted",
            "too_late",
            "committed",
            "aborted",
            "unknown",
            "pending",
        ] {
            let (mut stream, _) = listener.accept().unwrap();
            let mut decoder = ClientFrameDecoder::default();
            establish_query_session(&mut stream, &mut decoder);
            let request = read_frame(&mut stream, &mut decoder);
            assert_eq!(request.kind, 9);
            assert_eq!(
                request.payload,
                br#"{"invocation_id":"11111111111111111111111111111111"}"#
            );
            respond(
                &mut stream,
                &request,
                format!("{{\"state\":\"{state}\"}}").as_bytes(),
            );
        }
    });

    for expected in [
        CancellationResult::Accepted,
        CancellationResult::TooLate,
        CancellationResult::Terminal(TerminalOutcome::Committed),
        CancellationResult::Terminal(TerminalOutcome::Aborted),
        CancellationResult::Unknown,
    ] {
        let mut secret = [5; 32];
        let mut client = SubmissionClient::connect_authenticated_with_timeout(
            address,
            [6; 16],
            &mut secret,
            Duration::from_secs(3),
        )
        .unwrap();
        assert_eq!(secret, [0; 32]);
        assert!(matches!(
            client.cancel([0; 16]),
            Err(prdb_engine::client_submit::ClientSubmitError::InvalidInvocationId)
        ));
        assert_eq!(client.cancel([0x11; 16]).unwrap(), expected);
    }
    let mut secret = [5; 32];
    let mut client = SubmissionClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    assert!(matches!(
        client.cancel([0x11; 16]),
        Err(prdb_engine::client_submit::ClientSubmitError::InvalidResponse)
    ));
    server.join().unwrap();
}

#[test]
fn admin_client_sends_typed_index_prepare_start_and_status_requests() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);

        let operations = [
            ("indexes.create", [0x11; 16], 11),
            ("indexes.rebuild", [0x12; 16], 12),
            ("indexes.drop", [0x13; 16], 13),
        ];
        for (operation, operation_id, sequence) in &operations {
            let prepare = read_frame(&mut stream, &mut decoder);
            assert_eq!(prepare.kind, 10);
            let prepare_payload = String::from_utf8(prepare.payload.clone()).unwrap();
            assert!(prepare_payload.contains(operation));
            match *operation {
                "indexes.create" => {
                    assert!(prepare_payload.contains("\"name\":\"users_by_name\""));
                    assert!(prepare_payload.contains("\"unique\":true"));
                }
                "indexes.rebuild" => {
                    assert!(
                        prepare_payload.contains(&format!("\"index_id\":\"{}\"", "a1".repeat(16)))
                    );
                    assert!(prepare_payload.contains("\"unique\":false"));
                }
                "indexes.drop" => assert!(prepare_payload.contains("\"index_id\":")),
                _ => unreachable!(),
            }
            respond(
                &mut stream,
                &prepare,
                format!(
                    "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                    hex(operation_id)
                )
                .as_bytes(),
            );

            let start = read_frame(&mut stream, &mut decoder);
            assert_eq!(start.kind, 10);
            assert!(
                String::from_utf8(start.payload.clone())
                    .unwrap()
                    .contains(&hex(operation_id))
            );
            respond(
                &mut stream,
                &start,
                format!(
                    "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"index_id\":\"{}\",\"commit_sequence\":\"{sequence}\"}}",
                    hex(operation_id),
                    "a1".repeat(16),
                )
                .as_bytes(),
            );
        }
        let status = read_frame(&mut stream, &mut decoder);
        assert_eq!(status.kind, 8);
        assert!(
            String::from_utf8(status.payload.clone())
                .unwrap()
                .contains(&hex(&[0x13; 16]))
        );
        respond(&mut stream, &status, br#"{"state":"committed"}"#);
    });

    let mut secret = [9; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [7; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    assert_eq!(secret, [0; 32]);
    let columns = [[0xb2; 16], [0xb3; 16]];
    let created = client
        .create_index([0xb1; 16], "users_by_name", &columns, true)
        .unwrap();
    assert_eq!(created.operation_id, [0x11; 16]);
    assert_eq!(created.index_id, [0xa1; 16]);
    assert_eq!(created.commit_sequence, 11);
    let rebuilt = client
        .rebuild_index(created.index_id, &columns, false)
        .unwrap();
    assert_eq!(rebuilt.operation_id, [0x12; 16]);
    assert_eq!(rebuilt.index_id, created.index_id);
    let dropped = client.drop_index(created.index_id).unwrap();
    assert_eq!(dropped.operation_id, [0x13; 16]);
    assert_eq!(dropped.commit_sequence, 13);
    assert_eq!(
        client.status(dropped.operation_id).unwrap(),
        InvocationStatus::Committed
    );
    server.join().unwrap();
}

#[test]
fn admin_client_returns_operation_id_when_start_reply_is_lost() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let prepare = read_frame(&mut stream, &mut decoder);
        respond(
            &mut stream,
            &prepare,
            format!(
                "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                hex(&[0x55; 16])
            )
            .as_bytes(),
        );
        let start = read_frame(&mut stream, &mut decoder);
        assert_eq!(start.kind, 10);
        assert!(
            String::from_utf8(start.payload)
                .unwrap()
                .contains(&hex(&[0x55; 16]))
        );
        drop(stream);
    });

    let mut secret = [8; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    let error = client
        .create_index([0xb1; 16], "users_by_name", &[[0xb2; 16]], false)
        .unwrap_err();
    assert!(matches!(
        error,
        prdb_engine::client_submit::ClientSubmitError::AdministrativeOutcomeUncertain(id)
            if id == [0x55; 16]
    ));
    server.join().unwrap();
}

#[test]
fn admin_client_sends_typed_user_and_role_lifecycle_requests() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let cases = [
            ("users.create", r#"{"name":"alice"}"#, Some("user_id")),
            ("roles.create", r#"{"name":"analyst"}"#, Some("role_id")),
            (
                "roles.grant",
                r#"{"member_id":"01010101010101010101010101010101","role_id":"02020202020202020202020202020202"}"#,
                None,
            ),
            (
                "users.rename",
                r#"{"user_id":"01010101010101010101010101010101","name":"alice_2"}"#,
                None,
            ),
            (
                "users.login",
                r#"{"user_id":"01010101010101010101010101010101","enabled":"false"}"#,
                None,
            ),
            (
                "roles.revoke",
                r#"{"member_id":"01010101010101010101010101010101","role_id":"02020202020202020202020202020202"}"#,
                None,
            ),
            (
                "users.drop",
                r#"{"user_id":"01010101010101010101010101010101"}"#,
                None,
            ),
            (
                "roles.drop",
                r#"{"role_id":"02020202020202020202020202020202"}"#,
                None,
            ),
        ];
        for (index, (operation, arguments, created_key)) in cases.into_iter().enumerate() {
            let prepare = read_frame(&mut stream, &mut decoder);
            assert_eq!(prepare.kind, 10);
            let payload = String::from_utf8(prepare.payload.clone()).unwrap();
            assert!(payload.contains(&format!("\"operation\":\"{operation}\"")));
            assert!(payload.contains(&format!("\"arguments\":{arguments}")));
            let operation_id = [0x30 + index as u8; 16];
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
            assert_eq!(start.kind, 10);
            assert!(
                String::from_utf8(start.payload.clone())
                    .unwrap()
                    .contains(&hex(&operation_id))
            );
            let identity = created_key
                .map(|key| format!(",\"{key}\":\"{}\"", "a1".repeat(16)))
                .unwrap_or_default();
            respond(
                &mut stream,
                &start,
                format!(
                    "{{\"operation_id\":\"{}\",\"outcome\":\"committed\"{identity},\"commit_sequence\":\"{}\"}}",
                    hex(&operation_id),
                    index + 1
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
    assert_eq!(secret, [0; 32]);
    assert_eq!(client.create_user("alice").unwrap().identity_id, [0xa1; 16]);
    assert_eq!(
        client.create_role("analyst").unwrap().identity_id,
        [0xa1; 16]
    );
    client.grant_role([1; 16], [2; 16]).unwrap();
    client.rename_user([1; 16], "alice_2").unwrap();
    client.set_user_login([1; 16], false).unwrap();
    client.revoke_role([1; 16], [2; 16]).unwrap();
    client.drop_user([1; 16]).unwrap();
    client.drop_role([2; 16]).unwrap();
    server.join().unwrap();
}

#[test]
fn admin_client_issues_one_time_token_and_revokes_tokens() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        for (operation, arguments, operation_id) in [
            (
                "tokens.issue",
                r#"{"user_id":"01010101010101010101010101010101","label":"daily \"alpha\"","expires_at_us":"123456"}"#,
                [0x41; 16],
            ),
            (
                "tokens.revoke",
                r#"{"token_id":"a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2"}"#,
                [0x42; 16],
            ),
        ] {
            let prepare = read_frame(&mut stream, &mut decoder);
            assert_eq!(prepare.kind, 10);
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
            assert_eq!(start.kind, 10);
            let result = if operation == "tokens.issue" {
                format!(
                    "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"token_id\":\"{}\",\"token_secret\":\"paWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaU=\",\"commit_sequence\":\"21\"}}",
                    hex(&operation_id),
                    "a2".repeat(16)
                )
            } else {
                format!(
                    "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"commit_sequence\":\"22\"}}",
                    hex(&operation_id)
                )
            };
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
    let issued = client
        .issue_token([1; 16], "daily \"alpha\"", 123456)
        .unwrap();
    assert_eq!(issued.token_id, [0xa2; 16]);
    assert_eq!(issued.token_secret(), &[0xa5; 32]);
    assert_eq!(issued.commit_sequence, 21);
    client.revoke_token(issued.token_id).unwrap();
    server.join().unwrap();
}

#[test]
fn admin_client_grants_and_revokes_database_system_privileges() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        for (operation, arguments, operation_id) in [
            (
                "privileges.grant",
                r#"{"grantee_id":"01010101010101010101010101010101","privilege":"manage_users"}"#,
                [0x51; 16],
            ),
            (
                "privileges.revoke",
                r#"{"grantee_id":"01010101010101010101010101010101","privilege":"create_schema"}"#,
                [0x52; 16],
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
                    "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"commit_sequence\":\"{}\"}}",
                    hex(&operation_id),
                    if operation == "privileges.grant" { 31 } else { 32 }
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
    client
        .grant_system_privilege([1; 16], AdminSystemPrivilege::ManageUsers)
        .unwrap();
    client
        .revoke_system_privilege([1; 16], AdminSystemPrivilege::CreateSchema)
        .unwrap();
    server.join().unwrap();
}

#[test]
fn admin_client_creates_and_drops_schemas() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
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
    });

    let mut secret = [5; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    let schema = client.create_schema("analytics", [1; 16]).unwrap();
    assert_eq!(schema.identity_id, [0xa3; 16]);
    client
        .rename_schema(schema.identity_id, "insights")
        .unwrap();
    client
        .set_schema_owner(schema.identity_id, [1; 16])
        .unwrap();
    client.drop_schema(schema.identity_id).unwrap();
    server.join().unwrap();
}

#[test]
fn admin_client_renames_and_drops_tables() {
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

#[test]
fn admin_client_creates_table_with_typed_columns_and_defaults() {
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

#[test]
fn admin_client_adds_renames_and_drops_columns() {
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

#[test]
fn admin_client_changes_table_owner_nullability_and_defaults() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let table_id = "a7".repeat(16);
        let column_id = "b7".repeat(16);
        let cases = [
            (
                "tables.set_owner",
                format!(
                    r#"{{"table_id":"{table_id}","owner_id":"{}"}}"#,
                    "c7".repeat(16)
                ),
                [0xa1; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","table_id":"{table_id}","owner_id":"{}","commit_sequence":"81"}}"#,
                    hex(&[0xa1; 16]),
                    "c7".repeat(16)
                ),
            ),
            (
                "tables.alter_column_nullability",
                format!(
                    r#"{{"table_id":"{table_id}","column_id":"{column_id}","nullable":"true"}}"#
                ),
                [0xa2; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","table_id":"{table_id}","column_id":"{column_id}","nullable":true,"commit_sequence":"82"}}"#,
                    hex(&[0xa2; 16])
                ),
            ),
            (
                "tables.set_column_default",
                format!(
                    r#"{{"table_id":"{table_id}","column_id":"{column_id}","type":"bigint","default":{{"kind":"literal","value":"5"}} }}"#
                ),
                [0xa3; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","table_id":"{table_id}","column_id":"{column_id}","commit_sequence":"83"}}"#,
                    hex(&[0xa3; 16])
                ),
            ),
            (
                "tables.drop_column_default",
                format!(r#"{{"table_id":"{table_id}","column_id":"{column_id}"}}"#),
                [0xa4; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","table_id":"{table_id}","column_id":"{column_id}","commit_sequence":"84"}}"#,
                    hex(&[0xa4; 16])
                ),
            ),
        ];
        for (operation, arguments, operation_id, result) in cases {
            let prepare = read_frame(&mut stream, &mut decoder);
            let payload = String::from_utf8(prepare.payload.clone()).unwrap();
            assert!(payload.contains(&format!("\"operation\":\"{operation}\"")));
            assert!(
                payload.contains(&format!("\"arguments\":{arguments}")),
                "{payload}"
            );
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
    client.set_table_owner([0xa7; 16], [0xc7; 16]).unwrap();
    client
        .alter_column_nullability([0xa7; 16], [0xb7; 16], true)
        .unwrap();
    client
        .set_column_default(
            [0xa7; 16],
            [0xb7; 16],
            ScalarType::BigInt,
            AdminColumnDefault::Literal(ScalarValue::BigInt(5)),
        )
        .unwrap();
    client.drop_column_default([0xa7; 16], [0xb7; 16]).unwrap();
    server.join().unwrap();
}

#[test]
fn admin_client_sends_typed_constraint_and_foreign_key_operations() {
    use prdb_engine::authorization::Expr;

    let expression = Expr::Equal(
        Box::new(Expr::Field(0)),
        Box::new(Expr::Literal(Some(ScalarValue::BigInt(0)))),
    );
    let expression_hex = prdb_engine::expression::encode_expression(&expression)
        .unwrap()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let table_id = [0x11; 16];
    let column_ids = [[0x21; 16], [0x22; 16]];
    let constraint_id = [0x31; 16];
    let index_id = [0x32; 16];
    let expression_id = [0x33; 16];
    let foreign_key_id = [0x41; 16];
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);

        let cases = [
            (
                "constraints.create_unique",
                format!(
                    r#"{{"table_id":"{}","column_ids":["{}","{}"]}}"#,
                    hex(&table_id),
                    hex(&column_ids[0]),
                    hex(&column_ids[1])
                ),
                [0x51; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","constraint_id":"{}","index_id":"{}","commit_sequence":"91"}}"#,
                    hex(&[0x51; 16]),
                    hex(&constraint_id),
                    hex(&index_id)
                ),
            ),
            (
                "constraints.create_primary_key",
                format!(
                    r#"{{"table_id":"{}","column_ids":["{}"]}}"#,
                    hex(&table_id),
                    hex(&column_ids[0])
                ),
                [0x52; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","constraint_id":"{}","index_id":"{}","commit_sequence":"92"}}"#,
                    hex(&[0x52; 16]),
                    hex(&[0x34; 16]),
                    hex(&[0x35; 16])
                ),
            ),
            (
                "constraints.create_check",
                format!(
                    r#"{{"table_id":"{}","expression_eir1":"{}","dependency_column_ids":[]}}"#,
                    hex(&table_id),
                    expression_hex
                ),
                [0x53; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","constraint_id":"{}","expression_id":"{}","commit_sequence":"93"}}"#,
                    hex(&[0x53; 16]),
                    hex(&[0x36; 16]),
                    hex(&expression_id)
                ),
            ),
            (
                "constraints.drop_primary_key",
                format!(r#"{{"constraint_id":"{}"}}"#, hex(&constraint_id)),
                [0x54; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","constraint_id":"{}","commit_sequence":"94"}}"#,
                    hex(&[0x54; 16]),
                    hex(&constraint_id)
                ),
            ),
            (
                "constraints.drop_unique",
                format!(r#"{{"constraint_id":"{}"}}"#, hex(&[0x34; 16])),
                [0x55; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","constraint_id":"{}","commit_sequence":"95"}}"#,
                    hex(&[0x55; 16]),
                    hex(&[0x34; 16])
                ),
            ),
            (
                "constraints.drop_check",
                format!(r#"{{"constraint_id":"{}"}}"#, hex(&[0x36; 16])),
                [0x56; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","constraint_id":"{}","commit_sequence":"96"}}"#,
                    hex(&[0x56; 16]),
                    hex(&[0x36; 16])
                ),
            ),
            (
                "foreign_keys.create",
                format!(
                    r#"{{"source_table_id":"{}","source_column_ids":["{}"],"target_constraint_id":"{}"}}"#,
                    hex(&table_id),
                    hex(&column_ids[0]),
                    hex(&constraint_id)
                ),
                [0x57; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","foreign_key_id":"{}","commit_sequence":"97"}}"#,
                    hex(&[0x57; 16]),
                    hex(&foreign_key_id)
                ),
            ),
            (
                "foreign_keys.drop",
                format!(r#"{{"foreign_key_id":"{}"}}"#, hex(&foreign_key_id)),
                [0x58; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","foreign_key_id":"{}","commit_sequence":"98"}}"#,
                    hex(&[0x58; 16]),
                    hex(&foreign_key_id)
                ),
            ),
        ];
        for (operation, arguments, operation_id, result) in cases {
            let prepare = read_frame(&mut stream, &mut decoder);
            let payload = String::from_utf8(prepare.payload.clone()).unwrap();
            assert!(
                payload.contains(&format!(r#""operation":"{operation}""#)),
                "{payload}"
            );
            assert!(
                payload.contains(&format!(r#""arguments":{arguments}"#)),
                "{payload}"
            );
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
    let mut secret = [7; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [8; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    let unique = client
        .create_unique_constraint(table_id, &column_ids)
        .unwrap();
    assert_eq!(unique.constraint_id, constraint_id);
    assert_eq!(unique.index_id, index_id);
    let primary_key = client
        .create_primary_key(table_id, &column_ids[..1])
        .unwrap();
    assert_eq!(primary_key.constraint_id, [0x34; 16]);
    assert_eq!(primary_key.index_id, [0x35; 16]);
    let check = client
        .create_check_constraint(table_id, &expression, &[])
        .unwrap();
    assert_eq!(check.constraint_id, [0x36; 16]);
    assert_eq!(check.expression_id, expression_id);
    client.drop_primary_key(constraint_id).unwrap();
    client.drop_unique_constraint([0x34; 16]).unwrap();
    client.drop_check_constraint([0x36; 16]).unwrap();
    let foreign_key = client
        .create_foreign_key(table_id, &column_ids[..1], constraint_id)
        .unwrap();
    assert_eq!(foreign_key.foreign_key_id, foreign_key_id);
    client.drop_foreign_key(foreign_key_id).unwrap();
    server.join().unwrap();
}

fn hex(id: &[u8; 16]) -> String {
    id.iter().map(|byte| format!("{byte:02x}")).collect()
}
