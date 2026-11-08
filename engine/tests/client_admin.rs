use prdb_engine::client_protocol::{ClientFrame, ClientFrameDecoder};
use prdb_engine::client_submit::{AdminClient, AdminSystemPrivilege, InvocationStatus};
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
                "schemas.drop",
                r#"{"schema_id":"a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3"}"#,
                [0x63; 16],
                format!(
                    "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"schema_id\":\"{}\",\"commit_sequence\":\"43\"}}",
                    hex(&[0x63; 16]),
                    "a3".repeat(16)
                ),
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
    client.drop_schema(schema.identity_id).unwrap();
    server.join().unwrap();
}

fn hex(id: &[u8; 16]) -> String {
    id.iter().map(|byte| format!("{byte:02x}")).collect()
}
