use prdb_engine::client_protocol::{ClientFrame, ClientFrameDecoder};
use prdb_engine::client_submit::{AdminClient, InvocationStatus};
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

fn hex(id: &[u8; 16]) -> String {
    id.iter().map(|byte| format!("{byte:02x}")).collect()
}
