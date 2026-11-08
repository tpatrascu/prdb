use prdb_engine::client_protocol::{ClientFrame, ClientFrameDecoder};
use prdb_engine::client_submit::MetadataClient;
use prdb_engine::submission_scope::{RequestedScopeBuilder, decode_requested_scope_v1};
use prdb_engine::{authorization::AccessTarget, authorization::Operation};
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

#[test]
fn metadata_client_decodes_visible_records_and_builds_requested_scope() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        let hello = read_frame(&mut stream, &mut decoder);
        assert_eq!(hello.kind, 1);
        assert!(
            String::from_utf8(hello.payload.clone())
                .unwrap()
                .contains("\"mode\":\"control\"")
        );
        respond(
            &mut stream,
            &hello,
            br#"{"transport":"plaintext_tcp","limits":{"chunk_bytes":131072,"control_bytes":32768,"payload_bytes":524288},"mode":"control","minor":0,"major":1}"#,
        );

        let auth = read_frame(&mut stream, &mut decoder);
        assert_eq!(auth.kind, 2);
        respond(
            &mut stream,
            &auth,
            br#"{"session_id":"01010101010101010101010101010101","authenticated":true}"#,
        );

        for (operation, response) in [
            (
                "metadata.schemas.list",
                &br#"{"schemas":[{"schema_id":"01010101010101010101010101010101","name":"app"}]}"#[..],
            ),
            (
                "metadata.relations.list",
                &br#"{"relations":[{"relation_id":"02020202020202020202020202020202","name":"users"}]}"#[..],
            ),
            (
                "metadata.columns.list",
                &br#"{"columns":[{"column_id":"03030303030303030303030303030303","name":"user_id","ordinal":2,"scalar_type":"integer","nullable":false}]}"#[..],
            ),
        ] {
            let request = read_frame(&mut stream, &mut decoder);
            assert_eq!(request.kind, 10);
            assert!(String::from_utf8(request.payload.clone())
                .unwrap()
                .contains(operation));
            respond(&mut stream, &request, response);
        }
    });

    let mut secret = [9; 32];
    let mut client = MetadataClient::connect_authenticated_with_timeout(
        address,
        [7; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    assert_eq!(secret, [0; 32]);
    let schemas = client.list_schemas().unwrap();
    let relations = client.list_relations(schemas[0].schema_id).unwrap();
    let columns = client
        .list_columns(schemas[0].schema_id, relations[0].relation_id)
        .unwrap();
    assert_eq!(schemas[0].name, "app");
    assert_eq!(relations[0].name, "users");
    assert_eq!(columns[0].name, "user_id");
    assert_eq!(columns[0].ordinal, 2);
    let mut builder = RequestedScopeBuilder::new();
    builder
        .include_column(
            &schemas[0],
            &relations[0],
            &columns[0],
            &[Operation::Select],
        )
        .unwrap();
    let scope = decode_requested_scope_v1(&builder.encode().unwrap()).unwrap();
    assert!(scope.contains(&(
        Operation::Select,
        AccessTarget::Column {
            table_id: [2; 16],
            column_id: [3; 16],
        }
    )));
    server.join().unwrap();
}
