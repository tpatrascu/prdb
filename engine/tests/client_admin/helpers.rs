#![allow(dead_code)]
use prdb_engine::client_protocol::{ClientFrame, ClientFrameDecoder};
use std::io::{Read, Write};
use std::net::TcpStream;

pub fn read_frame(stream: &mut TcpStream, decoder: &mut ClientFrameDecoder) -> ClientFrame {
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

pub fn respond(stream: &mut TcpStream, request: &ClientFrame, payload: &[u8]) {
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

pub fn operation_id_from_payload(payload: &[u8]) -> [u8; 16] {
    let text = std::str::from_utf8(payload).unwrap();
    let hex = text
        .split_once("\"operation_id\":\"")
        .unwrap()
        .1
        .split_once('"')
        .unwrap()
        .0;
    assert_eq!(hex.len(), 32);
    let mut operation_id = [0; 16];
    for (index, byte) in operation_id.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16).unwrap();
    }
    operation_id
}

pub fn establish_control_session(stream: &mut TcpStream, decoder: &mut ClientFrameDecoder) {
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

pub fn establish_query_session(stream: &mut TcpStream, decoder: &mut ClientFrameDecoder) {
    let hello = read_frame(stream, decoder);
    assert_eq!(hello.kind, 1);
    assert!(
        String::from_utf8(hello.payload.clone())
            .unwrap()
            .contains("\"mode\":\"query\"")
    );
    assert!(
        String::from_utf8(hello.payload.clone())
            .unwrap()
            .contains("\"sdk_library_mode\":\"repository\"")
    );
    respond(
        stream,
        &hello,
        br#"{"transport":"plaintext_tcp","limits":{"chunk_bytes":131072,"control_bytes":32768,"payload_bytes":524288},"mode":"query","sdk_library_mode":"repository","minor":0,"major":1}"#,
    );
    let auth = read_frame(stream, decoder);
    assert_eq!(auth.kind, 2);
    respond(
        stream,
        &auth,
        br#"{"session_id":"01010101010101010101010101010101","authenticated":true}"#,
    );
}

pub fn hex(id: &[u8; 16]) -> String {
    id.iter().map(|byte| format!("{byte:02x}")).collect()
}
