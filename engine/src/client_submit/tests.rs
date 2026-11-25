use super::codec::*;
use super::response::*;
use super::*;
use crate::client_protocol::{
    CLIENT_MAX_CHUNK_BYTES, CLIENT_MAX_CONTROL_BYTES, CLIENT_MAX_PAYLOAD_BYTES, ClientFrame,
    ClientFrameDecoder,
};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::net::TcpStream;
use std::thread;

fn read_v2_frame(stream: &mut TcpStream, decoder: &mut ClientFrameDecoder) -> ClientFrame {
    let mut input = [0; 8192];
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

fn write_v2_response(stream: &mut TcpStream, request: &ClientFrame, payload: &str) {
    let response = ClientFrame {
        kind: 100,
        request_id: request.request_id,
        response_sequence: 0,
        payload: payload.as_bytes().to_vec(),
    }
    .encode_v2()
    .unwrap();
    stream.write_all(&response).unwrap();
}

fn serve_v2_auth(stream: &mut TcpStream, decoder: &mut ClientFrameDecoder) {
    let hello = read_v2_frame(stream, decoder);
    assert_eq!(hello.kind, 1);
    let hello_response = format!(
        "{{\"major\":2,\"minor\":0,\"mode\":\"query\",\"sdk_library_mode\":\"repository\",\"transport\":\"plaintext_tcp\",\"limits\":{{\"payload_bytes\":{CLIENT_MAX_PAYLOAD_BYTES},\"control_bytes\":{CLIENT_MAX_CONTROL_BYTES},\"chunk_bytes\":{CLIENT_MAX_CHUNK_BYTES}}}}}"
    );
    write_v2_response(stream, &hello, &hello_response);
    let auth = read_v2_frame(stream, decoder);
    assert_eq!(auth.kind, 2);
    let auth_json = std::str::from_utf8(&auth.payload).unwrap();
    assert!(auth_json.contains("\"token_id\":\"01010101010101010101010101010101\""));
    write_v2_response(
        stream,
        &auth,
        "{\"session_id\":\"11111111111111111111111111111111\",\"authenticated\":true}",
    );
}

#[test]
fn interactive_client_retries_begin_with_same_key_and_reattaches_fresh() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let transaction_id = "abababababababababababababababab";
        {
            let (mut stream, _) = listener.accept().unwrap();
            let mut decoder = ClientFrameDecoder::negotiating();
            serve_v2_auth(&mut stream, &mut decoder);
            let begin = read_v2_frame(&mut stream, &mut decoder);
            assert_eq!(begin.kind, 13);
            assert_eq!(
                begin.payload,
                b"{\"begin_request_id\":\"09090909090909090909090909090909\"}"
            );
            // Simulate a committed server-side begin whose response is lost.
        }
        {
            let (mut stream, _) = listener.accept().unwrap();
            let mut decoder = ClientFrameDecoder::negotiating();
            serve_v2_auth(&mut stream, &mut decoder);
            let begin = read_v2_frame(&mut stream, &mut decoder);
            assert_eq!(begin.kind, 13);
            assert_eq!(
                begin.payload,
                b"{\"begin_request_id\":\"09090909090909090909090909090909\"}"
            );
            write_v2_response(
                &mut stream,
                &begin,
                &format!(
                    "{{\"transaction_id\":\"{transaction_id}\",\"state\":\"active\",\"idle_timeout_ms\":300000,\"lifetime_ms\":900000,\"max_package_bytes\":67108864,\"max_packages\":128}}"
                ),
            );
        }
        {
            let (mut stream, _) = listener.accept().unwrap();
            let mut decoder = ClientFrameDecoder::negotiating();
            serve_v2_auth(&mut stream, &mut decoder);
            let attach = read_v2_frame(&mut stream, &mut decoder);
            assert_eq!(attach.kind, 14);
            assert_eq!(
                std::str::from_utf8(&attach.payload).unwrap(),
                format!("{{\"transaction_id\":\"{transaction_id}\"}}")
            );
            write_v2_response(
                &mut stream,
                &attach,
                &format!("{{\"transaction_id\":\"{transaction_id}\",\"state\":\"active\"}}"),
            );
        }
    });

    let mut first_secret = [7; 32];
    let mut first =
        SubmissionClient::connect_interactive_authenticated(address, [1; 16], &mut first_secret)
            .unwrap();
    assert_eq!(first_secret, [0; 32]);
    assert!(matches!(
        first.interactive_transaction_begin([9; 16]),
        Err(ClientSubmitError::InteractiveTransactionBeginOutcomeUncertain)
    ));
    drop(first);

    let mut retry_secret = [8; 32];
    let mut retry =
        SubmissionClient::connect_interactive_authenticated(address, [1; 16], &mut retry_secret)
            .unwrap();
    let begin = retry.interactive_transaction_begin([9; 16]).unwrap();
    let transaction_id = match begin {
        crate::client_protocol::ClientV2Response::TransactionBegun { transaction_id, .. } => {
            transaction_id
        }
        _ => panic!("expected transaction begin response"),
    };
    drop(retry);

    let mut next_secret = [7; 32];
    let mut second =
        SubmissionClient::connect_interactive_authenticated(address, [1; 16], &mut next_secret)
            .unwrap();
    let attached = second
        .interactive_transaction_attach(transaction_id)
        .unwrap();
    assert!(matches!(
        attached,
        crate::client_protocol::ClientV2Response::Transaction {
            transaction_id: attached_id,
            state: crate::client_protocol::ClientV2TransactionState::Active,
        } if attached_id == transaction_id
    ));
    server.join().unwrap();
}

#[test]
fn administrative_index_request_validation_rejects_invalid_ids_names_and_columns() {
    assert!(matches!(
        validate_index_request([0; 16], &[[1; 16]]),
        Err(ClientSubmitError::InvalidAdministrativeRequest)
    ));
    assert!(matches!(
        validate_index_request([1; 16], &[]),
        Err(ClientSubmitError::InvalidAdministrativeRequest)
    ));
    assert!(matches!(
        validate_index_request([1; 16], &[[2; 16], [2; 16]]),
        Err(ClientSubmitError::InvalidAdministrativeRequest)
    ));
    assert!(matches!(
        validate_index_request([1; 16], &[[0; 16]]),
        Err(ClientSubmitError::InvalidAdministrativeRequest)
    ));
    assert!(is_normalized_index_name("_user_idx9"));
    assert!(!is_normalized_index_name("UserIdx"));
    assert!(!is_normalized_index_name("user-index"));
    assert_eq!(parse_canonical_positive_u64("42".to_owned()), Some(42));
    assert_eq!(parse_canonical_positive_u64("0".to_owned()), None);
    assert_eq!(parse_canonical_positive_u64("042".to_owned()), None);
}

#[test]
fn administrative_identity_validation_matches_canonical_names_and_nonzero_ids() {
    assert!(validate_admin_name("alice_9").is_ok());
    assert!(matches!(
        validate_admin_name("Alice"),
        Err(ClientSubmitError::InvalidAdministrativeRequest)
    ));
    assert!(matches!(
        validate_admin_name("has-dash"),
        Err(ClientSubmitError::InvalidAdministrativeRequest)
    ));
    assert!(validate_admin_id([1; 16]).is_ok());
    assert!(matches!(
        validate_admin_id([0; 16]),
        Err(ClientSubmitError::InvalidAdministrativeRequest)
    ));
}

#[test]
fn one_time_token_secret_decoder_requires_canonical_fixed_width_base64() {
    assert_eq!(
        decode_token_secret("paWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaU=").as_deref(),
        Some(&[0xa5; 32])
    );
    assert!(decode_token_secret("paWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaU").is_none());
    assert!(decode_token_secret("p=WlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaU=").is_none());
    assert!(decode_token_secret("paWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaV=").is_none());
}

#[test]
fn admin_table_column_encoder_rejects_invalid_shapes_and_defaults() {
    use crate::tuple::{ScalarType, ScalarValue};

    assert!(encode_admin_table_columns(&[]).is_err());
    let duplicate_names = [
        AdminColumnDefinition {
            name: "value".into(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        },
        AdminColumnDefinition {
            name: "value".into(),
            scalar_type: ScalarType::Integer,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        },
    ];
    assert!(encode_admin_table_columns(&duplicate_names).is_err());
    let generated_non_uuid = [AdminColumnDefinition {
        name: "id".into(),
        scalar_type: ScalarType::Text,
        nullable: false,
        generated_uuid_identity: true,
        default: None,
    }];
    assert!(encode_admin_table_columns(&generated_non_uuid).is_err());
    let null_not_null = [AdminColumnDefinition {
        name: "required".into(),
        scalar_type: ScalarType::Integer,
        nullable: false,
        generated_uuid_identity: false,
        default: Some(AdminColumnDefault::Null),
    }];
    assert!(encode_admin_table_columns(&null_not_null).is_err());
    let mismatched_literal = [AdminColumnDefinition {
        name: "count".into(),
        scalar_type: ScalarType::Integer,
        nullable: false,
        generated_uuid_identity: false,
        default: Some(AdminColumnDefault::Literal(ScalarValue::Text("1".into()))),
    }];
    assert!(encode_admin_table_columns(&mismatched_literal).is_err());
}

#[test]
fn response_decoder_rejects_duplicate_unknown_json_shapes_and_noncanonical_numbers() {
    assert!(parse_flat_response(br#"{"state":"prepared"}"#).is_ok());
    assert!(parse_flat_response(br#"{"state":"prepared","state":"prepared"}"#).is_err());
    assert_eq!(
        parse_flat_response(br#"{"nested":[]}"#).unwrap()["nested"],
        ResponseValue::Array(vec![])
    );
    assert!(parse_flat_response(br#"{"nested":{"a":1,"a":2}}"#).is_err());
    assert!(
        parse_bounded_json_value(
            br#"{"outer":["parameter-secret",{"inner":"partial"},"unterminated"#,
            CLIENT_MAX_CONTROL_BYTES,
        )
        .is_err()
    );
    assert!(parse_flat_response(br#"{"value":01}"#).is_err());
    assert!(parse_flat_response(br#"{"value":"partial\uD800"}"#).is_err());
    assert_eq!(
        parse_flat_response(br#"{"value":"\u0061"}"#).unwrap()["value"],
        ResponseValue::String("a".to_owned())
    );
    assert!(parse_flat_response(br#"{"a":1,"\u0061":2}"#).is_err());
}

#[test]
fn response_decoder_accepts_canonical_invocation_ids_and_typed_fields() {
    let fields = parse_flat_response(
        br#"{"invocation_id":"01010101010101010101010101010101","chunk":262144,"ok":true}"#,
    )
    .unwrap();
    assert_eq!(fields.get("ok"), Some(&ResponseValue::Boolean(true)));
    assert_eq!(fields.get("chunk"), Some(&ResponseValue::Number(262144)));
    assert_eq!(
        decode_hex_array::<16>(fields["invocation_id"].as_string().unwrap()),
        Some([1; 16])
    );
}
