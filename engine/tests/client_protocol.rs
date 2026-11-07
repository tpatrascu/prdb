use prdb_engine::client_protocol::{
    CLIENT_MAX_CHUNK_BYTES, ClientControlError, ClientMode, decode_client_chunk,
    decode_client_credentials, decode_client_hello,
};

#[test]
fn hello_decoder_accepts_the_version_one_shape_and_json_escapes() {
    let hello = decode_client_hello(br#"{"mode":"query","client_name":"tool\u0041"}"#).unwrap();
    assert_eq!(hello.mode, ClientMode::Query);
    assert_eq!(hello.client_name, "toolA");
}

#[test]
fn hello_decoder_rejects_duplicate_unknown_and_non_string_fields() {
    assert_eq!(
        decode_client_hello(br#"{"mode":"query","mode":"control","client_name":"tool"}"#),
        Err(ClientControlError::DuplicateKey)
    );
    assert_eq!(
        decode_client_hello(br#"{"mode":"query","client_name":"tool","principal":"admin"}"#),
        Err(ClientControlError::UnknownKey)
    );
    assert_eq!(
        decode_client_hello(br#"{"mode":"query","client_name":42}"#),
        Err(ClientControlError::InvalidJson)
    );
}

#[test]
fn auth_decoder_requires_canonical_id_and_fixed_size_padded_base64() {
    let token_secret = "A".repeat(43) + "=";
    let payload = format!(
        "{{\"token_id\":\"000102030405060708090a0b0c0d0e0f\",\"token_secret\":\"{token_secret}\"}}"
    );
    let credentials = decode_client_credentials(payload.as_bytes()).unwrap();
    assert_eq!(
        credentials.token_id,
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
    );
    assert_eq!(credentials.token_secret, [0; 32]);

    let noncanonical_secret = "A".repeat(40) + "AAB=";
    let payload = format!(
        "{{\"token_id\":\"000102030405060708090a0b0c0d0e0f\",\"token_secret\":\"{noncanonical_secret}\"}}"
    );
    assert!(matches!(
        decode_client_credentials(payload.as_bytes()),
        Err(ClientControlError::InvalidTokenSecret)
    ));

    let uppercase_id = format!(
        "{{\"token_id\":\"000102030405060708090A0b0c0d0e0f\",\"token_secret\":\"{token_secret}\"}}"
    );
    assert!(matches!(
        decode_client_credentials(uppercase_id.as_bytes()),
        Err(ClientControlError::InvalidTokenId)
    ));
}

#[test]
fn control_json_rejects_trailing_bytes_and_unpaired_surrogates() {
    assert_eq!(
        decode_client_hello(br#"{"mode":"query","client_name":"tool"} false"#),
        Err(ClientControlError::InvalidJson)
    );
    assert_eq!(
        decode_client_hello(br#"{"mode":"query","client_name":"\uD800"}"#),
        Err(ClientControlError::InvalidJson)
    );
}

#[test]
fn binary_chunks_validate_id_offset_and_data_bounds() {
    let mut payload = vec![0x11; 16];
    payload.extend_from_slice(&0x0807_0605_0403_0201u64.to_le_bytes());
    payload.extend_from_slice(b"chunk");
    let chunk = decode_client_chunk(&payload).unwrap();
    assert_eq!(chunk.invocation_id, [0x11; 16]);
    assert_eq!(chunk.offset, 0x0807_0605_0403_0201);
    assert_eq!(chunk.data, b"chunk");

    assert!(matches!(
        decode_client_chunk(&[0x11; 24]),
        Err(ClientControlError::InvalidChunk)
    ));
    let mut zero_id = vec![0; 16];
    zero_id.extend_from_slice(&0u64.to_le_bytes());
    zero_id.push(1);
    assert!(matches!(
        decode_client_chunk(&zero_id),
        Err(ClientControlError::InvalidChunk)
    ));
    assert!(matches!(
        decode_client_chunk(&vec![0x11; CLIENT_MAX_CHUNK_BYTES + 25]),
        Err(ClientControlError::InvalidChunk)
    ));
}
