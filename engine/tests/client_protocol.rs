use prdb_engine::client_protocol::{
    CLIENT_MAX_CHUNK_BYTES, ClientAdminCommand, ClientAdminRead, ClientControlError, ClientMode,
    decode_client_admin_command, decode_client_admin_read, decode_client_chunk,
    decode_client_credentials, decode_client_hello, decode_client_prepare, decode_client_seal,
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

#[test]
fn prepare_decoder_enforces_exact_canonical_schema_and_limits() {
    let hash = "00".repeat(32);
    let payload = format!(
        "{{\"manifest_version\":1,\"manifest\":\"bQ==\",\"artifact_bytes\":\"3\",\"artifact_sha256\":\"{hash}\",\"parameter_bytes\":\"0\",\"parameter_sha256\":\"{hash}\",\"scope_version\":1,\"requested_scope\":\"cw==\",\"deadline_ms\":\"30000\",\"workload_class\":\"analytical\"}}"
    );
    let prepare = decode_client_prepare(payload.as_bytes()).unwrap();
    assert_eq!(prepare.manifest, b"m");
    assert_eq!(prepare.requested_scope, b"s");
    assert_eq!(prepare.artifact_bytes, 3);
    assert_eq!(prepare.parameter_bytes, 0);
    assert_eq!(
        prepare.workload_class,
        prdb_engine::supervisor::WorkloadClass::Analytical
    );

    let leading_zero = payload.replace("\"artifact_bytes\":\"3\"", "\"artifact_bytes\":\"03\"");
    assert_eq!(
        decode_client_prepare(leading_zero.as_bytes()),
        Err(ClientControlError::InvalidPrepare)
    );
    let duplicate = payload.replace(
        "\"scope_version\":1",
        "\"scope_version\":1,\"scope_version\":1",
    );
    assert_eq!(
        decode_client_prepare(duplicate.as_bytes()),
        Err(ClientControlError::DuplicateKey)
    );
    let unknown = payload.replace(
        "\"workload_class\":\"analytical\"",
        "\"workload_class\":\"analytical\",\"principal\":\"x\"",
    );
    assert_eq!(
        decode_client_prepare(unknown.as_bytes()),
        Err(ClientControlError::UnknownKey)
    );
}

#[test]
fn seal_decoder_accepts_only_one_nonzero_canonical_invocation_id() {
    assert_eq!(
        decode_client_seal(br#"{"invocation_id":"000102030405060708090a0b0c0d0e0f"}"#),
        Ok([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15])
    );
    assert_eq!(
        decode_client_seal(br#"{"invocation_id":"00000000000000000000000000000000"}"#),
        Err(ClientControlError::InvalidInvocationId)
    );
    assert_eq!(
        decode_client_seal(br#"{"invocation_id":"000102030405060708090a0b0c0d0e0f","extra":"x"}"#),
        Err(ClientControlError::UnknownKey)
    );
}

#[test]
fn admin_read_decoder_accepts_token_inventory_and_rejects_mutation_shapes() {
    assert_eq!(
        decode_client_admin_read(
            br#"{"phase":"read","operation_version":1,"operation":"tokens.list","arguments":{"user_id":"000102030405060708090a0b0c0d0e0f","after_token_id":"101112131415161718191a1b1c1d1e1f"}}"#
        ),
        Ok(ClientAdminRead::ListTokens {
            user_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            after_token_id: Some([16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]),
        })
    );
    assert_eq!(
        decode_client_admin_read(
            br#"{"phase":"prepare","operation_version":1,"operation":"tokens.list","arguments":{"user_id":"000102030405060708090a0b0c0d0e0f"}}"#
        ),
        Err(ClientControlError::UnsupportedAdminOperation)
    );
    assert_eq!(
        decode_client_admin_read(
            br#"{"phase":"read","operation_version":1,"operation":"tokens.list","arguments":{"user_id":"000102030405060708090a0b0c0d0e0f","extra":"x"}}"#
        ),
        Err(ClientControlError::UnknownKey)
    );
    assert_eq!(
        decode_client_admin_read(
            br#"{"phase":"read","operation_version":1,"operation":"tokens.list","arguments":{"user_id":"000102030405060708090a0b0c0d0e0f",}}"#
        ),
        Err(ClientControlError::InvalidJson)
    );
}

#[test]
fn admin_revoke_decoder_requires_prepare_then_start_schemas() {
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"tokens.revoke","arguments":{"token_id":"000102030405060708090a0b0c0d0e0f"}}"#
        ),
        Ok(ClientAdminCommand::PrepareRevoke {
            token_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"start","operation_id":"101112131415161718191a1b1c1d1e1f"}"#
        ),
        Ok(ClientAdminCommand::Start {
            operation_id: [
                16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31
            ]
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"tokens.issue","arguments":{"user_id":"000102030405060708090a0b0c0d0e0f","label":"batch worker","expires_at_us":"1700000000000000"}}"#
        ),
        Ok(ClientAdminCommand::PrepareIssue {
            user_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            label: "batch worker".to_owned(),
            expires_at_us: 1_700_000_000_000_000,
        })
    );
}
