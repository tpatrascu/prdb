use super::json::encode_base64;
use super::{
    CLIENT_V2_PACKAGE_STATUS_KIND, CLIENT_V2_TX_ATTACH_KIND, CLIENT_V2_TX_BEGIN_KIND,
    CLIENT_V2_TX_COMMIT_KIND, CLIENT_V2_TX_ROLLBACK_KIND, CLIENT_V2_TX_STATUS_KIND,
    ClientControlError, ClientFrame, ClientFrameDecoder, ClientFrameError, ClientV2PackageState,
    ClientV2Request, ClientV2Response, ClientV2TransactionState,
    decode_client_prepare_for_protocol, decode_client_start_for_protocol,
};
use crate::credentials::TOKEN_SECRET_BYTES;

#[test]
fn v2_transaction_frames_roundtrip_fragmented_and_v1_rejects_them() {
    let frame = ClientFrame {
        kind: CLIENT_V2_TX_BEGIN_KIND,
        request_id: 1,
        response_sequence: 0,
        payload: b"{}".to_vec(),
    };
    let bytes = frame.encode_v2().unwrap();
    assert_eq!(
        &bytes[4..10],
        &[2, 0, 0, 0, CLIENT_V2_TX_BEGIN_KIND as u8, 0]
    );

    let mut decoder = ClientFrameDecoder::v2();
    let mut decoded = Vec::new();
    for part in bytes.chunks(3) {
        decoded.extend(decoder.push(part).unwrap());
    }
    decoder.finish().unwrap();
    assert_eq!(decoded, vec![frame.clone()]);

    let mut v1_decoder = ClientFrameDecoder::default();
    assert_eq!(
        v1_decoder.push(&bytes),
        Err(ClientFrameError::UnsupportedVersion { major: 2, minor: 0 })
    );

    let mut negotiating = ClientFrameDecoder::negotiating();
    assert_eq!(negotiating.protocol_version(), None);
    assert_eq!(negotiating.push(&bytes).unwrap(), vec![frame]);
    assert_eq!(negotiating.protocol_version(), Some((2, 0)));
    let v1_frame = ClientFrame {
        kind: 11,
        request_id: 2,
        response_sequence: 0,
        payload: b"{}".to_vec(),
    }
    .encode()
    .unwrap();
    assert_eq!(
        negotiating.push(&v1_frame),
        Err(ClientFrameError::UnsupportedVersion { major: 1, minor: 0 })
    );
}

#[test]
fn negotiating_decoder_selects_v1_for_legacy_connections() {
    let bytes = ClientFrame {
        kind: 11,
        request_id: 1,
        response_sequence: 0,
        payload: b"{}".to_vec(),
    }
    .encode()
    .unwrap();
    let mut decoder = ClientFrameDecoder::negotiating();
    assert_eq!(decoder.push(&bytes).unwrap().len(), 1);
    assert_eq!(decoder.protocol_version(), Some((1, 0)));
}

#[test]
fn v1_encoder_cannot_emit_v2_only_transaction_kinds() {
    let frame = ClientFrame {
        kind: CLIENT_V2_TX_BEGIN_KIND,
        request_id: 1,
        response_sequence: 0,
        payload: b"{}".to_vec(),
    };
    assert_eq!(frame.encode(), Err(ClientFrameError::InvalidKind(13)));
}

#[test]
fn prepare_transaction_binding_is_v2_only_and_requires_both_ids() {
    let base = r#"{"manifest_version":1,"manifest":"AQ==","artifact_bytes":"1","artifact_sha256":"0000000000000000000000000000000000000000000000000000000000000000","parameter_bytes":"0","parameter_sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","sql_data_bytes":"0","sql_data_sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","scope_version":1,"requested_scope":"AQ==","deadline_ms":"1","sdk_library_mode":"repository""#;
    let v2 = format!(
        "{base},\"transaction_id\":\"11111111111111111111111111111111\",\"package_id\":\"22222222222222222222222222222222\"}}"
    );
    let decoded = decode_client_prepare_for_protocol(v2.as_bytes(), 2).unwrap();
    assert_eq!(decoded.transaction_id, Some([0x11; 16]));
    assert_eq!(decoded.package_id, Some([0x22; 16]));
    assert_eq!(
        decode_client_prepare_for_protocol(v2.as_bytes(), 1),
        Err(ClientControlError::InvalidPrepare)
    );

    let only_transaction =
        format!("{base},\"transaction_id\":\"11111111111111111111111111111111\"}}");
    assert_eq!(
        decode_client_prepare_for_protocol(only_transaction.as_bytes(), 2),
        Err(ClientControlError::InvalidPrepare)
    );
    let zero_package = format!(
        "{base},\"transaction_id\":\"11111111111111111111111111111111\",\"package_id\":\"00000000000000000000000000000000\"}}"
    );
    assert_eq!(
        decode_client_prepare_for_protocol(zero_package.as_bytes(), 2),
        Err(ClientControlError::InvalidPrepare)
    );
}

#[test]
fn sql_text_prepare_requires_no_artifact_or_manifest_and_defaults_to_native_artifact() {
    let empty_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    let sql_text = format!(
        "{{\"submission_kind\":\"sql_text\",\"manifest_version\":0,\"manifest\":\"\",\"artifact_bytes\":\"0\",\"artifact_sha256\":\"{empty_sha256}\",\"parameter_bytes\":\"0\",\"parameter_sha256\":\"{empty_sha256}\",\"sql_data_bytes\":\"0\",\"sql_data_sha256\":\"{empty_sha256}\",\"scope_version\":1,\"requested_scope\":\"AQ==\",\"deadline_ms\":\"1\"}}"
    );
    let decoded = decode_client_prepare_for_protocol(sql_text.as_bytes(), 1).unwrap();
    assert_eq!(
        decoded.submission_kind,
        crate::supervisor::SubmissionKind::SqlText
    );
    assert_eq!(decoded.artifact_bytes, 0);
    assert!(decoded.manifest.is_empty());
    assert!(!decoded.repository_sdk_library);

    // A nonzero artifact for a SqlText submission is rejected.
    let with_artifact = sql_text.replace("\"artifact_bytes\":\"0\"", "\"artifact_bytes\":\"1\"");
    assert_eq!(
        decode_client_prepare_for_protocol(with_artifact.as_bytes(), 1),
        Err(ClientControlError::InvalidPrepare)
    );

    // Declaring repository SDK mode for a SqlText submission is rejected.
    let with_sdk_mode = sql_text.replace(
        "\"deadline_ms\":\"1\"}",
        "\"deadline_ms\":\"1\",\"sdk_library_mode\":\"repository\"}",
    );
    assert_eq!(
        decode_client_prepare_for_protocol(with_sdk_mode.as_bytes(), 1),
        Err(ClientControlError::InvalidPrepare)
    );

    // Omitting submission_kind entirely keeps today's legacy NativeArtifact
    // behavior, which still requires a nonempty artifact and manifest.
    let base = r#"{"manifest_version":1,"manifest":"AQ==","artifact_bytes":"1","artifact_sha256":"0000000000000000000000000000000000000000000000000000000000000000","parameter_bytes":"0","parameter_sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","sql_data_bytes":"0","sql_data_sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","scope_version":1,"requested_scope":"AQ==","deadline_ms":"1","sdk_library_mode":"repository"}"#;
    let decoded_legacy = decode_client_prepare_for_protocol(base.as_bytes(), 1).unwrap();
    assert_eq!(
        decoded_legacy.submission_kind,
        crate::supervisor::SubmissionKind::NativeArtifact
    );
}

#[test]
fn start_must_echo_sealed_v2_package_binding() {
    let start = br#"{"invocation_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","transaction_id":"11111111111111111111111111111111","package_id":"22222222222222222222222222222222"}"#;
    let decoded = decode_client_start_for_protocol(start, 2).unwrap();
    assert_eq!(decoded.invocation_id, [0xaa; 16]);
    assert_eq!(decoded.transaction_id, Some([0x11; 16]));
    assert_eq!(decoded.package_id, Some([0x22; 16]));
    assert!(decoded.actor.is_none());
    assert_eq!(
        decode_client_start_for_protocol(start, 1).err(),
        Some(ClientControlError::InvalidPrepare)
    );
    let missing_package = br#"{"invocation_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","transaction_id":"11111111111111111111111111111111"}"#;
    assert_eq!(
        decode_client_start_for_protocol(missing_package, 2).err(),
        Some(ClientControlError::InvalidPrepare)
    );
}

#[test]
fn start_actor_proof_requires_a_complete_v2_credential_set() {
    let secret = encode_base64(&[0x5a; TOKEN_SECRET_BYTES]);
    let start = format!(
        "{{\"invocation_id\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"expected_actor_name\":\"target_user\",\"actor_token_id\":\"11111111111111111111111111111111\",\"actor_token_secret\":\"{secret}\"}}"
    );
    let decoded = decode_client_start_for_protocol(start.as_bytes(), 2).unwrap();
    let actor = decoded.actor.expect("actor proof");
    assert_eq!(actor.expected_name, "target_user");
    assert_eq!(actor.token_id, [0x11; 16]);
    assert_eq!(*actor.token_secret, [0x5a; TOKEN_SECRET_BYTES]);
    assert_eq!(
        decode_client_start_for_protocol(start.as_bytes(), 1).err(),
        Some(ClientControlError::InvalidPrepare)
    );
    let incomplete = br#"{"invocation_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","expected_actor_name":"target_user","actor_token_id":"11111111111111111111111111111111"}"#;
    assert_eq!(
        decode_client_start_for_protocol(incomplete, 2).err(),
        Some(ClientControlError::InvalidPrepare)
    );
}

#[test]
fn transaction_request_payloads_have_canonical_golden_encodings() {
    let transaction_id = [0xabu8; 16];
    let package_id = [0xcdu8; 16];
    let cases = [
        (
            ClientV2Request::TxBegin {
                begin_request_id: [0xabu8; 16],
            },
            CLIENT_V2_TX_BEGIN_KIND,
            "{\"begin_request_id\":\"abababababababababababababababab\"}",
        ),
        (
            ClientV2Request::TxAttach { transaction_id },
            CLIENT_V2_TX_ATTACH_KIND,
            "{\"transaction_id\":\"abababababababababababababababab\"}",
        ),
        (
            ClientV2Request::TxCommit { transaction_id },
            CLIENT_V2_TX_COMMIT_KIND,
            "{\"transaction_id\":\"abababababababababababababababab\"}",
        ),
        (
            ClientV2Request::TxRollback { transaction_id },
            CLIENT_V2_TX_ROLLBACK_KIND,
            "{\"transaction_id\":\"abababababababababababababababab\"}",
        ),
        (
            ClientV2Request::TxStatus { transaction_id },
            CLIENT_V2_TX_STATUS_KIND,
            "{\"transaction_id\":\"abababababababababababababababab\"}",
        ),
        (
            ClientV2Request::PackageStatus {
                transaction_id,
                package_id,
            },
            CLIENT_V2_PACKAGE_STATUS_KIND,
            "{\"transaction_id\":\"abababababababababababababababab\",\"package_id\":\"cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd\"}",
        ),
    ];
    for (request, expected_kind, expected_payload) in cases {
        let (kind, payload) = request.encode().unwrap();
        assert_eq!(kind, expected_kind);
        assert_eq!(payload, expected_payload.as_bytes());
        assert_eq!(ClientV2Request::decode(kind, &payload), Ok(request));
    }
}

#[test]
fn transaction_request_decoder_rejects_ambiguous_or_extra_fields() {
    assert!(ClientV2Request::decode(CLIENT_V2_TX_BEGIN_KIND, b"{\"extra\":\"x\"}").is_err());
    assert!(
        ClientV2Request::decode(
            CLIENT_V2_TX_ATTACH_KIND,
            b"{\"transaction_id\":\"abababababababababababababababab\",\"extra\":\"x\"}"
        )
        .is_err()
    );
    assert!(
        ClientV2Request::decode(
            CLIENT_V2_TX_STATUS_KIND,
            b"{\"transaction_id\":\"ABABABABABABABABABABABABABABABAB\"}"
        )
        .is_err()
    );
}

#[test]
fn transaction_responses_match_canonical_schemas() {
    let transaction_id = [0xabu8; 16];
    let begin = ClientV2Response::TransactionBegun {
        transaction_id,
        state: ClientV2TransactionState::Active,
        idle_timeout_ms: 1_000,
        lifetime_ms: 60_000,
        max_package_bytes: 65_536,
        max_packages: 128,
    };
    let begin_bytes = begin.encode();
    assert_eq!(
            begin_bytes,
            b"{\"transaction_id\":\"abababababababababababababababab\",\"state\":\"active\",\"idle_timeout_ms\":1000,\"lifetime_ms\":60000,\"max_package_bytes\":65536,\"max_packages\":128}"
        );
    assert_eq!(
        ClientV2Response::decode(CLIENT_V2_TX_BEGIN_KIND, &begin_bytes),
        Ok(begin)
    );
    let expired_begin = ClientV2Response::TransactionBegun {
        transaction_id,
        state: ClientV2TransactionState::Expired,
        idle_timeout_ms: 1_000,
        lifetime_ms: 60_000,
        max_package_bytes: 65_536,
        max_packages: 128,
    };
    assert_eq!(
        ClientV2Response::decode(CLIENT_V2_TX_BEGIN_KIND, &expired_begin.encode()),
        Ok(expired_begin)
    );

    let committed = ClientV2Response::Transaction {
        transaction_id,
        state: ClientV2TransactionState::Committed,
    };
    let committed_bytes = committed.encode();
    assert_eq!(
        committed_bytes,
        b"{\"transaction_id\":\"abababababababababababababababab\",\"state\":\"committed\"}"
    );
    assert_eq!(
        ClientV2Response::decode(CLIENT_V2_TX_COMMIT_KIND, &committed_bytes),
        Ok(committed)
    );

    let attached = ClientV2Response::Transaction {
        transaction_id,
        state: ClientV2TransactionState::AbortOnly,
    };
    assert_eq!(
        ClientV2Response::decode(CLIENT_V2_TX_ATTACH_KIND, &attached.encode()),
        Ok(attached)
    );
    assert!(ClientV2Response::decode(CLIENT_V2_TX_ROLLBACK_KIND, &committed_bytes).is_err());

    let package = ClientV2Response::Package {
        transaction_id,
        package_id: [0xcdu8; 16],
        state: ClientV2PackageState::Completed,
        result_lost: true,
    };
    let package_bytes = package.encode();
    assert_eq!(
            package_bytes,
            b"{\"transaction_id\":\"abababababababababababababababab\",\"package_id\":\"cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd\",\"state\":\"completed\",\"result_lost\":true}"
        );
    assert_eq!(
        ClientV2Response::decode(CLIENT_V2_PACKAGE_STATUS_KIND, &package_bytes),
        Ok(package)
    );
}

#[test]
fn transaction_response_decoder_rejects_invalid_limits_and_lost_state() {
    assert!(ClientV2Response::decode(
            CLIENT_V2_TX_BEGIN_KIND,
            b"{\"transaction_id\":\"abababababababababababababababab\",\"state\":\"active\",\"idle_timeout_ms\":0,\"lifetime_ms\":1,\"max_package_bytes\":1,\"max_packages\":1}"
        )
        .is_err());
    assert!(ClientV2Response::decode(
            CLIENT_V2_PACKAGE_STATUS_KIND,
            b"{\"transaction_id\":\"abababababababababababababababab\",\"package_id\":\"cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd\",\"state\":\"running\",\"result_lost\":true}"
        )
        .is_err());
}
