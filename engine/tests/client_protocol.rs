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
fn admin_read_decoder_accepts_empty_visible_schema_discovery_request() {
    let request = br#"{"phase":"read","operation_version":1,"operation":"metadata.schemas.list","arguments":{}}"#;
    assert_eq!(
        decode_client_admin_read(request),
        Ok(ClientAdminRead::ListSchemas)
    );
    assert_eq!(
        decode_client_admin_read(
            br#"{"phase":"read","operation_version":1,"operation":"metadata.schemas.list","arguments":{"principal_id":"000102030405060708090a0b0c0d0e0f"}}"#
        ),
        Err(ClientControlError::UnknownKey)
    );
    assert_eq!(
        decode_client_admin_read(
            br#"{"phase":"prepare","operation_version":1,"operation":"metadata.schemas.list","arguments":{}}"#
        ),
        Err(ClientControlError::UnsupportedAdminOperation)
    );
}

#[test]
fn admin_read_decoder_requires_exact_relation_discovery_arguments() {
    assert_eq!(
        decode_client_admin_read(
            br#"{"phase":"read","operation_version":1,"operation":"metadata.relations.list","arguments":{"schema_id":"000102030405060708090a0b0c0d0e0f"}}"#
        ),
        Ok(ClientAdminRead::ListRelations {
            schema_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        })
    );
    assert_eq!(
        decode_client_admin_read(
            br#"{"phase":"read","operation_version":1,"operation":"metadata.relations.list","arguments":{"schema_id":"000102030405060708090a0b0c0d0e0f","cursor":"x"}}"#
        ),
        Err(ClientControlError::InvalidMetadataCursor)
    );
}

#[test]
fn metadata_page_decoders_validate_limits_and_session_cursor_shape() {
    let cursor = "d30102030405060708090a0b0c0d0e0f";
    assert_eq!(
        decode_client_admin_read(
            format!("{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"metadata.schemas.list\",\"arguments\":{{\"limit\":\"2\",\"cursor\":\"{cursor}\"}}}}")
                .as_bytes()
        ),
        Ok(ClientAdminRead::ListSchemasPage {
            page_limit: 2,
            cursor: Some([0xd3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
        })
    );
    assert_eq!(
        decode_client_admin_read(
            br#"{"phase":"read","operation_version":1,"operation":"metadata.relations.list","arguments":{"schema_id":"000102030405060708090a0b0c0d0e0f","limit":"128"}}"#
        ),
        Ok(ClientAdminRead::ListRelationsPage {
            schema_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            page_limit: 128,
            cursor: None,
        })
    );
    assert_eq!(
        decode_client_admin_read(
            br#"{"phase":"read","operation_version":1,"operation":"metadata.schemas.list","arguments":{"limit":"0"}}"#
        ),
        Err(ClientControlError::InvalidMetadataPageLimit)
    );
    assert_eq!(
        decode_client_admin_read(
            br#"{"phase":"read","operation_version":1,"operation":"metadata.schemas.list","arguments":{"limit":"01"}}"#
        ),
        Err(ClientControlError::InvalidMetadataPageLimit)
    );
    assert_eq!(
        decode_client_admin_read(
            br#"{"phase":"read","operation_version":1,"operation":"metadata.schemas.list","arguments":{"limit":"129"}}"#
        ),
        Err(ClientControlError::InvalidMetadataPageLimit)
    );
    assert_eq!(
        decode_client_admin_read(
            br#"{"phase":"read","operation_version":1,"operation":"metadata.schemas.list","arguments":{"limit":"1","cursor":"000102030405060708090a0b0c0d0e0f"}}"#
        ),
        Err(ClientControlError::InvalidMetadataCursor)
    );
}

#[test]
fn admin_read_decoder_requires_schema_and_relation_for_column_discovery() {
    assert_eq!(
        decode_client_admin_read(
            br#"{"phase":"read","operation_version":1,"operation":"metadata.columns.list","arguments":{"schema_id":"000102030405060708090a0b0c0d0e0f","relation_id":"101112131415161718191a1b1c1d1e1f"}}"#
        ),
        Ok(ClientAdminRead::ListColumns {
            schema_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            relation_id: [16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31],
        })
    );
    assert_eq!(
        decode_client_admin_read(
            br#"{"phase":"read","operation_version":1,"operation":"metadata.columns.list","arguments":{"schema_id":"000102030405060708090a0b0c0d0e0f"}}"#
        ),
        Err(ClientControlError::MissingKey)
    );
}

#[test]
fn admin_revoke_decoder_requires_prepare_then_start_schemas() {
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"users.drop","arguments":{"user_id":"000102030405060708090a0b0c0d0e0f"}}"#
        ),
        Ok(ClientAdminCommand::PrepareDropIdentity {
            identity_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        })
    );
    let check_eir =
        prdb_engine::expression::encode_expression(&prdb_engine::authorization::Expr::Literal(
            Some(prdb_engine::tuple::ScalarValue::Boolean(true)),
        ))
        .expect("encode CHECK fixture");
    let check_eir_hex = check_eir
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(
        decode_client_admin_command(
            format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"constraints.create_check\",\"arguments\":{{\"table_id\":\"000102030405060708090a0b0c0d0e0f\",\"expression_eir1\":\"{check_eir_hex}\",\"dependency_column_ids\":[]}}}}"
            )
            .as_bytes()
        ),
        Ok(ClientAdminCommand::PrepareCreateCheckConstraint {
            table_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            expression: prdb_engine::authorization::Expr::Literal(Some(
                prdb_engine::tuple::ScalarValue::Boolean(true),
            )),
            dependency_column_ids: vec![],
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"constraints.drop_check","arguments":{"constraint_id":"000102030405060708090a0b0c0d0e0f"}}"#
        ),
        Ok(ClientAdminCommand::PrepareDropCheckConstraint {
            constraint_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        })
    );
    for (operation, command) in [
        (
            "constraints.drop_primary_key",
            ClientAdminCommand::PrepareDropPrimaryKey {
                constraint_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            },
        ),
        (
            "constraints.drop_unique",
            ClientAdminCommand::PrepareDropUniqueConstraint {
                constraint_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            },
        ),
    ] {
        assert_eq!(
            decode_client_admin_command(
                format!(
                    "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"{operation}\",\"arguments\":{{\"constraint_id\":\"000102030405060708090a0b0c0d0e0f\"}}}}"
                )
                .as_bytes()
            ),
            Ok(command)
        );
    }
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"roles.drop","arguments":{"role_id":"000102030405060708090a0b0c0d0e0f"}}"#
        ),
        Ok(ClientAdminCommand::PrepareDropIdentity {
            identity_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"users.login","arguments":{"user_id":"000102030405060708090a0b0c0d0e0f","enabled":"false"}}"#
        ),
        Ok(ClientAdminCommand::PrepareSetUserLogin {
            user_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            enabled: false,
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"users.rename","arguments":{"user_id":"000102030405060708090a0b0c0d0e0f","name":"renamed_user"}}"#
        ),
        Ok(ClientAdminCommand::PrepareRenameIdentity {
            identity_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            name: "renamed_user".into(),
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"roles.rename","arguments":{"role_id":"000102030405060708090a0b0c0d0e0f","name":"renamed_role"}}"#
        ),
        Ok(ClientAdminCommand::PrepareRenameIdentity {
            identity_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            name: "renamed_role".into(),
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"privileges.grant","arguments":{"grantee_id":"000102030405060708090a0b0c0d0e0f","privilege":"manage_roles"}}"#
        ),
        Ok(ClientAdminCommand::PrepareSystemPrivilege {
            grantee_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            privilege: prdb_engine::storage_manager::AdministrativeSystemPrivilege::ManageRoles,
            grant: true,
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"privileges.revoke","arguments":{"grantee_id":"000102030405060708090a0b0c0d0e0f","privilege":"create_schema"}}"#
        ),
        Ok(ClientAdminCommand::PrepareSystemPrivilege {
            grantee_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            privilege: prdb_engine::storage_manager::AdministrativeSystemPrivilege::CreateSchema,
            grant: false,
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"objects.grant","arguments":{"grantee_id":"000102030405060708090a0b0c0d0e0f","object_id":"101112131415161718191a1b1c1d1e1f","privilege":"references","target_scope":"column","target_id":"202122232425262728292a2b2c2d2e2f"}}"#
        ),
        Ok(ClientAdminCommand::PrepareObjectGrant {
            grantee_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            object_id: [16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31],
            privilege: prdb_engine::storage_manager::ObjectGrantPrivilege::References,
            target: prdb_engine::storage_manager::ObjectGrantTarget::Column([
                32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
            ]),
            grant: true,
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"objects.revoke","arguments":{"grantee_id":"000102030405060708090a0b0c0d0e0f","object_id":"101112131415161718191a1b1c1d1e1f","privilege":"references","target_scope":"object","target_id":"101112131415161718191a1b1c1d1e1f"}}"#
        ),
        Ok(ClientAdminCommand::PrepareObjectGrant {
            grantee_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            object_id: [16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31],
            privilege: prdb_engine::storage_manager::ObjectGrantPrivilege::References,
            target: prdb_engine::storage_manager::ObjectGrantTarget::Object,
            grant: false,
        })
    );
    assert!(decode_client_admin_command(
        br#"{"phase":"prepare","operation_version":1,"operation":"objects.grant","arguments":{"grantee_id":"000102030405060708090a0b0c0d0e0f","object_id":"101112131415161718191a1b1c1d1e1f","privilege":"all","target_scope":"object","target_id":"101112131415161718191a1b1c1d1e1f"}}"#
    )
    .is_err());
    let policy_eir =
        prdb_engine::expression::encode_expression(&prdb_engine::authorization::Expr::Literal(
            Some(prdb_engine::tuple::ScalarValue::Boolean(true)),
        ))
        .expect("encode policy fixture");
    let policy_eir_hex = policy_eir
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let policy_create = format!(
        "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"policies.create\",\"arguments\":{{\"table_id\":\"101112131415161718191a1b1c1d1e1f\",\"name\":\"visible_rows\",\"kind\":\"row\",\"policy_operation\":\"select\",\"effect\":\"allow\",\"recipients\":[\"role:000102030405060708090a0b0c0d0e0f\"],\"target_column_ids\":[],\"using_expression_eir1\":\"{policy_eir_hex}\",\"with_check_expression_eir1\":null}}}}"
    );
    assert!(matches!(
        decode_client_admin_command(policy_create.as_bytes()),
        Ok(ClientAdminCommand::PrepareCreatePolicy {
            table_id: [16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31],
            name,
            kind: prdb_engine::storage_manager::PolicyKind::Row,
            operation: prdb_engine::authorization::Operation::Select,
            effect: prdb_engine::authorization::Effect::Allow,
            recipients,
            target_column_ids,
            using_expression: Some(prdb_engine::authorization::Expr::Literal(Some(
                prdb_engine::tuple::ScalarValue::Boolean(true)
            ))),
            with_check_expression: None,
        }) if name == "visible_rows"
            && recipients == vec![prdb_engine::authorization::Subject::Role([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15])]
            && target_column_ids.is_empty()
    ));
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"policies.drop","arguments":{"policy_id":"000102030405060708090a0b0c0d0e0f"}}"#
        ),
        Ok(ClientAdminCommand::PrepareDropPolicy {
            policy_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        })
    );
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
    let wire_cursor = format!(
        "01{}{}{}",
        "01".repeat(16),
        "00".repeat(15) + "01",
        "02".repeat(16)
    );
    let runtime_request = format!(
        "{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"runtime.invocations.list\",\"arguments\":{{\"limit\":\"4\",\"cursor\":\"{wire_cursor}\"}}}}"
    );
    assert!(matches!(
        decode_client_admin_command(runtime_request.as_bytes()),
        Ok(ClientAdminCommand::ListRuntimeInvocations { page_limit: 4, cursor: Some(cursor) })
            if cursor.to_wire() == wire_cursor
    ));
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"users.create","arguments":{"name":"new_user_1"}}"#
        ),
        Ok(ClientAdminCommand::PrepareCreateUser {
            name: "new_user_1".into(),
        })
    );
    assert!(decode_client_admin_command(
        br#"{"phase":"prepare","operation_version":1,"operation":"users.create","arguments":{"name":"NewUser"}}"#
    )
    .is_err());
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"roles.create","arguments":{"name":"reporting_role"}}"#
        ),
        Ok(ClientAdminCommand::PrepareCreateRole {
            name: "reporting_role".into(),
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"schemas.create","arguments":{"name":"app_data","owner_id":"000102030405060708090a0b0c0d0e0f"}}"#
        ),
        Ok(ClientAdminCommand::PrepareCreateSchema {
            name: "app_data".into(),
            owner_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        })
    );
    assert!(decode_client_admin_command(
        br#"{"phase":"prepare","operation_version":1,"operation":"schemas.create","arguments":{"name":"AppData","owner_id":"000102030405060708090a0b0c0d0e0f"}}"#
    )
    .is_err());
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"schemas.drop","arguments":{"schema_id":"000102030405060708090a0b0c0d0e0f"}}"#
        ),
        Ok(ClientAdminCommand::PrepareDropSchema {
            schema_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"schemas.set_owner","arguments":{"schema_id":"000102030405060708090a0b0c0d0e0f","owner_id":"101112131415161718191a1b1c1d1e1f"}}"#
        ),
        Ok(ClientAdminCommand::PrepareSetSchemaOwner {
            schema_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            owner_id: [16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31],
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"schemas.rename","arguments":{"schema_id":"000102030405060708090a0b0c0d0e0f","name":"archive_data"}}"#
        ),
        Ok(ClientAdminCommand::PrepareRenameSchema {
            schema_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            name: "archive_data".into(),
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"tables.drop","arguments":{"table_id":"000102030405060708090a0b0c0d0e0f"}}"#
        ),
        Ok(ClientAdminCommand::PrepareDropTable {
            table_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"foreign_keys.drop","arguments":{"foreign_key_id":"000102030405060708090a0b0c0d0e0f"}}"#
        ),
        Ok(ClientAdminCommand::PrepareDropForeignKey {
            foreign_key_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        })
    );
    assert!(decode_client_admin_command(
        br#"{"phase":"prepare","operation_version":1,"operation":"foreign_keys.drop","arguments":{"foreign_key_id":"000102030405060708090a0b0c0d0e0f","cascade":"true"}}"#
    )
    .is_err());
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"foreign_keys.create","arguments":{"source_table_id":"000102030405060708090a0b0c0d0e0f","source_column_ids":["101112131415161718191a1b1c1d1e1f"],"target_constraint_id":"202122232425262728292a2b2c2d2e2f"}}"#
        ),
        Ok(ClientAdminCommand::PrepareCreateForeignKey {
            source_table_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            source_column_ids: vec![[16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]],
            target_constraint_id: [32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47],
        })
    );
    assert!(decode_client_admin_command(
        br#"{"phase":"prepare","operation_version":1,"operation":"foreign_keys.create","arguments":{"source_table_id":"000102030405060708090a0b0c0d0e0f","source_column_ids":["101112131415161718191a1b1c1d1e1f"],"target_constraint_id":"202122232425262728292a2b2c2d2e2f","cascade":"true"}}"#
    )
    .is_err());
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"constraints.create_unique","arguments":{"table_id":"000102030405060708090a0b0c0d0e0f","column_ids":["101112131415161718191a1b1c1d1e1f","202122232425262728292a2b2c2d2e2f"]}}"#
        ),
        Ok(ClientAdminCommand::PrepareCreateUniqueConstraint {
            table_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            column_ids: vec![
                [16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31],
                [32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47],
            ],
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"indexes.create","arguments":{"table_id":"000102030405060708090a0b0c0d0e0f","name":"user_lookup","column_ids":["101112131415161718191a1b1c1d1e1f"],"unique":true}}"#
        ),
        Ok(ClientAdminCommand::PrepareCreateIndex {
            table_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            name: "user_lookup".to_owned(),
            column_ids: vec![[16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]],
            unique: true,
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"indexes.drop","arguments":{"index_id":"000102030405060708090a0b0c0d0e0f"}}"#
        ),
        Ok(ClientAdminCommand::PrepareDropIndex {
            index_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"indexes.rebuild","arguments":{"index_id":"000102030405060708090a0b0c0d0e0f","column_ids":["101112131415161718191a1b1c1d1e1f"],"unique":false}}"#
        ),
        Ok(ClientAdminCommand::PrepareRebuildIndex {
            index_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            column_ids: vec![[16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]],
            unique: false,
        })
    );
    assert!(decode_client_admin_command(
        br#"{"phase":"prepare","operation_version":1,"operation":"constraints.create_unique","arguments":{"table_id":"000102030405060708090a0b0c0d0e0f","column_ids":["101112131415161718191a1b1c1d1e1f"],"nullable":"true"}}"#
    )
    .is_err());
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"constraints.create_primary_key","arguments":{"table_id":"000102030405060708090a0b0c0d0e0f","column_ids":["101112131415161718191a1b1c1d1e1f"]}}"#
        ),
        Ok(ClientAdminCommand::PrepareCreatePrimaryKey {
            table_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            column_ids: vec![[16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]],
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"tables.rename","arguments":{"table_id":"000102030405060708090a0b0c0d0e0f","name":"renamed_table"}}"#
        ),
        Ok(ClientAdminCommand::PrepareRenameTable {
            table_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            name: "renamed_table".into(),
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"tables.set_owner","arguments":{"table_id":"000102030405060708090a0b0c0d0e0f","owner_id":"101112131415161718191a1b1c1d1e1f"}}"#
        ),
        Ok(ClientAdminCommand::PrepareSetTableOwner {
            table_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            owner_id: [16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31],
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"tables.rename_column","arguments":{"table_id":"000102030405060708090a0b0c0d0e0f","column_id":"101112131415161718191a1b1c1d1e1f","name":"renamed_column"}}"#
        ),
        Ok(ClientAdminCommand::PrepareRenameColumn {
            table_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            column_id: [16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31],
            name: "renamed_column".into(),
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"tables.alter_column_nullability","arguments":{"table_id":"000102030405060708090a0b0c0d0e0f","column_id":"101112131415161718191a1b1c1d1e1f","nullable":"false"}}"#
        ),
        Ok(ClientAdminCommand::PrepareSetColumnNullable {
            table_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            column_id: [16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31],
            nullable: false,
        })
    );
    assert!(decode_client_admin_command(
        br#"{"phase":"prepare","operation_version":1,"operation":"tables.alter_column_nullability","arguments":{"table_id":"000102030405060708090a0b0c0d0e0f","column_id":"101112131415161718191a1b1c1d1e1f","nullable":"maybe"}}"#
    )
    .is_err());
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"tables.set_column_default","arguments":{"table_id":"000102030405060708090a0b0c0d0e0f","column_id":"101112131415161718191a1b1c1d1e1f","type":"integer","default":{"kind":"literal","value":"42"}}}"#
        ),
        Ok(ClientAdminCommand::PrepareAlterColumnDefault {
            table_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            column_id: [16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31],
            default: Some(
                prdb_engine::storage_manager::TableColumnDefault::Literal(Some(
                    prdb_engine::tuple::ScalarValue::Integer(42),
                )),
            ),
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"tables.drop_column_default","arguments":{"table_id":"000102030405060708090a0b0c0d0e0f","column_id":"101112131415161718191a1b1c1d1e1f"}}"#
        ),
        Ok(ClientAdminCommand::PrepareAlterColumnDefault {
            table_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            column_id: [16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31],
            default: None,
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"tables.add_column","arguments":{"table_id":"000102030405060708090a0b0c0d0e0f","column":{"name":"label","type":"text","nullable":false,"generated_uuid_identity":false,"default":{"kind":"literal","value":"ready"}}}}"#
        ),
        Ok(ClientAdminCommand::PrepareAddColumn {
            table_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            column: prdb_engine::storage_manager::TableColumnDefinition {
                name: "label".into(),
                scalar_type: prdb_engine::tuple::ScalarType::Text,
                nullable: false,
                generated_uuid_identity: false,
                default: Some(
                    prdb_engine::storage_manager::TableColumnDefault::Literal(Some(
                        prdb_engine::tuple::ScalarValue::Text("ready".into()),
                    )),
                ),
            },
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"tables.drop_column","arguments":{"table_id":"000102030405060708090a0b0c0d0e0f","column_id":"101112131415161718191a1b1c1d1e1f"}}"#
        ),
        Ok(ClientAdminCommand::PrepareDropColumn {
            table_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            column_id: [16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31],
        })
    );
    assert_eq!(
        decode_client_admin_command(
            br#"{"phase":"prepare","operation_version":1,"operation":"tables.create","arguments":{"schema_id":"000102030405060708090a0b0c0d0e0f","name":"events","columns":[{"name":"event_id","type":"uuid","nullable":false,"generated_uuid_identity":true},{"name":"amount","type":"decimal(12,2)","nullable":true,"generated_uuid_identity":false,"default":{"kind":"literal","value":"12.30"}}]}}"#
        ),
        Ok(ClientAdminCommand::PrepareCreateTable {
            schema_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            name: "events".into(),
            columns: vec![
                prdb_engine::storage_manager::TableColumnDefinition {
                    name: "event_id".into(),
                    scalar_type: prdb_engine::tuple::ScalarType::Uuid,
                    nullable: false,
                    generated_uuid_identity: true,
                    default: None,
                },
                prdb_engine::storage_manager::TableColumnDefinition {
                    name: "amount".into(),
                    scalar_type: prdb_engine::tuple::ScalarType::Decimal {
                        precision: 12,
                        scale: 2,
                    },
                    nullable: true,
                    generated_uuid_identity: false,
                    default: Some(
                        prdb_engine::storage_manager::TableColumnDefault::Literal(Some(
                            prdb_engine::tuple::ScalarValue::Decimal(1230),
                        )),
                    ),
                },
            ],
        })
    );
    for invalid in [
        br#"{"phase":"prepare","operation_version":1,"operation":"tables.create","arguments":{"schema_id":"000102030405060708090a0b0c0d0e0f","name":"events","columns":[{"name":"id","type":"uuid","nullable":false}]}}"#.as_slice(),
        br#"{"phase":"prepare","operation_version":1,"operation":"tables.create","arguments":{"schema_id":"000102030405060708090a0b0c0d0e0f","name":"events","columns":[{"name":"id","type":"custom","nullable":false,"generated_uuid_identity":false}]}}"#.as_slice(),
        br#"{"phase":"prepare","operation_version":1,"operation":"tables.create","arguments":{"schema_id":"000102030405060708090a0b0c0d0e0f","name":"events","columns":[{"name":"id","type":"text","nullable":false,"generated_uuid_identity":false,"default":{"kind":"current_principal_id"}}]}}"#.as_slice(),
        br#"{"phase":"prepare","operation_version":1,"operation":"tables.add_column","arguments":{"table_id":"000102030405060708090a0b0c0d0e0f","column":{"name":"id","type":"text","nullable":false,"generated_uuid_identity":false}}}"#.as_slice(),
        br#"{"phase":"prepare","operation_version":1,"operation":"tables.add_column","arguments":{"table_id":"000102030405060708090a0b0c0d0e0f","column":{"name":"id","type":"uuid","nullable":false,"generated_uuid_identity":false,"default":{"kind":"current_principal_id"}}}}"#.as_slice(),
        br#"{"phase":"prepare","operation_version":1,"operation":"tables.create","arguments":{"schema_id":"000102030405060708090a0b0c0d0e0f","name":"events","columns":[]}}"#.as_slice(),
    ] {
        assert!(decode_client_admin_command(invalid).is_err());
    }
    let membership_request = format!(
        "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"roles.grant\",\"arguments\":{{\"member_id\":\"{}\",\"role_id\":\"{}\"}}}}",
        "01".repeat(16),
        "02".repeat(16)
    );
    assert!(matches!(
        decode_client_admin_command(membership_request.as_bytes()),
        Ok(ClientAdminCommand::PrepareGrantRole { member_id, role_id })
            if member_id == [1; 16] && role_id == [2; 16]
    ));
    let revoke_membership_request = membership_request.replace("roles.grant", "roles.revoke");
    assert!(matches!(
        decode_client_admin_command(revoke_membership_request.as_bytes()),
        Ok(ClientAdminCommand::PrepareRevokeRole { member_id, role_id })
            if member_id == [1; 16] && role_id == [2; 16]
    ));
    assert_eq!(
        decode_client_admin_command(
            format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"privileges.grant\",\"arguments\":{{\"grantee_id\":\"{}\",\"privilege\":\"manage_roles\"}}}}",
                "03".repeat(16)
            )
            .as_bytes()
        ),
        Ok(ClientAdminCommand::PrepareSystemPrivilege {
            grantee_id: [3; 16],
            privilege: prdb_engine::storage_manager::AdministrativeSystemPrivilege::ManageRoles,
            grant: true,
        })
    );
}
