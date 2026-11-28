use prdb_engine::authorization::Operation;
use prdb_engine::client_config::{
    ClientConfig, ClientCredential, ClientInstance, ClientTools, SdkSelection, encode_token_secret,
};
use prdb_engine::client_credentials::{ClientCredentialProfiles, write_client_credentials};
use prdb_engine::client_protocol::ClientCredentials;
use prdb_engine::client_submit::{
    AdminColumnDefault, AdminPolicyRecipient, AdminSystemPrivilege, SdkLibraryInfo,
};
use prdb_engine::interactive_sql::{InteractiveStatement, parse_interactive_sql};
use prdb_engine::tuple::{ScalarType, ScalarValue};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::{self};
use std::io::{self};
use std::os::unix::fs::PermissionsExt;
use std::time::{SystemTime, UNIX_EPOCH};

use super::*;

#[test]
fn instance_set_preserves_sdk_selection_and_updates_endpoint_credentials() {
    let root = std::env::temp_dir().join(format!(
        "prdb-client-instance-test-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let config_path = root.join("client.json");
    let credential_path = root.join("credential.json");
    let named_credential_path = root.join("runas.json");
    let config = ClientConfig {
        format: prdb_engine::client_config::CONFIG_FORMAT.to_owned(),
        default_instance: Some("dev".to_owned()),
        client_tools: None,
        instances: vec![ClientInstance {
            name: "dev".to_owned(),
            address: "127.0.0.1".to_owned(),
            port: 7443,
            sdk: SdkSelection {
                release: 7,
                target: "linux-x86_64".to_owned(),
                cpu_baseline: "x86_64-v1".to_owned(),
                runtime_profile: "glibc-test".to_owned(),
                native_abi: 1,
            },
            default_credential: "alice".to_owned(),
            credentials: vec![ClientCredential {
                name: "alice".to_owned(),
                username: "alice".to_owned(),
                token_id: "00".repeat(16),
                token_secret: encode_token_secret(&[0; 32]),
            }],
        }],
    };
    config.save_atomic(&config_path).unwrap();
    let credentials = ClientCredentials {
        token_id: [0x12; 16],
        token_secret: [0x34; 32],
    };
    write_client_credentials(&credential_path, &credentials).unwrap();
    drop(credentials);
    let runas = ClientCredentials {
        token_id: [0x56; 16],
        token_secret: [0x78; 32],
    };
    write_client_credentials(&named_credential_path, &runas).unwrap();
    drop(runas);
    let mut fields = HashMap::from([
        ("--config".to_owned(), config_path.clone().into_os_string()),
        ("--instance".to_owned(), "dev".into()),
        ("--address".to_owned(), "192.0.2.8".into()),
        ("--port".to_owned(), "7444".into()),
        ("--default-credential".to_owned(), "alice".into()),
        ("--username".to_owned(), "alice".into()),
        ("--credentials".to_owned(), credential_path.into_os_string()),
    ]);
    run_client_config_command(
        "instance set",
        &mut fields,
        &[std::ffi::OsString::from(format!(
            "runas=bob={}",
            named_credential_path.display()
        ))],
    )
    .unwrap();
    let updated = ClientConfig::load(&config_path).unwrap();
    let instance = updated.instance(None).unwrap();
    assert_eq!(instance.address, "192.0.2.8");
    assert_eq!(instance.port, 7444);
    assert_eq!(instance.sdk.release, 7);
    assert_eq!(instance.sdk.native_abi, 1);
    assert_eq!(instance.credentials[0].token_id, "12".repeat(16));
    assert_eq!(instance.credentials[1].name, "runas");
    assert_eq!(instance.credentials[1].username, "bob");
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn sdk_selection_requires_exact_local_release_profile_abi_and_digest() {
    let root = std::env::temp_dir().join(format!(
        "prdb-client-sdk-selection-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let include = root.join("sdk/include");
    let release = root.join("sdk/releases/linux-x86_64/x86_64-v1/glibc-test/sdk-3-abis1,4");
    std::fs::create_dir_all(&include).unwrap();
    std::fs::create_dir_all(&release).unwrap();
    let bytes = b"matching sdk release";
    std::fs::write(release.join("libprdb_sql.so.3"), bytes).unwrap();
    std::fs::write(release.join("sdk-release.json"), b"metadata").unwrap();
    let tools = ClientTools {
        include_dir: include.display().to_string(),
        tcc: "tcc".to_owned(),
        shared_linker: "cc".to_owned(),
        patchelf: "patchelf".to_owned(),
    };
    let digest: [u8; 32] = Sha256::digest(bytes).into();
    let info = SdkLibraryInfo {
        sdk_release: 3,
        native_abis: vec![1],
        target: "linux-x86_64".to_owned(),
        cpu_baseline: "x86_64-v1".to_owned(),
        runtime_profile: "glibc-test".to_owned(),
        soname: "libprdb_sql.so.3".to_owned(),
        sha256: digest,
        needed: vec!["libc.so.6".to_owned()],
        library_bytes: bytes.len() as u64,
    };
    assert!(verify_local_sdk_build(&root.join("client.json"), Some(&tools), &info, 4,).is_ok());
    assert!(verify_local_sdk_build(&root.join("client.json"), Some(&tools), &info, 2,).is_err());
    let mut bad_digest = info.clone();
    bad_digest.sha256 = [0; 32];
    assert!(
        verify_local_sdk_build(&root.join("client.json"), Some(&tools), &bad_digest, 4,).is_err()
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn configured_submit_rejects_package_mismatch_and_legacy_sdk_files() {
    let root = std::env::temp_dir().join(format!(
        "prdb-client-submit-selection-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let manifest = prdb_engine::submission_manifest::encode_manifest_v1(
        &prdb_engine::submission_manifest::ArtifactManifestV1 {
            native_abi_version: 1,
            sdk_release: 7,
            cpu_baseline: "x86_64-v1".to_owned(),
            runtime_profile: "glibc-test".to_owned(),
            compiler_identity: "test".to_owned(),
            descriptor_symbol: "prdb_query_descriptor_v1".to_owned(),
            entry_point_symbol: "prdb_query_main_v1".to_owned(),
            dependencies: vec!["libc.so.6".to_owned(), "libprdb_sql.so.7".to_owned()],
        },
    )
    .unwrap();
    std::fs::write(root.join("manifest.pmf1"), manifest).unwrap();
    let selection = SdkSelection {
        release: 7,
        target: "linux-x86_64".to_owned(),
        cpu_baseline: "x86_64-v1".to_owned(),
        runtime_profile: "glibc-test".to_owned(),
        native_abi: 1,
    };
    assert!(validate_package_selection(&root, &selection).is_ok());
    let mut mismatch = selection.clone();
    mismatch.release = 8;
    assert!(validate_package_selection(&root, &mismatch).is_err());
    std::fs::write(root.join("sql.so"), b"legacy SDK").unwrap();
    assert!(validate_package_selection(&root, &selection).is_err());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn interactive_script_rejects_invalid_transaction_placement_before_dispatch() {
    let valid = parse_interactive_sql("BEGIN; SELECT 1; COMMIT;").unwrap();
    assert!(validate_interactive_script_placement(&valid).is_ok());

    for sql in [
        "COMMIT;",
        "BEGIN; BEGIN; ROLLBACK;",
        "BEGIN; CREATE TABLE t (id INTEGER); ROLLBACK;",
    ] {
        let parsed = parse_interactive_sql(sql).unwrap();
        assert!(
            validate_interactive_script_placement(&parsed).is_err(),
            "{sql}"
        );
    }
}

#[test]
fn interactive_repl_waits_for_statement_or_runas_terminator() {
    assert!(!interactive_input_complete("SELECT 1"));
    assert!(interactive_input_complete("SELECT\n  1;\n"));
    assert!(!interactive_input_complete("RUNAS analyst\nSELECT 1;\n"));
    assert!(interactive_input_complete(
        "RUNAS analyst\nSELECT 1;\nENDRUNAS;\n"
    ));
    assert!(!interactive_input_complete("SELECT 'semi;colon'"));
}

#[test]
fn interactive_repl_dispatches_transaction_and_multiline_runas_in_order() {
    let root = std::env::temp_dir().join(format!(
        "prdb-client-repl-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    let credentials_path = root.join("client.cred");
    write_client_credentials(
        &credentials_path,
        &ClientCredentials {
            token_id: [3; 16],
            token_secret: [4; 32],
        },
    )
    .unwrap();
    let profiles = ClientCredentialProfiles::load(
        "controller",
        &credentials_path,
        &[("analyst".to_owned(), credentials_path.clone())],
    )
    .unwrap();
    let sql = "BEGIN;\nSELECT 1;\nRUNAS analyst\nSELECT 2;\nENDRUNAS;\nROLLBACK;\n";
    let mut input = io::Cursor::new(sql.as_bytes());
    let mut output = Vec::new();
    let mut dispatched = Vec::new();
    let mut dispatch = |statement: &InteractiveStatement<'_>, ordinal| {
        let label = match statement {
            InteractiveStatement::Begin => "BEGIN".to_owned(),
            InteractiveStatement::Query(sql) => format!("QUERY:{sql}"),
            InteractiveStatement::RunAs {
                username,
                statement_count,
                ..
            } => format!("RUNAS:{username}:{statement_count}"),
            InteractiveStatement::Rollback => "ROLLBACK".to_owned(),
            _ => "OTHER".to_owned(),
        };
        dispatched.push((ordinal, label));
        Ok(())
    };
    run_interactive_repl(&mut input, &mut output, false, &profiles, &mut dispatch).unwrap();
    assert_eq!(
        dispatched,
        [
            (0, "BEGIN".to_owned()),
            (1, "QUERY:SELECT 1".to_owned()),
            (2, "RUNAS:analyst:1".to_owned()),
            (3, "ROLLBACK".to_owned()),
        ]
    );
    assert!(output.is_empty());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn sql_batch_result_decoder_handles_fragmented_statement_records() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"SQLB");
    bytes.extend_from_slice(&[1, 1, 0, 3]);
    bytes.extend_from_slice(&0u32.to_be_bytes());
    bytes.extend_from_slice(b"SQLB");
    bytes.extend_from_slice(&[1, 2, 0, 3]);
    bytes.extend_from_slice(&3u32.to_be_bytes());
    bytes.extend_from_slice(b"row");
    bytes.extend_from_slice(b"SQLB");
    bytes.extend_from_slice(&[1, 3, 0, 3]);
    bytes.extend_from_slice(&0u32.to_be_bytes());

    let mut decoder = SqlBatchResultDecoder::default();
    assert!(decoder.push(&bytes[..7]).unwrap().is_empty());
    let records = decoder.push(&bytes[7..]).unwrap();
    assert_eq!(records.len(), 3);
    assert!(matches!(records[0], SqlBatchRecord::Start { ordinal: 3 }));
    assert!(matches!(
        &records[1],
        SqlBatchRecord::Data { ordinal: 3, payload } if payload == b"row"
    ));
    assert!(matches!(
        records[2],
        SqlBatchRecord::Complete { ordinal: 3 }
    ));
    assert!(decoder.finish().is_ok());
}

#[test]
fn token_inventory_json_escapes_untrusted_labels() {
    assert_eq!(
        json_string("laptop \"one\"\\work\n"),
        "\"laptop \\\"one\\\"\\\\work\\n\""
    );
}

#[test]
fn credential_remove_uses_the_private_file_validator_and_removes_file() {
    let root = std::env::temp_dir().join(format!(
        "prdb-client-remove-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("client.cred");
    write_client_credentials(
        &path,
        &ClientCredentials {
            token_id: [1; 16],
            token_secret: [2; 32],
        },
    )
    .unwrap();

    credential_remove(path.clone()).unwrap();
    assert!(!path.exists());
    std::fs::remove_dir(root).unwrap();
}

#[test]
fn invocation_id_parser_requires_canonical_nonzero_hex() {
    assert_eq!(parse_id("0102030405060708090a0b0c0d0e0f10").unwrap()[0], 1);
    assert!(parse_id("0102030405060708090a0b0c0d0e0f1A").is_err());
    assert!(parse_id("00000000000000000000000000000000").is_err());
    assert!(parse_id("1").is_err());
}

#[test]
fn scope_operation_and_name_lists_are_strict() {
    assert_eq!(
        parse_operations("select,insert,delete").unwrap(),
        vec![Operation::Select, Operation::Insert, Operation::Delete]
    );
    assert!(parse_operations("select,select").is_err());
    assert!(parse_operations("usage").is_err());
    assert!(split_names("id,name", "--columns").is_ok());
    assert!(split_names("id,id", "--columns").is_err());
    assert!(split_names("id,,name", "--columns").is_err());
}

#[test]
fn key_constraint_columns_require_ordered_distinct_nonzero_ids() {
    assert_eq!(
        parse_constraint_columns(
            "00000000000000000000000000000001,00000000000000000000000000000002"
        )
        .unwrap(),
        vec![
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2],
        ]
    );
    assert!(parse_constraint_columns("").is_err());
    assert!(
        parse_constraint_columns(
            "00000000000000000000000000000001,00000000000000000000000000000001"
        )
        .is_err()
    );
    assert!(parse_constraint_columns("00000000000000000000000000000000").is_err());
}

#[test]
fn system_privilege_parser_accepts_only_the_closed_wire_enum() {
    assert_eq!(
        parse_system_privilege("manage_users").unwrap(),
        AdminSystemPrivilege::ManageUsers
    );
    assert_eq!(
        parse_system_privilege("manage_roles").unwrap(),
        AdminSystemPrivilege::ManageRoles
    );
    assert_eq!(
        parse_system_privilege("create_schema").unwrap(),
        AdminSystemPrivilege::CreateSchema
    );
    assert!(parse_system_privilege("MANAGE_USERS").is_err());
    assert!(parse_system_privilege("manage_tables").is_err());
}

#[test]
fn object_privilege_parser_accepts_only_catalog_privileges() {
    for name in [
        "usage",
        "select",
        "insert",
        "update",
        "delete",
        "references",
        "create",
        "alter",
        "index",
        "manage_policies",
        "manage_grants",
    ] {
        assert!(parse_object_privilege(name).is_ok(), "{name}");
    }
    assert!(parse_object_privilege("all").is_err());
    assert!(parse_object_privilege("REFERENCES").is_err());
}

#[test]
fn policy_recipient_parser_accepts_only_canonical_user_and_role_ids() {
    assert_eq!(
        parse_policy_recipients(
            "user:000102030405060708090a0b0c0d0e0f,role:101112131415161718191a1b1c1d1e1f"
        )
        .unwrap(),
        vec![
            AdminPolicyRecipient::User([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
            AdminPolicyRecipient::Role([
                16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
            ]),
        ]
    );
    assert!(parse_policy_recipients("").is_err());
    assert!(parse_policy_recipients("group:000102030405060708090a0b0c0d0e0f").is_err());
    assert!(parse_policy_recipients("user:000102030405060708090a0b0c0d0e0A").is_err());
    assert!(parse_policy_recipients("user:00000000000000000000000000000000").is_err());
}

#[test]
fn check_expression_hex_parser_is_canonical_and_dependencies_may_be_empty() {
    assert_eq!(
        parse_lowercase_hex_bytes("0102ff").unwrap(),
        vec![1, 2, 255]
    );
    assert!(parse_lowercase_hex_bytes("").is_err());
    assert!(parse_lowercase_hex_bytes("0").is_err());
    assert!(parse_lowercase_hex_bytes("0A").is_err());
    assert!(parse_lowercase_hex_bytes("0g").is_err());
    assert!(parse_optional_constraint_columns("").unwrap().is_empty());
    assert_eq!(
        parse_optional_constraint_columns("00000000000000000000000000000001").unwrap(),
        vec![[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]]
    );
}

#[test]
fn scope_file_is_private_and_never_overwritten() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory =
        std::env::temp_dir().join(format!("prdb-client-scope-{}-{nonce}", std::process::id()));
    fs::create_dir(&directory).unwrap();
    let path = directory.join("requested.psc1");
    write_scope_file(&path, b"PSC1test").unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"PSC1test");
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert!(write_scope_file(&path, b"replacement").is_err());
    assert_eq!(fs::read(&path).unwrap(), b"PSC1test");
    fs::remove_file(path).unwrap();
    fs::remove_dir(directory).unwrap();
}

#[test]
fn parameters_encode_reads_json_and_writes_raw_psp1_bytes() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "prdb-client-parameters-encode-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir(&directory).unwrap();
    let input_path = directory.join("input.json");
    let output_path = directory.join("output.psp1");
    fs::write(
        &input_path,
        br#"{"format":"PDB-SQL-PARAMETERS-1","values":[{"type":"integer","value":"1"}]}"#,
    )
    .unwrap();

    let mut fields = HashMap::new();
    fields.insert(
        "--input".to_owned(),
        std::ffi::OsString::from(input_path.as_os_str()),
    );
    fields.insert(
        "--output".to_owned(),
        std::ffi::OsString::from(output_path.as_os_str()),
    );
    parameters_encode(&mut fields).unwrap();

    let encoded = fs::read(&output_path).unwrap();
    assert_eq!(&encoded[..4], b"PSP1");
    assert_eq!(
        prdb_engine::client_sql::encode_parameter_file(
            br#"{"format":"PDB-SQL-PARAMETERS-1","values":[{"type":"integer","value":"1"}]}"#
        )
        .unwrap()
        .as_slice(),
        encoded.as_slice()
    );

    // A second run against the same --output must fail closed rather than
    // silently overwrite a prior encoding.
    let mut fields = HashMap::new();
    fields.insert(
        "--input".to_owned(),
        std::ffi::OsString::from(input_path.as_os_str()),
    );
    fields.insert(
        "--output".to_owned(),
        std::ffi::OsString::from(output_path.as_os_str()),
    );
    assert!(parameters_encode(&mut fields).is_err());

    fs::remove_file(&input_path).unwrap();
    fs::remove_file(&output_path).unwrap();
    fs::remove_dir(directory).unwrap();
}

#[test]
fn token_expiration_requires_positive_bounded_lifetime() {
    assert!(expiration_time(0).is_err());
    let expiry = expiration_time(60).unwrap();
    let now: i64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
        .try_into()
        .unwrap();
    assert!(expiry > now);
    assert!(expiration_time(u64::MAX).is_err());
}

#[test]
fn login_switch_parser_accepts_only_canonical_booleans() {
    assert!(parse_boolean("true", "--enabled").unwrap());
    assert!(!parse_boolean("false", "--enabled").unwrap());
    assert!(parse_boolean("yes", "--enabled").is_err());
    assert!(parse_boolean("TRUE", "--enabled").is_err());
}

#[test]
fn table_column_parser_validates_types_nullability_and_names() {
    let columns = parse_column_definitions(
        "id:uuid:not-null:identity;name:varchar(80):nullable;amount:decimal(12,2):nullable",
    )
    .unwrap();
    assert_eq!(columns.len(), 3);
    assert!(columns[0].generated_uuid_identity);
    assert_eq!(
        columns[1].scalar_type,
        ScalarType::Varchar { max_chars: 80 }
    );
    assert_eq!(
        columns[2].scalar_type,
        ScalarType::Decimal {
            precision: 12,
            scale: 2
        }
    );
    assert!(parse_column_definitions("id:integer:nullable;id:text:nullable").is_err());
    assert!(parse_column_definitions("id:integer:not-null").is_err());
    assert!(parse_column_definitions("id:text:nullable:identity").is_err());
    assert!(parse_column_definitions("id:uuid:nullable:identity").is_err());
    assert!(parse_column_definitions("id:decimal(2,3):nullable").is_err());
    assert!(parse_column_definitions("id:varchar(0):nullable").is_err());
    assert!(parse_added_column("extra:integer:nullable").is_ok());
    assert!(parse_added_column("id:integer:not-null:identity").is_err());
    assert!(parse_added_column("one:text:nullable;two:text:nullable").is_err());
    assert_eq!(
        parse_column_default(
            "-12.30",
            ScalarType::Decimal {
                precision: 5,
                scale: 2
            }
        )
        .unwrap(),
        AdminColumnDefault::Literal(ScalarValue::Decimal(-1230))
    );
    assert_eq!(
        parse_column_default("current-principal-id", ScalarType::Uuid).unwrap(),
        AdminColumnDefault::CurrentPrincipalId
    );
    assert_eq!(
        parse_column_default("hex:00ff", ScalarType::Bytea).unwrap(),
        AdminColumnDefault::Literal(ScalarValue::Bytea(vec![0, 255]))
    );
    assert!(
        parse_column_default(
            "1.234",
            ScalarType::Decimal {
                precision: 4,
                scale: 2
            }
        )
        .is_err()
    );
    assert!(parse_column_default("current-principal-id", ScalarType::Text).is_err());
    assert_eq!(
        parse_id_list("00000000000000000000000000000001", "--columns").unwrap(),
        vec![[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]]
    );
    assert!(
        parse_id_list(
            "00000000000000000000000000000001,00000000000000000000000000000001",
            "--columns"
        )
        .is_err()
    );
}

#[test]
fn discovery_type_formatter_preserves_declared_parameters() {
    assert_eq!(format_scalar_type(&ScalarType::Boolean), "boolean");
    assert_eq!(format_scalar_type(&ScalarType::SmallInt), "smallint");
    assert_eq!(format_scalar_type(&ScalarType::Integer), "integer");
    assert_eq!(format_scalar_type(&ScalarType::BigInt), "bigint");
    assert_eq!(
        format_scalar_type(&ScalarType::Decimal {
            precision: 18,
            scale: 4,
        }),
        "decimal(18,4)"
    );
    assert_eq!(format_scalar_type(&ScalarType::Text), "text");
    assert_eq!(
        format_scalar_type(&ScalarType::Varchar { max_chars: 240 }),
        "varchar(240)"
    );
    assert_eq!(format_scalar_type(&ScalarType::Bytea), "bytea");
    assert_eq!(format_scalar_type(&ScalarType::Uuid), "uuid");
    assert_eq!(format_scalar_type(&ScalarType::Date), "date");
    assert_eq!(format_scalar_type(&ScalarType::TimestampTz), "timestamptz");
}

#[test]
fn table_owner_arguments_require_canonical_nonzero_ids_and_no_extras() {
    let mut fields = HashMap::from([
        (
            "--table-id".to_owned(),
            "000102030405060708090a0b0c0d0e0f".into(),
        ),
        (
            "--owner-id".to_owned(),
            "101112131415161718191a1b1c1d1e1f".into(),
        ),
    ]);
    assert_eq!(
        take_table_owner_args(&mut fields).unwrap(),
        (
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            [
                16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31
            ],
        )
    );
    assert!(fields.is_empty());

    for owner in [
        "00000000000000000000000000000000",
        "000102030405060708090a0b0c0d0e0A",
        "000102030405060708090a0b0c0d0e0",
    ] {
        let mut fields = HashMap::from([
            (
                "--table-id".to_owned(),
                "000102030405060708090a0b0c0d0e0f".into(),
            ),
            ("--owner-id".to_owned(), owner.into()),
        ]);
        assert!(take_table_owner_args(&mut fields).is_err());
    }

    let mut fields = HashMap::from([
        (
            "--table-id".to_owned(),
            "000102030405060708090a0b0c0d0e0f".into(),
        ),
        (
            "--owner-id".to_owned(),
            "101112131415161718191a1b1c1d1e1f".into(),
        ),
        ("--unexpected".to_owned(), "value".into()),
    ]);
    assert!(take_table_owner_args(&mut fields).is_err());
}

#[test]
fn schema_owner_arguments_require_canonical_nonzero_ids_and_no_extras() {
    let mut fields = HashMap::from([
        (
            "--schema-id".to_owned(),
            "000102030405060708090a0b0c0d0e0f".into(),
        ),
        (
            "--owner-id".to_owned(),
            "101112131415161718191a1b1c1d1e1f".into(),
        ),
    ]);
    assert_eq!(
        take_schema_owner_args(&mut fields).unwrap(),
        (
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            [
                16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31
            ],
        )
    );
    assert!(fields.is_empty());

    for owner in [
        "00000000000000000000000000000000",
        "000102030405060708090a0b0c0d0e0A",
        "000102030405060708090a0b0c0d0e0",
    ] {
        let mut fields = HashMap::from([
            (
                "--schema-id".to_owned(),
                "000102030405060708090a0b0c0d0e0f".into(),
            ),
            ("--owner-id".to_owned(), owner.into()),
        ]);
        assert!(take_schema_owner_args(&mut fields).is_err());
    }

    let mut fields = HashMap::from([
        (
            "--schema-id".to_owned(),
            "000102030405060708090a0b0c0d0e0f".into(),
        ),
        (
            "--owner-id".to_owned(),
            "101112131415161718191a1b1c1d1e1f".into(),
        ),
        ("--unexpected".to_owned(), "value".into()),
    ]);
    assert!(take_schema_owner_args(&mut fields).is_err());
}
