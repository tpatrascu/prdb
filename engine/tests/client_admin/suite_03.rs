use prdb_engine::client_config::{
    CONFIG_FORMAT, ClientConfig, ClientCredential as ConfigCredential, ClientInstance,
    SdkSelection, encode_token_secret,
};
use prdb_engine::client_protocol::ClientFrameDecoder;
use prdb_engine::client_submit::{AdminClient, AdminColumnDefault};
use prdb_engine::tuple::{ScalarType, ScalarValue};
use sha2::{Digest, Sha256};
use std::fs;
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;
use std::time::Duration;

use super::helpers::*;

pub(super) fn admin_client_changes_table_owner_nullability_and_defaults() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let table_id = "a7".repeat(16);
        let column_id = "b7".repeat(16);
        let cases = [
            (
                "tables.set_owner",
                format!(
                    r#"{{"table_id":"{table_id}","owner_id":"{}"}}"#,
                    "c7".repeat(16)
                ),
                [0xa1; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","table_id":"{table_id}","owner_id":"{}","commit_sequence":"81"}}"#,
                    hex(&[0xa1; 16]),
                    "c7".repeat(16)
                ),
            ),
            (
                "tables.alter_column_nullability",
                format!(
                    r#"{{"table_id":"{table_id}","column_id":"{column_id}","nullable":"true"}}"#
                ),
                [0xa2; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","table_id":"{table_id}","column_id":"{column_id}","nullable":true,"commit_sequence":"82"}}"#,
                    hex(&[0xa2; 16])
                ),
            ),
            (
                "tables.set_column_default",
                format!(
                    r#"{{"table_id":"{table_id}","column_id":"{column_id}","type":"bigint","default":{{"kind":"literal","value":"5"}} }}"#
                ),
                [0xa3; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","table_id":"{table_id}","column_id":"{column_id}","commit_sequence":"83"}}"#,
                    hex(&[0xa3; 16])
                ),
            ),
            (
                "tables.drop_column_default",
                format!(r#"{{"table_id":"{table_id}","column_id":"{column_id}"}}"#),
                [0xa4; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","table_id":"{table_id}","column_id":"{column_id}","commit_sequence":"84"}}"#,
                    hex(&[0xa4; 16])
                ),
            ),
        ];
        for (operation, arguments, operation_id, result) in cases {
            let prepare = read_frame(&mut stream, &mut decoder);
            let payload = String::from_utf8(prepare.payload.clone()).unwrap();
            assert!(payload.contains(&format!("\"operation\":\"{operation}\"")));
            assert!(
                payload.contains(&format!("\"arguments\":{arguments}")),
                "{payload}"
            );
            respond(
                &mut stream,
                &prepare,
                format!(
                    r#"{{"operation_id":"{}","state":"prepared"}}"#,
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
    client.set_table_owner([0xa7; 16], [0xc7; 16]).unwrap();
    client
        .alter_column_nullability([0xa7; 16], [0xb7; 16], true)
        .unwrap();
    client
        .set_column_default(
            [0xa7; 16],
            [0xb7; 16],
            ScalarType::BigInt,
            AdminColumnDefault::Literal(ScalarValue::BigInt(5)),
        )
        .unwrap();
    client.drop_column_default([0xa7; 16], [0xb7; 16]).unwrap();
    server.join().unwrap();
}

pub(super) fn admin_client_sends_typed_constraint_and_foreign_key_operations() {
    use prdb_engine::authorization::Expr;

    let expression = Expr::Equal(
        Box::new(Expr::Field(0)),
        Box::new(Expr::Literal(Some(ScalarValue::BigInt(0)))),
    );
    let expression_hex = prdb_engine::expression::encode_expression(&expression)
        .unwrap()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let table_id = [0x11; 16];
    let column_ids = [[0x21; 16], [0x22; 16]];
    let constraint_id = [0x31; 16];
    let index_id = [0x32; 16];
    let expression_id = [0x33; 16];
    let foreign_key_id = [0x41; 16];
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);

        let cases = [
            (
                "constraints.create_unique",
                format!(
                    r#"{{"table_id":"{}","column_ids":["{}","{}"]}}"#,
                    hex(&table_id),
                    hex(&column_ids[0]),
                    hex(&column_ids[1])
                ),
                [0x51; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","constraint_id":"{}","index_id":"{}","commit_sequence":"91"}}"#,
                    hex(&[0x51; 16]),
                    hex(&constraint_id),
                    hex(&index_id)
                ),
            ),
            (
                "constraints.create_primary_key",
                format!(
                    r#"{{"table_id":"{}","column_ids":["{}"]}}"#,
                    hex(&table_id),
                    hex(&column_ids[0])
                ),
                [0x52; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","constraint_id":"{}","index_id":"{}","commit_sequence":"92"}}"#,
                    hex(&[0x52; 16]),
                    hex(&[0x34; 16]),
                    hex(&[0x35; 16])
                ),
            ),
            (
                "constraints.create_check",
                format!(
                    r#"{{"table_id":"{}","expression_eir1":"{}","dependency_column_ids":[]}}"#,
                    hex(&table_id),
                    expression_hex
                ),
                [0x53; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","constraint_id":"{}","expression_id":"{}","commit_sequence":"93"}}"#,
                    hex(&[0x53; 16]),
                    hex(&[0x36; 16]),
                    hex(&expression_id)
                ),
            ),
            (
                "constraints.drop_primary_key",
                format!(r#"{{"constraint_id":"{}"}}"#, hex(&constraint_id)),
                [0x54; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","constraint_id":"{}","commit_sequence":"94"}}"#,
                    hex(&[0x54; 16]),
                    hex(&constraint_id)
                ),
            ),
            (
                "constraints.drop_unique",
                format!(r#"{{"constraint_id":"{}"}}"#, hex(&[0x34; 16])),
                [0x55; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","constraint_id":"{}","commit_sequence":"95"}}"#,
                    hex(&[0x55; 16]),
                    hex(&[0x34; 16])
                ),
            ),
            (
                "constraints.drop_check",
                format!(r#"{{"constraint_id":"{}"}}"#, hex(&[0x36; 16])),
                [0x56; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","constraint_id":"{}","commit_sequence":"96"}}"#,
                    hex(&[0x56; 16]),
                    hex(&[0x36; 16])
                ),
            ),
            (
                "foreign_keys.create",
                format!(
                    r#"{{"source_table_id":"{}","source_column_ids":["{}"],"target_constraint_id":"{}"}}"#,
                    hex(&table_id),
                    hex(&column_ids[0]),
                    hex(&constraint_id)
                ),
                [0x57; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","foreign_key_id":"{}","commit_sequence":"97"}}"#,
                    hex(&[0x57; 16]),
                    hex(&foreign_key_id)
                ),
            ),
            (
                "foreign_keys.drop",
                format!(r#"{{"foreign_key_id":"{}"}}"#, hex(&foreign_key_id)),
                [0x58; 16],
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","foreign_key_id":"{}","commit_sequence":"98"}}"#,
                    hex(&[0x58; 16]),
                    hex(&foreign_key_id)
                ),
            ),
        ];
        for (operation, arguments, operation_id, result) in cases {
            let prepare = read_frame(&mut stream, &mut decoder);
            let payload = String::from_utf8(prepare.payload.clone()).unwrap();
            assert!(
                payload.contains(&format!(r#""operation":"{operation}""#)),
                "{payload}"
            );
            assert!(
                payload.contains(&format!(r#""arguments":{arguments}"#)),
                "{payload}"
            );
            respond(
                &mut stream,
                &prepare,
                format!(
                    r#"{{"operation_id":"{}","state":"prepared"}}"#,
                    hex(&operation_id)
                )
                .as_bytes(),
            );
            let start = read_frame(&mut stream, &mut decoder);
            respond(&mut stream, &start, result.as_bytes());
        }
    });
    let mut secret = [7; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [8; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    let unique = client
        .create_unique_constraint(table_id, &column_ids)
        .unwrap();
    assert_eq!(unique.constraint_id, constraint_id);
    assert_eq!(unique.index_id, index_id);
    let primary_key = client
        .create_primary_key(table_id, &column_ids[..1])
        .unwrap();
    assert_eq!(primary_key.constraint_id, [0x34; 16]);
    assert_eq!(primary_key.index_id, [0x35; 16]);
    let check = client
        .create_check_constraint(table_id, &expression, &[])
        .unwrap();
    assert_eq!(check.constraint_id, [0x36; 16]);
    assert_eq!(check.expression_id, expression_id);
    client.drop_primary_key(constraint_id).unwrap();
    client.drop_unique_constraint([0x34; 16]).unwrap();
    client.drop_check_constraint([0x36; 16]).unwrap();
    let foreign_key = client
        .create_foreign_key(table_id, &column_ids[..1], constraint_id)
        .unwrap();
    assert_eq!(foreign_key.foreign_key_id, foreign_key_id);
    client.drop_foreign_key(foreign_key_id).unwrap();
    server.join().unwrap();
}

pub(super) fn configured_sdk_library_install_and_remove_commands_use_typed_gateway_operations() {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!("prdb-sdk-admin-cli-{stamp}"));
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let config_path = root.join("client.json");
    let metadata_path = root.join("sdk-release.json");
    let library_path = root.join("libprdb_sql.so.7");
    let metadata = b"repository metadata fixture";
    let library = b"repository library fixture";
    fs::write(&metadata_path, metadata).unwrap();
    fs::write(&library_path, library).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    ClientConfig {
        format: CONFIG_FORMAT.to_owned(),
        default_instance: Some("test".to_owned()),
        client_tools: None,
        instances: vec![ClientInstance {
            name: "test".to_owned(),
            address: address.ip().to_string(),
            port: address.port(),
            sdk: SdkSelection {
                release: 7,
                target: "linux-x86_64".to_owned(),
                cpu_baseline: "x86_64-v1".to_owned(),
                runtime_profile: "test-runtime".to_owned(),
                native_abi: 5,
            },
            default_credential: "admin".to_owned(),
            credentials: vec![ConfigCredential {
                name: "admin".to_owned(),
                username: "admin".to_owned(),
                token_id: "07".repeat(16),
                token_secret: encode_token_secret(&[9; 32]),
            }],
        }],
    }
    .save_atomic(&config_path)
    .unwrap();

    let library_digest: [u8; 32] = Sha256::digest(library).into();
    let library_digest_text = library_digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let begin = read_frame(&mut stream, &mut decoder);
        assert_eq!(begin.kind, 10);
        let operation_id = operation_id_from_payload(&begin.payload);
        let operation_id_text = hex(&operation_id);
        assert!(
            std::str::from_utf8(&begin.payload)
                .unwrap()
                .contains(r#""operation":"sdk_libraries.install""#)
        );
        respond(
            &mut stream,
            &begin,
            format!(r#"{{"operation_id":"{operation_id_text}","next_offset":"0"}}"#).as_bytes(),
        );
        let mut transferred = Vec::new();
        let expected = [metadata.as_slice(), library.as_slice()].concat();
        while transferred.len() < expected.len() {
            let chunk = read_frame(&mut stream, &mut decoder);
            assert_eq!(chunk.kind, 19);
            assert_eq!(&chunk.payload[..16], &operation_id);
            assert_eq!(
                u64::from_le_bytes(chunk.payload[16..24].try_into().unwrap()),
                transferred.len() as u64
            );
            transferred.extend_from_slice(&chunk.payload[24..]);
            respond(
                &mut stream,
                &chunk,
                format!(r#"{{"next_offset":"{}"}}"#, transferred.len()).as_bytes(),
            );
        }
        assert_eq!(transferred, expected);
        let finish = read_frame(&mut stream, &mut decoder);
        assert!(
            std::str::from_utf8(&finish.payload)
                .unwrap()
                .contains("sdk_libraries.install.finish")
        );
        respond(
            &mut stream,
            &finish,
            format!(
                r#"{{"operation_id":"{operation_id_text}","release":7,"sha256":"{library_digest_text}","library_bytes":{}}}"#,
                library.len()
            )
            .as_bytes(),
        );
        let list = read_frame(&mut stream, &mut decoder);
        respond(
            &mut stream,
            &list,
            format!(
                r#"{{"sdk_libraries":[{{"release":7,"native_abis":[5],"target":"linux-x86_64","cpu_baseline":"x86_64-v1","runtime_profile":"test-runtime","soname":"libprdb_sql.so.7","sha256":"{library_digest_text}","needed":["libc.so.6"],"library_bytes":{}}}],"next_cursor":null}}"#,
                library.len()
            )
            .as_bytes(),
        );

        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let remove = read_frame(&mut stream, &mut decoder);
        assert_eq!(remove.kind, 10);
        let remove_operation = operation_id_from_payload(&remove.payload);
        let remove_operation_text = hex(&remove_operation);
        assert!(
            std::str::from_utf8(&remove.payload)
                .unwrap()
                .contains(r#""operation":"sdk_libraries.remove""#)
        );
        respond(
            &mut stream,
            &remove,
            format!(
                r#"{{"operation_id":"{remove_operation_text}","release":7,"removed_bytes":{}}}"#,
                library.len()
            )
            .as_bytes(),
        );
    });

    let client_binary = std::env::var_os("CARGO_BIN_EXE_prdb-client").unwrap();
    let install = Command::new(&client_binary)
        .arg("sdk-library")
        .arg("install")
        .arg("--config")
        .arg(&config_path)
        .arg("--metadata")
        .arg(&metadata_path)
        .arg("--library")
        .arg(&library_path)
        .output()
        .unwrap();
    assert!(
        install.status.success(),
        "{}",
        String::from_utf8_lossy(&install.stderr)
    );
    let remove = Command::new(&client_binary)
        .arg("sdk-library")
        .arg("remove")
        .arg("--config")
        .arg(&config_path)
        .arg("--release")
        .arg("7")
        .output()
        .unwrap();
    assert!(
        remove.status.success(),
        "{}",
        String::from_utf8_lossy(&remove.stderr)
    );
    server.join().unwrap();
    fs::remove_dir_all(root).unwrap();
}
