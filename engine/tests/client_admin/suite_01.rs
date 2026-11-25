use prdb_engine::client_protocol::{
    ClientAdminCommand, ClientControlError, ClientFrameDecoder, decode_client_admin_command,
};
use prdb_engine::client_submit::{
    AdminClient, AdminSystemPrivilege, CancellationResult, InvocationStatus, SubmissionClient,
    TerminalOutcome,
};
use sha2::{Digest, Sha256};
use std::io::Read;
use std::net::TcpListener;
use std::time::Duration;

use super::helpers::*;

pub(super) fn admin_create_table_decoder_validates_constraints_and_check_expressions() {
    let valid = br#"{"phase":"prepare","operation_version":1,"operation":"tables.create","arguments":{"schema_id":"11111111111111111111111111111111","name":"events","columns":[{"name":"id","type":"uuid","nullable":false,"generated_uuid_identity":true}],"primary_key_columns":["id"],"unique_constraints":[["id"]],"check_expressions_eir1":["4549523101000000020000"]}}"#;
    assert!(matches!(
        decode_client_admin_command(valid),
        Ok(ClientAdminCommand::PrepareCreateTable {
            primary_key_columns: Some(names),
            unique_constraints,
            check_expressions,
            ..
        }) if names == ["id"] && unique_constraints == [vec!["id"]] && check_expressions == [prdb_engine::authorization::Expr::Field(0)]
    ));

    let nullable = br#"{"phase":"prepare","operation_version":1,"operation":"tables.create","arguments":{"schema_id":"11111111111111111111111111111111","name":"events","columns":[{"name":"id","type":"uuid","nullable":true,"generated_uuid_identity":false}],"primary_key_columns":["id"]}}"#;
    assert_eq!(
        decode_client_admin_command(nullable),
        Err(ClientControlError::InvalidPrepare)
    );
    let duplicate = br#"{"phase":"prepare","operation_version":1,"operation":"tables.create","arguments":{"schema_id":"11111111111111111111111111111111","name":"events","columns":[{"name":"id","type":"uuid","nullable":false,"generated_uuid_identity":true}],"primary_key_columns":["id","id"]}}"#;
    assert_eq!(
        decode_client_admin_command(duplicate),
        Err(ClientControlError::InvalidPrepare)
    );
    let invalid_unique = br#"{"phase":"prepare","operation_version":1,"operation":"tables.create","arguments":{"schema_id":"11111111111111111111111111111111","name":"events","columns":[{"name":"id","type":"uuid","nullable":false,"generated_uuid_identity":true}],"unique_constraints":[["unknown"]]}}"#;
    assert_eq!(
        decode_client_admin_command(invalid_unique),
        Err(ClientControlError::InvalidPrepare)
    );
}

pub(super) fn admin_create_table_v2_decoder_accepts_same_statement_foreign_key_target() {
    let self_reference = br#"{"phase":"prepare","operation_version":2,"operation":"tables.create","arguments":{"schema_id":"11111111111111111111111111111111","name":"nodes","columns":[{"name":"id","type":"integer","nullable":false,"generated_uuid_identity":false},{"name":"parent_id","type":"integer","nullable":true,"generated_uuid_identity":false}],"primary_key_columns":["id"],"foreign_key_source_columns":[["parent_id"]],"foreign_key_target_constraint_ids":["00000000000000000000000000000000"],"foreign_key_target_columns":[["id"]]}}"#;
    assert!(matches!(
        decode_client_admin_command(self_reference),
        Ok(ClientAdminCommand::PrepareCreateTable { foreign_keys, .. })
            if foreign_keys == [prdb_engine::storage_manager::CreateTableForeignKeyDefinition {
                source_columns: vec!["parent_id".to_owned()],
                target_constraint_id: [0; 16],
                target_columns: Some(vec!["id".to_owned()]),
            }]
    ));
}

pub(super) fn admin_client_lists_bounded_secret_free_token_pages() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let request = read_frame(&mut stream, &mut decoder);
        assert_eq!(request.kind, 10);
        assert_eq!(
            std::str::from_utf8(&request.payload).unwrap(),
            r#"{"phase":"read","operation_version":1,"operation":"tokens.list","arguments":{"user_id":"02020202020202020202020202020202","after_token_id":"03030303030303030303030303030303"}}"#
        );
        let mut first_page = String::from("{\"tokens\":[");
        for value in 4u8..=67 {
            if value != 4 {
                first_page.push(',');
            }
            let token_id = format!("{value:02x}").repeat(16);
            first_page.push_str(&format!(
                "{{\"token_id\":\"{token_id}\",\"label\":\"work laptop\",\"issued_at_us\":100,\"expires_at_us\":200,\"revoked\":false,\"revoked_at_us\":null}}"
            ));
        }
        first_page.push_str("],\"next_after_token_id\":\"43434343434343434343434343434343\"}");
        respond(&mut stream, &request, first_page.as_bytes());

        let next_request = read_frame(&mut stream, &mut decoder);
        assert_eq!(
            std::str::from_utf8(&next_request.payload).unwrap(),
            r#"{"phase":"read","operation_version":1,"operation":"tokens.list","arguments":{"user_id":"02020202020202020202020202020202","after_token_id":"43434343434343434343434343434343"}}"#
        );
        respond(
            &mut stream,
            &next_request,
            br#"{"tokens":[{"token_id":"44444444444444444444444444444444","label":"renewed","issued_at_us":300,"expires_at_us":400,"revoked":false,"revoked_at_us":null}],"next_after_token_id":null}"#,
        );
    });
    let mut secret = [7; 32];
    let mut client = AdminClient::connect_authenticated(address, [1; 16], &mut secret).unwrap();
    let page = client.list_tokens_page([2; 16], Some([3; 16])).unwrap();
    assert_eq!(page.next_after_token_id, Some([0x43; 16]));
    assert_eq!(page.tokens.len(), 64);
    assert_eq!(page.tokens[0].token_id, [4; 16]);
    assert_eq!(page.tokens[0].label, "work laptop");
    assert_eq!(page.tokens[0].issued_at_us, 100);
    assert_eq!(page.tokens[0].expires_at_us, 200);
    assert!(!page.tokens[0].revoked);
    assert_eq!(page.tokens[0].revoked_at_us, None);
    let next = client
        .list_tokens_page([2; 16], page.next_after_token_id)
        .unwrap();
    assert_eq!(next.tokens.len(), 1);
    assert_eq!(next.tokens[0].token_id, [0x44; 16]);
    assert_eq!(next.next_after_token_id, None);
    server.join().unwrap();
}

pub(super) fn admin_client_decodes_bounded_audit_pages_and_cursor() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let request = read_frame(&mut stream, &mut decoder);
        assert_eq!(
            std::str::from_utf8(&request.payload).unwrap(),
            r#"{"phase":"read","operation_version":1,"operation":"audit.events.list","arguments":{"limit":"1"}}"#
        );
        respond(
            &mut stream,
            &request,
            br#"{"events":[{"event_id":"05050505050505050505050505050505","actor_id":null,"correlation_id":"06060606060606060606060606060606","occurred_at_us":123,"action":4,"result":2,"target_ids":["07070707070707070707070707070707"]}],"next_cursor":"010101010101010101010101010101010105050505050505050505050505050505"}"#,
        );
        let request = read_frame(&mut stream, &mut decoder);
        assert_eq!(
            std::str::from_utf8(&request.payload).unwrap(),
            r#"{"phase":"read","operation_version":1,"operation":"audit.events.list","arguments":{"limit":"1","cursor":"010101010101010101010101010101010105050505050505050505050505050505"}}"#
        );
        respond(
            &mut stream,
            &request,
            br#"{"events":[{"event_id":"08080808080808080808080808080808","actor_id":"01010101010101010101010101010101","correlation_id":"09090909090909090909090909090909","occurred_at_us":124,"action":1,"result":1,"target_ids":[]}],"next_cursor":null}"#,
        );
    });
    let mut secret = [7; 32];
    let mut client = AdminClient::connect_authenticated(address, [1; 16], &mut secret).unwrap();
    let first = client.list_audit_events_page(1, None).unwrap();
    assert_eq!(first.events[0].event_id, [5; 16]);
    assert_eq!(first.events[0].actor_id, None);
    assert_eq!(first.events[0].action, 4);
    assert_eq!(first.events[0].result, 2);
    assert_eq!(first.events[0].target_ids, vec![[7; 16]]);
    let second = client
        .list_audit_events_page(1, first.next_cursor.as_deref())
        .unwrap();
    assert_eq!(second.events[0].event_id, [8; 16]);
    assert_eq!(second.events[0].actor_id, Some([1; 16]));
    assert_eq!(second.next_cursor, None);
    server.join().unwrap();
}

pub(super) fn admin_client_lists_administratively_visible_schema_pages() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let request = read_frame(&mut stream, &mut decoder);
        assert_eq!(request.kind, 10);
        assert_eq!(
            std::str::from_utf8(&request.payload).unwrap(),
            r#"{"phase":"read","operation_version":1,"operation":"metadata.schemas.list","arguments":{"limit":"1"}}"#
        );
        respond(
            &mut stream,
            &request,
            br#"{"schemas":[{"schema_id":"01010101010101010101010101010101","name":"owned_schema"}],"next_cursor":"d30102030405060708090a0b0c0d0e0f"}"#,
        );
        let request = read_frame(&mut stream, &mut decoder);
        assert_eq!(
            std::str::from_utf8(&request.payload).unwrap(),
            r#"{"phase":"read","operation_version":1,"operation":"metadata.schemas.list","arguments":{"limit":"1","cursor":"d30102030405060708090a0b0c0d0e0f"}}"#
        );
        respond(
            &mut stream,
            &request,
            br#"{"schemas":[{"schema_id":"02020202020202020202020202020202","name":"information_schema"}],"next_cursor":null}"#,
        );
    });

    let mut secret = [7; 32];
    let mut client = AdminClient::connect_authenticated(address, [1; 16], &mut secret).unwrap();
    let first = client.list_schemas_page(1, None).unwrap();
    assert_eq!(first.records.len(), 1);
    assert_eq!(first.records[0].schema_id, [1; 16]);
    assert_eq!(first.records[0].name, "owned_schema");
    let second = client
        .list_schemas_page(1, first.next_cursor.as_deref())
        .unwrap();
    assert_eq!(second.records[0].schema_id, [2; 16]);
    assert_eq!(second.records[0].name, "information_schema");
    assert_eq!(second.next_cursor, None);
    server.join().unwrap();
}

pub(super) fn admin_client_lists_sdk_libraries_with_compatibility_metadata() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let request = read_frame(&mut stream, &mut decoder);
        assert_eq!(request.kind, 10);
        assert_eq!(
            std::str::from_utf8(&request.payload).unwrap(),
            r#"{"phase":"read","operation_version":1,"operation":"sdk_libraries.list","arguments":{"limit":"64"}}"#
        );
        respond(
            &mut stream,
            &request,
            br#"{"sdk_libraries":[{"release":7,"native_abis":[5],"target":"linux-x86_64","cpu_baseline":"x86_64-v1","runtime_profile":"nixos-glibc-2.42","soname":"libprdb_sql.so.7","sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","needed":["libc.so.6"],"library_bytes":4096}],"next_cursor":null}"#,
        );
    });

    let mut secret = [7; 32];
    let mut client = AdminClient::connect_authenticated(address, [1; 16], &mut secret).unwrap();
    let releases = client.list_sdk_libraries().unwrap();
    assert_eq!(releases.len(), 1);
    assert_eq!(releases[0].sdk_release, 7);
    assert_eq!(releases[0].native_abis, [5]);
    assert_eq!(releases[0].target, "linux-x86_64");
    assert_eq!(releases[0].soname, "libprdb_sql.so.7");
    assert_eq!(releases[0].sha256, [0xaa; 32]);
    assert_eq!(releases[0].needed, ["libc.so.6"]);
    assert_eq!(releases[0].library_bytes, 4096);
    server.join().unwrap();
}

pub(super) fn admin_client_follows_opaque_sdk_library_page_cursors() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let cursor = "d30102030405060708090a0b0c0d0e0f";
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let request = read_frame(&mut stream, &mut decoder);
        assert_eq!(
            std::str::from_utf8(&request.payload).unwrap(),
            r#"{"phase":"read","operation_version":1,"operation":"sdk_libraries.list","arguments":{"limit":"1"}}"#
        );
        respond(
            &mut stream,
            &request,
            format!(
                r#"{{"sdk_libraries":[{{"release":7,"native_abis":[5],"target":"linux-x86_64","cpu_baseline":"x86_64-v1","runtime_profile":"test-runtime","soname":"libprdb_sql.so.7","sha256":"{}","needed":["libc.so.6"],"library_bytes":4096}}],"next_cursor":"{cursor}"}}"#,
                "aa".repeat(32)
            )
            .as_bytes(),
        );
        let request = read_frame(&mut stream, &mut decoder);
        assert_eq!(
            std::str::from_utf8(&request.payload).unwrap(),
            format!(
                r#"{{"phase":"read","operation_version":1,"operation":"sdk_libraries.list","arguments":{{"limit":"1","cursor":"{cursor}"}}}}"#
            )
        );
        respond(
            &mut stream,
            &request,
            format!(
                r#"{{"sdk_libraries":[{{"release":8,"native_abis":[5],"target":"linux-x86_64","cpu_baseline":"x86_64-v1","runtime_profile":"test-runtime","soname":"libprdb_sql.so.8","sha256":"{}","needed":["libc.so.6"],"library_bytes":4096}}],"next_cursor":null}}"#,
                "bb".repeat(32)
            )
            .as_bytes(),
        );
    });

    let mut secret = [7; 32];
    let mut client = AdminClient::connect_authenticated(address, [1; 16], &mut secret).unwrap();
    let first = client.list_sdk_libraries_page(1, None).unwrap();
    assert_eq!(first.records[0].sdk_release, 7);
    let second = client
        .list_sdk_libraries_page(1, first.next_cursor.as_deref())
        .unwrap();
    assert_eq!(second.records[0].sdk_release, 8);
    assert_eq!(second.next_cursor, None);
    server.join().unwrap();
}

pub(super) fn admin_client_sends_typed_sdk_library_remove_request() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let request = read_frame(&mut stream, &mut decoder);
        assert_eq!(request.kind, 10);
        let operation_id = operation_id_from_payload(&request.payload);
        assert!(
            std::str::from_utf8(&request.payload)
                .unwrap()
                .contains(r#""release":"7"}}"#)
        );
        respond(
            &mut stream,
            &request,
            format!(
                r#"{{"operation_id":"{}","release":7,"removed_bytes":4096}}"#,
                operation_id
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            )
            .as_bytes(),
        );
    });

    let mut secret = [7; 32];
    let mut client = AdminClient::connect_authenticated(address, [1; 16], &mut secret).unwrap();
    assert_eq!(client.remove_sdk_library(7).unwrap(), 4096);
    server.join().unwrap();
}

pub(super) fn admin_client_reads_owner_scoped_sdk_library_operation_status() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let operation_id = [0x11; 16];
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let request = read_frame(&mut stream, &mut decoder);
        assert_eq!(request.kind, 10);
        assert_eq!(
            std::str::from_utf8(&request.payload).unwrap(),
            r#"{"phase":"read","operation_version":1,"operation":"sdk_libraries.operation_status","arguments":{"operation_id":"11111111111111111111111111111111"}}"#
        );
        respond(
            &mut stream,
            &request,
            br#"{"operation_id":"11111111111111111111111111111111","state":"succeeded","operation":"install","release":7,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","library_bytes":4096}"#,
        );
    });

    let mut secret = [7; 32];
    let mut client = AdminClient::connect_authenticated(address, [1; 16], &mut secret).unwrap();
    assert_eq!(
        client.sdk_library_operation_status(operation_id).unwrap(),
        prdb_engine::client_submit::SdkLibraryOperationStatus::Installed {
            sdk_release: 7,
            sha256: [0xaa; 32],
            library_bytes: 4096,
        }
    );
    server.join().unwrap();
}

pub(super) fn repository_profile_does_not_fall_back_when_peer_omits_capability() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        let hello = read_frame(&mut stream, &mut decoder);
        assert!(
            std::str::from_utf8(&hello.payload)
                .unwrap()
                .contains(r#""sdk_library_mode":"repository""#)
        );
        respond(
            &mut stream,
            &hello,
            br#"{"transport":"plaintext_tcp","limits":{"chunk_bytes":131072,"control_bytes":32768,"payload_bytes":524288},"mode":"query","minor":0,"major":1}"#,
        );
        let mut byte = [0; 1];
        assert_eq!(stream.read(&mut byte).unwrap(), 0);
    });
    let mut secret = [9; 32];
    let result = SubmissionClient::connect_repository_authenticated_with_timeout(
        address,
        [7; 16],
        &mut secret,
        Duration::from_secs(5),
    );
    assert!(matches!(
        result,
        Err(prdb_engine::client_submit::ClientSubmitError::InvalidResponse)
    ));
    assert_eq!(secret, [0; 32]);
    server.join().unwrap();
}

pub(super) fn admin_client_streams_sdk_library_install_over_control_chunks() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let metadata = b"metadata";
    let library = b"library";
    let digest: [u8; 32] = Sha256::digest(library).into();
    let digest_text = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let server_digest = digest_text.clone();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let begin = read_frame(&mut stream, &mut decoder);
        assert_eq!(begin.kind, 10);
        let operation_id = operation_id_from_payload(&begin.payload);
        let operation_id_text = operation_id
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let server_operation_id = operation_id_text.clone();
        assert_eq!(
            std::str::from_utf8(&begin.payload).unwrap(),
            format!(
                r#"{{"phase":"prepare","operation_version":1,"operation":"sdk_libraries.install","arguments":{{"operation_id":"{}","metadata_bytes":"{}","library_bytes":"{}","library_sha256":"{}"}}}}"#,
                server_operation_id,
                metadata.len(),
                library.len(),
                server_digest
            )
        );
        respond(
            &mut stream,
            &begin,
            format!(r#"{{"operation_id":"{server_operation_id}","next_offset":"0"}}"#).as_bytes(),
        );
        let metadata_chunk = read_frame(&mut stream, &mut decoder);
        assert_eq!(metadata_chunk.kind, 19);
        assert_eq!(&metadata_chunk.payload[..16], &operation_id);
        assert_eq!(
            u64::from_le_bytes(metadata_chunk.payload[16..24].try_into().unwrap()),
            0
        );
        assert_eq!(&metadata_chunk.payload[24..], metadata);
        respond(
            &mut stream,
            &metadata_chunk,
            format!(r#"{{"next_offset":"{}"}}"#, metadata.len()).as_bytes(),
        );
        let library_chunk = read_frame(&mut stream, &mut decoder);
        assert_eq!(library_chunk.kind, 19);
        assert_eq!(&library_chunk.payload[..16], &operation_id);
        assert_eq!(
            u64::from_le_bytes(library_chunk.payload[16..24].try_into().unwrap()),
            metadata.len() as u64
        );
        assert_eq!(&library_chunk.payload[24..], library);
        respond(
            &mut stream,
            &library_chunk,
            format!(r#"{{"next_offset":"{}"}}"#, metadata.len() + library.len()).as_bytes(),
        );
        let finish = read_frame(&mut stream, &mut decoder);
        assert_eq!(finish.kind, 10);
        assert_eq!(
            std::str::from_utf8(&finish.payload).unwrap(),
            format!(
                r#"{{"phase":"write","operation_version":1,"operation":"sdk_libraries.install.finish","arguments":{{"operation_id":"{server_operation_id}"}}}}"#
            )
        );
        respond(
            &mut stream,
            &finish,
            format!(
                r#"{{"operation_id":"{server_operation_id}","release":7,"sha256":"{server_digest}","library_bytes":{}}}"#,
                library.len()
            )
            .as_bytes(),
        );
        let list = read_frame(&mut stream, &mut decoder);
        assert_eq!(list.kind, 10);
        respond(
            &mut stream,
            &list,
            format!(
                r#"{{"sdk_libraries":[{{"release":7,"native_abis":[5],"target":"linux-x86_64","cpu_baseline":"x86_64-v1","runtime_profile":"test-runtime","soname":"libprdb_sql.so.7","sha256":"{server_digest}","needed":["libc.so.6"],"library_bytes":{}}}],"next_cursor":null}}"#,
                library.len()
            )
            .as_bytes(),
        );
    });

    let mut secret = [7; 32];
    let mut client = AdminClient::connect_authenticated(address, [1; 16], &mut secret).unwrap();
    let installed = client.install_sdk_library(metadata, library).unwrap();
    assert_eq!(installed.sdk_release, 7);
    assert_eq!(installed.sha256, digest);
    assert_eq!(installed.library_bytes, library.len() as u64);
    server.join().unwrap();
}

pub(super) fn admin_client_lists_runtime_invocation_pages_with_best_effort_metadata() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let cursor = format!(
        "01{}{}{}",
        "02".repeat(16),
        "03".repeat(16),
        "04".repeat(16)
    );
    let server_cursor = cursor.clone();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let request = read_frame(&mut stream, &mut decoder);
        assert_eq!(request.kind, 10);
        assert_eq!(
            std::str::from_utf8(&request.payload).unwrap(),
            r#"{"phase":"read","operation_version":8,"operation":"runtime.invocations.list","arguments":{"limit":"1"}}"#
        );
        let first_response = format!(
            "{{\"observed_at_us\":1000,\"consistency\":\"best_effort\",\"invocations\":[{{\"invocation_id\":\"{}\",\"transaction_id\":\"{}\",\"principal_id\":\"{}\",\"state\":\"active\",\"workload_class\":\"operational\",\"started_at_us\":900,\"age_us\":100,\"deadline_at_us\":1200,\"staged_rows\":1,\"staged_value_bytes\":12,\"snapshot_pin_count\":1,\"cancellation_requested\":false,\"cancellation_reason_code\":null}}],\"pinned_snapshot_count\":2,\"oldest_snapshot_pin_age_us\":500,\"invocations_admitted_total\":2,\"invocation_conflicts_total\":1,\"admin_cancellations_accepted_total\":1,\"admin_cancellations_denied_total\":2,\"invocation_policy_denials_total\":3,\"invocation_staged_value_bytes_total\":32,\"invocation_commit_latency_buckets\":[11,12,13,14,15,16],\"invocation_redb_write_acquisition_buckets\":[1,2,3,4,5,6],\"outcome_maintenance_passes_total\":7,\"outcome_maintenance_failures_total\":8,\"outcomes_pruned_total\":9,\"fence_waits\":[{{\"wait_id\":\"{}\",\"category\":\"ddl\",\"waited_us\":50,\"resource_ids\":[\"{}\"],\"resources_truncated\":false,\"blocker_ids\":[\"{}\"],\"blockers_truncated\":false}}],\"fence_waits_truncated\":false,\"gateway_result_bytes_relayed_total\":12345,\"gateway_result_bytes_sent_to_clients_total\":6789,\"supervisor_queue\":{{\"pending_submissions\":4,\"starting_submissions\":1,\"started_submissions\":2,\"queued_operational_starts\":3,\"queued_analytical_starts\":5,\"oldest_queued_start_wait_us\":6000,\"operational_start_wait_buckets\":[1,2,3,4,5,6],\"analytical_start_wait_buckets\":[6,5,4,3,2,1],\"reserved_submission_bytes\":4096}},\"next_cursor\":\"{server_cursor}\"}}",
            "10".repeat(16),
            "30".repeat(16),
            "20".repeat(16),
            "42".repeat(16),
            "40".repeat(16),
            "41".repeat(16)
        );
        respond(&mut stream, &request, first_response.as_bytes());

        let request = read_frame(&mut stream, &mut decoder);
        assert_eq!(
            std::str::from_utf8(&request.payload).unwrap(),
            format!(
                "{{\"phase\":\"read\",\"operation_version\":8,\"operation\":\"runtime.invocations.list\",\"arguments\":{{\"limit\":\"1\",\"cursor\":\"{server_cursor}\"}}}}"
            )
        );
        respond(
            &mut stream,
            &request,
            format!(
                "{{\"observed_at_us\":1001,\"consistency\":\"best_effort\",\"invocations\":[{{\"invocation_id\":\"{}\",\"transaction_id\":\"{}\",\"principal_id\":\"{}\",\"state\":\"committing\",\"workload_class\":\"analytical\",\"started_at_us\":901,\"age_us\":100,\"deadline_at_us\":null,\"staged_rows\":2,\"staged_value_bytes\":24,\"snapshot_pin_count\":1,\"cancellation_requested\":true,\"cancellation_reason_code\":7}}],\"pinned_snapshot_count\":2,\"oldest_snapshot_pin_age_us\":600,\"invocations_admitted_total\":2,\"invocation_conflicts_total\":2,\"admin_cancellations_accepted_total\":2,\"admin_cancellations_denied_total\":4,\"invocation_policy_denials_total\":5,\"invocation_staged_value_bytes_total\":64,\"invocation_commit_latency_buckets\":[21,22,23,24,25,26],\"invocation_redb_write_acquisition_buckets\":[6,5,4,3,2,1],\"outcome_maintenance_passes_total\":10,\"outcome_maintenance_failures_total\":11,\"outcomes_pruned_total\":12,\"fence_waits\":[],\"fence_waits_truncated\":false,\"gateway_result_bytes_relayed_total\":12346,\"gateway_result_bytes_sent_to_clients_total\":6790,\"supervisor_queue\":{{\"pending_submissions\":0,\"starting_submissions\":0,\"started_submissions\":1,\"queued_operational_starts\":0,\"queued_analytical_starts\":0,\"oldest_queued_start_wait_us\":0,\"operational_start_wait_buckets\":[0,0,0,0,0,0],\"analytical_start_wait_buckets\":[0,0,0,0,0,0],\"reserved_submission_bytes\":0}},\"next_cursor\":null}}",
                "11".repeat(16),
                "31".repeat(16),
                "20".repeat(16)
            )
            .as_bytes(),
        );
    });

    let mut secret = [7; 32];
    let mut client = AdminClient::connect_authenticated(address, [1; 16], &mut secret).unwrap();
    let first = client.list_runtime_invocations_page(1, None).unwrap();
    assert_eq!(first.observed_at_us, 1000);
    assert_eq!(
        first.consistency,
        prdb_engine::client_submit::RuntimeConsistency::BestEffort
    );
    assert_eq!(first.records.len(), 1);
    assert_eq!(first.records[0].invocation_id, [0x10; 16]);
    assert_eq!(first.records[0].transaction_id, [0x30; 16]);
    assert_eq!(first.records[0].principal_id, [0x20; 16]);
    assert_eq!(
        first.records[0].workload_class,
        prdb_engine::client_submit::RuntimeWorkloadClass::Operational
    );
    assert_eq!(
        first.records[0].state,
        prdb_engine::client_submit::RuntimeInvocationState::Active
    );
    assert_eq!(first.records[0].started_at_us, Some(900));
    assert_eq!(first.records[0].age_us, 100);
    assert_eq!(first.records[0].deadline_at_us, Some(1200));
    assert_eq!(first.records[0].staged_rows, 1);
    assert_eq!(first.records[0].staged_value_bytes, 12);
    assert_eq!(first.records[0].snapshot_pin_count, 1);
    assert_eq!(first.pinned_snapshot_count, 2);
    assert_eq!(first.oldest_snapshot_pin_age_us, 500);
    assert_eq!(first.invocations_admitted_total, 2);
    assert_eq!(first.invocation_conflicts_total, 1);
    assert_eq!(first.admin_cancellations_accepted_total, 1);
    assert_eq!(first.admin_cancellations_denied_total, 2);
    assert_eq!(first.invocation_policy_denials_total, 3);
    assert_eq!(first.invocation_staged_value_bytes_total, 32);
    assert_eq!(
        first.invocation_commit_latency_buckets,
        [11, 12, 13, 14, 15, 16]
    );
    assert_eq!(
        first.invocation_redb_write_acquisition_buckets,
        [1, 2, 3, 4, 5, 6]
    );
    assert_eq!(first.outcome_maintenance_passes_total, 7);
    assert_eq!(first.outcome_maintenance_failures_total, 8);
    assert_eq!(first.outcomes_pruned_total, 9);
    assert_eq!(first.fence_waits.len(), 1);
    assert_eq!(first.fence_waits[0].wait_id, [0x42; 16]);
    assert_eq!(
        first.fence_waits[0].category,
        prdb_engine::client_submit::RuntimeFenceWaitCategory::Ddl
    );
    assert_eq!(first.fence_waits[0].waited_us, 50);
    assert_eq!(first.fence_waits[0].resource_ids, vec![[0x40; 16]]);
    assert!(!first.fence_waits[0].resources_truncated);
    assert_eq!(first.fence_waits[0].blocker_ids, vec![[0x41; 16]]);
    assert!(!first.fence_waits[0].blockers_truncated);
    assert!(!first.fence_waits_truncated);
    assert_eq!(first.gateway_result_bytes_relayed_total, 12345);
    assert_eq!(first.gateway_result_bytes_sent_to_clients_total, 6789);
    assert_eq!(
        first.supervisor_queue,
        prdb_engine::supervisor::StagingRuntimeUsage {
            pending_submissions: 4,
            starting_submissions: 1,
            started_submissions: 2,
            queued_operational_starts: 3,
            queued_analytical_starts: 5,
            oldest_queued_start_wait_us: 6000,
            operational_start_wait_buckets: [1, 2, 3, 4, 5, 6],
            analytical_start_wait_buckets: [6, 5, 4, 3, 2, 1],
            reserved_submission_bytes: 4096,
        }
    );
    assert!(!first.records[0].cancellation_requested);
    assert_eq!(first.records[0].cancellation_reason_code, None);
    assert_eq!(first.next_cursor.as_deref(), Some(cursor.as_str()));

    let second = client
        .list_runtime_invocations_page(1, first.next_cursor.as_deref())
        .unwrap();
    assert_eq!(second.observed_at_us, 1001);
    assert_eq!(second.records[0].invocation_id, [0x11; 16]);
    assert_eq!(second.records[0].transaction_id, [0x31; 16]);
    assert_eq!(second.records[0].age_us, 100);
    assert!(second.fence_waits.is_empty());
    assert_eq!(second.supervisor_queue.started_submissions, 1);
    assert_eq!(
        second.supervisor_queue.operational_start_wait_buckets,
        [0; 6]
    );
    assert_eq!(
        second.supervisor_queue.analytical_start_wait_buckets,
        [0; 6]
    );
    assert_eq!(second.gateway_result_bytes_relayed_total, 12346);
    assert_eq!(second.gateway_result_bytes_sent_to_clients_total, 6790);
    assert_eq!(second.pinned_snapshot_count, 2);
    assert_eq!(second.oldest_snapshot_pin_age_us, 600);
    assert_eq!(second.invocations_admitted_total, 2);
    assert_eq!(second.invocation_conflicts_total, 2);
    assert_eq!(second.admin_cancellations_accepted_total, 2);
    assert_eq!(second.admin_cancellations_denied_total, 4);
    assert_eq!(second.invocation_policy_denials_total, 5);
    assert_eq!(second.invocation_staged_value_bytes_total, 64);
    assert_eq!(
        second.invocation_commit_latency_buckets,
        [21, 22, 23, 24, 25, 26]
    );
    assert_eq!(second.outcome_maintenance_passes_total, 10);
    assert_eq!(second.outcome_maintenance_failures_total, 11);
    assert_eq!(second.outcomes_pruned_total, 12);
    assert!(!second.fence_waits_truncated);
    assert_eq!(
        second.records[0].workload_class,
        prdb_engine::client_submit::RuntimeWorkloadClass::Analytical
    );
    assert_eq!(
        second.records[0].state,
        prdb_engine::client_submit::RuntimeInvocationState::Committing
    );
    assert_eq!(second.records[0].staged_rows, 2);
    assert_eq!(second.records[0].staged_value_bytes, 24);
    assert_eq!(second.records[0].snapshot_pin_count, 1);
    assert!(second.records[0].cancellation_requested);
    assert_eq!(second.records[0].cancellation_reason_code, Some(7));
    assert_eq!(second.next_cursor, None);
    server.join().unwrap();
}

pub(super) fn admin_client_lists_logical_index_definitions_without_physical_details() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let request = read_frame(&mut stream, &mut decoder);
        assert_eq!(request.kind, 10);
        assert_eq!(
            std::str::from_utf8(&request.payload).unwrap(),
            r#"{"phase":"read","operation_version":1,"operation":"metadata.indexes.list","arguments":{"schema_id":"11111111111111111111111111111111","table_id":"22222222222222222222222222222222","limit":"1"}}"#
        );
        respond(
            &mut stream,
            &request,
            br#"{"indexes":[{"index_id":"33333333333333333333333333333333","name":"users_by_name","column_ids":["44444444444444444444444444444444"],"unique":true}],"next_cursor":null}"#,
        );
    });

    let mut secret = [7; 32];
    let mut client = AdminClient::connect_authenticated(address, [1; 16], &mut secret).unwrap();
    let page = client
        .list_indexes_page([0x11; 16], [0x22; 16], 1, None)
        .unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].index_id, [0x33; 16]);
    assert_eq!(page.records[0].name, "users_by_name");
    assert_eq!(page.records[0].column_ids, vec![[0x44; 16]]);
    assert!(page.records[0].unique);
    assert_eq!(page.next_cursor, None);
    server.join().unwrap();
}

pub(super) fn admin_client_lists_constraints_and_accepts_redacted_foreign_key_targets() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let request = read_frame(&mut stream, &mut decoder);
        assert_eq!(request.kind, 10);
        assert_eq!(
            std::str::from_utf8(&request.payload).unwrap(),
            r#"{"phase":"read","operation_version":1,"operation":"metadata.constraints.list","arguments":{"schema_id":"11111111111111111111111111111111","table_id":"22222222222222222222222222222222","limit":"4"}}"#
        );
        respond(
            &mut stream,
            &request,
            br#"{"constraints":[{"constraint_id":"33333333333333333333333333333333","name":"fk_orders_customer","kind":"foreign_key","column_ids":["44444444444444444444444444444444"],"referenced_table_id":null,"referenced_column_ids":[]}],"next_cursor":null}"#,
        );
    });

    let mut secret = [7; 32];
    let mut client = AdminClient::connect_authenticated(address, [1; 16], &mut secret).unwrap();
    let page = client
        .list_constraints_page([0x11; 16], [0x22; 16], 4, None)
        .unwrap();
    assert_eq!(page.records.len(), 1);
    let constraint = &page.records[0];
    assert_eq!(constraint.constraint_id, [0x33; 16]);
    assert_eq!(
        constraint.kind,
        prdb_engine::metadata_discovery::AdministrativeConstraintKind::ForeignKey
    );
    assert_eq!(constraint.column_ids, vec![[0x44; 16]]);
    assert_eq!(constraint.referenced_table_id, None);
    assert!(constraint.referenced_column_ids.is_empty());
    server.join().unwrap();
}

pub(super) fn admin_client_lists_direct_table_grants_with_provenance() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let request = read_frame(&mut stream, &mut decoder);
        assert_eq!(request.kind, 10);
        assert_eq!(
            std::str::from_utf8(&request.payload).unwrap(),
            r#"{"phase":"read","operation_version":1,"operation":"metadata.grants.list","arguments":{"schema_id":"11111111111111111111111111111111","table_id":"22222222222222222222222222222222","limit":"4"}}"#
        );
        respond(
            &mut stream,
            &request,
            br#"{"grants":[{"grantee_id":"33333333333333333333333333333333","grantor_id":"44444444444444444444444444444444","privilege":"select","target_scope":"column","target_id":"55555555555555555555555555555555"}],"next_cursor":null}"#,
        );
    });

    let mut secret = [7; 32];
    let mut client = AdminClient::connect_authenticated(address, [1; 16], &mut secret).unwrap();
    let page = client
        .list_grants_page([0x11; 16], [0x22; 16], 4, None)
        .unwrap();
    assert_eq!(page.records.len(), 1);
    let grant = &page.records[0];
    assert_eq!(grant.grantee_id, [0x33; 16]);
    assert_eq!(grant.grantor_id, [0x44; 16]);
    assert_eq!(
        grant.privilege,
        prdb_engine::metadata_discovery::AdministrativeGrantPrivilege::Select
    );
    assert_eq!(
        grant.target_scope,
        prdb_engine::metadata_discovery::AdministrativeGrantTargetScope::Column
    );
    assert_eq!(grant.target_id, [0x55; 16]);
    server.join().unwrap();
}

pub(super) fn admin_client_lists_secret_free_policy_summaries() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let request = read_frame(&mut stream, &mut decoder);
        assert_eq!(request.kind, 10);
        assert_eq!(
            std::str::from_utf8(&request.payload).unwrap(),
            r#"{"phase":"read","operation_version":1,"operation":"metadata.policies.list","arguments":{"schema_id":"11111111111111111111111111111111","table_id":"22222222222222222222222222222222","limit":"4"}}"#
        );
        respond(
            &mut stream,
            &request,
            br#"{"policies":[{"policy_id":"33333333333333333333333333333333","kind":"row","operation":"select","effect":"allow","column_ids":[],"recipients":[{"kind":"user","principal_id":"44444444444444444444444444444444"}]}],"next_cursor":null}"#,
        );
    });

    let mut secret = [7; 32];
    let mut client = AdminClient::connect_authenticated(address, [1; 16], &mut secret).unwrap();
    let page = client
        .list_policies_page([0x11; 16], [0x22; 16], 4, None)
        .unwrap();
    assert_eq!(page.records.len(), 1);
    let policy = &page.records[0];
    assert_eq!(policy.policy_id, [0x33; 16]);
    assert_eq!(
        policy.kind,
        prdb_engine::metadata_discovery::AdministrativePolicyKind::Row
    );
    assert_eq!(
        policy.operation,
        prdb_engine::metadata_discovery::AdministrativePolicyOperation::Select
    );
    assert_eq!(
        policy.effect,
        prdb_engine::metadata_discovery::AdministrativePolicyEffect::Allow
    );
    assert!(policy.column_ids.is_empty());
    let recipients = policy.recipients.as_ref().unwrap();
    assert_eq!(recipients.len(), 1);
    assert_eq!(
        recipients[0].kind,
        prdb_engine::metadata_discovery::AdministrativePolicyRecipientKind::User
    );
    assert_eq!(recipients[0].principal_id, [0x44; 16]);
    server.join().unwrap();
}

pub(super) fn admin_client_lists_visible_dependency_edges() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let request = read_frame(&mut stream, &mut decoder);
        assert_eq!(request.kind, 10);
        assert_eq!(
            std::str::from_utf8(&request.payload).unwrap(),
            r#"{"phase":"read","operation_version":1,"operation":"metadata.dependencies.list","arguments":{"object_id":"11111111111111111111111111111111","limit":"4"}}"#
        );
        respond(
            &mut stream,
            &request,
            br#"{"dependencies":[{"dependent_id":"22222222222222222222222222222222","referenced_id":"11111111111111111111111111111111","kind":"foreign_key_target"}],"next_cursor":null}"#,
        );
    });

    let mut secret = [7; 32];
    let mut client = AdminClient::connect_authenticated(address, [1; 16], &mut secret).unwrap();
    let page = client.list_dependencies_page([0x11; 16], 4, None).unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].dependent_id, [0x22; 16]);
    assert_eq!(page.records[0].referenced_id, [0x11; 16]);
    assert_eq!(
        page.records[0].kind,
        prdb_engine::metadata_discovery::AdministrativeDependencyKind::ForeignKeyTarget
    );
    server.join().unwrap();
}

pub(super) fn submission_client_decodes_cancellation_acknowledgements() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        for state in [
            "accepted",
            "too_late",
            "committed",
            "aborted",
            "unknown",
            "pending",
        ] {
            let (mut stream, _) = listener.accept().unwrap();
            let mut decoder = ClientFrameDecoder::default();
            establish_query_session(&mut stream, &mut decoder);
            let request = read_frame(&mut stream, &mut decoder);
            assert_eq!(request.kind, 9);
            assert_eq!(
                request.payload,
                br#"{"invocation_id":"11111111111111111111111111111111"}"#
            );
            respond(
                &mut stream,
                &request,
                format!("{{\"state\":\"{state}\"}}").as_bytes(),
            );
        }
    });

    for expected in [
        CancellationResult::Accepted,
        CancellationResult::TooLate,
        CancellationResult::Terminal(TerminalOutcome::Committed),
        CancellationResult::Terminal(TerminalOutcome::Aborted),
        CancellationResult::Unknown,
    ] {
        let mut secret = [5; 32];
        let mut client = SubmissionClient::connect_authenticated_with_timeout(
            address,
            [6; 16],
            &mut secret,
            Duration::from_secs(3),
        )
        .unwrap();
        assert_eq!(secret, [0; 32]);
        assert!(matches!(
            client.cancel([0; 16]),
            Err(prdb_engine::client_submit::ClientSubmitError::InvalidInvocationId)
        ));
        assert_eq!(client.cancel([0x11; 16]).unwrap(), expected);
    }
    let mut secret = [5; 32];
    let mut client = SubmissionClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    assert!(matches!(
        client.cancel([0x11; 16]),
        Err(prdb_engine::client_submit::ClientSubmitError::InvalidResponse)
    ));
    server.join().unwrap();
}

pub(super) fn admin_client_cancels_invocations_on_control_mode() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let request = read_frame(&mut stream, &mut decoder);
        assert_eq!(request.kind, 9);
        assert_eq!(
            request.payload,
            br#"{"invocation_id":"22222222222222222222222222222222"}"#
        );
        respond(&mut stream, &request, br#"{"state":"committed"}"#);
    });

    let mut secret = [5; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    assert_eq!(secret, [0; 32]);
    assert_eq!(
        client.cancel_invocation([0x22; 16]).unwrap(),
        CancellationResult::Terminal(TerminalOutcome::Committed)
    );
    server.join().unwrap();
}

pub(super) fn admin_client_sends_typed_index_prepare_start_and_status_requests() {
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

pub(super) fn admin_client_creates_index_from_qualified_sql_names() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        for (operation, body) in [
            (
                "metadata.schemas.list",
                format!(
                    r#"{{"schemas":[{{"schema_id":"{}","name":"app"}}],"next_cursor":null}}"#,
                    "a3".repeat(16)
                ),
            ),
            (
                "metadata.relations.list",
                format!(
                    r#"{{"relations":[{{"relation_id":"{}","name":"employees"}}],"next_cursor":null}}"#,
                    "b4".repeat(16)
                ),
            ),
            (
                "metadata.columns.list",
                format!(
                    r#"{{"columns":[{{"column_id":"{}","name":"department_id","ordinal":0,"scalar_type":"integer","nullable":false}},{{"column_id":"{}","name":"name","ordinal":1,"scalar_type":"text","nullable":false}}],"next_cursor":null}}"#,
                    "c5".repeat(16),
                    "c6".repeat(16)
                ),
            ),
        ] {
            let request = read_frame(&mut stream, &mut decoder);
            assert!(
                String::from_utf8_lossy(&request.payload)
                    .contains(&format!(r#""operation":"{operation}""#))
            );
            respond(&mut stream, &request, body.as_bytes());
        }
        let prepare = read_frame(&mut stream, &mut decoder);
        let payload = String::from_utf8(prepare.payload.clone()).unwrap();
        assert!(payload.contains(r#""operation":"indexes.create""#));
        assert!(payload.contains(r#""name":"employee_department""#));
        assert!(payload.contains(&format!(
            r#""table_id":"{}","name":"employee_department","column_ids":["{}","{}"],"unique":false"#,
            "b4".repeat(16),
            "c5".repeat(16),
            "c6".repeat(16)
        )));
        respond(
            &mut stream,
            &prepare,
            format!(
                r#"{{"operation_id":"{}","state":"prepared"}}"#,
                "d7".repeat(16)
            )
            .as_bytes(),
        );
        let start = read_frame(&mut stream, &mut decoder);
        respond(
            &mut stream,
            &start,
            format!(
                r#"{{"operation_id":"{}","outcome":"committed","index_id":"{}","commit_sequence":"83"}}"#,
                "d7".repeat(16),
                "e8".repeat(16)
            )
            .as_bytes(),
        );
    });

    let mut secret = [5; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    let created = client
        .execute_create_index_sql(
            "CREATE INDEX employee_department ON app.employees USING BTREE (department_id, name)",
        )
        .unwrap();
    assert_eq!(created.index_id, [0xe8; 16]);
    assert_eq!(created.commit_sequence, 83);
    server.join().unwrap();
}

pub(super) fn admin_client_drops_index_from_schema_qualified_sql_name() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        for (operation, body) in [
            (
                "metadata.schemas.list",
                format!(
                    r#"{{"schemas":[{{"schema_id":"{}","name":"app"}}],"next_cursor":null}}"#,
                    "a3".repeat(16)
                ),
            ),
            (
                "metadata.relations.list",
                format!(
                    r#"{{"relations":[{{"relation_id":"{}","name":"employees"}}],"next_cursor":null}}"#,
                    "b4".repeat(16)
                ),
            ),
            (
                "metadata.indexes.list",
                format!(
                    r#"{{"indexes":[{{"index_id":"{}","name":"employee_department","column_ids":["{}"],"unique":false}}],"next_cursor":null}}"#,
                    "c5".repeat(16),
                    "d6".repeat(16)
                ),
            ),
        ] {
            let request = read_frame(&mut stream, &mut decoder);
            assert!(
                String::from_utf8_lossy(&request.payload)
                    .contains(&format!(r#""operation":"{operation}""#))
            );
            respond(&mut stream, &request, body.as_bytes());
        }
        let prepare = read_frame(&mut stream, &mut decoder);
        let payload = String::from_utf8(prepare.payload.clone()).unwrap();
        assert!(payload.contains(r#""operation":"indexes.drop""#));
        assert!(payload.contains(&format!(r#""index_id":"{}""#, "c5".repeat(16))));
        respond(
            &mut stream,
            &prepare,
            format!(
                r#"{{"operation_id":"{}","state":"prepared"}}"#,
                "e7".repeat(16)
            )
            .as_bytes(),
        );
        let start = read_frame(&mut stream, &mut decoder);
        respond(
            &mut stream,
            &start,
            format!(
                r#"{{"operation_id":"{}","outcome":"committed","index_id":"{}","commit_sequence":"84"}}"#,
                "e7".repeat(16),
                "c5".repeat(16)
            )
            .as_bytes(),
        );
    });

    let mut secret = [5; 32];
    let mut client = AdminClient::connect_authenticated_with_timeout(
        address,
        [6; 16],
        &mut secret,
        Duration::from_secs(3),
    )
    .unwrap();
    let dropped = client
        .execute_drop_index_sql("DROP INDEX app.employee_department RESTRICT")
        .unwrap();
    assert_eq!(dropped.index_id, [0xc5; 16]);
    assert_eq!(dropped.commit_sequence, 84);
    server.join().unwrap();
}

pub(super) fn admin_client_creates_user_and_role_from_sql_names() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        for (operation, name, id_field, operation_id, identity_id) in [
            ("users.create", "analyst", "user_id", [0x11; 16], [0xa1; 16]),
            (
                "users.create_no_login",
                "service_loader",
                "user_id",
                [0x13; 16],
                [0xa3; 16],
            ),
            (
                "roles.create",
                "reporting",
                "role_id",
                [0x12; 16],
                [0xa2; 16],
            ),
        ] {
            let prepare = read_frame(&mut stream, &mut decoder);
            let payload = String::from_utf8(prepare.payload.clone()).unwrap();
            assert!(payload.contains(&format!(r#""operation":"{operation}""#)));
            assert!(payload.contains(&format!(r#""name":"{name}""#)));
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
            respond(
                &mut stream,
                &start,
                format!(
                    r#"{{"operation_id":"{}","outcome":"committed","{id_field}":"{}","commit_sequence":"{}"}}"#,
                    hex(&operation_id),
                    hex(&identity_id),
                    identity_id[0]
                )
                .as_bytes(),
            );
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
    let user = client
        .execute_create_identity_sql("CREATE USER analyst LOGIN")
        .unwrap();
    assert_eq!(user.identity_id, [0xa1; 16]);
    let no_login_user = client
        .execute_create_identity_sql("CREATE USER service_loader NOLOGIN")
        .unwrap();
    assert_eq!(no_login_user.identity_id, [0xa3; 16]);
    let role = client
        .execute_create_identity_sql("CREATE ROLE reporting;")
        .unwrap();
    assert_eq!(role.identity_id, [0xa2; 16]);
    server.join().unwrap();
}

pub(super) fn admin_client_returns_operation_id_when_start_reply_is_lost() {
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

pub(super) fn admin_client_sends_typed_user_and_role_lifecycle_requests() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        let cases = [
            ("users.create", r#"{"name":"alice"}"#, Some("user_id")),
            ("roles.create", r#"{"name":"analyst"}"#, Some("role_id")),
            (
                "roles.grant",
                r#"{"member_id":"01010101010101010101010101010101","role_id":"02020202020202020202020202020202"}"#,
                None,
            ),
            (
                "users.rename",
                r#"{"user_id":"01010101010101010101010101010101","name":"alice_2"}"#,
                None,
            ),
            (
                "users.login",
                r#"{"user_id":"01010101010101010101010101010101","enabled":"false"}"#,
                None,
            ),
            (
                "roles.revoke",
                r#"{"member_id":"01010101010101010101010101010101","role_id":"02020202020202020202020202020202"}"#,
                None,
            ),
            (
                "users.drop",
                r#"{"user_id":"01010101010101010101010101010101"}"#,
                None,
            ),
            (
                "roles.drop",
                r#"{"role_id":"02020202020202020202020202020202"}"#,
                None,
            ),
        ];
        for (index, (operation, arguments, created_key)) in cases.into_iter().enumerate() {
            let prepare = read_frame(&mut stream, &mut decoder);
            assert_eq!(prepare.kind, 10);
            let payload = String::from_utf8(prepare.payload.clone()).unwrap();
            assert!(payload.contains(&format!("\"operation\":\"{operation}\"")));
            assert!(payload.contains(&format!("\"arguments\":{arguments}")));
            let operation_id = [0x30 + index as u8; 16];
            respond(
                &mut stream,
                &prepare,
                format!(
                    "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                    hex(&operation_id)
                )
                .as_bytes(),
            );
            let start = read_frame(&mut stream, &mut decoder);
            assert_eq!(start.kind, 10);
            assert!(
                String::from_utf8(start.payload.clone())
                    .unwrap()
                    .contains(&hex(&operation_id))
            );
            let identity = created_key
                .map(|key| format!(",\"{key}\":\"{}\"", "a1".repeat(16)))
                .unwrap_or_default();
            respond(
                &mut stream,
                &start,
                format!(
                    "{{\"operation_id\":\"{}\",\"outcome\":\"committed\"{identity},\"commit_sequence\":\"{}\"}}",
                    hex(&operation_id),
                    index + 1
                )
                .as_bytes(),
            );
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
    assert_eq!(secret, [0; 32]);
    assert_eq!(client.create_user("alice").unwrap().identity_id, [0xa1; 16]);
    assert_eq!(
        client.create_role("analyst").unwrap().identity_id,
        [0xa1; 16]
    );
    client.grant_role([1; 16], [2; 16]).unwrap();
    client.rename_user([1; 16], "alice_2").unwrap();
    client.set_user_login([1; 16], false).unwrap();
    client.revoke_role([1; 16], [2; 16]).unwrap();
    client.drop_user([1; 16]).unwrap();
    client.drop_role([2; 16]).unwrap();
    server.join().unwrap();
}

pub(super) fn admin_client_issues_one_time_token_and_revokes_tokens() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        for (operation, arguments, operation_id) in [
            (
                "tokens.issue",
                r#"{"user_id":"01010101010101010101010101010101","label":"daily \"alpha\"","expires_at_us":"123456"}"#,
                [0x41; 16],
            ),
            (
                "tokens.revoke",
                r#"{"token_id":"a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2"}"#,
                [0x42; 16],
            ),
        ] {
            let prepare = read_frame(&mut stream, &mut decoder);
            assert_eq!(prepare.kind, 10);
            let payload = String::from_utf8(prepare.payload.clone()).unwrap();
            assert!(payload.contains(&format!("\"operation\":\"{operation}\"")));
            assert!(payload.contains(&format!("\"arguments\":{arguments}")));
            respond(
                &mut stream,
                &prepare,
                format!(
                    "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                    hex(&operation_id)
                )
                .as_bytes(),
            );
            let start = read_frame(&mut stream, &mut decoder);
            assert_eq!(start.kind, 10);
            let result = if operation == "tokens.issue" {
                format!(
                    "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"token_id\":\"{}\",\"token_secret\":\"paWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaU=\",\"commit_sequence\":\"21\"}}",
                    hex(&operation_id),
                    "a2".repeat(16)
                )
            } else {
                format!(
                    "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"commit_sequence\":\"22\"}}",
                    hex(&operation_id)
                )
            };
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
    let issued = client
        .issue_token([1; 16], "daily \"alpha\"", 123456)
        .unwrap();
    assert_eq!(issued.token_id, [0xa2; 16]);
    assert_eq!(issued.token_secret(), &[0xa5; 32]);
    assert_eq!(issued.commit_sequence, 21);
    client.revoke_token(issued.token_id).unwrap();
    server.join().unwrap();
}

pub(super) fn admin_client_grants_and_revokes_database_system_privileges() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut decoder = ClientFrameDecoder::default();
        establish_control_session(&mut stream, &mut decoder);
        for (operation, arguments, operation_id) in [
            (
                "privileges.grant",
                r#"{"grantee_id":"01010101010101010101010101010101","privilege":"manage_users"}"#,
                [0x51; 16],
            ),
            (
                "privileges.revoke",
                r#"{"grantee_id":"01010101010101010101010101010101","privilege":"create_schema"}"#,
                [0x52; 16],
            ),
        ] {
            let prepare = read_frame(&mut stream, &mut decoder);
            let payload = String::from_utf8(prepare.payload.clone()).unwrap();
            assert!(payload.contains(&format!("\"operation\":\"{operation}\"")));
            assert!(payload.contains(&format!("\"arguments\":{arguments}")));
            respond(
                &mut stream,
                &prepare,
                format!(
                    "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                    hex(&operation_id)
                )
                .as_bytes(),
            );
            let start = read_frame(&mut stream, &mut decoder);
            respond(
                &mut stream,
                &start,
                format!(
                    "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"commit_sequence\":\"{}\"}}",
                    hex(&operation_id),
                    if operation == "privileges.grant" { 31 } else { 32 }
                )
                .as_bytes(),
            );
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
    client
        .grant_system_privilege([1; 16], AdminSystemPrivilege::ManageUsers)
        .unwrap();
    client
        .revoke_system_privilege([1; 16], AdminSystemPrivilege::CreateSchema)
        .unwrap();
    server.join().unwrap();
}
