use prdb_engine::audit::AuditAction;
use prdb_engine::catalog::decode_registered_envelope;
use prdb_engine::catalog_index::{
    lookup_column_id_by_name, lookup_column_id_by_ordinal, lookup_index_id_by_name,
    lookup_relation_id_by_name, lookup_schema_id_by_name,
};
use prdb_engine::commit::{
    CatalogWrite, CommitRequest, IndexWrite, RowWrite, commit_catalog_and_audit, commit_rows,
    commit_storage_changes,
};
use prdb_engine::index_locator::IndexLocator;
use prdb_engine::outcome::{InvocationOutcome, OutcomeStatus};
use prdb_engine::schema::{self, CATALOG};
use prdb_engine::storage_manager::StorageManager;
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, encode_tuple};
use redb::{Database, ReadableDatabase};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::helpers::*;

pub(super) fn gateway_service_listener_authenticates_control_admin_over_socket()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::ipc::{IpcFrame, Operation, read_frame, write_frame};
    use prdb_engine::supervisor::{
        StagingLimits, StagingStore, SubmissionMetadata, SubmissionMetadataValidator,
    };
    use prdb_engine::supervisor_ipc::{
        GatewayAdmissionConfig, GatewayServiceListenerConfig, bind_gateway_service_socket,
        serve_gateway_service_listener,
    };
    use std::os::unix::net::UnixStream;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct AcceptUpload;
    impl SubmissionMetadataValidator for AcceptUpload {
        fn supports_versions(&self, manifest_version: u32, scope_version: u32) -> bool {
            manifest_version == 1 && scope_version == 1
        }

        fn validate(&self, _metadata: &SubmissionMetadata) -> bool {
            true
        }
    }

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_id = id(0x91);
    let admin_role = builtin_owner_id();
    let admin_token_id = id(0x92);
    let admin_secret = [0u8; 32];
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_micros() as i64;
    let mut setup_audit = identity_audit_event(0x93, admin_role, admin_id);
    setup_audit.correlation_id = id(0x93);
    setup_audit.action = AuditAction::AuthorityChange;
    commit_catalog_and_audit(
        &database,
        id(0x93),
        admin_role,
        now + 3_600_000_000,
        &[
            principal_write_with_name(admin_id, 1, 1, "gateway_socket_admin"),
            object_write_with_name(admin_id, 8, "gateway_socket_admin"),
            membership_write_with_grantor(admin_id, admin_role, admin_role),
            dependency_write(admin_id, admin_role, 5),
            credential_verifier_write(
                admin_token_id,
                admin_id,
                &admin_secret,
                now - 1_000_000,
                now + 3_600_000_000,
                None,
                1,
            ),
        ],
        &[setup_audit],
    )?;
    let staging = StagingStore::new(StagingLimits::default(), Arc::new(AcceptUpload))?;
    let socket_path =
        std::path::PathBuf::from(format!("{}.gateway.sock", fixture.path().display()));
    let listener = bind_gateway_service_socket(&socket_path)?;
    let shutdown = Arc::new(AtomicBool::new(false));
    let admission = GatewayAdmissionConfig {
        retention_period_us: i64::MAX,
        max_rows: 2,
        max_bytes: 64,
    };
    let executor_path = std::env::current_exe()?;
    let cgroup_parent = fixture.path().parent().unwrap().to_path_buf();
    let service_storage = &storage;
    std::thread::scope(|scope| {
        let shutdown_server = Arc::clone(&shutdown);
        let server = scope.spawn(move || {
            serve_gateway_service_listener(
                listener,
                service_storage,
                &staging,
                prdb_engine::supervisor_ipc::GatewayServiceRuntimeConfig {
                    admission,
                    executor: prdb_engine::supervisor_ipc::GatewayExecutorConfig {
                        cgroup_parent: cgroup_parent.clone(),
                        resource_limits: prdb_engine::linux_sandbox::WorkerResourceLimits {
                            cpu_quota_micros: 100_000,
                            cpu_period_micros: 100_000,
                            memory_max_bytes: 64 * 1024 * 1024,
                            max_processes: 16,
                        },
                        analytical_resource_limits: None,
                        launch: prdb_engine::executor_launcher::ExecutorLaunchConfig {
                            executor_path: executor_path.clone(),
                            optimizer_path: std::path::PathBuf::from(env!(
                                "CARGO_BIN_EXE_prdb-optimizer"
                            )),
                            scratch_parent: std::env::temp_dir(),
                            runtime_read_execute: Vec::new(),
                            runtime_read_only: Vec::new(),
                            spill_broker: None,
                            max_total_result_bytes: prdb_engine::query_abi::MAX_TOTAL_RESULT_BYTES,
                            analytical_max_total_result_bytes: None,
                            max_sql_index_probe_candidates:
                                prdb_engine::query_abi::DEFAULT_SQL_INDEX_PROBE_CANDIDATES,
                            analytical_max_sql_index_probe_candidates: None,
                            cleanup_timeout: Duration::from_millis(100),
                        },
                    },
                    listener: GatewayServiceListenerConfig {
                        expected_gateway_uid: unsafe { libc::geteuid() },
                        max_connections: 2,
                        idle_timeout: Duration::from_secs(5),
                        accept_poll_interval: Duration::from_millis(5),
                        outcome_maintenance_interval: Duration::from_secs(60),
                        outcome_maintenance_batch: prdb_engine::outcome::MAX_OUTCOME_PRUNE_BATCH,
                        statistics_maintenance_interval: Duration::from_secs(60),
                    },
                },
                shutdown_server,
                |_manager, _request_id, _stream, _invocation, _submission, _executor| {},
            )
        });

        let mut gateway = UnixStream::connect(&socket_path)?;
        write_frame(
            &mut gateway,
            &IpcFrame {
                operation: Operation::OpenClientSession as u16,
                request_id: 1,
                payload: [
                    vec![1],
                    br#"{"mode":"control","client_name":"listener"}"#.to_vec(),
                ]
                .concat(),
            },
        )?;
        let response = read_frame(&mut gateway)?.ok_or("missing HELLO response")?;
        assert_eq!(
            response.operation,
            Operation::OpenClientSession as u16,
            "gateway HELLO failed: {}",
            String::from_utf8_lossy(&response.payload)
        );
        assert_eq!(response.request_id, 1);
        assert!(std::str::from_utf8(&response.payload)?.contains("\"mode\":\"control\""));
        let token_id_hex = admin_token_id
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        write_frame(
            &mut gateway,
            &IpcFrame {
                operation: Operation::AuthenticateSession as u16,
                request_id: 2,
                payload: [
                    vec![1],
                    format!(
                        "{{\"token_id\":\"{token_id_hex}\",\"token_secret\":\"{}\"}}",
                        "A".repeat(43) + "="
                    )
                    .into_bytes(),
                ]
                .concat(),
            },
        )?;
        let response = read_frame(&mut gateway)?.ok_or("missing AUTH response")?;
        assert_eq!(response.operation, Operation::AuthenticateSession as u16);
        assert_eq!(response.request_id, 2);
        assert!(std::str::from_utf8(&response.payload)?.contains("\"authenticated\":true"));
        write_frame(
            &mut gateway,
            &IpcFrame {
                operation: Operation::SubmitAdministrativeOperation as u16,
                request_id: 3,
                payload: br#"{"phase":"prepare","operation_version":1,"operation":"users.create","arguments":{"name":"socket_created_user"}}"#.to_vec(),
            },
        )?;
        let response = read_frame(&mut gateway)?.ok_or("missing ADMIN prepare response")?;
        assert_eq!(
            response.operation,
            Operation::SubmitAdministrativeOperation as u16
        );
        assert_eq!(response.request_id, 3);
        let prepared = std::str::from_utf8(&response.payload)?;
        let operation_id = prepared
            .split("\"operation_id\":\"")
            .nth(1)
            .ok_or("missing socket ADMIN operation ID")?
            .get(..32)
            .ok_or("invalid socket ADMIN operation ID")?;
        assert!(prepared.ends_with("\",\"state\":\"prepared\"}"));
        write_frame(
            &mut gateway,
            &IpcFrame {
                operation: Operation::SubmitAdministrativeOperation as u16,
                request_id: 4,
                payload: format!("{{\"phase\":\"start\",\"operation_id\":\"{operation_id}\"}}")
                    .into_bytes(),
            },
        )?;
        let response = read_frame(&mut gateway)?.ok_or("missing ADMIN start response")?;
        assert_eq!(
            response.operation,
            Operation::SubmitAdministrativeOperation as u16
        );
        assert_eq!(response.request_id, 4);
        let started = std::str::from_utf8(&response.payload)?;
        assert!(started.contains("\"outcome\":\"committed\""));
        let user_id = started
            .split("\"user_id\":\"")
            .nth(1)
            .ok_or("missing socket-created user ID")?
            .get(..32)
            .ok_or("invalid socket-created user ID")?
            .to_owned();
        let mut next_request_id = 5u64;
        let mut admin_execute =
            |prepare_payload: Vec<u8>| -> Result<String, Box<dyn std::error::Error>> {
                let prepare_id = next_request_id;
                next_request_id += 1;
                write_frame(
                    &mut gateway,
                    &IpcFrame {
                        operation: Operation::SubmitAdministrativeOperation as u16,
                        request_id: prepare_id,
                        payload: prepare_payload,
                    },
                )?;
                let response = read_frame(&mut gateway)?.ok_or("missing ADMIN prepare response")?;
                if response.operation != Operation::SubmitAdministrativeOperation as u16
                    || response.request_id != prepare_id
                {
                    return Err(std::io::Error::other(format!(
                        "unexpected ADMIN prepare response: {} {} {}",
                        response.operation,
                        response.request_id,
                        String::from_utf8_lossy(&response.payload)
                    ))
                    .into());
                }
                let prepared = String::from_utf8(response.payload)?;
                let operation_id = prepared
                    .split("\"operation_id\":\"")
                    .nth(1)
                    .ok_or("missing ADMIN operation ID")?
                    .get(..32)
                    .ok_or("invalid ADMIN operation ID")?
                    .to_owned();
                if !prepared.ends_with("\",\"state\":\"prepared\"}") {
                    return Err(std::io::Error::other(format!(
                        "ADMIN operation was not prepared: {prepared}"
                    ))
                    .into());
                }
                let start_id = next_request_id;
                next_request_id += 1;
                write_frame(
                    &mut gateway,
                    &IpcFrame {
                        operation: Operation::SubmitAdministrativeOperation as u16,
                        request_id: start_id,
                        payload: format!(
                            "{{\"phase\":\"start\",\"operation_id\":\"{operation_id}\"}}"
                        )
                        .into_bytes(),
                    },
                )?;
                let response = read_frame(&mut gateway)?.ok_or("missing ADMIN start response")?;
                if response.operation != Operation::SubmitAdministrativeOperation as u16
                    || response.request_id != start_id
                {
                    return Err(std::io::Error::other(format!(
                        "unexpected ADMIN start response: {} {} {}",
                        response.operation,
                        response.request_id,
                        String::from_utf8_lossy(&response.payload)
                    ))
                    .into());
                }
                Ok(String::from_utf8(response.payload)?)
            };
        let schema_result = admin_execute(
            format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"schemas.create\",\"arguments\":{{\"name\":\"socket_policy_schema\",\"owner_id\":\"{user_id}\"}}}}"
            )
            .into_bytes(),
        )?;
        let schema_id = schema_result
            .split("\"schema_id\":\"")
            .nth(1)
            .ok_or("missing socket-created schema ID")?
            .get(..32)
            .ok_or("invalid socket-created schema ID")?
            .to_owned();
        assert!(schema_result.contains("\"outcome\":\"committed\""));
        let table_result = admin_execute(
            format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tables.create\",\"arguments\":{{\"schema_id\":\"{schema_id}\",\"name\":\"socket_policy_table\",\"columns\":[{{\"name\":\"secret\",\"type\":\"bytea\",\"nullable\":false,\"generated_uuid_identity\":false}}]}}}}"
            )
            .into_bytes(),
        )?;
        let table_id = table_result
            .split("\"table_id\":\"")
            .nth(1)
            .ok_or("missing socket-created table ID")?
            .get(..32)
            .ok_or("invalid socket-created table ID")?
            .to_owned();
        let column_id = table_result
            .split("\"column_ids\":[\"")
            .nth(1)
            .ok_or("missing socket-created column ID")?
            .get(..32)
            .ok_or("invalid socket-created column ID")?
            .to_owned();
        assert!(table_result.contains("\"outcome\":\"committed\""));
        let expression = prdb_engine::expression::encode_expression(
            &prdb_engine::authorization::Expr::Literal(Some(
                prdb_engine::tuple::ScalarValue::Boolean(true),
            )),
        )?
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
        let policy_result = admin_execute(
            format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"policies.create\",\"arguments\":{{\"table_id\":\"{table_id}\",\"name\":\"socket_cell_policy\",\"kind\":\"cell\",\"policy_operation\":\"select\",\"effect\":\"allow\",\"recipients\":[\"user:{user_id}\"],\"target_column_ids\":[\"{column_id}\"],\"using_expression_eir1\":\"{expression}\",\"with_check_expression_eir1\":null}}}}"
            )
            .into_bytes(),
        )?;
        assert!(policy_result.contains("\"outcome\":\"committed\""));
        assert!(policy_result.contains("\"policy_id\":"));

        let restricted_result = admin_execute(
            br#"{"phase":"prepare","operation_version":1,"operation":"users.create","arguments":{"name":"socket_restricted_user"}}"#.to_vec(),
        )?;
        let restricted_user_id = restricted_result
            .split("\"user_id\":\"")
            .nth(1)
            .ok_or("missing socket-created restricted user ID")?
            .get(..32)
            .ok_or("invalid socket-created restricted user ID")?
            .to_owned();
        for grantee_id in [user_id.as_str(), restricted_user_id.as_str()] {
            for (object_id, privilege, target_scope, target_id) in [
                (schema_id.as_str(), "usage", "object", schema_id.as_str()),
                (table_id.as_str(), "select", "object", table_id.as_str()),
                (table_id.as_str(), "select", "column", column_id.as_str()),
            ] {
                let grant_result = admin_execute(
                    format!(
                        "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"objects.grant\",\"arguments\":{{\"grantee_id\":\"{grantee_id}\",\"object_id\":\"{object_id}\",\"privilege\":\"{privilege}\",\"target_scope\":\"{target_scope}\",\"target_id\":\"{target_id}\"}}}}"
                    )
                    .into_bytes(),
                )?;
                assert!(grant_result.contains("\"outcome\":\"committed\""));
            }
        }
        for (recipient, policy_name) in [
            (user_id.as_str(), "socket_reader_row_policy"),
            (restricted_user_id.as_str(), "socket_restricted_row_policy"),
        ] {
            let row_policy = admin_execute(
                format!(
                    "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"policies.create\",\"arguments\":{{\"table_id\":\"{table_id}\",\"name\":\"{policy_name}\",\"kind\":\"row\",\"policy_operation\":\"select\",\"effect\":\"allow\",\"recipients\":[\"user:{recipient}\"],\"target_column_ids\":[],\"using_expression_eir1\":\"{expression}\",\"with_check_expression_eir1\":null}}}}"
                )
                .into_bytes(),
            )?;
            assert!(row_policy.contains("\"outcome\":\"committed\""));
        }
        let reader_token = admin_execute(
            format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tokens.issue\",\"arguments\":{{\"user_id\":\"{user_id}\",\"label\":\"socket reader\",\"expires_at_us\":\"{}\"}}}}",
                now + 3_600_000_000
            )
            .into_bytes(),
        )?;
        let restricted_token = admin_execute(
            format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tokens.issue\",\"arguments\":{{\"user_id\":\"{restricted_user_id}\",\"label\":\"socket restricted\",\"expires_at_us\":\"{}\"}}}}",
                now + 3_600_000_000
            )
            .into_bytes(),
        )?;
        let id_bytes = |value: &str| -> Result<[u8; 16], Box<dyn std::error::Error>> {
            let mut parsed = [0; 16];
            if value.len() != 32 {
                return Err("invalid 16-byte identifier".into());
            }
            for (index, byte) in parsed.iter_mut().enumerate() {
                *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)?;
            }
            Ok(parsed)
        };
        let table_id_bytes = id_bytes(&table_id)?;
        let column_id_bytes = id_bytes(&column_id)?;
        drop(gateway);
        shutdown.store(true, Ordering::Release);
        server
            .join()
            .map_err(|_| std::io::Error::other("gateway service listener panicked"))??;

        let decode_token = |response: &str| -> Result<
            prdb_engine::client_protocol::ClientCredentials,
            Box<dyn std::error::Error>,
        > {
            let token_id = response
                .split("\"token_id\":\"")
                .nth(1)
                .ok_or("missing socket-issued token ID")?
                .get(..32)
                .ok_or("invalid socket-issued token ID")?;
            let secret = response
                .split("\"token_secret\":\"")
                .nth(1)
                .ok_or("missing socket-issued token secret")?
                .get(..44)
                .ok_or("invalid socket-issued token secret")?;
            let credentials = prdb_engine::client_protocol::decode_client_credentials(
                format!("{{\"token_id\":\"{token_id}\",\"token_secret\":\"{secret}\"}}").as_bytes(),
            )?;
            Ok(credentials)
        };
        let reader_credentials = decode_token(&reader_token)?;
        let restricted_credentials = decode_token(&restricted_token)?;
        let payload = encode_tuple(
            &[ColumnSchema {
                scalar_type: ScalarType::Bytea,
                nullable: false,
            }],
            1,
            &[Some(ScalarValue::Bytea(vec![
                0x73, 0x65, 0x63, 0x72, 0x65, 0x74,
            ]))],
        )?;
        commit_rows(
            &database,
            id(0x97),
            admin_id,
            now + 3_600_000_000,
            &[RowWrite {
                key: table_row_key(table_id_bytes, id(0x98)),
                expected_version: None,
                payload: Some(payload),
            }],
        )?;
        let (mut reader, roles) = storage.begin_token_authenticated_invocation(
            id(0x99),
            reader_credentials.token_id,
            &reader_credentials.token_secret,
            now + 3_600_000_000,
            4,
            4096,
        )?;
        assert!(roles.is_empty());
        let (mut restricted, roles) = storage.begin_token_authenticated_invocation(
            id(0x9a),
            restricted_credentials.token_id,
            &restricted_credentials.token_secret,
            now + 3_600_000_000,
            4,
            4096,
        )?;
        assert!(roles.is_empty());
        assert_eq!(
            executor_ipc_scan_cells(
                &mut reader,
                "socket_policy_schema",
                "socket_policy_table",
                vec![column_id_bytes]
            )?,
            vec![vec![prdb_engine::authorization::SanitizedCell::Value(
                ScalarValue::Bytea(b"secret".to_vec())
            )]],
        );
        assert_eq!(
            executor_ipc_scan_cells(
                &mut restricted,
                "socket_policy_schema",
                "socket_policy_table",
                vec![column_id_bytes]
            )?,
            vec![vec![prdb_engine::authorization::SanitizedCell::Redacted]],
        );
        executor_ipc_rejects_forged_handles(&mut restricted)?;
        assert_eq!(
            executor_ipc_scan_cells(
                &mut restricted,
                "socket_policy_schema",
                "socket_policy_table",
                vec![column_id_bytes]
            )?,
            vec![vec![prdb_engine::authorization::SanitizedCell::Redacted]],
            "forged IPC attempts must not widen the restricted principal's cell view"
        );
        reader.abort()?;
        restricted.abort()?;
        run_sandboxed_forged_handle_client(
            &storage,
            &restricted_credentials,
            id_bytes(&restricted_user_id)?,
            now + 3_600_000_000,
            "PRDB_TEST_FORGED_HANDLE_QUERY_ARTIFACT",
        )?;
        run_sandboxed_forged_handle_client(
            &storage,
            &restricted_credentials,
            id_bytes(&restricted_user_id)?,
            now + 3_600_000_000,
            "PRDB_TEST_FORGED_ROW_HANDLE_QUERY_ARTIFACT",
        )?;
        let (mut restricted_after_forgery, _) = storage.begin_token_authenticated_invocation(
            id(0x9b),
            restricted_credentials.token_id,
            &restricted_credentials.token_secret,
            now + 3_600_000_000,
            4,
            4096,
        )?;
        assert_eq!(
            executor_ipc_scan_cells(
                &mut restricted_after_forgery,
                "socket_policy_schema",
                "socket_policy_table",
                vec![column_id_bytes]
            )?,
            vec![vec![prdb_engine::authorization::SanitizedCell::Redacted]],
            "confined forged-handle client must not widen the restricted principal's cell view"
        );
        restricted_after_forgery.abort()?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    std::fs::remove_file(socket_path)?;
    Ok(())
}

pub(super) fn gateway_disconnect_after_storage_commit_is_reconcilable()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::ipc::{Operation, read_frame};
    use prdb_engine::outcome::OutcomeStatus;
    use prdb_engine::storage_manager::InvocationStatus;
    use prdb_engine::supervisor_ipc::{
        GatewayAbortReason, GatewayTerminalDecision, finalize_gateway_invocation_and_send_terminal,
        send_gateway_result_data_event, send_gateway_terminal_for_confirmed_outcome,
    };
    use std::os::unix::net::UnixStream;
    use std::time::{SystemTime, UNIX_EPOCH};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let user_id = id(0x70);
    let token_id = id(0x71);
    let secret = [0x31; 32];
    commit_catalog_and_audit(
        &database,
        id(0x72),
        id(9),
        now + 3_600_000_000,
        &[
            principal_write_with_name(user_id, 1, 1, "terminal_user"),
            object_write_with_name(user_id, 8, "terminal_user"),
            credential_verifier_write(
                token_id,
                user_id,
                &secret,
                now - 1_000_000,
                now + 60_000_000,
                None,
                1,
            ),
        ],
        &[audit_event(0x72)],
    )?;
    let mut invocation = storage.admit_supervisor_invocation(
        id(0x73),
        token_id,
        &secret,
        now + 60_000_000,
        2,
        64,
    )?;
    let mut worker = std::process::Command::new("true").spawn()?;
    invocation.observe_worker_exit(&mut worker)?;
    let (mut service, mut gateway) = UnixStream::pair()?;
    send_gateway_result_data_event(&mut service, 9, b"provisional".to_vec())?;
    let data = read_frame(&mut gateway)?.ok_or("missing provisional event")?;
    assert_eq!(data.operation, Operation::GatewayResultData as u16);
    assert_eq!(data.request_id, 9);
    assert_eq!(data.payload, b"provisional");
    drop(gateway);
    assert!(
        finalize_gateway_invocation_and_send_terminal(
            &mut invocation,
            &mut service,
            9,
            GatewayTerminalDecision::Commit,
        )
        .is_err()
    );
    assert_eq!(
        storage
            .lookup_invocation_outcome(id(0x73), user_id, now + 1)?
            .unwrap()
            .status,
        OutcomeStatus::Committed
    );
    assert!(matches!(
        storage
            .get_invocation_status(id(0x73), user_id, now + 1)?
            .unwrap()
            .status,
        InvocationStatus::Terminal(outcome) if outcome.status == OutcomeStatus::Committed
    ));
    let (mut replay_service, mut replay_gateway) = UnixStream::pair()?;
    assert_eq!(
        send_gateway_terminal_for_confirmed_outcome(
            &storage,
            &invocation,
            &mut replay_service,
            9,
            GatewayAbortReason::InternalError,
        )?,
        GatewayTerminalDecision::Commit
    );
    let reconciled_terminal = read_frame(&mut replay_gateway)?.ok_or("missing terminal replay")?;
    assert_eq!(
        reconciled_terminal.operation,
        Operation::GatewayTerminal as u16
    );
    assert!(String::from_utf8(reconciled_terminal.payload)?.contains("\"outcome\":\"committed\""));
    Ok(())
}

pub(super) fn process_exit_after_durable_commit_preserves_reconcilable_outcome()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::outcome::OutcomeStatus;
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_CRASH_AFTER_COMMIT_DATABASE";
    const CHILD_INVOCATION_ID: [u8; 16] = [0xa1; 16];
    const PRINCIPAL_ID: [u8; 16] = [9; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database = Database::open(database_path)?;
        let retention_deadline_us = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_micros()
                .saturating_add(60_000_000),
        )?;
        commit_storage_changes(
            &database,
            CommitRequest {
                invocation_id: CHILD_INVOCATION_ID,
                principal_id: PRINCIPAL_ID,
                retention_deadline_us,
                rows: &[],
                indexes: &[],
                foreign_keys: &[],
                catalog_guards: &[],
                catalog: &[],
                audit: &[],
            },
        )?;
        // Simulate losing the supervisor reply and all process-local cleanup
        // immediately after redb reports durable success.
        std::process::exit(0);
    }

    let fixture = Fixture::new();
    let database_path = fixture.path();
    drop(initialize(&database_path)?);
    let child_status = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("process_exit_after_durable_commit_preserves_reconcilable_outcome")
        .arg("--nocapture")
        .env(CHILD_DATABASE_ENV, &database_path)
        .status()?;
    assert!(
        child_status.success(),
        "commit child exited with {child_status}"
    );

    let reopened = Database::open(&database_path)?;
    let storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
    let now_us = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let outcome = storage
        .lookup_invocation_outcome(CHILD_INVOCATION_ID, PRINCIPAL_ID, now_us)?
        .ok_or("committed outcome was not recoverable after process exit")?;
    assert_eq!(outcome.status, OutcomeStatus::Committed);
    assert!(outcome.commit_sequence > 0);
    Ok(())
}

pub(super) fn process_exit_after_atomic_row_index_commit_preserves_publication()
-> Result<(), Box<dyn std::error::Error>> {
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_CRASH_AFTER_ROW_INDEX_COMMIT_DATABASE";
    const INVOCATION_ID: [u8; 16] = [0xa2; 16];
    const PRINCIPAL_ID: [u8; 16] = [9; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database = Database::open(database_path)?;
        let row_key = key(7);
        let retention_deadline_us = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_micros()
                .saturating_add(60_000_000),
        )?;
        let (catalog_key, catalog_payload) = dependency_record([6; 16], [7; 16], 1);
        let indexed_key = index_key(&row_key);
        commit_storage_changes(
            &database,
            CommitRequest {
                invocation_id: INVOCATION_ID,
                principal_id: PRINCIPAL_ID,
                retention_deadline_us,
                rows: &[RowWrite {
                    key: row_key,
                    expected_version: None,
                    payload: Some(b"durable row".to_vec()),
                }],
                indexes: &[IndexWrite {
                    key: indexed_key,
                    row_key,
                    expected_version: None,
                    present: true,
                    unique: false,
                    null_exempt: false,
                }],
                foreign_keys: &[],
                catalog_guards: &[],
                catalog: &[
                    object_write_with_parent([6; 16], 4, Some([7; 16])),
                    object_write([7; 16], 3),
                    column_write([6; 16], [7; 16], "object_6"),
                    table_write([7; 16], builtin_id(1)),
                    CatalogWrite {
                        key: catalog_key,
                        expected_value: None,
                        payload: Some(catalog_payload),
                    },
                ],
                audit: &[audit_event(0xa3)],
            },
        )?;
        // Simulate process loss after durable publication and before a reply.
        std::process::exit(0);
    }

    let fixture = Fixture::new();
    drop(initialize(&fixture.path())?);
    let child_status = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("process_exit_after_atomic_row_index_commit_preserves_publication")
        .arg("--nocapture")
        .env(CHILD_DATABASE_ENV, fixture.path())
        .status()?;
    assert!(
        child_status.success(),
        "row/index commit child exited with {child_status}"
    );

    let reopened = Database::open(fixture.path())?;
    let row_key = key(7);
    assert_eq!(row(&reopened, &row_key)?.unwrap().payload, b"durable row");
    let indexed_key = index_key(&row_key);
    let (catalog_key, _) = dependency_record([6; 16], [7; 16], 1);
    let read = reopened.begin_read()?;
    let indexes = read.open_table(schema::INDEXES)?;
    let locator = indexes
        .get(indexed_key.as_slice())?
        .ok_or("index locator was not recovered after process exit")?
        .value()
        .to_vec();
    assert_eq!(
        IndexLocator::decode_for_key(&indexed_key, &locator)?.row_version,
        1
    );
    assert!(
        read.open_table(CATALOG)?
            .get(catalog_key.as_slice())?
            .is_some()
    );
    assert!(
        read.open_table(schema::AUDIT)?
            .get(&[0xa3; 16][..])?
            .is_some()
    );
    let outcome = InvocationOutcome::decode(
        read.open_table(schema::OUTCOMES)?
            .get(&INVOCATION_ID[..])?
            .ok_or("committed outcome was not recovered after process exit")?
            .value(),
    )?;
    assert_eq!(outcome.status, OutcomeStatus::Committed);
    assert_eq!(outcome.commit_sequence, 1);
    assert_eq!(commit_sequence(&reopened)?, 1);
    Ok(())
}

pub(super) fn process_crash_during_authorized_index_changes_recovers_all_or_none()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{CreateTableRequest, TableColumnDefinition};
    use std::process::Command;

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_CRASH_DURING_INDEX_DDL_DATABASE";
    const CHILD_BOUNDARY_ENV: &str = "PRDB_TEST_CRASH_DURING_INDEX_DDL_BOUNDARY";
    const CHILD_OPERATION_ENV: &str = "PRDB_TEST_CRASH_DURING_INDEX_DDL_OPERATION";
    const CHILD_EXIT_CODE: i32 = 87;
    const POST_COMMIT_EXIT_CODE: i32 = 88;
    const ACTOR_ID: [u8; 16] = [0xa5; 16];
    const SEED_INVOCATION_ID: [u8; 16] = [0xa6; 16];
    const INDEX_INVOCATION_ID: [u8; 16] = [0xa7; 16];
    const INDEX_AUDIT_ID: [u8; 16] = [0xa8; 16];
    const PREPARE_INDEX_INVOCATION_ID: [u8; 16] = [0xb3; 16];
    const PREPARE_INDEX_AUDIT_ID: [u8; 16] = [0xb4; 16];
    const ROW_ID: [u8; 16] = [0xa9; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database_path = std::path::PathBuf::from(database_path);
        let database = initialize(&database_path)?;
        let storage = StorageManager::new(&database, Duration::from_millis(100))?;
        let admin_role = builtin_owner_id();
        let setup = [
            principal_write_with_name(ACTOR_ID, 1, 1, "index_crash_actor"),
            object_write_with_name(ACTOR_ID, 8, "index_crash_actor"),
            membership_write(ACTOR_ID, admin_role),
            dependency_write(ACTOR_ID, admin_role, 5),
        ];
        let mut setup_audit = identity_audit_event(0xaa, admin_role, id(0xab));
        setup_audit.action = AuditAction::AuthorityChange;
        storage.commit_catalog_and_audit(id(0xab), admin_role, i64::MAX, &setup, &[setup_audit])?;

        let schema_invocation = id(0xac);
        let mut schema_audit = identity_audit_event(0xad, ACTOR_ID, id(0xae));
        schema_audit.correlation_id = schema_invocation;
        let schema = storage.create_schema(
            schema_invocation,
            ACTOR_ID,
            "index_crash_schema",
            ACTOR_ID,
            i64::MAX,
            schema_audit,
        )?;
        let table_invocation = id(0xaf);
        let mut table_audit = identity_audit_event(0xb0, ACTOR_ID, id(0xb1));
        table_audit.correlation_id = table_invocation;
        let table = storage.create_table(CreateTableRequest {
            invocation_id: table_invocation,
            authenticated_actor_id: ACTOR_ID,
            schema_id: schema.schema_id,
            name: "index_crash_table".to_owned(),
            columns: vec![
                TableColumnDefinition {
                    name: "value".to_owned(),
                    scalar_type: ScalarType::Integer,
                    nullable: false,
                    generated_uuid_identity: false,
                    default: None,
                },
                TableColumnDefinition {
                    name: "alternate".to_owned(),
                    scalar_type: ScalarType::Integer,
                    nullable: false,
                    generated_uuid_identity: false,
                    default: None,
                },
            ],
            retention_deadline_us: i64::MAX,
            audit_event: table_audit,
        })?;
        let mut seed = storage.begin_invocation(SEED_INVOCATION_ID, ACTOR_ID, i64::MAX, 1, 1024)?;
        let table_schema = seed.load_catalog_table_schema(table.table_id)?;
        seed.load_catalog_indexes(&table_schema)?;
        let row = prdb_engine::overlay::RowId {
            table: table.table_id,
            row: ROW_ID,
        };
        let row_payload = encode_tuple(
            &table_schema.columns,
            table_schema.schema_version,
            &[Some(ScalarValue::Integer(7)), Some(ScalarValue::Integer(8))],
        )?;
        seed.stage_put(row, &row_payload)?;
        seed.commit()?;
        let operation = std::env::var(CHILD_OPERATION_ENV)?;
        let prepared_index_id = if operation == "drop" || operation == "rebuild" {
            let mut prepare_audit =
                identity_audit_event(PREPARE_INDEX_AUDIT_ID[0], ACTOR_ID, table.table_id);
            prepare_audit.event_id = PREPARE_INDEX_AUDIT_ID;
            prepare_audit.correlation_id = PREPARE_INDEX_INVOCATION_ID;
            Some(
                storage
                    .create_index(prdb_engine::storage_manager::CreateIndexRequest {
                        invocation_id: PREPARE_INDEX_INVOCATION_ID,
                        authenticated_actor_id: ACTOR_ID,
                        table_id: table.table_id,
                        name: "crash_index".to_owned(),
                        column_ids: vec![table.column_ids[0]],
                        unique: false,
                        retention_deadline_us: i64::MAX,
                        audit_event: prepare_audit,
                    })?
                    .index_id,
            )
        } else if operation == "create" {
            None
        } else {
            return Err(format!("unknown index DDL operation {operation}").into());
        };
        let before_ddl_sequence = commit_sequence(&database)?;
        let metadata_path = database_path.with_extension("index-crash-metadata");
        let mut metadata = Vec::with_capacity(120);
        metadata.extend_from_slice(&schema.schema_id);
        metadata.extend_from_slice(&table.table_id);
        metadata.extend_from_slice(&table.column_ids[0]);
        metadata.extend_from_slice(&table.column_ids[1]);
        metadata.extend_from_slice(&ROW_ID);
        metadata.extend_from_slice(&table_schema.schema_version.to_le_bytes());
        metadata.extend_from_slice(&before_ddl_sequence.to_le_bytes());
        metadata.extend_from_slice(&prepared_index_id.unwrap_or([0; 16]));
        let before_table_epoch = {
            let read = database.begin_read()?;
            let catalog = read.open_table(CATALOG)?;
            let table_record = catalog
                .get(id_catalog_key(5, table.table_id).as_slice())?
                .ok_or("index crash table missing before DDL")?;
            u64::from_le_bytes(
                decode_registered_envelope(table_record.value())?
                    .fields
                    .iter()
                    .find(|field| field.tag == 3)
                    .ok_or("index crash table epoch missing before DDL")?
                    .payload
                    .as_slice()
                    .try_into()?,
            )
        };
        metadata.extend_from_slice(&before_table_epoch.to_le_bytes());
        std::fs::write(&metadata_path, metadata)?;
        drop(storage);
        drop(database);

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&database_path)?;
        let armed = Arc::new(AtomicBool::new(false));
        let instrumented = Database::builder().create_with_backend(ExitAfterNextWriteBackend {
            inner: redb::backends::FileBackend::new(file)?,
            armed: armed.clone(),
            exit_code: CHILD_EXIT_CODE,
        })?;
        let storage = StorageManager::new(&instrumented, Duration::from_millis(100))?;
        let prepared_index_id: [u8; 16] = std::fs::read(&metadata_path)?[96..112].try_into()?;
        let operation = std::env::var(CHILD_OPERATION_ENV)?;
        let mut index_audit = identity_audit_event(INDEX_AUDIT_ID[0], ACTOR_ID, id(0xb2));
        index_audit.event_id = INDEX_AUDIT_ID;
        index_audit.correlation_id = INDEX_INVOCATION_ID;
        let boundary = std::env::var(CHILD_BOUNDARY_ENV)?;
        if boundary == "write" {
            armed.store(true, Ordering::SeqCst);
        } else if boundary != "after_commit" {
            return Err(format!("unknown index DDL crash boundary {boundary}").into());
        }
        if operation == "create" {
            let result = storage.create_index(prdb_engine::storage_manager::CreateIndexRequest {
                invocation_id: INDEX_INVOCATION_ID,
                authenticated_actor_id: ACTOR_ID,
                table_id: table.table_id,
                name: "crash_index".to_owned(),
                column_ids: vec![table.column_ids[0]],
                unique: false,
                retention_deadline_us: i64::MAX,
                audit_event: index_audit,
            });
            if boundary == "write" {
                return Err(format!(
                    "index creation returned without injected backend-write exit: {result:?}"
                )
                .into());
            }
            result?;
        } else if operation == "drop" {
            let result = storage.drop_index(prdb_engine::storage_manager::DropIndexRequest {
                invocation_id: INDEX_INVOCATION_ID,
                authenticated_actor_id: ACTOR_ID,
                index_id: prepared_index_id,
                retention_deadline_us: i64::MAX,
                audit_event: index_audit,
            });
            if boundary == "write" {
                return Err(format!(
                    "index removal returned without injected backend-write exit: {result:?}"
                )
                .into());
            }
            result?;
        } else {
            let result = storage.rebuild_index(prdb_engine::storage_manager::RebuildIndexRequest {
                invocation_id: INDEX_INVOCATION_ID,
                authenticated_actor_id: ACTOR_ID,
                index_id: prepared_index_id,
                column_ids: vec![table.column_ids[1]],
                unique: false,
                retention_deadline_us: i64::MAX,
                audit_event: index_audit,
            });
            if boundary == "write" {
                return Err(format!(
                    "index rebuild returned without injected backend-write exit: {result:?}"
                )
                .into());
            }
            result?;
        }
        std::process::exit(POST_COMMIT_EXIT_CODE);
    }

    for operation in ["create", "drop", "rebuild"] {
        for (boundary, expected_exit) in [
            ("write", CHILD_EXIT_CODE),
            ("after_commit", POST_COMMIT_EXIT_CODE),
        ] {
            let fixture = Fixture::new();
            let database_path = fixture.path();
            let child_status = Command::new(std::env::current_exe()?)
                .arg("--exact")
                .arg("process_crash_during_authorized_index_changes_recovers_all_or_none")
                .arg("--nocapture")
                .env(CHILD_DATABASE_ENV, &database_path)
                .env(CHILD_BOUNDARY_ENV, boundary)
                .env(CHILD_OPERATION_ENV, operation)
                .status()?;
            assert_eq!(
                child_status.code(),
                Some(expected_exit),
                "index DDL child missed the {boundary} crash boundary: {child_status}"
            );

            let metadata = std::fs::read(database_path.with_extension("index-crash-metadata"))?;
            assert_eq!(metadata.len(), 120);
            let schema_id: [u8; 16] = metadata[0..16].try_into()?;
            let table_id: [u8; 16] = metadata[16..32].try_into()?;
            let column_id: [u8; 16] = metadata[32..48].try_into()?;
            let alternate_column_id: [u8; 16] = metadata[48..64].try_into()?;
            let row_id: [u8; 16] = metadata[64..80].try_into()?;
            let schema_version = u64::from_le_bytes(metadata[80..88].try_into()?);
            let before_ddl_sequence = u64::from_le_bytes(metadata[88..96].try_into()?);
            let prepared_index_id: [u8; 16] = metadata[96..112].try_into()?;
            let before_table_epoch = u64::from_le_bytes(metadata[112..120].try_into()?);
            let reopened = Database::open(&database_path)?;
            let _validated_storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
            let read = reopened.begin_read()?;
            let published_index = lookup_index_id_by_name(&read, schema_id, "crash_index")?;
            let recovered_sequence = commit_sequence(&reopened)?;
            let outcome = read
                .open_table(schema::OUTCOMES)?
                .get(&INDEX_INVOCATION_ID[..])?
                .map(|value| InvocationOutcome::decode(value.value()))
                .transpose()?;
            let audit_exists = read
                .open_table(schema::AUDIT)?
                .get(&INDEX_AUDIT_ID[..])?
                .is_some();
            let row_key = table_row_key(table_id, row_id);
            assert_eq!(
                row(&reopened, &row_key)?.unwrap().payload,
                encode_tuple(
                    &[
                        ColumnSchema {
                            scalar_type: ScalarType::Integer,
                            nullable: false,
                        },
                        ColumnSchema {
                            scalar_type: ScalarType::Integer,
                            nullable: false,
                        },
                    ],
                    schema_version,
                    &[Some(ScalarValue::Integer(7)), Some(ScalarValue::Integer(8)),],
                )?
            );

            let committed = boundary == "after_commit";
            let should_index_exist = if operation == "create" {
                committed
            } else if operation == "rebuild" {
                true
            } else {
                !committed
            };
            assert_eq!(published_index.is_some(), should_index_exist);
            assert_eq!(audit_exists, committed);
            assert_eq!(
                recovered_sequence,
                before_ddl_sequence + u64::from(committed)
            );
            if committed {
                let outcome = outcome.ok_or("committed index DDL outcome missing")?;
                assert_eq!(outcome.status, OutcomeStatus::Committed);
                assert_eq!(outcome.commit_sequence, recovered_sequence);
            } else {
                assert_eq!(outcome, None);
            }
            let table = read
                .open_table(CATALOG)?
                .get(id_catalog_key(5, table_id).as_slice())?
                .ok_or("index crash table missing after reopen")?;
            let table_epoch = u64::from_le_bytes(
                decode_registered_envelope(table.value())?
                    .fields
                    .iter()
                    .find(|field| field.tag == 3)
                    .ok_or("index crash table epoch missing after reopen")?
                    .payload
                    .as_slice()
                    .try_into()?,
            );
            assert_eq!(table_epoch, before_table_epoch + u64::from(committed));

            let index_id = published_index.unwrap_or(prepared_index_id);
            if index_id != [0; 16] {
                let indexed_column_id = if operation == "rebuild" && committed {
                    alternate_column_id
                } else {
                    column_id
                };
                let index_key = prdb_engine::index_key::derive_index_key(
                    index_id,
                    row_id,
                    schema_version,
                    &[column_id, alternate_column_id],
                    &[
                        ColumnSchema {
                            scalar_type: ScalarType::Integer,
                            nullable: false,
                        },
                        ColumnSchema {
                            scalar_type: ScalarType::Integer,
                            nullable: false,
                        },
                    ],
                    &encode_tuple(
                        &[
                            ColumnSchema {
                                scalar_type: ScalarType::Integer,
                                nullable: false,
                            },
                            ColumnSchema {
                                scalar_type: ScalarType::Integer,
                                nullable: false,
                            },
                        ],
                        schema_version,
                        &[Some(ScalarValue::Integer(7)), Some(ScalarValue::Integer(8))],
                    )?,
                    &[indexed_column_id],
                )?;
                let locator = read
                    .open_table(schema::INDEXES)?
                    .get(index_key.key.as_slice())?
                    .map(|value| value.value().to_vec());
                let mut reverse_key = row_key.to_vec();
                reverse_key.extend_from_slice(&index_key.key[..index_key.key.len() - 16]);
                let reverse_exists = read
                    .open_table(schema::ROW_INDEXES)?
                    .get(reverse_key.as_slice())?
                    .is_some();
                assert_eq!(locator.is_some(), should_index_exist);
                assert_eq!(reverse_exists, should_index_exist);
                assert_eq!(
                    read.open_table(CATALOG)?
                        .get(id_catalog_key(9, index_id).as_slice())?
                        .is_some(),
                    should_index_exist
                );
                assert_eq!(
                    read.open_table(CATALOG)?
                        .get(id_catalog_key(2, index_id).as_slice())?
                        .is_some(),
                    should_index_exist
                );
                assert_eq!(
                    read.open_table(CATALOG)?
                        .get(dependency_write(index_id, schema_id, 1).key.as_slice())?
                        .is_some(),
                    should_index_exist
                );
                if let Some(locator) = locator {
                    assert_eq!(
                        IndexLocator::decode_for_key(&index_key.key, &locator)?.row_version,
                        row(&reopened, &row_key)?
                            .ok_or("index DDL row missing")?
                            .row_version
                    );
                }
                if operation == "rebuild" {
                    let old_key = prdb_engine::index_key::derive_index_key(
                        index_id,
                        row_id,
                        schema_version,
                        &[column_id, alternate_column_id],
                        &[
                            ColumnSchema {
                                scalar_type: ScalarType::Integer,
                                nullable: false,
                            },
                            ColumnSchema {
                                scalar_type: ScalarType::Integer,
                                nullable: false,
                            },
                        ],
                        &encode_tuple(
                            &[
                                ColumnSchema {
                                    scalar_type: ScalarType::Integer,
                                    nullable: false,
                                },
                                ColumnSchema {
                                    scalar_type: ScalarType::Integer,
                                    nullable: false,
                                },
                            ],
                            schema_version,
                            &[Some(ScalarValue::Integer(7)), Some(ScalarValue::Integer(8))],
                        )?,
                        &[column_id],
                    )?;
                    let old_locator_exists = read
                        .open_table(schema::INDEXES)?
                        .get(old_key.key.as_slice())?
                        .is_some();
                    let mut old_reverse_key = row_key.to_vec();
                    old_reverse_key.extend_from_slice(&old_key.key[..old_key.key.len() - 16]);
                    let old_reverse_exists = read
                        .open_table(schema::ROW_INDEXES)?
                        .get(old_reverse_key.as_slice())?
                        .is_some();
                    assert_eq!(old_locator_exists, !committed);
                    assert_eq!(old_reverse_exists, !committed);
                }
            }
        }
    }
    Ok(())
}

pub(super) fn process_crash_during_table_rename_recovers_all_or_none()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{CreateTableRequest, TableColumnDefinition};
    use std::process::Command;

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_CRASH_DURING_RENAME_DATABASE";
    const CHILD_BOUNDARY_ENV: &str = "PRDB_TEST_CRASH_DURING_RENAME_BOUNDARY";
    const CHILD_OPERATION_ENV: &str = "PRDB_TEST_CRASH_DURING_RENAME_OPERATION";
    const CHILD_EXIT_CODE: i32 = 87;
    const POST_COMMIT_EXIT_CODE: i32 = 88;
    const ACTOR_ID: [u8; 16] = [0xc1; 16];
    const RENAME_INVOCATION_ID: [u8; 16] = [0xc2; 16];
    const RENAME_AUDIT_ID: [u8; 16] = [0xc3; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database_path = std::path::PathBuf::from(database_path);
        let database = initialize(&database_path)?;
        let storage = StorageManager::new(&database, Duration::from_millis(100))?;
        let admin_role = builtin_owner_id();
        let setup = [
            principal_write_with_name(ACTOR_ID, 1, 1, "rename_crash_actor"),
            object_write_with_name(ACTOR_ID, 8, "rename_crash_actor"),
            membership_write(ACTOR_ID, admin_role),
            dependency_write(ACTOR_ID, admin_role, 5),
        ];
        let mut setup_audit = identity_audit_event(0xcb, admin_role, id(0xcc));
        setup_audit.action = AuditAction::AuthorityChange;
        storage.commit_catalog_and_audit(id(0xcc), admin_role, i64::MAX, &setup, &[setup_audit])?;
        let schema_invocation = id(0xc4);
        let mut schema_audit = identity_audit_event(0xc5, ACTOR_ID, id(0xc6));
        schema_audit.correlation_id = schema_invocation;
        let schema = storage.create_schema(
            schema_invocation,
            ACTOR_ID,
            "rename_crash_schema",
            ACTOR_ID,
            i64::MAX,
            schema_audit,
        )?;
        let table_invocation = id(0xc7);
        let mut table_audit = identity_audit_event(0xc8, ACTOR_ID, id(0xc9));
        table_audit.correlation_id = table_invocation;
        let table = storage.create_table(CreateTableRequest {
            invocation_id: table_invocation,
            authenticated_actor_id: ACTOR_ID,
            schema_id: schema.schema_id,
            name: "before_rename".to_owned(),
            columns: vec![TableColumnDefinition {
                name: "value".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            }],
            retention_deadline_us: i64::MAX,
            audit_event: table_audit,
        })?;
        let operation = std::env::var(CHILD_OPERATION_ENV)?;
        let before_rename_sequence = commit_sequence(&database)?;
        let mut metadata = Vec::with_capacity(56);
        metadata.extend_from_slice(&schema.schema_id);
        metadata.extend_from_slice(&table.table_id);
        metadata.extend_from_slice(&table.column_ids[0]);
        metadata.extend_from_slice(&before_rename_sequence.to_le_bytes());
        std::fs::write(
            database_path.with_extension("rename-crash-metadata"),
            metadata,
        )?;
        drop(storage);
        drop(database);

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&database_path)?;
        let armed = Arc::new(AtomicBool::new(false));
        let instrumented = Database::builder().create_with_backend(ExitAfterNextWriteBackend {
            inner: redb::backends::FileBackend::new(file)?,
            armed: armed.clone(),
            exit_code: CHILD_EXIT_CODE,
        })?;
        let storage = StorageManager::new(&instrumented, Duration::from_millis(100))?;
        let mut rename_audit = identity_audit_event(RENAME_AUDIT_ID[0], ACTOR_ID, id(0xca));
        rename_audit.event_id = RENAME_AUDIT_ID;
        rename_audit.correlation_id = RENAME_INVOCATION_ID;
        let boundary = std::env::var(CHILD_BOUNDARY_ENV)?;
        if boundary == "write" {
            armed.store(true, Ordering::SeqCst);
        } else if boundary != "after_commit" {
            return Err(format!("unknown table rename crash boundary {boundary}").into());
        }
        let rename_result = match operation.as_str() {
            "table" => storage
                .rename_table(
                    RENAME_INVOCATION_ID,
                    ACTOR_ID,
                    table.table_id,
                    "after_rename",
                    i64::MAX,
                    rename_audit,
                )
                .map(|_| ())
                .map_err(|error| Box::new(error) as Box<dyn std::error::Error>),
            "column" => storage
                .rename_column(
                    RENAME_INVOCATION_ID,
                    ACTOR_ID,
                    table.table_id,
                    table.column_ids[0],
                    "after_rename",
                    i64::MAX,
                    rename_audit,
                )
                .map(|_| ())
                .map_err(|error| Box::new(error) as Box<dyn std::error::Error>),
            "schema" => storage
                .rename_schema(
                    RENAME_INVOCATION_ID,
                    ACTOR_ID,
                    schema.schema_id,
                    "after_rename",
                    i64::MAX,
                    rename_audit,
                )
                .map(|_| ())
                .map_err(|error| Box::new(error) as Box<dyn std::error::Error>),
            _ => return Err(format!("unknown rename crash operation {operation}").into()),
        };
        if boundary == "write" {
            return Err(format!(
                "{operation} rename returned without injected backend-write exit: {rename_result:?}"
            )
            .into());
        }
        rename_result?;
        std::process::exit(POST_COMMIT_EXIT_CODE);
    }

    for operation in ["schema", "table", "column"] {
        for (boundary, expected_exit) in [
            ("write", CHILD_EXIT_CODE),
            ("after_commit", POST_COMMIT_EXIT_CODE),
        ] {
            let fixture = Fixture::new();
            let database_path = fixture.path();
            let child_status = Command::new(std::env::current_exe()?)
                .arg("--exact")
                .arg("process_crash_during_table_rename_recovers_all_or_none")
                .arg("--nocapture")
                .env(CHILD_DATABASE_ENV, &database_path)
                .env(CHILD_BOUNDARY_ENV, boundary)
                .env(CHILD_OPERATION_ENV, operation)
                .status()?;
            assert_eq!(
                child_status.code(),
                Some(expected_exit),
                "{operation} rename child missed the {boundary} crash boundary: {child_status}"
            );

            let metadata = std::fs::read(database_path.with_extension("rename-crash-metadata"))?;
            assert_eq!(metadata.len(), 56);
            let schema_id: [u8; 16] = metadata[0..16].try_into()?;
            let table_id: [u8; 16] = metadata[16..32].try_into()?;
            let column_id: [u8; 16] = metadata[32..48].try_into()?;
            let before_rename_sequence = u64::from_le_bytes(metadata[48..56].try_into()?);
            let reopened = Database::open(&database_path)?;
            let _validated_storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
            let read = reopened.begin_read()?;
            let sequence = commit_sequence(&reopened)?;
            let outcome = read
                .open_table(schema::OUTCOMES)?
                .get(&RENAME_INVOCATION_ID[..])?
                .map(|value| InvocationOutcome::decode(value.value()))
                .transpose()?;
            let audit_exists = read
                .open_table(schema::AUDIT)?
                .get(&RENAME_AUDIT_ID[..])?
                .is_some();
            let committed = boundary == "after_commit";
            if operation == "schema" {
                assert_eq!(
                    lookup_schema_id_by_name(&read, "rename_crash_schema")?,
                    (!committed).then_some(schema_id)
                );
                assert_eq!(
                    lookup_schema_id_by_name(&read, "after_rename")?,
                    committed.then_some(schema_id)
                );
            } else if operation == "table" {
                assert_eq!(
                    lookup_relation_id_by_name(&read, schema_id, "before_rename")?,
                    (!committed).then_some(table_id)
                );
                assert_eq!(
                    lookup_relation_id_by_name(&read, schema_id, "after_rename")?,
                    committed.then_some(table_id)
                );
            } else {
                assert_eq!(
                    lookup_column_id_by_name(&read, table_id, "value")?,
                    (!committed).then_some(column_id)
                );
                assert_eq!(
                    lookup_column_id_by_name(&read, table_id, "after_rename")?,
                    committed.then_some(column_id)
                );
                assert_eq!(
                    lookup_column_id_by_ordinal(&read, table_id, 0)?,
                    Some(column_id)
                );
            }
            assert_eq!(sequence, before_rename_sequence + u64::from(committed));
            assert_eq!(audit_exists, committed);
            if committed {
                let outcome = outcome.ok_or("committed rename outcome was not recovered")?;
                assert_eq!(outcome.status, OutcomeStatus::Committed);
                assert_eq!(outcome.commit_sequence, sequence);
            } else {
                assert_eq!(outcome, None);
            }
        }
    }
    Ok(())
}

pub(super) fn process_crash_during_schema_create_or_drop_recovers_all_or_none()
-> Result<(), Box<dyn std::error::Error>> {
    use std::process::Command;

    const CHILD_DATABASE_ENV: &str = "PRDB_TEST_CRASH_DURING_SCHEMA_DDL_DATABASE";
    const CHILD_BOUNDARY_ENV: &str = "PRDB_TEST_CRASH_DURING_SCHEMA_DDL_BOUNDARY";
    const CHILD_OPERATION_ENV: &str = "PRDB_TEST_CRASH_DURING_SCHEMA_DDL_OPERATION";
    const CHILD_EXIT_CODE: i32 = 87;
    const POST_COMMIT_EXIT_CODE: i32 = 88;
    const ACTOR_ID: [u8; 16] = [0xe1; 16];
    const SCHEMA_INVOCATION_ID: [u8; 16] = [0xe2; 16];
    const SCHEMA_AUDIT_ID: [u8; 16] = [0xe3; 16];
    const PREPARE_INVOCATION_ID: [u8; 16] = [0xe4; 16];
    const PREPARE_AUDIT_ID: [u8; 16] = [0xe5; 16];

    if let Some(database_path) = std::env::var_os(CHILD_DATABASE_ENV) {
        let database_path = std::path::PathBuf::from(database_path);
        let database = initialize(&database_path)?;
        let storage = StorageManager::new(&database, Duration::from_millis(100))?;
        let admin_role = builtin_owner_id();
        let setup = [
            principal_write_with_name(ACTOR_ID, 1, 1, "schema_crash_actor"),
            object_write_with_name(ACTOR_ID, 8, "schema_crash_actor"),
            membership_write(ACTOR_ID, admin_role),
            dependency_write(ACTOR_ID, admin_role, 5),
        ];
        let mut setup_audit = identity_audit_event(0xe6, admin_role, id(0xe7));
        setup_audit.action = AuditAction::AuthorityChange;
        storage.commit_catalog_and_audit(id(0xe7), admin_role, i64::MAX, &setup, &[setup_audit])?;

        let operation = std::env::var(CHILD_OPERATION_ENV)?;
        let prepared_schema_id = if operation == "drop" {
            let mut prepare_audit = identity_audit_event(PREPARE_AUDIT_ID[0], ACTOR_ID, id(0xe8));
            prepare_audit.event_id = PREPARE_AUDIT_ID;
            prepare_audit.correlation_id = PREPARE_INVOCATION_ID;
            Some(
                storage
                    .create_schema(
                        PREPARE_INVOCATION_ID,
                        ACTOR_ID,
                        "schema_crash_name",
                        ACTOR_ID,
                        i64::MAX,
                        prepare_audit,
                    )?
                    .schema_id,
            )
        } else if operation == "create" {
            None
        } else {
            return Err(format!("unknown schema DDL operation {operation}").into());
        };
        let before_ddl_sequence = commit_sequence(&database)?;
        let metadata_path = database_path.with_extension("schema-ddl-crash-metadata");
        let mut metadata = Vec::with_capacity(24);
        metadata.extend_from_slice(&before_ddl_sequence.to_le_bytes());
        metadata.extend_from_slice(&prepared_schema_id.unwrap_or([0; 16]));
        std::fs::write(&metadata_path, metadata)?;
        drop(storage);
        drop(database);

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&database_path)?;
        let armed = Arc::new(AtomicBool::new(false));
        let instrumented = Database::builder().create_with_backend(ExitAfterNextWriteBackend {
            inner: redb::backends::FileBackend::new(file)?,
            armed: armed.clone(),
            exit_code: CHILD_EXIT_CODE,
        })?;
        let storage = StorageManager::new(&instrumented, Duration::from_millis(100))?;
        let metadata = std::fs::read(&metadata_path)?;
        let prepared_schema_id: [u8; 16] = metadata[8..24].try_into()?;
        let mut schema_audit = identity_audit_event(SCHEMA_AUDIT_ID[0], ACTOR_ID, id(0xe9));
        schema_audit.event_id = SCHEMA_AUDIT_ID;
        schema_audit.correlation_id = SCHEMA_INVOCATION_ID;
        let boundary = std::env::var(CHILD_BOUNDARY_ENV)?;
        if boundary == "write" {
            armed.store(true, Ordering::SeqCst);
        } else if boundary != "after_commit" {
            return Err(format!("unknown schema DDL crash boundary {boundary}").into());
        }
        let result = if operation == "create" {
            storage
                .create_schema(
                    SCHEMA_INVOCATION_ID,
                    ACTOR_ID,
                    "schema_crash_name",
                    ACTOR_ID,
                    i64::MAX,
                    schema_audit,
                )
                .map(|_| ())
        } else {
            storage
                .drop_schema(
                    SCHEMA_INVOCATION_ID,
                    ACTOR_ID,
                    prepared_schema_id,
                    i64::MAX,
                    schema_audit,
                )
                .map(|_| ())
        };
        if boundary == "write" {
            return Err(format!(
                "schema {operation} returned without injected backend-write exit: {result:?}"
            )
            .into());
        }
        result?;
        std::process::exit(POST_COMMIT_EXIT_CODE);
    }

    for operation in ["create", "drop"] {
        for (boundary, expected_exit) in [
            ("write", CHILD_EXIT_CODE),
            ("after_commit", POST_COMMIT_EXIT_CODE),
        ] {
            let fixture = Fixture::new();
            let database_path = fixture.path();
            let child_status = Command::new(std::env::current_exe()?)
                .arg("--exact")
                .arg("process_crash_during_schema_create_or_drop_recovers_all_or_none")
                .arg("--nocapture")
                .env(CHILD_DATABASE_ENV, &database_path)
                .env(CHILD_BOUNDARY_ENV, boundary)
                .env(CHILD_OPERATION_ENV, operation)
                .status()?;
            assert_eq!(
                child_status.code(),
                Some(expected_exit),
                "schema {operation} child missed the {boundary} crash boundary: {child_status}"
            );

            let metadata =
                std::fs::read(database_path.with_extension("schema-ddl-crash-metadata"))?;
            assert_eq!(metadata.len(), 24);
            let before_ddl_sequence = u64::from_le_bytes(metadata[0..8].try_into()?);
            let prepared_schema_id: [u8; 16] = metadata[8..24].try_into()?;
            let reopened = Database::open(&database_path)?;
            let _validated_storage = StorageManager::new(&reopened, Duration::from_millis(100))?;
            let read = reopened.begin_read()?;
            let published_schema_id = lookup_schema_id_by_name(&read, "schema_crash_name")?;
            let committed = boundary == "after_commit";
            let should_exist = if operation == "create" {
                committed
            } else {
                !committed
            };
            assert_eq!(published_schema_id.is_some(), should_exist);
            let schema_id = published_schema_id.unwrap_or(prepared_schema_id);
            if schema_id != [0; 16] {
                assert_eq!(
                    read.open_table(CATALOG)?
                        .get(id_catalog_key(3, schema_id).as_slice())?
                        .is_some(),
                    should_exist
                );
                assert_eq!(
                    read.open_table(CATALOG)?
                        .get(id_catalog_key(2, schema_id).as_slice())?
                        .is_some(),
                    should_exist
                );
            }
            let sequence = commit_sequence(&reopened)?;
            assert_eq!(sequence, before_ddl_sequence + u64::from(committed));
            let outcome = read
                .open_table(schema::OUTCOMES)?
                .get(&SCHEMA_INVOCATION_ID[..])?
                .map(|value| InvocationOutcome::decode(value.value()))
                .transpose()?;
            let audit_exists = read
                .open_table(schema::AUDIT)?
                .get(&SCHEMA_AUDIT_ID[..])?
                .is_some();
            assert_eq!(audit_exists, committed);
            if committed {
                let outcome = outcome.ok_or("committed schema DDL outcome was not recovered")?;
                assert_eq!(outcome.status, OutcomeStatus::Committed);
                assert_eq!(outcome.commit_sequence, sequence);
            } else {
                assert_eq!(outcome, None);
            }
        }
    }
    Ok(())
}
