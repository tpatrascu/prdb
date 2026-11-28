use prdb_engine::audit::AuditAction;
use prdb_engine::schema::CATALOG;
use prdb_engine::storage_manager::StorageManager;
use redb::ReadableDatabase;
use std::time::Duration;

use super::helpers::*;

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(super) fn sandboxed_optimizer_metadata_submission_cancels_over_tcp()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::client_submit::{
        AdminClient, CancellationResult, ClientResultEvent, SubmissionClient, TerminalOutcome,
    };
    use prdb_engine::gateway::serve_tcp_client_connection;
    use prdb_engine::outcome::OutcomeStatus;
    use prdb_engine::storage_manager::{CreateTableRequest, TableColumnDefinition};
    use prdb_engine::submission_manifest::{
        ArtifactCompatibilityProfile, VersionedSubmissionMetadataValidator, encode_manifest_v1,
        manifest_v1_from_elf,
    };
    use prdb_engine::supervisor::{StagingLimits, StagingStore};
    use prdb_engine::supervisor_ipc::{
        GatewayAdmissionConfig, GatewayClientServiceEvent, GatewayExecutorConfig,
        GatewayTerminalDecision,
        observe_executor_relay_and_finalize_gateway_with_optimizer_planner_observer,
        serve_gateway_client_session_with_executor,
    };
    use prdb_engine::tuple::ScalarType;
    use sha2::{Digest, Sha256};
    use std::collections::HashSet;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixStream;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn required_paths(name: &str) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
        let value = std::env::var_os(name)
            .ok_or_else(|| format!("{name} must list the sandbox runtime paths"))?;
        if name == "PRDB_TEST_RUNTIME_READ_ONLY" && value.is_empty() {
            return Ok(Vec::new());
        }
        let paths = std::env::split_paths(&value).collect::<Vec<_>>();
        if paths.is_empty() || paths.iter().any(|path| !path.is_absolute()) {
            return Err(format!("{name} must contain absolute paths").into());
        }
        Ok(paths)
    }

    let cgroup_parent = std::env::var_os("PRDB_TEST_CGROUP_PARENT")
        .map(PathBuf::from)
        .ok_or("PRDB_TEST_CGROUP_PARENT must name a delegated cgroup v2 parent")?;
    let artifact_path = std::env::var_os("PRDB_TEST_RESTRICTED_METADATA_QUERY_ARTIFACT")
        .map(PathBuf::from)
        .ok_or("PRDB_TEST_RESTRICTED_METADATA_QUERY_ARTIFACT must name the TinyCC fixture")?;
    if !cgroup_parent.is_absolute() || !artifact_path.is_absolute() {
        return Err("sandbox test paths must be absolute".into());
    }
    let artifact = std::fs::read(artifact_path)?;
    let manifest = manifest_v1_from_elf(
        &artifact,
        1,
        1,
        "x86_64-v1".to_owned(),
        "nixos-x86_64-glibc-2.42".to_owned(),
        "TinyCC integration test".to_owned(),
    )
    .map_err(|error| format!("manifest inspection failed: {error:?}"))?;
    let manifest_bytes = encode_manifest_v1(&manifest)
        .map_err(|error| format!("manifest encoding failed: {error:?}"))?;
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let sdk_release_dir = std::env::var_os("PRDB_TEST_INSTALLED_SDK_RELEASE_DIR")
        .map(PathBuf::from)
        .ok_or("PRDB_TEST_INSTALLED_SDK_RELEASE_DIR must name the installed SDK")?;
    let sdk_repository = Arc::new(
        prdb_engine::sdk_registry::SdkLibraryRepository::open(
            &fixture.path().parent().unwrap().join("sdk-libraries"),
            "linux-x86_64".to_owned(),
            manifest.cpu_baseline.clone(),
            manifest.runtime_profile.clone(),
            ["libc.so.6".to_owned()].into_iter().collect(),
            128 * 1024 * 1024,
            None,
        )
        .map_err(|error| format!("open SDK repository: {error:?}"))?,
    );
    sdk_repository
        .install(
            &std::fs::read(sdk_release_dir.join("sdk-release.json"))?,
            &std::fs::read(sdk_release_dir.join("libprdb_sql.so.1"))?,
            &["libc.so.6".to_owned()].into_iter().collect(),
        )
        .map_err(|error| format!("install SDK release: {error:?}"))?;
    let validator = VersionedSubmissionMetadataValidator::new(
        ArtifactCompatibilityProfile::for_repository(
            manifest.cpu_baseline.clone(),
            manifest.runtime_profile.clone(),
            manifest
                .dependencies
                .iter()
                .cloned()
                .collect::<HashSet<_>>(),
        )
        .map_err(|error| format!("compatibility profile failed: {error:?}"))?,
    )
    .with_sdk_registry(sdk_repository)
    .map_err(|error| format!("configure SDK repository: {error:?}"))?;

    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let user_id = id(0xc0);
    let token_id = id(0xc1);
    let secret = [0x53; 32];
    let admin_token_id = id(0xce);
    let admin_secret = [0x54; 32];
    let restricted_user_id = id(0xcd);
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0xc2, admin_role, user_id);
    setup_audit.action = AuditAction::AuthorityChange;
    setup_audit.correlation_id = id(0xc2);
    storage
        .commit_catalog_and_audit(
            id(0xc2),
            admin_role,
            now + 3_600_000_000,
            &[
                principal_write_with_name(user_id, 1, 1, "tcp_submission_admin"),
                object_write_with_name(user_id, 8, "tcp_submission_admin"),
                principal_write_with_name(restricted_user_id, 1, 1, "tcp_submission_user"),
                object_write_with_name(restricted_user_id, 8, "tcp_submission_user"),
                credential_verifier_write(
                    token_id,
                    restricted_user_id,
                    &secret,
                    now - 1_000_000,
                    now + 60_000_000,
                    None,
                    1,
                ),
                credential_verifier_write(
                    admin_token_id,
                    user_id,
                    &admin_secret,
                    now - 1_000_000,
                    now + 60_000_000,
                    None,
                    1,
                ),
                membership_write(user_id, admin_role),
                dependency_write(user_id, admin_role, 5),
            ],
            &[setup_audit],
        )
        .map_err(|error| format!("TCP identity setup failed: {error:?}"))?;

    let empty_schema_invocation = id(0xc3);
    let mut empty_schema_audit = identity_audit_event(0xc4, user_id, id(0xc5));
    empty_schema_audit.correlation_id = empty_schema_invocation;
    let empty_schema = storage
        .create_schema(
            empty_schema_invocation,
            user_id,
            "empty_roundtrip",
            user_id,
            now + 3_600_000_000,
            empty_schema_audit,
        )
        .map_err(|error| format!("TCP empty schema creation failed: {error:?}"))?;
    let hidden_schema_invocation = id(0xc6);
    let mut hidden_schema_audit = identity_audit_event(0xc7, user_id, id(0xc8));
    hidden_schema_audit.correlation_id = hidden_schema_invocation;
    let hidden_schema = storage
        .create_schema(
            hidden_schema_invocation,
            user_id,
            "sql_roundtrip",
            user_id,
            now + 3_600_000_000,
            hidden_schema_audit,
        )
        .map_err(|error| format!("TCP hidden schema creation failed: {error:?}"))?;
    let hidden_table_invocation = id(0xcb);
    let mut hidden_table_audit = identity_audit_event(0xcc, user_id, hidden_schema.schema_id);
    hidden_table_audit.correlation_id = hidden_table_invocation;
    storage.create_table(CreateTableRequest {
        invocation_id: hidden_table_invocation,
        authenticated_actor_id: user_id,
        schema_id: hidden_schema.schema_id,
        name: "records".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "id".to_owned(),
            scalar_type: ScalarType::Integer,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us: now + 3_600_000_000,
        audit_event: hidden_table_audit,
    })?;
    let grant_invocation = id(0xc9);
    let mut grant_audit = identity_audit_event(0xca, user_id, empty_schema.schema_id);
    grant_audit.action = AuditAction::AuthorityChange;
    grant_audit.correlation_id = grant_invocation;
    let (old_user_record, old_token_record) = {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        (
            catalog
                .get(id_catalog_key(10, restricted_user_id).as_slice())?
                .ok_or("TCP restricted submission user record missing")?
                .value()
                .to_vec(),
            catalog
                .get(id_catalog_key(16, token_id).as_slice())?
                .ok_or("TCP submission token record missing")?
                .value()
                .to_vec(),
        )
    };
    let mut authority_writes = vec![
        grant_write(
            restricted_user_id,
            empty_schema.schema_id,
            1,
            1,
            empty_schema.schema_id,
            user_id,
        ),
        dependency_write(restricted_user_id, empty_schema.schema_id, 5),
    ];
    let mut bumped_user =
        principal_write_with_name(restricted_user_id, 1, 2, "tcp_submission_user");
    bumped_user.expected_value = Some(old_user_record);
    authority_writes.push(bumped_user);
    let mut refreshed_token = credential_verifier_write(
        token_id,
        restricted_user_id,
        &secret,
        now - 1_000_000,
        now + 60_000_000,
        None,
        2,
    );
    refreshed_token.expected_value = Some(old_token_record);
    authority_writes.push(refreshed_token);
    storage
        .commit_catalog_and_audit(
            grant_invocation,
            user_id,
            now + 3_600_000_000,
            &authority_writes,
            &[grant_audit],
        )
        .map_err(|error| format!("TCP schema grant failed: {error:?}"))?;

    let requested_scope = [
        b"PSC1\0\x01".as_slice(),
        &[1, 1],
        empty_schema.schema_id.as_slice(),
    ]
    .concat();
    let package = fixture.0.join("package");
    std::fs::create_dir(&package)?;
    std::fs::set_permissions(&package, std::fs::Permissions::from_mode(0o700))?;
    std::fs::write(package.join("query.so"), &artifact)?;
    std::fs::write(package.join("manifest.pmf1"), &manifest_bytes)?;
    std::fs::write(package.join("requested_scope.psc1"), &requested_scope)?;
    let artifact_digest = Sha256::digest(&artifact)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
        + "\n";
    std::fs::write(package.join("artifact.sha256"), artifact_digest)?;
    for name in [
        "query.so",
        "manifest.pmf1",
        "requested_scope.psc1",
        "artifact.sha256",
    ] {
        std::fs::set_permissions(package.join(name), std::fs::Permissions::from_mode(0o600))?;
    }
    let staging = StagingStore::new(StagingLimits::default(), Arc::new(validator))?;
    let runtime = GatewayExecutorConfig {
        cgroup_parent,
        resource_limits: prdb_engine::linux_sandbox::WorkerResourceLimits {
            cpu_quota_micros: 500_000,
            cpu_period_micros: 100_000,
            memory_max_bytes: 256 * 1024 * 1024,
            max_processes: 32,
        },
        launch: prdb_engine::executor_launcher::ExecutorLaunchConfig {
            executor_path: PathBuf::from(env!("CARGO_BIN_EXE_prdb-executor")),
            optimizer_path: PathBuf::from(env!("CARGO_BIN_EXE_prdb-optimizer")),
            scratch_parent: std::env::temp_dir(),
            runtime_read_execute: required_paths("PRDB_TEST_RUNTIME_READ_EXECUTE")?,
            runtime_read_only: required_paths("PRDB_TEST_RUNTIME_READ_ONLY")?,
            spill_broker: None,
            max_total_result_bytes: prdb_engine::query_abi::MAX_TOTAL_RESULT_BYTES,
            max_sql_index_probe_candidates:
                prdb_engine::query_abi::DEFAULT_SQL_INDEX_PROBE_CANDIDATES,
            cleanup_timeout: Duration::from_secs(2),
        },
    };
    let admission = GatewayAdmissionConfig {
        retention_period_us: 3_600_000_000,
        max_rows: 8,
        max_bytes: 4096,
    };
    let (mut gateway_service, mut supervisor_service) = UnixStream::pair()?;
    let tcp_listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = tcp_listener.local_addr()?;
    let (mut control_gateway_service, mut control_supervisor_service) = UnixStream::pair()?;
    let control_tcp_listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let control_address = control_tcp_listener.local_addr()?;
    let receipt = fixture.0.join("invocation.receipt");
    let supervisor_start_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let optimizer_estimate_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let cancellation_result = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (estimate_pending_sender, estimate_pending_receiver) = std::sync::mpsc::channel();
    let cancellation_finished = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));

    let cancelled_invocation_id = std::thread::scope(
        |scope| -> Result<[u8; 16], Box<dyn std::error::Error>> {
            let gateway = scope.spawn(move || -> Result<(), String> {
                let (mut client_stream, _) = tcp_listener
                    .accept()
                    .map_err(|error| format!("TCP accept failed: {error}"))?;
                serve_tcp_client_connection(&mut client_stream, &mut gateway_service)
                    .map_err(|error| format!("TCP gateway failed: {error:?}"))?;
                Ok(())
            });
            let control_gateway = scope.spawn(move || -> Result<(), String> {
                let (mut client_stream, _) = control_tcp_listener
                    .accept()
                    .map_err(|error| format!("control TCP accept failed: {error}"))?;
                serve_tcp_client_connection(&mut client_stream, &mut control_gateway_service)
                    .map_err(|error| format!("control TCP gateway failed: {error:?}"))?;
                Ok(())
            });
            let control_supervisor = scope.spawn(|| -> Result<(), String> {
                let mut session = prdb_engine::client_protocol::ClientConnectionSession::default();
                match prdb_engine::supervisor_ipc::serve_gateway_client_session(
                    &mut control_supervisor_service,
                    &storage,
                    &staging,
                    &mut session,
                    admission,
                )
                .map_err(|error| format!("control supervisor session failed: {error:?}"))?
                {
                    GatewayClientServiceEvent::Disconnected => Ok(()),
                    GatewayClientServiceEvent::Started { .. } => {
                        Err("control session unexpectedly started a query".into())
                    }
                }
            });
            let cancellation_result_for_client = Arc::clone(&cancellation_result);
            let cancellation_finished_for_client = Arc::clone(&cancellation_finished);
            let control_canceller = scope.spawn(move || -> Result<(), String> {
                let mut token_secret = admin_secret;
                let mut client = AdminClient::connect_authenticated(
                    control_address,
                    admin_token_id,
                    &mut token_secret,
                )
                .map_err(|error| format!("control-mode TCP authentication failed: {error:?}"))?;
                let invocation_id = estimate_pending_receiver
                    .recv()
                    .map_err(|error| format!("pending estimate signal failed: {error}"))?;
                let result = client
                    .cancel_invocation(invocation_id)
                    .map_err(|error| format!("control-mode TCP CANCEL failed: {error:?}"))?;
                cancellation_result_for_client.store(
                    if result == CancellationResult::Accepted {
                        1
                    } else {
                        2
                    },
                    std::sync::atomic::Ordering::SeqCst,
                );
                let (finished, changed) = &*cancellation_finished_for_client;
                *finished.lock().map_err(|_| "cancellation latch poisoned")? = true;
                changed.notify_all();
                Ok(())
            });
            let supervisor = scope.spawn(|| -> Result<(), String> {
            let supervisor_start_count = Arc::clone(&supervisor_start_count);
            let optimizer_estimate_count = Arc::clone(&optimizer_estimate_count);
            let mut session = prdb_engine::client_protocol::ClientConnectionSession::default();
            loop {
                let event = serve_gateway_client_session_with_executor(
                    &mut supervisor_service,
                    &storage,
                    &staging,
                    &mut session,
                    admission,
                    &runtime,
                )
                .map_err(|error| format!("supervisor session failed: {error:?}"))?;
                match event {
                    GatewayClientServiceEvent::Disconnected => return Ok(()),
                    GatewayClientServiceEvent::Started {
                        request_id,
                        mut invocation,
                        executor: Some(mut executor),
                        ..
                    } => {
                        let invocation_index = supervisor_start_count
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let invocation_id = invocation.invocation_id();
                        let staging_owner = session
                            .staging_owner()
                            .map_err(|error| format!("TCP staging owner unavailable: {error:?}"))?;
                        let schemas = invocation
                            .scan_information_schema_schemata(128)
                            .map_err(|error| format!("TCP metadata API scan failed: {error:?}"))?
                            .rows;
                        if !schemas
                            .iter()
                            .any(|row| row.schema_name == "empty_roundtrip")
                            || schemas.iter().any(|row| row.schema_name == "sql_roundtrip")
                        {
                            return Err("TCP invocation metadata scope was not enforced".into());
                        }
                        let mut tables = Vec::new();
                        let mut table_cursor = None;
                        loop {
                            let page = invocation
                                .scan_information_schema_tables(128, table_cursor)
                                .map_err(|error| {
                                    format!("TCP metadata table API scan failed: {error:?}")
                                })?;
                            tables.extend(page.rows);
                            table_cursor = page.continuation;
                            if table_cursor.is_none() {
                                break;
                            }
                        }
                        if !tables
                            .iter()
                            .any(|row| row.table_schema == "information_schema")
                            || tables.iter().any(|row| row.table_schema == "sql_roundtrip")
                        {
                            return Err("TCP metadata table scope was not enforced".into());
                        }
                        let mut columns = Vec::new();
                        let mut column_cursor = None;
                        loop {
                            let page = invocation
                                .scan_information_schema_columns(128, column_cursor)
                                .map_err(|error| {
                                    format!("TCP metadata column API scan failed: {error:?}")
                                })?;
                            columns.extend(page.rows);
                            column_cursor = page.continuation;
                            if column_cursor.is_none() {
                                break;
                            }
                        }
                        if !columns
                            .iter()
                            .any(|row| row.table_schema == "information_schema")
                            || columns
                                .iter()
                                .any(|row| row.table_schema == "sql_roundtrip")
                        {
                            return Err("TCP metadata column scope was not enforced".into());
                        }
                        let estimate_count = Arc::clone(&optimizer_estimate_count);
                        let estimate_pending_sender = estimate_pending_sender.clone();
                        let cancellation_finished = Arc::clone(&cancellation_finished);
                        let cancellation_signal_sent = Arc::new(
                            std::sync::atomic::AtomicBool::new(false),
                        );
                        let decision = observe_executor_relay_and_finalize_gateway_with_optimizer_planner_observer(
                            &storage,
                            &mut invocation,
                            &mut executor,
                            &mut supervisor_service,
                            request_id,
                            move |operation, _request_id| {
                                if operation != prdb_engine::ipc::Operation::EstimateScan as u16
                                    && operation != prdb_engine::ipc::Operation::EstimateScans as u16
                                {
                                    return;
                                }
                                if invocation_index == 0 {
                                    estimate_count.fetch_add(
                                        1,
                                        std::sync::atomic::Ordering::Relaxed,
                                    );
                                } else if invocation_index == 1
                                    && !cancellation_signal_sent.swap(
                                        true,
                                        std::sync::atomic::Ordering::SeqCst,
                                    )
                                {
                                    let _ = estimate_pending_sender.send(invocation_id);
                                    let (finished, changed) = &*cancellation_finished;
                                    if let Ok(finished) = finished.lock() {
                                        let _ = changed.wait_timeout_while(
                                            finished,
                                            Duration::from_secs(5),
                                            |finished| !*finished,
                                        );
                                    }
                                }
                            },
                        )
                        .map_err(|error| format!("executor relay failed: {error:?}"))?;
                        match (invocation_index, decision) {
                            (0, GatewayTerminalDecision::Commit) => {}
                            (1, GatewayTerminalDecision::Abort(_)) => {}
                            (_, decision) => {
                                return Err(format!(
                                    "unexpected TCP submission outcome for START {invocation_index}: {decision:?}"
                                ));
                            }
                        }
                        staging
                            .finish_started(staging_owner, invocation_id)
                            .map_err(|error| {
                                format!("TCP staging cleanup failed: {error:?}")
                            })?;
                    }
                    GatewayClientServiceEvent::Started { executor: None, .. } => {
                        return Err("supervisor did not launch the sandbox executor".into());
                    }
                }
            }
        });

            let mut client_secret = secret;
            let mut client =
                SubmissionClient::connect_authenticated(address, token_id, &mut client_secret)?;
            assert_eq!(client_secret, [0; 32]);
            let invocation_id = client
                .upload_and_seal(&package, &[], 30_000)
                .map_err(|error| format!("first TCP upload/seal failed: {error:?}"))?;
            {
                let mut started = client
                    .start(invocation_id, &receipt)
                    .map_err(|error| format!("first TCP START failed: {error:?}"))?;
                assert_eq!(
                    started.next_event()?,
                    Some(ClientResultEvent::Data(b"empty_roundtrip".to_vec()))
                );
                assert!(matches!(
                    started.next_event()?,
                    Some(ClientResultEvent::Terminal(terminal))
                        if terminal.invocation_id == invocation_id
                            && terminal.outcome == TerminalOutcome::Committed
                            && terminal.reason_code == "success"
                ));
                assert_eq!(started.next_event()?, None);
            }
            assert_eq!(
                client.status(invocation_id)?,
                prdb_engine::client_submit::InvocationStatus::Committed
            );
            let cancelled_id = client
                .upload_and_seal(&package, &[], 30_000)
                .map_err(|error| format!("second TCP upload/seal failed: {error:?}"))?;
            let cancelled_receipt = fixture.0.join("cancelled-invocation.receipt");
            {
                let mut started = client
                    .start(cancelled_id, &cancelled_receipt)
                    .map_err(|error| format!("second TCP START failed: {error:?}"))?;
                assert!(matches!(
                    started.next_event()?,
                    Some(ClientResultEvent::Terminal(terminal))
                        if terminal.invocation_id == cancelled_id
                            && terminal.outcome == TerminalOutcome::Aborted
                            && terminal.reason_code != "success"
                ));
                assert_eq!(started.next_event()?, None);
            }
            assert_eq!(
                client.status(cancelled_id)?,
                prdb_engine::client_submit::InvocationStatus::Aborted
            );
            drop(client);

            gateway
                .join()
                .map_err(|_| std::io::Error::other("TCP gateway thread panicked"))?
                .map_err(std::io::Error::other)?;
            control_gateway
                .join()
                .map_err(|_| std::io::Error::other("control TCP gateway thread panicked"))?
                .map_err(std::io::Error::other)?;
            control_supervisor
                .join()
                .map_err(|_| std::io::Error::other("control supervisor thread panicked"))?
                .map_err(std::io::Error::other)?;
            control_canceller
                .join()
                .map_err(|_| std::io::Error::other("control canceller thread panicked"))?
                .map_err(std::io::Error::other)?;
            supervisor
                .join()
                .map_err(|_| std::io::Error::other("supervisor thread panicked"))?
                .map_err(std::io::Error::other)?;
            assert!(
                optimizer_estimate_count.load(std::sync::atomic::Ordering::Relaxed) > 0,
                "the committed TCP optimizer query did not dispatch an EstimateScan"
            );
            assert_eq!(
                cancellation_result.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "the second TCP optimizer query was not cancelled through control-mode TCP"
            );
            assert_eq!(
                storage
                    .lookup_invocation_outcome(invocation_id, restricted_user_id, now + 1)?
                    .ok_or("TCP submission outcome missing from storage")?
                    .status,
                OutcomeStatus::Committed
            );
            Ok(cancelled_id)
        },
    )?;
    assert_eq!(
        storage
            .lookup_invocation_outcome(cancelled_invocation_id, restricted_user_id, now + 3)?
            .ok_or("TCP cancelled invocation outcome missing from storage")?
            .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}
