use prdb_engine::audit::{AuditAction, AuditEvent, AuditResult};
use prdb_engine::catalog::{
    CatalogField, WireType, decode_registered_envelope, encode_envelope, family_schema,
};
use prdb_engine::commit::{CatalogWrite, CommitError, commit_catalog_and_audit};
use prdb_engine::credentials::hash_token_secret;
use prdb_engine::outcome::{OutcomeStatus, lookup_outcome};
use prdb_engine::schema::{CATALOG, PRINCIPAL_NAMES};
use prdb_engine::storage_manager::{StorageManager, StorageManagerConfig};
use redb::{ReadableDatabase, ReadableTableMetadata};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use super::helpers::*;

pub(super) fn supervisor_cancellation_and_deadline_during_worker_wait_abort()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::outcome::OutcomeStatus;
    use prdb_engine::storage_manager::{
        OwnerCancellationResult, SupervisorAdmissionRequest, SupervisorInvocationError,
    };
    use prdb_engine::transaction::TransactionError;
    use std::process::Command;
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
            principal_write_with_name(user_id, 1, 1, "worker_cancel_user"),
            object_write_with_name(user_id, 8, "worker_cancel_user"),
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
    let invocation = storage.admit_supervisor_invocation(
        id(0x73),
        token_id,
        &secret,
        now + 60_000_000,
        2,
        64,
    )?;
    let mut worker = Command::new("sleep").arg("30").spawn()?;

    let (observed, cancellation) = std::thread::scope(|scope| {
        let cancel = scope.spawn(|| {
            std::thread::sleep(Duration::from_millis(25));
            storage.cancel_owned_invocation(user_id, id(0x73), now + 1)
        });
        let observed = invocation.observe_worker_exit(&mut worker);
        let cancellation = cancel
            .join()
            .map_err(|_| std::io::Error::other("cancellation thread panicked"))??;
        Ok::<_, Box<dyn std::error::Error>>((observed, cancellation))
    })?;
    assert_eq!(cancellation, OwnerCancellationResult::Accepted);
    assert!(matches!(
        observed,
        Err(SupervisorInvocationError::Deadline(
            TransactionError::CancellationRequested
        ))
    ));
    assert!(worker.try_wait()?.is_some());
    assert_eq!(
        storage
            .lookup_invocation_outcome(id(0x73), user_id, now + 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );

    let expired = storage.admit_supervisor_invocation_with_deadline(
        SupervisorAdmissionRequest {
            invocation_id: id(0x74),
            token_id,
            retention_deadline_us: now + 3_600_000_000,
            max_rows: 2,
            max_bytes: 64,
            deadline: Some(std::time::Instant::now() + Duration::from_millis(40)),
            workload_class: prdb_engine::supervisor::WorkloadClass::Operational,
        },
        &secret,
    )?;
    let mut deadline_worker = Command::new("sleep").arg("30").spawn()?;
    assert!(matches!(
        expired.observe_worker_exit(&mut deadline_worker),
        Err(SupervisorInvocationError::Deadline(
            TransactionError::Expired
        ))
    ));
    assert!(deadline_worker.try_wait()?.is_some());
    assert_eq!(
        storage
            .lookup_invocation_outcome(id(0x74), user_id, now + 2)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

pub(super) fn supervisor_cancellation_cleans_descendants_from_worker_cgroup()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::outcome::OutcomeStatus;
    use prdb_engine::storage_manager::{OwnerCancellationResult, SupervisorInvocationError};
    use prdb_engine::transaction::TransactionError;
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

    let cgroup_parent = std::env::var_os("PRDB_TEST_CGROUP_PARENT")
        .ok_or("PRDB_TEST_CGROUP_PARENT is required for process-tree cancellation")?;
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let user_id = id(0x76);
    let token_id = id(0x77);
    let secret = [0x32; 32];
    let invocation_id = id(0x78);
    commit_catalog_and_audit(
        &database,
        id(0x79),
        id(9),
        now + 3_600_000_000,
        &[
            principal_write_with_name(user_id, 1, 1, "optimizer_cancel_user"),
            object_write_with_name(user_id, 8, "optimizer_cancel_user"),
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
        &[audit_event(0x79)],
    )?;
    let invocation = storage.admit_supervisor_invocation(
        invocation_id,
        token_id,
        &secret,
        now + 60_000_000,
        2,
        64,
    )?;
    let cgroup = prdb_engine::linux_sandbox::WorkerCgroup::create(
        cgroup_parent,
        invocation_id,
        prdb_engine::linux_sandbox::WorkerResourceLimits {
            cpu_quota_micros: 100_000,
            cpu_period_micros: 100_000,
            memory_max_bytes: 128 * 1024 * 1024,
            max_processes: 8,
        },
    )?;
    let descendant_pid_path = fixture.path().with_extension("worker-descendant.pid");
    let mut command = Command::new("sh");
    command
        .arg("-c")
        .arg("sleep 30 & echo $! > \"$1\"; wait")
        .arg("sh")
        .arg(&descendant_pid_path);
    cgroup.attach_command_before_exec(&mut command)?;
    let mut worker = command.spawn()?;
    let pid_deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !descendant_pid_path.exists() && std::time::Instant::now() < pid_deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    let descendant_pid = std::fs::read_to_string(&descendant_pid_path)
        .map_err(|_| "worker did not start its child process")?
        .trim()
        .parse::<u32>()?;

    let (observed, cancellation) = std::thread::scope(|scope| {
        let cancel = scope.spawn(|| {
            std::thread::sleep(Duration::from_millis(25));
            storage.cancel_owned_invocation(user_id, invocation_id, now + 1)
        });
        let observed =
            invocation.observe_worker_exit_in_cgroup(&mut worker, cgroup, Duration::from_secs(2));
        let cancellation = cancel
            .join()
            .map_err(|_| std::io::Error::other("cancellation thread panicked"))??;
        Ok::<_, Box<dyn std::error::Error>>((observed, cancellation))
    })?;
    assert_eq!(cancellation, OwnerCancellationResult::Accepted);
    assert!(matches!(
        observed,
        Err(SupervisorInvocationError::Deadline(
            TransactionError::CancellationRequested
        ))
    ));
    assert!(worker.try_wait()?.is_some());
    assert!(
        !std::path::Path::new(&format!("/proc/{descendant_pid}")).exists(),
        "worker child process {descendant_pid} survived cgroup cancellation"
    );
    assert_eq!(
        storage
            .lookup_invocation_outcome(invocation_id, user_id, now + 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

pub(super) fn cancellation_during_optimizer_predicate_estimate_kills_process_tree_without_fallback()
-> Result<(), Box<dyn std::error::Error>> {
    run_optimizer_predicate_estimate_termination_case(true)
}

pub(super) fn deadline_during_optimizer_predicate_estimate_kills_process_tree_without_fallback()
-> Result<(), Box<dyn std::error::Error>> {
    run_optimizer_predicate_estimate_termination_case(false)
}

pub(super) fn sealed_start_admits_only_current_credentials_and_enforces_request_deadline()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::client_protocol::{
        ClientConnectionSession, ClientFrame, ClientMode, ClientStartError,
    };
    use prdb_engine::supervisor::{
        ChunkKind, PrepareSubmission, StagingError, StagingLimits, StagingStore,
        SubmissionMetadata, SubmissionMetadataValidator, WorkloadClass,
    };
    use prdb_engine::transaction::TransactionError;
    use sha2::{Digest, Sha256};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct AcceptPrepared;
    impl SubmissionMetadataValidator for AcceptPrepared {
        fn supports_versions(&self, manifest_version: u32, scope_version: u32) -> bool {
            manifest_version == 1 && scope_version == 1
        }

        fn validate(&self, metadata: &SubmissionMetadata) -> bool {
            metadata.manifest == b"m" && metadata.requested_scope == requested_scope_fixture()
        }
    }

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let user_id = id(0x70);
    let token_id = id(0x71);
    let secret = [0x31; 32];
    let credential = credential_verifier_write(
        token_id,
        user_id,
        &secret,
        now - 1_000_000,
        now + 60_000_000,
        None,
        1,
    );
    let credential_value = credential.payload.clone().unwrap();
    commit_catalog_and_audit(
        &database,
        id(0x72),
        id(9),
        now + 3_600_000_000,
        &[
            principal_write_with_name(user_id, 1, 1, "start_user"),
            object_write_with_name(user_id, 8, "start_user"),
            credential,
        ],
        &[audit_event(0x72)],
    )?;
    let mut client = ClientConnectionSession::default();
    client.accept_hello(ClientMode::Query, "start-test")?;
    client.authenticate(&storage, token_id, &secret)?;
    let staging = StagingStore::new(StagingLimits::default(), Arc::new(AcceptPrepared))?;
    let owner = client.staging_owner()?;

    let artifact = b"x";
    let first = staging.prepare(
        owner,
        PrepareSubmission {
            manifest_version: 1,
            manifest: b"m".to_vec(),
            artifact_bytes: artifact.len() as u64,
            artifact_sha256: Sha256::digest(artifact).into(),
            scope_version: 1,
            requested_scope: requested_scope_fixture(),
            parameter_bytes: 0,
            parameter_sha256: Sha256::digest([]).into(),
            sql_data_bytes: 0,
            sql_data_sha256: Sha256::digest([]).into(),
            deadline_ms: 10_000,
            workload_class: WorkloadClass::Operational,
        },
    )?;
    staging.upload_chunk(owner, first.invocation_id, ChunkKind::Artifact, 0, artifact)?;
    staging.seal(owner, first.invocation_id)?;
    assert_eq!(
        storage.get_invocation_status(first.invocation_id, user_id, now)?,
        None
    );
    assert_eq!(
        lookup_outcome(&database, first.invocation_id, user_id, now)?,
        None
    );

    let invocation_hex = first
        .invocation_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let (accepted, admitted) = client.handle_start_request(
        &storage,
        &staging,
        ClientFrame {
            kind: 7,
            request_id: 1,
            response_sequence: 0,
            payload: format!("{{\"invocation_id\":\"{invocation_hex}\"}}").into_bytes(),
        },
        now + 3_600_000_000,
        2,
        64,
    )?;
    assert_eq!(accepted.payload, b"{\"state\":\"accepted\"}");
    assert_eq!(accepted.request_id, 1);
    let started_submission = staging.started_submission(owner, first.invocation_id)?;
    assert_eq!(started_submission.invocation_id(), first.invocation_id);
    assert_eq!(started_submission.artifact(), artifact);
    assert!(started_submission.parameters().is_empty());
    let mut cancellation_client = ClientConnectionSession::default();
    cancellation_client.accept_hello(ClientMode::Query, "worker-cancel-control")?;
    cancellation_client.authenticate(&storage, token_id, &secret)?;
    let cancelled = cancellation_client.handle_cancel_prepared_request(
        &storage,
        &staging,
        ClientFrame {
            kind: 9,
            request_id: 2,
            response_sequence: 0,
            payload: format!("{{\"invocation_id\":\"{invocation_hex}\"}}").into_bytes(),
        },
    )?;
    assert_eq!(cancelled.payload, b"{\"state\":\"accepted\"}");
    assert!(
        storage
            .get_invocation_status(first.invocation_id, user_id, now)?
            .is_some()
    );
    assert!(matches!(
        admitted.check_deadline(),
        Err(
            prdb_engine::storage_manager::SupervisorInvocationError::Deadline(
                TransactionError::CancellationRequested
            )
        )
    ));
    assert_eq!(
        lookup_outcome(&database, first.invocation_id, user_id, now)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    staging.finish_started(owner, first.invocation_id)?;
    drop(admitted);

    let second = staging.prepare(
        owner,
        PrepareSubmission {
            manifest_version: 1,
            manifest: b"m".to_vec(),
            artifact_bytes: artifact.len() as u64,
            artifact_sha256: Sha256::digest(artifact).into(),
            scope_version: 1,
            requested_scope: requested_scope_fixture(),
            parameter_bytes: 0,
            parameter_sha256: Sha256::digest([]).into(),
            sql_data_bytes: 0,
            sql_data_sha256: Sha256::digest([]).into(),
            deadline_ms: 10_000,
            workload_class: WorkloadClass::Operational,
        },
    )?;
    staging.upload_chunk(
        owner,
        second.invocation_id,
        ChunkKind::Artifact,
        0,
        artifact,
    )?;
    staging.seal(owner, second.invocation_id)?;
    let second_invocation_hex = second
        .invocation_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let cgroup_parent =
        std::env::temp_dir().join(format!("prdb-start-cgroup-{}", std::process::id()));
    std::fs::create_dir_all(&cgroup_parent)?;
    let executor = prdb_engine::supervisor_ipc::GatewayExecutorConfig {
        cgroup_parent: cgroup_parent.clone(),
        resource_limits: prdb_engine::linux_sandbox::WorkerResourceLimits {
            cpu_quota_micros: 100_000,
            cpu_period_micros: 100_000,
            memory_max_bytes: 64 * 1024 * 1024,
            max_processes: 16,
        },
        analytical_resource_limits: None,
        launch: prdb_engine::executor_launcher::ExecutorLaunchConfig {
            executor_path: std::env::current_exe()?,
            optimizer_path: std::env::current_exe()?,
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
    };
    assert!(matches!(
        prdb_engine::supervisor_ipc::dispatch_gateway_client_request_with_executor(
            prdb_engine::ipc::ServiceRole::Gateway,
            &storage,
            &staging,
            &mut client,
            &prdb_engine::ipc::IpcFrame {
                operation: prdb_engine::ipc::Operation::StartSubmission as u16,
                request_id: 3,
                payload: format!("{{\"invocation_id\":\"{second_invocation_hex}\"}}").into_bytes(),
            },
            prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
                retention_period_us: 3_600_000_000,
                max_rows: 2,
                max_bytes: 64,
            },
            &executor,
        ),
        Err(prdb_engine::supervisor_ipc::GatewayRequestError::Start(
            ClientStartError::ExecutorStart(prdb_engine::supervisor::StartError::Sandbox(_))
        ))
    ));
    assert_eq!(
        staging.status(owner, second.invocation_id)?,
        prdb_engine::supervisor::SubmissionState::Prepared
    );
    assert_eq!(
        storage.get_invocation_status(second.invocation_id, user_id, now)?,
        None
    );
    assert_eq!(
        lookup_outcome(&database, second.invocation_id, user_id, now)?,
        None
    );
    std::fs::remove_dir_all(cgroup_parent)?;

    let mut revoked = credential_verifier_write(
        token_id,
        user_id,
        &secret,
        now - 1_000_000,
        now + 60_000_000,
        Some(now),
        2,
    );
    revoked.expected_value = Some(credential_value);
    storage.commit_catalog_and_audit(
        id(0x73),
        id(9),
        now + 3_600_000_000,
        &[revoked],
        &[audit_event(0x73)],
    )?;
    assert!(matches!(
        client.start_submission(
            &storage,
            &staging,
            second.invocation_id,
            now + 3_600_000_000,
            2,
            64,
        ),
        Err(ClientStartError::Start(
            prdb_engine::supervisor::StartError::Admission(TransactionError::AuthenticationFailed)
        ))
    ));
    assert_eq!(
        lookup_outcome(&database, second.invocation_id, user_id, now)?,
        None
    );
    assert_eq!(
        storage.get_invocation_status(second.invocation_id, user_id, now)?,
        None
    );
    assert_eq!(
        staging.status(owner, second.invocation_id),
        Err(StagingError::Unavailable)
    );
    Ok(())
}

pub(super) fn storage_manager_issues_and_revokes_tokens_for_manage_users()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{
        TokenIssueRequest, TokenLifecycleError, TokenRotationRequest,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let admin_user = id(0x90);
    let user_manager = id(0x91);
    let target_user = id(0x92);
    let denied_user = id(0x93);
    let manage_users_role = id(0x9a);
    let database_id = id(0x33);
    let admin_role = builtin_owner_id();
    let token_config = StorageManagerConfig {
        ddl_fence_timeout: Duration::from_millis(100),
        max_active_invocations: 128,
        max_active_analytical_invocations: 8,
        max_invocation_age: Duration::from_secs(15 * 60),
        max_invocation_scan_work: prdb_engine::transaction::MAX_INVOCATION_SCAN_WORK,
        max_authorized_index_range_candidates: 128,
        max_analytical_authorized_index_range_candidates: None,
        max_authorized_index_range_bytes: 16 * 1024 * 1024,
        max_analytical_authorized_index_range_bytes: None,
        max_analytical_invocation_scan_work: None,
        max_planner_estimate_requests: prdb_engine::transaction::DEFAULT_PLANNER_ESTIMATE_REQUESTS,
        max_analytical_planner_estimate_requests: None,
        max_planner_estimate_candidates: prdb_engine::statistics::DEFAULT_ESTIMATE_CANDIDATES,
        max_analytical_planner_estimate_candidates: None,
        max_statistics_collection_rows: prdb_engine::statistics::MAX_COMMITTED_POPULATION_ROWS
            as usize,
        max_token_lifecycle_requests_per_minute: 5,
        max_admin_cancellation_requests_per_minute: 30,
    };
    let storage = StorageManager::with_config(&database, token_config)?;
    let second_storage_handle = StorageManager::with_config(&database, token_config)?;
    let seed_records = [
        principal_write_with_name(admin_user, 1, 1, "token_admin"),
        object_write_with_name(admin_user, 8, "token_admin"),
        principal_write_with_name(user_manager, 1, 1, "token_manager"),
        object_write_with_name(user_manager, 8, "token_manager"),
        principal_write_with_name(manage_users_role, 2, 1, "token_managers"),
        object_write_with_name(manage_users_role, 9, "token_managers"),
        principal_write_with_name(target_user, 1, 1, "token_target"),
        object_write_with_name(target_user, 8, "token_target"),
        principal_write_with_name(denied_user, 1, 1, "token_denied"),
        object_write_with_name(denied_user, 8, "token_denied"),
        membership_write_with_grantor(admin_user, admin_role, admin_role),
        membership_write_with_grantor(user_manager, manage_users_role, admin_role),
        dependency_write(admin_user, admin_role, 5),
        dependency_write(user_manager, manage_users_role, 5),
        credential_verifier_write(id(0x9b), admin_user, &[0x9b; 32], 1, i64::MAX, None, 1),
        grant_write(
            manage_users_role,
            database_id,
            12,
            1,
            database_id,
            admin_role,
        ),
        dependency_write(manage_users_role, database_id, 5),
    ];
    let mut seed_audit = identity_audit_event(0x90, admin_role, admin_user);
    seed_audit.action = AuditAction::AuthorityChange;
    storage.commit_catalog_and_audit(id(0x90), admin_role, 9_999, &seed_records, &[seed_audit])?;

    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let lifecycle_event = |event_id: u8, actor_id, target_ids| AuditEvent {
        event_id: id(event_id),
        actor_id: Some(actor_id),
        correlation_id: id(event_id.wrapping_add(1)),
        occurred_at_us: now,
        action: AuditAction::TokenLifecycle,
        result: AuditResult::Succeeded,
        target_ids,
    };
    let expires_at_us = now + 3_600_000_000;
    let mut issued = storage.issue_token(TokenIssueRequest {
        invocation_id: id(0x94),
        authenticated_actor_id: user_manager,
        user_id: target_user,
        label: "integration client",
        expires_at_us,
        retention_deadline_us: now + 7_200_000_000,
        audit_event: lifecycle_event(0x94, user_manager, vec![target_user]),
    })?;
    let token_id = issued.token_id;
    assert_eq!(issued.commit_sequence, 2);
    let secret = issued.take_secret().ok_or("issued secret missing")?;
    assert!(issued.take_secret().is_none());
    let own_inventory = storage.list_tokens(target_user, target_user)?;
    assert_eq!(own_inventory.len(), 1);
    assert_eq!(own_inventory[0].token_id, token_id);
    assert_eq!(own_inventory[0].label, "integration client");
    assert!(own_inventory[0].issued_at_us >= now);
    assert_eq!(own_inventory[0].expires_at_us, expires_at_us);
    assert!(!own_inventory[0].revoked);
    let managed_inventory = storage.list_tokens(user_manager, target_user)?;
    assert_eq!(managed_inventory, own_inventory);
    let unauthorized_existing = storage.rotate_token(TokenRotationRequest {
        invocation_id: id(0x9c),
        authenticated_actor_id: denied_user,
        old_token_id: token_id,
        label: "must not rotate existing token",
        expires_at_us,
        retention_deadline_us: now + 7_200_000_000,
        audit_event: lifecycle_event(0x9c, denied_user, vec![token_id]),
    });
    let unauthorized_unknown = storage.rotate_token(TokenRotationRequest {
        invocation_id: id(0x9e),
        authenticated_actor_id: denied_user,
        old_token_id: id(0x9f),
        label: "must not reveal unknown token",
        expires_at_us,
        retention_deadline_us: now + 7_200_000_000,
        audit_event: lifecycle_event(0x9e, denied_user, vec![id(0x9f)]),
    });
    let unauthorized_existing_error = match unauthorized_existing {
        Err(error) => error,
        Ok(_) => panic!("unauthorized rotation of an existing token unexpectedly succeeded"),
    };
    let unauthorized_unknown_error = match unauthorized_unknown {
        Err(error) => error,
        Ok(_) => panic!("unauthorized rotation of an unknown token unexpectedly succeeded"),
    };
    assert_eq!(
        unauthorized_existing_error,
        TokenLifecycleError::TokenUnavailable
    );
    assert_eq!(
        unauthorized_unknown_error,
        TokenLifecycleError::TokenUnavailable
    );
    assert_eq!(
        storage.list_tokens(denied_user, target_user).unwrap_err(),
        TokenLifecycleError::TokenUnavailable
    );
    assert_eq!(
        storage.list_tokens(user_manager, id(0x94)).unwrap_err(),
        TokenLifecycleError::TokenUnavailable
    );

    let read = database.begin_read()?;
    let token_key = id_catalog_key(16, token_id);
    let token_bytes = read
        .open_table(CATALOG)?
        .get(token_key.as_slice())?
        .ok_or("issued verifier missing")?
        .value()
        .to_vec();
    let verifier_record = decode_registered_envelope(&token_bytes)?;
    let verifier = verifier_record
        .fields
        .iter()
        .find(|field| field.tag == 4)
        .ok_or("verifier field missing")?;
    assert_eq!(verifier.payload, hash_token_secret(&secret));
    assert_ne!(verifier.payload, secret);
    drop(read);

    let (admitted, roles) = storage.begin_token_authenticated_invocation(
        id(0x95),
        token_id,
        &secret,
        now + 7_200_000_000,
        2,
        64,
    )?;
    assert!(roles.is_empty());
    assert_eq!(
        storage.revoke_token(
            id(0x96),
            user_manager,
            token_id,
            now + 7_200_000_000,
            lifecycle_event(0x96, user_manager, vec![token_id]),
        )?,
        3
    );
    assert!(matches!(
        admitted.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            CommitError::Conflict
        ))
    ));
    assert!(matches!(
        storage.begin_token_authenticated_invocation(
            id(0x97),
            token_id,
            &secret,
            now + 7_200_000_000,
            2,
            64,
        ),
        Err(prdb_engine::transaction::TransactionError::AuthenticationFailed)
    ));
    let revoked_inventory = storage.list_tokens(user_manager, target_user)?;
    assert_eq!(revoked_inventory.len(), 1);
    assert!(revoked_inventory[0].revoked);
    assert!(revoked_inventory[0].revoked_at_us.is_some());

    let login_race_issue_invocation = id(0xa4);
    let login_race_disable_invocation = id(0xa6);
    let before_login_race = commit_sequence(&database)?;
    let mut race_issue_audit = lifecycle_event(0xa3, admin_user, vec![target_user]);
    race_issue_audit.correlation_id = login_race_issue_invocation;
    let mut race_disable_audit = identity_audit_event(0xa5, admin_user, target_user);
    race_disable_audit.correlation_id = login_race_disable_invocation;
    let login_race_barrier = Arc::new(Barrier::new(3));
    let (race_issue_result, race_disable_result) = std::thread::scope(|scope| {
        let issue_barrier = Arc::clone(&login_race_barrier);
        let issue_storage = &storage;
        let issue = scope.spawn(move || {
            issue_barrier.wait();
            issue_storage.issue_token(TokenIssueRequest {
                invocation_id: login_race_issue_invocation,
                authenticated_actor_id: admin_user,
                user_id: target_user,
                label: "login race token",
                expires_at_us,
                retention_deadline_us: now + 7_200_000_000,
                audit_event: race_issue_audit,
            })
        });
        let disable_barrier = Arc::clone(&login_race_barrier);
        let disable_storage = &storage;
        let disable = scope.spawn(move || {
            disable_barrier.wait();
            disable_storage.set_user_login_enabled(
                login_race_disable_invocation,
                admin_user,
                target_user,
                false,
                now + 7_200_000_000,
                race_disable_audit,
            )
        });
        login_race_barrier.wait();
        (
            issue.join().expect("login-race token issue panicked"),
            disable.join().expect("login-disable thread panicked"),
        )
    });
    let issued_racing_token_id = match race_issue_result {
        Ok(issued) => Some((issued.token_id, issued.commit_sequence)),
        Err(TokenLifecycleError::Commit(
            CommitError::Conflict
            | CommitError::InvalidCatalog
            | CommitError::DdlAuthorizationDenied,
        ))
        | Err(TokenLifecycleError::TokenUnavailable) => None,
        Err(error) => return Err(Box::new(error)),
    };
    let login_disable_sequence = match race_disable_result {
        Ok(sequence) => Some(sequence),
        Err(prdb_engine::storage_manager::IdentityLifecycleError::Commit(
            CommitError::Conflict | CommitError::InvalidCatalog,
        )) => None,
        Err(error) => return Err(Box::new(error)),
    };
    assert!(
        issued_racing_token_id.is_some() || login_disable_sequence.is_some(),
        "neither token issuance nor LOGIN disable committed"
    );
    if let (Some((_, issue_sequence)), Some(disable_sequence)) =
        (issued_racing_token_id, login_disable_sequence)
    {
        assert!(issue_sequence < disable_sequence);
    }
    let login_race_issue_status = if issued_racing_token_id.is_some() {
        OutcomeStatus::Committed
    } else {
        OutcomeStatus::Aborted
    };
    let login_race_disable_status = if login_disable_sequence.is_some() {
        OutcomeStatus::Committed
    } else {
        OutcomeStatus::Aborted
    };
    assert_eq!(
        lookup_outcome(&database, login_race_issue_invocation, admin_user, now)?
            .ok_or("missing racing token-issue outcome")?
            .status,
        login_race_issue_status
    );
    assert_eq!(
        lookup_outcome(&database, login_race_disable_invocation, admin_user, now)?
            .ok_or("missing racing LOGIN-disable outcome")?
            .status,
        login_race_disable_status
    );
    assert_eq!(
        commit_sequence(&database)?,
        before_login_race
            + u64::from(issued_racing_token_id.is_some())
            + u64::from(login_disable_sequence.is_some())
    );
    let login_race_tokens = storage.list_tokens(admin_user, target_user)?;
    let login_enabled = {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        let principal = catalog
            .get(id_catalog_key(10, target_user).as_slice())?
            .ok_or("token-race target principal missing")?;
        decode_registered_envelope(principal.value())?
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .ok_or("target LOGIN state missing")?
            .payload
            == [1]
    };
    assert_eq!(login_enabled, login_disable_sequence.is_none());
    if !login_enabled {
        assert!(
            login_race_tokens.iter().all(|token| token.revoked),
            "LOGIN disable left an active token after concurrent issuance"
        );
    }
    if let Some((racing_token_id, _)) = issued_racing_token_id {
        let racing_token = login_race_tokens
            .iter()
            .find(|token| token.token_id == racing_token_id)
            .ok_or("issued login-race token is absent from inventory")?;
        assert_eq!(racing_token.revoked, !login_enabled);
    }

    let denied_issue = storage.issue_token(TokenIssueRequest {
        invocation_id: id(0x98),
        authenticated_actor_id: denied_user,
        user_id: target_user,
        label: "denied client",
        expires_at_us,
        retention_deadline_us: now + 7_200_000_000,
        audit_event: lifecycle_event(0x98, denied_user, vec![target_user]),
    });
    let denied_missing_issue = storage.issue_token(TokenIssueRequest {
        invocation_id: id(0xaa),
        authenticated_actor_id: denied_user,
        user_id: id(0xab),
        label: "must not reveal missing target",
        expires_at_us,
        retention_deadline_us: now + 7_200_000_000,
        audit_event: lifecycle_event(0xaa, denied_user, vec![id(0xab)]),
    });
    let denied_issue_error = match denied_issue {
        Err(error) => error,
        Ok(_) => panic!("unauthorized token issue for an existing target unexpectedly succeeded"),
    };
    let denied_missing_issue_error = match denied_missing_issue {
        Err(error) => error,
        Ok(_) => panic!("unauthorized token issue for a missing target unexpectedly succeeded"),
    };
    assert_eq!(denied_issue_error, TokenLifecycleError::TokenUnavailable);
    assert_eq!(
        denied_missing_issue_error,
        TokenLifecycleError::TokenUnavailable
    );

    assert_eq!(
        second_storage_handle
            .list_tokens(user_manager, target_user)
            .unwrap_err(),
        TokenLifecycleError::RateLimited
    );
    assert!(prdb_engine::startup::validate_persisted_records(&database).is_ok());
    Ok(())
}

pub(super) fn catalog_commit_requires_principal_object_identity_and_kind()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let principal = principal_write(id(70), 1);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(70),
            id(9),
            9_999,
            std::slice::from_ref(&principal),
            &[audit_event(70)],
        ),
        Err(CommitError::InvalidCatalog),
    );

    let object = object_write(id(70), 8);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(71),
            id(9),
            9_999,
            &[principal, object.clone()],
            &[audit_event(71)],
        )?,
        1
    );

    let wrong_kind = object_write(id(70), 9);
    let wrong_kind_update = CatalogWrite {
        key: wrong_kind.key,
        expected_value: object.payload.clone(),
        payload: wrong_kind.payload,
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(72),
            id(9),
            9_999,
            &[wrong_kind_update],
            &[audit_event(72)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 1);
    Ok(())
}

pub(super) fn catalog_commit_requires_matching_column_and_principal_names()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let principal_id = id(76);
    let mismatched_principal =
        principal_write_with_name(principal_id, 1, 1, "different_principal_name");
    let principal_object = principal_object_write(principal_id, 1);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(76),
            id(9),
            9_999,
            &[mismatched_principal, principal_object],
            &[audit_event(76)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);

    let schema_id = id(77);
    let table_id = id(78);
    let column_id = id(79);
    let make_column_batch = |column_name: &str| {
        vec![
            schema_write(schema_id),
            table_write(table_id, schema_id),
            column_write(column_id, table_id, column_name),
            object_write(schema_id, 1),
            object_write(table_id, 3),
            object_write(column_id, 4),
        ]
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(77),
            id(9),
            9_999,
            &make_column_batch("different_column_name"),
            &[audit_event(77)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(78),
            id(9),
            9_999,
            &make_column_batch("object_79"),
            &[audit_event(78)],
        )?,
        1
    );
    assert_eq!(commit_sequence(&database)?, 1);
    Ok(())
}

pub(super) fn catalog_commit_rejects_duplicate_user_and_role_names()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let user_id = id(80);
    let principal = principal_write_with_name(user_id, 1, 1, "database_admin");
    let object = object_write_with_name(user_id, 8, "database_admin");

    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(80),
            id(9),
            9_999,
            &[principal, object],
            &[audit_event(80)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);
    Ok(())
}

pub(super) fn catalog_commit_checks_principal_name_uniqueness_in_the_final_view()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let user_id = id(81);
    let mut user = principal_write_with_name(user_id, 1, 1, "alice");
    let mut user_object = object_write_with_name(user_id, 8, "alice");
    commit_catalog_and_audit(
        &database,
        id(81),
        id(9),
        9_999,
        &[user.clone(), user_object.clone()],
        &[audit_event(81)],
    )?;

    let mut renamed_user = principal_write_with_name(user_id, 1, 2, "bob");
    renamed_user.expected_value = user.payload.take();
    let mut renamed_object = object_write_with_name(user_id, 8, "bob");
    renamed_object.expected_value = user_object.payload.take();
    let role_id = id(82);
    let role = principal_write_with_login(role_id, 2, false, 1, "alice");
    let role_object = object_write_with_name(role_id, 9, "alice");
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(82),
            id(9),
            9_999,
            &[renamed_user, renamed_object, role, role_object],
            &[audit_event(82)],
        )?,
        2
    );
    assert_eq!(commit_sequence(&database)?, 2);
    Ok(())
}

pub(super) fn catalog_commit_requires_schema_object_identity_and_kind()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let schema = schema_write(id(73));
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(73),
            id(9),
            9_999,
            std::slice::from_ref(&schema),
            &[audit_event(73)],
        ),
        Err(CommitError::InvalidCatalog),
    );

    let object = object_write(id(73), 1);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(74),
            id(9),
            9_999,
            &[schema.clone(), object.clone()],
            &[audit_event(74)],
        )?,
        1
    );

    let wrong_kind = object_write(id(73), 2);
    let wrong_kind_update = CatalogWrite {
        key: wrong_kind.key,
        expected_value: object.payload.clone(),
        payload: wrong_kind.payload,
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(75),
            id(9),
            9_999,
            &[wrong_kind_update],
            &[audit_event(75)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 1);
    Ok(())
}

pub(super) fn catalog_commit_rejects_database_identity_as_schema_object()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let database_id = [0x33; 16];
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(0x74),
            id(9),
            9_999,
            &[schema_write(database_id), object_write(database_id, 1)],
            &[audit_event(0x74)],
        ),
        Err(CommitError::InvalidCatalog),
    );
    assert_eq!(commit_sequence(&database)?, 0);
    Ok(())
}

pub(super) fn policy_target_limit_checks_final_set_and_allows_replacements()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::catalog::{PolicyTarget, encode_policy_target};
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_id = id(77);
    let policy_id = id(78);
    let key = |family: u16, object_id: [u8; 16]| {
        let mut key = family.to_be_bytes().to_vec();
        key.push(1);
        key.extend_from_slice(&object_id);
        key
    };
    let field = |tag, wire_type, payload| CatalogField {
        tag,
        wire_type,
        payload,
    };
    let table_key = key(5, table_id);
    let table_payload = |epoch: u64| {
        encode_envelope(
            &family_schema(5).unwrap(),
            &table_key,
            &[
                field(1, WireType::Id, id(79).to_vec()),
                field(2, WireType::U64, 1u64.to_le_bytes().to_vec()),
                field(3, WireType::U64, epoch.to_le_bytes().to_vec()),
                field(4, WireType::Id, id(80).to_vec()),
                field(5, WireType::U16, 1u16.to_le_bytes().to_vec()),
            ],
        )
        .unwrap()
    };
    let policy_key = key(13, policy_id);
    let expression_id = id(81);
    let policy_payload = |epoch: u64| {
        encode_envelope(
            &family_schema(13).unwrap(),
            &policy_key,
            &[
                field(1, WireType::Id, table_id.to_vec()),
                field(2, WireType::U16, 1u16.to_le_bytes().to_vec()),
                field(3, WireType::U16, 1u16.to_le_bytes().to_vec()),
                field(4, WireType::U16, 1u16.to_le_bytes().to_vec()),
                field(5, WireType::Id, expression_id.to_vec()),
                field(7, WireType::U64, epoch.to_le_bytes().to_vec()),
            ],
        )
        .unwrap()
    };
    let target = |kind: u16, target_id: [u8; 16]| {
        let payload = encode_policy_target(&PolicyTarget {
            policy_id,
            kind,
            target_id,
        })
        .unwrap();
        CatalogWrite {
            key: payload[32..].to_vec(),
            expected_value: None,
            payload: Some(payload),
        }
    };
    {
        let write = database.begin_write()?;
        {
            let mut catalog = write.open_table(prdb_engine::schema::CATALOG)?;
            catalog.insert(table_key.as_slice(), table_payload(1).as_slice())?;
            catalog.insert(policy_key.as_slice(), policy_payload(1).as_slice())?;
            for object in [object_write(table_id, 3), object_write(policy_id, 10)] {
                catalog.insert(
                    object.key.as_slice(),
                    object.payload.as_ref().unwrap().as_slice(),
                )?;
            }
            let expression = expression_write(
                expression_id,
                3,
                &[],
                &prdb_engine::authorization::Expr::CurrentPrincipalId,
            );
            let expression_object = object_write(expression_id, 5);
            let expression_edge = dependency_write(policy_id, expression_id, 2);
            let recipient_edge = dependency_write(policy_id, builtin_owner_id(), 5);
            for record in [
                expression,
                expression_object,
                expression_edge,
                recipient_edge,
            ] {
                catalog.insert(
                    record.key.as_slice(),
                    record.payload.as_ref().unwrap().as_slice(),
                )?;
            }
            let recipient = target(2, builtin_owner_id());
            catalog.insert(
                recipient.key.as_slice(),
                recipient.payload.as_ref().unwrap().as_slice(),
            )?;
            for n in 1..=4095u128 {
                let principal_id = n.to_be_bytes();
                for record in [
                    principal_write(principal_id, 1),
                    principal_object_write(principal_id, 1),
                    dependency_write(policy_id, principal_id, 5),
                    target(1, principal_id),
                ] {
                    catalog.insert(
                        record.key.as_slice(),
                        record.payload.as_ref().unwrap().as_slice(),
                    )?;
                }
            }
        }
        write.commit()?;
    }
    let new_principal_id = 4096u128.to_be_bytes();
    let addition = target(1, new_principal_id);
    let new_principal = principal_write(new_principal_id, 1);
    let new_principal_object = principal_object_write(new_principal_id, 1);
    let new_principal_edge = dependency_write(policy_id, new_principal_id, 5);
    let old = target(1, 1u128.to_be_bytes());
    let updates = [
        CatalogWrite {
            key: policy_key.clone(),
            expected_value: Some(policy_payload(1)),
            payload: Some(policy_payload(2)),
        },
        CatalogWrite {
            key: table_key.clone(),
            expected_value: Some(table_payload(1)),
            payload: Some(table_payload(2)),
        },
    ];
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(1),
            id(9),
            9_999,
            &[
                addition.clone(),
                new_principal.clone(),
                new_principal_object.clone(),
                new_principal_edge.clone(),
                updates[0].clone(),
                updates[1].clone(),
            ],
            &[audit_event(10)]
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(commit_sequence(&database)?, 0);
    let removal = CatalogWrite {
        key: old.key.clone(),
        expected_value: old.payload,
        payload: None,
    };
    let old_edge = dependency_write(policy_id, 1u128.to_be_bytes(), 5);
    let edge_removal = CatalogWrite {
        key: old_edge.key,
        expected_value: old_edge.payload,
        payload: None,
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(2),
            id(9),
            9_999,
            &[
                addition.clone(),
                new_principal,
                new_principal_object,
                new_principal_edge,
                removal,
                edge_removal,
                updates[0].clone(),
                updates[1].clone()
            ],
            &[audit_event(20)]
        )?,
        1
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(prdb_engine::schema::CATALOG)?;
    assert!(catalog.get(old.key.as_slice())?.is_none());
    assert!(catalog.get(addition.key.as_slice())?.is_some());
    Ok(())
}

pub(super) fn policy_column_targets_require_matching_cell_policy_columns()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::catalog::{PolicyTarget, encode_policy_target};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_id = id(77);
    let policy_id = id(78);
    let expression_id = id(81);
    let first_column_id = id(82);
    let second_column_id = id(84);
    let missing_column_id = id(85);
    let other_table_id = id(86);
    let other_column_id = id(87);
    let schema_id = id(79);
    let table_key = id_catalog_key(5, table_id);
    let table_payload = |epoch: u64| {
        encode_envelope(
            &family_schema(5).unwrap(),
            &table_key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::Id,
                    payload: schema_id.to_vec(),
                },
                CatalogField {
                    tag: 2,
                    wire_type: WireType::U64,
                    payload: 1u64.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::U64,
                    payload: epoch.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 4,
                    wire_type: WireType::Id,
                    payload: builtin_owner_id().to_vec(),
                },
                CatalogField {
                    tag: 5,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
            ],
        )
        .unwrap()
    };
    let policy_key = id_catalog_key(13, policy_id);
    let policy_payload = |kind: u16, epoch: u64| {
        encode_envelope(
            &family_schema(13).unwrap(),
            &policy_key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::Id,
                    payload: table_id.to_vec(),
                },
                CatalogField {
                    tag: 2,
                    wire_type: WireType::U16,
                    payload: kind.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 4,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 5,
                    wire_type: WireType::Id,
                    payload: expression_id.to_vec(),
                },
                CatalogField {
                    tag: 7,
                    wire_type: WireType::U64,
                    payload: epoch.to_le_bytes().to_vec(),
                },
            ],
        )
        .unwrap()
    };
    let policy_target = |kind: u16, target_id: [u8; 16]| {
        let payload = encode_policy_target(&PolicyTarget {
            policy_id,
            kind,
            target_id,
        })
        .unwrap();
        CatalogWrite {
            key: payload[32..].to_vec(),
            expected_value: None,
            payload: Some(payload),
        }
    };
    let target_write = |kind: u16, target_id: [u8; 16]| policy_target(kind, target_id);
    let dependency =
        |dependent_id, referenced_id, kind| dependency_write(dependent_id, referenced_id, kind);
    let table_record = CatalogWrite {
        key: table_key.clone(),
        expected_value: None,
        payload: Some(table_payload(1)),
    };
    let policy_record = CatalogWrite {
        key: policy_key.clone(),
        expected_value: None,
        payload: Some(policy_payload(2, 1)),
    };
    let expression = expression_write(
        expression_id,
        3,
        &[],
        &prdb_engine::authorization::Expr::CurrentPrincipalId,
    );
    let first_column = column_write(first_column_id, table_id, "object_82");
    let second_column = column_write(second_column_id, table_id, "object_84");
    let other_table = table_write(other_table_id, schema_id);
    let other_column = column_write(other_column_id, other_table_id, "object_87");
    {
        let write = database.begin_write()?;
        {
            let mut catalog = write.open_table(prdb_engine::schema::CATALOG)?;
            for record in [
                table_record.clone(),
                policy_record.clone(),
                object_write(table_id, 3),
                object_write(other_table_id, 3),
                object_write(policy_id, 10),
                other_table.clone(),
                object_write(expression_id, 5),
                expression.clone(),
                dependency(policy_id, expression_id, 2),
                dependency(policy_id, builtin_owner_id(), 5),
                object_write(first_column_id, 4),
                first_column.clone(),
                object_write(second_column_id, 4),
                second_column.clone(),
                object_write(other_column_id, 4),
                other_column.clone(),
                target_write(2, builtin_owner_id()),
                target_write(3, first_column_id),
            ] {
                catalog.insert(
                    record.key.as_slice(),
                    record.payload.as_ref().unwrap().as_slice(),
                )?;
            }
        }
        write.commit()?;
    }

    let policy_update = |kind, epoch| CatalogWrite {
        key: policy_key.clone(),
        expected_value: Some(policy_payload(2, 1)),
        payload: Some(policy_payload(kind, epoch)),
    };
    let table_update = CatalogWrite {
        key: table_key.clone(),
        expected_value: Some(table_payload(1)),
        payload: Some(table_payload(2)),
    };
    let missing_target = target_write(3, missing_column_id);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(1),
            id(9),
            9_999,
            &[missing_target, policy_update(2, 2), table_update.clone(),],
            &[audit_event(10)],
        ),
        Err(CommitError::InvalidCatalog),
    );

    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(2),
            id(9),
            9_999,
            &[policy_update(1, 2), table_update.clone(),],
            &[audit_event(20)],
        ),
        Err(CommitError::InvalidCatalog),
    );

    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(3),
            id(9),
            9_999,
            &[
                target_write(3, other_column_id),
                policy_update(2, 2),
                table_update.clone(),
            ],
            &[audit_event(30)],
        ),
        Err(CommitError::InvalidCatalog),
    );

    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(4),
            id(9),
            9_999,
            &[
                target_write(3, second_column_id),
                policy_update(2, 2),
                table_update,
            ],
            &[audit_event(40)],
        )?,
        1,
    );
    Ok(())
}

pub(super) fn membership_limit_applies_to_final_graph_independent_of_write_order()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let node = |n: u128| n.to_be_bytes();
    let member = node(1);
    let old_role = node(100);
    let new_role = node(164);
    {
        let transaction = database.begin_write()?;
        {
            let mut catalog = transaction.open_table(prdb_engine::schema::CATALOG)?;
            let mut principal_names = transaction.open_table(PRINCIPAL_NAMES)?;
            for n in (1..=64).chain(100..=164) {
                let principal = principal_write(node(n), 2);
                let object = principal_object_write(node(n), 2);
                principal_names.insert(principal_test_name(node(n)).as_bytes(), &node(n)[..])?;
                catalog.insert(
                    principal.key.as_slice(),
                    principal.payload.as_ref().unwrap().as_slice(),
                )?;
                catalog.insert(
                    object.key.as_slice(),
                    object.payload.as_ref().unwrap().as_slice(),
                )?;
            }
            for member_number in 1..=64 {
                for role_number in 100..=163 {
                    let edge = membership_write(node(member_number), node(role_number));
                    catalog.insert(
                        edge.key.as_slice(),
                        edge.payload.as_ref().unwrap().as_slice(),
                    )?;
                    let authority = dependency_write(node(member_number), node(role_number), 5);
                    catalog.insert(
                        authority.key.as_slice(),
                        authority.payload.as_ref().unwrap().as_slice(),
                    )?;
                }
            }
        }
        transaction.commit()?;
    }
    let old = membership_write(member, old_role);
    let new = membership_write(member, new_role);
    let old_authority = dependency_write(member, old_role, 5);
    let new_authority = dependency_write(member, new_role, 5);
    let removal = CatalogWrite {
        key: old.key.clone(),
        expected_value: old.payload.clone(),
        payload: None,
    };
    let authority_removal = CatalogWrite {
        key: old_authority.key.clone(),
        expected_value: old_authority.payload.clone(),
        payload: None,
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(1),
            id(9),
            9_999,
            &[
                new.clone(),
                removal.clone(),
                authority_removal.clone(),
                new_authority.clone(),
                principal_epoch_update(member, 2, 2)
            ],
            &[audit_event(10)]
        )?,
        1
    );
    let read = database.begin_read()?;
    let catalog = read.open_table(prdb_engine::schema::CATALOG)?;
    assert!(catalog.get(old.key.as_slice())?.is_none());
    assert!(catalog.get(new.key.as_slice())?.is_some());
    assert_eq!(catalog.len()?, 293 + 8192);
    drop(catalog);
    drop(read);
    let mut advance = principal_write_with_epoch(member, 2, 3);
    advance.expected_value = principal_write_with_epoch(member, 2, 2).payload;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(2),
            id(9),
            9_999,
            &[old.clone(), advance.clone()],
            &[audit_event(20)]
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(commit_sequence(&database)?, 1);
    let removal_new = CatalogWrite {
        key: new.key.clone(),
        expected_value: new.payload.clone(),
        payload: None,
    };
    let new_authority_removal = CatalogWrite {
        key: new_authority.key,
        expected_value: new_authority.payload,
        payload: None,
    };
    let old_authority_restore = CatalogWrite {
        key: old_authority.key,
        expected_value: None,
        payload: old_authority.payload,
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(3),
            id(9),
            9_999,
            &[
                removal_new,
                old,
                new_authority_removal,
                old_authority_restore,
                advance
            ],
            &[audit_event(30)]
        )?,
        2
    );
    Ok(())
}
