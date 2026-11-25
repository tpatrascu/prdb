use prdb_engine::audit::{AuditAction, AuditResult};
use prdb_engine::catalog::decode_registered_envelope;
use prdb_engine::outcome::OutcomeStatus;
use prdb_engine::schema::{AUDIT, CATALOG};
use prdb_engine::storage_manager::StorageManager;
use prdb_engine::tuple::{ScalarType, ScalarValue};
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata};
use std::time::Duration;

use super::helpers::*;

pub(super) fn storage_manager_runtime_and_metadata_reads_enforce_authorization()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::client_protocol::{ClientConnectionSession, ClientFrame, ClientMode};
    use prdb_engine::storage_manager::{
        AdministrativeInvocationListError, InvocationStatus, InvocationStatusConsistency,
        MAX_ADMIN_INVOCATION_PAGE,
    };
    use sha2::Digest;
    use zeroize::{Zeroize, Zeroizing};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_id = id(230);
    let ordinary_id = id(231);
    let backup_admin_id = id(235);
    let user_manager_id = id(238);
    let manage_users_role = id(239);
    let database_id = id(0x33);
    let admin_role = builtin_owner_id();
    let records = [
        principal_write_with_name(admin_id, 1, 1, "runtime_admin"),
        object_write_with_name(admin_id, 8, "runtime_admin"),
        principal_write_with_name(ordinary_id, 1, 1, "runtime_user"),
        object_write_with_name(ordinary_id, 8, "runtime_user"),
        principal_write_with_name(backup_admin_id, 1, 1, "backup_admin"),
        object_write_with_name(backup_admin_id, 8, "backup_admin"),
        principal_write_with_name(user_manager_id, 1, 1, "token_manager"),
        object_write_with_name(user_manager_id, 8, "token_manager"),
        principal_write_with_name(manage_users_role, 2, 1, "manage_users"),
        object_write_with_name(manage_users_role, 9, "manage_users"),
        membership_write_with_grantor(admin_id, admin_role, admin_role),
        dependency_write(admin_id, admin_role, 5),
        membership_write_with_grantor(backup_admin_id, admin_role, admin_role),
        dependency_write(backup_admin_id, admin_role, 5),
        membership_write_with_grantor(user_manager_id, manage_users_role, admin_role),
        dependency_write(user_manager_id, manage_users_role, 5),
        grant_write(
            manage_users_role,
            database_id,
            12,
            1,
            database_id,
            admin_role,
        ),
        dependency_write(manage_users_role, database_id, 5),
        credential_verifier_write(id(236), backup_admin_id, &[0x6a; 32], 1, i64::MAX, None, 1),
        credential_verifier_write(id(237), admin_id, &[0x5a; 32], 1, i64::MAX, None, 1),
        credential_verifier_write(id(242), ordinary_id, &[0x6b; 32], 1, i64::MAX, None, 1),
        credential_verifier_write(id(240), user_manager_id, &[0x4b; 32], 1, i64::MAX, None, 1),
        credential_verifier_write(id(241), user_manager_id, &[0x3c; 32], 1, i64::MAX, None, 1),
    ];
    let mut setup_audit = audit_event(216);
    setup_audit.actor_id = Some(admin_role);
    let mut second_setup_audit = audit_event(218);
    second_setup_audit.actor_id = Some(admin_id);
    storage
        .commit_catalog_and_audit(
            id(216),
            admin_role,
            9_999,
            &records,
            &[setup_audit, second_setup_audit],
        )
        .map_err(|error| format!("setup catalog commit: {error:?}"))?;

    use prdb_engine::storage_manager::{AdministrativeAuditListError, MAX_ADMIN_AUDIT_PAGE};
    assert!(matches!(
        storage.list_audit_events(ordinary_id, 1, None),
        Err(AdministrativeAuditListError::Unauthorized)
    ));
    let first_audit_page = storage.list_audit_events(admin_id, 1, None)?;
    assert_eq!(first_audit_page.events.len(), 1);
    let audit_cursor = first_audit_page.continuation.unwrap();
    assert_eq!(
        prdb_engine::storage_manager::AdministrativeAuditCursor::from_wire(&audit_cursor.to_wire()),
        Some(audit_cursor.clone())
    );
    assert!(matches!(
        storage.list_audit_events(backup_admin_id, 1, Some(&audit_cursor)),
        Err(AdministrativeAuditListError::InvalidCursor)
    ));
    let second_audit_page = storage.list_audit_events(admin_id, 1, Some(&audit_cursor))?;
    assert_eq!(second_audit_page.events.len(), 1);
    assert!(second_audit_page.continuation.is_none());
    assert!(first_audit_page.events[0].event_id < second_audit_page.events[0].event_id);
    assert!(storage.list_audit_events(admin_id, 0, None).is_err());
    assert!(
        storage
            .list_audit_events(admin_id, MAX_ADMIN_AUDIT_PAGE + 1, None)
            .is_err()
    );
    let failed_token_id = id(237);
    for _ in 0..5 {
        assert!(matches!(
            storage.authenticate_query_session(failed_token_id, &[0; 32]),
            Err(prdb_engine::transaction::TransactionError::AuthenticationFailed)
        ));
    }
    let failed_authentication_events = storage
        .list_audit_events(admin_id, MAX_ADMIN_AUDIT_PAGE, None)?
        .events
        .into_iter()
        .filter(|event| {
            event.action == AuditAction::AuthenticationFailure
                && event.target_ids.as_slice() == [failed_token_id]
        })
        .collect::<Vec<_>>();
    assert_eq!(failed_authentication_events.len(), 4);
    assert!(
        failed_authentication_events
            .iter()
            .all(|event| { event.actor_id.is_none() && event.result == AuditResult::Denied })
    );
    assert!(matches!(
        storage.authenticate_query_session([0; 16], &[0; 32]),
        Err(prdb_engine::transaction::TransactionError::AuthenticationFailed)
    ));
    let zero_token_failure = storage
        .list_audit_events(admin_id, MAX_ADMIN_AUDIT_PAGE, None)?
        .events
        .into_iter()
        .find(|event| {
            event.action == AuditAction::AuthenticationFailure && event.target_ids.is_empty()
        })
        .ok_or("zero token authentication failure was not audited")?;
    assert!(zero_token_failure.actor_id.is_none());

    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_micros() as i64;
    let retention_deadline_us = now_us + 60_000_000;
    let mut first = storage.begin_invocation(id(232), admin_id, retention_deadline_us, 2, 64)?;
    let second = storage.begin_invocation(id(233), ordinary_id, retention_deadline_us, 2, 64)?;
    first.stage_put(
        prdb_engine::overlay::RowId {
            table: id(250),
            row: id(251),
        },
        b"value",
    )?;
    assert!(matches!(
        storage.list_runtime_invocations(ordinary_id, now_us, 1, None),
        Err(AdministrativeInvocationListError::Unauthorized)
    ));
    let first_page = storage.list_runtime_invocations(admin_id, now_us, 1, None)?;
    assert_eq!(first_page.invocations.len(), 1);
    assert_eq!(first_page.invocations[0].invocation_id, id(232));
    assert_eq!(first_page.invocations[0].principal_id, admin_id);
    assert_eq!(first_page.invocations[0].staged_rows, 1);
    assert_eq!(first_page.invocations[0].staged_value_bytes, 5);
    assert_eq!(first_page.invocation_staged_value_bytes_total, 5);
    assert_eq!(first_page.invocations[0].snapshot_pin_count, 1);
    assert_eq!(first_page.pinned_snapshot_count, 2);
    assert!(first_page.oldest_snapshot_pin_age_us > 0);
    assert_eq!(first_page.invocation_conflicts_total, 0);
    assert_ne!(first_page.invocations[0].transaction_id, [0; 16]);
    assert!(first_page.invocations[0].age_us <= 1_000_000);
    assert!(first_page.fence_waits.is_empty());
    assert_eq!(first_page.observed_at_us, now_us);
    assert_eq!(
        first_page.consistency,
        InvocationStatusConsistency::BestEffort
    );
    let cursor = first_page.continuation.unwrap();
    assert!(matches!(
        storage.list_runtime_invocations(backup_admin_id, now_us, 1, Some(&cursor)),
        Err(AdministrativeInvocationListError::InvalidCursor)
    ));
    let second_page = storage.list_runtime_invocations(admin_id, now_us, 1, Some(&cursor))?;
    assert_eq!(second_page.invocations.len(), 1);
    assert_eq!(second_page.invocations[0].invocation_id, id(233));
    assert_eq!(second_page.invocations[0].principal_id, ordinary_id);
    assert_eq!(second_page.invocations[0].staged_rows, 0);
    assert_eq!(second_page.invocations[0].staged_value_bytes, 0);
    assert_eq!(second_page.invocations[0].snapshot_pin_count, 1);
    assert_eq!(second_page.pinned_snapshot_count, 2);
    assert!(second_page.oldest_snapshot_pin_age_us > 0);
    assert_ne!(second_page.invocations[0].transaction_id, [0; 16]);
    assert_ne!(
        first_page.invocations[0].transaction_id,
        second_page.invocations[0].transaction_id
    );
    let usage_cursor = storage
        .list_runtime_invocations(admin_id, now_us, 1, None)?
        .continuation
        .unwrap();
    first.stage_put(
        prdb_engine::overlay::RowId {
            table: id(250),
            row: id(251),
        },
        b"new",
    )?;
    assert!(matches!(
        storage.list_runtime_invocations(admin_id, now_us, 1, Some(&usage_cursor)),
        Err(AdministrativeInvocationListError::StaleCursor)
    ));
    let replacement_page = storage.list_runtime_invocations(admin_id, now_us, 1, None)?;
    assert_eq!(replacement_page.invocations[0].staged_rows, 1);
    assert_eq!(replacement_page.invocations[0].staged_value_bytes, 3);
    assert_eq!(replacement_page.invocation_staged_value_bytes_total, 8);
    first.stage_delete(prdb_engine::overlay::RowId {
        table: id(250),
        row: id(251),
    })?;
    let deleted_page = storage.list_runtime_invocations(admin_id, now_us, 1, None)?;
    assert_eq!(deleted_page.invocations[0].staged_rows, 0);
    assert_eq!(deleted_page.invocations[0].staged_value_bytes, 0);
    assert_eq!(deleted_page.invocation_staged_value_bytes_total, 8);
    assert!(matches!(
        storage.get_administrative_invocation_status(admin_id, id(233), now_us)?,
        Some(snapshot) if matches!(snapshot.status, InvocationStatus::Active { .. })
    ));
    assert!(matches!(
        storage.get_administrative_invocation_status(ordinary_id, id(233), now_us),
        Err(AdministrativeInvocationListError::Unauthorized)
    ));
    assert_eq!(
        storage.get_administrative_invocation_status(admin_id, id(239), now_us)?,
        None
    );

    let stale = storage
        .list_runtime_invocations(admin_id, now_us, 1, None)?
        .continuation
        .unwrap();
    let mut third = storage.begin_invocation(id(234), ordinary_id, retention_deadline_us, 2, 64)?;
    assert!(matches!(
        storage.list_runtime_invocations(admin_id, now_us, 1, Some(&stale)),
        Err(AdministrativeInvocationListError::StaleCursor)
    ));
    assert!(matches!(
        storage.list_runtime_invocations(admin_id, now_us, 0, None),
        Err(AdministrativeInvocationListError::InvalidPageLimit)
    ));
    assert!(
        storage
            .list_runtime_invocations(admin_id, now_us, MAX_ADMIN_INVOCATION_PAGE + 1, None)
            .is_err()
    );

    third.abort()?;
    assert!(matches!(
        storage.get_administrative_invocation_status(admin_id, id(234), now_us)?,
        Some(snapshot) if matches!(&snapshot.status, InvocationStatus::Terminal(outcome)
            if outcome.status == OutcomeStatus::Aborted)
    ));

    struct AcceptMetadata;
    impl prdb_engine::supervisor::SubmissionMetadataValidator for AcceptMetadata {
        fn supports_versions(&self, manifest_version: u32, scope_version: u32) -> bool {
            manifest_version == 1 && scope_version == 1
        }

        fn validate(&self, _metadata: &prdb_engine::supervisor::SubmissionMetadata) -> bool {
            true
        }
    }
    let workspace = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or("workspace root is missing")?
        .to_path_buf();
    let sdk_build_dir = fixture.0.join("sdk-build");
    let sdk_build = std::process::Command::new("make")
        .current_dir(&workspace)
        .args([
            "-f",
            "engine/sdk/Makefile",
            &format!("BUILD_DIR={}", sdk_build_dir.display()),
            "SDK_RUNTIME_PROFILE=nixos-glibc-2.42",
            "sdk-shared-release",
        ])
        .output()?;
    if !sdk_build.status.success() {
        return Err(format!(
            "SDK fixture build failed: {}",
            String::from_utf8_lossy(&sdk_build.stderr)
        )
        .into());
    }
    let sdk_release_dir =
        sdk_build_dir.join("releases/linux-x86_64/x86_64-v1/nixos-glibc-2.42/sdk-1-abis1");
    let sdk_metadata = std::fs::read(sdk_release_dir.join("sdk-release.json"))?;
    let sdk_library = std::fs::read(sdk_release_dir.join("libprdb_sql.so.1"))?;
    let sdk_repository = std::sync::Arc::new(
        prdb_engine::sdk_registry::SdkLibraryRepository::open(
            &fixture.path().parent().unwrap().join("sdk-libraries"),
            "linux-x86_64".to_owned(),
            "x86_64-v1".to_owned(),
            "nixos-glibc-2.42".to_owned(),
            std::collections::HashSet::from(["libc.so.6".to_owned()]),
            1024 * 1024,
        )
        .unwrap(),
    );
    let staging = prdb_engine::supervisor::StagingStore::new(
        prdb_engine::supervisor::StagingLimits::default(),
        std::sync::Arc::new(AcceptMetadata),
    )?
    .with_sdk_repository(sdk_repository.clone());

    use prdb_engine::storage_manager::{CreateTableRequest, TableColumnDefinition};
    let visible_schema_invocation = id(243);
    let mut visible_schema_audit = identity_audit_event(0xf0, admin_id, id(0xf1));
    visible_schema_audit.correlation_id = visible_schema_invocation;
    let visible_schema = storage.create_schema(
        visible_schema_invocation,
        admin_id,
        "visible_app",
        admin_id,
        retention_deadline_us,
        visible_schema_audit,
    )?;
    let hidden_schema_invocation = id(244);
    let mut hidden_schema_audit = identity_audit_event(0xf2, admin_id, id(0xf3));
    hidden_schema_audit.correlation_id = hidden_schema_invocation;
    let hidden_schema = storage.create_schema(
        hidden_schema_invocation,
        admin_id,
        "hidden_app",
        admin_id,
        retention_deadline_us,
        hidden_schema_audit,
    )?;
    let table_invocation = id(245);
    let mut table_audit = identity_audit_event(0xf4, admin_id, id(0xf5));
    table_audit.correlation_id = table_invocation;
    let visible_table = storage.create_table(CreateTableRequest {
        invocation_id: table_invocation,
        authenticated_actor_id: admin_id,
        schema_id: visible_schema.schema_id,
        name: "metrics".to_owned(),
        columns: vec![
            TableColumnDefinition {
                name: "reading".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            },
            TableColumnDefinition {
                name: "reading_extra".to_owned(),
                scalar_type: ScalarType::Text,
                nullable: true,
                generated_uuid_identity: false,
                default: None,
            },
        ],
        retention_deadline_us,
        audit_event: table_audit,
    })?;
    let ungranted_table_invocation = id(248);
    let mut ungranted_table_audit = identity_audit_event(0xf9, admin_id, id(0xfa));
    ungranted_table_audit.correlation_id = ungranted_table_invocation;
    let _ungranted_table = storage.create_table(CreateTableRequest {
        invocation_id: ungranted_table_invocation,
        authenticated_actor_id: admin_id,
        schema_id: visible_schema.schema_id,
        name: "ungranted_metrics".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "secret".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us,
        audit_event: ungranted_table_audit,
    })?;
    let hidden_table_invocation = id(246);
    let mut hidden_table_audit = identity_audit_event(0xf6, admin_id, id(0xf7));
    hidden_table_audit.correlation_id = hidden_table_invocation;
    let _hidden_table = storage.create_table(CreateTableRequest {
        invocation_id: hidden_table_invocation,
        authenticated_actor_id: admin_id,
        schema_id: hidden_schema.schema_id,
        name: "secrets".to_owned(),
        columns: vec![TableColumnDefinition {
            name: "value".to_owned(),
            scalar_type: ScalarType::Text,
            nullable: true,
            generated_uuid_identity: false,
            default: None,
        }],
        retention_deadline_us,
        audit_event: hidden_table_audit,
    })?;
    let read = database.begin_read()?;
    let mut ordinary_epoch = principal_write_with_name(ordinary_id, 1, 2, "runtime_user");
    ordinary_epoch.expected_value = Some(
        read.open_table(CATALOG)?
            .get(id_catalog_key(10, ordinary_id).as_slice())?
            .ok_or("ordinary metadata principal is missing")?
            .value()
            .to_vec(),
    );
    drop(read);
    let usage_grant = grant_write(
        ordinary_id,
        visible_schema.schema_id,
        1,
        1,
        visible_schema.schema_id,
        admin_id,
    );
    let select_grant = grant_write(
        ordinary_id,
        visible_table.table_id,
        2,
        1,
        visible_table.table_id,
        admin_id,
    );
    let metadata_grant_invocation = id(247);
    let mut metadata_grant_audit = identity_audit_event(0xf8, admin_id, visible_table.table_id);
    metadata_grant_audit.action = AuditAction::AuthorityChange;
    metadata_grant_audit.correlation_id = metadata_grant_invocation;
    storage.commit_authorized_object_grant_ddl(
        metadata_grant_invocation,
        admin_id,
        retention_deadline_us,
        &[
            usage_grant.clone(),
            dependency_write(ordinary_id, visible_schema.schema_id, 5),
            select_grant,
            dependency_write(ordinary_id, visible_table.table_id, 5),
            ordinary_epoch,
        ],
        &[metadata_grant_audit],
    )?;
    for sequence in 0..64u8 {
        let mut invocation_id = [0; 16];
        invocation_id[..8].copy_from_slice(b"DENIED00");
        invocation_id[15] = sequence;
        let mut policy_denial_invocation =
            storage.begin_invocation(invocation_id, ordinary_id, retention_deadline_us, 2, 64)?;
        let mut executor = policy_denial_invocation.open_executor_metadata_connection();
        let resolved = prdb_engine::executor_ipc::dispatch_executor_request(
            prdb_engine::ipc::ServiceRole::Executor,
            &mut executor,
            &prdb_engine::ipc::IpcFrame {
                operation: prdb_engine::ipc::Operation::ResolveRelation as u16,
                request_id: 1,
                payload: prdb_engine::executor_ipc::ResolveRelationRequest {
                    schema_name: "visible_app".to_owned(),
                    relation_name: "metrics".to_owned(),
                }
                .encode()?,
            },
        )?;
        let handle =
            match prdb_engine::executor_ipc::ResolveRelationResponse::decode(&resolved.payload)? {
                prdb_engine::executor_ipc::ResolveRelationResponse::Resolved { handle, .. } => {
                    handle
                }
                prdb_engine::executor_ipc::ResolveRelationResponse::Unavailable => {
                    return Err("SELECT-visible relation unexpectedly unavailable".into());
                }
            };
        let denied_insert = prdb_engine::executor_ipc::dispatch_executor_request(
            prdb_engine::ipc::ServiceRole::Executor,
            &mut executor,
            &prdb_engine::ipc::IpcFrame {
                operation: prdb_engine::ipc::Operation::StageInsert as u16,
                request_id: 2,
                payload: prdb_engine::executor_ipc::StageInsertRequest {
                    relation: handle,
                    values: vec![
                        prdb_engine::transaction::InsertInput::Value(Some(ScalarValue::Integer(1))),
                        prdb_engine::transaction::InsertInput::Value(Some(ScalarValue::Text(
                            "denied".to_owned(),
                        ))),
                    ],
                }
                .encode()?,
            },
        );
        assert!(matches!(
            denied_insert,
            Err(
                prdb_engine::executor_ipc::ExecutorRequestError::Transaction(
                    prdb_engine::transaction::TransactionError::Authorization(
                        prdb_engine::authorization::AuthorizationError::Denied
                    )
                )
            )
        ));
        drop(executor);
        drop(policy_denial_invocation);
    }
    let post_denial_page =
        storage.list_runtime_invocations(admin_id, now_us, MAX_ADMIN_INVOCATION_PAGE, None)?;
    assert_eq!(post_denial_page.invocations.len(), 2);
    let mut control = ClientConnectionSession::default();
    control.accept_hello(ClientMode::Control, "runtime-status")?;
    control.authenticate(&storage, id(237), &[0x5a; 32])?;
    let encode_invocation_id = |invocation_id: [u8; 16]| {
        let mut encoded = String::with_capacity(32);
        for byte in invocation_id {
            encoded.push_str(&format!("{byte:02x}"));
        }
        format!("{{\"invocation_id\":\"{encoded}\"}}").into_bytes()
    };
    let active_status = control.handle_status_request(
        &storage,
        &staging,
        ClientFrame {
            kind: 8,
            request_id: 1,
            response_sequence: 0,
            payload: encode_invocation_id(id(233)),
        },
    )?;
    assert_eq!(active_status.payload, b"{\"state\":\"active\"}");
    let terminal_status = control.handle_status_request(
        &storage,
        &staging,
        ClientFrame {
            kind: 8,
            request_id: 2,
            response_sequence: 0,
            payload: encode_invocation_id(id(234)),
        },
    )?;
    assert_eq!(terminal_status.payload, b"{\"state\":\"aborted\"}");
    let cancelled = control.handle_cancel_prepared_request(
        &storage,
        &staging,
        ClientFrame {
            kind: 9,
            request_id: 3,
            response_sequence: 0,
            payload: encode_invocation_id(id(233)),
        },
    )?;
    assert_eq!(cancelled.payload, b"{\"state\":\"accepted\"}");
    let mut discovery_control = ClientConnectionSession::default();
    discovery_control.accept_hello(ClientMode::Control, "schema-discovery")?;
    discovery_control.authenticate(&storage, id(242), &[0x6b; 32])?;
    assert!(storage.is_database_admin(admin_id)?);
    assert!(!storage.is_database_admin(ordinary_id)?);
    let mut sdk_library_control = ClientConnectionSession::default();
    sdk_library_control.accept_hello(ClientMode::Control, "sdk-library-discovery")?;
    sdk_library_control.authenticate(&storage, id(242), &[0x6b; 32])?;
    let sdk_library_discovery = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut sdk_library_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 1,
            payload: br#"{"phase":"read","operation_version":1,"operation":"sdk_libraries.list","arguments":{"limit":"1"}}"#.to_vec(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let sdk_library_discovery = match sdk_library_discovery {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => response.payload,
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("SDK library listing unexpectedly started an invocation".into());
        }
    };
    assert_eq!(
        sdk_library_discovery,
        br#"{"sdk_libraries":[],"next_cursor":null}"#
    );
    let denied_sdk_library_remove = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut sdk_library_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 2,
            payload: br#"{"phase":"write","operation_version":1,"operation":"sdk_libraries.remove","arguments":{"operation_id":"11111111111111111111111111111111","release":"1"}}"#.to_vec(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    );
    assert!(matches!(
        denied_sdk_library_remove,
        Err(prdb_engine::supervisor_ipc::GatewayRequestError::Client(
            prdb_engine::client_protocol::ClientSessionError::SdkLibraryRepositoryUnauthorized
        ))
    ));
    let sdk_operation_id = [0x88; 16];
    let sdk_operation_id_hex = sdk_operation_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let sdk_digest: [u8; 32] = sha2::Sha256::digest(&sdk_library).into();
    let sdk_digest_hex = sdk_digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let mut sdk_admin_control = ClientConnectionSession::default();
    sdk_admin_control.accept_hello(ClientMode::Control, "sdk-library-admin")?;
    sdk_admin_control.authenticate(&storage, id(237), &[0x5a; 32])?;
    let begin_install = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut sdk_admin_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 1,
            payload: format!(
                r#"{{"phase":"prepare","operation_version":1,"operation":"sdk_libraries.install","arguments":{{"operation_id":"{sdk_operation_id_hex}","metadata_bytes":"{}","library_bytes":"{}","library_sha256":"{sdk_digest_hex}"}}}}"#,
                sdk_metadata.len(),
                sdk_library.len()
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )
    .map_err(|error| std::io::Error::other(format!("SDK install begin dispatch: {error:?}")))?;
    assert!(matches!(
        begin_install,
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(_)
    ));
    let sdk_upload = [sdk_metadata.as_slice(), sdk_library.as_slice()].concat();
    let mut sdk_offset = 0u64;
    let mut sdk_request_id = 2u64;
    for chunk in sdk_upload.chunks(prdb_engine::client_protocol::CLIENT_MAX_CHUNK_BYTES) {
        let mut payload = vec![prdb_engine::client_protocol::CLIENT_SDK_LIBRARY_CHUNK_KIND as u8];
        payload.extend_from_slice(&sdk_operation_id);
        payload.extend_from_slice(&sdk_offset.to_le_bytes());
        payload.extend_from_slice(chunk);
        assert!(
            sdk_admin_control
                .permits_request_kind(prdb_engine::client_protocol::CLIENT_SDK_LIBRARY_CHUNK_KIND)
        );
        prdb_engine::client_protocol::decode_client_chunk(&payload[1..])
            .map_err(|error| std::io::Error::other(format!("SDK test chunk decode: {error:?}")))?;
        let response = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
            prdb_engine::ipc::ServiceRole::Gateway,
            &storage,
            &staging,
            &mut sdk_admin_control,
            &prdb_engine::ipc::IpcFrame {
                operation: prdb_engine::ipc::Operation::UploadSubmissionChunk as u16,
                request_id: sdk_request_id,
                payload,
            },
            prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
                retention_period_us: 60_000_000,
                max_rows: 1,
                max_bytes: 1,
            },
        )
        .map_err(|error| std::io::Error::other(format!("SDK upload chunk dispatch: {error:?}")))?;
        assert!(matches!(
            response,
            prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(_)
        ));
        sdk_offset += chunk.len() as u64;
        sdk_request_id += 1;
    }
    let finish_install = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut sdk_admin_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: sdk_request_id,
            payload: format!(
                r#"{{"phase":"write","operation_version":1,"operation":"sdk_libraries.install.finish","arguments":{{"operation_id":"{sdk_operation_id_hex}"}}}}"#
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )
    .map_err(|error| std::io::Error::other(format!("SDK install finish dispatch: {error:?}")))?;
    assert!(matches!(
        finish_install,
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(_)
    ));
    assert_eq!(
        sdk_repository
            .list()
            .map_err(|error| format!("SDK repository listing failed: {error:?}"))?
            .len(),
        1
    );
    assert!(matches!(
        sdk_repository
            .operation_outcome(sdk_operation_id)
            .map_err(|error| format!("SDK operation lookup failed: {error:?}"))?,
        Some(prdb_engine::sdk_registry::SdkLibraryOperationOutcome::Installed {
            owner_id,
            sdk_release: 1,
            sha256,
            ..
        }) if owner_id == admin_id && sha256 == sdk_digest
    ));
    let install_status = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut sdk_admin_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: sdk_request_id + 1,
            payload: format!(
                r#"{{"phase":"read","operation_version":1,"operation":"sdk_libraries.operation_status","arguments":{{"operation_id":"{sdk_operation_id_hex}"}}}}"#
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    assert!(matches!(
        install_status,
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response)
            if String::from_utf8_lossy(&response.payload).contains("\"operation\":\"install\"")
    ));
    let remove_operation_id = [0x99; 16];
    let remove_operation_id_hex = remove_operation_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    for request_id in [sdk_request_id + 2, sdk_request_id + 3] {
        let remove = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
            prdb_engine::ipc::ServiceRole::Gateway,
            &storage,
            &staging,
            &mut sdk_admin_control,
            &prdb_engine::ipc::IpcFrame {
                operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
                request_id,
                payload: format!(
                    r#"{{"phase":"write","operation_version":1,"operation":"sdk_libraries.remove","arguments":{{"operation_id":"{remove_operation_id_hex}","release":"1"}}}}"#
                )
                .into_bytes(),
            },
            prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
                retention_period_us: 60_000_000,
                max_rows: 1,
                max_bytes: 1,
            },
        )?;
        assert!(matches!(
            remove,
            prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response)
                if String::from_utf8_lossy(&response.payload).contains("\"removed_bytes\"")
        ));
    }
    assert!(
        sdk_repository
            .list()
            .map_err(|error| format!("SDK post-remove listing failed: {error:?}"))?
            .is_empty()
    );
    let mut repository_query = ClientConnectionSession::default();
    repository_query.accept_hello_with_sdk_library_mode(
        ClientMode::Query,
        "repository-query",
        true,
    )?;
    repository_query.authenticate(&storage, id(242), &[0x6b; 32])?;
    let repository_prepare = repository_query.handle_prepare_request(
        &storage,
        &staging,
        ClientFrame {
            kind: 3,
            request_id: 1,
            response_sequence: 0,
            payload: br#"{"manifest_version":1,"manifest":"bQ==","artifact_bytes":"3","artifact_sha256":"0000000000000000000000000000000000000000000000000000000000000000","parameter_bytes":"0","parameter_sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","scope_version":1,"requested_scope":"cw==","deadline_ms":"30000","workload_class":"analytical","sdk_library_mode":"repository"}"#.to_vec(),
        },
    )?;
    assert!(
        !String::from_utf8_lossy(&repository_prepare.payload).contains("sql_library_chunk_bytes")
    );
    let mut legacy_library_chunk = vec![0x55; 16];
    legacy_library_chunk.extend_from_slice(&0u64.to_le_bytes());
    legacy_library_chunk.push(0x01);
    assert!(matches!(
        repository_query.handle_upload_request(
            &storage,
            &staging,
            ClientFrame {
                kind: 12,
                request_id: 2,
                response_sequence: 0,
                payload: legacy_library_chunk,
            },
        ),
        Err(prdb_engine::client_protocol::ClientSessionError::WrongMode)
    ));
    let schema_discovery = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut discovery_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 1,
            payload: br#"{"phase":"read","operation_version":1,"operation":"metadata.schemas.list","arguments":{}}"#.to_vec(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let schema_discovery = match schema_discovery {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => response.payload,
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("metadata discovery unexpectedly started an invocation".into());
        }
    };
    let schema_discovery = String::from_utf8(schema_discovery)?;
    assert!(schema_discovery.contains("\"schemas\":["));
    assert!(schema_discovery.contains("\"name\":\"information_schema\""));
    assert!(schema_discovery.contains("\"name\":\"visible_app\""));
    assert!(!schema_discovery.contains("\"name\":\"hidden_app\""));
    let public_relations =
        prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
            prdb_engine::ipc::ServiceRole::Gateway,
            &storage,
            &staging,
            &mut discovery_control,
            &prdb_engine::ipc::IpcFrame {
                operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
                request_id: 2,
                payload: br#"{"phase":"read","operation_version":1,"operation":"metadata.relations.list","arguments":{"schema_id":"80808080808080808080808080808002"}}"#.to_vec(),
            },
            prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
                retention_period_us: 60_000_000,
                max_rows: 1,
                max_bytes: 1,
            },
        )?;
    let public_relations = match public_relations {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => response.payload,
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("relation discovery unexpectedly started an invocation".into());
        }
    };
    let public_relations = String::from_utf8(public_relations)?;
    assert!(public_relations.contains("\"name\":\"columns\""));
    assert!(public_relations.contains("\"name\":\"schemata\""));
    assert!(public_relations.contains("\"name\":\"tables\""));
    let public_columns = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut discovery_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 3,
            payload: br#"{"phase":"read","operation_version":1,"operation":"metadata.columns.list","arguments":{"schema_id":"80808080808080808080808080808002","relation_id":"81818181818181818181818181818101"}}"#.to_vec(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let public_columns = match public_columns {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => response.payload,
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("column discovery unexpectedly started an invocation".into());
        }
    };
    let public_columns = String::from_utf8(public_columns)?;
    assert!(public_columns.contains("\"name\":\"table_schema\""));
    assert!(public_columns.contains("\"scalar_type\":\"text\""));
    assert!(public_columns.contains("\"nullable\":false"));
    let hex_id = |value: [u8; 16]| {
        value
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    let visible_relations = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut discovery_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 5,
            payload: format!(
                "{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"metadata.relations.list\",\"arguments\":{{\"schema_id\":\"{}\"}}}}",
                hex_id(visible_schema.schema_id)
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let visible_relations = match visible_relations {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("granted relation discovery unexpectedly started an invocation".into());
        }
    };
    assert!(
        visible_relations.contains("\"name\":\"metrics\""),
        "{visible_relations}"
    );
    assert!(!visible_relations.contains("\"name\":\"ungranted_metrics\""));
    let visible_columns = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut discovery_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 6,
            payload: format!(
                "{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"metadata.columns.list\",\"arguments\":{{\"schema_id\":\"{}\",\"relation_id\":\"{}\"}}}}",
                hex_id(visible_schema.schema_id),
                hex_id(visible_table.table_id)
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )
    .map_err(|error| format!("visible column gateway dispatch: {error:?}"))?;
    let visible_columns = match visible_columns {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("granted column discovery unexpectedly started an invocation".into());
        }
    };
    assert!(visible_columns.contains("\"name\":\"reading\""));
    assert!(visible_columns.contains("\"scalar_type\":\"integer\""));
    let hidden_relations = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut discovery_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 7,
            payload: format!(
                "{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"metadata.relations.list\",\"arguments\":{{\"schema_id\":\"{}\"}}}}",
                hex_id(hidden_schema.schema_id)
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )
    .map_err(|error| format!("hidden relation gateway dispatch: {error:?}"))?;
    let hidden_relations = match hidden_relations {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => response.payload,
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ungranted relation discovery unexpectedly started an invocation".into());
        }
    };
    assert_eq!(hidden_relations, br#"{"relations":[]}"#);
    let unavailable_relations = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut discovery_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 8,
            payload: br#"{"phase":"read","operation_version":1,"operation":"metadata.relations.list","arguments":{"schema_id":"f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0"}}"#.to_vec(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let unavailable_relations = match unavailable_relations {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => response.payload,
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("relation discovery unexpectedly started an invocation".into());
        }
    };
    assert_eq!(unavailable_relations, br#"{"relations":[]}"#);

    let metadata_listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let metadata_address = metadata_listener.local_addr()?;
    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let server = scope.spawn(|| {
            use std::io::{Read, Write};
            for _ in 0..2 {
                let (mut stream, _) = metadata_listener.accept().unwrap();
                let mut decoder = prdb_engine::client_protocol::ClientFrameDecoder::default();
                let mut session = ClientConnectionSession::default();
                let mut input = [0u8; 4096];
                loop {
                    let count = stream.read(&mut input).unwrap();
                    if count == 0 {
                        break;
                    }
                    for request in decoder.push(&input[..count]).unwrap() {
                        let request_id = request.request_id;
                        let result = match request.kind {
                            1 | 2 => session.handle_handshake_request(&storage, request),
                            10 => session.handle_admin_request(
                                &storage,
                                request,
                                retention_deadline_us,
                            ),
                            kind => panic!("unexpected metadata client frame kind {kind}"),
                        };
                        let response = result.unwrap_or_else(|error| {
                            prdb_engine::client_protocol::ClientFrame {
                                kind: prdb_engine::client_protocol::CLIENT_ERROR_KIND,
                                request_id,
                                response_sequence: 0,
                                payload: format!("{error:?}").into_bytes(),
                            }
                        });
                        stream.write_all(&response.encode().unwrap()).unwrap();
                    }
                }
            }
        });

        let mut secret = [0x6b; 32];
        let mut metadata_client =
            prdb_engine::client_submit::MetadataClient::connect_authenticated(
                metadata_address,
                id(242),
                &mut secret,
            )?;
        assert_eq!(secret, [0; 32]);
        let schemas = metadata_client.list_schemas()?;
        let schema_page = metadata_client.list_schemas_page(1, None)?;
        assert_eq!(schema_page.records.len(), 1);
        let schema_page_cursor = schema_page
            .next_cursor
            .as_deref()
            .ok_or("expected multiple visible schemas")?;
        assert!(matches!(
            metadata_client.list_schemas_page(2, Some(schema_page_cursor)),
            Err(prdb_engine::client_submit::ClientSubmitError::ServerRejected(_))
        ));
        assert!(matches!(
            metadata_client.list_relations_page(
                prdb_engine::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID,
                1,
                Some(schema_page_cursor),
            ),
            Err(prdb_engine::client_submit::ClientSubmitError::ServerRejected(_))
        ));
        let final_schema_page = metadata_client.list_schemas_page(1, Some(schema_page_cursor))?;
        assert_eq!(final_schema_page.records.len(), 1);
        assert!(final_schema_page.next_cursor.is_none());
        let schema = schemas
            .iter()
            .find(|schema| schema.name == "visible_app")
            .ok_or("grant-visible schema missing through MetadataClient")?;
        assert!(!schemas.iter().any(|schema| schema.name == "hidden_app"));
        let public_schema_id = prdb_engine::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID;
        let public_relation_page =
            metadata_client.list_relations_page(public_schema_id, 1, None)?;
        let public_cursor = public_relation_page
            .next_cursor
            .as_deref()
            .ok_or("expected multiple public information_schema relations")?;
        assert!(matches!(
            metadata_client.list_relations_page(public_schema_id, 2, Some(public_cursor)),
            Err(prdb_engine::client_submit::ClientSubmitError::ServerRejected(_))
        ));
        let second_public_relation =
            metadata_client.list_relations_page(public_schema_id, 1, Some(public_cursor))?;
        assert_eq!(second_public_relation.records.len(), 1);
        let second_public_cursor = second_public_relation
            .next_cursor
            .as_deref()
            .ok_or("expected third public information_schema relation")?;
        let third_public_relation =
            metadata_client.list_relations_page(public_schema_id, 1, Some(second_public_cursor))?;
        assert_eq!(third_public_relation.records.len(), 1);
        assert!(third_public_relation.next_cursor.is_none());
        let relations = metadata_client.list_relations(schema.schema_id)?;
        let relation = relations
            .iter()
            .find(|relation| relation.name == "metrics")
            .ok_or("grant-visible relation missing through MetadataClient")?;
        assert!(
            !relations
                .iter()
                .any(|relation| relation.name == "ungranted_metrics")
        );
        let columns = metadata_client.list_columns(schema.schema_id, relation.relation_id)?;
        let column = columns
            .iter()
            .find(|column| column.name == "reading")
            .ok_or("grant-visible column missing through MetadataClient")?;
        let first_column_page =
            metadata_client.list_columns_page(schema.schema_id, relation.relation_id, 1, None)?;
        assert_eq!(first_column_page.records[0].name, "reading");
        let cursor = first_column_page
            .next_cursor
            .as_deref()
            .ok_or("expected a second declared column page")?;
        assert!(matches!(
            metadata_client.list_columns_page(
                schema.schema_id,
                relation.relation_id,
                2,
                Some(cursor),
            ),
            Err(prdb_engine::client_submit::ClientSubmitError::ServerRejected(_))
        ));
        let rename_invocation = id(249);
        let mut rename_audit = identity_audit_event(0xfb, admin_id, visible_table.column_ids[1]);
        rename_audit.correlation_id = rename_invocation;
        storage.rename_column(
            rename_invocation,
            admin_id,
            visible_table.table_id,
            visible_table.column_ids[1],
            "reading_current",
            retention_deadline_us,
            rename_audit,
        )?;
        let second_column_page = metadata_client.list_columns_page(
            schema.schema_id,
            relation.relation_id,
            1,
            Some(cursor),
        )?;
        assert_eq!(second_column_page.records[0].name, "reading_extra");
        assert!(second_column_page.next_cursor.is_none());
        assert!(matches!(
            metadata_client.list_columns_page(
                schema.schema_id,
                relation.relation_id,
                1,
                Some(cursor),
            ),
            Err(prdb_engine::client_submit::ClientSubmitError::ServerRejected(_))
        ));
        let current_columns =
            metadata_client.list_columns(schema.schema_id, relation.relation_id)?;
        assert!(
            current_columns
                .iter()
                .any(|column| column.name == "reading_current")
        );
        let mut requested_scope = prdb_engine::submission_scope::RequestedScopeBuilder::new();
        requested_scope
            .include_column(
                schema,
                relation,
                column,
                &[prdb_engine::authorization::Operation::Select],
            )
            .map_err(|error| format!("requested-scope selection failed: {error:?}"))?;
        let decoded_scope = prdb_engine::submission_scope::decode_requested_scope_v1(
            &requested_scope
                .encode()
                .map_err(|error| format!("requested-scope encoding failed: {error:?}"))?,
        )
        .map_err(|error| format!("requested-scope decoding failed: {error:?}"))?;
        assert!(decoded_scope.contains(&(
            prdb_engine::authorization::Operation::Usage,
            prdb_engine::authorization::AccessTarget::Schema(schema.schema_id),
        )));
        assert!(decoded_scope.contains(&(
            prdb_engine::authorization::Operation::Select,
            prdb_engine::authorization::AccessTarget::Column {
                table_id: relation.relation_id,
                column_id: column.column_id,
            },
        )));
        let revoked_cursor = metadata_client
            .list_schemas_page(1, None)?
            .next_cursor
            .ok_or("expected a cursor before schema USAGE is revoked")?;
        let usage_dependency = dependency_write(ordinary_id, visible_schema.schema_id, 5);
        let (grant_bytes, dependency_bytes, principal_bytes) = {
            let read = database.begin_read()?;
            let catalog = read.open_table(CATALOG)?;
            let grant_bytes = catalog
                .get(usage_grant.key.as_slice())?
                .ok_or("schema USAGE grant disappeared before revocation")?
                .value()
                .to_vec();
            let dependency_bytes = catalog
                .get(usage_dependency.key.as_slice())?
                .ok_or("schema USAGE dependency disappeared before revocation")?
                .value()
                .to_vec();
            let principal_bytes = catalog
                .get(id_catalog_key(10, ordinary_id).as_slice())?
                .ok_or("metadata principal disappeared before grant revocation")?
                .value()
                .to_vec();
            (grant_bytes, dependency_bytes, principal_bytes)
        };
        let mut ordinary_epoch = principal_write_with_name(ordinary_id, 1, 3, "runtime_user");
        ordinary_epoch.expected_value = Some(principal_bytes);
        let revoke_schema_usage_invocation = id(250);
        let mut revoke_schema_usage_audit =
            identity_audit_event(0xfc, admin_id, visible_schema.schema_id);
        revoke_schema_usage_audit.action = AuditAction::AuthorityChange;
        revoke_schema_usage_audit.correlation_id = revoke_schema_usage_invocation;
        storage.commit_authorized_object_grant_ddl(
            revoke_schema_usage_invocation,
            admin_id,
            retention_deadline_us,
            &[
                catalog_delete(usage_grant.clone(), grant_bytes),
                catalog_delete(usage_dependency, dependency_bytes),
                ordinary_epoch,
            ],
            &[revoke_schema_usage_audit],
        )?;
        assert!(matches!(
            metadata_client.list_schemas_page(1, Some(&revoked_cursor)),
            Err(prdb_engine::client_submit::ClientSubmitError::ServerRejected(_))
        ));

        let foreign_cursor = metadata_client
            .list_relations_page(
                prdb_engine::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID,
                1,
                None,
            )?
            .next_cursor
            .ok_or("expected a public relation cursor for the cross-connection test")?;
        drop(metadata_client);
        let mut second_secret = [0x6b; 32];
        let mut second_client = prdb_engine::client_submit::MetadataClient::connect_authenticated(
            metadata_address,
            id(242),
            &mut second_secret,
        )?;
        assert!(matches!(
            second_client.list_relations_page(
                prdb_engine::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID,
                1,
                Some(&foreign_cursor),
            ),
            Err(prdb_engine::client_submit::ClientSubmitError::ServerRejected(_))
        ));
        drop(second_client);
        server.join().map_err(|_| "metadata TCP server panicked")?;
        Ok(())
    })?;

    let token_request = prdb_engine::ipc::IpcFrame {
        operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
        request_id: 4,
        payload: format!(
            "{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"tokens.list\",\"arguments\":{{\"user_id\":\"{}\"}}}}",
            id(235).iter().map(|byte| format!("{byte:02x}")).collect::<String>()
        ).into_bytes(),
    };
    let token_inventory = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &token_request,
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let token_inventory = match token_inventory {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => response.payload,
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN read unexpectedly started an invocation".into());
        }
    };
    let token_inventory = String::from_utf8(token_inventory)?;
    assert!(token_inventory.contains(&format!(
        "\"token_id\":\"{}\"",
        format!("{:02x}", 236).repeat(16)
    )));
    assert!(token_inventory.contains("test token"));
    assert!(token_inventory.contains("\"revoked\":false"));
    assert!(!token_inventory.contains("verifier"));
    assert!(!token_inventory.contains("secret"));
    first.stage_put(
        prdb_engine::overlay::RowId {
            table: id(250),
            row: id(251),
        },
        b"value",
    )?;
    let runtime_page = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 5,
            payload: b"{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"runtime.invocations.list\",\"arguments\":{\"limit\":\"1\"}}".to_vec(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let runtime_page = match runtime_page {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN runtime listing unexpectedly started an invocation".into());
        }
    };
    assert!(runtime_page.contains(&format!(
        "\"invocation_id\":\"{}\"",
        format!("{:02x}", 232).repeat(16)
    )));
    assert!(runtime_page.contains("\"consistency\":\"best_effort\""));
    assert!(runtime_page.contains("\"workload_class\":\"operational\""));
    assert!(runtime_page.contains("\"staged_rows\":1"));
    assert!(runtime_page.contains("\"staged_value_bytes\":5"));
    assert!(runtime_page.contains("\"snapshot_pin_count\":1"));
    assert!(runtime_page.contains("\"transaction_id\":\""));
    assert!(runtime_page.contains("\"age_us\":"));
    assert!(runtime_page.contains("\"fence_waits\":[]"));
    assert!(runtime_page.contains("\"fence_waits_truncated\":false"));
    assert!(runtime_page.contains("\"pinned_snapshot_count\":2"));
    assert!(runtime_page.contains("\"oldest_snapshot_pin_age_us\":"));
    assert!(runtime_page.contains("\"invocations_admitted_total\":67"));
    assert!(runtime_page.contains("\"invocation_conflicts_total\":0"));
    assert!(runtime_page.contains("\"admin_cancellations_accepted_total\":1"));
    assert!(runtime_page.contains("\"admin_cancellations_denied_total\":0"));
    assert!(
        runtime_page.contains("\"invocation_policy_denials_total\":64"),
        "unexpected policy denial runtime counter: {runtime_page}"
    );
    assert!(
        runtime_page.contains("\"invocation_staged_value_bytes_total\":13"),
        "unexpected staged value byte runtime counter: {runtime_page}"
    );
    assert!(runtime_page.contains("\"invocation_commit_latency_buckets\":["));
    let runtime_cursor = runtime_page
        .split("\"next_cursor\":\"")
        .nth(1)
        .ok_or("missing ADMIN runtime listing cursor")?
        .get(..98)
        .ok_or("invalid ADMIN runtime listing cursor")?;
    let runtime_second_page = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 6,
            payload: format!(
                "{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"runtime.invocations.list\",\"arguments\":{{\"limit\":\"1\",\"cursor\":\"{runtime_cursor}\"}}}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let runtime_second_page = match runtime_second_page {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN runtime listing unexpectedly started an invocation".into());
        }
    };
    assert!(runtime_second_page.contains(&format!(
        "\"invocation_id\":\"{}\"",
        format!("{:02x}", 233).repeat(16)
    )));
    assert!(runtime_second_page.contains("\"cancellation_requested\":true"));
    let mut user_manager_control = ClientConnectionSession::default();
    user_manager_control.accept_hello(ClientMode::Control, "token-manager")?;
    user_manager_control.authenticate(&storage, id(240), &[0x4b; 32])?;
    let mut failed_issue_control = ClientConnectionSession::default();
    failed_issue_control.accept_hello(ClientMode::Control, "failed-issue-admin")?;
    failed_issue_control.authenticate(&storage, id(237), &[0x5a; 32])?;
    let failed_issue_prepare = failed_issue_control.handle_admin_request(
        &storage,
        ClientFrame {
            kind: 10,
            request_id: 1,
            response_sequence: 0,
            payload: format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tokens.issue\",\"arguments\":{{\"user_id\":\"{}\",\"label\":\"unknown target\",\"expires_at_us\":\"{}\"}}}}",
                id(222).iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
                now_us + 3_600_000_000
            )
            .into_bytes(),
        },
        now_us + 60_000_000,
    )?;
    let failed_issue_prepare = String::from_utf8(failed_issue_prepare.payload)?;
    let failed_issue_operation_id = failed_issue_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing failed ADMIN issue operation ID")?
        .get(..32)
        .ok_or("invalid failed ADMIN issue operation ID")?;
    assert!(matches!(
        failed_issue_control.handle_admin_request(
            &storage,
            ClientFrame {
                kind: 10,
                request_id: 2,
                response_sequence: 0,
                payload: format!(
                    "{{\"phase\":\"start\",\"operation_id\":\"{failed_issue_operation_id}\"}}"
                )
                .into_bytes(),
            },
            now_us + 60_000_000,
        ),
        Err(prdb_engine::client_protocol::ClientSessionError::TokenInventory(_))
    ));
    assert!(matches!(
        failed_issue_control.handle_admin_request(
            &storage,
            ClientFrame {
                kind: 10,
                request_id: 3,
                response_sequence: 0,
                payload: format!(
                    "{{\"phase\":\"start\",\"operation_id\":\"{failed_issue_operation_id}\"}}"
                )
                .into_bytes(),
            },
            now_us + 60_000_000,
        ),
        Err(prdb_engine::client_protocol::ClientSessionError::WrongMode)
    ));
    let issue_prepare = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut user_manager_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 1,
            payload: format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tokens.issue\",\"arguments\":{{\"user_id\":\"{}\",\"label\":\"issued by ADMIN\",\"expires_at_us\":\"{}\"}}}}",
                id(231).iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
                now_us + 3_600_000_000
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let issue_prepare = match issue_prepare {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN issue prepare unexpectedly started".into());
        }
    };
    let issue_operation_id = issue_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN issue operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN issue operation ID")?;
    let issue = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut user_manager_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 2,
            payload: format!("{{\"phase\":\"start\",\"operation_id\":\"{issue_operation_id}\"}}")
                .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let mut issue = match issue {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN issue start returned a worker invocation".into());
        }
    };
    assert!(issue.contains("\"outcome\":\"committed\""));
    let issued_token_id = issue
        .split("\"token_id\":\"")
        .nth(1)
        .ok_or("missing one-time issued token ID")?
        .get(..32)
        .ok_or("invalid one-time issued token ID")?;
    let issued_secret = issue
        .split("\"token_secret\":\"")
        .nth(1)
        .ok_or("missing one-time issued token secret")?
        .get(..44)
        .ok_or("invalid one-time issued token secret")?;
    let mut credentials_payload = Zeroizing::new(format!(
        "{{\"token_id\":\"{issued_token_id}\",\"token_secret\":\"{issued_secret}\"}}"
    ));
    let credentials =
        prdb_engine::client_protocol::decode_client_credentials(credentials_payload.as_bytes())?;
    credentials_payload.zeroize();
    let mut issued_session = ClientConnectionSession::default();
    issued_session.accept_hello(ClientMode::Query, "new-token")?;
    issued_session.authenticate(&storage, credentials.token_id, &credentials.token_secret)?;
    assert!(storage
        .list_tokens(admin_id, ordinary_id)?
        .iter()
        .any(|token| token.token_id == credentials.token_id && token.label == "issued by ADMIN"));
    let issuance_status = control.handle_status_request(
        &storage,
        &staging,
        ClientFrame {
            kind: 8,
            request_id: 7,
            response_sequence: 0,
            payload: format!("{{\"invocation_id\":\"{issue_operation_id}\"}}").into_bytes(),
        },
    )?;
    assert_eq!(issuance_status.payload, b"{\"state\":\"committed\"}");
    issue.zeroize();
    let prepare_revoke = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut user_manager_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 3,
            payload: format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tokens.revoke\",\"arguments\":{{\"token_id\":\"{}\"}}}}",
                id(240).iter().map(|byte| format!("{byte:02x}")).collect::<String>()
            ).into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let prepare_revoke = match prepare_revoke {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN revoke prepare unexpectedly started".into());
        }
    };
    let operation_id = prepare_revoke
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN operation ID")?;
    assert!(prepare_revoke.ends_with("\",\"state\":\"prepared\"}"));
    let wrong_operation = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut user_manager_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 4,
            payload: format!(
                "{{\"phase\":\"start\",\"operation_id\":\"{}\"}}",
                format!("{:02x}", 241).repeat(16)
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    );
    assert!(matches!(
        wrong_operation,
        Err(prdb_engine::supervisor_ipc::GatewayRequestError::Client(
            prdb_engine::client_protocol::ClientSessionError::WrongMode
        ))
    ));
    let revoke = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut user_manager_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 5,
            payload: format!("{{\"phase\":\"start\",\"operation_id\":\"{operation_id}\"}}")
                .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let revoke = match revoke {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => response.payload,
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN revoke start returned a worker invocation".into());
        }
    };
    drop(revoke);
    let mut manager_recovery_control = ClientConnectionSession::default();
    manager_recovery_control.accept_hello(ClientMode::Control, "token-manager-recovery")?;
    manager_recovery_control.authenticate(&storage, id(241), &[0x3c; 32])?;
    let owner_reconciled_revoke = manager_recovery_control.handle_status_request(
        &storage,
        &staging,
        ClientFrame {
            kind: 8,
            request_id: 6,
            response_sequence: 0,
            payload: format!("{{\"invocation_id\":\"{operation_id}\"}}").into_bytes(),
        },
    )?;
    assert_eq!(
        owner_reconciled_revoke.payload,
        b"{\"state\":\"committed\"}"
    );
    let create_user_prepare = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut manager_recovery_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 7,
            payload: br#"{"phase":"prepare","operation_version":1,"operation":"users.create","arguments":{"name":"new_runtime_user"}}"#.to_vec(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )
    .map_err(|error| format!("users.create PREPARE failed: {error:?}"))?;
    let create_user_prepare = match create_user_prepare {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN user-create prepare unexpectedly started an invocation".into());
        }
    };
    let create_user_operation_id = create_user_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN user-create operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN user-create operation ID")?;
    let create_user = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut manager_recovery_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 8,
            payload: format!(
                "{{\"phase\":\"start\",\"operation_id\":\"{create_user_operation_id}\"}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )
    .map_err(|error| format!("users.create START failed: {error:?}"))?;
    let create_user = match create_user {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN user-create start returned a worker invocation".into());
        }
    };
    assert!(create_user.contains("\"outcome\":\"committed\""));
    let created_user_id_wire = create_user
        .split("\"user_id\":\"")
        .nth(1)
        .ok_or("missing created user ID")?
        .get(..32)
        .ok_or("invalid created user ID")?;
    let created_user_id: [u8; 16] = std::array::from_fn(|index| {
        u8::from_str_radix(&created_user_id_wire[index * 2..index * 2 + 2], 16).unwrap()
    });
    let create_user_issue_prepare = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut manager_recovery_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 9,
            payload: format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tokens.issue\",\"arguments\":{{\"user_id\":\"{created_user_id_wire}\",\"label\":\"new user login\",\"expires_at_us\":\"{}\"}}}}",
                now_us + 3_600_000_000
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )
    .map_err(|error| format!("new-user token PREPARE failed: {error:?}"))?;
    let create_user_issue_prepare = match create_user_issue_prepare {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("new-user token prepare unexpectedly started an invocation".into());
        }
    };
    let create_user_issue_operation_id = create_user_issue_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing new-user token operation ID")?
        .get(..32)
        .ok_or("invalid new-user token operation ID")?;
    let create_user_issue = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut manager_recovery_control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 10,
            payload: format!(
                "{{\"phase\":\"start\",\"operation_id\":\"{create_user_issue_operation_id}\"}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )
    .map_err(|error| format!("new-user token START failed: {error:?}"))?;
    let mut create_user_issue = match create_user_issue {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("new-user token start returned a worker invocation".into());
        }
    };
    let created_user_credentials = Zeroizing::new(format!(
        "{{\"token_id\":\"{}\",\"token_secret\":\"{}\"}}",
        create_user_issue
            .split("\"token_id\":\"")
            .nth(1)
            .ok_or("missing new-user token ID")?
            .get(..32)
            .ok_or("invalid new-user token ID")?,
        create_user_issue
            .split("\"token_secret\":\"")
            .nth(1)
            .ok_or("missing new-user token secret")?
            .get(..44)
            .ok_or("invalid new-user token secret")?
    ));
    let created_user_credentials = prdb_engine::client_protocol::decode_client_credentials(
        created_user_credentials.as_bytes(),
    )?;
    let mut created_user_session = ClientConnectionSession::default();
    created_user_session.accept_hello(ClientMode::Query, "new-runtime-user")?;
    created_user_session
        .authenticate(
            &storage,
            created_user_credentials.token_id,
            &created_user_credentials.token_secret,
        )
        .map_err(|error| format!("new-user AUTH failed: {error:?}"))?;
    assert_eq!(storage.list_tokens(admin_id, created_user_id)?.len(), 1);
    create_user_issue.zeroize();
    let reconciled_revoke = control.handle_status_request(
        &storage,
        &staging,
        ClientFrame {
            kind: 8,
            request_id: 8,
            response_sequence: 0,
            payload: format!("{{\"invocation_id\":\"{operation_id}\"}}").into_bytes(),
        },
    )?;
    assert_eq!(reconciled_revoke.payload, b"{\"state\":\"committed\"}");
    let create_role_prepare = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 9,
            payload: br#"{"phase":"prepare","operation_version":1,"operation":"roles.create","arguments":{"name":"runtime_readers"}}"#.to_vec(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let create_role_prepare = match create_role_prepare {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN role-create prepare unexpectedly started an invocation".into());
        }
    };
    let create_role_operation_id = create_role_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN role-create operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN role-create operation ID")?;
    let create_role = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 10,
            payload: format!(
                "{{\"phase\":\"start\",\"operation_id\":\"{create_role_operation_id}\"}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let create_role = match create_role {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN role-create start returned a worker invocation".into());
        }
    };
    assert!(create_role.contains("\"outcome\":\"committed\""));
    let created_role_id_wire = create_role
        .split("\"role_id\":\"")
        .nth(1)
        .ok_or("missing created role ID")?
        .get(..32)
        .ok_or("invalid created role ID")?;
    let created_role_id: [u8; 16] = std::array::from_fn(|index| {
        u8::from_str_radix(&created_role_id_wire[index * 2..index * 2 + 2], 16).unwrap()
    });
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let role_bytes = catalog
        .get(id_catalog_key(10, created_role_id).as_slice())?
        .ok_or("created role principal is missing")?
        .value()
        .to_vec();
    drop(catalog);
    drop(read);
    let role_record = decode_registered_envelope(&role_bytes)?;
    assert_eq!(
        role_record
            .fields
            .iter()
            .find(|field| field.tag == 1)
            .ok_or("created role has no kind")?
            .payload,
        2u16.to_le_bytes()
    );
    assert_eq!(
        role_record
            .fields
            .iter()
            .find(|field| field.tag == 3)
            .ok_or("created role has no login state")?
            .payload,
        [0]
    );
    let grant_role_prepare = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 11,
            payload: format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"roles.grant\",\"arguments\":{{\"member_id\":\"{created_user_id_wire}\",\"role_id\":\"{created_role_id_wire}\"}}}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let grant_role_prepare = match grant_role_prepare {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN role-grant prepare unexpectedly started an invocation".into());
        }
    };
    let grant_role_operation_id = grant_role_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN role-grant operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN role-grant operation ID")?;
    let grant_role = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 12,
            payload: format!(
                "{{\"phase\":\"start\",\"operation_id\":\"{grant_role_operation_id}\"}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let grant_role = match grant_role {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN role-grant start returned a worker invocation".into());
        }
    };
    assert!(grant_role.contains("\"outcome\":\"committed\""));
    let role_session = storage.authenticate_query_session(
        created_user_credentials.token_id,
        &created_user_credentials.token_secret,
    )?;
    assert!(role_session.role_ids().contains(&created_role_id));
    let revoke_role_prepare = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 13,
            payload: format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"roles.revoke\",\"arguments\":{{\"member_id\":\"{created_user_id_wire}\",\"role_id\":\"{created_role_id_wire}\"}}}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let revoke_role_prepare = match revoke_role_prepare {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN role-revoke prepare unexpectedly started an invocation".into());
        }
    };
    let revoke_role_operation_id = revoke_role_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN role-revoke operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN role-revoke operation ID")?;
    let revoke_role = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 14,
            payload: format!(
                "{{\"phase\":\"start\",\"operation_id\":\"{revoke_role_operation_id}\"}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let revoke_role = match revoke_role {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN role-revoke start returned a worker invocation".into());
        }
    };
    assert!(revoke_role.contains("\"outcome\":\"committed\""));
    let role_session = storage.authenticate_query_session(
        created_user_credentials.token_id,
        &created_user_credentials.token_secret,
    )?;
    assert!(!role_session.role_ids().contains(&created_role_id));
    let privilege_prepare = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 15,
            payload: format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"privileges.grant\",\"arguments\":{{\"grantee_id\":\"{created_user_id_wire}\",\"privilege\":\"manage_roles\"}}}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let privilege_prepare = match privilege_prepare {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN privilege grant prepare unexpectedly started an invocation".into());
        }
    };
    let privilege_operation_id = privilege_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN privilege operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN privilege operation ID")?;
    let privilege_grant = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 16,
            payload: format!(
                "{{\"phase\":\"start\",\"operation_id\":\"{privilege_operation_id}\"}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let privilege_grant = match privilege_grant {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN privilege grant start returned a worker invocation".into());
        }
    };
    assert!(privilege_grant.contains("\"outcome\":\"committed\""));
    let mut delegated_admin = ClientConnectionSession::default();
    delegated_admin.accept_hello(ClientMode::Control, "delegated-role-manager")?;
    delegated_admin.authenticate(
        &storage,
        created_user_credentials.token_id,
        &created_user_credentials.token_secret,
    )?;
    let delegated_role_prepare = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut delegated_admin,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 1,
            payload: br#"{"phase":"prepare","operation_version":1,"operation":"roles.create","arguments":{"name":"delegated_role"}}"#.to_vec(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let delegated_role_prepare = match delegated_role_prepare {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("delegated role-create prepare unexpectedly started an invocation".into());
        }
    };
    let delegated_role_operation_id = delegated_role_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing delegated role-create operation ID")?
        .get(..32)
        .ok_or("invalid delegated role-create operation ID")?;
    let delegated_role = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut delegated_admin,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 2,
            payload: format!(
                "{{\"phase\":\"start\",\"operation_id\":\"{delegated_role_operation_id}\"}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    assert!(matches!(
        &delegated_role,
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response)
            if String::from_utf8_lossy(&response.payload).contains("\"outcome\":\"committed\"")
    ));
    let delegated_role_response = match &delegated_role {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8_lossy(&response.payload)
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => unreachable!(),
    };
    let delegated_role_id = delegated_role_response
        .split("\"role_id\":\"")
        .nth(1)
        .ok_or("missing delegated role ID")?
        .get(..32)
        .ok_or("invalid delegated role ID")?;
    let drop_role_prepare = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut delegated_admin,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 3,
            payload: format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"roles.drop\",\"arguments\":{{\"role_id\":\"{delegated_role_id}\"}}}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let drop_role_prepare = match drop_role_prepare {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN role-drop prepare unexpectedly started an invocation".into());
        }
    };
    let drop_role_operation_id = drop_role_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing delegated role-drop operation ID")?
        .get(..32)
        .ok_or("invalid delegated role-drop operation ID")?;
    let drop_role = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut delegated_admin,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 4,
            payload: format!(
                "{{\"phase\":\"start\",\"operation_id\":\"{drop_role_operation_id}\"}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    assert!(matches!(
        drop_role,
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response)
            if String::from_utf8_lossy(&response.payload).contains("\"outcome\":\"committed\"")
    ));
    let login_prepare = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 17,
            payload: format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"users.login\",\"arguments\":{{\"user_id\":\"{created_user_id_wire}\",\"enabled\":\"false\"}}}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let login_prepare = match login_prepare {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN user-login prepare unexpectedly started an invocation".into());
        }
    };
    let login_operation_id = login_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN user-login operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN user-login operation ID")?;
    let login_change = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 18,
            payload: format!("{{\"phase\":\"start\",\"operation_id\":\"{login_operation_id}\"}}")
                .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    assert!(matches!(
        login_change,
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response)
            if String::from_utf8_lossy(&response.payload).contains("\"outcome\":\"committed\"")
    ));
    let schema_prepare = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 19,
            payload: format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"schemas.create\",\"arguments\":{{\"name\":\"protocol_schema\",\"owner_id\":\"{}\"}}}}",
                admin_id.iter().map(|byte| format!("{byte:02x}")).collect::<String>()
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let schema_prepare = match schema_prepare {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN schema-create prepare unexpectedly started an operation".into());
        }
    };
    let schema_operation_id = schema_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN schema-create operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN schema-create operation ID")?;
    let schema_create = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 20,
            payload: format!("{{\"phase\":\"start\",\"operation_id\":\"{schema_operation_id}\"}}")
                .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let schema_create = match schema_create {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN schema-create start returned a worker invocation".into());
        }
    };
    assert!(schema_create.contains("\"outcome\":\"committed\""));
    assert!(schema_create.contains("\"schema_id\":"));
    let created_schema_id = schema_create
        .split("\"schema_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN schema ID")?
        .get(..32)
        .ok_or("invalid ADMIN schema ID")?;
    let table_prepare = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 21,
            payload: format!(
                "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tables.create\",\"arguments\":{{\"schema_id\":\"{created_schema_id}\",\"name\":\"protocol_table\",\"columns\":[{{\"name\":\"id\",\"type\":\"uuid\",\"nullable\":false,\"generated_uuid_identity\":true}},{{\"name\":\"label\",\"type\":\"text\",\"nullable\":true,\"generated_uuid_identity\":false}}]}}}}"
            )
            .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let table_prepare = match table_prepare {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN table-create prepare unexpectedly started an operation".into());
        }
    };
    let table_operation_id = table_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN table-create operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN table-create operation ID")?;
    let table_create = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        &storage,
        &staging,
        &mut control,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id: 22,
            payload: format!("{{\"phase\":\"start\",\"operation_id\":\"{table_operation_id}\"}}")
                .into_bytes(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us: 60_000_000,
            max_rows: 1,
            max_bytes: 1,
        },
    )?;
    let table_create = match table_create {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            String::from_utf8(response.payload)?
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            return Err("ADMIN table-create start returned a worker invocation".into());
        }
    };
    assert!(table_create.contains("\"outcome\":\"committed\""));
    assert!(table_create.contains("\"table_id\":"));
    assert!(table_create.contains("\"column_ids\":[\""));
    let table_id_wire = table_create
        .split("\"table_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN table ID")?
        .get(..32)
        .ok_or("invalid ADMIN table ID")?;
    let column_ids_wire = table_create
        .split("\"column_ids\":[\"")
        .nth(1)
        .ok_or("missing ADMIN column IDs")?;
    let first_column_id_wire = column_ids_wire
        .get(..32)
        .ok_or("invalid first ADMIN column ID")?;
    let default_column_id_wire = column_ids_wire
        .split("\",\"")
        .nth(1)
        .and_then(|wire| wire.get(..32))
        .ok_or("invalid second ADMIN column ID")?;
    let add_column_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        23,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tables.add_column\",\"arguments\":{{\"table_id\":\"{table_id_wire}\",\"column\":{{\"name\":\"note\",\"type\":\"text\",\"nullable\":true,\"generated_uuid_identity\":false}}}}}}"
        ),
        60_000_000,
    )?;
    let add_column_operation_id = add_column_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN add-column operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN add-column operation ID")?;
    let add_column = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        24,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{add_column_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(add_column.contains("\"outcome\":\"committed\""));
    assert!(add_column.contains("\"schema_version\":\"2\""));
    let default_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        25,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tables.set_column_default\",\"arguments\":{{\"table_id\":\"{table_id_wire}\",\"column_id\":\"{default_column_id_wire}\",\"type\":\"text\",\"default\":{{\"kind\":\"literal\",\"value\":\"ready\"}}}}}}"
        ),
        60_000_000,
    )?;
    let default_operation_id = default_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN default operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN default operation ID")?;
    let default_set = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        26,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{default_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(default_set.contains("\"outcome\":\"committed\""));
    let default_drop_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        27,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tables.drop_column_default\",\"arguments\":{{\"table_id\":\"{table_id_wire}\",\"column_id\":\"{default_column_id_wire}\"}}}}"
        ),
        60_000_000,
    )?;
    let default_drop_operation_id = default_drop_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN default-drop operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN default-drop operation ID")?;
    let default_drop = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        28,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{default_drop_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(default_drop.contains("\"outcome\":\"committed\""));
    let column_rename_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        33,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tables.rename_column\",\"arguments\":{{\"table_id\":\"{table_id_wire}\",\"column_id\":\"{first_column_id_wire}\",\"name\":\"row_id\"}}}}"
        ),
        60_000_000,
    )?;
    let column_rename_id = column_rename_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN column-rename operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN column-rename operation ID")?;
    let column_rename = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        34,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{column_rename_id}\"}}"),
        60_000_000,
    )?;
    assert!(column_rename.contains("\"outcome\":\"committed\""));
    assert!(column_rename.contains(&format!("\"column_id\":\"{first_column_id_wire}\"")));
    assert!(column_rename.contains("\"name\":\"row_id\""));
    let owner_transfer_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        35,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tables.set_owner\",\"arguments\":{{\"table_id\":\"{table_id_wire}\",\"owner_id\":\"{created_user_id_wire}\"}}}}"
        ),
        60_000_000,
    )?;
    let owner_transfer_id = owner_transfer_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN table-owner transfer operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN table-owner transfer operation ID")?;
    let owner_transfer = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        36,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{owner_transfer_id}\"}}"),
        60_000_000,
    )?;
    assert!(owner_transfer.contains("\"outcome\":\"committed\""));
    assert!(owner_transfer.contains(&format!("\"owner_id\":\"{created_user_id_wire}\"")));
    let table_drop_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        37,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"tables.drop\",\"arguments\":{{\"table_id\":\"{table_id_wire}\"}}}}"
        ),
        60_000_000,
    )?;
    let table_drop_id = table_drop_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN table-drop operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN table-drop operation ID")?;
    let table_drop = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        38,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{table_drop_id}\"}}"),
        60_000_000,
    )?;
    assert!(table_drop.contains("\"outcome\":\"committed\""));
    assert!(table_drop.contains(&format!("\"table_id\":\"{table_id_wire}\"")));
    let admin_id_wire = admin_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let empty_schema_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        39,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"schemas.create\",\"arguments\":{{\"name\":\"protocol_empty\",\"owner_id\":\"{admin_id_wire}\"}}}}"
        ),
        60_000_000,
    )?;
    let empty_schema_create_id = empty_schema_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing empty-schema create operation ID")?
        .get(..32)
        .ok_or("invalid empty-schema create operation ID")?;
    let empty_schema_create = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        40,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{empty_schema_create_id}\"}}"),
        60_000_000,
    )?;
    let empty_schema_id = empty_schema_create
        .split("\"schema_id\":\"")
        .nth(1)
        .ok_or("missing empty schema ID")?
        .get(..32)
        .ok_or("invalid empty schema ID")?;
    let schema_owner_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        41,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"schemas.set_owner\",\"arguments\":{{\"schema_id\":\"{empty_schema_id}\",\"owner_id\":\"{created_user_id_wire}\"}}}}"
        ),
        60_000_000,
    )?;
    let schema_owner_operation_id = schema_owner_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN schema-owner transfer operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN schema-owner transfer operation ID")?;
    let schema_owner_transfer = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        42,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{schema_owner_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(schema_owner_transfer.contains("\"outcome\":\"committed\""));
    assert!(schema_owner_transfer.contains(&format!("\"owner_id\":\"{created_user_id_wire}\"")));
    let schema_rename_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        43,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"schemas.rename\",\"arguments\":{{\"schema_id\":\"{empty_schema_id}\",\"name\":\"protocol_renamed\"}}}}"
        ),
        60_000_000,
    )?;
    let schema_rename_operation_id = schema_rename_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN schema-rename operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN schema-rename operation ID")?;
    let schema_rename = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        44,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{schema_rename_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(schema_rename.contains("\"outcome\":\"committed\""));
    assert!(schema_rename.contains("\"name\":\"protocol_renamed\""));
    let empty_schema_drop_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        45,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"schemas.drop\",\"arguments\":{{\"schema_id\":\"{empty_schema_id}\"}}}}"
        ),
        60_000_000,
    )?;
    let empty_schema_drop_id = empty_schema_drop_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing empty-schema drop operation ID")?
        .get(..32)
        .ok_or("invalid empty-schema drop operation ID")?;
    let empty_schema_drop = admin_dispatch_json(
        &storage,
        &staging,
        &mut control,
        46,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{empty_schema_drop_id}\"}}"),
        60_000_000,
    )?;
    assert!(empty_schema_drop.contains("\"outcome\":\"committed\""));
    assert!(empty_schema_drop.contains(&format!("\"schema_id\":\"{empty_schema_id}\"")));

    let mut foreign_key_setup_audit = identity_audit_event(0xd1, admin_role, id(80));
    foreign_key_setup_audit.action = AuditAction::AuthorityChange;
    let mut fk_setup_writes = foreign_key_catalog_writes(true, true)
        .into_iter()
        .filter(|write| write.key != id_catalog_key(6, id(84)))
        .collect::<Vec<_>>();
    fk_setup_writes.push(column_write_not_null(id(84), id(82), "object_84"));
    storage
        .commit_catalog_and_audit(
            id(0xd2),
            admin_role,
            60_000_000,
            &fk_setup_writes,
            &[foreign_key_setup_audit],
        )
        .map_err(|error| format!("FK ADMIN fixture setup failed: {error:?}"))?;
    let mut references_audit = identity_audit_event(0xd3, admin_role, backup_admin_id);
    references_audit.action = AuditAction::AuthorityChange;
    let mut references_admin_epoch =
        principal_write_with_name(backup_admin_id, 1, 2, "backup_admin");
    references_admin_epoch.expected_value =
        principal_write_with_name(backup_admin_id, 1, 1, "backup_admin").payload;
    storage
        .commit_catalog_and_audit(
            id(0xd3),
            admin_role,
            60_000_000,
            &[
                grant_write(backup_admin_id, id(81), 6, 1, id(81), admin_role),
                dependency_write(backup_admin_id, id(81), 5),
                references_admin_epoch,
            ],
            &[references_audit],
        )
        .map_err(|error| format!("FK REFERENCES grant setup failed: {error:?}"))?;
    let source_table_id_wire = id(82)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let source_column_id_wire = id(84)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let target_constraint_id_wire = id(85)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let mut foreign_key_control = ClientConnectionSession::default();
    foreign_key_control.accept_hello(ClientMode::Control, "fk-admin")?;
    foreign_key_control.authenticate(&storage, id(236), &[0x6a; 32])?;
    let unique_table_id_wire = id(82)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let unique_column_id_wire = id(84)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let primary_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        47,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"constraints.create_primary_key\",\"arguments\":{{\"table_id\":\"{unique_table_id_wire}\",\"column_ids\":[\"{unique_column_id_wire}\"]}}}}"
        ),
        60_000_000,
    )?;
    let primary_operation_id = primary_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN PRIMARY KEY operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN PRIMARY KEY operation ID")?;
    let primary_create = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        48,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{primary_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(primary_create.contains("\"outcome\":\"committed\""));
    assert!(primary_create.contains("\"index_id\":"));
    let primary_constraint_id_wire = primary_create
        .split("\"constraint_id\":\"")
        .nth(1)
        .ok_or("missing created PRIMARY KEY ID")?
        .get(..32)
        .ok_or("invalid created PRIMARY KEY ID")?;
    let unique_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        49,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"constraints.create_unique\",\"arguments\":{{\"table_id\":\"{unique_table_id_wire}\",\"column_ids\":[\"{unique_column_id_wire}\"]}}}}"
        ),
        60_000_000,
    )?;
    let unique_operation_id = unique_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN UNIQUE operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN UNIQUE operation ID")?;
    let unique_create = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        50,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{unique_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(unique_create.contains("\"outcome\":\"committed\""));
    assert!(unique_create.contains("\"constraint_id\":"));
    let unique_constraint_id_wire = unique_create
        .split("\"constraint_id\":\"")
        .nth(1)
        .ok_or("missing created UNIQUE ID")?
        .get(..32)
        .ok_or("invalid created UNIQUE ID")?;
    let check_eir = prdb_engine::expression::encode_expression(
        &prdb_engine::authorization::Expr::Literal(Some(ScalarValue::Boolean(true))),
    )?;
    let check_eir_hex = check_eir
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let check_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        51,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"constraints.create_check\",\"arguments\":{{\"table_id\":\"{unique_table_id_wire}\",\"expression_eir1\":\"{check_eir_hex}\",\"dependency_column_ids\":[]}}}}"
        ),
        60_000_000,
    )?;
    let check_operation_id = check_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN CHECK operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN CHECK operation ID")?;
    let check_create = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        52,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{check_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(check_create.contains("\"outcome\":\"committed\""));
    let check_constraint_id_wire = check_create
        .split("\"constraint_id\":\"")
        .nth(1)
        .ok_or("missing created CHECK constraint ID")?
        .get(..32)
        .ok_or("invalid created CHECK constraint ID")?;
    let check_drop_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        53,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"constraints.drop_check\",\"arguments\":{{\"constraint_id\":\"{check_constraint_id_wire}\"}}}}"
        ),
        60_000_000,
    )?;
    let check_drop_operation_id = check_drop_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN CHECK-drop operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN CHECK-drop operation ID")?;
    let check_drop = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        54,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{check_drop_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(check_drop.contains("\"outcome\":\"committed\""));
    let foreign_key_create_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        55,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"foreign_keys.create\",\"arguments\":{{\"source_table_id\":\"{source_table_id_wire}\",\"source_column_ids\":[\"{source_column_id_wire}\"],\"target_constraint_id\":\"{target_constraint_id_wire}\"}}}}"
        ),
        60_000_000,
    )?;
    let foreign_key_create_operation_id = foreign_key_create_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN foreign-key-create operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN foreign-key-create operation ID")?;
    let foreign_key_create = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        56,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{foreign_key_create_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(foreign_key_create.contains("\"outcome\":\"committed\""));
    let foreign_key_id_wire = foreign_key_create
        .split("\"foreign_key_id\":\"")
        .nth(1)
        .ok_or("missing created foreign-key ID")?
        .get(..32)
        .ok_or("invalid created foreign-key ID")?;
    let foreign_key_drop_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        57,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"foreign_keys.drop\",\"arguments\":{{\"foreign_key_id\":\"{foreign_key_id_wire}\"}}}}"
        ),
        60_000_000,
    )?;
    let foreign_key_drop_operation_id = foreign_key_drop_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN foreign-key-drop operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN foreign-key-drop operation ID")?;
    let foreign_key_drop = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        58,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{foreign_key_drop_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(foreign_key_drop.contains("\"outcome\":\"committed\""));
    assert!(foreign_key_drop.contains(&format!("\"foreign_key_id\":\"{foreign_key_id_wire}\"")));
    let unique_drop_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        59,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"constraints.drop_unique\",\"arguments\":{{\"constraint_id\":\"{unique_constraint_id_wire}\"}}}}"
        ),
        60_000_000,
    )?;
    let unique_drop_operation_id = unique_drop_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN UNIQUE-drop operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN UNIQUE-drop operation ID")?;
    let unique_drop = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        60,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{unique_drop_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(unique_drop.contains("\"outcome\":\"committed\""));
    let index_create_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        63,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"indexes.create\",\"arguments\":{{\"table_id\":\"{unique_table_id_wire}\",\"name\":\"protocol_lookup\",\"column_ids\":[\"{unique_column_id_wire}\"],\"unique\":false}}}}"
        ),
        60_000_000,
    )?;
    let index_create_operation_id = index_create_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN index-create operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN index-create operation ID")?;
    let index_create = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        64,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{index_create_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(index_create.contains("\"outcome\":\"committed\""));
    let index_id_wire = index_create
        .split("\"index_id\":\"")
        .nth(1)
        .ok_or("missing created ordinary index ID")?
        .get(..32)
        .ok_or("invalid created ordinary index ID")?;
    let index_rebuild_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        65,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"indexes.rebuild\",\"arguments\":{{\"index_id\":\"{index_id_wire}\",\"column_ids\":[\"{unique_column_id_wire}\"],\"unique\":true}}}}"
        ),
        60_000_000,
    )?;
    let index_rebuild_operation_id = index_rebuild_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN index-rebuild operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN index-rebuild operation ID")?;
    let index_rebuild = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        66,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{index_rebuild_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(index_rebuild.contains("\"outcome\":\"committed\""));
    let index_drop_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        67,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"indexes.drop\",\"arguments\":{{\"index_id\":\"{index_id_wire}\"}}}}"
        ),
        60_000_000,
    )?;
    let index_drop_operation_id = index_drop_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN index-drop operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN index-drop operation ID")?;
    let index_drop = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        68,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{index_drop_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(index_drop.contains("\"outcome\":\"committed\""));
    assert!(index_drop.contains(&format!("\"index_id\":\"{index_id_wire}\"")));
    let primary_drop_prepare = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        69,
        &format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"constraints.drop_primary_key\",\"arguments\":{{\"constraint_id\":\"{primary_constraint_id_wire}\"}}}}"
        ),
        60_000_000,
    )?;
    let primary_drop_operation_id = primary_drop_prepare
        .split("\"operation_id\":\"")
        .nth(1)
        .ok_or("missing ADMIN PRIMARY KEY-drop operation ID")?
        .get(..32)
        .ok_or("invalid ADMIN PRIMARY KEY-drop operation ID")?;
    let primary_drop = admin_dispatch_json(
        &storage,
        &staging,
        &mut foreign_key_control,
        70,
        &format!("{{\"phase\":\"start\",\"operation_id\":\"{primary_drop_operation_id}\"}}"),
        60_000_000,
    )?;
    assert!(primary_drop.contains("\"outcome\":\"committed\""));
    prdb_engine::startup::validate_persisted_records(&database)?;

    assert!(storage.list_tokens(admin_id, created_user_id)?[0].revoked);
    assert!(
        storage
            .list_tokens(admin_id, user_manager_id)?
            .iter()
            .any(|token| token.token_id == id(240) && token.revoked)
    );
    assert!(matches!(
        storage.get_administrative_invocation_status(admin_id, id(233), now_us)?,
        Some(snapshot) if matches!(snapshot.status,
            InvocationStatus::Active { cancellation_requested: true, .. })
    ));

    let revocation_cursor = storage
        .list_runtime_invocations(admin_id, now_us, 1, None)?
        .continuation
        .unwrap();
    let audit_revocation_cursor = storage
        .list_audit_events(admin_id, 1, None)?
        .continuation
        .ok_or("expected another durable audit event")?;
    let membership = membership_write_with_grantor(admin_id, admin_role, admin_role);
    let dependency = dependency_write(admin_id, admin_role, 5);
    let read = database.begin_read()?;
    let catalog = read.open_table(CATALOG)?;
    let membership_value = catalog
        .get(membership.key.as_slice())?
        .ok_or("missing admin membership")?
        .value()
        .to_vec();
    let dependency_value = catalog
        .get(dependency.key.as_slice())?
        .ok_or("missing admin dependency")?
        .value()
        .to_vec();
    drop(catalog);
    drop(read);
    let removals = [
        catalog_delete(membership, membership_value),
        catalog_delete(dependency, dependency_value),
    ];
    let mut admin_epoch = principal_write_with_name(admin_id, 1, 2, "runtime_admin");
    let read = database.begin_read()?;
    admin_epoch.expected_value = Some(
        read.open_table(CATALOG)?
            .get(id_catalog_key(10, admin_id).as_slice())?
            .ok_or("missing admin principal")?
            .value()
            .to_vec(),
    );
    drop(read);
    let mut revoke_audit = audit_event(217);
    revoke_audit.actor_id = Some(backup_admin_id);
    revoke_audit.action = AuditAction::AuthorityChange;
    storage
        .commit_authorized_membership_ddl(
            id(217),
            backup_admin_id,
            9_999,
            &[removals[0].clone(), removals[1].clone(), admin_epoch],
            &[revoke_audit],
        )
        .map_err(|error| format!("admin revocation commit: {error:?}"))?;
    assert!(matches!(
        storage.list_runtime_invocations(admin_id, now_us, 1, Some(&revocation_cursor)),
        Err(AdministrativeInvocationListError::Unauthorized)
    ));
    assert!(matches!(
        storage.list_audit_events(admin_id, 1, Some(&audit_revocation_cursor)),
        Err(prdb_engine::storage_manager::AdministrativeAuditListError::Unauthorized)
    ));

    assert!(second.commit().is_err());
    let latency_page = storage.list_runtime_invocations(backup_admin_id, now_us, 1, None)?;
    assert_eq!(
        latency_page
            .invocation_commit_latency_buckets
            .iter()
            .sum::<u64>(),
        1
    );
    first.abort()?;
    let mismatched_audit_value = audit_event(225).encode()?;
    let write = database.begin_write()?;
    write
        .open_table(AUDIT)?
        .insert([0xfd; 16].as_slice(), mismatched_audit_value.as_slice())?;
    write.commit()?;
    assert!(matches!(
        storage.list_audit_events(backup_admin_id, MAX_ADMIN_AUDIT_PAGE, None),
        Err(prdb_engine::storage_manager::AdministrativeAuditListError::Storage)
    ));
    let write = database.begin_write()?;
    {
        let mut audit = write.open_table(AUDIT)?;
        let mut sequence = 0u64;
        while audit.len()? < prdb_engine::audit::MAX_AUDIT_EVENTS as u64 {
            let mut key = [0; 16];
            key[..8].copy_from_slice(b"CAPACITY");
            key[8..].copy_from_slice(&sequence.to_be_bytes());
            sequence = sequence.saturating_add(1);
            if audit.get(key.as_slice())?.is_none() {
                audit.insert(key.as_slice(), &b"bounded-capacity-fixture"[..])?;
            }
        }
    }
    write.commit()?;
    assert!(matches!(
        storage.authenticate_query_session(id(254), &[0; 32]),
        Err(prdb_engine::transaction::TransactionError::AuditCapacityExceeded)
    ));

    Ok(())
}
