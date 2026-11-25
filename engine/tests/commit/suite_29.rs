use prdb_engine::audit::AuditAction;
use prdb_engine::outcome::{OutcomeStatus, lookup_outcome};
use prdb_engine::storage_manager::StorageManager;
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, decode_tuple, encode_tuple};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use super::helpers::*;

pub(super) fn planner_stream_estimates_same_typed_decimal_column_predicate()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::authorization::{Effect, Expr, Operation, Subject};
    use prdb_engine::ipc::{IpcFrame, Operation as IpcOperation};
    use prdb_engine::optimizer_binding::{BoundColumn, BoundPredicate};
    use prdb_engine::optimizer_request::Comparison;
    use prdb_engine::planner_ipc::{
        DescribePlanRelationsRequest, DescribePlanRelationsResponse, EstimatePredicateRequest,
        EstimatePredicateResponse, EstimateScanRequest, EstimateScanResponse, EstimateScansRequest,
        EstimateScansResponse, serve_planner,
    };
    use prdb_engine::storage_manager::{
        CreatePolicyRequest, CreateTableRequest, ObjectGrantPrivilege, ObjectGrantTarget,
        PolicyKind, SetObjectGrantRequest, StorageManagerConfig, TableColumnDefinition,
    };
    use std::net::Shutdown;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let actor = id(0xb0);
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0xb1, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage
        .commit_catalog_and_audit(
            id(0xb1),
            admin_role,
            9_999,
            &[
                principal_write_with_name(actor, 1, 1, "planner_decimal_owner"),
                object_write_with_name(actor, 8, "planner_decimal_owner"),
                membership_write(actor, admin_role),
                dependency_write(actor, admin_role, 5),
            ],
            &[setup_audit],
        )
        .map_err(|error| format!("typed planner fixture identity setup: {error:?}"))?;

    let mut schema_audit = identity_audit_event(0xb2, actor, actor);
    schema_audit.correlation_id = id(0xb2);
    let schema = storage
        .create_schema(
            id(0xb2),
            actor,
            "planner_decimal",
            actor,
            9_999,
            schema_audit,
        )
        .map_err(|error| format!("typed planner fixture schema creation: {error:?}"))?;
    let mut table_audit = identity_audit_event(0xb3, actor, schema.schema_id);
    table_audit.correlation_id = id(0xb3);
    let decimal_type = ScalarType::Decimal {
        precision: 8,
        scale: 2,
    };
    let table = storage
        .create_table(CreateTableRequest {
            invocation_id: id(0xb3),
            authenticated_actor_id: actor,
            schema_id: schema.schema_id,
            name: "typed_observations".to_owned(),
            columns: vec![
                TableColumnDefinition {
                    name: "left_value".to_owned(),
                    scalar_type: decimal_type,
                    nullable: true,
                    generated_uuid_identity: false,
                    default: None,
                },
                TableColumnDefinition {
                    name: "right_value".to_owned(),
                    scalar_type: decimal_type,
                    nullable: true,
                    generated_uuid_identity: false,
                    default: None,
                },
                TableColumnDefinition {
                    name: "large_payload".to_owned(),
                    scalar_type: ScalarType::Text,
                    nullable: false,
                    generated_uuid_identity: false,
                    default: None,
                },
            ],
            retention_deadline_us: 9_999,
            audit_event: table_audit,
        })
        .map_err(|error| format!("typed planner fixture table creation: {error:?}"))?;
    let mut hidden_table_audit = identity_audit_event(0xbd, actor, schema.schema_id);
    hidden_table_audit.correlation_id = id(0xbd);
    storage
        .create_table(CreateTableRequest {
            invocation_id: id(0xbd),
            authenticated_actor_id: actor,
            schema_id: schema.schema_id,
            name: "hidden_observations".to_owned(),
            columns: vec![TableColumnDefinition {
                name: "secret_value".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            }],
            retention_deadline_us: 9_999,
            audit_event: hidden_table_audit,
        })
        .map_err(|error| format!("typed planner hidden table creation: {error:?}"))?;

    for (audit_id, privilege, target) in [
        (0xb4, ObjectGrantPrivilege::Usage, ObjectGrantTarget::Object),
        (
            0xb5,
            ObjectGrantPrivilege::Select,
            ObjectGrantTarget::Object,
        ),
        (
            0xb6,
            ObjectGrantPrivilege::Select,
            ObjectGrantTarget::Column(table.column_ids[0]),
        ),
        (
            0xb7,
            ObjectGrantPrivilege::Select,
            ObjectGrantTarget::Column(table.column_ids[1]),
        ),
        (
            0xbf,
            ObjectGrantPrivilege::Select,
            ObjectGrantTarget::Column(table.column_ids[2]),
        ),
    ] {
        let object_id = if privilege == ObjectGrantPrivilege::Usage {
            schema.schema_id
        } else {
            table.table_id
        };
        let invocation_id = id(audit_id);
        let mut audit = identity_audit_event(audit_id, actor, object_id);
        audit.correlation_id = invocation_id;
        audit.action = AuditAction::AuthorityChange;
        storage
            .set_object_grant(SetObjectGrantRequest {
                invocation_id,
                authenticated_actor_id: actor,
                grantee_id: actor,
                object_id,
                privilege,
                target,
                grant: true,
                retention_deadline_us: 9_999,
                audit_event: audit,
            })
            .map_err(|error| format!("typed planner fixture grant creation: {error:?}"))?;
    }

    let mut row_policy_audit = identity_audit_event(0xb8, actor, table.table_id);
    row_policy_audit.correlation_id = id(0xb8);
    storage
        .create_policy(CreatePolicyRequest {
            invocation_id: id(0xb8),
            authenticated_actor_id: actor,
            table_id: table.table_id,
            name: "planner_decimal_select".to_owned(),
            kind: PolicyKind::Row,
            operation: Operation::Select,
            effect: Effect::Allow,
            recipients: vec![Subject::Principal(actor)],
            target_column_ids: Vec::new(),
            using_expression: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check_expression: None,
            retention_deadline_us: 9_999,
            audit_event: row_policy_audit,
        })
        .map_err(|error| format!("typed planner fixture row policy: {error:?}"))?;
    for (audit_id, column_id) in [
        (0xb9, table.column_ids[0]),
        (0xba, table.column_ids[1]),
        (0xc0, table.column_ids[2]),
    ] {
        let mut audit = identity_audit_event(audit_id, actor, table.table_id);
        audit.correlation_id = id(audit_id);
        storage
            .create_policy(CreatePolicyRequest {
                invocation_id: id(audit_id),
                authenticated_actor_id: actor,
                table_id: table.table_id,
                name: format!("planner_decimal_read_{}", audit_id),
                kind: PolicyKind::Cell,
                operation: Operation::Select,
                effect: Effect::Allow,
                recipients: vec![Subject::Principal(actor)],
                target_column_ids: vec![column_id],
                using_expression: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
                with_check_expression: None,
                retention_deadline_us: 9_999,
                audit_event: audit,
            })
            .map_err(|error| format!("typed planner fixture cell policy: {error:?}"))?;
    }

    let mut writer = storage.begin_invocation(id(0xbb), actor, 9_999, 8, 4096)?;
    let row_schema = [
        ColumnSchema {
            scalar_type: decimal_type,
            nullable: true,
        },
        ColumnSchema {
            scalar_type: decimal_type,
            nullable: true,
        },
        ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: false,
        },
    ];
    for (row_id, left, right) in [
        (1, Some(110), Some(110)),
        (2, Some(220), Some(330)),
        (3, None, None),
    ] {
        writer.stage_put(
            prdb_engine::overlay::RowId {
                table: table.table_id,
                row: id(row_id),
            },
            &encode_tuple(
                &row_schema,
                1,
                &[
                    left.map(ScalarValue::Decimal),
                    right.map(ScalarValue::Decimal),
                    Some(ScalarValue::Text(String::new())),
                ],
            )?,
        )?;
    }
    writer.commit()?;
    assert_eq!(
        storage.collect_committed_table_population(table.table_id)?,
        prdb_engine::statistics::StatisticsCollectionOutcome::Exact,
        "the snapshot estimate test needs exact committed population metadata"
    );

    let invocation_id = id(0xbc);
    let mut invocation = storage.begin_invocation(invocation_id, actor, 9_999, 8, 4096)?;
    let predicate = BoundPredicate::Compare {
        operator: Comparison::Equal,
        left: Box::new(BoundPredicate::Column(BoundColumn {
            column_id: table.column_ids[0],
            ordinal: 0,
            scalar_type: decimal_type,
            nullable: true,
        })),
        right: Box::new(BoundPredicate::Column(BoundColumn {
            column_id: table.column_ids[1],
            ordinal: 1,
            scalar_type: decimal_type,
            nullable: true,
        })),
    };
    let (mut client, mut server) = std::os::unix::net::UnixStream::pair()?;
    client.set_read_timeout(Some(Duration::from_secs(5)))?;
    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let planner = scope.spawn(|| serve_planner(&mut server, &mut invocation));
        prdb_engine::ipc::write_frame(
            &mut client,
            &IpcFrame {
                operation: IpcOperation::EstimateScan as u16,
                request_id: 1,
                payload: EstimatePredicateRequest::from_bound_predicate(
                    "planner_decimal".to_owned(),
                    "typed_observations".to_owned(),
                    &predicate,
                )?
                .encode()?,
            },
        )?;
        let response = prdb_engine::ipc::read_frame(&mut client)?
            .ok_or("typed planner stream closed before predicate estimate")?;
        assert_eq!(response.request_id, 1);
        assert_eq!(
            EstimatePredicateResponse::decode(&response.payload)?,
            EstimatePredicateResponse::Exact {
                visible_rows: 3,
                matching_rows: 1,
            }
        );
        prdb_engine::ipc::write_frame(
            &mut client,
            &IpcFrame {
                operation: IpcOperation::DescribePlanRelations as u16,
                request_id: 2,
                payload: DescribePlanRelationsRequest {
                    relations: vec![
                        (
                            "planner_decimal".to_owned(),
                            "typed_observations".to_owned(),
                        ),
                        ("planner_decimal".to_owned(), "missing_table".to_owned()),
                        (
                            "planner_decimal".to_owned(),
                            "hidden_observations".to_owned(),
                        ),
                    ],
                }
                .encode()?,
            },
        )?;
        let response = prdb_engine::ipc::read_frame(&mut client)?
            .ok_or("planner stream closed before relation batch")?;
        let described = DescribePlanRelationsResponse::decode(&response.payload)?;
        assert_eq!(described.entries.len(), 3);
        assert_eq!(
            described.entries[0]
                .response
                .relation
                .as_ref()
                .map(|relation| relation.relation.name.as_str()),
            Some("typed_observations")
        );
        assert!(described.entries[0].handle.is_some());
        assert!(described.entries[1].handle.is_none());
        assert!(described.entries[1].response.relation.is_none());
        assert!(described.entries[2].handle.is_none());
        assert!(described.entries[2].response.relation.is_none());
        assert_eq!(described.entries[1], described.entries[2]);
        let mut concurrent_writer = storage.begin_invocation(id(0xbe), actor, 9_999, 1, 128)?;
        concurrent_writer.stage_put(
            prdb_engine::overlay::RowId {
                table: table.table_id,
                row: id(4),
            },
            &encode_tuple(
                &[
                    ColumnSchema {
                        scalar_type: decimal_type,
                        nullable: true,
                    },
                    ColumnSchema {
                        scalar_type: decimal_type,
                        nullable: true,
                    },
                    ColumnSchema {
                        scalar_type: ScalarType::Text,
                        nullable: false,
                    },
                ],
                1,
                &[
                    Some(ScalarValue::Decimal(440)),
                    Some(ScalarValue::Decimal(440)),
                    Some(ScalarValue::Text(String::new())),
                ],
            )?,
        )?;
        concurrent_writer.commit()?;
        prdb_engine::ipc::write_frame(
            &mut client,
            &IpcFrame {
                operation: IpcOperation::EstimateScans as u16,
                request_id: 3,
                payload: EstimateScansRequest {
                    entries: vec![EstimateScanRequest {
                        handle: described.entries[0].handle.clone().unwrap(),
                        column_ids: vec![table.column_ids[0]],
                    }],
                }
                .encode()?,
            },
        )?;
        let response = prdb_engine::ipc::read_frame(&mut client)?
            .ok_or("planner stream closed before estimate batch")?;
        let estimates = EstimateScansResponse::decode(&response.payload)?;
        assert_eq!(estimates.entries.len(), 1);
        assert_eq!(
            estimates.entries[0],
            EstimateScanResponse::Unknown,
            "batched summary may conservatively become UNKNOWN after concurrent mutation"
        );
        let post_mutation_predicate = BoundPredicate::Compare {
            operator: Comparison::Equal,
            left: Box::new(BoundPredicate::Column(BoundColumn {
                column_id: table.column_ids[0],
                ordinal: 0,
                scalar_type: decimal_type,
                nullable: true,
            })),
            right: Box::new(BoundPredicate::Decimal(440)),
        };
        prdb_engine::ipc::write_frame(
            &mut client,
            &IpcFrame {
                operation: IpcOperation::EstimateScan as u16,
                request_id: 4,
                payload: EstimatePredicateRequest::from_bound_predicate(
                    "planner_decimal".to_owned(),
                    "typed_observations".to_owned(),
                    &post_mutation_predicate,
                )?
                .encode()?,
            },
        )?;
        let response = prdb_engine::ipc::read_frame(&mut client)?
            .ok_or("planner stream closed before post-mutation predicate estimate")?;
        assert_eq!(
            EstimatePredicateResponse::decode(&response.payload)?,
            EstimatePredicateResponse::Exact {
                visible_rows: 3,
                matching_rows: 0,
            },
            "planner predicate estimate must use the invocation snapshot, not the new committed row"
        );
        client.shutdown(Shutdown::Write)?;
        drop(client);
        planner
            .join()
            .expect("typed planner server thread panicked")
            .map_err(|error| format!("typed planner server failed: {error:?}"))?;
        Ok(())
    })?;
    invocation.abort()?;
    assert_eq!(
        storage
            .lookup_invocation_outcome(invocation_id, actor, 9_998)?
            .ok_or("typed planner invocation outcome missing")?
            .status,
        OutcomeStatus::Aborted
    );

    assert_eq!(
        storage.collect_committed_table_population(table.table_id)?,
        prdb_engine::statistics::StatisticsCollectionOutcome::Exact,
        "the staged-overlay estimate needs fresh committed population metadata"
    );

    struct StagedOverlayEstimate<'a> {
        table_id: [u8; 16],
        row_payload: &'a [u8],
        decimal_type: ScalarType,
        key_column_id: [u8; 16],
        row_count: u16,
    }

    fn staged_overlay_predicate_estimate(
        storage: &StorageManager<'_>,
        invocation_id: [u8; 16],
        actor: [u8; 16],
        estimate: StagedOverlayEstimate<'_>,
    ) -> Result<(EstimatePredicateResponse, Duration), Box<dyn std::error::Error>> {
        let mut invocation = storage.begin_invocation(
            invocation_id,
            actor,
            9_999,
            usize::from(estimate.row_count) + 8,
            32 * 1024 * 1024,
        )?;
        for offset in 0..estimate.row_count {
            let row_number = offset + 4;
            let mut row_id = [0; 16];
            row_id[14..].copy_from_slice(&row_number.to_be_bytes());
            invocation.stage_put(
                prdb_engine::overlay::RowId {
                    table: estimate.table_id,
                    row: row_id,
                },
                estimate.row_payload,
            )?;
        }
        let predicate = BoundPredicate::Compare {
            operator: Comparison::Equal,
            left: Box::new(BoundPredicate::Column(BoundColumn {
                column_id: estimate.key_column_id,
                ordinal: 0,
                scalar_type: estimate.decimal_type,
                nullable: true,
            })),
            right: Box::new(BoundPredicate::Decimal(440)),
        };
        let (mut client, mut server) = std::os::unix::net::UnixStream::pair()?;
        client.set_read_timeout(Some(Duration::from_secs(15)))?;
        let (response, latency) = std::thread::scope(|scope| {
            let planner = scope.spawn(|| serve_planner(&mut server, &mut invocation));
            let started_at = std::time::Instant::now();
            prdb_engine::ipc::write_frame(
                &mut client,
                &IpcFrame {
                    operation: IpcOperation::EstimateScan as u16,
                    request_id: 1,
                    payload: EstimatePredicateRequest::from_bound_predicate(
                        "planner_decimal".to_owned(),
                        "typed_observations".to_owned(),
                        &predicate,
                    )?
                    .encode()?,
                },
            )?;
            let frame = prdb_engine::ipc::read_frame(&mut client)?
                .ok_or("planner stream closed during staged-overlay estimate")?;
            let latency = started_at.elapsed();
            let response = EstimatePredicateResponse::decode(&frame.payload)?;
            client.shutdown(Shutdown::Write)?;
            drop(client);
            planner
                .join()
                .expect("staged-overlay planner thread panicked")
                .map_err(|error| format!("staged-overlay planner failed: {error:?}"))?;
            Ok::<_, Box<dyn std::error::Error>>((response, latency))
        })?;
        invocation.abort()?;
        Ok((response, latency))
    }

    let large_payload = "x".repeat(17 * 1024);
    let large_row_payload = encode_tuple(
        &row_schema,
        1,
        &[
            Some(ScalarValue::Decimal(440)),
            Some(ScalarValue::Decimal(440)),
            Some(ScalarValue::Text(large_payload)),
        ],
    )?;
    let staged_rows = 1_023u16;
    let staged_payload_bytes = large_row_payload.len() * usize::from(staged_rows);
    assert!(staged_payload_bytes > 16 * 1024 * 1024);
    let (default_estimate, default_latency) = staged_overlay_predicate_estimate(
        &storage,
        id(0xc2),
        actor,
        StagedOverlayEstimate {
            table_id: table.table_id,
            row_payload: &large_row_payload,
            decimal_type,
            key_column_id: table.column_ids[0],
            row_count: staged_rows,
        },
    )?;
    assert_eq!(default_estimate, EstimatePredicateResponse::Unknown);
    let elevated_storage = StorageManager::with_config(
        &database,
        StorageManagerConfig {
            max_authorized_index_range_bytes: 32 * 1024 * 1024,
            ..StorageManagerConfig::default()
        },
    )?;
    let (elevated_estimate, elevated_latency) = staged_overlay_predicate_estimate(
        &elevated_storage,
        id(0xc3),
        actor,
        StagedOverlayEstimate {
            table_id: table.table_id,
            row_payload: &large_row_payload,
            decimal_type,
            key_column_id: table.column_ids[0],
            row_count: staged_rows,
        },
    )?;
    assert_eq!(
        elevated_estimate,
        EstimatePredicateResponse::Exact {
            visible_rows: 4 + u64::from(staged_rows),
            matching_rows: 1 + u64::from(staged_rows),
        }
    );
    eprintln!(
        "planner predicate estimate IPC latency for >16 MiB private overlay: default UNKNOWN {:.3} ms, elevated exact {:.3} ms ({} staged bytes)",
        default_latency.as_secs_f64() * 1000.0,
        elevated_latency.as_secs_f64() * 1000.0,
        staged_payload_bytes,
    );
    Ok(())
}

pub(super) fn catalog_backed_executor_generates_uuid_identity_and_rejects_override_and_update()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::authorization::{Effect, Expr, Operation, Subject};
    use prdb_engine::storage_manager::{
        CreatePolicyRequest, CreateTableRequest, ObjectGrantPrivilege, ObjectGrantTarget,
        PolicyKind, SetObjectGrantRequest, TableColumnDefinition,
    };
    use prdb_engine::transaction::{InsertInput, TransactionError};
    use prdb_engine::tuple::FieldPatch;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let admin_role = builtin_owner_id();
    let actor = id(0x91);
    let mut setup_audit = identity_audit_event(0x92, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage
        .commit_catalog_and_audit(
            id(0x92),
            admin_role,
            9_999,
            &[
                principal_write_with_name(actor, 1, 1, "generated_identity_owner"),
                object_write_with_name(actor, 8, "generated_identity_owner"),
                membership_write(actor, admin_role),
                dependency_write(actor, admin_role, 5),
            ],
            &[setup_audit],
        )
        .map_err(|error| format!("generated identity actor setup: {error:?}"))?;

    let schema_invocation = id(0x93);
    let mut schema_audit = identity_audit_event(0x93, actor, actor);
    schema_audit.correlation_id = schema_invocation;
    let schema = storage
        .create_schema(
            schema_invocation,
            actor,
            "generated_identity",
            actor,
            9_999,
            schema_audit,
        )
        .map_err(|error| format!("generated identity schema creation: {error:?}"))?;

    let table_invocation = id(0x94);
    let mut table_audit = identity_audit_event(0x94, actor, schema.schema_id);
    table_audit.correlation_id = table_invocation;
    let table = storage
        .create_table(CreateTableRequest {
            invocation_id: table_invocation,
            authenticated_actor_id: actor,
            schema_id: schema.schema_id,
            name: "records".to_owned(),
            columns: vec![TableColumnDefinition {
                name: "record_id".to_owned(),
                scalar_type: ScalarType::Uuid,
                nullable: false,
                generated_uuid_identity: true,
                default: None,
            }],
            retention_deadline_us: 9_999,
            audit_event: table_audit,
        })
        .map_err(|error| format!("generated identity table creation: {error:?}"))?;

    for (ordinal, (object_id, privilege, target)) in [
        (
            schema.schema_id,
            ObjectGrantPrivilege::Usage,
            ObjectGrantTarget::Object,
        ),
        (
            table.table_id,
            ObjectGrantPrivilege::Select,
            ObjectGrantTarget::Object,
        ),
        (
            table.table_id,
            ObjectGrantPrivilege::Insert,
            ObjectGrantTarget::Object,
        ),
        (
            table.table_id,
            ObjectGrantPrivilege::Update,
            ObjectGrantTarget::Object,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let invocation_id = id(0x95 + ordinal as u8);
        let mut grant_audit = identity_audit_event(0x95 + ordinal as u8, actor, object_id);
        grant_audit.correlation_id = invocation_id;
        grant_audit.action = AuditAction::AuthorityChange;
        storage
            .set_object_grant(SetObjectGrantRequest {
                invocation_id,
                authenticated_actor_id: actor,
                grantee_id: actor,
                object_id,
                privilege,
                target,
                grant: true,
                retention_deadline_us: 9_999,
                audit_event: grant_audit,
            })
            .map_err(|error| format!("generated identity grant creation: {error:?}"))?;
    }

    let policy_invocation = id(0x9a);
    let mut policy_audit = identity_audit_event(0x9a, actor, table.table_id);
    policy_audit.correlation_id = policy_invocation;
    storage
        .create_policy(CreatePolicyRequest {
            invocation_id: policy_invocation,
            authenticated_actor_id: actor,
            table_id: table.table_id,
            name: "generated_identity_insert".to_owned(),
            kind: PolicyKind::Row,
            operation: Operation::Insert,
            effect: Effect::Allow,
            recipients: vec![Subject::Principal(actor)],
            target_column_ids: Vec::new(),
            using_expression: None,
            with_check_expression: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            retention_deadline_us: 9_999,
            audit_event: policy_audit,
        })
        .map_err(|error| format!("generated identity INSERT policy creation: {error:?}"))?;

    let generated_column = ColumnSchema {
        scalar_type: ScalarType::Uuid,
        nullable: false,
    };
    let mut override_invocation = storage.begin_invocation(id(0x99), actor, 9_999, 8, 512)?;
    {
        let mut executor = override_invocation.open_executor_metadata_connection();
        let (relation, _) = executor
            .resolve_relation("generated_identity", "records")
            .map_err(|error| format!("generated identity relation resolve: {error:?}"))?
            .ok_or("generated identity relation was not visible")?;
        assert!(matches!(
            executor.stage_insert(
                &relation,
                &[InsertInput::Value(Some(ScalarValue::Uuid([0x9a; 16])))],
            ),
            Err(TransactionError::InvalidContext)
        ));
    }
    assert_eq!(
        lookup_outcome(&database, id(0x99), actor, 1)?
            .ok_or("generated identity override has no durable outcome")?
            .status,
        OutcomeStatus::Aborted
    );

    let mut update_invocation = storage.begin_invocation(id(0x9b), actor, 9_999, 8, 512)?;
    {
        let mut executor = update_invocation.open_executor_metadata_connection();
        let (relation, _) = executor
            .resolve_relation("generated_identity", "records")
            .map_err(|error| format!("generated identity relation resolve: {error:?}"))?
            .ok_or("generated identity relation was not visible")?;
        let row_handle = executor
            .stage_insert(&relation, &[InsertInput::Default])
            .map_err(|error| format!("generated identity default insert: {error:?}"))?;
        assert!(matches!(
            executor.stage_update(
                &row_handle,
                &[FieldPatch {
                    ordinal: 0,
                    value: Some(ScalarValue::Uuid([0x9c; 16])),
                }],
            ),
            Err(TransactionError::InvalidContext)
        ));
    }
    assert_eq!(
        lookup_outcome(&database, id(0x9b), actor, 1)?
            .ok_or("generated identity update has no durable outcome")?
            .status,
        OutcomeStatus::Aborted
    );

    let mut insert_invocation = storage.begin_invocation(id(0x9d), actor, 9_999, 8, 512)?;
    let row_id = {
        let mut executor = insert_invocation.open_executor_metadata_connection();
        let (relation, _) = executor
            .resolve_relation("generated_identity", "records")
            .map_err(|error| format!("generated identity relation resolve: {error:?}"))?
            .ok_or("generated identity relation was not visible")?;
        let row_handle = executor
            .stage_insert(&relation, &[InsertInput::Default])
            .map_err(|error| format!("generated identity default insert: {error:?}"))?;
        executor.revalidate_row_handle(&row_handle)?
    };
    let payload = insert_invocation
        .read(row_id)?
        .ok_or("generated identity row was not staged")?;
    let tuple = decode_tuple(std::slice::from_ref(&generated_column), 1, &payload)?;
    let generated_id = match tuple.values.first() {
        Some(Some(ScalarValue::Uuid(value))) if *value != [0; 16] => *value,
        _ => return Err("storage did not materialize a nonzero UUID identity".into()),
    };
    insert_invocation.commit()?;

    let stored = row(&database, &table_row_key(table.table_id, row_id.row))?
        .ok_or("committed generated identity row was not stored")?;
    let committed_tuple =
        decode_tuple(std::slice::from_ref(&generated_column), 1, &stored.payload)?;
    assert_eq!(
        committed_tuple.values,
        [Some(ScalarValue::Uuid(generated_id))]
    );
    assert_eq!(
        lookup_outcome(&database, id(0x9d), actor, 1)?
            .ok_or("generated identity insert has no durable outcome")?
            .status,
        OutcomeStatus::Committed
    );

    // Exercise the commit-time collision guard with a deterministic UUID. This
    // low-level staging entry point is trusted-only; the executor path above
    // remains the only caller-facing path that materializes generated values.
    let schema_target = prdb_engine::authorization::AccessTarget::Schema(schema.schema_id);
    let table_target = prdb_engine::authorization::AccessTarget::Table(table.table_id);
    let context = prdb_engine::authorization::AuthorizationContext {
        principal_id: actor,
        role_ids: Default::default(),
        scope: [
            (Operation::Usage, schema_target),
            (Operation::Insert, table_target),
        ]
        .into(),
    };
    let grants = [
        prdb_engine::authorization::Grant {
            subject: Subject::Principal(actor),
            operation: Operation::Usage,
            target: schema_target,
        },
        prdb_engine::authorization::Grant {
            subject: Subject::Principal(actor),
            operation: Operation::Insert,
            target: table_target,
        },
    ];
    let policies = [prdb_engine::authorization::Policy {
        subjects: vec![Subject::Principal(actor)],
        operation: Operation::Insert,
        target: table_target,
        effect: Effect::Allow,
        using: None,
        with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
    }];
    let projection = prdb_engine::authorization::AuthorizedProjection {
        columns: std::slice::from_ref(&generated_column),
        column_ids: &table.column_ids,
        schema_version: 1,
        schema_id: schema.schema_id,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let mut duplicate = prdb_engine::transaction::InvocationTransaction::begin(
        &database,
        id(0x9e),
        actor,
        9_999,
        8,
        512,
    )?;
    duplicate.stage_insert_authorized(
        prdb_engine::overlay::RowId {
            table: table.table_id,
            row: id(0x9f),
        },
        &projection,
        &[Some(ScalarValue::Uuid(generated_id))],
        &table.column_ids,
    )?;
    assert!(matches!(
        duplicate.commit(),
        Err(prdb_engine::transaction::TransactionError::Commit(
            prdb_engine::commit::CommitError::UniqueViolation
        ))
    ));
    assert_eq!(
        lookup_outcome(&database, id(0x9e), actor, 1)?
            .ok_or("generated identity collision has no durable outcome")?
            .status,
        OutcomeStatus::Aborted
    );

    let mut concurrent_identity = generated_id;
    concurrent_identity[0] ^= 1;
    let mut left = prdb_engine::transaction::InvocationTransaction::begin(
        &database,
        id(0xa2),
        actor,
        9_999,
        8,
        512,
    )?;
    let mut right = prdb_engine::transaction::InvocationTransaction::begin(
        &database,
        id(0xa3),
        actor,
        9_999,
        8,
        512,
    )?;
    left.stage_insert_authorized(
        prdb_engine::overlay::RowId {
            table: table.table_id,
            row: id(0xa0),
        },
        &projection,
        &[Some(ScalarValue::Uuid(concurrent_identity))],
        &table.column_ids,
    )?;
    right.stage_insert_authorized(
        prdb_engine::overlay::RowId {
            table: table.table_id,
            row: id(0xa1),
        },
        &projection,
        &[Some(ScalarValue::Uuid(concurrent_identity))],
        &table.column_ids,
    )?;
    let (left_result, right_result) = std::thread::scope(|scope| {
        let barrier = Arc::new(Barrier::new(2));
        let left_barrier = Arc::clone(&barrier);
        let right_barrier = Arc::clone(&barrier);
        let left_commit = scope.spawn(move || {
            left_barrier.wait();
            left.commit()
        });
        let right_commit = scope.spawn(move || {
            right_barrier.wait();
            right.commit()
        });
        (
            left_commit
                .join()
                .expect("left UUID commit thread panicked"),
            right_commit
                .join()
                .expect("right UUID commit thread panicked"),
        )
    });
    assert!(matches!(
        (&left_result, &right_result),
        (
            Ok(_),
            Err(prdb_engine::transaction::TransactionError::Commit(
                prdb_engine::commit::CommitError::UniqueViolation
            ))
        ) | (
            Err(prdb_engine::transaction::TransactionError::Commit(
                prdb_engine::commit::CommitError::UniqueViolation
            )),
            Ok(_)
        )
    ));
    let left_outcome = lookup_outcome(&database, id(0xa2), actor, 1)?
        .ok_or("left concurrent UUID commit has no durable outcome")?;
    let right_outcome = lookup_outcome(&database, id(0xa3), actor, 1)?
        .ok_or("right concurrent UUID commit has no durable outcome")?;
    assert_ne!(left_outcome.status, right_outcome.status);
    assert!(matches!(
        (left_outcome.status, right_outcome.status),
        (OutcomeStatus::Committed, OutcomeStatus::Aborted)
            | (OutcomeStatus::Aborted, OutcomeStatus::Committed)
    ));
    Ok(())
}
