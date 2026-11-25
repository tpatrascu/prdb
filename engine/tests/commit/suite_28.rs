use prdb_engine::audit::AuditAction;
use prdb_engine::commit::{RowWrite, commit_rows};
use prdb_engine::outcome::OutcomeStatus;
use prdb_engine::storage_manager::{StorageManager, StorageManagerConfig};
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, encode_tuple};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use super::helpers::*;

pub(super) fn planner_stream_estimate_allowance_falls_back_without_aborting_invocation()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::authorization::{Effect, Expr, Operation, Subject};
    use prdb_engine::executor_ipc::ResolveRelationRequest;
    use prdb_engine::ipc::{IpcFrame, Operation as IpcOperation};
    use prdb_engine::optimizer_binding::{
        BindError, BoundColumn, MetadataBindError, bind_select_request_via_metadata,
    };
    use prdb_engine::optimizer_ipc::OptimizerResponse;
    use prdb_engine::optimizer_plan::{PlanNode, decode_plan_v1};
    use prdb_engine::optimizer_service::{OptimizerError, optimize_select_v1};
    use prdb_engine::planner_ipc::{
        DescribePlanRelationRequest, DescribePlanRelationResponse, DescribeRelationResponse,
        EstimateJoinRequest, EstimateJoinResponse, EstimatePredicateRequest,
        EstimatePredicateResponse, EstimateScanRequest, EstimateScanResponse,
        MAX_PLANNER_REQUESTS_PER_CONNECTION, serve_planner, serve_planner_shared_with_observer,
    };
    use prdb_engine::storage_manager::{
        CreatePolicyRequest, CreateTableRequest, ObjectGrantPrivilege, ObjectGrantTarget,
        PolicyKind, SetObjectGrantRequest, TableColumnDefinition,
    };
    use prdb_engine::transaction::MAX_PLANNER_ESTIMATE_REQUESTS_PER_CONNECTION;
    use std::net::Shutdown;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let candidate_override_profile =
        std::env::var_os("PRDB_TEST_PROFILE_PLANNER_ESTIMATE_CANDIDATES").is_some();
    let storage = StorageManager::with_config(
        &database,
        StorageManagerConfig {
            max_planner_estimate_candidates: if candidate_override_profile {
                32_768
            } else {
                prdb_engine::statistics::DEFAULT_ESTIMATE_CANDIDATES
            },
            ..StorageManagerConfig::default()
        },
    )?;
    let actor = id(0x81);
    let admin_role = builtin_owner_id();
    let mut setup_audit = identity_audit_event(0x82, admin_role, actor);
    setup_audit.action = AuditAction::AuthorityChange;
    storage
        .commit_catalog_and_audit(
            id(0x82),
            admin_role,
            9_999,
            &[
                principal_write_with_name(actor, 1, 1, "planner_stats_owner"),
                object_write_with_name(actor, 8, "planner_stats_owner"),
                membership_write(actor, admin_role),
                dependency_write(actor, admin_role, 5),
            ],
            &[setup_audit],
        )
        .map_err(|error| format!("planner fixture identity setup: {error:?}"))?;

    let mut schema_audit = identity_audit_event(0x83, actor, actor);
    schema_audit.correlation_id = id(0x83);
    let schema = storage
        .create_schema(id(0x83), actor, "planner_stats", actor, 9_999, schema_audit)
        .map_err(|error| format!("planner fixture schema creation: {error:?}"))?;
    let mut table_audit = identity_audit_event(0x84, actor, schema.schema_id);
    table_audit.correlation_id = id(0x84);
    let table = storage
        .create_table(CreateTableRequest {
            invocation_id: id(0x84),
            authenticated_actor_id: actor,
            schema_id: schema.schema_id,
            name: "observations".to_owned(),
            columns: vec![TableColumnDefinition {
                name: "value".to_owned(),
                scalar_type: ScalarType::Integer,
                nullable: true,
                generated_uuid_identity: false,
                default: None,
            }],
            retention_deadline_us: 9_999,
            audit_event: table_audit,
        })
        .map_err(|error| format!("planner fixture table creation: {error:?}"))?;

    for (audit_id, invocation_id, object_id, privilege, target) in [
        (
            0x85,
            id(0x85),
            schema.schema_id,
            ObjectGrantPrivilege::Usage,
            ObjectGrantTarget::Object,
        ),
        (
            0x86,
            id(0x86),
            table.table_id,
            ObjectGrantPrivilege::Select,
            ObjectGrantTarget::Object,
        ),
        (
            0x89,
            id(0x89),
            table.table_id,
            ObjectGrantPrivilege::Select,
            ObjectGrantTarget::Column(table.column_ids[0]),
        ),
    ] {
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
            .map_err(|error| format!("planner fixture grant creation: {error:?}"))?;
    }
    let mut policy_audit = identity_audit_event(0x87, actor, table.table_id);
    policy_audit.correlation_id = id(0x87);
    storage
        .create_policy(CreatePolicyRequest {
            invocation_id: id(0x87),
            authenticated_actor_id: actor,
            table_id: table.table_id,
            name: "planner_stats_select".to_owned(),
            kind: PolicyKind::Row,
            operation: Operation::Select,
            effect: Effect::Allow,
            recipients: vec![Subject::Principal(actor)],
            target_column_ids: Vec::new(),
            using_expression: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check_expression: None,
            retention_deadline_us: 9_999,
            audit_event: policy_audit,
        })
        .map_err(|error| format!("planner fixture policy creation: {error:?}"))?;
    let mut column_policy_audit = identity_audit_event(0x90, actor, table.table_id);
    column_policy_audit.correlation_id = id(0x90);
    storage
        .create_policy(CreatePolicyRequest {
            invocation_id: id(0x90),
            authenticated_actor_id: actor,
            table_id: table.table_id,
            name: "planner_stats_read_value".to_owned(),
            kind: PolicyKind::Cell,
            operation: Operation::Select,
            effect: Effect::Allow,
            recipients: vec![Subject::Principal(actor)],
            target_column_ids: vec![table.column_ids[0]],
            using_expression: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check_expression: None,
            retention_deadline_us: 9_999,
            audit_event: column_policy_audit,
        })
        .map_err(|error| format!("planner fixture column policy creation: {error:?}"))?;

    if candidate_override_profile {
        const PROFILE_ROWS: usize = prdb_engine::statistics::DEFAULT_ESTIMATE_CANDIDATES + 1;
        let mut seed =
            storage.begin_invocation(id(0xc1), actor, 9_999, PROFILE_ROWS, PROFILE_ROWS * 64)?;
        for value in 0..PROFILE_ROWS {
            seed.stage_put(
                prdb_engine::overlay::RowId {
                    table: table.table_id,
                    row: u128::try_from(value + 1)?.to_be_bytes(),
                },
                &encode_tuple(
                    &[ColumnSchema {
                        scalar_type: ScalarType::Integer,
                        nullable: true,
                    }],
                    1,
                    &[Some(ScalarValue::Integer(i32::try_from(value)?))],
                )?,
            )?;
        }
        seed.commit()?;
        assert_eq!(
            storage.collect_committed_table_population(table.table_id)?,
            prdb_engine::statistics::StatisticsCollectionOutcome::Unknown,
            "the independent committed-population maintenance cap remains in force"
        );

        let mut transaction = storage.begin_invocation(id(0xc2), actor, 9_999, 1, 64)?;
        let (mut client, mut server) = std::os::unix::net::UnixStream::pair()?;
        client.set_read_timeout(Some(Duration::from_secs(5)))?;
        let predicate = prdb_engine::optimizer_binding::BoundPredicate::Compare {
            operator: prdb_engine::optimizer_request::Comparison::Equal,
            left: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Column(
                BoundColumn {
                    column_id: table.column_ids[0],
                    ordinal: 0,
                    scalar_type: ScalarType::Integer,
                    nullable: true,
                },
            )),
            right: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Integer(7)),
        };
        let request = EstimatePredicateRequest::from_bound_predicate(
            "planner_stats".to_owned(),
            "observations".to_owned(),
            &predicate,
        )?;
        std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
            let planner = scope.spawn(|| serve_planner(&mut server, &mut transaction));
            prdb_engine::ipc::write_frame(
                &mut client,
                &IpcFrame {
                    operation: IpcOperation::DescribeRelation as u16,
                    request_id: 1,
                    payload: prdb_engine::executor_ipc::ResolveRelationRequest {
                        schema_name: "planner_stats".to_owned(),
                        relation_name: "observations".to_owned(),
                    }
                    .encode()?,
                },
            )?;
            let described = prdb_engine::ipc::read_frame(&mut client)?
                .ok_or("planner stream closed before elevated candidate handle")?;
            let (handle, _) = DescribeRelationResponse::decode(&described.payload)?
                .table
                .ok_or("planner did not return the candidate-profile handle")?;
            prdb_engine::ipc::write_frame(
                &mut client,
                &IpcFrame {
                    operation: IpcOperation::EstimateScan as u16,
                    request_id: 2,
                    payload: EstimateScanRequest {
                        handle: handle.clone(),
                        column_ids: Vec::new(),
                    }
                    .encode()?,
                },
            )?;
            let summary = prdb_engine::ipc::read_frame(&mut client)?
                .ok_or("planner stream closed before elevated candidate summary")?;
            assert_eq!(
                EstimateScanResponse::decode(&summary.payload)?,
                EstimateScanResponse::Exact(prdb_engine::statistics::AuthorizedScanSummary {
                    visible_rows: PROFILE_ROWS as u64,
                    columns: vec![prdb_engine::statistics::ColumnPopulation {
                        readable_values: PROFILE_ROWS as u64,
                        nulls: 0,
                        redacted: 0,
                    }],
                }),
                "the elevated profile must return a complete summary beyond the old default"
            );
            prdb_engine::ipc::write_frame(
                &mut client,
                &IpcFrame {
                    operation: IpcOperation::EstimateScan as u16,
                    request_id: 3,
                    payload: request.encode()?,
                },
            )?;
            let response = prdb_engine::ipc::read_frame(&mut client)?
                .ok_or("planner stream closed before elevated candidate estimate")?;
            assert_eq!(
                EstimatePredicateResponse::decode(&response.payload)?,
                EstimatePredicateResponse::Exact {
                    visible_rows: PROFILE_ROWS as u64,
                    matching_rows: 1,
                },
                "the elevated trusted candidate profile must estimate beyond the old default"
            );
            client.shutdown(Shutdown::Write)?;
            planner
                .join()
                .expect("elevated candidate planner panicked")
                .map_err(|error| format!("elevated candidate planner failed: {error:?}"))?;
            Ok(())
        })?;
        transaction.abort()?;

        let default_storage =
            StorageManager::with_config(&database, StorageManagerConfig::default())?;
        let mut default_transaction =
            default_storage.begin_invocation(id(0xc3), actor, 9_999, 1, 64)?;
        let (mut default_client, mut default_server) = std::os::unix::net::UnixStream::pair()?;
        default_client.set_read_timeout(Some(Duration::from_secs(5)))?;
        std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
            let planner =
                scope.spawn(|| serve_planner(&mut default_server, &mut default_transaction));
            prdb_engine::ipc::write_frame(
                &mut default_client,
                &IpcFrame {
                    operation: IpcOperation::EstimateScan as u16,
                    request_id: 1,
                    payload: request.encode()?,
                },
            )?;
            let response = prdb_engine::ipc::read_frame(&mut default_client)?
                .ok_or("default planner closed before estimate fallback")?;
            assert_eq!(
                EstimatePredicateResponse::decode(&response.payload)?,
                EstimatePredicateResponse::Unknown,
                "the default candidate ceiling must return UNKNOWN above 16,384 rows"
            );
            default_client.shutdown(Shutdown::Write)?;
            planner
                .join()
                .expect("default candidate planner panicked")
                .map_err(|error| format!("default candidate planner failed: {error:?}"))?;
            Ok(())
        })?;
        default_transaction.abort()?;
        return Ok(());
    }

    if std::env::var_os("PRDB_TEST_PROFILE_PLANNER_ESTIMATE_IPC").is_some() {
        const PROFILE_ROWS: usize = 2_052;
        const PROFILE_RUNS: usize = 11;
        const CONCURRENT_OPTIMIZERS: usize = 8;
        const CONCURRENT_OPERATIONAL_TRANSACTIONS: usize = 64;
        const SATURATION_RUNS: usize = 5;
        let mut seed_invocation_id = [0xd1; 16];
        seed_invocation_id[15] = 1;
        let mut seed = storage.begin_invocation(
            seed_invocation_id,
            actor,
            9_999,
            PROFILE_ROWS + 1,
            PROFILE_ROWS * 64,
        )?;
        for value in 0..PROFILE_ROWS {
            seed.stage_put(
                prdb_engine::overlay::RowId {
                    table: table.table_id,
                    row: ((value + 1) as u128).to_be_bytes(),
                },
                &encode_tuple(
                    &[ColumnSchema {
                        scalar_type: ScalarType::Integer,
                        nullable: true,
                    }],
                    1,
                    &[Some(ScalarValue::Integer(value as i32))],
                )?,
            )?;
        }
        seed.commit()
            .map_err(|error| format!("planner IPC profile seed commit: {error:?}"))?;
        let mut index_invocation_id = [0xd1; 16];
        index_invocation_id[15] = 2;
        let mut index_audit = identity_audit_event(0xd2, actor, table.table_id);
        index_audit.correlation_id = index_invocation_id;
        storage
            .create_index(prdb_engine::storage_manager::CreateIndexRequest {
                invocation_id: index_invocation_id,
                authenticated_actor_id: actor,
                table_id: table.table_id,
                name: "planner_estimate_profile_idx".to_owned(),
                column_ids: vec![table.column_ids[0]],
                unique: false,
                retention_deadline_us: 9_999,
                audit_event: index_audit,
            })
            .map_err(|error| format!("planner IPC profile index creation: {error:?}"))?;
        assert_eq!(
            storage.collect_committed_table_population(table.table_id)?,
            prdb_engine::statistics::StatisticsCollectionOutcome::Exact,
            "the IPC profile needs an exact committed population record"
        );

        let predicate = prdb_engine::optimizer_binding::BoundPredicate::Compare {
            operator: prdb_engine::optimizer_request::Comparison::Equal,
            left: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Column(
                BoundColumn {
                    column_id: table.column_ids[0],
                    ordinal: 0,
                    scalar_type: ScalarType::Integer,
                    nullable: true,
                },
            )),
            right: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Integer(7)),
        };
        let predicate_request = EstimatePredicateRequest::from_bound_predicate(
            "planner_stats".to_owned(),
            "observations".to_owned(),
            &predicate,
        )?;
        let optimizer_request = || {
            let mut bytes = vec![0, 1, 1, 0];
            for name in ["planner_stats", "observations"] {
                bytes.push(name.len() as u8);
                bytes.extend_from_slice(name.as_bytes());
            }
            bytes.extend_from_slice(&1u16.to_be_bytes());
            bytes.push(5);
            bytes.extend_from_slice(b"value");
            let mut encoded_predicate = vec![0x20, 0, 0x10, 5];
            encoded_predicate.extend_from_slice(b"value");
            encoded_predicate.push(0x02);
            encoded_predicate.extend_from_slice(&7i64.to_be_bytes());
            bytes.extend_from_slice(&(encoded_predicate.len() as u32).to_be_bytes());
            bytes.extend_from_slice(&encoded_predicate);
            bytes.push(1);
            bytes.extend_from_slice(&3u32.to_be_bytes());
            bytes
        };
        let predicate_bytes = predicate_request.predicate_bytes.clone();
        let mut describe_roundtrips = Vec::with_capacity(PROFILE_RUNS);
        let mut direct_estimate_times = Vec::with_capacity(PROFILE_RUNS);
        let mut estimate_roundtrips = Vec::with_capacity(PROFILE_RUNS);
        let mut optimizer_process_roundtrips = Vec::with_capacity(PROFILE_RUNS);
        let mut saturated_optimizer_latencies = Vec::new();
        let mut saturated_optimizer_batch_times = Vec::new();
        let mut saturated_optimizer_total_times = Vec::new();
        let mut saturated_operational_read_latencies = Vec::new();
        let mut saturated_operational_commit_latencies = Vec::new();
        for run_number in 0..PROFILE_RUNS {
            let mut direct_invocation_id = [0xd1; 16];
            direct_invocation_id[14] = 2;
            direct_invocation_id[15] = run_number as u8 + 1;
            let mut direct_transaction =
                storage.begin_invocation(direct_invocation_id, actor, 9_999, 1, 64)?;
            let mut connection = direct_transaction.open_planner_metadata_connection();
            let started = std::time::Instant::now();
            let direct_estimate = connection.estimate_predicate_for_plan(
                "planner_stats",
                "observations",
                &predicate_bytes,
            )?;
            direct_estimate_times.push(started.elapsed());
            assert_eq!(
                direct_estimate,
                Some((PROFILE_ROWS as u64, 1)),
                "direct planner service must preserve the exact authorized estimate"
            );
            drop(connection);
            direct_transaction.abort()?;

            let mut estimate_invocation_id = [0xd1; 16];
            estimate_invocation_id[14] = 1;
            estimate_invocation_id[15] = run_number as u8 + 1;
            let mut transaction =
                storage.begin_invocation(estimate_invocation_id, actor, 9_999, 1, 64)?;
            let (mut client, mut server) = std::os::unix::net::UnixStream::pair()?;
            client.set_read_timeout(Some(Duration::from_secs(5)))?;
            std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
                let planner = scope.spawn(|| serve_planner(&mut server, &mut transaction));
                let describe = IpcFrame {
                    operation: IpcOperation::DescribePlanRelation as u16,
                    request_id: 1,
                    payload: DescribePlanRelationRequest {
                        schema_name: "planner_stats".to_owned(),
                        relation_name: "observations".to_owned(),
                        include_scan_estimate: false,
                        population_estimate_max_rows: None,
                    }
                    .encode()?,
                };
                let started = std::time::Instant::now();
                prdb_engine::ipc::write_frame(&mut client, &describe)?;
                let described = prdb_engine::ipc::read_frame(&mut client)?
                    .ok_or("planner IPC stream closed before relation response")?;
                describe_roundtrips.push(started.elapsed());
                assert_eq!(
                    described.operation,
                    IpcOperation::DescribePlanRelation as u16
                );
                assert_eq!(described.request_id, 1);
                assert_eq!(
                    DescribePlanRelationResponse::decode(&described.payload)?
                        .relation
                        .ok_or("planner IPC did not resolve the benchmark relation")?
                        .relation
                        .relation_id,
                    table.table_id
                );
                let request = IpcFrame {
                    operation: IpcOperation::EstimateScan as u16,
                    request_id: 2,
                    payload: predicate_request.encode()?,
                };
                let started = std::time::Instant::now();
                prdb_engine::ipc::write_frame(&mut client, &request)?;
                let response = prdb_engine::ipc::read_frame(&mut client)?
                    .ok_or("planner IPC stream closed before estimate response")?;
                estimate_roundtrips.push(started.elapsed());
                assert_eq!(response.operation, IpcOperation::EstimateScan as u16);
                assert_eq!(response.request_id, 2);
                assert_eq!(
                    EstimatePredicateResponse::decode(&response.payload)?,
                    EstimatePredicateResponse::Exact {
                        visible_rows: PROFILE_ROWS as u64,
                        matching_rows: 1,
                    },
                    "planner IPC must preserve the exact authorized estimate"
                );
                client.shutdown(Shutdown::Write)?;
                planner
                    .join()
                    .expect("planner IPC profile server panicked")
                    .map_err(|error| format!("planner IPC profile failed: {error:?}"))?;
                Ok(())
            })?;
            transaction.abort()?;

            let mut process_invocation_id = [0xd1; 16];
            process_invocation_id[13] = 1;
            process_invocation_id[14] = 3;
            process_invocation_id[15] = run_number as u8 + 1;
            let mut process_transaction =
                storage.begin_invocation(process_invocation_id, actor, 9_999, 1, 64)?;
            let (optimizer_worker, mut optimizer_client) = std::os::unix::net::UnixStream::pair()?;
            let (planner_worker, mut planner_storage) = std::os::unix::net::UnixStream::pair()?;
            use std::os::fd::AsRawFd;
            use std::os::unix::process::CommandExt;
            use std::process::{Command, Stdio};
            let submission_fd = optimizer_worker.as_raw_fd();
            let planner_fd = planner_worker.as_raw_fd();
            let mut optimizer = Command::new(env!("CARGO_BIN_EXE_prdb-optimizer"));
            optimizer
                .arg("--submission-fd")
                .arg(submission_fd.to_string())
                .arg("--planner-fd")
                .arg(planner_fd.to_string())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            // SAFETY: the child callback touches only the two live Unix-stream
            // descriptors captured above before exec, clearing their close-on-exec
            // flag so the optimizer receives the intended channel endpoints.
            unsafe {
                optimizer.pre_exec(move || {
                    for descriptor in [submission_fd, planner_fd] {
                        // SAFETY: fcntl operates on inherited descriptors in the child.
                        let flags = libc::fcntl(descriptor, libc::F_GETFD);
                        if flags < 0
                            || libc::fcntl(descriptor, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0
                        {
                            return Err(std::io::Error::last_os_error());
                        }
                    }
                    Ok(())
                });
            }
            let mut optimizer = optimizer.spawn()?;
            drop(optimizer_worker);
            drop(planner_worker);
            optimizer_client.set_read_timeout(Some(Duration::from_secs(5)))?;
            std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
                let planner =
                    scope.spawn(|| serve_planner(&mut planner_storage, &mut process_transaction));
                let started = std::time::Instant::now();
                prdb_engine::ipc::write_frame(
                    &mut optimizer_client,
                    &IpcFrame {
                        operation: IpcOperation::OptimizeSelect as u16,
                        request_id: 1,
                        payload: optimizer_request(),
                    },
                )?;
                let response = prdb_engine::ipc::read_frame(&mut optimizer_client)?
                    .ok_or("optimizer process closed before its filtered plan response")?;
                optimizer_process_roundtrips.push(started.elapsed());
                assert_eq!(response.operation, IpcOperation::OptimizeSelect as u16);
                assert_eq!(response.request_id, 1);
                let response = OptimizerResponse::decode(&response.payload)?;
                assert_eq!(
                    response.status,
                    prdb_engine::optimizer_ipc::OptimizerStatus::Success,
                    "isolated optimizer must return a filtered plan"
                );
                let plan = decode_plan_v1(
                    response
                        .plan
                        .as_deref()
                        .ok_or("successful optimizer response omitted its plan")?,
                )?;
                assert_eq!(
                    plan.relation.relation_id, table.table_id,
                    "isolated optimizer must bind the authorized relation"
                );
                assert!(
                    plan.nodes
                        .iter()
                        .any(|node| matches!(node, PlanNode::Filter(found) if found == &predicate)),
                    "isolated optimizer must preserve the exact equality predicate"
                );
                optimizer_client.shutdown(Shutdown::Write)?;
                drop(optimizer_client);
                let status = optimizer.wait()?;
                assert!(status.success(), "optimizer child exited with {status}");
                planner
                    .join()
                    .expect("optimizer profile planner thread panicked")
                    .map_err(|error| format!("optimizer profile planner failed: {error:?}"))?;
                Ok(())
            })?;
            process_transaction.abort()?;
        }
        if std::env::var_os("PRDB_TEST_PROFILE_OPTIMIZER_SATURATION").is_some() {
            struct ReleaseBarrierOnDrop {
                barrier: Arc<Barrier>,
                released: bool,
            }
            impl Drop for ReleaseBarrierOnDrop {
                fn drop(&mut self) {
                    if !self.released {
                        self.barrier.wait();
                    }
                }
            }
            for run_number in 0..SATURATION_RUNS {
                let all_started = std::time::Instant::now();
                let barrier = Arc::new(Barrier::new(
                    CONCURRENT_OPTIMIZERS + CONCURRENT_OPERATIONAL_TRANSACTIONS + 1,
                ));
                let storage_for_workers = storage.clone();
                let table_id = table.table_id;
                let operational_inflight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let (latencies, operational_latencies, batch_time) =
                    std::thread::scope(|scope| {
                    let mut workers = Vec::with_capacity(CONCURRENT_OPTIMIZERS);
                    let mut operational_workers =
                        Vec::with_capacity(CONCURRENT_OPERATIONAL_TRANSACTIONS);
                    for worker_number in 0..CONCURRENT_OPTIMIZERS {
                        let worker_storage = storage_for_workers.clone();
                        let worker_barrier = Arc::clone(&barrier);
                        let worker_operational_inflight = Arc::clone(&operational_inflight);
                        let request_payload = optimizer_request();
                        workers.push(scope.spawn(move || -> Result<Duration, String> {
                            let mut release_barrier = ReleaseBarrierOnDrop {
                                barrier: Arc::clone(&worker_barrier),
                                released: false,
                            };
                            let mut invocation_id = [0xe7; 16];
                            invocation_id[14] = run_number as u8 + 1;
                            invocation_id[15] = worker_number as u8 + 1;
                            let mut transaction = worker_storage
                                .begin_invocation(invocation_id, actor, 9_999, 1, 64)
                                .map_err(|error| {
                                    format!("begin saturation invocation: {error:?}")
                                })?;
                            let (optimizer_worker, mut optimizer_client) =
                                std::os::unix::net::UnixStream::pair().map_err(|error| {
                                    format!("optimizer saturation socket: {error}")
                                })?;
                            let (planner_worker, mut planner_storage) =
                                std::os::unix::net::UnixStream::pair().map_err(|error| {
                                    format!("planner saturation socket: {error}")
                                })?;
                            use std::os::fd::AsRawFd;
                            use std::os::unix::process::CommandExt;
                            use std::process::{Command, Stdio};
                            let submission_fd = optimizer_worker.as_raw_fd();
                            let planner_fd = planner_worker.as_raw_fd();
                            let mut command = Command::new(env!("CARGO_BIN_EXE_prdb-optimizer"));
                            command
                                .arg("--submission-fd")
                                .arg(submission_fd.to_string())
                                .arg("--planner-fd")
                                .arg(planner_fd.to_string())
                                .stdin(Stdio::null())
                                .stdout(Stdio::null())
                                .stderr(Stdio::null());
                            // SAFETY: the child callback touches only the two live Unix-stream
                            // descriptors captured above before exec, clearing their close-on-exec
                            // flag so this optimizer receives the intended channel endpoints.
                            unsafe {
                                command.pre_exec(move || {
                                    for descriptor in [submission_fd, planner_fd] {
                                        // SAFETY: fcntl operates on inherited descriptors in the child.
                                        let flags = libc::fcntl(descriptor, libc::F_GETFD);
                                        if flags < 0
                                            || libc::fcntl(
                                                descriptor,
                                                libc::F_SETFD,
                                                flags & !libc::FD_CLOEXEC,
                                            ) < 0
                                        {
                                            return Err(std::io::Error::last_os_error());
                                        }
                                    }
                                    Ok(())
                                });
                            }
                            let mut optimizer = command
                                .spawn()
                                .map_err(|error| format!("spawn optimizer: {error}"))?;
                            drop(optimizer_worker);
                            drop(planner_worker);
                            optimizer_client
                                .set_read_timeout(Some(Duration::from_secs(10)))
                                .map_err(|error| format!("set optimizer timeout: {error}"))?;
                            let latency = std::thread::scope(|planner_scope| {
                                let planner = planner_scope.spawn(|| {
                                    serve_planner(&mut planner_storage, &mut transaction)
                                });
                                worker_barrier.wait();
                                release_barrier.released = true;
                                let started = std::time::Instant::now();
                                prdb_engine::ipc::write_frame(
                                    &mut optimizer_client,
                                    &IpcFrame {
                                        operation: IpcOperation::OptimizeSelect as u16,
                                        request_id: 1,
                                        payload: request_payload,
                                    },
                                )
                                .map_err(|error| {
                                    format!("write optimizer saturation request: {error}")
                                })?;
                                let response = prdb_engine::ipc::read_frame(&mut optimizer_client)
                                    .map_err(|error| {
                                        format!("read optimizer saturation response: {error}")
                                    })?
                                    .ok_or_else(|| {
                                        "optimizer closed before saturation response".to_owned()
                                    })?;
                                if worker_operational_inflight
                                    .load(std::sync::atomic::Ordering::Acquire)
                                    == 0
                                {
                                    return Err(
                                        "operational reads and commits did not overlap optimizer planning"
                                            .to_owned(),
                                    );
                                }
                                let latency = started.elapsed();
                                if response.operation != IpcOperation::OptimizeSelect as u16
                                    || response.request_id != 1
                                {
                                    return Err(
                                        "optimizer saturation response mismatched the request"
                                            .to_owned(),
                                    );
                                }
                                let response = OptimizerResponse::decode(&response.payload)
                                    .map_err(|error| {
                                        format!("decode optimizer saturation response: {error:?}")
                                    })?;
                                if response.status
                                    != prdb_engine::optimizer_ipc::OptimizerStatus::Success
                                {
                                    return Err(format!(
                                        "optimizer saturation failed: {:?}",
                                        response.status
                                    ));
                                }
                                let plan =
                                    decode_plan_v1(response.plan.as_deref().ok_or_else(|| {
                                        "optimizer omitted saturation plan".to_owned()
                                    })?)
                                    .map_err(|error| {
                                        format!("decode saturation plan: {error:?}")
                                    })?;
                                if plan.relation.relation_id != table_id
                                    || !plan
                                        .nodes
                                        .iter()
                                        .any(|node| matches!(node, PlanNode::Filter(_)))
                                {
                                    return Err("optimizer saturation returned an unexpected plan"
                                        .to_owned());
                                }
                                optimizer_client
                                    .shutdown(Shutdown::Write)
                                    .map_err(|error| {
                                        format!("close optimizer saturation request: {error}")
                                    })?;
                                drop(optimizer_client);
                                let status = optimizer
                                    .wait()
                                    .map_err(|error| format!("wait for optimizer: {error}"))?;
                                if !status.success() {
                                    return Err(format!(
                                        "optimizer exited unsuccessfully: {status}"
                                    ));
                                }
                                planner
                                    .join()
                                    .map_err(|_| "planner saturation thread panicked".to_owned())?
                                    .map_err(|error| {
                                        format!("planner saturation failed: {error:?}")
                                    })?;
                                Ok::<_, String>(latency)
                            })?;
                            transaction.abort().map_err(|error| {
                                format!("abort saturation invocation: {error:?}")
                            })?;
                            Ok(latency)
                        }));
                    }
                    for worker_number in 0..CONCURRENT_OPERATIONAL_TRANSACTIONS {
                        let worker_storage = storage_for_workers.clone();
                        let worker_barrier = Arc::clone(&barrier);
                        let worker_operational_inflight = Arc::clone(&operational_inflight);
                        operational_workers.push(scope.spawn(move || {
                            let mut release_barrier = ReleaseBarrierOnDrop {
                                barrier: Arc::clone(&worker_barrier),
                                released: false,
                            };
                            let mut invocation_id = [0xe8; 16];
                            invocation_id[14] = run_number as u8 + 1;
                            invocation_id[15] = worker_number as u8 + 1;
                            let mut transaction = worker_storage
                                .begin_invocation(invocation_id, actor, 9_999, 1, 64)
                                .map_err(|error| {
                                    format!("begin concurrent operational invocation: {error:?}")
                                })?;
                            worker_operational_inflight
                                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                            worker_barrier.wait();
                            release_barrier.released = true;
                            let read_started = std::time::Instant::now();
                            let cells = transaction
                                .read_catalog_authorized_row(
                                    table_id,
                                    ((worker_number + 1) as u128).to_be_bytes(),
                                )
                                .map_err(|error| {
                                    format!("concurrent authorized operational read: {error:?}")
                                })?
                                .ok_or_else(|| {
                                    "concurrent operational point read returned no row".to_owned()
                                })?;
                            if cells.len() != 1 {
                                return Err(format!(
                                    "concurrent operational point read returned {} cells, expected one",
                                    cells.len()
                                ));
                            }
                            let read_latency = read_started.elapsed();
                            let commit_started = std::time::Instant::now();
                            transaction.commit().map_err(|error| {
                                format!("concurrent operational read-only commit: {error:?}")
                            })?;
                            worker_operational_inflight
                                .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                            Ok::<_, String>((read_latency, commit_started.elapsed()))
                        }));
                    }
                    let batch_started = std::time::Instant::now();
                    barrier.wait();
                    let mut latencies = Vec::with_capacity(CONCURRENT_OPTIMIZERS);
                    for worker in workers {
                        latencies.push(
                            worker
                                .join()
                                .map_err(|_| "optimizer saturation worker panicked".to_owned())??,
                        );
                    }
                    let mut operational_latencies =
                        Vec::with_capacity(CONCURRENT_OPERATIONAL_TRANSACTIONS);
                    for worker in operational_workers {
                        operational_latencies.push(worker.join().map_err(|_| {
                            "concurrent operational worker panicked".to_owned()
                        })??);
                    }
                    Ok::<_, String>((latencies, operational_latencies, batch_started.elapsed()))
                    })
                    .map_err(|error| format!("optimizer saturation run failed: {error}"))?;
                saturated_optimizer_latencies.extend(latencies);
                saturated_operational_read_latencies
                    .extend(operational_latencies.iter().map(|(read, _)| *read));
                saturated_operational_commit_latencies
                    .extend(operational_latencies.iter().map(|(_, commit)| *commit));
                saturated_optimizer_batch_times.push(batch_time);
                saturated_optimizer_total_times.push(all_started.elapsed());
            }
        }
        describe_roundtrips.sort_unstable();
        direct_estimate_times.sort_unstable();
        estimate_roundtrips.sort_unstable();
        optimizer_process_roundtrips.sort_unstable();
        let describe_median = describe_roundtrips[describe_roundtrips.len() / 2];
        let describe_p95 =
            describe_roundtrips[((describe_roundtrips.len() - 1) * 95).div_ceil(100)];
        let direct_median = direct_estimate_times[direct_estimate_times.len() / 2];
        let direct_p95 =
            direct_estimate_times[((direct_estimate_times.len() - 1) * 95).div_ceil(100)];
        let estimate_median = estimate_roundtrips[estimate_roundtrips.len() / 2];
        let estimate_p95 =
            estimate_roundtrips[((estimate_roundtrips.len() - 1) * 95).div_ceil(100)];
        let optimizer_process_median =
            optimizer_process_roundtrips[optimizer_process_roundtrips.len() / 2];
        let optimizer_process_p95 = optimizer_process_roundtrips
            [((optimizer_process_roundtrips.len() - 1) * 95).div_ceil(100)];
        eprintln!(
            "planner estimate profile: rows={PROFILE_ROWS}, runs={PROFILE_RUNS}, indexed exact equality, direct service median/p95={:.3}/{:.3} ms, DescribePlanRelation median/p95={:.3}/{:.3} ms, exact EstimateScan roundtrip median/p95={:.3}/{:.3} ms, isolated optimizer plan request median/p95={:.3}/{:.3} ms",
            direct_median.as_secs_f64() * 1_000.0,
            direct_p95.as_secs_f64() * 1_000.0,
            describe_median.as_secs_f64() * 1_000.0,
            describe_p95.as_secs_f64() * 1_000.0,
            estimate_median.as_secs_f64() * 1_000.0,
            estimate_p95.as_secs_f64() * 1_000.0,
            optimizer_process_median.as_secs_f64() * 1_000.0,
            optimizer_process_p95.as_secs_f64() * 1_000.0,
        );
        if !saturated_optimizer_latencies.is_empty() {
            saturated_optimizer_latencies.sort_unstable();
            saturated_optimizer_batch_times.sort_unstable();
            saturated_optimizer_total_times.sort_unstable();
            let latency_p50 =
                saturated_optimizer_latencies[saturated_optimizer_latencies.len() / 2];
            let latency_p95 = saturated_optimizer_latencies
                [((saturated_optimizer_latencies.len() - 1) * 95).div_ceil(100)];
            let batch_p50 =
                saturated_optimizer_batch_times[saturated_optimizer_batch_times.len() / 2];
            let total_p50 =
                saturated_optimizer_total_times[saturated_optimizer_total_times.len() / 2];
            eprintln!(
                "optimizer concurrency profile: processes={CONCURRENT_OPTIMIZERS}, operational transactions per batch={CONCURRENT_OPERATIONAL_TRANSACTIONS}, runs={SATURATION_RUNS}, requests={}, request latency p50/p95={:.3}/{:.3} ms, request-batch p50={:.3} ms ({:.1} requests/s), setup-to-completion p50={:.3} ms",
                saturated_optimizer_latencies.len(),
                latency_p50.as_secs_f64() * 1_000.0,
                latency_p95.as_secs_f64() * 1_000.0,
                batch_p50.as_secs_f64() * 1_000.0,
                CONCURRENT_OPTIMIZERS as f64 / batch_p50.as_secs_f64(),
                total_p50.as_secs_f64() * 1_000.0,
            );
            saturated_operational_read_latencies.sort_unstable();
            saturated_operational_commit_latencies.sort_unstable();
            let operational_read_p50 = saturated_operational_read_latencies
                [saturated_operational_read_latencies.len() / 2];
            let operational_read_p95 = saturated_operational_read_latencies
                [((saturated_operational_read_latencies.len() - 1) * 95).div_ceil(100)];
            let operational_commit_p50 = saturated_operational_commit_latencies
                [saturated_operational_commit_latencies.len() / 2];
            let operational_commit_p95 = saturated_operational_commit_latencies
                [((saturated_operational_commit_latencies.len() - 1) * 95).div_ceil(100)];
            eprintln!(
                "operational transactions during optimizer saturation: transactions={}, authorized-read p50/p95={:.3}/{:.3} ms, read-only commit p50/p95={:.3}/{:.3} ms",
                saturated_operational_read_latencies.len(),
                operational_read_p50.as_secs_f64() * 1_000.0,
                operational_read_p95.as_secs_f64() * 1_000.0,
                operational_commit_p50.as_secs_f64() * 1_000.0,
                operational_commit_p95.as_secs_f64() * 1_000.0,
            );
        }
        return Ok(());
    }

    assert_eq!(
        storage.collect_committed_table_population(table.table_id)?,
        prdb_engine::statistics::StatisticsCollectionOutcome::Exact,
        "the planner fixture starts with a fresh committed population record"
    );

    let encode_select = |schema_name: &str, relation_name: &str, column_name: &str| {
        let mut bytes = vec![0, 1, 1, 0];
        for name in [schema_name, relation_name] {
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.extend_from_slice(&1u16.to_be_bytes());
        bytes.push(column_name.len() as u8);
        bytes.extend_from_slice(column_name.as_bytes());
        bytes.extend_from_slice(&0u32.to_be_bytes());
        bytes.push(0);
        bytes
    };
    let encode_filtered_select = |schema_name: &str, relation_name: &str, column_name: &str| {
        let mut bytes = vec![0, 1, 1, 0];
        for name in [schema_name, relation_name] {
            bytes.push(name.len() as u8);
            bytes.extend_from_slice(name.as_bytes());
        }
        bytes.extend_from_slice(&1u16.to_be_bytes());
        bytes.push(column_name.len() as u8);
        bytes.extend_from_slice(column_name.as_bytes());
        let mut predicate = vec![0x20, 0, 0x10, column_name.len() as u8];
        predicate.extend_from_slice(column_name.as_bytes());
        predicate.push(0x02);
        predicate.extend_from_slice(&7i64.to_be_bytes());
        bytes.extend_from_slice(&(predicate.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&predicate);
        bytes.push(1);
        bytes.extend_from_slice(&3u32.to_be_bytes());
        bytes
    };
    let mut binding_transaction = storage.begin_invocation(id(0x8a), actor, 9_999, 8, 512)?;
    {
        let mut planner_connection = binding_transaction.open_planner_metadata_connection();
        let (population_handle, _) = planner_connection
            .resolve_relation("planner_stats", "observations")?
            .ok_or("authorized population relation was not described")?;
        assert_eq!(
            planner_connection.estimate_population_at_most(&population_handle, 128)?,
            Some(0),
            "a fresh committed count is usable when the whole table is authorized"
        );
        let bound = bind_select_request_via_metadata(
            &encode_select("planner_stats", "observations", "value"),
            &mut planner_connection,
        )?;
        assert_eq!(bound.schema_name, "planner_stats");
        assert_eq!(bound.schema_id, schema.schema_id);
        assert_eq!(bound.schema_version, 1);
        assert_eq!(bound.table_epoch, 3);
        assert_eq!(bound.relation_id, table.table_id);
        assert_eq!(bound.relation_name, "observations");
        assert_eq!(
            bound.projections,
            [BoundColumn {
                column_id: table.column_ids[0],
                ordinal: 0,
                scalar_type: ScalarType::Integer,
                nullable: true,
            }]
        );
        let encoded_plan = optimize_select_v1(
            &encode_select("planner_stats", "observations", "value"),
            &mut planner_connection,
        )?;
        let plan = decode_plan_v1(&encoded_plan)?;
        assert_eq!(plan.relation.schema_id, schema.schema_id);
        assert_eq!(plan.relation.schema_version, 1);
        assert_eq!(plan.relation.relation_id, table.table_id);
        assert_eq!(plan.relation.table_epoch, 3);
        assert!(
            matches!(plan.nodes.first(), Some(PlanNode::Scan { columns }) if columns == &[BoundColumn {
                column_id: table.column_ids[0],
                ordinal: 0,
                scalar_type: ScalarType::Integer,
                nullable: true,
            }])
        );
        assert!(matches!(
            optimize_select_v1(
                &encode_select("planner_stats", "hidden_or_missing", "value"),
                &mut planner_connection,
            ),
            Err(OptimizerError::Bind(MetadataBindError::Bind(
                BindError::RelationUnavailable
            )))
        ));
        assert!(matches!(
            bind_select_request_via_metadata(
                &encode_select("planner_stats", "hidden_or_missing", "value"),
                &mut planner_connection,
            ),
            Err(MetadataBindError::Bind(BindError::RelationUnavailable))
        ));
    }
    binding_transaction.abort()?;

    let cancelled_invocation_id = id(0x8d);
    let mut cancelled_estimate =
        storage.begin_invocation(cancelled_invocation_id, actor, 9_999, 8, 512)?;
    let (mut cancelled_planner_client, mut cancelled_planner_server) =
        std::os::unix::net::UnixStream::pair()?;
    cancelled_planner_client.set_read_timeout(Some(Duration::from_secs(2)))?;
    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let planner = scope.spawn(|| {
            let result = serve_planner(&mut cancelled_planner_server, &mut cancelled_estimate);
            let _ = cancelled_planner_server.shutdown(Shutdown::Both);
            result
        });
        prdb_engine::ipc::write_frame(
            &mut cancelled_planner_client,
            &IpcFrame {
                operation: IpcOperation::DescribeRelation as u16,
                request_id: 1,
                payload: ResolveRelationRequest {
                    schema_name: "planner_stats".to_owned(),
                    relation_name: "observations".to_owned(),
                }
                .encode()?,
            },
        )?;
        let description = prdb_engine::ipc::read_frame(&mut cancelled_planner_client)?
            .ok_or("planner stream closed before cancellation test relation")?;
        let handle = DescribeRelationResponse::decode(&description.payload)?
            .table
            .ok_or("authorized relation was not described for cancellation test")?
            .0;
        assert_eq!(
            storage.cancel_owned_invocation(actor, cancelled_invocation_id, 9_998)?,
            prdb_engine::storage_manager::OwnerCancellationResult::Accepted
        );
        prdb_engine::ipc::write_frame(
            &mut cancelled_planner_client,
            &IpcFrame {
                operation: IpcOperation::EstimateScan as u16,
                request_id: 2,
                payload: EstimateScanRequest {
                    handle,
                    column_ids: Vec::new(),
                }
                .encode()?,
            },
        )?;
        let cancelled_response = prdb_engine::ipc::read_frame(&mut cancelled_planner_client)?;
        cancelled_planner_client.shutdown(Shutdown::Write)?;
        drop(cancelled_planner_client);
        let planner_result = planner.join().expect("planner thread panicked");
        assert!(
            cancelled_response.is_none(),
            "cancelled estimation must terminate the planner stream instead of returning a fallback response: {cancelled_response:?}"
        );
        assert!(
            planner_result.is_err(),
            "cancelled planning must abort the worker stream"
        );
        Ok(())
    })?;
    assert_eq!(
        storage
            .lookup_invocation_outcome(cancelled_invocation_id, actor, 9_998)?
            .ok_or("cancelled estimate outcome missing")?
            .status,
        OutcomeStatus::Aborted
    );

    let expired_storage = StorageManager::with_config(
        &database,
        prdb_engine::storage_manager::StorageManagerConfig {
            max_invocation_age: Duration::from_secs(1),
            ..Default::default()
        },
    )?;
    let expired_invocation_id = id(0x8e);
    let mut expired_estimate =
        expired_storage.begin_invocation(expired_invocation_id, actor, 9_999, 8, 512)?;
    let (mut expired_planner_client, mut expired_planner_server) =
        std::os::unix::net::UnixStream::pair()?;
    expired_planner_client.set_read_timeout(Some(Duration::from_secs(2)))?;
    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let planner = scope.spawn(|| {
            let result = serve_planner(&mut expired_planner_server, &mut expired_estimate);
            let _ = expired_planner_server.shutdown(Shutdown::Both);
            result
        });
        prdb_engine::ipc::write_frame(
            &mut expired_planner_client,
            &IpcFrame {
                operation: IpcOperation::DescribeRelation as u16,
                request_id: 1,
                payload: ResolveRelationRequest {
                    schema_name: "planner_stats".to_owned(),
                    relation_name: "observations".to_owned(),
                }
                .encode()?,
            },
        )?;
        let description = prdb_engine::ipc::read_frame(&mut expired_planner_client)?
            .ok_or("planner stream closed before expiry test relation")?;
        let handle = DescribeRelationResponse::decode(&description.payload)?
            .table
            .ok_or("authorized relation was not described for expiry test")?
            .0;
        std::thread::sleep(Duration::from_millis(1_100));
        prdb_engine::ipc::write_frame(
            &mut expired_planner_client,
            &IpcFrame {
                operation: IpcOperation::EstimateScan as u16,
                request_id: 2,
                payload: EstimateScanRequest {
                    handle,
                    column_ids: Vec::new(),
                }
                .encode()?,
            },
        )?;
        let expired_response = prdb_engine::ipc::read_frame(&mut expired_planner_client)?;
        expired_planner_client.shutdown(Shutdown::Write)?;
        drop(expired_planner_client);
        let planner_result = planner.join().expect("planner thread panicked");
        assert!(
            expired_response.is_none(),
            "expired estimation must terminate the planner stream instead of returning a fallback response: {expired_response:?}"
        );
        assert!(
            planner_result.is_err(),
            "expired planning must abort the worker stream"
        );
        Ok(())
    })?;
    assert_eq!(
        expired_storage
            .lookup_invocation_outcome(expired_invocation_id, actor, 9_998)?
            .ok_or("expired estimate outcome missing")?
            .status,
        OutcomeStatus::Aborted
    );

    let mut transaction = storage.begin_invocation(id(0x88), actor, 9_999, 8, 512)?;
    transaction.stage_put(
        prdb_engine::overlay::RowId {
            table: table.table_id,
            row: id(0x8c),
        },
        &encode_tuple(
            &[ColumnSchema {
                scalar_type: ScalarType::Integer,
                nullable: true,
            }],
            1,
            &[Some(ScalarValue::Integer(7))],
        )?,
    )?;
    for (row_id, value) in [(0x8d, 8), (0x8e, 9), (0x8f, 10), (0x90, 11)] {
        transaction.stage_put(
            prdb_engine::overlay::RowId {
                table: table.table_id,
                row: id(row_id),
            },
            &encode_tuple(
                &[ColumnSchema {
                    scalar_type: ScalarType::Integer,
                    nullable: true,
                }],
                1,
                &[Some(ScalarValue::Integer(value))],
            )?,
        )?;
    }
    let shared_transaction = std::sync::Arc::new(std::sync::Mutex::new(Some(transaction)));
    let (mut client, mut server) = std::os::unix::net::UnixStream::pair()?;
    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let service_transaction = std::sync::Arc::clone(&shared_transaction);
        let observer_transaction = std::sync::Arc::clone(&shared_transaction);
        let planner = scope.spawn(move || {
            serve_planner_shared_with_observer(
                &mut server,
                &service_transaction,
                |operation, request_id| {
                    if operation == IpcOperation::EstimateScan as u16 && request_id == 7 {
                        let mut invocation = observer_transaction
                            .lock()
                            .expect("shared planner invocation lock");
                        invocation
                            .as_mut()
                            .expect("shared planner invocation remains active")
                            .stage_put(
                                prdb_engine::overlay::RowId {
                                    table: table.table_id,
                                    row: id(0x91),
                                },
                                &encode_tuple(
                                    &[ColumnSchema {
                                        scalar_type: ScalarType::Integer,
                                        nullable: true,
                                    }],
                                    1,
                                    &[Some(ScalarValue::Integer(7))],
                                )
                                .expect("encode private predicate-match row"),
                            )
                            .expect("stage private predicate-match row");
                    }
                    if operation == IpcOperation::EstimateScan as u16 && request_id == 10 {
                        let mut invocation = observer_transaction
                            .lock()
                            .expect("shared planner invocation lock");
                        invocation
                            .as_mut()
                            .expect("shared planner invocation remains active")
                            .stage_put(
                                prdb_engine::overlay::RowId {
                                    table: table.table_id,
                                    row: id(0x92),
                                },
                                &encode_tuple(
                                    &[ColumnSchema {
                                        scalar_type: ScalarType::Integer,
                                        nullable: true,
                                    }],
                                    1,
                                    &[Some(ScalarValue::Integer(12))],
                                )
                                .expect("encode second private population row"),
                            )
                            .expect("stage second private population row");
                    }
                    Ok(())
                },
            )
        });
        prdb_engine::ipc::write_frame(
            &mut client,
            &IpcFrame {
                operation: IpcOperation::DescribeRelation as u16,
                request_id: 1,
                payload: ResolveRelationRequest {
                    schema_name: "planner_stats".to_owned(),
                    relation_name: "observations".to_owned(),
                }
                .encode()?,
            },
        )?;
        let description = prdb_engine::ipc::read_frame(&mut client)?
            .ok_or("planner stream closed after relation description")?;
        let (handle, _) = DescribeRelationResponse::decode(&description.payload)?
            .table
            .ok_or("authorized relation was not described")?;

        prdb_engine::ipc::write_frame(
            &mut client,
            &IpcFrame {
                operation: IpcOperation::DescribePlanRelation as u16,
                request_id: 2,
                payload: DescribePlanRelationRequest {
                    schema_name: "planner_stats".to_owned(),
                    relation_name: "observations".to_owned(),
                    include_scan_estimate: true,
                    population_estimate_max_rows: None,
                }
                .encode()?,
            },
        )?;
        let plan_relation = prdb_engine::ipc::read_frame(&mut client)?
            .ok_or("planner stream closed before plan relation description")?;
        let plan_relation_response = DescribePlanRelationResponse::decode(&plan_relation.payload)?;
        assert!(!plan_relation_response.proven_empty);
        assert_eq!(plan_relation_response.visible_rows_estimate, None);
        let plan_relation = plan_relation_response
            .relation
            .ok_or("authorized plan relation was not described")?;
        assert_eq!(plan_relation.schema_id, schema.schema_id);
        assert_eq!(plan_relation.schema_version, 1);
        assert_eq!(plan_relation.table_epoch, 3);
        assert_eq!(plan_relation.relation.relation_id, table.table_id);

        prdb_engine::ipc::write_frame(
            &mut client,
            &IpcFrame {
                operation: IpcOperation::DescribePlanRelation as u16,
                request_id: 3,
                payload: DescribePlanRelationRequest {
                    schema_name: "planner_stats".to_owned(),
                    relation_name: "observations".to_owned(),
                    include_scan_estimate: false,
                    population_estimate_max_rows: Some(128),
                }
                .encode()?,
            },
        )?;
        let join_population = prdb_engine::ipc::read_frame(&mut client)?
            .ok_or("planner stream closed before join population estimate")?;
        let join_population = DescribePlanRelationResponse::decode(&join_population.payload)?;
        assert_eq!(join_population.visible_rows_estimate, Some(5));

        prdb_engine::ipc::write_frame(
            &mut client,
            &IpcFrame {
                operation: IpcOperation::DescribePlanRelation as u16,
                request_id: 4,
                payload: DescribePlanRelationRequest {
                    schema_name: "planner_stats".to_owned(),
                    relation_name: "observations".to_owned(),
                    include_scan_estimate: false,
                    population_estimate_max_rows: Some(128),
                }
                .encode()?,
            },
        )?;
        let repeated_population = prdb_engine::ipc::read_frame(&mut client)?
            .ok_or("planner stream closed before repeated population estimate")?;
        assert_eq!(
            DescribePlanRelationResponse::decode(&repeated_population.payload)?
                .visible_rows_estimate,
            Some(5),
            "an unchanged planner session must reuse its exact population estimate"
        );

        let estimate_predicate = prdb_engine::optimizer_binding::BoundPredicate::Compare {
            operator: prdb_engine::optimizer_request::Comparison::Equal,
            left: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Column(
                BoundColumn {
                    column_id: table.column_ids[0],
                    ordinal: 0,
                    scalar_type: ScalarType::Integer,
                    nullable: true,
                },
            )),
            right: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Integer(7)),
        };
        prdb_engine::ipc::write_frame(
            &mut client,
            &IpcFrame {
                operation: IpcOperation::EstimateScan as u16,
                request_id: 5,
                payload: EstimatePredicateRequest::from_bound_predicate(
                    "planner_stats".to_owned(),
                    "observations".to_owned(),
                    &estimate_predicate,
                )?
                .encode()?,
            },
        )?;
        let predicate_estimate = prdb_engine::ipc::read_frame(&mut client)?
            .ok_or("planner stream closed before predicate estimate")?;
        assert_eq!(
            EstimatePredicateResponse::decode(&predicate_estimate.payload)?,
            EstimatePredicateResponse::Exact {
                visible_rows: 5,
                matching_rows: 1,
            }
        );

        prdb_engine::ipc::write_frame(
            &mut client,
            &IpcFrame {
                operation: IpcOperation::EstimateScan as u16,
                request_id: 6,
                payload: EstimatePredicateRequest::from_bound_predicate(
                    "planner_stats".to_owned(),
                    "observations".to_owned(),
                    &estimate_predicate,
                )?
                .encode()?,
            },
        )?;
        let repeated_estimate = prdb_engine::ipc::read_frame(&mut client)?
            .ok_or("planner stream closed before repeated predicate estimate")?;
        assert_eq!(
            EstimatePredicateResponse::decode(&repeated_estimate.payload)?,
            EstimatePredicateResponse::Exact {
                visible_rows: 5,
                matching_rows: 1,
            },
            "an identical request in one unchanged session must preserve its exact result"
        );

        prdb_engine::ipc::write_frame(
            &mut client,
            &IpcFrame {
                operation: IpcOperation::EstimateScan as u16,
                request_id: 7,
                payload: EstimatePredicateRequest::from_bound_predicate(
                    "planner_stats".to_owned(),
                    "observations".to_owned(),
                    &estimate_predicate,
                )?
                .encode()?,
            },
        )?;
        let post_write_estimate = prdb_engine::ipc::read_frame(&mut client)?
            .ok_or("planner stream closed before post-write predicate estimate")?;
        assert_eq!(
            EstimatePredicateResponse::decode(&post_write_estimate.payload)?,
            EstimatePredicateResponse::Exact {
                visible_rows: 6,
                matching_rows: 2,
            },
            "a private write between identical estimates must invalidate session reuse"
        );

        prdb_engine::ipc::write_frame(
            &mut client,
            &IpcFrame {
                operation: IpcOperation::DescribePlanRelation as u16,
                request_id: 8,
                payload: DescribePlanRelationRequest {
                    schema_name: "planner_stats".to_owned(),
                    relation_name: "observations".to_owned(),
                    include_scan_estimate: false,
                    population_estimate_max_rows: Some(128),
                }
                .encode()?,
            },
        )?;
        let post_write_population = prdb_engine::ipc::read_frame(&mut client)?
            .ok_or("planner stream closed before post-write population estimate")?;
        assert_eq!(
            DescribePlanRelationResponse::decode(&post_write_population.payload)?
                .visible_rows_estimate,
            Some(6),
            "a private write must invalidate the cached population estimate"
        );

        for (request_id, expected_joined_rows) in [(9, 8), (10, 9), (11, 9)] {
            prdb_engine::ipc::write_frame(
                &mut client,
                &IpcFrame {
                    operation: IpcOperation::EstimateScan as u16,
                    request_id,
                    payload: EstimateJoinRequest {
                        schema_name: "planner_stats".to_owned(),
                        left_relation_name: "observations".to_owned(),
                        right_relation_name: "observations".to_owned(),
                        left_key_column_ids: vec![table.column_ids[0]],
                        right_key_column_ids: vec![table.column_ids[0]],
                        maximum_rows: 128,
                    }
                    .encode()?,
                },
            )?;
            let response = prdb_engine::ipc::read_frame(&mut client)?
                .ok_or("planner stream closed before join estimate cache check")?;
            assert_eq!(
                EstimateJoinResponse::decode(&response.payload)?,
                EstimateJoinResponse::Exact {
                    joined_rows: expected_joined_rows,
                },
                "join estimate cache must reflect the write generation and reuse only within it"
            );
        }

        for request_id in 12..(2 + MAX_PLANNER_ESTIMATE_REQUESTS_PER_CONNECTION as u64) {
            prdb_engine::ipc::write_frame(
                &mut client,
                &IpcFrame {
                    operation: IpcOperation::EstimateScan as u16,
                    request_id,
                    payload: EstimateScanRequest {
                        handle: handle.clone(),
                        column_ids: Vec::new(),
                    }
                    .encode()?,
                },
            )?;
            let response = prdb_engine::ipc::read_frame(&mut client)?
                .ok_or("planner stream closed before allowance exhaustion")?;
            let visible_rows = if request_id >= 10 { 7 } else { 6 };
            assert_eq!(
                EstimateScanResponse::decode(&response.payload)?,
                EstimateScanResponse::Exact(prdb_engine::statistics::AuthorizedScanSummary {
                    visible_rows,
                    columns: vec![prdb_engine::statistics::ColumnPopulation {
                        readable_values: visible_rows,
                        nulls: 0,
                        redacted: 0,
                    }],
                })
            );
        }

        let exhausted_id = 2 + MAX_PLANNER_ESTIMATE_REQUESTS_PER_CONNECTION as u64;
        prdb_engine::ipc::write_frame(
            &mut client,
            &IpcFrame {
                operation: IpcOperation::EstimateScan as u16,
                request_id: exhausted_id,
                payload: EstimateScanRequest {
                    handle,
                    column_ids: Vec::new(),
                }
                .encode()?,
            },
        )?;
        let exhausted = prdb_engine::ipc::read_frame(&mut client)?
            .ok_or("planner stream closed on estimate fallback")?;
        assert_eq!(
            EstimateScanResponse::decode(&exhausted.payload)?,
            EstimateScanResponse::Unknown
        );

        let fallback_id = exhausted_id + 1;
        assert!(fallback_id < MAX_PLANNER_REQUESTS_PER_CONNECTION as u64);
        prdb_engine::ipc::write_frame(
            &mut client,
            &IpcFrame {
                operation: IpcOperation::DescribePlanRelation as u16,
                request_id: fallback_id,
                payload: DescribePlanRelationRequest {
                    schema_name: "planner_stats".to_owned(),
                    relation_name: "observations".to_owned(),
                    include_scan_estimate: true,
                    population_estimate_max_rows: None,
                }
                .encode()?,
            },
        )?;
        let fallback = prdb_engine::ipc::read_frame(&mut client)?
            .ok_or("planner stream closed after estimate fallback")?;
        let fallback = DescribePlanRelationResponse::decode(&fallback.payload)?;
        assert!(!fallback.proven_empty);
        assert_eq!(fallback.visible_rows_estimate, None);
        assert!(fallback.relation.is_some());
        client.shutdown(Shutdown::Write)?;
        drop(client);

        planner
            .join()
            .expect("planner server thread panicked")
            .map_err(|error| format!("planner server failed: {error:?}"))?;
        Ok(())
    })?;
    let mut transaction = std::sync::Arc::try_unwrap(shared_transaction)
        .unwrap_or_else(|_| panic!("planner service released its invocation handle"))
        .into_inner()
        .expect("planner invocation lock remains healthy")
        .expect("planner invocation remains available after clean EOF");
    assert!(transaction.check_deadline().is_ok());

    let (mut executor_client, mut executor_storage) = std::os::unix::net::UnixStream::pair()?;
    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let executor = scope.spawn(|| {
            prdb_engine::executor_ipc::serve_executor(&mut executor_storage, &mut transaction)
        });
        for (request_id, relation_name) in [(1, "observations"), (2, "hidden_or_missing")] {
            prdb_engine::ipc::write_frame(
                &mut executor_client,
                &IpcFrame {
                    operation: IpcOperation::ResolvePlanRelation as u16,
                    request_id,
                    payload: ResolveRelationRequest {
                        schema_name: "planner_stats".to_owned(),
                        relation_name: relation_name.to_owned(),
                    }
                    .encode()?,
                },
            )?;
            let response = prdb_engine::ipc::read_frame(&mut executor_client)?
                .ok_or("executor stream closed before plan relation response")?;
            let decoded = DescribePlanRelationResponse::decode(&response.payload)?;
            let relation = decoded.relation;
            if request_id == 1 {
                assert!(!decoded.proven_empty);
                let relation =
                    relation.ok_or("executor did not re-resolve authorized plan relation")?;
                assert_eq!(relation.schema_id, schema.schema_id);
                assert_eq!(relation.schema_version, 1);
                assert_eq!(relation.table_epoch, 3);
                assert_eq!(relation.relation.relation_id, table.table_id);
                assert_eq!(relation.relation.columns[0].column_id, table.column_ids[0]);
            } else {
                assert!(relation.is_none());
            }
        }
        executor_client.shutdown(Shutdown::Write)?;
        drop(executor_client);
        executor
            .join()
            .expect("executor metadata server thread panicked")
            .map_err(|error| format!("executor metadata server failed: {error:?}"))?;
        Ok(())
    })?;

    let denied_executor_invocation = id(0xa1);
    let mut denied_executor_transaction =
        storage.begin_invocation(denied_executor_invocation, actor, 9_999, 1, 64)?;
    let (mut denied_executor_client, mut denied_executor_server) =
        std::os::unix::net::UnixStream::pair()?;
    denied_executor_client.set_read_timeout(Some(Duration::from_secs(5)))?;
    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let executor = scope.spawn(move || {
            prdb_engine::executor_ipc::serve_executor(
                &mut denied_executor_server,
                &mut denied_executor_transaction,
            )
        });
        prdb_engine::ipc::write_frame(
            &mut denied_executor_client,
            &IpcFrame {
                operation: IpcOperation::EstimateScan as u16,
                request_id: 1,
                payload: Vec::new(),
            },
        )?;
        denied_executor_client.shutdown(Shutdown::Write)?;
        let result = executor.join().expect("executor denial server panicked");
        assert!(matches!(
            result,
            Err(prdb_engine::executor_ipc::ExecutorConnectionError::Request(
                prdb_engine::executor_ipc::ExecutorRequestError::Dispatch(
                    prdb_engine::ipc::DispatchError::OperationNotAllowed {
                        role: prdb_engine::ipc::ServiceRole::Executor,
                        operation: IpcOperation::EstimateScan,
                    }
                )
            ))
        ));
        assert!(
            prdb_engine::ipc::read_frame(&mut denied_executor_client)
                .map_err(|error| format!("executor denial stream did not close: {error:?}"))?
                .is_none()
        );
        Ok(())
    })?;
    assert_eq!(
        storage
            .lookup_invocation_outcome(denied_executor_invocation, actor, 9_998)?
            .ok_or("executor estimate denial did not durably abort")?
            .status,
        OutcomeStatus::Aborted
    );

    let denied_planner_invocation = id(0xa2);
    let mut denied_planner_transaction =
        storage.begin_invocation(denied_planner_invocation, actor, 9_999, 1, 64)?;
    let (mut denied_planner_client, mut denied_planner_server) =
        std::os::unix::net::UnixStream::pair()?;
    denied_planner_client.set_read_timeout(Some(Duration::from_secs(5)))?;
    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let planner = scope.spawn(move || {
            serve_planner(&mut denied_planner_server, &mut denied_planner_transaction)
        });
        prdb_engine::ipc::write_frame(
            &mut denied_planner_client,
            &IpcFrame {
                operation: IpcOperation::OpenScan as u16,
                request_id: 1,
                payload: Vec::new(),
            },
        )?;
        denied_planner_client.shutdown(Shutdown::Write)?;
        let result = planner.join().expect("planner denial server panicked");
        assert!(matches!(
            result,
            Err(prdb_engine::planner_ipc::PlannerConnectionError::Request(
                prdb_engine::planner_ipc::PlannerRequestError::Dispatch(
                    prdb_engine::ipc::DispatchError::OperationNotAllowed {
                        role: prdb_engine::ipc::ServiceRole::Planner,
                        operation: IpcOperation::OpenScan,
                    }
                )
            ))
        ));
        assert!(
            prdb_engine::ipc::read_frame(&mut denied_planner_client)
                .map_err(|error| format!("planner denial stream did not close: {error:?}"))?
                .is_none()
        );
        Ok(())
    })?;
    assert_eq!(
        storage
            .lookup_invocation_outcome(denied_planner_invocation, actor, 9_998)?
            .ok_or("planner scan denial did not durably abort")?
            .status,
        OutcomeStatus::Aborted
    );

    let mut large_join_estimate =
        storage.begin_invocation(id(0xa3), actor, 9_999, 4096, 8 * 1024 * 1024)?;
    for row_number in 1..=4096u32 {
        let mut row_id = [0; 16];
        row_id[12..].copy_from_slice(&row_number.to_be_bytes());
        large_join_estimate.stage_put(
            prdb_engine::overlay::RowId {
                table: table.table_id,
                row: row_id,
            },
            &encode_tuple(
                &[ColumnSchema {
                    scalar_type: ScalarType::Integer,
                    nullable: true,
                }],
                1,
                &[Some(ScalarValue::Integer(7))],
            )?,
        )?;
    }
    let (mut large_join_client, mut large_join_server) = std::os::unix::net::UnixStream::pair()?;
    large_join_client.set_read_timeout(Some(Duration::from_secs(5)))?;
    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let planner =
            scope.spawn(|| serve_planner(&mut large_join_server, &mut large_join_estimate));
        prdb_engine::ipc::write_frame(
            &mut large_join_client,
            &IpcFrame {
                operation: IpcOperation::EstimateScan as u16,
                request_id: 1,
                payload: EstimateJoinRequest {
                    schema_name: "planner_stats".to_owned(),
                    left_relation_name: "observations".to_owned(),
                    right_relation_name: "observations".to_owned(),
                    left_key_column_ids: vec![table.column_ids[0]],
                    right_key_column_ids: vec![table.column_ids[0]],
                    maximum_rows: 4096,
                }
                .encode()?,
            },
        )?;
        let response = prdb_engine::ipc::read_frame(&mut large_join_client)?
            .ok_or("planner stream closed before the 4,096-row join estimate")?;
        assert_eq!(response.request_id, 1);
        assert_eq!(
            EstimateJoinResponse::decode(&response.payload)?,
            EstimateJoinResponse::Exact {
                joined_rows: 4096 * 4096,
            }
        );
        large_join_client.shutdown(Shutdown::Write)?;
        drop(large_join_client);
        planner
            .join()
            .expect("large join estimate planner thread panicked")
            .map_err(|error| format!("large join estimate planner failed: {error:?}"))?;
        Ok(())
    })?;
    large_join_estimate.abort()?;

    let mut optimizer_metadata_transaction =
        storage.begin_invocation(id(0x8b), actor, 9_999, 8, 512)?;
    let (optimizer_submission_worker, mut optimizer_client) =
        std::os::unix::net::UnixStream::pair()?;
    let (optimizer_planner_worker, mut optimizer_planner_storage) =
        std::os::unix::net::UnixStream::pair()?;
    use std::os::fd::AsRawFd;
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};
    let submission_fd = optimizer_submission_worker.as_raw_fd();
    let planner_fd = optimizer_planner_worker.as_raw_fd();
    let mut optimizer = Command::new(env!("CARGO_BIN_EXE_prdb-optimizer"));
    optimizer
        .arg("--submission-fd")
        .arg(submission_fd.to_string())
        .arg("--planner-fd")
        .arg(planner_fd.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // SAFETY: the child callback touches only the two live Unix-stream file
    // descriptors captured above before exec, clearing their close-on-exec
    // flag so the optimizer receives the intended channel endpoints.
    unsafe {
        optimizer.pre_exec(move || {
            for descriptor in [submission_fd, planner_fd] {
                // SAFETY: fcntl operates on inherited descriptors in the child.
                let flags = libc::fcntl(descriptor, libc::F_GETFD);
                if flags < 0
                    || libc::fcntl(descriptor, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0
                {
                    return Err(std::io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }
    let mut optimizer = optimizer.spawn()?;
    drop(optimizer_submission_worker);
    drop(optimizer_planner_worker);
    optimizer_client.set_read_timeout(Some(Duration::from_secs(5)))?;
    let mut filtered_plan_for_resolution = None;
    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let planner_thread = scope.spawn(|| {
            serve_planner(
                &mut optimizer_planner_storage,
                &mut optimizer_metadata_transaction,
            )
        });
        prdb_engine::ipc::write_frame(
            &mut optimizer_client,
            &IpcFrame {
                operation: IpcOperation::OptimizeSelect as u16,
                request_id: 1,
                payload: Vec::new(),
            },
        )?;
        let invalid_frame = prdb_engine::ipc::read_frame(&mut optimizer_client)?
            .ok_or("optimizer stream closed after a malformed request")?;
        assert_eq!(invalid_frame.request_id, 1);
        assert_eq!(
            OptimizerResponse::decode(&invalid_frame.payload)?.status,
            prdb_engine::optimizer_ipc::OptimizerStatus::InvalidRequest
        );
        prdb_engine::ipc::write_frame(
            &mut optimizer_client,
            &IpcFrame {
                operation: IpcOperation::OptimizeSelect as u16,
                request_id: 2,
                payload: encode_select("planner_stats", "observations", "value"),
            },
        )?;
        let optimizer_frame = prdb_engine::ipc::read_frame(&mut optimizer_client)?
            .ok_or("optimizer stream closed without a plan response")?;
        assert_eq!(
            optimizer_frame.operation,
            IpcOperation::OptimizeSelect as u16
        );
        assert_eq!(optimizer_frame.request_id, 2);
        let optimizer_response = OptimizerResponse::decode(&optimizer_frame.payload)?;
        assert_eq!(
            optimizer_response.status,
            prdb_engine::optimizer_ipc::OptimizerStatus::Success
        );
        let plan = decode_plan_v1(
            optimizer_response
                .plan
                .as_deref()
                .ok_or("successful optimizer response omitted its plan")?,
        )?;
        assert_eq!(plan.relation.schema_id, schema.schema_id);
        assert_eq!(plan.relation.relation_id, table.table_id);
        assert!(plan.empty_view_hint);
        prdb_engine::ipc::write_frame(
            &mut optimizer_client,
            &IpcFrame {
                operation: IpcOperation::OptimizeSelect as u16,
                request_id: 3,
                payload: encode_filtered_select("planner_stats", "observations", "value"),
            },
        )?;
        let filtered_frame = prdb_engine::ipc::read_frame(&mut optimizer_client)?
            .ok_or("optimizer stream closed without a filtered plan response")?;
        assert_eq!(filtered_frame.request_id, 3);
        let filtered_response = OptimizerResponse::decode(&filtered_frame.payload)?;
        assert_eq!(
            filtered_response.status,
            prdb_engine::optimizer_ipc::OptimizerStatus::Success
        );
        let filtered_plan_bytes = filtered_response
            .plan
            .as_deref()
            .ok_or("successful filtered response omitted its plan")?;
        let filtered_plan = decode_plan_v1(filtered_plan_bytes)?;
        assert!(matches!(
            filtered_plan.nodes.get(1),
            Some(PlanNode::Filter(prdb_engine::optimizer_binding::BoundPredicate::Compare {
                operator,
                left,
                right,
                ..
            })) if *operator == prdb_engine::optimizer_request::Comparison::Equal
                && matches!(left.as_ref(), prdb_engine::optimizer_binding::BoundPredicate::Column(column) if column.column_id == table.column_ids[0])
                && matches!(right.as_ref(), prdb_engine::optimizer_binding::BoundPredicate::Integer(7))
        ));
        filtered_plan_for_resolution = Some(filtered_plan.clone());
        prdb_engine::ipc::write_frame(
            &mut optimizer_client,
            &IpcFrame {
                operation: IpcOperation::OptimizeSelect as u16,
                request_id: 4,
                payload: encode_select("planner_stats", "hidden_or_missing", "value"),
            },
        )?;
        let unavailable_frame = prdb_engine::ipc::read_frame(&mut optimizer_client)?
            .ok_or("optimizer stream closed without an unavailable response")?;
        assert_eq!(unavailable_frame.request_id, 4);
        assert_eq!(
            OptimizerResponse::decode(&unavailable_frame.payload)?.status,
            prdb_engine::optimizer_ipc::OptimizerStatus::Unavailable
        );
        optimizer_client.shutdown(Shutdown::Write)?;
        drop(optimizer_client);
        let status = optimizer.wait()?;
        assert!(status.success(), "optimizer child exited with {status}");
        planner_thread
            .join()
            .expect("planner storage thread panicked")
            .map_err(|error| format!("planner storage stream failed: {error:?}"))?;
        Ok(())
    })?;
    let filtered_plan =
        filtered_plan_for_resolution.ok_or("optimizer did not return a filtered plan")?;
    let (mut executor_plan_client, mut executor_plan_storage) =
        std::os::unix::net::UnixStream::pair()?;
    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let executor = scope.spawn(|| {
            prdb_engine::executor_ipc::serve_executor(
                &mut executor_plan_storage,
                &mut optimizer_metadata_transaction,
            )
        });
        prdb_engine::ipc::write_frame(
            &mut executor_plan_client,
            &IpcFrame {
                operation: IpcOperation::ResolvePlanRelation as u16,
                request_id: 1,
                payload: ResolveRelationRequest {
                    schema_name: filtered_plan.relation.schema_name.clone(),
                    relation_name: filtered_plan.relation.relation_name.clone(),
                }
                .encode()?,
            },
        )?;
        let resolved_frame = prdb_engine::ipc::read_frame(&mut executor_plan_client)?
            .ok_or("executor stream closed without plan identity response")?;
        let resolved_relation = DescribePlanRelationResponse::decode(&resolved_frame.payload)?
            .relation
            .ok_or("executor could not independently resolve filtered plan relation")?;
        assert_eq!(
            resolved_relation.schema_id,
            filtered_plan.relation.schema_id
        );
        assert_eq!(
            resolved_relation.schema_version,
            filtered_plan.relation.schema_version
        );
        assert_eq!(
            resolved_relation.table_epoch,
            filtered_plan.relation.table_epoch
        );
        assert_eq!(
            resolved_relation.relation.relation_id,
            filtered_plan.relation.relation_id
        );
        let planned_scan_columns = match filtered_plan.nodes.first() {
            Some(PlanNode::Scan { columns }) => columns,
            _ => return Err("filtered plan did not begin with a scan".into()),
        };
        assert_eq!(
            planned_scan_columns.len(),
            resolved_relation.relation.columns.len()
        );
        for planned_column in planned_scan_columns {
            let declared_column = resolved_relation
                .relation
                .columns
                .iter()
                .find(|column| column.ordinal == planned_column.ordinal)
                .ok_or("executor metadata omitted a planned scan column")?;
            assert_eq!(declared_column.column_id, planned_column.column_id);
            assert_eq!(declared_column.scalar_type, planned_column.scalar_type);
            assert_eq!(declared_column.nullable, planned_column.nullable);
        }
        prdb_engine::ipc::write_frame(
            &mut executor_plan_client,
            &IpcFrame {
                operation: IpcOperation::ResolveRelation as u16,
                request_id: 2,
                payload: ResolveRelationRequest {
                    schema_name: filtered_plan.relation.schema_name.clone(),
                    relation_name: filtered_plan.relation.relation_name.clone(),
                }
                .encode()?,
            },
        )?;
        let capability_frame = prdb_engine::ipc::read_frame(&mut executor_plan_client)?
            .ok_or("executor stream closed without a relation capability")?;
        assert_eq!(capability_frame.request_id, 2);
        let (executor_handle, _) = match prdb_engine::executor_ipc::ResolveRelationResponse::decode(
            &capability_frame.payload,
        )? {
            prdb_engine::executor_ipc::ResolveRelationResponse::Resolved {
                handle,
                description,
            } => {
                assert_eq!(description.relation_id, filtered_plan.relation.relation_id);
                (handle, description)
            }
            prdb_engine::executor_ipc::ResolveRelationResponse::Unavailable => {
                return Err("executor denied the validated plan relation".into());
            }
        };
        prdb_engine::ipc::write_frame(
            &mut executor_plan_client,
            &IpcFrame {
                operation: IpcOperation::OpenScan as u16,
                request_id: 3,
                payload: prdb_engine::executor_ipc::OpenScanRequest {
                    relation: executor_handle,
                    column_ids: planned_scan_columns
                        .iter()
                        .map(|column| column.column_id)
                        .collect(),
                }
                .encode()?,
            },
        )?;
        let scan_frame = prdb_engine::ipc::read_frame(&mut executor_plan_client)?
            .ok_or("executor stream closed without a scan cursor")?;
        assert_eq!(scan_frame.request_id, 3);
        let scan_cursor = prdb_engine::executor_ipc::OpenScanResponse::decode(&scan_frame.payload)?;
        prdb_engine::ipc::write_frame(
            &mut executor_plan_client,
            &IpcFrame {
                operation: IpcOperation::NextBatch as u16,
                request_id: 4,
                payload: prdb_engine::executor_ipc::NextBatchRequest {
                    cursor: scan_cursor.cursor,
                    max_scanned: 8,
                    max_bytes: 4096,
                }
                .encode()?,
            },
        )?;
        let batch_frame = prdb_engine::ipc::read_frame(&mut executor_plan_client)?
            .ok_or("executor stream closed without a scan batch")?;
        assert_eq!(batch_frame.request_id, 4);
        let batch = prdb_engine::executor_ipc::NextBatchResponse::decode(&batch_frame.payload)?;
        assert!(batch.rows.is_empty());
        assert!(batch.continuation.is_none());
        executor_plan_client.shutdown(Shutdown::Write)?;
        drop(executor_plan_client);
        executor
            .join()
            .expect("executor plan metadata thread panicked")
            .map_err(|error| format!("executor plan metadata stream failed: {error:?}"))?;
        Ok(())
    })?;
    optimizer_metadata_transaction.abort()?;

    // Exercise the constant-predicate fast path through the real planner IPC
    // server, using a nonempty committed population and its fresh stats record.
    let committed_row_id = id(0xe1);
    let mut committed_key = [0; 32];
    committed_key[..16].copy_from_slice(&table.table_id);
    committed_key[16..].copy_from_slice(&committed_row_id);
    let committed_tuple = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: true,
        }],
        1,
        &[Some(ScalarValue::Integer(7))],
    )?;
    commit_rows(
        &database,
        id(0xe2),
        actor,
        9_999,
        &[RowWrite {
            key: committed_key,
            expected_version: None,
            payload: Some(committed_tuple),
        }],
    )?;
    assert_eq!(
        storage.collect_committed_table_population(table.table_id)?,
        prdb_engine::statistics::StatisticsCollectionOutcome::Exact
    );

    let constant_invocation_id = id(0xe3);
    let mut constant_invocation =
        storage.begin_invocation(constant_invocation_id, actor, 9_999, 8, 512)?;
    let constant_predicate = prdb_engine::optimizer_binding::BoundPredicate::Compare {
        operator: prdb_engine::optimizer_request::Comparison::Equal,
        left: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Integer(1)),
        right: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Integer(2)),
    };
    let (mut constant_client, mut constant_server) = std::os::unix::net::UnixStream::pair()?;
    constant_client.set_read_timeout(Some(Duration::from_secs(5)))?;
    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        let planner = scope.spawn(|| serve_planner(&mut constant_server, &mut constant_invocation));
        prdb_engine::ipc::write_frame(
            &mut constant_client,
            &IpcFrame {
                operation: IpcOperation::EstimateScan as u16,
                request_id: 1,
                payload: EstimatePredicateRequest::from_bound_predicate(
                    "planner_stats".to_owned(),
                    "observations".to_owned(),
                    &constant_predicate,
                )?
                .encode()?,
            },
        )?;
        let response = prdb_engine::ipc::read_frame(&mut constant_client)?
            .ok_or("planner stream closed before constant predicate estimate")?;
        assert_eq!(response.request_id, 1);
        assert_eq!(
            EstimatePredicateResponse::decode(&response.payload)?,
            EstimatePredicateResponse::Exact {
                visible_rows: 1,
                matching_rows: 0,
            }
        );
        constant_client.shutdown(Shutdown::Write)?;
        drop(constant_client);
        planner
            .join()
            .expect("constant predicate planner server thread panicked")
            .map_err(|error| format!("constant predicate planner stream failed: {error:?}"))?;
        Ok(())
    })?;
    constant_invocation.abort()?;
    Ok(())
}
