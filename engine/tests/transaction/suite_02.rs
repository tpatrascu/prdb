use prdb_engine::authorization::{Expr, Operation, SanitizedCell};
use prdb_engine::catalog::{CatalogField, WireType};
use prdb_engine::outcome::OutcomeStatus;
use prdb_engine::row_record::RowRecord;
use prdb_engine::schema::{self, CATALOG, ROWS};
use prdb_engine::transaction::{InsertInput, InvocationTransaction, TransactionError};
use prdb_engine::tuple::{ColumnSchema, FieldPatch, ScalarType, ScalarValue, decode_tuple};
use redb::ReadableDatabase;

use super::helpers::*;

pub(super) fn executor_resolve_relation_ipc_dispatch_returns_only_authorized_handle()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::executor_ipc::{
        ExecutorRequestError, OpenScanRequest, ResolveRelationRequest, ResolveRelationResponse,
        StageInsertRequest, dispatch_executor_request, dispatch_executor_resolve_relation,
    };
    use prdb_engine::ipc::{IpcFrame, Operation, ServiceRole};
    use prdb_engine::transaction::InsertInput;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    install_second_relation_column(&database)?;
    let app_schema_id = [0x91; 16];
    let write = database.begin_write()?;
    {
        let mut catalog = write.open_table(CATALOG)?;
        let mut grants = write.open_table(prdb_engine::schema::GRANTS)?;
        let (grant_key, grant_value) = grant_catalog_record(
            [10; 16],
            app_schema_id,
            1,
            1,
            app_schema_id,
            builtin_owner_id(),
            1,
        );
        catalog.insert(&grant_key[..], &grant_value[..])?;
        for (direction, indexed_id) in [(1u8, [10; 16]), (2u8, app_schema_id)] {
            let mut index_key = Vec::with_capacity(76);
            index_key.push(direction);
            index_key.extend_from_slice(&indexed_id);
            index_key.extend_from_slice(&grant_key);
            grants.insert(&index_key[..], &[][..])?;
        }
    }
    write.commit()?;
    let mut invocation = InvocationTransaction::begin(&database, [217; 16], [9; 16], 9_999, 1, 64)?;
    let mut connection = invocation.open_executor_metadata_connection();

    let encode_request = |schema: &str, relation: &str| {
        ResolveRelationRequest {
            schema_name: schema.to_owned(),
            relation_name: relation.to_owned(),
        }
        .encode()
        .expect("valid relation names encode")
    };
    let request = IpcFrame {
        operation: Operation::ResolveRelation as u16,
        request_id: 41,
        payload: encode_request("app", "metrics"),
    };
    assert_eq!(
        request.payload,
        [
            0, 1, 0, 3, 0, 7, b'a', b'p', b'p', b'm', b'e', b't', b'r', b'i', b'c', b's'
        ]
    );
    let response =
        dispatch_executor_resolve_relation(ServiceRole::Executor, &mut connection, &request)?;
    assert_eq!(response.operation, request.operation);
    assert_eq!(response.request_id, request.request_id);
    let ResolveRelationResponse::Resolved {
        handle,
        description,
    } = ResolveRelationResponse::decode(&response.payload)?
    else {
        panic!("visible relation must resolve")
    };
    assert_eq!(description.relation_id, [0x10; 16]);
    assert_eq!(description.name, "metrics");
    assert_eq!(description.columns[0].name, "value");
    assert_eq!(
        connection.revalidate_relation_handle(&handle)?.relation_id,
        [0x10; 16]
    );
    let mut forged_handle_bytes = handle.to_bytes();
    forged_handle_bytes[1] ^= 1;
    let forged_handle =
        prdb_engine::metadata_discovery::ResolvedRelationHandle::from_bytes(&forged_handle_bytes)
            .expect("forged token retains the valid wire shape");
    let forged_open_scan = IpcFrame {
        operation: Operation::OpenScan as u16,
        request_id: 42,
        payload: OpenScanRequest {
            relation: forged_handle,
            column_ids: vec![[0x66; 16]],
        }
        .encode()?,
    };
    assert!(matches!(
        dispatch_executor_request(ServiceRole::Executor, &mut connection, &forged_open_scan),
        Err(ExecutorRequestError::Transaction(
            prdb_engine::transaction::TransactionError::InvalidMetadataRequest
        ))
    ));

    connection.close_relation_handle(&handle)?;
    let closed_handle_scan = IpcFrame {
        operation: Operation::OpenScan as u16,
        request_id: 43,
        payload: OpenScanRequest {
            relation: handle.clone(),
            column_ids: vec![[0x66; 16]],
        }
        .encode()?,
    };
    assert!(matches!(
        dispatch_executor_request(ServiceRole::Executor, &mut connection, &closed_handle_scan),
        Err(ExecutorRequestError::Transaction(
            prdb_engine::transaction::TransactionError::InvalidMetadataRequest
        ))
    ));
    let closed_handle_insert = IpcFrame {
        operation: Operation::StageInsert as u16,
        request_id: 45,
        payload: StageInsertRequest {
            relation: handle.clone(),
            values: vec![InsertInput::Value(Some(ScalarValue::BigInt(1)))],
        }
        .encode()?,
    };
    assert!(matches!(
        dispatch_executor_request(
            ServiceRole::Executor,
            &mut connection,
            &closed_handle_insert
        ),
        Err(ExecutorRequestError::Transaction(
            prdb_engine::transaction::TransactionError::InvalidMetadataRequest
        ))
    ));
    let reopened = dispatch_executor_resolve_relation(
        ServiceRole::Executor,
        &mut connection,
        &IpcFrame {
            operation: Operation::ResolveRelation as u16,
            request_id: 44,
            payload: encode_request("app", "metrics"),
        },
    )?;
    let ResolveRelationResponse::Resolved {
        handle: reopened_handle,
        ..
    } = ResolveRelationResponse::decode(&reopened.payload)?
    else {
        panic!("visible relation must resolve after handle close")
    };
    assert_ne!(
        reopened_handle, handle,
        "reopening issues a fresh capability"
    );
    let mut stale_after_reopen = closed_handle_scan;
    stale_after_reopen.request_id = 45;
    assert!(matches!(
        dispatch_executor_request(ServiceRole::Executor, &mut connection, &stale_after_reopen),
        Err(ExecutorRequestError::Transaction(
            prdb_engine::transaction::TransactionError::InvalidMetadataRequest
        ))
    ));

    let hidden_request = IpcFrame {
        payload: encode_request("private", "metrics"),
        ..request.clone()
    };
    let hidden_response = dispatch_executor_resolve_relation(
        ServiceRole::Executor,
        &mut connection,
        &hidden_request,
    )?;
    assert_eq!(
        ResolveRelationResponse::decode(&hidden_response.payload)?,
        ResolveRelationResponse::Unavailable
    );
    let hidden_table_request = IpcFrame {
        payload: encode_request("app", "secret"),
        ..request.clone()
    };
    let absent_table_request = IpcFrame {
        payload: encode_request("app", "missing"),
        ..request.clone()
    };
    let mut restricted_invocation =
        InvocationTransaction::begin(&database, [218; 16], [10; 16], 9_999, 1, 64)?;
    let mut restricted_connection = restricted_invocation.open_executor_metadata_connection();
    let hidden_table_response = dispatch_executor_resolve_relation(
        ServiceRole::Executor,
        &mut restricted_connection,
        &hidden_table_request,
    )?;
    let absent_table_response = dispatch_executor_resolve_relation(
        ServiceRole::Executor,
        &mut restricted_connection,
        &absent_table_request,
    )?;
    assert_eq!(
        ResolveRelationResponse::decode(&hidden_table_response.payload)?,
        ResolveRelationResponse::Unavailable
    );
    assert_eq!(
        hidden_table_response.payload, absent_table_response.payload,
        "a hidden relation and an absent name must have identical executor IPC diagnostics"
    );

    assert!(matches!(
        dispatch_executor_resolve_relation(ServiceRole::Planner, &mut connection, &request),
        Err(ExecutorRequestError::Dispatch(_))
    ));
    let malformed = IpcFrame {
        payload: vec![0, 1, 0],
        ..request
    };
    assert!(matches!(
        dispatch_executor_resolve_relation(ServiceRole::Executor, &mut connection, &malformed),
        Err(ExecutorRequestError::InvalidPayload)
    ));
    drop(connection);
    let mut replay_connection = invocation.open_executor_metadata_connection();
    let cross_connection_replay = IpcFrame {
        operation: Operation::OpenScan as u16,
        request_id: 46,
        payload: OpenScanRequest {
            relation: reopened_handle.clone(),
            column_ids: vec![[0x66; 16]],
        }
        .encode()?,
    };
    assert!(matches!(
        dispatch_executor_request(
            ServiceRole::Executor,
            &mut replay_connection,
            &cross_connection_replay,
        ),
        Err(ExecutorRequestError::Transaction(
            prdb_engine::transaction::TransactionError::InvalidMetadataRequest
        ))
    ));
    let mut other_principal_invocation =
        InvocationTransaction::begin(&database, [0x9f; 16], [10; 16], 9_999, 1, 64)?;
    let mut other_principal_connection =
        other_principal_invocation.open_executor_metadata_connection();
    let cross_principal_replay = IpcFrame {
        operation: Operation::OpenScan as u16,
        request_id: 47,
        payload: OpenScanRequest {
            relation: reopened_handle,
            column_ids: vec![[0x66; 16]],
        }
        .encode()?,
    };
    assert!(matches!(
        dispatch_executor_request(
            ServiceRole::Executor,
            &mut other_principal_connection,
            &cross_principal_replay,
        ),
        Err(ExecutorRequestError::Transaction(
            prdb_engine::transaction::TransactionError::InvalidMetadataRequest
        ))
    ));
    Ok(())
}

pub(super) fn executor_relation_socket_loop_ends_session_without_finalizing()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::executor_ipc::{
        CloseCursorRequest, ExecutorConnectionError, OpenScanRequest, OpenScanResponse,
        ResolveRelationRequest, ResolveRelationResponse, serve_executor,
    };
    use prdb_engine::ipc::{IpcFrame, Operation, read_frame, write_frame};
    use prdb_engine::outcome::{OutcomeStatus, lookup_outcome};
    use std::net::Shutdown;
    use std::os::unix::net::UnixStream;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let invocation_id = [218; 16];
    let principal_id = [9; 16];
    let mut invocation =
        InvocationTransaction::begin(&database, invocation_id, principal_id, 9_999, 1, 64)?;
    let (mut client, mut server) = UnixStream::pair()?;
    let client_task = std::thread::spawn(move || {
        let resolve_request = IpcFrame {
            operation: Operation::ResolveRelation as u16,
            request_id: 51,
            payload: ResolveRelationRequest {
                schema_name: "app".to_owned(),
                relation_name: "metrics".to_owned(),
            }
            .encode()
            .unwrap(),
        };
        write_frame(&mut client, &resolve_request).unwrap();
        let resolve_response = read_frame(&mut client).unwrap().unwrap();
        let ResolveRelationResponse::Resolved { handle, .. } =
            ResolveRelationResponse::decode(&resolve_response.payload).unwrap()
        else {
            panic!("visible relation must resolve over the stream")
        };

        let open_request = IpcFrame {
            operation: Operation::OpenScan as u16,
            request_id: 52,
            payload: OpenScanRequest {
                relation: handle,
                column_ids: Vec::new(),
            }
            .encode()
            .unwrap(),
        };
        write_frame(&mut client, &open_request).unwrap();
        let open_response = read_frame(&mut client).unwrap().unwrap();
        let cursor = OpenScanResponse::decode(&open_response.payload)
            .unwrap()
            .cursor;

        let close_request = IpcFrame {
            operation: Operation::CloseCursor as u16,
            request_id: 53,
            payload: CloseCursorRequest { cursor }.encode(),
        };
        write_frame(&mut client, &close_request).unwrap();
        let close_response = read_frame(&mut client).unwrap().unwrap();
        assert_eq!(close_response.payload, [0, 1, 0]);
        client.shutdown(Shutdown::Write).unwrap();
    });
    serve_executor(&mut server, &mut invocation)?;
    client_task
        .join()
        .expect("client stream sequence completes");
    assert!(lookup_outcome(&database, invocation_id, principal_id, 1)?.is_none());
    invocation.abort()?;
    let outcome = lookup_outcome(&database, invocation_id, principal_id, 1)?
        .expect("the supervisor's abort persists an outcome");
    assert_eq!(outcome.status, OutcomeStatus::Aborted);

    let duplicate_invocation_id = [219; 16];
    let mut duplicate_invocation = InvocationTransaction::begin(
        &database,
        duplicate_invocation_id,
        principal_id,
        9_999,
        1,
        64,
    )?;
    let (mut duplicate_client, mut duplicate_server) = UnixStream::pair()?;
    let duplicate_request = IpcFrame {
        request_id: 52,
        operation: Operation::ResolveRelation as u16,
        payload: ResolveRelationRequest {
            schema_name: "app".to_owned(),
            relation_name: "metrics".to_owned(),
        }
        .encode()?,
    };
    write_frame(&mut duplicate_client, &duplicate_request)?;
    write_frame(&mut duplicate_client, &duplicate_request)?;
    duplicate_client.shutdown(Shutdown::Write)?;
    assert!(matches!(
        serve_executor(&mut duplicate_server, &mut duplicate_invocation),
        Err(ExecutorConnectionError::DuplicateRequestId)
    ));
    assert!(read_frame(&mut duplicate_client)?.is_some());
    let duplicate_outcome = lookup_outcome(&database, duplicate_invocation_id, principal_id, 1)?
        .expect("duplicate request persists an aborted outcome");
    assert_eq!(duplicate_outcome.status, OutcomeStatus::Aborted);
    Ok(())
}

pub(super) fn information_schema_virtual_scans_round_trip_through_executor_ipc()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::executor_ipc::{
        NextBatchRequest, NextBatchResponse, OpenScanRequest, OpenScanResponse,
        ResolveRelationRequest, ResolveRelationResponse, serve_executor,
    };
    use prdb_engine::ipc::{IpcFrame, Operation, read_frame, write_frame};
    use std::net::Shutdown;
    use std::os::unix::net::UnixStream;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    install_second_relation_column(&database)?;

    let mut invocation = InvocationTransaction::begin(&database, [222; 16], [9; 16], 9_999, 1, 64)?;
    let (mut client, mut server) = UnixStream::pair()?;
    let client_task = std::thread::spawn(move || {
        let mut request_id = 1u64;
        macro_rules! exchange {
            ($operation:expr, $payload:expr) => {{
                let request = IpcFrame {
                    operation: $operation as u16,
                    request_id,
                    payload: $payload,
                };
                request_id += 1;
                write_frame(&mut client, &request).unwrap();
                read_frame(&mut client).unwrap().unwrap()
            }};
        }

        let mut collected = Vec::new();
        for relation_name in ["schemata", "tables", "columns"] {
            let response = exchange!(
                Operation::ResolveRelation,
                ResolveRelationRequest {
                    schema_name: "information_schema".to_owned(),
                    relation_name: relation_name.to_owned(),
                }
                .encode()
                .unwrap()
            );
            let ResolveRelationResponse::Resolved {
                handle,
                description,
            } = ResolveRelationResponse::decode(&response.payload).unwrap()
            else {
                panic!("public information_schema relation resolves")
            };
            assert_eq!(description.name, relation_name);

            let response = exchange!(
                Operation::OpenScan,
                OpenScanRequest {
                    relation: handle,
                    column_ids: Vec::new(),
                }
                .encode()
                .unwrap()
            );
            let mut cursor = OpenScanResponse::decode(&response.payload).unwrap().cursor;
            let mut relation_rows = Vec::new();
            loop {
                let response = exchange!(
                    Operation::NextBatch,
                    NextBatchRequest {
                        cursor: cursor.clone(),
                        max_scanned: 1,
                        max_bytes: 4096,
                    }
                    .encode()
                    .unwrap()
                );
                let page = NextBatchResponse::decode(&response.payload).unwrap();
                relation_rows.extend(page.rows.into_iter().map(|row| row.cells));
                match page.continuation {
                    Some(next) => cursor = next,
                    None => break,
                }
            }
            collected.push((relation_name, relation_rows));
        }
        client.shutdown(Shutdown::Write).unwrap();
        collected
    });

    serve_executor(&mut server, &mut invocation)?;
    let collected = client_task
        .join()
        .expect("executor IPC scan sequence completes");

    let schemata = &collected[0].1;
    assert_eq!(
        schemata,
        &[
            vec![SanitizedCell::Value(ScalarValue::Text(
                "information_schema".to_owned(),
            ))],
            vec![SanitizedCell::Value(ScalarValue::Text("app".to_owned()))],
            vec![SanitizedCell::Value(ScalarValue::Text("public".to_owned()))],
        ]
    );
    let tables = &collected[1].1;
    assert_eq!(tables.len(), 5);
    assert_eq!(
        tables[0],
        [
            SanitizedCell::Value(ScalarValue::Text("information_schema".to_owned())),
            SanitizedCell::Value(ScalarValue::Text("columns".to_owned())),
            SanitizedCell::Value(ScalarValue::Text("VIEW".to_owned())),
        ]
    );
    assert_eq!(
        tables[3],
        [
            SanitizedCell::Value(ScalarValue::Text("app".to_owned())),
            SanitizedCell::Value(ScalarValue::Text("metrics".to_owned())),
            SanitizedCell::Value(ScalarValue::Text("BASE TABLE".to_owned())),
        ]
    );

    let columns = &collected[2].1;
    assert_eq!(columns.len(), 16);
    assert_eq!(
        columns[0],
        [
            SanitizedCell::Value(ScalarValue::Text("information_schema".to_owned())),
            SanitizedCell::Value(ScalarValue::Text("columns".to_owned())),
            SanitizedCell::Value(ScalarValue::Text("table_schema".to_owned())),
            SanitizedCell::Value(ScalarValue::Integer(1)),
            SanitizedCell::Value(ScalarValue::Text("TEXT".to_owned())),
            SanitizedCell::Value(ScalarValue::Text("NO".to_owned())),
            SanitizedCell::Null,
            SanitizedCell::Null,
            SanitizedCell::Null,
        ]
    );
    assert_eq!(
        columns[14][..4],
        [
            SanitizedCell::Value(ScalarValue::Text("app".to_owned())),
            SanitizedCell::Value(ScalarValue::Text("metrics".to_owned())),
            SanitizedCell::Value(ScalarValue::Text("value2".to_owned())),
            SanitizedCell::Value(ScalarValue::Integer(2)),
        ]
    );
    invocation.abort()?;
    Ok(())
}

pub(super) fn catalog_authorized_scan_builds_its_projection_from_pinned_records()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::executor_ipc::{
        CloseCursorRequest, ExecutorRequestError, NextBatchRequest, NextBatchResponse,
        OpenScanRequest, OpenScanResponse, StageDeleteRequest, StageInsertRequest,
        StageInsertResponse, StageUpdateRequest, dispatch_executor_request,
    };
    use prdb_engine::ipc::{IpcFrame, Operation as IpcOperation, ServiceRole};

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;

    let principal_id = [9; 16];
    let restricted_principal_id = [10; 16];
    let table_id = [0x10; 16];
    let boolean_type_id = [46; 16];
    let policy_id = [221; 16];
    let expression_id = [222; 16];
    let policy_key = catalog_id_key(13, policy_id);
    let cell_policy_id = [226; 16];
    let cell_policy_key = catalog_id_key(13, cell_policy_id);
    let expression_key = catalog_id_key(7, expression_id);
    let (target_key, target_payload) = policy_target_catalog_record(policy_id, 1, principal_id);
    let (cell_principal_target_key, cell_principal_target_payload) =
        policy_target_catalog_record(cell_policy_id, 1, principal_id);
    let (cell_column_target_key, cell_column_target_payload) =
        policy_target_catalog_record(cell_policy_id, 3, [0x66; 16]);
    let (type_object_key, type_object_payload) = object_catalog_record_for(boolean_type_id, 2);
    let type_write = boolean_type_catalog_write(boolean_type_id)?;
    let (table_select_key, table_select_payload) = grant_catalog_record(
        principal_id,
        table_id,
        2,
        1,
        table_id,
        builtin_owner_id(),
        1,
    );
    let (table_delete_key, table_delete_payload) = grant_catalog_record(
        principal_id,
        table_id,
        5,
        1,
        table_id,
        builtin_owner_id(),
        1,
    );
    let delete_policy_id = [228; 16];
    let delete_policy_key = catalog_id_key(13, delete_policy_id);
    let (delete_policy_target_key, delete_policy_target_payload) =
        policy_target_catalog_record(delete_policy_id, 1, principal_id);
    let update_policy_id = [230; 16];
    let update_policy_key = catalog_id_key(13, update_policy_id);
    let (update_policy_target_key, update_policy_target_payload) =
        policy_target_catalog_record(update_policy_id, 1, principal_id);
    let cell_update_policy_id = [231; 16];
    let cell_update_policy_key = catalog_id_key(13, cell_update_policy_id);
    let (cell_update_principal_target_key, cell_update_principal_target_payload) =
        policy_target_catalog_record(cell_update_policy_id, 1, principal_id);
    let (cell_update_column_target_key, cell_update_column_target_payload) =
        policy_target_catalog_record(cell_update_policy_id, 3, [0x66; 16]);
    let update_check_expression_id = [232; 16];
    let update_check_expression_key = catalog_id_key(7, update_check_expression_id);
    let insert_policy_id = [233; 16];
    let (insert_policy_key, insert_policy_payload) = policy_catalog_record(
        insert_policy_id,
        table_id,
        1,
        2,
        None,
        Some(update_check_expression_id),
    );
    let (insert_policy_target_key, insert_policy_target_payload) =
        policy_target_catalog_record(insert_policy_id, 1, principal_id);
    let cell_insert_policy_id = [234; 16];
    let (cell_insert_policy_key, cell_insert_policy_payload) = policy_catalog_record(
        cell_insert_policy_id,
        table_id,
        2,
        2,
        None,
        Some(update_check_expression_id),
    );
    let (cell_insert_principal_target_key, cell_insert_principal_target_payload) =
        policy_target_catalog_record(cell_insert_policy_id, 1, principal_id);
    let (cell_insert_column_target_key, cell_insert_column_target_payload) =
        policy_target_catalog_record(cell_insert_policy_id, 3, [0x66; 16]);
    let (restricted_schema_grant_key, restricted_schema_grant_payload) = grant_catalog_record(
        restricted_principal_id,
        [0x91; 16],
        1,
        1,
        [0x91; 16],
        builtin_owner_id(),
        1,
    );
    let (restricted_table_grant_key, restricted_table_grant_payload) = grant_catalog_record(
        restricted_principal_id,
        table_id,
        2,
        1,
        table_id,
        builtin_owner_id(),
        1,
    );
    let (restricted_column_grant_key, restricted_column_grant_payload) = grant_catalog_record(
        restricted_principal_id,
        table_id,
        2,
        2,
        [0x66; 16],
        builtin_owner_id(),
        1,
    );
    let restricted_row_policy_id = [235; 16];
    let (restricted_row_policy_key, restricted_row_policy_payload) = policy_catalog_record(
        restricted_row_policy_id,
        table_id,
        1,
        1,
        Some(expression_id),
        None,
    );
    let (restricted_row_target_key, restricted_row_target_payload) =
        policy_target_catalog_record(restricted_row_policy_id, 1, restricted_principal_id);
    let write = database.begin_write()?;
    {
        let mut catalog = write.open_table(CATALOG)?;
        let mut grants = write.open_table(prdb_engine::schema::GRANTS)?;
        for (key, payload) in [
            (
                type_write.key.clone(),
                type_write.payload.clone().expect("type definition payload"),
            ),
            (type_object_key, type_object_payload),
            (
                expression_key.clone(),
                expression_catalog_payload_full(
                    expression_id,
                    3,
                    boolean_type_id,
                    [0; 8],
                    &[],
                    &Expr::Literal(Some(ScalarValue::Boolean(true))),
                )?,
            ),
            (
                update_check_expression_key.clone(),
                expression_catalog_payload_full(
                    update_check_expression_id,
                    4,
                    boolean_type_id,
                    [0; 8],
                    &[],
                    &Expr::Literal(Some(ScalarValue::Boolean(true))),
                )?,
            ),
            (
                policy_key.clone(),
                catalog_record_payload(
                    13,
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
                            payload: 1u16.to_le_bytes().to_vec(),
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
                            payload: 1u64.to_le_bytes().to_vec(),
                        },
                    ],
                )?,
            ),
            (
                cell_policy_key.clone(),
                catalog_record_payload(
                    13,
                    &cell_policy_key,
                    &[
                        CatalogField {
                            tag: 1,
                            wire_type: WireType::Id,
                            payload: table_id.to_vec(),
                        },
                        CatalogField {
                            tag: 2,
                            wire_type: WireType::U16,
                            payload: 2u16.to_le_bytes().to_vec(),
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
                            payload: 1u64.to_le_bytes().to_vec(),
                        },
                    ],
                )?,
            ),
            (
                delete_policy_key.clone(),
                catalog_record_payload(
                    13,
                    &delete_policy_key,
                    &[
                        CatalogField {
                            tag: 1,
                            wire_type: WireType::Id,
                            payload: table_id.to_vec(),
                        },
                        CatalogField {
                            tag: 2,
                            wire_type: WireType::U16,
                            payload: 1u16.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 3,
                            wire_type: WireType::U16,
                            payload: 4u16.to_le_bytes().to_vec(),
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
                            payload: 1u64.to_le_bytes().to_vec(),
                        },
                    ],
                )?,
            ),
            (
                update_policy_key.clone(),
                catalog_record_payload(
                    13,
                    &update_policy_key,
                    &[
                        CatalogField {
                            tag: 1,
                            wire_type: WireType::Id,
                            payload: table_id.to_vec(),
                        },
                        CatalogField {
                            tag: 2,
                            wire_type: WireType::U16,
                            payload: 1u16.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 3,
                            wire_type: WireType::U16,
                            payload: 3u16.to_le_bytes().to_vec(),
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
                            tag: 6,
                            wire_type: WireType::Id,
                            payload: update_check_expression_id.to_vec(),
                        },
                        CatalogField {
                            tag: 7,
                            wire_type: WireType::U64,
                            payload: 1u64.to_le_bytes().to_vec(),
                        },
                    ],
                )?,
            ),
            (
                cell_update_policy_key.clone(),
                catalog_record_payload(
                    13,
                    &cell_update_policy_key,
                    &[
                        CatalogField {
                            tag: 1,
                            wire_type: WireType::Id,
                            payload: table_id.to_vec(),
                        },
                        CatalogField {
                            tag: 2,
                            wire_type: WireType::U16,
                            payload: 2u16.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 3,
                            wire_type: WireType::U16,
                            payload: 3u16.to_le_bytes().to_vec(),
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
                            tag: 6,
                            wire_type: WireType::Id,
                            payload: update_check_expression_id.to_vec(),
                        },
                        CatalogField {
                            tag: 7,
                            wire_type: WireType::U64,
                            payload: 1u64.to_le_bytes().to_vec(),
                        },
                    ],
                )?,
            ),
            (target_key, target_payload),
            (insert_policy_key, insert_policy_payload),
            (cell_insert_policy_key, cell_insert_policy_payload),
            (cell_principal_target_key, cell_principal_target_payload),
            (cell_column_target_key, cell_column_target_payload),
            (delete_policy_target_key, delete_policy_target_payload),
            (insert_policy_target_key, insert_policy_target_payload),
            (
                cell_insert_principal_target_key,
                cell_insert_principal_target_payload,
            ),
            (
                cell_insert_column_target_key,
                cell_insert_column_target_payload,
            ),
            (restricted_schema_grant_key, restricted_schema_grant_payload),
            (restricted_table_grant_key, restricted_table_grant_payload),
            (restricted_column_grant_key, restricted_column_grant_payload),
            (restricted_row_policy_key, restricted_row_policy_payload),
            (restricted_row_target_key, restricted_row_target_payload),
            (update_policy_target_key, update_policy_target_payload),
            (
                cell_update_principal_target_key,
                cell_update_principal_target_payload,
            ),
            (
                cell_update_column_target_key,
                cell_update_column_target_payload,
            ),
            (table_select_key, table_select_payload),
            (table_delete_key, table_delete_payload),
            grant_catalog_record(
                principal_id,
                table_id,
                3,
                1,
                table_id,
                builtin_owner_id(),
                1,
            ),
            grant_catalog_record(
                principal_id,
                table_id,
                3,
                2,
                [0x66; 16],
                builtin_owner_id(),
                1,
            ),
            grant_catalog_record(
                principal_id,
                table_id,
                4,
                1,
                table_id,
                builtin_owner_id(),
                1,
            ),
            grant_catalog_record(
                principal_id,
                table_id,
                4,
                2,
                [0x66; 16],
                builtin_owner_id(),
                1,
            ),
        ] {
            catalog.insert(&key[..], &payload[..])?;
            if key.starts_with(&12u16.to_be_bytes()) {
                let grantee: [u8; 16] = key[3..19].try_into()?;
                let object: [u8; 16] = key[20..36].try_into()?;
                for (direction, indexed_id) in [(1u8, grantee), (2u8, object)] {
                    let mut index_key = Vec::with_capacity(76);
                    index_key.push(direction);
                    index_key.extend_from_slice(&indexed_id);
                    index_key.extend_from_slice(&key);
                    grants.insert(&index_key[..], &[][..])?;
                }
            }
        }
    }
    write.commit()?;

    let mut seed =
        InvocationTransaction::begin(&database, [224; 16], principal_id, 9_999, 2, 16_384)?;
    let mut seed_connection = seed.open_executor_metadata_connection();
    let (seed_relation, _) = seed_connection
        .resolve_relation("app", "metrics")?
        .expect("insert grants keep the relation discoverable");
    let seed_first = seed_connection.stage_insert(
        &seed_relation,
        &[InsertInput::Value(Some(ScalarValue::Bytea(vec![1, 2, 3])))],
    )?;
    let seed_first_id = seed_connection.revalidate_row_handle(&seed_first)?;
    let seed_second = seed_connection.stage_insert(
        &seed_relation,
        &[InsertInput::Value(Some(ScalarValue::Bytea(vec![4, 5, 6])))],
    )?;
    let seed_second_id = seed_connection.revalidate_row_handle(&seed_second)?;
    drop(seed_connection);
    seed.commit()?;
    let (row_id, first_value, second_row_id, second_value) =
        if seed_first_id.row < seed_second_id.row {
            (seed_first_id, vec![1, 2, 3], seed_second_id, vec![4, 5, 6])
        } else {
            (seed_second_id, vec![4, 5, 6], seed_first_id, vec![1, 2, 3])
        };

    let mut invocation =
        InvocationTransaction::begin(&database, [225; 16], principal_id, 9_999, 4, 16_384)?;
    let point_read = invocation
        .read_catalog_authorized_row(table_id, row_id.row)?
        .expect("catalog grants authorize the seeded point read");
    assert_eq!(
        point_read,
        vec![SanitizedCell::Value(ScalarValue::Bytea(
            first_value.clone()
        ))]
    );
    assert!(
        invocation
            .read_catalog_authorized_row(table_id, [0x99; 16])?
            .is_none()
    );
    for (invocation_id, requested_table_id, requested_row_id) in [
        ([226; 16], [0; 16], [0xd3; 16]),
        ([227; 16], table_id, [0; 16]),
    ] {
        let mut invalid =
            InvocationTransaction::begin(&database, invocation_id, principal_id, 9_999, 1, 1024)?;
        assert!(matches!(
            invalid.read_catalog_authorized_row(requested_table_id, requested_row_id),
            Err(TransactionError::InvalidContext)
        ));
    }
    let page = invocation.scan_catalog_authorized_rows(table_id, None, 3)?;
    assert_eq!(page.rows.len(), 2);
    assert_eq!(page.rows[0].0, row_id);
    assert_eq!(page.rows[1].0, second_row_id);
    assert!(matches!(page.rows[0].1[0], SanitizedCell::Value(_)));
    assert!(page.continuation.is_none());

    let mut connection = invocation.open_executor_metadata_connection();
    let (relation_handle, _) = connection
        .resolve_relation("app", "metrics")?
        .expect("catalog grants authorize relation resolution");
    let open_request = IpcFrame {
        operation: IpcOperation::OpenScan as u16,
        request_id: 61,
        payload: OpenScanRequest {
            relation: relation_handle.clone(),
            column_ids: vec![[0x66; 16]],
        }
        .encode()?,
    };
    let open_response =
        dispatch_executor_request(ServiceRole::Executor, &mut connection, &open_request)?;
    let scan_cursor = OpenScanResponse::decode(&open_response.payload)?.cursor;
    assert_eq!(scan_cursor.to_bytes()[0], 7);
    assert!(
        prdb_engine::metadata_discovery::SchemaScanCursor::from_bytes(&scan_cursor.to_bytes())
            .is_none()
    );

    let first_request = IpcFrame {
        operation: IpcOperation::NextBatch as u16,
        request_id: 62,
        payload: NextBatchRequest {
            cursor: scan_cursor.clone(),
            max_scanned: 2,
            max_bytes: 48,
        }
        .encode()?,
    };
    let first_response =
        dispatch_executor_request(ServiceRole::Executor, &mut connection, &first_request)?;
    let first_page = NextBatchResponse::decode(&first_response.payload)?;
    assert_eq!(first_page.rows.len(), 1);
    assert_eq!(
        first_page.rows[0].cells,
        [SanitizedCell::Value(ScalarValue::Bytea(
            first_value.clone()
        ))]
    );
    assert_eq!(first_page.rows[0].handle.to_bytes()[0], 8);
    assert_eq!(
        connection.revalidate_row_handle(&first_page.rows[0].handle)?,
        row_id
    );
    let mut replay_invocation =
        InvocationTransaction::begin(&database, [0x9f; 16], restricted_principal_id, 9_999, 1, 64)?;
    let mut replay_connection = replay_invocation.open_executor_metadata_connection();
    let mut replay_target_row_key = Vec::with_capacity(32);
    replay_target_row_key.extend_from_slice(&table_id);
    replay_target_row_key.extend_from_slice(&row_id.row);
    let replay_target_before = database
        .begin_read()?
        .open_table(ROWS)?
        .get(replay_target_row_key.as_slice())?
        .expect("seeded row is present before cross-principal IPC")
        .value()
        .to_vec();
    let cross_principal_row_replay = IpcFrame {
        operation: IpcOperation::StageDelete as u16,
        request_id: 73,
        payload: StageDeleteRequest {
            handle: first_page.rows[0].handle.clone(),
        }
        .encode(),
    };
    assert!(matches!(
        dispatch_executor_request(
            ServiceRole::Executor,
            &mut replay_connection,
            &cross_principal_row_replay,
        ),
        Err(ExecutorRequestError::Transaction(
            TransactionError::InvalidMetadataRequest
        ))
    ));
    let cross_principal_row_update_replay = IpcFrame {
        operation: IpcOperation::StageUpdate as u16,
        request_id: 74,
        payload: StageUpdateRequest {
            handle: first_page.rows[0].handle.clone(),
            patches: vec![FieldPatch {
                ordinal: 0,
                value: Some(ScalarValue::Bytea(
                    b"forged cross-principal update".to_vec(),
                )),
            }],
        }
        .encode()?,
    };
    assert!(matches!(
        dispatch_executor_request(
            ServiceRole::Executor,
            &mut replay_connection,
            &cross_principal_row_update_replay,
        ),
        Err(ExecutorRequestError::Transaction(
            TransactionError::InvalidMetadataRequest
        ))
    ));
    drop(replay_connection);
    replay_invocation.abort()?;

    let mut same_principal_replay_invocation =
        InvocationTransaction::begin(&database, [0xa1; 16], principal_id, 9_999, 1, 64)?;
    let mut same_principal_replay_connection =
        same_principal_replay_invocation.open_executor_metadata_connection();
    let same_principal_row_replay = IpcFrame {
        operation: IpcOperation::StageDelete as u16,
        request_id: 75,
        payload: StageDeleteRequest {
            handle: first_page.rows[0].handle.clone(),
        }
        .encode(),
    };
    assert!(matches!(
        dispatch_executor_request(
            ServiceRole::Executor,
            &mut same_principal_replay_connection,
            &same_principal_row_replay,
        ),
        Err(ExecutorRequestError::Transaction(
            TransactionError::InvalidMetadataRequest
        ))
    ));
    let same_principal_row_update_replay = IpcFrame {
        operation: IpcOperation::StageUpdate as u16,
        request_id: 76,
        payload: StageUpdateRequest {
            handle: first_page.rows[0].handle.clone(),
            patches: vec![FieldPatch {
                ordinal: 0,
                value: Some(ScalarValue::Bytea(
                    b"forged cross-invocation update".to_vec(),
                )),
            }],
        }
        .encode()?,
    };
    assert!(matches!(
        dispatch_executor_request(
            ServiceRole::Executor,
            &mut same_principal_replay_connection,
            &same_principal_row_update_replay,
        ),
        Err(ExecutorRequestError::Transaction(
            TransactionError::InvalidMetadataRequest
        ))
    ));
    drop(same_principal_replay_connection);
    same_principal_replay_invocation.abort()?;
    assert_eq!(
        database
            .begin_read()?
            .open_table(ROWS)?
            .get(replay_target_row_key.as_slice())?
            .expect("forged IPC requests leave the seeded row present")
            .value(),
        replay_target_before,
        "cross-principal and cross-invocation DELETE and UPDATE requests must not change the stored row"
    );
    let next_cursor = first_page
        .continuation
        .expect("full page returns a fresh continuation");
    assert!(matches!(
        connection.next_relation_scan_page(&scan_cursor, 1),
        Err(TransactionError::InvalidMetadataRequest)
    ));

    let second_request = IpcFrame {
        request_id: 63,
        payload: NextBatchRequest {
            cursor: next_cursor.clone(),
            max_scanned: 1,
            max_bytes: 256,
        }
        .encode()?,
        ..first_request.clone()
    };
    let second_response =
        dispatch_executor_request(ServiceRole::Executor, &mut connection, &second_request)?;
    let second_page = NextBatchResponse::decode(&second_response.payload)?;
    assert_eq!(second_page.rows.len(), 1);
    assert_eq!(
        second_page.rows[0].cells,
        [SanitizedCell::Value(ScalarValue::Bytea(second_value))]
    );
    let final_cursor = second_page
        .continuation
        .expect("full final page requires an EOF confirmation");
    let final_request = IpcFrame {
        request_id: 64,
        payload: NextBatchRequest {
            cursor: final_cursor.clone(),
            max_scanned: 1,
            max_bytes: 256,
        }
        .encode()?,
        ..first_request.clone()
    };
    let final_response =
        dispatch_executor_request(ServiceRole::Executor, &mut connection, &final_request)?;
    let final_page = NextBatchResponse::decode(&final_response.payload)?;
    assert!(final_page.rows.is_empty());
    assert!(final_page.continuation.is_none());
    assert!(matches!(
        connection.next_relation_scan_page(&final_cursor, 1),
        Err(TransactionError::InvalidMetadataRequest)
    ));

    let second_open_request = IpcFrame {
        request_id: 65,
        ..open_request
    };
    let second_open_response =
        dispatch_executor_request(ServiceRole::Executor, &mut connection, &second_open_request)?;
    let close_cursor = OpenScanResponse::decode(&second_open_response.payload)?.cursor;
    let alias_page_response = dispatch_executor_request(
        ServiceRole::Executor,
        &mut connection,
        &IpcFrame {
            operation: IpcOperation::NextBatch as u16,
            request_id: 70,
            payload: NextBatchRequest {
                cursor: close_cursor.clone(),
                max_scanned: 2,
                max_bytes: 256,
            }
            .encode()?,
        },
    )?;
    let alias_page = NextBatchResponse::decode(&alias_page_response.payload)?;
    assert_eq!(alias_page.rows.len(), 2);
    let update_alias = alias_page
        .rows
        .iter()
        .find(|row| connection.revalidate_row_handle(&row.handle).ok() == Some(row_id))
        .expect("second scan issues an alias handle for the updated row")
        .handle
        .clone();
    let delete_alias = alias_page
        .rows
        .iter()
        .find(|row| connection.revalidate_row_handle(&row.handle).ok() == Some(second_row_id))
        .expect("second scan issues an alias handle for the deleted row")
        .handle
        .clone();
    assert_ne!(update_alias, first_page.rows[0].handle);
    assert_ne!(delete_alias, second_page.rows[0].handle);
    let alias_cursor = alias_page
        .continuation
        .expect("full alias page requires an EOF confirmation");
    let close_response = dispatch_executor_request(
        ServiceRole::Executor,
        &mut connection,
        &IpcFrame {
            operation: IpcOperation::CloseCursor as u16,
            request_id: 66,
            payload: CloseCursorRequest {
                cursor: alias_cursor.clone(),
            }
            .encode(),
        },
    )?;
    assert_eq!(close_response.payload, [0, 1, 0]);
    assert!(matches!(
        connection.next_relation_scan_page(&alias_cursor, 1),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    let insert_response = dispatch_executor_request(
        ServiceRole::Executor,
        &mut connection,
        &IpcFrame {
            operation: IpcOperation::StageInsert as u16,
            request_id: 67,
            payload: StageInsertRequest {
                relation: relation_handle.clone(),
                values: vec![InsertInput::Value(Some(ScalarValue::Bytea(vec![4, 5])))],
            }
            .encode()?,
        },
    )?;
    let inserted_handle = StageInsertResponse::decode(&insert_response.payload)?.handle;
    let inserted_row_id = connection.revalidate_row_handle(&inserted_handle)?;
    assert_eq!(inserted_row_id.table, table_id);
    let updated_row_handle = first_page.rows[0].handle.clone();
    let update_response = dispatch_executor_request(
        ServiceRole::Executor,
        &mut connection,
        &IpcFrame {
            operation: IpcOperation::StageUpdate as u16,
            request_id: 68,
            payload: StageUpdateRequest {
                handle: updated_row_handle.clone(),
                patches: vec![FieldPatch {
                    ordinal: 0,
                    value: Some(ScalarValue::Bytea(vec![9, 9])),
                }],
            }
            .encode()?,
        },
    )?;
    assert_eq!(update_response.payload, [0, 1, 0]);
    assert!(matches!(
        connection.revalidate_row_handle(&updated_row_handle),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    assert!(
        matches!(
            connection.revalidate_row_handle(&update_alias),
            Err(TransactionError::InvalidMetadataRequest)
        ),
        "UPDATE invalidates every handle previously issued for the row"
    );
    let deleted_row_handle = second_page.rows[0].handle.clone();
    let delete_response = dispatch_executor_request(
        ServiceRole::Executor,
        &mut connection,
        &IpcFrame {
            operation: IpcOperation::StageDelete as u16,
            request_id: 69,
            payload: StageDeleteRequest {
                handle: deleted_row_handle.clone(),
            }
            .encode(),
        },
    )?;
    assert_eq!(delete_response.payload, [0, 1, 0]);
    assert!(matches!(
        connection.revalidate_row_handle(&deleted_row_handle),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    assert!(
        matches!(
            connection.revalidate_row_handle(&delete_alias),
            Err(TransactionError::InvalidMetadataRequest)
        ),
        "DELETE invalidates every handle previously issued for the row"
    );
    let relation_close_cursor = connection.open_relation_scan(&relation_handle, &[[0x66; 16]])?;
    connection.close_relation_handle(&relation_handle)?;
    assert!(matches!(
        connection.revalidate_relation_handle(&relation_handle),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    assert!(matches!(
        connection.revalidate_row_handle(&inserted_handle),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    assert!(matches!(
        connection.next_relation_scan_page(&relation_close_cursor, 1),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    drop(connection);
    let mut replacement = invocation.open_executor_metadata_connection();
    assert!(matches!(
        replacement.next_relation_scan_page(&next_cursor, 1),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    drop(replacement);
    invocation.commit()?;
    let mut verify =
        InvocationTransaction::begin(&database, [236; 16], principal_id, 9_999, 1, 128)?;
    let persisted_schema = verify.load_catalog_table_schema(table_id)?;
    let persisted = verify
        .read(row_id)?
        .expect("updated row remains visible after commit");
    assert_eq!(
        decode_tuple(
            &persisted_schema.columns,
            persisted_schema.schema_version,
            &persisted
        )?
        .values,
        [Some(ScalarValue::Bytea(vec![9, 9]))]
    );
    assert!(verify.read(second_row_id)?.is_none());
    let inserted = verify
        .read(inserted_row_id)?
        .expect("inserted row remains visible after commit");
    assert_eq!(
        decode_tuple(
            &persisted_schema.columns,
            persisted_schema.schema_version,
            &inserted
        )?
        .values,
        [Some(ScalarValue::Bytea(vec![4, 5]))]
    );
    verify.abort()?;

    let mut restricted = InvocationTransaction::begin(
        &database,
        [238; 16],
        restricted_principal_id,
        9_999,
        1,
        1024,
    )?;
    let restricted_schema = restricted.load_catalog_table_schema(table_id)?;
    let (_, restricted_grants) =
        restricted.load_catalog_grants(restricted_principal_id, &restricted_schema)?;
    let restricted_policies =
        restricted.load_catalog_policies(restricted_principal_id, &restricted_schema)?;
    assert_eq!(restricted_grants.len(), 3);
    assert_eq!(restricted_policies.len(), 1);
    assert_eq!(
        restricted.read_catalog_authorized_row(table_id, row_id.row)?,
        Some(vec![SanitizedCell::Redacted])
    );
    let mut restricted_connection = restricted.open_executor_metadata_connection();
    let (restricted_relation, _) = restricted_connection
        .resolve_relation("app", "metrics")?
        .expect("the table grant should make the relation available");
    let restricted_open = dispatch_executor_request(
        ServiceRole::Executor,
        &mut restricted_connection,
        &IpcFrame {
            operation: IpcOperation::OpenScan as u16,
            request_id: 71,
            payload: OpenScanRequest {
                relation: restricted_relation,
                column_ids: vec![[0x66; 16]],
            }
            .encode()?,
        },
    )?;
    let restricted_cursor = OpenScanResponse::decode(&restricted_open.payload)?.cursor;
    let restricted_batch = dispatch_executor_request(
        ServiceRole::Executor,
        &mut restricted_connection,
        &IpcFrame {
            operation: IpcOperation::NextBatch as u16,
            request_id: 72,
            payload: NextBatchRequest {
                cursor: restricted_cursor,
                max_scanned: 8,
                max_bytes: 1024,
            }
            .encode()?,
        },
    )?;
    let restricted_rows = NextBatchResponse::decode(&restricted_batch.payload)?.rows;
    assert_eq!(restricted_rows.len(), 2);
    assert!(
        restricted_rows
            .iter()
            .all(|row| { row.cells == [SanitizedCell::Redacted] })
    );
    drop(restricted_connection);
    restricted.abort()?;

    let oversized_invocation_id = [237; 16];
    let mut oversized = InvocationTransaction::begin(
        &database,
        oversized_invocation_id,
        principal_id,
        9_999,
        1,
        128,
    )?;
    let mut oversized_connection = oversized.open_executor_metadata_connection();
    let (oversized_relation, _) = oversized_connection
        .resolve_relation("app", "metrics")?
        .expect("visible relation resolves");
    let open_response = dispatch_executor_request(
        ServiceRole::Executor,
        &mut oversized_connection,
        &IpcFrame {
            operation: IpcOperation::OpenScan as u16,
            request_id: 71,
            payload: OpenScanRequest {
                relation: oversized_relation,
                column_ids: vec![[0x66; 16]],
            }
            .encode()?,
        },
    )?;
    let oversized_cursor = OpenScanResponse::decode(&open_response.payload)?.cursor;
    assert!(matches!(
        dispatch_executor_request(
            ServiceRole::Executor,
            &mut oversized_connection,
            &IpcFrame {
                operation: IpcOperation::NextBatch as u16,
                request_id: 72,
                payload: NextBatchRequest {
                    cursor: oversized_cursor,
                    max_scanned: 1,
                    max_bytes: 46,
                }
                .encode()?,
            },
        ),
        Err(
            prdb_engine::executor_ipc::ExecutorRequestError::Transaction(
                TransactionError::MetadataResultTooLarge
            )
        )
    ));
    assert_eq!(
        prdb_engine::outcome::lookup_outcome(&database, oversized_invocation_id, principal_id, 1,)?
            .expect("oversized batch aborts durably")
            .status,
        OutcomeStatus::Aborted
    );
    let guarded_value = vec![9, 9];
    let policy_expression = Expr::Equal(Box::new(Expr::Field(0)), Box::new(Expr::Field(0)));
    let policy_id = [0xf4; 16];
    let using_expression_id = [0xf5; 16];
    let check_expression_id = [0xf6; 16];
    let external_policy_id = [0xf8; 16];
    let external_using_expression_id = [0xf9; 16];
    let external_check_expression_id = [0xfa; 16];
    let select_policy_id = [0xf1; 16];
    let select_expression_id = [0xf2; 16];
    let select_expression = Expr::Literal(Some(ScalarValue::Boolean(true)));
    let (select_policy_key, select_policy_payload) = policy_catalog_record(
        select_policy_id,
        table_id,
        1,
        1,
        Some(select_expression_id),
        None,
    );
    let (select_policy_target_key, select_policy_target_payload) =
        policy_target_catalog_record(select_policy_id, 1, principal_id);
    let select_expression_key = catalog_id_key(7, select_expression_id);
    let select_expression_payload = expression_catalog_payload_full(
        select_expression_id,
        3,
        [46; 16],
        [0; 8],
        &[],
        &select_expression,
    )?;
    let (policy_key, policy_payload) = policy_catalog_record(
        policy_id,
        table_id,
        1,
        3,
        Some(using_expression_id),
        Some(check_expression_id),
    );
    let (policy_target_key, policy_target_payload) =
        policy_target_catalog_record(policy_id, 1, principal_id);
    let allow_expression = Expr::Literal(Some(ScalarValue::Boolean(true)));
    let using_expression_key = catalog_id_key(7, using_expression_id);
    let check_expression_key = catalog_id_key(7, check_expression_id);
    let using_expression_payload = expression_catalog_payload_full(
        using_expression_id,
        3,
        [46; 16],
        [0; 8],
        &[],
        &allow_expression,
    )?;
    let check_expression_payload = expression_catalog_payload_full(
        check_expression_id,
        4,
        [46; 16],
        [0; 8],
        &[],
        &allow_expression,
    )?;
    let (external_policy_key, external_policy_payload) = policy_catalog_record(
        external_policy_id,
        table_id,
        1,
        3,
        Some(external_using_expression_id),
        Some(external_check_expression_id),
    );
    let (external_policy_target_key, external_policy_target_payload) =
        policy_target_catalog_record(external_policy_id, 1, restricted_principal_id);
    let external_using_expression_key = catalog_id_key(7, external_using_expression_id);
    let external_check_expression_key = catalog_id_key(7, external_check_expression_id);
    let external_using_expression_payload = expression_catalog_payload_full(
        external_using_expression_id,
        3,
        [46; 16],
        [0; 8],
        &[[0x66; 16]],
        &policy_expression,
    )?;
    let external_check_expression_payload = expression_catalog_payload_full(
        external_check_expression_id,
        4,
        [46; 16],
        [0; 8],
        &[[0x66; 16]],
        &policy_expression,
    )?;
    let write = database.begin_write()?;
    {
        let mut catalog = write.open_table(CATALOG)?;
        catalog.insert(
            select_policy_key.as_slice(),
            select_policy_payload.as_slice(),
        )?;
        catalog.insert(
            select_policy_target_key.as_slice(),
            select_policy_target_payload.as_slice(),
        )?;
        catalog.insert(
            select_expression_key.as_slice(),
            select_expression_payload.as_slice(),
        )?;
        catalog.insert(policy_key.as_slice(), policy_payload.as_slice())?;
        catalog.insert(
            policy_target_key.as_slice(),
            policy_target_payload.as_slice(),
        )?;
        catalog.insert(
            using_expression_key.as_slice(),
            using_expression_payload.as_slice(),
        )?;
        catalog.insert(
            check_expression_key.as_slice(),
            check_expression_payload.as_slice(),
        )?;
        catalog.insert(
            external_policy_key.as_slice(),
            external_policy_payload.as_slice(),
        )?;
        catalog.insert(
            external_policy_target_key.as_slice(),
            external_policy_target_payload.as_slice(),
        )?;
        catalog.insert(
            external_using_expression_key.as_slice(),
            external_using_expression_payload.as_slice(),
        )?;
        catalog.insert(
            external_check_expression_key.as_slice(),
            external_check_expression_payload.as_slice(),
        )?;
        let mut policies = write.open_table(schema::POLICIES)?;
        for (operation, indexed_policy_id) in [
            (1u16, select_policy_id),
            (1u16, restricted_row_policy_id),
            (3u16, policy_id),
            (3u16, external_policy_id),
        ] {
            let mut index_key = Vec::with_capacity(34);
            index_key.extend_from_slice(&table_id);
            index_key.extend_from_slice(&operation.to_be_bytes());
            index_key.extend_from_slice(&indexed_policy_id);
            policies.insert(index_key.as_slice(), &[][..])?;
        }
    }
    write.commit()?;
    let mut guarded_writer =
        InvocationTransaction::begin(&database, [0xf7; 16], principal_id, 9_999, 2, 1024)?;
    let guarded_schema = guarded_writer.load_catalog_table_schema(table_id)?;
    let guarded_policies = guarded_writer
        .load_catalog_policies(principal_id, &guarded_schema)
        .map_err(|error| format!("policy-driving expression failed to load: {error:?}"))?;
    let update_policies = guarded_policies
        .iter()
        .filter(|policy| policy.operation == Operation::Update)
        .collect::<Vec<_>>();
    assert_eq!(update_policies.len(), 1);
    assert_eq!(update_policies[0].using, Some(allow_expression.clone()));
    assert_eq!(update_policies[0].with_check, Some(allow_expression));
    let mut guarded_connection = guarded_writer.open_executor_metadata_connection();
    let (guarded_relation, _) = guarded_connection
        .resolve_relation("app", "metrics")?
        .expect("the SELECT grant keeps the relation available");
    let guarded_scan = guarded_connection.open_relation_scan(&guarded_relation, &[[0x66; 16]])?;
    let guarded_page = guarded_connection.next_relation_scan_page(&guarded_scan, 8)?;
    let guarded_handle = &guarded_page
        .rows
        .first()
        .ok_or("policy-driving test row was not visible")?
        .0;
    assert!(matches!(
        guarded_connection.stage_update(
            guarded_handle,
            &[FieldPatch {
                ordinal: 0,
                value: Some(ScalarValue::Bytea(vec![0xaa])),
            }],
        ),
        Err(TransactionError::Authorization(
            prdb_engine::authorization::AuthorizationError::Denied
        ))
    ));
    let guarded_row = RowRecord::decode(
        database
            .begin_read()?
            .open_table(ROWS)?
            .get(&key(row_id)[..])?
            .ok_or("policy-driven row was unexpectedly removed")?
            .value(),
    )?;
    assert_eq!(
        decode_tuple(
            &[ColumnSchema {
                scalar_type: ScalarType::Bytea,
                nullable: false,
            }],
            1,
            &guarded_row.payload
        )?
        .values,
        [Some(ScalarValue::Bytea(guarded_value))],
        "a write must not mutate a field used by another principal's policy"
    );
    drop(guarded_connection);
    guarded_writer.abort()?;

    Ok(())
}
