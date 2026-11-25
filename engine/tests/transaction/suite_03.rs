use prdb_engine::audit::{AuditAction, AuditEvent, AuditResult};
use prdb_engine::authorization::{
    AccessTarget, AuthorizationContext, AuthorizedProjection, Effect, Expr, Grant, Operation,
    Policy, SanitizedCell, Subject,
};
use prdb_engine::commit::{
    CatalogWrite, CommitError, RowWrite, commit_catalog_and_audit, commit_rows,
};
use prdb_engine::outcome::{InvocationOutcome, OutcomeStatus};
use prdb_engine::row_record::RowRecord;
use prdb_engine::schema::{CATALOG, PRINCIPAL_NAMES, ROWS};
use prdb_engine::transaction::{InvocationTransaction, TransactionError};
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, encode_tuple};
use redb::ReadableDatabase;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{SystemTime, UNIX_EPOCH};

use super::helpers::*;

pub(super) fn authorized_column_description_pages_bind_the_snapshot_and_order()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    install_second_relation_column(&database)?;
    let schema_id = [0x91; 16];
    let table_id = [0x10; 16];
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (Operation::Insert, AccessTarget::Table(table_id)),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema_id),
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Insert,
            target: AccessTarget::Table(table_id),
        },
    ];
    let mut invocation = InvocationTransaction::begin(&database, [186; 16], [9; 16], 9_999, 1, 64)?;
    let first = invocation
        .describe_authorized_columns_page(table_id, 1, None, &context, &grants, &[])?
        .expect("table write grant permits shape discovery");
    assert_eq!(first.columns.len(), 1);
    assert_eq!(first.columns[0].name, "value");
    assert_eq!(first.columns[0].ordinal, 0);
    let cursor = first.continuation.expect("second column");
    let cursor_bytes = cursor.to_bytes();
    assert!(prdb_engine::metadata_discovery::SchemaScanCursor::from_bytes(&cursor_bytes).is_none());
    assert!(
        prdb_engine::metadata_discovery::RelationScanCursor::from_bytes(&cursor_bytes).is_none()
    );
    let cursor = prdb_engine::metadata_discovery::ColumnScanCursor::from_bytes(&cursor_bytes)
        .expect("column cursor wire form is fixed width");

    let mut other_invocation =
        InvocationTransaction::begin(&database, [187; 16], [9; 16], 9_999, 1, 64)?;
    assert!(matches!(
        other_invocation.describe_authorized_columns_page(
            table_id,
            1,
            Some(cursor.clone()),
            &context,
            &grants,
            &[],
        ),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    let second = invocation
        .describe_authorized_columns_page(
            table_id,
            1,
            Some(cursor.clone()),
            &context,
            &grants,
            &[],
        )?
        .expect("same snapshot continues");
    assert_eq!(second.columns.len(), 1);
    assert_eq!(second.columns[0].column_id, [0x67; 16]);
    assert_eq!(second.columns[0].name, "value2");
    assert_eq!(second.columns[0].ordinal, 1);
    assert!(second.continuation.is_none());
    assert!(matches!(
        invocation.describe_authorized_columns_page(
            table_id,
            1,
            Some(cursor),
            &context,
            &grants,
            &[],
        ),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    Ok(())
}

pub(super) fn invocation_reads_are_projected_after_snapshot_selection()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let columns = [
        ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Uuid,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: false,
        },
    ];
    let column_ids = [[31; 16], [32; 16], [33; 16]];
    let row_id = id(33);
    let payload = encode_tuple(
        &columns,
        1,
        &[
            Some(ScalarValue::Text("Alice".to_owned())),
            Some(ScalarValue::Uuid([9; 16])),
            Some(ScalarValue::Text(
                "long protected value that must not escape".to_owned(),
            )),
        ],
    )?;
    commit_rows(
        &database,
        [30; 16],
        [9; 16],
        9_999,
        &[RowWrite {
            key: key(row_id),
            expected_version: None,
            payload: Some(payload),
        }],
    )?;

    let table_target = AccessTarget::Table(row_id.table);
    let contexts = [[9; 16], [7; 16]].map(|principal_id| AuthorizationContext {
        principal_id,
        role_ids: std::collections::HashSet::from([[40; 16]]),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(row_id.table)),
            (Operation::Select, table_target),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: row_id.table,
                    column_id: column_ids[0],
                },
            ),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: row_id.table,
                    column_id: column_ids[1],
                },
            ),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: row_id.table,
                    column_id: column_ids[2],
                },
            ),
        ]),
    });
    let mut grants = column_ids
        .map(|column_id| Grant {
            subject: Subject::Role([40; 16]),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: row_id.table,
                column_id,
            },
        })
        .to_vec();
    grants.push(Grant {
        subject: Subject::Role([40; 16]),
        operation: Operation::Usage,
        target: AccessTarget::Schema(row_id.table),
    });
    let policies = [
        Policy {
            subjects: vec![Subject::Role([40; 16])],
            operation: Operation::Select,
            target: table_target,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Role([40; 16])],
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: row_id.table,
                column_id: column_ids[0],
            },
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Role([40; 16])],
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: row_id.table,
                column_id: column_ids[2],
            },
            effect: Effect::Allow,
            using: Some(Expr::Equal(
                Box::new(Expr::Field(1)),
                Box::new(Expr::CurrentPrincipalId),
            )),
            with_check: None,
        },
    ];

    for (index, context) in contexts.iter().enumerate() {
        let mut invocation = InvocationTransaction::begin(
            &database,
            [34 + index as u8; 16],
            context.principal_id,
            9_999,
            2,
            128,
        )?;
        let output = invocation
            .read_authorized(
                row_id,
                &AuthorizedProjection {
                    columns: &columns,
                    column_ids: &column_ids,
                    schema_version: 1,
                    schema_id: row_id.table,
                    context,
                    grants: &grants,
                    policies: &policies,
                    checks: &[],
                    policy_driving_columns: &[],
                    dependencies: &[],
                },
            )?
            .unwrap();
        assert_eq!(
            output[0],
            SanitizedCell::Value(ScalarValue::Text("Alice".into()))
        );
        if index == 0 {
            assert_eq!(
                output[2],
                SanitizedCell::Value(ScalarValue::Text(
                    "long protected value that must not escape".to_owned()
                ))
            );
        } else {
            assert_eq!(output[2], SanitizedCell::Redacted);
        }
        assert_eq!(output[1], SanitizedCell::Redacted);
        invocation.abort()?;
    }
    Ok(())
}

pub(super) fn authorization_evaluation_error_aborts_and_persists_terminal_status()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_id = id(44);
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Text,
        nullable: false,
    }];
    let column_ids = [[45; 16]];
    let payload = encode_tuple(
        &columns,
        1,
        &[Some(ScalarValue::Text("protected".to_owned()))],
    )?;
    commit_rows(
        &database,
        [46; 16],
        [9; 16],
        9_999,
        &[RowWrite {
            key: key(row_id),
            expected_version: None,
            payload: Some(payload),
        }],
    )?;
    let target = AccessTarget::Column {
        table_id: row_id.table,
        column_id: column_ids[0],
    };
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::from([[47; 16]]),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(row_id.table)),
            (Operation::Select, AccessTarget::Table(row_id.table)),
            (Operation::Select, target),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Role([47; 16]),
            operation: Operation::Select,
            target,
        },
        Grant {
            subject: Subject::Role([47; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(row_id.table),
        },
    ];
    let policies = [
        Policy {
            subjects: vec![Subject::Role([47; 16])],
            operation: Operation::Select,
            target: AccessTarget::Table(row_id.table),
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Role([47; 16])],
            operation: Operation::Select,
            target,
            effect: Effect::Allow,
            using: Some(Expr::Field(99)),
            with_check: None,
        },
    ];
    let mut invocation = InvocationTransaction::begin(&database, [48; 16], [9; 16], 9_999, 2, 64)?;
    assert!(matches!(
        invocation.read_authorized(
            row_id,
            &AuthorizedProjection {
                columns: &columns,
                column_ids: &column_ids,
                schema_version: 1,
                schema_id: row_id.table,
                context: &context,
                grants: &grants,
                policies: &policies,
                checks: &[],
                policy_driving_columns: &[],
                dependencies: &[],
            }
        ),
        Err(TransactionError::Authorization(
            prdb_engine::authorization::AuthorizationError::EvaluationFailed
        ))
    ));
    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Aborted)
    ));
    let read = database.begin_read()?;
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&[48; 16][..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

pub(super) fn authorization_context_cannot_change_the_admitted_principal()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Text,
        nullable: false,
    }];
    let column_ids = [[91; 16]];
    let context = AuthorizationContext {
        principal_id: [8; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::new(),
    };
    let projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id: [90; 16],
        context: &context,
        grants: &[],
        policies: &[],
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let mut invocation = InvocationTransaction::begin(&database, [92; 16], [9; 16], 9_999, 2, 64)?;
    assert!(matches!(
        invocation.read_authorized(id(93), &projection),
        Err(TransactionError::InvalidContext)
    ));
    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Aborted)
    ));
    let read = database.begin_read()?;
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&[92; 16][..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

pub(super) fn latest_catalog_authority_change_blocks_a_stale_invocation_commit()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let (principal_key, principal_v1) = principal_catalog_record(1);
    let (object_key, object_value) = principal_object_record_for([7; 16], 1);
    let write = database.begin_write()?;
    write
        .open_table(CATALOG)?
        .insert(&principal_key[..], &principal_v1[..])?;
    write
        .open_table(CATALOG)?
        .insert(&object_key[..], &object_value[..])?;
    write
        .open_table(PRINCIPAL_NAMES)?
        .insert(principal_test_name([7; 16]).as_bytes(), &[7; 16][..])?;
    write.commit()?;

    let mut invocation = InvocationTransaction::begin(&database, [18; 16], [9; 16], 9_999, 2, 64)?;
    let principal = invocation.read_catalog_record(&principal_key)?.unwrap();
    assert_eq!(principal.family_code, 10);
    invocation.stage_put(id(18), b"write authorized at old epoch")?;

    let (_, principal_v2) = principal_catalog_record(2);
    let authority_change = AuditEvent {
        event_id: [20; 16],
        actor_id: Some([9; 16]),
        correlation_id: [21; 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::AuthorityChange,
        result: AuditResult::Succeeded,
        target_ids: vec![[7; 16]],
    };
    commit_catalog_and_audit(
        &database,
        [19; 16],
        [9; 16],
        9_999,
        &[CatalogWrite {
            key: principal_key,
            expected_value: Some(principal_v1),
            payload: Some(principal_v2),
        }],
        &[authority_change],
    )?;

    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Commit(CommitError::Conflict))
    ));
    assert_eq!(commit_sequence(&database)?, 1);
    let read = database.begin_read()?;
    assert!(read.open_table(ROWS)?.get(&key(id(18))[..])?.is_none());
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&[18; 16][..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

pub(super) fn racing_catalog_authority_change_linearizes_before_or_after_pending_write()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let principal_id = [0xA1; 16];
    let (principal_key, principal_v1) =
        principal_catalog_record_for(principal_id, 1, b"race_user", true, 1);
    let (object_key, object_value) = principal_object_record_for(principal_id, 1);
    let seed = database.begin_write()?;
    seed.open_table(CATALOG)?
        .insert(principal_key.as_slice(), principal_v1.as_slice())?;
    seed.open_table(CATALOG)?
        .insert(object_key.as_slice(), object_value.as_slice())?;
    seed.open_table(PRINCIPAL_NAMES)?.insert(
        principal_test_name(principal_id).as_bytes(),
        &principal_id[..],
    )?;
    seed.commit()?;

    let (principal_key, principal_v2) =
        principal_catalog_record_for(principal_id, 1, b"race_user", true, 2);
    let row_id = id(0xA2);
    let invocation_id = [0xA3; 16];
    let authority_invocation_id = [0xA4; 16];
    let barrier = Arc::new(Barrier::new(3));
    let (write_result, authority_result) = std::thread::scope(|scope| {
        let write_barrier = barrier.clone();
        let write_database = &database;
        let write = scope.spawn(move || {
            let mut invocation = InvocationTransaction::begin(
                write_database,
                invocation_id,
                principal_id,
                9_999,
                2,
                64,
            )?;
            assert!(
                invocation
                    .load_catalog_role_closure(principal_id)?
                    .is_empty()
            );
            invocation.stage_put(row_id, b"authorized before race")?;
            write_barrier.wait();
            invocation.commit()
        });

        let authority_barrier = barrier.clone();
        let authority_database = &database;
        let authority_key = principal_key.clone();
        let authority_expected = principal_v1.clone();
        let authority_payload = principal_v2.clone();
        let authority = scope.spawn(move || {
            authority_barrier.wait();
            let event = AuditEvent {
                event_id: authority_invocation_id,
                actor_id: Some([9; 16]),
                correlation_id: [0xA5; 16],
                occurred_at_us: 1_700_000_000_000_000,
                action: AuditAction::AuthorityChange,
                result: AuditResult::Succeeded,
                target_ids: vec![principal_id],
            };
            commit_catalog_and_audit(
                authority_database,
                authority_invocation_id,
                [9; 16],
                9_999,
                &[CatalogWrite {
                    key: authority_key,
                    expected_value: Some(authority_expected),
                    payload: Some(authority_payload),
                }],
                &[event],
            )
        });

        barrier.wait();
        (write.join().unwrap(), authority.join().unwrap())
    });

    let authority_sequence = authority_result?;
    let write_sequence = match write_result {
        Ok(sequence) => Some(sequence),
        Err(TransactionError::Commit(CommitError::Conflict)) => None,
        Err(error) => return Err(Box::new(error)),
    };
    let read = database.begin_read()?;
    let row_exists = read.open_table(ROWS)?.get(&key(row_id)[..])?.is_some();
    let write_outcome = InvocationOutcome::decode(
        read.open_table(prdb_engine::schema::OUTCOMES)?
            .get(&invocation_id[..])?
            .ok_or("invocation outcome missing")?
            .value(),
    )?;
    assert_eq!(row_exists, write_sequence.is_some());
    assert_eq!(
        write_outcome.status,
        if write_sequence.is_some() {
            OutcomeStatus::Committed
        } else {
            OutcomeStatus::Aborted
        }
    );
    assert_eq!(commit_sequence(&database)?, authority_sequence);
    assert!(
        write_sequence.is_none() || write_sequence.unwrap() < authority_sequence,
        "a pending write may commit only before the concurrent authority change"
    );
    Ok(())
}

pub(super) fn catalog_absence_guard_blocks_a_phantom_authority_record()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let mut invocation = InvocationTransaction::begin(&database, [22; 16], [9; 16], 9_999, 2, 64)?;
    let (principal_key, principal_v1) = principal_catalog_record(1);
    assert!(invocation.read_catalog_record(&principal_key)?.is_none());
    invocation.stage_put(id(22), b"write based on absent authority record")?;

    let authority_change = AuditEvent {
        event_id: [23; 16],
        actor_id: Some([9; 16]),
        correlation_id: [24; 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::AuthorityChange,
        result: AuditResult::Succeeded,
        target_ids: vec![[7; 16]],
    };
    commit_catalog_and_audit(
        &database,
        [25; 16],
        [9; 16],
        9_999,
        &[
            CatalogWrite {
                key: principal_key,
                expected_value: None,
                payload: Some(principal_v1),
            },
            {
                let (key, payload) = principal_object_record_for([7; 16], 1);
                CatalogWrite {
                    key,
                    expected_value: None,
                    payload: Some(payload),
                }
            },
        ],
        &[authority_change],
    )?;

    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Commit(CommitError::Conflict))
    ));
    assert_eq!(commit_sequence(&database)?, 1);
    let read = database.begin_read()?;
    assert!(read.open_table(ROWS)?.get(&key(id(22))[..])?.is_none());
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&[22; 16][..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

pub(super) fn catalog_expression_reads_are_decoded_and_guarded_at_commit()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let expression_id = [100; 16];
    let expression_key = {
        let mut key = 7u16.to_be_bytes().to_vec();
        key.push(1);
        key.extend_from_slice(&expression_id);
        key
    };
    let original =
        expression_catalog_payload(expression_id, &Expr::Literal(Some(ScalarValue::Integer(1))))?;
    let (object_key, object_payload) = object_catalog_record_for(expression_id, 5);
    let ddl_audit = |event_id, target_id| AuditEvent {
        event_id: [event_id; 16],
        actor_id: Some([9; 16]),
        correlation_id: [event_id.wrapping_add(1); 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![[target_id; 16]],
    };
    commit_catalog_and_audit(
        &database,
        [101; 16],
        [9; 16],
        9_999,
        &[
            CatalogWrite {
                key: expression_key.clone(),
                expected_value: None,
                payload: Some(original.clone()),
            },
            CatalogWrite {
                key: object_key,
                expected_value: None,
                payload: Some(object_payload),
            },
        ],
        &[ddl_audit(102, 100)],
    )?;

    let mut invocation = InvocationTransaction::begin(&database, [103; 16], [9; 16], 9_999, 1, 64)?;
    let expression = invocation.read_catalog_expression(expression_id)?.unwrap();
    assert_eq!(
        expression.purpose,
        prdb_engine::expression::ExpressionPurpose::Default
    );
    assert_eq!(
        expression.expression,
        Expr::Literal(Some(ScalarValue::Integer(1)))
    );
    invocation.stage_put(id(104), b"write based on pinned expression")?;

    let replacement =
        expression_catalog_payload(expression_id, &Expr::Literal(Some(ScalarValue::Integer(2))))?;
    commit_catalog_and_audit(
        &database,
        [105; 16],
        [9; 16],
        9_999,
        &[CatalogWrite {
            key: expression_key,
            expected_value: Some(original),
            payload: Some(replacement),
        }],
        &[ddl_audit(106, 100)],
    )?;
    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Commit(CommitError::Conflict))
    ));
    let read = database.begin_read()?;
    assert!(read.open_table(ROWS)?.get(&key(id(104))[..])?.is_none());
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&[103; 16][..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

pub(super) fn invocation_sees_own_writes_and_commits_against_its_pinned_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    stage_initial_row(&database)?;

    let mut invocation = InvocationTransaction::begin(&database, [3; 16], [9; 16], 9_999, 4, 64)?;
    assert_eq!(invocation.read(id(1))?, Some(b"snapshot".to_vec()));
    invocation.stage_put(id(1), b"private")?;
    assert_eq!(invocation.read(id(1))?, Some(b"private".to_vec()));
    invocation.stage_put(id(2), b"inserted")?;
    assert_eq!(invocation.read(id(2))?, Some(b"inserted".to_vec()));

    // A different transaction can commit a disjoint row while this snapshot remains pinned.
    commit_rows(
        &database,
        [2; 16],
        [9; 16],
        9_999,
        &[RowWrite {
            key: key(id(3)),
            expected_version: None,
            payload: Some(b"other".to_vec()),
        }],
    )?;
    assert_eq!(invocation.read(id(1))?, Some(b"private".to_vec()));
    assert_eq!(invocation.commit()?, 3);

    let read = database.begin_read()?;
    let rows = read.open_table(ROWS)?;
    let updated = RowRecord::decode(rows.get(&key(id(1))[..])?.unwrap().value())?;
    let inserted = RowRecord::decode(rows.get(&key(id(2))[..])?.unwrap().value())?;
    let disjoint = RowRecord::decode(rows.get(&key(id(3))[..])?.unwrap().value())?;
    assert_eq!(
        (updated.row_version, updated.payload.as_slice()),
        (3, &b"private"[..])
    );
    assert_eq!(
        (inserted.row_version, inserted.payload.as_slice()),
        (3, &b"inserted"[..])
    );
    assert_eq!(
        (disjoint.row_version, disjoint.payload.as_slice()),
        (2, &b"other"[..])
    );
    Ok(())
}

pub(super) fn invocation_snapshot_hides_private_writes_and_keeps_repeated_reads_stable()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    stage_initial_row(&database)?;

    let mut reader = InvocationTransaction::begin(&database, [20; 16], [9; 16], 9_999, 4, 64)?;
    let mut writer = InvocationTransaction::begin(&database, [21; 16], [9; 16], 9_999, 4, 64)?;
    assert_eq!(reader.read(id(1))?, Some(b"snapshot".to_vec()));
    writer.stage_put(id(1), b"writer-private")?;
    writer.stage_put(id(2), b"insert-private")?;
    assert_eq!(writer.read(id(1))?, Some(b"writer-private".to_vec()));
    assert_eq!(writer.read(id(2))?, Some(b"insert-private".to_vec()));

    // Another invocation sees neither the writer's private update nor insert.
    assert_eq!(reader.read(id(1))?, Some(b"snapshot".to_vec()));
    assert_eq!(reader.read(id(2))?, None);

    assert_eq!(writer.commit()?, 2);

    // The already admitted reader retains the old snapshot after publication.
    assert_eq!(reader.read(id(1))?, Some(b"snapshot".to_vec()));
    assert_eq!(reader.read(id(2))?, None);
    reader.abort()?;

    // A new invocation admitted after commit observes both published writes.
    let mut observer = InvocationTransaction::begin(&database, [22; 16], [9; 16], 9_999, 4, 64)?;
    assert_eq!(observer.read(id(1))?, Some(b"writer-private".to_vec()));
    assert_eq!(observer.read(id(2))?, Some(b"insert-private".to_vec()));
    observer.abort()?;

    let read = database.begin_read()?;
    let outcomes = read.open_table(prdb_engine::schema::OUTCOMES)?;
    for (invocation_id, expected_status) in [
        ([20; 16], OutcomeStatus::Aborted),
        ([21; 16], OutcomeStatus::Committed),
        ([22; 16], OutcomeStatus::Aborted),
    ] {
        assert_eq!(
            InvocationOutcome::decode(outcomes.get(&invocation_id[..])?.unwrap().value())?.status,
            expected_status
        );
    }
    Ok(())
}

pub(super) fn retained_invocation_snapshot_survives_a_durable_write_burst()
-> Result<(), Box<dyn std::error::Error>> {
    const VALUE_BYTES: usize = 32 * 1024;
    const WRITE_COUNT: u8 = 64;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    stage_initial_row(&database)?;
    let mut reader =
        InvocationTransaction::begin(&database, [0x30; 16], [9; 16], 9_999, 4, 32 * 1024)?;
    let original_value = reader.read(id(1))?.ok_or("initial row missing")?;

    let mut latest_value = Vec::new();
    for version in 0..WRITE_COUNT {
        latest_value = vec![version; VALUE_BYTES];
        let mut writer = InvocationTransaction::begin(
            &database,
            [0x40u8.wrapping_add(version); 16],
            [9; 16],
            9_999,
            4,
            32 * 1024,
        )?;
        writer.stage_put(id(1), &latest_value)?;
        writer.commit()?;
        assert_eq!(
            reader.read(id(1))?,
            Some(original_value.clone()),
            "the pinned invocation must retain its original row after write {version}"
        );
    }
    reader.abort()?;

    let mut observer =
        InvocationTransaction::begin(&database, [0x90; 16], [9; 16], 9_999, 4, 32 * 1024)?;
    assert_eq!(observer.read(id(1))?, Some(latest_value));
    observer.abort()?;
    Ok(())
}

pub(super) fn retained_invocation_snapshot_survives_concurrent_writer_pressure()
-> Result<(), Box<dyn std::error::Error>> {
    const WRITER_COUNT: usize = 4;
    const WRITES_PER_WRITER: u8 = 64;
    const VALUE_BYTES: usize = 32 * 1024;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_ids = (1..=WRITER_COUNT as u8).map(id).collect::<Vec<_>>();
    let original_values = (0..WRITER_COUNT)
        .map(|writer| vec![0x10 + writer as u8; VALUE_BYTES])
        .collect::<Vec<_>>();
    let mut seed =
        InvocationTransaction::begin(&database, [0x20; 16], [9; 16], 9_999, 8, 512 * 1024)?;
    for (row_id, value) in row_ids.iter().copied().zip(&original_values) {
        seed.stage_put(row_id, value)?;
    }
    seed.commit()?;

    let mut reader =
        InvocationTransaction::begin(&database, [0x30; 16], [9; 16], 9_999, 8, 512 * 1024)?;
    for (row_id, value) in row_ids.iter().copied().zip(&original_values) {
        assert_eq!(reader.read(row_id)?, Some(value.clone()));
    }

    let start_round = Arc::new(Barrier::new(WRITER_COUNT + 1));
    let finish_round = Arc::new(Barrier::new(WRITER_COUNT + 1));
    let database_ref = &database;
    let (reader_succeeded, writers_succeeded) = std::thread::scope(|scope| {
        let mut writers = Vec::with_capacity(WRITER_COUNT);
        for (writer_index, row_id) in row_ids.iter().copied().enumerate() {
            let start_round = Arc::clone(&start_round);
            let finish_round = Arc::clone(&finish_round);
            let database = database_ref;
            writers.push(scope.spawn(move || {
                let mut succeeded = true;
                for version in 0..WRITES_PER_WRITER {
                    start_round.wait();
                    if succeeded {
                        let result = (|| {
                            let value = vec![
                                0x40u8.wrapping_add(
                                    (writer_index as u8)
                                        .wrapping_mul(WRITES_PER_WRITER)
                                        .wrapping_add(version),
                                );
                                VALUE_BYTES
                            ];
                            let mut invocation_id = [0; 16];
                            invocation_id[0] = 0x80 + writer_index as u8;
                            invocation_id[1] = version;
                            let mut writer = InvocationTransaction::begin(
                                database,
                                invocation_id,
                                [9; 16],
                                9_999,
                                4,
                                VALUE_BYTES * 2,
                            )?;
                            writer.stage_put(row_id, &value)?;
                            writer.commit()?;
                            Ok::<(), Box<dyn std::error::Error>>(())
                        })();
                        succeeded = result.is_ok();
                    }
                    finish_round.wait();
                }
                succeeded
            }));
        }
        let mut reader_succeeded = true;
        for _ in 0..WRITES_PER_WRITER {
            start_round.wait();
            for (row_id, value) in row_ids.iter().copied().zip(&original_values) {
                match reader.read(row_id) {
                    Ok(actual) if actual == Some(value.clone()) => {}
                    _ => reader_succeeded = false,
                }
            }
            finish_round.wait();
        }
        let writers_succeeded = writers
            .into_iter()
            .all(|writer| writer.join().unwrap_or(false));
        (reader_succeeded, writers_succeeded)
    });
    assert!(writers_succeeded, "a pressure writer failed");
    assert!(
        reader_succeeded,
        "the retained invocation snapshot changed during writer rounds"
    );

    for (row_id, value) in row_ids.iter().copied().zip(&original_values) {
        assert_eq!(
            reader.read(row_id)?,
            Some(value.clone()),
            "the retained invocation snapshot changed under concurrent commits"
        );
    }
    reader.abort()?;

    let mut observer =
        InvocationTransaction::begin(&database, [0xd0; 16], [9; 16], 9_999, 8, 512 * 1024)?;
    for (writer_index, row_id) in row_ids.iter().copied().enumerate() {
        let expected_tag = 0x40u8.wrapping_add(
            (writer_index as u8)
                .wrapping_mul(WRITES_PER_WRITER)
                .wrapping_add(WRITES_PER_WRITER - 1),
        );
        let expected = vec![expected_tag; VALUE_BYTES];
        assert_eq!(observer.read(row_id)?, Some(expected));
    }
    observer.abort()?;
    Ok(())
}

pub(super) fn managed_cancellation_releases_snapshot_admission_during_write_pressure()
-> Result<(), Box<dyn std::error::Error>> {
    const WRITE_COUNT: u8 = 32;
    const VALUE_BYTES: usize = 32 * 1024;

    let fixture = Fixture::new();
    let data_directory = fixture.0.join("managed-cancellation");
    let mut bootstrap =
        prdb_engine::bootstrap::initialize(&data_directory, "cancel_admin", i64::MAX)?;
    let admin_user_id = bootstrap.admin_user_id();
    let mut secret = bootstrap.take_secret().ok_or("bootstrap secret missing")?;
    use zeroize::Zeroize;
    secret.zeroize();
    let instance = bootstrap.acknowledge()?;
    let database = instance.database();
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let retention_deadline_us = now + 60_000_000;
    let storage = prdb_engine::storage_manager::StorageManager::with_config(
        database,
        prdb_engine::storage_manager::StorageManagerConfig {
            max_active_invocations: 3,
            max_active_analytical_invocations: 1,
            ..Default::default()
        },
    )?;
    stage_initial_row(database)?;
    let mut retained =
        storage.begin_invocation([0x30; 16], admin_user_id, retention_deadline_us, 4, 64)?;
    let original_value = retained.read(id(1))?;
    let mut cancelled =
        storage.begin_invocation([0x31; 16], admin_user_id, retention_deadline_us, 4, 64)?;
    cancelled.stage_put(id(2), b"cancelled private write")?;
    let mut staged_writer =
        storage.begin_invocation([0x32; 16], admin_user_id, retention_deadline_us, 4, 64)?;
    staged_writer.stage_put(id(1), b"staged writer value")?;
    assert!(matches!(
        storage.begin_invocation([0x33; 16], admin_user_id, retention_deadline_us, 4, 64),
        Err(TransactionError::AdmissionLimitExceeded)
    ));

    let mut executor = cancelled.open_executor_metadata_connection();
    assert!(!executor.check_cancellation()?);
    let stop_monitoring = Arc::new(AtomicBool::new(false));
    let pages_during_cancellation = std::thread::scope(|scope| {
        let stop_monitoring_thread = Arc::clone(&stop_monitoring);
        let storage = &storage;
        let (monitoring_ready_tx, monitoring_ready_rx) = std::sync::mpsc::channel();
        let monitor = scope.spawn(move || {
            let mut pages = 0usize;
            let first_page = storage
                .list_runtime_invocations(
                    admin_user_id,
                    now + 1,
                    prdb_engine::storage_manager::MAX_ADMIN_INVOCATION_PAGE,
                    None,
                )
                .expect("first admin runtime page during cancellation");
            assert!(
                first_page
                    .invocations
                    .iter()
                    .any(|invocation| invocation.invocation_id == [0x31; 16])
            );
            pages += 1;
            monitoring_ready_tx
                .send(())
                .expect("signal first completed runtime page");
            while !stop_monitoring_thread.load(Ordering::Acquire) {
                let page = storage
                    .list_runtime_invocations(
                        admin_user_id,
                        now + 1,
                        prdb_engine::storage_manager::MAX_ADMIN_INVOCATION_PAGE,
                        None,
                    )
                    .expect("admin runtime page concurrent with cancellation");
                assert!(
                    page.invocations
                        .iter()
                        .any(|invocation| invocation.invocation_id == [0x31; 16])
                );
                pages += 1;
                std::thread::yield_now();
            }
            pages
        });
        monitoring_ready_rx
            .recv()
            .expect("runtime page finished before cancellation");
        let cancellation = storage.cancel_owned_invocation(admin_user_id, [0x31; 16], now + 1);
        stop_monitoring.store(true, Ordering::Release);
        let pages = monitor
            .join()
            .expect("runtime monitoring thread must finish");
        (cancellation, pages)
    });
    assert_eq!(
        pages_during_cancellation.0?,
        prdb_engine::storage_manager::OwnerCancellationResult::Accepted
    );
    assert!(
        pages_during_cancellation.1 > 1,
        "monitoring must continue after cancellation begins"
    );
    assert!(executor.check_cancellation()?);
    drop(executor);

    // The cancellation poll durably aborts and drops its snapshot permit. The
    // other reader and a staged writer still occupy two of the three slots.
    let mut replacement =
        storage.begin_invocation([0x33; 16], admin_user_id, retention_deadline_us, 4, 64)?;
    replacement.abort()?;
    assert_eq!(
        storage
            .lookup_invocation_outcome([0x31; 16], admin_user_id, now + 2)?
            .ok_or("cancelled outcome missing")?
            .status,
        OutcomeStatus::Aborted
    );
    staged_writer.commit()?;

    let completed = AtomicBool::new(false);
    let start = Arc::new(Barrier::new(2));
    let first_commit = Arc::new(Barrier::new(2));
    let samples = std::thread::scope(|scope| {
        let start_writer = Arc::clone(&start);
        let first_commit_writer = Arc::clone(&first_commit);
        let storage = &storage;
        let completed = &completed;
        let writer = scope.spawn(move || {
            start_writer.wait();
            for sequence in 0..WRITE_COUNT {
                let value = vec![0x40u8.wrapping_add(sequence); VALUE_BYTES];
                let mut invocation_id = [0; 16];
                invocation_id[0] = 0x70;
                invocation_id[1] = sequence;
                let mut transaction = storage
                    .begin_invocation(
                        invocation_id,
                        admin_user_id,
                        retention_deadline_us,
                        4,
                        VALUE_BYTES * 2,
                    )
                    .expect("admit pressure writer");
                transaction
                    .stage_put(id(1), &value)
                    .expect("stage pressure write");
                if sequence == 0 {
                    first_commit_writer.wait();
                }
                transaction.commit().expect("commit pressure write");
            }
            completed.store(true, Ordering::Release);
        });
        start.wait();
        assert_eq!(
            retained.read(id(1))?,
            original_value.clone(),
            "the retained snapshot changed before the first pressure commit"
        );
        let mut samples = 1;
        first_commit.wait();
        let first_monitoring_page = storage.list_runtime_invocations(
            admin_user_id,
            now + 3,
            prdb_engine::storage_manager::MAX_ADMIN_INVOCATION_PAGE,
            None,
        )?;
        assert!(
            first_monitoring_page
                .invocations
                .iter()
                .any(|invocation| invocation.invocation_id == [0x30; 16])
        );
        let mut monitoring_pages = 1;
        while !completed.load(Ordering::Acquire) && !writer.is_finished() {
            assert_eq!(
                retained.read(id(1))?,
                original_value.clone(),
                "cancellation or concurrent commits changed the retained snapshot"
            );
            let page = storage.list_runtime_invocations(
                admin_user_id,
                now + i64::from(samples + 3),
                prdb_engine::storage_manager::MAX_ADMIN_INVOCATION_PAGE,
                None,
            )?;
            assert!(
                page.invocations
                    .iter()
                    .any(|invocation| invocation.invocation_id == [0x30; 16])
            );
            monitoring_pages += 1;
            samples += 1;
            std::thread::yield_now();
        }
        writer.join().expect("pressure writer thread");
        Ok::<_, Box<dyn std::error::Error>>((samples, monitoring_pages))
    })?;
    assert!(samples.1 > 0, "monitoring did no work during the writes");
    assert!(
        samples.0 > 0,
        "the retained reader did no work during the writes"
    );

    assert_eq!(retained.read(id(1))?, original_value);
    retained.abort()?;
    let mut observer =
        storage.begin_invocation([0x34; 16], admin_user_id, retention_deadline_us, 4, 64)?;
    assert_eq!(
        observer.read(id(1))?,
        Some(vec![0x40u8.wrapping_add(WRITE_COUNT - 1); VALUE_BYTES])
    );
    assert_eq!(observer.read(id(2))?, None);
    observer.abort()?;
    drop(storage);
    drop(instance);
    Ok(())
}

pub(super) fn authorized_row_scan_pages_over_the_pinned_snapshot_and_private_overlay()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Text,
        nullable: false,
    }];
    let column_id = [0x22; 16];
    let column_ids = [column_id];
    let schema_id = [0x23; 16];
    let table_id = id(0).table;
    let first = encode_tuple(&columns, 1, &[Some(ScalarValue::Text("first".to_owned()))])?;
    let third = encode_tuple(&columns, 1, &[Some(ScalarValue::Text("third".to_owned()))])?;
    let mut seed = InvocationTransaction::begin(&database, [0x31; 16], [9; 16], 9_999, 8, 512)?;
    seed.stage_put(id(1), &first)?;
    seed.stage_put(id(3), &third)?;
    seed.commit()?;

    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (Operation::Select, AccessTarget::Table(table_id)),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id,
                },
            ),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema_id),
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id,
            },
        },
    ];
    let allow = Policy {
        subjects: vec![Subject::Principal([9; 16])],
        operation: Operation::Select,
        target: AccessTarget::Table(table_id),
        effect: Effect::Allow,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        with_check: None,
    };
    let cell_allow = Policy {
        target: AccessTarget::Column {
            table_id,
            column_id,
        },
        ..allow.clone()
    };
    let policies = [allow, cell_allow];
    let projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };

    let mut invocation =
        InvocationTransaction::begin(&database, [0x32; 16], [9; 16], 9_999, 8, 512)?;
    invocation.stage_delete(id(1))?;
    invocation.stage_put(
        id(2),
        &encode_tuple(&columns, 1, &[Some(ScalarValue::Text("second".to_owned()))])?,
    )?;
    invocation.stage_put(
        id(3),
        &encode_tuple(
            &columns,
            1,
            &[Some(ScalarValue::Text("changed".to_owned()))],
        )?,
    )?;
    commit_rows(
        &database,
        [0x33; 16],
        [9; 16],
        9_999,
        &[RowWrite {
            key: key(id(4)),
            expected_version: None,
            payload: Some(encode_tuple(
                &columns,
                1,
                &[Some(ScalarValue::Text("concurrent".to_owned()))],
            )?),
        }],
    )?;

    let first_page = invocation.scan_authorized(table_id, None, 2, &projection)?;
    assert!(first_page.continuation.is_some());
    assert_eq!(first_page.rows.len(), 1);
    assert_eq!(first_page.rows[0].0, id(2));
    assert_eq!(
        first_page.rows[0].1,
        vec![SanitizedCell::Value(ScalarValue::Text("second".to_owned()))]
    );

    let second_page =
        invocation.scan_authorized(table_id, first_page.continuation.as_ref(), 2, &projection)?;
    assert!(second_page.continuation.is_none());
    assert_eq!(second_page.rows.len(), 1);
    assert_eq!(second_page.rows[0].0, id(3));
    assert_eq!(
        second_page.rows[0].1,
        vec![SanitizedCell::Value(ScalarValue::Text(
            "changed".to_owned()
        ))]
    );
    invocation.abort()?;

    let mut active_cursor =
        InvocationTransaction::begin(&database, [0x35; 16], [9; 16], 9_999, 8, 512)?;
    let page = active_cursor.scan_authorized(table_id, None, 1, &projection)?;
    assert!(page.continuation.is_some());
    assert!(matches!(
        active_cursor.stage_put(id(5), &first),
        Err(TransactionError::ScanStillActive)
    ));

    let mut unclosed_cursor =
        InvocationTransaction::begin(&database, [0x36; 16], [9; 16], 9_999, 8, 512)?;
    let page = unclosed_cursor.scan_authorized(table_id, None, 1, &projection)?;
    assert!(page.continuation.is_some());
    assert!(matches!(
        unclosed_cursor.commit(),
        Err(TransactionError::ScanStillActive)
    ));

    let mut changed_authority =
        InvocationTransaction::begin(&database, [0x37; 16], [9; 16], 9_999, 8, 512)?;
    let page = changed_authority.scan_authorized(table_id, None, 1, &projection)?;
    let no_grants: [Grant; 0] = [];
    let changed_projection = AuthorizedProjection {
        grants: &no_grants,
        ..projection
    };
    assert!(matches!(
        changed_authority.scan_authorized(
            table_id,
            page.continuation.as_ref(),
            1,
            &changed_projection
        ),
        Err(TransactionError::InvalidScanRequest)
    ));

    Ok(())
}

pub(super) fn managed_scan_work_budget_is_cumulative_and_aborts_private_writes()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::storage_manager::{StorageManager, StorageManagerConfig};
    use zeroize::Zeroize;

    let fixture = Fixture::new();
    let data_directory = fixture.0.join("data");
    let mut bootstrap =
        prdb_engine::bootstrap::initialize(&data_directory, "scan_admin", i64::MAX)?;
    let admin_user_id = bootstrap.admin_user_id();
    let mut secret = bootstrap.take_secret().ok_or("bootstrap secret missing")?;
    secret.zeroize();
    let instance = bootstrap.acknowledge()?;
    let database = instance.database();
    let storage = StorageManager::with_config(
        database,
        StorageManagerConfig {
            max_invocation_scan_work: 2,
            ..StorageManagerConfig::default()
        },
    )?;
    let metadata_storage = StorageManager::with_config(
        database,
        StorageManagerConfig {
            max_invocation_scan_work: 1,
            ..StorageManagerConfig::default()
        },
    )?;

    let columns = [ColumnSchema {
        scalar_type: ScalarType::Text,
        nullable: false,
    }];
    let column_id = [0x22; 16];
    let column_ids = [column_id];
    let schema_id = [0x23; 16];
    let table_id = id(0).table;
    let mut seed = InvocationTransaction::begin(database, [0x31; 16], [9; 16], i64::MAX, 4, 512)?;
    for row_id in 1..=3 {
        let payload = encode_tuple(
            &columns,
            1,
            &[Some(ScalarValue::Text(format!("row-{row_id}")))],
        )?;
        seed.stage_put(id(row_id), &payload)?;
    }
    seed.commit()?;

    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (Operation::Select, AccessTarget::Table(table_id)),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id,
                },
            ),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema_id),
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id,
            },
        },
    ];
    let table_policy = Policy {
        subjects: vec![Subject::Principal([9; 16])],
        operation: Operation::Select,
        target: AccessTarget::Table(table_id),
        effect: Effect::Allow,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        with_check: None,
    };
    let cell_policy = Policy {
        target: AccessTarget::Column {
            table_id,
            column_id,
        },
        ..table_policy.clone()
    };
    let policies = [table_policy, cell_policy];
    let projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };

    let invocation_id = [0x38; 16];
    let mut invocation = storage.begin_invocation(invocation_id, [9; 16], i64::MAX, 4, 512)?;
    let staged_id = prdb_engine::overlay::RowId {
        table: [0x11; 16],
        row: [0x12; 16],
    };
    invocation.stage_put(staged_id, b"discard after the scan budget is exceeded")?;
    let first_page = invocation.scan_authorized(table_id, None, 1, &projection)?;
    assert!(first_page.continuation.is_some());
    let second_page =
        invocation.scan_authorized(table_id, first_page.continuation.as_ref(), 1, &projection)?;
    assert!(second_page.continuation.is_some());
    assert!(matches!(
        invocation.scan_authorized(table_id, second_page.continuation.as_ref(), 1, &projection,),
        Err(TransactionError::ScanBudgetExceeded)
    ));
    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Aborted)
    ));

    let read = database.begin_read()?;
    assert!(read.open_table(ROWS)?.get(&key(staged_id)[..])?.is_none());
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&invocation_id[..])?
                .ok_or("scan budget failure did not persist ABORTED")?
                .value(),
        )?
        .status,
        OutcomeStatus::Aborted
    );

    let metadata_invocation_id = [0x39; 16];
    let metadata_row = prdb_engine::overlay::RowId {
        table: [0x41; 16],
        row: [0x42; 16],
    };
    let mut metadata_invocation = metadata_storage.begin_invocation(
        metadata_invocation_id,
        admin_user_id,
        i64::MAX,
        4,
        512,
    )?;
    metadata_invocation.stage_put(metadata_row, b"discard after catalog scan budget")?;
    assert!(matches!(
        metadata_invocation.scan_information_schema_schemata(1, None),
        Err(TransactionError::ScanBudgetExceeded)
    ));
    assert!(matches!(
        metadata_invocation.commit(),
        Err(TransactionError::Aborted)
    ));
    let read = database.begin_read()?;
    assert!(
        read.open_table(ROWS)?
            .get(&key(metadata_row)[..])?
            .is_none()
    );
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&metadata_invocation_id[..])?
                .ok_or("catalog scan budget failure did not persist ABORTED")?
                .value(),
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}
