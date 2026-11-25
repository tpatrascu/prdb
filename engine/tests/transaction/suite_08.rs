use prdb_engine::audit::{AuditAction, AuditEvent, AuditResult};
use prdb_engine::authorization::{
    AccessTarget, AuthorizationContext, AuthorizedProjection, Effect, Expr, Grant, Operation,
    Policy, Subject,
};
use prdb_engine::catalog::{CatalogField, WireType};
use prdb_engine::commit::{
    CatalogWrite, CommitError, RowWrite, commit_catalog_and_audit, commit_rows,
};
use prdb_engine::metadata::{EngineMetadata, Lifecycle};
use prdb_engine::outcome::{InvocationOutcome, OutcomeStatus};
use prdb_engine::row_record::RowRecord;
use prdb_engine::schema::{CATALOG, ENGINE_META, ROWS};
use prdb_engine::transaction::{InsertInput, InvocationTransaction, TransactionError};
use prdb_engine::tuple::{
    ColumnSchema, FieldPatch, ScalarType, ScalarValue, decode_tuple, encode_tuple,
};
use redb::ReadableDatabase;
use std::sync::{Arc, Barrier};

use super::helpers::*;

pub(super) fn statement_error_aborts_earlier_writes_and_prevents_finalization()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    stage_initial_row(&database)?;

    let mut invocation = InvocationTransaction::begin(&database, [3; 16], [9; 16], 9_999, 4, 64)?;
    invocation.stage_put(id(1), b"must-abort")?;
    assert!(matches!(
        invocation.stage_delete(id(2)),
        Err(TransactionError::NotFound)
    ));
    assert!(matches!(
        invocation.read(id(1)),
        Err(TransactionError::Aborted)
    ));
    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Aborted)
    ));

    let read = database.begin_read()?;
    let rows = read.open_table(ROWS)?;
    let row = RowRecord::decode(rows.get(&key(id(1))[..])?.unwrap().value())?;
    assert_eq!(row.row_version, 1);
    assert_eq!(row.payload, b"snapshot");
    assert_eq!(
        EngineMetadata::decode(
            read.open_table(ENGINE_META)?
                .get(&b"format"[..])?
                .unwrap()
                .value()
        )?
        .commit_sequence,
        1
    );
    let aborted = read
        .open_table(prdb_engine::schema::OUTCOMES)?
        .get(&[3; 16][..])?
        .unwrap()
        .value()
        .to_vec();
    assert_eq!(
        InvocationOutcome::decode(&aborted)?,
        InvocationOutcome {
            status: OutcomeStatus::Aborted,
            invocation_id: [3; 16],
            principal_id: [9; 16],
            commit_sequence: 0,
            retention_deadline_us: 9_999,
        }
    );
    Ok(())
}

pub(super) fn admission_requires_ready_metadata_and_a_fresh_valid_invocation_id()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    stage_initial_row(&database)?;
    assert!(matches!(
        InvocationTransaction::begin(&database, [1; 16], [9; 16], 9_999, 1, 1),
        Err(TransactionError::DuplicateInvocation)
    ));
    assert!(matches!(
        InvocationTransaction::begin(&database, [0; 16], [9; 16], 9_999, 1, 1),
        Err(TransactionError::InvalidContext)
    ));

    let write = database.begin_write()?;
    let next = EngineMetadata {
        database_id: [0x33; 16],
        catalog_version: 2,
        expression_version: 1,
        builtin_semantics_version: 1,
        commit_sequence: 1,
        lifecycle: Lifecycle::Initializing,
    }
    .encode()?;
    write
        .open_table(ENGINE_META)?
        .insert(&b"format"[..], &next[..])?;
    write.commit()?;
    assert!(matches!(
        InvocationTransaction::begin(&database, [2; 16], [9; 16], 9_999, 1, 1),
        Err(TransactionError::NotReady)
    ));
    Ok(())
}

pub(super) fn concurrent_same_row_writers_have_one_commit_and_one_durable_conflict()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    stage_initial_row(&database)?;
    let barrier = Arc::new(Barrier::new(3));

    let (left, right) = std::thread::scope(|scope| {
        let left_barrier = barrier.clone();
        let left_database = &database;
        let left = scope.spawn(move || {
            let mut transaction =
                InvocationTransaction::begin(left_database, [10; 16], [9; 16], 9_999, 2, 32)?;
            transaction.stage_put(id(1), b"left")?;
            left_barrier.wait();
            transaction.commit()
        });
        let right_barrier = barrier.clone();
        let right_database = &database;
        let right = scope.spawn(move || {
            let mut transaction =
                InvocationTransaction::begin(right_database, [11; 16], [9; 16], 9_999, 2, 32)?;
            transaction.stage_put(id(1), b"right")?;
            right_barrier.wait();
            transaction.commit()
        });
        barrier.wait();
        (left.join().unwrap(), right.join().unwrap())
    });

    let outcomes = [left, right];
    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(result, Err(TransactionError::Commit(CommitError::Conflict))))
            .count(),
        1
    );
    assert_eq!(
        commit_sequence(&database)?,
        2,
        "only the committed writer advances the data sequence"
    );
    let read = database.begin_read()?;
    let row = RowRecord::decode(
        read.open_table(ROWS)?
            .get(&key(id(1))[..])?
            .unwrap()
            .value(),
    )?;
    assert_eq!(row.row_version, 2);
    assert!(row.payload == b"left" || row.payload == b"right");
    let outcomes = read.open_table(prdb_engine::schema::OUTCOMES)?;
    let left = InvocationOutcome::decode(outcomes.get(&[10; 16][..])?.unwrap().value())?;
    let right = InvocationOutcome::decode(outcomes.get(&[11; 16][..])?.unwrap().value())?;
    assert_eq!(
        [left.status, right.status]
            .iter()
            .filter(|status| **status == OutcomeStatus::Committed)
            .count(),
        1
    );
    assert_eq!(
        [left.status, right.status]
            .iter()
            .filter(|status| **status == OutcomeStatus::Aborted)
            .count(),
        1
    );
    Ok(())
}

pub(super) fn concurrent_authorized_updates_have_one_commit_and_one_durable_conflict()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_id = id(2);
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Integer,
        nullable: false,
    }];
    let column_ids = [[0x51; 16]];
    commit_rows(
        &database,
        [20; 16],
        [9; 16],
        9_999,
        &[RowWrite {
            key: key(row_id),
            expected_version: None,
            payload: Some(encode_tuple(
                &columns,
                1,
                &[Some(ScalarValue::Integer(10))],
            )?),
        }],
    )?;

    let table = AccessTarget::Table(row_id.table);
    let column = AccessTarget::Column {
        table_id: row_id.table,
        column_id: column_ids[0],
    };
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::from([[55; 16]]),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(row_id.table)),
            (Operation::Select, table),
            (Operation::Update, table),
            (Operation::Select, column),
            (Operation::Update, column),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Role([55; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(row_id.table),
        },
        Grant {
            subject: Subject::Role([55; 16]),
            operation: Operation::Select,
            target: table,
        },
        Grant {
            subject: Subject::Role([55; 16]),
            operation: Operation::Update,
            target: table,
        },
        Grant {
            subject: Subject::Role([55; 16]),
            operation: Operation::Select,
            target: column,
        },
        Grant {
            subject: Subject::Role([55; 16]),
            operation: Operation::Update,
            target: column,
        },
    ];
    let policies = [
        Policy {
            subjects: vec![Subject::Role([55; 16])],
            operation: Operation::Select,
            target: table,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Role([55; 16])],
            operation: Operation::Update,
            target: table,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
        Policy {
            subjects: vec![Subject::Role([55; 16])],
            operation: Operation::Select,
            target: column,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Role([55; 16])],
            operation: Operation::Update,
            target: column,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
    ];
    let projection = AuthorizedProjection {
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
    };
    let barrier = Arc::new(Barrier::new(3));
    let outcomes = std::thread::scope(|scope| {
        let left_barrier = barrier.clone();
        let left_database = &database;
        let left_projection = &projection;
        let left = scope.spawn(move || {
            let mut transaction =
                InvocationTransaction::begin(left_database, [21; 16], [9; 16], 9_999, 2, 128)?;
            transaction.stage_patch_authorized(
                row_id,
                left_projection,
                &[FieldPatch {
                    ordinal: 0,
                    value: Some(ScalarValue::Integer(20)),
                }],
            )?;
            left_barrier.wait();
            transaction.commit()
        });
        let right_barrier = barrier.clone();
        let right_database = &database;
        let right_projection = &projection;
        let right = scope.spawn(move || {
            let mut transaction =
                InvocationTransaction::begin(right_database, [22; 16], [9; 16], 9_999, 2, 128)?;
            transaction.stage_patch_authorized(
                row_id,
                right_projection,
                &[FieldPatch {
                    ordinal: 0,
                    value: Some(ScalarValue::Integer(30)),
                }],
            )?;
            right_barrier.wait();
            transaction.commit()
        });
        barrier.wait();
        [left.join().unwrap(), right.join().unwrap()]
    });

    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(result, Err(TransactionError::Commit(CommitError::Conflict))))
            .count(),
        1
    );
    assert_eq!(commit_sequence(&database)?, 2);
    let read = database.begin_read()?;
    let record = RowRecord::decode(
        read.open_table(ROWS)?
            .get(&key(row_id)[..])?
            .unwrap()
            .value(),
    )?;
    assert_eq!(record.row_version, 2);
    assert!(matches!(
        decode_tuple(&columns, 1, &record.payload)?
            .values
            .as_slice(),
        [Some(ScalarValue::Integer(20 | 30))]
    ));
    let outcome_table = read.open_table(prdb_engine::schema::OUTCOMES)?;
    let left = InvocationOutcome::decode(outcome_table.get(&[21; 16][..])?.unwrap().value())?;
    let right = InvocationOutcome::decode(outcome_table.get(&[22; 16][..])?.unwrap().value())?;
    assert_eq!(
        [left.status, right.status]
            .iter()
            .filter(|status| **status == OutcomeStatus::Committed)
            .count(),
        1
    );
    assert_eq!(
        [left.status, right.status]
            .iter()
            .filter(|status| **status == OutcomeStatus::Aborted)
            .count(),
        1
    );
    Ok(())
}

pub(super) fn snapshot_isolation_allows_documented_disjoint_row_write_skew()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    commit_rows(
        &database,
        [1; 16],
        [9; 16],
        9_999,
        &[
            RowWrite {
                key: key(id(1)),
                expected_version: None,
                payload: Some(b"on-call".to_vec()),
            },
            RowWrite {
                key: key(id(2)),
                expected_version: None,
                payload: Some(b"on-call".to_vec()),
            },
        ],
    )?;

    // Each transaction observes both people on call, then independently takes
    // one person off call. Their write sets are disjoint, so snapshot isolation
    // permits both commits even though the resulting state violates the
    // application invariant that at least one person remains on call.
    let barrier = Arc::new(Barrier::new(3));
    let (left, right) = std::thread::scope(|scope| {
        let left_barrier = barrier.clone();
        let left_database = &database;
        let left = scope.spawn(move || {
            let mut transaction =
                InvocationTransaction::begin(left_database, [12; 16], [9; 16], 9_999, 2, 32)?;
            assert_eq!(
                transaction.read(id(1))?.as_deref(),
                Some(b"on-call".as_slice())
            );
            assert_eq!(
                transaction.read(id(2))?.as_deref(),
                Some(b"on-call".as_slice())
            );
            transaction.stage_put(id(1), b"off-call")?;
            left_barrier.wait();
            transaction.commit()
        });
        let right_barrier = barrier.clone();
        let right_database = &database;
        let right = scope.spawn(move || {
            let mut transaction =
                InvocationTransaction::begin(right_database, [13; 16], [9; 16], 9_999, 2, 32)?;
            assert_eq!(
                transaction.read(id(1))?.as_deref(),
                Some(b"on-call".as_slice())
            );
            assert_eq!(
                transaction.read(id(2))?.as_deref(),
                Some(b"on-call".as_slice())
            );
            transaction.stage_put(id(2), b"off-call")?;
            right_barrier.wait();
            transaction.commit()
        });
        barrier.wait();
        (left.join().unwrap(), right.join().unwrap())
    });

    let mut commit_sequences = [left?, right?];
    commit_sequences.sort();
    assert_eq!(commit_sequences, [2, 3]);
    assert_eq!(commit_sequence(&database)?, 3);
    let read = database.begin_read()?;
    let rows = read.open_table(ROWS)?;
    for row_id in [id(1), id(2)] {
        let record = RowRecord::decode(rows.get(&key(row_id)[..])?.unwrap().value())?;
        assert_eq!(record.payload, b"off-call");
    }
    let outcomes = read.open_table(prdb_engine::schema::OUTCOMES)?;
    for invocation_id in [[12; 16], [13; 16]] {
        assert_eq!(
            InvocationOutcome::decode(outcomes.get(&invocation_id[..])?.unwrap().value())?.status,
            OutcomeStatus::Committed
        );
    }
    Ok(())
}

pub(super) fn field_patch_merges_with_pinned_tuple_and_preserves_untouched_values()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let columns = [
        ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        },
    ];
    let original = vec![
        Some(ScalarValue::Text("private unchanged cell".into())),
        Some(ScalarValue::Integer(10)),
        None,
    ];
    let payload = encode_tuple(&columns, 1, &original)?;
    commit_rows(
        &database,
        [1; 16],
        [9; 16],
        9_999,
        &[RowWrite {
            key: key(id(1)),
            expected_version: None,
            payload: Some(payload),
        }],
    )?;

    let mut transaction = InvocationTransaction::begin(&database, [2; 16], [9; 16], 9_999, 4, 64)?;
    transaction.stage_patch(
        id(1),
        &columns,
        1,
        &[FieldPatch {
            ordinal: 1,
            value: Some(ScalarValue::Integer(11)),
        }],
    )?;
    let staged_bytes = transaction.read(id(1))?.unwrap();
    let staged = decode_tuple(&columns, 1, &staged_bytes)?;
    assert_eq!(staged.values[0], original[0]);
    assert_eq!(staged.values[1], Some(ScalarValue::Integer(11)));
    assert_eq!(staged.values[2], None);
    transaction.commit()?;

    let read = database.begin_read()?;
    let bytes = RowRecord::decode(
        read.open_table(ROWS)?
            .get(&key(id(1))[..])?
            .unwrap()
            .value(),
    )?;
    let committed = decode_tuple(&columns, 1, &bytes.payload)?;
    assert_eq!(committed.values[0], original[0]);
    assert_eq!(committed.values[1], Some(ScalarValue::Integer(11)));
    assert_eq!(committed.values[2], None);
    assert_eq!(committed.schema_version, 1);
    Ok(())
}

pub(super) fn authorized_patch_checks_changed_cells_and_freezes_policy_dependencies()
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
    let column_ids = [[51; 16], [52; 16], [53; 16]];
    let row_id = id(50);
    let second_row_id = prdb_engine::overlay::RowId {
        table: row_id.table,
        row: [0x51; 16],
    };
    let original = [
        Some(ScalarValue::Text("Alice".to_owned())),
        Some(ScalarValue::Uuid([9; 16])),
        Some(ScalarValue::Text("hidden unchanged".to_owned())),
    ];
    commit_rows(
        &database,
        [54; 16],
        [9; 16],
        9_999,
        &[
            RowWrite {
                key: key(row_id),
                expected_version: None,
                payload: Some(encode_tuple(&columns, 1, &original)?),
            },
            RowWrite {
                key: key(second_row_id),
                expected_version: None,
                payload: Some(encode_tuple(&columns, 1, &original)?),
            },
        ],
    )?;
    let table_target = AccessTarget::Table(row_id.table);
    let name_target = AccessTarget::Column {
        table_id: row_id.table,
        column_id: column_ids[0],
    };
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::from([[55; 16]]),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(row_id.table)),
            (Operation::Select, table_target),
            (Operation::Update, table_target),
            (Operation::Select, name_target),
            (Operation::Update, name_target),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Role([55; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(row_id.table),
        },
        Grant {
            subject: Subject::Role([55; 16]),
            operation: Operation::Select,
            target: table_target,
        },
        Grant {
            subject: Subject::Role([55; 16]),
            operation: Operation::Update,
            target: table_target,
        },
        Grant {
            subject: Subject::Role([55; 16]),
            operation: Operation::Select,
            target: name_target,
        },
        Grant {
            subject: Subject::Role([55; 16]),
            operation: Operation::Update,
            target: name_target,
        },
    ];
    let policies = [
        Policy {
            subjects: vec![Subject::Role([55; 16])],
            operation: Operation::Select,
            target: table_target,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Role([55; 16])],
            operation: Operation::Update,
            target: table_target,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
        Policy {
            subjects: vec![Subject::Role([55; 16])],
            operation: Operation::Select,
            target: name_target,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Role([55; 16])],
            operation: Operation::Update,
            target: name_target,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
    ];
    let projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id: row_id.table,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[Expr::NotEqual(
            Box::new(Expr::Field(0)),
            Box::new(Expr::Literal(Some(ScalarValue::Text(
                "forbidden".to_owned(),
            )))),
        )],
        policy_driving_columns: &[column_ids[1]],
        dependencies: &[],
    };
    let hidden_row_policies = [
        Policy {
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(false)))),
            ..policies[0].clone()
        },
        policies[1].clone(),
        policies[2].clone(),
        policies[3].clone(),
    ];
    let hidden_row_projection = AuthorizedProjection {
        policies: &hidden_row_policies,
        ..projection
    };
    let absent_row_id = prdb_engine::overlay::RowId {
        table: row_id.table,
        row: [0xef; 16],
    };
    let update_patch = [FieldPatch {
        ordinal: 0,
        value: Some(ScalarValue::Text("attempted update".to_owned())),
    }];
    let mut hidden_row_update =
        InvocationTransaction::begin(&database, [59; 16], [9; 16], 9_999, 2, 128)?;
    let hidden_update_error = hidden_row_update
        .stage_patch_authorized(row_id, &hidden_row_projection, &update_patch)
        .expect_err("a row hidden by its SELECT policy cannot be updated");
    let mut absent_row_update =
        InvocationTransaction::begin(&database, [60; 16], [9; 16], 9_999, 2, 128)?;
    let absent_update_error = absent_row_update
        .stage_patch_authorized(absent_row_id, &hidden_row_projection, &update_patch)
        .expect_err("an absent row cannot be updated");
    assert_eq!(
        format!("{hidden_update_error:?}"),
        format!("{absent_update_error:?}"),
        "UPDATE diagnostics must not distinguish a policy-hidden row from an absent row"
    );
    assert!(matches!(hidden_update_error, TransactionError::NotFound));
    assert!(matches!(absent_update_error, TransactionError::NotFound));

    let mut invocation = InvocationTransaction::begin(&database, [56; 16], [9; 16], 9_999, 2, 128)?;
    invocation.stage_patch_authorized(
        row_id,
        &projection,
        &[FieldPatch {
            ordinal: 0,
            value: Some(ScalarValue::Text("Alice updated".to_owned())),
        }],
    )?;
    invocation.commit()?;

    let mut constraint_denied =
        InvocationTransaction::begin(&database, [58; 16], [9; 16], 9_999, 2, 128)?;
    let first_constraint_error = constraint_denied
        .stage_patch_authorized(
            row_id,
            &projection,
            &[FieldPatch {
                ordinal: 0,
                value: Some(ScalarValue::Text("forbidden".to_owned())),
            }],
        )
        .expect_err("the CHECK expression must reject its first forbidden value");
    let alternate_checks = [Expr::NotEqual(
        Box::new(Expr::Field(0)),
        Box::new(Expr::Literal(Some(ScalarValue::Text(
            "classified".to_owned(),
        )))),
    )];
    let alternate_projection = AuthorizedProjection {
        checks: &alternate_checks,
        ..projection
    };
    let mut second_constraint_denied =
        InvocationTransaction::begin(&database, [0x5a; 16], [9; 16], 9_999, 2, 128)?;
    let second_constraint_error = second_constraint_denied
        .stage_patch_authorized(
            second_row_id,
            &alternate_projection,
            &[FieldPatch {
                ordinal: 0,
                value: Some(ScalarValue::Text("classified".to_owned())),
            }],
        )
        .expect_err("the CHECK expression must reject its second forbidden value");
    assert_eq!(
        format!("{first_constraint_error:?}"),
        format!("{second_constraint_error:?}"),
        "CHECK diagnostics must not include rejected values or row identities"
    );
    assert!(matches!(
        first_constraint_error,
        TransactionError::ConstraintViolation
    ));
    assert!(matches!(
        second_constraint_error,
        TransactionError::ConstraintViolation
    ));

    let mut denied = InvocationTransaction::begin(&database, [57; 16], [9; 16], 9_999, 2, 128)?;
    assert!(matches!(
        denied.stage_patch_authorized(
            row_id,
            &projection,
            &[FieldPatch {
                ordinal: 1,
                value: Some(ScalarValue::Uuid([8; 16])),
            }]
        ),
        Err(TransactionError::Authorization(
            prdb_engine::authorization::AuthorizationError::Denied
        ))
    ));
    let read = database.begin_read()?;
    let record = RowRecord::decode(
        read.open_table(ROWS)?
            .get(&key(row_id)[..])?
            .unwrap()
            .value(),
    )?;
    let updated = decode_tuple(&columns, 1, &record.payload)?;
    assert_eq!(
        updated.values,
        [
            Some(ScalarValue::Text("Alice updated".to_owned())),
            Some(ScalarValue::Uuid([9; 16])),
            Some(ScalarValue::Text("hidden unchanged".to_owned())),
        ]
    );
    Ok(())
}

pub(super) fn authorized_delete_requires_visibility_and_an_explicit_delete_grant()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_id = id(60);
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Text,
        nullable: false,
    }];
    let column_ids = [[61; 16]];
    commit_rows(
        &database,
        [62; 16],
        [9; 16],
        9_999,
        &[RowWrite {
            key: key(row_id),
            expected_version: None,
            payload: Some(encode_tuple(
                &columns,
                1,
                &[Some(ScalarValue::Text("visible but not deletable".into()))],
            )?),
        }],
    )?;
    let target = AccessTarget::Table(row_id.table);
    let cell_target = AccessTarget::Column {
        table_id: row_id.table,
        column_id: column_ids[0],
    };
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::from([[63; 16]]),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(row_id.table)),
            (Operation::Select, target),
            (Operation::Delete, target),
            (Operation::Select, cell_target),
        ]),
    };
    let row_select_policy = Policy {
        subjects: vec![Subject::Role([63; 16])],
        operation: Operation::Select,
        target,
        effect: Effect::Allow,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        with_check: None,
    };
    let cell_select_policy = Policy {
        subjects: vec![Subject::Role([63; 16])],
        operation: Operation::Select,
        target: cell_target,
        effect: Effect::Allow,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        with_check: None,
    };
    let delete_policy = Policy {
        subjects: vec![Subject::Role([63; 16])],
        operation: Operation::Delete,
        target,
        effect: Effect::Allow,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        with_check: None,
    };
    let select_grant = Grant {
        subject: Subject::Role([63; 16]),
        operation: Operation::Select,
        target: cell_target,
    };
    let usage_grant = Grant {
        subject: Subject::Role([63; 16]),
        operation: Operation::Usage,
        target: AccessTarget::Schema(row_id.table),
    };
    let select_only_grants = [select_grant, usage_grant];
    let denied_policies = [
        row_select_policy.clone(),
        cell_select_policy.clone(),
        delete_policy.clone(),
    ];
    let denied_projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id: row_id.table,
        context: &context,
        grants: &select_only_grants,
        policies: &denied_policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let mut denied = InvocationTransaction::begin(&database, [64; 16], [9; 16], 9_999, 2, 64)?;
    assert!(matches!(
        denied.stage_delete_authorized(row_id, &denied_projection),
        Err(TransactionError::Authorization(
            prdb_engine::authorization::AuthorizationError::Denied
        ))
    ));

    let delete_grant = Grant {
        subject: Subject::Role([63; 16]),
        operation: Operation::Delete,
        target,
    };
    let mut allowed = InvocationTransaction::begin(&database, [65; 16], [9; 16], 9_999, 2, 64)?;
    let allowed_grants = [select_grant, delete_grant, usage_grant];
    let allowed_policies = [row_select_policy, cell_select_policy, delete_policy];
    let allowed_projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id: row_id.table,
        context: &context,
        grants: &allowed_grants,
        policies: &allowed_policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };

    let hidden_row_policies = [
        Policy {
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(false)))),
            ..allowed_policies[0].clone()
        },
        allowed_policies[1].clone(),
        allowed_policies[2].clone(),
    ];
    let hidden_row_projection = AuthorizedProjection {
        policies: &hidden_row_policies,
        ..allowed_projection
    };
    let hidden_row_id = prdb_engine::overlay::RowId {
        table: row_id.table,
        row: row_id.row,
    };
    let absent_row_id = prdb_engine::overlay::RowId {
        table: row_id.table,
        row: [0xee; 16],
    };
    let mut hidden_row_delete =
        InvocationTransaction::begin(&database, [66; 16], [9; 16], 9_999, 2, 64)?;
    let hidden_row_error = hidden_row_delete
        .stage_delete_authorized(hidden_row_id, &hidden_row_projection)
        .expect_err("a row hidden by its SELECT policy cannot be deleted");
    let mut absent_row_delete =
        InvocationTransaction::begin(&database, [67; 16], [9; 16], 9_999, 2, 64)?;
    let absent_row_error = absent_row_delete
        .stage_delete_authorized(absent_row_id, &hidden_row_projection)
        .expect_err("an absent row cannot be deleted");
    assert_eq!(
        format!("{hidden_row_error:?}"),
        format!("{absent_row_error:?}"),
        "DELETE diagnostics must not distinguish a policy-hidden row from an absent row"
    );
    assert!(matches!(hidden_row_error, TransactionError::NotFound));
    assert!(matches!(absent_row_error, TransactionError::NotFound));

    allowed.stage_delete_authorized(row_id, &allowed_projection)?;
    allowed.commit()?;
    let read = database.begin_read()?;
    assert!(read.open_table(ROWS)?.get(&key(row_id)[..])?.is_none());
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&[64; 16][..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

pub(super) fn authorized_insert_materializes_defaults_before_policy_and_check_validation()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_id = id(110);
    let column_id = [0x66; 16];
    let table = AccessTarget::Table(row_id.table);
    let schema = AccessTarget::Schema(row_id.table);
    let column = AccessTarget::Column {
        table_id: row_id.table,
        column_id,
    };
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::from([[114; 16]]),
        scope: std::collections::HashSet::from([
            (Operation::Usage, schema),
            (Operation::Insert, table),
            (Operation::Insert, column),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Role([114; 16]),
            operation: Operation::Usage,
            target: schema,
        },
        Grant {
            subject: Subject::Role([114; 16]),
            operation: Operation::Insert,
            target: table,
        },
        Grant {
            subject: Subject::Role([114; 16]),
            operation: Operation::Insert,
            target: column,
        },
    ];
    let policies = [
        Policy {
            subjects: vec![Subject::Role([114; 16])],
            operation: Operation::Insert,
            target: table,
            effect: Effect::Allow,
            using: None,
            with_check: Some(Expr::Equal(
                Box::new(Expr::Field(0)),
                Box::new(Expr::CurrentPrincipalId),
            )),
        },
        Policy {
            subjects: vec![Subject::Role([114; 16])],
            operation: Operation::Insert,
            target: column,
            effect: Effect::Allow,
            using: None,
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
    ];
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Uuid,
        nullable: false,
    }];
    let column_ids = [column_id];
    let checks = [Expr::Equal(
        Box::new(Expr::Field(0)),
        Box::new(Expr::Literal(Some(ScalarValue::Uuid([9; 16])))),
    )];
    let projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id: row_id.table,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &checks,
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let mut invalid_default =
        InvocationTransaction::begin(&database, [115; 16], [9; 16], 9_999, 1, 128)?;
    assert!(matches!(
        invalid_default.stage_insert_authorized_with_defaults(
            id(116),
            &projection,
            &[InsertInput::Default],
            &[Some(Expr::Field(0))],
            &[],
        ),
        Err(TransactionError::Authorization(
            prdb_engine::authorization::AuthorizationError::InvalidPolicy
        ))
    ));
    let default_expression_id = [117; 16];
    let check_expression_id = [121; 16];
    let boolean_type_id = [46; 16];
    let mut default_expression_key = 7u16.to_be_bytes().to_vec();
    default_expression_key.push(1);
    default_expression_key.extend_from_slice(&default_expression_id);
    let mut check_expression_key = 7u16.to_be_bytes().to_vec();
    check_expression_key.push(1);
    check_expression_key.extend_from_slice(&check_expression_id);
    let default_expression =
        expression_catalog_payload(default_expression_id, &Expr::CurrentPrincipalId)?;
    let type_id = [45; 16];
    let schema_id = row_id.table;
    let storage_id = [145; 16];
    let type_key = catalog_id_key(4, type_id);
    let table_key = catalog_id_key(5, row_id.table);
    let column_key = catalog_id_key(6, column_id);
    let type_payload = catalog_record_payload(
        4,
        &type_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::U16,
                payload: 9u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: 0u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::U32,
                payload: 7u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
        ],
    )?;
    let table_payload = |schema_version: u64| {
        catalog_record_payload(
            5,
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
                    payload: schema_version.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::U64,
                    payload: 1u64.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 4,
                    wire_type: WireType::Id,
                    payload: storage_id.to_vec(),
                },
                CatalogField {
                    tag: 5,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
            ],
        )
    };
    let initial_table_payload = table_payload(1u64)?;
    let updated_table_payload = table_payload(2u64)?;
    let column_payload = catalog_record_payload(
        6,
        &column_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: row_id.table.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U32,
                payload: 0u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Text,
                payload: format!("object_{}", column_id[0]).into_bytes(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Id,
                payload: type_id.to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::Bytes,
                payload: vec![0; 8],
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Bool,
                payload: vec![0],
            },
            CatalogField {
                tag: 7,
                wire_type: WireType::Id,
                payload: default_expression_id.to_vec(),
            },
            CatalogField {
                tag: 8,
                wire_type: WireType::U16,
                payload: 0u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 9,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
        ],
    )?;
    let initial_column_payload = catalog_record_payload(
        6,
        &column_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: row_id.table.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U32,
                payload: 0u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Text,
                payload: format!("object_{}", column_id[0]).into_bytes(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Id,
                payload: [0x40; 16].to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::Bytes,
                payload: vec![0; 8],
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Bool,
                payload: vec![1],
            },
            CatalogField {
                tag: 8,
                wire_type: WireType::U16,
                payload: 0u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 9,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
        ],
    )?;
    let check_expression = expression_catalog_payload_full(
        check_expression_id,
        2,
        boolean_type_id,
        [0; 8],
        &[column_ids[0]],
        &Expr::Equal(
            Box::new(Expr::Field(0)),
            Box::new(Expr::Literal(Some(ScalarValue::Uuid([9; 16])))),
        ),
    )?;
    let default_audit = AuditEvent {
        event_id: [119; 16],
        actor_id: Some([9; 16]),
        correlation_id: [120; 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![
            row_id.table,
            type_id,
            column_id,
            default_expression_id,
            check_expression_id,
        ],
    };
    let mut catalog_writes = vec![
        CatalogWrite {
            key: default_expression_key,
            expected_value: None,
            payload: Some(default_expression),
        },
        CatalogWrite {
            key: check_expression_key,
            expected_value: None,
            payload: Some(check_expression),
        },
        CatalogWrite {
            key: type_key,
            expected_value: None,
            payload: Some(type_payload),
        },
        boolean_type_catalog_write(boolean_type_id)?,
        CatalogWrite {
            key: table_key,
            expected_value: Some(initial_table_payload),
            payload: Some(updated_table_payload),
        },
        CatalogWrite {
            key: column_key,
            expected_value: Some(initial_column_payload),
            payload: Some(column_payload),
        },
        object_catalog_write_for(default_expression_id, 5),
        object_catalog_write_for(check_expression_id, 5),
        object_catalog_write_for(type_id, 2),
        object_catalog_write_for(boolean_type_id, 2),
        dependency_catalog_write(column_id, default_expression_id, 2)?,
        dependency_catalog_write(check_expression_id, column_id, 2)?,
    ];
    {
        let read = database.begin_read()?;
        let catalog = read.open_table(CATALOG)?;
        for index_id in [[0x55; 16], [1; 16], [2; 16]] {
            let (key, payload) = index_catalog_record(index_id, row_id.table, column_id, 2)?;
            let expected_value = catalog
                .get(key.as_slice())?
                .ok_or("missing test index definition")?
                .value()
                .to_vec();
            catalog_writes.push(object_catalog_write_for(index_id, 7));
            catalog_writes.push(CatalogWrite {
                key,
                expected_value: Some(expected_value),
                payload: Some(payload),
            });
        }
    }
    commit_catalog_and_audit(
        &database,
        [118; 16],
        [9; 16],
        9_999,
        &catalog_writes,
        &[default_audit],
    )?;
    let mut invocation =
        InvocationTransaction::begin(&database, [113; 16], [9; 16], 9_999, 1, 512)?;
    let catalog_schema = invocation.load_catalog_table_schema(row_id.table)?;
    invocation.load_catalog_indexes(&catalog_schema)?;
    let catalog_checks = invocation.read_catalog_bound_expressions(
        &[check_expression_id],
        prdb_engine::expression::ExpressionPurpose::Check,
        &catalog_schema.columns,
        &catalog_schema.column_ids,
        &[[46; 16]],
        &[ScalarType::Boolean],
    )?;
    let catalog_projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: catalog_schema.schema_version,
        schema_id: row_id.table,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &catalog_checks,
        policy_driving_columns: &[],
        dependencies: &[],
    };
    invocation.stage_insert_authorized_from_catalog_defaults(
        row_id,
        &catalog_projection,
        &[InsertInput::Default],
        &catalog_schema,
    )?;
    invocation.commit()?;

    let missing_column_grant = AuthorizedProjection {
        columns: catalog_projection.columns,
        column_ids: catalog_projection.column_ids,
        schema_version: catalog_projection.schema_version,
        schema_id: catalog_projection.schema_id,
        context: catalog_projection.context,
        grants: &grants[..2],
        policies: catalog_projection.policies,
        checks: catalog_projection.checks,
        policy_driving_columns: catalog_projection.policy_driving_columns,
        dependencies: catalog_projection.dependencies,
    };
    let denied_row_id = id(122);
    let mut denied_insert =
        InvocationTransaction::begin(&database, [123; 16], [9; 16], 9_999, 1, 512)?;
    assert!(matches!(
        denied_insert.stage_insert_authorized_from_catalog_defaults(
            denied_row_id,
            &missing_column_grant,
            &[InsertInput::Default],
            &catalog_schema,
        ),
        Err(TransactionError::Authorization(
            prdb_engine::authorization::AuthorizationError::Denied
        ))
    ));
    assert!(matches!(
        denied_insert.commit(),
        Err(TransactionError::Aborted)
    ));

    let read = database.begin_read()?;
    let record = RowRecord::decode(
        read.open_table(ROWS)?
            .get(&key(row_id)[..])?
            .unwrap()
            .value(),
    )?;
    assert_eq!(
        decode_tuple(&columns, catalog_schema.schema_version, &record.payload)?.values,
        [Some(ScalarValue::Uuid([9; 16]))]
    );
    assert!(read.open_table(ROWS)?.get(&key(id(116))[..])?.is_none());
    assert!(
        read.open_table(ROWS)?
            .get(&key(denied_row_id)[..])?
            .is_none()
    );
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&[123; 16][..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&[115; 16][..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

pub(super) fn authorized_insert_implicit_null_requires_column_insert_grant()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_id = [0x31; 16];
    let schema_id = [0x32; 16];
    let principal_id = [0x33; 16];
    let value_column_id = [0x34; 16];
    let nullable_column_id = [0x35; 16];
    let table = AccessTarget::Table(table_id);
    let schema = AccessTarget::Schema(schema_id);
    let value_column = AccessTarget::Column {
        table_id,
        column_id: value_column_id,
    };
    let nullable_column = AccessTarget::Column {
        table_id,
        column_id: nullable_column_id,
    };
    let context = AuthorizationContext {
        principal_id,
        role_ids: Default::default(),
        scope: [
            (Operation::Usage, schema),
            (Operation::Insert, table),
            (Operation::Insert, value_column),
            (Operation::Insert, nullable_column),
        ]
        .into(),
    };
    let grants = [
        Grant {
            subject: Subject::Principal(principal_id),
            operation: Operation::Usage,
            target: schema,
        },
        Grant {
            subject: Subject::Principal(principal_id),
            operation: Operation::Insert,
            target: table,
        },
        Grant {
            subject: Subject::Principal(principal_id),
            operation: Operation::Insert,
            target: value_column,
        },
        Grant {
            subject: Subject::Principal(principal_id),
            operation: Operation::Insert,
            target: nullable_column,
        },
    ];
    let policies = [
        Policy {
            subjects: vec![Subject::Principal(principal_id)],
            operation: Operation::Insert,
            target: table,
            effect: Effect::Allow,
            using: None,
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
        Policy {
            subjects: vec![Subject::Principal(principal_id)],
            operation: Operation::Insert,
            target: value_column,
            effect: Effect::Allow,
            using: None,
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
        Policy {
            subjects: vec![Subject::Principal(principal_id)],
            operation: Operation::Insert,
            target: nullable_column,
            effect: Effect::Allow,
            using: None,
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
    ];
    let columns = [
        ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: true,
        },
    ];
    let column_ids = [value_column_id, nullable_column_id];
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
    let insert_inputs = [
        InsertInput::Value(Some(ScalarValue::Integer(7))),
        InsertInput::Default,
    ];
    let defaults = [None, None];
    let inserted = prdb_engine::overlay::RowId {
        table: table_id,
        row: [0x36; 16],
    };
    let mut allowed =
        InvocationTransaction::begin(&database, [0x37; 16], principal_id, 9_999, 2, 128)?;
    allowed.stage_insert_authorized_with_defaults(
        inserted,
        &projection,
        &insert_inputs,
        &defaults,
        &[],
    )?;
    allowed.commit()?;

    let denied = prdb_engine::overlay::RowId {
        table: table_id,
        row: [0x38; 16],
    };
    let projection_without_nullable_column_grant = AuthorizedProjection {
        grants: &grants[..3],
        ..projection
    };
    let mut denied_insert =
        InvocationTransaction::begin(&database, [0x39; 16], principal_id, 9_999, 2, 128)?;
    assert!(matches!(
        denied_insert.stage_insert_authorized_with_defaults(
            denied,
            &projection_without_nullable_column_grant,
            &insert_inputs,
            &defaults,
            &[],
        ),
        Err(TransactionError::Authorization(
            prdb_engine::authorization::AuthorizationError::Denied
        ))
    ));
    assert!(matches!(
        denied_insert.commit(),
        Err(TransactionError::Aborted)
    ));

    let read = database.begin_read()?;
    let rows = read.open_table(ROWS)?;
    let stored = RowRecord::decode(
        rows.get(&key(inserted)[..])?
            .ok_or("authorized implicit-NULL insert is missing")?
            .value(),
    )?;
    assert_eq!(
        decode_tuple(&columns, 1, &stored.payload)?.values,
        [Some(ScalarValue::Integer(7)), None]
    );
    assert!(rows.get(&key(denied)[..])?.is_none());
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&[0x39; 16][..])?
                .ok_or("implicit-NULL denial outcome is missing")?
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

pub(super) fn catalog_insert_allocates_generated_identity_and_rejects_caller_override()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_id = id(212);
    let column_id = [213; 16];
    let table = AccessTarget::Table(row_id.table);
    let schema_target = AccessTarget::Schema(row_id.table);
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: Default::default(),
        scope: [
            (Operation::Usage, schema_target),
            (Operation::Insert, table),
        ]
        .into(),
    };
    let grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: schema_target,
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Insert,
            target: table,
        },
    ];
    let policies = [Policy {
        subjects: vec![Subject::Principal([9; 16])],
        operation: Operation::Insert,
        target: table,
        effect: Effect::Allow,
        using: None,
        with_check: Some(Expr::NotEqual(
            Box::new(Expr::Field(0)),
            Box::new(Expr::Literal(Some(ScalarValue::Uuid([0; 16])))),
        )),
    }];
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Uuid,
        nullable: false,
    }];
    let column_ids = [column_id];
    let projection = AuthorizedProjection {
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
    };
    let schema = prdb_engine::transaction::CatalogTableSchema {
        table_id: row_id.table,
        schema_id: row_id.table,
        schema_version: 1,
        write_epoch: 1,
        storage_object_id: [214; 16],
        columns: columns.to_vec(),
        column_ids: column_ids.to_vec(),
        column_type_ids: vec![[215; 16]],
        column_default_masks: vec![7],
        default_expression_ids: vec![None],
        generation_modes: vec![1],
    };
    let mut override_invocation =
        InvocationTransaction::begin(&database, [216; 16], [9; 16], 9_999, 2, 128)?;
    assert!(matches!(
        override_invocation.stage_insert_authorized_from_catalog_defaults(
            row_id,
            &projection,
            &[InsertInput::Value(Some(ScalarValue::Uuid([217; 16])))],
            &schema,
        ),
        Err(TransactionError::InvalidContext)
    ));
    assert!(matches!(
        override_invocation.commit(),
        Err(TransactionError::Aborted)
    ));

    let mut invocation =
        InvocationTransaction::begin(&database, [218; 16], [9; 16], 9_999, 2, 128)?;
    invocation.stage_insert_authorized_from_catalog_defaults(
        row_id,
        &projection,
        &[InsertInput::Default],
        &schema,
    )?;
    let payload = invocation.read(row_id)?.expect("generated row is staged");
    let tuple = decode_tuple(&columns, 1, &payload)?;
    assert!(matches!(
        tuple.values.first(),
        Some(Some(ScalarValue::Uuid(value))) if *value != [0; 16]
    ));
    invocation.abort()?;
    assert_eq!(
        prdb_engine::outcome::lookup_outcome(&database, [218; 16], [9; 16], 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    let read = database.begin_read()?;
    assert!(read.open_table(ROWS)?.get(&key(row_id)[..])?.is_none());
    Ok(())
}
