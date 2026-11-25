use prdb_engine::audit::{AuditAction, AuditEvent, AuditResult};
use prdb_engine::authorization::{
    AccessTarget, AuthorityDependency, AuthorizationContext, AuthorizedProjection, Effect, Expr,
    Grant, Operation, Policy, Subject,
};
use prdb_engine::commit::{
    CatalogWrite, CommitError, RowWrite, commit_catalog_and_audit, commit_rows,
};
use prdb_engine::row_record::RowRecord;
use prdb_engine::schema::{CATALOG, PRINCIPAL_NAMES, ROWS};
use prdb_engine::transaction::{InvocationTransaction, TransactionError};
use prdb_engine::tuple::{
    ColumnSchema, FieldPatch, ScalarType, ScalarValue, decode_tuple, encode_tuple,
};
use redb::ReadableDatabase;

use super::helpers::*;

pub(super) fn authorized_write_revalidates_projection_catalog_dependencies()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_id = id(80);
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Text,
        nullable: false,
    }];
    let column_ids = [[81; 16]];
    commit_rows(
        &database,
        [82; 16],
        [9; 16],
        9_999,
        &[RowWrite {
            key: key(row_id),
            expected_version: None,
            payload: Some(encode_tuple(
                &columns,
                1,
                &[Some(ScalarValue::Text("before".to_owned()))],
            )?),
        }],
    )?;
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

    let table = AccessTarget::Table(row_id.table);
    let cell = AccessTarget::Column {
        table_id: row_id.table,
        column_id: column_ids[0],
    };
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::from([[83; 16]]),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(row_id.table)),
            (Operation::Select, table),
            (Operation::Update, table),
            (Operation::Select, cell),
            (Operation::Update, cell),
        ]),
    };
    let column_grants = [Operation::Select, Operation::Update].map(|operation| Grant {
        subject: Subject::Role([83; 16]),
        operation,
        target: cell,
    });
    let mut policies = Vec::new();
    for operation in [Operation::Select, Operation::Update] {
        policies.push(Policy {
            subjects: vec![Subject::Role([83; 16])],
            operation,
            target: table,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: (operation == Operation::Update)
                .then_some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        });
        policies.push(Policy {
            subjects: vec![Subject::Role([83; 16])],
            operation,
            target: cell,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: (operation == Operation::Update)
                .then_some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        });
    }
    let select_table_grant = Grant {
        subject: Subject::Role([83; 16]),
        operation: Operation::Select,
        target: table,
    };
    let update_table_grant = Grant {
        subject: Subject::Role([83; 16]),
        operation: Operation::Update,
        target: table,
    };
    let usage_grant = Grant {
        subject: Subject::Role([83; 16]),
        operation: Operation::Usage,
        target: AccessTarget::Schema(row_id.table),
    };
    let grants = [
        column_grants[0],
        column_grants[1],
        select_table_grant,
        update_table_grant,
        usage_grant,
    ];
    let dependencies = [AuthorityDependency {
        key: principal_key.clone(),
        expected_value: Some(principal_v1.clone()),
    }];
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
        dependencies: &dependencies,
    };
    let mut invocation = InvocationTransaction::begin(&database, [84; 16], [9; 16], 9_999, 2, 128)?;
    invocation.stage_patch_authorized(
        row_id,
        &projection,
        &[FieldPatch {
            ordinal: 0,
            value: Some(ScalarValue::Text("stale authorization".to_owned())),
        }],
    )?;

    let (_, principal_v2) = principal_catalog_record(2);
    let authority_change = AuditEvent {
        event_id: [85; 16],
        actor_id: Some([9; 16]),
        correlation_id: [86; 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::AuthorityChange,
        result: AuditResult::Succeeded,
        target_ids: vec![[7; 16]],
    };
    commit_catalog_and_audit(
        &database,
        [87; 16],
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
    let read = database.begin_read()?;
    let tuple = decode_tuple(
        &columns,
        1,
        &RowRecord::decode(
            read.open_table(ROWS)?
                .get(&key(row_id)[..])?
                .unwrap()
                .value(),
        )?
        .payload,
    )?;
    assert_eq!(tuple.values, [Some(ScalarValue::Text("before".to_owned()))]);
    assert_eq!(commit_sequence(&database)?, 2);
    Ok(())
}
