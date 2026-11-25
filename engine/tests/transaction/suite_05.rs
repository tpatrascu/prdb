use prdb_engine::authorization::{
    AccessTarget, AuthorizationContext, AuthorizedProjection, Effect, Expr, Grant, Operation,
    Policy, Subject,
};
use prdb_engine::commit::CommitError;
use prdb_engine::index_key::{compose_key, derive_index_key};
use prdb_engine::index_locator::IndexLocator;
use prdb_engine::schema::ROWS;
use prdb_engine::transaction::{InvocationTransaction, TransactionError};
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, encode_tuple};
use redb::ReadableDatabase;

use super::helpers::*;

pub(super) fn authorized_join_estimate_counts_duplicate_matches_and_private_overlay()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let schema_id = [0x72; 16];
    let left_table = [0x73; 16];
    let right_table = [0x74; 16];
    let left_key = [0x75; 16];
    let right_key = [0x76; 16];
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Integer,
        nullable: true,
    }];
    let column_ids = [[left_key], [right_key]];
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (Operation::Select, AccessTarget::Table(left_table)),
            (Operation::Select, AccessTarget::Table(right_table)),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: left_table,
                    column_id: left_key,
                },
            ),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: right_table,
                    column_id: right_key,
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
                table_id: left_table,
                column_id: left_key,
            },
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: right_table,
                column_id: right_key,
            },
        },
    ];
    let table_policy = |table_id| Policy {
        subjects: vec![Subject::Principal([9; 16])],
        operation: Operation::Select,
        target: AccessTarget::Table(table_id),
        effect: Effect::Allow,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        with_check: None,
    };
    let left_policy = table_policy(left_table);
    let right_policy = table_policy(right_table);
    let policies = [
        left_policy.clone(),
        Policy {
            target: AccessTarget::Column {
                table_id: left_table,
                column_id: left_key,
            },
            ..left_policy
        },
        right_policy.clone(),
        Policy {
            target: AccessTarget::Column {
                table_id: right_table,
                column_id: right_key,
            },
            ..right_policy
        },
    ];
    let left_projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids[0],
        schema_version: 1,
        schema_id,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let right_projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids[1],
        schema_version: 1,
        schema_id,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let row =
        |value: Option<i32>| encode_tuple(&columns, 1, &[value.map(ScalarValue::Integer)]).unwrap();
    let mut seed = InvocationTransaction::begin(&database, [0x77; 16], [9; 16], 9_999, 32, 4096)?;
    for (number, value) in [(1, Some(7)), (2, Some(7)), (3, Some(9)), (4, None)] {
        seed.stage_put(numbered_row_id(left_table, number), &row(value))?;
    }
    for (number, value) in [
        (1, Some(7)),
        (2, Some(7)),
        (3, Some(7)),
        (4, Some(9)),
        (5, Some(8)),
        (6, None),
    ] {
        seed.stage_put(numbered_row_id(right_table, number), &row(value))?;
    }
    seed.commit()?;

    let mut planner =
        InvocationTransaction::begin(&database, [0x78; 16], [9; 16], 9_999, 512, 1_048_576)?;
    let left_ids = [left_key];
    let right_ids = [right_key];
    assert_eq!(
        planner.estimate_authorized_join_cardinality_at_most(
            left_table,
            &left_projection,
            &left_ids,
            right_table,
            &right_projection,
            &right_ids,
            8,
        )?,
        Some(7),
        "duplicate keys contribute their full SQL join multiplicity and NULLs do not match"
    );

    let hidden_key_policies = [
        policies[0].clone(),
        policies[2].clone(),
        policies[3].clone(),
    ];
    let hidden_left_projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids[0],
        schema_version: 1,
        schema_id,
        context: &context,
        grants: &grants,
        policies: &hidden_key_policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    assert_eq!(
        planner.estimate_authorized_join_cardinality_at_most(
            left_table,
            &hidden_left_projection,
            &left_ids,
            right_table,
            &right_projection,
            &right_ids,
            8,
        )?,
        None,
        "the estimator must not inspect an unreadable join key"
    );
    assert_eq!(
        planner.estimate_authorized_join_cardinality_at_most(
            left_table,
            &left_projection,
            &left_ids,
            right_table,
            &right_projection,
            &right_ids,
            4097,
        )?,
        Some(7),
        "the estimate request may exceed the former 4,096-row cap when the actual populations are smaller"
    );

    planner.stage_put(numbered_row_id(left_table, 5), &row(Some(7)))?;
    assert_eq!(
        planner.estimate_authorized_join_cardinality_at_most(
            left_table,
            &left_projection,
            &left_ids,
            right_table,
            &right_projection,
            &right_ids,
            8,
        )?,
        Some(10),
        "the estimate must reflect the invocation's private overlay"
    );
    for number in 100..226 {
        planner.stage_put(numbered_row_id(left_table, number), &row(Some(7)))?;
    }
    for number in 100..225 {
        planner.stage_put(numbered_row_id(right_table, number), &row(Some(7)))?;
    }
    assert_eq!(
        planner.estimate_authorized_join_cardinality_at_most(
            left_table,
            &left_projection,
            &left_ids,
            right_table,
            &right_projection,
            &right_ids,
            512,
        )?,
        Some(16_513),
        "a 512-row bound must estimate populations above the old 128-row ceiling"
    );
    planner.commit()?;

    let mut expansion =
        InvocationTransaction::begin(&database, [0x7a; 16], [9; 16], 9_999, 2048, 8 * 1024 * 1024)?;
    for number in 1_000..1_893 {
        expansion.stage_put(numbered_row_id(left_table, number), &row(Some(7)))?;
        expansion.stage_put(numbered_row_id(right_table, number), &row(Some(7)))?;
    }
    expansion.commit()?;

    let mut exact_1024 =
        InvocationTransaction::begin(&database, [0x7b; 16], [9; 16], 9_999, 32, 4096)?;
    assert_eq!(
        exact_1024.estimate_authorized_join_cardinality_at_most(
            left_table,
            &left_projection,
            &left_ids,
            right_table,
            &right_projection,
            &right_ids,
            1024,
        )?,
        Some(1_043_463),
        "the estimator should preserve duplicate join multiplicity at the 1,024-row boundary"
    );
    assert_eq!(
        exact_1024.estimate_authorized_join_cardinality_at_most(
            left_table,
            &left_projection,
            &left_ids,
            right_table,
            &right_projection,
            &right_ids,
            512,
        )?,
        None,
        "populations above the requested bound must return Unknown without partial counts"
    );
    exact_1024.abort()?;

    let mut expand_to_4096 =
        InvocationTransaction::begin(&database, [0x7c; 16], [9; 16], 9_999, 4096, 8 * 1024 * 1024)?;
    for number in 5_000..7_048 {
        expand_to_4096.stage_put(numbered_row_id(left_table, number), &row(None))?;
        expand_to_4096.stage_put(numbered_row_id(right_table, number), &row(None))?;
    }
    expand_to_4096.commit()?;

    let mut exact_4096 =
        InvocationTransaction::begin(&database, [0x7d; 16], [9; 16], 9_999, 32, 4096)?;
    assert_eq!(
        exact_4096.estimate_authorized_join_cardinality_at_most(
            left_table,
            &left_projection,
            &left_ids,
            right_table,
            &right_projection,
            &right_ids,
            4096,
        )?,
        Some(1_043_463),
        "the estimator should cover 4,096-row inputs while NULL keys preserve the prior match count"
    );
    assert_eq!(
        exact_4096.estimate_authorized_join_cardinality_at_most(
            left_table,
            &left_projection,
            &left_ids,
            right_table,
            &right_projection,
            &right_ids,
            2048,
        )?,
        None,
        "the estimate must remain Unknown when the actual population exceeds the requested bound"
    );
    exact_4096.abort()?;

    let mut expand_to_4097 =
        InvocationTransaction::begin(&database, [0x7e; 16], [9; 16], 9_999, 4096, 8 * 1024 * 1024)?;
    for number in 8_000..9_025 {
        expand_to_4097.stage_put(numbered_row_id(left_table, number), &row(None))?;
        expand_to_4097.stage_put(numbered_row_id(right_table, number), &row(None))?;
    }
    expand_to_4097.commit()?;

    let mut exact_4097 =
        InvocationTransaction::begin(&database, [0x7f; 16], [9; 16], 9_999, 32, 4096)?;
    assert_eq!(
        exact_4097.estimate_authorized_join_cardinality_at_most(
            left_table,
            &left_projection,
            &left_ids,
            right_table,
            &right_projection,
            &right_ids,
            4097,
        )?,
        Some(1_043_463),
        "the exact pair estimator should continue across the former 4,096-row cap"
    );
    assert_eq!(
        exact_4097.estimate_authorized_join_cardinality_at_most(
            left_table,
            &left_projection,
            &left_ids,
            right_table,
            &right_projection,
            &right_ids,
            4096,
        )?,
        None,
        "populations above the requested pair-estimate bound must remain Unknown"
    );
    exact_4097.abort()?;

    let mut expand_to_16_384 = InvocationTransaction::begin(
        &database,
        [0x80; 16],
        [9; 16],
        9_999,
        32_768,
        8 * 1024 * 1024,
    )?;
    for number in 10_000..22_287 {
        expand_to_16_384.stage_put(numbered_row_id(left_table, number), &row(None))?;
        expand_to_16_384.stage_put(numbered_row_id(right_table, number), &row(None))?;
    }
    expand_to_16_384.commit()?;

    let mut exact_16_384 =
        InvocationTransaction::begin(&database, [0x81; 16], [9; 16], 9_999, 32, 4096)?;
    assert_eq!(
        exact_16_384.estimate_authorized_join_cardinality_at_most(
            left_table,
            &left_projection,
            &left_ids,
            right_table,
            &right_projection,
            &right_ids,
            16_384,
        )?,
        Some(1_043_463),
        "the exact pair estimator should use its complete 16,384-row budget"
    );
    assert_eq!(
        exact_16_384.estimate_authorized_join_cardinality_at_most(
            left_table,
            &left_projection,
            &left_ids,
            right_table,
            &right_projection,
            &right_ids,
            16_383,
        )?,
        None,
        "a smaller requested bound must not return a partial pair count"
    );
    exact_16_384.abort()?;

    let mut oversized =
        InvocationTransaction::begin(&database, [0x79; 16], [9; 16], 9_999, 512, 1_048_576)?;
    for number in [2_000, 2_001] {
        oversized.stage_put(numbered_row_id(right_table, number), &row(Some(7)))?;
    }
    for number in [2_000, 2_001] {
        oversized.stage_put(numbered_row_id(left_table, number), &row(Some(7)))?;
    }
    assert_eq!(
        oversized.estimate_authorized_join_cardinality_at_most(
            left_table,
            &left_projection,
            &left_ids,
            right_table,
            &right_projection,
            &right_ids,
            1024,
        )?,
        None,
        "a 1,025-row population must produce Unknown without a partial count"
    );
    assert_eq!(
        oversized.estimate_authorized_join_cardinality_at_most(
            left_table,
            &left_projection,
            &left_ids,
            right_table,
            &right_projection,
            &right_ids,
            1025,
        )?,
        None,
        "join-cardinality work remains capped at the declared bound"
    );
    Ok(())
}

pub(super) fn authorized_join_estimate_returns_unknown_at_row_and_key_byte_limits()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let schema_id = [0x81; 16];
    let left_table = [0x82; 16];
    let right_table = [0x83; 16];
    let left_integer_key = [0x84; 16];
    let left_text_key = [0x85; 16];
    let right_integer_key = [0x86; 16];
    let right_text_key = [0x87; 16];
    let columns = [
        ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: false,
        },
    ];
    let left_column_ids = [left_integer_key, left_text_key];
    let right_column_ids = [right_integer_key, right_text_key];
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (Operation::Select, AccessTarget::Table(left_table)),
            (Operation::Select, AccessTarget::Table(right_table)),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: left_table,
                    column_id: left_integer_key,
                },
            ),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: left_table,
                    column_id: left_text_key,
                },
            ),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: right_table,
                    column_id: right_integer_key,
                },
            ),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: right_table,
                    column_id: right_text_key,
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
                table_id: left_table,
                column_id: left_integer_key,
            },
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: left_table,
                column_id: left_text_key,
            },
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: right_table,
                column_id: right_integer_key,
            },
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: right_table,
                column_id: right_text_key,
            },
        },
    ];
    let table_policy = |table_id| Policy {
        subjects: vec![Subject::Principal([9; 16])],
        operation: Operation::Select,
        target: AccessTarget::Table(table_id),
        effect: Effect::Allow,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        with_check: None,
    };
    let left_policy = table_policy(left_table);
    let right_policy = table_policy(right_table);
    let policies = [
        left_policy.clone(),
        Policy {
            target: AccessTarget::Column {
                table_id: left_table,
                column_id: left_integer_key,
            },
            ..left_policy.clone()
        },
        Policy {
            target: AccessTarget::Column {
                table_id: left_table,
                column_id: left_text_key,
            },
            ..left_policy
        },
        right_policy.clone(),
        Policy {
            target: AccessTarget::Column {
                table_id: right_table,
                column_id: right_integer_key,
            },
            ..right_policy.clone()
        },
        Policy {
            target: AccessTarget::Column {
                table_id: right_table,
                column_id: right_text_key,
            },
            ..right_policy
        },
    ];
    let left_projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &left_column_ids,
        schema_version: 1,
        schema_id,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let right_projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &right_column_ids,
        schema_version: 1,
        schema_id,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let large_value = ScalarValue::Text("x".repeat(940_000));
    let encode_row = |text_value: ScalarValue| {
        encode_tuple(
            &columns,
            1,
            &[Some(ScalarValue::Integer(0)), Some(text_value)],
        )
        .unwrap()
    };
    let mut seed =
        InvocationTransaction::begin(&database, [0x88; 16], [9; 16], 9_999, 16, 12 * 1024 * 1024)?;
    for number in 0..5 {
        seed.stage_put(
            numbered_row_id(left_table, number),
            &encode_row(large_value.clone()),
        )?;
    }
    for number in 0..4 {
        seed.stage_put(
            numbered_row_id(right_table, number),
            &encode_row(large_value.clone()),
        )?;
    }
    seed.commit()?;

    let mut planner =
        InvocationTransaction::begin(&database, [0x89; 16], [9; 16], 9_999, 16, 4096)?;
    assert_eq!(
        planner.estimate_authorized_join_cardinality_at_most(
            left_table,
            &left_projection,
            &[left_integer_key],
            right_table,
            &right_projection,
            &[right_integer_key],
            8,
        )?,
        None,
        "combined decoded row payload above 8 MiB must not produce a partial join estimate"
    );
    assert_eq!(
        planner.estimate_authorized_join_cardinality_at_most(
            left_table,
            &left_projection,
            &[left_text_key],
            right_table,
            &right_projection,
            &[right_text_key],
            8,
        )?,
        None,
        "an encoded join key above 4 KiB must return Unknown"
    );
    Ok(())
}

pub(super) fn authorized_scan_estimate_returns_unknown_at_candidate_budget()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::statistics::{
        AuthorizedScanEstimate, AuthorizedScanSummary, MAX_ESTIMATE_CANDIDATES,
    };

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_id = id(0).table;
    let schema_id = [0x23; 16];
    let column_id = [0x22; 16];
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Integer,
        nullable: false,
    }];
    let row = |value: i32| encode_tuple(&columns, 1, &[Some(ScalarValue::Integer(value))]).unwrap();

    let mut seed = InvocationTransaction::begin(
        &database,
        [0x51; 16],
        [9; 16],
        9_999,
        MAX_ESTIMATE_CANDIDATES + 1,
        4 * 1024 * 1024,
    )?;
    for index in 1..MAX_ESTIMATE_CANDIDATES {
        seed.stage_put(
            prdb_engine::overlay::RowId {
                table: table_id,
                row: (index as u128).to_be_bytes(),
            },
            &row(index as i32),
        )?;
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
    let table_allow = Policy {
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
        ..table_allow.clone()
    };
    let policies = [table_allow, cell_allow];
    let projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &[column_id],
        schema_version: 1,
        schema_id,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let mut below_limit =
        InvocationTransaction::begin(&database, [0x52; 16], [9; 16], 9_999, 1, 64)?;
    assert_eq!(
        below_limit.estimate_authorized_scan(table_id, &projection)?,
        AuthorizedScanEstimate::Exact(AuthorizedScanSummary {
            visible_rows: (MAX_ESTIMATE_CANDIDATES - 1) as u64,
            columns: vec![prdb_engine::statistics::ColumnPopulation {
                readable_values: (MAX_ESTIMATE_CANDIDATES - 1) as u64,
                nulls: 0,
                redacted: 0,
            }],
        })
    );

    let mut extra = InvocationTransaction::begin(&database, [0x53; 16], [9; 16], 9_999, 1, 64)?;
    extra.stage_put(
        prdb_engine::overlay::RowId {
            table: table_id,
            row: (MAX_ESTIMATE_CANDIDATES as u128).to_be_bytes(),
        },
        &row(MAX_ESTIMATE_CANDIDATES as i32),
    )?;
    extra.commit()?;
    let mut at_limit = InvocationTransaction::begin(&database, [0x54; 16], [9; 16], 9_999, 1, 64)?;
    assert_eq!(
        at_limit.estimate_authorized_scan(table_id, &projection)?,
        AuthorizedScanEstimate::Unknown
    );
    Ok(())
}

pub(super) fn invocation_auto_indexes_row_writes() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_id = id(5);
    let payload = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        }],
        1,
        &[Some(ScalarValue::Bytea(b"indexed".to_vec()))],
    )?;

    let mut missing = InvocationTransaction::begin(&database, [5; 16], [9; 16], 9_999, 4, 2_048)?;
    let schema = missing.load_catalog_table_schema(row_id.table)?;
    let indexes = missing.load_catalog_indexes(&schema)?;
    assert_eq!(indexes.len(), 3);
    missing.stage_put(row_id, &payload)?;
    let mut index_keys = Vec::new();
    for index in &indexes {
        index_keys.push(
            derive_index_key(
                index.index_id,
                row_id.row,
                schema.schema_version,
                &schema.column_ids,
                &schema.columns,
                &payload,
                &index.column_ids,
            )?
            .key,
        );
    }
    assert_eq!(missing.commit()?, 1);
    {
        let read = database.begin_read()?;
        let indexes = read.open_table(prdb_engine::schema::INDEXES)?;
        for index_key in &index_keys {
            let locator = indexes.get(index_key.as_slice())?.unwrap().value().to_vec();
            assert_eq!(
                IndexLocator::decode_for_key(index_key, &locator)?.row_version,
                1
            );
        }
    }

    let mut without_index_load =
        InvocationTransaction::begin(&database, [12; 16], [9; 16], 9_999, 4, 2_048)?;
    without_index_load.load_catalog_table_schema(row_id.table)?;
    without_index_load.stage_put(row_id, &payload)?;
    assert!(matches!(
        without_index_load.commit(),
        Err(TransactionError::Commit(CommitError::InvalidIndex))
    ));

    let mut forged = InvocationTransaction::begin(&database, [8; 16], [9; 16], 9_999, 4, 2_048)?;
    let schema = forged.load_catalog_table_schema(row_id.table)?;
    let indexes = forged.load_catalog_indexes(&schema)?;
    forged.stage_put(row_id, &payload)?;
    forged.stage_index_membership(
        row_id,
        compose_key(indexes[0].index_id, &[1, 0x7f], row_id.row),
        true,
    )?;
    assert!(matches!(
        forged.commit(),
        Err(TransactionError::Commit(CommitError::InvalidIndex))
    ));

    let small_row_id = id(13);
    let small_payload = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        }],
        1,
        &[Some(ScalarValue::Bytea(b"x".to_vec()))],
    )?;
    let mut bounded = InvocationTransaction::begin(&database, [13; 16], [9; 16], 9_999, 256, 40)?;
    let schema = bounded.load_catalog_table_schema(small_row_id.table)?;
    bounded.load_catalog_indexes(&schema)?;
    bounded.stage_put(small_row_id, &small_payload)?;
    assert!(matches!(
        bounded.commit(),
        Err(TransactionError::IndexOverlayLimitExceeded)
    ));
    {
        let read = database.begin_read()?;
        assert!(
            read.open_table(ROWS)?
                .get(&key(small_row_id)[..])?
                .is_none()
        );
    }
    let updated_payload = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        }],
        1,
        &[Some(ScalarValue::Bytea(b"changed".to_vec()))],
    )?;
    let mut update = InvocationTransaction::begin(&database, [7; 16], [9; 16], 9_999, 4, 2_048)?;
    let schema = update.load_catalog_table_schema(row_id.table)?;
    let indexes_for_update = update.load_catalog_indexes(&schema)?;
    update.stage_put(row_id, &updated_payload)?;
    let mut updated_index_keys = Vec::new();
    for index in &indexes_for_update {
        let new = derive_index_key(
            index.index_id,
            row_id.row,
            schema.schema_version,
            &schema.column_ids,
            &schema.columns,
            &updated_payload,
            &index.column_ids,
        )
        .unwrap();
        updated_index_keys.push(new.key);
    }
    assert_eq!(update.commit()?, 2);
    {
        let read = database.begin_read()?;
        let indexes = read.open_table(prdb_engine::schema::INDEXES)?;
        for index_key in &index_keys {
            assert!(indexes.get(index_key.as_slice())?.is_none());
        }
        for index_key in &updated_index_keys {
            let locator = indexes.get(index_key.as_slice())?.unwrap().value().to_vec();
            assert_eq!(
                IndexLocator::decode_for_key(index_key, &locator)?.row_version,
                2
            );
        }
    }

    let mut delete = InvocationTransaction::begin(&database, [9; 16], [9; 16], 9_999, 4, 2_048)?;
    let schema = delete.load_catalog_table_schema(row_id.table)?;
    delete.load_catalog_indexes(&schema)?;
    delete.stage_delete(row_id)?;
    assert_eq!(delete.commit()?, 3);
    let read = database.begin_read()?;
    let indexes = read.open_table(prdb_engine::schema::INDEXES)?;
    for index_key in &updated_index_keys {
        assert!(indexes.get(index_key.as_slice())?.is_none());
    }
    Ok(())
}
