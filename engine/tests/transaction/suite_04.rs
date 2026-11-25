use prdb_engine::authorization::{
    AccessTarget, AuthorizationContext, AuthorizedProjection, Effect, Expr, Grant, Operation,
    Policy, SanitizedCell, Subject,
};
use prdb_engine::commit::{RowWrite, commit_rows};
use prdb_engine::metadata::EngineMetadata;
use prdb_engine::schema::{self, CATALOG, ENGINE_META, ROWS};
use prdb_engine::transaction::InvocationTransaction;
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, encode_tuple};
use redb::ReadableDatabase;

use super::helpers::*;

pub(super) fn authorized_scan_estimate_counts_sanitized_cells_and_private_writes()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let columns = [
        ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: true,
        },
        ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: true,
        },
    ];
    let column_ids = [[0x22; 16], [0x24; 16]];
    let schema_id = [0x23; 16];
    let table_id = id(0).table;
    let row = |visible: Option<&str>, hidden: &str| {
        encode_tuple(
            &columns,
            1,
            &[
                visible.map(|value| ScalarValue::Text(value.to_owned())),
                Some(ScalarValue::Text(hidden.to_owned())),
            ],
        )
        .unwrap()
    };
    let mut seed = InvocationTransaction::begin(&database, [0x31; 16], [9; 16], 9_999, 8, 512)?;
    seed.stage_put(id(1), &row(Some("first"), "secret-one"))?;
    seed.stage_put(id(2), &row(None, "secret-two"))?;
    seed.commit()?;

    let mut population_key = b"statistics:committed-population:v1:".to_vec();
    population_key.extend_from_slice(&table_id);
    let mut population_record = [0; 21];
    population_record[..4].copy_from_slice(b"CPR1");
    population_record[4..12].copy_from_slice(&commit_sequence(&database)?.to_le_bytes());
    population_record[12] = 1;
    population_record[13..21].copy_from_slice(&2u64.to_le_bytes());
    {
        let write = database.begin_write()?;
        write
            .open_table(schema::STATS)?
            .insert(population_key.as_slice(), population_record.as_slice())?;
        write.commit()?;
    }

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
                    column_id: column_ids[0],
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
                column_id: column_ids[0],
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
    let readable_cell_allow = Policy {
        target: AccessTarget::Column {
            table_id,
            column_id: column_ids[0],
        },
        ..table_allow.clone()
    };
    let policies = [table_allow, readable_cell_allow];
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

    let predicate = prdb_engine::optimizer_binding::BoundPredicate::Compare {
        operator: prdb_engine::optimizer_request::Comparison::Equal,
        left: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Column(
            prdb_engine::optimizer_binding::BoundColumn {
                column_id: column_ids[0],
                ordinal: 0,
                scalar_type: ScalarType::Text,
                nullable: true,
            },
        )),
        right: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Text(
            "first".to_owned(),
        )),
    };

    let mut observer = InvocationTransaction::begin(&database, [0x32; 16], [9; 16], 9_999, 8, 512)?;
    let mut writer = InvocationTransaction::begin(&database, [0x33; 16], [9; 16], 9_999, 8, 512)?;
    assert_eq!(
        observer.estimate_authorized_population_at_most(table_id, &projection, 4)?,
        Some(2)
    );
    let mut deleting_probe =
        InvocationTransaction::begin(&database, [0x38; 16], [9; 16], 9_999, 8, 512)?;
    deleting_probe.stage_delete(id(2))?;
    assert_eq!(
        deleting_probe.estimate_authorized_population_at_most(table_id, &projection, 1)?,
        Some(1),
        "a fresh committed count should be adjusted for a private delete at the estimate bound"
    );
    deleting_probe.abort()?;
    assert_eq!(
        observer.estimate_authorized_predicate_at_most(table_id, &projection, &predicate, 4,)?,
        Some((2, 1))
    );
    let hidden_predicate = prdb_engine::optimizer_binding::BoundPredicate::Compare {
        operator: prdb_engine::optimizer_request::Comparison::Equal,
        left: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Column(
            prdb_engine::optimizer_binding::BoundColumn {
                column_id: column_ids[1],
                ordinal: 1,
                scalar_type: ScalarType::Text,
                nullable: false,
            },
        )),
        right: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Text(
            "secret-one".to_owned(),
        )),
    };
    assert_eq!(
        observer.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &hidden_predicate,
            128,
        )?,
        None
    );
    let mut boundary_probe =
        InvocationTransaction::begin(&database, [0x3a; 16], [9; 16], 9_999, 256, 32_768)?;
    boundary_probe.stage_put(id(3), &row(Some("third"), "secret-three"))?;
    boundary_probe.stage_put(id(4), &row(Some("fourth"), "secret-four"))?;
    assert_eq!(
        boundary_probe.estimate_authorized_population_at_most(table_id, &projection, 4)?,
        Some(4)
    );
    boundary_probe.stage_put(id(5), &row(Some("fifth"), "secret-five"))?;
    assert_eq!(
        boundary_probe.estimate_authorized_population_at_most(table_id, &projection, 4)?,
        None
    );
    assert_eq!(
        boundary_probe.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &predicate,
            4,
        )?,
        None
    );
    assert_eq!(
        boundary_probe.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &predicate,
            128,
        )?,
        Some((5, 1)),
        "predicate estimates should cover exact populations larger than four rows"
    );
    assert_eq!(
        boundary_probe.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &predicate,
            129,
        )?,
        Some((5, 1)),
        "predicate estimate limit must cover more than 128 rows when population is smaller"
    );
    for row_number in 6..=129 {
        boundary_probe.stage_put(
            id(row_number),
            &row(Some("other"), &format!("secret-{row_number}")),
        )?;
    }
    assert_eq!(
        boundary_probe.estimate_authorized_population_at_most(
            table_id,
            &projection,
            prdb_engine::statistics::MAX_ESTIMATE_CANDIDATES as u64,
        )?,
        Some(129)
    );
    assert_eq!(
        boundary_probe.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &predicate,
            128,
        )?,
        None,
        "populations above the caller's bound must remain unknown"
    );
    assert_eq!(
        boundary_probe.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &predicate,
            prdb_engine::statistics::MAX_ESTIMATE_CANDIDATES as u64,
        )?,
        Some((129, 1)),
        "bounded predicate estimates should cover exact authorized populations above 128 rows"
    );
    assert_eq!(
        boundary_probe.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &predicate,
            prdb_engine::statistics::MAX_ESTIMATE_CANDIDATES as u64 + 1,
        )?,
        None,
        "predicate estimate limit must remain explicitly bounded"
    );
    boundary_probe.abort()?;
    assert_eq!(
        observer.estimate_authorized_scan(table_id, &projection)?,
        prdb_engine::statistics::AuthorizedScanEstimate::Exact(
            prdb_engine::statistics::AuthorizedScanSummary {
                visible_rows: 2,
                columns: vec![
                    prdb_engine::statistics::ColumnPopulation {
                        readable_values: 1,
                        nulls: 1,
                        redacted: 0,
                    },
                    prdb_engine::statistics::ColumnPopulation {
                        readable_values: 0,
                        nulls: 0,
                        redacted: 2,
                    },
                ],
            }
        )
    );

    writer.stage_put(id(3), &row(Some("third"), "secret-three"))?;
    assert_eq!(
        writer.estimate_authorized_population_at_most(table_id, &projection, 4)?,
        Some(3)
    );
    assert_eq!(
        writer.estimate_authorized_predicate_at_most(table_id, &projection, &predicate, 4,)?,
        Some((3, 1))
    );
    let private_summary = writer.estimate_authorized_scan(table_id, &projection)?;
    assert_eq!(
        private_summary,
        prdb_engine::statistics::AuthorizedScanEstimate::Exact(
            prdb_engine::statistics::AuthorizedScanSummary {
                visible_rows: 3,
                columns: vec![
                    prdb_engine::statistics::ColumnPopulation {
                        readable_values: 2,
                        nulls: 1,
                        redacted: 0,
                    },
                    prdb_engine::statistics::ColumnPopulation {
                        readable_values: 0,
                        nulls: 0,
                        redacted: 3,
                    },
                ],
            }
        )
    );
    writer.stage_put(id(1), &row(Some("first"), "replacement-hidden-secret"))?;
    assert_eq!(
        writer.estimate_authorized_predicate_at_most(table_id, &projection, &predicate, 4)?,
        Some((3, 1)),
        "changing only a redacted cell must not change predicate estimate counts"
    );
    assert_eq!(
        writer.estimate_authorized_scan(table_id, &projection)?,
        private_summary,
        "changing only a redacted cell must not change released column populations"
    );
    writer.stage_put(id(1), &row(Some("changed"), "changed-secret"))?;
    assert_eq!(
        writer.estimate_authorized_population_at_most(table_id, &projection, 4)?,
        Some(3)
    );
    assert_eq!(
        writer.estimate_authorized_scan(table_id, &projection)?,
        private_summary
    );
    let changed_predicate = prdb_engine::optimizer_binding::BoundPredicate::Compare {
        operator: prdb_engine::optimizer_request::Comparison::Equal,
        left: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Column(
            prdb_engine::optimizer_binding::BoundColumn {
                column_id: column_ids[0],
                ordinal: 0,
                scalar_type: ScalarType::Text,
                nullable: true,
            },
        )),
        right: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Text(
            "changed".to_owned(),
        )),
    };
    assert_eq!(
        writer.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &changed_predicate,
            4,
        )?,
        Some((3, 1))
    );
    writer.stage_delete(id(2))?;
    assert_eq!(
        writer.estimate_authorized_population_at_most(table_id, &projection, 4)?,
        Some(2)
    );
    assert_eq!(
        writer.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &changed_predicate,
            4,
        )?,
        Some((2, 1))
    );
    assert_eq!(
        writer.estimate_authorized_scan(table_id, &projection)?,
        prdb_engine::statistics::AuthorizedScanEstimate::Exact(
            prdb_engine::statistics::AuthorizedScanSummary {
                visible_rows: 2,
                columns: vec![
                    prdb_engine::statistics::ColumnPopulation {
                        readable_values: 2,
                        nulls: 0,
                        redacted: 0,
                    },
                    prdb_engine::statistics::ColumnPopulation {
                        readable_values: 0,
                        nulls: 0,
                        redacted: 2,
                    },
                ],
            }
        )
    );
    assert_eq!(
        observer.estimate_authorized_scan(table_id, &projection)?,
        prdb_engine::statistics::AuthorizedScanEstimate::Exact(
            prdb_engine::statistics::AuthorizedScanSummary {
                visible_rows: 2,
                columns: vec![
                    prdb_engine::statistics::ColumnPopulation {
                        readable_values: 1,
                        nulls: 1,
                        redacted: 0,
                    },
                    prdb_engine::statistics::ColumnPopulation {
                        readable_values: 0,
                        nulls: 0,
                        redacted: 2,
                    },
                ],
            }
        )
    );

    let row_filter = Policy {
        using: Some(Expr::Equal(
            Box::new(Expr::Field(0)),
            Box::new(Expr::Literal(Some(ScalarValue::Text("first".to_owned())))),
        )),
        ..policies[0].clone()
    };
    let filtered_policies = [row_filter, policies[1].clone()];
    let filtered_projection = AuthorizedProjection {
        policies: &filtered_policies,
        ..projection
    };
    let mut hidden_value_a =
        InvocationTransaction::begin(&database, [0x36; 16], [9; 16], 9_999, 32, 2_048)?;
    let mut hidden_value_b =
        InvocationTransaction::begin(&database, [0x37; 16], [9; 16], 9_999, 32, 2_048)?;
    hidden_value_a.stage_put(id(3), &row(Some("not-visible"), "private-a"))?;
    hidden_value_b.stage_put(id(3), &row(Some("not-visible"), "private-b"))?;
    let hidden_estimate_a =
        hidden_value_a.estimate_authorized_scan(table_id, &filtered_projection)?;
    let hidden_estimate_b =
        hidden_value_b.estimate_authorized_scan(table_id, &filtered_projection)?;
    assert_eq!(
        hidden_estimate_a,
        prdb_engine::statistics::AuthorizedScanEstimate::Unknown
    );
    assert_eq!(hidden_estimate_a, hidden_estimate_b);
    assert_eq!(
        hidden_value_a.estimate_authorized_predicate_at_most(
            table_id,
            &filtered_projection,
            &predicate,
            128,
        )?,
        None,
        "value-dependent row policy must suppress predicate estimates"
    );
    assert_eq!(
        hidden_value_b.estimate_authorized_predicate_at_most(
            table_id,
            &filtered_projection,
            &predicate,
            128,
        )?,
        None
    );
    assert_eq!(
        hidden_value_a.estimate_authorized_population_at_most(table_id, &projection, 4)?,
        Some(3)
    );
    assert_eq!(
        hidden_value_b.estimate_authorized_population_at_most(table_id, &projection, 4)?,
        Some(3)
    );

    let mut fewer_candidates =
        InvocationTransaction::begin(&database, [0x34; 16], [9; 16], 9_999, 32, 2_048)?;
    let mut more_candidates =
        InvocationTransaction::begin(&database, [0x35; 16], [9; 16], 9_999, 32, 2_048)?;
    for row_id in 3..=10 {
        more_candidates.stage_put(
            id(row_id),
            &row(Some("not-visible"), &format!("hidden-{row_id}")),
        )?;
    }
    assert_eq!(
        fewer_candidates.estimate_authorized_scan(table_id, &filtered_projection)?,
        prdb_engine::statistics::AuthorizedScanEstimate::Unknown
    );
    assert_eq!(
        more_candidates.estimate_authorized_scan(table_id, &filtered_projection)?,
        prdb_engine::statistics::AuthorizedScanEstimate::Unknown
    );
    assert_eq!(
        fewer_candidates.estimate_authorized_predicate_at_most(
            table_id,
            &filtered_projection,
            &predicate,
            4,
        )?,
        None
    );
    assert_eq!(
        more_candidates.estimate_authorized_predicate_at_most(
            table_id,
            &filtered_projection,
            &predicate,
            128,
        )?,
        None,
        "adding invisible candidates must not change the released estimate"
    );
    assert_eq!(
        more_candidates.estimate_authorized_population_at_most(
            table_id,
            &filtered_projection,
            4
        )?,
        None
    );
    let fewer_authorized_rows =
        fewer_candidates.scan_authorized(table_id, None, 32, &filtered_projection)?;
    let more_authorized_rows =
        more_candidates.scan_authorized(table_id, None, 32, &filtered_projection)?;
    assert!(fewer_authorized_rows.continuation.is_none());
    assert!(more_authorized_rows.continuation.is_none());
    assert_eq!(fewer_authorized_rows.rows, more_authorized_rows.rows);
    assert_eq!(
        fewer_authorized_rows.rows,
        vec![(
            id(1),
            vec![
                SanitizedCell::Value(ScalarValue::Text("first".to_owned())),
                SanitizedCell::Redacted,
            ],
        )]
    );

    let deny_all_policy = Policy {
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(false)))),
        ..policies[0].clone()
    };
    let deny_all_policies = [deny_all_policy, policies[1].clone()];
    let deny_all_projection = AuthorizedProjection {
        policies: &deny_all_policies,
        ..projection
    };
    let expected_empty_estimate = prdb_engine::statistics::AuthorizedScanEstimate::Exact(
        prdb_engine::statistics::AuthorizedScanSummary {
            visible_rows: 0,
            columns: vec![prdb_engine::statistics::ColumnPopulation::default(); 2],
        },
    );
    assert_eq!(
        fewer_candidates.estimate_authorized_scan(table_id, &deny_all_projection)?,
        expected_empty_estimate
    );
    assert_eq!(
        more_candidates.estimate_authorized_scan(table_id, &deny_all_projection)?,
        expected_empty_estimate
    );
    let fewer_denied_rows =
        fewer_candidates.scan_authorized(table_id, None, 32, &deny_all_projection)?;
    let more_denied_rows =
        more_candidates.scan_authorized(table_id, None, 32, &deny_all_projection)?;
    assert!(fewer_denied_rows.rows.is_empty());
    assert!(more_denied_rows.rows.is_empty());

    let deny_restrict_policy = Policy {
        effect: Effect::Restrict,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(false)))),
        ..policies[0].clone()
    };
    let allow_and_deny_policies = [
        policies[0].clone(),
        deny_restrict_policy.clone(),
        policies[1].clone(),
    ];
    let allow_and_deny_projection = AuthorizedProjection {
        policies: &allow_and_deny_policies,
        ..projection
    };
    assert_eq!(
        fewer_candidates.estimate_authorized_scan(table_id, &allow_and_deny_projection)?,
        expected_empty_estimate
    );
    assert!(
        fewer_candidates
            .scan_authorized(table_id, None, 32, &allow_and_deny_projection)?
            .rows
            .is_empty()
    );
    let deny_and_dynamic_policies = [
        deny_restrict_policy,
        filtered_policies[0].clone(),
        policies[1].clone(),
    ];
    let deny_and_dynamic_projection = AuthorizedProjection {
        policies: &deny_and_dynamic_policies,
        ..projection
    };
    assert_eq!(
        more_candidates.estimate_authorized_scan(table_id, &deny_and_dynamic_projection)?,
        prdb_engine::statistics::AuthorizedScanEstimate::Unknown
    );
    assert!(fewer_candidates.check_deadline().is_ok());
    assert!(more_candidates.check_deadline().is_ok());
    Ok(())
}

pub(super) fn constant_predicate_reuses_fresh_population_without_reading_rows()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_id = [0x10; 16];
    let schema_id = [0x10; 16];
    let column_id = [0x66; 16];
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Bytea,
        nullable: true,
    }];
    let column_ids = [column_id];
    let (index_key, index_value) = index_catalog_record([0x57; 16], table_id, column_id, 1)?;
    {
        let write = database.begin_write()?;
        write
            .open_table(CATALOG)?
            .insert(index_key.as_slice(), index_value.as_slice())?;
        write.commit()?;
    }
    let row = encode_tuple(&columns, 1, &[Some(ScalarValue::Bytea(b"seed".to_vec()))])?;
    let mut seed = InvocationTransaction::begin(&database, [0x31; 16], [9; 16], 9_999, 4, 512)?;
    let seed_schema = seed.load_catalog_table_schema(table_id)?;
    seed.load_catalog_indexes(&seed_schema)?;
    seed.stage_put(id(1), &row)?;
    let other_row = encode_tuple(&columns, 1, &[Some(ScalarValue::Bytea(b"other".to_vec()))])?;
    seed.stage_put(id(2), &other_row)?;
    seed.commit()?;

    let commit_sequence = {
        let read = database.begin_read()?;
        let metadata = read
            .open_table(ENGINE_META)?
            .get(&b"format"[..])?
            .ok_or("engine metadata is missing")?;
        EngineMetadata::decode(metadata.value())?.commit_sequence
    };
    let mut population_key = b"statistics:committed-population:v1:".to_vec();
    population_key.extend_from_slice(&table_id);
    let mut population_record = [0; 21];
    population_record[..4].copy_from_slice(b"CPR1");
    population_record[4..12].copy_from_slice(&commit_sequence.to_le_bytes());
    population_record[12] = 1;
    population_record[13..21].copy_from_slice(&2u64.to_le_bytes());
    {
        let write = database.begin_write()?;
        write
            .open_table(ROWS)?
            .insert(key(id(2)).as_slice(), &b"malformed tuple"[..])?;
        write
            .open_table(schema::STATS)?
            .insert(population_key.as_slice(), population_record.as_slice())?;
        write.commit()?;
    }

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
    let policies = [
        table_allow.clone(),
        Policy {
            target: AccessTarget::Column {
                table_id,
                column_id,
            },
            ..table_allow
        },
    ];
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
        InvocationTransaction::begin(&database, [0x32; 16], [9; 16], 9_999, 4, 512)?;
    let estimate = invocation.estimate_authorized_predicate_at_most(
        table_id,
        &projection,
        &prdb_engine::optimizer_binding::BoundPredicate::Boolean(false),
        4,
    )?;
    assert_eq!(
        estimate,
        Some((2, 0)),
        "constant FALSE needs only the fresh full-view population, not tuple decoding"
    );
    assert_eq!(
        invocation.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &prdb_engine::optimizer_binding::BoundPredicate::Boolean(true),
            4,
        )?,
        Some((2, 2))
    );
    assert_eq!(
        invocation.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &prdb_engine::optimizer_binding::BoundPredicate::Null,
            4,
        )?,
        Some((2, 0)),
        "SQL NULL is not a matching WHERE predicate"
    );
    let constant_false_comparison = prdb_engine::optimizer_binding::BoundPredicate::Compare {
        operator: prdb_engine::optimizer_request::Comparison::Equal,
        left: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Integer(1)),
        right: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Integer(2)),
    };
    assert_eq!(
        invocation.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &constant_false_comparison,
            4,
        )?,
        Some((2, 0))
    );
    let null_is_null = prdb_engine::optimizer_binding::BoundPredicate::IsNull(Box::new(
        prdb_engine::optimizer_binding::BoundPredicate::Null,
    ));
    assert_eq!(
        invocation.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &null_is_null,
            4,
        )?,
        Some((2, 2))
    );
    let indexed_match = prdb_engine::optimizer_binding::BoundPredicate::Compare {
        operator: prdb_engine::optimizer_request::Comparison::Equal,
        left: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Column(
            prdb_engine::optimizer_binding::BoundColumn {
                column_id,
                ordinal: 0,
                scalar_type: ScalarType::Bytea,
                nullable: true,
            },
        )),
        right: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Bytea(
            b"seed".to_vec(),
        )),
    };
    let mut bounded_scan_work =
        InvocationTransaction::begin(&database, [0x35; 16], [9; 16], 9_999, 4, 1)?;
    assert_eq!(
        bounded_scan_work.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &indexed_match,
            4,
        )?,
        Some((2, 1))
    );
    assert!(matches!(
        bounded_scan_work.scan_catalog_index_range(
            table_id,
            [1; 16],
            Some(prdb_engine::transaction::AuthorizedIndexRangeBound {
                value: ScalarValue::Bytea(b"seed".to_vec()),
                inclusive: true,
            }),
            Some(prdb_engine::transaction::AuthorizedIndexRangeBound {
                value: ScalarValue::Bytea(b"seed".to_vec()),
                inclusive: true,
            }),
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedIndexRangeScan::Rows(rows)
            if rows.len() == 1 && rows[0].0 == id(1)
    ));
    bounded_scan_work.abort()?;

    let changed_row = encode_tuple(
        &columns,
        1,
        &[Some(ScalarValue::Bytea(b"changed".to_vec()))],
    )?;
    let mut overlay_update =
        InvocationTransaction::begin(&database, [0x36; 16], [9; 16], 9_999, 4, 512)?;
    overlay_update.stage_put(id(1), &changed_row)?;
    assert_eq!(
        overlay_update.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &indexed_match,
            4,
        )?,
        Some((2, 0)),
        "a private update must remove the committed indexed match"
    );
    overlay_update.abort()?;

    let mut overlay_delete =
        InvocationTransaction::begin(&database, [0x37; 16], [9; 16], 9_999, 4, 512)?;
    overlay_delete.stage_delete(id(1))?;
    assert_eq!(
        overlay_delete.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &indexed_match,
            4,
        )?,
        Some((1, 0)),
        "a private delete must remove the committed indexed match and row"
    );
    overlay_delete.abort()?;

    assert_eq!(
        invocation.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &indexed_match,
            4,
        )?,
        Some((2, 1)),
        "an indexed exact estimate should not decode unrelated malformed rows"
    );
    invocation.stage_put(id(3), &row)?;
    assert_eq!(
        invocation.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &indexed_match,
            4,
        )?,
        Some((3, 2)),
        "a fresh committed index count should be adjusted by the bounded private insert"
    );
    assert_eq!(
        invocation.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &prdb_engine::optimizer_binding::BoundPredicate::Boolean(false),
            4,
        )?,
        Some((3, 0)),
        "a row-independent constant predicate should use the adjusted private population"
    );
    invocation.abort()?;

    commit_rows(
        &database,
        [0x33; 16],
        [9; 16],
        9_999,
        &[RowWrite {
            key: key(id(3)),
            expected_version: None,
            payload: Some(row),
        }],
    )?;
    let mut stale_statistics_invocation =
        InvocationTransaction::begin(&database, [0x34; 16], [9; 16], 9_999, 4, 512)?;
    assert_eq!(
        stale_statistics_invocation.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &prdb_engine::optimizer_binding::BoundPredicate::Boolean(false),
            4,
        )?,
        Some((3, 0)),
        "a stale population record must fall back to a fresh bounded population probe"
    );
    stale_statistics_invocation.abort()?;
    Ok(())
}

pub(super) fn authorized_predicate_estimate_compares_readable_decimal_columns()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::optimizer_binding::{BoundColumn, BoundPredicate};
    use prdb_engine::optimizer_request::Comparison;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let decimal_type = ScalarType::Decimal {
        precision: 8,
        scale: 2,
    };
    let columns = [
        ColumnSchema {
            scalar_type: decimal_type,
            nullable: true,
        },
        ColumnSchema {
            scalar_type: decimal_type,
            nullable: true,
        },
    ];
    let column_ids = [[0x52; 16], [0x53; 16]];
    let table_id = id(0).table;
    let encode_row = |left: Option<i128>, right: Option<i128>| {
        encode_tuple(
            &columns,
            1,
            &[
                left.map(ScalarValue::Decimal),
                right.map(ScalarValue::Decimal),
            ],
        )
        .unwrap()
    };
    let mut seed = InvocationTransaction::begin(&database, [0x51; 16], [9; 16], 9_999, 8, 512)?;
    seed.stage_put(id(1), &encode_row(Some(110), Some(110)))?;
    seed.stage_put(id(2), &encode_row(Some(220), Some(330)))?;
    seed.stage_put(id(3), &encode_row(None, None))?;
    seed.commit()?;

    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema([0x54; 16])),
            (Operation::Select, AccessTarget::Table(table_id)),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id: column_ids[0],
                },
            ),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id: column_ids[1],
                },
            ),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema([0x54; 16]),
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id: column_ids[0],
            },
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id: column_ids[1],
            },
        },
    ];
    let policies = [
        Policy {
            subjects: vec![Subject::Principal([9; 16])],
            operation: Operation::Select,
            target: AccessTarget::Table(table_id),
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Principal([9; 16])],
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id: column_ids[0],
            },
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Principal([9; 16])],
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id: column_ids[1],
            },
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
    ];
    let projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id: [0x54; 16],
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let predicate = |operator| BoundPredicate::Compare {
        operator,
        left: Box::new(BoundPredicate::Column(BoundColumn {
            column_id: column_ids[0],
            ordinal: 0,
            scalar_type: decimal_type,
            nullable: true,
        })),
        right: Box::new(BoundPredicate::Column(BoundColumn {
            column_id: column_ids[1],
            ordinal: 1,
            scalar_type: decimal_type,
            nullable: true,
        })),
    };
    let mut observer = InvocationTransaction::begin(&database, [0x55; 16], [9; 16], 9_999, 8, 512)?;
    assert_eq!(
        observer.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &predicate(Comparison::Equal),
            3,
        )?,
        Some((3, 1))
    );
    assert_eq!(
        observer.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &predicate(Comparison::Less),
            3,
        )?,
        Some((3, 1))
    );
    assert_eq!(
        observer.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &predicate(Comparison::NotEqual),
            3,
        )?,
        Some((3, 1))
    );
    Ok(())
}

pub(super) fn authorized_predicate_estimate_compares_readable_scalar_columns()
-> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::optimizer_binding::{BoundColumn, BoundPredicate};
    use prdb_engine::optimizer_request::Comparison;

    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let cases = [
        (
            ScalarType::Boolean,
            ScalarValue::Boolean(false),
            ScalarValue::Boolean(false),
            ScalarValue::Boolean(true),
        ),
        (
            ScalarType::SmallInt,
            ScalarValue::SmallInt(1),
            ScalarValue::SmallInt(1),
            ScalarValue::SmallInt(2),
        ),
        (
            ScalarType::Integer,
            ScalarValue::Integer(10),
            ScalarValue::Integer(10),
            ScalarValue::Integer(11),
        ),
        (
            ScalarType::BigInt,
            ScalarValue::BigInt(20),
            ScalarValue::BigInt(20),
            ScalarValue::BigInt(21),
        ),
        (
            ScalarType::Decimal {
                precision: 8,
                scale: 2,
            },
            ScalarValue::Decimal(1_100),
            ScalarValue::Decimal(1_100),
            ScalarValue::Decimal(1_200),
        ),
        (
            ScalarType::Text,
            ScalarValue::Text("same".to_owned()),
            ScalarValue::Text("left".to_owned()),
            ScalarValue::Text("right".to_owned()),
        ),
        (
            ScalarType::Varchar { max_chars: 32 },
            ScalarValue::Varchar("same".to_owned()),
            ScalarValue::Varchar("left".to_owned()),
            ScalarValue::Varchar("right".to_owned()),
        ),
        (
            ScalarType::Bytea,
            ScalarValue::Bytea(vec![1, 2]),
            ScalarValue::Bytea(vec![1, 2]),
            ScalarValue::Bytea(vec![1, 3]),
        ),
        (
            ScalarType::Uuid,
            ScalarValue::Uuid([1; 16]),
            ScalarValue::Uuid([1; 16]),
            ScalarValue::Uuid([2; 16]),
        ),
        (
            ScalarType::Date,
            ScalarValue::Date(10),
            ScalarValue::Date(10),
            ScalarValue::Date(11),
        ),
        (
            ScalarType::TimestampTz,
            ScalarValue::TimestampTz(100),
            ScalarValue::TimestampTz(100),
            ScalarValue::TimestampTz(101),
        ),
    ];
    let columns: Vec<_> = cases
        .iter()
        .flat_map(|(scalar_type, _, _, _)| {
            [
                ColumnSchema {
                    scalar_type: *scalar_type,
                    nullable: true,
                },
                ColumnSchema {
                    scalar_type: *scalar_type,
                    nullable: true,
                },
            ]
        })
        .collect();
    let column_ids: Vec<_> = (1..=columns.len())
        .map(|ordinal| [ordinal as u8; 16])
        .collect();
    let table_id = id(0).table;
    let equal_values: Vec<_> = cases
        .iter()
        .flat_map(|(_, equal, _, _)| [Some(equal.clone()), Some(equal.clone())])
        .collect();
    let unequal_values: Vec<_> = cases
        .iter()
        .flat_map(|(_, _, left, right)| [Some(left.clone()), Some(right.clone())])
        .collect();
    let null_values = vec![None; columns.len()];
    let mut seed = InvocationTransaction::begin(&database, [0x61; 16], [9; 16], 9_999, 8, 65_536)?;
    for (row_id, values) in [(1, equal_values), (2, unequal_values), (3, null_values)] {
        seed.stage_put(id(row_id), &encode_tuple(&columns, 1, &values)?)?;
    }
    seed.commit()?;

    let schema_id = [0x62; 16];
    let mut scope = std::collections::HashSet::from([
        (Operation::Usage, AccessTarget::Schema(schema_id)),
        (Operation::Select, AccessTarget::Table(table_id)),
    ]);
    let mut grants = vec![Grant {
        subject: Subject::Principal([9; 16]),
        operation: Operation::Usage,
        target: AccessTarget::Schema(schema_id),
    }];
    let mut policies = vec![Policy {
        subjects: vec![Subject::Principal([9; 16])],
        operation: Operation::Select,
        target: AccessTarget::Table(table_id),
        effect: Effect::Allow,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        with_check: None,
    }];
    for column_id in &column_ids {
        let target = AccessTarget::Column {
            table_id,
            column_id: *column_id,
        };
        scope.insert((Operation::Select, target));
        grants.push(Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Select,
            target,
        });
        policies.push(Policy {
            subjects: vec![Subject::Principal([9; 16])],
            operation: Operation::Select,
            target,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        });
    }
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope,
    };
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
    let mut observer =
        InvocationTransaction::begin(&database, [0x63; 16], [9; 16], 9_999, 8, 65_536)?;
    for (case_index, (scalar_type, _, _, _)) in cases.iter().enumerate() {
        let left_ordinal = case_index * 2;
        let right_ordinal = left_ordinal + 1;
        let predicate = |operator| BoundPredicate::Compare {
            operator,
            left: Box::new(BoundPredicate::Column(BoundColumn {
                column_id: column_ids[left_ordinal],
                ordinal: left_ordinal as u32,
                scalar_type: *scalar_type,
                nullable: true,
            })),
            right: Box::new(BoundPredicate::Column(BoundColumn {
                column_id: column_ids[right_ordinal],
                ordinal: right_ordinal as u32,
                scalar_type: *scalar_type,
                nullable: true,
            })),
        };
        assert_eq!(
            observer.estimate_authorized_predicate_at_most(
                table_id,
                &projection,
                &predicate(Comparison::Equal),
                3,
            )?,
            Some((3, 1)),
            "equality estimate for {scalar_type:?}"
        );
        assert_eq!(
            observer.estimate_authorized_predicate_at_most(
                table_id,
                &projection,
                &predicate(Comparison::NotEqual),
                3,
            )?,
            Some((3, 1)),
            "inequality estimate for {scalar_type:?}"
        );
        assert_eq!(
            observer.estimate_authorized_predicate_at_most(
                table_id,
                &projection,
                &predicate(Comparison::Less),
                3,
            )?,
            Some((3, 1)),
            "ordered estimate for {scalar_type:?}"
        );
    }
    let date_case = cases
        .iter()
        .position(|(scalar_type, _, _, _)| *scalar_type == ScalarType::Date)
        .expect("fixture has a DATE comparison pair");
    let date_ordinal = date_case * 2 + 1;
    let date_literal_predicate = BoundPredicate::Compare {
        operator: Comparison::Equal,
        left: Box::new(BoundPredicate::Column(BoundColumn {
            column_id: column_ids[date_ordinal],
            ordinal: date_ordinal as u32,
            scalar_type: ScalarType::Date,
            nullable: true,
        })),
        right: Box::new(BoundPredicate::Date(11)),
    };
    assert_eq!(
        observer.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &date_literal_predicate,
            3,
        )?,
        Some((3, 1)),
        "DATE literals should compare against authorized DATE cells"
    );
    let uuid_case = cases
        .iter()
        .position(|(scalar_type, _, _, _)| *scalar_type == ScalarType::Uuid)
        .expect("fixture has a UUID comparison pair");
    let uuid_ordinal = uuid_case * 2 + 1;
    let uuid_literal_predicate = BoundPredicate::Compare {
        operator: Comparison::Equal,
        left: Box::new(BoundPredicate::Column(BoundColumn {
            column_id: column_ids[uuid_ordinal],
            ordinal: uuid_ordinal as u32,
            scalar_type: ScalarType::Uuid,
            nullable: true,
        })),
        right: Box::new(BoundPredicate::Uuid([2; 16])),
    };
    assert_eq!(
        observer.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &uuid_literal_predicate,
            3,
        )?,
        Some((3, 1)),
        "UUID literals should compare against authorized UUID cells"
    );
    let timestamp_case = cases
        .iter()
        .position(|(scalar_type, _, _, _)| *scalar_type == ScalarType::TimestampTz)
        .expect("fixture has a TIMESTAMPTZ comparison pair");
    let timestamp_ordinal = timestamp_case * 2 + 1;
    let timestamp_literal_predicate = BoundPredicate::Compare {
        operator: Comparison::Equal,
        left: Box::new(BoundPredicate::Column(BoundColumn {
            column_id: column_ids[timestamp_ordinal],
            ordinal: timestamp_ordinal as u32,
            scalar_type: ScalarType::TimestampTz,
            nullable: true,
        })),
        right: Box::new(BoundPredicate::TimestampTz(100)),
    };
    assert_eq!(
        observer.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &timestamp_literal_predicate,
            3,
        )?,
        Some((3, 1)),
        "TIMESTAMPTZ literals should compare against authorized cells"
    );
    let bytea_case = cases
        .iter()
        .position(|(scalar_type, _, _, _)| *scalar_type == ScalarType::Bytea)
        .expect("fixture has a BYTEA comparison pair");
    let bytea_ordinal = bytea_case * 2 + 1;
    let bytea_literal_predicate = BoundPredicate::Compare {
        operator: Comparison::Equal,
        left: Box::new(BoundPredicate::Column(BoundColumn {
            column_id: column_ids[bytea_ordinal],
            ordinal: bytea_ordinal as u32,
            scalar_type: ScalarType::Bytea,
            nullable: true,
        })),
        right: Box::new(BoundPredicate::Bytea(vec![1, 2])),
    };
    assert_eq!(
        observer.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &bytea_literal_predicate,
            3,
        )?,
        Some((3, 1)),
        "BYTEA literals should compare against authorized cells"
    );
    let decimal_case = cases
        .iter()
        .position(|(scalar_type, _, _, _)| matches!(scalar_type, ScalarType::Decimal { .. }))
        .expect("fixture has a DECIMAL comparison pair");
    let decimal_ordinal = decimal_case * 2 + 1;
    let decimal_literal_predicate = BoundPredicate::Compare {
        operator: Comparison::Equal,
        left: Box::new(BoundPredicate::Column(BoundColumn {
            column_id: column_ids[decimal_ordinal],
            ordinal: decimal_ordinal as u32,
            scalar_type: ScalarType::Decimal {
                precision: 8,
                scale: 2,
            },
            nullable: true,
        })),
        right: Box::new(BoundPredicate::Decimal(1_200)),
    };
    assert_eq!(
        observer.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &decimal_literal_predicate,
            3,
        )?,
        Some((3, 1)),
        "DECIMAL literals should compare at the bound column scale"
    );
    let invalid_date_literal = BoundPredicate::Compare {
        operator: Comparison::Equal,
        left: Box::new(BoundPredicate::Column(BoundColumn {
            column_id: column_ids[date_ordinal],
            ordinal: date_ordinal as u32,
            scalar_type: ScalarType::Date,
            nullable: true,
        })),
        right: Box::new(BoundPredicate::Date(i32::MAX)),
    };
    assert_eq!(
        observer.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &invalid_date_literal,
            3,
        )?,
        None,
        "DATE estimates must reject values outside the scalar range"
    );
    let incompatible_column_predicate = BoundPredicate::Compare {
        operator: Comparison::Equal,
        left: Box::new(BoundPredicate::Column(BoundColumn {
            column_id: column_ids[2],
            ordinal: 2,
            scalar_type: ScalarType::SmallInt,
            nullable: true,
        })),
        right: Box::new(BoundPredicate::Column(BoundColumn {
            column_id: column_ids[4],
            ordinal: 4,
            scalar_type: ScalarType::Integer,
            nullable: true,
        })),
    };
    assert_eq!(
        observer.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &incompatible_column_predicate,
            3,
        )?,
        None,
        "same-width estimate values must not permit cross-type comparisons"
    );
    let bytea_case = cases
        .iter()
        .position(|(scalar_type, _, _, _)| *scalar_type == ScalarType::Bytea)
        .expect("fixture has a BYTEA comparison pair");
    let bytea_left_ordinal = bytea_case * 2;
    let bytea_right_ordinal = bytea_left_ordinal + 1;
    let mut oversized_values: Vec<_> = cases
        .iter()
        .flat_map(|(_, equal, _, _)| [Some(equal.clone()), Some(equal.clone())])
        .collect();
    oversized_values[bytea_left_ordinal] = Some(ScalarValue::Bytea(vec![7; 4097]));
    oversized_values[bytea_right_ordinal] = Some(ScalarValue::Bytea(vec![7; 4097]));
    let oversized_predicate = BoundPredicate::Compare {
        operator: Comparison::Equal,
        left: Box::new(BoundPredicate::Column(BoundColumn {
            column_id: column_ids[bytea_left_ordinal],
            ordinal: bytea_left_ordinal as u32,
            scalar_type: ScalarType::Bytea,
            nullable: true,
        })),
        right: Box::new(BoundPredicate::Column(BoundColumn {
            column_id: column_ids[bytea_right_ordinal],
            ordinal: bytea_right_ordinal as u32,
            scalar_type: ScalarType::Bytea,
            nullable: true,
        })),
    };
    let mut oversized =
        InvocationTransaction::begin(&database, [0x64; 16], [9; 16], 9_999, 8, 65_536)?;
    oversized.stage_put(id(4), &encode_tuple(&columns, 1, &oversized_values)?)?;
    assert_eq!(
        oversized.estimate_authorized_predicate_at_most(
            table_id,
            &projection,
            &oversized_predicate,
            4,
        )?,
        None,
        "oversized variable-width predicate cells must fall back without partial counts"
    );
    Ok(())
}
