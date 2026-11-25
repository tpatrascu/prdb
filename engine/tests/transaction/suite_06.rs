use prdb_engine::authorization::{
    AccessTarget, AuthorizationContext, AuthorizedProjection, Effect, Expr, Grant, Operation,
    Policy, SanitizedCell, Subject,
};
use prdb_engine::commit::CommitError;
use prdb_engine::metadata::EngineMetadata;
use prdb_engine::outcome::OutcomeStatus;
use prdb_engine::schema::{self, CATALOG, ENGINE_META, ROWS};
use prdb_engine::transaction::{InvocationTransaction, TransactionError};
use prdb_engine::tuple::{ScalarValue, encode_tuple};
use redb::ReadableDatabase;
use std::time::{Duration, Instant};

use super::helpers::*;

pub(super) fn unique_constraint_uses_its_backing_index_for_invocation_writes()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_id = [0x10; 16];
    let column_id = [0x66; 16];
    let index_id = [0x55; 16];
    let constraint_id = [0x77; 16];
    let range_index_id = [0x56; 16];
    let (index_key, index_value) = index_catalog_record_with_constraint(
        index_id,
        table_id,
        column_id,
        1,
        true,
        Some(constraint_id),
    )?;
    let (constraint_key, constraint_value) =
        unique_constraint_catalog_record(constraint_id, table_id, column_id, index_id)?;
    let (range_index_key, range_index_value) =
        index_catalog_record(range_index_id, table_id, column_id, 1)?;
    {
        let write = database.begin_write()?;
        let mut catalog = write.open_table(CATALOG)?;
        catalog.insert(index_key.as_slice(), index_value.as_slice())?;
        catalog.insert(range_index_key.as_slice(), range_index_value.as_slice())?;
        catalog.insert(constraint_key.as_slice(), constraint_value.as_slice())?;
        drop(catalog);
        write.commit()?;
    }

    let index_profile_rows = std::env::var("PRDB_BENCH_INDEX_ROWS")
        .map(|value| {
            value
                .parse::<u64>()
                .expect("PRDB_BENCH_INDEX_ROWS must be a u64")
        })
        .unwrap_or(4_096);
    let index_profile_matches = std::env::var("PRDB_BENCH_INDEX_MATCHES")
        .map(|value| {
            value
                .parse::<u64>()
                .expect("PRDB_BENCH_INDEX_MATCHES must be a u64")
        })
        .unwrap_or(1);
    let index_profile_text = std::env::var_os("PRDB_BENCH_INDEX_TEXT").is_some();
    assert!(index_profile_matches <= index_profile_rows);
    let benchmark_value = |value: &str| {
        if index_profile_text {
            ScalarValue::Text(value.to_owned())
        } else {
            ScalarValue::Bytea(value.as_bytes().to_vec())
        }
    };
    let mut seed = InvocationTransaction::begin(
        &database,
        [241; 16],
        [9; 16],
        9_999,
        index_profile_rows as usize + 2,
        (index_profile_rows as usize + 2) * 2_048,
    )?;
    let seed_schema = seed.load_catalog_table_schema(table_id)?;
    seed.load_catalog_indexes(&seed_schema)?;
    let payload = encode_tuple(
        &seed_schema.columns,
        seed_schema.schema_version,
        &[Some(benchmark_value("duplicate"))],
    )?;
    seed.stage_put(id(243), &payload)?;
    let alternate_payload = encode_tuple(
        &seed_schema.columns,
        seed_schema.schema_version,
        &[Some(benchmark_value("alternate duplicate"))],
    )?;
    seed.stage_put(id(245), &alternate_payload)?;
    for number in 0..index_profile_rows {
        let value = format!("key-{number:05}");
        let payload = encode_tuple(
            &seed_schema.columns,
            seed_schema.schema_version,
            &[Some(benchmark_value(&value))],
        )?;
        seed.stage_put(numbered_row_id(table_id, number + 1_000), &payload)?;
    }
    assert_eq!(seed.commit()?, 1);

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
    population_record[13..21].copy_from_slice(&(index_profile_rows + 2).to_le_bytes());
    {
        let write = database.begin_write()?;
        write
            .open_table(schema::STATS)?
            .insert(population_key.as_slice(), population_record.as_slice())?;
        write.commit()?;
    }

    let mut invocation =
        InvocationTransaction::begin(&database, [242; 16], [9; 16], 9_999, 8, 512)?;
    let schema = invocation.load_catalog_table_schema(table_id)?;
    assert!(
        invocation
            .load_catalog_check_constraints(&schema)?
            .is_empty()
    );
    let indexes = invocation.load_catalog_indexes(&schema)?;
    assert!(
        indexes
            .iter()
            .any(|index| index.index_id == index_id && index.unique)
    );
    let payload = encode_tuple(
        &schema.columns,
        schema.schema_version,
        &[Some(benchmark_value("duplicate"))],
    )?;
    let principal_id = [9; 16];
    let context = AuthorizationContext {
        principal_id,
        role_ids: std::collections::HashSet::new(),
        scope: [
            (Operation::Usage, AccessTarget::Schema(schema.schema_id)),
            (Operation::Select, AccessTarget::Table(table_id)),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id,
                },
            ),
        ]
        .into_iter()
        .collect(),
    };
    let grants = [
        Grant {
            subject: Subject::Principal(principal_id),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema.schema_id),
        },
        Grant {
            subject: Subject::Principal(principal_id),
            operation: Operation::Select,
            target: AccessTarget::Table(table_id),
        },
        Grant {
            subject: Subject::Principal(principal_id),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id,
            },
        },
    ];
    let policies = [
        Policy {
            subjects: vec![Subject::Principal(principal_id)],
            operation: Operation::Select,
            target: AccessTarget::Table(table_id),
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Principal(principal_id)],
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id,
            },
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
    ];
    let projection = AuthorizedProjection {
        columns: &schema.columns,
        column_ids: &schema.column_ids,
        schema_version: schema.schema_version,
        schema_id: schema.schema_id,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let visible_row_count = index_profile_rows + 2;
    let expected_small_population = (visible_row_count
        <= prdb_engine::optimizer_plan::SMALL_POPULATION_SCAN_ROWS)
        .then_some(visible_row_count);
    assert_eq!(
        invocation.estimate_authorized_population_at_most(
            table_id,
            &projection,
            prdb_engine::optimizer_plan::SMALL_POPULATION_SCAN_ROWS,
        )?,
        expected_small_population
    );
    let expected_join_population = (visible_row_count
        <= prdb_engine::optimizer_plan::JOIN_POPULATION_ESTIMATE_MAX_ROWS)
        .then_some(visible_row_count);
    assert_eq!(
        invocation.estimate_authorized_population_at_most(
            table_id,
            &projection,
            prdb_engine::optimizer_plan::JOIN_POPULATION_ESTIMATE_MAX_ROWS,
        )?,
        expected_join_population
    );
    let range_bound = || prdb_engine::transaction::AuthorizedIndexRangeBound {
        value: benchmark_value("duplicate"),
        inclusive: true,
    };
    let target_cell = SanitizedCell::Value(benchmark_value("duplicate"));
    let probe_values = [Some(benchmark_value("duplicate"))];
    let mut reference_matches = Vec::new();
    let mut reference_cursor = None;
    loop {
        let page =
            invocation.scan_authorized(table_id, reference_cursor.as_ref(), 128, &projection)?;
        reference_matches.extend(page.rows.into_iter().filter_map(|(row_id, cells)| {
            (cells.first() == Some(&target_cell)).then_some((row_id, cells))
        }));
        let Some(cursor) = page.continuation else {
            break;
        };
        reference_cursor = Some(cursor);
    }
    let unique_probe = invocation.probe_catalog_unique_index(
        table_id,
        index_id,
        &[Some(benchmark_value("duplicate"))],
        &projection,
    )?;
    assert!(matches!(
        &unique_probe,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Match { id, cells }
            if reference_matches.len() == 1
                && *id == reference_matches[0].0
                && cells == &reference_matches[0].1
    ));
    assert_eq!(
        invocation.probe_catalog_unique_index(table_id, index_id, &[None], &projection)?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::NoMatch,
        "SQL equality with NULL cannot match a unique-index key"
    );
    let matching_range = invocation.scan_catalog_index_range(
        table_id,
        range_index_id,
        Some(range_bound()),
        Some(range_bound()),
        &projection,
    )?;
    assert!(matches!(
        &matching_range,
        prdb_engine::transaction::AuthorizedIndexRangeScan::Rows(rows)
            if rows.len() == reference_matches.len()
                && rows.iter().zip(&reference_matches).all(|(indexed, reference)| {
                    indexed.0 == reference.0 && indexed.1 == reference.1
                })
    ));
    if std::env::var_os("PRDB_BENCH_INDEX_ACCESS").is_some() {
        const RUNS: usize = 11;
        let mut point_probe_times = Vec::with_capacity(RUNS);
        let mut range_times = Vec::with_capacity(RUNS);
        let mut reference_scan_times = Vec::with_capacity(RUNS);
        let mut small_population_probe_times = Vec::with_capacity(RUNS);
        for _ in 0..RUNS {
            let started = Instant::now();
            let small_population = invocation.estimate_authorized_population_at_most(
                table_id,
                &projection,
                prdb_engine::optimizer_plan::SMALL_POPULATION_SCAN_ROWS,
            )?;
            small_population_probe_times.push(started.elapsed());
            let total_rows = index_profile_rows + 2;
            let expected_population = (total_rows
                <= prdb_engine::optimizer_plan::SMALL_POPULATION_SCAN_ROWS)
                .then_some(total_rows);
            assert_eq!(small_population, expected_population);

            let started = Instant::now();
            let probe = invocation.probe_catalog_unique_index(
                table_id,
                index_id,
                &probe_values,
                &projection,
            )?;
            point_probe_times.push(started.elapsed());
            assert!(matches!(
                probe,
                prdb_engine::transaction::AuthorizedUniqueIndexProbe::Match { id: found, .. }
                    if found == id(243)
            ));

            let lower = Some(range_bound());
            let upper = Some(range_bound());
            let started = Instant::now();
            let range = invocation.scan_catalog_index_range(
                table_id,
                range_index_id,
                lower,
                upper,
                &projection,
            )?;
            range_times.push(started.elapsed());
            assert!(matches!(
                range,
                prdb_engine::transaction::AuthorizedIndexRangeScan::Rows(rows)
                    if rows.len() == 1 && rows[0].0 == id(243)
            ));

            let started = Instant::now();
            let mut cursor = None;
            let mut matches = Vec::new();
            loop {
                let page =
                    invocation.scan_authorized(table_id, cursor.as_ref(), 128, &projection)?;
                matches.extend(page.rows.into_iter().filter_map(|(row_id, cells)| {
                    (cells.first() == Some(&target_cell)).then_some(row_id)
                }));
                let Some(next) = page.continuation else {
                    break;
                };
                cursor = Some(next);
            }
            reference_scan_times.push(started.elapsed());
            assert_eq!(matches, [id(243)]);
        }
        let median_and_p95_ms = |samples: &mut [Duration]| {
            samples.sort_unstable();
            (
                samples[samples.len() / 2].as_secs_f64() * 1_000.0,
                samples[((samples.len() - 1) * 95).div_ceil(100)].as_secs_f64() * 1_000.0,
            )
        };
        let point_probe = median_and_p95_ms(&mut point_probe_times);
        let range = median_and_p95_ms(&mut range_times);
        let reference_scan = median_and_p95_ms(&mut reference_scan_times);
        let small_population_probe = median_and_p95_ms(&mut small_population_probe_times);
        eprintln!(
            "index access benchmark: numbered_rows={index_profile_rows}, total_rows={}, runs={RUNS}, small-population probe median/p95={:.3}/{:.3} ms, point probe median/p95={:.3}/{:.3} ms, range median/p95={:.3}/{:.3} ms, authorized scan+filter median/p95={:.3}/{:.3} ms",
            index_profile_rows + 2,
            small_population_probe.0,
            small_population_probe.1,
            point_probe.0,
            point_probe.1,
            range.0,
            range.1,
            reference_scan.0,
            reference_scan.1,
        );

        if index_profile_matches > 0 {
            let lower_value = format!("key-{:05}", 0);
            let upper_value = format!("key-{:05}", index_profile_matches - 1);
            let lower_key = lower_value.as_bytes().to_vec();
            let upper_key = upper_value.as_bytes().to_vec();
            let fanout_bound = |value: String| {
                Some(prdb_engine::transaction::AuthorizedIndexRangeBound {
                    value: benchmark_value(&value),
                    inclusive: true,
                })
            };
            let lower = fanout_bound(lower_value);
            let upper = fanout_bound(upper_value);
            let expected_rows = (0..index_profile_matches)
                .map(|number| numbered_row_id(table_id, number + 1_000))
                .collect::<Vec<_>>();
            let key_column = prdb_engine::optimizer_binding::BoundColumn {
                column_id,
                ordinal: 0,
                scalar_type: schema.columns[0].scalar_type,
                nullable: schema.columns[0].nullable,
            };
            let range_predicate = prdb_engine::optimizer_binding::BoundPredicate::And(vec![
                prdb_engine::optimizer_binding::BoundPredicate::Compare {
                    operator: prdb_engine::optimizer_request::Comparison::GreaterOrEqual,
                    left: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Column(
                        key_column.clone(),
                    )),
                    right: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Text(
                        lower_key.iter().copied().map(char::from).collect(),
                    )),
                },
                prdb_engine::optimizer_binding::BoundPredicate::Compare {
                    operator: prdb_engine::optimizer_request::Comparison::LessOrEqual,
                    left: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Column(
                        key_column,
                    )),
                    right: Box::new(prdb_engine::optimizer_binding::BoundPredicate::Text(
                        upper_key.iter().copied().map(char::from).collect(),
                    )),
                },
            ]);
            let mut predicate_estimate_times = Vec::with_capacity(RUNS);
            let mut range_fanout_times = Vec::with_capacity(RUNS);
            let mut scan_fanout_times = Vec::with_capacity(RUNS);
            for _ in 0..RUNS {
                let started = Instant::now();
                let estimate = invocation.estimate_authorized_predicate_at_most(
                    table_id,
                    &projection,
                    &range_predicate,
                    prdb_engine::statistics::MAX_PREDICATE_ESTIMATE_ROWS,
                )?;
                predicate_estimate_times.push(started.elapsed());
                assert_eq!(
                    estimate,
                    Some((index_profile_rows + 2, index_profile_matches))
                );

                let started = Instant::now();
                let indexed = invocation.scan_catalog_index_range(
                    table_id,
                    range_index_id,
                    lower.clone(),
                    upper.clone(),
                    &projection,
                )?;
                range_fanout_times.push(started.elapsed());
                assert!(matches!(
                    indexed,
                    prdb_engine::transaction::AuthorizedIndexRangeScan::Rows(rows)
                        if rows.iter().map(|(row_id, _)| *row_id).collect::<Vec<_>>() == expected_rows
                ));

                let started = Instant::now();
                let mut cursor = None;
                let mut scan_matches = Vec::new();
                loop {
                    let page =
                        invocation.scan_authorized(table_id, cursor.as_ref(), 128, &projection)?;
                    scan_matches.extend(page.rows.into_iter().filter_map(|(row_id, cells)| {
                        let matches = match cells.first() {
                            Some(SanitizedCell::Value(ScalarValue::Text(value))) => {
                                value.as_bytes() >= lower_key.as_slice()
                                    && value.as_bytes() <= upper_key.as_slice()
                            }
                            Some(SanitizedCell::Value(ScalarValue::Bytea(value))) => {
                                value.as_slice() >= lower_key.as_slice()
                                    && value.as_slice() <= upper_key.as_slice()
                            }
                            _ => false,
                        };
                        matches.then_some(row_id)
                    }));
                    let Some(next) = page.continuation else {
                        break;
                    };
                    cursor = Some(next);
                }
                scan_fanout_times.push(started.elapsed());
                assert_eq!(scan_matches, expected_rows);
            }
            let predicate_estimate = median_and_p95_ms(&mut predicate_estimate_times);
            let range_fanout = median_and_p95_ms(&mut range_fanout_times);
            let scan_fanout = median_and_p95_ms(&mut scan_fanout_times);
            eprintln!(
                "index selectivity benchmark: total_rows={}, matching_rows={}, predicate estimate median/p95={:.3}/{:.3} ms, range median/p95={:.3}/{:.3} ms, authorized scan+filter median/p95={:.3}/{:.3} ms",
                index_profile_rows + 2,
                index_profile_matches,
                predicate_estimate.0,
                predicate_estimate.1,
                range_fanout.0,
                range_fanout.1,
                scan_fanout.0,
                scan_fanout.1,
            );
        }
    }
    if index_profile_text {
        return Ok(());
    }
    let hidden_context = AuthorizationContext {
        principal_id,
        role_ids: std::collections::HashSet::new(),
        scope: [
            (Operation::Usage, AccessTarget::Schema(schema.schema_id)),
            (Operation::Select, AccessTarget::Table(table_id)),
        ]
        .into_iter()
        .collect(),
    };
    let hidden_projection = AuthorizedProjection {
        columns: projection.columns,
        column_ids: projection.column_ids,
        schema_version: projection.schema_version,
        schema_id: projection.schema_id,
        context: &hidden_context,
        grants: &grants[..2],
        policies: projection.policies,
        checks: projection.checks,
        policy_driving_columns: projection.policy_driving_columns,
        dependencies: projection.dependencies,
    };
    assert_eq!(
        invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(b"duplicate".to_vec()))],
            &hidden_projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Unavailable
    );
    assert_eq!(
        invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(
                b"absent key with another size".to_vec()
            ))],
            &hidden_projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Unavailable,
        "a hidden key must not distinguish a matching value from a missing value"
    );
    assert_eq!(
        invocation.probe_catalog_unique_index(table_id, index_id, &[None], &hidden_projection)?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Unavailable,
        "key readability must be checked before a NULL probe can expose its outcome"
    );
    let wrong_key_types = [
        ScalarValue::Boolean(true),
        ScalarValue::SmallInt(7),
        ScalarValue::Integer(7),
        ScalarValue::BigInt(7),
        ScalarValue::Decimal(7),
        ScalarValue::Text("duplicate".to_owned()),
        ScalarValue::Varchar("duplicate".to_owned()),
        ScalarValue::Uuid([7; 16]),
        ScalarValue::Date(7),
        ScalarValue::TimestampTz(7),
    ];
    for value in wrong_key_types {
        assert_eq!(
            invocation.probe_catalog_unique_index(
                table_id,
                index_id,
                &[Some(value)],
                &hidden_projection,
            )?,
            prdb_engine::transaction::AuthorizedUniqueIndexProbe::Unavailable,
            "a hidden BYTEA key must not reveal whether a mismatched equality type is accepted"
        );
    }
    let conditional_policies = [
        policies[0].clone(),
        Policy {
            using: Some(Expr::Equal(
                Box::new(Expr::Field(0)),
                Box::new(Expr::Literal(Some(ScalarValue::Bytea(
                    b"duplicate".to_vec(),
                )))),
            )),
            ..policies[1].clone()
        },
    ];
    let conditional_projection = AuthorizedProjection {
        columns: projection.columns,
        column_ids: projection.column_ids,
        schema_version: projection.schema_version,
        schema_id: projection.schema_id,
        context: projection.context,
        grants: projection.grants,
        policies: &conditional_policies,
        checks: projection.checks,
        policy_driving_columns: projection.policy_driving_columns,
        dependencies: projection.dependencies,
    };
    assert_eq!(
        invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(b"duplicate".to_vec()))],
            &conditional_projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Unavailable
    );
    assert_eq!(
        invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(
                b"not the conditional key".to_vec()
            ))],
            &conditional_projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Unavailable,
        "a conditionally readable key must always fall back before inspecting the predicate value"
    );
    let failing_policies = [
        policies[0].clone(),
        Policy {
            using: Some(Expr::And(vec![
                Expr::Equal(
                    Box::new(Expr::Divide(
                        Box::new(Expr::Literal(Some(ScalarValue::SmallInt(1)))),
                        Box::new(Expr::Literal(Some(ScalarValue::SmallInt(0)))),
                    )),
                    Box::new(Expr::Literal(Some(ScalarValue::SmallInt(0)))),
                ),
                Expr::Equal(
                    Box::new(Expr::CurrentPrincipalId),
                    Box::new(Expr::Literal(Some(ScalarValue::Uuid([9; 16])))),
                ),
            ])),
            ..policies[1].clone()
        },
    ];
    let failing_projection = AuthorizedProjection {
        columns: projection.columns,
        column_ids: projection.column_ids,
        schema_version: projection.schema_version,
        schema_id: projection.schema_id,
        context: projection.context,
        grants: projection.grants,
        policies: &failing_policies,
        checks: projection.checks,
        policy_driving_columns: &[column_id],
        dependencies: projection.dependencies,
    };
    let mut failing_invocation =
        InvocationTransaction::begin(&database, [0x48; 16], [9; 16], 9_999, 8, 512)?;
    failing_invocation.load_catalog_table_schema(table_id)?;
    assert_eq!(
        failing_invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(b"duplicate".to_vec()))],
            &failing_projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Unavailable,
        "an erroring policy predicate must force the authorized reference scan"
    );
    assert!(matches!(
        failing_invocation.scan_authorized(table_id, None, 8, &failing_projection),
        Err(TransactionError::Authorization(
            prdb_engine::authorization::AuthorizationError::EvaluationFailed
        ))
    ));
    assert_eq!(
        prdb_engine::outcome::lookup_outcome(&database, [0x48; 16], [9; 16], 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted,
        "the failing reference predicate must durably abort the invocation"
    );
    let modulo_error_policies = [
        policies[0].clone(),
        Policy {
            using: Some(Expr::And(vec![
                Expr::Equal(
                    Box::new(Expr::Modulo(
                        Box::new(Expr::Literal(Some(ScalarValue::SmallInt(1)))),
                        Box::new(Expr::Literal(Some(ScalarValue::SmallInt(0)))),
                    )),
                    Box::new(Expr::Literal(Some(ScalarValue::SmallInt(0)))),
                ),
                Expr::Equal(
                    Box::new(Expr::CurrentPrincipalId),
                    Box::new(Expr::Literal(Some(ScalarValue::Uuid([9; 16])))),
                ),
            ])),
            ..policies[1].clone()
        },
    ];
    let modulo_error_projection = AuthorizedProjection {
        policies: &modulo_error_policies,
        ..failing_projection
    };
    let mut modulo_error_invocation =
        InvocationTransaction::begin(&database, [0x49; 16], [9; 16], 9_999, 8, 512)?;
    modulo_error_invocation.load_catalog_table_schema(table_id)?;
    assert_eq!(
        modulo_error_invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(b"duplicate".to_vec()))],
            &modulo_error_projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Unavailable,
        "a modulo-erroring policy must force the authorized reference scan"
    );
    assert!(matches!(
        modulo_error_invocation.scan_authorized(table_id, None, 8, &modulo_error_projection),
        Err(TransactionError::Authorization(
            prdb_engine::authorization::AuthorizationError::EvaluationFailed
        ))
    ));
    assert_eq!(
        prdb_engine::outcome::lookup_outcome(&database, [0x49; 16], [9; 16], 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted,
        "the modulo-erroring reference predicate must durably abort the invocation"
    );
    let negation_error_policies = [
        policies[0].clone(),
        Policy {
            using: Some(Expr::And(vec![
                Expr::Equal(
                    Box::new(Expr::Negate(Box::new(Expr::Literal(Some(
                        ScalarValue::SmallInt(i16::MIN),
                    ))))),
                    Box::new(Expr::Literal(Some(ScalarValue::SmallInt(0)))),
                ),
                Expr::Equal(
                    Box::new(Expr::CurrentPrincipalId),
                    Box::new(Expr::Literal(Some(ScalarValue::Uuid([9; 16])))),
                ),
            ])),
            ..policies[1].clone()
        },
    ];
    let negation_error_projection = AuthorizedProjection {
        columns: projection.columns,
        column_ids: projection.column_ids,
        schema_version: projection.schema_version,
        schema_id: projection.schema_id,
        context: projection.context,
        grants: projection.grants,
        policies: &negation_error_policies,
        checks: projection.checks,
        policy_driving_columns: &[column_id],
        dependencies: projection.dependencies,
    };
    let mut negation_error_invocation =
        InvocationTransaction::begin(&database, [0x4a; 16], [9; 16], 9_999, 8, 512)?;
    negation_error_invocation.load_catalog_table_schema(table_id)?;
    assert_eq!(
        negation_error_invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(b"duplicate".to_vec()))],
            &negation_error_projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Unavailable,
        "an overflowing-negation policy must force the authorized reference scan"
    );
    assert!(matches!(
        negation_error_invocation.scan_authorized(table_id, None, 8, &negation_error_projection),
        Err(TransactionError::Authorization(
            prdb_engine::authorization::AuthorizationError::EvaluationFailed
        ))
    ));
    assert_eq!(
        prdb_engine::outcome::lookup_outcome(&database, [0x4a; 16], [9; 16], 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted,
        "the overflowing-negation reference predicate must durably abort the invocation"
    );
    let addition_error_policies = [
        policies[0].clone(),
        Policy {
            using: Some(Expr::And(vec![
                Expr::Equal(
                    Box::new(Expr::Add(
                        Box::new(Expr::Literal(Some(ScalarValue::SmallInt(i16::MAX)))),
                        Box::new(Expr::Literal(Some(ScalarValue::SmallInt(1)))),
                    )),
                    Box::new(Expr::Literal(Some(ScalarValue::SmallInt(0)))),
                ),
                Expr::Equal(
                    Box::new(Expr::CurrentPrincipalId),
                    Box::new(Expr::Literal(Some(ScalarValue::Uuid([9; 16])))),
                ),
            ])),
            ..policies[1].clone()
        },
    ];
    let addition_error_projection = AuthorizedProjection {
        policies: &addition_error_policies,
        ..failing_projection
    };
    let mut addition_error_invocation =
        InvocationTransaction::begin(&database, [0x4b; 16], [9; 16], 9_999, 8, 512)?;
    addition_error_invocation.load_catalog_table_schema(table_id)?;
    assert_eq!(
        addition_error_invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(b"duplicate".to_vec()))],
            &addition_error_projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Unavailable,
        "an overflowing-addition policy must force the authorized reference scan"
    );
    assert!(matches!(
        addition_error_invocation.scan_authorized(table_id, None, 8, &addition_error_projection),
        Err(TransactionError::Authorization(
            prdb_engine::authorization::AuthorizationError::EvaluationFailed
        ))
    ));
    assert_eq!(
        prdb_engine::outcome::lookup_outcome(&database, [0x4b; 16], [9; 16], 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted,
        "the overflowing-addition reference predicate must durably abort the invocation"
    );
    for (offset, (label, arithmetic)) in [
        (
            "multiplication",
            Expr::Multiply(
                Box::new(Expr::Literal(Some(ScalarValue::SmallInt(i16::MAX)))),
                Box::new(Expr::Literal(Some(ScalarValue::SmallInt(2)))),
            ),
        ),
        (
            "subtraction",
            Expr::Subtract(
                Box::new(Expr::Literal(Some(ScalarValue::SmallInt(i16::MIN)))),
                Box::new(Expr::Literal(Some(ScalarValue::SmallInt(1)))),
            ),
        ),
        (
            "division",
            Expr::Divide(
                Box::new(Expr::Literal(Some(ScalarValue::SmallInt(i16::MIN)))),
                Box::new(Expr::Literal(Some(ScalarValue::SmallInt(-1)))),
            ),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let invocation_id = [0x4c + offset as u8; 16];
        let arithmetic_error_policies = [
            policies[0].clone(),
            Policy {
                using: Some(Expr::And(vec![
                    Expr::Equal(
                        Box::new(arithmetic),
                        Box::new(Expr::Literal(Some(ScalarValue::SmallInt(0)))),
                    ),
                    Expr::Equal(
                        Box::new(Expr::CurrentPrincipalId),
                        Box::new(Expr::Literal(Some(ScalarValue::Uuid([9; 16])))),
                    ),
                ])),
                ..policies[1].clone()
            },
        ];
        let arithmetic_error_projection = AuthorizedProjection {
            policies: &arithmetic_error_policies,
            ..failing_projection
        };
        let mut arithmetic_error_invocation =
            InvocationTransaction::begin(&database, invocation_id, [9; 16], 9_999, 8, 512)?;
        arithmetic_error_invocation.load_catalog_table_schema(table_id)?;
        assert_eq!(
            arithmetic_error_invocation.probe_catalog_unique_index(
                table_id,
                index_id,
                &[Some(ScalarValue::Bytea(b"duplicate".to_vec()))],
                &arithmetic_error_projection,
            )?,
            prdb_engine::transaction::AuthorizedUniqueIndexProbe::Unavailable,
            "an overflowing-{label} policy must force the authorized reference scan"
        );
        assert!(matches!(
            arithmetic_error_invocation.scan_authorized(
                table_id,
                None,
                8,
                &arithmetic_error_projection,
            ),
            Err(TransactionError::Authorization(
                prdb_engine::authorization::AuthorizationError::EvaluationFailed
            ))
        ));
        assert_eq!(
            prdb_engine::outcome::lookup_outcome(&database, invocation_id, [9; 16], 1,)?
                .unwrap()
                .status,
            OutcomeStatus::Aborted,
            "the overflowing-{label} reference predicate must durably abort the invocation"
        );
    }
    assert_eq!(
        invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(b"duplicate".to_vec()))],
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Match {
            id: id(243),
            cells: vec![SanitizedCell::Value(ScalarValue::Bytea(
                b"duplicate".to_vec()
            ))],
        },
    );
    assert_eq!(
        invocation.scan_catalog_index_range(
            table_id,
            range_index_id,
            Some(range_bound()),
            Some(range_bound()),
            &conditional_projection,
        )?,
        prdb_engine::transaction::AuthorizedIndexRangeScan::Unavailable,
        "conditional key permissions must use the reference scan"
    );
    assert_eq!(
        invocation.scan_catalog_index_range(
            table_id,
            range_index_id,
            Some(range_bound()),
            Some(range_bound()),
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedIndexRangeScan::Rows(vec![(
            id(243),
            vec![SanitizedCell::Value(ScalarValue::Bytea(
                b"duplicate".to_vec()
            ))],
        )]),
    );
    assert_eq!(
        invocation.scan_catalog_index_range(
            table_id,
            range_index_id,
            Some(range_bound()),
            Some(range_bound()),
            &hidden_projection,
        )?,
        prdb_engine::transaction::AuthorizedIndexRangeScan::Unavailable
    );
    invocation.stage_put(
        id(243),
        &encode_tuple(
            &schema.columns,
            schema.schema_version,
            &[Some(ScalarValue::Bytea(b"moved-away".to_vec()))],
        )?,
    )?;
    assert_eq!(
        invocation.scan_catalog_index_range(
            table_id,
            range_index_id,
            Some(range_bound()),
            Some(range_bound()),
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedIndexRangeScan::Rows(Vec::new()),
        "staged key changes must shadow the pinned index candidate"
    );
    let moved_bound = || prdb_engine::transaction::AuthorizedIndexRangeBound {
        value: ScalarValue::Bytea(b"moved-away".to_vec()),
        inclusive: true,
    };
    assert!(matches!(
        invocation.scan_catalog_index_range(
            table_id,
            range_index_id,
            Some(moved_bound()),
            Some(moved_bound()),
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedIndexRangeScan::Rows(rows)
            if rows.len() == 1 && rows[0].0 == id(243)
    ));
    assert_eq!(
        invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(b"duplicate".to_vec()))],
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::NoMatch
    );
    assert!(matches!(
        invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(b"moved-away".to_vec()))],
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Match { id: found, .. }
            if found == id(243)
    ));
    invocation.stage_delete(id(243))?;
    assert_eq!(
        invocation.scan_catalog_index_range(
            table_id,
            range_index_id,
            Some(range_bound()),
            Some(range_bound()),
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedIndexRangeScan::Rows(Vec::new()),
        "staged deletes must shadow pinned range candidates"
    );
    assert_eq!(
        invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(b"duplicate".to_vec()))],
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::NoMatch
    );
    invocation.stage_put(id(243), &payload)?;
    invocation.stage_put(id(244), &payload)?;
    assert!(matches!(
        invocation.scan_catalog_index_range(
            table_id,
            range_index_id,
            Some(range_bound()),
            Some(range_bound()),
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedIndexRangeScan::Rows(rows)
            if rows.len() == 2
    ));
    assert_eq!(
        invocation.probe_catalog_unique_index(
            table_id,
            index_id,
            &[Some(ScalarValue::Bytea(b"duplicate".to_vec()))],
            &projection,
        )?,
        prdb_engine::transaction::AuthorizedUniqueIndexProbe::Unavailable,
    );
    let first_unique_error = invocation
        .commit()
        .expect_err("duplicate indexed values must fail the first commit");
    assert!(matches!(
        &first_unique_error,
        TransactionError::Commit(CommitError::UniqueViolation)
    ));

    let mut alternate_invocation =
        InvocationTransaction::begin(&database, [0x47; 16], [9; 16], 9_999, 8, 512)?;
    let alternate_schema = alternate_invocation.load_catalog_table_schema(table_id)?;
    alternate_invocation.load_catalog_check_constraints(&alternate_schema)?;
    alternate_invocation.load_catalog_indexes(&alternate_schema)?;
    alternate_invocation.stage_put(id(246), &alternate_payload)?;
    let alternate_unique_error = alternate_invocation
        .commit()
        .expect_err("the second duplicate indexed value must fail its commit");
    assert_eq!(
        format!("{first_unique_error:?}"),
        format!("{alternate_unique_error:?}"),
        "unique diagnostics must not include conflicting values or row identities"
    );
    assert!(matches!(
        alternate_unique_error,
        TransactionError::Commit(CommitError::UniqueViolation)
    ));

    let read = database.begin_read()?;
    assert!(read.open_table(ROWS)?.get(&key(id(243))[..])?.is_some());
    assert!(read.open_table(ROWS)?.get(&key(id(244))[..])?.is_none());
    assert_eq!(
        prdb_engine::outcome::lookup_outcome(&database, [242; 16], [9; 16], 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}
