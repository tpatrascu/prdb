use super::{InvocationCoordination, InvocationTransaction};
use crate::authorization::{
    AccessTarget, AuthorizationContext, Effect, Expr, Grant, Operation, Policy, Subject,
};
use crate::tuple::{ColumnSchema, ScalarType, ScalarValue};
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn bounded_row_estimate_does_not_consume_query_scan_work() {
    static NEXT_FIXTURE: AtomicUsize = AtomicUsize::new(0);
    let root = std::env::temp_dir().join(format!(
        "prdb-estimate-scan-budget-{}-{}",
        std::process::id(),
        NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&root);
    let mut bootstrap = crate::bootstrap::initialize(&root, "estimate_admin", i64::MAX)
        .expect("initialize estimate scan budget fixture");
    bootstrap.take_secret().expect("take bootstrap secret");
    let instance = bootstrap
        .acknowledge()
        .expect("publish estimate scan budget fixture");
    let database = instance.database();
    let table_id = [0x21; 16];
    let schema_id = [0x22; 16];
    let column_id = [0x23; 16];
    let principal_id = [0x24; 16];
    let schema_target = AccessTarget::Schema(schema_id);
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Text,
        nullable: true,
    }];
    let column_ids = [column_id];
    let encode_row = |value: Option<ScalarValue>| {
        crate::tuple::encode_tuple(&columns, 1, &[value]).expect("encode fixture row")
    };
    let row_id = |value: u64| crate::overlay::RowId {
        table: table_id,
        row: u128::from(value).to_be_bytes(),
    };
    let mut seed =
        InvocationTransaction::begin(database, [0x25; 16], principal_id, i64::MAX, 4, 512)
            .expect("begin estimate fixture seed");
    seed.stage_put(
        row_id(1),
        &encode_row(Some(ScalarValue::Text("value".to_owned()))),
    )
    .expect("stage readable fixture row");
    seed.stage_put(row_id(2), &encode_row(None))
        .expect("stage NULL fixture row");
    seed.commit().expect("commit estimate fixture rows");

    let table_target = AccessTarget::Table(table_id);
    let column_target = AccessTarget::Column {
        table_id,
        column_id,
    };
    let scope = HashSet::from([
        (Operation::Usage, schema_target),
        (Operation::Select, table_target),
        (Operation::Select, column_target),
    ]);
    let context = AuthorizationContext {
        principal_id,
        role_ids: HashSet::new(),
        scope,
    };
    let grants = [
        Grant {
            subject: Subject::Principal(principal_id),
            operation: Operation::Usage,
            target: schema_target,
        },
        Grant {
            subject: Subject::Principal(principal_id),
            operation: Operation::Select,
            target: table_target,
        },
        Grant {
            subject: Subject::Principal(principal_id),
            operation: Operation::Select,
            target: column_target,
        },
    ];
    let policies = [
        Policy {
            subjects: vec![Subject::Principal(principal_id)],
            operation: Operation::Select,
            target: table_target,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Principal(principal_id)],
            operation: Operation::Select,
            target: column_target,
            effect: Effect::Allow,
            using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            with_check: None,
        },
    ];
    let projection = crate::authorization::AuthorizedProjection {
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
    let predicate = crate::optimizer_binding::BoundPredicate::IsNull(Box::new(
        crate::optimizer_binding::BoundPredicate::Column(crate::optimizer_binding::BoundColumn {
            column_id,
            ordinal: 0,
            scalar_type: ScalarType::Text,
            nullable: true,
        }),
    ));
    let mut coordination = InvocationCoordination::uncoordinated();
    coordination.max_scan_work = 1;
    let mut invocation = InvocationTransaction::begin_inner(
        database,
        coordination,
        [0x26; 16],
        principal_id,
        i64::MAX,
        4,
        512,
    )
    .expect("begin one-unit scan-work invocation");

    assert_eq!(
        invocation
            .estimate_authorized_predicate_at_most(table_id, &projection, &predicate, 4)
            .expect("estimate predicate over bounded authorized rows"),
        Some((2, 1))
    );
    let page = invocation
        .scan_authorized(table_id, None, 1, &projection)
        .expect("one query scan unit must remain after estimating two candidates");
    assert_eq!(page.rows.len(), 1);
    assert!(page.continuation.is_some());
    invocation
        .abort()
        .expect("abort estimate fixture invocation");

    let value_dependent_policy = Policy {
        using: Some(Expr::Equal(
            Box::new(Expr::Field(0)),
            Box::new(Expr::Literal(Some(ScalarValue::Text("value".to_owned())))),
        )),
        ..policies[0].clone()
    };
    let filtered_policies = [value_dependent_policy, policies[1].clone()];
    let filtered_projection = crate::authorization::AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id,
        context: &context,
        grants: &grants,
        policies: &filtered_policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let mut few_hidden_coordination = InvocationCoordination::uncoordinated();
    few_hidden_coordination.max_scan_work = 1;
    let mut few_hidden = InvocationTransaction::begin_inner(
        database,
        few_hidden_coordination,
        [0x27; 16],
        principal_id,
        i64::MAX,
        16_384,
        2 * 1024 * 1024,
    )
    .expect("begin few-hidden-row invocation");
    // Keep this transaction's snapshot before publishing the hidden rows so the
    // paired estimate compares committed candidate populations under one policy.
    let hidden_candidate_count =
        if std::env::var_os("PRDB_TEST_PROFILE_HIDDEN_ESTIMATE_CPU").is_some() {
            8_192
        } else {
            10
        };
    let mut hidden_seed = InvocationTransaction::begin_inner(
        database,
        InvocationCoordination::uncoordinated(),
        [0x28; 16],
        principal_id,
        i64::MAX,
        hidden_candidate_count + 2,
        2 * 1024 * 1024,
    )
    .expect("begin hidden-row seed invocation");
    for hidden_id in 3..=hidden_candidate_count + 2 {
        hidden_seed
            .stage_put(
                row_id(u64::try_from(hidden_id).expect("hidden row ID fits u64")),
                &encode_row(Some(ScalarValue::Text(format!("hidden-{hidden_id}")))),
            )
            .expect("stage hidden committed candidate");
    }
    hidden_seed
        .commit()
        .expect("commit rows hidden by the value-dependent policy");
    let mut many_hidden_coordination = InvocationCoordination::uncoordinated();
    many_hidden_coordination.max_scan_work = 1;
    let mut many_hidden = InvocationTransaction::begin_inner(
        database,
        many_hidden_coordination,
        [0x29; 16],
        principal_id,
        i64::MAX,
        16_384,
        2 * 1024 * 1024,
    )
    .expect("begin many-hidden-row invocation");
    assert_eq!(
        few_hidden
            .estimate_authorized_predicate_at_most(table_id, &filtered_projection, &predicate, 16,)
            .expect("unknown estimate for value-dependent policy"),
        None
    );
    assert_eq!(
        many_hidden
            .estimate_authorized_predicate_at_most(table_id, &filtered_projection, &predicate, 16,)
            .expect("unknown estimate with additional hidden candidates"),
        None
    );
    let few_hidden_page = few_hidden
        .scan_authorized(table_id, None, 1, &filtered_projection)
        .expect("one-unit query scan after unknown estimate");
    let many_hidden_page = many_hidden
        .scan_authorized(table_id, None, 1, &filtered_projection)
        .expect("same one-unit query scan after hidden candidates increase");
    assert_eq!(few_hidden_page.rows, many_hidden_page.rows);
    assert_eq!(few_hidden_page.rows.len(), 1);
    assert!(few_hidden_page.continuation.is_some());
    if std::env::var_os("PRDB_TEST_PROFILE_HIDDEN_ESTIMATE_CPU").is_some() {
        fn thread_cpu_time() -> std::time::Duration {
            let mut value = libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            };
            // SAFETY: `value` is a valid writable timespec for the duration of the call.
            let status = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut value) };
            assert_eq!(status, 0, "read current thread CPU clock");
            std::time::Duration::new(
                u64::try_from(value.tv_sec).expect("thread CPU seconds are nonnegative"),
                u32::try_from(value.tv_nsec).expect("thread CPU nanoseconds fit u32"),
            )
        }
        const PROFILE_SAMPLES: usize = 5;
        const ESTIMATES_PER_SAMPLE: usize = 1_024;
        let mut few_hidden_times = Vec::with_capacity(PROFILE_SAMPLES);
        let mut many_hidden_times = Vec::with_capacity(PROFILE_SAMPLES);
        let measure_estimates = |transaction: &mut InvocationTransaction<'_>| {
            let started = thread_cpu_time();
            for _ in 0..ESTIMATES_PER_SAMPLE {
                assert_eq!(
                    transaction
                        .estimate_authorized_predicate_at_most(
                            table_id,
                            &filtered_projection,
                            &predicate,
                            16_384,
                        )
                        .expect("repeat hidden-candidate estimate"),
                    None
                );
            }
            thread_cpu_time().saturating_sub(started)
        };
        for sample in 0..PROFILE_SAMPLES {
            if sample % 2 == 0 {
                few_hidden_times.push(measure_estimates(&mut few_hidden));
                many_hidden_times.push(measure_estimates(&mut many_hidden));
            } else {
                many_hidden_times.push(measure_estimates(&mut many_hidden));
                few_hidden_times.push(measure_estimates(&mut few_hidden));
            }
        }
        few_hidden_times.sort_unstable();
        many_hidden_times.sort_unstable();
        eprintln!(
            "hidden-row estimate thread-CPU profile: hidden_committed_rows={hidden_candidate_count}, estimates_per_sample={ESTIMATES_PER_SAMPLE}, samples={PROFILE_SAMPLES}, few-hidden p50={:.3} ms, many-hidden p50={:.3} ms",
            few_hidden_times[few_hidden_times.len() / 2].as_secs_f64() * 1_000.0,
            many_hidden_times[many_hidden_times.len() / 2].as_secs_f64() * 1_000.0,
        );
    }
    few_hidden.abort().expect("abort few-hidden invocation");
    many_hidden.abort().expect("abort many-hidden invocation");

    drop(instance);
    std::fs::remove_dir_all(root).expect("remove estimate scan budget fixture");
}
