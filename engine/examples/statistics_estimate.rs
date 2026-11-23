use prdb_engine::authorization::{
    AccessTarget, AuthorizationContext, AuthorizedProjection, Effect, Expr, Grant, Operation,
    Policy, Subject,
};
use prdb_engine::bootstrap;
use prdb_engine::optimizer_binding::{BoundColumn, BoundPredicate};
use prdb_engine::optimizer_request::Comparison;
use prdb_engine::overlay::RowId;
use prdb_engine::statistics::{
    AuthorizedScanEstimate, MAX_ESTIMATE_CANDIDATES, MAX_PREDICATE_ESTIMATE_ROWS,
};
use prdb_engine::transaction::InvocationTransaction;
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, encode_tuple};
use std::collections::HashSet;
use std::error::Error;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use zeroize::Zeroize;

const DATASET_ROWS: [usize; 4] = [
    1_024,
    8_192,
    MAX_ESTIMATE_CANDIDATES - 1,
    MAX_ESTIMATE_CANDIDATES,
];
const ESTIMATE_RUNS: usize = 11;
const SCHEMA_ID: [u8; 16] = [0x72; 16];
const COLUMN_ID: [u8; 16] = [0x73; 16];
const PRINCIPAL_ID: [u8; 16] = [0x74; 16];

struct TimingProfile {
    median: Duration,
    p95: Duration,
}

struct DatasetProfile {
    row_count: usize,
    population_estimate: TimingProfile,
    predicate_estimate: Option<TimingProfile>,
}

fn table_id(dataset: usize) -> [u8; 16] {
    [0x71 + dataset as u8; 16]
}

fn row_id(table: [u8; 16], number: usize) -> RowId {
    RowId {
        table,
        row: (number as u128).to_be_bytes(),
    }
}

fn invocation_id(number: usize) -> [u8; 16] {
    let mut id = [0xa5; 16];
    id[8..].copy_from_slice(&(number as u64).to_be_bytes());
    id
}

fn temporary_data_directory() -> Result<PathBuf, Box<dyn Error>> {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(std::env::temp_dir().join(format!(
        "prdb-statistics-estimate-{}-{timestamp}",
        std::process::id()
    )))
}

fn run() -> Result<(), Box<dyn Error>> {
    let data_directory = temporary_data_directory()?;
    let result = run_in_directory(&data_directory);
    let cleanup = fs::remove_dir_all(&data_directory);
    result?;
    cleanup?;
    Ok(())
}

fn run_in_directory(data_directory: &std::path::Path) -> Result<(), Box<dyn Error>> {
    let mut initialized = bootstrap::initialize(data_directory, "estimate_admin", i64::MAX)?;
    let mut secret = initialized
        .take_secret()
        .ok_or("bootstrap secret missing")?;
    secret.zeroize();
    let instance = initialized.acknowledge()?;
    let database = instance.database();

    let columns = [ColumnSchema {
        scalar_type: ScalarType::Integer,
        nullable: false,
    }];
    let column_ids = [COLUMN_ID];
    let total_rows: usize = DATASET_ROWS.iter().sum::<usize>() + 2 * 512;
    let mut seed = InvocationTransaction::begin(
        database,
        invocation_id(1),
        PRINCIPAL_ID,
        i64::MAX,
        total_rows,
        total_rows * 64,
    )?;
    for (dataset, row_count) in DATASET_ROWS.iter().copied().enumerate() {
        let table = table_id(dataset);
        for number in 1..=row_count {
            let payload = encode_tuple(&columns, 1, &[Some(ScalarValue::Integer(number as i32))])?;
            seed.stage_put(row_id(table, number), &payload)?;
        }
    }
    for dataset in 4..=5 {
        let table = table_id(dataset);
        for number in 1..=512 {
            let payload = encode_tuple(
                &columns,
                1,
                &[Some(ScalarValue::Integer((number % 64) as i32))],
            )?;
            seed.stage_put(row_id(table, number), &payload)?;
        }
    }
    seed.commit()?;

    let mut results = Vec::with_capacity(DATASET_ROWS.len());
    for (dataset, row_count) in DATASET_ROWS.iter().copied().enumerate() {
        results.push(benchmark_dataset(
            database,
            dataset,
            row_count,
            &columns,
            &column_ids,
        )?);
    }
    println!(
        "profile: redb 4.4.0 / {} / {}",
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    println!(
        "estimate runs per dataset: {ESTIMATE_RUNS}; candidate budget {MAX_ESTIMATE_CANDIDATES}"
    );
    for result in results {
        println!(
            "dataset: {row_count} authorized INTEGER rows; median {:.3} ms; p95 {:.3} ms",
            as_millis(result.population_estimate.median),
            as_millis(result.population_estimate.p95),
            row_count = result.row_count
        );
        if let Some(predicate_result) = result.predicate_estimate {
            println!(
                "predicate estimate: {row_count} authorized INTEGER rows; one equality match; median {:.3} ms; p95 {:.3} ms",
                as_millis(predicate_result.median),
                as_millis(predicate_result.p95),
                row_count = result.row_count
            );
        }
    }
    let join_profile = benchmark_join_estimate(database, 512, &columns, &column_ids)?;
    println!(
        "join estimate: 512 authorized INTEGER rows per side; median {:.3} ms; p95 {:.3} ms",
        as_millis(join_profile.median),
        as_millis(join_profile.p95)
    );
    println!(
        "scope: estimate call only; database bootstrap, seeding, compilation, and planner IPC excluded"
    );
    drop(instance);
    Ok(())
}

fn benchmark_join_estimate(
    database: &redb::Database,
    row_count: usize,
    columns: &[ColumnSchema],
    column_ids: &[[u8; 16]],
) -> Result<TimingProfile, Box<dyn Error>> {
    let left_table = table_id(4);
    let right_table = table_id(5);
    let context = AuthorizationContext {
        principal_id: PRINCIPAL_ID,
        role_ids: HashSet::new(),
        scope: HashSet::from([
            (Operation::Usage, AccessTarget::Schema(SCHEMA_ID)),
            (Operation::Select, AccessTarget::Table(left_table)),
            (Operation::Select, AccessTarget::Table(right_table)),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: left_table,
                    column_id: COLUMN_ID,
                },
            ),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: right_table,
                    column_id: COLUMN_ID,
                },
            ),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Principal(PRINCIPAL_ID),
            operation: Operation::Usage,
            target: AccessTarget::Schema(SCHEMA_ID),
        },
        Grant {
            subject: Subject::Principal(PRINCIPAL_ID),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: left_table,
                column_id: COLUMN_ID,
            },
        },
        Grant {
            subject: Subject::Principal(PRINCIPAL_ID),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: right_table,
                column_id: COLUMN_ID,
            },
        },
    ];
    let table_policy = |table_id| Policy {
        subjects: vec![Subject::Principal(PRINCIPAL_ID)],
        operation: Operation::Select,
        target: AccessTarget::Table(table_id),
        effect: Effect::Allow,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        with_check: None,
    };
    let policies = [
        table_policy(left_table),
        Policy {
            target: AccessTarget::Column {
                table_id: left_table,
                column_id: COLUMN_ID,
            },
            ..table_policy(left_table)
        },
        table_policy(right_table),
        Policy {
            target: AccessTarget::Column {
                table_id: right_table,
                column_id: COLUMN_ID,
            },
            ..table_policy(right_table)
        },
    ];
    let projection = AuthorizedProjection {
        columns,
        column_ids,
        schema_version: 1,
        schema_id: SCHEMA_ID,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let left_keys = [COLUMN_ID];
    let right_keys = [COLUMN_ID];
    let expected_pairs = 4_096;
    let mut elapsed = Vec::with_capacity(ESTIMATE_RUNS);
    for run_number in 0..ESTIMATE_RUNS {
        let mut invocation = InvocationTransaction::begin(
            database,
            invocation_id(10_000 + row_count * ESTIMATE_RUNS + run_number),
            PRINCIPAL_ID,
            i64::MAX,
            1,
            64,
        )?;
        let started = Instant::now();
        let estimate = invocation.estimate_authorized_join_cardinality_at_most(
            left_table,
            &projection,
            &left_keys,
            right_table,
            &projection,
            &right_keys,
            row_count as u64,
        )?;
        elapsed.push(started.elapsed());
        if estimate != Some(expected_pairs) {
            return Err(format!(
                "join estimate at {row_count} rows per side returned {estimate:?}, expected {expected_pairs}"
            )
            .into());
        }
    }
    elapsed.sort_unstable();
    Ok(TimingProfile {
        median: elapsed[elapsed.len() / 2],
        p95: elapsed[((elapsed.len() - 1) * 95).div_ceil(100)],
    })
}

fn benchmark_dataset(
    database: &redb::Database,
    dataset: usize,
    row_count: usize,
    columns: &[ColumnSchema],
    column_ids: &[[u8; 16]],
) -> Result<DatasetProfile, Box<dyn Error>> {
    let table = table_id(dataset);
    let context = AuthorizationContext {
        principal_id: PRINCIPAL_ID,
        role_ids: HashSet::new(),
        scope: HashSet::from([
            (Operation::Usage, AccessTarget::Schema(SCHEMA_ID)),
            (Operation::Select, AccessTarget::Table(table)),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: table,
                    column_id: COLUMN_ID,
                },
            ),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Principal(PRINCIPAL_ID),
            operation: Operation::Usage,
            target: AccessTarget::Schema(SCHEMA_ID),
        },
        Grant {
            subject: Subject::Principal(PRINCIPAL_ID),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: table,
                column_id: COLUMN_ID,
            },
        },
    ];
    let table_allow = Policy {
        subjects: vec![Subject::Principal(PRINCIPAL_ID)],
        operation: Operation::Select,
        target: AccessTarget::Table(table),
        effect: Effect::Allow,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        with_check: None,
    };
    let cell_allow = Policy {
        target: AccessTarget::Column {
            table_id: table,
            column_id: COLUMN_ID,
        },
        ..table_allow.clone()
    };
    let policies = [table_allow, cell_allow];
    let projection = AuthorizedProjection {
        columns,
        column_ids,
        schema_version: 1,
        schema_id: SCHEMA_ID,
        context: &context,
        grants: &grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };

    let mut elapsed = Vec::with_capacity(ESTIMATE_RUNS);
    for run_number in 0..ESTIMATE_RUNS {
        let mut invocation = InvocationTransaction::begin(
            database,
            invocation_id(dataset * ESTIMATE_RUNS + run_number + 2),
            PRINCIPAL_ID,
            i64::MAX,
            1,
            64,
        )?;
        let started = Instant::now();
        let estimate_valid = if row_count == MAX_ESTIMATE_CANDIDATES {
            invocation.estimate_authorized_population_at_most(
                table,
                &projection,
                row_count as u64,
            )? == Some(row_count as u64)
        } else {
            matches!(
                invocation.estimate_authorized_scan(table, &projection)?,
                AuthorizedScanEstimate::Exact(summary)
                    if summary.visible_rows == row_count as u64
                        && summary.columns.len() == 1
                        && summary.columns[0].readable_values == row_count as u64
                        && summary.columns[0].nulls == 0
                        && summary.columns[0].redacted == 0
            )
        };
        elapsed.push(started.elapsed());
        if !estimate_valid {
            return Err("estimate did not match the seeded authorized population".into());
        }
    }
    elapsed.sort_unstable();
    let median = elapsed[elapsed.len() / 2];
    let p95 = elapsed[((elapsed.len() - 1) * 95).div_ceil(100)];
    let predicate_result = if row_count == MAX_PREDICATE_ESTIMATE_ROWS as usize {
        let predicate = BoundPredicate::Compare {
            operator: Comparison::Equal,
            left: Box::new(BoundPredicate::Column(BoundColumn {
                column_id: COLUMN_ID,
                ordinal: 0,
                scalar_type: ScalarType::Integer,
                nullable: false,
            })),
            right: Box::new(BoundPredicate::Integer(64)),
        };
        let mut predicate_elapsed = Vec::with_capacity(ESTIMATE_RUNS);
        for run_number in 0..ESTIMATE_RUNS {
            let mut invocation = InvocationTransaction::begin(
                database,
                invocation_id(dataset * ESTIMATE_RUNS * 2 + run_number + ESTIMATE_RUNS + 2),
                PRINCIPAL_ID,
                i64::MAX,
                1,
                64,
            )?;
            let started = Instant::now();
            let estimate = invocation.estimate_authorized_predicate_at_most(
                table,
                &projection,
                &predicate,
                MAX_PREDICATE_ESTIMATE_ROWS,
            )?;
            predicate_elapsed.push(started.elapsed());
            if estimate != Some((row_count as u64, 1)) {
                return Err("predicate estimate did not match the seeded authorized rows".into());
            }
        }
        predicate_elapsed.sort_unstable();
        Some((
            predicate_elapsed[predicate_elapsed.len() / 2],
            predicate_elapsed[((predicate_elapsed.len() - 1) * 95).div_ceil(100)],
        ))
    } else {
        None
    };
    Ok(DatasetProfile {
        row_count,
        population_estimate: TimingProfile { median, p95 },
        predicate_estimate: predicate_result.map(|(median, p95)| TimingProfile { median, p95 }),
    })
}

fn as_millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn main() -> Result<(), Box<dyn Error>> {
    run()
}
