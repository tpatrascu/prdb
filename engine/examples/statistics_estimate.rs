use prdb_engine::authorization::{
    AccessTarget, AuthorizationContext, AuthorizedProjection, Effect, Expr, Grant, Operation,
    Policy, Subject,
};
use prdb_engine::bootstrap;
use prdb_engine::overlay::RowId;
use prdb_engine::statistics::{AuthorizedScanEstimate, MAX_ESTIMATE_CANDIDATES};
use prdb_engine::transaction::InvocationTransaction;
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, encode_tuple};
use std::collections::HashSet;
use std::error::Error;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use zeroize::Zeroize;

const ROW_COUNT: usize = 8_192;
const ESTIMATE_RUNS: usize = 11;
const TABLE_ID: [u8; 16] = [0x71; 16];
const SCHEMA_ID: [u8; 16] = [0x72; 16];
const COLUMN_ID: [u8; 16] = [0x73; 16];
const PRINCIPAL_ID: [u8; 16] = [0x74; 16];

fn row_id(number: usize) -> RowId {
    RowId {
        table: TABLE_ID,
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
    let mut seed = InvocationTransaction::begin(
        database,
        invocation_id(1),
        PRINCIPAL_ID,
        i64::MAX,
        ROW_COUNT,
        ROW_COUNT * 64,
    )?;
    for number in 1..=ROW_COUNT {
        let payload = encode_tuple(&columns, 1, &[Some(ScalarValue::Integer(number as i32))])?;
        seed.stage_put(row_id(number), &payload)?;
    }
    seed.commit()?;

    let context = AuthorizationContext {
        principal_id: PRINCIPAL_ID,
        role_ids: HashSet::new(),
        scope: HashSet::from([
            (Operation::Usage, AccessTarget::Schema(SCHEMA_ID)),
            (Operation::Select, AccessTarget::Table(TABLE_ID)),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: TABLE_ID,
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
                table_id: TABLE_ID,
                column_id: COLUMN_ID,
            },
        },
    ];
    let table_allow = Policy {
        subjects: vec![Subject::Principal(PRINCIPAL_ID)],
        operation: Operation::Select,
        target: AccessTarget::Table(TABLE_ID),
        effect: Effect::Allow,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        with_check: None,
    };
    let cell_allow = Policy {
        target: AccessTarget::Column {
            table_id: TABLE_ID,
            column_id: COLUMN_ID,
        },
        ..table_allow.clone()
    };
    let policies = [table_allow, cell_allow];
    let projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
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
            invocation_id(run_number + 2),
            PRINCIPAL_ID,
            i64::MAX,
            1,
            64,
        )?;
        let started = Instant::now();
        let estimate = invocation.estimate_authorized_scan(TABLE_ID, &projection)?;
        elapsed.push(started.elapsed());
        match estimate {
            AuthorizedScanEstimate::Exact(summary)
                if summary.visible_rows == ROW_COUNT as u64
                    && summary.columns.len() == 1
                    && summary.columns[0].readable_values == ROW_COUNT as u64
                    && summary.columns[0].nulls == 0
                    && summary.columns[0].redacted == 0 => {}
            _ => return Err("estimate did not match the seeded authorized population".into()),
        }
    }
    elapsed.sort_unstable();
    let median = elapsed[elapsed.len() / 2];
    let p95 = elapsed[((elapsed.len() - 1) * 95).div_ceil(100)];
    println!(
        "profile: redb 4.4.0 / {} / {}",
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    println!(
        "dataset: {ROW_COUNT} authorized INTEGER rows; candidate budget {MAX_ESTIMATE_CANDIDATES}"
    );
    println!(
        "estimate runs: {ESTIMATE_RUNS}; median {:.3} ms; p95 {:.3} ms",
        as_millis(median),
        as_millis(p95)
    );
    println!(
        "scope: estimate call only; database bootstrap, seeding, compilation, and planner IPC excluded"
    );
    drop(instance);
    Ok(())
}

fn as_millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn main() -> Result<(), Box<dyn Error>> {
    run()
}
