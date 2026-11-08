use prdb_engine::authorization::{
    AccessTarget, AuthorizationContext, AuthorizedProjection, Effect, Expr, Grant, Operation,
    Policy, SanitizedCell, Subject,
};
use prdb_engine::bootstrap;
use prdb_engine::overlay::RowId;
use prdb_engine::storage_manager::{StorageManager, StorageManagerConfig};
use prdb_engine::transaction::InvocationTransaction;
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, encode_tuple};
use std::collections::HashSet;
use std::error::Error;
use std::fs;
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Barrier;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use zeroize::Zeroize;

const WRITERS: usize = 4;
const WRITES_PER_WRITER: usize = 64;
const ANALYTIC_ROW_BYTES: usize = 32 * 1024;
const TABLE_ID: [u8; 16] = [0x71; 16];
const SCHEMA_ID: [u8; 16] = [0x72; 16];
const COLUMN_ID: [u8; 16] = [0x73; 16];
const PRINCIPAL_ID: [u8; 16] = [0x74; 16];

fn row_id(number: usize) -> RowId {
    let mut row = [0; 16];
    row[8..].copy_from_slice(&(number as u64).to_be_bytes());
    RowId {
        table: TABLE_ID,
        row,
    }
}

fn invocation_id(number: usize) -> [u8; 16] {
    let mut id = [0xa5; 16];
    id[8..].copy_from_slice(&(number as u64).to_be_bytes());
    id
}

fn payload(columns: &[ColumnSchema], value: String) -> Result<Vec<u8>, Box<dyn Error>> {
    Ok(encode_tuple(columns, 1, &[Some(ScalarValue::Text(value))])?)
}

fn temporary_data_directory() -> Result<PathBuf, Box<dyn Error>> {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(std::env::temp_dir().join(format!(
        "prdb-storage-pressure-{}-{timestamp}",
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
    let mut bootstrap = bootstrap::initialize(data_directory, "pressure_admin", i64::MAX)?;
    let mut secret = bootstrap.take_secret().ok_or("bootstrap secret missing")?;
    secret.zeroize();
    let instance = bootstrap.acknowledge()?;
    let database = instance.database();
    let storage = StorageManager::with_config(database, StorageManagerConfig::default())?;
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Text,
        nullable: false,
    }];
    let column_ids = [COLUMN_ID];

    let initial_value = "s".repeat(ANALYTIC_ROW_BYTES);
    let mut seed = InvocationTransaction::begin(
        database,
        [0x51; 16],
        PRINCIPAL_ID,
        i64::MAX,
        WRITERS + 1,
        (WRITERS + 1) * ANALYTIC_ROW_BYTES * 2,
    )?;
    for writer in 0..WRITERS {
        seed.stage_put(
            row_id(writer + 1),
            &payload(&columns, initial_value.clone())?,
        )?;
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
            target: AccessTarget::Table(TABLE_ID),
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
    let table_policy = Policy {
        subjects: vec![Subject::Principal(PRINCIPAL_ID)],
        operation: Operation::Select,
        target: AccessTarget::Table(TABLE_ID),
        effect: Effect::Allow,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        with_check: None,
    };
    let column_policy = Policy {
        target: AccessTarget::Column {
            table_id: TABLE_ID,
            column_id: COLUMN_ID,
        },
        ..table_policy.clone()
    };
    let policies = [table_policy, column_policy];
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
    let mut reader = storage.begin_invocation(
        [0x61; 16],
        PRINCIPAL_ID,
        i64::MAX,
        WRITERS + 1,
        (WRITERS + 1) * ANALYTIC_ROW_BYTES * 2,
    )?;
    let first_page = reader.scan_authorized(TABLE_ID, None, 128, &projection)?;
    if !scan_contains_original_rows(&first_page.rows, &initial_value)
        || first_page.continuation.is_some()
    {
        return Err("initial authorized scan did not return the seeded rows".into());
    }

    let barrier = Barrier::new(WRITERS + 1);
    let completed = AtomicUsize::new(0);
    let writers_started = Instant::now();
    let mut analytical_rounds = 0usize;
    let mut point_reads = 0usize;

    thread::scope(|scope| -> Result<(), Box<dyn Error>> {
        for writer in 0..WRITERS {
            let barrier = &barrier;
            let completed = &completed;
            let columns = &columns;
            let storage = &storage;
            scope.spawn(move || {
                barrier.wait();
                for sequence in 0..WRITES_PER_WRITER {
                    let mut value = "x".repeat(ANALYTIC_ROW_BYTES);
                    let prefix = format!("w{writer}-n{sequence}");
                    value.replace_range(..prefix.len(), &prefix);
                    let mut transaction = storage
                        .begin_invocation(
                            invocation_id(writer * WRITES_PER_WRITER + sequence + 1),
                            PRINCIPAL_ID,
                            i64::MAX,
                            1,
                            ANALYTIC_ROW_BYTES * 2,
                        )
                        .expect("begin writer transaction");
                    transaction
                        .stage_put(
                            row_id(writer + 1),
                            &payload(columns, value).expect("encode row"),
                        )
                        .expect("stage writer row");
                    transaction.commit().expect("commit writer transaction");
                }
                completed.fetch_add(1, Ordering::Release);
            });
        }
        barrier.wait();
        while completed.load(Ordering::Acquire) != WRITERS {
            let page = reader.scan_authorized(TABLE_ID, None, 128, &projection)?;
            if !scan_contains_original_rows(&page.rows, &initial_value)
                || page.continuation.is_some()
            {
                return Err("retained snapshot changed during concurrent commits".into());
            }
            analytical_rounds += 1;
            for writer in 0..WRITERS {
                let row = reader
                    .read_authorized(row_id(writer + 1), &projection)?
                    .ok_or("retained point read lost a row")?;
                if !matches!(
                    row.as_slice(),
                    [SanitizedCell::Value(ScalarValue::Text(value))]
                        if value == &initial_value
                ) {
                    return Err("retained point read changed during concurrent commits".into());
                }
                black_box(row);
                point_reads += 1;
            }
        }
        Ok(())
    })?;

    let elapsed = writers_started.elapsed();
    reader.abort()?;
    println!("profile: redb 4.4.0 / Rust 1.95.0 / x86_64 Linux");
    println!(
        "writers: {WRITERS} threads x {WRITES_PER_WRITER} durable commits ({} KiB per row)",
        ANALYTIC_ROW_BYTES / 1024
    );
    println!(
        "writer elapsed: {:.3}s; commit throughput: {:.1}/s",
        elapsed.as_secs_f64(),
        (WRITERS * WRITES_PER_WRITER) as f64 / elapsed.as_secs_f64()
    );
    println!(
        "concurrent analytics: {analytical_rounds} full authorized scans; {point_reads} point reads; {:.1} scans/s",
        analytical_rounds as f64 / elapsed.as_secs_f64()
    );
    println!("reader snapshot: retained original values through all writer commits");
    drop(storage);
    drop(instance);
    Ok(())
}

fn scan_contains_original_rows(rows: &[(RowId, Vec<SanitizedCell>)], initial_value: &str) -> bool {
    rows.len() == WRITERS
        && rows.iter().all(|(_, cells)| {
            matches!(
                cells.as_slice(),
                [SanitizedCell::Value(ScalarValue::Text(value))]
                    if value == initial_value
            )
        })
}

fn main() -> Result<(), Box<dyn Error>> {
    run()
}
