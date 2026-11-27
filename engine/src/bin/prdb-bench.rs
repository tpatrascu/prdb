use std::collections::BTreeMap;
use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, PartialEq, Eq)]
enum WorkloadMode {
    Mixed,
    Tpcc,
    Tpch,
}

struct Config {
    address: String,
    admin_credentials: PathBuf,
    prdb_client: PathBuf,
    sdk_library: PathBuf,
    include_dir: PathBuf,
    runtime_profile: String,
    cpu_baseline: String,
    sdk_release: u64,
    size_gb: f64,
    bench_root: PathBuf,
    run_dir: Option<PathBuf>,
    oltp_threads: usize,
    olap_threads: usize,
    duration_secs: u64,
    warmup_secs: u64,
    workload: WorkloadMode,
    insert_batch_size: usize,
    keep_data: bool,
}

impl Default for Config {
    fn default() -> Self {
        let home = env::var("HOME").unwrap_or_else(|_| "/tmp".to_owned());
        Self {
            address: "127.0.0.1:15432".to_owned(),
            admin_credentials: PathBuf::new(),
            prdb_client: PathBuf::from("target/release/prdb-client"),
            sdk_library: PathBuf::from("target/release-profile/sdk/libprdb_sql.so.1"),
            include_dir: PathBuf::from("engine/include"),
            runtime_profile: "nixos-x86_64-glibc-2.42".to_owned(),
            cpu_baseline: "x86_64-v1".to_owned(),
            sdk_release: 1,
            size_gb: 5.0,
            bench_root: PathBuf::from(home).join("tmp/prdb-bench"),
            run_dir: None,
            oltp_threads: 4,
            olap_threads: 2,
            duration_secs: 60,
            warmup_secs: 5,
            workload: WorkloadMode::Mixed,
            insert_batch_size: 1000,
            keep_data: false,
        }
    }
}

fn print_help() {
    println!(
        "prdb-bench [options]\n\
         --address IP:PORT             Gateway address (default 127.0.0.1:15432)\n\
         --admin-credentials PATH      Admin credentials file (required)\n\
         --prdb-client PATH            Path to prdb-client binary\n\
         --sdk-library PATH            Path to libprdb_sql.so.1\n\
         --include-dir PATH            Path to SDK include directory\n\
         --runtime-profile NAME        Runtime profile name (default nixos-x86_64-glibc-2.42)\n\
         --cpu-baseline NAME           CPU baseline name (default x86_64-v1)\n\
         --sdk-release N               SDK release number (default 1)\n\
         --size-gb N                   Target database size in GB (default 5.0)\n\
         --bench-root PATH             External benchmark root (default $HOME/tmp/prdb-bench)\n\
         --run-dir PATH                Explicit run directory\n\
         --oltp-threads N              Concurrent TPC-C threads (default 4)\n\
         --olap-threads N              Concurrent TPC-H threads (default 2)\n\
         --duration-secs N             Measurement duration in seconds (default 60)\n\
         --warmup-secs N               Warmup duration in seconds (default 5)\n\
         --workload mixed|tpcc|tpch    Workload mode (default mixed)\n\
         --insert-batch-size N         Number of rows per insert batch (default 1000)\n\
         --keep-data                   Preserve run directory after completion"
    );
}

fn parse_args() -> Result<Config, String> {
    let mut cfg = Config::default();
    let mut args = env::args().skip(1);
    while let Some(flag) = args.next() {
        if flag == "--help" || flag == "-h" {
            print_help();
            std::process::exit(0);
        }
        if flag == "--keep-data" {
            cfg.keep_data = true;
            continue;
        }
        let val = args
            .next()
            .ok_or_else(|| format!("missing value for {flag}"))?;
        match flag.as_str() {
            "--address" => cfg.address = val,
            "--admin-credentials" => cfg.admin_credentials = PathBuf::from(val),
            "--prdb-client" => cfg.prdb_client = PathBuf::from(val),
            "--sdk-library" => cfg.sdk_library = PathBuf::from(val),
            "--include-dir" => cfg.include_dir = PathBuf::from(val),
            "--runtime-profile" => cfg.runtime_profile = val,
            "--cpu-baseline" => cfg.cpu_baseline = val,
            "--sdk-release" => {
                cfg.sdk_release = val.parse().map_err(|_| "invalid --sdk-release")?;
            }
            "--size-gb" => {
                cfg.size_gb = val.parse().map_err(|_| "invalid --size-gb")?;
                if cfg.size_gb <= 0.0 {
                    return Err("--size-gb must be positive".to_owned());
                }
            }
            "--bench-root" => cfg.bench_root = PathBuf::from(val),
            "--run-dir" => cfg.run_dir = Some(PathBuf::from(val)),
            "--oltp-threads" => {
                cfg.oltp_threads = val.parse().map_err(|_| "invalid --oltp-threads")?;
            }
            "--olap-threads" => {
                cfg.olap_threads = val.parse().map_err(|_| "invalid --olap-threads")?;
            }
            "--duration-secs" => {
                cfg.duration_secs = val.parse().map_err(|_| "invalid --duration-secs")?;
            }
            "--warmup-secs" => {
                cfg.warmup_secs = val.parse().map_err(|_| "invalid --warmup-secs")?;
            }
            "--workload" => match val.as_str() {
                "mixed" => cfg.workload = WorkloadMode::Mixed,
                "tpcc" => cfg.workload = WorkloadMode::Tpcc,
                "tpch" => cfg.workload = WorkloadMode::Tpch,
                _ => return Err(format!("invalid --workload: {val}")),
            },
            "--insert-batch-size" => {
                cfg.insert_batch_size = val.parse().map_err(|_| "invalid --insert-batch-size")?;
                if cfg.insert_batch_size == 0 {
                    return Err("--insert-batch-size must be positive".to_owned());
                }
            }
            other => return Err(format!("unknown option: {other}")),
        }
    }
    if cfg.admin_credentials.as_os_str().is_empty() {
        return Err("--admin-credentials is required".to_owned());
    }
    Ok(cfg)
}

type QueryLogger = Arc<Mutex<BufWriter<File>>>;

fn log_query(
    logger: &QueryLogger,
    phase: &str,
    user: &str,
    workload: &str,
    elapsed_us: u64,
    ok: bool,
    sql: &str,
) {
    let ts_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let status = if ok { "OK" } else { "ERR" };
    if let Ok(mut guard) = logger.lock() {
        let _ = writeln!(
            guard,
            "[{ts_ms}] phase={phase} user={user} workload={workload} status={status} elapsed_ms={:.3}\n{sql}",
            elapsed_us as f64 / 1000.0
        );
        let _ = guard.flush();
    }
}

fn run_admin_sql(cfg: &Config, logger: &QueryLogger, sql: &str) -> Result<String, String> {
    let t0 = Instant::now();
    let mut child = Command::new(&cfg.prdb_client)
        .args(["admin-sql", "--address", &cfg.address, "--credentials"])
        .arg(&cfg.admin_credentials)
        .args(["--sql-file", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("failed to spawn prdb-client admin-sql: {e}"))?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(sql.as_bytes())
            .map_err(|e| format!("failed to write admin-sql stdin: {e}"))?;
    }
    let out = child
        .wait_with_output()
        .map_err(|e| format!("failed waiting for admin-sql: {e}"))?;
    let elapsed_us = t0.elapsed().as_micros() as u64;
    log_query(
        logger,
        "admin_sql",
        "admin",
        "admin",
        elapsed_us,
        out.status.success(),
        sql.trim_end(),
    );
    if !out.status.success() {
        return Err(format!(
            "admin-sql failed ({}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

fn run_interactive_sql(
    cfg: &Config,
    credentials: &Path,
    user: &str,
    scope: &Path,
    workload_class: &str,
    sql_file: &Path,
) -> Result<(), String> {
    let deadline_ms = if workload_class == "analytical" {
        "600000"
    } else {
        "60000"
    };
    let out = Command::new(&cfg.prdb_client)
        .args([
            "interactive-sql",
            "--address",
            &cfg.address,
            "--credentials",
        ])
        .arg(credentials)
        .args(["--default-user", user, "--sql-file"])
        .arg(sql_file)
        .args(["--schema", "bench", "--scope"])
        .arg(scope)
        .arg("--sdk-library")
        .arg(&cfg.sdk_library)
        .arg("--include-dir")
        .arg(&cfg.include_dir)
        .args([
            "--sdk-release",
            &cfg.sdk_release.to_string(),
            "--cpu-baseline",
            &cfg.cpu_baseline,
            "--runtime-profile",
            &cfg.runtime_profile,
            "--compiler-identity",
            "tcc",
            "--deadline-ms",
            deadline_ms,
            "--workload",
            workload_class,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .map_err(|e| format!("failed to spawn interactive-sql: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "interactive-sql failed ({}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(())
}

fn create_bench_users_and_credentials(
    cfg: &Config,
    oltp_creds_path: &Path,
    olap_creds_path: &Path,
) -> Result<(), String> {
    use prdb_engine::client_credentials::{read_client_credentials, write_client_credentials};
    use prdb_engine::client_protocol::ClientCredentials;
    use prdb_engine::client_submit::AdminClient;
    use std::net::SocketAddr;

    let addr: SocketAddr = cfg
        .address
        .parse()
        .map_err(|e| format!("invalid address {}: {e}", cfg.address))?;
    let mut admin_creds =
        read_client_credentials(&cfg.admin_credentials).map_err(|e| e.to_string())?;
    let mut admin = AdminClient::connect_authenticated(
        addr,
        admin_creds.token_id,
        &mut admin_creds.token_secret,
    )
    .map_err(|e| e.to_string())?;

    let now_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_micros() as i64;
    let expires_at_us = now_us + 86_400_000_000;

    let oltp_user = admin
        .create_user_with_profile("bench_oltp_user", "bench_oltp")
        .map_err(|e| format!("create bench_oltp_user failed: {e}"))?;
    let mut oltp_tok = admin
        .issue_token(oltp_user.identity_id, "bench_oltp", expires_at_us)
        .map_err(|e| format!("issue bench_oltp_user token failed: {e}"))?;
    write_client_credentials(
        oltp_creds_path,
        &ClientCredentials {
            token_id: oltp_tok.token_id,
            token_secret: *oltp_tok.token_secret(),
        },
    )
    .map_err(|e| e.to_string())?;
    let _ = &mut oltp_tok;

    let olap_user = admin
        .create_user_with_profile("bench_olap_user", "bench_olap")
        .map_err(|e| format!("create bench_olap_user failed: {e}"))?;
    let mut olap_tok = admin
        .issue_token(olap_user.identity_id, "bench_olap", expires_at_us)
        .map_err(|e| format!("issue bench_olap_user token failed: {e}"))?;
    write_client_credentials(
        olap_creds_path,
        &ClientCredentials {
            token_id: olap_tok.token_id,
            token_secret: *olap_tok.token_secret(),
        },
    )
    .map_err(|e| e.to_string())?;
    let _ = &mut olap_tok;
    Ok(())
}

fn compute_profile_limits(size_gb: f64) -> (u64, u64, u64, u64, u64, u64, u64) {
    // Baseline is the verified 0.1 GB run.
    let scale = (size_gb / 0.1).max(1.0);
    let sqrt_scale = scale.sqrt();

    let oltp_mem = ((512.0 * 1024.0 * 1024.0) * sqrt_scale)
        .min(1024.0 * 1024.0 * 1024.0)
        .round() as u64;
    let oltp_deadline = (60_000.0 * sqrt_scale.min(4.0)).round() as u64;

    let olap_mem = ((2.0 * 1024.0 * 1024.0 * 1024.0) * sqrt_scale)
        .min(8.0 * 1024.0 * 1024.0 * 1024.0)
        .round() as u64;
    let olap_scan_work = (50_000_000.0 * scale).round() as u64;
    let olap_result_bytes = ((256.0 * 1024.0 * 1024.0) * sqrt_scale)
        .min(2.0 * 1024.0 * 1024.0 * 1024.0)
        .round() as u64;
    let olap_spill_bytes = ((32.0 * 1024.0 * 1024.0 * 1024.0) * sqrt_scale)
        .min(32.0 * 1024.0 * 1024.0 * 1024.0)
        .round() as u64;
    let olap_deadline = (600_000.0 * sqrt_scale).min(3_600_000.0).round() as u64;

    (
        oltp_mem,
        oltp_deadline,
        olap_mem,
        olap_scan_work,
        olap_result_bytes,
        olap_spill_bytes,
        olap_deadline,
    )
}

fn provision_profiles_and_schema(
    cfg: &Config,
    logger: &QueryLogger,
    oltp_creds_path: &Path,
    olap_creds_path: &Path,
) -> Result<(), String> {
    let (
        oltp_mem,
        oltp_deadline,
        olap_mem,
        olap_scan_work,
        olap_result_bytes,
        olap_spill_bytes,
        olap_deadline,
    ) = compute_profile_limits(cfg.size_gb);
    println!(
        "prdb-bench: configuring profiles for {:.2} GB (oltp_mem={} MiB, olap_mem={} MiB, olap_scan_work={}, olap_result_bytes={} MiB, olap_spill={} GiB)",
        cfg.size_gb,
        oltp_mem / (1024 * 1024),
        olap_mem / (1024 * 1024),
        olap_scan_work,
        olap_result_bytes / (1024 * 1024),
        olap_spill_bytes / (1024 * 1024 * 1024),
    );
    let oltp_profile_sql = format!(
        "CREATE WORKLOAD PROFILE IF NOT EXISTS bench_oltp FROM large_operational WITH (memory_max_bytes = {oltp_mem}, max_invocation_scan_work = {olap_scan_work}, max_deadline_ms = {oltp_deadline});"
    );
    let olap_profile_sql = format!(
        "CREATE WORKLOAD PROFILE IF NOT EXISTS bench_olap FROM large_analytical WITH (memory_max_bytes = {olap_mem}, max_invocation_scan_work = {olap_scan_work}, max_total_result_bytes = {olap_result_bytes}, spill_max_bytes = {olap_spill_bytes}, max_deadline_ms = {olap_deadline});"
    );
    let pre_stmts = [
        "CREATE SCHEMA IF NOT EXISTS bench;",
        oltp_profile_sql.as_str(),
        olap_profile_sql.as_str(),
    ];
    for stmt in pre_stmts {
        run_admin_sql(cfg, logger, stmt)?;
    }
    create_bench_users_and_credentials(cfg, oltp_creds_path, olap_creds_path)?;
    let stmts = [
        "GRANT USAGE ON SCHEMA bench TO USER admin;",
        "GRANT USAGE ON SCHEMA bench TO USER bench_oltp_user;",
        "GRANT USAGE ON SCHEMA bench TO USER bench_olap_user;",
        "CREATE TABLE bench.warehouse (w_id BIGINT NOT NULL, w_name TEXT NOT NULL, w_ytd DECIMAL(12,2) NOT NULL, w_tax DECIMAL(4,4) NOT NULL);",
        "CREATE TABLE bench.district (d_id BIGINT NOT NULL, d_w_id BIGINT NOT NULL, d_name TEXT NOT NULL, d_ytd DECIMAL(12,2) NOT NULL, d_next_o_id BIGINT NOT NULL);",
        "CREATE TABLE bench.customer (c_id BIGINT NOT NULL, c_d_id BIGINT NOT NULL, c_w_id BIGINT NOT NULL, c_last TEXT NOT NULL, c_balance DECIMAL(12,2) NOT NULL, c_ytd_payment DECIMAL(12,2) NOT NULL, c_payment_cnt BIGINT NOT NULL);",
        "CREATE TABLE bench.orders (o_id BIGINT NOT NULL, o_d_id BIGINT NOT NULL, o_w_id BIGINT NOT NULL, o_c_id BIGINT NOT NULL, o_ol_cnt BIGINT NOT NULL);",
        "CREATE TABLE bench.order_line (ol_o_id BIGINT NOT NULL, ol_d_id BIGINT NOT NULL, ol_w_id BIGINT NOT NULL, ol_number BIGINT NOT NULL, ol_i_id BIGINT NOT NULL, ol_amount DECIMAL(12,2) NOT NULL, ol_quantity BIGINT NOT NULL);",
        "CREATE TABLE bench.stock (s_i_id BIGINT NOT NULL, s_w_id BIGINT NOT NULL, s_quantity BIGINT NOT NULL);",
        "CREATE TABLE bench.item (i_id BIGINT NOT NULL, i_name TEXT NOT NULL, i_price DECIMAL(10,2) NOT NULL);",
        "CREATE TABLE bench.region (r_regionkey BIGINT NOT NULL, r_name TEXT NOT NULL);",
        "CREATE TABLE bench.nation (n_nationkey BIGINT NOT NULL, n_name TEXT NOT NULL, n_regionkey BIGINT NOT NULL);",
        "CREATE TABLE bench.supplier (s_suppkey BIGINT NOT NULL, s_name TEXT NOT NULL, s_nationkey BIGINT NOT NULL, s_acctbal DECIMAL(12,2) NOT NULL);",
        "CREATE TABLE bench.part (p_partkey BIGINT NOT NULL, p_name TEXT NOT NULL, p_size BIGINT NOT NULL, p_retailprice DECIMAL(12,2) NOT NULL);",
        "CREATE TABLE bench.h_orders (o_orderkey BIGINT NOT NULL, o_custkey BIGINT NOT NULL, o_orderstatus TEXT NOT NULL, o_totalprice DECIMAL(12,2) NOT NULL, o_orderdate BIGINT NOT NULL);",
        "CREATE TABLE bench.lineitem (l_orderkey BIGINT NOT NULL, l_partkey BIGINT NOT NULL, l_suppkey BIGINT NOT NULL, l_linenumber BIGINT NOT NULL, l_quantity DECIMAL(12,2) NOT NULL, l_extendedprice DECIMAL(12,2) NOT NULL, l_discount DECIMAL(4,2) NOT NULL, l_tax DECIMAL(4,2) NOT NULL, l_returnflag TEXT NOT NULL, l_linestatus TEXT NOT NULL, l_shipdate BIGINT NOT NULL);",
        "CREATE UNIQUE INDEX idx_warehouse_pk ON bench.warehouse (w_id);",
        "CREATE UNIQUE INDEX idx_district_pk ON bench.district (d_w_id, d_id);",
        "CREATE UNIQUE INDEX idx_customer_pk ON bench.customer (c_w_id, c_d_id, c_id);",
        "CREATE UNIQUE INDEX idx_orders_pk ON bench.orders (o_w_id, o_d_id, o_id);",
        "CREATE UNIQUE INDEX idx_order_line_pk ON bench.order_line (ol_w_id, ol_d_id, ol_o_id, ol_number);",
        "CREATE UNIQUE INDEX idx_stock_pk ON bench.stock (s_w_id, s_i_id);",
        "CREATE UNIQUE INDEX idx_item_pk ON bench.item (i_id);",
        "CREATE UNIQUE INDEX idx_region_pk ON bench.region (r_regionkey);",
        "CREATE UNIQUE INDEX idx_nation_pk ON bench.nation (n_nationkey);",
        "CREATE UNIQUE INDEX idx_supplier_pk ON bench.supplier (s_suppkey);",
        "CREATE UNIQUE INDEX idx_part_pk ON bench.part (p_partkey);",
        "CREATE UNIQUE INDEX idx_h_orders_pk ON bench.h_orders (o_orderkey);",
        "CREATE UNIQUE INDEX idx_lineitem_pk ON bench.lineitem (l_orderkey, l_linenumber);",
    ];
    for stmt in stmts {
        run_admin_sql(cfg, logger, stmt)?;
    }

    let tables: [(&str, &str); 13] = [
        ("warehouse", "w_id, w_name, w_ytd, w_tax"),
        ("district", "d_id, d_w_id, d_name, d_ytd, d_next_o_id"),
        (
            "customer",
            "c_id, c_d_id, c_w_id, c_last, c_balance, c_ytd_payment, c_payment_cnt",
        ),
        ("orders", "o_id, o_d_id, o_w_id, o_c_id, o_ol_cnt"),
        (
            "order_line",
            "ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_amount, ol_quantity",
        ),
        ("stock", "s_i_id, s_w_id, s_quantity"),
        ("item", "i_id, i_name, i_price"),
        ("region", "r_regionkey, r_name"),
        ("nation", "n_nationkey, n_name, n_regionkey"),
        ("supplier", "s_suppkey, s_name, s_nationkey, s_acctbal"),
        ("part", "p_partkey, p_name, p_size, p_retailprice"),
        (
            "h_orders",
            "o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate",
        ),
        (
            "lineitem",
            "l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate",
        ),
    ];
    for (t, cols) in tables {
        run_admin_sql(
            cfg,
            logger,
            &format!("GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE bench.{t} TO USER admin;"),
        )?;
        run_admin_sql(
            cfg,
            logger,
            &format!(
                "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE bench.{t} TO USER bench_oltp_user;"
            ),
        )?;
        run_admin_sql(
            cfg,
            logger,
            &format!("GRANT SELECT ON TABLE bench.{t} TO USER bench_olap_user;"),
        )?;
        run_admin_sql(
            cfg,
            logger,
            &format!(
                "CREATE ROW POLICY allow_sel_{t} ON bench.{t} FOR SELECT TO USER admin, USER bench_oltp_user, USER bench_olap_user AS ALLOW USING (TRUE);"
            ),
        )?;
        run_admin_sql(
            cfg,
            logger,
            &format!(
                "CREATE ROW POLICY allow_ins_{t} ON bench.{t} FOR INSERT TO USER admin, USER bench_oltp_user AS ALLOW WITH CHECK (TRUE);"
            ),
        )?;
        run_admin_sql(
            cfg,
            logger,
            &format!(
                "CREATE ROW POLICY allow_upd_{t} ON bench.{t} FOR UPDATE TO USER admin, USER bench_oltp_user AS ALLOW USING (TRUE) WITH CHECK (TRUE);"
            ),
        )?;
        run_admin_sql(
            cfg,
            logger,
            &format!(
                "CREATE ROW POLICY allow_del_{t} ON bench.{t} FOR DELETE TO USER admin, USER bench_oltp_user AS ALLOW USING (TRUE);"
            ),
        )?;
        run_admin_sql(
            cfg,
            logger,
            &format!(
                "CREATE CELL POLICY allow_cell_sel_{t} ON bench.{t} ({cols}) FOR SELECT TO USER admin, USER bench_oltp_user, USER bench_olap_user AS ALLOW USING (TRUE);"
            ),
        )?;
        run_admin_sql(
            cfg,
            logger,
            &format!(
                "CREATE CELL POLICY allow_cell_ins_{t} ON bench.{t} ({cols}) FOR INSERT TO USER admin, USER bench_oltp_user AS ALLOW WITH CHECK (TRUE);"
            ),
        )?;
        run_admin_sql(
            cfg,
            logger,
            &format!(
                "CREATE CELL POLICY allow_cell_upd_{t} ON bench.{t} ({cols}) FOR UPDATE TO USER admin, USER bench_oltp_user AS ALLOW USING (TRUE) WITH CHECK (TRUE);"
            ),
        )?;
    }
    Ok(())
}

fn generate_scope(
    cfg: &Config,
    credentials_path: &Path,
    output: &Path,
    table_names: &[&str],
    rel_ops: &[prdb_engine::authorization::Operation],
    col_ops: &[prdb_engine::authorization::Operation],
) -> Result<(), String> {
    use prdb_engine::client_credentials::read_client_credentials;
    use prdb_engine::client_submit::MetadataClient;
    use prdb_engine::submission_scope::RequestedScopeBuilder;
    use std::net::SocketAddr;

    let addr: SocketAddr = cfg
        .address
        .parse()
        .map_err(|e| format!("invalid address {}: {e}", cfg.address))?;
    let mut credentials = read_client_credentials(credentials_path).map_err(|e| e.to_string())?;
    let mut client = MetadataClient::connect_authenticated(
        addr,
        credentials.token_id,
        &mut credentials.token_secret,
    )
    .map_err(|e| e.to_string())?;
    let schemas = client.list_schemas().map_err(|e| e.to_string())?;
    let schema = schemas
        .iter()
        .find(|s| s.name == "bench")
        .ok_or_else(|| "schema \"bench\" is not visible".to_owned())?;
    let relations = client
        .list_relations(schema.schema_id)
        .map_err(|e| e.to_string())?;

    let mut builder = RequestedScopeBuilder::new();
    builder
        .include_schema(schema)
        .map_err(|e| format!("include_schema failed: {e:?}"))?;
    for rel in &relations {
        if !table_names.contains(&rel.name.as_str()) {
            continue;
        }
        builder
            .include_relation(schema, rel, rel_ops)
            .map_err(|e| format!("include_relation {} failed: {e:?}", rel.name))?;
        let columns = client
            .list_columns(schema.schema_id, rel.relation_id)
            .map_err(|e| e.to_string())?;
        for col in &columns {
            builder
                .include_column(schema, rel, col, col_ops)
                .map_err(|e| format!("include_column {}.{} failed: {e:?}", rel.name, col.name))?;
        }
    }
    let bytes = builder
        .encode()
        .map_err(|e| format!("scope encode failed: {e:?}"))?;
    fs::write(output, bytes).map_err(|e| format!("failed to write scope: {e}"))?;
    Ok(())
}

fn generate_and_load_data(
    cfg: &Config,
    logger: &QueryLogger,
    admin_scope: &Path,
    query_dir: &Path,
) -> Result<(), String> {
    let batch_path = query_dir.join("load_batch.sql");
    let load_batch = |sql: &str| -> Result<(), String> {
        fs::write(&batch_path, sql).map_err(|e| e.to_string())?;
        let t0 = Instant::now();
        let res = run_interactive_sql(
            cfg,
            &cfg.admin_credentials,
            "admin",
            admin_scope,
            "operational",
            &batch_path,
        );
        let elapsed_us = t0.elapsed().as_micros() as u64;
        log_query(
            logger,
            "data_load",
            "admin",
            "operational",
            elapsed_us,
            res.is_ok(),
            sql.trim_end(),
        );
        res
    };
    // Scale factor: 5.0 GB -> ~50 warehouses / SF 5.0 equivalent; minimum 1 warehouse.
    let warehouses = ((cfg.size_gb / 0.1).round() as usize).max(1);
    let items = (warehouses * 200).min(100_000);
    let lineitems_per_order = 4usize;
    let h_orders = (warehouses * 500).max(100);

    let mut sql = String::with_capacity(64 * 1024);
    for r in 0..5 {
        sql.push_str(&format!("INSERT INTO region VALUES ({r}, 'REGION_{r}');\n"));
    }
    for n in 0..25 {
        sql.push_str(&format!(
            "INSERT INTO nation VALUES ({n}, 'NATION_{n}', {});\n",
            n % 5
        ));
    }
    for w in 1..=warehouses {
        sql.push_str(&format!(
            "INSERT INTO warehouse VALUES ({w}, 'WH_{w}', 300000.00, 0.0500);\n"
        ));
        for d in 1..=10 {
            sql.push_str(&format!(
                "INSERT INTO district VALUES ({d}, {w}, 'DIST_{w}_{d}', 30000.00, 3001);\n"
            ));
        }
    }
    load_batch(&sql)?;
    sql.clear();

    // Load items and h_orders/lineitem in multi-row INSERTs respecting --insert-batch-size
    // (clamped to MAX_EXECUTOR_ROW_HANDLES = 4096 per transaction and <= 60 statements per batch).
    let effective_batch_rows = cfg.insert_batch_size.min(4096);
    if cfg.insert_batch_size > 4096 {
        println!(
            "prdb-bench: clamping --insert-batch-size {} to 4096 rows per transaction (MAX_EXECUTOR_ROW_HANDLES)",
            cfg.insert_batch_size
        );
    }
    let rows_per_stmt = effective_batch_rows.div_ceil(60).clamp(1, 64);

    let mut pending_rows = 0usize;
    let mut stmt_count = 0usize;
    let mut i = 1usize;
    while i <= items {
        let max_this_stmt = rows_per_stmt
            .min(effective_batch_rows - pending_rows)
            .max(1);
        let end = (i + max_this_stmt - 1).min(items);
        let mut stmt = String::from("INSERT INTO item VALUES ");
        for k in i..=end {
            if k > i {
                stmt.push_str(", ");
            }
            stmt.push_str(&format!(
                "({k}, 'ITEM_{k}', {:.2})",
                10.0 + (k % 100) as f64
            ));
        }
        stmt.push_str(";\n");
        if !sql.is_empty() && sql.len() + stmt.len() > 60_000 {
            load_batch(&sql)?;
            sql.clear();
            pending_rows = 0;
            stmt_count = 0;
        }
        sql.push_str(&stmt);
        pending_rows += end - i + 1;
        stmt_count += 1;
        i = end + 1;
        if pending_rows >= effective_batch_rows || stmt_count >= 60 {
            load_batch(&sql)?;
            sql.clear();
            pending_rows = 0;
            stmt_count = 0;
        }
    }
    if !sql.is_empty() {
        load_batch(&sql)?;
        sql.clear();
    }

    pending_rows = 0;
    stmt_count = 0;
    let mut o = 1usize;
    while o <= h_orders {
        let max_this_stmt = rows_per_stmt
            .min(effective_batch_rows - pending_rows)
            .max(1);
        let end = (o + max_this_stmt - 1).min(h_orders);
        let mut stmt = String::from("INSERT INTO h_orders VALUES ");
        for k in o..=end {
            if k > o {
                stmt.push_str(", ");
            }
            stmt.push_str(&format!(
                "({k}, {}, 'O', {:.2}, {})",
                (k % 1000) + 1,
                100.0 + (k % 500) as f64,
                19950101 + (k % 1000)
            ));
        }
        stmt.push_str(";\n");
        if !sql.is_empty() && sql.len() + stmt.len() > 60_000 {
            load_batch(&sql)?;
            sql.clear();
            pending_rows = 0;
            stmt_count = 0;
        }
        sql.push_str(&stmt);
        pending_rows += end - o + 1;
        stmt_count += 1;
        o = end + 1;
        if pending_rows >= effective_batch_rows || stmt_count >= 60 {
            load_batch(&sql)?;
            sql.clear();
            pending_rows = 0;
            stmt_count = 0;
        }
    }
    if !sql.is_empty() {
        load_batch(&sql)?;
        sql.clear();
    }

    pending_rows = 0;
    stmt_count = 0;
    let total_lineitems = h_orders * lineitems_per_order;
    let mut li_idx = 0usize;
    while li_idx < total_lineitems {
        let max_this_stmt = rows_per_stmt
            .min(effective_batch_rows - pending_rows)
            .max(1);
        let end_idx = (li_idx + max_this_stmt).min(total_lineitems);
        let mut stmt = String::from("INSERT INTO lineitem VALUES ");
        for idx in li_idx..end_idx {
            if idx > li_idx {
                stmt.push_str(", ");
            }
            let k = (idx / lineitems_per_order) + 1;
            let l = (idx % lineitems_per_order) + 1;
            stmt.push_str(&format!(
                "({k}, {}, {}, {l}, {:.2}, {:.2}, 0.05, 0.08, 'N', 'O', {})",
                ((k + l) % items) + 1,
                ((k + l) % 100) + 1,
                (l * 5) as f64,
                (l * 100) as f64,
                19950201 + (k % 1000)
            ));
        }
        stmt.push_str(";\n");
        if !sql.is_empty() && sql.len() + stmt.len() > 60_000 {
            load_batch(&sql)?;
            sql.clear();
            pending_rows = 0;
            stmt_count = 0;
        }
        sql.push_str(&stmt);
        pending_rows += end_idx - li_idx;
        stmt_count += 1;
        li_idx = end_idx;
        if pending_rows >= effective_batch_rows || stmt_count >= 60 {
            load_batch(&sql)?;
            sql.clear();
            pending_rows = 0;
            stmt_count = 0;
        }
    }
    if !sql.is_empty() {
        load_batch(&sql)?;
    }
    Ok(())
}

#[derive(Default)]
struct QueryStats {
    latencies_us: Vec<u64>,
    errors: u64,
    logged_err: bool,
}

fn percentile(sorted: &[u64], pct: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * pct).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

type QueryFiles = Vec<(&'static str, PathBuf)>;

fn write_query_files(dir: &Path) -> Result<(QueryFiles, QueryFiles), String> {
    let oltp_queries = vec![
        (
            "tpcc_new_order_read",
            "SELECT i_id, i_name, i_price FROM item WHERE i_id = 1 LIMIT 1;\n",
        ),
        (
            "tpcc_payment_update",
            "UPDATE warehouse SET w_ytd = w_ytd + 10.00 WHERE w_id = 1;\n",
        ),
        (
            "tpcc_order_status",
            "SELECT d_id, d_w_id, d_next_o_id FROM district WHERE d_w_id = 1 LIMIT 10;\n",
        ),
    ];
    let olap_queries = vec![
        (
            "tpch_q1_agg",
            "SELECT l_returnflag, l_linestatus, COUNT(*), SUM(l_quantity), AVG(l_extendedprice) \
             FROM lineitem GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag ASC LIMIT 10;\n",
        ),
        (
            "tpch_q6_scan",
            "SELECT SUM(l_extendedprice) FROM lineitem WHERE l_discount >= 0.04 AND l_quantity <= 25.00;\n",
        ),
        (
            "tpch_q13_join",
            "SELECT o.o_orderkey, COUNT(*) FROM h_orders AS o \
             INNER JOIN lineitem AS l ON o.o_orderkey = l.l_orderkey \
             GROUP BY o.o_orderkey ORDER BY o.o_orderkey ASC LIMIT 20;\n",
        ),
    ];

    let mut oltp_paths = Vec::new();
    for (name, sql) in oltp_queries {
        let p = dir.join(format!("{name}.sql"));
        fs::write(&p, sql).map_err(|e| format!("failed to write {}: {e}", p.display()))?;
        oltp_paths.push((name, p));
    }
    let mut olap_paths = Vec::new();
    for (name, sql) in olap_queries {
        let p = dir.join(format!("{name}.sql"));
        fs::write(&p, sql).map_err(|e| format!("failed to write {}: {e}", p.display()))?;
        olap_paths.push((name, p));
    }
    Ok((oltp_paths, olap_paths))
}

fn prepare_sdk_repo(repo_dir: &Path, release_dir: &Path) -> Result<(), String> {
    use prdb_engine::sdk_registry::SdkLibraryRepository;
    use std::collections::HashSet;

    let mut allowed = HashSet::new();
    allowed.insert("libc.so.6".to_owned());
    allowed.insert("libprdb_sql.so.1".to_owned());

    let repo = SdkLibraryRepository::open(
        repo_dir,
        "linux-x86_64".to_owned(),
        "x86_64-v1".to_owned(),
        "nixos-x86_64-glibc-2.42".to_owned(),
        allowed.clone(),
        1024 * 1024 * 1024,
    )
    .map_err(|e| format!("failed to open sdk repo: {e:?}"))?;

    let tmp_lib = repo_dir.join("tmp_libprdb_sql.so.1");
    fs::copy(release_dir.join("libprdb_sql.so.1"), &tmp_lib)
        .map_err(|e| format!("failed to copy libprdb_sql.so.1: {e}"))?;
    let _ = Command::new("patchelf")
        .arg("--remove-rpath")
        .arg(&tmp_lib)
        .output();
    let library =
        fs::read(&tmp_lib).map_err(|e| format!("failed to read libprdb_sql.so.1: {e}"))?;
    let _ = fs::remove_file(&tmp_lib);

    use sha2::{Digest, Sha256};
    let digest: String = Sha256::digest(&library)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();

    let raw_meta = fs::read_to_string(release_dir.join("sdk-release.json"))
        .map_err(|e| format!("failed to read sdk-release.json: {e}"))?;
    // Replace the sha256 value in sdk-release.json with the patched library's digest
    let mut metadata = raw_meta.trim_end().to_owned();
    if let Some(pos) = metadata.find("\"sha256\":\"") {
        let start = pos + "\"sha256\":\"".len();
        if let Some(end_rel) = metadata[start..].find('"') {
            metadata.replace_range(start..start + end_rel, &digest);
        }
    }
    metadata.push('\n');

    repo.install(metadata.as_bytes(), &library, &allowed)
        .map_err(|e| {
            let dyn_info = prdb_engine::submission_manifest::decode_elf_dynamic_info(&library);
            format!("failed to install sdk release: {e:?} (dyn={dyn_info:?}, meta={metadata:?})")
        })?;
    Ok(())
}

fn main() {
    let raw_args: Vec<String> = env::args().collect();
    if raw_args.len() == 4 && raw_args[1] == "--prepare-sdk-repo" {
        if let Err(e) = prepare_sdk_repo(Path::new(&raw_args[2]), Path::new(&raw_args[3])) {
            eprintln!("prdb-bench: {e}");
            std::process::exit(1);
        }
        return;
    }
    if let Err(e) = run() {
        eprintln!("prdb-bench: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let cfg = parse_args()?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_secs();
    let run_dir = cfg
        .run_dir
        .clone()
        .unwrap_or_else(|| cfg.bench_root.join(format!("runs/r{run_id}")));
    let query_dir = run_dir.join("queries");
    let log_dir = run_dir.join("logs");
    fs::create_dir_all(&query_dir)
        .map_err(|e| format!("failed to create {}: {e}", query_dir.display()))?;
    fs::create_dir_all(&log_dir)
        .map_err(|e| format!("failed to create {}: {e}", log_dir.display()))?;
    let query_log_path = log_dir.join("queries.log");
    let query_log_file = File::create(&query_log_path)
        .map_err(|e| format!("failed to create {}: {e}", query_log_path.display()))?;
    let logger: QueryLogger = Arc::new(Mutex::new(BufWriter::new(query_log_file)));

    let oltp_creds = run_dir.join("oltp.creds");
    let olap_creds = run_dir.join("olap.creds");

    println!("prdb-bench: provisioning schema and workload profiles...");
    provision_profiles_and_schema(&cfg, &logger, &oltp_creds, &olap_creds)?;

    use prdb_engine::authorization::Operation;
    let load_scope = run_dir.join("load.scope");
    generate_scope(
        &cfg,
        &cfg.admin_credentials,
        &load_scope,
        &[
            "region",
            "nation",
            "warehouse",
            "district",
            "item",
            "h_orders",
            "lineitem",
        ],
        &[Operation::Insert],
        &[Operation::Insert],
    )?;

    println!(
        "prdb-bench: generating data for target size {:.2} GB...",
        cfg.size_gb
    );
    generate_and_load_data(&cfg, &logger, &load_scope, &query_dir)?;

    let (oltp_paths, olap_paths) = write_query_files(&query_dir)?;
    let oltp_scope = run_dir.join("oltp.scope");
    let olap_scope = run_dir.join("olap.scope");
    generate_scope(
        &cfg,
        &cfg.admin_credentials,
        &oltp_scope,
        &["item", "warehouse", "district"],
        &[Operation::Select, Operation::Update],
        &[Operation::Select, Operation::Update],
    )?;
    generate_scope(
        &cfg,
        &cfg.admin_credentials,
        &olap_scope,
        &["lineitem", "h_orders"],
        &[Operation::Select],
        &[Operation::Select],
    )?;

    let stats: Arc<Mutex<BTreeMap<String, QueryStats>>> = Arc::new(Mutex::new(BTreeMap::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let record = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();

    if cfg.workload == WorkloadMode::Mixed || cfg.workload == WorkloadMode::Tpcc {
        let warehouses = ((cfg.size_gb / 0.1).round() as usize).max(1);
        for tid in 0..cfg.oltp_threads {
            let cfg_ref = Arc::new(&cfg);
            let _ = cfg_ref;
            let addr = cfg.address.clone();
            let client = cfg.prdb_client.clone();
            let creds = oltp_creds.clone();
            let sdk_lib = cfg.sdk_library.clone();
            let inc_dir = cfg.include_dir.clone();
            let rt_prof = cfg.runtime_profile.clone();
            let cpu_base = cfg.cpu_baseline.clone();
            let sdk_rel = cfg.sdk_release;
            let scope = oltp_scope.clone();
            let w_id = (tid % warehouses) + 1;
            let pay_path = query_dir.join(format!("tpcc_payment_update_t{tid}.sql"));
            fs::write(
                &pay_path,
                format!("UPDATE warehouse SET w_ytd = w_ytd + 10.00 WHERE w_id = {w_id};\n"),
            )
            .map_err(|e| format!("failed to write {}: {e}", pay_path.display()))?;
            let mut queries = oltp_paths.clone();
            for (name, path) in &mut queries {
                if *name == "tpcc_payment_update" {
                    *path = pay_path.clone();
                }
            }
            let stats_c = Arc::clone(&stats);
            let logger_c = Arc::clone(&logger);
            let stop_c = Arc::clone(&stop);
            let rec_c = Arc::clone(&record);

            handles.push(thread::spawn(move || {
                let local_cfg = Config {
                    address: addr,
                    admin_credentials: creds.clone(),
                    prdb_client: client,
                    sdk_library: sdk_lib,
                    include_dir: inc_dir,
                    runtime_profile: rt_prof,
                    cpu_baseline: cpu_base,
                    sdk_release: sdk_rel,
                    ..Config::default()
                };
                let mut idx = 0usize;
                while !stop_c.load(Ordering::Relaxed) {
                    let (qname, qpath) = &queries[idx % queries.len()];
                    idx += 1;
                    let t0 = Instant::now();
                    let res = run_interactive_sql(
                        &local_cfg,
                        &creds,
                        "bench_oltp_user",
                        &scope,
                        "operational",
                        qpath,
                    );
                    let elapsed_us = t0.elapsed().as_micros() as u64;
                    let sql_text = fs::read_to_string(qpath).unwrap_or_default();
                    log_query(
                        &logger_c,
                        qname,
                        "bench_oltp_user",
                        "operational",
                        elapsed_us,
                        res.is_ok(),
                        sql_text.trim_end(),
                    );
                    if let Err(ref e) = res {
                        let mut map = stats_c.lock().unwrap();
                        let entry = map.entry((*qname).to_owned()).or_default();
                        if !entry.logged_err {
                            entry.logged_err = true;
                            eprintln!("prdb-bench: first error for {qname}: {e}");
                        }
                        if rec_c.load(Ordering::Relaxed) {
                            entry.errors += 1;
                        }
                    } else if rec_c.load(Ordering::Relaxed) {
                        let mut map = stats_c.lock().unwrap();
                        let entry = map.entry((*qname).to_owned()).or_default();
                        entry.latencies_us.push(elapsed_us);
                    }
                }
            }));
        }
    }

    if cfg.workload == WorkloadMode::Mixed || cfg.workload == WorkloadMode::Tpch {
        for _ in 0..cfg.olap_threads {
            let addr = cfg.address.clone();
            let client = cfg.prdb_client.clone();
            let creds = olap_creds.clone();
            let sdk_lib = cfg.sdk_library.clone();
            let inc_dir = cfg.include_dir.clone();
            let rt_prof = cfg.runtime_profile.clone();
            let cpu_base = cfg.cpu_baseline.clone();
            let sdk_rel = cfg.sdk_release;
            let scope = olap_scope.clone();
            let queries = olap_paths.clone();
            let stats_c = Arc::clone(&stats);
            let logger_c = Arc::clone(&logger);
            let stop_c = Arc::clone(&stop);
            let rec_c = Arc::clone(&record);

            handles.push(thread::spawn(move || {
                let local_cfg = Config {
                    address: addr,
                    admin_credentials: creds.clone(),
                    prdb_client: client,
                    sdk_library: sdk_lib,
                    include_dir: inc_dir,
                    runtime_profile: rt_prof,
                    cpu_baseline: cpu_base,
                    sdk_release: sdk_rel,
                    ..Config::default()
                };
                let mut idx = 0usize;
                while !stop_c.load(Ordering::Relaxed) {
                    let (qname, qpath) = &queries[idx % queries.len()];
                    idx += 1;
                    let t0 = Instant::now();
                    let res = run_interactive_sql(
                        &local_cfg,
                        &creds,
                        "bench_olap_user",
                        &scope,
                        "analytical",
                        qpath,
                    );
                    let elapsed_us = t0.elapsed().as_micros() as u64;
                    let sql_text = fs::read_to_string(qpath).unwrap_or_default();
                    log_query(
                        &logger_c,
                        qname,
                        "bench_olap_user",
                        "analytical",
                        elapsed_us,
                        res.is_ok(),
                        sql_text.trim_end(),
                    );
                    if let Err(ref e) = res {
                        let mut map = stats_c.lock().unwrap();
                        let entry = map.entry((*qname).to_owned()).or_default();
                        if !entry.logged_err {
                            entry.logged_err = true;
                            eprintln!("prdb-bench: first error for {qname}: {e}");
                        }
                        if rec_c.load(Ordering::Relaxed) {
                            entry.errors += 1;
                        }
                    } else if rec_c.load(Ordering::Relaxed) {
                        let mut map = stats_c.lock().unwrap();
                        let entry = map.entry((*qname).to_owned()).or_default();
                        entry.latencies_us.push(elapsed_us);
                    }
                }
            }));
        }
    }

    println!("prdb-bench: warming up for {}s...", cfg.warmup_secs);
    thread::sleep(Duration::from_secs(cfg.warmup_secs));
    record.store(true, Ordering::Release);
    println!("prdb-bench: measuring for {}s...", cfg.duration_secs);
    thread::sleep(Duration::from_secs(cfg.duration_secs));
    stop.store(true, Ordering::Release);

    for h in handles {
        let _ = h.join();
    }

    let mut map = stats.lock().unwrap();
    println!(
        "\n{:<24} {:>8} {:>8} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "Query", "Count", "Errors", "Min(ms)", "p50(ms)", "p95(ms)", "p99(ms)", "Max(ms)"
    );
    let mut json_entries = Vec::new();
    for (name, st) in map.iter_mut() {
        st.latencies_us.sort_unstable();
        let cnt = st.latencies_us.len();
        let min = st.latencies_us.first().copied().unwrap_or(0) as f64 / 1000.0;
        let p50 = percentile(&st.latencies_us, 0.50) as f64 / 1000.0;
        let p95 = percentile(&st.latencies_us, 0.95) as f64 / 1000.0;
        let p99 = percentile(&st.latencies_us, 0.99) as f64 / 1000.0;
        let max = st.latencies_us.last().copied().unwrap_or(0) as f64 / 1000.0;
        println!(
            "{:<24} {:>8} {:>8} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>10.2}",
            name, cnt, st.errors, min, p50, p95, p99, max
        );
        json_entries.push(format!(
            "  \"{name}\": {{\"count\": {cnt}, \"errors\": {}, \"min_ms\": {min:.3}, \"p50_ms\": {p50:.3}, \"p95_ms\": {p95:.3}, \"p99_ms\": {p99:.3}, \"max_ms\": {max:.3}}}",
            st.errors
        ));
    }

    let results_path = run_dir.join("results.json");
    let mut f = File::create(&results_path).map_err(|e| e.to_string())?;
    writeln!(f, "{{\n{}\n}}", json_entries.join(",\n")).map_err(|e| e.to_string())?;
    println!("\nResults written to {}", results_path.display());
    println!("Query log written to {}", query_log_path.display());

    if !cfg.keep_data {
        let _ = fs::remove_dir_all(&run_dir);
    }
    Ok(())
}
