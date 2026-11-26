#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
fn main() {
    eprintln!("prdb-supervisor currently supports Linux x86_64 only");
    std::process::exit(1);
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
mod service {
    use prdb_engine::bootstrap::StorageInstance;
    use prdb_engine::executor_launcher::ExecutorLaunchConfig;
    use prdb_engine::linux_sandbox::WorkerResourceLimits;
    use prdb_engine::spill::{SpillBroker, SpillConfig};
    use prdb_engine::submission_manifest::{
        ArtifactCompatibilityProfile, VersionedSubmissionMetadataValidator,
    };
    use prdb_engine::supervisor::{StagingLimits, StagingStore};
    use prdb_engine::supervisor_ipc::{
        GatewayAdmissionConfig, GatewayExecutorConfig, GatewayServiceListenerConfig,
        GatewayServiceRuntimeConfig, bind_gateway_service_socket_with_mode,
        observe_executor_relay_and_finalize_gateway, run_gateway_service_with_recovery,
    };
    use std::collections::HashSet;
    use std::os::unix::fs::{FileTypeExt, MetadataExt};
    use std::os::unix::net::UnixListener;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    static SIGNAL_REQUESTED: AtomicBool = AtomicBool::new(false);

    extern "C" fn request_shutdown(_signal: libc::c_int) {
        SIGNAL_REQUESTED.store(true, Ordering::Release);
    }

    #[derive(Default)]
    struct Options {
        instance_dir: Option<PathBuf>,
        service_socket: Option<PathBuf>,
        gateway_uid: Option<u32>,
        service_socket_mode: Option<u32>,
        cgroup_parent: Option<PathBuf>,
        executor: Option<PathBuf>,
        optimizer: Option<PathBuf>,
        scratch_parent: Option<PathBuf>,
        spill_parent: Option<PathBuf>,
        spill_max_bytes: Option<u64>,
        spill_max_io_bytes: Option<u64>,
        spill_max_streams: Option<u64>,
        spill_max_operations: Option<u64>,
        spill_max_response_bytes: Option<u64>,
        spill_max_service_bytes: Option<u64>,
        runtime_read_execute: Vec<PathBuf>,
        runtime_read_only: Vec<PathBuf>,
        profile_cpu: Option<String>,
        profile_runtime: Option<String>,
        sdk_repository_dir: Option<PathBuf>,
        sdk_repository_max_bytes: Option<u64>,
        dependencies: HashSet<String>,
        retention_seconds: Option<u64>,
        max_rows: Option<usize>,
        max_bytes: Option<usize>,
        max_total_result_bytes: Option<u64>,
        max_sql_index_probe_candidates: Option<u32>,
        max_sql_index_range_bytes: Option<usize>,
        max_principal_reservations: Option<usize>,
        max_pending_submissions: Option<usize>,
        max_active_invocations: Option<usize>,
        max_storage_active_snapshots: Option<usize>,
        max_invocation_scan_work: Option<usize>,
        max_planner_estimate_requests: Option<usize>,
        max_planner_estimate_candidates: Option<usize>,
        max_statistics_collection_rows: Option<usize>,
        max_active_per_principal: Option<usize>,
        cpu_quota_us: Option<u64>,
        cpu_period_us: Option<u64>,
        memory_max_bytes: Option<u64>,
        max_processes: Option<u64>,
    }

    pub fn main() {
        if std::env::args().any(|argument| argument == "--help" || argument == "-h") {
            print_help();
            return;
        }
        if let Err(error) = run() {
            eprintln!("prdb-supervisor: {error}");
            std::process::exit(1);
        }
    }

    fn print_help() {
        println!(
            "prdb-supervisor [options]\n\
             Required: --instance-dir PATH --service-socket PATH --gateway-uid UID\n\
             [--service-socket-mode 0600|0660]\n\
             --cgroup-parent PATH --executor PATH --optimizer PATH --scratch-parent PATH\n\
             [--spill-parent PATH --spill-max-bytes N --spill-max-io-bytes N\n\
             --spill-max-streams N --spill-max-operations N\n\
             --spill-max-response-bytes N --spill-max-service-bytes N]\n\
             --runtime-read-execute PATH (repeatable) --runtime-profile NAME\n\
             --cpu-baseline NAME --allowed-dependency NAME (repeatable)\n\
             [--sdk-repository-dir PATH --sdk-repository-max-bytes N]\n\
             --retention-seconds N --max-rows N --max-bytes N\n\
             [--max-total-result-bytes N] (default 67108864)\n\
             [--analytical-max-total-result-bytes N] (overrides the operational value)\n\
             [--max-sql-index-probe-candidates N] (default 128; maximum 16384)\n\
             [--analytical-max-sql-index-probe-candidates N] (overrides the operational value)\n\
             [--max-sql-index-range-bytes N] (default 16777216)\n\
             [--analytical-max-sql-index-range-bytes N] (overrides the operational value)\n\
             --max-principal-reservations N\n\
             [--max-pending-operational N --max-pending-analytical N]\n\
             [--max-active-operational N --max-active-analytical N]\n\
             [--max-active-operational-per-principal N\n\
             --max-active-analytical-per-principal N]\n\
             [--max-storage-active-snapshots N\n\
             --max-storage-active-analytical-snapshots N]\n\
             [--max-invocation-scan-work N] (default 65536)\n\
             [--analytical-max-invocation-scan-work N] (overrides the operational value)\n\
             [--max-planner-estimate-requests N] (default 16; maximum 4096)\n\
             [--analytical-max-planner-estimate-requests N] (overrides the operational value)\n\
             [--max-planner-estimate-candidates N] (default 16384; maximum 1048576)\n\
             [--analytical-max-planner-estimate-candidates N] (overrides the operational value)\n\
             [--max-statistics-collection-rows N] (default 16384; maximum 1048576)\n\
             --cpu-quota-us N --cpu-period-us N --memory-max-bytes N --max-processes N\n\
             [--analytical-cpu-quota-us N --analytical-cpu-period-us N\n\
             --analytical-memory-max-bytes N --analytical-max-processes N]\n\
             --runtime-read-only PATH (repeatable)"
        );
    }

    fn run() -> Result<(), Box<dyn std::error::Error>> {
        let options = parse_options()?;
        let spill_config = spill_config(&options)?;
        // Validate and retain the service broker independently from artifact
        // staging. Worker-channel binding is added in the executor launch path.
        let spill_broker = spill_config
            .map(SpillBroker::open)
            .transpose()
            .map_err(|error| format!("secure spill configuration rejected: {error:?}"))?;
        let storage_config = storage_manager_config(&options)?;
        let instance_dir = required(options.instance_dir, "--instance-dir")?;
        let socket_path = required(options.service_socket, "--service-socket")?;
        let gateway_uid = required(options.gateway_uid, "--gateway-uid")?;
        let service_socket_mode = options.service_socket_mode.unwrap_or(0o600);
        let cgroup_parent =
            resolve_cgroup_parent(required(options.cgroup_parent, "--cgroup-parent")?)?;
        let executor_path = required(options.executor, "--executor")?;
        let optimizer_path = required(options.optimizer, "--optimizer")?;
        let scratch_parent = required(options.scratch_parent, "--scratch-parent")?;
        let cpu_baseline = required(options.profile_cpu, "--cpu-baseline")?;
        let runtime_profile = required(options.profile_runtime, "--runtime-profile")?;
        let retention_seconds = required(options.retention_seconds, "--retention-seconds")?;
        let retention_period_us = retention_seconds
            .checked_mul(1_000_000)
            .and_then(|value| i64::try_from(value).ok())
            .filter(|value| *value > 0)
            .ok_or("--retention-seconds must be positive and fit in signed microseconds")?;
        let max_rows = positive(required(options.max_rows, "--max-rows")?, "--max-rows")?;
        let max_bytes = positive(required(options.max_bytes, "--max-bytes")?, "--max-bytes")?;
        let max_total_result_bytes = positive(
            options
                .max_total_result_bytes
                .unwrap_or(prdb_engine::query_abi::MAX_TOTAL_RESULT_BYTES),
            "--max-total-result-bytes",
        )?;
        let max_sql_index_probe_candidates = options
            .max_sql_index_probe_candidates
            .unwrap_or(prdb_engine::query_abi::DEFAULT_SQL_INDEX_PROBE_CANDIDATES);
        if max_sql_index_probe_candidates == 0
            || max_sql_index_probe_candidates
                > prdb_engine::query_abi::MAX_SQL_INDEX_PROBE_CANDIDATES
        {
            return Err(format!(
                "--max-sql-index-probe-candidates must be in 1..={}",
                prdb_engine::query_abi::MAX_SQL_INDEX_PROBE_CANDIDATES
            )
            .into());
        }
        let resource_limits = WorkerResourceLimits {
            cpu_quota_micros: positive(
                required(options.cpu_quota_us, "--cpu-quota-us")?,
                "--cpu-quota-us",
            )?,
            cpu_period_micros: positive(
                required(options.cpu_period_us, "--cpu-period-us")?,
                "--cpu-period-us",
            )?,
            memory_max_bytes: positive(
                required(options.memory_max_bytes, "--memory-max-bytes")?,
                "--memory-max-bytes",
            )?,
            max_processes: positive(
                required(options.max_processes, "--max-processes")?,
                "--max-processes",
            )?,
        };
        if options.runtime_read_execute.is_empty() {
            return Err("at least one --runtime-read-execute path is required".into());
        }
        let profile = ArtifactCompatibilityProfile::for_repository(
            cpu_baseline.clone(),
            runtime_profile.clone(),
            options.dependencies.clone(),
        )
        .map_err(|error| format!("invalid compatibility profile: {error:?}"))?;
        let repository_root = options
            .sdk_repository_dir
            .unwrap_or_else(|| instance_dir.join(prdb_engine::bootstrap::SDK_REPOSITORY_DIRECTORY));
        let repository = Arc::new(
            prdb_engine::sdk_registry::SdkLibraryRepository::open(
                &repository_root,
                "linux-x86_64".to_owned(),
                cpu_baseline.clone(),
                runtime_profile.clone(),
                options.dependencies.clone(),
                options
                    .sdk_repository_max_bytes
                    .unwrap_or(1024 * 1024 * 1024),
            )
            .map_err(|error| format!("invalid SQL SDK library repository: {error:?}"))?,
        );
        let mut validator = VersionedSubmissionMetadataValidator::new(profile);
        validator = validator
            .with_sdk_registry(Arc::clone(&repository))
            .map_err(|error| format!("invalid SQL SDK repository profile: {error:?}"))?;
        let validator = Arc::new(validator);
        let default_staging_limits = StagingLimits::default();
        let staging_limits = StagingLimits {
            max_pending_submissions: positive(
                options
                    .max_pending_submissions
                    .unwrap_or(default_staging_limits.max_pending_submissions),
                "--max-pending-submissions",
            )?,
            max_live_reservations_per_principal: positive(
                options
                    .max_principal_reservations
                    .unwrap_or(default_staging_limits.max_live_reservations_per_principal),
                "--max-principal-reservations",
            )?,
            max_active_invocations: positive(
                options
                    .max_active_invocations
                    .unwrap_or(default_staging_limits.max_active_invocations),
                "--max-active-invocations",
            )?,
            max_active_per_principal: positive(
                options
                    .max_active_per_principal
                    .unwrap_or(default_staging_limits.max_active_per_principal),
                "--max-active-per-principal",
            )?,
            ..default_staging_limits
        };
        validate_storage_admission_capacity(&staging_limits, &storage_config)?;
        let staging = StagingStore::new(staging_limits, validator)?
            .with_sdk_repository(Arc::clone(&repository));
        let instance = StorageInstance::open(&instance_dir)?;
        let runtime = GatewayServiceRuntimeConfig {
            admission: GatewayAdmissionConfig {
                retention_period_us,
                max_rows,
                max_bytes,
            },
            executor: GatewayExecutorConfig {
                cgroup_parent,
                resource_limits,
                launch: ExecutorLaunchConfig {
                    executor_path,
                    optimizer_path,
                    scratch_parent,
                    runtime_read_execute: options.runtime_read_execute,
                    runtime_read_only: options.runtime_read_only,
                    spill_broker: spill_broker.clone(),
                    max_total_result_bytes,
                    max_sql_index_probe_candidates,
                    cleanup_timeout: Duration::from_secs(2),
                },
            },
            listener: GatewayServiceListenerConfig {
                expected_gateway_uid: gateway_uid,
                max_connections: 128,
                idle_timeout: Duration::from_secs(30),
                accept_poll_interval: Duration::from_millis(50),
                outcome_maintenance_interval: Duration::from_secs(60),
                outcome_maintenance_batch: prdb_engine::outcome::MAX_OUTCOME_PRUNE_BATCH,
                statistics_maintenance_interval: Duration::from_secs(900),
            },
        };

        install_signal_handlers()?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let signal_shutdown = Arc::clone(&shutdown);
        let socket_identity = Arc::new(std::sync::Mutex::new(None));
        let bind_socket_state = Arc::clone(&socket_identity);
        let bind_socket_path = socket_path.clone();
        let signal_monitor = std::thread::spawn(move || {
            while !SIGNAL_REQUESTED.load(Ordering::Acquire)
                && !signal_shutdown.load(Ordering::Acquire)
            {
                std::thread::sleep(Duration::from_millis(20));
            }
            signal_shutdown.store(true, Ordering::Release);
        });
        let result = run_gateway_service_with_recovery(
            instance,
            storage_config,
            &staging,
            runtime,
            Arc::clone(&shutdown),
            move || {
                let previous_identity = bind_socket_state
                    .lock()
                    .map_err(|_| std::io::Error::other("service socket identity lock poisoned"))?
                    .take();
                let listener =
                    bind_private_socket(&bind_socket_path, previous_identity, service_socket_mode)?;
                let metadata = std::fs::symlink_metadata(&bind_socket_path)?;
                let mut identity = bind_socket_state
                    .lock()
                    .map_err(|_| std::io::Error::other("service socket identity lock poisoned"))?;
                *identity = Some((metadata.dev(), metadata.ino()));
                Ok(listener)
            },
            |manager, request_id, stream, mut invocation, _submission, mut executor| {
                if let Err(error) = observe_executor_relay_and_finalize_gateway(
                    manager,
                    &mut invocation,
                    &mut executor,
                    stream,
                    request_id,
                ) {
                    eprintln!("prdb-supervisor: invocation relay failed: {error}");
                }
            },
        );
        shutdown.store(true, Ordering::Release);
        signal_monitor
            .join()
            .map_err(|_| "supervisor signal monitor panicked")?;
        let identity = socket_identity
            .lock()
            .map_err(|_| "service socket identity lock poisoned")?
            .take();
        let cleanup_result = remove_owned_service_socket(&socket_path, identity);
        result?;
        cleanup_result?;
        Ok(())
    }

    fn resolve_cgroup_parent(path: PathBuf) -> Result<PathBuf, Box<dyn std::error::Error>> {
        resolve_cgroup_parent_with_delegated(path, std::env::var_os("PRDB_SYSTEMD_CGROUP_PARENT"))
    }

    fn resolve_cgroup_parent_with_delegated(
        path: PathBuf,
        delegated: Option<std::ffi::OsString>,
    ) -> Result<PathBuf, Box<dyn std::error::Error>> {
        if path == std::path::Path::new("systemd") {
            let delegated =
                delegated.ok_or("--cgroup-parent systemd requires PRDB_SYSTEMD_CGROUP_PARENT")?;
            let delegated = PathBuf::from(delegated);
            if !delegated.is_absolute() {
                return Err("PRDB_SYSTEMD_CGROUP_PARENT must be an absolute path".into());
            }
            Ok(delegated)
        } else {
            Ok(path)
        }
    }

    fn bind_private_socket(
        path: &std::path::Path,
        previously_owned: Option<(u64, u64)>,
        mode: u32,
    ) -> std::io::Result<UnixListener> {
        match std::fs::symlink_metadata(path) {
            Ok(metadata)
                if metadata.file_type().is_socket()
                    && previously_owned == Some((metadata.dev(), metadata.ino())) =>
            {
                std::fs::remove_file(path)?;
            }
            Ok(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "service socket path exists and is not this supervisor's socket",
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        bind_gateway_service_socket_with_mode(path, mode)
    }

    fn remove_owned_service_socket(
        path: &std::path::Path,
        owned: Option<(u64, u64)>,
    ) -> std::io::Result<()> {
        let Some(owned) = owned else {
            return Ok(());
        };
        match std::fs::symlink_metadata(path) {
            Ok(metadata)
                if metadata.file_type().is_socket()
                    && owned == (metadata.dev(), metadata.ino()) =>
            {
                std::fs::remove_file(path)
            }
            Ok(_) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn parse_options() -> Result<Options, Box<dyn std::error::Error>> {
        let mut options = Options::default();
        let mut arguments = std::env::args().skip(1);
        while let Some(flag) = arguments.next() {
            let value = arguments
                .next()
                .ok_or_else(|| format!("missing value for {flag}"))?;
            macro_rules! set {
                ($field:ident, $parse:expr) => {{
                    if options.$field.is_some() {
                        return Err(format!("{flag} may be specified only once").into());
                    }
                    options.$field = Some($parse);
                }};
            }
            match flag.as_str() {
                "--instance-dir" => set!(instance_dir, PathBuf::from(value)),
                "--service-socket" => set!(service_socket, PathBuf::from(value)),
                "--gateway-uid" => set!(gateway_uid, value.parse::<u32>()?),
                "--service-socket-mode" => {
                    set!(
                        service_socket_mode,
                        u32::from_str_radix(value.trim_start_matches("0o"), 8)?
                    )
                }
                "--cgroup-parent" => set!(cgroup_parent, PathBuf::from(value)),
                "--executor" => set!(executor, PathBuf::from(value)),
                "--optimizer" => set!(optimizer, PathBuf::from(value)),
                "--scratch-parent" => set!(scratch_parent, PathBuf::from(value)),
                "--spill-parent" => set!(spill_parent, PathBuf::from(value)),
                "--spill-max-bytes" => set!(spill_max_bytes, value.parse::<u64>()?),
                "--spill-max-io-bytes" => set!(spill_max_io_bytes, value.parse::<u64>()?),
                "--spill-max-streams" => set!(spill_max_streams, value.parse::<u64>()?),
                "--spill-max-operations" => {
                    set!(spill_max_operations, value.parse::<u64>()?)
                }
                "--spill-max-response-bytes" => {
                    set!(spill_max_response_bytes, value.parse::<u64>()?)
                }
                "--spill-max-service-bytes" => {
                    set!(spill_max_service_bytes, value.parse::<u64>()?)
                }
                "--runtime-read-execute" => options.runtime_read_execute.push(value.into()),
                "--runtime-read-only" => options.runtime_read_only.push(value.into()),
                "--cpu-baseline" => set!(profile_cpu, value),
                "--runtime-profile" => set!(profile_runtime, value),
                "--sdk-repository-dir" => set!(sdk_repository_dir, PathBuf::from(value)),
                "--sdk-repository-max-bytes" => {
                    set!(sdk_repository_max_bytes, value.parse::<u64>()?)
                }
                "--allowed-dependency" => {
                    if !options.dependencies.insert(value) {
                        return Err("duplicate --allowed-dependency".into());
                    }
                }
                "--retention-seconds" => set!(retention_seconds, value.parse::<u64>()?),
                "--max-rows" => set!(max_rows, value.parse::<usize>()?),
                "--max-bytes" => set!(max_bytes, value.parse::<usize>()?),
                "--max-total-result-bytes" => {
                    set!(max_total_result_bytes, value.parse::<u64>()?)
                }
                "--max-sql-index-probe-candidates" => {
                    set!(max_sql_index_probe_candidates, value.parse::<u32>()?)
                }
                "--max-sql-index-range-bytes" => {
                    set!(max_sql_index_range_bytes, value.parse::<usize>()?)
                }
                "--max-principal-reservations" => {
                    set!(max_principal_reservations, value.parse::<usize>()?)
                }
                "--max-pending-submissions" => {
                    set!(max_pending_submissions, value.parse::<usize>()?)
                }
                "--max-active-invocations" => {
                    set!(max_active_invocations, value.parse::<usize>()?)
                }
                "--max-storage-active-snapshots" => {
                    set!(max_storage_active_snapshots, value.parse::<usize>()?)
                }
                "--max-invocation-scan-work" => {
                    set!(max_invocation_scan_work, value.parse::<usize>()?)
                }
                "--max-planner-estimate-requests" => {
                    set!(max_planner_estimate_requests, value.parse::<usize>()?)
                }
                "--max-planner-estimate-candidates" => {
                    set!(max_planner_estimate_candidates, value.parse::<usize>()?)
                }
                "--max-statistics-collection-rows" => {
                    set!(max_statistics_collection_rows, value.parse::<usize>()?)
                }
                "--max-active-per-principal" => {
                    set!(max_active_per_principal, value.parse::<usize>()?)
                }
                "--cpu-quota-us" => set!(cpu_quota_us, value.parse::<u64>()?),
                "--cpu-period-us" => set!(cpu_period_us, value.parse::<u64>()?),
                "--memory-max-bytes" => set!(memory_max_bytes, value.parse::<u64>()?),
                "--max-processes" => set!(max_processes, value.parse::<u64>()?),
                _ => return Err(format!("unknown option {flag}").into()),
            }
        }
        Ok(options)
    }

    fn required<T>(value: Option<T>, option: &str) -> Result<T, String> {
        value.ok_or_else(|| format!("{option} is required"))
    }

    fn positive<T>(value: T, option: &str) -> Result<T, String>
    where
        T: Copy + PartialOrd + From<u8>,
    {
        if value < T::from(1) {
            Err(format!("{option} must be positive"))
        } else {
            Ok(value)
        }
    }

    fn spill_config(options: &Options) -> Result<Option<SpillConfig>, String> {
        let supplied = [
            options.spill_parent.is_some(),
            options.spill_max_bytes.is_some(),
            options.spill_max_io_bytes.is_some(),
            options.spill_max_streams.is_some(),
            options.spill_max_operations.is_some(),
            options.spill_max_response_bytes.is_some(),
            options.spill_max_service_bytes.is_some(),
        ]
        .into_iter()
        .filter(|present| *present)
        .count();
        if supplied == 0 {
            return Ok(None);
        }
        if supplied != 7 {
            return Err("all seven --spill-* options must be supplied together".to_owned());
        }
        let config = SpillConfig {
            parent: required(options.spill_parent.clone(), "--spill-parent")?,
            max_bytes_per_invocation: positive(
                required(options.spill_max_bytes, "--spill-max-bytes")?,
                "--spill-max-bytes",
            )?,
            max_io_bytes_per_invocation: positive(
                required(options.spill_max_io_bytes, "--spill-max-io-bytes")?,
                "--spill-max-io-bytes",
            )?,
            max_streams_per_invocation: positive(
                required(options.spill_max_streams, "--spill-max-streams")?,
                "--spill-max-streams",
            )?,
            max_operations_per_invocation: positive(
                required(options.spill_max_operations, "--spill-max-operations")?,
                "--spill-max-operations",
            )?,
            max_response_bytes: positive(
                required(
                    options.spill_max_response_bytes,
                    "--spill-max-response-bytes",
                )?,
                "--spill-max-response-bytes",
            )?,
            max_service_bytes: positive(
                required(options.spill_max_service_bytes, "--spill-max-service-bytes")?,
                "--spill-max-service-bytes",
            )?,
        };
        config
            .validate()
            .map_err(|error| format!("invalid spill limits: {error:?}"))?;
        Ok(Some(config))
    }

    fn storage_manager_config(
        options: &Options,
    ) -> Result<prdb_engine::storage_manager::StorageManagerConfig, String> {
        let defaults = prdb_engine::storage_manager::StorageManagerConfig::default();
        let max_invocation_scan_work = positive(
            options
                .max_invocation_scan_work
                .unwrap_or(defaults.max_invocation_scan_work),
            "--max-invocation-scan-work",
        )?;
        let max_authorized_index_range_candidates = positive(
            options
                .max_sql_index_probe_candidates
                .map(|value| value as usize)
                .unwrap_or(defaults.max_authorized_index_range_candidates),
            "--max-sql-index-probe-candidates",
        )?;
        let max_authorized_index_range_bytes = positive(
            options
                .max_sql_index_range_bytes
                .unwrap_or(defaults.max_authorized_index_range_bytes),
            "--max-sql-index-range-bytes",
        )?;
        if max_authorized_index_range_candidates
            > prdb_engine::query_abi::MAX_SQL_INDEX_PROBE_CANDIDATES as usize
        {
            return Err(format!(
                "SQL index probe candidates must not exceed {}",
                prdb_engine::query_abi::MAX_SQL_INDEX_PROBE_CANDIDATES
            ));
        }
        let max_planner_estimate_requests = positive(
            options
                .max_planner_estimate_requests
                .unwrap_or(defaults.max_planner_estimate_requests),
            "--max-planner-estimate-requests",
        )?;
        let max_planner_estimate_candidates = positive(
            options
                .max_planner_estimate_candidates
                .unwrap_or(prdb_engine::statistics::DEFAULT_ESTIMATE_CANDIDATES),
            "--max-planner-estimate-candidates",
        )?;
        let max_statistics_collection_rows = positive(
            options
                .max_statistics_collection_rows
                .unwrap_or(defaults.max_statistics_collection_rows),
            "--max-statistics-collection-rows",
        )?;
        if max_planner_estimate_requests > prdb_engine::transaction::MAX_PLANNER_ESTIMATE_REQUESTS {
            return Err(format!(
                "planner estimate requests must not exceed {}",
                prdb_engine::transaction::MAX_PLANNER_ESTIMATE_REQUESTS
            ));
        }
        if max_planner_estimate_candidates
            > prdb_engine::statistics::MAX_CONFIGURABLE_ESTIMATE_CANDIDATES
        {
            return Err(format!(
                "planner estimate candidates must not exceed {}",
                prdb_engine::statistics::MAX_CONFIGURABLE_ESTIMATE_CANDIDATES
            ));
        }
        if max_statistics_collection_rows as u64
            > prdb_engine::statistics::MAX_CONFIGURABLE_COMMITTED_POPULATION_ROWS
        {
            return Err(format!(
                "statistics collection rows must not exceed {}",
                prdb_engine::statistics::MAX_CONFIGURABLE_COMMITTED_POPULATION_ROWS
            ));
        }
        Ok(prdb_engine::storage_manager::StorageManagerConfig {
            max_active_invocations: positive(
                options
                    .max_storage_active_snapshots
                    .unwrap_or(defaults.max_active_invocations),
                "--max-storage-active-snapshots",
            )?,
            max_invocation_scan_work,
            max_authorized_index_range_candidates,
            max_authorized_index_range_bytes,
            max_planner_estimate_requests,
            max_planner_estimate_candidates,
            max_statistics_collection_rows,
            ..defaults
        })
    }

    fn validate_storage_admission_capacity(
        staging: &StagingLimits,
        storage: &prdb_engine::storage_manager::StorageManagerConfig,
    ) -> Result<(), String> {
        if storage.max_active_invocations < staging.max_active_invocations {
            return Err(format!(
                "--max-storage-active-snapshots must be at least --max-active-invocations ({})",
                staging.max_active_invocations
            ));
        }
        Ok(())
    }

    fn install_signal_handlers() -> Result<(), std::io::Error> {
        // SAFETY: the handler only performs an atomic store before shutdown.
        unsafe {
            for signal in [libc::SIGINT, libc::SIGTERM] {
                if libc::signal(signal, request_shutdown as *const () as libc::sighandler_t)
                    == libc::SIG_ERR
                {
                    return Err(std::io::Error::last_os_error());
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::{
            Options, resolve_cgroup_parent_with_delegated, spill_config, storage_manager_config,
            validate_storage_admission_capacity,
        };
        use prdb_engine::storage_manager::StorageManagerConfig;
        use prdb_engine::supervisor::StagingLimits;

        #[test]
        fn spill_configuration_is_disabled_or_complete_and_positive() {
            assert!(spill_config(&Options::default()).unwrap().is_none());

            let partial = Options {
                spill_parent: Some("/var/lib/prdb/spill".into()),
                spill_max_bytes: Some(4096),
                ..Options::default()
            };
            assert!(spill_config(&partial).is_err());

            let complete = Options {
                spill_parent: Some("/var/lib/prdb/spill".into()),
                spill_max_bytes: Some(4096),
                spill_max_io_bytes: Some(8192),
                spill_max_streams: Some(8),
                spill_max_operations: Some(128),
                spill_max_response_bytes: Some(1024),
                spill_max_service_bytes: Some(32768),
                ..Options::default()
            };
            let config = spill_config(&complete).unwrap().unwrap();
            assert_eq!(config.max_bytes_per_invocation, 4096);
            assert_eq!(config.max_service_bytes, 32768);

            let zero = Options {
                spill_parent: Some("/var/lib/prdb/spill".into()),
                spill_max_bytes: Some(0),
                spill_max_io_bytes: Some(8192),
                spill_max_streams: Some(8),
                spill_max_operations: Some(128),
                spill_max_response_bytes: Some(1024),
                spill_max_service_bytes: Some(32768),
                ..Options::default()
            };
            assert!(spill_config(&zero).is_err());
        }

        #[test]
        fn systemd_cgroup_parent_requires_an_absolute_exported_path() {
            let systemd = std::path::PathBuf::from("systemd");
            assert!(resolve_cgroup_parent_with_delegated(systemd.clone(), None).is_err());
            assert!(
                resolve_cgroup_parent_with_delegated(
                    systemd.clone(),
                    Some("relative/cgroup".into())
                )
                .is_err()
            );
            let delegated = std::path::PathBuf::from("/sys/fs/cgroup/user.slice/prdb.service");
            assert_eq!(
                resolve_cgroup_parent_with_delegated(
                    systemd,
                    Some(delegated.as_os_str().to_os_string())
                )
                .expect("absolute delegated path"),
                delegated
            );
            assert_eq!(
                resolve_cgroup_parent_with_delegated(
                    std::path::PathBuf::from("/custom/cgroup"),
                    None
                )
                .expect("explicit path remains supported"),
                std::path::PathBuf::from("/custom/cgroup")
            );
        }

        #[test]
        fn storage_caps_must_cover_configured_supervisor_active_admissions() {
            let staging = StagingLimits::default();
            let storage = StorageManagerConfig::default();
            assert!(validate_storage_admission_capacity(&staging, &storage).is_ok());

            let insufficient_global = StorageManagerConfig {
                max_active_invocations: 63,
                ..storage
            };
            assert!(
                validate_storage_admission_capacity(&staging, &insufficient_global)
                    .unwrap_err()
                    .contains("--max-active-invocations")
            );
        }

        #[test]
        fn storage_snapshot_limits_use_cli_values_and_documented_defaults() {
            let defaults = storage_manager_config(&Options::default()).expect("default config");
            assert_eq!(defaults.max_active_invocations, 128);
            assert_eq!(defaults.max_planner_estimate_requests, 16);
            assert_eq!(defaults.max_invocation_scan_work, 65_536);
            assert_eq!(
                defaults.max_authorized_index_range_candidates,
                prdb_engine::query_abi::DEFAULT_SQL_INDEX_PROBE_CANDIDATES as usize
            );
            assert_eq!(defaults.max_authorized_index_range_bytes, 16 * 1024 * 1024);
            assert_eq!(defaults.max_planner_estimate_candidates, 16_384);
            assert_eq!(defaults.max_statistics_collection_rows, 16_384);

            let configured = storage_manager_config(&Options {
                max_storage_active_snapshots: Some(48),
                max_invocation_scan_work: Some(131_072),
                max_sql_index_probe_candidates: Some(512),
                max_sql_index_range_bytes: Some(64 * 1024 * 1024),
                max_planner_estimate_requests: Some(64),
                max_planner_estimate_candidates: Some(32_768),
                max_statistics_collection_rows: Some(65_536),
                ..Options::default()
            })
            .expect("configured limits");
            assert_eq!(configured.max_active_invocations, 48);
            assert_eq!(configured.max_invocation_scan_work, 131_072);
            assert_eq!(configured.max_authorized_index_range_candidates, 512);
            assert_eq!(
                configured.max_authorized_index_range_bytes,
                64 * 1024 * 1024
            );
            assert_eq!(configured.max_planner_estimate_requests, 64);
            assert_eq!(configured.max_planner_estimate_candidates, 32_768);
            assert_eq!(configured.max_statistics_collection_rows, 65_536);
        }

        #[test]
        fn storage_snapshot_limits_reject_zero_cli_values() {
            let error = storage_manager_config(&Options {
                max_storage_active_snapshots: Some(0),
                ..Options::default()
            })
            .expect_err("zero global storage limit must fail");
            assert!(error.contains("--max-storage-active-snapshots must be positive"));

            let candidate_error = storage_manager_config(&Options {
                max_sql_index_probe_candidates: Some(0),
                ..Options::default()
            })
            .expect_err("zero candidate limit must fail");
            assert!(candidate_error.contains("--max-sql-index-probe-candidates must be positive"));

            let maintenance_error = storage_manager_config(&Options {
                max_statistics_collection_rows: Some(0),
                ..Options::default()
            })
            .expect_err("zero statistics maintenance limit must fail");
            assert!(
                maintenance_error.contains("--max-statistics-collection-rows must be positive")
            );

            let too_large = storage_manager_config(&Options {
                max_planner_estimate_requests: Some(
                    prdb_engine::transaction::MAX_PLANNER_ESTIMATE_REQUESTS + 1,
                ),
                ..Options::default()
            })
            .expect_err("estimate allowance above the implementation envelope must fail");
            assert!(too_large.contains("must not exceed"));

            let too_many_candidates = storage_manager_config(&Options {
                max_planner_estimate_candidates: Some(
                    prdb_engine::statistics::MAX_CONFIGURABLE_ESTIMATE_CANDIDATES + 1,
                ),
                ..Options::default()
            })
            .expect_err("candidate limit above the implementation envelope must fail");
            assert!(too_many_candidates.contains("must not exceed"));

            let too_many_statistics_rows = storage_manager_config(&Options {
                max_statistics_collection_rows: Some(
                    prdb_engine::statistics::MAX_CONFIGURABLE_COMMITTED_POPULATION_ROWS as usize
                        + 1,
                ),
                ..Options::default()
            })
            .expect_err("maintenance limit above implementation envelope must fail");
            assert!(too_many_statistics_rows.contains("must not exceed"));
        }
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn main() {
    service::main();
}
