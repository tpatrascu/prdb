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
    use prdb_engine::submission_manifest::{
        ArtifactCompatibilityProfile, VersionedSubmissionMetadataValidator,
    };
    use prdb_engine::supervisor::{StagingLimits, StagingStore};
    use prdb_engine::supervisor_ipc::{
        GatewayAdmissionConfig, GatewayExecutorConfig, GatewayServiceListenerConfig,
        GatewayServiceRuntimeConfig, bind_gateway_service_socket,
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
        cgroup_parent: Option<PathBuf>,
        executor: Option<PathBuf>,
        scratch_parent: Option<PathBuf>,
        runtime_read_execute: Vec<PathBuf>,
        runtime_read_only: Vec<PathBuf>,
        profile_cpu: Option<String>,
        profile_runtime: Option<String>,
        sdk_release: Option<u32>,
        dependencies: HashSet<String>,
        retention_seconds: Option<u64>,
        max_rows: Option<usize>,
        max_bytes: Option<usize>,
        max_principal_reservations: Option<usize>,
        max_active_operational: Option<usize>,
        max_active_analytical: Option<usize>,
        max_active_operational_per_principal: Option<usize>,
        max_active_analytical_per_principal: Option<usize>,
        cpu_quota_us: Option<u64>,
        cpu_period_us: Option<u64>,
        memory_max_bytes: Option<u64>,
        max_processes: Option<u64>,
        analytical_cpu_quota_us: Option<u64>,
        analytical_cpu_period_us: Option<u64>,
        analytical_memory_max_bytes: Option<u64>,
        analytical_max_processes: Option<u64>,
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
             --cgroup-parent PATH --executor PATH --scratch-parent PATH\n\
             --runtime-read-execute PATH (repeatable) --runtime-profile NAME\n\
             --cpu-baseline NAME --sdk-release NUMBER --allowed-dependency NAME (repeatable)\n\
             --retention-seconds N --max-rows N --max-bytes N\n\
             --max-principal-reservations N\n\
             [--max-active-operational N --max-active-analytical N]\n\
             [--max-active-operational-per-principal N\n\
             --max-active-analytical-per-principal N]\n\
             --cpu-quota-us N --cpu-period-us N --memory-max-bytes N --max-processes N\n\
             [--analytical-cpu-quota-us N --analytical-cpu-period-us N\n\
             --analytical-memory-max-bytes N --analytical-max-processes N]\n\
             --runtime-read-only PATH (repeatable)"
        );
    }

    fn run() -> Result<(), Box<dyn std::error::Error>> {
        let options = parse_options()?;
        let instance_dir = required(options.instance_dir, "--instance-dir")?;
        let socket_path = required(options.service_socket, "--service-socket")?;
        let gateway_uid = required(options.gateway_uid, "--gateway-uid")?;
        let cgroup_parent = required(options.cgroup_parent, "--cgroup-parent")?;
        let executor_path = required(options.executor, "--executor")?;
        let scratch_parent = required(options.scratch_parent, "--scratch-parent")?;
        let cpu_baseline = required(options.profile_cpu, "--cpu-baseline")?;
        let runtime_profile = required(options.profile_runtime, "--runtime-profile")?;
        let sdk_release = required(options.sdk_release, "--sdk-release")?;
        let retention_seconds = required(options.retention_seconds, "--retention-seconds")?;
        let retention_period_us = retention_seconds
            .checked_mul(1_000_000)
            .and_then(|value| i64::try_from(value).ok())
            .filter(|value| *value > 0)
            .ok_or("--retention-seconds must be positive and fit in signed microseconds")?;
        let max_rows = positive(required(options.max_rows, "--max-rows")?, "--max-rows")?;
        let max_bytes = positive(required(options.max_bytes, "--max-bytes")?, "--max-bytes")?;
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
        let analytical_values = [
            options.analytical_cpu_quota_us,
            options.analytical_cpu_period_us,
            options.analytical_memory_max_bytes,
            options.analytical_max_processes,
        ];
        let analytical_resource_limits =
            match analytical_values.iter().filter(|v| v.is_some()).count() {
                0 => None,
                4 => Some(WorkerResourceLimits {
                    cpu_quota_micros: positive(
                        options.analytical_cpu_quota_us.unwrap(),
                        "--analytical-cpu-quota-us",
                    )?,
                    cpu_period_micros: positive(
                        options.analytical_cpu_period_us.unwrap(),
                        "--analytical-cpu-period-us",
                    )?,
                    memory_max_bytes: positive(
                        options.analytical_memory_max_bytes.unwrap(),
                        "--analytical-memory-max-bytes",
                    )?,
                    max_processes: positive(
                        options.analytical_max_processes.unwrap(),
                        "--analytical-max-processes",
                    )?,
                }),
                _ => {
                    return Err(
                        "all four --analytical-* resource flags must be supplied together".into(),
                    );
                }
            };
        if options.runtime_read_execute.is_empty() {
            return Err("at least one --runtime-read-execute path is required".into());
        }
        let profile = ArtifactCompatibilityProfile::new(
            cpu_baseline,
            runtime_profile,
            sdk_release,
            options.dependencies,
        )
        .map_err(|error| format!("invalid compatibility profile: {error:?}"))?;
        let validator = Arc::new(VersionedSubmissionMetadataValidator::new(profile));
        let default_staging_limits = StagingLimits::default();
        let staging = StagingStore::new(
            StagingLimits {
                max_live_reservations_per_principal: positive(
                    options
                        .max_principal_reservations
                        .unwrap_or(default_staging_limits.max_live_reservations_per_principal),
                    "--max-principal-reservations",
                )?,
                max_active_operational: positive(
                    options
                        .max_active_operational
                        .unwrap_or(default_staging_limits.max_active_operational),
                    "--max-active-operational",
                )?,
                max_active_analytical: positive(
                    options
                        .max_active_analytical
                        .unwrap_or(default_staging_limits.max_active_analytical),
                    "--max-active-analytical",
                )?,
                max_active_operational_per_principal: positive(
                    options
                        .max_active_operational_per_principal
                        .unwrap_or(default_staging_limits.max_active_operational_per_principal),
                    "--max-active-operational-per-principal",
                )?,
                max_active_analytical_per_principal: positive(
                    options
                        .max_active_analytical_per_principal
                        .unwrap_or(default_staging_limits.max_active_analytical_per_principal),
                    "--max-active-analytical-per-principal",
                )?,
                ..default_staging_limits
            },
            validator,
        )?;
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
                analytical_resource_limits,
                launch: ExecutorLaunchConfig {
                    executor_path,
                    scratch_parent,
                    runtime_read_execute: options.runtime_read_execute,
                    runtime_read_only: options.runtime_read_only,
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
            Default::default(),
            &staging,
            runtime,
            Arc::clone(&shutdown),
            move || {
                let previous_identity = bind_socket_state
                    .lock()
                    .map_err(|_| std::io::Error::other("service socket identity lock poisoned"))?
                    .take();
                let listener = bind_private_socket(&bind_socket_path, previous_identity)?;
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

    fn bind_private_socket(
        path: &std::path::Path,
        previously_owned: Option<(u64, u64)>,
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
        bind_gateway_service_socket(path)
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
                "--cgroup-parent" => set!(cgroup_parent, PathBuf::from(value)),
                "--executor" => set!(executor, PathBuf::from(value)),
                "--scratch-parent" => set!(scratch_parent, PathBuf::from(value)),
                "--runtime-read-execute" => options.runtime_read_execute.push(value.into()),
                "--runtime-read-only" => options.runtime_read_only.push(value.into()),
                "--cpu-baseline" => set!(profile_cpu, value),
                "--runtime-profile" => set!(profile_runtime, value),
                "--sdk-release" => set!(sdk_release, value.parse::<u32>()?),
                "--allowed-dependency" => {
                    if !options.dependencies.insert(value) {
                        return Err("duplicate --allowed-dependency".into());
                    }
                }
                "--retention-seconds" => set!(retention_seconds, value.parse::<u64>()?),
                "--max-rows" => set!(max_rows, value.parse::<usize>()?),
                "--max-bytes" => set!(max_bytes, value.parse::<usize>()?),
                "--max-principal-reservations" => {
                    set!(max_principal_reservations, value.parse::<usize>()?)
                }
                "--max-active-operational" => {
                    set!(max_active_operational, value.parse::<usize>()?)
                }
                "--max-active-analytical" => {
                    set!(max_active_analytical, value.parse::<usize>()?)
                }
                "--max-active-operational-per-principal" => {
                    set!(
                        max_active_operational_per_principal,
                        value.parse::<usize>()?
                    )
                }
                "--max-active-analytical-per-principal" => {
                    set!(max_active_analytical_per_principal, value.parse::<usize>()?)
                }
                "--cpu-quota-us" => set!(cpu_quota_us, value.parse::<u64>()?),
                "--cpu-period-us" => set!(cpu_period_us, value.parse::<u64>()?),
                "--memory-max-bytes" => set!(memory_max_bytes, value.parse::<u64>()?),
                "--max-processes" => set!(max_processes, value.parse::<u64>()?),
                "--analytical-cpu-quota-us" => {
                    set!(analytical_cpu_quota_us, value.parse::<u64>()?)
                }
                "--analytical-cpu-period-us" => {
                    set!(analytical_cpu_period_us, value.parse::<u64>()?)
                }
                "--analytical-memory-max-bytes" => {
                    set!(analytical_memory_max_bytes, value.parse::<u64>()?)
                }
                "--analytical-max-processes" => {
                    set!(analytical_max_processes, value.parse::<u64>()?)
                }
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
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn main() {
    service::main();
}
