#![cfg(all(unix, target_os = "linux", target_arch = "x86_64"))]

use prdb_engine::audit::{AuditAction, AuditEvent, AuditResult};
use prdb_engine::bootstrap::initialize;
use prdb_engine::client_submit::{InvocationStatus, MetadataClient, SubmissionClient};
use prdb_engine::storage_manager::{CreateTableRequest, TableColumnDefinition, TokenIssueRequest};
use prdb_engine::submission_manifest::{encode_manifest_v1, manifest_v1_from_elf};
use prdb_engine::supervisor::WorkloadClass;
use prdb_engine::tuple::ScalarType;
use sha2::{Digest, Sha256};
use std::io::{BufRead, BufReader};
use std::os::unix::fs::PermissionsExt;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct ChildGuard(Option<Child>);

impl ChildGuard {
    fn child(&mut self) -> &mut Child {
        self.0.as_mut().expect("child process still running")
    }

    fn stop(&mut self) -> std::io::Result<std::process::ExitStatus> {
        let mut child = self.0.take().expect("child process still running");
        // SAFETY: the PID belongs to the child process held by this guard.
        unsafe {
            libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = child.try_wait()? {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                return child.wait();
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn kill(&mut self) -> std::io::Result<std::process::ExitStatus> {
        let mut child = self.0.take().expect("child process still running");
        let _ = child.kill();
        child.wait()
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[test]
fn authenticated_metadata_reads_cross_gateway_and_supervisor_processes()
-> Result<(), Box<dyn std::error::Error>> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root = std::env::temp_dir().join(format!(
        "prdb-metadata-process-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&root)?;
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
    let _cleanup = DirectoryGuard(root.clone());

    let instance_dir = root.join("instance");
    let mut setup = initialize(&instance_dir, "metadata_process_admin", i64::MAX)?;
    let token_id = setup.token_id();
    let admin_user_id = setup.admin_user_id();
    let mut token_secret = setup.take_secret().ok_or("bootstrap secret missing")?;
    let committed_invocation_id = [0xa3; 16];
    let restricted_token;
    let mut restricted_secret;
    let restricted_schema_id;
    let restricted_table_id;
    {
        let instance = setup.acknowledge()?;
        let storage = instance.storage_manager(Default::default())?;
        let now_us = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
        let retention_deadline_us = now_us.saturating_add(3_600_000_000);

        let restricted_schema = storage.create_schema(
            [0xa4; 16],
            admin_user_id,
            "admin_private",
            admin_user_id,
            retention_deadline_us,
            AuditEvent {
                event_id: [0xa5; 16],
                actor_id: Some(admin_user_id),
                correlation_id: [0xa4; 16],
                occurred_at_us: now_us,
                action: AuditAction::Ddl,
                result: AuditResult::Succeeded,
                target_ids: Vec::new(),
            },
        )?;
        restricted_schema_id = restricted_schema.schema_id;
        let private_table = storage.create_table(CreateTableRequest {
            invocation_id: [0xaa; 16],
            authenticated_actor_id: admin_user_id,
            schema_id: restricted_schema_id,
            name: "private_records".to_owned(),
            columns: vec![TableColumnDefinition {
                name: "payload".to_owned(),
                scalar_type: ScalarType::Text,
                nullable: false,
                generated_uuid_identity: false,
                default: None,
            }],
            retention_deadline_us,
            audit_event: AuditEvent {
                event_id: [0xab; 16],
                actor_id: Some(admin_user_id),
                correlation_id: [0xaa; 16],
                occurred_at_us: now_us,
                action: AuditAction::Ddl,
                result: AuditResult::Succeeded,
                target_ids: Vec::new(),
            },
        })?;
        restricted_table_id = private_table.table_id;
        let restricted_user = storage.create_user(
            [0xa6; 16],
            admin_user_id,
            "metadata_reader",
            retention_deadline_us,
            AuditEvent {
                event_id: [0xa7; 16],
                actor_id: Some(admin_user_id),
                correlation_id: [0xa6; 16],
                occurred_at_us: now_us,
                action: AuditAction::Ddl,
                result: AuditResult::Succeeded,
                target_ids: Vec::new(),
            },
        )?;
        let mut issued = storage.issue_token(TokenIssueRequest {
            invocation_id: [0xa8; 16],
            authenticated_actor_id: admin_user_id,
            user_id: restricted_user.user_id,
            label: "metadata reader",
            expires_at_us: now_us.saturating_add(1_800_000_000),
            retention_deadline_us,
            audit_event: AuditEvent {
                event_id: [0xa9; 16],
                actor_id: Some(admin_user_id),
                correlation_id: [0xa8; 16],
                occurred_at_us: now_us,
                action: AuditAction::TokenLifecycle,
                result: AuditResult::Succeeded,
                target_ids: vec![restricted_user.user_id],
            },
        })?;
        restricted_token = issued.token_id;
        restricted_secret = issued
            .take_secret()
            .ok_or("restricted metadata token secret missing")?;

        let mut invocation = storage.admit_supervisor_invocation(
            committed_invocation_id,
            token_id,
            &token_secret,
            retention_deadline_us,
            8,
            4096,
        )?;
        let mut successful_worker = Command::new("true").spawn()?;
        invocation.observe_worker_exit(&mut successful_worker)?;

        let (mut terminal_writer, terminal_reader) = std::os::unix::net::UnixStream::pair()?;
        drop(terminal_reader);
        assert!(
            prdb_engine::supervisor_ipc::finalize_gateway_invocation_and_send_terminal(
                &mut invocation,
                &mut terminal_writer,
                1,
                prdb_engine::supervisor_ipc::GatewayTerminalDecision::Commit,
            )
            .is_err(),
            "closed terminal receiver must lose delivery after storage commits"
        );
        assert!(matches!(
            storage
                .get_invocation_status(
                    committed_invocation_id,
                    invocation.principal_id(),
                    now_us,
                )?
                .map(|snapshot| snapshot.status),
            Some(prdb_engine::storage_manager::InvocationStatus::Terminal(outcome))
                if outcome.status == prdb_engine::outcome::OutcomeStatus::Committed
        ));
    }
    let mut outcome_status_secret = token_secret;

    let socket_path = root.join("supervisor.sock");
    let uid = unsafe { libc::geteuid() };
    let mut supervisor_command = Command::new(env!("CARGO_BIN_EXE_prdb-supervisor"));
    supervisor_command.args([
        "--instance-dir",
        instance_dir.to_str().ok_or("non-UTF-8 instance path")?,
        "--service-socket",
        socket_path.to_str().ok_or("non-UTF-8 socket path")?,
        "--gateway-uid",
        &uid.to_string(),
        "--cgroup-parent",
        root.to_str().ok_or("non-UTF-8 cgroup path")?,
        "--executor",
        env!("CARGO_BIN_EXE_prdb-executor"),
        "--scratch-parent",
        root.to_str().ok_or("non-UTF-8 scratch path")?,
        "--runtime-read-execute",
        "/usr",
        "--runtime-read-only",
        "/etc/ld.so.cache",
        "--cpu-baseline",
        "x86_64-v1",
        "--runtime-profile",
        "test-runtime",
        "--sdk-release",
        "1",
        "--retention-seconds",
        "3600",
        "--max-rows",
        "8",
        "--max-bytes",
        "4096",
        "--cpu-quota-us",
        "100000",
        "--cpu-period-us",
        "100000",
        "--memory-max-bytes",
        "67108864",
        "--max-processes",
        "16",
    ]);
    let mut supervisor = ChildGuard(Some(
        supervisor_command
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?,
    ));

    let socket_deadline = Instant::now() + Duration::from_secs(5);
    while !socket_path.exists() {
        if let Some(status) = supervisor.child().try_wait()? {
            return Err(format!("supervisor exited before binding: {status}").into());
        }
        if Instant::now() >= socket_deadline {
            return Err("supervisor did not bind its service socket".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    let mut gateway = ChildGuard(Some(
        Command::new(env!("CARGO_BIN_EXE_prdb-gateway"))
            .args([
                "--bind",
                "127.0.0.1:0",
                "--service-socket",
                socket_path.to_str().ok_or("non-UTF-8 socket path")?,
                "--service-uid",
                &uid.to_string(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?,
    ));
    let stderr = gateway
        .child()
        .stderr
        .take()
        .ok_or("gateway stderr missing")?;
    let (address_sender, address_receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut line = String::new();
        let result = BufReader::new(stderr).read_line(&mut line).map(|_| line);
        let _ = address_sender.send(result);
    });
    let address_line = address_receiver.recv_timeout(Duration::from_secs(5))??;
    let address: std::net::SocketAddr = address_line
        .trim()
        .strip_prefix("prdb-gateway listening on ")
        .ok_or("gateway listen line had an unexpected prefix")?
        .parse()?;

    let mut metadata = MetadataClient::connect_authenticated(address, token_id, &mut token_secret)?;
    assert_eq!(token_secret, [0; 32]);
    let schemas = metadata.list_schemas()?;
    assert!(schemas.iter().any(|schema| {
        schema.schema_id == prdb_engine::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID
            && schema.name == "information_schema"
    }));
    let relations =
        metadata.list_relations(prdb_engine::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID)?;
    assert_eq!(relations.len(), 3);
    let columns = metadata.list_columns(
        prdb_engine::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID,
        relations
            .iter()
            .find(|relation| relation.name == "schemata")
            .ok_or("information_schema.schemata was missing")?
            .relation_id,
    )?;
    assert!(!columns.is_empty());
    drop(metadata);

    let mut restricted_metadata =
        MetadataClient::connect_authenticated(address, restricted_token, &mut restricted_secret)?;
    let restricted_schemas = restricted_metadata.list_schemas()?;
    assert!(restricted_schemas.iter().any(|schema| {
        schema.schema_id == prdb_engine::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID
    }));
    assert!(
        !restricted_schemas
            .iter()
            .any(|schema| schema.name == "admin_private")
    );
    assert!(
        restricted_metadata
            .list_relations(prdb_engine::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID)?
            .iter()
            .any(|relation| relation.name == "schemata")
    );
    assert!(
        restricted_metadata
            .list_relations(restricted_schema_id)?
            .is_empty()
    );
    assert!(
        restricted_metadata
            .list_columns(restricted_schema_id, restricted_table_id)?
            .is_empty()
    );
    drop(restricted_metadata);

    let mut outcome_status_client =
        SubmissionClient::connect_authenticated(address, token_id, &mut outcome_status_secret)?;
    assert_eq!(
        outcome_status_client.status(committed_invocation_id)?,
        InvocationStatus::Committed,
        "the supervisor must recover a committed outcome after storage and terminal delivery end"
    );
    drop(outcome_status_client);

    assert!(gateway.stop()?.success());
    assert!(supervisor.stop()?.success());
    Ok(())
}

#[test]
#[ignore = "requires a delegated cgroup and configured sandbox runtime paths"]
fn disconnected_submission_is_reconciled_after_gateway_restart()
-> Result<(), Box<dyn std::error::Error>> {
    fn required_paths(name: &str) -> Result<Vec<std::path::PathBuf>, Box<dyn std::error::Error>> {
        let value = std::env::var_os(name).ok_or(format!("{name} is required"))?;
        let paths = std::env::split_paths(&value).collect::<Vec<_>>();
        if paths.is_empty() || paths.iter().any(|path| !path.is_absolute()) {
            return Err(format!("{name} must contain absolute paths").into());
        }
        Ok(paths)
    }

    fn start_gateway(
        service_socket: &std::path::Path,
        uid: u32,
    ) -> Result<(ChildGuard, std::net::SocketAddr), Box<dyn std::error::Error>> {
        let mut gateway = ChildGuard(Some(
            Command::new(env!("CARGO_BIN_EXE_prdb-gateway"))
                .args([
                    "--bind",
                    "127.0.0.1:0",
                    "--service-socket",
                    service_socket.to_str().ok_or("non-UTF-8 socket path")?,
                    "--service-uid",
                    &uid.to_string(),
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()?,
        ));
        let stderr = gateway
            .child()
            .stderr
            .take()
            .ok_or("gateway stderr missing")?;
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut line = String::new();
            let result = BufReader::new(stderr).read_line(&mut line).map(|_| line);
            let _ = sender.send(result);
        });
        let line = receiver.recv_timeout(Duration::from_secs(5))??;
        let address = line
            .trim()
            .strip_prefix("prdb-gateway listening on ")
            .ok_or("gateway listen line had an unexpected prefix")?
            .parse()?;
        Ok((gateway, address))
    }

    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root = std::env::temp_dir().join(format!(
        "prdb-outcome-process-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&root)?;
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
    let _cleanup = DirectoryGuard(root.clone());

    let cgroup_parent = std::env::var_os("PRDB_TEST_CGROUP_PARENT")
        .map(std::path::PathBuf::from)
        .ok_or("PRDB_TEST_CGROUP_PARENT must name a delegated cgroup v2 parent")?;
    if !cgroup_parent.is_absolute() {
        return Err("PRDB_TEST_CGROUP_PARENT must be absolute".into());
    }
    let runtime_read_execute = required_paths("PRDB_TEST_RUNTIME_READ_EXECUTE")?;
    let runtime_read_only = required_paths("PRDB_TEST_RUNTIME_READ_ONLY")?;

    let source = root.join("query.c");
    let artifact_path = root.join("query.so");
    std::fs::write(
        &source,
        "#include \"prdb_query_abi_v3.h\"\n\
         const prdb_query_descriptor_v3_record prdb_query_descriptor_v3 = {\n\
             sizeof(prdb_query_descriptor_v3_record), 3,\n\
             sizeof(prdb_host_api_v3), sizeof(prdb_invocation_v3)\n\
         };\n\
         int32_t prdb_query_main_v3(const prdb_host_api_v3 *host,\n\
             const prdb_invocation_v3 *invocation) {\n\
             (void)host; (void)invocation; return 0;\n\
         }\n",
    )?;
    let include_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("include");
    let compile = Command::new("tcc")
        .arg("-shared")
        .arg("-I")
        .arg(include_dir)
        .arg(&source)
        .arg("-o")
        .arg(&artifact_path)
        .output()?;
    if !compile.status.success() {
        return Err(format!(
            "TinyCC failed: {}",
            String::from_utf8_lossy(&compile.stderr)
        )
        .into());
    }
    let artifact = std::fs::read(&artifact_path)?;
    let manifest_value = manifest_v1_from_elf(
        &artifact,
        3,
        1,
        "x86_64-v1".to_owned(),
        "test-runtime".to_owned(),
        "TinyCC process integration fixture".to_owned(),
    )
    .map_err(|error| format!("derive TinyCC fixture manifest: {error:?}"))?;
    let manifest = encode_manifest_v1(&manifest_value)
        .map_err(|error| format!("encode TinyCC fixture manifest: {error:?}"))?;
    let dependencies = manifest_value.dependencies.clone();
    let package = root.join("package");
    std::fs::create_dir(&package)?;
    std::fs::set_permissions(&package, std::fs::Permissions::from_mode(0o700))?;
    let mut requested_scope = b"PSC1\0\x01\x01\x01".to_vec();
    requested_scope.extend_from_slice(&[1; 16]);
    for (name, bytes) in [
        ("query.so", artifact.as_slice()),
        ("manifest.pmf1", manifest.as_slice()),
        // One canonical schema-USAGE capability; the query performs no data access.
        ("requested_scope.psc1", requested_scope.as_slice()),
    ] {
        std::fs::write(package.join(name), bytes)?;
        std::fs::set_permissions(package.join(name), std::fs::Permissions::from_mode(0o600))?;
    }
    let artifact_hash = Sha256::digest(&artifact);
    let hash_sidecar = format!(
        "{}\n",
        artifact_hash
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
    std::fs::write(package.join("artifact.sha256"), hash_sidecar)?;
    std::fs::set_permissions(
        package.join("artifact.sha256"),
        std::fs::Permissions::from_mode(0o600),
    )?;

    let instance_dir = root.join("instance");
    let mut setup = initialize(&instance_dir, "outcome_process_admin", i64::MAX)?;
    let token_id = setup.token_id();
    let mut token_secret = setup.take_secret().ok_or("bootstrap secret missing")?;
    drop(setup.acknowledge()?);

    let service_socket = root.join("supervisor.sock");
    let uid = unsafe { libc::geteuid() };
    let uid_string = uid.to_string();
    let cgroup_string = cgroup_parent.to_str().ok_or("non-UTF-8 cgroup path")?;
    let mut supervisor_command = Command::new(env!("CARGO_BIN_EXE_prdb-supervisor"));
    supervisor_command.args([
        "--instance-dir",
        instance_dir.to_str().ok_or("non-UTF-8 instance path")?,
        "--service-socket",
        service_socket.to_str().ok_or("non-UTF-8 socket path")?,
        "--gateway-uid",
        &uid_string,
        "--cgroup-parent",
        cgroup_string,
        "--executor",
        env!("CARGO_BIN_EXE_prdb-executor"),
        "--scratch-parent",
        root.to_str().ok_or("non-UTF-8 scratch path")?,
        "--cpu-baseline",
        "x86_64-v1",
        "--runtime-profile",
        "test-runtime",
        "--sdk-release",
        "1",
        "--retention-seconds",
        "3600",
        "--max-rows",
        "8",
        "--max-bytes",
        "4096",
        "--cpu-quota-us",
        "100000",
        "--cpu-period-us",
        "100000",
        "--memory-max-bytes",
        "67108864",
        "--max-processes",
        "16",
    ]);
    for path in runtime_read_execute {
        supervisor_command.arg("--runtime-read-execute").arg(path);
    }
    for path in runtime_read_only {
        supervisor_command.arg("--runtime-read-only").arg(path);
    }
    for dependency in dependencies {
        supervisor_command
            .arg("--allowed-dependency")
            .arg(dependency);
    }
    let mut supervisor = ChildGuard(Some(
        supervisor_command
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?,
    ));
    let deadline = Instant::now() + Duration::from_secs(5);
    while !service_socket.exists() {
        if let Some(status) = supervisor.child().try_wait()? {
            return Err(format!("supervisor exited before binding: {status}").into());
        }
        if Instant::now() >= deadline {
            return Err("supervisor did not bind its service socket".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    let (mut gateway, address) = start_gateway(&service_socket, uid)?;
    let mut status_secret = token_secret;
    let mut client = SubmissionClient::connect_authenticated(address, token_id, &mut token_secret)?;
    let invocation_id =
        client.upload_and_seal(&package, &[], 30_000, WorkloadClass::Operational)?;
    let receipt = root.join("start.receipt");
    {
        let _started = client.start(invocation_id, &receipt)?;
        let _gateway_crash_status = gateway.kill()?;
    }
    drop(client);

    let (mut replacement_gateway, replacement_address) = start_gateway(&service_socket, uid)?;
    let mut restart_secret = status_secret;
    let mut status_client =
        SubmissionClient::connect_authenticated(replacement_address, token_id, &mut status_secret)?;
    let status_deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        let status = status_client.status(invocation_id)?;
        if matches!(
            status,
            InvocationStatus::Committed | InvocationStatus::Aborted
        ) {
            break status;
        }
        if Instant::now() >= status_deadline {
            return Err(
                format!("invocation did not reach a durable terminal status: {status:?}").into(),
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(status, InvocationStatus::Aborted);
    drop(status_client);
    assert!(replacement_gateway.stop()?.success());
    assert!(supervisor.stop()?.success());
    assert!(!service_socket.exists(), "supervisor left a stale socket");

    let mut restarted_supervisor_command = Command::new(env!("CARGO_BIN_EXE_prdb-supervisor"));
    restarted_supervisor_command.args([
        "--instance-dir",
        instance_dir.to_str().ok_or("non-UTF-8 instance path")?,
        "--service-socket",
        service_socket.to_str().ok_or("non-UTF-8 socket path")?,
        "--gateway-uid",
        &uid_string,
        "--cgroup-parent",
        cgroup_string,
        "--executor",
        env!("CARGO_BIN_EXE_prdb-executor"),
        "--scratch-parent",
        root.to_str().ok_or("non-UTF-8 scratch path")?,
        "--cpu-baseline",
        "x86_64-v1",
        "--runtime-profile",
        "test-runtime",
        "--sdk-release",
        "1",
        "--retention-seconds",
        "3600",
        "--max-rows",
        "8",
        "--max-bytes",
        "4096",
        "--cpu-quota-us",
        "100000",
        "--cpu-period-us",
        "100000",
        "--memory-max-bytes",
        "67108864",
        "--max-processes",
        "16",
    ]);
    for path in required_paths("PRDB_TEST_RUNTIME_READ_EXECUTE")? {
        restarted_supervisor_command
            .arg("--runtime-read-execute")
            .arg(path);
    }
    for path in required_paths("PRDB_TEST_RUNTIME_READ_ONLY")? {
        restarted_supervisor_command
            .arg("--runtime-read-only")
            .arg(path);
    }
    for dependency in &manifest_value.dependencies {
        restarted_supervisor_command
            .arg("--allowed-dependency")
            .arg(dependency);
    }
    let mut restarted_supervisor = ChildGuard(Some(
        restarted_supervisor_command
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?,
    ));
    let restart_deadline = Instant::now() + Duration::from_secs(5);
    while !service_socket.exists() {
        if let Some(status) = restarted_supervisor.child().try_wait()? {
            return Err(format!("restarted supervisor exited before binding: {status}").into());
        }
        if Instant::now() >= restart_deadline {
            return Err("restarted supervisor did not bind its service socket".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let (mut recovered_gateway, recovered_address) = start_gateway(&service_socket, uid)?;
    let mut recovered_status_client =
        SubmissionClient::connect_authenticated(recovered_address, token_id, &mut restart_secret)?;
    assert_eq!(
        recovered_status_client.status(invocation_id)?,
        InvocationStatus::Aborted,
        "ABORTED must survive closing and reopening the storage process"
    );
    drop(recovered_status_client);
    assert!(recovered_gateway.stop()?.success());
    assert!(restarted_supervisor.stop()?.success());
    Ok(())
}

struct DirectoryGuard(std::path::PathBuf);

impl Drop for DirectoryGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
