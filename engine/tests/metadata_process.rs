#![cfg(all(unix, target_os = "linux", target_arch = "x86_64"))]

use prdb_engine::bootstrap::initialize;
use prdb_engine::client_submit::MetadataClient;
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
    let mut token_secret = setup.take_secret().ok_or("bootstrap secret missing")?;
    drop(setup.acknowledge()?);

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

    assert!(gateway.stop()?.success());
    assert!(supervisor.stop()?.success());
    Ok(())
}

struct DirectoryGuard(std::path::PathBuf);

impl Drop for DirectoryGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
