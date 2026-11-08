use std::process::Command;

#[test]
fn supervisor_help_documents_explicit_runtime_and_compatibility_configuration() {
    let output = Command::new(env!("CARGO_BIN_EXE_prdb-supervisor"))
        .arg("--help")
        .output()
        .expect("run supervisor help");
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).expect("UTF-8 help");
    for option in [
        "--instance-dir",
        "--service-socket",
        "--cgroup-parent",
        "--executor",
        "--runtime-read-execute",
        "--cpu-baseline",
        "--runtime-profile",
        "--sdk-release",
        "--allowed-dependency",
    ] {
        assert!(help.contains(option), "missing {option} from help");
    }
}

#[test]
fn supervisor_refuses_to_start_without_explicit_instance_and_sandbox_profile() {
    let output = Command::new(env!("CARGO_BIN_EXE_prdb-supervisor"))
        .output()
        .expect("run supervisor without configuration");
    assert!(!output.status.success());
    let error = String::from_utf8(output.stderr).expect("UTF-8 error");
    assert!(error.contains("--instance-dir is required"));
}

#[cfg(unix)]
struct ChildGuard(Option<std::process::Child>);

#[cfg(unix)]
impl ChildGuard {
    fn child(&mut self) -> &mut std::process::Child {
        self.0.as_mut().expect("child process still running")
    }

    fn stop(&mut self) -> std::io::Result<std::process::ExitStatus> {
        let mut child = self.0.take().expect("child process still running");
        // SAFETY: the PID belongs to the child process held by this guard.
        unsafe {
            libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if let Some(status) = child.try_wait()? {
                return Ok(status);
            }
            if std::time::Instant::now() >= deadline {
                let _ = child.kill();
                return child.wait();
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}

#[cfg(unix)]
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[cfg(unix)]
#[test]
fn supervisor_and_gateway_binaries_exchange_real_tcp_hello_and_ping() {
    use prdb_engine::bootstrap::initialize;
    use prdb_engine::client_protocol::{ClientFrame, ClientFrameDecoder};
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpStream;
    use std::os::unix::fs::PermissionsExt;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "prdb-supervisor-tcp-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&root).expect("create test root");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("protect test root");
    let instance_dir = root.join("instance");
    let mut setup =
        initialize(&instance_dir, "tcp_test_admin", i64::MAX).expect("initialize test instance");
    let mut bootstrap_secret = setup.take_secret().expect("take setup secret");
    bootstrap_secret.fill(0);
    drop(setup.acknowledge().expect("publish READY instance"));

    let service_socket = root.join("supervisor.sock");
    let uid = unsafe { libc::geteuid() };
    let supervisor = Command::new(env!("CARGO_BIN_EXE_prdb-supervisor"))
        .args([
            "--instance-dir",
            instance_dir.to_str().expect("UTF-8 instance path"),
            "--service-socket",
            service_socket.to_str().expect("UTF-8 socket path"),
            "--gateway-uid",
            &uid.to_string(),
            "--cgroup-parent",
            "/sys/fs/cgroup",
            "--executor",
            env!("CARGO_BIN_EXE_prdb-executor"),
            "--scratch-parent",
            root.to_str().expect("UTF-8 scratch path"),
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
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("start supervisor binary");
    let mut supervisor = ChildGuard(Some(supervisor));

    let deadline = Instant::now() + Duration::from_secs(5);
    while !service_socket.exists() {
        if let Some(status) = supervisor.child().try_wait().expect("poll supervisor") {
            panic!("supervisor exited before binding its socket: {status}");
        }
        assert!(
            Instant::now() < deadline,
            "supervisor did not bind its socket"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    let gateway = Command::new(env!("CARGO_BIN_EXE_prdb-gateway"))
        .args([
            "--bind",
            "127.0.0.1:0",
            "--service-socket",
            service_socket.to_str().expect("UTF-8 socket path"),
            "--service-uid",
            &uid.to_string(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start gateway binary");
    let mut gateway = ChildGuard(Some(gateway));
    let stderr = gateway.child().stderr.take().expect("gateway stderr");
    let (address_sender, address_receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut line = String::new();
        let result = BufReader::new(stderr).read_line(&mut line).map(|_| line);
        let _ = address_sender.send(result);
    });
    let address_line = address_receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("gateway listen line")
        .expect("read gateway listen line");
    let address: std::net::SocketAddr = address_line
        .trim()
        .strip_prefix("prdb-gateway listening on ")
        .expect("gateway listen address prefix")
        .parse()
        .expect("gateway listen address");

    let mut client = TcpStream::connect(address).expect("connect external TCP client");
    client
        .set_read_timeout(Some(Duration::from_secs(3)))
        .expect("set TCP client timeout");
    {
        let mut exchange = |kind, request_id, payload: Vec<u8>| {
            let frame = ClientFrame {
                kind,
                request_id,
                response_sequence: 0,
                payload,
            }
            .encode()
            .expect("encode client frame");
            client.write_all(&frame).expect("write client frame");
            let mut decoder = ClientFrameDecoder::default();
            let mut bytes = [0u8; 1024];
            loop {
                let count = client.read(&mut bytes).expect("read client response");
                assert_ne!(count, 0, "connection closed before a response");
                let mut frames = decoder.push(&bytes[..count]).expect("decode client frame");
                if let Some(frame) = frames.pop() {
                    assert!(frames.is_empty(), "unexpected coalesced extra response");
                    return frame;
                }
            }
        };
        let hello = exchange(
            1,
            1,
            br#"{"mode":"query","client_name":"supervisor-e2e"}"#.to_vec(),
        );
        assert_eq!(hello.kind, 100);
        assert_eq!(hello.request_id, 1);
        let expected_hello = format!(
            "{{\"major\":1,\"minor\":0,\"mode\":\"query\",\"limits\":{{\"payload_bytes\":{},\"control_bytes\":{},\"chunk_bytes\":{}}},\"transport\":\"plaintext_tcp\"}}",
            prdb_engine::client_protocol::CLIENT_MAX_PAYLOAD_BYTES,
            prdb_engine::client_protocol::CLIENT_MAX_CONTROL_BYTES,
            prdb_engine::client_protocol::CLIENT_MAX_CHUNK_BYTES,
        );
        assert_eq!(hello.payload, expected_hello.as_bytes());
        let ping = exchange(11, 2, b"{}".to_vec());
        assert_eq!(ping.kind, 100);
        assert_eq!(ping.request_id, 2);
        assert_eq!(ping.payload, br#"{"alive":true}"#);
    }
    client
        .shutdown(std::net::Shutdown::Write)
        .expect("close TCP client write side");
    drop(client);

    let gateway_status = gateway.stop().expect("stop gateway");
    let supervisor_status = supervisor.stop().expect("stop supervisor");
    assert!(
        gateway_status.success(),
        "gateway exited with {gateway_status}"
    );
    assert!(
        supervisor_status.success(),
        "supervisor exited with {supervisor_status}"
    );
    assert!(!service_socket.exists(), "supervisor left a stale socket");
    std::fs::remove_dir_all(root).expect("remove test files");
}
