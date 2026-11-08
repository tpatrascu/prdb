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
fn supervisor_and_gateway_binaries_cover_submission_status_cancel_and_start_preflight() {
    use prdb_engine::bootstrap::initialize;
    use prdb_engine::client_protocol::{ClientFrame, ClientFrameDecoder};
    use prdb_engine::submission_manifest::{
        ArtifactManifestV1, decode_elf_needed_dependencies, encode_manifest_v1,
    };
    use sha2::{Digest, Sha256};
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpStream;
    use std::os::unix::fs::PermissionsExt;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
    use zeroize::Zeroize;

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
    let query_source = root.join("query.c");
    let query_artifact = root.join("query.so");
    std::fs::write(
        &query_source,
        "#include \"prdb_query_abi_v3.h\"\n\
         const prdb_query_descriptor_v3_record prdb_query_descriptor_v3 = {\n\
             sizeof(prdb_query_descriptor_v3_record), 3,\n\
             sizeof(prdb_host_api_v3), sizeof(prdb_invocation_v3)\n\
         };\n\
         int32_t prdb_query_main_v3(const prdb_host_api_v3 *host,\n\
             const prdb_invocation_v3 *invocation) {\n\
             (void)host; (void)invocation; return 0;\n\
         }\n",
    )
    .expect("write TinyCC query fixture");
    let include_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("include");
    let compile = Command::new("tcc")
        .arg("-shared")
        .arg("-I")
        .arg(include_dir)
        .arg(&query_source)
        .arg("-o")
        .arg(&query_artifact)
        .output()
        .expect("run TinyCC for the TCP query fixture");
    assert!(
        compile.status.success(),
        "TinyCC failed: {}",
        String::from_utf8_lossy(&compile.stderr)
    );
    let artifact = std::fs::read(&query_artifact).expect("read TinyCC query fixture");
    let dependencies = decode_elf_needed_dependencies(&artifact).expect("inspect fixture ELF");
    let manifest = encode_manifest_v1(&ArtifactManifestV1 {
        native_abi_version: 3,
        sdk_release: 1,
        cpu_baseline: "x86_64-v1".to_owned(),
        runtime_profile: "test-runtime".to_owned(),
        compiler_identity: "TinyCC integration fixture".to_owned(),
        descriptor_symbol: "prdb_query_descriptor_v3".to_owned(),
        entry_point_symbol: "prdb_query_main_v3".to_owned(),
        dependencies: dependencies.clone(),
    })
    .expect("encode fixture manifest");
    let instance_dir = root.join("instance");
    let mut setup =
        initialize(&instance_dir, "tcp_test_admin", i64::MAX).expect("initialize test instance");
    let token_id = setup.token_id();
    let mut bootstrap_secret = setup.take_secret().expect("take setup secret");
    drop(setup.acknowledge().expect("publish READY instance"));

    let service_socket = root.join("supervisor.sock");
    let uid = unsafe { libc::geteuid() };
    let mut supervisor_command = Command::new(env!("CARGO_BIN_EXE_prdb-supervisor"));
    supervisor_command.args([
        "--instance-dir",
        instance_dir.to_str().expect("UTF-8 instance path"),
        "--service-socket",
        service_socket.to_str().expect("UTF-8 socket path"),
        "--gateway-uid",
        &uid.to_string(),
        "--cgroup-parent",
        root.to_str().expect("UTF-8 invalid cgroup path"),
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
    ]);
    for dependency in &dependencies {
        supervisor_command
            .arg("--allowed-dependency")
            .arg(dependency);
    }
    let supervisor = supervisor_command
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
        let token_id_hex = token_id
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let mut token_secret = encode_base64(&bootstrap_secret);
        bootstrap_secret.zeroize();
        let mut auth_payload =
            format!("{{\"token_id\":\"{token_id_hex}\",\"token_secret\":\"{token_secret}\"}}");
        let auth = exchange(2, 2, auth_payload.as_bytes().to_vec());
        auth_payload.zeroize();
        assert_eq!(auth.kind, 100);
        assert!(String::from_utf8_lossy(&auth.payload).contains("\"authenticated\":true"));

        let scope = [b"PSC1\0\x01\x02\x02".as_slice(), &[1; 16]].concat();
        let artifact_sha256 = Sha256::digest(&artifact);
        let empty_sha256 = Sha256::digest([]);
        let prepare_payload = format!(
            "{{\"manifest_version\":1,\"manifest\":\"{}\",\"artifact_bytes\":\"{}\",\"artifact_sha256\":\"{}\",\"parameter_bytes\":\"0\",\"parameter_sha256\":\"{}\",\"scope_version\":1,\"requested_scope\":\"{}\",\"deadline_ms\":\"30000\",\"workload_class\":\"operational\"}}",
            encode_base64(&manifest),
            artifact.len(),
            encode_hex(&artifact_sha256),
            encode_hex(&empty_sha256),
            encode_base64(&scope),
        );
        let prepared = exchange(3, 3, prepare_payload.as_bytes().to_vec());
        assert_eq!(prepared.kind, 100);
        let prepared_json = String::from_utf8(prepared.payload).expect("prepared JSON");
        let invocation_id_hex = prepared_json
            .split("\"invocation_id\":\"")
            .nth(1)
            .and_then(|tail| tail.split('\"').next())
            .expect("prepared invocation id");
        let invocation_id = decode_hex_id(invocation_id_hex);

        let mut chunk = Vec::with_capacity(24 + artifact.len());
        chunk.extend(invocation_id);
        chunk.extend(0_u64.to_le_bytes());
        chunk.extend(&artifact);
        let uploaded = exchange(4, 4, chunk);
        assert_eq!(uploaded.kind, 100);
        assert_eq!(
            String::from_utf8_lossy(&uploaded.payload),
            format!("{{\"next_offset\":\"{}\"}}", artifact.len())
        );
        let seal_payload = format!("{{\"invocation_id\":\"{invocation_id_hex}\"}}");
        let sealed = exchange(6, 5, seal_payload.into_bytes());
        assert_eq!(sealed.kind, 100);
        assert!(String::from_utf8_lossy(&sealed.payload).contains("\"state\":\"prepared\""));

        let status_payload = format!("{{\"invocation_id\":\"{invocation_id_hex}\"}}");
        let status = exchange(8, 6, status_payload.as_bytes().to_vec());
        assert_eq!(status.kind, 100);
        assert_eq!(status.payload, br#"{"state":"prepared"}"#);

        let cancel = exchange(9, 7, status_payload.as_bytes().to_vec());
        assert_eq!(cancel.kind, 100);
        assert_eq!(cancel.payload, br#"{"state":"accepted"}"#);

        let cancelled_status = exchange(8, 8, status_payload.into_bytes());
        assert_eq!(cancelled_status.kind, 100);
        assert_eq!(cancelled_status.payload, br#"{"state":"unknown"}"#);

        let ping = exchange(11, 9, b"{}".to_vec());
        assert_eq!(ping.kind, 100);
        assert_eq!(ping.request_id, 9);
        assert_eq!(ping.payload, br#"{"alive":true}"#);

        let second_prepared = exchange(3, 10, prepare_payload.into_bytes());
        assert_eq!(second_prepared.kind, 100);
        let second_prepared_json =
            String::from_utf8(second_prepared.payload).expect("second prepared JSON");
        let second_invocation_hex = second_prepared_json
            .split("\"invocation_id\":\"")
            .nth(1)
            .and_then(|tail| tail.split('\"').next())
            .expect("second prepared invocation id");
        let second_invocation = decode_hex_id(second_invocation_hex);
        let mut second_chunk = Vec::with_capacity(24 + artifact.len());
        second_chunk.extend(second_invocation);
        second_chunk.extend(0_u64.to_le_bytes());
        second_chunk.extend(&artifact);
        let second_upload = exchange(4, 11, second_chunk);
        assert_eq!(second_upload.kind, 100);
        let second_seal_payload = format!("{{\"invocation_id\":\"{second_invocation_hex}\"}}");
        let second_sealed = exchange(6, 12, second_seal_payload.as_bytes().to_vec());
        assert_eq!(second_sealed.kind, 100);
        assert_eq!(second_sealed.payload, br#"{"state":"prepared"}"#);

        let start = exchange(7, 13, second_seal_payload.into_bytes());
        assert_eq!(start.kind, prdb_engine::client_protocol::CLIENT_ERROR_KIND);
        assert_eq!(start.request_id, 13);
        assert_eq!(
            start.payload,
            br#"{"code":"unavailable","message":"Requested resource is unavailable."}"#
        );
        let unknown = {
            let mut verification_client =
                TcpStream::connect(address).expect("reconnect after rejected START");
            verification_client
                .set_read_timeout(Some(Duration::from_secs(3)))
                .expect("set reconnected TCP client timeout");
            let mut exchange = |kind, request_id, payload: Vec<u8>| {
                let frame = ClientFrame {
                    kind,
                    request_id,
                    response_sequence: 0,
                    payload,
                }
                .encode()
                .expect("encode reconnect client frame");
                verification_client
                    .write_all(&frame)
                    .expect("write reconnect client frame");
                let mut decoder = ClientFrameDecoder::default();
                let mut bytes = [0u8; 1024];
                loop {
                    let count = verification_client
                        .read(&mut bytes)
                        .expect("read reconnect response");
                    assert_ne!(count, 0, "connection closed before reconnect response");
                    let mut frames = decoder
                        .push(&bytes[..count])
                        .expect("decode reconnect response");
                    if let Some(frame) = frames.pop() {
                        assert!(frames.is_empty(), "unexpected extra reconnect response");
                        return frame;
                    }
                }
            };
            let hello = exchange(
                1,
                1,
                br#"{"mode":"query","client_name":"start-retry"}"#.to_vec(),
            );
            assert_eq!(hello.kind, 100);
            let mut auth_payload =
                format!("{{\"token_id\":\"{token_id_hex}\",\"token_secret\":\"{token_secret}\"}}");
            let auth = exchange(2, 2, auth_payload.as_bytes().to_vec());
            auth_payload.zeroize();
            assert_eq!(auth.kind, 100);
            let status_payload = format!("{{\"invocation_id\":\"{second_invocation_hex}\"}}");
            exchange(8, 3, status_payload.into_bytes())
        };
        token_secret.zeroize();
        assert_eq!(unknown.kind, 100);
        assert_eq!(unknown.payload, br#"{"state":"unknown"}"#);
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

#[cfg(unix)]
fn encode_base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        let second = *chunk.get(1).unwrap_or(&0);
        let third = *chunk.get(2).unwrap_or(&0);
        output.push(TABLE[(first >> 2) as usize] as char);
        output.push(TABLE[(((first & 0x03) << 4) | (second >> 4)) as usize] as char);
        output.push(if chunk.len() > 1 {
            TABLE[(((second & 0x0f) << 2) | (third >> 6)) as usize] as char
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            TABLE[(third & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    output
}

#[cfg(unix)]
fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(unix)]
fn decode_hex_id(value: &str) -> [u8; 16] {
    let mut id = [0; 16];
    for (index, byte) in id.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .expect("valid response invocation id");
    }
    id
}
