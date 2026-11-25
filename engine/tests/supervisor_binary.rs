use std::io::{Read, Write};
use std::process::Command;

#[cfg(unix)]
fn remove_test_tree(path: &std::path::Path) -> std::io::Result<()> {
    fn restore_directory_permissions(path: &std::path::Path) -> std::io::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        if !path.is_dir() {
            return Ok(());
        }
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
        for child in std::fs::read_dir(path)? {
            restore_directory_permissions(&child?.path())?;
        }
        Ok(())
    }

    restore_directory_permissions(path)?;
    std::fs::remove_dir_all(path)
}

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
        "--service-socket-mode",
        "--cgroup-parent",
        "--executor",
        "--runtime-read-execute",
        "--cpu-baseline",
        "--runtime-profile",
        "--sdk-repository-dir",
        "--sdk-repository-max-bytes",
        "--allowed-dependency",
        "--max-active-operational",
        "--max-active-analytical",
        "--max-principal-reservations",
        "--max-pending-operational",
        "--max-pending-analytical",
        "--max-active-operational-per-principal",
        "--max-active-analytical-per-principal",
        "--max-storage-active-snapshots",
        "--max-storage-active-analytical-snapshots",
    ] {
        assert!(help.contains(option), "missing {option} from help");
    }
    for retired_option in [
        "--sdk-release",
        "--sdk-release-dir",
        "--sdk-cache-dir",
        "--sdk-cache-max-bytes",
        "--allow-legacy-static-sdk-release",
    ] {
        assert!(
            !help.contains(retired_option),
            "retired option {retired_option} remains in help"
        );
        let output = Command::new(env!("CARGO_BIN_EXE_prdb-supervisor"))
            .arg(retired_option)
            .arg("/tmp/retired-option")
            .output()
            .expect("run supervisor with retired SDK option");
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("unknown option"));
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

#[cfg(target_os = "linux")]
#[test]
fn supervisor_rejects_configured_memory_backed_spill_at_startup() {
    use std::os::unix::fs::PermissionsExt;

    let spill_parent = std::path::PathBuf::from(format!(
        "/dev/shm/prdb-supervisor-spill-{}",
        std::process::id()
    ));
    std::fs::create_dir(&spill_parent).expect("create tmpfs spill parent");
    std::fs::set_permissions(&spill_parent, std::fs::Permissions::from_mode(0o700))
        .expect("restrict tmpfs spill parent");
    let output = Command::new(env!("CARGO_BIN_EXE_prdb-supervisor"))
        .args([
            "--spill-parent",
            spill_parent.to_str().expect("UTF-8 tmpfs path"),
            "--spill-max-bytes",
            "4096",
            "--spill-max-io-bytes",
            "8192",
            "--spill-max-streams",
            "4",
            "--spill-max-operations",
            "32",
            "--spill-max-response-bytes",
            "1024",
            "--spill-max-service-bytes",
            "8192",
        ])
        .output()
        .expect("run supervisor with memory-backed spill configuration");
    let cleanup = std::fs::remove_dir(&spill_parent);
    assert!(
        cleanup.is_ok(),
        "remove temporary tmpfs spill parent: {cleanup:?}"
    );
    assert!(!output.status.success());
    let error = String::from_utf8(output.stderr).expect("UTF-8 supervisor error");
    assert!(
        error.contains("secure spill configuration rejected: UnsupportedFilesystem"),
        "unexpected supervisor error: {error}"
    );
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
fn next_client_response(
    client: &mut std::net::TcpStream,
    decoder: &mut prdb_engine::client_protocol::ClientFrameDecoder,
    buffered: &mut std::collections::VecDeque<prdb_engine::client_protocol::ClientFrame>,
) -> prdb_engine::client_protocol::ClientFrame {
    loop {
        if let Some(frame) = buffered.pop_front() {
            return frame;
        }
        let mut bytes = [0u8; 4096];
        let count = client.read(&mut bytes).expect("read client response");
        assert_ne!(count, 0, "connection closed before a response");
        buffered.extend(decoder.push(&bytes[..count]).expect("decode client frame"));
    }
}

#[cfg(unix)]
fn exchange_client_request(
    client: &mut std::net::TcpStream,
    decoder: &mut prdb_engine::client_protocol::ClientFrameDecoder,
    buffered: &mut std::collections::VecDeque<prdb_engine::client_protocol::ClientFrame>,
    kind: u16,
    request_id: u64,
    payload: Vec<u8>,
) -> prdb_engine::client_protocol::ClientFrame {
    let encoded = prdb_engine::client_protocol::ClientFrame {
        kind,
        request_id,
        response_sequence: 0,
        payload,
    }
    .encode()
    .expect("encode client frame");
    client.write_all(&encoded).expect("write client frame");
    next_client_response(client, decoder, buffered)
}

#[cfg(unix)]
#[test]
fn supervisor_and_gateway_binaries_commit_a_confined_submission() {
    use prdb_engine::bootstrap::initialize;
    use prdb_engine::client_protocol::{ClientFrameDecoder, ClientResponseSequences};
    use prdb_engine::client_submit::{
        AdminClient, AdminColumnDefault, AdminColumnDefinition, AdminObjectGrantTarget,
        AdminObjectPrivilege, AdminSystemPrivilege, InvocationStatus,
    };
    use prdb_engine::submission_manifest::{encode_manifest_v1, manifest_v1_from_elf};
    use sha2::{Digest, Sha256};
    use std::io::{BufRead, BufReader};
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
    let query_object = root.join("query.o");
    let query_artifact = root.join("query.so");
    let sdk_release_dir = std::env::var_os("PRDB_TEST_INSTALLED_SDK_RELEASE_DIR")
        .map(std::path::PathBuf::from)
        .expect("installed SDK release fixture for repository startup test");
    assert!(sdk_release_dir.is_absolute());
    std::fs::write(
        &query_source,
        "#include \"prdb_query_abi_v1.h\"\n\
         const prdb_query_descriptor_v1_record prdb_query_descriptor_v1 = {\n\
             sizeof(prdb_query_descriptor_v1_record), 1,\n\
             sizeof(prdb_host_api_v1), sizeof(prdb_invocation_v1)\n\
         };\n\
         int32_t prdb_query_main_v1(const prdb_host_api_v1 *host,\n\
             const prdb_invocation_v1 *invocation) {\n\
             static const uint8_t output[16384] = {0};\n\
             (void)invocation;\n\
             for (uint32_t frame = 0; frame < 1024; ++frame) {\n\
                 if (host->write_result(host->context, output, sizeof(output)) != 0) return 1;\n\
             }\n\
             return 0;\n\
         }\n",
    )
    .expect("write TinyCC query fixture");
    let include_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("include");
    let compile = Command::new("tcc")
        .arg("-fPIC")
        .arg("-c")
        .arg("-I")
        .arg(include_dir)
        .arg(&query_source)
        .arg("-o")
        .arg(&query_object)
        .output()
        .expect("compile TCP query fixture with TinyCC");
    assert!(
        compile.status.success(),
        "TinyCC failed: {}",
        String::from_utf8_lossy(&compile.stderr)
    );
    let link = Command::new("env")
        .arg("-u")
        .arg("NIX_LDFLAGS")
        .arg("NIX_DONT_SET_RPATH=1")
        .arg("cc")
        .arg("-shared")
        .arg("-Wl,--no-eh-frame-hdr,-z,noexecstack")
        .arg(&query_object)
        .arg("-L")
        .arg(&sdk_release_dir)
        .arg("-Wl,--no-as-needed")
        .arg("-l:libprdb_sql.so.1")
        .arg("-Wl,-rpath,$ORIGIN")
        .arg("-o")
        .arg(&query_artifact)
        .output()
        .expect("link TCP query fixture with a non-executable stack");
    assert!(
        link.status.success(),
        "query fixture link failed: {}",
        String::from_utf8_lossy(&link.stderr)
    );
    let artifact = std::fs::read(&query_artifact).expect("read TinyCC query fixture");
    let manifest_value = manifest_v1_from_elf(
        &artifact,
        1,
        1,
        "x86_64-v1".to_owned(),
        "nixos-x86_64-glibc-2.42".to_owned(),
        "TinyCC integration fixture".to_owned(),
    )
    .expect("derive manifest from fixture ELF");
    let dependencies = manifest_value.dependencies.clone();
    let manifest = encode_manifest_v1(&manifest_value).expect("encode fixture manifest");
    let instance_dir = root.join("instance");
    let mut setup =
        initialize(&instance_dir, "tcp_test_admin", i64::MAX).expect("initialize test instance");
    let sdk_repository = prdb_engine::sdk_registry::SdkLibraryRepository::open(
        &instance_dir.join("sdk-libraries"),
        "linux-x86_64".to_owned(),
        "x86_64-v1".to_owned(),
        "nixos-x86_64-glibc-2.42".to_owned(),
        dependencies.iter().cloned().collect(),
        128 * 1024 * 1024,
    )
    .expect("open instance SDK repository");
    let sdk_metadata = std::fs::read(sdk_release_dir.join("sdk-release.json"))
        .expect("read installed SDK metadata");
    let sdk_library = std::fs::read(sdk_release_dir.join("libprdb_sql.so.1"))
        .expect("read installed SDK library");
    sdk_repository
        .install(
            &sdk_metadata,
            &sdk_library,
            &dependencies.iter().cloned().collect(),
        )
        .expect("install SDK release in instance repository");
    let token_id = setup.token_id();
    let admin_user_id = setup.admin_user_id();
    let mut bootstrap_secret = setup.take_secret().expect("take setup secret");
    let mut control_secret = bootstrap_secret;
    drop(setup.acknowledge().expect("publish READY instance"));

    let service_socket = root.join("supervisor.sock");
    let uid = unsafe { libc::geteuid() };
    let cgroup_parent = std::env::var_os("PRDB_TEST_CGROUP_PARENT")
        .map(std::path::PathBuf::from)
        .expect("delegated cgroup parent for process integration test");
    let runtime_read_execute = std::env::var_os("PRDB_TEST_RUNTIME_READ_EXECUTE")
        .expect("sandbox runtime paths for process integration test");
    let runtime_read_execute = std::env::split_paths(&runtime_read_execute).collect::<Vec<_>>();
    assert!(!runtime_read_execute.is_empty());
    let runtime_read_only = std::env::var_os("PRDB_TEST_RUNTIME_READ_ONLY")
        .filter(|paths| !paths.is_empty())
        .map(|paths| std::env::split_paths(&paths).collect::<Vec<_>>())
        .unwrap_or_default();
    let mut supervisor_command = Command::new(env!("CARGO_BIN_EXE_prdb-supervisor"));
    supervisor_command.args([
        "--instance-dir",
        instance_dir.to_str().expect("UTF-8 instance path"),
        "--service-socket",
        service_socket.to_str().expect("UTF-8 socket path"),
        "--gateway-uid",
        &uid.to_string(),
        "--cgroup-parent",
        cgroup_parent.to_str().expect("UTF-8 cgroup path"),
        "--executor",
        env!("CARGO_BIN_EXE_prdb-executor"),
        "--optimizer",
        env!("CARGO_BIN_EXE_prdb-optimizer"),
        "--scratch-parent",
        root.to_str().expect("UTF-8 scratch path"),
        "--cpu-baseline",
        "x86_64-v1",
        "--runtime-profile",
        "nixos-x86_64-glibc-2.42",
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
    for path in &runtime_read_execute {
        supervisor_command.arg("--runtime-read-execute").arg(path);
    }
    for path in &runtime_read_only {
        supervisor_command.arg("--runtime-read-only").arg(path);
    }
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
            "--workers",
            "1",
            "--queue-capacity",
            "1",
            "--control-workers",
            "3",
            "--control-queue-capacity",
            "1",
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
        let mut stderr = BufReader::new(stderr);
        let result = (|| -> std::io::Result<_> {
            let mut ordinary = String::new();
            let mut control = String::new();
            stderr.read_line(&mut ordinary)?;
            stderr.read_line(&mut control)?;
            Ok((ordinary, control))
        })();
        let _ = address_sender.send(result);
    });
    let (address_line, control_address_line) = address_receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("gateway listen line")
        .expect("read gateway listen line");
    let address: std::net::SocketAddr = address_line
        .trim()
        .strip_prefix("prdb-gateway listening on ")
        .expect("gateway listen address prefix")
        .parse()
        .expect("gateway listen address");
    let control_address: std::net::SocketAddr = control_address_line
        .trim()
        .strip_prefix("prdb-gateway control listening on ")
        .expect("gateway control listen address prefix")
        .parse()
        .expect("gateway control listen address");

    let mut client = TcpStream::connect(address).expect("connect external TCP client");
    client
        .set_read_timeout(Some(Duration::from_secs(3)))
        .expect("set TCP client timeout");
    let mut decoder = ClientFrameDecoder::default();
    let mut buffered = std::collections::VecDeque::new();
    {
        let hello = exchange_client_request(
            &mut client,
            &mut decoder,
            &mut buffered,
            1,
            1,
            br#"{"mode":"query","client_name":"supervisor-e2e","sdk_library_mode":"repository"}"#
                .to_vec(),
        );
        assert_eq!(hello.kind, 100);
        assert_eq!(hello.request_id, 1);
        let expected_hello = format!(
            "{{\"major\":1,\"minor\":0,\"mode\":\"query\",\"sdk_library_mode\":\"repository\",\"limits\":{{\"payload_bytes\":{},\"control_bytes\":{},\"chunk_bytes\":{}}},\"transport\":\"plaintext_tcp\"}}",
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
        let auth = exchange_client_request(
            &mut client,
            &mut decoder,
            &mut buffered,
            2,
            2,
            auth_payload.as_bytes().to_vec(),
        );
        auth_payload.zeroize();
        assert_eq!(auth.kind, 100);
        assert!(String::from_utf8_lossy(&auth.payload).contains("\"authenticated\":true"));
        token_secret.zeroize();

        let scope = [b"PSC1\0\x01\x02\x02".as_slice(), &[1; 16]].concat();
        let artifact_sha256 = Sha256::digest(&artifact);
        let empty_sha256 = Sha256::digest([]);
        let prepare_payload = format!(
            "{{\"manifest_version\":1,\"manifest\":\"{}\",\"artifact_bytes\":\"{}\",\"artifact_sha256\":\"{}\",\"parameter_bytes\":\"0\",\"parameter_sha256\":\"{}\",\"scope_version\":1,\"requested_scope\":\"{}\",\"deadline_ms\":\"30000\",\"workload_class\":\"operational\",\"sdk_library_mode\":\"repository\"}}",
            encode_base64(&manifest),
            artifact.len(),
            encode_hex(&artifact_sha256),
            encode_hex(&empty_sha256),
            encode_base64(&scope),
        );
        let prepared = exchange_client_request(
            &mut client,
            &mut decoder,
            &mut buffered,
            3,
            3,
            prepare_payload.as_bytes().to_vec(),
        );
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
        let uploaded =
            exchange_client_request(&mut client, &mut decoder, &mut buffered, 4, 4, chunk);
        assert_eq!(uploaded.kind, 100);
        assert_eq!(
            String::from_utf8_lossy(&uploaded.payload),
            format!("{{\"next_offset\":\"{}\"}}", artifact.len())
        );
        let seal_payload = format!("{{\"invocation_id\":\"{invocation_id_hex}\"}}");
        let sealed = exchange_client_request(
            &mut client,
            &mut decoder,
            &mut buffered,
            6,
            5,
            seal_payload.into_bytes(),
        );
        assert_eq!(sealed.kind, 100);
        assert!(String::from_utf8_lossy(&sealed.payload).contains("\"state\":\"prepared\""));

        let status_payload = format!("{{\"invocation_id\":\"{invocation_id_hex}\"}}");
        let status = exchange_client_request(
            &mut client,
            &mut decoder,
            &mut buffered,
            8,
            6,
            status_payload.as_bytes().to_vec(),
        );
        assert_eq!(status.kind, 100);
        assert_eq!(status.payload, br#"{"state":"prepared"}"#);

        let cancel = exchange_client_request(
            &mut client,
            &mut decoder,
            &mut buffered,
            9,
            7,
            status_payload.as_bytes().to_vec(),
        );
        assert_eq!(cancel.kind, 100);
        assert_eq!(cancel.payload, br#"{"state":"accepted"}"#);

        let cancelled_status = exchange_client_request(
            &mut client,
            &mut decoder,
            &mut buffered,
            8,
            8,
            status_payload.into_bytes(),
        );
        assert_eq!(cancelled_status.kind, 100);
        assert_eq!(cancelled_status.payload, br#"{"state":"unknown"}"#);

        let ping = exchange_client_request(
            &mut client,
            &mut decoder,
            &mut buffered,
            11,
            9,
            b"{}".to_vec(),
        );
        assert_eq!(ping.kind, 100);
        assert_eq!(ping.request_id, 9);
        assert_eq!(ping.payload, br#"{"alive":true}"#);

        let second_prepared = exchange_client_request(
            &mut client,
            &mut decoder,
            &mut buffered,
            3,
            10,
            prepare_payload.into_bytes(),
        );
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
        let second_upload = exchange_client_request(
            &mut client,
            &mut decoder,
            &mut buffered,
            4,
            11,
            second_chunk,
        );
        assert_eq!(second_upload.kind, 100);
        let second_seal_payload = format!("{{\"invocation_id\":\"{second_invocation_hex}\"}}");
        let second_sealed = exchange_client_request(
            &mut client,
            &mut decoder,
            &mut buffered,
            6,
            12,
            second_seal_payload.as_bytes().to_vec(),
        );
        assert_eq!(second_sealed.kind, 100);
        assert_eq!(second_sealed.payload, br#"{"state":"prepared"}"#);

        let start = exchange_client_request(
            &mut client,
            &mut decoder,
            &mut buffered,
            7,
            13,
            second_seal_payload.into_bytes(),
        );
        assert_eq!(start.kind, 100);
        assert_eq!(start.request_id, 13);
        assert_eq!(start.payload, br#"{"state":"accepted"}"#);
        let mut stream_sequences = ClientResponseSequences::default();
        stream_sequences
            .begin_stream(13)
            .expect("begin START stream");

        let mut control =
            AdminClient::connect_authenticated(control_address, token_id, &mut control_secret)
                .expect("authenticate typed control client through reserved listener");
        assert_eq!(
            control
                .status(decode_hex_id(second_invocation_hex))
                .expect("read active status through control client"),
            InvocationStatus::Active,
            "status while result client is unread"
        );

        let mut data_frames = 0;
        loop {
            let frame = next_client_response(&mut client, &mut decoder, &mut buffered);
            let terminal = stream_sequences
                .accept_stream_frame(&frame)
                .expect("validate streamed response sequence");
            assert_eq!(frame.request_id, 13);
            if frame.kind == 103 {
                assert!(terminal);
                let terminal_payload = String::from_utf8(frame.payload).expect("terminal JSON");
                assert!(
                    terminal_payload.contains("\"outcome\":\"committed\""),
                    "unexpected process START terminal: {terminal_payload}"
                );
                break;
            }
            assert_eq!(frame.kind, 102);
            assert_eq!(frame.payload.len(), 16384);
            assert!(frame.payload.iter().all(|byte| *byte == 0));
            data_frames += 1;
        }
        assert_eq!(data_frames, 1024);
        assert_eq!(
            control
                .status(decode_hex_id(second_invocation_hex))
                .expect("read committed status through control client"),
            InvocationStatus::Committed
        );
        let token_page = control
            .list_tokens_page(admin_user_id, None)
            .expect("list bootstrap token through process ADMIN path");
        assert!(
            token_page
                .tokens
                .iter()
                .any(|token| token.token_id == token_id)
        );
        let limited_user = control
            .create_user("process_limited")
            .expect("create regular user through process ADMIN path");
        let limited_token = control
            .issue_token(limited_user.identity_id, "process-test", i64::MAX)
            .expect("issue regular-user token through process ADMIN path");
        let grant_observer = control
            .create_user("process_grant_observer")
            .expect("create object-grant recipient through process ADMIN path");
        let grant_observer_token = control
            .issue_token(grant_observer.identity_id, "process-grant-test", i64::MAX)
            .expect("issue object-grant recipient token through process ADMIN path");
        let mut grant_observer_secret = *grant_observer_token.token_secret();
        let mut limited_secret = *limited_token.token_secret();
        let mut limited_secret_after_grant = *limited_token.token_secret();
        let mut limited = AdminClient::connect_authenticated(
            control_address,
            limited_token.token_id,
            &mut limited_secret,
        )
        .expect("authenticate regular user through process control listener");
        let denied_schema = limited.create_schema("process_denied", limited_user.identity_id);
        assert!(matches!(
            denied_schema,
            Err(
                prdb_engine::client_submit::ClientSubmitError::AdministrativeOperationRejected { .. }
            )
        ));
        drop(limited);
        control
            .grant_system_privilege(limited_user.identity_id, AdminSystemPrivilege::CreateSchema)
            .expect("grant CREATE_SCHEMA through process ADMIN path");
        let mut limited = AdminClient::connect_authenticated(
            control_address,
            limited_token.token_id,
            &mut limited_secret_after_grant,
        )
        .expect("re-authenticate regular user after CREATE_SCHEMA grant");
        control
            .grant_system_privilege(limited_user.identity_id, AdminSystemPrivilege::ManageUsers)
            .expect("grant MANAGE_USERS while the ADMIN connection remains open");
        let mut limited_manage_users_secret = *limited_token.token_secret();
        let mut limited_manage_users = AdminClient::connect_authenticated(
            control_address,
            limited_token.token_id,
            &mut limited_manage_users_secret,
        )
        .expect("authenticate a second open ADMIN connection with MANAGE_USERS");
        let created_by_open_connection = limited_manage_users
            .create_user("process_open_manage_users")
            .expect("open ADMIN connection observes the new MANAGE_USERS grant");
        control
            .drop_user(created_by_open_connection.identity_id)
            .expect("clean up user created through the open ADMIN connection");
        control
            .revoke_system_privilege(limited_user.identity_id, AdminSystemPrivilege::ManageUsers)
            .expect("revoke MANAGE_USERS while the ADMIN connection remains open");
        assert!(matches!(
            limited_manage_users.create_user("process_open_manage_users"),
            Err(
                prdb_engine::client_submit::ClientSubmitError::AdministrativeOperationRejected { .. }
            )
        ));
        drop(limited_manage_users);
        let created_after_revocation = control
            .create_user("process_open_manage_users")
            .expect("revoked create-user request left no identity state behind");
        control
            .drop_user(created_after_revocation.identity_id)
            .expect("remove proof user created after MANAGE_USERS revocation");
        let user_schema = limited
            .create_schema("process_user_owned", limited_user.identity_id)
            .expect("CREATE_SCHEMA grant permits schema creation across processes");
        let grant_table = limited
            .create_table(
                user_schema.identity_id,
                "grant_target",
                &[AdminColumnDefinition {
                    name: "payload".to_owned(),
                    scalar_type: prdb_engine::tuple::ScalarType::Text,
                    nullable: true,
                    generated_uuid_identity: false,
                    default: None,
                }],
            )
            .expect("schema owner creates an owned table through process ADMIN");
        let mut grant_observer_client = AdminClient::connect_authenticated(
            control_address,
            grant_observer_token.token_id,
            &mut grant_observer_secret,
        )
        .expect("authenticate object-grant observer through process control listener");
        let unauthorized_grant = grant_observer_client.grant_object_privilege(
            grant_observer.identity_id,
            grant_table.table_id,
            AdminObjectPrivilege::Select,
            AdminObjectGrantTarget::Column(grant_table.column_ids[0]),
        );
        assert!(matches!(
            unauthorized_grant,
            Err(
                prdb_engine::client_submit::ClientSubmitError::AdministrativeOperationRejected { .. }
            )
        ));
        let object_grant = limited
            .grant_object_privilege(
                grant_observer.identity_id,
                grant_table.table_id,
                AdminObjectPrivilege::Select,
                AdminObjectGrantTarget::Column(grant_table.column_ids[0]),
            )
            .expect("table owner grants SELECT on one column through process ADMIN");
        assert!(object_grant.commit_sequence > 0);
        let defaulted = limited
            .set_column_default(
                grant_table.table_id,
                grant_table.column_ids[0],
                prdb_engine::tuple::ScalarType::Text,
                AdminColumnDefault::Literal(prdb_engine::tuple::ScalarValue::Text(
                    "gateway-default".to_owned(),
                )),
            )
            .expect("set a typed column default through gateway and supervisor");
        assert!(defaulted.commit_sequence > object_grant.commit_sequence);
        let renamed_column = limited
            .rename_column(grant_table.table_id, grant_table.column_ids[0], "body")
            .expect("rename an owned column through gateway and supervisor");
        assert_eq!(renamed_column.column_id, grant_table.column_ids[0]);
        let not_null = limited
            .alter_column_nullability(grant_table.table_id, grant_table.column_ids[0], false)
            .expect("set column nullability through gateway and supervisor");
        assert!(not_null.commit_sequence > renamed_column.commit_sequence);
        let dropped_default = limited
            .drop_column_default(grant_table.table_id, grant_table.column_ids[0])
            .expect("drop the column default through gateway and supervisor");
        assert!(dropped_default.commit_sequence > not_null.commit_sequence);
        let columns = limited
            .list_columns_page(user_schema.identity_id, grant_table.table_id, 64, None)
            .expect("read the updated column shape through gateway and supervisor");
        assert_eq!(columns.records.len(), 1);
        assert_eq!(columns.records[0].column_id, grant_table.column_ids[0]);
        assert_eq!(columns.records[0].name, "body");
        assert_eq!(
            columns.records[0].scalar_type,
            prdb_engine::tuple::ScalarType::Text
        );
        assert!(!columns.records[0].nullable);
        drop(grant_observer_client);
        limited
            .drop_table(grant_table.table_id)
            .expect("drop table and its direct grant through process ADMIN");
        limited
            .drop_schema(user_schema.identity_id)
            .expect("schema owner can drop its empty schema across processes");
        control
            .drop_user(grant_observer.identity_id)
            .expect("drop object-grant recipient after its table was removed");
        control
            .revoke_system_privilege(limited_user.identity_id, AdminSystemPrivilege::CreateSchema)
            .expect("revoke CREATE_SCHEMA before dropping its grantee");
        let revoked_schema = limited.create_schema("process_revoked", limited_user.identity_id);
        assert!(matches!(
            &revoked_schema,
            Err(
                prdb_engine::client_submit::ClientSubmitError::AdministrativeOperationRejected { .. }
            )
        ), "revoked schema-create request returned {revoked_schema:?}");
        drop(limited);
        let authorized_revoked_name = control
            .create_schema("process_revoked", admin_user_id)
            .expect("revoked control request left no schema state behind");
        control
            .drop_schema(authorized_revoked_name.identity_id)
            .expect("remove schema created after the revoked user's denial");
        control
            .drop_user(limited_user.identity_id)
            .expect("drop regular user and revoke its token through process ADMIN path");

        let authorized_schema = control
            .create_schema("process_denied", admin_user_id)
            .expect("denied schema creation left no catalog state behind");
        control
            .drop_schema(authorized_schema.identity_id)
            .expect("remove schema created after regular-user denial");
        let schema = control
            .create_schema("process_admin", admin_user_id)
            .expect("create schema through process ADMIN path");
        let table = control
            .create_table(
                schema.identity_id,
                "events",
                &[AdminColumnDefinition {
                    name: "event_id".to_owned(),
                    scalar_type: prdb_engine::tuple::ScalarType::Uuid,
                    nullable: false,
                    generated_uuid_identity: true,
                    default: None,
                }],
            )
            .expect("create table through process ADMIN path");
        let primary_key = control
            .create_primary_key(table.table_id, &[table.column_ids[0]])
            .expect("create primary key through process ADMIN path");
        let child = control
            .create_table(
                schema.identity_id,
                "event_links",
                &[AdminColumnDefinition {
                    name: "event_id".to_owned(),
                    scalar_type: prdb_engine::tuple::ScalarType::Uuid,
                    nullable: true,
                    generated_uuid_identity: false,
                    default: None,
                }],
            )
            .expect("create referencing table through process ADMIN path");
        control
            .grant_object_privilege(
                admin_user_id,
                table.table_id,
                AdminObjectPrivilege::References,
                AdminObjectGrantTarget::Column(table.column_ids[0]),
            )
            .expect("grant REFERENCES on the target key through process ADMIN path");
        control
            .create_foreign_key(
                child.table_id,
                &[child.column_ids[0]],
                primary_key.constraint_id,
            )
            .expect("create foreign key through process ADMIN path");
        control
            .drop_table(child.table_id)
            .expect("drop referencing table and owned foreign key through process ADMIN path");
        control
            .drop_table(table.table_id)
            .expect("drop table through process ADMIN path");
        control
            .drop_schema(schema.identity_id)
            .expect("drop empty schema through process ADMIN path");
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
    remove_test_tree(&root).expect("remove test files");
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
