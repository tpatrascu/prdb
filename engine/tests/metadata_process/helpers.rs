#![allow(dead_code)]
#![cfg(all(unix, target_os = "linux", target_arch = "x86_64"))]

use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::process::Child;
use std::time::{Duration, Instant};

pub struct ChildGuard(pub Option<Child>);

pub fn install_test_sdk_repository(
    instance_dir: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let sdk_release_dir = std::env::var_os("PRDB_TEST_INSTALLED_SDK_RELEASE_DIR")
        .map(std::path::PathBuf::from)
        .ok_or("PRDB_TEST_INSTALLED_SDK_RELEASE_DIR is required for process tests")?;
    let repository = prdb_engine::sdk_registry::SdkLibraryRepository::open(
        &instance_dir.join("sdk-libraries"),
        "linux-x86_64".to_owned(),
        "x86_64-v1".to_owned(),
        "nixos-x86_64-glibc-2.42".to_owned(),
        ["libc.so.6".to_owned()].into_iter().collect(),
        128 * 1024 * 1024,
        None,
    )
    .map_err(|error| format!("open test SDK repository: {error:?}"))?;
    repository
        .install(
            &std::fs::read(sdk_release_dir.join("sdk-release.json"))?,
            &std::fs::read(sdk_release_dir.join("libprdb_sql.so.1"))?,
            &["libc.so.6".to_owned()].into_iter().collect(),
        )
        .map_err(|error| format!("install test SDK release: {error:?}"))?;
    Ok(())
}

pub fn read_raw_client_frame(
    stream: &mut std::net::TcpStream,
) -> std::io::Result<Option<(Vec<u8>, u16)>> {
    let mut header = [0u8; prdb_engine::client_protocol::CLIENT_FRAME_HEADER_BYTES];
    match stream.read(&mut header[..1])? {
        0 => return Ok(None),
        1 => (),
        _ => unreachable!("one-byte read cannot exceed its buffer"),
    }
    stream.read_exact(&mut header[1..])?;
    if &header[..4] != b"PDBN" {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid client frame magic",
        ));
    }
    let payload_bytes =
        u32::from_le_bytes(header[12..16].try_into().expect("fixed frame header")) as usize;
    if payload_bytes > prdb_engine::client_protocol::CLIENT_MAX_PAYLOAD_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "client frame exceeds the payload limit",
        ));
    }
    let kind = u16::from_le_bytes([header[8], header[9]]);
    let mut raw = Vec::with_capacity(header.len() + payload_bytes);
    raw.extend_from_slice(&header);
    raw.resize(header.len() + payload_bytes, 0);
    stream.read_exact(&mut raw[header.len()..])?;
    Ok(Some((raw, kind)))
}

pub type InteractiveResponseDropStats = (usize, usize, usize, usize);

pub type InteractiveResponseDropHandle =
    std::thread::JoinHandle<std::io::Result<InteractiveResponseDropStats>>;

pub fn assert_interactive_response_drop_stats(
    stats: InteractiveResponseDropStats,
    expected_begin_requests: usize,
    expected_matching_requests: usize,
) {
    assert_eq!(stats.0, 1, "exactly one response should be dropped");
    assert!(stats.1 >= 1, "outcome reconciliation should query status");
    assert_eq!(stats.2, expected_begin_requests);
    assert_eq!(stats.3, expected_matching_requests);
}

pub fn start_interactive_response_dropping_proxy(
    gateway_address: std::net::SocketAddr,
    dropped_request_kind: u16,
    dropped_request_occurrence: usize,
    drop_stream_data: bool,
) -> std::io::Result<(std::net::SocketAddr, InteractiveResponseDropHandle)> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let proxy_address = listener.local_addr()?;
    let handle = std::thread::spawn(move || {
        let mut dropped = 0usize;
        let mut matching_requests = 0usize;
        let mut status_requests = 0usize;
        let mut begin_requests = 0usize;
        let mut last_activity = Instant::now();
        while last_activity.elapsed() < Duration::from_secs(3) {
            let (mut client, _) = match listener.accept() {
                Ok(connection) => connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
                Err(error) => return Err(error),
            };
            last_activity = Instant::now();
            let mut gateway = std::net::TcpStream::connect(gateway_address)?;
            client.set_read_timeout(Some(Duration::from_secs(10)))?;
            client.set_write_timeout(Some(Duration::from_secs(10)))?;
            gateway.set_read_timeout(Some(Duration::from_secs(10)))?;
            gateway.set_write_timeout(Some(Duration::from_secs(10)))?;
            while let Some((request, request_kind)) = read_raw_client_frame(&mut client)? {
                if request_kind == dropped_request_kind {
                    matching_requests += 1;
                }
                let is_admin_status = request
                    .windows(b"runtime.invocation.status".len())
                    .any(|window| window == b"runtime.invocation.status");
                if request_kind == 8
                    || is_admin_status
                    || request_kind == prdb_engine::client_protocol::CLIENT_V2_TX_STATUS_KIND
                    || request_kind == prdb_engine::client_protocol::CLIENT_V2_PACKAGE_STATUS_KIND
                {
                    status_requests += 1;
                }
                if request_kind == prdb_engine::client_protocol::CLIENT_V2_TX_BEGIN_KIND {
                    begin_requests += 1;
                }
                gateway.write_all(&request)?;
                let Some((response, response_kind)) = read_raw_client_frame(&mut gateway)? else {
                    break;
                };
                if drop_stream_data && request_kind == 7 && response_kind == 100 {
                    client.write_all(&response)?;
                    let mut client_connected = true;
                    while let Some((event, event_kind)) = read_raw_client_frame(&mut gateway)? {
                        if event_kind == 102 && dropped == 0 && dropped_request_occurrence == 1 {
                            dropped += 1;
                            last_activity = Instant::now();
                            continue;
                        }
                        if client_connected && client.write_all(&event).is_err() {
                            client_connected = false;
                        }
                        if event_kind == 103 {
                            break;
                        }
                    }
                    break;
                }
                if request_kind == dropped_request_kind
                    && matching_requests == dropped_request_occurrence
                    && response_kind == 100
                    && dropped == 0
                {
                    dropped += 1;
                    last_activity = Instant::now();
                    break;
                }
                client.write_all(&response)?;
            }
        }
        Ok((dropped, status_requests, begin_requests, matching_requests))
    });
    Ok((proxy_address, handle))
}

impl ChildGuard {
    pub fn child(&mut self) -> &mut Child {
        self.0.as_mut().expect("child process still running")
    }

    pub fn stop(&mut self) -> std::io::Result<std::process::ExitStatus> {
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

    pub fn kill(&mut self) -> std::io::Result<std::process::ExitStatus> {
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

pub struct DirectoryGuard(pub std::path::PathBuf);

pub fn encode_id_hex(id: &[u8; 16]) -> String {
    let mut encoded = String::with_capacity(32);
    for byte in id {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

pub fn decode_id_hex(encoded: &str) -> Option<[u8; 16]> {
    if encoded.len() != 32 || !encoded.is_ascii() {
        return None;
    }
    let mut id = [0; 16];
    for (index, byte) in id.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&encoded[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(id)
}

impl Drop for DirectoryGuard {
    fn drop(&mut self) {
        fn make_removable(path: &std::path::Path) -> std::io::Result<()> {
            let metadata = std::fs::symlink_metadata(path)?;
            if metadata.file_type().is_symlink() {
                return Ok(());
            }
            if metadata.is_dir() {
                for entry in std::fs::read_dir(path)? {
                    make_removable(&entry?.path())?;
                }
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
            } else {
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
            }
            Ok(())
        }

        let _ = make_removable(&self.0);
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub type DecodedSqlrRow = Vec<(String, Option<prdb_engine::tuple::ScalarValue>)>;

pub fn decode_sqlr_rows(bytes: &[u8]) -> Result<Vec<DecodedSqlrRow>, String> {
    use prdb_engine::tuple::ScalarValue;

    let mut rows = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        if bytes.len() - offset < 8 {
            return Err("truncated SQLR header".to_owned());
        }
        if &bytes[offset..offset + 4] != b"SQLR" {
            return Err(format!("invalid SQLR magic at offset {offset}"));
        }
        let version = u16::from_be_bytes([bytes[offset + 4], bytes[offset + 5]]);
        if version != 1 {
            return Err(format!("unsupported SQLR version {version}"));
        }
        let column_count = u16::from_be_bytes([bytes[offset + 6], bytes[offset + 7]]) as usize;
        offset += 8;

        let mut row = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            if bytes.len() - offset < 2 {
                return Err("truncated SQLR column name length".to_owned());
            }
            let name_len = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]) as usize;
            offset += 2;
            if bytes.len() - offset < name_len + 11 {
                return Err("truncated SQLR column header".to_owned());
            }
            let name = std::str::from_utf8(&bytes[offset..offset + name_len])
                .map_err(|e| format!("invalid UTF-8 column name: {e}"))?
                .to_owned();
            offset += name_len;
            let type_code = u32::from_be_bytes(
                bytes[offset..offset + 4]
                    .try_into()
                    .expect("4-byte type_code"),
            );
            let _precision = bytes[offset + 4];
            let _scale = bytes[offset + 5];
            let state = bytes[offset + 6];
            let value_len = u32::from_be_bytes(
                bytes[offset + 7..offset + 11]
                    .try_into()
                    .expect("4-byte value_len"),
            ) as usize;
            offset += 11;
            if bytes.len() - offset < value_len {
                return Err("truncated SQLR cell value".to_owned());
            }
            let raw = &bytes[offset..offset + value_len];
            offset += value_len;

            let value = match state {
                0 => Some(match type_code {
                    1 => {
                        if raw.len() != 1 {
                            return Err("invalid boolean length".to_owned());
                        }
                        ScalarValue::Boolean(raw[0] != 0)
                    }
                    2 => ScalarValue::SmallInt(i16::from_be_bytes(
                        raw.try_into().map_err(|_| "invalid smallint length")?,
                    )),
                    3 => ScalarValue::Integer(i32::from_be_bytes(
                        raw.try_into().map_err(|_| "invalid integer length")?,
                    )),
                    4 => ScalarValue::BigInt(i64::from_be_bytes(
                        raw.try_into().map_err(|_| "invalid bigint length")?,
                    )),
                    5 => ScalarValue::Decimal(i128::from_be_bytes(
                        raw.try_into().map_err(|_| "invalid decimal length")?,
                    )),
                    6 => ScalarValue::Text(
                        std::str::from_utf8(raw)
                            .map_err(|e| format!("invalid text UTF-8: {e}"))?
                            .to_owned(),
                    ),
                    7 => ScalarValue::Varchar(
                        std::str::from_utf8(raw)
                            .map_err(|e| format!("invalid varchar UTF-8: {e}"))?
                            .to_owned(),
                    ),
                    8 => ScalarValue::Bytea(raw.to_vec()),
                    9 => ScalarValue::Uuid(raw.try_into().map_err(|_| "invalid uuid length")?),
                    10 => ScalarValue::Date(i32::from_be_bytes(
                        raw.try_into().map_err(|_| "invalid date length")?,
                    )),
                    11 => ScalarValue::TimestampTz(i64::from_be_bytes(
                        raw.try_into().map_err(|_| "invalid timestamptz length")?,
                    )),
                    other => return Err(format!("unknown SQLR type code {other}")),
                }),
                1 => None,
                2 => return Err(format!("unexpected REDACTED cell for column {name}")),
                other => return Err(format!("unknown SQLR cell state {other}")),
            };
            row.push((name, value));
        }
        rows.push(row);
    }
    Ok(rows)
}

pub struct InteractiveTestInstance {
    pub root: std::path::PathBuf,
    pub _cleanup: DirectoryGuard,
    pub address: std::net::SocketAddr,
    pub _supervisor: ChildGuard,
    pub _gateway: ChildGuard,
    pub admin_credentials_path: std::path::PathBuf,
    pub admin_user: String,
    pub sdk_library: std::path::PathBuf,
    pub include_dir: std::path::PathBuf,
}

impl InteractiveTestInstance {
    pub fn start(name_prefix: &str, admin_user: &str) -> Result<Self, Box<dyn std::error::Error>> {
        use std::io::{BufRead, BufReader};
        use std::process::{Command, Stdio};

        fn runtime_paths(
            name: &str,
        ) -> Result<Vec<std::path::PathBuf>, Box<dyn std::error::Error>> {
            let value = std::env::var_os(name).ok_or(format!("{name} is required"))?;
            if name == "PRDB_TEST_RUNTIME_READ_ONLY" && value.is_empty() {
                return Ok(Vec::new());
            }
            let paths = std::env::split_paths(&value).collect::<Vec<_>>();
            if paths.is_empty() || paths.iter().any(|path| !path.is_absolute()) {
                return Err(format!("{name} must contain absolute paths").into());
            }
            Ok(paths)
        }

        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("{name_prefix}-{}-{nonce}", std::process::id()));
        std::fs::create_dir(&root)?;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
        let cleanup = DirectoryGuard(root.clone());

        let instance_dir = root.join("instance");
        let mut setup = prdb_engine::bootstrap::initialize(&instance_dir, admin_user, i64::MAX)?;
        let token_id = setup.token_id();
        let token_secret = setup.take_secret().ok_or("bootstrap secret missing")?;
        drop(setup.acknowledge()?);
        install_test_sdk_repository(&instance_dir)?;

        let admin_credentials_path = root.join("admin.cred");
        prdb_engine::client_credentials::write_client_credentials(
            &admin_credentials_path,
            &prdb_engine::client_protocol::ClientCredentials {
                token_id,
                token_secret,
            },
        )?;

        let cgroup_parent = std::env::var_os("PRDB_TEST_CGROUP_PARENT")
            .map(std::path::PathBuf::from)
            .ok_or("PRDB_TEST_CGROUP_PARENT must name a delegated cgroup v2 parent")?;
        if !cgroup_parent.is_absolute() {
            return Err("PRDB_TEST_CGROUP_PARENT must be absolute".into());
        }
        let runtime_read_execute = runtime_paths("PRDB_TEST_RUNTIME_READ_EXECUTE")?;
        let runtime_read_only = runtime_paths("PRDB_TEST_RUNTIME_READ_ONLY")?;
        let service_socket = root.join("supervisor.sock");
        // SAFETY: geteuid has no preconditions and returns the effective user ID.
        let uid = unsafe { libc::geteuid() };
        let uid_string = uid.to_string();

        let mut supervisor_command = Command::new(env!("CARGO_BIN_EXE_prdb-supervisor"));
        supervisor_command.args([
            "--instance-dir",
            instance_dir.to_str().ok_or("non-UTF-8 instance path")?,
            "--service-socket",
            service_socket.to_str().ok_or("non-UTF-8 socket path")?,
            "--gateway-uid",
            &uid_string,
            "--cgroup-parent",
            cgroup_parent.to_str().ok_or("non-UTF-8 cgroup path")?,
            "--executor",
            env!("CARGO_BIN_EXE_prdb-executor"),
            "--optimizer",
            env!("CARGO_BIN_EXE_prdb-optimizer"),
            "--scratch-parent",
            root.to_str().ok_or("non-UTF-8 scratch path")?,
            "--cpu-baseline",
            "x86_64-v1",
            "--runtime-profile",
            "nixos-x86_64-glibc-2.42",
            "--allowed-dependency",
            "libc.so.6",
            "--retention-seconds",
            "3600",
            "--max-rows",
            "256",
            "--max-bytes",
            "65536",
            "--cpu-quota-us",
            "100000",
            "--cpu-period-us",
            "100000",
            "--memory-max-bytes",
            "134217728",
            "--max-processes",
            "16",
        ]);
        for path in &runtime_read_execute {
            supervisor_command.arg("--runtime-read-execute").arg(path);
        }
        for path in &runtime_read_only {
            supervisor_command.arg("--runtime-read-only").arg(path);
        }
        let mut supervisor = ChildGuard(Some(
            supervisor_command
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()?,
        ));
        let socket_deadline = Instant::now() + Duration::from_secs(5);
        while !service_socket.exists() {
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
                    service_socket.to_str().ok_or("non-UTF-8 socket path")?,
                    "--service-uid",
                    &uid_string,
                    "--connections-per-ip",
                    "500",
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()?,
        ));
        let gateway_stderr = gateway
            .child()
            .stderr
            .take()
            .ok_or("gateway stderr missing")?;
        let (address_sender, address_receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut reader = BufReader::new(gateway_stderr);
            let mut line = String::new();
            let result = reader.read_line(&mut line).map(|_| line);
            let _ = address_sender.send(result);
            let mut drain = String::new();
            let _ = std::io::Read::read_to_string(&mut reader, &mut drain);
        });
        let address_line = address_receiver.recv_timeout(Duration::from_secs(5))??;
        let address: std::net::SocketAddr = address_line
            .trim()
            .strip_prefix("prdb-gateway listening on ")
            .ok_or("gateway listen line had an unexpected prefix")?
            .parse()?;

        let sdk_release_dir = std::env::var_os("PRDB_TEST_INSTALLED_SDK_RELEASE_DIR")
            .map(std::path::PathBuf::from)
            .ok_or("PRDB_TEST_INSTALLED_SDK_RELEASE_DIR is required for interactive SQL")?;
        let sdk_library = sdk_release_dir.join("libprdb_sql.so.1");
        let include_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("include");

        Ok(Self {
            root,
            _cleanup: cleanup,
            address,
            _supervisor: supervisor,
            _gateway: gateway,
            admin_credentials_path,
            admin_user: admin_user.to_owned(),
            sdk_library,
            include_dir,
        })
    }

    pub fn write_scope_file(
        &self,
        name: &str,
        schema_names: &[&str],
    ) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
        use prdb_engine::authorization::Operation;
        use prdb_engine::client_credentials::read_client_credentials;
        use prdb_engine::client_submit::AdminClient;
        use prdb_engine::submission_scope::RequestedScopeBuilder;

        let mut creds = read_client_credentials(&self.admin_credentials_path)?;
        let mut admin = AdminClient::connect_authenticated(
            self.address,
            creds.token_id,
            &mut creds.token_secret,
        )?;
        let schemas = admin.list_schemas_page(128, None)?.records;
        let mut builder = RequestedScopeBuilder::new();
        for schema_name in schema_names {
            let schema = schemas
                .iter()
                .find(|s| s.name == *schema_name)
                .ok_or_else(|| format!("schema {schema_name} not found for scope"))?;
            builder
                .include_schema(schema)
                .map_err(|e| format!("scope include_schema: {e:?}"))?;
            let relations = admin
                .list_relations_page(schema.schema_id, 128, None)?
                .records;
            for relation in &relations {
                let ops = [
                    Operation::Select,
                    Operation::Insert,
                    Operation::Update,
                    Operation::Delete,
                ];
                builder
                    .include_relation(schema, relation, &ops)
                    .map_err(|e| format!("scope include_relation: {e:?}"))?;
                let columns = admin
                    .list_columns_page(schema.schema_id, relation.relation_id, 128, None)?
                    .records;
                for column in &columns {
                    builder
                        .include_column(schema, relation, column, &ops)
                        .map_err(|e| format!("scope include_column: {e:?}"))?;
                }
            }
        }
        let path = self.root.join(name);
        let encoded = builder
            .encode()
            .map_err(|e| format!("scope encode: {e:?}"))?;
        std::fs::write(&path, encoded)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        Ok(path)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_interactive_sql(
        &self,
        schema: &str,
        scope_path: &std::path::Path,
        default_user: &str,
        credentials_path: &std::path::Path,
        credential_profiles: &[(&str, &std::path::Path)],
        parameters_path: Option<&std::path::Path>,
        sql_or_file: InteractiveSqlInput<'_>,
    ) -> Result<std::process::Output, Box<dyn std::error::Error>> {
        use std::process::{Command, Stdio};

        let mut command = Command::new(env!("CARGO_BIN_EXE_prdb-client"));
        command.args([
            "interactive-sql",
            "--address",
            &self.address.to_string(),
            "--credentials",
            credentials_path.to_str().ok_or("non-UTF-8 credentials")?,
            "--default-user",
            default_user,
            "--schema",
            schema,
            "--scope",
            scope_path.to_str().ok_or("non-UTF-8 scope path")?,
            "--sdk-library",
            self.sdk_library.to_str().ok_or("non-UTF-8 sdk path")?,
            "--include-dir",
            self.include_dir.to_str().ok_or("non-UTF-8 include path")?,
            "--sdk-release",
            "1",
            "--cpu-baseline",
            "x86_64-v1",
            "--runtime-profile",
            "nixos-x86_64-glibc-2.42",
            "--compiler-identity",
            "TinyCC interactive SQL e2e",
        ]);
        for (user, path) in credential_profiles {
            command.arg("--credential-profile").arg(format!(
                "{user}={}",
                path.to_str().ok_or("non-UTF-8 profile path")?
            ));
        }
        if let Some(params) = parameters_path {
            command
                .arg("--parameters")
                .arg(params.to_str().ok_or("non-UTF-8 parameters path")?);
        }
        match sql_or_file {
            InteractiveSqlInput::File(path) => {
                command
                    .arg("--sql-file")
                    .arg(path.to_str().ok_or("non-UTF-8 sql path")?);
                Ok(command.output()?)
            }
            InteractiveSqlInput::Stdin(sql) => {
                let mut child = command
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()?;
                child
                    .stdin
                    .take()
                    .ok_or("stdin missing")?
                    .write_all(sql.as_bytes())?;
                Ok(child.wait_with_output()?)
            }
        }
    }
}

pub enum InteractiveSqlInput<'a> {
    File(&'a std::path::Path),
    Stdin(&'a str),
}
