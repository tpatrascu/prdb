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
