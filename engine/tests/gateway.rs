use prdb_engine::client_protocol::{ClientFrame, ClientFrameDecoder};
use prdb_engine::gateway::{
    GatewayConnectionLimiter, GatewayListenerConfig, GatewayListenerError, GatewayRuntimeConfig,
    TcpGatewayListener, run_gateway,
};
use prdb_engine::ipc::{IpcFrame, Operation, read_frame, write_frame};
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[test]
fn default_gateway_listener_binds_to_loopback() {
    let listener = TcpGatewayListener::bind(GatewayListenerConfig::default())
        .expect("default loopback listener should bind");

    assert!(listener.local_addr().unwrap().ip().is_loopback());
}

#[test]
fn non_loopback_plaintext_requires_explicit_opt_in() {
    let config = GatewayListenerConfig {
        bind_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        allow_non_loopback_plaintext: false,
    };

    assert!(matches!(
        TcpGatewayListener::bind(config),
        Err(GatewayListenerError::NonLoopbackPlaintextDisabled(_))
    ));
}

#[test]
fn explicit_non_loopback_plaintext_opt_in_allows_bind() {
    let config = GatewayListenerConfig {
        bind_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        allow_non_loopback_plaintext: true,
    };

    let listener = TcpGatewayListener::bind(config).expect("explicit opt-in should allow bind");
    assert_eq!(
        listener.local_addr().unwrap().ip(),
        IpAddr::V4(Ipv4Addr::UNSPECIFIED)
    );
}

#[test]
fn tcp_client_hello_and_ping_bridge_over_the_gateway_service_channel() {
    let listener = TcpGatewayListener::bind(GatewayListenerConfig::default()).unwrap();
    let address = listener.local_addr().unwrap();
    let (mut service_gateway, mut service_supervisor) =
        std::os::unix::net::UnixStream::pair().unwrap();
    let supervisor = std::thread::spawn(move || {
        let hello = read_frame(&mut service_supervisor).unwrap().unwrap();
        assert_eq!(hello.operation, Operation::OpenClientSession as u16);
        assert_eq!(hello.request_id, 1);
        write_frame(
            &mut service_supervisor,
            &IpcFrame {
                operation: hello.operation,
                request_id: hello.request_id,
                payload: br#"{"major":1,"minor":0}"#.to_vec(),
            },
        )
        .unwrap();

        let ping = read_frame(&mut service_supervisor).unwrap().unwrap();
        assert_eq!(ping.operation, Operation::PingClientSession as u16);
        assert_eq!(ping.request_id, 2);
        assert_eq!(ping.payload, b"{}");
        write_frame(
            &mut service_supervisor,
            &IpcFrame {
                operation: ping.operation,
                request_id: ping.request_id,
                payload: br#"{"alive":true}"#.to_vec(),
            },
        )
        .unwrap();
    });

    let client = std::net::TcpStream::connect(address).unwrap();
    let (mut gateway_client, _) = listener.accept().unwrap();
    let gateway = std::thread::spawn(move || {
        prdb_engine::gateway::serve_tcp_client_connection(
            &mut gateway_client,
            &mut service_gateway,
        )
        .unwrap();
    });

    let mut client = client;
    for (kind, request_id, payload) in [
        (1, 1, br#"{"mode":"query","client_name":"test"}"#.to_vec()),
        (11, 2, b"{}".to_vec()),
    ] {
        let bytes = ClientFrame {
            kind,
            request_id,
            response_sequence: 0,
            payload,
        }
        .encode()
        .unwrap();
        client.write_all(&bytes).unwrap();

        let mut decoder = ClientFrameDecoder::default();
        let mut buffer = [0u8; 1024];
        let response = loop {
            let count = client.read(&mut buffer).unwrap();
            assert_ne!(count, 0, "gateway closed before responding");
            let mut frames = decoder.push(&buffer[..count]).unwrap();
            if let Some(frame) = frames.pop() {
                assert!(frames.is_empty());
                break frame;
            }
        };
        assert_eq!(response.kind, 100);
        assert_eq!(response.request_id, request_id);
        if kind == 11 {
            assert_eq!(response.payload, br#"{"alive":true}"#);
        }
    }
    client.shutdown(std::net::Shutdown::Write).unwrap();
    gateway.join().unwrap();
    supervisor.join().unwrap();
}

#[test]
fn accepted_gateway_connections_obey_a_shared_limit() {
    let listener = TcpGatewayListener::bind(GatewayListenerConfig::default()).unwrap();
    let limiter = GatewayConnectionLimiter::new(1).unwrap();

    let _client_one = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (_accepted_one, _, permit_one) = listener.accept_bounded(&limiter).unwrap();

    let _client_two = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    assert!(listener.accept_bounded(&limiter).is_err());

    drop(permit_one);
    let _client_three = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    assert!(listener.accept_bounded(&limiter).is_ok());
}

#[test]
fn gateway_worker_pool_opens_one_service_stream_per_client() {
    let listener = TcpGatewayListener::bind(GatewayListenerConfig::default()).unwrap();
    let address = listener.local_addr().unwrap();
    let (service_tx, service_rx) = std::sync::mpsc::channel();
    let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let service_shutdown = std::sync::Arc::clone(&shutdown);
    let gateway = std::thread::spawn(move || {
        let config = GatewayRuntimeConfig {
            worker_count: 1,
            queue_capacity: 1,
            idle_timeout: std::time::Duration::from_secs(5),
            accept_poll_interval: std::time::Duration::from_millis(5),
        };
        run_gateway(
            listener,
            config,
            move || {
                let (gateway_service, supervisor_service) = std::os::unix::net::UnixStream::pair()?;
                service_tx.send(supervisor_service).map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::BrokenPipe, "test service closed")
                })?;
                Ok(gateway_service)
            },
            service_shutdown,
        )
        .unwrap();
    });

    let mut client = std::net::TcpStream::connect(address).unwrap();
    let mut service = service_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    client
        .write_all(
            &ClientFrame {
                kind: 1,
                request_id: 1,
                response_sequence: 0,
                payload: br#"{"mode":"query","client_name":"pooled"}"#.to_vec(),
            }
            .encode()
            .unwrap(),
        )
        .unwrap();
    let hello = read_frame(&mut service).unwrap().unwrap();
    assert_eq!(hello.operation, Operation::OpenClientSession as u16);
    write_frame(
        &mut service,
        &IpcFrame {
            operation: hello.operation,
            request_id: hello.request_id,
            payload: br#"{"mode":"query"}"#.to_vec(),
        },
    )
    .unwrap();
    let mut decoder = ClientFrameDecoder::default();
    let mut bytes = [0u8; 256];
    let response = loop {
        let count = client.read(&mut bytes).unwrap();
        assert_ne!(count, 0);
        if let Some(frame) = decoder.push(&bytes[..count]).unwrap().pop() {
            break frame;
        }
    };
    assert_eq!(response.kind, 100);
    assert_eq!(response.request_id, 1);
    client.shutdown(std::net::Shutdown::Write).unwrap();
    drop(service);
    shutdown.store(true, std::sync::atomic::Ordering::Release);
    gateway.join().unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn unix_gateway_connector_checks_peer_uid() {
    use prdb_engine::gateway::UnixGatewayServiceConnector;
    use std::os::unix::net::UnixListener;
    use std::path::PathBuf;

    fn socket_path(label: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "prdb-gateway-{label}-{}-{nonce}.sock",
            std::process::id()
        ))
    }

    let expected_uid = unsafe { libc::geteuid() };
    let accepted_path = socket_path("peer-ok");
    let accepted_listener = UnixListener::bind(&accepted_path).unwrap();
    let accept_thread = std::thread::spawn(move || accepted_listener.accept().unwrap().0);
    let connector = UnixGatewayServiceConnector::new(&accepted_path, expected_uid);
    let stream = connector.connect().unwrap();
    let peer = accept_thread.join().unwrap();
    drop(stream);
    drop(peer);
    std::fs::remove_file(&accepted_path).unwrap();

    let rejected_path = socket_path("peer-denied");
    let rejected_listener = UnixListener::bind(&rejected_path).unwrap();
    let accept_thread = std::thread::spawn(move || rejected_listener.accept().unwrap().0);
    let connector = UnixGatewayServiceConnector::new(&rejected_path, expected_uid.wrapping_add(1));
    assert!(connector.connect().is_err());
    drop(accept_thread.join().unwrap());
    std::fs::remove_file(&rejected_path).unwrap();
}

#[test]
fn gateway_binary_exposes_its_explicit_listener_and_service_options() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_prdb-gateway"))
        .arg("--help")
        .output()
        .unwrap();

    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    assert!(help.contains("--bind"));
    assert!(help.contains("--service-socket"));
    assert!(help.contains("--service-uid"));
}

#[test]
fn gateway_binary_rejects_plaintext_wildcard_without_opt_in() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_prdb-gateway"))
        .args([
            "--bind",
            "0.0.0.0:0",
            "--service-socket",
            "/tmp/prdb-unused.sock",
            "--service-uid",
            "0",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let error = String::from_utf8(output.stderr).unwrap();
    assert!(error.contains("requires explicit opt-in"));
}
