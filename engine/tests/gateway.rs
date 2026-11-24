use prdb_engine::client_protocol::{ClientFrame, ClientFrameDecoder};
use prdb_engine::gateway::{
    GatewayAuthFailureLimiter, GatewayConnectionLimiter, GatewayListenerConfig,
    GatewayListenerError, GatewayPeerRateLimiter, GatewayRuntimeConfig, TcpGatewayListener,
    run_gateway,
};
use prdb_engine::ipc::{IpcFrame, Operation, read_frame, write_frame};
use std::io::{BufRead, Read, Write};
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
        assert_eq!(ping.payload, [vec![1], b"{}".to_vec()].concat());
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
fn gateway_bridge_returns_sanitized_supervisor_error_frames() {
    let canonical_errors = [
        br#"{"code":"invalid_request","message":"Request is invalid."}"#.as_slice(),
        br#"{"code":"invalid_state","message":"Request is not valid in the current session state."}"#.as_slice(),
        br#"{"code":"unauthenticated","message":"Authentication is required."}"#.as_slice(),
        br#"{"code":"resource_limit","message":"Request exceeds a configured limit."}"#.as_slice(),
        br#"{"code":"unavailable","message":"Requested resource is unavailable."}"#.as_slice(),
    ];

    for expected_payload in canonical_errors {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let (mut gateway_service, mut supervisor_service) =
            std::os::unix::net::UnixStream::pair().unwrap();
        let expected_payload = expected_payload.to_vec();
        let supervisor_payload = expected_payload.clone();
        let supervisor = std::thread::spawn(move || {
            let request = read_frame(&mut supervisor_service).unwrap().unwrap();
            write_frame(
                &mut supervisor_service,
                &IpcFrame {
                    operation: Operation::GatewayClientError as u16,
                    request_id: request.request_id,
                    payload: supervisor_payload,
                },
            )
            .unwrap();
        });
        let gateway = std::thread::spawn(move || {
            let (mut client, _) = listener.accept().unwrap();
            prdb_engine::gateway::serve_tcp_client_connection(&mut client, &mut gateway_service)
                .unwrap();
        });

        let mut client = std::net::TcpStream::connect(address).unwrap();
        client
            .write_all(
                &ClientFrame {
                    kind: 10,
                    request_id: 7,
                    response_sequence: 0,
                    payload: b"{}".to_vec(),
                }
                .encode()
                .unwrap(),
            )
            .unwrap();
        let mut decoder = ClientFrameDecoder::default();
        let mut buffer = [0u8; 256];
        let response = loop {
            let count = client.read(&mut buffer).unwrap();
            assert_ne!(count, 0);
            if let Some(response) = decoder.push(&buffer[..count]).unwrap().pop() {
                break response;
            }
        };
        assert_eq!(
            response.kind,
            prdb_engine::client_protocol::CLIENT_ERROR_KIND
        );
        assert_eq!(response.request_id, 7);
        assert_eq!(response.payload, expected_payload);
        assert_eq!(client.read(&mut buffer).unwrap(), 0);
        gateway.join().unwrap();
        supervisor.join().unwrap();
    }
}

#[test]
fn gateway_rejects_unsanitized_supervisor_error_payloads() {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let (mut gateway_service, mut supervisor_service) =
        std::os::unix::net::UnixStream::pair().unwrap();
    let supervisor = std::thread::spawn(move || {
        let request = read_frame(&mut supervisor_service).unwrap().unwrap();
        write_frame(
            &mut supervisor_service,
            &IpcFrame {
                operation: Operation::GatewayClientError as u16,
                request_id: request.request_id,
                payload: br#"{"code":"internal","message":"secret principal detail"}"#.to_vec(),
            },
        )
        .unwrap();
    });
    let gateway = std::thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        prdb_engine::gateway::serve_tcp_client_connection(&mut client, &mut gateway_service)
    });

    let mut client = std::net::TcpStream::connect(address).unwrap();
    client
        .write_all(
            &ClientFrame {
                kind: 10,
                request_id: 7,
                response_sequence: 0,
                payload: b"{}".to_vec(),
            }
            .encode()
            .unwrap(),
        )
        .unwrap();
    let mut buffer = [0u8; 256];
    assert_eq!(client.read(&mut buffer).unwrap(), 0);
    assert!(matches!(
        gateway.join().unwrap(),
        Err(prdb_engine::gateway::GatewayConnectionError::MismatchedServiceResponse)
    ));
    supervisor.join().unwrap();
}

#[test]
fn gateway_rejects_error_frames_with_mismatched_request_ids() {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let (mut gateway_service, mut supervisor_service) =
        std::os::unix::net::UnixStream::pair().unwrap();
    let supervisor = std::thread::spawn(move || {
        let request = read_frame(&mut supervisor_service).unwrap().unwrap();
        write_frame(
            &mut supervisor_service,
            &IpcFrame {
                operation: Operation::GatewayClientError as u16,
                request_id: request.request_id + 1,
                payload: br#"{"code":"invalid_request","message":"Request is invalid."}"#.to_vec(),
            },
        )
        .unwrap();
    });
    let gateway = std::thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        prdb_engine::gateway::serve_tcp_client_connection(&mut client, &mut gateway_service)
    });

    let mut client = std::net::TcpStream::connect(address).unwrap();
    client
        .write_all(
            &ClientFrame {
                kind: 10,
                request_id: 7,
                response_sequence: 0,
                payload: b"{}".to_vec(),
            }
            .encode()
            .unwrap(),
        )
        .unwrap();
    let mut buffer = [0u8; 256];
    assert_eq!(client.read(&mut buffer).unwrap(), 0);
    assert!(matches!(
        gateway.join().unwrap(),
        Err(prdb_engine::gateway::GatewayConnectionError::MismatchedServiceResponse)
    ));
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
            worker_count: 2,
            queue_capacity: 2,
            control_worker_count: 1,
            control_queue_capacity: 1,
            connections_per_ip_per_minute: 60,
            auth_failures_per_ip_per_minute: 10,
            idle_timeout: std::time::Duration::from_secs(5),
            pre_auth_timeout: std::time::Duration::from_secs(2),
            accept_poll_interval: std::time::Duration::from_millis(5),
            max_total_result_bytes: prdb_engine::query_abi::MAX_TOTAL_RESULT_BYTES,
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

    let mut clients = [
        std::net::TcpStream::connect(address).unwrap(),
        std::net::TcpStream::connect(address).unwrap(),
    ];
    for (client, name) in clients.iter_mut().zip(["first", "second"]) {
        client
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        client
            .write_all(
                &ClientFrame {
                    kind: 1,
                    request_id: 1,
                    response_sequence: 0,
                    payload: format!("{{\"mode\":\"query\",\"client_name\":\"{name}\"}}")
                        .into_bytes(),
                }
                .encode()
                .unwrap(),
            )
            .unwrap();
    }
    let mut services = (0..2)
        .map(|_| {
            service_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap()
        })
        .collect::<Vec<_>>();
    for service in &mut services {
        let hello = read_frame(service).unwrap().unwrap();
        assert_eq!(hello.operation, Operation::OpenClientSession as u16);
        assert_eq!(hello.request_id, 1);
        let response = if hello.payload.windows(5).any(|part| part == b"first") {
            br#"{"client":"first"}"#.to_vec()
        } else {
            assert!(hello.payload.windows(6).any(|part| part == b"second"));
            br#"{"client":"second"}"#.to_vec()
        };
        write_frame(
            service,
            &IpcFrame {
                operation: hello.operation,
                request_id: hello.request_id,
                payload: response,
            },
        )
        .unwrap();
    }
    for (client, expected) in clients.iter_mut().zip(["first", "second"]) {
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
        assert_eq!(
            response.payload,
            format!("{{\"client\":\"{expected}\"}}").as_bytes()
        );
        client.shutdown(std::net::Shutdown::Write).unwrap();
    }
    drop(services);
    shutdown.store(true, std::sync::atomic::Ordering::Release);
    gateway.join().unwrap();
}

#[test]
fn stalled_partial_frame_does_not_block_another_gateway_connection() {
    let listener = TcpGatewayListener::bind(GatewayListenerConfig::default()).unwrap();
    let address = listener.local_addr().unwrap();
    let (service_tx, service_rx) = std::sync::mpsc::channel();
    let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let service_shutdown = std::sync::Arc::clone(&shutdown);
    let gateway = std::thread::spawn(move || {
        let config = GatewayRuntimeConfig {
            worker_count: 2,
            queue_capacity: 2,
            control_worker_count: 1,
            control_queue_capacity: 1,
            connections_per_ip_per_minute: 60,
            auth_failures_per_ip_per_minute: 10,
            idle_timeout: std::time::Duration::from_secs(5),
            pre_auth_timeout: std::time::Duration::from_secs(2),
            accept_poll_interval: std::time::Duration::from_millis(5),
            max_total_result_bytes: prdb_engine::query_abi::MAX_TOTAL_RESULT_BYTES,
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

    let mut stalled_client = std::net::TcpStream::connect(address).unwrap();
    let stalled_service = service_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("gateway should assign a worker to the first connection");
    let partial_frame = ClientFrame {
        kind: 1,
        request_id: 1,
        response_sequence: 0,
        payload: br#"{"mode":"query","client_name":"stalled"}"#.to_vec(),
    }
    .encode()
    .unwrap();
    stalled_client.write_all(&partial_frame[..12]).unwrap();

    let mut active_client = std::net::TcpStream::connect(address).unwrap();
    active_client
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    active_client
        .write_all(
            &ClientFrame {
                kind: 1,
                request_id: 1,
                response_sequence: 0,
                payload: br#"{"mode":"query","client_name":"active"}"#.to_vec(),
            }
            .encode()
            .unwrap(),
        )
        .unwrap();

    let mut active_service = service_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("second connection should receive an independent service stream");
    let hello = read_frame(&mut active_service).unwrap().unwrap();
    assert_eq!(hello.operation, Operation::OpenClientSession as u16);
    assert!(hello.payload.windows(6).any(|part| part == b"active"));
    write_frame(
        &mut active_service,
        &IpcFrame {
            operation: hello.operation,
            request_id: hello.request_id,
            payload: br#"{"accepted":true}"#.to_vec(),
        },
    )
    .unwrap();

    let mut decoder = ClientFrameDecoder::default();
    let mut buffer = [0u8; 256];
    let response = loop {
        let count = active_client.read(&mut buffer).unwrap();
        assert_ne!(count, 0, "active connection closed before HELLO response");
        if let Some(frame) = decoder.push(&buffer[..count]).unwrap().pop() {
            break frame;
        }
    };
    assert_eq!(response.kind, 100);
    assert_eq!(response.request_id, 1);
    assert_eq!(response.payload, br#"{"accepted":true}"#);

    drop(stalled_service);
    drop(stalled_client);
    active_client.shutdown(std::net::Shutdown::Write).unwrap();
    drop(active_service);
    shutdown.store(true, std::sync::atomic::Ordering::Release);
    gateway.join().unwrap();
}

#[test]
fn gateway_binary_relays_tcp_hello_to_its_private_unix_service() {
    struct GatewayChild(std::process::Child);

    impl Drop for GatewayChild {
        fn drop(&mut self) {
            if self.0.try_wait().ok().flatten().is_none() {
                let _ = self.0.kill();
            }
            let _ = self.0.wait();
        }
    }

    let socket_path = std::env::temp_dir().join(format!(
        "prdb-gateway-process-{}-{}.sock",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let service_listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    service_listener.set_nonblocking(true).unwrap();
    // SAFETY: geteuid has no preconditions and only reads the current effective UID.
    let service_uid = unsafe { libc::geteuid() }.to_string();
    let mut child = GatewayChild(
        std::process::Command::new(env!("CARGO_BIN_EXE_prdb-gateway"))
            .args([
                "--bind",
                "127.0.0.1:0",
                "--control-bind",
                "127.0.0.1:0",
                "--service-socket",
                socket_path.to_str().unwrap(),
                "--service-uid",
                &service_uid,
                "--workers",
                "2",
                "--queue-capacity",
                "2",
                "--control-workers",
                "1",
                "--control-queue-capacity",
                "1",
                "--idle-timeout-ms",
                "2000",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stderr = child.0.stderr.take().unwrap();
    let mut stderr = std::io::BufReader::new(stderr);
    let mut startup = String::new();
    assert_ne!(stderr.read_line(&mut startup).unwrap(), 0);
    let address = startup
        .strip_prefix("prdb-gateway listening on ")
        .unwrap()
        .trim()
        .parse::<SocketAddr>()
        .unwrap();
    let mut control_startup = String::new();
    assert_ne!(stderr.read_line(&mut control_startup).unwrap(), 0);
    let control_address = control_startup
        .strip_prefix("prdb-gateway control listening on ")
        .unwrap()
        .trim()
        .parse::<SocketAddr>()
        .unwrap();

    let mut client = std::net::TcpStream::connect(address).unwrap();
    client
        .write_all(
            &ClientFrame {
                kind: 1,
                request_id: 1,
                response_sequence: 0,
                payload: br#"{"mode":"query","client_name":"process-smoke"}"#.to_vec(),
            }
            .encode()
            .unwrap(),
        )
        .unwrap();
    let service_deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let (mut service, _) = loop {
        match service_listener.accept() {
            Ok(accepted) => break accepted,
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    && std::time::Instant::now() < service_deadline =>
            {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            Err(error) => panic!("gateway process did not connect to service socket: {error}"),
        }
    };
    service
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
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
    let mut buffer = [0u8; 256];
    let response = loop {
        let count = client.read(&mut buffer).unwrap();
        assert_ne!(count, 0, "gateway process closed before HELLO response");
        if let Some(frame) = decoder.push(&buffer[..count]).unwrap().pop() {
            break frame;
        }
    };
    assert_eq!(response.kind, 100);
    assert_eq!(response.request_id, 1);
    assert_eq!(response.payload, br#"{"mode":"query"}"#);
    client.shutdown(std::net::Shutdown::Write).unwrap();
    assert_eq!(client.read(&mut buffer).unwrap(), 0);

    let mut control_client = std::net::TcpStream::connect(control_address).unwrap();
    control_client
        .write_all(
            &ClientFrame {
                kind: 7,
                request_id: 2,
                response_sequence: 0,
                payload: b"{}".to_vec(),
            }
            .encode()
            .unwrap(),
        )
        .unwrap();
    let mut control_decoder = ClientFrameDecoder::default();
    let control_response = loop {
        let count = control_client.read(&mut buffer).unwrap();
        assert_ne!(count, 0, "control listener closed before rejecting START");
        if let Some(frame) = control_decoder.push(&buffer[..count]).unwrap().pop() {
            break frame;
        }
    };
    assert_eq!(
        control_response.kind,
        prdb_engine::client_protocol::CLIENT_ERROR_KIND
    );
    assert_eq!(control_response.request_id, 2);
    assert_eq!(
        control_response.payload,
        br#"{"code":"invalid_request","message":"Request is invalid."}"#
    );
    control_client.shutdown(std::net::Shutdown::Write).unwrap();
    drop(client);
    drop(control_client);
    drop(service);

    // SAFETY: the PID belongs to the child process above and SIGTERM is valid.
    assert_eq!(
        unsafe { libc::kill(child.0.id() as libc::pid_t, libc::SIGTERM) },
        0
    );
    assert!(child.0.wait().unwrap().success());
    std::fs::remove_file(socket_path).unwrap();
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
    assert!(help.contains("--control-bind"));
    assert!(help.contains("--control-workers"));
    assert!(help.contains("--service-socket"));
    assert!(help.contains("--service-uid"));
    assert!(help.contains("--auth-failures-per-ip"));
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

#[test]
fn incomplete_tcp_client_frame_hits_absolute_read_deadline() {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let (mut gateway_service, _supervisor_service) =
        std::os::unix::net::UnixStream::pair().unwrap();
    let gateway = std::thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        prdb_engine::gateway::serve_tcp_client_connection_with_timeout(
            &mut client,
            &mut gateway_service,
            std::time::Duration::from_millis(100),
        )
    });
    let mut client = std::net::TcpStream::connect(address).unwrap();
    client.write_all(b"P").unwrap();
    let started = std::time::Instant::now();
    let result = gateway.join().unwrap();

    assert!(result.is_err());
    assert!(started.elapsed() < std::time::Duration::from_secs(2));
}

#[test]
fn per_ip_gateway_rate_limit_is_bounded_and_expires() {
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::{Duration, Instant};

    let limiter = GatewayPeerRateLimiter::new(2, 1, Duration::from_secs(60)).unwrap();
    let now = Instant::now();
    let first_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let other_ip = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));

    assert!(limiter.admit_at(first_ip, now));
    assert!(limiter.admit_at(first_ip, now + Duration::from_secs(1)));
    assert!(!limiter.admit_at(first_ip, now + Duration::from_secs(2)));
    assert!(!limiter.admit_at(other_ip, now + Duration::from_secs(2)));
    assert!(limiter.admit_at(other_ip, now + Duration::from_secs(61)));
    assert!(!limiter.admit_at(first_ip, now + Duration::from_secs(61)));
}

#[test]
fn failed_auth_budget_is_bounded_per_ip_and_expires() {
    use std::time::{Duration, Instant};

    let limiter = GatewayAuthFailureLimiter::new(2, 1, Duration::from_secs(60)).unwrap();
    let now = Instant::now();
    let source = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let other = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 4));

    assert!(limiter.allows_attempt_at(source, now));
    assert!(limiter.record_failure_at(source, now));
    assert!(limiter.allows_attempt_at(source, now + Duration::from_secs(1)));
    assert!(limiter.record_failure_at(source, now + Duration::from_secs(1)));
    assert!(!limiter.allows_attempt_at(source, now + Duration::from_secs(2)));
    assert!(!limiter.allows_attempt_at(other, now + Duration::from_secs(2)));
    assert!(limiter.allows_attempt_at(other, now + Duration::from_secs(61)));
}

#[test]
fn gateway_stops_forwarding_auth_after_failed_attempt_budget() {
    use std::time::Duration;

    let listener = TcpGatewayListener::bind(GatewayListenerConfig::default()).unwrap();
    let address = listener.local_addr().unwrap();
    let (service_tx, service_rx) = std::sync::mpsc::channel();
    let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let gateway_shutdown = std::sync::Arc::clone(&shutdown);
    let gateway = std::thread::spawn(move || {
        run_gateway(
            listener,
            GatewayRuntimeConfig {
                worker_count: 1,
                queue_capacity: 1,
                control_worker_count: 1,
                control_queue_capacity: 1,
                connections_per_ip_per_minute: 60,
                auth_failures_per_ip_per_minute: 1,
                idle_timeout: Duration::from_secs(3),
                pre_auth_timeout: Duration::from_secs(3),
                accept_poll_interval: Duration::from_millis(5),
                max_total_result_bytes: prdb_engine::query_abi::MAX_TOTAL_RESULT_BYTES,
            },
            move || {
                let (gateway_service, supervisor_service) = std::os::unix::net::UnixStream::pair()?;
                service_tx.send(supervisor_service).map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::BrokenPipe, "test service closed")
                })?;
                Ok(gateway_service)
            },
            gateway_shutdown,
        )
        .unwrap();
    });

    let mut client = std::net::TcpStream::connect(address).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let mut service = service_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    let supervisor = std::thread::spawn(move || {
        let hello = read_frame(&mut service).unwrap().unwrap();
        write_frame(
            &mut service,
            &IpcFrame {
                operation: hello.operation,
                request_id: hello.request_id,
                payload: br#"{"mode":"query"}"#.to_vec(),
            },
        )
        .unwrap();
        let auth = read_frame(&mut service).unwrap().unwrap();
        assert_eq!(auth.operation, Operation::AuthenticateSession as u16);
        write_frame(
            &mut service,
            &IpcFrame {
                operation: auth.operation,
                request_id: auth.request_id,
                payload: br#"{"authenticated":false}"#.to_vec(),
            },
        )
        .unwrap();
        assert!(read_frame(&mut service).unwrap().is_none());
    });

    let mut decoder = ClientFrameDecoder::default();
    let send_and_read =
        |client: &mut std::net::TcpStream, decoder: &mut ClientFrameDecoder, kind, request_id| {
            client
                .write_all(
                    &ClientFrame {
                        kind,
                        request_id,
                        response_sequence: 0,
                        payload: if kind == 1 {
                            br#"{"mode":"query","client_name":"auth-limit"}"#.to_vec()
                        } else {
                            b"{}".to_vec()
                        },
                    }
                    .encode()
                    .unwrap(),
                )
                .unwrap();
            let mut buffer = [0u8; 1024];
            loop {
                let count = client.read(&mut buffer).unwrap();
                assert_ne!(count, 0, "gateway closed before the response");
                if let Some(frame) = decoder.push(&buffer[..count]).unwrap().pop() {
                    return frame;
                }
            }
        };
    assert_eq!(send_and_read(&mut client, &mut decoder, 1, 1).request_id, 1);
    assert_eq!(
        send_and_read(&mut client, &mut decoder, 2, 2).payload,
        br#"{"authenticated":false}"#
    );
    client
        .write_all(
            &ClientFrame {
                kind: 2,
                request_id: 3,
                response_sequence: 0,
                payload: b"{}".to_vec(),
            }
            .encode()
            .unwrap(),
        )
        .unwrap();
    let mut byte = [0u8; 1];
    let count = client.read(&mut byte).unwrap();
    assert_ne!(count, 0);
    let error = decoder.push(&byte[..count]).unwrap();
    let mut response = error;
    while response.is_empty() {
        let count = client.read(&mut byte).unwrap();
        assert_ne!(count, 0);
        response.extend(decoder.push(&byte[..count]).unwrap());
    }
    assert_eq!(response.len(), 1);
    assert_eq!(
        response[0].kind,
        prdb_engine::client_protocol::CLIENT_ERROR_KIND
    );
    assert_eq!(response[0].request_id, 3);
    assert_eq!(
        response[0].payload,
        br#"{"code":"unauthenticated","message":"Authentication is required."}"#
    );
    assert_eq!(client.read(&mut byte).unwrap(), 0);

    supervisor.join().unwrap();
    shutdown.store(true, std::sync::atomic::Ordering::Release);
    gateway.join().unwrap();
}

#[test]
fn unauthenticated_gateway_phase_has_one_absolute_deadline() {
    use std::time::Duration;

    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let (mut gateway_service, mut supervisor_service) =
        std::os::unix::net::UnixStream::pair().unwrap();
    let supervisor = std::thread::spawn(move || {
        let hello = read_frame(&mut supervisor_service).unwrap().unwrap();
        write_frame(
            &mut supervisor_service,
            &IpcFrame {
                operation: hello.operation,
                request_id: hello.request_id,
                payload: br#"{"mode":"query"}"#.to_vec(),
            },
        )
        .unwrap();
        assert!(read_frame(&mut supervisor_service).unwrap().is_none());
    });
    let gateway = std::thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        prdb_engine::gateway::serve_tcp_client_connection_with_timeouts(
            &mut client,
            &mut gateway_service,
            Duration::from_secs(1),
            Duration::from_millis(100),
        )
    });
    let mut client = std::net::TcpStream::connect(address).unwrap();
    client
        .write_all(
            &ClientFrame {
                kind: 1,
                request_id: 1,
                response_sequence: 0,
                payload: br#"{"mode":"query","client_name":"deadline"}"#.to_vec(),
            }
            .encode()
            .unwrap(),
        )
        .unwrap();
    let mut decoder = ClientFrameDecoder::default();
    let mut buffer = [0u8; 256];
    loop {
        let count = client.read(&mut buffer).unwrap();
        assert_ne!(count, 0);
        if !decoder.push(&buffer[..count]).unwrap().is_empty() {
            break;
        }
    }
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(client.read(&mut buffer).unwrap(), 0);
    assert!(gateway.join().unwrap().is_err());
    supervisor.join().unwrap();
}

#[test]
fn start_gateway_bridge_relays_provisional_data_then_terminal() {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let (mut gateway_service, mut supervisor_service) =
        std::os::unix::net::UnixStream::pair().unwrap();
    let supervisor = std::thread::spawn(move || {
        let start = read_frame(&mut supervisor_service).unwrap().unwrap();
        assert_eq!(start.operation, Operation::StartSubmission as u16);
        write_frame(
            &mut supervisor_service,
            &IpcFrame {
                operation: start.operation,
                request_id: start.request_id,
                payload: br#"{"state":"accepted"}"#.to_vec(),
            },
        )
        .unwrap();
        write_frame(
            &mut supervisor_service,
            &IpcFrame {
                operation: Operation::GatewayResultData as u16,
                request_id: start.request_id,
                payload: b"provisional bytes".to_vec(),
            },
        )
        .unwrap();
        write_frame(
            &mut supervisor_service,
            &IpcFrame {
                operation: Operation::GatewayTerminal as u16,
                request_id: start.request_id,
                payload: br#"{"outcome":"committed"}"#.to_vec(),
            },
        )
        .unwrap();
    });
    let gateway = std::thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        prdb_engine::gateway::serve_tcp_client_connection(&mut client, &mut gateway_service)
            .unwrap();
    });

    let mut client = std::net::TcpStream::connect(address).unwrap();
    client
        .write_all(
            &ClientFrame {
                kind: 7,
                request_id: 8,
                response_sequence: 0,
                payload: b"{}".to_vec(),
            }
            .encode()
            .unwrap(),
        )
        .unwrap();
    let mut decoder = ClientFrameDecoder::default();
    let mut buffer = [0u8; 256];
    let mut responses = Vec::new();
    while responses.len() < 3 {
        let count = client.read(&mut buffer).unwrap();
        assert_ne!(count, 0);
        responses.extend(decoder.push(&buffer[..count]).unwrap());
    }
    assert_eq!(responses[0].kind, 100);
    assert_eq!(responses[0].response_sequence, 0);
    assert_eq!(responses[1].kind, 102);
    assert_eq!(responses[1].request_id, 8);
    assert_eq!(responses[1].response_sequence, 1);
    assert_eq!(responses[1].payload, b"provisional bytes");
    assert_eq!(responses[2].kind, 103);
    assert_eq!(responses[2].request_id, 8);
    assert_eq!(responses[2].response_sequence, 2);
    client.shutdown(std::net::Shutdown::Write).unwrap();
    gateway.join().unwrap();
    supervisor.join().unwrap();
}

#[test]
fn gateway_forwards_admin_request_and_correlated_response_over_tcp() {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let (mut gateway_service, mut supervisor_service) =
        std::os::unix::net::UnixStream::pair().unwrap();
    let admin_payload = br#"{"operation":"list_schemas"}"#.to_vec();
    let supervisor_payload = br#"{"schemas":["public"]}"#.to_vec();
    let supervisor = std::thread::spawn(move || {
        let request = read_frame(&mut supervisor_service).unwrap().unwrap();
        assert_eq!(
            request.operation,
            Operation::SubmitAdministrativeOperation as u16
        );
        assert_eq!(request.request_id, 15);
        assert_eq!(request.payload, admin_payload);
        write_frame(
            &mut supervisor_service,
            &IpcFrame {
                operation: request.operation,
                request_id: request.request_id,
                payload: supervisor_payload,
            },
        )
        .unwrap();
    });
    let gateway = std::thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        prdb_engine::gateway::serve_tcp_client_connection(&mut client, &mut gateway_service)
            .unwrap();
    });
    let mut client = std::net::TcpStream::connect(address).unwrap();
    client
        .write_all(
            &ClientFrame {
                kind: 10,
                request_id: 15,
                response_sequence: 0,
                payload: br#"{"operation":"list_schemas"}"#.to_vec(),
            }
            .encode()
            .unwrap(),
        )
        .unwrap();
    let mut decoder = ClientFrameDecoder::default();
    let mut buffer = [0u8; 256];
    let response = loop {
        let count = client.read(&mut buffer).unwrap();
        assert_ne!(count, 0, "gateway disconnected before ADMIN response");
        if let Some(response) = decoder.push(&buffer[..count]).unwrap().pop() {
            break response;
        }
    };
    assert_eq!(response.kind, 100);
    assert_eq!(response.request_id, 15);
    assert_eq!(response.response_sequence, 0);
    assert_eq!(response.payload, br#"{"schemas":["public"]}"#);
    client.shutdown(std::net::Shutdown::Write).unwrap();
    assert_eq!(client.read(&mut buffer).unwrap(), 0);
    gateway.join().unwrap();
    supervisor.join().unwrap();
}

#[test]
fn status_on_another_gateway_connection_progresses_while_result_worker_is_blocked() {
    const RESULT_FRAME_BYTES: usize = 1024 * 1024;
    const RESULT_FRAME_COUNT: usize = 8;

    fn set_receive_buffer_size(stream: &std::net::TcpStream, size: libc::c_int) {
        // SAFETY: the stream owns a live socket descriptor and `size` is an integer option value.
        let result = unsafe {
            libc::setsockopt(
                std::os::fd::AsRawFd::as_raw_fd(stream),
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                (&size as *const libc::c_int).cast(),
                std::mem::size_of_val(&size) as libc::socklen_t,
            )
        };
        assert_eq!(result, 0);
    }

    let listener = TcpGatewayListener::bind(GatewayListenerConfig::default()).unwrap();
    let address = listener.local_addr().unwrap();
    let control_listener = TcpGatewayListener::bind(GatewayListenerConfig::default()).unwrap();
    let control_address = control_listener.local_addr().unwrap();
    let (service_streams_tx, service_streams_rx) = std::sync::mpsc::channel();
    let connector = move || {
        let (gateway, supervisor) = std::os::unix::net::UnixStream::pair()?;
        service_streams_tx
            .send(supervisor)
            .map_err(|_| std::io::Error::other("test service receiver disconnected"))?;
        Ok(gateway)
    };
    let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let gateway_shutdown = std::sync::Arc::clone(&shutdown);
    let gateway = std::thread::spawn(move || {
        prdb_engine::gateway::run_gateway_with_control(
            listener,
            control_listener,
            GatewayRuntimeConfig {
                worker_count: 1,
                queue_capacity: 1,
                control_worker_count: 1,
                control_queue_capacity: 1,
                connections_per_ip_per_minute: 60,
                auth_failures_per_ip_per_minute: 10,
                idle_timeout: std::time::Duration::from_secs(2),
                pre_auth_timeout: std::time::Duration::from_secs(1),
                accept_poll_interval: std::time::Duration::from_millis(1),
                max_total_result_bytes: prdb_engine::query_abi::MAX_TOTAL_RESULT_BYTES,
            },
            connector,
            gateway_shutdown,
        )
    });

    let mut stalled_client = std::net::TcpStream::connect(address).unwrap();
    set_receive_buffer_size(&stalled_client, 256 * 1024);
    stalled_client
        .write_all(
            &ClientFrame {
                kind: 7,
                request_id: 1,
                response_sequence: 0,
                payload: b"{}".to_vec(),
            }
            .encode()
            .unwrap(),
        )
        .unwrap();
    let mut result_service = service_streams_rx.recv().unwrap();
    let start = read_frame(&mut result_service).unwrap().unwrap();
    assert_eq!(start.operation, Operation::StartSubmission as u16);
    write_frame(
        &mut result_service,
        &IpcFrame {
            operation: start.operation,
            request_id: start.request_id,
            payload: br#"{"state":"accepted"}"#.to_vec(),
        },
    )
    .unwrap();
    let (producer_finished_tx, producer_finished_rx) = std::sync::mpsc::channel();
    let result_producer = std::thread::spawn(move || {
        let payload = vec![b'z'; RESULT_FRAME_BYTES];
        for _ in 0..RESULT_FRAME_COUNT {
            write_frame(
                &mut result_service,
                &IpcFrame {
                    operation: Operation::GatewayResultData as u16,
                    request_id: start.request_id,
                    payload: payload.clone(),
                },
            )
            .unwrap();
        }
        write_frame(
            &mut result_service,
            &IpcFrame {
                operation: Operation::GatewayTerminal as u16,
                request_id: start.request_id,
                payload: br#"{"outcome":"committed"}"#.to_vec(),
            },
        )
        .unwrap();
        producer_finished_tx.send(()).unwrap();
    });
    assert!(matches!(
        producer_finished_rx.recv_timeout(std::time::Duration::from_millis(100)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));

    let mut status_client = std::net::TcpStream::connect(control_address).unwrap();
    status_client
        .write_all(
            &ClientFrame {
                kind: 8,
                request_id: 2,
                response_sequence: 0,
                payload: b"{}".to_vec(),
            }
            .encode()
            .unwrap(),
        )
        .unwrap();
    let mut status_service = service_streams_rx.recv().unwrap();
    let status_request = read_frame(&mut status_service).unwrap().unwrap();
    assert_eq!(
        status_request.operation,
        Operation::ClientGetInvocationStatus as u16
    );
    assert_eq!(status_request.request_id, 2);
    write_frame(
        &mut status_service,
        &IpcFrame {
            operation: status_request.operation,
            request_id: status_request.request_id,
            payload: br#"{"status":"active"}"#.to_vec(),
        },
    )
    .unwrap();
    let mut status_decoder = ClientFrameDecoder::default();
    let mut buffer = [0u8; 256];
    let status_response = loop {
        let count = status_client.read(&mut buffer).unwrap();
        assert_ne!(count, 0, "status connection closed without a response");
        if let Some(frame) = status_decoder.push(&buffer[..count]).unwrap().pop() {
            break frame;
        }
    };
    assert_eq!(status_response.kind, 100);
    assert_eq!(status_response.request_id, 2);
    assert_eq!(status_response.payload, br#"{"status":"active"}"#);
    producer_finished_rx
        .recv_timeout(std::time::Duration::from_millis(100))
        .unwrap_err();

    let mut result_decoder = ClientFrameDecoder::default();
    let mut result_buffer = [0u8; 256 * 1024];
    let mut accepted = false;
    let mut data_frames = 0usize;
    let mut terminal = false;
    while !terminal {
        let count = stalled_client.read(&mut result_buffer).unwrap();
        assert_ne!(count, 0, "result connection closed before terminal");
        for frame in result_decoder.push(&result_buffer[..count]).unwrap() {
            if frame.kind == 100 {
                accepted = true;
            } else if frame.kind == 102 {
                data_frames += 1;
                assert_eq!(frame.request_id, 1);
                assert_eq!(frame.payload.len(), RESULT_FRAME_BYTES);
                assert!(frame.payload.iter().all(|byte| *byte == b'z'));
            } else if frame.kind == 103 {
                terminal = true;
                assert_eq!(frame.request_id, 1);
            } else {
                panic!("unexpected result frame kind {}", frame.kind);
            }
        }
    }
    assert!(accepted);
    assert_eq!(data_frames, RESULT_FRAME_COUNT);
    producer_finished_rx.recv().unwrap();

    status_client.shutdown(std::net::Shutdown::Write).unwrap();
    stalled_client.shutdown(std::net::Shutdown::Write).unwrap();
    shutdown.store(true, std::sync::atomic::Ordering::Release);
    gateway.join().unwrap().unwrap();
    result_producer.join().unwrap();
}

#[test]
fn slow_tcp_result_consumer_backpressures_supervisor_service_writer() {
    const RESULT_FRAME_BYTES: usize = 256 * 1024;
    const RESULT_FRAME_COUNT: usize = 16;

    fn set_socket_buffer_size(
        stream: &std::net::TcpStream,
        option: libc::c_int,
        size: libc::c_int,
    ) {
        // SAFETY: the stream owns a live socket descriptor and `size` is an integer option value.
        let result = unsafe {
            libc::setsockopt(
                std::os::fd::AsRawFd::as_raw_fd(stream),
                libc::SOL_SOCKET,
                option,
                (&size as *const libc::c_int).cast(),
                std::mem::size_of_val(&size) as libc::socklen_t,
            )
        };
        assert_eq!(
            result,
            0,
            "setsockopt failed: {}",
            std::io::Error::last_os_error()
        );
    }

    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let (mut gateway_service, mut supervisor_service) =
        std::os::unix::net::UnixStream::pair().unwrap();
    let (producer_finished_tx, producer_finished_rx) = std::sync::mpsc::channel();
    let supervisor = std::thread::spawn(move || {
        let start = read_frame(&mut supervisor_service).unwrap().unwrap();
        assert_eq!(start.operation, Operation::StartSubmission as u16);
        write_frame(
            &mut supervisor_service,
            &IpcFrame {
                operation: start.operation,
                request_id: start.request_id,
                payload: br#"{"state":"accepted"}"#.to_vec(),
            },
        )
        .unwrap();
        let payload = vec![b'x'; RESULT_FRAME_BYTES];
        for _ in 0..RESULT_FRAME_COUNT {
            write_frame(
                &mut supervisor_service,
                &IpcFrame {
                    operation: Operation::GatewayResultData as u16,
                    request_id: start.request_id,
                    payload: payload.clone(),
                },
            )
            .unwrap();
        }
        write_frame(
            &mut supervisor_service,
            &IpcFrame {
                operation: Operation::GatewayTerminal as u16,
                request_id: start.request_id,
                payload: br#"{"outcome":"committed"}"#.to_vec(),
            },
        )
        .unwrap();
        producer_finished_tx.send(()).unwrap();
    });
    let gateway = std::thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        set_socket_buffer_size(&client, libc::SO_SNDBUF, 64 * 1024);
        prdb_engine::gateway::serve_tcp_client_connection(&mut client, &mut gateway_service)
            .unwrap();
    });

    let mut client = std::net::TcpStream::connect(address).unwrap();
    set_socket_buffer_size(&client, libc::SO_RCVBUF, 256 * 1024);
    client
        .write_all(
            &ClientFrame {
                kind: 7,
                request_id: 8,
                response_sequence: 0,
                payload: b"{}".to_vec(),
            }
            .encode()
            .unwrap(),
        )
        .unwrap();

    let mut decoder = ClientFrameDecoder::default();
    let mut buffer = [0u8; 256 * 1024];
    let mut responses = Vec::new();
    while responses.is_empty() {
        let count = client.read(&mut buffer).unwrap();
        assert_ne!(
            count, 0,
            "gateway disconnected before START acknowledgement"
        );
        responses.extend(decoder.push(&buffer[..count]).unwrap());
    }
    assert_eq!(responses[0].kind, 100);
    assert!(matches!(
        producer_finished_rx.recv_timeout(std::time::Duration::from_millis(100)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));

    let mut data_frames = 0usize;
    let mut terminal_seen = false;
    let mut pending = Vec::new();
    while !terminal_seen {
        let count = client.read(&mut buffer).unwrap();
        assert_ne!(count, 0, "gateway disconnected before terminal result");
        pending.extend(decoder.push(&buffer[..count]).unwrap());
        for frame in pending.drain(..) {
            assert_eq!(frame.request_id, 8);
            if frame.kind == 102 {
                data_frames += 1;
                assert_eq!(frame.response_sequence, data_frames as u64);
                assert_eq!(frame.payload.len(), RESULT_FRAME_BYTES);
                assert!(frame.payload.iter().all(|byte| *byte == b'x'));
            } else if frame.kind == 103 {
                terminal_seen = true;
                assert_eq!(frame.response_sequence, (RESULT_FRAME_COUNT + 1) as u64);
                assert_eq!(frame.payload, br#"{"outcome":"committed"}"#);
            } else {
                panic!("unexpected response frame kind {}", frame.kind);
            }
        }
    }
    assert_eq!(data_frames, RESULT_FRAME_COUNT);
    producer_finished_rx.recv().unwrap();
    client.shutdown(std::net::Shutdown::Write).unwrap();
    gateway.join().unwrap();
    supervisor.join().unwrap();
}

#[test]
fn stalled_tcp_result_write_times_out_and_closes_service_stream() {
    const RESULT_FRAME_BYTES: usize = 256 * 1024;
    const RESULT_FRAME_COUNT: usize = 16;
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let (mut gateway_service, mut supervisor_service) =
        std::os::unix::net::UnixStream::pair().unwrap();
    let supervisor = std::thread::spawn(move || -> Result<(), prdb_engine::ipc::FrameError> {
        let start = read_frame(&mut supervisor_service).unwrap().unwrap();
        write_frame(
            &mut supervisor_service,
            &IpcFrame {
                operation: start.operation,
                request_id: start.request_id,
                payload: br#"{"state":"accepted"}"#.to_vec(),
            },
        )
        .unwrap();
        let payload = vec![b'y'; RESULT_FRAME_BYTES];
        for _ in 0..RESULT_FRAME_COUNT {
            write_frame(
                &mut supervisor_service,
                &IpcFrame {
                    operation: Operation::GatewayResultData as u16,
                    request_id: start.request_id,
                    payload: payload.clone(),
                },
            )?;
        }
        Ok(())
    });
    let gateway = std::thread::spawn(move || {
        let (mut client, _) = listener.accept().unwrap();
        // SAFETY: the accepted stream owns a live socket descriptor and the option value is valid.
        let size: libc::c_int = 16 * 1024;
        let result = unsafe {
            libc::setsockopt(
                std::os::fd::AsRawFd::as_raw_fd(&client),
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                (&size as *const libc::c_int).cast(),
                std::mem::size_of_val(&size) as libc::socklen_t,
            )
        };
        assert_eq!(result, 0);
        prdb_engine::gateway::serve_tcp_client_connection_with_timeouts(
            &mut client,
            &mut gateway_service,
            std::time::Duration::from_millis(100),
            std::time::Duration::from_millis(100),
        )
    });

    let mut client = std::net::TcpStream::connect(address).unwrap();
    client
        .write_all(
            &ClientFrame {
                kind: 7,
                request_id: 9,
                response_sequence: 0,
                payload: b"{}".to_vec(),
            }
            .encode()
            .unwrap(),
        )
        .unwrap();
    let mut decoder = ClientFrameDecoder::default();
    let mut buffer = [0u8; 256];
    let mut responses = Vec::new();
    while responses.is_empty() {
        let count = client.read(&mut buffer).unwrap();
        assert_ne!(
            count, 0,
            "gateway disconnected before START acknowledgement"
        );
        responses.extend(decoder.push(&buffer[..count]).unwrap());
    }
    assert_eq!(responses[0].kind, 100);

    let gateway_result = gateway.join().unwrap();
    assert!(matches!(
        gateway_result,
        Err(prdb_engine::gateway::GatewayConnectionError::Io(ref error))
            if matches!(error.kind(), std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock)
    ));
    assert!(supervisor.join().unwrap().is_err());
}
