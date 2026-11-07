use prdb_engine::client_protocol::{ClientFrame, ClientFrameDecoder};
use prdb_engine::gateway::{GatewayListenerConfig, GatewayListenerError, TcpGatewayListener};
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
