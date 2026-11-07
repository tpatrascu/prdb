use prdb_engine::gateway::{GatewayListenerConfig, GatewayListenerError, TcpGatewayListener};
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
