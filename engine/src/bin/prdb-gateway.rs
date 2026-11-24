use prdb_engine::gateway::{
    GatewayListenerConfig, GatewayRuntimeConfig, TcpGatewayListener, UnixGatewayServiceConnector,
    run_gateway_with_control,
};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

static SIGNAL_REQUESTED: AtomicBool = AtomicBool::new(false);

extern "C" fn request_shutdown(_signal: libc::c_int) {
    SIGNAL_REQUESTED.store(true, Ordering::Release);
}

fn main() {
    if std::env::args().any(|argument| argument == "--help" || argument == "-h") {
        println!(
            "prdb-gateway [options]\n\
             --bind ADDRESS:PORT                 default 127.0.0.1:0\n\
             --control-bind ADDRESS:PORT         default 127.0.0.1:0; status/cancel/admin lane\n\
             --service-socket PATH               required\n\
             --service-uid UID                   required\n\
             --workers COUNT                     default 32\n\
             --queue-capacity COUNT              default 64\n\
             --control-workers COUNT             default 2\n\
             --control-queue-capacity COUNT       default 8\n\
             --connections-per-ip COUNT           default 60 per minute\n\
             --auth-failures-per-ip COUNT         default 10 per minute\n\
             --idle-timeout-ms DURATION          default 30000\n\
             --pre-auth-timeout-ms DURATION      default 10000\n\
             --max-total-result-bytes N          default 67108864\n\
             --allow-plaintext-non-loopback      explicit plaintext bind opt-in"
        );
        return;
    }
    if let Err(error) = run() {
        eprintln!("prdb-gateway: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut bind_address: SocketAddr = "127.0.0.1:0".parse()?;
    let mut control_bind_address: SocketAddr = "127.0.0.1:0".parse()?;
    let mut service_socket = None::<PathBuf>;
    let mut service_uid = None::<u32>;
    let mut worker_count = 32usize;
    let mut queue_capacity = 64usize;
    let mut control_worker_count = 2usize;
    let mut control_queue_capacity = 8usize;
    let mut connections_per_ip_per_minute = 60usize;
    let mut auth_failures_per_ip_per_minute = 10usize;
    let mut idle_timeout = Duration::from_secs(30);
    let mut pre_auth_timeout = Duration::from_secs(10);
    let mut max_total_result_bytes = prdb_engine::query_abi::MAX_TOTAL_RESULT_BYTES;
    let mut allow_non_loopback_plaintext = false;
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        let value = |arguments: &mut std::iter::Skip<std::env::Args>, name: &str| {
            arguments
                .next()
                .ok_or_else(|| format!("missing value for {name}"))
        };
        match argument.as_str() {
            "--bind" => bind_address = value(&mut arguments, "--bind")?.parse()?,
            "--control-bind" => {
                control_bind_address = value(&mut arguments, "--control-bind")?.parse()?;
            }
            "--service-socket" => {
                service_socket = Some(value(&mut arguments, "--service-socket")?.into());
            }
            "--service-uid" => {
                service_uid = Some(value(&mut arguments, "--service-uid")?.parse()?);
            }
            "--workers" => worker_count = value(&mut arguments, "--workers")?.parse()?,
            "--queue-capacity" => {
                queue_capacity = value(&mut arguments, "--queue-capacity")?.parse()?;
            }
            "--control-workers" => {
                control_worker_count = value(&mut arguments, "--control-workers")?.parse()?;
            }
            "--control-queue-capacity" => {
                control_queue_capacity =
                    value(&mut arguments, "--control-queue-capacity")?.parse()?;
            }
            "--connections-per-ip" => {
                connections_per_ip_per_minute =
                    value(&mut arguments, "--connections-per-ip")?.parse()?;
            }
            "--auth-failures-per-ip" => {
                auth_failures_per_ip_per_minute =
                    value(&mut arguments, "--auth-failures-per-ip")?.parse()?;
            }
            "--idle-timeout-ms" => {
                idle_timeout =
                    Duration::from_millis(value(&mut arguments, "--idle-timeout-ms")?.parse()?);
            }
            "--pre-auth-timeout-ms" => {
                pre_auth_timeout =
                    Duration::from_millis(value(&mut arguments, "--pre-auth-timeout-ms")?.parse()?);
            }
            "--max-total-result-bytes" => {
                max_total_result_bytes =
                    value(&mut arguments, "--max-total-result-bytes")?.parse()?;
                if max_total_result_bytes == 0 {
                    return Err("--max-total-result-bytes must be positive".into());
                }
            }
            "--allow-plaintext-non-loopback" => allow_non_loopback_plaintext = true,
            unknown => return Err(format!("unknown option {unknown}").into()),
        }
    }

    let service_socket = service_socket.ok_or("--service-socket is required")?;
    let service_uid = service_uid.ok_or("--service-uid is required")?;
    let listener = TcpGatewayListener::bind(GatewayListenerConfig {
        bind_address,
        allow_non_loopback_plaintext,
    })?;
    let control_listener = TcpGatewayListener::bind(GatewayListenerConfig {
        bind_address: control_bind_address,
        allow_non_loopback_plaintext,
    })?;
    eprintln!("prdb-gateway listening on {}", listener.local_addr()?);
    eprintln!(
        "prdb-gateway control listening on {}",
        control_listener.local_addr()?
    );
    let connector = UnixGatewayServiceConnector::new(service_socket, service_uid);

    install_signal_handlers()?;
    let shutdown = Arc::new(AtomicBool::new(false));
    let signal_shutdown = Arc::clone(&shutdown);
    let signal_monitor = std::thread::spawn(move || {
        while !SIGNAL_REQUESTED.load(Ordering::Acquire) && !signal_shutdown.load(Ordering::Acquire)
        {
            std::thread::sleep(Duration::from_millis(20));
        }
        signal_shutdown.store(true, Ordering::Release);
    });
    let result = run_gateway_with_control(
        listener,
        control_listener,
        GatewayRuntimeConfig {
            worker_count,
            queue_capacity,
            control_worker_count,
            control_queue_capacity,
            connections_per_ip_per_minute,
            auth_failures_per_ip_per_minute,
            idle_timeout,
            pre_auth_timeout,
            max_total_result_bytes,
            ..GatewayRuntimeConfig::default()
        },
        move || connector.connect(),
        Arc::clone(&shutdown),
    );
    shutdown.store(true, Ordering::Release);
    signal_monitor
        .join()
        .map_err(|_| "gateway signal monitor panicked")?;
    result?;
    Ok(())
}

fn install_signal_handlers() -> Result<(), std::io::Error> {
    // SAFETY: the handler performs only an atomic store and is installed for
    // process termination signals before the listener starts.
    unsafe {
        for signal in [libc::SIGINT, libc::SIGTERM] {
            if libc::signal(signal, request_shutdown as *const () as libc::sighandler_t)
                == libc::SIG_ERR
            {
                return Err(std::io::Error::last_os_error());
            }
        }
    }
    Ok(())
}
