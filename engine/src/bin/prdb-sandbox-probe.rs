use std::path::PathBuf;

fn main() {
    let cgroup_parent = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/sys/fs/cgroup"));
    let report = prdb_engine::linux_sandbox::probe(&cgroup_parent);

    println!("User namespaces configured: {}", report.user_namespaces);
    println!("Landlock ABI: {}", display_option(report.landlock_abi));
    println!(
        "Seccomp KILL_PROCESS action: {}",
        report.seccomp_kill_process
    );
    println!("Cgroup v2 mounted: {}", report.cgroup_v2_mounted);
    println!(
        "Cgroup parent writable: {} ({})",
        report.cgroup_parent_writable,
        report.cgroup_parent.display()
    );
    println!(
        "Cgroup controllers: {}",
        report.cgroup_controllers.join(" ")
    );
    println!(
        "Enabled child controllers: {}",
        report.cgroup_enabled_controllers.join(" ")
    );

    match report.require_supported() {
        Ok(()) => {
            println!("The host passes the capability preflight; worker setup is still required.")
        }
        Err(error) => {
            eprintln!("Host rejected for worker launch: {error}");
            std::process::exit(1);
        }
    }
}

fn display_option(value: Option<i32>) -> String {
    value.map_or_else(|| "unavailable".to_owned(), |version| version.to_string())
}
