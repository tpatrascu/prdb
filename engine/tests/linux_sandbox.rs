use prdb_engine::linux_sandbox::{self, WorkerResourceLimits};
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture(std::path::PathBuf);

impl Fixture {
    fn new() -> Self {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("prdb-linux-sandbox-{}-{id}", std::process::id()));
        fs::create_dir(&path).unwrap();
        for name in [
            "cpu.max",
            "memory.max",
            "pids.max",
            "cgroup.procs",
            "cgroup.kill",
        ] {
            fs::write(path.join(name), "").unwrap();
        }
        Self(path)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn worker_resource_limits_configure_cgroup_v2_and_can_be_validated() {
    let fixture = Fixture::new();
    let limits = WorkerResourceLimits {
        cpu_quota_micros: 150_000,
        cpu_period_micros: 100_000,
        memory_max_bytes: 512 * 1024 * 1024,
        max_processes: 8,
    };
    limits.apply(&fixture.0).unwrap();
    assert_eq!(
        fs::read_to_string(fixture.0.join("cpu.max")).unwrap(),
        "150000 100000"
    );
    assert_eq!(
        fs::read_to_string(fixture.0.join("memory.max")).unwrap(),
        "536870912"
    );
    assert_eq!(fs::read_to_string(fixture.0.join("pids.max")).unwrap(), "8");

    linux_sandbox::attach_process(&fixture.0, 4242).unwrap();
    assert_eq!(
        fs::read_to_string(fixture.0.join("cgroup.procs")).unwrap(),
        "4242"
    );
    linux_sandbox::kill_all(&fixture.0).unwrap();
    assert_eq!(
        fs::read_to_string(fixture.0.join("cgroup.kill")).unwrap(),
        "1"
    );
}

#[test]
fn worker_resource_limits_reject_zero_values_before_touching_cgroup() {
    let fixture = Fixture::new();
    let limits = WorkerResourceLimits {
        cpu_quota_micros: 0,
        cpu_period_micros: 100_000,
        memory_max_bytes: 512 * 1024 * 1024,
        max_processes: 8,
    };
    assert!(limits.apply(&fixture.0).is_err());
    assert_eq!(fs::read_to_string(fixture.0.join("cpu.max")).unwrap(), "");
    assert_eq!(
        fs::read_to_string(fixture.0.join("memory.max")).unwrap(),
        ""
    );
}

#[test]
fn worker_cgroup_creation_rejects_undelegated_parent_before_creating_leaf() {
    let fixture = Fixture::new();
    let invocation_id = [0x42; 16];
    let limits = WorkerResourceLimits {
        cpu_quota_micros: 100_000,
        cpu_period_micros: 100_000,
        memory_max_bytes: 128 * 1024 * 1024,
        max_processes: 4,
    };
    let leaf = fixture.0.join(format!("prdb-{}", "42".repeat(16)));
    let error = prdb_engine::linux_sandbox::WorkerCgroup::create(&fixture.0, invocation_id, limits)
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    assert!(!leaf.exists());
}

#[test]
fn worker_cgroup_creation_rejects_regular_directory_before_creating_leaf() {
    let fixture = Fixture::new();
    fs::write(fixture.0.join("cgroup.controllers"), "cpu memory pids").unwrap();
    fs::write(fixture.0.join("cgroup.subtree_control"), "cpu memory pids").unwrap();
    let invocation_id = [0x43; 16];
    let leaf = fixture.0.join(format!("prdb-{}", "43".repeat(16)));
    let limits = WorkerResourceLimits {
        cpu_quota_micros: 100_000,
        cpu_period_micros: 100_000,
        memory_max_bytes: 128 * 1024 * 1024,
        max_processes: 4,
    };
    assert!(
        prdb_engine::linux_sandbox::WorkerCgroup::create(&fixture.0, invocation_id, limits,)
            .is_err()
    );
    assert!(!leaf.exists());
}

#[test]
fn worker_command_is_attached_to_cgroup_before_exec() {
    use std::process::Command;

    let fixture = Fixture::new();
    let process_file = fixture.0.join("cgroup.procs");
    let mut command = Command::new("sh");
    command
        .args(["-c", "cat \"$1\"", "worker", process_file.to_str().unwrap()])
        .stdout(std::process::Stdio::piped());
    linux_sandbox::attach_command_before_exec(&fixture.0, &mut command).unwrap();
    let output = command.output().unwrap();
    assert!(output.status.success());
    let pid = String::from_utf8(output.stdout).unwrap();
    assert!(pid.trim().parse::<u32>().unwrap() > 0);
}

#[test]
fn sandbox_probe_rejects_regular_directory_as_cgroup_v2_mount() {
    let fixture = Fixture::new();
    fs::write(fixture.0.join("cgroup.controllers"), "cpu memory pids").unwrap();
    fs::write(fixture.0.join("cgroup.subtree_control"), "cpu memory pids").unwrap();
    let report = linux_sandbox::probe(&fixture.0);
    assert!(!report.cgroup_v2_mounted);
    assert!(report.missing_requirements().contains(&"cgroup v2 mount"));
}

#[cfg(target_os = "linux")]
#[test]
fn worker_command_enters_private_namespaces_without_capabilities() {
    use std::process::Command;

    fn resolve(name: &str) -> std::path::PathBuf {
        std::env::split_paths(&std::env::var_os("PATH").unwrap())
            .map(|directory| directory.join(name))
            .find(|candidate| candidate.is_file())
            .unwrap()
    }

    let namespace_link = |name: &str| fs::read_link(format!("/proc/self/ns/{name}")).unwrap();
    let parent_namespaces = [
        namespace_link("user"),
        namespace_link("mnt"),
        namespace_link("net"),
        namespace_link("pid"),
    ];
    let readlink = resolve("readlink");
    let awk = resolve("awk");
    let script = r#"
        "$1" /proc/self/ns/user
        "$1" /proc/self/ns/mnt
        "$1" /proc/self/ns/net
        "$1" /proc/self/ns/pid
        printf '%s\n' "$$"
        "$2" '/^CapEff:/ {eff=$2} /^CapBnd:/ {bnd=$2} /^NoNewPrivs:/ {nnp=$2} END {print eff, bnd, nnp}' /proc/self/status
        test ! -d "/proc/$3" || exit 12
        printf '%s\n' isolated
    "#;
    let cgroup_fixture = Fixture::new();
    let host_pid = std::process::id().to_string();
    let mut command = Command::new("/bin/sh");
    command
        .env_clear()
        .args([
            "-c",
            script,
            "worker",
            readlink.to_str().unwrap(),
            awk.to_str().unwrap(),
            host_pid.as_str(),
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    linux_sandbox::attach_command_before_exec(&cgroup_fixture.0, &mut command).unwrap();
    linux_sandbox::attach_namespaces_before_exec(&mut command).unwrap();

    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "namespace setup failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lines = String::from_utf8(output.stdout).unwrap();
    let lines = lines.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 7);
    for (index, parent_namespace) in parent_namespaces.iter().enumerate() {
        assert_ne!(std::path::Path::new(lines[index]), parent_namespace);
    }
    assert_eq!(lines[4], "1", "worker must be PID 1 in its PID namespace");
    assert_eq!(
        lines[5], "0000000000000000 0000000000000000 1",
        "worker capability sets must be empty and no_new_privs enabled"
    );
    assert_eq!(
        lines[6], "isolated",
        "the worker proc mount must not expose the supervisor PID"
    );
    assert!(
        fs::read_to_string(cgroup_fixture.0.join("cgroup.procs"))
            .unwrap()
            .trim()
            .parse::<u32>()
            .unwrap()
            > 0,
        "cgroup placement hook must run before namespace setup"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn namespace_command_spawn_returns_after_worker_exec() {
    use std::process::Command;

    let mut command = Command::new("/bin/sh");
    command.args(["-c", "sleep 2"]);
    linux_sandbox::attach_namespaces_before_exec(&mut command).unwrap();

    let mut child = command.spawn().unwrap();
    assert!(
        child.try_wait().unwrap().is_none(),
        "Command::spawn waited for the worker to exit"
    );
    assert!(child.wait().unwrap().success());
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[test]
fn worker_seccomp_filter_allows_runtime_calls_and_kills_unlisted_syscalls() {
    use std::os::unix::process::ExitStatusExt;
    use std::process::Command;

    const CHILD_MODE: &str = "PRDB_SECCOMP_PROBE_MODE";
    if let Some(mode) = std::env::var_os(CHILD_MODE) {
        linux_sandbox::install_worker_seccomp_filter().unwrap();
        match mode.to_str().unwrap() {
            "allow" => {
                assert!(unsafe { libc::syscall(libc::SYS_getpid) } > 0);
            }
            "deny" => {
                // Creating a new socket is outside the worker profile. The
                // filter must terminate the whole child process with SIGSYS.
                unsafe { libc::syscall(libc::SYS_socket, libc::AF_INET, libc::SOCK_STREAM, 0) };
                panic!("unlisted socket syscall unexpectedly returned");
            }
            other => panic!("unknown seccomp probe mode: {other}"),
        }
        return;
    }

    let executable = std::env::current_exe().unwrap();
    let run_probe = |mode: &str| {
        Command::new(&executable)
            .args([
                "--exact",
                "worker_seccomp_filter_allows_runtime_calls_and_kills_unlisted_syscalls",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD_MODE, mode)
            .output()
            .unwrap()
    };

    let allowed = run_probe("allow");
    assert!(
        allowed.status.success(),
        "allowlisted probe failed: {}",
        String::from_utf8_lossy(&allowed.stderr)
    );

    let denied = run_probe("deny");
    assert_eq!(
        denied.status.signal(),
        Some(libc::SIGSYS),
        "unlisted syscall was not killed: status={:?}, stderr={}",
        denied.status,
        String::from_utf8_lossy(&denied.stderr)
    );
}

#[cfg(target_os = "linux")]
#[test]
fn prepare_clean_worker_command_scrubs_environment_and_descriptors() {
    use std::os::fd::AsRawFd;
    use std::process::Command;

    let source = fs::File::open("/dev/null").unwrap();
    // SAFETY: fcntl duplicates the owned source descriptor; both duplicates
    // are explicitly closed after the child has been observed.
    let hidden = unsafe { libc::fcntl(source.as_raw_fd(), libc::F_DUPFD, 64) };
    assert!(hidden >= 64);
    let preserved = unsafe { libc::fcntl(source.as_raw_fd(), libc::F_DUPFD, hidden + 1) };
    assert!(preserved > hidden);
    assert_eq!(
        unsafe { libc::fcntl(preserved, libc::F_SETFD, libc::FD_CLOEXEC) },
        0
    );
    let hidden = hidden.to_string();
    let preserved = preserved.to_string();

    let mut command = Command::new("/bin/sh");
    command
        .env("PRDB_SUPERVISOR_SECRET", "must-not-inherit")
        .args([
            "-c",
            "test -e \"/proc/self/fd/$1\" && test ! -e \"/proc/self/fd/$2\" && test -z \"${PRDB_SUPERVISOR_SECRET+x}\"",
            "worker",
            &preserved,
            &hidden,
        ]);
    linux_sandbox::prepare_clean_worker_command(&mut command, &[preserved.parse().unwrap()])
        .unwrap();

    let status = command.status().unwrap();
    // SAFETY: these descriptors were created above and are no longer needed.
    unsafe {
        libc::close(hidden.parse().unwrap());
        libc::close(preserved.parse().unwrap());
    }
    assert!(status.success());
}

#[cfg(target_os = "linux")]
#[test]
fn configured_worker_command_applies_the_pre_exec_confinement_chain() {
    use prdb_engine::linux_sandbox::{LandlockPolicy, LandlockRuleset};
    use std::process::Command;

    let fixture = Fixture::new();
    let mut policy = LandlockPolicy::default();
    for path in [
        "/bin",
        "/usr/bin",
        "/lib",
        "/lib64",
        "/usr/lib",
        "/usr/lib64",
        "/nix/store",
    ] {
        if std::path::Path::new(path).exists() {
            policy.allow_read_execute(path).unwrap();
        }
    }
    if std::path::Path::new("/etc/ld.so.cache").exists() {
        policy.allow_read("/etc/ld.so.cache").unwrap();
    }
    policy.allow_writable(&fixture.0).unwrap();
    let ruleset = LandlockRuleset::new(&policy).unwrap();
    let cat = std::env::split_paths(&std::env::var_os("PATH").unwrap())
        .map(|directory| directory.join("cat"))
        .find(|candidate| candidate.is_file())
        .unwrap();
    let mut command = Command::new("/bin/sh");
    command
        .env("PRDB_SUPERVISOR_SECRET", "must-not-inherit")
        .args([
            "-c",
            "test -z \"${PRDB_SUPERVISOR_SECRET+x}\" || exit 20; printf '%s' \"$$\" > \"$1/worker-pid\"; \"$2\" /etc/passwd > \"$1/cat-output\" 2>&1; printf '%s' \"$?\" > \"$1/denied\"",
            "worker",
            fixture.0.to_str().unwrap(),
            cat.to_str().unwrap(),
        ]);

    linux_sandbox::configure_worker_command(&mut command, &fixture.0, &ruleset, &[]).unwrap();
    assert!(command.status().unwrap().success());
    assert!(
        fs::read_to_string(fixture.0.join("worker-pid"))
            .unwrap()
            .parse::<u32>()
            .unwrap()
            > 0
    );
    assert_ne!(fs::read_to_string(fixture.0.join("denied")).unwrap(), "0");
    assert!(
        fs::read_to_string(fixture.0.join("cat-output"))
            .unwrap()
            .contains("/etc/passwd")
    );
    assert!(
        fs::read_to_string(fixture.0.join("cgroup.procs"))
            .unwrap()
            .trim()
            .parse::<u32>()
            .unwrap()
            > 0
    );
}

#[cfg(target_os = "linux")]
#[test]
fn executor_host_self_check_installs_seccomp_after_exec() {
    use prdb_engine::linux_sandbox::{LandlockPolicy, LandlockRuleset};
    use std::process::Command;

    let fixture = Fixture::new();
    let executable = std::path::PathBuf::from(env!("CARGO_BIN_EXE_prdb-executor"));
    let mut policy = LandlockPolicy::default();
    policy
        .allow_read_execute(executable.parent().unwrap())
        .unwrap();
    for path in ["/lib", "/lib64", "/usr/lib", "/usr/lib64", "/nix/store"] {
        if std::path::Path::new(path).exists() {
            policy.allow_read_execute(path).unwrap();
        }
    }
    policy.allow_writable(&fixture.0).unwrap();
    let ruleset = LandlockRuleset::new(&policy).unwrap();
    let mut command = Command::new(executable);
    command.arg("--self-check");
    linux_sandbox::configure_worker_command(&mut command, &fixture.0, &ruleset, &[]).unwrap();

    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "executor self-check failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(target_os = "linux")]
#[test]
fn executor_host_rejects_direct_unconfined_launch_before_library_loading() {
    use std::process::Command;

    let executable = env!("CARGO_BIN_EXE_prdb-executor");
    let output = Command::new(executable)
        .args([
            "--library",
            "/etc/passwd",
            "--parameters-fd",
            "3",
            "--planner-fd",
            "4",
            "--executor-fd",
            "5",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("PID 1 in its private PID namespace"),
        "unexpected rejection: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(target_os = "linux")]
#[test]
fn landlock_blocks_unlisted_filesystem_reads_before_exec() {
    use prdb_engine::linux_sandbox::{LandlockPolicy, LandlockRuleset};
    use std::process::{Command, Stdio};

    let fixture = Fixture::new();
    let mut policy = LandlockPolicy::default();
    for path in [
        "/bin",
        "/usr/bin",
        "/lib",
        "/lib64",
        "/usr/lib",
        "/usr/lib64",
        "/nix/store",
    ] {
        if std::path::Path::new(path).exists() {
            policy.allow_read_execute(path).unwrap();
        }
    }
    if std::path::Path::new("/etc/ld.so.cache").exists() {
        policy.allow_read("/etc/ld.so.cache").unwrap();
    }
    policy.allow_writable(&fixture.0).unwrap();
    let ruleset = LandlockRuleset::new(&policy).unwrap();
    let marker = fixture.0.join("landlock-result");
    let resolve = |name: &str| {
        std::env::split_paths(&std::env::var_os("PATH").unwrap())
            .map(|directory| directory.join(name))
            .find(|candidate| candidate.is_file())
            .unwrap()
    };
    let shell = resolve("sh");
    let cat = resolve("cat");
    let mut command = Command::new(shell);
    command
        .args([
            "-c",
            &format!("{} /etc/passwd; printf '%s' \"$?\" > \"$1\"", cat.display()),
            "worker",
            marker.to_str().unwrap(),
        ])
        .stdout(Stdio::null());
    ruleset.attach_command_before_exec(&mut command).unwrap();
    assert!(command.status().unwrap().success());
    assert_eq!(fs::read_to_string(marker).unwrap(), "1");
}
