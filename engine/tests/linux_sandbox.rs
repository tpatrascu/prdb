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
fn worker_cgroup_creation_removes_partial_leaf_when_kernel_controls_are_missing() {
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
