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
