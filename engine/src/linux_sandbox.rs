//! Capability detection for the fail-closed Linux worker profile.
//!
//! This module only reports whether the host exposes the required primitives.
//! It does not establish confinement or permit loading a worker artifact.

use std::path::{Path, PathBuf};

const REQUIRED_CGROUP_CONTROLLERS: [&str; 3] = ["cpu", "memory", "pids"];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxCapabilityReport {
    pub user_namespaces: bool,
    pub landlock_abi: Option<i32>,
    pub seccomp_kill_process: bool,
    pub cgroup_v2_mounted: bool,
    pub cgroup_parent_writable: bool,
    pub cgroup_controllers: Vec<String>,
    pub cgroup_enabled_controllers: Vec<String>,
    pub cgroup_parent: PathBuf,
}

impl SandboxCapabilityReport {
    /// Return each missing capability required by the initial worker profile.
    pub fn missing_requirements(&self) -> Vec<&'static str> {
        let mut missing = Vec::new();
        if !self.user_namespaces {
            missing.push("user namespaces");
        }
        if self.landlock_abi.is_none_or(|abi| abi < 1) {
            missing.push("Landlock ABI 1 or newer");
        }
        if !self.seccomp_kill_process {
            missing.push("seccomp filter with KILL_PROCESS");
        }
        if !self.cgroup_v2_mounted {
            missing.push("cgroup v2 mount");
        }
        if !self.cgroup_parent_writable {
            missing.push("writable delegated cgroup parent");
        }
        if REQUIRED_CGROUP_CONTROLLERS.iter().any(|controller| {
            !self
                .cgroup_controllers
                .iter()
                .any(|item| item == controller)
        }) {
            missing.push("cpu, memory, and pids cgroup controllers");
        }
        if REQUIRED_CGROUP_CONTROLLERS.iter().any(|controller| {
            !self
                .cgroup_enabled_controllers
                .iter()
                .any(|item| item == controller)
        }) {
            missing.push("enabled cpu, memory, and pids cgroup controllers");
        }
        missing
    }

    /// Refuse worker startup if the host cannot enforce the minimum profile.
    pub fn require_supported(&self) -> Result<(), SandboxCapabilityError> {
        let missing = self.missing_requirements();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(SandboxCapabilityError {
                missing,
                cgroup_parent: self.cgroup_parent.clone(),
            })
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxCapabilityError {
    pub missing: Vec<&'static str>,
    pub cgroup_parent: PathBuf,
}

impl std::fmt::Display for SandboxCapabilityError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "worker launch requires unavailable sandbox capabilities: {}; cgroup parent {}",
            self.missing.join(", "),
            self.cgroup_parent.display()
        )
    }
}

impl std::error::Error for SandboxCapabilityError {}

/// Inspect host support without changing the calling process or starting a worker.
pub fn probe(cgroup_parent: impl AsRef<Path>) -> SandboxCapabilityReport {
    let cgroup_parent = cgroup_parent.as_ref().to_path_buf();
    let (user_namespaces, landlock_abi, seccomp_kill_process) = probe_linux_features();
    let controllers_path = cgroup_parent.join("cgroup.controllers");
    let enabled_path = cgroup_parent.join("cgroup.subtree_control");
    let cgroup_controllers = read_words(&controllers_path);
    let cgroup_enabled_controllers = read_words(&enabled_path);
    let cgroup_v2_mounted = cgroup_parent.join("cgroup.controllers").is_file();
    let cgroup_parent_writable = cgroup_v2_mounted && directory_is_writable(&cgroup_parent);

    SandboxCapabilityReport {
        user_namespaces,
        landlock_abi,
        seccomp_kill_process,
        cgroup_v2_mounted,
        cgroup_parent_writable,
        cgroup_controllers,
        cgroup_enabled_controllers,
        cgroup_parent,
    }
}

#[cfg(target_os = "linux")]
fn probe_linux_features() -> (bool, Option<i32>, bool) {
    let maximum_user_namespaces = std::fs::read_to_string("/proc/sys/user/max_user_namespaces")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .is_some_and(|maximum| maximum > 0);
    let unprivileged_user_namespaces =
        std::fs::read_to_string("/proc/sys/kernel/unprivileged_userns_clone")
            .ok()
            .is_none_or(|value| value.trim() == "1");
    let landlock_abi = probe_landlock_abi();
    let seccomp_kill_process = std::fs::read_to_string("/proc/sys/kernel/seccomp/actions_avail")
        .ok()
        .is_some_and(|actions| {
            actions
                .split_whitespace()
                .any(|item| item == "kill_process")
        });
    (
        maximum_user_namespaces && unprivileged_user_namespaces,
        landlock_abi,
        seccomp_kill_process,
    )
}

#[cfg(not(target_os = "linux"))]
fn probe_linux_features() -> (bool, Option<i32>, bool) {
    (false, None, false)
}

#[cfg(target_os = "linux")]
fn probe_landlock_abi() -> Option<i32> {
    const LANDLOCK_CREATE_RULESET_VERSION: u32 = 1;
    // SAFETY: the version query passes a null ruleset pointer and zero size as
    // required by landlock_create_ruleset; the syscall does not retain pointers.
    let result = unsafe {
        libc::syscall(
            libc::SYS_landlock_create_ruleset,
            std::ptr::null::<libc::c_void>(),
            0usize,
            LANDLOCK_CREATE_RULESET_VERSION,
        )
    };
    (result >= 1).then_some(result as i32)
}

#[cfg(not(target_os = "linux"))]
fn probe_landlock_abi() -> Option<i32> {
    None
}

fn read_words(path: &Path) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .split_whitespace()
        .map(str::to_owned)
        .collect()
}

#[cfg(unix)]
fn directory_is_writable(path: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;
    let Ok(path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    // SAFETY: `path` is a valid NUL-terminated string and access does not retain it.
    unsafe { libc::access(path.as_ptr(), libc::W_OK) == 0 }
}

#[cfg(not(unix))]
fn directory_is_writable(_path: &Path) -> bool {
    false
}
