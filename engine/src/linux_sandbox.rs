//! Capability detection for the fail-closed Linux worker profile.
//!
//! This module only reports whether the host exposes the required primitives.
//! It does not establish confinement or permit loading a worker artifact.

use std::io;
use std::path::{Path, PathBuf};

const REQUIRED_CGROUP_CONTROLLERS: [&str; 3] = ["cpu", "memory", "pids"];

const LANDLOCK_ABI_REQUIRED: i32 = 3;

/// Filesystem paths explicitly exposed to a worker before it starts.
/// Unlisted filesystem access is denied by the Landlock ruleset.
#[cfg(target_os = "linux")]
#[derive(Clone, Debug, Default)]
pub struct LandlockPolicy {
    rules: Vec<(PathBuf, u64)>,
}

#[cfg(target_os = "linux")]
impl LandlockPolicy {
    pub fn allow_read(&mut self, path: impl AsRef<Path>) -> io::Result<&mut Self> {
        self.add_path(path.as_ref(), LANDLOCK_READ_ACCESS)
    }

    pub fn allow_read_execute(&mut self, path: impl AsRef<Path>) -> io::Result<&mut Self> {
        self.add_path(path.as_ref(), LANDLOCK_READ_ACCESS | LANDLOCK_EXECUTE)
    }

    /// Grant read and write access, including creation, removal, and truncate
    /// operations, beneath a directory.
    pub fn allow_writable(&mut self, path: impl AsRef<Path>) -> io::Result<&mut Self> {
        let path = path.as_ref();
        if !path.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "writable Landlock rules require a directory",
            ));
        }
        self.add_path(path, LANDLOCK_WRITE_ACCESS | LANDLOCK_READ_ACCESS)
    }

    fn add_path(&mut self, path: &Path, access: u64) -> io::Result<&mut Self> {
        let path = path.canonicalize()?;
        let metadata = path.metadata()?;
        let access = if metadata.is_dir() {
            access | LANDLOCK_READ_DIR
        } else {
            access & !LANDLOCK_READ_DIR
        };
        if let Some((_, existing)) = self.rules.iter_mut().find(|(item, _)| *item == path) {
            *existing |= access;
        } else {
            self.rules.push((path, access));
        }
        Ok(self)
    }
}

/// A prepared Landlock ruleset that can be installed in a worker child.
#[cfg(target_os = "linux")]
#[derive(Debug)]
pub struct LandlockRuleset {
    fd: std::os::fd::OwnedFd,
}

#[cfg(target_os = "linux")]
impl LandlockRuleset {
    pub fn new(policy: &LandlockPolicy) -> io::Result<Self> {
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

        let abi = probe_landlock_abi()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "Landlock is unavailable"))?;
        if abi < LANDLOCK_ABI_REQUIRED {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("Landlock ABI {LANDLOCK_ABI_REQUIRED} or newer is required"),
            ));
        }
        let attr = LandlockRulesetAttr {
            handled_access_fs: LANDLOCK_HANDLED_ACCESS,
        };
        // SAFETY: the attribute is initialized and remains valid for this syscall.
        let fd = unsafe {
            libc::syscall(
                libc::SYS_landlock_create_ruleset,
                &attr,
                std::mem::size_of::<LandlockRulesetAttr>(),
                0u32,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful create_ruleset returns a new owned file descriptor.
        let fd = unsafe { OwnedFd::from_raw_fd(fd as i32) };
        for (path, access) in &policy.rules {
            add_landlock_path_rule(fd.as_raw_fd(), path, *access)?;
        }
        Ok(Self { fd })
    }

    /// Install the ruleset after earlier pre-exec hooks have placed the child
    /// in its cgroup, and before `exec` transfers control to worker code.
    #[cfg(unix)]
    pub fn attach_command_before_exec(
        &self,
        command: &mut std::process::Command,
    ) -> io::Result<()> {
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
        use std::os::unix::process::CommandExt;

        // Duplicate in the parent so the child hook only performs syscalls.
        // SAFETY: fcntl duplicates the valid ruleset descriptor.
        let child_fd = unsafe { libc::fcntl(self.fd.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
        if child_fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: fcntl returned a new owned descriptor.
        let child_fd = unsafe { OwnedFd::from_raw_fd(child_fd) };
        let raw_fd = child_fd.as_raw_fd();
        // SAFETY: pre_exec uses only prctl and landlock_restrict_self, with a
        // pre-opened descriptor. The captured OwnedFd remains alive through fork.
        unsafe {
            command.pre_exec(move || {
                let _keep_ruleset_open = &child_fd;
                if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
                    return Err(io::Error::last_os_error());
                }
                let result = libc::syscall(libc::SYS_landlock_restrict_self, raw_fd, 0u32);
                if result < 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        Ok(())
    }
}

/// Place a command in private rootless user, mount, network, and PID
/// namespaces before its executable image starts. Register cgroup placement
/// before this hook and Landlock/seccomp hooks after it. PID namespace setup
/// forks a small trusted reaper; callers must manage the command in its
/// per-invocation cgroup so killing the reaper also cleans up the worker.
#[cfg(target_os = "linux")]
pub fn attach_namespaces_before_exec(command: &mut std::process::Command) -> io::Result<()> {
    use std::os::unix::process::CommandExt;

    // Capture IDs before entering the child user namespace. The root mapping
    // in the new namespace maps only these host IDs.
    // SAFETY: getuid and getgid have no pointer arguments and do not mutate state.
    let host_uid = unsafe { libc::getuid() };
    let host_gid = unsafe { libc::getgid() };
    // SAFETY: the child hook uses direct syscalls, stack buffers, and
    // async-signal-safe libc calls after Command forks.
    unsafe {
        command.pre_exec(move || enter_worker_namespaces(host_uid, host_gid));
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn attach_namespaces_before_exec(_command: &mut std::process::Command) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "worker namespaces are supported only on Linux",
    ))
}

/// Clear inherited environment variables and close every descriptor above
/// stderr except the explicit worker IPC descriptors before `exec`.
///
/// Register this after cgroup, namespace, and Landlock pre-exec hooks so their
/// setup descriptors remain available until installed. The caller may then
/// add only non-secret environment values with `Command::env`.
/// Preserved descriptors have `FD_CLOEXEC` cleared in the child so they survive
/// the final exec. Linux `close_range` support is required; failure aborts spawn.
pub fn prepare_clean_worker_command(
    command: &mut std::process::Command,
    inherited_fds: &[i32],
) -> io::Result<()> {
    if inherited_fds.iter().any(|fd| *fd < 3) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "worker IPC descriptors must be greater than stderr",
        ));
    }
    command.env_clear();

    #[cfg(target_os = "linux")]
    {
        use std::os::unix::process::CommandExt;

        let mut preserved = inherited_fds.to_vec();
        preserved.sort_unstable();
        preserved.dedup();
        let mut close_ranges = Vec::with_capacity(preserved.len() + 1);
        let mut next = 3u32;
        for fd in &preserved {
            let fd = *fd as u32;
            if next < fd {
                close_ranges.push((next, fd - 1));
            }
            next = fd + 1;
        }
        close_ranges.push((next, u32::MAX));

        // SAFETY: the pre-exec closure uses only async-signal-safe syscalls and
        // owned scalar/vector data prepared before fork.
        unsafe {
            command.pre_exec(move || {
                for fd in &preserved {
                    let flags = libc::fcntl(*fd, libc::F_GETFD);
                    if flags < 0 || libc::fcntl(*fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0 {
                        return Err(io::Error::last_os_error());
                    }
                }
                for (first, last) in &close_ranges {
                    if libc::syscall(libc::SYS_close_range, *first, *last, 0u32) < 0 {
                        return Err(io::Error::last_os_error());
                    }
                }
                Ok(())
            });
        }
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = inherited_fds;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "clean worker descriptor setup requires Linux close_range",
        ))
    }
}

/// Register the supported Linux worker pre-exec hooks in their required
/// order: cgroup placement, namespaces, Landlock, then descriptor scrubbing.
/// Add only non-secret environment values after this function returns. The
/// caller must still install seccomp inside the clean worker host after exec
/// and before loading any query library.
#[cfg(target_os = "linux")]
pub fn configure_worker_command(
    command: &mut std::process::Command,
    cgroup: impl AsRef<Path>,
    landlock: &LandlockRuleset,
    inherited_fds: &[i32],
) -> io::Result<()> {
    attach_command_before_exec(cgroup, command)?;
    attach_namespaces_before_exec(command)?;
    landlock.attach_command_before_exec(command)?;
    prepare_clean_worker_command(command, inherited_fds)
}

#[cfg(target_os = "linux")]
unsafe fn enter_worker_namespaces(host_uid: libc::uid_t, host_gid: libc::gid_t) -> io::Result<()> {
    // SAFETY: unshare receives only a fixed namespace flag mask.
    if unsafe { libc::unshare(libc::CLONE_NEWUSER) } != 0 {
        return Err(io::Error::last_os_error());
    }

    write_proc_file(b"/proc/self/setgroups\0", b"deny\n")?;
    write_id_map(b"/proc/self/uid_map\0", host_uid)?;
    write_id_map(b"/proc/self/gid_map\0", host_gid)?;

    let namespace_flags = libc::CLONE_NEWNS | libc::CLONE_NEWNET | libc::CLONE_NEWPID;
    // SAFETY: the new user namespace owns the mount, network, and PID namespaces.
    if unsafe { libc::unshare(namespace_flags) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // Stop mount events in this namespace from propagating to the host or
    // other mount namespaces before any caller-specific mounts are made.
    // SAFETY: pointers are null or static NUL-terminated path; this changes
    // mount propagation only within the new mount namespace.
    if unsafe {
        libc::mount(
            std::ptr::null(),
            c"/".as_ptr(),
            std::ptr::null(),
            libc::MS_REC | libc::MS_PRIVATE,
            std::ptr::null(),
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }

    // A process created after CLONE_NEWPID becomes PID 1 in the new namespace.
    // Its parent remains a trusted reaper outside that PID namespace.
    // SAFETY: fork is called once in the single-threaded post-fork child.
    let worker_pid = unsafe { libc::fork() };
    if worker_pid < 0 {
        return Err(io::Error::last_os_error());
    }
    if worker_pid > 0 {
        if drop_worker_bounding_capabilities().is_err() || clear_worker_capabilities().is_err() {
            // SAFETY: discard the worker if the trusted reaper cannot shed its
            // own namespace capabilities before waiting.
            unsafe { terminate_and_reap_worker(worker_pid) };
        }
        // The outer Command child inherited Rust's exec-error pipe as well as
        // the worker's descriptors. Close its copy before waiting so
        // Command::spawn can observe successful exec in the worker promptly.
        // The worker fork retains its own descriptors and applies the later
        // confinement hooks before exec.
        // SAFETY: close_range receives a valid inclusive FD range and flags 0.
        if unsafe { libc::syscall(libc::SYS_close_range, 3u32, u32::MAX, 0u32) } != 0 {
            // SAFETY: terminate the inner process if the trusted reaper cannot
            // close the inherited descriptors required by the launch protocol.
            unsafe { terminate_and_reap_worker(worker_pid) };
        }
        let mut status = 0;
        loop {
            // SAFETY: waitpid observes the direct child and writes to a valid
            // stack status word.
            let waited = unsafe { libc::waitpid(worker_pid, &mut status, 0) };
            if waited == worker_pid {
                break;
            }
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EINTR) {
                unsafe { terminate_and_reap_worker(worker_pid) };
            }
        }
        let exit_code = if libc::WIFEXITED(status) {
            libc::WEXITSTATUS(status)
        } else if libc::WIFSIGNALED(status) {
            128 + libc::WTERMSIG(status)
        } else {
            127
        };
        // SAFETY: _exit terminates the trusted namespace reaper without
        // running inherited Rust destructors after fork.
        unsafe { libc::_exit(exit_code) };
    }

    // Mount procfs only after entering the new PID namespace. Reusing the
    // supervisor's proc mount would expose host process IDs despite the PID
    // namespace.
    // SAFETY: the process is PID 1 in the new PID namespace and holds
    // CAP_SYS_ADMIN only in its owning user namespace; the mount is scoped to
    // the private mount namespace created above.
    if unsafe {
        libc::mount(
            c"proc".as_ptr(),
            c"/proc".as_ptr(),
            c"proc".as_ptr(),
            libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC,
            std::ptr::null(),
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    drop_worker_bounding_capabilities()?;
    clear_worker_capabilities()?;

    // Prevent privilege gain across the executable image transition.
    // SAFETY: prctl uses scalar arguments and no pointers for this option.
    if unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
unsafe fn terminate_and_reap_worker(worker_pid: libc::pid_t) -> ! {
    // SAFETY: worker_pid is the direct namespace child returned by fork.
    unsafe { libc::kill(worker_pid, libc::SIGKILL) };
    let mut ignored_status = 0;
    loop {
        // SAFETY: waitpid observes the direct child and writes to a valid
        // stack status word.
        let waited = unsafe { libc::waitpid(worker_pid, &mut ignored_status, 0) };
        if waited == worker_pid {
            break;
        }
        if io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
            break;
        }
    }
    // SAFETY: this is a post-fork child; do not run inherited Rust destructors.
    unsafe { libc::_exit(127) }
}

/// Install the initial x86_64 executor syscall allowlist in the current
/// process. Call from the clean worker host after exec and before loading the
/// query library or creating threads. Unsupported architectures fail closed.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub fn install_worker_seccomp_filter() -> io::Result<()> {
    #[repr(C)]
    struct SockFilter {
        code: u16,
        jt: u8,
        jf: u8,
        k: u32,
    }
    #[repr(C)]
    struct SockFprog {
        len: u16,
        filter: *const SockFilter,
    }
    #[repr(C)]
    struct SeccompData {
        nr: i32,
        arch: u32,
        instruction_pointer: u64,
        args: [u64; 6],
    }

    const AUDIT_ARCH_X86_64: u32 = 0xc000_003e;
    const BPF_LD_W_ABS: u16 = 0x20;
    const BPF_JMP_JEQ_K: u16 = 0x15;
    const BPF_RET_K: u16 = 0x06;
    const SECCOMP_RET_KILL_PROCESS: u32 = 0x8000_0000;
    const SECCOMP_RET_ALLOW: u32 = 0x7fff_0000;
    const SECCOMP_SET_MODE_FILTER: libc::c_long = 1;
    const SECCOMP_FILTER_FLAG_TSYNC: libc::c_long = 1;

    // This list supports the current Rust/libc worker, dynamic loading, and
    // inherited Unix IPC streams. New worker or SDK behavior must update this
    // list deliberately and add a positive/negative syscall regression.
    const ALLOWED_SYSCALLS: &[libc::c_long] = &[
        libc::SYS_read,
        libc::SYS_write,
        libc::SYS_readv,
        libc::SYS_writev,
        libc::SYS_close,
        libc::SYS_fstat,
        libc::SYS_newfstatat,
        libc::SYS_statx,
        libc::SYS_lseek,
        libc::SYS_openat,
        libc::SYS_readlink,
        libc::SYS_readlinkat,
        libc::SYS_access,
        libc::SYS_faccessat,
        libc::SYS_faccessat2,
        libc::SYS_pread64,
        libc::SYS_mmap,
        libc::SYS_mprotect,
        libc::SYS_munmap,
        libc::SYS_mremap,
        libc::SYS_brk,
        libc::SYS_madvise,
        libc::SYS_fcntl,
        libc::SYS_dup,
        libc::SYS_dup2,
        libc::SYS_dup3,
        libc::SYS_ioctl,
        libc::SYS_rt_sigaction,
        libc::SYS_rt_sigprocmask,
        libc::SYS_rt_sigreturn,
        libc::SYS_sigaltstack,
        libc::SYS_sched_yield,
        libc::SYS_futex,
        libc::SYS_set_robust_list,
        libc::SYS_rseq,
        libc::SYS_clock_gettime,
        libc::SYS_clock_getres,
        libc::SYS_clock_nanosleep,
        libc::SYS_nanosleep,
        libc::SYS_getpid,
        libc::SYS_gettid,
        libc::SYS_getuid,
        libc::SYS_geteuid,
        libc::SYS_getgid,
        libc::SYS_getegid,
        libc::SYS_getrandom,
        libc::SYS_uname,
        libc::SYS_getcwd,
        libc::SYS_poll,
        libc::SYS_ppoll,
        libc::SYS_select,
        libc::SYS_pselect6,
        libc::SYS_epoll_create1,
        libc::SYS_epoll_ctl,
        libc::SYS_epoll_wait,
        libc::SYS_epoll_pwait,
        libc::SYS_recvfrom,
        libc::SYS_sendto,
        libc::SYS_recvmsg,
        libc::SYS_sendmsg,
        libc::SYS_getsockopt,
        libc::SYS_getsockname,
        libc::SYS_getpeername,
        libc::SYS_shutdown,
        libc::SYS_arch_prctl,
        libc::SYS_set_tid_address,
        libc::SYS_restart_syscall,
        libc::SYS_exit,
        libc::SYS_exit_group,
    ];

    let mut filters = Vec::with_capacity(4 + ALLOWED_SYSCALLS.len() * 2);
    filters.push(SockFilter {
        code: BPF_LD_W_ABS,
        jt: 0,
        jf: 0,
        k: std::mem::offset_of!(SeccompData, arch) as u32,
    });
    filters.push(SockFilter {
        code: BPF_JMP_JEQ_K,
        jt: 1,
        jf: 0,
        k: AUDIT_ARCH_X86_64,
    });
    filters.push(SockFilter {
        code: BPF_RET_K,
        jt: 0,
        jf: 0,
        k: SECCOMP_RET_KILL_PROCESS,
    });
    filters.push(SockFilter {
        code: BPF_LD_W_ABS,
        jt: 0,
        jf: 0,
        k: std::mem::offset_of!(SeccompData, nr) as u32,
    });
    for syscall_number in ALLOWED_SYSCALLS {
        filters.push(SockFilter {
            code: BPF_JMP_JEQ_K,
            jt: 0,
            jf: 1,
            k: *syscall_number as u32,
        });
        filters.push(SockFilter {
            code: BPF_RET_K,
            jt: 0,
            jf: 0,
            k: SECCOMP_RET_ALLOW,
        });
    }
    filters.push(SockFilter {
        code: BPF_RET_K,
        jt: 0,
        jf: 0,
        k: SECCOMP_RET_KILL_PROCESS,
    });
    let program = SockFprog {
        len: filters.len() as u16,
        filter: filters.as_ptr(),
    };

    // SAFETY: prctl receives scalar arguments; setting no_new_privs is
    // required before installing an unprivileged seccomp filter.
    if unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // TSYNC applies the filter to every existing thread. The worker must
    // install it before artifact initialization; future threads inherit it.
    // SAFETY: program and its filter vector remain alive for the syscall.
    let result = unsafe {
        libc::syscall(
            libc::SYS_seccomp,
            SECCOMP_SET_MODE_FILTER,
            SECCOMP_FILTER_FLAG_TSYNC,
            &program,
        )
    };
    if result == 0 {
        Ok(())
    } else if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Err(io::Error::other(format!(
            "seccomp TSYNC could not filter worker thread {result}"
        )))
    }
}

#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
pub fn install_worker_seccomp_filter() -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "the worker syscall profile is currently defined only for Linux x86_64",
    ))
}

#[cfg(target_os = "linux")]
fn write_proc_file(path: &'static [u8], contents: &[u8]) -> io::Result<()> {
    // SAFETY: path is a static NUL-terminated byte string.
    let fd = unsafe { libc::open(path.as_ptr().cast(), libc::O_WRONLY | libc::O_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let result = write_fd_all(fd, contents);
    // SAFETY: fd is the descriptor returned by open above.
    unsafe { libc::close(fd) };
    result
}

#[cfg(target_os = "linux")]
fn write_id_map(path: &'static [u8], host_id: u32) -> io::Result<()> {
    let mut line = [0u8; 32];
    let mut cursor = 0;
    line[cursor] = b'0';
    cursor += 1;
    line[cursor] = b' ';
    cursor += 1;
    cursor += write_decimal(&mut line[cursor..], host_id);
    line[cursor] = b' ';
    cursor += 1;
    line[cursor] = b'1';
    cursor += 1;
    line[cursor] = b'\n';
    cursor += 1;
    write_proc_file(path, &line[..cursor])
}

#[cfg(target_os = "linux")]
fn write_decimal(buffer: &mut [u8], mut value: u32) -> usize {
    let mut digits = [0u8; 10];
    let mut count = 0;
    loop {
        digits[count] = b'0' + (value % 10) as u8;
        count += 1;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    for index in 0..count {
        buffer[index] = digits[count - index - 1];
    }
    count
}

#[cfg(target_os = "linux")]
fn write_fd_all(fd: libc::c_int, bytes: &[u8]) -> io::Result<()> {
    let mut written = 0;
    while written < bytes.len() {
        // SAFETY: bytes is valid for the remaining length and fd was opened by
        // the caller for this bounded procfs write.
        let result = unsafe {
            libc::write(
                fd,
                bytes.as_ptr().add(written).cast(),
                bytes.len() - written,
            )
        };
        if result < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return Err(error);
        }
        if result == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "short write while configuring worker ID map",
            ));
        }
        written += result as usize;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn clear_worker_capabilities() -> io::Result<()> {
    #[repr(C)]
    struct CapHeader {
        version: u32,
        pid: i32,
    }
    #[repr(C)]
    struct CapData {
        effective: u32,
        permitted: u32,
        inheritable: u32,
    }
    const LINUX_CAPABILITY_VERSION_3: u32 = 0x2008_0522;

    let header = CapHeader {
        version: LINUX_CAPABILITY_VERSION_3,
        pid: 0,
    };
    let data = [
        CapData {
            effective: 0,
            permitted: 0,
            inheritable: 0,
        },
        CapData {
            effective: 0,
            permitted: 0,
            inheritable: 0,
        },
    ];
    // SAFETY: header and data match the Linux capset v3 ABI and remain valid
    // for the duration of the syscall.
    let result = unsafe { libc::syscall(libc::SYS_capset, &header, data.as_ptr()) };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn drop_worker_bounding_capabilities() -> io::Result<()> {
    // Linux capability numbers are currently bounded by the 64-bit capability
    // masks. Probe each number so the launcher also handles newer kernels.
    for capability in 0..64u32 {
        // SAFETY: PR_CAPBSET_READ takes one scalar capability number.
        let state = unsafe { libc::prctl(libc::PR_CAPBSET_READ, capability, 0, 0, 0) };
        if state < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EINVAL) {
                break;
            }
            return Err(error);
        }
        if state == 1 {
            // SAFETY: PR_CAPBSET_DROP takes one scalar capability number; the
            // process still has CAP_SETPCAP in its newly created user namespace.
            if unsafe { libc::prctl(libc::PR_CAPBSET_DROP, capability, 0, 0, 0) } != 0 {
                return Err(io::Error::last_os_error());
            }
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[repr(C)]
struct LandlockRulesetAttr {
    handled_access_fs: u64,
}

#[cfg(target_os = "linux")]
#[repr(C)]
struct LandlockPathBeneathAttr {
    allowed_access: u64,
    parent_fd: i32,
    reserved: u32,
}

#[cfg(target_os = "linux")]
const LANDLOCK_EXECUTE: u64 = 1 << 0;
#[cfg(target_os = "linux")]
const LANDLOCK_READ_DIR: u64 = 1 << 3;
#[cfg(target_os = "linux")]
const LANDLOCK_READ_ACCESS: u64 = 1 << 2;
#[cfg(target_os = "linux")]
const LANDLOCK_WRITE_ACCESS: u64 = (1 << 1) | (0x1ff << 4) | (1 << 13) | (1 << 14);
#[cfg(target_os = "linux")]
const LANDLOCK_HANDLED_ACCESS: u64 = (1 << 15) - 1;

#[cfg(target_os = "linux")]
fn add_landlock_path_rule(ruleset_fd: i32, path: &Path, access: u64) -> io::Result<()> {
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;

    let path_c = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Landlock path contains NUL"))?;
    // SAFETY: the canonical path is a NUL-terminated string and open returns
    // an O_PATH descriptor used only for this rule addition.
    let path_fd = unsafe { libc::open(path_c.as_ptr(), libc::O_PATH | libc::O_CLOEXEC) };
    if path_fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: path_fd is an owned descriptor returned by open.
    let path_fd = unsafe { std::os::fd::OwnedFd::from_raw_fd(path_fd) };
    let attr = LandlockPathBeneathAttr {
        allowed_access: access,
        parent_fd: path_fd.as_raw_fd(),
        reserved: 0,
    };
    // SAFETY: attr is initialized and both descriptors are valid for the call.
    let result =
        unsafe { libc::syscall(libc::SYS_landlock_add_rule, ruleset_fd, 1u32, &attr, 0u32) };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Per-invocation resource ceilings for a non-root cgroup v2 leaf.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerResourceLimits {
    pub cpu_quota_micros: u64,
    pub cpu_period_micros: u64,
    pub memory_max_bytes: u64,
    pub max_processes: u64,
}

/// A private cgroup v2 leaf assigned to one invocation.
#[derive(Debug)]
pub struct WorkerCgroup {
    path: PathBuf,
}

impl WorkerCgroup {
    /// Create and configure a per-invocation leaf under an already delegated
    /// cgroup parent. The worker must not be spawned if this fails.
    pub fn create(
        parent: impl AsRef<Path>,
        invocation_id: [u8; 16],
        limits: WorkerResourceLimits,
    ) -> io::Result<Self> {
        if invocation_id.iter().all(|byte| *byte == 0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invocation ID must be nonzero",
            ));
        }
        let parent = parent.as_ref();
        probe(parent)
            .require_supported()
            .map_err(|error| io::Error::new(io::ErrorKind::PermissionDenied, error))?;
        validate_delegated_parent(parent)?;
        let name = invocation_id
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let path = parent.join(format!("prdb-{name}"));
        std::fs::create_dir(&path)?;
        let configured = (|| {
            limits.apply(&path)?;
            for control in ["cgroup.procs", "pids.current", "cgroup.kill"] {
                if !path.join(control).exists() {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        format!("required cgroup v2 control {control} is unavailable"),
                    ));
                }
            }
            Ok(())
        })();
        if let Err(error) = configured {
            let _ = std::fs::remove_dir(&path);
            return Err(error);
        }
        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Attach a stopped worker before allowing it to execute untrusted code.
    pub fn attach_process(&self, pid: u32) -> io::Result<()> {
        attach_process(&self.path, pid)
    }

    /// Register cgroup placement in the child's pre-exec setup, before the
    /// executable receives control. Add this before hooks that drop the
    /// privilege needed to write `cgroup.procs`.
    #[cfg(unix)]
    pub fn attach_command_before_exec(
        &self,
        command: &mut std::process::Command,
    ) -> io::Result<()> {
        attach_command_before_exec(&self.path, command)
    }

    pub fn kill_all(&self) -> io::Result<()> {
        kill_all(&self.path)
    }

    /// Kill descendants and remove the cgroup only after the kernel reports
    /// that the leaf is empty. On timeout, leave it in place for the caller
    /// to continue cleanup or report an operational failure.
    pub fn cleanup(self, timeout: std::time::Duration) -> io::Result<()> {
        self.kill_all()?;
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let current = std::fs::read_to_string(self.path.join("pids.current"))?;
            let current = current.trim().parse::<u64>().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid cgroup pids.current value",
                )
            })?;
            if current == 0 {
                return std::fs::remove_dir(&self.path);
            }
            if std::time::Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "worker cgroup remained populated after kill request",
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}

impl Drop for WorkerCgroup {
    fn drop(&mut self) {
        let _ = kill_all(&self.path);
    }
}

fn validate_delegated_parent(parent: &Path) -> io::Result<()> {
    if !parent.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "cgroup v2 parent directory does not exist",
        ));
    }
    let controllers = read_words(&parent.join("cgroup.controllers"));
    let enabled = read_words(&parent.join("cgroup.subtree_control"));
    let has_required = |values: &[String]| {
        REQUIRED_CGROUP_CONTROLLERS.iter().all(|required| {
            values
                .iter()
                .any(|value| value.trim_start_matches('+') == *required)
        })
    };
    if !has_required(&controllers) || !has_required(&enabled) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "cgroup parent must delegate and enable cpu, memory, and pids",
        ));
    }
    Ok(())
}

impl WorkerResourceLimits {
    /// Apply hard CPU, memory, and process-count limits to a prepared cgroup.
    /// Callers must not start the worker unless every write succeeds.
    pub fn apply(self, cgroup: impl AsRef<Path>) -> io::Result<()> {
        if self.cpu_quota_micros == 0
            || self.cpu_period_micros == 0
            || self.memory_max_bytes == 0
            || self.max_processes == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker cgroup limits must be positive",
            ));
        }
        let cgroup = cgroup.as_ref();
        if !cgroup.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "worker cgroup directory does not exist",
            ));
        }
        write_control(
            cgroup.join("cpu.max"),
            format!("{} {}", self.cpu_quota_micros, self.cpu_period_micros),
        )?;
        write_control(cgroup.join("memory.max"), self.memory_max_bytes.to_string())?;
        write_control(cgroup.join("pids.max"), self.max_processes.to_string())
    }
}

/// Place a process in its invocation cgroup. The supervisor must arrange
/// this before the process can execute untrusted code.
pub fn attach_process(cgroup: impl AsRef<Path>, pid: u32) -> io::Result<()> {
    if pid == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "worker process ID must be nonzero",
        ));
    }
    write_control(cgroup.as_ref().join("cgroup.procs"), pid.to_string())
}

/// Register process placement in `Command`'s child-side pre-exec hooks. The
/// callback performs only `getpid` and `write`, which are async-signal-safe;
/// its file descriptor is opened in the parent and closes on exec.
#[cfg(unix)]
pub fn attach_command_before_exec(
    cgroup: impl AsRef<Path>,
    command: &mut std::process::Command,
) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    use std::os::unix::process::CommandExt;

    let process_file = std::fs::OpenOptions::new()
        .write(true)
        .open(cgroup.as_ref().join("cgroup.procs"))?;
    let process_fd = process_file.as_raw_fd();
    // SAFETY: the hook uses only async-signal-safe getpid/write calls and
    // stack memory after fork. The captured file remains alive in Command and
    // is close-on-exec; the parent opened the cgroup control before spawning.
    unsafe {
        command.pre_exec(move || {
            let _keep_process_file_open = &process_file;
            let pid = libc::getpid();
            if pid <= 0 {
                return Err(io::Error::last_os_error());
            }
            let mut digits = [0u8; 10];
            let mut value = pid as u32;
            let mut count = 0usize;
            loop {
                digits[count] = b'0' + (value % 10) as u8;
                count += 1;
                value /= 10;
                if value == 0 {
                    break;
                }
            }
            let mut bytes = [0u8; 11];
            for index in 0..count {
                bytes[index] = digits[count - index - 1];
            }
            bytes[count] = b'\n';
            let length = count + 1;
            let mut written = 0usize;
            while written < length {
                // SAFETY: both pointers reference valid stack/file data for
                // the requested remaining length, and the FD was opened above.
                let result = libc::write(
                    process_fd,
                    bytes.as_ptr().add(written).cast(),
                    length - written,
                );
                if result < 0 {
                    let error = io::Error::last_os_error();
                    if error.raw_os_error() == Some(libc::EINTR) {
                        continue;
                    }
                    return Err(error);
                }
                if result == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to attach worker to its cgroup",
                    ));
                }
                written += result as usize;
            }
            Ok(())
        });
    }
    Ok(())
}

/// Ask cgroup v2 to kill every process in the invocation cgroup and its
/// descendants. Failure must be treated as incomplete worker cleanup.
pub fn kill_all(cgroup: impl AsRef<Path>) -> io::Result<()> {
    write_control(cgroup.as_ref().join("cgroup.kill"), "1")
}

fn write_control(path: PathBuf, value: impl AsRef<[u8]>) -> io::Result<()> {
    if !path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("required cgroup control {} is unavailable", path.display()),
        ));
    }
    std::fs::write(path, value)
}

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
        if self
            .landlock_abi
            .is_none_or(|abi| abi < LANDLOCK_ABI_REQUIRED)
        {
            missing.push("Landlock ABI 3 or newer");
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
    let cgroup_v2_mounted =
        is_cgroup_v2_mount(&cgroup_parent) && cgroup_parent.join("cgroup.controllers").is_file();
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
fn is_cgroup_v2_mount(path: &Path) -> bool {
    const CGROUP2_SUPER_MAGIC: libc::c_long = 0x6367_7270;
    use std::os::unix::ffi::OsStrExt;

    let Ok(path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    // SAFETY: `path` is NUL-terminated and `stats` points to valid writable
    // storage for the statfs result.
    unsafe {
        let mut stats = std::mem::zeroed::<libc::statfs>();
        libc::statfs(path.as_ptr(), &mut stats) == 0
            && stats.f_type as libc::c_long == CGROUP2_SUPER_MAGIC
    }
}

#[cfg(not(target_os = "linux"))]
fn is_cgroup_v2_mount(_path: &Path) -> bool {
    false
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
