#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
fn main() {
    eprintln!("prdb-optimizer currently supports Linux x86_64 only");
    std::process::exit(1);
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn main() {
    use std::os::fd::FromRawFd;
    use std::os::unix::net::UnixStream;

    let (submission_fd, planner_fd) = match parse_args() {
        Ok(fds) => fds,
        Err(error) => {
            eprintln!("prdb-optimizer: {error}");
            std::process::exit(2);
        }
    };
    if let Err(error) = initialize_optimizer() {
        eprintln!("prdb-optimizer: setup failed: {error}");
        std::process::exit(70);
    }
    // SAFETY: the supervisor passes two distinct connected Unix-stream fds
    // and transfers ownership to this process.
    let mut submission = unsafe { UnixStream::from_raw_fd(submission_fd) };
    // SAFETY: same descriptor ownership contract as the submission stream.
    let mut planner = unsafe { UnixStream::from_raw_fd(planner_fd) };
    if let Err(error) =
        prdb_engine::optimizer_process::serve_optimizer_process(&mut submission, &mut planner)
    {
        eprintln!("prdb-optimizer: {error}");
        std::process::exit(70);
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn initialize_optimizer() -> std::io::Result<()> {
    // The launcher already installs no_new_privs before exec. Setting it here
    // as well keeps direct invocations fail-closed before the syscall filter.
    // SAFETY: PR_SET_NO_NEW_PRIVS takes only scalar arguments.
    if unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    prdb_engine::linux_sandbox::install_worker_seccomp_filter()
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn parse_args() -> Result<(i32, i32), String> {
    let values = std::env::args_os().skip(1).collect::<Vec<_>>();
    if values.len() != 4 || values[0] != "--submission-fd" || values[2] != "--planner-fd" {
        return Err("usage: prdb-optimizer --submission-fd FD --planner-fd FD".to_owned());
    }
    let submission_fd = values[1]
        .to_str()
        .ok_or_else(|| "submission descriptor is not UTF-8".to_owned())?
        .parse::<i32>()
        .map_err(|_| "invalid submission descriptor".to_owned())?;
    let planner_fd = values[3]
        .to_str()
        .ok_or_else(|| "planner descriptor is not UTF-8".to_owned())?
        .parse::<i32>()
        .map_err(|_| "invalid planner descriptor".to_owned())?;
    if submission_fd < 3 || planner_fd < 3 || submission_fd == planner_fd {
        return Err("optimizer descriptors must be distinct and greater than stderr".to_owned());
    }
    Ok((submission_fd, planner_fd))
}
