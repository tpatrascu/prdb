#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
fn main() {
    eprintln!("prdb-executor currently supports Linux x86_64 only");
    std::process::exit(1);
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
mod worker {
    use prdb_engine::query_abi::{
        BorrowedInvocationV1, HostApiV1, QUERY_ABI_VERSION, QUERY_DESCRIPTOR_SYMBOL,
        QUERY_ENTRY_SYMBOL, QueryDescriptorV1, QueryMainV1, validate_descriptor_v1,
    };
    use std::ffi::{CStr, CString, OsStr};
    use std::io::{self, Read};
    use std::os::fd::FromRawFd;
    use std::os::unix::ffi::OsStrExt;
    use std::path::PathBuf;

    const EXIT_ERROR: i32 = 70;
    const CAPABILITY_VERSION_3: u32 = 0x2008_0522;

    #[repr(C)]
    struct CapHeader {
        version: u32,
        pid: i32,
    }

    #[repr(C)]
    #[derive(Default)]
    struct CapData {
        effective: u32,
        permitted: u32,
        inheritable: u32,
    }

    struct WorkerArgs {
        library: PathBuf,
        parameters_fd: i32,
        _planner_fd: i32,
        _executor_fd: i32,
    }

    pub fn main() {
        let args = match std::env::args_os().skip(1).collect::<Vec<_>>().as_slice() {
            [flag] if flag == "--self-check" => {
                if let Err(error) = initialize_worker() {
                    eprintln!("prdb-executor: worker setup failed: {error}");
                    std::process::exit(EXIT_ERROR);
                }
                return;
            }
            values => match parse_args(values) {
                Ok(args) => args,
                Err(error) => {
                    eprintln!("prdb-executor: {error}");
                    std::process::exit(2);
                }
            },
        };

        let result = initialize_worker().and_then(|()| execute_library(args));
        match result {
            Ok(0) => unsafe { libc::_exit(0) },
            Ok(_) => unsafe { libc::_exit(1) },
            Err(error) => {
                eprintln!("prdb-executor: {error}");
                // Do not run unload handlers after loading native query code.
                unsafe { libc::_exit(EXIT_ERROR) }
            }
        }
    }

    fn parse_args(values: &[std::ffi::OsString]) -> Result<WorkerArgs, String> {
        if values.len() != 8
            || values[0] != "--library"
            || values[2] != "--parameters-fd"
            || values[4] != "--planner-fd"
            || values[6] != "--executor-fd"
        {
            return Err("usage: prdb-executor --library ABSOLUTE_PATH --parameters-fd FD --planner-fd FD --executor-fd FD (or --self-check)".to_owned());
        }
        let library = PathBuf::from(&values[1]);
        if !library.is_absolute() {
            return Err("library path must be absolute".to_owned());
        }
        let parameters_fd = values[3]
            .to_str()
            .ok_or_else(|| "parameter descriptor is not UTF-8".to_owned())?
            .parse::<i32>()
            .map_err(|_| "invalid parameter descriptor".to_owned())?;
        let planner_fd = values[5]
            .to_str()
            .ok_or_else(|| "planner descriptor is not UTF-8".to_owned())?
            .parse::<i32>()
            .map_err(|_| "invalid planner descriptor".to_owned())?;
        let executor_fd = values[7]
            .to_str()
            .ok_or_else(|| "executor descriptor is not UTF-8".to_owned())?
            .parse::<i32>()
            .map_err(|_| "invalid executor descriptor".to_owned())?;
        if parameters_fd < 3 || planner_fd < 3 || executor_fd < 3 {
            return Err("worker descriptors must be greater than stderr".to_owned());
        }
        if parameters_fd == planner_fd || parameters_fd == executor_fd || planner_fd == executor_fd
        {
            return Err("worker descriptors must be distinct".to_owned());
        }
        Ok(WorkerArgs {
            library,
            parameters_fd,
            _planner_fd: planner_fd,
            _executor_fd: executor_fd,
        })
    }

    fn initialize_worker() -> io::Result<()> {
        verify_worker_context()?;
        prdb_engine::linux_sandbox::install_worker_seccomp_filter()
    }

    fn verify_worker_context() -> io::Result<()> {
        // The namespace launcher makes the executor PID 1, clears all
        // capability sets, and sets no_new_privs before this image starts.
        // These checks prevent an accidental direct launch from loading code
        // in an ordinary supervisor or shell process.
        if unsafe { libc::getpid() } != 1 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "executor must be PID 1 in its private PID namespace",
            ));
        }
        // SAFETY: PR_GET_NO_NEW_PRIVS takes only scalar arguments.
        if unsafe { libc::prctl(libc::PR_GET_NO_NEW_PRIVS, 0, 0, 0, 0) } != 1 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "executor requires no_new_privs",
            ));
        }

        let mut header = CapHeader {
            version: CAPABILITY_VERSION_3,
            pid: 0,
        };
        let mut data = [CapData::default(), CapData::default()];
        // SAFETY: capget uses the documented v3 header and two-word capability
        // array, both of which remain live for the syscall.
        if unsafe { libc::syscall(libc::SYS_capget, &mut header, data.as_mut_ptr()) } < 0 {
            return Err(io::Error::last_os_error());
        }
        if data
            .iter()
            .any(|set| set.effective != 0 || set.permitted != 0 || set.inheritable != 0)
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "executor capability sets must be empty",
            ));
        }
        for capability in 0..64u32 {
            // SAFETY: PR_CAPBSET_READ takes one scalar capability index.
            let state = unsafe { libc::prctl(libc::PR_CAPBSET_READ, capability, 0, 0, 0) };
            if state < 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() == Some(libc::EINVAL) {
                    break;
                }
                return Err(error);
            }
            if state != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "executor bounding capability set must be empty",
                ));
            }
        }
        Ok(())
    }

    fn execute_library(args: WorkerArgs) -> io::Result<i32> {
        let library_path = c_path(args.library.as_os_str())?;
        // SAFETY: the absolute path is explicitly supplied by the trusted
        // launcher and the current process has installed its syscall filter.
        let handle =
            unsafe { libc::dlopen(library_path.as_ptr(), libc::RTLD_NOW | libc::RTLD_LOCAL) };
        if handle.is_null() {
            return Err(dynamic_loader_error("dlopen"));
        }

        // Validate the descriptor before resolving the entry point. Constructors
        // have already run, so this code is intentionally inside the sandbox.
        let descriptor_symbol = CString::new(QUERY_DESCRIPTOR_SYMBOL).expect("static symbol");
        let descriptor_address = lookup_symbol(handle, &descriptor_symbol)?;
        if !(descriptor_address as usize).is_multiple_of(std::mem::align_of::<QueryDescriptorV1>())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "query descriptor has invalid alignment",
            ));
        }
        // SAFETY: a malformed descriptor pointer can fault only this disposable
        // worker. The pointed-to C record is copied before validation.
        let descriptor = unsafe { descriptor_address.cast::<QueryDescriptorV1>().read() };
        validate_descriptor_v1(&descriptor).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("query ABI descriptor rejected: {error:?}"),
            )
        })?;

        let entry_symbol = CString::new(QUERY_ENTRY_SYMBOL).expect("static symbol");
        let entry_address = lookup_symbol(handle, &entry_symbol)?;
        // SAFETY: the ABI descriptor has been validated and the header defines
        // this symbol as QueryMainV1. A bad function pointer terminates worker.
        let entry: QueryMainV1 = unsafe { std::mem::transmute(entry_address) };

        let parameters = read_parameters(args.parameters_fd)?;
        let invocation = BorrowedInvocationV1::new(&parameters).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid parameters: {error:?}"),
            )
        })?;
        let host = HostApiV1 {
            struct_size: std::mem::size_of::<HostApiV1>() as u32,
            abi_version: QUERY_ABI_VERSION,
        };
        // SAFETY: both records follow the validated v1 ABI. The parameter
        // bytes and host/invocation records remain alive for the entire call.
        let status = unsafe { entry(&host, invocation.as_ptr()) };
        // Deliberately leak the loader handle and let main use _exit; arbitrary
        // library finalizers must not run during host cleanup.
        let _ = handle;
        Ok(status)
    }

    fn read_parameters(fd: i32) -> io::Result<Vec<u8>> {
        // SAFETY: the launcher passes an open parameter pipe/memfd and this
        // worker takes ownership of the descriptor for the rest of its life.
        let file = unsafe { std::fs::File::from_raw_fd(fd) };
        let mut parameters = Vec::new();
        file.take(prdb_engine::query_abi::MAX_PARAMETER_BYTES + 1)
            .read_to_end(&mut parameters)?;
        if parameters.len() as u64 > prdb_engine::query_abi::MAX_PARAMETER_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invocation parameters exceed the ABI limit",
            ));
        }
        Ok(parameters)
    }

    fn c_path(path: &OsStr) -> io::Result<CString> {
        CString::new(path.as_bytes()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "library path contains an embedded NUL byte",
            )
        })
    }

    fn lookup_symbol(handle: *mut libc::c_void, symbol: &CStr) -> io::Result<*mut libc::c_void> {
        // SAFETY: dlerror and dlsym receive valid loader-owned handles and a
        // NUL-terminated symbol name.
        unsafe { libc::dlerror() };
        let address = unsafe { libc::dlsym(handle, symbol.as_ptr()) };
        let error = unsafe { libc::dlerror() };
        if !error.is_null() {
            let message = unsafe { CStr::from_ptr(error) }.to_string_lossy();
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("{}: {message}", symbol.to_string_lossy()),
            ));
        }
        if address.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("{} resolved to a null symbol", symbol.to_string_lossy()),
            ));
        }
        Ok(address)
    }

    fn dynamic_loader_error(operation: &str) -> io::Error {
        // SAFETY: dlerror returns a loader-owned NUL-terminated string or NULL.
        let error = unsafe { libc::dlerror() };
        if error.is_null() {
            io::Error::other(format!("{operation} failed without a loader message"))
        } else {
            let message = unsafe { CStr::from_ptr(error) }.to_string_lossy();
            io::Error::other(format!("{operation}: {message}"))
        }
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn main() {
    worker::main();
}
