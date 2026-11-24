#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
fn main() {
    eprintln!("prdb-executor currently supports Linux x86_64 only");
    std::process::exit(1);
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
mod worker {
    use prdb_engine::ipc::{
        IpcFrame, Operation, ServiceRole, authorize_operation, read_frame, write_frame,
    };
    use prdb_engine::query_abi::{
        BorrowedInvocationV1, HostApiV1, HostApiV2, HostApiV3, HostApiV4, HostApiV5, HostApiV6,
        InvocationV2, InvocationV3, InvocationV4, InvocationV5, InvocationV6,
        MAX_IPC_REQUEST_BYTES, MAX_IPC_RESPONSE_BYTES, MAX_RESULT_CHUNK_BYTES,
        QUERY_ABI_V2_VERSION, QUERY_ABI_V3_VERSION, QUERY_ABI_V4_VERSION, QUERY_ABI_V5_VERSION,
        QUERY_ABI_V6_VERSION, QUERY_ABI_VERSION, QUERY_DESCRIPTOR_SYMBOL,
        QUERY_DESCRIPTOR_V2_SYMBOL, QUERY_DESCRIPTOR_V3_SYMBOL, QUERY_DESCRIPTOR_V4_SYMBOL,
        QUERY_DESCRIPTOR_V5_SYMBOL, QUERY_DESCRIPTOR_V6_SYMBOL, QUERY_ENTRY_SYMBOL,
        QUERY_ENTRY_V2_SYMBOL, QUERY_ENTRY_V3_SYMBOL, QUERY_ENTRY_V4_SYMBOL, QUERY_ENTRY_V5_SYMBOL,
        QUERY_ENTRY_V6_SYMBOL, QUERY_STATEMENT_FAILURE_SQL, QueryDescriptorV1, QueryDescriptorV2,
        QueryDescriptorV3, QueryDescriptorV4, QueryDescriptorV5, QueryDescriptorV6,
        QueryIpcRequestV2, QueryMainV1, QueryMainV2, QueryMainV3, QueryMainV4, QueryMainV5,
        QueryMainV6, QueryOptimizerRequestV4, QueryOptimizerRequestV5, QuerySpillRequestV1,
        QueryStatementFailureV3, QueryWriteResultV2, SPILL_CHANNEL_HEADER_BYTES,
        SPILL_CHANNEL_MAGIC, SPILL_CHANNEL_VERSION, SPILL_OP_APPEND, SPILL_OP_CREATE,
        SPILL_OP_DESTROY, SPILL_OP_READ_AT, SPILL_OP_SEAL, SPILL_STATUS_OK, validate_descriptor_v1,
        validate_descriptor_v2, validate_descriptor_v3, validate_descriptor_v4,
        validate_descriptor_v5, validate_descriptor_v6,
    };
    use std::ffi::{CStr, CString, OsStr, c_void};
    use std::io::{self, Read, Write};
    use std::os::fd::FromRawFd;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::net::UnixStream;
    use std::path::PathBuf;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

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
        planner_fd: i32,
        executor_fd: i32,
        optimizer_fd: i32,
        result_fd: i32,
        spill_fd: i32,
        max_total_result_bytes: u64,
        max_sql_index_probe_candidates: u32,
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
        if values.len() != 18
            || values[0] != "--library"
            || values[2] != "--parameters-fd"
            || values[4] != "--planner-fd"
            || values[6] != "--executor-fd"
            || values[8] != "--optimizer-fd"
            || values[10] != "--result-fd"
            || values[12] != "--spill-fd"
            || values[14] != "--max-total-result-bytes"
            || values[16] != "--max-sql-index-probe-candidates"
        {
            return Err("usage: prdb-executor --library ABSOLUTE_PATH --parameters-fd FD --planner-fd FD --executor-fd FD --optimizer-fd FD --result-fd FD --spill-fd FD --max-total-result-bytes N --max-sql-index-probe-candidates N (or --self-check)".to_owned());
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
        let optimizer_fd = values[9]
            .to_str()
            .ok_or_else(|| "optimizer descriptor is not UTF-8".to_owned())?
            .parse::<i32>()
            .map_err(|_| "invalid optimizer descriptor".to_owned())?;
        let result_fd = values[11]
            .to_str()
            .ok_or_else(|| "result descriptor is not UTF-8".to_owned())?
            .parse::<i32>()
            .map_err(|_| "invalid result descriptor".to_owned())?;
        let spill_fd = values[13]
            .to_str()
            .ok_or_else(|| "spill descriptor is not UTF-8".to_owned())?
            .parse::<i32>()
            .map_err(|_| "invalid spill descriptor".to_owned())?;
        let max_total_result_bytes = values[15]
            .to_str()
            .ok_or_else(|| "result budget is not UTF-8".to_owned())?
            .parse::<u64>()
            .map_err(|_| "invalid result budget".to_owned())?;
        if max_total_result_bytes == 0 {
            return Err("result budget must be positive".to_owned());
        }
        let max_sql_index_probe_candidates = values[17]
            .to_str()
            .ok_or_else(|| "index candidate budget is not UTF-8".to_owned())?
            .parse::<u32>()
            .map_err(|_| "invalid index candidate budget".to_owned())?;
        if max_sql_index_probe_candidates == 0
            || max_sql_index_probe_candidates
                > prdb_engine::query_abi::MAX_SQL_INDEX_PROBE_CANDIDATES
        {
            return Err("index candidate budget is outside the supported envelope".to_owned());
        }
        if parameters_fd < 3
            || planner_fd < 3
            || executor_fd < 3
            || optimizer_fd < 3
            || result_fd < 3
            || (spill_fd != -1 && spill_fd < 3)
        {
            return Err("worker descriptors must be greater than stderr".to_owned());
        }
        let mut descriptors = vec![
            parameters_fd,
            planner_fd,
            executor_fd,
            optimizer_fd,
            result_fd,
        ];
        if spill_fd >= 3 {
            descriptors.push(spill_fd);
        }
        if descriptors
            .iter()
            .enumerate()
            .any(|(index, descriptor)| descriptors[..index].contains(descriptor))
        {
            return Err("worker descriptors must be distinct".to_owned());
        }
        Ok(WorkerArgs {
            library,
            parameters_fd,
            planner_fd,
            executor_fd,
            optimizer_fd,
            result_fd,
            spill_fd,
            max_total_result_bytes,
            max_sql_index_probe_candidates,
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

        // Prefer v6, then v5, v4, v3 and v2. An invalid descriptor is rejected rather than
        // falling back to a second ABI exported by the same library.
        let v6_descriptor_symbol = CString::new(QUERY_DESCRIPTOR_V6_SYMBOL).expect("static symbol");
        if let Some(descriptor_address) = lookup_optional_symbol(handle, &v6_descriptor_symbol)? {
            return unsafe { execute_v6_library(handle, descriptor_address, args) };
        }
        let v5_descriptor_symbol = CString::new(QUERY_DESCRIPTOR_V5_SYMBOL).expect("static symbol");
        if let Some(descriptor_address) = lookup_optional_symbol(handle, &v5_descriptor_symbol)? {
            return unsafe { execute_v5_library(handle, descriptor_address, args) };
        }
        let v4_descriptor_symbol = CString::new(QUERY_DESCRIPTOR_V4_SYMBOL).expect("static symbol");
        if let Some(descriptor_address) = lookup_optional_symbol(handle, &v4_descriptor_symbol)? {
            return unsafe { execute_v4_library(handle, descriptor_address, args) };
        }
        let v3_descriptor_symbol = CString::new(QUERY_DESCRIPTOR_V3_SYMBOL).expect("static symbol");
        if let Some(descriptor_address) = lookup_optional_symbol(handle, &v3_descriptor_symbol)? {
            return unsafe { execute_v3_library(handle, descriptor_address, args) };
        }
        let v2_descriptor_symbol = CString::new(QUERY_DESCRIPTOR_V2_SYMBOL).expect("static symbol");
        if let Some(descriptor_address) = lookup_optional_symbol(handle, &v2_descriptor_symbol)? {
            return unsafe { execute_v2_library(handle, descriptor_address, args) };
        }
        // SAFETY: this host has already verified confinement and the loader
        // handle remains live through the entry-point call.
        unsafe { execute_v1_library(handle, args) }
    }

    unsafe fn execute_v1_library(handle: *mut libc::c_void, args: WorkerArgs) -> io::Result<i32> {
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

    unsafe fn execute_v6_library(
        handle: *mut libc::c_void,
        descriptor_address: *mut libc::c_void,
        args: WorkerArgs,
    ) -> io::Result<i32> {
        if !(descriptor_address as usize).is_multiple_of(std::mem::align_of::<QueryDescriptorV6>())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "query v6 descriptor has invalid alignment",
            ));
        }
        let descriptor = unsafe { descriptor_address.cast::<QueryDescriptorV6>().read() };
        validate_descriptor_v6(&descriptor).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("query ABI v6 descriptor rejected: {error:?}"),
            )
        })?;
        let entry_symbol = CString::new(QUERY_ENTRY_V6_SYMBOL).expect("static symbol");
        let entry_address = lookup_symbol(handle, &entry_symbol)?;
        let entry: QueryMainV6 = unsafe { std::mem::transmute(entry_address) };
        let parameters = read_parameters(args.parameters_fd)?;
        let parameters_len = u64::try_from(parameters.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "parameters length overflow")
        })?;
        let parameters_ptr = if parameters.is_empty() {
            std::ptr::null()
        } else {
            parameters.as_ptr()
        };
        let invocation = InvocationV6 {
            struct_size: std::mem::size_of::<InvocationV6>() as u32,
            abi_version: QUERY_ABI_V6_VERSION,
            parameters: parameters_ptr,
            parameters_len,
        };
        let context = unsafe {
            CallbackContext::from_raw_fds(
                args.planner_fd,
                args.executor_fd,
                args.optimizer_fd,
                args.result_fd,
                (args.spill_fd >= 3).then_some(args.spill_fd),
                args.max_total_result_bytes,
                args.max_sql_index_probe_candidates,
            )
        };
        let context = Box::leak(Box::new(context));
        let v3 = HostApiV3 {
            struct_size: std::mem::size_of::<HostApiV3>() as u32,
            abi_version: QUERY_ABI_V3_VERSION,
            context: (context as *mut CallbackContext).cast(),
            executor_request: Some(executor_request_v2 as QueryIpcRequestV2),
            planner_request: Some(planner_request_v2 as QueryIpcRequestV2),
            write_result: Some(write_result_v2 as QueryWriteResultV2),
            statement_failure: Some(statement_failure_v3 as QueryStatementFailureV3),
        };
        let v5 = HostApiV5 {
            v4: HostApiV4 {
                v3,
                optimizer_request: Some(optimizer_request_v4 as QueryOptimizerRequestV4),
            },
            optimizer_request: Some(optimizer_request_v5 as QueryOptimizerRequestV5),
        };
        let host = HostApiV6 {
            v5,
            spill_request: context
                .spill
                .as_ref()
                .map(|_| spill_request_v1 as QuerySpillRequestV1),
        };
        let status = unsafe { entry(&host, &invocation) };
        let failed = context.statement_failed.load(Ordering::SeqCst);
        let _ = handle;
        Ok(query_exit_status(status, failed))
    }

    unsafe fn execute_v5_library(
        handle: *mut libc::c_void,
        descriptor_address: *mut libc::c_void,
        args: WorkerArgs,
    ) -> io::Result<i32> {
        if !(descriptor_address as usize).is_multiple_of(std::mem::align_of::<QueryDescriptorV5>())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "query v5 descriptor has invalid alignment",
            ));
        }
        // SAFETY: malformed native records can fault only this disposable,
        // confined worker. Copy before validation to avoid retaining a library pointer.
        let descriptor = unsafe { descriptor_address.cast::<QueryDescriptorV5>().read() };
        validate_descriptor_v5(&descriptor).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("query ABI v5 descriptor rejected: {error:?}"),
            )
        })?;
        let entry_symbol = CString::new(QUERY_ENTRY_V5_SYMBOL).expect("static symbol");
        let entry_address = lookup_symbol(handle, &entry_symbol)?;
        // SAFETY: the validated descriptor declares this ABI and entry point.
        let entry: QueryMainV5 = unsafe { std::mem::transmute(entry_address) };
        let parameters = read_parameters(args.parameters_fd)?;
        let parameters_len = u64::try_from(parameters.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "parameters length overflow")
        })?;
        let parameters_ptr = if parameters.is_empty() {
            std::ptr::null()
        } else {
            parameters.as_ptr()
        };
        let invocation = InvocationV5 {
            struct_size: std::mem::size_of::<InvocationV5>() as u32,
            abi_version: QUERY_ABI_V5_VERSION,
            parameters: parameters_ptr,
            parameters_len,
        };
        let context = unsafe {
            CallbackContext::from_raw_fds(
                args.planner_fd,
                args.executor_fd,
                args.optimizer_fd,
                args.result_fd,
                (args.spill_fd >= 3).then_some(args.spill_fd),
                args.max_total_result_bytes,
                args.max_sql_index_probe_candidates,
            )
        };
        let context = Box::leak(Box::new(context));
        let v3 = HostApiV3 {
            struct_size: std::mem::size_of::<HostApiV3>() as u32,
            abi_version: QUERY_ABI_V3_VERSION,
            context: (context as *mut CallbackContext).cast(),
            executor_request: Some(executor_request_v2 as QueryIpcRequestV2),
            planner_request: Some(planner_request_v2 as QueryIpcRequestV2),
            write_result: Some(write_result_v2 as QueryWriteResultV2),
            statement_failure: Some(statement_failure_v3 as QueryStatementFailureV3),
        };
        let host = HostApiV5 {
            v4: HostApiV4 {
                v3,
                optimizer_request: Some(optimizer_request_v4 as QueryOptimizerRequestV4),
            },
            optimizer_request: Some(optimizer_request_v5 as QueryOptimizerRequestV5),
        };
        // SAFETY: all host and invocation storage remains alive through the call.
        let status = unsafe { entry(&host, &invocation) };
        let latched_statement_failure = context.statement_failed.load(Ordering::SeqCst);
        let _ = handle;
        Ok(query_exit_status(status, latched_statement_failure))
    }

    unsafe fn execute_v4_library(
        handle: *mut libc::c_void,
        descriptor_address: *mut libc::c_void,
        args: WorkerArgs,
    ) -> io::Result<i32> {
        if !(descriptor_address as usize).is_multiple_of(std::mem::align_of::<QueryDescriptorV4>())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "query v4 descriptor has invalid alignment",
            ));
        }
        // SAFETY: malformed native records can fault only this disposable,
        // confined worker. Copy before validation to avoid retaining a library pointer.
        let descriptor = unsafe { descriptor_address.cast::<QueryDescriptorV4>().read() };
        validate_descriptor_v4(&descriptor).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("query ABI v4 descriptor rejected: {error:?}"),
            )
        })?;

        let entry_symbol = CString::new(QUERY_ENTRY_V4_SYMBOL).expect("static symbol");
        let entry_address = lookup_symbol(handle, &entry_symbol)?;
        // SAFETY: the validated descriptor declares this ABI and entry point.
        let entry: QueryMainV4 = unsafe { std::mem::transmute(entry_address) };

        let parameters = read_parameters(args.parameters_fd)?;
        let parameters_len = u64::try_from(parameters.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "parameters length overflow")
        })?;
        let parameters_ptr = if parameters.is_empty() {
            std::ptr::null()
        } else {
            parameters.as_ptr()
        };
        let invocation = InvocationV4 {
            struct_size: std::mem::size_of::<InvocationV4>() as u32,
            abi_version: QUERY_ABI_V4_VERSION,
            parameters: parameters_ptr,
            parameters_len,
        };
        // SAFETY: the trusted launcher supplied distinct inherited socket fds.
        let context = unsafe {
            CallbackContext::from_raw_fds(
                args.planner_fd,
                args.executor_fd,
                args.optimizer_fd,
                args.result_fd,
                (args.spill_fd >= 3).then_some(args.spill_fd),
                args.max_total_result_bytes,
                args.max_sql_index_probe_candidates,
            )
        };
        // Keep callback state alive until the worker's immediate `_exit`, even
        // if hostile library code retains the context pointer on another thread.
        let context = Box::leak(Box::new(context));
        let host = HostApiV4 {
            v3: HostApiV3 {
                struct_size: std::mem::size_of::<HostApiV3>() as u32,
                abi_version: QUERY_ABI_V3_VERSION,
                context: (context as *mut CallbackContext).cast(),
                executor_request: Some(executor_request_v2 as QueryIpcRequestV2),
                planner_request: Some(planner_request_v2 as QueryIpcRequestV2),
                write_result: Some(write_result_v2 as QueryWriteResultV2),
                statement_failure: Some(statement_failure_v3 as QueryStatementFailureV3),
            },
            optimizer_request: Some(optimizer_request_v4 as QueryOptimizerRequestV4),
        };
        // SAFETY: all host and invocation storage remains alive through the call.
        let status = unsafe { entry(&host, &invocation) };
        let latched_statement_failure = context.statement_failed.load(Ordering::SeqCst);
        let _ = handle;
        Ok(query_exit_status(status, latched_statement_failure))
    }

    unsafe fn execute_v3_library(
        handle: *mut libc::c_void,
        descriptor_address: *mut libc::c_void,
        args: WorkerArgs,
    ) -> io::Result<i32> {
        if !(descriptor_address as usize).is_multiple_of(std::mem::align_of::<QueryDescriptorV3>())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "query v3 descriptor has invalid alignment",
            ));
        }
        // SAFETY: malformed native records can fault only this disposable,
        // confined worker. Copy before validation to avoid retaining a library pointer.
        let descriptor = unsafe { descriptor_address.cast::<QueryDescriptorV3>().read() };
        validate_descriptor_v3(&descriptor).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("query ABI v3 descriptor rejected: {error:?}"),
            )
        })?;

        let entry_symbol = CString::new(QUERY_ENTRY_V3_SYMBOL).expect("static symbol");
        let entry_address = lookup_symbol(handle, &entry_symbol)?;
        // SAFETY: the validated descriptor declares this ABI and entry point.
        let entry: QueryMainV3 = unsafe { std::mem::transmute(entry_address) };

        let parameters = read_parameters(args.parameters_fd)?;
        let parameters_len = u64::try_from(parameters.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "parameters length overflow")
        })?;
        let parameters_ptr = if parameters.is_empty() {
            std::ptr::null()
        } else {
            parameters.as_ptr()
        };
        let invocation = InvocationV3 {
            struct_size: std::mem::size_of::<InvocationV3>() as u32,
            abi_version: QUERY_ABI_V3_VERSION,
            parameters: parameters_ptr,
            parameters_len,
        };
        // SAFETY: the trusted launcher supplied distinct inherited socket fds.
        let context = unsafe {
            CallbackContext::from_raw_fds(
                args.planner_fd,
                args.executor_fd,
                args.optimizer_fd,
                args.result_fd,
                (args.spill_fd >= 3).then_some(args.spill_fd),
                args.max_total_result_bytes,
                args.max_sql_index_probe_candidates,
            )
        };
        // Keep callback state alive until the worker's immediate `_exit`, even
        // if hostile library code retains the context pointer on another thread.
        let context = Box::leak(Box::new(context));
        let host = HostApiV3 {
            struct_size: std::mem::size_of::<HostApiV3>() as u32,
            abi_version: QUERY_ABI_V3_VERSION,
            context: (context as *mut CallbackContext).cast(),
            executor_request: Some(executor_request_v2 as QueryIpcRequestV2),
            planner_request: Some(planner_request_v2 as QueryIpcRequestV2),
            write_result: Some(write_result_v2 as QueryWriteResultV2),
            statement_failure: Some(statement_failure_v3 as QueryStatementFailureV3),
        };
        // SAFETY: all host and invocation storage remains alive through the call.
        let status = unsafe { entry(&host, &invocation) };
        let latched_statement_failure = context.statement_failed.load(Ordering::SeqCst);
        let _ = handle;
        Ok(query_exit_status(status, latched_statement_failure))
    }

    fn query_exit_status(entry_status: i32, statement_failed: bool) -> i32 {
        if statement_failed { 1 } else { entry_status }
    }

    unsafe fn execute_v2_library(
        handle: *mut libc::c_void,
        descriptor_address: *mut libc::c_void,
        args: WorkerArgs,
    ) -> io::Result<i32> {
        if !(descriptor_address as usize).is_multiple_of(std::mem::align_of::<QueryDescriptorV2>())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "query v2 descriptor has invalid alignment",
            ));
        }
        // SAFETY: malformed native records can fault only this disposable,
        // confined worker. Copy before validation to avoid retaining a library pointer.
        let descriptor = unsafe { descriptor_address.cast::<QueryDescriptorV2>().read() };
        validate_descriptor_v2(&descriptor).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("query ABI v2 descriptor rejected: {error:?}"),
            )
        })?;

        let entry_symbol = CString::new(QUERY_ENTRY_V2_SYMBOL).expect("static symbol");
        let entry_address = lookup_symbol(handle, &entry_symbol)?;
        // SAFETY: the validated descriptor declares this ABI and entry point.
        let entry: QueryMainV2 = unsafe { std::mem::transmute(entry_address) };

        let parameters = read_parameters(args.parameters_fd)?;
        let parameters_len = u64::try_from(parameters.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "parameters length overflow")
        })?;
        let parameters_ptr = if parameters.is_empty() {
            std::ptr::null()
        } else {
            parameters.as_ptr()
        };
        let invocation = InvocationV2 {
            struct_size: std::mem::size_of::<InvocationV2>() as u32,
            abi_version: QUERY_ABI_V2_VERSION,
            parameters: parameters_ptr,
            parameters_len,
        };
        // SAFETY: the trusted launcher supplied distinct inherited socket fds.
        let context = unsafe {
            CallbackContext::from_raw_fds(
                args.planner_fd,
                args.executor_fd,
                args.optimizer_fd,
                args.result_fd,
                (args.spill_fd >= 3).then_some(args.spill_fd),
                args.max_total_result_bytes,
                args.max_sql_index_probe_candidates,
            )
        };
        // Keep callback state alive until the worker's immediate `_exit`, even
        // if hostile library code retains the context pointer on another thread.
        let context = Box::leak(Box::new(context));
        let host = HostApiV2 {
            struct_size: std::mem::size_of::<HostApiV2>() as u32,
            abi_version: QUERY_ABI_V2_VERSION,
            context: (context as *mut CallbackContext).cast(),
            executor_request: Some(executor_request_v2 as QueryIpcRequestV2),
            planner_request: Some(planner_request_v2 as QueryIpcRequestV2),
            write_result: Some(write_result_v2 as QueryWriteResultV2),
        };
        // SAFETY: host, invocation, callback state and parameter bytes all
        // remain alive for the complete entry-point call.
        let status = unsafe { entry(&host, &invocation) };
        let _ = handle;
        Ok(status)
    }

    const CALLBACK_OK: i32 = 0;
    const CALLBACK_INVALID_ARGUMENT: i32 = 1;
    const CALLBACK_LIMIT_EXCEEDED: i32 = 2;
    const CALLBACK_IO_ERROR: i32 = 3;
    const CALLBACK_PROTOCOL_ERROR: i32 = 4;
    const CALLBACK_RESPONSE_TOO_SMALL: i32 = 5;
    const CALLBACK_INTERNAL_ERROR: i32 = 6;

    struct IpcChannel {
        role: ServiceRole,
        stream: Mutex<UnixStream>,
        next_request_id: AtomicU64,
    }

    impl IpcChannel {
        unsafe fn from_raw_fd(fd: i32, role: ServiceRole) -> Self {
            // SAFETY: caller transfers one unique, open Unix socket descriptor.
            let stream = unsafe { UnixStream::from_raw_fd(fd) };
            Self {
                role,
                stream: Mutex::new(stream),
                next_request_id: AtomicU64::new(1),
            }
        }

        fn request(&self, operation_code: u16, payload: Vec<u8>) -> io::Result<Vec<u8>> {
            if payload.len() > MAX_IPC_REQUEST_BYTES as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "query IPC request exceeds the ABI limit",
                ));
            }
            let operation = Operation::from_code(operation_code).map_err(|error| {
                io::Error::new(io::ErrorKind::PermissionDenied, error.to_string())
            })?;
            authorize_operation(self.role, operation).map_err(|error| {
                io::Error::new(io::ErrorKind::PermissionDenied, error.to_string())
            })?;
            let request_id = self
                .next_request_id
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
                .map_err(|_| io::Error::other("query IPC request ID exhausted"))?;
            let mut stream = self
                .stream
                .lock()
                .map_err(|_| io::Error::other("query IPC channel lock poisoned"))?;
            write_frame(
                &mut *stream,
                &IpcFrame {
                    operation: operation_code,
                    request_id,
                    payload,
                },
            )
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
            let response = read_frame(&mut *stream)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?
                .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "IPC peer closed"))?;
            if response.operation != operation_code || response.request_id != request_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "query IPC response does not match its request",
                ));
            }
            if response.payload.len() > MAX_IPC_REQUEST_BYTES as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "query IPC response exceeds the ABI limit",
                ));
            }
            Ok(response.payload)
        }
    }

    struct CallbackContext {
        executor: IpcChannel,
        planner: IpcChannel,
        optimizer: IpcChannel,
        result: Mutex<UnixStream>,
        spill: Option<Mutex<UnixStream>>,
        next_spill_request_id: AtomicU64,
        total_result_bytes: AtomicU64,
        max_total_result_bytes: u64,
        max_sql_index_probe_candidates: u32,
        statement_failed: AtomicBool,
    }

    impl CallbackContext {
        unsafe fn from_raw_fds(
            planner_fd: i32,
            executor_fd: i32,
            optimizer_fd: i32,
            result_fd: i32,
            spill_fd: Option<i32>,
            max_total_result_bytes: u64,
            max_sql_index_probe_candidates: u32,
        ) -> Self {
            // SAFETY: the trusted launcher passes distinct, open socket fds and
            // transfers ownership to this invocation context. Spill is optional.
            let planner = unsafe { IpcChannel::from_raw_fd(planner_fd, ServiceRole::Planner) };
            // SAFETY: same descriptor ownership contract as the planner fd.
            let executor = unsafe { IpcChannel::from_raw_fd(executor_fd, ServiceRole::Executor) };
            // SAFETY: the optimizer submission fd is a distinct connected Unix stream.
            let optimizer =
                unsafe { IpcChannel::from_raw_fd(optimizer_fd, ServiceRole::OptimizerSubmission) };
            // SAFETY: the result fd is a distinct connected Unix stream.
            let result = unsafe { UnixStream::from_raw_fd(result_fd) };
            // SAFETY: the launcher transfers this dedicated worker channel.
            let spill = spill_fd.map(|spill_fd| {
                // SAFETY: the trusted launcher transfers this distinct channel fd.
                Mutex::new(unsafe { UnixStream::from_raw_fd(spill_fd) })
            });
            Self {
                executor,
                planner,
                optimizer,
                result: Mutex::new(result),
                spill,
                next_spill_request_id: AtomicU64::new(1),
                total_result_bytes: AtomicU64::new(0),
                max_total_result_bytes,
                max_sql_index_probe_candidates,
                statement_failed: AtomicBool::new(false),
            }
        }
    }

    unsafe extern "C" fn statement_failure_v3(context: *mut c_void, failure_code: u32) -> i32 {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if context.is_null() || failure_code != QUERY_STATEMENT_FAILURE_SQL {
                return CALLBACK_INVALID_ARGUMENT;
            }
            // SAFETY: the host table exposes this only with its live callback context.
            let context = unsafe { &*context.cast::<CallbackContext>() };
            context.statement_failed.store(true, Ordering::SeqCst);
            CALLBACK_OK
        }))
        .unwrap_or(CALLBACK_INTERNAL_ERROR)
    }

    unsafe extern "C" fn spill_request_v1(
        context: *mut c_void,
        operation: u16,
        request: *const u8,
        request_len: u64,
        response: *mut u8,
        response_capacity: u64,
        response_len: *mut u64,
    ) -> i32 {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
            if context.is_null() || response_len.is_null() {
                return CALLBACK_INVALID_ARGUMENT;
            }
            let context = &*context.cast::<CallbackContext>();
            *response_len = 0;
            let request_len = match usize::try_from(request_len) {
                Ok(value) if value <= MAX_IPC_REQUEST_BYTES as usize => value,
                _ => return spill_callback_failure(context, CALLBACK_LIMIT_EXCEEDED),
            };
            let response_capacity = match usize::try_from(response_capacity) {
                Ok(value) if value <= MAX_IPC_RESPONSE_BYTES as usize => value,
                _ => return spill_callback_failure(context, CALLBACK_LIMIT_EXCEEDED),
            };
            if (request_len > 0 && request.is_null())
                || (response_capacity > 0 && response.is_null())
                || !matches!(
                    operation,
                    SPILL_OP_CREATE
                        | SPILL_OP_APPEND
                        | SPILL_OP_SEAL
                        | SPILL_OP_READ_AT
                        | SPILL_OP_DESTROY
                )
            {
                return spill_callback_failure(context, CALLBACK_INVALID_ARGUMENT);
            }
            let request_bytes = if request_len == 0 {
                &[][..]
            } else {
                std::slice::from_raw_parts(request, request_len)
            };
            let payload_len = match u32::try_from(request_len) {
                Ok(value) => value,
                Err(_) => return spill_callback_failure(context, CALLBACK_LIMIT_EXCEEDED),
            };
            let request_id = match context.next_spill_request_id.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |id| id.checked_add(1),
            ) {
                Ok(id) => id,
                Err(_) => return spill_callback_failure(context, CALLBACK_INTERNAL_ERROR),
            };
            let mut header = [0u8; SPILL_CHANNEL_HEADER_BYTES as usize];
            header[..4].copy_from_slice(SPILL_CHANNEL_MAGIC.as_bytes());
            header[4..6].copy_from_slice(&SPILL_CHANNEL_VERSION.to_be_bytes());
            header[6..8].copy_from_slice(&operation.to_be_bytes());
            header[8..16].copy_from_slice(&request_id.to_be_bytes());
            header[16..20].copy_from_slice(&payload_len.to_be_bytes());

            let Some(spill) = context.spill.as_ref() else {
                return spill_callback_failure(context, CALLBACK_IO_ERROR);
            };
            let mut stream = match spill.lock() {
                Ok(stream) => stream,
                Err(_) => return spill_callback_failure(context, CALLBACK_INTERNAL_ERROR),
            };
            if stream
                .write_all(&header)
                .and_then(|()| stream.write_all(request_bytes))
                .is_err()
            {
                return spill_callback_failure(context, CALLBACK_IO_ERROR);
            }
            let mut response_header = [0u8; SPILL_CHANNEL_HEADER_BYTES as usize];
            if stream.read_exact(&mut response_header).is_err()
                || response_header[..4] != SPILL_CHANNEL_MAGIC.as_bytes()[..]
                || u16::from_be_bytes([response_header[4], response_header[5]])
                    != SPILL_CHANNEL_VERSION
                || u16::from_be_bytes([response_header[6], response_header[7]])
                    != (operation | 0x8000)
                || u64::from_be_bytes(response_header[8..16].try_into().unwrap()) != request_id
            {
                return spill_callback_failure(context, CALLBACK_PROTOCOL_ERROR);
            }
            let wire_len = u32::from_be_bytes(response_header[16..20].try_into().unwrap()) as usize;
            if wire_len < 2 || wire_len > response_capacity.saturating_add(2) {
                return spill_callback_failure(context, CALLBACK_RESPONSE_TOO_SMALL);
            }
            let mut wire_response = vec![0; wire_len];
            if stream.read_exact(&mut wire_response).is_err() {
                return spill_callback_failure(context, CALLBACK_IO_ERROR);
            }
            let status = u16::from_be_bytes([wire_response[0], wire_response[1]]);
            if status != SPILL_STATUS_OK {
                return spill_callback_failure(
                    context,
                    if status == 255 {
                        CALLBACK_IO_ERROR
                    } else {
                        CALLBACK_LIMIT_EXCEEDED
                    },
                );
            }
            let body = &wire_response[2..];
            if body.len() > response_capacity {
                return spill_callback_failure(context, CALLBACK_RESPONSE_TOO_SMALL);
            }
            if !body.is_empty() {
                std::ptr::copy_nonoverlapping(body.as_ptr(), response, body.len());
            }
            *response_len = body.len() as u64;
            CALLBACK_OK
        }))
        .unwrap_or(CALLBACK_INTERNAL_ERROR)
    }

    fn spill_callback_failure(context: &CallbackContext, status: i32) -> i32 {
        context.statement_failed.store(true, Ordering::SeqCst);
        status
    }

    unsafe extern "C" fn executor_request_v2(
        context: *mut c_void,
        operation: u16,
        request: *const u8,
        request_len: u64,
        response: *mut u8,
        response_capacity: u64,
        response_len: *mut u64,
    ) -> i32 {
        if operation == prdb_engine::query_abi::QUERY_HOST_OP_GET_WORKLOAD_BUDGETS {
            return std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
                if context.is_null() || response_len.is_null() || request_len != 0 {
                    return CALLBACK_INVALID_ARGUMENT;
                }
                let context = &*context.cast::<CallbackContext>();
                *response_len = 0;
                if response_capacity < 12 || response.is_null() {
                    return CALLBACK_RESPONSE_TOO_SMALL;
                }
                let body = std::slice::from_raw_parts_mut(response, 12);
                body[..4].copy_from_slice(
                    prdb_engine::query_abi::QUERY_WORKLOAD_BUDGETS_MAGIC.as_bytes(),
                );
                body[4..6].copy_from_slice(
                    &prdb_engine::query_abi::QUERY_WORKLOAD_BUDGETS_VERSION.to_be_bytes(),
                );
                body[6..8].copy_from_slice(&12u16.to_be_bytes());
                body[8..12].copy_from_slice(&context.max_sql_index_probe_candidates.to_be_bytes());
                *response_len = 12;
                CALLBACK_OK
            }))
            .unwrap_or(CALLBACK_INTERNAL_ERROR);
        }
        callback_ipc_v2(
            context,
            ServiceRole::Executor,
            operation,
            IpcCallbackBuffers {
                request,
                request_len,
                response,
                response_capacity,
                response_len,
            },
        )
    }

    unsafe extern "C" fn planner_request_v2(
        context: *mut c_void,
        operation: u16,
        request: *const u8,
        request_len: u64,
        response: *mut u8,
        response_capacity: u64,
        response_len: *mut u64,
    ) -> i32 {
        callback_ipc_v2(
            context,
            ServiceRole::Planner,
            operation,
            IpcCallbackBuffers {
                request,
                request_len,
                response,
                response_capacity,
                response_len,
            },
        )
    }

    unsafe extern "C" fn optimizer_request_v4(
        context: *mut c_void,
        request: *const u8,
        request_len: u64,
        response: *mut u8,
        response_capacity: u64,
        response_len: *mut u64,
    ) -> i32 {
        callback_ipc_v2(
            context,
            ServiceRole::OptimizerSubmission,
            Operation::OptimizeSelect as u16,
            IpcCallbackBuffers {
                request,
                request_len,
                response,
                response_capacity,
                response_len,
            },
        )
    }

    unsafe extern "C" fn optimizer_request_v5(
        context: *mut c_void,
        operation: u16,
        request: *const u8,
        request_len: u64,
        response: *mut u8,
        response_capacity: u64,
        response_len: *mut u64,
    ) -> i32 {
        callback_ipc_v2(
            context,
            ServiceRole::OptimizerSubmission,
            operation,
            IpcCallbackBuffers {
                request,
                request_len,
                response,
                response_capacity,
                response_len,
            },
        )
    }

    struct IpcCallbackBuffers {
        request: *const u8,
        request_len: u64,
        response: *mut u8,
        response_capacity: u64,
        response_len: *mut u64,
    }

    fn callback_ipc_v2(
        context: *mut c_void,
        role: ServiceRole,
        operation: u16,
        buffers: IpcCallbackBuffers,
    ) -> i32 {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if context.is_null() || buffers.response_len.is_null() {
                return CALLBACK_INVALID_ARGUMENT;
            }
            if buffers.request_len > MAX_IPC_REQUEST_BYTES
                || buffers.response_capacity > MAX_IPC_REQUEST_BYTES
            {
                return CALLBACK_LIMIT_EXCEEDED;
            }
            if buffers.request_len > 0 && buffers.request.is_null() {
                return CALLBACK_INVALID_ARGUMENT;
            }
            // SAFETY: ABI callers must provide buffers valid for their declared
            // lengths. Null is accepted only for an empty request.
            let request_bytes = unsafe {
                if buffers.request_len == 0 {
                    &[]
                } else {
                    std::slice::from_raw_parts(buffers.request, buffers.request_len as usize)
                }
            };
            // SAFETY: context is supplied by this host in the v2 host table and
            // remains alive for the duration of the entry-point call.
            let context = unsafe { &*context.cast::<CallbackContext>() };
            // SAFETY: the ABI requires a writable response_len pointer.
            unsafe { buffers.response_len.write(0) };
            let channel = match role {
                ServiceRole::Executor => &context.executor,
                ServiceRole::Planner => &context.planner,
                ServiceRole::OptimizerSubmission => &context.optimizer,
                _ => return CALLBACK_INVALID_ARGUMENT,
            };
            let payload = match channel.request(operation, request_bytes.to_vec()) {
                Ok(payload) => payload,
                Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                    return CALLBACK_INVALID_ARGUMENT;
                }
                Err(error) if error.kind() == io::ErrorKind::InvalidInput => {
                    return CALLBACK_LIMIT_EXCEEDED;
                }
                Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                    return CALLBACK_PROTOCOL_ERROR;
                }
                Err(_) => return CALLBACK_IO_ERROR,
            };
            // SAFETY: response_len was checked non-null above.
            unsafe { buffers.response_len.write(payload.len() as u64) };
            if payload.len() as u64 > buffers.response_capacity {
                return CALLBACK_RESPONSE_TOO_SMALL;
            }
            if !payload.is_empty() {
                if buffers.response.is_null() {
                    return CALLBACK_INVALID_ARGUMENT;
                }
                // SAFETY: caller supplied a writable response buffer of at
                // least response_capacity bytes.
                unsafe {
                    std::ptr::copy_nonoverlapping(payload.as_ptr(), buffers.response, payload.len())
                };
            }
            CALLBACK_OK
        }))
        .unwrap_or(CALLBACK_INTERNAL_ERROR)
    }

    unsafe extern "C" fn write_result_v2(
        context: *mut c_void,
        bytes: *const u8,
        bytes_len: u64,
    ) -> i32 {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if context.is_null() || (bytes_len > 0 && bytes.is_null()) {
                return CALLBACK_INVALID_ARGUMENT;
            }
            if bytes_len > MAX_RESULT_CHUNK_BYTES {
                return CALLBACK_LIMIT_EXCEEDED;
            }
            if bytes_len == 0 {
                return CALLBACK_OK;
            }
            // SAFETY: ABI callers must provide a readable buffer for bytes_len.
            let output = unsafe { std::slice::from_raw_parts(bytes, bytes_len as usize) };
            // SAFETY: context is host-owned and lives for the entry-point call.
            let context = unsafe { &*context.cast::<CallbackContext>() };
            let next_total = context.total_result_bytes.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |total| {
                    total
                        .checked_add(bytes_len)
                        .filter(|total| *total <= context.max_total_result_bytes)
                },
            );
            if next_total.is_err() {
                return CALLBACK_LIMIT_EXCEEDED;
            }
            let Ok(mut result) = context.result.lock() else {
                return CALLBACK_INTERNAL_ERROR;
            };
            let length = (bytes_len as u32).to_be_bytes();
            if result
                .write_all(&length)
                .and_then(|()| result.write_all(output))
                .is_err()
            {
                return CALLBACK_IO_ERROR;
            }
            CALLBACK_OK
        }))
        .unwrap_or(CALLBACK_INTERNAL_ERROR)
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

    fn lookup_optional_symbol(
        handle: *mut libc::c_void,
        symbol: &CStr,
    ) -> io::Result<Option<*mut libc::c_void>> {
        // SAFETY: dlerror and dlsym receive a valid loader handle and a
        // NUL-terminated symbol name.
        unsafe { libc::dlerror() };
        let address = unsafe { libc::dlsym(handle, symbol.as_ptr()) };
        let error = unsafe { libc::dlerror() };
        if !error.is_null() {
            return Ok(None);
        }
        if address.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("{} resolved to a null symbol", symbol.to_string_lossy()),
            ));
        }
        Ok(Some(address))
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

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::os::fd::IntoRawFd;

        #[test]
        fn worker_requires_a_distinct_dedicated_spill_channel_fd() {
            let arguments = [
                "--library",
                "/tmp/query.so",
                "--parameters-fd",
                "3",
                "--planner-fd",
                "4",
                "--executor-fd",
                "5",
                "--optimizer-fd",
                "6",
                "--result-fd",
                "7",
                "--spill-fd",
                "8",
                "--max-total-result-bytes",
                "67108864",
                "--max-sql-index-probe-candidates",
                "128",
            ];
            let values = arguments.map(std::ffi::OsString::from);
            let parsed = parse_args(&values).unwrap();
            assert_eq!(parsed.spill_fd, 8);

            let mut disabled = arguments.map(std::ffi::OsString::from);
            disabled[13] = std::ffi::OsString::from("-1");
            assert_eq!(parse_args(&disabled).unwrap().spill_fd, -1);

            let duplicate = [
                "--library",
                "/tmp/query.so",
                "--parameters-fd",
                "3",
                "--planner-fd",
                "4",
                "--executor-fd",
                "5",
                "--optimizer-fd",
                "6",
                "--result-fd",
                "7",
                "--spill-fd",
                "7",
                "--max-total-result-bytes",
                "67108864",
                "--max-sql-index-probe-candidates",
                "128",
            ]
            .map(std::ffi::OsString::from);
            assert!(parse_args(&duplicate).is_err());

            let legacy = [
                "--library",
                "/tmp/query.so",
                "--parameters-fd",
                "3",
                "--planner-fd",
                "4",
                "--executor-fd",
                "5",
                "--optimizer-fd",
                "6",
                "--result-fd",
                "7",
            ]
            .map(std::ffi::OsString::from);
            assert!(parse_args(&legacy).is_err());
        }

        fn callback_context(
            planner: UnixStream,
            executor: UnixStream,
            result: UnixStream,
        ) -> CallbackContext {
            let (optimizer, _optimizer_peer) = UnixStream::pair().unwrap();
            callback_context_with_optimizer(planner, executor, optimizer, result)
        }

        fn callback_context_with_optimizer(
            planner: UnixStream,
            executor: UnixStream,
            optimizer: UnixStream,
            result: UnixStream,
        ) -> CallbackContext {
            let (spill, _spill_peer) = UnixStream::pair().unwrap();
            // SAFETY: each stream contributes one unique fd and ownership is
            // transferred into the callback context.
            unsafe {
                CallbackContext::from_raw_fds(
                    planner.into_raw_fd(),
                    executor.into_raw_fd(),
                    optimizer.into_raw_fd(),
                    result.into_raw_fd(),
                    Some(spill.into_raw_fd()),
                    prdb_engine::query_abi::MAX_TOTAL_RESULT_BYTES,
                    prdb_engine::query_abi::DEFAULT_SQL_INDEX_PROBE_CANDIDATES,
                )
            }
        }

        fn spill_callback_context() -> (CallbackContext, UnixStream) {
            let (planner, _planner_peer) = UnixStream::pair().unwrap();
            let (executor, _executor_peer) = UnixStream::pair().unwrap();
            let (optimizer, _optimizer_peer) = UnixStream::pair().unwrap();
            let (result, _result_peer) = UnixStream::pair().unwrap();
            let (spill, spill_peer) = UnixStream::pair().unwrap();
            // SAFETY: each stream contributes a distinct owned endpoint.
            let context = unsafe {
                CallbackContext::from_raw_fds(
                    planner.into_raw_fd(),
                    executor.into_raw_fd(),
                    optimizer.into_raw_fd(),
                    result.into_raw_fd(),
                    Some(spill.into_raw_fd()),
                    prdb_engine::query_abi::MAX_TOTAL_RESULT_BYTES,
                    prdb_engine::query_abi::DEFAULT_SQL_INDEX_PROBE_CANDIDATES,
                )
            };
            (context, spill_peer)
        }

        fn answer_spill_request(mut peer: UnixStream, status: u16, body: Vec<u8>) {
            let mut request_header = [0u8; SPILL_CHANNEL_HEADER_BYTES as usize];
            peer.read_exact(&mut request_header).unwrap();
            let request_len =
                u32::from_be_bytes(request_header[16..20].try_into().unwrap()) as usize;
            let mut request = vec![0; request_len];
            peer.read_exact(&mut request).unwrap();
            let operation = u16::from_be_bytes([request_header[6], request_header[7]]);
            let request_id = &request_header[8..16];
            let mut response_header = [0u8; SPILL_CHANNEL_HEADER_BYTES as usize];
            response_header[..4].copy_from_slice(SPILL_CHANNEL_MAGIC.as_bytes());
            response_header[4..6].copy_from_slice(&SPILL_CHANNEL_VERSION.to_be_bytes());
            response_header[6..8].copy_from_slice(&(operation | 0x8000).to_be_bytes());
            response_header[8..16].copy_from_slice(request_id);
            response_header[16..20].copy_from_slice(&((body.len() + 2) as u32).to_be_bytes());
            peer.write_all(&response_header).unwrap();
            peer.write_all(&status.to_be_bytes()).unwrap();
            peer.write_all(&body).unwrap();
        }

        #[test]
        fn spill_callback_relays_bounded_bytes_and_latches_broker_failures() {
            let (context, peer) = spill_callback_context();
            let responder = std::thread::spawn(move || {
                answer_spill_request(peer, SPILL_STATUS_OK, vec![0x5a; 16]);
            });
            let mut response = [0u8; 16];
            let mut response_len = 0u64;
            // SAFETY: pointers and context remain valid for the callback.
            let result = unsafe {
                spill_request_v1(
                    (&context as *const CallbackContext).cast_mut().cast(),
                    SPILL_OP_CREATE,
                    std::ptr::null(),
                    0,
                    response.as_mut_ptr(),
                    response.len() as u64,
                    &mut response_len,
                )
            };
            responder.join().unwrap();
            assert_eq!(result, CALLBACK_OK);
            assert_eq!(response_len, 16);
            assert_eq!(response, [0x5a; 16]);
            assert!(!context.statement_failed.load(Ordering::SeqCst));

            let (failed_context, peer) = spill_callback_context();
            let responder = std::thread::spawn(move || answer_spill_request(peer, 6, Vec::new()));
            // SAFETY: callback uses valid context and response-length storage.
            let result = unsafe {
                spill_request_v1(
                    (&failed_context as *const CallbackContext)
                        .cast_mut()
                        .cast(),
                    SPILL_OP_CREATE,
                    std::ptr::null(),
                    0,
                    response.as_mut_ptr(),
                    response.len() as u64,
                    &mut response_len,
                )
            };
            responder.join().unwrap();
            assert_eq!(result, CALLBACK_LIMIT_EXCEEDED);
            assert!(failed_context.statement_failed.load(Ordering::SeqCst));
            assert_ne!(query_exit_status(0, true), 0);
        }

        #[test]
        fn v6_spill_callback_roundtrips_through_the_trusted_broker() {
            use prdb_engine::spill::{SpillBroker, SpillConfig, SpillPackage};
            use prdb_engine::supervisor::WorkloadClass;
            use std::os::unix::fs::PermissionsExt;
            use std::time::{Duration, Instant};

            let root = std::env::temp_dir().join(format!(
                "prdb-executor-spill-{}-{:032x}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir(&root).unwrap();
            std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
            let broker = SpillBroker::open(SpillConfig {
                parent: root.clone(),
                max_bytes_per_invocation: 8192,
                max_io_bytes_per_invocation: 32768,
                max_streams_per_invocation: 4,
                max_operations_per_invocation: 32,
                max_response_bytes: 4096,
                max_service_bytes: 16384,
            })
            .unwrap();
            let package = SpillPackage::new([0x31; 16], [0x32; 16]);
            broker
                .open_package(
                    package,
                    WorkloadClass::Analytical,
                    Instant::now() + Duration::from_secs(10),
                )
                .unwrap();
            let (context, broker_stream) = spill_callback_context();
            let serving_broker = broker.clone();
            let server =
                std::thread::spawn(move || serving_broker.serve_channel(broker_stream, package));
            let context_ptr = (&context as *const CallbackContext).cast_mut().cast();
            let mut handle = [0u8; 16];
            let mut response_len = 0u64;
            // SAFETY: every callback pointer refers to live bounded test storage.
            assert_eq!(
                unsafe {
                    spill_request_v1(
                        context_ptr,
                        SPILL_OP_CREATE,
                        std::ptr::null(),
                        0,
                        handle.as_mut_ptr(),
                        handle.len() as u64,
                        &mut response_len,
                    )
                },
                CALLBACK_OK
            );
            assert_eq!(response_len, 16);

            let payload = b"through-broker";
            let mut append = handle.to_vec();
            append.extend_from_slice(payload);
            // SAFETY: request and response buffers are valid for the callback.
            assert_eq!(
                unsafe {
                    spill_request_v1(
                        context_ptr,
                        SPILL_OP_APPEND,
                        append.as_ptr(),
                        append.len() as u64,
                        std::ptr::null_mut(),
                        0,
                        &mut response_len,
                    )
                },
                CALLBACK_OK
            );

            // SAFETY: handle is a live 16-byte request buffer.
            assert_eq!(
                unsafe {
                    spill_request_v1(
                        context_ptr,
                        SPILL_OP_SEAL,
                        handle.as_ptr(),
                        handle.len() as u64,
                        std::ptr::null_mut(),
                        0,
                        &mut response_len,
                    )
                },
                CALLBACK_OK
            );

            let mut read_request = Vec::from(handle);
            read_request.extend_from_slice(&0u64.to_be_bytes());
            read_request.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            let mut read_response = [0u8; 32];
            // SAFETY: read request and response buffers are valid and bounded.
            assert_eq!(
                unsafe {
                    spill_request_v1(
                        context_ptr,
                        SPILL_OP_READ_AT,
                        read_request.as_ptr(),
                        read_request.len() as u64,
                        read_response.as_mut_ptr(),
                        read_response.len() as u64,
                        &mut response_len,
                    )
                },
                CALLBACK_OK
            );
            assert_eq!(response_len, payload.len() as u64);
            assert_eq!(&read_response[..payload.len()], payload);

            // SAFETY: handle is a live 16-byte request buffer.
            assert_eq!(
                unsafe {
                    spill_request_v1(
                        context_ptr,
                        SPILL_OP_DESTROY,
                        handle.as_ptr(),
                        handle.len() as u64,
                        std::ptr::null_mut(),
                        0,
                        &mut response_len,
                    )
                },
                CALLBACK_OK
            );
            drop(context);
            assert_eq!(server.join().unwrap(), Ok(()));
            broker.close_package(package).unwrap();
            std::fs::remove_dir_all(root).unwrap();
        }

        #[test]
        fn optimizer_callback_uses_fixed_operation_and_bounded_response() {
            let (planner, _planner_peer) = UnixStream::pair().unwrap();
            let (executor, _executor_peer) = UnixStream::pair().unwrap();
            let (optimizer, mut optimizer_peer) = UnixStream::pair().unwrap();
            let (result, _result_peer) = UnixStream::pair().unwrap();
            let context = callback_context_with_optimizer(planner, executor, optimizer, result);
            let context_ptr = (&context as *const CallbackContext).cast_mut().cast();
            let server = std::thread::spawn(move || {
                for request_id in 1..=2 {
                    let request = read_frame(&mut optimizer_peer).unwrap().unwrap();
                    assert_eq!(request.operation, Operation::OptimizeSelect as u16);
                    assert_eq!(request.request_id, request_id);
                    assert_eq!(request.payload, b"select request");
                    write_frame(
                        &mut optimizer_peer,
                        &IpcFrame {
                            operation: request.operation,
                            request_id,
                            payload: vec![0xa1, 0xb2, 0xc3],
                        },
                    )
                    .unwrap();
                }
            });

            let request = b"select request";
            let mut response = [0x7f; 3];
            let mut response_len = 0;
            // SAFETY: all callback buffers and context remain live and sized.
            let status = unsafe {
                optimizer_request_v4(
                    context_ptr,
                    request.as_ptr(),
                    request.len() as u64,
                    response.as_mut_ptr(),
                    response.len() as u64,
                    &mut response_len,
                )
            };
            assert_eq!(status, CALLBACK_OK);
            assert_eq!(response_len, 3);
            assert_eq!(response, [0xa1, 0xb2, 0xc3]);

            let mut small_response = [0x7f; 2];
            response_len = 0;
            // SAFETY: callback buffers remain live; undersizing is reported
            // without copying any response bytes.
            let status = unsafe {
                optimizer_request_v4(
                    context_ptr,
                    request.as_ptr(),
                    request.len() as u64,
                    small_response.as_mut_ptr(),
                    small_response.len() as u64,
                    &mut response_len,
                )
            };
            assert_eq!(status, CALLBACK_RESPONSE_TOO_SMALL);
            assert_eq!(response_len, 3);
            assert_eq!(small_response, [0x7f; 2]);
            server.join().unwrap();
        }

        #[test]
        fn optimizer_v5_callback_selects_an_allowlisted_operation() {
            let (planner, _planner_peer) = UnixStream::pair().unwrap();
            let (executor, _executor_peer) = UnixStream::pair().unwrap();
            let (optimizer, mut optimizer_peer) = UnixStream::pair().unwrap();
            let (result, _result_peer) = UnixStream::pair().unwrap();
            let context = callback_context_with_optimizer(planner, executor, optimizer, result);
            let context_ptr = (&context as *const CallbackContext).cast_mut().cast();
            let server = std::thread::spawn(move || {
                let request = read_frame(&mut optimizer_peer).unwrap().unwrap();
                assert_eq!(request.operation, Operation::OptimizeDelete as u16);
                assert_eq!(request.payload, b"delete request");
                write_frame(
                    &mut optimizer_peer,
                    &IpcFrame {
                        operation: request.operation,
                        request_id: request.request_id,
                        payload: b"response".to_vec(),
                    },
                )
                .unwrap();
            });
            let request = b"delete request";
            let mut response = [0; 8];
            let mut response_len = 0;
            let status = unsafe {
                optimizer_request_v5(
                    context_ptr,
                    Operation::OptimizeDelete as u16,
                    request.as_ptr(),
                    request.len() as u64,
                    response.as_mut_ptr(),
                    response.len() as u64,
                    &mut response_len,
                )
            };
            assert_eq!(status, CALLBACK_OK);
            assert_eq!(response_len, 8);
            assert_eq!(&response, b"response");

            let status = unsafe {
                optimizer_request_v5(
                    context_ptr,
                    Operation::DescribePlanRelation as u16,
                    request.as_ptr(),
                    request.len() as u64,
                    response.as_mut_ptr(),
                    response.len() as u64,
                    &mut response_len,
                )
            };
            assert_eq!(status, CALLBACK_INVALID_ARGUMENT);
            server.join().unwrap();
        }

        #[test]
        fn executor_callback_checks_role_and_round_trips_framed_response() {
            let (planner, _planner_peer) = UnixStream::pair().unwrap();
            let (executor, mut executor_peer) = UnixStream::pair().unwrap();
            let (result, _result_peer) = UnixStream::pair().unwrap();
            let context = callback_context(planner, executor, result);
            let context_ptr = (&context as *const CallbackContext).cast_mut().cast();
            let peer = std::thread::spawn(move || {
                for request_id in 1..=2 {
                    let request = read_frame(&mut executor_peer).unwrap().unwrap();
                    assert_eq!(request.operation, Operation::ResolveRelation as u16);
                    assert_eq!(request.request_id, request_id);
                    assert_eq!(request.payload, b"request");
                    write_frame(
                        &mut executor_peer,
                        &IpcFrame {
                            operation: request.operation,
                            request_id: request.request_id,
                            payload: b"response".to_vec(),
                        },
                    )
                    .unwrap();
                }
            });

            let request = b"request";
            let mut response = [0u8; 16];
            let mut response_len = 0;
            // SAFETY: the callback context and buffers remain live and match
            // the lengths passed through the C ABI.
            let status = unsafe {
                executor_request_v2(
                    context_ptr,
                    Operation::ResolveRelation as u16,
                    request.as_ptr(),
                    request.len() as u64,
                    response.as_mut_ptr(),
                    response.len() as u64,
                    &mut response_len,
                )
            };
            assert_eq!(status, CALLBACK_OK);
            assert_eq!(response_len, 8);
            assert_eq!(&response[..response_len as usize], b"response");

            let mut short_response = [0u8; 2];
            let mut required_len = 0;
            // SAFETY: the callback context and buffers remain live and match
            // the lengths passed through the C ABI.
            let status = unsafe {
                executor_request_v2(
                    context_ptr,
                    Operation::ResolveRelation as u16,
                    request.as_ptr(),
                    request.len() as u64,
                    short_response.as_mut_ptr(),
                    short_response.len() as u64,
                    &mut required_len,
                )
            };
            assert_eq!(status, CALLBACK_RESPONSE_TOO_SMALL);
            assert_eq!(required_len, 8);
            assert_eq!(short_response, [0, 0]);
            peer.join().unwrap();

            // A valid operation code on the wrong provisioned role is denied
            // before any request is sent to that endpoint.
            let mut empty_response = [];
            let mut empty_len = 0;
            // SAFETY: the same host-owned context is still live.
            let status = unsafe {
                planner_request_v2(
                    context_ptr,
                    Operation::ResolveRelation as u16,
                    std::ptr::null(),
                    0,
                    empty_response.as_mut_ptr(),
                    0,
                    &mut empty_len,
                )
            };
            assert_eq!(status, CALLBACK_INVALID_ARGUMENT);
        }

        #[test]
        fn executor_callback_serves_trusted_workload_budget_locally() {
            let (planner, _planner_peer) = UnixStream::pair().unwrap();
            let (executor, mut executor_peer) = UnixStream::pair().unwrap();
            let (optimizer, _optimizer_peer) = UnixStream::pair().unwrap();
            let (result, _result_peer) = UnixStream::pair().unwrap();
            let (spill, _spill_peer) = UnixStream::pair().unwrap();
            // SAFETY: each stream contributes one distinct owned endpoint.
            let context = unsafe {
                CallbackContext::from_raw_fds(
                    planner.into_raw_fd(),
                    executor.into_raw_fd(),
                    optimizer.into_raw_fd(),
                    result.into_raw_fd(),
                    Some(spill.into_raw_fd()),
                    prdb_engine::query_abi::MAX_TOTAL_RESULT_BYTES,
                    512,
                )
            };
            let context_ptr = (&context as *const CallbackContext).cast_mut().cast();
            let mut response = [0u8; 12];
            let mut response_len = 0;
            // SAFETY: the trusted context and response buffer remain live.
            let status = unsafe {
                executor_request_v2(
                    context_ptr,
                    prdb_engine::query_abi::QUERY_HOST_OP_GET_WORKLOAD_BUDGETS,
                    std::ptr::null(),
                    0,
                    response.as_mut_ptr(),
                    response.len() as u64,
                    &mut response_len,
                )
            };
            assert_eq!(status, CALLBACK_OK);
            assert_eq!(response_len, 12);
            assert_eq!(&response[..8], b"PQB1\0\x01\0\x0c");
            assert_eq!(u32::from_be_bytes(response[8..12].try_into().unwrap()), 512);
            executor_peer
                .set_nonblocking(true)
                .expect("configure unused IPC peer");
            let mut unexpected = [0u8; 1];
            assert!(matches!(
                executor_peer.read(&mut unexpected),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock
            ));
        }

        #[test]
        fn result_callback_frames_output_and_enforces_invocation_quota() {
            let (planner, _planner_peer) = UnixStream::pair().unwrap();
            let (executor, _executor_peer) = UnixStream::pair().unwrap();
            let (result, mut result_peer) = UnixStream::pair().unwrap();
            let context = callback_context(planner, executor, result);
            let context_ptr = (&context as *const CallbackContext).cast_mut().cast();
            let bytes = b"application result";
            // SAFETY: the callback context and output slice remain live.
            let status =
                unsafe { write_result_v2(context_ptr, bytes.as_ptr(), bytes.len() as u64) };
            assert_eq!(status, CALLBACK_OK);
            let mut header = [0u8; 4];
            result_peer.read_exact(&mut header).unwrap();
            assert_eq!(u32::from_be_bytes(header), bytes.len() as u32);
            let mut received = vec![0; bytes.len()];
            result_peer.read_exact(&mut received).unwrap();
            assert_eq!(received, bytes);

            context.total_result_bytes.store(
                prdb_engine::query_abi::MAX_TOTAL_RESULT_BYTES,
                Ordering::Relaxed,
            );
            // SAFETY: context and output slice remain valid; quota rejection
            // occurs before the result stream is touched.
            let status =
                unsafe { write_result_v2(context_ptr, bytes.as_ptr(), bytes.len() as u64) };
            assert_eq!(status, CALLBACK_LIMIT_EXCEEDED);
        }

        #[test]
        fn result_callback_streams_above_default_budget_when_trusted_limit_is_raised() {
            let elevated_limit = 128 * 1024 * 1024u64;
            let chunk = vec![0x5a; MAX_RESULT_CHUNK_BYTES as usize];
            let chunk_len = chunk.len() as u64;
            let frame_count = elevated_limit / chunk_len;
            assert_eq!(elevated_limit % chunk_len, 0);

            let (planner, _planner_peer) = UnixStream::pair().unwrap();
            let (executor, _executor_peer) = UnixStream::pair().unwrap();
            let (result, mut result_peer) = UnixStream::pair().unwrap();
            let mut context = callback_context(planner, executor, result);
            context.max_total_result_bytes = elevated_limit;
            let context_ptr = (&context as *const CallbackContext).cast_mut().cast();
            let reader = std::thread::spawn(move || {
                let mut received = 0u64;
                let mut payload = vec![0; MAX_RESULT_CHUNK_BYTES as usize];
                for _ in 0..frame_count {
                    let mut header = [0u8; 4];
                    result_peer.read_exact(&mut header).unwrap();
                    assert_eq!(u32::from_be_bytes(header), chunk_len as u32);
                    result_peer.read_exact(&mut payload).unwrap();
                    assert!(payload.iter().all(|byte| *byte == 0x5a));
                    received += payload.len() as u64;
                }
                received
            });

            for _ in 0..frame_count {
                // SAFETY: callback context and chunk bytes remain live for each call.
                assert_eq!(
                    unsafe { write_result_v2(context_ptr, chunk.as_ptr(), chunk_len) },
                    CALLBACK_OK
                );
            }
            assert_eq!(reader.join().unwrap(), elevated_limit);
            assert_eq!(
                context.total_result_bytes.load(Ordering::Relaxed),
                elevated_limit
            );
        }

        #[test]
        fn caught_statement_failure_forces_worker_failure() {
            let (planner, _planner_peer) = UnixStream::pair().unwrap();
            let (executor, _executor_peer) = UnixStream::pair().unwrap();
            let (result, _result_peer) = UnixStream::pair().unwrap();
            let context = callback_context(planner, executor, result);
            let context_ptr = (&context as *const CallbackContext).cast_mut().cast();
            // SAFETY: the host-owned callback context is live and valid.
            assert_eq!(unsafe { statement_failure_v3(context_ptr, 1) }, CALLBACK_OK);
            assert!(context.statement_failed.load(Ordering::SeqCst));
            // A query may catch the SDK return and report success. The trusted
            // worker still converts the sticky failure into process failure.
            assert_eq!(query_exit_status(0, true), 1);
            assert_eq!(query_exit_status(17, true), 1);
            assert_eq!(query_exit_status(0, false), 0);
        }
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn main() {
    worker::main();
}
