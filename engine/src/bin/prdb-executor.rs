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
        BorrowedInvocationV1, HostApiV1, HostApiV2, HostApiV3, InvocationV2, InvocationV3,
        MAX_IPC_REQUEST_BYTES, MAX_RESULT_CHUNK_BYTES, MAX_TOTAL_RESULT_BYTES,
        QUERY_ABI_V2_VERSION, QUERY_ABI_V3_VERSION, QUERY_ABI_VERSION, QUERY_DESCRIPTOR_SYMBOL,
        QUERY_DESCRIPTOR_V2_SYMBOL, QUERY_DESCRIPTOR_V3_SYMBOL, QUERY_ENTRY_SYMBOL,
        QUERY_ENTRY_V2_SYMBOL, QUERY_ENTRY_V3_SYMBOL, QUERY_STATEMENT_FAILURE_SQL,
        QueryDescriptorV1, QueryDescriptorV2, QueryDescriptorV3, QueryIpcRequestV2, QueryMainV1,
        QueryMainV2, QueryMainV3, QueryStatementFailureV3, QueryWriteResultV2,
        validate_descriptor_v1, validate_descriptor_v2, validate_descriptor_v3,
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
        result_fd: i32,
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
        if values.len() != 10
            || values[0] != "--library"
            || values[2] != "--parameters-fd"
            || values[4] != "--planner-fd"
            || values[6] != "--executor-fd"
            || values[8] != "--result-fd"
        {
            return Err("usage: prdb-executor --library ABSOLUTE_PATH --parameters-fd FD --planner-fd FD --executor-fd FD --result-fd FD (or --self-check)".to_owned());
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
        let result_fd = values[9]
            .to_str()
            .ok_or_else(|| "result descriptor is not UTF-8".to_owned())?
            .parse::<i32>()
            .map_err(|_| "invalid result descriptor".to_owned())?;
        if parameters_fd < 3 || planner_fd < 3 || executor_fd < 3 || result_fd < 3 {
            return Err("worker descriptors must be greater than stderr".to_owned());
        }
        let descriptors = [parameters_fd, planner_fd, executor_fd, result_fd];
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
            result_fd,
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

        // Prefer v3, then v2. An invalid descriptor is rejected rather than
        // falling back to a second ABI exported by the same library.
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
            CallbackContext::from_raw_fds(args.planner_fd, args.executor_fd, args.result_fd)
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
            CallbackContext::from_raw_fds(args.planner_fd, args.executor_fd, args.result_fd)
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
        result: Mutex<UnixStream>,
        total_result_bytes: AtomicU64,
        statement_failed: AtomicBool,
    }

    impl CallbackContext {
        unsafe fn from_raw_fds(planner_fd: i32, executor_fd: i32, result_fd: i32) -> Self {
            // SAFETY: the trusted launcher passes distinct, open socket fds and
            // transfers ownership to this invocation context.
            let planner = unsafe { IpcChannel::from_raw_fd(planner_fd, ServiceRole::Planner) };
            // SAFETY: same descriptor ownership contract as the planner fd.
            let executor = unsafe { IpcChannel::from_raw_fd(executor_fd, ServiceRole::Executor) };
            // SAFETY: the result fd is a distinct connected Unix stream.
            let result = unsafe { UnixStream::from_raw_fd(result_fd) };
            Self {
                executor,
                planner,
                result: Mutex::new(result),
                total_result_bytes: AtomicU64::new(0),
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

    unsafe extern "C" fn executor_request_v2(
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
                        .filter(|total| *total <= MAX_TOTAL_RESULT_BYTES)
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

        fn callback_context(
            planner: UnixStream,
            executor: UnixStream,
            result: UnixStream,
        ) -> CallbackContext {
            // SAFETY: each stream contributes one unique fd and ownership is
            // transferred into the callback context.
            unsafe {
                CallbackContext::from_raw_fds(
                    planner.into_raw_fd(),
                    executor.into_raw_fd(),
                    result.into_raw_fd(),
                )
            }
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

            context
                .total_result_bytes
                .store(MAX_TOTAL_RESULT_BYTES, Ordering::Relaxed);
            // SAFETY: context and output slice remain valid; quota rejection
            // occurs before the result stream is touched.
            let status =
                unsafe { write_result_v2(context_ptr, bytes.as_ptr(), bytes.len() as u64) };
            assert_eq!(status, CALLBACK_LIMIT_EXCEEDED);
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
