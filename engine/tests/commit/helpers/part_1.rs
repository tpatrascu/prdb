use prdb_engine::audit::{AuditAction, AuditEvent, AuditResult};
use prdb_engine::catalog::{CatalogField, WireType, encode_envelope, family_schema};
use prdb_engine::commit::CatalogWrite;
use prdb_engine::metadata::{EngineMetadata, Lifecycle};
use prdb_engine::row_record::RowRecord;
use prdb_engine::schema::{self, CATALOG, ENGINE_META, PRINCIPAL_NAMES, ROWS};
use prdb_engine::storage_manager::StorageManager;
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, encode_tuple};
use redb::{Database, ReadableDatabase, StorageBackend};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use super::*;

pub static NEXT: AtomicU64 = AtomicU64::new(0);

pub struct SpillDiskFullFixture {
    pub filler_path: std::path::PathBuf,
    pub spill_parent: std::path::PathBuf,
    pub filler_created: std::cell::Cell<bool>,
}

impl SpillDiskFullFixture {
    pub fn new(filesystem_root: &std::path::Path, spill_parent: &std::path::Path) -> Self {
        Self {
            filler_path: filesystem_root.join("prdb-spill-test-filler"),
            spill_parent: spill_parent.to_path_buf(),
            filler_created: std::cell::Cell::new(false),
        }
    }

    pub fn fill_until_below_eight_kibibytes(&self) -> Result<(), Box<dyn std::error::Error>> {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::OpenOptionsExt;

        let filler = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&self.filler_path)?;
        self.filler_created.set(true);
        let block = 1024 * 1024u64;
        let tail_block = 4 * 1024u64;
        let available_headroom = 8 * 1024u64;
        let mut offset = 0u64;
        loop {
            let mut capacity: libc::statvfs = unsafe { std::mem::zeroed() };
            // SAFETY: fstatvfs writes capacity for this live filler-file descriptor.
            if unsafe { libc::fstatvfs(filler.as_raw_fd(), &mut capacity) } != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            let available = capacity.f_bavail.saturating_mul(capacity.f_frsize);
            if available < available_headroom {
                break;
            }
            let length = if available > 2 * block {
                block
            } else {
                tail_block
            };
            let start = libc::off_t::try_from(offset)?;
            let length = libc::off_t::try_from(length)?;
            // SAFETY: posix_fallocate reserves blocks in this test-owned regular file.
            let result = unsafe { libc::posix_fallocate(filler.as_raw_fd(), start, length) };
            if result != 0 {
                if result == libc::ENOSPC {
                    break;
                }
                return Err(std::io::Error::from_raw_os_error(result).into());
            }
            offset = offset
                .checked_add(u64::try_from(length)?)
                .ok_or("spill disk-full fixture offset overflow")?;
        }
        let mut capacity: libc::statvfs = unsafe { std::mem::zeroed() };
        // SAFETY: fstatvfs writes capacity for this live filler-file descriptor.
        if unsafe { libc::fstatvfs(filler.as_raw_fd(), &mut capacity) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let available = capacity.f_bavail.saturating_mul(capacity.f_frsize);
        if available >= available_headroom {
            return Err(format!(
                "could not leave less than 8 KiB free on spill filesystem ({available} bytes remain)"
            )
            .into());
        }
        drop(filler);
        Ok(())
    }
}

impl Drop for SpillDiskFullFixture {
    fn drop(&mut self) {
        if self.filler_created.replace(false) {
            let _ = std::fs::remove_file(&self.filler_path);
        }
        let _ = std::fs::remove_file(self.spill_parent.join(".prdb-spill.lock"));
        let _ = std::fs::remove_dir(&self.spill_parent);
    }
}

#[derive(Debug)]
pub struct FailNextSyncBackend {
    pub inner: redb::backends::FileBackend,
    pub fail_next_sync: Arc<AtomicBool>,
}

impl StorageBackend for FailNextSyncBackend {
    fn len(&self) -> Result<u64, std::io::Error> {
        self.inner.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), std::io::Error> {
        self.inner.read(offset, out)
    }

    fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
        self.inner.set_len(len)
    }

    fn sync_data(&self) -> Result<(), std::io::Error> {
        if self.fail_next_sync.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::other("injected commit sync failure"));
        }
        self.inner.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
        self.inner.write(offset, data)
    }

    fn close(&self) -> Result<(), std::io::Error> {
        self.inner.close()
    }
}

#[derive(Debug)]
pub struct ExitAfterNextWriteBackend {
    pub inner: redb::backends::FileBackend,
    pub armed: Arc<AtomicBool>,
    pub exit_code: i32,
}

impl StorageBackend for ExitAfterNextWriteBackend {
    fn len(&self) -> Result<u64, std::io::Error> {
        self.inner.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), std::io::Error> {
        self.inner.read(offset, out)
    }

    fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
        self.inner.set_len(len)
    }

    fn sync_data(&self) -> Result<(), std::io::Error> {
        self.inner.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
        self.inner.write(offset, data)?;
        if self.armed.swap(false, Ordering::SeqCst) {
            std::process::exit(self.exit_code);
        }
        Ok(())
    }

    fn close(&self) -> Result<(), std::io::Error> {
        self.inner.close()
    }
}

#[derive(Debug)]
pub struct PauseNextWriteBackend {
    pub inner: redb::backends::FileBackend,
    pub state: Arc<PauseNextWriteState>,
}

#[derive(Debug)]
pub struct PauseNextWriteState {
    pub armed: AtomicBool,
    pub entered: Barrier,
    pub release: Barrier,
}

impl PauseNextWriteState {
    pub fn new() -> Self {
        Self {
            armed: AtomicBool::new(false),
            entered: Barrier::new(2),
            release: Barrier::new(2),
        }
    }
}

impl StorageBackend for PauseNextWriteBackend {
    fn len(&self) -> Result<u64, std::io::Error> {
        self.inner.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), std::io::Error> {
        self.inner.read(offset, out)
    }

    fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
        self.inner.set_len(len)
    }

    fn sync_data(&self) -> Result<(), std::io::Error> {
        self.inner.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
        if self.state.armed.swap(false, Ordering::SeqCst) {
            self.state.entered.wait();
            self.state.release.wait();
        }
        self.inner.write(offset, data)
    }

    fn close(&self) -> Result<(), std::io::Error> {
        self.inner.close()
    }
}

pub struct Fixture(pub std::path::PathBuf);

impl Fixture {
    pub fn new() -> Self {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("prdb-commit-{}-{id}", std::process::id()));
        std::fs::create_dir(&dir).unwrap();
        Self(dir)
    }
    pub fn path(&self) -> std::path::PathBuf {
        self.0.join("commit.redb")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;

        fn make_removable(path: &std::path::Path) {
            let metadata = std::fs::symlink_metadata(path).unwrap();
            if metadata.file_type().is_symlink() {
                return;
            }
            if metadata.is_dir() {
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
                for entry in std::fs::read_dir(path).unwrap() {
                    make_removable(&entry.unwrap().path());
                }
            } else {
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
            }
        }

        make_removable(&self.0);
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

pub fn key(n: u8) -> [u8; 32] {
    let mut key = [0x10; 32];
    key[31] = n;
    key
}

pub fn table_row_key(table_id: [u8; 16], row_id: [u8; 16]) -> [u8; 32] {
    let mut key = [0; 32];
    key[..16].copy_from_slice(&table_id);
    key[16..].copy_from_slice(&row_id);
    key
}

pub fn executor_ipc_scan_cells(
    invocation: &mut prdb_engine::transaction::InvocationTransaction<'_>,
    schema_name: &str,
    table_name: &str,
    column_ids: Vec<[u8; 16]>,
) -> Result<Vec<Vec<prdb_engine::authorization::SanitizedCell>>, Box<dyn std::error::Error>> {
    use prdb_engine::executor_ipc::{
        NextBatchRequest, NextBatchResponse, OpenScanRequest, OpenScanResponse,
        ResolveRelationRequest, ResolveRelationResponse, dispatch_executor_request,
    };
    use prdb_engine::ipc::{IpcFrame, Operation, ServiceRole};

    let mut connection = invocation.open_executor_metadata_connection();
    let resolved = dispatch_executor_request(
        ServiceRole::Executor,
        &mut connection,
        &IpcFrame {
            operation: Operation::ResolveRelation as u16,
            request_id: 1,
            payload: ResolveRelationRequest {
                schema_name: schema_name.to_owned(),
                relation_name: table_name.to_owned(),
            }
            .encode()?,
        },
    )
    .map_err(|error| format!("executor IPC resolve failed: {error:?}"))?;
    let ResolveRelationResponse::Resolved { handle, .. } =
        ResolveRelationResponse::decode(&resolved.payload)?
    else {
        return Err("authorized relation did not resolve".into());
    };
    let opened = dispatch_executor_request(
        ServiceRole::Executor,
        &mut connection,
        &IpcFrame {
            operation: Operation::OpenScan as u16,
            request_id: 2,
            payload: OpenScanRequest {
                relation: handle,
                column_ids,
            }
            .encode()?,
        },
    )
    .map_err(|error| format!("executor IPC open scan failed: {error:?}"))?;
    let cursor = OpenScanResponse::decode(&opened.payload)?.cursor;
    let page = dispatch_executor_request(
        ServiceRole::Executor,
        &mut connection,
        &IpcFrame {
            operation: Operation::NextBatch as u16,
            request_id: 3,
            payload: NextBatchRequest {
                cursor: cursor.clone(),
                max_scanned: 16,
                max_bytes: 4096,
            }
            .encode()?,
        },
    )
    .map_err(|error| format!("executor IPC next batch failed: {error:?}"))?;
    let page = NextBatchResponse::decode(&page.payload)?;
    if page.continuation.is_some() {
        return Err("bounded cell-view fixture unexpectedly required a continuation".into());
    }
    Ok(page.rows.into_iter().map(|row| row.cells).collect())
}

pub fn executor_ipc_rejects_forged_handles(
    invocation: &mut prdb_engine::transaction::InvocationTransaction<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::executor_ipc::{
        OpenScanRequest, StageDeleteRequest, StageInsertRequest, StageUpdateRequest,
        dispatch_executor_request,
    };
    use prdb_engine::ipc::{IpcFrame, Operation, ServiceRole};
    use prdb_engine::metadata_discovery::{ExecutorRowHandle, ResolvedRelationHandle};
    use prdb_engine::transaction::InsertInput;
    use prdb_engine::tuple::FieldPatch;

    let mut connection = invocation.open_executor_metadata_connection();
    let forged_relation =
        ResolvedRelationHandle::from_bytes(&[6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
            .ok_or("invalid forged relation test handle")?;
    let forged_row =
        ExecutorRowHandle::from_bytes(&[8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
            .ok_or("invalid forged row test handle")?;
    let requests = [
        (
            Operation::OpenScan,
            OpenScanRequest {
                relation: forged_relation.clone(),
                column_ids: Vec::new(),
            }
            .encode()?,
        ),
        (
            Operation::StageInsert,
            StageInsertRequest {
                relation: forged_relation,
                values: vec![InsertInput::Value(Some(ScalarValue::Bytea(Vec::new())))],
            }
            .encode()?,
        ),
        (
            Operation::StageDelete,
            StageDeleteRequest {
                handle: forged_row.clone(),
            }
            .encode(),
        ),
        (
            Operation::StageUpdate,
            StageUpdateRequest {
                handle: forged_row,
                patches: vec![FieldPatch {
                    ordinal: 0,
                    value: None,
                }],
            }
            .encode()?,
        ),
    ];
    for (request_id, (operation, payload)) in requests.into_iter().enumerate() {
        let result = dispatch_executor_request(
            ServiceRole::Executor,
            &mut connection,
            &IpcFrame {
                operation: operation as u16,
                request_id: request_id as u64 + 1,
                payload,
            },
        );
        if result.is_ok() {
            return Err(format!("executor accepted forged {operation:?} handle").into());
        }
    }
    Ok(())
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub fn run_sandboxed_forged_handle_client(
    storage: &StorageManager,
    credentials: &prdb_engine::client_protocol::ClientCredentials,
    user_id: [u8; 16],
    now_us: i64,
    artifact_env: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::client_protocol::{ClientConnectionSession, ClientFrame, ClientMode};
    use prdb_engine::ipc::{Operation, read_frame};
    use prdb_engine::supervisor::{
        ChunkKind, PrepareSubmission, StagingLimits, StagingStore, SubmissionMetadata,
        SubmissionMetadataValidator, WorkloadClass,
    };
    use prdb_engine::supervisor_ipc::{
        GatewayAdmissionConfig, GatewayExecutorConfig, GatewayTerminalDecision,
        observe_executor_relay_and_finalize_gateway,
    };
    use sha2::{Digest, Sha256};
    use std::os::unix::net::UnixStream;
    use std::path::PathBuf;
    use std::time::Duration;

    struct AcceptFixture;
    impl SubmissionMetadataValidator for AcceptFixture {
        fn supports_versions(&self, manifest_version: u32, scope_version: u32) -> bool {
            manifest_version == 1 && scope_version == 1
        }

        fn validate(&self, metadata: &SubmissionMetadata) -> bool {
            metadata.manifest == b"m" && metadata.requested_scope == requested_scope_fixture()
        }
    }

    pub fn required_paths(name: &str) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
        let value = std::env::var_os(name)
            .ok_or_else(|| format!("{name} must list sandbox runtime paths"))?;
        if name == "PRDB_TEST_RUNTIME_READ_ONLY" && value.is_empty() {
            return Ok(Vec::new());
        }
        let paths = std::env::split_paths(&value).collect::<Vec<_>>();
        if paths.is_empty() || paths.iter().any(|path| !path.is_absolute()) {
            return Err(format!("{name} must contain absolute paths").into());
        }
        Ok(paths)
    }

    let cgroup_parent = std::env::var_os("PRDB_TEST_CGROUP_PARENT")
        .map(PathBuf::from)
        .ok_or("PRDB_TEST_CGROUP_PARENT must name a delegated cgroup v2 parent")?;
    let artifact_path = std::env::var_os(artifact_env)
        .map(PathBuf::from)
        .ok_or_else(|| format!("{artifact_env} must name its TinyCC artifact"))?;
    if !cgroup_parent.is_absolute() || !artifact_path.is_absolute() {
        return Err("sandbox test paths must be absolute".into());
    }
    let artifact = std::fs::read(&artifact_path)?;
    let mut client = ClientConnectionSession::default();
    client.accept_hello(ClientMode::Query, "sandbox-cell-policy-forgery")?;
    client.authenticate(storage, credentials.token_id, &credentials.token_secret)?;
    let owner = client.staging_owner()?;
    let manifest = b"m".to_vec();
    let staging = StagingStore::new(StagingLimits::default(), Arc::new(AcceptFixture))?;
    let reservation = staging.prepare(
        owner,
        PrepareSubmission {
            manifest_version: 1,
            manifest: manifest.clone(),
            artifact_bytes: u64::try_from(artifact.len())?,
            artifact_sha256: Sha256::digest(&artifact).into(),
            scope_version: 1,
            requested_scope: requested_scope_fixture(),
            parameter_bytes: 0,
            parameter_sha256: Sha256::digest([]).into(),
            sql_data_bytes: 0,
            sql_data_sha256: Sha256::digest([]).into(),
            deadline_ms: 30_000,
            workload_class: WorkloadClass::Operational,
        },
    )?;
    let mut offset = 0;
    for chunk in artifact.chunks(prdb_engine::supervisor::MAX_CHUNK_BYTES) {
        offset = staging
            .upload_chunk(
                owner,
                reservation.invocation_id,
                ChunkKind::Artifact,
                offset,
                chunk,
            )
            .map_err(|error| format!("hostile query upload: {error:?}"))?;
    }
    staging
        .seal(owner, reservation.invocation_id)
        .map_err(|error| format!("hostile query seal: {error:?}"))?;

    let runtime = GatewayExecutorConfig {
        cgroup_parent,
        resource_limits: prdb_engine::linux_sandbox::WorkerResourceLimits {
            cpu_quota_micros: 500_000,
            cpu_period_micros: 100_000,
            memory_max_bytes: 256 * 1024 * 1024,
            max_processes: 32,
        },
        analytical_resource_limits: None,
        launch: prdb_engine::executor_launcher::ExecutorLaunchConfig {
            executor_path: PathBuf::from(env!("CARGO_BIN_EXE_prdb-executor")),
            optimizer_path: PathBuf::from(env!("CARGO_BIN_EXE_prdb-optimizer")),
            scratch_parent: std::env::temp_dir(),
            runtime_read_execute: required_paths("PRDB_TEST_RUNTIME_READ_EXECUTE")?,
            runtime_read_only: required_paths("PRDB_TEST_RUNTIME_READ_ONLY")?,
            spill_broker: None,
            max_total_result_bytes: prdb_engine::query_abi::MAX_TOTAL_RESULT_BYTES,
            analytical_max_total_result_bytes: None,
            max_sql_index_probe_candidates:
                prdb_engine::query_abi::DEFAULT_SQL_INDEX_PROBE_CANDIDATES,
            analytical_max_sql_index_probe_candidates: None,
            cleanup_timeout: Duration::from_secs(2),
        },
    };
    let invocation_id = reservation.invocation_id;
    let id_hex = invocation_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let (accepted, mut invocation, mut executor) = client.handle_start_request_with_executor(
        storage,
        &staging,
        ClientFrame {
            kind: 7,
            request_id: 1,
            response_sequence: 0,
            payload: format!("{{\"invocation_id\":\"{id_hex}\"}}").into_bytes(),
        },
        GatewayAdmissionConfig {
            retention_period_us: 3_600_000_000,
            max_rows: 2,
            max_bytes: 1024,
        },
        &runtime,
    )?;
    assert_eq!(accepted.payload, b"{\"state\":\"accepted\"}");
    let (mut service, mut gateway) = UnixStream::pair()?;
    gateway.set_read_timeout(Some(Duration::from_secs(10)))?;
    assert_eq!(
        observe_executor_relay_and_finalize_gateway(
            storage,
            &mut invocation,
            &mut executor,
            &mut service,
            accepted.request_id,
        )?,
        GatewayTerminalDecision::Abort(prdb_engine::supervisor_ipc::GatewayAbortReason::QueryError)
    );
    staging.finish_started(owner, invocation_id)?;
    drop(executor);
    let terminal = read_frame(&mut gateway)?.ok_or("missing hostile-query terminal")?;
    assert_eq!(terminal.operation, Operation::GatewayTerminal as u16);
    assert_eq!(terminal.request_id, accepted.request_id);
    assert!(String::from_utf8(terminal.payload)?.contains("\"outcome\":\"aborted\""));
    assert_eq!(
        storage
            .lookup_invocation_outcome(invocation_id, user_id, now_us + 1)?
            .ok_or("missing hostile-query outcome")?
            .status,
        prdb_engine::outcome::OutcomeStatus::Aborted
    );
    Ok(())
}

pub fn executor_ipc_probe_unique(
    invocation: &mut prdb_engine::transaction::InvocationTransaction<'_>,
    schema_name: &str,
    table_name: &str,
    index_id: [u8; 16],
    key: prdb_engine::tuple::ScalarValue,
    column_ids: Vec<[u8; 16]>,
) -> Result<prdb_engine::executor_ipc::ProbeUniqueIndexResponse, Box<dyn std::error::Error>> {
    use prdb_engine::executor_ipc::{
        ProbeUniqueIndexRequest, ProbeUniqueIndexResponse, ResolveRelationRequest,
        ResolveRelationResponse, dispatch_executor_request,
    };
    use prdb_engine::ipc::{IpcFrame, Operation, ServiceRole};

    let mut connection = invocation.open_executor_metadata_connection();
    let resolved = dispatch_executor_request(
        ServiceRole::Executor,
        &mut connection,
        &IpcFrame {
            operation: Operation::ResolveRelation as u16,
            request_id: 1,
            payload: ResolveRelationRequest {
                schema_name: schema_name.to_owned(),
                relation_name: table_name.to_owned(),
            }
            .encode()?,
        },
    )
    .map_err(|error| format!("executor IPC resolve failed: {error:?}"))?;
    let ResolveRelationResponse::Resolved { handle, .. } =
        ResolveRelationResponse::decode(&resolved.payload)?
    else {
        return Err("authorized relation did not resolve".into());
    };
    let response = dispatch_executor_request(
        ServiceRole::Executor,
        &mut connection,
        &IpcFrame {
            operation: Operation::ProbeUniqueIndex as u16,
            request_id: 2,
            payload: ProbeUniqueIndexRequest {
                relation: handle,
                index_id,
                values: vec![key],
                column_ids,
            }
            .encode()?,
        },
    )
    .map_err(|error| format!("executor IPC unique probe failed: {error:?}"))?;
    Ok(ProbeUniqueIndexResponse::decode(&response.payload)?)
}

pub fn executor_ipc_scan_index_range(
    invocation: &mut prdb_engine::transaction::InvocationTransaction<'_>,
    schema_name: &str,
    table_name: &str,
    index_id: [u8; 16],
    lower: Option<prdb_engine::transaction::AuthorizedIndexRangeBound>,
    upper: Option<prdb_engine::transaction::AuthorizedIndexRangeBound>,
    column_ids: Vec<[u8; 16]>,
) -> Result<prdb_engine::executor_ipc::ScanIndexRangeResponse, Box<dyn std::error::Error>> {
    use prdb_engine::executor_ipc::{
        ResolveRelationRequest, ResolveRelationResponse, ScanIndexRangeRequest,
        ScanIndexRangeResponse, dispatch_executor_request,
    };
    use prdb_engine::ipc::{IpcFrame, Operation, ServiceRole};

    let mut connection = invocation.open_executor_metadata_connection();
    let resolved = dispatch_executor_request(
        ServiceRole::Executor,
        &mut connection,
        &IpcFrame {
            operation: Operation::ResolveRelation as u16,
            request_id: 1,
            payload: ResolveRelationRequest {
                schema_name: schema_name.to_owned(),
                relation_name: table_name.to_owned(),
            }
            .encode()?,
        },
    )
    .map_err(|error| format!("executor IPC resolve failed: {error:?}"))?;
    let ResolveRelationResponse::Resolved { handle, .. } =
        ResolveRelationResponse::decode(&resolved.payload)?
    else {
        return Err("authorized relation did not resolve".into());
    };
    let response = dispatch_executor_request(
        ServiceRole::Executor,
        &mut connection,
        &IpcFrame {
            operation: Operation::ScanIndexRange as u16,
            request_id: 2,
            payload: ScanIndexRangeRequest {
                relation: handle,
                index_id,
                lower,
                upper,
                column_ids,
            }
            .encode()?,
        },
    )
    .map_err(|error| format!("executor IPC index range failed: {error:?}"))?;
    Ok(ScanIndexRangeResponse::decode(&response.payload)?)
}

pub fn id(n: u8) -> [u8; 16] {
    [n; 16]
}

pub fn run_operational_read_commit_samples(
    storage: &StorageManager<'_>,
    table_id: [u8; 16],
    expected_scan_rows: usize,
    token_id: [u8; 16],
    secret: [u8; 32],
    retention_deadline_us: i64,
    invocation_namespace: u8,
) -> Result<(Vec<Duration>, Vec<Duration>), Box<dyn std::error::Error>> {
    std::thread::scope(|scope| {
        let start = Arc::new(Barrier::new(5));
        let mut workers = Vec::with_capacity(4);
        for worker_index in 0..4u8 {
            let start = Arc::clone(&start);
            workers.push(
                scope.spawn(move || -> Result<(Vec<Duration>, Vec<Duration>), String> {
                    start.wait();
                    let mut point_read_samples = Vec::with_capacity(16);
                    let mut commit_samples = Vec::with_capacity(16);
                    for index in 0..16u8 {
                        let mut invocation_id = [invocation_namespace; 16];
                        invocation_id[14] = worker_index;
                        invocation_id[15] = index;
                        let (mut transaction, _) = storage
                            .begin_token_authenticated_invocation(
                                invocation_id,
                                token_id,
                                &secret,
                                retention_deadline_us,
                                16,
                                4096,
                            )
                            .map_err(|error| format!("operational admission: {error:?}"))?;
                        let point_read_started = std::time::Instant::now();
                        let point_read = transaction
                            .read_catalog_authorized_row(table_id, id(0xd3))
                            .map_err(|error| {
                                format!("operational authorized point read: {error:?}")
                            })?;
                        if point_read.is_none_or(|cells| !(2..=3).contains(&cells.len())) {
                            return Err(
                                "operational point read returned an unexpected row shape".into()
                            );
                        }
                        point_read_samples.push(point_read_started.elapsed());
                        let mut continuation = None;
                        let mut scanned_rows = 0usize;
                        loop {
                            let page = transaction
                                .scan_catalog_authorized_rows(
                                    table_id,
                                    continuation.as_ref(),
                                    128,
                                )
                                .map_err(|error| {
                                    format!("operational authorized scan: {error:?}")
                                })?;
                            scanned_rows += page.rows.len();
                            continuation = page.continuation;
                            if continuation.is_none() {
                                break;
                            }
                        }
                        if scanned_rows != expected_scan_rows {
                            return Err(format!(
                                "operational authorized scan returned {scanned_rows} rows, expected {expected_scan_rows}"
                            ));
                        }
                        let commit_started = std::time::Instant::now();
                        transaction
                            .commit()
                            .map_err(|error| format!("operational durable commit: {error:?}"))?;
                        commit_samples.push(commit_started.elapsed());
                    }
                    Ok((point_read_samples, commit_samples))
                }),
            );
        }
        start.wait();
        let mut point_read_samples = Vec::with_capacity(64);
        let mut commit_samples = Vec::with_capacity(64);
        for worker in workers {
            let (point_reads, commits) = worker
                .join()
                .map_err(|_| std::io::Error::other("operational workload thread panicked"))?
                .map_err(std::io::Error::other)?;
            point_read_samples.extend(point_reads);
            commit_samples.extend(commits);
        }
        Ok((point_read_samples, commit_samples))
    })
}

pub fn requested_scope_fixture() -> Vec<u8> {
    let mut bytes = b"PSC1\0\x01\x02\x02".to_vec();
    bytes.extend([1; 16]);
    bytes
}

pub fn admin_dispatch_json(
    storage: &StorageManager,
    staging: &prdb_engine::supervisor::StagingStore,
    session: &mut prdb_engine::client_protocol::ClientConnectionSession,
    request_id: u64,
    payload: &str,
    retention_period_us: i64,
) -> Result<String, Box<dyn std::error::Error>> {
    let dispatch = prdb_engine::supervisor_ipc::dispatch_gateway_client_request(
        prdb_engine::ipc::ServiceRole::Gateway,
        storage,
        staging,
        session,
        &prdb_engine::ipc::IpcFrame {
            operation: prdb_engine::ipc::Operation::SubmitAdministrativeOperation as u16,
            request_id,
            payload: payload.as_bytes().to_vec(),
        },
        prdb_engine::supervisor_ipc::GatewayAdmissionConfig {
            retention_period_us,
            max_rows: 1,
            max_bytes: 1,
        },
    );
    match dispatch.map_err(|error| format!("ADMIN request {payload:?} failed: {error:?}"))? {
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Response(response) => {
            Ok(String::from_utf8(response.payload)?)
        }
        prdb_engine::supervisor_ipc::GatewayClientDispatch::Started { .. } => {
            Err("ADMIN DDL unexpectedly started a worker invocation".into())
        }
    }
}

pub fn gateway_service_admin_json(
    storage: &StorageManager,
    staging: &prdb_engine::supervisor::StagingStore,
    session: &mut prdb_engine::client_protocol::ClientConnectionSession,
    request_id: u64,
    payload: &str,
    retention_period_us: i64,
) -> Result<String, Box<dyn std::error::Error>> {
    use prdb_engine::ipc::{IpcFrame, Operation, read_frame, write_frame};
    use prdb_engine::supervisor_ipc::{
        GatewayAdmissionConfig, GatewayClientServiceEvent, serve_gateway_client_session,
    };
    use std::os::unix::net::UnixStream;

    let (mut service, mut client) = UnixStream::pair()?;
    service.set_read_timeout(Some(Duration::from_secs(10)))?;
    client.set_read_timeout(Some(Duration::from_secs(10)))?;
    let admission = GatewayAdmissionConfig {
        retention_period_us,
        max_rows: 1,
        max_bytes: 1,
    };
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            serve_gateway_client_session(&mut service, storage, staging, session, admission)
        });
        write_frame(
            &mut client,
            &IpcFrame {
                operation: Operation::SubmitAdministrativeOperation as u16,
                request_id,
                payload: payload.as_bytes().to_vec(),
            },
        )?;
        client.shutdown(std::net::Shutdown::Write)?;
        let response =
            read_frame(&mut client)?.ok_or("gateway service closed before ADMIN response")?;
        let service_result = server
            .join()
            .map_err(|_| std::io::Error::other("gateway service thread panicked"))?
            .map_err(|error| std::io::Error::other(format!("gateway service failed: {error:?}")))?;
        if !matches!(service_result, GatewayClientServiceEvent::Disconnected) {
            return Err(
                std::io::Error::other("ADMIN request unexpectedly started a worker").into(),
            );
        }
        if response.operation != Operation::SubmitAdministrativeOperation as u16
            || response.request_id != request_id
        {
            return Err(std::io::Error::other(format!(
                "unexpected gateway ADMIN response: {} {} {}",
                response.operation,
                response.request_id,
                String::from_utf8_lossy(&response.payload)
            ))
            .into());
        }
        Ok(String::from_utf8(response.payload)?)
    })
}

pub fn id_catalog_key(family: u16, object_id: [u8; 16]) -> Vec<u8> {
    let mut key = family.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&object_id);
    key
}

pub fn identity_tombstone_write(object_id: [u8; 16]) -> CatalogWrite {
    let key = id_catalog_key(17, object_id);
    let payload = encode_envelope(&family_schema(17).unwrap(), &key, &[]).unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

pub fn catalog_delete(mut write: CatalogWrite, current_value: Vec<u8>) -> CatalogWrite {
    write.expected_value = Some(current_value);
    write.payload = None;
    write
}

pub fn catalog_expect(mut write: CatalogWrite, current_value: Vec<u8>) -> CatalogWrite {
    write.expected_value = Some(current_value);
    write
}

pub fn builtin_id(code: u16) -> [u8; 16] {
    let mut id = [0x80; 16];
    id[15] = code as u8;
    id
}

pub fn builtin_owner_id() -> [u8; 16] {
    let mut id = [0x80; 16];
    id[15] = 3;
    id
}

pub fn principal_test_name(principal_id: [u8; 16]) -> String {
    if principal_id == builtin_owner_id() {
        return "database_admin".to_owned();
    }
    let suffix = principal_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("principal_{suffix}")
}

pub fn bytea_tuple(value: &[u8]) -> Vec<u8> {
    encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        }],
        1,
        &[Some(ScalarValue::Bytea(value.to_vec()))],
    )
    .unwrap()
}

pub fn two_bytea_tuple(first: &[u8], second: &[u8]) -> Vec<u8> {
    encode_tuple(
        &[
            ColumnSchema {
                scalar_type: ScalarType::Bytea,
                nullable: true,
            },
            ColumnSchema {
                scalar_type: ScalarType::Bytea,
                nullable: true,
            },
        ],
        1,
        &[
            Some(ScalarValue::Bytea(first.to_vec())),
            Some(ScalarValue::Bytea(second.to_vec())),
        ],
    )
    .unwrap()
}

pub fn index_definition(index_id: [u8; 16], table_id: [u8; 16]) -> (Vec<u8>, Vec<u8>) {
    index_definition_with_columns(index_id, table_id, &[[0x66; 16]])
}

pub fn index_definition_with_columns(
    index_id: [u8; 16],
    table_id: [u8; 16],
    column_ids: &[[u8; 16]],
) -> (Vec<u8>, Vec<u8>) {
    index_definition_with_options(index_id, table_id, column_ids, false)
}

pub fn index_definition_with_options(
    index_id: [u8; 16],
    table_id: [u8; 16],
    column_ids: &[[u8; 16]],
    unique: bool,
) -> (Vec<u8>, Vec<u8>) {
    let mut key = 9u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&index_id);
    let mut columns = (column_ids.len() as u32).to_le_bytes().to_vec();
    columns.extend(column_ids.iter().flatten());
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: table_id.to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::Ids,
            payload: columns,
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::Bool,
            payload: vec![u8::from(unique)],
        },
        CatalogField {
            tag: 5,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 7,
            wire_type: WireType::Id,
            payload: [0x77; 16].to_vec(),
        },
        CatalogField {
            tag: 8,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 9,
            wire_type: WireType::U32,
            payload: 1u32.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 10,
            wire_type: WireType::U64,
            payload: 1u64.to_le_bytes().to_vec(),
        },
    ];
    let payload = encode_envelope(&family_schema(9).unwrap(), &key, &fields).unwrap();
    (key, payload)
}

pub fn typed_bytea_index_key(
    index_id: [u8; 16],
    row_key: &[u8; 32],
    payload: &[u8],
) -> prdb_engine::index_key::DerivedIndexKey {
    prdb_engine::index_key::derive_index_key(
        index_id,
        row_key[16..].try_into().unwrap(),
        1,
        &[id(0x66)],
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        }],
        payload,
        &[id(0x66)],
    )
    .unwrap()
}

pub fn typed_two_bytea_index_key(
    index_id: [u8; 16],
    row_key: &[u8; 32],
    payload: &[u8],
    column_id: [u8; 16],
) -> prdb_engine::index_key::DerivedIndexKey {
    prdb_engine::index_key::derive_index_key(
        index_id,
        row_key[16..].try_into().unwrap(),
        1,
        &[id(0x66), id(0x67)],
        &[
            ColumnSchema {
                scalar_type: ScalarType::Bytea,
                nullable: true,
            },
            ColumnSchema {
                scalar_type: ScalarType::Bytea,
                nullable: true,
            },
        ],
        payload,
        &[column_id],
    )
    .unwrap()
}

pub fn owned_unique_index_definition(
    index_id: [u8; 16],
    table_id: [u8; 16],
    column_ids: &[[u8; 16]],
    constraint_id: [u8; 16],
) -> (Vec<u8>, Vec<u8>) {
    let mut key = 9u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&index_id);
    let mut columns = (column_ids.len() as u32).to_le_bytes().to_vec();
    columns.extend(column_ids.iter().flatten());
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: table_id.to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::Ids,
            payload: columns,
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::Bool,
            payload: vec![1],
        },
        CatalogField {
            tag: 5,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 6,
            wire_type: WireType::Id,
            payload: constraint_id.to_vec(),
        },
        CatalogField {
            tag: 7,
            wire_type: WireType::Id,
            payload: [0x77; 16].to_vec(),
        },
        CatalogField {
            tag: 8,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 9,
            wire_type: WireType::U32,
            payload: 1u32.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 10,
            wire_type: WireType::U64,
            payload: 1u64.to_le_bytes().to_vec(),
        },
    ];
    let payload = encode_envelope(&family_schema(9).unwrap(), &key, &fields).unwrap();
    (key, payload)
}

pub fn bootstrap_database_record(database_id: [u8; 16]) -> (Vec<u8>, Vec<u8>) {
    let mut key = 1u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&database_id);
    let mut builtins = 31u32.to_le_bytes().to_vec();
    for code in [1u16, 2, 3]
        .into_iter()
        .chain(101u16..=111)
        .chain(201u16..=217)
    {
        builtins.extend_from_slice(&code.to_le_bytes());
        builtins.extend_from_slice(&0u16.to_le_bytes());
        builtins.extend_from_slice(&builtin_id(code));
    }
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Text,
            payload: b"test_database".to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::U32,
            payload: 2u32.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::Bytes,
            payload: builtins,
        },
        CatalogField {
            tag: 5,
            wire_type: WireType::U32,
            payload: 1u32.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 6,
            wire_type: WireType::U32,
            payload: 1u32.to_le_bytes().to_vec(),
        },
    ];
    let payload = encode_envelope(&family_schema(1).unwrap(), &key, &fields).unwrap();
    (key, payload)
}

pub fn initialize(path: &std::path::Path) -> Result<Database, Box<dyn std::error::Error>> {
    let database = Database::create(path)?;
    initialize_database(&database)?;
    Ok(database)
}

pub fn initialize_database(database: &Database) -> Result<(), Box<dyn std::error::Error>> {
    let mut transaction = database.begin_write()?;
    transaction.set_durability(redb::Durability::Immediate)?;
    transaction.set_two_phase_commit(true);
    schema::create_tables(&transaction)?;
    let (index_key, index_payload) = index_definition([0x44; 16], [0x10; 16]);
    let (database_key, database_payload) = bootstrap_database_record([0x33; 16]);
    let index_object = object_write([0x44; 16], 7);
    let table_object = object_write(id(16), 3);
    let table = table_write(id(16), id(20));
    let column_object = object_write(id(0x66), 4);
    let column = column_write(id(0x66), id(16), "object_102");
    transaction
        .open_table(CATALOG)?
        .insert(&database_key[..], &database_payload[..])?;
    transaction.open_table(CATALOG)?.insert(
        &index_object.key[..],
        index_object.payload.as_ref().unwrap().as_slice(),
    )?;
    transaction
        .open_table(CATALOG)?
        .insert(&index_key[..], &index_payload[..])?;
    for record in [table_object, table, column_object, column] {
        transaction
            .open_table(CATALOG)?
            .insert(&record.key[..], record.payload.as_ref().unwrap().as_slice())?;
    }
    let owner_id = builtin_owner_id();
    let owner_principal = principal_write_with_name(owner_id, 2, 1, "database_admin");
    let owner_object = principal_object_write(owner_id, 2);
    transaction.open_table(CATALOG)?.insert(
        &owner_principal.key[..],
        owner_principal.payload.as_ref().unwrap().as_slice(),
    )?;
    transaction
        .open_table(PRINCIPAL_NAMES)?
        .insert(&b"database_admin"[..], &owner_id[..])?;
    transaction.open_table(CATALOG)?.insert(
        &owner_object.key[..],
        owner_object.payload.as_ref().unwrap().as_slice(),
    )?;
    for code in [1u16, 2] {
        let schema_id = builtin_id(code);
        let schema = schema_write(schema_id);
        let object = object_write(schema_id, 1);
        for record in [schema, object] {
            transaction
                .open_table(CATALOG)?
                .insert(&record.key[..], record.payload.as_ref().unwrap().as_slice())?;
        }
    }
    for builtin_kind in 1u16..=11 {
        let type_id = builtin_id(100 + builtin_kind);
        let ty = builtin_type_write(type_id, builtin_kind);
        let object = object_write(type_id, 2);
        for record in [ty, object] {
            transaction
                .open_table(CATALOG)?
                .insert(&record.key[..], record.payload.as_ref().unwrap().as_slice())?;
        }
    }
    let metadata = EngineMetadata {
        database_id: [0x33; 16],
        catalog_version: 2,
        expression_version: 1,
        builtin_semantics_version: 1,
        commit_sequence: 0,
        lifecycle: Lifecycle::Ready,
    }
    .encode()?;
    transaction
        .open_table(ENGINE_META)?
        .insert(&b"format"[..], &metadata[..])?;
    transaction.commit()?;
    Ok(())
}

pub fn row(
    database: &Database,
    key: &[u8; 32],
) -> Result<Option<RowRecord>, Box<dyn std::error::Error>> {
    let transaction = database.begin_read()?;
    let value = transaction
        .open_table(ROWS)?
        .get(&key[..])?
        .map(|guard| guard.value().to_vec());
    value
        .map(|bytes| RowRecord::decode(&bytes).map_err(Into::into))
        .transpose()
}

pub fn commit_sequence(database: &Database) -> Result<u64, Box<dyn std::error::Error>> {
    let transaction = database.begin_read()?;
    let bytes = transaction
        .open_table(ENGINE_META)?
        .get(&b"format"[..])?
        .unwrap()
        .value()
        .to_vec();
    Ok(EngineMetadata::decode(&bytes)?.commit_sequence)
}

pub fn index_key(row_key: &[u8; 32]) -> Vec<u8> {
    let mut key = vec![0x44; 16];
    key.extend_from_slice(&[1, 0x80, 0, 0, 1]);
    key.extend_from_slice(&row_key[16..]);
    key
}

pub fn audit_event(n: u8) -> AuditEvent {
    AuditEvent {
        event_id: [n; 16],
        actor_id: Some(id(9)),
        correlation_id: [n + 1; 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![[n + 2; 16]],
    }
}

pub fn identity_audit_event(n: u8, actor_id: [u8; 16], target_id: [u8; 16]) -> AuditEvent {
    AuditEvent {
        event_id: [n; 16],
        actor_id: Some(actor_id),
        correlation_id: [n.wrapping_add(1); 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![target_id],
    }
}

pub fn membership_write(member_id: [u8; 16], role_id: [u8; 16]) -> CatalogWrite {
    membership_write_with_grantor(member_id, role_id, builtin_owner_id())
}

pub fn membership_write_with_grantor(
    member_id: [u8; 16],
    role_id: [u8; 16],
    grantor_id: [u8; 16],
) -> CatalogWrite {
    let mut key = 11u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&member_id);
    key.push(1);
    key.extend_from_slice(&role_id);
    let payload = encode_envelope(
        &family_schema(11).unwrap(),
        &key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: grantor_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
        ],
    )
    .unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

pub fn grant_write(
    grantee_id: [u8; 16],
    object_id: [u8; 16],
    privilege: u16,
    target_scope: u16,
    target_id: [u8; 16],
    grantor_id: [u8; 16],
) -> CatalogWrite {
    let mut key = 12u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&grantee_id);
    key.push(1);
    key.extend_from_slice(&object_id);
    key.push(1);
    key.extend_from_slice(&privilege.to_be_bytes());
    key.push(1);
    key.extend_from_slice(&target_scope.to_be_bytes());
    key.push(1);
    key.extend_from_slice(&target_id);
    let payload = encode_envelope(
        &family_schema(12).unwrap(),
        &key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: grantor_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
        ],
    )
    .unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

pub fn object_write(object_id: [u8; 16], kind: u16) -> CatalogWrite {
    object_write_with_parent(object_id, kind, None)
}

pub fn object_write_with_parent(
    object_id: [u8; 16],
    kind: u16,
    parent_id: Option<[u8; 16]>,
) -> CatalogWrite {
    object_write_with_parent_and_owner(object_id, kind, parent_id, builtin_owner_id())
}

pub fn object_write_with_owner(object_id: [u8; 16], kind: u16, owner_id: [u8; 16]) -> CatalogWrite {
    object_write_with_parent_and_owner(object_id, kind, None, owner_id)
}

pub fn object_write_with_parent_and_owner(
    object_id: [u8; 16],
    kind: u16,
    parent_id: Option<[u8; 16]>,
    owner_id: [u8; 16],
) -> CatalogWrite {
    let name = if object_id == builtin_owner_id() && kind == 9 {
        "database_admin".to_owned()
    } else if kind == 1 {
        format!("schema_{}", object_id[15])
    } else if matches!(kind, 8 | 9) {
        principal_test_name(object_id)
    } else {
        format!("object_{}", object_id[0])
    };
    object_write_with_parent_and_owner_and_name(object_id, kind, parent_id, owner_id, &name)
}

pub fn object_write_with_name(object_id: [u8; 16], kind: u16, name: &str) -> CatalogWrite {
    object_write_with_parent_and_owner_and_name(object_id, kind, None, builtin_owner_id(), name)
}

pub fn object_write_with_parent_and_owner_and_name(
    object_id: [u8; 16],
    kind: u16,
    parent_id: Option<[u8; 16]>,
    owner_id: [u8; 16],
    name: &str,
) -> CatalogWrite {
    let mut key = 2u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&object_id);
    let mut fields = vec![CatalogField {
        tag: 1,
        wire_type: WireType::U16,
        payload: kind.to_le_bytes().to_vec(),
    }];
    if let Some(parent_id) = parent_id {
        fields.push(CatalogField {
            tag: 2,
            wire_type: WireType::Id,
            payload: parent_id.to_vec(),
        });
    }
    fields.extend([
        CatalogField {
            tag: 3,
            wire_type: WireType::Text,
            payload: name.as_bytes().to_vec(),
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::Id,
            payload: owner_id.to_vec(),
        },
        CatalogField {
            tag: 5,
            wire_type: WireType::U64,
            payload: 1u64.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 6,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
    ]);
    let payload = encode_envelope(&family_schema(2).unwrap(), &key, &fields).unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

pub fn principal_object_write(principal_id: [u8; 16], principal_kind: u16) -> CatalogWrite {
    let object_kind = match principal_kind {
        1 => 8,
        2 => 9,
        _ => panic!("unsupported test principal kind"),
    };
    object_write(principal_id, object_kind)
}

pub fn schema_write(schema_id: [u8; 16]) -> CatalogWrite {
    schema_write_with_search_path(schema_id, &[])
}

pub fn schema_write_with_search_path(
    schema_id: [u8; 16],
    search_path: &[[u8; 16]],
) -> CatalogWrite {
    let mut key = 3u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&schema_id);
    let mut search_path_payload = (search_path.len() as u32).to_le_bytes().to_vec();
    for target_id in search_path {
        search_path_payload.extend_from_slice(target_id);
    }
    let payload = encode_envelope(
        &family_schema(3).unwrap(),
        &key,
        &[CatalogField {
            tag: 1,
            wire_type: WireType::Ids,
            payload: search_path_payload,
        }],
    )
    .unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

pub fn builtin_type_write(type_id: [u8; 16], builtin_kind: u16) -> CatalogWrite {
    let parameter_rule = match builtin_kind {
        5 => 1,
        7 => 2,
        _ => 0,
    };
    let default_mask = if builtin_kind == 9 { 7 } else { 1 };
    builtin_type_write_with_profile(type_id, builtin_kind, parameter_rule, 1, 1, default_mask, 1)
}

pub fn builtin_type_write_with_profile(
    type_id: [u8; 16],
    builtin_kind: u16,
    parameter_rule: u16,
    scalar_encoding: u32,
    comparison_version: u32,
    default_mask: u32,
    cast_profile: u32,
) -> CatalogWrite {
    let key = id_catalog_key(4, type_id);
    let payload = encode_envelope(
        &family_schema(4).unwrap(),
        &key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::U16,
                payload: builtin_kind.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: parameter_rule.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::U32,
                payload: scalar_encoding.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::U32,
                payload: comparison_version.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::U32,
                payload: default_mask.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::U32,
                payload: cast_profile.to_le_bytes().to_vec(),
            },
        ],
    )
    .unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

pub fn dependency_write(
    dependent_id: [u8; 16],
    referenced_id: [u8; 16],
    kind: u16,
) -> CatalogWrite {
    let mut key = 15u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&dependent_id);
    key.push(1);
    key.extend_from_slice(&referenced_id);
    key.push(1);
    key.extend_from_slice(&kind.to_be_bytes());
    let payload = encode_envelope(&family_schema(15).unwrap(), &key, &[]).unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

pub fn principal_write(principal_id: [u8; 16], kind: u16) -> CatalogWrite {
    principal_write_with_epoch(principal_id, kind, 1)
}

pub fn principal_write_with_name(
    principal_id: [u8; 16],
    kind: u16,
    epoch: u64,
    name: &str,
) -> CatalogWrite {
    principal_write_with_login(principal_id, kind, kind == 1, epoch, name)
}

pub fn principal_write_with_login(
    principal_id: [u8; 16],
    kind: u16,
    login_enabled: bool,
    epoch: u64,
    name: &str,
) -> CatalogWrite {
    let mut write = principal_write_with_epoch(principal_id, kind, epoch);
    let key = write.key.clone();
    write.payload = Some(
        encode_envelope(
            &family_schema(10).unwrap(),
            &key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::U16,
                    payload: kind.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 2,
                    wire_type: WireType::Text,
                    payload: name.as_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::Bool,
                    payload: vec![u8::from(login_enabled)],
                },
                CatalogField {
                    tag: 4,
                    wire_type: WireType::U64,
                    payload: epoch.to_le_bytes().to_vec(),
                },
            ],
        )
        .unwrap(),
    );
    write
}
