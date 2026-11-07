//! Coordinated entry points for invocation commits and administrative DDL.

use crate::audit::{AuditAction, AuditEvent, AuditResult};
use crate::authorization::{MAX_MEMBERSHIP_EDGES, Membership, resolve_role_closure};
use crate::catalog::{
    CatalogField, CatalogRecord, WireType, decode_registered_envelope, encode_envelope,
    family_schema,
};
use crate::commit::{
    CatalogWrite, CommitError, commit_authorized_constraint_ddl_with_fences,
    commit_authorized_foreign_key_ddl_with_fences, commit_authorized_identity_ddl_with_fences,
    commit_authorized_index_ddl_with_fences, commit_authorized_membership_ddl_with_fences,
    commit_authorized_object_grant_ddl_with_fences, commit_authorized_policy_ddl_with_fences,
    commit_authorized_system_privilege_ddl_with_fences, commit_catalog_and_audit_with_fences,
};
use crate::credentials::{
    MAX_TOKEN_LABEL_BYTES, TOKEN_SECRET_BYTES, decode_verifier_record, encode_new_verifier_record,
    encode_revoked_verifier_record, now_unix_micros,
};
use crate::ddl_fence::{DdlFenceCoordinator, DdlFenceManager};
use crate::metadata::{EngineMetadata, Lifecycle};
use crate::outcome::{InvocationOutcome, lookup_outcome};
use crate::schema::{AUDIT, CATALOG, ENGINE_META, OUTCOMES};
use crate::snapshot::{MAX_CATALOG_FAMILY_SCAN, RowSnapshot};
use crate::startup::{StartupError, validate_persisted_records};
use crate::transaction::{InvocationCoordination, InvocationTransaction, TransactionError};
use redb::{Database, ReadableDatabase, ReadableTable};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};
use zeroize::Zeroize;

const TOKEN_REQUEST_WINDOW: Duration = Duration::from_secs(60);
const MAX_TRACKED_TOKEN_REQUEST_ACTORS: usize = 4096;
const SUPERVISOR_IDLE_POLL_INTERVAL: Duration = Duration::from_millis(100);
pub const MAX_OWN_INVOCATION_PAGE: usize = 128;
pub const MAX_ADMIN_INVOCATION_PAGE: usize = 128;
static TOKEN_REQUEST_LIMITERS: OnceLock<Mutex<HashMap<usize, Weak<TokenRequestLimiter>>>> =
    OnceLock::new();
static ADMIN_CANCELLATION_LIMITERS: OnceLock<Mutex<HashMap<usize, Weak<TokenRequestLimiter>>>> =
    OnceLock::new();
static INVOCATION_REGISTRIES: OnceLock<Mutex<HashMap<usize, Weak<InvocationAdmissionState>>>> =
    OnceLock::new();

/// Admission and coordination limits for one storage-manager handle.
///
/// Defaults allow at most 128 simultaneous invocation snapshots, a 15-minute
/// lifetime per managed invocation, and 30 token-lifecycle and administrative
/// cancellation requests per actor per rolling minute.
#[derive(Clone, Copy, Debug)]
pub struct StorageManagerConfig {
    pub ddl_fence_timeout: Duration,
    pub max_active_invocations: usize,
    pub max_invocation_age: Duration,
    pub max_token_lifecycle_requests_per_minute: usize,
    pub max_admin_cancellation_requests_per_minute: usize,
}

impl Default for StorageManagerConfig {
    fn default() -> Self {
        Self {
            ddl_fence_timeout: Duration::from_secs(5),
            max_active_invocations: 128,
            max_invocation_age: Duration::from_secs(15 * 60),
            max_token_lifecycle_requests_per_minute: 30,
            max_admin_cancellation_requests_per_minute: 30,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageManagerConfigError {
    ZeroActiveInvocationLimit,
    ZeroInvocationAge,
    ZeroTokenLifecycleRequestLimit,
    ZeroAdminCancellationRequestLimit,
}

impl std::fmt::Display for StorageManagerConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invalid storage manager configuration: {self:?}")
    }
}

impl std::error::Error for StorageManagerConfigError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageManagerInitError {
    Config(StorageManagerConfigError),
    Startup(StartupError),
}

impl std::fmt::Display for StorageManagerInitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "storage manager initialization failed: {self:?}")
    }
}

impl std::error::Error for StorageManagerInitError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvocationStatusConsistency {
    BestEffort,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InvocationStatus {
    Active {
        started_at_us: i64,
        deadline_at_us: Option<i64>,
        cancellation_requested: bool,
    },
    Committing {
        started_at_us: i64,
        deadline_at_us: Option<i64>,
        cancellation_requested: bool,
    },
    Terminal(InvocationOutcome),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvocationStatusSnapshot {
    pub observed_at_us: i64,
    pub consistency: InvocationStatusConsistency,
    pub status: InvocationStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnInvocationStatus {
    pub invocation_id: [u8; 16],
    pub status: InvocationStatus,
}

/// Opaque continuation for one owner's live invocation page sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvocationListCursor {
    principal_id: [u8; 16],
    owner_generation: u128,
    after_invocation_id: [u8; 16],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvocationStatusPage {
    pub observed_at_us: i64,
    pub consistency: InvocationStatusConsistency,
    pub invocations: Vec<OwnInvocationStatus>,
    pub continuation: Option<InvocationListCursor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdministrativeInvocationStatus {
    pub invocation_id: [u8; 16],
    pub principal_id: [u8; 16],
    pub status: InvocationStatus,
    pub cancellation_reason_code: Option<u16>,
}

/// Opaque continuation for an administrator's live runtime page sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdministrativeInvocationCursor {
    administrator_id: [u8; 16],
    registry_generation: u128,
    after_invocation_id: [u8; 16],
}

impl AdministrativeInvocationCursor {
    /// Serialize the process-local page cursor as lowercase hexadecimal.
    pub fn to_wire(&self) -> String {
        let mut bytes = [0; 49];
        bytes[0] = 1;
        bytes[1..17].copy_from_slice(&self.administrator_id);
        bytes[17..33].copy_from_slice(&self.registry_generation.to_be_bytes());
        bytes[33..49].copy_from_slice(&self.after_invocation_id);
        let mut output = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            output.push(char::from(b"0123456789abcdef"[(byte >> 4) as usize]));
            output.push(char::from(b"0123456789abcdef"[(byte & 0x0f) as usize]));
        }
        output
    }

    /// Parse a canonical wire cursor. Authorization and generation freshness
    /// are still checked by `list_runtime_invocations` on every page request.
    pub fn from_wire(value: &str) -> Option<Self> {
        if value.len() != 98 {
            return None;
        }
        let mut bytes = [0; 49];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            let high = lower_hex_value(pair[0])?;
            let low = lower_hex_value(pair[1])?;
            bytes[index] = high << 4 | low;
        }
        if bytes[0] != 1 {
            return None;
        }
        let administrator_id = bytes[1..17].try_into().ok()?;
        let registry_generation = u128::from_be_bytes(bytes[17..33].try_into().ok()?);
        let after_invocation_id = bytes[33..49].try_into().ok()?;
        if administrator_id == [0; 16] || registry_generation == 0 || after_invocation_id == [0; 16]
        {
            return None;
        }
        Some(Self {
            administrator_id,
            registry_generation,
            after_invocation_id,
        })
    }
}

fn lower_hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdministrativeInvocationPage {
    pub observed_at_us: i64,
    pub consistency: InvocationStatusConsistency,
    pub invocations: Vec<AdministrativeInvocationStatus>,
    pub continuation: Option<AdministrativeInvocationCursor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InvocationStatusError {
    InvalidRequest,
    InvalidPageLimit,
    InvalidCursor,
    StaleCursor,
    Storage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OwnerCancellationResult {
    Accepted,
    TooLate,
    Terminal(InvocationOutcome),
    Unknown,
}

impl std::fmt::Display for InvocationStatusError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invocation status lookup failed: {self:?}")
    }
}

impl std::error::Error for InvocationStatusError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdministrativeInvocationListError {
    InvalidRequest,
    InvalidPageLimit,
    InvalidCursor,
    StaleCursor,
    Unauthorized,
    Storage,
}

impl std::fmt::Display for AdministrativeInvocationListError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "administrative invocation listing failed: {self:?}"
        )
    }
}

impl std::error::Error for AdministrativeInvocationListError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdministrativeCancellationResult {
    Accepted,
    TooLate,
    Terminal(InvocationOutcome),
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdministrativeCancellationError {
    InvalidRequest,
    InvalidAudit,
    Unauthorized,
    RateLimited,
    Storage,
}

impl std::fmt::Display for AdministrativeCancellationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "administrative cancellation failed: {self:?}")
    }
}

impl std::error::Error for AdministrativeCancellationError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TokenLifecycleError {
    InvalidRequest,
    InvalidAudit,
    RandomnessUnavailable,
    TokenIdGenerationFailed,
    TokenUnavailable,
    EncodingFailed,
    Storage(String),
    RateLimited,
    Commit(CommitError),
}

impl std::fmt::Display for TokenLifecycleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "token lifecycle operation failed: {self:?}")
    }
}

impl std::error::Error for TokenLifecycleError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IdentityLifecycleError {
    InvalidRequest,
    InvalidAudit,
    RandomnessUnavailable,
    IdentityIdGenerationFailed,
    Unauthorized,
    Storage,
    Commit(CommitError),
}

impl std::fmt::Display for IdentityLifecycleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "identity lifecycle operation failed: {self:?}")
    }
}

impl std::error::Error for IdentityLifecycleError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipLifecycleError {
    InvalidRequest,
    InvalidAudit,
    Unauthorized,
    Storage,
    Commit(CommitError),
}

impl std::fmt::Display for MembershipLifecycleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "membership lifecycle operation failed: {self:?}")
    }
}

impl std::error::Error for MembershipLifecycleError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdministrativeSystemPrivilege {
    ManageUsers,
    ManageRoles,
    CreateSchema,
}

impl AdministrativeSystemPrivilege {
    fn catalog_code(self) -> u16 {
        match self {
            Self::ManageUsers => 12,
            Self::ManageRoles => 13,
            Self::CreateSchema => 14,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SystemPrivilegeChange {
    pub grantee_id: [u8; 16],
    pub privilege: AdministrativeSystemPrivilege,
    pub grant: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SystemPrivilegeLifecycleError {
    InvalidRequest,
    InvalidAudit,
    Unauthorized,
    Storage,
    Commit(CommitError),
}

impl std::fmt::Display for SystemPrivilegeLifecycleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "system privilege operation failed: {self:?}")
    }
}

impl std::error::Error for SystemPrivilegeLifecycleError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedUser {
    pub user_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedRole {
    pub role_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoleMembershipChange {
    pub member_id: [u8; 16],
    pub role_id: [u8; 16],
    pub grant: bool,
}

#[derive(Clone, Copy)]
enum NewPrincipalKind {
    User,
    Role,
}

#[derive(Clone, Debug)]
pub struct TokenIssueRequest<'a> {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub user_id: [u8; 16],
    pub label: &'a str,
    pub expires_at_us: i64,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

/// Non-secret metadata for one issued token. Verifier bytes are never exposed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TokenMetadata {
    pub token_id: [u8; 16],
    pub label: String,
    pub issued_at_us: i64,
    pub expires_at_us: i64,
    pub revoked: bool,
    pub revoked_at_us: Option<i64>,
}

/// A bounded page of token metadata ordered by token ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TokenMetadataPage {
    pub tokens: Vec<TokenMetadata>,
    pub next_after_token_id: Option<[u8; 16]>,
}

/// Non-secret identity context established by read-only gateway AUTH.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedQuerySession {
    connection_id: [u8; 16],
    token_id: [u8; 16],
    principal_id: [u8; 16],
    role_ids: HashSet<[u8; 16]>,
}

impl AuthenticatedQuerySession {
    /// Opaque server-generated session identifier returned to the gateway
    /// client after AUTH succeeds.
    pub fn connection_id(&self) -> [u8; 16] {
        self.connection_id
    }

    pub fn principal_id(&self) -> [u8; 16] {
        self.principal_id
    }

    pub fn role_ids(&self) -> &HashSet<[u8; 16]> {
        &self.role_ids
    }

    pub(crate) fn token_id(&self) -> [u8; 16] {
        self.token_id
    }

    /// Create the staging owner from this authenticated identity. The
    /// connection ID is generated by storage and cannot be supplied by a
    /// network client.
    pub fn staging_owner(&self) -> crate::supervisor::StagingOwner {
        crate::supervisor::StagingOwner::authenticated(self.connection_id, self.principal_id)
    }
}

/// Newly issued bearer token. Call [`Self::take_secret`] once to transfer the
/// secret to the protected caller; any secret still held here is zeroized on
/// drop. This type intentionally has no `Debug` implementation.
pub struct IssuedToken {
    pub token_id: [u8; 16],
    pub commit_sequence: u64,
    secret: Option<[u8; TOKEN_SECRET_BYTES]>,
}

impl IssuedToken {
    pub fn take_secret(&mut self) -> Option<[u8; TOKEN_SECRET_BYTES]> {
        self.secret.take()
    }
}

impl Drop for IssuedToken {
    fn drop(&mut self) {
        if let Some(secret) = &mut self.secret {
            secret.zeroize();
        }
    }
}

/// Shares one per-table DDL fence manager across row and catalog operations for
/// a database handle.
#[derive(Clone)]
pub struct StorageManager<'db> {
    database: &'db Database,
    ddl_fences: DdlFenceCoordinator,
    max_active_invocations: usize,
    admissions: Arc<InvocationAdmissionState>,
    token_requests: Arc<TokenRequestLimiter>,
    admin_cancellation_requests: Arc<TokenRequestLimiter>,
    max_invocation_age: Duration,
}

/// One token-authenticated invocation owned by the trusted supervisor-facing
/// storage service. The transaction cannot be finalized through executor or
/// planner methods and is aborted if this owner drops it without a decision.
pub struct SupervisorInvocation<'db> {
    invocation_id: [u8; 16],
    principal_id: [u8; 16],
    role_ids: HashSet<[u8; 16]>,
    transaction: Arc<Mutex<Option<InvocationTransaction<'db>>>>,
    channels: Arc<Mutex<SupervisorChannelState>>,
    deadline: Option<std::time::Instant>,
}

#[derive(Clone, Copy, Debug)]
pub struct SupervisorAdmissionRequest {
    pub invocation_id: [u8; 16],
    pub token_id: [u8; 16],
    pub retention_deadline_us: i64,
    pub max_rows: usize,
    pub max_bytes: usize,
    pub deadline: Option<std::time::Instant>,
}

#[derive(Default)]
struct SupervisorChannelState {
    active_worker_channels: usize,
    terminal_requested: bool,
    worker_exit_observing: bool,
    worker_exit_succeeded: bool,
}

struct SupervisorWorkerChannelLease {
    channels: Arc<Mutex<SupervisorChannelState>>,
}

#[derive(Debug)]
pub enum SupervisorInvocationError {
    Closed,
    WorkerChannelsActive,
    WorkerExitNotObserved,
    WorkerExitObservationActive,
    WorkerChannelIo(std::io::Error),
    WorkerProcessIo(std::io::Error),
    WorkerCgroupIo(std::io::Error),
    WorkerExited(std::process::ExitStatus),
    WorkerHandlerPanicked,
    Deadline(TransactionError),
    Executor(crate::executor_ipc::ExecutorConnectionError),
    Planner(crate::planner_ipc::PlannerConnectionError),
    Supervisor(crate::supervisor_ipc::SupervisorConnectionError),
}

impl std::fmt::Display for SupervisorInvocationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "supervisor invocation failed: {self:?}")
    }
}

impl std::error::Error for SupervisorInvocationError {}

impl Drop for SupervisorWorkerChannelLease {
    fn drop(&mut self) {
        let mut channels = self
            .channels
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        channels.active_worker_channels = channels.active_worker_channels.saturating_sub(1);
    }
}

impl SupervisorInvocation<'_> {
    pub fn invocation_id(&self) -> [u8; 16] {
        self.invocation_id
    }

    pub fn principal_id(&self) -> [u8; 16] {
        self.principal_id
    }

    pub fn role_ids(&self) -> &HashSet<[u8; 16]> {
        &self.role_ids
    }

    fn enter_worker_channel(
        &self,
    ) -> Result<SupervisorWorkerChannelLease, SupervisorInvocationError> {
        let mut channels = match self.channels.lock() {
            Ok(channels) => channels,
            Err(poisoned) => {
                let mut channels = poisoned.into_inner();
                channels.terminal_requested = true;
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::Closed);
            }
        };
        if channels.terminal_requested
            || channels.worker_exit_observing
            || channels.worker_exit_succeeded
        {
            return Err(SupervisorInvocationError::Closed);
        }
        channels.active_worker_channels = channels
            .active_worker_channels
            .checked_add(1)
            .ok_or(SupervisorInvocationError::Closed)?;
        Ok(SupervisorWorkerChannelLease {
            channels: Arc::clone(&self.channels),
        })
    }

    fn abort_owned_invocation(&self) {
        let mut owner = self
            .transaction
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(mut transaction) = owner.take() {
            let _ = transaction.abort();
        }
    }

    /// Poll cancellation and transaction age while worker channels are idle.
    /// A failed check durably aborts the invocation through its transaction.
    pub fn check_deadline(&self) -> Result<(), SupervisorInvocationError> {
        let mut owner = match self.transaction.lock() {
            Ok(owner) => owner,
            Err(poisoned) => {
                let mut owner = poisoned.into_inner();
                if let Some(mut transaction) = owner.take() {
                    let _ = transaction.abort();
                }
                return Err(SupervisorInvocationError::Closed);
            }
        };
        let transaction = owner.as_mut().ok_or(SupervisorInvocationError::Closed)?;
        transaction
            .check_deadline()
            .map_err(SupervisorInvocationError::Deadline)?;
        if self
            .deadline
            .is_some_and(|deadline| std::time::Instant::now() >= deadline)
        {
            transaction
                .abort()
                .map_err(SupervisorInvocationError::Deadline)?;
            return Err(SupervisorInvocationError::Deadline(
                TransactionError::Expired,
            ));
        }
        Ok(())
    }

    /// Observe and reap a worker after its planner and executor channels have
    /// drained. A failed exit or process-inspection error durably aborts the
    /// invocation; the caller may request commit only after this returns Ok.
    pub fn observe_worker_exit(
        &self,
        worker: &mut std::process::Child,
    ) -> Result<std::process::ExitStatus, SupervisorInvocationError> {
        self.observe_worker_exit_inner(worker, None)
    }

    /// Observe a worker whose process was placed in `cgroup` before exec.
    /// The cgroup is killed and verified empty before successful exit can
    /// authorize terminal storage decisions.
    pub fn observe_worker_exit_in_cgroup(
        &self,
        worker: &mut std::process::Child,
        cgroup: crate::linux_sandbox::WorkerCgroup,
        cleanup_timeout: Duration,
    ) -> Result<std::process::ExitStatus, SupervisorInvocationError> {
        self.observe_worker_exit_inner(worker, Some((cgroup, cleanup_timeout)))
    }

    fn observe_worker_exit_inner(
        &self,
        worker: &mut std::process::Child,
        mut cgroup: Option<(crate::linux_sandbox::WorkerCgroup, Duration)>,
    ) -> Result<std::process::ExitStatus, SupervisorInvocationError> {
        {
            let mut channels = match self.channels.lock() {
                Ok(channels) => channels,
                Err(poisoned) => {
                    let mut channels = poisoned.into_inner();
                    channels.terminal_requested = true;
                    self.abort_owned_invocation();
                    return Err(SupervisorInvocationError::Closed);
                }
            };
            if channels.terminal_requested {
                return Err(SupervisorInvocationError::Closed);
            }
            if channels.worker_exit_succeeded {
                return Err(SupervisorInvocationError::Closed);
            }
            if channels.active_worker_channels != 0 {
                return Err(SupervisorInvocationError::WorkerChannelsActive);
            }
            if channels.worker_exit_observing {
                return Err(SupervisorInvocationError::WorkerExitObservationActive);
            }
            channels.worker_exit_observing = true;
        }

        loop {
            match worker.try_wait() {
                Ok(Some(status)) if status.success() => {
                    if let Err(error) = Self::cleanup_worker_cgroup(&mut cgroup) {
                        self.finish_worker_exit_observation(false)?;
                        self.abort_owned_invocation();
                        return Err(error);
                    }
                    self.finish_worker_exit_observation(true)?;
                    return Ok(status);
                }
                Ok(Some(status)) => {
                    let cleanup_error = Self::cleanup_worker_cgroup(&mut cgroup).err();
                    self.finish_worker_exit_observation(false)?;
                    self.abort_owned_invocation();
                    if let Some(error) = cleanup_error {
                        return Err(error);
                    }
                    return Err(SupervisorInvocationError::WorkerExited(status));
                }
                Ok(None) => {
                    if let Err(error) = self.check_deadline() {
                        let _ = worker.kill();
                        let _ = worker.wait();
                        let cleanup_error = Self::cleanup_worker_cgroup(&mut cgroup).err();
                        self.finish_worker_exit_observation(false)?;
                        if let Some(cleanup_error) = cleanup_error {
                            self.abort_owned_invocation();
                            return Err(cleanup_error);
                        }
                        return Err(error);
                    }
                    std::thread::sleep(SUPERVISOR_IDLE_POLL_INTERVAL);
                }
                Err(error) => {
                    let _ = worker.kill();
                    let _ = worker.wait();
                    let cleanup_error = Self::cleanup_worker_cgroup(&mut cgroup).err();
                    self.finish_worker_exit_observation(false)?;
                    self.abort_owned_invocation();
                    if let Some(cleanup_error) = cleanup_error {
                        return Err(cleanup_error);
                    }
                    return Err(SupervisorInvocationError::WorkerProcessIo(error));
                }
            }
        }
    }

    fn cleanup_worker_cgroup(
        cgroup: &mut Option<(crate::linux_sandbox::WorkerCgroup, Duration)>,
    ) -> Result<(), SupervisorInvocationError> {
        if let Some((cgroup, timeout)) = cgroup.take() {
            cgroup
                .cleanup(timeout)
                .map_err(SupervisorInvocationError::WorkerCgroupIo)?;
        }
        Ok(())
    }

    fn finish_worker_exit_observation(
        &self,
        succeeded: bool,
    ) -> Result<(), SupervisorInvocationError> {
        let mut channels = match self.channels.lock() {
            Ok(channels) => channels,
            Err(poisoned) => {
                let mut channels = poisoned.into_inner();
                channels.worker_exit_observing = false;
                channels.terminal_requested = true;
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::Closed);
            }
        };
        channels.worker_exit_observing = false;
        channels.worker_exit_succeeded = succeeded;
        if !succeeded {
            channels.terminal_requested = true;
        }
        Ok(())
    }

    /// Serve provisioned planner and executor Unix streams concurrently.
    /// Poll cancellation/deadline while either stream is idle, stop its peer
    /// after a handler error, and return only after both handlers have drained.
    pub fn serve_worker_channels(
        &self,
        mut planner_stream: std::os::unix::net::UnixStream,
        mut executor_stream: std::os::unix::net::UnixStream,
    ) -> Result<(), SupervisorInvocationError> {
        use std::net::Shutdown;

        let stop_planner = match planner_stream.try_clone() {
            Ok(stream) => stream,
            Err(error) => {
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::WorkerChannelIo(error));
            }
        };
        let stop_executor = match executor_stream.try_clone() {
            Ok(stream) => stream,
            Err(error) => {
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::WorkerChannelIo(error));
            }
        };
        let planner_peer = match stop_executor.try_clone() {
            Ok(stream) => stream,
            Err(error) => {
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::WorkerChannelIo(error));
            }
        };
        let executor_peer = match stop_planner.try_clone() {
            Ok(stream) => stream,
            Err(error) => {
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::WorkerChannelIo(error));
            }
        };

        std::thread::scope(|scope| {
            let (completed_tx, completed_rx) = std::sync::mpsc::channel();
            let planner_completed = completed_tx.clone();
            let planner = scope.spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    self.serve_planner(&mut planner_stream)
                }))
                .unwrap_or(Err(SupervisorInvocationError::WorkerHandlerPanicked));
                if result.is_err() {
                    let _ = planner_peer.shutdown(Shutdown::Both);
                }
                let _ = planner_completed.send((true, result));
            });
            let executor_completed = completed_tx.clone();
            let executor = scope.spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    self.serve_executor(&mut executor_stream)
                }))
                .unwrap_or(Err(SupervisorInvocationError::WorkerHandlerPanicked));
                if result.is_err() {
                    let _ = executor_peer.shutdown(Shutdown::Both);
                }
                let _ = executor_completed.send((false, result));
            });
            drop(completed_tx);

            let mut planner_result = None;
            let mut executor_result = None;
            let mut completed = 0;
            let mut deadline_error = None;
            while completed < 2 {
                match completed_rx.recv_timeout(SUPERVISOR_IDLE_POLL_INTERVAL) {
                    Ok((is_planner, result)) => {
                        completed += 1;
                        if is_planner {
                            planner_result = Some(result);
                        } else {
                            executor_result = Some(result);
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        if let Err(error) = self.check_deadline() {
                            deadline_error = Some(error);
                            let _ = stop_planner.shutdown(Shutdown::Both);
                            let _ = stop_executor.shutdown(Shutdown::Both);
                            break;
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
            while completed < 2 {
                let Ok((is_planner, result)) = completed_rx.recv() else {
                    break;
                };
                completed += 1;
                if is_planner {
                    planner_result = Some(result);
                } else {
                    executor_result = Some(result);
                }
            }

            let planner_panicked = planner.join().is_err();
            let executor_panicked = executor.join().is_err();
            if planner_panicked || executor_panicked {
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::WorkerHandlerPanicked);
            }
            if let Some(error) = deadline_error {
                return Err(error);
            }
            if planner_result.as_ref().is_some_and(|result| {
                matches!(
                    result,
                    Err(SupervisorInvocationError::WorkerHandlerPanicked)
                )
            }) || executor_result.as_ref().is_some_and(|result| {
                matches!(
                    result,
                    Err(SupervisorInvocationError::WorkerHandlerPanicked)
                )
            }) {
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::WorkerHandlerPanicked);
            }
            planner_result.unwrap_or(Err(SupervisorInvocationError::Closed))?;
            executor_result.unwrap_or(Err(SupervisorInvocationError::Closed))?;
            Ok(())
        })
    }

    pub fn serve_planner<S: std::io::Read + std::io::Write>(
        &self,
        stream: &mut S,
    ) -> Result<(), SupervisorInvocationError> {
        let _lease = self.enter_worker_channel()?;
        crate::planner_ipc::serve_planner_shared(stream, &self.transaction)
            .map_err(SupervisorInvocationError::Planner)
    }

    pub fn serve_executor<S: std::io::Read + std::io::Write>(
        &self,
        stream: &mut S,
    ) -> Result<(), SupervisorInvocationError> {
        let _lease = self.enter_worker_channel()?;
        crate::executor_ipc::serve_executor_shared(stream, &self.transaction)
            .map_err(SupervisorInvocationError::Executor)
    }

    pub fn serve_terminal<S: std::io::Read + std::io::Write>(
        &mut self,
        stream: &mut S,
    ) -> Result<(), SupervisorInvocationError> {
        let mut channels = match self.channels.lock() {
            Ok(channels) => channels,
            Err(poisoned) => {
                let mut channels = poisoned.into_inner();
                channels.terminal_requested = true;
                self.abort_owned_invocation();
                return Err(SupervisorInvocationError::Closed);
            }
        };
        if channels.terminal_requested {
            return Err(SupervisorInvocationError::Closed);
        }
        if channels.active_worker_channels != 0 {
            return Err(SupervisorInvocationError::WorkerChannelsActive);
        }
        if channels.worker_exit_observing {
            return Err(SupervisorInvocationError::WorkerExitObservationActive);
        }
        if !channels.worker_exit_succeeded {
            return Err(SupervisorInvocationError::WorkerExitNotObserved);
        }
        channels.terminal_requested = true;
        drop(channels);
        let mut transaction = match self.transaction.lock() {
            Ok(transaction) => transaction,
            Err(poisoned) => {
                let mut transaction = poisoned.into_inner();
                if let Some(mut invocation) = transaction.take() {
                    let _ = invocation.abort();
                }
                return Err(SupervisorInvocationError::Closed);
            }
        };
        crate::supervisor_ipc::serve_supervisor(stream, &mut transaction)
            .map_err(SupervisorInvocationError::Supervisor)
    }

    pub fn serve_control<S: std::io::Read + std::io::Write>(
        &self,
        manager: &StorageManager<'_>,
        stream: &mut S,
    ) -> Result<(), SupervisorInvocationError> {
        crate::supervisor_ipc::serve_supervisor_control(stream, manager, self.principal_id)
            .map_err(SupervisorInvocationError::Supervisor)
    }
}

impl Drop for SupervisorInvocation<'_> {
    fn drop(&mut self) {
        let mut owner = self
            .transaction
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(mut transaction) = owner.take() {
            let _ = transaction.abort();
        }
    }
}

struct TokenRequestLimiter {
    limit: AtomicUsize,
    requests: Mutex<HashMap<[u8; 16], VecDeque<Instant>>>,
}

impl TokenRequestLimiter {
    fn new(limit: usize) -> Self {
        Self {
            limit: AtomicUsize::new(limit),
            requests: Mutex::new(HashMap::new()),
        }
    }

    fn consume(&self, actor_id: [u8; 16]) -> Result<(), TokenLifecycleError> {
        let now = Instant::now();
        let mut requests = self
            .requests
            .lock()
            .map_err(|_| TokenLifecycleError::RateLimited)?;
        requests.retain(|_, history| {
            while history.front().is_some_and(|oldest| {
                now.saturating_duration_since(*oldest) >= TOKEN_REQUEST_WINDOW
            }) {
                history.pop_front();
            }
            !history.is_empty()
        });
        if !requests.contains_key(&actor_id) {
            if requests.len() >= MAX_TRACKED_TOKEN_REQUEST_ACTORS {
                return Err(TokenLifecycleError::RateLimited);
            }
            requests.insert(actor_id, VecDeque::new());
        }
        let history = requests
            .get_mut(&actor_id)
            .ok_or(TokenLifecycleError::RateLimited)?;
        if history.len() >= self.limit.load(Ordering::Relaxed) {
            return Err(TokenLifecycleError::RateLimited);
        }
        history.push_back(now);
        Ok(())
    }
}

fn shared_token_request_limiter(
    database: &Database,
    configured_limit: usize,
) -> Arc<TokenRequestLimiter> {
    let database_key = database as *const Database as usize;
    let registry = TOKEN_REQUEST_LIMITERS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    registry.retain(|_, limiter| limiter.strong_count() != 0);
    if let Some(limiter) = registry.get(&database_key).and_then(Weak::upgrade) {
        limiter.limit.fetch_min(configured_limit, Ordering::Relaxed);
        return limiter;
    }
    let limiter = Arc::new(TokenRequestLimiter::new(configured_limit));
    registry.insert(database_key, Arc::downgrade(&limiter));
    limiter
}

fn shared_admin_cancellation_limiter(
    database: &Database,
    configured_limit: usize,
) -> Arc<TokenRequestLimiter> {
    let database_key = database as *const Database as usize;
    let registry = ADMIN_CANCELLATION_LIMITERS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    registry.retain(|_, limiter| limiter.strong_count() != 0);
    if let Some(limiter) = registry.get(&database_key).and_then(Weak::upgrade) {
        limiter.limit.fetch_min(configured_limit, Ordering::Relaxed);
        return limiter;
    }
    let limiter = Arc::new(TokenRequestLimiter::new(configured_limit));
    registry.insert(database_key, Arc::downgrade(&limiter));
    limiter
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LiveInvocationPhase {
    Active,
    CancellationPending,
    Committing,
}

#[derive(Clone, Copy)]
struct LiveInvocation {
    principal_id: [u8; 16],
    started_at_us: i64,
    deadline_at_us: Option<i64>,
    phase: LiveInvocationPhase,
    cancellation_reason_code: Option<u16>,
    owner_generation: u128,
}

struct InvocationAdmissionState {
    limit: AtomicUsize,
    live: Mutex<LiveInvocationRegistry>,
    changed: Condvar,
}

struct LiveInvocationRegistry {
    generation: u128,
    entries: HashMap<[u8; 16], LiveInvocation>,
}

impl LiveInvocationRegistry {
    fn advance_owner_generation(&mut self, principal_id: [u8; 16]) {
        self.generation = self.generation.wrapping_add(1).max(1);
        let generation = self.generation;
        for invocation in self.entries.values_mut() {
            if invocation.principal_id == principal_id {
                invocation.owner_generation = generation;
            }
        }
    }
}

pub(crate) struct InvocationAdmissionPermit {
    state: Arc<InvocationAdmissionState>,
    invocation_id: [u8; 16],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdmissionError {
    Limit,
    Duplicate,
}

impl InvocationAdmissionState {
    fn acquire(
        self: &Arc<Self>,
        invocation_id: [u8; 16],
        principal_id: [u8; 16],
        started_at_us: i64,
        max_age: Duration,
    ) -> Result<InvocationAdmissionPermit, AdmissionError> {
        let mut live = self
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if live.entries.contains_key(&invocation_id) {
            return Err(AdmissionError::Duplicate);
        }
        if live.entries.len() >= self.limit.load(Ordering::Acquire) {
            return Err(AdmissionError::Limit);
        }
        let deadline_at_us = i64::try_from(max_age.as_micros())
            .ok()
            .and_then(|delta| started_at_us.checked_add(delta));
        live.entries.insert(
            invocation_id,
            LiveInvocation {
                principal_id,
                started_at_us,
                deadline_at_us,
                phase: LiveInvocationPhase::Active,
                cancellation_reason_code: None,
                owner_generation: 0,
            },
        );
        live.advance_owner_generation(principal_id);
        Ok(InvocationAdmissionPermit {
            state: Arc::clone(self),
            invocation_id,
        })
    }

    fn status(&self, invocation_id: [u8; 16]) -> Option<LiveInvocation> {
        self.live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .get(&invocation_id)
            .copied()
    }

    fn own_page(
        &self,
        principal_id: [u8; 16],
        limit: usize,
        cursor: Option<&InvocationListCursor>,
    ) -> Result<(Vec<OwnInvocationStatus>, Option<InvocationListCursor>), InvocationStatusError>
    {
        let live = self
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let owner_generation = live
            .entries
            .values()
            .filter(|invocation| invocation.principal_id == principal_id)
            .map(|invocation| invocation.owner_generation)
            .max();
        if let Some(cursor) = cursor {
            if cursor.principal_id != principal_id {
                return Err(InvocationStatusError::InvalidCursor);
            }
            if owner_generation != Some(cursor.owner_generation) {
                return Err(InvocationStatusError::StaleCursor);
            }
        }
        let after = cursor.map(|cursor| cursor.after_invocation_id);
        let mut entries = live
            .entries
            .iter()
            .filter(|(invocation_id, invocation)| {
                invocation.principal_id == principal_id
                    && after.is_none_or(|after| **invocation_id > after)
            })
            .map(|(invocation_id, invocation)| OwnInvocationStatus {
                invocation_id: *invocation_id,
                status: match invocation.phase {
                    LiveInvocationPhase::Active => InvocationStatus::Active {
                        started_at_us: invocation.started_at_us,
                        deadline_at_us: invocation.deadline_at_us,
                        cancellation_requested: invocation.cancellation_reason_code.is_some(),
                    },
                    LiveInvocationPhase::CancellationPending => InvocationStatus::Active {
                        started_at_us: invocation.started_at_us,
                        deadline_at_us: invocation.deadline_at_us,
                        cancellation_requested: false,
                    },
                    LiveInvocationPhase::Committing => InvocationStatus::Committing {
                        started_at_us: invocation.started_at_us,
                        deadline_at_us: invocation.deadline_at_us,
                        cancellation_requested: invocation.cancellation_reason_code.is_some(),
                    },
                },
            })
            .collect::<Vec<_>>();
        entries.sort_unstable_by_key(|entry| entry.invocation_id);
        let has_more = entries.len() > limit;
        entries.truncate(limit);
        let continuation = if has_more {
            let owner_generation = owner_generation.ok_or(InvocationStatusError::InvalidRequest)?;
            entries.last().map(|entry| InvocationListCursor {
                principal_id,
                owner_generation,
                after_invocation_id: entry.invocation_id,
            })
        } else {
            None
        };
        Ok((entries, continuation))
    }

    fn administrative_page(
        &self,
        administrator_id: [u8; 16],
        limit: usize,
        cursor: Option<&AdministrativeInvocationCursor>,
    ) -> Result<
        (
            Vec<AdministrativeInvocationStatus>,
            Option<AdministrativeInvocationCursor>,
        ),
        AdministrativeInvocationListError,
    > {
        let live = self
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(cursor) = cursor {
            if cursor.administrator_id != administrator_id {
                return Err(AdministrativeInvocationListError::InvalidCursor);
            }
            if cursor.registry_generation != live.generation {
                return Err(AdministrativeInvocationListError::StaleCursor);
            }
        }
        let after = cursor.map(|cursor| cursor.after_invocation_id);
        let mut entries = live
            .entries
            .iter()
            .filter(|(invocation_id, _)| after.is_none_or(|after| **invocation_id > after))
            .map(
                |(invocation_id, invocation)| AdministrativeInvocationStatus {
                    invocation_id: *invocation_id,
                    principal_id: invocation.principal_id,
                    status: match invocation.phase {
                        LiveInvocationPhase::Active => InvocationStatus::Active {
                            started_at_us: invocation.started_at_us,
                            deadline_at_us: invocation.deadline_at_us,
                            cancellation_requested: invocation.cancellation_reason_code.is_some(),
                        },
                        LiveInvocationPhase::CancellationPending => InvocationStatus::Active {
                            started_at_us: invocation.started_at_us,
                            deadline_at_us: invocation.deadline_at_us,
                            cancellation_requested: false,
                        },
                        LiveInvocationPhase::Committing => InvocationStatus::Committing {
                            started_at_us: invocation.started_at_us,
                            deadline_at_us: invocation.deadline_at_us,
                            cancellation_requested: invocation.cancellation_reason_code.is_some(),
                        },
                    },
                    cancellation_reason_code: invocation.cancellation_reason_code,
                },
            )
            .collect::<Vec<_>>();
        entries.sort_unstable_by_key(|entry| entry.invocation_id);
        let has_more = entries.len() > limit;
        entries.truncate(limit);
        let continuation = if has_more {
            entries.last().map(|entry| AdministrativeInvocationCursor {
                administrator_id,
                registry_generation: live.generation,
                after_invocation_id: entry.invocation_id,
            })
        } else {
            None
        };
        Ok((entries, continuation))
    }
}

impl InvocationAdmissionPermit {
    pub(crate) fn may_continue(&self) -> bool {
        let mut live = self
            .state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            let phase = live
                .entries
                .get(&self.invocation_id)
                .map(|invocation| (invocation.phase, invocation.cancellation_reason_code));
            match phase {
                Some((LiveInvocationPhase::Active, None)) => return true,
                Some((LiveInvocationPhase::CancellationPending, _)) => {
                    live = self
                        .state
                        .changed
                        .wait(live)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                _ => return false,
            }
        }
    }

    pub(crate) fn try_mark_committing(&self) -> bool {
        let mut live = self
            .state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            let phase = live
                .entries
                .get(&self.invocation_id)
                .map(|invocation| (invocation.phase, invocation.cancellation_reason_code));
            match phase {
                Some((LiveInvocationPhase::Active, None)) => {
                    let Some(invocation) = live.entries.get_mut(&self.invocation_id) else {
                        return false;
                    };
                    invocation.phase = LiveInvocationPhase::Committing;
                    let principal_id = invocation.principal_id;
                    live.advance_owner_generation(principal_id);
                    self.state.changed.notify_all();
                    return true;
                }
                Some((LiveInvocationPhase::CancellationPending, _)) => {
                    live = self
                        .state
                        .changed
                        .wait(live)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                _ => return false,
            }
        }
    }
}

impl Drop for InvocationAdmissionPermit {
    fn drop(&mut self) {
        let mut live = self
            .state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(invocation) = live.entries.remove(&self.invocation_id) {
            live.advance_owner_generation(invocation.principal_id);
            self.state.changed.notify_all();
        }
    }
}

fn shared_invocation_registry(
    database: &Database,
    configured_limit: usize,
) -> Arc<InvocationAdmissionState> {
    let database_key = database as *const Database as usize;
    let registry = INVOCATION_REGISTRIES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    registry.retain(|_, invocations| invocations.strong_count() != 0);
    if let Some(invocations) = registry.get(&database_key).and_then(Weak::upgrade) {
        invocations
            .limit
            .fetch_min(configured_limit, Ordering::Relaxed);
        return invocations;
    }
    let invocations = Arc::new(InvocationAdmissionState {
        limit: AtomicUsize::new(configured_limit),
        live: Mutex::new(LiveInvocationRegistry {
            generation: 1,
            entries: HashMap::new(),
        }),
        changed: Condvar::new(),
    });
    registry.insert(database_key, Arc::downgrade(&invocations));
    invocations
}

impl<'db> StorageManager<'db> {
    /// Validate the recovered database, then use the default snapshot limit
    /// and the supplied DDL-fence timeout.
    pub fn new(
        database: &'db Database,
        ddl_fence_timeout: Duration,
    ) -> Result<Self, StorageManagerInitError> {
        Self::with_config(
            database,
            StorageManagerConfig {
                ddl_fence_timeout,
                ..StorageManagerConfig::default()
            },
        )
    }

    pub fn with_config(
        database: &'db Database,
        config: StorageManagerConfig,
    ) -> Result<Self, StorageManagerInitError> {
        Self::build(database, config, true)
    }

    pub(crate) fn from_validated_database(
        database: &'db Database,
        config: StorageManagerConfig,
    ) -> Result<Self, StorageManagerInitError> {
        Self::build(database, config, false)
    }

    fn build(
        database: &'db Database,
        config: StorageManagerConfig,
        validate_startup: bool,
    ) -> Result<Self, StorageManagerInitError> {
        if config.max_active_invocations == 0 {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::ZeroActiveInvocationLimit,
            ));
        }
        if config.max_invocation_age.is_zero() {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::ZeroInvocationAge,
            ));
        }
        if config.max_token_lifecycle_requests_per_minute == 0 {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::ZeroTokenLifecycleRequestLimit,
            ));
        }
        if config.max_admin_cancellation_requests_per_minute == 0 {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::ZeroAdminCancellationRequestLimit,
            ));
        }
        if validate_startup {
            validate_persisted_records(database).map_err(StorageManagerInitError::Startup)?;
        }
        let token_requests =
            shared_token_request_limiter(database, config.max_token_lifecycle_requests_per_minute);
        let admin_cancellation_requests = shared_admin_cancellation_limiter(
            database,
            config.max_admin_cancellation_requests_per_minute,
        );
        let admissions = shared_invocation_registry(database, config.max_active_invocations);
        Ok(Self {
            database,
            ddl_fences: DdlFenceCoordinator::new(DdlFenceManager::new(), config.ddl_fence_timeout),
            max_active_invocations: config.max_active_invocations,
            admissions,
            token_requests,
            admin_cancellation_requests,
            max_invocation_age: config.max_invocation_age,
        })
    }

    /// The effective hard cap shared by live manager handles for this database.
    pub fn max_active_invocations(&self) -> usize {
        self.max_active_invocations
            .min(self.admissions.limit.load(Ordering::Acquire))
    }

    pub fn max_invocation_age(&self) -> Duration {
        self.max_invocation_age
    }

    /// Begin a managed invocation after a trusted caller has established its
    /// identity and performed the required admission checks. Service callers
    /// should use [`Self::begin_authenticated_invocation`] to validate the
    /// principal against the pinned catalog. The slot remains held until
    /// commit, abort, expiry, or transaction drop.
    pub fn begin_invocation(
        &self,
        invocation_id: [u8; 16],
        principal_id: [u8; 16],
        retention_deadline_us: i64,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<InvocationTransaction<'db>, TransactionError> {
        let started_at_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        let admission_permit = self
            .admissions
            .acquire(
                invocation_id,
                principal_id,
                started_at_us,
                self.max_invocation_age,
            )
            .map_err(|error| match error {
                AdmissionError::Limit => TransactionError::AdmissionLimitExceeded,
                AdmissionError::Duplicate => TransactionError::DuplicateInvocation,
            })?;
        InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::managed(
                self.ddl_fences.clone(),
                admission_permit,
                self.max_invocation_age,
            ),
            invocation_id,
            principal_id,
            retention_deadline_us,
            max_rows,
            max_bytes,
        )
    }

    /// Admit an invocation for a principal already authenticated by a trusted
    /// caller, then load its role closure from the invocation's pinned catalog
    /// snapshot. This validates that the principal is a live LOGIN user and
    /// guards the identity and membership records through commit. It does not
    /// verify credentials, token expiry, or revocation; callers must complete
    /// those checks before invoking this method.
    pub fn begin_authenticated_invocation(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<(InvocationTransaction<'db>, HashSet<[u8; 16]>), TransactionError> {
        let mut transaction = self.begin_invocation(
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            max_rows,
            max_bytes,
        )?;
        let roles = transaction.load_catalog_role_closure(authenticated_principal_id)?;
        Ok((transaction, roles))
    }

    /// Authenticate a database-issued bearer token, then admit the invocation
    /// against one pinned catalog snapshot. Token, user, and membership bytes
    /// are guarded through commit; a concurrent revocation makes the pending
    /// invocation conflict.
    pub fn begin_token_authenticated_invocation(
        &self,
        invocation_id: [u8; 16],
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        retention_deadline_us: i64,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<(InvocationTransaction<'db>, HashSet<[u8; 16]>), TransactionError> {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut transaction = self.begin_invocation(
            invocation_id,
            principal_id,
            retention_deadline_us,
            max_rows,
            max_bytes,
        )?;
        let Some(now_us) = now_unix_micros() else {
            transaction.discard_unstarted_admission();
            return Err(TransactionError::ClockUnavailable);
        };
        let roles = transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        Ok((transaction, roles))
    }

    /// Authenticate a database token for a query/control gateway session using
    /// one read snapshot. This does not allocate an invocation or durable
    /// outcome; START must revalidate through token-bound admission.
    pub fn authenticate_query_session(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
    ) -> Result<AuthenticatedQuerySession, TransactionError> {
        let (principal_id, role_ids) = self.resolve_query_identity(token_id, token_secret)?;
        let connection_id = generate_query_connection_id()?;
        Ok(AuthenticatedQuerySession {
            connection_id,
            token_id,
            principal_id,
            role_ids,
        })
    }

    /// Refresh the current login and role closure before a new execution or
    /// control operation, preserving this network connection's session ID.
    pub fn revalidate_query_session(
        &self,
        session: &mut AuthenticatedQuerySession,
        token_secret: &[u8; TOKEN_SECRET_BYTES],
    ) -> Result<(), TransactionError> {
        let (principal_id, role_ids) =
            self.resolve_query_identity(session.token_id, token_secret)?;
        if principal_id != session.principal_id {
            return Err(TransactionError::AuthenticationFailed);
        }
        session.role_ids = role_ids;
        Ok(())
    }

    fn resolve_query_identity(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
    ) -> Result<([u8; 16], HashSet<[u8; 16]>), TransactionError> {
        if token_id == [0; 16] {
            return Err(TransactionError::AuthenticationFailed);
        }
        let Some(now_us) = now_unix_micros() else {
            return Err(TransactionError::ClockUnavailable);
        };
        let read = self
            .database
            .begin_read()
            .map_err(|error| TransactionError::Storage(error.to_string()))?;
        let key = credential_catalog_key(token_id);
        let bytes = {
            let catalog = read
                .open_table(CATALOG)
                .map_err(|error| TransactionError::Storage(error.to_string()))?;
            catalog
                .get(key.as_slice())
                .map_err(|error| TransactionError::Storage(error.to_string()))?
                .map(|value| value.value().to_vec())
                .ok_or(TransactionError::AuthenticationFailed)?
        };
        let record = decode_registered_envelope(&bytes)
            .map_err(|_| TransactionError::AuthenticationFailed)?;
        if record.logical_key != key {
            return Err(TransactionError::AuthenticationFailed);
        }
        let credential =
            decode_verifier_record(&record).map_err(|_| TransactionError::AuthenticationFailed)?;
        if !credential.is_valid_at(now_us) || !credential.matches_secret(token_secret) {
            return Err(TransactionError::AuthenticationFailed);
        }
        let principal_id = credential.user_id;
        let snapshot = RowSnapshot::new(read);
        let role_ids = token_actor_roles(&snapshot, principal_id).map_err(|error| match error {
            TokenLifecycleError::TokenUnavailable => TransactionError::AuthenticationFailed,
            TokenLifecycleError::Storage(message) => TransactionError::Storage(message),
            _ => TransactionError::AuthenticationFailed,
        })?;
        Ok((principal_id, role_ids))
    }

    /// Admit a token-authenticated invocation into the supervisor-owned
    /// storage context used by planner, executor, and terminal IPC handlers.
    pub fn admit_supervisor_invocation(
        &self,
        invocation_id: [u8; 16],
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        retention_deadline_us: i64,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<SupervisorInvocation<'db>, TransactionError> {
        self.admit_supervisor_invocation_inner(
            SupervisorAdmissionRequest {
                invocation_id,
                token_id,
                retention_deadline_us,
                max_rows,
                max_bytes,
                deadline: None,
            },
            token_secret,
        )
    }

    /// Admit a token-authenticated supervisor invocation with an additional
    /// caller-requested execution deadline bounded by trusted scheduling.
    pub fn admit_supervisor_invocation_with_deadline(
        &self,
        request: SupervisorAdmissionRequest,
        token_secret: &[u8; TOKEN_SECRET_BYTES],
    ) -> Result<SupervisorInvocation<'db>, TransactionError> {
        if request.deadline.is_none() {
            return Err(TransactionError::InvalidContext);
        }
        self.admit_supervisor_invocation_inner(request, token_secret)
    }

    fn admit_supervisor_invocation_inner(
        &self,
        request: SupervisorAdmissionRequest,
        token_secret: &[u8; TOKEN_SECRET_BYTES],
    ) -> Result<SupervisorInvocation<'db>, TransactionError> {
        let principal_id = lookup_credential_principal(self.database, request.token_id)?;
        let (transaction, role_ids) = self.begin_token_authenticated_invocation(
            request.invocation_id,
            request.token_id,
            token_secret,
            request.retention_deadline_us,
            request.max_rows,
            request.max_bytes,
        )?;
        Ok(SupervisorInvocation {
            invocation_id: request.invocation_id,
            principal_id,
            role_ids,
            transaction: Arc::new(Mutex::new(Some(transaction))),
            channels: Arc::new(Mutex::new(SupervisorChannelState::default())),
            deadline: request.deadline,
        })
    }

    /// Create a LOGIN user through the authorized identity DDL path.
    pub fn create_user(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        name: &str,
        retention_deadline_us: i64,
        audit_event: AuditEvent,
    ) -> Result<CreatedUser, IdentityLifecycleError> {
        let (user_id, commit_sequence) = self.create_identity(
            invocation_id,
            authenticated_actor_id,
            name,
            NewPrincipalKind::User,
            retention_deadline_us,
            audit_event,
        )?;
        Ok(CreatedUser {
            user_id,
            commit_sequence,
        })
    }

    /// Create a non-login role through the authorized identity DDL path.
    pub fn create_role(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        name: &str,
        retention_deadline_us: i64,
        audit_event: AuditEvent,
    ) -> Result<CreatedRole, IdentityLifecycleError> {
        let (role_id, commit_sequence) = self.create_identity(
            invocation_id,
            authenticated_actor_id,
            name,
            NewPrincipalKind::Role,
            retention_deadline_us,
            audit_event,
        )?;
        Ok(CreatedRole {
            role_id,
            commit_sequence,
        })
    }

    /// Rename a user or role through the authorized identity DDL path.
    pub fn rename_identity(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        identity_id: [u8; 16],
        name: &str,
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, IdentityLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || identity_id == [0; 16]
            || retention_deadline_us <= 0
            || !is_normalized_principal_name(name)
        {
            return Err(IdentityLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(IdentityLifecycleError::InvalidAudit);
        }
        let principal_key = principal_catalog_key(identity_id);
        let object_key = identity_object_catalog_key(identity_id);
        let read = self
            .database
            .begin_read()
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let principal_current = catalog
            .get(principal_key.as_slice())
            .map_err(|_| IdentityLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(IdentityLifecycleError::InvalidRequest)?;
        let object_current = catalog
            .get(object_key.as_slice())
            .map_err(|_| IdentityLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(IdentityLifecycleError::Storage)?;
        drop(catalog);
        drop(read);

        let mut principal = decode_registered_envelope(&principal_current)
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let kind = principal
            .fields
            .iter()
            .find(|field| field.tag == 1 && field.wire_type == WireType::U16)
            .and_then(|field| <[u8; 2]>::try_from(field.payload.as_slice()).ok())
            .map(u16::from_le_bytes)
            .filter(|kind| matches!(kind, 1 | 2))
            .ok_or(IdentityLifecycleError::Storage)?;
        let principal_name = principal
            .fields
            .iter_mut()
            .find(|field| field.tag == 2 && field.wire_type == WireType::Text)
            .ok_or(IdentityLifecycleError::Storage)?;
        if principal_name.payload == name.as_bytes() {
            return Err(IdentityLifecycleError::InvalidRequest);
        }
        principal_name.payload = name.as_bytes().to_vec();
        let epoch = principal
            .fields
            .iter_mut()
            .find(|field| field.tag == 4 && field.wire_type == WireType::U64)
            .ok_or(IdentityLifecycleError::Storage)?;
        let current_epoch = u64::from_le_bytes(
            epoch
                .payload
                .as_slice()
                .try_into()
                .map_err(|_| IdentityLifecycleError::Storage)?,
        );
        epoch.payload = current_epoch
            .checked_add(1)
            .filter(|next| *next != 0)
            .ok_or(IdentityLifecycleError::InvalidRequest)?
            .to_le_bytes()
            .to_vec();
        let principal_payload = encode_envelope(
            &family_schema(10).ok_or(IdentityLifecycleError::Storage)?,
            &principal_key,
            &principal.fields,
        )
        .map_err(|_| IdentityLifecycleError::Storage)?;

        let mut object = decode_registered_envelope(&object_current)
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let expected_object_kind = if kind == 1 { 8 } else { 9 };
        let object_kind = object
            .fields
            .iter()
            .find(|field| field.tag == 1 && field.wire_type == WireType::U16)
            .and_then(|field| <[u8; 2]>::try_from(field.payload.as_slice()).ok())
            .map(u16::from_le_bytes);
        if object_kind != Some(expected_object_kind) {
            return Err(IdentityLifecycleError::Storage);
        }
        let object_name = object
            .fields
            .iter_mut()
            .find(|field| field.tag == 3 && field.wire_type == WireType::Text)
            .ok_or(IdentityLifecycleError::Storage)?;
        object_name.payload = name.as_bytes().to_vec();
        let version = object
            .fields
            .iter_mut()
            .find(|field| field.tag == 5 && field.wire_type == WireType::U64)
            .ok_or(IdentityLifecycleError::Storage)?;
        let current_version = u64::from_le_bytes(
            version
                .payload
                .as_slice()
                .try_into()
                .map_err(|_| IdentityLifecycleError::Storage)?,
        );
        version.payload = current_version
            .checked_add(1)
            .filter(|next| *next != 0)
            .ok_or(IdentityLifecycleError::InvalidRequest)?
            .to_le_bytes()
            .to_vec();
        let object_payload = encode_envelope(
            &family_schema(2).ok_or(IdentityLifecycleError::Storage)?,
            &object_key,
            &object.fields,
        )
        .map_err(|_| IdentityLifecycleError::Storage)?;
        audit_event.target_ids = vec![identity_id];
        let writes = [
            CatalogWrite {
                key: principal_key,
                expected_value: Some(principal_current),
                payload: Some(principal_payload),
            },
            CatalogWrite {
                key: object_key,
                expected_value: Some(object_current),
                payload: Some(object_payload),
            },
        ];
        self.commit_authorized_identity_ddl(
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => IdentityLifecycleError::Unauthorized,
            error => IdentityLifecycleError::Commit(error),
        })
    }

    /// Change a user's LOGIN state through the authorized identity DDL path.
    /// Disabling LOGIN revokes every outstanding token in the same commit.
    pub fn set_user_login_enabled(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        user_id: [u8; 16],
        enabled: bool,
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, IdentityLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || user_id == [0; 16]
            || retention_deadline_us <= 0
        {
            return Err(IdentityLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(IdentityLifecycleError::InvalidAudit);
        }
        let principal_key = principal_catalog_key(user_id);
        let read = self
            .database
            .begin_read()
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let principal_current = catalog
            .get(principal_key.as_slice())
            .map_err(|_| IdentityLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(IdentityLifecycleError::InvalidRequest)?;
        let mut principal = decode_registered_envelope(&principal_current)
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let kind = principal
            .fields
            .iter()
            .find(|field| field.tag == 1 && field.wire_type == WireType::U16)
            .and_then(|field| <[u8; 2]>::try_from(field.payload.as_slice()).ok())
            .map(u16::from_le_bytes);
        if kind != Some(1) {
            return Err(IdentityLifecycleError::InvalidRequest);
        }
        let login_field = principal
            .fields
            .iter_mut()
            .find(|field| field.tag == 3 && field.wire_type == WireType::Bool)
            .ok_or(IdentityLifecycleError::Storage)?;
        let current_enabled = match login_field.payload.as_slice() {
            [0] => false,
            [1] => true,
            _ => return Err(IdentityLifecycleError::Storage),
        };
        if current_enabled == enabled {
            return Err(IdentityLifecycleError::InvalidRequest);
        }
        login_field.payload = vec![u8::from(enabled)];
        let epoch_field = principal
            .fields
            .iter_mut()
            .find(|field| field.tag == 4 && field.wire_type == WireType::U64)
            .ok_or(IdentityLifecycleError::Storage)?;
        let current_epoch = u64::from_le_bytes(
            epoch_field
                .payload
                .as_slice()
                .try_into()
                .map_err(|_| IdentityLifecycleError::Storage)?,
        );
        epoch_field.payload = current_epoch
            .checked_add(1)
            .filter(|next| *next != 0)
            .ok_or(IdentityLifecycleError::InvalidRequest)?
            .to_le_bytes()
            .to_vec();
        let principal_payload = encode_envelope(
            &family_schema(10).ok_or(IdentityLifecycleError::Storage)?,
            &principal_key,
            &principal.fields,
        )
        .map_err(|_| IdentityLifecycleError::Storage)?;
        let mut writes = vec![CatalogWrite {
            key: principal_key,
            expected_value: Some(principal_current),
            payload: Some(principal_payload),
        }];
        if !enabled {
            let revoked_at_us = now_unix_micros().ok_or(IdentityLifecycleError::InvalidRequest)?;
            let prefix = 16u16.to_be_bytes();
            let mut count = 0usize;
            for entry in catalog
                .range(prefix.as_slice()..)
                .map_err(|_| IdentityLifecycleError::Storage)?
            {
                let (key, value) = entry.map_err(|_| IdentityLifecycleError::Storage)?;
                let key = key.value().to_vec();
                if !key.starts_with(&prefix) {
                    break;
                }
                count += 1;
                if count > MAX_CATALOG_FAMILY_SCAN || key.len() != 19 || key[2] != 1 {
                    return Err(IdentityLifecycleError::Storage);
                }
                let current = value.value().to_vec();
                let record = decode_registered_envelope(&current)
                    .map_err(|_| IdentityLifecycleError::Storage)?;
                let verifier =
                    decode_verifier_record(&record).map_err(|_| IdentityLifecycleError::Storage)?;
                if verifier.user_id == user_id && !verifier.revoked {
                    let payload = encode_revoked_verifier_record(
                        &record,
                        revoked_at_us.max(verifier.issued_at_us),
                    )
                    .map_err(|_| IdentityLifecycleError::Storage)?;
                    writes.push(CatalogWrite {
                        key,
                        expected_value: Some(current),
                        payload: Some(payload),
                    });
                }
            }
        }
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![user_id];
        self.commit_authorized_identity_ddl(
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => IdentityLifecycleError::Unauthorized,
            error => IdentityLifecycleError::Commit(error),
        })
    }

    fn create_identity(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        name: &str,
        kind: NewPrincipalKind,
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<([u8; 16], u64), IdentityLifecycleError> {
        let (principal_kind, login_enabled, object_kind) = match kind {
            NewPrincipalKind::User => (1u16, true, 8u16),
            NewPrincipalKind::Role => (2u16, false, 9u16),
        };
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || retention_deadline_us <= 0
            || !is_normalized_principal_name(name)
        {
            return Err(IdentityLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(IdentityLifecycleError::InvalidAudit);
        }

        let user_id = self.generate_identity_id()?;
        let principal_key = principal_catalog_key(user_id);
        let object_key = identity_object_catalog_key(user_id);
        let principal_payload = encode_envelope(
            &family_schema(10).ok_or(IdentityLifecycleError::Storage)?,
            &principal_key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::U16,
                    payload: principal_kind.to_le_bytes().to_vec(),
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
                    payload: 1u64.to_le_bytes().to_vec(),
                },
            ],
        )
        .map_err(|_| IdentityLifecycleError::InvalidRequest)?;
        let object_payload = encode_envelope(
            &family_schema(2).ok_or(IdentityLifecycleError::Storage)?,
            &object_key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::U16,
                    payload: object_kind.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::Text,
                    payload: name.as_bytes().to_vec(),
                },
                CatalogField {
                    tag: 4,
                    wire_type: WireType::Id,
                    payload: crate::bootstrap::database_admin_role_id().to_vec(),
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
            ],
        )
        .map_err(|_| IdentityLifecycleError::InvalidRequest)?;
        audit_event.target_ids = vec![user_id];
        let writes = [
            CatalogWrite {
                key: principal_key,
                expected_value: None,
                payload: Some(principal_payload),
            },
            CatalogWrite {
                key: object_key,
                expected_value: None,
                payload: Some(object_payload),
            },
        ];
        let commit_sequence = self
            .commit_authorized_identity_ddl(
                invocation_id,
                authenticated_actor_id,
                retention_deadline_us,
                &writes,
                &[audit_event],
            )
            .map_err(|error| match error {
                CommitError::DdlAuthorizationDenied => IdentityLifecycleError::Unauthorized,
                error => IdentityLifecycleError::Commit(error),
            })?;
        Ok((user_id, commit_sequence))
    }

    /// Grant or revoke one role membership through the audited membership DDL path.
    pub fn set_role_membership(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        change: RoleMembershipChange,
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, MembershipLifecycleError> {
        let RoleMembershipChange {
            member_id,
            role_id,
            grant,
        } = change;
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || member_id == [0; 16]
            || role_id == [0; 16]
            || member_id == role_id
            || retention_deadline_us <= 0
        {
            return Err(MembershipLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::AuthorityChange
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(MembershipLifecycleError::InvalidAudit);
        }
        let membership_key = membership_catalog_key(member_id, role_id);
        let dependency_key = dependency_catalog_key(member_id, role_id, 5);
        let member_key = principal_catalog_key(member_id);
        let read = self
            .database
            .begin_read()
            .map_err(|_| MembershipLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| MembershipLifecycleError::Storage)?;
        let membership_value = catalog
            .get(membership_key.as_slice())
            .map_err(|_| MembershipLifecycleError::Storage)?
            .map(|value| value.value().to_vec());
        let dependency_value = catalog
            .get(dependency_key.as_slice())
            .map_err(|_| MembershipLifecycleError::Storage)?
            .map(|value| value.value().to_vec());
        let member_value = catalog
            .get(member_key.as_slice())
            .map_err(|_| MembershipLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(MembershipLifecycleError::InvalidRequest)?;
        drop(catalog);
        drop(read);
        if membership_value.is_some() != dependency_value.is_some() {
            return Err(MembershipLifecycleError::Storage);
        }
        let (membership_payload, dependency_payload) = if grant {
            if membership_value.is_some() {
                return Err(MembershipLifecycleError::InvalidRequest);
            }
            let membership_payload = encode_envelope(
                &family_schema(11).ok_or(MembershipLifecycleError::Storage)?,
                &membership_key,
                &[
                    CatalogField {
                        tag: 1,
                        wire_type: WireType::Id,
                        payload: authenticated_actor_id.to_vec(),
                    },
                    CatalogField {
                        tag: 2,
                        wire_type: WireType::U64,
                        payload: 1u64.to_le_bytes().to_vec(),
                    },
                ],
            )
            .map_err(|_| MembershipLifecycleError::InvalidRequest)?;
            let dependency_payload = encode_envelope(
                &family_schema(15).ok_or(MembershipLifecycleError::Storage)?,
                &dependency_key,
                &[],
            )
            .map_err(|_| MembershipLifecycleError::InvalidRequest)?;
            (Some(membership_payload), Some(dependency_payload))
        } else {
            if membership_value.is_none() {
                return Err(MembershipLifecycleError::InvalidRequest);
            }
            (None, None)
        };
        let mut member_record = decode_registered_envelope(&member_value)
            .map_err(|_| MembershipLifecycleError::Storage)?;
        let epoch_field = member_record
            .fields
            .iter_mut()
            .find(|field| field.tag == 4 && field.wire_type == WireType::U64)
            .ok_or(MembershipLifecycleError::Storage)?;
        let current_epoch = u64::from_le_bytes(
            epoch_field
                .payload
                .as_slice()
                .try_into()
                .map_err(|_| MembershipLifecycleError::Storage)?,
        );
        let next_epoch = current_epoch
            .checked_add(1)
            .filter(|epoch| *epoch != 0)
            .ok_or(MembershipLifecycleError::InvalidRequest)?;
        epoch_field.payload = next_epoch.to_le_bytes().to_vec();
        let member_payload = encode_envelope(
            &family_schema(10).ok_or(MembershipLifecycleError::Storage)?,
            &member_key,
            &member_record.fields,
        )
        .map_err(|_| MembershipLifecycleError::Storage)?;
        let mut target_ids = vec![member_id, role_id];
        target_ids.sort_unstable();
        target_ids.dedup();
        audit_event.target_ids = target_ids;
        let writes = [
            CatalogWrite {
                key: membership_key,
                expected_value: membership_value,
                payload: membership_payload,
            },
            CatalogWrite {
                key: dependency_key,
                expected_value: dependency_value,
                payload: dependency_payload,
            },
            CatalogWrite {
                key: member_key,
                expected_value: Some(member_value),
                payload: Some(member_payload),
            },
        ];
        self.commit_authorized_membership_ddl(
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => MembershipLifecycleError::Unauthorized,
            error => MembershipLifecycleError::Commit(error),
        })
    }

    /// Grant or revoke one database-scoped system privilege as an administrator.
    pub fn set_system_privilege(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        change: SystemPrivilegeChange,
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, SystemPrivilegeLifecycleError> {
        let SystemPrivilegeChange {
            grantee_id,
            privilege,
            grant,
        } = change;
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || grantee_id == [0; 16]
            || retention_deadline_us <= 0
        {
            return Err(SystemPrivilegeLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::AuthorityChange
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(SystemPrivilegeLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| SystemPrivilegeLifecycleError::Storage)?;
        let metadata_bytes = read
            .open_table(ENGINE_META)
            .map_err(|_| SystemPrivilegeLifecycleError::Storage)?
            .get(&b"format"[..])
            .map_err(|_| SystemPrivilegeLifecycleError::Storage)?
            .ok_or(SystemPrivilegeLifecycleError::Storage)?
            .value()
            .to_vec();
        let metadata = EngineMetadata::decode(&metadata_bytes)
            .map_err(|_| SystemPrivilegeLifecycleError::Storage)?;
        if metadata.lifecycle != Lifecycle::Ready || !metadata.supports_current_formats() {
            return Err(SystemPrivilegeLifecycleError::Storage);
        }
        let database_id = metadata.database_id;
        let key = system_privilege_catalog_key(grantee_id, database_id, privilege.catalog_code());
        let dependency_key = dependency_catalog_key(grantee_id, database_id, 5);
        let grantee_key = principal_catalog_key(grantee_id);
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| SystemPrivilegeLifecycleError::Storage)?;
        let current = catalog
            .get(key.as_slice())
            .map_err(|_| SystemPrivilegeLifecycleError::Storage)?
            .map(|value| value.value().to_vec());
        let dependency_current = catalog
            .get(dependency_key.as_slice())
            .map_err(|_| SystemPrivilegeLifecycleError::Storage)?
            .map(|value| value.value().to_vec());
        let grantee_current = catalog
            .get(grantee_key.as_slice())
            .map_err(|_| SystemPrivilegeLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(SystemPrivilegeLifecycleError::InvalidRequest)?;
        drop(catalog);
        drop(read);
        if current.is_some() != dependency_current.is_some() {
            return Err(SystemPrivilegeLifecycleError::Storage);
        }
        let (payload, dependency_payload) = if grant {
            if current.is_some() {
                return Err(SystemPrivilegeLifecycleError::InvalidRequest);
            }
            let payload = Some(
                encode_envelope(
                    &family_schema(12).ok_or(SystemPrivilegeLifecycleError::Storage)?,
                    &key,
                    &[
                        CatalogField {
                            tag: 1,
                            wire_type: WireType::Id,
                            payload: authenticated_actor_id.to_vec(),
                        },
                        CatalogField {
                            tag: 2,
                            wire_type: WireType::U64,
                            payload: 1u64.to_le_bytes().to_vec(),
                        },
                    ],
                )
                .map_err(|_| SystemPrivilegeLifecycleError::InvalidRequest)?,
            );
            let dependency_payload = Some(
                encode_envelope(
                    &family_schema(15).ok_or(SystemPrivilegeLifecycleError::Storage)?,
                    &dependency_key,
                    &[],
                )
                .map_err(|_| SystemPrivilegeLifecycleError::InvalidRequest)?,
            );
            (payload, dependency_payload)
        } else {
            if current.is_none() {
                return Err(SystemPrivilegeLifecycleError::InvalidRequest);
            }
            (None, None)
        };
        let mut grantee_record = decode_registered_envelope(&grantee_current)
            .map_err(|_| SystemPrivilegeLifecycleError::Storage)?;
        let epoch_field = grantee_record
            .fields
            .iter_mut()
            .find(|field| field.tag == 4 && field.wire_type == WireType::U64)
            .ok_or(SystemPrivilegeLifecycleError::Storage)?;
        let current_epoch = u64::from_le_bytes(
            epoch_field
                .payload
                .as_slice()
                .try_into()
                .map_err(|_| SystemPrivilegeLifecycleError::Storage)?,
        );
        let next_epoch = current_epoch
            .checked_add(1)
            .filter(|epoch| *epoch != 0)
            .ok_or(SystemPrivilegeLifecycleError::InvalidRequest)?;
        epoch_field.payload = next_epoch.to_le_bytes().to_vec();
        let grantee_payload = encode_envelope(
            &family_schema(10).ok_or(SystemPrivilegeLifecycleError::Storage)?,
            &grantee_key,
            &grantee_record.fields,
        )
        .map_err(|_| SystemPrivilegeLifecycleError::Storage)?;
        audit_event.target_ids = vec![database_id, grantee_id];
        audit_event.target_ids.sort_unstable();
        audit_event.target_ids.dedup();
        let writes = [
            CatalogWrite {
                key,
                expected_value: current,
                payload,
            },
            CatalogWrite {
                key: dependency_key,
                expected_value: dependency_current,
                payload: dependency_payload,
            },
            CatalogWrite {
                key: grantee_key,
                expected_value: Some(grantee_current),
                payload: Some(grantee_payload),
            },
        ];
        self.commit_authorized_system_privilege_ddl(
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => SystemPrivilegeLifecycleError::Unauthorized,
            error => SystemPrivilegeLifecycleError::Commit(error),
        })
    }

    fn generate_identity_id(&self) -> Result<[u8; 16], IdentityLifecycleError> {
        for _ in 0..8 {
            let mut user_id = [0; 16];
            if getrandom::fill(&mut user_id).is_err() {
                user_id.zeroize();
                return Err(IdentityLifecycleError::RandomnessUnavailable);
            }
            if user_id == [0; 16] {
                user_id.zeroize();
                continue;
            }
            let principal_key = principal_catalog_key(user_id);
            let object_key = identity_object_catalog_key(user_id);
            let tombstone_key = identity_tombstone_catalog_key(user_id);
            let read = self
                .database
                .begin_read()
                .map_err(|_| IdentityLifecycleError::Storage)?;
            let catalog = read
                .open_table(CATALOG)
                .map_err(|_| IdentityLifecycleError::Storage)?;
            let conflict = catalog
                .get(principal_key.as_slice())
                .map_err(|_| IdentityLifecycleError::Storage)?
                .is_some()
                || catalog
                    .get(object_key.as_slice())
                    .map_err(|_| IdentityLifecycleError::Storage)?
                    .is_some()
                || catalog
                    .get(tombstone_key.as_slice())
                    .map_err(|_| IdentityLifecycleError::Storage)?
                    .is_some();
            if !conflict {
                return Ok(user_id);
            }
            user_id.zeroize();
        }
        Err(IdentityLifecycleError::IdentityIdGenerationFailed)
    }

    /// Issue one random bearer token for a LOGIN user through the authorized
    /// identity DDL path. The secret is returned once and only its verifier is
    /// written to the catalog and audit event.
    pub fn issue_token(
        &self,
        request: TokenIssueRequest<'_>,
    ) -> Result<IssuedToken, TokenLifecycleError> {
        let TokenIssueRequest {
            invocation_id,
            authenticated_actor_id,
            user_id,
            label,
            expires_at_us,
            retention_deadline_us,
            mut audit_event,
        } = request;
        let Some(issued_at_us) = now_unix_micros() else {
            return Err(TokenLifecycleError::InvalidRequest);
        };
        if user_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || expires_at_us <= issued_at_us
            || label.len() > MAX_TOKEN_LABEL_BYTES
        {
            return Err(TokenLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.action != AuditAction::TokenLifecycle
            || audit_event.result != AuditResult::Succeeded
            || !audit_event.target_ids.contains(&user_id)
        {
            return Err(TokenLifecycleError::InvalidAudit);
        }
        self.token_requests.consume(authenticated_actor_id)?;

        let mut generated = None;
        for _ in 0..8 {
            let mut material = [0u8; 16 + TOKEN_SECRET_BYTES];
            if getrandom::fill(&mut material).is_err() {
                material.zeroize();
                return Err(TokenLifecycleError::RandomnessUnavailable);
            }
            let token_id: [u8; 16] = material[..16].try_into().expect("fixed token ID length");
            let mut secret: [u8; TOKEN_SECRET_BYTES] = material[16..]
                .try_into()
                .expect("fixed token secret length");
            material.zeroize();
            if token_id == [0; 16] {
                secret.zeroize();
                continue;
            }
            match credential_token_exists(self.database, token_id) {
                Ok(false) => {
                    generated = Some((token_id, secret));
                    secret.zeroize();
                    break;
                }
                Ok(true) => secret.zeroize(),
                Err(error) => {
                    secret.zeroize();
                    return Err(error);
                }
            }
        }
        let Some((token_id, mut secret)) = generated else {
            return Err(TokenLifecycleError::TokenIdGenerationFailed);
        };
        let payload = match encode_new_verifier_record(
            token_id,
            user_id,
            &secret,
            issued_at_us,
            expires_at_us,
            label,
        ) {
            Ok(payload) => payload,
            Err(_) => {
                secret.zeroize();
                return Err(TokenLifecycleError::EncodingFailed);
            }
        };
        let mut target_ids = audit_event.target_ids;
        if target_ids.len() >= 256 {
            secret.zeroize();
            return Err(TokenLifecycleError::InvalidAudit);
        }
        target_ids.push(token_id);
        target_ids.sort_unstable();
        target_ids.dedup();
        audit_event.target_ids = target_ids;
        let write = CatalogWrite {
            key: credential_catalog_key(token_id),
            expected_value: None,
            payload: Some(payload),
        };
        let sequence = match self.commit_authorized_identity_ddl(
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &[write],
            &[audit_event],
        ) {
            Ok(sequence) => sequence,
            Err(error) => {
                secret.zeroize();
                return Err(TokenLifecycleError::Commit(error));
            }
        };
        let issued = IssuedToken {
            token_id,
            commit_sequence: sequence,
            secret: Some(secret),
        };
        secret.zeroize();
        Ok(issued)
    }

    /// Revoke one token through the authorized identity DDL path. Unknown,
    /// already revoked, and unauthorized token requests share
    /// `TokenUnavailable` so this method cannot be used to enumerate tokens.
    pub fn revoke_token(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        token_id: [u8; 16],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, TokenLifecycleError> {
        if authenticated_actor_id == [0; 16] || token_id == [0; 16] {
            return Err(TokenLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.action != AuditAction::TokenLifecycle
            || audit_event.result != AuditResult::Succeeded
            || !audit_event.target_ids.contains(&token_id)
        {
            return Err(TokenLifecycleError::InvalidAudit);
        }
        self.token_requests.consume(authenticated_actor_id)?;
        let key = credential_catalog_key(token_id);
        let read = self
            .database
            .begin_read()
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
        let table = read
            .open_table(CATALOG)
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
        let current_bytes = table
            .get(key.as_slice())
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?
            .map(|value| value.value().to_vec())
            .ok_or(TokenLifecycleError::TokenUnavailable)?;
        let current_record = decode_registered_envelope(&current_bytes)
            .map_err(|_| TokenLifecycleError::TokenUnavailable)?;
        if current_record.logical_key != key {
            return Err(TokenLifecycleError::TokenUnavailable);
        }
        let current = decode_verifier_record(&current_record)
            .map_err(|_| TokenLifecycleError::TokenUnavailable)?;
        if current.revoked {
            return Err(TokenLifecycleError::TokenUnavailable);
        }
        let Some(revoked_at_us) = now_unix_micros() else {
            return Err(TokenLifecycleError::InvalidRequest);
        };
        let payload = encode_revoked_verifier_record(&current_record, revoked_at_us)
            .map_err(|_| TokenLifecycleError::EncodingFailed)?;
        drop(table);
        drop(read);
        let mut target_ids = audit_event.target_ids;
        if target_ids.len() >= 256 {
            return Err(TokenLifecycleError::InvalidAudit);
        }
        target_ids.push(current.user_id);
        target_ids.sort_unstable();
        target_ids.dedup();
        audit_event.target_ids = target_ids;
        let write = CatalogWrite {
            key,
            expected_value: Some(current_bytes),
            payload: Some(payload),
        };
        self.commit_authorized_identity_ddl(
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &[write],
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => TokenLifecycleError::TokenUnavailable,
            error => TokenLifecycleError::Commit(error),
        })
    }

    /// List token metadata for the authenticated user, or for another user
    /// when the actor has database-admin membership or MANAGE_USERS. The read
    /// uses one catalog snapshot and never returns verifier material.
    pub fn list_tokens(
        &self,
        authenticated_actor_id: [u8; 16],
        user_id: [u8; 16],
    ) -> Result<Vec<TokenMetadata>, TokenLifecycleError> {
        if authenticated_actor_id == [0; 16] || user_id == [0; 16] {
            return Err(TokenLifecycleError::InvalidRequest);
        }
        self.token_requests.consume(authenticated_actor_id)?;
        let read = self
            .database
            .begin_read()
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
        let snapshot = RowSnapshot::new(read);
        let can_list = token_inventory_authority(&snapshot, authenticated_actor_id, user_id)?;
        if !can_list {
            return Err(TokenLifecycleError::TokenUnavailable);
        }
        let records = snapshot
            .scan_catalog_family(16, MAX_CATALOG_FAMILY_SCAN)
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
        let mut tokens = Vec::new();
        for record in records {
            let verifier = decode_verifier_record(&record)
                .map_err(|_| TokenLifecycleError::Storage("invalid token catalog record".into()))?;
            if verifier.user_id != user_id {
                continue;
            }
            let token_id = record
                .logical_key
                .get(3..19)
                .and_then(|bytes| bytes.try_into().ok())
                .ok_or_else(|| TokenLifecycleError::Storage("invalid token catalog key".into()))?;
            let label = String::from_utf8(verifier.label)
                .map_err(|_| TokenLifecycleError::Storage("invalid token label".into()))?;
            tokens.push(TokenMetadata {
                token_id,
                label,
                issued_at_us: verifier.issued_at_us,
                expires_at_us: verifier.expires_at_us,
                revoked: verifier.revoked,
                revoked_at_us: verifier.revoked_at_us,
            });
        }
        Ok(tokens)
    }

    /// Return at most 64 token records after an exclusive token ID cursor.
    /// The scan and authorization semantics match `list_tokens`.
    pub fn list_tokens_page(
        &self,
        authenticated_actor_id: [u8; 16],
        user_id: [u8; 16],
        after_token_id: Option<[u8; 16]>,
    ) -> Result<TokenMetadataPage, TokenLifecycleError> {
        const PAGE_SIZE: usize = 64;
        let mut tokens = self.list_tokens(authenticated_actor_id, user_id)?;
        if let Some(cursor) = after_token_id {
            tokens.retain(|token| token.token_id > cursor);
        }
        let has_more = tokens.len() > PAGE_SIZE;
        tokens.truncate(PAGE_SIZE);
        let next_after_token_id = has_more
            .then(|| tokens.last().map(|token| token.token_id))
            .flatten();
        Ok(TokenMetadataPage {
            tokens,
            next_after_token_id,
        })
    }

    /// Trusted coordinated catalog publication with no actor authorization.
    /// Use only from an internal caller that has already applied the relevant
    /// action-specific administrative checks.
    pub fn commit_catalog_and_audit(
        &self,
        invocation_id: [u8; 16],
        principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_catalog_and_audit_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    /// Authorize ordinary index DDL for a principal authenticated by the
    /// trusted administrative channel, then commit it under the shared DDL
    /// fences. Supports family-9 index definitions and matching family-2 index
    /// objects. Other catalog DDL remains on the trusted internal entry point
    /// until its action-specific authority rules are implemented.
    pub fn commit_authorized_index_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_authorized_index_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    /// Authorize user/role creation, rename, login-state changes, and
    /// `DROP ... RESTRICT` when no dependencies reference the identity. The
    /// actor is authenticated by the trusted administrative channel. User
    /// lifecycle changes require `MANAGE_USERS`, role changes require
    /// `MANAGE_ROLES`; a dropped user's active tokens are revoked in the same
    /// batch as its tombstone.
    pub fn commit_authorized_identity_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_authorized_identity_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    /// Authorize and commit row/cell policy DDL for an actor authenticated by
    /// the trusted administrative channel. The actor must own each affected
    /// table or hold inherited `MANAGE_POLICIES` on it.
    pub fn commit_authorized_policy_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_authorized_policy_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    /// Authorize and commit foreign-key additions/removals on existing
    /// tables. Creation requires source-table `ALTER` and target-table or
    /// target-column `REFERENCES`; removal requires source-table `ALTER`.
    pub fn commit_authorized_foreign_key_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_authorized_foreign_key_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    /// Authorize NOT NULL, PRIMARY KEY, UNIQUE, and CHECK constraint add/drop
    /// for an existing table. Key constraints atomically manage their owned
    /// unique backing indexes; additions validate stored rows before
    /// publication. The actor must own the table or hold inherited ALTER on
    /// the table/schema.
    pub fn commit_authorized_constraint_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_authorized_constraint_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    /// Authorize and commit role membership changes for an actor authenticated
    /// by the trusted administrative channel. Requires `MANAGE_ROLES`, except
    /// changes to the reserved database-administrator role, which require an
    /// existing administrator.
    pub fn commit_authorized_membership_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_authorized_membership_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    /// Authorize system privilege grant/revoke operations for an actor
    /// authenticated by the trusted administrative channel. Only current
    /// database administrators may use this route.
    pub fn commit_authorized_system_privilege_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_authorized_system_privilege_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    /// Authorize and commit object grant/revoke changes for an actor
    /// authenticated by the trusted administrative channel. The actor must
    /// own each target, hold `MANAGE_GRANTS` on it, or be a database admin.
    pub fn commit_authorized_object_grant_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_authorized_object_grant_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    /// Resolve a retained terminal outcome for an already authenticated owner.
    /// Unknown, mismatched, and expired records all return `None`.
    pub fn lookup_invocation_outcome(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        now_us: i64,
    ) -> Result<Option<InvocationOutcome>, Box<dyn std::error::Error>> {
        lookup_outcome(
            self.database,
            invocation_id,
            authenticated_principal_id,
            now_us,
        )
    }

    /// Return live state from this process registry or a retained durable
    /// outcome. Callers must authenticate the principal before this lookup;
    /// unknown IDs, expired outcomes, and other owners all return `None`.
    pub fn get_invocation_status(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        now_us: i64,
    ) -> Result<Option<InvocationStatusSnapshot>, InvocationStatusError> {
        if invocation_id == [0; 16] || authenticated_principal_id == [0; 16] || now_us <= 0 {
            return Err(InvocationStatusError::InvalidRequest);
        }
        if let Some(live) = self.admissions.status(invocation_id) {
            if live.principal_id != authenticated_principal_id {
                return Ok(None);
            }
            let status = match live.phase {
                LiveInvocationPhase::Active => InvocationStatus::Active {
                    started_at_us: live.started_at_us,
                    deadline_at_us: live.deadline_at_us,
                    cancellation_requested: live.cancellation_reason_code.is_some(),
                },
                LiveInvocationPhase::CancellationPending => InvocationStatus::Active {
                    started_at_us: live.started_at_us,
                    deadline_at_us: live.deadline_at_us,
                    cancellation_requested: false,
                },
                LiveInvocationPhase::Committing => InvocationStatus::Committing {
                    started_at_us: live.started_at_us,
                    deadline_at_us: live.deadline_at_us,
                    cancellation_requested: live.cancellation_reason_code.is_some(),
                },
            };
            return Ok(Some(InvocationStatusSnapshot {
                observed_at_us: now_us,
                consistency: InvocationStatusConsistency::BestEffort,
                status,
            }));
        }
        lookup_outcome(
            self.database,
            invocation_id,
            authenticated_principal_id,
            now_us,
        )
        .map_err(|_| InvocationStatusError::Storage)
        .map(|outcome| {
            outcome.map(|outcome| InvocationStatusSnapshot {
                observed_at_us: now_us,
                consistency: InvocationStatusConsistency::BestEffort,
                status: InvocationStatus::Terminal(outcome),
            })
        })
    }

    /// Return one invocation's live or retained terminal status to a current
    /// database administrator. Authorization is checked on a fresh catalog
    /// snapshot before the invocation ID is inspected.
    pub fn get_administrative_invocation_status(
        &self,
        authenticated_administrator_id: [u8; 16],
        invocation_id: [u8; 16],
        now_us: i64,
    ) -> Result<Option<InvocationStatusSnapshot>, AdministrativeInvocationListError> {
        if authenticated_administrator_id == [0; 16] || invocation_id == [0; 16] || now_us <= 0 {
            return Err(AdministrativeInvocationListError::InvalidRequest);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| AdministrativeInvocationListError::Storage)?;
        let snapshot = RowSnapshot::new(read);
        let is_administrator = database_admin_authority(&snapshot, authenticated_administrator_id)
            .map_err(|error| match error {
                TokenLifecycleError::TokenUnavailable => {
                    AdministrativeInvocationListError::Unauthorized
                }
                _ => AdministrativeInvocationListError::Storage,
            })?;
        drop(snapshot);
        if !is_administrator {
            return Err(AdministrativeInvocationListError::Unauthorized);
        }

        if let Some(live) = self.admissions.status(invocation_id) {
            let status = match live.phase {
                LiveInvocationPhase::Active | LiveInvocationPhase::CancellationPending => {
                    InvocationStatus::Active {
                        started_at_us: live.started_at_us,
                        deadline_at_us: live.deadline_at_us,
                        cancellation_requested: live.cancellation_reason_code.is_some(),
                    }
                }
                LiveInvocationPhase::Committing => InvocationStatus::Committing {
                    started_at_us: live.started_at_us,
                    deadline_at_us: live.deadline_at_us,
                    cancellation_requested: live.cancellation_reason_code.is_some(),
                },
            };
            return Ok(Some(InvocationStatusSnapshot {
                observed_at_us: now_us,
                consistency: InvocationStatusConsistency::BestEffort,
                status,
            }));
        }

        let read = self
            .database
            .begin_read()
            .map_err(|_| AdministrativeInvocationListError::Storage)?;
        let bytes = read
            .open_table(OUTCOMES)
            .map_err(|_| AdministrativeInvocationListError::Storage)?
            .get(&invocation_id[..])
            .map_err(|_| AdministrativeInvocationListError::Storage)?
            .map(|guard| guard.value().to_vec());
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let outcome = InvocationOutcome::decode(&bytes)
            .map_err(|_| AdministrativeInvocationListError::Storage)?;
        if outcome.invocation_id != invocation_id {
            return Err(AdministrativeInvocationListError::Storage);
        }
        if now_us >= outcome.retention_deadline_us {
            return Ok(None);
        }
        Ok(Some(InvocationStatusSnapshot {
            observed_at_us: now_us,
            consistency: InvocationStatusConsistency::BestEffort,
            status: InvocationStatus::Terminal(outcome),
        }))
    }

    /// Request cancellation for an invocation owned by the authenticated
    /// principal. The commit transition and cancellation request are
    /// serialized by the admission registry; unknown and foreign IDs match.
    pub fn cancel_owned_invocation(
        &self,
        authenticated_principal_id: [u8; 16],
        invocation_id: [u8; 16],
        now_us: i64,
    ) -> Result<OwnerCancellationResult, InvocationStatusError> {
        if authenticated_principal_id == [0; 16] || invocation_id == [0; 16] || now_us <= 0 {
            return Err(InvocationStatusError::InvalidRequest);
        }
        loop {
            let mut live = self
                .admissions
                .live
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let invocation = live.entries.get(&invocation_id).copied();
            match invocation {
                Some(invocation) if invocation.principal_id != authenticated_principal_id => {
                    return Ok(OwnerCancellationResult::Unknown);
                }
                Some(invocation) if invocation.phase == LiveInvocationPhase::Committing => {
                    return Ok(OwnerCancellationResult::TooLate);
                }
                Some(invocation)
                    if invocation.phase == LiveInvocationPhase::CancellationPending =>
                {
                    live = self
                        .admissions
                        .changed
                        .wait(live)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    drop(live);
                }
                Some(invocation) if invocation.cancellation_reason_code.is_some() => {
                    return Ok(OwnerCancellationResult::Accepted);
                }
                Some(invocation) if invocation.phase == LiveInvocationPhase::Active => {
                    if let Some(entry) = live.entries.get_mut(&invocation_id) {
                        entry.phase = LiveInvocationPhase::CancellationPending;
                    }
                    drop(live);
                    let mut live = self
                        .admissions
                        .live
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let Some(entry) = live.entries.get_mut(&invocation_id) else {
                        self.admissions.changed.notify_all();
                        drop(live);
                        continue;
                    };
                    if entry.phase == LiveInvocationPhase::CancellationPending {
                        entry.phase = LiveInvocationPhase::Active;
                        entry.cancellation_reason_code = Some(1);
                        live.advance_owner_generation(authenticated_principal_id);
                    }
                    self.admissions.changed.notify_all();
                    return Ok(OwnerCancellationResult::Accepted);
                }
                None => {
                    drop(live);
                    return match lookup_outcome(
                        self.database,
                        invocation_id,
                        authenticated_principal_id,
                        now_us,
                    )
                    .map_err(|_| InvocationStatusError::Storage)?
                    {
                        Some(outcome) => Ok(OwnerCancellationResult::Terminal(outcome)),
                        None => Ok(OwnerCancellationResult::Unknown),
                    };
                }
                Some(_) => return Ok(OwnerCancellationResult::TooLate),
            }
        }
    }

    /// Return a bounded best-effort page of this authenticated owner's live
    /// invocations. Terminal outcomes remain available through point status.
    /// A change to this owner's entries invalidates an outstanding
    /// continuation so pages never silently skip or repeat this owner's rows.
    pub fn list_own_invocations(
        &self,
        authenticated_principal_id: [u8; 16],
        now_us: i64,
        page_limit: usize,
        continuation: Option<&InvocationListCursor>,
    ) -> Result<InvocationStatusPage, InvocationStatusError> {
        if authenticated_principal_id == [0; 16] || now_us <= 0 {
            return Err(InvocationStatusError::InvalidRequest);
        }
        if page_limit == 0 || page_limit > MAX_OWN_INVOCATION_PAGE {
            return Err(InvocationStatusError::InvalidPageLimit);
        }
        if continuation.is_some_and(|cursor| {
            cursor.principal_id == [0; 16] || cursor.after_invocation_id == [0; 16]
        }) {
            return Err(InvocationStatusError::InvalidCursor);
        }
        let (invocations, continuation) =
            self.admissions
                .own_page(authenticated_principal_id, page_limit, continuation)?;
        Ok(InvocationStatusPage {
            observed_at_us: now_us,
            consistency: InvocationStatusConsistency::BestEffort,
            invocations,
            continuation,
        })
    }

    /// Return a bounded best-effort page of live invocations for a current
    /// database administrator. Authority is revalidated from the latest
    /// catalog snapshot on every call; no global total is returned.
    pub fn list_runtime_invocations(
        &self,
        authenticated_administrator_id: [u8; 16],
        now_us: i64,
        page_limit: usize,
        continuation: Option<&AdministrativeInvocationCursor>,
    ) -> Result<AdministrativeInvocationPage, AdministrativeInvocationListError> {
        if authenticated_administrator_id == [0; 16] || now_us <= 0 {
            return Err(AdministrativeInvocationListError::InvalidRequest);
        }
        if page_limit == 0 || page_limit > MAX_ADMIN_INVOCATION_PAGE {
            return Err(AdministrativeInvocationListError::InvalidPageLimit);
        }
        if continuation.is_some_and(|cursor| {
            cursor.administrator_id == [0; 16] || cursor.after_invocation_id == [0; 16]
        }) {
            return Err(AdministrativeInvocationListError::InvalidCursor);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| AdministrativeInvocationListError::Storage)?;
        let snapshot = RowSnapshot::new(read);
        let is_administrator = database_admin_authority(&snapshot, authenticated_administrator_id)
            .map_err(|error| match error {
                TokenLifecycleError::TokenUnavailable => {
                    AdministrativeInvocationListError::Unauthorized
                }
                _ => AdministrativeInvocationListError::Storage,
            })?;
        drop(snapshot);
        if !is_administrator {
            return Err(AdministrativeInvocationListError::Unauthorized);
        }
        let (invocations, continuation) = self.admissions.administrative_page(
            authenticated_administrator_id,
            page_limit,
            continuation,
        )?;
        Ok(AdministrativeInvocationPage {
            observed_at_us: now_us,
            consistency: InvocationStatusConsistency::BestEffort,
            invocations,
            continuation,
        })
    }

    /// Request audited cancellation for a live invocation. A short-lived
    /// pending state makes commit wait for the durable audit decision without
    /// holding the registry lock during redb I/O.
    pub fn cancel_invocation(
        &self,
        authenticated_administrator_id: [u8; 16],
        invocation_id: [u8; 16],
        reason_code: u16,
        now_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<AdministrativeCancellationResult, AdministrativeCancellationError> {
        if authenticated_administrator_id == [0; 16]
            || invocation_id == [0; 16]
            || reason_code == 0
            || now_us <= 0
        {
            return Err(AdministrativeCancellationError::InvalidRequest);
        }
        self.admin_cancellation_requests
            .consume(authenticated_administrator_id)
            .map_err(|_| AdministrativeCancellationError::RateLimited)?;
        if audit_event.actor_id != Some(authenticated_administrator_id)
            || audit_event.action != AuditAction::AdministrativeCancellation
            || audit_event.target_ids.as_slice() != [invocation_id]
        {
            return Err(AdministrativeCancellationError::InvalidAudit);
        }

        loop {
            let mut live = self
                .admissions
                .live
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let invocation = live.entries.get(&invocation_id).copied();
            match invocation {
                Some(invocation)
                    if invocation.phase == LiveInvocationPhase::CancellationPending =>
                {
                    live = self
                        .admissions
                        .changed
                        .wait(live)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    drop(live);
                }
                Some(invocation) if invocation.phase == LiveInvocationPhase::Committing => {
                    drop(live);
                    audit_event.result = AuditResult::Denied;
                    persist_admin_cancellation_audit(
                        self.database,
                        authenticated_administrator_id,
                        &audit_event,
                    )?;
                    return Ok(AdministrativeCancellationResult::TooLate);
                }
                Some(invocation) if invocation.cancellation_reason_code.is_some() => {
                    drop(live);
                    audit_event.result = AuditResult::Accepted;
                    persist_admin_cancellation_audit(
                        self.database,
                        authenticated_administrator_id,
                        &audit_event,
                    )?;
                    return Ok(AdministrativeCancellationResult::Accepted);
                }
                Some(invocation) if invocation.phase == LiveInvocationPhase::Active => {
                    if let Some(entry) = live.entries.get_mut(&invocation_id) {
                        entry.phase = LiveInvocationPhase::CancellationPending;
                    }
                    drop(live);

                    audit_event.result = AuditResult::Accepted;
                    let audit_result = persist_admin_cancellation_audit(
                        self.database,
                        authenticated_administrator_id,
                        &audit_event,
                    );

                    let mut live = self
                        .admissions
                        .live
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let mut changed = false;
                    if let Some(entry) = live.entries.get_mut(&invocation_id)
                        && entry.phase == LiveInvocationPhase::CancellationPending
                    {
                        entry.phase = LiveInvocationPhase::Active;
                        if audit_result.is_ok() {
                            entry.cancellation_reason_code = Some(reason_code);
                            changed = true;
                        }
                    }
                    if changed {
                        live.advance_owner_generation(invocation.principal_id);
                    }
                    self.admissions.changed.notify_all();
                    audit_result?;
                    return Ok(AdministrativeCancellationResult::Accepted);
                }
                None => {
                    drop(live);
                    audit_event.result = AuditResult::Denied;
                    persist_admin_cancellation_audit(
                        self.database,
                        authenticated_administrator_id,
                        &audit_event,
                    )?;
                    return match lookup_any_outcome(self.database, invocation_id, now_us)? {
                        Some(outcome) => Ok(AdministrativeCancellationResult::Terminal(outcome)),
                        None => Ok(AdministrativeCancellationResult::Unknown),
                    };
                }
                Some(_) => return Ok(AdministrativeCancellationResult::TooLate),
            }
        }
    }
}

fn generate_query_connection_id() -> Result<[u8; 16], TransactionError> {
    for _ in 0..8 {
        let mut connection_id = [0u8; 16];
        getrandom::fill(&mut connection_id).map_err(|_| TransactionError::RandomnessUnavailable)?;
        if connection_id != [0; 16] {
            return Ok(connection_id);
        }
    }
    Err(TransactionError::RandomnessUnavailable)
}

fn database_admin_authority(
    snapshot: &RowSnapshot,
    actor_id: [u8; 16],
) -> Result<bool, TokenLifecycleError> {
    let metadata_bytes = snapshot
        .get_engine_metadata_value()
        .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?
        .ok_or_else(|| TokenLifecycleError::Storage("engine metadata is missing".into()))?;
    let metadata = EngineMetadata::decode(&metadata_bytes)
        .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
    if metadata.lifecycle != Lifecycle::Ready || !metadata.supports_current_formats() {
        return Err(TokenLifecycleError::Storage(
            "database is not ready for runtime listing".into(),
        ));
    }
    let roles = token_actor_roles(snapshot, actor_id)?;
    let mut database_admin_role = [0x80; 16];
    database_admin_role[15] = 3;
    Ok(roles.contains(&database_admin_role))
}

fn persist_admin_cancellation_audit(
    database: &Database,
    administrator_id: [u8; 16],
    event: &AuditEvent,
) -> Result<(), AdministrativeCancellationError> {
    if event.actor_id != Some(administrator_id)
        || event.action != AuditAction::AdministrativeCancellation
        || !matches!(event.result, AuditResult::Accepted | AuditResult::Denied)
    {
        return Err(AdministrativeCancellationError::InvalidAudit);
    }
    let encoded = event
        .encode()
        .map_err(|_| AdministrativeCancellationError::InvalidAudit)?;

    let read = database
        .begin_read()
        .map_err(|_| AdministrativeCancellationError::Storage)?;
    let snapshot = RowSnapshot::new(read);
    let is_administrator =
        database_admin_authority(&snapshot, administrator_id).map_err(|error| match error {
            TokenLifecycleError::TokenUnavailable => AdministrativeCancellationError::Unauthorized,
            _ => AdministrativeCancellationError::Storage,
        })?;
    if !is_administrator {
        return Err(AdministrativeCancellationError::Unauthorized);
    }

    let membership_records = snapshot
        .scan_catalog_family(11, MAX_CATALOG_FAMILY_SCAN)
        .map_err(|_| AdministrativeCancellationError::Storage)?;
    let mut guarded_values = HashMap::<Vec<u8>, Vec<u8>>::new();
    let mut principal_ids = HashSet::from([administrator_id]);
    for record in membership_records {
        let key = record.logical_key;
        if key.len() != 36 || key[2] != 1 || key[19] != 1 {
            return Err(AdministrativeCancellationError::Storage);
        }
        let member_id: [u8; 16] = key[3..19]
            .try_into()
            .map_err(|_| AdministrativeCancellationError::Storage)?;
        let role_id: [u8; 16] = key[20..36]
            .try_into()
            .map_err(|_| AdministrativeCancellationError::Storage)?;
        principal_ids.insert(member_id);
        principal_ids.insert(role_id);
        let value = snapshot
            .get_catalog_value(&key)
            .map_err(|_| AdministrativeCancellationError::Storage)?
            .ok_or(AdministrativeCancellationError::Storage)?;
        guarded_values.insert(key, value);
    }
    for principal_id in principal_ids {
        let key = principal_catalog_key(principal_id);
        let value = snapshot
            .get_catalog_value(&key)
            .map_err(|_| AdministrativeCancellationError::Storage)?
            .ok_or(AdministrativeCancellationError::Storage)?;
        guarded_values.insert(key, value);
    }
    drop(snapshot);

    let mut write = database
        .begin_write()
        .map_err(|_| AdministrativeCancellationError::Storage)?;
    write
        .set_durability(redb::Durability::Immediate)
        .map_err(|_| AdministrativeCancellationError::Storage)?;
    write.set_two_phase_commit(true);
    {
        let catalog = write
            .open_table(CATALOG)
            .map_err(|_| AdministrativeCancellationError::Storage)?;
        for (key, expected) in guarded_values {
            let current = catalog
                .get(key.as_slice())
                .map_err(|_| AdministrativeCancellationError::Storage)?
                .map(|value| value.value().to_vec());
            if current.as_deref() != Some(expected.as_slice()) {
                return Err(AdministrativeCancellationError::Unauthorized);
            }
        }
    }
    {
        let mut audit = write
            .open_table(AUDIT)
            .map_err(|_| AdministrativeCancellationError::Storage)?;
        if audit
            .get(event.event_id.as_slice())
            .map_err(|_| AdministrativeCancellationError::Storage)?
            .is_some()
        {
            return Err(AdministrativeCancellationError::InvalidAudit);
        }
        audit
            .insert(event.event_id.as_slice(), encoded.as_slice())
            .map_err(|_| AdministrativeCancellationError::Storage)?;
    }
    write
        .commit()
        .map_err(|_| AdministrativeCancellationError::Storage)
}

fn lookup_any_outcome(
    database: &Database,
    invocation_id: [u8; 16],
    now_us: i64,
) -> Result<Option<InvocationOutcome>, AdministrativeCancellationError> {
    let read = database
        .begin_read()
        .map_err(|_| AdministrativeCancellationError::Storage)?;
    let outcomes = read
        .open_table(OUTCOMES)
        .map_err(|_| AdministrativeCancellationError::Storage)?;
    let Some(value) = outcomes
        .get(invocation_id.as_slice())
        .map_err(|_| AdministrativeCancellationError::Storage)?
    else {
        return Ok(None);
    };
    let outcome = InvocationOutcome::decode(value.value())
        .map_err(|_| AdministrativeCancellationError::Storage)?;
    if outcome.invocation_id != invocation_id {
        return Err(AdministrativeCancellationError::Storage);
    }
    Ok((now_us < outcome.retention_deadline_us).then_some(outcome))
}

fn token_actor_roles(
    snapshot: &RowSnapshot,
    actor_id: [u8; 16],
) -> Result<HashSet<[u8; 16]>, TokenLifecycleError> {
    let actor = snapshot
        .get_catalog_record(&principal_catalog_key(actor_id))
        .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?
        .ok_or(TokenLifecycleError::TokenUnavailable)?;
    if token_catalog_u16(&actor, 1)? != 1
        || token_catalog_field(&actor, 3)? != [1]
        || token_catalog_u64(&actor, 4)? == 0
    {
        return Err(TokenLifecycleError::TokenUnavailable);
    }

    let membership_records = snapshot
        .scan_catalog_family(11, MAX_CATALOG_FAMILY_SCAN)
        .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
    if membership_records.len() > MAX_MEMBERSHIP_EDGES {
        return Err(TokenLifecycleError::Storage(
            "membership graph exceeds its configured limit".into(),
        ));
    }
    let mut memberships = Vec::with_capacity(membership_records.len());
    let mut principal_ids = HashSet::from([actor_id]);
    for record in membership_records {
        let key = &record.logical_key;
        if key.len() != 36 || key[2] != 1 || key[19] != 1 {
            return Err(TokenLifecycleError::Storage(
                "invalid membership catalog key".into(),
            ));
        }
        if token_catalog_id(&record, 1)? == [0; 16] || token_catalog_u64(&record, 2)? == 0 {
            return Err(TokenLifecycleError::Storage(
                "invalid membership catalog record".into(),
            ));
        }
        let member_id: [u8; 16] = key[3..19]
            .try_into()
            .map_err(|_| TokenLifecycleError::Storage("invalid membership key".into()))?;
        let role_id: [u8; 16] = key[20..36]
            .try_into()
            .map_err(|_| TokenLifecycleError::Storage("invalid membership key".into()))?;
        memberships.push(Membership { member_id, role_id });
        principal_ids.insert(member_id);
        principal_ids.insert(role_id);
    }
    for principal_id in &principal_ids {
        let record = snapshot
            .get_catalog_record(&principal_catalog_key(*principal_id))
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?
            .ok_or_else(|| {
                TokenLifecycleError::Storage("membership references a missing principal".into())
            })?;
        let kind = token_catalog_u16(&record, 1)?;
        let login = token_catalog_field(&record, 3)?;
        if !matches!(kind, 1 | 2)
            || !matches!(login, [0] | [1])
            || token_catalog_u64(&record, 4)? == 0
            || (kind == 2 && login != [0])
            || (memberships.iter().any(|edge| edge.role_id == *principal_id) && kind != 2)
        {
            return Err(TokenLifecycleError::Storage(
                "invalid membership principal".into(),
            ));
        }
    }
    let roles = resolve_role_closure(actor_id, &memberships)
        .map_err(|_| TokenLifecycleError::Storage("invalid membership graph".into()))?;
    Ok(roles)
}

fn token_inventory_authority(
    snapshot: &RowSnapshot,
    actor_id: [u8; 16],
    user_id: [u8; 16],
) -> Result<bool, TokenLifecycleError> {
    let metadata_bytes = snapshot
        .get_engine_metadata_value()
        .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?
        .ok_or_else(|| TokenLifecycleError::Storage("engine metadata is missing".into()))?;
    let metadata = EngineMetadata::decode(&metadata_bytes)
        .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
    if metadata.lifecycle != Lifecycle::Ready || !metadata.supports_current_formats() {
        return Err(TokenLifecycleError::Storage(
            "database is not ready for token inventory".into(),
        ));
    }

    let roles = token_actor_roles(snapshot, actor_id)?;
    let mut authority_subjects = roles.clone();
    authority_subjects.insert(actor_id);
    let mut database_admin_role = [0x80; 16];
    database_admin_role[15] = 3;
    let is_database_admin = roles.contains(&database_admin_role);

    if actor_id == user_id {
        return Ok(true);
    }
    if is_database_admin
        || token_inventory_has_manage_users(snapshot, metadata.database_id, &authority_subjects)?
    {
        let target = snapshot
            .get_catalog_record(&principal_catalog_key(user_id))
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
        return match target {
            Some(record) if token_catalog_u16(&record, 1)? == 1 => Ok(true),
            _ => Ok(false),
        };
    }
    Ok(false)
}

fn token_inventory_has_manage_users(
    snapshot: &RowSnapshot,
    database_id: [u8; 16],
    authority_subjects: &HashSet<[u8; 16]>,
) -> Result<bool, TokenLifecycleError> {
    for subject_id in authority_subjects {
        let key = system_privilege_catalog_key(*subject_id, database_id, 12);
        let Some(record) = snapshot
            .get_catalog_record(&key)
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?
        else {
            continue;
        };
        if token_catalog_id(&record, 1)? == [0; 16] || token_catalog_u64(&record, 2)? == 0 {
            return Err(TokenLifecycleError::Storage("invalid system grant".into()));
        }
        return Ok(true);
    }
    Ok(false)
}

fn principal_catalog_key(principal_id: [u8; 16]) -> Vec<u8> {
    let mut key = 10u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&principal_id);
    key
}

fn identity_object_catalog_key(object_id: [u8; 16]) -> Vec<u8> {
    let mut key = 2u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&object_id);
    key
}

fn identity_tombstone_catalog_key(object_id: [u8; 16]) -> Vec<u8> {
    let mut key = 17u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&object_id);
    key
}

fn membership_catalog_key(member_id: [u8; 16], role_id: [u8; 16]) -> Vec<u8> {
    let mut key = 11u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&member_id);
    key.push(1);
    key.extend_from_slice(&role_id);
    key
}

fn dependency_catalog_key(
    dependent_id: [u8; 16],
    referenced_id: [u8; 16],
    dependency_kind: u16,
) -> Vec<u8> {
    let mut key = 15u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&dependent_id);
    key.push(1);
    key.extend_from_slice(&referenced_id);
    key.push(1);
    key.extend_from_slice(&dependency_kind.to_be_bytes());
    key
}

fn is_normalized_principal_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 128
        && (bytes[0].is_ascii_lowercase() || bytes[0] == b'_')
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
}

fn system_privilege_catalog_key(
    grantee_id: [u8; 16],
    database_id: [u8; 16],
    privilege: u16,
) -> Vec<u8> {
    let mut key = 12u16.to_be_bytes().to_vec();
    key.extend_from_slice(&[1]);
    key.extend_from_slice(&grantee_id);
    key.extend_from_slice(&[1]);
    key.extend_from_slice(&database_id);
    key.extend_from_slice(&[1]);
    key.extend_from_slice(&privilege.to_be_bytes());
    key.extend_from_slice(&[1]);
    key.extend_from_slice(&1u16.to_be_bytes());
    key.extend_from_slice(&[1]);
    key.extend_from_slice(&database_id);
    key
}

fn token_catalog_field(record: &CatalogRecord, tag: u16) -> Result<&[u8], TokenLifecycleError> {
    record
        .fields
        .iter()
        .find(|field| field.tag == tag)
        .map(|field| field.payload.as_slice())
        .ok_or_else(|| TokenLifecycleError::Storage("invalid catalog payload".into()))
}

fn token_catalog_id(record: &CatalogRecord, tag: u16) -> Result<[u8; 16], TokenLifecycleError> {
    token_catalog_field(record, tag)?
        .try_into()
        .map_err(|_| TokenLifecycleError::Storage("invalid catalog ID".into()))
}

fn token_catalog_u16(record: &CatalogRecord, tag: u16) -> Result<u16, TokenLifecycleError> {
    token_catalog_field(record, tag)?
        .try_into()
        .map(u16::from_le_bytes)
        .map_err(|_| TokenLifecycleError::Storage("invalid catalog integer".into()))
}

fn token_catalog_u64(record: &CatalogRecord, tag: u16) -> Result<u64, TokenLifecycleError> {
    token_catalog_field(record, tag)?
        .try_into()
        .map(u64::from_le_bytes)
        .map_err(|_| TokenLifecycleError::Storage("invalid catalog integer".into()))
}

fn credential_catalog_key(token_id: [u8; 16]) -> Vec<u8> {
    let mut key = 16u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&token_id);
    key
}

fn credential_token_exists(
    database: &Database,
    token_id: [u8; 16],
) -> Result<bool, TokenLifecycleError> {
    let read = database
        .begin_read()
        .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
    let table = read
        .open_table(CATALOG)
        .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
    table
        .get(credential_catalog_key(token_id).as_slice())
        .map(|value| value.is_some())
        .map_err(|error| TokenLifecycleError::Storage(error.to_string()))
}

fn lookup_credential_principal(
    database: &Database,
    token_id: [u8; 16],
) -> Result<[u8; 16], TransactionError> {
    if token_id == [0; 16] {
        return Err(TransactionError::AuthenticationFailed);
    }
    let read = database
        .begin_read()
        .map_err(|error| TransactionError::Storage(error.to_string()))?;
    let table = read
        .open_table(CATALOG)
        .map_err(|error| TransactionError::Storage(error.to_string()))?;
    let key = credential_catalog_key(token_id);
    let bytes = table
        .get(key.as_slice())
        .map_err(|error| TransactionError::Storage(error.to_string()))?
        .map(|value| value.value().to_vec())
        .ok_or(TransactionError::AuthenticationFailed)?;
    let record =
        decode_registered_envelope(&bytes).map_err(|_| TransactionError::AuthenticationFailed)?;
    if record.logical_key != key {
        return Err(TransactionError::AuthenticationFailed);
    }
    let credential =
        decode_verifier_record(&record).map_err(|_| TransactionError::AuthenticationFailed)?;
    Ok(credential.user_id)
}
