//! Coordinated entry points for invocation commits and administrative DDL.

use crate::audit::{AuditAction, AuditEvent, AuditResult};
use crate::authorization::{MAX_MEMBERSHIP_EDGES, Membership, resolve_role_closure};
use crate::catalog::{CatalogRecord, decode_registered_envelope};
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
use crate::schema::{AUDIT, CATALOG, OUTCOMES};
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
}

#[derive(Debug)]
pub enum SupervisorInvocationError {
    Closed,
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

    pub fn serve_planner<S: std::io::Read + std::io::Write>(
        &self,
        stream: &mut S,
    ) -> Result<(), SupervisorInvocationError> {
        crate::planner_ipc::serve_planner_shared(stream, &self.transaction)
            .map_err(SupervisorInvocationError::Planner)
    }

    pub fn serve_executor<S: std::io::Read + std::io::Write>(
        &self,
        stream: &mut S,
    ) -> Result<(), SupervisorInvocationError> {
        crate::executor_ipc::serve_executor_shared(stream, &self.transaction)
            .map_err(SupervisorInvocationError::Executor)
    }

    pub fn serve_terminal<S: std::io::Read + std::io::Write>(
        &mut self,
        stream: &mut S,
    ) -> Result<(), SupervisorInvocationError> {
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
            transaction.abort()?;
            return Err(TransactionError::AuthenticationFailed);
        };
        let roles = transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        Ok((transaction, roles))
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
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let (transaction, role_ids) = self.begin_token_authenticated_invocation(
            invocation_id,
            token_id,
            token_secret,
            retention_deadline_us,
            max_rows,
            max_bytes,
        )?;
        Ok(SupervisorInvocation {
            invocation_id,
            principal_id,
            role_ids,
            transaction: Arc::new(Mutex::new(Some(transaction))),
        })
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
