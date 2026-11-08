//! Coordinated entry points for invocation commits and administrative DDL.

use crate::audit::{AuditAction, AuditEvent, AuditResult};
use crate::authorization::{MAX_MEMBERSHIP_EDGES, Membership, resolve_role_closure};
use crate::catalog::{
    CatalogField, CatalogRecord, WireType, decode_builtin_scalar_type, decode_policy_target,
    decode_registered_envelope, encode_envelope, family_schema,
};
use crate::commit::{
    CatalogWrite, CommitError, commit_authorized_column_drop_ddl_with_fences,
    commit_authorized_column_rename_ddl_with_fences, commit_authorized_constraint_ddl_with_fences,
    commit_authorized_foreign_key_ddl_with_fences, commit_authorized_identity_ddl_with_fences,
    commit_authorized_index_ddl_with_fences, commit_authorized_membership_ddl_with_fences,
    commit_authorized_object_grant_ddl_with_fences, commit_authorized_policy_ddl_with_fences,
    commit_authorized_schema_create_ddl_with_fences, commit_authorized_schema_drop_ddl_with_fences,
    commit_authorized_schema_owner_ddl_with_fences,
    commit_authorized_schema_rename_ddl_with_fences,
    commit_authorized_system_privilege_ddl_with_fences,
    commit_authorized_table_create_ddl_with_fences, commit_authorized_table_drop_ddl_with_fences,
    commit_authorized_table_owner_ddl_with_fences, commit_authorized_table_rename_ddl_with_fences,
    commit_catalog_and_audit_with_fences,
};
use crate::credentials::{
    MAX_TOKEN_LABEL_BYTES, TOKEN_SECRET_BYTES, decode_verifier_record, encode_new_verifier_record,
    encode_revoked_verifier_record, now_unix_micros,
};
use crate::ddl_fence::{DdlFenceCoordinator, DdlFenceManager};
use crate::metadata::{EngineMetadata, Lifecycle};
use crate::metadata_discovery::{
    InformationSchemaColumnsCursor, InformationSchemaColumnsScanPage,
    InformationSchemaSchemataPage, InformationSchemaTablesCursor, InformationSchemaTablesScanPage,
};
use crate::outcome::{InvocationOutcome, lookup_outcome};
use crate::schema::{AUDIT, CATALOG, ENGINE_META, OUTCOMES};
use crate::snapshot::{MAX_CATALOG_FAMILY_SCAN, RowSnapshot};
use crate::startup::{StartupError, validate_persisted_records};
use crate::transaction::{InvocationCoordination, InvocationTransaction, TransactionError};
use crate::tuple::{ColumnSchema, ScalarType, encode_tuple};
use redb::{Database, ReadableDatabase, ReadableTable};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};
use zeroize::Zeroize;

const TOKEN_REQUEST_WINDOW: Duration = Duration::from_secs(60);
const MAX_TRACKED_TOKEN_REQUEST_ACTORS: usize = 4096;
const SUPERVISOR_IDLE_POLL_INTERVAL: Duration = Duration::from_millis(100);
pub const MAX_OWN_INVOCATION_PAGE: usize = 128;
pub const MAX_ADMIN_INVOCATION_PAGE: usize = 128;
/// Maximum number of schema records returned by one authenticated discovery
/// snapshot. Clients can request a fresh snapshot when this bound is reached.
pub const MAX_DISCOVERED_SCHEMAS: usize = 4096;
const MAX_DISCOVERED_SCHEMA_BYTES: usize = 1024 * 1024;
pub const MAX_DISCOVERED_RELATIONS: usize = 4096;
const MAX_DISCOVERED_RELATION_BYTES: usize = 1024 * 1024;
pub const MAX_DISCOVERED_COLUMNS: usize = 4096;
const MAX_DISCOVERED_COLUMN_BYTES: usize = 1024 * 1024;
static TOKEN_REQUEST_LIMITERS: OnceLock<Mutex<HashMap<usize, Weak<TokenRequestLimiter>>>> =
    OnceLock::new();
static ADMIN_CANCELLATION_LIMITERS: OnceLock<Mutex<HashMap<usize, Weak<TokenRequestLimiter>>>> =
    OnceLock::new();
static INVOCATION_REGISTRIES: OnceLock<Mutex<HashMap<usize, Weak<InvocationAdmissionState>>>> =
    OnceLock::new();
static UNCERTAIN_INVOCATION_REGISTRIES: OnceLock<
    Mutex<HashMap<usize, Arc<InvocationAdmissionState>>>,
> = OnceLock::new();

/// Admission and coordination limits for one storage-manager handle.
///
/// Defaults allow at most 128 simultaneous invocation snapshots, a 15-minute
/// lifetime, and 65,536 row-scan candidates per managed invocation, plus 30
/// token-lifecycle and administrative cancellation requests per actor per
/// rolling minute.
#[derive(Clone, Copy, Debug)]
pub struct StorageManagerConfig {
    pub ddl_fence_timeout: Duration,
    pub max_active_invocations: usize,
    pub max_invocation_age: Duration,
    /// Maximum catalog records and row candidates scanned in one invocation.
    pub max_invocation_scan_work: usize,
    pub max_token_lifecycle_requests_per_minute: usize,
    pub max_admin_cancellation_requests_per_minute: usize,
}

impl Default for StorageManagerConfig {
    fn default() -> Self {
        Self {
            ddl_fence_timeout: Duration::from_secs(5),
            max_active_invocations: 128,
            max_invocation_age: Duration::from_secs(15 * 60),
            max_invocation_scan_work: crate::transaction::MAX_INVOCATION_SCAN_WORK,
            max_token_lifecycle_requests_per_minute: 30,
            max_admin_cancellation_requests_per_minute: 30,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageManagerConfigError {
    ZeroActiveInvocationLimit,
    ZeroInvocationAge,
    ZeroInvocationScanWorkLimit,
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
pub enum SchemaLifecycleError {
    InvalidRequest,
    InvalidAudit,
    RandomnessUnavailable,
    SchemaIdGenerationFailed,
    Unauthorized,
    Storage,
    Commit(CommitError),
}

impl std::fmt::Display for SchemaLifecycleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "schema lifecycle operation failed: {self:?}")
    }
}

impl std::error::Error for SchemaLifecycleError {}

fn schema_lifecycle_from_table_error(error: TableLifecycleError) -> SchemaLifecycleError {
    match error {
        TableLifecycleError::Commit(error) => SchemaLifecycleError::Commit(error),
        TableLifecycleError::Storage => SchemaLifecycleError::Storage,
        _ => SchemaLifecycleError::Commit(CommitError::InvalidCatalog),
    }
}

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedSchema {
    pub schema_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DroppedSchema {
    pub schema_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DroppedTable {
    pub table_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DroppedColumn {
    pub table_id: [u8; 16],
    pub column_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableColumnDefinition {
    pub name: String,
    pub scalar_type: ScalarType,
    pub nullable: bool,
    pub generated_uuid_identity: bool,
    pub default: Option<TableColumnDefault>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TableColumnDefault {
    Literal(Option<crate::tuple::ScalarValue>),
    CurrentPrincipalId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTableRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub schema_id: [u8; 16],
    pub name: String,
    pub columns: Vec<TableColumnDefinition>,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateForeignKeyRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub source_table_id: [u8; 16],
    pub source_column_ids: Vec<[u8; 16]>,
    pub target_constraint_id: [u8; 16],
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateUniqueConstraintRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub table_id: [u8; 16],
    pub column_ids: Vec<[u8; 16]>,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatePrimaryKeyRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub table_id: [u8; 16],
    pub column_ids: Vec<[u8; 16]>,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterColumnDefaultRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub table_id: [u8; 16],
    pub column_id: [u8; 16],
    pub default: Option<TableColumnDefault>,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddTableColumnRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub table_id: [u8; 16],
    pub column: TableColumnDefinition,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedTable {
    pub table_id: [u8; 16],
    pub column_ids: Vec<[u8; 16]>,
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedForeignKey {
    pub foreign_key_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedUniqueConstraint {
    pub constraint_id: [u8; 16],
    pub index_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedPrimaryKey {
    pub constraint_id: [u8; 16],
    pub index_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddedTableColumn {
    pub table_id: [u8; 16],
    pub column_id: [u8; 16],
    pub schema_version: u64,
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TableLifecycleError {
    InvalidRequest,
    InvalidAudit,
    RandomnessUnavailable,
    ObjectIdGenerationFailed,
    Unauthorized,
    Storage,
    Commit(CommitError),
}

impl std::fmt::Display for TableLifecycleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "table lifecycle operation failed: {self:?}")
    }
}

impl std::error::Error for TableLifecycleError {}

fn table_lifecycle_commit_error(error: TableLifecycleError) -> CommitError {
    match error {
        TableLifecycleError::InvalidAudit => CommitError::InvalidAudit,
        TableLifecycleError::Unauthorized => CommitError::DdlAuthorizationDenied,
        TableLifecycleError::Storage => {
            CommitError::Storage("storage manager catalog read failed".to_owned())
        }
        TableLifecycleError::Commit(error) => error,
        TableLifecycleError::InvalidRequest
        | TableLifecycleError::RandomnessUnavailable
        | TableLifecycleError::ObjectIdGenerationFailed => CommitError::InvalidWrite,
    }
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
    max_invocation_scan_work: usize,
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
    Transaction(TransactionError),
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

    fn with_owned_transaction<T>(
        &self,
        operation: impl FnOnce(&mut InvocationTransaction<'_>) -> Result<T, TransactionError>,
    ) -> Result<T, SupervisorInvocationError> {
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
        operation(transaction).map_err(SupervisorInvocationError::Transaction)
    }

    /// Read one invocation-pinned page of schema metadata through the trusted
    /// storage discovery API. Executor channels remain separate and opaque.
    pub fn scan_information_schema_schemata(
        &self,
        page_limit: usize,
    ) -> Result<InformationSchemaSchemataPage, SupervisorInvocationError> {
        self.with_owned_transaction(|transaction| {
            transaction.scan_information_schema_schemata(page_limit, None)
        })
    }

    /// Read one invocation-pinned page of table metadata through trusted
    /// storage discovery.
    pub fn scan_information_schema_tables(
        &self,
        page_limit: usize,
        continuation: Option<InformationSchemaTablesCursor>,
    ) -> Result<InformationSchemaTablesScanPage, SupervisorInvocationError> {
        self.with_owned_transaction(|transaction| {
            transaction.scan_information_schema_tables(page_limit, continuation)
        })
    }

    /// Read one invocation-pinned page of column metadata through trusted
    /// storage discovery.
    pub fn scan_information_schema_columns(
        &self,
        page_limit: usize,
        continuation: Option<InformationSchemaColumnsCursor>,
    ) -> Result<InformationSchemaColumnsScanPage, SupervisorInvocationError> {
        self.with_owned_transaction(|transaction| {
            transaction.scan_information_schema_columns(page_limit, continuation)
        })
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
    // Keep ambiguous commit outcomes visible as COMMITTING until a reopened
    // Database handle resolves them from redb's recovered durable state.
    commit_uncertain: bool,
    cancellation_reason_code: Option<u16>,
    owner_generation: u128,
}

struct InvocationAdmissionState {
    database_key: usize,
    limit: AtomicUsize,
    database_uncertain: AtomicBool,
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
    DatabaseUncertain,
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
        if self.database_uncertain.load(Ordering::Acquire) {
            return Err(AdmissionError::DatabaseUncertain);
        }
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
                commit_uncertain: false,
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
    pub(crate) fn mark_commit_uncertain(&self) {
        let mut live = self
            .state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.state.database_uncertain.store(true, Ordering::Release);
        if let Some(invocation) = live.entries.get_mut(&self.invocation_id) {
            invocation.phase = LiveInvocationPhase::Committing;
            invocation.commit_uncertain = true;
            let principal_id = invocation.principal_id;
            live.advance_owner_generation(principal_id);
        }
        self.state.changed.notify_all();
        retain_uncertain_registry(self.state.database_key, Arc::clone(&self.state));
    }

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
        self.try_mark_committing_then(|| {})
    }

    fn try_mark_committing_then(&self, after_transition: impl FnOnce()) -> bool {
        let mut live = self
            .state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut after_transition = Some(after_transition);
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
                    drop(live);
                    after_transition
                        .take()
                        .expect("commit transition hook is consumed once")();
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
        let should_remove = live
            .entries
            .get(&self.invocation_id)
            .is_some_and(|invocation| !invocation.commit_uncertain);
        if should_remove && let Some(invocation) = live.entries.remove(&self.invocation_id) {
            live.advance_owner_generation(invocation.principal_id);
            self.state.changed.notify_all();
        }
    }
}

fn invocation_registry(
    database: &Database,
    configured_limit: usize,
) -> Arc<InvocationAdmissionState> {
    let database_key = database as *const Database as usize;
    let uncertain = UNCERTAIN_INVOCATION_REGISTRIES.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(invocations) = uncertain
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&database_key)
        .cloned()
    {
        invocations
            .limit
            .fetch_min(configured_limit, Ordering::Relaxed);
        return invocations;
    }
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
        database_key,
        limit: AtomicUsize::new(configured_limit),
        database_uncertain: AtomicBool::new(false),
        live: Mutex::new(LiveInvocationRegistry {
            generation: 1,
            entries: HashMap::new(),
        }),
        changed: Condvar::new(),
    });
    registry.insert(database_key, Arc::downgrade(&invocations));
    invocations
}

fn shared_invocation_registry(
    database: &Database,
    configured_limit: usize,
) -> Arc<InvocationAdmissionState> {
    invocation_registry(database, configured_limit)
}

fn retain_uncertain_registry(database_key: usize, state: Arc<InvocationAdmissionState>) {
    UNCERTAIN_INVOCATION_REGISTRIES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(database_key, state);
}

/// Quarantine this database handle after redb reports a commit error whose
/// publication state requires close/reopen recovery.
pub(crate) fn mark_database_uncertain(database: &Database) {
    let invocations = invocation_registry(
        database,
        StorageManagerConfig::default().max_active_invocations,
    );
    let first_uncertain = !invocations.database_uncertain.swap(true, Ordering::AcqRel);
    if first_uncertain {
        let mut live = invocations
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        live.generation = live.generation.wrapping_add(1).max(1);
    }
    retain_uncertain_registry(invocations.database_key, invocations);
}

/// Forget the closed handle's quarantined process-local state after a fresh
/// redb handle has been opened and validated.
pub(crate) fn clear_uncertain_database_key(database_key: usize) {
    UNCERTAIN_INVOCATION_REGISTRIES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&database_key);
    INVOCATION_REGISTRIES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&database_key);
}

impl<'db> StorageManager<'db> {
    /// Prune one bounded batch of expired durable outcomes. Storage failures
    /// quarantine this database handle; the owning service must reopen it.
    pub(crate) fn prune_expired_outcomes(
        &self,
        now_us: i64,
        max_scanned: usize,
    ) -> Result<crate::outcome::OutcomePruneResult, crate::outcome::OutcomeMaintenanceError> {
        crate::outcome::prune_expired_outcomes(self.database, now_us, max_scanned)
    }

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
        if config.max_invocation_scan_work == 0 {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::ZeroInvocationScanWorkLimit,
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
            max_invocation_scan_work: config.max_invocation_scan_work,
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

    pub fn max_invocation_scan_work(&self) -> usize {
        self.max_invocation_scan_work
    }

    /// Whether a managed invocation hit an ambiguous redb commit error and
    /// this database handle must be closed and reopened before more work.
    pub fn requires_reopen_after_commit_error(&self) -> bool {
        self.admissions.database_uncertain.load(Ordering::Acquire)
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
                AdmissionError::DatabaseUncertain => TransactionError::Storage(
                    "database requires reopen after an ambiguous commit error".into(),
                ),
            })?;
        InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::managed(
                self.ddl_fences.clone(),
                admission_permit,
                self.max_invocation_age,
                self.max_invocation_scan_work,
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

    /// Discover every schema visible to a token-authenticated principal from
    /// one request-scoped catalog snapshot. This read does not register an
    /// invocation or write a durable outcome. The bounded result is suitable
    /// for client-side requested-scope construction.
    pub fn discover_token_authorized_schemas(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
    ) -> Result<Vec<crate::metadata_discovery::VisibleSchema>, TransactionError> {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;

        let mut schemas = Vec::new();
        let mut encoded_bytes = 0usize;
        let mut continuation = None;
        loop {
            let page = transaction.list_catalog_authorized_schemas(
                crate::metadata_discovery::MAX_SCHEMAS_PER_PAGE,
                continuation,
            )?;
            if schemas.len().saturating_add(page.schemas.len()) > MAX_DISCOVERED_SCHEMAS {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            for schema in &page.schemas {
                encoded_bytes = encoded_bytes
                    .saturating_add(16)
                    .saturating_add(schema.name.len());
                if encoded_bytes > MAX_DISCOVERED_SCHEMA_BYTES {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
            }
            schemas.extend(page.schemas);
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        for schema in transaction.list_catalog_administrative_schemas()? {
            if !schemas
                .iter()
                .any(|existing| existing.schema_id == schema.schema_id)
            {
                encoded_bytes = encoded_bytes
                    .saturating_add(16)
                    .saturating_add(schema.name.len());
                if schemas.len() >= MAX_DISCOVERED_SCHEMAS
                    || encoded_bytes > MAX_DISCOVERED_SCHEMA_BYTES
                {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
                schemas.push(schema);
            }
        }
        schemas.sort_unstable_by_key(|schema| schema.schema_id);
        Ok(schemas)
    }

    /// Discover relations for one schema visible to a token-authenticated
    /// principal. Hidden and absent schemas return `None`; an authorized
    /// empty schema returns an empty vector. The read uses one request-scoped
    /// catalog snapshot and does not admit an invocation or write an outcome.
    pub fn discover_token_authorized_relations(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        schema_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::VisibleRelation>>, TransactionError> {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        if schema_id == crate::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID {
            return Ok(Some(
                crate::metadata_discovery::information_schema_relations().into(),
            ));
        }
        if let Some(relations) = transaction.list_catalog_administrative_relations(schema_id)? {
            return Ok(Some(relations));
        }

        let mut relations = Vec::new();
        let mut encoded_bytes = 0usize;
        let mut continuation = None;
        loop {
            let Some(page) = transaction.list_catalog_authorized_relations(
                schema_id,
                crate::metadata_discovery::MAX_RELATIONS_PER_PAGE,
                continuation,
            )?
            else {
                return Ok(None);
            };
            if relations.len().saturating_add(page.relations.len()) > MAX_DISCOVERED_RELATIONS {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            for relation in &page.relations {
                encoded_bytes = encoded_bytes
                    .saturating_add(16)
                    .saturating_add(relation.name.len());
                if encoded_bytes > MAX_DISCOVERED_RELATION_BYTES {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
            }
            relations.extend(page.relations);
            continuation = page.continuation;
            if continuation.is_none() {
                return Ok(Some(relations));
            }
        }
    }

    /// Describe every declared column for a relation visible to a
    /// token-authenticated principal. Hidden and absent schema/relation pairs
    /// return `None`; the public information_schema shapes are always visible.
    /// The bounded read uses a single catalog snapshot and creates no
    /// invocation admission or durable outcome.
    pub fn discover_token_authorized_columns(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::VisibleColumn>>, TransactionError> {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        if schema_id != crate::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID
            && let Some(columns) =
                transaction.describe_catalog_administrative_columns(schema_id, relation_id)?
        {
            return Ok(Some(columns));
        }

        let mut columns = Vec::new();
        let mut encoded_bytes = 0usize;
        let mut continuation = None;
        loop {
            let Some(page) = transaction.describe_catalog_authorized_columns_page(
                schema_id,
                relation_id,
                crate::metadata_discovery::MAX_COLUMNS_PER_PAGE,
                continuation,
            )?
            else {
                return Ok(None);
            };
            if columns.len().saturating_add(page.columns.len()) > MAX_DISCOVERED_COLUMNS {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            for column in &page.columns {
                encoded_bytes = encoded_bytes
                    .saturating_add(16)
                    .saturating_add(column.name.len())
                    .saturating_add(
                        crate::metadata_discovery::scalar_type_name(column.scalar_type).len(),
                    )
                    .saturating_add(5);
                if encoded_bytes > MAX_DISCOVERED_COLUMN_BYTES {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
            }
            columns.extend(page.columns);
            continuation = page.continuation;
            if continuation.is_none() {
                return Ok(Some(columns));
            }
        }
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
            None,
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
        self.admit_supervisor_invocation_inner(request, token_secret, None)
    }

    /// Admit an invocation and intersect its catalog-derived authority with
    /// the client's decoded requested capability set.
    pub fn admit_supervisor_invocation_with_deadline_and_scope(
        &self,
        request: SupervisorAdmissionRequest,
        requested_scope: crate::submission_scope::RequestedScope,
        token_secret: &[u8; TOKEN_SECRET_BYTES],
    ) -> Result<SupervisorInvocation<'db>, TransactionError> {
        if request.deadline.is_none() {
            return Err(TransactionError::InvalidContext);
        }
        self.admit_supervisor_invocation_inner(request, token_secret, Some(requested_scope))
    }

    fn admit_supervisor_invocation_inner(
        &self,
        request: SupervisorAdmissionRequest,
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        requested_scope: Option<crate::submission_scope::RequestedScope>,
    ) -> Result<SupervisorInvocation<'db>, TransactionError> {
        let principal_id = lookup_credential_principal(self.database, request.token_id)?;
        let (mut transaction, role_ids) = self.begin_token_authenticated_invocation(
            request.invocation_id,
            request.token_id,
            token_secret,
            request.retention_deadline_us,
            request.max_rows,
            request.max_bytes,
        )?;
        if let Some(requested_scope) = requested_scope {
            transaction.restrict_to_requested_scope(requested_scope);
        }
        Ok(SupervisorInvocation {
            invocation_id: request.invocation_id,
            principal_id,
            role_ids,
            transaction: Arc::new(Mutex::new(Some(transaction))),
            channels: Arc::new(Mutex::new(SupervisorChannelState::default())),
            deadline: request.deadline,
        })
    }

    /// Create a schema and assign its initial owner atomically. The actor must
    /// hold CREATE_SCHEMA or database-administrator membership.
    pub fn create_schema(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        name: &str,
        owner_id: [u8; 16],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<CreatedSchema, SchemaLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || owner_id == [0; 16]
            || retention_deadline_us <= 0
            || !is_normalized_principal_name(name)
        {
            return Err(SchemaLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(SchemaLifecycleError::InvalidAudit);
        }
        let mut schema_id = None;
        for _ in 0..8 {
            let mut candidate = [0; 16];
            if getrandom::fill(&mut candidate).is_err() {
                candidate.zeroize();
                return Err(SchemaLifecycleError::RandomnessUnavailable);
            }
            if candidate == [0; 16] {
                candidate.zeroize();
                continue;
            }
            let mut conflict = false;
            for family in [2, 3, 17] {
                let key = id_catalog_key(family, candidate);
                let read = self
                    .database
                    .begin_read()
                    .map_err(|_| SchemaLifecycleError::Storage)?;
                let catalog = read
                    .open_table(CATALOG)
                    .map_err(|_| SchemaLifecycleError::Storage)?;
                conflict |= catalog
                    .get(key.as_slice())
                    .map_err(|_| SchemaLifecycleError::Storage)?
                    .is_some();
            }
            if !conflict {
                schema_id = Some(candidate);
                break;
            }
            candidate.zeroize();
        }
        let schema_id = schema_id.ok_or(SchemaLifecycleError::SchemaIdGenerationFailed)?;
        let object_key = id_catalog_key(2, schema_id);
        let schema_key = id_catalog_key(3, schema_id);
        let object_payload = encode_envelope(
            &family_schema(2).ok_or(SchemaLifecycleError::Storage)?,
            &object_key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
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
            ],
        )
        .map_err(|_| SchemaLifecycleError::InvalidRequest)?;
        let schema_payload = encode_envelope(
            &family_schema(3).ok_or(SchemaLifecycleError::Storage)?,
            &schema_key,
            &[CatalogField {
                tag: 1,
                wire_type: WireType::Ids,
                payload: 0u32.to_le_bytes().to_vec(),
            }],
        )
        .map_err(|_| SchemaLifecycleError::InvalidRequest)?;
        audit_event.target_ids = vec![schema_id];
        let writes = [
            CatalogWrite {
                key: object_key,
                expected_value: None,
                payload: Some(object_payload),
            },
            CatalogWrite {
                key: schema_key,
                expected_value: None,
                payload: Some(schema_payload),
            },
        ];
        let commit_sequence = commit_authorized_schema_create_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => SchemaLifecycleError::Unauthorized,
            error => SchemaLifecycleError::Commit(error),
        })?;
        Ok(CreatedSchema {
            schema_id,
            commit_sequence,
        })
    }

    /// Drop an empty schema and its object record atomically. The schema owner
    /// or a database administrator must authorize the operation.
    pub fn drop_schema(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        schema_id: [u8; 16],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<DroppedSchema, SchemaLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || schema_id == [0; 16]
            || retention_deadline_us <= 0
        {
            return Err(SchemaLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(SchemaLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| SchemaLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| SchemaLifecycleError::Storage)?;
        let object_key = id_catalog_key(2, schema_id);
        let schema_key = id_catalog_key(3, schema_id);
        let object_bytes = catalog
            .get(object_key.as_slice())
            .map_err(|_| SchemaLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(SchemaLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let schema_bytes = catalog
            .get(schema_key.as_slice())
            .map_err(|_| SchemaLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(SchemaLifecycleError::Commit(CommitError::InvalidCatalog))?;
        decode_registered_envelope(&object_bytes)
            .map_err(|_| SchemaLifecycleError::Commit(CommitError::InvalidCatalog))?;
        decode_registered_envelope(&schema_bytes)
            .map_err(|_| SchemaLifecycleError::Commit(CommitError::InvalidCatalog))?;
        audit_event.target_ids = vec![schema_id];
        let writes = [
            CatalogWrite {
                key: object_key,
                expected_value: Some(object_bytes),
                payload: None,
            },
            CatalogWrite {
                key: schema_key,
                expected_value: Some(schema_bytes),
                payload: None,
            },
        ];
        let commit_sequence = commit_authorized_schema_drop_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => SchemaLifecycleError::Unauthorized,
            error => SchemaLifecycleError::Commit(error),
        })?;
        Ok(DroppedSchema {
            schema_id,
            commit_sequence,
        })
    }

    /// Transfer a schema's ownership without adding table data privileges.
    pub fn set_schema_owner(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        schema_id: [u8; 16],
        new_owner_id: [u8; 16],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, SchemaLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || schema_id == [0; 16]
            || new_owner_id == [0; 16]
            || retention_deadline_us <= 0
        {
            return Err(SchemaLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(SchemaLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| SchemaLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| SchemaLifecycleError::Storage)?;
        let object_key = id_catalog_key(2, schema_id);
        let schema_key = id_catalog_key(3, schema_id);
        let owner_key = id_catalog_key(10, new_owner_id);
        let object_current = catalog
            .get(object_key.as_slice())
            .map_err(|_| SchemaLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(SchemaLifecycleError::InvalidRequest)?;
        let _schema_current = catalog
            .get(schema_key.as_slice())
            .map_err(|_| SchemaLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(SchemaLifecycleError::InvalidRequest)?;
        let owner_current = catalog
            .get(owner_key.as_slice())
            .map_err(|_| SchemaLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(SchemaLifecycleError::InvalidRequest)?;
        if read_catalog_u16(&object_key, &object_current, 2, 1)
            .map_err(schema_lifecycle_from_table_error)?
            != 1
            || read_catalog_id(&object_key, &object_current, 2, 4)
                .map_err(schema_lifecycle_from_table_error)?
                == new_owner_id
        {
            return Err(SchemaLifecycleError::InvalidRequest);
        }
        let owner_record = decode_registered_envelope(&owner_current)
            .map_err(|_| SchemaLifecycleError::InvalidRequest)?;
        let owner_kind = owner_record
            .fields
            .iter()
            .find(|field| field.tag == 1 && field.wire_type == WireType::U16)
            .and_then(|field| <[u8; 2]>::try_from(field.payload.as_slice()).ok())
            .map(u16::from_le_bytes);
        let owner_epoch = owner_record
            .fields
            .iter()
            .find(|field| field.tag == 4 && field.wire_type == WireType::U64)
            .and_then(|field| <[u8; 8]>::try_from(field.payload.as_slice()).ok())
            .map(u64::from_le_bytes)
            .unwrap_or_default();
        if !matches!(owner_kind, Some(1 | 2)) || owner_epoch == 0 {
            return Err(SchemaLifecycleError::InvalidRequest);
        }
        let next_object_version = read_catalog_u64(&object_key, &object_current, 2, 5)
            .map_err(schema_lifecycle_from_table_error)?
            .checked_add(1)
            .ok_or(SchemaLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let object_payload = rewrite_catalog_fields(
            &object_key,
            &object_current,
            2,
            &[
                (4, WireType::Id, new_owner_id.to_vec()),
                (5, WireType::U64, next_object_version.to_le_bytes().to_vec()),
            ],
        )
        .map_err(schema_lifecycle_from_table_error)?;
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![schema_id];
        commit_authorized_schema_owner_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &[CatalogWrite {
                key: object_key,
                expected_value: Some(object_current),
                payload: Some(object_payload),
            }],
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => SchemaLifecycleError::Unauthorized,
            error => SchemaLifecycleError::Commit(error),
        })
    }

    /// Rename a schema while preserving its stable ID and contents.
    #[allow(clippy::too_many_arguments)]
    pub fn rename_schema(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        schema_id: [u8; 16],
        name: &str,
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, SchemaLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || schema_id == [0; 16]
            || retention_deadline_us <= 0
            || !is_normalized_principal_name(name)
        {
            return Err(SchemaLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(SchemaLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| SchemaLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| SchemaLifecycleError::Storage)?;
        let object_key = id_catalog_key(2, schema_id);
        let schema_key = id_catalog_key(3, schema_id);
        let object_current = catalog
            .get(object_key.as_slice())
            .map_err(|_| SchemaLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(SchemaLifecycleError::InvalidRequest)?;
        let _schema_current = catalog
            .get(schema_key.as_slice())
            .map_err(|_| SchemaLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(SchemaLifecycleError::InvalidRequest)?;
        if read_catalog_u16(&object_key, &object_current, 2, 1)
            .map_err(schema_lifecycle_from_table_error)?
            != 1
            || read_catalog_field(&object_key, &object_current, 2, 3)
                .map_err(schema_lifecycle_from_table_error)?
                == name.as_bytes()
        {
            return Err(SchemaLifecycleError::InvalidRequest);
        }
        let next_object_version = read_catalog_u64(&object_key, &object_current, 2, 5)
            .map_err(schema_lifecycle_from_table_error)?
            .checked_add(1)
            .ok_or(SchemaLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let object_payload = rewrite_catalog_fields(
            &object_key,
            &object_current,
            2,
            &[
                (3, WireType::Text, name.as_bytes().to_vec()),
                (5, WireType::U64, next_object_version.to_le_bytes().to_vec()),
            ],
        )
        .map_err(schema_lifecycle_from_table_error)?;
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![schema_id];
        commit_authorized_schema_rename_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &[CatalogWrite {
                key: object_key,
                expected_value: Some(object_current),
                payload: Some(object_payload),
            }],
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => SchemaLifecycleError::Unauthorized,
            error => SchemaLifecycleError::Commit(error),
        })
    }

    /// Drop a table with RESTRICT semantics, removing owned definitions and rows atomically.
    pub fn drop_table(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        table_id: [u8; 16],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<DroppedTable, TableLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || retention_deadline_us <= 0
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(TableLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| TableLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| TableLifecycleError::Storage)?;
        let mut deletes = HashMap::<Vec<u8>, Vec<u8>>::new();
        let mut owned_ids = HashSet::from([table_id]);
        let mut typed_records = Vec::<(Vec<u8>, Vec<u8>)>::new();
        for family in [5u16, 6, 8, 9, 13] {
            let prefix = family.to_be_bytes();
            let mut scanned = 0usize;
            for entry in catalog
                .range(prefix.as_slice()..)
                .map_err(|_| TableLifecycleError::Storage)?
            {
                let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
                let key = key.value().to_vec();
                if !key.starts_with(&prefix) {
                    break;
                }
                scanned += 1;
                if scanned > MAX_CATALOG_FAMILY_SCAN {
                    return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
                }
                let bytes = value.value().to_vec();
                let record = decode_registered_envelope(&bytes)
                    .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                let belongs = family == 5 && key == id_catalog_key(5, table_id)
                    || family != 5
                        && record
                            .fields
                            .iter()
                            .any(|field| field.tag == 1 && field.payload.as_slice() == table_id);
                if belongs {
                    if key.len() != 19 {
                        return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
                    }
                    owned_ids.insert(
                        key[3..19].try_into().map_err(|_| {
                            TableLifecycleError::Commit(CommitError::InvalidCatalog)
                        })?,
                    );
                    typed_records.push((key, bytes));
                }
            }
        }
        if typed_records
            .iter()
            .all(|(key, _)| key != &id_catalog_key(5, table_id))
        {
            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
        }
        let mut expression_ids = HashSet::<[u8; 16]>::new();
        let mut policy_ids = HashSet::<[u8; 16]>::new();
        for (key, bytes) in &typed_records {
            let family = u16::from_be_bytes([key[0], key[1]]);
            let record = decode_registered_envelope(bytes)
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            for field in &record.fields {
                if ((family == 6 && matches!(field.tag, 7 | 10))
                    || (family == 8 && field.tag == 4)
                    || (family == 13 && matches!(field.tag, 5 | 6)))
                    && field.payload.len() == 16
                {
                    let id: [u8; 16] = field.payload.as_slice().try_into().unwrap();
                    expression_ids.insert(id);
                    owned_ids.insert(id);
                }
            }
            if family == 13 {
                policy_ids.insert(
                    key[3..19]
                        .try_into()
                        .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?,
                );
            }
        }
        // Include object records for the table's typed children and their defaults.
        let object_prefix = 2u16.to_be_bytes();
        let mut changed = true;
        while changed {
            changed = false;
            let mut scanned = 0usize;
            for entry in catalog
                .range(object_prefix.as_slice()..)
                .map_err(|_| TableLifecycleError::Storage)?
            {
                let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
                let key = key.value().to_vec();
                if !key.starts_with(&object_prefix) {
                    break;
                }
                scanned += 1;
                if scanned > MAX_CATALOG_FAMILY_SCAN {
                    return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
                }
                if key.len() != 19 {
                    return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
                }
                let id: [u8; 16] = key[3..19].try_into().unwrap();
                let bytes = value.value().to_vec();
                let record = decode_registered_envelope(&bytes)
                    .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                let parent = record
                    .fields
                    .iter()
                    .find(|field| field.tag == 2)
                    .and_then(|field| <[u8; 16]>::try_from(field.payload.as_slice()).ok());
                if owned_ids.contains(&id)
                    || parent.is_some_and(|parent| owned_ids.contains(&parent))
                {
                    if owned_ids.insert(id) {
                        changed = true;
                    }
                    deletes.insert(key, bytes);
                }
            }
        }
        for (key, bytes) in typed_records {
            deletes.insert(key, bytes);
        }
        // Removing an owned foreign key changes the referenced table's
        // constraint graph too, so advance that table's write epoch in the
        // same transaction. The table being dropped is already removed.
        let mut referenced_tables = HashSet::new();
        for (key, bytes) in &deletes {
            if key.starts_with(&8u16.to_be_bytes()) && read_catalog_u16(key, bytes, 8, 2)? == 5 {
                let target_constraint_id = read_catalog_id(key, bytes, 8, 5)?;
                let target_constraint_key = id_catalog_key(8, target_constraint_id);
                let target_constraint_bytes = catalog
                    .get(target_constraint_key.as_slice())
                    .map_err(|_| TableLifecycleError::Storage)?
                    .map(|value| value.value().to_vec())
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                let target_table_id =
                    read_catalog_id(&target_constraint_key, &target_constraint_bytes, 8, 1)?;
                if target_table_id != table_id {
                    referenced_tables.insert(target_table_id);
                }
            }
        }
        let mut table_epoch_updates = Vec::with_capacity(referenced_tables.len());
        for referenced_table_id in referenced_tables {
            let key = id_catalog_key(5, referenced_table_id);
            let bytes = catalog
                .get(key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let next_epoch = read_catalog_u64(&key, &bytes, 5, 3)?
                .checked_add(1)
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            table_epoch_updates.push(CatalogWrite {
                key: key.clone(),
                expected_value: Some(bytes.clone()),
                payload: Some(rewrite_catalog_u64_field(&key, &bytes, 5, 3, next_epoch)?),
            });
        }
        for id in &expression_ids {
            let key = id_catalog_key(7, *id);
            if let Some(value) = catalog
                .get(key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
            {
                deletes.insert(key, value.value().to_vec());
            }
        }
        let policy_target_prefix = 14u16.to_be_bytes();
        let mut scanned = 0usize;
        for entry in catalog
            .range(policy_target_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value().to_vec();
            if !key.starts_with(&policy_target_prefix) {
                break;
            }
            scanned += 1;
            if scanned > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            let bytes = value.value().to_vec();
            let target = decode_policy_target(&bytes)
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            if policy_ids.contains(&target.policy_id) {
                deletes.insert(key, bytes);
            }
        }
        // Remove grants on owned objects and their matching authority dependency edges.
        let grants_prefix = 12u16.to_be_bytes();
        let mut scanned = 0usize;
        let mut grant_pairs = Vec::new();
        let mut changed_grantees = HashSet::<[u8; 16]>::new();
        for entry in catalog
            .range(grants_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value().to_vec();
            if !key.starts_with(&grants_prefix) {
                break;
            }
            scanned += 1;
            if scanned > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            let object_id = key
                .get(20..36)
                .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok());
            if key.len() == 59 && object_id.is_some_and(|object_id| owned_ids.contains(&object_id))
            {
                let grantee: [u8; 16] = key[3..19].try_into().unwrap();
                let target: [u8; 16] = key[43..59].try_into().unwrap();
                grant_pairs.push((grantee, target));
                changed_grantees.insert(grantee);
                deletes.insert(key, value.value().to_vec());
            }
        }
        for (grantee, target) in grant_pairs {
            let key = dependency_catalog_key(grantee, target, 5);
            if let Some(value) = catalog
                .get(key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
            {
                deletes.insert(key, value.value().to_vec());
            }
        }
        let mut principal_updates = Vec::with_capacity(changed_grantees.len());
        for grantee_id in changed_grantees {
            let principal_key = id_catalog_key(10, grantee_id);
            let principal_bytes = catalog
                .get(principal_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?
                .value()
                .to_vec();
            let mut principal = decode_registered_envelope(&principal_bytes)
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let epoch = principal
                .fields
                .iter_mut()
                .find(|field| field.tag == 4 && field.wire_type == WireType::U64)
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let current_epoch = u64::from_le_bytes(
                epoch
                    .payload
                    .as_slice()
                    .try_into()
                    .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?,
            );
            epoch.payload = current_epoch
                .checked_add(1)
                .filter(|next| *next != 0)
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?
                .to_le_bytes()
                .to_vec();
            let payload = encode_envelope(
                &family_schema(10)
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?,
                &principal_key,
                &principal.fields,
            )
            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            principal_updates.push(CatalogWrite {
                key: principal_key,
                expected_value: Some(principal_bytes),
                payload: Some(payload),
            });
        }
        let dependency_prefix = 15u16.to_be_bytes();
        let mut scanned = 0usize;
        for entry in catalog
            .range(dependency_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value().to_vec();
            if !key.starts_with(&dependency_prefix) {
                break;
            }
            scanned += 1;
            if scanned > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            let dependent: [u8; 16] = key
                .get(3..19)
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?
                .try_into()
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            if owned_ids.contains(&dependent) {
                deletes.insert(key, value.value().to_vec());
            }
        }
        let writes = principal_updates
            .into_iter()
            .chain(table_epoch_updates)
            .chain(
                deletes
                    .into_iter()
                    .map(|(key, expected_value)| CatalogWrite {
                        key,
                        expected_value: Some(expected_value),
                        payload: None,
                    }),
            )
            .collect::<Vec<_>>();
        audit_event.target_ids = vec![table_id];
        let commit_sequence = commit_authorized_table_drop_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
            error => TableLifecycleError::Commit(error),
        })?;
        Ok(DroppedTable {
            table_id,
            commit_sequence,
        })
    }

    /// Add one column and backfill every existing row from its immutable default.
    pub fn add_table_column(
        &self,
        request: AddTableColumnRequest,
    ) -> Result<AddedTableColumn, TableLifecycleError> {
        let AddTableColumnRequest {
            invocation_id,
            authenticated_actor_id,
            table_id,
            column,
            retention_deadline_us,
            mut audit_event,
        } = request;
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || retention_deadline_us <= 0
            || !is_normalized_principal_name(&column.name)
            || column.generated_uuid_identity
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(TableLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| TableLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| TableLifecycleError::Storage)?;
        let table_key = id_catalog_key(5, table_id);
        let table_bytes = catalog
            .get(table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        let old_schema_version = read_catalog_u64(&table_key, &table_bytes, 5, 2)?;
        let old_write_epoch = read_catalog_u64(&table_key, &table_bytes, 5, 3)?;
        let next_schema_version = old_schema_version
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let next_write_epoch = old_write_epoch
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let column_prefix = 6u16.to_be_bytes();
        let mut ordinal = 0usize;
        let mut scanned = 0usize;
        for entry in catalog
            .range(column_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value().to_vec();
            if !key.starts_with(&column_prefix) {
                break;
            }
            scanned += 1;
            if scanned > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            let bytes = value.value().to_vec();
            if read_catalog_id(&key, &bytes, 6, 1)? != table_id {
                continue;
            }
            if read_catalog_field(&key, &bytes, 6, 3)? == column.name.as_bytes() {
                return Err(TableLifecycleError::InvalidRequest);
            }
            ordinal = ordinal
                .checked_add(1)
                .ok_or(TableLifecycleError::InvalidRequest)?;
        }
        if ordinal == 0 || ordinal >= crate::tuple::MAX_COLUMNS {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let type_definitions = load_builtin_type_definitions(self.database)?;
        let type_kind = scalar_type_kind(column.scalar_type);
        let (type_id, type_record) = type_definitions
            .get(&type_kind)
            .ok_or(TableLifecycleError::InvalidRequest)?;
        let type_parameters = scalar_type_parameters(column.scalar_type);
        decode_builtin_scalar_type(type_record, &type_parameters)
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
        let default = match column.default.as_ref() {
            Some(TableColumnDefault::Literal(value)) => {
                let default_mask = u32::from_le_bytes(
                    type_record
                        .fields
                        .iter()
                        .find(|field| field.tag == 5)
                        .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?
                        .payload
                        .as_slice()
                        .try_into()
                        .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?,
                );
                if default_mask & 1 == 0 {
                    return Err(TableLifecycleError::InvalidRequest);
                }
                encode_tuple(
                    &[ColumnSchema {
                        scalar_type: column.scalar_type,
                        nullable: column.nullable,
                    }],
                    1,
                    std::slice::from_ref(value),
                )
                .map_err(|_| TableLifecycleError::InvalidRequest)?;
                value.clone()
            }
            Some(TableColumnDefault::CurrentPrincipalId) => {
                return Err(TableLifecycleError::InvalidRequest);
            }
            None if column.nullable => None,
            None => return Err(TableLifecycleError::InvalidRequest),
        };
        let mut column_id = self.generate_table_object_id()?;
        let expression_id = if column.default.is_some() {
            let candidate = self.generate_table_object_id()?;
            if candidate == column_id {
                column_id.zeroize();
                return Err(TableLifecycleError::ObjectIdGenerationFailed);
            }
            Some(candidate)
        } else {
            None
        };
        let column_key = id_catalog_key(6, column_id);
        let object_key = id_catalog_key(2, column_id);
        let mut writes = vec![
            CatalogWrite {
                key: table_key.clone(),
                expected_value: Some(table_bytes.clone()),
                payload: Some(rewrite_catalog_u64_fields(
                    &table_key,
                    &table_bytes,
                    5,
                    &[(2, next_schema_version), (3, next_write_epoch)],
                )?),
            },
            CatalogWrite {
                key: object_key.clone(),
                expected_value: None,
                payload: Some(
                    encode_envelope(
                        &family_schema(2).ok_or(TableLifecycleError::Storage)?,
                        &object_key,
                        &[
                            CatalogField {
                                tag: 1,
                                wire_type: WireType::U16,
                                payload: 4u16.to_le_bytes().to_vec(),
                            },
                            CatalogField {
                                tag: 2,
                                wire_type: WireType::Id,
                                payload: table_id.to_vec(),
                            },
                            CatalogField {
                                tag: 3,
                                wire_type: WireType::Text,
                                payload: column.name.as_bytes().to_vec(),
                            },
                            CatalogField {
                                tag: 4,
                                wire_type: WireType::Id,
                                payload: authenticated_actor_id.to_vec(),
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
                    .map_err(|_| TableLifecycleError::InvalidRequest)?,
                ),
            },
        ];
        let mut fields = vec![
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U32,
                payload: (ordinal as u32).to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Text,
                payload: column.name.as_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Id,
                payload: type_id.to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::Bytes,
                payload: type_parameters.to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Bool,
                payload: vec![u8::from(column.nullable)],
            },
        ];
        if let Some(expression_id) = expression_id {
            fields.push(CatalogField {
                tag: 7,
                wire_type: WireType::Id,
                payload: expression_id.to_vec(),
            });
        }
        fields.extend([
            CatalogField {
                tag: 8,
                wire_type: WireType::U16,
                payload: 0u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 9,
                wire_type: WireType::U64,
                payload: next_schema_version.to_le_bytes().to_vec(),
            },
        ]);
        if let Some(expression_id) = expression_id {
            fields.push(CatalogField {
                tag: 10,
                wire_type: WireType::Id,
                payload: expression_id.to_vec(),
            });
        }
        writes.push(CatalogWrite {
            key: column_key.clone(),
            expected_value: None,
            payload: Some(
                encode_envelope(
                    &family_schema(6).ok_or(TableLifecycleError::Storage)?,
                    &column_key,
                    &fields,
                )
                .map_err(|_| TableLifecycleError::InvalidRequest)?,
            ),
        });
        writes.push(new_dependency_write(column_id, table_id, 1)?);
        if let Some(expression_id) = expression_id {
            writes.push(new_default_expression_object_write(
                expression_id,
                ordinal,
                authenticated_actor_id,
            )?);
            writes.push(new_default_expression_write(
                expression_id,
                *type_id,
                type_parameters,
                crate::authorization::Expr::Literal(default),
            )?);
            writes.push(new_dependency_write(column_id, expression_id, 2)?);
        }
        let index_prefix = 9u16.to_be_bytes();
        let mut index_scanned = 0usize;
        for entry in catalog
            .range(index_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value().to_vec();
            if !key.starts_with(&index_prefix) {
                break;
            }
            index_scanned += 1;
            if index_scanned > MAX_CATALOG_FAMILY_SCAN {
                column_id.zeroize();
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            let bytes = value.value().to_vec();
            if read_catalog_id(&key, &bytes, 9, 1)? == table_id {
                writes.push(CatalogWrite {
                    key: key.clone(),
                    expected_value: Some(bytes.clone()),
                    payload: Some(rewrite_catalog_u64_field(
                        &key,
                        &bytes,
                        9,
                        10,
                        next_schema_version,
                    )?),
                });
            }
        }
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![table_id, column_id];
        audit_event.target_ids.sort_unstable();
        let commit_sequence = self
            .commit_authorized_constraint_ddl(
                invocation_id,
                authenticated_actor_id,
                retention_deadline_us,
                &writes,
                &[audit_event],
            )
            .map_err(|error| match error {
                CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
                error => TableLifecycleError::Commit(error),
            })?;
        Ok(AddedTableColumn {
            table_id,
            column_id,
            schema_version: next_schema_version,
            commit_sequence,
        })
    }

    /// Drop one unreferenced table column and rewrite stored tuples atomically.
    pub fn drop_column(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        table_id: [u8; 16],
        column_id: [u8; 16],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<DroppedColumn, TableLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || column_id == [0; 16]
            || retention_deadline_us <= 0
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(TableLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| TableLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| TableLifecycleError::Storage)?;
        let table_key = id_catalog_key(5, table_id);
        let table_bytes = catalog
            .get(table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let column_key = id_catalog_key(6, column_id);
        let column_bytes = catalog
            .get(column_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let column_record = decode_registered_envelope(&column_bytes)
            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let actual_table_id: [u8; 16] = column_record
            .fields
            .iter()
            .find(|field| field.tag == 1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?
            .payload
            .as_slice()
            .try_into()
            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        if actual_table_id != table_id {
            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
        }
        let schema_version = read_catalog_u64(&table_key, &table_bytes, 5, 2)?;
        let write_epoch = read_catalog_u64(&table_key, &table_bytes, 5, 3)?;
        let next_schema_version = schema_version
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let next_write_epoch = write_epoch
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let mut writes = Vec::new();
        writes.push(CatalogWrite {
            key: table_key.clone(),
            expected_value: Some(table_bytes.clone()),
            payload: Some(rewrite_catalog_u64_fields(
                &table_key,
                &table_bytes,
                5,
                &[(2, next_schema_version), (3, next_write_epoch)],
            )?),
        });
        let old_ordinal = read_catalog_u32(&column_key, &column_bytes, 6, 2)?;
        let object_key = id_catalog_key(2, column_id);
        let object_bytes = catalog
            .get(object_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        writes.push(CatalogWrite {
            key: column_key.clone(),
            expected_value: Some(column_bytes.clone()),
            payload: None,
        });
        writes.push(CatalogWrite {
            key: object_key,
            expected_value: Some(object_bytes),
            payload: None,
        });
        let mut expression_ids = HashSet::<[u8; 16]>::new();
        for tag in [7u16, 10] {
            if let Some(field) = column_record.fields.iter().find(|field| field.tag == tag) {
                let expression_id: [u8; 16] = field
                    .payload
                    .as_slice()
                    .try_into()
                    .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                expression_ids.insert(expression_id);
            }
        }
        for expression_id in &expression_ids {
            for family in [2u16, 7] {
                let key = id_catalog_key(family, *expression_id);
                if let Some(value) = catalog
                    .get(key.as_slice())
                    .map_err(|_| TableLifecycleError::Storage)?
                {
                    writes.push(CatalogWrite {
                        key,
                        expected_value: Some(value.value().to_vec()),
                        payload: None,
                    });
                }
            }
        }
        let prefix = 6u16.to_be_bytes();
        let mut scanned = 0usize;
        for entry in catalog
            .range(prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value().to_vec();
            if !key.starts_with(&prefix) {
                break;
            }
            scanned += 1;
            if scanned > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            if key == column_key {
                continue;
            }
            let bytes = value.value().to_vec();
            if read_catalog_id(&key, &bytes, 6, 1)? != table_id {
                continue;
            }
            let ordinal = read_catalog_u32(&key, &bytes, 6, 2)?;
            if ordinal > old_ordinal {
                writes.push(CatalogWrite {
                    key: key.clone(),
                    expected_value: Some(bytes.clone()),
                    payload: Some(rewrite_catalog_u32_field(&key, &bytes, 6, 2, ordinal - 1)?),
                });
            }
        }
        let index_prefix = 9u16.to_be_bytes();
        let mut scanned = 0usize;
        for entry in catalog
            .range(index_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value().to_vec();
            if !key.starts_with(&index_prefix) {
                break;
            }
            scanned += 1;
            if scanned > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            let bytes = value.value().to_vec();
            if read_catalog_id(&key, &bytes, 9, 1)? != table_id {
                continue;
            }
            writes.push(CatalogWrite {
                key: key.clone(),
                expected_value: Some(bytes.clone()),
                payload: Some(rewrite_catalog_u64_field(
                    &key,
                    &bytes,
                    9,
                    10,
                    next_schema_version,
                )?),
            });
        }
        let grants_prefix = 12u16.to_be_bytes();
        let mut scanned = 0usize;
        let mut grant_pairs = Vec::new();
        for entry in catalog
            .range(grants_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value().to_vec();
            if !key.starts_with(&grants_prefix) {
                break;
            }
            scanned += 1;
            if scanned > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            if key.len() == 59
                && key[20..36] == table_id
                && key[40..42] == 2u16.to_be_bytes()
                && key[43..59] == column_id
            {
                let grantee: [u8; 16] = key[3..19]
                    .try_into()
                    .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                grant_pairs.push((grantee, column_id));
                writes.push(CatalogWrite {
                    key,
                    expected_value: Some(value.value().to_vec()),
                    payload: None,
                });
            }
        }
        let mut dependent_ids = expression_ids.clone();
        dependent_ids.insert(column_id);
        let dependency_prefix = 15u16.to_be_bytes();
        let mut scanned = 0usize;
        for entry in catalog
            .range(dependency_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value().to_vec();
            if !key.starts_with(&dependency_prefix) {
                break;
            }
            scanned += 1;
            if scanned > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            let dependent: [u8; 16] = key
                .get(3..19)
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?
                .try_into()
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let referenced: [u8; 16] = key
                .get(20..36)
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?
                .try_into()
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let kind = u16::from_be_bytes(
                key.get(37..39)
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?
                    .try_into()
                    .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?,
            );
            let remove_grant_edge = kind == 5 && grant_pairs.contains(&(dependent, referenced));
            if dependent_ids.contains(&dependent) || remove_grant_edge {
                writes.push(CatalogWrite {
                    key,
                    expected_value: Some(value.value().to_vec()),
                    payload: None,
                });
            }
        }
        audit_event.target_ids = vec![table_id, column_id];
        audit_event.target_ids.sort_unstable();
        let commit_sequence = commit_authorized_column_drop_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
            error => TableLifecycleError::Commit(error),
        })?;
        Ok(DroppedColumn {
            table_id,
            column_id,
            commit_sequence,
        })
    }

    /// Rename a table while preserving its stable object ID.
    pub fn rename_table(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        table_id: [u8; 16],
        name: &str,
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, TableLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || retention_deadline_us <= 0
            || !is_normalized_principal_name(name)
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(TableLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| TableLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| TableLifecycleError::Storage)?;
        let table_key = id_catalog_key(5, table_id);
        let object_key = id_catalog_key(2, table_id);
        let table_current = catalog
            .get(table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        let object_current = catalog
            .get(object_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        if read_catalog_u16(&object_key, &object_current, 2, 1)? != 3
            || read_catalog_id(&object_key, &object_current, 2, 2)?
                != read_catalog_id(&table_key, &table_current, 5, 1)?
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let old_name = read_catalog_field(&object_key, &object_current, 2, 3)?;
        if old_name == name.as_bytes() {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let table_epoch = read_catalog_u64(&table_key, &table_current, 5, 3)?
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let object_version = read_catalog_u64(&object_key, &object_current, 2, 5)?
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let table_payload =
            rewrite_catalog_u64_field(&table_key, &table_current, 5, 3, table_epoch)?;
        let object_payload = rewrite_catalog_fields(
            &object_key,
            &object_current,
            2,
            &[
                (3, WireType::Text, name.as_bytes().to_vec()),
                (5, WireType::U64, object_version.to_le_bytes().to_vec()),
            ],
        )?;
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![table_id];
        commit_authorized_table_rename_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &[
                CatalogWrite {
                    key: table_key,
                    expected_value: Some(table_current),
                    payload: Some(table_payload),
                },
                CatalogWrite {
                    key: object_key,
                    expected_value: Some(object_current),
                    payload: Some(object_payload),
                },
            ],
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
            error => TableLifecycleError::Commit(error),
        })
    }

    /// Transfer a table's DDL ownership without granting data privileges.
    #[allow(clippy::too_many_arguments)]
    pub fn set_table_owner(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        table_id: [u8; 16],
        new_owner_id: [u8; 16],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, TableLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || new_owner_id == [0; 16]
            || retention_deadline_us <= 0
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(TableLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| TableLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| TableLifecycleError::Storage)?;
        let table_key = id_catalog_key(5, table_id);
        let object_key = id_catalog_key(2, table_id);
        let owner_key = id_catalog_key(10, new_owner_id);
        let table_current = catalog
            .get(table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        let object_current = catalog
            .get(object_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        let owner_current = catalog
            .get(owner_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        let table_schema_id = read_catalog_id(&table_key, &table_current, 5, 1)?;
        if read_catalog_u16(&object_key, &object_current, 2, 1)? != 3
            || read_catalog_id(&object_key, &object_current, 2, 2)? != table_schema_id
            || read_catalog_id(&object_key, &object_current, 2, 4)? == new_owner_id
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let owner_record = decode_registered_envelope(&owner_current)
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
        let owner_kind = owner_record
            .fields
            .iter()
            .find(|field| field.tag == 1 && field.wire_type == WireType::U16)
            .and_then(|field| <[u8; 2]>::try_from(field.payload.as_slice()).ok())
            .map(u16::from_le_bytes);
        let owner_epoch = owner_record
            .fields
            .iter()
            .find(|field| field.tag == 4 && field.wire_type == WireType::U64)
            .and_then(|field| <[u8; 8]>::try_from(field.payload.as_slice()).ok())
            .map(u64::from_le_bytes)
            .unwrap_or_default();
        if !matches!(owner_kind, Some(1 | 2)) || owner_epoch == 0 {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let next_epoch = read_catalog_u64(&table_key, &table_current, 5, 3)?
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let next_object_version = read_catalog_u64(&object_key, &object_current, 2, 5)?
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let table_payload =
            rewrite_catalog_u64_field(&table_key, &table_current, 5, 3, next_epoch)?;
        let object_payload = rewrite_catalog_fields(
            &object_key,
            &object_current,
            2,
            &[
                (4, WireType::Id, new_owner_id.to_vec()),
                (5, WireType::U64, next_object_version.to_le_bytes().to_vec()),
            ],
        )?;
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![table_id];
        commit_authorized_table_owner_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &[
                CatalogWrite {
                    key: table_key,
                    expected_value: Some(table_current),
                    payload: Some(table_payload),
                },
                CatalogWrite {
                    key: object_key,
                    expected_value: Some(object_current),
                    payload: Some(object_payload),
                },
            ],
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
            error => TableLifecycleError::Commit(error),
        })
    }

    /// Rename a column while preserving its stable ID and tuple position.
    #[allow(clippy::too_many_arguments)]
    pub fn rename_column(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        table_id: [u8; 16],
        column_id: [u8; 16],
        name: &str,
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, TableLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || column_id == [0; 16]
            || retention_deadline_us <= 0
            || !is_normalized_principal_name(name)
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(TableLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| TableLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| TableLifecycleError::Storage)?;
        let table_key = id_catalog_key(5, table_id);
        let column_key = id_catalog_key(6, column_id);
        let object_key = id_catalog_key(2, column_id);
        let table_current = catalog
            .get(table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        let column_current = catalog
            .get(column_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        let object_current = catalog
            .get(object_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        if read_catalog_id(&column_key, &column_current, 6, 1)? != table_id
            || read_catalog_u16(&object_key, &object_current, 2, 1)? != 4
            || read_catalog_id(&object_key, &object_current, 2, 2)? != table_id
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let old_name = read_catalog_field(&column_key, &column_current, 6, 3)?;
        if old_name == name.as_bytes()
            || read_catalog_field(&object_key, &object_current, 2, 3)? != old_name
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let table_epoch = read_catalog_u64(&table_key, &table_current, 5, 3)?
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let object_version = read_catalog_u64(&object_key, &object_current, 2, 5)?
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let table_payload =
            rewrite_catalog_u64_field(&table_key, &table_current, 5, 3, table_epoch)?;
        let column_payload = rewrite_catalog_fields(
            &column_key,
            &column_current,
            6,
            &[(3, WireType::Text, name.as_bytes().to_vec())],
        )?;
        let object_payload = rewrite_catalog_fields(
            &object_key,
            &object_current,
            2,
            &[
                (3, WireType::Text, name.as_bytes().to_vec()),
                (5, WireType::U64, object_version.to_le_bytes().to_vec()),
            ],
        )?;
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![table_id, column_id];
        audit_event.target_ids.sort_unstable();
        commit_authorized_column_rename_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &[
                CatalogWrite {
                    key: table_key,
                    expected_value: Some(table_current),
                    payload: Some(table_payload),
                },
                CatalogWrite {
                    key: column_key,
                    expected_value: Some(column_current),
                    payload: Some(column_payload),
                },
                CatalogWrite {
                    key: object_key,
                    expected_value: Some(object_current),
                    payload: Some(object_payload),
                },
            ],
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
            error => TableLifecycleError::Commit(error),
        })
    }

    /// Change an existing column's nullability through authorized constraint DDL.
    #[allow(clippy::too_many_arguments)]
    pub fn set_column_nullable(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        table_id: [u8; 16],
        column_id: [u8; 16],
        nullable: bool,
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, TableLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || column_id == [0; 16]
            || retention_deadline_us <= 0
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(TableLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| TableLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| TableLifecycleError::Storage)?;
        let table_key = id_catalog_key(5, table_id);
        let column_key = id_catalog_key(6, column_id);
        let table_current = catalog
            .get(table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        let column_current = catalog
            .get(column_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        if read_catalog_id(&column_key, &column_current, 6, 1)? != table_id {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let current_nullable = read_catalog_field(&column_key, &column_current, 6, 6)?;
        let current_nullable = match current_nullable.as_slice() {
            [0] => false,
            [1] => true,
            _ => return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog)),
        };
        if current_nullable == nullable {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let constraint_prefix = 8u16.to_be_bytes();
        let mut existing_not_null = Vec::new();
        let mut scanned = 0usize;
        for entry in catalog
            .range(constraint_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value().to_vec();
            if !key.starts_with(&constraint_prefix) {
                break;
            }
            scanned += 1;
            if scanned > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            let bytes = value.value().to_vec();
            if read_catalog_u16(&key, &bytes, 8, 2)? != 1
                || read_catalog_id(&key, &bytes, 8, 1)? != table_id
            {
                continue;
            }
            let record = read_catalog_record(&key, &bytes, 8)?;
            let field = record
                .fields
                .iter()
                .find(|field| field.tag == 3 && field.wire_type == WireType::Ids)
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let payload = field.payload.as_slice();
            if payload.len() < 4 {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            let count = u32::from_le_bytes(payload[..4].try_into().unwrap()) as usize;
            if count > crate::tuple::MAX_COLUMNS || payload.len() != 4 + count * 16 {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            if payload[4..]
                .chunks_exact(16)
                .any(|source| source == column_id)
            {
                existing_not_null.push((key, bytes));
            }
        }
        if existing_not_null.len() > 1
            || (nullable && current_nullable && !existing_not_null.is_empty())
        {
            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
        }
        let mut writes = Vec::new();
        let next_epoch = read_catalog_u64(&table_key, &table_current, 5, 3)?
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        writes.push(CatalogWrite {
            key: table_key.clone(),
            expected_value: Some(table_current.clone()),
            payload: Some(rewrite_catalog_u64_field(
                &table_key,
                &table_current,
                5,
                3,
                next_epoch,
            )?),
        });
        writes.push(CatalogWrite {
            key: column_key.clone(),
            expected_value: Some(column_current.clone()),
            payload: Some(rewrite_catalog_fields(
                &column_key,
                &column_current,
                6,
                &[(6, WireType::Bool, vec![u8::from(nullable)])],
            )?),
        });
        if nullable {
            if let Some((constraint_key, constraint_bytes)) = existing_not_null.first() {
                let constraint_id: [u8; 16] = constraint_key[3..19]
                    .try_into()
                    .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                let object_key = id_catalog_key(2, constraint_id);
                let object_bytes = catalog
                    .get(object_key.as_slice())
                    .map_err(|_| TableLifecycleError::Storage)?
                    .map(|value| value.value().to_vec())
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                writes.push(CatalogWrite {
                    key: constraint_key.clone(),
                    expected_value: Some(constraint_bytes.clone()),
                    payload: None,
                });
                writes.push(CatalogWrite {
                    key: object_key,
                    expected_value: Some(object_bytes),
                    payload: None,
                });
            }
        } else {
            if !existing_not_null.is_empty() {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            let constraint_id = self.generate_table_object_id()?;
            let constraint_key = id_catalog_key(8, constraint_id);
            let mut source_columns = 1u32.to_le_bytes().to_vec();
            source_columns.extend_from_slice(&column_id);
            let constraint_payload = encode_envelope(
                &family_schema(8).ok_or(TableLifecycleError::Storage)?,
                &constraint_key,
                &[
                    CatalogField {
                        tag: 1,
                        wire_type: WireType::Id,
                        payload: table_id.to_vec(),
                    },
                    CatalogField {
                        tag: 2,
                        wire_type: WireType::U16,
                        payload: 1u16.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 3,
                        wire_type: WireType::Ids,
                        payload: source_columns,
                    },
                    CatalogField {
                        tag: 6,
                        wire_type: WireType::Ids,
                        payload: 0u32.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 7,
                        wire_type: WireType::U16,
                        payload: 0u16.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 8,
                        wire_type: WireType::U16,
                        payload: 0u16.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 9,
                        wire_type: WireType::U16,
                        payload: 0u16.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 10,
                        wire_type: WireType::U16,
                        payload: 1u16.to_le_bytes().to_vec(),
                    },
                ],
            )
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
            let object_key = id_catalog_key(2, constraint_id);
            let name = format!(
                "not_null_{}",
                constraint_id
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            );
            let object_payload = encode_envelope(
                &family_schema(2).ok_or(TableLifecycleError::Storage)?,
                &object_key,
                &[
                    CatalogField {
                        tag: 1,
                        wire_type: WireType::U16,
                        payload: 6u16.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 3,
                        wire_type: WireType::Text,
                        payload: name.into_bytes(),
                    },
                    CatalogField {
                        tag: 4,
                        wire_type: WireType::Id,
                        payload: authenticated_actor_id.to_vec(),
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
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
            writes.push(CatalogWrite {
                key: constraint_key,
                expected_value: None,
                payload: Some(constraint_payload),
            });
            writes.push(CatalogWrite {
                key: object_key,
                expected_value: None,
                payload: Some(object_payload),
            });
        }
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![table_id, column_id];
        audit_event.target_ids.sort_unstable();
        self.commit_authorized_constraint_ddl(
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
            error => TableLifecycleError::Commit(error),
        })
    }

    /// Set or drop a column's current insert default.
    pub fn alter_column_default(
        &self,
        request: AlterColumnDefaultRequest,
    ) -> Result<u64, TableLifecycleError> {
        let AlterColumnDefaultRequest {
            invocation_id,
            authenticated_actor_id,
            table_id,
            column_id,
            default,
            retention_deadline_us,
            mut audit_event,
        } = request;
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || column_id == [0; 16]
            || retention_deadline_us <= 0
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(TableLifecycleError::InvalidAudit);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| TableLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| TableLifecycleError::Storage)?;
        let table_key = id_catalog_key(5, table_id);
        let column_key = id_catalog_key(6, column_id);
        let table_current = catalog
            .get(table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        let column_current = catalog
            .get(column_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        if read_catalog_id(&column_key, &column_current, 6, 1)? != table_id
            || read_catalog_u16(&column_key, &column_current, 6, 8)? != 0
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let type_id = read_catalog_id(&column_key, &column_current, 6, 4)?;
        let type_parameters: [u8; 8] = read_catalog_field(&column_key, &column_current, 6, 5)?
            .as_slice()
            .try_into()
            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let type_key = id_catalog_key(4, type_id);
        let type_bytes = catalog
            .get(type_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let type_record = read_catalog_record(&type_key, &type_bytes, 4)?;
        let scalar_type = decode_builtin_scalar_type(&type_record, &type_parameters)
            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        if let Some(default) = &default {
            let expression = match default {
                TableColumnDefault::Literal(value) => {
                    crate::authorization::Expr::Literal(value.clone())
                }
                TableColumnDefault::CurrentPrincipalId => {
                    crate::authorization::Expr::CurrentPrincipalId
                }
            };
            let default_mask = read_catalog_u32(&type_key, &type_bytes, 4, 5)?;
            let permitted = match &expression {
                crate::authorization::Expr::Literal(_) => default_mask & 1 != 0,
                crate::authorization::Expr::CurrentPrincipalId => {
                    default_mask & 4 != 0 && scalar_type == ScalarType::Uuid
                }
                _ => false,
            };
            if !permitted {
                return Err(TableLifecycleError::InvalidRequest);
            }
            if let crate::authorization::Expr::Literal(value) = &expression
                && crate::tuple::encode_tuple(
                    &[ColumnSchema {
                        scalar_type,
                        nullable: true,
                    }],
                    1,
                    std::slice::from_ref(value),
                )
                .is_err()
            {
                return Err(TableLifecycleError::InvalidRequest);
            }
        }
        let column_record = read_catalog_record(&column_key, &column_current, 6)?;
        let old_default_id = column_record
            .fields
            .iter()
            .find(|field| field.tag == 7)
            .map(|field| {
                field
                    .payload
                    .as_slice()
                    .try_into()
                    .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))
            })
            .transpose()?;
        let ordinal = read_catalog_u32(&column_key, &column_current, 6, 2)? as usize;
        let expression = default.as_ref().map(|default| match default {
            TableColumnDefault::Literal(value) => {
                crate::authorization::Expr::Literal(value.clone())
            }
            TableColumnDefault::CurrentPrincipalId => {
                crate::authorization::Expr::CurrentPrincipalId
            }
        });
        if let (Some(old_id), Some(next_expression)) = (old_default_id, expression.as_ref()) {
            let old_key = id_catalog_key(7, old_id);
            let old_bytes = catalog
                .get(old_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let old = crate::expression::decode_catalog_expression_record(&read_catalog_record(
                &old_key, &old_bytes, 7,
            )?)
            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            if old.expression == *next_expression {
                return Err(TableLifecycleError::InvalidRequest);
            }
        } else if old_default_id.is_none() && default.is_none() {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let next_default_id = if expression.is_some() {
            Some(self.generate_table_object_id()?)
        } else {
            None
        };
        let mut writes = Vec::new();
        let next_epoch = read_catalog_u64(&table_key, &table_current, 5, 3)?
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        writes.push(CatalogWrite {
            key: table_key.clone(),
            expected_value: Some(table_current.clone()),
            payload: Some(rewrite_catalog_u64_field(
                &table_key,
                &table_current,
                5,
                3,
                next_epoch,
            )?),
        });
        writes.push(CatalogWrite {
            key: column_key.clone(),
            expected_value: Some(column_current.clone()),
            payload: Some(rewrite_optional_catalog_id_field(
                &column_key,
                &column_current,
                6,
                7,
                next_default_id,
            )?),
        });
        if let Some(old_id) = old_default_id {
            let expression_key = id_catalog_key(7, old_id);
            let expression_bytes = catalog
                .get(expression_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let object_key = id_catalog_key(2, old_id);
            let object_bytes = catalog
                .get(object_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let dependency_key = dependency_catalog_key(column_id, old_id, 2);
            let dependency_bytes = catalog
                .get(dependency_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            writes.extend([
                CatalogWrite {
                    key: expression_key,
                    expected_value: Some(expression_bytes),
                    payload: None,
                },
                CatalogWrite {
                    key: object_key,
                    expected_value: Some(object_bytes),
                    payload: None,
                },
                CatalogWrite {
                    key: dependency_key,
                    expected_value: Some(dependency_bytes),
                    payload: None,
                },
            ]);
        }
        if let (Some(expression_id), Some(expression)) = (next_default_id, expression) {
            writes.push(new_default_expression_write(
                expression_id,
                type_id,
                type_parameters,
                expression,
            )?);
            writes.push(new_default_expression_object_write(
                expression_id,
                ordinal,
                authenticated_actor_id,
            )?);
            writes.push(new_dependency_write(column_id, expression_id, 2)?);
        }
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![table_id, column_id];
        audit_event.target_ids.sort_unstable();
        self.commit_authorized_constraint_ddl(
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
            error => TableLifecycleError::Commit(error),
        })
    }

    /// Create a table and its ordered column catalog in one authorized DDL
    /// commit. Columns receive no implicit query grants.
    pub fn create_table(
        &self,
        request: CreateTableRequest,
    ) -> Result<CreatedTable, TableLifecycleError> {
        let CreateTableRequest {
            invocation_id,
            authenticated_actor_id,
            schema_id,
            name,
            columns,
            retention_deadline_us,
            mut audit_event,
        } = request;
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || schema_id == [0; 16]
            || retention_deadline_us <= 0
            || !is_normalized_principal_name(&name)
            || columns.is_empty()
            || columns.len() > crate::tuple::MAX_COLUMNS
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(TableLifecycleError::InvalidAudit);
        }
        let mut column_names = HashSet::with_capacity(columns.len());
        for column in &columns {
            if !is_normalized_principal_name(&column.name)
                || !column_names.insert(column.name.as_str())
                || (column.generated_uuid_identity
                    && (column.scalar_type != ScalarType::Uuid
                        || column.nullable
                        || column.default.is_some()))
                || matches!(
                    column.default.as_ref(),
                    Some(TableColumnDefault::CurrentPrincipalId)
                        if column.scalar_type != ScalarType::Uuid
                )
            {
                return Err(TableLifecycleError::InvalidRequest);
            }
            let (nullable, value) = match &column.default {
                Some(TableColumnDefault::Literal(value)) => (column.nullable, value.clone()),
                Some(TableColumnDefault::CurrentPrincipalId) => (
                    column.nullable,
                    Some(crate::tuple::ScalarValue::Uuid([1; 16])),
                ),
                None => (true, None),
            };
            encode_tuple(
                &[ColumnSchema {
                    scalar_type: column.scalar_type,
                    nullable,
                }],
                1,
                &[value],
            )
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
        }

        let type_definitions = load_builtin_type_definitions(self.database)?;
        let table_id = self.generate_table_object_id()?;
        let mut used_ids = HashSet::with_capacity(columns.len() + 1);
        used_ids.insert(table_id);
        let mut column_ids = Vec::with_capacity(columns.len());
        for _ in &columns {
            let mut column_id = self.generate_table_object_id()?;
            if !used_ids.insert(column_id) {
                column_id.zeroize();
                return Err(TableLifecycleError::ObjectIdGenerationFailed);
            }
            column_ids.push(column_id);
        }
        let mut default_expression_ids = Vec::with_capacity(columns.len());
        for column in &columns {
            if column.default.is_some() {
                let mut expression_id = self.generate_table_object_id()?;
                if !used_ids.insert(expression_id) {
                    expression_id.zeroize();
                    return Err(TableLifecycleError::ObjectIdGenerationFailed);
                }
                default_expression_ids.push(Some(expression_id));
            } else {
                default_expression_ids.push(None);
            }
        }

        let default_count = default_expression_ids.iter().flatten().count();
        let mut writes = Vec::with_capacity(3 + columns.len() * 3 + default_count * 3);
        let table_object_key = id_catalog_key(2, table_id);
        writes.push(CatalogWrite {
            key: table_object_key.clone(),
            expected_value: None,
            payload: Some(
                encode_envelope(
                    &family_schema(2).ok_or(TableLifecycleError::Storage)?,
                    &table_object_key,
                    &[
                        CatalogField {
                            tag: 1,
                            wire_type: WireType::U16,
                            payload: 3u16.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 2,
                            wire_type: WireType::Id,
                            payload: schema_id.to_vec(),
                        },
                        CatalogField {
                            tag: 3,
                            wire_type: WireType::Text,
                            payload: name.as_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 4,
                            wire_type: WireType::Id,
                            payload: authenticated_actor_id.to_vec(),
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
                .map_err(|_| TableLifecycleError::InvalidRequest)?,
            ),
        });

        let table_key = id_catalog_key(5, table_id);
        writes.push(CatalogWrite {
            key: table_key.clone(),
            expected_value: None,
            payload: Some(
                encode_envelope(
                    &family_schema(5).ok_or(TableLifecycleError::Storage)?,
                    &table_key,
                    &[
                        CatalogField {
                            tag: 1,
                            wire_type: WireType::Id,
                            payload: schema_id.to_vec(),
                        },
                        CatalogField {
                            tag: 2,
                            wire_type: WireType::U64,
                            payload: 1u64.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 3,
                            wire_type: WireType::U64,
                            payload: 1u64.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 4,
                            wire_type: WireType::Id,
                            payload: table_id.to_vec(),
                        },
                        CatalogField {
                            tag: 5,
                            wire_type: WireType::U16,
                            payload: 1u16.to_le_bytes().to_vec(),
                        },
                    ],
                )
                .map_err(|_| TableLifecycleError::InvalidRequest)?,
            ),
        });
        writes.push(new_dependency_write(table_id, schema_id, 1)?);

        let table_columns = columns
            .iter()
            .map(|column| ColumnSchema {
                scalar_type: column.scalar_type,
                nullable: column.nullable,
            })
            .collect::<Vec<_>>();
        for (ordinal, ((column_id, column), default_expression_id)) in column_ids
            .iter()
            .zip(&columns)
            .zip(&default_expression_ids)
            .enumerate()
        {
            let object_key = id_catalog_key(2, *column_id);
            writes.push(CatalogWrite {
                key: object_key.clone(),
                expected_value: None,
                payload: Some(
                    encode_envelope(
                        &family_schema(2).ok_or(TableLifecycleError::Storage)?,
                        &object_key,
                        &[
                            CatalogField {
                                tag: 1,
                                wire_type: WireType::U16,
                                payload: 4u16.to_le_bytes().to_vec(),
                            },
                            CatalogField {
                                tag: 2,
                                wire_type: WireType::Id,
                                payload: table_id.to_vec(),
                            },
                            CatalogField {
                                tag: 3,
                                wire_type: WireType::Text,
                                payload: column.name.as_bytes().to_vec(),
                            },
                            CatalogField {
                                tag: 4,
                                wire_type: WireType::Id,
                                payload: authenticated_actor_id.to_vec(),
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
                    .map_err(|_| TableLifecycleError::InvalidRequest)?,
                ),
            });
            let type_kind = scalar_type_kind(column.scalar_type);
            let (type_id, type_record) = type_definitions
                .get(&type_kind)
                .ok_or(TableLifecycleError::Storage)?;
            let type_parameters = scalar_type_parameters(column.scalar_type);
            decode_builtin_scalar_type(type_record, &type_parameters)
                .map_err(|_| TableLifecycleError::InvalidRequest)?;
            if let (Some(expression_id), Some(default)) = (default_expression_id, &column.default) {
                let expression = match default {
                    TableColumnDefault::Literal(value) => {
                        crate::authorization::Expr::Literal(value.clone())
                    }
                    TableColumnDefault::CurrentPrincipalId => {
                        crate::authorization::Expr::CurrentPrincipalId
                    }
                };
                let catalog_expression = crate::expression::CatalogExpression {
                    expression_id: *expression_id,
                    purpose: crate::expression::ExpressionPurpose::Default,
                    result_type_id: *type_id,
                    result_parameters: type_parameters,
                    dependencies: Vec::new(),
                    expression: expression.clone(),
                };
                crate::expression::validate_expression_binding(
                    &catalog_expression,
                    &table_columns,
                    &column_ids,
                    *type_id,
                    column.scalar_type,
                )
                .map_err(|_| TableLifecycleError::InvalidRequest)?;
                writes.push(new_default_expression_object_write(
                    *expression_id,
                    ordinal,
                    authenticated_actor_id,
                )?);
                writes.push(new_default_expression_write(
                    *expression_id,
                    *type_id,
                    type_parameters,
                    expression,
                )?);
                writes.push(new_dependency_write(*column_id, *expression_id, 2)?);
            }
            let column_key = id_catalog_key(6, *column_id);
            let mut column_fields = vec![
                CatalogField {
                    tag: 1,
                    wire_type: WireType::Id,
                    payload: table_id.to_vec(),
                },
                CatalogField {
                    tag: 2,
                    wire_type: WireType::U32,
                    payload: (ordinal as u32).to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::Text,
                    payload: column.name.as_bytes().to_vec(),
                },
                CatalogField {
                    tag: 4,
                    wire_type: WireType::Id,
                    payload: type_id.to_vec(),
                },
                CatalogField {
                    tag: 5,
                    wire_type: WireType::Bytes,
                    payload: type_parameters.to_vec(),
                },
                CatalogField {
                    tag: 6,
                    wire_type: WireType::Bool,
                    payload: vec![u8::from(column.nullable)],
                },
            ];
            if let Some(expression_id) = default_expression_id {
                column_fields.push(CatalogField {
                    tag: 7,
                    wire_type: WireType::Id,
                    payload: expression_id.to_vec(),
                });
            }
            column_fields.extend([
                CatalogField {
                    tag: 8,
                    wire_type: WireType::U16,
                    payload: u16::from(column.generated_uuid_identity)
                        .to_le_bytes()
                        .to_vec(),
                },
                CatalogField {
                    tag: 9,
                    wire_type: WireType::U64,
                    payload: 1u64.to_le_bytes().to_vec(),
                },
            ]);
            writes.push(CatalogWrite {
                key: column_key.clone(),
                expected_value: None,
                payload: Some(
                    encode_envelope(
                        &family_schema(6).ok_or(TableLifecycleError::Storage)?,
                        &column_key,
                        &column_fields,
                    )
                    .map_err(|_| TableLifecycleError::InvalidRequest)?,
                ),
            });
            writes.push(new_dependency_write(*column_id, table_id, 1)?);
        }

        audit_event.target_ids = vec![table_id];
        let commit_sequence = commit_authorized_table_create_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
            error => TableLifecycleError::Commit(error),
        })?;
        Ok(CreatedTable {
            table_id,
            column_ids,
            commit_sequence,
        })
    }

    fn generate_table_object_id(&self) -> Result<[u8; 16], TableLifecycleError> {
        for _ in 0..8 {
            let mut candidate = [0; 16];
            if getrandom::fill(&mut candidate).is_err() {
                candidate.zeroize();
                return Err(TableLifecycleError::RandomnessUnavailable);
            }
            if candidate == [0; 16] {
                candidate.zeroize();
                continue;
            }
            let read = self
                .database
                .begin_read()
                .map_err(|_| TableLifecycleError::Storage)?;
            let catalog = read
                .open_table(CATALOG)
                .map_err(|_| TableLifecycleError::Storage)?;
            if catalog
                .get(id_catalog_key(2, candidate).as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .is_none()
                && catalog
                    .get(id_catalog_key(17, candidate).as_slice())
                    .map_err(|_| TableLifecycleError::Storage)?
                    .is_none()
            {
                return Ok(candidate);
            }
            candidate.zeroize();
        }
        Err(TableLifecycleError::ObjectIdGenerationFailed)
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

    /// Drop a user or role only when the authorized identity DDL path finds no
    /// remaining references. User credentials are revoked in the same commit.
    pub fn drop_identity(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        identity_id: [u8; 16],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, IdentityLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || identity_id == [0; 16]
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
        let principal_key = principal_catalog_key(identity_id);
        let object_key = identity_object_catalog_key(identity_id);
        let tombstone_key = identity_tombstone_catalog_key(identity_id);
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
        if catalog
            .get(tombstone_key.as_slice())
            .map_err(|_| IdentityLifecycleError::Storage)?
            .is_some()
        {
            return Err(IdentityLifecycleError::InvalidRequest);
        }
        let principal = decode_registered_envelope(&principal_current)
            .map_err(|_| IdentityLifecycleError::Storage)?;
        let kind = principal
            .fields
            .iter()
            .find(|field| field.tag == 1 && field.wire_type == WireType::U16)
            .and_then(|field| <[u8; 2]>::try_from(field.payload.as_slice()).ok())
            .map(u16::from_le_bytes)
            .filter(|kind| matches!(kind, 1 | 2))
            .ok_or(IdentityLifecycleError::Storage)?;
        let expected_object_kind = if kind == 1 { 8 } else { 9 };
        let object = decode_registered_envelope(&object_current)
            .map_err(|_| IdentityLifecycleError::Storage)?;
        if object
            .fields
            .iter()
            .find(|field| field.tag == 1 && field.wire_type == WireType::U16)
            .and_then(|field| <[u8; 2]>::try_from(field.payload.as_slice()).ok())
            .map(u16::from_le_bytes)
            != Some(expected_object_kind)
        {
            return Err(IdentityLifecycleError::Storage);
        }
        let mut writes = vec![
            CatalogWrite {
                key: principal_key,
                expected_value: Some(principal_current),
                payload: None,
            },
            CatalogWrite {
                key: object_key,
                expected_value: Some(object_current),
                payload: None,
            },
            CatalogWrite {
                key: tombstone_key.clone(),
                expected_value: None,
                payload: Some(
                    encode_envelope(
                        &family_schema(17).ok_or(IdentityLifecycleError::Storage)?,
                        &tombstone_key,
                        &[],
                    )
                    .map_err(|_| IdentityLifecycleError::Storage)?,
                ),
            },
        ];
        if kind == 1 {
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
                if verifier.user_id == identity_id && !verifier.revoked {
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
        audit_event.target_ids = vec![identity_id];
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

    /// Create an FK on existing tables. The final serialized DDL commit
    /// validates source/target column compatibility, REFERENCES and ALTER
    /// authority, and existing rows while fencing both tables.
    pub fn create_foreign_key(
        &self,
        request: CreateForeignKeyRequest,
    ) -> Result<CreatedForeignKey, TableLifecycleError> {
        let CreateForeignKeyRequest {
            invocation_id,
            authenticated_actor_id: authenticated_principal_id,
            source_table_id,
            source_column_ids,
            target_constraint_id,
            retention_deadline_us,
            mut audit_event,
        } = request;
        if invocation_id == [0; 16]
            || authenticated_principal_id == [0; 16]
            || source_table_id == [0; 16]
            || target_constraint_id == [0; 16]
            || source_column_ids.is_empty()
            || source_column_ids.len() > 4096
            || source_column_ids.contains(&[0; 16])
            || source_column_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                .len()
                != source_column_ids.len()
            || retention_deadline_us <= 0
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_principal_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(TableLifecycleError::InvalidAudit);
        }

        let foreign_key_id = self.generate_table_object_id()?;
        let read = self
            .database
            .begin_read()
            .map_err(|_| TableLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| TableLifecycleError::Storage)?;
        let source_table_key = id_catalog_key(5, source_table_id);
        let source_table_bytes = catalog
            .get(source_table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        read_catalog_record(&source_table_key, &source_table_bytes, 5)?;

        let target_constraint_key = id_catalog_key(8, target_constraint_id);
        let target_constraint_bytes = catalog
            .get(target_constraint_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let target_constraint =
            read_catalog_record(&target_constraint_key, &target_constraint_bytes, 8)?;
        if !matches!(
            read_catalog_u16(&target_constraint_key, &target_constraint_bytes, 8, 2)?,
            2 | 3
        ) {
            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
        }
        let target_table_id =
            read_catalog_id(&target_constraint_key, &target_constraint_bytes, 8, 1)?;
        let target_column_ids = target_constraint
            .fields
            .iter()
            .find(|field| field.tag == 3 && field.wire_type == WireType::Ids)
            .map(|field| decode_catalog_id_list(&field.payload))
            .transpose()?
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        if target_column_ids.len() != source_column_ids.len() {
            return Err(TableLifecycleError::InvalidRequest);
        }

        let mut writes = Vec::with_capacity(7);
        let constraint_key = id_catalog_key(8, foreign_key_id);
        let encode_ids = |ids: &[[u8; 16]]| {
            let mut payload = (ids.len() as u32).to_le_bytes().to_vec();
            payload.extend(ids.iter().flatten());
            payload
        };
        let constraint_fields = [
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: source_table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: 5u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Ids,
                payload: encode_ids(&source_column_ids),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::Id,
                payload: target_constraint_id.to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Ids,
                payload: encode_ids(&target_column_ids),
            },
            CatalogField {
                tag: 7,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 8,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 9,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 10,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
        ];
        let constraint_payload = encode_envelope(
            &family_schema(8).ok_or(TableLifecycleError::Storage)?,
            &constraint_key,
            &constraint_fields,
        )
        .map_err(|_| TableLifecycleError::InvalidRequest)?;
        writes.push(CatalogWrite {
            key: constraint_key,
            expected_value: None,
            payload: Some(constraint_payload),
        });

        let source_object_key = id_catalog_key(2, source_table_id);
        let source_object_bytes = catalog
            .get(source_object_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let owner_id = read_catalog_id(&source_object_key, &source_object_bytes, 2, 4)?;
        let object_key = id_catalog_key(2, foreign_key_id);
        let name = format!(
            "foreign_key_{}",
            foreign_key_id
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
        let object_payload = encode_envelope(
            &family_schema(2).ok_or(TableLifecycleError::Storage)?,
            &object_key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::U16,
                    payload: 6u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::Text,
                    payload: name.into_bytes(),
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
            ],
        )
        .map_err(|_| TableLifecycleError::InvalidRequest)?;
        writes.push(CatalogWrite {
            key: object_key,
            expected_value: None,
            payload: Some(object_payload),
        });
        writes.push(new_dependency_write(
            foreign_key_id,
            target_constraint_id,
            3,
        )?);

        let mut table_ids = vec![source_table_id, target_table_id];
        table_ids.sort_unstable();
        table_ids.dedup();
        for table_id in table_ids {
            let table_key = id_catalog_key(5, table_id);
            let table_bytes = catalog
                .get(table_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let epoch = read_catalog_u64(&table_key, &table_bytes, 5, 3)?;
            let next_epoch = epoch
                .checked_add(1)
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            writes.push(CatalogWrite {
                key: table_key.clone(),
                expected_value: Some(table_bytes.clone()),
                payload: Some(rewrite_catalog_u64_field(
                    &table_key,
                    &table_bytes,
                    5,
                    3,
                    next_epoch,
                )?),
            });
        }
        drop(catalog);
        drop(read);

        audit_event.target_ids = vec![foreign_key_id];
        let commit_sequence = self
            .commit_authorized_foreign_key_ddl(
                invocation_id,
                authenticated_principal_id,
                retention_deadline_us,
                &writes,
                &[audit_event],
            )
            .map_err(|error| match error {
                CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
                error => TableLifecycleError::Commit(error),
            })?;
        Ok(CreatedForeignKey {
            foreign_key_id,
            commit_sequence,
        })
    }

    /// Add a UNIQUE constraint and its owned unique index. The authorized
    /// constraint commit backfills and validates the index against the final
    /// table state under the table DDL fence.
    pub fn create_unique_constraint(
        &self,
        request: CreateUniqueConstraintRequest,
    ) -> Result<CreatedUniqueConstraint, TableLifecycleError> {
        self.create_key_constraint(request, 3)
    }

    /// Add a PRIMARY KEY and its owned unique index. Existing rows are
    /// validated and the index is backfilled while the table DDL fence is held.
    pub fn create_primary_key(
        &self,
        request: CreatePrimaryKeyRequest,
    ) -> Result<CreatedPrimaryKey, TableLifecycleError> {
        let created = self.create_key_constraint(
            CreateUniqueConstraintRequest {
                invocation_id: request.invocation_id,
                authenticated_actor_id: request.authenticated_actor_id,
                table_id: request.table_id,
                column_ids: request.column_ids,
                retention_deadline_us: request.retention_deadline_us,
                audit_event: request.audit_event,
            },
            2,
        )?;
        Ok(CreatedPrimaryKey {
            constraint_id: created.constraint_id,
            index_id: created.index_id,
            commit_sequence: created.commit_sequence,
        })
    }

    fn create_key_constraint(
        &self,
        request: CreateUniqueConstraintRequest,
        constraint_kind: u16,
    ) -> Result<CreatedUniqueConstraint, TableLifecycleError> {
        let CreateUniqueConstraintRequest {
            invocation_id,
            authenticated_actor_id,
            table_id,
            column_ids,
            retention_deadline_us,
            mut audit_event,
        } = request;
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || column_ids.is_empty()
            || column_ids.len() > crate::tuple::MAX_COLUMNS
            || column_ids.contains(&[0; 16])
            || column_ids.iter().copied().collect::<HashSet<_>>().len() != column_ids.len()
            || retention_deadline_us <= 0
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(TableLifecycleError::InvalidAudit);
        }

        let constraint_id = self.generate_table_object_id()?;
        let mut index_id = self.generate_table_object_id()?;
        for _ in 0..8 {
            if index_id != constraint_id {
                break;
            }
            index_id = self.generate_table_object_id()?;
        }
        if index_id == constraint_id {
            return Err(TableLifecycleError::ObjectIdGenerationFailed);
        }

        let read = self
            .database
            .begin_read()
            .map_err(|_| TableLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| TableLifecycleError::Storage)?;
        let table_key = id_catalog_key(5, table_id);
        let table_bytes = catalog
            .get(table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        read_catalog_record(&table_key, &table_bytes, 5)?;
        let schema_version = read_catalog_u64(&table_key, &table_bytes, 5, 2)?;
        let table_epoch = read_catalog_u64(&table_key, &table_bytes, 5, 3)?;
        let next_table_epoch = table_epoch
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;

        let table_object_key = id_catalog_key(2, table_id);
        let table_object_bytes = catalog
            .get(table_object_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let owner_id = read_catalog_id(&table_object_key, &table_object_bytes, 2, 4)?;

        let encode_ids = |ids: &[[u8; 16]]| {
            let mut payload = (ids.len() as u32).to_le_bytes().to_vec();
            payload.extend(ids.iter().flatten());
            payload
        };
        let constraint_key = id_catalog_key(8, constraint_id);
        let constraint_fields = [
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: constraint_kind.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Ids,
                payload: encode_ids(&column_ids),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Ids,
                payload: 0u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 7,
                wire_type: WireType::U16,
                payload: 0u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 8,
                wire_type: WireType::U16,
                payload: 0u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 9,
                wire_type: WireType::U16,
                payload: 0u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 10,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 11,
                wire_type: WireType::Id,
                payload: index_id.to_vec(),
            },
        ];
        let constraint_payload = encode_envelope(
            &family_schema(8).ok_or(TableLifecycleError::Storage)?,
            &constraint_key,
            &constraint_fields,
        )
        .map_err(|_| TableLifecycleError::InvalidRequest)?;

        let make_object_write = |object_id: [u8; 16], kind: u16, prefix: &str| {
            let key = id_catalog_key(2, object_id);
            let name = format!(
                "{prefix}_{}",
                object_id
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            );
            let payload = encode_envelope(
                &family_schema(2).ok_or(TableLifecycleError::Storage)?,
                &key,
                &[
                    CatalogField {
                        tag: 1,
                        wire_type: WireType::U16,
                        payload: kind.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 3,
                        wire_type: WireType::Text,
                        payload: name.into_bytes(),
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
                ],
            )
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
            Ok::<_, TableLifecycleError>(CatalogWrite {
                key,
                expected_value: None,
                payload: Some(payload),
            })
        };
        let index_key = id_catalog_key(9, index_id);
        let index_fields = [
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::Ids,
                payload: encode_ids(&column_ids),
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
                payload: index_id.to_vec(),
            },
            CatalogField {
                tag: 8,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 9,
                wire_type: WireType::U32,
                payload: u32::try_from(schema_version)
                    .map_err(|_| TableLifecycleError::InvalidRequest)?
                    .to_le_bytes()
                    .to_vec(),
            },
            CatalogField {
                tag: 10,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
        ];
        let index_payload = encode_envelope(
            &family_schema(9).ok_or(TableLifecycleError::Storage)?,
            &index_key,
            &index_fields,
        )
        .map_err(|_| TableLifecycleError::InvalidRequest)?;
        let writes = [
            CatalogWrite {
                key: constraint_key,
                expected_value: None,
                payload: Some(constraint_payload),
            },
            make_object_write(
                constraint_id,
                6,
                if constraint_kind == 2 {
                    "primary_key_constraint"
                } else {
                    "unique_constraint"
                },
            )?,
            CatalogWrite {
                key: index_key,
                expected_value: None,
                payload: Some(index_payload),
            },
            make_object_write(
                index_id,
                7,
                if constraint_kind == 2 {
                    "primary_key_index"
                } else {
                    "unique_index"
                },
            )?,
            new_dependency_write(constraint_id, index_id, 4)?,
            CatalogWrite {
                key: table_key.clone(),
                expected_value: Some(table_bytes.clone()),
                payload: Some(rewrite_catalog_u64_field(
                    &table_key,
                    &table_bytes,
                    5,
                    3,
                    next_table_epoch,
                )?),
            },
        ];
        drop(catalog);
        drop(read);

        audit_event.target_ids = vec![constraint_id, index_id];
        audit_event.target_ids.sort_unstable();
        let commit_sequence = self
            .commit_authorized_constraint_ddl(
                invocation_id,
                authenticated_actor_id,
                retention_deadline_us,
                &writes,
                &[audit_event],
            )
            .map_err(|error| match error {
                CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
                error => TableLifecycleError::Commit(error),
            })?;
        Ok(CreatedUniqueConstraint {
            constraint_id,
            index_id,
            commit_sequence,
        })
    }

    /// Drop one foreign-key constraint and its dependency edge atomically.
    /// Both participating tables' write epochs advance so invocations pinned
    /// to the old referential graph cannot publish afterward.
    pub fn drop_foreign_key(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        foreign_key_id: [u8; 16],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, CommitError> {
        if invocation_id == [0; 16]
            || authenticated_principal_id == [0; 16]
            || foreign_key_id == [0; 16]
            || retention_deadline_us <= 0
        {
            return Err(CommitError::InvalidWrite);
        }
        if audit_event.actor_id != Some(authenticated_principal_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(CommitError::InvalidAudit);
        }

        let read = self
            .database
            .begin_read()
            .map_err(|error| CommitError::Storage(error.to_string()))?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|error| CommitError::Storage(error.to_string()))?;
        let foreign_key_key = id_catalog_key(8, foreign_key_id);
        let foreign_key_bytes = catalog
            .get(foreign_key_key.as_slice())
            .map_err(|error| CommitError::Storage(error.to_string()))?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        if read_catalog_u16(&foreign_key_key, &foreign_key_bytes, 8, 2)
            .map_err(table_lifecycle_commit_error)?
            != 5
        {
            return Err(CommitError::InvalidCatalog);
        }
        let source_table_id = read_catalog_id(&foreign_key_key, &foreign_key_bytes, 8, 1)
            .map_err(table_lifecycle_commit_error)?;
        let target_constraint_id = read_catalog_id(&foreign_key_key, &foreign_key_bytes, 8, 5)
            .map_err(table_lifecycle_commit_error)?;
        let target_constraint_key = id_catalog_key(8, target_constraint_id);
        let target_constraint_bytes = catalog
            .get(target_constraint_key.as_slice())
            .map_err(|error| CommitError::Storage(error.to_string()))?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        if !matches!(
            read_catalog_u16(&target_constraint_key, &target_constraint_bytes, 8, 2)
                .map_err(table_lifecycle_commit_error)?,
            2 | 3
        ) {
            return Err(CommitError::InvalidCatalog);
        }
        let target_table_id =
            read_catalog_id(&target_constraint_key, &target_constraint_bytes, 8, 1)
                .map_err(table_lifecycle_commit_error)?;

        let object_key = id_catalog_key(2, foreign_key_id);
        let object_bytes = catalog
            .get(object_key.as_slice())
            .map_err(|error| CommitError::Storage(error.to_string()))?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        if read_catalog_u16(&object_key, &object_bytes, 2, 1)
            .map_err(table_lifecycle_commit_error)?
            != 6
        {
            return Err(CommitError::InvalidCatalog);
        }
        let dependency_key = dependency_catalog_key(foreign_key_id, target_constraint_id, 3);
        let dependency_bytes = catalog
            .get(dependency_key.as_slice())
            .map_err(|error| CommitError::Storage(error.to_string()))?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;

        let mut writes = vec![
            CatalogWrite {
                key: foreign_key_key,
                expected_value: Some(foreign_key_bytes),
                payload: None,
            },
            CatalogWrite {
                key: object_key,
                expected_value: Some(object_bytes),
                payload: None,
            },
            CatalogWrite {
                key: dependency_key,
                expected_value: Some(dependency_bytes),
                payload: None,
            },
        ];
        let mut table_ids = vec![source_table_id, target_table_id];
        table_ids.sort_unstable();
        table_ids.dedup();
        for table_id in table_ids {
            let table_key = id_catalog_key(5, table_id);
            let table_bytes = catalog
                .get(table_key.as_slice())
                .map_err(|error| CommitError::Storage(error.to_string()))?
                .map(|value| value.value().to_vec())
                .ok_or(CommitError::InvalidCatalog)?;
            let next_epoch = read_catalog_u64(&table_key, &table_bytes, 5, 3)
                .map_err(table_lifecycle_commit_error)?
                .checked_add(1)
                .ok_or(CommitError::InvalidCatalog)?;
            writes.push(CatalogWrite {
                key: table_key.clone(),
                expected_value: Some(table_bytes.clone()),
                payload: Some(
                    rewrite_catalog_u64_field(&table_key, &table_bytes, 5, 3, next_epoch)
                        .map_err(table_lifecycle_commit_error)?,
                ),
            });
        }
        drop(catalog);
        drop(read);

        audit_event.target_ids = vec![foreign_key_id];
        self.commit_authorized_foreign_key_ddl(
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
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
        if self.requires_reopen_after_commit_error() {
            return Err(std::io::Error::other(
                "database requires reopen after an ambiguous commit error",
            )
            .into());
        }
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
            if self.requires_reopen_after_commit_error() && !live.commit_uncertain {
                return Err(InvocationStatusError::Storage);
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
        if self.requires_reopen_after_commit_error() {
            return Err(InvocationStatusError::Storage);
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

        if self.requires_reopen_after_commit_error() {
            return match self.admissions.status(invocation_id) {
                Some(live) if live.commit_uncertain => Ok(Some(InvocationStatusSnapshot {
                    observed_at_us: now_us,
                    consistency: InvocationStatusConsistency::BestEffort,
                    status: InvocationStatus::Committing {
                        started_at_us: live.started_at_us,
                        deadline_at_us: live.deadline_at_us,
                        cancellation_requested: live.cancellation_reason_code.is_some(),
                    },
                })),
                _ => Err(AdministrativeInvocationListError::Storage),
            };
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
    let storage_error = || {
        mark_database_uncertain(database);
        AdministrativeCancellationError::Storage
    };
    if event.actor_id != Some(administrator_id)
        || event.action != AuditAction::AdministrativeCancellation
        || !matches!(event.result, AuditResult::Accepted | AuditResult::Denied)
    {
        return Err(AdministrativeCancellationError::InvalidAudit);
    }
    let encoded = event
        .encode()
        .map_err(|_| AdministrativeCancellationError::InvalidAudit)?;

    let read = database.begin_read().map_err(|_| storage_error())?;
    let snapshot = RowSnapshot::new(read);
    let is_administrator =
        database_admin_authority(&snapshot, administrator_id).map_err(|error| match error {
            TokenLifecycleError::TokenUnavailable => AdministrativeCancellationError::Unauthorized,
            _ => storage_error(),
        })?;
    if !is_administrator {
        return Err(AdministrativeCancellationError::Unauthorized);
    }

    let membership_records = snapshot
        .scan_catalog_family(11, MAX_CATALOG_FAMILY_SCAN)
        .map_err(|_| storage_error())?;
    let mut guarded_values = HashMap::<Vec<u8>, Vec<u8>>::new();
    let mut principal_ids = HashSet::from([administrator_id]);
    for record in membership_records {
        let key = record.logical_key;
        if key.len() != 36 || key[2] != 1 || key[19] != 1 {
            return Err(storage_error());
        }
        let member_id: [u8; 16] = key[3..19].try_into().map_err(|_| storage_error())?;
        let role_id: [u8; 16] = key[20..36].try_into().map_err(|_| storage_error())?;
        principal_ids.insert(member_id);
        principal_ids.insert(role_id);
        let value = snapshot
            .get_catalog_value(&key)
            .map_err(|_| storage_error())?
            .ok_or_else(&storage_error)?;
        guarded_values.insert(key, value);
    }
    for principal_id in principal_ids {
        let key = principal_catalog_key(principal_id);
        let value = snapshot
            .get_catalog_value(&key)
            .map_err(|_| storage_error())?
            .ok_or_else(&storage_error)?;
        guarded_values.insert(key, value);
    }
    drop(snapshot);

    let mut write = database.begin_write().map_err(|_| storage_error())?;
    write
        .set_durability(redb::Durability::Immediate)
        .map_err(|_| storage_error())?;
    write.set_two_phase_commit(true);
    {
        let catalog = write.open_table(CATALOG).map_err(|_| storage_error())?;
        for (key, expected) in guarded_values {
            let current = catalog
                .get(key.as_slice())
                .map_err(|_| storage_error())?
                .map(|value| value.value().to_vec());
            if current.as_deref() != Some(expected.as_slice()) {
                return Err(AdministrativeCancellationError::Unauthorized);
            }
        }
    }
    {
        let mut audit = write.open_table(AUDIT).map_err(|_| storage_error())?;
        if audit
            .get(event.event_id.as_slice())
            .map_err(|_| storage_error())?
            .is_some()
        {
            return Err(AdministrativeCancellationError::InvalidAudit);
        }
        audit
            .insert(event.event_id.as_slice(), encoded.as_slice())
            .map_err(|_| storage_error())?;
    }
    match write.commit() {
        Ok(()) => Ok(()),
        Err(_) => Err(storage_error()),
    }
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

fn id_catalog_key(family: u16, object_id: [u8; 16]) -> Vec<u8> {
    let mut key = family.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&object_id);
    key
}

fn read_catalog_record(
    key: &[u8],
    bytes: &[u8],
    family: u16,
) -> Result<CatalogRecord, TableLifecycleError> {
    let record = decode_registered_envelope(bytes)
        .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
    if record.family_code != family || record.logical_key != key {
        return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
    }
    Ok(record)
}

fn decode_catalog_id_list(bytes: &[u8]) -> Result<Vec<[u8; 16]>, TableLifecycleError> {
    let invalid = || TableLifecycleError::Commit(CommitError::InvalidCatalog);
    let count_bytes: [u8; 4] = bytes
        .get(..4)
        .ok_or_else(invalid)?
        .try_into()
        .map_err(|_| invalid())?;
    let count = u32::from_le_bytes(count_bytes) as usize;
    let payload_len = count.checked_mul(16).ok_or_else(invalid)?;
    if bytes.len() != 4usize.checked_add(payload_len).ok_or_else(invalid)? {
        return Err(invalid());
    }
    bytes[4..]
        .chunks_exact(16)
        .map(|chunk| chunk.try_into().map_err(|_| invalid()))
        .collect()
}

fn read_catalog_field(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
) -> Result<Vec<u8>, TableLifecycleError> {
    read_catalog_record(key, bytes, family)?
        .fields
        .iter()
        .find(|field| field.tag == tag)
        .map(|field| field.payload.clone())
        .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))
}

fn read_catalog_id(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
) -> Result<[u8; 16], TableLifecycleError> {
    read_catalog_field(key, bytes, family, tag)?
        .as_slice()
        .try_into()
        .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))
}

fn read_catalog_u32(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
) -> Result<u32, TableLifecycleError> {
    let value = read_catalog_field(key, bytes, family, tag)?;
    Ok(u32::from_le_bytes(value.as_slice().try_into().map_err(
        |_| TableLifecycleError::Commit(CommitError::InvalidCatalog),
    )?))
}

fn read_catalog_u16(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
) -> Result<u16, TableLifecycleError> {
    let value = read_catalog_field(key, bytes, family, tag)?;
    Ok(u16::from_le_bytes(value.as_slice().try_into().map_err(
        |_| TableLifecycleError::Commit(CommitError::InvalidCatalog),
    )?))
}

fn read_catalog_u64(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
) -> Result<u64, TableLifecycleError> {
    let value = read_catalog_field(key, bytes, family, tag)?;
    Ok(u64::from_le_bytes(value.as_slice().try_into().map_err(
        |_| TableLifecycleError::Commit(CommitError::InvalidCatalog),
    )?))
}

fn rewrite_catalog_u32_field(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
    value: u32,
) -> Result<Vec<u8>, TableLifecycleError> {
    rewrite_catalog_fields(
        key,
        bytes,
        family,
        &[(tag, WireType::U32, value.to_le_bytes().to_vec())],
    )
}

fn rewrite_catalog_u64_field(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
    value: u64,
) -> Result<Vec<u8>, TableLifecycleError> {
    rewrite_catalog_u64_fields(key, bytes, family, &[(tag, value)])
}

fn rewrite_catalog_u64_fields(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    values: &[(u16, u64)],
) -> Result<Vec<u8>, TableLifecycleError> {
    rewrite_catalog_fields(
        key,
        bytes,
        family,
        &values
            .iter()
            .map(|(tag, value)| (*tag, WireType::U64, value.to_le_bytes().to_vec()))
            .collect::<Vec<_>>(),
    )
}

fn rewrite_catalog_fields(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    values: &[(u16, WireType, Vec<u8>)],
) -> Result<Vec<u8>, TableLifecycleError> {
    let mut record = read_catalog_record(key, bytes, family)?;
    for (tag, wire_type, payload) in values {
        let field = record
            .fields
            .iter_mut()
            .find(|field| field.tag == *tag)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        if field.wire_type != *wire_type {
            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
        }
        field.payload.clone_from(payload);
    }
    encode_envelope(
        &family_schema(family).ok_or(TableLifecycleError::Storage)?,
        key,
        &record.fields,
    )
    .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))
}

fn rewrite_optional_catalog_id_field(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
    value: Option<[u8; 16]>,
) -> Result<Vec<u8>, TableLifecycleError> {
    let mut record = read_catalog_record(key, bytes, family)?;
    if let Some(index) = record.fields.iter().position(|field| field.tag == tag) {
        if record.fields[index].wire_type != WireType::Id {
            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
        }
        match value {
            Some(value) => record.fields[index].payload = value.to_vec(),
            None => {
                record.fields.remove(index);
            }
        }
    } else if let Some(value) = value {
        let index = record
            .fields
            .binary_search_by_key(&tag, |field| field.tag)
            .unwrap_or_else(|index| index);
        record.fields.insert(
            index,
            CatalogField {
                tag,
                wire_type: WireType::Id,
                payload: value.to_vec(),
            },
        );
    }
    encode_envelope(
        &family_schema(family).ok_or(TableLifecycleError::Storage)?,
        key,
        &record.fields,
    )
    .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))
}

fn new_dependency_write(
    dependent_id: [u8; 16],
    referenced_id: [u8; 16],
    kind: u16,
) -> Result<CatalogWrite, TableLifecycleError> {
    let key = dependency_catalog_key(dependent_id, referenced_id, kind);
    let payload = encode_envelope(
        &family_schema(15).ok_or(TableLifecycleError::Storage)?,
        &key,
        &[],
    )
    .map_err(|_| TableLifecycleError::InvalidRequest)?;
    Ok(CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    })
}

fn new_default_expression_object_write(
    expression_id: [u8; 16],
    ordinal: usize,
    owner_id: [u8; 16],
) -> Result<CatalogWrite, TableLifecycleError> {
    let key = id_catalog_key(2, expression_id);
    let payload = encode_envelope(
        &family_schema(2).ok_or(TableLifecycleError::Storage)?,
        &key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::U16,
                payload: 5u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Text,
                payload: format!("default_{ordinal}").into_bytes(),
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
        ],
    )
    .map_err(|_| TableLifecycleError::InvalidRequest)?;
    Ok(CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    })
}

fn new_default_expression_write(
    expression_id: [u8; 16],
    type_id: [u8; 16],
    type_parameters: [u8; 8],
    expression: crate::authorization::Expr,
) -> Result<CatalogWrite, TableLifecycleError> {
    let key = id_catalog_key(7, expression_id);
    let ir = crate::expression::encode_expression(&expression)
        .map_err(|_| TableLifecycleError::InvalidRequest)?;
    let payload = encode_envelope(
        &family_schema(7).ok_or(TableLifecycleError::Storage)?,
        &key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Id,
                payload: type_id.to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Bytes,
                payload: type_parameters.to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::Bytes,
                payload: ir,
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Ids,
                payload: 0u32.to_le_bytes().to_vec(),
            },
        ],
    )
    .map_err(|_| TableLifecycleError::InvalidRequest)?;
    Ok(CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    })
}

fn scalar_type_kind(scalar_type: ScalarType) -> u16 {
    match scalar_type {
        ScalarType::Boolean => 1,
        ScalarType::SmallInt => 2,
        ScalarType::Integer => 3,
        ScalarType::BigInt => 4,
        ScalarType::Decimal { .. } => 5,
        ScalarType::Text => 6,
        ScalarType::Varchar { .. } => 7,
        ScalarType::Bytea => 8,
        ScalarType::Uuid => 9,
        ScalarType::Date => 10,
        ScalarType::TimestampTz => 11,
    }
}

fn scalar_type_parameters(scalar_type: ScalarType) -> [u8; 8] {
    let mut parameters = [0; 8];
    match scalar_type {
        ScalarType::Decimal { precision, scale } => {
            parameters[..2].copy_from_slice(&u16::from(precision).to_le_bytes());
            parameters[2..4].copy_from_slice(&u16::from(scale).to_le_bytes());
        }
        ScalarType::Varchar { max_chars } => {
            parameters[4..].copy_from_slice(&max_chars.to_le_bytes());
        }
        _ => {}
    }
    parameters
}

fn load_builtin_type_definitions(
    database: &Database,
) -> Result<HashMap<u16, ([u8; 16], CatalogRecord)>, TableLifecycleError> {
    let read = database
        .begin_read()
        .map_err(|_| TableLifecycleError::Storage)?;
    let metadata_bytes = read
        .open_table(ENGINE_META)
        .map_err(|_| TableLifecycleError::Storage)?
        .get(&b"format"[..])
        .map_err(|_| TableLifecycleError::Storage)?
        .ok_or(TableLifecycleError::Storage)?
        .value()
        .to_vec();
    let metadata =
        EngineMetadata::decode(&metadata_bytes).map_err(|_| TableLifecycleError::Storage)?;
    if metadata.lifecycle != Lifecycle::Ready || !metadata.supports_current_formats() {
        return Err(TableLifecycleError::Storage);
    }
    let database_key = id_catalog_key(1, metadata.database_id);
    let catalog = read
        .open_table(CATALOG)
        .map_err(|_| TableLifecycleError::Storage)?;
    let database_record = catalog
        .get(database_key.as_slice())
        .map_err(|_| TableLifecycleError::Storage)?
        .map(|value| decode_registered_envelope(value.value()))
        .transpose()
        .map_err(|_| TableLifecycleError::Storage)?
        .ok_or(TableLifecycleError::Storage)?;
    if database_record.family_code != 1 || database_record.logical_key != database_key {
        return Err(TableLifecycleError::Storage);
    }
    let builtins = database_record
        .fields
        .iter()
        .find(|field| field.tag == 3 && field.wire_type == WireType::Bytes)
        .map(|field| field.payload.as_slice())
        .ok_or(TableLifecycleError::Storage)?;
    if builtins.len() < 4 {
        return Err(TableLifecycleError::Storage);
    }
    let count = u32::from_le_bytes(builtins[..4].try_into().unwrap()) as usize;
    if count != 31 || builtins.len() != 4 + count * 20 {
        return Err(TableLifecycleError::Storage);
    }
    let mut type_ids = HashMap::with_capacity(11);
    for entry in builtins[4..].chunks_exact(20) {
        let code = u16::from_le_bytes(entry[..2].try_into().unwrap());
        let reserved = u16::from_le_bytes(entry[2..4].try_into().unwrap());
        if (101..=111).contains(&code) {
            let type_id: [u8; 16] = entry[4..20].try_into().unwrap();
            if reserved != 0 || type_id == [0; 16] || type_ids.insert(code - 100, type_id).is_some()
            {
                return Err(TableLifecycleError::Storage);
            }
        }
    }
    if type_ids.len() != 11 {
        return Err(TableLifecycleError::Storage);
    }
    let mut result = HashMap::with_capacity(11);
    for (kind, type_id) in type_ids {
        let key = id_catalog_key(4, type_id);
        let record = catalog
            .get(key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| decode_registered_envelope(value.value()))
            .transpose()
            .map_err(|_| TableLifecycleError::Storage)?
            .ok_or(TableLifecycleError::Storage)?;
        if record.family_code != 4 || record.logical_key != key {
            return Err(TableLifecycleError::Storage);
        }
        result.insert(kind, (type_id, record));
    }
    Ok(result)
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

#[cfg(test)]
mod commit_cancellation_boundary_tests {
    use super::{OwnerCancellationResult, StorageManager, StorageManagerConfig};
    use redb::Database;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn cancellation_after_committing_transition_is_too_late()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture_id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let directory = std::env::temp_dir().join(format!(
            "prdb-cancel-boundary-{}-{fixture_id}",
            std::process::id()
        ));
        std::fs::create_dir(&directory)?;
        let database = Database::create(directory.join("boundary.redb"))?;
        let storage =
            StorageManager::from_validated_database(&database, StorageManagerConfig::default())?;
        let invocation_id = [0x73; 16];
        let principal_id = [0x74; 16];
        let permit = storage
            .admissions
            .acquire(invocation_id, principal_id, 1_000, Duration::from_secs(60))
            .map_err(|error| {
                std::io::Error::other(format!("unexpected admission error: {error:?}"))
            })?;
        let (transition_sender, transition_receiver) = mpsc::sync_channel(0);
        let (resume_sender, resume_receiver) = mpsc::sync_channel(0);

        std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
            let transition_thread = scope.spawn(move || {
                permit.try_mark_committing_then(|| {
                    transition_sender.send(()).unwrap();
                    resume_receiver.recv().unwrap();
                })
            });
            transition_receiver.recv()?;
            assert_eq!(
                storage.cancel_owned_invocation(principal_id, invocation_id, 1_001)?,
                OwnerCancellationResult::TooLate
            );
            resume_sender.send(())?;
            assert!(
                transition_thread
                    .join()
                    .map_err(|_| std::io::Error::other("commit transition thread panicked"))?
            );
            Ok(())
        })?;

        drop(storage);
        drop(database);
        std::fs::remove_dir_all(directory)?;
        Ok(())
    }
}

#[cfg(test)]
mod worker_storage_lifetime_tests {
    use super::{SupervisorChannelState, SupervisorInvocation, SupervisorInvocationError};
    use std::collections::HashSet;
    use std::os::unix::net::UnixStream;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    #[test]
    fn worker_exit_waits_for_both_channel_handlers_to_drain()
    -> Result<(), Box<dyn std::error::Error>> {
        let invocation = SupervisorInvocation {
            invocation_id: [0x81; 16],
            principal_id: [0x82; 16],
            role_ids: HashSet::new(),
            transaction: Arc::new(Mutex::new(None)),
            channels: Arc::new(Mutex::new(SupervisorChannelState::default())),
            deadline: None,
        };
        let (planner_worker, planner_storage) = UnixStream::pair()?;
        let (executor_worker, executor_storage) = UnixStream::pair()?;

        std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
            let service =
                scope.spawn(|| invocation.serve_worker_channels(planner_storage, executor_storage));
            let active_channel_deadline = Instant::now() + Duration::from_secs(2);
            loop {
                let active = invocation
                    .channels
                    .lock()
                    .map_err(|_| std::io::Error::other("channel state poisoned"))?
                    .active_worker_channels;
                if active == 2 {
                    break;
                }
                assert!(Instant::now() < active_channel_deadline);
                std::thread::yield_now();
            }

            let mut worker = std::process::Command::new("true").spawn()?;
            assert!(matches!(
                invocation.observe_worker_exit(&mut worker),
                Err(SupervisorInvocationError::WorkerChannelsActive)
            ));
            drop(planner_worker);
            drop(executor_worker);
            let service_result = service
                .join()
                .map_err(|_| std::io::Error::other("worker-channel service panicked"))?;
            assert!(
                service_result.is_ok(),
                "worker-channel service failed: {service_result:?}"
            );
            {
                let channels = invocation.channels.lock().unwrap();
                assert_eq!(channels.active_worker_channels, 0);
                assert!(!channels.worker_exit_succeeded);
                assert!(!channels.terminal_requested);
            }
            assert!(worker.wait()?.success());
            let exit_result = invocation.observe_worker_exit(&mut worker);
            assert!(
                exit_result.is_ok(),
                "worker exit observation failed: {exit_result:?}"
            );
            assert!(exit_result?.success());
            Ok(())
        })?;
        Ok(())
    }
}
