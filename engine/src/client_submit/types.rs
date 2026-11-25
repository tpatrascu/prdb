use super::response::lower_hex;
use crate::client_protocol::{ClientFrame, ClientFrameDecoder};
use crate::credentials::TOKEN_SECRET_BYTES;
use std::collections::{HashSet, VecDeque};
use std::io::{self};
use std::net::TcpStream;
use zeroize::Zeroizing;

#[derive(Debug)]
pub enum ClientSubmitError {
    Io(io::Error),
    Frame(crate::client_protocol::ClientFrameError),
    InvalidCredentials,
    InteractiveV2LoopbackOnly,
    InvalidPackage,
    InvalidAdministrativeRequest,
    InvalidInvocationId,
    InvalidMetadataPage,
    InvalidResponse,
    ServerRejected(String),
    OutcomeUncertain([u8; 16]),
    AdministrativeOutcomeUncertain([u8; 16]),
    InteractiveTransactionOutcomeUncertain([u8; 16]),
    InteractiveTransactionBeginOutcomeUncertain,
    InteractivePackageOutcomeUncertain {
        transaction_id: [u8; 16],
        package_id: [u8; 16],
    },
    AdministrativeOperationRejected {
        operation_id: [u8; 16],
        message: String,
    },
}

impl std::fmt::Display for ClientSubmitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AdministrativeOutcomeUncertain(operation_id) => write!(
                formatter,
                "administrative operation outcome is uncertain; operation_id={}",
                lower_hex(operation_id)
            ),
            _ => write!(formatter, "client submission failed: {self:?}"),
        }
    }
}

impl std::error::Error for ClientSubmitError {}

impl From<io::Error> for ClientSubmitError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub(super) fn uncertain_transaction_error(
    error: ClientSubmitError,
    transaction_id: [u8; 16],
) -> ClientSubmitError {
    match error {
        ClientSubmitError::ServerRejected(_) => error,
        _ => ClientSubmitError::InteractiveTransactionOutcomeUncertain(transaction_id),
    }
}

pub(super) fn is_normalized_interactive_username(username: &str) -> bool {
    let bytes = username.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 128
        && (bytes[0].is_ascii_lowercase() || bytes[0] == b'_')
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
}

impl From<crate::client_protocol::ClientFrameError> for ClientSubmitError {
    fn from(error: crate::client_protocol::ClientFrameError) -> Self {
        Self::Frame(error)
    }
}

/// One authenticated query-mode TCP connection for upload, execution, and status.
pub struct SubmissionClient {
    pub(super) stream: TcpStream,
    pub(super) decoder: ClientFrameDecoder,
    pub(super) pending_frames: VecDeque<ClientFrame>,
    pub(super) started_invocations: HashSet<[u8; 16]>,
    pub(super) next_request_id: u64,
    pub(super) max_payload_bytes: usize,
    pub(super) max_control_bytes: usize,
    pub(super) max_chunk_bytes: usize,
    pub(super) protocol_major: u16,
}

/// Authenticated control-mode client for permission-filtered metadata reads.
/// The same transport validates protocol limits and zeroizes AUTH payloads as
/// the query submission client.
pub struct MetadataClient {
    pub(super) transport: SubmissionClient,
}

/// Authenticated control-mode client for typed administrative DDL operations.
/// It is separate from query packages and never exposes admin credentials to
/// native executor code.
pub struct AdminClient {
    pub(super) transport: SubmissionClient,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminIndexResult {
    pub operation_id: [u8; 16],
    pub index_id: [u8; 16],
    pub commit_sequence: u64,
}

/// One immutable SQL SDK release advertised by a database instance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SdkLibraryInfo {
    pub sdk_release: u32,
    pub native_abis: Vec<u8>,
    pub target: String,
    pub cpu_baseline: String,
    pub runtime_profile: String,
    pub soname: String,
    pub sha256: [u8; 32],
    pub needed: Vec<String>,
    pub library_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SdkLibraryOperationStatus {
    Unknown,
    Installed {
        sdk_release: u32,
        sha256: [u8; 32],
        library_bytes: u64,
    },
    Removed {
        sdk_release: u32,
        removed_bytes: u64,
    },
}

/// Result of creating a user or role through the control channel.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminIdentityResult {
    pub operation_id: [u8; 16],
    pub identity_id: [u8; 16],
    pub commit_sequence: u64,
}

/// Result of an administrative operation that does not create an identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminOperationResult {
    pub operation_id: [u8; 16],
    pub commit_sequence: u64,
}

/// One-time token material returned only by a successful token issuance.
/// The secret is zeroized when this value is dropped and is omitted from Debug.
pub struct AdminTokenResult {
    pub operation_id: [u8; 16],
    pub token_id: [u8; 16],
    pub(super) token_secret: Zeroizing<[u8; TOKEN_SECRET_BYTES]>,
    pub commit_sequence: u64,
}

/// Non-secret metadata for one credential returned by the ADMIN token inventory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminTokenMetadata {
    pub token_id: [u8; 16],
    pub label: String,
    pub issued_at_us: i64,
    pub expires_at_us: i64,
    pub revoked: bool,
    pub revoked_at_us: Option<i64>,
}

/// One bounded token inventory page, ordered by token ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminTokenPage {
    pub tokens: Vec<AdminTokenMetadata>,
    pub next_after_token_id: Option<[u8; 16]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminAuditEvent {
    pub event_id: [u8; 16],
    pub actor_id: Option<[u8; 16]>,
    pub correlation_id: [u8; 16],
    pub occurred_at_us: i64,
    pub action: u16,
    pub result: u16,
    pub target_ids: Vec<[u8; 16]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminAuditPage {
    pub events: Vec<AdminAuditEvent>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminColumnDefinition {
    pub name: String,
    pub scalar_type: crate::tuple::ScalarType,
    pub nullable: bool,
    pub generated_uuid_identity: bool,
    pub default: Option<AdminColumnDefault>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminTableForeignKeyDefinition {
    pub source_columns: Vec<String>,
    pub target_constraint_id: [u8; 16],
    /// Present only when this key targets a PRIMARY KEY or UNIQUE declared by
    /// the same CREATE TABLE statement.
    pub target_columns: Option<Vec<String>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminColumnDefault {
    Null,
    Literal(crate::tuple::ScalarValue),
    CurrentPrincipalId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminTableResult {
    pub operation_id: [u8; 16],
    pub table_id: [u8; 16],
    pub column_ids: Vec<[u8; 16]>,
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminTableColumnResult {
    pub operation_id: [u8; 16],
    pub table_id: [u8; 16],
    pub column_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminKeyConstraintResult {
    pub operation_id: [u8; 16],
    pub constraint_id: [u8; 16],
    pub index_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminCheckConstraintResult {
    pub operation_id: [u8; 16],
    pub constraint_id: [u8; 16],
    pub expression_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminPolicyResult {
    pub operation_id: [u8; 16],
    pub policy_id: [u8; 16],
    pub commit_sequence: u64,
}

pub struct AdminCreatePolicyRequest<'a> {
    pub table_id: [u8; 16],
    pub name: &'a str,
    pub kind: AdminPolicyKind,
    pub operation: AdminPolicyOperation,
    pub effect: AdminPolicyEffect,
    pub recipients: &'a [AdminPolicyRecipient],
    pub target_column_ids: &'a [[u8; 16]],
    pub using_expression: Option<&'a crate::authorization::Expr>,
    pub with_check_expression: Option<&'a crate::authorization::Expr>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminPolicyKind {
    Row,
    Cell,
}

impl AdminPolicyKind {
    pub(super) fn as_wire_name(self) -> &'static str {
        match self {
            Self::Row => "row",
            Self::Cell => "cell",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminPolicyOperation {
    Select,
    Insert,
    Update,
    Delete,
}

impl AdminPolicyOperation {
    pub(super) fn as_wire_name(self) -> &'static str {
        match self {
            Self::Select => "select",
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminPolicyEffect {
    Allow,
    Restrict,
}

impl AdminPolicyEffect {
    pub(super) fn as_wire_name(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Restrict => "restrict",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AdminPolicyRecipient {
    User([u8; 16]),
    Role([u8; 16]),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminForeignKeyResult {
    pub operation_id: [u8; 16],
    pub foreign_key_id: [u8; 16],
    pub commit_sequence: u64,
}

/// Database-scoped privileges that an administrator may grant or revoke.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminSystemPrivilege {
    ManageUsers,
    ManageRoles,
    CreateSchema,
}

impl AdminSystemPrivilege {
    pub(super) fn as_wire_name(self) -> &'static str {
        match self {
            Self::ManageUsers => "manage_users",
            Self::ManageRoles => "manage_roles",
            Self::CreateSchema => "create_schema",
        }
    }
}

/// Object-scoped grant values accepted by authenticated ADMIN.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminObjectPrivilege {
    Usage,
    Select,
    Insert,
    Update,
    Delete,
    References,
    Create,
    Alter,
    Index,
    ManagePolicies,
    ManageGrants,
}

impl AdminObjectPrivilege {
    pub(super) fn as_wire_name(self) -> &'static str {
        match self {
            Self::Usage => "usage",
            Self::Select => "select",
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::References => "references",
            Self::Create => "create",
            Self::Alter => "alter",
            Self::Index => "index",
            Self::ManagePolicies => "manage_policies",
            Self::ManageGrants => "manage_grants",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminObjectGrantTarget {
    Object,
    Column([u8; 16]),
    Columns(Vec<[u8; 16]>),
    AllColumns,
}

/// One privilege and its independently scoped grant target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminObjectGrantEntry {
    pub privilege: AdminObjectPrivilege,
    pub target: AdminObjectGrantTarget,
}

impl AdminTokenResult {
    /// Borrow the one-time bearer secret for secure storage or immediate use.
    pub fn token_secret(&self) -> &[u8; TOKEN_SECRET_BYTES] {
        &self.token_secret
    }
}

/// One bounded page from a permission-filtered metadata query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataPage<T> {
    pub records: Vec<T>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeInvocationState {
    Active,
    Committing,
    Committed,
    Aborted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeConsistency {
    BestEffort,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeWorkloadClass {
    Operational,
    Analytical,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeInvocation {
    pub invocation_id: [u8; 16],
    pub transaction_id: [u8; 16],
    pub principal_id: [u8; 16],
    pub workload_class: RuntimeWorkloadClass,
    pub state: RuntimeInvocationState,
    pub started_at_us: Option<u64>,
    pub age_us: u64,
    pub deadline_at_us: Option<u64>,
    pub staged_rows: u64,
    pub staged_value_bytes: u64,
    pub snapshot_pin_count: u64,
    pub cancellation_requested: bool,
    pub cancellation_reason_code: Option<u16>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeInvocationPage {
    pub observed_at_us: u64,
    pub consistency: RuntimeConsistency,
    pub records: Vec<RuntimeInvocation>,
    pub pinned_snapshot_count: usize,
    pub oldest_snapshot_pin_age_us: u64,
    pub invocations_admitted_total: u64,
    pub invocation_conflicts_total: u64,
    pub admin_cancellations_accepted_total: u64,
    pub admin_cancellations_denied_total: u64,
    pub invocation_policy_denials_total: u64,
    pub invocation_staged_value_bytes_total: u64,
    pub invocation_commit_latency_buckets: [u64; 6],
    pub invocation_redb_write_acquisition_buckets: [u64; 6],
    pub outcome_maintenance_passes_total: u64,
    pub outcome_maintenance_failures_total: u64,
    pub outcomes_pruned_total: u64,
    pub fence_waits: Vec<RuntimeFenceWait>,
    pub fence_waits_truncated: bool,
    pub gateway_result_bytes_relayed_total: u64,
    pub gateway_result_bytes_sent_to_clients_total: u64,
    pub supervisor_queue: crate::supervisor::StagingRuntimeUsage,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnInvocation {
    pub invocation_id: [u8; 16],
    pub state: RuntimeInvocationState,
    pub started_at_us: u64,
    pub deadline_at_us: Option<u64>,
    pub cancellation_requested: bool,
    pub staged_rows: u64,
    pub staged_value_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnInvocationPage {
    pub observed_at_us: u64,
    pub consistency: RuntimeConsistency,
    pub records: Vec<OwnInvocation>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFenceWaitCategory {
    Writer,
    Ddl,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFenceWait {
    pub wait_id: [u8; 16],
    pub category: RuntimeFenceWaitCategory,
    pub waited_us: u64,
    pub resource_ids: Vec<[u8; 16]>,
    pub resources_truncated: bool,
    pub blocker_ids: Vec<[u8; 16]>,
    pub blockers_truncated: bool,
}

pub const DEFAULT_METADATA_PAGE_SIZE: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvocationStatus {
    Prepared,
    Active,
    Committing,
    Committed,
    Aborted,
    Unknown,
    Expired,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TerminalOutcome {
    Committed,
    Aborted,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CancellationResult {
    Accepted,
    TooLate,
    Terminal(TerminalOutcome),
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvocationTerminal {
    pub invocation_id: [u8; 16],
    pub outcome: TerminalOutcome,
    pub reason_code: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientResultEvent {
    Data(Vec<u8>),
    Terminal(InvocationTerminal),
    PackageTerminal {
        transaction_id: [u8; 16],
        package_id: [u8; 16],
        state: crate::client_protocol::ClientV2PackageState,
        result_lost: bool,
    },
}

pub struct StartedInvocation<'a> {
    pub(super) client: &'a mut SubmissionClient,
    pub(super) request_id: u64,
    pub(super) next_sequence: u64,
    pub(super) invocation_id: [u8; 16],
    pub(super) interactive_package: Option<([u8; 16], [u8; 16])>,
    pub(super) terminal: bool,
}

pub struct PackageBytes {
    pub artifact: Vec<u8>,
    pub manifest: Vec<u8>,
    pub scope: Vec<u8>,
}
