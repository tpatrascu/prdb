use crate::credentials::TOKEN_SECRET_BYTES;
use crate::transaction::TransactionError;
use zeroize::Zeroize;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientFrame {
    pub kind: u16,
    pub request_id: u64,
    pub response_sequence: u64,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Header {
    pub(super) kind: u16,
    pub(super) payload_bytes: usize,
    pub(super) request_id: u64,
    pub(super) response_sequence: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ClientFrameError {
    InvalidMagic,
    UnsupportedVersion { major: u16, minor: u16 },
    InvalidKind(u16),
    NonzeroFlags,
    PayloadTooLarge,
    InvalidChunkLength,
    InvalidSequence,
    ZeroRequestId,
    AllocationFailed,
    IncompleteFrame,
    RequestIdsNotIncreasing,
    UnexpectedResponseSequence,
    PipelinedRequests,
}

impl std::fmt::Display for ClientFrameError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "client protocol frame error: {self:?}")
    }
}

impl std::error::Error for ClientFrameError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientMode {
    Query,
    Control,
}

#[derive(Debug)]
pub enum ClientSessionError {
    InvalidHello,
    HelloAlreadyAccepted,
    AuthenticationOutOfOrder,
    AlreadyAuthenticated,
    AuthenticationRequired,
    WrongMode,
    UnsupportedRequest,
    ResourceLimit,
    InvalidControlPayload(ClientControlError),
    InteractiveTransaction(crate::storage_manager::InteractiveTransactionError),
    Staging(crate::supervisor::StagingError),
    Status(crate::storage_manager::InvocationStatusError),
    AdministrativeStatus(crate::storage_manager::AdministrativeInvocationListError),
    AdministrativeRuntimeList(crate::storage_manager::AdministrativeInvocationListError),
    AdministrativeAuditList(crate::storage_manager::AdministrativeAuditListError),
    AdministrativeCancellation(crate::storage_manager::AdministrativeCancellationError),
    SdkLibraryRepositoryUnauthorized,
    SdkLibraryRepositoryUnavailable,
    SdkLibraryRepository(crate::sdk_registry::SdkRegistryError),
    TokenInventory(crate::storage_manager::TokenLifecycleError),
    IdentityLifecycle(crate::storage_manager::IdentityLifecycleError),
    MembershipLifecycle(crate::storage_manager::MembershipLifecycleError),
    SystemPrivilegeLifecycle(crate::storage_manager::SystemPrivilegeLifecycleError),
    ObjectGrantLifecycle(crate::commit::CommitError),
    SchemaLifecycle(crate::storage_manager::SchemaLifecycleError),
    TableLifecycle(crate::storage_manager::TableLifecycleError),
    ForeignKeyLifecycle(crate::commit::CommitError),
    ForeignKeyCreateLifecycle(crate::storage_manager::TableLifecycleError),
    ConstraintLifecycle(crate::storage_manager::TableLifecycleError),
    PolicyLifecycle(crate::storage_manager::TableLifecycleError),
    IndexLifecycle(crate::storage_manager::IndexLifecycleError),
    Storage(TransactionError),
}

#[derive(Debug)]
pub enum ClientStartError {
    Session(ClientSessionError),
    Start(crate::supervisor::StartError<TransactionError>),
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    ExecutorStart(
        crate::supervisor::StartError<
            crate::executor_launcher::ExecutorStartError<TransactionError>,
        >,
    ),
}

impl std::fmt::Display for ClientStartError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "client START failed: {self:?}")
    }
}

impl std::error::Error for ClientStartError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientControlError {
    InvalidJson,
    DuplicateKey,
    UnknownKey,
    MissingKey,
    InvalidMode,
    InvalidClientName,
    InvalidTokenId,
    InvalidTokenSecret,
    InvalidChunk,
    InvalidPrepare,
    InvalidInvocationId,
    UnsupportedAdminOperation,
    UnsupportedV2Operation,
    InvalidMetadataCursor,
    InvalidMetadataPageLimit,
}

impl std::fmt::Display for ClientControlError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invalid client control payload: {self:?}")
    }
}

impl std::error::Error for ClientControlError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientHello {
    pub mode: ClientMode,
    pub client_name: String,
    pub repository_sdk_library: bool,
}

#[derive(Eq, PartialEq)]
pub struct ClientCredentials {
    pub token_id: [u8; 16],
    pub token_secret: [u8; TOKEN_SECRET_BYTES],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientChunk {
    pub invocation_id: [u8; 16],
    pub offset: u64,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientPrepare {
    pub manifest_version: u32,
    pub manifest: Vec<u8>,
    pub artifact_bytes: u64,
    pub artifact_sha256: [u8; 32],
    pub repository_sdk_library: bool,
    pub parameter_bytes: u64,
    pub parameter_sha256: [u8; 32],
    pub sql_data_bytes: u64,
    pub sql_data_sha256: [u8; 32],
    pub scope_version: u32,
    pub requested_scope: Vec<u8>,
    pub deadline_ms: u64,
    /// Present together only for protocol v2 packages executing inside an
    /// explicit transaction.
    pub transaction_id: Option<[u8; 16]>,
    pub package_id: Option<[u8; 16]>,
}

pub(super) enum PendingAdminAction {
    CreateUser {
        name: String,
        workload_profile: Option<String>,
    },
    CreateUserNoLogin {
        name: String,
        workload_profile: Option<String>,
    },
    CreateRole {
        name: String,
    },
    CreateSchema {
        name: String,
        owner_id: Option<[u8; 16]>,
    },
    DropSchema {
        schema_id: [u8; 16],
    },
    SetSchemaOwner {
        schema_id: [u8; 16],
        owner_id: [u8; 16],
    },
    RenameSchema {
        schema_id: [u8; 16],
        name: String,
    },
    DropTable {
        table_id: [u8; 16],
    },
    TruncateTable {
        table_id: [u8; 16],
    },
    DropForeignKey {
        foreign_key_id: [u8; 16],
    },
    CreateForeignKey {
        source_table_id: [u8; 16],
        source_column_ids: Vec<[u8; 16]>,
        target_constraint_id: [u8; 16],
        name: Option<String>,
    },
    CreateUniqueConstraint {
        table_id: [u8; 16],
        column_ids: Vec<[u8; 16]>,
        name: Option<String>,
    },
    CreatePrimaryKey {
        table_id: [u8; 16],
        column_ids: Vec<[u8; 16]>,
        name: Option<String>,
    },
    CreateCheckConstraint {
        table_id: [u8; 16],
        name: Option<String>,
        expression: crate::authorization::Expr,
        dependency_column_ids: Vec<[u8; 16]>,
    },
    CreatePolicy {
        replaced_policy_id: Option<[u8; 16]>,
        table_id: [u8; 16],
        name: String,
        kind: crate::storage_manager::PolicyKind,
        operation: crate::authorization::Operation,
        effect: crate::authorization::Effect,
        recipients: Vec<crate::authorization::Subject>,
        target_column_ids: Vec<[u8; 16]>,
        using_expression: Option<crate::authorization::Expr>,
        with_check_expression: Option<crate::authorization::Expr>,
    },
    DropPolicy {
        policy_id: [u8; 16],
    },
    DropCheckConstraint {
        constraint_id: [u8; 16],
    },
    DropPrimaryKey {
        constraint_id: [u8; 16],
    },
    DropUniqueConstraint {
        constraint_id: [u8; 16],
    },
    CreateIndex {
        table_id: [u8; 16],
        name: String,
        column_ids: Vec<[u8; 16]>,
        unique: bool,
    },
    DropIndex {
        index_id: [u8; 16],
    },
    RebuildIndex {
        index_id: [u8; 16],
        column_ids: Vec<[u8; 16]>,
        unique: bool,
    },
    RenameIndex {
        index_id: [u8; 16],
        name: String,
    },
    RenameTable {
        table_id: [u8; 16],
        name: String,
    },
    SetTableOwner {
        table_id: [u8; 16],
        owner_id: [u8; 16],
    },
    RenameColumn {
        table_id: [u8; 16],
        column_id: [u8; 16],
        name: String,
    },
    SetColumnNullable {
        table_id: [u8; 16],
        column_id: [u8; 16],
        nullable: bool,
    },
    WidenVarcharColumn {
        table_id: [u8; 16],
        column_id: [u8; 16],
        max_chars: u32,
    },
    AlterColumnDefault {
        table_id: [u8; 16],
        column_id: [u8; 16],
        default: Option<crate::storage_manager::TableColumnDefault>,
    },
    DropColumn {
        table_id: [u8; 16],
        column_id: [u8; 16],
    },
    AddColumn {
        table_id: [u8; 16],
        column: crate::storage_manager::TableColumnDefinition,
    },
    CreateTable {
        schema_id: [u8; 16],
        name: String,
        columns: Vec<crate::storage_manager::TableColumnDefinition>,
        primary_key_columns: Option<Vec<String>>,
        primary_key_name: Option<String>,
        unique_constraints: Vec<Vec<String>>,
        unique_constraint_names: Vec<Option<String>>,
        check_expressions: Vec<crate::authorization::Expr>,
        check_constraint_names: Vec<Option<String>>,
        foreign_keys: Vec<crate::storage_manager::CreateTableForeignKeyDefinition>,
        foreign_key_names: Vec<Option<String>>,
    },
    RenameIdentity {
        identity_id: [u8; 16],
        name: String,
    },
    DropIdentity {
        identity_id: [u8; 16],
    },
    SetUserLogin {
        user_id: [u8; 16],
        enabled: bool,
    },
    SetUserWorkloadProfile {
        user_id: [u8; 16],
        profile_name: String,
    },
    CreateWorkloadProfile {
        name: String,
        base_profile: Option<String>,
        settings: Vec<(String, u64)>,
    },
    AlterWorkloadProfile {
        name: String,
        settings: Vec<(String, u64)>,
    },
    RenameWorkloadProfile {
        old_name: String,
        new_name: String,
    },
    DropWorkloadProfile {
        name: String,
    },
    ChangeRoleMembership {
        member_id: [u8; 16],
        role_id: [u8; 16],
        grant: bool,
    },
    ChangeSystemPrivilege {
        grantee_id: [u8; 16],
        privilege: crate::storage_manager::AdministrativeSystemPrivilege,
        grant: bool,
    },
    ChangeObjectGrant {
        grantee_id: [u8; 16],
        object_id: [u8; 16],
        privileges: Vec<crate::storage_manager::ObjectGrantPrivilege>,
        target: crate::storage_manager::ObjectGrantTarget,
        entries: Option<Vec<crate::storage_manager::ObjectGrantEntry>>,
        grant: bool,
    },
    Revoke {
        token_id: [u8; 16],
    },
    Issue {
        user_id: [u8; 16],
        label: String,
        expires_at_us: i64,
    },
    Rotate {
        old_token_id: [u8; 16],
        label: String,
        expires_at_us: i64,
    },
}

pub(super) struct PendingAdminOperation {
    pub(super) operation_id: [u8; 16],
    pub(super) action: PendingAdminAction,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum MetadataPageQuery {
    Schemas,
    Relations([u8; 16]),
    Columns {
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    },
    Indexes {
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    },
    Constraints {
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    },
    Grants {
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    },
    Policies {
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    },
    Dependencies([u8; 16]),
}

#[derive(Clone, Debug)]
pub(super) enum MetadataPageRows {
    Schemas(Vec<crate::metadata_discovery::VisibleSchema>),
    Relations(Vec<crate::metadata_discovery::VisibleRelation>),
    Columns(Vec<crate::metadata_discovery::VisibleColumn>),
    Indexes(Vec<crate::metadata_discovery::AdministrativeIndex>),
    Constraints(Vec<crate::metadata_discovery::AdministrativeConstraint>),
    Grants(Vec<crate::metadata_discovery::AdministrativeGrant>),
    Policies(Vec<crate::metadata_discovery::AdministrativePolicy>),
    Dependencies(Vec<crate::metadata_discovery::AdministrativeDependency>),
}

impl MetadataPageRows {
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Schemas(rows) => rows.len(),
            Self::Relations(rows) => rows.len(),
            Self::Columns(rows) => rows.len(),
            Self::Indexes(rows) => rows.len(),
            Self::Constraints(rows) => rows.len(),
            Self::Grants(rows) => rows.len(),
            Self::Policies(rows) => rows.len(),
            Self::Dependencies(rows) => rows.len(),
        }
    }

    pub(super) fn has_same_object_ids(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Schemas(left), Self::Schemas(right)) => left
                .iter()
                .map(|schema| schema.schema_id)
                .eq(right.iter().map(|schema| schema.schema_id)),
            (Self::Relations(left), Self::Relations(right)) => left
                .iter()
                .map(|relation| relation.relation_id)
                .eq(right.iter().map(|relation| relation.relation_id)),
            (Self::Columns(left), Self::Columns(right)) => left
                .iter()
                .map(|column| column.column_id)
                .eq(right.iter().map(|column| column.column_id)),
            (Self::Indexes(left), Self::Indexes(right)) => left
                .iter()
                .map(|index| index.index_id)
                .eq(right.iter().map(|index| index.index_id)),
            (Self::Constraints(left), Self::Constraints(right)) => left == right,
            (Self::Grants(left), Self::Grants(right)) => left == right,
            (Self::Policies(left), Self::Policies(right)) => left == right,
            (Self::Dependencies(left), Self::Dependencies(right)) => left == right,
            _ => false,
        }
    }
}

pub(super) struct ActiveMetadataPageSnapshot {
    pub(super) query: MetadataPageQuery,
    pub(super) rows: MetadataPageRows,
    pub(super) principal_id: [u8; 16],
    pub(super) role_ids: std::collections::HashSet<[u8; 16]>,
    pub(super) page_limit: usize,
    pub(super) next_index: usize,
    pub(super) current_cursor: Option<[u8; 16]>,
    pub(super) expires_at: std::time::Instant,
}

pub(super) struct ActiveSdkLibraryPageSnapshot {
    pub(super) releases: Vec<crate::sdk_registry::SdkReleaseInfo>,
    pub(super) principal_id: [u8; 16],
    pub(super) role_ids: std::collections::HashSet<[u8; 16]>,
    pub(super) page_limit: usize,
    pub(super) next_index: usize,
    pub(super) current_cursor: Option<[u8; 16]>,
    pub(super) expires_at: std::time::Instant,
}

pub(super) struct MetadataPagination {
    pub(super) page_limit: usize,
    pub(super) cursor: Option<[u8; 16]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientAdminRead {
    ResolveIdentityName {
        name: String,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
        policy_scope: Option<([u8; 16], [u8; 16])>,
    },
    ResolveGrantIdentityName {
        schema_name: String,
        table_name: Option<String>,
        name: String,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
    },
    ResolveAlterIdentityName {
        object_id: [u8; 16],
        name: String,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
    },
    ResolvePolicyName {
        schema_id: [u8; 16],
        table_id: [u8; 16],
        name: String,
        is_cell: Option<bool>,
    },
    ListSdkLibraries,
    ListSdkLibrariesPage {
        page_limit: usize,
        cursor: Option<[u8; 16]>,
    },
    ListSchemas,
    ListSchemasPage {
        page_limit: usize,
        cursor: Option<[u8; 16]>,
    },
    ListRelations {
        schema_id: [u8; 16],
    },
    ListRelationsPage {
        schema_id: [u8; 16],
        page_limit: usize,
        cursor: Option<[u8; 16]>,
    },
    ListColumns {
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    },
    ListColumnsPage {
        schema_id: [u8; 16],
        relation_id: [u8; 16],
        page_limit: usize,
        cursor: Option<[u8; 16]>,
    },
    ListIndexesPage {
        schema_id: [u8; 16],
        relation_id: [u8; 16],
        page_limit: usize,
        cursor: Option<[u8; 16]>,
    },
    ListConstraintsPage {
        schema_id: [u8; 16],
        relation_id: [u8; 16],
        page_limit: usize,
        cursor: Option<[u8; 16]>,
    },
    ListGrantsPage {
        schema_id: [u8; 16],
        relation_id: [u8; 16],
        page_limit: usize,
        cursor: Option<[u8; 16]>,
    },
    ListPoliciesPage {
        schema_id: [u8; 16],
        relation_id: [u8; 16],
        page_limit: usize,
        cursor: Option<[u8; 16]>,
    },
    ListDependenciesPage {
        object_id: [u8; 16],
        page_limit: usize,
        cursor: Option<[u8; 16]>,
    },
    ListTokens {
        user_id: [u8; 16],
        after_token_id: Option<[u8; 16]>,
    },
    ListAuditEvents {
        page_limit: usize,
        cursor: Option<crate::storage_manager::AdministrativeAuditCursor>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientAdminCommand {
    ResolveIdentityName {
        name: String,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
        policy_scope: Option<([u8; 16], [u8; 16])>,
    },
    ResolveGrantIdentityName {
        schema_name: String,
        table_name: Option<String>,
        name: String,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
    },
    ResolveAlterIdentityName {
        object_id: [u8; 16],
        name: String,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
    },
    ResolvePolicyName {
        schema_id: [u8; 16],
        table_id: [u8; 16],
        name: String,
        is_cell: Option<bool>,
    },
    ListSdkLibraries,
    ListSdkLibrariesPage {
        page_limit: usize,
        cursor: Option<[u8; 16]>,
    },
    ListSchemas,
    ListSchemasPage {
        page_limit: usize,
        cursor: Option<[u8; 16]>,
    },
    ListRelations {
        schema_id: [u8; 16],
    },
    ListRelationsPage {
        schema_id: [u8; 16],
        page_limit: usize,
        cursor: Option<[u8; 16]>,
    },
    ListColumns {
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    },
    ListColumnsPage {
        schema_id: [u8; 16],
        relation_id: [u8; 16],
        page_limit: usize,
        cursor: Option<[u8; 16]>,
    },
    ListIndexesPage {
        schema_id: [u8; 16],
        relation_id: [u8; 16],
        page_limit: usize,
        cursor: Option<[u8; 16]>,
    },
    ListConstraintsPage {
        schema_id: [u8; 16],
        relation_id: [u8; 16],
        page_limit: usize,
        cursor: Option<[u8; 16]>,
    },
    ListGrantsPage {
        schema_id: [u8; 16],
        relation_id: [u8; 16],
        page_limit: usize,
        cursor: Option<[u8; 16]>,
    },
    ListPoliciesPage {
        schema_id: [u8; 16],
        relation_id: [u8; 16],
        page_limit: usize,
        cursor: Option<[u8; 16]>,
    },
    ListDependenciesPage {
        object_id: [u8; 16],
        page_limit: usize,
        cursor: Option<[u8; 16]>,
    },
    ListTokens {
        user_id: [u8; 16],
        after_token_id: Option<[u8; 16]>,
    },
    ListAuditEvents {
        page_limit: usize,
        cursor: Option<crate::storage_manager::AdministrativeAuditCursor>,
    },
    ListRuntimeInvocations {
        response_version: u64,
        page_limit: usize,
        cursor: Option<crate::storage_manager::AdministrativeInvocationCursor>,
    },
    ListOwnInvocations {
        response_version: u64,
        page_limit: usize,
        cursor: Option<crate::storage_manager::InvocationListCursor>,
    },
    RemoveSdkLibrary {
        operation_id: [u8; 16],
        sdk_release: u32,
    },
    BeginSdkLibraryInstall {
        operation_id: [u8; 16],
        metadata_bytes: u64,
        library_bytes: u64,
        library_sha256: [u8; 32],
    },
    FinishSdkLibraryInstall {
        operation_id: [u8; 16],
    },
    SdkLibraryOperationStatus {
        operation_id: [u8; 16],
    },
    PrepareCreateUser {
        name: String,
        workload_profile: Option<String>,
    },
    PrepareCreateUserNoLogin {
        name: String,
        workload_profile: Option<String>,
    },
    PrepareCreateRole {
        name: String,
    },
    PrepareCreateSchema {
        name: String,
        owner_id: Option<[u8; 16]>,
    },
    PrepareDropSchema {
        schema_id: [u8; 16],
    },
    PrepareSetSchemaOwner {
        schema_id: [u8; 16],
        owner_id: [u8; 16],
    },
    PrepareRenameSchema {
        schema_id: [u8; 16],
        name: String,
    },
    PrepareDropTable {
        table_id: [u8; 16],
    },
    PrepareTruncateTable {
        table_id: [u8; 16],
    },
    PrepareDropForeignKey {
        foreign_key_id: [u8; 16],
    },
    PrepareCreateForeignKey {
        source_table_id: [u8; 16],
        source_column_ids: Vec<[u8; 16]>,
        target_constraint_id: [u8; 16],
        name: Option<String>,
    },
    PrepareCreateUniqueConstraint {
        table_id: [u8; 16],
        column_ids: Vec<[u8; 16]>,
        name: Option<String>,
    },
    PrepareCreatePrimaryKey {
        table_id: [u8; 16],
        column_ids: Vec<[u8; 16]>,
        name: Option<String>,
    },
    PrepareCreateCheckConstraint {
        table_id: [u8; 16],
        name: Option<String>,
        expression: crate::authorization::Expr,
        dependency_column_ids: Vec<[u8; 16]>,
    },
    PrepareCreatePolicy {
        replaced_policy_id: Option<[u8; 16]>,
        table_id: [u8; 16],
        name: String,
        kind: crate::storage_manager::PolicyKind,
        operation: crate::authorization::Operation,
        effect: crate::authorization::Effect,
        recipients: Vec<crate::authorization::Subject>,
        target_column_ids: Vec<[u8; 16]>,
        using_expression: Option<crate::authorization::Expr>,
        with_check_expression: Option<crate::authorization::Expr>,
    },
    PrepareDropPolicy {
        policy_id: [u8; 16],
    },
    PrepareDropCheckConstraint {
        constraint_id: [u8; 16],
    },
    PrepareDropPrimaryKey {
        constraint_id: [u8; 16],
    },
    PrepareDropUniqueConstraint {
        constraint_id: [u8; 16],
    },
    PrepareCreateIndex {
        table_id: [u8; 16],
        name: String,
        column_ids: Vec<[u8; 16]>,
        unique: bool,
    },
    PrepareDropIndex {
        index_id: [u8; 16],
    },
    PrepareRebuildIndex {
        index_id: [u8; 16],
        column_ids: Vec<[u8; 16]>,
        unique: bool,
    },
    PrepareRenameIndex {
        index_id: [u8; 16],
        name: String,
    },
    PrepareRenameTable {
        table_id: [u8; 16],
        name: String,
    },
    PrepareSetTableOwner {
        table_id: [u8; 16],
        owner_id: [u8; 16],
    },
    PrepareRenameColumn {
        table_id: [u8; 16],
        column_id: [u8; 16],
        name: String,
    },
    PrepareSetColumnNullable {
        table_id: [u8; 16],
        column_id: [u8; 16],
        nullable: bool,
    },
    PrepareWidenVarcharColumn {
        table_id: [u8; 16],
        column_id: [u8; 16],
        max_chars: u32,
    },
    PrepareAlterColumnDefault {
        table_id: [u8; 16],
        column_id: [u8; 16],
        default: Option<crate::storage_manager::TableColumnDefault>,
    },
    PrepareDropColumn {
        table_id: [u8; 16],
        column_id: [u8; 16],
    },
    PrepareAddColumn {
        table_id: [u8; 16],
        column: crate::storage_manager::TableColumnDefinition,
    },
    PrepareCreateTable {
        schema_id: [u8; 16],
        name: String,
        columns: Vec<crate::storage_manager::TableColumnDefinition>,
        primary_key_columns: Option<Vec<String>>,
        primary_key_name: Option<String>,
        unique_constraints: Vec<Vec<String>>,
        unique_constraint_names: Vec<Option<String>>,
        check_expressions: Vec<crate::authorization::Expr>,
        check_constraint_names: Vec<Option<String>>,
        foreign_keys: Vec<crate::storage_manager::CreateTableForeignKeyDefinition>,
        foreign_key_names: Vec<Option<String>>,
    },
    PrepareRenameIdentity {
        identity_id: [u8; 16],
        name: String,
    },
    PrepareDropIdentity {
        identity_id: [u8; 16],
    },
    PrepareSetUserLogin {
        user_id: [u8; 16],
        enabled: bool,
    },
    PrepareSetUserWorkloadProfile {
        user_id: [u8; 16],
        profile_name: String,
    },
    PrepareCreateWorkloadProfile {
        name: String,
        base_profile: Option<String>,
        settings: Vec<(String, u64)>,
    },
    PrepareAlterWorkloadProfile {
        name: String,
        settings: Vec<(String, u64)>,
    },
    PrepareRenameWorkloadProfile {
        old_name: String,
        new_name: String,
    },
    PrepareDropWorkloadProfile {
        name: String,
    },
    PrepareGrantRole {
        member_id: [u8; 16],
        role_id: [u8; 16],
    },
    PrepareRevokeRole {
        member_id: [u8; 16],
        role_id: [u8; 16],
    },
    PrepareSystemPrivilege {
        grantee_id: [u8; 16],
        privilege: crate::storage_manager::AdministrativeSystemPrivilege,
        grant: bool,
    },
    PrepareObjectGrant {
        grantee_id: [u8; 16],
        object_id: [u8; 16],
        privileges: Vec<crate::storage_manager::ObjectGrantPrivilege>,
        target: crate::storage_manager::ObjectGrantTarget,
        entries: Option<Vec<crate::storage_manager::ObjectGrantEntry>>,
        grant: bool,
    },
    PrepareRevoke {
        token_id: [u8; 16],
    },
    PrepareIssue {
        user_id: [u8; 16],
        label: String,
        expires_at_us: i64,
    },
    PrepareRotateToken {
        old_token_id: [u8; 16],
        label: String,
        expires_at_us: i64,
    },
    Start {
        operation_id: [u8; 16],
    },
}

impl Drop for ClientCredentials {
    fn drop(&mut self) {
        self.token_secret.zeroize();
    }
}

impl std::fmt::Display for ClientSessionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "client session error: {self:?}")
    }
}

impl std::error::Error for ClientSessionError {}
