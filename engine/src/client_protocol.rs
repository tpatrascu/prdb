//! Bounded framing primitives for client network protocol version 1.0.

use crate::credentials::TOKEN_SECRET_BYTES;
use crate::storage_manager::{AuthenticatedQuerySession, StorageManager};
use crate::supervisor::StagingOwner;
use crate::transaction::TransactionError;
use std::collections::HashMap;
use zeroize::{Zeroize, Zeroizing};

pub const CLIENT_FRAME_HEADER_BYTES: usize = 32;
pub const CLIENT_MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
pub const CLIENT_MAX_CONTROL_BYTES: usize = 64 * 1024;
pub const CLIENT_MAX_CHUNK_BYTES: usize = 256 * 1024;
pub const CLIENT_ERROR_KIND: u16 = 101;
const CLIENT_MAGIC: &[u8; 4] = b"PDBN";
const CLIENT_METADATA_CURSOR_KIND: u8 = 0xd3;
pub const CLIENT_METADATA_MAX_PAGE: usize = 128;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientFrame {
    pub kind: u16,
    pub request_id: u64,
    pub response_sequence: u64,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Header {
    kind: u16,
    payload_bytes: usize,
    request_id: u64,
    response_sequence: u64,
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
    Staging(crate::supervisor::StagingError),
    Status(crate::storage_manager::InvocationStatusError),
    AdministrativeStatus(crate::storage_manager::AdministrativeInvocationListError),
    AdministrativeRuntimeList(crate::storage_manager::AdministrativeInvocationListError),
    AdministrativeCancellation(crate::storage_manager::AdministrativeCancellationError),
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
    pub parameter_bytes: u64,
    pub parameter_sha256: [u8; 32],
    pub scope_version: u32,
    pub requested_scope: Vec<u8>,
    pub deadline_ms: u64,
    pub workload_class: crate::supervisor::WorkloadClass,
}

enum PendingAdminAction {
    CreateUser {
        name: String,
    },
    CreateRole {
        name: String,
    },
    CreateSchema {
        name: String,
        owner_id: [u8; 16],
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
    DropForeignKey {
        foreign_key_id: [u8; 16],
    },
    CreateForeignKey {
        source_table_id: [u8; 16],
        source_column_ids: Vec<[u8; 16]>,
        target_constraint_id: [u8; 16],
    },
    CreateUniqueConstraint {
        table_id: [u8; 16],
        column_ids: Vec<[u8; 16]>,
    },
    CreatePrimaryKey {
        table_id: [u8; 16],
        column_ids: Vec<[u8; 16]>,
    },
    CreateCheckConstraint {
        table_id: [u8; 16],
        expression: crate::authorization::Expr,
        dependency_column_ids: Vec<[u8; 16]>,
    },
    CreatePolicy {
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
        privilege: crate::storage_manager::ObjectGrantPrivilege,
        target: crate::storage_manager::ObjectGrantTarget,
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
}

struct PendingAdminOperation {
    operation_id: [u8; 16],
    action: PendingAdminAction,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum MetadataPageQuery {
    Schemas,
    Relations([u8; 16]),
    Columns {
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    },
}

#[derive(Clone, Debug)]
enum MetadataPageRows {
    Schemas(Vec<crate::metadata_discovery::VisibleSchema>),
    Relations(Vec<crate::metadata_discovery::VisibleRelation>),
    Columns(Vec<crate::metadata_discovery::VisibleColumn>),
}

impl MetadataPageRows {
    fn len(&self) -> usize {
        match self {
            Self::Schemas(rows) => rows.len(),
            Self::Relations(rows) => rows.len(),
            Self::Columns(rows) => rows.len(),
        }
    }

    fn has_same_object_ids(&self, other: &Self) -> bool {
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
            _ => false,
        }
    }
}

struct ActiveMetadataPageSnapshot {
    query: MetadataPageQuery,
    rows: MetadataPageRows,
    principal_id: [u8; 16],
    role_ids: std::collections::HashSet<[u8; 16]>,
    page_limit: usize,
    next_index: usize,
    current_cursor: Option<[u8; 16]>,
    expires_at: std::time::Instant,
}

struct MetadataPagination {
    page_limit: usize,
    cursor: Option<[u8; 16]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientAdminRead {
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
    ListTokens {
        user_id: [u8; 16],
        after_token_id: Option<[u8; 16]>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientAdminCommand {
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
    ListTokens {
        user_id: [u8; 16],
        after_token_id: Option<[u8; 16]>,
    },
    ListRuntimeInvocations {
        page_limit: usize,
        cursor: Option<crate::storage_manager::AdministrativeInvocationCursor>,
    },
    PrepareCreateUser {
        name: String,
    },
    PrepareCreateRole {
        name: String,
    },
    PrepareCreateSchema {
        name: String,
        owner_id: [u8; 16],
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
    PrepareDropForeignKey {
        foreign_key_id: [u8; 16],
    },
    PrepareCreateForeignKey {
        source_table_id: [u8; 16],
        source_column_ids: Vec<[u8; 16]>,
        target_constraint_id: [u8; 16],
    },
    PrepareCreateUniqueConstraint {
        table_id: [u8; 16],
        column_ids: Vec<[u8; 16]>,
    },
    PrepareCreatePrimaryKey {
        table_id: [u8; 16],
        column_ids: Vec<[u8; 16]>,
    },
    PrepareCreateCheckConstraint {
        table_id: [u8; 16],
        expression: crate::authorization::Expr,
        dependency_column_ids: Vec<[u8; 16]>,
    },
    PrepareCreatePolicy {
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
        privilege: crate::storage_manager::ObjectGrantPrivilege,
        target: crate::storage_manager::ObjectGrantTarget,
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
    Start {
        operation_id: [u8; 16],
    },
}

/// Decode the versioned, read-only ADMIN envelope and its first supported operation.
pub fn decode_client_admin_read(payload: &[u8]) -> Result<ClientAdminRead, ClientControlError> {
    match decode_client_admin_command(payload)? {
        ClientAdminCommand::ListSchemas => Ok(ClientAdminRead::ListSchemas),
        ClientAdminCommand::ListSchemasPage { page_limit, cursor } => {
            Ok(ClientAdminRead::ListSchemasPage { page_limit, cursor })
        }
        ClientAdminCommand::ListRelations { schema_id } => {
            Ok(ClientAdminRead::ListRelations { schema_id })
        }
        ClientAdminCommand::ListRelationsPage {
            schema_id,
            page_limit,
            cursor,
        } => Ok(ClientAdminRead::ListRelationsPage {
            schema_id,
            page_limit,
            cursor,
        }),
        ClientAdminCommand::ListColumns {
            schema_id,
            relation_id,
        } => Ok(ClientAdminRead::ListColumns {
            schema_id,
            relation_id,
        }),
        ClientAdminCommand::ListColumnsPage {
            schema_id,
            relation_id,
            page_limit,
            cursor,
        } => Ok(ClientAdminRead::ListColumnsPage {
            schema_id,
            relation_id,
            page_limit,
            cursor,
        }),
        ClientAdminCommand::ListTokens {
            user_id,
            after_token_id,
        } => Ok(ClientAdminRead::ListTokens {
            user_id,
            after_token_id,
        }),
        _ => Err(ClientControlError::UnsupportedAdminOperation),
    }
}

/// Decode the currently supported ADMIN read and credential lifecycle commands.
pub fn decode_client_admin_command(
    payload: &[u8],
) -> Result<ClientAdminCommand, ClientControlError> {
    if payload.len() > CLIENT_MAX_CONTROL_BYTES {
        return Err(ClientControlError::InvalidJson);
    }
    let text = std::str::from_utf8(payload).map_err(|_| ClientControlError::InvalidJson)?;
    let mut parser = JsonStringObjectParser {
        text: text.as_bytes(),
        offset: 0,
    };
    parser.skip_whitespace();
    parser.expect(b'{')?;
    let mut phase = None;
    let mut version = None;
    let mut operation = None;
    let mut arguments = None;
    let mut table_columns = None;
    let mut foreign_key_source_columns = None;
    let mut constraint_column_ids = None;
    let mut index_unique = None;
    let mut check_dependency_column_ids = None;
    let mut policy_recipients = None;
    let mut policy_target_column_ids = None;
    let mut table_column = None;
    let mut column_default = None;
    let mut operation_id = None;
    let mut seen = std::collections::HashSet::new();
    loop {
        parser.skip_whitespace();
        if parser.consume(b'}') {
            break;
        }
        let key = parser.parse_string()?;
        if !seen.insert(key.clone()) {
            return Err(ClientControlError::DuplicateKey);
        }
        parser.skip_whitespace();
        parser.expect(b':')?;
        parser.skip_whitespace();
        match key.as_str() {
            "phase" => phase = Some(parser.parse_string()?),
            "operation_version" => version = Some(parser.parse_u64_number()?),
            "operation" => operation = Some(parser.parse_string()?),
            "operation_id" => operation_id = Some(parser.parse_string()?),
            "arguments" => {
                parser.expect(b'{')?;
                let mut args = HashMap::new();
                loop {
                    parser.skip_whitespace();
                    if parser.consume(b'}') {
                        break;
                    }
                    let arg = parser.parse_string()?;
                    if args.contains_key(&arg) {
                        return Err(ClientControlError::DuplicateKey);
                    }
                    parser.skip_whitespace();
                    parser.expect(b':')?;
                    parser.skip_whitespace();
                    if arg == "columns" && parser.text.get(parser.offset) == Some(&b'[') {
                        table_columns = Some(parser.parse_table_column_array()?);
                        args.insert(arg, "<structured-columns>".to_owned());
                    } else if arg == "source_column_ids"
                        && parser.text.get(parser.offset) == Some(&b'[')
                    {
                        foreign_key_source_columns = Some(parser.parse_id_array()?);
                        args.insert(arg, "<structured-ids>".to_owned());
                    } else if arg == "column_ids" && parser.text.get(parser.offset) == Some(&b'[') {
                        constraint_column_ids = Some(parser.parse_id_array()?);
                        args.insert(arg, "<structured-ids>".to_owned());
                    } else if arg == "dependency_column_ids"
                        && parser.text.get(parser.offset) == Some(&b'[')
                    {
                        check_dependency_column_ids = Some(parser.parse_id_array()?);
                        args.insert(arg, "<structured-ids>".to_owned());
                    } else if arg == "target_column_ids"
                        && parser.text.get(parser.offset) == Some(&b'[')
                    {
                        policy_target_column_ids = Some(parser.parse_id_array()?);
                        args.insert(arg, "<structured-ids>".to_owned());
                    } else if arg == "recipients" && parser.text.get(parser.offset) == Some(&b'[') {
                        policy_recipients = Some(parser.parse_policy_recipient_array()?);
                        args.insert(arg, "<structured-recipients>".to_owned());
                    } else if arg == "unique" {
                        index_unique = Some(parser.parse_boolean()?);
                        args.insert(arg, "<boolean>".to_owned());
                    } else if arg == "column" && parser.text.get(parser.offset) == Some(&b'{') {
                        table_column = Some(parser.parse_table_column_definition()?);
                        args.insert(arg, "<structured-column>".to_owned());
                    } else if arg == "default" && parser.text.get(parser.offset) == Some(&b'{') {
                        column_default = Some(parser.parse_table_column_default()?);
                        args.insert(arg, "<structured-default>".to_owned());
                    } else if matches!(
                        arg.as_str(),
                        "using_expression_eir1" | "with_check_expression_eir1"
                    ) && parser.text.get(parser.offset..parser.offset + 4)
                        == Some(b"null")
                    {
                        parser.offset += 4;
                        args.insert(arg, "<null>".to_owned());
                    } else {
                        args.insert(arg, parser.parse_string()?);
                    }
                    parser.skip_whitespace();
                    if !parser.consume(b',') {
                        parser.expect(b'}')?;
                        break;
                    }
                    parser.skip_whitespace();
                    if parser.text.get(parser.offset) == Some(&b'}') {
                        return Err(ClientControlError::InvalidJson);
                    }
                }
                arguments = Some(args);
            }
            _ => return Err(ClientControlError::UnknownKey),
        }
        parser.skip_whitespace();
        if !parser.consume(b',') {
            parser.expect(b'}')?;
            break;
        }
        parser.skip_whitespace();
        if parser.text.get(parser.offset) == Some(&b'}') {
            return Err(ClientControlError::InvalidJson);
        }
    }
    parser.skip_whitespace();
    if parser.offset != parser.text.len() {
        return Err(ClientControlError::InvalidJson);
    }
    match phase.as_deref() {
        Some("read")
            if version == Some(1) && operation.as_deref() == Some("metadata.schemas.list") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let pagination = decode_metadata_pagination(&mut args)?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(match pagination {
                Some(pagination) => ClientAdminCommand::ListSchemasPage {
                    page_limit: pagination.page_limit,
                    cursor: pagination.cursor,
                },
                None => ClientAdminCommand::ListSchemas,
            })
        }
        Some("read")
            if version == Some(1) && operation.as_deref() == Some("metadata.relations.list") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let schema_id = decode_lower_hex_id(
                &args
                    .remove("schema_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let pagination = decode_metadata_pagination(&mut args)?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(match pagination {
                Some(pagination) => ClientAdminCommand::ListRelationsPage {
                    schema_id,
                    page_limit: pagination.page_limit,
                    cursor: pagination.cursor,
                },
                None => ClientAdminCommand::ListRelations { schema_id },
            })
        }
        Some("read")
            if version == Some(1) && operation.as_deref() == Some("metadata.columns.list") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let schema_id = decode_lower_hex_id(
                &args
                    .remove("schema_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let relation_id = decode_lower_hex_id(
                &args
                    .remove("relation_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let pagination = decode_metadata_pagination(&mut args)?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(match pagination {
                Some(pagination) => ClientAdminCommand::ListColumnsPage {
                    schema_id,
                    relation_id,
                    page_limit: pagination.page_limit,
                    cursor: pagination.cursor,
                },
                None => ClientAdminCommand::ListColumns {
                    schema_id,
                    relation_id,
                },
            })
        }
        Some("read") if version == Some(1) && operation.as_deref() == Some("tokens.list") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let user_id = decode_lower_hex_id(
                &args
                    .remove("user_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let after_token_id = args
                .remove("after_token_id")
                .map(|value| decode_lower_hex_id(&value))
                .transpose()?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::ListTokens {
                user_id,
                after_token_id,
            })
        }
        Some("read")
            if version == Some(1) && operation.as_deref() == Some("runtime.invocations.list") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let page_limit = args.remove("limit").ok_or(ClientControlError::MissingKey)?;
            let page_limit = parse_canonical_positive_i64(&page_limit)
                .and_then(|value| usize::try_from(value).ok())
                .filter(|value| *value <= crate::storage_manager::MAX_ADMIN_INVOCATION_PAGE)
                .ok_or(ClientControlError::InvalidPrepare)?;
            let cursor = args
                .remove("cursor")
                .map(|value| {
                    crate::storage_manager::AdministrativeInvocationCursor::from_wire(&value)
                        .ok_or(ClientControlError::InvalidPrepare)
                })
                .transpose()?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::ListRuntimeInvocations { page_limit, cursor })
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("users.create") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
            if !is_canonical_admin_principal_name(&name) || !args.is_empty() {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareCreateUser { name })
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("roles.create") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
            if !is_canonical_admin_principal_name(&name) || !args.is_empty() {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareCreateRole { name })
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("schemas.create") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
            let owner_id = decode_lower_hex_id(
                &args
                    .remove("owner_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !is_canonical_admin_principal_name(&name) || !args.is_empty() {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareCreateSchema { name, owner_id })
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("schemas.drop") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let schema_id = decode_lower_hex_id(
                &args
                    .remove("schema_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareDropSchema { schema_id })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("schemas.set_owner") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let schema_id = decode_lower_hex_id(
                &args
                    .remove("schema_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let owner_id = decode_lower_hex_id(
                &args
                    .remove("owner_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareSetSchemaOwner {
                schema_id,
                owner_id,
            })
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("schemas.rename") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let schema_id = decode_lower_hex_id(
                &args
                    .remove("schema_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
            if !args.is_empty() || !is_canonical_admin_principal_name(&name) {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareRenameSchema { schema_id, name })
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("tables.drop") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareDropTable { table_id })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("foreign_keys.drop") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let foreign_key_id = decode_lower_hex_id(
                &args
                    .remove("foreign_key_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareDropForeignKey { foreign_key_id })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("foreign_keys.create") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let source_table_id = decode_lower_hex_id(
                &args
                    .remove("source_table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let target_constraint_id = decode_lower_hex_id(
                &args
                    .remove("target_constraint_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            args.remove("source_column_ids")
                .ok_or(ClientControlError::MissingKey)?;
            let source_column_ids = foreign_key_source_columns
                .take()
                .ok_or(ClientControlError::MissingKey)?;
            if source_column_ids.is_empty() || !args.is_empty() {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareCreateForeignKey {
                source_table_id,
                source_column_ids,
                target_constraint_id,
            })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("constraints.create_unique") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            args.remove("column_ids")
                .ok_or(ClientControlError::MissingKey)?;
            let column_ids = constraint_column_ids
                .take()
                .ok_or(ClientControlError::MissingKey)?;
            if column_ids.is_empty() || !args.is_empty() {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareCreateUniqueConstraint {
                table_id,
                column_ids,
            })
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("indexes.create") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
            if args.remove("column_ids").as_deref() != Some("<structured-ids>") {
                return Err(ClientControlError::MissingKey);
            }
            let column_ids = constraint_column_ids
                .take()
                .ok_or(ClientControlError::MissingKey)?;
            if args.remove("unique").as_deref() != Some("<boolean>") || !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            let unique = index_unique.ok_or(ClientControlError::MissingKey)?;
            if !is_canonical_admin_principal_name(&name)
                || column_ids.is_empty()
                || column_ids
                    .iter()
                    .copied()
                    .collect::<std::collections::HashSet<_>>()
                    .len()
                    != column_ids.len()
            {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareCreateIndex {
                table_id,
                name,
                column_ids,
                unique,
            })
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("indexes.drop") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let index_id = decode_lower_hex_id(
                &args
                    .remove("index_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareDropIndex { index_id })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("indexes.rebuild") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let index_id = decode_lower_hex_id(
                &args
                    .remove("index_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if args.remove("column_ids").as_deref() != Some("<structured-ids>") {
                return Err(ClientControlError::MissingKey);
            }
            let column_ids = constraint_column_ids
                .take()
                .ok_or(ClientControlError::MissingKey)?;
            if args.remove("unique").as_deref() != Some("<boolean>") || !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            let unique = index_unique.ok_or(ClientControlError::MissingKey)?;
            if column_ids.is_empty()
                || column_ids
                    .iter()
                    .copied()
                    .collect::<std::collections::HashSet<_>>()
                    .len()
                    != column_ids.len()
            {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareRebuildIndex {
                index_id,
                column_ids,
                unique,
            })
        }
        Some("prepare")
            if version == Some(1)
                && operation.as_deref() == Some("constraints.create_primary_key") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            args.remove("column_ids")
                .ok_or(ClientControlError::MissingKey)?;
            let column_ids = constraint_column_ids
                .take()
                .ok_or(ClientControlError::MissingKey)?;
            if column_ids.is_empty() || !args.is_empty() {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareCreatePrimaryKey {
                table_id,
                column_ids,
            })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("constraints.create_check") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let expression_bytes = decode_lower_hex_bytes(
                &args
                    .remove("expression_eir1")
                    .ok_or(ClientControlError::MissingKey)?,
                crate::expression::MAX_EXPRESSION_BYTES,
            )?;
            let expression = crate::expression::decode_expression(&expression_bytes)
                .map_err(|_| ClientControlError::InvalidPrepare)?;
            args.remove("dependency_column_ids")
                .ok_or(ClientControlError::MissingKey)?;
            let dependency_column_ids = check_dependency_column_ids
                .take()
                .ok_or(ClientControlError::MissingKey)?;
            if !args.is_empty() || !crate::expression::is_row_local_expression(&expression) {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareCreateCheckConstraint {
                table_id,
                expression,
                dependency_column_ids,
            })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("policies.create") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
            let kind = match args
                .remove("kind")
                .ok_or(ClientControlError::MissingKey)?
                .as_str()
            {
                "row" => crate::storage_manager::PolicyKind::Row,
                "cell" => crate::storage_manager::PolicyKind::Cell,
                _ => return Err(ClientControlError::InvalidPrepare),
            };
            let policy_operation = match args
                .remove("policy_operation")
                .ok_or(ClientControlError::MissingKey)?
                .as_str()
            {
                "select" => crate::authorization::Operation::Select,
                "insert" => crate::authorization::Operation::Insert,
                "update" => crate::authorization::Operation::Update,
                "delete" => crate::authorization::Operation::Delete,
                _ => return Err(ClientControlError::InvalidPrepare),
            };
            let effect = match args
                .remove("effect")
                .ok_or(ClientControlError::MissingKey)?
                .as_str()
            {
                "allow" => crate::authorization::Effect::Allow,
                "restrict" => crate::authorization::Effect::Restrict,
                _ => return Err(ClientControlError::InvalidPrepare),
            };
            if args.remove("recipients").as_deref() != Some("<structured-recipients>") {
                return Err(ClientControlError::MissingKey);
            }
            let recipients = policy_recipients
                .take()
                .ok_or(ClientControlError::MissingKey)?;
            if args.remove("target_column_ids").as_deref() != Some("<structured-ids>") {
                return Err(ClientControlError::MissingKey);
            }
            let target_column_ids = policy_target_column_ids
                .take()
                .ok_or(ClientControlError::MissingKey)?;
            let using_expression = decode_optional_policy_expression(
                args.remove("using_expression_eir1")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let with_check_expression = decode_optional_policy_expression(
                args.remove("with_check_expression_eir1")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty()
                || !is_canonical_admin_principal_name(&name)
                || recipients.is_empty()
                || recipients
                    .iter()
                    .copied()
                    .collect::<std::collections::HashSet<_>>()
                    .len()
                    != recipients.len()
                || target_column_ids
                    .iter()
                    .copied()
                    .collect::<std::collections::HashSet<_>>()
                    .len()
                    != target_column_ids.len()
                || (kind == crate::storage_manager::PolicyKind::Row
                    && !target_column_ids.is_empty())
                || (kind == crate::storage_manager::PolicyKind::Cell
                    && (target_column_ids.is_empty()
                        || policy_operation == crate::authorization::Operation::Delete))
                || !match policy_operation {
                    crate::authorization::Operation::Select
                    | crate::authorization::Operation::Delete => {
                        using_expression.is_some() && with_check_expression.is_none()
                    }
                    crate::authorization::Operation::Insert => {
                        using_expression.is_none() && with_check_expression.is_some()
                    }
                    crate::authorization::Operation::Update => {
                        using_expression.is_some() && with_check_expression.is_some()
                    }
                    crate::authorization::Operation::Usage => false,
                }
            {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareCreatePolicy {
                table_id,
                name,
                kind,
                operation: policy_operation,
                effect,
                recipients,
                target_column_ids,
                using_expression,
                with_check_expression,
            })
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("policies.drop") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let policy_id = decode_lower_hex_id(
                &args
                    .remove("policy_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareDropPolicy { policy_id })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("constraints.drop_check") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let constraint_id = decode_lower_hex_id(
                &args
                    .remove("constraint_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareDropCheckConstraint { constraint_id })
        }
        Some("prepare")
            if version == Some(1)
                && matches!(
                    operation.as_deref(),
                    Some("constraints.drop_primary_key" | "constraints.drop_unique")
                ) =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let constraint_id = decode_lower_hex_id(
                &args
                    .remove("constraint_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            if operation.as_deref() == Some("constraints.drop_primary_key") {
                Ok(ClientAdminCommand::PrepareDropPrimaryKey { constraint_id })
            } else {
                Ok(ClientAdminCommand::PrepareDropUniqueConstraint { constraint_id })
            }
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("tables.rename") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
            if !args.is_empty() || !is_canonical_admin_principal_name(&name) {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareRenameTable { table_id, name })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("tables.set_owner") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let owner_id = decode_lower_hex_id(
                &args
                    .remove("owner_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareSetTableOwner { table_id, owner_id })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("tables.rename_column") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let column_id = decode_lower_hex_id(
                &args
                    .remove("column_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
            if !args.is_empty() || !is_canonical_admin_principal_name(&name) {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareRenameColumn {
                table_id,
                column_id,
                name,
            })
        }
        Some("prepare")
            if version == Some(1)
                && operation.as_deref() == Some("tables.alter_column_nullability") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let column_id = decode_lower_hex_id(
                &args
                    .remove("column_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let nullable = match args.remove("nullable").as_deref() {
                Some("true") => true,
                Some("false") => false,
                _ => return Err(ClientControlError::InvalidPrepare),
            };
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareSetColumnNullable {
                table_id,
                column_id,
                nullable,
            })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("tables.set_column_default") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let column_id = decode_lower_hex_id(
                &args
                    .remove("column_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let scalar_type = decode_table_scalar_type(
                &args.remove("type").ok_or(ClientControlError::MissingKey)?,
            )?;
            if args.remove("default").as_deref() != Some("<structured-default>") || !args.is_empty()
            {
                return Err(ClientControlError::InvalidPrepare);
            }
            let (kind, value) = column_default.ok_or(ClientControlError::MissingKey)?;
            let default = decode_table_column_default(&kind, value, scalar_type)?;
            Ok(ClientAdminCommand::PrepareAlterColumnDefault {
                table_id,
                column_id,
                default: Some(default),
            })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("tables.drop_column_default") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let column_id = decode_lower_hex_id(
                &args
                    .remove("column_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareAlterColumnDefault {
                table_id,
                column_id,
                default: None,
            })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("tables.add_column") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if args.remove("column").as_deref() != Some("<structured-column>") || !args.is_empty() {
                return Err(ClientControlError::InvalidPrepare);
            }
            let column = table_column.ok_or(ClientControlError::MissingKey)?;
            if column.generated_uuid_identity
                || (!column.nullable && column.default.is_none())
                || matches!(
                    column.default,
                    Some(crate::storage_manager::TableColumnDefault::CurrentPrincipalId)
                )
            {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareAddColumn { table_id, column })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("tables.drop_column") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let column_id = decode_lower_hex_id(
                &args
                    .remove("column_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareDropColumn {
                table_id,
                column_id,
            })
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("tables.create") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let schema_id = decode_lower_hex_id(
                &args
                    .remove("schema_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
            let columns_value = args
                .remove("columns")
                .ok_or(ClientControlError::MissingKey)?;
            let columns = table_columns.ok_or(ClientControlError::MissingKey)?;
            if columns_value != "<structured-columns>"
                || !is_canonical_admin_principal_name(&name)
                || columns.is_empty()
                || columns.len() > crate::tuple::MAX_COLUMNS
                || !args.is_empty()
            {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareCreateTable {
                schema_id,
                name,
                columns,
            })
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("users.rename") => {
            decode_identity_rename_prepare(arguments, operation_id, seen.len(), "user_id")
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("roles.rename") => {
            decode_identity_rename_prepare(arguments, operation_id, seen.len(), "role_id")
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("users.drop") => {
            decode_identity_drop_prepare(arguments, operation_id, seen.len(), "user_id")
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("roles.drop") => {
            decode_identity_drop_prepare(arguments, operation_id, seen.len(), "role_id")
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("users.login") => {
            decode_user_login_prepare(arguments, operation_id, seen.len())
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("roles.grant") => {
            decode_role_membership_prepare(arguments, operation_id, seen.len(), true)
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("roles.revoke") => {
            decode_role_membership_prepare(arguments, operation_id, seen.len(), false)
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("privileges.grant") =>
        {
            decode_system_privilege_prepare(arguments, operation_id, seen.len(), true)
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("privileges.revoke") =>
        {
            decode_system_privilege_prepare(arguments, operation_id, seen.len(), false)
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("objects.grant") => {
            decode_object_grant_prepare(arguments, operation_id, seen.len(), true)
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("objects.revoke") => {
            decode_object_grant_prepare(arguments, operation_id, seen.len(), false)
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("tokens.revoke") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let token_id = decode_lower_hex_id(
                &args
                    .remove("token_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareRevoke { token_id })
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("tokens.issue") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let user_id = decode_lower_hex_id(
                &args
                    .remove("user_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let label = args.remove("label").ok_or(ClientControlError::MissingKey)?;
            let expires_at_us = args
                .remove("expires_at_us")
                .ok_or(ClientControlError::MissingKey)?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            if label.is_empty() || label.len() > 128 {
                return Err(ClientControlError::InvalidPrepare);
            }
            let expires_at_us = parse_canonical_positive_i64(&expires_at_us)
                .ok_or(ClientControlError::InvalidPrepare)?;
            Ok(ClientAdminCommand::PrepareIssue {
                user_id,
                label,
                expires_at_us,
            })
        }
        Some("start") if version.is_none() && operation.is_none() => {
            if arguments.is_some() || seen.len() != 2 {
                return Err(ClientControlError::UnknownKey);
            }
            let operation_id =
                decode_lower_hex_id(&operation_id.ok_or(ClientControlError::MissingKey)?)?;
            Ok(ClientAdminCommand::Start { operation_id })
        }
        _ => Err(ClientControlError::UnsupportedAdminOperation),
    }
}

fn decode_role_membership_prepare(
    arguments: Option<HashMap<String, String>>,
    operation_id: Option<String>,
    seen_len: usize,
    grant: bool,
) -> Result<ClientAdminCommand, ClientControlError> {
    if operation_id.is_some() || seen_len != 4 {
        return Err(ClientControlError::UnknownKey);
    }
    let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
    let member_id = decode_lower_hex_id(
        &args
            .remove("member_id")
            .ok_or(ClientControlError::MissingKey)?,
    )?;
    let role_id = decode_lower_hex_id(
        &args
            .remove("role_id")
            .ok_or(ClientControlError::MissingKey)?,
    )?;
    if !args.is_empty() || member_id == role_id {
        return Err(ClientControlError::InvalidPrepare);
    }
    Ok(if grant {
        ClientAdminCommand::PrepareGrantRole { member_id, role_id }
    } else {
        ClientAdminCommand::PrepareRevokeRole { member_id, role_id }
    })
}

fn decode_system_privilege_prepare(
    arguments: Option<HashMap<String, String>>,
    operation_id: Option<String>,
    seen_len: usize,
    grant: bool,
) -> Result<ClientAdminCommand, ClientControlError> {
    if operation_id.is_some() || seen_len != 4 {
        return Err(ClientControlError::UnknownKey);
    }
    let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
    let grantee_id = decode_lower_hex_id(
        &args
            .remove("grantee_id")
            .ok_or(ClientControlError::MissingKey)?,
    )?;
    let privilege = match args
        .remove("privilege")
        .ok_or(ClientControlError::MissingKey)?
        .as_str()
    {
        "manage_users" => crate::storage_manager::AdministrativeSystemPrivilege::ManageUsers,
        "manage_roles" => crate::storage_manager::AdministrativeSystemPrivilege::ManageRoles,
        "create_schema" => crate::storage_manager::AdministrativeSystemPrivilege::CreateSchema,
        _ => return Err(ClientControlError::InvalidPrepare),
    };
    if !args.is_empty() {
        return Err(ClientControlError::UnknownKey);
    }
    Ok(ClientAdminCommand::PrepareSystemPrivilege {
        grantee_id,
        privilege,
        grant,
    })
}

fn decode_object_grant_prepare(
    arguments: Option<HashMap<String, String>>,
    operation_id: Option<String>,
    seen_len: usize,
    grant: bool,
) -> Result<ClientAdminCommand, ClientControlError> {
    if operation_id.is_some() || seen_len != 4 {
        return Err(ClientControlError::UnknownKey);
    }
    let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
    let grantee_id = decode_lower_hex_id(
        &args
            .remove("grantee_id")
            .ok_or(ClientControlError::MissingKey)?,
    )?;
    let object_id = decode_lower_hex_id(
        &args
            .remove("object_id")
            .ok_or(ClientControlError::MissingKey)?,
    )?;
    let privilege = match args
        .remove("privilege")
        .ok_or(ClientControlError::MissingKey)?
        .as_str()
    {
        "usage" => crate::storage_manager::ObjectGrantPrivilege::Usage,
        "select" => crate::storage_manager::ObjectGrantPrivilege::Select,
        "insert" => crate::storage_manager::ObjectGrantPrivilege::Insert,
        "update" => crate::storage_manager::ObjectGrantPrivilege::Update,
        "delete" => crate::storage_manager::ObjectGrantPrivilege::Delete,
        "references" => crate::storage_manager::ObjectGrantPrivilege::References,
        "create" => crate::storage_manager::ObjectGrantPrivilege::Create,
        "alter" => crate::storage_manager::ObjectGrantPrivilege::Alter,
        "index" => crate::storage_manager::ObjectGrantPrivilege::Index,
        "manage_policies" => crate::storage_manager::ObjectGrantPrivilege::ManagePolicies,
        "manage_grants" => crate::storage_manager::ObjectGrantPrivilege::ManageGrants,
        _ => return Err(ClientControlError::InvalidPrepare),
    };
    let target_scope = args
        .remove("target_scope")
        .ok_or(ClientControlError::MissingKey)?;
    let target_id = decode_lower_hex_id(
        &args
            .remove("target_id")
            .ok_or(ClientControlError::MissingKey)?,
    )?;
    if !args.is_empty() {
        return Err(ClientControlError::UnknownKey);
    }
    let target = match target_scope.as_str() {
        "object" if target_id == object_id => crate::storage_manager::ObjectGrantTarget::Object,
        "column" => crate::storage_manager::ObjectGrantTarget::Column(target_id),
        _ => return Err(ClientControlError::InvalidPrepare),
    };
    Ok(ClientAdminCommand::PrepareObjectGrant {
        grantee_id,
        object_id,
        privilege,
        target,
        grant,
    })
}

fn decode_optional_policy_expression(
    value: String,
) -> Result<Option<crate::authorization::Expr>, ClientControlError> {
    if value == "<null>" {
        return Ok(None);
    }
    let bytes = decode_lower_hex_bytes(&value, crate::expression::MAX_EXPRESSION_BYTES)?;
    crate::expression::decode_expression(&bytes)
        .map(Some)
        .map_err(|_| ClientControlError::InvalidPrepare)
}

fn decode_identity_rename_prepare(
    arguments: Option<HashMap<String, String>>,
    operation_id: Option<String>,
    seen_len: usize,
    id_field: &str,
) -> Result<ClientAdminCommand, ClientControlError> {
    if operation_id.is_some() || seen_len != 4 {
        return Err(ClientControlError::UnknownKey);
    }
    let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
    let identity_id = decode_lower_hex_id(
        &args
            .remove(id_field)
            .ok_or(ClientControlError::MissingKey)?,
    )?;
    let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
    if !is_canonical_admin_principal_name(&name) || !args.is_empty() {
        return Err(ClientControlError::InvalidPrepare);
    }
    Ok(ClientAdminCommand::PrepareRenameIdentity { identity_id, name })
}

fn decode_user_login_prepare(
    arguments: Option<HashMap<String, String>>,
    operation_id: Option<String>,
    seen_len: usize,
) -> Result<ClientAdminCommand, ClientControlError> {
    if operation_id.is_some() || seen_len != 4 {
        return Err(ClientControlError::UnknownKey);
    }
    let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
    let user_id = decode_lower_hex_id(
        &args
            .remove("user_id")
            .ok_or(ClientControlError::MissingKey)?,
    )?;
    let enabled = match args
        .remove("enabled")
        .ok_or(ClientControlError::MissingKey)?
        .as_str()
    {
        "true" => true,
        "false" => false,
        _ => return Err(ClientControlError::InvalidPrepare),
    };
    if !args.is_empty() {
        return Err(ClientControlError::UnknownKey);
    }
    Ok(ClientAdminCommand::PrepareSetUserLogin { user_id, enabled })
}

fn decode_identity_drop_prepare(
    arguments: Option<HashMap<String, String>>,
    operation_id: Option<String>,
    seen_len: usize,
    id_field: &str,
) -> Result<ClientAdminCommand, ClientControlError> {
    if operation_id.is_some() || seen_len != 4 {
        return Err(ClientControlError::UnknownKey);
    }
    let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
    let identity_id = decode_lower_hex_id(
        &args
            .remove(id_field)
            .ok_or(ClientControlError::MissingKey)?,
    )?;
    if !args.is_empty() {
        return Err(ClientControlError::UnknownKey);
    }
    Ok(ClientAdminCommand::PrepareDropIdentity { identity_id })
}

fn is_canonical_admin_principal_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 128
        && (bytes[0].is_ascii_lowercase() || bytes[0] == b'_')
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
}

fn parse_canonical_positive_i64(value: &str) -> Option<i64> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    value.parse::<i64>().ok().filter(|value| *value > 0)
}

impl Drop for ClientCredentials {
    fn drop(&mut self) {
        self.token_secret.zeroize();
    }
}

/// Decode the exact HELLO v1 JSON object. Keys and values are strictly typed
/// strings; duplicate and unknown keys are rejected.
pub fn decode_client_hello(payload: &[u8]) -> Result<ClientHello, ClientControlError> {
    let mut fields = parse_string_object(payload)?;
    let mode = match take_required(&mut fields, "mode")?.as_str() {
        "query" => ClientMode::Query,
        "control" => ClientMode::Control,
        _ => return Err(ClientControlError::InvalidMode),
    };
    let client_name = take_required(&mut fields, "client_name")?;
    if client_name.len() > 128 {
        return Err(ClientControlError::InvalidClientName);
    }
    reject_unknown_fields(fields)?;
    Ok(ClientHello { mode, client_name })
}

/// Decode the exact AUTH v1 JSON object, requiring a lowercase opaque token
/// ID and canonical padded base64 for the fixed-size bearer secret.
pub fn decode_client_credentials(payload: &[u8]) -> Result<ClientCredentials, ClientControlError> {
    let mut fields = parse_string_object(payload)?;
    let token_id = decode_lower_hex_id(&take_required(&mut fields, "token_id")?)?;
    let secret_bytes = decode_canonical_base64(&take_required(&mut fields, "token_secret")?)?;
    let token_secret = secret_bytes
        .as_slice()
        .try_into()
        .map_err(|_| ClientControlError::InvalidTokenSecret)?;
    reject_unknown_fields(fields)?;
    Ok(ClientCredentials {
        token_id,
        token_secret,
    })
}

/// Decode ARTIFACT_CHUNK/PARAMETER_CHUNK as invocation ID, little-endian
/// contiguous offset, and 1–256 KiB of opaque data.
pub fn decode_client_chunk(payload: &[u8]) -> Result<ClientChunk, ClientControlError> {
    if !(25..=24 + CLIENT_MAX_CHUNK_BYTES).contains(&payload.len()) {
        return Err(ClientControlError::InvalidChunk);
    }
    let invocation_id: [u8; 16] = payload[..16]
        .try_into()
        .map_err(|_| ClientControlError::InvalidChunk)?;
    if invocation_id == [0; 16] {
        return Err(ClientControlError::InvalidChunk);
    }
    let offset = u64::from_le_bytes(
        payload[16..24]
            .try_into()
            .map_err(|_| ClientControlError::InvalidChunk)?,
    );
    let mut data = Vec::new();
    data.try_reserve_exact(payload.len() - 24)
        .map_err(|_| ClientControlError::InvalidChunk)?;
    data.extend_from_slice(&payload[24..]);
    Ok(ClientChunk {
        invocation_id,
        offset,
        data,
    })
}

/// Decode the exact PREPARE JSON schema, validating declared sizes and all
/// canonical representations before allocating decoded metadata buffers.
pub fn decode_client_prepare(payload: &[u8]) -> Result<ClientPrepare, ClientControlError> {
    let mut fields = parse_scalar_object(payload)?;
    let manifest_version = take_number(&mut fields, "manifest_version")?
        .try_into()
        .map_err(|_| ClientControlError::InvalidPrepare)?;
    let manifest_text = take_string(&mut fields, "manifest")?;
    let artifact_bytes = take_decimal_string(&mut fields, "artifact_bytes")?;
    let artifact_sha256 = decode_lower_hex_hash(&take_string(&mut fields, "artifact_sha256")?)?;
    let parameter_bytes = take_decimal_string(&mut fields, "parameter_bytes")?;
    let parameter_sha256 = decode_lower_hex_hash(&take_string(&mut fields, "parameter_sha256")?)?;
    let scope_version = take_number(&mut fields, "scope_version")?
        .try_into()
        .map_err(|_| ClientControlError::InvalidPrepare)?;
    let scope_text = take_string(&mut fields, "requested_scope")?;
    let deadline_ms = take_decimal_string(&mut fields, "deadline_ms")?;
    let workload_class = match take_string(&mut fields, "workload_class")?.as_str() {
        "operational" => crate::supervisor::WorkloadClass::Operational,
        "analytical" => crate::supervisor::WorkloadClass::Analytical,
        _ => return Err(ClientControlError::InvalidPrepare),
    };
    reject_unknown_scalars(fields)?;
    if artifact_bytes == 0
        || artifact_bytes > crate::supervisor::MAX_ARTIFACT_BYTES
        || parameter_bytes > crate::supervisor::MAX_PARAMETER_BYTES
        || deadline_ms == 0
        || manifest_text.len() > crate::supervisor::MAX_MANIFEST_BYTES.div_ceil(3) * 4
        || scope_text.len() > crate::supervisor::MAX_SCOPE_BYTES.div_ceil(3) * 4
    {
        return Err(ClientControlError::InvalidPrepare);
    }
    let manifest = decode_base64_bytes(&manifest_text)
        .filter(|bytes| !bytes.is_empty() && bytes.len() <= crate::supervisor::MAX_MANIFEST_BYTES)
        .ok_or(ClientControlError::InvalidPrepare)?;
    let requested_scope = decode_base64_bytes(&scope_text)
        .filter(|bytes| !bytes.is_empty() && bytes.len() <= crate::supervisor::MAX_SCOPE_BYTES)
        .ok_or(ClientControlError::InvalidPrepare)?;
    Ok(ClientPrepare {
        manifest_version,
        manifest,
        artifact_bytes,
        artifact_sha256,
        parameter_bytes,
        parameter_sha256,
        scope_version,
        requested_scope,
        deadline_ms,
        workload_class,
    })
}

pub fn decode_client_seal(payload: &[u8]) -> Result<[u8; 16], ClientControlError> {
    let mut fields = parse_string_object(payload)?;
    let invocation_id = decode_lower_hex_id(&take_required(&mut fields, "invocation_id")?)
        .map_err(|_| ClientControlError::InvalidInvocationId)?;
    reject_unknown_fields(fields)?;
    Ok(invocation_id)
}

#[derive(Debug)]
enum JsonScalar {
    String(String),
    Number(u64),
}

fn parse_scalar_object(payload: &[u8]) -> Result<HashMap<String, JsonScalar>, ClientControlError> {
    if payload.len() > CLIENT_MAX_CONTROL_BYTES {
        return Err(ClientControlError::InvalidJson);
    }
    let text = std::str::from_utf8(payload).map_err(|_| ClientControlError::InvalidJson)?;
    let mut parser = JsonStringObjectParser {
        text: text.as_bytes(),
        offset: 0,
    };
    parser.skip_whitespace();
    parser.expect(b'{')?;
    let mut fields = HashMap::new();
    parser.skip_whitespace();
    if !parser.consume(b'}') {
        loop {
            parser.skip_whitespace();
            let key = parser.parse_string()?;
            parser.skip_whitespace();
            parser.expect(b':')?;
            parser.skip_whitespace();
            let value = if parser.text.get(parser.offset) == Some(&b'"') {
                JsonScalar::String(parser.parse_string()?)
            } else {
                JsonScalar::Number(parser.parse_u64_number()?)
            };
            if fields.insert(key, value).is_some() {
                return Err(ClientControlError::DuplicateKey);
            }
            parser.skip_whitespace();
            if parser.consume(b'}') {
                break;
            }
            parser.expect(b',')?;
        }
    }
    parser.skip_whitespace();
    if parser.offset != parser.text.len() {
        return Err(ClientControlError::InvalidJson);
    }
    Ok(fields)
}

fn take_string(
    fields: &mut HashMap<String, JsonScalar>,
    key: &str,
) -> Result<String, ClientControlError> {
    match fields.remove(key).ok_or(ClientControlError::MissingKey)? {
        JsonScalar::String(value) => Ok(value),
        JsonScalar::Number(_) => Err(ClientControlError::InvalidPrepare),
    }
}

fn take_number(
    fields: &mut HashMap<String, JsonScalar>,
    key: &str,
) -> Result<u64, ClientControlError> {
    match fields.remove(key).ok_or(ClientControlError::MissingKey)? {
        JsonScalar::Number(value) => Ok(value),
        JsonScalar::String(_) => Err(ClientControlError::InvalidPrepare),
    }
}

fn take_decimal_string(
    fields: &mut HashMap<String, JsonScalar>,
    key: &str,
) -> Result<u64, ClientControlError> {
    let value = take_string(fields, key)?;
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(ClientControlError::InvalidPrepare);
    }
    value
        .parse()
        .map_err(|_| ClientControlError::InvalidPrepare)
}

fn decode_lower_hex_hash(text: &str) -> Result<[u8; 32], ClientControlError> {
    if text.len() != 64 {
        return Err(ClientControlError::InvalidPrepare);
    }
    let mut hash = [0; 32];
    for (index, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        hash[index] = (lower_hex_nibble(pair[0]).ok_or(ClientControlError::InvalidPrepare)? << 4)
            | lower_hex_nibble(pair[1]).ok_or(ClientControlError::InvalidPrepare)?;
    }
    Ok(hash)
}

fn decode_base64_bytes(text: &str) -> Option<Vec<u8>> {
    let input = text.as_bytes();
    if input.is_empty() || !input.len().is_multiple_of(4) {
        return None;
    }
    let mut output = Vec::new();
    output.try_reserve_exact(input.len() / 4 * 3).ok()?;
    for (index, chunk) in input.chunks_exact(4).enumerate() {
        let last = index + 1 == input.len() / 4;
        let a = base64_value(chunk[0])?;
        let b = base64_value(chunk[1])?;
        output.push((a << 2) | (b >> 4));
        match (chunk[2], chunk[3]) {
            (b'=', b'=') if last && b & 0x0f == 0 => (),
            (c, b'=') if last => {
                let c = base64_value(c)?;
                if c & 0x03 != 0 {
                    return None;
                }
                output.push((b << 4) | (c >> 2));
            }
            (c, d) if c != b'=' && d != b'=' => {
                let c = base64_value(c)?;
                let d = base64_value(d)?;
                output.push((b << 4) | (c >> 2));
                output.push((c << 6) | d);
            }
            _ => return None,
        }
    }
    Some(output)
}

fn parse_string_object(payload: &[u8]) -> Result<HashMap<String, String>, ClientControlError> {
    if payload.len() > CLIENT_MAX_CONTROL_BYTES {
        return Err(ClientControlError::InvalidJson);
    }
    let text = std::str::from_utf8(payload).map_err(|_| ClientControlError::InvalidJson)?;
    let mut parser = JsonStringObjectParser {
        text: text.as_bytes(),
        offset: 0,
    };
    parser.skip_whitespace();
    parser.expect(b'{')?;
    parser.skip_whitespace();
    let mut fields = HashMap::new();
    if parser.consume(b'}') {
        parser.skip_whitespace();
        return if parser.offset == parser.text.len() {
            Ok(fields)
        } else {
            Err(ClientControlError::InvalidJson)
        };
    }
    loop {
        parser.skip_whitespace();
        let key = parser.parse_string()?;
        parser.skip_whitespace();
        parser.expect(b':')?;
        parser.skip_whitespace();
        let value = parser.parse_string()?;
        if fields.insert(key, value).is_some() {
            return Err(ClientControlError::DuplicateKey);
        }
        parser.skip_whitespace();
        if parser.consume(b'}') {
            parser.skip_whitespace();
            return if parser.offset == parser.text.len() {
                Ok(fields)
            } else {
                Err(ClientControlError::InvalidJson)
            };
        }
        parser.expect(b',')?;
    }
}

pub(crate) struct JsonStringObjectParser<'a> {
    pub(crate) text: &'a [u8],
    pub(crate) offset: usize,
}

impl JsonStringObjectParser<'_> {
    pub(crate) fn skip_whitespace(&mut self) {
        while self
            .text
            .get(self.offset)
            .is_some_and(|byte| matches!(byte, b' ' | b'\n' | b'\r' | b'\t'))
        {
            self.offset += 1;
        }
    }

    pub(crate) fn consume(&mut self, expected: u8) -> bool {
        if self.text.get(self.offset) == Some(&expected) {
            self.offset += 1;
            true
        } else {
            false
        }
    }

    pub(crate) fn expect(&mut self, expected: u8) -> Result<(), ClientControlError> {
        self.consume(expected)
            .then_some(())
            .ok_or(ClientControlError::InvalidJson)
    }

    pub(crate) fn parse_u64_number(&mut self) -> Result<u64, ClientControlError> {
        let start = self.offset;
        while self.text.get(self.offset).is_some_and(u8::is_ascii_digit) {
            self.offset += 1;
        }
        let digits = self
            .text
            .get(start..self.offset)
            .ok_or(ClientControlError::InvalidJson)?;
        if digits.is_empty() || (digits.len() > 1 && digits[0] == b'0') {
            return Err(ClientControlError::InvalidJson);
        }
        std::str::from_utf8(digits)
            .map_err(|_| ClientControlError::InvalidJson)?
            .parse()
            .map_err(|_| ClientControlError::InvalidJson)
    }

    pub(crate) fn parse_string(&mut self) -> Result<String, ClientControlError> {
        self.expect(b'"')?;
        let mut output = Vec::new();
        loop {
            let byte = *self
                .text
                .get(self.offset)
                .ok_or(ClientControlError::InvalidJson)?;
            self.offset += 1;
            match byte {
                b'"' => {
                    return String::from_utf8(output).map_err(|_| ClientControlError::InvalidJson);
                }
                b'\\' => {
                    let escaped = *self
                        .text
                        .get(self.offset)
                        .ok_or(ClientControlError::InvalidJson)?;
                    self.offset += 1;
                    match escaped {
                        b'"' | b'\\' | b'/' => output.push(escaped),
                        b'b' => output.push(0x08),
                        b'f' => output.push(0x0c),
                        b'n' => output.push(b'\n'),
                        b'r' => output.push(b'\r'),
                        b't' => output.push(b'\t'),
                        b'u' => {
                            let first = self.parse_hex_quad()?;
                            let scalar = if (0xd800..=0xdbff).contains(&first) {
                                self.expect(b'\\')?;
                                self.expect(b'u')?;
                                let second = self.parse_hex_quad()?;
                                if !(0xdc00..=0xdfff).contains(&second) {
                                    return Err(ClientControlError::InvalidJson);
                                }
                                0x1_0000
                                    + (((first as u32 - 0xd800) << 10) | (second as u32 - 0xdc00))
                            } else if (0xdc00..=0xdfff).contains(&first) {
                                return Err(ClientControlError::InvalidJson);
                            } else {
                                first as u32
                            };
                            let character =
                                char::from_u32(scalar).ok_or(ClientControlError::InvalidJson)?;
                            let mut encoded = [0; 4];
                            output
                                .extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
                        }
                        _ => return Err(ClientControlError::InvalidJson),
                    }
                }
                0x00..=0x1f => return Err(ClientControlError::InvalidJson),
                other => output.push(other),
            }
        }
    }

    pub(crate) fn parse_boolean(&mut self) -> Result<bool, ClientControlError> {
        if self.text[self.offset..].starts_with(b"true") {
            self.offset += 4;
            Ok(true)
        } else if self.text[self.offset..].starts_with(b"false") {
            self.offset += 5;
            Ok(false)
        } else {
            Err(ClientControlError::InvalidJson)
        }
    }

    fn parse_table_column_array(
        &mut self,
    ) -> Result<Vec<crate::storage_manager::TableColumnDefinition>, ClientControlError> {
        self.expect(b'[')?;
        self.skip_whitespace();
        let mut columns = Vec::new();
        if self.consume(b']') {
            return Ok(columns);
        }
        loop {
            if columns.len() >= crate::tuple::MAX_COLUMNS {
                return Err(ClientControlError::InvalidPrepare);
            }
            columns.push(self.parse_table_column_definition()?);
            self.skip_whitespace();
            if self.consume(b']') {
                break;
            }
            self.expect(b',')?;
        }
        Ok(columns)
    }

    fn parse_id_array(&mut self) -> Result<Vec<[u8; 16]>, ClientControlError> {
        self.expect(b'[')?;
        self.skip_whitespace();
        let mut ids = Vec::new();
        if self.consume(b']') {
            return Ok(ids);
        }
        loop {
            if ids.len() >= crate::tuple::MAX_COLUMNS {
                return Err(ClientControlError::InvalidPrepare);
            }
            ids.push(decode_lower_hex_id(&self.parse_string()?)?);
            self.skip_whitespace();
            if self.consume(b']') {
                break;
            }
            self.expect(b',')?;
        }
        Ok(ids)
    }

    fn parse_policy_recipient_array(
        &mut self,
    ) -> Result<Vec<crate::authorization::Subject>, ClientControlError> {
        self.expect(b'[')?;
        self.skip_whitespace();
        let mut recipients = Vec::new();
        if self.consume(b']') {
            return Ok(recipients);
        }
        loop {
            if recipients.len() >= crate::authorization::MAX_POLICY_SUBJECTS {
                return Err(ClientControlError::InvalidPrepare);
            }
            let value = self.parse_string()?;
            let (kind, id) = value
                .split_once(':')
                .ok_or(ClientControlError::InvalidPrepare)?;
            let id = decode_lower_hex_id(id)?;
            recipients.push(match kind {
                "user" => crate::authorization::Subject::Principal(id),
                "role" => crate::authorization::Subject::Role(id),
                _ => return Err(ClientControlError::InvalidPrepare),
            });
            self.skip_whitespace();
            if self.consume(b']') {
                break;
            }
            self.expect(b',')?;
        }
        Ok(recipients)
    }

    fn parse_table_column_definition(
        &mut self,
    ) -> Result<crate::storage_manager::TableColumnDefinition, ClientControlError> {
        self.expect(b'{')?;
        let mut fields = std::collections::HashSet::new();
        let mut name = None;
        let mut scalar_type = None;
        let mut nullable = None;
        let mut generated_uuid_identity = None;
        let mut default = None;
        self.skip_whitespace();
        if self.consume(b'}') {
            return Err(ClientControlError::InvalidPrepare);
        }
        loop {
            self.skip_whitespace();
            let key = self.parse_string()?;
            if !fields.insert(key.clone()) {
                return Err(ClientControlError::DuplicateKey);
            }
            self.skip_whitespace();
            self.expect(b':')?;
            self.skip_whitespace();
            match key.as_str() {
                "name" => name = Some(self.parse_string()?),
                "type" => scalar_type = Some(decode_table_scalar_type(&self.parse_string()?)?),
                "nullable" => nullable = Some(self.parse_boolean()?),
                "generated_uuid_identity" => generated_uuid_identity = Some(self.parse_boolean()?),
                "default" => default = Some(self.parse_table_column_default()?),
                _ => return Err(ClientControlError::UnknownKey),
            }
            self.skip_whitespace();
            if self.consume(b'}') {
                break;
            }
            self.expect(b',')?;
        }
        let name = name.ok_or(ClientControlError::MissingKey)?;
        let scalar_type = scalar_type.ok_or(ClientControlError::MissingKey)?;
        let generated_uuid_identity =
            generated_uuid_identity.ok_or(ClientControlError::MissingKey)?;
        if generated_uuid_identity && default.is_some() {
            return Err(ClientControlError::InvalidPrepare);
        }
        Ok(crate::storage_manager::TableColumnDefinition {
            name,
            scalar_type,
            nullable: nullable.ok_or(ClientControlError::MissingKey)?,
            generated_uuid_identity,
            default: default
                .map(|(kind, value)| decode_table_column_default(&kind, value, scalar_type))
                .transpose()?,
        })
    }

    fn parse_table_column_default(
        &mut self,
    ) -> Result<(String, Option<String>), ClientControlError> {
        self.expect(b'{')?;
        let mut fields = std::collections::HashSet::new();
        let mut kind = None;
        let mut value = None;
        self.skip_whitespace();
        if self.consume(b'}') {
            return Err(ClientControlError::InvalidPrepare);
        }
        loop {
            self.skip_whitespace();
            let key = self.parse_string()?;
            if !fields.insert(key.clone()) {
                return Err(ClientControlError::DuplicateKey);
            }
            self.skip_whitespace();
            self.expect(b':')?;
            self.skip_whitespace();
            match key.as_str() {
                "kind" => kind = Some(self.parse_string()?),
                "value" => value = Some(self.parse_string()?),
                _ => return Err(ClientControlError::UnknownKey),
            }
            self.skip_whitespace();
            if self.consume(b'}') {
                break;
            }
            self.expect(b',')?;
        }
        let kind = kind.ok_or(ClientControlError::MissingKey)?;
        if (kind == "literal" && (fields.len() != 2 || value.is_none()))
            || (kind != "literal" && (fields.len() != 1 || value.is_some()))
        {
            return Err(ClientControlError::InvalidPrepare);
        }
        Ok((kind, value))
    }

    fn parse_hex_quad(&mut self) -> Result<u16, ClientControlError> {
        let end = self
            .offset
            .checked_add(4)
            .ok_or(ClientControlError::InvalidJson)?;
        let digits = self
            .text
            .get(self.offset..end)
            .ok_or(ClientControlError::InvalidJson)?;
        let mut value = 0u16;
        for digit in digits {
            value = value
                .checked_mul(16)
                .and_then(|value| value.checked_add(hex_nibble(*digit)? as u16))
                .ok_or(ClientControlError::InvalidJson)?;
        }
        self.offset = end;
        Ok(value)
    }
}

pub(crate) fn decode_table_scalar_type(
    text: &str,
) -> Result<crate::tuple::ScalarType, ClientControlError> {
    use crate::tuple::ScalarType;
    let invalid = || ClientControlError::InvalidPrepare;
    let parameter = |value: &str| -> Result<u32, ClientControlError> {
        if value.is_empty()
            || (value.len() > 1 && value.starts_with('0'))
            || !value.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(invalid());
        }
        value.parse().map_err(|_| invalid())
    };
    let scalar_type = match text {
        "boolean" => ScalarType::Boolean,
        "smallint" => ScalarType::SmallInt,
        "integer" => ScalarType::Integer,
        "bigint" => ScalarType::BigInt,
        "text" => ScalarType::Text,
        "bytea" => ScalarType::Bytea,
        "uuid" => ScalarType::Uuid,
        "date" => ScalarType::Date,
        "timestamptz" => ScalarType::TimestampTz,
        _ if text.starts_with("decimal(") && text.ends_with(')') => {
            let params = &text[8..text.len() - 1];
            let (precision, scale) = params.split_once(',').ok_or_else(invalid)?;
            if scale.contains(',') {
                return Err(invalid());
            }
            let precision = parameter(precision)?;
            let scale = parameter(scale)?;
            if !(1..=38).contains(&precision) || scale > precision {
                return Err(invalid());
            }
            ScalarType::Decimal {
                precision: precision as u8,
                scale: scale as u8,
            }
        }
        _ if text.starts_with("varchar(") && text.ends_with(')') => {
            let max_chars = parameter(&text[8..text.len() - 1])?;
            if max_chars == 0 {
                return Err(invalid());
            }
            ScalarType::Varchar { max_chars }
        }
        _ => return Err(invalid()),
    };
    Ok(scalar_type)
}

fn decode_table_column_default(
    kind: &str,
    value: Option<String>,
    scalar_type: crate::tuple::ScalarType,
) -> Result<crate::storage_manager::TableColumnDefault, ClientControlError> {
    use crate::storage_manager::TableColumnDefault;
    use crate::tuple::{ScalarType, ScalarValue};
    let invalid = || ClientControlError::InvalidPrepare;
    match (kind, value) {
        ("null", None) => Ok(TableColumnDefault::Literal(None)),
        ("current_principal_id", None) if scalar_type == ScalarType::Uuid => {
            Ok(TableColumnDefault::CurrentPrincipalId)
        }
        ("literal", Some(text)) => {
            let value = match scalar_type {
                ScalarType::Boolean => ScalarValue::Boolean(match text.as_str() {
                    "true" => true,
                    "false" => false,
                    _ => return Err(invalid()),
                }),
                ScalarType::SmallInt => ScalarValue::SmallInt(parse_default_integer(&text)?),
                ScalarType::Integer => ScalarValue::Integer(parse_default_integer(&text)?),
                ScalarType::BigInt => ScalarValue::BigInt(parse_default_integer(&text)?),
                ScalarType::Decimal { scale, .. } => {
                    ScalarValue::Decimal(parse_default_decimal(&text, scale)?)
                }
                ScalarType::Text => ScalarValue::Text(text),
                ScalarType::Varchar { .. } => ScalarValue::Varchar(text),
                ScalarType::Bytea => ScalarValue::Bytea(decode_default_hex(&text)?),
                ScalarType::Uuid => {
                    ScalarValue::Uuid(decode_lower_hex_id(&text).map_err(|_| invalid())?)
                }
                ScalarType::Date => ScalarValue::Date(parse_default_integer(&text)?),
                ScalarType::TimestampTz => ScalarValue::TimestampTz(parse_default_integer(&text)?),
            };
            Ok(TableColumnDefault::Literal(Some(value)))
        }
        _ => Err(invalid()),
    }
}

fn parse_default_integer<T>(text: &str) -> Result<T, ClientControlError>
where
    T: std::str::FromStr,
{
    let digits = text.strip_prefix('-').unwrap_or(text);
    if digits.is_empty()
        || (digits.len() > 1 && digits.starts_with('0'))
        || text == "-0"
        || !digits.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(ClientControlError::InvalidPrepare);
    }
    text.parse().map_err(|_| ClientControlError::InvalidPrepare)
}

fn parse_default_decimal(text: &str, scale: u8) -> Result<i128, ClientControlError> {
    let negative = text.starts_with('-');
    let unsigned = text.strip_prefix('-').unwrap_or(text);
    let (whole, fraction) = match unsigned.split_once('.') {
        Some(parts) => parts,
        None if scale == 0 => (unsigned, ""),
        None => return Err(ClientControlError::InvalidPrepare),
    };
    if whole.is_empty()
        || (whole.len() > 1 && whole.starts_with('0'))
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.len() != scale as usize
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        || text == "-0"
        || (negative && whole == "0" && fraction.bytes().all(|byte| byte == b'0'))
    {
        return Err(ClientControlError::InvalidPrepare);
    }
    let digits = format!("{whole}{fraction}");
    let magnitude = digits
        .parse::<i128>()
        .map_err(|_| ClientControlError::InvalidPrepare)?;
    if negative {
        magnitude
            .checked_neg()
            .ok_or(ClientControlError::InvalidPrepare)
    } else {
        Ok(magnitude)
    }
}

fn decode_default_hex(text: &str) -> Result<Vec<u8>, ClientControlError> {
    if !text.len().is_multiple_of(2)
        || !text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ClientControlError::InvalidPrepare);
    }
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_nibble(pair[0]).ok_or(ClientControlError::InvalidPrepare)?;
            let low = hex_nibble(pair[1]).ok_or(ClientControlError::InvalidPrepare)?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn take_required(
    fields: &mut HashMap<String, String>,
    key: &str,
) -> Result<String, ClientControlError> {
    fields.remove(key).ok_or(ClientControlError::MissingKey)
}

fn reject_unknown_fields(fields: HashMap<String, String>) -> Result<(), ClientControlError> {
    if fields.is_empty() {
        Ok(())
    } else {
        Err(ClientControlError::UnknownKey)
    }
}

fn reject_unknown_scalars(fields: HashMap<String, JsonScalar>) -> Result<(), ClientControlError> {
    if fields.is_empty() {
        Ok(())
    } else {
        Err(ClientControlError::UnknownKey)
    }
}

fn decode_lower_hex_id(text: &str) -> Result<[u8; 16], ClientControlError> {
    if text.len() != 32 {
        return Err(ClientControlError::InvalidTokenId);
    }
    let mut id = [0; 16];
    for (index, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        let high = lower_hex_nibble(pair[0]).ok_or(ClientControlError::InvalidTokenId)?;
        let low = lower_hex_nibble(pair[1]).ok_or(ClientControlError::InvalidTokenId)?;
        id[index] = (high << 4) | low;
    }
    if id == [0; 16] {
        return Err(ClientControlError::InvalidTokenId);
    }
    Ok(id)
}

fn decode_lower_hex_bytes(text: &str, max_bytes: usize) -> Result<Vec<u8>, ClientControlError> {
    if text.is_empty() || !text.len().is_multiple_of(2) || text.len() / 2 > max_bytes {
        return Err(ClientControlError::InvalidPrepare);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(text.len() / 2)
        .map_err(|_| ClientControlError::InvalidPrepare)?;
    for pair in text.as_bytes().chunks_exact(2) {
        let high = lower_hex_nibble(pair[0]).ok_or(ClientControlError::InvalidPrepare)?;
        let low = lower_hex_nibble(pair[1]).ok_or(ClientControlError::InvalidPrepare)?;
        bytes.push((high << 4) | low);
    }
    Ok(bytes)
}

fn decode_metadata_pagination(
    arguments: &mut HashMap<String, String>,
) -> Result<Option<MetadataPagination>, ClientControlError> {
    let Some(limit) = arguments.remove("limit") else {
        return if arguments.contains_key("cursor") {
            Err(ClientControlError::InvalidMetadataCursor)
        } else {
            Ok(None)
        };
    };
    let page_limit = limit
        .parse::<usize>()
        .ok()
        .filter(|value| {
            (1..=CLIENT_METADATA_MAX_PAGE).contains(value) && value.to_string() == limit
        })
        .ok_or(ClientControlError::InvalidMetadataPageLimit)?;
    let cursor = arguments
        .remove("cursor")
        .map(|encoded| {
            let cursor = decode_lower_hex_id(&encoded)
                .map_err(|_| ClientControlError::InvalidMetadataCursor)?;
            if cursor[0] != CLIENT_METADATA_CURSOR_KIND {
                return Err(ClientControlError::InvalidMetadataCursor);
            }
            Ok(cursor)
        })
        .transpose()?;
    Ok(Some(MetadataPagination { page_limit, cursor }))
}

fn lower_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn decode_canonical_base64(text: &str) -> Result<Zeroizing<Vec<u8>>, ClientControlError> {
    let input = text.as_bytes();
    if input.is_empty() || !input.len().is_multiple_of(4) {
        return Err(ClientControlError::InvalidTokenSecret);
    }
    let mut output = Zeroizing::new(Vec::new());
    output
        .try_reserve_exact(input.len() / 4 * 3)
        .map_err(|_| ClientControlError::InvalidTokenSecret)?;
    for (index, chunk) in input.chunks_exact(4).enumerate() {
        let last = index + 1 == input.len() / 4;
        let a = base64_value(chunk[0]).ok_or(ClientControlError::InvalidTokenSecret)?;
        let b = base64_value(chunk[1]).ok_or(ClientControlError::InvalidTokenSecret)?;
        output.push((a << 2) | (b >> 4));
        match (chunk[2], chunk[3]) {
            (b'=', b'=') if last && b & 0x0f == 0 => (),
            (c, b'=') if last => {
                let c = base64_value(c).ok_or(ClientControlError::InvalidTokenSecret)?;
                if c & 0x03 != 0 {
                    return Err(ClientControlError::InvalidTokenSecret);
                }
                output.push((b << 4) | (c >> 2));
            }
            (c, d) if c != b'=' && d != b'=' => {
                let c = base64_value(c).ok_or(ClientControlError::InvalidTokenSecret)?;
                let d = base64_value(d).ok_or(ClientControlError::InvalidTokenSecret)?;
                output.push((b << 4) | (c >> 2));
                output.push((c << 6) | d);
            }
            _ => return Err(ClientControlError::InvalidTokenSecret),
        }
    }
    if output.len() != TOKEN_SECRET_BYTES {
        return Err(ClientControlError::InvalidTokenSecret);
    }
    Ok(output)
}

fn base64_value(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

fn encode_base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let a = chunk[0];
        let b = *chunk.get(1).unwrap_or(&0);
        let c = *chunk.get(2).unwrap_or(&0);
        output.push(ALPHABET[(a >> 2) as usize] as char);
        output.push(ALPHABET[(((a & 0x03) << 4) | (b >> 4)) as usize] as char);
        if chunk.len() > 1 {
            output.push(ALPHABET[(((b & 0x0f) << 2) | (c >> 6)) as usize] as char);
        } else {
            output.push('=');
        }
        if chunk.len() > 2 {
            output.push(ALPHABET[(c & 0x3f) as usize] as char);
        } else {
            output.push('=');
        }
    }
    output
}

impl std::fmt::Display for ClientSessionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "client session error: {self:?}")
    }
}

impl std::error::Error for ClientSessionError {}

/// Per-connection authentication and mode state used by the gateway dispatcher.
/// It keeps the bearer secret only for this connection lifetime, zeroizes it
/// on drop, and gives staging authority only after storage AUTH.
#[derive(Default)]
pub struct ClientConnectionSession {
    hello: Option<ClientMode>,
    authenticated: Option<AuthenticatedQuerySession>,
    token_secret: Option<[u8; TOKEN_SECRET_BYTES]>,
    request_ids: ClientRequestIds,
    pending_admin_operation: Option<PendingAdminOperation>,
    metadata_page_snapshot: Option<ActiveMetadataPageSnapshot>,
}

impl ClientConnectionSession {
    /// Process the protocol handshake and PING requests. The TCP adapter must
    /// decode bounded frames first and close the connection on framing errors.
    pub fn handle_handshake_request(
        &mut self,
        storage: &StorageManager<'_>,
        request: ClientFrame,
    ) -> Result<ClientFrame, ClientSessionError> {
        if request.response_sequence != 0 {
            return Err(ClientSessionError::UnsupportedRequest);
        }
        self.request_ids
            .accept(&request)
            .map_err(|_| ClientSessionError::UnsupportedRequest)?;
        if !self.permits_request_kind(request.kind) {
            return Err(if self.hello.is_none() || request.kind == 1 {
                ClientSessionError::UnsupportedRequest
            } else if self.authenticated.is_none() {
                ClientSessionError::AuthenticationRequired
            } else {
                ClientSessionError::WrongMode
            });
        }
        let payload = match request.kind {
            1 => {
                let hello = decode_client_hello(&request.payload)
                    .map_err(ClientSessionError::InvalidControlPayload)?;
                let mode_text = match hello.mode {
                    ClientMode::Query => "query",
                    ClientMode::Control => "control",
                };
                self.accept_hello(hello.mode, &hello.client_name)?;
                format!(
                    "{{\"major\":1,\"minor\":0,\"mode\":\"{mode_text}\",\"limits\":{{\"payload_bytes\":{CLIENT_MAX_PAYLOAD_BYTES},\"control_bytes\":{CLIENT_MAX_CONTROL_BYTES},\"chunk_bytes\":{CLIENT_MAX_CHUNK_BYTES}}},\"transport\":\"plaintext_tcp\"}}"
                )
                .into_bytes()
            }
            2 => {
                let credentials = decode_client_credentials(&request.payload)
                    .map_err(ClientSessionError::InvalidControlPayload)?;
                let session =
                    self.authenticate(storage, credentials.token_id, &credentials.token_secret)?;
                let session_id = encode_lower_hex_id(session.connection_id());
                format!("{{\"session_id\":\"{session_id}\",\"authenticated\":true}}").into_bytes()
            }
            11 if request.payload == b"{}" => b"{\"alive\":true}".to_vec(),
            _ => return Err(ClientSessionError::UnsupportedRequest),
        };
        Ok(ClientFrame {
            kind: 100,
            request_id: request.request_id,
            response_sequence: 0,
            payload,
        })
    }

    /// Route one artifact/parameter chunk frame to the staging store using
    /// this connection's storage-authenticated owner, and acknowledge only
    /// the contiguous next offset.
    pub fn handle_upload_request(
        &mut self,
        storage: &StorageManager<'_>,
        staging: &crate::supervisor::StagingStore,
        request: ClientFrame,
    ) -> Result<ClientFrame, ClientSessionError> {
        if request.response_sequence != 0 || !matches!(request.kind, 4 | 5) {
            return Err(ClientSessionError::UnsupportedRequest);
        }
        self.request_ids
            .accept(&request)
            .map_err(|_| ClientSessionError::UnsupportedRequest)?;
        if !self.permits_request_kind(request.kind) {
            return Err(if self.authenticated.is_none() {
                ClientSessionError::AuthenticationRequired
            } else {
                ClientSessionError::WrongMode
            });
        }
        self.revalidate_storage(storage)?;
        let chunk = decode_client_chunk(&request.payload)
            .map_err(ClientSessionError::InvalidControlPayload)?;
        let kind = if request.kind == 4 {
            crate::supervisor::ChunkKind::Artifact
        } else {
            crate::supervisor::ChunkKind::Parameters
        };
        let next_offset = staging
            .upload_chunk(
                self.staging_owner()?,
                chunk.invocation_id,
                kind,
                chunk.offset,
                &chunk.data,
            )
            .map_err(ClientSessionError::Staging)?;
        Ok(ClientFrame {
            kind: 100,
            request_id: request.request_id,
            response_sequence: 0,
            payload: format!("{{\"next_offset\":\"{next_offset}\"}}").into_bytes(),
        })
    }

    /// Reserve bounded supervisor staging from a fully validated PREPARE
    /// request. No storage snapshot or executor is created by this operation.
    pub fn handle_prepare_request(
        &mut self,
        storage: &StorageManager<'_>,
        staging: &crate::supervisor::StagingStore,
        request: ClientFrame,
    ) -> Result<ClientFrame, ClientSessionError> {
        if request.response_sequence != 0 || request.kind != 3 {
            return Err(ClientSessionError::UnsupportedRequest);
        }
        self.request_ids
            .accept(&request)
            .map_err(|_| ClientSessionError::UnsupportedRequest)?;
        if !self.permits_request_kind(request.kind) {
            return Err(if self.authenticated.is_none() {
                ClientSessionError::AuthenticationRequired
            } else {
                ClientSessionError::WrongMode
            });
        }
        self.revalidate_storage(storage)?;
        let prepare = decode_client_prepare(&request.payload)
            .map_err(ClientSessionError::InvalidControlPayload)?;
        let reservation = staging
            .prepare(
                self.staging_owner()?,
                crate::supervisor::PrepareSubmission {
                    manifest_version: prepare.manifest_version,
                    manifest: prepare.manifest,
                    artifact_bytes: prepare.artifact_bytes,
                    artifact_sha256: prepare.artifact_sha256,
                    scope_version: prepare.scope_version,
                    requested_scope: prepare.requested_scope,
                    parameter_bytes: prepare.parameter_bytes,
                    parameter_sha256: prepare.parameter_sha256,
                    deadline_ms: prepare.deadline_ms,
                    workload_class: prepare.workload_class,
                },
            )
            .map_err(ClientSessionError::Staging)?;
        let invocation_id = encode_lower_hex_id(reservation.invocation_id);
        Ok(ClientFrame {
            kind: 100,
            request_id: request.request_id,
            response_sequence: 0,
            payload: format!(
                "{{\"invocation_id\":\"{invocation_id}\",\"artifact_chunk_bytes\":{CLIENT_MAX_CHUNK_BYTES},\"parameter_chunk_bytes\":{CLIENT_MAX_CHUNK_BYTES}}}"
            )
            .into_bytes(),
        })
    }

    /// Verify uploaded lengths, hashes, and versioned admission metadata,
    /// then make the staged submission immutable.
    pub fn handle_seal_request(
        &mut self,
        storage: &StorageManager<'_>,
        staging: &crate::supervisor::StagingStore,
        request: ClientFrame,
    ) -> Result<ClientFrame, ClientSessionError> {
        if request.response_sequence != 0 || request.kind != 6 {
            return Err(ClientSessionError::UnsupportedRequest);
        }
        self.request_ids
            .accept(&request)
            .map_err(|_| ClientSessionError::UnsupportedRequest)?;
        if !self.permits_request_kind(request.kind) {
            return Err(if self.authenticated.is_none() {
                ClientSessionError::AuthenticationRequired
            } else {
                ClientSessionError::WrongMode
            });
        }
        self.revalidate_storage(storage)?;
        let invocation_id = decode_client_seal(&request.payload)
            .map_err(ClientSessionError::InvalidControlPayload)?;
        staging
            .seal(self.staging_owner()?, invocation_id)
            .map_err(ClientSessionError::Staging)?;
        Ok(ClientFrame {
            kind: 100,
            request_id: request.request_id,
            response_sequence: 0,
            payload: b"{\"state\":\"prepared\"}".to_vec(),
        })
    }

    /// Accept one sealed invocation and return both the sequence-zero wire
    /// acknowledgement and the storage admission that the supervisor owns.
    pub fn handle_start_request<'db>(
        &mut self,
        storage: &StorageManager<'db>,
        staging: &crate::supervisor::StagingStore,
        request: ClientFrame,
        retention_deadline_us: i64,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<
        (
            ClientFrame,
            crate::storage_manager::SupervisorInvocation<'db>,
        ),
        ClientStartError,
    > {
        if request.response_sequence != 0 || request.kind != 7 {
            return Err(ClientStartError::Session(
                ClientSessionError::UnsupportedRequest,
            ));
        }
        self.request_ids
            .accept(&request)
            .map_err(|_| ClientStartError::Session(ClientSessionError::UnsupportedRequest))?;
        if !self.permits_request_kind(request.kind) {
            return Err(ClientStartError::Session(if self.authenticated.is_none() {
                ClientSessionError::AuthenticationRequired
            } else {
                ClientSessionError::WrongMode
            }));
        }
        let invocation_id = decode_client_seal(&request.payload).map_err(|error| {
            ClientStartError::Session(ClientSessionError::InvalidControlPayload(error))
        })?;
        let invocation = self.start_submission(
            storage,
            staging,
            invocation_id,
            retention_deadline_us,
            max_rows,
            max_bytes,
        )?;
        let response = ClientFrame {
            kind: 100,
            request_id: request.request_id,
            response_sequence: 0,
            payload: b"{\"state\":\"accepted\"}".to_vec(),
        };
        Ok((response, invocation))
    }

    /// Accept START through the configured executor launch path. The host
    /// profile and cgroup are established before storage admission; the
    /// executor is spawned only after its artifact and confinement command
    /// have been prepared.
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    pub fn handle_start_request_with_executor<'db>(
        &mut self,
        storage: &StorageManager<'db>,
        staging: &crate::supervisor::StagingStore,
        request: ClientFrame,
        admission: crate::supervisor_ipc::GatewayAdmissionConfig,
        executor: &crate::supervisor_ipc::GatewayExecutorConfig,
    ) -> Result<
        (
            ClientFrame,
            crate::storage_manager::SupervisorInvocation<'db>,
            crate::executor_launcher::RunningExecutor,
        ),
        ClientStartError,
    > {
        if request.response_sequence != 0 || request.kind != 7 {
            return Err(ClientStartError::Session(
                ClientSessionError::UnsupportedRequest,
            ));
        }
        self.request_ids
            .accept(&request)
            .map_err(|_| ClientStartError::Session(ClientSessionError::UnsupportedRequest))?;
        if !self.permits_request_kind(request.kind) {
            return Err(ClientStartError::Session(if self.authenticated.is_none() {
                ClientSessionError::AuthenticationRequired
            } else {
                ClientSessionError::WrongMode
            }));
        }
        let invocation_id = decode_client_seal(&request.payload).map_err(|error| {
            ClientStartError::Session(ClientSessionError::InvalidControlPayload(error))
        })?;
        let (invocation, worker) = self.start_submission_with_executor(
            storage,
            staging,
            invocation_id,
            admission,
            executor,
        )?;
        let response = ClientFrame {
            kind: 100,
            request_id: request.request_id,
            response_sequence: 0,
            payload: b"{\"state\":\"accepted\"}".to_vec(),
        };
        Ok((response, invocation, worker))
    }

    /// Report the caller-owned staging state or current storage outcome after
    /// revalidating the connection's credential and role closure.
    pub fn handle_status_request(
        &mut self,
        storage: &StorageManager<'_>,
        staging: &crate::supervisor::StagingStore,
        request: ClientFrame,
    ) -> Result<ClientFrame, ClientSessionError> {
        let invocation_id = self.validate_control_request(storage, &request, 8, false)?;
        let session = self
            .authenticated
            .as_ref()
            .ok_or(ClientSessionError::AuthenticationRequired)?;
        let now_us = current_unix_micros().ok_or(ClientSessionError::UnsupportedRequest)?;
        if self.hello == Some(ClientMode::Control) {
            let snapshot = storage
                .get_invocation_status(invocation_id, session.principal_id(), now_us)
                .map_err(ClientSessionError::Status)?;
            let snapshot = match snapshot {
                Some(snapshot) => Some(snapshot),
                None => storage
                    .get_administrative_invocation_status(
                        session.principal_id(),
                        invocation_id,
                        now_us,
                    )
                    .map_err(ClientSessionError::AdministrativeStatus)?,
            };
            let payload = match snapshot.map(|snapshot| snapshot.status) {
                Some(crate::storage_manager::InvocationStatus::Active { .. }) => {
                    b"{\"state\":\"active\"}".to_vec()
                }
                Some(crate::storage_manager::InvocationStatus::Committing { .. }) => {
                    b"{\"state\":\"committing\"}".to_vec()
                }
                Some(crate::storage_manager::InvocationStatus::Terminal(outcome)) => {
                    match outcome.status {
                        crate::outcome::OutcomeStatus::Committed => {
                            b"{\"state\":\"committed\"}".to_vec()
                        }
                        crate::outcome::OutcomeStatus::Aborted => {
                            b"{\"state\":\"aborted\"}".to_vec()
                        }
                    }
                }
                None => b"{\"state\":\"unknown\"}".to_vec(),
            };
            return self.json_response(request.request_id, payload);
        }
        match staging.status(session.staging_owner(), invocation_id) {
            Ok(
                crate::supervisor::SubmissionState::Uploading
                | crate::supervisor::SubmissionState::Sealing
                | crate::supervisor::SubmissionState::Prepared
                | crate::supervisor::SubmissionState::Starting,
            ) => self.json_response(request.request_id, b"{\"state\":\"prepared\"}".to_vec()),
            Ok(crate::supervisor::SubmissionState::Started)
            | Err(crate::supervisor::StagingError::Unavailable) => {
                let snapshot = storage
                    .get_invocation_status(invocation_id, session.principal_id(), now_us)
                    .map_err(ClientSessionError::Status)?;
                let payload = match snapshot.map(|snapshot| snapshot.status) {
                    Some(crate::storage_manager::InvocationStatus::Active { .. }) => {
                        b"{\"state\":\"active\"}".to_vec()
                    }
                    Some(crate::storage_manager::InvocationStatus::Committing { .. }) => {
                        b"{\"state\":\"committing\"}".to_vec()
                    }
                    Some(crate::storage_manager::InvocationStatus::Terminal(outcome)) => {
                        match outcome.status {
                            crate::outcome::OutcomeStatus::Committed => {
                                b"{\"state\":\"committed\"}".to_vec()
                            }
                            crate::outcome::OutcomeStatus::Aborted => {
                                b"{\"state\":\"aborted\"}".to_vec()
                            }
                        }
                    }
                    None => b"{\"state\":\"unknown\"}".to_vec(),
                };
                self.json_response(request.request_id, payload)
            }
            Err(error) => Err(ClientSessionError::Staging(error)),
        }
    }

    /// Execute supported ADMIN reads and connection-owned prepare/start operations.
    pub fn handle_admin_request(
        &mut self,
        storage: &StorageManager<'_>,
        request: ClientFrame,
        retention_deadline_us: i64,
    ) -> Result<ClientFrame, ClientSessionError> {
        if request.response_sequence != 0 || request.kind != 10 {
            return Err(ClientSessionError::UnsupportedRequest);
        }
        self.request_ids
            .accept(&request)
            .map_err(|_| ClientSessionError::UnsupportedRequest)?;
        if !self.permits_request_kind(10) {
            return Err(if self.authenticated.is_none() {
                ClientSessionError::AuthenticationRequired
            } else {
                ClientSessionError::WrongMode
            });
        }
        self.revalidate_storage(storage)?;
        let command = decode_client_admin_command(&request.payload)
            .map_err(ClientSessionError::InvalidControlPayload)?;
        let actor_id = self
            .authenticated
            .as_ref()
            .ok_or(ClientSessionError::AuthenticationRequired)?
            .principal_id();
        if matches!(
            &command,
            ClientAdminCommand::ListSchemas
                | ClientAdminCommand::ListRelations { .. }
                | ClientAdminCommand::ListColumns { .. }
        ) {
            self.metadata_page_snapshot = None;
        }
        match command {
            ClientAdminCommand::ListSchemas => {
                let token_id = self
                    .authenticated
                    .as_ref()
                    .ok_or(ClientSessionError::AuthenticationRequired)?
                    .token_id();
                let token_secret = self
                    .token_secret
                    .as_ref()
                    .ok_or(ClientSessionError::AuthenticationRequired)?;
                let schemas = storage
                    .discover_token_authorized_schemas(token_id, token_secret)
                    .map_err(ClientSessionError::Storage)?;
                let mut payload = String::from("{\"schemas\":[");
                for (index, schema) in schemas.iter().enumerate() {
                    if index != 0 {
                        payload.push(',');
                    }
                    payload.push_str("{\"schema_id\":\"");
                    payload.push_str(&encode_lower_hex_id(schema.schema_id));
                    payload.push_str("\",\"name\":\"");
                    push_json_string(&mut payload, &schema.name);
                    payload.push_str("\"}");
                }
                payload.push_str("]}");
                if payload.len() > CLIENT_MAX_CONTROL_BYTES {
                    return Err(ClientSessionError::ResourceLimit);
                }
                self.json_response(request.request_id, payload.into_bytes())
            }
            ClientAdminCommand::ListSchemasPage { page_limit, cursor } => self
                .handle_metadata_discovery_page(
                    storage,
                    request.request_id,
                    MetadataPageQuery::Schemas,
                    page_limit,
                    cursor,
                ),
            ClientAdminCommand::ListRelations { schema_id } => {
                let token_id = self
                    .authenticated
                    .as_ref()
                    .ok_or(ClientSessionError::AuthenticationRequired)?
                    .token_id();
                let token_secret = self
                    .token_secret
                    .as_ref()
                    .ok_or(ClientSessionError::AuthenticationRequired)?;
                let relations = storage
                    .discover_token_authorized_relations(token_id, token_secret, schema_id)
                    .map_err(ClientSessionError::Storage)?;
                let mut payload = String::from("{\"relations\":[");
                if let Some(relations) = relations {
                    for (index, relation) in relations.iter().enumerate() {
                        if index != 0 {
                            payload.push(',');
                        }
                        payload.push_str("{\"relation_id\":\"");
                        payload.push_str(&encode_lower_hex_id(relation.relation_id));
                        payload.push_str("\",\"name\":\"");
                        push_json_string(&mut payload, &relation.name);
                        payload.push_str("\"}");
                    }
                }
                payload.push_str("]}");
                if payload.len() > CLIENT_MAX_CONTROL_BYTES {
                    return Err(ClientSessionError::ResourceLimit);
                }
                self.json_response(request.request_id, payload.into_bytes())
            }
            ClientAdminCommand::ListRelationsPage {
                schema_id,
                page_limit,
                cursor,
            } => self.handle_metadata_discovery_page(
                storage,
                request.request_id,
                MetadataPageQuery::Relations(schema_id),
                page_limit,
                cursor,
            ),
            ClientAdminCommand::ListColumns {
                schema_id,
                relation_id,
            } => {
                let token_id = self
                    .authenticated
                    .as_ref()
                    .ok_or(ClientSessionError::AuthenticationRequired)?
                    .token_id();
                let token_secret = self
                    .token_secret
                    .as_ref()
                    .ok_or(ClientSessionError::AuthenticationRequired)?;
                let columns = storage
                    .discover_token_authorized_columns(
                        token_id,
                        token_secret,
                        schema_id,
                        relation_id,
                    )
                    .map_err(ClientSessionError::Storage)?;
                let mut payload = String::from("{\"columns\":[");
                if let Some(columns) = columns {
                    for (index, column) in columns.iter().enumerate() {
                        if index != 0 {
                            payload.push(',');
                        }
                        payload.push_str("{\"column_id\":\"");
                        payload.push_str(&encode_lower_hex_id(column.column_id));
                        payload.push_str("\",\"name\":\"");
                        push_json_string(&mut payload, &column.name);
                        payload.push_str("\",\"ordinal\":");
                        payload.push_str(&column.ordinal.to_string());
                        payload.push_str(" ,\"scalar_type\":\"");
                        push_json_string(
                            &mut payload,
                            &crate::metadata_discovery::scalar_type_name(column.scalar_type),
                        );
                        payload.push_str("\",\"nullable\":");
                        payload.push_str(if column.nullable { "true" } else { "false" });
                        payload.push('}');
                    }
                }
                payload.push_str("]}");
                if payload.len() > CLIENT_MAX_CONTROL_BYTES {
                    return Err(ClientSessionError::ResourceLimit);
                }
                self.json_response(request.request_id, payload.into_bytes())
            }
            ClientAdminCommand::ListColumnsPage {
                schema_id,
                relation_id,
                page_limit,
                cursor,
            } => self.handle_metadata_discovery_page(
                storage,
                request.request_id,
                MetadataPageQuery::Columns {
                    schema_id,
                    relation_id,
                },
                page_limit,
                cursor,
            ),
            ClientAdminCommand::ListTokens {
                user_id,
                after_token_id,
            } => {
                let page = storage
                    .list_tokens_page(actor_id, user_id, after_token_id)
                    .map_err(ClientSessionError::TokenInventory)?;
                let mut payload = String::from("{\"tokens\":[");
                for (index, token) in page.tokens.iter().enumerate() {
                    if index != 0 {
                        payload.push(',');
                    }
                    payload.push_str("{\"token_id\":\"");
                    payload.push_str(&encode_lower_hex_id(token.token_id));
                    payload.push_str("\",\"label\":\"");
                    push_json_string(&mut payload, &token.label);
                    payload.push_str("\",\"issued_at_us\":");
                    payload.push_str(&token.issued_at_us.to_string());
                    payload.push_str(" ,\"expires_at_us\":");
                    payload.push_str(&token.expires_at_us.to_string());
                    payload.push_str(" ,\"revoked\":");
                    payload.push_str(if token.revoked { "true" } else { "false" });
                    payload.push_str(" ,\"revoked_at_us\":");
                    if let Some(revoked_at) = token.revoked_at_us {
                        payload.push_str(&revoked_at.to_string());
                    } else {
                        payload.push_str("null");
                    }
                    payload.push('}');
                }
                payload.push_str("],\"next_after_token_id\":");
                if let Some(cursor) = page.next_after_token_id {
                    payload.push('"');
                    payload.push_str(&encode_lower_hex_id(cursor));
                    payload.push('"');
                } else {
                    payload.push_str("null");
                }
                payload.push('}');
                if payload.len() > CLIENT_MAX_CONTROL_BYTES {
                    return Err(ClientSessionError::ResourceLimit);
                }
                self.json_response(request.request_id, payload.into_bytes())
            }
            ClientAdminCommand::ListRuntimeInvocations { page_limit, cursor } => {
                let now_us = current_unix_micros().ok_or(ClientSessionError::UnsupportedRequest)?;
                let page = storage
                    .list_runtime_invocations(actor_id, now_us, page_limit, cursor.as_ref())
                    .map_err(ClientSessionError::AdministrativeRuntimeList)?;
                let mut payload = format!(
                    "{{\"observed_at_us\":{},\"consistency\":\"best_effort\",\"invocations\":[",
                    page.observed_at_us
                );
                for (index, invocation) in page.invocations.iter().enumerate() {
                    if index != 0 {
                        payload.push(',');
                    }
                    let (state, started_at_us, deadline_at_us, cancellation_requested) =
                        match &invocation.status {
                            crate::storage_manager::InvocationStatus::Active {
                                started_at_us,
                                deadline_at_us,
                                cancellation_requested,
                            } => (
                                "active",
                                Some(*started_at_us),
                                *deadline_at_us,
                                *cancellation_requested,
                            ),
                            crate::storage_manager::InvocationStatus::Committing {
                                started_at_us,
                                deadline_at_us,
                                cancellation_requested,
                            } => (
                                "committing",
                                Some(*started_at_us),
                                *deadline_at_us,
                                *cancellation_requested,
                            ),
                            crate::storage_manager::InvocationStatus::Terminal(outcome) => (
                                match outcome.status {
                                    crate::outcome::OutcomeStatus::Committed => "committed",
                                    crate::outcome::OutcomeStatus::Aborted => "aborted",
                                },
                                None,
                                None,
                                false,
                            ),
                        };
                    payload.push_str("{\"invocation_id\":\"");
                    payload.push_str(&encode_lower_hex_id(invocation.invocation_id));
                    payload.push_str("\",\"principal_id\":\"");
                    payload.push_str(&encode_lower_hex_id(invocation.principal_id));
                    payload.push_str("\",\"state\":\"");
                    payload.push_str(state);
                    payload.push_str("\",\"started_at_us\":");
                    push_json_optional_i64(&mut payload, started_at_us);
                    payload.push_str(" ,\"deadline_at_us\":");
                    push_json_optional_i64(&mut payload, deadline_at_us);
                    payload.push_str(" ,\"cancellation_requested\":");
                    payload.push_str(if cancellation_requested {
                        "true"
                    } else {
                        "false"
                    });
                    payload.push_str(" ,\"cancellation_reason_code\":");
                    if let Some(reason) = invocation.cancellation_reason_code {
                        payload.push_str(&reason.to_string());
                    } else {
                        payload.push_str("null");
                    }
                    payload.push('}');
                }
                payload.push_str("],\"next_cursor\":");
                if let Some(cursor) = page.continuation {
                    payload.push('"');
                    payload.push_str(&cursor.to_wire());
                    payload.push('"');
                } else {
                    payload.push_str("null");
                }
                payload.push('}');
                if payload.len() > CLIENT_MAX_CONTROL_BYTES {
                    return Err(ClientSessionError::ResourceLimit);
                }
                self.json_response(request.request_id, payload.into_bytes())
            }
            ClientAdminCommand::PrepareCreateUser { name } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::CreateUser { name },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareCreateRole { name } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::CreateRole { name },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareCreateSchema { name, owner_id } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::CreateSchema { name, owner_id },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareDropSchema { schema_id } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::DropSchema { schema_id },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareSetSchemaOwner {
                schema_id,
                owner_id,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::SetSchemaOwner {
                        schema_id,
                        owner_id,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareRenameSchema { schema_id, name } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::RenameSchema { schema_id, name },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareDropTable { table_id } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::DropTable { table_id },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareDropForeignKey { foreign_key_id } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::DropForeignKey { foreign_key_id },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareDropCheckConstraint { constraint_id } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::DropCheckConstraint { constraint_id },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareDropPrimaryKey { constraint_id } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::DropPrimaryKey { constraint_id },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareDropUniqueConstraint { constraint_id } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::DropUniqueConstraint { constraint_id },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareCreateForeignKey {
                source_table_id,
                source_column_ids,
                target_constraint_id,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::CreateForeignKey {
                        source_table_id,
                        source_column_ids,
                        target_constraint_id,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareCreateUniqueConstraint {
                table_id,
                column_ids,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::CreateUniqueConstraint {
                        table_id,
                        column_ids,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareCreateIndex {
                table_id,
                name,
                column_ids,
                unique,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::CreateIndex {
                        table_id,
                        name,
                        column_ids,
                        unique,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareDropIndex { index_id } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::DropIndex { index_id },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareRebuildIndex {
                index_id,
                column_ids,
                unique,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::RebuildIndex {
                        index_id,
                        column_ids,
                        unique,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareCreatePrimaryKey {
                table_id,
                column_ids,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::CreatePrimaryKey {
                        table_id,
                        column_ids,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareCreateCheckConstraint {
                table_id,
                expression,
                dependency_column_ids,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::CreateCheckConstraint {
                        table_id,
                        expression,
                        dependency_column_ids,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareCreatePolicy {
                table_id,
                name,
                kind,
                operation,
                effect,
                recipients,
                target_column_ids,
                using_expression,
                with_check_expression,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::CreatePolicy {
                        table_id,
                        name,
                        kind,
                        operation,
                        effect,
                        recipients,
                        target_column_ids,
                        using_expression,
                        with_check_expression,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareDropPolicy { policy_id } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::DropPolicy { policy_id },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareRenameTable { table_id, name } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::RenameTable { table_id, name },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareSetTableOwner { table_id, owner_id } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::SetTableOwner { table_id, owner_id },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareRenameColumn {
                table_id,
                column_id,
                name,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::RenameColumn {
                        table_id,
                        column_id,
                        name,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareSetColumnNullable {
                table_id,
                column_id,
                nullable,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::SetColumnNullable {
                        table_id,
                        column_id,
                        nullable,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareAlterColumnDefault {
                table_id,
                column_id,
                default,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::AlterColumnDefault {
                        table_id,
                        column_id,
                        default,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareAddColumn { table_id, column } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::AddColumn { table_id, column },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareDropColumn {
                table_id,
                column_id,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::DropColumn {
                        table_id,
                        column_id,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareCreateTable {
                schema_id,
                name,
                columns,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::CreateTable {
                        schema_id,
                        name,
                        columns,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareRenameIdentity { identity_id, name } => {
                self.prepare_identity_rename(request.request_id, identity_id, name)
            }
            ClientAdminCommand::PrepareDropIdentity { identity_id } => {
                self.prepare_identity_drop(request.request_id, identity_id)
            }
            ClientAdminCommand::PrepareSetUserLogin { user_id, enabled } => {
                self.prepare_user_login(request.request_id, user_id, enabled)
            }
            ClientAdminCommand::PrepareGrantRole { member_id, role_id } => {
                self.prepare_role_membership(request.request_id, member_id, role_id, true)
            }
            ClientAdminCommand::PrepareRevokeRole { member_id, role_id } => {
                self.prepare_role_membership(request.request_id, member_id, role_id, false)
            }
            ClientAdminCommand::PrepareSystemPrivilege {
                grantee_id,
                privilege,
                grant,
            } => self.prepare_system_privilege(request.request_id, grantee_id, privilege, grant),
            ClientAdminCommand::PrepareObjectGrant {
                grantee_id,
                object_id,
                privilege,
                target,
                grant,
            } => self.prepare_object_grant(
                request.request_id,
                grantee_id,
                object_id,
                privilege,
                target,
                grant,
            ),
            ClientAdminCommand::PrepareRevoke { token_id } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::Revoke { token_id },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareIssue {
                user_id,
                label,
                expires_at_us,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let now_us = current_unix_micros().ok_or(ClientSessionError::UnsupportedRequest)?;
                if expires_at_us <= now_us {
                    return Err(ClientSessionError::InvalidControlPayload(
                        ClientControlError::InvalidPrepare,
                    ));
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::Issue {
                        user_id,
                        label,
                        expires_at_us,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::Start { operation_id } => {
                let Some(pending) = self.pending_admin_operation.as_ref() else {
                    return Err(ClientSessionError::WrongMode);
                };
                if pending.operation_id != operation_id {
                    return Err(ClientSessionError::WrongMode);
                }
                let pending = self
                    .pending_admin_operation
                    .take()
                    .ok_or(ClientSessionError::WrongMode)?;
                let now_us = current_unix_micros().ok_or(ClientSessionError::UnsupportedRequest)?;
                let mut event_id = [0; 16];
                getrandom::fill(&mut event_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if event_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                match pending.action {
                    PendingAdminAction::ChangeRoleMembership {
                        member_id,
                        role_id,
                        grant,
                    } => {
                        let sequence = storage
                            .set_role_membership(
                                operation_id,
                                actor_id,
                                crate::storage_manager::RoleMembershipChange {
                                    member_id,
                                    role_id,
                                    grant,
                                },
                                retention_deadline_us,
                                crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::AuthorityChange,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            )
                            .map_err(ClientSessionError::MembershipLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"commit_sequence\":\"{sequence}\"}}",
                                encode_lower_hex_id(operation_id)
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::RenameIdentity { identity_id, name } => {
                        let sequence = storage
                            .rename_identity(
                                operation_id,
                                actor_id,
                                identity_id,
                                &name,
                                retention_deadline_us,
                                crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            )
                            .map_err(ClientSessionError::IdentityLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"commit_sequence\":\"{sequence}\"}}",
                                encode_lower_hex_id(operation_id)
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::DropIdentity { identity_id } => {
                        let sequence = storage
                            .drop_identity(
                                operation_id,
                                actor_id,
                                identity_id,
                                retention_deadline_us,
                                crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            )
                            .map_err(ClientSessionError::IdentityLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"commit_sequence\":\"{sequence}\"}}",
                                encode_lower_hex_id(operation_id)
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::SetUserLogin { user_id, enabled } => {
                        let sequence = storage
                            .set_user_login_enabled(
                                operation_id,
                                actor_id,
                                user_id,
                                enabled,
                                retention_deadline_us,
                                crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            )
                            .map_err(ClientSessionError::IdentityLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"commit_sequence\":\"{sequence}\"}}",
                                encode_lower_hex_id(operation_id)
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::ChangeSystemPrivilege {
                        grantee_id,
                        privilege,
                        grant,
                    } => {
                        let sequence = storage
                            .set_system_privilege(
                                operation_id,
                                actor_id,
                                crate::storage_manager::SystemPrivilegeChange {
                                    grantee_id,
                                    privilege,
                                    grant,
                                },
                                retention_deadline_us,
                                crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::AuthorityChange,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            )
                            .map_err(ClientSessionError::SystemPrivilegeLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"commit_sequence\":\"{sequence}\"}}",
                                encode_lower_hex_id(operation_id)
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::ChangeObjectGrant {
                        grantee_id,
                        object_id,
                        privilege,
                        target,
                        grant,
                    } => {
                        let sequence = storage
                            .set_object_grant(crate::storage_manager::SetObjectGrantRequest {
                                invocation_id: operation_id,
                                authenticated_actor_id: actor_id,
                                grantee_id,
                                object_id,
                                privilege,
                                target,
                                grant,
                                retention_deadline_us,
                                audit_event: crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::AuthorityChange,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            })
                            .map_err(ClientSessionError::ObjectGrantLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"commit_sequence\":\"{sequence}\"}}",
                                encode_lower_hex_id(operation_id)
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::CreateSchema { name, owner_id } => {
                        let created = storage
                            .create_schema(
                                operation_id,
                                actor_id,
                                &name,
                                owner_id,
                                retention_deadline_us,
                                crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            )
                            .map_err(ClientSessionError::SchemaLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"schema_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(created.schema_id),
                                created.commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::DropSchema { schema_id } => {
                        let dropped = storage
                            .drop_schema(
                                operation_id,
                                actor_id,
                                schema_id,
                                retention_deadline_us,
                                crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            )
                            .map_err(ClientSessionError::SchemaLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"schema_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(dropped.schema_id),
                                dropped.commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::SetSchemaOwner {
                        schema_id,
                        owner_id,
                    } => {
                        let commit_sequence = storage
                            .set_schema_owner(
                                operation_id,
                                actor_id,
                                schema_id,
                                owner_id,
                                retention_deadline_us,
                                crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            )
                            .map_err(ClientSessionError::SchemaLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"schema_id\":\"{}\",\"owner_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(schema_id),
                                encode_lower_hex_id(owner_id),
                                commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::RenameSchema { schema_id, name } => {
                        let commit_sequence = storage
                            .rename_schema(
                                operation_id,
                                actor_id,
                                schema_id,
                                &name,
                                retention_deadline_us,
                                crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            )
                            .map_err(ClientSessionError::SchemaLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"schema_id\":\"{}\",\"name\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(schema_id),
                                name,
                                commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::DropTable { table_id } => {
                        let dropped = storage
                            .drop_table(
                                operation_id,
                                actor_id,
                                table_id,
                                retention_deadline_us,
                                crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            )
                            .map_err(ClientSessionError::TableLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"table_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(dropped.table_id),
                                dropped.commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::DropForeignKey { foreign_key_id } => {
                        let commit_sequence = storage
                            .drop_foreign_key(
                                operation_id,
                                actor_id,
                                foreign_key_id,
                                retention_deadline_us,
                                crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            )
                            .map_err(ClientSessionError::ForeignKeyLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"foreign_key_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(foreign_key_id),
                                commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::DropCheckConstraint { constraint_id } => {
                        let commit_sequence = storage
                            .drop_check_constraint(
                                operation_id,
                                actor_id,
                                constraint_id,
                                retention_deadline_us,
                                crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            )
                            .map_err(ClientSessionError::ConstraintLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"constraint_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(constraint_id),
                                commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::DropPrimaryKey { constraint_id } => {
                        let commit_sequence = storage
                            .drop_primary_key(
                                operation_id,
                                actor_id,
                                constraint_id,
                                retention_deadline_us,
                                crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            )
                            .map_err(ClientSessionError::ConstraintLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"constraint_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(constraint_id),
                                commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::DropUniqueConstraint { constraint_id } => {
                        let commit_sequence = storage
                            .drop_unique_constraint(
                                operation_id,
                                actor_id,
                                constraint_id,
                                retention_deadline_us,
                                crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            )
                            .map_err(ClientSessionError::ConstraintLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"constraint_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(constraint_id),
                                commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::CreateForeignKey {
                        source_table_id,
                        source_column_ids,
                        target_constraint_id,
                    } => {
                        let created = storage
                            .create_foreign_key(crate::storage_manager::CreateForeignKeyRequest {
                                invocation_id: operation_id,
                                authenticated_actor_id: actor_id,
                                source_table_id,
                                source_column_ids,
                                target_constraint_id,
                                retention_deadline_us,
                                audit_event: crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            })
                            .map_err(ClientSessionError::ForeignKeyCreateLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"foreign_key_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(created.foreign_key_id),
                                created.commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::CreateUniqueConstraint {
                        table_id,
                        column_ids,
                    } => {
                        let created = storage
                            .create_unique_constraint(
                                crate::storage_manager::CreateUniqueConstraintRequest {
                                    invocation_id: operation_id,
                                    authenticated_actor_id: actor_id,
                                    table_id,
                                    column_ids,
                                    retention_deadline_us,
                                    audit_event: crate::audit::AuditEvent {
                                        event_id,
                                        actor_id: Some(actor_id),
                                        correlation_id: operation_id,
                                        occurred_at_us: now_us,
                                        action: crate::audit::AuditAction::Ddl,
                                        result: crate::audit::AuditResult::Succeeded,
                                        target_ids: Vec::new(),
                                    },
                                },
                            )
                            .map_err(ClientSessionError::ConstraintLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"constraint_id\":\"{}\",\"index_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(created.constraint_id),
                                encode_lower_hex_id(created.index_id),
                                created.commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::CreateIndex {
                        table_id,
                        name,
                        column_ids,
                        unique,
                    } => {
                        let created = storage
                            .create_index(crate::storage_manager::CreateIndexRequest {
                                invocation_id: operation_id,
                                authenticated_actor_id: actor_id,
                                table_id,
                                name,
                                column_ids,
                                unique,
                                retention_deadline_us,
                                audit_event: crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            })
                            .map_err(ClientSessionError::IndexLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"index_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(created.index_id),
                                created.commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::DropIndex { index_id } => {
                        let dropped = storage
                            .drop_index(crate::storage_manager::DropIndexRequest {
                                invocation_id: operation_id,
                                authenticated_actor_id: actor_id,
                                index_id,
                                retention_deadline_us,
                                audit_event: crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            })
                            .map_err(ClientSessionError::IndexLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"index_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(dropped.index_id),
                                dropped.commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::RebuildIndex {
                        index_id,
                        column_ids,
                        unique,
                    } => {
                        let rebuilt = storage
                            .rebuild_index(crate::storage_manager::RebuildIndexRequest {
                                invocation_id: operation_id,
                                authenticated_actor_id: actor_id,
                                index_id,
                                column_ids,
                                unique,
                                retention_deadline_us,
                                audit_event: crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            })
                            .map_err(ClientSessionError::IndexLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"index_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(rebuilt.index_id),
                                rebuilt.commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::CreatePrimaryKey {
                        table_id,
                        column_ids,
                    } => {
                        let created = storage
                            .create_primary_key(crate::storage_manager::CreatePrimaryKeyRequest {
                                invocation_id: operation_id,
                                authenticated_actor_id: actor_id,
                                table_id,
                                column_ids,
                                retention_deadline_us,
                                audit_event: crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            })
                            .map_err(ClientSessionError::ConstraintLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"constraint_id\":\"{}\",\"index_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(created.constraint_id),
                                encode_lower_hex_id(created.index_id),
                                created.commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::CreateCheckConstraint {
                        table_id,
                        expression,
                        dependency_column_ids,
                    } => {
                        let created = storage
                            .create_check_constraint(
                                crate::storage_manager::CreateCheckConstraintRequest {
                                    invocation_id: operation_id,
                                    authenticated_actor_id: actor_id,
                                    table_id,
                                    expression,
                                    dependency_column_ids,
                                    retention_deadline_us,
                                    audit_event: crate::audit::AuditEvent {
                                        event_id,
                                        actor_id: Some(actor_id),
                                        correlation_id: operation_id,
                                        occurred_at_us: now_us,
                                        action: crate::audit::AuditAction::Ddl,
                                        result: crate::audit::AuditResult::Succeeded,
                                        target_ids: Vec::new(),
                                    },
                                },
                            )
                            .map_err(ClientSessionError::ConstraintLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"constraint_id\":\"{}\",\"expression_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(created.constraint_id),
                                encode_lower_hex_id(created.expression_id),
                                created.commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::CreatePolicy {
                        table_id,
                        name,
                        kind,
                        operation,
                        effect,
                        recipients,
                        target_column_ids,
                        using_expression,
                        with_check_expression,
                    } => {
                        let created = storage
                            .create_policy(crate::storage_manager::CreatePolicyRequest {
                                invocation_id: operation_id,
                                authenticated_actor_id: actor_id,
                                table_id,
                                name,
                                kind,
                                operation,
                                effect,
                                recipients,
                                target_column_ids,
                                using_expression,
                                with_check_expression,
                                retention_deadline_us,
                                audit_event: crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            })
                            .map_err(ClientSessionError::PolicyLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"policy_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(created.policy_id),
                                created.commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::DropPolicy { policy_id } => {
                        let commit_sequence = storage
                            .drop_policy(crate::storage_manager::DropPolicyRequest {
                                invocation_id: operation_id,
                                authenticated_actor_id: actor_id,
                                policy_id,
                                retention_deadline_us,
                                audit_event: crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            })
                            .map_err(ClientSessionError::PolicyLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"policy_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(policy_id),
                                commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::RenameTable { table_id, name } => {
                        let commit_sequence = storage
                            .rename_table(
                                operation_id,
                                actor_id,
                                table_id,
                                &name,
                                retention_deadline_us,
                                crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            )
                            .map_err(ClientSessionError::TableLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"table_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(table_id),
                                commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::SetTableOwner { table_id, owner_id } => {
                        let commit_sequence = storage
                            .set_table_owner(
                                operation_id,
                                actor_id,
                                table_id,
                                owner_id,
                                retention_deadline_us,
                                crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            )
                            .map_err(ClientSessionError::TableLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"table_id\":\"{}\",\"owner_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(table_id),
                                encode_lower_hex_id(owner_id),
                                commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::RenameColumn {
                        table_id,
                        column_id,
                        name,
                    } => {
                        let commit_sequence = storage
                            .rename_column(
                                operation_id,
                                actor_id,
                                table_id,
                                column_id,
                                &name,
                                retention_deadline_us,
                                crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            )
                            .map_err(ClientSessionError::TableLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"table_id\":\"{}\",\"column_id\":\"{}\",\"name\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(table_id),
                                encode_lower_hex_id(column_id),
                                name,
                                commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::SetColumnNullable {
                        table_id,
                        column_id,
                        nullable,
                    } => {
                        let commit_sequence = storage
                            .set_column_nullable(
                                operation_id,
                                actor_id,
                                table_id,
                                column_id,
                                nullable,
                                retention_deadline_us,
                                crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            )
                            .map_err(ClientSessionError::TableLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"table_id\":\"{}\",\"column_id\":\"{}\",\"nullable\":{},\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(table_id),
                                encode_lower_hex_id(column_id),
                                nullable,
                                commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::AlterColumnDefault {
                        table_id,
                        column_id,
                        default,
                    } => {
                        let commit_sequence = storage
                            .alter_column_default(
                                crate::storage_manager::AlterColumnDefaultRequest {
                                    invocation_id: operation_id,
                                    authenticated_actor_id: actor_id,
                                    table_id,
                                    column_id,
                                    default,
                                    retention_deadline_us,
                                    audit_event: crate::audit::AuditEvent {
                                        event_id,
                                        actor_id: Some(actor_id),
                                        correlation_id: operation_id,
                                        occurred_at_us: now_us,
                                        action: crate::audit::AuditAction::Ddl,
                                        result: crate::audit::AuditResult::Succeeded,
                                        target_ids: Vec::new(),
                                    },
                                },
                            )
                            .map_err(ClientSessionError::TableLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"table_id\":\"{}\",\"column_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(table_id),
                                encode_lower_hex_id(column_id),
                                commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::AddColumn { table_id, column } => {
                        let added = storage
                            .add_table_column(crate::storage_manager::AddTableColumnRequest {
                                invocation_id: operation_id,
                                authenticated_actor_id: actor_id,
                                table_id,
                                column,
                                retention_deadline_us,
                                audit_event: crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            })
                            .map_err(ClientSessionError::TableLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"table_id\":\"{}\",\"column_id\":\"{}\",\"schema_version\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(added.table_id),
                                encode_lower_hex_id(added.column_id),
                                added.schema_version,
                                added.commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::DropColumn {
                        table_id,
                        column_id,
                    } => {
                        let dropped = storage
                            .drop_column(
                                operation_id,
                                actor_id,
                                table_id,
                                column_id,
                                retention_deadline_us,
                                crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            )
                            .map_err(ClientSessionError::TableLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"table_id\":\"{}\",\"column_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(dropped.table_id),
                                encode_lower_hex_id(dropped.column_id),
                                dropped.commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::CreateTable {
                        schema_id,
                        name,
                        columns,
                    } => {
                        let created = storage
                            .create_table(crate::storage_manager::CreateTableRequest {
                                invocation_id: operation_id,
                                authenticated_actor_id: actor_id,
                                schema_id,
                                name,
                                columns,
                                retention_deadline_us,
                                audit_event: crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            })
                            .map_err(ClientSessionError::TableLifecycle)?;
                        let mut payload = format!(
                            "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"table_id\":\"{}\",\"column_ids\":[",
                            encode_lower_hex_id(operation_id),
                            encode_lower_hex_id(created.table_id),
                        );
                        for (index, column_id) in created.column_ids.iter().enumerate() {
                            if index != 0 {
                                payload.push(',');
                            }
                            payload.push('"');
                            payload.push_str(&encode_lower_hex_id(*column_id));
                            payload.push('"');
                        }
                        payload.push_str(&format!(
                            "],\"commit_sequence\":\"{}\"}}",
                            created.commit_sequence
                        ));
                        if payload.len() > CLIENT_MAX_CONTROL_BYTES {
                            return Err(ClientSessionError::ResourceLimit);
                        }
                        self.json_response(request.request_id, payload.into_bytes())
                    }
                    PendingAdminAction::CreateRole { name } => {
                        let created = storage
                            .create_role(
                                operation_id,
                                actor_id,
                                &name,
                                retention_deadline_us,
                                crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            )
                            .map_err(ClientSessionError::IdentityLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"role_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(created.role_id),
                                created.commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::CreateUser { name } => {
                        let created = storage
                            .create_user(
                                operation_id,
                                actor_id,
                                &name,
                                retention_deadline_us,
                                crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::Ddl,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: Vec::new(),
                                },
                            )
                            .map_err(ClientSessionError::IdentityLifecycle)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"user_id\":\"{}\",\"commit_sequence\":\"{}\"}}",
                                encode_lower_hex_id(operation_id),
                                encode_lower_hex_id(created.user_id),
                                created.commit_sequence,
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::Revoke { token_id } => {
                        let sequence = storage
                            .revoke_token(
                                operation_id,
                                actor_id,
                                token_id,
                                retention_deadline_us,
                                crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::TokenLifecycle,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: vec![token_id],
                                },
                            )
                            .map_err(ClientSessionError::TokenInventory)?;
                        self.json_response(
                            request.request_id,
                            format!(
                                "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"commit_sequence\":\"{sequence}\"}}",
                                encode_lower_hex_id(operation_id)
                            )
                            .into_bytes(),
                        )
                    }
                    PendingAdminAction::Issue {
                        user_id,
                        label,
                        expires_at_us,
                    } => {
                        let mut issued = storage
                            .issue_token(crate::storage_manager::TokenIssueRequest {
                                invocation_id: operation_id,
                                authenticated_actor_id: actor_id,
                                user_id,
                                label: &label,
                                expires_at_us,
                                retention_deadline_us,
                                audit_event: crate::audit::AuditEvent {
                                    event_id,
                                    actor_id: Some(actor_id),
                                    correlation_id: operation_id,
                                    occurred_at_us: now_us,
                                    action: crate::audit::AuditAction::TokenLifecycle,
                                    result: crate::audit::AuditResult::Succeeded,
                                    target_ids: vec![user_id],
                                },
                            })
                            .map_err(ClientSessionError::TokenInventory)?;
                        let secret = issued
                            .take_secret()
                            .ok_or(ClientSessionError::UnsupportedRequest)?;
                        let mut secret = Zeroizing::new(secret);
                        let secret_base64 = Zeroizing::new(encode_base64(secret.as_ref()));
                        secret.zeroize();
                        let mut payload = Zeroizing::new(format!(
                            "{{\"operation_id\":\"{}\",\"outcome\":\"committed\",\"token_id\":\"{}\",\"token_secret\":\"{}\",\"commit_sequence\":\"{}\"}}",
                            encode_lower_hex_id(operation_id),
                            encode_lower_hex_id(issued.token_id),
                            secret_base64.as_str(),
                            issued.commit_sequence,
                        ));
                        let response_payload = payload.as_bytes().to_vec();
                        payload.zeroize();
                        self.json_response(request.request_id, response_payload)
                    }
                }
            }
        }
    }

    fn prepare_role_membership(
        &mut self,
        request_id: u64,
        member_id: [u8; 16],
        role_id: [u8; 16],
        grant: bool,
    ) -> Result<ClientFrame, ClientSessionError> {
        if self.pending_admin_operation.is_some() {
            return Err(ClientSessionError::WrongMode);
        }
        let mut operation_id = [0; 16];
        getrandom::fill(&mut operation_id)
            .map_err(|_| ClientSessionError::Storage(TransactionError::RandomnessUnavailable))?;
        if operation_id == [0; 16] {
            return Err(ClientSessionError::Storage(
                TransactionError::RandomnessUnavailable,
            ));
        }
        let action = PendingAdminAction::ChangeRoleMembership {
            member_id,
            role_id,
            grant,
        };
        self.pending_admin_operation = Some(PendingAdminOperation {
            operation_id,
            action,
        });
        self.json_response(
            request_id,
            format!(
                "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                encode_lower_hex_id(operation_id)
            )
            .into_bytes(),
        )
    }

    fn prepare_system_privilege(
        &mut self,
        request_id: u64,
        grantee_id: [u8; 16],
        privilege: crate::storage_manager::AdministrativeSystemPrivilege,
        grant: bool,
    ) -> Result<ClientFrame, ClientSessionError> {
        if self.pending_admin_operation.is_some() {
            return Err(ClientSessionError::WrongMode);
        }
        let mut operation_id = [0; 16];
        getrandom::fill(&mut operation_id)
            .map_err(|_| ClientSessionError::Storage(TransactionError::RandomnessUnavailable))?;
        if operation_id == [0; 16] {
            return Err(ClientSessionError::Storage(
                TransactionError::RandomnessUnavailable,
            ));
        }
        self.pending_admin_operation = Some(PendingAdminOperation {
            operation_id,
            action: PendingAdminAction::ChangeSystemPrivilege {
                grantee_id,
                privilege,
                grant,
            },
        });
        self.json_response(
            request_id,
            format!(
                "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                encode_lower_hex_id(operation_id)
            )
            .into_bytes(),
        )
    }

    fn prepare_object_grant(
        &mut self,
        request_id: u64,
        grantee_id: [u8; 16],
        object_id: [u8; 16],
        privilege: crate::storage_manager::ObjectGrantPrivilege,
        target: crate::storage_manager::ObjectGrantTarget,
        grant: bool,
    ) -> Result<ClientFrame, ClientSessionError> {
        if self.pending_admin_operation.is_some() {
            return Err(ClientSessionError::WrongMode);
        }
        let mut operation_id = [0; 16];
        getrandom::fill(&mut operation_id)
            .map_err(|_| ClientSessionError::Storage(TransactionError::RandomnessUnavailable))?;
        if operation_id == [0; 16] {
            return Err(ClientSessionError::Storage(
                TransactionError::RandomnessUnavailable,
            ));
        }
        self.pending_admin_operation = Some(PendingAdminOperation {
            operation_id,
            action: PendingAdminAction::ChangeObjectGrant {
                grantee_id,
                object_id,
                privilege,
                target,
                grant,
            },
        });
        self.json_response(
            request_id,
            format!(
                "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                encode_lower_hex_id(operation_id)
            )
            .into_bytes(),
        )
    }

    fn prepare_identity_rename(
        &mut self,
        request_id: u64,
        identity_id: [u8; 16],
        name: String,
    ) -> Result<ClientFrame, ClientSessionError> {
        if self.pending_admin_operation.is_some() {
            return Err(ClientSessionError::WrongMode);
        }
        let mut operation_id = [0; 16];
        getrandom::fill(&mut operation_id)
            .map_err(|_| ClientSessionError::Storage(TransactionError::RandomnessUnavailable))?;
        if operation_id == [0; 16] {
            return Err(ClientSessionError::Storage(
                TransactionError::RandomnessUnavailable,
            ));
        }
        self.pending_admin_operation = Some(PendingAdminOperation {
            operation_id,
            action: PendingAdminAction::RenameIdentity { identity_id, name },
        });
        self.json_response(
            request_id,
            format!(
                "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                encode_lower_hex_id(operation_id)
            )
            .into_bytes(),
        )
    }

    fn prepare_user_login(
        &mut self,
        request_id: u64,
        user_id: [u8; 16],
        enabled: bool,
    ) -> Result<ClientFrame, ClientSessionError> {
        if self.pending_admin_operation.is_some() {
            return Err(ClientSessionError::WrongMode);
        }
        let mut operation_id = [0; 16];
        getrandom::fill(&mut operation_id)
            .map_err(|_| ClientSessionError::Storage(TransactionError::RandomnessUnavailable))?;
        if operation_id == [0; 16] {
            return Err(ClientSessionError::Storage(
                TransactionError::RandomnessUnavailable,
            ));
        }
        self.pending_admin_operation = Some(PendingAdminOperation {
            operation_id,
            action: PendingAdminAction::SetUserLogin { user_id, enabled },
        });
        self.json_response(
            request_id,
            format!(
                "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                encode_lower_hex_id(operation_id)
            )
            .into_bytes(),
        )
    }

    fn prepare_identity_drop(
        &mut self,
        request_id: u64,
        identity_id: [u8; 16],
    ) -> Result<ClientFrame, ClientSessionError> {
        if self.pending_admin_operation.is_some() {
            return Err(ClientSessionError::WrongMode);
        }
        let mut operation_id = [0; 16];
        getrandom::fill(&mut operation_id)
            .map_err(|_| ClientSessionError::Storage(TransactionError::RandomnessUnavailable))?;
        if operation_id == [0; 16] {
            return Err(ClientSessionError::Storage(
                TransactionError::RandomnessUnavailable,
            ));
        }
        self.pending_admin_operation = Some(PendingAdminOperation {
            operation_id,
            action: PendingAdminAction::DropIdentity { identity_id },
        });
        self.json_response(
            request_id,
            format!(
                "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                encode_lower_hex_id(operation_id)
            )
            .into_bytes(),
        )
    }

    /// Cancel an unstarted reservation without creating a storage outcome.
    /// Started invocations require the supervisor's cancellation service.
    fn handle_metadata_discovery_page(
        &mut self,
        storage: &StorageManager<'_>,
        request_id: u64,
        query: MetadataPageQuery,
        page_limit: usize,
        cursor: Option<[u8; 16]>,
    ) -> Result<ClientFrame, ClientSessionError> {
        if !(1..=CLIENT_METADATA_MAX_PAGE).contains(&page_limit) {
            return Err(ClientSessionError::InvalidControlPayload(
                ClientControlError::InvalidMetadataPageLimit,
            ));
        }
        let session = self
            .authenticated
            .as_ref()
            .ok_or(ClientSessionError::AuthenticationRequired)?;
        let token_id = session.token_id();
        let principal_id = session.principal_id();
        let role_ids = session.role_ids().clone();
        let secret = Zeroizing::new(
            *self
                .token_secret
                .as_ref()
                .ok_or(ClientSessionError::AuthenticationRequired)?,
        );

        let mut snapshot = if let Some(cursor) = cursor {
            let current_snapshot = self.metadata_page_snapshot.as_ref();
            let expired = current_snapshot
                .is_some_and(|snapshot| snapshot.expires_at <= std::time::Instant::now());
            let identity_changed = current_snapshot.is_some_and(|snapshot| {
                snapshot.principal_id != principal_id || snapshot.role_ids != role_ids
            });
            if expired || identity_changed {
                self.metadata_page_snapshot = None;
                return Err(ClientSessionError::InvalidControlPayload(
                    ClientControlError::InvalidMetadataCursor,
                ));
            }
            let valid_cursor = current_snapshot.is_some_and(|snapshot| {
                snapshot.current_cursor == Some(cursor)
                    && snapshot.query == query
                    && snapshot.page_limit == page_limit
            });
            if !valid_cursor {
                return Err(ClientSessionError::InvalidControlPayload(
                    ClientControlError::InvalidMetadataCursor,
                ));
            }
            let current_rows = match &query {
                MetadataPageQuery::Schemas => MetadataPageRows::Schemas(
                    storage
                        .discover_token_authorized_schemas(token_id, &secret)
                        .map_err(ClientSessionError::Storage)?,
                ),
                MetadataPageQuery::Relations(schema_id) => MetadataPageRows::Relations(
                    storage
                        .discover_token_authorized_relations(token_id, &secret, *schema_id)
                        .map_err(ClientSessionError::Storage)?
                        .unwrap_or_default(),
                ),
                MetadataPageQuery::Columns {
                    schema_id,
                    relation_id,
                } => MetadataPageRows::Columns(
                    storage
                        .discover_token_authorized_columns(
                            token_id,
                            &secret,
                            *schema_id,
                            *relation_id,
                        )
                        .map_err(ClientSessionError::Storage)?
                        .unwrap_or_default(),
                ),
            };
            if !current_snapshot
                .is_some_and(|snapshot| snapshot.rows.has_same_object_ids(&current_rows))
            {
                self.metadata_page_snapshot = None;
                return Err(ClientSessionError::InvalidControlPayload(
                    ClientControlError::InvalidMetadataCursor,
                ));
            }
            self.metadata_page_snapshot
                .take()
                .ok_or(ClientSessionError::InvalidControlPayload(
                    ClientControlError::InvalidMetadataCursor,
                ))?
        } else {
            self.metadata_page_snapshot = None;
            let rows = match &query {
                MetadataPageQuery::Schemas => MetadataPageRows::Schemas(
                    storage
                        .discover_token_authorized_schemas(token_id, &secret)
                        .map_err(ClientSessionError::Storage)?,
                ),
                MetadataPageQuery::Relations(schema_id) => MetadataPageRows::Relations(
                    storage
                        .discover_token_authorized_relations(token_id, &secret, *schema_id)
                        .map_err(ClientSessionError::Storage)?
                        .unwrap_or_default(),
                ),
                MetadataPageQuery::Columns {
                    schema_id,
                    relation_id,
                } => MetadataPageRows::Columns(
                    storage
                        .discover_token_authorized_columns(
                            token_id,
                            &secret,
                            *schema_id,
                            *relation_id,
                        )
                        .map_err(ClientSessionError::Storage)?
                        .unwrap_or_default(),
                ),
            };
            ActiveMetadataPageSnapshot {
                query,
                rows,
                principal_id,
                role_ids,
                page_limit,
                next_index: 0,
                current_cursor: None,
                expires_at: std::time::Instant::now() + std::time::Duration::from_secs(60),
            }
        };

        let start = snapshot.next_index;
        let end = start
            .saturating_add(snapshot.page_limit)
            .min(snapshot.rows.len());
        let next_cursor = if end < snapshot.rows.len() {
            let mut token = [0; 16];
            loop {
                getrandom::fill(&mut token).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                token[0] = CLIENT_METADATA_CURSOR_KIND;
                if Some(token) != cursor {
                    break;
                }
            }
            Some(token)
        } else {
            None
        };
        let payload = encode_metadata_page(&snapshot, start, end, next_cursor)?;
        if payload.len() > CLIENT_MAX_CONTROL_BYTES {
            return Err(ClientSessionError::ResourceLimit);
        }

        if let Some(next_cursor) = next_cursor {
            snapshot.next_index = end;
            snapshot.current_cursor = Some(next_cursor);
            snapshot.expires_at = std::time::Instant::now() + std::time::Duration::from_secs(60);
            self.metadata_page_snapshot = Some(snapshot);
        } else {
            self.metadata_page_snapshot = None;
        }
        self.json_response(request_id, payload)
    }

    pub fn handle_cancel_prepared_request(
        &mut self,
        storage: &StorageManager<'_>,
        staging: &crate::supervisor::StagingStore,
        request: ClientFrame,
    ) -> Result<ClientFrame, ClientSessionError> {
        let invocation_id = self.validate_control_request(storage, &request, 9, false)?;
        let session = self
            .authenticated
            .as_ref()
            .ok_or(ClientSessionError::AuthenticationRequired)?;
        if self.hello == Some(ClientMode::Control) {
            let now_us = current_unix_micros().ok_or(ClientSessionError::UnsupportedRequest)?;
            let mut event_id = [0; 16];
            getrandom::fill(&mut event_id).map_err(|_| {
                ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
            })?;
            if event_id == [0; 16] {
                return Err(ClientSessionError::Storage(
                    TransactionError::RandomnessUnavailable,
                ));
            }
            let result = storage
                .cancel_invocation(
                    session.principal_id(),
                    invocation_id,
                    1,
                    now_us,
                    crate::audit::AuditEvent {
                        event_id,
                        actor_id: Some(session.principal_id()),
                        correlation_id: invocation_id,
                        occurred_at_us: now_us,
                        action: crate::audit::AuditAction::AdministrativeCancellation,
                        result: crate::audit::AuditResult::Accepted,
                        target_ids: vec![invocation_id],
                    },
                )
                .map_err(ClientSessionError::AdministrativeCancellation)?;
            let payload = match result {
                crate::storage_manager::AdministrativeCancellationResult::Accepted => {
                    b"{\"state\":\"accepted\"}".to_vec()
                }
                crate::storage_manager::AdministrativeCancellationResult::TooLate => {
                    b"{\"state\":\"too_late\"}".to_vec()
                }
                crate::storage_manager::AdministrativeCancellationResult::Terminal(outcome) => {
                    match outcome.status {
                        crate::outcome::OutcomeStatus::Committed => {
                            b"{\"state\":\"committed\"}".to_vec()
                        }
                        crate::outcome::OutcomeStatus::Aborted => {
                            b"{\"state\":\"aborted\"}".to_vec()
                        }
                    }
                }
                crate::storage_manager::AdministrativeCancellationResult::Unknown => {
                    b"{\"state\":\"unknown\"}".to_vec()
                }
            };
            return self.json_response(request.request_id, payload);
        }
        match staging.cancel_prepared(session.staging_owner(), invocation_id) {
            Ok(()) => self.json_response(request.request_id, b"{\"state\":\"accepted\"}".to_vec()),
            Err(
                crate::supervisor::StagingError::InvalidState
                | crate::supervisor::StagingError::Unavailable,
            ) => {
                let now_us = current_unix_micros().ok_or(ClientSessionError::UnsupportedRequest)?;
                let result = storage
                    .cancel_owned_invocation(session.principal_id(), invocation_id, now_us)
                    .map_err(ClientSessionError::Status)?;
                let payload = match result {
                    crate::storage_manager::OwnerCancellationResult::Accepted => {
                        b"{\"state\":\"accepted\"}".to_vec()
                    }
                    crate::storage_manager::OwnerCancellationResult::TooLate => {
                        b"{\"state\":\"too_late\"}".to_vec()
                    }
                    crate::storage_manager::OwnerCancellationResult::Terminal(outcome) => {
                        match outcome.status {
                            crate::outcome::OutcomeStatus::Committed => {
                                b"{\"state\":\"committed\"}".to_vec()
                            }
                            crate::outcome::OutcomeStatus::Aborted => {
                                b"{\"state\":\"aborted\"}".to_vec()
                            }
                        }
                    }
                    crate::storage_manager::OwnerCancellationResult::Unknown => {
                        b"{\"state\":\"unknown\"}".to_vec()
                    }
                };
                self.json_response(request.request_id, payload)
            }
            Err(error) => Err(ClientSessionError::Staging(error)),
        }
    }

    fn validate_control_request(
        &mut self,
        storage: &StorageManager<'_>,
        request: &ClientFrame,
        kind: u16,
        query_mode_required: bool,
    ) -> Result<[u8; 16], ClientSessionError> {
        if request.response_sequence != 0 || request.kind != kind {
            return Err(ClientSessionError::UnsupportedRequest);
        }
        self.request_ids
            .accept(request)
            .map_err(|_| ClientSessionError::UnsupportedRequest)?;
        if !self.permits_request_kind(kind) {
            return Err(if self.authenticated.is_none() {
                ClientSessionError::AuthenticationRequired
            } else {
                ClientSessionError::WrongMode
            });
        }
        let invocation_id = decode_client_seal(&request.payload)
            .map_err(ClientSessionError::InvalidControlPayload)?;
        if query_mode_required && self.hello != Some(ClientMode::Query) {
            return Err(ClientSessionError::WrongMode);
        }
        self.revalidate_storage(storage)?;
        Ok(invocation_id)
    }

    fn revalidate_storage(
        &mut self,
        storage: &StorageManager<'_>,
    ) -> Result<(), ClientSessionError> {
        let session = self
            .authenticated
            .as_mut()
            .ok_or(ClientSessionError::AuthenticationRequired)?;
        let mut secret = *self
            .token_secret
            .as_ref()
            .ok_or(ClientSessionError::AuthenticationRequired)?;
        let result = storage.revalidate_query_session(session, &secret);
        secret.zeroize();
        result.map_err(ClientSessionError::Storage)
    }

    fn json_response(
        &self,
        request_id: u64,
        payload: Vec<u8>,
    ) -> Result<ClientFrame, ClientSessionError> {
        Ok(ClientFrame {
            kind: 100,
            request_id,
            response_sequence: 0,
            payload,
        })
    }

    /// Accept the one HELLO for this connection before any AUTH request.
    pub fn accept_hello(
        &mut self,
        mode: ClientMode,
        client_name: &str,
    ) -> Result<(), ClientSessionError> {
        if self.hello.is_some() {
            return Err(ClientSessionError::HelloAlreadyAccepted);
        }
        if client_name.len() > 128 || self.authenticated.is_some() {
            return Err(ClientSessionError::InvalidHello);
        }
        self.hello = Some(mode);
        Ok(())
    }

    pub fn accept_hello_payload(&mut self, payload: &[u8]) -> Result<(), ClientSessionError> {
        let hello =
            decode_client_hello(payload).map_err(ClientSessionError::InvalidControlPayload)?;
        self.accept_hello(hello.mode, &hello.client_name)
    }

    /// Authenticate using storage's current token and login/role snapshot.
    /// Callers should map all storage authentication failures to one public
    /// unauthenticated response category.
    pub fn authenticate(
        &mut self,
        storage: &StorageManager<'_>,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
    ) -> Result<&AuthenticatedQuerySession, ClientSessionError> {
        if self.hello.is_none() {
            return Err(ClientSessionError::AuthenticationOutOfOrder);
        }
        if self.authenticated.is_some() {
            return Err(ClientSessionError::AlreadyAuthenticated);
        }
        let session = storage
            .authenticate_query_session(token_id, token_secret)
            .map_err(ClientSessionError::Storage)?;
        self.authenticated = Some(session);
        self.token_secret = Some(*token_secret);
        Ok(self
            .authenticated
            .as_ref()
            .expect("session was just stored"))
    }

    pub fn authenticate_payload(
        &mut self,
        storage: &StorageManager<'_>,
        payload: &[u8],
    ) -> Result<&AuthenticatedQuerySession, ClientSessionError> {
        let credentials = decode_client_credentials(payload)
            .map_err(ClientSessionError::InvalidControlPayload)?;
        self.authenticate(storage, credentials.token_id, &credentials.token_secret)
    }

    /// Authorize a request kind and refresh token/login/role authority before
    /// each execution, status, cancellation, or administrative operation.
    pub fn authorize_request(
        &mut self,
        storage: &StorageManager<'_>,
        kind: u16,
    ) -> Result<(), ClientSessionError> {
        if !self.permits_request_kind(kind) {
            return if self.authenticated.is_none() {
                Err(ClientSessionError::AuthenticationRequired)
            } else {
                Err(ClientSessionError::WrongMode)
            };
        }
        if (3..=10).contains(&kind) {
            let session = self
                .authenticated
                .as_mut()
                .ok_or(ClientSessionError::AuthenticationRequired)?;
            let secret = self
                .token_secret
                .as_ref()
                .ok_or(ClientSessionError::AuthenticationRequired)?;
            storage
                .revalidate_query_session(session, secret)
                .map_err(ClientSessionError::Storage)?;
        }
        Ok(())
    }

    pub fn session_id(&self) -> Result<[u8; 16], ClientSessionError> {
        self.authenticated
            .as_ref()
            .map(AuthenticatedQuerySession::connection_id)
            .ok_or(ClientSessionError::AuthenticationRequired)
    }

    /// Return the authenticated owner required for query staging.
    pub fn staging_owner(&self) -> Result<StagingOwner, ClientSessionError> {
        if self.hello != Some(ClientMode::Query) {
            return Err(ClientSessionError::WrongMode);
        }
        self.authenticated
            .as_ref()
            .map(AuthenticatedQuerySession::staging_owner)
            .ok_or(ClientSessionError::AuthenticationRequired)
    }

    /// Revalidate authority and open the storage invocation only after a
    /// sealed reservation wins the one-shot START transition.
    pub fn start_submission<'db>(
        &mut self,
        storage: &StorageManager<'db>,
        staging: &crate::supervisor::StagingStore,
        invocation_id: [u8; 16],
        retention_deadline_us: i64,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<crate::storage_manager::SupervisorInvocation<'db>, ClientStartError> {
        let owner = self.staging_owner().map_err(ClientStartError::Session)?;
        let authenticated = self
            .authenticated
            .as_ref()
            .ok_or(ClientStartError::Session(
                ClientSessionError::AuthenticationRequired,
            ))?;
        let token_id = authenticated.token_id();
        let mut token_secret = *self.token_secret.as_ref().ok_or(ClientStartError::Session(
            ClientSessionError::AuthenticationRequired,
        ))?;
        let result = staging.start_with(owner, invocation_id, |submission| {
            let duration = std::time::Duration::from_millis(submission.metadata().deadline_ms);
            let deadline = std::time::Instant::now()
                .checked_add(duration)
                .ok_or(TransactionError::InvalidContext)?;
            let requested_scope = crate::submission_scope::decode_requested_scope_v1(
                &submission.metadata().requested_scope,
            )
            .map_err(|_| TransactionError::InvalidContext)?;
            storage.admit_supervisor_invocation_with_deadline_and_scope(
                crate::storage_manager::SupervisorAdmissionRequest {
                    invocation_id: submission.invocation_id(),
                    token_id,
                    retention_deadline_us,
                    max_rows,
                    max_bytes,
                    deadline: Some(deadline),
                    workload_class: submission.metadata().workload_class,
                },
                requested_scope,
                &token_secret,
            )
        });
        token_secret.zeroize();
        result.map_err(ClientStartError::Start)
    }

    /// Revalidate token authority and run the one-shot storage plus executor
    /// launch sequence for a sealed query.
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    pub fn start_submission_with_executor<'db>(
        &mut self,
        storage: &StorageManager<'db>,
        staging: &crate::supervisor::StagingStore,
        invocation_id: [u8; 16],
        admission: crate::supervisor_ipc::GatewayAdmissionConfig,
        executor: &crate::supervisor_ipc::GatewayExecutorConfig,
    ) -> Result<
        (
            crate::storage_manager::SupervisorInvocation<'db>,
            crate::executor_launcher::RunningExecutor,
        ),
        ClientStartError,
    > {
        let owner = self.staging_owner().map_err(ClientStartError::Session)?;
        let authenticated = self
            .authenticated
            .as_ref()
            .ok_or(ClientStartError::Session(
                ClientSessionError::AuthenticationRequired,
            ))?;
        let token_id = authenticated.token_id();
        let mut token_secret = *self.token_secret.as_ref().ok_or(ClientStartError::Session(
            ClientSessionError::AuthenticationRequired,
        ))?;
        let result = crate::executor_launcher::start_and_launch_executor_selected(
            staging,
            owner,
            invocation_id,
            &executor.cgroup_parent,
            |metadata| executor.resource_limits_for(metadata.workload_class),
            &executor.launch,
            |submission| {
                let duration = std::time::Duration::from_millis(submission.metadata().deadline_ms);
                let deadline = std::time::Instant::now()
                    .checked_add(duration)
                    .ok_or(TransactionError::InvalidContext)?;
                let requested_scope = crate::submission_scope::decode_requested_scope_v1(
                    &submission.metadata().requested_scope,
                )
                .map_err(|_| TransactionError::InvalidContext)?;
                storage.admit_supervisor_invocation_with_deadline_and_scope(
                    crate::storage_manager::SupervisorAdmissionRequest {
                        invocation_id: submission.invocation_id(),
                        token_id,
                        retention_deadline_us: admission
                            .retention_deadline_us()
                            .map_err(|_| TransactionError::InvalidContext)?,
                        max_rows: admission.max_rows,
                        max_bytes: admission.max_bytes,
                        deadline: Some(deadline),
                        workload_class: submission.metadata().workload_class,
                    },
                    requested_scope,
                    &token_secret,
                )
            },
        );
        token_secret.zeroize();
        result.map_err(ClientStartError::ExecutorStart)
    }

    /// Enforce the v1 mode and pre-authentication operation allowlists.
    pub fn permits_request_kind(&self, kind: u16) -> bool {
        let Some(mode) = self.hello else {
            return kind == 1;
        };
        let Some(_) = self.authenticated else {
            return kind == 2 || kind == 11;
        };
        match mode {
            ClientMode::Query => (3..=9).contains(&kind) || kind == 11,
            ClientMode::Control => (8..=10).contains(&kind) || kind == 11,
        }
    }
}

fn encode_lower_hex_id(id: [u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(32);
    for byte in id {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn push_json_string(output: &mut String, value: &str) {
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\u{08}' => output.push_str("\\b"),
            '\u{0c}' => output.push_str("\\f"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            c if c <= '\u{1f}' => {
                use std::fmt::Write as _;
                let _ = write!(output, "\\u{:04x}", c as u32);
            }
            c => output.push(c),
        }
    }
}

fn push_json_optional_i64(output: &mut String, value: Option<i64>) {
    if let Some(value) = value {
        output.push_str(&value.to_string());
    } else {
        output.push_str("null");
    }
}

fn encode_metadata_page(
    snapshot: &ActiveMetadataPageSnapshot,
    start: usize,
    end: usize,
    next_cursor: Option<[u8; 16]>,
) -> Result<Vec<u8>, ClientSessionError> {
    let mut output = match &snapshot.query {
        MetadataPageQuery::Schemas => String::from("{\"schemas\":["),
        MetadataPageQuery::Relations(_) => String::from("{\"relations\":["),
        MetadataPageQuery::Columns { .. } => String::from("{\"columns\":["),
    };
    for index in start..end {
        if index != start {
            output.push(',');
        }
        match (&snapshot.query, &snapshot.rows) {
            (MetadataPageQuery::Schemas, MetadataPageRows::Schemas(rows)) => {
                let row = rows.get(index).ok_or(ClientSessionError::ResourceLimit)?;
                output.push_str("{\"schema_id\":\"");
                output.push_str(&encode_lower_hex_id(row.schema_id));
                output.push_str("\",\"name\":\"");
                push_json_string(&mut output, &row.name);
                output.push_str("\"}");
            }
            (MetadataPageQuery::Relations(_), MetadataPageRows::Relations(rows)) => {
                let row = rows.get(index).ok_or(ClientSessionError::ResourceLimit)?;
                output.push_str("{\"relation_id\":\"");
                output.push_str(&encode_lower_hex_id(row.relation_id));
                output.push_str("\",\"name\":\"");
                push_json_string(&mut output, &row.name);
                output.push_str("\"}");
            }
            (MetadataPageQuery::Columns { .. }, MetadataPageRows::Columns(rows)) => {
                let row = rows.get(index).ok_or(ClientSessionError::ResourceLimit)?;
                output.push_str("{\"column_id\":\"");
                output.push_str(&encode_lower_hex_id(row.column_id));
                output.push_str("\",\"name\":\"");
                push_json_string(&mut output, &row.name);
                output.push_str("\",\"ordinal\":");
                output.push_str(&row.ordinal.to_string());
                output.push_str(" ,\"scalar_type\":\"");
                push_json_string(
                    &mut output,
                    &crate::metadata_discovery::scalar_type_name(row.scalar_type),
                );
                output.push_str("\",\"nullable\":");
                output.push_str(if row.nullable { "true" } else { "false" });
                output.push('}');
            }
            _ => return Err(ClientSessionError::ResourceLimit),
        }
    }
    output.push_str("],\"next_cursor\":");
    match next_cursor {
        Some(cursor) => {
            output.push('"');
            output.push_str(&encode_lower_hex_id(cursor));
            output.push('"');
        }
        None => output.push_str("null"),
    }
    output.push('}');
    Ok(output.into_bytes())
}

fn current_unix_micros() -> Option<i64> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?;
    i64::try_from(duration.as_micros()).ok()
}

impl Drop for ClientConnectionSession {
    fn drop(&mut self) {
        if let Some(secret) = &mut self.token_secret {
            secret.zeroize();
        }
    }
}

#[derive(Default)]
pub struct ClientFrameDecoder {
    header: [u8; CLIENT_FRAME_HEADER_BYTES],
    header_bytes: usize,
    pending_header: Option<Header>,
    pending_payload: Vec<u8>,
}

impl ClientFrameDecoder {
    /// Add any part of the TCP byte stream and return all complete frames.
    /// Header limits are checked before payload storage is reserved.
    pub fn push(&mut self, mut input: &[u8]) -> Result<Vec<ClientFrame>, ClientFrameError> {
        let mut frames = Vec::new();
        while !input.is_empty() {
            if self.pending_header.is_none() {
                let needed = CLIENT_FRAME_HEADER_BYTES - self.header_bytes;
                let copied = needed.min(input.len());
                self.header[self.header_bytes..self.header_bytes + copied]
                    .copy_from_slice(&input[..copied]);
                self.header_bytes += copied;
                input = &input[copied..];
                if self.header_bytes < CLIENT_FRAME_HEADER_BYTES {
                    continue;
                }
                let header = decode_header(&self.header)?;
                self.pending_payload = Vec::new();
                self.pending_payload
                    .try_reserve_exact(header.payload_bytes)
                    .map_err(|_| ClientFrameError::AllocationFailed)?;
                self.pending_header = Some(header);
                if header.payload_bytes == 0 {
                    frames.push(self.take_pending_frame());
                    continue;
                }
            }

            let header = self.pending_header.expect("pending frame header");
            let needed = header.payload_bytes - self.pending_payload.len();
            let copied = needed.min(input.len());
            self.pending_payload.extend_from_slice(&input[..copied]);
            input = &input[copied..];
            if self.pending_payload.len() == header.payload_bytes {
                frames.push(self.take_pending_frame());
            }
        }
        Ok(frames)
    }

    /// Reject an EOF that leaves any partial header or payload buffered.
    pub fn finish(&self) -> Result<(), ClientFrameError> {
        if self.header_bytes != 0 || self.pending_header.is_some() {
            Err(ClientFrameError::IncompleteFrame)
        } else {
            Ok(())
        }
    }

    fn take_pending_frame(&mut self) -> ClientFrame {
        let header = self.pending_header.take().expect("pending frame header");
        let payload = std::mem::take(&mut self.pending_payload);
        self.header_bytes = 0;
        ClientFrame {
            kind: header.kind,
            request_id: header.request_id,
            response_sequence: header.response_sequence,
            payload,
        }
    }
}

impl Drop for ClientFrameDecoder {
    fn drop(&mut self) {
        self.pending_payload.zeroize();
    }
}

impl ClientFrame {
    pub fn encode(&self) -> Result<Vec<u8>, ClientFrameError> {
        let header = Header {
            kind: self.kind,
            payload_bytes: self.payload.len(),
            request_id: self.request_id,
            response_sequence: self.response_sequence,
        };
        validate_header_fields(header)?;
        let payload_bytes =
            u32::try_from(self.payload.len()).map_err(|_| ClientFrameError::PayloadTooLarge)?;
        let total_bytes = CLIENT_FRAME_HEADER_BYTES
            .checked_add(self.payload.len())
            .ok_or(ClientFrameError::PayloadTooLarge)?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(total_bytes)
            .map_err(|_| ClientFrameError::AllocationFailed)?;
        output.extend_from_slice(CLIENT_MAGIC);
        output.extend_from_slice(&1u16.to_le_bytes());
        output.extend_from_slice(&0u16.to_le_bytes());
        output.extend_from_slice(&self.kind.to_le_bytes());
        output.extend_from_slice(&0u16.to_le_bytes());
        output.extend_from_slice(&payload_bytes.to_le_bytes());
        output.extend_from_slice(&self.request_id.to_le_bytes());
        output.extend_from_slice(&self.response_sequence.to_le_bytes());
        output.extend_from_slice(&self.payload);
        Ok(output)
    }
}

fn decode_header(bytes: &[u8; CLIENT_FRAME_HEADER_BYTES]) -> Result<Header, ClientFrameError> {
    if &bytes[..4] != CLIENT_MAGIC {
        return Err(ClientFrameError::InvalidMagic);
    }
    let major = u16::from_le_bytes([bytes[4], bytes[5]]);
    let minor = u16::from_le_bytes([bytes[6], bytes[7]]);
    if major != 1 || minor != 0 {
        return Err(ClientFrameError::UnsupportedVersion { major, minor });
    }
    let kind = u16::from_le_bytes([bytes[8], bytes[9]]);
    let flags = u16::from_le_bytes([bytes[10], bytes[11]]);
    if flags != 0 {
        return Err(ClientFrameError::NonzeroFlags);
    }
    let payload_bytes = u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]) as usize;
    let request_id = u64::from_le_bytes(bytes[16..24].try_into().expect("fixed request ID"));
    let response_sequence =
        u64::from_le_bytes(bytes[24..32].try_into().expect("fixed response sequence"));
    let header = Header {
        kind,
        payload_bytes,
        request_id,
        response_sequence,
    };
    validate_header_fields(header)?;
    Ok(header)
}

fn validate_header_fields(header: Header) -> Result<(), ClientFrameError> {
    let maximum = match header.kind {
        1..=3 | 6..=11 | 100 | 101 | 103 => CLIENT_MAX_CONTROL_BYTES,
        4 | 5 => CLIENT_MAX_CHUNK_BYTES + 24,
        102 => CLIENT_MAX_PAYLOAD_BYTES,
        kind => return Err(ClientFrameError::InvalidKind(kind)),
    };
    if header.payload_bytes > maximum || header.payload_bytes > CLIENT_MAX_PAYLOAD_BYTES {
        return Err(ClientFrameError::PayloadTooLarge);
    }
    if header.request_id == 0 {
        return Err(ClientFrameError::ZeroRequestId);
    }
    match header.kind {
        4 | 5 if header.payload_bytes <= 24 => Err(ClientFrameError::InvalidChunkLength),
        1..=11 if header.response_sequence != 0 => Err(ClientFrameError::InvalidSequence),
        100 | 101 if header.response_sequence != 0 => Err(ClientFrameError::InvalidSequence),
        102 | 103 if header.response_sequence == 0 => Err(ClientFrameError::InvalidSequence),
        _ => Ok(()),
    }
}

#[derive(Default)]
pub struct ClientRequestIds {
    last_request_id: u64,
}

impl ClientRequestIds {
    /// Enforce strictly increasing nonzero IDs on a single client connection.
    pub fn accept(&mut self, frame: &ClientFrame) -> Result<(), ClientFrameError> {
        if !(1..=11).contains(&frame.kind)
            || frame.request_id == 0
            || frame.request_id <= self.last_request_id
        {
            return Err(ClientFrameError::RequestIdsNotIncreasing);
        }
        self.last_request_id = frame.request_id;
        Ok(())
    }
}

#[derive(Default)]
pub struct ClientResponseSequences {
    next_by_request: HashMap<u64, u64>,
}

impl ClientResponseSequences {
    /// Begin streamed responses after the sequence-zero START acceptance.
    pub fn begin_stream(&mut self, request_id: u64) -> Result<(), ClientFrameError> {
        if request_id == 0
            || !self.next_by_request.is_empty()
            || self.next_by_request.contains_key(&request_id)
        {
            return Err(ClientFrameError::UnexpectedResponseSequence);
        }
        self.next_by_request.insert(request_id, 1);
        Ok(())
    }

    /// Validate consecutive RESULT_DATA frames and the terminal sequence.
    pub fn accept_stream_frame(&mut self, frame: &ClientFrame) -> Result<bool, ClientFrameError> {
        let expected = self
            .next_by_request
            .get(&frame.request_id)
            .copied()
            .ok_or(ClientFrameError::UnexpectedResponseSequence)?;
        if !matches!(frame.kind, 102 | 103) || frame.response_sequence != expected {
            return Err(ClientFrameError::UnexpectedResponseSequence);
        }
        if frame.kind == 103 {
            self.next_by_request.remove(&frame.request_id);
            Ok(true)
        } else {
            self.next_by_request.insert(
                frame.request_id,
                expected
                    .checked_add(1)
                    .ok_or(ClientFrameError::UnexpectedResponseSequence)?,
            );
            Ok(false)
        }
    }
}
