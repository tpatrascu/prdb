use crate::client_submit::AdminColumnDefinition;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTableSql {
    pub schema: String,
    pub table: String,
    pub if_not_exists: bool,
    pub columns: Vec<AdminColumnDefinition>,
    pub primary_key_columns: Option<Vec<String>>,
    pub primary_key_name: Option<String>,
    pub unique_constraints: Vec<Vec<String>>,
    pub unique_constraint_names: Vec<Option<String>>,
    pub check_constraints: Vec<CheckExpressionSql>,
    pub check_constraint_names: Vec<Option<String>>,
    pub foreign_key_names: Vec<Option<String>>,
    pub foreign_keys: Vec<CreateTableForeignKeySql>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTableForeignKeySql {
    pub name: Option<String>,
    pub columns: Vec<String>,
    pub referenced_schema: String,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateSchemaSql {
    pub schema: String,
    pub owner: CreateSchemaOwnerSql,
    pub if_not_exists: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlIdentityKind {
    Either,
    User,
    Role,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateSchemaOwnerSql {
    CurrentUser,
    Id([u8; 16]),
    Identity { name: String, kind: SqlIdentityKind },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RenameSchemaSql {
    pub schema: String,
    pub new_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RenameTableSql {
    pub schema: String,
    pub table: String,
    pub new_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DropTableSql {
    pub schema: String,
    pub table: String,
    pub if_exists: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TruncateTableSql {
    pub schema: String,
    pub table: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DropSchemaSql {
    pub schema: String,
    pub if_exists: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnerTransferSql {
    pub schema: String,
    pub table: Option<String>,
    pub owner_name: String,
    pub owner_kind: SqlIdentityKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyConstraintKind {
    PrimaryKey,
    Unique,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddKeyConstraintSql {
    pub schema: String,
    pub table: String,
    pub name: Option<String>,
    pub columns: Vec<String>,
    pub kind: KeyConstraintKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddForeignKeySql {
    pub schema: String,
    pub table: String,
    pub name: Option<String>,
    pub columns: Vec<String>,
    pub referenced_schema: String,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DropConstraintSql {
    pub schema: String,
    pub table: String,
    pub constraint_id: [u8; 16],
    pub constraint_name: Option<String>,
    pub if_exists: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddCheckConstraintSql {
    pub schema: String,
    pub table: String,
    pub name: Option<String>,
    pub expression: CheckExpressionSql,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterColumnNullabilitySql {
    pub schema: String,
    pub table: String,
    pub column: String,
    pub nullable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WidenVarcharSql {
    pub schema: String,
    pub table: String,
    pub column: String,
    pub max_chars: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterColumnDefaultSql {
    pub schema: String,
    pub table: String,
    pub column: String,
    pub default: Option<DefaultLiteralSql>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DefaultLiteralSql {
    Null,
    CurrentPrincipalId,
    Boolean(bool),
    Number(String),
    String(String),
    Bytea(Vec<u8>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddColumnSql {
    pub schema: String,
    pub table: String,
    pub if_not_exists: bool,
    pub column: AdminColumnDefinition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RenameColumnSql {
    pub schema: String,
    pub table: String,
    pub column: String,
    pub new_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DropColumnSql {
    pub schema: String,
    pub table: String,
    pub column: String,
    pub if_exists: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateIndexSql {
    pub schema: String,
    pub table: String,
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub if_not_exists: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DropIndexSql {
    pub schema: String,
    pub name: String,
    pub if_exists: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebuildIndexSql {
    pub schema: String,
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterIndexRebuildSql {
    pub schema: String,
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RenameIndexSql {
    pub schema: String,
    pub name: String,
    pub new_name: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateIdentityKind {
    User,
    Role,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateIdentitySql {
    pub kind: CreateIdentityKind,
    pub if_not_exists: bool,
    pub name: String,
    pub login_enabled: bool,
    pub workload_profile: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SetUserWorkloadProfileSql {
    pub user_name: String,
    pub profile_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateWorkloadProfileSql {
    pub if_not_exists: bool,
    pub name: String,
    pub base_profile: Option<String>,
    pub settings: Vec<(String, u64)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterWorkloadProfileSql {
    pub name: String,
    pub settings: Vec<(String, u64)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RenameWorkloadProfileSql {
    pub old_name: String,
    pub new_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DropWorkloadProfileSql {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RenameIdentitySql {
    pub kind: CreateIdentityKind,
    pub old_name: String,
    pub new_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DropIdentitySql {
    pub kind: CreateIdentityKind,
    pub name: String,
    pub if_exists: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SetUserLoginSql {
    pub name: String,
    pub enabled: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoleMembershipSql {
    pub grant: bool,
    pub role_name: String,
    pub member_name: String,
    pub member_is_role: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SystemPrivilegeSql {
    pub grant: bool,
    pub privilege: String,
    pub grantee_name: String,
    pub grantee_is_role: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectPrivilegeSql {
    pub grant: bool,
    pub privileges: Vec<String>,
    pub schema_name: String,
    pub table_name: Option<String>,
    pub column_names: Vec<String>,
    pub grantee_name: String,
    pub grantee_is_role: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckExpressionSql {
    Column(String),
    Literal(CheckLiteralSql),
    CurrentPrincipalId,
    HasRole(String),
    Add(Box<CheckExpressionSql>, Box<CheckExpressionSql>),
    Subtract(Box<CheckExpressionSql>, Box<CheckExpressionSql>),
    Multiply(Box<CheckExpressionSql>, Box<CheckExpressionSql>),
    Modulo(Box<CheckExpressionSql>, Box<CheckExpressionSql>),
    Divide(Box<CheckExpressionSql>, Box<CheckExpressionSql>),
    Negate(Box<CheckExpressionSql>),
    IsNull(Box<CheckExpressionSql>),
    IsNotNull(Box<CheckExpressionSql>),
    BooleanTest {
        expression: Box<CheckExpressionSql>,
        test: CheckBooleanTest,
        negated: bool,
    },
    Compare {
        operator: CheckComparisonOperator,
        left: Box<CheckExpressionSql>,
        right: Box<CheckExpressionSql>,
    },
    In {
        expression: Box<CheckExpressionSql>,
        values: Vec<Box<CheckExpressionSql>>,
        negated: bool,
    },
    Between {
        expression: Box<CheckExpressionSql>,
        lower: Box<CheckExpressionSql>,
        upper: Box<CheckExpressionSql>,
        negated: bool,
    },
    DistinctFrom {
        expression: Box<CheckExpressionSql>,
        right: Box<CheckExpressionSql>,
        negated: bool,
    },
    Not(Box<CheckExpressionSql>),
    And(Vec<CheckExpressionSql>),
    Or(Vec<CheckExpressionSql>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyRecipientSql {
    pub name: String,
    pub is_role: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatePolicySql {
    pub replace_existing: bool,
    pub schema: String,
    pub table: String,
    pub name: String,
    pub is_cell: bool,
    pub columns: Vec<String>,
    pub operation: String,
    pub recipients: Vec<PolicyRecipientSql>,
    pub effect: String,
    pub using_expression: Option<CheckExpressionSql>,
    pub with_check_expression: Option<CheckExpressionSql>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DropPolicySql {
    pub schema: String,
    pub table: String,
    pub name: String,
    pub is_cell: bool,
    pub if_exists: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckComparisonOperator {
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckBooleanTest {
    True,
    False,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckLiteralSql {
    Null,
    Boolean(bool),
    Number(String),
    String(String),
    Bytea(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DdlSqlError {
    InvalidStatement,
}
