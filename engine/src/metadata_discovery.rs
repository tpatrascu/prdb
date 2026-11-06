//! Permission-filtered logical catalog discovery results.

pub const MAX_RELATIONS_PER_PAGE: usize = 128;
pub const MAX_SCHEMAS_PER_PAGE: usize = 128;
pub const MAX_COLUMNS_PER_PAGE: usize = 128;
pub const MAX_METADATA_PAGE_BYTES: usize = 1_048_576;
pub const SCHEMA_CURSOR_TOKEN_KIND: u8 = 1;
pub const RELATION_CURSOR_TOKEN_KIND: u8 = 2;
pub const COLUMN_CURSOR_TOKEN_KIND: u8 = 3;

/// One relation admitted by schema usage and a data grant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VisibleRelation {
    pub relation_id: [u8; 16],
    pub name: String,
}

/// One declared column that is safe for the authorized caller to bind.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VisibleColumn {
    pub column_id: [u8; 16],
    pub name: String,
    pub ordinal: u32,
    pub scalar_type: crate::tuple::ScalarType,
    pub nullable: bool,
}

/// The declared column shape of one discoverable relation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VisibleRelationSchema {
    pub relation_id: [u8; 16],
    pub columns: Vec<VisibleColumn>,
}

/// An exact-name resolution result containing only an independently
/// authorized relation and its declared shape.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizedRelationDescription {
    pub relation_id: [u8; 16],
    pub name: String,
    pub columns: Vec<VisibleColumn>,
}

/// Opaque continuation for column descriptions in one invocation snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnScanCursor {
    pub(crate) token: [u8; 16],
}

impl ColumnScanCursor {
    /// Encode the cursor for the bounded IPC payload.
    pub fn to_bytes(&self) -> [u8; 16] {
        self.token
    }

    /// Decode a cursor token from its fixed-width wire form.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 16 || bytes[0] != COLUMN_CURSOR_TOKEN_KIND {
            return None;
        }
        Some(Self {
            token: bytes.try_into().ok()?,
        })
    }
}

/// One bounded page of declared columns for a discoverable relation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VisibleColumnPage {
    pub relation_id: [u8; 16],
    pub columns: Vec<VisibleColumn>,
    pub continuation: Option<ColumnScanCursor>,
}

/// One schema visible through an effective USAGE grant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VisibleSchema {
    pub schema_id: [u8; 16],
    pub name: String,
}

/// Opaque continuation for schema discovery in one invocation snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaScanCursor {
    pub(crate) token: [u8; 16],
}

impl SchemaScanCursor {
    /// Encode the cursor for the bounded IPC payload.
    pub fn to_bytes(&self) -> [u8; 16] {
        self.token
    }

    /// Decode a cursor token from its fixed-width wire form.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 16 || bytes[0] != SCHEMA_CURSOR_TOKEN_KIND {
            return None;
        }
        Some(Self {
            token: bytes.try_into().ok()?,
        })
    }
}

/// One bounded page of visible schemas.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VisibleSchemaPage {
    pub schemas: Vec<VisibleSchema>,
    pub continuation: Option<SchemaScanCursor>,
}

/// One row exposed by the read-only `information_schema.schemata` relation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InformationSchemaSchemataRow {
    pub schema_name: String,
}

/// One bounded page from `information_schema.schemata`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InformationSchemaSchemataPage {
    pub rows: Vec<InformationSchemaSchemataRow>,
    pub continuation: Option<SchemaScanCursor>,
}

/// One table row exposed for an authorized schema by `information_schema.tables`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InformationSchemaTableRow {
    pub table_schema: String,
    pub table_name: String,
    pub table_type: String,
}

/// One bounded schema-scoped page from `information_schema.tables`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InformationSchemaTablesPage {
    pub rows: Vec<InformationSchemaTableRow>,
    pub continuation: Option<RelationScanCursor>,
}

/// One declared column row exposed by `information_schema.columns`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InformationSchemaColumnRow {
    pub table_schema: String,
    pub table_name: String,
    pub column_name: String,
    pub ordinal_position: u32,
    pub data_type: String,
    pub is_nullable: String,
    pub character_maximum_length: Option<u32>,
    pub numeric_precision: Option<u32>,
    pub numeric_scale: Option<u32>,
}

/// One bounded relation-scoped page from `information_schema.columns`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InformationSchemaColumnsPage {
    pub rows: Vec<InformationSchemaColumnRow>,
    pub continuation: Option<ColumnScanCursor>,
}

/// Opaque continuation for relation discovery in one invocation snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationScanCursor {
    pub(crate) token: [u8; 16],
}

impl RelationScanCursor {
    /// Encode the cursor for the bounded IPC payload.
    pub fn to_bytes(&self) -> [u8; 16] {
        self.token
    }

    /// Decode a cursor token from its fixed-width wire form.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 16 || bytes[0] != RELATION_CURSOR_TOKEN_KIND {
            return None;
        }
        Some(Self {
            token: bytes.try_into().ok()?,
        })
    }
}

/// One bounded page of visible relations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VisibleRelationPage {
    pub relations: Vec<VisibleRelation>,
    pub continuation: Option<RelationScanCursor>,
}
