//! Permission-filtered logical catalog discovery results.

pub const MAX_RELATIONS_PER_PAGE: usize = 128;
pub const MAX_SCHEMAS_PER_PAGE: usize = 128;
pub const MAX_COLUMNS_PER_PAGE: usize = 128;
pub const MAX_METADATA_PAGE_BYTES: usize = 1_048_576;

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
    pub(crate) invocation_id: [u8; 16],
    pub(crate) relation_id: [u8; 16],
    pub(crate) generation: u64,
    pub(crate) after_ordinal: u32,
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

/// Opaque continuation for relation discovery in one invocation snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationScanCursor {
    pub(crate) invocation_id: [u8; 16],
    pub(crate) schema_id: [u8; 16],
    pub(crate) generation: u64,
    pub(crate) after_relation_id: [u8; 16],
}

/// One bounded page of visible relations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VisibleRelationPage {
    pub relations: Vec<VisibleRelation>,
    pub continuation: Option<RelationScanCursor>,
}
