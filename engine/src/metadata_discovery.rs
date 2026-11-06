//! Permission-filtered logical catalog discovery results.

pub const MAX_RELATIONS_PER_PAGE: usize = 128;
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
