//! Permission-filtered logical catalog discovery results.

pub const MAX_RELATIONS_PER_PAGE: usize = 128;
pub const MAX_METADATA_PAGE_BYTES: usize = 1_048_576;

/// One relation admitted by schema usage and a data grant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VisibleRelation {
    pub relation_id: [u8; 16],
    pub name: String,
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
