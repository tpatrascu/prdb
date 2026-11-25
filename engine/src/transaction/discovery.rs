use super::catalog_loader::*;
use super::*;
use crate::authorization::{
    AccessTarget, AuthorityDependency, AuthorizationContext, AuthorizedProjection, Expr, Grant,
    Operation, Policy, Subject, has_permission,
};
use crate::catalog::{CatalogRecord, decode_registered_envelope};
use crate::expression::decode_catalog_expression_record;
use crate::metadata_discovery::{
    AuthorizedRelationDescription, COLUMN_CURSOR_TOKEN_KIND, ColumnScanCursor,
    MAX_COLUMNS_PER_PAGE, MAX_METADATA_PAGE_BYTES, MAX_RELATIONS_PER_PAGE, MAX_SCHEMAS_PER_PAGE,
    RELATION_CURSOR_TOKEN_KIND, RelationScanCursor, SCHEMA_CURSOR_TOKEN_KIND, SchemaScanCursor,
    VisibleColumn, VisibleColumnPage, VisibleRelation, VisibleRelationPage, VisibleRelationSchema,
    VisibleSchema, VisibleSchemaPage,
};
use std::collections::{HashMap, HashSet};

pub(super) struct CatalogTableProjection {
    pub(super) schema: CatalogTableSchema,
    pub(super) context: AuthorizationContext,
    pub(super) grants: Vec<Grant>,
    pub(super) policies: Vec<Policy>,
    pub(super) checks: Vec<Expr>,
    pub(super) policy_driving_columns: Vec<[u8; 16]>,
}

impl CatalogTableProjection {
    pub(super) fn as_projection(&self) -> AuthorizedProjection<'_> {
        AuthorizedProjection {
            columns: &self.schema.columns,
            column_ids: &self.schema.column_ids,
            schema_version: self.schema.schema_version,
            schema_id: self.schema.schema_id,
            context: &self.context,
            grants: &self.grants,
            policies: &self.policies,
            checks: &self.checks,
            policy_driving_columns: &self.policy_driving_columns,
            dependencies: &[],
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ActiveRelationScan {
    pub(super) schema_id: [u8; 16],
    pub(super) extra_bytes_per_relation: usize,
    pub(super) principal_id: [u8; 16],
    pub(super) role_ids: HashSet<[u8; 16]>,
    pub(super) scope: HashSet<(Operation, AccessTarget)>,
    pub(super) grants: Vec<Grant>,
    pub(super) dependencies: Vec<AuthorityDependency>,
    pub(super) continuation: Option<([u8; 16], [u8; 16])>,
    pub(super) used_tokens: HashSet<[u8; 16]>,
}

pub(super) struct MetadataDiscoveryAccess<'a> {
    pub(super) context: &'a AuthorizationContext,
    pub(super) grants: &'a [Grant],
    pub(super) dependencies: &'a [AuthorityDependency],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ActiveSchemaScan {
    pub(super) principal_id: [u8; 16],
    pub(super) role_ids: HashSet<[u8; 16]>,
    pub(super) scope: HashSet<(Operation, AccessTarget)>,
    pub(super) grants: Vec<Grant>,
    pub(super) dependencies: Vec<AuthorityDependency>,
    pub(super) continuation: Option<([u8; 16], [u8; 16])>,
    pub(super) used_tokens: HashSet<[u8; 16]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ActiveColumnScan {
    pub(super) relation_id: [u8; 16],
    pub(super) extra_bytes_per_column: usize,
    pub(super) principal_id: [u8; 16],
    pub(super) role_ids: HashSet<[u8; 16]>,
    pub(super) scope: HashSet<(Operation, AccessTarget)>,
    pub(super) grants: Vec<Grant>,
    pub(super) dependencies: Vec<AuthorityDependency>,
    pub(super) continuation: Option<([u8; 16], u32)>,
    pub(super) used_tokens: HashSet<[u8; 16]>,
}

impl<'db> InvocationTransaction<'db> {
    /// Read one catalog definition from the pinned snapshot and retain its exact
    /// bytes for schema/authority revalidation at final commit.
    pub fn read_catalog_record(
        &mut self,
        key: &[u8],
    ) -> Result<Option<CatalogRecord>, TransactionError> {
        if self.aborted {
            return Err(TransactionError::Aborted);
        }
        let bytes = match self.pinned_snapshot()?.get_catalog_value(key) {
            Ok(bytes) => bytes,
            Err(error) => {
                self.abort()?;
                return Err(TransactionError::Storage(error.to_string()));
            }
        };
        let Some(bytes) = bytes else {
            self.catalog_guards.insert(key.to_vec(), None);
            return Ok(None);
        };
        let record = match decode_registered_envelope(&bytes) {
            Ok(record) if record.logical_key == key => record,
            _ => {
                self.abort()?;
                return Err(TransactionError::CorruptCatalog);
            }
        };
        if record.family_code == 7 && decode_catalog_expression_record(&record).is_err() {
            self.abort()?;
            return Err(TransactionError::CorruptCatalog);
        }
        self.catalog_guards.insert(key.to_vec(), Some(bytes));
        Ok(Some(record))
    }

    /// List visible relations using grants derived from the admitted
    /// principal's pinned catalog. The caller supplies only the schema and
    /// page continuation.
    pub fn list_catalog_authorized_relations(
        &mut self,
        schema_id: [u8; 16],
        page_limit: usize,
        continuation: Option<RelationScanCursor>,
    ) -> Result<Option<VisibleRelationPage>, TransactionError> {
        self.list_catalog_authorized_relations_with_extra_bytes(
            schema_id,
            page_limit,
            continuation,
            0,
        )
    }

    /// Describe a relation's declared columns using grants derived from the
    /// pinned catalog. Both schema and relation IDs are references only.
    pub fn describe_catalog_authorized_columns_page(
        &mut self,
        schema_id: [u8; 16],
        table_id: [u8; 16],
        page_limit: usize,
        continuation: Option<ColumnScanCursor>,
    ) -> Result<Option<VisibleColumnPage>, TransactionError> {
        let result = (|| {
            if page_limit == 0 || page_limit > MAX_COLUMNS_PER_PAGE {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            if schema_id == INFORMATION_SCHEMA_ID {
                if continuation.is_some() {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
                let Some(description) = builtin_information_schema_relation_schema(table_id) else {
                    return Ok(None);
                };
                if description.columns.len() > page_limit {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
                return Ok(Some(VisibleColumnPage {
                    relation_id: table_id,
                    columns: description.columns,
                    continuation: None,
                }));
            }
            if schema_id == [0; 16] {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let principal_id = self.principal_id;
            let (role_ids, grants) = self.load_catalog_relation_discovery_grants(schema_id)?;
            let scope = self.scope_from_grants(&grants);
            let context = AuthorizationContext {
                principal_id,
                role_ids,
                scope,
            };
            self.describe_authorized_columns_page(
                table_id,
                page_limit,
                continuation,
                &context,
                &grants,
                &[],
            )
        })();
        match result {
            Ok(page) => Ok(page),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    pub(super) fn list_catalog_authorized_relations_with_extra_bytes(
        &mut self,
        schema_id: [u8; 16],
        page_limit: usize,
        continuation: Option<RelationScanCursor>,
        extra_bytes_per_relation: usize,
    ) -> Result<Option<VisibleRelationPage>, TransactionError> {
        let result = (|| {
            if schema_id == [0; 16] || page_limit == 0 || page_limit > MAX_RELATIONS_PER_PAGE {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let principal_id = self.principal_id;
            let (role_ids, grants) = self.load_catalog_relation_discovery_grants(schema_id)?;
            let scope = self.scope_from_grants(&grants);
            let context = AuthorizationContext {
                principal_id,
                role_ids,
                scope,
            };
            self.list_authorized_relations_with_extra_bytes(
                schema_id,
                page_limit,
                continuation,
                extra_bytes_per_relation,
                MetadataDiscoveryAccess {
                    context: &context,
                    grants: &grants,
                    dependencies: &[],
                },
            )
        })();
        match result {
            Ok(page) => Ok(page),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Resolve a qualified schema and relation name using authority derived
    /// from the admitted invocation's pinned catalog.
    pub fn resolve_catalog_authorized_relation(
        &mut self,
        schema_name: &str,
        relation_name: &str,
    ) -> Result<Option<AuthorizedRelationDescription>, TransactionError> {
        let result = (|| {
            if schema_name.is_empty()
                || schema_name.len() > MAX_METADATA_PAGE_BYTES
                || relation_name.is_empty()
                || relation_name.len() > MAX_METADATA_PAGE_BYTES
            {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let principal_id = self.principal_id;
            let (schema_roles, schema_grants) =
                self.load_catalog_schema_usage_grants(principal_id)?;
            let schema_scope = self.scope_from_grants(&schema_grants);
            let schema_context = AuthorizationContext {
                principal_id,
                role_ids: schema_roles,
                scope: schema_scope,
            };
            let Some(schema) =
                self.resolve_authorized_schema(schema_name, &schema_context, &schema_grants, &[])?
            else {
                return Ok(None);
            };

            if schema.schema_id == INFORMATION_SCHEMA_ID {
                return self.resolve_authorized_relation(
                    schema.schema_id,
                    relation_name,
                    &schema_context,
                    &schema_grants,
                    &[],
                );
            }

            let (relation_roles, relation_grants) =
                self.load_catalog_relation_discovery_grants(schema.schema_id)?;
            let relation_scope = self.scope_from_grants(&relation_grants);
            let relation_context = AuthorizationContext {
                principal_id,
                role_ids: relation_roles,
                scope: relation_scope,
            };
            self.resolve_authorized_relation(
                schema.schema_id,
                relation_name,
                &relation_context,
                &relation_grants,
                &[],
            )
        })();
        match result {
            Ok(description) => Ok(description),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// List relations in one schema only when the invocation has schema usage
    /// and at least one table or column data grant. The role closure and each
    /// supplied grant are checked against this transaction's pinned catalog.
    pub fn list_authorized_relations(
        &mut self,
        schema_id: [u8; 16],
        page_limit: usize,
        continuation: Option<RelationScanCursor>,
        context: &AuthorizationContext,
        grants: &[Grant],
        dependencies: &[AuthorityDependency],
    ) -> Result<Option<VisibleRelationPage>, TransactionError> {
        self.list_authorized_relations_with_extra_bytes(
            schema_id,
            page_limit,
            continuation,
            0,
            MetadataDiscoveryAccess {
                context,
                grants,
                dependencies,
            },
        )
    }

    pub(super) fn list_authorized_relations_with_extra_bytes(
        &mut self,
        schema_id: [u8; 16],
        page_limit: usize,
        continuation: Option<RelationScanCursor>,
        extra_bytes_per_relation: usize,
        access: MetadataDiscoveryAccess<'_>,
    ) -> Result<Option<VisibleRelationPage>, TransactionError> {
        let result = (|| {
            let MetadataDiscoveryAccess {
                context,
                grants,
                dependencies,
            } = access;
            if schema_id == [0; 16]
                || page_limit == 0
                || page_limit > MAX_RELATIONS_PER_PAGE
                || grants.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
            {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            self.check_deadline()?;
            self.capture_authority_dependencies(context, dependencies)?;
            let pinned_roles = self.load_catalog_role_closure(context.principal_id)?;
            if pinned_roles != context.role_ids {
                return Err(TransactionError::InvalidContext);
            }
            for grant in grants {
                self.validate_metadata_grant(context, *grant)?;
            }

            if !has_permission(
                context,
                grants,
                Operation::Usage,
                AccessTarget::Schema(schema_id),
            ) {
                return Ok(None);
            }

            let mut active_scan = match continuation.as_ref() {
                Some(cursor) => {
                    let Some(active) = self.active_relation_scan.as_ref() else {
                        return Err(TransactionError::InvalidMetadataRequest);
                    };
                    if active.schema_id != schema_id
                        || active.extra_bytes_per_relation != extra_bytes_per_relation
                        || active.principal_id != context.principal_id
                        || active.role_ids != context.role_ids
                        || active.scope != context.scope
                        || active.grants != grants
                        || active.dependencies != dependencies
                        || active
                            .continuation
                            .is_none_or(|(token, _)| token != cursor.token)
                    {
                        return Err(TransactionError::InvalidMetadataRequest);
                    }
                    active.clone()
                }
                None => ActiveRelationScan {
                    schema_id,
                    principal_id: context.principal_id,
                    role_ids: context.role_ids.clone(),
                    scope: context.scope.clone(),
                    grants: grants.to_vec(),
                    dependencies: dependencies.to_vec(),
                    extra_bytes_per_relation,
                    continuation: None,
                    used_tokens: HashSet::new(),
                },
            };

            let schema_object_key = catalog_id_key(2, schema_id);
            let Some(schema_object) = self.read_catalog_record(&schema_object_key)? else {
                return Ok(None);
            };
            if schema_object.family_code != 2
                || catalog_u16(&schema_object, 1)? != 1
                || catalog_optional_id(&schema_object, 2)?.is_some()
                || catalog_id(&schema_object, 4)? == [0; 16]
                || catalog_u64(&schema_object, 5)? == 0
                || catalog_u16(&schema_object, 6)? != 1
            {
                return Err(TransactionError::CorruptCatalog);
            }
            let schema_key = catalog_id_key(3, schema_id);
            let schema = self
                .read_catalog_record(&schema_key)?
                .ok_or(TransactionError::CorruptCatalog)?;
            if schema.family_code != 3 {
                return Err(TransactionError::CorruptCatalog);
            }
            let mut table_ids = self.scan_relation_ids_for_schema(schema_id)?;
            if table_ids.is_empty() {
                // Retain bounded support for snapshots written before the
                // schema-scoped relation-name index was maintained.
                for table_record in self.scan_catalog_family(5)? {
                    if table_record.family_code != 5 {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    if catalog_id(&table_record, 1)? != schema_id {
                        continue;
                    }
                    let table_id = table_record
                        .logical_key
                        .get(3..19)
                        .ok_or(TransactionError::CorruptCatalog)?
                        .try_into()
                        .map_err(|_| TransactionError::CorruptCatalog)?;
                    if table_id == [0; 16] {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    table_ids.push(table_id);
                }
            }
            table_ids.sort_unstable();
            if table_ids.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(TransactionError::CorruptCatalog);
            }

            let mut table_grants = HashSet::new();
            let mut column_grants = HashMap::<[u8; 16], HashSet<[u8; 16]>>::new();
            for grant in grants {
                let subject_matches = match grant.subject {
                    Subject::Principal(id) => id == context.principal_id,
                    Subject::Role(id) => context.role_ids.contains(&id),
                };
                if !subject_matches || !context.scope.contains(&(grant.operation, grant.target)) {
                    continue;
                }
                match (grant.operation, grant.target) {
                    (
                        Operation::Select
                        | Operation::Insert
                        | Operation::Update
                        | Operation::Delete,
                        AccessTarget::Table(table_id),
                    ) => {
                        table_grants.insert(table_id);
                    }
                    (
                        Operation::Select | Operation::Insert | Operation::Update,
                        AccessTarget::Column {
                            table_id,
                            column_id,
                        },
                    ) => {
                        column_grants.entry(table_id).or_default().insert(column_id);
                    }
                    _ => {}
                }
            }

            let after_id = if let Some(cursor) = continuation.as_ref() {
                let (token, after_id) = active_scan
                    .continuation
                    .take()
                    .ok_or(TransactionError::InvalidMetadataRequest)?;
                if token != cursor.token {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
                Some(after_id)
            } else {
                None
            };
            let mut relations = Vec::with_capacity(page_limit);
            let mut page_bytes = 0usize;
            let mut has_more = false;
            for table_id in table_ids {
                if after_id.is_some_and(|after| table_id <= after) {
                    continue;
                }
                let mut discoverable = table_grants.contains(&table_id);
                if !discoverable && let Some(column_ids) = column_grants.get(&table_id) {
                    for column_id in column_ids {
                        self.validate_discovery_column(table_id, *column_id)?;
                    }
                    discoverable = !column_ids.is_empty();
                }
                if !discoverable {
                    continue;
                }
                let relation = self.read_discovery_relation(schema_id, table_id)?;
                let encoded_size = relation
                    .name
                    .len()
                    .checked_add(24)
                    .and_then(|size| size.checked_add(extra_bytes_per_relation))
                    .ok_or(TransactionError::MetadataResultTooLarge)?;
                if relations.len() == page_limit
                    || page_bytes.saturating_add(encoded_size) > MAX_METADATA_PAGE_BYTES
                {
                    if relations.is_empty() {
                        return Err(TransactionError::MetadataResultTooLarge);
                    }
                    has_more = true;
                    break;
                }
                page_bytes += encoded_size;
                relations.push(relation);
            }
            let continuation = if has_more {
                let after_id = relations
                    .last()
                    .map(|relation| relation.relation_id)
                    .ok_or(TransactionError::InvalidMetadataRequest)?;
                let token = new_metadata_cursor_token(
                    &mut active_scan.used_tokens,
                    RELATION_CURSOR_TOKEN_KIND,
                )?;
                active_scan.continuation = Some((token, after_id));
                Some(RelationScanCursor { token })
            } else {
                active_scan.continuation = None;
                None
            };
            self.active_relation_scan = Some(active_scan);
            Ok(Some(VisibleRelationPage {
                relations,
                continuation,
            }))
        })();
        match result {
            Ok(page) => Ok(page),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Resolve an exact relation name from filtered catalog pages, then
    /// independently recheck authority before returning its declared shape.
    /// The reserved information_schema relations have fixed public shapes.
    pub fn resolve_authorized_relation(
        &mut self,
        schema_id: [u8; 16],
        relation_name: &str,
        context: &AuthorizationContext,
        grants: &[Grant],
        dependencies: &[AuthorityDependency],
    ) -> Result<Option<AuthorizedRelationDescription>, TransactionError> {
        let result = (|| {
            if relation_name.is_empty() || relation_name.len() > MAX_METADATA_PAGE_BYTES {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            if schema_id == INFORMATION_SCHEMA_ID {
                self.check_deadline()?;
                let Some(relation) = builtin_information_schema_relation(relation_name) else {
                    return Ok(None);
                };
                let schema = builtin_information_schema_relation_schema(relation.relation_id)
                    .ok_or(TransactionError::CorruptCatalog)?;
                let response_bytes = relation
                    .name
                    .len()
                    .checked_add(24)
                    .and_then(|bytes| {
                        schema.columns.iter().try_fold(bytes, |total, column| {
                            total
                                .checked_add(column.name.len())
                                .and_then(|sum| sum.checked_add(32))
                        })
                    })
                    .ok_or(TransactionError::MetadataResultTooLarge)?;
                if response_bytes > MAX_METADATA_PAGE_BYTES {
                    return Err(TransactionError::MetadataResultTooLarge);
                }
                return Ok(Some(AuthorizedRelationDescription {
                    relation_id: relation.relation_id,
                    name: relation.name,
                    columns: schema.columns,
                }));
            }
            let indexed = self
                .pinned_snapshot()?
                .get_relation_name_index_value(schema_id, relation_name.as_bytes())
                .map_err(|error| TransactionError::Storage(error.to_string()))?;
            if let Some(indexed) = indexed {
                let relation_id: [u8; 16] = indexed
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if relation_id == [0; 16] {
                    return Err(TransactionError::CorruptCatalog);
                }
                let Some(description) =
                    self.describe_authorized_columns(relation_id, context, grants, dependencies)?
                else {
                    return Ok(None);
                };
                let relation = self.read_discovery_relation(schema_id, relation_id)?;
                if relation.name != relation_name {
                    return Err(TransactionError::CorruptCatalog);
                }
                let initial_response_bytes = relation
                    .name
                    .len()
                    .checked_add(24)
                    .ok_or(TransactionError::MetadataResultTooLarge)?;
                let response_bytes = description
                    .columns
                    .iter()
                    .try_fold(initial_response_bytes, |bytes, column| {
                        bytes
                            .checked_add(column.name.len())
                            .and_then(|total| total.checked_add(32))
                    })
                    .ok_or(TransactionError::MetadataResultTooLarge)?;
                if response_bytes > MAX_METADATA_PAGE_BYTES {
                    return Err(TransactionError::MetadataResultTooLarge);
                }
                return Ok(Some(AuthorizedRelationDescription {
                    relation_id,
                    name: relation.name,
                    columns: description.columns,
                }));
            }
            let mut continuation = None;
            loop {
                let Some(page) = self.list_authorized_relations(
                    schema_id,
                    MAX_RELATIONS_PER_PAGE,
                    continuation,
                    context,
                    grants,
                    dependencies,
                )?
                else {
                    return Ok(None);
                };
                let relation = page
                    .relations
                    .into_iter()
                    .find(|relation| relation.name == relation_name);
                if let Some(relation) = relation {
                    let Some(description) = self.describe_authorized_columns(
                        relation.relation_id,
                        context,
                        grants,
                        dependencies,
                    )?
                    else {
                        return Ok(None);
                    };
                    let initial_response_bytes = relation
                        .name
                        .len()
                        .checked_add(24)
                        .ok_or(TransactionError::MetadataResultTooLarge)?;
                    let response_bytes = description
                        .columns
                        .iter()
                        .try_fold(initial_response_bytes, |bytes, column| {
                            bytes
                                .checked_add(column.name.len())
                                .and_then(|total| total.checked_add(32))
                        })
                        .ok_or(TransactionError::MetadataResultTooLarge)?;
                    if response_bytes > MAX_METADATA_PAGE_BYTES {
                        return Err(TransactionError::MetadataResultTooLarge);
                    }
                    return Ok(Some(AuthorizedRelationDescription {
                        relation_id: relation.relation_id,
                        name: relation.name,
                        columns: description.columns,
                    }));
                }
                continuation = page.continuation;
                if continuation.is_none() {
                    return Ok(None);
                }
            }
        })();
        match result {
            Ok(description) => Ok(description),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Describe declared column shape after checking relation discovery authority.
    pub fn describe_authorized_columns(
        &mut self,
        table_id: [u8; 16],
        context: &AuthorizationContext,
        grants: &[Grant],
        dependencies: &[AuthorityDependency],
    ) -> Result<Option<VisibleRelationSchema>, TransactionError> {
        let result = (|| {
            let Some(trusted_schema) =
                self.load_authorized_discovery_schema(table_id, context, grants, dependencies)?
            else {
                return Ok(None);
            };
            let mut columns = Vec::with_capacity(trusted_schema.column_ids.len());
            let mut response_bytes = 0usize;
            for ordinal in 0..trusted_schema.column_ids.len() {
                let column = self.read_visible_column(table_id, &trusted_schema, ordinal)?;
                response_bytes = response_bytes
                    .checked_add(column.name.len())
                    .and_then(|bytes| bytes.checked_add(32))
                    .ok_or(TransactionError::MetadataResultTooLarge)?;
                if response_bytes > MAX_METADATA_PAGE_BYTES {
                    return Err(TransactionError::MetadataResultTooLarge);
                }
                columns.push(column);
            }
            Ok(Some(VisibleRelationSchema {
                relation_id: table_id,
                columns,
            }))
        })();
        match result {
            Ok(description) => Ok(description),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Return one bounded page of a relation's declared columns.
    pub fn describe_authorized_columns_page(
        &mut self,
        table_id: [u8; 16],
        page_limit: usize,
        continuation: Option<ColumnScanCursor>,
        context: &AuthorizationContext,
        grants: &[Grant],
        dependencies: &[AuthorityDependency],
    ) -> Result<Option<VisibleColumnPage>, TransactionError> {
        self.describe_authorized_columns_page_with_extra_bytes(
            table_id,
            page_limit,
            continuation,
            0,
            MetadataDiscoveryAccess {
                context,
                grants,
                dependencies,
            },
        )
    }

    pub(super) fn describe_authorized_columns_page_with_extra_bytes(
        &mut self,
        table_id: [u8; 16],
        page_limit: usize,
        continuation: Option<ColumnScanCursor>,
        extra_bytes_per_column: usize,
        access: MetadataDiscoveryAccess<'_>,
    ) -> Result<Option<VisibleColumnPage>, TransactionError> {
        let result = (|| {
            let MetadataDiscoveryAccess {
                context,
                grants,
                dependencies,
            } = access;
            if page_limit == 0 || page_limit > MAX_COLUMNS_PER_PAGE {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let Some(trusted_schema) =
                self.load_authorized_discovery_schema(table_id, context, grants, dependencies)?
            else {
                return Ok(None);
            };
            let mut active_scan = match continuation.as_ref() {
                Some(cursor) => {
                    let Some(active) = self.active_column_scan.as_ref() else {
                        return Err(TransactionError::InvalidMetadataRequest);
                    };
                    if active.relation_id != table_id
                        || active.extra_bytes_per_column != extra_bytes_per_column
                        || active.principal_id != context.principal_id
                        || active.role_ids != context.role_ids
                        || active.scope != context.scope
                        || active.grants != grants
                        || active.dependencies != dependencies
                        || active
                            .continuation
                            .is_none_or(|(token, _)| token != cursor.token)
                    {
                        return Err(TransactionError::InvalidMetadataRequest);
                    }
                    active.clone()
                }
                None => ActiveColumnScan {
                    relation_id: table_id,
                    extra_bytes_per_column,
                    principal_id: context.principal_id,
                    role_ids: context.role_ids.clone(),
                    scope: context.scope.clone(),
                    grants: grants.to_vec(),
                    dependencies: dependencies.to_vec(),
                    continuation: None,
                    used_tokens: HashSet::new(),
                },
            };
            let start_ordinal = if let Some(cursor) = continuation.as_ref() {
                let (token, after_ordinal) = active_scan
                    .continuation
                    .take()
                    .ok_or(TransactionError::InvalidMetadataRequest)?;
                if token != cursor.token {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
                usize::try_from(after_ordinal)
                    .ok()
                    .and_then(|ordinal| ordinal.checked_add(1))
                    .ok_or(TransactionError::InvalidMetadataRequest)?
            } else {
                0
            };
            if start_ordinal > trusted_schema.column_ids.len() {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let mut columns = Vec::with_capacity(page_limit);
            let mut response_bytes = 0usize;
            let mut has_more = false;
            for ordinal in start_ordinal..trusted_schema.column_ids.len() {
                if columns.len() == page_limit {
                    has_more = true;
                    break;
                }
                let column = self.read_visible_column(table_id, &trusted_schema, ordinal)?;
                let encoded_size = column
                    .name
                    .len()
                    .checked_add(32)
                    .and_then(|size| size.checked_add(extra_bytes_per_column))
                    .ok_or(TransactionError::MetadataResultTooLarge)?;
                if response_bytes.saturating_add(encoded_size) > MAX_METADATA_PAGE_BYTES {
                    if columns.is_empty() {
                        return Err(TransactionError::MetadataResultTooLarge);
                    }
                    has_more = true;
                    break;
                }
                response_bytes += encoded_size;
                columns.push(column);
            }
            let continuation = if has_more {
                let after_ordinal = columns
                    .last()
                    .map(|column| column.ordinal)
                    .ok_or(TransactionError::InvalidMetadataRequest)?;
                let token = new_metadata_cursor_token(
                    &mut active_scan.used_tokens,
                    COLUMN_CURSOR_TOKEN_KIND,
                )?;
                active_scan.continuation = Some((token, after_ordinal));
                Some(ColumnScanCursor { token })
            } else {
                active_scan.continuation = None;
                None
            };
            self.active_column_scan = Some(active_scan);
            Ok(Some(VisibleColumnPage {
                relation_id: table_id,
                columns,
                continuation,
            }))
        })();
        match result {
            Ok(page) => Ok(page),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// List schemas for which the invocation has schema USAGE, plus the public
    /// reserved information_schema namespace.
    pub fn list_catalog_authorized_schemas(
        &mut self,
        page_limit: usize,
        continuation: Option<SchemaScanCursor>,
    ) -> Result<VisibleSchemaPage, TransactionError> {
        let result = (|| {
            if page_limit == 0 || page_limit > MAX_SCHEMAS_PER_PAGE {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let principal_id = self.principal_id;
            let (role_ids, grants) = self.load_catalog_schema_usage_grants(principal_id)?;
            let scope = self.scope_from_grants(&grants);
            let context = AuthorizationContext {
                principal_id,
                role_ids,
                scope,
            };
            self.list_authorized_schemas(page_limit, continuation, &context, &grants, &[])
        })();
        match result {
            Ok(page) => Ok(page),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// List schemas for which the invocation has schema USAGE, plus the public
    /// reserved information_schema namespace.
    pub fn list_authorized_schemas(
        &mut self,
        page_limit: usize,
        continuation: Option<SchemaScanCursor>,
        context: &AuthorizationContext,
        grants: &[Grant],
        dependencies: &[AuthorityDependency],
    ) -> Result<VisibleSchemaPage, TransactionError> {
        let result = (|| {
            if page_limit == 0
                || page_limit > MAX_SCHEMAS_PER_PAGE
                || grants.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
            {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            self.check_deadline()?;
            self.capture_authority_dependencies(context, dependencies)?;
            let pinned_roles = self.load_catalog_role_closure(context.principal_id)?;
            if pinned_roles != context.role_ids {
                return Err(TransactionError::InvalidContext);
            }
            for grant in grants {
                self.validate_metadata_grant(context, *grant)?;
            }

            let mut active_scan = match continuation.as_ref() {
                Some(cursor) => {
                    let Some(active) = self.active_schema_scan.as_ref() else {
                        return Err(TransactionError::InvalidMetadataRequest);
                    };
                    if active.principal_id != context.principal_id
                        || active.role_ids != context.role_ids
                        || active.scope != context.scope
                        || active.grants != grants
                        || active.dependencies != dependencies
                        || active
                            .continuation
                            .is_none_or(|(token, _)| token != cursor.token)
                    {
                        return Err(TransactionError::InvalidMetadataRequest);
                    }
                    active.clone()
                }
                None => ActiveSchemaScan {
                    principal_id: context.principal_id,
                    role_ids: context.role_ids.clone(),
                    scope: context.scope.clone(),
                    grants: grants.to_vec(),
                    dependencies: dependencies.to_vec(),
                    continuation: None,
                    used_tokens: HashSet::new(),
                },
            };

            let mut visible_schema_ids = grants
                .iter()
                .filter_map(|grant| {
                    let AccessTarget::Schema(schema_id) = grant.target else {
                        return None;
                    };
                    let subject_matches = match grant.subject {
                        Subject::Principal(id) => id == context.principal_id,
                        Subject::Role(id) => context.role_ids.contains(&id),
                    };
                    (grant.operation == Operation::Usage
                        && subject_matches
                        && context
                            .scope
                            .contains(&(Operation::Usage, AccessTarget::Schema(schema_id))))
                    .then_some(schema_id)
                })
                .collect::<Vec<_>>();
            // The reserved information_schema namespace is public even though
            // bootstrap does not represent its virtual views as stored tables.
            visible_schema_ids.push(INFORMATION_SCHEMA_ID);
            visible_schema_ids.sort_unstable();
            visible_schema_ids.dedup();
            let after_id = if let Some(cursor) = continuation.as_ref() {
                let (token, after_id) = active_scan
                    .continuation
                    .take()
                    .ok_or(TransactionError::InvalidMetadataRequest)?;
                if token != cursor.token {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
                Some(after_id)
            } else {
                None
            };
            let mut schemas = Vec::with_capacity(page_limit);
            let mut page_bytes = 0usize;
            let mut has_more = false;
            for schema_id in visible_schema_ids {
                if after_id.is_some_and(|after| schema_id <= after) {
                    continue;
                }
                let schema = if schema_id == INFORMATION_SCHEMA_ID {
                    VisibleSchema {
                        schema_id,
                        name: "information_schema".to_owned(),
                    }
                } else {
                    self.read_discovery_schema(schema_id)?
                };
                let encoded_size = schema
                    .name
                    .len()
                    .checked_add(24)
                    .ok_or(TransactionError::MetadataResultTooLarge)?;
                if schemas.len() == page_limit
                    || page_bytes.saturating_add(encoded_size) > MAX_METADATA_PAGE_BYTES
                {
                    if schemas.is_empty() {
                        return Err(TransactionError::MetadataResultTooLarge);
                    }
                    has_more = true;
                    break;
                }
                page_bytes += encoded_size;
                schemas.push(schema);
            }
            let continuation = if has_more {
                let after_id = schemas
                    .last()
                    .map(|schema| schema.schema_id)
                    .ok_or(TransactionError::InvalidMetadataRequest)?;
                let token = new_metadata_cursor_token(
                    &mut active_scan.used_tokens,
                    SCHEMA_CURSOR_TOKEN_KIND,
                )?;
                active_scan.continuation = Some((token, after_id));
                Some(SchemaScanCursor { token })
            } else {
                active_scan.continuation = None;
                None
            };
            self.active_schema_scan = Some(active_scan);
            Ok(VisibleSchemaPage {
                schemas,
                continuation,
            })
        })();
        match result {
            Ok(page) => Ok(page),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Resolve an exact schema name through the pinned schema-name index after
    /// applying the same schema-usage visibility checks as schema listing.
    pub fn resolve_authorized_schema(
        &mut self,
        schema_name: &str,
        context: &AuthorizationContext,
        grants: &[Grant],
        dependencies: &[AuthorityDependency],
    ) -> Result<Option<VisibleSchema>, TransactionError> {
        let result = (|| {
            if schema_name.is_empty() || schema_name.len() > MAX_METADATA_PAGE_BYTES {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            self.check_deadline()?;
            self.capture_authority_dependencies(context, dependencies)?;
            if self.load_catalog_role_closure(context.principal_id)? != context.role_ids {
                return Err(TransactionError::InvalidContext);
            }
            for grant in grants {
                self.validate_metadata_grant(context, *grant)?;
            }
            let visible = grants.iter().any(|grant| {
                let AccessTarget::Schema(schema_id) = grant.target else {
                    return false;
                };
                let subject_matches = match grant.subject {
                    Subject::Principal(id) => id == context.principal_id,
                    Subject::Role(id) => context.role_ids.contains(&id),
                };
                grant.operation == Operation::Usage
                    && subject_matches
                    && context
                        .scope
                        .contains(&(Operation::Usage, AccessTarget::Schema(schema_id)))
            });
            if schema_name == "information_schema" {
                return Ok(Some(VisibleSchema {
                    schema_id: INFORMATION_SCHEMA_ID,
                    name: schema_name.to_owned(),
                }));
            }
            if !visible {
                return Ok(None);
            }
            let indexed = self
                .pinned_snapshot()?
                .get_schema_name_index_value(schema_name.as_bytes())
                .map_err(|error| TransactionError::Storage(error.to_string()))?;
            let Some(indexed) = indexed else {
                // Startup normally guarantees a complete derived index. Some
                // in-process callers can hold a catalog snapshot created
                // before startup validation, so preserve listing semantics on
                // an index miss while those snapshots remain supported.
                let mut continuation = None;
                loop {
                    let page = self.list_authorized_schemas(
                        MAX_SCHEMAS_PER_PAGE,
                        continuation,
                        context,
                        grants,
                        dependencies,
                    )?;
                    if let Some(schema) = page
                        .schemas
                        .into_iter()
                        .find(|schema| schema.name == schema_name)
                    {
                        return Ok(Some(schema));
                    }
                    continuation = page.continuation;
                    if continuation.is_none() {
                        return Ok(None);
                    }
                }
            };
            let schema_id: [u8; 16] = indexed
                .try_into()
                .map_err(|_| TransactionError::CorruptCatalog)?;
            if schema_id == [0; 16] {
                return Err(TransactionError::CorruptCatalog);
            }
            let authorized = grants.iter().any(|grant| {
                grant.target == AccessTarget::Schema(schema_id)
                    && grant.operation == Operation::Usage
                    && match grant.subject {
                        Subject::Principal(id) => id == context.principal_id,
                        Subject::Role(id) => context.role_ids.contains(&id),
                    }
                    && context
                        .scope
                        .contains(&(Operation::Usage, AccessTarget::Schema(schema_id)))
            });
            if !authorized {
                return Ok(None);
            }
            let schema = self.read_discovery_schema(schema_id)?;
            if schema.name != schema_name {
                return Err(TransactionError::CorruptCatalog);
            }
            Ok(Some(schema))
        })();
        match result {
            Ok(schema) => Ok(schema),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    pub(super) fn validate_discovery_column(
        &mut self,
        table_id: [u8; 16],
        column_id: [u8; 16],
    ) -> Result<(), TransactionError> {
        let column_key = catalog_id_key(6, column_id);
        let column = self
            .read_catalog_record(&column_key)?
            .ok_or(TransactionError::CorruptCatalog)?;
        if column.family_code != 6 || catalog_id(&column, 1)? != table_id {
            return Err(TransactionError::CorruptCatalog);
        }
        let column_object_key = catalog_id_key(2, column_id);
        let column_object = self
            .read_catalog_record(&column_object_key)?
            .ok_or(TransactionError::CorruptCatalog)?;
        if column_object.family_code != 2
            || catalog_u16(&column_object, 1)? != 4
            || catalog_optional_id(&column_object, 2)? != Some(table_id)
            || catalog_id(&column_object, 4)? == [0; 16]
            || catalog_u64(&column_object, 5)? == 0
            || catalog_u16(&column_object, 6)? != 1
            || catalog_field(&column_object, 3)? != catalog_field(&column, 3)?
        {
            return Err(TransactionError::CorruptCatalog);
        }
        Ok(())
    }

    pub(super) fn load_authorized_discovery_schema(
        &mut self,
        table_id: [u8; 16],
        context: &AuthorizationContext,
        grants: &[Grant],
        dependencies: &[AuthorityDependency],
    ) -> Result<Option<CatalogTableSchema>, TransactionError> {
        if table_id == [0; 16] || grants.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        self.check_deadline()?;
        self.capture_authority_dependencies(context, dependencies)?;
        let pinned_roles = self.load_catalog_role_closure(context.principal_id)?;
        if pinned_roles != context.role_ids {
            return Err(TransactionError::InvalidContext);
        }
        for grant in grants {
            self.validate_metadata_grant(context, *grant)?;
        }

        let table_key = catalog_id_key(5, table_id);
        let Some(table) = self.read_catalog_record(&table_key)? else {
            return Ok(None);
        };
        if table.family_code != 5 {
            return Err(TransactionError::CorruptCatalog);
        }
        let schema_id = catalog_id(&table, 1)?;
        if !has_permission(
            context,
            grants,
            Operation::Usage,
            AccessTarget::Schema(schema_id),
        ) {
            return Ok(None);
        }

        let table_target = AccessTarget::Table(table_id);
        let table_grant = [
            Operation::Select,
            Operation::Insert,
            Operation::Update,
            Operation::Delete,
        ]
        .into_iter()
        .any(|operation| has_permission(context, grants, operation, table_target));
        let column_grants = grants
            .iter()
            .filter_map(|grant| {
                let AccessTarget::Column {
                    table_id: granted_table_id,
                    column_id,
                } = grant.target
                else {
                    return None;
                };
                let operation_allowed = matches!(
                    grant.operation,
                    Operation::Select | Operation::Insert | Operation::Update
                );
                let subject_matches = match grant.subject {
                    Subject::Principal(id) => id == context.principal_id,
                    Subject::Role(id) => context.role_ids.contains(&id),
                };
                (granted_table_id == table_id
                    && operation_allowed
                    && subject_matches
                    && context.scope.contains(&(grant.operation, grant.target)))
                .then_some(column_id)
            })
            .collect::<HashSet<_>>();
        if !table_grant && column_grants.is_empty() {
            return Ok(None);
        }
        if !table_grant {
            for column_id in &column_grants {
                self.validate_discovery_column(table_id, *column_id)?;
            }
        }

        self.read_discovery_relation(schema_id, table_id)?;
        self.load_catalog_table_schema(table_id).map(Some)
    }

    pub(super) fn read_visible_column(
        &mut self,
        table_id: [u8; 16],
        schema: &CatalogTableSchema,
        ordinal: usize,
    ) -> Result<VisibleColumn, TransactionError> {
        let column_id = *schema
            .column_ids
            .get(ordinal)
            .ok_or(TransactionError::CorruptCatalog)?;
        self.validate_discovery_column(table_id, column_id)?;
        let record = self
            .read_catalog_record(&catalog_id_key(6, column_id))?
            .ok_or(TransactionError::CorruptCatalog)?;
        let name = String::from_utf8(catalog_field(&record, 3)?.to_vec())
            .map_err(|_| TransactionError::CorruptCatalog)?;
        let schema_column = schema
            .columns
            .get(ordinal)
            .ok_or(TransactionError::CorruptCatalog)?;
        Ok(VisibleColumn {
            column_id,
            name,
            ordinal: ordinal as u32,
            scalar_type: schema_column.scalar_type,
            nullable: schema_column.nullable,
        })
    }

    pub(super) fn validate_metadata_grant(
        &mut self,
        context: &AuthorizationContext,
        grant: Grant,
    ) -> Result<(), TransactionError> {
        let grantee_id = match grant.subject {
            Subject::Principal(id) if id == context.principal_id => id,
            Subject::Role(id) if context.role_ids.contains(&id) => id,
            _ => return Err(TransactionError::InvalidContext),
        };
        let (object_id, privilege, target_scope, target_id) = match (grant.operation, grant.target)
        {
            (Operation::Usage, AccessTarget::Schema(schema_id)) => (schema_id, 1, 1, schema_id),
            (Operation::Select, AccessTarget::Table(table_id)) => (table_id, 2, 1, table_id),
            (Operation::Insert, AccessTarget::Table(table_id)) => (table_id, 3, 1, table_id),
            (Operation::Update, AccessTarget::Table(table_id)) => (table_id, 4, 1, table_id),
            (Operation::Delete, AccessTarget::Table(table_id)) => (table_id, 5, 1, table_id),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id,
                },
            ) => (table_id, 2, 2, column_id),
            (
                Operation::Insert,
                AccessTarget::Column {
                    table_id,
                    column_id,
                },
            ) => (table_id, 3, 2, column_id),
            (
                Operation::Update,
                AccessTarget::Column {
                    table_id,
                    column_id,
                },
            ) => (table_id, 4, 2, column_id),
            _ => return Err(TransactionError::InvalidContext),
        };
        if grantee_id == [0; 16] || object_id == [0; 16] || target_id == [0; 16] {
            return Err(TransactionError::InvalidContext);
        }
        let key = catalog_grant_key(grantee_id, object_id, privilege, target_scope, target_id);
        let record = self
            .read_catalog_record(&key)?
            .ok_or(TransactionError::InvalidContext)?;
        let grantor_id = catalog_id(&record, 1)?;
        if record.family_code != 12 || grantor_id == [0; 16] || catalog_u64(&record, 2)? == 0 {
            return Err(TransactionError::CorruptCatalog);
        }
        let grantor = self
            .read_catalog_record(&catalog_id_key(10, grantor_id))?
            .ok_or(TransactionError::CorruptCatalog)?;
        let grantor_kind = catalog_u16(&grantor, 1)?;
        let grantor_login = catalog_field(&grantor, 3)?;
        if grantor.family_code != 10
            || !matches!(grantor_kind, 1 | 2)
            || catalog_u64(&grantor, 4)? == 0
            || !matches!(grantor_login, [0] | [1])
            || (grantor_kind == 2 && grantor_login != [0])
        {
            return Err(TransactionError::CorruptCatalog);
        }
        Ok(())
    }

    pub(super) fn read_discovery_relation(
        &mut self,
        schema_id: [u8; 16],
        table_id: [u8; 16],
    ) -> Result<VisibleRelation, TransactionError> {
        let table_key = catalog_id_key(5, table_id);
        let table = self
            .read_catalog_record(&table_key)?
            .ok_or(TransactionError::CorruptCatalog)?;
        if table.family_code != 5
            || catalog_id(&table, 1)? != schema_id
            || catalog_u64(&table, 2)? == 0
            || catalog_u64(&table, 3)? == 0
            || catalog_id(&table, 4)? == [0; 16]
            || catalog_u16(&table, 5)? != 1
        {
            return Err(TransactionError::CorruptCatalog);
        }
        let object_key = catalog_id_key(2, table_id);
        let object = self
            .read_catalog_record(&object_key)?
            .ok_or(TransactionError::CorruptCatalog)?;
        if object.family_code != 2
            || catalog_u16(&object, 1)? != 3
            || catalog_optional_id(&object, 2)? != Some(schema_id)
            || catalog_id(&object, 4)? == [0; 16]
            || catalog_u64(&object, 5)? == 0
            || catalog_u16(&object, 6)? != 1
        {
            return Err(TransactionError::CorruptCatalog);
        }
        let name = String::from_utf8(catalog_field(&object, 3)?.to_vec())
            .map_err(|_| TransactionError::CorruptCatalog)?;
        Ok(VisibleRelation {
            relation_id: table_id,
            name,
        })
    }

    pub(super) fn read_discovery_schema(
        &mut self,
        schema_id: [u8; 16],
    ) -> Result<VisibleSchema, TransactionError> {
        let object_key = catalog_id_key(2, schema_id);
        let object = self
            .read_catalog_record(&object_key)?
            .ok_or(TransactionError::CorruptCatalog)?;
        if object.family_code != 2
            || catalog_u16(&object, 1)? != 1
            || catalog_optional_id(&object, 2)?.is_some()
            || catalog_id(&object, 4)? == [0; 16]
            || catalog_u64(&object, 5)? == 0
            || catalog_u16(&object, 6)? != 1
        {
            return Err(TransactionError::CorruptCatalog);
        }
        let schema_key = catalog_id_key(3, schema_id);
        let schema = self
            .read_catalog_record(&schema_key)?
            .ok_or(TransactionError::CorruptCatalog)?;
        if schema.family_code != 3 {
            return Err(TransactionError::CorruptCatalog);
        }
        let table_ids = catalog_id_list(&schema, 1)?;
        if table_ids.contains(&[0; 16])
            || table_ids.iter().collect::<HashSet<_>>().len() != table_ids.len()
        {
            return Err(TransactionError::CorruptCatalog);
        }
        let name = String::from_utf8(catalog_field(&object, 3)?.to_vec())
            .map_err(|_| TransactionError::CorruptCatalog)?;
        Ok(VisibleSchema { schema_id, name })
    }
}
pub(super) fn new_metadata_cursor_token(
    used_tokens: &mut HashSet<[u8; 16]>,
    token_kind: u8,
) -> Result<[u8; 16], TransactionError> {
    for _ in 0..8 {
        let mut token = [0; 16];
        token[0] = token_kind;
        getrandom::fill(&mut token[1..]).map_err(|_| TransactionError::RandomnessUnavailable)?;
        if token != [0; 16] && used_tokens.insert(token) {
            return Ok(token);
        }
    }
    Err(TransactionError::RandomnessUnavailable)
}
