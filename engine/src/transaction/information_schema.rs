use super::discovery::*;
use super::*;
use crate::authorization::{AuthorizationContext, SanitizedCell};
use crate::metadata_discovery::{
    COLUMN_CURSOR_TOKEN_KIND, ColumnScanCursor, INFORMATION_SCHEMA_COLUMNS_CURSOR_TOKEN_KIND,
    INFORMATION_SCHEMA_TABLES_CURSOR_TOKEN_KIND, InformationSchemaColumnRow,
    InformationSchemaColumnsCursor, InformationSchemaColumnsPage, InformationSchemaColumnsScanPage,
    InformationSchemaSchemataPage, InformationSchemaSchemataRow, InformationSchemaTableRow,
    InformationSchemaTablesCursor, InformationSchemaTablesPage, InformationSchemaTablesScanPage,
    MAX_COLUMNS_PER_PAGE, MAX_METADATA_PAGE_BYTES, MAX_RELATIONS_PER_PAGE, MAX_SCHEMAS_PER_PAGE,
    RELATION_CURSOR_TOKEN_KIND, RelationScanCursor, SchemaScanCursor, VisibleColumn,
    VisibleColumnPage, VisibleRelation, VisibleRelationPage, VisibleRelationSchema, VisibleSchema,
};
use crate::tuple::{ScalarType, ScalarValue};
use std::collections::HashSet;

pub(super) fn information_schema_optional_integer(
    value: Option<u32>,
) -> Result<SanitizedCell, TransactionError> {
    match value {
        Some(value) => i32::try_from(value)
            .map(|value| SanitizedCell::Value(ScalarValue::Integer(value)))
            .map_err(|_| TransactionError::InvalidMetadataRequest),
        None => Ok(SanitizedCell::Null),
    }
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub(super) enum BuiltinInformationSchemaRelation {
    Schemata,
    Tables,
    Columns,
}

#[derive(Clone)]
pub(super) enum BuiltinInformationSchemaCursor {
    Schemata(Option<SchemaScanCursor>),
    Tables(Option<InformationSchemaTablesCursor>),
    Columns(Option<InformationSchemaColumnsCursor>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BuiltinRelationScanPurpose {
    Tables,
    Columns,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ActiveBuiltinRelationScan {
    purpose: BuiltinRelationScanPurpose,
    extra_bytes_per_relation: usize,
    principal_id: [u8; 16],
    continuation: Option<([u8; 16], usize)>,
    used_tokens: HashSet<[u8; 16]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ActiveBuiltinColumnScan {
    relation_id: [u8; 16],
    extra_bytes_per_column: usize,
    principal_id: [u8; 16],
    continuation: Option<([u8; 16], u32)>,
    used_tokens: HashSet<[u8; 16]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ActiveInformationSchemaTablesScan {
    pub(super) principal_id: [u8; 16],
    pub(super) current_schema: Option<VisibleSchema>,
    pub(super) schema_continuation: Option<SchemaScanCursor>,
    pub(super) relation_continuation: Option<RelationScanCursor>,
    pub(super) continuation: Option<[u8; 16]>,
    pub(super) used_tokens: HashSet<[u8; 16]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ActiveInformationSchemaColumnsScan {
    pub(super) principal_id: [u8; 16],
    pub(super) schema_scan_started: bool,
    pub(super) current_schema: Option<VisibleSchema>,
    pub(super) schema_continuation: Option<SchemaScanCursor>,
    pub(super) relation_scan_started: bool,
    pub(super) relation_scan_exhausted: bool,
    pub(super) current_relation: Option<VisibleRelation>,
    pub(super) relation_continuation: Option<RelationScanCursor>,
    pub(super) current_relation_columns_started: bool,
    pub(super) column_continuation: Option<ColumnScanCursor>,
    pub(super) continuation: Option<[u8; 16]>,
    pub(super) used_tokens: HashSet<[u8; 16]>,
}

impl<'db> InvocationTransaction<'db> {
    /// Return the authorized rows exposed by `information_schema.schemata`.
    pub fn scan_information_schema_schemata(
        &mut self,
        page_limit: usize,
        continuation: Option<SchemaScanCursor>,
    ) -> Result<InformationSchemaSchemataPage, TransactionError> {
        let page = self.list_catalog_authorized_schemas(page_limit, continuation)?;
        Ok(InformationSchemaSchemataPage {
            rows: page
                .schemas
                .into_iter()
                .map(|schema| InformationSchemaSchemataRow {
                    schema_name: schema.name,
                })
                .collect(),
            continuation: page.continuation,
        })
    }

    /// Return the authorized `information_schema.tables` rows for one schema.
    pub fn scan_information_schema_tables_in_schema(
        &mut self,
        schema_name: &str,
        page_limit: usize,
        continuation: Option<RelationScanCursor>,
    ) -> Result<Option<InformationSchemaTablesPage>, TransactionError> {
        let result = (|| {
            if schema_name.is_empty()
                || schema_name.len() > MAX_METADATA_PAGE_BYTES
                || page_limit == 0
                || page_limit > MAX_RELATIONS_PER_PAGE
            {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let principal_id = self.principal_id;
            let (role_ids, schema_grants) = self.load_catalog_schema_usage_grants(principal_id)?;
            let scope = self.scope_from_grants(&schema_grants);
            let schema_context = AuthorizationContext {
                principal_id,
                role_ids,
                scope,
            };
            let Some(schema) =
                self.resolve_authorized_schema(schema_name, &schema_context, &schema_grants, &[])?
            else {
                return Ok(None);
            };
            self.scan_information_schema_tables_for_visible_schema(
                &schema,
                page_limit,
                continuation,
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

    pub(super) fn scan_information_schema_tables_for_visible_schema(
        &mut self,
        schema: &VisibleSchema,
        page_limit: usize,
        continuation: Option<RelationScanCursor>,
    ) -> Result<Option<InformationSchemaTablesPage>, TransactionError> {
        if schema.schema_id == INFORMATION_SCHEMA_ID {
            let extra_bytes_per_relation = schema
                .name
                .len()
                .checked_add("VIEW".len())
                .ok_or(TransactionError::MetadataResultTooLarge)?;
            let page = self.list_builtin_information_schema_relations(
                page_limit,
                continuation,
                BuiltinRelationScanPurpose::Tables,
                extra_bytes_per_relation,
            )?;
            return Ok(Some(InformationSchemaTablesPage {
                rows: page
                    .relations
                    .into_iter()
                    .map(|relation| InformationSchemaTableRow {
                        table_schema: schema.name.clone(),
                        table_name: relation.name,
                        table_type: "VIEW".to_owned(),
                    })
                    .collect(),
                continuation: page.continuation,
            }));
        }
        let extra_bytes_per_relation = schema
            .name
            .len()
            .checked_add("BASE TABLE".len())
            .ok_or(TransactionError::MetadataResultTooLarge)?;
        let Some(page) = self.list_catalog_authorized_relations_with_extra_bytes(
            schema.schema_id,
            page_limit,
            continuation,
            extra_bytes_per_relation,
        )?
        else {
            return Ok(None);
        };
        Ok(Some(InformationSchemaTablesPage {
            rows: page
                .relations
                .into_iter()
                .map(|relation| InformationSchemaTableRow {
                    table_schema: schema.name.clone(),
                    table_name: relation.name,
                    table_type: "BASE TABLE".to_owned(),
                })
                .collect(),
            continuation: page.continuation,
        }))
    }

    pub(super) fn list_builtin_information_schema_relations(
        &mut self,
        page_limit: usize,
        continuation: Option<RelationScanCursor>,
        purpose: BuiltinRelationScanPurpose,
        extra_bytes_per_relation: usize,
    ) -> Result<VisibleRelationPage, TransactionError> {
        let result = (|| {
            if page_limit == 0 || page_limit > MAX_RELATIONS_PER_PAGE {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            self.check_deadline()?;
            let principal_id = self.principal_id;
            let mut active_scan = match continuation.as_ref() {
                Some(cursor) => {
                    let Some(active) = self.active_builtin_relation_scan.as_ref() else {
                        return Err(TransactionError::InvalidMetadataRequest);
                    };
                    if active.principal_id != principal_id
                        || active.purpose != purpose
                        || active.extra_bytes_per_relation != extra_bytes_per_relation
                        || active
                            .continuation
                            .is_none_or(|(token, _)| token != cursor.token)
                    {
                        return Err(TransactionError::InvalidMetadataRequest);
                    }
                    active.clone()
                }
                None => ActiveBuiltinRelationScan {
                    purpose,
                    extra_bytes_per_relation,
                    principal_id,
                    continuation: None,
                    used_tokens: HashSet::new(),
                },
            };
            let start_index = if let Some(cursor) = continuation.as_ref() {
                let (token, after_index) = active_scan
                    .continuation
                    .take()
                    .ok_or(TransactionError::InvalidMetadataRequest)?;
                if token != cursor.token {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
                after_index
            } else {
                0
            };
            let available = builtin_information_schema_relations();
            let mut relations = Vec::with_capacity(page_limit);
            let mut page_bytes = 0usize;
            let mut next_index = None;
            for (index, relation) in available.into_iter().enumerate().skip(start_index) {
                if relations.len() == page_limit {
                    next_index = Some(index);
                    break;
                }
                let encoded_size = relation
                    .name
                    .len()
                    .checked_add(24)
                    .and_then(|size| size.checked_add(extra_bytes_per_relation))
                    .ok_or(TransactionError::MetadataResultTooLarge)?;
                if page_bytes.saturating_add(encoded_size) > MAX_METADATA_PAGE_BYTES {
                    if relations.is_empty() {
                        return Err(TransactionError::MetadataResultTooLarge);
                    }
                    next_index = Some(index);
                    break;
                }
                page_bytes += encoded_size;
                relations.push(relation);
            }
            let continuation = if let Some(after_index) = next_index {
                let token = new_metadata_cursor_token(
                    &mut active_scan.used_tokens,
                    RELATION_CURSOR_TOKEN_KIND,
                )?;
                active_scan.continuation = Some((token, after_index));
                Some(RelationScanCursor { token })
            } else {
                active_scan.continuation = None;
                None
            };
            self.active_builtin_relation_scan = continuation.as_ref().map(|_| active_scan);
            Ok(VisibleRelationPage {
                relations,
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

    pub(super) fn describe_builtin_information_schema_columns_page(
        &mut self,
        relation_id: [u8; 16],
        page_limit: usize,
        continuation: Option<ColumnScanCursor>,
        extra_bytes_per_column: usize,
    ) -> Result<Option<VisibleColumnPage>, TransactionError> {
        let result = (|| {
            if page_limit == 0 || page_limit > MAX_COLUMNS_PER_PAGE {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let Some(schema) = builtin_information_schema_relation_schema(relation_id) else {
                return Ok(None);
            };
            self.check_deadline()?;
            let principal_id = self.principal_id;
            let mut active_scan = match continuation.as_ref() {
                Some(cursor) => {
                    let Some(active) = self.active_builtin_column_scan.as_ref() else {
                        return Err(TransactionError::InvalidMetadataRequest);
                    };
                    if active.relation_id != relation_id
                        || active.extra_bytes_per_column != extra_bytes_per_column
                        || active.principal_id != principal_id
                        || active
                            .continuation
                            .is_none_or(|(token, _)| token != cursor.token)
                    {
                        return Err(TransactionError::InvalidMetadataRequest);
                    }
                    active.clone()
                }
                None => ActiveBuiltinColumnScan {
                    relation_id,
                    extra_bytes_per_column,
                    principal_id,
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
            if start_ordinal > schema.columns.len() {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let mut columns = Vec::with_capacity(page_limit);
            let mut page_bytes = 0usize;
            let mut has_more = false;
            for ordinal in start_ordinal..schema.columns.len() {
                if columns.len() == page_limit {
                    has_more = true;
                    break;
                }
                let column = schema.columns[ordinal].clone();
                let encoded_size = column
                    .name
                    .len()
                    .checked_add(32)
                    .and_then(|size| size.checked_add(extra_bytes_per_column))
                    .ok_or(TransactionError::MetadataResultTooLarge)?;
                if page_bytes.saturating_add(encoded_size) > MAX_METADATA_PAGE_BYTES {
                    if columns.is_empty() {
                        return Err(TransactionError::MetadataResultTooLarge);
                    }
                    has_more = true;
                    break;
                }
                page_bytes += encoded_size;
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
            self.active_builtin_column_scan = continuation.as_ref().map(|_| active_scan);
            Ok(Some(VisibleColumnPage {
                relation_id,
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

    /// Return all schema-authorized table rows in bounded metadata pages.
    pub fn scan_information_schema_tables(
        &mut self,
        page_limit: usize,
        continuation: Option<InformationSchemaTablesCursor>,
    ) -> Result<InformationSchemaTablesScanPage, TransactionError> {
        let result = (|| {
            if page_limit == 0 || page_limit > MAX_RELATIONS_PER_PAGE {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let principal_id = self.principal_id;
            let mut active_scan = match continuation.as_ref() {
                Some(cursor) => {
                    let Some(active) = self.active_information_schema_tables_scan.as_ref() else {
                        return Err(TransactionError::InvalidMetadataRequest);
                    };
                    if active.principal_id != principal_id
                        || active
                            .continuation
                            .is_none_or(|token| token != cursor.token)
                    {
                        return Err(TransactionError::InvalidMetadataRequest);
                    }
                    active.clone()
                }
                None => ActiveInformationSchemaTablesScan {
                    principal_id,
                    current_schema: None,
                    schema_continuation: None,
                    relation_continuation: None,
                    continuation: None,
                    used_tokens: HashSet::new(),
                },
            };
            if let Some(cursor) = continuation.as_ref() {
                let token = active_scan
                    .continuation
                    .take()
                    .ok_or(TransactionError::InvalidMetadataRequest)?;
                if token != cursor.token {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
            }
            if active_scan.current_schema.is_none() || active_scan.relation_continuation.is_none() {
                let schema_page = self
                    .list_catalog_authorized_schemas(1, active_scan.schema_continuation.take())?;
                active_scan.current_schema = schema_page.schemas.into_iter().next();
                active_scan.schema_continuation = schema_page.continuation;
                active_scan.relation_continuation = None;
            }
            let Some(schema) = active_scan.current_schema.clone() else {
                self.active_information_schema_tables_scan = None;
                return Ok(InformationSchemaTablesScanPage {
                    rows: Vec::new(),
                    continuation: None,
                });
            };
            let page = self
                .scan_information_schema_tables_for_visible_schema(
                    &schema,
                    page_limit,
                    active_scan.relation_continuation.take(),
                )?
                .ok_or(TransactionError::InvalidContext)?;
            active_scan.relation_continuation = page.continuation;
            let continuation = if active_scan.relation_continuation.is_some()
                || active_scan.schema_continuation.is_some()
            {
                let token = new_metadata_cursor_token(
                    &mut active_scan.used_tokens,
                    INFORMATION_SCHEMA_TABLES_CURSOR_TOKEN_KIND,
                )?;
                active_scan.continuation = Some(token);
                Some(InformationSchemaTablesCursor { token })
            } else {
                active_scan.continuation = None;
                None
            };
            if continuation.is_some() {
                self.active_information_schema_tables_scan = Some(active_scan);
            } else {
                self.active_information_schema_tables_scan = None;
            }
            Ok(InformationSchemaTablesScanPage {
                rows: page.rows,
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

    /// Return the authorized `information_schema.columns` rows for one relation.
    pub fn scan_information_schema_columns_for_relation(
        &mut self,
        schema_name: &str,
        relation_name: &str,
        page_limit: usize,
        continuation: Option<ColumnScanCursor>,
    ) -> Result<Option<InformationSchemaColumnsPage>, TransactionError> {
        let result = (|| {
            if schema_name.is_empty()
                || schema_name.len() > MAX_METADATA_PAGE_BYTES
                || relation_name.is_empty()
                || relation_name.len() > MAX_METADATA_PAGE_BYTES
                || page_limit == 0
                || page_limit > MAX_COLUMNS_PER_PAGE
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
                let Some(relation) = builtin_information_schema_relation(relation_name) else {
                    return Ok(None);
                };
                return self.scan_information_schema_columns_for_visible_relation(
                    &schema,
                    &relation,
                    page_limit,
                    continuation,
                );
            }

            let mut relation_continuation = None;
            let relation = loop {
                let Some(page) = self.list_catalog_authorized_relations(
                    schema.schema_id,
                    MAX_RELATIONS_PER_PAGE,
                    relation_continuation,
                )?
                else {
                    return Ok(None);
                };
                if let Some(relation) = page
                    .relations
                    .into_iter()
                    .find(|relation| relation.name == relation_name)
                {
                    break relation;
                }
                relation_continuation = page.continuation;
                if relation_continuation.is_none() {
                    return Ok(None);
                }
            };

            self.scan_information_schema_columns_for_visible_relation(
                &schema,
                &relation,
                page_limit,
                continuation,
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

    pub(super) fn scan_information_schema_columns_for_visible_relation(
        &mut self,
        schema: &VisibleSchema,
        relation: &VisibleRelation,
        page_limit: usize,
        continuation: Option<ColumnScanCursor>,
    ) -> Result<Option<InformationSchemaColumnsPage>, TransactionError> {
        let extra_bytes_per_column = schema
            .name
            .len()
            .checked_add(relation.name.len())
            .and_then(|size| size.checked_add(INFORMATION_SCHEMA_COLUMN_EXTRA_BYTES))
            .ok_or(TransactionError::MetadataResultTooLarge)?;
        let Some(page) = (if schema.schema_id == INFORMATION_SCHEMA_ID {
            self.describe_builtin_information_schema_columns_page(
                relation.relation_id,
                page_limit,
                continuation,
                extra_bytes_per_column,
            )?
        } else {
            let principal_id = self.principal_id;
            let (relation_roles, relation_grants) =
                self.load_catalog_relation_discovery_grants(schema.schema_id)?;
            let relation_scope = self.scope_from_grants(&relation_grants);
            let relation_context = AuthorizationContext {
                principal_id,
                role_ids: relation_roles,
                scope: relation_scope,
            };
            self.describe_authorized_columns_page_with_extra_bytes(
                relation.relation_id,
                page_limit,
                continuation,
                extra_bytes_per_column,
                MetadataDiscoveryAccess {
                    context: &relation_context,
                    grants: &relation_grants,
                    dependencies: &[],
                },
            )?
        }) else {
            return Ok(None);
        };
        let rows = page
            .columns
            .into_iter()
            .map(|column| {
                let ordinal_position = column
                    .ordinal
                    .checked_add(1)
                    .ok_or(TransactionError::CorruptCatalog)?;
                let (data_type, character_maximum_length, numeric_precision, numeric_scale) =
                    information_schema_column_type(column.scalar_type);
                Ok(InformationSchemaColumnRow {
                    table_schema: schema.name.clone(),
                    table_name: relation.name.clone(),
                    column_name: column.name,
                    ordinal_position,
                    data_type: data_type.to_owned(),
                    is_nullable: if column.nullable { "YES" } else { "NO" }.to_owned(),
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                })
            })
            .collect::<Result<Vec<_>, TransactionError>>()?;
        Ok(Some(InformationSchemaColumnsPage {
            rows,
            continuation: page.continuation,
        }))
    }

    /// Return declared columns for every relation admitted to the invocation.
    pub fn scan_information_schema_columns(
        &mut self,
        page_limit: usize,
        continuation: Option<InformationSchemaColumnsCursor>,
    ) -> Result<InformationSchemaColumnsScanPage, TransactionError> {
        let result = (|| {
            if page_limit == 0 || page_limit > MAX_COLUMNS_PER_PAGE {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let principal_id = self.principal_id;
            let mut active_scan = match continuation.as_ref() {
                Some(cursor) => {
                    let Some(active) = self.active_information_schema_columns_scan.as_ref() else {
                        return Err(TransactionError::InvalidMetadataRequest);
                    };
                    if active.principal_id != principal_id
                        || active
                            .continuation
                            .is_none_or(|token| token != cursor.token)
                    {
                        return Err(TransactionError::InvalidMetadataRequest);
                    }
                    active.clone()
                }
                None => ActiveInformationSchemaColumnsScan {
                    principal_id,
                    schema_scan_started: false,
                    current_schema: None,
                    schema_continuation: None,
                    relation_scan_started: false,
                    relation_scan_exhausted: false,
                    current_relation: None,
                    relation_continuation: None,
                    current_relation_columns_started: false,
                    column_continuation: None,
                    continuation: None,
                    used_tokens: HashSet::new(),
                },
            };
            if let Some(cursor) = continuation.as_ref() {
                let token = active_scan
                    .continuation
                    .take()
                    .ok_or(TransactionError::InvalidMetadataRequest)?;
                if token != cursor.token {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
            }
            if active_scan.current_relation_columns_started
                && active_scan.column_continuation.is_none()
            {
                active_scan.current_relation = None;
                active_scan.current_relation_columns_started = false;
            }
            let mut empty_schema_pages = 0usize;
            while active_scan.current_relation.is_none() {
                if active_scan.current_schema.is_none() {
                    if active_scan.schema_scan_started && active_scan.schema_continuation.is_none()
                    {
                        break;
                    }
                    let schema_page = self.list_catalog_authorized_schemas(
                        1,
                        active_scan.schema_continuation.take(),
                    )?;
                    active_scan.schema_scan_started = true;
                    active_scan.current_schema = schema_page.schemas.into_iter().next();
                    active_scan.schema_continuation = schema_page.continuation;
                    active_scan.relation_scan_started = false;
                    active_scan.relation_scan_exhausted = false;
                    active_scan.relation_continuation = None;
                    if active_scan.current_schema.is_none() {
                        break;
                    }
                }
                if active_scan.relation_scan_started && active_scan.relation_scan_exhausted {
                    active_scan.current_schema = None;
                    active_scan.relation_scan_started = false;
                    active_scan.relation_scan_exhausted = false;
                    active_scan.relation_continuation = None;
                    empty_schema_pages += 1;
                    if empty_schema_pages >= MAX_SCHEMAS_PER_PAGE {
                        break;
                    }
                    continue;
                }
                let schema = active_scan
                    .current_schema
                    .as_ref()
                    .ok_or(TransactionError::InvalidContext)?
                    .clone();
                let relation_page = if schema.schema_id == INFORMATION_SCHEMA_ID {
                    self.list_builtin_information_schema_relations(
                        1,
                        active_scan.relation_continuation.take(),
                        BuiltinRelationScanPurpose::Columns,
                        0,
                    )?
                } else {
                    self.list_catalog_authorized_relations(
                        schema.schema_id,
                        1,
                        active_scan.relation_continuation.take(),
                    )?
                    .ok_or(TransactionError::InvalidContext)?
                };
                active_scan.relation_continuation = relation_page.continuation;
                active_scan.relation_scan_started = true;
                active_scan.relation_scan_exhausted = active_scan.relation_continuation.is_none();
                if let Some(relation) = relation_page.relations.into_iter().next() {
                    active_scan.current_relation = Some(relation);
                    break;
                }
                if active_scan.relation_continuation.is_some() {
                    continue;
                }
                active_scan.current_schema = None;
                active_scan.relation_scan_started = false;
                active_scan.relation_scan_exhausted = false;
                empty_schema_pages += 1;
                if empty_schema_pages >= MAX_SCHEMAS_PER_PAGE {
                    break;
                }
            }
            let projected_page = match (
                active_scan.current_schema.as_ref(),
                active_scan.current_relation.as_ref(),
            ) {
                (Some(schema), Some(relation)) => {
                    let schema = schema.clone();
                    let relation = relation.clone();
                    let column_continuation = active_scan.column_continuation.take();
                    Some(
                        self.scan_information_schema_columns_for_visible_relation(
                            &schema,
                            &relation,
                            page_limit,
                            column_continuation,
                        )?
                        .ok_or(TransactionError::InvalidContext)?,
                    )
                }
                _ => None,
            };
            if let Some(page) = projected_page {
                active_scan.column_continuation = page.continuation;
                active_scan.current_relation_columns_started = true;
                let more = active_scan.column_continuation.is_some()
                    || active_scan.relation_continuation.is_some()
                    || active_scan.schema_continuation.is_some();
                let continuation = if more {
                    let token = new_metadata_cursor_token(
                        &mut active_scan.used_tokens,
                        INFORMATION_SCHEMA_COLUMNS_CURSOR_TOKEN_KIND,
                    )?;
                    active_scan.continuation = Some(token);
                    Some(InformationSchemaColumnsCursor { token })
                } else {
                    active_scan.continuation = None;
                    None
                };
                self.active_information_schema_columns_scan =
                    continuation.as_ref().map(|_| active_scan);
                Ok(InformationSchemaColumnsScanPage {
                    rows: page.rows,
                    continuation,
                })
            } else {
                let continuation = if active_scan.relation_continuation.is_some()
                    || active_scan.schema_continuation.is_some()
                {
                    let token = new_metadata_cursor_token(
                        &mut active_scan.used_tokens,
                        INFORMATION_SCHEMA_COLUMNS_CURSOR_TOKEN_KIND,
                    )?;
                    active_scan.continuation = Some(token);
                    Some(InformationSchemaColumnsCursor { token })
                } else {
                    None
                };
                self.active_information_schema_columns_scan =
                    continuation.as_ref().map(|_| active_scan);
                Ok(InformationSchemaColumnsScanPage {
                    rows: Vec::new(),
                    continuation,
                })
            }
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
}
pub(super) fn builtin_information_schema_relations() -> [VisibleRelation; 3] {
    crate::metadata_discovery::information_schema_relations()
}

pub(super) fn builtin_information_schema_relation(relation_name: &str) -> Option<VisibleRelation> {
    builtin_information_schema_relations()
        .into_iter()
        .find(|relation| relation.name == relation_name)
}

pub(super) fn builtin_information_schema_relation_schema(
    relation_id: [u8; 16],
) -> Option<VisibleRelationSchema> {
    let relation_code = match relation_id {
        id if id == builtin_information_schema_relation_id(1) => 1,
        id if id == builtin_information_schema_relation_id(2) => 2,
        id if id == builtin_information_schema_relation_id(3) => 3,
        _ => return None,
    };
    let columns = match relation_code {
        1 => vec![
            ("table_schema", ScalarType::Text, false),
            ("table_name", ScalarType::Text, false),
            ("column_name", ScalarType::Text, false),
            ("ordinal_position", ScalarType::Integer, false),
            ("data_type", ScalarType::Text, false),
            ("is_nullable", ScalarType::Text, false),
            ("character_maximum_length", ScalarType::Integer, true),
            ("numeric_precision", ScalarType::Integer, true),
            ("numeric_scale", ScalarType::Integer, true),
        ],
        2 => vec![("schema_name", ScalarType::Text, false)],
        3 => vec![
            ("table_schema", ScalarType::Text, false),
            ("table_name", ScalarType::Text, false),
            ("table_type", ScalarType::Text, false),
        ],
        _ => return None,
    };
    Some(VisibleRelationSchema {
        relation_id,
        columns: columns
            .into_iter()
            .enumerate()
            .map(|(ordinal, (name, scalar_type, nullable))| VisibleColumn {
                column_id: builtin_information_schema_column_id(
                    relation_code * 16 + ordinal as u8 + 1,
                ),
                name: name.to_owned(),
                ordinal: ordinal as u32,
                scalar_type,
                nullable,
            })
            .collect(),
    })
}

pub(super) const fn builtin_information_schema_relation_id(code: u8) -> [u8; 16] {
    let mut id = [0x81; 16];
    id[15] = code;
    id
}

pub(super) const fn builtin_information_schema_column_id(code: u8) -> [u8; 16] {
    let mut id = [0x82; 16];
    id[15] = code;
    id
}

pub(super) fn information_schema_column_type(
    scalar_type: ScalarType,
) -> (&'static str, Option<u32>, Option<u32>, Option<u32>) {
    match scalar_type {
        ScalarType::Boolean => ("BOOLEAN", None, None, None),
        ScalarType::SmallInt => ("SMALLINT", None, Some(5), Some(0)),
        ScalarType::Integer => ("INTEGER", None, Some(10), Some(0)),
        ScalarType::BigInt => ("BIGINT", None, Some(19), Some(0)),
        ScalarType::Decimal { precision, scale } => (
            "DECIMAL",
            None,
            Some(u32::from(precision)),
            Some(u32::from(scale)),
        ),
        ScalarType::Text => ("TEXT", None, None, None),
        ScalarType::Varchar { max_chars } => ("CHARACTER VARYING", Some(max_chars), None, None),
        ScalarType::Bytea => ("BYTEA", None, None, None),
        ScalarType::Uuid => ("UUID", None, None, None),
        ScalarType::Date => ("DATE", None, None, None),
        ScalarType::TimestampTz => ("TIMESTAMP WITH TIME ZONE", None, None, None),
    }
}
