use super::*;
use crate::authorization::SanitizedCell;
use crate::metadata_discovery::{
    AuthorizedPlanRelation, AuthorizedRelationDescription, EXECUTOR_ROW_HANDLE_TOKEN_KIND,
    EXECUTOR_SCAN_CURSOR_TOKEN_KIND, ExecutorRowHandle, ExecutorScanCursor,
    MAX_EXECUTOR_BATCH_CANDIDATES, MAX_EXECUTOR_ROW_HANDLES, MAX_RELATION_HANDLES,
    RELATION_HANDLE_TOKEN_KIND, ResolvedRelationHandle,
};
use crate::overlay::RowId;
use crate::tuple::{FieldPatch, ScalarValue};
use std::collections::{HashMap, HashSet};

/// Executor metadata session whose opaque relation handles expire when the
/// session is dropped. Handle resolution and revalidation always consult the
/// transaction's pinned catalog and never grant row access by themselves.
pub struct ExecutorMetadataConnection<'connection, 'db> {
    pub(super) transaction: &'connection mut InvocationTransaction<'db>,
    pub(super) handles: HashMap<[u8; 16], ResolvedRelationHandleState>,
    pub(super) used_tokens: HashSet<[u8; 16]>,
    pub(super) scans: HashMap<[u8; 16], ActiveExecutorScan>,
    pub(super) used_scan_tokens: HashSet<[u8; 16]>,
    pub(super) row_handles: HashMap<[u8; 16], RowId>,
    pub(super) used_row_tokens: HashSet<[u8; 16]>,
    pub(super) detached: bool,
}

#[derive(Default)]
pub(crate) struct ExecutorMetadataSessionState {
    pub(super) handles: HashMap<[u8; 16], ResolvedRelationHandleState>,
    pub(super) used_tokens: HashSet<[u8; 16]>,
    pub(super) scans: HashMap<[u8; 16], ActiveExecutorScan>,
    pub(super) used_scan_tokens: HashSet<[u8; 16]>,
    pub(super) row_handles: HashMap<[u8; 16], RowId>,
    pub(super) used_row_tokens: HashSet<[u8; 16]>,
}
#[derive(Clone)]
pub(super) struct ResolvedRelationHandleState {
    pub(super) schema_name: String,
    pub(super) relation_name: String,
    pub(super) relation_id: [u8; 16],
}

#[derive(Clone)]
pub(super) struct ActiveExecutorScan {
    pub(super) relation_handle: [u8; 16],
    pub(super) selected_column_ordinals: Vec<usize>,
    pub(super) source: ExecutorScanSource,
}

#[derive(Clone)]
pub(super) enum ExecutorScanSource {
    Physical {
        row_cursor: Option<RowScanCursor>,
        pending_rows: std::collections::VecDeque<(RowId, Vec<SanitizedCell>)>,
    },
    InformationSchema {
        relation: BuiltinInformationSchemaRelation,
        continuation: BuiltinInformationSchemaCursor,
    },
}

impl ExecutorScanSource {
    pub(super) fn is_information_schema(&self) -> bool {
        matches!(self, Self::InformationSchema { .. })
    }

    pub(super) fn information_schema_relation(&self) -> Option<BuiltinInformationSchemaRelation> {
        match self {
            Self::InformationSchema { relation, .. } => Some(*relation),
            Self::Physical { .. } => None,
        }
    }
}

impl ExecutorMetadataConnection<'_, '_> {
    /// Abort the complete invocation after an all-or-error executor batch fails.
    pub fn abort_invocation(&mut self) -> Result<(), TransactionError> {
        self.transaction.abort()
    }

    pub(crate) fn record_policy_denial(&mut self) {
        self.transaction.coordination.record_policy_denial();
    }

    /// Poll the invocation's cancellation and deadline state. Expired or
    /// cancelled invocations are aborted before returning `true`.
    pub fn check_cancellation(&mut self) -> Result<bool, TransactionError> {
        match self.transaction.check_deadline() {
            Ok(()) => Ok(false),
            Err(TransactionError::CancellationRequested | TransactionError::Expired) => Ok(true),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn detach(mut self) -> ExecutorMetadataSessionState {
        self.detached = true;
        ExecutorMetadataSessionState {
            handles: std::mem::take(&mut self.handles),
            used_tokens: std::mem::take(&mut self.used_tokens),
            scans: std::mem::take(&mut self.scans),
            used_scan_tokens: std::mem::take(&mut self.used_scan_tokens),
            row_handles: std::mem::take(&mut self.row_handles),
            used_row_tokens: std::mem::take(&mut self.used_row_tokens),
        }
    }

    /// Resolve a qualified name through the invocation's pinned catalog and
    /// issue an opaque handle owned by this executor metadata session.
    pub fn resolve_relation(
        &mut self,
        schema_name: &str,
        relation_name: &str,
    ) -> Result<Option<(ResolvedRelationHandle, AuthorizedRelationDescription)>, TransactionError>
    {
        let Some(description) = self
            .transaction
            .resolve_catalog_authorized_relation(schema_name, relation_name)?
        else {
            return Ok(None);
        };
        if self.used_tokens.len() >= MAX_RELATION_HANDLES {
            return Err(TransactionError::RelationHandleLimitExceeded);
        }
        let token = new_metadata_cursor_token(&mut self.used_tokens, RELATION_HANDLE_TOKEN_KIND)?;
        let handle = ResolvedRelationHandle { token };
        self.handles.insert(
            token,
            ResolvedRelationHandleState {
                schema_name: schema_name.to_owned(),
                relation_name: relation_name.to_owned(),
                relation_id: description.relation_id,
            },
        );
        Ok(Some((handle, description)))
    }

    /// Re-resolve a plan's qualified relation name against this invocation's
    /// pinned authorized catalog and return its stable logical identity. This
    /// creates no executor handle or row-access capability.
    pub fn describe_relation_for_plan(
        &mut self,
        schema_name: &str,
        relation_name: &str,
    ) -> Result<Option<AuthorizedPlanRelation>, TransactionError> {
        let Some(relation) = self
            .transaction
            .resolve_catalog_authorized_relation(schema_name, relation_name)?
        else {
            return Ok(None);
        };
        attach_plan_identity(self.transaction, schema_name, relation).map(Some)
    }

    /// Re-resolve the saved qualified name against this connection's pinned
    /// catalog before returning its description. The handle is not authority.
    pub fn revalidate_relation_handle(
        &mut self,
        handle: &ResolvedRelationHandle,
    ) -> Result<AuthorizedRelationDescription, TransactionError> {
        let state = self
            .handles
            .get(&handle.token)
            .cloned()
            .ok_or(TransactionError::InvalidMetadataRequest)?;
        let description = self
            .transaction
            .resolve_catalog_authorized_relation(&state.schema_name, &state.relation_name)?
            .ok_or(TransactionError::InvalidMetadataRequest)?;
        if description.relation_id != state.relation_id || description.name != state.relation_name {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        Ok(description)
    }

    pub(super) fn revalidate_scan_relation(
        &mut self,
        relation_handle: [u8; 16],
    ) -> Result<AuthorizedRelationDescription, TransactionError> {
        let state = self
            .handles
            .get(&relation_handle)
            .cloned()
            .ok_or(TransactionError::InvalidMetadataRequest)?;
        if let Some(schema) = builtin_information_schema_relation_schema(state.relation_id) {
            let relation = builtin_information_schema_relations()
                .into_iter()
                .find(|relation| relation.relation_id == state.relation_id)
                .ok_or(TransactionError::InvalidMetadataRequest)?;
            if state.schema_name != "information_schema" || relation.name != state.relation_name {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            return Ok(AuthorizedRelationDescription {
                relation_id: schema.relation_id,
                name: relation.name,
                columns: schema.columns,
            });
        }
        self.revalidate_relation_handle(&ResolvedRelationHandle {
            token: relation_handle,
        })
    }

    pub(super) fn clear_information_schema_scan(
        &mut self,
        relation: BuiltinInformationSchemaRelation,
    ) {
        match relation {
            BuiltinInformationSchemaRelation::Schemata => {
                self.transaction.active_schema_scan = None;
            }
            BuiltinInformationSchemaRelation::Tables => {
                self.transaction.active_information_schema_tables_scan = None;
            }
            BuiltinInformationSchemaRelation::Columns => {
                self.transaction.active_information_schema_columns_scan = None;
            }
        }
    }

    /// Invalidate a relation handle before the connection closes.
    pub fn close_relation_handle(
        &mut self,
        handle: &ResolvedRelationHandle,
    ) -> Result<(), TransactionError> {
        let state = self
            .handles
            .remove(&handle.token)
            .ok_or(TransactionError::InvalidMetadataRequest)?;
        self.row_handles
            .retain(|_, row_id| row_id.table != state.relation_id);
        if let Some((token, scan)) = self
            .scans
            .iter()
            .find(|(_, scan)| scan.relation_handle == handle.token)
            .map(|(token, scan)| (*token, scan.clone()))
        {
            self.scans.remove(&token);
            if let Some(relation) = scan.source.information_schema_relation() {
                self.clear_information_schema_scan(relation);
            } else if let ExecutorScanSource::Physical {
                row_cursor: Some(cursor),
                ..
            } = scan.source
            {
                self.transaction
                    .discard_active_scan_for_table(cursor.table_id);
            } else if let Some(active) = self.transaction.active_scan.as_ref() {
                self.transaction
                    .discard_active_scan_for_table(active.table_id);
            }
        }
        Ok(())
    }

    /// Open one connection-local scan for a resolved relation. The returned
    /// opaque cursor does not expose row IDs or own authorization.
    pub fn open_relation_scan(
        &mut self,
        handle: &ResolvedRelationHandle,
        requested_column_ids: &[[u8; 16]],
    ) -> Result<ExecutorScanCursor, TransactionError> {
        let description = self.revalidate_relation_handle(handle)?;
        let is_information_schema_relation =
            builtin_information_schema_relation_schema(description.relation_id).is_some();
        if is_information_schema_relation
            && !matches!(description.name.as_str(), "schemata" | "tables" | "columns")
        {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        if requested_column_ids.len() > crate::tuple::MAX_COLUMNS
            || requested_column_ids.contains(&[0; 16])
            || requested_column_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                .len()
                != requested_column_ids.len()
        {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        if !self.scans.is_empty() {
            return Err(TransactionError::ScanStillActive);
        }
        if self.used_scan_tokens.len() >= MAX_RELATION_HANDLES {
            return Err(TransactionError::RelationHandleLimitExceeded);
        }
        let token =
            new_metadata_cursor_token(&mut self.used_scan_tokens, EXECUTOR_SCAN_CURSOR_TOKEN_KIND)?;
        let selected_column_ordinals = if requested_column_ids.is_empty() {
            (0..description.columns.len()).collect()
        } else {
            let selected = description
                .columns
                .iter()
                .enumerate()
                .filter_map(|(index, column)| {
                    requested_column_ids
                        .contains(&column.column_id)
                        .then_some(index)
                })
                .collect::<Vec<_>>();
            if selected.len() != requested_column_ids.len() {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            selected
        };
        self.scans.insert(
            token,
            ActiveExecutorScan {
                relation_handle: handle.token,
                selected_column_ordinals,
                source: if is_information_schema_relation {
                    let relation = match description.name.as_str() {
                        "schemata" => BuiltinInformationSchemaRelation::Schemata,
                        "tables" => BuiltinInformationSchemaRelation::Tables,
                        "columns" => BuiltinInformationSchemaRelation::Columns,
                        _ => return Err(TransactionError::InvalidMetadataRequest),
                    };
                    ExecutorScanSource::InformationSchema {
                        relation,
                        continuation: match relation {
                            BuiltinInformationSchemaRelation::Schemata => {
                                BuiltinInformationSchemaCursor::Schemata(None)
                            }
                            BuiltinInformationSchemaRelation::Tables => {
                                BuiltinInformationSchemaCursor::Tables(None)
                            }
                            BuiltinInformationSchemaRelation::Columns => {
                                BuiltinInformationSchemaCursor::Columns(None)
                            }
                        },
                    }
                } else {
                    ExecutorScanSource::Physical {
                        row_cursor: None,
                        pending_rows: std::collections::VecDeque::new(),
                    }
                },
            },
        );
        Ok(ExecutorScanCursor { token })
    }

    /// Probe one catalog-loaded unique index through the invocation's pinned
    /// authorization projection. The raw storage row ID is retained only in
    /// this connection's opaque handle map.
    pub fn probe_relation_unique_index(
        &mut self,
        handle: &ResolvedRelationHandle,
        index_id: [u8; 16],
        values: &[Option<ScalarValue>],
        requested_column_ids: &[[u8; 16]],
    ) -> Result<ExecutorUniqueIndexProbe, TransactionError> {
        let description = self.revalidate_relation_handle(handle)?;
        if values.is_empty()
            || values.len() > crate::tuple::MAX_COLUMNS
            || requested_column_ids.len() > crate::tuple::MAX_COLUMNS
            || requested_column_ids.contains(&[0; 16])
            || requested_column_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                .len()
                != requested_column_ids.len()
        {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        let selected_ordinals = if requested_column_ids.is_empty() {
            (0..description.columns.len()).collect::<Vec<_>>()
        } else {
            let selected = description
                .columns
                .iter()
                .enumerate()
                .filter_map(|(ordinal, column)| {
                    requested_column_ids
                        .contains(&column.column_id)
                        .then_some(ordinal)
                })
                .collect::<Vec<_>>();
            if selected.len() != requested_column_ids.len() {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            selected
        };
        match self.transaction.probe_catalog_unique_index_for_principal(
            description.relation_id,
            index_id,
            values,
        )? {
            AuthorizedUniqueIndexProbe::Unavailable => Ok(ExecutorUniqueIndexProbe::Unavailable),
            AuthorizedUniqueIndexProbe::NoMatch => Ok(ExecutorUniqueIndexProbe::NoMatch),
            AuthorizedUniqueIndexProbe::Match { id, cells } => {
                if self.used_row_tokens.len() >= MAX_EXECUTOR_ROW_HANDLES {
                    self.transaction.abort()?;
                    return Err(TransactionError::RelationHandleLimitExceeded);
                }
                let selected_cells = selected_ordinals
                    .iter()
                    .map(|ordinal| {
                        cells
                            .get(*ordinal)
                            .cloned()
                            .ok_or(TransactionError::InvalidMetadataRequest)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let token = match new_metadata_cursor_token(
                    &mut self.used_row_tokens,
                    EXECUTOR_ROW_HANDLE_TOKEN_KIND,
                ) {
                    Ok(token) => token,
                    Err(error) => {
                        self.transaction.abort()?;
                        return Err(error);
                    }
                };
                self.row_handles.insert(token, id);
                Ok(ExecutorUniqueIndexProbe::Match {
                    handle: ExecutorRowHandle { token },
                    cells: selected_cells,
                })
            }
        }
    }

    /// Scan a catalog-validated leading-key range and return only authorized
    /// projected cells. Storage row identifiers never cross this boundary.
    pub fn scan_relation_index_range(
        &mut self,
        handle: &ResolvedRelationHandle,
        index_id: [u8; 16],
        lower: Option<AuthorizedIndexRangeBound>,
        upper: Option<AuthorizedIndexRangeBound>,
        requested_column_ids: &[[u8; 16]],
    ) -> Result<ExecutorIndexRangeScan, TransactionError> {
        let description = self.revalidate_relation_handle(handle)?;
        if builtin_information_schema_relation_schema(description.relation_id).is_some() {
            return Ok(ExecutorIndexRangeScan::Unavailable);
        }
        if index_id == [0; 16]
            || (lower.is_none() && upper.is_none())
            || requested_column_ids.len() > crate::tuple::MAX_COLUMNS
            || requested_column_ids.contains(&[0; 16])
            || requested_column_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                .len()
                != requested_column_ids.len()
        {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        let selected_ordinals = if requested_column_ids.is_empty() {
            (0..description.columns.len()).collect::<Vec<_>>()
        } else {
            let selected = description
                .columns
                .iter()
                .enumerate()
                .filter_map(|(ordinal, column)| {
                    requested_column_ids
                        .contains(&column.column_id)
                        .then_some(ordinal)
                })
                .collect::<Vec<_>>();
            if selected.len() != requested_column_ids.len() {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            selected
        };
        match self.transaction.scan_catalog_index_range_for_principal(
            description.relation_id,
            index_id,
            lower,
            upper,
        )? {
            AuthorizedIndexRangeScan::Unavailable => Ok(ExecutorIndexRangeScan::Unavailable),
            AuthorizedIndexRangeScan::Rows(rows) => rows
                .into_iter()
                .map(|(_, cells)| {
                    selected_ordinals
                        .iter()
                        .map(|ordinal| {
                            cells
                                .get(*ordinal)
                                .cloned()
                                .ok_or(TransactionError::InvalidMetadataRequest)
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .collect::<Result<Vec<_>, _>>()
                .map(ExecutorIndexRangeScan::Rows),
        }
    }

    pub fn scan_relation_index_prefix(
        &mut self,
        handle: &ResolvedRelationHandle,
        index_id: [u8; 16],
        values: &[ScalarValue],
        requested_column_ids: &[[u8; 16]],
    ) -> Result<ExecutorIndexRangeScan, TransactionError> {
        let description = self.revalidate_relation_handle(handle)?;
        if builtin_information_schema_relation_schema(description.relation_id).is_some() {
            return Ok(ExecutorIndexRangeScan::Unavailable);
        }
        if index_id == [0; 16]
            || !(2..=8).contains(&values.len())
            || requested_column_ids.len() > crate::tuple::MAX_COLUMNS
            || requested_column_ids.contains(&[0; 16])
            || requested_column_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                .len()
                != requested_column_ids.len()
        {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        let selected_ordinals = if requested_column_ids.is_empty() {
            (0..description.columns.len()).collect::<Vec<_>>()
        } else {
            let selected = description
                .columns
                .iter()
                .enumerate()
                .filter_map(|(ordinal, column)| {
                    requested_column_ids
                        .contains(&column.column_id)
                        .then_some(ordinal)
                })
                .collect::<Vec<_>>();
            if selected.len() != requested_column_ids.len() {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            selected
        };
        match self.transaction.scan_catalog_index_prefix_for_principal(
            description.relation_id,
            index_id,
            values,
        )? {
            AuthorizedIndexRangeScan::Unavailable => Ok(ExecutorIndexRangeScan::Unavailable),
            AuthorizedIndexRangeScan::Rows(rows) => rows
                .into_iter()
                .map(|(_, cells)| {
                    selected_ordinals
                        .iter()
                        .map(|ordinal| {
                            cells
                                .get(*ordinal)
                                .cloned()
                                .ok_or(TransactionError::InvalidMetadataRequest)
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .collect::<Result<Vec<_>, _>>()
                .map(ExecutorIndexRangeScan::Rows),
        }
    }

    /// Execute an ordered set of authorized point or leading-prefix probes.
    /// The caller publishes the result only after every entry is available.
    pub fn scan_relation_index_ranges(
        &mut self,
        handle: &ResolvedRelationHandle,
        index_id: [u8; 16],
        probes: &[Vec<ScalarValue>],
        requested_column_ids: &[[u8; 16]],
    ) -> Result<Option<Vec<Vec<Vec<SanitizedCell>>>>, TransactionError> {
        if probes.is_empty() || probes.len() > 128 {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        let mut groups = Vec::with_capacity(probes.len());
        let mut candidates = 0usize;
        for values in probes {
            if !(1..=8).contains(&values.len()) {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let scan = if values.len() == 1 {
                let bound = AuthorizedIndexRangeBound {
                    value: values[0].clone(),
                    inclusive: true,
                };
                self.scan_relation_index_range(
                    handle,
                    index_id,
                    Some(bound.clone()),
                    Some(bound),
                    requested_column_ids,
                )?
            } else {
                self.scan_relation_index_prefix(handle, index_id, values, requested_column_ids)?
            };
            let ExecutorIndexRangeScan::Rows(rows) = scan else {
                return Ok(None);
            };
            candidates = candidates.saturating_add(rows.len());
            if candidates > 16_384 {
                return Ok(None);
            }
            groups.push(rows);
        }
        Ok(Some(groups))
    }

    pub(super) fn next_information_schema_page(
        &mut self,
        scan: ActiveExecutorScan,
        max_scanned: usize,
    ) -> Result<ExecutorAuthorizedRowScanPage, TransactionError> {
        let ExecutorScanSource::InformationSchema {
            relation,
            continuation,
        } = scan.source
        else {
            return Err(TransactionError::InvalidMetadataRequest);
        };
        let (full_rows, next_cursor) = match (relation, continuation) {
            (
                BuiltinInformationSchemaRelation::Schemata,
                BuiltinInformationSchemaCursor::Schemata(cursor),
            ) => {
                let page = self
                    .transaction
                    .scan_information_schema_schemata(max_scanned, cursor)?;
                (
                    page.rows
                        .into_iter()
                        .map(|row| vec![SanitizedCell::Value(ScalarValue::Text(row.schema_name))])
                        .collect::<Vec<_>>(),
                    page.continuation
                        .map(|cursor| BuiltinInformationSchemaCursor::Schemata(Some(cursor))),
                )
            }
            (
                BuiltinInformationSchemaRelation::Tables,
                BuiltinInformationSchemaCursor::Tables(cursor),
            ) => {
                let page = self
                    .transaction
                    .scan_information_schema_tables(max_scanned, cursor)?;
                (
                    page.rows
                        .into_iter()
                        .map(|row| {
                            Ok(vec![
                                SanitizedCell::Value(ScalarValue::Text(row.table_schema)),
                                SanitizedCell::Value(ScalarValue::Text(row.table_name)),
                                SanitizedCell::Value(ScalarValue::Text(row.table_type)),
                            ])
                        })
                        .collect::<Result<Vec<_>, TransactionError>>()?,
                    page.continuation
                        .map(|cursor| BuiltinInformationSchemaCursor::Tables(Some(cursor))),
                )
            }
            (
                BuiltinInformationSchemaRelation::Columns,
                BuiltinInformationSchemaCursor::Columns(cursor),
            ) => {
                let page = self
                    .transaction
                    .scan_information_schema_columns(max_scanned, cursor)?;
                (
                    page.rows
                        .into_iter()
                        .map(|row| {
                            Ok(vec![
                                SanitizedCell::Value(ScalarValue::Text(row.table_schema)),
                                SanitizedCell::Value(ScalarValue::Text(row.table_name)),
                                SanitizedCell::Value(ScalarValue::Text(row.column_name)),
                                SanitizedCell::Value(ScalarValue::Integer(
                                    i32::try_from(row.ordinal_position)
                                        .map_err(|_| TransactionError::InvalidMetadataRequest)?,
                                )),
                                SanitizedCell::Value(ScalarValue::Text(row.data_type)),
                                SanitizedCell::Value(ScalarValue::Text(row.is_nullable)),
                                information_schema_optional_integer(row.character_maximum_length)?,
                                information_schema_optional_integer(row.numeric_precision)?,
                                information_schema_optional_integer(row.numeric_scale)?,
                            ])
                        })
                        .collect::<Result<Vec<_>, TransactionError>>()?,
                    page.continuation
                        .map(|cursor| BuiltinInformationSchemaCursor::Columns(Some(cursor))),
                )
            }
            _ => return Err(TransactionError::InvalidMetadataRequest),
        };
        if self.used_row_tokens.len().saturating_add(full_rows.len()) > MAX_EXECUTOR_ROW_HANDLES {
            self.transaction.abort()?;
            return Err(TransactionError::RelationHandleLimitExceeded);
        }
        let mut rows = Vec::with_capacity(full_rows.len());
        for full_shape in full_rows {
            let cells = scan
                .selected_column_ordinals
                .iter()
                .map(|ordinal| {
                    full_shape
                        .get(*ordinal)
                        .cloned()
                        .ok_or(TransactionError::InvalidMetadataRequest)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let token = match new_metadata_cursor_token(
                &mut self.used_row_tokens,
                EXECUTOR_ROW_HANDLE_TOKEN_KIND,
            ) {
                Ok(token) => token,
                Err(error) => {
                    self.transaction.abort()?;
                    return Err(error);
                }
            };
            // Virtual metadata rows can be selected but never staged for update
            // or deletion; their opaque tokens are deliberately not row-map keys.
            rows.push((ExecutorRowHandle { token }, cells));
        }
        let continuation = if let Some(continuation) = next_cursor {
            if self.used_scan_tokens.len() >= MAX_EXECUTOR_ROW_HANDLES {
                self.transaction.abort()?;
                return Err(TransactionError::RelationHandleLimitExceeded);
            }
            let token = match new_metadata_cursor_token(
                &mut self.used_scan_tokens,
                EXECUTOR_SCAN_CURSOR_TOKEN_KIND,
            ) {
                Ok(token) => token,
                Err(error) => {
                    self.transaction.abort()?;
                    return Err(error);
                }
            };
            self.scans.insert(
                token,
                ActiveExecutorScan {
                    relation_handle: scan.relation_handle,
                    selected_column_ordinals: scan.selected_column_ordinals,
                    source: ExecutorScanSource::InformationSchema {
                        relation,
                        continuation,
                    },
                },
            );
            Some(ExecutorScanCursor { token })
        } else {
            None
        };
        Ok(ExecutorAuthorizedRowScanPage { rows, continuation })
    }

    /// Consume one cursor and return the next authorized row page. A live
    /// continuation receives a fresh token; completed and old tokens cannot
    /// be replayed.
    pub fn next_relation_scan_page(
        &mut self,
        cursor: &ExecutorScanCursor,
        max_scanned: usize,
    ) -> Result<ExecutorAuthorizedRowScanPage, TransactionError> {
        let scan = self
            .scans
            .remove(&cursor.token)
            .ok_or(TransactionError::InvalidMetadataRequest)?;
        let description = self.revalidate_scan_relation(scan.relation_handle)?;
        if scan.source.is_information_schema() {
            return self.next_information_schema_page(scan, max_scanned);
        }
        let ExecutorScanSource::Physical { row_cursor, .. } = scan.source else {
            return Err(TransactionError::InvalidMetadataRequest);
        };
        let page = self.transaction.scan_catalog_authorized_rows(
            description.relation_id,
            row_cursor.as_ref(),
            max_scanned,
        )?;
        if self.used_row_tokens.len().saturating_add(page.rows.len()) > MAX_EXECUTOR_ROW_HANDLES {
            self.transaction.abort()?;
            return Err(TransactionError::RelationHandleLimitExceeded);
        }
        let mut rows = Vec::with_capacity(page.rows.len());
        for (row_id, cells) in page.rows {
            if row_id.table != description.relation_id {
                self.transaction.abort()?;
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let selected_cells = scan
                .selected_column_ordinals
                .iter()
                .map(|ordinal| {
                    cells
                        .get(*ordinal)
                        .cloned()
                        .ok_or(TransactionError::InvalidMetadataRequest)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let token = match new_metadata_cursor_token(
                &mut self.used_row_tokens,
                EXECUTOR_ROW_HANDLE_TOKEN_KIND,
            ) {
                Ok(token) => token,
                Err(error) => {
                    self.transaction.abort()?;
                    return Err(error);
                }
            };
            self.row_handles.insert(token, row_id);
            rows.push((ExecutorRowHandle { token }, selected_cells));
        }
        let continuation = if let Some(row_cursor) = page.continuation {
            if self.used_scan_tokens.len() >= MAX_EXECUTOR_ROW_HANDLES {
                self.transaction.abort()?;
                return Err(TransactionError::RelationHandleLimitExceeded);
            }
            let token = match new_metadata_cursor_token(
                &mut self.used_scan_tokens,
                EXECUTOR_SCAN_CURSOR_TOKEN_KIND,
            ) {
                Ok(token) => token,
                Err(error) => {
                    self.transaction.abort()?;
                    return Err(error);
                }
            };
            self.scans.insert(
                token,
                ActiveExecutorScan {
                    relation_handle: scan.relation_handle,
                    selected_column_ordinals: scan.selected_column_ordinals,
                    source: ExecutorScanSource::Physical {
                        row_cursor: Some(row_cursor),
                        pending_rows: std::collections::VecDeque::new(),
                    },
                },
            );
            Some(ExecutorScanCursor { token })
        } else {
            None
        };
        Ok(ExecutorAuthorizedRowScanPage { rows, continuation })
    }

    /// Build an executor batch while bounding both candidate work and encoded
    /// response size. One authorized row that does not fit is retained for the
    /// next request; an individually oversized row aborts the invocation.
    pub fn next_relation_scan_batch(
        &mut self,
        cursor: &ExecutorScanCursor,
        max_scanned: usize,
        max_bytes: usize,
    ) -> Result<ExecutorAuthorizedRowScanPage, TransactionError> {
        if max_scanned == 0
            || max_scanned > MAX_EXECUTOR_BATCH_CANDIDATES
            || max_bytes > crate::ipc::MAX_PAYLOAD_BYTES
        {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        let scan = self
            .scans
            .remove(&cursor.token)
            .ok_or(TransactionError::InvalidMetadataRequest)?;
        let description = self.revalidate_scan_relation(scan.relation_handle)?;
        if scan.source.is_information_schema() {
            let page = self.next_information_schema_page(scan, max_scanned)?;
            let encoded_bytes = page.rows.iter().try_fold(21usize, |bytes, (_, cells)| {
                bytes
                    .checked_add(executor_batch_row_encoded_size(cells)?)
                    .ok_or(TransactionError::MetadataResultTooLarge)
            })?;
            if encoded_bytes > max_bytes {
                self.transaction.abort()?;
                return Err(TransactionError::MetadataResultTooLarge);
            }
            return Ok(page);
        }
        let ExecutorScanSource::Physical {
            row_cursor,
            pending_rows,
        } = scan.source
        else {
            return Err(TransactionError::InvalidMetadataRequest);
        };
        // Reserve the larger header, which includes a continuation token.
        let mut encoded_bytes = 21usize;
        let mut scanned = 0usize;
        let mut rows = Vec::new();
        let mut row_cursor = row_cursor;
        let mut pending_rows = pending_rows;

        while rows.len() < max_scanned {
            if pending_rows.is_empty() {
                let remaining_candidates = max_scanned.saturating_sub(scanned);
                if remaining_candidates == 0 {
                    break;
                }
                let page = self.transaction.scan_catalog_authorized_rows(
                    description.relation_id,
                    row_cursor.as_ref(),
                    remaining_candidates,
                )?;
                scanned += remaining_candidates;
                row_cursor = page.continuation;
                pending_rows.extend(page.rows);
                if pending_rows.is_empty() {
                    if row_cursor.is_none() {
                        break;
                    }
                    continue;
                }
            }
            let Some((row_id, cells)) = pending_rows.pop_front() else {
                continue;
            };
            if row_id.table != description.relation_id {
                self.transaction.abort()?;
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let selected_cells = scan
                .selected_column_ordinals
                .iter()
                .map(|ordinal| {
                    cells
                        .get(*ordinal)
                        .cloned()
                        .ok_or(TransactionError::InvalidMetadataRequest)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let row_bytes = executor_batch_row_encoded_size(&selected_cells)?;
            if encoded_bytes.saturating_add(row_bytes) > max_bytes {
                if rows.is_empty() {
                    self.transaction.abort()?;
                    return Err(TransactionError::MetadataResultTooLarge);
                }
                pending_rows.push_front((row_id, cells));
                break;
            }
            if self.used_row_tokens.len() >= MAX_EXECUTOR_ROW_HANDLES {
                self.transaction.abort()?;
                return Err(TransactionError::RelationHandleLimitExceeded);
            }
            let token = match new_metadata_cursor_token(
                &mut self.used_row_tokens,
                EXECUTOR_ROW_HANDLE_TOKEN_KIND,
            ) {
                Ok(token) => token,
                Err(error) => {
                    self.transaction.abort()?;
                    return Err(error);
                }
            };
            self.row_handles.insert(token, row_id);
            rows.push((ExecutorRowHandle { token }, selected_cells));
            encoded_bytes += row_bytes;
        }

        let continuation = if !pending_rows.is_empty() || row_cursor.is_some() {
            if self.used_scan_tokens.len() >= MAX_EXECUTOR_ROW_HANDLES {
                self.transaction.abort()?;
                return Err(TransactionError::RelationHandleLimitExceeded);
            }
            let token = match new_metadata_cursor_token(
                &mut self.used_scan_tokens,
                EXECUTOR_SCAN_CURSOR_TOKEN_KIND,
            ) {
                Ok(token) => token,
                Err(error) => {
                    self.transaction.abort()?;
                    return Err(error);
                }
            };
            self.scans.insert(
                token,
                ActiveExecutorScan {
                    relation_handle: scan.relation_handle,
                    selected_column_ordinals: scan.selected_column_ordinals,
                    source: ExecutorScanSource::Physical {
                        row_cursor,
                        pending_rows,
                    },
                },
            );
            Some(ExecutorScanCursor { token })
        } else {
            None
        };
        Ok(ExecutorAuthorizedRowScanPage { rows, continuation })
    }

    /// Resolve a connection-local visible-row handle to its internal row ID.
    /// The caller must still perform operation-specific authorization.
    pub fn revalidate_row_handle(
        &self,
        handle: &ExecutorRowHandle,
    ) -> Result<RowId, TransactionError> {
        self.row_handles
            .get(&handle.token)
            .copied()
            .ok_or(TransactionError::InvalidMetadataRequest)
    }

    /// Stage an insert from a complete declared-shape value vector. The new
    /// row receives an opaque connection-local handle. DEFAULT markers and
    /// generated UUID identities are materialized from the pinned catalog.
    pub fn stage_insert(
        &mut self,
        relation: &ResolvedRelationHandle,
        values: &[InsertInput],
    ) -> Result<ExecutorRowHandle, TransactionError> {
        let description = self.revalidate_relation_handle(relation)?;
        if builtin_information_schema_relation_schema(description.relation_id).is_some() {
            self.transaction.abort()?;
            return Err(TransactionError::InvalidContext);
        }
        if self.used_row_tokens.len() >= MAX_EXECUTOR_ROW_HANDLES {
            self.transaction.abort()?;
            return Err(TransactionError::RelationHandleLimitExceeded);
        }
        let projection_state = self
            .transaction
            .load_catalog_executor_write_projection(description.relation_id)?;
        let mut row_id_bytes = [0; 16];
        getrandom::fill(&mut row_id_bytes).map_err(|_| TransactionError::RandomnessUnavailable)?;
        if row_id_bytes == [0; 16] {
            self.transaction.abort()?;
            return Err(TransactionError::RandomnessUnavailable);
        }
        let id = RowId {
            table: description.relation_id,
            row: row_id_bytes,
        };
        let handle_token =
            new_metadata_cursor_token(&mut self.used_row_tokens, EXECUTOR_ROW_HANDLE_TOKEN_KIND)?;
        let projection = projection_state.as_projection();
        self.transaction
            .stage_insert_authorized_from_catalog_defaults(
                id,
                &projection,
                values,
                &projection_state.schema,
            )?;
        self.row_handles.insert(handle_token, id);
        Ok(ExecutorRowHandle {
            token: handle_token,
        })
    }

    /// Apply a catalog-authorized field patch to a visible opaque row handle.
    /// All previously issued handles for that row expire after the update.
    pub fn stage_update(
        &mut self,
        handle: &ExecutorRowHandle,
        patches: &[FieldPatch],
    ) -> Result<(), TransactionError> {
        let id = self.revalidate_row_handle(handle)?;
        let projection_state = self
            .transaction
            .load_catalog_executor_write_projection(id.table)?;
        if patches.iter().any(|patch| {
            projection_state
                .schema
                .generation_modes
                .get(patch.ordinal)
                .is_none_or(|mode| *mode != 0)
        }) {
            self.transaction.abort()?;
            return Err(TransactionError::InvalidContext);
        }
        let projection = projection_state.as_projection();
        self.transaction
            .stage_patch_authorized(id, &projection, patches)?;
        self.row_handles.retain(|_, row_id| *row_id != id);
        Ok(())
    }

    /// Delete a visible opaque row handle after pinned-catalog authorization.
    /// All previously issued handles for that row expire after deletion.
    pub fn stage_delete(&mut self, handle: &ExecutorRowHandle) -> Result<(), TransactionError> {
        let id = self.revalidate_row_handle(handle)?;
        let projection_state = self
            .transaction
            .load_catalog_executor_write_projection(id.table)?;
        let projection = projection_state.as_projection();
        self.transaction.stage_delete_authorized(id, &projection)?;
        self.row_handles.retain(|_, row_id| *row_id != id);
        Ok(())
    }

    /// Close a live cursor and release the invocation's active row-scan slot.
    pub fn close_relation_scan(
        &mut self,
        cursor: &ExecutorScanCursor,
    ) -> Result<(), TransactionError> {
        let scan = self
            .scans
            .remove(&cursor.token)
            .ok_or(TransactionError::InvalidMetadataRequest)?;
        let table_id = self
            .handles
            .get(&scan.relation_handle)
            .map(|handle| handle.relation_id)
            .ok_or(TransactionError::InvalidMetadataRequest)?;
        if let Some(relation) = scan.source.information_schema_relation() {
            self.clear_information_schema_scan(relation);
        } else {
            self.transaction.discard_active_scan_for_table(table_id);
        }
        Ok(())
    }
}

impl Drop for ExecutorMetadataConnection<'_, '_> {
    fn drop(&mut self) {
        if self.detached {
            return;
        }
        let information_schema_scans = self
            .scans
            .values()
            .filter_map(|scan| scan.source.information_schema_relation())
            .collect::<HashSet<_>>();
        for relation in information_schema_scans {
            self.clear_information_schema_scan(relation);
        }
        let table_ids = self
            .handles
            .values()
            .map(|handle| handle.relation_id)
            .collect::<HashSet<_>>();
        for table_id in table_ids {
            self.transaction.discard_active_scan_for_table(table_id);
        }
    }
}

pub(super) fn executor_batch_row_encoded_size(
    cells: &[SanitizedCell],
) -> Result<usize, TransactionError> {
    let mut size = 18usize; // Opaque row handle and cell count.
    for cell in cells {
        size = size
            .checked_add(match cell {
                SanitizedCell::Null | SanitizedCell::Redacted => 1,
                SanitizedCell::Value(value) => {
                    let value_bytes = match value {
                        ScalarValue::Boolean(_) => 1,
                        ScalarValue::SmallInt(_) => 2,
                        ScalarValue::Integer(_) | ScalarValue::Date(_) => 4,
                        ScalarValue::BigInt(_) | ScalarValue::TimestampTz(_) => 8,
                        ScalarValue::Decimal(_) | ScalarValue::Uuid(_) => 16,
                        ScalarValue::Text(value) | ScalarValue::Varchar(value) => value.len(),
                        ScalarValue::Bytea(value) => value.len(),
                    };
                    6usize
                        .checked_add(value_bytes)
                        .ok_or(TransactionError::MetadataResultTooLarge)?
                }
            })
            .ok_or(TransactionError::MetadataResultTooLarge)?;
    }
    Ok(size)
}
