use super::*;
use crate::authorization::{
    AuthorizationContext, AuthorizedProjection, SanitizedCell, project_row,
};
use crate::index_key::{DeriveIndexKeyError, derive_index_key, encode_sql_tuple};
use crate::overlay::{RowId, StagedRow};
use crate::row_record::RowRecord;
use crate::snapshot::MAX_ROW_SCAN_PAGE;
use crate::tuple::{ScalarValue, decode_tuple};
use std::collections::HashSet;

impl<'db> InvocationTransaction<'db> {
    pub fn read(&mut self, id: RowId) -> Result<Option<Vec<u8>>, TransactionError> {
        match self.read_inner(id) {
            Ok(value) => Ok(value),
            Err(error) => {
                self.abort()?;
                Err(error)
            }
        }
    }

    /// Read the snapshot/private row, decode it with the pinned schema, and
    /// return only its authorized sanitized projection. Predicate/runtime
    /// errors abort the invocation; denied rows disappear and denied cells are
    /// represented without their value bytes or lengths.
    pub fn read_authorized(
        &mut self,
        id: RowId,
        projection: &AuthorizedProjection<'_>,
    ) -> Result<Option<Vec<SanitizedCell>>, TransactionError> {
        let result = (|| {
            self.capture_authority_dependencies(projection.context, projection.dependencies)?;
            let Some(payload) = self.read_inner(id)? else {
                return Ok(None);
            };
            let tuple = decode_tuple(projection.columns, projection.schema_version, &payload)
                .map_err(TransactionError::Tuple)?;
            project_row(
                projection.context,
                projection.grants,
                projection.policies,
                projection.schema_id,
                id.table,
                projection.column_ids,
                &tuple.values,
            )
            .map_err(TransactionError::Authorization)
        })();
        match result {
            Ok(projected) => Ok(projected),
            Err(error) => {
                self.abort()?;
                Err(error)
            }
        }
    }

    /// Scan a bounded page from the invocation snapshot plus its private
    /// overlay, then apply the pinned SELECT policy and cell projection to
    /// each resolved row. Full pages require a follow-up call to confirm EOF.
    pub fn scan_authorized(
        &mut self,
        table_id: [u8; 16],
        cursor: Option<&RowScanCursor>,
        max_scanned: usize,
        projection: &AuthorizedProjection<'_>,
    ) -> Result<AuthorizedRowScanPage, TransactionError> {
        let result = (|| {
            if table_id == [0; 16]
                || max_scanned == 0
                || max_scanned > MAX_ROW_SCAN_PAGE
                || projection.columns.is_empty()
                || projection.columns.len() != projection.column_ids.len()
                || projection.columns.len() > crate::tuple::MAX_COLUMNS
                || projection.schema_version == 0
                || projection.schema_id == [0; 16]
                || projection.column_ids.contains(&[0; 16])
                || projection.grants.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
                || projection.policies.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
                || projection.checks.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
                || projection.policy_driving_columns.len()
                    > crate::authorization::MAX_AUTHORIZATION_INPUTS
                || projection.dependencies.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
            {
                return Err(TransactionError::InvalidScanRequest);
            }
            let after_row_id = cursor.map(|cursor| cursor.after_row_id);
            match self.active_scan.as_ref() {
                Some(active)
                    if active.table_id == table_id
                        && Some(active.after_row_id) == after_row_id
                        && cursor.is_some_and(|cursor| {
                            cursor.invocation_id == self.invocation_id
                                && cursor.table_id == table_id
                        })
                        && active.authority.matches(projection) => {}
                Some(_) => return Err(TransactionError::InvalidScanRequest),
                None if after_row_id.is_some() => {
                    return Err(TransactionError::InvalidScanRequest);
                }
                None => {}
            }
            self.capture_authority_dependencies(projection.context, projection.dependencies)?;
            let (snapshot_ids, snapshot_complete) = {
                self.pinned_snapshot()?
                    .scan_row_ids_after(table_id, after_row_id, max_scanned)
                    .map_err(map_snapshot_scan_error)?
            };
            let (overlay_ids, overlay_complete) =
                self.overlay
                    .row_ids_after(table_id, after_row_id, max_scanned)?;

            let mut row_ids = snapshot_ids;
            row_ids.extend(overlay_ids);
            row_ids.sort_unstable();
            row_ids.dedup();
            row_ids.truncate(max_scanned);
            self.charge_scan_work(row_ids.len())?;
            let complete = snapshot_complete && overlay_complete && row_ids.len() < max_scanned;
            let after_row_id = row_ids.last().copied();
            let mut rows = Vec::with_capacity(row_ids.len());
            for row in &row_ids {
                let id = RowId {
                    table: table_id,
                    row: *row,
                };
                if let Some(cells) = self.read_authorized(id, projection)? {
                    rows.push((id, cells));
                }
            }
            self.active_scan = if complete {
                None
            } else {
                after_row_id.map(|after_row_id| ActiveRowScan {
                    table_id,
                    after_row_id,
                    authority: ScanAuthority::capture(projection),
                })
            };
            Ok(AuthorizedRowScanPage {
                rows,
                continuation: if complete {
                    None
                } else {
                    after_row_id.map(|after_row_id| RowScanCursor {
                        invocation_id: self.invocation_id,
                        table_id,
                        after_row_id,
                    })
                },
            })
        })();
        match result {
            Ok(page) => Ok(page),
            Err(error) => {
                self.abort()?;
                Err(error)
            }
        }
    }

    /// Read one authorized row using schema, grants, and policies loaded from
    /// this invocation's pinned catalog. Callers cannot substitute authorization
    /// contexts or grant records for this read path.
    pub fn read_catalog_authorized_row(
        &mut self,
        table_id: [u8; 16],
        row_id: [u8; 16],
    ) -> Result<Option<Vec<SanitizedCell>>, TransactionError> {
        let result = (|| {
            if table_id == [0; 16] || row_id == [0; 16] {
                return Err(TransactionError::InvalidContext);
            }
            let schema = self.load_catalog_table_schema(table_id)?;
            let principal_id = self.principal_id;
            let (role_ids, grants) = self.load_catalog_grants(principal_id, &schema)?;
            let policies = self.load_catalog_policies(principal_id, &schema)?;
            let scope = self.scope_from_grants(&grants);
            let context = AuthorizationContext {
                principal_id,
                role_ids,
                scope,
            };
            let projection = AuthorizedProjection {
                columns: &schema.columns,
                column_ids: &schema.column_ids,
                schema_version: schema.schema_version,
                schema_id: schema.schema_id,
                context: &context,
                grants: &grants,
                policies: &policies,
                checks: &[],
                policy_driving_columns: &[],
                dependencies: &[],
            };
            self.read_authorized(
                RowId {
                    table: table_id,
                    row: row_id,
                },
                &projection,
            )
        })();
        match result {
            Ok(row) => Ok(row),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Scan authorized rows using schema, grants, and policies loaded from this
    /// invocation's pinned catalog. Callers cannot substitute authorization
    /// contexts or grant records for this read path.
    pub fn scan_catalog_authorized_rows(
        &mut self,
        table_id: [u8; 16],
        cursor: Option<&RowScanCursor>,
        max_scanned: usize,
    ) -> Result<AuthorizedRowScanPage, TransactionError> {
        let result = (|| {
            let schema = self.load_catalog_table_schema(table_id)?;
            let principal_id = self.principal_id;
            let (role_ids, grants) = self.load_catalog_grants(principal_id, &schema)?;
            let policies = self.load_catalog_policies(principal_id, &schema)?;
            let scope = self.scope_from_grants(&grants);
            let context = AuthorizationContext {
                principal_id,
                role_ids,
                scope,
            };
            let projection = AuthorizedProjection {
                columns: &schema.columns,
                column_ids: &schema.column_ids,
                schema_version: schema.schema_version,
                schema_id: schema.schema_id,
                context: &context,
                grants: &grants,
                policies: &policies,
                checks: &[],
                policy_driving_columns: &[],
                dependencies: &[],
            };
            self.scan_authorized(table_id, cursor, max_scanned, &projection)
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

    /// Probe a catalog-loaded unique index for one non-NULL equality tuple.
    /// The path is available only when each key column is readable without a
    /// cell policy; otherwise the caller must retain the reference scan so
    /// redacted predicate behavior stays unchanged. Snapshot candidates are
    /// checked against their row version, then reconciled with staged writes
    /// and projected through the same pinned row/cell authorization path.
    pub fn probe_catalog_unique_index(
        &mut self,
        table_id: [u8; 16],
        index_id: [u8; 16],
        values: &[Option<ScalarValue>],
        projection: &AuthorizedProjection<'_>,
    ) -> Result<AuthorizedUniqueIndexProbe, TransactionError> {
        let result = (|| {
            if table_id == [0; 16]
                || index_id == [0; 16]
                || projection.columns.is_empty()
                || projection.columns.len() != projection.column_ids.len()
                || projection.columns.len() > crate::tuple::MAX_COLUMNS
                || projection.schema_version == 0
                || projection.schema_id == [0; 16]
                || projection.column_ids.contains(&[0; 16])
                || projection.grants.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
                || projection.policies.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
                || projection.checks.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
                || projection.policy_driving_columns.len()
                    > crate::authorization::MAX_AUTHORIZATION_INPUTS
                || projection.dependencies.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
            {
                return Err(TransactionError::InvalidScanRequest);
            }
            if self.active_scan.is_some() {
                return Err(TransactionError::ScanStillActive);
            }
            self.capture_authority_dependencies(projection.context, projection.dependencies)?;
            let schema = self.load_catalog_table_schema(table_id)?;
            if schema.schema_id != projection.schema_id
                || schema.schema_version != projection.schema_version
                || schema.column_ids != projection.column_ids
                || schema.columns != projection.columns
            {
                return Err(TransactionError::InvalidContext);
            }
            let indexes = if let Some(indexes) = self.loaded_indexes.get(&table_id) {
                indexes.clone()
            } else {
                self.load_catalog_indexes(&schema)?
            };
            let Some(index) = indexes
                .into_iter()
                .find(|index| index.index_id == index_id && index.table_id == table_id)
            else {
                return Ok(AuthorizedUniqueIndexProbe::Unavailable);
            };
            if !index.unique || index.column_ids.len() != values.len() {
                return Ok(AuthorizedUniqueIndexProbe::Unavailable);
            }
            let ordinals = index
                .column_ids
                .iter()
                .map(|column_id| {
                    schema
                        .column_ids
                        .iter()
                        .position(|candidate| candidate == column_id)
                        .ok_or(TransactionError::CorruptCatalog)
                })
                .collect::<Result<Vec<_>, _>>()?;
            if ordinals.iter().any(|ordinal| {
                !index_component_unconditionally_readable(
                    projection,
                    table_id,
                    schema.column_ids[*ordinal],
                )
            }) {
                return Ok(AuthorizedUniqueIndexProbe::Unavailable);
            }
            if values.iter().any(Option::is_none) {
                return Ok(AuthorizedUniqueIndexProbe::NoMatch);
            }
            let key_columns = ordinals
                .iter()
                .map(|ordinal| schema.columns[*ordinal].clone())
                .collect::<Vec<_>>();
            let encoded_tuple =
                encode_sql_tuple(&key_columns, values).map_err(TransactionError::Tuple)?;
            let mut prefix = Vec::with_capacity(16 + encoded_tuple.len());
            prefix.extend_from_slice(&index_id);
            prefix.extend_from_slice(&encoded_tuple);

            let snapshot_page = self
                .pinned_snapshot()?
                .scan_index_prefix(&prefix, None, 2)
                .map_err(|error| TransactionError::Storage(error.to_string()))?;
            if snapshot_page.candidates.len() > 1 || snapshot_page.continuation.is_some() {
                return Err(TransactionError::CorruptRowKey);
            }
            let remaining_work = self.max_scan_work.saturating_sub(self.scanned_work);
            let overlay_scan_work = self.overlay.staged_row_count()?;
            let Some(overlay_row_ids) = self
                .overlay
                .bounded_row_ids_for_table(table_id, remaining_work)?
            else {
                return Err(TransactionError::ScanBudgetExceeded);
            };
            let candidate_work = overlay_scan_work
                .checked_add(snapshot_page.candidates.len())
                .ok_or(TransactionError::ScanBudgetExceeded)?;
            self.charge_scan_work(candidate_work)?;

            let mut candidate_ids = HashSet::new();
            for candidate in &snapshot_page.candidates {
                let row_id = RowId {
                    table: table_id,
                    row: candidate.locator.row_id,
                };
                let stored = self
                    .pinned_snapshot()?
                    .get(&row_key(row_id))
                    .map_err(|error| TransactionError::Storage(error.to_string()))?
                    .ok_or(TransactionError::CorruptRowKey)?;
                let record = RowRecord::decode(&stored).map_err(TransactionError::CorruptRow)?;
                if record.row_version != candidate.locator.row_version {
                    return Err(TransactionError::CorruptRowKey);
                }
                candidate_ids.insert(row_id);
            }
            for row_id in overlay_row_ids {
                let id = RowId {
                    table: table_id,
                    row: row_id,
                };
                let Some(StagedRow::Value(payload)) =
                    self.overlay.get_for_actor(&id, self.principal_id)?
                else {
                    continue;
                };
                let derived = derive_index_key(
                    index_id,
                    row_id,
                    schema.schema_version,
                    &schema.column_ids,
                    &schema.columns,
                    payload,
                    &index.column_ids,
                )
                .map_err(|error| match error {
                    DeriveIndexKeyError::InvalidDefinition => TransactionError::CorruptCatalog,
                    DeriveIndexKeyError::Tuple(error) => TransactionError::Tuple(error),
                })?;
                if derived.key.starts_with(&prefix) {
                    candidate_ids.insert(id);
                }
            }
            if candidate_ids.len() > 1 {
                // A staged duplicate violates the unique-index assumption for
                // this access path; use the reference scan until finalization.
                return Ok(AuthorizedUniqueIndexProbe::Unavailable);
            }
            let mut candidate_ids = candidate_ids.into_iter().collect::<Vec<_>>();
            candidate_ids.sort_unstable_by_key(|id| id.row);
            let Some(candidate_id) = candidate_ids.into_iter().next() else {
                return Ok(AuthorizedUniqueIndexProbe::NoMatch);
            };
            let Some(cells) = self.read_authorized(candidate_id, projection)? else {
                return Ok(AuthorizedUniqueIndexProbe::NoMatch);
            };
            for (ordinal, expected) in ordinals.iter().zip(values) {
                match (&cells[*ordinal], expected) {
                    (SanitizedCell::Value(actual), Some(expected)) if actual == expected => {}
                    (SanitizedCell::Redacted, _) => {
                        return Ok(AuthorizedUniqueIndexProbe::Unavailable);
                    }
                    _ => return Ok(AuthorizedUniqueIndexProbe::NoMatch),
                }
            }
            Ok(AuthorizedUniqueIndexProbe::Match {
                id: candidate_id,
                cells,
            })
        })();
        match result {
            Ok(probe) => Ok(probe),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Read a bounded leading-key range from one catalog-loaded index. Storage
    /// validates the index against the pinned schema, requires every index
    /// component to be unconditionally readable, reconciles snapshot entries
    /// with private writes, and returns only sanitized authorized rows. Wide
    /// ranges fall back to the reference scan instead of returning a prefix.
    pub fn scan_catalog_index_range(
        &mut self,
        table_id: [u8; 16],
        index_id: [u8; 16],
        lower: Option<AuthorizedIndexRangeBound>,
        upper: Option<AuthorizedIndexRangeBound>,
        projection: &AuthorizedProjection<'_>,
    ) -> Result<AuthorizedIndexRangeScan, TransactionError> {
        let result = (|| {
            if table_id == [0; 16]
                || index_id == [0; 16]
                || projection.columns.is_empty()
                || projection.columns.len() != projection.column_ids.len()
                || projection.columns.len() > crate::tuple::MAX_COLUMNS
                || projection.schema_version == 0
                || projection.schema_id == [0; 16]
                || projection.column_ids.contains(&[0; 16])
                || projection.grants.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
                || projection.policies.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
                || projection.checks.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
                || projection.policy_driving_columns.len()
                    > crate::authorization::MAX_AUTHORIZATION_INPUTS
                || projection.dependencies.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
            {
                return Err(TransactionError::InvalidScanRequest);
            }
            if self.active_scan.is_some() {
                return Err(TransactionError::ScanStillActive);
            }
            self.capture_authority_dependencies(projection.context, projection.dependencies)?;
            let schema = self.load_catalog_table_schema(table_id)?;
            if schema.schema_id != projection.schema_id
                || schema.schema_version != projection.schema_version
                || schema.column_ids != projection.column_ids
                || schema.columns != projection.columns
            {
                return Err(TransactionError::InvalidContext);
            }
            let indexes = if let Some(indexes) = self.loaded_indexes.get(&table_id) {
                indexes.clone()
            } else {
                self.load_catalog_indexes(&schema)?
            };
            let Some(index) = indexes
                .into_iter()
                .find(|index| index.index_id == index_id && index.table_id == table_id)
            else {
                return Ok(AuthorizedIndexRangeScan::Unavailable);
            };
            let Some(first_column_id) = index.column_ids.first().copied() else {
                return Err(TransactionError::CorruptCatalog);
            };
            let key_ordinal = schema
                .column_ids
                .iter()
                .position(|column_id| *column_id == first_column_id)
                .ok_or(TransactionError::CorruptCatalog)?;
            if index.column_ids.iter().any(|column_id| {
                let Some(ordinal) = schema
                    .column_ids
                    .iter()
                    .position(|candidate| candidate == column_id)
                else {
                    return true;
                };
                !index_component_unconditionally_readable(
                    projection,
                    table_id,
                    schema.column_ids[ordinal],
                )
            }) {
                return Ok(AuthorizedIndexRangeScan::Unavailable);
            }

            let encode_bound = |bound: &AuthorizedIndexRangeBound| {
                encode_sql_tuple(
                    std::slice::from_ref(&schema.columns[key_ordinal]),
                    &[Some(bound.value.clone())],
                )
                .map_err(TransactionError::Tuple)
            };
            let lower_tuple = lower.as_ref().map(encode_bound).transpose()?;
            let upper_tuple = upper.as_ref().map(encode_bound).transpose()?;
            let snapshot_lower =
                lower_tuple
                    .as_ref()
                    .map(|tuple| crate::snapshot::IndexRangeBound {
                        tuple: tuple.clone(),
                        inclusive: lower.as_ref().is_some_and(|bound| bound.inclusive),
                    });
            let snapshot_upper =
                upper_tuple
                    .as_ref()
                    .map(|tuple| crate::snapshot::IndexRangeBound {
                        tuple: tuple.clone(),
                        inclusive: upper.as_ref().is_some_and(|bound| bound.inclusive),
                    });

            let lower_start = match &lower_tuple {
                Some(tuple) => {
                    let mut prefix = Vec::with_capacity(16 + tuple.len());
                    prefix.extend_from_slice(&index_id);
                    prefix.extend_from_slice(tuple);
                    if lower.as_ref().is_some_and(|bound| bound.inclusive) {
                        prefix
                    } else if let Some(successor) = crate::index_key::prefix_successor(&prefix) {
                        successor
                    } else {
                        return Ok(AuthorizedIndexRangeScan::Rows(Vec::new()));
                    }
                }
                None => index_id.to_vec(),
            };
            let upper_stop = match &upper_tuple {
                Some(tuple) => {
                    let mut prefix = Vec::with_capacity(16 + tuple.len());
                    prefix.extend_from_slice(&index_id);
                    prefix.extend_from_slice(tuple);
                    if upper.as_ref().is_some_and(|bound| bound.inclusive) {
                        crate::index_key::prefix_successor(&prefix)
                    } else {
                        Some(prefix)
                    }
                }
                None => crate::index_key::prefix_successor(&index_id),
            };
            if upper_stop.as_ref().is_some_and(|stop| lower_start >= *stop) {
                return Ok(AuthorizedIndexRangeScan::Rows(Vec::new()));
            }

            let mut snapshot_candidates = Vec::new();
            let mut snapshot_bytes = 0usize;
            let mut after_key: Option<Vec<u8>> = None;
            loop {
                let page = self
                    .pinned_snapshot()?
                    .scan_index_range(
                        &index_id,
                        snapshot_lower.clone(),
                        snapshot_upper.clone(),
                        after_key.as_deref(),
                        crate::snapshot::MAX_ROW_SCAN_PAGE,
                    )
                    .map_err(|error| TransactionError::Storage(error.to_string()))?;
                snapshot_bytes = snapshot_bytes.saturating_add(
                    page.candidates
                        .iter()
                        .map(|candidate| candidate.key.len())
                        .sum::<usize>(),
                );
                if snapshot_candidates
                    .len()
                    .saturating_add(page.candidates.len())
                    > self.max_authorized_index_range_candidates
                    || snapshot_bytes > self.max_authorized_index_range_bytes
                {
                    self.charge_scan_work(
                        snapshot_candidates
                            .len()
                            .saturating_add(page.candidates.len()),
                    )?;
                    return Ok(AuthorizedIndexRangeScan::Unavailable);
                }
                snapshot_candidates.extend(page.candidates);
                let Some(continuation) = page.continuation else {
                    break;
                };
                after_key = Some(continuation);
            }

            let remaining_work = self.max_scan_work.saturating_sub(self.scanned_work);
            let overlay_scan_work = self.overlay.staged_row_count()?;
            let Some(overlay_row_ids) = self
                .overlay
                .bounded_row_ids_for_table(table_id, remaining_work)?
            else {
                return Err(TransactionError::ScanBudgetExceeded);
            };
            let candidate_work = overlay_scan_work
                .checked_add(snapshot_candidates.len())
                .ok_or(TransactionError::ScanBudgetExceeded)?;
            self.charge_scan_work(candidate_work)?;

            let in_range = |key: &[u8]| {
                key.len() >= 16
                    && key[..16] == index_id
                    && key >= lower_start.as_slice()
                    && upper_stop.as_ref().is_none_or(|stop| key < stop.as_slice())
            };
            let mut candidate_ids = HashSet::new();
            let mut candidate_row_bytes = 0usize;
            for candidate in &snapshot_candidates {
                let row_id = RowId {
                    table: table_id,
                    row: candidate.locator.row_id,
                };
                let stored = self
                    .pinned_snapshot()?
                    .get(&row_key(row_id))
                    .map_err(|error| TransactionError::Storage(error.to_string()))?
                    .ok_or(TransactionError::CorruptRowKey)?;
                let record = RowRecord::decode(&stored).map_err(TransactionError::CorruptRow)?;
                if record.row_version != candidate.locator.row_version {
                    return Err(TransactionError::CorruptRowKey);
                }
                let derived = derive_index_key(
                    index_id,
                    row_id.row,
                    schema.schema_version,
                    &schema.column_ids,
                    &schema.columns,
                    &record.payload,
                    &index.column_ids,
                )
                .map_err(|error| match error {
                    DeriveIndexKeyError::InvalidDefinition => TransactionError::CorruptCatalog,
                    DeriveIndexKeyError::Tuple(error) => TransactionError::Tuple(error),
                })?;
                if derived.key != candidate.key {
                    return Err(TransactionError::CorruptRowKey);
                }
                let tuple = decode_tuple(&schema.columns, schema.schema_version, &record.payload)
                    .map_err(TransactionError::Tuple)?;
                if tuple.values[key_ordinal].is_some() && in_range(&candidate.key) {
                    candidate_row_bytes = candidate_row_bytes.saturating_add(record.payload.len());
                    if candidate_row_bytes > self.max_authorized_index_range_bytes {
                        return Ok(AuthorizedIndexRangeScan::Unavailable);
                    }
                    candidate_ids.insert(row_id);
                }
            }
            for row_bytes in overlay_row_ids {
                let row_id = RowId {
                    table: table_id,
                    row: row_bytes,
                };
                match self.overlay.get_for_actor(&row_id, self.principal_id)? {
                    Some(StagedRow::Delete) => {
                        candidate_ids.remove(&row_id);
                    }
                    Some(StagedRow::Value(payload)) => {
                        let derived = derive_index_key(
                            index_id,
                            row_id.row,
                            schema.schema_version,
                            &schema.column_ids,
                            &schema.columns,
                            payload,
                            &index.column_ids,
                        )
                        .map_err(|error| match error {
                            DeriveIndexKeyError::InvalidDefinition => {
                                TransactionError::CorruptCatalog
                            }
                            DeriveIndexKeyError::Tuple(error) => TransactionError::Tuple(error),
                        })?;
                        let tuple = decode_tuple(&schema.columns, schema.schema_version, payload)
                            .map_err(TransactionError::Tuple)?;
                        if tuple.values[key_ordinal].is_some() && in_range(&derived.key) {
                            candidate_row_bytes = candidate_row_bytes.saturating_add(payload.len());
                            if candidate_row_bytes > self.max_authorized_index_range_bytes {
                                return Ok(AuthorizedIndexRangeScan::Unavailable);
                            }
                            candidate_ids.insert(row_id);
                        } else {
                            candidate_ids.remove(&row_id);
                        }
                    }
                    None => {}
                }
            }
            if candidate_ids.len() > self.max_authorized_index_range_candidates {
                return Ok(AuthorizedIndexRangeScan::Unavailable);
            }
            let mut candidate_ids = candidate_ids.into_iter().collect::<Vec<_>>();
            candidate_ids.sort_unstable_by_key(|id| id.row);
            let mut rows = Vec::with_capacity(candidate_ids.len());
            for candidate_id in candidate_ids {
                if let Some(cells) = self.read_authorized(candidate_id, projection)? {
                    rows.push((candidate_id, cells));
                }
            }
            Ok(AuthorizedIndexRangeScan::Rows(rows))
        })();
        match result {
            Ok(scan) => Ok(scan),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Scan the contiguous leading prefix of a composite index. Every value is
    /// typed by the corresponding pinned index column; only authorized
    /// sanitized rows are returned, and an oversized prefix result is never
    /// returned partially.
    pub fn scan_catalog_index_prefix(
        &mut self,
        table_id: [u8; 16],
        index_id: [u8; 16],
        values: &[ScalarValue],
        projection: &AuthorizedProjection<'_>,
    ) -> Result<AuthorizedIndexRangeScan, TransactionError> {
        let result = (|| {
            if table_id == [0; 16]
                || index_id == [0; 16]
                || !(2..=8).contains(&values.len())
                || projection.columns.is_empty()
                || projection.columns.len() != projection.column_ids.len()
                || projection.columns.len() > crate::tuple::MAX_COLUMNS
                || projection.schema_version == 0
                || projection.schema_id == [0; 16]
                || projection.column_ids.contains(&[0; 16])
                || projection.grants.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
                || projection.policies.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
                || projection.checks.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
                || projection.policy_driving_columns.len()
                    > crate::authorization::MAX_AUTHORIZATION_INPUTS
                || projection.dependencies.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
            {
                return Err(TransactionError::InvalidScanRequest);
            }
            if self.active_scan.is_some() {
                return Err(TransactionError::ScanStillActive);
            }
            self.capture_authority_dependencies(projection.context, projection.dependencies)?;
            let schema = self.load_catalog_table_schema(table_id)?;
            if schema.schema_id != projection.schema_id
                || schema.schema_version != projection.schema_version
                || schema.column_ids != projection.column_ids
                || schema.columns != projection.columns
            {
                return Err(TransactionError::InvalidContext);
            }
            let indexes = if let Some(indexes) = self.loaded_indexes.get(&table_id) {
                indexes.clone()
            } else {
                self.load_catalog_indexes(&schema)?
            };
            let Some(index) = indexes
                .into_iter()
                .find(|index| index.index_id == index_id && index.table_id == table_id)
            else {
                return Ok(AuthorizedIndexRangeScan::Unavailable);
            };
            if index.column_ids.len() < values.len()
                || index.column_ids.iter().any(|column_id| {
                    let Some(ordinal) = schema
                        .column_ids
                        .iter()
                        .position(|candidate| candidate == column_id)
                    else {
                        return true;
                    };
                    !index_component_unconditionally_readable(
                        projection,
                        table_id,
                        schema.column_ids[ordinal],
                    )
                })
            {
                return Ok(AuthorizedIndexRangeScan::Unavailable);
            }
            let mut prefix_schemas = Vec::with_capacity(values.len());
            for column_id in index.column_ids.iter().take(values.len()) {
                let Some(ordinal) = schema
                    .column_ids
                    .iter()
                    .position(|candidate| candidate == column_id)
                else {
                    return Err(TransactionError::CorruptCatalog);
                };
                // `encode_sql_tuple` below validates each value against the
                // catalog column's scalar type.
                prefix_schemas.push(schema.columns[ordinal].clone());
            }
            let values = values.iter().cloned().map(Some).collect::<Vec<_>>();
            let tuple =
                encode_sql_tuple(&prefix_schemas, &values).map_err(TransactionError::Tuple)?;
            let mut key_prefix = Vec::with_capacity(16 + tuple.len());
            key_prefix.extend_from_slice(&index_id);
            key_prefix.extend_from_slice(&tuple);

            let mut snapshot_candidates = Vec::new();
            let mut snapshot_bytes = 0usize;
            let mut after_key: Option<Vec<u8>> = None;
            loop {
                let page = self
                    .pinned_snapshot()?
                    .scan_index_prefix(
                        &key_prefix,
                        after_key.as_deref(),
                        crate::snapshot::MAX_ROW_SCAN_PAGE,
                    )
                    .map_err(|error| TransactionError::Storage(error.to_string()))?;
                snapshot_bytes = snapshot_bytes.saturating_add(
                    page.candidates
                        .iter()
                        .map(|candidate| candidate.key.len())
                        .sum::<usize>(),
                );
                if snapshot_candidates
                    .len()
                    .saturating_add(page.candidates.len())
                    > self.max_authorized_index_range_candidates
                    || snapshot_bytes > self.max_authorized_index_range_bytes
                {
                    self.charge_scan_work(
                        snapshot_candidates
                            .len()
                            .saturating_add(page.candidates.len()),
                    )?;
                    return Ok(AuthorizedIndexRangeScan::Unavailable);
                }
                snapshot_candidates.extend(page.candidates);
                let Some(continuation) = page.continuation else {
                    break;
                };
                after_key = Some(continuation);
            }

            let remaining_work = self.max_scan_work.saturating_sub(self.scanned_work);
            let overlay_scan_work = self.overlay.staged_row_count()?;
            let Some(overlay_row_ids) = self
                .overlay
                .bounded_row_ids_for_table(table_id, remaining_work)?
            else {
                return Err(TransactionError::ScanBudgetExceeded);
            };
            let candidate_work = overlay_scan_work
                .checked_add(snapshot_candidates.len())
                .ok_or(TransactionError::ScanBudgetExceeded)?;
            self.charge_scan_work(candidate_work)?;

            let mut candidate_ids = HashSet::new();
            let mut candidate_row_bytes = 0usize;
            for candidate in &snapshot_candidates {
                let row_id = RowId {
                    table: table_id,
                    row: candidate.locator.row_id,
                };
                let stored = self
                    .pinned_snapshot()?
                    .get(&row_key(row_id))
                    .map_err(|error| TransactionError::Storage(error.to_string()))?
                    .ok_or(TransactionError::CorruptRowKey)?;
                let record = RowRecord::decode(&stored).map_err(TransactionError::CorruptRow)?;
                if record.row_version != candidate.locator.row_version {
                    return Err(TransactionError::CorruptRowKey);
                }
                let derived = derive_index_key(
                    index_id,
                    row_id.row,
                    schema.schema_version,
                    &schema.column_ids,
                    &schema.columns,
                    &record.payload,
                    &index.column_ids,
                )
                .map_err(|error| match error {
                    DeriveIndexKeyError::InvalidDefinition => TransactionError::CorruptCatalog,
                    DeriveIndexKeyError::Tuple(error) => TransactionError::Tuple(error),
                })?;
                if derived.key != candidate.key {
                    return Err(TransactionError::CorruptRowKey);
                }
                if candidate.key.starts_with(&key_prefix) {
                    candidate_row_bytes = candidate_row_bytes.saturating_add(record.payload.len());
                    if candidate_row_bytes > self.max_authorized_index_range_bytes {
                        return Ok(AuthorizedIndexRangeScan::Unavailable);
                    }
                    candidate_ids.insert(row_id);
                }
            }
            for row_bytes in overlay_row_ids {
                let row_id = RowId {
                    table: table_id,
                    row: row_bytes,
                };
                match self.overlay.get_for_actor(&row_id, self.principal_id)? {
                    Some(StagedRow::Delete) => {
                        candidate_ids.remove(&row_id);
                    }
                    Some(StagedRow::Value(payload)) => {
                        let derived = derive_index_key(
                            index_id,
                            row_id.row,
                            schema.schema_version,
                            &schema.column_ids,
                            &schema.columns,
                            payload,
                            &index.column_ids,
                        )
                        .map_err(|error| match error {
                            DeriveIndexKeyError::InvalidDefinition => {
                                TransactionError::CorruptCatalog
                            }
                            DeriveIndexKeyError::Tuple(error) => TransactionError::Tuple(error),
                        })?;
                        if derived.key.starts_with(&key_prefix) {
                            candidate_row_bytes = candidate_row_bytes.saturating_add(payload.len());
                            if candidate_row_bytes > self.max_authorized_index_range_bytes {
                                return Ok(AuthorizedIndexRangeScan::Unavailable);
                            }
                            candidate_ids.insert(row_id);
                        } else {
                            candidate_ids.remove(&row_id);
                        }
                    }
                    None => {}
                }
            }
            if candidate_ids.len() > self.max_authorized_index_range_candidates {
                return Ok(AuthorizedIndexRangeScan::Unavailable);
            }
            let mut candidate_ids = candidate_ids.into_iter().collect::<Vec<_>>();
            candidate_ids.sort_unstable_by_key(|id| id.row);
            let mut rows = Vec::with_capacity(candidate_ids.len());
            for candidate_id in candidate_ids {
                if let Some(cells) = self.read_authorized(candidate_id, projection)? {
                    rows.push((candidate_id, cells));
                }
            }
            Ok(AuthorizedIndexRangeScan::Rows(rows))
        })();
        match result {
            Ok(scan) => Ok(scan),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Catalog-derived entry point for a unique equality probe. Schema,
    /// principal/role grants, and row/cell policies all come from this
    /// invocation's pinned catalog before the projection-based trusted helper
    /// is called.
    pub fn probe_catalog_unique_index_for_principal(
        &mut self,
        table_id: [u8; 16],
        index_id: [u8; 16],
        values: &[Option<ScalarValue>],
    ) -> Result<AuthorizedUniqueIndexProbe, TransactionError> {
        let result = (|| {
            let projection = self.load_catalog_estimate_projection(table_id)?;
            self.load_catalog_indexes(&projection.schema)?;
            self.probe_catalog_unique_index(table_id, index_id, values, &projection.as_projection())
        })();
        match result {
            Ok(probe) => Ok(probe),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Catalog-derived entry point for a bounded leading-key range scan.
    pub fn scan_catalog_index_range_for_principal(
        &mut self,
        table_id: [u8; 16],
        index_id: [u8; 16],
        lower: Option<AuthorizedIndexRangeBound>,
        upper: Option<AuthorizedIndexRangeBound>,
    ) -> Result<AuthorizedIndexRangeScan, TransactionError> {
        let result = (|| {
            let catalog_projection = self.load_catalog_estimate_projection(table_id)?;
            self.load_catalog_indexes(&catalog_projection.schema)?;
            self.scan_catalog_index_range(
                table_id,
                index_id,
                lower,
                upper,
                &catalog_projection.as_projection(),
            )
        })();
        match result {
            Ok(scan) => Ok(scan),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    pub fn scan_catalog_index_prefix_for_principal(
        &mut self,
        table_id: [u8; 16],
        index_id: [u8; 16],
        values: &[ScalarValue],
    ) -> Result<AuthorizedIndexRangeScan, TransactionError> {
        let result = (|| {
            let catalog_projection = self.load_catalog_estimate_projection(table_id)?;
            self.load_catalog_indexes(&catalog_projection.schema)?;
            self.scan_catalog_index_prefix(
                table_id,
                index_id,
                values,
                &catalog_projection.as_projection(),
            )
        })();
        match result {
            Ok(scan) => Ok(scan),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    pub(super) fn load_catalog_executor_write_projection(
        &mut self,
        table_id: [u8; 16],
    ) -> Result<CatalogTableProjection, TransactionError> {
        let schema = self.load_catalog_table_schema(table_id)?;
        let principal_id = self.principal_id;
        let (role_ids, grants) = self.load_catalog_grants(principal_id, &schema)?;
        let (policies, policy_driving_columns) =
            self.load_catalog_policies_with_policy_inputs(principal_id, &schema, true)?;
        let checks = self.load_catalog_check_constraints(&schema)?;
        self.load_catalog_indexes(&schema)?;
        let scope = self.scope_from_grants(&grants);
        let context = AuthorizationContext {
            principal_id,
            role_ids,
            scope,
        };
        Ok(CatalogTableProjection {
            schema,
            context,
            grants,
            policies,
            checks,
            policy_driving_columns,
        })
    }

    pub(super) fn load_catalog_estimate_projection(
        &mut self,
        table_id: [u8; 16],
    ) -> Result<CatalogTableProjection, TransactionError> {
        let schema = self.load_catalog_table_schema(table_id)?;
        let principal_id = self.principal_id;
        let (role_ids, grants) = self.load_catalog_grants(principal_id, &schema)?;
        let policies = self.load_catalog_policies(principal_id, &schema)?;
        let scope = self.scope_from_grants(&grants);
        let context = AuthorizationContext {
            principal_id,
            role_ids,
            scope,
        };
        let mut referenced_ordinals = HashSet::new();
        for policy in &policies {
            if let Some(expression) = &policy.using {
                collect_expression_fields(expression, &mut referenced_ordinals);
            }
            if let Some(expression) = &policy.with_check {
                collect_expression_fields(expression, &mut referenced_ordinals);
            }
        }
        let mut policy_driving_columns = referenced_ordinals
            .into_iter()
            .map(|ordinal| {
                schema
                    .column_ids
                    .get(ordinal)
                    .copied()
                    .ok_or(TransactionError::CorruptCatalog)
            })
            .collect::<Result<Vec<_>, _>>()?;
        policy_driving_columns.sort_unstable();
        Ok(CatalogTableProjection {
            schema,
            context,
            grants,
            policies,
            checks: Vec::new(),
            policy_driving_columns,
        })
    }

    pub(super) fn discard_active_scan_for_table(&mut self, table_id: [u8; 16]) {
        if self
            .active_scan
            .as_ref()
            .is_some_and(|active| active.table_id == table_id)
        {
            self.active_scan = None;
        }
    }
}
pub(super) fn row_key(id: RowId) -> [u8; 32] {
    let mut key = [0; 32];
    key[..16].copy_from_slice(&id.table);
    key[16..].copy_from_slice(&id.row);
    key
}
