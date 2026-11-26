use super::*;
use crate::authorization::{
    AccessTarget, AuthorizedProjection, Effect, Expr, Operation, SanitizedCell, Subject,
    has_permission, project_row,
};
use crate::commit::CommitError;
use crate::index_key::encode_sql_tuple;
use crate::overlay::{OverlayError, RowId, StagedRow};
use crate::row_record::RowRecord;
use crate::snapshot::MAX_ROW_SCAN_PAGE;
use crate::statistics::{
    AuthorizedScanEstimate, AuthorizedScanSummary, ColumnPopulation, CommittedPopulationRecord,
    DEFAULT_ESTIMATE_CANDIDATES, MAX_CONFIGURABLE_ESTIMATE_CANDIDATES, MAX_ESTIMATE_CANDIDATES,
    MAX_JOIN_ESTIMATE_BYTES, MAX_JOIN_ESTIMATE_KEY_BYTES, MAX_JOIN_ESTIMATE_ROWS,
    MAX_PREDICATE_ESTIMATE_ROWS,
};
use crate::tuple::{ScalarType, ScalarValue, decode_tuple};
use std::collections::{HashMap, HashSet};

impl<'db> InvocationTransaction<'db> {
    /// Estimate whether the authorized population fits under a fixed bound.
    /// It reuses a fresh protected committed count only when the pinned
    /// snapshot and literal row policies prove the full committed population
    /// is visible. It then adjusts for bounded current-actor overlay inserts
    /// and deletes using row-key existence, without decoding values. Otherwise
    /// it reads at most `maximum_rows + 1` snapshot keys in bounded pages.
    pub fn estimate_authorized_population_at_most(
        &mut self,
        table_id: [u8; 16],
        projection: &AuthorizedProjection<'_>,
        maximum_rows: u64,
    ) -> Result<Option<u64>, TransactionError> {
        self.check_deadline()?;
        if maximum_rows > MAX_ESTIMATE_CANDIDATES as u64
            || !valid_estimate_projection(table_id, projection)
        {
            return Ok(None);
        }
        if let Err(error) =
            self.capture_authority_dependencies(projection.context, projection.dependencies)
        {
            return if estimate_error_is_terminal(&error) {
                Err(error)
            } else {
                Ok(None)
            };
        }
        match estimate_population(table_id, projection) {
            Some(EstimatePopulation::NoRows) => return self.finish_population_probe(Some(0)),
            Some(EstimatePopulation::AllRows) => {
                let overlay_is_empty = match self.overlay.staged_row_count() {
                    Ok(rows) => rows == 0,
                    Err(OverlayError::Aborted) => return Err(TransactionError::Aborted),
                    Err(_) => return self.finish_population_probe(None),
                };
                let record = self
                    .pinned_snapshot()?
                    .committed_population_record(table_id)
                    .ok()
                    .flatten();
                match record {
                    Some(CommittedPopulationRecord::Exact { rows, .. }) if overlay_is_empty => {
                        return self
                            .finish_population_probe((rows <= maximum_rows).then_some(rows));
                    }
                    Some(CommittedPopulationRecord::Exact { rows, .. }) => {
                        let mut overlay_complete = true;
                        let overlay_candidate_limit =
                            usize::try_from(maximum_rows).unwrap_or(MAX_ESTIMATE_CANDIDATES);
                        let overlay_row_ids = match self
                            .overlay
                            .bounded_row_ids_for_table(table_id, overlay_candidate_limit)
                        {
                            Ok(Some(row_ids)) => row_ids,
                            Ok(None) => {
                                overlay_complete = false;
                                Vec::new()
                            }
                            Err(OverlayError::Aborted) => return Err(TransactionError::Aborted),
                            Err(_) => return self.finish_population_probe(None),
                        };
                        let mut adjusted_rows = rows;
                        for row_id in overlay_row_ids {
                            self.check_deadline()?;
                            let id = RowId {
                                table: table_id,
                                row: row_id,
                            };
                            let new_exists =
                                match self.overlay.get_for_actor(&id, self.principal_id) {
                                    Ok(Some(StagedRow::Value(_))) => true,
                                    Ok(Some(StagedRow::Delete)) => false,
                                    Ok(None) => continue,
                                    Err(OverlayError::Aborted) => {
                                        return Err(TransactionError::Aborted);
                                    }
                                    Err(_) => {
                                        overlay_complete = false;
                                        break;
                                    }
                                };
                            let old_exists = match self.pinned_snapshot()?.get(&row_key(id)) {
                                Ok(row) => row.is_some(),
                                Err(_) => {
                                    overlay_complete = false;
                                    break;
                                }
                            };
                            let Some(next_rows) = adjusted_rows
                                .checked_sub(u64::from(old_exists))
                                .and_then(|count| count.checked_add(u64::from(new_exists)))
                            else {
                                overlay_complete = false;
                                break;
                            };
                            adjusted_rows = next_rows;
                        }
                        if overlay_complete {
                            return self.finish_population_probe(
                                (adjusted_rows <= maximum_rows).then_some(adjusted_rows),
                            );
                        }
                    }
                    Some(CommittedPopulationRecord::Unknown { .. }) if overlay_is_empty => {
                        return self.finish_population_probe(None);
                    }
                    None | Some(CommittedPopulationRecord::Unknown { .. }) => {}
                }
            }
            None => return self.finish_population_probe(None),
        }
        let Some(candidate_limit) = usize::try_from(maximum_rows)
            .ok()
            .and_then(|rows| rows.checked_add(1))
        else {
            return self.finish_population_probe(None);
        };
        let mut snapshot_ids = Vec::with_capacity(candidate_limit.min(MAX_ROW_SCAN_PAGE));
        let mut after_row_id = None;
        loop {
            let limit = candidate_limit
                .saturating_sub(snapshot_ids.len())
                .min(MAX_ROW_SCAN_PAGE);
            if limit == 0 {
                return self.finish_population_probe(None);
            }
            let (page, complete) =
                match self
                    .pinned_snapshot()?
                    .scan_row_ids_after(table_id, after_row_id, limit)
                {
                    Ok(page) => page,
                    Err(_) => return self.finish_population_probe(None),
                };
            if page.is_empty() {
                break;
            }
            after_row_id = page.last().copied();
            snapshot_ids.extend(page);
            if complete {
                break;
            }
        }
        if snapshot_ids.len() == candidate_limit {
            return self.finish_population_probe(None);
        }
        let mut visible_rows = snapshot_ids.into_iter().collect::<HashSet<_>>();
        let overlay_limit = match self.overlay.staged_row_count() {
            Ok(rows) if rows <= maximum_rows as usize => rows,
            Ok(_) => return self.finish_population_probe(None),
            Err(OverlayError::Aborted) => return Err(TransactionError::Aborted),
            Err(_) => return self.finish_population_probe(None),
        };
        let overlay_row_ids = match self
            .overlay
            .bounded_row_ids_for_table(table_id, overlay_limit)
        {
            Ok(Some(row_ids)) => row_ids,
            Ok(None) => return self.finish_population_probe(None),
            Err(OverlayError::Aborted) => return Err(TransactionError::Aborted),
            Err(_) => return self.finish_population_probe(None),
        };
        for row_id in overlay_row_ids {
            let staged = match self.overlay.get_for_actor(
                &RowId {
                    table: table_id,
                    row: row_id,
                },
                self.principal_id,
            ) {
                Ok(Some(staged)) => staged,
                Ok(None) => return self.finish_population_probe(None),
                Err(OverlayError::Aborted) => return Err(TransactionError::Aborted),
                Err(_) => return self.finish_population_probe(None),
            };
            match staged {
                StagedRow::Value(_) => {
                    visible_rows.insert(row_id);
                }
                StagedRow::Delete => {
                    visible_rows.remove(&row_id);
                }
            }
            if visible_rows.len() > maximum_rows as usize {
                return self.finish_population_probe(None);
            }
        }
        self.finish_population_probe(Some(visible_rows.len() as u64))
    }

    /// Estimate the exact INNER JOIN output count for bounded authorized
    /// populations. Join-key values are read only when each key column is
    /// unconditionally readable and both row populations are value-independent.
    #[allow(clippy::too_many_arguments)] // Each relation supplies an ID, projection, and paired key IDs.
    pub fn estimate_authorized_join_cardinality_at_most(
        &mut self,
        left_table_id: [u8; 16],
        left_projection: &AuthorizedProjection<'_>,
        left_key_column_ids: &[[u8; 16]],
        right_table_id: [u8; 16],
        right_projection: &AuthorizedProjection<'_>,
        right_key_column_ids: &[[u8; 16]],
        maximum_rows: u64,
    ) -> Result<Option<u64>, TransactionError> {
        self.check_deadline()?;
        if maximum_rows > MAX_JOIN_ESTIMATE_ROWS
            || left_key_column_ids.is_empty()
            || left_key_column_ids.len() > 8
            || left_key_column_ids.len() != right_key_column_ids.len()
            || !valid_estimate_projection(left_table_id, left_projection)
            || !valid_estimate_projection(right_table_id, right_projection)
            || left_projection.schema_id != right_projection.schema_id
            || left_projection.schema_version != right_projection.schema_version
            || left_projection.context.principal_id != right_projection.context.principal_id
            || left_projection.context.role_ids != right_projection.context.role_ids
        {
            return Ok(None);
        }
        let left_ordinals = join_key_ordinals(left_table_id, left_projection, left_key_column_ids);
        let right_ordinals =
            join_key_ordinals(right_table_id, right_projection, right_key_column_ids);
        let (Some(left_ordinals), Some(right_ordinals)) = (left_ordinals, right_ordinals) else {
            return Ok(None);
        };
        if left_ordinals
            .iter()
            .zip(&right_ordinals)
            .any(|(left, right)| {
                !estimate_join_types_compatible(
                    left_projection.columns[*left].scalar_type,
                    right_projection.columns[*right].scalar_type,
                )
            })
        {
            return Ok(None);
        }
        if let Err(error) = self
            .capture_authority_dependencies(left_projection.context, left_projection.dependencies)
            .and_then(|()| {
                self.capture_authority_dependencies(
                    right_projection.context,
                    right_projection.dependencies,
                )
            })
        {
            return if estimate_error_is_terminal(&error) {
                Err(error)
            } else {
                Ok(None)
            };
        }
        let left_population = estimate_population(left_table_id, left_projection);
        let right_population = estimate_population(right_table_id, right_projection);
        let (Some(left_population), Some(right_population)) = (left_population, right_population)
        else {
            return Ok(None);
        };
        if left_population == EstimatePopulation::NoRows
            || right_population == EstimatePopulation::NoRows
        {
            self.check_deadline()?;
            return Ok(Some(0));
        }
        if left_key_column_ids.iter().any(|column_id| {
            !index_component_unconditionally_readable(left_projection, left_table_id, *column_id)
        }) || right_key_column_ids.iter().any(|column_id| {
            !index_component_unconditionally_readable(right_projection, right_table_id, *column_id)
        }) {
            return Ok(None);
        }
        let mut row_bytes = 0usize;
        let Some(left_frequencies) = self.estimate_join_key_frequencies(
            left_table_id,
            left_projection,
            &left_ordinals,
            maximum_rows,
            &mut row_bytes,
        )?
        else {
            return Ok(None);
        };
        let Some(right_frequencies) = self.estimate_join_key_frequencies(
            right_table_id,
            right_projection,
            &right_ordinals,
            maximum_rows,
            &mut row_bytes,
        )?
        else {
            return Ok(None);
        };
        let mut joined_rows = 0u64;
        for (key, left_count) in left_frequencies {
            let Some(right_count) = right_frequencies.get(&key) else {
                continue;
            };
            let Some(matches) = left_count.checked_mul(*right_count) else {
                return Ok(None);
            };
            let Some(total) = joined_rows.checked_add(matches) else {
                return Ok(None);
            };
            joined_rows = total;
        }
        self.check_deadline()?;
        Ok(Some(joined_rows))
    }

    pub(super) fn estimate_join_key_frequencies(
        &mut self,
        table_id: [u8; 16],
        projection: &AuthorizedProjection<'_>,
        key_ordinals: &[usize],
        maximum_rows: u64,
        bytes_read: &mut usize,
    ) -> Result<Option<HashMap<Vec<u8>, u64>>, TransactionError> {
        let Some(candidate_limit) = usize::try_from(maximum_rows)
            .ok()
            .and_then(|rows| rows.checked_add(1))
        else {
            return Ok(None);
        };
        let mut row_ids = Vec::with_capacity(candidate_limit.min(MAX_ROW_SCAN_PAGE));
        let mut after_row_id = None;
        loop {
            let limit = candidate_limit
                .saturating_sub(row_ids.len())
                .min(MAX_ROW_SCAN_PAGE);
            if limit == 0 {
                return Ok(None);
            }
            let (page, complete) =
                match self
                    .pinned_snapshot()?
                    .scan_row_ids_after(table_id, after_row_id, limit)
                {
                    Ok(page) => page,
                    Err(_) => return Ok(None),
                };
            if page.is_empty() {
                if complete {
                    break;
                }
                return Ok(None);
            }
            if page.len() < limit && !complete {
                return Ok(None);
            }
            after_row_id = page.last().copied();
            row_ids.extend(page);
            if complete {
                break;
            }
        }
        if row_ids.len() == candidate_limit {
            return Ok(None);
        }
        let overlay_ids = match self
            .overlay
            .bounded_row_ids_for_table(table_id, maximum_rows as usize)
        {
            Ok(Some(ids)) => ids,
            Ok(None) => return Ok(None),
            Err(OverlayError::Aborted) => return Err(TransactionError::Aborted),
            Err(_) => return Ok(None),
        };
        row_ids.extend(overlay_ids);
        row_ids.sort_unstable();
        row_ids.dedup();
        if row_ids.len() as u64 > maximum_rows {
            return Ok(None);
        }
        let key_columns = key_ordinals
            .iter()
            .map(|ordinal| projection.columns[*ordinal].clone())
            .collect::<Vec<_>>();
        let mut frequencies = HashMap::new();
        for row_id in row_ids {
            self.check_deadline()?;
            let id = RowId {
                table: table_id,
                row: row_id,
            };
            match self.overlay.get_for_actor(&id, self.principal_id) {
                Ok(Some(StagedRow::Delete)) => continue,
                Ok(Some(StagedRow::Value(_))) | Ok(None) => {}
                Err(OverlayError::Aborted) => return Err(TransactionError::Aborted),
                Err(_) => return Ok(None),
            }
            let payload = match self.read_inner(id) {
                Ok(Some(payload)) => payload,
                Ok(None) => return Ok(None),
                Err(error) if estimate_error_is_terminal(&error) => return Err(error),
                Err(_) => return Ok(None),
            };
            *bytes_read = bytes_read.saturating_add(payload.len());
            if *bytes_read > MAX_JOIN_ESTIMATE_BYTES {
                return Ok(None);
            }
            let tuple = match decode_tuple(projection.columns, projection.schema_version, &payload)
            {
                Ok(tuple) => tuple,
                Err(_) => return Ok(None),
            };
            let cells = match project_row(
                projection.context,
                projection.grants,
                projection.policies,
                projection.schema_id,
                table_id,
                projection.column_ids,
                &tuple.values,
            ) {
                Ok(Some(cells)) => cells,
                Ok(None) => return Ok(None),
                Err(_) => return Ok(None),
            };
            let mut values = Vec::with_capacity(key_ordinals.len());
            let mut contains_null = false;
            for ordinal in key_ordinals {
                match cells.get(*ordinal) {
                    Some(SanitizedCell::Value(value)) => values.push(Some(value.clone())),
                    Some(SanitizedCell::Null) => contains_null = true,
                    Some(SanitizedCell::Redacted) | None => return Ok(None),
                }
            }
            if contains_null {
                continue;
            }
            let key = match encode_sql_tuple(&key_columns, &values) {
                Ok(key) if key.len() <= MAX_JOIN_ESTIMATE_KEY_BYTES => key,
                _ => return Ok(None),
            };
            *frequencies.entry(key).or_insert(0u64) += 1;
        }
        Ok(Some(frequencies))
    }

    /// Estimate exact matches for a bounded predicate over an authorized
    /// population. This intentionally returns unknown unless every referenced
    /// cell is readable for every candidate row.
    pub fn estimate_authorized_predicate_at_most(
        &mut self,
        table_id: [u8; 16],
        projection: &AuthorizedProjection<'_>,
        predicate: &crate::optimizer_ir::BoundExpr,
        maximum_rows: u64,
    ) -> Result<Option<(u64, u64)>, TransactionError> {
        if maximum_rows > DEFAULT_ESTIMATE_CANDIDATES as u64 {
            return Ok(None);
        }
        self.estimate_authorized_predicate_at_most_with_limit(
            table_id,
            projection,
            predicate,
            maximum_rows,
            DEFAULT_ESTIMATE_CANDIDATES,
        )
    }

    pub(super) fn estimate_authorized_predicate_at_most_with_limit(
        &mut self,
        table_id: [u8; 16],
        projection: &AuthorizedProjection<'_>,
        predicate: &crate::optimizer_ir::BoundExpr,
        maximum_rows: u64,
        candidate_limit: usize,
    ) -> Result<Option<(u64, u64)>, TransactionError> {
        self.check_deadline()?;
        let maximum_rows = maximum_rows.min(candidate_limit as u64);
        if candidate_limit == 0
            || candidate_limit > MAX_CONFIGURABLE_ESTIMATE_CANDIDATES
            || maximum_rows > MAX_PREDICATE_ESTIMATE_ROWS
            || !valid_estimate_projection(table_id, projection)
            || !valid_estimate_predicate(predicate, projection, 32, 256)
        {
            return Ok(None);
        }
        if let Err(error) =
            self.capture_authority_dependencies(projection.context, projection.dependencies)
        {
            return if estimate_error_is_terminal(&error) {
                Err(error)
            } else {
                self.finish_predicate_estimate(None)
            };
        }
        match estimate_population(table_id, projection) {
            Some(EstimatePopulation::NoRows) => {
                return self.finish_predicate_estimate(Some((0, 0)));
            }
            Some(EstimatePopulation::AllRows) => {
                let constant_matches_all =
                    evaluate_estimate_predicate(predicate, &[], projection, 0, 256)
                        .map(|truth| truth == EstimateTruth::True);
                if let Some(matches_all) = constant_matches_all {
                    let visible_rows = self.estimate_authorized_population_at_most(
                        table_id,
                        projection,
                        maximum_rows,
                    )?;
                    if let Some(visible_rows) = visible_rows {
                        let matching_rows = if matches_all { visible_rows } else { 0 };
                        return self.finish_predicate_estimate(Some((visible_rows, matching_rows)));
                    }
                    return self.finish_predicate_estimate(None);
                }
            }
            None => return self.finish_predicate_estimate(None),
        }
        if matches!(
            estimate_population(table_id, projection),
            Some(EstimatePopulation::AllRows)
        ) && let Some(estimate) = self.estimate_indexed_predicate_from_fresh_population(
            table_id,
            projection,
            predicate,
            maximum_rows,
            candidate_limit,
        )? {
            return self.finish_predicate_estimate(Some(estimate));
        }
        let Ok(candidate_limit) = usize::try_from(maximum_rows + 1) else {
            return self.finish_predicate_estimate(None);
        };
        let mut row_ids = Vec::with_capacity(candidate_limit.min(MAX_ROW_SCAN_PAGE));
        let mut after_row_id = None;
        loop {
            let limit = candidate_limit
                .saturating_sub(row_ids.len())
                .min(MAX_ROW_SCAN_PAGE);
            if limit == 0 {
                return self.finish_predicate_estimate(None);
            }
            let (page, complete) =
                match self
                    .pinned_snapshot()?
                    .scan_row_ids_after(table_id, after_row_id, limit)
                {
                    Ok(page) => page,
                    Err(_) => return self.finish_predicate_estimate(None),
                };
            if page.is_empty() {
                if complete {
                    break;
                }
                return self.finish_predicate_estimate(None);
            }
            if page.len() < limit && !complete {
                return self.finish_predicate_estimate(None);
            }
            after_row_id = page.last().copied();
            row_ids.extend(page);
            if complete {
                break;
            }
        }
        if row_ids.len() == candidate_limit {
            return self.finish_predicate_estimate(None);
        }
        let overlay_limit = usize::try_from(maximum_rows)
            .unwrap_or(0)
            .min(candidate_limit);
        let overlay_ids = match self
            .overlay
            .bounded_row_ids_for_table(table_id, overlay_limit)
        {
            Ok(Some(ids)) => ids,
            Ok(None) => return self.finish_predicate_estimate(None),
            Err(OverlayError::Aborted) => return Err(TransactionError::Aborted),
            Err(_) => return self.finish_predicate_estimate(None),
        };
        row_ids.extend(overlay_ids);
        row_ids.sort_unstable();
        row_ids.dedup();
        if row_ids.len() as u64 > maximum_rows {
            return self.finish_predicate_estimate(None);
        }

        let mut visible_rows = 0u64;
        let mut matching_rows = 0u64;
        let mut overlay_bytes = 0usize;
        for row_id in row_ids {
            self.check_deadline()?;
            let id = RowId {
                table: table_id,
                row: row_id,
            };
            let staged_payload_bytes = match self.overlay.get_for_actor(&id, self.principal_id) {
                Ok(Some(StagedRow::Delete)) => Some(None),
                Ok(Some(StagedRow::Value(payload))) => Some(Some(payload.len())),
                Ok(None) => None,
                Err(OverlayError::Aborted) => return Err(TransactionError::Aborted),
                Err(_) => return self.finish_predicate_estimate(None),
            };
            if let Some(new_payload_bytes) = staged_payload_bytes {
                let old_payload_bytes = match self.pinned_snapshot()?.get(&row_key(id)) {
                    Ok(Some(bytes)) => match RowRecord::decode(&bytes) {
                        Ok(record) => record.payload.len(),
                        Err(_) => return self.finish_predicate_estimate(None),
                    },
                    Ok(None) => 0,
                    Err(_) => return self.finish_predicate_estimate(None),
                };
                let Some(next_overlay_bytes) = overlay_bytes
                    .checked_add(old_payload_bytes)
                    .and_then(|bytes| bytes.checked_add(new_payload_bytes.unwrap_or_default()))
                else {
                    return self.finish_predicate_estimate(None);
                };
                if next_overlay_bytes > self.max_authorized_index_range_bytes {
                    return self.finish_predicate_estimate(None);
                }
                overlay_bytes = next_overlay_bytes;
                if new_payload_bytes.is_none() {
                    continue;
                }
            }
            let cells = match self.project_row_for_estimate(id, projection) {
                Ok(Some(cells)) => cells,
                Ok(None) => return self.finish_predicate_estimate(None),
                Err(error) if estimate_error_is_terminal(&error) => return Err(error),
                Err(_) => return self.finish_predicate_estimate(None),
            };
            let Some(matches) = evaluate_estimate_predicate(predicate, &cells, projection, 0, 256)
            else {
                return self.finish_predicate_estimate(None);
            };
            visible_rows += 1;
            if matches == EstimateTruth::True {
                matching_rows += 1;
            }
        }
        self.finish_predicate_estimate(Some((visible_rows, matching_rows)))
    }

    pub(super) fn estimate_indexed_predicate_from_fresh_population(
        &mut self,
        table_id: [u8; 16],
        projection: &AuthorizedProjection<'_>,
        predicate: &crate::optimizer_ir::BoundExpr,
        maximum_rows: u64,
        candidate_limit: usize,
    ) -> Result<Option<(u64, u64)>, TransactionError> {
        let Some(CommittedPopulationRecord::Exact {
            rows: visible_rows, ..
        }) = self
            .pinned_snapshot()?
            .committed_population_record(table_id)
            .ok()
            .flatten()
        else {
            return Ok(None);
        };
        if visible_rows > maximum_rows {
            return Ok(None);
        }
        let Ok(overlay_limit) = usize::try_from(maximum_rows) else {
            return Ok(None);
        };
        let overlay_limit = overlay_limit.min(candidate_limit);
        let overlay_row_ids = match self
            .overlay
            .bounded_row_ids_for_table(table_id, overlay_limit)
        {
            Ok(Some(row_ids)) => row_ids,
            Ok(None) => return Ok(None),
            Err(OverlayError::Aborted) => return Err(TransactionError::Aborted),
            Err(_) => return Ok(None),
        };
        let Some((key_column, lower, upper)) =
            crate::optimizer_ir::index_range_bounds_from_expr(predicate)
        else {
            return Ok(None);
        };
        let schema = self.load_catalog_table_schema(table_id)?;
        if schema.schema_id != projection.schema_id
            || schema.schema_version != projection.schema_version
            || schema.column_ids != projection.column_ids
            || schema.columns != projection.columns
        {
            return Ok(None);
        }
        let indexes = self.load_catalog_indexes(&schema)?;
        let Some(index) = indexes.into_iter().find(|index| {
            index.column_ids.first() == Some(&key_column.column_id)
                && index.column_ids.iter().all(|column_id| {
                    index_component_unconditionally_readable(projection, table_id, *column_id)
                })
        }) else {
            return Ok(None);
        };
        let Some(key_ordinal) = schema
            .column_ids
            .iter()
            .position(|column_id| *column_id == key_column.column_id)
        else {
            return Ok(None);
        };
        if schema.columns[key_ordinal].scalar_type != key_column.scalar_type {
            return Ok(None);
        }
        let encode_bound = |(bound_val, inclusive): (crate::optimizer_ir::BoundExpr, bool)| {
            let value = bound_predicate_scalar(&key_column, &bound_val)?;
            let tuple = encode_sql_tuple(
                std::slice::from_ref(&schema.columns[key_ordinal]),
                &[Some(value)],
            )
            .ok()?;
            Some(crate::snapshot::IndexRangeBound { tuple, inclusive })
        };
        let lower = match lower {
            Some(bound) => match encode_bound(bound) {
                Some(bound) => Some(bound),
                None => return Ok(None),
            },
            None => None,
        };
        let upper = match upper {
            Some(bound) => match encode_bound(bound) {
                Some(bound) => Some(bound),
                None => return Ok(None),
            },
            None => None,
        };
        let mut after_key = None;
        let mut matching_rows = 0u64;
        let mut key_bytes = 0usize;
        loop {
            self.check_deadline()?;
            let page = match self.pinned_snapshot()?.scan_index_range(
                &index.index_id,
                lower.clone(),
                upper.clone(),
                after_key.as_deref(),
                crate::snapshot::MAX_ROW_SCAN_PAGE,
            ) {
                Ok(page) => page,
                Err(_) => return Ok(None),
            };
            matching_rows = matching_rows.saturating_add(page.candidates.len() as u64);
            key_bytes = key_bytes.saturating_add(
                page.candidates
                    .iter()
                    .map(|candidate| candidate.key.len())
                    .sum::<usize>(),
            );
            if matching_rows > self.max_authorized_index_range_candidates as u64
                || key_bytes > self.max_authorized_index_range_bytes
                || matching_rows > visible_rows
            {
                return Ok(None);
            }
            let Some(continuation) = page.continuation else {
                break;
            };
            after_key = Some(continuation);
        }
        let mut visible_rows = visible_rows;
        let mut overlay_bytes = 0usize;
        for row_id in overlay_row_ids {
            self.check_deadline()?;
            let id = RowId {
                table: table_id,
                row: row_id,
            };
            let new_payload = match self.overlay.get_for_actor(&id, self.principal_id) {
                Ok(Some(StagedRow::Value(payload))) => Some(payload.to_vec()),
                Ok(Some(StagedRow::Delete)) => None,
                Ok(None) => continue,
                Err(OverlayError::Aborted) => return Err(TransactionError::Aborted),
                Err(_) => return Ok(None),
            };
            let old_payload = match self.pinned_snapshot()?.get(&row_key(id)) {
                Ok(Some(bytes)) => match RowRecord::decode(&bytes) {
                    Ok(record) => Some(record.payload),
                    Err(_) => return Ok(None),
                },
                Ok(None) => None,
                Err(_) => return Ok(None),
            };
            let Some(next_overlay_bytes) = overlay_bytes
                .checked_add(old_payload.as_ref().map_or(0, Vec::len))
                .and_then(|bytes| bytes.checked_add(new_payload.as_ref().map_or(0, Vec::len)))
            else {
                return Ok(None);
            };
            if next_overlay_bytes > self.max_authorized_index_range_bytes {
                return Ok(None);
            }
            overlay_bytes = next_overlay_bytes;
            let old_matches = match old_payload.as_deref() {
                Some(payload) => match self
                    .estimate_payload_matches_predicate(id, payload, projection, predicate)
                {
                    Some(matches) => Some(matches),
                    None => return Ok(None),
                },
                None => None,
            };
            let new_matches = match new_payload.as_deref() {
                Some(payload) => match self
                    .estimate_payload_matches_predicate(id, payload, projection, predicate)
                {
                    Some(matches) => Some(matches),
                    None => return Ok(None),
                },
                None => None,
            };
            let Some(next_visible_rows) = visible_rows
                .checked_sub(if old_payload.is_some() { 1 } else { 0 })
                .and_then(|rows| rows.checked_add(if new_payload.is_some() { 1 } else { 0 }))
            else {
                return Ok(None);
            };
            let Some(next_matching_rows) = matching_rows
                .checked_sub(if old_matches == Some(true) { 1 } else { 0 })
                .and_then(|rows| rows.checked_add(if new_matches == Some(true) { 1 } else { 0 }))
            else {
                return Ok(None);
            };
            visible_rows = next_visible_rows;
            matching_rows = next_matching_rows;
        }
        if visible_rows > maximum_rows || matching_rows > visible_rows {
            return Ok(None);
        }
        Ok(Some((visible_rows, matching_rows)))
    }

    pub(super) fn estimate_payload_matches_predicate(
        &self,
        id: RowId,
        payload: &[u8],
        projection: &AuthorizedProjection<'_>,
        predicate: &crate::optimizer_ir::BoundExpr,
    ) -> Option<bool> {
        let tuple = decode_tuple(projection.columns, projection.schema_version, payload).ok()?;
        let cells = project_row(
            projection.context,
            projection.grants,
            projection.policies,
            projection.schema_id,
            id.table,
            projection.column_ids,
            &tuple.values,
        )
        .ok()??;
        evaluate_estimate_predicate(predicate, &cells, projection, 0, 256)
            .map(|truth| truth == EstimateTruth::True)
    }

    pub(super) fn finish_predicate_estimate(
        &mut self,
        estimate: Option<(u64, u64)>,
    ) -> Result<Option<(u64, u64)>, TransactionError> {
        self.check_deadline()?;
        Ok(estimate)
    }

    pub(super) fn finish_population_probe(
        &mut self,
        visible_rows: Option<u64>,
    ) -> Result<Option<u64>, TransactionError> {
        self.check_deadline()?;
        Ok(visible_rows)
    }

    /// Return summary counts for the trusted planner without returning row
    /// values. This entry point is intended for a provisioned planner channel.
    pub fn estimate_authorized_scan(
        &mut self,
        table_id: [u8; 16],
        projection: &AuthorizedProjection<'_>,
    ) -> Result<AuthorizedScanEstimate, TransactionError> {
        self.estimate_authorized_scan_with_limit(table_id, projection, DEFAULT_ESTIMATE_CANDIDATES)
    }

    pub(super) fn estimate_authorized_scan_with_limit(
        &mut self,
        table_id: [u8; 16],
        projection: &AuthorizedProjection<'_>,
        candidate_limit: usize,
    ) -> Result<AuthorizedScanEstimate, TransactionError> {
        self.check_deadline()?;
        if candidate_limit == 0
            || candidate_limit > MAX_CONFIGURABLE_ESTIMATE_CANDIDATES
            || !valid_estimate_projection(table_id, projection)
        {
            return self.finish_estimate(AuthorizedScanEstimate::Unknown);
        }
        if let Err(error) =
            self.capture_authority_dependencies(projection.context, projection.dependencies)
        {
            return if estimate_error_is_terminal(&error) {
                Err(error)
            } else {
                self.finish_estimate(AuthorizedScanEstimate::Unknown)
            };
        }
        match estimate_population(table_id, projection) {
            Some(EstimatePopulation::AllRows) => {}
            Some(EstimatePopulation::NoRows) => {
                return self.finish_estimate(AuthorizedScanEstimate::Exact(
                    AuthorizedScanSummary {
                        visible_rows: 0,
                        columns: vec![ColumnPopulation::default(); projection.columns.len()],
                    },
                ));
            }
            None => return self.finish_estimate(AuthorizedScanEstimate::Unknown),
        }
        let overlay_row_ids = match self
            .overlay
            .bounded_row_ids_for_table(table_id, candidate_limit)
        {
            Ok(Some(row_ids)) => row_ids,
            Ok(None) => return self.finish_estimate(AuthorizedScanEstimate::Unknown),
            Err(OverlayError::Aborted) => return Err(TransactionError::Aborted),
            Err(_) => return self.finish_estimate(AuthorizedScanEstimate::Unknown),
        };

        let mut populations = vec![ColumnPopulation::default(); projection.columns.len()];
        let mut visible_rows = 0u64;
        let mut scanned = 0usize;
        let mut after_row_id = None;
        loop {
            let remaining = candidate_limit.saturating_sub(scanned);
            if remaining == 0 {
                return self.finish_estimate(AuthorizedScanEstimate::Unknown);
            }
            let limit = remaining.min(MAX_ROW_SCAN_PAGE);
            let (mut snapshot_ids, snapshot_complete) = match self
                .pinned_snapshot()?
                .scan_row_ids_after(table_id, after_row_id, limit)
            {
                Ok(page) => page,
                Err(_) => return self.finish_estimate(AuthorizedScanEstimate::Unknown),
            };
            let overlay_start = after_row_id.map_or(0, |after| {
                overlay_row_ids.partition_point(|row_id| *row_id <= after)
            });
            let overlay_end = overlay_start
                .saturating_add(limit)
                .min(overlay_row_ids.len());
            let overlay_complete = overlay_end == overlay_row_ids.len();
            snapshot_ids.extend_from_slice(&overlay_row_ids[overlay_start..overlay_end]);
            snapshot_ids.sort_unstable();
            snapshot_ids.dedup();
            snapshot_ids.truncate(limit);
            let page_complete = snapshot_complete && overlay_complete && snapshot_ids.len() < limit;
            let Some(next_after_row_id) = snapshot_ids.last().copied() else {
                if page_complete {
                    return self.finish_estimate(AuthorizedScanEstimate::Exact(
                        AuthorizedScanSummary {
                            visible_rows,
                            columns: populations,
                        },
                    ));
                }
                return self.finish_estimate(AuthorizedScanEstimate::Unknown);
            };

            for row_id in &snapshot_ids {
                let id = RowId {
                    table: table_id,
                    row: *row_id,
                };
                let cells = match self.project_row_for_estimate(id, projection) {
                    Ok(Some(cells)) => cells,
                    Ok(None) => continue,
                    Err(error) if estimate_error_is_terminal(&error) => return Err(error),
                    Err(_) => return self.finish_estimate(AuthorizedScanEstimate::Unknown),
                };
                if cells.len() != populations.len() {
                    return self.finish_estimate(AuthorizedScanEstimate::Unknown);
                }
                visible_rows += 1;
                for (population, cell) in populations.iter_mut().zip(cells) {
                    match cell {
                        SanitizedCell::Value(_) => population.readable_values += 1,
                        SanitizedCell::Null => population.nulls += 1,
                        SanitizedCell::Redacted => population.redacted += 1,
                    }
                }
            }
            scanned += snapshot_ids.len();
            if page_complete {
                return self.finish_estimate(AuthorizedScanEstimate::Exact(
                    AuthorizedScanSummary {
                        visible_rows,
                        columns: populations,
                    },
                ));
            }
            if scanned >= candidate_limit {
                return self.finish_estimate(AuthorizedScanEstimate::Unknown);
            }
            after_row_id = Some(next_after_row_id);
        }
    }

    pub(super) fn finish_estimate(
        &mut self,
        estimate: AuthorizedScanEstimate,
    ) -> Result<AuthorizedScanEstimate, TransactionError> {
        self.check_deadline()?;
        Ok(estimate)
    }

    /// Compute authorized-view relation statistics (visible row count, average
    /// authorized row width in bytes, and per-column NDV, null/redacted
    /// fractions, and equi-depth histograms) strictly over visible rows and
    /// statically readable cells. Returns `Ok(None)` (`Unknown`) for
    /// value-dependent row policies or over-budget scans.
    pub fn estimate_authorized_relation_stats(
        &mut self,
        table_id: [u8; 16],
        projection: &AuthorizedProjection<'_>,
        candidate_limit: usize,
    ) -> Result<Option<crate::statistics::AuthorizedRelationStats>, TransactionError> {
        use crate::statistics::{
            AuthorizedColumnStats, AuthorizedRelationStats, EquiDepthHistogram,
            compare_scalar_values,
        };
        const MAX_SAMPLE_VALUES: usize = 256;

        self.check_deadline()?;
        if candidate_limit == 0
            || candidate_limit > MAX_CONFIGURABLE_ESTIMATE_CANDIDATES
            || !valid_estimate_projection(table_id, projection)
        {
            return Ok(None);
        }
        if let Err(error) =
            self.capture_authority_dependencies(projection.context, projection.dependencies)
        {
            return if estimate_error_is_terminal(&error) {
                Err(error)
            } else {
                Ok(None)
            };
        }
        match estimate_population(table_id, projection) {
            Some(EstimatePopulation::AllRows) => {}
            Some(EstimatePopulation::NoRows) => {
                let columns = projection
                    .column_ids
                    .iter()
                    .map(|&column_id| AuthorizedColumnStats {
                        column_id,
                        ndv: Some(0),
                        null_fraction_ppm: Some(0),
                        redacted_fraction_ppm: 0,
                        histogram: None,
                    })
                    .collect();
                return Ok(Some(AuthorizedRelationStats {
                    visible_rows: 0,
                    is_exact: true,
                    avg_authorized_row_bytes: 0,
                    columns,
                }));
            }
            None => return Ok(None),
        }

        let overlay_row_ids = match self
            .overlay
            .bounded_row_ids_for_table(table_id, candidate_limit)
        {
            Ok(Some(row_ids)) => row_ids,
            Ok(None) => return Ok(None),
            Err(OverlayError::Aborted) => return Err(TransactionError::Aborted),
            Err(_) => return Ok(None),
        };

        let col_count = projection.column_ids.len();
        let mut visible_rows = 0u64;
        let mut total_bytes = 0u64;
        let mut nulls = vec![0u64; col_count];
        let mut redacted = vec![0u64; col_count];
        let mut samples: Vec<Vec<ScalarValue>> = vec![Vec::new(); col_count];
        let mut scanned = 0usize;
        let mut after_row_id = None;
        let mut is_exact = false;

        loop {
            let remaining = candidate_limit.saturating_sub(scanned);
            if remaining == 0 {
                break;
            }
            let limit = remaining.min(MAX_ROW_SCAN_PAGE);
            let (mut snapshot_ids, snapshot_complete) = match self
                .pinned_snapshot()?
                .scan_row_ids_after(table_id, after_row_id, limit)
            {
                Ok(page) => page,
                Err(_) => return Ok(None),
            };
            let overlay_start = after_row_id.map_or(0, |after| {
                overlay_row_ids.partition_point(|row_id| *row_id <= after)
            });
            let overlay_end = overlay_start
                .saturating_add(limit)
                .min(overlay_row_ids.len());
            let overlay_complete = overlay_end == overlay_row_ids.len();
            snapshot_ids.extend_from_slice(&overlay_row_ids[overlay_start..overlay_end]);
            snapshot_ids.sort_unstable();
            snapshot_ids.dedup();
            snapshot_ids.truncate(limit);
            let page_complete = snapshot_complete && overlay_complete && snapshot_ids.len() < limit;
            let Some(next_after_row_id) = snapshot_ids.last().copied() else {
                if page_complete {
                    is_exact = true;
                }
                break;
            };

            for row_id in &snapshot_ids {
                let id = RowId {
                    table: table_id,
                    row: *row_id,
                };
                let cells = match self.project_row_for_estimate(id, projection) {
                    Ok(Some(cells)) => cells,
                    Ok(None) => continue,
                    Err(error) if estimate_error_is_terminal(&error) => return Err(error),
                    Err(_) => return Ok(None),
                };
                if cells.len() != col_count {
                    return Ok(None);
                }
                visible_rows += 1;
                for (i, cell) in cells.into_iter().enumerate() {
                    match cell {
                        SanitizedCell::Value(val) => {
                            total_bytes = total_bytes.saturating_add(scalar_value_byte_width(&val));
                            if samples[i].len() < MAX_SAMPLE_VALUES {
                                samples[i].push(val);
                            } else {
                                // Deterministic stride replacement for bounded reservoir sampling.
                                let slot = (visible_rows as usize).wrapping_mul(2654435761)
                                    % (visible_rows as usize);
                                if slot < MAX_SAMPLE_VALUES {
                                    samples[i][slot] = val;
                                }
                            }
                        }
                        SanitizedCell::Null => nulls[i] += 1,
                        SanitizedCell::Redacted => redacted[i] += 1,
                    }
                }
            }
            scanned += snapshot_ids.len();
            if page_complete {
                is_exact = true;
                break;
            }
            if scanned >= candidate_limit {
                break;
            }
            after_row_id = Some(next_after_row_id);
        }

        if !is_exact {
            return Ok(None);
        }

        let avg_authorized_row_bytes = total_bytes.checked_div(visible_rows).map_or(0, |avg| {
            u32::try_from(avg.min(u64::from(u32::MAX))).unwrap_or(u32::MAX)
        });

        let mut columns = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let redacted_fraction_ppm = (redacted[i] * 1_000_000)
                .checked_div(visible_rows)
                .unwrap_or(0) as u32;
            if redacted[i] > 0 {
                columns.push(AuthorizedColumnStats {
                    column_id: projection.column_ids[i],
                    ndv: None,
                    null_fraction_ppm: None,
                    redacted_fraction_ppm,
                    histogram: None,
                });
                continue;
            }
            let null_fraction_ppm = Some(
                (nulls[i] * 1_000_000)
                    .checked_div(visible_rows)
                    .unwrap_or(0) as u32,
            );
            let mut vals = std::mem::take(&mut samples[i]);
            let sample_rows = vals.len() as u64;
            vals.sort_by(|a, b| compare_scalar_values(a, b).unwrap_or(std::cmp::Ordering::Equal));
            let mut distinct_vals = vals.clone();
            distinct_vals.dedup();
            let ndv = Some(distinct_vals.len() as u64);
            let histogram = if vals.len() >= 2 {
                let num_buckets = EquiDepthHistogram::MAX_BUCKETS.min(vals.len() - 1);
                let mut boundaries = Vec::with_capacity(num_buckets + 1);
                for b in 0..=num_buckets {
                    let idx = (b * (vals.len() - 1)) / num_buckets;
                    boundaries.push(vals[idx].clone());
                }
                Some(EquiDepthHistogram {
                    sample_rows,
                    boundaries,
                })
            } else {
                None
            };
            columns.push(AuthorizedColumnStats {
                column_id: projection.column_ids[i],
                ndv,
                null_fraction_ppm,
                redacted_fraction_ppm: 0,
                histogram,
            });
        }

        self.check_deadline()?;
        Ok(Some(AuthorizedRelationStats {
            visible_rows,
            is_exact,
            avg_authorized_row_bytes,
            columns,
        }))
    }

    pub(super) fn project_row_for_estimate(
        &mut self,
        id: RowId,
        projection: &AuthorizedProjection<'_>,
    ) -> Result<Option<Vec<SanitizedCell>>, TransactionError> {
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
    }
}

fn scalar_value_byte_width(val: &ScalarValue) -> u64 {
    match val {
        ScalarValue::Boolean(_) => 1,
        ScalarValue::SmallInt(_) => 2,
        ScalarValue::Integer(_) | ScalarValue::Date(_) => 4,
        ScalarValue::BigInt(_) | ScalarValue::TimestampTz(_) => 8,
        ScalarValue::Decimal(_) | ScalarValue::Uuid(_) => 16,
        ScalarValue::Text(s) | ScalarValue::Varchar(s) => s.len() as u64,
        ScalarValue::Bytea(b) => b.len() as u64,
    }
}
pub(super) fn consume_planner_estimate_budget(requests: &mut usize, maximum: usize) -> bool {
    if *requests >= maximum {
        return false;
    }
    *requests += 1;
    true
}

pub(super) fn cached_exact_estimate<K, V, E>(
    cache: &mut HashMap<K, (u64, V)>,
    key: K,
    write_generation: u64,
    estimate: impl FnOnce() -> Result<Option<V>, E>,
) -> Result<Option<V>, E>
where
    K: Eq + std::hash::Hash,
    V: Clone,
{
    cache.retain(|_, (generation, _)| *generation == write_generation);
    if let Some((_, exact_value)) = cache.get(&key) {
        return Ok(Some(exact_value.clone()));
    }
    let exact_value = estimate()?;
    if let Some(exact_value) = &exact_value {
        cache.insert(key, (write_generation, exact_value.clone()));
    }
    Ok(exact_value)
}

pub(super) fn bound_predicate_scalar(
    column: &crate::optimizer_ir::BoundColumn,
    value: &crate::optimizer_ir::BoundExpr,
) -> Option<ScalarValue> {
    use crate::optimizer_ir::BoundExpr;

    match (column.scalar_type, value) {
        (ScalarType::Boolean, BoundExpr::Boolean(value)) => Some(ScalarValue::Boolean(*value)),
        (ScalarType::SmallInt, BoundExpr::Integer(value)) => {
            i16::try_from(*value).ok().map(ScalarValue::SmallInt)
        }
        (ScalarType::Integer, BoundExpr::Integer(value)) => {
            i32::try_from(*value).ok().map(ScalarValue::Integer)
        }
        (ScalarType::BigInt, BoundExpr::Integer(value)) => Some(ScalarValue::BigInt(*value)),
        (ScalarType::Decimal { precision, .. }, BoundExpr::Decimal(value))
            if crate::tuple::decimal_coefficient_is_valid(*value, precision) =>
        {
            Some(ScalarValue::Decimal(*value))
        }
        (ScalarType::Text, BoundExpr::Text(value)) => Some(ScalarValue::Text(value.clone())),
        (ScalarType::Varchar { .. }, BoundExpr::Text(value)) => {
            Some(ScalarValue::Varchar(value.clone()))
        }
        (ScalarType::Bytea, BoundExpr::Bytea(value)) => Some(ScalarValue::Bytea(value.clone())),
        (ScalarType::Uuid, BoundExpr::Uuid(value)) => Some(ScalarValue::Uuid(*value)),
        (ScalarType::Date, BoundExpr::Date(value)) => Some(ScalarValue::Date(*value)),
        (ScalarType::TimestampTz, BoundExpr::TimestampTz(value)) => {
            Some(ScalarValue::TimestampTz(*value))
        }
        _ => None,
    }
}

pub(super) fn index_component_unconditionally_readable(
    projection: &AuthorizedProjection<'_>,
    table_id: [u8; 16],
    column_id: [u8; 16],
) -> bool {
    let target = AccessTarget::Column {
        table_id,
        column_id,
    };
    if !has_permission(
        projection.context,
        projection.grants,
        Operation::Select,
        target,
    ) {
        return false;
    }
    let mut has_allow = false;
    for policy in projection.policies.iter().filter(|policy| {
        policy.operation == Operation::Select
            && policy.target == target
            && policy.subjects.iter().any(|subject| match subject {
                Subject::Principal(principal_id) => {
                    *principal_id == projection.context.principal_id
                }
                Subject::Role(role_id) => projection.context.role_ids.contains(role_id),
            })
    }) {
        if !matches!(
            policy.using,
            Some(Expr::Literal(Some(ScalarValue::Boolean(true))))
        ) || policy.with_check.is_some()
        {
            return false;
        }
        has_allow |= policy.effect == Effect::Allow;
    }
    has_allow
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum EstimateTruth {
    True,
    False,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum EstimateValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Decimal(i128),
    Text(String),
    Bytea(Vec<u8>),
    Uuid([u8; 16]),
    Date(i32),
    TimestampTz(i64),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum EstimateValueKind {
    Null,
    Boolean,
    Integer,
    Decimal,
    Text,
    Bytea,
    Uuid,
    Date,
    TimestampTz,
}

pub(super) fn valid_estimate_predicate(
    predicate: &crate::optimizer_ir::BoundExpr,
    projection: &AuthorizedProjection<'_>,
    maximum_depth: usize,
    maximum_nodes: usize,
) -> bool {
    use crate::optimizer_ir::BoundExpr;

    pub(super) fn visit(
        predicate: &BoundExpr,
        projection: &AuthorizedProjection<'_>,
        depth: usize,
        remaining_nodes: &mut usize,
        maximum_depth: usize,
    ) -> bool {
        if depth > maximum_depth || *remaining_nodes == 0 {
            return false;
        }
        *remaining_nodes -= 1;
        match predicate {
            BoundExpr::Null | BoundExpr::Boolean(_) | BoundExpr::Integer(_) => true,
            BoundExpr::Date(value) => crate::tuple::date_day_count_is_valid(*value),
            BoundExpr::Uuid(_) => true,
            BoundExpr::Bytea(value) => value.len() <= 4096,
            BoundExpr::TimestampTz(value) => crate::tuple::timestamp_micros_is_valid(*value),
            BoundExpr::Decimal(_) => true,
            BoundExpr::Text(value) => value.len() <= 4096,
            BoundExpr::Parameter(_) => false,
            BoundExpr::Column(column_ref) => usize::try_from(column_ref.column.ordinal)
                .ok()
                .and_then(|ordinal| {
                    projection.column_ids.get(ordinal).map(|column_id| {
                        *column_id == column_ref.column.column_id
                            && projection.columns[ordinal].scalar_type
                                == column_ref.column.scalar_type
                            && projection.columns[ordinal].nullable == column_ref.column.nullable
                    })
                })
                .unwrap_or(false),
            BoundExpr::Compare { left, right, .. } => {
                let decimal_context_valid = match (left.as_ref(), right.as_ref()) {
                    (BoundExpr::Decimal(value), BoundExpr::Column(column_ref))
                    | (BoundExpr::Column(column_ref), BoundExpr::Decimal(value)) => {
                        let ordinal = match usize::try_from(column_ref.column.ordinal) {
                            Ok(ordinal) => ordinal,
                            Err(_) => return false,
                        };
                        matches!(projection.columns.get(ordinal).map(|column| column.scalar_type),
                            Some(crate::tuple::ScalarType::Decimal { precision, .. })
                                if crate::tuple::decimal_coefficient_is_valid(*value, precision))
                    }
                    (BoundExpr::Decimal(_), BoundExpr::Null)
                    | (BoundExpr::Null, BoundExpr::Decimal(_)) => true,
                    (BoundExpr::Decimal(_), _) | (_, BoundExpr::Decimal(_)) => false,
                    _ => true,
                };
                visit(left, projection, depth + 1, remaining_nodes, maximum_depth)
                    && visit(right, projection, depth + 1, remaining_nodes, maximum_depth)
                    && decimal_context_valid
            }
            BoundExpr::And(children) | BoundExpr::Or(children) => {
                !children.is_empty()
                    && children.len() <= 16
                    && children.iter().all(|child| {
                        visit(child, projection, depth + 1, remaining_nodes, maximum_depth)
                    })
            }
            BoundExpr::Not(child) | BoundExpr::IsNull { expr: child, .. } => {
                visit(child, projection, depth + 1, remaining_nodes, maximum_depth)
            }
            BoundExpr::InList { expr, items, .. } => {
                !items.is_empty()
                    && items.len() <= 64
                    && visit(expr, projection, depth + 1, remaining_nodes, maximum_depth)
                    && items.iter().all(|item| {
                        visit(item, projection, depth + 1, remaining_nodes, maximum_depth)
                    })
            }
        }
    }

    let mut remaining_nodes = maximum_nodes;
    visit(
        predicate,
        projection,
        0,
        &mut remaining_nodes,
        maximum_depth,
    ) && estimate_predicate_kind(predicate, projection)
        .is_some_and(|kind| matches!(kind, EstimateValueKind::Boolean | EstimateValueKind::Null))
}

pub(super) fn estimate_predicate_kind(
    predicate: &crate::optimizer_ir::BoundExpr,
    projection: &AuthorizedProjection<'_>,
) -> Option<EstimateValueKind> {
    use crate::optimizer_ir::BoundExpr;
    use crate::tuple::ScalarType;
    match predicate {
        BoundExpr::Null => Some(EstimateValueKind::Null),
        BoundExpr::Boolean(_) => Some(EstimateValueKind::Boolean),
        BoundExpr::Integer(_) => Some(EstimateValueKind::Integer),
        BoundExpr::Text(_) => Some(EstimateValueKind::Text),
        BoundExpr::Date(_) => Some(EstimateValueKind::Date),
        BoundExpr::Uuid(_) => Some(EstimateValueKind::Uuid),
        BoundExpr::Bytea(value) if value.len() <= 4096 => Some(EstimateValueKind::Bytea),
        BoundExpr::Bytea(_) => None,
        BoundExpr::TimestampTz(_) => Some(EstimateValueKind::TimestampTz),
        BoundExpr::Decimal(_) => Some(EstimateValueKind::Decimal),
        BoundExpr::Parameter(_) => None,
        BoundExpr::Column(column_ref) => {
            let ordinal = usize::try_from(column_ref.column.ordinal).ok()?;
            let column = &projection.columns[ordinal];
            match column.scalar_type {
                ScalarType::Boolean => Some(EstimateValueKind::Boolean),
                ScalarType::SmallInt | ScalarType::Integer | ScalarType::BigInt => {
                    Some(EstimateValueKind::Integer)
                }
                ScalarType::Decimal { .. } => Some(EstimateValueKind::Decimal),
                ScalarType::Text | ScalarType::Varchar { .. } => Some(EstimateValueKind::Text),
                ScalarType::Bytea => Some(EstimateValueKind::Bytea),
                ScalarType::Uuid => Some(EstimateValueKind::Uuid),
                ScalarType::Date => Some(EstimateValueKind::Date),
                ScalarType::TimestampTz => Some(EstimateValueKind::TimestampTz),
            }
        }
        BoundExpr::Compare { left, right, .. } => {
            let left_kind = estimate_predicate_kind(left, projection)?;
            let right_kind = estimate_predicate_kind(right, projection)?;
            let column_types_match = match (left.as_ref(), right.as_ref()) {
                (BoundExpr::Column(left_column), BoundExpr::Column(right_column)) => {
                    left_column.column.scalar_type == right_column.column.scalar_type
                }
                _ => true,
            };
            (column_types_match
                && (left_kind == right_kind
                    || left_kind == EstimateValueKind::Null
                    || right_kind == EstimateValueKind::Null))
                .then_some(EstimateValueKind::Boolean)
        }
        BoundExpr::And(children) | BoundExpr::Or(children) => children
            .iter()
            .all(|child| {
                estimate_predicate_kind(child, projection).is_some_and(|kind| {
                    matches!(kind, EstimateValueKind::Boolean | EstimateValueKind::Null)
                })
            })
            .then_some(EstimateValueKind::Boolean),
        BoundExpr::Not(child) => estimate_predicate_kind(child, projection)
            .is_some_and(|kind| {
                matches!(kind, EstimateValueKind::Boolean | EstimateValueKind::Null)
            })
            .then_some(EstimateValueKind::Boolean),
        BoundExpr::IsNull { expr: child, .. } => estimate_predicate_kind(child, projection)
            .is_some()
            .then_some(EstimateValueKind::Boolean),
        BoundExpr::InList { expr, items, .. } => {
            let target_kind = estimate_predicate_kind(expr, projection)?;
            items
                .iter()
                .all(|item| {
                    estimate_predicate_kind(item, projection).is_some_and(|ik| {
                        ik == target_kind
                            || ik == EstimateValueKind::Null
                            || target_kind == EstimateValueKind::Null
                    })
                })
                .then_some(EstimateValueKind::Boolean)
        }
    }
}

pub(super) fn evaluate_estimate_predicate(
    predicate: &crate::optimizer_ir::BoundExpr,
    cells: &[SanitizedCell],
    projection: &AuthorizedProjection<'_>,
    depth: usize,
    remaining_nodes: usize,
) -> Option<EstimateTruth> {
    use crate::optimizer_ir::BoundExpr;

    pub(super) fn value(
        predicate: &BoundExpr,
        cells: &[SanitizedCell],
        projection: &AuthorizedProjection<'_>,
        depth: usize,
        remaining_nodes: usize,
    ) -> Option<EstimateValue> {
        if depth > 32 || remaining_nodes == 0 {
            return None;
        }
        let next_budget = remaining_nodes - 1;
        match predicate {
            BoundExpr::Null => Some(EstimateValue::Null),
            BoundExpr::Boolean(value) => Some(EstimateValue::Boolean(*value)),
            BoundExpr::Integer(value) => Some(EstimateValue::Integer(*value)),
            BoundExpr::Date(value) => Some(EstimateValue::Date(*value)),
            BoundExpr::Uuid(value) => Some(EstimateValue::Uuid(*value)),
            BoundExpr::Bytea(value) if value.len() <= 4096 => {
                Some(EstimateValue::Bytea(value.clone()))
            }
            BoundExpr::Bytea(_) => None,
            BoundExpr::TimestampTz(value) => Some(EstimateValue::TimestampTz(*value)),
            BoundExpr::Decimal(value) => Some(EstimateValue::Decimal(*value)),
            BoundExpr::Text(value) if value.len() <= 4096 => {
                Some(EstimateValue::Text(value.clone()))
            }
            BoundExpr::Text(_) | BoundExpr::Parameter(_) => None,
            BoundExpr::Column(column_ref) => {
                let column = &column_ref.column;
                let ordinal = usize::try_from(column.ordinal).ok()?;
                if projection.column_ids.get(ordinal) != Some(&column.column_id)
                    || projection.columns.get(ordinal)?.scalar_type != column.scalar_type
                    || projection.columns.get(ordinal)?.nullable != column.nullable
                {
                    return None;
                }
                match cells.get(ordinal)? {
                    SanitizedCell::Value(value) => scalar_estimate_value(value),
                    SanitizedCell::Null => Some(EstimateValue::Null),
                    SanitizedCell::Redacted => None,
                }
            }
            BoundExpr::Compare { op, left, right } => {
                let left = value(left, cells, projection, depth + 1, next_budget)?;
                let right = value(right, cells, projection, depth + 1, next_budget)?;
                match compare_estimate_values(&left, &right, *op)? {
                    Some(value) => Some(EstimateValue::Boolean(value)),
                    None => Some(EstimateValue::Null),
                }
            }
            BoundExpr::And(children) | BoundExpr::Or(children) => {
                if children.is_empty() || children.len() > 16 {
                    return None;
                }
                let is_and = matches!(predicate, BoundExpr::And(_));
                let mut truth = if is_and {
                    EstimateTruth::True
                } else {
                    EstimateTruth::False
                };
                let mut budget = next_budget;
                for child in children {
                    budget = budget.checked_sub(1)?;
                    let child_value = value(child, cells, projection, depth + 1, budget)?;
                    let child_truth = match child_value {
                        EstimateValue::Boolean(true) => EstimateTruth::True,
                        EstimateValue::Boolean(false) => EstimateTruth::False,
                        EstimateValue::Null => EstimateTruth::Unknown,
                        EstimateValue::Integer(_)
                        | EstimateValue::Decimal(_)
                        | EstimateValue::Text(_)
                        | EstimateValue::Bytea(_)
                        | EstimateValue::Uuid(_)
                        | EstimateValue::Date(_)
                        | EstimateValue::TimestampTz(_) => return None,
                    };
                    truth = if is_and {
                        estimate_and(truth, child_truth)
                    } else {
                        estimate_or(truth, child_truth)
                    };
                }
                match truth {
                    EstimateTruth::True => Some(EstimateValue::Boolean(true)),
                    EstimateTruth::False => Some(EstimateValue::Boolean(false)),
                    EstimateTruth::Unknown => Some(EstimateValue::Null),
                }
            }
            BoundExpr::Not(child) => {
                let child = value(child, cells, projection, depth + 1, next_budget)?;
                match child {
                    EstimateValue::Boolean(value) => Some(EstimateValue::Boolean(!value)),
                    EstimateValue::Null => Some(EstimateValue::Null),
                    EstimateValue::Integer(_)
                    | EstimateValue::Decimal(_)
                    | EstimateValue::Text(_)
                    | EstimateValue::Bytea(_)
                    | EstimateValue::Uuid(_)
                    | EstimateValue::Date(_)
                    | EstimateValue::TimestampTz(_) => None,
                }
            }
            BoundExpr::IsNull {
                expr: child,
                negated,
            } => {
                let child = value(child, cells, projection, depth + 1, next_budget)?;
                let is_null = matches!(child, EstimateValue::Null);
                Some(EstimateValue::Boolean(if *negated {
                    !is_null
                } else {
                    is_null
                }))
            }
            BoundExpr::InList {
                expr,
                items,
                negated,
            } => {
                let lhs = value(expr, cells, projection, depth + 1, next_budget)?;
                let mut any_match = false;
                let mut any_null = false;
                for item in items {
                    let rhs = value(item, cells, projection, depth + 1, next_budget)?;
                    match compare_estimate_values(
                        &lhs,
                        &rhs,
                        crate::optimizer_ir::Comparison::Equal,
                    )? {
                        Some(true) => {
                            any_match = true;
                            break;
                        }
                        Some(false) => {}
                        None => any_null = true,
                    }
                }
                if any_match {
                    Some(EstimateValue::Boolean(!negated))
                } else if any_null {
                    Some(EstimateValue::Null)
                } else {
                    Some(EstimateValue::Boolean(*negated))
                }
            }
        }
    }

    if remaining_nodes == 0 || depth != 0 {
        return None;
    }
    match value(predicate, cells, projection, depth, remaining_nodes)? {
        EstimateValue::Boolean(true) => Some(EstimateTruth::True),
        EstimateValue::Boolean(false) => Some(EstimateTruth::False),
        EstimateValue::Null => Some(EstimateTruth::Unknown),
        EstimateValue::Integer(_)
        | EstimateValue::Decimal(_)
        | EstimateValue::Text(_)
        | EstimateValue::Bytea(_)
        | EstimateValue::Uuid(_)
        | EstimateValue::Date(_)
        | EstimateValue::TimestampTz(_) => None,
    }
}

pub(super) fn scalar_estimate_value(value: &crate::tuple::ScalarValue) -> Option<EstimateValue> {
    use crate::tuple::ScalarValue;
    match value {
        ScalarValue::Boolean(value) => Some(EstimateValue::Boolean(*value)),
        ScalarValue::SmallInt(value) => Some(EstimateValue::Integer(i64::from(*value))),
        ScalarValue::Integer(value) => Some(EstimateValue::Integer(i64::from(*value))),
        ScalarValue::BigInt(value) => Some(EstimateValue::Integer(*value)),
        ScalarValue::Decimal(value) => Some(EstimateValue::Decimal(*value)),
        ScalarValue::Text(value) | ScalarValue::Varchar(value) if value.len() <= 4096 => {
            Some(EstimateValue::Text(value.clone()))
        }
        ScalarValue::Bytea(value) if value.len() <= 4096 => {
            Some(EstimateValue::Bytea(value.clone()))
        }
        ScalarValue::Uuid(value) => Some(EstimateValue::Uuid(*value)),
        ScalarValue::Date(value) => Some(EstimateValue::Date(*value)),
        ScalarValue::TimestampTz(value) => Some(EstimateValue::TimestampTz(*value)),
        ScalarValue::Text(_) | ScalarValue::Varchar(_) | ScalarValue::Bytea(_) => None,
    }
}

pub(super) fn compare_estimate_values(
    left: &EstimateValue,
    right: &EstimateValue,
    operator: crate::optimizer_ir::Comparison,
) -> Option<Option<bool>> {
    use crate::optimizer_ir::Comparison;
    let ordering = match (left, right) {
        (EstimateValue::Null, _) | (_, EstimateValue::Null) => return Some(None),
        (EstimateValue::Boolean(left), EstimateValue::Boolean(right)) => left.cmp(right),
        (EstimateValue::Integer(left), EstimateValue::Integer(right)) => left.cmp(right),
        (EstimateValue::Decimal(left), EstimateValue::Decimal(right)) => left.cmp(right),
        (EstimateValue::Text(left), EstimateValue::Text(right)) => left.cmp(right),
        (EstimateValue::Bytea(left), EstimateValue::Bytea(right)) => left.cmp(right),
        (EstimateValue::Uuid(left), EstimateValue::Uuid(right)) => left.cmp(right),
        (EstimateValue::Date(left), EstimateValue::Date(right)) => left.cmp(right),
        (EstimateValue::TimestampTz(left), EstimateValue::TimestampTz(right)) => left.cmp(right),
        _ => return None,
    };
    let result = match operator {
        Comparison::Equal => ordering.is_eq(),
        Comparison::NotEqual => !ordering.is_eq(),
        Comparison::Less => ordering.is_lt(),
        Comparison::LessOrEqual => !ordering.is_gt(),
        Comparison::Greater => ordering.is_gt(),
        Comparison::GreaterOrEqual => !ordering.is_lt(),
    };
    Some(Some(result))
}

pub(super) fn estimate_and(left: EstimateTruth, right: EstimateTruth) -> EstimateTruth {
    match (left, right) {
        (EstimateTruth::False, _) | (_, EstimateTruth::False) => EstimateTruth::False,
        (EstimateTruth::True, EstimateTruth::True) => EstimateTruth::True,
        _ => EstimateTruth::Unknown,
    }
}

pub(super) fn estimate_or(left: EstimateTruth, right: EstimateTruth) -> EstimateTruth {
    match (left, right) {
        (EstimateTruth::True, _) | (_, EstimateTruth::True) => EstimateTruth::True,
        (EstimateTruth::False, EstimateTruth::False) => EstimateTruth::False,
        _ => EstimateTruth::Unknown,
    }
}

pub(super) fn valid_estimate_projection(
    table_id: [u8; 16],
    projection: &AuthorizedProjection<'_>,
) -> bool {
    table_id != [0; 16]
        && !projection.columns.is_empty()
        && projection.columns.len() == projection.column_ids.len()
        && projection.columns.len() <= crate::tuple::MAX_COLUMNS
        && projection.schema_version != 0
        && projection.schema_id != [0; 16]
        && projection.column_ids.iter().all(|id| id != &[0; 16])
        && projection
            .column_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>()
            .len()
            == projection.column_ids.len()
        && projection.grants.len() <= crate::authorization::MAX_AUTHORIZATION_INPUTS
        && projection.policies.len() <= crate::authorization::MAX_AUTHORIZATION_INPUTS
        && projection.checks.len() <= crate::authorization::MAX_AUTHORIZATION_INPUTS
        && projection.policy_driving_columns.len() <= crate::authorization::MAX_AUTHORIZATION_INPUTS
        && projection.dependencies.len() <= crate::authorization::MAX_AUTHORIZATION_INPUTS
        && projection.context.principal_id != [0; 16]
        && projection.context.role_ids.len() <= crate::authorization::MAX_AUTHORIZATION_INPUTS
        && projection.context.scope.len() <= crate::authorization::MAX_AUTHORIZATION_INPUTS
        && projection
            .context
            .role_ids
            .iter()
            .all(|role_id| role_id != &[0; 16])
        && projection
            .policies
            .iter()
            .all(|policy| policy.subjects.len() <= crate::authorization::MAX_POLICY_SUBJECTS)
}

pub(super) fn join_key_ordinals(
    table_id: [u8; 16],
    projection: &AuthorizedProjection<'_>,
    column_ids: &[[u8; 16]],
) -> Option<Vec<usize>> {
    if table_id == [0; 16]
        || column_ids.is_empty()
        || column_ids.len() > 8
        || column_ids.contains(&[0; 16])
        || column_ids.iter().copied().collect::<HashSet<_>>().len() != column_ids.len()
    {
        return None;
    }
    column_ids
        .iter()
        .map(|column_id| projection.column_ids.iter().position(|id| id == column_id))
        .collect()
}

pub(super) fn estimate_join_types_compatible(left: ScalarType, right: ScalarType) -> bool {
    match (left, right) {
        (ScalarType::Decimal { scale: left, .. }, ScalarType::Decimal { scale: right, .. }) => {
            left == right
        }
        (ScalarType::Varchar { .. }, ScalarType::Varchar { .. }) => true,
        (left, right) => left == right,
    }
}

pub(super) fn collect_expression_fields(expression: &Expr, ordinals: &mut HashSet<usize>) {
    match expression {
        Expr::Field(ordinal) => {
            ordinals.insert(*ordinal);
        }
        Expr::Equal(left, right)
        | Expr::NotEqual(left, right)
        | Expr::Less(left, right)
        | Expr::LessEqual(left, right)
        | Expr::Greater(left, right)
        | Expr::GreaterEqual(left, right)
        | Expr::Add(left, right)
        | Expr::Subtract(left, right)
        | Expr::Multiply(left, right)
        | Expr::Modulo(left, right)
        | Expr::Divide(left, right) => {
            collect_expression_fields(left, ordinals);
            collect_expression_fields(right, ordinals);
        }
        Expr::And(expressions) | Expr::Or(expressions) => {
            for expression in expressions {
                collect_expression_fields(expression, ordinals);
            }
        }
        Expr::Not(expression) | Expr::IsNull(expression) | Expr::Negate(expression) => {
            collect_expression_fields(expression, ordinals);
        }
        Expr::Literal(_) | Expr::CurrentPrincipalId | Expr::HasRole(_) => {}
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum EstimatePopulation {
    AllRows,
    NoRows,
}

/// Exact counts are released only when row inclusion is independent of row
/// values. Constant policy outcomes can prove either the full or empty view;
/// value-dependent policies remain unknown so a work-limit result cannot
/// reveal how many candidates failed visibility.
pub(super) fn estimate_population(
    table_id: [u8; 16],
    projection: &AuthorizedProjection<'_>,
) -> Option<EstimatePopulation> {
    let context = projection.context;
    if !has_permission(
        context,
        projection.grants,
        Operation::Usage,
        AccessTarget::Schema(projection.schema_id),
    ) || !context
        .scope
        .contains(&(Operation::Select, AccessTarget::Table(table_id)))
        || !projection.column_ids.iter().any(|&column_id| {
            has_permission(
                context,
                projection.grants,
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id,
                },
            )
        })
    {
        return None;
    }

    let mut has_allow = false;
    let mut has_applicable_policy = false;
    let mut denied_all_rows = false;
    for policy in projection.policies.iter().filter(|policy| {
        policy.operation == Operation::Select
            && policy.target == AccessTarget::Table(table_id)
            && policy.subjects.iter().any(|subject| match subject {
                Subject::Principal(principal_id) => *principal_id == context.principal_id,
                Subject::Role(role_id) => context.role_ids.contains(role_id),
            })
    }) {
        has_applicable_policy = true;
        if policy.with_check.is_some() {
            return None;
        }
        match policy.using.as_ref() {
            Some(Expr::Literal(Some(ScalarValue::Boolean(true)))) => {
                has_allow |= policy.effect == Effect::Allow;
            }
            Some(Expr::Literal(Some(ScalarValue::Boolean(false)))) => {
                if policy.effect == Effect::Restrict {
                    denied_all_rows = true;
                }
            }
            _ => return None,
        }
    }
    if !has_applicable_policy {
        return None;
    }
    if denied_all_rows {
        Some(EstimatePopulation::NoRows)
    } else if has_allow {
        Some(EstimatePopulation::AllRows)
    } else {
        Some(EstimatePopulation::NoRows)
    }
}

pub(super) fn estimate_error_is_terminal(error: &TransactionError) -> bool {
    matches!(
        error,
        TransactionError::Expired
            | TransactionError::CancellationRequested
            | TransactionError::Aborted
            | TransactionError::Commit(CommitError::InvocationCancelled)
    )
}
