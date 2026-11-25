use super::scans_and_indexes::*;
use super::*;
use crate::authorization::{
    AccessTarget, AuthorizationError, AuthorizedProjection, Expr, Operation, authorize,
    check_constraint_passes, has_permission, materialize_default, row_visible,
};
use crate::commit::CommitError;
use crate::expression::{ExpressionPurpose, validate_expression_binding};
use crate::overlay::{ResolvedRow, RowId, StagedRow};
use crate::row_record::RowRecord;
use crate::tuple::{ColumnSchema, FieldPatch, apply_patch, decode_tuple, encode_tuple};
use std::collections::HashSet;

impl<'db> InvocationTransaction<'db> {
    /// Materialize defaults and generated UUID identities from the pinned
    /// catalog. Reading each DEFAULT expression adds its exact bytes to the
    /// final-commit catalog guard set; generated values are collision-checked
    /// against the serialized final table state during commit.
    pub fn stage_insert_authorized_from_catalog_defaults(
        &mut self,
        id: RowId,
        projection: &AuthorizedProjection<'_>,
        inputs: &[InsertInput],
        schema: &CatalogTableSchema,
    ) -> Result<(), TransactionError> {
        let result = (|| {
            if schema.table_id != id.table
                || schema.schema_id != projection.schema_id
                || schema.schema_version != projection.schema_version
                || schema.columns != projection.columns
                || schema.column_ids != projection.column_ids
                || inputs.len() != schema.columns.len()
                || schema.default_expression_ids.len() != schema.columns.len()
                || schema.column_type_ids.len() != schema.columns.len()
                || schema.column_default_masks.len() != schema.columns.len()
                || schema.generation_modes.len() != schema.columns.len()
                || inputs.len() > crate::tuple::MAX_COLUMNS
            {
                return Err(TransactionError::InvalidContext);
            }
            let generated_columns = schema
                .generation_modes
                .iter()
                .enumerate()
                .filter_map(|(ordinal, mode)| (*mode == 1).then_some(schema.column_ids[ordinal]))
                .collect::<Vec<_>>();
            let mut materialized_inputs = inputs.to_vec();
            for (ordinal, mode) in schema.generation_modes.iter().enumerate() {
                if *mode == 1 {
                    if inputs[ordinal] != InsertInput::Default {
                        return Err(TransactionError::InvalidContext);
                    }
                    let mut uuid = [0; 16];
                    getrandom::fill(&mut uuid)
                        .map_err(|_| TransactionError::RandomnessUnavailable)?;
                    if uuid == [0; 16] {
                        return Err(TransactionError::RandomnessUnavailable);
                    }
                    materialized_inputs[ordinal] =
                        InsertInput::Value(Some(crate::tuple::ScalarValue::Uuid(uuid)));
                }
            }
            let mut defaults = Vec::with_capacity(inputs.len());
            for (ordinal, expression_id) in schema.default_expression_ids.iter().enumerate() {
                let Some(expression_id) = expression_id else {
                    defaults.push(None);
                    continue;
                };
                let expression = self
                    .read_catalog_expression(*expression_id)?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if expression.purpose != ExpressionPurpose::Default {
                    return Err(TransactionError::CorruptCatalog);
                }
                validate_expression_binding(
                    &expression,
                    &schema.columns,
                    &schema.column_ids,
                    schema.column_type_ids[ordinal],
                    schema.columns[ordinal].scalar_type,
                )
                .map_err(|_| TransactionError::CorruptCatalog)?;
                let allowed_default = match &expression.expression {
                    Expr::Literal(_) => schema.column_default_masks[ordinal] & 1 != 0,
                    Expr::CurrentPrincipalId => schema.column_default_masks[ordinal] & 4 != 0,
                    _ => false,
                };
                if !allowed_default {
                    return Err(TransactionError::CorruptCatalog);
                }
                defaults.push(Some(expression.expression));
            }
            self.stage_insert_authorized_with_defaults(
                id,
                projection,
                &materialized_inputs,
                &defaults,
                &generated_columns,
            )
        })();
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    pub(super) fn read_inner(&mut self, id: RowId) -> Result<Option<Vec<u8>>, TransactionError> {
        if self.aborted {
            return Err(TransactionError::Aborted);
        }
        let snapshot_row = self
            .pinned_snapshot()?
            .get(&row_key(id))
            .map_err(|error| TransactionError::Storage(error.to_string()))?
            .map(|bytes| RowRecord::decode(&bytes))
            .transpose()
            .map_err(TransactionError::CorruptRow)?;
        match self.overlay.read_for_actor(&id, self.principal_id, |_| {
            snapshot_row
                .as_ref()
                .map(|record| record.payload.as_slice())
        })? {
            Some(ResolvedRow::OverlayValue(bytes) | ResolvedRow::SnapshotValue(bytes)) => {
                Ok(Some(bytes.to_vec()))
            }
            None => Ok(None),
        }
    }

    pub fn stage_put(&mut self, id: RowId, payload: &[u8]) -> Result<(), TransactionError> {
        match self.stage_put_inner(id, payload) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.abort()?;
                Err(error)
            }
        }
    }

    /// Insert a fully materialized row after row INSERT policy validation and
    /// per-field INSERT checks. Trusted generated fields are exempt from cell
    /// grants/policies but remain subject to the row creation policy.
    pub fn stage_insert_authorized(
        &mut self,
        id: RowId,
        projection: &AuthorizedProjection<'_>,
        values: &[Option<crate::tuple::ScalarValue>],
        generated_columns: &[[u8; 16]],
    ) -> Result<(), TransactionError> {
        let result = self.stage_insert_authorized_inner(id, projection, values, generated_columns);
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                self.abort()?;
                Err(error)
            }
        }
    }

    /// Materialize explicit DEFAULT markers from trusted schema-bound
    /// expressions before applying the usual authorization and CHECK path.
    /// A missing expression means SQL NULL; tuple encoding enforces nullability.
    pub fn stage_insert_authorized_with_defaults(
        &mut self,
        id: RowId,
        projection: &AuthorizedProjection<'_>,
        inputs: &[InsertInput],
        defaults: &[Option<Expr>],
        generated_columns: &[[u8; 16]],
    ) -> Result<(), TransactionError> {
        let result = (|| {
            if inputs.len() != projection.columns.len()
                || defaults.len() != projection.columns.len()
                || inputs.len() > crate::tuple::MAX_COLUMNS
            {
                return Err(TransactionError::InvalidContext);
            }
            let mut values = Vec::with_capacity(inputs.len());
            for (input, default) in inputs.iter().zip(defaults) {
                values.push(match input {
                    InsertInput::Value(value) => value.clone(),
                    InsertInput::Default => match default {
                        Some(expression) => materialize_default(expression, projection.context)
                            .map_err(TransactionError::Authorization)?,
                        None => None,
                    },
                });
            }
            self.stage_insert_authorized(id, projection, &values, generated_columns)
        })();
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    pub(super) fn stage_insert_authorized_inner(
        &mut self,
        id: RowId,
        projection: &AuthorizedProjection<'_>,
        values: &[Option<crate::tuple::ScalarValue>],
        generated_columns: &[[u8; 16]],
    ) -> Result<(), TransactionError> {
        if self.aborted {
            return Err(TransactionError::Aborted);
        }
        self.capture_authority_dependencies(projection.context, projection.dependencies)?;
        let generated = generated_columns.iter().copied().collect::<HashSet<_>>();
        if projection.columns.len() != projection.column_ids.len()
            || projection.columns.len() != values.len()
            || generated.len() != generated_columns.len()
            || generated
                .iter()
                .any(|column_id| !projection.column_ids.contains(column_id))
        {
            return Err(TransactionError::InvalidContext);
        }
        let table = AccessTarget::Table(id.table);
        let schema = AccessTarget::Schema(projection.schema_id);
        if !has_permission(
            projection.context,
            projection.grants,
            Operation::Usage,
            schema,
        ) {
            return Err(TransactionError::Authorization(AuthorizationError::Denied));
        }
        if !has_permission(
            projection.context,
            projection.grants,
            Operation::Insert,
            table,
        ) {
            return Err(TransactionError::Authorization(AuthorizationError::Denied));
        }
        for &column_id in projection.column_ids {
            if generated.contains(&column_id) {
                continue;
            }
            let target = AccessTarget::Column {
                table_id: id.table,
                column_id,
            };
            if !has_permission(
                projection.context,
                projection.grants,
                Operation::Insert,
                target,
            ) {
                return Err(TransactionError::Authorization(AuthorizationError::Denied));
            }
        }
        let payload = encode_tuple(projection.columns, projection.schema_version, values)
            .map_err(TransactionError::Tuple)?;
        authorize(
            projection.context,
            projection.grants,
            projection.policies,
            Operation::Insert,
            table,
            None,
            Some(values),
        )
        .map_err(TransactionError::Authorization)?;
        validate_check_constraints(projection, values)?;
        for &column_id in projection.column_ids {
            if generated.contains(&column_id) {
                continue;
            }
            let target = AccessTarget::Column {
                table_id: id.table,
                column_id,
            };
            authorize(
                projection.context,
                projection.grants,
                projection.policies,
                Operation::Insert,
                target,
                None,
                Some(values),
            )
            .map_err(TransactionError::Authorization)?;
        }
        if self.read_inner(id)?.is_some() || self.capture_base_version(id)?.is_some() {
            return Err(TransactionError::RowAlreadyExists);
        }
        self.stage_put_inner(id, &payload)
    }

    pub(super) fn stage_put_inner(
        &mut self,
        id: RowId,
        payload: &[u8],
    ) -> Result<(), TransactionError> {
        if self.aborted {
            return Err(TransactionError::Aborted);
        }
        if self.active_scan.is_some() {
            return Err(TransactionError::ScanStillActive);
        }
        self.capture_base_version(id)?;
        if let Err(error) = self.overlay.put_as(id, payload, self.principal_id) {
            self.coordination.update_write_set_usage(0, 0);
            return Err(error.into());
        }
        self.coordination.record_staged_value_bytes(payload.len());
        self.update_write_set_usage()?;
        Ok(())
    }

    pub(super) fn update_write_set_usage(&self) -> Result<(), TransactionError> {
        self.coordination.update_write_set_usage(
            self.overlay.staged_row_count()?,
            self.overlay.staged_value_bytes()?,
        );
        Ok(())
    }

    pub fn stage_delete(&mut self, id: RowId) -> Result<(), TransactionError> {
        match self.stage_delete_inner(id) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.abort()?;
                Err(error)
            }
        }
    }

    /// Delete a row only when it is visible and the pinned table DELETE policy
    /// and explicit DELETE grant pass. Cell readability is not required.
    pub fn stage_delete_authorized(
        &mut self,
        id: RowId,
        projection: &AuthorizedProjection<'_>,
    ) -> Result<(), TransactionError> {
        let result = self.stage_delete_authorized_inner(id, projection);
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                self.abort()?;
                Err(error)
            }
        }
    }

    pub(super) fn stage_delete_authorized_inner(
        &mut self,
        id: RowId,
        projection: &AuthorizedProjection<'_>,
    ) -> Result<(), TransactionError> {
        if self.aborted {
            return Err(TransactionError::Aborted);
        }
        self.capture_authority_dependencies(projection.context, projection.dependencies)?;
        let table = AccessTarget::Table(id.table);
        if !has_permission(
            projection.context,
            projection.grants,
            Operation::Delete,
            table,
        ) {
            return Err(TransactionError::Authorization(AuthorizationError::Denied));
        }
        let payload = self.read_inner(id)?.ok_or(TransactionError::NotFound)?;
        let tuple = decode_tuple(projection.columns, projection.schema_version, &payload)
            .map_err(TransactionError::Tuple)?;
        if !row_visible(
            projection.context,
            projection.grants,
            projection.policies,
            projection.schema_id,
            id.table,
            projection.column_ids,
            &tuple.values,
        )
        .map_err(TransactionError::Authorization)?
        {
            return Err(TransactionError::NotFound);
        }
        authorize(
            projection.context,
            projection.grants,
            projection.policies,
            Operation::Delete,
            table,
            Some(&tuple.values),
            None,
        )
        .map_err(TransactionError::Authorization)?;
        self.stage_delete_inner(id)
    }

    /// Stage a schema-derived index membership change for an already staged row.
    /// The caller must provide the complete key; trusted storage still owns
    /// catalog validation, uniqueness, and index-delta completeness.
    pub fn stage_index_membership(
        &mut self,
        id: RowId,
        key: Vec<u8>,
        present: bool,
    ) -> Result<(), TransactionError> {
        self.stage_index_membership_with_semantics(id, key, present, false, false)
    }

    /// Stage membership with catalog-derived uniqueness and NULL semantics.
    pub fn stage_index_membership_with_semantics(
        &mut self,
        id: RowId,
        key: Vec<u8>,
        present: bool,
        unique: bool,
        null_exempt: bool,
    ) -> Result<(), TransactionError> {
        match self.stage_index_membership_inner(id, key, present, unique, null_exempt) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.abort()?;
                Err(error)
            }
        }
    }

    pub(super) fn stage_index_membership_inner(
        &mut self,
        id: RowId,
        key: Vec<u8>,
        present: bool,
        unique: bool,
        null_exempt: bool,
    ) -> Result<(), TransactionError> {
        if self.aborted {
            return Err(TransactionError::Aborted);
        }
        if self.active_scan.is_some() {
            return Err(TransactionError::ScanStillActive);
        }
        match self.overlay.get_for_actor(&id, self.principal_id)? {
            Some(StagedRow::Value(_)) => {}
            Some(StagedRow::Delete) if !present => {}
            _ => return Err(TransactionError::IndexRowNotStaged),
        }
        if key.len() < 32
            || key.len() > crate::index_key::MAX_INDEX_KEY_BYTES
            || (unique && key.len() == 32)
            || key[key.len() - 16..] != id.row
        {
            return Err(TransactionError::Commit(CommitError::InvalidIndex));
        }
        if self
            .index_writes
            .get(&key)
            .is_some_and(|write| write.row_id != id)
        {
            return Err(TransactionError::Commit(CommitError::InvalidIndex));
        }
        let expected_version = self
            .pinned_snapshot()?
            .get_index_version(&key)
            .map_err(|error| TransactionError::Storage(error.to_string()))?;
        if !present && expected_version.is_none() {
            if self
                .index_writes
                .get(&key)
                .is_some_and(|write| write.present)
            {
                self.index_writes.remove(&key);
                self.index_bytes -= key.len();
                return Ok(());
            }
            return Err(TransactionError::Commit(CommitError::InvalidIndex));
        }
        let old_len = self.index_writes.get(&key).map_or(0, |_| key.len());
        let next_bytes = self
            .index_bytes
            .checked_sub(old_len)
            .and_then(|bytes| bytes.checked_add(key.len()))
            .filter(|&bytes| bytes <= self.max_index_bytes)
            .ok_or(TransactionError::IndexOverlayLimitExceeded)?;
        if !self.index_writes.contains_key(&key)
            && self.index_writes.len() >= self.max_index_changes
        {
            return Err(TransactionError::IndexOverlayLimitExceeded);
        }
        self.index_writes.insert(
            key,
            StagedIndexChange {
                row_id: id,
                expected_version,
                present,
                unique,
                null_exempt,
            },
        );
        self.index_bytes = next_bytes;
        Ok(())
    }

    /// Merge validated field changes into the pinned row while retaining untouched fields.
    /// Caller-side authorization must restrict which ordinals may be patched before this is
    /// exposed to any executor request path.
    pub fn stage_patch(
        &mut self,
        id: RowId,
        columns: &[ColumnSchema],
        schema_version: u64,
        patches: &[FieldPatch],
    ) -> Result<(), TransactionError> {
        match self.stage_patch_inner(id, columns, schema_version, patches) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.abort()?;
                Err(error)
            }
        }
    }

    /// Apply a cell patch only after checking the pinned row and cell policies
    /// against both old and complete proposed rows. A dependency-driving field
    /// is immutable to query updates.
    pub fn stage_patch_authorized(
        &mut self,
        id: RowId,
        projection: &AuthorizedProjection<'_>,
        patches: &[FieldPatch],
    ) -> Result<(), TransactionError> {
        let result = self.stage_patch_authorized_inner(id, projection, patches);
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                self.abort()?;
                Err(error)
            }
        }
    }

    pub(super) fn stage_patch_authorized_inner(
        &mut self,
        id: RowId,
        projection: &AuthorizedProjection<'_>,
        patches: &[FieldPatch],
    ) -> Result<(), TransactionError> {
        if self.aborted {
            return Err(TransactionError::Aborted);
        }
        self.capture_authority_dependencies(projection.context, projection.dependencies)?;
        if projection.columns.len() != projection.column_ids.len()
            || patches.iter().any(|patch| {
                projection
                    .column_ids
                    .get(patch.ordinal)
                    .is_none_or(|column_id| projection.policy_driving_columns.contains(column_id))
            })
        {
            return Err(TransactionError::Authorization(AuthorizationError::Denied));
        }
        let table = AccessTarget::Table(id.table);
        if !has_permission(
            projection.context,
            projection.grants,
            Operation::Update,
            table,
        ) {
            return Err(TransactionError::Authorization(AuthorizationError::Denied));
        }
        let old_payload = self.read_inner(id)?.ok_or(TransactionError::NotFound)?;
        let old_tuple = decode_tuple(projection.columns, projection.schema_version, &old_payload)
            .map_err(TransactionError::Tuple)?;
        if !row_visible(
            projection.context,
            projection.grants,
            projection.policies,
            projection.schema_id,
            id.table,
            projection.column_ids,
            &old_tuple.values,
        )
        .map_err(TransactionError::Authorization)?
        {
            return Err(TransactionError::NotFound);
        }
        for patch in patches {
            let target = AccessTarget::Column {
                table_id: id.table,
                column_id: projection.column_ids[patch.ordinal],
            };
            if !has_permission(
                projection.context,
                projection.grants,
                Operation::Select,
                target,
            ) || !has_permission(
                projection.context,
                projection.grants,
                Operation::Update,
                target,
            ) {
                return Err(TransactionError::Authorization(AuthorizationError::Denied));
            }
            authorize(
                projection.context,
                projection.grants,
                projection.policies,
                Operation::Select,
                target,
                Some(&old_tuple.values),
                None,
            )
            .map_err(TransactionError::Authorization)?;
        }
        let mut new_tuple = old_tuple.clone();
        apply_patch(projection.columns, &mut new_tuple, patches)
            .map_err(TransactionError::Tuple)?;
        validate_check_constraints(projection, &new_tuple.values)?;
        let new_payload = encode_tuple(
            projection.columns,
            projection.schema_version,
            &new_tuple.values,
        )
        .map_err(TransactionError::Tuple)?;
        authorize(
            projection.context,
            projection.grants,
            projection.policies,
            Operation::Update,
            table,
            Some(&old_tuple.values),
            Some(&new_tuple.values),
        )
        .map_err(TransactionError::Authorization)?;
        for patch in patches {
            let target = AccessTarget::Column {
                table_id: id.table,
                column_id: projection.column_ids[patch.ordinal],
            };
            authorize(
                projection.context,
                projection.grants,
                projection.policies,
                Operation::Select,
                target,
                Some(&old_tuple.values),
                None,
            )
            .map_err(TransactionError::Authorization)?;
            authorize(
                projection.context,
                projection.grants,
                projection.policies,
                Operation::Update,
                target,
                Some(&old_tuple.values),
                Some(&new_tuple.values),
            )
            .map_err(TransactionError::Authorization)?;
        }
        self.stage_put_inner(id, &new_payload)?;
        Ok(())
    }

    pub(super) fn stage_patch_inner(
        &mut self,
        id: RowId,
        columns: &[ColumnSchema],
        schema_version: u64,
        patches: &[FieldPatch],
    ) -> Result<(), TransactionError> {
        if self.aborted {
            return Err(TransactionError::Aborted);
        }
        if self.active_scan.is_some() {
            return Err(TransactionError::ScanStillActive);
        }
        let payload = self.read_inner(id)?.ok_or(TransactionError::NotFound)?;
        let mut tuple =
            decode_tuple(columns, schema_version, &payload).map_err(TransactionError::Tuple)?;
        apply_patch(columns, &mut tuple, patches).map_err(TransactionError::Tuple)?;
        let merged = encode_tuple(columns, schema_version, &tuple.values)
            .map_err(TransactionError::Tuple)?;
        self.stage_put_inner(id, &merged)
    }

    pub(super) fn stage_delete_inner(&mut self, id: RowId) -> Result<(), TransactionError> {
        if self.aborted {
            return Err(TransactionError::Aborted);
        }
        if self.active_scan.is_some() {
            return Err(TransactionError::ScanStillActive);
        }
        let base_version = self.capture_base_version(id)?;
        let current = self.overlay.get_for_actor(&id, self.principal_id)?;
        if matches!(current, Some(StagedRow::Delete)) {
            return Err(TransactionError::NotFound);
        }
        if base_version.is_none() {
            if current.is_some() {
                self.overlay.remove(&id)?;
                self.update_write_set_usage()?;
                self.base_versions.remove(&id);
                self.index_writes.retain(|_, write| write.row_id != id);
                self.index_bytes = self.index_writes.keys().map(Vec::len).sum();
                return Ok(());
            }
            return Err(TransactionError::NotFound);
        }
        if let Err(error) = self.overlay.delete_as(id, self.principal_id) {
            self.coordination.update_write_set_usage(0, 0);
            return Err(error.into());
        }
        self.update_write_set_usage()?;
        Ok(())
    }
}
pub(super) fn validate_check_constraints(
    projection: &AuthorizedProjection<'_>,
    values: &[Option<crate::tuple::ScalarValue>],
) -> Result<(), TransactionError> {
    if projection.checks.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS {
        return Err(TransactionError::InvalidContext);
    }
    for expression in projection.checks {
        if !check_constraint_passes(expression, values, projection.context)
            .map_err(TransactionError::Authorization)?
        {
            return Err(TransactionError::ConstraintViolation);
        }
    }
    Ok(())
}
