use super::*;
use crate::authorization::{
    AccessTarget, AuthorizationError, Effect, Expr, Grant, Membership, Operation, Policy, Subject,
    resolve_role_closure,
};
use crate::catalog::{CatalogRecord, decode_builtin_scalar_type};
use crate::commit::{CommitError, ForeignKeyCheck, IndexWrite, RowWrite};
use crate::expression::{
    CatalogExpression, ExpressionPurpose, decode_catalog_expression_record,
    is_row_local_expression, validate_expression_binding,
};
use crate::index_key::{DeriveIndexKeyError, derive_index_key};
use crate::row_record::RowRecord;
use crate::snapshot::{MAX_CATALOG_FAMILY_SCAN, RowSnapshot, SnapshotError};
use crate::tuple::{ColumnSchema, ScalarType, decode_tuple, encode_tuple};
use std::collections::{HashMap, HashSet};

impl<'db> InvocationTransaction<'db> {
    pub(super) fn scan_catalog_family(
        &mut self,
        family_code: u16,
    ) -> Result<Vec<CatalogRecord>, TransactionError> {
        let result = (|| {
            let remaining = self.max_scan_work.saturating_sub(self.scanned_work);
            if remaining == 0 {
                return Err(TransactionError::ScanBudgetExceeded);
            }
            let family_limit = MAX_CATALOG_FAMILY_SCAN;
            let records =
                self.pinned_snapshot()?
                    .scan_catalog_family_limited(family_code, remaining.min(family_limit))
                    .map_err(|error| match error {
                        SnapshotError::RecordLimitExceeded if remaining < family_limit => {
                            TransactionError::ScanBudgetExceeded
                        }
                        SnapshotError::RecordLimitExceeded
                        | SnapshotError::InvalidCatalogRecord => TransactionError::CorruptCatalog,
                        SnapshotError::Storage(error) => TransactionError::Storage(error),
                        _ => TransactionError::CorruptCatalog,
                    })?;
            self.charge_scan_work(records.len())?;
            Ok(records)
        })();
        if matches!(&result, Err(TransactionError::ScanBudgetExceeded)) && !self.aborted {
            self.abort()?;
        }
        result
    }

    pub(super) fn scan_policy_ids_for_table(
        &mut self,
        table_id: [u8; 16],
    ) -> Result<Vec<[u8; 16]>, TransactionError> {
        let result = (|| {
            let remaining = self.max_scan_work.saturating_sub(self.scanned_work);
            if remaining == 0 {
                return Err(TransactionError::ScanBudgetExceeded);
            }
            let policy_limit = crate::authorization::MAX_AUTHORIZATION_INPUTS;
            let limit = remaining.min(policy_limit);
            let ids =
                self.pinned_snapshot()?
                    .scan_policy_ids_for_table_limited(table_id, limit)
                    .map_err(|error| match error {
                        SnapshotError::RecordLimitExceeded if remaining <= policy_limit => {
                            TransactionError::ScanBudgetExceeded
                        }
                        SnapshotError::RecordLimitExceeded
                        | SnapshotError::InvalidCatalogRecord => TransactionError::CorruptCatalog,
                        SnapshotError::Storage(error) => TransactionError::Storage(error),
                        _ => TransactionError::CorruptCatalog,
                    })?;
            self.charge_scan_work(ids.len())?;
            Ok(ids)
        })();
        if matches!(&result, Err(TransactionError::ScanBudgetExceeded)) && !self.aborted {
            self.abort()?;
        }
        result
    }

    pub(super) fn scan_column_ids_for_table(
        &mut self,
        table_id: [u8; 16],
    ) -> Result<Vec<[u8; 16]>, TransactionError> {
        let result = (|| {
            let remaining = self.max_scan_work.saturating_sub(self.scanned_work);
            if remaining == 0 {
                return Err(TransactionError::ScanBudgetExceeded);
            }
            let column_limit = crate::tuple::MAX_COLUMNS;
            let limit = remaining.min(column_limit);
            let ids =
                self.pinned_snapshot()?
                    .scan_column_ids_for_table(table_id, limit)
                    .map_err(|error| match error {
                        SnapshotError::RecordLimitExceeded if remaining <= column_limit => {
                            TransactionError::ScanBudgetExceeded
                        }
                        SnapshotError::RecordLimitExceeded
                        | SnapshotError::InvalidCatalogRecord => TransactionError::CorruptCatalog,
                        SnapshotError::Storage(error) => TransactionError::Storage(error),
                        _ => TransactionError::CorruptCatalog,
                    })?;
            self.charge_scan_work(ids.len())?;
            Ok(ids)
        })();
        if matches!(&result, Err(TransactionError::ScanBudgetExceeded)) && !self.aborted {
            self.abort()?;
        }
        result
    }

    pub(super) fn scan_index_ids_for_table(
        &mut self,
        table_id: [u8; 16],
    ) -> Result<Vec<[u8; 16]>, TransactionError> {
        let result = (|| {
            let remaining = self.max_scan_work.saturating_sub(self.scanned_work);
            if remaining == 0 {
                return Err(TransactionError::ScanBudgetExceeded);
            }
            let limit = remaining.min(MAX_CATALOG_FAMILY_SCAN);
            let ids = self
                .pinned_snapshot()?
                .scan_index_ids_for_table(table_id, limit)
                .map_err(|error| match error {
                    SnapshotError::RecordLimitExceeded if remaining <= MAX_CATALOG_FAMILY_SCAN => {
                        TransactionError::ScanBudgetExceeded
                    }
                    SnapshotError::RecordLimitExceeded | SnapshotError::InvalidCatalogRecord => {
                        TransactionError::CorruptCatalog
                    }
                    SnapshotError::Storage(error) => TransactionError::Storage(error),
                    _ => TransactionError::CorruptCatalog,
                })?;
            self.charge_scan_work(ids.len())?;
            Ok(ids)
        })();
        if matches!(&result, Err(TransactionError::ScanBudgetExceeded)) && !self.aborted {
            self.abort()?;
        }
        result
    }

    pub(super) fn scan_constraint_ids_for_table(
        &mut self,
        table_id: [u8; 16],
    ) -> Result<Vec<[u8; 16]>, TransactionError> {
        let result = (|| {
            let remaining = self.max_scan_work.saturating_sub(self.scanned_work);
            if remaining == 0 {
                return Err(TransactionError::ScanBudgetExceeded);
            }
            let limit = remaining.min(MAX_CATALOG_FAMILY_SCAN);
            let ids = self
                .pinned_snapshot()?
                .scan_constraint_ids_for_table(table_id, limit)
                .map_err(|error| match error {
                    SnapshotError::RecordLimitExceeded if remaining <= MAX_CATALOG_FAMILY_SCAN => {
                        TransactionError::ScanBudgetExceeded
                    }
                    SnapshotError::RecordLimitExceeded | SnapshotError::InvalidCatalogRecord => {
                        TransactionError::CorruptCatalog
                    }
                    SnapshotError::Storage(error) => TransactionError::Storage(error),
                    _ => TransactionError::CorruptCatalog,
                })?;
            self.charge_scan_work(ids.len())?;
            Ok(ids)
        })();
        if matches!(&result, Err(TransactionError::ScanBudgetExceeded)) && !self.aborted {
            self.abort()?;
        }
        result
    }

    pub(super) fn scan_relation_ids_for_schema(
        &mut self,
        schema_id: [u8; 16],
    ) -> Result<Vec<[u8; 16]>, TransactionError> {
        let result = (|| {
            let remaining = self.max_scan_work.saturating_sub(self.scanned_work);
            if remaining == 0 {
                return Err(TransactionError::ScanBudgetExceeded);
            }
            let limit = remaining.min(MAX_CATALOG_FAMILY_SCAN);
            let ids = self
                .pinned_snapshot()?
                .scan_relation_ids_for_schema(schema_id, limit)
                .map_err(|error| match error {
                    SnapshotError::RecordLimitExceeded if remaining <= MAX_CATALOG_FAMILY_SCAN => {
                        TransactionError::ScanBudgetExceeded
                    }
                    SnapshotError::RecordLimitExceeded | SnapshotError::InvalidCatalogRecord => {
                        TransactionError::CorruptCatalog
                    }
                    SnapshotError::Storage(error) => TransactionError::Storage(error),
                    _ => TransactionError::CorruptCatalog,
                })?;
            self.charge_scan_work(ids.len())?;
            Ok(ids)
        })();
        if matches!(&result, Err(TransactionError::ScanBudgetExceeded)) && !self.aborted {
            self.abort()?;
        }
        result
    }

    pub(super) fn scan_object_ids_for_owner(
        &mut self,
        owner_id: [u8; 16],
    ) -> Result<Vec<[u8; 16]>, TransactionError> {
        let result = (|| {
            let remaining = self.max_scan_work.saturating_sub(self.scanned_work);
            if remaining == 0 {
                return Err(TransactionError::ScanBudgetExceeded);
            }
            let limit = remaining.min(MAX_CATALOG_FAMILY_SCAN);
            let ids = self
                .pinned_snapshot()?
                .scan_object_ids_for_owner(owner_id, limit)
                .map_err(|error| match error {
                    SnapshotError::RecordLimitExceeded if remaining <= MAX_CATALOG_FAMILY_SCAN => {
                        TransactionError::ScanBudgetExceeded
                    }
                    SnapshotError::RecordLimitExceeded | SnapshotError::InvalidCatalogRecord => {
                        TransactionError::CorruptCatalog
                    }
                    SnapshotError::Storage(error) => TransactionError::Storage(error),
                    _ => TransactionError::CorruptCatalog,
                })?;
            self.charge_scan_work(ids.len())?;
            Ok(ids)
        })();
        if matches!(&result, Err(TransactionError::ScanBudgetExceeded)) && !self.aborted {
            self.abort()?;
        }
        result
    }

    pub(super) fn lookup_catalog_dependents_for_referenced(
        &mut self,
        referenced_id: [u8; 16],
    ) -> Result<Vec<CatalogRecord>, TransactionError> {
        let result = (|| {
            let remaining = self.max_scan_work.saturating_sub(self.scanned_work);
            if remaining == 0 {
                return Err(TransactionError::ScanBudgetExceeded);
            }
            let limit = remaining.min(MAX_CATALOG_FAMILY_SCAN);
            let records = self
                .pinned_snapshot()?
                .lookup_dependents_for_referenced(referenced_id, limit)
                .map_err(|error| match error {
                    SnapshotError::RecordLimitExceeded if remaining <= MAX_CATALOG_FAMILY_SCAN => {
                        TransactionError::ScanBudgetExceeded
                    }
                    SnapshotError::RecordLimitExceeded | SnapshotError::InvalidCatalogRecord => {
                        TransactionError::CorruptCatalog
                    }
                    SnapshotError::Storage(error) => TransactionError::Storage(error),
                    _ => TransactionError::CorruptCatalog,
                })?;
            self.charge_scan_work(records.len())?;
            Ok(records)
        })();
        if matches!(&result, Err(TransactionError::ScanBudgetExceeded)) && !self.aborted {
            self.abort()?;
        }
        result
    }

    pub(super) fn scan_policy_targets_for_policy(
        &mut self,
        policy_id: [u8; 16],
    ) -> Result<Vec<CatalogRecord>, TransactionError> {
        let result = (|| {
            let remaining = self.max_scan_work.saturating_sub(self.scanned_work);
            if remaining == 0 {
                return Err(TransactionError::ScanBudgetExceeded);
            }
            let policy_limit = crate::authorization::MAX_AUTHORIZATION_INPUTS;
            let limit = remaining.min(policy_limit);
            let records =
                self.pinned_snapshot()?
                    .scan_policy_targets_for_policy_limited(policy_id, limit)
                    .map_err(|error| match error {
                        SnapshotError::RecordLimitExceeded if remaining <= policy_limit => {
                            TransactionError::ScanBudgetExceeded
                        }
                        SnapshotError::RecordLimitExceeded
                        | SnapshotError::InvalidCatalogRecord => TransactionError::CorruptCatalog,
                        SnapshotError::Storage(error) => TransactionError::Storage(error),
                        _ => TransactionError::CorruptCatalog,
                    })?;
            self.charge_scan_work(records.len())?;
            Ok(records)
        })();
        if matches!(&result, Err(TransactionError::ScanBudgetExceeded)) && !self.aborted {
            self.abort()?;
        }
        result
    }

    pub(super) fn lookup_catalog_grants_for_grantees(
        &mut self,
        grantees: &HashSet<[u8; 16]>,
    ) -> Result<Vec<CatalogRecord>, TransactionError> {
        let mut grantee_ids = grantees.iter().copied().collect::<Vec<_>>();
        grantee_ids.sort_unstable();
        let mut records = Vec::new();
        for grantee_id in grantee_ids {
            let indexed = self
                .pinned_snapshot()?
                .lookup_grants_for_grantee(grantee_id)
                .map_err(|error| match error {
                    SnapshotError::Storage(error) => TransactionError::Storage(error),
                    SnapshotError::InvalidCatalogRecord => TransactionError::CorruptCatalog,
                    _ => TransactionError::CorruptCatalog,
                })?;
            self.charge_scan_work(indexed.len())?;
            records.extend(indexed);
        }
        Ok(records)
    }

    /// Resolve a table's current schema from its pinned catalog snapshot.
    /// Column records are selected by their table ID, sorted by ordinal, and
    /// checked for contiguous ordinals. Every table, column, and type record
    /// is retained as an exact-byte guard for final commit validation.
    pub fn load_catalog_table_schema(
        &mut self,
        table_id: [u8; 16],
    ) -> Result<CatalogTableSchema, TransactionError> {
        let result = (|| {
            if table_id == [0; 16] {
                return Err(TransactionError::InvalidContext);
            }
            let table_key = catalog_id_key(5, table_id);
            let table = self
                .read_catalog_record(&table_key)?
                .ok_or(TransactionError::CorruptCatalog)?;
            if table.family_code != 5 {
                return Err(TransactionError::CorruptCatalog);
            }
            let schema_id = catalog_id(&table, 1)?;
            let schema_version = catalog_u64(&table, 2)?;
            let write_epoch = catalog_u64(&table, 3)?;
            let storage_object_id = catalog_id(&table, 4)?;
            if schema_version == 0 || write_epoch == 0 || catalog_u16(&table, 5)? != 1 {
                return Err(TransactionError::CorruptCatalog);
            }

            let column_ids = self.scan_column_ids_for_table(table_id)?;
            let records = if column_ids.is_empty() {
                // Preserve the low-level path for snapshots written before
                // catalog-derived column indexes were maintained.
                self.scan_catalog_family(6)?
            } else {
                column_ids
                    .into_iter()
                    .map(|column_id| {
                        self.read_catalog_record(&catalog_id_key(6, column_id))?
                            .ok_or(TransactionError::CorruptCatalog)
                    })
                    .collect::<Result<Vec<_>, _>>()?
            };
            let mut columns = Vec::new();
            for record in records.into_iter().filter(|record| {
                record.family_code == 6
                    && record
                        .fields
                        .iter()
                        .find(|field| field.tag == 1)
                        .is_some_and(|field| field.payload == table_id)
            }) {
                let key = record.logical_key.clone();
                let guarded = self
                    .read_catalog_record(&key)?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if guarded != record || key.len() != 19 || catalog_id(&record, 1)? != table_id {
                    return Err(TransactionError::CorruptCatalog);
                }
                columns.push((
                    catalog_u32(&record, 2)?,
                    key[3..19].try_into().unwrap(),
                    record,
                ));
                if columns.len() > crate::tuple::MAX_COLUMNS {
                    return Err(TransactionError::CorruptCatalog);
                }
            }
            columns.sort_by_key(|(ordinal, _, _)| *ordinal);
            if columns.is_empty() {
                return Err(TransactionError::CorruptCatalog);
            }

            let mut column_schemas = Vec::with_capacity(columns.len());
            let mut column_ids = Vec::with_capacity(columns.len());
            let mut column_type_ids = Vec::with_capacity(columns.len());
            let mut column_default_masks = Vec::with_capacity(columns.len());
            let mut default_expression_ids = Vec::with_capacity(columns.len());
            let mut generation_modes = Vec::with_capacity(columns.len());
            let mut type_records = HashMap::<[u8; 16], CatalogRecord>::new();
            for (expected_ordinal, (ordinal, column_id, record)) in columns.into_iter().enumerate()
            {
                if ordinal as usize != expected_ordinal
                    || catalog_id(&record, 1)? != table_id
                    || catalog_u64(&record, 9)? == 0
                    || catalog_u64(&record, 9)? > schema_version
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                let type_id = catalog_id(&record, 4)?;
                if let std::collections::hash_map::Entry::Vacant(entry) =
                    type_records.entry(type_id)
                {
                    entry.insert(
                        self.read_catalog_record(&catalog_id_key(4, type_id))?
                            .ok_or(TransactionError::CorruptCatalog)?,
                    );
                }
                let type_record = &type_records[&type_id];
                let scalar_type = decode_catalog_scalar_type(type_record, &record)?;
                let default_mask = catalog_u32(type_record, 5)?;
                let nullable = match catalog_field(&record, 6)? {
                    [0] => false,
                    [1] => true,
                    _ => return Err(TransactionError::CorruptCatalog),
                };
                let generation_mode = catalog_u16(&record, 8)?;
                if generation_mode > 1
                    || (generation_mode == 1 && scalar_type != ScalarType::Uuid)
                    || (generation_mode == 1 && record.fields.iter().any(|field| field.tag == 7))
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                encode_tuple(
                    &[ColumnSchema {
                        scalar_type,
                        nullable: true,
                    }],
                    1,
                    &[None],
                )
                .map_err(|_| TransactionError::CorruptCatalog)?;
                column_schemas.push(ColumnSchema {
                    scalar_type,
                    nullable,
                });
                column_ids.push(column_id);
                column_type_ids.push(type_id);
                column_default_masks.push(default_mask);
                default_expression_ids.push(
                    record
                        .fields
                        .iter()
                        .find(|field| field.tag == 7)
                        .map(|field| field.payload.as_slice().try_into().unwrap()),
                );
                generation_modes.push(generation_mode);
            }
            Ok(CatalogTableSchema {
                table_id,
                schema_id,
                schema_version,
                write_epoch,
                storage_object_id,
                columns: column_schemas,
                column_ids,
                column_type_ids,
                column_default_masks,
                default_expression_ids,
                generation_modes,
            })
        })();
        match result {
            Ok(schema) => {
                self.loaded_schemas.insert(schema.table_id, schema.clone());
                Ok(schema)
            }
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Load the table's ordered index definitions from the pinned catalog.
    /// Each definition is checked against the catalog-derived schema, and its
    /// exact bytes are guarded for final commit validation.
    pub fn load_catalog_indexes(
        &mut self,
        schema: &CatalogTableSchema,
    ) -> Result<Vec<CatalogIndexDefinition>, TransactionError> {
        let result = (|| {
            let trusted_schema = self.load_catalog_table_schema(schema.table_id)?;
            if trusted_schema != *schema {
                return Err(TransactionError::InvalidContext);
            }
            let index_ids = self.scan_index_ids_for_table(schema.table_id)?;
            let records = if index_ids.is_empty() {
                // Preserve the low-level path for snapshots whose family-9
                // records predate derived table-index maintenance.
                self.scan_catalog_family(9)?
            } else {
                index_ids
                    .into_iter()
                    .map(|index_id| {
                        self.read_catalog_record(&catalog_id_key(9, index_id))?
                            .ok_or(TransactionError::CorruptCatalog)
                    })
                    .collect::<Result<Vec<_>, _>>()?
            };
            let mut indexes = Vec::new();
            let mut seen_index_ids = HashSet::new();
            for record in records {
                if record.logical_key.len() != 19 || record.family_code != 9 {
                    return Err(TransactionError::CorruptCatalog);
                }
                let table_id = catalog_id(&record, 1)?;
                if table_id != schema.table_id {
                    continue;
                }
                let guarded = self
                    .read_catalog_record(&record.logical_key)?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if guarded != record {
                    return Err(TransactionError::CorruptCatalog);
                }
                let index_id: [u8; 16] = record.logical_key[3..19]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                let column_ids = catalog_id_list(&record, 2)?;
                let mut seen_columns = HashSet::new();
                let unique = match catalog_field(&record, 4)? {
                    [0] => false,
                    [1] => true,
                    _ => return Err(TransactionError::CorruptCatalog),
                };
                if index_id == [0; 16]
                    || !seen_index_ids.insert(index_id)
                    || column_ids.is_empty()
                    || column_ids.len() > 256
                    || column_ids.iter().any(|column_id| {
                        *column_id == [0; 16]
                            || !seen_columns.insert(*column_id)
                            || !schema.column_ids.contains(column_id)
                    })
                    || catalog_u16(&record, 3)? != 1
                    || catalog_u16(&record, 5)? != 1
                    || catalog_optional_id(&record, 6)?.is_some_and(|id| id == [0; 16])
                    || catalog_id(&record, 7)? == [0; 16]
                    || catalog_u16(&record, 8)? != 1
                    || catalog_u32(&record, 9)? != 1
                    || catalog_u64(&record, 10)? != schema.schema_version
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                indexes.push(CatalogIndexDefinition {
                    index_id,
                    table_id,
                    column_ids,
                    unique,
                });
            }
            indexes.sort_by_key(|index| index.index_id);
            self.load_catalog_foreign_keys_for_table(schema)?;
            Ok(indexes)
        })();
        match result {
            Ok(indexes) => {
                self.loaded_schemas.insert(schema.table_id, schema.clone());
                self.loaded_indexes.insert(schema.table_id, indexes.clone());
                Ok(indexes)
            }
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    pub(super) fn load_catalog_foreign_keys_for_table(
        &mut self,
        schema: &CatalogTableSchema,
    ) -> Result<(), TransactionError> {
        let constraint_ids = self.scan_constraint_ids_for_table(schema.table_id)?;
        let records = if constraint_ids.is_empty() {
            // Preserve raw low-level snapshots created before derived indexes
            // were maintained atomically with family-8 writes.
            self.scan_catalog_family(8)?
        } else {
            let mut candidate_ids = constraint_ids.iter().copied().collect::<HashSet<_>>();
            let own_records = constraint_ids
                .into_iter()
                .map(|constraint_id| {
                    self.read_catalog_record(&catalog_id_key(8, constraint_id))?
                        .ok_or(TransactionError::CorruptCatalog)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let mut referenced_constraint_ids = Vec::new();
            for record in &own_records {
                if matches!(catalog_u16(record, 2)?, 2 | 3) {
                    let constraint_id = record
                        .logical_key
                        .get(3..19)
                        .and_then(|bytes| bytes.try_into().ok())
                        .ok_or(TransactionError::CorruptCatalog)?;
                    referenced_constraint_ids.push(constraint_id);
                }
            }
            referenced_constraint_ids.sort_unstable();
            referenced_constraint_ids.dedup();
            for referenced_id in referenced_constraint_ids {
                for edge in self.lookup_catalog_dependents_for_referenced(referenced_id)? {
                    let edge_kind = edge
                        .logical_key
                        .get(37..39)
                        .and_then(|bytes| bytes.try_into().ok())
                        .map(u16::from_be_bytes)
                        .ok_or(TransactionError::CorruptCatalog)?;
                    if edge.logical_key.len() != 39 {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    if edge_kind == 3 {
                        let foreign_key_id = edge
                            .logical_key
                            .get(3..19)
                            .and_then(|bytes| bytes.try_into().ok())
                            .ok_or(TransactionError::CorruptCatalog)?;
                        if foreign_key_id == [0; 16] {
                            return Err(TransactionError::CorruptCatalog);
                        }
                        candidate_ids.insert(foreign_key_id);
                    }
                }
            }
            let mut candidate_ids = candidate_ids.into_iter().collect::<Vec<_>>();
            candidate_ids.sort_unstable();
            candidate_ids
                .into_iter()
                .map(|constraint_id| {
                    self.read_catalog_record(&catalog_id_key(8, constraint_id))?
                        .ok_or(TransactionError::CorruptCatalog)
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        for record in records {
            if record.family_code != 8 || record.logical_key.len() != 19 {
                return Err(TransactionError::CorruptCatalog);
            }
            if catalog_u16(&record, 2)? != 5 {
                continue;
            }
            let constraint_id: [u8; 16] = record.logical_key[3..19]
                .try_into()
                .map_err(|_| TransactionError::CorruptCatalog)?;
            let source_table_id = catalog_id(&record, 1)?;
            let target_constraint_id =
                catalog_optional_id(&record, 5)?.ok_or(TransactionError::CorruptCatalog)?;
            if constraint_id == [0; 16] || target_constraint_id == [0; 16] {
                return Err(TransactionError::CorruptCatalog);
            }
            let target_constraint_key = catalog_id_key(8, target_constraint_id);
            let target_snapshot_record = self
                .pinned_snapshot()?
                .get_catalog_record(&target_constraint_key)
                .map_err(|error| TransactionError::Storage(error.to_string()))?
                .ok_or(TransactionError::CorruptCatalog)?;
            if target_snapshot_record.family_code != 8 {
                return Err(TransactionError::CorruptCatalog);
            }
            let target_table_id = catalog_id(&target_snapshot_record, 1)?;
            if source_table_id != schema.table_id && target_table_id != schema.table_id {
                continue;
            }

            let guarded_foreign_key = self
                .read_catalog_record(&record.logical_key)?
                .ok_or(TransactionError::CorruptCatalog)?;
            let guarded_target = self
                .read_catalog_record(&target_constraint_key)?
                .ok_or(TransactionError::CorruptCatalog)?;
            if guarded_foreign_key != record || guarded_target != target_snapshot_record {
                return Err(TransactionError::CorruptCatalog);
            }
            let target_kind = catalog_u16(&guarded_target, 2)?;
            let source_column_ids = catalog_id_list(&guarded_foreign_key, 3)?;
            let target_column_ids = catalog_id_list(&guarded_target, 3)?;
            let referenced_column_ids = catalog_id_list(&guarded_foreign_key, 6)?;
            let target_index_id = catalog_optional_id(&guarded_target, 11)?
                .ok_or(TransactionError::CorruptCatalog)?;
            if !matches!(target_kind, 2 | 3)
                || catalog_id(&guarded_target, 1)? != target_table_id
                || source_column_ids.is_empty()
                || source_column_ids.len() != referenced_column_ids.len()
                || referenced_column_ids != target_column_ids
                || source_column_ids.contains(&[0; 16])
                || referenced_column_ids.contains(&[0; 16])
                || source_column_ids.iter().collect::<HashSet<_>>().len() != source_column_ids.len()
                || referenced_column_ids.iter().collect::<HashSet<_>>().len()
                    != referenced_column_ids.len()
                || catalog_optional_id(&guarded_foreign_key, 4)?.is_some()
                || catalog_optional_id(&guarded_foreign_key, 11)?.is_some()
                || catalog_u16(&guarded_foreign_key, 7)? != 1
                || catalog_u16(&guarded_foreign_key, 8)? != 1
                || catalog_u16(&guarded_foreign_key, 9)? != 1
                || catalog_u16(&guarded_foreign_key, 10)? != 1
                || catalog_optional_id(&guarded_target, 5)?.is_some()
                || !catalog_id_list(&guarded_target, 6)?.is_empty()
                || catalog_u16(&guarded_target, 7)? != 0
                || catalog_u16(&guarded_target, 8)? != 0
                || catalog_u16(&guarded_target, 9)? != 0
                || catalog_u16(&guarded_target, 10)? != 1
                || target_index_id == [0; 16]
            {
                return Err(TransactionError::CorruptCatalog);
            }

            let source_schema = if source_table_id == schema.table_id {
                schema.clone()
            } else {
                self.load_catalog_table_schema(source_table_id)?
            };
            let target_schema = if target_table_id == schema.table_id {
                schema.clone()
            } else {
                self.load_catalog_table_schema(target_table_id)?
            };
            let source_ordinals = source_column_ids
                .iter()
                .map(|id| {
                    source_schema
                        .column_ids
                        .iter()
                        .position(|column_id| column_id == id)
                        .ok_or(TransactionError::CorruptCatalog)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let target_ordinals = referenced_column_ids
                .iter()
                .map(|id| {
                    target_schema
                        .column_ids
                        .iter()
                        .position(|column_id| column_id == id)
                        .ok_or(TransactionError::CorruptCatalog)
                })
                .collect::<Result<Vec<_>, _>>()?;
            if source_ordinals
                .iter()
                .zip(&target_ordinals)
                .any(|(source, target)| {
                    source_schema.columns[*source].scalar_type
                        != target_schema.columns[*target].scalar_type
                })
            {
                return Err(TransactionError::CorruptCatalog);
            }
            if target_kind == 2
                && target_ordinals
                    .iter()
                    .any(|ordinal| target_schema.columns[*ordinal].nullable)
            {
                return Err(TransactionError::CorruptCatalog);
            }

            let target_index = self
                .read_catalog_record(&catalog_id_key(9, target_index_id))?
                .ok_or(TransactionError::CorruptCatalog)?;
            if target_index.family_code != 9
                || catalog_id(&target_index, 1)? != target_table_id
                || catalog_id_list(&target_index, 2)? != target_column_ids
                || catalog_u16(&target_index, 3)? != 1
                || catalog_field(&target_index, 4)? != [1]
                || catalog_u16(&target_index, 5)? != 1
                || catalog_optional_id(&target_index, 6)? != Some(target_constraint_id)
                || catalog_id(&target_index, 7)? == [0; 16]
                || catalog_u16(&target_index, 8)? != 1
                || catalog_u32(&target_index, 9)? != 1
                || catalog_u64(&target_index, 10)? != target_schema.schema_version
            {
                return Err(TransactionError::CorruptCatalog);
            }

            let check = ForeignKeyCheck {
                source_table_id,
                source_schema_version: source_schema.schema_version,
                source_schema: source_schema.columns,
                source_ordinals,
                target_table_id,
                target_schema_version: target_schema.schema_version,
                target_schema: target_schema.columns,
                target_ordinals,
            };
            if let Some(existing) = self.loaded_foreign_keys.get(&constraint_id) {
                if existing != &check {
                    return Err(TransactionError::CorruptCatalog);
                }
            } else {
                self.loaded_foreign_keys.insert(constraint_id, check);
            }
            if self.loaded_foreign_keys.len() > 256 {
                return Err(TransactionError::CorruptCatalog);
            }
        }
        Ok(())
    }

    /// Load the pinned membership graph, validate every live endpoint, and
    /// return the transitive role closure for one principal. Membership and
    /// principal records are guarded so authority changes conflict at commit.
    pub fn load_catalog_role_closure(
        &mut self,
        principal_id: [u8; 16],
    ) -> Result<HashSet<[u8; 16]>, TransactionError> {
        let result = (|| {
            if principal_id == [0; 16] {
                return Err(TransactionError::InvalidContext);
            }
            let mut memberships = Vec::new();
            let mut principal_ids = HashSet::from([principal_id]);
            let mut pending = vec![principal_id];
            while let Some(member_id) = pending.pop() {
                let role_ids = self
                    .pinned_snapshot()?
                    .lookup_roles_for_member(member_id)
                    .map_err(|error| match error {
                        SnapshotError::Storage(error) => TransactionError::Storage(error),
                        SnapshotError::InvalidCatalogRecord => TransactionError::CorruptCatalog,
                        _ => TransactionError::CorruptCatalog,
                    })?;
                if memberships.len().saturating_add(role_ids.len())
                    > crate::authorization::MAX_MEMBERSHIP_EDGES
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                for role_id in role_ids {
                    let mut key = 11u16.to_be_bytes().to_vec();
                    key.push(1);
                    key.extend_from_slice(&member_id);
                    key.push(1);
                    key.extend_from_slice(&role_id);
                    let record = self
                        .read_catalog_record(&key)?
                        .ok_or(TransactionError::CorruptCatalog)?;
                    if record.family_code != 11
                        || record.logical_key != key
                        || catalog_id(&record, 1)? == [0; 16]
                        || catalog_u64(&record, 2)? == 0
                    {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    memberships.push(Membership { member_id, role_id });
                    if principal_ids.insert(role_id) {
                        pending.push(role_id);
                    }
                }
            }
            self.charge_scan_work(memberships.len())?;
            let principal_is_membership_endpoint = memberships
                .iter()
                .any(|edge| edge.member_id == principal_id || edge.role_id == principal_id);
            for id in principal_ids {
                let record = self
                    .read_catalog_record(&catalog_id_key(10, id))?
                    .ok_or_else(|| {
                        if id == self.principal_id && !principal_is_membership_endpoint {
                            TransactionError::Authorization(AuthorizationError::Denied)
                        } else {
                            TransactionError::CorruptCatalog
                        }
                    })?;
                let kind = catalog_u16(&record, 1)?;
                let login_enabled = catalog_field(&record, 3)?;
                if !matches!(kind, 1 | 2)
                    || catalog_u64(&record, 4)? == 0
                    || !matches!(login_enabled, [0] | [1])
                    || (kind == 2 && login_enabled != [0])
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                if id == self.principal_id && (kind != 1 || login_enabled != [1]) {
                    return Err(TransactionError::Authorization(AuthorizationError::Denied));
                }
                if memberships.iter().any(|edge| edge.role_id == id) && kind != 2 {
                    return Err(TransactionError::CorruptCatalog);
                }
            }
            resolve_role_closure(principal_id, &memberships)
                .map_err(|_| TransactionError::CorruptCatalog)
        })();
        match result {
            Ok(roles) => Ok(roles),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Load data-access grants for the admitted principal and its pinned role
    /// closure, scoped to one catalog-derived table schema. Matching grant,
    /// grantor, and target column records are retained as commit guards.
    pub fn load_catalog_grants(
        &mut self,
        principal_id: [u8; 16],
        schema: &CatalogTableSchema,
    ) -> Result<(HashSet<[u8; 16]>, Vec<Grant>), TransactionError> {
        let result = (|| {
            if principal_id != self.principal_id || schema.table_id == [0; 16] {
                return Err(TransactionError::InvalidContext);
            }
            let roles = self.load_catalog_role_closure(principal_id)?;
            let mut grantees = roles.clone();
            grantees.insert(principal_id);
            let records = self.lookup_catalog_grants_for_grantees(&grantees)?;
            let mut grants = Vec::new();
            let mut guarded_principals = HashSet::new();
            for record in records {
                let key = &record.logical_key;
                if key.len() != 59
                    || key[2] != 1
                    || key[19] != 1
                    || key[36] != 1
                    || key[39] != 1
                    || key[42] != 1
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                let grantee_id: [u8; 16] = key[3..19]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if !grantees.contains(&grantee_id) {
                    continue;
                }
                let object_id: [u8; 16] = key[20..36]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                let privilege = u16::from_be_bytes([key[37], key[38]]);
                let scope = u16::from_be_bytes([key[40], key[41]]);
                let target_id: [u8; 16] = key[43..59]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                let target = match scope {
                    1 if object_id == schema.schema_id && target_id == object_id => {
                        AccessTarget::Schema(object_id)
                    }
                    1 if object_id == schema.table_id && target_id == object_id => {
                        AccessTarget::Table(object_id)
                    }
                    2 if object_id == schema.table_id && schema.column_ids.contains(&target_id) => {
                        let column_record = self
                            .read_catalog_record(&catalog_id_key(6, target_id))?
                            .ok_or(TransactionError::CorruptCatalog)?;
                        if catalog_id(&column_record, 1)? != schema.table_id {
                            return Err(TransactionError::CorruptCatalog);
                        }
                        AccessTarget::Column {
                            table_id: schema.table_id,
                            column_id: target_id,
                        }
                    }
                    _ => continue,
                };
                let operation = match (privilege, target) {
                    (1, AccessTarget::Schema(_)) => Operation::Usage,
                    (2, AccessTarget::Table(_) | AccessTarget::Column { .. }) => Operation::Select,
                    (3, AccessTarget::Table(_) | AccessTarget::Column { .. }) => Operation::Insert,
                    (4, AccessTarget::Table(_) | AccessTarget::Column { .. }) => Operation::Update,
                    (5, AccessTarget::Table(_)) => Operation::Delete,
                    (1..=5, _) => return Err(TransactionError::CorruptCatalog),
                    _ => continue,
                };
                let guarded = self
                    .read_catalog_record(key)?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if guarded != record || catalog_u64(&record, 2)? == 0 {
                    return Err(TransactionError::CorruptCatalog);
                }
                let grantor_id = catalog_id(&record, 1)?;
                if guarded_principals.insert(grantor_id) {
                    let grantor = self
                        .read_catalog_record(&catalog_id_key(10, grantor_id))?
                        .ok_or(TransactionError::CorruptCatalog)?;
                    let grantor_kind = catalog_u16(&grantor, 1)?;
                    let grantor_login = catalog_field(&grantor, 3)?;
                    if !matches!(grantor_kind, 1 | 2)
                        || catalog_u64(&grantor, 4)? == 0
                        || !matches!(grantor_login, [0] | [1])
                        || (grantor_kind == 2 && grantor_login != [0])
                    {
                        return Err(TransactionError::CorruptCatalog);
                    }
                }
                let subject = if grantee_id == principal_id {
                    Subject::Principal(principal_id)
                } else {
                    Subject::Role(grantee_id)
                };
                grants.push(Grant {
                    subject,
                    operation,
                    target,
                });
                if grants.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                    return Err(TransactionError::CorruptCatalog);
                }
            }
            Ok((roles, grants))
        })();
        match result {
            Ok(authorization) => Ok(authorization),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Load schema USAGE grants for the admitted principal and its pinned role
    /// closure. Matching grants, grantors, and schema records become commit
    /// guards on this invocation.
    pub fn load_catalog_schema_usage_grants(
        &mut self,
        principal_id: [u8; 16],
    ) -> Result<(HashSet<[u8; 16]>, Vec<Grant>), TransactionError> {
        let result = (|| {
            if principal_id != self.principal_id || principal_id == [0; 16] {
                return Err(TransactionError::InvalidContext);
            }
            let roles = self.load_catalog_role_closure(principal_id)?;
            let mut grantees = roles.clone();
            grantees.insert(principal_id);
            let records = self.lookup_catalog_grants_for_grantees(&grantees)?;
            let mut grants = Vec::new();
            let mut guarded_principals = HashSet::new();
            for record in records {
                let key = &record.logical_key;
                if key.len() != 59
                    || key[2] != 1
                    || key[19] != 1
                    || key[36] != 1
                    || key[39] != 1
                    || key[42] != 1
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                let grantee_id: [u8; 16] = key[3..19]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if !grantees.contains(&grantee_id) {
                    continue;
                }
                let schema_id: [u8; 16] = key[20..36]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                let privilege = u16::from_be_bytes([key[37], key[38]]);
                let target_scope = u16::from_be_bytes([key[40], key[41]]);
                let target_id: [u8; 16] = key[43..59]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if privilege != 1 {
                    continue;
                }
                if target_scope != 1 || schema_id == [0; 16] || target_id != schema_id {
                    return Err(TransactionError::CorruptCatalog);
                }
                let guarded = self
                    .read_catalog_record(key)?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if guarded != record || guarded.family_code != 12 || catalog_u64(&record, 2)? == 0 {
                    return Err(TransactionError::CorruptCatalog);
                }
                self.read_discovery_schema(schema_id)?;
                let grantor_id = catalog_id(&record, 1)?;
                if grantor_id == [0; 16] {
                    return Err(TransactionError::CorruptCatalog);
                }
                if guarded_principals.insert(grantor_id) {
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
                }
                grants.push(Grant {
                    subject: if grantee_id == principal_id {
                        Subject::Principal(principal_id)
                    } else {
                        Subject::Role(grantee_id)
                    },
                    operation: Operation::Usage,
                    target: AccessTarget::Schema(schema_id),
                });
                if grants.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                    return Err(TransactionError::CorruptCatalog);
                }
            }
            Ok((roles, grants))
        })();
        match result {
            Ok(authorization) => Ok(authorization),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Load the admitted principal's schema USAGE and relation data grants for
    /// one schema from the pinned catalog and role closure.
    pub fn load_catalog_relation_discovery_grants(
        &mut self,
        schema_id: [u8; 16],
    ) -> Result<(HashSet<[u8; 16]>, Vec<Grant>), TransactionError> {
        let result = (|| {
            if schema_id == [0; 16] {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            let principal_id = self.principal_id;
            let roles = self.load_catalog_role_closure(principal_id)?;
            let mut grantees = roles.clone();
            grantees.insert(principal_id);
            let records = self.lookup_catalog_grants_for_grantees(&grantees)?;

            let mut has_schema_usage = false;
            for record in &records {
                let key = &record.logical_key;
                if key.len() != 59
                    || key[2] != 1
                    || key[19] != 1
                    || key[36] != 1
                    || key[39] != 1
                    || key[42] != 1
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                let grantee_id: [u8; 16] = key[3..19]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if !grantees.contains(&grantee_id) {
                    continue;
                }
                let object_id: [u8; 16] = key[20..36]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                let privilege = u16::from_be_bytes([key[37], key[38]]);
                if object_id == schema_id && privilege == 1 {
                    let target_scope = u16::from_be_bytes([key[40], key[41]]);
                    let target_id: [u8; 16] = key[43..59]
                        .try_into()
                        .map_err(|_| TransactionError::CorruptCatalog)?;
                    if target_scope != 1 || target_id != schema_id {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    has_schema_usage = true;
                }
            }
            if !has_schema_usage {
                return Ok((roles, Vec::new()));
            }

            self.read_discovery_schema(schema_id)?;
            let mut table_ids = self.scan_relation_ids_for_schema(schema_id)?;
            if table_ids.is_empty() {
                for table_record in self.scan_catalog_family(5)? {
                    if table_record.family_code != 5 {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    if catalog_id(&table_record, 1)? == schema_id {
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
            }
            table_ids.sort_unstable();
            if table_ids.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(TransactionError::CorruptCatalog);
            }

            let mut grants = Vec::new();
            let mut guarded_principals = HashSet::new();
            for record in records {
                let key = &record.logical_key;
                let grantee_id: [u8; 16] = key[3..19]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if !grantees.contains(&grantee_id) {
                    continue;
                }
                let object_id: [u8; 16] = key[20..36]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                let privilege = u16::from_be_bytes([key[37], key[38]]);
                let target_scope = u16::from_be_bytes([key[40], key[41]]);
                let target_id: [u8; 16] = key[43..59]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                let target = if object_id == schema_id {
                    if target_scope != 1 || target_id != schema_id {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    match privilege {
                        1 => Some((Operation::Usage, AccessTarget::Schema(schema_id))),
                        7 | 8 | 10 | 11 => None,
                        _ => return Err(TransactionError::CorruptCatalog),
                    }
                } else if table_ids.binary_search(&object_id).is_ok() {
                    match target_scope {
                        1 if target_id == object_id => {
                            let operation = match privilege {
                                2 => Some(Operation::Select),
                                3 => Some(Operation::Insert),
                                4 => Some(Operation::Update),
                                5 => Some(Operation::Delete),
                                6 | 8 | 9 | 10 | 11 => None,
                                _ => return Err(TransactionError::CorruptCatalog),
                            };
                            operation.map(|operation| (operation, AccessTarget::Table(object_id)))
                        }
                        2 if matches!(privilege, 2..=4 | 6) => {
                            self.validate_discovery_column(object_id, target_id)?;
                            let operation = match privilege {
                                2 => Some(Operation::Select),
                                3 => Some(Operation::Insert),
                                4 => Some(Operation::Update),
                                6 => None,
                                _ => unreachable!(),
                            };
                            operation.map(|operation| {
                                (
                                    operation,
                                    AccessTarget::Column {
                                        table_id: object_id,
                                        column_id: target_id,
                                    },
                                )
                            })
                        }
                        _ => return Err(TransactionError::CorruptCatalog),
                    }
                } else {
                    None
                };
                let Some((operation, target)) = target else {
                    continue;
                };
                let guarded = self
                    .read_catalog_record(key)?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if guarded != record || guarded.family_code != 12 || catalog_u64(&record, 2)? == 0 {
                    return Err(TransactionError::CorruptCatalog);
                }
                let grantor_id = catalog_id(&record, 1)?;
                if grantor_id == [0; 16] {
                    return Err(TransactionError::CorruptCatalog);
                }
                if guarded_principals.insert(grantor_id) {
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
                }
                grants.push(Grant {
                    subject: if grantee_id == principal_id {
                        Subject::Principal(principal_id)
                    } else {
                        Subject::Role(grantee_id)
                    },
                    operation,
                    target,
                });
                if grants.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                    return Err(TransactionError::CorruptCatalog);
                }
            }
            Ok((roles, grants))
        })();
        match result {
            Ok(authorization) => Ok(authorization),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Resolve one unique built-in type ID from the pinned type catalog and
    /// retain the type definition as a commit guard.
    pub fn load_catalog_builtin_type_id(
        &mut self,
        builtin_kind: u16,
    ) -> Result<[u8; 16], TransactionError> {
        let result = (|| {
            if !(1..=11).contains(&builtin_kind) {
                return Err(TransactionError::InvalidContext);
            }
            let records = self.scan_catalog_family(4)?;
            let mut matched = None;
            for record in records {
                if catalog_u16(&record, 1)? != builtin_kind {
                    continue;
                }
                if matched.is_some() || record.logical_key.len() != 19 {
                    return Err(TransactionError::CorruptCatalog);
                }
                let guarded = self
                    .read_catalog_record(&record.logical_key)?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if guarded != record {
                    return Err(TransactionError::CorruptCatalog);
                }
                let scalar_type = decode_catalog_scalar_type_parameters(&record, &[0; 8])?;
                if !matches!(
                    (builtin_kind, scalar_type),
                    (1, ScalarType::Boolean)
                        | (2, ScalarType::SmallInt)
                        | (3, ScalarType::Integer)
                        | (4, ScalarType::BigInt)
                        | (5, ScalarType::Decimal { .. })
                        | (6, ScalarType::Text)
                        | (7, ScalarType::Varchar { .. })
                        | (8, ScalarType::Bytea)
                        | (9, ScalarType::Uuid)
                        | (10, ScalarType::Date)
                        | (11, ScalarType::TimestampTz)
                ) {
                    return Err(TransactionError::CorruptCatalog);
                }
                matched = Some(record.logical_key[3..19].try_into().unwrap());
            }
            matched.ok_or(TransactionError::CorruptCatalog)
        })();
        match result {
            Ok(type_id) => Ok(type_id),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Load applicable row and cell policies for the admitted principal from
    /// the pinned catalog. Definitions, target rows, recipient principals,
    /// and referenced schema-bound expressions are retained as commit guards.
    pub fn load_catalog_policies(
        &mut self,
        principal_id: [u8; 16],
        schema: &CatalogTableSchema,
    ) -> Result<Vec<Policy>, TransactionError> {
        self.load_catalog_policies_with_policy_inputs(principal_id, schema, false)
            .map(|(policies, _)| policies)
    }

    pub(super) fn load_catalog_policies_with_policy_inputs(
        &mut self,
        principal_id: [u8; 16],
        schema: &CatalogTableSchema,
        collect_all_policy_inputs: bool,
    ) -> Result<(Vec<Policy>, Vec<[u8; 16]>), TransactionError> {
        let result = (|| {
            if principal_id != self.principal_id || schema.table_id == [0; 16] {
                return Err(TransactionError::InvalidContext);
            }
            let roles = self.load_catalog_role_closure(principal_id)?;
            let policy_ids = self.scan_policy_ids_for_table(schema.table_id)?;
            let policies = if policy_ids.is_empty() {
                // Preserve semantics for in-process snapshots created before
                // startup has reconstructed derived catalog indexes.
                self.scan_catalog_family(13)?
            } else {
                policy_ids
                    .into_iter()
                    .map(|policy_id| {
                        self.read_catalog_record(&catalog_id_key(13, policy_id))?
                            .ok_or(TransactionError::CorruptCatalog)
                    })
                    .collect::<Result<Vec<_>, _>>()?
            };
            if policies.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                return Err(TransactionError::CorruptCatalog);
            }
            let boolean_type_id = self.load_catalog_builtin_type_id(1)?;
            let mut loaded = Vec::new();
            let mut referenced_ordinals = HashSet::new();
            for record in policies {
                if catalog_id(&record, 1)? != schema.table_id {
                    continue;
                }
                let policy_key = record.logical_key.clone();
                if policy_key.len() != 19 {
                    return Err(TransactionError::CorruptCatalog);
                }
                let guarded = self
                    .read_catalog_record(&policy_key)?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if guarded != record || catalog_u64(&record, 7)? == 0 {
                    return Err(TransactionError::CorruptCatalog);
                }
                let policy_id: [u8; 16] = policy_key[3..19]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                let targets = self.scan_policy_targets_for_policy(policy_id)?;
                let kind = catalog_u16(&record, 2)?;
                let operation = match catalog_u16(&record, 3)? {
                    1 => Operation::Select,
                    2 => Operation::Insert,
                    3 => Operation::Update,
                    4 => Operation::Delete,
                    _ => return Err(TransactionError::CorruptCatalog),
                };
                let effect = match catalog_u16(&record, 4)? {
                    1 => Effect::Allow,
                    2 => Effect::Restrict,
                    _ => return Err(TransactionError::CorruptCatalog),
                };
                if kind == 2 && operation == Operation::Delete {
                    return Err(TransactionError::CorruptCatalog);
                }
                let using_id = catalog_optional_id(&record, 5)?;
                let check_id = catalog_optional_id(&record, 6)?;
                match operation {
                    Operation::Select | Operation::Delete
                        if using_id.is_some() && check_id.is_none() => {}
                    Operation::Insert if using_id.is_none() && check_id.is_some() => {}
                    Operation::Update if using_id.is_some() && check_id.is_some() => {}
                    _ => return Err(TransactionError::CorruptCatalog),
                }

                let mut subjects = Vec::new();
                let mut policy_columns = Vec::new();
                let mut has_recipient = false;
                for target_record in &targets {
                    let target_key = target_record.logical_key.clone();
                    let target_guard = self
                        .read_catalog_record(&target_key)?
                        .ok_or(TransactionError::CorruptCatalog)?;
                    if target_guard != *target_record {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    let target_kind = u16::from_be_bytes([target_key[20], target_key[21]]);
                    let target_id: [u8; 16] = target_key[23..39]
                        .try_into()
                        .map_err(|_| TransactionError::CorruptCatalog)?;
                    match target_kind {
                        1 | 2 => {
                            has_recipient = true;
                            let principal_record = self
                                .read_catalog_record(&catalog_id_key(10, target_id))?
                                .ok_or(TransactionError::CorruptCatalog)?;
                            let expected_kind = if target_kind == 1 { 1 } else { 2 };
                            let login_enabled = catalog_field(&principal_record, 3)?;
                            if catalog_u16(&principal_record, 1)? != expected_kind
                                || catalog_u64(&principal_record, 4)? == 0
                                || !matches!(login_enabled, [0] | [1])
                                || (expected_kind == 2 && login_enabled != [0])
                            {
                                return Err(TransactionError::CorruptCatalog);
                            }
                            if target_kind == 1 && target_id == principal_id {
                                subjects.push(Subject::Principal(principal_id));
                            } else if target_kind == 2 && roles.contains(&target_id) {
                                subjects.push(Subject::Role(target_id));
                            }
                        }
                        3 => {
                            if kind != 2
                                || operation == Operation::Delete
                                || !schema.column_ids.contains(&target_id)
                            {
                                return Err(TransactionError::CorruptCatalog);
                            }
                            let column_record = self
                                .read_catalog_record(&catalog_id_key(6, target_id))?
                                .ok_or(TransactionError::CorruptCatalog)?;
                            if catalog_id(&column_record, 1)? != schema.table_id {
                                return Err(TransactionError::CorruptCatalog);
                            }
                            policy_columns.push(target_id);
                        }
                        _ => return Err(TransactionError::CorruptCatalog),
                    }
                }
                if !has_recipient
                    || (kind == 1 && !policy_columns.is_empty())
                    || (kind == 2 && policy_columns.is_empty())
                    || !matches!(kind, 1 | 2)
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                if subjects.is_empty() && !collect_all_policy_inputs {
                    continue;
                }
                let using = using_id
                    .map(|expression_id| {
                        self.read_bound_policy_expression(
                            expression_id,
                            ExpressionPurpose::PolicyUsing,
                            schema,
                            boolean_type_id,
                        )
                    })
                    .transpose()?;
                let with_check = check_id
                    .map(|expression_id| {
                        self.read_bound_policy_expression(
                            expression_id,
                            ExpressionPurpose::PolicyCheck,
                            schema,
                            boolean_type_id,
                        )
                    })
                    .transpose()?;
                if let Some(expression) = &using {
                    collect_expression_fields(expression, &mut referenced_ordinals);
                }
                if let Some(expression) = &with_check {
                    collect_expression_fields(expression, &mut referenced_ordinals);
                }
                if subjects.is_empty() {
                    continue;
                }
                if subjects.len() > crate::authorization::MAX_POLICY_SUBJECTS {
                    return Err(TransactionError::CorruptCatalog);
                }
                let targets = if kind == 1 {
                    vec![AccessTarget::Table(schema.table_id)]
                } else {
                    policy_columns
                        .into_iter()
                        .map(|column_id| AccessTarget::Column {
                            table_id: schema.table_id,
                            column_id,
                        })
                        .collect()
                };
                for target in targets {
                    loaded.push(Policy {
                        subjects: subjects.clone(),
                        operation,
                        target,
                        effect,
                        using: using.clone(),
                        with_check: with_check.clone(),
                    });
                    if loaded.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                        return Err(TransactionError::CorruptCatalog);
                    }
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
            Ok((loaded, policy_driving_columns))
        })();
        match result {
            Ok(policies) => Ok(policies),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Load the table's pinned row constraints. CHECK expressions are bound to
    /// the catalog BOOLEAN type, and PRIMARY KEY/UNIQUE constraints are checked
    /// against their backing unique index. Unsupported kinds fail closed.
    pub fn load_catalog_check_constraints(
        &mut self,
        schema: &CatalogTableSchema,
    ) -> Result<Vec<Expr>, TransactionError> {
        let result = (|| {
            self.load_catalog_foreign_keys_for_table(schema)?;
            let constraint_ids = self.scan_constraint_ids_for_table(schema.table_id)?;
            let records = if constraint_ids.is_empty() {
                // Retain the bounded low-level path for snapshots whose
                // family-8 rows predate derived table-constraint indexes.
                self.scan_catalog_family(8)?
            } else {
                constraint_ids
                    .into_iter()
                    .map(|constraint_id| {
                        self.read_catalog_record(&catalog_id_key(8, constraint_id))?
                            .ok_or(TransactionError::CorruptCatalog)
                    })
                    .collect::<Result<Vec<_>, _>>()?
            };
            let mut checks = Vec::new();
            for record in records {
                if catalog_id(&record, 1)? != schema.table_id {
                    continue;
                }
                let key = record.logical_key.clone();
                if key.len() != 19 {
                    return Err(TransactionError::CorruptCatalog);
                }
                let guarded = self
                    .read_catalog_record(&key)?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if guarded != record {
                    return Err(TransactionError::CorruptCatalog);
                }
                let kind = catalog_u16(&record, 2)?;
                let source_columns = catalog_id_list(&record, 3)?;
                let referenced_columns = catalog_id_list(&record, 6)?;
                let expression_id = catalog_optional_id(&record, 4)?;
                let constraint_id: [u8; 16] = key[3..19]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if constraint_id == [0; 16] {
                    return Err(TransactionError::CorruptCatalog);
                }
                if kind == 1 {
                    if source_columns.len() != 1
                        || expression_id.is_some()
                        || catalog_optional_id(&record, 5)?.is_some()
                        || !referenced_columns.is_empty()
                        || catalog_u16(&record, 7)? != 0
                        || catalog_u16(&record, 8)? != 0
                        || catalog_u16(&record, 9)? != 0
                        || catalog_u16(&record, 10)? != 1
                        || catalog_optional_id(&record, 11)?.is_some()
                    {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    let Some(ordinal) = schema
                        .column_ids
                        .iter()
                        .position(|column_id| *column_id == source_columns[0])
                    else {
                        return Err(TransactionError::CorruptCatalog);
                    };
                    if schema.columns[ordinal].nullable {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    continue;
                }
                if matches!(kind, 2 | 3) {
                    let backing_index_id = catalog_optional_id(&record, 11)?
                        .ok_or(TransactionError::CorruptCatalog)?;
                    let mut seen_columns = HashSet::new();
                    if source_columns.is_empty()
                        || source_columns.len() > crate::tuple::MAX_COLUMNS
                        || source_columns.iter().any(|column_id| {
                            *column_id == [0; 16]
                                || !seen_columns.insert(*column_id)
                                || !schema.column_ids.contains(column_id)
                        })
                        || expression_id.is_some()
                        || catalog_optional_id(&record, 5)?.is_some()
                        || !referenced_columns.is_empty()
                        || catalog_u16(&record, 7)? != 0
                        || catalog_u16(&record, 8)? != 0
                        || catalog_u16(&record, 9)? != 0
                        || catalog_u16(&record, 10)? != 1
                        || backing_index_id == [0; 16]
                    {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    if kind == 2
                        && source_columns.iter().any(|column_id| {
                            let ordinal = schema
                                .column_ids
                                .iter()
                                .position(|schema_column_id| schema_column_id == column_id);
                            ordinal.is_none_or(|ordinal| schema.columns[ordinal].nullable)
                        })
                    {
                        return Err(TransactionError::CorruptCatalog);
                    }

                    let backing_index = self
                        .read_catalog_record(&catalog_id_key(9, backing_index_id))?
                        .ok_or(TransactionError::CorruptCatalog)?;
                    let index_columns = catalog_id_list(&backing_index, 2)?;
                    if backing_index.family_code != 9
                        || catalog_id(&backing_index, 1)? != schema.table_id
                        || index_columns != source_columns
                        || catalog_u16(&backing_index, 3)? != 1
                        || catalog_field(&backing_index, 4)? != [1]
                        || catalog_u16(&backing_index, 5)? != 1
                        || catalog_optional_id(&backing_index, 6)? != Some(constraint_id)
                        || catalog_id(&backing_index, 7)? == [0; 16]
                        || catalog_u16(&backing_index, 8)? != 1
                        || catalog_u32(&backing_index, 9)? != 1
                        || catalog_u64(&backing_index, 10)? != schema.schema_version
                    {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    continue;
                }
                if kind != 4 {
                    if kind == 5 {
                        continue;
                    }
                    return Err(TransactionError::CorruptCatalog);
                }
                let expression_id = expression_id.ok_or(TransactionError::CorruptCatalog)?;
                if !source_columns.is_empty()
                    || !referenced_columns.is_empty()
                    || catalog_optional_id(&record, 5)?.is_some()
                    || catalog_u16(&record, 7)? != 0
                    || catalog_u16(&record, 8)? != 0
                    || catalog_u16(&record, 9)? != 0
                    || catalog_u16(&record, 10)? != 1
                    || catalog_optional_id(&record, 11)?.is_some()
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                let boolean_type_id = self.load_catalog_builtin_type_id(1)?;
                checks.push(self.read_bound_policy_expression(
                    expression_id,
                    ExpressionPurpose::Check,
                    schema,
                    boolean_type_id,
                )?);
                if checks.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                    return Err(TransactionError::CorruptCatalog);
                }
            }
            Ok(checks)
        })();
        match result {
            Ok(checks) => Ok(checks),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }

    /// Read and decode one schema-bound expression from the pinned catalog,
    /// retaining its exact bytes as a final-commit dependency guard.
    pub fn read_catalog_expression(
        &mut self,
        expression_id: [u8; 16],
    ) -> Result<Option<CatalogExpression>, TransactionError> {
        let mut key = 7u16.to_be_bytes().to_vec();
        key.push(1);
        key.extend_from_slice(&expression_id);
        let Some(record) = self.read_catalog_record(&key)? else {
            return Ok(None);
        };
        let expression = decode_catalog_expression_record(&record)
            .map_err(|_| TransactionError::CorruptCatalog)?;
        if expression.expression_id != expression_id {
            self.abort()?;
            return Err(TransactionError::CorruptCatalog);
        }
        Ok(Some(expression))
    }

    pub(super) fn read_bound_policy_expression(
        &mut self,
        expression_id: [u8; 16],
        purpose: ExpressionPurpose,
        schema: &CatalogTableSchema,
        boolean_type_id: [u8; 16],
    ) -> Result<Expr, TransactionError> {
        let expression = self
            .read_catalog_expression(expression_id)?
            .ok_or(TransactionError::CorruptCatalog)?;
        if expression.purpose != purpose
            || (purpose == ExpressionPurpose::Check
                && !is_row_local_expression(&expression.expression))
        {
            return Err(TransactionError::CorruptCatalog);
        }
        validate_expression_binding(
            &expression,
            &schema.columns,
            &schema.column_ids,
            boolean_type_id,
            ScalarType::Boolean,
        )
        .map_err(|_| TransactionError::CorruptCatalog)?;
        Ok(expression.expression)
    }

    /// Load and schema-bind a bounded set of expressions from the pinned
    /// catalog. Each record must match its purpose, result type, and stable
    /// dependencies before its executable form is returned.
    pub fn read_catalog_bound_expressions(
        &mut self,
        expression_ids: &[[u8; 16]],
        expected_purpose: ExpressionPurpose,
        columns: &[ColumnSchema],
        column_ids: &[[u8; 16]],
        expected_result_type_ids: &[[u8; 16]],
        expected_result_types: &[ScalarType],
    ) -> Result<Vec<Expr>, TransactionError> {
        let result = (|| {
            if expression_ids.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                return Err(TransactionError::InvalidContext);
            }
            if expression_ids.len() != expected_result_type_ids.len()
                || expression_ids.len() != expected_result_types.len()
            {
                return Err(TransactionError::InvalidContext);
            }
            let mut expressions = Vec::with_capacity(expression_ids.len());
            for (ordinal, expression_id) in expression_ids.iter().enumerate() {
                let expression = self
                    .read_catalog_expression(*expression_id)?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if expression.purpose != expected_purpose
                    || (expected_purpose == ExpressionPurpose::Check
                        && !is_row_local_expression(&expression.expression))
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                validate_expression_binding(
                    &expression,
                    columns,
                    column_ids,
                    expected_result_type_ids[ordinal],
                    expected_result_types[ordinal],
                )
                .map_err(|_| TransactionError::CorruptCatalog)?;
                expressions.push(expression.expression);
            }
            Ok(expressions)
        })();
        match result {
            Ok(expressions) => Ok(expressions),
            Err(error) => {
                if !self.aborted {
                    self.abort()?;
                }
                Err(error)
            }
        }
    }
}
pub(super) fn derive_catalog_index_key(
    schema: &CatalogTableSchema,
    index: &CatalogIndexDefinition,
    row_id: [u8; 16],
    payload: &[u8],
) -> Result<crate::index_key::DerivedIndexKey, CommitError> {
    derive_index_key(
        index.index_id,
        row_id,
        schema.schema_version,
        &schema.column_ids,
        &schema.columns,
        payload,
        &index.column_ids,
    )
    .map_err(|error| match error {
        DeriveIndexKeyError::InvalidDefinition => CommitError::InvalidCatalog,
        DeriveIndexKeyError::Tuple(_) => CommitError::InvalidRow,
    })
}

pub(super) fn validate_catalog_index_deltas(
    snapshot: &RowSnapshot,
    loaded_schemas: &HashMap<[u8; 16], CatalogTableSchema>,
    loaded_indexes: &HashMap<[u8; 16], Vec<CatalogIndexDefinition>>,
    staged_indexes: &HashMap<Vec<u8>, StagedIndexChange>,
    row_writes: &[RowWrite],
    max_index_changes: usize,
    max_index_bytes: usize,
) -> Result<Vec<IndexWrite>, CommitError> {
    for row_write in row_writes {
        let table_id: [u8; 16] = row_write.key[..16]
            .try_into()
            .map_err(|_| CommitError::InvalidWrite)?;
        if loaded_schemas.contains_key(&table_id) && !loaded_indexes.contains_key(&table_id) {
            return Err(CommitError::InvalidIndex);
        }
        if let Some(schema) = loaded_schemas.get(&table_id) {
            if schema.table_id != table_id {
                return Err(CommitError::InvalidCatalog);
            }
            if let Some(payload) = row_write.payload.as_deref() {
                decode_tuple(&schema.columns, schema.schema_version, payload)
                    .map_err(|_| CommitError::InvalidRow)?;
            }
        }
    }
    let tracked_tables = loaded_indexes.keys().copied().collect::<HashSet<_>>();
    let mut expected = HashMap::<Vec<u8>, IndexWrite>::new();

    for row_write in row_writes {
        let table_id: [u8; 16] = row_write.key[..16]
            .try_into()
            .map_err(|_| CommitError::InvalidWrite)?;
        let Some(indexes) = loaded_indexes.get(&table_id) else {
            continue;
        };
        let schema = loaded_schemas
            .get(&table_id)
            .ok_or(CommitError::InvalidCatalog)?;
        let row_id: [u8; 16] = row_write.key[16..]
            .try_into()
            .map_err(|_| CommitError::InvalidWrite)?;
        let previous = snapshot
            .get(&row_write.key)
            .map_err(|error| CommitError::Storage(error.to_string()))?
            .map(|bytes| RowRecord::decode(&bytes).map_err(|_| CommitError::InvalidRow))
            .transpose()?;
        if previous.as_ref().map(|record| record.row_version) != row_write.expected_version {
            return Err(CommitError::Conflict);
        }
        if let Some(record) = &previous {
            decode_tuple(&schema.columns, schema.schema_version, &record.payload)
                .map_err(|_| CommitError::InvalidRow)?;
        }

        for index in indexes {
            if index.table_id != table_id {
                return Err(CommitError::InvalidCatalog);
            }
            let old = previous
                .as_ref()
                .map(|record| derive_catalog_index_key(schema, index, row_id, &record.payload))
                .transpose()?;
            let new = row_write
                .payload
                .as_deref()
                .map(|payload| derive_catalog_index_key(schema, index, row_id, payload))
                .transpose()?;
            if old.as_ref().map(|derived| &derived.key) == new.as_ref().map(|derived| &derived.key)
            {
                if let Some(record) = &previous {
                    let version = snapshot
                        .get_index_version(&old.as_ref().ok_or(CommitError::InvalidIndex)?.key)
                        .map_err(|error| CommitError::Storage(error.to_string()))?;
                    if version != Some(record.row_version) {
                        return Err(CommitError::InvalidIndex);
                    }
                }
                continue;
            }

            if let (Some(record), Some(derived)) = (&previous, &old) {
                let version = snapshot
                    .get_index_version(&derived.key)
                    .map_err(|error| CommitError::Storage(error.to_string()))?;
                if version != Some(record.row_version) {
                    return Err(CommitError::InvalidIndex);
                }
                let write = IndexWrite {
                    key: derived.key.clone(),
                    row_key: row_write.key,
                    expected_version: version,
                    present: false,
                    unique: index.unique,
                    null_exempt: index.unique && derived.has_null,
                };
                if expected.insert(write.key.clone(), write).is_some() {
                    return Err(CommitError::InvalidIndex);
                }
            }
            if let Some(derived) = &new {
                let version = snapshot
                    .get_index_version(&derived.key)
                    .map_err(|error| CommitError::Storage(error.to_string()))?;
                if version.is_some() {
                    return Err(CommitError::InvalidIndex);
                }
                let write = IndexWrite {
                    key: derived.key.clone(),
                    row_key: row_write.key,
                    expected_version: None,
                    present: true,
                    unique: index.unique,
                    null_exempt: index.unique && derived.has_null,
                };
                if expected.insert(write.key.clone(), write).is_some() {
                    return Err(CommitError::InvalidIndex);
                }
            }
        }
    }

    let mut untracked_writes = Vec::new();
    for (key, staged) in staged_indexes {
        if !tracked_tables.contains(&staged.row_id.table) {
            untracked_writes.push(IndexWrite {
                key: key.clone(),
                row_key: row_key(staged.row_id),
                expected_version: staged.expected_version,
                present: staged.present,
                unique: staged.unique,
                null_exempt: staged.null_exempt,
            });
            continue;
        }
        let write = IndexWrite {
            key: key.clone(),
            row_key: row_key(staged.row_id),
            expected_version: staged.expected_version,
            present: staged.present,
            unique: staged.unique,
            null_exempt: staged.null_exempt,
        };
        if expected.get(key) != Some(&write) {
            return Err(CommitError::InvalidIndex);
        }
    }
    let mut writes = expected.into_values().collect::<Vec<_>>();
    writes.extend(untracked_writes);
    let total_bytes = writes
        .iter()
        .try_fold(0usize, |total, write| total.checked_add(write.key.len()));
    if writes.len() > max_index_changes || total_bytes.is_none_or(|bytes| bytes > max_index_bytes) {
        return Err(CommitError::IndexOverlayLimitExceeded);
    }
    Ok(writes)
}

pub(super) fn catalog_field(record: &CatalogRecord, tag: u16) -> Result<&[u8], TransactionError> {
    record
        .fields
        .iter()
        .find(|field| field.tag == tag)
        .map(|field| field.payload.as_slice())
        .ok_or(TransactionError::CorruptCatalog)
}

pub(super) fn catalog_u16(record: &CatalogRecord, tag: u16) -> Result<u16, TransactionError> {
    Ok(u16::from_le_bytes(
        catalog_field(record, tag)?
            .try_into()
            .map_err(|_| TransactionError::CorruptCatalog)?,
    ))
}

pub(super) fn catalog_u32(record: &CatalogRecord, tag: u16) -> Result<u32, TransactionError> {
    Ok(u32::from_le_bytes(
        catalog_field(record, tag)?
            .try_into()
            .map_err(|_| TransactionError::CorruptCatalog)?,
    ))
}

pub(super) fn catalog_u64(record: &CatalogRecord, tag: u16) -> Result<u64, TransactionError> {
    Ok(u64::from_le_bytes(
        catalog_field(record, tag)?
            .try_into()
            .map_err(|_| TransactionError::CorruptCatalog)?,
    ))
}

pub(super) fn catalog_id(record: &CatalogRecord, tag: u16) -> Result<[u8; 16], TransactionError> {
    catalog_field(record, tag)?
        .try_into()
        .map_err(|_| TransactionError::CorruptCatalog)
}

pub(super) fn catalog_optional_id(
    record: &CatalogRecord,
    tag: u16,
) -> Result<Option<[u8; 16]>, TransactionError> {
    record
        .fields
        .iter()
        .find(|field| field.tag == tag)
        .map(|field| {
            field
                .payload
                .as_slice()
                .try_into()
                .map_err(|_| TransactionError::CorruptCatalog)
        })
        .transpose()
}

pub(super) fn catalog_id_list(
    record: &CatalogRecord,
    tag: u16,
) -> Result<Vec<[u8; 16]>, TransactionError> {
    let bytes = catalog_field(record, tag)?;
    if bytes.len() < 4 {
        return Err(TransactionError::CorruptCatalog);
    }
    let count = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
    if count > crate::authorization::MAX_AUTHORIZATION_INPUTS || bytes.len() != 4 + count * 16 {
        return Err(TransactionError::CorruptCatalog);
    }
    bytes[4..]
        .chunks_exact(16)
        .map(|id| id.try_into().map_err(|_| TransactionError::CorruptCatalog))
        .collect()
}

pub(super) fn catalog_id_key(family: u16, id: [u8; 16]) -> Vec<u8> {
    let mut key = family.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id);
    key
}

pub(super) fn catalog_grant_key(
    grantee_id: [u8; 16],
    object_id: [u8; 16],
    privilege: u16,
    target_scope: u16,
    target_id: [u8; 16],
) -> Vec<u8> {
    let mut key = 12u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&grantee_id);
    key.push(1);
    key.extend_from_slice(&object_id);
    key.push(1);
    key.extend_from_slice(&privilege.to_be_bytes());
    key.push(1);
    key.extend_from_slice(&target_scope.to_be_bytes());
    key.push(1);
    key.extend_from_slice(&target_id);
    key
}

pub(super) fn decode_catalog_scalar_type(
    type_record: &CatalogRecord,
    column_record: &CatalogRecord,
) -> Result<ScalarType, TransactionError> {
    decode_catalog_scalar_type_parameters(type_record, catalog_field(column_record, 5)?)
}

pub(super) fn decode_catalog_scalar_type_parameters(
    type_record: &CatalogRecord,
    parameters: &[u8],
) -> Result<ScalarType, TransactionError> {
    decode_builtin_scalar_type(type_record, parameters)
        .map_err(|_| TransactionError::CorruptCatalog)
}
