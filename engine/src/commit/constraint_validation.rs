use super::*;
use crate::authorization::{AuthorizationContext, check_constraint_passes};
use crate::catalog::{
    decode_builtin_scalar_type, decode_registered_envelope, validate_builtin_type_profile,
};
use crate::expression::{
    ExpressionPurpose, decode_catalog_expression_record, is_row_local_expression,
    validate_expression_binding,
};
use crate::row_record::RowRecord;
use crate::schema::{CATALOG, ROWS};
use crate::tuple::{ColumnSchema, ScalarType, ScalarValue, decode_tuple, encode_tuple};
use redb::{Database, ReadableDatabase, ReadableTable, WriteTransaction};
use std::collections::{HashMap, HashSet};

pub(super) fn validate_foreign_key_constraints(
    transaction: &WriteTransaction,
    writes: &[RowWrite],
    foreign_keys: &[ForeignKeyCheck],
    force_validation: bool,
) -> Result<(), CommitError> {
    let mut row_visits = 0usize;
    let write_keys = writes.iter().map(|write| write.key).collect::<HashSet<_>>();
    for foreign_key in foreign_keys {
        let source_touched = writes
            .iter()
            .any(|write| write.key[..16] == foreign_key.source_table_id);
        let target_touched = writes
            .iter()
            .any(|write| write.key[..16] == foreign_key.target_table_id);
        if !force_validation && !source_touched && !target_touched {
            continue;
        }
        validate_foreign_key_definition(foreign_key)?;
        let mut comparison_key_bytes = 0usize;
        let target_keys = collect_final_foreign_key_keys(
            transaction,
            writes,
            foreign_key.target_table_id,
            foreign_key.target_schema_version,
            &foreign_key.target_schema,
            &foreign_key.target_ordinals,
            &foreign_key.target_ordinals,
            &foreign_key.target_schema,
            &mut row_visits,
            &mut comparison_key_bytes,
            &write_keys,
        )?;
        let source_keys = collect_final_foreign_key_keys(
            transaction,
            writes,
            foreign_key.source_table_id,
            foreign_key.source_schema_version,
            &foreign_key.source_schema,
            &foreign_key.source_ordinals,
            &foreign_key.target_ordinals,
            &foreign_key.target_schema,
            &mut row_visits,
            &mut comparison_key_bytes,
            &write_keys,
        )?;
        if source_keys.iter().any(|key| !target_keys.contains(key)) {
            return Err(CommitError::ForeignKeyViolation);
        }
    }
    Ok(())
}

/// Check generated UUID values against the final table contents inside the
/// serialized commit transaction, including concurrent inserts.
pub(super) fn validate_generated_identity_collisions(
    transaction: &WriteTransaction,
    writes: &[RowWrite],
    catalog_writes: &[CatalogWrite],
) -> Result<(), CommitError> {
    let touched_tables = writes
        .iter()
        .map(|write| {
            write.key[..16]
                .try_into()
                .map_err(|_| CommitError::InvalidWrite)
        })
        .collect::<Result<HashSet<[u8; 16]>, _>>()?;
    if touched_tables.is_empty() {
        return Ok(());
    }

    let catalog = transaction.open_table(CATALOG).map_err(storage_error)?;
    for table_id in touched_tables {
        let prefix = 6u16.to_be_bytes();
        let mut columns = HashMap::<Vec<u8>, (usize, u16)>::new();
        for entry in catalog.range(prefix.as_slice()..).map_err(storage_error)? {
            let (key, value) = entry.map_err(storage_error)?;
            let key = key.value();
            if !key.starts_with(&prefix) {
                break;
            }
            if catalog_writes.iter().any(|write| write.key == key) {
                continue;
            }
            let bytes = value.value();
            if column_table_id(key, bytes)? == table_id {
                let ordinal = u32::from_le_bytes(
                    catalog_field_bytes(key, bytes, 6, 2)?
                        .try_into()
                        .map_err(|_| CommitError::InvalidCatalog)?,
                ) as usize;
                let mode = u16::from_le_bytes(
                    catalog_field_bytes(key, bytes, 6, 8)?
                        .try_into()
                        .map_err(|_| CommitError::InvalidCatalog)?,
                );
                if mode > 1 {
                    return Err(CommitError::InvalidCatalog);
                }
                columns.insert(key.to_vec(), (ordinal, mode));
            }
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&prefix))
        {
            let Some(bytes) = write.payload.as_deref() else {
                continue;
            };
            if column_table_id(&write.key, bytes)? == table_id {
                let ordinal = u32::from_le_bytes(
                    catalog_field_bytes(&write.key, bytes, 6, 2)?
                        .try_into()
                        .map_err(|_| CommitError::InvalidCatalog)?,
                ) as usize;
                let mode = u16::from_le_bytes(
                    catalog_field_bytes(&write.key, bytes, 6, 8)?
                        .try_into()
                        .map_err(|_| CommitError::InvalidCatalog)?,
                );
                if mode > 1 {
                    return Err(CommitError::InvalidCatalog);
                }
                columns.insert(write.key.clone(), (ordinal, mode));
            }
        }
        let mut generated = columns
            .values()
            .filter_map(|(ordinal, mode)| (*mode == 1).then_some(*ordinal))
            .collect::<Vec<_>>();
        generated.sort_unstable();
        generated.dedup();
        if generated.is_empty() {
            continue;
        }
        let schema = load_final_table_schema(&catalog, catalog_writes, table_id)?;
        if generated.iter().any(|ordinal| {
            *ordinal >= schema.columns.len()
                || schema.columns[*ordinal].scalar_type != ScalarType::Uuid
        }) {
            return Err(CommitError::InvalidCatalog);
        }

        let write_keys = writes.iter().map(|write| write.key).collect::<HashSet<_>>();
        let mut values = generated
            .iter()
            .map(|ordinal| (*ordinal, HashSet::<[u8; 16]>::new()))
            .collect::<HashMap<_, _>>();
        let mut visits = 0usize;
        let mut start = [0; 32];
        start[..16].copy_from_slice(&table_id);
        let mut end = [u8::MAX; 32];
        end[..16].copy_from_slice(&table_id);
        let rows = transaction.open_table(ROWS).map_err(storage_error)?;
        for entry in rows
            .range(start.as_slice()..=end.as_slice())
            .map_err(storage_error)?
        {
            let (key, record) = entry.map_err(storage_error)?;
            let row_key: [u8; 32] = key
                .value()
                .try_into()
                .map_err(|_| CommitError::InvalidRow)?;
            visits = visits
                .checked_add(1)
                .ok_or(CommitError::ForeignKeyValidationLimitExceeded)?;
            if visits > MAX_FOREIGN_KEY_ROW_VISITS_PER_COMMIT {
                return Err(CommitError::ForeignKeyValidationLimitExceeded);
            }
            if write_keys.contains(&row_key) {
                continue;
            }
            let row = RowRecord::decode(record.value()).map_err(|_| CommitError::InvalidRow)?;
            let tuple = decode_tuple(&schema.columns, schema.schema_version, &row.payload)
                .map_err(|_| CommitError::InvalidRow)?;
            for ordinal in &generated {
                let Some(Some(ScalarValue::Uuid(value))) = tuple.values.get(*ordinal) else {
                    return Err(CommitError::InvalidRow);
                };
                if !values.get_mut(ordinal).unwrap().insert(*value) {
                    return Err(CommitError::UniqueViolation);
                }
            }
        }
        drop(rows);
        for write in writes.iter().filter(|write| write.key[..16] == table_id) {
            let Some(payload) = write.payload.as_deref() else {
                continue;
            };
            let tuple = decode_tuple(&schema.columns, schema.schema_version, payload)
                .map_err(|_| CommitError::InvalidRow)?;
            for ordinal in &generated {
                let Some(Some(ScalarValue::Uuid(value))) = tuple.values.get(*ordinal) else {
                    return Err(CommitError::InvalidRow);
                };
                if !values.get_mut(ordinal).unwrap().insert(*value) {
                    return Err(CommitError::UniqueViolation);
                }
            }
        }
    }
    Ok(())
}

pub(super) fn load_final_table_schema<T>(
    catalog: &T,
    writes: &[CatalogWrite],
    table_id: [u8; 16],
) -> Result<FinalTableSchema, CommitError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let table_key = id_catalog_key(5, table_id);
    let table_bytes =
        final_catalog_value(catalog, writes, &table_key)?.ok_or(CommitError::InvalidCatalog)?;
    let schema_version = u64::from_le_bytes(
        catalog_field_bytes(&table_key, &table_bytes, 5, 2)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?,
    );
    if schema_version == 0 {
        return Err(CommitError::InvalidCatalog);
    }

    let prefix = 6u16.to_be_bytes();
    let mut count = 0usize;
    let mut columns = Vec::new();
    for entry in catalog.range(prefix.as_slice()..).map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        if writes.iter().any(|write| write.key == key) {
            continue;
        }
        count += 1;
        if count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
            return Err(CommitError::InvalidCatalog);
        }
        if column_table_id(key, value.value())? == table_id {
            columns.push(decode_final_column(catalog, writes, key, value.value())?);
        }
    }
    for write in writes.iter().filter(|write| write.key.starts_with(&prefix)) {
        let Some(bytes) = &write.payload else {
            continue;
        };
        count += 1;
        if count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
            return Err(CommitError::InvalidCatalog);
        }
        if column_table_id(&write.key, bytes)? == table_id {
            columns.push(decode_final_column(catalog, writes, &write.key, bytes)?);
        }
    }
    if columns.len() > crate::tuple::MAX_COLUMNS {
        return Err(CommitError::InvalidCatalog);
    }
    columns.sort_by_key(|(ordinal, _, _)| *ordinal);
    let mut column_ids = Vec::with_capacity(columns.len());
    let mut schemas = Vec::with_capacity(columns.len());
    for (expected_ordinal, (ordinal, column_id, schema)) in columns.into_iter().enumerate() {
        if ordinal != expected_ordinal {
            return Err(CommitError::InvalidCatalog);
        }
        column_ids.push(column_id);
        schemas.push(schema);
    }
    Ok(FinalTableSchema {
        schema_version,
        column_ids,
        columns: schemas,
    })
}

pub(super) fn decode_final_column<T>(
    catalog: &T,
    writes: &[CatalogWrite],
    key: &[u8],
    bytes: &[u8],
) -> Result<(usize, [u8; 16], ColumnSchema), CommitError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let column_id: [u8; 16] = key
        .get(3..19)
        .ok_or(CommitError::InvalidCatalog)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    if column_id == [0; 16] {
        return Err(CommitError::InvalidCatalog);
    }
    let ordinal = u32::from_le_bytes(
        catalog_field_bytes(key, bytes, 6, 2)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?,
    ) as usize;
    let type_id: [u8; 16] = catalog_field_bytes(key, bytes, 6, 4)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let parameters = catalog_field_bytes(key, bytes, 6, 5)?;
    let nullable = match catalog_field_bytes(key, bytes, 6, 6)?.as_slice() {
        [0] => false,
        [1] => true,
        _ => return Err(CommitError::InvalidCatalog),
    };
    let introduced_schema_version = u64::from_le_bytes(
        catalog_field_bytes(key, bytes, 6, 9)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?,
    );
    let table_id = column_table_id(key, bytes)?;
    let table_key = id_catalog_key(5, table_id);
    let table_bytes =
        final_catalog_value(catalog, writes, &table_key)?.ok_or(CommitError::InvalidCatalog)?;
    let schema_version = u64::from_le_bytes(
        catalog_field_bytes(&table_key, &table_bytes, 5, 2)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?,
    );
    if introduced_schema_version == 0 || introduced_schema_version > schema_version {
        return Err(CommitError::InvalidCatalog);
    }
    let type_key = id_catalog_key(4, type_id);
    let type_bytes =
        final_catalog_value(catalog, writes, &type_key)?.ok_or(CommitError::InvalidCatalog)?;
    let type_record =
        decode_registered_envelope(&type_bytes).map_err(|_| CommitError::InvalidCatalog)?;
    if type_record.family_code != 4 || type_record.logical_key != type_key {
        return Err(CommitError::InvalidCatalog);
    }
    let scalar_type = decode_builtin_scalar_type(&type_record, &parameters)
        .map_err(|_| CommitError::InvalidCatalog)?;
    Ok((
        ordinal,
        column_id,
        ColumnSchema {
            scalar_type,
            nullable,
        },
    ))
}

pub(super) fn load_new_foreign_key_checks(
    catalog: &redb::Table<'_, &[u8], &[u8]>,
    writes: &[CatalogWrite],
) -> Result<Vec<ForeignKeyCheck>, CommitError> {
    let prefix = 8u16.to_be_bytes();
    if !writes.iter().any(|write| write.key.starts_with(&prefix)) {
        return Ok(Vec::new());
    }
    let mut foreign_key_count = 0usize;
    for entry in catalog.range(prefix.as_slice()..).map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        if writes.iter().any(|write| write.key == key) {
            continue;
        }
        if u16::from_le_bytes(
            catalog_field_bytes(key, value.value(), 8, 2)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?,
        ) == 5
        {
            foreign_key_count += 1;
            if foreign_key_count > 256 {
                return Err(CommitError::InvalidCatalog);
            }
        }
    }
    let mut checks = Vec::new();
    for write in writes.iter().filter(|write| write.key.starts_with(&prefix)) {
        let Some(bytes) = write.payload.as_deref() else {
            continue;
        };
        if u16::from_le_bytes(
            catalog_field_bytes(&write.key, bytes, 8, 2)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?,
        ) != 5
        {
            continue;
        }
        foreign_key_count += 1;
        if foreign_key_count > 256 {
            return Err(CommitError::InvalidCatalog);
        }
        if write.expected_value == write.payload {
            continue;
        }
        let source_table_id: [u8; 16] = catalog_field_bytes(&write.key, bytes, 8, 1)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let source_column_ids = catalog_id_list_field(&write.key, bytes, 8, 3)?;
        let target_constraint_id: [u8; 16] = catalog_field_bytes(&write.key, bytes, 8, 5)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let target_column_ids = catalog_id_list_field(&write.key, bytes, 8, 6)?;
        let target_constraint_key = id_catalog_key(8, target_constraint_id);
        let target_constraint_bytes = final_catalog_value(catalog, writes, &target_constraint_key)?
            .ok_or(CommitError::InvalidCatalog)?;
        let target_table_id: [u8; 16] =
            catalog_field_bytes(&target_constraint_key, &target_constraint_bytes, 8, 1)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
        if source_column_ids.is_empty() || source_column_ids.len() != target_column_ids.len() {
            return Err(CommitError::InvalidCatalog);
        }
        let source = load_final_table_schema(catalog, writes, source_table_id)?;
        let target = load_final_table_schema(catalog, writes, target_table_id)?;
        let source_ordinals = source_column_ids
            .iter()
            .map(|column_id| {
                source
                    .column_ids
                    .iter()
                    .position(|candidate| candidate == column_id)
                    .ok_or(CommitError::InvalidCatalog)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let target_ordinals = target_column_ids
            .iter()
            .map(|column_id| {
                target
                    .column_ids
                    .iter()
                    .position(|candidate| candidate == column_id)
                    .ok_or(CommitError::InvalidCatalog)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if source_ordinals
            .iter()
            .zip(&target_ordinals)
            .any(|(source_ordinal, target_ordinal)| {
                source.columns[*source_ordinal].scalar_type
                    != target.columns[*target_ordinal].scalar_type
            })
        {
            return Err(CommitError::InvalidCatalog);
        }
        checks.push(ForeignKeyCheck {
            source_table_id,
            source_schema_version: source.schema_version,
            source_schema: source.columns,
            source_ordinals,
            target_table_id,
            target_schema_version: target.schema_version,
            target_schema: target.columns,
            target_ordinals,
        });
    }
    Ok(checks)
}

pub(super) fn validate_foreign_key_definition(
    foreign_key: &ForeignKeyCheck,
) -> Result<(), CommitError> {
    if foreign_key.source_table_id == [0; 16]
        || foreign_key.target_table_id == [0; 16]
        || foreign_key.source_schema_version == 0
        || foreign_key.target_schema_version == 0
        || foreign_key.source_ordinals.is_empty()
        || foreign_key.source_ordinals.len() != foreign_key.target_ordinals.len()
        || foreign_key
            .source_ordinals
            .iter()
            .any(|ordinal| *ordinal >= foreign_key.source_schema.len())
        || foreign_key
            .target_ordinals
            .iter()
            .any(|ordinal| *ordinal >= foreign_key.target_schema.len())
        || foreign_key
            .source_ordinals
            .iter()
            .zip(&foreign_key.target_ordinals)
            .any(|(source, target)| {
                foreign_key.source_schema[*source].scalar_type
                    != foreign_key.target_schema[*target].scalar_type
            })
        || foreign_key
            .source_ordinals
            .iter()
            .collect::<HashSet<_>>()
            .len()
            != foreign_key.source_ordinals.len()
        || foreign_key
            .target_ordinals
            .iter()
            .collect::<HashSet<_>>()
            .len()
            != foreign_key.target_ordinals.len()
    {
        return Err(CommitError::InvalidCatalog);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn collect_final_foreign_key_keys(
    transaction: &WriteTransaction,
    writes: &[RowWrite],
    table_id: [u8; 16],
    schema_version: u64,
    schema: &[ColumnSchema],
    ordinals: &[usize],
    comparison_ordinals: &[usize],
    comparison_schema: &[ColumnSchema],
    row_visits: &mut usize,
    comparison_key_bytes: &mut usize,
    write_keys: &HashSet<[u8; 32]>,
) -> Result<HashSet<Vec<u8>>, CommitError> {
    let comparison_columns = comparison_ordinals
        .iter()
        .map(|ordinal| {
            comparison_schema
                .get(*ordinal)
                .map(|column| ColumnSchema {
                    scalar_type: column.scalar_type,
                    nullable: true,
                })
                .ok_or(CommitError::InvalidCatalog)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut keys = HashSet::new();
    let mut start = [0; 32];
    start[..16].copy_from_slice(&table_id);
    let mut end = [u8::MAX; 32];
    end[..16].copy_from_slice(&table_id);
    let table = transaction.open_table(ROWS).map_err(storage_error)?;
    for entry in table
        .range(start.as_slice()..=end.as_slice())
        .map_err(storage_error)?
    {
        let (key, value) = entry.map_err(storage_error)?;
        let row_key: [u8; 32] = key
            .value()
            .try_into()
            .map_err(|_| CommitError::InvalidRow)?;
        *row_visits = row_visits
            .checked_add(1)
            .ok_or(CommitError::ForeignKeyValidationLimitExceeded)?;
        if *row_visits > MAX_FOREIGN_KEY_ROW_VISITS_PER_COMMIT {
            return Err(CommitError::ForeignKeyValidationLimitExceeded);
        }
        if write_keys.contains(&row_key) {
            continue;
        }
        let row = RowRecord::decode(value.value()).map_err(|_| CommitError::InvalidRow)?;
        let tuple = decode_tuple(schema, schema_version, &row.payload)
            .map_err(|_| CommitError::InvalidRow)?;
        if let Some(key) = foreign_key_tuple_key(&tuple.values, ordinals, &comparison_columns)? {
            remember_foreign_key_key(&mut keys, key, comparison_key_bytes)?;
        }
    }
    drop(table);

    for write in writes.iter().filter(|write| write.key[..16] == table_id) {
        *row_visits = row_visits
            .checked_add(1)
            .ok_or(CommitError::ForeignKeyValidationLimitExceeded)?;
        if *row_visits > MAX_FOREIGN_KEY_ROW_VISITS_PER_COMMIT {
            return Err(CommitError::ForeignKeyValidationLimitExceeded);
        }
        let Some(payload) = write.payload.as_deref() else {
            continue;
        };
        let tuple =
            decode_tuple(schema, schema_version, payload).map_err(|_| CommitError::InvalidRow)?;
        if let Some(key) = foreign_key_tuple_key(&tuple.values, ordinals, &comparison_columns)? {
            remember_foreign_key_key(&mut keys, key, comparison_key_bytes)?;
        }
    }
    Ok(keys)
}

pub(super) fn remember_foreign_key_key(
    keys: &mut HashSet<Vec<u8>>,
    key: Vec<u8>,
    comparison_key_bytes: &mut usize,
) -> Result<(), CommitError> {
    if keys.contains(&key) {
        return Ok(());
    }
    let next_bytes = comparison_key_bytes
        .checked_add(key.len())
        .filter(|bytes| *bytes <= MAX_FOREIGN_KEY_COMPARISON_KEY_BYTES_PER_COMMIT)
        .ok_or(CommitError::ForeignKeyValidationLimitExceeded)?;
    keys.insert(key);
    *comparison_key_bytes = next_bytes;
    Ok(())
}

pub(super) fn foreign_key_tuple_key(
    values: &[Option<crate::tuple::ScalarValue>],
    ordinals: &[usize],
    comparison_columns: &[ColumnSchema],
) -> Result<Option<Vec<u8>>, CommitError> {
    let projected = ordinals
        .iter()
        .map(|ordinal| {
            values
                .get(*ordinal)
                .cloned()
                .ok_or(CommitError::InvalidCatalog)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if projected.iter().any(Option::is_none) {
        return Ok(None);
    }
    encode_tuple(comparison_columns, 1, &projected)
        .map(Some)
        .map_err(|_| CommitError::ForeignKeyViolation)
}

pub(super) fn build_constraint_validation_rows(
    database: &Database,
    catalog_writes: &[CatalogWrite],
    row_writes: &[RowWrite],
) -> Result<ExpectedTableRows, CommitError> {
    let column_prefix = 6u16.to_be_bytes();
    let constraint_prefix = 8u16.to_be_bytes();
    if !catalog_writes.iter().any(|write| {
        write.key.starts_with(&column_prefix) || write.key.starts_with(&constraint_prefix)
    }) {
        return Ok(HashMap::new());
    }

    let read = database.begin_read().map_err(storage_error)?;
    let catalog = read.open_table(CATALOG).map_err(storage_error)?;
    let rows = read.open_table(ROWS).map_err(storage_error)?;
    let mut changed_columns = HashMap::<[u8; 16], HashSet<[u8; 16]>>::new();
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&column_prefix))
    {
        if write.key.len() != 19 {
            return Err(CommitError::InvalidCatalog);
        }
        let current = catalog
            .get(write.key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        let Some(current) = current else {
            continue;
        };
        let Some(next) = write.payload.as_deref() else {
            continue;
        };
        if catalog_field_bytes(&write.key, &current, 6, 6)? == [1]
            && catalog_field_bytes(&write.key, next, 6, 6)? == [0]
        {
            let table_id = column_table_id(&write.key, &current)?;
            if column_table_id(&write.key, next)? != table_id {
                return Err(CommitError::InvalidCatalog);
            }
            let column_id: [u8; 16] = write.key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            changed_columns
                .entry(table_id)
                .or_default()
                .insert(column_id);
        }
    }
    let mut changed_checks = HashMap::<[u8; 16], Vec<[u8; 16]>>::new();
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&constraint_prefix))
    {
        if write.key.len() != 19 {
            return Err(CommitError::InvalidCatalog);
        }
        let current = catalog
            .get(write.key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if current.as_deref() == write.payload.as_deref() {
            continue;
        }
        let Some(next) = write.payload.as_deref() else {
            continue;
        };
        if catalog_field_u16(&write.key, next, 8, 2)? == 4 {
            let constraint_table_id = constraint_table_id(&write.key, next)?;
            let expression_id: [u8; 16] = catalog_field_bytes(&write.key, next, 8, 4)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            if expression_id == [0; 16] {
                return Err(CommitError::InvalidCatalog);
            }
            changed_checks
                .entry(constraint_table_id)
                .or_default()
                .push(expression_id);
        }
    }
    if changed_columns.is_empty() && changed_checks.is_empty() {
        return Ok(HashMap::new());
    }
    let check_count = changed_checks
        .values()
        .try_fold(0usize, |count, checks| count.checked_add(checks.len()));
    if check_count.is_none_or(|count| count > crate::authorization::MAX_AUTHORIZATION_INPUTS) {
        return Err(CommitError::ConstraintValidationLimitExceeded);
    }

    let has_check_validation = !changed_checks.is_empty();
    let limit_error = if has_check_validation {
        CommitError::ConstraintValidationLimitExceeded
    } else {
        CommitError::NotNullValidationLimitExceeded
    };
    let mut affected_tables = changed_columns.keys().copied().collect::<HashSet<_>>();
    affected_tables.extend(changed_checks.keys().copied());
    let mut validation_rows = HashMap::with_capacity(affected_tables.len());
    let mut visited = 0usize;
    let mut accounted_bytes = 0usize;
    let mut check_evaluations = 0usize;
    let check_context = AuthorizationContext {
        principal_id: [0; 16],
        role_ids: HashSet::new(),
        scope: HashSet::new(),
    };
    for table_id in affected_tables {
        let column_ids = changed_columns.get(&table_id).cloned().unwrap_or_default();
        let check_expression_ids = changed_checks.get(&table_id).cloned().unwrap_or_default();
        if column_ids.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
            || check_expression_ids.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
            || row_writes
                .iter()
                .any(|write| write.key.get(..16) == Some(table_id.as_slice()))
        {
            return Err(CommitError::InvalidCatalog);
        }
        let table_key = id_catalog_key(5, table_id);
        let table_existed = catalog
            .get(table_key.as_slice())
            .map_err(storage_error)?
            .is_some();
        if !table_existed && !column_ids.is_empty() {
            return Err(CommitError::InvalidCatalog);
        }
        let mut persisted_rows = Vec::new();
        let mut start = [0; 32];
        start[..16].copy_from_slice(&table_id);
        let mut end = [u8::MAX; 32];
        end[..16].copy_from_slice(&table_id);
        for entry in rows
            .range(start.as_slice()..=end.as_slice())
            .map_err(storage_error)?
        {
            let (key, value) = entry.map_err(storage_error)?;
            if key.value().len() != 32 || key.value()[..16] != table_id {
                return Err(CommitError::InvalidRow);
            }
            if !table_existed {
                return Err(CommitError::InvalidCatalog);
            }
            visited = visited.checked_add(1).ok_or(limit_error.clone())?;
            accounted_bytes = accounted_bytes
                .checked_add(value.value().len())
                .ok_or(limit_error.clone())?;
            if visited > MAX_NOT_NULL_VALIDATION_ROWS_PER_COMMIT
                || accounted_bytes > MAX_INDEX_DDL_BYTES_PER_COMMIT
            {
                return Err(limit_error.clone());
            }
            persisted_rows.push((
                key.value()
                    .try_into()
                    .map_err(|_| CommitError::InvalidRow)?,
                RowRecord::decode(value.value()).map_err(|_| CommitError::InvalidRow)?,
            ));
        }
        if persisted_rows.is_empty() && check_expression_ids.is_empty() {
            validation_rows.insert(table_id, Vec::new());
            continue;
        }

        let next_schema = load_final_table_schema(&catalog, catalog_writes, table_id)?;
        let old_schema = if table_existed {
            load_final_table_schema(&catalog, &[], table_id)?
        } else {
            next_schema.clone()
        };
        if old_schema.column_ids != next_schema.column_ids
            || old_schema.columns.len() != next_schema.columns.len()
            || old_schema.schema_version != next_schema.schema_version
        {
            return Err(CommitError::InvalidCatalog);
        }
        let mut ordinals = Vec::with_capacity(column_ids.len());
        for column_id in &column_ids {
            let ordinal = old_schema
                .column_ids
                .iter()
                .position(|candidate| *candidate == *column_id)
                .ok_or(CommitError::InvalidCatalog)?;
            if !old_schema.columns[ordinal].nullable
                || next_schema.columns[ordinal].nullable
                || old_schema.columns[ordinal].scalar_type
                    != next_schema.columns[ordinal].scalar_type
            {
                return Err(CommitError::InvalidCatalog);
            }
            ordinals.push(ordinal);
        }
        if old_schema
            .columns
            .iter()
            .zip(&next_schema.columns)
            .enumerate()
            .any(|(ordinal, (old, next))| {
                old.scalar_type != next.scalar_type
                    || (old.nullable != next.nullable && !ordinals.contains(&ordinal))
            })
        {
            return Err(CommitError::InvalidCatalog);
        }

        let mut checks = Vec::with_capacity(check_expression_ids.len());
        for expression_id in check_expression_ids {
            let expression_key = id_catalog_key(7, expression_id);
            let expression_bytes = final_catalog_value(&catalog, catalog_writes, &expression_key)?
                .ok_or(CommitError::InvalidCatalog)?;
            let expression_record = decode_catalog_record(&expression_key, &expression_bytes)?;
            let expression = decode_catalog_expression_record(&expression_record)
                .map_err(|_| CommitError::InvalidCatalog)?;
            if expression.expression_id != expression_id
                || expression.purpose != ExpressionPurpose::Check
                || !is_row_local_expression(&expression.expression)
            {
                return Err(CommitError::InvalidCatalog);
            }
            let type_key = id_catalog_key(4, expression.result_type_id);
            let type_bytes = final_catalog_value(&catalog, catalog_writes, &type_key)?
                .ok_or(CommitError::InvalidCatalog)?;
            let type_record = decode_catalog_record(&type_key, &type_bytes)?;
            if validate_builtin_type_profile(&type_record)
                .map_err(|_| CommitError::InvalidCatalog)?
                != 1
            {
                return Err(CommitError::InvalidCatalog);
            }
            validate_expression_binding(
                &expression,
                &next_schema.columns,
                &next_schema.column_ids,
                expression.result_type_id,
                ScalarType::Boolean,
            )
            .map_err(|_| CommitError::InvalidCatalog)?;
            checks.push(expression.expression);
        }

        let mut expected_rows = Vec::with_capacity(persisted_rows.len());
        for (key, row) in persisted_rows {
            let tuple = decode_tuple(&old_schema.columns, old_schema.schema_version, &row.payload)
                .map_err(|_| CommitError::InvalidRow)?;
            if ordinals
                .iter()
                .any(|ordinal| !matches!(tuple.values.get(*ordinal), Some(Some(_))))
            {
                return Err(CommitError::NotNullViolation);
            }
            for expression in &checks {
                check_evaluations = check_evaluations
                    .checked_add(1)
                    .ok_or(CommitError::ConstraintValidationLimitExceeded)?;
                if check_evaluations > MAX_CHECK_VALIDATION_EVALUATIONS_PER_COMMIT {
                    return Err(CommitError::ConstraintValidationLimitExceeded);
                }
                if !check_constraint_passes(expression, &tuple.values, &check_context)
                    .map_err(|_| CommitError::InvalidCatalog)?
                {
                    return Err(CommitError::CheckViolation);
                }
            }
            expected_rows.push((key, row.row_version));
        }
        validation_rows.insert(table_id, expected_rows);
    }
    Ok(validation_rows)
}
