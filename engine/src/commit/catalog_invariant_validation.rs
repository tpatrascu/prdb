use super::*;
use crate::authorization::{MAX_MEMBERSHIP_EDGES, Membership};
use crate::catalog::{decode_builtin_scalar_type, decode_registered_envelope};
use crate::credentials::decode_verifier_record;
use crate::expression::{
    ExpressionPurpose, decode_catalog_expression_record, is_row_local_expression,
    validate_expression_binding,
};
use crate::schema::CATALOG;
use crate::tuple::ScalarType;
use redb::{ReadableTable, WriteTransaction};
use std::collections::{HashMap, HashSet};

pub(super) fn is_normalized_constraint_name(name: &[u8]) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && (name[0].is_ascii_lowercase() || name[0] == b'_')
        && name
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
}

/// Require the final set of user and role names to be unique in their shared
/// database namespace. The DDL layer supplies already-normalized names.
pub(super) fn validate_final_principal_names(
    transaction: &WriteTransaction,
    catalog_writes: &[CatalogWrite],
) -> Result<(), CommitError> {
    let prefix = 10u16.to_be_bytes();
    if !catalog_writes
        .iter()
        .any(|write| write.key.starts_with(&prefix))
    {
        return Ok(());
    }

    let catalog = transaction.open_table(CATALOG).map_err(storage_error)?;
    let name_index = transaction
        .open_table(crate::schema::PRINCIPAL_NAMES)
        .map_err(storage_error)?;
    let principal_writes = catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&prefix))
        .collect::<Vec<_>>();
    let changed_ids = principal_writes
        .iter()
        .map(|write| {
            write
                .key
                .get(3..19)
                .and_then(|id| id.try_into().ok())
                .ok_or(CommitError::InvalidCatalog)
        })
        .collect::<Result<HashSet<[u8; 16]>, _>>()?;

    for write in &principal_writes {
        if let Some(current) = catalog.get(write.key.as_slice()).map_err(storage_error)? {
            let principal_id: [u8; 16] = write
                .key
                .get(3..19)
                .and_then(|id| id.try_into().ok())
                .ok_or(CommitError::InvalidCatalog)?;
            let name = catalog_field_bytes(&write.key, current.value(), 10, 2)?;
            if name_index
                .get(name.as_slice())
                .map_err(storage_error)?
                .is_none_or(|indexed_id| indexed_id.value() != principal_id)
            {
                return Err(CommitError::InvalidCatalog);
            }
        }
    }

    let mut names = HashSet::<Vec<u8>>::new();
    for write in principal_writes {
        if write.key.len() != 19 {
            return Err(CommitError::InvalidCatalog);
        }
        let Some(bytes) = &write.payload else {
            continue;
        };
        decode_catalog_record(&write.key, bytes)?;
        let name = catalog_field_bytes(&write.key, bytes, 10, 2)?;
        if !names.insert(name) {
            return Err(CommitError::InvalidCatalog);
        }
    }
    for name in &names {
        if let Some(indexed_id) = name_index.get(name.as_slice()).map_err(storage_error)? {
            let indexed_id: [u8; 16] = indexed_id
                .value()
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            if !changed_ids.contains(&indexed_id) {
                return Err(CommitError::InvalidCatalog);
            }
        }
    }
    Ok(())
}

/// Keep user-defined schema names unique when schema objects are created or changed.
pub(super) fn validate_final_schema_names(
    transaction: &WriteTransaction,
    catalog_writes: &[CatalogWrite],
) -> Result<(), CommitError> {
    if !catalog_writes.iter().any(|write| {
        write.key.starts_with(&2u16.to_be_bytes())
            && write
                .payload
                .as_deref()
                .is_some_and(|bytes| catalog_field_u16(&write.key, bytes, 2, 1).ok() == Some(1))
    }) {
        return Ok(());
    }
    let table = transaction.open_table(CATALOG).map_err(storage_error)?;
    let changed = catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&2u16.to_be_bytes()))
        .map(|write| write.key.as_slice())
        .collect::<HashSet<_>>();
    let prefix = 2u16.to_be_bytes();
    let mut names = HashSet::<Vec<u8>>::new();
    let mut count = 0usize;
    for entry in table.range(prefix.as_slice()..).map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        if changed.contains(key) || key.len() != 19 {
            continue;
        }
        if catalog_field_u16(key, value.value(), 2, 1)? != 1 {
            continue;
        }
        count += 1;
        if count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN
            || !names.insert(catalog_field_bytes(key, value.value(), 2, 3)?)
        {
            return Err(CommitError::InvalidCatalog);
        }
    }
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&prefix))
    {
        let Some(bytes) = &write.payload else {
            continue;
        };
        if write.key.len() != 19 || catalog_field_u16(&write.key, bytes, 2, 1)? != 1 {
            continue;
        }
        count += 1;
        if count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN
            || !names.insert(catalog_field_bytes(&write.key, bytes, 2, 3)?)
        {
            return Err(CommitError::InvalidCatalog);
        }
    }
    Ok(())
}

pub(super) fn validate_final_schema_search_paths(
    transaction: &WriteTransaction,
    catalog_writes: &[CatalogWrite],
) -> Result<(), CommitError> {
    if !catalog_writes.iter().any(|write| {
        write.key.len() >= 2 && matches!(u16::from_be_bytes([write.key[0], write.key[1]]), 2 | 3)
    }) {
        return Ok(());
    }

    let catalog = transaction.open_table(CATALOG).map_err(storage_error)?;
    let prefix = 3u16.to_be_bytes();
    let mut schema_keys = Vec::<Vec<u8>>::new();
    for entry in catalog.range(prefix.as_slice()..).map_err(storage_error)? {
        let (key, _) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        if catalog_writes.iter().any(|write| write.key == key) {
            continue;
        }
        schema_keys.push(key.to_vec());
        if schema_keys.len() > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
            return Err(CommitError::InvalidCatalog);
        }
    }
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&prefix) && write.payload.is_some())
    {
        schema_keys.push(write.key.clone());
        if schema_keys.len() > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
            return Err(CommitError::InvalidCatalog);
        }
    }

    for schema_key in schema_keys {
        let schema_bytes = final_catalog_value(&catalog, catalog_writes, &schema_key)?
            .ok_or(CommitError::InvalidCatalog)?;
        let search_path = catalog_id_list_field(&schema_key, &schema_bytes, 3, 1)?;
        for schema_id in search_path {
            let target_schema_key = id_catalog_key(3, schema_id);
            if final_catalog_value(&catalog, catalog_writes, &target_schema_key)?.is_none() {
                return Err(CommitError::InvalidCatalog);
            }
            let object_key = id_catalog_key(2, schema_id);
            let Some(object_bytes) = final_catalog_value(&catalog, catalog_writes, &object_key)?
            else {
                return Err(CommitError::InvalidCatalog);
            };
            if catalog_field_u16(&object_key, &object_bytes, 2, 1)? != 1 {
                return Err(CommitError::InvalidCatalog);
            }
        }
    }
    Ok(())
}

/// Keep table names unique within each schema across concurrent DDL commits.
pub(super) fn validate_final_table_names(
    transaction: &WriteTransaction,
    catalog_writes: &[CatalogWrite],
) -> Result<(), CommitError> {
    if !catalog_writes.iter().any(|write| {
        write.key.starts_with(&2u16.to_be_bytes())
            && write
                .payload
                .as_deref()
                .is_some_and(|bytes| catalog_field_u16(&write.key, bytes, 2, 1).ok() == Some(3))
    }) {
        return Ok(());
    }
    let table = transaction.open_table(CATALOG).map_err(storage_error)?;
    let changed = catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&2u16.to_be_bytes()))
        .map(|write| write.key.as_slice())
        .collect::<HashSet<_>>();
    let prefix = 2u16.to_be_bytes();
    let mut names = HashSet::<(Vec<u8>, Vec<u8>)>::new();
    let mut count = 0usize;
    for entry in table.range(prefix.as_slice()..).map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        if changed.contains(key) || key.len() != 19 {
            continue;
        }
        let bytes = value.value();
        if catalog_field_u16(key, bytes, 2, 1)? != 3 {
            continue;
        }
        let object_id: [u8; 16] = key[3..19]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let table_key = id_catalog_key(5, object_id);
        let table_record = table
            .get(table_key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        let schema_id = catalog_field_bytes(&table_key, &table_record, 5, 1)?;
        count += 1;
        let name = catalog_field_bytes(key, bytes, 2, 3)?;
        if count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN || !names.insert((schema_id, name)) {
            return Err(CommitError::InvalidCatalog);
        }
    }
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&prefix))
    {
        let Some(bytes) = &write.payload else {
            continue;
        };
        if write.key.len() != 19 || catalog_field_u16(&write.key, bytes, 2, 1)? != 3 {
            continue;
        }
        count += 1;
        let schema_id = catalog_field_bytes(&write.key, bytes, 2, 2)?;
        let name = catalog_field_bytes(&write.key, bytes, 2, 3)?;
        if count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN || !names.insert((schema_id, name)) {
            return Err(CommitError::InvalidCatalog);
        }
    }
    Ok(())
}

/// Keep column names unique within each table across concurrent DDL commits.
pub(super) fn validate_final_column_names(
    transaction: &WriteTransaction,
    catalog_writes: &[CatalogWrite],
) -> Result<(), CommitError> {
    if !catalog_writes.iter().any(|write| {
        write.key.starts_with(&6u16.to_be_bytes())
            && write
                .payload
                .as_deref()
                .is_some_and(|bytes| catalog_field_bytes(&write.key, bytes, 6, 3).is_ok())
    }) {
        return Ok(());
    }
    let catalog = transaction.open_table(CATALOG).map_err(storage_error)?;
    let prefix = 6u16.to_be_bytes();
    let changed = catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&prefix))
        .map(|write| write.key.as_slice())
        .collect::<HashSet<_>>();
    let mut names = HashSet::<([u8; 16], Vec<u8>)>::new();
    let mut scanned = 0usize;
    for entry in catalog.range(prefix.as_slice()..).map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        if changed.contains(key) || key.len() != 19 {
            continue;
        }
        scanned += 1;
        if scanned > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
            return Err(CommitError::InvalidCatalog);
        }
        let bytes = value.value();
        let table_id = column_table_id(key, bytes)?;
        let name = catalog_field_bytes(key, bytes, 6, 3)?;
        if !names.insert((table_id, name)) {
            return Err(CommitError::InvalidCatalog);
        }
    }
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&prefix))
    {
        let Some(bytes) = &write.payload else {
            continue;
        };
        if write.key.len() != 19 {
            return Err(CommitError::InvalidCatalog);
        }
        scanned += 1;
        if scanned > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
            return Err(CommitError::InvalidCatalog);
        }
        let table_id = column_table_id(&write.key, bytes)?;
        let name = catalog_field_bytes(&write.key, bytes, 6, 3)?;
        if !names.insert((table_id, name)) {
            return Err(CommitError::InvalidCatalog);
        }
    }
    Ok(())
}

/// Revalidate surviving constraints when definitions they reference can change.
/// Stream the current family and overlay staged changes so deletion of a
/// referenced object cannot leave an unchanged constraint dangling.
pub(super) fn validate_final_constraints(
    catalog: &redb::Table<'_, &[u8], &[u8]>,
    writes: &[CatalogWrite],
) -> Result<(), CommitError> {
    if !writes.iter().any(|write| {
        write.key.len() >= 2 && (5..=9).contains(&u16::from_be_bytes([write.key[0], write.key[1]]))
    }) {
        return Ok(());
    }
    let prefix = 8u16.to_be_bytes();
    let mut count = 0usize;
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
        validate_constraint_shape(key, value.value())?;
        validate_constraint_references(catalog, writes, key, value.value())?;
    }
    for write in writes.iter().filter(|write| write.key.starts_with(&prefix)) {
        if let Some(bytes) = &write.payload {
            count += 1;
            if count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
                return Err(CommitError::InvalidCatalog);
            }
            validate_constraint_shape(&write.key, bytes)?;
            validate_constraint_references(catalog, writes, &write.key, bytes)?;
        }
    }
    Ok(())
}

pub(super) fn validate_constraint_shape(key: &[u8], bytes: &[u8]) -> Result<(), CommitError> {
    let record = decode_registered_envelope(bytes).map_err(|_| CommitError::InvalidCatalog)?;
    if record.family_code != 8 || record.logical_key != key {
        return Err(CommitError::InvalidCatalog);
    }
    let field = |tag: u16| {
        record
            .fields
            .iter()
            .find(|candidate| candidate.tag == tag)
            .map(|candidate| candidate.payload.as_slice())
            .ok_or(CommitError::InvalidCatalog)
    };
    let optional = |tag: u16| {
        record
            .fields
            .iter()
            .find(|candidate| candidate.tag == tag)
            .map(|candidate| candidate.payload.as_slice())
    };
    let read_u16 = |tag| -> Result<u16, CommitError> {
        Ok(u16::from_le_bytes(
            field(tag)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?,
        ))
    };
    let read_ids = |tag| -> Result<Vec<[u8; 16]>, CommitError> {
        let bytes = field(tag)?;
        if bytes.len() < 4 {
            return Err(CommitError::InvalidCatalog);
        }
        let count = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        if count > 256 || bytes.len() != 4 + count * 16 {
            return Err(CommitError::InvalidCatalog);
        }
        bytes[4..]
            .chunks_exact(16)
            .map(|value| value.try_into().map_err(|_| CommitError::InvalidCatalog))
            .collect()
    };
    let kind = read_u16(2)?;
    let source_columns = read_ids(3)?;
    let referenced_columns = read_ids(6)?;
    let referenced_constraint = optional(5).is_some();
    let expression = optional(4).is_some();
    let backing_index = optional(11).is_some();
    let actions = [read_u16(7)?, read_u16(8)?, read_u16(9)?];
    let valid = read_u16(10)? == 1
        && match kind {
            1 => {
                source_columns.len() == 1
                    && !expression
                    && !referenced_constraint
                    && referenced_columns.is_empty()
                    && !backing_index
                    && actions == [0, 0, 0]
            }
            2 | 3 => {
                !source_columns.is_empty()
                    && !expression
                    && !referenced_constraint
                    && referenced_columns.is_empty()
                    && backing_index
                    && actions == [0, 0, 0]
            }
            4 => {
                source_columns.is_empty()
                    && expression
                    && !referenced_constraint
                    && referenced_columns.is_empty()
                    && !backing_index
                    && actions == [0, 0, 0]
            }
            5 => {
                !source_columns.is_empty()
                    && referenced_columns.len() == source_columns.len()
                    && !expression
                    && referenced_constraint
                    && actions == [1, 1, 1]
            }
            _ => false,
        };
    if valid {
        Ok(())
    } else {
        Err(CommitError::InvalidCatalog)
    }
}

pub(super) fn validate_final_identity_tombstones(
    transaction: &WriteTransaction,
    catalog_writes: &[CatalogWrite],
) -> Result<(), CommitError> {
    if !catalog_writes.iter().any(|write| {
        write.key.len() >= 2
            && matches!(
                u16::from_be_bytes([write.key[0], write.key[1]]),
                2 | 10 | 16 | 17
            )
    }) {
        return Ok(());
    }

    let table = transaction.open_table(CATALOG).map_err(storage_error)?;
    let collect_final_ids = |family: u16| -> Result<HashSet<[u8; 16]>, CommitError> {
        let prefix = family.to_be_bytes();
        let mut ids = HashSet::new();
        let mut count = 0usize;
        for entry in table.range(prefix.as_slice()..).map_err(storage_error)? {
            let (key, value) = entry.map_err(storage_error)?;
            let key = key.value();
            if !key.starts_with(&prefix) {
                break;
            }
            if catalog_writes.iter().any(|write| write.key == key) {
                continue;
            }
            count += 1;
            if count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN || key.len() != 19 || key[2] != 1 {
                return Err(CommitError::InvalidCatalog);
            }
            decode_catalog_record(key, value.value())?;
            let id = key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            ids.insert(id);
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&prefix))
        {
            if write.key.len() != 19 || write.key[2] != 1 {
                return Err(CommitError::InvalidCatalog);
            }
            let Some(bytes) = &write.payload else {
                continue;
            };
            count += 1;
            if count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
                return Err(CommitError::InvalidCatalog);
            }
            decode_catalog_record(&write.key, bytes)?;
            let id = write.key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            ids.insert(id);
        }
        Ok(ids)
    };

    let object_ids = collect_final_ids(2)?;
    let principal_ids = collect_final_ids(10)?;
    let prefix = 17u16.to_be_bytes();
    let mut tombstone_ids = HashSet::<[u8; 16]>::new();
    let mut tombstone_count = 0usize;
    for entry in table.range(prefix.as_slice()..).map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        if catalog_writes.iter().any(|write| write.key == key) {
            continue;
        }
        tombstone_count += 1;
        if tombstone_count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN
            || key.len() != 19
            || key[2] != 1
        {
            return Err(CommitError::InvalidCatalog);
        }
        decode_catalog_record(key, value.value())?;
        let id = key[3..19]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        tombstone_ids.insert(id);
    }
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&prefix))
    {
        if write.expected_value.is_some() || write.payload.is_none() {
            return Err(CommitError::InvalidCatalog);
        }
        let bytes = write
            .payload
            .as_deref()
            .ok_or(CommitError::InvalidCatalog)?;
        if write.key.len() != 19 || write.key[2] != 1 {
            return Err(CommitError::InvalidCatalog);
        }
        decode_catalog_record(&write.key, bytes)?;
        tombstone_count += 1;
        if tombstone_count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
            return Err(CommitError::InvalidCatalog);
        }
        let id = write.key[3..19]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        if !tombstone_ids.insert(id) {
            return Err(CommitError::InvalidCatalog);
        }
    }
    if tombstone_ids
        .iter()
        .any(|id| object_ids.contains(id) || principal_ids.contains(id))
    {
        return Err(CommitError::InvalidCatalog);
    }

    for write in catalog_writes {
        if write.key.starts_with(&10u16.to_be_bytes()) && write.payload.is_none() {
            let id: [u8; 16] = write
                .key
                .get(3..19)
                .and_then(|bytes| bytes.try_into().ok())
                .ok_or(CommitError::InvalidCatalog)?;
            if !tombstone_ids.contains(&id) {
                return Err(CommitError::InvalidCatalog);
            }
        }
        if write.key.starts_with(&2u16.to_be_bytes()) && write.payload.is_none() {
            let current = table
                .get(write.key.as_slice())
                .map_err(storage_error)?
                .ok_or(CommitError::InvalidCatalog)?
                .value()
                .to_vec();
            let kind = catalog_field_u16(&write.key, &current, 2, 1)?;
            if matches!(kind, 1 | 3..=10) {
                let id: [u8; 16] = write
                    .key
                    .get(3..19)
                    .and_then(|bytes| bytes.try_into().ok())
                    .ok_or(CommitError::InvalidCatalog)?;
                if !tombstone_ids.contains(&id) {
                    return Err(CommitError::InvalidCatalog);
                }
            }
        }
    }
    Ok(())
}

pub(super) fn validate_final_credential_verifiers(
    transaction: &WriteTransaction,
    catalog_writes: &[CatalogWrite],
) -> Result<(), CommitError> {
    if !catalog_writes.iter().any(|write| {
        write.key.starts_with(&10u16.to_be_bytes()) || write.key.starts_with(&16u16.to_be_bytes())
    }) {
        return Ok(());
    }

    let table = transaction.open_table(CATALOG).map_err(storage_error)?;
    let prefix = 16u16.to_be_bytes();
    let mut verifiers = HashMap::<Vec<u8>, Vec<u8>>::new();
    for entry in table.range(prefix.as_slice()..).map_err(storage_error)? {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value().to_vec();
        if !key.starts_with(&prefix) {
            break;
        }
        verifiers.insert(key, value.value().to_vec());
        if verifiers.len() > 65_536 {
            return Err(CommitError::InvalidCatalog);
        }
    }
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&prefix))
    {
        if let Some(payload) = &write.payload {
            verifiers.insert(write.key.clone(), payload.clone());
        } else {
            verifiers.remove(&write.key);
        }
    }
    if verifiers.len() > 65_536 {
        return Err(CommitError::InvalidCatalog);
    }

    for (key, bytes) in verifiers {
        let record = decode_registered_envelope(&bytes).map_err(|_| CommitError::InvalidCatalog)?;
        if record.logical_key != key {
            return Err(CommitError::InvalidCatalog);
        }
        let next = decode_verifier_record(&record).map_err(|_| CommitError::InvalidCatalog)?;
        let principal_key = id_catalog_key(10, next.user_id);
        match final_catalog_value(&table, catalog_writes, &principal_key)? {
            Some(principal_bytes) => {
                if decode_principal_kind(&principal_key, &principal_bytes)? != 1 {
                    return Err(CommitError::InvalidCatalog);
                }
                let login_enabled = catalog_field_bytes(&principal_key, &principal_bytes, 10, 3)?;
                if !matches!(login_enabled.as_slice(), [0] | [1])
                    || (!next.revoked && login_enabled != [1])
                {
                    return Err(CommitError::InvalidCatalog);
                }
            }
            None => {
                let tombstone_key = id_catalog_key(17, next.user_id);
                if !next.revoked
                    || final_catalog_value(&table, catalog_writes, &tombstone_key)?.is_none()
                {
                    return Err(CommitError::InvalidCatalog);
                }
            }
        }

        if catalog_writes.iter().any(|write| write.key == key)
            && let Some(current_bytes) = table
                .get(&key[..])
                .map_err(storage_error)?
                .map(|value| value.value().to_vec())
        {
            let current_record = decode_registered_envelope(&current_bytes)
                .map_err(|_| CommitError::InvalidCatalog)?;
            let current =
                decode_verifier_record(&current_record).map_err(|_| CommitError::InvalidCatalog)?;
            if current.user_id != next.user_id
                || current.verifier != next.verifier
                || current.issued_at_us != next.issued_at_us
                || current.expires_at_us != next.expires_at_us
                || current.label != next.label
                || (current.revoked && !next.revoked)
                || (current.revoked && current.revoked_at_us != next.revoked_at_us)
                || (current_record != record && next.authority_epoch <= current.authority_epoch)
            {
                return Err(CommitError::InvalidCatalog);
            }
        }
    }
    Ok(())
}

pub(super) fn validate_final_admin_credential(
    transaction: &WriteTransaction,
    catalog_writes: &[CatalogWrite],
) -> Result<(), CommitError> {
    let now_us =
        crate::credentials::now_unix_micros().ok_or(CommitError::DdlAuthorizationDenied)?;
    let membership_prefix = 11u16.to_be_bytes();
    let mut memberships = {
        let catalog = transaction.open_table(CATALOG).map_err(storage_error)?;
        let mut memberships = HashMap::<Vec<u8>, Membership>::new();
        for entry in catalog
            .range(membership_prefix.as_slice()..)
            .map_err(storage_error)?
        {
            let (key, value) = entry.map_err(storage_error)?;
            let key = key.value().to_vec();
            if !key.starts_with(&membership_prefix) {
                break;
            }
            memberships.insert(key.clone(), decode_membership_record(&key, value.value())?);
        }
        memberships
    };
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&membership_prefix) && write.payload.is_none())
    {
        memberships.remove(&write.key);
    }
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&membership_prefix) && write.payload.is_some())
    {
        let bytes = write
            .payload
            .as_deref()
            .ok_or(CommitError::InvalidCatalog)?;
        memberships.insert(
            write.key.clone(),
            decode_membership_record(&write.key, bytes)?,
        );
    }
    if memberships.len() > MAX_MEMBERSHIP_EDGES {
        return Err(CommitError::InvalidCatalog);
    }
    let membership_pairs = memberships
        .values()
        .map(|edge| (edge.member_id, edge.role_id))
        .collect::<HashSet<_>>();
    let mut admin_role_id = [0x80; 16];
    admin_role_id[15] = 3;
    let admin_members = members_in_role_closure(&membership_pairs, admin_role_id);

    let login_admins = {
        let catalog = transaction.open_table(CATALOG).map_err(storage_error)?;
        let mut login_admins = HashSet::new();
        for admin_id in admin_members {
            let key = id_catalog_key(10, admin_id);
            let Some(bytes) = final_catalog_value(&catalog, catalog_writes, &key)? else {
                return Err(CommitError::InvalidCatalog);
            };
            if decode_principal_kind(&key, &bytes)? == 1 {
                let login = catalog_field_bytes(&key, &bytes, 10, 3)?;
                if !matches!(login.as_slice(), [0] | [1]) {
                    return Err(CommitError::InvalidCatalog);
                }
                if login == [1] {
                    login_admins.insert(admin_id);
                }
            }
        }
        login_admins
    };
    if login_admins.is_empty() {
        return Err(CommitError::DdlAuthorizationDenied);
    }

    let verifier_prefix = 16u16.to_be_bytes();
    let mut verifiers = {
        let catalog = transaction.open_table(CATALOG).map_err(storage_error)?;
        let mut verifiers = HashMap::<Vec<u8>, Vec<u8>>::new();
        for entry in catalog
            .range(verifier_prefix.as_slice()..)
            .map_err(storage_error)?
        {
            let (key, value) = entry.map_err(storage_error)?;
            let key = key.value().to_vec();
            if !key.starts_with(&verifier_prefix) {
                break;
            }
            verifiers.insert(key, value.value().to_vec());
            if verifiers.len() > 65_536 {
                return Err(CommitError::InvalidCatalog);
            }
        }
        verifiers
    };
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&verifier_prefix))
    {
        if let Some(payload) = &write.payload {
            verifiers.insert(write.key.clone(), payload.clone());
        } else {
            verifiers.remove(&write.key);
        }
    }
    if verifiers.len() > 65_536 {
        return Err(CommitError::InvalidCatalog);
    }
    for (key, bytes) in verifiers {
        let record = decode_registered_envelope(&bytes).map_err(|_| CommitError::InvalidCatalog)?;
        if record.logical_key != key {
            return Err(CommitError::InvalidCatalog);
        }
        let verifier = decode_verifier_record(&record).map_err(|_| CommitError::InvalidCatalog)?;
        if login_admins.contains(&verifier.user_id) && verifier.is_valid_at(now_us) {
            return Ok(());
        }
    }
    Err(CommitError::DdlAuthorizationDenied)
}

pub(super) fn validate_constraint_references(
    catalog: &redb::Table<'_, &[u8], &[u8]>,
    writes: &[CatalogWrite],
    key: &[u8],
    bytes: &[u8],
) -> Result<(), CommitError> {
    let table_id = constraint_table_id(key, bytes)?;
    let table_key = id_catalog_key(5, table_id);
    let Some(table_bytes) = final_catalog_value(catalog, writes, &table_key)? else {
        return Err(CommitError::InvalidCatalog);
    };
    let table_record =
        decode_registered_envelope(&table_bytes).map_err(|_| CommitError::InvalidCatalog)?;
    if table_record.family_code != 5 || table_record.logical_key != table_key {
        return Err(CommitError::InvalidCatalog);
    }
    let kind = u16::from_le_bytes(
        catalog_field_bytes(key, bytes, 8, 2)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?,
    );
    let source_columns = catalog_id_list_field(key, bytes, 8, 3)?;
    let referenced_columns = catalog_id_list_field(key, bytes, 8, 6)?;
    for column_id in &source_columns {
        let column_key = id_catalog_key(6, *column_id);
        let Some(column_bytes) = final_catalog_value(catalog, writes, &column_key)? else {
            return Err(CommitError::InvalidCatalog);
        };
        if column_table_id(&column_key, &column_bytes)? != table_id {
            return Err(CommitError::InvalidCatalog);
        }
        if kind == 2 && catalog_field_bytes(&column_key, &column_bytes, 6, 6)? != [0] {
            return Err(CommitError::InvalidCatalog);
        }
    }
    if kind == 1 {
        let column_key = id_catalog_key(6, source_columns[0]);
        if catalog_field_bytes(
            &column_key,
            &final_catalog_value(catalog, writes, &column_key)?
                .ok_or(CommitError::InvalidCatalog)?,
            6,
            6,
        )? != [0]
        {
            return Err(CommitError::InvalidCatalog);
        }
    } else if kind == 2 || kind == 3 {
        let constraint_id: [u8; 16] = key[3..19]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let index_id: [u8; 16] = catalog_field_bytes(key, bytes, 8, 11)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let index_key = id_catalog_key(9, index_id);
        let Some(index_bytes) = final_catalog_value(catalog, writes, &index_key)? else {
            return Err(CommitError::InvalidCatalog);
        };
        let index_table: [u8; 16] = catalog_field_bytes(&index_key, &index_bytes, 9, 1)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let index_columns = catalog_id_list_field(&index_key, &index_bytes, 9, 2)?;
        let unique = catalog_field_bytes(&index_key, &index_bytes, 9, 4)? == [1];
        let owner = catalog_field_bytes(&index_key, &index_bytes, 9, 6)?;
        let state = u16::from_le_bytes(
            catalog_field_bytes(&index_key, &index_bytes, 9, 8)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?,
        );
        if index_table != table_id
            || index_columns != source_columns
            || !unique
            || owner != constraint_id
            || state != 1
        {
            return Err(CommitError::InvalidCatalog);
        }
    } else if kind == 4 {
        let expression_id: [u8; 16] = catalog_field_bytes(key, bytes, 8, 4)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let expression_key = id_catalog_key(7, expression_id);
        let Some(expression_bytes) = final_catalog_value(catalog, writes, &expression_key)? else {
            return Err(CommitError::InvalidCatalog);
        };
        let expression_record = decode_catalog_record(&expression_key, &expression_bytes)?;
        let expression = decode_catalog_expression_record(&expression_record)
            .map_err(|_| CommitError::InvalidCatalog)?;
        if expression.expression_id != expression_id
            || expression.purpose != ExpressionPurpose::Check
            || !is_row_local_expression(&expression.expression)
        {
            return Err(CommitError::InvalidCatalog);
        }
        let schema = load_final_table_schema(catalog, writes, table_id)?;
        let result_type_key = id_catalog_key(4, expression.result_type_id);
        let result_type_bytes = final_catalog_value(catalog, writes, &result_type_key)?
            .ok_or(CommitError::InvalidCatalog)?;
        let result_type_record = decode_catalog_record(&result_type_key, &result_type_bytes)?;
        if decode_builtin_scalar_type(&result_type_record, &expression.result_parameters)
            .map_err(|_| CommitError::InvalidCatalog)?
            != ScalarType::Boolean
        {
            return Err(CommitError::InvalidCatalog);
        }
        validate_expression_binding(
            &expression,
            &schema.columns,
            &schema.column_ids,
            expression.result_type_id,
            ScalarType::Boolean,
        )
        .map_err(|_| CommitError::InvalidCatalog)?;
    } else if kind == 5 {
        let target_id: [u8; 16] = catalog_field_bytes(key, bytes, 8, 5)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let target_key = id_catalog_key(8, target_id);
        let Some(target_bytes) = final_catalog_value(catalog, writes, &target_key)? else {
            return Err(CommitError::InvalidCatalog);
        };
        let target_kind = u16::from_le_bytes(
            catalog_field_bytes(&target_key, &target_bytes, 8, 2)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?,
        );
        let target_table_id = constraint_table_id(&target_key, &target_bytes)?;
        let target_columns = catalog_id_list_field(&target_key, &target_bytes, 8, 3)?;
        if !matches!(target_kind, 2 | 3) || target_columns != referenced_columns {
            return Err(CommitError::InvalidCatalog);
        }
        validate_constraint_shape(&target_key, &target_bytes)?;
        validate_constraint_references(catalog, writes, &target_key, &target_bytes)?;
        for column_id in &referenced_columns {
            let column_key = id_catalog_key(6, *column_id);
            let Some(column_bytes) = final_catalog_value(catalog, writes, &column_key)? else {
                return Err(CommitError::InvalidCatalog);
            };
            if column_table_id(&column_key, &column_bytes)? != target_table_id {
                return Err(CommitError::InvalidCatalog);
            }
        }
        for (source_column_id, target_column_id) in source_columns.iter().zip(&referenced_columns) {
            let source_column_key = id_catalog_key(6, *source_column_id);
            let target_column_key = id_catalog_key(6, *target_column_id);
            let source_column = final_catalog_value(catalog, writes, &source_column_key)?
                .ok_or(CommitError::InvalidCatalog)?;
            let target_column = final_catalog_value(catalog, writes, &target_column_key)?
                .ok_or(CommitError::InvalidCatalog)?;
            if catalog_field_bytes(&source_column_key, &source_column, 6, 4)?
                != catalog_field_bytes(&target_column_key, &target_column, 6, 4)?
                || catalog_field_bytes(&source_column_key, &source_column, 6, 5)?
                    != catalog_field_bytes(&target_column_key, &target_column, 6, 5)?
            {
                return Err(CommitError::InvalidCatalog);
            }
        }
    }
    Ok(())
}

pub(super) fn validate_column_schema_epochs(
    catalog: &redb::Table<'_, &[u8], &[u8]>,
    catalog_writes: &[CatalogWrite],
) -> Result<(), CommitError> {
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&5u16.to_be_bytes()))
    {
        let Some(next) = &write.payload else {
            continue;
        };
        let next_schema = table_schema_version(&write.key, next)?;
        let next_write = table_write_epoch(&write.key, next)?;
        let current = catalog
            .get(write.key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if let Some(current) = current
            && current != *next
        {
            let current_schema = table_schema_version(&write.key, &current)?;
            let current_write = table_write_epoch(&write.key, &current)?;
            if next_schema < current_schema
                || next_write < current_write
                || (next_schema == current_schema && next_write == current_write)
            {
                return Err(CommitError::InvalidCatalog);
            }
        }
    }
    let column_prefix = 6u16.to_be_bytes();
    let mut affected_tables = HashMap::<[u8; 16], bool>::new();
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&column_prefix))
    {
        let current = catalog
            .get(&write.key[..])
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if current == write.payload {
            continue;
        }
        if write.key.len() != 19 {
            return Err(CommitError::InvalidCatalog);
        }
        let current_table = current
            .as_deref()
            .map(|bytes| column_table_id(&write.key, bytes))
            .transpose()?;
        let final_table = write
            .payload
            .as_deref()
            .map(|bytes| column_table_id(&write.key, bytes))
            .transpose()?;
        if current_table.is_some() && final_table.is_some() && current_table != final_table {
            return Err(CommitError::InvalidCatalog);
        }
        if let Some(table_id) = current_table.or(final_table) {
            let column_only_metadata_change = match (current.as_deref(), write.payload.as_deref()) {
                (Some(current), Some(next)) => {
                    let current_record = decode_catalog_record(&write.key, current)?;
                    let next_record = decode_catalog_record(&write.key, next)?;
                    let nullability_only = catalog_field_bytes(&write.key, current, 6, 6)?
                        != catalog_field_bytes(&write.key, next, 6, 6)?
                        && catalog_fields_equal_except(&current_record, &next_record, &[6]);
                    let rename_only = catalog_field_bytes(&write.key, current, 6, 3)?
                        != catalog_field_bytes(&write.key, next, 6, 3)?
                        && catalog_fields_equal_except(&current_record, &next_record, &[3]);
                    let current_default = current_record
                        .fields
                        .iter()
                        .find(|field| field.tag == 7)
                        .map(|field| field.payload.as_slice());
                    let next_default = next_record
                        .fields
                        .iter()
                        .find(|field| field.tag == 7)
                        .map(|field| field.payload.as_slice());
                    let default_only = current_default != next_default
                        && catalog_fields_equal_except(&current_record, &next_record, &[7]);
                    let varchar_widening_only = if catalog_field_bytes(&write.key, current, 6, 4)?
                        == catalog_field_bytes(&write.key, next, 6, 4)?
                        && catalog_field_bytes(&write.key, current, 6, 5)?
                            != catalog_field_bytes(&write.key, next, 6, 5)?
                        && catalog_fields_equal_except(&current_record, &next_record, &[5])
                    {
                        let type_id: [u8; 16] = catalog_field_bytes(&write.key, current, 6, 4)?
                            .try_into()
                            .map_err(|_| CommitError::InvalidCatalog)?;
                        let type_key = id_catalog_key(4, type_id);
                        let type_bytes = catalog
                            .get(type_key.as_slice())
                            .map_err(storage_error)?
                            .ok_or(CommitError::InvalidCatalog)?
                            .value()
                            .to_vec();
                        let type_record = decode_catalog_record(&type_key, &type_bytes)?;
                        let old_parameters: [u8; 8] =
                            catalog_field_bytes(&write.key, current, 6, 5)?
                                .try_into()
                                .map_err(|_| CommitError::InvalidCatalog)?;
                        let new_parameters: [u8; 8] = catalog_field_bytes(&write.key, next, 6, 5)?
                            .try_into()
                            .map_err(|_| CommitError::InvalidCatalog)?;
                        matches!(
                            (
                                decode_builtin_scalar_type(&type_record, &old_parameters),
                                decode_builtin_scalar_type(&type_record, &new_parameters),
                            ),
                            (
                                Ok(ScalarType::Varchar { max_chars: old }),
                                Ok(ScalarType::Varchar { max_chars: new }),
                            ) if new > old
                        )
                    } else {
                        false
                    };
                    nullability_only || rename_only || default_only || varchar_widening_only
                }
                _ => false,
            };
            let requires_schema_bump = !column_only_metadata_change;
            affected_tables
                .entry(table_id)
                .and_modify(|required| *required |= requires_schema_bump)
                .or_insert(requires_schema_bump);
        }
    }
    for (table_id, requires_schema_bump) in affected_tables {
        let table_key = catalog_id_key(5, table_id);
        let current = catalog
            .get(&table_key[..])
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        let final_bytes =
            if let Some(write) = catalog_writes.iter().find(|write| write.key == table_key) {
                write.payload.clone()
            } else {
                current.clone()
            };
        if let (Some(current), Some(final_bytes)) = (current, final_bytes) {
            let current_schema = table_schema_version(&table_key, &current)?;
            let next_schema = table_schema_version(&table_key, &final_bytes)?;
            let current_write = table_write_epoch(&table_key, &current)?;
            let next_write = table_write_epoch(&table_key, &final_bytes)?;
            if (requires_schema_bump && next_schema <= current_schema)
                || (!requires_schema_bump
                    && (next_schema != current_schema || next_write <= current_write))
            {
                return Err(CommitError::InvalidCatalog);
            }
        }
    }
    Ok(())
}
