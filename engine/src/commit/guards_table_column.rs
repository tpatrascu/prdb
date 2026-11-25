use super::fenced_ddl::add_catalog_guard;
use super::*;
use crate::audit::AuditEvent;
use crate::authorization::{MAX_MEMBERSHIP_EDGES, resolve_role_closure};
use crate::metadata::EngineMetadata;
use crate::schema::{CATALOG, ENGINE_META};
use redb::{Database, ReadableDatabase};
use std::collections::{HashMap, HashSet};

pub(super) fn table_rename_ddl_authority_guards(
    database: &Database,
    principal_id: [u8; 16],
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
    mutation_kind: TableObjectMutationKind,
) -> Result<Vec<CatalogGuard>, CommitError> {
    if principal_id == [0; 16] || catalog_writes.len() != 2 || audit_events.is_empty() {
        return Err(CommitError::InvalidCatalog);
    }
    let table_write = catalog_writes
        .iter()
        .find(|write| write.key.starts_with(&5u16.to_be_bytes()))
        .ok_or(CommitError::InvalidCatalog)?;
    let table_id: [u8; 16] = table_write
        .key
        .get(3..19)
        .ok_or(CommitError::InvalidCatalog)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let table_key = id_catalog_key(5, table_id);
    let object_key = id_catalog_key(2, table_id);
    let object_write = catalog_writes
        .iter()
        .find(|write| write.key == object_key)
        .ok_or(CommitError::InvalidCatalog)?;
    if table_write.key != table_key
        || table_write.expected_value.is_none()
        || table_write.payload.is_none()
        || object_write.expected_value.is_none()
        || object_write.payload.is_none()
        || catalog_writes
            .iter()
            .any(|write| write.key != table_key && write.key != object_key)
        || audit_events.iter().any(|event| {
            event.actor_id != Some(principal_id)
                || event.action != crate::audit::AuditAction::Ddl
                || event.result != crate::audit::AuditResult::Succeeded
                || event.target_ids.as_slice() != [table_id]
        })
    {
        return Err(CommitError::InvalidAudit);
    }
    let (Some(old_table_bytes), Some(new_table_bytes)) = (
        table_write.expected_value.as_deref(),
        table_write.payload.as_deref(),
    ) else {
        return Err(CommitError::InvalidCatalog);
    };
    let (Some(old_object_bytes), Some(new_object_bytes)) = (
        object_write.expected_value.as_deref(),
        object_write.payload.as_deref(),
    ) else {
        return Err(CommitError::InvalidCatalog);
    };
    let old_table = decode_catalog_record(&table_key, old_table_bytes)?;
    let new_table = decode_catalog_record(&table_key, new_table_bytes)?;
    let old_object = decode_catalog_record(&object_key, old_object_bytes)?;
    let new_object = decode_catalog_record(&object_key, new_object_bytes)?;
    let schema_id: [u8; 16] = catalog_field_bytes(&table_key, old_table_bytes, 5, 1)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let old_name = catalog_field_bytes(&object_key, old_object_bytes, 2, 3)?;
    let new_name = catalog_field_bytes(&object_key, new_object_bytes, 2, 3)?;
    let old_owner_id: [u8; 16] = catalog_field_bytes(&object_key, old_object_bytes, 2, 4)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let new_owner_id: [u8; 16] = catalog_field_bytes(&object_key, new_object_bytes, 2, 4)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let object_mutation_valid = match mutation_kind {
        TableObjectMutationKind::OwnerTransfer => {
            old_owner_id != new_owner_id
                && old_name == new_name
                && catalog_fields_equal_except(&old_object, &new_object, &[4, 5])
        }
        TableObjectMutationKind::Rename => {
            old_name != new_name
                && is_canonical_catalog_name(&new_name)
                && catalog_fields_equal_except(&old_object, &new_object, &[3, 5])
        }
        TableObjectMutationKind::Truncate => {
            old_owner_id == new_owner_id
                && old_name == new_name
                && catalog_fields_equal_except(&old_object, &new_object, &[5])
        }
    };
    if old_table.family_code != 5
        || new_table.family_code != 5
        || old_object.family_code != 2
        || new_object.family_code != 2
        || catalog_field_bytes(&table_key, old_table_bytes, 5, 4)? != table_id
        || catalog_field_bytes(&table_key, new_table_bytes, 5, 4)? != table_id
        || !catalog_fields_equal_except(&old_table, &new_table, &[3])
        || catalog_field_u16(&object_key, old_object_bytes, 2, 1)? != 3
        || catalog_field_u16(&object_key, new_object_bytes, 2, 1)? != 3
        || catalog_field_bytes(&object_key, old_object_bytes, 2, 2)? != schema_id
        || catalog_field_bytes(&object_key, new_object_bytes, 2, 2)? != schema_id
        || !object_mutation_valid
        || catalog_field_u64(&object_key, new_object_bytes, 2, 5)?
            != catalog_field_u64(&object_key, old_object_bytes, 2, 5)?
                .checked_add(1)
                .ok_or(CommitError::InvalidCatalog)?
        || table_schema_version(&table_key, new_table_bytes)?
            != table_schema_version(&table_key, old_table_bytes)?
        || table_write_epoch(&table_key, new_table_bytes)?
            != table_write_epoch(&table_key, old_table_bytes)?
                .checked_add(1)
                .ok_or(CommitError::InvalidCatalog)?
    {
        return Err(CommitError::InvalidCatalog);
    }
    let owner_id = old_owner_id;
    let read = database.begin_read().map_err(storage_error)?;
    let catalog = read.open_table(CATALOG).map_err(storage_error)?;
    let actor_key = id_catalog_key(10, principal_id);
    let actor_bytes = catalog
        .get(actor_key.as_slice())
        .map_err(storage_error)?
        .map(|value| value.value().to_vec())
        .ok_or(CommitError::DdlAuthorizationDenied)?;
    if decode_principal_kind(&actor_key, &actor_bytes)? != 1
        || catalog_field_bytes(&actor_key, &actor_bytes, 10, 3)? != [1]
        || catalog_field_u64(&actor_key, &actor_bytes, 10, 4)? == 0
    {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    let mut guards = HashMap::<Vec<u8>, Option<Vec<u8>>>::new();
    add_catalog_guard(&mut guards, table_key, Some(old_table_bytes.to_vec()));
    add_catalog_guard(&mut guards, object_key, Some(old_object_bytes.to_vec()));
    add_catalog_guard(&mut guards, actor_key, Some(actor_bytes));
    let membership_prefix = 11u16.to_be_bytes();
    let mut memberships = Vec::new();
    let mut subject_ids = HashSet::from([principal_id, owner_id, new_owner_id]);
    let mut scanned = 0usize;
    for entry in catalog
        .range(membership_prefix.as_slice()..)
        .map_err(storage_error)?
    {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value().to_vec();
        if !key.starts_with(&membership_prefix) {
            break;
        }
        scanned += 1;
        if scanned > MAX_MEMBERSHIP_EDGES {
            return Err(CommitError::InvalidCatalog);
        }
        let bytes = value.value().to_vec();
        let edge = decode_membership_record(&key, &bytes)?;
        subject_ids.extend([edge.member_id, edge.role_id]);
        memberships.push(edge);
        add_catalog_guard(&mut guards, key, Some(bytes));
    }
    let roles = resolve_role_closure(principal_id, &memberships)
        .map_err(|_| CommitError::InvalidCatalog)?;
    let mut admin_role_id = [0x80; 16];
    admin_role_id[15] = 3;
    let mut authorized =
        owner_id == principal_id || roles.contains(&owner_id) || roles.contains(&admin_role_id);
    let metadata = read
        .open_table(ENGINE_META)
        .map_err(storage_error)?
        .get(&b"format"[..])
        .map_err(storage_error)?
        .ok_or(CommitError::InvalidMetadata)?;
    let database_id = EngineMetadata::decode(metadata.value())
        .map_err(|_| CommitError::InvalidMetadata)?
        .database_id;
    let database_key = id_catalog_key(1, database_id);
    let database_bytes = catalog
        .get(database_key.as_slice())
        .map_err(storage_error)?
        .map(|value| value.value().to_vec())
        .ok_or(CommitError::InvalidCatalog)?;
    add_catalog_guard(
        &mut guards,
        database_key.clone(),
        Some(database_bytes.clone()),
    );
    let builtins = catalog_field_bytes(&database_key, &database_bytes, 1, 3)?;
    if builtins.len() < 4 {
        return Err(CommitError::InvalidCatalog);
    }
    let builtin_count = u32::from_le_bytes(
        builtins[..4]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?,
    ) as usize;
    if builtin_count > 256 || builtins.len() != 4 + builtin_count * 20 {
        return Err(CommitError::InvalidCatalog);
    }
    if builtins[4..].chunks_exact(20).any(|entry| {
        matches!(u16::from_le_bytes([entry[0], entry[1]]), 1 | 2) && entry[4..20] == schema_id
    }) {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    let mut authority_subjects = roles.clone();
    authority_subjects.insert(principal_id);
    for subject in authority_subjects {
        for object_id in [table_id, schema_id] {
            let key = grant_key(subject, object_id, 8, 1, object_id);
            let bytes = catalog
                .get(key.as_slice())
                .map_err(storage_error)?
                .map(|value| value.value().to_vec());
            if let Some(value) = &bytes {
                if catalog_field_bytes(&key, value, 12, 1)? == [0; 16]
                    || catalog_field_u64(&key, value, 12, 2)? == 0
                {
                    return Err(CommitError::InvalidCatalog);
                }
                authorized = true;
            }
            add_catalog_guard(&mut guards, key, bytes);
        }
    }
    if !authorized {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    subject_ids.extend(roles);
    let role_subject_ids = memberships
        .iter()
        .map(|edge| edge.role_id)
        .collect::<HashSet<_>>();
    for subject in subject_ids {
        let key = id_catalog_key(10, subject);
        let bytes = catalog
            .get(key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        let kind = decode_principal_kind(&key, &bytes)?;
        if (role_subject_ids.contains(&subject)
            && (kind != 2 || catalog_field_bytes(&key, &bytes, 10, 3)? != [0]))
            || !matches!(kind, 1 | 2)
            || catalog_field_u64(&key, &bytes, 10, 4)? == 0
        {
            return Err(CommitError::InvalidCatalog);
        }
        add_catalog_guard(&mut guards, key, Some(bytes));
    }
    let mut result = guards
        .into_iter()
        .map(|(key, expected_value)| CatalogGuard {
            key,
            expected_value,
        })
        .collect::<Vec<_>>();
    result.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(result)
}

pub(super) fn column_rename_ddl_authority_guards(
    database: &Database,
    principal_id: [u8; 16],
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<Vec<CatalogGuard>, CommitError> {
    if principal_id == [0; 16] || catalog_writes.len() != 3 || audit_events.is_empty() {
        return Err(CommitError::InvalidCatalog);
    }
    let table_write = catalog_writes
        .iter()
        .find(|write| write.key.starts_with(&5u16.to_be_bytes()))
        .ok_or(CommitError::InvalidCatalog)?;
    let column_write = catalog_writes
        .iter()
        .find(|write| write.key.starts_with(&6u16.to_be_bytes()))
        .ok_or(CommitError::InvalidCatalog)?;
    let table_id: [u8; 16] = table_write
        .key
        .get(3..19)
        .ok_or(CommitError::InvalidCatalog)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let column_id: [u8; 16] = column_write
        .key
        .get(3..19)
        .ok_or(CommitError::InvalidCatalog)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let table_key = id_catalog_key(5, table_id);
    let table_object_key = id_catalog_key(2, table_id);
    let column_key = id_catalog_key(6, column_id);
    let column_object_key = id_catalog_key(2, column_id);
    let object_write = catalog_writes
        .iter()
        .find(|write| write.key == column_object_key)
        .ok_or(CommitError::InvalidCatalog)?;
    if table_id == [0; 16]
        || column_id == [0; 16]
        || table_write.key != table_key
        || column_write.key != column_key
        || table_write.expected_value.is_none()
        || table_write.payload.is_none()
        || column_write.expected_value.is_none()
        || column_write.payload.is_none()
        || object_write.expected_value.is_none()
        || object_write.payload.is_none()
        || catalog_writes.iter().any(|write| {
            write.key != table_key && write.key != column_key && write.key != column_object_key
        })
    {
        return Err(CommitError::InvalidCatalog);
    }
    let mut target_ids = vec![table_id, column_id];
    target_ids.sort_unstable();
    if audit_events.iter().any(|event| {
        event.actor_id != Some(principal_id)
            || event.action != crate::audit::AuditAction::Ddl
            || event.result != crate::audit::AuditResult::Succeeded
            || event.target_ids != target_ids
    }) {
        return Err(CommitError::InvalidAudit);
    }
    let old_table_bytes = table_write.expected_value.as_deref().unwrap();
    let new_table_bytes = table_write.payload.as_deref().unwrap();
    let old_column_bytes = column_write.expected_value.as_deref().unwrap();
    let new_column_bytes = column_write.payload.as_deref().unwrap();
    let old_object_bytes = object_write.expected_value.as_deref().unwrap();
    let new_object_bytes = object_write.payload.as_deref().unwrap();
    let old_table = decode_catalog_record(&table_key, old_table_bytes)?;
    let new_table = decode_catalog_record(&table_key, new_table_bytes)?;
    let old_column = decode_catalog_record(&column_key, old_column_bytes)?;
    let new_column = decode_catalog_record(&column_key, new_column_bytes)?;
    let old_object = decode_catalog_record(&column_object_key, old_object_bytes)?;
    let new_object = decode_catalog_record(&column_object_key, new_object_bytes)?;
    let old_name = catalog_field_bytes(&column_key, old_column_bytes, 6, 3)?;
    let new_name = catalog_field_bytes(&column_key, new_column_bytes, 6, 3)?;
    if old_table.family_code != 5
        || new_table.family_code != 5
        || old_column.family_code != 6
        || new_column.family_code != 6
        || old_object.family_code != 2
        || new_object.family_code != 2
        || column_table_id(&column_key, old_column_bytes)? != table_id
        || column_table_id(&column_key, new_column_bytes)? != table_id
        || catalog_field_u16(&column_object_key, old_object_bytes, 2, 1)? != 4
        || catalog_field_u16(&column_object_key, new_object_bytes, 2, 1)? != 4
        || catalog_field_bytes(&column_object_key, old_object_bytes, 2, 2)? != table_id
        || catalog_field_bytes(&column_object_key, new_object_bytes, 2, 2)? != table_id
        || catalog_field_bytes(&column_object_key, old_object_bytes, 2, 3)? != old_name
        || catalog_field_bytes(&column_object_key, new_object_bytes, 2, 3)? != new_name
        || old_name == new_name
        || !is_canonical_catalog_name(&new_name)
        || !catalog_fields_equal_except(&old_table, &new_table, &[3])
        || !catalog_fields_equal_except(&old_column, &new_column, &[3])
        || !catalog_fields_equal_except(&old_object, &new_object, &[3, 5])
        || catalog_field_u64(&column_object_key, new_object_bytes, 2, 5)?
            != catalog_field_u64(&column_object_key, old_object_bytes, 2, 5)?
                .checked_add(1)
                .ok_or(CommitError::InvalidCatalog)?
        || table_schema_version(&table_key, new_table_bytes)?
            != table_schema_version(&table_key, old_table_bytes)?
        || table_write_epoch(&table_key, new_table_bytes)?
            != table_write_epoch(&table_key, old_table_bytes)?
                .checked_add(1)
                .ok_or(CommitError::InvalidCatalog)?
    {
        return Err(CommitError::InvalidCatalog);
    }

    let read = database.begin_read().map_err(storage_error)?;
    let catalog = read.open_table(CATALOG).map_err(storage_error)?;
    let columns_prefix = 6u16.to_be_bytes();
    let mut scanned = 0usize;
    for entry in catalog
        .range(columns_prefix.as_slice()..)
        .map_err(storage_error)?
    {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&columns_prefix) {
            break;
        }
        scanned += 1;
        if scanned > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
            return Err(CommitError::InvalidCatalog);
        }
        if key == column_key.as_slice() || key.len() != 19 {
            continue;
        }
        let bytes = value.value();
        if column_table_id(key, bytes)? == table_id
            && catalog_field_bytes(key, bytes, 6, 3)? == new_name
        {
            return Err(CommitError::InvalidCatalog);
        }
    }

    let owner_key = table_object_key;
    let owner_bytes = catalog
        .get(owner_key.as_slice())
        .map_err(storage_error)?
        .map(|value| value.value().to_vec())
        .ok_or(CommitError::InvalidCatalog)?;
    if catalog_field_u16(&owner_key, &owner_bytes, 2, 1)? != 3
        || catalog_field_bytes(&owner_key, &owner_bytes, 2, 2)?
            != catalog_field_bytes(&table_key, old_table_bytes, 5, 1)?
    {
        return Err(CommitError::InvalidCatalog);
    }
    let owner_id: [u8; 16] = catalog_field_bytes(&owner_key, &owner_bytes, 2, 4)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let schema_id: [u8; 16] = catalog_field_bytes(&owner_key, &owner_bytes, 2, 2)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;

    let actor_key = id_catalog_key(10, principal_id);
    let actor_bytes = catalog
        .get(actor_key.as_slice())
        .map_err(storage_error)?
        .map(|value| value.value().to_vec())
        .ok_or(CommitError::DdlAuthorizationDenied)?;
    if decode_principal_kind(&actor_key, &actor_bytes)? != 1
        || catalog_field_bytes(&actor_key, &actor_bytes, 10, 3)? != [1]
        || catalog_field_u64(&actor_key, &actor_bytes, 10, 4)? == 0
    {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    let mut guards = HashMap::<Vec<u8>, Option<Vec<u8>>>::new();
    add_catalog_guard(&mut guards, table_key, Some(old_table_bytes.to_vec()));
    add_catalog_guard(&mut guards, owner_key, Some(owner_bytes));
    add_catalog_guard(&mut guards, column_key, Some(old_column_bytes.to_vec()));
    add_catalog_guard(
        &mut guards,
        column_object_key,
        Some(old_object_bytes.to_vec()),
    );
    add_catalog_guard(&mut guards, actor_key, Some(actor_bytes));
    let membership_prefix = 11u16.to_be_bytes();
    let mut memberships = Vec::new();
    let mut subjects = HashSet::from([principal_id, owner_id]);
    scanned = 0;
    for entry in catalog
        .range(membership_prefix.as_slice()..)
        .map_err(storage_error)?
    {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value().to_vec();
        if !key.starts_with(&membership_prefix) {
            break;
        }
        scanned += 1;
        if scanned > MAX_MEMBERSHIP_EDGES {
            return Err(CommitError::InvalidCatalog);
        }
        let bytes = value.value().to_vec();
        let edge = decode_membership_record(&key, &bytes)?;
        subjects.extend([edge.member_id, edge.role_id]);
        memberships.push(edge);
        add_catalog_guard(&mut guards, key, Some(bytes));
    }
    let roles = resolve_role_closure(principal_id, &memberships)
        .map_err(|_| CommitError::InvalidCatalog)?;
    let mut admin_role_id = [0x80; 16];
    admin_role_id[15] = 3;
    let mut authorized =
        owner_id == principal_id || roles.contains(&owner_id) || roles.contains(&admin_role_id);
    let metadata = read
        .open_table(ENGINE_META)
        .map_err(storage_error)?
        .get(&b"format"[..])
        .map_err(storage_error)?
        .ok_or(CommitError::InvalidMetadata)?;
    let database_id = EngineMetadata::decode(metadata.value())
        .map_err(|_| CommitError::InvalidMetadata)?
        .database_id;
    let database_key = id_catalog_key(1, database_id);
    let database_bytes = catalog
        .get(database_key.as_slice())
        .map_err(storage_error)?
        .map(|value| value.value().to_vec())
        .ok_or(CommitError::InvalidCatalog)?;
    add_catalog_guard(
        &mut guards,
        database_key.clone(),
        Some(database_bytes.clone()),
    );
    let builtins = catalog_field_bytes(&database_key, &database_bytes, 1, 3)?;
    if builtins.len() < 4 {
        return Err(CommitError::InvalidCatalog);
    }
    let builtin_count = u32::from_le_bytes(
        builtins[..4]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?,
    ) as usize;
    if builtin_count > 256 || builtins.len() != 4 + builtin_count * 20 {
        return Err(CommitError::InvalidCatalog);
    }
    if builtins[4..].chunks_exact(20).any(|entry| {
        matches!(u16::from_le_bytes([entry[0], entry[1]]), 1 | 2) && entry[4..20] == schema_id
    }) {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    let mut authority_subjects = roles.clone();
    authority_subjects.insert(principal_id);
    for subject in authority_subjects {
        for object_id in [table_id, schema_id] {
            let key = grant_key(subject, object_id, 8, 1, object_id);
            let bytes = catalog
                .get(key.as_slice())
                .map_err(storage_error)?
                .map(|value| value.value().to_vec());
            if let Some(value) = &bytes {
                if catalog_field_bytes(&key, value, 12, 1)? == [0; 16]
                    || catalog_field_u64(&key, value, 12, 2)? == 0
                {
                    return Err(CommitError::InvalidCatalog);
                }
                authorized = true;
            }
            add_catalog_guard(&mut guards, key, bytes);
        }
    }
    if !authorized {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    subjects.extend(roles);
    let role_subjects = memberships
        .iter()
        .map(|edge| edge.role_id)
        .collect::<HashSet<_>>();
    for subject in subjects {
        let key = id_catalog_key(10, subject);
        let bytes = catalog
            .get(key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        let kind = decode_principal_kind(&key, &bytes)?;
        if (role_subjects.contains(&subject)
            && (kind != 2 || catalog_field_bytes(&key, &bytes, 10, 3)? != [0]))
            || !matches!(kind, 1 | 2)
            || catalog_field_u64(&key, &bytes, 10, 4)? == 0
        {
            return Err(CommitError::InvalidCatalog);
        }
        add_catalog_guard(&mut guards, key, Some(bytes));
    }
    let mut result = guards
        .into_iter()
        .map(|(key, expected_value)| CatalogGuard {
            key,
            expected_value,
        })
        .collect::<Vec<_>>();
    result.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(result)
}

pub(super) fn table_drop_ddl_authority_guards(
    database: &Database,
    principal_id: [u8; 16],
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<Vec<CatalogGuard>, CommitError> {
    let table_id = catalog_writes
        .iter()
        .find(|write| write.key.starts_with(&5u16.to_be_bytes()) && write.payload.is_none())
        .and_then(|write| write.key.get(3..19))
        .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
        .ok_or(CommitError::InvalidCatalog)?;
    let table_key = id_catalog_key(5, table_id);
    let object_key = id_catalog_key(2, table_id);
    if principal_id == [0; 16]
        || table_id == [0; 16]
        || catalog_writes.iter().any(|write| {
            (write.expected_value.is_none()
                && !(write.key.starts_with(&17u16.to_be_bytes()) && write.payload.is_some()))
                || (write.payload.is_some()
                    && !write.key.starts_with(&10u16.to_be_bytes())
                    && !write.key.starts_with(&5u16.to_be_bytes())
                    && !write.key.starts_with(&17u16.to_be_bytes()))
        })
        || audit_events.is_empty()
        || audit_events.iter().any(|event| {
            event.actor_id != Some(principal_id)
                || event.action != crate::audit::AuditAction::Ddl
                || event.result != crate::audit::AuditResult::Succeeded
                || event.target_ids.as_slice() != [table_id]
        })
    {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    let table_write = catalog_writes
        .iter()
        .find(|write| write.key == table_key)
        .ok_or(CommitError::InvalidCatalog)?;
    let object_write = catalog_writes
        .iter()
        .find(|write| write.key == object_key)
        .ok_or(CommitError::InvalidCatalog)?;
    let table_bytes = table_write.expected_value.as_deref().unwrap();
    let object_bytes = object_write.expected_value.as_deref().unwrap();
    let table_record = decode_catalog_record(&table_key, table_bytes)?;
    let object_record = decode_catalog_record(&object_key, object_bytes)?;
    if table_record.family_code != 5
        || object_record.family_code != 2
        || catalog_field_u16(&object_key, object_bytes, 2, 1)? != 3
    {
        return Err(CommitError::InvalidCatalog);
    }
    let mut required_retired_object_ids = HashSet::<[u8; 16]>::new();
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&2u16.to_be_bytes()) && write.payload.is_none())
    {
        let bytes = write
            .expected_value
            .as_deref()
            .ok_or(CommitError::InvalidCatalog)?;
        if matches!(catalog_field_u16(&write.key, bytes, 2, 1)?, 3..=7 | 10) {
            let id: [u8; 16] = write
                .key
                .get(3..19)
                .ok_or(CommitError::InvalidCatalog)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            required_retired_object_ids.insert(id);
        }
    }
    if !required_retired_object_ids.contains(&table_id) {
        return Err(CommitError::InvalidCatalog);
    }
    let mut retired_object_ids = HashSet::<[u8; 16]>::new();
    let owner_id: [u8; 16] = catalog_field_bytes(&object_key, object_bytes, 2, 4)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let read = database.begin_read().map_err(storage_error)?;
    let catalog = read.open_table(CATALOG).map_err(storage_error)?;
    let actor_key = id_catalog_key(10, principal_id);
    let actor_bytes = catalog
        .get(actor_key.as_slice())
        .map_err(storage_error)?
        .map(|value| value.value().to_vec())
        .ok_or(CommitError::DdlAuthorizationDenied)?;
    if decode_principal_kind(&actor_key, &actor_bytes)? != 1
        || catalog_field_bytes(&actor_key, &actor_bytes, 10, 3)? != [1]
        || catalog_field_u64(&actor_key, &actor_bytes, 10, 4)? == 0
    {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    let mut guards = HashMap::<Vec<u8>, Option<Vec<u8>>>::new();
    add_catalog_guard(&mut guards, table_key.clone(), Some(table_bytes.to_vec()));
    add_catalog_guard(&mut guards, object_key, Some(object_bytes.to_vec()));
    add_catalog_guard(&mut guards, actor_key.clone(), Some(actor_bytes));
    let membership_prefix = 11u16.to_be_bytes();
    let mut memberships = Vec::new();
    let mut subject_ids = HashSet::from([principal_id, owner_id]);
    let mut scanned = 0usize;
    for entry in catalog
        .range(membership_prefix.as_slice()..)
        .map_err(storage_error)?
    {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value().to_vec();
        if !key.starts_with(&membership_prefix) {
            break;
        }
        scanned += 1;
        if scanned > MAX_MEMBERSHIP_EDGES {
            return Err(CommitError::InvalidCatalog);
        }
        let bytes = value.value().to_vec();
        let edge = decode_membership_record(&key, &bytes)?;
        subject_ids.extend([edge.member_id, edge.role_id]);
        memberships.push(edge);
        add_catalog_guard(&mut guards, key, Some(bytes));
    }
    let roles = resolve_role_closure(principal_id, &memberships)
        .map_err(|_| CommitError::InvalidCatalog)?;
    let mut admin_role_id = [0x80; 16];
    admin_role_id[15] = 3;

    let schema_id: [u8; 16] = catalog_field_bytes(&table_key, table_bytes, 5, 1)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let metadata = read
        .open_table(ENGINE_META)
        .map_err(storage_error)?
        .get(&b"format"[..])
        .map_err(storage_error)?
        .ok_or(CommitError::InvalidMetadata)?;
    let database_id = EngineMetadata::decode(metadata.value())
        .map_err(|_| CommitError::InvalidMetadata)?
        .database_id;
    let database_key = id_catalog_key(1, database_id);
    let database_bytes = catalog
        .get(database_key.as_slice())
        .map_err(storage_error)?
        .map(|value| value.value().to_vec())
        .ok_or(CommitError::InvalidCatalog)?;
    add_catalog_guard(
        &mut guards,
        database_key.clone(),
        Some(database_bytes.clone()),
    );
    let builtins = catalog_field_bytes(&database_key, &database_bytes, 1, 3)?;
    if builtins.len() < 4 {
        return Err(CommitError::InvalidCatalog);
    }
    let builtin_count = u32::from_le_bytes(
        builtins[..4]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?,
    ) as usize;
    if builtin_count > 256 || builtins.len() != 4 + builtin_count * 20 {
        return Err(CommitError::InvalidCatalog);
    }
    if builtins[4..].chunks_exact(20).any(|entry| {
        matches!(u16::from_le_bytes([entry[0], entry[1]]), 1 | 2) && entry[4..20] == schema_id
    }) {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    let mut authorized =
        owner_id == principal_id || roles.contains(&owner_id) || roles.contains(&admin_role_id);
    let mut authority_subjects = roles.clone();
    authority_subjects.insert(principal_id);
    for subject in authority_subjects {
        for object_id in [table_id, schema_id] {
            let key = grant_key(subject, object_id, 8, 1, object_id);
            let bytes = catalog
                .get(key.as_slice())
                .map_err(storage_error)?
                .map(|value| value.value().to_vec());
            if let Some(value) = &bytes {
                if catalog_field_bytes(&key, value, 12, 1)? == [0; 16]
                    || catalog_field_u64(&key, value, 12, 2)? == 0
                {
                    return Err(CommitError::InvalidCatalog);
                }
                authorized = true;
            }
            add_catalog_guard(&mut guards, key, bytes);
        }
    }
    if !authorized {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    let mut owned_ids = HashSet::from([table_id]);
    let mut typed_ids = HashSet::from([table_id]);
    let mut expression_ids = HashSet::new();
    let mut policy_ids = HashSet::new();
    let mut referenced_table_ids = HashSet::new();
    for family in [6u16, 8, 9, 13] {
        let prefix = family.to_be_bytes();
        let mut scanned = 0usize;
        for entry in catalog.range(prefix.as_slice()..).map_err(storage_error)? {
            let (key, value) = entry.map_err(storage_error)?;
            let key = key.value();
            if !key.starts_with(&prefix) {
                break;
            }
            scanned += 1;
            if scanned > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
                return Err(CommitError::InvalidCatalog);
            }
            let bytes = value.value().to_vec();
            if catalog_field_bytes(key, &bytes, family, 1)? != table_id {
                continue;
            }
            if key.len() != 19 {
                return Err(CommitError::InvalidCatalog);
            }
            let id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            owned_ids.insert(id);
            typed_ids.insert(id);
            if family == 13 {
                policy_ids.insert(id);
            }
            let record = decode_catalog_record(key, &bytes)?;
            if family == 8 && catalog_field_u16(key, &bytes, 8, 2)? == 5 {
                let target_constraint_id: [u8; 16] = catalog_field_bytes(key, &bytes, 8, 5)?
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                let target_key = id_catalog_key(8, target_constraint_id);
                let target_bytes = catalog
                    .get(target_key.as_slice())
                    .map_err(storage_error)?
                    .ok_or(CommitError::InvalidCatalog)?
                    .value()
                    .to_vec();
                let target_table_id: [u8; 16] =
                    catalog_field_bytes(&target_key, &target_bytes, 8, 1)?
                        .try_into()
                        .map_err(|_| CommitError::InvalidCatalog)?;
                if target_table_id != table_id {
                    referenced_table_ids.insert(target_table_id);
                }
            }
            for tag in match family {
                6 => vec![7, 10],
                8 => vec![4],
                13 => vec![5, 6],
                _ => Vec::new(),
            } {
                if let Some(field) = record.fields.iter().find(|field| field.tag == tag) {
                    let expression_id: [u8; 16] = field
                        .payload
                        .as_slice()
                        .try_into()
                        .map_err(|_| CommitError::InvalidCatalog)?;
                    owned_ids.insert(expression_id);
                    expression_ids.insert(expression_id);
                }
            }
        }
    }
    let object_prefix = 2u16.to_be_bytes();
    let mut discovered = true;
    while discovered {
        discovered = false;
        let mut scanned = 0usize;
        for entry in catalog
            .range(object_prefix.as_slice()..)
            .map_err(storage_error)?
        {
            let (key, value) = entry.map_err(storage_error)?;
            let key = key.value();
            if !key.starts_with(&object_prefix) {
                break;
            }
            scanned += 1;
            if scanned > crate::snapshot::MAX_CATALOG_FAMILY_SCAN || key.len() != 19 {
                return Err(CommitError::InvalidCatalog);
            }
            let id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let bytes = value.value().to_vec();
            let record = decode_catalog_record(key, &bytes)?;
            let parent = record
                .fields
                .iter()
                .find(|field| field.tag == 2)
                .map(|field| <[u8; 16]>::try_from(field.payload.as_slice()))
                .transpose()
                .map_err(|_| CommitError::InvalidCatalog)?;
            if (id == table_id || parent.is_some_and(|parent| owned_ids.contains(&parent)))
                && owned_ids.insert(id)
            {
                discovered = true;
            }
        }
    }
    let grant_pairs = catalog_writes
        .iter()
        .filter(|write| {
            write.key.starts_with(&12u16.to_be_bytes())
                && write.key.len() == 59
                && write.payload.is_none()
        })
        .map(|write| {
            Ok::<_, CommitError>((
                <[u8; 16]>::try_from(&write.key[3..19]).map_err(|_| CommitError::InvalidCatalog)?,
                <[u8; 16]>::try_from(&write.key[43..59])
                    .map_err(|_| CommitError::InvalidCatalog)?,
            ))
        })
        .collect::<Result<HashSet<_>, _>>()?;
    let changed_grantees = grant_pairs
        .iter()
        .map(|(grantee, _)| *grantee)
        .collect::<HashSet<_>>();
    let principal_writes = catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&10u16.to_be_bytes()))
        .collect::<Vec<_>>();
    if principal_writes.len() != changed_grantees.len() {
        return Err(CommitError::InvalidCatalog);
    }
    let mut updated_grantees = HashSet::new();
    for write in principal_writes {
        let grantee_id: [u8; 16] = write
            .key
            .get(3..19)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or(CommitError::InvalidCatalog)?;
        if write.key != id_catalog_key(10, grantee_id) {
            return Err(CommitError::InvalidCatalog);
        }
        let Some(expected) = write.expected_value.as_deref() else {
            return Err(CommitError::InvalidCatalog);
        };
        let Some(next) = write.payload.as_deref() else {
            return Err(CommitError::InvalidCatalog);
        };
        let current_record = decode_catalog_record(&write.key, expected)?;
        let next_record = decode_catalog_record(&write.key, next)?;
        let current_fields = current_record
            .fields
            .iter()
            .filter(|field| field.tag != 4)
            .collect::<Vec<_>>();
        let next_fields = next_record
            .fields
            .iter()
            .filter(|field| field.tag != 4)
            .collect::<Vec<_>>();
        if current_record.family_code != 10
            || next_record.family_code != 10
            || current_record.logical_key != next_record.logical_key
            || current_fields != next_fields
            || !changed_grantees.contains(&grantee_id)
            || !updated_grantees.insert(grantee_id)
            || principal_authority_epoch(&write.key, expected)?.checked_add(1)
                != Some(principal_authority_epoch(&write.key, next)?)
        {
            return Err(CommitError::InvalidCatalog);
        }
    }
    if updated_grantees != changed_grantees {
        return Err(CommitError::InvalidCatalog);
    }
    for write in catalog_writes {
        let family = write
            .key
            .get(..2)
            .map(|bytes| u16::from_be_bytes([bytes[0], bytes[1]]))
            .ok_or(CommitError::InvalidCatalog)?;
        if (write.expected_value.is_none() && !(family == 17 && write.payload.is_some()))
            || (write.payload.is_some() && !matches!(family, 5 | 10 | 17))
        {
            return Err(CommitError::InvalidCatalog);
        }
        let id = write
            .key
            .get(3..19)
            .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok());
        let belongs = match family {
            2 => id.is_some_and(|id| owned_ids.contains(&id)),
            5 => {
                if write.key == table_key {
                    write.payload.is_none()
                } else if referenced_table_ids
                    .iter()
                    .any(|table_id| write.key == id_catalog_key(5, *table_id))
                {
                    let expected = write.expected_value.as_deref().unwrap();
                    let next = write
                        .payload
                        .as_deref()
                        .ok_or(CommitError::InvalidCatalog)?;
                    let expected_fields = decode_catalog_record(&write.key, expected)?
                        .fields
                        .into_iter()
                        .filter(|field| field.tag != 3)
                        .collect::<Vec<_>>();
                    let next_fields = decode_catalog_record(&write.key, next)?
                        .fields
                        .into_iter()
                        .filter(|field| field.tag != 3)
                        .collect::<Vec<_>>();
                    expected_fields == next_fields
                        && table_write_epoch(&write.key, expected)?.checked_add(1)
                            == Some(table_write_epoch(&write.key, next)?)
                } else {
                    false
                }
            }
            6 | 8 | 9 | 13 => id.is_some_and(|id| typed_ids.contains(&id)),
            7 => id.is_some_and(|id| expression_ids.contains(&id)),
            12 => {
                write.key.len() == 59
                    && write
                        .key
                        .get(20..36)
                        .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
                        .is_some_and(|id| owned_ids.contains(&id))
            }
            10 => id.is_some_and(|id| changed_grantees.contains(&id)),
            14 => id.is_some_and(|id| policy_ids.contains(&id)),
            15 => {
                let dependent = write
                    .key
                    .get(3..19)
                    .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok());
                let referenced = write
                    .key
                    .get(20..36)
                    .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok());
                let kind = write
                    .key
                    .get(37..39)
                    .and_then(|bytes| <[u8; 2]>::try_from(bytes).ok())
                    .map(u16::from_be_bytes);
                dependent.is_some_and(|id| owned_ids.contains(&id))
                    || (kind == Some(5)
                        && dependent
                            .zip(referenced)
                            .is_some_and(|pair| grant_pairs.contains(&pair)))
            }
            17 => {
                let retired_id = id.ok_or(CommitError::InvalidCatalog)?;
                if !required_retired_object_ids.contains(&retired_id) {
                    false
                } else {
                    let bytes = write
                        .payload
                        .as_deref()
                        .ok_or(CommitError::InvalidCatalog)?;
                    let reservation = decode_catalog_record(&write.key, bytes)?;
                    let valid = reservation.family_code == 17 && reservation.fields.is_empty();
                    if valid {
                        retired_object_ids.insert(retired_id);
                    }
                    valid
                }
            }
            _ => false,
        };
        if !belongs {
            return Err(CommitError::InvalidCatalog);
        }
    }
    if retired_object_ids != required_retired_object_ids {
        return Err(CommitError::InvalidCatalog);
    }
    let deleted_ids = catalog_writes
        .iter()
        .filter(|write| {
            matches!(
                write
                    .key
                    .get(..2)
                    .map(|bytes| u16::from_be_bytes([bytes[0], bytes[1]])),
                Some(2 | 5 | 6 | 7 | 8 | 9 | 13)
            ) && write.key.len() == 19
                && write.payload.is_none()
        })
        .filter_map(|write| <[u8; 16]>::try_from(&write.key[3..19]).ok())
        .collect::<HashSet<_>>();
    let deleted_dependency_keys = catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&15u16.to_be_bytes()) && write.payload.is_none())
        .map(|write| write.key.as_slice())
        .collect::<HashSet<_>>();
    let dependency_index = read
        .open_table(crate::schema::DEPENDENCIES)
        .map_err(storage_error)?;
    let mut dependency_count = 0usize;
    let mut deleted_id_order = deleted_ids.iter().copied().collect::<Vec<_>>();
    deleted_id_order.sort_unstable();
    for referenced in &deleted_id_order {
        let mut prefix = vec![2];
        prefix.extend_from_slice(referenced);
        for entry in dependency_index
            .range::<&[u8]>(prefix.as_slice()..)
            .map_err(storage_error)?
        {
            let (index_key, marker) = entry.map_err(storage_error)?;
            let index_key = index_key.value();
            if !index_key.starts_with(&prefix) {
                break;
            }
            dependency_count += 1;
            if dependency_count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN
                || index_key.len() != 35
                || !marker.value().is_empty()
            {
                return Err(CommitError::InvalidCatalog);
            }
            let dependent: [u8; 16] = index_key[17..33]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let kind = u16::from_be_bytes([index_key[33], index_key[34]]);
            if !matches!(kind, 1..=5) {
                return Err(CommitError::InvalidCatalog);
            }
            let mut catalog_key = 15u16.to_be_bytes().to_vec();
            catalog_key.push(1);
            catalog_key.extend_from_slice(&dependent);
            catalog_key.push(1);
            catalog_key.extend_from_slice(referenced);
            catalog_key.push(1);
            catalog_key.extend_from_slice(&kind.to_be_bytes());
            let bytes = catalog
                .get(catalog_key.as_slice())
                .map_err(storage_error)?
                .ok_or(CommitError::InvalidCatalog)?;
            let (actual_dependent, actual_referenced, actual_kind) =
                decode_dependency_edge(&catalog_key, bytes.value())?;
            if actual_dependent != dependent
                || actual_referenced != *referenced
                || actual_kind != kind
            {
                return Err(CommitError::InvalidCatalog);
            }
            if !deleted_ids.contains(&dependent)
                && !deleted_dependency_keys.contains(catalog_key.as_slice())
            {
                return Err(CommitError::TableHasDependencies);
            }
        }
    }
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&15u16.to_be_bytes()))
    {
        let Some(bytes) = &write.payload else {
            continue;
        };
        let (dependent, referenced, _) = decode_dependency_edge(&write.key, bytes)?;
        if deleted_ids.contains(&referenced) && !deleted_ids.contains(&dependent) {
            return Err(CommitError::TableHasDependencies);
        }
    }
    for write in catalog_writes {
        let family = write
            .key
            .get(..2)
            .map(|bytes| u16::from_be_bytes([bytes[0], bytes[1]]));
        if !matches!(
            family,
            Some(2 | 5 | 6 | 7 | 8 | 9 | 10 | 12 | 13 | 14 | 15 | 17)
        ) {
            return Err(CommitError::InvalidCatalog);
        }
        add_catalog_guard(&mut guards, write.key.clone(), write.expected_value.clone());
    }
    subject_ids.extend(roles);
    for subject in subject_ids {
        let key = id_catalog_key(10, subject);
        if let Some(bytes) = catalog.get(key.as_slice()).map_err(storage_error)? {
            add_catalog_guard(&mut guards, key, Some(bytes.value().to_vec()));
        }
    }
    let mut result = guards
        .into_iter()
        .map(|(key, expected_value)| CatalogGuard {
            key,
            expected_value,
        })
        .collect::<Vec<_>>();
    result.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(result)
}

pub(super) fn column_drop_ddl_authority_guards(
    database: &Database,
    principal_id: [u8; 16],
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<Vec<CatalogGuard>, CommitError> {
    let column_writes = catalog_writes
        .iter()
        .filter(|write| {
            write.key.starts_with(&6u16.to_be_bytes())
                && write.expected_value.is_some()
                && write.payload.is_none()
        })
        .collect::<Vec<_>>();
    if column_writes.len() != 1 {
        return Err(CommitError::InvalidCatalog);
    }
    let column_write = column_writes[0];
    let column_id: [u8; 16] = column_write
        .key
        .get(3..19)
        .ok_or(CommitError::InvalidCatalog)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let column_bytes = column_write
        .expected_value
        .as_deref()
        .ok_or(CommitError::InvalidCatalog)?;
    let table_id = column_table_id(&column_write.key, column_bytes)?;
    let mut audit_targets = [table_id, column_id];
    audit_targets.sort_unstable();
    if principal_id == [0; 16]
        || table_id == [0; 16]
        || column_id == [0; 16]
        || audit_events.is_empty()
        || audit_events.iter().any(|event| {
            event.actor_id != Some(principal_id)
                || event.action != crate::audit::AuditAction::Ddl
                || event.result != crate::audit::AuditResult::Succeeded
                || event.target_ids.as_slice() != audit_targets
        })
    {
        return Err(CommitError::InvalidAudit);
    }
    let table_key = id_catalog_key(5, table_id);
    let table_object_key = id_catalog_key(2, table_id);
    let column_object_key = id_catalog_key(2, column_id);
    let table_write = catalog_writes
        .iter()
        .find(|write| write.key == table_key && write.payload.is_some())
        .ok_or(CommitError::InvalidCatalog)?;
    let table_bytes = table_write
        .expected_value
        .as_deref()
        .ok_or(CommitError::InvalidCatalog)?;
    let read = database.begin_read().map_err(storage_error)?;
    let catalog = read.open_table(CATALOG).map_err(storage_error)?;
    let table_object_bytes = catalog
        .get(table_object_key.as_slice())
        .map_err(storage_error)?
        .map(|value| value.value().to_vec())
        .ok_or(CommitError::InvalidCatalog)?;
    let column_object_bytes = catalog
        .get(column_object_key.as_slice())
        .map_err(storage_error)?
        .map(|value| value.value().to_vec())
        .ok_or(CommitError::InvalidCatalog)?;
    let owner_id: [u8; 16] = catalog_field_bytes(&table_object_key, &table_object_bytes, 2, 4)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let actor_key = id_catalog_key(10, principal_id);
    let actor_bytes = catalog
        .get(actor_key.as_slice())
        .map_err(storage_error)?
        .map(|value| value.value().to_vec())
        .ok_or(CommitError::DdlAuthorizationDenied)?;
    if decode_principal_kind(&actor_key, &actor_bytes)? != 1
        || catalog_field_bytes(&actor_key, &actor_bytes, 10, 3)? != [1]
        || catalog_field_u64(&actor_key, &actor_bytes, 10, 4)? == 0
    {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    let mut guards = HashMap::<Vec<u8>, Option<Vec<u8>>>::new();
    add_catalog_guard(&mut guards, table_key.clone(), Some(table_bytes.to_vec()));
    add_catalog_guard(
        &mut guards,
        table_object_key.clone(),
        Some(table_object_bytes),
    );
    add_catalog_guard(
        &mut guards,
        column_write.key.clone(),
        Some(column_bytes.to_vec()),
    );
    add_catalog_guard(
        &mut guards,
        column_object_key.clone(),
        Some(column_object_bytes),
    );
    add_catalog_guard(&mut guards, actor_key.clone(), Some(actor_bytes));
    let membership_prefix = 11u16.to_be_bytes();
    let mut memberships = Vec::new();
    let mut subject_ids = HashSet::from([principal_id, owner_id]);
    let mut scanned = 0usize;
    for entry in catalog
        .range(membership_prefix.as_slice()..)
        .map_err(storage_error)?
    {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value().to_vec();
        if !key.starts_with(&membership_prefix) {
            break;
        }
        scanned += 1;
        if scanned > MAX_MEMBERSHIP_EDGES {
            return Err(CommitError::InvalidCatalog);
        }
        let bytes = value.value().to_vec();
        let edge = decode_membership_record(&key, &bytes)?;
        subject_ids.extend([edge.member_id, edge.role_id]);
        memberships.push(edge);
        add_catalog_guard(&mut guards, key, Some(bytes));
    }
    let roles = resolve_role_closure(principal_id, &memberships)
        .map_err(|_| CommitError::InvalidCatalog)?;
    let mut admin_role_id = [0x80; 16];
    admin_role_id[15] = 3;
    if owner_id != principal_id && !roles.contains(&owner_id) && !roles.contains(&admin_role_id) {
        return Err(CommitError::DdlAuthorizationDenied);
    }

    let schema_id: [u8; 16] = catalog_field_bytes(&table_key, table_bytes, 5, 1)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let metadata = read
        .open_table(ENGINE_META)
        .map_err(storage_error)?
        .get(&b"format"[..])
        .map_err(storage_error)?
        .ok_or(CommitError::InvalidMetadata)?;
    let database_id = EngineMetadata::decode(metadata.value())
        .map_err(|_| CommitError::InvalidMetadata)?
        .database_id;
    let database_key = id_catalog_key(1, database_id);
    let database_bytes = catalog
        .get(database_key.as_slice())
        .map_err(storage_error)?
        .map(|value| value.value().to_vec())
        .ok_or(CommitError::InvalidCatalog)?;
    add_catalog_guard(
        &mut guards,
        database_key.clone(),
        Some(database_bytes.clone()),
    );
    let builtins = catalog_field_bytes(&database_key, &database_bytes, 1, 3)?;
    if builtins.len() < 4 {
        return Err(CommitError::InvalidCatalog);
    }
    let builtin_count = u32::from_le_bytes(
        builtins[..4]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?,
    ) as usize;
    if builtin_count > 256 || builtins.len() != 4 + builtin_count * 20 {
        return Err(CommitError::InvalidCatalog);
    }
    if builtins[4..].chunks_exact(20).any(|entry| {
        matches!(u16::from_le_bytes([entry[0], entry[1]]), 1 | 2) && entry[4..20] == schema_id
    }) {
        return Err(CommitError::DdlAuthorizationDenied);
    }

    let mut refs = HashMap::<u16, HashSet<[u8; 16]>>::new();
    for family in [7u16, 8, 9, 14] {
        let prefix = family.to_be_bytes();
        let mut count = 0usize;
        for entry in catalog.range(prefix.as_slice()..).map_err(storage_error)? {
            let (key, value) = entry.map_err(storage_error)?;
            let key = key.value().to_vec();
            if !key.starts_with(&prefix) {
                break;
            }
            count += 1;
            if count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
                return Err(CommitError::InvalidCatalog);
            }
            let bytes = value.value().to_vec();
            add_catalog_guard(&mut guards, key.clone(), Some(bytes.clone()));
            match family {
                7 => {
                    let dependencies = catalog_id_list_field(&key, &bytes, 7, 6)?;
                    refs.entry(7).or_default().extend(dependencies);
                }
                8 => {
                    for tag in [3, 6] {
                        refs.entry(8)
                            .or_default()
                            .extend(catalog_id_list_field(&key, &bytes, 8, tag)?);
                    }
                }
                9 => refs
                    .entry(9)
                    .or_default()
                    .extend(catalog_id_list_field(&key, &bytes, 9, 2)?),
                14 if key.len() == 39 && u16::from_be_bytes([key[20], key[21]]) == 3 => {
                    refs.entry(14).or_default().insert(
                        key[23..39]
                            .try_into()
                            .map_err(|_| CommitError::InvalidCatalog)?,
                    );
                }
                _ => {}
            }
        }
    }
    if refs.values().any(|ids| ids.contains(&column_id)) {
        return Err(CommitError::ColumnHasDependencies);
    }

    let mut owned_ids = HashSet::from([column_id]);
    for tag in [7u16, 10] {
        if let Ok(bytes) = catalog_field_bytes(&column_write.key, column_bytes, 6, tag) {
            let expression_id: [u8; 16] =
                bytes.try_into().map_err(|_| CommitError::InvalidCatalog)?;
            owned_ids.insert(expression_id);
        }
    }
    let mut required_retired_ids = HashSet::<[u8; 16]>::new();
    let mut retired_ids = HashSet::<[u8; 16]>::new();
    let allowed_grants = catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&12u16.to_be_bytes()) && write.payload.is_none())
        .filter_map(|write| {
            let target = write
                .key
                .get(43..59)
                .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())?;
            let grantee = write
                .key
                .get(3..19)
                .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())?;
            (target == column_id).then_some((grantee, target))
        })
        .collect::<HashSet<_>>();
    for write in catalog_writes {
        let family = write
            .key
            .get(..2)
            .map(|bytes| u16::from_be_bytes([bytes[0], bytes[1]]))
            .ok_or(CommitError::InvalidCatalog)?;
        let id = write
            .key
            .get(3..19)
            .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok());
        let belongs = match family {
            2 => {
                let belongs = id.is_some_and(|id| owned_ids.contains(&id));
                if belongs && write.payload.is_none() {
                    let object_id = id.ok_or(CommitError::InvalidCatalog)?;
                    if !matches!(
                        catalog_field_u16(
                            &write.key,
                            write
                                .expected_value
                                .as_deref()
                                .ok_or(CommitError::InvalidCatalog)?,
                            2,
                            1,
                        )?,
                        4 | 5
                    ) || !required_retired_ids.insert(object_id)
                    {
                        return Err(CommitError::InvalidCatalog);
                    }
                }
                belongs
            }
            5 => write.key == table_key,
            6 => {
                id == Some(column_id)
                    || write.expected_value.as_deref().is_some_and(|bytes| {
                        column_table_id(&write.key, bytes).ok() == Some(table_id)
                    })
            }
            7 => id.is_some_and(|id| owned_ids.contains(&id)),
            9 => write.expected_value.as_deref().is_some_and(|bytes| {
                catalog_field_bytes(&write.key, bytes, 9, 1)
                    .is_ok_and(|value| value.as_slice() == table_id)
            }),
            12 => {
                write.key.len() == 59
                    && write.key.get(20..36) == Some(table_id.as_slice())
                    && write.key.get(43..59) == Some(column_id.as_slice())
            }
            15 => {
                let dependent = write
                    .key
                    .get(3..19)
                    .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok());
                let referenced = write
                    .key
                    .get(20..36)
                    .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok());
                let kind = write
                    .key
                    .get(37..39)
                    .and_then(|bytes| <[u8; 2]>::try_from(bytes).ok())
                    .map(u16::from_be_bytes);
                dependent.is_some_and(|id| owned_ids.contains(&id))
                    || (kind == Some(5)
                        && dependent
                            .zip(referenced)
                            .is_some_and(|pair| allowed_grants.contains(&pair)))
            }
            17 => {
                let retired_id = id.ok_or(CommitError::InvalidCatalog)?;
                write.key.len() == 19
                    && write.key[2] == 1
                    && write.payload.is_some()
                    && write.expected_value.is_none()
                    && required_retired_ids.contains(&retired_id)
                    && decode_catalog_record(
                        &write.key,
                        write
                            .payload
                            .as_deref()
                            .ok_or(CommitError::InvalidCatalog)?,
                    )?
                    .fields
                    .is_empty()
                    && retired_ids.insert(retired_id)
            }
            _ => false,
        };
        if !belongs {
            return Err(CommitError::InvalidCatalog);
        }
        if family != 17 && write.expected_value.is_none() {
            return Err(CommitError::InvalidCatalog);
        }
    }
    if retired_ids != required_retired_ids {
        return Err(CommitError::InvalidCatalog);
    }
    subject_ids.extend(roles);
    for subject in subject_ids {
        let key = id_catalog_key(10, subject);
        if let Some(value) = catalog.get(key.as_slice()).map_err(storage_error)? {
            add_catalog_guard(&mut guards, key, Some(value.value().to_vec()));
        }
    }
    let mut result = guards
        .into_iter()
        .map(|(key, expected_value)| CatalogGuard {
            key,
            expected_value,
        })
        .collect::<Vec<_>>();
    result.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(result)
}
