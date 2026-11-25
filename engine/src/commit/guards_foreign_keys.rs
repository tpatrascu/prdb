use super::fenced_ddl::add_catalog_guard;
use super::*;
use crate::audit::AuditEvent;
use crate::authorization::{MAX_MEMBERSHIP_EDGES, resolve_role_closure, validate_membership_graph};
use crate::schema::CATALOG;
use redb::{Database, ReadableDatabase};
use std::collections::{HashMap, HashSet};

pub(super) fn foreign_key_ddl_authority_guards(
    database: &Database,
    principal_id: [u8; 16],
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<Vec<CatalogGuard>, CommitError> {
    if principal_id == [0; 16] {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    if catalog_writes.is_empty()
        || catalog_writes.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
    {
        return Err(CommitError::InvalidCatalog);
    }
    if audit_events.is_empty()
        || audit_events.iter().any(|event| {
            event.actor_id != Some(principal_id)
                || event.action != crate::audit::AuditAction::Ddl
                || event.result != crate::audit::AuditResult::Succeeded
        })
    {
        return Err(CommitError::InvalidAudit);
    }

    #[derive(Clone)]
    struct ForeignKeyChange {
        target_constraint_id: [u8; 16],
        target_table_id: [u8; 16],
        target_columns: Vec<[u8; 16]>,
        current_exists: bool,
        next_exists: bool,
    }

    let read = database.begin_read().map_err(storage_error)?;
    let catalog = read.open_table(CATALOG).map_err(storage_error)?;
    let mut changes = HashMap::<[u8; 16], ForeignKeyChange>::new();
    let mut object_changes = HashMap::<[u8; 16], (u16, bool, bool)>::new();
    let mut dependency_changes = HashMap::<([u8; 16], [u8; 16]), (u16, bool, bool)>::new();
    let mut tombstone_changes = HashSet::<[u8; 16]>::new();
    let mut updated_tables = HashSet::<[u8; 16]>::new();
    let mut affected_tables = HashSet::<[u8; 16]>::new();
    let mut source_tables = HashSet::<[u8; 16]>::new();
    let mut references_by_table = HashMap::<[u8; 16], HashSet<[u8; 16]>>::new();
    let mut referenced_column_count = 0usize;

    for write in catalog_writes {
        let family = write
            .key
            .get(..2)
            .and_then(|bytes| <[u8; 2]>::try_from(bytes).ok())
            .map(u16::from_be_bytes)
            .ok_or(CommitError::InvalidCatalog)?;
        if !matches!(family, 2 | 5 | 8 | 15 | 17) {
            return Err(CommitError::InvalidCatalog);
        }
        let current = catalog
            .get(write.key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if current == write.payload {
            continue;
        }
        if family == 17 {
            let retired_id: [u8; 16] = write
                .key
                .get(3..19)
                .ok_or(CommitError::InvalidCatalog)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            if write.key.len() != 19
                || write.key[2] != 1
                || retired_id == [0; 16]
                || current.is_some()
                || write.expected_value.is_some()
                || write.payload.is_none()
                || !decode_catalog_record(
                    &write.key,
                    write
                        .payload
                        .as_deref()
                        .ok_or(CommitError::InvalidCatalog)?,
                )?
                .fields
                .is_empty()
                || !tombstone_changes.insert(retired_id)
            {
                return Err(CommitError::InvalidCatalog);
            }
            continue;
        }
        match family {
            2 => {
                if write.key.len() != 19 || current.is_some() == write.payload.is_some() {
                    return Err(CommitError::InvalidCatalog);
                }
                let object_id: [u8; 16] = write.key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                let object_bytes = current
                    .as_deref()
                    .or(write.payload.as_deref())
                    .ok_or(CommitError::InvalidCatalog)?;
                let kind = catalog_field_u16(&write.key, object_bytes, 2, 1)?;
                if kind != 6
                    || object_changes
                        .insert(
                            object_id,
                            (kind, current.is_some(), write.payload.is_some()),
                        )
                        .is_some()
                {
                    return Err(CommitError::InvalidCatalog);
                }
            }
            5 => {
                if write.key.len() != 19 || current.is_none() || write.payload.is_none() {
                    return Err(CommitError::InvalidCatalog);
                }
                let table_id: [u8; 16] = write.key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                let current_bytes = current.as_deref().ok_or(CommitError::InvalidCatalog)?;
                let next_bytes = write
                    .payload
                    .as_deref()
                    .ok_or(CommitError::InvalidCatalog)?;
                let current_record = decode_catalog_record(&write.key, current_bytes)?;
                let next_record = decode_catalog_record(&write.key, next_bytes)?;
                if !catalog_fields_equal_except(&current_record, &next_record, &[3])
                    || table_write_epoch(&write.key, next_bytes)?
                        <= table_write_epoch(&write.key, current_bytes)?
                    || !updated_tables.insert(table_id)
                {
                    return Err(CommitError::InvalidCatalog);
                }
            }
            8 => {
                if write.key.len() != 19 || current.is_some() == write.payload.is_some() {
                    return Err(CommitError::InvalidCatalog);
                }
                let constraint_id: [u8; 16] = write.key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                let bytes = current
                    .as_deref()
                    .or(write.payload.as_deref())
                    .ok_or(CommitError::InvalidCatalog)?;
                if catalog_field_u16(&write.key, bytes, 8, 2)? != 5 {
                    return Err(CommitError::InvalidCatalog);
                }
                let source_table_id = constraint_table_id(&write.key, bytes)?;
                let target_constraint_id: [u8; 16] = catalog_field_bytes(&write.key, bytes, 8, 5)?
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                let target_constraint_key = id_catalog_key(8, target_constraint_id);
                let target_constraint_bytes = catalog
                    .get(target_constraint_key.as_slice())
                    .map_err(storage_error)?
                    .map(|value| value.value().to_vec())
                    .ok_or(CommitError::InvalidCatalog)?;
                let target_kind =
                    catalog_field_u16(&target_constraint_key, &target_constraint_bytes, 8, 2)?;
                let target_table_id =
                    constraint_table_id(&target_constraint_key, &target_constraint_bytes)?;
                let target_columns =
                    catalog_id_list_field(&target_constraint_key, &target_constraint_bytes, 8, 3)?;
                let referenced_columns = catalog_id_list_field(&write.key, bytes, 8, 6)?;
                if !matches!(target_kind, 2 | 3)
                    || source_table_id == [0; 16]
                    || target_table_id == [0; 16]
                    || target_columns.is_empty()
                    || target_columns != referenced_columns
                    || target_columns.contains(&[0; 16])
                    || target_columns.iter().collect::<HashSet<_>>().len() != target_columns.len()
                {
                    return Err(CommitError::InvalidCatalog);
                }
                referenced_column_count += target_columns.len();
                if referenced_column_count > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                    return Err(CommitError::InvalidCatalog);
                }
                if changes
                    .insert(
                        constraint_id,
                        ForeignKeyChange {
                            target_constraint_id,
                            target_table_id,
                            target_columns: target_columns.clone(),
                            current_exists: current.is_some(),
                            next_exists: write.payload.is_some(),
                        },
                    )
                    .is_some()
                {
                    return Err(CommitError::InvalidCatalog);
                }
                source_tables.insert(source_table_id);
                affected_tables.insert(source_table_id);
                affected_tables.insert(target_table_id);
                if write.payload.is_some() {
                    references_by_table
                        .entry(target_table_id)
                        .or_default()
                        .extend(target_columns);
                }
            }
            15 => {
                if current.is_some() == write.payload.is_some() {
                    return Err(CommitError::InvalidCatalog);
                }
                let bytes = current
                    .as_deref()
                    .or(write.payload.as_deref())
                    .ok_or(CommitError::InvalidCatalog)?;
                let (dependent_id, referenced_id, kind) =
                    decode_dependency_edge(&write.key, bytes)?;
                if kind != 3
                    || dependency_changes
                        .insert(
                            (dependent_id, referenced_id),
                            (kind, current.is_some(), write.payload.is_some()),
                        )
                        .is_some()
                {
                    return Err(CommitError::InvalidCatalog);
                }
            }
            _ => return Err(CommitError::InvalidCatalog),
        }
    }

    if changes.is_empty()
        || changes.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
        || affected_tables.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
        || object_changes.len() != changes.len()
        || dependency_changes.len() != changes.len()
        || updated_tables != affected_tables
    {
        return Err(CommitError::InvalidCatalog);
    }
    for (constraint_id, change) in &changes {
        if object_changes.get(constraint_id)
            != Some(&(6, change.current_exists, change.next_exists))
            || dependency_changes.get(&(*constraint_id, change.target_constraint_id))
                != Some(&(3, change.current_exists, change.next_exists))
        {
            return Err(CommitError::InvalidCatalog);
        }
    }
    let required_tombstones = changes
        .iter()
        .filter_map(|(constraint_id, change)| {
            (change.current_exists && !change.next_exists).then_some(*constraint_id)
        })
        .collect::<HashSet<_>>();
    if tombstone_changes != required_tombstones {
        return Err(CommitError::InvalidCatalog);
    }
    if object_changes.keys().any(|id| !changes.contains_key(id))
        || dependency_changes
            .keys()
            .any(|(dependent, _)| !changes.contains_key(dependent))
    {
        return Err(CommitError::InvalidCatalog);
    }

    let actor_key = id_catalog_key(10, principal_id);
    let actor_bytes = catalog
        .get(actor_key.as_slice())
        .map_err(storage_error)?
        .map(|value| value.value().to_vec())
        .ok_or(CommitError::DdlAuthorizationDenied)?;
    if decode_principal_kind(&actor_key, &actor_bytes)? != 1
        || catalog_field_bytes(&actor_key, &actor_bytes, 10, 3)? != [1]
    {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    principal_authority_epoch(&actor_key, &actor_bytes)?;
    let mut guards = HashMap::<Vec<u8>, Option<Vec<u8>>>::new();
    add_catalog_guard(&mut guards, actor_key, Some(actor_bytes));

    let membership_prefix = 11u16.to_be_bytes();
    let mut memberships = Vec::new();
    let mut principal_ids = HashSet::from([principal_id]);
    let mut membership_count = 0usize;
    for entry in catalog
        .range(membership_prefix.as_slice()..)
        .map_err(storage_error)?
    {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value().to_vec();
        if !key.starts_with(&membership_prefix) {
            break;
        }
        membership_count += 1;
        if membership_count > MAX_MEMBERSHIP_EDGES || key.len() != 36 {
            return Err(CommitError::InvalidCatalog);
        }
        let bytes = value.value().to_vec();
        let membership = decode_membership_record(&key, &bytes)?;
        if membership.member_id == [0; 16]
            || membership.role_id == [0; 16]
            || catalog_field_bytes(&key, &bytes, 11, 1)? == [0; 16]
            || catalog_field_u64(&key, &bytes, 11, 2)? == 0
        {
            return Err(CommitError::InvalidCatalog);
        }
        principal_ids.insert(membership.member_id);
        principal_ids.insert(membership.role_id);
        memberships.push(membership);
        add_catalog_guard(&mut guards, key, Some(bytes));
    }
    validate_membership_graph(&memberships).map_err(|_| CommitError::InvalidCatalog)?;
    let mut principals = HashMap::<[u8; 16], Vec<u8>>::new();
    for id in principal_ids {
        let key = id_catalog_key(10, id);
        let bytes = catalog
            .get(key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        let kind = decode_principal_kind(&key, &bytes)?;
        if catalog_field_u64(&key, &bytes, 10, 4)? == 0
            || (id == principal_id
                && (kind != 1 || catalog_field_bytes(&key, &bytes, 10, 3)? != [1]))
            || (memberships.iter().any(|edge| edge.role_id == id)
                && (kind != 2 || catalog_field_bytes(&key, &bytes, 10, 3)? != [0]))
        {
            return Err(CommitError::InvalidCatalog);
        }
        principals.insert(id, bytes.clone());
        add_catalog_guard(&mut guards, key, Some(bytes));
    }
    for edge in &memberships {
        if decode_principal_kind(
            &id_catalog_key(10, edge.role_id),
            principals
                .get(&edge.role_id)
                .ok_or(CommitError::InvalidCatalog)?,
        )? != 2
        {
            return Err(CommitError::InvalidCatalog);
        }
    }
    let roles = resolve_role_closure(principal_id, &memberships)
        .map_err(|_| CommitError::InvalidCatalog)?;
    let mut authority_subjects = roles;
    authority_subjects.insert(principal_id);

    let mut table_owners = HashMap::<[u8; 16], [u8; 16]>::new();
    let mut table_schemas = HashMap::<[u8; 16], [u8; 16]>::new();
    for table_id in &affected_tables {
        let table_key = id_catalog_key(2, *table_id);
        let table_bytes = catalog
            .get(table_key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        if catalog_field_u16(&table_key, &table_bytes, 2, 1)? != 3 {
            return Err(CommitError::InvalidCatalog);
        }
        let owner_id: [u8; 16] = catalog_field_bytes(&table_key, &table_bytes, 2, 4)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        if owner_id == [0; 16] {
            return Err(CommitError::InvalidCatalog);
        }
        let owner_key = id_catalog_key(10, owner_id);
        let owner_bytes = catalog
            .get(owner_key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        if !matches!(decode_principal_kind(&owner_key, &owner_bytes)?, 1 | 2) {
            return Err(CommitError::InvalidCatalog);
        }
        let table_record_key = id_catalog_key(5, *table_id);
        let table_record_bytes = catalog
            .get(table_record_key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        let schema_id: [u8; 16] =
            catalog_field_bytes(&table_record_key, &table_record_bytes, 5, 1)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
        if schema_id == [0; 16] {
            return Err(CommitError::InvalidCatalog);
        }
        let schema_key = id_catalog_key(2, schema_id);
        let schema_bytes = catalog
            .get(schema_key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        if catalog_field_u16(&schema_key, &schema_bytes, 2, 1)? != 1 {
            return Err(CommitError::InvalidCatalog);
        }
        let schema_record_key = id_catalog_key(3, schema_id);
        let schema_record_bytes = catalog
            .get(schema_record_key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        decode_catalog_record(&schema_record_key, &schema_record_bytes)?;
        table_owners.insert(*table_id, owner_id);
        table_schemas.insert(*table_id, schema_id);
        add_catalog_guard(&mut guards, table_key, Some(table_bytes));
        add_catalog_guard(&mut guards, owner_key, Some(owner_bytes));
        add_catalog_guard(&mut guards, table_record_key, Some(table_record_bytes));
        add_catalog_guard(&mut guards, schema_key, Some(schema_bytes));
        add_catalog_guard(&mut guards, schema_record_key, Some(schema_record_bytes));
    }

    for change in changes.values() {
        let target_constraint_key = id_catalog_key(8, change.target_constraint_id);
        let target_constraint_bytes = catalog
            .get(target_constraint_key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        add_catalog_guard(
            &mut guards,
            target_constraint_key.clone(),
            Some(target_constraint_bytes),
        );
        let target_object_key = id_catalog_key(2, change.target_constraint_id);
        let target_object_bytes = catalog
            .get(target_object_key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        if catalog_field_u16(&target_object_key, &target_object_bytes, 2, 1)? != 6 {
            return Err(CommitError::InvalidCatalog);
        }
        add_catalog_guard(&mut guards, target_object_key, Some(target_object_bytes));
        for column_id in &change.target_columns {
            let column_key = id_catalog_key(6, *column_id);
            let column_bytes = catalog
                .get(column_key.as_slice())
                .map_err(storage_error)?
                .map(|value| value.value().to_vec())
                .ok_or(CommitError::InvalidCatalog)?;
            if column_table_id(&column_key, &column_bytes)? != change.target_table_id {
                return Err(CommitError::InvalidCatalog);
            }
            let column_object_key = id_catalog_key(2, *column_id);
            let column_object_bytes = catalog
                .get(column_object_key.as_slice())
                .map_err(storage_error)?
                .map(|value| value.value().to_vec())
                .ok_or(CommitError::InvalidCatalog)?;
            if catalog_field_u16(&column_object_key, &column_object_bytes, 2, 1)? != 4 {
                return Err(CommitError::InvalidCatalog);
            }
            add_catalog_guard(&mut guards, column_key, Some(column_bytes));
            add_catalog_guard(&mut guards, column_object_key, Some(column_object_bytes));
        }
    }

    let mut grant_lookups = 0usize;
    for table_id in &source_tables {
        let owner_id = *table_owners
            .get(table_id)
            .ok_or(CommitError::InvalidCatalog)?;
        if authority_subjects.contains(&owner_id) {
            continue;
        }
        let schema_id = *table_schemas
            .get(table_id)
            .ok_or(CommitError::InvalidCatalog)?;
        let mut has_alter = false;
        for subject in &authority_subjects {
            for object_id in [*table_id, schema_id] {
                grant_lookups += 1;
                if grant_lookups > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                    return Err(CommitError::InvalidCatalog);
                }
                let key = grant_key(*subject, object_id, 8, 1, object_id);
                let bytes = catalog
                    .get(key.as_slice())
                    .map_err(storage_error)?
                    .map(|value| value.value().to_vec());
                if let Some(bytes) = &bytes {
                    if catalog_field_bytes(&key, bytes, 12, 1)? == [0; 16]
                        || catalog_field_u64(&key, bytes, 12, 2)? == 0
                    {
                        return Err(CommitError::InvalidCatalog);
                    }
                    has_alter = true;
                }
                add_catalog_guard(&mut guards, key, bytes);
            }
        }
        if !has_alter {
            return Err(CommitError::DdlAuthorizationDenied);
        }
    }

    for (table_id, columns) in &references_by_table {
        let mut has_table_references = false;
        for subject in &authority_subjects {
            grant_lookups += 1;
            if grant_lookups > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                return Err(CommitError::InvalidCatalog);
            }
            let key = grant_key(*subject, *table_id, 6, 1, *table_id);
            let bytes = catalog
                .get(key.as_slice())
                .map_err(storage_error)?
                .map(|value| value.value().to_vec());
            if let Some(bytes) = &bytes {
                if catalog_field_bytes(&key, bytes, 12, 1)? == [0; 16]
                    || catalog_field_u64(&key, bytes, 12, 2)? == 0
                {
                    return Err(CommitError::InvalidCatalog);
                }
                has_table_references = true;
            }
            add_catalog_guard(&mut guards, key, bytes);
        }
        if has_table_references {
            continue;
        }
        for column_id in columns {
            let mut has_column_references = false;
            for subject in &authority_subjects {
                grant_lookups += 1;
                if grant_lookups > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                    return Err(CommitError::InvalidCatalog);
                }
                let key = grant_key(*subject, *table_id, 6, 2, *column_id);
                let bytes = catalog
                    .get(key.as_slice())
                    .map_err(storage_error)?
                    .map(|value| value.value().to_vec());
                if let Some(bytes) = &bytes {
                    if catalog_field_bytes(&key, bytes, 12, 1)? == [0; 16]
                        || catalog_field_u64(&key, bytes, 12, 2)? == 0
                    {
                        return Err(CommitError::InvalidCatalog);
                    }
                    has_column_references = true;
                }
                add_catalog_guard(&mut guards, key, bytes);
            }
            if !has_column_references {
                return Err(CommitError::DdlAuthorizationDenied);
            }
        }
    }

    let mut new_foreign_key_names = HashMap::<[u8; 16], HashSet<Vec<u8>>>::new();
    for (constraint_id, (kind, was_present, is_present)) in &object_changes {
        if *kind != 6 || *was_present || !*is_present {
            continue;
        }
        if !changes.contains_key(constraint_id) {
            return Err(CommitError::InvalidCatalog);
        }
        let source_table_id = catalog_writes
            .iter()
            .find(|write| write.key == id_catalog_key(8, *constraint_id))
            .and_then(|write| write.payload.as_deref())
            .map(|bytes| catalog_field_bytes(&id_catalog_key(8, *constraint_id), bytes, 8, 1))
            .transpose()?
            .ok_or(CommitError::InvalidCatalog)?
            .as_slice()
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let object_key = id_catalog_key(2, *constraint_id);
        let object_bytes = catalog_writes
            .iter()
            .find(|write| write.key == object_key)
            .and_then(|write| write.payload.as_deref())
            .ok_or(CommitError::InvalidCatalog)?;
        let name = catalog_field_bytes(&object_key, object_bytes, 2, 3)?;
        if !is_normalized_constraint_name(&name)
            || !new_foreign_key_names
                .entry(source_table_id)
                .or_default()
                .insert(name.clone())
        {
            return Err(CommitError::DuplicateConstraintName);
        }
        let constraint_prefix = 8u16.to_be_bytes();
        for entry in catalog
            .range(constraint_prefix.as_slice()..)
            .map_err(storage_error)?
        {
            let (key, value) = entry.map_err(storage_error)?;
            let key = key.value();
            if !key.starts_with(&constraint_prefix) {
                break;
            }
            if key.len() != 19 {
                return Err(CommitError::InvalidCatalog);
            }
            let existing_id: [u8; 16] = key[3..]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let existing_constraint_key = id_catalog_key(8, existing_id);
            if catalog_field_bytes(&existing_constraint_key, value.value(), 8, 1)?
                != source_table_id
                || changes.contains_key(&existing_id)
            {
                continue;
            }
            let existing_object_key = id_catalog_key(2, existing_id);
            let existing_object = catalog
                .get(existing_object_key.as_slice())
                .map_err(storage_error)?
                .ok_or(CommitError::InvalidCatalog)?;
            if catalog_field_bytes(&existing_object_key, existing_object.value(), 2, 3)? == name {
                return Err(CommitError::DuplicateConstraintName);
            }
        }
    }

    let mut guards = guards
        .into_iter()
        .map(|(key, expected_value)| CatalogGuard {
            key,
            expected_value,
        })
        .collect::<Vec<_>>();
    guards.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(guards)
}
