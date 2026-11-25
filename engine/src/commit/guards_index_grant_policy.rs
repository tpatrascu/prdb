use super::fenced_ddl::add_catalog_guard;
use super::*;
use crate::audit::AuditEvent;
use crate::authorization::{MAX_MEMBERSHIP_EDGES, resolve_role_closure, validate_membership_graph};
use crate::catalog::{CatalogRecord, decode_policy_target};
use crate::schema::CATALOG;
use redb::{Database, ReadableDatabase};
use std::collections::{HashMap, HashSet};

pub(super) fn index_ddl_authority_guards(
    database: &Database,
    principal_id: [u8; 16],
    catalog_writes: &[CatalogWrite],
) -> Result<Vec<CatalogGuard>, CommitError> {
    if principal_id == [0; 16] {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    let read = database.begin_read().map_err(storage_error)?;
    let catalog = read.open_table(CATALOG).map_err(storage_error)?;
    let mut changed_index_ids = HashSet::<[u8; 16]>::new();
    let mut index_definition_write_ids = HashSet::<[u8; 16]>::new();
    let mut index_definition_states = HashMap::<[u8; 16], (bool, bool)>::new();
    let mut affected_tables = HashSet::<[u8; 16]>::new();
    let mut changed_object_ids = HashSet::<[u8; 16]>::new();
    let mut object_record_states = HashMap::<[u8; 16], (bool, bool)>::new();
    let mut changed_owned_child_dependents = Vec::<[u8; 16]>::new();
    let mut retired_object_ids = HashSet::<[u8; 16]>::new();
    let mut required_retired_object_ids = HashSet::<[u8; 16]>::new();
    for write in catalog_writes {
        let family = write
            .key
            .get(..2)
            .and_then(|bytes| <[u8; 2]>::try_from(bytes).ok())
            .map(u16::from_be_bytes)
            .ok_or(CommitError::InvalidCatalog)?;
        let current = catalog
            .get(write.key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if current.as_deref() == write.payload.as_deref() && family != 9 {
            continue;
        }
        match family {
            2 => {
                if write.key.len() != 19 {
                    return Err(CommitError::InvalidCatalog);
                }
                let object_id: [u8; 16] = write.key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                changed_object_ids.insert(object_id);
                object_record_states
                    .insert(object_id, (current.is_some(), write.payload.is_some()));
                if let Some(bytes) = current.as_deref()
                    && write.payload.is_none()
                    && matches!(catalog_field_u16(&write.key, bytes, 2, 1)?, 3 | 7)
                {
                    required_retired_object_ids.insert(object_id);
                }
                if current.as_deref() != write.payload.as_deref()
                    && current.as_deref().is_some_and(|bytes| {
                        catalog_field_u16(&write.key, bytes, 2, 1).ok() == Some(7)
                    })
                    && write.payload.as_deref().is_some_and(|bytes| {
                        catalog_field_u16(&write.key, bytes, 2, 1).ok() == Some(7)
                    })
                {
                    let old = current.as_deref().ok_or(CommitError::InvalidCatalog)?;
                    let new = write
                        .payload
                        .as_deref()
                        .ok_or(CommitError::InvalidCatalog)?;
                    let old_version = catalog_field_u64(&write.key, old, 2, 5)?;
                    if catalog_field_bytes(&write.key, old, 2, 2)?
                        != catalog_field_bytes(&write.key, new, 2, 2)?
                        || catalog_field_bytes(&write.key, old, 2, 4)?
                            != catalog_field_bytes(&write.key, new, 2, 4)?
                        || catalog_field_u16(&write.key, old, 2, 6)?
                            != catalog_field_u16(&write.key, new, 2, 6)?
                        || catalog_field_bytes(&write.key, old, 2, 3)?
                            == catalog_field_bytes(&write.key, new, 2, 3)?
                        || catalog_field_u64(&write.key, new, 2, 5)?
                            != old_version
                                .checked_add(1)
                                .ok_or(CommitError::InvalidCatalog)?
                    {
                        return Err(CommitError::InvalidCatalog);
                    }
                    let index_key = id_catalog_key(9, object_id);
                    let index_bytes = catalog
                        .get(index_key.as_slice())
                        .map_err(storage_error)?
                        .ok_or(CommitError::InvalidCatalog)?
                        .value()
                        .to_vec();
                    let (decoded_id, table_id) = decode_index_definition(&index_key, &index_bytes)?;
                    if decoded_id != object_id {
                        return Err(CommitError::InvalidCatalog);
                    }
                    changed_index_ids.insert(object_id);
                    index_definition_states.insert(object_id, (true, true));
                    affected_tables.insert(table_id);
                }
            }
            9 => {
                if write.key.len() != 19 {
                    return Err(CommitError::InvalidCatalog);
                }
                let index_id: [u8; 16] = write.key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                if !index_definition_write_ids.insert(index_id) {
                    return Err(CommitError::InvalidCatalog);
                }
                changed_index_ids.insert(index_id);
                index_definition_states
                    .insert(index_id, (current.is_some(), write.payload.is_some()));
                for bytes in [current.as_deref(), write.payload.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    let (decoded_id, table_id) = decode_index_definition(&write.key, bytes)?;
                    if decoded_id != index_id {
                        return Err(CommitError::InvalidCatalog);
                    }
                    affected_tables.insert(table_id);
                }
            }
            15 => {
                for bytes in [current.as_deref(), write.payload.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    let (dependent_id, _, kind) = decode_dependency_edge(&write.key, bytes)?;
                    if kind != 1 {
                        return Err(CommitError::InvalidCatalog);
                    }
                    changed_owned_child_dependents.push(dependent_id);
                }
            }
            17 => {
                let object_id: [u8; 16] = write
                    .key
                    .get(3..19)
                    .ok_or(CommitError::InvalidCatalog)?
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                let bytes = write
                    .payload
                    .as_deref()
                    .ok_or(CommitError::InvalidCatalog)?;
                let reservation = decode_catalog_record(&write.key, bytes)?;
                if current.is_some()
                    || write.key != id_catalog_key(17, object_id)
                    || reservation.family_code != 17
                    || !reservation.fields.is_empty()
                    || !retired_object_ids.insert(object_id)
                {
                    return Err(CommitError::InvalidCatalog);
                }
            }
            _ => return Err(CommitError::InvalidCatalog),
        }
    }
    if changed_index_ids.is_empty()
        || changed_index_ids.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
        || changed_object_ids
            .iter()
            .any(|object_id| !changed_index_ids.contains(object_id))
        || index_definition_states
            .iter()
            .any(|(index_id, state)| match state {
                (false, true) => object_record_states.get(index_id) != Some(&(false, true)),
                (true, false) => object_record_states.get(index_id) != Some(&(true, false)),
                (true, true) => false,
                _ => true,
            })
        || changed_owned_child_dependents
            .iter()
            .any(|dependent_id| !changed_index_ids.contains(dependent_id))
        || required_retired_object_ids != retired_object_ids
    {
        return Err(CommitError::InvalidCatalog);
    }

    let mut guards = HashMap::<Vec<u8>, Option<Vec<u8>>>::new();
    let principal_key = id_catalog_key(10, principal_id);
    let principal_bytes = catalog
        .get(principal_key.as_slice())
        .map_err(storage_error)?
        .map(|value| value.value().to_vec())
        .ok_or(CommitError::DdlAuthorizationDenied)?;
    let principal_kind = decode_principal_kind(&principal_key, &principal_bytes)?;
    let principal_login = catalog_field_bytes(&principal_key, &principal_bytes, 10, 3)?;
    if principal_kind != 1 || principal_login != [1] {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    principal_authority_epoch(&principal_key, &principal_bytes)?;
    add_catalog_guard(&mut guards, principal_key, Some(principal_bytes));

    let mut memberships = Vec::new();
    let mut membership_count = 0usize;
    let membership_prefix = 11u16.to_be_bytes();
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
        if membership_count > MAX_MEMBERSHIP_EDGES || key[2] != 1 || key[19] != 1 {
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
        memberships.push(membership);
        add_catalog_guard(&mut guards, key, Some(bytes));
    }
    let role_ids = resolve_role_closure(principal_id, &memberships)
        .map_err(|_| CommitError::InvalidCatalog)?;
    let mut authority_subjects = role_ids.clone();
    authority_subjects.insert(principal_id);
    let mut database_admin_role = [0x80; 16];
    database_admin_role[15] = 3;
    let is_database_admin = role_ids.contains(&database_admin_role);
    for role_id in &role_ids {
        let role_key = id_catalog_key(10, *role_id);
        let role_bytes = catalog
            .get(role_key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        if decode_principal_kind(&role_key, &role_bytes)? != 2
            || catalog_field_bytes(&role_key, &role_bytes, 10, 3)? != [0]
        {
            return Err(CommitError::InvalidCatalog);
        }
        principal_authority_epoch(&role_key, &role_bytes)?;
        add_catalog_guard(&mut guards, role_key, Some(role_bytes));
    }

    let mut table_owners = HashMap::new();
    for table_id in &affected_tables {
        if *table_id == [0; 16] {
            return Err(CommitError::InvalidCatalog);
        }
        let object_key = id_catalog_key(2, *table_id);
        let object_bytes = catalog
            .get(object_key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        if catalog_field_u16(&object_key, &object_bytes, 2, 1)? != 3 {
            return Err(CommitError::InvalidCatalog);
        }
        let owner_id: [u8; 16] = catalog_field_bytes(&object_key, &object_bytes, 2, 4)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        if owner_id == [0; 16] {
            return Err(CommitError::InvalidCatalog);
        }
        table_owners.insert(*table_id, owner_id);
        add_catalog_guard(&mut guards, object_key, Some(object_bytes));
    }

    let mut granted_tables = HashSet::new();
    let grant_prefix = 12u16.to_be_bytes();
    let mut grant_count = 0usize;
    for entry in catalog
        .range(grant_prefix.as_slice()..)
        .map_err(storage_error)?
    {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value().to_vec();
        if !key.starts_with(&grant_prefix) {
            break;
        }
        grant_count += 1;
        if grant_count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN
            || key.len() != 59
            || [key[2], key[19], key[36], key[39], key[42]] != [1; 5]
        {
            return Err(CommitError::InvalidCatalog);
        }
        let grantee_id: [u8; 16] = key[3..19]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let object_id: [u8; 16] = key[20..36]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let privilege = u16::from_be_bytes([key[37], key[38]]);
        let scope = u16::from_be_bytes([key[40], key[41]]);
        let target_id: [u8; 16] = key[43..59]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        if !authority_subjects.contains(&grantee_id)
            || privilege != 9
            || scope != 1
            || object_id != target_id
            || !affected_tables.contains(&target_id)
        {
            continue;
        }
        let bytes = value.value().to_vec();
        if catalog_field_u64(&key, &bytes, 12, 2)? == 0
            || catalog_field_bytes(&key, &bytes, 12, 1)? == [0; 16]
        {
            return Err(CommitError::InvalidCatalog);
        }
        granted_tables.insert(target_id);
        add_catalog_guard(&mut guards, key, Some(bytes));
    }

    for (table_id, owner_id) in table_owners {
        if !is_database_admin
            && !authority_subjects.contains(&owner_id)
            && !granted_tables.contains(&table_id)
        {
            return Err(CommitError::DdlAuthorizationDenied);
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

pub(super) fn object_grant_ddl_authority_guards(
    database: &Database,
    principal_id: [u8; 16],
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<Vec<CatalogGuard>, CommitError> {
    if principal_id == [0; 16]
        || audit_events.is_empty()
        || audit_events.iter().any(|event| {
            event.actor_id != Some(principal_id)
                || event.action != crate::audit::AuditAction::AuthorityChange
                || event.result != crate::audit::AuditResult::Succeeded
        })
    {
        return Err(if principal_id == [0; 16] {
            CommitError::DdlAuthorizationDenied
        } else {
            CommitError::InvalidAudit
        });
    }
    let read = database.begin_read().map_err(storage_error)?;
    let catalog = read.open_table(CATALOG).map_err(storage_error)?;
    let principal_key = id_catalog_key(10, principal_id);
    let principal_bytes = catalog
        .get(principal_key.as_slice())
        .map_err(storage_error)?
        .map(|value| value.value().to_vec())
        .ok_or(CommitError::DdlAuthorizationDenied)?;
    if decode_principal_kind(&principal_key, &principal_bytes)? != 1
        || catalog_field_bytes(&principal_key, &principal_bytes, 10, 3)? != [1]
    {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    principal_authority_epoch(&principal_key, &principal_bytes)?;
    let mut guards = HashMap::<Vec<u8>, Option<Vec<u8>>>::new();
    add_catalog_guard(&mut guards, principal_key, Some(principal_bytes));

    let membership_prefix = 11u16.to_be_bytes();
    let mut memberships = Vec::new();
    let mut principal_ids = HashSet::new();
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
        if membership_count > MAX_MEMBERSHIP_EDGES || key.len() != 36 || key[2] != 1 || key[19] != 1
        {
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
    let roles = resolve_role_closure(principal_id, &memberships)
        .map_err(|_| CommitError::InvalidCatalog)?;
    principal_ids.insert(principal_id);
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
            || (memberships.iter().any(|edge| edge.role_id == id) && kind != 2)
        {
            return Err(CommitError::InvalidCatalog);
        }
        add_catalog_guard(&mut guards, key, Some(bytes));
    }
    let mut admin_role_id = [0x80; 16];
    admin_role_id[15] = 3;
    let is_database_admin = roles.contains(&admin_role_id);
    let mut authority_subjects = roles;
    authority_subjects.insert(principal_id);

    let mut changed_grantees = HashSet::new();
    let mut changed_objects = HashSet::new();
    let mut changed_authority_pairs = HashSet::new();
    let mut dependency_pairs = HashSet::new();
    let mut principal_updates = HashSet::new();
    let mut object_owners = HashMap::<[u8; 16], [u8; 16]>::new();
    for write in catalog_writes {
        let family = write
            .key
            .get(..2)
            .and_then(|bytes| <[u8; 2]>::try_from(bytes).ok())
            .map(u16::from_be_bytes)
            .ok_or(CommitError::InvalidCatalog)?;
        let current = catalog
            .get(write.key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if current.as_deref() == write.payload.as_deref() {
            continue;
        }
        match family {
            12 => {
                if write.key.len() != 59
                    || [
                        write.key[2],
                        write.key[19],
                        write.key[36],
                        write.key[39],
                        write.key[42],
                    ] != [1; 5]
                    || current.is_some() == write.payload.is_some()
                {
                    return Err(CommitError::InvalidCatalog);
                }
                let grantee_id: [u8; 16] = write.key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                let object_id: [u8; 16] = write.key[20..36]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                let privilege = u16::from_be_bytes([write.key[37], write.key[38]]);
                let scope = u16::from_be_bytes([write.key[40], write.key[41]]);
                let target_id: [u8; 16] = write.key[43..59]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                if !(1..=11).contains(&privilege)
                    || grantee_id == [0; 16]
                    || object_id == [0; 16]
                    || target_id == [0; 16]
                {
                    return Err(CommitError::InvalidCatalog);
                }
                let object_key = id_catalog_key(2, object_id);
                let object_bytes = catalog
                    .get(object_key.as_slice())
                    .map_err(storage_error)?
                    .map(|value| value.value().to_vec())
                    .ok_or(CommitError::InvalidCatalog)?;
                let object_kind = catalog_field_u16(&object_key, &object_bytes, 2, 1)?;
                let owner_id: [u8; 16] = catalog_field_bytes(&object_key, &object_bytes, 2, 4)?
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                let valid_scope = match privilege {
                    1 | 7 => scope == 1 && object_kind == 1 && target_id == object_id,
                    2..=4 | 6 => {
                        object_kind == 3
                            && ((scope == 1 && target_id == object_id)
                                || (scope == 2 && target_id != object_id))
                    }
                    5 => scope == 1 && object_kind == 3 && target_id == object_id,
                    8..=11 => scope == 1 && matches!(object_kind, 1 | 3) && target_id == object_id,
                    _ => false,
                };
                if !valid_scope || owner_id == [0; 16] {
                    return Err(CommitError::InvalidCatalog);
                }
                if let Some(bytes) = &write.payload
                    && (catalog_field_bytes(&write.key, bytes, 12, 1)? != principal_id
                        || catalog_field_u64(&write.key, bytes, 12, 2)? == 0)
                {
                    return Err(CommitError::DdlAuthorizationDenied);
                }
                changed_grantees.insert(grantee_id);
                changed_objects.insert(object_id);
                changed_authority_pairs.insert((grantee_id, target_id));
                object_owners.insert(object_id, owner_id);
                add_catalog_guard(&mut guards, object_key, Some(object_bytes));
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
                if kind != 5 {
                    return Err(CommitError::InvalidCatalog);
                }
                dependency_pairs.insert((dependent_id, referenced_id));
            }
            10 => {
                if write.key.len() != 19 || current.is_none() || write.payload.is_none() {
                    return Err(CommitError::InvalidCatalog);
                }
                let grantee_id: [u8; 16] = write.key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                let old = current.as_deref().ok_or(CommitError::InvalidCatalog)?;
                let next = write
                    .payload
                    .as_deref()
                    .ok_or(CommitError::InvalidCatalog)?;
                for tag in 1..=3 {
                    if catalog_field_bytes(&write.key, old, 10, tag)?
                        != catalog_field_bytes(&write.key, next, 10, tag)?
                    {
                        return Err(CommitError::InvalidCatalog);
                    }
                }
                if catalog_field_u64(&write.key, next, 10, 4)?
                    <= catalog_field_u64(&write.key, old, 10, 4)?
                {
                    return Err(CommitError::InvalidCatalog);
                }
                principal_updates.insert(grantee_id);
            }
            _ => return Err(CommitError::InvalidCatalog),
        }
    }
    if changed_grantees.is_empty()
        || changed_grantees != principal_updates
        || dependency_pairs
            .iter()
            .any(|pair| !changed_authority_pairs.contains(pair))
    {
        return Err(CommitError::InvalidCatalog);
    }
    for object_id in changed_objects {
        let owner_id = object_owners
            .get(&object_id)
            .copied()
            .ok_or(CommitError::InvalidCatalog)?;
        if is_database_admin || authority_subjects.contains(&owner_id) {
            continue;
        }
        let mut has_manage_grants = false;
        for subject in &authority_subjects {
            let key = grant_key(*subject, object_id, 11, 1, object_id);
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
                has_manage_grants = true;
            }
            add_catalog_guard(&mut guards, key, bytes);
        }
        if !has_manage_grants {
            return Err(CommitError::DdlAuthorizationDenied);
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

pub(super) fn grant_key(
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

pub(super) fn catalog_fields_equal_except(
    left: &CatalogRecord,
    right: &CatalogRecord,
    ignored_tags: &[u16],
) -> bool {
    left.family_code == right.family_code
        && left.logical_key == right.logical_key
        && left
            .fields
            .iter()
            .filter(|field| !ignored_tags.contains(&field.tag))
            .eq(right
                .fields
                .iter()
                .filter(|field| !ignored_tags.contains(&field.tag)))
}

/// Authorize policy DDL for an already authenticated administrative caller.
/// Owners and principals with inherited `MANAGE_POLICIES` may change policies
/// on each affected existing table.
pub(super) fn policy_ddl_authority_guards(
    database: &Database,
    principal_id: [u8; 16],
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<Vec<CatalogGuard>, CommitError> {
    if principal_id == [0; 16] {
        return Err(CommitError::DdlAuthorizationDenied);
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

    let read = database.begin_read().map_err(storage_error)?;
    let catalog = read.open_table(CATALOG).map_err(storage_error)?;
    let mut actual_writes = Vec::new();
    for write in catalog_writes {
        let family = write
            .key
            .get(..2)
            .and_then(|bytes| <[u8; 2]>::try_from(bytes).ok())
            .map(u16::from_be_bytes)
            .ok_or(CommitError::InvalidCatalog)?;
        if !matches!(family, 2 | 5 | 7 | 13 | 14 | 15 | 17) {
            return Err(CommitError::InvalidCatalog);
        }
        let current = catalog
            .get(write.key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if current != write.payload {
            actual_writes.push((family, write, current));
        }
    }

    let mut changed_policies = HashSet::<[u8; 16]>::new();
    let mut policy_tables = HashMap::<[u8; 16], (Option<[u8; 16]>, Option<[u8; 16]>)>::new();
    let mut referenced_expressions = HashSet::<[u8; 16]>::new();
    for (_, write, current) in actual_writes.iter().filter(|(family, _, _)| *family == 13) {
        if write.key.len() != 19 || write.key[2] != 1 {
            return Err(CommitError::InvalidCatalog);
        }
        let policy_id: [u8; 16] = write.key[3..19]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let current_table = current
            .as_deref()
            .map(|bytes| policy_table_id(&write.key, bytes))
            .transpose()?;
        let next_table = write
            .payload
            .as_deref()
            .map(|bytes| policy_table_id(&write.key, bytes))
            .transpose()?;
        if current_table.is_some() && next_table.is_some() && current_table != next_table {
            return Err(CommitError::InvalidCatalog);
        }
        for bytes in [current.as_deref(), write.payload.as_deref()]
            .into_iter()
            .flatten()
        {
            let record = decode_catalog_record(&write.key, bytes)?;
            for tag in [5, 6] {
                if let Some(field) = record.fields.iter().find(|field| field.tag == tag) {
                    let expression_id: [u8; 16] = field
                        .payload
                        .as_slice()
                        .try_into()
                        .map_err(|_| CommitError::InvalidCatalog)?;
                    referenced_expressions.insert(expression_id);
                }
            }
        }
        changed_policies.insert(policy_id);
        policy_tables.insert(policy_id, (current_table, next_table));
    }
    if changed_policies.is_empty()
        || changed_policies.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS
    {
        return Err(CommitError::InvalidCatalog);
    }

    let mut target_policy_ids = HashSet::<[u8; 16]>::new();
    for (_, write, current) in actual_writes.iter().filter(|(family, _, _)| *family == 14) {
        if write.key.len() != 39 {
            return Err(CommitError::InvalidCatalog);
        }
        let policy_id: [u8; 16] = write.key[3..19]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        for bytes in [current.as_deref(), write.payload.as_deref()]
            .into_iter()
            .flatten()
        {
            let target = decode_policy_target(bytes).map_err(|_| CommitError::InvalidCatalog)?;
            if target.policy_id != policy_id || bytes[32..] != write.key {
                return Err(CommitError::InvalidCatalog);
            }
        }
        target_policy_ids.insert(policy_id);
    }
    if target_policy_ids
        .iter()
        .any(|policy_id| !changed_policies.contains(policy_id))
    {
        return Err(CommitError::InvalidCatalog);
    }

    let mut changed_expressions = HashSet::<[u8; 16]>::new();
    for (_, write, current) in actual_writes.iter().filter(|(family, _, _)| *family == 7) {
        if write.key.len() != 19 || current.is_some() == write.payload.is_some() {
            return Err(CommitError::InvalidCatalog);
        }
        let expression_id: [u8; 16] = write.key[3..19]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        if !referenced_expressions.contains(&expression_id) {
            return Err(CommitError::InvalidCatalog);
        }
        changed_expressions.insert(expression_id);
    }

    let mut object_states = HashMap::<[u8; 16], (u16, bool, bool)>::new();
    for (_, write, current) in actual_writes.iter().filter(|(family, _, _)| *family == 2) {
        if write.key.len() != 19 || current.is_some() == write.payload.is_some() {
            return Err(CommitError::InvalidCatalog);
        }
        let object_id: [u8; 16] = write.key[3..19]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let kind = current
            .as_deref()
            .or(write.payload.as_deref())
            .map(|bytes| catalog_field_u16(&write.key, bytes, 2, 1))
            .transpose()?
            .ok_or(CommitError::InvalidCatalog)?;
        if !matches!(kind, 5 | 10) {
            return Err(CommitError::InvalidCatalog);
        }
        if let Some(old) = current.as_deref()
            && catalog_field_u16(&write.key, old, 2, 1)? != kind
        {
            return Err(CommitError::InvalidCatalog);
        }
        if let Some(next) = write.payload.as_deref()
            && catalog_field_u16(&write.key, next, 2, 1)? != kind
        {
            return Err(CommitError::InvalidCatalog);
        }
        object_states.insert(
            object_id,
            (kind, current.is_some(), write.payload.is_some()),
        );
    }

    let expected_retired_ids = object_states
        .iter()
        .filter_map(|(object_id, (kind, was_present, is_present))| {
            (*was_present && !*is_present && matches!(kind, 5 | 10)).then_some(*object_id)
        })
        .collect::<HashSet<_>>();
    let mut retired_ids = HashSet::new();
    for (_, write, current) in actual_writes.iter().filter(|(family, _, _)| *family == 17) {
        if write.key.len() != 19
            || write.key[2] != 1
            || current.is_some()
            || write.expected_value.is_some()
            || write.payload.is_none()
        {
            return Err(CommitError::InvalidCatalog);
        }
        let object_id: [u8; 16] = write.key[3..19]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        if !expected_retired_ids.contains(&object_id)
            || !decode_catalog_record(
                &write.key,
                write
                    .payload
                    .as_deref()
                    .ok_or(CommitError::InvalidCatalog)?,
            )?
            .fields
            .is_empty()
            || !retired_ids.insert(object_id)
        {
            return Err(CommitError::InvalidCatalog);
        }
    }
    if retired_ids != expected_retired_ids {
        return Err(CommitError::InvalidCatalog);
    }

    for policy_id in &changed_policies {
        let (current, next) = policy_tables
            .get(policy_id)
            .copied()
            .ok_or(CommitError::InvalidCatalog)?;
        let policy_write = actual_writes
            .iter()
            .find(|(family, write, _)| *family == 13 && write.key == id_catalog_key(13, *policy_id))
            .ok_or(CommitError::InvalidCatalog)?;
        let mut target_prefix = 14u16.to_be_bytes().to_vec();
        target_prefix.push(1);
        target_prefix.extend_from_slice(policy_id);
        let mut final_targets = HashSet::<Vec<u8>>::new();
        for entry in catalog
            .range(target_prefix.as_slice()..)
            .map_err(storage_error)?
        {
            let (key, _) = entry.map_err(storage_error)?;
            let key = key.value().to_vec();
            if !key.starts_with(&target_prefix) {
                break;
            }
            if key.len() != 39 || !final_targets.insert(key) {
                return Err(CommitError::InvalidCatalog);
            }
            if final_targets.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                return Err(CommitError::InvalidCatalog);
            }
        }
        for (_, target_write, _) in actual_writes
            .iter()
            .filter(|(family, write, _)| *family == 14 && write.key.starts_with(&target_prefix))
        {
            if target_write.payload.is_some() {
                final_targets.insert(target_write.key.clone());
            } else {
                final_targets.remove(&target_write.key);
            }
        }
        if policy_write.1.payload.is_some() == final_targets.is_empty() {
            return Err(CommitError::InvalidCatalog);
        }
        if policy_write.2.is_some() != policy_write.1.payload.is_some() {
            let object_state = object_states.get(policy_id);
            if object_state
                != Some(&(
                    10,
                    policy_write.2.is_some(),
                    policy_write.1.payload.is_some(),
                ))
            {
                return Err(CommitError::InvalidCatalog);
            }
        } else if object_states.contains_key(policy_id) {
            return Err(CommitError::InvalidCatalog);
        }
        let table_id = current.or(next).ok_or(CommitError::InvalidCatalog)?;
        if table_id == [0; 16] {
            return Err(CommitError::InvalidCatalog);
        }
    }
    for expression_id in &changed_expressions {
        let expression_write = actual_writes
            .iter()
            .find(|(family, write, _)| {
                *family == 7 && write.key == id_catalog_key(7, *expression_id)
            })
            .ok_or(CommitError::InvalidCatalog)?;
        if object_states.get(expression_id)
            != Some(&(
                5,
                expression_write.2.is_some(),
                expression_write.1.payload.is_some(),
            ))
        {
            return Err(CommitError::InvalidCatalog);
        }
    }
    if object_states.iter().any(|(object_id, (kind, _, _))| {
        (*kind == 10 && !changed_policies.contains(object_id))
            || (*kind == 5 && !changed_expressions.contains(object_id))
    }) {
        return Err(CommitError::InvalidCatalog);
    }

    let mut affected_tables = HashSet::<[u8; 16]>::new();
    for (current_table, next_table) in policy_tables.values() {
        affected_tables.extend(current_table.iter().copied());
        affected_tables.extend(next_table.iter().copied());
    }
    for (_, write, current) in actual_writes.iter().filter(|(family, _, _)| *family == 5) {
        if write.key.len() != 19 || current.is_none() || write.payload.is_none() {
            return Err(CommitError::InvalidCatalog);
        }
        let table_id: [u8; 16] = write.key[3..19]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let next = decode_catalog_record(&write.key, write.payload.as_deref().unwrap())?;
        let old = decode_catalog_record(&write.key, current.as_deref().unwrap())?;
        if !affected_tables.contains(&table_id)
            || !catalog_fields_equal_except(&old, &next, &[3])
            || table_write_epoch(&write.key, write.payload.as_deref().unwrap())?
                <= table_write_epoch(&write.key, current.as_deref().unwrap())?
        {
            return Err(CommitError::InvalidCatalog);
        }
    }

    for (_, write, current) in actual_writes.iter().filter(|(family, _, _)| *family == 15) {
        let bytes = current
            .as_deref()
            .or(write.payload.as_deref())
            .ok_or(CommitError::InvalidCatalog)?;
        let (dependent_id, _, kind) = decode_dependency_edge(&write.key, bytes)?;
        if !matches!(kind, 2 | 5)
            || !(changed_policies.contains(&dependent_id)
                || kind == 2 && changed_expressions.contains(&dependent_id))
        {
            return Err(CommitError::InvalidCatalog);
        }
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
            || (memberships.iter().any(|edge| edge.role_id == id) && kind != 2)
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
        add_catalog_guard(&mut guards, table_key, Some(table_bytes));
        add_catalog_guard(&mut guards, table_record_key, Some(table_record_bytes));
        add_catalog_guard(&mut guards, owner_key, Some(owner_bytes));
        add_catalog_guard(&mut guards, schema_key, Some(schema_bytes));
        add_catalog_guard(&mut guards, schema_record_key, Some(schema_record_bytes));
        if authority_subjects.contains(&owner_id) {
            continue;
        }
        let mut has_manage_policies = false;
        for subject in &authority_subjects {
            for object_id in [*table_id, schema_id] {
                let key = grant_key(*subject, object_id, 10, 1, object_id);
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
                    has_manage_policies = true;
                }
                add_catalog_guard(&mut guards, key, bytes);
            }
        }
        if !has_manage_policies {
            return Err(CommitError::DdlAuthorizationDenied);
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
