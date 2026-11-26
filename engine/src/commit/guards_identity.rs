use super::fenced_ddl::add_catalog_guard;
use super::*;
use crate::audit::AuditEvent;
use crate::authorization::{MAX_MEMBERSHIP_EDGES, resolve_role_closure, validate_membership_graph};
use crate::credentials::decode_verifier_record;
use crate::metadata::EngineMetadata;
use crate::schema::{CATALOG, ENGINE_META};
use redb::{Database, ReadOnlyTable, ReadableDatabase};
use std::collections::{HashMap, HashSet};

pub(super) fn membership_ddl_authority_guards(
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
                || event.action != crate::audit::AuditAction::AuthorityChange
                || event.result != crate::audit::AuditResult::Succeeded
        })
    {
        return Err(CommitError::InvalidAudit);
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
    for entry in catalog
        .range(membership_prefix.as_slice()..)
        .map_err(storage_error)?
    {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value().to_vec();
        if !key.starts_with(&membership_prefix) {
            break;
        }
        if key.len() != 36 || key[2] != 1 || key[19] != 1 {
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
    let metadata = read
        .open_table(ENGINE_META)
        .map_err(storage_error)?
        .get(&b"format"[..])
        .map_err(storage_error)?
        .ok_or(CommitError::InvalidMetadata)?;
    let database_id = EngineMetadata::decode(metadata.value())
        .map_err(|_| CommitError::InvalidMetadata)?
        .database_id;
    let mut has_manage_roles = false;
    for subject in authority_subjects {
        let key = grant_key(subject, database_id, 13, 1, database_id);
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
            has_manage_roles = true;
        }
        add_catalog_guard(&mut guards, key, bytes);
    }

    let mut membership_pairs = HashSet::new();
    let mut membership_updates = HashMap::new();
    let mut changed_members = HashSet::new();
    let mut dependency_pairs = HashSet::new();
    let mut principal_updates = HashSet::new();
    let mut targets_admin_role = false;
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
            11 => {
                if write.key.len() != 36 || current.is_some() == write.payload.is_some() {
                    return Err(CommitError::InvalidCatalog);
                }
                let pair = if let Some(bytes) = current.as_deref() {
                    decode_membership_record(&write.key, bytes)?
                } else {
                    decode_membership_record(
                        &write.key,
                        write
                            .payload
                            .as_deref()
                            .ok_or(CommitError::InvalidCatalog)?,
                    )?
                };
                if let Some(bytes) = &write.payload
                    && catalog_field_bytes(&write.key, bytes, 11, 1)? != principal_id
                {
                    return Err(CommitError::DdlAuthorizationDenied);
                }
                if !membership_pairs.insert((pair.member_id, pair.role_id)) {
                    return Err(CommitError::InvalidCatalog);
                }
                membership_updates.insert((pair.member_id, pair.role_id), write.payload.is_some());
                changed_members.insert(pair.member_id);
                targets_admin_role |= pair.role_id == admin_role_id;
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
                let member_id: [u8; 16] = write.key[3..19]
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
                principal_updates.insert(member_id);
            }
            _ => return Err(CommitError::InvalidCatalog),
        }
    }
    if membership_pairs.is_empty()
        || membership_pairs != dependency_pairs
        || changed_members != principal_updates
    {
        return Err(CommitError::InvalidCatalog);
    }
    let current_edges = memberships
        .iter()
        .map(|edge| (edge.member_id, edge.role_id))
        .collect::<HashSet<_>>();
    let mut final_edges = current_edges.clone();
    for (pair, present) in membership_updates {
        if present {
            final_edges.insert(pair);
        } else {
            final_edges.remove(&pair);
        }
    }
    let current_admin_members = members_in_role_closure(&current_edges, admin_role_id);
    let final_admin_members = members_in_role_closure(&final_edges, admin_role_id);
    if !is_database_admin
        && (!has_manage_roles || targets_admin_role || current_admin_members != final_admin_members)
    {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    let principal_prefix = 10u16.to_be_bytes();
    let mut principal_count = 0usize;
    let mut has_login_enabled_admin = false;
    for entry in catalog
        .range(principal_prefix.as_slice()..)
        .map_err(storage_error)?
    {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&principal_prefix) {
            break;
        }
        principal_count += 1;
        if principal_count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
            return Err(CommitError::InvalidCatalog);
        }
        if key.len() != 19 || key[2] != 1 {
            return Err(CommitError::InvalidCatalog);
        }
        let id: [u8; 16] = key[3..19]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        if final_admin_members.contains(&id)
            && decode_principal_kind(key, value.value())? == 1
            && catalog_field_bytes(key, value.value(), 10, 3)? == [1]
        {
            has_login_enabled_admin = true;
            break;
        }
    }
    if !has_login_enabled_admin {
        return Err(CommitError::DdlAuthorizationDenied);
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

pub(super) fn members_in_role_closure(
    memberships: &HashSet<([u8; 16], [u8; 16])>,
    target_role_id: [u8; 16],
) -> HashSet<[u8; 16]> {
    let mut members_by_role = HashMap::<[u8; 16], Vec<[u8; 16]>>::new();
    for (member_id, role_id) in memberships {
        members_by_role
            .entry(*role_id)
            .or_default()
            .push(*member_id);
    }
    let mut closure = HashSet::from([target_role_id]);
    let mut pending = vec![target_role_id];
    while let Some(role_id) = pending.pop() {
        for member_id in members_by_role.get(&role_id).into_iter().flatten() {
            if closure.insert(*member_id) {
                pending.push(*member_id);
            }
        }
    }
    closure
}

pub(super) fn catalog_changes_may_remove_last_admin_credential(
    authorization_mode: CatalogAuthorizationMode,
    catalog_writes: &[CatalogWrite],
) -> Result<bool, CommitError> {
    let potential_check = match authorization_mode {
        CatalogAuthorizationMode::MembershipDdl => catalog_writes.iter().any(|write| {
            write.key.starts_with(&11u16.to_be_bytes()) && write.expected_value != write.payload
        }),
        CatalogAuthorizationMode::IdentityDdl => catalog_writes.iter().any(|write| {
            (write.key.starts_with(&10u16.to_be_bytes()) && write.expected_value.is_some())
                || (write.key.starts_with(&16u16.to_be_bytes())
                    && write.expected_value.is_some()
                    && write.payload.is_some())
        }),
        _ => false,
    };
    if !potential_check {
        return Ok(false);
    }

    let prefix = 11u16.to_be_bytes();
    match authorization_mode {
        CatalogAuthorizationMode::MembershipDdl => {
            return Ok(catalog_writes.iter().any(|write| {
                write.key.starts_with(&prefix) && write.expected_value != write.payload
            }));
        }
        CatalogAuthorizationMode::IdentityDdl => {
            for write in catalog_writes {
                if write.key.starts_with(&10u16.to_be_bytes())
                    && write.expected_value.is_some()
                    && write.payload.is_none()
                {
                    return Ok(true);
                }
                if write.key.starts_with(&10u16.to_be_bytes())
                    && let (Some(current), Some(next)) =
                        (write.expected_value.as_deref(), write.payload.as_deref())
                {
                    let current_login = catalog_field_bytes(&write.key, current, 10, 3)?;
                    let next_login = catalog_field_bytes(&write.key, next, 10, 3)?;
                    if current_login == [1] && next_login == [0] {
                        return Ok(true);
                    }
                }
                if write.key.starts_with(&16u16.to_be_bytes())
                    && let (Some(current), Some(next)) =
                        (write.expected_value.as_deref(), write.payload.as_deref())
                {
                    let current_record = decode_catalog_record(&write.key, current)?;
                    let next_record = decode_catalog_record(&write.key, next)?;
                    let current_credential = decode_verifier_record(&current_record)
                        .map_err(|_| CommitError::InvalidCatalog)?;
                    let next_credential = decode_verifier_record(&next_record)
                        .map_err(|_| CommitError::InvalidCatalog)?;
                    if !current_credential.revoked && next_credential.revoked {
                        return Ok(true);
                    }
                }
            }
        }
        _ => {}
    }
    Ok(false)
}

pub(super) fn system_privilege_ddl_authority_guards(
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
    if !roles.contains(&admin_role_id) {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    let metadata = read
        .open_table(ENGINE_META)
        .map_err(storage_error)?
        .get(&b"format"[..])
        .map_err(storage_error)?
        .ok_or(CommitError::InvalidMetadata)?;
    let database_id = EngineMetadata::decode(metadata.value())
        .map_err(|_| CommitError::InvalidMetadata)?
        .database_id;

    let mut changed_grantees = HashSet::new();
    let mut changed_authority_pairs = HashSet::new();
    let mut dependency_pairs = HashSet::new();
    let mut principal_updates = HashSet::new();
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
                if !matches!(privilege, 12..=14)
                    || scope != 1
                    || object_id != database_id
                    || target_id != database_id
                    || grantee_id == [0; 16]
                {
                    return Err(CommitError::InvalidCatalog);
                }
                if let Some(bytes) = &write.payload
                    && (catalog_field_bytes(&write.key, bytes, 12, 1)? != principal_id
                        || catalog_field_u64(&write.key, bytes, 12, 2)? == 0)
                {
                    return Err(CommitError::DdlAuthorizationDenied);
                }
                changed_grantees.insert(grantee_id);
                changed_authority_pairs.insert((grantee_id, database_id));
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

/// Authorize user/role creation, rename, login-state changes, and restricted
/// drops after checking all live catalog references.
pub(super) fn identity_ddl_authority_guards(
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
                || !matches!(
                    event.action,
                    crate::audit::AuditAction::Ddl | crate::audit::AuditAction::TokenLifecycle
                )
                || event.result != crate::audit::AuditResult::Succeeded
        })
    {
        return Err(CommitError::InvalidAudit);
    }

    // Identity rows, their named object rows, tombstones, and one-way verifier
    // revocations are the only writes accepted by this route.
    let read = database.begin_read().map_err(storage_error)?;
    let catalog = read.open_table(CATALOG).map_err(storage_error)?;
    let mut principal_changes = HashMap::<[u8; 16], (u16, bool, bool, bool, bool, Vec<u8>)>::new();
    let mut workload_profile_changes = HashMap::<[u8; 16], (bool, bool, bool)>::new();
    let mut require_admin = false;
    let mut object_changes = HashMap::<[u8; 16], (u16, bool, bool, bool)>::new();
    let mut credential_changes = HashMap::<[u8; 16], [u8; 16]>::new();
    let mut credential_issues = HashSet::<[u8; 16]>::new();
    let mut tombstone_changes = HashSet::<[u8; 16]>::new();
    let mut required_privileges = HashSet::<u16>::new();
    let mut guards = HashMap::<Vec<u8>, Option<Vec<u8>>>::new();

    for write in catalog_writes {
        let family = write
            .key
            .get(..2)
            .and_then(|bytes| <[u8; 2]>::try_from(bytes).ok())
            .map(u16::from_be_bytes)
            .ok_or(CommitError::InvalidCatalog)?;
        if !matches!(family, 2 | 10 | 15 | 16 | 17 | 18) {
            return Err(CommitError::InvalidCatalog);
        }
        let current = catalog
            .get(write.key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if current.as_deref() == write.payload.as_deref() {
            continue;
        }
        if family == 15 {
            if write.key.len() != 39
                || write.key[2] != 1
                || write.key[19] != 1
                || write.key[36] != 1
                || u16::from_be_bytes([write.key[37], write.key[38]]) != 6
            {
                return Err(CommitError::InvalidCatalog);
            }
            add_catalog_guard(&mut guards, write.key.clone(), current);
            required_privileges.insert(12);
            continue;
        }
        if family == 17 {
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
            if object_id == [0; 16]
                || !decode_catalog_record(
                    &write.key,
                    write
                        .payload
                        .as_deref()
                        .ok_or(CommitError::InvalidCatalog)?,
                )?
                .fields
                .is_empty()
                || !tombstone_changes.insert(object_id)
            {
                return Err(CommitError::InvalidCatalog);
            }
            continue;
        }
        if family == 16 {
            if write.key.len() != 19 || write.key[2] != 1 || write.payload.is_none() {
                return Err(CommitError::InvalidCatalog);
            }
            let next_bytes = write
                .payload
                .as_deref()
                .ok_or(CommitError::InvalidCatalog)?;
            let next_record = decode_catalog_record(&write.key, next_bytes)?;
            let next_credential =
                decode_verifier_record(&next_record).map_err(|_| CommitError::InvalidCatalog)?;
            let token_id: [u8; 16] = write.key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let user_id = if let Some(current_bytes) = current.as_deref() {
                let current_record = decode_catalog_record(&write.key, current_bytes)?;
                let current_credential = decode_verifier_record(&current_record)
                    .map_err(|_| CommitError::InvalidCatalog)?;
                if current_credential.revoked
                    || !next_credential.revoked
                    || current_credential.user_id != next_credential.user_id
                    || current_credential.verifier != next_credential.verifier
                    || current_credential.issued_at_us != next_credential.issued_at_us
                    || current_credential.expires_at_us != next_credential.expires_at_us
                    || current_credential.label != next_credential.label
                    || next_credential.authority_epoch <= current_credential.authority_epoch
                {
                    return Err(CommitError::InvalidCatalog);
                }
                current_credential.user_id
            } else {
                if next_credential.revoked {
                    return Err(CommitError::InvalidCatalog);
                }
                let user_key = id_catalog_key(10, next_credential.user_id);
                let user_bytes = catalog
                    .get(user_key.as_slice())
                    .map_err(storage_error)?
                    .map(|value| value.value().to_vec())
                    .ok_or(CommitError::DdlAuthorizationDenied)?;
                if decode_principal_kind(&user_key, &user_bytes)? != 1
                    || catalog_field_bytes(&user_key, &user_bytes, 10, 3)? != [1]
                {
                    return Err(CommitError::DdlAuthorizationDenied);
                }
                credential_issues.insert(token_id);
                next_credential.user_id
            };
            if credential_changes.insert(token_id, user_id).is_some() {
                return Err(CommitError::InvalidCatalog);
            }
            add_catalog_guard(&mut guards, write.key.clone(), current);
            required_privileges.insert(12);
            continue;
        }
        if write.key.len() != 19 || write.key[2] != 1 {
            return Err(CommitError::InvalidCatalog);
        }
        let object_id: [u8; 16] = write.key[3..19]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        if object_id == [0; 16] {
            return Err(CommitError::InvalidCatalog);
        }
        if write.payload.is_none() {
            let old_bytes = current.as_deref().ok_or(CommitError::InvalidCatalog)?;
            let old = decode_catalog_record(&write.key, old_bytes)?;
            match family {
                2 => {
                    let old_kind = catalog_field_u16(&write.key, old_bytes, 2, 1)?;
                    if !matches!(old_kind, 8 | 9 | 11)
                        || old.fields.iter().any(|field| field.tag == 2)
                        || catalog_field_u64(&write.key, old_bytes, 2, 5)? == 0
                    {
                        return Err(CommitError::InvalidCatalog);
                    }
                    object_changes.insert(object_id, (old_kind, false, false, true));
                }
                10 => {
                    let old_kind = decode_principal_kind(&write.key, old_bytes)?;
                    if !matches!(old_kind, 1 | 2)
                        || (old_kind == 2
                            && catalog_field_bytes(&write.key, old_bytes, 10, 3)? != [0])
                        || principal_authority_epoch(&write.key, old_bytes)? == 0
                    {
                        return Err(CommitError::InvalidCatalog);
                    }
                    required_privileges.insert(if old_kind == 1 { 12 } else { 13 });
                    principal_changes.insert(
                        object_id,
                        (
                            old_kind,
                            false,
                            false,
                            false,
                            true,
                            catalog_field_bytes(&write.key, old_bytes, 10, 2)?,
                        ),
                    );
                }
                18 => {
                    let is_builtin = catalog_field_bytes(&write.key, old_bytes, 18, 3)?;
                    if is_builtin != [0] || catalog_field_u64(&write.key, old_bytes, 18, 2)? == 0 {
                        return Err(CommitError::InvalidCatalog);
                    }
                    require_admin = true;
                    workload_profile_changes.insert(object_id, (false, false, true));
                }
                _ => return Err(CommitError::InvalidCatalog),
            }
            continue;
        }
        let next_bytes = write
            .payload
            .as_deref()
            .ok_or(CommitError::InvalidCatalog)?;

        match family {
            2 => {
                let next = decode_catalog_record(&write.key, next_bytes)?;
                let next_kind = catalog_field_u16(&write.key, next_bytes, 2, 1)?;
                if !matches!(next_kind, 8 | 9 | 11)
                    || next.fields.iter().any(|field| field.tag == 2)
                    || catalog_field_u16(&write.key, next_bytes, 2, 6)? != 1
                    || catalog_field_u64(&write.key, next_bytes, 2, 5)? == 0
                {
                    return Err(CommitError::InvalidCatalog);
                }
                let (created, renamed) = if let Some(old_bytes) = current.as_deref() {
                    let old = decode_catalog_record(&write.key, old_bytes)?;
                    if catalog_field_u16(&write.key, old_bytes, 2, 1)? != next_kind
                        || !catalog_fields_equal_except(&old, &next, &[3, 5])
                        || catalog_field_bytes(&write.key, old_bytes, 2, 3)?
                            == catalog_field_bytes(&write.key, next_bytes, 2, 3)?
                        || catalog_field_u64(&write.key, next_bytes, 2, 5)?
                            <= catalog_field_u64(&write.key, old_bytes, 2, 5)?
                    {
                        return Err(CommitError::InvalidCatalog);
                    }
                    (false, true)
                } else {
                    (true, false)
                };
                object_changes.insert(object_id, (next_kind, created, renamed, false));
            }
            10 => {
                let next_kind = decode_principal_kind(&write.key, next_bytes)?;
                let next_name = catalog_field_bytes(&write.key, next_bytes, 10, 2)?;
                let next_login = catalog_field_bytes(&write.key, next_bytes, 10, 3)?;
                let next_epoch = principal_authority_epoch(&write.key, next_bytes)?;
                if !matches!(next_kind, 1 | 2)
                    || (next_kind == 2 && next_login != [0])
                    || next_epoch == 0
                {
                    return Err(CommitError::InvalidCatalog);
                }
                let (created, name_changed, login_changed) =
                    if let Some(old_bytes) = current.as_deref() {
                        let old = decode_catalog_record(&write.key, old_bytes)?;
                        let old_kind = decode_principal_kind(&write.key, old_bytes)?;
                        let old_name = catalog_field_bytes(&write.key, old_bytes, 10, 2)?;
                        let old_login = catalog_field_bytes(&write.key, old_bytes, 10, 3)?;
                        if old_kind != next_kind
                            || !catalog_fields_equal_except(
                                &old,
                                &decode_catalog_record(&write.key, next_bytes)?,
                                &[2, 3, 4, 5],
                            )
                            || next_epoch <= principal_authority_epoch(&write.key, old_bytes)?
                        {
                            return Err(CommitError::InvalidCatalog);
                        }
                        let name_changed = old_name != next_name;
                        let login_changed = old_login != next_login;
                        let old_profile = old
                            .fields
                            .iter()
                            .find(|f| f.tag == 5)
                            .map(|f| f.payload.as_slice());
                        let next_record = decode_catalog_record(&write.key, next_bytes)?;
                        let next_profile = next_record
                            .fields
                            .iter()
                            .find(|f| f.tag == 5)
                            .map(|f| f.payload.as_slice());
                        let profile_changed = old_profile != next_profile;
                        if !name_changed && !login_changed && !profile_changed {
                            return Err(CommitError::InvalidCatalog);
                        }
                        (false, name_changed, login_changed)
                    } else {
                        (true, false, false)
                    };
                if next_kind == 1 {
                    required_privileges.insert(12);
                } else {
                    required_privileges.insert(13);
                }
                principal_changes.insert(
                    object_id,
                    (
                        next_kind,
                        created,
                        name_changed,
                        login_changed,
                        false,
                        next_name,
                    ),
                );
            }
            18 => {
                let next_name = catalog_field_bytes(&write.key, next_bytes, 18, 1)?;
                let next_epoch = catalog_field_u64(&write.key, next_bytes, 18, 2)?;
                if next_epoch == 0 {
                    return Err(CommitError::InvalidCatalog);
                }
                let (created, renamed) = if let Some(old_bytes) = current.as_deref() {
                    let old_name = catalog_field_bytes(&write.key, old_bytes, 18, 1)?;
                    let old_epoch = catalog_field_u64(&write.key, old_bytes, 18, 2)?;
                    if next_epoch <= old_epoch {
                        return Err(CommitError::InvalidCatalog);
                    }
                    (false, old_name != next_name)
                } else {
                    (true, false)
                };
                require_admin = true;
                workload_profile_changes.insert(object_id, (created, renamed, false));
            }
            _ => unreachable!(),
        }
    }

    if principal_changes.is_empty()
        && credential_changes.is_empty()
        && workload_profile_changes.is_empty()
    {
        return Err(CommitError::InvalidCatalog);
    }
    if (principal_changes.is_empty()
        && workload_profile_changes.is_empty()
        && audit_events
            .iter()
            .any(|event| event.action != crate::audit::AuditAction::TokenLifecycle))
        || ((!principal_changes.is_empty() || !workload_profile_changes.is_empty())
            && audit_events
                .iter()
                .any(|event| event.action != crate::audit::AuditAction::Ddl))
        || credential_issues.iter().any(|token_id| {
            credential_changes
                .get(token_id)
                .is_some_and(|user_id| principal_changes.contains_key(user_id))
        })
    {
        return Err(CommitError::InvalidAudit);
    }
    if object_changes.keys().any(|object_id| {
        !principal_changes.contains_key(object_id)
            && !workload_profile_changes.contains_key(object_id)
    }) {
        return Err(CommitError::InvalidCatalog);
    }
    let mut dropped_ids = HashSet::new();
    for (object_id, (principal_kind, created, name_changed, _, dropped, _)) in &principal_changes {
        let required_object_kind = if *principal_kind == 1 { 8 } else { 9 };
        if *dropped {
            if object_changes.get(object_id) != Some(&(required_object_kind, false, false, true)) {
                return Err(CommitError::InvalidCatalog);
            }
            dropped_ids.insert(*object_id);
            continue;
        }
        match (created, name_changed, object_changes.get(object_id)) {
            (true, _, Some((object_kind, true, false, false)))
                if *object_kind == required_object_kind => {}
            (false, true, Some((object_kind, false, true, false)))
                if *object_kind == required_object_kind => {}
            (false, false, None) => {}
            _ => return Err(CommitError::InvalidCatalog),
        }
    }
    for (object_id, (created, renamed, dropped)) in &workload_profile_changes {
        if *dropped {
            if object_changes.get(object_id) != Some(&(11, false, false, true)) {
                return Err(CommitError::InvalidCatalog);
            }
            dropped_ids.insert(*object_id);
            continue;
        }
        match (created, renamed, object_changes.get(object_id)) {
            (true, _, Some((11, true, false, false))) => {}
            (false, true, Some((11, false, true, false))) => {}
            (false, false, None) => {}
            _ => return Err(CommitError::InvalidCatalog),
        }
    }
    if tombstone_changes != dropped_ids
        || object_changes
            .iter()
            .any(|(object_id, (_, _, _, dropped))| *dropped && !dropped_ids.contains(object_id))
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

    add_catalog_guard(&mut guards, actor_key, Some(actor_bytes));
    let membership_prefix = 11u16.to_be_bytes();
    let mut memberships = Vec::new();
    let mut principal_ids = HashSet::from([principal_id]);
    let mut admin_role_id = [0x80; 16];
    admin_role_id[15] = 3;
    if dropped_ids.contains(&admin_role_id) {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    principal_ids.insert(admin_role_id);
    principal_ids.extend(credential_changes.values().copied());
    for (object_id, (_, created, _, _, dropped, _)) in &principal_changes {
        if !created && !dropped {
            continue;
        }
        let tombstone_key = id_catalog_key(17, *object_id);
        let tombstone = catalog
            .get(tombstone_key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if tombstone.is_some() {
            return Err(CommitError::DdlAuthorizationDenied);
        }
        add_catalog_guard(&mut guards, tombstone_key, None);
    }
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
        let grantor_id: [u8; 16] = catalog_field_bytes(&key, &bytes, 11, 1)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        if membership.member_id == [0; 16]
            || membership.role_id == [0; 16]
            || grantor_id == [0; 16]
            || catalog_field_u64(&key, &bytes, 11, 2)? == 0
        {
            return Err(CommitError::InvalidCatalog);
        }
        if dropped_ids.contains(&membership.member_id)
            || dropped_ids.contains(&membership.role_id)
            || dropped_ids.contains(&grantor_id)
        {
            return Err(CommitError::DdlAuthorizationDenied);
        }
        principal_ids.insert(membership.member_id);
        principal_ids.insert(membership.role_id);
        memberships.push(membership);
        add_catalog_guard(&mut guards, key, Some(bytes));
    }
    identity_drop_restrict_references(&catalog, &dropped_ids)?;
    if !dropped_ids.is_empty() {
        let prefix = 16u16.to_be_bytes();
        let mut count = 0usize;
        for entry in catalog.range(prefix.as_slice()..).map_err(storage_error)? {
            let (key, value) = entry.map_err(storage_error)?;
            let key = key.value().to_vec();
            if !key.starts_with(&prefix) {
                break;
            }
            count += 1;
            if count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN || key.len() != 19 || key[2] != 1 {
                return Err(CommitError::InvalidCatalog);
            }
            let bytes = value.value().to_vec();
            let record = decode_catalog_record(&key, &bytes)?;
            let credential =
                decode_verifier_record(&record).map_err(|_| CommitError::InvalidCatalog)?;
            if dropped_ids.contains(&credential.user_id) {
                let token_id: [u8; 16] = key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                if !credential.revoked
                    && credential_changes.get(&token_id) != Some(&credential.user_id)
                {
                    return Err(CommitError::DdlAuthorizationDenied);
                }
                add_catalog_guard(&mut guards, key, Some(bytes));
            }
        }
    }
    validate_membership_graph(&memberships).map_err(|_| CommitError::InvalidCatalog)?;

    let mut principal_records = HashMap::<[u8; 16], Vec<u8>>::new();
    for id in principal_ids {
        let key = id_catalog_key(10, id);
        let bytes = catalog
            .get(key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        let kind = decode_principal_kind(&key, &bytes)?;
        if (memberships.iter().any(|edge| edge.role_id == id) && kind != 2)
            || catalog_field_u64(&key, &bytes, 10, 4)? == 0
        {
            return Err(CommitError::InvalidCatalog);
        }
        principal_records.insert(id, bytes.clone());
        add_catalog_guard(&mut guards, key, Some(bytes));
    }
    for edge in &memberships {
        if decode_principal_kind(
            &id_catalog_key(10, edge.role_id),
            principal_records
                .get(&edge.role_id)
                .ok_or(CommitError::InvalidCatalog)?,
        )? != 2
        {
            return Err(CommitError::InvalidCatalog);
        }
    }

    let roles = resolve_role_closure(principal_id, &memberships)
        .map_err(|_| CommitError::InvalidCatalog)?;
    let is_database_admin = roles.contains(&admin_role_id);
    if require_admin && !is_database_admin {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    let mut authority_subjects = roles.clone();
    authority_subjects.insert(principal_id);
    let metadata = read
        .open_table(ENGINE_META)
        .map_err(storage_error)?
        .get(&b"format"[..])
        .map_err(storage_error)?
        .ok_or(CommitError::InvalidMetadata)?;
    let database_id = EngineMetadata::decode(metadata.value())
        .map_err(|_| CommitError::InvalidMetadata)?
        .database_id;
    if !is_database_admin {
        for privilege in &required_privileges {
            let mut authorized = false;
            for subject in &authority_subjects {
                let key = grant_key(*subject, database_id, *privilege, 1, database_id);
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
                    authorized = true;
                }
                add_catalog_guard(&mut guards, key, bytes);
            }
            if !authorized {
                return Err(CommitError::DdlAuthorizationDenied);
            }
        }
    }

    let admin_members = members_in_role_closure(
        &memberships
            .iter()
            .map(|edge| (edge.member_id, edge.role_id))
            .collect(),
        admin_role_id,
    );
    let mut has_login_enabled_admin = false;
    for member_id in admin_members {
        let Some(current) = principal_records.get(&member_id) else {
            return Err(CommitError::InvalidCatalog);
        };
        let key = id_catalog_key(10, member_id);
        let final_bytes = catalog_writes
            .iter()
            .find(|write| write.key == key)
            .and_then(|write| write.payload.as_deref())
            .unwrap_or(current);
        if decode_principal_kind(&key, final_bytes)? == 1
            && catalog_field_bytes(&key, final_bytes, 10, 3)? == [1]
        {
            has_login_enabled_admin = true;
            break;
        }
    }
    if !has_login_enabled_admin {
        return Err(CommitError::DdlAuthorizationDenied);
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

pub(super) fn identity_drop_restrict_references(
    catalog: &ReadOnlyTable<&[u8], &[u8]>,
    dropped_ids: &HashSet<[u8; 16]>,
) -> Result<(), CommitError> {
    if dropped_ids.is_empty() {
        return Ok(());
    }

    let object_prefix = 2u16.to_be_bytes();
    let mut object_count = 0usize;
    for entry in catalog
        .range(object_prefix.as_slice()..)
        .map_err(storage_error)?
    {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&object_prefix) {
            break;
        }
        object_count += 1;
        if object_count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN || key.len() != 19 || key[2] != 1
        {
            return Err(CommitError::InvalidCatalog);
        }
        let object_id: [u8; 16] = key[3..19]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        if dropped_ids.contains(&object_id) {
            continue;
        }
        let record = decode_catalog_record(key, value.value())?;
        let owner_id: [u8; 16] = catalog_field_bytes(key, value.value(), 2, 4)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let parent_references_dropped = record
            .fields
            .iter()
            .find(|field| field.tag == 2)
            .is_some_and(|field| {
                field
                    .payload
                    .as_slice()
                    .try_into()
                    .is_ok_and(|parent_id: &[u8; 16]| dropped_ids.contains(parent_id))
            });
        if dropped_ids.contains(&owner_id) || parent_references_dropped {
            return Err(CommitError::DdlAuthorizationDenied);
        }
    }

    let grant_prefix = 12u16.to_be_bytes();
    let mut grant_count = 0usize;
    for entry in catalog
        .range(grant_prefix.as_slice()..)
        .map_err(storage_error)?
    {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
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
        decode_catalog_record(key, value.value())?;
        let grantee_id: [u8; 16] = key[3..19]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let object_id: [u8; 16] = key[20..36]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let target_id: [u8; 16] = key[43..59]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let grantor_id: [u8; 16] = catalog_field_bytes(key, value.value(), 12, 1)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        if dropped_ids.contains(&grantee_id)
            || dropped_ids.contains(&grantor_id)
            || dropped_ids.contains(&object_id)
            || dropped_ids.contains(&target_id)
        {
            return Err(CommitError::DdlAuthorizationDenied);
        }
    }

    let policy_target_prefix = 14u16.to_be_bytes();
    let mut target_count = 0usize;
    for entry in catalog
        .range(policy_target_prefix.as_slice()..)
        .map_err(storage_error)?
    {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&policy_target_prefix) {
            break;
        }
        target_count += 1;
        if target_count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN
            || key.len() != 39
            || [key[2], key[19], key[22]] != [1; 3]
        {
            return Err(CommitError::InvalidCatalog);
        }
        decode_catalog_record(key, value.value())?;
        let target_id: [u8; 16] = key[23..39]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        if dropped_ids.contains(&target_id) {
            return Err(CommitError::DdlAuthorizationDenied);
        }
    }

    let dependency_prefix = 15u16.to_be_bytes();
    let mut dependency_count = 0usize;
    for entry in catalog
        .range(dependency_prefix.as_slice()..)
        .map_err(storage_error)?
    {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&dependency_prefix) {
            break;
        }
        dependency_count += 1;
        if dependency_count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
            return Err(CommitError::InvalidCatalog);
        }
        let (dependent_id, referenced_id, kind) = decode_dependency_edge(key, value.value())?;
        if (kind != 6 && dropped_ids.contains(&dependent_id))
            || dropped_ids.contains(&referenced_id)
        {
            return Err(CommitError::DdlAuthorizationDenied);
        }
    }
    Ok(())
}

pub(super) fn decode_dependency_edge(
    key: &[u8],
    bytes: &[u8],
) -> Result<([u8; 16], [u8; 16], u16), CommitError> {
    let record = decode_catalog_record(key, bytes)?;
    if record.family_code != 15 || key.len() != 39 || key[2] != 1 || key[19] != 1 || key[36] != 1 {
        return Err(CommitError::InvalidCatalog);
    }
    let dependent_id = key[3..19]
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let referenced_id = key[20..36]
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let kind = u16::from_be_bytes([key[37], key[38]]);
    if dependent_id == [0; 16] || referenced_id == [0; 16] {
        return Err(CommitError::InvalidCatalog);
    }
    Ok((dependent_id, referenced_id, kind))
}
