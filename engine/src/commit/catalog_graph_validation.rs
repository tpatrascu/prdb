use super::*;
use crate::authorization::{MAX_MEMBERSHIP_EDGES, Membership, validate_membership_graph};
use crate::catalog::validate_builtin_type_profile;
use crate::expression::decode_catalog_expression_record;
use crate::schema::CATALOG;
use redb::{ReadableTable, WriteTransaction};
use std::collections::{HashMap, HashSet};

pub(super) fn validate_final_membership_graph(
    transaction: &WriteTransaction,
    catalog_writes: &[CatalogWrite],
) -> Result<(), CommitError> {
    let membership_prefix = 11u16.to_be_bytes();
    let mut memberships = {
        let table = transaction.open_table(CATALOG).map_err(storage_error)?;
        let mut memberships = HashMap::<Vec<u8>, Membership>::new();
        for entry in table
            .range(membership_prefix.as_slice()..)
            .map_err(storage_error)?
        {
            let (key, value) = entry.map_err(storage_error)?;
            let key = key.value().to_vec();
            if !key.starts_with(&membership_prefix) {
                break;
            }
            let edge = decode_membership_record(&key, value.value())?;
            memberships.insert(key, edge);
            if memberships.len() > MAX_MEMBERSHIP_EDGES {
                return Err(CommitError::InvalidCatalog);
            }
        }
        memberships
    };
    // Remove old edges first so the bound applies to the final graph, not to
    // an arbitrary ordering of additions and removals in the submitted batch.
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
        if let Some(bytes) = &write.payload {
            memberships.insert(
                write.key.clone(),
                decode_membership_record(&write.key, bytes)?,
            );
            if memberships.len() > MAX_MEMBERSHIP_EDGES {
                return Err(CommitError::InvalidCatalog);
            }
        }
    }
    let edges = memberships.values().copied().collect::<Vec<_>>();
    let mut principal_kinds = HashMap::<[u8; 16], u16>::new();
    for edge in &edges {
        for principal_id in [edge.member_id, edge.role_id] {
            if let std::collections::hash_map::Entry::Vacant(entry) =
                principal_kinds.entry(principal_id)
            {
                entry.insert(final_principal_kind(
                    transaction,
                    catalog_writes,
                    principal_id,
                )?);
            }
        }
        if principal_kinds.get(&edge.role_id) != Some(&2) {
            return Err(CommitError::InvalidCatalog);
        }
    }
    validate_membership_graph(&edges).map_err(|_| CommitError::InvalidCatalog)?;

    // Membership changes invalidate the member's effective role closure. For
    // existing principals, require the same DDL transaction to advance the
    // principal authority epoch so readers also detect newly added edges.
    let table = transaction.open_table(CATALOG).map_err(storage_error)?;
    // Epochs must never be restored to an earlier byte value after a change:
    // snapshot guards rely on them to detect newly added membership/grants.
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&10u16.to_be_bytes()))
    {
        let Some(next) = &write.payload else {
            continue;
        };
        decode_principal_kind(&write.key, next)?;
        let next_epoch = principal_authority_epoch(&write.key, next)?;
        let current = table
            .get(&write.key[..])
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if let Some(current) = current
            && current != *next
            && next_epoch <= principal_authority_epoch(&write.key, &current)?
        {
            return Err(CommitError::InvalidCatalog);
        }
    }
    let validate_principal_epoch_bumps = |changed_principals: &HashSet<[u8; 16]>| {
        for principal_id in changed_principals {
            let mut principal_key = 10u16.to_be_bytes().to_vec();
            principal_key.push(1);
            principal_key.extend_from_slice(principal_id);
            let current_epoch = table
                .get(&principal_key[..])
                .map_err(storage_error)?
                .map(|value| principal_authority_epoch(&principal_key, value.value()))
                .transpose()?;
            let final_bytes = if let Some(write) = catalog_writes
                .iter()
                .find(|write| write.key == principal_key)
            {
                write.payload.clone()
            } else {
                table
                    .get(&principal_key[..])
                    .map_err(storage_error)?
                    .map(|value| value.value().to_vec())
            };
            if let (Some(current_epoch), Some(final_bytes)) = (current_epoch, final_bytes)
                && principal_authority_epoch(&principal_key, &final_bytes)? <= current_epoch
            {
                return Err(CommitError::InvalidCatalog);
            }
        }
        Ok::<(), CommitError>(())
    };
    let mut changed_members = HashSet::<[u8; 16]>::new();
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&membership_prefix))
    {
        let current = table
            .get(&write.key[..])
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if current == write.payload {
            continue;
        }
        if write.key.len() != 36 {
            return Err(CommitError::InvalidCatalog);
        }
        if let Some(bytes) = &write.payload {
            let next_epoch = membership_epoch(&write.key, bytes)?;
            if let Some(current_bytes) = &current
                && next_epoch <= membership_epoch(&write.key, current_bytes)?
            {
                return Err(CommitError::InvalidCatalog);
            }
        }
        changed_members.insert(
            write.key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?,
        );
    }
    validate_principal_epoch_bumps(&changed_members)?;

    // A grant addition or revocation changes the effective permissions for
    // its grantee, so existing grantees must advance their authority epoch.
    let grant_prefix = 12u16.to_be_bytes();
    let mut changed_grantees = HashSet::<[u8; 16]>::new();
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&grant_prefix))
    {
        let current = table
            .get(&write.key[..])
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if current == write.payload {
            continue;
        }
        if write.key.len() != 59 {
            return Err(CommitError::InvalidCatalog);
        }
        if let Some(bytes) = &write.payload {
            let next_epoch = grant_authority_epoch(&write.key, bytes)?;
            if let Some(current_bytes) = &current
                && next_epoch <= grant_authority_epoch(&write.key, current_bytes)?
            {
                return Err(CommitError::InvalidCatalog);
            }
        }
        changed_grantees.insert(
            write.key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?,
        );
    }
    validate_principal_epoch_bumps(&changed_grantees)?;

    let mut changed_policies = HashSet::<[u8; 16]>::new();
    let mut affected_tables = HashSet::<[u8; 16]>::new();
    for write in catalog_writes.iter().filter(|write| {
        write.key.starts_with(&8u16.to_be_bytes())
            || write.key.starts_with(&13u16.to_be_bytes())
            || write.key.starts_with(&14u16.to_be_bytes())
    }) {
        let current = table
            .get(&write.key[..])
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        if current == write.payload {
            continue;
        }
        if write.key.starts_with(&8u16.to_be_bytes()) {
            if write.key.len() != 19 {
                return Err(CommitError::InvalidCatalog);
            }
            if let Some(bytes) = &write.payload {
                validate_constraint_shape(&write.key, bytes)?;
            }
            let current_table = current
                .as_deref()
                .map(|bytes| constraint_table_id(&write.key, bytes))
                .transpose()?;
            let final_table = write
                .payload
                .as_deref()
                .map(|bytes| constraint_table_id(&write.key, bytes))
                .transpose()?;
            if current_table.is_some() && final_table.is_some() && current_table != final_table {
                return Err(CommitError::InvalidCatalog);
            }
            if let Some(table_id) = current_table.or(final_table) {
                affected_tables.insert(table_id);
                for constraint_bytes in [current.as_deref(), write.payload.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    if u16::from_le_bytes(
                        catalog_field_bytes(&write.key, constraint_bytes, 8, 2)?
                            .try_into()
                            .map_err(|_| CommitError::InvalidCatalog)?,
                    ) != 5
                    {
                        continue;
                    }
                    let target_constraint_id: [u8; 16] =
                        catalog_field_bytes(&write.key, constraint_bytes, 8, 5)?
                            .try_into()
                            .map_err(|_| CommitError::InvalidCatalog)?;
                    let target_key = id_catalog_key(8, target_constraint_id);
                    let target_bytes = final_catalog_value(&table, catalog_writes, &target_key)?
                        .or(table
                            .get(&target_key[..])
                            .map_err(storage_error)?
                            .map(|value| value.value().to_vec()))
                        .ok_or(CommitError::InvalidCatalog)?;
                    affected_tables.insert(constraint_table_id(&target_key, &target_bytes)?);
                }
            }
        } else if write.key.starts_with(&13u16.to_be_bytes()) {
            let policy_id: [u8; 16] = write
                .key
                .get(3..19)
                .ok_or(CommitError::InvalidCatalog)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            changed_policies.insert(policy_id);
            if let Some(bytes) = &write.payload {
                affected_tables.insert(policy_table_id(&write.key, bytes)?);
            }
            if let Some(bytes) = &current {
                affected_tables.insert(policy_table_id(&write.key, bytes)?);
            }
        } else {
            if write.key.len() != 39 {
                return Err(CommitError::InvalidCatalog);
            }
            let policy_id: [u8; 16] = write.key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            changed_policies.insert(policy_id);
            let policy_key = catalog_id_key(13, policy_id);
            let policy_bytes = if let Some(candidate) = catalog_writes
                .iter()
                .find(|candidate| candidate.key == policy_key)
            {
                if let Some(bytes) = &candidate.payload {
                    bytes.clone()
                } else {
                    table
                        .get(&policy_key[..])
                        .map_err(storage_error)?
                        .ok_or(CommitError::InvalidCatalog)?
                        .value()
                        .to_vec()
                }
            } else {
                table
                    .get(&policy_key[..])
                    .map_err(storage_error)?
                    .ok_or(CommitError::InvalidCatalog)?
                    .value()
                    .to_vec()
            };
            affected_tables.insert(policy_table_id(&policy_key, &policy_bytes)?);
        }
    }
    for policy_id in &changed_policies {
        let policy_key = catalog_id_key(13, *policy_id);
        let current = table
            .get(&policy_key[..])
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        let final_bytes =
            if let Some(write) = catalog_writes.iter().find(|write| write.key == policy_key) {
                write.payload.clone()
            } else {
                current.clone()
            };
        let final_policy_exists = final_bytes.is_some();
        if let (Some(current), Some(final_bytes)) = (current, final_bytes)
            && policy_authority_epoch(&policy_key, &final_bytes)?
                <= policy_authority_epoch(&policy_key, &current)?
        {
            return Err(CommitError::InvalidCatalog);
        }
        let mut target_prefix = 14u16.to_be_bytes().to_vec();
        target_prefix.push(1);
        target_prefix.extend_from_slice(policy_id);
        let mut final_targets = HashSet::new();
        for entry in table
            .range(target_prefix.as_slice()..)
            .map_err(storage_error)?
        {
            let (key, _) = entry.map_err(storage_error)?;
            let key = key.value().to_vec();
            if !key.starts_with(&target_prefix) {
                break;
            }
            final_targets.insert(key);
            if final_targets.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                return Err(CommitError::InvalidCatalog);
            }
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&target_prefix) && write.payload.is_none())
        {
            final_targets.remove(&write.key);
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&target_prefix) && write.payload.is_some())
        {
            final_targets.insert(write.key.clone());
            if final_targets.len() > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                return Err(CommitError::InvalidCatalog);
            }
        }
        if !final_policy_exists && !final_targets.is_empty() {
            return Err(CommitError::InvalidCatalog);
        }
    }
    for table_id in affected_tables {
        let table_key = catalog_id_key(5, table_id);
        let current = table
            .get(&table_key[..])
            .map_err(storage_error)?
            .map(|value| value.value().to_vec());
        let final_bytes =
            if let Some(write) = catalog_writes.iter().find(|write| write.key == table_key) {
                write.payload.clone()
            } else {
                current.clone()
            };
        if let (Some(current), Some(final_bytes)) = (current, final_bytes)
            && table_write_epoch(&table_key, &final_bytes)?
                <= table_write_epoch(&table_key, &current)?
        {
            return Err(CommitError::InvalidCatalog);
        }
    }
    validate_column_schema_epochs(&table, catalog_writes)?;
    validate_final_constraints(&table, catalog_writes)?;
    Ok(())
}

#[derive(Default)]
pub(super) struct DependencyCatalogGraph {
    pub(super) database_id: Option<[u8; 16]>,
    pub(super) object_parents: HashMap<[u8; 16], Option<[u8; 16]>>,
    pub(super) object_kinds: HashMap<[u8; 16], u16>,
    pub(super) typed_object_kinds: HashMap<[u8; 16], u16>,
    pub(super) column_tables: HashMap<[u8; 16], [u8; 16]>,
    pub(super) index_tables: HashMap<[u8; 16], [u8; 16]>,
    pub(super) table_schemas: HashMap<[u8; 16], [u8; 16]>,
    pub(super) index_columns: HashMap<[u8; 16], Vec<[u8; 16]>>,
    pub(super) policy_tables: HashMap<[u8; 16], [u8; 16]>,
    pub(super) policy_kinds: HashMap<[u8; 16], u16>,
    pub(super) constraint_kinds: HashMap<[u8; 16], u16>,
    pub(super) foreign_key_targets: HashMap<[u8; 16], [u8; 16]>,
    pub(super) backing_index_targets: HashMap<[u8; 16], [u8; 16]>,
    pub(super) index_owners: HashMap<[u8; 16], [u8; 16]>,
    pub(super) expression_reference_edges: HashSet<([u8; 16], [u8; 16])>,
    pub(super) authority_edges: HashSet<([u8; 16], [u8; 16])>,
    pub(super) authority_principals: HashSet<[u8; 16]>,
    pub(super) dependencies: Vec<([u8; 16], [u8; 16], u16)>,
}

/// Keep dependency references valid over the final catalog produced by this
/// transaction, including dependencies or endpoint objects created together.
pub(super) fn validate_final_dependency_edges(
    transaction: &WriteTransaction,
    catalog_writes: &[CatalogWrite],
    database_uuid: [u8; 16],
) -> Result<(), CommitError> {
    if !catalog_writes.iter().any(|write| {
        write.key.len() >= 2
            && matches!(
                u16::from_be_bytes([write.key[0], write.key[1]]),
                1 | 2 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15
            )
    }) {
        return Ok(());
    }

    let table = transaction.open_table(CATALOG).map_err(storage_error)?;
    let mut graph = DependencyCatalogGraph::default();
    for family_code in [1u16, 2, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15] {
        let prefix = family_code.to_be_bytes();
        let mut family_count = 0usize;
        for entry in table.range(prefix.as_slice()..).map_err(storage_error)? {
            let (key, value) = entry.map_err(storage_error)?;
            let key = key.value();
            if !key.starts_with(&prefix) {
                break;
            }
            if catalog_writes.iter().any(|write| write.key == key) {
                continue;
            }
            family_count += 1;
            if family_count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
                return Err(CommitError::InvalidCatalog);
            }
            collect_dependency_catalog_record(key, value.value(), &mut graph)?;
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&prefix))
        {
            if let Some(bytes) = &write.payload {
                family_count += 1;
                if family_count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
                    return Err(CommitError::InvalidCatalog);
                }
                collect_dependency_catalog_record(&write.key, bytes, &mut graph)?;
            }
        }
    }

    let DependencyCatalogGraph {
        database_id,
        object_parents,
        object_kinds,
        typed_object_kinds,
        column_tables,
        index_tables,
        table_schemas,
        index_columns,
        policy_tables: _,
        policy_kinds: _,
        constraint_kinds,
        foreign_key_targets,
        backing_index_targets,
        index_owners,
        expression_reference_edges,
        authority_edges,
        authority_principals,
        dependencies,
    } = &graph;
    let database_record_changed = catalog_writes
        .iter()
        .any(|write| write.key.starts_with(&1u16.to_be_bytes()));
    if database_id.is_some_and(|database_id| database_id != database_uuid)
        || (database_record_changed && *database_id != Some(database_uuid))
        || object_parents.contains_key(&database_uuid)
        || dependencies
            .iter()
            .any(|(dependent_id, referenced_id, kind)| {
                !object_parents.contains_key(dependent_id)
                    || !(object_parents.contains_key(referenced_id)
                        || *kind == 5 && Some(*referenced_id) == *database_id)
            })
    {
        return Err(CommitError::InvalidCatalog);
    }
    if index_tables.iter().any(|(index_id, table_id)| {
        let Some(columns) = index_columns.get(index_id) else {
            return true;
        };
        object_kinds.get(table_id) != Some(&3)
            || typed_object_kinds.get(table_id) != Some(&3)
            || object_parents.get(index_id).is_some_and(|parent_id| {
                parent_id.is_some_and(|parent_id| table_schemas.get(table_id) != Some(&parent_id))
            })
            || columns.is_empty()
            || columns.iter().any(|column_id| {
                object_kinds.get(column_id) != Some(&4)
                    || typed_object_kinds.get(column_id) != Some(&4)
                    || column_tables.get(column_id) != Some(table_id)
            })
    }) {
        return Err(CommitError::InvalidCatalog);
    }
    if table_schemas.iter().any(|(table_id, schema_id)| {
        object_parents
            .get(table_id)
            .is_some_and(|parent_id| parent_id.is_some_and(|parent_id| parent_id != *schema_id))
    }) {
        return Err(CommitError::InvalidCatalog);
    }
    let actual_expression_reference_edges = dependencies
        .iter()
        .filter(|(_, _, kind)| *kind == 2)
        .map(|(dependent_id, referenced_id, _)| (*dependent_id, *referenced_id))
        .collect::<HashSet<_>>();
    if &actual_expression_reference_edges != expression_reference_edges
        || expression_reference_edges
            .iter()
            .any(|(dependent_id, referenced_id)| {
                let endpoint_kinds = (
                    object_kinds.get(dependent_id),
                    object_kinds.get(referenced_id),
                    typed_object_kinds.get(dependent_id),
                    typed_object_kinds.get(referenced_id),
                );
                !matches!(
                    endpoint_kinds,
                    (Some(4), Some(5), Some(4), Some(5))
                        | (Some(6), Some(5), Some(6), Some(5))
                        | (Some(10), Some(5), Some(10), Some(5))
                        | (Some(5), Some(4), Some(5), Some(4))
                        | (Some(5), Some(9), Some(5), Some(9))
                )
            })
    {
        return Err(CommitError::InvalidCatalog);
    }
    let actual_authority_edges = dependencies
        .iter()
        .filter(|(_, _, kind)| *kind == 5)
        .map(|(dependent_id, referenced_id, _)| (*dependent_id, *referenced_id))
        .collect::<HashSet<_>>();
    if &actual_authority_edges != authority_edges
        || authority_principals.iter().any(|principal_id| {
            object_kinds.get(principal_id) != typed_object_kinds.get(principal_id)
        })
        || authority_edges.iter().any(|(dependent_id, referenced_id)| {
            !authority_principals.contains(dependent_id)
                && !matches!(object_kinds.get(dependent_id), Some(10))
                || (!authority_principals.contains(referenced_id)
                    && !object_parents.contains_key(referenced_id)
                    && Some(*referenced_id) != *database_id)
        })
    {
        return Err(CommitError::InvalidCatalog);
    }
    if !crate::catalog::validate_owned_child_graph(object_parents, dependencies) {
        return Err(CommitError::InvalidCatalog);
    }
    let foreign_key_edges = dependencies
        .iter()
        .filter(|(_, _, kind)| *kind == 3)
        .map(|(dependent_id, referenced_id, _)| (*dependent_id, *referenced_id))
        .collect::<HashSet<_>>();
    if dependencies.iter().filter(|(_, _, kind)| *kind == 3).any(
        |(dependent_id, referenced_id, _)| {
            object_kinds.get(dependent_id) != Some(&6)
                || object_kinds.get(referenced_id) != Some(&6)
                || constraint_kinds.get(dependent_id) != Some(&5)
                || !matches!(constraint_kinds.get(referenced_id), Some(2 | 3))
                || foreign_key_targets.get(dependent_id) != Some(referenced_id)
        },
    ) || foreign_key_targets
        .iter()
        .any(|(dependent_id, referenced_id)| {
            !foreign_key_edges.contains(&(*dependent_id, *referenced_id))
        })
    {
        return Err(CommitError::InvalidCatalog);
    }
    let backing_index_edges = dependencies
        .iter()
        .filter(|(_, _, kind)| *kind == 4)
        .map(|(dependent_id, referenced_id, _)| (*dependent_id, *referenced_id))
        .collect::<HashSet<_>>();
    if dependencies.iter().filter(|(_, _, kind)| *kind == 4).any(
        |(dependent_id, referenced_id, _)| {
            object_kinds.get(dependent_id) != Some(&6)
                || object_kinds.get(referenced_id) != Some(&7)
                || !matches!(constraint_kinds.get(dependent_id), Some(2 | 3))
                || backing_index_targets.get(dependent_id) != Some(referenced_id)
                || index_owners.get(referenced_id) != Some(dependent_id)
        },
    ) || backing_index_targets
        .iter()
        .any(|(dependent_id, referenced_id)| {
            !backing_index_edges.contains(&(*dependent_id, *referenced_id))
        })
        || index_owners.iter().any(|(referenced_id, dependent_id)| {
            !backing_index_edges.contains(&(*dependent_id, *referenced_id))
        })
    {
        return Err(CommitError::InvalidCatalog);
    }
    Ok(())
}

/// Require every object owner to resolve to a user or role in the final
/// catalog, including when a principal is changed or removed.
pub(super) fn validate_final_object_owners(
    transaction: &WriteTransaction,
    catalog_writes: &[CatalogWrite],
) -> Result<(), CommitError> {
    let is_relevant_family =
        |key: &[u8]| key.len() >= 2 && matches!(u16::from_be_bytes([key[0], key[1]]), 2 | 10);
    if !catalog_writes
        .iter()
        .any(|write| is_relevant_family(&write.key))
    {
        return Ok(());
    }

    let table = transaction.open_table(CATALOG).map_err(storage_error)?;
    let mut owner_ids = Vec::<[u8; 16]>::new();
    let mut principal_ids = HashSet::<[u8; 16]>::new();
    for family_code in [2u16, 10] {
        let prefix = family_code.to_be_bytes();
        let mut family_count = 0usize;
        for entry in table.range(prefix.as_slice()..).map_err(storage_error)? {
            let (key, value) = entry.map_err(storage_error)?;
            let key = key.value();
            if !key.starts_with(&prefix) {
                break;
            }
            if catalog_writes.iter().any(|write| write.key == key) {
                continue;
            }
            family_count += 1;
            if family_count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
                return Err(CommitError::InvalidCatalog);
            }
            collect_owner_catalog_record(
                family_code,
                key,
                value.value(),
                &mut owner_ids,
                &mut principal_ids,
            )?;
        }
        for write in catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&prefix))
        {
            if let Some(bytes) = &write.payload {
                family_count += 1;
                if family_count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
                    return Err(CommitError::InvalidCatalog);
                }
                collect_owner_catalog_record(
                    family_code,
                    &write.key,
                    bytes,
                    &mut owner_ids,
                    &mut principal_ids,
                )?;
            }
        }
    }
    if owner_ids
        .iter()
        .any(|owner_id| !principal_ids.contains(owner_id))
    {
        return Err(CommitError::InvalidCatalog);
    }
    Ok(())
}

pub(super) fn collect_owner_catalog_record(
    family_code: u16,
    key: &[u8],
    bytes: &[u8],
    owner_ids: &mut Vec<[u8; 16]>,
    principal_ids: &mut HashSet<[u8; 16]>,
) -> Result<(), CommitError> {
    let record = decode_catalog_record(key, bytes)?;
    if key.len() != 19 || record.family_code != family_code {
        return Err(CommitError::InvalidCatalog);
    }
    let object_id = key[3..19]
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    match family_code {
        2 => {
            let owner_id = catalog_field_bytes(key, bytes, 2, 4)?
                .as_slice()
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            owner_ids.push(owner_id);
        }
        10 => {
            let kind = u16::from_le_bytes(
                catalog_field_bytes(key, bytes, 10, 1)?
                    .as_slice()
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?,
            );
            if !matches!(kind, 1 | 2) || !principal_ids.insert(object_id) {
                return Err(CommitError::InvalidCatalog);
            }
        }
        _ => return Err(CommitError::InvalidCatalog),
    }
    Ok(())
}

/// Require affected object IDs to retain exactly their matching typed rows in
/// the final catalog, with neither side surviving alone.
pub(super) fn validate_final_typed_object_mappings(
    transaction: &WriteTransaction,
    catalog_writes: &[CatalogWrite],
) -> Result<(), CommitError> {
    const TYPED_FAMILIES: [u16; 9] = [3, 4, 5, 6, 7, 8, 9, 10, 13];
    let is_relevant_family = |family: u16| family == 2 || TYPED_FAMILIES.contains(&family);
    if !catalog_writes.iter().any(|write| {
        write.key.len() >= 2 && is_relevant_family(u16::from_be_bytes([write.key[0], write.key[1]]))
    }) {
        return Ok(());
    }
    let table = transaction.open_table(CATALOG).map_err(storage_error)?;
    if catalog_writes
        .iter()
        .any(|write| write.key.len() >= 2 && write.key[..2] == 4u16.to_be_bytes())
    {
        validate_final_builtin_type_catalog(&table, catalog_writes)?;
    }
    let mut changed_ids = HashSet::<[u8; 16]>::new();
    for write in catalog_writes.iter().filter(|write| {
        write.key.len() >= 2 && is_relevant_family(u16::from_be_bytes([write.key[0], write.key[1]]))
    }) {
        if write.key.len() != 19 {
            return Err(CommitError::InvalidCatalog);
        }
        let object_id = write.key[3..19]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        changed_ids.insert(object_id);
    }

    for object_id in changed_ids {
        let object_key = id_catalog_key(2, object_id);
        let object_bytes = final_catalog_value(&table, catalog_writes, &object_key)?;
        let object_kind = object_bytes
            .as_deref()
            .map(|bytes| catalog_field_bytes(&object_key, bytes, 2, 1))
            .transpose()?
            .map(|bytes| -> Result<u16, CommitError> {
                let bytes = bytes
                    .as_slice()
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                Ok(u16::from_le_bytes(bytes))
            })
            .transpose()?;

        if let Some(object_kind) = object_kind {
            let typed_family = match object_kind {
                1 => 3,
                2 => 4,
                3 => 5,
                4 => 6,
                5 => 7,
                6 => 8,
                7 => 9,
                8 | 9 => 10,
                10 => 13,
                _ => return Err(CommitError::InvalidCatalog),
            };
            let typed_key = id_catalog_key(typed_family, object_id);
            if final_catalog_value(&table, catalog_writes, &typed_key)?.is_none() {
                return Err(CommitError::InvalidCatalog);
            }
        }

        for family in TYPED_FAMILIES {
            let typed_key = id_catalog_key(family, object_id);
            let Some(typed_bytes) = final_catalog_value(&table, catalog_writes, &typed_key)? else {
                continue;
            };
            decode_catalog_record(&typed_key, &typed_bytes)?;
            let expected_kind = match family {
                3 => 1,
                4 => 2,
                5 => 3,
                6 => 4,
                7 => 5,
                8 => 6,
                9 => 7,
                10 => {
                    let principal_kind = u16::from_le_bytes(
                        catalog_field_bytes(&typed_key, &typed_bytes, family, 1)?
                            .as_slice()
                            .try_into()
                            .map_err(|_| CommitError::InvalidCatalog)?,
                    );
                    match principal_kind {
                        1 => 8,
                        2 => 9,
                        _ => return Err(CommitError::InvalidCatalog),
                    }
                }
                13 => 10,
                _ => unreachable!(),
            };
            if object_kind != Some(expected_kind) {
                return Err(CommitError::InvalidCatalog);
            }
            let name_tag = match family {
                6 => Some(3),
                10 => Some(2),
                _ => None,
            };
            if let Some(name_tag) = name_tag {
                let object_bytes = object_bytes.as_deref().ok_or(CommitError::InvalidCatalog)?;
                if catalog_field_bytes(&object_key, object_bytes, 2, 3)?
                    != catalog_field_bytes(&typed_key, &typed_bytes, family, name_tag)?
                {
                    return Err(CommitError::InvalidCatalog);
                }
            }
        }
    }
    Ok(())
}

/// Keep the object and typed rows named by the database bootstrap descriptor
/// valid in the final catalog view.
pub(super) fn validate_final_bootstrap_builtin_targets(
    transaction: &WriteTransaction,
    writes: &[CatalogWrite],
    database_uuid: [u8; 16],
) -> Result<(), CommitError> {
    let catalog = transaction.open_table(CATALOG).map_err(storage_error)?;
    let database_key = id_catalog_key(1, database_uuid);
    let Some(database_bytes) = final_catalog_value(&catalog, writes, &database_key)? else {
        return Ok(());
    };
    let encoded = catalog_field_bytes(&database_key, &database_bytes, 1, 3)?;
    let count_bytes: [u8; 4] = encoded
        .get(..4)
        .ok_or(CommitError::InvalidCatalog)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let count = u32::from_le_bytes(count_bytes) as usize;
    if count != 31 || encoded.len() != 4 + count * 20 {
        return Err(CommitError::InvalidCatalog);
    }
    let expected_codes = [1u16, 2, 3].into_iter().chain(101..=111).chain(201..=217);
    let mut mappings = HashMap::<u16, [u8; 16]>::new();
    let mut ids = HashSet::<[u8; 16]>::new();
    for (entry, expected_code) in encoded[4..].chunks_exact(20).zip(expected_codes) {
        let code = u16::from_le_bytes([entry[0], entry[1]]);
        let reserved = u16::from_le_bytes([entry[2], entry[3]]);
        let id: [u8; 16] = entry[4..]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        if code != expected_code
            || reserved != 0
            || id == [0; 16]
            || mappings.insert(code, id).is_some()
            || !ids.insert(id)
        {
            return Err(CommitError::InvalidCatalog);
        }
    }
    for code in [1u16, 2] {
        let schema_id = mappings.get(&code).ok_or(CommitError::InvalidCatalog)?;
        validate_final_builtin_typed_object(&catalog, writes, *schema_id, 3, 1)?;
    }
    let admin_role_id = mappings.get(&3).ok_or(CommitError::InvalidCatalog)?;
    validate_final_builtin_typed_object(&catalog, writes, *admin_role_id, 10, 9)?;
    let principal_key = id_catalog_key(10, *admin_role_id);
    let principal_bytes = final_catalog_value(&catalog, writes, &principal_key)?
        .ok_or(CommitError::InvalidCatalog)?;
    if catalog_field_bytes(&principal_key, &principal_bytes, 10, 1)? != 2u16.to_le_bytes()
        || catalog_field_bytes(&principal_key, &principal_bytes, 10, 2)? != b"database_admin"
    {
        return Err(CommitError::InvalidCatalog);
    }
    let object_key = id_catalog_key(2, *admin_role_id);
    let object_bytes =
        final_catalog_value(&catalog, writes, &object_key)?.ok_or(CommitError::InvalidCatalog)?;
    if catalog_field_bytes(&object_key, &object_bytes, 2, 3)? != b"database_admin" {
        return Err(CommitError::InvalidCatalog);
    }
    for code in 101u16..=111 {
        let type_id = mappings.get(&code).ok_or(CommitError::InvalidCatalog)?;
        validate_final_builtin_typed_object(&catalog, writes, *type_id, 4, 2)?;
        let type_key = id_catalog_key(4, *type_id);
        let type_bytes =
            final_catalog_value(&catalog, writes, &type_key)?.ok_or(CommitError::InvalidCatalog)?;
        let type_record = decode_catalog_record(&type_key, &type_bytes)?;
        if validate_builtin_type_profile(&type_record).map_err(|_| CommitError::InvalidCatalog)?
            != code - 100
        {
            return Err(CommitError::InvalidCatalog);
        }
    }
    Ok(())
}

pub(super) fn validate_final_builtin_typed_object(
    catalog: &redb::Table<'_, &[u8], &[u8]>,
    writes: &[CatalogWrite],
    object_id: [u8; 16],
    family: u16,
    object_kind: u16,
) -> Result<(), CommitError> {
    let typed_key = id_catalog_key(family, object_id);
    let typed_bytes =
        final_catalog_value(catalog, writes, &typed_key)?.ok_or(CommitError::InvalidCatalog)?;
    let typed_record = decode_catalog_record(&typed_key, &typed_bytes)?;
    if family == 4 && validate_builtin_type_profile(&typed_record).is_err() {
        return Err(CommitError::InvalidCatalog);
    }
    let object_key = id_catalog_key(2, object_id);
    let object_bytes =
        final_catalog_value(catalog, writes, &object_key)?.ok_or(CommitError::InvalidCatalog)?;
    if catalog_field_bytes(&object_key, &object_bytes, 2, 1)? != object_kind.to_le_bytes() {
        return Err(CommitError::InvalidCatalog);
    }
    Ok(())
}

pub(super) fn insert_expression_reference_edge(
    edges: &mut HashSet<([u8; 16], [u8; 16])>,
    dependent_id: [u8; 16],
    referenced_id: [u8; 16],
) -> Result<(), CommitError> {
    edges.insert((dependent_id, referenced_id));
    if edges.len() > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
        return Err(CommitError::InvalidCatalog);
    }
    Ok(())
}

pub(super) fn insert_authority_edge(
    edges: &mut HashSet<([u8; 16], [u8; 16])>,
    dependent_id: [u8; 16],
    referenced_id: [u8; 16],
) -> Result<(), CommitError> {
    edges.insert((dependent_id, referenced_id));
    if edges.len() > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
        return Err(CommitError::InvalidCatalog);
    }
    Ok(())
}

pub(super) fn validate_final_builtin_type_catalog(
    table: &redb::Table<'_, &[u8], &[u8]>,
    catalog_writes: &[CatalogWrite],
) -> Result<(), CommitError> {
    let family_prefix = 4u16.to_be_bytes();
    let mut seen_kinds = HashSet::new();
    let mut family_count = 0usize;
    for entry in table
        .range(family_prefix.as_slice()..)
        .map_err(storage_error)?
    {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&family_prefix) {
            break;
        }
        if catalog_writes.iter().any(|write| write.key == key) {
            continue;
        }
        family_count += 1;
        if family_count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
            return Err(CommitError::InvalidCatalog);
        }
        let record = decode_catalog_record(key, value.value())?;
        let builtin_kind =
            validate_builtin_type_profile(&record).map_err(|_| CommitError::InvalidCatalog)?;
        if !seen_kinds.insert(builtin_kind) {
            return Err(CommitError::InvalidCatalog);
        }
    }
    for write in catalog_writes
        .iter()
        .filter(|write| write.key.starts_with(&family_prefix))
    {
        if let Some(bytes) = &write.payload {
            family_count += 1;
            if family_count > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
                return Err(CommitError::InvalidCatalog);
            }
            let record = decode_catalog_record(&write.key, bytes)?;
            let builtin_kind =
                validate_builtin_type_profile(&record).map_err(|_| CommitError::InvalidCatalog)?;
            if !seen_kinds.insert(builtin_kind) {
                return Err(CommitError::InvalidCatalog);
            }
        }
    }
    Ok(())
}

pub(super) fn collect_dependency_catalog_record(
    key: &[u8],
    bytes: &[u8],
    graph: &mut DependencyCatalogGraph,
) -> Result<(), CommitError> {
    let record = decode_catalog_record(key, bytes)?;
    match record.family_code {
        1 => {
            if key.len() != 19
                || graph
                    .database_id
                    .replace(key[3..19].try_into().unwrap())
                    .is_some()
            {
                return Err(CommitError::InvalidCatalog);
            }
        }
        2 => {
            if key.len() != 19 {
                return Err(CommitError::InvalidCatalog);
            }
            let object_id = key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let parent_id = record
                .fields
                .iter()
                .find(|field| field.tag == 2)
                .map(|field| {
                    field
                        .payload
                        .as_slice()
                        .try_into()
                        .map_err(|_| CommitError::InvalidCatalog)
                })
                .transpose()?;
            let object_kind = u16::from_le_bytes(
                catalog_field_bytes(key, bytes, 2, 1)?
                    .as_slice()
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?,
            );
            if !(1..=10).contains(&object_kind)
                || graph.object_parents.insert(object_id, parent_id).is_some()
                || graph.object_kinds.insert(object_id, object_kind).is_some()
            {
                return Err(CommitError::InvalidCatalog);
            }
        }
        5 => {
            if key.len() != 19 {
                return Err(CommitError::InvalidCatalog);
            }
            let table_id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            if graph.typed_object_kinds.insert(table_id, 3).is_some() {
                return Err(CommitError::InvalidCatalog);
            }
            let schema_id: [u8; 16] = catalog_field_bytes(key, bytes, 5, 1)?
                .as_slice()
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            if graph.table_schemas.insert(table_id, schema_id).is_some() {
                return Err(CommitError::InvalidCatalog);
            }
        }
        6 => {
            if key.len() != 19 {
                return Err(CommitError::InvalidCatalog);
            }
            let column_id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            if graph.typed_object_kinds.insert(column_id, 4).is_some() {
                return Err(CommitError::InvalidCatalog);
            }
            let table_id: [u8; 16] = catalog_field_bytes(key, bytes, 6, 1)?
                .as_slice()
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            if graph.column_tables.insert(column_id, table_id).is_some() {
                return Err(CommitError::InvalidCatalog);
            }
            for tag in [7, 10] {
                if let Some(field) = record.fields.iter().find(|field| field.tag == tag) {
                    let expression_id: [u8; 16] = field
                        .payload
                        .as_slice()
                        .try_into()
                        .map_err(|_| CommitError::InvalidCatalog)?;
                    insert_expression_reference_edge(
                        &mut graph.expression_reference_edges,
                        column_id,
                        expression_id,
                    )?;
                }
            }
        }
        7 => {
            if key.len() != 19 {
                return Err(CommitError::InvalidCatalog);
            }
            let expression = decode_catalog_expression_record(&record)
                .map_err(|_| CommitError::InvalidCatalog)?;
            if graph
                .typed_object_kinds
                .insert(expression.expression_id, 5)
                .is_some()
            {
                return Err(CommitError::InvalidCatalog);
            }
            let mut unique_dependencies = HashSet::new();
            for dependency_id in expression.dependencies {
                if dependency_id == [0; 16] || !unique_dependencies.insert(dependency_id) {
                    return Err(CommitError::InvalidCatalog);
                }
                insert_expression_reference_edge(
                    &mut graph.expression_reference_edges,
                    expression.expression_id,
                    dependency_id,
                )?;
            }
        }
        8 => {
            if key.len() != 19 {
                return Err(CommitError::InvalidCatalog);
            }
            let constraint_id = key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            if graph.typed_object_kinds.insert(constraint_id, 6).is_some() {
                return Err(CommitError::InvalidCatalog);
            }
            let kind = u16::from_le_bytes(
                catalog_field_bytes(key, bytes, 8, 2)?
                    .as_slice()
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?,
            );
            if !matches!(kind, 1..=5)
                || graph.constraint_kinds.insert(constraint_id, kind).is_some()
            {
                return Err(CommitError::InvalidCatalog);
            }
            if kind == 5
                && graph
                    .foreign_key_targets
                    .insert(
                        constraint_id,
                        catalog_field_bytes(key, bytes, 8, 5)?
                            .as_slice()
                            .try_into()
                            .map_err(|_| CommitError::InvalidCatalog)?,
                    )
                    .is_some()
            {
                return Err(CommitError::InvalidCatalog);
            }
            if matches!(kind, 2 | 3)
                && graph
                    .backing_index_targets
                    .insert(
                        constraint_id,
                        catalog_field_bytes(key, bytes, 8, 11)?
                            .as_slice()
                            .try_into()
                            .map_err(|_| CommitError::InvalidCatalog)?,
                    )
                    .is_some()
            {
                return Err(CommitError::InvalidCatalog);
            }
            if kind == 4 {
                insert_expression_reference_edge(
                    &mut graph.expression_reference_edges,
                    constraint_id,
                    catalog_field_bytes(key, bytes, 8, 4)?
                        .as_slice()
                        .try_into()
                        .map_err(|_| CommitError::InvalidCatalog)?,
                )?;
            } else if record.fields.iter().any(|field| field.tag == 4) {
                return Err(CommitError::InvalidCatalog);
            }
        }
        9 => {
            if key.len() != 19 {
                return Err(CommitError::InvalidCatalog);
            }
            let index_id = key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let table_id: [u8; 16] = catalog_field_bytes(key, bytes, 9, 1)?
                .as_slice()
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let columns = catalog_id_list_field(key, bytes, 9, 2)?;
            let mut unique_columns = HashSet::new();
            if graph.typed_object_kinds.insert(index_id, 7).is_some() {
                return Err(CommitError::InvalidCatalog);
            }
            if columns.is_empty()
                || columns
                    .iter()
                    .any(|column_id| !unique_columns.insert(*column_id))
                || graph.index_tables.insert(index_id, table_id).is_some()
                || graph.index_columns.insert(index_id, columns).is_some()
            {
                return Err(CommitError::InvalidCatalog);
            }
            if let Some(field) = record.fields.iter().find(|field| field.tag == 6)
                && graph
                    .index_owners
                    .insert(
                        index_id,
                        field
                            .payload
                            .as_slice()
                            .try_into()
                            .map_err(|_| CommitError::InvalidCatalog)?,
                    )
                    .is_some()
            {
                return Err(CommitError::InvalidCatalog);
            }
        }
        10 => {
            if key.len() != 19 {
                return Err(CommitError::InvalidCatalog);
            }
            let principal_id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let principal_kind = decode_principal_kind(key, bytes)?;
            let object_kind = match principal_kind {
                1 => 8,
                2 => 9,
                _ => return Err(CommitError::InvalidCatalog),
            };
            if graph
                .typed_object_kinds
                .insert(principal_id, object_kind)
                .is_some()
            {
                return Err(CommitError::InvalidCatalog);
            }
            graph.authority_principals.insert(principal_id);
        }
        11 => {
            if key.len() != 36 || catalog_field_u64(key, bytes, 11, 2)? == 0 {
                return Err(CommitError::InvalidCatalog);
            }
            let member_id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let role_id: [u8; 16] = key[20..36]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let grantor_id: [u8; 16] = catalog_field_bytes(key, bytes, 11, 1)?
                .as_slice()
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            if !graph.authority_principals.contains(&grantor_id)
                || !graph
                    .typed_object_kinds
                    .get(&member_id)
                    .is_some_and(|kind| matches!(*kind, 8 | 9))
                || graph.typed_object_kinds.get(&role_id) != Some(&9)
            {
                return Err(CommitError::InvalidCatalog);
            }
            insert_authority_edge(&mut graph.authority_edges, member_id, role_id)?;
        }
        12 => {
            if key.len() != 59
                || key[2] != 1
                || key[19] != 1
                || key[36] != 1
                || key[39] != 1
                || key[42] != 1
                || catalog_field_u64(key, bytes, 12, 2)? == 0
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
            let grantor_id: [u8; 16] = catalog_field_bytes(key, bytes, 12, 1)?
                .as_slice()
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            if !graph.authority_principals.contains(&grantee_id)
                || !graph.authority_principals.contains(&grantor_id)
            {
                return Err(CommitError::InvalidCatalog);
            }
            let target_kind = graph.object_kinds.get(&target_id).copied();
            let valid_scope = match (privilege, scope) {
                (1 | 7, 1) => object_id == target_id && target_kind == Some(1),
                (2..=4 | 6, 1) => object_id == target_id && target_kind == Some(3),
                (5, 1) => object_id == target_id && target_kind == Some(3),
                (8..=11, 1) => object_id == target_id && matches!(target_kind, Some(1 | 3)),
                (2..=4 | 6, 2) => {
                    graph.object_kinds.get(&object_id) == Some(&3)
                        && target_kind == Some(4)
                        && graph.column_tables.get(&target_id) == Some(&object_id)
                }
                (12..=14, 1) => object_id == target_id && Some(target_id) == graph.database_id,
                _ => false,
            };
            if !valid_scope {
                return Err(CommitError::InvalidCatalog);
            }
            insert_authority_edge(&mut graph.authority_edges, grantee_id, target_id)?;
        }
        14 => {
            if key.len() != 39 || key[2] != 1 || key[19] != 1 || key[22] != 1 {
                return Err(CommitError::InvalidCatalog);
            }
            let policy_id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let target_kind = u16::from_be_bytes([key[20], key[21]]);
            let target_id: [u8; 16] = key[23..39]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            if graph.typed_object_kinds.get(&policy_id) != Some(&10) {
                return Err(CommitError::InvalidCatalog);
            }
            match target_kind {
                1 if graph.typed_object_kinds.get(&target_id) == Some(&8) => {
                    insert_authority_edge(&mut graph.authority_edges, policy_id, target_id)?;
                }
                2 if graph.typed_object_kinds.get(&target_id) == Some(&9) => {
                    insert_authority_edge(&mut graph.authority_edges, policy_id, target_id)?;
                }
                3 if graph.policy_kinds.get(&policy_id) == Some(&2)
                    && graph.typed_object_kinds.get(&target_id) == Some(&4)
                    && graph.column_tables.get(&target_id)
                        == graph.policy_tables.get(&policy_id)
                    && graph.policy_tables.get(&policy_id).is_some_and(|table_id| {
                        graph.object_kinds.get(table_id) == Some(&3)
                            && graph.typed_object_kinds.get(table_id) == Some(&3)
                    }) => {}
                _ => return Err(CommitError::InvalidCatalog),
            }
        }
        13 => {
            if key.len() != 19 {
                return Err(CommitError::InvalidCatalog);
            }
            let policy_id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            if graph.typed_object_kinds.insert(policy_id, 10).is_some() {
                return Err(CommitError::InvalidCatalog);
            }
            let table_id: [u8; 16] = catalog_field_bytes(key, bytes, 13, 1)?
                .as_slice()
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let policy_kind = u16::from_le_bytes(
                catalog_field_bytes(key, bytes, 13, 2)?
                    .as_slice()
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?,
            );
            if !matches!(policy_kind, 1 | 2)
                || graph.policy_tables.insert(policy_id, table_id).is_some()
                || graph.policy_kinds.insert(policy_id, policy_kind).is_some()
            {
                return Err(CommitError::InvalidCatalog);
            }
            for tag in [5, 6] {
                if let Some(field) = record.fields.iter().find(|field| field.tag == tag) {
                    let expression_id: [u8; 16] = field
                        .payload
                        .as_slice()
                        .try_into()
                        .map_err(|_| CommitError::InvalidCatalog)?;
                    insert_expression_reference_edge(
                        &mut graph.expression_reference_edges,
                        policy_id,
                        expression_id,
                    )?;
                }
            }
        }
        15 => {
            if key.len() != 39 {
                return Err(CommitError::InvalidCatalog);
            }
            let dependent_id = key[3..19]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let referenced_id = key[20..36]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let kind = u16::from_be_bytes(
                key[37..39]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?,
            );
            if !matches!(kind, 1..=5) {
                return Err(CommitError::InvalidCatalog);
            }
            graph.dependencies.push((dependent_id, referenced_id, kind));
        }
        _ => return Err(CommitError::InvalidCatalog),
    }
    Ok(())
}
