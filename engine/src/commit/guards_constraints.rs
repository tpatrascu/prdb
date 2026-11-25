use super::fenced_ddl::add_catalog_guard;
use super::*;
use crate::audit::AuditEvent;
use crate::authorization::{MAX_MEMBERSHIP_EDGES, resolve_role_closure, validate_membership_graph};
use crate::catalog::decode_builtin_scalar_type;
use crate::expression::decode_catalog_expression_record;
use crate::metadata::EngineMetadata;
use crate::schema::{CATALOG, ENGINE_META};
use redb::{Database, ReadableDatabase};
use std::collections::{HashMap, HashSet};

pub(super) fn constraint_ddl_authority_guards(
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

    let read = database.begin_read().map_err(storage_error)?;
    let catalog = read.open_table(CATALOG).map_err(storage_error)?;
    #[derive(Clone)]
    struct ConstraintChange {
        table_id: [u8; 16],
        kind: u16,
        subject_id: [u8; 16],
        columns: Vec<[u8; 16]>,
        adding: bool,
    }
    let mut constraint_changes = HashMap::<[u8; 16], ConstraintChange>::new();
    let mut object_changes = HashMap::<[u8; 16], (u16, bool, bool)>::new();
    let mut column_changes = HashMap::<[u8; 16], ([u8; 16], bool, bool)>::new();
    let mut type_column_changes = HashMap::<[u8; 16], ([u8; 16], [u8; 8], [u8; 8])>::new();
    let mut added_columns =
        HashMap::<[u8; 16], ([u8; 16], Option<[u8; 16]>, Option<[u8; 16]>)>::new();
    let mut default_column_changes =
        HashMap::<[u8; 16], ([u8; 16], Option<[u8; 16]>, Option<[u8; 16]>)>::new();
    let mut table_changes = HashSet::<[u8; 16]>::new();
    let mut table_schema_changes = HashMap::<[u8; 16], (u64, u64, u64, u64)>::new();
    let mut index_definition_states = HashMap::<[u8; 16], (bool, bool)>::new();
    let mut index_schema_updates = HashMap::<[u8; 16], ([u8; 16], u64, u64)>::new();
    let mut index_details = HashMap::<[u8; 16], ([u8; 16], Vec<[u8; 16]>, [u8; 16])>::new();
    let mut tombstone_changes = HashSet::<[u8; 16]>::new();
    let mut expression_changes =
        HashMap::<[u8; 16], (bool, Vec<[u8; 16]>, crate::expression::ExpressionPurpose)>::new();
    let mut dependency_changes = HashMap::<([u8; 16], [u8; 16]), (u16, bool, bool)>::new();

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
            return Err(CommitError::InvalidCatalog);
        }
        match family {
            17 => {
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
            }
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
                let object_kind = catalog_field_u16(&write.key, object_bytes, 2, 1)?;
                if object_id == [0; 16]
                    || !matches!(object_kind, 4..=7)
                    || object_changes
                        .insert(
                            object_id,
                            (object_kind, current.is_some(), write.payload.is_some()),
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
                let current_schema = table_schema_version(&write.key, current_bytes)?;
                let next_schema = table_schema_version(&write.key, next_bytes)?;
                let current_epoch = table_write_epoch(&write.key, current_bytes)?;
                let next_epoch = table_write_epoch(&write.key, next_bytes)?;
                if !catalog_fields_equal_except(&current_record, &next_record, &[2, 3])
                    || (current_schema != next_schema
                        && current_schema.checked_add(1) != Some(next_schema))
                    || next_epoch <= current_epoch
                    || !table_changes.insert(table_id)
                {
                    return Err(CommitError::InvalidCatalog);
                }
                table_schema_changes.insert(
                    table_id,
                    (current_schema, next_schema, current_epoch, next_epoch),
                );
            }
            6 => {
                if write.key.len() != 19 || write.payload.is_none() {
                    return Err(CommitError::InvalidCatalog);
                }
                let column_id: [u8; 16] = write.key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                if current.is_none() {
                    let next_bytes = write
                        .payload
                        .as_deref()
                        .ok_or(CommitError::InvalidCatalog)?;
                    let table_id = column_table_id(&write.key, next_bytes)?;
                    let next_record = decode_catalog_record(&write.key, next_bytes)?;
                    let default_id = next_record
                        .fields
                        .iter()
                        .find(|field| field.tag == 7)
                        .map(|field| {
                            field
                                .payload
                                .as_slice()
                                .try_into()
                                .map_err(|_| CommitError::InvalidCatalog)
                        })
                        .transpose()?;
                    let add_default_id = next_record
                        .fields
                        .iter()
                        .find(|field| field.tag == 10)
                        .map(|field| {
                            field
                                .payload
                                .as_slice()
                                .try_into()
                                .map_err(|_| CommitError::InvalidCatalog)
                        })
                        .transpose()?;
                    if column_id == [0; 16]
                        || added_columns
                            .insert(column_id, (table_id, default_id, add_default_id))
                            .is_some()
                    {
                        return Err(CommitError::InvalidCatalog);
                    }
                    continue;
                }
                let current_bytes = current.as_deref().ok_or(CommitError::InvalidCatalog)?;
                let next_bytes = write
                    .payload
                    .as_deref()
                    .ok_or(CommitError::InvalidCatalog)?;
                let current_nullable = catalog_field_bytes(&write.key, current_bytes, 6, 6)?;
                let next_nullable = catalog_field_bytes(&write.key, next_bytes, 6, 6)?;
                let current_type_id = catalog_field_bytes(&write.key, current_bytes, 6, 4)?;
                let next_type_id = catalog_field_bytes(&write.key, next_bytes, 6, 4)?;
                let current_type_parameters: [u8; 8] =
                    catalog_field_bytes(&write.key, current_bytes, 6, 5)?
                        .try_into()
                        .map_err(|_| CommitError::InvalidCatalog)?;
                let next_type_parameters: [u8; 8] =
                    catalog_field_bytes(&write.key, next_bytes, 6, 5)?
                        .try_into()
                        .map_err(|_| CommitError::InvalidCatalog)?;
                let table_id = column_table_id(&write.key, current_bytes)?;
                let current_record = decode_catalog_record(&write.key, current_bytes)?;
                let next_record = decode_catalog_record(&write.key, next_bytes)?;
                if !matches!(current_nullable.as_slice(), [0] | [1])
                    || !matches!(next_nullable.as_slice(), [0] | [1])
                    || column_table_id(&write.key, next_bytes)? != table_id
                {
                    return Err(CommitError::InvalidCatalog);
                }
                if current_type_parameters != next_type_parameters {
                    if current_type_id != next_type_id
                        || current_nullable != next_nullable
                        || !catalog_fields_equal_except(&current_record, &next_record, &[5])
                        || type_column_changes
                            .insert(
                                column_id,
                                (table_id, current_type_parameters, next_type_parameters),
                            )
                            .is_some()
                    {
                        return Err(CommitError::InvalidCatalog);
                    }
                } else if current_nullable != next_nullable {
                    if !catalog_fields_equal_except(&current_record, &next_record, &[6])
                        || column_changes
                            .insert(
                                column_id,
                                (table_id, current_nullable == [1], next_nullable == [1]),
                            )
                            .is_some()
                    {
                        return Err(CommitError::InvalidCatalog);
                    }
                } else {
                    let current_default = current_record
                        .fields
                        .iter()
                        .find(|field| field.tag == 7)
                        .map(|field| {
                            field
                                .payload
                                .as_slice()
                                .try_into()
                                .map_err(|_| CommitError::InvalidCatalog)
                        })
                        .transpose()?;
                    let next_default = next_record
                        .fields
                        .iter()
                        .find(|field| field.tag == 7)
                        .map(|field| {
                            field
                                .payload
                                .as_slice()
                                .try_into()
                                .map_err(|_| CommitError::InvalidCatalog)
                        })
                        .transpose()?;
                    if current_default == next_default
                        || !catalog_fields_equal_except(&current_record, &next_record, &[7])
                        || default_column_changes
                            .insert(column_id, (table_id, current_default, next_default))
                            .is_some()
                    {
                        return Err(CommitError::InvalidCatalog);
                    }
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
                let kind = catalog_field_u16(&write.key, bytes, 8, 2)?;
                if constraint_id == [0; 16] || !matches!(kind, 1..=4) {
                    return Err(CommitError::InvalidCatalog);
                }
                validate_constraint_shape(&write.key, bytes)?;
                let table_id = constraint_table_id(&write.key, bytes)?;
                let columns = catalog_id_list_field(&write.key, bytes, 8, 3)?;
                let subject_id = match kind {
                    1 if columns.len() == 1 => columns[0],
                    2 | 3 if !columns.is_empty() => {
                        let index_id: [u8; 16] = catalog_field_bytes(&write.key, bytes, 8, 11)?
                            .try_into()
                            .map_err(|_| CommitError::InvalidCatalog)?;
                        if index_id == [0; 16] {
                            return Err(CommitError::InvalidCatalog);
                        }
                        index_id
                    }
                    4 if columns.is_empty() => {
                        let expression_id: [u8; 16] = catalog_field_bytes(&write.key, bytes, 8, 4)?
                            .try_into()
                            .map_err(|_| CommitError::InvalidCatalog)?;
                        if expression_id == [0; 16] {
                            return Err(CommitError::InvalidCatalog);
                        }
                        expression_id
                    }
                    _ => return Err(CommitError::InvalidCatalog),
                };
                if constraint_changes
                    .insert(
                        constraint_id,
                        ConstraintChange {
                            table_id,
                            kind,
                            subject_id,
                            columns,
                            adding: write.payload.is_some(),
                        },
                    )
                    .is_some()
                {
                    return Err(CommitError::InvalidCatalog);
                }
            }
            9 => {
                if write.key.len() != 19 {
                    return Err(CommitError::InvalidCatalog);
                }
                let index_id: [u8; 16] = write.key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                if let (Some(old_bytes), Some(new_bytes)) =
                    (current.as_deref(), write.payload.as_deref())
                {
                    let old_record = decode_catalog_record(&write.key, old_bytes)?;
                    let new_record = decode_catalog_record(&write.key, new_bytes)?;
                    let (old_id, table_id) = decode_index_definition(&write.key, old_bytes)?;
                    let (new_id, new_table_id) = decode_index_definition(&write.key, new_bytes)?;
                    let old_version = catalog_field_u64(&write.key, old_bytes, 9, 10)?;
                    let new_version = catalog_field_u64(&write.key, new_bytes, 9, 10)?;
                    if index_id == [0; 16]
                        || old_id != index_id
                        || new_id != index_id
                        || table_id != new_table_id
                        || old_version.checked_add(1) != Some(new_version)
                        || !catalog_fields_equal_except(&old_record, &new_record, &[10])
                        || index_schema_updates
                            .insert(index_id, (table_id, old_version, new_version))
                            .is_some()
                    {
                        return Err(CommitError::InvalidCatalog);
                    }
                    continue;
                }
                if current.is_some() == write.payload.is_some() {
                    return Err(CommitError::InvalidCatalog);
                }
                let bytes = current
                    .as_deref()
                    .or(write.payload.as_deref())
                    .ok_or(CommitError::InvalidCatalog)?;
                let (decoded_id, table_id) = decode_index_definition(&write.key, bytes)?;
                let columns = catalog_id_list_field(&write.key, bytes, 9, 2)?;
                let unique = catalog_field_bytes(&write.key, bytes, 9, 4)?;
                let owner: [u8; 16] = catalog_field_bytes(&write.key, bytes, 9, 6)?
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                if index_id == [0; 16]
                    || decoded_id != index_id
                    || unique != [1]
                    || owner == [0; 16]
                    || index_definition_states
                        .insert(index_id, (current.is_some(), write.payload.is_some()))
                        .is_some()
                    || index_details
                        .insert(index_id, (table_id, columns, owner))
                        .is_some()
                {
                    return Err(CommitError::InvalidCatalog);
                }
            }
            7 => {
                if write.key.len() != 19 || current.is_some() == write.payload.is_some() {
                    return Err(CommitError::InvalidCatalog);
                }
                let expression_id: [u8; 16] = write.key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                let bytes = current
                    .as_deref()
                    .or(write.payload.as_deref())
                    .ok_or(CommitError::InvalidCatalog)?;
                let record = decode_catalog_record(&write.key, bytes)?;
                let expression = decode_catalog_expression_record(&record)
                    .map_err(|_| CommitError::InvalidCatalog)?;
                if expression_id == [0; 16]
                    || expression.expression_id != expression_id
                    || !matches!(
                        expression.purpose,
                        crate::expression::ExpressionPurpose::Check
                            | crate::expression::ExpressionPurpose::Default
                    )
                    || (expression.purpose == crate::expression::ExpressionPurpose::Default
                        && !expression.dependencies.is_empty())
                {
                    return Err(CommitError::InvalidCatalog);
                }
                let mut dependencies = HashSet::new();
                if expression
                    .dependencies
                    .iter()
                    .any(|dependency| *dependency == [0; 16] || !dependencies.insert(*dependency))
                    || expression_changes
                        .insert(
                            expression_id,
                            (
                                write.payload.is_some(),
                                expression.dependencies,
                                expression.purpose,
                            ),
                        )
                        .is_some()
                {
                    return Err(CommitError::InvalidCatalog);
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
                if !matches!(kind, 1 | 2 | 4)
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

    let direct_nullable_drop = constraint_changes.is_empty()
        && column_changes.len() == 1
        && column_changes
            .values()
            .all(|(_, was_nullable, is_nullable)| !was_nullable && *is_nullable)
        && object_changes.is_empty()
        && index_definition_states.is_empty()
        && index_details.is_empty()
        && expression_changes.is_empty()
        && dependency_changes.is_empty();
    let direct_default_change = constraint_changes.is_empty()
        && default_column_changes.len() == 1
        && column_changes.is_empty()
        && type_column_changes.is_empty()
        && index_definition_states.is_empty()
        && index_details.is_empty();
    let direct_varchar_widen = constraint_changes.is_empty()
        && type_column_changes.len() == 1
        && column_changes.is_empty()
        && default_column_changes.is_empty()
        && added_columns.is_empty()
        && object_changes.is_empty()
        && index_definition_states.is_empty()
        && index_details.is_empty()
        && expression_changes.is_empty()
        && dependency_changes.is_empty()
        && index_schema_updates.is_empty()
        && table_changes.len() == 1;
    let direct_column_add = constraint_changes.is_empty()
        && added_columns.len() == 1
        && column_changes.is_empty()
        && default_column_changes.is_empty()
        && index_definition_states.is_empty()
        && index_details.is_empty()
        && table_changes.len() == 1;
    if constraint_changes.is_empty()
        && !direct_nullable_drop
        && !direct_default_change
        && !direct_varchar_widen
        && !direct_column_add
    {
        return Err(CommitError::InvalidCatalog);
    }

    let mut affected_tables = HashSet::new();
    let mut expected_columns = HashMap::<[u8; 16], ([u8; 16], bool, bool)>::new();
    let mut expected_objects = HashMap::<[u8; 16], (u16, bool, bool)>::new();
    let mut expected_dependencies = HashMap::<([u8; 16], [u8; 16]), (u16, bool, bool)>::new();
    let mut expected_index_definitions = HashMap::<[u8; 16], (bool, bool)>::new();
    let mut check_expression_ids = HashSet::new();
    for (constraint_id, change) in &constraint_changes {
        affected_tables.insert(change.table_id);
        let constraint_object_state = (6, !change.adding, change.adding);
        if expected_objects
            .insert(*constraint_id, constraint_object_state)
            .is_some()
        {
            return Err(CommitError::InvalidCatalog);
        }
        match change.kind {
            1 => {
                let nullable = if change.adding {
                    (true, false)
                } else {
                    (false, true)
                };
                if expected_columns
                    .insert(change.subject_id, (change.table_id, nullable.0, nullable.1))
                    .is_some()
                {
                    return Err(CommitError::InvalidCatalog);
                }
            }
            4 => {
                if !check_expression_ids.insert(change.subject_id)
                    || expression_changes.get(&change.subject_id).is_none_or(
                        |(adding, _, purpose)| {
                            *adding != change.adding
                                || *purpose != crate::expression::ExpressionPurpose::Check
                        },
                    )
                {
                    return Err(CommitError::InvalidCatalog);
                }
                expected_dependencies.insert(
                    (*constraint_id, change.subject_id),
                    (2, !change.adding, change.adding),
                );
            }
            2 | 3 => {
                if expected_objects
                    .insert(change.subject_id, (7, !change.adding, change.adding))
                    .is_some()
                {
                    return Err(CommitError::InvalidCatalog);
                }
                let index_state = (!change.adding, change.adding);
                let (index_table_id, index_columns, index_owner) = index_details
                    .get(&change.subject_id)
                    .ok_or(CommitError::InvalidCatalog)?;
                if *index_table_id != change.table_id
                    || *index_columns != change.columns
                    || *index_owner != *constraint_id
                    || index_definition_states.get(&change.subject_id) != Some(&index_state)
                    || expected_index_definitions
                        .insert(change.subject_id, index_state)
                        .is_some()
                {
                    return Err(CommitError::InvalidCatalog);
                }
                expected_dependencies.insert(
                    (*constraint_id, change.subject_id),
                    (4, !change.adding, change.adding),
                );
            }
            _ => return Err(CommitError::InvalidCatalog),
        }
    }
    if direct_nullable_drop {
        for (column_id, (table_id, was_nullable, is_nullable)) in &column_changes {
            if expected_columns
                .insert(*column_id, (*table_id, *was_nullable, *is_nullable))
                .is_some()
            {
                return Err(CommitError::InvalidCatalog);
            }
            affected_tables.insert(*table_id);
        }
    }
    if direct_varchar_widen {
        for (column_id, (table_id, old_parameters, new_parameters)) in &type_column_changes {
            affected_tables.insert(*table_id);
            let (old_schema_version, next_schema_version, old_epoch, next_epoch) =
                table_schema_changes
                    .get(table_id)
                    .copied()
                    .ok_or(CommitError::InvalidCatalog)?;
            if old_schema_version != next_schema_version
                || old_epoch.checked_add(1) != Some(next_epoch)
            {
                return Err(CommitError::InvalidCatalog);
            }
            let old_schema = load_final_table_schema(&catalog, &[], *table_id)?;
            let next_schema = load_final_table_schema(&catalog, catalog_writes, *table_id)?;
            if old_schema.column_ids != next_schema.column_ids
                || old_schema.columns.len() != next_schema.columns.len()
                || next_schema.schema_version != old_schema_version
            {
                return Err(CommitError::InvalidCatalog);
            }
            let ordinal = old_schema
                .column_ids
                .iter()
                .position(|candidate| candidate == column_id)
                .ok_or(CommitError::InvalidCatalog)?;
            let old_type = old_schema.columns[ordinal].scalar_type;
            let new_type = next_schema.columns[ordinal].scalar_type;
            let (
                crate::tuple::ScalarType::Varchar { max_chars: old_max },
                crate::tuple::ScalarType::Varchar { max_chars: new_max },
            ) = (old_type, new_type)
            else {
                return Err(CommitError::InvalidCatalog);
            };
            if old_max >= new_max
                || old_schema
                    .columns
                    .iter()
                    .enumerate()
                    .any(|(index, old)| index != ordinal && *old != next_schema.columns[index])
                || old_schema.columns[ordinal].nullable != next_schema.columns[ordinal].nullable
            {
                return Err(CommitError::InvalidCatalog);
            }
            let column_key = id_catalog_key(6, *column_id);
            let column_write = catalog_writes
                .iter()
                .find(|write| write.key == column_key)
                .ok_or(CommitError::InvalidCatalog)?;
            let column_bytes = column_write
                .payload
                .as_deref()
                .ok_or(CommitError::InvalidCatalog)?;
            let current_bytes = column_write
                .expected_value
                .as_deref()
                .ok_or(CommitError::InvalidCatalog)?;
            let type_id = catalog_field_bytes(&column_key, current_bytes, 6, 4)?;
            if catalog_field_bytes(&column_key, column_bytes, 6, 4)? != type_id
                || catalog_field_bytes(&column_key, current_bytes, 6, 5)? != old_parameters
                || catalog_field_bytes(&column_key, column_bytes, 6, 5)? != new_parameters
                || catalog_field_u64(&column_key, column_bytes, 6, 9)? != old_schema_version
            {
                return Err(CommitError::InvalidCatalog);
            }
        }
    }
    let mut default_expression_ids = HashSet::new();
    if direct_default_change {
        for (column_id, (table_id, old_default, next_default)) in &default_column_changes {
            affected_tables.insert(*table_id);
            let column_key = id_catalog_key(6, *column_id);
            let column_write = catalog_writes
                .iter()
                .find(|write| write.key == column_key)
                .ok_or(CommitError::InvalidCatalog)?;
            let old_column = column_write
                .expected_value
                .as_deref()
                .ok_or(CommitError::InvalidCatalog)?;
            let next_column = column_write
                .payload
                .as_deref()
                .ok_or(CommitError::InvalidCatalog)?;
            let ordinal = catalog_field_u32(&column_key, old_column, 6, 2)?;
            let type_id: [u8; 16] = catalog_field_bytes(&column_key, old_column, 6, 4)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let type_parameters: [u8; 8] = catalog_field_bytes(&column_key, old_column, 6, 5)?
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let type_key = id_catalog_key(4, type_id);
            let type_bytes = catalog
                .get(type_key.as_slice())
                .map_err(storage_error)?
                .map(|value| value.value().to_vec())
                .ok_or(CommitError::InvalidCatalog)?;
            let type_record = decode_catalog_record(&type_key, &type_bytes)?;
            let scalar_type = decode_builtin_scalar_type(&type_record, &type_parameters)
                .map_err(|_| CommitError::InvalidCatalog)?;
            let default_mask = catalog_field_u32(&type_key, &type_bytes, 4, 5)?;
            for (expression_id, adding) in [
                old_default.map(|id| (id, false)),
                next_default.map(|id| (id, true)),
            ]
            .into_iter()
            .flatten()
            {
                let expression_key = id_catalog_key(7, expression_id);
                let expression_write = catalog_writes
                    .iter()
                    .find(|write| write.key == expression_key)
                    .ok_or(CommitError::InvalidCatalog)?;
                if expression_write.payload.is_some() != adding
                    || (!adding && expression_write.expected_value.is_none())
                    || (adding && expression_write.expected_value.is_some())
                {
                    return Err(CommitError::InvalidCatalog);
                }
                let expression_bytes = expression_write
                    .payload
                    .as_deref()
                    .or(expression_write.expected_value.as_deref())
                    .ok_or(CommitError::InvalidCatalog)?;
                let expression_record = decode_catalog_record(&expression_key, expression_bytes)?;
                let expression = decode_catalog_expression_record(&expression_record)
                    .map_err(|_| CommitError::InvalidCatalog)?;
                let allowed = match &expression.expression {
                    crate::authorization::Expr::Literal(_) => default_mask & 1 != 0,
                    crate::authorization::Expr::CurrentPrincipalId => {
                        default_mask & 4 != 0 && scalar_type == crate::tuple::ScalarType::Uuid
                    }
                    _ => false,
                };
                if !allowed
                    || expression.expression_id != expression_id
                    || expression.purpose != crate::expression::ExpressionPurpose::Default
                    || expression.result_type_id != type_id
                    || expression.result_parameters != type_parameters
                    || !expression.dependencies.is_empty()
                    || column_table_id(&column_key, next_column)? != *table_id
                {
                    return Err(CommitError::InvalidCatalog);
                }
                if let crate::authorization::Expr::Literal(value) = &expression.expression
                    && crate::tuple::encode_tuple(
                        &[crate::tuple::ColumnSchema {
                            scalar_type,
                            nullable: true,
                        }],
                        1,
                        std::slice::from_ref(value),
                    )
                    .is_err()
                {
                    return Err(CommitError::InvalidCatalog);
                }
                let expression_object_key = id_catalog_key(2, expression_id);
                let expression_object_write = catalog_writes
                    .iter()
                    .find(|write| write.key == expression_object_key)
                    .ok_or(CommitError::InvalidCatalog)?;
                let expression_object_bytes = expression_object_write
                    .payload
                    .as_deref()
                    .or(expression_object_write.expected_value.as_deref())
                    .ok_or(CommitError::InvalidCatalog)?;
                let expected_name = format!("default_{ordinal}");
                if catalog_field_u16(&expression_object_key, expression_object_bytes, 2, 1)? != 5
                    || catalog_field_bytes(&expression_object_key, expression_object_bytes, 2, 3)?
                        != expected_name.as_bytes()
                    || catalog_field_u64(&expression_object_key, expression_object_bytes, 2, 5)?
                        != 1
                    || catalog_field_u16(&expression_object_key, expression_object_bytes, 2, 6)?
                        != 1
                    || (adding
                        && catalog_field_bytes(
                            &expression_object_key,
                            expression_object_bytes,
                            2,
                            4,
                        )? != principal_id)
                {
                    return Err(CommitError::InvalidCatalog);
                }
            }
            for (expression_id, adding) in [
                old_default.map(|id| (id, false)),
                next_default.map(|id| (id, true)),
            ]
            .into_iter()
            .flatten()
            {
                if !default_expression_ids.insert(expression_id)
                    || expression_changes.get(&expression_id).is_none_or(
                        |(is_adding, _, purpose)| {
                            *is_adding != adding
                                || *purpose != crate::expression::ExpressionPurpose::Default
                        },
                    )
                {
                    return Err(CommitError::InvalidCatalog);
                }
                if expected_dependencies
                    .insert((*column_id, expression_id), (2, !adding, adding))
                    .is_some()
                {
                    return Err(CommitError::InvalidCatalog);
                }
            }
        }
    }
    if direct_column_add {
        let (column_id, (table_id, default_id, add_default_id)) = added_columns
            .iter()
            .next()
            .ok_or(CommitError::InvalidCatalog)?;
        if default_id != add_default_id {
            return Err(CommitError::InvalidCatalog);
        }
        affected_tables.insert(*table_id);
        let (old_schema_version, next_schema_version, old_epoch, next_epoch) = table_schema_changes
            .get(table_id)
            .copied()
            .ok_or(CommitError::InvalidCatalog)?;
        if old_schema_version.checked_add(1) != Some(next_schema_version)
            || old_epoch.checked_add(1) != Some(next_epoch)
        {
            return Err(CommitError::InvalidCatalog);
        }
        let old_schema = load_final_table_schema(&catalog, &[], *table_id)?;
        let next_schema = load_final_table_schema(&catalog, catalog_writes, *table_id)?;
        let table_key = id_catalog_key(5, *table_id);
        let table_bytes = catalog
            .get(table_key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        let schema_id: [u8; 16] = catalog_field_bytes(&table_key, &table_bytes, 5, 1)?
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
        if next_schema.schema_version != next_schema_version
            || next_schema.column_ids.len() != old_schema.column_ids.len() + 1
            || next_schema.column_ids[..old_schema.column_ids.len()] != old_schema.column_ids
            || next_schema.columns[..old_schema.columns.len()] != old_schema.columns
            || next_schema.column_ids.last() != Some(column_id)
        {
            return Err(CommitError::InvalidCatalog);
        }
        let column_key = id_catalog_key(6, *column_id);
        let column_write = catalog_writes
            .iter()
            .find(|write| write.key == column_key)
            .ok_or(CommitError::InvalidCatalog)?;
        if column_write.expected_value.is_some() {
            return Err(CommitError::InvalidCatalog);
        }
        let column_bytes = column_write
            .payload
            .as_deref()
            .ok_or(CommitError::InvalidCatalog)?;
        let ordinal = catalog_field_u32(&column_key, column_bytes, 6, 2)? as usize;
        let type_id: [u8; 16] = catalog_field_bytes(&column_key, column_bytes, 6, 4)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let type_parameters: [u8; 8] = catalog_field_bytes(&column_key, column_bytes, 6, 5)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let scalar_type = next_schema
            .columns
            .last()
            .ok_or(CommitError::InvalidCatalog)?
            .scalar_type;
        let nullable = next_schema
            .columns
            .last()
            .ok_or(CommitError::InvalidCatalog)?
            .nullable;
        if ordinal != old_schema.column_ids.len()
            || catalog_field_u16(&column_key, column_bytes, 6, 8)? != 0
            || catalog_field_u64(&column_key, column_bytes, 6, 9)? != next_schema_version
        {
            return Err(CommitError::InvalidCatalog);
        }
        if let Some(expression_id) = default_id {
            let expression_key = id_catalog_key(7, *expression_id);
            let expression_write = catalog_writes
                .iter()
                .find(|write| write.key == expression_key)
                .ok_or(CommitError::InvalidCatalog)?;
            let expression_bytes = expression_write
                .payload
                .as_deref()
                .ok_or(CommitError::InvalidCatalog)?;
            let expression = decode_catalog_expression_record(&decode_catalog_record(
                &expression_key,
                expression_bytes,
            )?)
            .map_err(|_| CommitError::InvalidCatalog)?;
            let type_key = id_catalog_key(4, type_id);
            let type_bytes = catalog
                .get(type_key.as_slice())
                .map_err(storage_error)?
                .map(|value| value.value().to_vec())
                .ok_or(CommitError::InvalidCatalog)?;
            let mask = catalog_field_u32(&type_key, &type_bytes, 4, 5)?;
            let crate::authorization::Expr::Literal(value) = &expression.expression else {
                return Err(CommitError::InvalidCatalog);
            };
            if mask & 1 == 0
                || expression.expression_id != *expression_id
                || expression.purpose != crate::expression::ExpressionPurpose::Default
                || expression.result_type_id != type_id
                || expression.result_parameters != type_parameters
                || !expression.dependencies.is_empty()
                || crate::tuple::encode_tuple(
                    &[crate::tuple::ColumnSchema {
                        scalar_type,
                        nullable,
                    }],
                    1,
                    std::slice::from_ref(value),
                )
                .is_err()
            {
                return Err(CommitError::InvalidCatalog);
            }
            if !default_expression_ids.insert(*expression_id) {
                return Err(CommitError::InvalidCatalog);
            }
            if expected_dependencies
                .insert((*column_id, *expression_id), (2, false, true))
                .is_some()
            {
                return Err(CommitError::InvalidCatalog);
            }
            let expression_object_key = id_catalog_key(2, *expression_id);
            let expression_object_write = catalog_writes
                .iter()
                .find(|write| write.key == expression_object_key)
                .ok_or(CommitError::InvalidCatalog)?;
            let expression_object_bytes = expression_object_write
                .payload
                .as_deref()
                .ok_or(CommitError::InvalidCatalog)?;
            if catalog_field_u16(&expression_object_key, expression_object_bytes, 2, 1)? != 5
                || catalog_field_bytes(&expression_object_key, expression_object_bytes, 2, 3)?
                    != format!("default_{ordinal}").as_bytes()
                || catalog_field_bytes(&expression_object_key, expression_object_bytes, 2, 4)?
                    != principal_id
                || catalog_field_u64(&expression_object_key, expression_object_bytes, 2, 5)? != 1
                || catalog_field_u16(&expression_object_key, expression_object_bytes, 2, 6)? != 1
            {
                return Err(CommitError::InvalidCatalog);
            }
        } else if !nullable {
            return Err(CommitError::InvalidCatalog);
        }
        let column_object_key = id_catalog_key(2, *column_id);
        let column_object_write = catalog_writes
            .iter()
            .find(|write| write.key == column_object_key)
            .ok_or(CommitError::InvalidCatalog)?;
        let column_object_bytes = column_object_write
            .payload
            .as_deref()
            .ok_or(CommitError::InvalidCatalog)?;
        let column_name = catalog_field_bytes(&column_key, column_bytes, 6, 3)?;
        if column_object_write.expected_value.is_some()
            || catalog_field_u16(&column_object_key, column_object_bytes, 2, 1)? != 4
            || catalog_field_bytes(&column_object_key, column_object_bytes, 2, 2)? != *table_id
            || catalog_field_bytes(&column_object_key, column_object_bytes, 2, 3)? != column_name
            || catalog_field_bytes(&column_object_key, column_object_bytes, 2, 4)? != principal_id
            || catalog_field_u64(&column_object_key, column_object_bytes, 2, 5)? != 1
            || catalog_field_u16(&column_object_key, column_object_bytes, 2, 6)? != 1
        {
            return Err(CommitError::InvalidCatalog);
        }
        expected_objects.insert(*column_id, (4, false, true));
        if expected_dependencies
            .insert((*column_id, *table_id), (1, false, true))
            .is_some()
        {
            return Err(CommitError::InvalidCatalog);
        }
        for (index_id, (index_table, old_version, new_version)) in &index_schema_updates {
            if *index_table != *table_id
                || *old_version != old_schema_version
                || *new_version != next_schema_version
            {
                return Err(CommitError::InvalidCatalog);
            }
            let index_key = id_catalog_key(9, *index_id);
            let index_write = catalog_writes
                .iter()
                .find(|write| write.key == index_key)
                .ok_or(CommitError::InvalidCatalog)?;
            let index_bytes = index_write
                .payload
                .as_deref()
                .ok_or(CommitError::InvalidCatalog)?;
            if catalog_field_bytes(&index_key, index_bytes, 9, 1)? != *table_id {
                return Err(CommitError::InvalidCatalog);
            }
        }
        let mut table_index_ids = HashSet::new();
        let index_prefix = 9u16.to_be_bytes();
        let mut scanned = 0usize;
        for entry in catalog
            .range(index_prefix.as_slice()..)
            .map_err(storage_error)?
        {
            let (key, value) = entry.map_err(storage_error)?;
            let key = key.value().to_vec();
            if !key.starts_with(&index_prefix) {
                break;
            }
            scanned += 1;
            if scanned > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
                return Err(CommitError::InvalidCatalog);
            }
            let bytes = value.value().to_vec();
            if catalog_field_bytes(&key, &bytes, 9, 1)? == *table_id {
                let index_id: [u8; 16] = key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                table_index_ids.insert(index_id);
            }
        }
        if table_index_ids
            != index_schema_updates
                .iter()
                .filter_map(|(index_id, (index_table, _, _))| {
                    (*index_table == *table_id).then_some(*index_id)
                })
                .collect::<HashSet<_>>()
        {
            return Err(CommitError::InvalidCatalog);
        }
    }
    let check_expression_count = expression_changes
        .values()
        .filter(|(_, _, purpose)| *purpose == crate::expression::ExpressionPurpose::Check)
        .count();
    if check_expression_ids.len() != check_expression_count
        || default_expression_ids.len()
            != expression_changes
                .len()
                .saturating_sub(check_expression_count)
    {
        return Err(CommitError::InvalidCatalog);
    }
    for (expression_id, (adding, dependencies, purpose)) in &expression_changes {
        if expected_objects
            .insert(*expression_id, (5, !adding, *adding))
            .is_some()
        {
            return Err(CommitError::InvalidCatalog);
        }
        if *purpose == crate::expression::ExpressionPurpose::Default
            && !default_expression_ids.contains(expression_id)
        {
            return Err(CommitError::InvalidCatalog);
        }
        for dependency_id in dependencies {
            expected_dependencies.insert((*expression_id, *dependency_id), (2, !adding, *adding));
        }
    }
    if expected_objects != object_changes
        || expected_columns != column_changes
        || expected_dependencies != dependency_changes
        || expected_index_definitions != index_definition_states
        || tombstone_changes
            != object_changes
                .iter()
                .filter_map(|(object_id, (kind, was_present, is_present))| {
                    (*was_present && !*is_present && matches!(kind, 5..=7)).then_some(*object_id)
                })
                .collect::<HashSet<_>>()
        || affected_tables.len() != table_changes.len()
        || table_changes
            .iter()
            .any(|table_id| !affected_tables.contains(table_id))
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
        let edge = decode_membership_record(&key, &bytes)?;
        if edge.member_id == [0; 16]
            || edge.role_id == [0; 16]
            || catalog_field_bytes(&key, &bytes, 11, 1)? == [0; 16]
            || catalog_field_u64(&key, &bytes, 11, 2)? == 0
        {
            return Err(CommitError::InvalidCatalog);
        }
        principal_ids.insert(edge.member_id);
        principal_ids.insert(edge.role_id);
        memberships.push(edge);
        add_catalog_guard(&mut guards, key, Some(bytes));
    }
    validate_membership_graph(&memberships).map_err(|_| CommitError::InvalidCatalog)?;
    let roles = resolve_role_closure(principal_id, &memberships)
        .map_err(|_| CommitError::InvalidCatalog)?;
    let mut authority_subjects = roles.clone();
    authority_subjects.insert(principal_id);
    for role_id in roles {
        let role_key = id_catalog_key(10, role_id);
        let role_bytes = catalog
            .get(role_key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        if decode_principal_kind(&role_key, &role_bytes)? != 2
            || catalog_field_bytes(&role_key, &role_bytes, 10, 3)? != [0]
            || principal_authority_epoch(&role_key, &role_bytes)? == 0
        {
            return Err(CommitError::InvalidCatalog);
        }
        add_catalog_guard(&mut guards, role_key, Some(role_bytes));
    }
    for id in principal_ids {
        let key = id_catalog_key(10, id);
        let Some(bytes) = catalog
            .get(key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
        else {
            return Err(CommitError::InvalidCatalog);
        };
        let kind = decode_principal_kind(&key, &bytes)?;
        if principal_authority_epoch(&key, &bytes)? == 0
            || (memberships.iter().any(|edge| edge.role_id == id) && kind != 2)
            || (kind == 2 && catalog_field_bytes(&key, &bytes, 10, 3)? != [0])
        {
            return Err(CommitError::InvalidCatalog);
        }
        add_catalog_guard(&mut guards, key, Some(bytes));
    }

    let mut grant_lookups = 0usize;
    for table_id in &affected_tables {
        let table_object_key = id_catalog_key(2, *table_id);
        let table_object = catalog
            .get(table_object_key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        if catalog_field_u16(&table_object_key, &table_object, 2, 1)? != 3 {
            return Err(CommitError::InvalidCatalog);
        }
        let owner_id: [u8; 16] = catalog_field_bytes(&table_object_key, &table_object, 2, 4)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let table_record_key = id_catalog_key(5, *table_id);
        let table_record = catalog
            .get(table_record_key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        let schema_id: [u8; 16] = catalog_field_bytes(&table_record_key, &table_record, 5, 1)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        if schema_id == [0; 16] {
            return Err(CommitError::InvalidCatalog);
        }
        let schema_object_key = id_catalog_key(2, schema_id);
        let schema_object = catalog
            .get(schema_object_key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        if catalog_field_u16(&schema_object_key, &schema_object, 2, 1)? != 1 {
            return Err(CommitError::InvalidCatalog);
        }
        let schema_record_key = id_catalog_key(3, schema_id);
        let schema_record = catalog
            .get(schema_record_key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        decode_catalog_record(&schema_record_key, &schema_record)?;
        add_catalog_guard(&mut guards, table_object_key, Some(table_object));
        add_catalog_guard(&mut guards, table_record_key, Some(table_record));
        add_catalog_guard(&mut guards, schema_object_key, Some(schema_object));
        add_catalog_guard(&mut guards, schema_record_key, Some(schema_record));

        let mut has_alter = authority_subjects.contains(&owner_id);
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

    let mut new_constraint_names = HashMap::<[u8; 16], HashSet<Vec<u8>>>::new();
    for (constraint_id, change) in &constraint_changes {
        if !matches!(change.kind, 2..=5) || !change.adding {
            continue;
        }
        let object_key = id_catalog_key(2, *constraint_id);
        let object_write = catalog_writes
            .iter()
            .find(|write| write.key == object_key)
            .ok_or(CommitError::InvalidCatalog)?;
        let object_bytes = object_write
            .payload
            .as_deref()
            .ok_or(CommitError::InvalidCatalog)?;
        let name = catalog_field_bytes(&object_key, object_bytes, 2, 3)?.to_vec();
        if !new_constraint_names
            .entry(change.table_id)
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
            if key.len() != 19 || key[..2] != constraint_prefix {
                break;
            }
            let existing_id: [u8; 16] = key[3..]
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
            let existing_key = id_catalog_key(8, existing_id);
            if catalog_field_bytes(&existing_key, value.value(), 8, 1)? != change.table_id {
                continue;
            }
            if constraint_changes
                .get(&existing_id)
                .is_some_and(|existing_change| !existing_change.adding)
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
