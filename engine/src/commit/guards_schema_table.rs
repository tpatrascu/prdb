use super::fenced_ddl::add_catalog_guard;
use super::*;
use crate::audit::AuditEvent;
use crate::authorization::{MAX_MEMBERSHIP_EDGES, resolve_role_closure};
use crate::catalog::{decode_builtin_scalar_type, decode_registered_envelope};
use crate::expression::{
    ExpressionPurpose, decode_catalog_expression_record, is_row_local_expression,
    validate_expression_binding,
};
use crate::metadata::EngineMetadata;
use crate::schema::{CATALOG, ENGINE_META};
use crate::tuple::ColumnSchema;
use redb::{Database, ReadableDatabase, WriteTransaction};
use std::collections::{HashMap, HashSet};

pub(super) fn table_create_ddl_authority_guards(
    database: &Database,
    principal_id: [u8; 16],
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<Vec<CatalogGuard>, CommitError> {
    const MAX_TABLE_CREATE_CATALOG_WRITES: usize =
        crate::tuple::MAX_COLUMNS * (crate::tuple::MAX_COLUMNS + 16) + 8;
    if principal_id == [0; 16] {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    if catalog_writes.len() < 6
        || catalog_writes.len() > MAX_TABLE_CREATE_CATALOG_WRITES
        || audit_events.is_empty()
        || audit_events.iter().any(|event| {
            event.actor_id != Some(principal_id)
                || event.action != crate::audit::AuditAction::Ddl
                || event.result != crate::audit::AuditResult::Succeeded
        })
    {
        return Err(if audit_events.is_empty() {
            CommitError::InvalidAudit
        } else {
            CommitError::InvalidCatalog
        });
    }
    let mut table_record_writes = HashMap::<[u8; 16], &CatalogWrite>::new();
    let mut object_writes = HashMap::<[u8; 16], &CatalogWrite>::new();
    let mut constraint_writes = HashMap::<[u8; 16], &CatalogWrite>::new();
    let mut index_writes = HashMap::<[u8; 16], &CatalogWrite>::new();
    let mut column_writes = Vec::<&CatalogWrite>::new();
    let mut expression_writes = HashMap::<[u8; 16], &CatalogWrite>::new();
    let mut dependencies = HashSet::<([u8; 16], [u8; 16], u16)>::new();
    for write in catalog_writes {
        if write.payload.is_none() {
            return Err(CommitError::InvalidCatalog);
        }
        let family = write
            .key
            .get(..2)
            .map(|bytes| u16::from_be_bytes([bytes[0], bytes[1]]))
            .ok_or(CommitError::InvalidCatalog)?;
        if family != 5 && write.expected_value.is_some() {
            return Err(CommitError::InvalidCatalog);
        }
        match family {
            2 if write.key.len() == 19 => {
                let id = write.key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                if object_writes.insert(id, write).is_some() {
                    return Err(CommitError::InvalidCatalog);
                }
            }
            5 if write.key.len() == 19 => {
                let id = write.key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                if table_record_writes.insert(id, write).is_some() {
                    return Err(CommitError::InvalidCatalog);
                }
            }
            6 if write.key.len() == 19 => column_writes.push(write),
            7 if write.key.len() == 19 => {
                let id = write.key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                if expression_writes.insert(id, write).is_some() {
                    return Err(CommitError::InvalidCatalog);
                }
            }
            8 if write.key.len() == 19 => {
                let id = write.key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                if constraint_writes.insert(id, write).is_some() {
                    return Err(CommitError::InvalidCatalog);
                }
            }
            9 if write.key.len() == 19 => {
                let id = write.key[3..19]
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
                if index_writes.insert(id, write).is_some() {
                    return Err(CommitError::InvalidCatalog);
                }
            }
            15 => {
                let bytes = write
                    .payload
                    .as_deref()
                    .ok_or(CommitError::InvalidCatalog)?;
                let (dependent, referenced, kind) = decode_dependency_edge(&write.key, bytes)?;
                if !matches!(kind, 1..=4) || !dependencies.insert((dependent, referenced, kind)) {
                    return Err(CommitError::InvalidCatalog);
                }
            }
            _ => return Err(CommitError::InvalidCatalog),
        }
    }
    if table_record_writes.is_empty() {
        return Err(CommitError::InvalidCatalog);
    }
    let table_id = table_record_writes
        .iter()
        .find_map(|(id, write)| write.expected_value.is_none().then_some(*id))
        .ok_or(CommitError::InvalidCatalog)?;
    if table_record_writes
        .values()
        .filter(|write| write.expected_value.is_none())
        .count()
        != 1
    {
        return Err(CommitError::InvalidCatalog);
    }
    let table_write = *table_record_writes
        .get(&table_id)
        .ok_or(CommitError::InvalidCatalog)?;
    if table_id == [0; 16]
        || column_writes.is_empty()
        || audit_events
            .iter()
            .any(|event| event.target_ids.as_slice() != [table_id])
    {
        return Err(CommitError::InvalidCatalog);
    }
    let table_key = id_catalog_key(5, table_id);
    if table_write.key != table_key {
        return Err(CommitError::InvalidCatalog);
    }
    let table_bytes = table_write
        .payload
        .as_deref()
        .ok_or(CommitError::InvalidCatalog)?;
    let schema_id: [u8; 16] = catalog_field_bytes(&table_key, table_bytes, 5, 1)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    if schema_id == [0; 16]
        || catalog_field_u64(&table_key, table_bytes, 5, 2)? != 1
        || catalog_field_u64(&table_key, table_bytes, 5, 3)? != 1
        || catalog_field_bytes(&table_key, table_bytes, 5, 4)? != table_id
        || catalog_field_u16(&table_key, table_bytes, 5, 5)? != 1
    {
        return Err(CommitError::InvalidCatalog);
    }
    let table_object_key = id_catalog_key(2, table_id);
    let table_object = object_writes
        .get(&table_id)
        .and_then(|write| write.payload.as_deref())
        .ok_or(CommitError::InvalidCatalog)?;
    if catalog_field_u16(&table_object_key, table_object, 2, 1)? != 3
        || catalog_field_bytes(&table_object_key, table_object, 2, 2)? != schema_id
        || catalog_field_bytes(&table_object_key, table_object, 2, 4)? != principal_id
        || catalog_field_u64(&table_object_key, table_object, 2, 5)? != 1
        || catalog_field_u16(&table_object_key, table_object, 2, 6)? != 1
    {
        return Err(CommitError::InvalidCatalog);
    }
    let table_name = catalog_field_bytes(&table_object_key, table_object, 2, 3)?;
    if !is_canonical_catalog_name(&table_name) {
        return Err(CommitError::InvalidCatalog);
    }

    let mut ordered_columns = column_writes
        .iter()
        .map(|write| {
            let bytes = write
                .payload
                .as_deref()
                .ok_or(CommitError::InvalidCatalog)?;
            Ok((catalog_field_u32(&write.key, bytes, 6, 2)?, *write))
        })
        .collect::<Result<Vec<_>, CommitError>>()?;
    ordered_columns.sort_by_key(|(ordinal, _)| *ordinal);
    let mut names = HashSet::<Vec<u8>>::new();
    let mut column_ids = HashSet::new();
    let mut ordered_column_ids = Vec::with_capacity(ordered_columns.len());
    let mut column_schemas = Vec::with_capacity(ordered_columns.len());
    let mut default_bindings = Vec::new();
    let read = database.begin_read().map_err(storage_error)?;
    let catalog = read.open_table(CATALOG).map_err(storage_error)?;
    let schema_object_key = id_catalog_key(2, schema_id);
    let schema_object = catalog
        .get(schema_object_key.as_slice())
        .map_err(storage_error)?
        .map(|value| value.value().to_vec())
        .ok_or(CommitError::InvalidCatalog)?;
    if catalog_field_u16(&schema_object_key, &schema_object, 2, 1)? != 1 {
        return Err(CommitError::InvalidCatalog);
    }
    let schema_owner_id: [u8; 16] = catalog_field_bytes(&schema_object_key, &schema_object, 2, 4)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let schema_record_key = id_catalog_key(3, schema_id);
    let schema_record = catalog
        .get(schema_record_key.as_slice())
        .map_err(storage_error)?
        .map(|value| value.value().to_vec())
        .ok_or(CommitError::InvalidCatalog)?;
    decode_catalog_record(&schema_record_key, &schema_record)?;
    let mut guards = HashMap::<Vec<u8>, Option<Vec<u8>>>::new();
    add_catalog_guard(&mut guards, schema_object_key, Some(schema_object));
    add_catalog_guard(&mut guards, schema_record_key, Some(schema_record));
    if !dependencies.contains(&(table_id, schema_id, 1)) {
        return Err(CommitError::InvalidCatalog);
    }
    for (expected_ordinal, (ordinal, column_write)) in ordered_columns.iter().enumerate() {
        let column_id: [u8; 16] = column_write.key[3..19]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        if column_id == [0; 16]
            || column_id == table_id
            || !column_ids.insert(column_id)
            || *ordinal as usize != expected_ordinal
        {
            return Err(CommitError::InvalidCatalog);
        }
        let column_bytes = column_write
            .payload
            .as_deref()
            .ok_or(CommitError::InvalidCatalog)?;
        if catalog_field_bytes(&column_write.key, column_bytes, 6, 1)? != table_id
            || catalog_field_u64(&column_write.key, column_bytes, 6, 9)? != 1
        {
            return Err(CommitError::InvalidCatalog);
        }
        let name = catalog_field_bytes(&column_write.key, column_bytes, 6, 3)?;
        if !is_canonical_catalog_name(&name) || !names.insert(name.clone()) {
            return Err(CommitError::InvalidCatalog);
        }
        let type_id: [u8; 16] = catalog_field_bytes(&column_write.key, column_bytes, 6, 4)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let type_key = id_catalog_key(4, type_id);
        let type_bytes = catalog
            .get(type_key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        let type_record = decode_catalog_record(&type_key, &type_bytes)?;
        let scalar_type = decode_builtin_scalar_type(
            &type_record,
            &catalog_field_bytes(&column_write.key, column_bytes, 6, 5)?,
        )
        .map_err(|_| CommitError::InvalidCatalog)?;
        add_catalog_guard(&mut guards, type_key.clone(), Some(type_bytes.clone()));
        let nullable = match catalog_field_bytes(&column_write.key, column_bytes, 6, 6)?.as_slice()
        {
            [0] => false,
            [1] => true,
            _ => return Err(CommitError::InvalidCatalog),
        };
        let generated_mode = catalog_field_u16(&column_write.key, column_bytes, 6, 8)?;
        let column_record = decode_catalog_record(&column_write.key, column_bytes)?;
        let default_expression_id = column_record
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
        if generated_mode > 1
            || (generated_mode == 1
                && (decode_builtin_scalar_type(
                    &type_record,
                    &catalog_field_bytes(&column_write.key, column_bytes, 6, 5)?,
                )
                .map_err(|_| CommitError::InvalidCatalog)?
                    != crate::tuple::ScalarType::Uuid
                    || nullable))
            || column_record.fields.iter().any(|field| field.tag == 10)
            || (generated_mode == 1 && default_expression_id.is_some())
        {
            return Err(CommitError::InvalidCatalog);
        }
        ordered_column_ids.push(column_id);
        column_schemas.push(ColumnSchema {
            scalar_type,
            nullable,
        });
        if let Some(expression_id) = default_expression_id {
            let expression_write = expression_writes
                .get(&expression_id)
                .ok_or(CommitError::InvalidCatalog)?;
            let expression_key = id_catalog_key(7, expression_id);
            let expression_bytes = expression_write
                .payload
                .as_deref()
                .ok_or(CommitError::InvalidCatalog)?;
            let expression = decode_catalog_expression_record(&decode_catalog_record(
                &expression_key,
                expression_bytes,
            )?)
            .map_err(|_| CommitError::InvalidCatalog)?;
            let default_mask = catalog_field_u32(&type_key, &type_bytes, 4, 5)?;
            let permitted = match &expression.expression {
                crate::authorization::Expr::Literal(_) => default_mask & 1 != 0,
                crate::authorization::Expr::CurrentPrincipalId => default_mask & 4 != 0,
                _ => false,
            };
            if !permitted
                || expression.purpose != ExpressionPurpose::Default
                || expression.expression_id != expression_id
                || expression.result_type_id != type_id
                || expression.result_parameters
                    != <[u8; 8]>::try_from(catalog_field_bytes(
                        &column_write.key,
                        column_bytes,
                        6,
                        5,
                    )?)
                    .map_err(|_| CommitError::InvalidCatalog)?
                || !expression.dependencies.is_empty()
                || !dependencies.contains(&(column_id, expression_id, 2))
            {
                return Err(CommitError::InvalidCatalog);
            }
            if let crate::authorization::Expr::Literal(value) = &expression.expression
                && crate::tuple::encode_tuple(
                    std::slice::from_ref(&column_schemas[expected_ordinal]),
                    1,
                    std::slice::from_ref(value),
                )
                .is_err()
            {
                return Err(CommitError::InvalidCatalog);
            }
            let expression_object_key = id_catalog_key(2, expression_id);
            let expression_object = object_writes
                .get(&expression_id)
                .and_then(|write| write.payload.as_deref())
                .ok_or(CommitError::InvalidCatalog)?;
            let expected_expression_name = format!("default_{expected_ordinal}");
            if catalog_field_u16(&expression_object_key, expression_object, 2, 1)? != 5
                || decode_registered_envelope(expression_object)
                    .map_err(|_| CommitError::InvalidCatalog)?
                    .fields
                    .iter()
                    .any(|field| field.tag == 2)
                || catalog_field_bytes(&expression_object_key, expression_object, 2, 3)?
                    != expected_expression_name.as_bytes()
                || catalog_field_bytes(&expression_object_key, expression_object, 2, 4)?
                    != principal_id
                || catalog_field_u64(&expression_object_key, expression_object, 2, 5)? != 1
                || catalog_field_u16(&expression_object_key, expression_object, 2, 6)? != 1
            {
                return Err(CommitError::InvalidCatalog);
            }
            default_bindings.push((
                expression_id,
                column_id,
                expected_ordinal,
                type_id,
                expression,
            ));
        }
        let object_key = id_catalog_key(2, column_id);
        let object_bytes = object_writes
            .get(&column_id)
            .and_then(|write| write.payload.as_deref())
            .ok_or(CommitError::InvalidCatalog)?;
        if catalog_field_u16(&object_key, object_bytes, 2, 1)? != 4
            || catalog_field_bytes(&object_key, object_bytes, 2, 2)? != table_id
            || catalog_field_bytes(&object_key, object_bytes, 2, 3)? != name
            || catalog_field_bytes(&object_key, object_bytes, 2, 4)? != principal_id
            || catalog_field_u64(&object_key, object_bytes, 2, 5)? != 1
            || catalog_field_u16(&object_key, object_bytes, 2, 6)? != 1
            || !dependencies.contains(&(column_id, table_id, 1))
        {
            return Err(CommitError::InvalidCatalog);
        }
    }
    for (_, _, ordinal, type_id, expression) in &default_bindings {
        validate_expression_binding(
            expression,
            &column_schemas,
            &ordered_column_ids,
            *type_id,
            column_schemas[*ordinal].scalar_type,
        )
        .map_err(|_| CommitError::InvalidCatalog)?;
    }
    let key_constraint_count = constraint_writes
        .values()
        .filter(|write| {
            write
                .payload
                .as_deref()
                .and_then(|bytes| catalog_field_u16(&write.key, bytes, 8, 2).ok())
                .is_some_and(|kind| matches!(kind, 2 | 3))
        })
        .count();
    let foreign_key_count = constraint_writes
        .values()
        .filter(|write| {
            write
                .payload
                .as_deref()
                .and_then(|bytes| catalog_field_u16(&write.key, bytes, 8, 2).ok())
                == Some(5)
        })
        .count();
    let check_constraint_count = constraint_writes.len() - key_constraint_count - foreign_key_count;
    if key_constraint_count != index_writes.len() {
        return Err(CommitError::InvalidCatalog);
    }
    let mut referenced_index_ids = HashSet::with_capacity(key_constraint_count);
    let mut referenced_table_ids = HashSet::new();
    let mut validated_foreign_key_ids = HashSet::new();
    let mut primary_key_count = 0usize;
    for (&constraint_id, constraint_write) in &constraint_writes {
        let constraint_key = id_catalog_key(8, constraint_id);
        let constraint_bytes = constraint_write
            .payload
            .as_deref()
            .ok_or(CommitError::InvalidCatalog)?;
        validate_constraint_shape(&constraint_key, constraint_bytes)?;
        let kind = catalog_field_u16(&constraint_key, constraint_bytes, 8, 2)?;
        if kind == 4 {
            continue;
        }
        if kind == 5 {
            if constraint_table_id(&constraint_key, constraint_bytes)? != table_id {
                return Err(CommitError::InvalidCatalog);
            }
            let source_columns = catalog_id_list_field(&constraint_key, constraint_bytes, 8, 3)?;
            let target_constraint_id: [u8; 16] =
                catalog_field_bytes(&constraint_key, constraint_bytes, 8, 5)?
                    .try_into()
                    .map_err(|_| CommitError::InvalidCatalog)?;
            let target_columns = catalog_id_list_field(&constraint_key, constraint_bytes, 8, 6)?;
            if source_columns.is_empty()
                || source_columns.len() != target_columns.len()
                || source_columns.iter().any(|id| !column_ids.contains(id))
                || target_columns.contains(&[0; 16])
                || target_columns.iter().collect::<HashSet<_>>().len() != target_columns.len()
            {
                return Err(CommitError::InvalidCatalog);
            }
            let target_constraint_key = id_catalog_key(8, target_constraint_id);
            let local_target_constraint = constraint_writes
                .get(&target_constraint_id)
                .and_then(|write| write.payload.as_deref());
            let is_local_target = local_target_constraint.is_some();
            let target_constraint_bytes = if let Some(bytes) = local_target_constraint {
                bytes.to_vec()
            } else {
                catalog
                    .get(target_constraint_key.as_slice())
                    .map_err(storage_error)?
                    .map(|value| value.value().to_vec())
                    .ok_or(CommitError::InvalidCatalog)?
            };
            let target_kind =
                catalog_field_u16(&target_constraint_key, &target_constraint_bytes, 8, 2)?;
            let target_table_id =
                constraint_table_id(&target_constraint_key, &target_constraint_bytes)?;
            let primary_target_columns =
                catalog_id_list_field(&target_constraint_key, &target_constraint_bytes, 8, 3)?;
            if !matches!(target_kind, 2 | 3)
                || target_table_id == [0; 16]
                || (target_table_id == table_id) != is_local_target
                || primary_target_columns != target_columns
            {
                return Err(CommitError::InvalidCatalog);
            }
            if !is_local_target {
                referenced_table_ids.insert(target_table_id);
            }
            let source_positions = source_columns
                .iter()
                .map(|source_id| {
                    ordered_column_ids
                        .iter()
                        .position(|column_id| column_id == source_id)
                        .ok_or(CommitError::InvalidCatalog)
                })
                .collect::<Result<Vec<_>, _>>()?;
            for (source_position, target_column_id) in source_positions.iter().zip(&target_columns)
            {
                if is_local_target {
                    let target_position = ordered_column_ids
                        .iter()
                        .position(|column_id| column_id == target_column_id)
                        .ok_or(CommitError::InvalidCatalog)?;
                    if column_schemas[*source_position].scalar_type
                        != column_schemas[target_position].scalar_type
                    {
                        return Err(CommitError::InvalidCatalog);
                    }
                    continue;
                }
                let target_column_key = id_catalog_key(6, *target_column_id);
                let target_column_bytes = catalog
                    .get(target_column_key.as_slice())
                    .map_err(storage_error)?
                    .map(|value| value.value().to_vec())
                    .ok_or(CommitError::InvalidCatalog)?;
                if column_table_id(&target_column_key, &target_column_bytes)? != target_table_id {
                    return Err(CommitError::InvalidCatalog);
                }
                let target_type_id =
                    catalog_field_bytes(&target_column_key, &target_column_bytes, 6, 4)?
                        .as_slice()
                        .try_into()
                        .map_err(|_| CommitError::InvalidCatalog)?;
                let target_type_key = id_catalog_key(4, target_type_id);
                let target_type_bytes = catalog
                    .get(target_type_key.as_slice())
                    .map_err(storage_error)?
                    .map(|value| value.value().to_vec())
                    .ok_or(CommitError::InvalidCatalog)?;
                let target_type_record =
                    decode_catalog_record(&target_type_key, &target_type_bytes)?;
                let target_parameters =
                    catalog_field_bytes(&target_column_key, &target_column_bytes, 6, 5)?;
                let target_scalar_type =
                    decode_builtin_scalar_type(&target_type_record, &target_parameters)
                        .map_err(|_| CommitError::InvalidCatalog)?;
                if column_schemas[*source_position].scalar_type != target_scalar_type {
                    return Err(CommitError::InvalidCatalog);
                }
                add_catalog_guard(&mut guards, target_column_key, Some(target_column_bytes));
                add_catalog_guard(&mut guards, target_type_key, Some(target_type_bytes));
            }
            if !is_local_target {
                add_catalog_guard(
                    &mut guards,
                    target_constraint_key.clone(),
                    Some(target_constraint_bytes),
                );
            }
            let target_object_key = id_catalog_key(2, target_constraint_id);
            let target_object_bytes = if is_local_target {
                object_writes
                    .get(&target_constraint_id)
                    .and_then(|write| write.payload.as_deref())
                    .map(ToOwned::to_owned)
                    .ok_or(CommitError::InvalidCatalog)?
            } else {
                catalog
                    .get(target_object_key.as_slice())
                    .map_err(storage_error)?
                    .map(|value| value.value().to_vec())
                    .ok_or(CommitError::InvalidCatalog)?
            };
            if catalog_field_u16(&target_object_key, &target_object_bytes, 2, 1)? != 6 {
                return Err(CommitError::InvalidCatalog);
            }
            if !is_local_target {
                add_catalog_guard(&mut guards, target_object_key, Some(target_object_bytes));
            }
            let foreign_key_object_key = id_catalog_key(2, constraint_id);
            let foreign_key_object = object_writes
                .get(&constraint_id)
                .and_then(|write| write.payload.as_deref())
                .ok_or(CommitError::InvalidCatalog)?;
            if catalog_field_u16(&foreign_key_object_key, foreign_key_object, 2, 1)? != 6
                || !is_normalized_constraint_name(&catalog_field_bytes(
                    &foreign_key_object_key,
                    foreign_key_object,
                    2,
                    3,
                )?)
                || catalog_field_bytes(&foreign_key_object_key, foreign_key_object, 2, 4)?
                    != principal_id
                || catalog_field_u64(&foreign_key_object_key, foreign_key_object, 2, 5)? != 1
                || catalog_field_u16(&foreign_key_object_key, foreign_key_object, 2, 6)? != 1
                || !dependencies.contains(&(constraint_id, target_constraint_id, 3))
                || !validated_foreign_key_ids.insert(constraint_id)
            {
                return Err(CommitError::InvalidCatalog);
            }
            continue;
        }
        if !matches!(kind, 2 | 3)
            || constraint_table_id(&constraint_key, constraint_bytes)? != table_id
        {
            return Err(CommitError::InvalidCatalog);
        }
        if kind == 2 {
            primary_key_count += 1;
            if primary_key_count > 1 {
                return Err(CommitError::InvalidCatalog);
            }
        }
        let key_columns = catalog_id_list_field(&constraint_key, constraint_bytes, 8, 3)?;
        if key_columns.is_empty()
            || key_columns
                .iter()
                .any(|column_id| !column_ids.contains(column_id))
        {
            return Err(CommitError::InvalidCatalog);
        }
        if kind == 2 {
            for column_id in &key_columns {
                let column_key = id_catalog_key(6, *column_id);
                let column_write = column_writes
                    .iter()
                    .find(|write| write.key == column_key)
                    .ok_or(CommitError::InvalidCatalog)?;
                let column_bytes = column_write
                    .payload
                    .as_deref()
                    .ok_or(CommitError::InvalidCatalog)?;
                if catalog_field_bytes(&column_key, column_bytes, 6, 6)? != [0] {
                    return Err(CommitError::InvalidCatalog);
                }
            }
        }
        let index_id: [u8; 16] = catalog_field_bytes(&constraint_key, constraint_bytes, 8, 11)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        if !referenced_index_ids.insert(index_id) {
            return Err(CommitError::InvalidCatalog);
        }
        let index_write = index_writes
            .get(&index_id)
            .ok_or(CommitError::InvalidCatalog)?;
        let index_key = id_catalog_key(9, index_id);
        let index_bytes = index_write
            .payload
            .as_deref()
            .ok_or(CommitError::InvalidCatalog)?;
        let (decoded_index_id, decoded_table_id) =
            decode_index_definition(&index_key, index_bytes)?;
        let index_object_key = id_catalog_key(2, index_id);
        let constraint_object_key = id_catalog_key(2, constraint_id);
        let (constraint_prefix, index_prefix) = if kind == 2 {
            ("primary_key_constraint", "primary_key_index")
        } else {
            ("unique_constraint", "unique_index")
        };
        let object_name = |prefix: &str, id: &[u8; 16]| {
            format!(
                "{prefix}_{}",
                id.iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            )
        };
        let expected_index_name = object_name(index_prefix, &index_id);
        let expected_constraint_name = object_name(constraint_prefix, &constraint_id);
        let index_object = object_writes
            .get(&index_id)
            .and_then(|write| write.payload.as_deref())
            .ok_or(CommitError::InvalidCatalog)?;
        let constraint_object = object_writes
            .get(&constraint_id)
            .and_then(|write| write.payload.as_deref())
            .ok_or(CommitError::InvalidCatalog)?;
        if decoded_index_id != index_id
            || decoded_table_id != table_id
            || catalog_id_list_field(&index_key, index_bytes, 9, 2)? != key_columns
            || catalog_field_u16(&index_key, index_bytes, 9, 3)? != 1
            || catalog_field_bytes(&index_key, index_bytes, 9, 4)? != [1]
            || catalog_field_u16(&index_key, index_bytes, 9, 5)? != 1
            || catalog_field_bytes(&index_key, index_bytes, 9, 6)? != constraint_id
            || catalog_field_bytes(&index_key, index_bytes, 9, 7)? != index_id
            || catalog_field_u16(&index_key, index_bytes, 9, 8)? != 1
            || catalog_field_u32(&index_key, index_bytes, 9, 9)? != 1
            || catalog_field_u64(&index_key, index_bytes, 9, 10)? != 1
            || catalog_field_u16(&constraint_object_key, constraint_object, 2, 1)? != 6
            || {
                let constraint_name =
                    catalog_field_bytes(&constraint_object_key, constraint_object, 2, 3)?;
                constraint_name != expected_constraint_name.as_bytes()
                    && !is_normalized_constraint_name(&constraint_name)
            }
            || catalog_field_bytes(&constraint_object_key, constraint_object, 2, 4)? != principal_id
            || catalog_field_u64(&constraint_object_key, constraint_object, 2, 5)? != 1
            || catalog_field_u16(&constraint_object_key, constraint_object, 2, 6)? != 1
            || catalog_field_u16(&index_object_key, index_object, 2, 1)? != 7
            || catalog_field_bytes(&index_object_key, index_object, 2, 3)?
                != expected_index_name.as_bytes()
            || catalog_field_bytes(&index_object_key, index_object, 2, 4)? != principal_id
            || catalog_field_u64(&index_object_key, index_object, 2, 5)? != 1
            || catalog_field_u16(&index_object_key, index_object, 2, 6)? != 1
            || !dependencies.contains(&(constraint_id, index_id, 4))
        {
            return Err(CommitError::InvalidCatalog);
        }
    }
    if referenced_index_ids.len() != index_writes.len()
        || index_writes
            .keys()
            .any(|index_id| !referenced_index_ids.contains(index_id))
    {
        return Err(CommitError::InvalidCatalog);
    }
    let mut check_bindings = Vec::with_capacity(check_constraint_count);
    for (&constraint_id, constraint_write) in &constraint_writes {
        let constraint_key = id_catalog_key(8, constraint_id);
        let constraint_bytes = constraint_write
            .payload
            .as_deref()
            .ok_or(CommitError::InvalidCatalog)?;
        let kind = catalog_field_u16(&constraint_key, constraint_bytes, 8, 2)?;
        if kind != 4 {
            continue;
        }
        validate_constraint_shape(&constraint_key, constraint_bytes)?;
        if constraint_table_id(&constraint_key, constraint_bytes)? != table_id
            || !catalog_id_list_field(&constraint_key, constraint_bytes, 8, 3)?.is_empty()
            || !catalog_id_list_field(&constraint_key, constraint_bytes, 8, 6)?.is_empty()
            || catalog_field_u16(&constraint_key, constraint_bytes, 8, 7)? != 0
            || catalog_field_u16(&constraint_key, constraint_bytes, 8, 8)? != 0
            || catalog_field_u16(&constraint_key, constraint_bytes, 8, 9)? != 0
            || catalog_field_u16(&constraint_key, constraint_bytes, 8, 10)? != 1
        {
            return Err(CommitError::InvalidCatalog);
        }
        let expression_id: [u8; 16] = catalog_field_bytes(&constraint_key, constraint_bytes, 8, 4)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        let expression_write = expression_writes
            .get(&expression_id)
            .ok_or(CommitError::InvalidCatalog)?;
        let expression_key = id_catalog_key(7, expression_id);
        let expression_bytes = expression_write
            .payload
            .as_deref()
            .ok_or(CommitError::InvalidCatalog)?;
        let expression = decode_catalog_expression_record(&decode_catalog_record(
            &expression_key,
            expression_bytes,
        )?)
        .map_err(|_| CommitError::InvalidCatalog)?;
        let result_type_key = id_catalog_key(4, expression.result_type_id);
        let result_type_bytes = catalog
            .get(result_type_key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        let result_type_record = decode_catalog_record(&result_type_key, &result_type_bytes)?;
        add_catalog_guard(
            &mut guards,
            result_type_key.clone(),
            Some(result_type_bytes),
        );
        if expression.expression_id != expression_id
            || expression.purpose != ExpressionPurpose::Check
            || expression.result_parameters != [0; 8]
            || decode_builtin_scalar_type(&result_type_record, &expression.result_parameters)
                .map_err(|_| CommitError::InvalidCatalog)?
                != crate::tuple::ScalarType::Boolean
            || !is_row_local_expression(&expression.expression)
            || !dependencies.contains(&(constraint_id, expression_id, 2))
        {
            return Err(CommitError::InvalidCatalog);
        }
        let dependency_ids = expression
            .dependencies
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        if dependency_ids.len() != expression.dependencies.len()
            || dependency_ids.iter().any(|column_id| {
                !column_ids.contains(column_id)
                    || !dependencies.contains(&(expression_id, *column_id, 2))
            })
            || dependencies
                .iter()
                .any(|(dependent, referenced, edge_kind)| {
                    *dependent == expression_id
                        && *edge_kind == 2
                        && !dependency_ids.contains(referenced)
                })
        {
            return Err(CommitError::InvalidCatalog);
        }
        validate_expression_binding(
            &expression,
            &column_schemas,
            &ordered_column_ids,
            expression.result_type_id,
            crate::tuple::ScalarType::Boolean,
        )
        .map_err(|_| CommitError::InvalidCatalog)?;
        let constraint_object_key = id_catalog_key(2, constraint_id);
        let expression_object_key = id_catalog_key(2, expression_id);
        let constraint_object = object_writes
            .get(&constraint_id)
            .and_then(|write| write.payload.as_deref())
            .ok_or(CommitError::InvalidCatalog)?;
        let expression_object = object_writes
            .get(&expression_id)
            .and_then(|write| write.payload.as_deref())
            .ok_or(CommitError::InvalidCatalog)?;
        for (key, bytes, object_id, kind, prefix) in [
            (
                &constraint_object_key,
                constraint_object,
                constraint_id,
                6,
                "check_constraint",
            ),
            (
                &expression_object_key,
                expression_object,
                expression_id,
                5,
                "check_expression",
            ),
        ] {
            let expected_name = format!(
                "{prefix}_{}",
                object_id
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            );
            let actual_name = catalog_field_bytes(key, bytes, 2, 3)?;
            let named_check = kind == 6
                && actual_name.len() <= 128
                && !actual_name.is_empty()
                && (actual_name[0].is_ascii_lowercase() || actual_name[0] == b'_')
                && actual_name.iter().all(|byte| {
                    byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_'
                });
            if catalog_field_u16(key, bytes, 2, 1)? != kind
                || (actual_name != expected_name.as_bytes() && !named_check)
                || catalog_field_bytes(key, bytes, 2, 4)? != principal_id
                || catalog_field_u64(key, bytes, 2, 5)? != 1
                || catalog_field_u16(key, bytes, 2, 6)? != 1
            {
                return Err(CommitError::InvalidCatalog);
            }
        }
        check_bindings.push((constraint_id, expression_id, dependency_ids));
    }
    if object_writes.len()
        != column_ids.len()
            + default_bindings.len()
            + 1
            + 2 * key_constraint_count
            + foreign_key_count
            + 2 * check_bindings.len()
        || expression_writes.len() != default_bindings.len() + check_bindings.len()
        || dependencies.len()
            != column_ids.len()
                + 1
                + default_bindings.len()
                + key_constraint_count
                + foreign_key_count
                + check_bindings
                    .iter()
                    .map(|(_, _, dependencies)| dependencies.len() + 1)
                    .sum::<usize>()
        || default_bindings
            .iter()
            .any(|(expression_id, column_id, _, _, _)| {
                !dependencies.contains(&(*column_id, *expression_id, 2))
            })
    {
        return Err(CommitError::InvalidCatalog);
    }
    if validated_foreign_key_ids.len() != foreign_key_count
        || referenced_table_ids.len() != table_record_writes.len() - 1
    {
        return Err(CommitError::InvalidCatalog);
    }
    for (target_table_id, target_write) in &table_record_writes {
        if *target_table_id == table_id {
            if target_write.expected_value.is_some() {
                return Err(CommitError::InvalidCatalog);
            }
            continue;
        }
        let target_key = id_catalog_key(5, *target_table_id);
        let current = catalog
            .get(target_key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        let next = target_write
            .payload
            .as_deref()
            .ok_or(CommitError::InvalidCatalog)?;
        if !referenced_table_ids.contains(target_table_id)
            || target_write.expected_value.as_deref() != Some(current.as_slice())
            || !catalog_fields_equal_except(
                &decode_catalog_record(&target_key, &current)?,
                &decode_catalog_record(&target_key, next)?,
                &[3],
            )
            || table_write_epoch(&target_key, next)?
                != table_write_epoch(&target_key, &current)?
                    .checked_add(1)
                    .ok_or(CommitError::InvalidCatalog)?
        {
            return Err(CommitError::InvalidCatalog);
        }
        add_catalog_guard(&mut guards, target_key, Some(current));
    }

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
    add_catalog_guard(&mut guards, actor_key, Some(actor_bytes));
    let mut memberships = Vec::new();
    let mut subject_ids = HashSet::from([principal_id]);
    let membership_prefix = 11u16.to_be_bytes();
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
        if membership_count > MAX_MEMBERSHIP_EDGES {
            return Err(CommitError::InvalidCatalog);
        }
        let bytes = value.value().to_vec();
        let edge = decode_membership_record(&key, &bytes)?;
        subject_ids.insert(edge.member_id);
        subject_ids.insert(edge.role_id);
        memberships.push(edge);
        add_catalog_guard(&mut guards, key, Some(bytes));
    }
    let roles = resolve_role_closure(principal_id, &memberships)
        .map_err(|_| CommitError::InvalidCatalog)?;
    subject_ids.extend(roles.iter().copied());
    subject_ids.insert(schema_owner_id);
    let role_ids = memberships
        .iter()
        .map(|edge| edge.role_id)
        .collect::<HashSet<_>>();
    for subject in &subject_ids {
        let key = id_catalog_key(10, *subject);
        let bytes = catalog
            .get(key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        let kind = decode_principal_kind(&key, &bytes)?;
        if (role_ids.contains(subject) && kind != 2)
            || (*subject == schema_owner_id
                && kind == 2
                && catalog_field_bytes(&key, &bytes, 10, 3)? != [0])
            || principal_authority_epoch(&key, &bytes)? == 0
        {
            return Err(CommitError::InvalidCatalog);
        }
        add_catalog_guard(&mut guards, key, Some(bytes));
    }
    let mut admin_role_id = [0x80; 16];
    admin_role_id[15] = 3;
    let is_admin = roles.contains(&admin_role_id);
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
    let database_record = catalog
        .get(database_key.as_slice())
        .map_err(storage_error)?
        .map(|value| value.value().to_vec())
        .ok_or(CommitError::InvalidCatalog)?;
    let builtin_descriptor = catalog_field_bytes(&database_key, &database_record, 1, 3)?;
    if builtin_descriptor.len() < 4 {
        return Err(CommitError::InvalidCatalog);
    }
    let builtin_count = u32::from_le_bytes(
        builtin_descriptor[..4]
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?,
    ) as usize;
    if builtin_count > 256 || builtin_descriptor.len() != 4 + builtin_count * 20 {
        return Err(CommitError::InvalidCatalog);
    }
    for entry in builtin_descriptor[4..].chunks_exact(20) {
        let code = u16::from_le_bytes([entry[0], entry[1]]);
        if matches!(code, 1 | 2) && entry[4..20] == schema_id {
            return Err(CommitError::DdlAuthorizationDenied);
        }
    }
    add_catalog_guard(&mut guards, database_key, Some(database_record));
    let mut authorized =
        is_admin || principal_id == schema_owner_id || roles.contains(&schema_owner_id);
    for subject in &subject_ids {
        let key = grant_key(*subject, schema_id, 7, 1, schema_id);
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
    let mut reference_subjects = roles.clone();
    reference_subjects.insert(principal_id);
    let mut reference_lookups = 0usize;
    for (constraint_id, constraint_write) in &constraint_writes {
        let constraint_bytes = constraint_write
            .payload
            .as_deref()
            .ok_or(CommitError::InvalidCatalog)?;
        if catalog_field_u16(&constraint_write.key, constraint_bytes, 8, 2)? != 5 {
            continue;
        }
        let target_constraint_id: [u8; 16] =
            catalog_field_bytes(&constraint_write.key, constraint_bytes, 8, 5)?
                .as_slice()
                .try_into()
                .map_err(|_| CommitError::InvalidCatalog)?;
        if constraint_writes.contains_key(&target_constraint_id) {
            // A self-reference targets a key declared by this same atomic
            // CREATE TABLE. The table creator's DDL authority covers its own
            // key definition; there is no pre-existing grant to consult.
            continue;
        }
        let target_constraint_key = id_catalog_key(8, target_constraint_id);
        let target_constraint_bytes = catalog
            .get(target_constraint_key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        let target_table_id =
            constraint_table_id(&target_constraint_key, &target_constraint_bytes)?;
        let target_column_ids =
            catalog_id_list_field(&constraint_write.key, constraint_bytes, 8, 6)?;
        add_catalog_guard(
            &mut guards,
            target_constraint_key,
            Some(target_constraint_bytes),
        );
        let mut has_table_references = false;
        for subject in &reference_subjects {
            reference_lookups += 1;
            if reference_lookups > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                return Err(CommitError::InvalidCatalog);
            }
            let key = grant_key(*subject, target_table_id, 6, 1, target_table_id);
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
                has_table_references = true;
            }
            add_catalog_guard(&mut guards, key, bytes);
        }
        if !has_table_references {
            for column_id in target_column_ids {
                let mut has_column_references = false;
                for subject in &reference_subjects {
                    reference_lookups += 1;
                    if reference_lookups > crate::authorization::MAX_AUTHORIZATION_INPUTS {
                        return Err(CommitError::InvalidCatalog);
                    }
                    let key = grant_key(*subject, target_table_id, 6, 2, column_id);
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
                        has_column_references = true;
                    }
                    add_catalog_guard(&mut guards, key, bytes);
                }
                if !has_column_references {
                    return Err(CommitError::DdlAuthorizationDenied);
                }
            }
        }
        let target_object_key = id_catalog_key(2, target_constraint_id);
        let target_object_bytes = catalog
            .get(target_object_key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        if catalog_field_u16(&target_object_key, &target_object_bytes, 2, 1)? != 6 {
            return Err(CommitError::InvalidCatalog);
        }
        add_catalog_guard(&mut guards, target_object_key, Some(target_object_bytes));
        let _ = constraint_id;
    }
    if !authorized {
        return Err(CommitError::DdlAuthorizationDenied);
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

pub(super) fn validate_schema_drop_empty(
    transaction: &WriteTransaction,
    schema_id: [u8; 16],
) -> Result<(), CommitError> {
    let catalog = transaction.open_table(CATALOG).map_err(storage_error)?;
    validate_schema_drop_empty_snapshot(&catalog, schema_id)
}

pub(super) fn schema_create_ddl_authority_guards(
    database: &Database,
    principal_id: [u8; 16],
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<Vec<CatalogGuard>, CommitError> {
    if principal_id == [0; 16] {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    if catalog_writes.len() != 2
        || audit_events.is_empty()
        || audit_events.iter().any(|event| {
            event.actor_id != Some(principal_id)
                || event.action != crate::audit::AuditAction::Ddl
                || event.result != crate::audit::AuditResult::Succeeded
        })
    {
        return Err(if audit_events.is_empty() {
            CommitError::InvalidAudit
        } else {
            CommitError::InvalidCatalog
        });
    }
    let mut schema_id = None;
    let mut object_write = None;
    let mut schema_write = None;
    for write in catalog_writes {
        let id = write
            .key
            .get(3..19)
            .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
            .ok_or(CommitError::InvalidCatalog)?;
        if id == [0; 16] || write.expected_value.is_some() || write.payload.is_none() {
            return Err(CommitError::InvalidCatalog);
        }
        if schema_id.replace(id).is_some_and(|previous| previous != id) {
            return Err(CommitError::InvalidCatalog);
        }
        match write
            .key
            .get(..2)
            .map(|bytes| u16::from_be_bytes([bytes[0], bytes[1]]))
        {
            Some(2) if write.key.len() == 19 && object_write.replace(write).is_none() => {}
            Some(3) if write.key.len() == 19 && schema_write.replace(write).is_none() => {}
            _ => return Err(CommitError::InvalidCatalog),
        }
    }
    let schema_id = schema_id.ok_or(CommitError::InvalidCatalog)?;
    let object_write = object_write.ok_or(CommitError::InvalidCatalog)?;
    let schema_write = schema_write.ok_or(CommitError::InvalidCatalog)?;
    let object_key = id_catalog_key(2, schema_id);
    let schema_key = id_catalog_key(3, schema_id);
    if object_write.key != object_key || schema_write.key != schema_key {
        return Err(CommitError::InvalidCatalog);
    }
    if audit_events
        .iter()
        .any(|event| event.target_ids.as_slice() != [schema_id])
    {
        return Err(CommitError::InvalidAudit);
    }
    let object_bytes = object_write
        .payload
        .as_deref()
        .ok_or(CommitError::InvalidCatalog)?;
    decode_catalog_record(&object_key, object_bytes)?;
    if catalog_field_u16(&object_key, object_bytes, 2, 1)? != 1 {
        return Err(CommitError::InvalidCatalog);
    }
    let object_record =
        decode_registered_envelope(object_bytes).map_err(|_| CommitError::InvalidCatalog)?;
    if object_record.fields.iter().any(|field| field.tag == 2)
        || catalog_field_u64(&object_key, object_bytes, 2, 5)? != 1
        || catalog_field_u16(&object_key, object_bytes, 2, 6)? != 1
    {
        return Err(CommitError::InvalidCatalog);
    }
    let owner_id: [u8; 16] = catalog_field_bytes(&object_key, object_bytes, 2, 4)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    if owner_id == [0; 16] {
        return Err(CommitError::InvalidCatalog);
    }
    let name = catalog_field_bytes(&object_key, object_bytes, 2, 3)?;
    if name.is_empty()
        || name.len() > 128
        || !(name[0].is_ascii_lowercase() || name[0] == b'_')
        || !name
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
    {
        return Err(CommitError::InvalidCatalog);
    }
    let schema_bytes = schema_write
        .payload
        .as_deref()
        .ok_or(CommitError::InvalidCatalog)?;
    let schema_record = decode_catalog_record(&schema_key, schema_bytes)?;
    if schema_record.family_code != 3
        || schema_record.fields.len() != 1
        || catalog_field_bytes(&schema_key, schema_bytes, 3, 1)? != 0u32.to_le_bytes()
    {
        return Err(CommitError::InvalidCatalog);
    }

    let read = database.begin_read().map_err(storage_error)?;
    let catalog = read.open_table(CATALOG).map_err(storage_error)?;
    let object_prefix = 2u16.to_be_bytes();
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
        if scanned > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
            return Err(CommitError::InvalidCatalog);
        }
        if key.len() == 19
            && catalog_field_u16(key, value.value(), 2, 1)? == 1
            && catalog_field_bytes(key, value.value(), 2, 3)? == name
        {
            return Err(CommitError::InvalidCatalog);
        }
    }
    for (family, id) in [(2, schema_id), (3, schema_id), (17, schema_id)] {
        let key = id_catalog_key(family, id);
        if catalog
            .get(key.as_slice())
            .map_err(storage_error)?
            .is_some()
        {
            return Err(CommitError::InvalidCatalog);
        }
    }
    let owner_key = id_catalog_key(10, owner_id);
    let owner_bytes = catalog
        .get(owner_key.as_slice())
        .map_err(storage_error)?
        .map(|value| value.value().to_vec())
        .ok_or(CommitError::InvalidCatalog)?;
    decode_principal_kind(&owner_key, &owner_bytes)?;

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
    let mut guards = HashMap::<Vec<u8>, Option<Vec<u8>>>::new();
    add_catalog_guard(&mut guards, owner_key, Some(owner_bytes));
    let membership_prefix = 11u16.to_be_bytes();
    let mut memberships = Vec::new();
    let mut membership_count = 0usize;
    let mut subject_ids = HashSet::from([principal_id]);
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
        if membership_count > MAX_MEMBERSHIP_EDGES {
            return Err(CommitError::InvalidCatalog);
        }
        let bytes = value.value().to_vec();
        let edge = decode_membership_record(&key, &bytes)?;
        subject_ids.insert(edge.member_id);
        subject_ids.insert(edge.role_id);
        memberships.push(edge);
        add_catalog_guard(&mut guards, key, Some(bytes));
    }
    let roles = resolve_role_closure(principal_id, &memberships)
        .map_err(|_| CommitError::InvalidCatalog)?;
    subject_ids.extend(roles.iter().copied());
    let role_ids = memberships
        .iter()
        .map(|edge| edge.role_id)
        .collect::<HashSet<_>>();
    for subject in &subject_ids {
        let key = id_catalog_key(10, *subject);
        let bytes = if *subject == principal_id {
            actor_bytes.clone()
        } else {
            catalog
                .get(key.as_slice())
                .map_err(storage_error)?
                .map(|value| value.value().to_vec())
                .ok_or(CommitError::InvalidCatalog)?
        };
        let kind = decode_principal_kind(&key, &bytes)?;
        if (*subject == principal_id
            && (kind != 1 || catalog_field_bytes(&key, &bytes, 10, 3)? != [1]))
            || (role_ids.contains(subject) && kind != 2)
            || principal_authority_epoch(&key, &bytes)? == 0
        {
            return Err(CommitError::InvalidCatalog);
        }
        add_catalog_guard(&mut guards, key, Some(bytes));
    }
    let mut admin_role_id = [0x80; 16];
    admin_role_id[15] = 3;
    let is_admin = roles.contains(&admin_role_id);
    let metadata = read
        .open_table(ENGINE_META)
        .map_err(storage_error)?
        .get(&b"format"[..])
        .map_err(storage_error)?
        .ok_or(CommitError::InvalidMetadata)?;
    let database_id = EngineMetadata::decode(metadata.value())
        .map_err(|_| CommitError::InvalidMetadata)?
        .database_id;
    let mut authorized = is_admin;
    for subject in subject_ids {
        let key = grant_key(subject, database_id, 14, 1, database_id);
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
    if !authorized {
        return Err(CommitError::DdlAuthorizationDenied);
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

pub(super) fn schema_drop_ddl_authority_guards(
    database: &Database,
    principal_id: [u8; 16],
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
) -> Result<Vec<CatalogGuard>, CommitError> {
    if principal_id == [0; 16]
        || catalog_writes.len() != 3
        || audit_events.is_empty()
        || audit_events.iter().any(|event| {
            event.actor_id != Some(principal_id)
                || event.action != crate::audit::AuditAction::Ddl
                || event.result != crate::audit::AuditResult::Succeeded
        })
    {
        return Err(if audit_events.is_empty() {
            CommitError::InvalidAudit
        } else if principal_id == [0; 16] {
            CommitError::DdlAuthorizationDenied
        } else {
            CommitError::InvalidCatalog
        });
    }
    let mut schema_id = None;
    let mut object_write = None;
    let mut schema_write = None;
    let mut reservation_write = None;
    for write in catalog_writes {
        let id: [u8; 16] = write
            .key
            .get(3..19)
            .ok_or(CommitError::InvalidCatalog)?
            .try_into()
            .map_err(|_| CommitError::InvalidCatalog)?;
        if id == [0; 16] || schema_id.replace(id).is_some_and(|previous| previous != id) {
            return Err(CommitError::InvalidCatalog);
        }
        match write
            .key
            .get(..2)
            .map(|bytes| u16::from_be_bytes([bytes[0], bytes[1]]))
        {
            Some(2)
                if write.key == id_catalog_key(2, id)
                    && write.payload.is_none()
                    && write.expected_value.is_some()
                    && object_write.replace(write).is_none() => {}
            Some(3)
                if write.key == id_catalog_key(3, id)
                    && write.payload.is_none()
                    && write.expected_value.is_some()
                    && schema_write.replace(write).is_none() => {}
            Some(17)
                if write.key == id_catalog_key(17, id)
                    && write.expected_value.is_none()
                    && write.payload.is_some()
                    && reservation_write.replace(write).is_none() => {}
            _ => return Err(CommitError::InvalidCatalog),
        }
    }
    let schema_id = schema_id.ok_or(CommitError::InvalidCatalog)?;
    let reservation_write = reservation_write.ok_or(CommitError::InvalidCatalog)?;
    if audit_events
        .iter()
        .any(|event| event.target_ids.as_slice() != [schema_id])
    {
        return Err(CommitError::InvalidAudit);
    }
    let object_key = id_catalog_key(2, schema_id);
    let schema_key = id_catalog_key(3, schema_id);
    let reservation_key = id_catalog_key(17, schema_id);
    let object_bytes = object_write
        .and_then(|write| write.expected_value.as_deref())
        .ok_or(CommitError::InvalidCatalog)?;
    let schema_bytes = schema_write
        .and_then(|write| write.expected_value.as_deref())
        .ok_or(CommitError::InvalidCatalog)?;
    let object = decode_catalog_record(&object_key, object_bytes)?;
    let schema = decode_catalog_record(&schema_key, schema_bytes)?;
    let bytes = reservation_write
        .payload
        .as_deref()
        .ok_or(CommitError::InvalidCatalog)?;
    let reservation = decode_catalog_record(&reservation_key, bytes)?;
    if reservation.family_code != 17 || !reservation.fields.is_empty() {
        return Err(CommitError::InvalidCatalog);
    }
    if object.family_code != 2
        || schema.family_code != 3
        || catalog_field_u16(&object_key, object_bytes, 2, 1)? != 1
        || object.fields.iter().any(|field| field.tag == 2)
        || catalog_field_u64(&object_key, object_bytes, 2, 5)? == 0
        || catalog_field_u16(&object_key, object_bytes, 2, 6)? != 1
        || schema.fields.len() != 1
        || catalog_field_bytes(&schema_key, schema_bytes, 3, 1)? != 0u32.to_le_bytes()
    {
        return Err(CommitError::InvalidCatalog);
    }

    let read = database.begin_read().map_err(storage_error)?;
    let catalog = read.open_table(CATALOG).map_err(storage_error)?;
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
    add_catalog_guard(&mut guards, object_key.clone(), Some(object_bytes.to_vec()));
    add_catalog_guard(&mut guards, schema_key, Some(schema_bytes.to_vec()));
    add_catalog_guard(&mut guards, reservation_key, None);
    add_catalog_guard(&mut guards, database_key, Some(database_bytes));
    add_catalog_guard(&mut guards, actor_key, Some(actor_bytes));

    let mut memberships = Vec::new();
    let mut subject_ids = HashSet::from([principal_id]);
    let membership_prefix = 11u16.to_be_bytes();
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
        if membership_count > MAX_MEMBERSHIP_EDGES {
            return Err(CommitError::InvalidCatalog);
        }
        let bytes = value.value().to_vec();
        let edge = decode_membership_record(&key, &bytes)?;
        subject_ids.insert(edge.member_id);
        subject_ids.insert(edge.role_id);
        memberships.push(edge);
        add_catalog_guard(&mut guards, key, Some(bytes));
    }
    let roles = resolve_role_closure(principal_id, &memberships)
        .map_err(|_| CommitError::InvalidCatalog)?;
    let mut admin_role_id = [0x80; 16];
    admin_role_id[15] = 3;
    let owner_id: [u8; 16] = catalog_field_bytes(&object_key, object_bytes, 2, 4)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let mut authorized =
        owner_id == principal_id || roles.contains(&owner_id) || roles.contains(&admin_role_id);
    let mut authority_subjects = roles.clone();
    authority_subjects.insert(principal_id);
    for subject in authority_subjects {
        let key = grant_key(subject, schema_id, 8, 1, schema_id);
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
    if !authorized {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    for entry in builtins[4..].chunks_exact(20) {
        let code = u16::from_le_bytes([entry[0], entry[1]]);
        if matches!(code, 1 | 2) && entry[4..20] == schema_id {
            return Err(CommitError::DdlAuthorizationDenied);
        }
    }
    validate_schema_drop_empty_snapshot(&catalog, schema_id)?;
    subject_ids.insert(owner_id);
    subject_ids.extend(roles);
    for subject in subject_ids {
        let key = id_catalog_key(10, subject);
        let bytes = catalog
            .get(key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        let kind = decode_principal_kind(&key, &bytes)?;
        if (memberships.iter().any(|edge| edge.role_id == subject) && kind != 2)
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

pub(super) fn validate_schema_drop_empty_snapshot<
    T: redb::ReadableTable<&'static [u8], &'static [u8]>,
>(
    catalog: &T,
    schema_id: [u8; 16],
) -> Result<(), CommitError> {
    let object_prefix = 2u16.to_be_bytes();
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
        if scanned > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
            return Err(CommitError::InvalidCatalog);
        }
        if key.len() != 19 {
            return Err(CommitError::InvalidCatalog);
        }
        let record = decode_catalog_record(key, value.value())?;
        if record
            .fields
            .iter()
            .any(|field| field.tag == 2 && field.payload.as_slice() == schema_id)
        {
            return Err(CommitError::SchemaNotEmpty);
        }
    }
    let dependency_prefix = 15u16.to_be_bytes();
    scanned = 0;
    for entry in catalog
        .range(dependency_prefix.as_slice()..)
        .map_err(storage_error)?
    {
        let (key, value) = entry.map_err(storage_error)?;
        let key = key.value();
        if !key.starts_with(&dependency_prefix) {
            break;
        }
        scanned += 1;
        if scanned > crate::snapshot::MAX_CATALOG_FAMILY_SCAN {
            return Err(CommitError::InvalidCatalog);
        }
        let (_, referenced, _) = decode_dependency_edge(key, value.value())?;
        if referenced == schema_id {
            return Err(CommitError::SchemaNotEmpty);
        }
    }
    Ok(())
}

pub(super) fn schema_metadata_ddl_authority_guards(
    database: &Database,
    principal_id: [u8; 16],
    catalog_writes: &[CatalogWrite],
    audit_events: &[AuditEvent],
    owner_transfer: bool,
) -> Result<Vec<CatalogGuard>, CommitError> {
    if principal_id == [0; 16] || catalog_writes.len() != 1 || audit_events.is_empty() {
        return Err(CommitError::InvalidCatalog);
    }
    let write = &catalog_writes[0];
    let schema_id: [u8; 16] = write
        .key
        .get(3..19)
        .ok_or(CommitError::InvalidCatalog)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let object_key = id_catalog_key(2, schema_id);
    if schema_id == [0; 16]
        || write.key != object_key
        || write.expected_value.is_none()
        || write.payload.is_none()
        || audit_events.iter().any(|event| {
            event.actor_id != Some(principal_id)
                || event.action != crate::audit::AuditAction::Ddl
                || event.result != crate::audit::AuditResult::Succeeded
                || event.target_ids.as_slice() != [schema_id]
        })
    {
        return Err(CommitError::InvalidAudit);
    }
    let old_bytes = write
        .expected_value
        .as_deref()
        .ok_or(CommitError::InvalidCatalog)?;
    let new_bytes = write
        .payload
        .as_deref()
        .ok_or(CommitError::InvalidCatalog)?;
    let old_record = decode_catalog_record(&object_key, old_bytes)?;
    let new_record = decode_catalog_record(&object_key, new_bytes)?;
    let old_owner: [u8; 16] = catalog_field_bytes(&object_key, old_bytes, 2, 4)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let new_owner: [u8; 16] = catalog_field_bytes(&object_key, new_bytes, 2, 4)?
        .try_into()
        .map_err(|_| CommitError::InvalidCatalog)?;
    let old_name = catalog_field_bytes(&object_key, old_bytes, 2, 3)?;
    let new_name = catalog_field_bytes(&object_key, new_bytes, 2, 3)?;
    let mutation_valid = if owner_transfer {
        old_owner != new_owner
            && old_name == new_name
            && catalog_fields_equal_except(&old_record, &new_record, &[4, 5])
    } else {
        old_owner == new_owner
            && old_name != new_name
            && is_canonical_catalog_name(&new_name)
            && catalog_fields_equal_except(&old_record, &new_record, &[3, 5])
    };
    if old_record.family_code != 2
        || new_record.family_code != 2
        || catalog_field_u16(&object_key, old_bytes, 2, 1)? != 1
        || catalog_field_u16(&object_key, new_bytes, 2, 1)? != 1
        || old_record.fields.iter().any(|field| field.tag == 2)
        || new_record.fields.iter().any(|field| field.tag == 2)
        || old_owner == [0; 16]
        || new_owner == [0; 16]
        || !mutation_valid
        || catalog_field_u16(&object_key, old_bytes, 2, 6)? != 1
        || catalog_field_u16(&object_key, new_bytes, 2, 6)? != 1
        || catalog_field_u64(&object_key, new_bytes, 2, 5)?
            != catalog_field_u64(&object_key, old_bytes, 2, 5)?
                .checked_add(1)
                .ok_or(CommitError::InvalidCatalog)?
    {
        return Err(CommitError::InvalidCatalog);
    }

    let read = database.begin_read().map_err(storage_error)?;
    let catalog = read.open_table(CATALOG).map_err(storage_error)?;
    let schema_key = id_catalog_key(3, schema_id);
    let schema_bytes = catalog
        .get(schema_key.as_slice())
        .map_err(storage_error)?
        .map(|value| value.value().to_vec())
        .ok_or(CommitError::InvalidCatalog)?;
    decode_catalog_record(&schema_key, &schema_bytes)?;
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

    let actor_key = id_catalog_key(10, principal_id);
    let actor_bytes = catalog
        .get(actor_key.as_slice())
        .map_err(storage_error)?
        .map(|value| value.value().to_vec())
        .ok_or(CommitError::DdlAuthorizationDenied)?;
    if decode_principal_kind(&actor_key, &actor_bytes)? != 1
        || catalog_field_bytes(&actor_key, &actor_bytes, 10, 3)? != [1]
        || principal_authority_epoch(&actor_key, &actor_bytes)? == 0
    {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    let mut guards = HashMap::<Vec<u8>, Option<Vec<u8>>>::new();
    add_catalog_guard(&mut guards, object_key, Some(old_bytes.to_vec()));
    add_catalog_guard(&mut guards, schema_key, Some(schema_bytes));
    add_catalog_guard(&mut guards, database_key, Some(database_bytes));
    add_catalog_guard(&mut guards, actor_key, Some(actor_bytes));

    let membership_prefix = 11u16.to_be_bytes();
    let mut memberships = Vec::new();
    let mut subjects = HashSet::from([principal_id, old_owner, new_owner]);
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
        subjects.extend([edge.member_id, edge.role_id]);
        memberships.push(edge);
        add_catalog_guard(&mut guards, key, Some(bytes));
    }
    let roles = resolve_role_closure(principal_id, &memberships)
        .map_err(|_| CommitError::InvalidCatalog)?;
    let mut admin_role_id = [0x80; 16];
    admin_role_id[15] = 3;
    let mut authorized =
        old_owner == principal_id || roles.contains(&old_owner) || roles.contains(&admin_role_id);
    let mut authority_subjects = roles.clone();
    authority_subjects.insert(principal_id);
    for subject in authority_subjects {
        let key = grant_key(subject, schema_id, 8, 1, schema_id);
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
    if !authorized {
        return Err(CommitError::DdlAuthorizationDenied);
    }
    let role_ids = memberships
        .iter()
        .map(|edge| edge.role_id)
        .collect::<HashSet<_>>();
    subjects.extend(roles);
    for subject in subjects {
        let key = id_catalog_key(10, subject);
        let bytes = catalog
            .get(key.as_slice())
            .map_err(storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        let kind = decode_principal_kind(&key, &bytes)?;
        if !matches!(kind, 1 | 2)
            || principal_authority_epoch(&key, &bytes)? == 0
            || (role_ids.contains(&subject)
                && (kind != 2 || catalog_field_bytes(&key, &bytes, 10, 3)? != [0]))
            || (subject == new_owner
                && kind == 2
                && catalog_field_bytes(&key, &bytes, 10, 3)? != [0])
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
