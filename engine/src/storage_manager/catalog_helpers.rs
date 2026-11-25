use super::*;
use crate::catalog::{
    CatalogField, CatalogRecord, WireType, decode_registered_envelope, encode_envelope,
    family_schema,
};
use crate::commit::{CatalogWrite, CommitError};
use crate::metadata::{EngineMetadata, Lifecycle};
use crate::schema::{CATALOG, ENGINE_META};
use crate::tuple::ScalarType;
use redb::{Database, ReadableDatabase};
use std::collections::{HashMap, HashSet};

pub(super) fn principal_catalog_key(principal_id: [u8; 16]) -> Vec<u8> {
    let mut key = 10u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&principal_id);
    key
}

pub(super) fn id_catalog_key(family: u16, object_id: [u8; 16]) -> Vec<u8> {
    let mut key = family.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&object_id);
    key
}

pub(super) fn read_catalog_record(
    key: &[u8],
    bytes: &[u8],
    family: u16,
) -> Result<CatalogRecord, TableLifecycleError> {
    let record = decode_registered_envelope(bytes)
        .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
    if record.family_code != family || record.logical_key != key {
        return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
    }
    Ok(record)
}

pub(super) fn decode_catalog_id_list(bytes: &[u8]) -> Result<Vec<[u8; 16]>, TableLifecycleError> {
    let invalid = || TableLifecycleError::Commit(CommitError::InvalidCatalog);
    let count_bytes: [u8; 4] = bytes
        .get(..4)
        .ok_or_else(invalid)?
        .try_into()
        .map_err(|_| invalid())?;
    let count = u32::from_le_bytes(count_bytes) as usize;
    let payload_len = count.checked_mul(16).ok_or_else(invalid)?;
    if bytes.len() != 4usize.checked_add(payload_len).ok_or_else(invalid)? {
        return Err(invalid());
    }
    bytes[4..]
        .chunks_exact(16)
        .map(|chunk| chunk.try_into().map_err(|_| invalid()))
        .collect()
}

pub(super) fn read_catalog_field(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
) -> Result<Vec<u8>, TableLifecycleError> {
    read_catalog_record(key, bytes, family)?
        .fields
        .iter()
        .find(|field| field.tag == tag)
        .map(|field| field.payload.clone())
        .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))
}

pub(super) fn read_catalog_id(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
) -> Result<[u8; 16], TableLifecycleError> {
    read_catalog_field(key, bytes, family, tag)?
        .as_slice()
        .try_into()
        .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))
}

pub(super) fn read_catalog_u32(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
) -> Result<u32, TableLifecycleError> {
    let value = read_catalog_field(key, bytes, family, tag)?;
    Ok(u32::from_le_bytes(value.as_slice().try_into().map_err(
        |_| TableLifecycleError::Commit(CommitError::InvalidCatalog),
    )?))
}

pub(super) fn read_catalog_u16(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
) -> Result<u16, TableLifecycleError> {
    let value = read_catalog_field(key, bytes, family, tag)?;
    Ok(u16::from_le_bytes(value.as_slice().try_into().map_err(
        |_| TableLifecycleError::Commit(CommitError::InvalidCatalog),
    )?))
}

pub(super) fn read_catalog_u64(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
) -> Result<u64, TableLifecycleError> {
    let value = read_catalog_field(key, bytes, family, tag)?;
    Ok(u64::from_le_bytes(value.as_slice().try_into().map_err(
        |_| TableLifecycleError::Commit(CommitError::InvalidCatalog),
    )?))
}

pub(super) fn rewrite_catalog_u32_field(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
    value: u32,
) -> Result<Vec<u8>, TableLifecycleError> {
    rewrite_catalog_fields(
        key,
        bytes,
        family,
        &[(tag, WireType::U32, value.to_le_bytes().to_vec())],
    )
}

pub(super) fn rewrite_catalog_u64_field(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
    value: u64,
) -> Result<Vec<u8>, TableLifecycleError> {
    rewrite_catalog_u64_fields(key, bytes, family, &[(tag, value)])
}

pub(super) fn rewrite_catalog_u64_fields(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    values: &[(u16, u64)],
) -> Result<Vec<u8>, TableLifecycleError> {
    rewrite_catalog_fields(
        key,
        bytes,
        family,
        &values
            .iter()
            .map(|(tag, value)| (*tag, WireType::U64, value.to_le_bytes().to_vec()))
            .collect::<Vec<_>>(),
    )
}

pub(super) fn rewrite_catalog_fields(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    values: &[(u16, WireType, Vec<u8>)],
) -> Result<Vec<u8>, TableLifecycleError> {
    let mut record = read_catalog_record(key, bytes, family)?;
    for (tag, wire_type, payload) in values {
        let field = record
            .fields
            .iter_mut()
            .find(|field| field.tag == *tag)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        if field.wire_type != *wire_type {
            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
        }
        field.payload.clone_from(payload);
    }
    encode_envelope(
        &family_schema(family).ok_or(TableLifecycleError::Storage)?,
        key,
        &record.fields,
    )
    .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))
}

pub(super) fn rewrite_optional_catalog_id_field(
    key: &[u8],
    bytes: &[u8],
    family: u16,
    tag: u16,
    value: Option<[u8; 16]>,
) -> Result<Vec<u8>, TableLifecycleError> {
    let mut record = read_catalog_record(key, bytes, family)?;
    if let Some(index) = record.fields.iter().position(|field| field.tag == tag) {
        if record.fields[index].wire_type != WireType::Id {
            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
        }
        match value {
            Some(value) => record.fields[index].payload = value.to_vec(),
            None => {
                record.fields.remove(index);
            }
        }
    } else if let Some(value) = value {
        let index = record
            .fields
            .binary_search_by_key(&tag, |field| field.tag)
            .unwrap_or_else(|index| index);
        record.fields.insert(
            index,
            CatalogField {
                tag,
                wire_type: WireType::Id,
                payload: value.to_vec(),
            },
        );
    }
    encode_envelope(
        &family_schema(family).ok_or(TableLifecycleError::Storage)?,
        key,
        &record.fields,
    )
    .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))
}

pub(super) fn new_dependency_write(
    dependent_id: [u8; 16],
    referenced_id: [u8; 16],
    kind: u16,
) -> Result<CatalogWrite, TableLifecycleError> {
    let key = dependency_catalog_key(dependent_id, referenced_id, kind);
    let payload = encode_envelope(
        &family_schema(15).ok_or(TableLifecycleError::Storage)?,
        &key,
        &[],
    )
    .map_err(|_| TableLifecycleError::InvalidRequest)?;
    Ok(CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    })
}

pub(super) fn policy_expression_dependencies(
    expression: &crate::authorization::Expr,
    column_ids: &[[u8; 16]],
) -> Result<Vec<[u8; 16]>, TableLifecycleError> {
    fn visit(
        expression: &crate::authorization::Expr,
        column_ids: &[[u8; 16]],
        dependencies: &mut HashSet<[u8; 16]>,
    ) -> Result<(), TableLifecycleError> {
        use crate::authorization::Expr;
        match expression {
            Expr::Literal(_) | Expr::CurrentPrincipalId => {}
            Expr::Field(ordinal) => {
                let column_id = column_ids
                    .get(*ordinal)
                    .copied()
                    .ok_or(TableLifecycleError::InvalidRequest)?;
                dependencies.insert(column_id);
            }
            Expr::HasRole(role_id) => {
                if *role_id == [0; 16] {
                    return Err(TableLifecycleError::InvalidRequest);
                }
                dependencies.insert(*role_id);
            }
            Expr::Equal(left, right)
            | Expr::NotEqual(left, right)
            | Expr::Less(left, right)
            | Expr::LessEqual(left, right)
            | Expr::Greater(left, right)
            | Expr::GreaterEqual(left, right)
            | Expr::Add(left, right)
            | Expr::Subtract(left, right)
            | Expr::Multiply(left, right)
            | Expr::Modulo(left, right)
            | Expr::Divide(left, right) => {
                visit(left, column_ids, dependencies)?;
                visit(right, column_ids, dependencies)?;
            }
            Expr::And(expressions) | Expr::Or(expressions) => {
                for expression in expressions {
                    visit(expression, column_ids, dependencies)?;
                }
            }
            Expr::Not(inner) | Expr::IsNull(inner) | Expr::Negate(inner) => {
                visit(inner, column_ids, dependencies)?;
            }
        }
        Ok(())
    }

    let mut dependencies = HashSet::new();
    visit(expression, column_ids, &mut dependencies)?;
    let mut dependencies = dependencies.into_iter().collect::<Vec<_>>();
    dependencies.sort_unstable();
    Ok(dependencies)
}

pub(super) fn new_default_expression_object_write(
    expression_id: [u8; 16],
    ordinal: usize,
    owner_id: [u8; 16],
) -> Result<CatalogWrite, TableLifecycleError> {
    let key = id_catalog_key(2, expression_id);
    let payload = encode_envelope(
        &family_schema(2).ok_or(TableLifecycleError::Storage)?,
        &key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::U16,
                payload: 5u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Text,
                payload: format!("default_{ordinal}").into_bytes(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Id,
                payload: owner_id.to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
        ],
    )
    .map_err(|_| TableLifecycleError::InvalidRequest)?;
    Ok(CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    })
}

pub(super) fn new_default_expression_write(
    expression_id: [u8; 16],
    type_id: [u8; 16],
    type_parameters: [u8; 8],
    expression: crate::authorization::Expr,
) -> Result<CatalogWrite, TableLifecycleError> {
    let key = id_catalog_key(7, expression_id);
    let ir = crate::expression::encode_expression(&expression)
        .map_err(|_| TableLifecycleError::InvalidRequest)?;
    let payload = encode_envelope(
        &family_schema(7).ok_or(TableLifecycleError::Storage)?,
        &key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Id,
                payload: type_id.to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Bytes,
                payload: type_parameters.to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::Bytes,
                payload: ir,
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Ids,
                payload: 0u32.to_le_bytes().to_vec(),
            },
        ],
    )
    .map_err(|_| TableLifecycleError::InvalidRequest)?;
    Ok(CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    })
}

pub(super) fn scalar_type_kind(scalar_type: ScalarType) -> u16 {
    match scalar_type {
        ScalarType::Boolean => 1,
        ScalarType::SmallInt => 2,
        ScalarType::Integer => 3,
        ScalarType::BigInt => 4,
        ScalarType::Decimal { .. } => 5,
        ScalarType::Text => 6,
        ScalarType::Varchar { .. } => 7,
        ScalarType::Bytea => 8,
        ScalarType::Uuid => 9,
        ScalarType::Date => 10,
        ScalarType::TimestampTz => 11,
    }
}

pub(super) fn scalar_type_parameters(scalar_type: ScalarType) -> [u8; 8] {
    let mut parameters = [0; 8];
    match scalar_type {
        ScalarType::Decimal { precision, scale } => {
            parameters[..2].copy_from_slice(&u16::from(precision).to_le_bytes());
            parameters[2..4].copy_from_slice(&u16::from(scale).to_le_bytes());
        }
        ScalarType::Varchar { max_chars } => {
            parameters[4..].copy_from_slice(&max_chars.to_le_bytes());
        }
        _ => {}
    }
    parameters
}

pub(super) fn load_builtin_type_definitions(
    database: &Database,
) -> Result<HashMap<u16, ([u8; 16], CatalogRecord)>, TableLifecycleError> {
    let read = database
        .begin_read()
        .map_err(|_| TableLifecycleError::Storage)?;
    let metadata_bytes = read
        .open_table(ENGINE_META)
        .map_err(|_| TableLifecycleError::Storage)?
        .get(&b"format"[..])
        .map_err(|_| TableLifecycleError::Storage)?
        .ok_or(TableLifecycleError::Storage)?
        .value()
        .to_vec();
    let metadata =
        EngineMetadata::decode(&metadata_bytes).map_err(|_| TableLifecycleError::Storage)?;
    if metadata.lifecycle != Lifecycle::Ready || !metadata.supports_current_formats() {
        return Err(TableLifecycleError::Storage);
    }
    let database_key = id_catalog_key(1, metadata.database_id);
    let catalog = read
        .open_table(CATALOG)
        .map_err(|_| TableLifecycleError::Storage)?;
    let database_record = catalog
        .get(database_key.as_slice())
        .map_err(|_| TableLifecycleError::Storage)?
        .map(|value| decode_registered_envelope(value.value()))
        .transpose()
        .map_err(|_| TableLifecycleError::Storage)?
        .ok_or(TableLifecycleError::Storage)?;
    if database_record.family_code != 1 || database_record.logical_key != database_key {
        return Err(TableLifecycleError::Storage);
    }
    let builtins = database_record
        .fields
        .iter()
        .find(|field| field.tag == 3 && field.wire_type == WireType::Bytes)
        .map(|field| field.payload.as_slice())
        .ok_or(TableLifecycleError::Storage)?;
    if builtins.len() < 4 {
        return Err(TableLifecycleError::Storage);
    }
    let count = u32::from_le_bytes(builtins[..4].try_into().unwrap()) as usize;
    if count != 31 || builtins.len() != 4 + count * 20 {
        return Err(TableLifecycleError::Storage);
    }
    let mut type_ids = HashMap::with_capacity(11);
    for entry in builtins[4..].chunks_exact(20) {
        let code = u16::from_le_bytes(entry[..2].try_into().unwrap());
        let reserved = u16::from_le_bytes(entry[2..4].try_into().unwrap());
        if (101..=111).contains(&code) {
            let type_id: [u8; 16] = entry[4..20].try_into().unwrap();
            if reserved != 0 || type_id == [0; 16] || type_ids.insert(code - 100, type_id).is_some()
            {
                return Err(TableLifecycleError::Storage);
            }
        }
    }
    if type_ids.len() != 11 {
        return Err(TableLifecycleError::Storage);
    }
    let mut result = HashMap::with_capacity(11);
    for (kind, type_id) in type_ids {
        let key = id_catalog_key(4, type_id);
        let record = catalog
            .get(key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| decode_registered_envelope(value.value()))
            .transpose()
            .map_err(|_| TableLifecycleError::Storage)?
            .ok_or(TableLifecycleError::Storage)?;
        if record.family_code != 4 || record.logical_key != key {
            return Err(TableLifecycleError::Storage);
        }
        result.insert(kind, (type_id, record));
    }
    Ok(result)
}

pub(super) fn identity_object_catalog_key(object_id: [u8; 16]) -> Vec<u8> {
    let mut key = 2u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&object_id);
    key
}

pub(super) fn retired_object_id_catalog_key(object_id: [u8; 16]) -> Vec<u8> {
    let mut key = 17u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&object_id);
    key
}

pub(super) fn membership_catalog_key(member_id: [u8; 16], role_id: [u8; 16]) -> Vec<u8> {
    let mut key = 11u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&member_id);
    key.push(1);
    key.extend_from_slice(&role_id);
    key
}

pub(super) fn dependency_catalog_key(
    dependent_id: [u8; 16],
    referenced_id: [u8; 16],
    dependency_kind: u16,
) -> Vec<u8> {
    let mut key = 15u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&dependent_id);
    key.push(1);
    key.extend_from_slice(&referenced_id);
    key.push(1);
    key.extend_from_slice(&dependency_kind.to_be_bytes());
    key
}

pub(super) fn object_grant_catalog_key(
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

pub(super) fn is_normalized_principal_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 128
        && (bytes[0].is_ascii_lowercase() || bytes[0] == b'_')
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
}

pub(super) fn system_privilege_catalog_key(
    grantee_id: [u8; 16],
    database_id: [u8; 16],
    privilege: u16,
) -> Vec<u8> {
    let mut key = 12u16.to_be_bytes().to_vec();
    key.extend_from_slice(&[1]);
    key.extend_from_slice(&grantee_id);
    key.extend_from_slice(&[1]);
    key.extend_from_slice(&database_id);
    key.extend_from_slice(&[1]);
    key.extend_from_slice(&privilege.to_be_bytes());
    key.extend_from_slice(&[1]);
    key.extend_from_slice(&1u16.to_be_bytes());
    key.extend_from_slice(&[1]);
    key.extend_from_slice(&database_id);
    key
}
