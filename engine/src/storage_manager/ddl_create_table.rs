use super::catalog_helpers::*;
use super::*;
use crate::audit::{AuditAction, AuditEvent, AuditResult};
use crate::catalog::{
    CatalogField, WireType, decode_builtin_scalar_type, encode_envelope, family_schema,
};
use crate::commit::{CatalogWrite, CommitError, commit_authorized_table_create_ddl_with_fences};
use crate::schema::CATALOG;
use crate::tuple::{ColumnSchema, ScalarType, encode_tuple};
use redb::ReadableDatabase;
use std::collections::{HashMap, HashSet};
use zeroize::Zeroize;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableColumnDefinition {
    pub name: String,
    pub scalar_type: ScalarType,
    pub nullable: bool,
    pub generated_uuid_identity: bool,
    pub default: Option<TableColumnDefault>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TableColumnDefault {
    Literal(Option<crate::tuple::ScalarValue>),
    CurrentPrincipalId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTableRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub schema_id: [u8; 16],
    pub name: String,
    pub columns: Vec<TableColumnDefinition>,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTableForeignKeyDefinition {
    pub source_columns: Vec<String>,
    pub target_constraint_id: [u8; 16],
    /// Local referenced key columns for an atomic self-reference.
    pub target_columns: Option<Vec<String>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTableCheckDefinition {
    pub name: Option<String>,
    pub expression: crate::authorization::Expr,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedTable {
    pub table_id: [u8; 16],
    pub column_ids: Vec<[u8; 16]>,
    pub commit_sequence: u64,
}

impl<'db> StorageManager<'db> {
    /// Create a table and its ordered column catalog in one authorized DDL
    /// commit. Columns receive no implicit query grants.
    pub fn create_table(
        &self,
        request: CreateTableRequest,
    ) -> Result<CreatedTable, TableLifecycleError> {
        self.create_table_inner(request, None, None, &[], &[], &[], &[], &[], &[])
    }

    /// Create a table and its primary key catalog/index records atomically.
    pub fn create_table_with_primary_key(
        &self,
        request: CreateTableRequest,
        primary_key_columns: &[String],
    ) -> Result<CreatedTable, TableLifecycleError> {
        self.create_table_inner(
            request,
            Some(primary_key_columns.to_vec()),
            None,
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
        )
    }

    /// Create a table and its table-level UNIQUE constraints and indexes atomically.
    pub fn create_table_with_unique_constraints(
        &self,
        request: CreateTableRequest,
        unique_constraints: &[Vec<String>],
    ) -> Result<CreatedTable, TableLifecycleError> {
        self.create_table_inner(
            request,
            None,
            None,
            unique_constraints,
            &[],
            &[],
            &[],
            &[],
            &[],
        )
    }

    /// Create a table with a primary key and table-level UNIQUE constraints atomically.
    pub fn create_table_with_constraints(
        &self,
        request: CreateTableRequest,
        primary_key_columns: Option<&[String]>,
        unique_constraints: &[Vec<String>],
    ) -> Result<CreatedTable, TableLifecycleError> {
        self.create_table_inner(
            request,
            primary_key_columns.map(<[String]>::to_vec),
            None,
            unique_constraints,
            &[],
            &[],
            &[],
            &[],
            &[],
        )
    }

    pub fn create_table_with_checks_and_constraints(
        &self,
        request: CreateTableRequest,
        primary_key_columns: Option<&[String]>,
        unique_constraints: &[Vec<String>],
        check_expressions: &[crate::authorization::Expr],
    ) -> Result<CreatedTable, TableLifecycleError> {
        self.create_table_inner(
            request,
            primary_key_columns.map(<[String]>::to_vec),
            None,
            unique_constraints,
            &[],
            check_expressions,
            &[],
            &[],
            &[],
        )
    }

    pub fn create_table_with_foreign_keys(
        &self,
        request: CreateTableRequest,
        primary_key_columns: Option<&[String]>,
        unique_constraints: &[Vec<String>],
        check_expressions: &[crate::authorization::Expr],
        foreign_keys: &[CreateTableForeignKeyDefinition],
    ) -> Result<CreatedTable, TableLifecycleError> {
        self.create_table_inner(
            request,
            primary_key_columns.map(<[String]>::to_vec),
            None,
            unique_constraints,
            &[],
            check_expressions,
            foreign_keys,
            &[],
            &[],
        )
    }

    pub fn create_table_with_named_checks_and_foreign_keys(
        &self,
        request: CreateTableRequest,
        primary_key_columns: Option<&[String]>,
        unique_constraints: &[Vec<String>],
        check_expressions: &[crate::authorization::Expr],
        check_constraint_names: &[Option<String>],
        foreign_keys: &[CreateTableForeignKeyDefinition],
    ) -> Result<CreatedTable, TableLifecycleError> {
        self.create_table_with_named_constraints_and_foreign_keys(
            request,
            primary_key_columns,
            None,
            unique_constraints,
            &[],
            check_expressions,
            check_constraint_names,
            foreign_keys,
            &[],
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_table_with_named_constraints_and_foreign_keys(
        &self,
        request: CreateTableRequest,
        primary_key_columns: Option<&[String]>,
        primary_key_name: Option<String>,
        unique_constraints: &[Vec<String>],
        unique_constraint_names: &[Option<String>],
        check_expressions: &[crate::authorization::Expr],
        check_constraint_names: &[Option<String>],
        foreign_keys: &[CreateTableForeignKeyDefinition],
        foreign_key_names: &[Option<String>],
    ) -> Result<CreatedTable, TableLifecycleError> {
        self.create_table_inner(
            request,
            primary_key_columns.map(<[String]>::to_vec),
            primary_key_name,
            unique_constraints,
            unique_constraint_names,
            check_expressions,
            foreign_keys,
            foreign_key_names,
            check_constraint_names,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn create_table_inner(
        &self,
        request: CreateTableRequest,
        primary_key_columns: Option<Vec<String>>,
        primary_key_name: Option<String>,
        unique_constraints: &[Vec<String>],
        unique_constraint_names: &[Option<String>],
        check_expressions: &[crate::authorization::Expr],
        foreign_keys: &[CreateTableForeignKeyDefinition],
        foreign_key_names: &[Option<String>],
        check_constraint_names: &[Option<String>],
    ) -> Result<CreatedTable, TableLifecycleError> {
        let CreateTableRequest {
            invocation_id,
            authenticated_actor_id,
            schema_id,
            name,
            columns,
            retention_deadline_us,
            mut audit_event,
        } = request;
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || schema_id == [0; 16]
            || retention_deadline_us <= 0
            || !is_normalized_principal_name(&name)
            || columns.is_empty()
            || columns.len() > crate::tuple::MAX_COLUMNS
            || check_expressions.len() > crate::tuple::MAX_COLUMNS
            || (!check_constraint_names.is_empty()
                && check_constraint_names.len() != check_expressions.len())
            || (primary_key_name.is_some() && primary_key_columns.is_none())
            || (!unique_constraint_names.is_empty()
                && unique_constraint_names.len() != unique_constraints.len())
            || (!foreign_key_names.is_empty() && foreign_key_names.len() != foreign_keys.len())
            || primary_key_name
                .as_ref()
                .is_some_and(|name| !is_normalized_principal_name(name))
            || unique_constraint_names
                .iter()
                .flatten()
                .chain(foreign_key_names.iter().flatten())
                .any(|name| !is_normalized_principal_name(name))
            || check_constraint_names
                .iter()
                .flatten()
                .any(|name| !is_normalized_principal_name(name))
            || check_constraint_names
                .iter()
                .flatten()
                .collect::<HashSet<_>>()
                .len()
                != check_constraint_names.iter().flatten().count()
            || foreign_keys.len() > crate::tuple::MAX_COLUMNS
            || check_expressions.iter().any(|expression| {
                !crate::expression::is_row_local_expression(expression)
                    || crate::expression::encode_expression(expression).is_err()
            })
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(TableLifecycleError::InvalidAudit);
        }
        let mut column_names = HashSet::with_capacity(columns.len());
        for column in &columns {
            if !is_normalized_principal_name(&column.name)
                || !column_names.insert(column.name.as_str())
                || (column.generated_uuid_identity
                    && (column.scalar_type != ScalarType::Uuid
                        || column.nullable
                        || column.default.is_some()))
                || matches!(
                    column.default.as_ref(),
                    Some(TableColumnDefault::CurrentPrincipalId)
                        if column.scalar_type != ScalarType::Uuid
                )
            {
                return Err(TableLifecycleError::InvalidRequest);
            }
            let (nullable, value) = match &column.default {
                Some(TableColumnDefault::Literal(value)) => (column.nullable, value.clone()),
                Some(TableColumnDefault::CurrentPrincipalId) => (
                    column.nullable,
                    Some(crate::tuple::ScalarValue::Uuid([1; 16])),
                ),
                None => (true, None),
            };
            encode_tuple(
                &[ColumnSchema {
                    scalar_type: column.scalar_type,
                    nullable,
                }],
                1,
                &[value],
            )
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
        }
        if let Some(key_columns) = &primary_key_columns
            && (key_columns.is_empty()
                || key_columns.len() > columns.len()
                || key_columns.iter().collect::<HashSet<_>>().len() != key_columns.len()
                || key_columns.iter().any(|name| {
                    !columns
                        .iter()
                        .any(|column| &column.name == name && !column.nullable)
                }))
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if unique_constraints.len() > crate::tuple::MAX_COLUMNS
            || unique_constraints.iter().any(|key_columns| {
                key_columns.is_empty()
                    || key_columns.len() > columns.len()
                    || key_columns.iter().collect::<HashSet<_>>().len() != key_columns.len()
                    || key_columns
                        .iter()
                        .any(|name| !columns.iter().any(|column| &column.name == name))
            })
            || unique_constraints
                .iter()
                .enumerate()
                .any(|(index, columns)| unique_constraints[..index].contains(columns))
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let mut supplied_constraint_names = HashSet::new();
        if primary_key_name
            .iter()
            .chain(unique_constraint_names.iter().flatten())
            .chain(check_constraint_names.iter().flatten())
            .chain(foreign_key_names.iter().flatten())
            .any(|name| !supplied_constraint_names.insert(name.as_str()))
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if foreign_keys.iter().any(|foreign_key| {
            foreign_key.source_columns.is_empty()
                || foreign_key.source_columns.len() > columns.len()
                || (foreign_key.target_constraint_id == [0; 16])
                    != foreign_key.target_columns.is_some()
                || foreign_key
                    .target_columns
                    .as_ref()
                    .is_some_and(|target_columns| {
                        target_columns.is_empty()
                            || target_columns.len() > columns.len()
                            || target_columns.iter().collect::<HashSet<_>>().len()
                                != target_columns.len()
                            || target_columns
                                .iter()
                                .any(|name| !columns.iter().any(|column| column.name == *name))
                    })
                || foreign_key
                    .source_columns
                    .iter()
                    .collect::<HashSet<_>>()
                    .len()
                    != foreign_key.source_columns.len()
                || foreign_key
                    .source_columns
                    .iter()
                    .any(|name| !columns.iter().any(|column| column.name == *name))
        }) {
            return Err(TableLifecycleError::InvalidRequest);
        }

        let type_definitions = load_builtin_type_definitions(self.database)?;
        let table_id = self.generate_table_object_id()?;
        let mut used_ids = HashSet::with_capacity(columns.len() + 1);
        used_ids.insert(table_id);
        let mut column_ids = Vec::with_capacity(columns.len());
        for _ in &columns {
            let mut column_id = self.generate_table_object_id()?;
            if !used_ids.insert(column_id) {
                column_id.zeroize();
                return Err(TableLifecycleError::ObjectIdGenerationFailed);
            }
            column_ids.push(column_id);
        }
        let mut key_constraints = Vec::with_capacity(
            unique_constraints.len() + usize::from(primary_key_columns.is_some()),
        );
        if let Some(key_columns) = &primary_key_columns {
            let mut key_column_ids = Vec::with_capacity(key_columns.len());
            for name in key_columns {
                let ordinal = columns
                    .iter()
                    .position(|column| &column.name == name)
                    .ok_or(TableLifecycleError::InvalidRequest)?;
                key_column_ids.push(column_ids[ordinal]);
            }
            let mut constraint_id = self.generate_table_object_id()?;
            if !used_ids.insert(constraint_id) {
                constraint_id.zeroize();
                return Err(TableLifecycleError::ObjectIdGenerationFailed);
            }
            let mut index_id = self.generate_table_object_id()?;
            if !used_ids.insert(index_id) {
                index_id.zeroize();
                return Err(TableLifecycleError::ObjectIdGenerationFailed);
            }
            key_constraints.push((
                2u16,
                key_column_ids,
                primary_key_name.clone(),
                constraint_id,
                index_id,
            ));
        }
        for (index, columns_for_key) in unique_constraints.iter().enumerate() {
            let key_column_ids = columns_for_key
                .iter()
                .map(|name| {
                    let ordinal = columns
                        .iter()
                        .position(|column| &column.name == name)
                        .ok_or(TableLifecycleError::InvalidRequest)?;
                    Ok(column_ids[ordinal])
                })
                .collect::<Result<Vec<_>, TableLifecycleError>>()?;
            let mut constraint_id = self.generate_table_object_id()?;
            if !used_ids.insert(constraint_id) {
                constraint_id.zeroize();
                return Err(TableLifecycleError::ObjectIdGenerationFailed);
            }
            let mut index_id = self.generate_table_object_id()?;
            if !used_ids.insert(index_id) {
                index_id.zeroize();
                return Err(TableLifecycleError::ObjectIdGenerationFailed);
            }
            key_constraints.push((
                3u16,
                key_column_ids,
                unique_constraint_names.get(index).cloned().flatten(),
                constraint_id,
                index_id,
            ));
        }
        let mut check_constraints = Vec::with_capacity(check_expressions.len());
        for (index, expression) in check_expressions.iter().enumerate() {
            let mut constraint_id = self.generate_table_object_id()?;
            if !used_ids.insert(constraint_id) {
                constraint_id.zeroize();
                return Err(TableLifecycleError::ObjectIdGenerationFailed);
            }
            let mut expression_id = self.generate_table_object_id()?;
            if !used_ids.insert(expression_id) {
                expression_id.zeroize();
                return Err(TableLifecycleError::ObjectIdGenerationFailed);
            }
            check_constraints.push((
                expression.clone(),
                check_constraint_names.get(index).cloned().flatten(),
                constraint_id,
                expression_id,
            ));
        }
        let mut default_expression_ids = Vec::with_capacity(columns.len());
        for column in &columns {
            if column.default.is_some() {
                let mut expression_id = self.generate_table_object_id()?;
                if !used_ids.insert(expression_id) {
                    expression_id.zeroize();
                    return Err(TableLifecycleError::ObjectIdGenerationFailed);
                }
                default_expression_ids.push(Some(expression_id));
            } else {
                default_expression_ids.push(None);
            }
        }

        let default_count = default_expression_ids.iter().flatten().count();
        let mut writes = Vec::with_capacity(3 + columns.len() * 3 + default_count * 3);
        let table_object_key = id_catalog_key(2, table_id);
        writes.push(CatalogWrite {
            key: table_object_key.clone(),
            expected_value: None,
            payload: Some(
                encode_envelope(
                    &family_schema(2).ok_or(TableLifecycleError::Storage)?,
                    &table_object_key,
                    &[
                        CatalogField {
                            tag: 1,
                            wire_type: WireType::U16,
                            payload: 3u16.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 2,
                            wire_type: WireType::Id,
                            payload: schema_id.to_vec(),
                        },
                        CatalogField {
                            tag: 3,
                            wire_type: WireType::Text,
                            payload: name.as_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 4,
                            wire_type: WireType::Id,
                            payload: authenticated_actor_id.to_vec(),
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
                .map_err(|_| TableLifecycleError::InvalidRequest)?,
            ),
        });

        let table_key = id_catalog_key(5, table_id);
        writes.push(CatalogWrite {
            key: table_key.clone(),
            expected_value: None,
            payload: Some(
                encode_envelope(
                    &family_schema(5).ok_or(TableLifecycleError::Storage)?,
                    &table_key,
                    &[
                        CatalogField {
                            tag: 1,
                            wire_type: WireType::Id,
                            payload: schema_id.to_vec(),
                        },
                        CatalogField {
                            tag: 2,
                            wire_type: WireType::U64,
                            payload: 1u64.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 3,
                            wire_type: WireType::U64,
                            payload: 1u64.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 4,
                            wire_type: WireType::Id,
                            payload: table_id.to_vec(),
                        },
                        CatalogField {
                            tag: 5,
                            wire_type: WireType::U16,
                            payload: 1u16.to_le_bytes().to_vec(),
                        },
                    ],
                )
                .map_err(|_| TableLifecycleError::InvalidRequest)?,
            ),
        });
        writes.push(new_dependency_write(table_id, schema_id, 1)?);

        let table_columns = columns
            .iter()
            .map(|column| ColumnSchema {
                scalar_type: column.scalar_type,
                nullable: column.nullable,
            })
            .collect::<Vec<_>>();
        for (ordinal, ((column_id, column), default_expression_id)) in column_ids
            .iter()
            .zip(&columns)
            .zip(&default_expression_ids)
            .enumerate()
        {
            let object_key = id_catalog_key(2, *column_id);
            writes.push(CatalogWrite {
                key: object_key.clone(),
                expected_value: None,
                payload: Some(
                    encode_envelope(
                        &family_schema(2).ok_or(TableLifecycleError::Storage)?,
                        &object_key,
                        &[
                            CatalogField {
                                tag: 1,
                                wire_type: WireType::U16,
                                payload: 4u16.to_le_bytes().to_vec(),
                            },
                            CatalogField {
                                tag: 2,
                                wire_type: WireType::Id,
                                payload: table_id.to_vec(),
                            },
                            CatalogField {
                                tag: 3,
                                wire_type: WireType::Text,
                                payload: column.name.as_bytes().to_vec(),
                            },
                            CatalogField {
                                tag: 4,
                                wire_type: WireType::Id,
                                payload: authenticated_actor_id.to_vec(),
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
                    .map_err(|_| TableLifecycleError::InvalidRequest)?,
                ),
            });
            let type_kind = scalar_type_kind(column.scalar_type);
            let (type_id, type_record) = type_definitions
                .get(&type_kind)
                .ok_or(TableLifecycleError::Storage)?;
            let type_parameters = scalar_type_parameters(column.scalar_type);
            decode_builtin_scalar_type(type_record, &type_parameters)
                .map_err(|_| TableLifecycleError::InvalidRequest)?;
            if let (Some(expression_id), Some(default)) = (default_expression_id, &column.default) {
                let expression = match default {
                    TableColumnDefault::Literal(value) => {
                        crate::authorization::Expr::Literal(value.clone())
                    }
                    TableColumnDefault::CurrentPrincipalId => {
                        crate::authorization::Expr::CurrentPrincipalId
                    }
                };
                let catalog_expression = crate::expression::CatalogExpression {
                    expression_id: *expression_id,
                    purpose: crate::expression::ExpressionPurpose::Default,
                    result_type_id: *type_id,
                    result_parameters: type_parameters,
                    dependencies: Vec::new(),
                    expression: expression.clone(),
                };
                crate::expression::validate_expression_binding(
                    &catalog_expression,
                    &table_columns,
                    &column_ids,
                    *type_id,
                    column.scalar_type,
                )
                .map_err(|_| TableLifecycleError::InvalidRequest)?;
                writes.push(new_default_expression_object_write(
                    *expression_id,
                    ordinal,
                    authenticated_actor_id,
                )?);
                writes.push(new_default_expression_write(
                    *expression_id,
                    *type_id,
                    type_parameters,
                    expression,
                )?);
                writes.push(new_dependency_write(*column_id, *expression_id, 2)?);
            }
            let column_key = id_catalog_key(6, *column_id);
            let mut column_fields = vec![
                CatalogField {
                    tag: 1,
                    wire_type: WireType::Id,
                    payload: table_id.to_vec(),
                },
                CatalogField {
                    tag: 2,
                    wire_type: WireType::U32,
                    payload: (ordinal as u32).to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::Text,
                    payload: column.name.as_bytes().to_vec(),
                },
                CatalogField {
                    tag: 4,
                    wire_type: WireType::Id,
                    payload: type_id.to_vec(),
                },
                CatalogField {
                    tag: 5,
                    wire_type: WireType::Bytes,
                    payload: type_parameters.to_vec(),
                },
                CatalogField {
                    tag: 6,
                    wire_type: WireType::Bool,
                    payload: vec![u8::from(column.nullable)],
                },
            ];
            if let Some(expression_id) = default_expression_id {
                column_fields.push(CatalogField {
                    tag: 7,
                    wire_type: WireType::Id,
                    payload: expression_id.to_vec(),
                });
            }
            column_fields.extend([
                CatalogField {
                    tag: 8,
                    wire_type: WireType::U16,
                    payload: u16::from(column.generated_uuid_identity)
                        .to_le_bytes()
                        .to_vec(),
                },
                CatalogField {
                    tag: 9,
                    wire_type: WireType::U64,
                    payload: 1u64.to_le_bytes().to_vec(),
                },
            ]);
            writes.push(CatalogWrite {
                key: column_key.clone(),
                expected_value: None,
                payload: Some(
                    encode_envelope(
                        &family_schema(6).ok_or(TableLifecycleError::Storage)?,
                        &column_key,
                        &column_fields,
                    )
                    .map_err(|_| TableLifecycleError::InvalidRequest)?,
                ),
            });
            writes.push(new_dependency_write(*column_id, table_id, 1)?);
        }

        let encode_ids = |ids: &[[u8; 16]]| {
            let mut payload = (ids.len() as u32).to_le_bytes().to_vec();
            payload.extend(ids.iter().flatten());
            payload
        };
        for (constraint_kind, key_column_ids, constraint_name, constraint_id, index_id) in
            &key_constraints
        {
            let constraint_key = id_catalog_key(8, *constraint_id);
            let constraint_fields = [
                CatalogField {
                    tag: 1,
                    wire_type: WireType::Id,
                    payload: table_id.to_vec(),
                },
                CatalogField {
                    tag: 2,
                    wire_type: WireType::U16,
                    payload: constraint_kind.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::Ids,
                    payload: encode_ids(key_column_ids),
                },
                CatalogField {
                    tag: 6,
                    wire_type: WireType::Ids,
                    payload: 0u32.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 7,
                    wire_type: WireType::U16,
                    payload: 0u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 8,
                    wire_type: WireType::U16,
                    payload: 0u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 9,
                    wire_type: WireType::U16,
                    payload: 0u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 10,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 11,
                    wire_type: WireType::Id,
                    payload: index_id.to_vec(),
                },
            ];
            writes.push(CatalogWrite {
                key: constraint_key.clone(),
                expected_value: None,
                payload: Some(
                    encode_envelope(
                        &family_schema(8).ok_or(TableLifecycleError::Storage)?,
                        &constraint_key,
                        &constraint_fields,
                    )
                    .map_err(|_| TableLifecycleError::InvalidRequest)?,
                ),
            });
            let (constraint_kind_object, index_kind_object, constraint_prefix, index_prefix) =
                if *constraint_kind == 2 {
                    (6u16, 7u16, "primary_key_constraint", "primary_key_index")
                } else {
                    (6u16, 7u16, "unique_constraint", "unique_index")
                };
            for (object_id, kind, prefix, supplied_name) in [
                (
                    *constraint_id,
                    constraint_kind_object,
                    constraint_prefix,
                    constraint_name.as_deref(),
                ),
                (*index_id, index_kind_object, index_prefix, None),
            ] {
                let key = id_catalog_key(2, object_id);
                let name = supplied_name.map_or_else(
                    || {
                        format!(
                            "{prefix}_{}",
                            object_id
                                .iter()
                                .map(|byte| format!("{byte:02x}"))
                                .collect::<String>()
                        )
                    },
                    str::to_owned,
                );
                let payload = encode_envelope(
                    &family_schema(2).ok_or(TableLifecycleError::Storage)?,
                    &key,
                    &[
                        CatalogField {
                            tag: 1,
                            wire_type: WireType::U16,
                            payload: kind.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 3,
                            wire_type: WireType::Text,
                            payload: name.into_bytes(),
                        },
                        CatalogField {
                            tag: 4,
                            wire_type: WireType::Id,
                            payload: authenticated_actor_id.to_vec(),
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
                writes.push(CatalogWrite {
                    key,
                    expected_value: None,
                    payload: Some(payload),
                });
            }
            let index_key = id_catalog_key(9, *index_id);
            let index_fields = [
                CatalogField {
                    tag: 1,
                    wire_type: WireType::Id,
                    payload: table_id.to_vec(),
                },
                CatalogField {
                    tag: 2,
                    wire_type: WireType::Ids,
                    payload: encode_ids(key_column_ids),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 4,
                    wire_type: WireType::Bool,
                    payload: vec![1],
                },
                CatalogField {
                    tag: 5,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 6,
                    wire_type: WireType::Id,
                    payload: constraint_id.to_vec(),
                },
                CatalogField {
                    tag: 7,
                    wire_type: WireType::Id,
                    payload: index_id.to_vec(),
                },
                CatalogField {
                    tag: 8,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 9,
                    wire_type: WireType::U32,
                    payload: 1u32.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 10,
                    wire_type: WireType::U64,
                    payload: 1u64.to_le_bytes().to_vec(),
                },
            ];
            writes.push(CatalogWrite {
                key: index_key.clone(),
                expected_value: None,
                payload: Some(
                    encode_envelope(
                        &family_schema(9).ok_or(TableLifecycleError::Storage)?,
                        &index_key,
                        &index_fields,
                    )
                    .map_err(|_| TableLifecycleError::InvalidRequest)?,
                ),
            });
            writes.push(new_dependency_write(*constraint_id, *index_id, 4)?);
        }
        let boolean_type_id = type_definitions
            .get(&1)
            .map(|(type_id, _)| *type_id)
            .ok_or(TableLifecycleError::Storage)?;
        fn collect_check_dependencies(
            expression: &crate::authorization::Expr,
            column_ids: &[[u8; 16]],
            dependencies: &mut std::collections::BTreeSet<[u8; 16]>,
        ) -> Result<(), TableLifecycleError> {
            use crate::authorization::Expr;
            match expression {
                Expr::Field(ordinal) => {
                    let id = column_ids
                        .get(*ordinal)
                        .copied()
                        .ok_or(TableLifecycleError::InvalidRequest)?;
                    dependencies.insert(id);
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
                    collect_check_dependencies(left, column_ids, dependencies)?;
                    collect_check_dependencies(right, column_ids, dependencies)?;
                }
                Expr::And(items) | Expr::Or(items) => {
                    for item in items {
                        collect_check_dependencies(item, column_ids, dependencies)?;
                    }
                }
                Expr::Not(inner) | Expr::IsNull(inner) | Expr::Negate(inner) => {
                    collect_check_dependencies(inner, column_ids, dependencies)?;
                }
                Expr::Literal(_) => {}
                Expr::CurrentPrincipalId | Expr::HasRole(_) => {
                    return Err(TableLifecycleError::InvalidRequest);
                }
            }
            Ok(())
        }
        for (expression, constraint_name, constraint_id, expression_id) in &check_constraints {
            let mut dependency_ids = std::collections::BTreeSet::new();
            collect_check_dependencies(expression, &column_ids, &mut dependency_ids)?;
            let dependency_ids = dependency_ids.into_iter().collect::<Vec<_>>();
            let catalog_expression = crate::expression::CatalogExpression {
                expression_id: *expression_id,
                purpose: crate::expression::ExpressionPurpose::Check,
                result_type_id: boolean_type_id,
                result_parameters: [0; 8],
                dependencies: dependency_ids.clone(),
                expression: expression.clone(),
            };
            crate::expression::validate_expression_binding(
                &catalog_expression,
                &table_columns,
                &column_ids,
                boolean_type_id,
                ScalarType::Boolean,
            )
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
            let expression_bytes = crate::expression::encode_expression(expression)
                .map_err(|_| TableLifecycleError::InvalidRequest)?;
            let expression_key = id_catalog_key(7, *expression_id);
            let expression_payload = encode_envelope(
                &family_schema(7).ok_or(TableLifecycleError::Storage)?,
                &expression_key,
                &[
                    CatalogField {
                        tag: 1,
                        wire_type: WireType::U16,
                        payload: 2u16.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 2,
                        wire_type: WireType::U32,
                        payload: 1u32.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 3,
                        wire_type: WireType::Id,
                        payload: boolean_type_id.to_vec(),
                    },
                    CatalogField {
                        tag: 4,
                        wire_type: WireType::Bytes,
                        payload: vec![0; 8],
                    },
                    CatalogField {
                        tag: 5,
                        wire_type: WireType::Bytes,
                        payload: expression_bytes,
                    },
                    CatalogField {
                        tag: 6,
                        wire_type: WireType::Ids,
                        payload: encode_ids(&dependency_ids),
                    },
                ],
            )
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
            let constraint_key = id_catalog_key(8, *constraint_id);
            let constraint_payload = encode_envelope(
                &family_schema(8).ok_or(TableLifecycleError::Storage)?,
                &constraint_key,
                &[
                    CatalogField {
                        tag: 1,
                        wire_type: WireType::Id,
                        payload: table_id.to_vec(),
                    },
                    CatalogField {
                        tag: 2,
                        wire_type: WireType::U16,
                        payload: 4u16.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 3,
                        wire_type: WireType::Ids,
                        payload: 0u32.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 4,
                        wire_type: WireType::Id,
                        payload: expression_id.to_vec(),
                    },
                    CatalogField {
                        tag: 6,
                        wire_type: WireType::Ids,
                        payload: 0u32.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 7,
                        wire_type: WireType::U16,
                        payload: 0u16.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 8,
                        wire_type: WireType::U16,
                        payload: 0u16.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 9,
                        wire_type: WireType::U16,
                        payload: 0u16.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 10,
                        wire_type: WireType::U16,
                        payload: 1u16.to_le_bytes().to_vec(),
                    },
                ],
            )
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
            for (object_id, kind, prefix, supplied_name) in [
                (
                    *constraint_id,
                    6u16,
                    "check_constraint",
                    constraint_name.as_deref(),
                ),
                (*expression_id, 5u16, "check_expression", None),
            ] {
                let object_key = id_catalog_key(2, object_id);
                let object_name = supplied_name.map_or_else(
                    || {
                        format!(
                            "{prefix}_{}",
                            object_id
                                .iter()
                                .map(|byte| format!("{byte:02x}"))
                                .collect::<String>()
                        )
                    },
                    str::to_owned,
                );
                let object_payload = encode_envelope(
                    &family_schema(2).ok_or(TableLifecycleError::Storage)?,
                    &object_key,
                    &[
                        CatalogField {
                            tag: 1,
                            wire_type: WireType::U16,
                            payload: kind.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 3,
                            wire_type: WireType::Text,
                            payload: object_name.into_bytes(),
                        },
                        CatalogField {
                            tag: 4,
                            wire_type: WireType::Id,
                            payload: authenticated_actor_id.to_vec(),
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
                writes.push(CatalogWrite {
                    key: object_key,
                    expected_value: None,
                    payload: Some(object_payload),
                });
            }
            writes.extend([
                CatalogWrite {
                    key: constraint_key,
                    expected_value: None,
                    payload: Some(constraint_payload),
                },
                CatalogWrite {
                    key: expression_key,
                    expected_value: None,
                    payload: Some(expression_payload),
                },
                new_dependency_write(*constraint_id, *expression_id, 2)?,
            ]);
            for dependency_id in dependency_ids {
                writes.push(new_dependency_write(*expression_id, dependency_id, 2)?);
            }
        }

        if !foreign_keys.is_empty() {
            let read = self
                .database
                .begin_read()
                .map_err(|_| TableLifecycleError::Storage)?;
            let catalog = read
                .open_table(CATALOG)
                .map_err(|_| TableLifecycleError::Storage)?;
            let mut referenced_tables = HashMap::<[u8; 16], Vec<u8>>::new();
            let mut foreign_key_shapes = HashSet::new();
            for (index, definition) in foreign_keys.iter().enumerate() {
                let source_column_ids = definition
                    .source_columns
                    .iter()
                    .map(|name| {
                        let ordinal = columns
                            .iter()
                            .position(|column| column.name == *name)
                            .ok_or(TableLifecycleError::InvalidRequest)?;
                        Ok(column_ids[ordinal])
                    })
                    .collect::<Result<Vec<_>, TableLifecycleError>>()?;
                let local_target = definition
                    .target_columns
                    .as_ref()
                    .map(|names| {
                        names
                            .iter()
                            .map(|name| {
                                columns
                                    .iter()
                                    .position(|column| &column.name == name)
                                    .map(|ordinal| column_ids[ordinal])
                                    .ok_or(TableLifecycleError::InvalidRequest)
                            })
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .transpose()?;
                let target_constraint_id = if let Some(target_column_ids) = &local_target {
                    key_constraints
                        .iter()
                        .find(|(kind, key_columns, _, _, _)| {
                            matches!(kind, 2 | 3) && key_columns == target_column_ids
                        })
                        .map(|(_, _, _, constraint_id, _)| *constraint_id)
                        .ok_or(TableLifecycleError::InvalidRequest)?
                } else {
                    definition.target_constraint_id
                };
                if !foreign_key_shapes.insert((source_column_ids.clone(), target_constraint_id)) {
                    return Err(TableLifecycleError::InvalidRequest);
                }
                let local_target = local_target.is_some();
                let (target_table_id, target_column_ids) = if let Some(target_column_ids) =
                    definition.target_columns.as_ref()
                {
                    let target_column_ids = target_column_ids
                        .iter()
                        .map(|name| {
                            columns
                                .iter()
                                .position(|column| &column.name == name)
                                .map(|ordinal| column_ids[ordinal])
                                .ok_or(TableLifecycleError::InvalidRequest)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    (table_id, target_column_ids)
                } else {
                    let target_constraint_key = id_catalog_key(8, target_constraint_id);
                    let target_constraint_bytes = catalog
                        .get(target_constraint_key.as_slice())
                        .map_err(|_| TableLifecycleError::Storage)?
                        .map(|value| value.value().to_vec())
                        .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                    let target_constraint =
                        read_catalog_record(&target_constraint_key, &target_constraint_bytes, 8)?;
                    if !matches!(
                        read_catalog_u16(&target_constraint_key, &target_constraint_bytes, 8, 2,)?,
                        2 | 3
                    ) {
                        return Err(TableLifecycleError::InvalidRequest);
                    }
                    let target_table_id =
                        read_catalog_id(&target_constraint_key, &target_constraint_bytes, 8, 1)?;
                    let target_column_ids = target_constraint
                        .fields
                        .iter()
                        .find(|field| field.tag == 3 && field.wire_type == WireType::Ids)
                        .map(|field| decode_catalog_id_list(&field.payload))
                        .transpose()?
                        .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                    (target_table_id, target_column_ids)
                };
                if target_column_ids.is_empty()
                    || target_column_ids.len() != source_column_ids.len()
                    || target_column_ids.contains(&[0; 16])
                    || target_column_ids.iter().collect::<HashSet<_>>().len()
                        != target_column_ids.len()
                {
                    return Err(TableLifecycleError::InvalidRequest);
                }
                for (source_name, target_column_id) in
                    definition.source_columns.iter().zip(&target_column_ids)
                {
                    let source_ordinal = columns
                        .iter()
                        .position(|column| column.name == *source_name)
                        .ok_or(TableLifecycleError::InvalidRequest)?;
                    let target_scalar_type = if local_target {
                        let target_ordinal = column_ids
                            .iter()
                            .position(|column_id| column_id == target_column_id);
                        let target_ordinal = target_ordinal
                            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                        columns[target_ordinal].scalar_type
                    } else {
                        let target_column_key = id_catalog_key(6, *target_column_id);
                        let target_column_bytes = catalog
                            .get(target_column_key.as_slice())
                            .map_err(|_| TableLifecycleError::Storage)?
                            .map(|value| value.value().to_vec())
                            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                        if read_catalog_id(&target_column_key, &target_column_bytes, 6, 1)?
                            != target_table_id
                        {
                            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
                        }
                        let target_type_id =
                            read_catalog_id(&target_column_key, &target_column_bytes, 6, 4)?;
                        let target_type_key = id_catalog_key(4, target_type_id);
                        let target_type_bytes = catalog
                            .get(target_type_key.as_slice())
                            .map_err(|_| TableLifecycleError::Storage)?
                            .map(|value| value.value().to_vec())
                            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                        let target_parameters =
                            read_catalog_field(&target_column_key, &target_column_bytes, 6, 5)?;
                        let target_type_record =
                            read_catalog_record(&target_type_key, &target_type_bytes, 4)?;
                        decode_builtin_scalar_type(&target_type_record, &target_parameters)
                            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?
                    };
                    if columns[source_ordinal].scalar_type != target_scalar_type {
                        return Err(TableLifecycleError::InvalidRequest);
                    }
                }

                let mut foreign_key_id = self.generate_table_object_id()?;
                if !used_ids.insert(foreign_key_id) {
                    foreign_key_id.zeroize();
                    return Err(TableLifecycleError::ObjectIdGenerationFailed);
                }
                let encode_id_list = |ids: &[[u8; 16]]| {
                    let mut payload = (ids.len() as u32).to_le_bytes().to_vec();
                    payload.extend(ids.iter().flatten());
                    payload
                };
                let constraint_key = id_catalog_key(8, foreign_key_id);
                let constraint_payload = encode_envelope(
                    &family_schema(8).ok_or(TableLifecycleError::Storage)?,
                    &constraint_key,
                    &[
                        CatalogField {
                            tag: 1,
                            wire_type: WireType::Id,
                            payload: table_id.to_vec(),
                        },
                        CatalogField {
                            tag: 2,
                            wire_type: WireType::U16,
                            payload: 5u16.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 3,
                            wire_type: WireType::Ids,
                            payload: encode_id_list(&source_column_ids),
                        },
                        CatalogField {
                            tag: 5,
                            wire_type: WireType::Id,
                            payload: target_constraint_id.to_vec(),
                        },
                        CatalogField {
                            tag: 6,
                            wire_type: WireType::Ids,
                            payload: encode_id_list(&target_column_ids),
                        },
                        CatalogField {
                            tag: 7,
                            wire_type: WireType::U16,
                            payload: 1u16.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 8,
                            wire_type: WireType::U16,
                            payload: 1u16.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 9,
                            wire_type: WireType::U16,
                            payload: 1u16.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 10,
                            wire_type: WireType::U16,
                            payload: 1u16.to_le_bytes().to_vec(),
                        },
                    ],
                )
                .map_err(|_| TableLifecycleError::InvalidRequest)?;
                let object_key = id_catalog_key(2, foreign_key_id);
                let object_name = foreign_key_names
                    .get(index)
                    .cloned()
                    .flatten()
                    .unwrap_or_else(|| {
                        format!(
                            "foreign_key_{}",
                            foreign_key_id
                                .iter()
                                .map(|byte| format!("{byte:02x}"))
                                .collect::<String>()
                        )
                    });
                let object_payload = encode_envelope(
                    &family_schema(2).ok_or(TableLifecycleError::Storage)?,
                    &object_key,
                    &[
                        CatalogField {
                            tag: 1,
                            wire_type: WireType::U16,
                            payload: 6u16.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 3,
                            wire_type: WireType::Text,
                            payload: object_name.into_bytes(),
                        },
                        CatalogField {
                            tag: 4,
                            wire_type: WireType::Id,
                            payload: authenticated_actor_id.to_vec(),
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
                writes.extend([
                    CatalogWrite {
                        key: constraint_key,
                        expected_value: None,
                        payload: Some(constraint_payload),
                    },
                    CatalogWrite {
                        key: object_key,
                        expected_value: None,
                        payload: Some(object_payload),
                    },
                    new_dependency_write(foreign_key_id, target_constraint_id, 3)?,
                ]);
                if !local_target
                    && let std::collections::hash_map::Entry::Vacant(entry) =
                        referenced_tables.entry(target_table_id)
                {
                    let table_key = id_catalog_key(5, target_table_id);
                    let table_bytes = catalog
                        .get(table_key.as_slice())
                        .map_err(|_| TableLifecycleError::Storage)?
                        .map(|value| value.value().to_vec())
                        .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                    entry.insert(table_bytes);
                }
            }
            for (target_table_id, table_bytes) in referenced_tables {
                let table_key = id_catalog_key(5, target_table_id);
                let epoch = read_catalog_u64(&table_key, &table_bytes, 5, 3)?;
                let next_epoch = epoch
                    .checked_add(1)
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                writes.push(CatalogWrite {
                    key: table_key.clone(),
                    expected_value: Some(table_bytes.clone()),
                    payload: Some(rewrite_catalog_u64_field(
                        &table_key,
                        &table_bytes,
                        5,
                        3,
                        next_epoch,
                    )?),
                });
            }
        }
        audit_event.target_ids = vec![table_id];
        let commit_sequence = commit_authorized_table_create_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
            error => TableLifecycleError::Commit(error),
        })?;
        Ok(CreatedTable {
            table_id,
            column_ids,
            commit_sequence,
        })
    }

    pub(super) fn generate_table_object_id(&self) -> Result<[u8; 16], TableLifecycleError> {
        for _ in 0..8 {
            let mut candidate = [0; 16];
            if getrandom::fill(&mut candidate).is_err() {
                candidate.zeroize();
                return Err(TableLifecycleError::RandomnessUnavailable);
            }
            if candidate == [0; 16] {
                candidate.zeroize();
                continue;
            }
            let read = self
                .database
                .begin_read()
                .map_err(|_| TableLifecycleError::Storage)?;
            let catalog = read
                .open_table(CATALOG)
                .map_err(|_| TableLifecycleError::Storage)?;
            if catalog
                .get(id_catalog_key(2, candidate).as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .is_none()
                && catalog
                    .get(id_catalog_key(17, candidate).as_slice())
                    .map_err(|_| TableLifecycleError::Storage)?
                    .is_none()
            {
                return Ok(candidate);
            }
            candidate.zeroize();
        }
        Err(TableLifecycleError::ObjectIdGenerationFailed)
    }
}
