use super::catalog_helpers::*;
use super::*;
use crate::audit::{AuditAction, AuditEvent, AuditResult};
use crate::catalog::{CatalogField, WireType, encode_envelope, family_schema};
use crate::commit::{
    CatalogWrite, CommitError, commit_authorized_constraint_ddl_with_fences,
    commit_authorized_foreign_key_ddl_with_fences,
};
use crate::schema::CATALOG;
use redb::ReadableDatabase;
use std::collections::HashSet;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateForeignKeyRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub source_table_id: [u8; 16],
    pub source_column_ids: Vec<[u8; 16]>,
    pub target_constraint_id: [u8; 16],
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateUniqueConstraintRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub table_id: [u8; 16],
    pub column_ids: Vec<[u8; 16]>,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatePrimaryKeyRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub table_id: [u8; 16],
    pub column_ids: Vec<[u8; 16]>,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateCheckConstraintRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub table_id: [u8; 16],
    pub name: Option<String>,
    pub expression: crate::authorization::Expr,
    pub dependency_column_ids: Vec<[u8; 16]>,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedForeignKey {
    pub foreign_key_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedUniqueConstraint {
    pub constraint_id: [u8; 16],
    pub index_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedPrimaryKey {
    pub constraint_id: [u8; 16],
    pub index_id: [u8; 16],
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedCheckConstraint {
    pub constraint_id: [u8; 16],
    pub expression_id: [u8; 16],
    pub commit_sequence: u64,
}

impl<'db> StorageManager<'db> {
    /// Authorize and commit foreign-key additions/removals on existing
    /// tables. Creation requires source-table `ALTER` and target-table or
    /// target-column `REFERENCES`; removal requires source-table `ALTER`.
    pub fn commit_authorized_foreign_key_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        let mut writes = catalog_writes.to_vec();
        let retired_ids = catalog_writes
            .iter()
            .filter(|write| write.key.starts_with(&2u16.to_be_bytes()) && write.payload.is_none())
            .filter_map(|write| write.key.get(3..19))
            .filter_map(|bytes| <[u8; 16]>::try_from(bytes).ok())
            .collect::<HashSet<_>>();
        for retired_id in retired_ids {
            let key = retired_object_id_catalog_key(retired_id);
            if writes.iter().any(|write| write.key == key) {
                continue;
            }
            let payload = encode_envelope(
                &family_schema(17).ok_or(CommitError::InvalidCatalog)?,
                &key,
                &[],
            )
            .map_err(|_| CommitError::InvalidCatalog)?;
            writes.push(CatalogWrite {
                key,
                expected_value: None,
                payload: Some(payload),
            });
        }
        commit_authorized_foreign_key_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            &writes,
            audit_events,
        )
    }

    /// Create an FK on existing tables. The final serialized DDL commit
    /// validates source/target column compatibility, REFERENCES and ALTER
    /// authority, and existing rows while fencing both tables.
    pub fn create_foreign_key(
        &self,
        request: CreateForeignKeyRequest,
    ) -> Result<CreatedForeignKey, TableLifecycleError> {
        self.create_foreign_key_named(request, None)
    }

    pub fn create_named_foreign_key(
        &self,
        request: CreateForeignKeyRequest,
        name: String,
    ) -> Result<CreatedForeignKey, TableLifecycleError> {
        if !is_normalized_principal_name(&name) {
            return Err(TableLifecycleError::InvalidRequest);
        }
        self.create_foreign_key_named(request, Some(name))
    }

    pub(super) fn create_foreign_key_named(
        &self,
        request: CreateForeignKeyRequest,
        name: Option<String>,
    ) -> Result<CreatedForeignKey, TableLifecycleError> {
        let CreateForeignKeyRequest {
            invocation_id,
            authenticated_actor_id: authenticated_principal_id,
            source_table_id,
            source_column_ids,
            target_constraint_id,
            retention_deadline_us,
            mut audit_event,
        } = request;
        if invocation_id == [0; 16]
            || authenticated_principal_id == [0; 16]
            || source_table_id == [0; 16]
            || target_constraint_id == [0; 16]
            || source_column_ids.is_empty()
            || source_column_ids.len() > 4096
            || source_column_ids.contains(&[0; 16])
            || source_column_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                .len()
                != source_column_ids.len()
            || retention_deadline_us <= 0
        {
            return Err(TableLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_principal_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(TableLifecycleError::InvalidAudit);
        }

        let foreign_key_id = self.generate_table_object_id()?;
        let read = self
            .database
            .begin_read()
            .map_err(|_| TableLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| TableLifecycleError::Storage)?;
        let source_table_key = id_catalog_key(5, source_table_id);
        let source_table_bytes = catalog
            .get(source_table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        read_catalog_record(&source_table_key, &source_table_bytes, 5)?;

        let target_constraint_key = id_catalog_key(8, target_constraint_id);
        let target_constraint_bytes = catalog
            .get(target_constraint_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let target_constraint =
            read_catalog_record(&target_constraint_key, &target_constraint_bytes, 8)?;
        if !matches!(
            read_catalog_u16(&target_constraint_key, &target_constraint_bytes, 8, 2)?,
            2 | 3
        ) {
            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
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
        if target_column_ids.len() != source_column_ids.len() {
            return Err(TableLifecycleError::InvalidRequest);
        }

        let mut writes = Vec::with_capacity(7);
        let constraint_key = id_catalog_key(8, foreign_key_id);
        let encode_ids = |ids: &[[u8; 16]]| {
            let mut payload = (ids.len() as u32).to_le_bytes().to_vec();
            payload.extend(ids.iter().flatten());
            payload
        };
        let constraint_fields = [
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: source_table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: 5u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Ids,
                payload: encode_ids(&source_column_ids),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::Id,
                payload: target_constraint_id.to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Ids,
                payload: encode_ids(&target_column_ids),
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
        ];
        let constraint_payload = encode_envelope(
            &family_schema(8).ok_or(TableLifecycleError::Storage)?,
            &constraint_key,
            &constraint_fields,
        )
        .map_err(|_| TableLifecycleError::InvalidRequest)?;
        writes.push(CatalogWrite {
            key: constraint_key,
            expected_value: None,
            payload: Some(constraint_payload),
        });

        let source_object_key = id_catalog_key(2, source_table_id);
        let source_object_bytes = catalog
            .get(source_object_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let owner_id = read_catalog_id(&source_object_key, &source_object_bytes, 2, 4)?;
        let object_key = id_catalog_key(2, foreign_key_id);
        let name = name.unwrap_or_else(|| {
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
                    payload: name.into_bytes(),
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
        writes.push(CatalogWrite {
            key: object_key,
            expected_value: None,
            payload: Some(object_payload),
        });
        writes.push(new_dependency_write(
            foreign_key_id,
            target_constraint_id,
            3,
        )?);

        let mut table_ids = vec![source_table_id, target_table_id];
        table_ids.sort_unstable();
        table_ids.dedup();
        for table_id in table_ids {
            let table_key = id_catalog_key(5, table_id);
            let table_bytes = catalog
                .get(table_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
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
        drop(catalog);
        drop(read);

        audit_event.target_ids = vec![foreign_key_id];
        let commit_sequence = self
            .commit_authorized_foreign_key_ddl(
                invocation_id,
                authenticated_principal_id,
                retention_deadline_us,
                &writes,
                &[audit_event],
            )
            .map_err(|error| match error {
                CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
                error => TableLifecycleError::Commit(error),
            })?;
        Ok(CreatedForeignKey {
            foreign_key_id,
            commit_sequence,
        })
    }

    /// Add a deterministic row-local CHECK expression and validate it against
    /// stored rows while the table DDL fence is held.
    pub fn create_check_constraint(
        &self,
        request: CreateCheckConstraintRequest,
    ) -> Result<CreatedCheckConstraint, TableLifecycleError> {
        let CreateCheckConstraintRequest {
            invocation_id,
            authenticated_actor_id,
            table_id,
            name,
            expression,
            dependency_column_ids,
            retention_deadline_us,
            mut audit_event,
        } = request;
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || retention_deadline_us <= 0
            || name
                .as_deref()
                .is_some_and(|value| !is_normalized_principal_name(value))
            || dependency_column_ids.len() > crate::tuple::MAX_COLUMNS
            || dependency_column_ids.contains(&[0; 16])
            || dependency_column_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                .len()
                != dependency_column_ids.len()
            || !crate::expression::is_row_local_expression(&expression)
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
        let expression_bytes = crate::expression::encode_expression(&expression)
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
        let boolean_type_id = load_builtin_type_definitions(self.database)?
            .get(&1)
            .map(|(type_id, _)| *type_id)
            .ok_or(TableLifecycleError::Storage)?;
        let constraint_id = self.generate_table_object_id()?;
        let mut expression_id = self.generate_table_object_id()?;
        for _ in 0..8 {
            if expression_id != constraint_id {
                break;
            }
            expression_id = self.generate_table_object_id()?;
        }
        if expression_id == constraint_id {
            return Err(TableLifecycleError::ObjectIdGenerationFailed);
        }

        let read = self
            .database
            .begin_read()
            .map_err(|_| TableLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| TableLifecycleError::Storage)?;
        let table_key = id_catalog_key(5, table_id);
        let table_bytes = catalog
            .get(table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        read_catalog_record(&table_key, &table_bytes, 5)?;
        let table_epoch = read_catalog_u64(&table_key, &table_bytes, 5, 3)?;
        let next_table_epoch = table_epoch
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let table_object_key = id_catalog_key(2, table_id);
        let table_object = catalog
            .get(table_object_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let owner_id = read_catalog_id(&table_object_key, &table_object, 2, 4)?;
        let encode_ids = |ids: &[[u8; 16]]| {
            let mut payload = (ids.len() as u32).to_le_bytes().to_vec();
            payload.extend(ids.iter().flatten());
            payload
        };
        let expression_key = id_catalog_key(7, expression_id);
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
                    payload: encode_ids(&dependency_column_ids),
                },
            ],
        )
        .map_err(|_| TableLifecycleError::InvalidRequest)?;
        let constraint_key = id_catalog_key(8, constraint_id);
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
        let object_write = |object_id: [u8; 16], kind: u16, name: String| {
            let key = id_catalog_key(2, object_id);
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
            Ok::<_, TableLifecycleError>(CatalogWrite {
                key,
                expected_value: None,
                payload: Some(payload),
            })
        };
        let mut writes = vec![
            CatalogWrite {
                key: constraint_key,
                expected_value: None,
                payload: Some(constraint_payload),
            },
            object_write(
                constraint_id,
                6,
                name.clone().unwrap_or_else(|| {
                    format!(
                        "check_constraint_{}",
                        constraint_id
                            .iter()
                            .map(|byte| format!("{byte:02x}"))
                            .collect::<String>()
                    )
                }),
            )?,
            CatalogWrite {
                key: expression_key,
                expected_value: None,
                payload: Some(expression_payload),
            },
            object_write(
                expression_id,
                5,
                format!(
                    "check_expression_{}",
                    expression_id
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect::<String>()
                ),
            )?,
            new_dependency_write(constraint_id, expression_id, 2)?,
        ];
        for column_id in &dependency_column_ids {
            writes.push(new_dependency_write(expression_id, *column_id, 2)?);
        }
        writes.push(CatalogWrite {
            key: table_key.clone(),
            expected_value: Some(table_bytes.clone()),
            payload: Some(rewrite_catalog_u64_field(
                &table_key,
                &table_bytes,
                5,
                3,
                next_table_epoch,
            )?),
        });
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![constraint_id, expression_id];
        audit_event.target_ids.sort_unstable();
        let commit_sequence = self
            .commit_authorized_constraint_ddl(
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
        Ok(CreatedCheckConstraint {
            constraint_id,
            expression_id,
            commit_sequence,
        })
    }

    /// Remove one CHECK constraint and its private expression records under
    /// the table DDL fence.
    pub fn drop_check_constraint(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        constraint_id: [u8; 16],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, TableLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || constraint_id == [0; 16]
            || retention_deadline_us <= 0
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
        let read = self
            .database
            .begin_read()
            .map_err(|_| TableLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| TableLifecycleError::Storage)?;
        let constraint_key = id_catalog_key(8, constraint_id);
        let constraint_bytes = catalog
            .get(constraint_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        read_catalog_record(&constraint_key, &constraint_bytes, 8)?;
        if read_catalog_u16(&constraint_key, &constraint_bytes, 8, 2)? != 4 {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let table_id = read_catalog_id(&constraint_key, &constraint_bytes, 8, 1)?;
        let expression_id = read_catalog_id(&constraint_key, &constraint_bytes, 8, 4)?;
        let expression_key = id_catalog_key(7, expression_id);
        let expression_bytes = catalog
            .get(expression_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let expression_record = crate::catalog::decode_registered_envelope(&expression_bytes)
            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let expression = crate::expression::decode_catalog_expression_record(&expression_record)
            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        if expression.purpose != crate::expression::ExpressionPurpose::Check {
            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
        }
        let table_key = id_catalog_key(5, table_id);
        let table_bytes = catalog
            .get(table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let table_epoch = read_catalog_u64(&table_key, &table_bytes, 5, 3)?;
        let next_table_epoch = table_epoch
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let mut writes = Vec::with_capacity(5 + expression.dependencies.len());
        for (object_id, key, bytes) in [
            (constraint_id, constraint_key, constraint_bytes),
            (expression_id, expression_key, expression_bytes),
        ] {
            writes.push(CatalogWrite {
                key,
                expected_value: Some(bytes),
                payload: None,
            });
            let object_key = id_catalog_key(2, object_id);
            let object_bytes = catalog
                .get(object_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            writes.push(CatalogWrite {
                key: object_key,
                expected_value: Some(object_bytes),
                payload: None,
            });
        }
        for (dependent_id, referenced_id) in std::iter::once((constraint_id, expression_id)).chain(
            expression
                .dependencies
                .iter()
                .map(|dependency_id| (expression_id, *dependency_id)),
        ) {
            let edge = new_dependency_write(dependent_id, referenced_id, 2)?;
            let edge_bytes = catalog
                .get(edge.key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            writes.push(CatalogWrite {
                key: edge.key,
                expected_value: Some(edge_bytes),
                payload: None,
            });
        }
        writes.push(CatalogWrite {
            key: table_key.clone(),
            expected_value: Some(table_bytes.clone()),
            payload: Some(rewrite_catalog_u64_field(
                &table_key,
                &table_bytes,
                5,
                3,
                next_table_epoch,
            )?),
        });
        let retired_ids = writes
            .iter()
            .filter(|write| write.key.starts_with(&2u16.to_be_bytes()) && write.payload.is_none())
            .filter_map(|write| write.key.get(3..19))
            .filter_map(|bytes| <[u8; 16]>::try_from(bytes).ok())
            .collect::<HashSet<_>>();
        for retired_id in retired_ids {
            let reservation_key = retired_object_id_catalog_key(retired_id);
            let reservation_payload = encode_envelope(
                &family_schema(17)
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?,
                &reservation_key,
                &[],
            )
            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            writes.push(CatalogWrite {
                key: reservation_key,
                expected_value: None,
                payload: Some(reservation_payload),
            });
        }
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![constraint_id, expression_id];
        audit_event.target_ids.sort_unstable();
        let commit_sequence = self
            .commit_authorized_constraint_ddl(
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
        Ok(commit_sequence)
    }

    /// Drop a UNIQUE constraint and its owned index. Referencing foreign keys
    /// are rejected by the final catalog dependency checks.
    pub fn drop_unique_constraint(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        constraint_id: [u8; 16],
        retention_deadline_us: i64,
        audit_event: AuditEvent,
    ) -> Result<u64, TableLifecycleError> {
        self.drop_key_constraint(
            invocation_id,
            authenticated_actor_id,
            constraint_id,
            3,
            retention_deadline_us,
            audit_event,
        )
    }

    /// Drop a PRIMARY KEY and its owned index. Referencing foreign keys are
    /// rejected by the final catalog dependency checks.
    pub fn drop_primary_key(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        constraint_id: [u8; 16],
        retention_deadline_us: i64,
        audit_event: AuditEvent,
    ) -> Result<u64, TableLifecycleError> {
        self.drop_key_constraint(
            invocation_id,
            authenticated_actor_id,
            constraint_id,
            2,
            retention_deadline_us,
            audit_event,
        )
    }

    pub(super) fn drop_key_constraint(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        constraint_id: [u8; 16],
        expected_kind: u16,
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, TableLifecycleError> {
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || constraint_id == [0; 16]
            || retention_deadline_us <= 0
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
        let read = self
            .database
            .begin_read()
            .map_err(|_| TableLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| TableLifecycleError::Storage)?;
        let constraint_key = id_catalog_key(8, constraint_id);
        let constraint_bytes = catalog
            .get(constraint_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        read_catalog_record(&constraint_key, &constraint_bytes, 8)?;
        if read_catalog_u16(&constraint_key, &constraint_bytes, 8, 2)? != expected_kind {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let table_id = read_catalog_id(&constraint_key, &constraint_bytes, 8, 1)?;
        let index_id = read_catalog_id(&constraint_key, &constraint_bytes, 8, 11)?;
        let index_key = id_catalog_key(9, index_id);
        let index_bytes = catalog
            .get(index_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        read_catalog_record(&index_key, &index_bytes, 9)?;
        let edge_key = dependency_catalog_key(constraint_id, index_id, 4);
        let edge_bytes = catalog
            .get(edge_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let mut writes = Vec::with_capacity(6);
        for (key, bytes) in [
            (constraint_key.clone(), constraint_bytes),
            (index_key.clone(), index_bytes),
        ] {
            writes.push(CatalogWrite {
                key,
                expected_value: Some(bytes),
                payload: None,
            });
        }
        for object_id in [constraint_id, index_id] {
            let object_key = id_catalog_key(2, object_id);
            let object_bytes = catalog
                .get(object_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            writes.push(CatalogWrite {
                key: object_key,
                expected_value: Some(object_bytes),
                payload: None,
            });
        }
        writes.push(CatalogWrite {
            key: edge_key,
            expected_value: Some(edge_bytes),
            payload: None,
        });
        let table_key = id_catalog_key(5, table_id);
        let table_bytes = catalog
            .get(table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let next_epoch = read_catalog_u64(&table_key, &table_bytes, 5, 3)?
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
        let retired_ids = writes
            .iter()
            .filter(|write| write.key.starts_with(&2u16.to_be_bytes()) && write.payload.is_none())
            .filter_map(|write| write.key.get(3..19))
            .filter_map(|bytes| <[u8; 16]>::try_from(bytes).ok())
            .collect::<HashSet<_>>();
        for retired_id in retired_ids {
            let reservation_key = retired_object_id_catalog_key(retired_id);
            let reservation_payload = encode_envelope(
                &family_schema(17)
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?,
                &reservation_key,
                &[],
            )
            .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            writes.push(CatalogWrite {
                key: reservation_key,
                expected_value: None,
                payload: Some(reservation_payload),
            });
        }
        drop(catalog);
        drop(read);
        audit_event.target_ids = vec![constraint_id, index_id];
        audit_event.target_ids.sort_unstable();
        self.commit_authorized_constraint_ddl(
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => TableLifecycleError::Unauthorized,
            error => TableLifecycleError::Commit(error),
        })
    }

    /// Add a UNIQUE constraint and its owned unique index. The authorized
    /// constraint commit backfills and validates the index against the final
    /// table state under the table DDL fence.
    pub fn create_unique_constraint(
        &self,
        request: CreateUniqueConstraintRequest,
    ) -> Result<CreatedUniqueConstraint, TableLifecycleError> {
        self.create_key_constraint(request, 3, None)
    }

    pub fn create_named_unique_constraint(
        &self,
        request: CreateUniqueConstraintRequest,
        name: String,
    ) -> Result<CreatedUniqueConstraint, TableLifecycleError> {
        if !is_normalized_principal_name(&name) {
            return Err(TableLifecycleError::InvalidRequest);
        }
        self.create_key_constraint(request, 3, Some(name))
    }

    /// Add a PRIMARY KEY and its owned unique index. Existing rows are
    /// validated and the index is backfilled while the table DDL fence is held.
    pub fn create_primary_key(
        &self,
        request: CreatePrimaryKeyRequest,
    ) -> Result<CreatedPrimaryKey, TableLifecycleError> {
        let created = self.create_key_constraint(
            CreateUniqueConstraintRequest {
                invocation_id: request.invocation_id,
                authenticated_actor_id: request.authenticated_actor_id,
                table_id: request.table_id,
                column_ids: request.column_ids,
                retention_deadline_us: request.retention_deadline_us,
                audit_event: request.audit_event,
            },
            2,
            None,
        )?;
        Ok(CreatedPrimaryKey {
            constraint_id: created.constraint_id,
            index_id: created.index_id,
            commit_sequence: created.commit_sequence,
        })
    }

    pub fn create_named_primary_key(
        &self,
        request: CreatePrimaryKeyRequest,
        name: String,
    ) -> Result<CreatedPrimaryKey, TableLifecycleError> {
        if !is_normalized_principal_name(&name) {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let created = self.create_key_constraint(
            CreateUniqueConstraintRequest {
                invocation_id: request.invocation_id,
                authenticated_actor_id: request.authenticated_actor_id,
                table_id: request.table_id,
                column_ids: request.column_ids,
                retention_deadline_us: request.retention_deadline_us,
                audit_event: request.audit_event,
            },
            2,
            Some(name),
        )?;
        Ok(CreatedPrimaryKey {
            constraint_id: created.constraint_id,
            index_id: created.index_id,
            commit_sequence: created.commit_sequence,
        })
    }

    pub(super) fn create_key_constraint(
        &self,
        request: CreateUniqueConstraintRequest,
        constraint_kind: u16,
        constraint_name: Option<String>,
    ) -> Result<CreatedUniqueConstraint, TableLifecycleError> {
        let CreateUniqueConstraintRequest {
            invocation_id,
            authenticated_actor_id,
            table_id,
            column_ids,
            retention_deadline_us,
            mut audit_event,
        } = request;
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || column_ids.is_empty()
            || column_ids.len() > crate::tuple::MAX_COLUMNS
            || column_ids.contains(&[0; 16])
            || column_ids.iter().copied().collect::<HashSet<_>>().len() != column_ids.len()
            || retention_deadline_us <= 0
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

        let constraint_id = self.generate_table_object_id()?;
        let mut index_id = self.generate_table_object_id()?;
        for _ in 0..8 {
            if index_id != constraint_id {
                break;
            }
            index_id = self.generate_table_object_id()?;
        }
        if index_id == constraint_id {
            return Err(TableLifecycleError::ObjectIdGenerationFailed);
        }

        let read = self
            .database
            .begin_read()
            .map_err(|_| TableLifecycleError::Storage)?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|_| TableLifecycleError::Storage)?;
        let table_key = id_catalog_key(5, table_id);
        let table_bytes = catalog
            .get(table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        read_catalog_record(&table_key, &table_bytes, 5)?;
        let schema_version = read_catalog_u64(&table_key, &table_bytes, 5, 2)?;
        let table_epoch = read_catalog_u64(&table_key, &table_bytes, 5, 3)?;
        let next_table_epoch = table_epoch
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;

        let table_object_key = id_catalog_key(2, table_id);
        let table_object_bytes = catalog
            .get(table_object_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let owner_id = read_catalog_id(&table_object_key, &table_object_bytes, 2, 4)?;

        let encode_ids = |ids: &[[u8; 16]]| {
            let mut payload = (ids.len() as u32).to_le_bytes().to_vec();
            payload.extend(ids.iter().flatten());
            payload
        };
        let constraint_key = id_catalog_key(8, constraint_id);
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
                payload: encode_ids(&column_ids),
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
        let constraint_payload = encode_envelope(
            &family_schema(8).ok_or(TableLifecycleError::Storage)?,
            &constraint_key,
            &constraint_fields,
        )
        .map_err(|_| TableLifecycleError::InvalidRequest)?;

        let make_object_write =
            |object_id: [u8; 16], kind: u16, prefix: &str, name: Option<&str>| {
                let key = id_catalog_key(2, object_id);
                let name = name.map(str::to_owned).unwrap_or_else(|| {
                    format!(
                        "{prefix}_{}",
                        object_id
                            .iter()
                            .map(|byte| format!("{byte:02x}"))
                            .collect::<String>()
                    )
                });
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
                Ok::<_, TableLifecycleError>(CatalogWrite {
                    key,
                    expected_value: None,
                    payload: Some(payload),
                })
            };
        let index_key = id_catalog_key(9, index_id);
        let index_fields = [
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::Ids,
                payload: encode_ids(&column_ids),
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
                payload: schema_version.to_le_bytes().to_vec(),
            },
        ];
        let index_payload = encode_envelope(
            &family_schema(9).ok_or(TableLifecycleError::Storage)?,
            &index_key,
            &index_fields,
        )
        .map_err(|_| TableLifecycleError::InvalidRequest)?;
        let writes = [
            CatalogWrite {
                key: constraint_key,
                expected_value: None,
                payload: Some(constraint_payload),
            },
            make_object_write(
                constraint_id,
                6,
                if constraint_kind == 2 {
                    "primary_key_constraint"
                } else {
                    "unique_constraint"
                },
                constraint_name.as_deref(),
            )?,
            CatalogWrite {
                key: index_key,
                expected_value: None,
                payload: Some(index_payload),
            },
            make_object_write(
                index_id,
                7,
                if constraint_kind == 2 {
                    "primary_key_index"
                } else {
                    "unique_index"
                },
                None,
            )?,
            new_dependency_write(constraint_id, index_id, 4)?,
            CatalogWrite {
                key: table_key.clone(),
                expected_value: Some(table_bytes.clone()),
                payload: Some(rewrite_catalog_u64_field(
                    &table_key,
                    &table_bytes,
                    5,
                    3,
                    next_table_epoch,
                )?),
            },
        ];
        drop(catalog);
        drop(read);

        audit_event.target_ids = vec![constraint_id, index_id];
        audit_event.target_ids.sort_unstable();
        let commit_sequence = self
            .commit_authorized_constraint_ddl(
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
        Ok(CreatedUniqueConstraint {
            constraint_id,
            index_id,
            commit_sequence,
        })
    }

    /// Drop one foreign-key constraint and its dependency edge atomically.
    /// Both participating tables' write epochs advance so invocations pinned
    /// to the old referential graph cannot publish afterward.
    pub fn drop_foreign_key(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        foreign_key_id: [u8; 16],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, CommitError> {
        if invocation_id == [0; 16]
            || authenticated_principal_id == [0; 16]
            || foreign_key_id == [0; 16]
            || retention_deadline_us <= 0
        {
            return Err(CommitError::InvalidWrite);
        }
        if audit_event.actor_id != Some(authenticated_principal_id)
            || audit_event.correlation_id != invocation_id
            || audit_event.action != AuditAction::Ddl
            || audit_event.result != AuditResult::Succeeded
        {
            return Err(CommitError::InvalidAudit);
        }

        let read = self
            .database
            .begin_read()
            .map_err(|error| CommitError::Storage(error.to_string()))?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|error| CommitError::Storage(error.to_string()))?;
        let foreign_key_key = id_catalog_key(8, foreign_key_id);
        let foreign_key_bytes = catalog
            .get(foreign_key_key.as_slice())
            .map_err(|error| CommitError::Storage(error.to_string()))?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        if read_catalog_u16(&foreign_key_key, &foreign_key_bytes, 8, 2)
            .map_err(table_lifecycle_commit_error)?
            != 5
        {
            return Err(CommitError::InvalidCatalog);
        }
        let source_table_id = read_catalog_id(&foreign_key_key, &foreign_key_bytes, 8, 1)
            .map_err(table_lifecycle_commit_error)?;
        let target_constraint_id = read_catalog_id(&foreign_key_key, &foreign_key_bytes, 8, 5)
            .map_err(table_lifecycle_commit_error)?;
        let target_constraint_key = id_catalog_key(8, target_constraint_id);
        let target_constraint_bytes = catalog
            .get(target_constraint_key.as_slice())
            .map_err(|error| CommitError::Storage(error.to_string()))?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        if !matches!(
            read_catalog_u16(&target_constraint_key, &target_constraint_bytes, 8, 2)
                .map_err(table_lifecycle_commit_error)?,
            2 | 3
        ) {
            return Err(CommitError::InvalidCatalog);
        }
        let target_table_id =
            read_catalog_id(&target_constraint_key, &target_constraint_bytes, 8, 1)
                .map_err(table_lifecycle_commit_error)?;

        let object_key = id_catalog_key(2, foreign_key_id);
        let object_bytes = catalog
            .get(object_key.as_slice())
            .map_err(|error| CommitError::Storage(error.to_string()))?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;
        if read_catalog_u16(&object_key, &object_bytes, 2, 1)
            .map_err(table_lifecycle_commit_error)?
            != 6
        {
            return Err(CommitError::InvalidCatalog);
        }
        let dependency_key = dependency_catalog_key(foreign_key_id, target_constraint_id, 3);
        let dependency_bytes = catalog
            .get(dependency_key.as_slice())
            .map_err(|error| CommitError::Storage(error.to_string()))?
            .map(|value| value.value().to_vec())
            .ok_or(CommitError::InvalidCatalog)?;

        let mut writes = vec![
            CatalogWrite {
                key: foreign_key_key,
                expected_value: Some(foreign_key_bytes),
                payload: None,
            },
            CatalogWrite {
                key: object_key,
                expected_value: Some(object_bytes),
                payload: None,
            },
            CatalogWrite {
                key: dependency_key,
                expected_value: Some(dependency_bytes),
                payload: None,
            },
        ];
        let mut table_ids = vec![source_table_id, target_table_id];
        table_ids.sort_unstable();
        table_ids.dedup();
        for table_id in table_ids {
            let table_key = id_catalog_key(5, table_id);
            let table_bytes = catalog
                .get(table_key.as_slice())
                .map_err(|error| CommitError::Storage(error.to_string()))?
                .map(|value| value.value().to_vec())
                .ok_or(CommitError::InvalidCatalog)?;
            let next_epoch = read_catalog_u64(&table_key, &table_bytes, 5, 3)
                .map_err(table_lifecycle_commit_error)?
                .checked_add(1)
                .ok_or(CommitError::InvalidCatalog)?;
            writes.push(CatalogWrite {
                key: table_key.clone(),
                expected_value: Some(table_bytes.clone()),
                payload: Some(
                    rewrite_catalog_u64_field(&table_key, &table_bytes, 5, 3, next_epoch)
                        .map_err(table_lifecycle_commit_error)?,
                ),
            });
        }
        drop(catalog);
        drop(read);

        audit_event.target_ids = vec![foreign_key_id];
        self.commit_authorized_foreign_key_ddl(
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            &writes,
            &[audit_event],
        )
    }

    /// Authorize NOT NULL, PRIMARY KEY, UNIQUE, and CHECK constraint add/drop
    /// for an existing table. Key constraints atomically manage their owned
    /// unique backing indexes; additions validate stored rows before
    /// publication. The actor must own the table or hold inherited ALTER on
    /// the table/schema.
    pub fn commit_authorized_constraint_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        let mut writes = catalog_writes.to_vec();
        let mut retired_ids = HashSet::new();
        for write in catalog_writes {
            if write.key.starts_with(&17u16.to_be_bytes())
                && let Some(id) = write.key.get(3..19).and_then(|bytes| bytes.try_into().ok())
            {
                retired_ids.insert(id);
            }
            if !write.key.starts_with(&2u16.to_be_bytes()) || write.payload.is_some() {
                continue;
            }
            let Some(id) = write.key.get(3..19).and_then(|bytes| bytes.try_into().ok()) else {
                continue;
            };
            let Some(bytes) = write.expected_value.as_deref() else {
                continue;
            };
            let record = read_catalog_record(&write.key, bytes, 2)
                .map_err(|_| CommitError::InvalidCatalog)?;
            let kind = record
                .fields
                .iter()
                .find(|field| field.tag == 1 && field.wire_type == WireType::U16)
                .and_then(|field| <[u8; 2]>::try_from(field.payload.as_slice()).ok())
                .map(u16::from_le_bytes);
            if kind.is_some_and(|kind| (5..=7).contains(&kind)) {
                retired_ids.insert(id);
            }
        }
        for id in retired_ids {
            let key = retired_object_id_catalog_key(id);
            if writes.iter().any(|write| write.key == key) {
                continue;
            }
            writes.push(CatalogWrite {
                payload: Some(
                    encode_envelope(
                        &family_schema(17).ok_or(CommitError::InvalidCatalog)?,
                        &key,
                        &[],
                    )
                    .map_err(|_| CommitError::InvalidCatalog)?,
                ),
                key,
                expected_value: None,
            });
        }
        commit_authorized_constraint_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            &writes,
            audit_events,
        )
    }
}
