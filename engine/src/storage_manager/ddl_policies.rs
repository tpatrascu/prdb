use super::catalog_helpers::*;
use super::*;
use crate::audit::{AuditAction, AuditEvent, AuditResult};
use crate::authorization::{Effect, Operation, Subject};
use crate::catalog::{
    CatalogField, PolicyTarget, WireType, decode_builtin_scalar_type, decode_policy_target,
    encode_envelope, encode_policy_target, family_schema,
};
use crate::commit::{CatalogWrite, CommitError, commit_authorized_policy_ddl_with_fences};
use crate::schema::CATALOG;
use crate::snapshot::MAX_CATALOG_FAMILY_SCAN;
use crate::tuple::{ColumnSchema, ScalarType};
use redb::{ReadableDatabase, ReadableTable};
use std::collections::HashSet;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PolicyKind {
    Row,
    Cell,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatePolicyRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub table_id: [u8; 16],
    pub name: String,
    pub kind: PolicyKind,
    pub operation: Operation,
    pub effect: Effect,
    pub recipients: Vec<Subject>,
    pub target_column_ids: Vec<[u8; 16]>,
    pub using_expression: Option<crate::authorization::Expr>,
    pub with_check_expression: Option<crate::authorization::Expr>,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedPolicy {
    pub policy_id: [u8; 16],
    pub using_expression_id: Option<[u8; 16]>,
    pub with_check_expression_id: Option<[u8; 16]>,
    pub commit_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DropPolicyRequest {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub policy_id: [u8; 16],
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

pub(super) struct PolicyDropWriteSet {
    table_id: [u8; 16],
    kind: u16,
    writes: Vec<CatalogWrite>,
    expression_ids: Vec<[u8; 16]>,
}

impl<'db> StorageManager<'db> {
    /// Authorize and commit row/cell policy DDL for an actor authenticated by
    /// the trusted administrative channel. The actor must own each affected
    /// table or hold inherited `MANAGE_POLICIES` on it.
    pub fn commit_authorized_policy_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_authorized_policy_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    pub(super) fn collect_policy_drop_writes(
        catalog: &impl ReadableTable<&'static [u8], &'static [u8]>,
        policy_id: [u8; 16],
    ) -> Result<PolicyDropWriteSet, TableLifecycleError> {
        let policy_key = id_catalog_key(13, policy_id);
        let policy_bytes = catalog
            .get(policy_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        let policy_record = read_catalog_record(&policy_key, &policy_bytes, 13)?;
        let table_id = read_catalog_id(&policy_key, &policy_bytes, 13, 1)?;
        let kind = read_catalog_u16(&policy_key, &policy_bytes, 13, 2)?;
        if !matches!(kind, 1 | 2) {
            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
        }

        let mut writes = Vec::<CatalogWrite>::new();
        let mut expression_ids = Vec::new();
        for (tag, expected_purpose) in [
            (5u16, crate::expression::ExpressionPurpose::PolicyUsing),
            (6u16, crate::expression::ExpressionPurpose::PolicyCheck),
        ] {
            let Some(field) = policy_record.fields.iter().find(|field| field.tag == tag) else {
                continue;
            };
            let expression_id: [u8; 16] = field
                .payload
                .as_slice()
                .try_into()
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let expression_key = id_catalog_key(7, expression_id);
            let expression_bytes = catalog
                .get(expression_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let expression = crate::expression::decode_catalog_expression(&expression_bytes)
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            if expression.purpose != expected_purpose {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            writes.push(CatalogWrite {
                key: expression_key,
                expected_value: Some(expression_bytes),
                payload: None,
            });
            let expression_object_key = id_catalog_key(2, expression_id);
            let expression_object = catalog
                .get(expression_object_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            writes.push(CatalogWrite {
                key: expression_object_key,
                expected_value: Some(expression_object),
                payload: None,
            });
            let policy_expression_edge = dependency_catalog_key(policy_id, expression_id, 2);
            let policy_expression_value = catalog
                .get(policy_expression_edge.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            writes.push(CatalogWrite {
                key: policy_expression_edge,
                expected_value: Some(policy_expression_value),
                payload: None,
            });
            for dependency_id in expression.dependencies {
                let key = dependency_catalog_key(expression_id, dependency_id, 2);
                let value = catalog
                    .get(key.as_slice())
                    .map_err(|_| TableLifecycleError::Storage)?
                    .map(|value| value.value().to_vec())
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                writes.push(CatalogWrite {
                    key,
                    expected_value: Some(value),
                    payload: None,
                });
            }
            expression_ids.push(expression_id);
        }

        let target_prefix = 14u16.to_be_bytes();
        let mut scanned_targets = 0usize;
        let mut has_recipient = false;
        for entry in catalog
            .range(target_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value().to_vec();
            if !key.starts_with(&target_prefix) {
                break;
            }
            scanned_targets += 1;
            if scanned_targets > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            if key.len() != 39 {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            if key[3..19] != policy_id {
                continue;
            }
            let target = decode_policy_target(value.value())
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            if target.policy_id != policy_id {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            if matches!(target.kind, 1 | 2) {
                has_recipient = true;
                let edge_key = dependency_catalog_key(policy_id, target.target_id, 5);
                let edge_value = catalog
                    .get(edge_key.as_slice())
                    .map_err(|_| TableLifecycleError::Storage)?
                    .map(|value| value.value().to_vec())
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                writes.push(CatalogWrite {
                    key: edge_key,
                    expected_value: Some(edge_value),
                    payload: None,
                });
            }
            writes.push(CatalogWrite {
                key,
                expected_value: Some(value.value().to_vec()),
                payload: None,
            });
        }
        if !has_recipient {
            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
        }
        for (key, value) in [
            (policy_key, policy_bytes),
            (
                id_catalog_key(2, policy_id),
                catalog
                    .get(id_catalog_key(2, policy_id).as_slice())
                    .map_err(|_| TableLifecycleError::Storage)?
                    .map(|value| value.value().to_vec())
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?,
            ),
        ] {
            writes.push(CatalogWrite {
                key,
                expected_value: Some(value),
                payload: None,
            });
        }
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
        Ok(PolicyDropWriteSet {
            table_id,
            kind,
            writes,
            expression_ids,
        })
    }

    /// Create a row or cell policy and its expression/recipient records as one
    /// authorized table DDL commit.
    pub fn create_policy(
        &self,
        request: CreatePolicyRequest,
    ) -> Result<CreatedPolicy, TableLifecycleError> {
        self.create_policy_inner(request, None)
    }

    /// Replace an existing same-name policy in the same catalog commit that
    /// publishes its successor. The old policy is removed from the final
    /// catalog view before duplicate-name validation and the table epoch
    /// advances once for the complete replacement.
    pub fn replace_policy(
        &self,
        request: CreatePolicyRequest,
        replaced_policy_id: [u8; 16],
    ) -> Result<CreatedPolicy, TableLifecycleError> {
        if replaced_policy_id == [0; 16] {
            return Err(TableLifecycleError::InvalidRequest);
        }
        self.create_policy_inner(request, Some(replaced_policy_id))
    }

    pub(super) fn create_policy_inner(
        &self,
        request: CreatePolicyRequest,
        replaced_policy_id: Option<[u8; 16]>,
    ) -> Result<CreatedPolicy, TableLifecycleError> {
        let CreatePolicyRequest {
            invocation_id,
            authenticated_actor_id,
            table_id,
            name,
            kind,
            operation,
            effect,
            recipients,
            target_column_ids,
            using_expression,
            with_check_expression,
            retention_deadline_us,
            mut audit_event,
        } = request;
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || table_id == [0; 16]
            || retention_deadline_us <= 0
            || !is_normalized_principal_name(&name)
            || recipients.is_empty()
            || recipients.len() > crate::authorization::MAX_POLICY_SUBJECTS
            || recipients.iter().copied().collect::<HashSet<_>>().len() != recipients.len()
            || recipients.iter().any(|subject| match subject {
                Subject::Principal(id) | Subject::Role(id) => *id == [0; 16],
            })
            || target_column_ids.len() > crate::tuple::MAX_COLUMNS
            || target_column_ids.contains(&[0; 16])
            || target_column_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                .len()
                != target_column_ids.len()
            || (kind == PolicyKind::Row && !target_column_ids.is_empty())
            || (kind == PolicyKind::Cell && target_column_ids.is_empty())
            || (kind == PolicyKind::Cell && operation == Operation::Delete)
            || !match operation {
                Operation::Select | Operation::Delete => {
                    using_expression.is_some() && with_check_expression.is_none()
                }
                Operation::Insert => using_expression.is_none() && with_check_expression.is_some(),
                Operation::Update => using_expression.is_some() && with_check_expression.is_some(),
                Operation::Usage => false,
            }
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

        let type_definitions = load_builtin_type_definitions(self.database)?;
        let boolean_type_id = type_definitions
            .get(&1)
            .map(|(type_id, _)| *type_id)
            .ok_or(TableLifecycleError::Storage)?;
        let mut column_records = Vec::<(u32, [u8; 16], ColumnSchema)>::new();
        let column_prefix = 6u16.to_be_bytes();
        let mut scanned_columns = 0usize;
        for entry in catalog
            .range(column_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value();
            if !key.starts_with(&column_prefix) {
                break;
            }
            scanned_columns += 1;
            if scanned_columns > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            let bytes = value.value().to_vec();
            if read_catalog_id(key, &bytes, 6, 1)? != table_id {
                continue;
            }
            let column_id: [u8; 16] = key
                .get(3..19)
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?
                .try_into()
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let type_id = read_catalog_id(key, &bytes, 6, 4)?;
            let parameters: [u8; 8] = read_catalog_field(key, &bytes, 6, 5)?
                .as_slice()
                .try_into()
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let type_record = type_definitions
                .values()
                .find(|(candidate_id, _)| *candidate_id == type_id)
                .map(|(_, record)| record)
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let scalar_type = decode_builtin_scalar_type(type_record, &parameters)
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let nullable = match read_catalog_field(key, &bytes, 6, 6)?.as_slice() {
                [0] => false,
                [1] => true,
                _ => return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog)),
            };
            column_records.push((
                read_catalog_u32(key, &bytes, 6, 2)?,
                column_id,
                ColumnSchema {
                    scalar_type,
                    nullable,
                },
            ));
        }
        column_records.sort_by_key(|(ordinal, _, _)| *ordinal);
        let column_ids = column_records
            .iter()
            .map(|(_, id, _)| *id)
            .collect::<Vec<_>>();
        let columns = column_records
            .iter()
            .map(|(_, _, column)| column.clone())
            .collect::<Vec<_>>();
        if target_column_ids
            .iter()
            .any(|column_id| !column_ids.contains(column_id))
        {
            return Err(TableLifecycleError::InvalidRequest);
        }

        for subject in &recipients {
            let (principal_id, expected_kind) = match subject {
                Subject::Principal(id) => (*id, 1),
                Subject::Role(id) => (*id, 2),
            };
            let principal_key = id_catalog_key(10, principal_id);
            let principal_bytes = catalog
                .get(principal_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::InvalidRequest)?;
            if read_catalog_u16(&principal_key, &principal_bytes, 10, 1)? != expected_kind {
                return Err(TableLifecycleError::InvalidRequest);
            }
        }

        let mut policy_scan = 0usize;
        let mut same_name_policy_id = None;
        let policy_prefix = 13u16.to_be_bytes();
        for entry in catalog
            .range(policy_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value();
            if !key.starts_with(&policy_prefix) {
                break;
            }
            policy_scan += 1;
            if policy_scan > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            let bytes = value.value().to_vec();
            if read_catalog_id(key, &bytes, 13, 1)? != table_id {
                continue;
            }
            let policy_id: [u8; 16] = key
                .get(3..19)
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?
                .try_into()
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let object_key = id_catalog_key(2, policy_id);
            let object_bytes = catalog
                .get(object_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            if read_catalog_field(&object_key, &object_bytes, 2, 3)? == name.as_bytes()
                && same_name_policy_id.replace(policy_id).is_some()
            {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
        }
        if same_name_policy_id != replaced_policy_id {
            return Err(TableLifecycleError::InvalidRequest);
        }
        let mut replaced_expression_ids = Vec::new();
        let mut writes = if let Some(policy_id) = replaced_policy_id {
            let drop_write_set = Self::collect_policy_drop_writes(&catalog, policy_id)?;
            if drop_write_set.table_id != table_id || !matches!(drop_write_set.kind, 1 | 2) {
                return Err(TableLifecycleError::InvalidRequest);
            }
            replaced_expression_ids = drop_write_set.expression_ids;
            drop_write_set.writes
        } else {
            Vec::new()
        };

        let mut generated = HashSet::new();
        let policy_id = self.generate_table_object_id()?;
        if replaced_policy_id == Some(policy_id) {
            return Err(TableLifecycleError::ObjectIdGenerationFailed);
        }
        generated.insert(policy_id);
        let mut generated_expression_id = || -> Result<[u8; 16], TableLifecycleError> {
            for _ in 0..8 {
                let candidate = self.generate_table_object_id()?;
                if generated.insert(candidate) {
                    return Ok(candidate);
                }
            }
            Err(TableLifecycleError::ObjectIdGenerationFailed)
        };
        let using_expression_id = using_expression
            .as_ref()
            .map(|_| generated_expression_id())
            .transpose()?;
        let with_check_expression_id = with_check_expression
            .as_ref()
            .map(|_| generated_expression_id())
            .transpose()?;

        let kind_code = match kind {
            PolicyKind::Row => 1u16,
            PolicyKind::Cell => 2u16,
        };
        let operation_code = match operation {
            Operation::Select => 1u16,
            Operation::Insert => 2u16,
            Operation::Update => 3u16,
            Operation::Delete => 4u16,
            Operation::Usage => return Err(TableLifecycleError::InvalidRequest),
        };
        let effect_code = match effect {
            Effect::Allow => 1u16,
            Effect::Restrict => 2u16,
        };
        let policy_key = id_catalog_key(13, policy_id);
        let policy_id_hex = policy_id
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let mut policy_fields = vec![
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: kind_code.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::U16,
                payload: operation_code.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::U16,
                payload: effect_code.to_le_bytes().to_vec(),
            },
        ];
        for (expression, expression_id, purpose, tag) in [
            (
                using_expression.as_ref(),
                using_expression_id,
                crate::expression::ExpressionPurpose::PolicyUsing,
                5,
            ),
            (
                with_check_expression.as_ref(),
                with_check_expression_id,
                crate::expression::ExpressionPurpose::PolicyCheck,
                6,
            ),
        ] {
            let (Some(expression), Some(expression_id)) = (expression, expression_id) else {
                continue;
            };
            let dependencies = policy_expression_dependencies(expression, &column_ids)?;
            for dependency_id in dependencies
                .iter()
                .filter(|dependency_id| !column_ids.contains(dependency_id))
            {
                let role_key = id_catalog_key(10, *dependency_id);
                let role_bytes = catalog
                    .get(role_key.as_slice())
                    .map_err(|_| TableLifecycleError::Storage)?
                    .map(|value| value.value().to_vec())
                    .ok_or(TableLifecycleError::InvalidRequest)?;
                if read_catalog_u16(&role_key, &role_bytes, 10, 1)? != 2 {
                    return Err(TableLifecycleError::InvalidRequest);
                }
            }
            let catalog_expression = crate::expression::CatalogExpression {
                expression_id,
                purpose,
                result_type_id: boolean_type_id,
                result_parameters: [0; 8],
                dependencies: dependencies.clone(),
                expression: expression.clone(),
            };
            crate::expression::validate_expression_binding(
                &catalog_expression,
                &columns,
                &column_ids,
                boolean_type_id,
                ScalarType::Boolean,
            )
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
            let expression_bytes = crate::expression::encode_expression(expression)
                .map_err(|_| TableLifecycleError::InvalidRequest)?;
            let expression_key = id_catalog_key(7, expression_id);
            let mut dependency_payload = (dependencies.len() as u32).to_le_bytes().to_vec();
            for dependency_id in &dependencies {
                dependency_payload.extend_from_slice(dependency_id);
            }
            let expression_payload = encode_envelope(
                &family_schema(7).ok_or(TableLifecycleError::Storage)?,
                &expression_key,
                &[
                    CatalogField {
                        tag: 1,
                        wire_type: WireType::U16,
                        payload: match purpose {
                            crate::expression::ExpressionPurpose::PolicyUsing => 3u16,
                            crate::expression::ExpressionPurpose::PolicyCheck => 4u16,
                            _ => unreachable!(),
                        }
                        .to_le_bytes()
                        .to_vec(),
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
                        payload: [0; 8].to_vec(),
                    },
                    CatalogField {
                        tag: 5,
                        wire_type: WireType::Bytes,
                        payload: expression_bytes,
                    },
                    CatalogField {
                        tag: 6,
                        wire_type: WireType::Ids,
                        payload: dependency_payload,
                    },
                ],
            )
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
            let object_key = id_catalog_key(2, expression_id);
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
                                payload: 5u16.to_le_bytes().to_vec(),
                            },
                            CatalogField {
                                tag: 3,
                                wire_type: WireType::Text,
                                payload: format!("policy_{policy_id_hex}_{tag}").into_bytes(),
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
                    .map_err(|_| TableLifecycleError::InvalidRequest)?,
                ),
            });
            writes.push(CatalogWrite {
                key: expression_key,
                expected_value: None,
                payload: Some(expression_payload),
            });
            writes.push(new_dependency_write(policy_id, expression_id, 2)?);
            for dependency_id in dependencies {
                writes.push(new_dependency_write(expression_id, dependency_id, 2)?);
            }
            policy_fields.push(CatalogField {
                tag,
                wire_type: WireType::Id,
                payload: expression_id.to_vec(),
            });
        }
        policy_fields.push(CatalogField {
            tag: 7,
            wire_type: WireType::U64,
            payload: 1u64.to_le_bytes().to_vec(),
        });
        let policy_payload = encode_envelope(
            &family_schema(13).ok_or(TableLifecycleError::Storage)?,
            &policy_key,
            &policy_fields,
        )
        .map_err(|_| TableLifecycleError::InvalidRequest)?;
        writes.push(CatalogWrite {
            key: policy_key,
            expected_value: None,
            payload: Some(policy_payload),
        });
        let policy_object_key = id_catalog_key(2, policy_id);
        writes.push(CatalogWrite {
            key: policy_object_key.clone(),
            expected_value: None,
            payload: Some(
                encode_envelope(
                    &family_schema(2).ok_or(TableLifecycleError::Storage)?,
                    &policy_object_key,
                    &[
                        CatalogField {
                            tag: 1,
                            wire_type: WireType::U16,
                            payload: 10u16.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 3,
                            wire_type: WireType::Text,
                            payload: name.as_bytes().to_vec(),
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
                .map_err(|_| TableLifecycleError::InvalidRequest)?,
            ),
        });
        for recipient in recipients {
            let (target_kind, target_id) = match recipient {
                Subject::Principal(id) => (1, id),
                Subject::Role(id) => (2, id),
            };
            let payload = encode_policy_target(&PolicyTarget {
                policy_id,
                kind: target_kind,
                target_id,
            })
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
            writes.push(CatalogWrite {
                key: payload[32..].to_vec(),
                expected_value: None,
                payload: Some(payload),
            });
            writes.push(new_dependency_write(policy_id, target_id, 5)?);
        }
        for column_id in target_column_ids {
            let payload = encode_policy_target(&PolicyTarget {
                policy_id,
                kind: 3,
                target_id: column_id,
            })
            .map_err(|_| TableLifecycleError::InvalidRequest)?;
            writes.push(CatalogWrite {
                key: payload[32..].to_vec(),
                expected_value: None,
                payload: Some(payload),
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
        drop(catalog);
        drop(read);

        audit_event.target_ids = vec![policy_id];
        audit_event.target_ids.extend(replaced_policy_id);
        audit_event.target_ids.extend(replaced_expression_ids);
        audit_event.target_ids.extend(using_expression_id);
        audit_event.target_ids.extend(with_check_expression_id);
        audit_event.target_ids.sort_unstable();
        let commit_sequence = self
            .commit_authorized_policy_ddl(
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
        Ok(CreatedPolicy {
            policy_id,
            using_expression_id,
            with_check_expression_id,
            commit_sequence,
        })
    }

    /// Remove one policy, its targets and private expressions under the
    /// authorized policy DDL path, advancing the table write epoch atomically.
    pub fn drop_policy(&self, request: DropPolicyRequest) -> Result<u64, TableLifecycleError> {
        let DropPolicyRequest {
            invocation_id,
            authenticated_actor_id,
            policy_id,
            retention_deadline_us,
            mut audit_event,
        } = request;
        if invocation_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || policy_id == [0; 16]
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
        let policy_key = id_catalog_key(13, policy_id);
        let policy_bytes = catalog
            .get(policy_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::InvalidRequest)?;
        let policy_record = read_catalog_record(&policy_key, &policy_bytes, 13)?;
        let table_id = read_catalog_id(&policy_key, &policy_bytes, 13, 1)?;
        let kind = read_catalog_u16(&policy_key, &policy_bytes, 13, 2)?;
        if !matches!(kind, 1 | 2) {
            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
        }
        let table_key = id_catalog_key(5, table_id);
        let table_bytes = catalog
            .get(table_key.as_slice())
            .map_err(|_| TableLifecycleError::Storage)?
            .map(|value| value.value().to_vec())
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let next_table_epoch = read_catalog_u64(&table_key, &table_bytes, 5, 3)?
            .checked_add(1)
            .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
        let mut writes = Vec::<CatalogWrite>::new();
        let mut expression_ids = Vec::new();
        for (tag, expected_purpose) in [
            (5u16, crate::expression::ExpressionPurpose::PolicyUsing),
            (6u16, crate::expression::ExpressionPurpose::PolicyCheck),
        ] {
            let Some(field) = policy_record.fields.iter().find(|field| field.tag == tag) else {
                continue;
            };
            let expression_id: [u8; 16] = field
                .payload
                .as_slice()
                .try_into()
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let expression_key = id_catalog_key(7, expression_id);
            let expression_bytes = catalog
                .get(expression_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            let expression = crate::expression::decode_catalog_expression(&expression_bytes)
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            if expression.purpose != expected_purpose {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            writes.push(CatalogWrite {
                key: expression_key,
                expected_value: Some(expression_bytes),
                payload: None,
            });
            let expression_object_key = id_catalog_key(2, expression_id);
            let expression_object = catalog
                .get(expression_object_key.as_slice())
                .map_err(|_| TableLifecycleError::Storage)?
                .map(|value| value.value().to_vec())
                .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            writes.push(CatalogWrite {
                key: expression_object_key,
                expected_value: Some(expression_object),
                payload: None,
            });
            writes.push(CatalogWrite {
                key: dependency_catalog_key(policy_id, expression_id, 2),
                expected_value: Some(
                    encode_envelope(
                        &family_schema(15).ok_or(TableLifecycleError::Storage)?,
                        &dependency_catalog_key(policy_id, expression_id, 2),
                        &[],
                    )
                    .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?,
                ),
                payload: None,
            });
            for dependency_id in expression.dependencies {
                let key = dependency_catalog_key(expression_id, dependency_id, 2);
                let value = catalog
                    .get(key.as_slice())
                    .map_err(|_| TableLifecycleError::Storage)?
                    .map(|value| value.value().to_vec())
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                writes.push(CatalogWrite {
                    key,
                    expected_value: Some(value),
                    payload: None,
                });
            }
            expression_ids.push(expression_id);
        }

        let target_prefix = 14u16.to_be_bytes();
        let mut scanned_targets = 0usize;
        let mut has_recipient = false;
        for entry in catalog
            .range(target_prefix.as_slice()..)
            .map_err(|_| TableLifecycleError::Storage)?
        {
            let (key, value) = entry.map_err(|_| TableLifecycleError::Storage)?;
            let key = key.value().to_vec();
            if !key.starts_with(&target_prefix) {
                break;
            }
            scanned_targets += 1;
            if scanned_targets > MAX_CATALOG_FAMILY_SCAN {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            if key.len() != 39 {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            if key[3..19] != policy_id {
                continue;
            }
            let target = decode_policy_target(value.value())
                .map_err(|_| TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
            if target.policy_id != policy_id {
                return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
            }
            if matches!(target.kind, 1 | 2) {
                has_recipient = true;
                let edge_key = dependency_catalog_key(policy_id, target.target_id, 5);
                let edge_value = catalog
                    .get(edge_key.as_slice())
                    .map_err(|_| TableLifecycleError::Storage)?
                    .map(|value| value.value().to_vec())
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?;
                writes.push(CatalogWrite {
                    key: edge_key,
                    expected_value: Some(edge_value),
                    payload: None,
                });
            }
            writes.push(CatalogWrite {
                key,
                expected_value: Some(value.value().to_vec()),
                payload: None,
            });
        }
        if !has_recipient {
            return Err(TableLifecycleError::Commit(CommitError::InvalidCatalog));
        }
        for (key, value) in [
            (policy_key.clone(), policy_bytes),
            (
                id_catalog_key(2, policy_id),
                catalog
                    .get(id_catalog_key(2, policy_id).as_slice())
                    .map_err(|_| TableLifecycleError::Storage)?
                    .map(|value| value.value().to_vec())
                    .ok_or(TableLifecycleError::Commit(CommitError::InvalidCatalog))?,
            ),
        ] {
            writes.push(CatalogWrite {
                key,
                expected_value: Some(value),
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
        audit_event.target_ids = std::iter::once(policy_id).chain(expression_ids).collect();
        audit_event.target_ids.sort_unstable();
        let commit_sequence = self
            .commit_authorized_policy_ddl(
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
}
