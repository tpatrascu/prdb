use prdb_engine::audit::{AuditAction, AuditEvent, AuditResult};
use prdb_engine::authorization::{
    AccessTarget, AuthorizationContext, AuthorizedProjection, Effect, Expr, Grant, Operation,
    Policy, SanitizedCell, Subject,
};
use prdb_engine::catalog::{CatalogField, WireType};
use prdb_engine::commit::{CatalogWrite, CommitError, commit_catalog_and_audit};
use prdb_engine::outcome::{InvocationOutcome, OutcomeStatus};
use prdb_engine::row_record::RowRecord;
use prdb_engine::schema::{CATALOG, PRINCIPAL_NAMES, ROWS};
use prdb_engine::transaction::{InvocationTransaction, TransactionError};
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, decode_tuple};
use redb::ReadableDatabase;

use super::helpers::*;

pub(super) fn invocation_loads_and_guards_catalog_table_schema()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_id = [131; 16];
    let schema_id = [132; 16];
    let storage_id = [133; 16];
    let varchar_type_id = [134; 16];
    let uuid_type_id = [135; 16];
    let column_text_id = [136; 16];
    let column_uuid_id = [137; 16];
    let principal_id = [128; 16];
    let boolean_type_id = [149; 16];
    let policy_id = [150; 16];
    let policy_expression_id = [161; 16];
    let role_id = [168; 16];
    let cell_policy_id = [169; 16];
    let cell_policy_expression_id = [170; 16];
    let cell_allow_policy_id = [171; 16];
    let cell_allow_expression_id = [172; 16];
    let check_constraint_id = [173; 16];
    let check_expression_id = [174; 16];
    let mut writes = Vec::new();
    let type_fields = |kind: u16, parameter_rule: u16, default_mask: u32| {
        vec![
            CatalogField {
                tag: 1,
                wire_type: WireType::U16,
                payload: kind.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: parameter_rule.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::U32,
                payload: default_mask.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
        ]
    };
    for (type_id, kind, parameter_rule, default_mask) in [
        (varchar_type_id, 7u16, 2u16, 1u32),
        (uuid_type_id, 9u16, 0u16, 7u32),
        (boolean_type_id, 1u16, 0u16, 1u32),
    ] {
        let key = catalog_id_key(4, type_id);
        writes.push(CatalogWrite {
            key: key.clone(),
            expected_value: None,
            payload: Some(catalog_record_payload(
                4,
                &key,
                &type_fields(kind, parameter_rule, default_mask),
            )?),
        });
    }
    let schema_key = catalog_id_key(3, schema_id);
    writes.push(CatalogWrite {
        key: schema_key.clone(),
        expected_value: None,
        payload: Some(catalog_record_payload(
            3,
            &schema_key,
            &[CatalogField {
                tag: 1,
                wire_type: WireType::Ids,
                payload: 0u32.to_le_bytes().to_vec(),
            }],
        )?),
    });
    let table_key = catalog_id_key(5, table_id);
    writes.push(CatalogWrite {
        key: table_key.clone(),
        expected_value: None,
        payload: Some(catalog_record_payload(
            5,
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
                    payload: 2u64.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::U64,
                    payload: 3u64.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 4,
                    wire_type: WireType::Id,
                    payload: storage_id.to_vec(),
                },
                CatalogField {
                    tag: 5,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
            ],
        )?),
    });
    for (column_id, name, ordinal, type_id, parameters) in [
        (
            column_text_id,
            b"object_136".as_slice(),
            1u32,
            varchar_type_id,
            {
                let mut bytes = [0; 8];
                bytes[4..].copy_from_slice(&32u32.to_le_bytes());
                bytes
            },
        ),
        (
            column_uuid_id,
            b"object_137".as_slice(),
            0u32,
            uuid_type_id,
            [0; 8],
        ),
    ] {
        let key = catalog_id_key(6, column_id);
        writes.push(CatalogWrite {
            key: key.clone(),
            expected_value: None,
            payload: Some(catalog_record_payload(
                6,
                &key,
                &[
                    CatalogField {
                        tag: 1,
                        wire_type: WireType::Id,
                        payload: table_id.to_vec(),
                    },
                    CatalogField {
                        tag: 2,
                        wire_type: WireType::U32,
                        payload: ordinal.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 3,
                        wire_type: WireType::Text,
                        payload: name.to_vec(),
                    },
                    CatalogField {
                        tag: 4,
                        wire_type: WireType::Id,
                        payload: type_id.to_vec(),
                    },
                    CatalogField {
                        tag: 5,
                        wire_type: WireType::Bytes,
                        payload: parameters.to_vec(),
                    },
                    CatalogField {
                        tag: 6,
                        wire_type: WireType::Bool,
                        payload: vec![u8::from(ordinal == 0)],
                    },
                    CatalogField {
                        tag: 8,
                        wire_type: WireType::U16,
                        payload: 0u16.to_le_bytes().to_vec(),
                    },
                    CatalogField {
                        tag: 9,
                        wire_type: WireType::U64,
                        payload: 1u64.to_le_bytes().to_vec(),
                    },
                ],
            )?),
        });
    }
    let (principal_key, principal_payload) =
        principal_catalog_record_for(principal_id, 1, b"reader", true, 1);
    writes.push(CatalogWrite {
        key: principal_key,
        expected_value: None,
        payload: Some(principal_payload),
    });
    let (principal_object_key, principal_object_payload) =
        principal_object_record_for(principal_id, 1);
    writes.push(CatalogWrite {
        key: principal_object_key,
        expected_value: None,
        payload: Some(principal_object_payload),
    });
    let (role_key, role_payload) =
        principal_catalog_record_for(role_id, 2, b"reader_role", false, 1);
    writes.push(CatalogWrite {
        key: role_key,
        expected_value: None,
        payload: Some(role_payload),
    });
    let (role_object_key, role_object_payload) = principal_object_record_for(role_id, 2);
    writes.push(CatalogWrite {
        key: role_object_key,
        expected_value: None,
        payload: Some(role_object_payload),
    });
    let (membership_key, membership_payload) =
        membership_catalog_record(principal_id, role_id, principal_id, 1);
    writes.push(CatalogWrite {
        key: membership_key,
        expected_value: None,
        payload: Some(membership_payload),
    });
    writes.push(dependency_catalog_write(principal_id, role_id, 5)?);
    let mut grant_authority_edges = std::collections::HashSet::new();
    for (privilege, scope, target) in [
        (1, 1, schema_id),
        (2, 1, table_id),
        (2, 2, column_uuid_id),
        (4, 2, column_uuid_id),
    ] {
        let (key, payload) = grant_catalog_record(
            principal_id,
            if scope == 1 { target } else { table_id },
            privilege,
            scope,
            target,
            principal_id,
            1,
        );
        writes.push(CatalogWrite {
            key,
            expected_value: None,
            payload: Some(payload),
        });
        if grant_authority_edges.insert((principal_id, target)) {
            writes.push(dependency_catalog_write(principal_id, target, 5)?);
        }
    }
    let (role_grant_key, role_grant_payload) =
        grant_catalog_record(role_id, table_id, 3, 2, column_text_id, principal_id, 1);
    writes.push(CatalogWrite {
        key: role_grant_key,
        expected_value: None,
        payload: Some(role_grant_payload),
    });
    writes.push(dependency_catalog_write(role_id, column_text_id, 5)?);
    let policy_expression_key = catalog_id_key(7, policy_expression_id);
    writes.push(CatalogWrite {
        key: policy_expression_key.clone(),
        expected_value: None,
        payload: Some(expression_catalog_payload_full(
            policy_expression_id,
            3,
            boolean_type_id,
            [0; 8],
            &[column_uuid_id],
            &Expr::Equal(Box::new(Expr::Field(0)), Box::new(Expr::CurrentPrincipalId)),
        )?),
    });
    let policy_key = catalog_id_key(13, policy_id);
    let policy_payload = catalog_record_payload(
        13,
        &policy_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::Id,
                payload: policy_expression_id.to_vec(),
            },
            CatalogField {
                tag: 7,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
        ],
    )?;
    writes.push(CatalogWrite {
        key: policy_key.clone(),
        expected_value: None,
        payload: Some(policy_payload.clone()),
    });
    let (policy_target_key, policy_target_payload) =
        policy_target_catalog_record(policy_id, 1, principal_id);
    writes.push(CatalogWrite {
        key: policy_target_key,
        expected_value: None,
        payload: Some(policy_target_payload),
    });
    writes.push(dependency_catalog_write(policy_id, principal_id, 5)?);
    let (role_policy_target_key, role_policy_target_payload) =
        policy_target_catalog_record(policy_id, 2, role_id);
    writes.push(CatalogWrite {
        key: role_policy_target_key,
        expected_value: None,
        payload: Some(role_policy_target_payload),
    });
    writes.push(dependency_catalog_write(policy_id, role_id, 5)?);
    let cell_policy_expression_key = catalog_id_key(7, cell_policy_expression_id);
    writes.push(CatalogWrite {
        key: cell_policy_expression_key.clone(),
        expected_value: None,
        payload: Some(expression_catalog_payload_full(
            cell_policy_expression_id,
            3,
            boolean_type_id,
            [0; 8],
            &[column_uuid_id],
            &Expr::IsNull(Box::new(Expr::Field(0))),
        )?),
    });
    let cell_policy_key = catalog_id_key(13, cell_policy_id);
    writes.push(CatalogWrite {
        key: cell_policy_key.clone(),
        expected_value: None,
        payload: Some(catalog_record_payload(
            13,
            &cell_policy_key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::Id,
                    payload: table_id.to_vec(),
                },
                CatalogField {
                    tag: 2,
                    wire_type: WireType::U16,
                    payload: 2u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 4,
                    wire_type: WireType::U16,
                    payload: 2u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 5,
                    wire_type: WireType::Id,
                    payload: cell_policy_expression_id.to_vec(),
                },
                CatalogField {
                    tag: 7,
                    wire_type: WireType::U64,
                    payload: 1u64.to_le_bytes().to_vec(),
                },
            ],
        )?),
    });
    for (target_kind, target_id) in [(2, role_id), (3, column_uuid_id)] {
        let (key, payload) = policy_target_catalog_record(cell_policy_id, target_kind, target_id);
        writes.push(CatalogWrite {
            key,
            expected_value: None,
            payload: Some(payload),
        });
        if target_kind == 2 {
            writes.push(dependency_catalog_write(cell_policy_id, target_id, 5)?);
        }
    }
    let check_expression_key = catalog_id_key(7, check_expression_id);
    writes.push(CatalogWrite {
        key: check_expression_key.clone(),
        expected_value: None,
        payload: Some(expression_catalog_payload_full(
            check_expression_id,
            2,
            boolean_type_id,
            [0; 8],
            &[column_uuid_id],
            &Expr::Equal(
                Box::new(Expr::Field(0)),
                Box::new(Expr::Literal(Some(ScalarValue::Uuid(principal_id)))),
            ),
        )?),
    });
    let (check_constraint_key, check_constraint_payload) =
        check_constraint_catalog_record(check_constraint_id, table_id, check_expression_id);
    writes.push(CatalogWrite {
        key: check_constraint_key,
        expected_value: None,
        payload: Some(check_constraint_payload),
    });
    let (not_null_constraint_key, not_null_constraint_payload) =
        not_null_constraint_catalog_record([180; 16], table_id, column_text_id);
    writes.push(CatalogWrite {
        key: not_null_constraint_key,
        expected_value: None,
        payload: Some(not_null_constraint_payload),
    });
    let cell_allow_expression_key = catalog_id_key(7, cell_allow_expression_id);
    writes.push(CatalogWrite {
        key: cell_allow_expression_key.clone(),
        expected_value: None,
        payload: Some(expression_catalog_payload_full(
            cell_allow_expression_id,
            3,
            boolean_type_id,
            [0; 8],
            &[],
            &Expr::Literal(Some(ScalarValue::Boolean(true))),
        )?),
    });
    let cell_allow_policy_key = catalog_id_key(13, cell_allow_policy_id);
    writes.push(CatalogWrite {
        key: cell_allow_policy_key.clone(),
        expected_value: None,
        payload: Some(catalog_record_payload(
            13,
            &cell_allow_policy_key,
            &[
                CatalogField {
                    tag: 1,
                    wire_type: WireType::Id,
                    payload: table_id.to_vec(),
                },
                CatalogField {
                    tag: 2,
                    wire_type: WireType::U16,
                    payload: 2u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 3,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 4,
                    wire_type: WireType::U16,
                    payload: 1u16.to_le_bytes().to_vec(),
                },
                CatalogField {
                    tag: 5,
                    wire_type: WireType::Id,
                    payload: cell_allow_expression_id.to_vec(),
                },
                CatalogField {
                    tag: 7,
                    wire_type: WireType::U64,
                    payload: 1u64.to_le_bytes().to_vec(),
                },
            ],
        )?),
    });
    for (target_kind, target_id) in [(2, role_id), (3, column_uuid_id)] {
        let (key, payload) =
            policy_target_catalog_record(cell_allow_policy_id, target_kind, target_id);
        writes.push(CatalogWrite {
            key,
            expected_value: None,
            payload: Some(payload),
        });
        if target_kind == 2 {
            writes.push(dependency_catalog_write(
                cell_allow_policy_id,
                target_id,
                5,
            )?);
        }
    }
    for (object_id, object_kind) in [
        (varchar_type_id, 2),
        (uuid_type_id, 2),
        (boolean_type_id, 2),
        (schema_id, 1),
        (table_id, 3),
        (column_text_id, 4),
        (column_uuid_id, 4),
        (policy_expression_id, 5),
        (cell_policy_expression_id, 5),
        (check_expression_id, 5),
        (cell_allow_expression_id, 5),
        (check_constraint_id, 6),
        ([180; 16], 6),
        (policy_id, 10),
        (cell_policy_id, 10),
        (cell_allow_policy_id, 10),
    ] {
        writes.push(object_catalog_write_for(object_id, object_kind));
    }
    for (dependent_id, referenced_id) in [
        (policy_expression_id, column_uuid_id),
        (cell_policy_expression_id, column_uuid_id),
        (check_expression_id, column_uuid_id),
        (policy_id, policy_expression_id),
        (cell_policy_id, cell_policy_expression_id),
        (check_constraint_id, check_expression_id),
        (cell_allow_policy_id, cell_allow_expression_id),
    ] {
        writes.push(dependency_catalog_write(dependent_id, referenced_id, 2)?);
    }
    let mut audit_targets = vec![
        principal_id,
        table_id,
        schema_id,
        storage_id,
        varchar_type_id,
        uuid_type_id,
        column_text_id,
        column_uuid_id,
        boolean_type_id,
        policy_id,
        policy_expression_id,
        role_id,
        cell_policy_id,
        cell_policy_expression_id,
        cell_allow_policy_id,
        cell_allow_expression_id,
        check_constraint_id,
        check_expression_id,
    ];
    audit_targets.sort_unstable();
    let audit = AuditEvent {
        event_id: [138; 16],
        actor_id: Some([9; 16]),
        correlation_id: [139; 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: audit_targets,
    };
    commit_catalog_and_audit(&database, [140; 16], [9; 16], 9_999, &writes, &[audit])?;

    let new_column_id = [164; 16];
    let new_column_key = catalog_id_key(6, new_column_id);
    let mut varchar_parameters = [0; 8];
    varchar_parameters[4..].copy_from_slice(&32u32.to_le_bytes());
    let new_column_payload = catalog_record_payload(
        6,
        &new_column_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U32,
                payload: 2u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Text,
                payload: format!("object_{}", new_column_id[0]).into_bytes(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Id,
                payload: varchar_type_id.to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::Bytes,
                payload: varchar_parameters.to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Bool,
                payload: vec![1],
            },
            CatalogField {
                tag: 8,
                wire_type: WireType::U16,
                payload: 0u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 9,
                wire_type: WireType::U64,
                payload: 2u64.to_le_bytes().to_vec(),
            },
        ],
    )?;
    let audit = AuditEvent {
        event_id: [165; 16],
        actor_id: Some(principal_id),
        correlation_id: [166; 16],
        occurred_at_us: 1_700_000_000_000_001,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![new_column_id],
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            [167; 16],
            principal_id,
            9_999,
            &[
                CatalogWrite {
                    key: new_column_key,
                    expected_value: None,
                    payload: Some(new_column_payload),
                },
                object_catalog_write_for(new_column_id, 4)
            ],
            &[audit],
        ),
        Err(CommitError::InvalidCatalog)
    );

    let (unbumped_constraint_key, unbumped_constraint_payload) =
        check_constraint_catalog_record([175; 16], table_id, check_expression_id);
    let unbumped_constraint_dependency =
        dependency_catalog_write([175; 16], check_expression_id, 2)?;
    let audit = AuditEvent {
        event_id: [176; 16],
        actor_id: Some(principal_id),
        correlation_id: [177; 16],
        occurred_at_us: 1_700_000_000_000_002,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![[175; 16]],
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            [178; 16],
            principal_id,
            9_999,
            &[
                CatalogWrite {
                    key: unbumped_constraint_key,
                    expected_value: None,
                    payload: Some(unbumped_constraint_payload),
                },
                object_catalog_write_for([175; 16], 6),
                unbumped_constraint_dependency,
            ],
            &[audit],
        ),
        Err(CommitError::InvalidCatalog)
    );

    let (_, new_policy_target) = policy_target_catalog_record(policy_id, 2, [162; 16]);
    let updated_policy_payload = catalog_record_payload(
        13,
        &policy_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::Id,
                payload: policy_expression_id.to_vec(),
            },
            CatalogField {
                tag: 7,
                wire_type: WireType::U64,
                payload: 2u64.to_le_bytes().to_vec(),
            },
        ],
    )?;
    let audit = AuditEvent {
        event_id: [145; 16],
        actor_id: Some(principal_id),
        correlation_id: [152; 16],
        occurred_at_us: 1_700_000_000_000_001,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![policy_id, [162; 16]],
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            [153; 16],
            principal_id,
            9_999,
            &[
                CatalogWrite {
                    key: policy_key.clone(),
                    expected_value: Some(policy_payload),
                    payload: Some(updated_policy_payload),
                },
                CatalogWrite {
                    key: policy_target_catalog_record(policy_id, 2, [162; 16]).0,
                    expected_value: None,
                    payload: Some(new_policy_target),
                },
            ],
            &[audit],
        ),
        Err(CommitError::InvalidCatalog)
    );

    let (unbumped_grant_key, unbumped_grant) =
        grant_catalog_record(principal_id, table_id, 3, 1, table_id, principal_id, 1);
    let audit = AuditEvent {
        event_id: [146; 16],
        actor_id: Some(principal_id),
        correlation_id: [147; 16],
        occurred_at_us: 1_700_000_000_000_001,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![principal_id, table_id],
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            [148; 16],
            principal_id,
            9_999,
            &[CatalogWrite {
                key: unbumped_grant_key,
                expected_value: None,
                payload: Some(unbumped_grant),
            }],
            &[audit],
        ),
        Err(CommitError::InvalidCatalog)
    );

    let mut invocation =
        InvocationTransaction::begin(&database, [141; 16], principal_id, 9_999, 1, 128)?;
    for (family, object_id) in [
        (4, varchar_type_id),
        (4, uuid_type_id),
        (5, table_id),
        (6, column_text_id),
        (6, column_uuid_id),
    ] {
        assert!(
            invocation
                .read_catalog_record(&catalog_id_key(family, object_id))?
                .is_some()
        );
    }
    let loaded = invocation.load_catalog_table_schema(table_id)?;
    let checks = invocation.load_catalog_check_constraints(&loaded)?;
    assert_eq!(
        checks,
        [Expr::Equal(
            Box::new(Expr::Field(0)),
            Box::new(Expr::Literal(Some(ScalarValue::Uuid(principal_id)))),
        )]
    );
    let (roles, grants) = invocation.load_catalog_grants(principal_id, &loaded)?;
    assert_eq!(roles, std::collections::HashSet::from([role_id]));
    assert_eq!(grants.len(), 5);
    assert!(grants.contains(&Grant {
        subject: Subject::Principal(principal_id),
        operation: Operation::Usage,
        target: AccessTarget::Schema(schema_id),
    }));
    assert!(grants.contains(&Grant {
        subject: Subject::Role(role_id),
        operation: Operation::Insert,
        target: AccessTarget::Column {
            table_id,
            column_id: column_text_id,
        },
    }));
    let policies = invocation.load_catalog_policies(principal_id, &loaded)?;
    assert_eq!(policies.len(), 3);
    assert!(policies.contains(&Policy {
        subjects: vec![Subject::Principal(principal_id), Subject::Role(role_id)],
        operation: Operation::Select,
        target: AccessTarget::Table(table_id),
        effect: Effect::Allow,
        using: Some(Expr::Equal(
            Box::new(Expr::Field(0)),
            Box::new(Expr::CurrentPrincipalId),
        )),
        with_check: None,
    }));
    assert!(policies.contains(&Policy {
        subjects: vec![Subject::Role(role_id)],
        operation: Operation::Select,
        target: AccessTarget::Column {
            table_id,
            column_id: column_uuid_id,
        },
        effect: Effect::Restrict,
        using: Some(Expr::IsNull(Box::new(Expr::Field(0)))),
        with_check: None,
    }));
    assert!(policies.contains(&Policy {
        subjects: vec![Subject::Role(role_id)],
        operation: Operation::Select,
        target: AccessTarget::Column {
            table_id,
            column_id: column_uuid_id,
        },
        effect: Effect::Allow,
        using: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        with_check: None,
    }));
    let query_context = AuthorizationContext {
        principal_id,
        role_ids: roles.clone(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (Operation::Select, AccessTarget::Table(table_id)),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id: column_uuid_id,
                },
            ),
        ]),
    };
    let projection = prdb_engine::authorization::project_row(
        &query_context,
        &grants,
        &policies,
        schema_id,
        table_id,
        &loaded.column_ids,
        &[
            Some(ScalarValue::Uuid(principal_id)),
            Some(ScalarValue::Varchar("private".to_owned())),
        ],
    )?
    .unwrap();
    assert!(matches!(projection[0], SanitizedCell::Redacted));
    assert!(matches!(projection[1], SanitizedCell::Redacted));
    assert!(grants.contains(&Grant {
        subject: Subject::Principal(principal_id),
        operation: Operation::Select,
        target: AccessTarget::Table(table_id),
    }));
    assert!(grants.contains(&Grant {
        subject: Subject::Principal(principal_id),
        operation: Operation::Update,
        target: AccessTarget::Column {
            table_id,
            column_id: column_uuid_id,
        },
    }));
    assert_eq!(loaded.table_id, table_id);
    assert_eq!(loaded.schema_id, schema_id);
    assert_eq!(loaded.schema_version, 2);
    assert_eq!(loaded.column_ids, [column_uuid_id, column_text_id]);
    assert_eq!(loaded.column_type_ids, [uuid_type_id, varchar_type_id]);
    assert_eq!(loaded.column_default_masks, [7, 1]);
    assert_eq!(loaded.columns[0].scalar_type, ScalarType::Uuid);
    assert!(loaded.columns[0].nullable);
    assert_eq!(
        loaded.columns[1].scalar_type,
        ScalarType::Varchar { max_chars: 32 }
    );
    assert!(!loaded.columns[1].nullable);

    let changed_column_key = catalog_id_key(6, column_text_id);
    let changed_column = catalog_record_payload(
        6,
        &changed_column_key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: table_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Text,
                payload: b"object_136".to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Id,
                payload: varchar_type_id.to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::Bytes,
                payload: {
                    let mut parameters = [0; 8];
                    parameters[4..].copy_from_slice(&16u32.to_le_bytes());
                    parameters.to_vec()
                },
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Bool,
                payload: vec![0],
            },
            CatalogField {
                tag: 8,
                wire_type: WireType::U16,
                payload: 0u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 9,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
        ],
    )?;
    let audit = AuditEvent {
        event_id: [142; 16],
        actor_id: Some([9; 16]),
        correlation_id: [143; 16],
        occurred_at_us: 1_700_000_000_000_001,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![table_id, column_text_id],
    };
    let key = changed_column_key;
    let current = database.begin_read()?;
    let previous = current
        .open_table(CATALOG)?
        .get(&key[..])?
        .unwrap()
        .value()
        .to_vec();
    let table_key = catalog_id_key(5, table_id);
    let previous_table = current
        .open_table(CATALOG)?
        .get(&table_key[..])?
        .unwrap()
        .value()
        .to_vec();
    drop(current);
    commit_catalog_and_audit(
        &database,
        [144; 16],
        [9; 16],
        9_999,
        &[
            CatalogWrite {
                key,
                expected_value: Some(previous),
                payload: Some(changed_column),
            },
            CatalogWrite {
                key: table_key.clone(),
                expected_value: Some(previous_table),
                payload: Some(catalog_record_payload(
                    5,
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
                            payload: 3u64.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 3,
                            wire_type: WireType::U64,
                            payload: 4u64.to_le_bytes().to_vec(),
                        },
                        CatalogField {
                            tag: 4,
                            wire_type: WireType::Id,
                            payload: storage_id.to_vec(),
                        },
                        CatalogField {
                            tag: 5,
                            wire_type: WireType::U16,
                            payload: 1u16.to_le_bytes().to_vec(),
                        },
                    ],
                )?),
            },
        ],
        &[audit],
    )?;
    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Commit(CommitError::Conflict))
    ));
    Ok(())
}

pub(super) fn catalog_authority_rejects_disabled_or_role_invocation_principals()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let records = [
        principal_catalog_record_for([219; 16], 1, b"disabled", false, 1),
        principal_catalog_record_for([220; 16], 2, b"role", false, 1),
        principal_catalog_record_for([221; 16], 1, b"enabled", true, 1),
    ];
    {
        let write = database.begin_write()?;
        {
            let mut catalog = write.open_table(CATALOG)?;
            for (key, value) in &records {
                catalog.insert(key.as_slice(), value.as_slice())?;
            }
            for (principal_id, kind) in [([219; 16], 1), ([220; 16], 2), ([221; 16], 1)] {
                let (key, value) = principal_object_record_for(principal_id, kind);
                catalog.insert(key.as_slice(), value.as_slice())?;
            }
        }
        {
            let mut principal_names = write.open_table(PRINCIPAL_NAMES)?;
            for principal_id in [[219; 16], [220; 16], [221; 16]] {
                principal_names.insert(
                    principal_test_name(principal_id).as_bytes(),
                    &principal_id[..],
                )?;
            }
        }
        write.commit()?;
    }
    for (invocation_id, principal_id) in [([222; 16], [219; 16]), ([223; 16], [220; 16])] {
        let mut transaction =
            InvocationTransaction::begin(&database, invocation_id, principal_id, 9_999, 1, 64)?;
        assert!(matches!(
            transaction.load_catalog_role_closure(principal_id),
            Err(TransactionError::Authorization(
                prdb_engine::authorization::AuthorizationError::Denied
            ))
        ));
        assert_eq!(
            prdb_engine::outcome::lookup_outcome(&database, invocation_id, principal_id, 1)?
                .unwrap()
                .status,
            OutcomeStatus::Aborted
        );
    }
    let mut valid = InvocationTransaction::begin(&database, [224; 16], [221; 16], 9_999, 1, 64)?;
    assert!(valid.load_catalog_role_closure([221; 16])?.is_empty());
    valid.stage_put(id(225), b"pending")?;
    let (principal_key, disabled_payload) =
        principal_catalog_record_for([221; 16], 1, b"enabled", false, 2);
    let audit = AuditEvent {
        event_id: [226; 16],
        actor_id: Some([9; 16]),
        correlation_id: [227; 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::AuthorityChange,
        result: AuditResult::Succeeded,
        target_ids: vec![[221; 16]],
    };
    commit_catalog_and_audit(
        &database,
        [228; 16],
        [9; 16],
        9_999,
        &[CatalogWrite {
            key: principal_key,
            expected_value: Some(records[2].1.clone()),
            payload: Some(disabled_payload),
        }],
        &[audit],
    )?;
    assert!(matches!(
        valid.commit(),
        Err(TransactionError::Commit(CommitError::Conflict))
    ));
    assert!(
        database
            .begin_read()?
            .open_table(ROWS)?
            .get(&key(id(225))[..])?
            .is_none()
    );
    Ok(())
}

pub(super) fn table_epochs_cannot_move_back_after_catalog_changes()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let table_id = [239; 16];
    let table_key = catalog_id_key(5, table_id);
    let field = |tag, wire_type, payload| CatalogField {
        tag,
        wire_type,
        payload,
    };
    let table_payload = |schema_version: u64, write_epoch: u64| {
        catalog_record_payload(
            5,
            &table_key,
            &[
                field(1, WireType::Id, vec![240; 16]),
                field(2, WireType::U64, schema_version.to_le_bytes().to_vec()),
                field(3, WireType::U64, write_epoch.to_le_bytes().to_vec()),
                field(4, WireType::Id, vec![241; 16]),
                field(5, WireType::U16, 1u16.to_le_bytes().to_vec()),
            ],
        )
    };
    let initial = table_payload(1, 1)?;
    let schema_bumped = table_payload(2, 1)?;
    let write_bumped = table_payload(2, 2)?;
    {
        let write = database.begin_write()?;
        {
            write
                .open_table(CATALOG)?
                .insert(table_key.as_slice(), initial.as_slice())?;
            let (object_key, object_value) = object_catalog_record_for(table_id, 3);
            write
                .open_table(CATALOG)?
                .insert(object_key.as_slice(), object_value.as_slice())?;
        }
        write.commit()?;
    }
    let mut stale = InvocationTransaction::begin(&database, [242; 16], [9; 16], 9_999, 1, 64)?;
    stale.read_catalog_record(&table_key)?;
    stale.stage_put(id(243), b"pending")?;
    let column_key = catalog_id_key(6, [244; 16]);
    let column = catalog_record_payload(
        6,
        &column_key,
        &[
            field(1, WireType::Id, table_id.to_vec()),
            field(2, WireType::U32, 0u32.to_le_bytes().to_vec()),
            field(3, WireType::Text, format!("object_{}", 244).into_bytes()),
            field(4, WireType::Id, vec![245; 16]),
            field(5, WireType::Bytes, vec![0; 8]),
            field(6, WireType::Bool, vec![1]),
            field(8, WireType::U16, 0u16.to_le_bytes().to_vec()),
            field(9, WireType::U64, 2u64.to_le_bytes().to_vec()),
        ],
    )?;
    let audit = |n| AuditEvent {
        event_id: [n; 16],
        actor_id: Some([9; 16]),
        correlation_id: [n + 1; 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![table_id],
    };
    commit_catalog_and_audit(
        &database,
        [246; 16],
        [9; 16],
        9_999,
        &[
            CatalogWrite {
                key: column_key,
                expected_value: None,
                payload: Some(column),
            },
            CatalogWrite {
                key: table_key.clone(),
                expected_value: Some(initial.clone()),
                payload: Some(schema_bumped.clone()),
            },
            object_catalog_write_for([244; 16], 4),
        ],
        &[audit(247)],
    )?;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            [249; 16],
            [9; 16],
            9_999,
            &[CatalogWrite {
                key: table_key.clone(),
                expected_value: Some(schema_bumped.clone()),
                payload: Some(initial)
            },],
            &[audit(250)]
        ),
        Err(CommitError::InvalidCatalog)
    );
    commit_catalog_and_audit(
        &database,
        [252; 16],
        [9; 16],
        9_999,
        &[CatalogWrite {
            key: table_key.clone(),
            expected_value: Some(schema_bumped.clone()),
            payload: Some(write_bumped.clone()),
        }],
        &[audit(253)],
    )?;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            [30; 16],
            [9; 16],
            9_999,
            &[CatalogWrite {
                key: table_key,
                expected_value: Some(write_bumped),
                payload: Some(schema_bumped)
            },],
            &[audit(31)]
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert!(matches!(
        stale.commit(),
        Err(TransactionError::Commit(CommitError::Conflict))
    ));
    assert!(
        database
            .begin_read()?
            .open_table(ROWS)?
            .get(&key(id(243))[..])?
            .is_none()
    );
    Ok(())
}

pub(super) fn new_membership_cannot_be_hidden_by_restoring_principal_epoch()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let user = [230; 16];
    let role = [231; 16];
    let old = principal_catalog_record_for(user, 1, b"user", true, 1);
    let new = principal_catalog_record_for(user, 1, b"user", true, 2);
    let role_record = principal_catalog_record_for(role, 2, b"role", false, 1);
    {
        let write = database.begin_write()?;
        {
            let mut catalog = write.open_table(CATALOG)?;
            for (key, value) in [&old, &role_record] {
                catalog.insert(key.as_slice(), value.as_slice())?;
            }
            for (principal_id, kind) in [(user, 1), (role, 2)] {
                let (key, value) = principal_object_record_for(principal_id, kind);
                catalog.insert(key.as_slice(), value.as_slice())?;
            }
        }
        {
            let mut principal_names = write.open_table(PRINCIPAL_NAMES)?;
            for principal_id in [user, role] {
                principal_names.insert(
                    principal_test_name(principal_id).as_bytes(),
                    &principal_id[..],
                )?;
            }
        }
        write.commit()?;
    }
    let mut stale = InvocationTransaction::begin(&database, [232; 16], user, 9_999, 1, 64)?;
    assert!(stale.load_catalog_role_closure(user)?.is_empty());
    stale.stage_put(id(233), b"pending")?;
    let (membership_key, membership_payload) = membership_catalog_record(user, role, user, 1);
    let audit = |n| AuditEvent {
        event_id: [n; 16],
        actor_id: Some(user),
        correlation_id: [n + 1; 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::AuthorityChange,
        result: AuditResult::Succeeded,
        target_ids: vec![user, role],
    };
    commit_catalog_and_audit(
        &database,
        [234; 16],
        user,
        9_999,
        &[
            CatalogWrite {
                key: membership_key,
                expected_value: None,
                payload: Some(membership_payload),
            },
            dependency_catalog_write(user, role, 5)?,
            CatalogWrite {
                key: new.0.clone(),
                expected_value: Some(old.1.clone()),
                payload: Some(new.1.clone()),
            },
        ],
        &[audit(235)],
    )?;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            [237; 16],
            user,
            9_999,
            &[CatalogWrite {
                key: old.0.clone(),
                expected_value: Some(new.1),
                payload: Some(old.1)
            },],
            &[audit(238)]
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert!(matches!(
        stale.commit(),
        Err(TransactionError::Commit(CommitError::Conflict))
    ));
    assert!(
        database
            .begin_read()?
            .open_table(ROWS)?
            .get(&key(id(233))[..])?
            .is_none()
    );
    Ok(())
}

pub(super) fn invocation_loads_and_guards_transitive_catalog_role_closure()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let user_id = [151; 16];
    let first_role = [152; 16];
    let second_role = [153; 16];
    let principals = [
        principal_catalog_record_for(user_id, 1, b"user", true, 1),
        principal_catalog_record_for(first_role, 2, b"first", false, 1),
        principal_catalog_record_for(second_role, 2, b"second", false, 1),
    ];
    let memberships = [
        membership_catalog_record(user_id, first_role, user_id, 1),
        membership_catalog_record(first_role, second_role, user_id, 1),
    ];
    let mut writes = Vec::new();
    for (key, payload) in principals.iter().chain(&memberships) {
        writes.push(CatalogWrite {
            key: key.clone(),
            expected_value: None,
            payload: Some(payload.clone()),
        });
    }
    for (principal_id, kind) in [(user_id, 1), (first_role, 2), (second_role, 2)] {
        let (key, payload) = principal_object_record_for(principal_id, kind);
        writes.push(CatalogWrite {
            key,
            expected_value: None,
            payload: Some(payload),
        });
    }
    writes.push(dependency_catalog_write(user_id, first_role, 5)?);
    writes.push(dependency_catalog_write(first_role, second_role, 5)?);
    let audit = AuditEvent {
        event_id: [154; 16],
        actor_id: Some(user_id),
        correlation_id: [155; 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![user_id, first_role, second_role],
    };
    commit_catalog_and_audit(&database, [156; 16], user_id, 9_999, &writes, &[audit])?;

    let mut invocation =
        InvocationTransaction::begin(&database, [157; 16], user_id, 9_999, 1, 128)?;
    let closure = invocation.load_catalog_role_closure(user_id)?;
    assert_eq!(
        closure,
        std::collections::HashSet::from([first_role, second_role])
    );

    let (membership_key, updated_membership) =
        membership_catalog_record(user_id, first_role, user_id, 2);
    let read = database.begin_read()?;
    let previous = read
        .open_table(CATALOG)?
        .get(&membership_key[..])?
        .unwrap()
        .value()
        .to_vec();
    drop(read);
    let audit = AuditEvent {
        event_id: [158; 16],
        actor_id: Some(user_id),
        correlation_id: [159; 16],
        occurred_at_us: 1_700_000_000_000_001,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![user_id, first_role],
    };
    let updated_user = principal_catalog_record_for(user_id, 1, b"user", true, 2);
    commit_catalog_and_audit(
        &database,
        [160; 16],
        user_id,
        9_999,
        &[
            CatalogWrite {
                key: membership_key,
                expected_value: Some(previous),
                payload: Some(updated_membership),
            },
            CatalogWrite {
                key: updated_user.0.clone(),
                expected_value: Some(principals[0].1.clone()),
                payload: Some(updated_user.1.clone()),
            },
        ],
        &[audit],
    )?;
    let rollback_audit = AuditEvent {
        event_id: [161; 16],
        actor_id: Some(user_id),
        correlation_id: [162; 16],
        occurred_at_us: 1_700_000_000_000_002,
        action: AuditAction::AuthorityChange,
        result: AuditResult::Succeeded,
        target_ids: vec![user_id],
    };
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            [163; 16],
            user_id,
            9_999,
            &[CatalogWrite {
                key: updated_user.0,
                expected_value: Some(updated_user.1),
                payload: Some(principals[0].1.clone())
            }],
            &[rollback_audit],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert!(matches!(
        invocation.commit(),
        Err(TransactionError::Commit(CommitError::Conflict))
    ));
    Ok(())
}

pub(super) fn authorized_insert_checks_non_generated_fields_and_allows_generated_fields()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_id = id(70);
    let columns = [
        ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Uuid,
            nullable: false,
        },
    ];
    let column_ids = [[71; 16], [72; 16]];
    let values = [
        Some(ScalarValue::Text("new row".to_owned())),
        Some(ScalarValue::Uuid([73; 16])),
    ];
    let table = AccessTarget::Table(row_id.table);
    let name = AccessTarget::Column {
        table_id: row_id.table,
        column_id: column_ids[0],
    };
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::from([[74; 16]]),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(row_id.table)),
            (Operation::Insert, table),
            (Operation::Insert, name),
        ]),
    };
    let usage_grant = Grant {
        subject: Subject::Role([74; 16]),
        operation: Operation::Usage,
        target: AccessTarget::Schema(row_id.table),
    };
    let table_grant = Grant {
        subject: Subject::Role([74; 16]),
        operation: Operation::Insert,
        target: table,
    };
    let name_grant = Grant {
        subject: Subject::Role([74; 16]),
        operation: Operation::Insert,
        target: name,
    };
    let policies = [
        Policy {
            subjects: vec![Subject::Role([74; 16])],
            operation: Operation::Insert,
            target: table,
            effect: Effect::Allow,
            using: None,
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
        Policy {
            subjects: vec![Subject::Role([74; 16])],
            operation: Operation::Insert,
            target: name,
            effect: Effect::Allow,
            using: None,
            with_check: Some(Expr::Literal(Some(ScalarValue::Boolean(true)))),
        },
    ];
    let denied_grants = [usage_grant, table_grant];
    let denied_projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id: row_id.table,
        context: &context,
        grants: &denied_grants,
        policies: &policies,
        checks: &[],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let mut denied = InvocationTransaction::begin(&database, [75; 16], [9; 16], 9_999, 2, 128)?;
    assert!(matches!(
        denied.stage_insert_authorized(row_id, &denied_projection, &values, &[column_ids[1]]),
        Err(TransactionError::Authorization(
            prdb_engine::authorization::AuthorizationError::Denied
        ))
    ));

    let allowed_grants = [usage_grant, table_grant, name_grant];
    let allowed_projection = AuthorizedProjection {
        columns: &columns,
        column_ids: &column_ids,
        schema_version: 1,
        schema_id: row_id.table,
        context: &context,
        grants: &allowed_grants,
        policies: &policies,
        checks: &[Expr::NotEqual(
            Box::new(Expr::Field(0)),
            Box::new(Expr::Literal(Some(ScalarValue::Text("invalid".to_owned())))),
        )],
        policy_driving_columns: &[],
        dependencies: &[],
    };
    let mut invalid_values = values.clone();
    invalid_values[0] = Some(ScalarValue::Text("invalid".to_owned()));
    let mut constraint_denied =
        InvocationTransaction::begin(&database, [77; 16], [9; 16], 9_999, 2, 128)?;
    assert!(matches!(
        constraint_denied.stage_insert_authorized(
            row_id,
            &allowed_projection,
            &invalid_values,
            &[column_ids[1]],
        ),
        Err(TransactionError::ConstraintViolation)
    ));
    let outcome_read = database.begin_read()?;
    assert_eq!(
        InvocationOutcome::decode(
            outcome_read
                .open_table(prdb_engine::schema::OUTCOMES)?
                .get(&[77; 16][..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    let mut allowed = InvocationTransaction::begin(&database, [76; 16], [9; 16], 9_999, 2, 128)?;
    allowed.stage_insert_authorized(row_id, &allowed_projection, &values, &[column_ids[1]])?;
    allowed.commit()?;

    let read = database.begin_read()?;
    let record = RowRecord::decode(
        read.open_table(ROWS)?
            .get(&key(row_id)[..])?
            .unwrap()
            .value(),
    )?;
    assert_eq!(decode_tuple(&columns, 1, &record.payload)?.values, values);
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&[75; 16][..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}
