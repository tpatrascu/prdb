use prdb_engine::catalog::{CatalogField, WireType, encode_envelope, family_schema};
use prdb_engine::commit::{CatalogWrite, commit_catalog_and_audit};
use prdb_engine::credentials::hash_token_secret;
use prdb_engine::storage_manager::StorageManager;
use std::time::Duration;

use super::*;

pub fn credential_verifier_write(
    token_id: [u8; 16],
    user_id: [u8; 16],
    secret: &[u8; 32],
    issued_at_us: i64,
    expires_at_us: i64,
    revoked_at_us: Option<i64>,
    authority_epoch: u64,
) -> CatalogWrite {
    let key = id_catalog_key(16, token_id);
    let mut fields = vec![
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: user_id.to_vec(),
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
            wire_type: WireType::Bytes,
            payload: hash_token_secret(secret).to_vec(),
        },
        CatalogField {
            tag: 5,
            wire_type: WireType::I64,
            payload: issued_at_us.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 6,
            wire_type: WireType::I64,
            payload: expires_at_us.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 7,
            wire_type: WireType::Bool,
            payload: vec![u8::from(revoked_at_us.is_some())],
        },
    ];
    if let Some(revoked_at_us) = revoked_at_us {
        fields.push(CatalogField {
            tag: 8,
            wire_type: WireType::I64,
            payload: revoked_at_us.to_le_bytes().to_vec(),
        });
    }
    fields.extend([
        CatalogField {
            tag: 9,
            wire_type: WireType::Text,
            payload: b"test token".to_vec(),
        },
        CatalogField {
            tag: 10,
            wire_type: WireType::U64,
            payload: authority_epoch.to_le_bytes().to_vec(),
        },
    ]);
    CatalogWrite {
        key: key.clone(),
        expected_value: None,
        payload: Some(encode_envelope(&family_schema(16).unwrap(), &key, &fields).unwrap()),
    }
}

pub fn table_write(table_id: [u8; 16], schema_id: [u8; 16]) -> CatalogWrite {
    table_write_with_versions(table_id, schema_id, 1, 1)
}

pub fn table_write_with_versions(
    table_id: [u8; 16],
    schema_id: [u8; 16],
    schema_version: u64,
    write_epoch: u64,
) -> CatalogWrite {
    let mut key = 5u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&table_id);
    let payload = encode_envelope(
        &family_schema(5).unwrap(),
        &key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: schema_id.to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U64,
                payload: schema_version.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::U64,
                payload: write_epoch.to_le_bytes().to_vec(),
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
    .unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

pub fn column_write(column_id: [u8; 16], table_id: [u8; 16], name: &str) -> CatalogWrite {
    column_write_with_defaults(column_id, table_id, name, None, None)
}

pub fn column_write_at_ordinal(
    column_id: [u8; 16],
    table_id: [u8; 16],
    ordinal: u32,
    name: &str,
) -> CatalogWrite {
    let key = id_catalog_key(6, column_id);
    let payload = encode_envelope(
        &family_schema(6).unwrap(),
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
                payload: name.as_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Id,
                payload: builtin_id(108).to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::Bytes,
                payload: vec![0; 8],
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
                payload: 1u64.to_le_bytes().to_vec(),
            },
        ],
    )
    .unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

pub fn column_write_not_null(column_id: [u8; 16], table_id: [u8; 16], name: &str) -> CatalogWrite {
    column_write_with_nullability_and_defaults(column_id, table_id, name, false, None, None)
}

pub fn column_write_with_defaults(
    column_id: [u8; 16],
    table_id: [u8; 16],
    name: &str,
    default_expression_id: Option<[u8; 16]>,
    add_column_default_id: Option<[u8; 16]>,
) -> CatalogWrite {
    column_write_with_nullability_and_defaults(
        column_id,
        table_id,
        name,
        true,
        default_expression_id,
        add_column_default_id,
    )
}

pub fn column_write_with_nullability_and_defaults(
    column_id: [u8; 16],
    table_id: [u8; 16],
    name: &str,
    nullable: bool,
    default_expression_id: Option<[u8; 16]>,
    add_column_default_id: Option<[u8; 16]>,
) -> CatalogWrite {
    column_write_with_type_and_nullability(
        column_id,
        table_id,
        name,
        nullable,
        default_expression_id,
        add_column_default_id,
        builtin_id(108),
    )
}

pub fn column_write_with_type(
    column_id: [u8; 16],
    table_id: [u8; 16],
    name: &str,
    type_id: [u8; 16],
) -> CatalogWrite {
    column_write_with_type_and_nullability(column_id, table_id, name, true, None, None, type_id)
}

#[allow(clippy::too_many_arguments)]
pub fn column_write_with_type_and_nullability(
    column_id: [u8; 16],
    table_id: [u8; 16],
    name: &str,
    nullable: bool,
    default_expression_id: Option<[u8; 16]>,
    add_column_default_id: Option<[u8; 16]>,
    type_id: [u8; 16],
) -> CatalogWrite {
    let mut key = 6u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&column_id);
    let mut fields = vec![
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: table_id.to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::U32,
            payload: 0u32.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::Text,
            payload: name.as_bytes().to_vec(),
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::Id,
            payload: type_id.to_vec(),
        },
        CatalogField {
            tag: 5,
            wire_type: WireType::Bytes,
            payload: vec![0; 8],
        },
        CatalogField {
            tag: 6,
            wire_type: WireType::Bool,
            payload: vec![u8::from(nullable)],
        },
    ];
    if let Some(expression_id) = default_expression_id {
        fields.push(CatalogField {
            tag: 7,
            wire_type: WireType::Id,
            payload: expression_id.to_vec(),
        });
    }
    fields.extend([
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
    ]);
    if let Some(expression_id) = add_column_default_id {
        fields.push(CatalogField {
            tag: 10,
            wire_type: WireType::Id,
            payload: expression_id.to_vec(),
        });
    }
    let payload = encode_envelope(&family_schema(6).unwrap(), &key, &fields).unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

pub fn expression_write(
    expression_id: [u8; 16],
    purpose: u16,
    dependencies: &[[u8; 16]],
    expression: &prdb_engine::authorization::Expr,
) -> CatalogWrite {
    let key = id_catalog_key(7, expression_id);
    let mut dependency_list = (dependencies.len() as u32).to_le_bytes().to_vec();
    dependency_list.extend(dependencies.iter().flatten());
    let ir = prdb_engine::expression::encode_expression(expression).unwrap();
    let payload = encode_envelope(
        &family_schema(7).unwrap(),
        &key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::U16,
                payload: purpose.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Id,
                payload: id(51).to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Bytes,
                payload: vec![0; 8],
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::Bytes,
                payload: ir,
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Ids,
                payload: dependency_list,
            },
        ],
    )
    .unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

pub fn constraint_write(
    constraint_id: [u8; 16],
    table_id: [u8; 16],
    kind: u16,
    source_columns: &[[u8; 16]],
    referenced_constraint_id: Option<[u8; 16]>,
    referenced_columns: &[[u8; 16]],
    backing_index_id: Option<[u8; 16]>,
) -> CatalogWrite {
    let key = id_catalog_key(8, constraint_id);
    let id_list = |ids: &[[u8; 16]]| {
        let mut encoded = (ids.len() as u32).to_le_bytes().to_vec();
        encoded.extend(ids.iter().flatten());
        encoded
    };
    let mut fields = vec![
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: table_id.to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::U16,
            payload: kind.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::Ids,
            payload: id_list(source_columns),
        },
    ];
    if let Some(referenced_constraint_id) = referenced_constraint_id {
        fields.push(CatalogField {
            tag: 5,
            wire_type: WireType::Id,
            payload: referenced_constraint_id.to_vec(),
        });
    }
    fields.extend([
        CatalogField {
            tag: 6,
            wire_type: WireType::Ids,
            payload: id_list(referenced_columns),
        },
        CatalogField {
            tag: 7,
            wire_type: WireType::U16,
            payload: u16::from(kind == 5).to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 8,
            wire_type: WireType::U16,
            payload: u16::from(kind == 5).to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 9,
            wire_type: WireType::U16,
            payload: u16::from(kind == 5).to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 10,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
    ]);
    if let Some(backing_index_id) = backing_index_id {
        fields.push(CatalogField {
            tag: 11,
            wire_type: WireType::Id,
            payload: backing_index_id.to_vec(),
        });
    }
    CatalogWrite {
        payload: Some(encode_envelope(&family_schema(8).unwrap(), &key, &fields).unwrap()),
        key,
        expected_value: None,
    }
}

pub fn primary_index_write(
    index_id: [u8; 16],
    table_id: [u8; 16],
    column_id: [u8; 16],
    constraint_id: [u8; 16],
) -> CatalogWrite {
    let key = id_catalog_key(9, index_id);
    let mut columns = 1u32.to_le_bytes().to_vec();
    columns.extend_from_slice(&column_id);
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: table_id.to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::Ids,
            payload: columns,
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
    CatalogWrite {
        payload: Some(encode_envelope(&family_schema(9).unwrap(), &key, &fields).unwrap()),
        key,
        expected_value: None,
    }
}

pub fn malformed_foreign_key_write() -> CatalogWrite {
    let constraint_id = id(81);
    let table_id = id(82);
    let mut key = 8u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&constraint_id);
    let payload = encode_envelope(
        &family_schema(8).unwrap(),
        &key,
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
                payload: {
                    let mut v = 1u32.to_le_bytes().to_vec();
                    v.extend_from_slice(&id(83));
                    v
                },
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Ids,
                payload: 0u32.to_le_bytes().to_vec(),
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
            CatalogField {
                tag: 11,
                wire_type: WireType::Id,
                payload: id(84).to_vec(),
            },
        ],
    )
    .unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

pub fn check_constraint_without_table_write() -> CatalogWrite {
    let mut key = 8u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&id(87));
    let payload = encode_envelope(
        &family_schema(8).unwrap(),
        &key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::Id,
                payload: id(88).to_vec(),
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
                payload: id(89).to_vec(),
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
    .unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

pub fn foreign_key_catalog_writes(
    with_target_edge: bool,
    with_backing_edge: bool,
) -> Vec<CatalogWrite> {
    let schema_id = id(80);
    let target_table_id = id(81);
    let source_table_id = id(82);
    let target_column_id = id(83);
    let source_column_id = id(84);
    let target_constraint_id = id(85);
    let foreign_key_id = id(86);
    let backing_index_id = id(87);
    let mut writes = vec![
        schema_write(schema_id),
        table_write(target_table_id, schema_id),
        table_write(source_table_id, schema_id),
        column_write_not_null(target_column_id, target_table_id, "object_83"),
        column_write(source_column_id, source_table_id, "object_84"),
        constraint_write(
            target_constraint_id,
            target_table_id,
            2,
            &[target_column_id],
            None,
            &[],
            Some(backing_index_id),
        ),
        constraint_write(
            foreign_key_id,
            source_table_id,
            5,
            &[source_column_id],
            Some(target_constraint_id),
            &[target_column_id],
            None,
        ),
        primary_index_write(
            backing_index_id,
            target_table_id,
            target_column_id,
            target_constraint_id,
        ),
        object_write(schema_id, 1),
        object_write(target_table_id, 3),
        object_write(source_table_id, 3),
        object_write(target_column_id, 4),
        object_write(source_column_id, 4),
        object_write(target_constraint_id, 6),
        object_write(foreign_key_id, 6),
        object_write(backing_index_id, 7),
    ];
    if with_target_edge {
        writes.push(dependency_write(foreign_key_id, target_constraint_id, 3));
    }
    if with_backing_edge {
        writes.push(dependency_write(target_constraint_id, backing_index_id, 4));
    }
    writes
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub fn run_sandboxed_v3_sql_error_fixture(
    artifact_env: &str,
    expect_network_denial: bool,
    shared_sdk_release_dir_env: Option<&str>,
    expect_terminal_without_result: bool,
    expect_query_error: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::client_protocol::{ClientConnectionSession, ClientFrame, ClientMode};
    use prdb_engine::ipc::{Operation, read_frame};
    use prdb_engine::outcome::OutcomeStatus;
    use prdb_engine::submission_manifest::{
        ArtifactCompatibilityProfile, VersionedSubmissionMetadataValidator, encode_manifest_v1,
        manifest_v1_from_elf,
    };
    use prdb_engine::supervisor::{
        ChunkKind, PrepareSubmission, StagingLimits, StagingStore, SubmissionMetadata,
        SubmissionMetadataValidator, WorkloadClass,
    };
    use prdb_engine::supervisor_ipc::{
        GatewayAdmissionConfig, GatewayExecutorConfig, GatewayTerminalDecision,
        observe_executor_relay_and_finalize_gateway,
    };
    use sha2::{Digest, Sha256};
    use std::collections::HashSet;
    use std::os::unix::net::UnixStream;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct AcceptFixture;
    impl SubmissionMetadataValidator for AcceptFixture {
        fn supports_versions(&self, manifest_version: u32, scope_version: u32) -> bool {
            manifest_version == 1 && scope_version == 1
        }

        fn validate(&self, metadata: &SubmissionMetadata) -> bool {
            metadata.manifest == b"m" && metadata.requested_scope == requested_scope_fixture()
        }
    }

    pub fn required_paths(name: &str) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
        let value = std::env::var_os(name)
            .ok_or_else(|| format!("{name} must list the sandbox runtime paths"))?;
        if name == "PRDB_TEST_RUNTIME_READ_ONLY" && value.is_empty() {
            return Ok(Vec::new());
        }
        let paths = std::env::split_paths(&value).collect::<Vec<_>>();
        if paths.is_empty() || paths.iter().any(|path| !path.is_absolute()) {
            return Err(format!("{name} must contain absolute paths").into());
        }
        Ok(paths)
    }

    let cgroup_parent = std::env::var_os("PRDB_TEST_CGROUP_PARENT")
        .map(PathBuf::from)
        .ok_or("PRDB_TEST_CGROUP_PARENT must name a delegated cgroup v2 parent")?;
    let artifact_path = std::env::var_os(artifact_env)
        .map(PathBuf::from)
        .ok_or_else(|| format!("{artifact_env} must name its TinyCC artifact"))?;
    let network_probe_listener = if expect_network_denial {
        Some(std::net::TcpListener::bind(("127.0.0.1", 0))?)
    } else {
        None
    };
    if !cgroup_parent.is_absolute() || !artifact_path.is_absolute() {
        return Err("sandbox test paths must be absolute".into());
    }
    let constructor_marker = PathBuf::from(if expect_network_denial {
        "/tmp/prdb-sandbox-network-constructor-probe"
    } else {
        "/tmp/prdb-sandbox-constructor-probe"
    });
    let sdk_constructor_marker = PathBuf::from("/tmp/prdb-sandbox-sdk-constructor-probe");
    for marker in [&constructor_marker, &sdk_constructor_marker] {
        match std::fs::remove_file(marker) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    let mut artifact = std::fs::read(&artifact_path)?;
    if let Some(listener) = &network_probe_listener {
        const PORT_PLACEHOLDER: &[u8] = b"PRDB_PORT_00000";
        let offset = artifact
            .windows(PORT_PLACEHOLDER.len())
            .position(|window| window == PORT_PLACEHOLDER)
            .ok_or("network-constructor artifact is missing its port placeholder")?;
        let port = format!("{:05}", listener.local_addr()?.port());
        artifact[offset + 10..offset + 15].copy_from_slice(port.as_bytes());
    }
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let user_id = id(0x70);
    let token_id = id(0x71);
    let secret = [0x31; 32];
    commit_catalog_and_audit(
        &database,
        id(0x72),
        id(9),
        now + 3_600_000_000,
        &[
            principal_write_with_name(user_id, 1, 1, "sandbox_sql_user"),
            object_write_with_name(user_id, 8, "sandbox_sql_user"),
            credential_verifier_write(
                token_id,
                user_id,
                &secret,
                now - 1_000_000,
                now + 60_000_000,
                None,
                1,
            ),
        ],
        &[audit_event(0x72)],
    )?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let mut client = ClientConnectionSession::default();
    client.accept_hello(ClientMode::Query, "sandbox-sql-v3")?;
    client.authenticate(&storage, token_id, &secret)?;
    let owner = client.staging_owner()?;
    let mut registry_for_assertion = None;
    let (manifest, validator): (Vec<u8>, Arc<dyn SubmissionMetadataValidator>) =
        if let Some(release_dir_env) = shared_sdk_release_dir_env {
            let release_dir = std::env::var_os(release_dir_env)
                .map(PathBuf::from)
                .ok_or_else(|| format!("{release_dir_env} must name the installed SDK release"))?;
            if !release_dir.is_absolute() {
                return Err("SDK release and cache paths must be absolute".into());
            }
            let runtime_profile = "nixos-x86_64-glibc-2.42";
            let sql_library = std::fs::read(release_dir.join("libprdb_sql.so.1"))?;
            let repository = prdb_engine::sdk_registry::SdkLibraryRepository::open(
                &fixture.path().parent().unwrap().join("sdk-libraries"),
                "linux-x86_64".to_owned(),
                "x86_64-v1".to_owned(),
                runtime_profile.to_owned(),
                ["libc.so.6".to_owned()].into_iter().collect(),
                128 * 1024 * 1024,
            )
            .map_err(|error| format!("open test SDK repository: {error:?}"))?;
            let metadata = std::fs::read(release_dir.join("sdk-release.json"))?;
            repository
                .install(
                    &metadata,
                    &sql_library,
                    &["libc.so.6".to_owned()].into_iter().collect(),
                )
                .map_err(|error| format!("install test SDK repository release: {error:?}"))?;
            let derived = manifest_v1_from_elf(
                &artifact,
                3,
                1,
                "x86_64-v1".to_owned(),
                runtime_profile.to_owned(),
                "shared SQL sandbox fixture".to_owned(),
            )
            .map_err(|error| format!("derive shared fixture manifest: {error:?}"))?;
            let manifest = encode_manifest_v1(&derived)
                .map_err(|error| format!("encode shared fixture manifest: {error:?}"))?;
            let mut allowed_dependencies =
                ["libc.so.6".to_owned()].into_iter().collect::<HashSet<_>>();
            if let Some(extra) = std::env::var_os("PRDB_TEST_EXTRA_ALLOWED_DEPENDENCY") {
                allowed_dependencies.insert(extra.to_string_lossy().into_owned());
            }
            let profile = ArtifactCompatibilityProfile::new(
                "x86_64-v1".to_owned(),
                runtime_profile.to_owned(),
                1,
                allowed_dependencies,
            )
            .map_err(|error| format!("build shared fixture profile: {error:?}"))?;
            let validator = VersionedSubmissionMetadataValidator::new(profile);
            let registry = Arc::new(repository);
            registry_for_assertion = Some(Arc::clone(&registry));
            let validator = validator
                .with_sdk_registry(registry)
                .map_err(|error| format!("configure shared SDK repository: {error:?}"))?;
            (manifest, Arc::new(validator))
        } else {
            (b"m".to_vec(), Arc::new(AcceptFixture))
        };
    let staging = StagingStore::new(StagingLimits::default(), validator)?;
    let reservation = staging.prepare(
        owner,
        PrepareSubmission {
            manifest_version: 1,
            manifest,
            artifact_bytes: u64::try_from(artifact.len())?,
            artifact_sha256: Sha256::digest(&artifact).into(),
            scope_version: 1,
            requested_scope: requested_scope_fixture(),
            parameter_bytes: 0,
            parameter_sha256: Sha256::digest([]).into(),
            deadline_ms: 30_000,
            workload_class: WorkloadClass::Operational,
        },
    )?;
    let mut artifact_offset = 0;
    for chunk in artifact.chunks(prdb_engine::supervisor::MAX_CHUNK_BYTES) {
        artifact_offset = staging
            .upload_chunk(
                owner,
                reservation.invocation_id,
                ChunkKind::Artifact,
                artifact_offset,
                chunk,
            )
            .map_err(|error| format!("SQL artifact chunk upload: {error:?}"))?;
    }
    staging
        .seal(owner, reservation.invocation_id)
        .map_err(|error| format!("SQL artifact seal: {error:?}"))?;

    let paths_hex = |value: [u8; 16]| {
        value
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    let runtime = GatewayExecutorConfig {
        cgroup_parent,
        resource_limits: prdb_engine::linux_sandbox::WorkerResourceLimits {
            cpu_quota_micros: 500_000,
            cpu_period_micros: 100_000,
            memory_max_bytes: 256 * 1024 * 1024,
            max_processes: 32,
        },
        analytical_resource_limits: None,
        launch: prdb_engine::executor_launcher::ExecutorLaunchConfig {
            executor_path: PathBuf::from(env!("CARGO_BIN_EXE_prdb-executor")),
            optimizer_path: PathBuf::from(env!("CARGO_BIN_EXE_prdb-optimizer")),
            scratch_parent: std::env::temp_dir(),
            runtime_read_execute: required_paths("PRDB_TEST_RUNTIME_READ_EXECUTE")?,
            runtime_read_only: required_paths("PRDB_TEST_RUNTIME_READ_ONLY")?,
            spill_broker: None,
            max_total_result_bytes: prdb_engine::query_abi::MAX_TOTAL_RESULT_BYTES,
            analytical_max_total_result_bytes: None,
            max_sql_index_probe_candidates:
                prdb_engine::query_abi::DEFAULT_SQL_INDEX_PROBE_CANDIDATES,
            analytical_max_sql_index_probe_candidates: None,
            cleanup_timeout: Duration::from_secs(2),
        },
    };
    let (accepted, mut invocation, mut executor) = client.handle_start_request_with_executor(
        &storage,
        &staging,
        ClientFrame {
            kind: 7,
            request_id: 1,
            response_sequence: 0,
            payload: format!(
                "{{\"invocation_id\":\"{}\"}}",
                paths_hex(reservation.invocation_id)
            )
            .into_bytes(),
        },
        GatewayAdmissionConfig {
            retention_period_us: 3_600_000_000,
            max_rows: 2,
            max_bytes: 1024,
        },
        &runtime,
    )?;
    assert_eq!(accepted.payload, b"{\"state\":\"accepted\"}");
    if let Some(registry) = &registry_for_assertion {
        assert_eq!(
            registry.remove_release(1),
            Err(prdb_engine::sdk_registry::SdkRegistryError::ReleasePinned)
        );
    }

    let (mut service, mut gateway) = UnixStream::pair()?;
    gateway.set_read_timeout(Some(Duration::from_secs(10)))?;
    let expected_abort_reason = if expect_query_error {
        prdb_engine::supervisor_ipc::GatewayAbortReason::QueryError
    } else {
        prdb_engine::supervisor_ipc::GatewayAbortReason::WorkerFailure
    };
    assert_eq!(
        observe_executor_relay_and_finalize_gateway(
            &storage,
            &mut invocation,
            &mut executor,
            &mut service,
            accepted.request_id,
        )?,
        GatewayTerminalDecision::Abort(expected_abort_reason)
    );
    staging.finish_started(owner, reservation.invocation_id)?;
    drop(executor);
    if let Some(registry) = &registry_for_assertion {
        registry
            .remove_release(1)
            .map_err(|error| format!("remove unpinned SDK repository release: {error:?}"))?;
    }

    let first_frame = read_frame(&mut gateway)?.ok_or("missing worker result")?;
    let constructor_escaped = constructor_marker.exists();
    let sdk_constructor_escaped =
        shared_sdk_release_dir_env.is_some() && sdk_constructor_marker.exists();
    if constructor_escaped {
        std::fs::remove_file(&constructor_marker)?;
    }
    if sdk_constructor_escaped {
        std::fs::remove_file(&sdk_constructor_marker)?;
    }
    assert!(
        !constructor_escaped,
        "native constructor created a file outside its invocation scratch"
    );
    assert!(
        !sdk_constructor_escaped,
        "SQL SDK constructor created a file outside its invocation scratch"
    );
    let terminal = if expect_terminal_without_result {
        assert_eq!(first_frame.operation, Operation::GatewayTerminal as u16);
        first_frame
    } else if expect_network_denial {
        if first_frame.operation == Operation::GatewayTerminal as u16 {
            first_frame
        } else {
            assert_eq!(first_frame.operation, Operation::GatewayResultData as u16);
            assert_eq!(first_frame.request_id, accepted.request_id);
            assert_eq!(
                first_frame.payload,
                b"constructor network connection blocked"
            );
            read_frame(&mut gateway)?.ok_or("missing aborted terminal event")?
        }
    } else {
        assert_eq!(first_frame.operation, Operation::GatewayResultData as u16);
        assert_eq!(first_frame.request_id, accepted.request_id);
        assert_eq!(first_frame.payload, b"partial SQL output");
        read_frame(&mut gateway)?.ok_or("missing aborted terminal event")?
    };
    assert_eq!(terminal.operation, Operation::GatewayTerminal as u16);
    assert_eq!(terminal.request_id, accepted.request_id);
    assert!(String::from_utf8(terminal.payload)?.contains("\"outcome\":\"aborted\""));
    assert_eq!(
        storage
            .lookup_invocation_outcome(reservation.invocation_id, user_id, now + 1)?
            .ok_or("missing durable invocation outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

pub fn run_optimizer_predicate_estimate_termination_case(
    cancel_owner: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    use prdb_engine::ipc::{IpcFrame, Operation, read_frame, write_frame};
    use prdb_engine::optimizer_ipc::{OptimizerResponse, OptimizerStatus};
    use prdb_engine::outcome::OutcomeStatus;
    use prdb_engine::storage_manager::{OwnerCancellationResult, SupervisorAdmissionRequest};
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

    pub fn set_cloexec(fd: i32, enabled: bool) -> std::io::Result<()> {
        // SAFETY: fcntl reads and updates flags for the caller-owned open fd.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if flags < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let next = if enabled {
            flags | libc::FD_CLOEXEC
        } else {
            flags & !libc::FD_CLOEXEC
        };
        // SAFETY: F_SETFD consumes the scalar descriptor flags above.
        if unsafe { libc::fcntl(fd, libc::F_SETFD, next) } < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    let cgroup_parent = std::env::var_os("PRDB_TEST_CGROUP_PARENT")
        .ok_or("PRDB_TEST_CGROUP_PARENT is required for optimizer cancellation")?;
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let storage = StorageManager::new(&database, Duration::from_millis(100))?;
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let user_id = id(0x7a);
    let token_id = id(0x7b);
    let secret = [0x33; 32];
    let invocation_id = id(0x7c);
    let worker_deadline = std::time::Instant::now()
        + if cancel_owner {
            Duration::from_secs(10)
        } else {
            Duration::from_secs(5)
        };
    commit_catalog_and_audit(
        &database,
        id(0x7d),
        id(9),
        now + 3_600_000_000,
        &[
            principal_write_with_name(user_id, 1, 1, "optimizer_estimate_cancel_user"),
            object_write_with_name(user_id, 8, "optimizer_estimate_cancel_user"),
            credential_verifier_write(
                token_id,
                user_id,
                &secret,
                now - 1_000_000,
                now + 60_000_000,
                None,
                1,
            ),
        ],
        &[audit_event(0x7d)],
    )?;
    let invocation = storage.admit_supervisor_invocation_with_deadline(
        SupervisorAdmissionRequest {
            invocation_id,
            token_id,
            retention_deadline_us: now + 3_600_000_000,
            max_rows: 2,
            max_bytes: 64,
            deadline: Some(worker_deadline),
            workload_class: prdb_engine::supervisor::WorkloadClass::Analytical,
        },
        &secret,
    )?;

    let (mut submission_client, optimizer_submission) = UnixStream::pair()?;
    let (planner_worker, planner_storage) = UnixStream::pair()?;
    let (executor_worker, executor_storage) = UnixStream::pair()?;
    let (optimizer_planner, optimizer_planner_storage) = UnixStream::pair()?;
    submission_client.set_read_timeout(Some(Duration::from_secs(3)))?;
    let submission_fd = optimizer_submission.as_raw_fd();
    let planner_fd = optimizer_planner.as_raw_fd();
    set_cloexec(submission_fd, false)?;
    set_cloexec(planner_fd, false)?;
    let cgroup = prdb_engine::linux_sandbox::WorkerCgroup::create(
        cgroup_parent,
        invocation_id,
        prdb_engine::linux_sandbox::WorkerResourceLimits {
            cpu_quota_micros: 100_000,
            cpu_period_micros: 100_000,
            memory_max_bytes: 128 * 1024 * 1024,
            max_processes: 8,
        },
    )?;
    let cgroup_path = cgroup.path().to_owned();
    let optimizer_pid_path = fixture.path().with_extension("optimizer-child.pid");
    let mut worker = Command::new("sh");
    worker
        .arg("-c")
        .arg("\"$1\" --submission-fd \"$2\" --planner-fd \"$3\" & echo $! > \"$4\"; wait")
        .arg("sh")
        .arg(env!("CARGO_BIN_EXE_prdb-optimizer"))
        .arg(submission_fd.to_string())
        .arg(planner_fd.to_string())
        .arg(&optimizer_pid_path);
    cgroup.attach_command_before_exec(&mut worker)?;
    let mut worker = worker.spawn()?;
    set_cloexec(submission_fd, true)?;
    set_cloexec(planner_fd, true)?;
    drop(optimizer_submission);
    drop(optimizer_planner);
    let optimizer_pid_deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !optimizer_pid_path.exists() && std::time::Instant::now() < optimizer_pid_deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    let optimizer_pid = std::fs::read_to_string(&optimizer_pid_path)
        .map_err(|_| "worker did not start the optimizer child")?
        .trim()
        .parse::<u32>()?;

    let mut request = vec![0, 1, 1, 0];
    for name in ["information_schema", "tables"] {
        request.push(name.len() as u8);
        request.extend_from_slice(name.as_bytes());
    }
    request.extend_from_slice(&1u16.to_be_bytes());
    request.push(10);
    request.extend_from_slice(b"table_name");
    request.extend_from_slice(&0u32.to_be_bytes());
    request.push(0);
    let predicate = [
        0x20, 5, 0x10, 10, b't', b'a', b'b', b'l', b'e', b'_', b'n', b'a', b'm', b'e', 0x03, 0, 1,
        b'm',
    ];
    request.truncate(request.len() - 5);
    request.extend_from_slice(&(predicate.len() as u32).to_be_bytes());
    request.extend_from_slice(&predicate);
    request.push(0);
    let (estimate_request_tx, estimate_request_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = std::sync::Arc::new(std::sync::Mutex::new(release_rx));
    let observer = {
        let release_rx = std::sync::Arc::clone(&release_rx);
        move |operation, request_id| {
            if operation == Operation::EstimateScan as u16 {
                let _ = estimate_request_tx.send(request_id);
                if let Ok(release) = release_rx.lock() {
                    let _ = release.recv_timeout(Duration::from_secs(10));
                }
            }
        }
    };
    let (service_result, cancellation) = std::thread::scope(|scope| {
        let serving = scope.spawn(|| {
            invocation.serve_worker_channels_with_optimizer_planner_observer(
                planner_storage,
                executor_storage,
                optimizer_planner_storage,
                observer,
            )
        });
        write_frame(
            &mut submission_client,
            &IpcFrame {
                operation: Operation::OptimizeSelect as u16,
                request_id: 91,
                payload: request,
            },
        )?;
        let estimate_request_id = estimate_request_rx
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| "production planner service did not reach EstimateScan")?;
        assert_ne!(estimate_request_id, 0);
        assert!(
            std::time::Instant::now() < worker_deadline,
            "optimizer reached its estimate request after the worker deadline"
        );

        let cancellation = if cancel_owner {
            let cancellation = storage.cancel_owned_invocation(user_id, invocation_id, now + 1)?;
            assert_eq!(cancellation, OwnerCancellationResult::Accepted);
            Some(cancellation)
        } else {
            std::thread::sleep(
                worker_deadline.saturating_duration_since(std::time::Instant::now()),
            );
            None
        };
        release_tx
            .send(())
            .map_err(|_| "estimate service gate was lost")?;
        let service_result = serving
            .join()
            .map_err(|_| std::io::Error::other("worker-channel service thread panicked"))?;
        Ok::<_, Box<dyn std::error::Error>>((service_result, cancellation))
    })?;
    drop(planner_worker);
    drop(executor_worker);
    if cancel_owner {
        assert_eq!(cancellation, Some(OwnerCancellationResult::Accepted));
    }
    assert!(service_result.is_err());
    if worker.try_wait()?.is_none() {
        cgroup.kill_all()?;
    }
    worker.wait()?;
    cgroup.cleanup(Duration::from_secs(2))?;
    assert!(
        !std::path::Path::new(&format!("/proc/{optimizer_pid}")).exists(),
        "optimizer child {optimizer_pid} survived worker cgroup cancellation"
    );
    assert!(!cgroup_path.exists(), "optimizer cgroup was not drained");
    if let Some(response) = read_frame(&mut submission_client)? {
        assert_eq!(response.operation, Operation::OptimizeSelect as u16);
        let response = OptimizerResponse::decode(&response.payload)?;
        assert_ne!(response.status, OptimizerStatus::Success);
        assert!(response.plan.is_none());
    }
    assert_eq!(
        storage
            .lookup_invocation_outcome(invocation_id, user_id, now + 1)?
            .unwrap()
            .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

pub fn principal_write_with_epoch(principal_id: [u8; 16], kind: u16, epoch: u64) -> CatalogWrite {
    let mut key = 10u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&principal_id);
    let payload = encode_envelope(
        &family_schema(10).unwrap(),
        &key,
        &[
            CatalogField {
                tag: 1,
                wire_type: WireType::U16,
                payload: kind.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::Text,
                payload: principal_test_name(principal_id).into_bytes(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Bool,
                payload: vec![u8::from(kind == 1)],
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::U64,
                payload: epoch.to_le_bytes().to_vec(),
            },
        ],
    )
    .unwrap();
    CatalogWrite {
        key,
        expected_value: None,
        payload: Some(payload),
    }
}

pub fn principal_epoch_update(principal_id: [u8; 16], kind: u16, epoch: u64) -> CatalogWrite {
    let mut write = principal_write_with_epoch(principal_id, kind, epoch);
    write.expected_value = Some(principal_write(principal_id, kind).payload.unwrap());
    write
}

pub fn dependency_record(
    dependent_id: [u8; 16],
    referenced_id: [u8; 16],
    kind: u16,
) -> (Vec<u8>, Vec<u8>) {
    let mut key = 15u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&dependent_id);
    key.push(1);
    key.extend_from_slice(&referenced_id);
    key.push(1);
    key.extend_from_slice(&kind.to_be_bytes());
    let payload = encode_envelope(&family_schema(15).unwrap(), &key, &[]).unwrap();
    (key, payload)
}
