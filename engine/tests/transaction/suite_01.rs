use prdb_engine::authorization::{
    AccessTarget, AuthorizationContext, Grant, Operation, SanitizedCell, Subject,
};
use prdb_engine::catalog::{encode_envelope, family_schema};
use prdb_engine::schema::{CATALOG, ROWS};
use prdb_engine::transaction::{InsertInput, InvocationTransaction, TransactionError};
use prdb_engine::tuple::{FieldPatch, ScalarType, ScalarValue};
use redb::ReadableDatabase;

use super::helpers::*;

pub(super) fn authorized_relation_listing_uses_schema_usage_and_data_grants()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    install_second_relation_column(&database)?;

    let schema_id = [0x91; 16];
    let table_id = [0x10; 16];
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (Operation::Insert, AccessTarget::Table(table_id)),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema_id),
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Insert,
            target: AccessTarget::Table(table_id),
        },
    ];
    let mut invocation = InvocationTransaction::begin(&database, [180; 16], [9; 16], 9_999, 1, 64)?;
    let page = invocation
        .list_authorized_relations(schema_id, 16, None, &context, &grants, &[])?
        .expect("schema is discoverable");
    assert_eq!(page.relations.len(), 1);
    assert_eq!(page.relations[0].relation_id, table_id);
    assert_eq!(page.relations[0].name, "metrics");
    assert!(page.continuation.is_none());

    let mut malformed_hidden_row_key = [0x20; 32];
    malformed_hidden_row_key[16..].fill(0xfe);
    let write = database.begin_write()?;
    write
        .open_table(ROWS)?
        .insert(&malformed_hidden_row_key[..], &b"not-a-row"[..])?;
    write.commit()?;
    let mut populated_hidden_invocation =
        InvocationTransaction::begin(&database, [181; 16], [9; 16], 9_999, 1, 64)?;
    let populated_hidden_page = populated_hidden_invocation
        .list_authorized_relations(schema_id, 16, None, &context, &grants, &[])?
        .expect("schema remains discoverable with hidden row data");
    assert_eq!(
        populated_hidden_page.relations, page.relations,
        "inaccessible relation rows cannot change ordinary metadata discovery"
    );

    let no_usage = AuthorizationContext {
        scope: std::collections::HashSet::from([(
            Operation::Insert,
            AccessTarget::Table(table_id),
        )]),
        ..context
    };
    assert!(
        invocation
            .list_authorized_relations(schema_id, 16, None, &no_usage, &grants, &[])?
            .is_none()
    );
    Ok(())
}

pub(super) fn relation_listing_rejects_grants_not_present_in_the_pinned_catalog()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let schema_id = [0x91; 16];
    let table_id = [0x10; 16];
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (Operation::Select, AccessTarget::Table(table_id)),
        ]),
    };
    let forged_grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema_id),
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Select,
            target: AccessTarget::Table(table_id),
        },
    ];
    let mut invocation = InvocationTransaction::begin(&database, [183; 16], [9; 16], 9_999, 1, 64)?;
    assert!(matches!(
        invocation.list_authorized_relations(schema_id, 16, None, &context, &forged_grants, &[]),
        Err(TransactionError::InvalidContext)
    ));
    Ok(())
}

pub(super) fn schema_listing_includes_the_public_namespace_and_usage_grants()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let app_schema_id = [0x91; 16];
    let public_schema_id = [0x93; 16];
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(app_schema_id)),
            (Operation::Usage, AccessTarget::Schema(public_schema_id)),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(app_schema_id),
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(public_schema_id),
        },
    ];
    let mut invocation = InvocationTransaction::begin(&database, [185; 16], [9; 16], 9_999, 1, 64)?;
    let first = invocation.list_authorized_schemas(1, None, &context, &grants, &[])?;
    assert_eq!(first.schemas.len(), 1);
    assert_eq!(first.schemas[0].name, "information_schema");
    let cursor = first.continuation.expect("another visible schema");
    let serialized_cursor = cursor.to_bytes();
    assert!(
        prdb_engine::metadata_discovery::RelationScanCursor::from_bytes(&serialized_cursor)
            .is_none()
    );
    assert!(
        prdb_engine::metadata_discovery::ColumnScanCursor::from_bytes(&serialized_cursor).is_none()
    );
    let cursor = prdb_engine::metadata_discovery::SchemaScanCursor::from_bytes(&serialized_cursor)
        .expect("cursor wire form is fixed width");
    let mut other_invocation =
        InvocationTransaction::begin(&database, [195; 16], [9; 16], 9_999, 1, 64)?;
    assert!(matches!(
        other_invocation.list_authorized_schemas(1, Some(cursor.clone()), &context, &grants, &[],),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    let second =
        invocation.list_authorized_schemas(1, Some(cursor.clone()), &context, &grants, &[])?;
    assert_eq!(second.schemas.len(), 1);
    assert_eq!(second.schemas[0].schema_id, app_schema_id);
    assert_eq!(second.schemas[0].name, "app");
    let third = invocation.list_authorized_schemas(
        1,
        second.continuation.clone(),
        &context,
        &grants,
        &[],
    )?;
    assert_eq!(third.schemas.len(), 1);
    assert_eq!(third.schemas[0].schema_id, public_schema_id);
    assert_eq!(third.schemas[0].name, "public");
    assert!(third.continuation.is_none());
    assert!(matches!(
        invocation.list_authorized_schemas(1, Some(cursor), &context, &grants, &[]),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    Ok(())
}

pub(super) fn schema_cursor_rejects_modified_wire_tokens() -> Result<(), Box<dyn std::error::Error>>
{
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let schema_id = [0x91; 16];
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (Operation::Usage, AccessTarget::Schema([0x93; 16])),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema_id),
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema([0x93; 16]),
        },
    ];
    let mut invocation = InvocationTransaction::begin(&database, [196; 16], [9; 16], 9_999, 1, 64)?;
    let first = invocation.list_authorized_schemas(1, None, &context, &grants, &[])?;
    let cursor = first.continuation.expect("another visible schema");
    let mut bytes = cursor.to_bytes();
    bytes[1] ^= 1;
    let modified = prdb_engine::metadata_discovery::SchemaScanCursor::from_bytes(&bytes).unwrap();
    assert!(matches!(
        invocation.list_authorized_schemas(1, Some(modified), &context, &grants, &[]),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    assert!(prdb_engine::metadata_discovery::SchemaScanCursor::from_bytes(&bytes[..15]).is_none());
    Ok(())
}

pub(super) fn authorized_schema_resolution_matches_only_visible_exact_names()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let schema_id = [0x91; 16];
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([(
            Operation::Usage,
            AccessTarget::Schema(schema_id),
        )]),
    };
    let grants = [Grant {
        subject: Subject::Principal([9; 16]),
        operation: Operation::Usage,
        target: AccessTarget::Schema(schema_id),
    }];
    let mut invocation = InvocationTransaction::begin(&database, [189; 16], [9; 16], 9_999, 1, 64)?;
    let resolved = invocation
        .resolve_authorized_schema("app", &context, &grants, &[])?
        .expect("USAGE-visible exact name resolves");
    assert_eq!(resolved.schema_id, schema_id);
    assert_eq!(resolved.name, "app");
    assert!(
        invocation
            .resolve_authorized_schema("private", &context, &grants, &[])?
            .is_none()
    );
    assert!(
        invocation
            .resolve_authorized_schema("missing", &context, &grants, &[])?
            .is_none()
    );
    assert!(
        invocation
            .resolve_authorized_schema("App", &context, &grants, &[])?
            .is_none()
    );
    Ok(())
}

pub(super) fn schema_access_loader_uses_pinned_principal_schema_usage_grants()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let app_schema_id = [0x91; 16];
    let public_schema_id = [0x93; 16];
    let mut invocation = InvocationTransaction::begin(&database, [190; 16], [9; 16], 9_999, 1, 64)?;
    let (roles, grants) = invocation.load_catalog_schema_usage_grants([9; 16])?;
    assert!(roles.is_empty());
    assert_eq!(grants.len(), 2);
    assert!(grants.iter().all(|grant| {
        grant.subject == Subject::Principal([9; 16]) && grant.operation == Operation::Usage
    }));
    let mut visible_schemas = grants
        .iter()
        .filter_map(|grant| match grant.target {
            AccessTarget::Schema(schema_id) => Some(schema_id),
            _ => None,
        })
        .collect::<Vec<_>>();
    visible_schemas.sort_unstable();
    assert_eq!(visible_schemas, [app_schema_id, public_schema_id]);
    Ok(())
}

pub(super) fn relation_discovery_grant_loader_filters_to_one_schema()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let schema_id = [0x91; 16];
    let mut invocation = InvocationTransaction::begin(&database, [192; 16], [9; 16], 9_999, 1, 64)?;
    let (roles, grants) = invocation.load_catalog_relation_discovery_grants(schema_id)?;
    assert!(roles.is_empty());
    assert_eq!(grants.len(), 4);
    assert!(
        grants
            .iter()
            .all(|grant| grant.subject == Subject::Principal([9; 16]))
    );
    assert!(grants.contains(&Grant {
        subject: Subject::Principal([9; 16]),
        operation: Operation::Usage,
        target: AccessTarget::Schema(schema_id),
    }));
    assert!(grants.contains(&Grant {
        subject: Subject::Principal([9; 16]),
        operation: Operation::Insert,
        target: AccessTarget::Table([0x10; 16]),
    }));
    assert!(grants.contains(&Grant {
        subject: Subject::Principal([9; 16]),
        operation: Operation::Select,
        target: AccessTarget::Column {
            table_id: [0x10; 16],
            column_id: [0x66; 16],
        },
    }));
    assert!(grants.contains(&Grant {
        subject: Subject::Principal([9; 16]),
        operation: Operation::Delete,
        target: AccessTarget::Table([0x20; 16]),
    }));
    Ok(())
}

pub(super) fn catalog_authorized_relation_listing_derives_the_invocation_scope()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let schema_id = [0x91; 16];
    let mut invocation = InvocationTransaction::begin(&database, [193; 16], [9; 16], 9_999, 1, 64)?;
    let first = invocation
        .list_catalog_authorized_relations(schema_id, 1, None)?
        .expect("schema is visible");
    assert_eq!(first.relations.len(), 1);
    assert_eq!(first.relations[0].relation_id, [0x10; 16]);
    assert_eq!(first.relations[0].name, "metrics");
    let columns = invocation
        .describe_catalog_authorized_columns_page(schema_id, [0x10; 16], 128, None)?
        .expect("table is discoverable through a column SELECT grant");
    assert_eq!(columns.relation_id, [0x10; 16]);
    assert_eq!(columns.columns.len(), 1);
    assert_eq!(columns.columns[0].column_id, [0x66; 16]);
    assert_eq!(columns.columns[0].name, "value");
    assert_eq!(columns.columns[0].ordinal, 0);
    assert_eq!(columns.columns[0].scalar_type, ScalarType::Bytea);
    assert!(columns.columns[0].nullable);
    let continuation = first.continuation.expect("second granted relation follows");
    let second = invocation
        .list_catalog_authorized_relations(schema_id, 1, Some(continuation))?
        .expect("schema remains visible");
    assert_eq!(second.relations.len(), 1);
    assert_eq!(second.relations[0].relation_id, [0x20; 16]);
    assert_eq!(second.relations[0].name, "secret");
    assert!(second.continuation.is_none());
    assert!(
        invocation
            .list_catalog_authorized_relations([0x92; 16], 1, None)?
            .is_none()
    );
    assert!(
        invocation
            .list_catalog_authorized_relations([0x94; 16], 1, None)?
            .is_none()
    );
    Ok(())
}

pub(super) fn catalog_authorized_schema_listing_derives_the_invocation_scope()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let mut invocation = InvocationTransaction::begin(&database, [191; 16], [9; 16], 9_999, 1, 64)?;
    let first = invocation.list_catalog_authorized_schemas(1, None)?;
    assert_eq!(first.schemas.len(), 1);
    assert_eq!(first.schemas[0].name, "information_schema");
    let continuation = first.continuation.expect("public schema follows");
    let second = invocation.list_catalog_authorized_schemas(1, Some(continuation))?;
    assert_eq!(second.schemas.len(), 1);
    assert_eq!(second.schemas[0].schema_id, [0x91; 16]);
    assert_eq!(second.schemas[0].name, "app");
    let third = invocation.list_catalog_authorized_schemas(1, second.continuation)?;
    assert_eq!(third.schemas[0].schema_id, [0x93; 16]);
    assert_eq!(third.schemas[0].name, "public");
    assert!(third.continuation.is_none());
    Ok(())
}

pub(super) fn information_schema_schemata_exposes_public_and_authorized_names()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let mut invocation = InvocationTransaction::begin(&database, [199; 16], [9; 16], 9_999, 1, 64)?;
    let first = invocation.scan_information_schema_schemata(1, None)?;
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.rows[0].schema_name, "information_schema");
    let continuation = first.continuation.expect("public schema follows");
    let second = invocation.scan_information_schema_schemata(1, Some(continuation))?;
    assert_eq!(second.rows.len(), 1);
    assert_eq!(second.rows[0].schema_name, "app");
    let third = invocation.scan_information_schema_schemata(1, second.continuation)?;
    assert_eq!(third.rows[0].schema_name, "public");
    assert!(third.continuation.is_none());
    Ok(())
}

pub(super) fn information_schema_builtin_relations_have_public_self_descriptions()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let mut invocation = InvocationTransaction::begin(&database, [212; 16], [9; 16], 9_999, 1, 64)?;

    let schemata = invocation.scan_information_schema_schemata(1, None)?;
    assert_eq!(schemata.rows.len(), 1);
    assert_eq!(schemata.rows[0].schema_name, "information_schema");

    let tables = invocation
        .scan_information_schema_tables_in_schema("information_schema", 1, None)?
        .expect("the built-in namespace is public");
    assert_eq!(tables.rows.len(), 1);
    assert_eq!(tables.rows[0].table_schema, "information_schema");
    assert_eq!(tables.rows[0].table_name, "columns");
    assert_eq!(tables.rows[0].table_type, "VIEW");
    let tables_continuation = tables
        .continuation
        .clone()
        .expect("built-in relation follows");

    let columns = invocation
        .scan_information_schema_columns_for_relation("information_schema", "columns", 1, None)?
        .expect("the built-in relation shape is public");
    assert_eq!(columns.rows.len(), 1);
    assert_eq!(columns.rows[0].table_schema, "information_schema");
    assert_eq!(columns.rows[0].table_name, "columns");
    assert_eq!(columns.rows[0].column_name, "table_schema");
    assert_eq!(columns.rows[0].ordinal_position, 1);
    assert_eq!(columns.rows[0].data_type, "TEXT");
    assert_eq!(columns.rows[0].is_nullable, "NO");
    let columns_continuation = columns
        .continuation
        .clone()
        .expect("built-in column follows");

    let mut other_table_invocation =
        InvocationTransaction::begin(&database, [213; 16], [9; 16], 9_999, 1, 64)?;
    assert!(matches!(
        other_table_invocation.scan_information_schema_tables_in_schema(
            "information_schema",
            1,
            Some(tables_continuation),
        ),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    let mut other_column_invocation =
        InvocationTransaction::begin(&database, [214; 16], [9; 16], 9_999, 1, 64)?;
    assert!(matches!(
        other_column_invocation.scan_information_schema_columns_for_relation(
            "information_schema",
            "columns",
            1,
            Some(columns_continuation),
        ),
        Err(TransactionError::InvalidMetadataRequest)
    ));

    let description = invocation
        .resolve_catalog_authorized_relation("information_schema", "columns")?
        .expect("the public definition resolves");
    assert_eq!(description.name, "columns");
    assert_eq!(description.columns.len(), 9);
    assert_eq!(
        description
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        [
            "table_schema",
            "table_name",
            "column_name",
            "ordinal_position",
            "data_type",
            "is_nullable",
            "character_maximum_length",
            "numeric_precision",
            "numeric_scale",
        ]
    );
    assert!(
        description.columns[6..]
            .iter()
            .all(|column| column.nullable)
    );
    assert!(
        description.columns[..6]
            .iter()
            .all(|column| !column.nullable)
    );
    assert_eq!(description.columns[3].scalar_type, ScalarType::Integer);
    assert_eq!(description.columns[6].scalar_type, ScalarType::Integer);
    let schemata_description = invocation
        .resolve_catalog_authorized_relation("information_schema", "schemata")?
        .expect("the public schema definition resolves");
    assert_eq!(schemata_description.columns[0].name, "schema_name");
    let tables_description = invocation
        .resolve_catalog_authorized_relation("information_schema", "tables")?
        .expect("the public table definition resolves");
    assert_eq!(
        tables_description
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["table_schema", "table_name", "table_type"]
    );

    let all_tables = invocation.scan_information_schema_tables(1, None)?;
    assert_eq!(all_tables.rows[0].table_schema, "information_schema");
    let all_columns = invocation.scan_information_schema_columns(1, None)?;
    assert_eq!(all_columns.rows[0].table_schema, "information_schema");
    Ok(())
}

pub(super) fn information_schema_tables_projects_authorized_schema_pages()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let mut invocation = InvocationTransaction::begin(&database, [201; 16], [9; 16], 9_999, 1, 64)?;
    let first = invocation
        .scan_information_schema_tables_in_schema("app", 1, None)?
        .expect("app is visible");
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.rows[0].table_schema, "app");
    assert_eq!(first.rows[0].table_name, "metrics");
    assert_eq!(first.rows[0].table_type, "BASE TABLE");
    let second = invocation
        .scan_information_schema_tables_in_schema("app", 1, first.continuation)?
        .expect("app is visible");
    assert_eq!(second.rows.len(), 1);
    assert_eq!(second.rows[0].table_name, "secret");
    assert!(second.continuation.is_none());

    let empty = invocation
        .scan_information_schema_tables_in_schema("public", 1, None)?
        .expect("public is visible");
    assert!(empty.rows.is_empty());
    assert!(empty.continuation.is_none());
    assert!(
        invocation
            .scan_information_schema_tables_in_schema("private", 1, None)?
            .is_none()
    );
    assert!(
        invocation
            .scan_information_schema_tables_in_schema("missing", 1, None)?
            .is_none()
    );

    let mut cursor_probe =
        InvocationTransaction::begin(&database, [202; 16], [9; 16], 9_999, 1, 64)?;
    let projected = cursor_probe
        .scan_information_schema_tables_in_schema("app", 1, None)?
        .expect("app is visible");
    let cursor = projected.continuation.expect("second relation follows");
    assert!(matches!(
        cursor_probe.list_catalog_authorized_relations([0x91; 16], 1, Some(cursor)),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    Ok(())
}

pub(super) fn information_schema_tables_scan_walks_visible_schema_pages()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let mut invocation = InvocationTransaction::begin(&database, [205; 16], [9; 16], 9_999, 1, 64)?;
    let first = invocation.scan_information_schema_tables(1, None)?;
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.rows[0].table_schema, "information_schema");
    assert_eq!(first.rows[0].table_name, "columns");
    assert_eq!(first.rows[0].table_type, "VIEW");
    let cursor = first
        .continuation
        .expect("second built-in relation follows");
    let cursor_bytes = cursor.to_bytes();
    assert!(
        prdb_engine::metadata_discovery::InformationSchemaTablesCursor::from_bytes(&cursor_bytes)
            .is_some()
    );
    assert!(
        prdb_engine::metadata_discovery::RelationScanCursor::from_bytes(&cursor_bytes).is_none()
    );
    assert!(prdb_engine::metadata_discovery::SchemaScanCursor::from_bytes(&cursor_bytes).is_none());
    assert!(prdb_engine::metadata_discovery::ColumnScanCursor::from_bytes(&cursor_bytes).is_none());
    let second = invocation.scan_information_schema_tables(
        1,
        Some(
            prdb_engine::metadata_discovery::InformationSchemaTablesCursor::from_bytes(
                &cursor_bytes,
            )
            .expect("table scan token round trips"),
        ),
    )?;
    assert_eq!(second.rows.len(), 1);
    assert_eq!(second.rows[0].table_schema, "information_schema");
    assert_eq!(second.rows[0].table_name, "schemata");
    assert_eq!(second.rows[0].table_type, "VIEW");
    let next_builtin_relation = second
        .continuation
        .expect("third built-in relation follows");
    let third = invocation.scan_information_schema_tables(1, Some(next_builtin_relation))?;
    assert_eq!(third.rows[0].table_schema, "information_schema");
    assert_eq!(third.rows[0].table_name, "tables");
    assert_eq!(third.rows[0].table_type, "VIEW");
    let next_app = third.continuation.expect("visible app schema follows");
    let fourth = invocation.scan_information_schema_tables(1, Some(next_app))?;
    assert_eq!(fourth.rows[0].table_schema, "app");
    assert_eq!(fourth.rows[0].table_name, "metrics");
    assert_eq!(fourth.rows[0].table_type, "BASE TABLE");
    let next_secret = fourth.continuation.expect("second app relation follows");
    let fifth = invocation.scan_information_schema_tables(1, Some(next_secret))?;
    assert_eq!(fifth.rows[0].table_schema, "app");
    assert_eq!(fifth.rows[0].table_name, "secret");
    assert_eq!(fifth.rows[0].table_type, "BASE TABLE");
    let next_schema = fifth.continuation.expect("visible public schema follows");
    let sixth = invocation.scan_information_schema_tables(1, Some(next_schema))?;
    assert!(sixth.rows.is_empty());
    assert!(sixth.continuation.is_none());

    let mut cross_invocation =
        InvocationTransaction::begin(&database, [206; 16], [9; 16], 9_999, 1, 64)?;
    assert!(matches!(
        cross_invocation.scan_information_schema_tables(
            1,
            Some(
                prdb_engine::metadata_discovery::InformationSchemaTablesCursor::from_bytes(
                    &cursor_bytes,
                )
                .expect("table scan token round trips"),
            ),
        ),
        Err(TransactionError::InvalidMetadataRequest)
    ));

    let mut replay_invocation =
        InvocationTransaction::begin(&database, [207; 16], [9; 16], 9_999, 1, 64)?;
    let first = replay_invocation.scan_information_schema_tables(1, None)?;
    let first_cursor = first.continuation.expect("second relation follows");
    let second = replay_invocation.scan_information_schema_tables(1, Some(first_cursor.clone()))?;
    assert!(second.continuation.is_some());
    assert!(matches!(
        replay_invocation.scan_information_schema_tables(1, Some(first_cursor)),
        Err(TransactionError::InvalidMetadataRequest)
    ));

    let mut substitution_probe =
        InvocationTransaction::begin(&database, [212; 16], [9; 16], 9_999, 1, 64)?;
    let first = substitution_probe.scan_information_schema_tables(1, None)?;
    let mut substituted_cursor_bytes = first
        .continuation
        .expect("another table follows")
        .to_bytes();
    substituted_cursor_bytes[8] ^= 1;
    assert!(matches!(
        substitution_probe.scan_information_schema_tables(
            1,
            Some(
                prdb_engine::metadata_discovery::InformationSchemaTablesCursor::from_bytes(
                    &substituted_cursor_bytes,
                )
                .expect("same-kind substituted cursor retains its wire shape"),
            ),
        ),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    Ok(())
}

pub(super) fn information_schema_tables_cursor_retains_renamed_relation_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let mut retained = InvocationTransaction::begin(&database, [211; 16], [9; 16], 9_999, 1, 64)?;
    let first = retained.scan_information_schema_tables(1, None)?;
    assert_eq!(first.rows[0].table_name, "columns");
    let continuation = first.continuation.expect("more visible relations follow");

    let table_id = [0x10; 16];
    let object_key = catalog_id_key(2, table_id);
    let current_object = database
        .begin_read()?
        .open_table(CATALOG)?
        .get(object_key.as_slice())?
        .ok_or("metrics table object missing")?
        .value()
        .to_vec();
    let mut object = prdb_engine::catalog::decode_registered_envelope(&current_object)?;
    let name = object
        .fields
        .iter_mut()
        .find(|field| field.tag == 3)
        .expect("table object has a name");
    name.payload = b"renamed_metrics".to_vec();
    let object_version = object
        .fields
        .iter_mut()
        .find(|field| field.tag == 5)
        .expect("table object has a version");
    let version = u64::from_le_bytes(object_version.payload.as_slice().try_into()?);
    object_version.payload = version
        .checked_add(1)
        .ok_or("table object version overflow")?
        .to_le_bytes()
        .to_vec();
    let renamed_object = encode_envelope(
        &family_schema(2).expect("table object family is registered"),
        &object.logical_key,
        &object.fields,
    )?;
    let table_key = catalog_id_key(5, table_id);
    let current_table = database
        .begin_read()?
        .open_table(CATALOG)?
        .get(table_key.as_slice())?
        .ok_or("metrics table definition missing")?
        .value()
        .to_vec();
    let mut table = prdb_engine::catalog::decode_registered_envelope(&current_table)?;
    let epoch = table
        .fields
        .iter_mut()
        .find(|field| field.tag == 3)
        .expect("table definition has a write epoch");
    let current_epoch = u64::from_le_bytes(epoch.payload.as_slice().try_into()?);
    epoch.payload = current_epoch
        .checked_add(1)
        .ok_or("table write epoch overflow")?
        .to_le_bytes()
        .to_vec();
    let renamed_table = encode_envelope(
        &family_schema(5).expect("table definition family is registered"),
        &table.logical_key,
        &table.fields,
    )?;
    // This fixture intentionally omits parts of the production catalog, so
    // publish the two catalog records directly. The invocation snapshots are
    // the behavior under test; DDL validation is covered elsewhere.
    let write = database.begin_write()?;
    {
        let mut catalog = write.open_table(CATALOG)?;
        catalog.insert(table_key.as_slice(), renamed_table.as_slice())?;
        catalog.insert(object_key.as_slice(), renamed_object.as_slice())?;
    }
    write.commit()?;

    let mut retained_names = first
        .rows
        .into_iter()
        .map(|row| (row.table_schema, row.table_name))
        .collect::<Vec<_>>();
    let mut cursor = Some(continuation);
    while let Some(next) = cursor {
        let page = retained.scan_information_schema_tables(1, Some(next))?;
        retained_names.extend(
            page.rows
                .into_iter()
                .map(|row| (row.table_schema, row.table_name)),
        );
        cursor = page.continuation;
    }
    assert!(retained_names.contains(&("app".to_owned(), "metrics".to_owned())));
    assert!(!retained_names.contains(&("app".to_owned(), "renamed_metrics".to_owned())));

    let mut current = InvocationTransaction::begin(&database, [215; 16], [9; 16], 9_999, 1, 64)?;
    let mut current_names = Vec::new();
    let mut page = current.scan_information_schema_tables(1, None)?;
    loop {
        current_names.extend(
            page.rows
                .into_iter()
                .map(|row| (row.table_schema, row.table_name)),
        );
        match page.continuation {
            Some(next) => page = current.scan_information_schema_tables(1, Some(next))?,
            None => break,
        }
    }
    assert!(!current_names.contains(&("app".to_owned(), "metrics".to_owned())));
    assert!(current_names.contains(&("app".to_owned(), "renamed_metrics".to_owned())));
    Ok(())
}

pub(super) fn information_schema_tables_cursor_retains_grant_snapshot_across_revocation()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;

    let mut retained = InvocationTransaction::begin(&database, [0xe1; 16], [9; 16], 9_999, 1, 64)?;
    let first = retained.scan_information_schema_tables(1, None)?;
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.rows[0].table_name, "columns");
    let continuation = first
        .continuation
        .expect("public relation rows precede visible application tables");

    let grantor_id = builtin_owner_id();
    let revoked_grants = [
        (
            [0x91; 16],
            grant_catalog_record([9; 16], [0x91; 16], 1, 1, [0x91; 16], grantor_id, 1).0,
        ),
        (
            [0x10; 16],
            grant_catalog_record([9; 16], [0x10; 16], 3, 1, [0x10; 16], grantor_id, 1).0,
        ),
        (
            [0x10; 16],
            grant_catalog_record([9; 16], [0x10; 16], 2, 2, [0x66; 16], grantor_id, 1).0,
        ),
    ];
    let write = database.begin_write()?;
    {
        let mut catalog = write.open_table(CATALOG)?;
        let mut grants = write.open_table(prdb_engine::schema::GRANTS)?;
        for (object_id, grant_key) in &revoked_grants {
            catalog.remove(grant_key.as_slice())?;
            for direction in [1u8, 2u8] {
                let indexed_id = if direction == 1 { [9; 16] } else { *object_id };
                let mut index_key = Vec::with_capacity(1 + 16 + grant_key.len());
                index_key.push(direction);
                index_key.extend_from_slice(&indexed_id);
                index_key.extend_from_slice(grant_key);
                grants.remove(index_key.as_slice())?;
            }
        }
    }
    write.commit()?;

    let mut retained_names = first
        .rows
        .into_iter()
        .map(|row| (row.table_schema, row.table_name))
        .collect::<Vec<_>>();
    let mut cursor = Some(continuation.clone());
    while let Some(next) = cursor {
        let page = retained.scan_information_schema_tables(1, Some(next))?;
        retained_names.extend(
            page.rows
                .into_iter()
                .map(|row| (row.table_schema, row.table_name)),
        );
        cursor = page.continuation;
    }
    assert!(retained_names.contains(&("app".to_owned(), "metrics".to_owned())));

    let mut current = InvocationTransaction::begin(&database, [0xe2; 16], [9; 16], 9_999, 1, 64)?;
    let mut current_names = Vec::new();
    let mut page = current.scan_information_schema_tables(1, None)?;
    loop {
        current_names.extend(
            page.rows
                .into_iter()
                .map(|row| (row.table_schema, row.table_name)),
        );
        match page.continuation {
            Some(next) => page = current.scan_information_schema_tables(1, Some(next))?,
            None => break,
        }
    }
    assert!(!current_names.contains(&("app".to_owned(), "metrics".to_owned())));
    assert!(
        current_names
            .iter()
            .all(|(schema, _)| schema == "information_schema")
    );

    let mut other = InvocationTransaction::begin(&database, [0xe3; 16], [9; 16], 9_999, 1, 64)?;
    assert!(matches!(
        other.scan_information_schema_tables(1, Some(continuation)),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    Ok(())
}

pub(super) fn information_schema_columns_projects_declared_shape_in_pages()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    install_second_relation_column(&database)?;
    let mut invocation = InvocationTransaction::begin(&database, [203; 16], [9; 16], 9_999, 1, 64)?;
    let first = invocation
        .scan_information_schema_columns_for_relation("app", "metrics", 1, None)?
        .expect("metrics is visible");
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.rows[0].table_schema, "app");
    assert_eq!(first.rows[0].table_name, "metrics");
    assert_eq!(first.rows[0].column_name, "value");
    assert_eq!(first.rows[0].ordinal_position, 1);
    assert_eq!(first.rows[0].data_type, "BYTEA");
    assert_eq!(first.rows[0].is_nullable, "YES");
    assert_eq!(first.rows[0].character_maximum_length, None);
    assert_eq!(first.rows[0].numeric_precision, None);
    assert_eq!(first.rows[0].numeric_scale, None);
    let second = invocation
        .scan_information_schema_columns_for_relation("app", "metrics", 1, first.continuation)?
        .expect("metrics is visible");
    assert_eq!(second.rows.len(), 1);
    assert_eq!(second.rows[0].column_name, "value2");
    assert_eq!(second.rows[0].ordinal_position, 2);
    assert_eq!(second.rows[0].is_nullable, "NO");
    assert!(second.continuation.is_none());
    assert!(
        invocation
            .scan_information_schema_columns_for_relation("private", "metrics", 1, None)?
            .is_none()
    );
    assert!(
        invocation
            .scan_information_schema_columns_for_relation("app", "missing", 1, None)?
            .is_none()
    );

    let mut cursor_probe =
        InvocationTransaction::begin(&database, [204; 16], [9; 16], 9_999, 1, 64)?;
    let projected = cursor_probe
        .scan_information_schema_columns_for_relation("app", "metrics", 1, None)?
        .expect("metrics is visible");
    let cursor = projected.continuation.expect("second column follows");
    let (role_ids, grants) = cursor_probe.load_catalog_relation_discovery_grants([0x91; 16])?;
    let scope = grants
        .iter()
        .map(|grant| (grant.operation, grant.target))
        .collect();
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids,
        scope,
    };
    assert!(matches!(
        cursor_probe.describe_authorized_columns_page(
            [0x10; 16],
            1,
            Some(cursor),
            &context,
            &grants,
            &[],
        ),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    Ok(())
}

pub(super) fn information_schema_columns_scan_walks_visible_relation_pages()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    install_second_relation_column(&database)?;
    let mut invocation = InvocationTransaction::begin(&database, [209; 16], [9; 16], 9_999, 1, 64)?;
    let first = invocation.scan_information_schema_columns(1, None)?;
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.rows[0].table_schema, "information_schema");
    assert_eq!(first.rows[0].table_name, "columns");
    assert_eq!(first.rows[0].column_name, "table_schema");
    assert_eq!(first.rows[0].ordinal_position, 1);
    let cursor = first.continuation.clone().expect("second column follows");
    let cursor_bytes = cursor.to_bytes();
    assert!(
        prdb_engine::metadata_discovery::InformationSchemaColumnsCursor::from_bytes(&cursor_bytes)
            .is_some()
    );
    assert!(prdb_engine::metadata_discovery::ColumnScanCursor::from_bytes(&cursor_bytes).is_none());
    assert!(
        prdb_engine::metadata_discovery::InformationSchemaTablesCursor::from_bytes(&cursor_bytes)
            .is_none()
    );
    assert!(prdb_engine::metadata_discovery::SchemaScanCursor::from_bytes(&cursor_bytes).is_none());
    assert!(
        prdb_engine::metadata_discovery::RelationScanCursor::from_bytes(&cursor_bytes).is_none()
    );
    let expected_columns = [
        ("information_schema", "columns", "table_schema", 1),
        ("information_schema", "columns", "table_name", 2),
        ("information_schema", "columns", "column_name", 3),
        ("information_schema", "columns", "ordinal_position", 4),
        ("information_schema", "columns", "data_type", 5),
        ("information_schema", "columns", "is_nullable", 6),
        (
            "information_schema",
            "columns",
            "character_maximum_length",
            7,
        ),
        ("information_schema", "columns", "numeric_precision", 8),
        ("information_schema", "columns", "numeric_scale", 9),
        ("information_schema", "schemata", "schema_name", 1),
        ("information_schema", "tables", "table_schema", 1),
        ("information_schema", "tables", "table_name", 2),
        ("information_schema", "tables", "table_type", 3),
        ("app", "metrics", "value", 1),
        ("app", "metrics", "value2", 2),
        ("app", "secret", "secret_value", 1),
    ];
    let mut page = first;
    for (index, (table_schema, table_name, column_name, ordinal_position)) in
        expected_columns.iter().enumerate()
    {
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0].table_schema, *table_schema);
        assert_eq!(page.rows[0].table_name, *table_name);
        assert_eq!(page.rows[0].column_name, *column_name);
        assert_eq!(page.rows[0].ordinal_position, *ordinal_position);
        if index + 1 < expected_columns.len() {
            let next = page.continuation.expect("next visible column follows");
            page = invocation.scan_information_schema_columns(1, Some(next))?;
        }
    }
    let next_schema = page.continuation.expect("visible public schema follows");
    let final_page = invocation.scan_information_schema_columns(1, Some(next_schema))?;
    assert!(final_page.rows.is_empty());
    assert!(final_page.continuation.is_none());

    let mut cross_invocation =
        InvocationTransaction::begin(&database, [210; 16], [9; 16], 9_999, 1, 64)?;
    assert!(matches!(
        cross_invocation.scan_information_schema_columns(
            1,
            Some(
                prdb_engine::metadata_discovery::InformationSchemaColumnsCursor::from_bytes(
                    &cursor_bytes,
                )
                .expect("columns scan token round trips"),
            ),
        ),
        Err(TransactionError::InvalidMetadataRequest)
    ));

    let mut replay_invocation =
        InvocationTransaction::begin(&database, [211; 16], [9; 16], 9_999, 1, 64)?;
    let first = replay_invocation.scan_information_schema_columns(1, None)?;
    let first_cursor = first.continuation.expect("second column follows");
    let second =
        replay_invocation.scan_information_schema_columns(1, Some(first_cursor.clone()))?;
    assert!(second.continuation.is_some());
    assert!(matches!(
        replay_invocation.scan_information_schema_columns(1, Some(first_cursor)),
        Err(TransactionError::InvalidMetadataRequest)
    ));

    let mut substitution_probe =
        InvocationTransaction::begin(&database, [212; 16], [9; 16], 9_999, 1, 64)?;
    let first = substitution_probe.scan_information_schema_columns(1, None)?;
    let mut substituted_cursor_bytes = first
        .continuation
        .expect("another column follows")
        .to_bytes();
    substituted_cursor_bytes[8] ^= 1;
    assert!(matches!(
        substitution_probe.scan_information_schema_columns(
            1,
            Some(
                prdb_engine::metadata_discovery::InformationSchemaColumnsCursor::from_bytes(
                    &substituted_cursor_bytes,
                )
                .expect("same-kind substituted cursor retains its wire shape"),
            ),
        ),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    Ok(())
}

pub(super) fn information_schema_scans_respect_each_principals_grants()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    install_second_relation_column(&database)?;

    let mut broader = InvocationTransaction::begin(&database, [218; 16], [9; 16], 9_999, 1, 64)?;
    let mut restricted =
        InvocationTransaction::begin(&database, [219; 16], [10; 16], 9_999, 1, 64)?;

    let broader_schemata = broader
        .scan_information_schema_schemata(128, None)?
        .rows
        .into_iter()
        .map(|row| row.schema_name)
        .collect::<Vec<_>>();
    let restricted_schemata = restricted
        .scan_information_schema_schemata(128, None)?
        .rows
        .into_iter()
        .map(|row| row.schema_name)
        .collect::<Vec<_>>();
    assert_eq!(broader_schemata, ["information_schema", "app", "public"]);
    assert_eq!(restricted_schemata, ["information_schema", "public"]);

    let mut broader_tables = Vec::new();
    let mut broader_table_cursor = None;
    loop {
        let page = broader.scan_information_schema_tables(128, broader_table_cursor)?;
        broader_tables.extend(
            page.rows
                .into_iter()
                .map(|row| (row.table_schema, row.table_name)),
        );
        broader_table_cursor = page.continuation;
        if broader_table_cursor.is_none() {
            break;
        }
    }
    let mut restricted_tables = Vec::new();
    let mut restricted_table_cursor = None;
    loop {
        let page = restricted.scan_information_schema_tables(128, restricted_table_cursor)?;
        restricted_tables.extend(
            page.rows
                .into_iter()
                .map(|row| (row.table_schema, row.table_name)),
        );
        restricted_table_cursor = page.continuation;
        if restricted_table_cursor.is_none() {
            break;
        }
    }
    assert!(broader_tables.contains(&("app".to_owned(), "metrics".to_owned())));
    assert!(broader_tables.contains(&("app".to_owned(), "secret".to_owned())));
    assert_eq!(restricted_tables.len(), 3);
    assert!(
        restricted_tables
            .iter()
            .all(|(schema, _)| schema == "information_schema")
    );

    let mut broader_columns = Vec::new();
    let mut broader_column_cursor = None;
    loop {
        let page = broader.scan_information_schema_columns(128, broader_column_cursor)?;
        broader_columns.extend(
            page.rows
                .into_iter()
                .map(|row| (row.table_schema, row.table_name, row.column_name)),
        );
        broader_column_cursor = page.continuation;
        if broader_column_cursor.is_none() {
            break;
        }
    }
    let mut restricted_columns = Vec::new();
    let mut restricted_column_cursor = None;
    loop {
        let page = restricted.scan_information_schema_columns(128, restricted_column_cursor)?;
        restricted_columns.extend(
            page.rows
                .into_iter()
                .map(|row| (row.table_schema, row.table_name, row.column_name)),
        );
        restricted_column_cursor = page.continuation;
        if restricted_column_cursor.is_none() {
            break;
        }
    }
    assert!(broader_columns.iter().any(|(schema, table, column)| {
        schema == "app" && table == "metrics" && column == "value2"
    }));
    assert!(broader_columns.iter().any(|(schema, table, column)| {
        schema == "app" && table == "secret" && column == "secret_value"
    }));
    assert_eq!(restricted_columns.len(), 13);
    assert!(
        restricted_columns
            .iter()
            .all(|(schema, _, _)| schema == "information_schema")
    );
    Ok(())
}

pub(super) fn executor_scan_reads_filtered_information_schema_schemata()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    install_second_relation_column(&database)?;
    let mut invocation = InvocationTransaction::begin(&database, [220; 16], [9; 16], 9_999, 1, 64)?;
    let mut connection = invocation.open_executor_metadata_connection();
    let (relation, description) = connection
        .resolve_relation("information_schema", "schemata")?
        .expect("public virtual relation resolves");
    assert_eq!(description.columns.len(), 1);
    assert_eq!(description.columns[0].name, "schema_name");

    let mut cursor = connection.open_relation_scan(&relation, &[])?;
    let mut names = Vec::new();
    let mut page_number = 0;
    let mut virtual_row_handle = None;
    loop {
        page_number += 1;
        let consumed_cursor = cursor.clone();
        let page = connection
            .next_relation_scan_batch(&cursor, 1, 1024)
            .map_err(|error| format!("batch {page_number} failed: {error:?}"))?;
        for (handle, cells) in page.rows {
            virtual_row_handle.get_or_insert(handle);
            match cells.as_slice() {
                [SanitizedCell::Value(ScalarValue::Text(name))] => names.push(name.clone()),
                _ => return Err("virtual schema row did not have one text cell".into()),
            }
        }
        assert!(matches!(
            connection.next_relation_scan_batch(&consumed_cursor, 1, 1024),
            Err(TransactionError::InvalidMetadataRequest)
        ));
        match page.continuation {
            Some(continuation) => cursor = continuation,
            None => break,
        }
    }
    assert_eq!(names, ["information_schema", "app", "public"]);

    let (tables_relation, _) = connection
        .resolve_relation("information_schema", "tables")?
        .expect("public tables relation resolves");
    let mut cursor = connection.open_relation_scan(&tables_relation, &[])?;
    let mut tables = Vec::new();
    let mut tables_row_handle = None;
    loop {
        let page = connection.next_relation_scan_batch(&cursor, 1, 1024)?;
        for (handle, cells) in page.rows {
            tables_row_handle.get_or_insert(handle);
            tables.push(cells);
        }
        match page.continuation {
            Some(continuation) => cursor = continuation,
            None => break,
        }
    }
    assert_eq!(tables.len(), 5);
    assert!(matches!(
        tables[0].as_slice(),
        [
            SanitizedCell::Value(ScalarValue::Text(schema)),
            SanitizedCell::Value(ScalarValue::Text(table)),
            SanitizedCell::Value(ScalarValue::Text(kind)),
        ] if schema == "information_schema" && table == "columns" && kind == "VIEW"
    ));
    assert!(matches!(
        tables[3].as_slice(),
        [
            SanitizedCell::Value(ScalarValue::Text(schema)),
            SanitizedCell::Value(ScalarValue::Text(table)),
            SanitizedCell::Value(ScalarValue::Text(kind)),
        ] if schema == "app" && table == "metrics" && kind == "BASE TABLE"
    ));

    let (columns_relation, _) = connection
        .resolve_relation("information_schema", "columns")?
        .expect("public columns relation resolves");
    let mut cursor = connection.open_relation_scan(&columns_relation, &[])?;
    let mut columns = Vec::new();
    let mut columns_row_handle = None;
    loop {
        let page = connection.next_relation_scan_batch(&cursor, 1, 1024)?;
        for (handle, cells) in page.rows {
            columns_row_handle.get_or_insert(handle);
            columns.push(cells);
        }
        match page.continuation {
            Some(continuation) => cursor = continuation,
            None => break,
        }
    }
    assert_eq!(columns.len(), 16);
    assert!(matches!(
        columns[0].as_slice(),
        [
            SanitizedCell::Value(ScalarValue::Text(schema)),
            SanitizedCell::Value(ScalarValue::Text(table)),
            SanitizedCell::Value(ScalarValue::Text(column)),
            SanitizedCell::Value(ScalarValue::Integer(1)),
            SanitizedCell::Value(ScalarValue::Text(data_type)),
            SanitizedCell::Value(ScalarValue::Text(nullable)),
            SanitizedCell::Null,
            SanitizedCell::Null,
            SanitizedCell::Null,
        ] if schema == "information_schema" && table == "columns" && column == "table_schema"
            && data_type == "TEXT" && nullable == "NO"
    ));
    assert!(matches!(
        columns[14].as_slice(),
        [
            SanitizedCell::Value(ScalarValue::Text(schema)),
            SanitizedCell::Value(ScalarValue::Text(table)),
            SanitizedCell::Value(ScalarValue::Text(column)),
            SanitizedCell::Value(ScalarValue::Integer(2)),
            ..
        ] if schema == "app" && table == "metrics" && column == "value2"
    ));
    for handle in [
        virtual_row_handle
            .as_ref()
            .expect("virtual schema row handle was issued"),
        tables_row_handle
            .as_ref()
            .expect("virtual table row handle was issued"),
        columns_row_handle
            .as_ref()
            .expect("virtual column row handle was issued"),
    ] {
        assert!(matches!(
            connection.stage_update(
                handle,
                &[FieldPatch {
                    ordinal: 0,
                    value: Some(ScalarValue::Text("forbidden".to_owned())),
                }],
            ),
            Err(TransactionError::InvalidMetadataRequest)
        ));
        assert!(matches!(
            connection.stage_delete(handle),
            Err(TransactionError::InvalidMetadataRequest)
        ));
    }
    drop(connection);

    for (index, relation_name) in ["schemata", "tables", "columns"].into_iter().enumerate() {
        let mut mutation_invocation = InvocationTransaction::begin(
            &database,
            [221 + index as u8; 16],
            [9; 16],
            9_999,
            1,
            64,
        )?;
        let mut mutation_connection = mutation_invocation.open_executor_metadata_connection();
        let (virtual_relation, description) = mutation_connection
            .resolve_relation("information_schema", relation_name)?
            .expect("public virtual relation resolves for read-only rejection");
        let insert_values = vec![InsertInput::Value(None); description.columns.len()];
        assert!(matches!(
            mutation_connection.stage_insert(&virtual_relation, &insert_values),
            Err(TransactionError::InvalidContext)
        ));
    }
    Ok(())
}

pub(super) fn column_grants_can_discover_a_relation_and_pages_are_snapshot_scoped()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let schema_id = [0x91; 16];
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id: [0x10; 16],
                    column_id: [0x66; 16],
                },
            ),
            (Operation::Delete, AccessTarget::Table([0x20; 16])),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema_id),
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id: [0x10; 16],
                column_id: [0x66; 16],
            },
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Delete,
            target: AccessTarget::Table([0x20; 16]),
        },
    ];
    let mut invocation = InvocationTransaction::begin(&database, [181; 16], [9; 16], 9_999, 1, 64)?;
    let first = invocation
        .list_authorized_relations(schema_id, 1, None, &context, &grants, &[])?
        .expect("schema is discoverable");
    assert_eq!(first.relations.len(), 1);
    assert_eq!(first.relations[0].name, "metrics");
    let cursor = first.continuation.expect("second granted relation");
    let cursor_bytes = cursor.to_bytes();
    assert!(prdb_engine::metadata_discovery::SchemaScanCursor::from_bytes(&cursor_bytes).is_none());
    assert!(prdb_engine::metadata_discovery::ColumnScanCursor::from_bytes(&cursor_bytes).is_none());
    let cursor = prdb_engine::metadata_discovery::RelationScanCursor::from_bytes(&cursor_bytes)
        .expect("relation cursor wire form is fixed width");
    let mut other_invocation =
        InvocationTransaction::begin(&database, [182; 16], [9; 16], 9_999, 1, 64)?;
    assert!(matches!(
        other_invocation.list_authorized_relations(
            schema_id,
            1,
            Some(cursor.clone()),
            &context,
            &grants,
            &[],
        ),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    let second = invocation
        .list_authorized_relations(schema_id, 1, Some(cursor.clone()), &context, &grants, &[])?
        .expect("schema remains discoverable");
    assert_eq!(second.relations.len(), 1);
    assert_eq!(second.relations[0].name, "secret");
    assert!(second.continuation.is_none());
    assert!(matches!(
        invocation.list_authorized_relations(schema_id, 1, Some(cursor), &context, &grants, &[]),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    Ok(())
}

pub(super) fn authorized_column_description_returns_only_declared_shape()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let schema_id = [0x91; 16];
    let table_id = [0x10; 16];
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (Operation::Insert, AccessTarget::Table(table_id)),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema_id),
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Insert,
            target: AccessTarget::Table(table_id),
        },
    ];
    let mut invocation = InvocationTransaction::begin(&database, [184; 16], [9; 16], 9_999, 1, 64)?;
    let description = invocation
        .describe_authorized_columns(table_id, &context, &grants, &[])?
        .expect("table write grant permits shape discovery");
    assert_eq!(description.relation_id, table_id);
    assert_eq!(description.columns.len(), 1);
    assert_eq!(
        description.columns[0],
        prdb_engine::metadata_discovery::VisibleColumn {
            column_id: [0x66; 16],
            name: "value".to_owned(),
            ordinal: 0,
            scalar_type: ScalarType::Bytea,
            nullable: true,
        }
    );

    let no_data_scope = AuthorizationContext {
        scope: std::collections::HashSet::from([(
            Operation::Usage,
            AccessTarget::Schema(schema_id),
        )]),
        ..context
    };
    assert!(
        invocation
            .describe_authorized_columns(table_id, &no_data_scope, &grants, &[])?
            .is_none()
    );
    Ok(())
}

pub(super) fn authorized_relation_resolution_requires_visibility_and_exact_name()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let mut malformed_hidden_row_key = [0x20; 32];
    malformed_hidden_row_key[16..].fill(0xfe);
    let write = database.begin_write()?;
    write
        .open_table(ROWS)?
        .insert(&malformed_hidden_row_key[..], &b"not-a-row"[..])?;
    write.commit()?;
    let schema_id = [0x91; 16];
    let table_id = [0x10; 16];
    let context = AuthorizationContext {
        principal_id: [9; 16],
        role_ids: std::collections::HashSet::new(),
        scope: std::collections::HashSet::from([
            (Operation::Usage, AccessTarget::Schema(schema_id)),
            (Operation::Insert, AccessTarget::Table(table_id)),
        ]),
    };
    let grants = [
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema(schema_id),
        },
        Grant {
            subject: Subject::Principal([9; 16]),
            operation: Operation::Insert,
            target: AccessTarget::Table(table_id),
        },
    ];
    let mut invocation = InvocationTransaction::begin(&database, [188; 16], [9; 16], 9_999, 1, 64)?;
    let resolved = invocation
        .resolve_authorized_relation(schema_id, "metrics", &context, &grants, &[])?
        .expect("visible exact name resolves");
    assert_eq!(resolved.relation_id, table_id);
    assert_eq!(resolved.name, "metrics");
    assert_eq!(resolved.columns.len(), 1);
    assert_eq!(resolved.columns[0].name, "value");
    assert!(
        invocation
            .resolve_authorized_relation(schema_id, "secret", &context, &grants, &[])?
            .is_none()
    );
    assert!(
        invocation
            .resolve_authorized_relation(schema_id, "missing", &context, &grants, &[])?
            .is_none()
    );
    assert!(
        invocation
            .resolve_authorized_relation(schema_id, "Metrics", &context, &grants, &[])?
            .is_none()
    );
    Ok(())
}

pub(super) fn catalog_authorized_qualified_relation_resolution_uses_pinned_grants()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let mut invocation = InvocationTransaction::begin(&database, [194; 16], [9; 16], 9_999, 1, 64)?;
    let resolved = invocation
        .resolve_catalog_authorized_relation("app", "metrics")?
        .expect("qualified visible relation resolves");
    assert_eq!(resolved.relation_id, [0x10; 16]);
    assert_eq!(resolved.name, "metrics");
    assert_eq!(resolved.columns.len(), 1);
    assert_eq!(resolved.columns[0].name, "value");
    assert!(
        invocation
            .resolve_catalog_authorized_relation("private", "secret")?
            .is_none()
    );
    assert!(
        invocation
            .resolve_catalog_authorized_relation("missing", "secret")?
            .is_none()
    );
    assert!(
        invocation
            .resolve_catalog_authorized_relation("app", "missing")?
            .is_none()
    );
    assert!(
        invocation
            .resolve_catalog_authorized_relation("APP", "metrics")?
            .is_none()
    );
    Ok(())
}

pub(super) fn executor_relation_handles_are_connection_scoped_and_revalidated()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    install_relation_discovery_catalog(&database)?;
    let mut invocation = InvocationTransaction::begin(&database, [215; 16], [9; 16], 9_999, 1, 64)?;
    let handle_bytes;
    {
        let mut connection = invocation.open_executor_metadata_connection();
        let (handle, description) = connection
            .resolve_relation("app", "metrics")?
            .expect("qualified visible relation resolves");
        assert_eq!(description.relation_id, [0x10; 16]);
        assert_eq!(description.columns[0].name, "value");
        handle_bytes = handle.to_bytes();
        let decoded =
            prdb_engine::metadata_discovery::ResolvedRelationHandle::from_bytes(&handle_bytes)
                .expect("opaque handle has its fixed wire form");
        assert!(
            prdb_engine::metadata_discovery::ColumnScanCursor::from_bytes(&handle_bytes).is_none()
        );
        assert!(
            prdb_engine::metadata_discovery::ResolvedRelationHandle::from_bytes(
                &handle_bytes[..15]
            )
            .is_none()
        );
        assert_eq!(
            connection.revalidate_relation_handle(&decoded)?,
            description
        );
        connection.close_relation_handle(&decoded)?;
        assert!(matches!(
            connection.revalidate_relation_handle(&decoded),
            Err(TransactionError::InvalidMetadataRequest)
        ));
    }

    let decoded =
        prdb_engine::metadata_discovery::ResolvedRelationHandle::from_bytes(&handle_bytes)
            .expect("opaque handle has its fixed wire form");
    let mut new_connection = invocation.open_executor_metadata_connection();
    assert!(matches!(
        new_connection.revalidate_relation_handle(&decoded),
        Err(TransactionError::InvalidMetadataRequest)
    ));
    drop(new_connection);

    let mut other_invocation =
        InvocationTransaction::begin(&database, [216; 16], [9; 16], 9_999, 1, 64)?;
    let mut other_connection = other_invocation.open_executor_metadata_connection();
    assert!(matches!(
        other_connection.revalidate_relation_handle(&decoded),
        Err(TransactionError::InvalidMetadataRequest)
    ));

    let mut bounded_connection = invocation.open_executor_metadata_connection();
    for _ in 0..prdb_engine::metadata_discovery::MAX_RELATION_HANDLES {
        assert!(
            bounded_connection
                .resolve_relation("app", "metrics")?
                .is_some()
        );
    }
    assert!(matches!(
        bounded_connection.resolve_relation("app", "metrics"),
        Err(TransactionError::RelationHandleLimitExceeded)
    ));
    Ok(())
}
