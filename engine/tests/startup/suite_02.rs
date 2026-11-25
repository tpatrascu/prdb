use prdb_engine::authorization::Expr;
use prdb_engine::catalog::{CatalogField, WireType, encode_envelope, family_schema};
use prdb_engine::startup::{StartupError, validate_persisted_records};
use redb::Database;

use super::helpers::*;

pub(super) fn accepts_pinned_acyclic_membership_graph() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    for record in [
        principal_record([1; 16], 1, true, 1),
        principal_record([2; 16], 2, false, 1),
        principal_record([3; 16], 2, false, 1),
        membership_record([1; 16], [2; 16], [1; 16], 1),
        membership_record([2; 16], [3; 16], [1; 16], 1),
        dependency_record([1; 16], [2; 16], 5),
        dependency_record([2; 16], [3; 16], 5),
    ] {
        put_catalog(&database, &record.0, &record.1);
    }
    for (principal_id, kind) in [([1; 16], 8), ([2; 16], 9), ([3; 16], 9)] {
        let object = object_record(principal_id, kind);
        put_catalog(&database, &object.0, &object.1);
    }
    let report = validate_persisted_records(&database)?;
    assert_eq!(report.catalog_records, 39);
    Ok(())
}

pub(super) fn authority_dependencies_must_exactly_match_membership_records() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    for record in [
        principal_record([11; 16], 1, true, 1),
        principal_record([12; 16], 2, false, 1),
        membership_record([11; 16], [12; 16], [11; 16], 1),
    ] {
        put_catalog(&database, &record.0, &record.1);
    }
    for (principal_id, kind) in [([11; 16], 8), ([12; 16], 9)] {
        let object = object_record(principal_id, kind);
        put_catalog(&database, &object.0, &object.1);
    }
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));

    let fixture = Fixture::new();
    let database = fixture.database(0);
    for record in [
        principal_record([11; 16], 1, true, 1),
        principal_record([12; 16], 2, false, 1),
        membership_record([11; 16], [12; 16], [11; 16], 1),
        dependency_record([11; 16], [12; 16], 5),
        dependency_record([12; 16], [11; 16], 5),
    ] {
        put_catalog(&database, &record.0, &record.1);
    }
    for (principal_id, kind) in [([11; 16], 8), ([12; 16], 9)] {
        let object = object_record(principal_id, kind);
        put_catalog(&database, &object.0, &object.1);
    }
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));
}

pub(super) fn authority_dependencies_cover_grants_and_policy_recipients()
-> Result<(), Box<dyn std::error::Error>> {
    let seed = |database: &Database, include_edges: bool| {
        let user_id = [13; 16];
        let table_id = [14; 16];
        let policy_id = [15; 16];
        let user = principal_record(user_id, 1, true, 1);
        put_catalog(database, &user.0, &user.1);
        for (object_id, kind) in [(user_id, 8), (table_id, 3), (policy_id, 10)] {
            let record = object_record(object_id, kind);
            put_catalog(database, &record.0, &record.1);
        }
        let table = table_record(table_id);
        put_catalog(database, &table.0, &table.1);
        let grant = grant_record(user_id, table_id, 2, 1, table_id, builtin_owner_id());
        put_catalog(database, &grant.0, &grant.1);
        let system_grant = grant_record(user_id, [0x11; 16], 12, 1, [0x11; 16], builtin_owner_id());
        put_catalog(database, &system_grant.0, &system_grant.1);
        let policy_key = {
            let mut key = 13u16.to_be_bytes().to_vec();
            key.push(1);
            key.extend_from_slice(&policy_id);
            key
        };
        let policy_fields = [
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
                tag: 7,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
        ];
        let policy =
            encode_envelope(&family_schema(13).unwrap(), &policy_key, &policy_fields).unwrap();
        put_catalog(database, &policy_key, &policy);
        let target = policy_target_record(policy_id, 1, user_id);
        put_catalog(database, &target.0, &target.1);
        if include_edges {
            for (dependent, referenced) in [
                (user_id, table_id),
                (user_id, [0x11; 16]),
                (policy_id, user_id),
            ] {
                let edge = dependency_record(dependent, referenced, 5);
                put_catalog(database, &edge.0, &edge.1);
            }
        }
    };

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, false);
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, true);
    validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn policy_column_targets_resolve_to_cell_policy_table_columns()
-> Result<(), Box<dyn std::error::Error>> {
    let seed = |database: &Database, policy_kind: u16, column_table: Option<[u8; 16]>| {
        let table_id = [140; 16];
        let other_table_id = [141; 16];
        let column_id = [142; 16];
        let expression_id = [143; 16];
        let policy_id = [144; 16];

        for table_id_to_add in [table_id, other_table_id] {
            let object = object_record(table_id_to_add, 3);
            put_catalog(database, &object.0, &object.1);
            let table = table_record(table_id_to_add);
            put_catalog(database, &table.0, &table.1);
        }

        let policy_object = object_record(policy_id, 10);
        put_catalog(database, &policy_object.0, &policy_object.1);
        let policy = policy_record_with_kind(policy_id, table_id, expression_id, policy_kind);
        put_catalog(database, &policy.0, &policy.1);

        let expression_object = object_record(expression_id, 5);
        put_catalog(database, &expression_object.0, &expression_object.1);
        let expression = expression_record(expression_id, 3, &[], &Expr::CurrentPrincipalId);
        put_catalog(database, &expression.0, &expression.1);
        let expression_edge = dependency_record(policy_id, expression_id, 2);
        put_catalog(database, &expression_edge.0, &expression_edge.1);

        let column_object = object_record(column_id, 4);
        put_catalog(database, &column_object.0, &column_object.1);
        if let Some(column_table) = column_table {
            let column = column_record_for_table(column_id, column_table, "object_142");
            put_catalog(database, &column.0, &column.1);
        }

        let recipient = policy_target_record(policy_id, 2, builtin_owner_id());
        put_catalog(database, &recipient.0, &recipient.1);
        let column_target = policy_target_record(policy_id, 3, column_id);
        put_catalog(database, &column_target.0, &column_target.1);
        let authority_edge = dependency_record(policy_id, builtin_owner_id(), 5);
        put_catalog(database, &authority_edge.0, &authority_edge.1);
    };

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, 2, Some([140; 16]));
    assert!(validate_persisted_records(&database).is_ok());

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, 2, None);
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
    );

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, 2, Some([141; 16]));
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
    );

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, 1, Some([140; 16]));
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
    );
    Ok(())
}

pub(super) fn constraints_resolve_to_source_columns_on_their_table()
-> Result<(), Box<dyn std::error::Error>> {
    let seed = |database: &Database,
                constraint_table: [u8; 16],
                column_table: Option<[u8; 16]>,
                column_nullable: bool| {
        let table_id = [150; 16];
        let other_table_id = [151; 16];
        let column_id = [152; 16];
        let constraint_id = [153; 16];

        for table_id_to_add in [table_id, other_table_id] {
            let object = object_record(table_id_to_add, 3);
            put_catalog(database, &object.0, &object.1);
            let table = table_record(table_id_to_add);
            put_catalog(database, &table.0, &table.1);
        }
        let column_object = object_record(column_id, 4);
        put_catalog(database, &column_object.0, &column_object.1);
        if let Some(column_table) = column_table {
            let column = column_record_for_table_with_nullability(
                column_id,
                column_table,
                "object_152",
                column_nullable,
            );
            put_catalog(database, &column.0, &column.1);
        }
        let constraint_object = object_record(constraint_id, 6);
        put_catalog(database, &constraint_object.0, &constraint_object.1);
        let constraint = constraint_record(
            constraint_id,
            constraint_table,
            1,
            &[[152; 16]],
            None,
            &[],
            None,
        );
        put_catalog(database, &constraint.0, &constraint.1);
    };

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, [150; 16], Some([150; 16]), false);
    assert!(validate_persisted_records(&database).is_ok());

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, [150; 16], None, false);
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
    );

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, [150; 16], Some([151; 16]), false);
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
    );

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, [154; 16], Some([150; 16]), false);
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
    );

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, [150; 16], Some([150; 16]), true);
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
    );
    Ok(())
}

pub(super) fn rejects_nullable_primary_key_columns_at_startup() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let table_id = [0x31; 16];
    let column_id = [0x32; 16];
    let constraint_id = [0x33; 16];
    let index_id = [0x34; 16];
    put_table_and_column(&database, table_id, column_id, true);
    let constraint = constraint_record(
        constraint_id,
        table_id,
        2,
        &[column_id],
        None,
        &[],
        Some(index_id),
    );
    let index = owned_index_record(index_id, table_id, column_id, constraint_id);
    let constraint_object = object_record(constraint_id, 6);
    let index_object = object_record(index_id, 7);
    let dependency = dependency_record(constraint_id, index_id, 4);
    for (key, value) in [
        constraint,
        index,
        constraint_object,
        index_object,
        dependency,
    ] {
        put_catalog(&database, &key, &value);
    }

    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    );
}

pub(super) fn indexes_resolve_to_columns_on_their_table() -> Result<(), Box<dyn std::error::Error>>
{
    let seed = |database: &Database, index_table: [u8; 16], column_table: Option<[u8; 16]>| {
        let table_id = [160; 16];
        let other_table_id = [161; 16];
        let index_id = [162; 16];
        let column_id = [163; 16];
        put_table(database, table_id);
        put_table(database, other_table_id);
        if let Some(column_table) = column_table {
            let column_object = object_record(column_id, 4);
            put_catalog(database, &column_object.0, &column_object.1);
            let column = column_record_for_table(column_id, column_table, "object_163");
            put_catalog(database, &column.0, &column.1);
        }
        let index_object = object_record(index_id, 7);
        put_catalog(database, &index_object.0, &index_object.1);
        let index = index_record_for_columns(index_id, index_table, &[column_id]);
        put_catalog(database, &index.0, &index.1);
    };

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, [160; 16], Some([160; 16]));
    assert!(validate_persisted_records(&database).is_ok());

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, [160; 16], None);
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
    );

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, [160; 16], Some([161; 16]));
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
    );

    let fixture = Fixture::new();
    let database = fixture.database(0);
    seed(&database, [164; 16], Some([160; 16]));
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
    );
    Ok(())
}

pub(super) fn rejects_cycles_and_invalid_membership_endpoints_at_startup() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    for record in [
        principal_record([1; 16], 1, true, 1),
        principal_record([2; 16], 2, false, 1),
        principal_record([3; 16], 2, false, 1),
        membership_record([2; 16], [3; 16], [1; 16], 1),
        membership_record([3; 16], [2; 16], [1; 16], 1),
    ] {
        put_catalog(&database, &record.0, &record.1);
    }
    for (principal_id, kind) in [([1; 16], 8), ([2; 16], 9), ([3; 16], 9)] {
        let object = object_record(principal_id, kind);
        put_catalog(&database, &object.0, &object.1);
    }
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));

    let fixture = Fixture::new();
    let database = fixture.database(0);
    for record in [
        principal_record([1; 16], 1, true, 1),
        principal_record([2; 16], 1, true, 1),
        membership_record([1; 16], [2; 16], [1; 16], 1),
    ] {
        put_catalog(&database, &record.0, &record.1);
    }
    for principal_id in [[1; 16], [2; 16]] {
        let object = object_record(principal_id, 8);
        put_catalog(&database, &object.0, &object.1);
    }
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));
}
