use prdb_engine::audit::{AuditAction, AuditEvent, AuditResult};
use prdb_engine::authorization::Expr;
use prdb_engine::catalog::{
    CatalogField, WireType, decode_registered_envelope, encode_envelope, family_schema,
};
use prdb_engine::catalog_index::{
    CatalogIndexError, lookup_principal_id_by_name, lookup_schema_id_by_name,
};
use prdb_engine::index_key::derive_index_key;
use prdb_engine::index_locator::IndexLocator;
use prdb_engine::metadata::{EngineMetadata, Lifecycle};
use prdb_engine::outcome::{InvocationOutcome, OutcomeStatus};
use prdb_engine::row_record::RowRecord;
use prdb_engine::schema::{
    AUDIT, CATALOG, ENGINE_META, INDEXES, OUTCOMES, PRINCIPAL_NAMES, ROW_INDEXES, ROWS,
    SCHEMA_NAMES,
};
use prdb_engine::startup::{
    StartupError, validate_persisted_records, validate_persisted_records_read_only,
};
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, encode_tuple};
use redb::{Database, ReadOnlyDatabase, ReadableDatabase, ReadableTable};

use super::helpers::*;

pub(super) fn rejects_schema_search_path_entries_without_schema_records() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let schema_id = builtin_id(1);
    let (schema_key, schema_value) = schema_record_with_search_path(schema_id, &[[0x55; 16]]);
    put_catalog(&database, &schema_key, &schema_value);

    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    );
}

pub(super) fn stale_principal_name_index_is_rebuilt_from_catalog() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let read = database.begin_read().unwrap();
    assert_eq!(
        lookup_principal_id_by_name(&read, "database_admin").unwrap(),
        Some(builtin_owner_id())
    );
    drop(read);

    let write = database.begin_write().unwrap();
    write
        .open_table(PRINCIPAL_NAMES)
        .unwrap()
        .insert(&b"database_admin"[..], &[0x22; 16][..])
        .unwrap();
    write
        .open_table(PRINCIPAL_NAMES)
        .unwrap()
        .insert(&b"stale_entry"[..], &[0x33; 3][..])
        .unwrap();
    write.commit().unwrap();
    let read = database.begin_read().unwrap();
    assert_eq!(
        lookup_principal_id_by_name(&read, "database_admin"),
        Err(CatalogIndexError::InvalidEntry)
    );
    drop(read);
    validate_persisted_records(&database).unwrap();
    let read = database.begin_read().unwrap();
    let catalog = read.open_table(CATALOG).unwrap();
    let principal_prefix = [10u16.to_be_bytes().as_slice(), &[1]].concat();
    let mut expected: Vec<[u8; 16]> = Vec::new();
    for entry in catalog.iter().unwrap() {
        let (key, value) = entry.unwrap();
        if !key.value().starts_with(&principal_prefix) {
            continue;
        }
        let record = decode_registered_envelope(value.value()).unwrap();
        if record
            .fields
            .iter()
            .find(|field| field.tag == 2)
            .is_some_and(|field| field.payload.as_slice() == b"database_admin")
        {
            expected.push(record.logical_key[3..].try_into().unwrap());
        }
    }
    expected.sort_unstable();
    assert_eq!(expected, vec![builtin_owner_id()]);
    assert_eq!(
        lookup_principal_id_by_name(&read, "database_admin").unwrap(),
        expected.first().copied()
    );
    assert_eq!(
        lookup_principal_id_by_name(&read, "stale_entry").unwrap(),
        None
    );
}

pub(super) fn missing_principal_name_index_is_rebuilt_from_catalog() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let write = database.begin_write().unwrap();
    assert!(write.delete_table(PRINCIPAL_NAMES).unwrap());
    write.commit().unwrap();

    validate_persisted_records(&database).unwrap();
    let read = database.begin_read().unwrap();
    assert_eq!(
        lookup_principal_id_by_name(&read, "database_admin").unwrap(),
        Some(builtin_owner_id())
    );
}

pub(super) fn stale_schema_name_index_is_rebuilt_from_catalog() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let write = database.begin_write().unwrap();
    write
        .open_table(SCHEMA_NAMES)
        .unwrap()
        .insert(&b"pg_catalog"[..], &[0x22; 16][..])
        .unwrap();
    write
        .open_table(SCHEMA_NAMES)
        .unwrap()
        .insert(&b"stale_entry"[..], &[0x33; 3][..])
        .unwrap();
    write.commit().unwrap();
    validate_persisted_records(&database).unwrap();
    let read = database.begin_read().unwrap();
    let catalog = read.open_table(CATALOG).unwrap();
    let object_prefix = [2u16.to_be_bytes().as_slice(), &[1]].concat();
    let mut expected: Vec<[u8; 16]> = Vec::new();
    for entry in catalog.iter().unwrap() {
        let (key, value) = entry.unwrap();
        if !key.value().starts_with(&object_prefix) {
            continue;
        }
        let record = decode_registered_envelope(value.value()).unwrap();
        let kind = record.fields.iter().find(|field| field.tag == 1);
        let name = record.fields.iter().find(|field| field.tag == 3);
        if kind.is_some_and(|field| field.payload.as_slice() == 1u16.to_le_bytes())
            && name.is_some_and(|field| field.payload.as_slice() == b"pg_catalog")
        {
            expected.push(record.logical_key[3..].try_into().unwrap());
        }
    }
    expected.sort_unstable();
    assert_eq!(expected, vec![builtin_id(1)]);
    assert_eq!(
        lookup_schema_id_by_name(&read, "pg_catalog").unwrap(),
        expected.first().copied()
    );
    assert_eq!(
        lookup_schema_id_by_name(&read, "stale_entry").unwrap(),
        None
    );
}

pub(super) fn missing_schema_name_index_is_rebuilt_from_catalog() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let write = database.begin_write().unwrap();
    assert!(write.delete_table(SCHEMA_NAMES).unwrap());
    write.commit().unwrap();
    validate_persisted_records(&database).unwrap();
    let read = database.begin_read().unwrap();
    assert!(
        lookup_schema_id_by_name(&read, "pg_catalog")
            .unwrap()
            .is_some()
    );
}

pub(super) fn identity_tombstones_are_valid_and_reject_live_id_reuse()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let retired_id = [0x42; 16];
    let (tombstone_key, tombstone_value) = identity_tombstone_record(retired_id);
    put_catalog(&database, &tombstone_key, &tombstone_value);
    validate_persisted_records(&database)?;

    let (principal_key, principal_value) =
        principal_record_with_name(retired_id, 1, true, 1, "retired_identity");
    let (object_key, object_value) =
        object_record_named(retired_id, 8, builtin_owner_id(), "retired_identity");
    put_catalog(&database, &principal_key, &principal_value);
    put_catalog(&database, &object_key, &object_value);
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    );
    Ok(())
}

pub(super) fn database_identity_cannot_have_an_object_record() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let database_id = [0x11; 16];
    let (schema_key, schema_value) = schema_record(database_id);
    let (object_key, object_value) = object_record(database_id, 1);
    put_catalog(&database, &schema_key, &schema_value);
    put_catalog(&database, &object_key, &object_value);

    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    );
}

pub(super) fn validates_ready_image_records_and_references()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = fixture.database(3);
    let table_id = [0x22; 16];
    let row_id = [0x33; 16];
    let key = row_key(table_id, row_id);
    insert_row(&database, &key, 2);
    put_table_and_column(&database, table_id, [0x66; 16], false);

    let (catalog_key, catalog_value) = index_definition([0x44; 16], table_id);
    let index_object = object_record([0x44; 16], 7);
    put_catalog(&database, &index_object.0, &index_object.1);
    let mut index_key = [0x44; 16].to_vec();
    index_key.extend_from_slice(&prdb_engine::index_key::encode_bytes(b"tuple-fixture"));
    index_key.extend_from_slice(&row_id);
    let locator = IndexLocator {
        row_id,
        row_version: 2,
    }
    .encode();
    let outcome = InvocationOutcome {
        status: OutcomeStatus::Committed,
        invocation_id: [0x55; 16],
        principal_id: [0x66; 16],
        commit_sequence: 3,
        retention_deadline_us: 9_999,
    }
    .encode()?;
    let audit = AuditEvent {
        event_id: [0x88; 16],
        actor_id: Some([0x66; 16]),
        correlation_id: [0x99; 16],
        occurred_at_us: 1_700_000_000_000_000,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![[0xaa; 16]],
    }
    .encode()?;
    let write = database.begin_write()?;
    write
        .open_table(CATALOG)?
        .insert(&catalog_key[..], &catalog_value[..])?;
    write
        .open_table(INDEXES)?
        .insert(&index_key[..], &locator[..])?;
    write
        .open_table(ROW_INDEXES)?
        .insert(&reverse_index_key(&key, &index_key)[..], &b""[..])?;
    write
        .open_table(OUTCOMES)?
        .insert(&[0x55; 16][..], &outcome[..])?;
    write
        .open_table(AUDIT)?
        .insert(&[0x88; 16][..], &audit[..])?;
    write.commit()?;

    let report = validate_persisted_records(&database)?;
    assert_eq!(report.metadata.commit_sequence, 3);
    assert_eq!(report.rows, 1);
    assert_eq!(report.index_entries, 1);
    assert_eq!(report.catalog_records, 35);
    assert_eq!(report.outcomes, 1);
    assert_eq!(report.audit_events, 1);
    Ok(())
}

pub(super) fn rejects_missing_membership_for_catalog_index() {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    let table_id = [0x22; 16];
    let row_id = [0x33; 16];
    insert_row(&database, &row_key(table_id, row_id), 1);
    put_table_and_column(&database, table_id, [0x66; 16], false);
    let (index_key, index_value) = index_definition([0x44; 16], table_id);
    put_catalog(&database, &index_key, &index_value);
    let index_object = object_record([0x44; 16], 7);
    put_catalog(&database, &index_object.0, &index_object.1);

    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidIndex)
    );
}

pub(super) fn rejects_index_key_that_does_not_match_its_row_tuple() {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    let table_id = [0x22; 16];
    let row_id = [0x33; 16];
    let row_key = row_key(table_id, row_id);
    insert_row(&database, &row_key, 1);
    put_table_and_column(&database, table_id, [0x66; 16], false);
    let (catalog_key, catalog_value) = index_definition([0x44; 16], table_id);
    put_catalog(&database, &catalog_key, &catalog_value);
    let index_object = object_record([0x44; 16], 7);
    put_catalog(&database, &index_object.0, &index_object.1);
    let mut index_key = [0x44; 16].to_vec();
    index_key.extend_from_slice(&prdb_engine::index_key::encode_bytes(b"different value"));
    index_key.extend_from_slice(&row_id);
    let locator = IndexLocator {
        row_id,
        row_version: 1,
    }
    .encode();
    let write = database.begin_write().unwrap();
    write
        .open_table(INDEXES)
        .unwrap()
        .insert(&index_key[..], &locator[..])
        .unwrap();
    write
        .open_table(ROW_INDEXES)
        .unwrap()
        .insert(&reverse_index_key(&row_key, &index_key)[..], &b""[..])
        .unwrap();
    write.commit().unwrap();

    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidIndex)
    );
}

pub(super) fn rejects_duplicate_non_null_keys_in_persisted_unique_indexes() {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    let table_id = [0x22; 16];
    let row_ids = [[0x33; 16], [0x34; 16]];
    for row_id in row_ids {
        insert_row(&database, &row_key(table_id, row_id), 1);
    }
    put_table_and_column(&database, table_id, [0x66; 16], false);
    let (catalog_key, catalog_value) =
        index_record_for_columns_with_unique([0x44; 16], table_id, &[[0x66; 16]], true);
    put_catalog(&database, &catalog_key, &catalog_value);
    let index_object = object_record([0x44; 16], 7);
    put_catalog(&database, &index_object.0, &index_object.1);

    let write = database.begin_write().unwrap();
    {
        let mut indexes = write.open_table(INDEXES).unwrap();
        let mut reverse = write.open_table(ROW_INDEXES).unwrap();
        for row_id in row_ids {
            let row_key = row_key(table_id, row_id);
            let mut index_key = [0x44; 16].to_vec();
            index_key.extend_from_slice(&prdb_engine::index_key::encode_bytes(b"tuple-fixture"));
            index_key.extend_from_slice(&row_id);
            let locator = IndexLocator {
                row_id,
                row_version: 1,
            }
            .encode();
            indexes.insert(&index_key[..], &locator[..]).unwrap();
            reverse
                .insert(&reverse_index_key(&row_key, &index_key)[..], &b""[..])
                .unwrap();
        }
    }
    write.commit().unwrap();

    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidIndex)
    );
}

pub(super) fn accepts_repeated_null_keys_in_persisted_unique_indexes()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    let table_id = [0x22; 16];
    let row_ids = [[0x33; 16], [0x34; 16]];
    let payload = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        }],
        1,
        &[None],
    )?;
    for row_id in row_ids {
        insert_row_payload(&database, &row_key(table_id, row_id), 1, &payload);
    }
    put_table_and_column(&database, table_id, [0x66; 16], true);
    let (catalog_key, catalog_value) =
        index_record_for_columns_with_unique([0x44; 16], table_id, &[[0x66; 16]], true);
    put_catalog(&database, &catalog_key, &catalog_value);
    let index_object = object_record([0x44; 16], 7);
    put_catalog(&database, &index_object.0, &index_object.1);

    let write = database.begin_write().unwrap();
    {
        let mut indexes = write.open_table(INDEXES).unwrap();
        let mut reverse = write.open_table(ROW_INDEXES).unwrap();
        for row_id in row_ids {
            let row_key = row_key(table_id, row_id);
            let index_key = prdb_engine::index_key::compose_key([0x44; 16], &[0xff], row_id);
            let locator = IndexLocator {
                row_id,
                row_version: 1,
            }
            .encode();
            indexes.insert(&index_key[..], &locator[..]).unwrap();
            reverse
                .insert(&reverse_index_key(&row_key, &index_key)[..], &b""[..])
                .unwrap();
        }
    }
    write.commit().unwrap();

    assert_eq!(validate_persisted_records(&database)?.rows, 2);
    Ok(())
}

pub(super) fn rejects_ready_image_without_database_bootstrap_record() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let (key, _) = database_record([0x11; 16], 2, 1, 1);
    let write = database.begin_write().unwrap();
    write.open_table(CATALOG).unwrap().remove(&key[..]).unwrap();
    write.commit().unwrap();
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));
}

pub(super) fn rejects_bootstrap_versions_that_disagree_with_engine_metadata() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let (key, value) = database_record([0x11; 16], 1, 1, 1);
    put_catalog(&database, &key, &value);
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));
}

pub(super) fn rejects_noncanonical_bootstrap_mappings() {
    for corrupt in ["missing", "unsorted", "reserved", "duplicate_id"] {
        let fixture = Fixture::new();
        let database = fixture.database(0);
        let (key, _) = database_record([0x11; 16], 2, 1, 1);
        let mut mapping = builtin_mapping();
        match corrupt {
            "missing" => {
                mapping[..4].copy_from_slice(&29u32.to_le_bytes());
                mapping.truncate(mapping.len() - 20);
            }
            "unsorted" => mapping[24..26].copy_from_slice(&1u16.to_le_bytes()),
            "reserved" => mapping[6..8].copy_from_slice(&1u16.to_le_bytes()),
            "duplicate_id" => {
                let first_id = mapping[8..24].to_vec();
                mapping[28..44].copy_from_slice(&first_id);
            }
            _ => unreachable!(),
        }
        let fields = [
            CatalogField {
                tag: 1,
                wire_type: WireType::Text,
                payload: b"test_database".to_vec(),
            },
            CatalogField {
                tag: 2,
                wire_type: WireType::U32,
                payload: 2u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Bytes,
                payload: mapping,
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::U32,
                payload: 1u32.to_le_bytes().to_vec(),
            },
        ];
        let payload = encode_envelope(&family_schema(1).unwrap(), &key, &fields).unwrap();
        put_catalog(&database, &key, &payload);
        assert!(
            matches!(
                validate_persisted_records(&database),
                Err(StartupError::InvalidCatalog)
            ),
            "corrupt mapping case: {corrupt}"
        );
    }
}

pub(super) fn accepts_credential_verifiers_with_the_supported_profile() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let user_id = [70; 16];
    let (user_key, user_value) = principal_record(user_id, 1, true, 1);
    let (object_key, object_value) = object_record(user_id, 8);
    let (verifier_key, verifier_value) =
        credential_verifier_record([71; 16], user_id, 1, 1, &[0xA5; 32]);
    for (key, value) in [
        (user_key, user_value),
        (object_key, object_value),
        (verifier_key, verifier_value),
    ] {
        put_catalog(&database, &key, &value);
    }
    assert!(validate_persisted_records(&database).is_ok());
}

pub(super) fn rejects_malformed_or_disabled_user_credential_verifiers() {
    for (login_enabled, verifier) in [(true, vec![0xA5; 31]), (false, vec![0xA5; 32])] {
        let fixture = Fixture::new();
        let database = fixture.database(0);
        let user_id = [72; 16];
        let (user_key, user_value) =
            principal_record_with_name(user_id, 1, login_enabled, 1, "credential_user");
        let (object_key, object_value) =
            object_record_named(user_id, 8, builtin_owner_id(), "credential_user");
        let (verifier_key, verifier_value) =
            credential_verifier_record([73; 16], user_id, 1, 1, &verifier);
        for (key, value) in [
            (user_key, user_value),
            (object_key, object_value),
            (verifier_key, verifier_value),
        ] {
            put_catalog(&database, &key, &value);
        }
        assert_eq!(
            validate_persisted_records(&database),
            Err(StartupError::InvalidCatalog),
        );
    }
}

pub(super) fn rejects_unsupported_credential_algorithms_and_profile_versions() {
    for (algorithm, profile_version) in [(2, 1), (1, 2)] {
        let fixture = Fixture::new();
        let database = fixture.database(0);
        let user_id = [74; 16];
        let (user_key, user_value) =
            principal_record_with_name(user_id, 1, true, 1, "versioned_credential_user");
        let (object_key, object_value) =
            object_record_named(user_id, 8, builtin_owner_id(), "versioned_credential_user");
        let (verifier_key, verifier_value) =
            credential_verifier_record([75; 16], user_id, algorithm, profile_version, &[0xA5; 32]);
        for (key, value) in [
            (user_key, user_value),
            (object_key, object_value),
            (verifier_key, verifier_value),
        ] {
            put_catalog(&database, &key, &value);
        }
        assert_eq!(
            validate_persisted_records(&database),
            Err(StartupError::InvalidCatalog),
            "algorithm={algorithm}, profile_version={profile_version}"
        );
    }
}

pub(super) fn mapped_builtin_ids_resolve_to_their_expected_typed_records()
-> Result<(), Box<dyn std::error::Error>> {
    let valid_fixture = Fixture::new();
    let valid_database = valid_fixture.database(0);
    assert!(validate_persisted_records(&valid_database).is_ok());

    let missing_fixture = Fixture::new();
    let missing_database = missing_fixture.database(0);
    let type_key = 4u16
        .to_be_bytes()
        .into_iter()
        .chain([1])
        .chain(builtin_id(101))
        .collect::<Vec<_>>();
    let write = missing_database.begin_write()?;
    assert!(
        write
            .open_table(CATALOG)?
            .remove(type_key.as_slice())?
            .is_some()
    );
    write.commit()?;
    assert_eq!(
        validate_persisted_records(&missing_database),
        Err(StartupError::InvalidCatalog),
    );

    let mistyped_fixture = Fixture::new();
    let mistyped_database = mistyped_fixture.database(0);
    let (type_key, type_value) = builtin_type_record(builtin_id(101), 2);
    put_catalog(&mistyped_database, &type_key, &type_value);
    assert_eq!(
        validate_persisted_records(&mistyped_database),
        Err(StartupError::InvalidCatalog),
    );

    let misnamed_fixture = Fixture::new();
    let misnamed_database = misnamed_fixture.database(0);
    let admin_role_id = builtin_id(3);
    let (principal_key, principal_value) =
        principal_record_with_name(admin_role_id, 2, false, 1, "other_role");
    let (object_key, object_value) =
        object_record_named(admin_role_id, 9, admin_role_id, "other_role");
    put_catalog(&misnamed_database, &principal_key, &principal_value);
    put_catalog(&misnamed_database, &object_key, &object_value);
    assert_eq!(
        validate_persisted_records(&misnamed_database),
        Err(StartupError::InvalidCatalog),
    );
    Ok(())
}

pub(super) fn rejects_objects_without_matching_typed_records()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let (object_key, object_value) = object_record([0x51; 16], 1);
    put_catalog(&database, &object_key, &object_value);
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
    );
    Ok(())
}

pub(super) fn rejects_builtin_type_records_with_unassigned_profiles()
-> Result<(), Box<dyn std::error::Error>> {
    let profiles = [
        (1, 1, 1, 1, 1),
        (0, 2, 1, 1, 1),
        (0, 1, 2, 1, 1),
        (0, 1, 1, 7, 1),
        (0, 1, 1, 1, 2),
    ];
    for (parameter_rule, scalar_encoding, comparison_version, default_mask, cast_profile) in
        profiles
    {
        let fixture = Fixture::new();
        let database = fixture.database(0);
        let (key, value) = builtin_type_record_with_profile(
            builtin_id(101),
            1,
            parameter_rule,
            scalar_encoding,
            comparison_version,
            default_mask,
            cast_profile,
        );
        put_catalog(&database, &key, &value);
        assert_eq!(
            validate_persisted_records(&database),
            Err(StartupError::InvalidCatalog),
        );
    }

    let uuid_fixture = Fixture::new();
    let uuid_database = uuid_fixture.database(0);
    let (key, value) = builtin_type_record_with_profile(builtin_id(109), 9, 0, 1, 1, 1, 1);
    put_catalog(&uuid_database, &key, &value);
    assert_eq!(
        validate_persisted_records(&uuid_database),
        Err(StartupError::InvalidCatalog),
    );
    Ok(())
}

pub(super) fn rejects_dependency_edges_with_missing_catalog_objects() {
    for present_ids in [[true, false], [false, true]] {
        let fixture = Fixture::new();
        let database = fixture.database(0);
        if present_ids[0] {
            let (key, value) = object_record([0x21; 16], 3);
            put_catalog(&database, &key, &value);
        }
        if present_ids[1] {
            let (key, value) = object_record([0x22; 16], 4);
            put_catalog(&database, &key, &value);
        }
        let (key, value) = dependency_record([0x21; 16], [0x22; 16], 1);
        put_catalog(&database, &key, &value);
        assert!(matches!(
            validate_persisted_records(&database),
            Err(StartupError::InvalidCatalog)
        ));
    }
}

pub(super) fn validates_foreign_key_target_edges_against_constraint_records()
-> Result<(), Box<dyn std::error::Error>> {
    let populate = |database: &Database,
                    edge_target: Option<[u8; 16]>,
                    add_alternate_target: bool,
                    fk_referenced_columns: &[[u8; 16]]| {
        let fk_id = [90; 16];
        let target_id = [91; 16];
        let alternate_id = [95; 16];
        put_table_and_column(database, [100; 16], [102; 16], false);
        put_table_and_column(database, [101; 16], [103; 16], false);
        let (extra_column_object_key, extra_column_object_value) = object_record([104; 16], 4);
        let (extra_column_key, extra_column_value) =
            column_record_for_table_at_ordinal([104; 16], [100; 16], "object_104", 1);
        put_catalog(
            database,
            &extra_column_object_key,
            &extra_column_object_value,
        );
        put_catalog(database, &extra_column_key, &extra_column_value);
        let (fk_key, fk_value) = object_record(fk_id, 6);
        let (target_key, target_value) = object_record(target_id, 6);
        put_catalog(database, &fk_key, &fk_value);
        put_catalog(database, &target_key, &target_value);
        if add_alternate_target {
            let (key, value) = object_record(alternate_id, 6);
            put_catalog(database, &key, &value);
        }
        let (fk_key, fk_value) = constraint_record(
            fk_id,
            [101; 16],
            5,
            &[[103; 16]],
            Some(target_id),
            fk_referenced_columns,
            None,
        );
        let (target_key, target_value) = constraint_record(
            target_id,
            [100; 16],
            2,
            &[[102; 16]],
            None,
            &[],
            Some([94; 16]),
        );
        put_catalog(database, &fk_key, &fk_value);
        put_catalog(database, &target_key, &target_value);
        let (target_object_key, target_object_value) = object_record([94; 16], 7);
        let (target_index_key, target_index_value) =
            owned_index_record([94; 16], [100; 16], [102; 16], target_id);
        put_catalog(database, &target_object_key, &target_object_value);
        put_catalog(database, &target_index_key, &target_index_value);
        let (key, value) = dependency_record(target_id, [94; 16], 4);
        put_catalog(database, &key, &value);
        if add_alternate_target {
            let (key, value) = constraint_record(
                alternate_id,
                [100; 16],
                3,
                &[[102; 16]],
                None,
                &[],
                Some([96; 16]),
            );
            put_catalog(database, &key, &value);
            let (object_key, object_value) = object_record([96; 16], 7);
            let (index_key, index_value) =
                owned_index_record([96; 16], [100; 16], [102; 16], alternate_id);
            put_catalog(database, &object_key, &object_value);
            put_catalog(database, &index_key, &index_value);
            let (key, value) = dependency_record(alternate_id, [96; 16], 4);
            put_catalog(database, &key, &value);
        }
        if let Some(edge_target) = edge_target {
            let (key, value) = dependency_record(fk_id, edge_target, 3);
            put_catalog(database, &key, &value);
        }
    };

    let missing_edge_fixture = Fixture::new();
    let missing_edge_database = missing_edge_fixture.database(0);
    populate(&missing_edge_database, None, false, &[[102; 16]]);
    assert_eq!(
        validate_persisted_records(&missing_edge_database),
        Err(StartupError::InvalidCatalog),
    );

    let mismatched_edge_fixture = Fixture::new();
    let mismatched_edge_database = mismatched_edge_fixture.database(0);
    populate(
        &mismatched_edge_database,
        Some([95; 16]),
        true,
        &[[102; 16]],
    );
    assert_eq!(
        validate_persisted_records(&mismatched_edge_database),
        Err(StartupError::InvalidCatalog),
    );

    let valid_edge_fixture = Fixture::new();
    let valid_edge_database = valid_edge_fixture.database(0);
    populate(&valid_edge_database, Some([91; 16]), false, &[[102; 16]]);
    assert!(validate_persisted_records(&valid_edge_database).is_ok());

    let matching_fixture = Fixture::new();
    let matching_database = matching_fixture.database(1);
    populate(&matching_database, Some([91; 16]), false, &[[102; 16]]);
    let target_row_id = [0x51; 16];
    let source_row_id = [0x52; 16];
    let target_column = ColumnSchema {
        scalar_type: ScalarType::Bytea,
        nullable: false,
    };
    let target_columns = [
        target_column.clone(),
        ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        },
    ];
    let target_payload = encode_tuple(
        &target_columns,
        1,
        &[Some(ScalarValue::Bytea(b"parent".to_vec())), None],
    )?;
    let target_index_key = derive_index_key(
        [94; 16],
        target_row_id,
        1,
        &[[102; 16], [104; 16]],
        &target_columns,
        &target_payload,
        &[[102; 16]],
    )?
    .key;
    let target_row_key = row_key([100; 16], target_row_id);
    let source_row_key = row_key([101; 16], source_row_id);
    let target_record = RowRecord {
        row_version: 1,
        payload: target_payload.clone(),
    }
    .encode()?;
    let source_record = RowRecord {
        row_version: 1,
        payload: encode_tuple(
            std::slice::from_ref(&target_column),
            1,
            &[Some(ScalarValue::Bytea(b"parent".to_vec()))],
        )?,
    }
    .encode()?;
    let locator = IndexLocator {
        row_id: target_row_id,
        row_version: 1,
    }
    .encode();
    let reverse_key = reverse_index_key(&target_row_key, &target_index_key);
    let write = matching_database.begin_write()?;
    write
        .open_table(ROWS)?
        .insert(&target_row_key[..], &target_record[..])?;
    write
        .open_table(INDEXES)?
        .insert(target_index_key.as_slice(), locator.as_slice())?;
    write
        .open_table(ROW_INDEXES)?
        .insert(reverse_key.as_slice(), &[][..])?;
    write.commit()?;
    let target_result = validate_persisted_records(&matching_database);
    assert!(target_result.is_ok(), "target row: {target_result:?}");
    let write = matching_database.begin_write()?;
    write
        .open_table(ROWS)?
        .insert(&source_row_key[..], &source_record[..])?;
    write.commit()?;
    let matching_result = validate_persisted_records(&matching_database);
    assert!(matching_result.is_ok(), "{matching_result:?}");

    let dangling_fixture = Fixture::new();
    let dangling_database = dangling_fixture.database(1);
    populate(&dangling_database, Some([91; 16]), false, &[[102; 16]]);
    let dangling_payload = encode_tuple(
        std::slice::from_ref(&target_column),
        1,
        &[Some(ScalarValue::Bytea(b"missing".to_vec()))],
    )?;
    insert_row_payload(
        &dangling_database,
        &row_key([101; 16], [0x53; 16]),
        1,
        &dangling_payload,
    );
    assert_eq!(
        validate_persisted_records(&dangling_database),
        Err(StartupError::InvalidForeignKey)
    );

    let null_fixture = Fixture::new();
    let null_database = null_fixture.database(1);
    populate(&null_database, Some([91; 16]), false, &[[102; 16]]);
    let (source_column_key, source_column) =
        column_record_for_table_with_nullability([103; 16], [101; 16], "object_103", true);
    put_catalog(&null_database, &source_column_key, &source_column);
    let nullable_source_column = ColumnSchema {
        scalar_type: ScalarType::Bytea,
        nullable: true,
    };
    let null_payload = encode_tuple(std::slice::from_ref(&nullable_source_column), 1, &[None])?;
    insert_row_payload(
        &null_database,
        &row_key([101; 16], [0x54; 16]),
        1,
        &null_payload,
    );
    assert!(validate_persisted_records(&null_database).is_ok());

    let mismatched_types_fixture = Fixture::new();
    let mismatched_types_database = mismatched_types_fixture.database(0);
    populate(
        &mismatched_types_database,
        Some([91; 16]),
        false,
        &[[102; 16]],
    );
    let (source_type_key, source_type) = column_record_for_table_with_type(
        [103; 16],
        [101; 16],
        "object_103",
        builtin_id(109),
        false,
    );
    put_catalog(&mismatched_types_database, &source_type_key, &source_type);
    assert_eq!(
        validate_persisted_records(&mismatched_types_database),
        Err(StartupError::InvalidCatalog)
    );

    let mismatched_columns_fixture = Fixture::new();
    let mismatched_columns_database = mismatched_columns_fixture.database(0);
    populate(
        &mismatched_columns_database,
        Some([91; 16]),
        false,
        &[[104; 16]],
    );
    assert_eq!(
        validate_persisted_records(&mismatched_columns_database),
        Err(StartupError::InvalidCatalog),
    );

    let bounded_fixture = Fixture::new();
    let bounded_database = bounded_fixture.database(1);
    populate(&bounded_database, Some([91; 16]), false, &[[102; 16]]);
    let (source_column_key, source_column) =
        column_record_for_table_with_nullability([103; 16], [101; 16], "object_103", true);
    put_catalog(&bounded_database, &source_column_key, &source_column);
    let null_payload = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        }],
        1,
        &[None],
    )?;
    let null_row = RowRecord {
        row_version: 1,
        payload: null_payload,
    }
    .encode()?;
    let write = bounded_database.begin_write()?;
    let mut rows = write.open_table(ROWS)?;
    for row_number in 0..65_537u64 {
        let mut row_id = [0; 16];
        row_id[8..].copy_from_slice(&row_number.to_be_bytes());
        let key = row_key([101; 16], row_id);
        rows.insert(&key[..], &null_row[..])?;
    }
    drop(rows);
    write.commit()?;
    assert_eq!(
        validate_persisted_records(&bounded_database),
        Err(StartupError::ForeignKeyValidationLimitExceeded)
    );
    Ok(())
}

pub(super) fn validates_backing_index_edges_against_constraint_and_index_records() {
    let populate = |database: &Database,
                    index_edge: Option<[u8; 16]>,
                    add_alternate_index: bool| {
        let constraint_id = [110; 16];
        let index_id = [111; 16];
        let alternate_constraint_id = [112; 16];
        let alternate_index_id = [113; 16];
        put_table_and_column(database, [100; 16], [102; 16], false);
        let (constraint_object_key, constraint_object_value) = object_record(constraint_id, 6);
        let (index_object_key, index_object_value) = object_record(index_id, 7);
        put_catalog(database, &constraint_object_key, &constraint_object_value);
        put_catalog(database, &index_object_key, &index_object_value);
        let (constraint_key, constraint_value) = constraint_record(
            constraint_id,
            [100; 16],
            2,
            &[[102; 16]],
            None,
            &[],
            Some(index_id),
        );
        let (index_key, index_value) =
            owned_index_record(index_id, [100; 16], [102; 16], constraint_id);
        put_catalog(database, &constraint_key, &constraint_value);
        put_catalog(database, &index_key, &index_value);
        if add_alternate_index {
            let (object_key, object_value) = object_record(alternate_constraint_id, 6);
            put_catalog(database, &object_key, &object_value);
            let (object_key, object_value) = object_record(alternate_index_id, 7);
            put_catalog(database, &object_key, &object_value);
            let (constraint_key, constraint_value) = constraint_record(
                alternate_constraint_id,
                [100; 16],
                3,
                &[[102; 16]],
                None,
                &[],
                Some(alternate_index_id),
            );
            let (index_key, index_value) = owned_index_record(
                alternate_index_id,
                [100; 16],
                [102; 16],
                alternate_constraint_id,
            );
            put_catalog(database, &constraint_key, &constraint_value);
            put_catalog(database, &index_key, &index_value);
            let (key, value) = dependency_record(alternate_constraint_id, alternate_index_id, 4);
            put_catalog(database, &key, &value);
        }
        if let Some(index_id) = index_edge {
            let (key, value) = dependency_record(constraint_id, index_id, 4);
            put_catalog(database, &key, &value);
        }
    };

    let missing_edge_fixture = Fixture::new();
    let missing_edge_database = missing_edge_fixture.database(0);
    populate(&missing_edge_database, None, false);
    assert_eq!(
        validate_persisted_records(&missing_edge_database),
        Err(StartupError::InvalidCatalog),
    );

    let mismatched_edge_fixture = Fixture::new();
    let mismatched_edge_database = mismatched_edge_fixture.database(0);
    populate(&mismatched_edge_database, Some([113; 16]), true);
    assert_eq!(
        validate_persisted_records(&mismatched_edge_database),
        Err(StartupError::InvalidCatalog),
    );

    let valid_edge_fixture = Fixture::new();
    let valid_edge_database = valid_edge_fixture.database(0);
    populate(&valid_edge_database, Some([111; 16]), false);
    assert!(validate_persisted_records(&valid_edge_database).is_ok());
}

pub(super) fn validates_expression_reference_edges_against_typed_catalog_records() {
    let populate = |database: &Database, edges: &[([u8; 16], [u8; 16])]| {
        let column_id = [120; 16];
        let default_expression_id = [121; 16];
        let field_expression_id = [122; 16];
        let check_constraint_id = [123; 16];
        let check_expression_id = [124; 16];
        let policy_id = [125; 16];
        let policy_expression_id = [126; 16];
        let add_column_default_id = [127; 16];
        let (key, value) = object_record(column_id, 4);
        put_catalog(database, &key, &value);
        for expression_id in [
            default_expression_id,
            field_expression_id,
            check_expression_id,
            policy_expression_id,
            add_column_default_id,
        ] {
            let (key, value) = object_record(expression_id, 5);
            put_catalog(database, &key, &value);
        }
        let (key, value) = object_record(check_constraint_id, 6);
        put_catalog(database, &key, &value);
        let table = object_record([130; 16], 3);
        put_catalog(database, &table.0, &table.1);
        let table = table_record([130; 16]);
        put_catalog(database, &table.0, &table.1);
        let (key, value) = object_record(policy_id, 10);
        put_catalog(database, &key, &value);
        let (key, value) = column_record_with_schema(
            column_id,
            [130; 16],
            "object_120",
            Some(default_expression_id),
            Some(add_column_default_id),
            true,
        );
        put_catalog(database, &key, &value);
        let (key, value) =
            expression_record(default_expression_id, 1, &[], &Expr::CurrentPrincipalId);
        put_catalog(database, &key, &value);
        let (key, value) = expression_record(field_expression_id, 2, &[column_id], &Expr::Field(0));
        put_catalog(database, &key, &value);
        let (key, value) =
            expression_record(check_expression_id, 2, &[], &Expr::CurrentPrincipalId);
        put_catalog(database, &key, &value);
        let (key, value) =
            expression_record(policy_expression_id, 3, &[], &Expr::CurrentPrincipalId);
        put_catalog(database, &key, &value);
        let (key, value) = expression_record(add_column_default_id, 1, &[], &Expr::Literal(None));
        put_catalog(database, &key, &value);
        let (key, value) =
            check_constraint_record(check_constraint_id, [130; 16], check_expression_id);
        put_catalog(database, &key, &value);
        let (key, value) = policy_record(policy_id, [130; 16], policy_expression_id);
        put_catalog(database, &key, &value);
        for (dependent_id, referenced_id) in edges {
            let (key, value) = dependency_record(*dependent_id, *referenced_id, 2);
            put_catalog(database, &key, &value);
        }
    };

    let missing_edge_fixture = Fixture::new();
    let missing_edge_database = missing_edge_fixture.database(0);
    populate(&missing_edge_database, &[([122; 16], [120; 16])]);
    assert_eq!(
        validate_persisted_records(&missing_edge_database),
        Err(StartupError::InvalidCatalog),
    );

    let mismatched_edge_fixture = Fixture::new();
    let mismatched_edge_database = mismatched_edge_fixture.database(0);
    populate(
        &mismatched_edge_database,
        &[
            ([120; 16], [121; 16]),
            ([120; 16], [127; 16]),
            ([120; 16], [122; 16]),
            ([122; 16], [120; 16]),
            ([123; 16], [124; 16]),
            ([125; 16], [126; 16]),
        ],
    );
    assert_eq!(
        validate_persisted_records(&mismatched_edge_database),
        Err(StartupError::InvalidCatalog),
    );

    let valid_edge_fixture = Fixture::new();
    let valid_edge_database = valid_edge_fixture.database(0);
    populate(
        &valid_edge_database,
        &[
            ([120; 16], [121; 16]),
            ([120; 16], [127; 16]),
            ([122; 16], [120; 16]),
            ([123; 16], [124; 16]),
            ([125; 16], [126; 16]),
        ],
    );
    assert!(validate_persisted_records(&valid_edge_database).is_ok());
}

pub(super) fn rejects_objects_with_missing_owner_principals() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let (key, value) = object_record_with_owner([0x51; 16], 3, [0x52; 16]);
    put_catalog(&database, &key, &value);
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));
}

pub(super) fn accepts_owned_child_dependency_with_catalog_object_endpoints()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    for record in [
        object_record([0x21; 16], 3),
        object_record_with_parent([0x22; 16], 4, Some([0x21; 16])),
        table_record([0x21; 16]),
        column_record_for_table([0x22; 16], [0x21; 16], "object_34"),
    ] {
        put_catalog(&database, &record.0, &record.1);
    }
    let (key, value) = dependency_record([0x22; 16], [0x21; 16], 1);
    put_catalog(&database, &key, &value);
    let report = validate_persisted_records(&database)?;
    assert_eq!(report.catalog_records, 34);
    Ok(())
}

pub(super) fn rejects_owned_child_edges_that_disagree_with_object_parents() {
    for edge_parent in [None, Some([0x23; 16])] {
        let fixture = Fixture::new();
        let database = fixture.database(0);
        for (key, value) in [
            object_record([0x21; 16], 3),
            object_record([0x23; 16], 3),
            object_record_with_parent([0x22; 16], 4, Some([0x21; 16])),
        ] {
            put_catalog(&database, &key, &value);
        }
        if let Some(parent_id) = edge_parent {
            let (key, value) = dependency_record([0x22; 16], parent_id, 1);
            put_catalog(&database, &key, &value);
        }
        assert!(matches!(
            validate_persisted_records(&database),
            Err(StartupError::InvalidCatalog)
        ));
    }
}

pub(super) fn rejects_column_object_parent_that_disagrees_with_typed_table() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let table_id = [0xa1; 16];
    let declared_parent_id = [0xa2; 16];
    let column_id = [0xa3; 16];
    put_table(&database, table_id);
    put_table(&database, declared_parent_id);
    let column_object = object_record_with_parent(column_id, 4, Some(declared_parent_id));
    put_catalog(&database, &column_object.0, &column_object.1);
    let column = column_record_for_table(column_id, table_id, "object_163");
    put_catalog(&database, &column.0, &column.1);
    let edge = dependency_record(column_id, declared_parent_id, 1);
    put_catalog(&database, &edge.0, &edge.1);

    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
        "typed column ownership must agree with any explicit object parent"
    );
}

pub(super) fn rejects_index_object_parent_that_disagrees_with_typed_table_schema() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let actual_schema_id = builtin_id(1);
    let wrong_schema_id = builtin_id(2);
    let table_id = [0xc1; 16];
    let column_id = [0xc2; 16];
    let index_id = [0xc3; 16];
    for (key, value) in [
        object_record_with_parent(table_id, 3, Some(actual_schema_id)),
        table_record_for_schema(table_id, actual_schema_id),
        object_record(column_id, 4),
        column_record_for_table(column_id, table_id, "object_194"),
        object_record_with_parent(index_id, 7, Some(wrong_schema_id)),
        index_record_for_columns(index_id, table_id, &[column_id]),
        dependency_record(table_id, actual_schema_id, 1),
        dependency_record(index_id, wrong_schema_id, 1),
    ] {
        put_catalog(&database, &key, &value);
    }

    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
        "an index parent's schema must match its typed table's schema"
    );
}

pub(super) fn read_only_startup_rejects_table_object_parent_that_disagrees_with_typed_schema() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let table_id = [0xc4; 16];
    let column_id = [0xc5; 16];
    let actual_schema_id = builtin_id(1);
    let wrong_schema_id = builtin_id(2);
    for (key, value) in [
        object_record_with_parent(table_id, 3, Some(wrong_schema_id)),
        table_record_for_schema(table_id, actual_schema_id),
        object_record(column_id, 4),
        column_record_for_table(column_id, table_id, "object_197"),
        dependency_record(table_id, wrong_schema_id, 1),
    ] {
        put_catalog(&database, &key, &value);
    }
    drop(database);

    let read_only = ReadOnlyDatabase::open(fixture.path()).unwrap();
    assert_eq!(
        validate_persisted_records_read_only(&read_only),
        Err(StartupError::InvalidCatalog),
        "read-only recovery validation must check the typed table namespace too"
    );
    drop(read_only);

    let writable = Database::open(fixture.path()).unwrap();
    assert_eq!(
        validate_persisted_records(&writable),
        Err(StartupError::InvalidCatalog),
        "writable startup validation must reject the same inconsistent graph"
    );
}

pub(super) fn rejects_owned_child_parent_cycles() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    for (key, value) in [
        object_record_with_parent([0x31; 16], 3, Some([0x32; 16])),
        object_record_with_parent([0x32; 16], 4, Some([0x31; 16])),
    ] {
        put_catalog(&database, &key, &value);
    }
    for (dependent, parent) in [([0x31; 16], [0x32; 16]), ([0x32; 16], [0x31; 16])] {
        let (key, value) = dependency_record(dependent, parent, 1);
        put_catalog(&database, &key, &value);
    }
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));
}

pub(super) fn rejects_unknown_dependency_edge_kinds() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    for (key, value) in [object_record([0x41; 16], 3), object_record([0x42; 16], 4)] {
        put_catalog(&database, &key, &value);
    }
    let (mut key, mut value) = dependency_record([0x41; 16], [0x42; 16], 5);
    key[37..39].copy_from_slice(&6u16.to_be_bytes());
    value[32 + 37..32 + 39].copy_from_slice(&6u16.to_be_bytes());
    put_catalog(&database, &key, &value);
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));
}

pub(super) fn rejects_typed_catalog_records_without_a_matching_object_record() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let (key, value) = index_definition([0x44; 16], [0x22; 16]);
    put_catalog(&database, &key, &value);
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));
}

pub(super) fn rejects_typed_catalog_records_with_a_different_object_kind() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let index = index_definition([0x44; 16], [0x22; 16]);
    let object = object_record([0x44; 16], 3);
    put_catalog(&database, &index.0, &index.1);
    put_catalog(&database, &object.0, &object.1);
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));
}

pub(super) fn rejects_principal_records_with_missing_or_mismatched_objects() {
    for object_kind in [None, Some(9)] {
        let fixture = Fixture::new();
        let database = fixture.database(0);
        let principal = principal_record([0x31; 16], 1, true, 1);
        put_catalog(&database, &principal.0, &principal.1);
        if let Some(kind) = object_kind {
            let object = object_record([0x31; 16], kind);
            put_catalog(&database, &object.0, &object.1);
        }
        assert!(matches!(
            validate_persisted_records(&database),
            Err(StartupError::InvalidCatalog)
        ));
    }
}

pub(super) fn rejects_column_and_principal_names_that_disagree_with_objects() {
    let principal_fixture = Fixture::new();
    let principal_database = principal_fixture.database(0);
    let owner_id = builtin_owner_id();
    let (principal_key, principal_value) =
        principal_record_with_name(owner_id, 2, false, 1, "renamed_role");
    put_catalog(&principal_database, &principal_key, &principal_value);
    assert_eq!(
        validate_persisted_records(&principal_database),
        Err(StartupError::InvalidCatalog),
    );

    let column_fixture = Fixture::new();
    let column_database = column_fixture.database(0);
    let column_id = [75; 16];
    let (column_key, column_value) = column_record(column_id, "different_column_name");
    let (object_key, object_value) = object_record(column_id, 4);
    put_catalog(&column_database, &column_key, &column_value);
    put_catalog(&column_database, &object_key, &object_value);
    assert_eq!(
        validate_persisted_records(&column_database),
        Err(StartupError::InvalidCatalog),
    );
}

pub(super) fn rejects_duplicate_names_across_users_and_roles() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let user_id = [76; 16];
    let (principal_key, principal_value) =
        principal_record_with_name(user_id, 1, true, 1, "database_admin");
    let (object_key, object_value) =
        object_record_named(user_id, 8, builtin_owner_id(), "database_admin");
    put_catalog(&database, &principal_key, &principal_value);
    put_catalog(&database, &object_key, &object_value);

    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog),
    );
}

pub(super) fn rejects_row_versions_beyond_the_commit_frontier() {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    insert_row(&database, &row_key([0x22; 16], [0x33; 16]), 2);
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::SequenceAhead)
    ));
}

pub(super) fn rejects_rows_with_invalid_or_mismatched_typed_payloads() {
    let malformed = b"not a TUP1 tuple".to_vec();
    let wrong_schema_version = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: false,
        }],
        2,
        &[Some(ScalarValue::Bytea(b"value".to_vec()))],
    )
    .unwrap();
    let wrong_column_count = encode_tuple(
        &[
            ColumnSchema {
                scalar_type: ScalarType::Bytea,
                nullable: false,
            },
            ColumnSchema {
                scalar_type: ScalarType::Bytea,
                nullable: false,
            },
        ],
        1,
        &[
            Some(ScalarValue::Bytea(b"first".to_vec())),
            Some(ScalarValue::Bytea(b"second".to_vec())),
        ],
    )
    .unwrap();
    let null_in_nonnullable_column = encode_tuple(
        &[ColumnSchema {
            scalar_type: ScalarType::Bytea,
            nullable: true,
        }],
        1,
        &[None],
    )
    .unwrap();

    for payload in [
        malformed,
        wrong_schema_version,
        wrong_column_count,
        null_in_nonnullable_column,
    ] {
        let fixture = Fixture::new();
        let database = fixture.database(1);
        let table_id = [0x22; 16];
        put_table_and_column(&database, table_id, [0x66; 16], false);
        insert_row_payload(&database, &row_key(table_id, [0x33; 16]), 1, &payload);
        assert_eq!(
            validate_persisted_records(&database),
            Err(StartupError::InvalidRow)
        );
    }
}

pub(super) fn rejects_rows_without_a_catalog_table_schema() {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    insert_row(&database, &row_key([0x22; 16], [0x33; 16]), 1);
    assert_eq!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidRow)
    );
}

pub(super) fn rejects_zero_object_ids_in_row_keys() {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    insert_row(&database, &row_key([0; 16], [0x33; 16]), 1);
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidRow)
    ));
}

pub(super) fn rejects_index_without_its_catalog_definition() {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    let row_id = [0x33; 16];
    let mut index_key = [0x44; 16].to_vec();
    index_key.extend_from_slice(&[1, 0x80, 0, 0, 1]);
    index_key.extend_from_slice(&row_id);
    let locator = IndexLocator {
        row_id,
        row_version: 1,
    }
    .encode();
    let write = database.begin_write().unwrap();
    write
        .open_table(INDEXES)
        .unwrap()
        .insert(&index_key[..], &locator[..])
        .unwrap();
    write.commit().unwrap();
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidIndex)
    ));
}

pub(super) fn rejects_index_without_its_referenced_row() {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    let table_id = [0x22; 16];
    let row_id = [0x33; 16];
    put_table_and_column(&database, table_id, [0x66; 16], false);
    let (catalog_key, catalog_value) = index_definition([0x44; 16], table_id);
    put_catalog(&database, &catalog_key, &catalog_value);
    let index_object = object_record([0x44; 16], 7);
    put_catalog(&database, &index_object.0, &index_object.1);
    let mut index_key = [0x44; 16].to_vec();
    index_key.extend_from_slice(&[1, 0x80, 0, 0, 1]);
    index_key.extend_from_slice(&row_id);
    let locator = IndexLocator {
        row_id,
        row_version: 1,
    }
    .encode();
    let write = database.begin_write().unwrap();
    write
        .open_table(INDEXES)
        .unwrap()
        .insert(&index_key[..], &locator[..])
        .unwrap();
    write
        .open_table(ROW_INDEXES)
        .unwrap()
        .insert(
            &reverse_index_key(&row_key(table_id, row_id), &index_key)[..],
            &b""[..],
        )
        .unwrap();
    write.commit().unwrap();
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidIndex)
    ));
}

pub(super) fn rejects_a_locator_whose_version_differs_from_its_row() {
    let fixture = Fixture::new();
    let database = fixture.database(2);
    let table_id = [0x22; 16];
    let row_id = [0x33; 16];
    let key = row_key(table_id, row_id);
    insert_row(&database, &key, 2);
    put_table_and_column(&database, table_id, [0x66; 16], false);
    let (catalog_key, catalog_value) = index_definition([0x44; 16], table_id);
    put_catalog(&database, &catalog_key, &catalog_value);
    let index_object = object_record([0x44; 16], 7);
    put_catalog(&database, &index_object.0, &index_object.1);
    let mut index_key = [0x44; 16].to_vec();
    index_key.extend_from_slice(&[1, 0x80, 0, 0, 1]);
    index_key.extend_from_slice(&row_id);
    let locator = IndexLocator {
        row_id,
        row_version: 1,
    }
    .encode();
    let write = database.begin_write().unwrap();
    write
        .open_table(INDEXES)
        .unwrap()
        .insert(&index_key[..], &locator[..])
        .unwrap();
    write
        .open_table(ROW_INDEXES)
        .unwrap()
        .insert(&reverse_index_key(&key, &index_key)[..], &b""[..])
        .unwrap();
    write.commit().unwrap();
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidIndex)
    ));
}

pub(super) fn rejects_catalog_record_stored_under_a_different_key() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let (key, value) = index_definition([0x44; 16], [0x22; 16]);
    let mut wrong_key = key;
    wrong_key[18] ^= 1;
    put_catalog(&database, &wrong_key, &value);
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidCatalog)
    ));
}

pub(super) fn rejects_a_committed_outcome_beyond_the_commit_frontier() {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    let outcome = InvocationOutcome {
        status: OutcomeStatus::Committed,
        invocation_id: [0x55; 16],
        principal_id: [0x66; 16],
        commit_sequence: 2,
        retention_deadline_us: 9_999,
    }
    .encode()
    .unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(OUTCOMES)
        .unwrap()
        .insert(&[0x55; 16][..], &outcome[..])
        .unwrap();
    write.commit().unwrap();
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidOutcome)
    ));
}

pub(super) fn initializing_database_is_not_ready_for_normal_startup() {
    let fixture = Fixture::new();
    let database = fixture.database(0);
    let metadata = EngineMetadata {
        database_id: [0x11; 16],
        catalog_version: 2,
        expression_version: 1,
        builtin_semantics_version: 1,
        commit_sequence: 0,
        lifecycle: Lifecycle::Initializing,
    }
    .encode()
    .unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(ENGINE_META)
        .unwrap()
        .insert(&b"format"[..], &metadata[..])
        .unwrap();
    write.commit().unwrap();
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::NotReady)
    ));
}

pub(super) fn rejects_outcome_key_value_mismatch() {
    let fixture = Fixture::new();
    let database = fixture.database(1);
    let outcome = InvocationOutcome {
        status: OutcomeStatus::Committed,
        invocation_id: [0x55; 16],
        principal_id: [0x66; 16],
        commit_sequence: 1,
        retention_deadline_us: 9_999,
    }
    .encode()
    .unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(OUTCOMES)
        .unwrap()
        .insert(&[0x54; 16][..], &outcome[..])
        .unwrap();
    write.commit().unwrap();
    assert!(matches!(
        validate_persisted_records(&database),
        Err(StartupError::InvalidOutcome)
    ));
}
