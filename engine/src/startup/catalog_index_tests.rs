use super::{
    ensure_column_lookup_index, ensure_dependency_index, ensure_grant_index,
    ensure_index_name_index, ensure_membership_index, ensure_object_owner_index,
    ensure_policy_index, ensure_relation_name_index, ensure_table_constraint_index,
    ensure_table_index_index, validate_name_domains,
};
use crate::catalog::{
    CatalogField, WireType, decode_registered_envelope, encode_envelope, family_schema,
};
use crate::catalog_index::{
    lookup_column_id_by_name, lookup_column_id_by_ordinal, lookup_dependencies_for_dependent,
    lookup_dependents_for_referenced, lookup_grants_for_grantee, lookup_grants_for_object,
    lookup_index_id_by_name, lookup_members_for_role, lookup_policies_for_table_operation,
    lookup_relation_id_by_name, lookup_roles_for_member, scan_column_ids_for_table,
    scan_constraint_ids_for_table, scan_index_ids_for_table, scan_object_ids_for_owner,
    scan_relation_ids_for_schema,
};
use crate::schema::{
    CATALOG, COLUMN_LOOKUPS, DEPENDENCIES, GRANTS, INDEX_NAMES, MEMBERSHIPS, OBJECT_OWNERS,
    POLICIES, RELATION_NAMES, TABLE_CONSTRAINTS, TABLE_INDEXES, create_tables,
};
use redb::{Database, ReadableDatabase, ReadableTable};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);

fn database() -> Database {
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "prdb-relation-index-{}-{id}.redb",
        std::process::id()
    ));
    let database = Database::create(path).unwrap();
    let write = database.begin_write().unwrap();
    create_tables(&write).unwrap();

    let schema_id = [0x31; 16];
    let relation_id = [0x32; 16];
    let mut table_key = 5u16.to_be_bytes().to_vec();
    table_key.push(1);
    table_key.extend_from_slice(&relation_id);
    let table_fields = [
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
            payload: [0x33; 16].to_vec(),
        },
        CatalogField {
            tag: 5,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
    ];
    let table_value =
        encode_envelope(&family_schema(5).unwrap(), &table_key, &table_fields).unwrap();
    let mut object_key = 2u16.to_be_bytes().to_vec();
    object_key.push(1);
    object_key.extend_from_slice(&relation_id);
    let object_fields = [
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
            payload: b"orders".to_vec(),
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::Id,
            payload: [0x34; 16].to_vec(),
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
    ];
    let object_value =
        encode_envelope(&family_schema(2).unwrap(), &object_key, &object_fields).unwrap();
    let index_id = [0x69; 16];
    let mut index_object_key = 2u16.to_be_bytes().to_vec();
    index_object_key.push(1);
    index_object_key.extend_from_slice(&index_id);
    let index_object_fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::U16,
            payload: 7u16.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::Id,
            payload: schema_id.to_vec(),
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::Text,
            payload: b"orders_idx".to_vec(),
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::Id,
            payload: [0x34; 16].to_vec(),
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
    ];
    let index_object_value = encode_envelope(
        &family_schema(2).unwrap(),
        &index_object_key,
        &index_object_fields,
    )
    .unwrap();
    let mut membership_key = 11u16.to_be_bytes().to_vec();
    membership_key.push(1);
    membership_key.extend_from_slice(&[0x41; 16]);
    membership_key.push(1);
    membership_key.extend_from_slice(&[0x42; 16]);
    let membership_fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: [0x43; 16].to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::U64,
            payload: 1u64.to_le_bytes().to_vec(),
        },
    ];
    let membership_value = encode_envelope(
        &family_schema(11).unwrap(),
        &membership_key,
        &membership_fields,
    )
    .unwrap();
    let grantee_id = [0x51; 16];
    let object_id = [0x52; 16];
    let target_id = object_id;
    let mut grant_key = 12u16.to_be_bytes().to_vec();
    grant_key.push(1);
    grant_key.extend_from_slice(&grantee_id);
    grant_key.push(1);
    grant_key.extend_from_slice(&object_id);
    grant_key.push(1);
    grant_key.extend_from_slice(&1u16.to_be_bytes());
    grant_key.push(1);
    grant_key.extend_from_slice(&1u16.to_be_bytes());
    grant_key.push(1);
    grant_key.extend_from_slice(&target_id);
    let grant_fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: [0x54; 16].to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::U64,
            payload: 1u64.to_le_bytes().to_vec(),
        },
    ];
    let grant_value =
        encode_envelope(&family_schema(12).unwrap(), &grant_key, &grant_fields).unwrap();
    let policy_id = [0x62; 16];
    let mut policy_key = 13u16.to_be_bytes().to_vec();
    policy_key.push(1);
    policy_key.extend_from_slice(&policy_id);
    let policy_fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: relation_id.to_vec(),
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
            payload: [0x63; 16].to_vec(),
        },
        CatalogField {
            tag: 7,
            wire_type: WireType::U64,
            payload: 1u64.to_le_bytes().to_vec(),
        },
    ];
    let policy_value =
        encode_envelope(&family_schema(13).unwrap(), &policy_key, &policy_fields).unwrap();
    let dependent_id = [0x65; 16];
    let referenced_id = [0x66; 16];
    let mut dependency_key = 15u16.to_be_bytes().to_vec();
    dependency_key.push(1);
    dependency_key.extend_from_slice(&dependent_id);
    dependency_key.push(1);
    dependency_key.extend_from_slice(&referenced_id);
    dependency_key.push(1);
    dependency_key.extend_from_slice(&5u16.to_be_bytes());
    let dependency_value =
        encode_envelope(&family_schema(15).unwrap(), &dependency_key, &[]).unwrap();
    let column_id = [0x67; 16];
    let mut column_key = 6u16.to_be_bytes().to_vec();
    column_key.push(1);
    column_key.extend_from_slice(&column_id);
    let column_fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: relation_id.to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::U32,
            payload: 0u32.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::Text,
            payload: b"order_id".to_vec(),
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::Id,
            payload: [0x68; 16].to_vec(),
        },
        CatalogField {
            tag: 5,
            wire_type: WireType::Bytes,
            payload: [0; 8].to_vec(),
        },
        CatalogField {
            tag: 6,
            wire_type: WireType::Bool,
            payload: vec![1],
        },
        CatalogField {
            tag: 8,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 9,
            wire_type: WireType::U64,
            payload: 1u64.to_le_bytes().to_vec(),
        },
    ];
    let column_value =
        encode_envelope(&family_schema(6).unwrap(), &column_key, &column_fields).unwrap();
    let mut catalog = write.open_table(CATALOG).unwrap();
    catalog
        .insert(table_key.as_slice(), table_value.as_slice())
        .unwrap();
    catalog
        .insert(object_key.as_slice(), object_value.as_slice())
        .unwrap();
    catalog
        .insert(index_object_key.as_slice(), index_object_value.as_slice())
        .unwrap();
    catalog
        .insert(membership_key.as_slice(), membership_value.as_slice())
        .unwrap();
    catalog
        .insert(grant_key.as_slice(), grant_value.as_slice())
        .unwrap();
    catalog
        .insert(policy_key.as_slice(), policy_value.as_slice())
        .unwrap();
    catalog
        .insert(dependency_key.as_slice(), dependency_value.as_slice())
        .unwrap();
    catalog
        .insert(column_key.as_slice(), column_value.as_slice())
        .unwrap();
    drop(catalog);
    write.commit().unwrap();
    database
}

#[test]
fn stale_relation_name_index_is_rebuilt_from_catalog_records() {
    let database = database();
    let mut key = vec![0x31; 16];
    key.extend_from_slice(b"orders");
    let write = database.begin_write().unwrap();
    let mut index = write.open_table(RELATION_NAMES).unwrap();
    index.insert(key.as_slice(), &[0x55; 16][..]).unwrap();
    index.insert(&b"stale"[..], &[0x44; 3][..]).unwrap();
    drop(index);
    write.commit().unwrap();
    ensure_relation_name_index(&database).unwrap();
    let read = database.begin_read().unwrap();
    let catalog = read.open_table(CATALOG).unwrap();
    let object_prefix = [2u16.to_be_bytes().as_slice(), &[1]].concat();
    let mut expected_name_ids: Vec<[u8; 16]> = Vec::new();
    let mut expected_schema_relations: Vec<[u8; 16]> = Vec::new();
    let relation_prefix = [5u16.to_be_bytes().as_slice(), &[1]].concat();
    for entry in catalog.iter().unwrap() {
        let (key, value) = entry.unwrap();
        let key = key.value();
        if key.starts_with(&object_prefix) {
            let record = decode_registered_envelope(value.value()).unwrap();
            let field = |tag| {
                record
                    .fields
                    .iter()
                    .find(|field| field.tag == tag)
                    .unwrap()
                    .payload
                    .as_slice()
            };
            if field(1) == 3u16.to_le_bytes() && field(2) == [0x31; 16] && field(3) == b"orders" {
                expected_name_ids.push(record.logical_key[3..].try_into().unwrap());
            }
        }
        if key.starts_with(&relation_prefix) {
            let record = decode_registered_envelope(value.value()).unwrap();
            if record
                .fields
                .iter()
                .find(|field| field.tag == 1)
                .is_some_and(|field| field.payload.as_slice() == [0x31; 16])
            {
                expected_schema_relations.push(record.logical_key[3..].try_into().unwrap());
            }
        }
    }
    expected_name_ids.sort_unstable();
    expected_schema_relations.sort_unstable();
    assert_eq!(expected_name_ids, vec![[0x32; 16]]);
    assert_eq!(expected_schema_relations, vec![[0x32; 16]]);
    assert_eq!(
        lookup_relation_id_by_name(&read, [0x31; 16], "orders").unwrap(),
        expected_name_ids.first().copied()
    );
    assert_eq!(
        scan_relation_ids_for_schema(&read, [0x31; 16], 1).unwrap(),
        expected_schema_relations
    );
    assert!(
        read.open_table(RELATION_NAMES)
            .unwrap()
            .get(&b"stale"[..])
            .unwrap()
            .is_none()
    );
}

#[test]
fn missing_relation_name_index_is_rebuilt_from_catalog_records() {
    let database = database();
    let write = database.begin_write().unwrap();
    assert!(write.delete_table(RELATION_NAMES).unwrap());
    write.commit().unwrap();
    ensure_relation_name_index(&database).unwrap();
    let read = database.begin_read().unwrap();
    assert_eq!(
        lookup_relation_id_by_name(&read, [0x31; 16], "orders").unwrap(),
        Some([0x32; 16])
    );
}

#[test]
fn object_owner_scan_rebuilds_and_uses_the_owner_prefix() {
    let database = database();
    let write = database.begin_write().unwrap();
    assert!(write.delete_table(OBJECT_OWNERS).unwrap());
    write.commit().unwrap();

    ensure_object_owner_index(&database).unwrap();
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
        if record
            .fields
            .iter()
            .find(|field| field.tag == 4)
            .is_some_and(|field| field.payload.as_slice() == [0x34; 16])
        {
            expected.push(record.logical_key[3..].try_into().unwrap());
        }
    }
    expected.sort_unstable();
    assert_eq!(expected, vec![[0x32; 16], [0x69; 16]]);
    assert_eq!(
        scan_object_ids_for_owner(&read, [0x34; 16], 2).unwrap(),
        expected
    );
    assert_eq!(
        scan_object_ids_for_owner(&read, [0x34; 16], 1),
        Err(crate::catalog_index::CatalogIndexError::RecordLimitExceeded)
    );
    assert_eq!(
        read.open_table(OBJECT_OWNERS)
            .unwrap()
            .iter()
            .unwrap()
            .count(),
        2
    );
}

#[test]
fn stale_membership_directions_are_rebuilt_from_catalog_records() {
    let database = database();
    let write = database.begin_write().unwrap();
    write
        .open_table(MEMBERSHIPS)
        .unwrap()
        .insert(&[1, 0x41][..], &b"stale"[..])
        .unwrap();
    write.commit().unwrap();
    ensure_membership_index(&database).unwrap();
    let read = database.begin_read().unwrap();
    assert_eq!(
        lookup_roles_for_member(&read, [0x41; 16]).unwrap(),
        vec![[0x42; 16]]
    );
    assert_eq!(
        lookup_members_for_role(&read, [0x42; 16]).unwrap(),
        vec![[0x41; 16]]
    );
}

#[test]
fn missing_membership_index_is_rebuilt_from_catalog_records() {
    let database = database();
    let write = database.begin_write().unwrap();
    assert!(write.delete_table(MEMBERSHIPS).unwrap());
    write.commit().unwrap();
    ensure_membership_index(&database).unwrap();
    let read = database.begin_read().unwrap();
    assert_eq!(
        lookup_roles_for_member(&read, [0x41; 16]).unwrap(),
        vec![[0x42; 16]]
    );
    assert_eq!(
        lookup_members_for_role(&read, [0x42; 16]).unwrap(),
        vec![[0x41; 16]]
    );
}

#[test]
fn stale_grant_indexes_are_rebuilt_from_catalog_records() {
    let database = database();
    let mut grantee_key = vec![1, 0x51];
    grantee_key.resize(17, 0x51);
    grantee_key.extend_from_slice(&[0x61; 59]);
    let write = database.begin_write().unwrap();
    write
        .open_table(GRANTS)
        .unwrap()
        .insert(grantee_key.as_slice(), &b"stale"[..])
        .unwrap();
    write
        .open_table(GRANTS)
        .unwrap()
        .insert(&b"stale"[..], &b"stale"[..])
        .unwrap();
    write.commit().unwrap();
    ensure_grant_index(&database).unwrap();
    let read = database.begin_read().unwrap();
    assert_eq!(
        lookup_grants_for_grantee(&read, [0x51; 16]).unwrap().len(),
        1
    );
    assert_eq!(
        lookup_grants_for_object(&read, [0x52; 16]).unwrap().len(),
        1
    );
    assert!(
        read.open_table(GRANTS)
            .unwrap()
            .get(&b"stale"[..])
            .unwrap()
            .is_none()
    );
}

#[test]
fn missing_grant_indexes_are_rebuilt_from_catalog_records() {
    let database = database();
    let write = database.begin_write().unwrap();
    assert!(write.delete_table(GRANTS).unwrap());
    write.commit().unwrap();
    ensure_grant_index(&database).unwrap();
    let read = database.begin_read().unwrap();
    assert_eq!(
        lookup_grants_for_grantee(&read, [0x51; 16]).unwrap().len(),
        1
    );
    assert_eq!(
        lookup_grants_for_object(&read, [0x52; 16]).unwrap().len(),
        1
    );
}

#[test]
fn stale_dependency_directions_are_rebuilt_from_catalog_records() {
    let database = database();
    let write = database.begin_write().unwrap();
    write
        .open_table(DEPENDENCIES)
        .unwrap()
        .insert(&[1, 0x65][..], &b"stale"[..])
        .unwrap();
    write.commit().unwrap();
    ensure_dependency_index(&database).unwrap();
    let read = database.begin_read().unwrap();
    assert_eq!(
        lookup_dependencies_for_dependent(&read, [0x65; 16])
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        lookup_dependents_for_referenced(&read, [0x66; 16])
            .unwrap()
            .len(),
        1
    );
    assert!(
        read.open_table(DEPENDENCIES)
            .unwrap()
            .get(&[1, 0x65][..])
            .unwrap()
            .is_none()
    );
}

#[test]
fn missing_dependency_index_is_rebuilt_from_catalog_records() {
    let database = database();
    let write = database.begin_write().unwrap();
    assert!(write.delete_table(DEPENDENCIES).unwrap());
    write.commit().unwrap();
    ensure_dependency_index(&database).unwrap();
    let read = database.begin_read().unwrap();
    assert_eq!(
        lookup_dependencies_for_dependent(&read, [0x65; 16])
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        lookup_dependents_for_referenced(&read, [0x66; 16])
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn missing_column_lookups_are_rebuilt_from_catalog_records() {
    let database = database();
    let write = database.begin_write().unwrap();
    assert!(write.delete_table(COLUMN_LOOKUPS).unwrap());
    write.commit().unwrap();
    ensure_column_lookup_index(&database).unwrap();
    let read = database.begin_read().unwrap();
    let catalog = read.open_table(CATALOG).unwrap();
    let column_prefix = [6u16.to_be_bytes().as_slice(), &[1]].concat();
    let mut expected_by_name: Vec<[u8; 16]> = Vec::new();
    let mut expected_by_ordinal: Vec<[u8; 16]> = Vec::new();
    for entry in catalog.iter().unwrap() {
        let (key, value) = entry.unwrap();
        if !key.value().starts_with(&column_prefix) {
            continue;
        }
        let record = decode_registered_envelope(value.value()).unwrap();
        let field = |tag| {
            record
                .fields
                .iter()
                .find(|field| field.tag == tag)
                .unwrap()
                .payload
                .as_slice()
        };
        if field(1) == [0x32; 16] && field(3) == b"order_id" {
            expected_by_name.push(record.logical_key[3..].try_into().unwrap());
        }
        if field(1) == [0x32; 16] && u32::from_le_bytes(field(2).try_into().unwrap()) == 0 {
            expected_by_ordinal.push(record.logical_key[3..].try_into().unwrap());
        }
    }
    expected_by_name.sort_unstable();
    expected_by_ordinal.sort_unstable();
    assert_eq!(expected_by_name, vec![[0x67; 16]]);
    assert_eq!(expected_by_ordinal, vec![[0x67; 16]]);
    assert_eq!(
        lookup_column_id_by_name(&read, [0x32; 16], "order_id").unwrap(),
        expected_by_name.first().copied()
    );
    assert_eq!(
        lookup_column_id_by_ordinal(&read, [0x32; 16], 0).unwrap(),
        expected_by_ordinal.first().copied()
    );
}

#[test]
fn table_column_scan_is_bounded_to_its_ordinal_prefix() {
    let database = database();
    ensure_column_lookup_index(&database).unwrap();
    let write = database.begin_write().unwrap();
    let mut index = write.open_table(COLUMN_LOOKUPS).unwrap();
    for ordinal in 0u32..4 {
        let mut key = vec![2];
        key.extend_from_slice(&[0x99; 16]);
        key.extend_from_slice(&ordinal.to_be_bytes());
        let indexed_id = [0xf0u8 + ordinal as u8; 16];
        index.insert(key.as_slice(), &indexed_id[..]).unwrap();
    }
    drop(index);
    write.commit().unwrap();

    let read = database.begin_read().unwrap();
    let catalog = read.open_table(CATALOG).unwrap();
    let column_prefix = [6u16.to_be_bytes().as_slice(), &[1]].concat();
    let mut expected: Vec<[u8; 16]> = Vec::new();
    for entry in catalog.iter().unwrap() {
        let (key, value) = entry.unwrap();
        if !key.value().starts_with(&column_prefix) {
            continue;
        }
        let record = decode_registered_envelope(value.value()).unwrap();
        if record
            .fields
            .iter()
            .find(|field| field.tag == 1)
            .is_some_and(|field| field.payload.as_slice() == [0x32; 16])
        {
            expected.push(record.logical_key[3..].try_into().unwrap());
        }
    }
    expected.sort_unstable();
    assert_eq!(expected, vec![[0x67; 16]]);
    assert_eq!(
        scan_column_ids_for_table(&read, [0x32; 16], 1).unwrap(),
        expected
    );
    assert_eq!(
        scan_column_ids_for_table(&read, [0x32; 16], 0),
        Err(crate::catalog_index::CatalogIndexError::InvalidName)
    );
}

#[test]
fn table_index_scan_rebuilds_and_uses_the_table_prefix() {
    let database = database();
    let write = database.begin_write().unwrap();
    let mut catalog = write.open_table(CATALOG).unwrap();
    for (index_id, table_id) in [([0x69; 16], [0x32; 16]), ([0x70; 16], [0x88; 16])] {
        let mut key = 9u16.to_be_bytes().to_vec();
        key.push(1);
        key.extend_from_slice(&index_id);
        let mut columns = 1u32.to_le_bytes().to_vec();
        columns.extend_from_slice(&[0x67; 16]);
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
                payload: vec![0],
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 7,
                wire_type: WireType::Id,
                payload: [0x77; 16].to_vec(),
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
        let value = encode_envelope(&family_schema(9).unwrap(), &key, &fields).unwrap();
        catalog.insert(key.as_slice(), value.as_slice()).unwrap();
    }
    drop(catalog);
    drop(write.open_table(TABLE_INDEXES).unwrap());
    write.delete_table(TABLE_INDEXES).unwrap();
    write.commit().unwrap();

    ensure_table_index_index(&database).unwrap();
    let read = database.begin_read().unwrap();
    let catalog = read.open_table(CATALOG).unwrap();
    let index_prefix = [9u16.to_be_bytes().as_slice(), &[1]].concat();
    let mut expected: Vec<[u8; 16]> = Vec::new();
    for entry in catalog.iter().unwrap() {
        let (key, value) = entry.unwrap();
        if !key.value().starts_with(&index_prefix) {
            continue;
        }
        let record = decode_registered_envelope(value.value()).unwrap();
        if record
            .fields
            .iter()
            .find(|field| field.tag == 1)
            .is_some_and(|field| field.payload.as_slice() == [0x32; 16])
        {
            expected.push(record.logical_key[3..].try_into().unwrap());
        }
    }
    expected.sort_unstable();
    assert_eq!(expected, vec![[0x69; 16]]);
    assert_eq!(
        scan_index_ids_for_table(&read, [0x32; 16], 1).unwrap(),
        expected
    );
    assert_eq!(
        scan_index_ids_for_table(&read, [0x32; 16], 0),
        Err(crate::catalog_index::CatalogIndexError::InvalidName)
    );
    assert_eq!(
        read.open_table(TABLE_INDEXES)
            .unwrap()
            .iter()
            .unwrap()
            .count(),
        2
    );
}

#[test]
fn table_constraint_scan_rebuilds_and_uses_the_table_prefix() {
    let database = database();
    let write = database.begin_write().unwrap();
    let mut catalog = write.open_table(CATALOG).unwrap();
    for (constraint_id, table_id) in [([0x69; 16], [0x32; 16]), ([0x70; 16], [0x88; 16])] {
        let mut key = 8u16.to_be_bytes().to_vec();
        key.push(1);
        key.extend_from_slice(&constraint_id);
        let mut columns = 1u32.to_le_bytes().to_vec();
        columns.extend_from_slice(&[0x67; 16]);
        let empty_ids = 0u32.to_le_bytes().to_vec();
        let fields = [
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
                wire_type: WireType::Ids,
                payload: columns,
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Ids,
                payload: empty_ids,
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
        ];
        let value = encode_envelope(&family_schema(8).unwrap(), &key, &fields).unwrap();
        catalog.insert(key.as_slice(), value.as_slice()).unwrap();
    }
    drop(catalog);
    drop(write.open_table(TABLE_CONSTRAINTS).unwrap());
    write.delete_table(TABLE_CONSTRAINTS).unwrap();
    write.commit().unwrap();

    ensure_table_constraint_index(&database).unwrap();
    let read = database.begin_read().unwrap();
    let catalog = read.open_table(CATALOG).unwrap();
    let constraint_prefix = [8u16.to_be_bytes().as_slice(), &[1]].concat();
    let mut expected: Vec<[u8; 16]> = Vec::new();
    for entry in catalog.iter().unwrap() {
        let (key, value) = entry.unwrap();
        if !key.value().starts_with(&constraint_prefix) {
            continue;
        }
        let record = decode_registered_envelope(value.value()).unwrap();
        if record
            .fields
            .iter()
            .find(|field| field.tag == 1)
            .is_some_and(|field| field.payload.as_slice() == [0x32; 16])
        {
            expected.push(record.logical_key[3..].try_into().unwrap());
        }
    }
    expected.sort_unstable();
    assert_eq!(expected, vec![[0x69; 16]]);
    assert_eq!(
        scan_constraint_ids_for_table(&read, [0x32; 16], 1).unwrap(),
        expected
    );
    assert_eq!(
        scan_constraint_ids_for_table(&read, [0x32; 16], 0),
        Err(crate::catalog_index::CatalogIndexError::InvalidName)
    );
    assert_eq!(
        read.open_table(TABLE_CONSTRAINTS)
            .unwrap()
            .iter()
            .unwrap()
            .count(),
        2
    );
}

#[test]
fn stale_column_lookups_are_rebuilt_from_catalog_records() {
    let database = database();
    let write = database.begin_write().unwrap();
    write
        .open_table(COLUMN_LOOKUPS)
        .unwrap()
        .insert(&[1, 0x32][..], &b"stale"[..])
        .unwrap();
    write.commit().unwrap();
    ensure_column_lookup_index(&database).unwrap();
    let read = database.begin_read().unwrap();
    assert_eq!(
        lookup_column_id_by_name(&read, [0x32; 16], "order_id").unwrap(),
        Some([0x67; 16])
    );
    assert_eq!(
        lookup_column_id_by_ordinal(&read, [0x32; 16], 0).unwrap(),
        Some([0x67; 16])
    );
    assert!(
        read.open_table(COLUMN_LOOKUPS)
            .unwrap()
            .get(&[1, 0x32][..])
            .unwrap()
            .is_none()
    );
}

#[test]
fn missing_index_name_lookup_is_rebuilt_from_catalog_records() {
    let database = database();
    let write = database.begin_write().unwrap();
    assert!(write.delete_table(INDEX_NAMES).unwrap());
    write.commit().unwrap();
    ensure_index_name_index(&database).unwrap();
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
        let field = |tag| {
            record
                .fields
                .iter()
                .find(|field| field.tag == tag)
                .unwrap()
                .payload
                .as_slice()
        };
        if field(1) == 7u16.to_le_bytes() && field(2) == [0x31; 16] && field(3) == b"orders_idx" {
            expected.push(record.logical_key[3..].try_into().unwrap());
        }
    }
    expected.sort_unstable();
    assert_eq!(expected, vec![[0x69; 16]]);
    assert_eq!(
        lookup_index_id_by_name(&read, [0x31; 16], "orders_idx").unwrap(),
        expected.first().copied()
    );
}

#[test]
fn stale_index_name_lookup_is_rebuilt_from_catalog_records() {
    let database = database();
    let write = database.begin_write().unwrap();
    write
        .open_table(INDEX_NAMES)
        .unwrap()
        .insert(&[0x31, 0x73][..], &[0x77; 16][..])
        .unwrap();
    write.commit().unwrap();
    ensure_index_name_index(&database).unwrap();
    let read = database.begin_read().unwrap();
    assert_eq!(
        lookup_index_id_by_name(&read, [0x31; 16], "orders_idx").unwrap(),
        Some([0x69; 16])
    );
    assert!(
        read.open_table(INDEX_NAMES)
            .unwrap()
            .get(&[0x31, 0x73][..])
            .unwrap()
            .is_none()
    );
}

#[test]
fn startup_name_domains_reject_relation_index_collisions() {
    let database = database();
    let index_id = [0x69; 16];
    let index_key = {
        let mut key = 2u16.to_be_bytes().to_vec();
        key.push(1);
        key.extend_from_slice(&index_id);
        key
    };
    let conflict_fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::U16,
            payload: 7u16.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::Id,
            payload: [0x31; 16].to_vec(),
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::Text,
            payload: b"orders".to_vec(),
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::Id,
            payload: [0x34; 16].to_vec(),
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
    ];
    let conflict_value =
        encode_envelope(&family_schema(2).unwrap(), &index_key, &conflict_fields).unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(CATALOG)
        .unwrap()
        .insert(index_key.as_slice(), conflict_value.as_slice())
        .unwrap();
    write.commit().unwrap();
    ensure_relation_name_index(&database).unwrap();
    ensure_index_name_index(&database).unwrap();
    let read = database.begin_read().unwrap();
    assert!(validate_name_domains(&read).is_err());
}

#[test]
fn stale_policy_index_is_rebuilt_from_catalog_records() {
    let database = database();
    let mut stale_key = vec![0x32; 16];
    stale_key.extend_from_slice(&1u16.to_be_bytes());
    stale_key.extend_from_slice(&[0x64; 16]);
    let write = database.begin_write().unwrap();
    write
        .open_table(POLICIES)
        .unwrap()
        .insert(stale_key.as_slice(), &b"stale"[..])
        .unwrap();
    write.commit().unwrap();
    ensure_policy_index(&database).unwrap();
    let read = database.begin_read().unwrap();
    let policies = lookup_policies_for_table_operation(&read, [0x32; 16], 1).unwrap();
    assert_eq!(policies.len(), 1);
    assert_eq!(policies[0].logical_key[3..19], [0x62; 16]);
}

#[test]
fn missing_policy_index_is_rebuilt_from_catalog_records() {
    let database = database();
    let write = database.begin_write().unwrap();
    assert!(write.delete_table(POLICIES).unwrap());
    write.commit().unwrap();
    ensure_policy_index(&database).unwrap();
    let read = database.begin_read().unwrap();
    assert_eq!(
        lookup_policies_for_table_operation(&read, [0x32; 16], 1)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn policy_and_dependency_lookups_match_independent_catalog_scans() {
    let database = database();
    let write = database.begin_write().unwrap();
    let mut catalog = write.open_table(CATALOG).unwrap();
    for (policy_id, table_id, operation) in [
        ([0x60; 16], [0x32; 16], 2u16),
        ([0x61; 16], [0x71; 16], 1u16),
    ] {
        let mut key = 13u16.to_be_bytes().to_vec();
        key.push(1);
        key.extend_from_slice(&policy_id);
        let fields = [
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
                payload: operation.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::U16,
                payload: 1u16.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::Id,
                payload: [0x63; 16].to_vec(),
            },
            CatalogField {
                tag: 7,
                wire_type: WireType::U64,
                payload: 1u64.to_le_bytes().to_vec(),
            },
        ];
        let value = encode_envelope(&family_schema(13).unwrap(), &key, &fields).unwrap();
        catalog.insert(key.as_slice(), value.as_slice()).unwrap();
    }
    for (dependent, referenced, kind) in [
        ([0x65; 16], [0x67; 16], 3u16),
        ([0x68; 16], [0x66; 16], 2u16),
    ] {
        let mut key = 15u16.to_be_bytes().to_vec();
        key.push(1);
        key.extend_from_slice(&dependent);
        key.push(1);
        key.extend_from_slice(&referenced);
        key.push(1);
        key.extend_from_slice(&kind.to_be_bytes());
        let value = encode_envelope(&family_schema(15).unwrap(), &key, &[]).unwrap();
        catalog.insert(key.as_slice(), value.as_slice()).unwrap();
    }
    drop(catalog);
    write.commit().unwrap();
    ensure_policy_index(&database).unwrap();
    ensure_dependency_index(&database).unwrap();

    let read = database.begin_read().unwrap();
    let catalog = read.open_table(CATALOG).unwrap();
    let policy_prefix = [13u16.to_be_bytes().as_slice(), &[1]].concat();
    let expected_policies = catalog
        .iter()
        .unwrap()
        .filter_map(|entry| {
            let (key, value) = entry.unwrap();
            if !key.value().starts_with(&policy_prefix) {
                return None;
            }
            let record = decode_registered_envelope(value.value()).unwrap();
            let table_id = record
                .fields
                .iter()
                .find(|field| field.tag == 1)
                .unwrap()
                .payload
                .as_slice();
            let operation = u16::from_le_bytes(
                record
                    .fields
                    .iter()
                    .find(|field| field.tag == 3)
                    .unwrap()
                    .payload
                    .as_slice()
                    .try_into()
                    .unwrap(),
            );
            (table_id == [0x32; 16] && operation == 1).then(|| record.logical_key.clone())
        })
        .collect::<Vec<_>>();
    let actual_policies = lookup_policies_for_table_operation(&read, [0x32; 16], 1)
        .unwrap()
        .into_iter()
        .map(|record| record.logical_key)
        .collect::<Vec<_>>();
    assert_eq!(actual_policies, expected_policies);
    assert_eq!(actual_policies.len(), 1);
    assert!(
        lookup_policies_for_table_operation(&read, [0x32; 16], 3)
            .unwrap()
            .is_empty()
    );

    let dependency_prefix = [15u16.to_be_bytes().as_slice(), &[1]].concat();
    let (mut expected_outgoing, mut expected_incoming) = (Vec::new(), Vec::new());
    for entry in catalog.iter().unwrap() {
        let (key, _) = entry.unwrap();
        let key = key.value();
        if key.starts_with(&dependency_prefix) {
            if key[3..19] == [0x65; 16] {
                expected_outgoing.push(key.to_vec());
            }
            if key[20..36] == [0x66; 16] {
                expected_incoming.push(key.to_vec());
            }
        }
    }
    let actual_outgoing = lookup_dependencies_for_dependent(&read, [0x65; 16])
        .unwrap()
        .into_iter()
        .map(|record| record.logical_key)
        .collect::<Vec<_>>();
    let actual_incoming = lookup_dependents_for_referenced(&read, [0x66; 16])
        .unwrap()
        .into_iter()
        .map(|record| record.logical_key)
        .collect::<Vec<_>>();
    assert_eq!(actual_outgoing, expected_outgoing);
    assert_eq!(actual_incoming, expected_incoming);
    assert_eq!(actual_outgoing.len(), 2);
    assert_eq!(actual_incoming.len(), 2);
}
