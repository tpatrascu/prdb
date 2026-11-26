use prdb_engine::catalog::{
    CatalogField, FamilySchema, FieldDefinition, PolicyTarget, WireType, decode_envelope,
    decode_policy_target, encode_envelope, encode_policy_target, family_schema,
};

fn example() -> PolicyTarget {
    PolicyTarget {
        policy_id: [1; 16],
        kind: 1,
        target_id: [2; 16],
    }
}

#[test]
fn policy_target_matches_the_catalog_v1_fixture() {
    let mut expected = vec![
        0x43, 0x41, 0x54, 0x31, 1, 0, 14, 0, 71, 0, 0, 0, 39, 0, 0, 0,
    ];
    expected.extend_from_slice(&[0; 16]);
    expected.extend_from_slice(&[0, 14, 1]);
    expected.extend_from_slice(&[1; 16]);
    expected.extend_from_slice(&[1, 0, 1, 1]);
    expected.extend_from_slice(&[2; 16]);
    assert_eq!(encode_policy_target(&example()).unwrap(), expected);
    assert_eq!(decode_policy_target(&expected).unwrap(), example());
    let schema = family_schema(14).unwrap();
    assert_eq!(
        decode_envelope(&schema, &expected)
            .unwrap()
            .logical_key
            .len(),
        39
    );
}

#[test]
fn database_record_matches_independent_catalog_v1_fixture() {
    let key = [vec![0, 1, 1], vec![0x11; 16]].concat();
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Text,
            payload: b"db".to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::U32,
            payload: 2u32.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::Bytes,
            payload: Vec::new(),
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
    let mut expected = vec![
        b'C', b'A', b'T', b'1', 1, 0, 1, 0, 105, 0, 0, 0, 19, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ];
    expected.extend_from_slice(&key);
    expected.extend_from_slice(&[1, 0, 7, 0, 2, 0, 0, 0, b'd', b'b']);
    expected.extend_from_slice(&[2, 0, 2, 0, 4, 0, 0, 0, 2, 0, 0, 0]);
    expected.extend_from_slice(&[3, 0, 8, 0, 0, 0, 0, 0]);
    expected.extend_from_slice(&[5, 0, 2, 0, 4, 0, 0, 0, 1, 0, 0, 0]);
    expected.extend_from_slice(&[6, 0, 2, 0, 4, 0, 0, 0, 1, 0, 0, 0]);

    let schema = family_schema(1).unwrap();
    assert_eq!(encode_envelope(&schema, &key, &fields).unwrap(), expected);
    let decoded = prdb_engine::catalog::decode_registered_envelope(&expected).unwrap();
    assert_eq!(decoded.family_code, 1);
    assert_eq!(decoded.logical_key, key);
    assert_eq!(decoded.fields, fields);
}

#[test]
fn object_record_matches_independent_catalog_v1_fixture() {
    let key = [vec![0, 2, 1], vec![0x11; 16]].concat();
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::Text,
            payload: b"public".to_vec(),
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::Id,
            payload: vec![0x22; 16],
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
    let mut expected = vec![
        b'C', b'A', b'T', b'1', 1, 0, 2, 0, 125, 0, 0, 0, 19, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ];
    expected.extend_from_slice(&key);
    expected.extend_from_slice(&[1, 0, 1, 0, 2, 0, 0, 0, 1, 0]);
    expected.extend_from_slice(&[3, 0, 7, 0, 6, 0, 0, 0, b'p', b'u', b'b', b'l', b'i', b'c']);
    expected.extend_from_slice(&[4, 0, 6, 0, 16, 0, 0, 0]);
    expected.extend_from_slice(&[0x22; 16]);
    expected.extend_from_slice(&[5, 0, 3, 0, 8, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]);
    expected.extend_from_slice(&[6, 0, 1, 0, 2, 0, 0, 0, 1, 0]);

    let schema = family_schema(2).unwrap();
    assert_eq!(encode_envelope(&schema, &key, &fields).unwrap(), expected);
    let decoded = prdb_engine::catalog::decode_registered_envelope(&expected).unwrap();
    assert_eq!(decoded.family_code, 2);
    assert_eq!(decoded.logical_key, key);
    assert_eq!(decoded.fields, fields);
}

#[test]
fn schema_record_matches_independent_catalog_v1_fixture() {
    let key = [vec![0, 3, 1], vec![0x33; 16]].concat();
    let mut search_path = 1u32.to_le_bytes().to_vec();
    search_path.extend_from_slice(&[0x44; 16]);
    let fields = [CatalogField {
        tag: 1,
        wire_type: WireType::Ids,
        payload: search_path,
    }];
    let mut expected = vec![
        b'C', b'A', b'T', b'1', 1, 0, 3, 0, 79, 0, 0, 0, 19, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0,
    ];
    expected.extend_from_slice(&key);
    expected.extend_from_slice(&[1, 0, 9, 0, 20, 0, 0, 0, 1, 0, 0, 0]);
    expected.extend_from_slice(&[0x44; 16]);

    let schema = family_schema(3).unwrap();
    assert_eq!(encode_envelope(&schema, &key, &fields).unwrap(), expected);
    let decoded = prdb_engine::catalog::decode_registered_envelope(&expected).unwrap();
    assert_eq!(decoded.family_code, 3);
    assert_eq!(decoded.logical_key, key);
    assert_eq!(decoded.fields, fields);
}

#[test]
fn type_record_matches_independent_catalog_v1_fixture() {
    let key = [vec![0, 4, 1], vec![0x44; 16]].concat();
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::U16,
            payload: 0u16.to_le_bytes().to_vec(),
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
            payload: 1u32.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 6,
            wire_type: WireType::U32,
            payload: 1u32.to_le_bytes().to_vec(),
        },
    ];
    let mut expected = vec![
        b'C', b'A', b'T', b'1', 1, 0, 4, 0, 119, 0, 0, 0, 19, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ];
    expected.extend_from_slice(&key);
    expected.extend_from_slice(&[1, 0, 1, 0, 2, 0, 0, 0, 1, 0]);
    expected.extend_from_slice(&[2, 0, 1, 0, 2, 0, 0, 0, 0, 0]);
    for tag in 3u8..=6 {
        expected.extend_from_slice(&[tag, 0, 2, 0, 4, 0, 0, 0, 1, 0, 0, 0]);
    }

    let schema = family_schema(4).unwrap();
    assert_eq!(encode_envelope(&schema, &key, &fields).unwrap(), expected);
    let decoded = prdb_engine::catalog::decode_registered_envelope(&expected).unwrap();
    assert_eq!(decoded.family_code, 4);
    assert_eq!(decoded.logical_key, key);
    assert_eq!(decoded.fields, fields);
}

#[test]
fn table_record_matches_independent_catalog_v1_fixture() {
    let key = [vec![0, 5, 1], vec![0x55; 16]].concat();
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: vec![0x56; 16],
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::U64,
            payload: 1u64.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::U64,
            payload: 2u64.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::Id,
            payload: vec![0x57; 16],
        },
        CatalogField {
            tag: 5,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
    ];
    let mut expected = vec![
        b'C', b'A', b'T', b'1', 1, 0, 5, 0, 141, 0, 0, 0, 19, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ];
    expected.extend_from_slice(&key);
    expected.extend_from_slice(&[1, 0, 6, 0, 16, 0, 0, 0]);
    expected.extend_from_slice(&[0x56; 16]);
    expected.extend_from_slice(&[2, 0, 3, 0, 8, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]);
    expected.extend_from_slice(&[3, 0, 3, 0, 8, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0]);
    expected.extend_from_slice(&[4, 0, 6, 0, 16, 0, 0, 0]);
    expected.extend_from_slice(&[0x57; 16]);
    expected.extend_from_slice(&[5, 0, 1, 0, 2, 0, 0, 0, 1, 0]);

    let schema = family_schema(5).unwrap();
    assert_eq!(encode_envelope(&schema, &key, &fields).unwrap(), expected);
    let decoded = prdb_engine::catalog::decode_registered_envelope(&expected).unwrap();
    assert_eq!(decoded.family_code, 5);
    assert_eq!(decoded.logical_key, key);
    assert_eq!(decoded.fields, fields);
}

#[test]
fn column_record_matches_independent_catalog_v1_fixture() {
    let key = [vec![0, 6, 1], vec![0x66; 16]].concat();
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: vec![0x55; 16],
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::U32,
            payload: 1u32.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::Text,
            payload: b"id".to_vec(),
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::Id,
            payload: vec![0x44; 16],
        },
        CatalogField {
            tag: 5,
            wire_type: WireType::Bytes,
            payload: vec![0; 8],
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
    ];
    let mut expected = vec![
        b'C', b'A', b'T', b'1', 1, 0, 6, 0, 172, 0, 0, 0, 19, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ];
    expected.extend_from_slice(&key);
    expected.extend_from_slice(&[1, 0, 6, 0, 16, 0, 0, 0]);
    expected.extend_from_slice(&[0x55; 16]);
    expected.extend_from_slice(&[2, 0, 2, 0, 4, 0, 0, 0, 1, 0, 0, 0]);
    expected.extend_from_slice(&[3, 0, 7, 0, 2, 0, 0, 0, b'i', b'd']);
    expected.extend_from_slice(&[4, 0, 6, 0, 16, 0, 0, 0]);
    expected.extend_from_slice(&[0x44; 16]);
    expected.extend_from_slice(&[5, 0, 8, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    expected.extend_from_slice(&[6, 0, 5, 0, 1, 0, 0, 0, 0]);
    expected.extend_from_slice(&[8, 0, 1, 0, 2, 0, 0, 0, 0, 0]);
    expected.extend_from_slice(&[9, 0, 3, 0, 8, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]);

    let schema = family_schema(6).unwrap();
    assert_eq!(encode_envelope(&schema, &key, &fields).unwrap(), expected);
    let decoded = prdb_engine::catalog::decode_registered_envelope(&expected).unwrap();
    assert_eq!(decoded.family_code, 6);
    assert_eq!(decoded.logical_key, key);
    assert_eq!(decoded.fields, fields);
}

#[test]
fn expression_record_matches_independent_catalog_v1_fixture() {
    let key = [vec![0, 7, 1], vec![0x77; 16]].concat();
    let mut dependencies = 1u32.to_le_bytes().to_vec();
    dependencies.extend_from_slice(&[0x88; 16]);
    let fields = [
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
            payload: vec![0x33; 16],
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::Bytes,
            payload: vec![0; 8],
        },
        CatalogField {
            tag: 5,
            wire_type: WireType::Bytes,
            payload: b"EIR1".to_vec(),
        },
        CatalogField {
            tag: 6,
            wire_type: WireType::Ids,
            payload: dependencies,
        },
    ];
    let mut expected = vec![
        b'C', b'A', b'T', b'1', 1, 0, 7, 0, 153, 0, 0, 0, 19, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ];
    expected.extend_from_slice(&key);
    expected.extend_from_slice(&[1, 0, 1, 0, 2, 0, 0, 0, 1, 0]);
    expected.extend_from_slice(&[2, 0, 2, 0, 4, 0, 0, 0, 1, 0, 0, 0]);
    expected.extend_from_slice(&[3, 0, 6, 0, 16, 0, 0, 0]);
    expected.extend_from_slice(&[0x33; 16]);
    expected.extend_from_slice(&[4, 0, 8, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    expected.extend_from_slice(&[5, 0, 8, 0, 4, 0, 0, 0, b'E', b'I', b'R', b'1']);
    expected.extend_from_slice(&[6, 0, 9, 0, 20, 0, 0, 0, 1, 0, 0, 0]);
    expected.extend_from_slice(&[0x88; 16]);

    let schema = family_schema(7).unwrap();
    assert_eq!(encode_envelope(&schema, &key, &fields).unwrap(), expected);
    let decoded = prdb_engine::catalog::decode_registered_envelope(&expected).unwrap();
    assert_eq!(decoded.family_code, 7);
    assert_eq!(decoded.logical_key, key);
    assert_eq!(decoded.fields, fields);
}

#[test]
fn constraint_record_matches_independent_catalog_v1_fixture() {
    let key = [vec![0, 8, 1], vec![0x88; 16]].concat();
    let mut source_columns = 1u32.to_le_bytes().to_vec();
    source_columns.extend_from_slice(&[0x66; 16]);
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: vec![0x55; 16],
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::U16,
            payload: 1u16.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::Ids,
            payload: source_columns,
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
    ];
    let mut expected = vec![
        b'C', b'A', b'T', b'1', 1, 0, 8, 0, 165, 0, 0, 0, 19, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ];
    expected.extend_from_slice(&key);
    expected.extend_from_slice(&[1, 0, 6, 0, 16, 0, 0, 0]);
    expected.extend_from_slice(&[0x55; 16]);
    expected.extend_from_slice(&[2, 0, 1, 0, 2, 0, 0, 0, 1, 0]);
    expected.extend_from_slice(&[3, 0, 9, 0, 20, 0, 0, 0, 1, 0, 0, 0]);
    expected.extend_from_slice(&[0x66; 16]);
    expected.extend_from_slice(&[6, 0, 9, 0, 4, 0, 0, 0, 0, 0, 0, 0]);
    for tag in 7u8..=10 {
        let value = u8::from(tag == 10);
        expected.extend_from_slice(&[tag, 0, 1, 0, 2, 0, 0, 0, value, 0]);
    }

    let schema = family_schema(8).unwrap();
    assert_eq!(encode_envelope(&schema, &key, &fields).unwrap(), expected);
    let decoded = prdb_engine::catalog::decode_registered_envelope(&expected).unwrap();
    assert_eq!(decoded.family_code, 8);
    assert_eq!(decoded.logical_key, key);
    assert_eq!(decoded.fields, fields);
}

#[test]
fn index_record_matches_independent_catalog_v1_fixture() {
    let key = [vec![0, 9, 1], vec![0x99; 16]].concat();
    let mut columns = 1u32.to_le_bytes().to_vec();
    columns.extend_from_slice(&[0x66; 16]);
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: vec![0x55; 16],
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
            tag: 7,
            wire_type: WireType::Id,
            payload: vec![0x77; 16],
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
    let mut expected = vec![
        b'C', b'A', b'T', b'1', 1, 0, 9, 0, 194, 0, 0, 0, 19, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ];
    expected.extend_from_slice(&key);
    expected.extend_from_slice(&[1, 0, 6, 0, 16, 0, 0, 0]);
    expected.extend_from_slice(&[0x55; 16]);
    expected.extend_from_slice(&[2, 0, 9, 0, 20, 0, 0, 0, 1, 0, 0, 0]);
    expected.extend_from_slice(&[0x66; 16]);
    expected.extend_from_slice(&[3, 0, 1, 0, 2, 0, 0, 0, 1, 0]);
    expected.extend_from_slice(&[4, 0, 5, 0, 1, 0, 0, 0, 1]);
    expected.extend_from_slice(&[5, 0, 1, 0, 2, 0, 0, 0, 1, 0]);
    expected.extend_from_slice(&[7, 0, 6, 0, 16, 0, 0, 0]);
    expected.extend_from_slice(&[0x77; 16]);
    expected.extend_from_slice(&[8, 0, 1, 0, 2, 0, 0, 0, 1, 0]);
    expected.extend_from_slice(&[9, 0, 2, 0, 4, 0, 0, 0, 1, 0, 0, 0]);
    expected.extend_from_slice(&[10, 0, 3, 0, 8, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]);

    let schema = family_schema(9).unwrap();
    assert_eq!(encode_envelope(&schema, &key, &fields).unwrap(), expected);
    let decoded = prdb_engine::catalog::decode_registered_envelope(&expected).unwrap();
    assert_eq!(decoded.family_code, 9);
    assert_eq!(decoded.logical_key, key);
    assert_eq!(decoded.fields, fields);
}

#[test]
fn principal_record_matches_independent_catalog_v1_fixture() {
    let key = [vec![0, 10, 1], vec![0xaa; 16]].concat();
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::U16,
            payload: 2u16.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::Text,
            payload: b"admin".to_vec(),
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::Bool,
            payload: vec![0],
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::U64,
            payload: 1u64.to_le_bytes().to_vec(),
        },
    ];
    let mut expected = vec![
        b'C', b'A', b'T', b'1', 1, 0, 10, 0, 99, 0, 0, 0, 19, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ];
    expected.extend_from_slice(&key);
    expected.extend_from_slice(&[1, 0, 1, 0, 2, 0, 0, 0, 2, 0]);
    expected.extend_from_slice(&[2, 0, 7, 0, 5, 0, 0, 0, b'a', b'd', b'm', b'i', b'n']);
    expected.extend_from_slice(&[3, 0, 5, 0, 1, 0, 0, 0, 0]);
    expected.extend_from_slice(&[4, 0, 3, 0, 8, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]);

    let schema = family_schema(10).unwrap();
    assert_eq!(encode_envelope(&schema, &key, &fields).unwrap(), expected);
    let decoded = prdb_engine::catalog::decode_registered_envelope(&expected).unwrap();
    assert_eq!(decoded.family_code, 10);
    assert_eq!(decoded.logical_key, key);
    assert_eq!(decoded.fields, fields);
}

#[test]
fn membership_record_matches_independent_catalog_v1_fixture() {
    let key = [vec![0, 11, 1], vec![0x11; 16], vec![1], vec![0x22; 16]].concat();
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: vec![0x44; 16],
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::U64,
            payload: 1u64.to_le_bytes().to_vec(),
        },
    ];
    let mut expected = vec![
        b'C', b'A', b'T', b'1', 1, 0, 11, 0, 108, 0, 0, 0, 36, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ];
    expected.extend_from_slice(&key);
    expected.extend_from_slice(&[1, 0, 6, 0, 16, 0, 0, 0]);
    expected.extend_from_slice(&[0x44; 16]);
    expected.extend_from_slice(&[2, 0, 3, 0, 8, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]);

    let schema = family_schema(11).unwrap();
    assert_eq!(encode_envelope(&schema, &key, &fields).unwrap(), expected);
    let decoded = prdb_engine::catalog::decode_registered_envelope(&expected).unwrap();
    assert_eq!(decoded.family_code, 11);
    assert_eq!(decoded.logical_key, key);
    assert_eq!(decoded.fields, fields);
}

#[test]
fn grant_record_matches_independent_catalog_v1_fixture() {
    let key = [
        vec![0, 12, 1],
        vec![0x11; 16],
        vec![1],
        vec![0x22; 16],
        vec![1, 0, 1],
        vec![1, 0, 1],
        vec![1],
        vec![0x22; 16],
    ]
    .concat();
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: vec![0x33; 16],
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::U64,
            payload: 1u64.to_le_bytes().to_vec(),
        },
    ];
    let mut expected = vec![
        b'C', b'A', b'T', b'1', 1, 0, 12, 0, 131, 0, 0, 0, 59, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ];
    expected.extend_from_slice(&key);
    expected.extend_from_slice(&[1, 0, 6, 0, 16, 0, 0, 0]);
    expected.extend_from_slice(&[0x33; 16]);
    expected.extend_from_slice(&[2, 0, 3, 0, 8, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]);

    let schema = family_schema(12).unwrap();
    assert_eq!(encode_envelope(&schema, &key, &fields).unwrap(), expected);
    let decoded = prdb_engine::catalog::decode_registered_envelope(&expected).unwrap();
    assert_eq!(decoded.family_code, 12);
    assert_eq!(decoded.logical_key, key);
    assert_eq!(decoded.fields, fields);
}

#[test]
fn policy_record_matches_independent_catalog_v1_fixture() {
    let key = [vec![0, 13, 1], vec![0x13; 16]].concat();
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: vec![0x55; 16],
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
            payload: vec![0x77; 16],
        },
        CatalogField {
            tag: 7,
            wire_type: WireType::U64,
            payload: 1u64.to_le_bytes().to_vec(),
        },
    ];
    let mut expected = vec![
        b'C', b'A', b'T', b'1', 1, 0, 13, 0, 145, 0, 0, 0, 19, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ];
    expected.extend_from_slice(&key);
    expected.extend_from_slice(&[1, 0, 6, 0, 16, 0, 0, 0]);
    expected.extend_from_slice(&[0x55; 16]);
    for tag in 2u8..=4 {
        expected.extend_from_slice(&[tag, 0, 1, 0, 2, 0, 0, 0, 1, 0]);
    }
    expected.extend_from_slice(&[5, 0, 6, 0, 16, 0, 0, 0]);
    expected.extend_from_slice(&[0x77; 16]);
    expected.extend_from_slice(&[7, 0, 3, 0, 8, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]);

    let schema = family_schema(13).unwrap();
    assert_eq!(encode_envelope(&schema, &key, &fields).unwrap(), expected);
    let decoded = prdb_engine::catalog::decode_registered_envelope(&expected).unwrap();
    assert_eq!(decoded.family_code, 13);
    assert_eq!(decoded.logical_key, key);
    assert_eq!(decoded.fields, fields);
}

#[test]
fn dependency_record_matches_independent_catalog_v1_fixture() {
    let key = [
        vec![0, 15, 1],
        vec![0x11; 16],
        vec![1],
        vec![0x22; 16],
        vec![1, 0, 1],
    ]
    .concat();
    let fields: [CatalogField; 0] = [];
    let mut expected = vec![
        b'C', b'A', b'T', b'1', 1, 0, 15, 0, 71, 0, 0, 0, 39, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ];
    expected.extend_from_slice(&key);

    let schema = family_schema(15).unwrap();
    assert_eq!(encode_envelope(&schema, &key, &fields).unwrap(), expected);
    let decoded = prdb_engine::catalog::decode_registered_envelope(&expected).unwrap();
    assert_eq!(decoded.family_code, 15);
    assert_eq!(decoded.logical_key, key);
    assert!(decoded.fields.is_empty());
}

#[test]
fn verifier_record_matches_independent_catalog_v1_fixture() {
    let key = [vec![0, 16, 1], vec![0x16; 16]].concat();
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: vec![0x10; 16],
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
            payload: vec![0xaa; 32],
        },
        CatalogField {
            tag: 5,
            wire_type: WireType::I64,
            payload: 1i64.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 6,
            wire_type: WireType::I64,
            payload: 2i64.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 7,
            wire_type: WireType::Bool,
            payload: vec![0],
        },
        CatalogField {
            tag: 9,
            wire_type: WireType::Text,
            payload: b"worker".to_vec(),
        },
        CatalogField {
            tag: 10,
            wire_type: WireType::U64,
            payload: 1u64.to_le_bytes().to_vec(),
        },
    ];
    let mut expected = vec![
        b'C', b'A', b'T', b'1', 1, 0, 16, 0, 206, 0, 0, 0, 19, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ];
    expected.extend_from_slice(&key);
    expected.extend_from_slice(&[1, 0, 6, 0, 16, 0, 0, 0]);
    expected.extend_from_slice(&[0x10; 16]);
    expected.extend_from_slice(&[2, 0, 1, 0, 2, 0, 0, 0, 1, 0]);
    expected.extend_from_slice(&[3, 0, 1, 0, 2, 0, 0, 0, 1, 0]);
    expected.extend_from_slice(&[4, 0, 8, 0, 32, 0, 0, 0]);
    expected.extend_from_slice(&[0xaa; 32]);
    expected.extend_from_slice(&[5, 0, 4, 0, 8, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]);
    expected.extend_from_slice(&[6, 0, 4, 0, 8, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0]);
    expected.extend_from_slice(&[7, 0, 5, 0, 1, 0, 0, 0, 0]);
    expected.extend_from_slice(&[9, 0, 7, 0, 6, 0, 0, 0, b'w', b'o', b'r', b'k', b'e', b'r']);
    expected.extend_from_slice(&[10, 0, 3, 0, 8, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]);

    let schema = family_schema(16).unwrap();
    assert_eq!(encode_envelope(&schema, &key, &fields).unwrap(), expected);
    let decoded = prdb_engine::catalog::decode_registered_envelope(&expected).unwrap();
    assert_eq!(decoded.family_code, 16);
    assert_eq!(decoded.logical_key, key);
    assert_eq!(decoded.fields, fields);
}

#[test]
fn tombstone_record_matches_independent_catalog_v1_fixture() {
    let key = [vec![0, 17, 1], vec![0x17; 16]].concat();
    let fields: [CatalogField; 0] = [];
    let mut expected = vec![
        b'C', b'A', b'T', b'1', 1, 0, 17, 0, 51, 0, 0, 0, 19, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ];
    expected.extend_from_slice(&key);

    let schema = family_schema(17).unwrap();
    assert_eq!(encode_envelope(&schema, &key, &fields).unwrap(), expected);
    let decoded = prdb_engine::catalog::decode_registered_envelope(&expected).unwrap();
    assert_eq!(decoded.family_code, 17);
    assert_eq!(decoded.logical_key, key);
    assert!(decoded.fields.is_empty());
}

#[test]
fn policy_target_rejects_noncanonical_data() {
    let bytes = encode_policy_target(&example()).unwrap();
    for (offset, value) in [
        (0, 0),
        (4, 2),
        (6, 15),
        (8, 70),
        (12, 38),
        (16, 1),
        (20, 1),
        (24, 1),
        (32, 1),
        (34, 0),
        (51, 0),
        (53, 4),
        (54, 0),
    ] {
        let mut invalid = bytes.clone();
        invalid[offset] = value;
        assert!(
            decode_policy_target(&invalid).is_err(),
            "accepted changed offset {offset}"
        );
    }
    assert!(decode_policy_target(&bytes[..70]).is_err());
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(decode_policy_target(&trailing).is_err());
    let mut zero_id = bytes.clone();
    zero_id[35..51].fill(0);
    assert!(decode_policy_target(&zero_id).is_err());
    assert!(
        encode_policy_target(&PolicyTarget {
            kind: 4,
            ..example()
        })
        .is_err()
    );
    assert!(
        encode_policy_target(&PolicyTarget {
            target_id: [0; 16],
            ..example()
        })
        .is_err()
    );
}

#[test]
fn generic_catalog_envelope_round_trips_registered_fields() {
    let definitions = [
        FieldDefinition {
            tag: 1,
            wire_type: WireType::Id,
            required: true,
            ids_are_set: false,
        },
        FieldDefinition {
            tag: 2,
            wire_type: WireType::Text,
            required: false,
            ids_are_set: false,
        },
    ];
    let schema = FamilySchema {
        family_code: 10,
        fields: &definitions,
    };
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: vec![1; 16],
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::Text,
            payload: b"alice".to_vec(),
        },
    ];
    let mut key = vec![0, 10, 1];
    key.extend_from_slice(&[7; 16]);
    let encoded = encode_envelope(&schema, &key, &fields).unwrap();
    let decoded = decode_envelope(&schema, &encoded).unwrap();
    assert_eq!(decoded.family_code, 10);
    assert_eq!(decoded.logical_key, key);
    assert_eq!(decoded.fields, fields);
}

#[test]
fn generic_catalog_envelope_rejects_unknown_fields_and_noncanonical_lengths() {
    let definitions = [FieldDefinition {
        tag: 1,
        wire_type: WireType::U16,
        required: true,
        ids_are_set: false,
    }];
    let schema = FamilySchema {
        family_code: 10,
        fields: &definitions,
    };
    let unknown = [CatalogField {
        tag: 2,
        wire_type: WireType::U16,
        payload: vec![0, 1],
    }];
    let mut key = vec![0, 10, 1];
    key.extend_from_slice(&[8; 16]);
    assert!(encode_envelope(&schema, &key, &unknown).is_err());
    let fields = [CatalogField {
        tag: 1,
        wire_type: WireType::U16,
        payload: vec![0, 1],
    }];
    let mut encoded = encode_envelope(&schema, &key, &fields).unwrap();
    encoded[58] = 3;
    assert!(decode_envelope(&schema, &encoded).is_err());
    assert!(decode_envelope(&schema, &encoded[..encoded.len() - 1]).is_err());
}

#[test]
fn generic_catalog_wire_types_enforce_widths_utf8_and_id_sets() {
    let definitions: Vec<_> = (1..=9)
        .map(|tag| FieldDefinition {
            tag,
            wire_type: WireType::try_from(tag as u8).unwrap(),
            required: true,
            ids_are_set: tag == 9,
        })
        .collect();
    let schema = FamilySchema {
        family_code: 11,
        fields: &definitions,
    };
    let mut ids = 2u32.to_le_bytes().to_vec();
    ids.extend_from_slice(&[1; 16]);
    ids.extend_from_slice(&[2; 16]);
    let fields = vec![
        CatalogField {
            tag: 1,
            wire_type: WireType::U16,
            payload: vec![0, 1],
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::U32,
            payload: 1u32.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::U64,
            payload: 1u64.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::I64,
            payload: (-1i64).to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 5,
            wire_type: WireType::Bool,
            payload: vec![1],
        },
        CatalogField {
            tag: 6,
            wire_type: WireType::Id,
            payload: vec![3; 16],
        },
        CatalogField {
            tag: 7,
            wire_type: WireType::Text,
            payload: "é".as_bytes().to_vec(),
        },
        CatalogField {
            tag: 8,
            wire_type: WireType::Bytes,
            payload: vec![0, 0xff],
        },
        CatalogField {
            tag: 9,
            wire_type: WireType::Ids,
            payload: ids.clone(),
        },
    ];
    let mut key = vec![0, 11, 1];
    key.extend_from_slice(&[1; 16]);
    key.push(1);
    key.extend_from_slice(&[2; 16]);
    let encoded = encode_envelope(&schema, &key, &fields).unwrap();
    assert_eq!(decode_envelope(&schema, &encoded).unwrap().fields, fields);

    let mut invalid = fields.clone();
    invalid[4].payload = vec![2];
    assert!(encode_envelope(&schema, &key, &invalid).is_err());
    let mut invalid = fields.clone();
    invalid[6].payload = vec![0xff];
    assert!(encode_envelope(&schema, &key, &invalid).is_err());
    let mut invalid = fields;
    let mut duplicate_ids = 2u32.to_le_bytes().to_vec();
    duplicate_ids.extend_from_slice(&[1; 16]);
    duplicate_ids.extend_from_slice(&[1; 16]);
    invalid[8].payload = duplicate_ids;
    assert!(encode_envelope(&schema, &key, &invalid).is_err());
}

#[test]
fn every_catalog_family_has_a_complete_required_field_registry() {
    for family_code in 1..=17 {
        let schema = family_schema(family_code).unwrap();
        let mut key = family_code.to_be_bytes().to_vec();
        match family_code {
            11 => {
                key.push(1);
                key.extend_from_slice(&[1; 16]);
                key.push(1);
                key.extend_from_slice(&[2; 16]);
            }
            12 => {
                key.push(1);
                key.extend_from_slice(&[1; 16]);
                key.push(1);
                key.extend_from_slice(&[2; 16]);
                key.extend_from_slice(&[1, 0, 1, 1, 0, 1]);
                key.push(1);
                key.extend_from_slice(&[2; 16]);
            }
            14 => {
                key.push(1);
                key.extend_from_slice(&[1; 16]);
                key.extend_from_slice(&[1, 0, 1]);
                key.push(1);
                key.extend_from_slice(&[2; 16]);
            }
            15 => {
                key.push(1);
                key.extend_from_slice(&[1; 16]);
                key.push(1);
                key.extend_from_slice(&[2; 16]);
                key.extend_from_slice(&[1, 0, 1]);
            }
            _ => {
                key.push(1);
                key.extend_from_slice(&[1; 16]);
            }
        }
        let fields: Vec<_> = schema
            .fields
            .iter()
            .filter(|definition| definition.required)
            .map(|definition| {
                let payload = match definition.wire_type {
                    WireType::U16 => vec![0, 1],
                    WireType::U32 => 1u32.to_le_bytes().to_vec(),
                    WireType::U64 => 1u64.to_le_bytes().to_vec(),
                    WireType::I64 => 1i64.to_le_bytes().to_vec(),
                    WireType::Bool => vec![0],
                    WireType::Id => vec![1; 16],
                    WireType::Text | WireType::Bytes => Vec::new(),
                    WireType::Ids => 0u32.to_le_bytes().to_vec(),
                };
                CatalogField {
                    tag: definition.tag,
                    wire_type: definition.wire_type,
                    payload,
                }
            })
            .collect();
        let encoded = encode_envelope(&schema, &key, &fields).unwrap();
        assert_eq!(decode_envelope(&schema, &encoded).unwrap().fields, fields);
    }
    assert!(family_schema(0).is_none());
    assert!(family_schema(19).is_none());
}
