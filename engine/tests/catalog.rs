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
    assert!(family_schema(18).is_none());
}
