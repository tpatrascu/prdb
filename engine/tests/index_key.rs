use prdb_engine::index_key::{
    DeriveIndexKeyError, compose_key, derive_index_key, encode_sql_tuple,
};
use prdb_engine::index_key::{
    decode_bool, decode_i16, decode_i32, decode_i64, decode_i128, decode_uuid, encode_bool,
    encode_i128, encode_uuid,
};
use prdb_engine::index_key::{decode_bytes, encode_bytes, prefix_successor};
use prdb_engine::index_key::{encode_i16, encode_i32, encode_i64};
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, TupleError, encode_tuple};

#[test]
fn fixed_components_decode_and_reject_noncanonical_inputs() {
    assert_eq!(decode_bool(&[1, 0, 7]), Ok((false, 2)));
    assert_eq!(decode_bool(&[1, 1]), Ok((true, 2)));
    assert_eq!(encode_bool(false), [1, 0]);
    assert_eq!(encode_bool(true), [1, 1]);
    for bad in [&[][..], &[1][..], &[0xff, 0][..], &[1, 2][..]] {
        assert!(decode_bool(bad).is_err());
    }
    for v in [i16::MIN, -1, 0, 1, i16::MAX] {
        assert_eq!(decode_i16(&encode_i16(v)), Ok((v, 3)));
    }
    for v in [i32::MIN, -1, 0, 1, i32::MAX] {
        assert_eq!(decode_i32(&encode_i32(v)), Ok((v, 5)));
    }
    for v in [i64::MIN, -1, 0, 1, i64::MAX] {
        assert_eq!(decode_i64(&encode_i64(v)), Ok((v, 9)));
    }
    for v in [i128::MIN, -120, 0, 120, i128::MAX] {
        assert_eq!(decode_i128(&encode_i128(v)), Ok((v, 17)));
    }
    assert_eq!(
        encode_i128(120),
        [1, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 120]
    );
    assert_eq!(
        encode_i128(-120),
        [
            1, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0x88
        ]
    );
    for len in 0..17 {
        assert!(decode_i128(&[1; 17][..len]).is_err());
    }
    assert!(decode_i16(&[0xff, 0, 0]).is_err());
    assert!(decode_i32(&[1, 0, 0, 0]).is_err());
    assert!(decode_i64(&[2; 9]).is_err());
    let id = [0xab; 16];
    assert_eq!(decode_uuid(&encode_uuid(id)), Ok((id, 17)));
    assert!(decode_uuid(&[1; 16]).is_err());
    assert!(decode_uuid(&[0; 17]).is_err());
}

#[test]
fn signed_components_match_the_v1_examples() {
    assert_eq!(encode_i16(i16::MIN), [0x01, 0x00, 0x00]);
    assert_eq!(encode_i16(-1), [0x01, 0x7f, 0xff]);
    assert_eq!(encode_i16(0), [0x01, 0x80, 0x00]);
    assert_eq!(encode_i16(i16::MAX), [0x01, 0xff, 0xff]);
    assert_eq!(encode_i32(-1), [0x01, 0x7f, 0xff, 0xff, 0xff]);
    assert_eq!(encode_i32(0), [0x01, 0x80, 0x00, 0x00, 0x00]);
    assert_eq!(encode_i32(1), [0x01, 0x80, 0x00, 0x00, 0x01]);
    assert_eq!(encode_i64(0), [0x01, 0x80, 0, 0, 0, 0, 0, 0, 0]);
}

#[test]
fn byte_components_have_canonical_escaping_and_boundaries() {
    assert_eq!(encode_bytes(b""), [1, 0, 0]);
    assert_eq!(encode_bytes(b"A\0B"), [1, 65, 0, 255, 66, 0, 0]);
    assert!(encode_bytes(b"A") < encode_bytes(b"A\0"));
    assert!(encode_bytes(b"A\0") < encode_bytes(b"A\x01"));
    assert_eq!(
        decode_bytes(&[1, 65, 0, 255, 66, 0, 0, 0xff]),
        Ok((b"A\0B".to_vec(), 7))
    );
}

#[test]
fn byte_components_reject_invalid_or_truncated_encodings() {
    for bytes in [&[][..], &[0xff][..], &[1][..], &[1, 0][..], &[1, 0, 1][..]] {
        assert!(decode_bytes(bytes).is_err(), "accepted {bytes:?}");
    }
}

#[test]
fn prefix_bounds_cover_exactly_the_matching_byte_prefix() {
    assert_eq!(prefix_successor(&[0x12, 0xff]), Some(vec![0x13]));
    assert_eq!(prefix_successor(&[0xff, 0xff]), None);
    assert_eq!(prefix_successor(&[]), None);
    let lower = [0x12, 0xff];
    let upper = prefix_successor(&lower).unwrap();
    for candidate in [
        &[0x12][..],
        &[0x12, 0xfe][..],
        &[0x12, 0xff][..],
        &[0x12, 0xff, 0][..],
        &[0x13][..],
    ] {
        assert_eq!(
            candidate >= &lower[..] && candidate < &upper[..],
            candidate.starts_with(&lower)
        );
    }
}

#[test]
fn signed_components_sort_in_numeric_order() {
    let values = [i64::MIN, -65536, -1, 0, 1, 65536, i64::MAX];
    for pair in values.windows(2) {
        assert!(encode_i64(pair[0]) < encode_i64(pair[1]));
    }
}

#[test]
fn typed_sql_tuple_key_uses_null_markers_and_declared_scalar_encodings() {
    let columns = [
        ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Boolean,
            nullable: true,
        },
    ];
    let values = [
        Some(ScalarValue::Integer(1)),
        Some(ScalarValue::Text("A\0".into())),
        None,
    ];
    let tuple = encode_sql_tuple(&columns, &values).unwrap();
    assert_eq!(
        tuple,
        [encode_i32(1).as_slice(), &[1, b'A', 0, 0xff, 0, 0], &[0xff]].concat()
    );
    let key = compose_key([0x22; 16], &tuple, [0x33; 16]);
    assert_eq!(&key[..16], &[0x22; 16]);
    assert_eq!(&key[16..key.len() - 16], &tuple);
    assert_eq!(&key[key.len() - 16..], &[0x33; 16]);
}

#[test]
fn typed_sql_tuple_validation_rejects_nullability_and_type_errors() {
    let not_null = [ColumnSchema {
        scalar_type: ScalarType::Integer,
        nullable: false,
    }];
    assert!(encode_sql_tuple(&not_null, &[None]).is_err());
    assert!(encode_sql_tuple(&not_null, &[Some(ScalarValue::Text("bad".into()))]).is_err());
}

#[test]
fn catalog_index_key_projects_typed_row_in_definition_order() {
    let schema_columns = [
        ColumnSchema {
            scalar_type: ScalarType::Text,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Boolean,
            nullable: true,
        },
    ];
    let schema_ids = [101, 205, 309];
    let values = [
        Some(ScalarValue::Text("a\0b".into())),
        Some(ScalarValue::Integer(-7)),
        Some(ScalarValue::Boolean(true)),
    ];
    let payload = encode_tuple(&schema_columns, 7, &values).unwrap();
    let derived = derive_index_key(
        [0x22; 16],
        [0x33; 16],
        7,
        &schema_ids,
        &schema_columns,
        &payload,
        &[309, 205],
    )
    .unwrap();

    let expected_tuple = [encode_bool(true).as_slice(), &encode_i32(-7)].concat();
    assert_eq!(
        derived.key,
        compose_key([0x22; 16], &expected_tuple, [0x33; 16])
    );
    assert!(!derived.has_null);
}

#[test]
fn catalog_index_key_reports_nulls_and_rejects_invalid_definitions_or_rows() {
    let schema_columns = [
        ColumnSchema {
            scalar_type: ScalarType::Integer,
            nullable: false,
        },
        ColumnSchema {
            scalar_type: ScalarType::Boolean,
            nullable: true,
        },
    ];
    let schema_ids = [101, 309];
    let payload = encode_tuple(&schema_columns, 4, &[Some(ScalarValue::Integer(8)), None]).unwrap();
    let args = (
        [0x22; 16],
        [0x33; 16],
        4,
        &schema_ids[..],
        &schema_columns[..],
        &payload[..],
    );
    let derived = derive_index_key(args.0, args.1, args.2, args.3, args.4, args.5, &[309]).unwrap();
    assert!(derived.has_null);
    assert_eq!(derived.key, compose_key([0x22; 16], &[0xff], [0x33; 16]));

    assert_eq!(
        derive_index_key(args.0, args.1, args.2, &[101, 101], args.4, args.5, &[101]),
        Err(DeriveIndexKeyError::InvalidDefinition)
    );
    assert_eq!(
        derive_index_key(args.0, args.1, args.2, args.3, args.4, args.5, &[101, 101]),
        Err(DeriveIndexKeyError::InvalidDefinition)
    );
    assert_eq!(
        derive_index_key(args.0, args.1, args.2, args.3, args.4, args.5, &[999]),
        Err(DeriveIndexKeyError::InvalidDefinition)
    );
    assert_eq!(
        derive_index_key(args.0, args.1, args.2, args.3, args.4, args.5, &[0]),
        Err(DeriveIndexKeyError::InvalidDefinition)
    );
    assert_eq!(
        derive_index_key(
            args.0,
            args.1,
            args.2,
            args.3,
            args.4,
            &payload[..payload.len() - 1],
            &[101]
        ),
        Err(DeriveIndexKeyError::Tuple(TupleError::InvalidLength))
    );
    assert_eq!(
        derive_index_key(args.0, args.1, 5, args.3, args.4, args.5, &[101]),
        Err(DeriveIndexKeyError::Tuple(
            TupleError::SchemaVersionMismatch
        ))
    );
}
