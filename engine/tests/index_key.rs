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

fn assert_typed_values_sort_like_encoded_keys<T: Ord + Clone + std::fmt::Debug>(
    scalar_type: ScalarType,
    values: &[T],
    wrap: impl Fn(&T) -> ScalarValue,
) {
    let column = ColumnSchema {
        scalar_type,
        nullable: false,
    };
    let mut ordered = values.to_vec();
    ordered.sort();
    let encoded = ordered
        .iter()
        .map(|value| encode_sql_tuple(std::slice::from_ref(&column), &[Some(wrap(value))]).unwrap())
        .collect::<Vec<_>>();
    for pair in encoded.windows(2) {
        assert!(pair[0] < pair[1], "encoded order differs for {ordered:?}");
    }
}

#[test]
fn typed_component_keys_preserve_independent_value_order() {
    let decimal_bound = 10_i128.pow(37);
    assert_typed_values_sort_like_encoded_keys(ScalarType::Boolean, &[false, true], |value| {
        ScalarValue::Boolean(*value)
    });
    assert_typed_values_sort_like_encoded_keys(
        ScalarType::SmallInt,
        &[i16::MIN, -1, 0, 1, i16::MAX],
        |value| ScalarValue::SmallInt(*value),
    );
    assert_typed_values_sort_like_encoded_keys(
        ScalarType::Integer,
        &[i32::MIN, -1, 0, 1, i32::MAX],
        |value| ScalarValue::Integer(*value),
    );
    assert_typed_values_sort_like_encoded_keys(
        ScalarType::BigInt,
        &[i64::MIN, -1, 0, 1, i64::MAX],
        |value| ScalarValue::BigInt(*value),
    );
    assert_typed_values_sort_like_encoded_keys(
        ScalarType::Decimal {
            precision: 38,
            scale: 0,
        },
        &[-decimal_bound, -120, -1, 0, 1, 120, decimal_bound],
        |value| ScalarValue::Decimal(*value),
    );
    assert_typed_values_sort_like_encoded_keys(
        ScalarType::Text,
        &["", "A", "A\0", "A\u{e9}", "B", "é", "𐀀"],
        |value| ScalarValue::Text((*value).to_owned()),
    );
    assert_typed_values_sort_like_encoded_keys(
        ScalarType::Varchar { max_chars: 8 },
        &["", "a", "aa", "b", "é", "𐀀"],
        |value| ScalarValue::Varchar((*value).to_owned()),
    );
    assert_typed_values_sort_like_encoded_keys(
        ScalarType::Bytea,
        &[vec![], vec![0], vec![0, 0], vec![0, 1], vec![1], vec![0xff]],
        |value| ScalarValue::Bytea(value.clone()),
    );
    let mut uuid_mid = [0; 16];
    uuid_mid[15] = 1;
    assert_typed_values_sort_like_encoded_keys(
        ScalarType::Uuid,
        &[[0; 16], uuid_mid, [0xff; 16]],
        |value| ScalarValue::Uuid(*value),
    );
    assert_typed_values_sort_like_encoded_keys(
        ScalarType::Date,
        &[-719_162, -1, 0, 1, 2_932_896],
        |value| ScalarValue::Date(*value),
    );
    assert_typed_values_sort_like_encoded_keys(
        ScalarType::TimestampTz,
        &[-62_135_596_800_000_000, -1, 0, 1, 253_402_300_799_999_999],
        |value| ScalarValue::TimestampTz(*value),
    );
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
    let schema_ids = [[0x10; 16], [0x20; 16], [0x30; 16]];
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
        &[[0x30; 16], [0x20; 16]],
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
    let schema_ids = [[0x10; 16], [0x30; 16]];
    let payload = encode_tuple(&schema_columns, 4, &[Some(ScalarValue::Integer(8)), None]).unwrap();
    let args = (
        [0x22; 16],
        [0x33; 16],
        4,
        &schema_ids[..],
        &schema_columns[..],
        &payload[..],
    );
    let derived = derive_index_key(
        args.0,
        args.1,
        args.2,
        args.3,
        args.4,
        args.5,
        &[[0x30; 16]],
    )
    .unwrap();
    assert!(derived.has_null);
    assert_eq!(derived.key, compose_key([0x22; 16], &[0xff], [0x33; 16]));

    assert_eq!(
        derive_index_key(
            args.0,
            args.1,
            args.2,
            &[[0x10; 16], [0x10; 16]],
            args.4,
            args.5,
            &[[0x10; 16]],
        ),
        Err(DeriveIndexKeyError::InvalidDefinition)
    );
    assert_eq!(
        derive_index_key(
            args.0,
            args.1,
            args.2,
            args.3,
            args.4,
            args.5,
            &[[0x10; 16], [0x10; 16]],
        ),
        Err(DeriveIndexKeyError::InvalidDefinition)
    );
    assert_eq!(
        derive_index_key(
            args.0,
            args.1,
            args.2,
            args.3,
            args.4,
            args.5,
            &[[0x99; 16]],
        ),
        Err(DeriveIndexKeyError::InvalidDefinition)
    );
    assert_eq!(
        derive_index_key(args.0, args.1, args.2, args.3, args.4, args.5, &[[0; 16]],),
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
            &[[0x10; 16]]
        ),
        Err(DeriveIndexKeyError::Tuple(TupleError::InvalidLength))
    );
    assert_eq!(
        derive_index_key(args.0, args.1, 5, args.3, args.4, args.5, &[[0x10; 16]]),
        Err(DeriveIndexKeyError::Tuple(
            TupleError::SchemaVersionMismatch
        ))
    );
}
