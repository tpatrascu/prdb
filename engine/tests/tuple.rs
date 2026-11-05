use prdb_engine::tuple::{
    ColumnSchema, FieldPatch, MAX_TUPLE_BYTES, ScalarType, ScalarValue, Tuple, TupleError,
    apply_patch, decode_tuple, encode_tuple,
};

fn col(scalar_type: ScalarType, nullable: bool) -> ColumnSchema {
    ColumnSchema {
        scalar_type,
        nullable,
    }
}

#[test]
fn every_initial_scalar_type_round_trips_with_canonical_widths() {
    let columns = [
        col(ScalarType::Boolean, false),
        col(ScalarType::SmallInt, false),
        col(ScalarType::Integer, false),
        col(ScalarType::BigInt, false),
        col(
            ScalarType::Decimal {
                precision: 8,
                scale: 2,
            },
            false,
        ),
        col(ScalarType::Text, false),
        col(ScalarType::Varchar { max_chars: 2 }, false),
        col(ScalarType::Bytea, false),
        col(ScalarType::Uuid, false),
        col(ScalarType::Date, false),
        col(ScalarType::TimestampTz, false),
        col(ScalarType::Text, true),
    ];
    let values = vec![
        Some(ScalarValue::Boolean(true)),
        Some(ScalarValue::SmallInt(-1234)),
        Some(ScalarValue::Integer(i32::MIN)),
        Some(ScalarValue::BigInt(i64::MAX)),
        Some(ScalarValue::Decimal(-123456)),
        Some(ScalarValue::Text("héllo".into())),
        Some(ScalarValue::Varchar("éx".into())),
        Some(ScalarValue::Bytea(vec![0, 0xff, 1])),
        Some(ScalarValue::Uuid([0x42; 16])),
        Some(ScalarValue::Date(-719_162)),
        Some(ScalarValue::TimestampTz(-62_135_596_800_000_000)),
        None,
    ];
    let encoded = encode_tuple(&columns, 7, &values).unwrap();
    assert_eq!(&encoded[..4], b"TUP1");
    assert_eq!(
        decode_tuple(&columns, 7, &encoded).unwrap(),
        Tuple {
            schema_version: 7,
            values
        }
    );
}

#[test]
fn patches_preserve_untouched_fields_and_are_all_or_nothing() {
    let columns = [
        col(ScalarType::Text, false),
        col(ScalarType::Integer, false),
        col(ScalarType::Bytea, true),
    ];
    let original_values = vec![
        Some(ScalarValue::Text("private salary".into())),
        Some(ScalarValue::Integer(10)),
        None,
    ];
    let mut tuple = decode_tuple(
        &columns,
        1,
        &encode_tuple(&columns, 1, &original_values).unwrap(),
    )
    .unwrap();
    apply_patch(
        &columns,
        &mut tuple,
        &[FieldPatch {
            ordinal: 1,
            value: Some(ScalarValue::Integer(11)),
        }],
    )
    .unwrap();
    assert_eq!(tuple.values[0], original_values[0]);
    assert_eq!(tuple.values[1], Some(ScalarValue::Integer(11)));
    assert_eq!(tuple.values[2], None);

    let before = tuple.clone();
    assert_eq!(
        apply_patch(
            &columns,
            &mut tuple,
            &[
                FieldPatch {
                    ordinal: 1,
                    value: Some(ScalarValue::Integer(12)),
                },
                FieldPatch {
                    ordinal: 2,
                    value: Some(ScalarValue::Text("wrong type".into())),
                },
            ]
        ),
        Err(TupleError::TypeMismatch)
    );
    assert_eq!(tuple, before);
}

#[test]
fn tuple_decoder_rejects_versions_states_nulls_types_and_trailing_bytes() {
    let columns = [
        col(ScalarType::Boolean, false),
        col(ScalarType::Varchar { max_chars: 1 }, true),
    ];
    let canonical = encode_tuple(
        &columns,
        1,
        &[
            Some(ScalarValue::Boolean(false)),
            Some(ScalarValue::Varchar("é".into())),
        ],
    )
    .unwrap();
    assert_eq!(
        decode_tuple(&columns, 2, &canonical),
        Err(TupleError::SchemaVersionMismatch)
    );

    let mut bytes = canonical.clone();
    bytes[6] = 1;
    assert_eq!(
        decode_tuple(&columns, 1, &bytes),
        Err(TupleError::NonzeroFlags)
    );
    let mut bytes = canonical.clone();
    bytes[20] = 2;
    assert_eq!(
        decode_tuple(&columns, 1, &bytes),
        Err(TupleError::InvalidFieldState)
    );
    let mut bytes = canonical.clone();
    bytes[25] = 2; // invalid BOOLEAN byte
    assert_eq!(
        decode_tuple(&columns, 1, &bytes),
        Err(TupleError::InvalidScalarEncoding)
    );
    let mut bytes = canonical.clone();
    bytes.push(0);
    assert_eq!(
        decode_tuple(&columns, 1, &bytes),
        Err(TupleError::InvalidLength)
    );
    let mut bytes = canonical;
    bytes[20] = 0; // NULL in a non-null BOOLEAN column
    bytes[21..25].fill(0);
    assert_eq!(
        decode_tuple(&columns, 1, &bytes),
        Err(TupleError::InvalidNull)
    );
}

#[test]
fn scalar_schema_ranges_and_patch_indices_are_checked() {
    assert_eq!(
        encode_tuple(
            &[col(
                ScalarType::Decimal {
                    precision: 2,
                    scale: 0
                },
                false
            )],
            1,
            &[Some(ScalarValue::Decimal(100))]
        ),
        Err(TupleError::DecimalPrecisionExceeded)
    );
    assert_eq!(
        encode_tuple(
            &[col(ScalarType::Date, false)],
            1,
            &[Some(ScalarValue::Date(2_932_897))]
        ),
        Err(TupleError::InvalidScalarRange)
    );
    let columns = [col(ScalarType::Integer, false)];
    let mut tuple = Tuple {
        schema_version: 1,
        values: vec![Some(ScalarValue::Integer(1))],
    };
    assert_eq!(
        apply_patch(
            &columns,
            &mut tuple,
            &[
                FieldPatch {
                    ordinal: 0,
                    value: Some(ScalarValue::Integer(2)),
                },
                FieldPatch {
                    ordinal: 0,
                    value: Some(ScalarValue::Integer(3)),
                },
            ]
        ),
        Err(TupleError::DuplicatePatch)
    );
    assert_eq!(
        apply_patch(
            &columns,
            &mut tuple,
            &[FieldPatch {
                ordinal: 3,
                value: Some(ScalarValue::Integer(2)),
            }]
        ),
        Err(TupleError::PatchOutOfRange)
    );
}

#[test]
fn tuple_encoder_checks_total_row_budget_before_copying_variable_values() {
    let columns = [col(ScalarType::Bytea, false)];
    let max_value = vec![0x5a; MAX_TUPLE_BYTES - 25];
    let encoded = encode_tuple(&columns, 1, &[Some(ScalarValue::Bytea(max_value))]).unwrap();
    assert_eq!(encoded.len(), MAX_TUPLE_BYTES);
    let too_large = vec![0x5a; MAX_TUPLE_BYTES - 24];
    assert_eq!(
        encode_tuple(&columns, 1, &[Some(ScalarValue::Bytea(too_large))]),
        Err(TupleError::TooLarge)
    );
}
