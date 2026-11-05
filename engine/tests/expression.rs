use prdb_engine::authorization::Expr;
use prdb_engine::catalog::{CatalogField, WireType, encode_envelope, family_schema};
use prdb_engine::expression::{
    CatalogExpression, ExpressionPurpose, decode_catalog_expression, decode_expression,
    encode_expression, validate_expression_binding,
};
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue};

fn expression_record(purpose: u16, ir_version: u32, ir: &[u8]) -> Vec<u8> {
    let mut key = 7u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&[8; 16]);
    encode_envelope(
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
                payload: ir_version.to_le_bytes().to_vec(),
            },
            CatalogField {
                tag: 3,
                wire_type: WireType::Id,
                payload: [9; 16].to_vec(),
            },
            CatalogField {
                tag: 4,
                wire_type: WireType::Bytes,
                payload: vec![0; 8],
            },
            CatalogField {
                tag: 5,
                wire_type: WireType::Bytes,
                payload: ir.to_vec(),
            },
            CatalogField {
                tag: 6,
                wire_type: WireType::Ids,
                payload: 0u32.to_le_bytes().to_vec(),
            },
        ],
    )
    .unwrap()
}

#[test]
fn expression_codec_matches_the_hand_checked_eir1_fixture() {
    let expression = Expr::Equal(
        Box::new(Expr::Field(0)),
        Box::new(Expr::Literal(Some(ScalarValue::Text("x".to_owned())))),
    );
    let expected = [
        b'E', b'I', b'R', b'1', 1, 0, 0, 0,  // header
        16, // Equal
        2, 0, 0, // Field(0)
        1, 6, 1, 0, 0, 0, b'x', // Literal(Text("x"))
    ];
    assert_eq!(encode_expression(&expression).unwrap(), expected);
    assert_eq!(decode_expression(&expected).unwrap(), expression);
}

#[test]
fn expression_codec_round_trips_all_nodes_and_scalar_literals() {
    let expression = Expr::And(vec![
        Expr::Or(vec![
            Expr::HasRole([1; 16]),
            Expr::Not(Box::new(Expr::IsNull(Box::new(Expr::Field(4095))))),
        ]),
        Expr::Equal(
            Box::new(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            Box::new(Expr::Literal(Some(ScalarValue::Boolean(false)))),
        ),
        Expr::NotEqual(
            Box::new(Expr::Literal(Some(ScalarValue::SmallInt(-1)))),
            Box::new(Expr::Literal(Some(ScalarValue::Integer(2)))),
        ),
        Expr::Less(
            Box::new(Expr::Literal(Some(ScalarValue::BigInt(-3)))),
            Box::new(Expr::Literal(Some(ScalarValue::Decimal(4)))),
        ),
        Expr::LessEqual(
            Box::new(Expr::Literal(Some(ScalarValue::Varchar("v".to_owned())))),
            Box::new(Expr::Literal(Some(ScalarValue::Text("t".to_owned())))),
        ),
        Expr::Greater(
            Box::new(Expr::Literal(Some(ScalarValue::Bytea(vec![0, 255])))),
            Box::new(Expr::Literal(Some(ScalarValue::Uuid([2; 16])))),
        ),
        Expr::GreaterEqual(
            Box::new(Expr::Literal(Some(ScalarValue::Date(0)))),
            Box::new(Expr::Literal(Some(ScalarValue::TimestampTz(0)))),
        ),
        Expr::Literal(None),
        Expr::CurrentPrincipalId,
    ]);
    let encoded = encode_expression(&expression).unwrap();
    assert_eq!(decode_expression(&encoded).unwrap(), expression);
}

#[test]
fn expression_codec_rejects_malformed_noncanonical_and_oversized_ir() {
    let valid = encode_expression(&Expr::CurrentPrincipalId).unwrap();
    for invalid in [
        b"BAD1\x01\0\0\0\x03".as_slice(),
        b"EIR1\x02\0\0\0\x03".as_slice(),
        b"EIR1\x01\0\x01\0\x03".as_slice(),
        b"EIR1\x01\0\0\0\xff".as_slice(),
        b"EIR1\x01\0\0\0\x03\0".as_slice(),
    ] {
        assert!(decode_expression(invalid).is_err());
    }
    let mut trailing = valid.clone();
    trailing.push(0);
    assert!(decode_expression(&trailing).is_err());
    assert!(decode_expression(&vec![0; 256 * 1024 + 1]).is_err());
    assert!(encode_expression(&Expr::Field(4096)).is_err());
    assert!(encode_expression(&Expr::HasRole([0; 16])).is_err());
    assert!(encode_expression(&Expr::Literal(Some(ScalarValue::Date(i32::MAX)))).is_err());
}

#[test]
fn expression_codec_enforces_node_and_depth_budgets() {
    let mut too_deep = Expr::CurrentPrincipalId;
    for _ in 0..32 {
        too_deep = Expr::Not(Box::new(too_deep));
    }
    assert!(encode_expression(&too_deep).is_err());

    let too_many = Expr::And(vec![Expr::CurrentPrincipalId; 256]);
    assert!(encode_expression(&too_many).is_err());
}

#[test]
fn catalog_expression_decoder_checks_metadata_and_decodes_purpose() {
    let ir = encode_expression(&Expr::CurrentPrincipalId).unwrap();
    let encoded = expression_record(1, 1, &ir);
    let decoded = decode_catalog_expression(&encoded).unwrap();
    assert_eq!(decoded.expression_id, [8; 16]);
    assert_eq!(decoded.purpose, ExpressionPurpose::Default);
    assert_eq!(decoded.result_type_id, [9; 16]);
    assert_eq!(decoded.result_parameters, [0; 8]);
    assert!(decoded.dependencies.is_empty());
    assert_eq!(decoded.expression, Expr::CurrentPrincipalId);
    assert!(matches!(
        decode_catalog_expression(&expression_record(1, 2, &ir)),
        Err(prdb_engine::expression::ExpressionCodecError::UnsupportedVersion)
    ));
    assert!(decode_catalog_expression(&expression_record(9, 1, &ir)).is_err());
    assert!(decode_catalog_expression(&expression_record(1, 1, b"bad")).is_err());
}

#[test]
fn schema_binding_checks_fields_types_result_metadata_and_dependencies() {
    let column_id = [21; 16];
    let expression = CatalogExpression {
        expression_id: [22; 16],
        purpose: ExpressionPurpose::Check,
        result_type_id: [23; 16],
        result_parameters: [0; 8],
        dependencies: vec![column_id],
        expression: Expr::Equal(Box::new(Expr::Field(0)), Box::new(Expr::CurrentPrincipalId)),
    };
    let columns = [ColumnSchema {
        scalar_type: ScalarType::Uuid,
        nullable: true,
    }];
    assert_eq!(
        validate_expression_binding(
            &expression,
            &columns,
            &[column_id],
            [23; 16],
            ScalarType::Boolean,
        ),
        Ok(())
    );

    let mut wrong_field = expression.clone();
    wrong_field.expression =
        Expr::Equal(Box::new(Expr::Field(1)), Box::new(Expr::CurrentPrincipalId));
    assert!(
        validate_expression_binding(
            &wrong_field,
            &columns,
            &[column_id],
            [23; 16],
            ScalarType::Boolean,
        )
        .is_err()
    );

    let mut wrong_type = expression.clone();
    wrong_type.expression = Expr::Equal(
        Box::new(Expr::Field(0)),
        Box::new(Expr::Literal(Some(ScalarValue::Text("x".to_owned())))),
    );
    assert!(
        validate_expression_binding(
            &wrong_type,
            &columns,
            &[column_id],
            [23; 16],
            ScalarType::Boolean,
        )
        .is_err()
    );

    let mut wrong_dependencies = expression.clone();
    wrong_dependencies.dependencies.clear();
    assert!(
        validate_expression_binding(
            &wrong_dependencies,
            &columns,
            &[column_id],
            [23; 16],
            ScalarType::Boolean,
        )
        .is_err()
    );

    let mut wrong_result = expression.clone();
    wrong_result.result_type_id = [24; 16];
    assert!(
        validate_expression_binding(
            &wrong_result,
            &columns,
            &[column_id],
            [23; 16],
            ScalarType::Boolean,
        )
        .is_err()
    );
}

#[test]
fn schema_binding_checks_type_parameters_and_boolean_policy_purposes() {
    let expression = CatalogExpression {
        expression_id: [31; 16],
        purpose: ExpressionPurpose::Default,
        result_type_id: [32; 16],
        result_parameters: [0; 8],
        dependencies: vec![],
        expression: Expr::Literal(Some(ScalarValue::Text("ok".to_owned()))),
    };
    assert!(
        validate_expression_binding(&expression, &[], &[], [32; 16], ScalarType::Text,).is_ok()
    );

    let varchar = CatalogExpression {
        purpose: ExpressionPurpose::Default,
        result_parameters: {
            let mut parameters = [0; 8];
            parameters[4..].copy_from_slice(&4u32.to_le_bytes());
            parameters
        },
        expression: Expr::Literal(Some(ScalarValue::Varchar("ok".to_owned()))),
        ..expression.clone()
    };
    assert!(
        validate_expression_binding(
            &varchar,
            &[],
            &[],
            [32; 16],
            ScalarType::Varchar { max_chars: 4 },
        )
        .is_ok()
    );

    let wrong_parameters = CatalogExpression {
        result_parameters: [1; 8],
        ..expression.clone()
    };
    assert!(
        validate_expression_binding(&wrong_parameters, &[], &[], [32; 16], ScalarType::Text,)
            .is_err()
    );

    let wrong_policy_result = CatalogExpression {
        purpose: ExpressionPurpose::PolicyUsing,
        ..expression
    };
    assert!(
        validate_expression_binding(&wrong_policy_result, &[], &[], [32; 16], ScalarType::Text,)
            .is_err()
    );
}
