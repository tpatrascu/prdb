use super::information_schema::information_schema_column_type;
use crate::tuple::ScalarType;

#[test]
fn reports_declared_type_names_and_applicable_parameters() {
    assert_eq!(
        information_schema_column_type(ScalarType::Varchar { max_chars: 37 }),
        ("CHARACTER VARYING", Some(37), None, None)
    );
    assert_eq!(
        information_schema_column_type(ScalarType::Decimal {
            precision: 12,
            scale: 3,
        }),
        ("DECIMAL", None, Some(12), Some(3))
    );
    assert_eq!(
        information_schema_column_type(ScalarType::SmallInt),
        ("SMALLINT", None, Some(5), Some(0))
    );
    assert_eq!(
        information_schema_column_type(ScalarType::Integer),
        ("INTEGER", None, Some(10), Some(0))
    );
    assert_eq!(
        information_schema_column_type(ScalarType::BigInt),
        ("BIGINT", None, Some(19), Some(0))
    );
    assert_eq!(
        information_schema_column_type(ScalarType::TimestampTz),
        ("TIMESTAMP WITH TIME ZONE", None, None, None)
    );
}
