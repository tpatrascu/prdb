use super::ast::*;
use super::lexer::*;
use crate::{
    client_submit::{AdminColumnDefault, AdminColumnDefinition},
    tuple::{ScalarType, ScalarValue},
};

pub(super) fn validate_check_columns(
    expression: &CheckExpressionSql,
    columns: &[AdminColumnDefinition],
) -> Result<(), DdlSqlError> {
    bind_check_expression(expression, |name| {
        columns
            .iter()
            .position(|column| column.name == name)
            .map(|ordinal| (ordinal, columns[ordinal].scalar_type))
    })?;
    Ok(())
}

pub(super) fn validate_column_check_expression(
    column: &str,
    expression: &CheckExpressionSql,
) -> Result<(), DdlSqlError> {
    fn only_column(column: &str, expression: &CheckExpressionSql) -> bool {
        match expression {
            CheckExpressionSql::Column(name) => name == column,
            CheckExpressionSql::IsNull(expression) | CheckExpressionSql::IsNotNull(expression) => {
                only_column(column, expression)
            }
            CheckExpressionSql::BooleanTest { expression, .. } => only_column(column, expression),
            CheckExpressionSql::Literal(_) => true,
            CheckExpressionSql::CurrentPrincipalId | CheckExpressionSql::HasRole(_) => false,
            CheckExpressionSql::Compare { left, right, .. } => {
                only_column(column, left) && only_column(column, right)
            }
            CheckExpressionSql::Add(left, right)
            | CheckExpressionSql::Subtract(left, right)
            | CheckExpressionSql::Multiply(left, right)
            | CheckExpressionSql::Modulo(left, right)
            | CheckExpressionSql::Divide(left, right) => {
                only_column(column, left) && only_column(column, right)
            }
            CheckExpressionSql::Negate(expression) => only_column(column, expression),
            CheckExpressionSql::In { expression, .. }
            | CheckExpressionSql::Between { expression, .. } => only_column(column, expression),
            CheckExpressionSql::DistinctFrom {
                expression, right, ..
            } => only_column(column, expression) && only_column(column, right),
            CheckExpressionSql::Not(inner) => only_column(column, inner),
            CheckExpressionSql::And(items) | CheckExpressionSql::Or(items) => {
                items.iter().all(|item| only_column(column, item))
            }
        }
    }

    if only_column(column, expression) {
        Ok(())
    } else {
        Err(DdlSqlError::InvalidStatement)
    }
}

/// Bind SQL CHECK names and context-typed literals to a row-local EIR1 tree.
pub fn bind_check_expression(
    expression: &CheckExpressionSql,
    lookup: impl Fn(&str) -> Option<(usize, ScalarType)>,
) -> Result<crate::authorization::Expr, DdlSqlError> {
    fn has_policy_context(expression: &CheckExpressionSql) -> bool {
        match expression {
            CheckExpressionSql::CurrentPrincipalId | CheckExpressionSql::HasRole(_) => true,
            CheckExpressionSql::Compare { left, right, .. } => {
                has_policy_context(left) || has_policy_context(right)
            }
            CheckExpressionSql::Add(left, right)
            | CheckExpressionSql::Subtract(left, right)
            | CheckExpressionSql::Multiply(left, right)
            | CheckExpressionSql::Modulo(left, right)
            | CheckExpressionSql::Divide(left, right) => {
                has_policy_context(left) || has_policy_context(right)
            }
            CheckExpressionSql::Negate(expression) => has_policy_context(expression),
            CheckExpressionSql::In { expression, .. }
            | CheckExpressionSql::Between { expression, .. } => has_policy_context(expression),
            CheckExpressionSql::DistinctFrom {
                expression, right, ..
            } => has_policy_context(expression) || has_policy_context(right),
            CheckExpressionSql::Not(inner) => has_policy_context(inner),
            CheckExpressionSql::And(items) | CheckExpressionSql::Or(items) => {
                items.iter().any(has_policy_context)
            }
            CheckExpressionSql::BooleanTest { expression, .. } => has_policy_context(expression),
            CheckExpressionSql::IsNull(expression) | CheckExpressionSql::IsNotNull(expression) => {
                has_policy_context(expression)
            }
            CheckExpressionSql::Column(_) | CheckExpressionSql::Literal(_) => false,
        }
    }
    if has_policy_context(expression) {
        return Err(DdlSqlError::InvalidStatement);
    }
    let mut no_roles = |_: &str| None;
    let expression = bind_expression(expression, &lookup, &mut no_roles)?;
    crate::expression::encode_expression(&expression).map_err(|_| DdlSqlError::InvalidStatement)?;
    Ok(expression)
}

pub fn bind_policy_expression(
    expression: &CheckExpressionSql,
    lookup: impl Fn(&str) -> Option<(usize, ScalarType)>,
    role_lookup: impl FnMut(&str) -> Option<[u8; 16]>,
) -> Result<crate::authorization::Expr, DdlSqlError> {
    let mut role_lookup = role_lookup;
    let expression = bind_expression(expression, &lookup, &mut role_lookup)?;
    crate::expression::encode_expression(&expression).map_err(|_| DdlSqlError::InvalidStatement)?;
    Ok(expression)
}

fn bind_expression(
    expression: &CheckExpressionSql,
    lookup: &impl Fn(&str) -> Option<(usize, ScalarType)>,
    role_lookup: &mut impl FnMut(&str) -> Option<[u8; 16]>,
) -> Result<crate::authorization::Expr, DdlSqlError> {
    fn scalar_compatible(left: ScalarType, right: ScalarType) -> bool {
        match (left, right) {
            (ScalarType::Decimal { scale: a, .. }, ScalarType::Decimal { scale: b, .. }) => a == b,
            (ScalarType::Varchar { .. }, ScalarType::Varchar { .. }) => true,
            _ => left == right,
        }
    }

    fn comparison_hint(
        expression: &CheckExpressionSql,
        lookup: &impl Fn(&str) -> Option<(usize, ScalarType)>,
    ) -> Option<ScalarType> {
        match expression {
            CheckExpressionSql::Column(name) => lookup(name).map(|(_, scalar_type)| scalar_type),
            CheckExpressionSql::CurrentPrincipalId => Some(ScalarType::Uuid),
            CheckExpressionSql::HasRole(_) => Some(ScalarType::Boolean),
            CheckExpressionSql::Literal(CheckLiteralSql::Boolean(_)) => Some(ScalarType::Boolean),
            CheckExpressionSql::Literal(CheckLiteralSql::String(_)) => Some(ScalarType::Text),
            CheckExpressionSql::Literal(CheckLiteralSql::Bytea(_)) => Some(ScalarType::Bytea),
            CheckExpressionSql::Literal(CheckLiteralSql::Number(number)) => {
                if number.contains('.') {
                    let scale = number.split_once('.')?.1.len();
                    (scale <= 38).then_some(ScalarType::Decimal {
                        precision: 38,
                        scale: scale as u8,
                    })
                } else {
                    Some(ScalarType::BigInt)
                }
            }
            CheckExpressionSql::IsNull(_)
            | CheckExpressionSql::IsNotNull(_)
            | CheckExpressionSql::BooleanTest { .. }
            | CheckExpressionSql::In { .. }
            | CheckExpressionSql::Between { .. }
            | CheckExpressionSql::DistinctFrom { .. }
            | CheckExpressionSql::Compare { .. }
            | CheckExpressionSql::Not(_)
            | CheckExpressionSql::And(_)
            | CheckExpressionSql::Or(_) => Some(ScalarType::Boolean),
            CheckExpressionSql::Literal(CheckLiteralSql::Null) => None,
            CheckExpressionSql::Add(left, right)
            | CheckExpressionSql::Subtract(left, right)
            | CheckExpressionSql::Multiply(left, right)
            | CheckExpressionSql::Modulo(left, right)
            | CheckExpressionSql::Divide(left, right) => {
                let left_hint = comparison_hint(left, lookup);
                let right_hint = comparison_hint(right, lookup);
                if matches!(left.as_ref(), CheckExpressionSql::Literal(_)) {
                    right_hint.or(left_hint)
                } else {
                    left_hint.or(right_hint)
                }
            }
            CheckExpressionSql::Negate(expression) => comparison_hint(expression, lookup),
        }
    }

    fn common_numeric_literal_type(
        left: &CheckExpressionSql,
        right: &CheckExpressionSql,
    ) -> Option<ScalarType> {
        let (
            CheckExpressionSql::Literal(CheckLiteralSql::Number(left)),
            CheckExpressionSql::Literal(CheckLiteralSql::Number(right)),
        ) = (left, right)
        else {
            return None;
        };
        let left_scale = left.split_once('.').map_or(0, |(_, scale)| scale.len());
        let right_scale = right.split_once('.').map_or(0, |(_, scale)| scale.len());
        let scale = left_scale.max(right_scale);
        if scale == 0 {
            Some(ScalarType::BigInt)
        } else if scale <= 38 {
            Some(ScalarType::Decimal {
                precision: 38,
                scale: scale as u8,
            })
        } else {
            None
        }
    }

    fn number_value(literal: &str, scalar_type: ScalarType) -> Result<ScalarValue, DdlSqlError> {
        match scalar_type {
            ScalarType::SmallInt if !literal.contains('.') => {
                Ok(ScalarValue::SmallInt(literal.parse().map_err(invalid)?))
            }
            ScalarType::Integer if !literal.contains('.') => {
                Ok(ScalarValue::Integer(literal.parse().map_err(invalid)?))
            }
            ScalarType::BigInt if !literal.contains('.') => {
                Ok(ScalarValue::BigInt(literal.parse().map_err(invalid)?))
            }
            ScalarType::Decimal { precision, scale } => Ok(ScalarValue::Decimal(
                parse_decimal_coefficient(literal, precision, scale)?,
            )),
            ScalarType::Date if !literal.contains('.') => {
                Ok(ScalarValue::Date(literal.parse().map_err(invalid)?))
            }
            ScalarType::TimestampTz if !literal.contains('.') => {
                Ok(ScalarValue::TimestampTz(literal.parse().map_err(invalid)?))
            }
            _ => Err(DdlSqlError::InvalidStatement),
        }
    }

    fn bind(
        expression: &CheckExpressionSql,
        expected: Option<ScalarType>,
        lookup: &impl Fn(&str) -> Option<(usize, ScalarType)>,
        role_lookup: &mut impl FnMut(&str) -> Option<[u8; 16]>,
    ) -> Result<(crate::authorization::Expr, Option<ScalarType>), DdlSqlError> {
        use crate::authorization::Expr;
        use CheckExpressionSql as Sql;
        let bound = match expression {
            Sql::Column(name) => {
                let (ordinal, scalar_type) = lookup(name).ok_or(DdlSqlError::InvalidStatement)?;
                (Expr::Field(ordinal), Some(scalar_type))
            }
            Sql::CurrentPrincipalId => (Expr::CurrentPrincipalId, Some(ScalarType::Uuid)),
            Sql::HasRole(name) => (
                Expr::HasRole(role_lookup(name).ok_or(DdlSqlError::InvalidStatement)?),
                Some(ScalarType::Boolean),
            ),
            Sql::Add(left, right)
            | Sql::Subtract(left, right)
            | Sql::Multiply(left, right)
            | Sql::Modulo(left, right)
            | Sql::Divide(left, right) => {
                let left_is_null_literal =
                    matches!(left.as_ref(), Sql::Literal(CheckLiteralSql::Null));
                let right_is_null_literal =
                    matches!(right.as_ref(), Sql::Literal(CheckLiteralSql::Null));
                let (left, right, left_type, right_type) =
                    if matches!(left.as_ref(), Sql::Literal(_)) {
                        let (right, right_type) = bind(right, None, lookup, role_lookup)?;
                        let (left, left_type) = bind(left, right_type, lookup, role_lookup)?;
                        (left, right, left_type, right_type)
                    } else {
                        let (left, left_type) = bind(left, None, lookup, role_lookup)?;
                        let (right, right_type) = bind(right, left_type, lookup, role_lookup)?;
                        (left, right, left_type, right_type)
                    };
                let arithmetic_type = match (left_type, right_type) {
                    (Some(left), Some(right)) if left == right => Some(left),
                    (None, Some(right)) if left_is_null_literal => Some(right),
                    (Some(left), None) if right_is_null_literal => Some(left),
                    _ => None,
                };
                if !matches!(
                    arithmetic_type,
                    Some(ScalarType::SmallInt | ScalarType::Integer | ScalarType::BigInt)
                ) {
                    return Err(DdlSqlError::InvalidStatement);
                }
                let expression = match expression {
                    Sql::Add(_, _) => Expr::Add(Box::new(left), Box::new(right)),
                    Sql::Subtract(_, _) => Expr::Subtract(Box::new(left), Box::new(right)),
                    Sql::Multiply(_, _) => Expr::Multiply(Box::new(left), Box::new(right)),
                    Sql::Modulo(_, _) => Expr::Modulo(Box::new(left), Box::new(right)),
                    Sql::Divide(_, _) => Expr::Divide(Box::new(left), Box::new(right)),
                    _ => unreachable!(),
                };
                (expression, arithmetic_type)
            }
            Sql::Negate(inner) => {
                let (inner, scalar_type) = bind(
                    inner,
                    expected.or_else(|| comparison_hint(inner, lookup)),
                    lookup,
                    role_lookup,
                )?;
                let null_literal = matches!(inner, Expr::Literal(None));
                if !null_literal
                    && !matches!(
                        scalar_type,
                        Some(ScalarType::SmallInt | ScalarType::Integer | ScalarType::BigInt)
                    )
                {
                    return Err(DdlSqlError::InvalidStatement);
                }
                (Expr::Negate(Box::new(inner)), scalar_type)
            }
            Sql::Literal(literal) => {
                let (value, scalar_type) = match literal {
                    CheckLiteralSql::Null => (None, None),
                    CheckLiteralSql::Boolean(value) => (
                        Some(ScalarValue::Boolean(*value)),
                        Some(ScalarType::Boolean),
                    ),
                    CheckLiteralSql::Number(number) => {
                        let scalar_type = expected
                            .or_else(|| comparison_hint(expression, lookup))
                            .unwrap_or_else(|| {
                                if number.contains('.') {
                                    let scale = number
                                        .split_once('.')
                                        .map_or(0, |(_, fraction)| fraction.len());
                                    ScalarType::Decimal {
                                        precision: 38,
                                        scale: scale as u8,
                                    }
                                } else {
                                    ScalarType::BigInt
                                }
                            });
                        (Some(number_value(number, scalar_type)?), Some(scalar_type))
                    }
                    CheckLiteralSql::String(value) => {
                        let scalar_type = expected.unwrap_or(ScalarType::Text);
                        let value = match scalar_type {
                            ScalarType::Text => ScalarValue::Text(value.clone()),
                            ScalarType::Varchar { max_chars }
                                if value.chars().count() <= max_chars as usize =>
                            {
                                ScalarValue::Varchar(value.clone())
                            }
                            ScalarType::Uuid => ScalarValue::Uuid(parse_uuid(value)?),
                            _ => return Err(DdlSqlError::InvalidStatement),
                        };
                        (Some(value), Some(scalar_type))
                    }
                    CheckLiteralSql::Bytea(value) => {
                        if expected.is_some_and(|scalar_type| scalar_type != ScalarType::Bytea) {
                            return Err(DdlSqlError::InvalidStatement);
                        }
                        (
                            Some(ScalarValue::Bytea(decode_hex(value)?)),
                            Some(ScalarType::Bytea),
                        )
                    }
                };
                (Expr::Literal(value), scalar_type)
            }
            Sql::IsNull(value) | Sql::IsNotNull(value) => {
                let (value, _) = bind(value, None, lookup, role_lookup)?;
                let is_null = Expr::IsNull(Box::new(value));
                let expression = if matches!(expression, Sql::IsNotNull(_)) {
                    Expr::Not(Box::new(is_null))
                } else {
                    is_null
                };
                (expression, Some(ScalarType::Boolean))
            }
            Sql::BooleanTest {
                expression,
                test,
                negated,
            } => {
                let (value, scalar_type) =
                    bind(expression, Some(ScalarType::Boolean), lookup, role_lookup)?;
                if scalar_type != Some(ScalarType::Boolean) {
                    return Err(DdlSqlError::InvalidStatement);
                }
                let expression = match test {
                    CheckBooleanTest::Unknown => Expr::IsNull(Box::new(value)),
                    CheckBooleanTest::True | CheckBooleanTest::False => {
                        let is_known = Expr::Not(Box::new(Expr::IsNull(Box::new(value.clone()))));
                        let expected = *test == CheckBooleanTest::True;
                        let equals_expected = Expr::Equal(
                            Box::new(value),
                            Box::new(Expr::Literal(Some(ScalarValue::Boolean(expected)))),
                        );
                        Expr::And(vec![is_known, equals_expected])
                    }
                };
                let expression = if *negated {
                    Expr::Not(Box::new(expression))
                } else {
                    expression
                };
                (expression, Some(ScalarType::Boolean))
            }
            Sql::Compare {
                operator,
                left,
                right,
            } => {
                let (left_hint, right_hint) =
                    if let Some(scalar_type) = common_numeric_literal_type(left, right) {
                        (Some(scalar_type), Some(scalar_type))
                    } else {
                        (
                            comparison_hint(left, lookup),
                            comparison_hint(right, lookup),
                        )
                    };
                let (left, left_type) = bind(left, right_hint, lookup, role_lookup)?;
                let (right, right_type) = bind(
                    right,
                    left_hint.or(right_hint).or(left_type),
                    lookup,
                    role_lookup,
                )?;
                if let (Some(left_type), Some(right_type)) = (left_type, right_type)
                    && !scalar_compatible(left_type, right_type)
                {
                    return Err(DdlSqlError::InvalidStatement);
                }
                let expression = match operator {
                    CheckComparisonOperator::Equal => Expr::Equal(Box::new(left), Box::new(right)),
                    CheckComparisonOperator::NotEqual => {
                        Expr::NotEqual(Box::new(left), Box::new(right))
                    }
                    CheckComparisonOperator::Less => Expr::Less(Box::new(left), Box::new(right)),
                    CheckComparisonOperator::LessEqual => {
                        Expr::LessEqual(Box::new(left), Box::new(right))
                    }
                    CheckComparisonOperator::Greater => {
                        Expr::Greater(Box::new(left), Box::new(right))
                    }
                    CheckComparisonOperator::GreaterEqual => {
                        Expr::GreaterEqual(Box::new(left), Box::new(right))
                    }
                };
                (expression, Some(ScalarType::Boolean))
            }
            Sql::In {
                expression,
                values,
                negated,
            } => {
                let (left, column_type) = bind(expression, None, lookup, role_lookup)?;
                let column_type = column_type.ok_or(DdlSqlError::InvalidStatement)?;
                let mut alternatives = Vec::with_capacity(values.len());
                for value in values {
                    let (value, value_type) = bind(value, Some(column_type), lookup, role_lookup)?;
                    if value_type
                        .is_some_and(|value_type| !scalar_compatible(column_type, value_type))
                    {
                        return Err(DdlSqlError::InvalidStatement);
                    }
                    alternatives.push(Expr::Equal(Box::new(left.clone()), Box::new(value)));
                }
                let expression = Expr::Or(alternatives);
                let expression = if *negated {
                    Expr::Not(Box::new(expression))
                } else {
                    expression
                };
                (expression, Some(ScalarType::Boolean))
            }
            Sql::Between {
                expression,
                lower,
                upper,
                negated,
            } => {
                let (left, column_type) = bind(expression, None, lookup, role_lookup)?;
                let column_type = column_type.ok_or(DdlSqlError::InvalidStatement)?;
                let bind_bound = |bound_expression: &CheckExpressionSql,
                                  role_lookup: &mut _|
                 -> Result<Expr, DdlSqlError> {
                    let (bound, bound_type) =
                        bind(bound_expression, Some(column_type), lookup, role_lookup)?;
                    if bound_type
                        .is_some_and(|bound_type| !scalar_compatible(column_type, bound_type))
                    {
                        return Err(DdlSqlError::InvalidStatement);
                    }
                    Ok(bound)
                };
                let lower = bind_bound(lower, role_lookup)?;
                let upper = bind_bound(upper, role_lookup)?;
                let expression = Expr::And(vec![
                    Expr::GreaterEqual(Box::new(left.clone()), Box::new(lower)),
                    Expr::LessEqual(Box::new(left), Box::new(upper)),
                ]);
                let expression = if *negated {
                    Expr::Not(Box::new(expression))
                } else {
                    expression
                };
                (expression, Some(ScalarType::Boolean))
            }
            Sql::DistinctFrom {
                expression,
                right,
                negated,
            } => {
                let right_is_literal = matches!(right.as_ref(), Sql::Literal(_));
                let (left_hint, right_hint) =
                    if let Some(scalar_type) = common_numeric_literal_type(expression, right) {
                        (Some(scalar_type), Some(scalar_type))
                    } else {
                        (
                            comparison_hint(expression, lookup),
                            comparison_hint(right, lookup),
                        )
                    };
                let (left, left_type) = bind(expression, right_hint, lookup, role_lookup)?;
                let (right, right_type) = bind(
                    right,
                    left_hint.or(right_hint).or(left_type),
                    lookup,
                    role_lookup,
                )?;
                if let (Some(left_type), Some(right_type)) = (left_type, right_type)
                    && !scalar_compatible(left_type, right_type)
                {
                    return Err(DdlSqlError::InvalidStatement);
                }
                let expression = if right_type.is_none() {
                    let is_null = Expr::IsNull(Box::new(left));
                    if *negated {
                        is_null
                    } else {
                        Expr::Not(Box::new(is_null))
                    }
                } else {
                    let distinct = if right_is_literal {
                        Expr::Or(vec![
                            Expr::IsNull(Box::new(left.clone())),
                            Expr::NotEqual(Box::new(left), Box::new(right)),
                        ])
                    } else {
                        let left_is_null = Expr::IsNull(Box::new(left.clone()));
                        let right_is_null = Expr::IsNull(Box::new(right.clone()));
                        let left_is_not_null = Expr::Not(Box::new(left_is_null.clone()));
                        let right_is_not_null = Expr::Not(Box::new(right_is_null.clone()));
                        Expr::Or(vec![
                            Expr::And(vec![left_is_null, right_is_not_null.clone()]),
                            Expr::And(vec![left_is_not_null.clone(), right_is_null]),
                            Expr::And(vec![
                                left_is_not_null,
                                right_is_not_null,
                                Expr::NotEqual(Box::new(left), Box::new(right)),
                            ]),
                        ])
                    };
                    if *negated {
                        Expr::Not(Box::new(distinct))
                    } else {
                        distinct
                    }
                };
                (expression, Some(ScalarType::Boolean))
            }
            Sql::Not(inner) => {
                let (inner, scalar_type) =
                    bind(inner, Some(ScalarType::Boolean), lookup, role_lookup)?;
                if scalar_type.is_some_and(|scalar_type| scalar_type != ScalarType::Boolean) {
                    return Err(DdlSqlError::InvalidStatement);
                }
                (Expr::Not(Box::new(inner)), Some(ScalarType::Boolean))
            }
            Sql::And(items) | Sql::Or(items) => {
                let mut expressions = Vec::with_capacity(items.len());
                for item in items {
                    let (item, scalar_type) =
                        bind(item, Some(ScalarType::Boolean), lookup, role_lookup)?;
                    if scalar_type.is_some_and(|scalar_type| scalar_type != ScalarType::Boolean) {
                        return Err(DdlSqlError::InvalidStatement);
                    }
                    expressions.push(item);
                }
                let expression = if matches!(expression, Sql::And(_)) {
                    Expr::And(expressions)
                } else {
                    Expr::Or(expressions)
                };
                (expression, Some(ScalarType::Boolean))
            }
        };
        Ok(bound)
    }

    let (expression, scalar_type) =
        bind(expression, Some(ScalarType::Boolean), lookup, role_lookup)?;
    if scalar_type.is_some_and(|scalar_type| scalar_type != ScalarType::Boolean) {
        return Err(DdlSqlError::InvalidStatement);
    }
    Ok(expression)
}

pub fn bind_default_literal(
    literal: &DefaultLiteralSql,
    scalar_type: ScalarType,
) -> Result<AdminColumnDefault, DdlSqlError> {
    let value = match literal {
        DefaultLiteralSql::Null => return Ok(AdminColumnDefault::Null),
        DefaultLiteralSql::CurrentPrincipalId if scalar_type == ScalarType::Uuid => {
            return Ok(AdminColumnDefault::CurrentPrincipalId);
        }
        DefaultLiteralSql::Boolean(value) if scalar_type == ScalarType::Boolean => {
            ScalarValue::Boolean(*value)
        }
        DefaultLiteralSql::Number(value) => match scalar_type {
            ScalarType::SmallInt => ScalarValue::SmallInt(value.parse().map_err(invalid)?),
            ScalarType::Integer => ScalarValue::Integer(value.parse().map_err(invalid)?),
            ScalarType::BigInt => ScalarValue::BigInt(value.parse().map_err(invalid)?),
            ScalarType::Decimal { precision, scale } => {
                ScalarValue::Decimal(parse_decimal_coefficient(value, precision, scale)?)
            }
            ScalarType::Date => ScalarValue::Date(value.parse().map_err(invalid)?),
            ScalarType::TimestampTz => ScalarValue::TimestampTz(value.parse().map_err(invalid)?),
            _ => return Err(DdlSqlError::InvalidStatement),
        },
        DefaultLiteralSql::String(value) => match scalar_type {
            ScalarType::Text => ScalarValue::Text(value.clone()),
            ScalarType::Varchar { max_chars } if value.chars().count() <= max_chars as usize => {
                ScalarValue::Varchar(value.clone())
            }
            ScalarType::Uuid => ScalarValue::Uuid(parse_uuid(value)?),
            _ => return Err(DdlSqlError::InvalidStatement),
        },
        DefaultLiteralSql::Bytea(value) if scalar_type == ScalarType::Bytea => {
            ScalarValue::Bytea(value.clone())
        }
        _ => return Err(DdlSqlError::InvalidStatement),
    };
    Ok(AdminColumnDefault::Literal(value))
}
