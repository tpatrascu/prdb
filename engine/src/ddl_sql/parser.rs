use super::ast::*;
use super::lexer::*;
use super::{MAX_CHECK_IN_VALUES, MAX_DDL_BYTES};
use crate::{
    client_submit::AdminColumnDefault,
    tuple::{ScalarType, ScalarValue},
};

pub(super) fn parse_column_name_list(parser: &mut Parser) -> Result<Vec<String>, DdlSqlError> {
    parser.take(Token::LeftParen)?;
    let mut names = vec![parser.identifier()?];
    while parser.peek() == Some(&Token::Comma) {
        parser.position += 1;
        names.push(parser.identifier()?);
    }
    parser.take(Token::RightParen)?;
    Ok(names)
}

pub(super) fn has_duplicates(names: &[String]) -> bool {
    names.iter().collect::<std::collections::HashSet<_>>().len() != names.len()
}

pub(super) fn statement_parser(sql: &str) -> Result<Parser, DdlSqlError> {
    if sql.is_empty() || sql.len() > MAX_DDL_BYTES || sql.as_bytes().contains(&0) {
        return Err(DdlSqlError::InvalidStatement);
    }
    Ok(Parser {
        tokens: tokenize(sql)?,
        position: 0,
    })
}

pub(super) fn finish_statement(parser: &mut Parser) -> Result<(), DdlSqlError> {
    if parser.peek() == Some(&Token::Semicolon) {
        parser.position += 1;
    }
    if parser.peek().is_some() {
        return Err(DdlSqlError::InvalidStatement);
    }
    Ok(())
}

pub(super) struct Parser {
    pub(super) tokens: Vec<Token>,
    pub(super) position: usize,
}

impl Parser {
    pub(super) fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.position)
    }

    pub(super) fn take(&mut self, expected: Token) -> Result<(), DdlSqlError> {
        if self.peek() == Some(&expected) {
            self.position += 1;
            Ok(())
        } else {
            Err(DdlSqlError::InvalidStatement)
        }
    }

    pub(super) fn word(&mut self, expected: &str) -> Result<(), DdlSqlError> {
        match self.peek() {
            Some(Token::Word(word)) if word == expected => {
                self.position += 1;
                Ok(())
            }
            _ => Err(DdlSqlError::InvalidStatement),
        }
    }

    pub(super) fn peek_word(&self, expected: &str) -> bool {
        matches!(self.peek(), Some(Token::Word(word)) if word == expected)
    }

    pub(super) fn check_or(&mut self) -> Result<CheckExpressionSql, DdlSqlError> {
        let mut expressions = vec![self.check_and()?];
        while self.peek_word("or") {
            self.position += 1;
            expressions.push(self.check_and()?);
        }
        Ok(if expressions.len() == 1 {
            expressions.pop().unwrap()
        } else {
            CheckExpressionSql::Or(expressions)
        })
    }

    pub(super) fn check_and(&mut self) -> Result<CheckExpressionSql, DdlSqlError> {
        let mut expressions = vec![self.check_unary()?];
        while self.peek_word("and") {
            self.position += 1;
            expressions.push(self.check_unary()?);
        }
        Ok(if expressions.len() == 1 {
            expressions.pop().unwrap()
        } else {
            CheckExpressionSql::And(expressions)
        })
    }

    pub(super) fn check_unary(&mut self) -> Result<CheckExpressionSql, DdlSqlError> {
        if self.peek_word("not") {
            self.position += 1;
            return Ok(CheckExpressionSql::Not(Box::new(self.check_unary()?)));
        }
        if self.peek() == Some(&Token::LeftParen) {
            self.position += 1;
            let expression = self.check_or()?;
            self.take(Token::RightParen)?;
            if self.peek_word("is")
                || self.peek_word("between")
                || self.peek_word("in")
                || matches!(
                    self.peek(),
                    Some(Token::Plus | Token::Minus | Token::Star | Token::Percent | Token::Slash)
                )
                || (self.peek_word("not")
                    && matches!(self.tokens.get(self.position + 1), Some(Token::Word(word)) if word == "between" || word == "in"))
                || matches!(
                    self.peek(),
                    Some(
                        Token::Equal
                            | Token::NotEqual
                            | Token::Less
                            | Token::LessEqual
                            | Token::Greater
                            | Token::GreaterEqual
                    )
                )
            {
                let expression = self.check_value_expression_tail(expression)?;
                return self.check_value_predicate(expression);
            }
            return Ok(expression);
        }
        self.check_predicate()
    }

    pub(super) fn check_predicate(&mut self) -> Result<CheckExpressionSql, DdlSqlError> {
        let left = self.check_value_expression()?;
        self.check_value_predicate(left)
    }

    pub(super) fn check_value_expression(&mut self) -> Result<CheckExpressionSql, DdlSqlError> {
        let expression = self.check_value_term()?;
        self.check_value_expression_tail(expression)
    }

    pub(super) fn check_value_expression_tail(
        &mut self,
        expression: CheckExpressionSql,
    ) -> Result<CheckExpressionSql, DdlSqlError> {
        let mut expression = self.check_value_term_tail(expression)?;
        loop {
            let constructor = match self.peek() {
                Some(Token::Plus) => CheckExpressionSql::Add,
                Some(Token::Minus) => CheckExpressionSql::Subtract,
                _ => break,
            };
            self.position += 1;
            let right = self.check_value_term()?;
            expression = constructor(Box::new(expression), Box::new(right));
        }
        Ok(expression)
    }

    pub(super) fn check_value_term(&mut self) -> Result<CheckExpressionSql, DdlSqlError> {
        let expression = self.check_value_atom()?;
        self.check_value_term_tail(expression)
    }

    pub(super) fn check_value_term_tail(
        &mut self,
        mut expression: CheckExpressionSql,
    ) -> Result<CheckExpressionSql, DdlSqlError> {
        while matches!(
            self.peek(),
            Some(Token::Star | Token::Percent | Token::Slash)
        ) {
            let modulo = self.peek() == Some(&Token::Percent);
            let divide = self.peek() == Some(&Token::Slash);
            self.position += 1;
            let right = self.check_value_atom()?;
            expression = if modulo {
                CheckExpressionSql::Modulo(Box::new(expression), Box::new(right))
            } else if divide {
                CheckExpressionSql::Divide(Box::new(expression), Box::new(right))
            } else {
                CheckExpressionSql::Multiply(Box::new(expression), Box::new(right))
            };
        }
        Ok(expression)
    }

    pub(super) fn check_value_predicate(
        &mut self,
        left: CheckExpressionSql,
    ) -> Result<CheckExpressionSql, DdlSqlError> {
        if self.peek_word("is") {
            self.check_is_predicate(left)
        } else if self.peek_word("between")
            || (self.peek_word("not")
                && matches!(self.tokens.get(self.position + 1), Some(Token::Word(word)) if word == "between"))
        {
            let negated = if self.peek_word("not") {
                self.position += 1;
                true
            } else {
                false
            };
            self.word("between")?;
            let lower = self.check_value_expression()?;
            self.word("and")?;
            let upper = self.check_value_expression()?;
            Ok(CheckExpressionSql::Between {
                expression: Box::new(left),
                lower: Box::new(lower),
                upper: Box::new(upper),
                negated,
            })
        } else if self.peek_word("in")
            || (self.peek_word("not")
                && matches!(self.tokens.get(self.position + 1), Some(Token::Word(word)) if word == "in"))
        {
            let negated = if self.peek_word("not") {
                self.position += 1;
                true
            } else {
                false
            };
            self.word("in")?;
            self.take(Token::LeftParen)?;
            let mut values = Vec::new();
            loop {
                if values.len() == MAX_CHECK_IN_VALUES {
                    return Err(DdlSqlError::InvalidStatement);
                }
                values.push(Box::new(self.check_value_expression()?));
                if self.peek() == Some(&Token::RightParen) {
                    break;
                }
                self.take(Token::Comma)?;
            }
            if values.is_empty() {
                return Err(DdlSqlError::InvalidStatement);
            }
            self.take(Token::RightParen)?;
            Ok(CheckExpressionSql::In {
                expression: Box::new(left),
                values,
                negated,
            })
        } else {
            let operator = match self.peek() {
                Some(Token::Equal) => Some(CheckComparisonOperator::Equal),
                Some(Token::NotEqual) => Some(CheckComparisonOperator::NotEqual),
                Some(Token::Less) => Some(CheckComparisonOperator::Less),
                Some(Token::LessEqual) => Some(CheckComparisonOperator::LessEqual),
                Some(Token::Greater) => Some(CheckComparisonOperator::Greater),
                Some(Token::GreaterEqual) => Some(CheckComparisonOperator::GreaterEqual),
                _ => None,
            };
            if let Some(operator) = operator {
                self.position += 1;
                let right = if self.peek() == Some(&Token::LeftParen) {
                    self.check_unary()?
                } else {
                    self.check_value_expression()?
                };
                Ok(CheckExpressionSql::Compare {
                    operator,
                    left: Box::new(left),
                    right: Box::new(right),
                })
            } else {
                Ok(left)
            }
        }
    }

    pub(super) fn check_is_predicate(
        &mut self,
        left: CheckExpressionSql,
    ) -> Result<CheckExpressionSql, DdlSqlError> {
        self.word("is")?;
        let negated = if self.peek_word("not") {
            self.position += 1;
            true
        } else {
            false
        };
        if self.peek_word("distinct") {
            self.word("distinct")?;
            self.word("from")?;
            let right = self.check_unary()?;
            Ok(CheckExpressionSql::DistinctFrom {
                expression: Box::new(left),
                right: Box::new(right),
                negated,
            })
        } else if self.peek_word("true") || self.peek_word("false") || self.peek_word("unknown") {
            let test = if self.peek_word("true") {
                CheckBooleanTest::True
            } else if self.peek_word("false") {
                CheckBooleanTest::False
            } else {
                CheckBooleanTest::Unknown
            };
            self.position += 1;
            Ok(CheckExpressionSql::BooleanTest {
                expression: Box::new(left),
                test,
                negated,
            })
        } else {
            self.word("null")?;
            Ok(if negated {
                CheckExpressionSql::IsNotNull(Box::new(left))
            } else {
                CheckExpressionSql::IsNull(Box::new(left))
            })
        }
    }

    pub(super) fn check_value_atom(&mut self) -> Result<CheckExpressionSql, DdlSqlError> {
        if self.peek() == Some(&Token::Minus)
            && !matches!(self.tokens.get(self.position + 1), Some(Token::Number(_)))
        {
            self.position += 1;
            return Ok(CheckExpressionSql::Negate(Box::new(
                self.check_value_atom()?,
            )));
        }
        if self.peek() == Some(&Token::LeftParen) {
            self.position += 1;
            let expression = self.check_value_expression()?;
            self.take(Token::RightParen)?;
            return Ok(expression);
        }
        if self.peek_word("current_principal_id") {
            self.position += 1;
            self.take(Token::LeftParen)?;
            self.take(Token::RightParen)?;
            return Ok(CheckExpressionSql::CurrentPrincipalId);
        }
        if self.peek_word("has_role") {
            self.position += 1;
            self.take(Token::LeftParen)?;
            let role_name = match self.peek() {
                Some(Token::String(name)) => name.clone(),
                _ => return Err(DdlSqlError::InvalidStatement),
            };
            self.position += 1;
            self.take(Token::RightParen)?;
            return Ok(CheckExpressionSql::HasRole(role_name));
        }
        if self.peek_word("null") {
            self.position += 1;
            return Ok(CheckExpressionSql::Literal(CheckLiteralSql::Null));
        }
        if self.peek_word("true") || self.peek_word("false") {
            let value = self.peek_word("true");
            self.position += 1;
            return Ok(CheckExpressionSql::Literal(CheckLiteralSql::Boolean(value)));
        }
        if self.peek_word("x")
            && matches!(self.tokens.get(self.position + 1), Some(Token::String(_)))
        {
            self.position += 1;
            let Some(Token::String(value)) = self.peek() else {
                return Err(DdlSqlError::InvalidStatement);
            };
            let value = value.clone();
            decode_hex(&value)?;
            self.position += 1;
            return Ok(CheckExpressionSql::Literal(CheckLiteralSql::Bytea(value)));
        }
        if matches!(
            self.peek(),
            Some(Token::Number(_) | Token::Minus | Token::Plus)
        ) {
            let value = self.signed_number()?;
            if value
                .split_once('.')
                .is_some_and(|(_, fraction)| fraction.len() > 38)
            {
                return Err(DdlSqlError::InvalidStatement);
            }
            if self
                .peek()
                .is_some_and(|token| matches!(token, Token::Minus | Token::Plus))
            {
                self.position += 2;
            } else {
                self.position += 1;
            }
            return Ok(CheckExpressionSql::Literal(CheckLiteralSql::Number(value)));
        }
        if let Some(Token::String(value)) = self.peek() {
            let value = value.clone();
            self.position += 1;
            return Ok(CheckExpressionSql::Literal(CheckLiteralSql::String(value)));
        }
        Ok(CheckExpressionSql::Column(self.identifier()?))
    }

    pub(super) fn check_constraint_expression(
        &mut self,
    ) -> Result<CheckExpressionSql, DdlSqlError> {
        self.word("check")?;
        self.take(Token::LeftParen)?;
        let expression = self.check_or()?;
        self.take(Token::RightParen)?;
        Ok(expression)
    }

    pub(super) fn identifier(&mut self) -> Result<String, DdlSqlError> {
        match self.peek() {
            Some(Token::Word(word))
                if !word.is_empty()
                    && word.len() <= 128
                    && (word.as_bytes()[0].is_ascii_lowercase() || word.as_bytes()[0] == b'_')
                    && word.bytes().all(|byte| {
                        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'
                    }) =>
            {
                let word = word.clone();
                self.position += 1;
                Ok(word)
            }
            _ => Err(DdlSqlError::InvalidStatement),
        }
    }

    pub(super) fn scalar_type(&mut self) -> Result<ScalarType, DdlSqlError> {
        let name = match self.peek() {
            Some(Token::Word(word)) => word.clone(),
            _ => return Err(DdlSqlError::InvalidStatement),
        };
        self.position += 1;
        let scalar_type = match name.as_str() {
            "boolean" => ScalarType::Boolean,
            "smallint" => ScalarType::SmallInt,
            "integer" | "int" => ScalarType::Integer,
            "bigint" => ScalarType::BigInt,
            "text" => ScalarType::Text,
            "bytea" => ScalarType::Bytea,
            "uuid" => ScalarType::Uuid,
            "date" => ScalarType::Date,
            "timestamptz" => ScalarType::TimestampTz,
            "decimal" => {
                self.take(Token::LeftParen)?;
                let precision = self.number()?;
                self.take(Token::Comma)?;
                let scale = self.number()?;
                self.take(Token::RightParen)?;
                if !(1..=38).contains(&precision) || scale > precision {
                    return Err(DdlSqlError::InvalidStatement);
                }
                ScalarType::Decimal {
                    precision: precision as u8,
                    scale: scale as u8,
                }
            }
            "varchar" => {
                self.take(Token::LeftParen)?;
                let length = self.number()?;
                self.take(Token::RightParen)?;
                if length == 0 {
                    return Err(DdlSqlError::InvalidStatement);
                }
                ScalarType::Varchar { max_chars: length }
            }
            _ => return Err(DdlSqlError::InvalidStatement),
        };
        Ok(scalar_type)
    }

    pub(super) fn number(&mut self) -> Result<u32, DdlSqlError> {
        match self.peek() {
            Some(Token::Number(value)) => {
                let parsed = value.parse().map_err(|_| DdlSqlError::InvalidStatement)?;
                self.position += 1;
                Ok(parsed)
            }
            _ => Err(DdlSqlError::InvalidStatement),
        }
    }

    pub(super) fn default_value(
        &mut self,
        scalar_type: ScalarType,
    ) -> Result<AdminColumnDefault, DdlSqlError> {
        if self.peek_word("null") {
            self.position += 1;
            return Ok(AdminColumnDefault::Null);
        }
        if self.peek_word("current_principal_id") {
            if scalar_type != ScalarType::Uuid {
                return Err(DdlSqlError::InvalidStatement);
            }
            self.position += 1;
            self.take(Token::LeftParen)?;
            self.take(Token::RightParen)?;
            return Ok(AdminColumnDefault::CurrentPrincipalId);
        }
        let mut consumed_tokens = if matches!(self.peek(), Some(Token::Minus | Token::Plus)) {
            2
        } else {
            1
        };
        let value = match scalar_type {
            ScalarType::Boolean => match self.peek() {
                Some(Token::Word(word)) if word == "true" => ScalarValue::Boolean(true),
                Some(Token::Word(word)) if word == "false" => ScalarValue::Boolean(false),
                _ => return Err(DdlSqlError::InvalidStatement),
            },
            ScalarType::SmallInt => {
                ScalarValue::SmallInt(self.signed_number()?.parse().map_err(invalid)?)
            }
            ScalarType::Integer => {
                ScalarValue::Integer(self.signed_number()?.parse().map_err(invalid)?)
            }
            ScalarType::BigInt => {
                ScalarValue::BigInt(self.signed_number()?.parse().map_err(invalid)?)
            }
            ScalarType::Decimal { precision, scale } => ScalarValue::Decimal(
                parse_decimal_coefficient(&self.signed_number()?, precision, scale)?,
            ),
            ScalarType::Text => match self.peek() {
                Some(Token::String(value)) => ScalarValue::Text(value.clone()),
                _ => return Err(DdlSqlError::InvalidStatement),
            },
            ScalarType::Varchar { max_chars } => match self.peek() {
                Some(Token::String(value)) if value.chars().count() <= max_chars as usize => {
                    ScalarValue::Varchar(value.clone())
                }
                _ => return Err(DdlSqlError::InvalidStatement),
            },
            ScalarType::Bytea => {
                if !self.peek_word("x") {
                    return Err(DdlSqlError::InvalidStatement);
                }
                consumed_tokens = 2;
                let Some(Token::String(hex)) = self.tokens.get(self.position + 1) else {
                    return Err(DdlSqlError::InvalidStatement);
                };
                ScalarValue::Bytea(decode_hex(hex)?)
            }
            ScalarType::Uuid => {
                let Some(Token::String(text)) = self.peek() else {
                    return Err(DdlSqlError::InvalidStatement);
                };
                ScalarValue::Uuid(parse_uuid(text)?)
            }
            ScalarType::Date => ScalarValue::Date(self.signed_number()?.parse().map_err(invalid)?),
            ScalarType::TimestampTz => {
                ScalarValue::TimestampTz(self.signed_number()?.parse().map_err(invalid)?)
            }
        };
        self.position += consumed_tokens;
        Ok(AdminColumnDefault::Literal(value))
    }

    pub(super) fn default_literal(&mut self) -> Result<DefaultLiteralSql, DdlSqlError> {
        if self.peek_word("null") {
            self.position += 1;
            return Ok(DefaultLiteralSql::Null);
        }
        if self.peek_word("current_principal_id") {
            self.position += 1;
            self.take(Token::LeftParen)?;
            self.take(Token::RightParen)?;
            return Ok(DefaultLiteralSql::CurrentPrincipalId);
        }
        if self.peek_word("true") || self.peek_word("false") {
            let value = self.peek_word("true");
            self.position += 1;
            return Ok(DefaultLiteralSql::Boolean(value));
        }
        if self.peek_word("x") {
            self.position += 1;
            let Some(Token::String(hex)) = self.peek() else {
                return Err(DdlSqlError::InvalidStatement);
            };
            let value = decode_hex(hex)?;
            self.position += 1;
            return Ok(DefaultLiteralSql::Bytea(value));
        }
        if let Some(Token::String(value)) = self.peek() {
            let value = value.clone();
            self.position += 1;
            return Ok(DefaultLiteralSql::String(value));
        }
        if matches!(
            self.peek(),
            Some(Token::Number(_) | Token::Minus | Token::Plus)
        ) {
            let value = self.signed_number()?;
            if value
                .split_once('.')
                .is_some_and(|(_, fraction)| fraction.len() > 38)
            {
                return Err(DdlSqlError::InvalidStatement);
            }
            if self
                .peek()
                .is_some_and(|token| matches!(token, Token::Minus | Token::Plus))
            {
                self.position += 2;
            } else {
                self.position += 1;
            }
            return Ok(DefaultLiteralSql::Number(value));
        }
        Err(DdlSqlError::InvalidStatement)
    }

    pub(super) fn signed_number(&mut self) -> Result<String, DdlSqlError> {
        let (negative, number_position) = match self.peek() {
            Some(Token::Minus) => (true, self.position + 1),
            Some(Token::Plus) => (false, self.position + 1),
            _ => (false, self.position),
        };
        let Some(Token::Number(number)) = self.tokens.get(number_position) else {
            return Err(DdlSqlError::InvalidStatement);
        };
        if number.contains('.') && !matches!(number.as_bytes().last(), Some(b'0'..=b'9')) {
            return Err(DdlSqlError::InvalidStatement);
        }
        let mut number = number.clone();
        if negative {
            number.insert(0, '-');
        }
        Ok(number)
    }
}
